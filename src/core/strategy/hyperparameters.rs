//! Runtime support for applying deterministic hyperparameter seeds to a strategy.
//!
//! Generated strategies embed only their immutable sweep definition. AQE owns seed
//! selection, values exposed through [`StrategyContext`], sweep manifests, and the
//! metadata persisted alongside a completed backtest.

use super::StrategyContext;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

pub const HYPERPARAMETER_VALUES_KEY: &str = "__HYPER__";
pub const HYPERPARAMETER_SEED_KEY: &str = "__HYPER_SEED__";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HyperParameterSelection {
    pub seed: String,
    pub values: JsonValue,
}

impl HyperParameterSelection {
    pub fn from_process_args(args: &[String]) -> Result<Option<Self>, String> {
        let payload = args
            .windows(2)
            .find(|pair| pair[0] == "--hyper-params-json")
            .map(|pair| pair[1].clone())
            .or_else(|| std::env::var("AQE_HYPERPARAMS_JSON").ok());
        payload
            .map(|payload| {
                let selection: Self = serde_json::from_str(&payload)
                    .map_err(|error| format!("Invalid hyperparameter payload: {error}"))?;
                selection.validate()?;
                Ok(selection)
            })
            .transpose()
    }

    pub fn validate(&self) -> Result<(), String> {
        let values: BTreeMap<String, JsonValue> = serde_json::from_value(self.values.clone())
            .map_err(|_| "Hyperparameter values must be an object".to_string())?;
        if values.is_empty() {
            return Err("Hyperparameter values cannot be empty".to_string());
        }
        let canonical = serde_json::to_vec(&values).map_err(|error| error.to_string())?;
        let expected = format!("{:x}", Sha256::digest(canonical));
        if self.seed != expected {
            return Err("Hyperparameter seed does not match its values".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HyperParameterConfig {
    pub sweep_id: String,
    pub strategy_fingerprint: String,
    #[serde(default)]
    pub source_targets: JsonValue,
    #[serde(default)]
    pub definitions: JsonValue,
}

/// A code-first hyperparameter definition. `fallback` documents the ordinary value used by a
/// strategy when no seed or sweep command was requested.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HyperParameter {
    #[serde(alias = "name")]
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<JsonValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub increment: Option<f64>,
    #[serde(default)]
    pub fallback: JsonValue,
}

impl HyperParameter {
    pub fn new(key: impl Into<String>, fallback: impl Into<JsonValue>) -> Self {
        Self {
            key: key.into(),
            values: None,
            min: None,
            max: None,
            increment: None,
            fallback: fallback.into(),
        }
    }

    pub fn values(mut self, values: impl IntoIterator<Item = impl Into<JsonValue>>) -> Self {
        self.values = Some(values.into_iter().map(Into::into).collect());
        self
    }

    pub fn range(mut self, min: f64, max: f64, increment: f64) -> Self {
        self.min = Some(min);
        self.max = Some(max);
        self.increment = Some(increment);
        self
    }

    fn value_type(&self) -> Result<&'static str, String> {
        let value = self
            .values
            .as_ref()
            .and_then(|values| values.first())
            .unwrap_or(&self.fallback);
        if value.is_i64() || value.is_u64() {
            Ok("int")
        } else if value.is_f64() {
            Ok("float")
        } else if value.is_boolean() {
            Ok("bool")
        } else if value.is_string() {
            Ok("string")
        } else {
            Err(format!(
                "hyperparameter '{}' needs a scalar fallback",
                self.key
            ))
        }
    }

    fn definition_json(&self) -> Result<JsonValue, String> {
        if self.key.trim().is_empty() {
            return Err("hyperparameter key cannot be empty".to_string());
        }
        let mut definition = serde_json::json!({
            "name": self.key,
            "type": self.value_type()?,
            "fallback": self.fallback,
        });
        let object = definition
            .as_object_mut()
            .expect("hyperparameter definition is an object");
        if let Some(values) = &self.values {
            object.insert("values".to_string(), JsonValue::Array(values.clone()));
        }
        if let Some(min) = self.min {
            object.insert("min".to_string(), JsonValue::from(min));
        }
        if let Some(max) = self.max {
            object.insert("max".to_string(), JsonValue::from(max));
        }
        if let Some(increment) = self.increment {
            object.insert("increment".to_string(), JsonValue::from(increment));
        }
        Ok(definition)
    }
}

/// Lazy Cartesian-product iterator. It stores domains and an index per domain, never all runs.
pub struct HyperParameterRunIter {
    domains: Vec<(String, Vec<JsonValue>)>,
    indices: Option<Vec<usize>>,
}

impl Iterator for HyperParameterRunIter {
    type Item = HyperParameterSelection;

    fn next(&mut self) -> Option<Self::Item> {
        let indices = self.indices.as_mut()?;
        let mut values = BTreeMap::new();
        for ((name, domain), index) in self.domains.iter().zip(indices.iter()) {
            values.insert(name.clone(), domain[*index].clone());
        }
        let seed = format!("{:x}", Sha256::digest(serde_json::to_vec(&values).ok()?));

        // Advance as an odometer, with the final parameter changing fastest.
        let mut carry = true;
        for position in (0..indices.len()).rev() {
            if indices[position] + 1 < self.domains[position].1.len() {
                indices[position] += 1;
                carry = false;
                break;
            }
            indices[position] = 0;
        }
        if carry {
            self.indices = None;
        }

        Some(HyperParameterSelection {
            seed,
            values: serde_json::to_value(values).expect("BTreeMap serializes"),
        })
    }
}

impl HyperParameterConfig {
    pub fn new() -> Self {
        Self {
            sweep_id: String::new(),
            strategy_fingerprint: String::new(),
            source_targets: JsonValue::Null,
            definitions: JsonValue::Array(Vec::new()),
        }
    }

    /// Set an explicit stable group ID. If omitted, `ensure_sweep_id` assigns a UUID when the
    /// configuration first executes a full sweep.
    pub fn set_sweep_id(&mut self, sweep_id: impl Into<String>) -> &mut Self {
        self.sweep_id = sweep_id.into();
        self
    }

    pub fn set_strategy_fingerprint(&mut self, fingerprint: impl Into<String>) -> &mut Self {
        self.strategy_fingerprint = fingerprint.into();
        self
    }

    pub fn set_source_targets(&mut self, source_targets: JsonValue) -> &mut Self {
        self.source_targets = source_targets;
        self
    }

    pub fn add_hyper_parameter(&mut self, parameter: HyperParameter) -> Result<&mut Self, String> {
        let definition = parameter.definition_json()?;
        let definitions = self
            .definitions
            .as_array_mut()
            .ok_or("Hyperparameter definitions must be an array")?;
        if definitions.iter().any(|item| {
            item.get("name").and_then(JsonValue::as_str) == Some(parameter.key.as_str())
        }) {
            return Err(format!(
                "hyperparameter '{}' is already registered",
                parameter.key
            ));
        }
        definitions.push(definition);
        Ok(self)
    }

    pub fn get_hyperparameters(&self) -> Result<Vec<HyperParameter>, String> {
        self.definitions
            .as_array()
            .ok_or("Hyperparameter definitions must be an array")?
            .iter()
            .map(|definition| {
                let object = definition
                    .as_object()
                    .ok_or("Invalid hyperparameter definition")?;
                Ok(HyperParameter {
                    key: object
                        .get("name")
                        .and_then(JsonValue::as_str)
                        .ok_or("Hyperparameter name is missing")?
                        .to_string(),
                    values: object.get("values").and_then(JsonValue::as_array).cloned(),
                    min: object.get("min").and_then(JsonValue::as_f64),
                    max: object.get("max").and_then(JsonValue::as_f64),
                    increment: object.get("increment").and_then(JsonValue::as_f64),
                    fallback: object.get("fallback").cloned().unwrap_or(JsonValue::Null),
                })
            })
            .collect()
    }

    pub fn ensure_sweep_id(&mut self) -> &str {
        if self.sweep_id.trim().is_empty() {
            self.sweep_id = uuid::Uuid::new_v4().to_string();
        }
        &self.sweep_id
    }

    pub fn is_sweep_requested(&self, args: &[String]) -> bool {
        args.iter().any(|arg| arg == "--hyper-sweep")
    }

    /// Parses AQS's compact payload when supplied, or resolves the ergonomic public
    /// `--hyper-seed <seed>` form against this config's lazy iterator.
    pub fn selection_from_process_args(
        &self,
        args: &[String],
    ) -> Result<Option<HyperParameterSelection>, String> {
        if let Some(selection) = HyperParameterSelection::from_process_args(args)? {
            return Ok(Some(selection));
        }
        let Some(seed_prefix) = args
            .windows(2)
            .find(|pair| pair[0] == "--hyper-seed")
            .map(|pair| pair[1].trim())
        else {
            return Ok(None);
        };
        if seed_prefix.is_empty() {
            return Err("Hyperparameter seed prefix cannot be empty".to_string());
        }
        let mut matches = self
            .iter_runs()?
            .filter(|selection| selection.seed.starts_with(seed_prefix));
        let Some(selection) = matches.next() else {
            return Err(format!(
                "Unknown hyperparameter seed or prefix: {seed_prefix}"
            ));
        };
        if let Some(other) = matches.next() {
            return Err(format!(
                "Ambiguous hyperparameter seed prefix '{seed_prefix}' (matches {} and {}); provide more characters",
                selection.seed.get(..12).unwrap_or(&selection.seed),
                other.seed.get(..12).unwrap_or(&other.seed),
            ));
        }
        Ok(Some(selection))
    }

    /// The only CLI-aware API needed by generated and handwritten entrypoints.
    pub fn process_runs(
        &mut self,
        args: &[String],
    ) -> Result<Box<dyn Iterator<Item = Option<HyperParameterSelection>>>, String> {
        if self.is_sweep_requested(args) {
            self.ensure_sweep_id();
            return Ok(Box::new(self.iter_runs()?.map(Some)));
        }
        Ok(Box::new(std::iter::once(
            self.selection_from_process_args(args)?,
        )))
    }

    /// Build a lazy iterator only for an explicit full sweep. Selected normal runs never call
    /// this and a sweep keeps no Cartesian product in memory.
    pub fn iter_runs(&self) -> Result<HyperParameterRunIter, String> {
        let definitions = self
            .definitions
            .as_array()
            .ok_or("Hyperparameter definitions must be an array")?;
        let mut domains = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let object = definition
                .as_object()
                .ok_or("Invalid hyperparameter definition")?;
            let name = object
                .get("name")
                .and_then(JsonValue::as_str)
                .ok_or("Hyperparameter name is missing")?
                .to_string();
            let kind = object
                .get("valueType")
                .or_else(|| object.get("type"))
                .and_then(JsonValue::as_str)
                .unwrap_or("float");
            let values = object
                .get("values")
                .and_then(JsonValue::as_array)
                .filter(|values| !values.is_empty())
                .cloned()
                .unwrap_or_else(|| {
                    if !matches!(kind, "int" | "float") {
                        return Vec::new();
                    }
                    let min = object.get("min").and_then(JsonValue::as_f64).unwrap_or(0.0);
                    let max = object.get("max").and_then(JsonValue::as_f64).unwrap_or(min);
                    let increment = object
                        .get("increment")
                        .and_then(JsonValue::as_f64)
                        .unwrap_or(1.0);
                    if !min.is_finite()
                        || !max.is_finite()
                        || !increment.is_finite()
                        || increment <= 0.0
                        || max < min
                    {
                        return Vec::new();
                    }
                    let mut values = Vec::new();
                    let mut value = min;
                    while value <= max + increment * 1e-9 {
                        values.push(if kind == "int" {
                            JsonValue::from(value.round() as i64)
                        } else {
                            JsonValue::from(value)
                        });
                        value += increment;
                    }
                    values
                });
            if values.is_empty() {
                return Err(format!("Hyperparameter '{name}' has no values"));
            }
            domains.push((name, values));
        }
        domains.sort_by(|left, right| left.0.cmp(&right.0));
        let indices = (!domains.is_empty()).then(|| vec![0; domains.len()]);
        Ok(HyperParameterRunIter { domains, indices })
    }

    pub fn write_sweep_manifest(&self, root: &Path) -> Result<(), String> {
        let directory = root.join("backtests").join("hyper").join(&self.sweep_id);
        std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
        let manifest = serde_json::json!({
            "sweep_id": self.sweep_id,
            "strategy_fingerprint": self.strategy_fingerprint,
            "source_targets": self.source_targets,
            "definitions": self.definitions,
        });
        std::fs::write(
            directory.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())
    }
}

/// Read the selected hyperparameter map from a strategy context.
pub fn hyper_values(ctx: &dyn StrategyContext) -> Option<JsonValue> {
    ctx.variables()
        .get(HYPERPARAMETER_VALUES_KEY)
        .map(|value| value.clone())
}

pub fn hyper_int<T>(ctx: &dyn StrategyContext, key: &str, fallback: T) -> T
where
    T: TryFrom<i64> + Copy,
{
    hyper_values(ctx)
        .and_then(|values| values.get(key).and_then(JsonValue::as_i64))
        .and_then(|value| T::try_from(value).ok())
        .unwrap_or(fallback)
}

pub fn hyper_float(ctx: &dyn StrategyContext, key: &str, fallback: f64) -> f64 {
    hyper_values(ctx)
        .and_then(|values| values.get(key).and_then(JsonValue::as_f64))
        .unwrap_or(fallback)
}

pub fn hyper_bool(ctx: &dyn StrategyContext, key: &str, fallback: bool) -> bool {
    hyper_values(ctx)
        .and_then(|values| values.get(key).and_then(JsonValue::as_bool))
        .unwrap_or(fallback)
}

pub fn hyper_string(ctx: &dyn StrategyContext, key: &str, fallback: String) -> String {
    hyper_values(ctx)
        .and_then(|values| {
            values
                .get(key)
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_validates_a_compact_selected_payload() {
        let values = serde_json::json!({ "period": 14 });
        let mut map = BTreeMap::new();
        map.insert("period", JsonValue::from(14));
        let seed = format!("{:x}", Sha256::digest(serde_json::to_vec(&map).unwrap()));
        let payload = serde_json::json!({ "seed": seed, "values": values }).to_string();
        let selection =
            HyperParameterSelection::from_process_args(&["--hyper-params-json".into(), payload])
                .unwrap()
                .unwrap();
        assert_eq!(selection.values["period"], 14);
    }

    #[test]
    fn iterates_domains_lazily_for_a_sweep() {
        let config = HyperParameterConfig {
            sweep_id: "sweep".into(),
            strategy_fingerprint: "sweep".into(),
            source_targets: JsonValue::Null,
            definitions: serde_json::json!([{
                "name": "period", "valueType": "int", "values": [7, 14]
            }]),
        };
        assert_eq!(config.iter_runs().unwrap().count(), 2);
    }

    #[test]
    fn code_first_config_resolves_public_seed_flags_without_a_payload() {
        let mut config = HyperParameterConfig::new();
        config
            .add_hyper_parameter(HyperParameter::new("period", 14).values([7, 14]))
            .unwrap();
        let seed = config.iter_runs().unwrap().last().unwrap().seed;
        let selection = config
            .selection_from_process_args(&["--hyper-seed".to_string(), seed.clone()])
            .unwrap()
            .unwrap();
        assert_eq!(selection.seed, seed);
        assert_eq!(selection.values["period"], 14);
        assert_eq!(
            config
                .process_runs(&["--hyper-sweep".to_string()])
                .unwrap()
                .count(),
            2
        );
        assert!(!config.sweep_id.is_empty());
    }

    #[test]
    fn code_first_config_resolves_a_unique_seed_prefix() {
        let mut config = HyperParameterConfig::new();
        config
            .add_hyper_parameter(HyperParameter::new("period", 14).values([7, 14]))
            .unwrap();
        let seed = config.iter_runs().unwrap().next().unwrap().seed;
        let prefix = seed[..8].to_string();
        let selection = config
            .selection_from_process_args(&["--hyper-seed".to_string(), prefix])
            .unwrap()
            .unwrap();
        assert_eq!(selection.seed, seed);
    }
}
