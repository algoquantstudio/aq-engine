use crate::core::strategy::StrategyContext;
use log::{error, warn};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct UniverseResult {
    pub symbols: HashSet<String>,
    pub success: bool,
    pub message: Option<String>,
    pub model_name: String,
}

impl UniverseResult {
    pub fn new(
        symbols: HashSet<String>,
        success: bool,
        message: Option<String>,
        model_name: String,
    ) -> Self {
        Self {
            symbols,
            success,
            message,
            model_name,
        }
    }

    pub fn passed(symbols: HashSet<String>, model_name: String) -> Self {
        Self::new(symbols, true, None, model_name)
    }
}

pub trait UniverseModel {
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("UniverseModel")
    }

    fn version(&self) -> &str;

    fn run(&mut self, ctx: &mut dyn StrategyContext) -> UniverseResult;
}

pub struct UniverseModelBuilder {
    inner: Box<dyn UniverseModel>,
    can_fail: bool,
}

impl UniverseModelBuilder {
    pub fn new(inner: Box<dyn UniverseModel>) -> Self {
        Self {
            inner,
            can_fail: false,
        }
    }

    pub fn can_fail(mut self, can_fail: bool) -> Self {
        self.can_fail = can_fail;
        self
    }

    pub fn build(self) -> WrappedUniverseModel {
        WrappedUniverseModel {
            inner: self.inner,
            can_fail: self.can_fail,
            runs_count: 0,
            success_count: 0,
        }
    }
}

pub struct WrappedUniverseModel {
    inner: Box<dyn UniverseModel>,
    can_fail: bool,
    pub runs_count: usize,
    pub success_count: usize,
}

impl WrappedUniverseModel {
    pub fn builder(inner: Box<dyn UniverseModel>) -> UniverseModelBuilder {
        UniverseModelBuilder::new(inner)
    }

    pub fn can_fail(&self) -> bool {
        self.can_fail
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn version(&self) -> &str {
        self.inner.version()
    }

    pub fn run(&mut self, ctx: &mut dyn StrategyContext) -> UniverseResult {
        self.runs_count += 1;
        let result = self.inner.run(ctx);
        if result.success {
            self.success_count += 1;
        }
        if !result.success {
            let message = result
                .message
                .as_deref()
                .unwrap_or("universe model returned an unsuccessful result");
            if self.can_fail {
                warn!(
                    "Universe model {} failed but can_fail=true: {}",
                    result.model_name, message
                );
            } else {
                error!("Universe model {} failed: {}", result.model_name, message);
                panic!("Universe model {} failed: {}", result.model_name, message);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EmptyUniverse;

    impl UniverseModel for EmptyUniverse {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(&mut self, _ctx: &mut dyn StrategyContext) -> UniverseResult {
            UniverseResult::passed(HashSet::new(), self.name().to_string())
        }
    }

    #[test]
    fn universe_model_defaults_to_strict_failure() {
        let wrapper = UniverseModelBuilder::new(Box::new(EmptyUniverse)).build();

        assert!(!wrapper.can_fail());
    }
}
