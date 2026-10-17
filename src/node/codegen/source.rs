/// Convert a source file path like `my_alpha.alpha.rs` to a Rust module name `my_alpha_alpha`.
pub(super) fn source_file_to_mod_name(path: &str) -> String {
    let file_name = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    // Strip .alpha or .pipe suffix if present
    let name = file_name
        .strip_suffix(".alpha")
        .or_else(|| file_name.strip_suffix(".pipe"))
        .or_else(|| file_name.strip_suffix(".logic"))
        .or_else(|| file_name.strip_suffix(".universe"))
        .unwrap_or(file_name);
    name.replace('-', "_").replace('.', "_")
}

pub(super) fn push_custom_module_decl(src: &mut String, mod_name: &str, source_file: &str) {
    let module_path = source_file
        .strip_prefix("src/")
        .unwrap_or(source_file)
        .replace('\\', "/")
        .replace('"', "\\\"");
    src.push_str(&format!(
        "#[path = \"{}\"]\nmod {};\n",
        module_path, mod_name
    ));
}

/// Convert "my_alpha_name" or "My Alpha Name" to "MyAlphaName".
pub(super) fn to_pascal_case(s: &str) -> String {
    s.split(|c: char| c == '_' || c == ' ' || c == '-')
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().to_string() + &chars.collect::<String>(),
            }
        })
        .collect()
}

/// Identifies if a source file corresponds to a built-in pipe and returns `(mod_name, struct_name)`
pub(super) fn get_builtin_pipe_info(
    src: &str,
    parsed_struct_name: &str,
) -> Option<(String, String)> {
    match src {
        "insight_submit" | "insight_submit.pipe.rs" => Some((
            "insight_submit".to_string(),
            "InsightSubmitPipe".to_string(),
        )),
        "allow_trading_window" => Some((
            "allow_trading_window".to_string(),
            "AllowTradingWindowPipe".to_string(),
        )),
        "and_pipe" => Some(("and_pipe".to_string(), "AndPipe".to_string())),
        "or_pipe" => Some(("or_pipe".to_string(), "OrPipe".to_string())),
        "cancel_opposite" => Some((
            "cancel_opposite".to_string(),
            "CancelOppositePipe".to_string(),
        )),
        "market_order_entry" => Some((
            "market_order_entry".to_string(),
            "MarketOrderEntryPipe".to_string(),
        )),
        "insight_ttl_config" => Some((
            "insight_ttl_config".to_string(),
            "InsightTtlConfigPipe".to_string(),
        )),
        "dynamic_quantity_to_risk" => Some((
            "dynamic_quantity_to_risk".to_string(),
            "DynamicQuantityToRiskPipe".to_string(),
        )),
        "percentage_risk_to_quantity" => Some((
            "percentage_risk_to_quantity".to_string(),
            "PercentageRiskToQuantityPipe".to_string(),
        )),
        "full_account_quantity_to_risk" => Some((
            "full_account_quantity_to_risk".to_string(),
            "FullAccountQuantityToRiskPipe".to_string(),
        )),
        "minimum_risk_to_reward" => Some((
            "minimum_risk_to_reward".to_string(),
            "MinimumRiskToRewardPipe".to_string(),
        )),
        "reject_expired_insight" => Some((
            "reject_expired_insight".to_string(),
            "RejectExpiredInsightPipe".to_string(),
        )),
        "percentage_dca_levels" => Some((
            "percentage_dca_levels".to_string(),
            "PercentageDcaLevelsPipe".to_string(),
        )),
        "basic_stop_loss" => Some((
            "basic_stop_loss".to_string(),
            "BasicStopLossPipe".to_string(),
        )),
        "basic_take_profit" => Some((
            "basic_take_profit".to_string(),
            "BasicTakeProfitPipe".to_string(),
        )),
        "scale_out" => Some(("scale_out".to_string(), "ScaleOutPipe".to_string())),
        "close_market_changed" => Some((
            "close_market_changed".to_string(),
            "CloseMarketChangedPipe".to_string(),
        )),
        "end_of_day_close" => Some((
            "end_of_day_close".to_string(),
            "EndOfDayClosePipe".to_string(),
        )),
        _ if src.starts_with("built_in") => {
            Some((source_file_to_mod_name(src), parsed_struct_name.to_string()))
        }
        _ => None,
    }
}

/// Identifies if a source file corresponds to a built-in lifecycle logic block and returns
/// `(mod_name, struct_name)`.
pub(super) fn get_builtin_lifecycle_info(
    src: &str,
    parsed_struct_name: &str,
) -> Option<(String, String)> {
    match src {
        "close_all_filled_positions" => Some((
            "close_all_filled_positions".to_string(),
            "CloseAllFilledPositions".to_string(),
        )),
        "preseed_warmup_history" => Some((
            "preseed_warmup_history".to_string(),
            "PreseedWarmupHistory".to_string(),
        )),
        _ if src.starts_with("built_in") => {
            Some((source_file_to_mod_name(src), parsed_struct_name.to_string()))
        }
        _ => None,
    }
}
