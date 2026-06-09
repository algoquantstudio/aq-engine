use crate::core::broker::types::Asset;
use crate::core::strategy::StrategyContext;
use log::{error, warn};

pub mod close_all_filled_positions;
pub mod preseed_warmup_history;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecyclePhase {
    OnStart,
    OnInit,
    OnTeardown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleTiming {
    BeforeGenerated,
    AfterGenerated,
}

impl Default for LifecycleTiming {
    fn default() -> Self {
        Self::BeforeGenerated
    }
}

#[derive(Clone, Debug)]
pub struct LifecycleResult {
    pub success: bool,
    pub message: Option<String>,
    pub logic_name: String,
}

impl LifecycleResult {
    pub fn new(success: bool, message: Option<String>, logic_name: String) -> Self {
        Self {
            success,
            message,
            logic_name,
        }
    }

    pub fn passed(logic_name: String) -> Self {
        Self::new(true, None, logic_name)
    }
}

pub trait OnStartLogic {
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("OnStartLogic")
    }

    fn version(&self) -> &str;

    fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult;
}

pub trait OnInitLogic {
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("OnInitLogic")
    }

    fn version(&self) -> &str;

    fn run(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) -> LifecycleResult;
}

pub trait OnTeardownLogic {
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("OnTeardownLogic")
    }

    fn version(&self) -> &str;

    fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult;
}

fn handle_lifecycle_result(kind: &str, can_fail: bool, result: &LifecycleResult) {
    if result.success {
        return;
    }

    let message = result
        .message
        .as_deref()
        .unwrap_or("lifecycle logic returned an unsuccessful result");
    if can_fail {
        warn!(
            "{} lifecycle logic {} failed but can_fail=true: {}",
            kind, result.logic_name, message
        );
    } else {
        error!(
            "{} lifecycle logic {} failed: {}",
            kind, result.logic_name, message
        );
        panic!(
            "{} lifecycle logic {} failed: {}",
            kind, result.logic_name, message
        );
    }
}

pub struct OnStartLogicBuilder {
    inner: Box<dyn OnStartLogic>,
    timing: LifecycleTiming,
    can_fail: bool,
}

impl OnStartLogicBuilder {
    pub fn new(inner: Box<dyn OnStartLogic>) -> Self {
        Self {
            inner,
            timing: LifecycleTiming::BeforeGenerated,
            can_fail: false,
        }
    }

    pub fn timing(mut self, timing: LifecycleTiming) -> Self {
        self.timing = timing;
        self
    }

    pub fn can_fail(mut self, can_fail: bool) -> Self {
        self.can_fail = can_fail;
        self
    }

    pub fn build(self) -> WrappedOnStartLogic {
        WrappedOnStartLogic {
            inner: self.inner,
            timing: self.timing,
            can_fail: self.can_fail,
            runs_count: 0,
            success_count: 0,
        }
    }
}

pub struct WrappedOnStartLogic {
    inner: Box<dyn OnStartLogic>,
    timing: LifecycleTiming,
    can_fail: bool,
    pub runs_count: usize,
    pub success_count: usize,
}

impl WrappedOnStartLogic {
    pub fn builder(inner: Box<dyn OnStartLogic>) -> OnStartLogicBuilder {
        OnStartLogicBuilder::new(inner)
    }

    pub fn timing(&self) -> LifecycleTiming {
        self.timing
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

    pub fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult {
        self.runs_count += 1;
        let result = self.inner.run(ctx);
        if result.success {
            self.success_count += 1;
        }
        handle_lifecycle_result("on_start", self.can_fail, &result);
        result
    }
}

pub struct OnInitLogicBuilder {
    inner: Box<dyn OnInitLogic>,
    timing: LifecycleTiming,
    can_fail: bool,
}

impl OnInitLogicBuilder {
    pub fn new(inner: Box<dyn OnInitLogic>) -> Self {
        Self {
            inner,
            timing: LifecycleTiming::BeforeGenerated,
            can_fail: false,
        }
    }

    pub fn timing(mut self, timing: LifecycleTiming) -> Self {
        self.timing = timing;
        self
    }

    pub fn can_fail(mut self, can_fail: bool) -> Self {
        self.can_fail = can_fail;
        self
    }

    pub fn build(self) -> WrappedOnInitLogic {
        WrappedOnInitLogic {
            inner: self.inner,
            timing: self.timing,
            can_fail: self.can_fail,
            runs_count: 0,
            success_count: 0,
        }
    }
}

pub struct WrappedOnInitLogic {
    inner: Box<dyn OnInitLogic>,
    timing: LifecycleTiming,
    can_fail: bool,
    pub runs_count: usize,
    pub success_count: usize,
}

impl WrappedOnInitLogic {
    pub fn builder(inner: Box<dyn OnInitLogic>) -> OnInitLogicBuilder {
        OnInitLogicBuilder::new(inner)
    }

    pub fn timing(&self) -> LifecycleTiming {
        self.timing
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

    pub fn run(&mut self, ctx: &mut dyn StrategyContext, asset: &Asset) -> LifecycleResult {
        self.runs_count += 1;
        let result = self.inner.run(ctx, asset);
        if result.success {
            self.success_count += 1;
        }
        handle_lifecycle_result("on_init", self.can_fail, &result);
        result
    }
}

pub struct OnTeardownLogicBuilder {
    inner: Box<dyn OnTeardownLogic>,
    timing: LifecycleTiming,
    can_fail: bool,
}

impl OnTeardownLogicBuilder {
    pub fn new(inner: Box<dyn OnTeardownLogic>) -> Self {
        Self {
            inner,
            timing: LifecycleTiming::BeforeGenerated,
            can_fail: true,
        }
    }

    pub fn timing(mut self, timing: LifecycleTiming) -> Self {
        self.timing = timing;
        self
    }

    pub fn can_fail(mut self, can_fail: bool) -> Self {
        self.can_fail = can_fail;
        self
    }

    pub fn build(self) -> WrappedOnTeardownLogic {
        WrappedOnTeardownLogic {
            inner: self.inner,
            timing: self.timing,
            can_fail: self.can_fail,
            runs_count: 0,
            success_count: 0,
        }
    }
}

pub struct WrappedOnTeardownLogic {
    inner: Box<dyn OnTeardownLogic>,
    timing: LifecycleTiming,
    can_fail: bool,
    pub runs_count: usize,
    pub success_count: usize,
}

impl WrappedOnTeardownLogic {
    pub fn builder(inner: Box<dyn OnTeardownLogic>) -> OnTeardownLogicBuilder {
        OnTeardownLogicBuilder::new(inner)
    }

    pub fn timing(&self) -> LifecycleTiming {
        self.timing
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

    pub fn run(&mut self, ctx: &mut dyn StrategyContext) -> LifecycleResult {
        self.runs_count += 1;
        let result = self.inner.run(ctx);
        if result.success {
            self.success_count += 1;
        }
        handle_lifecycle_result("on_teardown", self.can_fail, &result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingStart;

    impl OnStartLogic for FailingStart {
        fn version(&self) -> &str {
            "1.0"
        }

        fn run(&mut self, _ctx: &mut dyn StrategyContext) -> LifecycleResult {
            LifecycleResult::new(false, Some("failed".to_string()), self.name().to_string())
        }
    }

    #[test]
    fn start_logic_defaults_to_strict_failure() {
        let wrapper = OnStartLogicBuilder::new(Box::new(FailingStart)).build();

        assert_eq!(wrapper.timing(), LifecycleTiming::BeforeGenerated);
        assert!(!wrapper.can_fail());
    }

    #[test]
    fn teardown_logic_defaults_to_can_fail() {
        struct Teardown;
        impl OnTeardownLogic for Teardown {
            fn version(&self) -> &str {
                "1.0"
            }

            fn run(&mut self, _ctx: &mut dyn StrategyContext) -> LifecycleResult {
                LifecycleResult::passed(self.name().to_string())
            }
        }

        let wrapper = OnTeardownLogicBuilder::new(Box::new(Teardown)).build();

        assert_eq!(wrapper.timing(), LifecycleTiming::BeforeGenerated);
        assert!(wrapper.can_fail());
    }
}
