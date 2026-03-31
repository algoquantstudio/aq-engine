use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum InsightState {
    New,
    Executed,
    Filled,
    Closed,
    Cancelled,
    Rejected,
}

impl InsightState {
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            InsightState::New | InsightState::Executed | InsightState::Filled
        )
    }

    pub fn is_inactive(&self) -> bool {
        matches!(
            self,
            InsightState::Closed | InsightState::Cancelled | InsightState::Rejected
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StrategyType {
    Manual,
    Testing,
    Portfolio,
    Custom(String),
}

impl std::fmt::Display for StrategyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyType::Manual => write!(f, "Manual"),
            StrategyType::Testing => write!(f, "Testing"),
            StrategyType::Portfolio => write!(f, "Portfolio"),
            StrategyType::Custom(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StrategyDependentConfirmation {
    None,
    HighRelativeVolumeConfirmationModel,
    LowRelativeVolumeConfirmationModel,
    HighTimeFrameConfirmationModel,
    LowTimeFrameConfirmationModel,
    HighConfidenceConfirmationModel,
    LowConfidenceConfirmationModel,
    UpStateConfirmationModel,
    DownStateConfirmationModel,
    FlatStateConfirmationModel,
    Custom(String),
}

#[derive(Clone, Debug)]
pub enum InsightValidation {
    Valid,
    Invalid(String),
}

#[cfg(not(target_arch = "wasm32"))]
use crate::core::broker::types::OrderSide;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartialCloseResult {
    pub order_id: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub entry_price: f64,
    pub filled_price: Option<f64>,
}

#[cfg(not(target_arch = "wasm32"))]
impl PartialCloseResult {
    pub fn new(order_id: String, side: OrderSide, quantity: f64, entry_price: f64) -> Self {
        Self {
            order_id,
            side,
            quantity,
            entry_price,
            filled_price: None,
        }
    }

    pub fn set_filled_price(&mut self, filled_price: f64) {
        self.filled_price = Some(filled_price);
    }

    pub fn get_pl(&self) -> f64 {
        let fp = self.filled_price.unwrap_or(self.entry_price);
        if self.side == OrderSide::Buy {
            (fp - self.entry_price) * self.quantity
        } else {
            (self.entry_price - fp) * self.quantity
        }
    }
}
