use crate::core::strategy::StrategyMode;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::io::{IsTerminal, Write};
use std::sync::{
    Arc, Mutex, OnceLock, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;

const DEFAULT_EVENT_CAPACITY: usize = 256;
const GLOBAL_LOG_CAPACITY: usize = 512;
const DEFAULT_RENDER_INTERVAL_MS: u64 = 250;
#[cfg(feature = "tui")]
const EVENTS_VISIBLE_ROWS: usize = 5;
const VARIABLE_VALUE_LIMIT: usize = 160;
const VARIABLE_CHILD_LIMIT: usize = 64;
const VARIABLE_DEPTH_LIMIT: usize = 5;

static TERMINAL_OUTPUT_SUSPENDED: AtomicBool = AtomicBool::new(false);
static GLOBAL_TUI_LOG_EVENTS: OnceLock<Mutex<VecDeque<RuntimeEventSnapshot>>> = OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TuiConfig {
    pub enabled: bool,
    pub forced: bool,
    pub render_interval: Duration,
}

impl TuiConfig {
    pub fn from_args<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
        let disabled = args.iter().any(|arg| arg == "--no-tui");
        let forced = args.iter().any(|arg| arg == "--tui");
        let is_tty = std::io::stdout().is_terminal() && std::io::stderr().is_terminal();
        Self {
            enabled: !disabled && is_tty && Self::feature_enabled(),
            forced,
            render_interval: Duration::from_millis(DEFAULT_RENDER_INTERVAL_MS),
        }
    }

    pub fn from_process_args() -> Self {
        Self::from_args(std::env::args())
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            forced: false,
            render_interval: Duration::from_millis(DEFAULT_RENDER_INTERVAL_MS),
        }
    }

    fn feature_enabled() -> bool {
        cfg!(feature = "tui")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BacktestProgressSnapshot {
    pub processed_steps: usize,
    pub total_steps: usize,
    pub progress_pct: f64,
    pub current_time: Option<DateTime<Utc>>,
    pub stream_count: usize,
    pub tradable_stream_count: usize,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeVariableSnapshot {
    pub key: String,
    pub value: String,
    pub truncated: bool,
    pub children: Vec<RuntimeVariableSnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeInsightStateSnapshot {
    pub at: DateTime<Utc>,
    pub state: String,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeInsightSnapshot {
    pub insight_id: String,
    pub parent_id: Option<String>,
    pub symbol: String,
    pub state: String,
    pub side: String,
    pub strategy_type: String,
    pub order_id: Option<String>,
    pub close_order_id: Option<String>,
    pub quantity: Option<f64>,
    pub contracts: Option<f64>,
    pub order_type: String,
    pub order_class: String,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub take_profit_levels: Option<Vec<f64>>,
    pub stop_loss_levels: Option<Vec<f64>>,
    pub filled_price: Option<f64>,
    pub close_price: Option<f64>,
    pub confidence: u8,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub state_history: Vec<RuntimeInsightStateSnapshot>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeMetricsSnapshot {
    pub final_equity: f64,
    pub total_return_pct: f64,
    pub total_trades: usize,
    pub open_positions_count: usize,
    pub open_insights_count: usize,
    pub updated_at: Option<DateTime<Utc>>,
    pub summary_lines: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeEventSnapshot {
    pub created_at: DateTime<Utc>,
    pub level: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeSnapshot {
    pub mode: StrategyMode,
    pub strategy_name: String,
    pub strategy_id: String,
    pub status: String,
    pub current_time: Option<DateTime<Utc>>,
    pub universe: Vec<String>,
    pub variables: Vec<RuntimeVariableSnapshot>,
    pub insight_counts: HashMap<String, usize>,
    pub active_insights: Vec<RuntimeInsightSnapshot>,
    pub metrics: Option<RuntimeMetricsSnapshot>,
    pub backtest_progress: Option<BacktestProgressSnapshot>,
    pub aqs_sync_status: String,
    pub broker_status: String,
    pub datafeed_status: String,
    pub saved_result_path: Option<String>,
    pub dropped_events: u64,
    pub events: Vec<RuntimeEventSnapshot>,
    pub updated_at: DateTime<Utc>,
}

impl Default for RuntimeSnapshot {
    fn default() -> Self {
        Self {
            mode: StrategyMode::Backtest,
            strategy_name: String::new(),
            strategy_id: String::new(),
            status: "Initialised".to_string(),
            current_time: None,
            universe: Vec::new(),
            variables: Vec::new(),
            insight_counts: HashMap::new(),
            active_insights: Vec::new(),
            metrics: None,
            backtest_progress: None,
            aqs_sync_status: "not configured".to_string(),
            broker_status: "unknown".to_string(),
            datafeed_status: "unknown".to_string(),
            saved_result_path: None,
            dropped_events: 0,
            events: Vec::new(),
            updated_at: Utc::now(),
        }
    }
}

#[derive(Debug)]
pub struct RuntimeTelemetry {
    snapshot: Arc<RwLock<RuntimeSnapshot>>,
    events: VecDeque<RuntimeEventSnapshot>,
    event_capacity: usize,
    dropped_events: Arc<AtomicU64>,
    strategy_stop_requested: Arc<AtomicBool>,
    tui_close_requested: Arc<AtomicBool>,
    tui_handle: Option<JoinHandle<()>>,
}

impl Default for RuntimeTelemetry {
    fn default() -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(RuntimeSnapshot::default())),
            events: VecDeque::with_capacity(DEFAULT_EVENT_CAPACITY),
            event_capacity: DEFAULT_EVENT_CAPACITY,
            dropped_events: Arc::new(AtomicU64::new(0)),
            strategy_stop_requested: Arc::new(AtomicBool::new(false)),
            tui_close_requested: Arc::new(AtomicBool::new(false)),
            tui_handle: None,
        }
    }
}

impl RuntimeTelemetry {
    pub fn snapshot_handle(&self) -> Arc<RwLock<RuntimeSnapshot>> {
        self.snapshot.clone()
    }

    fn push_event_snapshot(&mut self, event: RuntimeEventSnapshot) {
        if self.events.len() >= self.event_capacity {
            self.events.pop_front();
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
        self.events.push_back(event);
    }

    fn drain_global_log_events(&mut self) {
        for event in drain_pending_global_log_events() {
            self.push_event_snapshot(event);
        }
    }

    pub fn update_snapshot<F>(&mut self, update: F)
    where
        F: FnOnce(&mut RuntimeSnapshot),
    {
        self.drain_global_log_events();
        let events = self.events.iter().cloned().collect::<Vec<_>>();
        let dropped_events = self.dropped_events.load(Ordering::Relaxed);
        if let Ok(mut snapshot) = self.snapshot.write() {
            update(&mut snapshot);
            snapshot.events = events;
            snapshot.dropped_events = dropped_events;
            snapshot.updated_at = Utc::now();
        }
    }

    pub fn push_event(&mut self, level: impl Into<String>, message: impl Into<String>) {
        self.drain_global_log_events();
        self.push_event_snapshot(RuntimeEventSnapshot {
            created_at: Utc::now(),
            level: level.into(),
            message: message.into(),
        });
    }

    pub fn start_tui(&mut self, config: TuiConfig) {
        if !config.enabled || self.tui_handle.is_some() {
            return;
        }
        self.strategy_stop_requested.store(false, Ordering::Relaxed);
        self.tui_close_requested.store(false, Ordering::Relaxed);
        let handle = start_tui_thread(
            self.snapshot.clone(),
            self.strategy_stop_requested.clone(),
            self.tui_close_requested.clone(),
            config.render_interval,
        );
        if handle.is_some() {
            TERMINAL_OUTPUT_SUSPENDED.store(true, Ordering::Relaxed);
        }
        self.tui_handle = handle;
    }

    pub fn shutdown_requested(&self) -> bool {
        self.strategy_stop_requested.load(Ordering::Relaxed)
    }

    pub fn stop_tui(&mut self) {
        self.tui_close_requested.store(true, Ordering::Relaxed);
        if let Some(handle) = self.tui_handle.take() {
            let _ = handle.join();
        }
        TERMINAL_OUTPUT_SUSPENDED.store(false, Ordering::Relaxed);
    }

    pub fn wait_for_tui_close(&mut self) {
        if let Some(handle) = self.tui_handle.take() {
            let _ = handle.join();
        }
        TERMINAL_OUTPUT_SUSPENDED.store(false, Ordering::Relaxed);
    }
}

impl Drop for RuntimeTelemetry {
    fn drop(&mut self) {
        self.stop_tui();
    }
}

pub fn summarise_value(value: &Value) -> RuntimeVariableSnapshot {
    summarise_variable_value(String::new(), value, 0)
}

fn summarise_variable_value(key: String, value: &Value, depth: usize) -> RuntimeVariableSnapshot {
    match value {
        Value::Array(items) => summarise_variable_children(
            key,
            format!("[{} items]", items.len()),
            items.len(),
            depth,
            items
                .iter()
                .enumerate()
                .map(|(index, value)| (format!("[{}]", index), value)),
        ),
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            summarise_variable_children(
                key,
                format!("{{{} fields}}", map.len()),
                map.len(),
                depth,
                entries.into_iter().map(|(key, value)| (key.clone(), value)),
            )
        }
        _ => summarise_variable_scalar(key, value),
    }
}

fn summarise_variable_children<'a, I>(
    key: String,
    value: String,
    total_children: usize,
    depth: usize,
    children: I,
) -> RuntimeVariableSnapshot
where
    I: IntoIterator<Item = (String, &'a Value)>,
{
    let mut truncated = total_children > VARIABLE_CHILD_LIMIT || depth >= VARIABLE_DEPTH_LIMIT;
    let mut child_nodes = Vec::new();

    if depth < VARIABLE_DEPTH_LIMIT {
        for (child_key, child_value) in children.into_iter().take(VARIABLE_CHILD_LIMIT) {
            child_nodes.push(summarise_variable_value(child_key, child_value, depth + 1));
        }
        if total_children > VARIABLE_CHILD_LIMIT {
            child_nodes.push(RuntimeVariableSnapshot {
                key: "...".to_string(),
                value: format!("{} more entries", total_children - VARIABLE_CHILD_LIMIT),
                truncated: true,
                children: Vec::new(),
            });
        }
    }

    if total_children == 0 {
        truncated = false;
    }

    RuntimeVariableSnapshot {
        key,
        value,
        truncated,
        children: child_nodes,
    }
}

fn summarise_variable_scalar(key: String, value: &Value) -> RuntimeVariableSnapshot {
    let mut value_text = match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    };
    let truncated = value_text.chars().count() > VARIABLE_VALUE_LIMIT;
    if truncated {
        value_text = value_text
            .chars()
            .take(VARIABLE_VALUE_LIMIT)
            .collect::<String>();
        value_text.push_str("...");
    }
    RuntimeVariableSnapshot {
        key,
        value: value_text,
        truncated,
        children: Vec::new(),
    }
}

pub fn terminal_output_suspended() -> bool {
    TERMINAL_OUTPUT_SUSPENDED.load(Ordering::Relaxed)
}

fn global_tui_log_events() -> &'static Mutex<VecDeque<RuntimeEventSnapshot>> {
    GLOBAL_TUI_LOG_EVENTS.get_or_init(|| Mutex::new(VecDeque::with_capacity(GLOBAL_LOG_CAPACITY)))
}

fn push_global_log_line(line: String) {
    let line = line.trim_end_matches(['\r', '\n']).to_string();
    if line.trim().is_empty() {
        return;
    }
    push_global_tui_event("log", line);
}

fn push_global_tui_event(level: impl Into<String>, message: impl Into<String>) {
    if let Ok(mut events) = global_tui_log_events().lock() {
        if events.len() >= GLOBAL_LOG_CAPACITY {
            events.pop_front();
        }
        events.push_back(RuntimeEventSnapshot {
            created_at: Utc::now(),
            level: level.into(),
            message: message.into(),
        });
    }
}

fn drain_pending_global_log_events() -> Vec<RuntimeEventSnapshot> {
    global_tui_log_events()
        .lock()
        .map(|mut events| events.drain(..).collect())
        .unwrap_or_default()
}

#[cfg(feature = "tui")]
fn pending_global_log_events() -> Vec<RuntimeEventSnapshot> {
    global_tui_log_events()
        .lock()
        .map(|events| events.iter().cloned().collect())
        .unwrap_or_default()
}

#[derive(Default)]
struct TuiLogWriter {
    line_buffer: Vec<u8>,
}

impl TuiLogWriter {
    fn flush_line(&mut self) {
        if self.line_buffer.is_empty() {
            return;
        }
        let line = String::from_utf8_lossy(&self.line_buffer).to_string();
        self.line_buffer.clear();
        push_global_log_line(line);
    }
}

impl Write for TuiLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for byte in buf {
            if *byte == b'\n' {
                self.flush_line();
            } else {
                self.line_buffer.push(*byte);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_line();
        Ok(())
    }
}

pub fn tui_log_writer() -> Box<dyn Write + Send> {
    Box::new(TuiLogWriter::default())
}

#[cfg(feature = "tui")]
fn start_tui_thread(
    snapshot: Arc<RwLock<RuntimeSnapshot>>,
    strategy_stop_requested: Arc<AtomicBool>,
    tui_close_requested: Arc<AtomicBool>,
    render_interval: Duration,
) -> Option<JoinHandle<()>> {
    Some(std::thread::spawn(move || {
        if let Err(error) = run_tui(
            snapshot,
            strategy_stop_requested,
            tui_close_requested,
            render_interval,
        ) {
            TERMINAL_OUTPUT_SUSPENDED.store(false, Ordering::Relaxed);
            eprintln!("AQE TUI failed: {}", error);
        }
    }))
}

#[cfg(not(feature = "tui"))]
fn start_tui_thread(
    _snapshot: Arc<RwLock<RuntimeSnapshot>>,
    _strategy_stop_requested: Arc<AtomicBool>,
    _tui_close_requested: Arc<AtomicBool>,
    _render_interval: Duration,
) -> Option<JoinHandle<()>> {
    None
}

#[cfg(feature = "tui")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TuiFocusPane {
    Metrics,
    Insights,
    Variables,
    Watch,
    Events,
}

#[cfg(feature = "tui")]
impl TuiFocusPane {
    fn next(self) -> Self {
        match self {
            Self::Metrics => Self::Insights,
            Self::Insights => Self::Variables,
            Self::Variables => Self::Watch,
            Self::Watch => Self::Events,
            Self::Events => Self::Metrics,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Metrics => Self::Events,
            Self::Insights => Self::Metrics,
            Self::Variables => Self::Insights,
            Self::Watch => Self::Variables,
            Self::Events => Self::Watch,
        }
    }
}

#[cfg(feature = "tui")]
struct TuiUiState {
    focus: TuiFocusPane,
    metrics_scroll: usize,
    events_scroll: usize,
    insight_index: usize,
    variable_index: usize,
    variable_path: Vec<usize>,
    watch_index: usize,
    watch_paths: Vec<Vec<String>>,
}

#[cfg(feature = "tui")]
impl Default for TuiUiState {
    fn default() -> Self {
        Self {
            focus: TuiFocusPane::Metrics,
            metrics_scroll: 0,
            events_scroll: 0,
            insight_index: 0,
            variable_index: 0,
            variable_path: Vec::new(),
            watch_index: 0,
            watch_paths: Vec::new(),
        }
    }
}

#[cfg(feature = "tui")]
fn clamp_index(index: &mut usize, len: usize) {
    if len == 0 {
        *index = 0;
    } else if *index >= len {
        *index = len - 1;
    }
}

#[cfg(feature = "tui")]
fn move_index(index: &mut usize, len: usize, delta: isize) {
    if len == 0 {
        *index = 0;
    } else if delta < 0 {
        *index = index.saturating_sub(1);
    } else {
        *index = (*index + delta as usize).min(len - 1);
    }
}

#[cfg(feature = "tui")]
fn variable_scope<'a>(
    variables: &'a [RuntimeVariableSnapshot],
    path: &[usize],
) -> (&'a [RuntimeVariableSnapshot], String) {
    let mut scope = variables;
    let mut crumbs = Vec::new();
    for index in path {
        let Some(node) = scope.get(*index) else {
            break;
        };
        crumbs.push(node.key.clone());
        scope = &node.children;
    }
    let title = if crumbs.is_empty() {
        "Variables".to_string()
    } else {
        format!("Variables / {}", crumbs.join(" / "))
    };
    (scope, title)
}

#[cfg(feature = "tui")]
fn variable_key_path(
    variables: &[RuntimeVariableSnapshot],
    path: &[usize],
    selected_index: usize,
) -> Option<Vec<String>> {
    let mut scope = variables;
    let mut keys = Vec::new();
    for index in path {
        let node = scope.get(*index)?;
        keys.push(node.key.clone());
        scope = &node.children;
    }
    let node = scope.get(selected_index)?;
    keys.push(node.key.clone());
    Some(keys)
}

#[cfg(feature = "tui")]
fn find_variable_by_key_path<'a>(
    variables: &'a [RuntimeVariableSnapshot],
    path: &[String],
) -> Option<&'a RuntimeVariableSnapshot> {
    let mut scope = variables;
    let mut node = None;
    for key in path {
        let current = scope.iter().find(|variable| &variable.key == key)?;
        node = Some(current);
        scope = &current.children;
    }
    node
}

#[cfg(feature = "tui")]
fn format_variable_path(path: &[String]) -> String {
    let mut result = String::new();
    for key in path {
        if result.is_empty() || key.starts_with('[') {
            result.push_str(key);
        } else {
            result.push('.');
            result.push_str(key);
        }
    }
    result
}

#[cfg(feature = "tui")]
fn toggle_watch_path(watches: &mut Vec<Vec<String>>, path: Vec<String>) {
    if let Some(index) = watches.iter().position(|watch| watch == &path) {
        watches.remove(index);
    } else {
        watches.push(path);
        watches.sort();
    }
}

#[cfg(feature = "tui")]
fn watch_items<'a>(
    variables: &'a [RuntimeVariableSnapshot],
    watches: &'a [Vec<String>],
) -> Vec<ratatui::widgets::ListItem<'a>> {
    watches
        .iter()
        .map(|path| {
            let value = find_variable_by_key_path(variables, path)
                .map(|variable| variable.value.clone())
                .unwrap_or_else(|| "<missing>".to_string());
            ratatui::widgets::ListItem::new(format!("{}: {}", format_variable_path(path), value))
        })
        .collect()
}

#[cfg(feature = "tui")]
fn normalise_ui_state(state: &mut TuiUiState, snapshot: &RuntimeSnapshot) {
    clamp_index(&mut state.insight_index, snapshot.active_insights.len());
    clamp_index(&mut state.watch_index, state.watch_paths.len());

    let mut valid_path = Vec::new();
    let mut scope = snapshot.variables.as_slice();
    for index in &state.variable_path {
        let Some(node) = scope.get(*index) else {
            break;
        };
        if node.children.is_empty() {
            break;
        }
        valid_path.push(*index);
        scope = &node.children;
    }
    if valid_path.len() != state.variable_path.len() {
        state.variable_path = valid_path;
        state.variable_index = 0;
    }

    let (scope, _) = variable_scope(&snapshot.variables, &state.variable_path);
    clamp_index(&mut state.variable_index, scope.len());
    state.metrics_scroll = state.metrics_scroll.min(
        snapshot
            .metrics
            .as_ref()
            .map(|metrics| metrics.summary_lines.len().saturating_sub(1))
            .unwrap_or(0),
    );
    state.events_scroll = state
        .events_scroll
        .min(snapshot.events.len().saturating_sub(EVENTS_VISIBLE_ROWS));
}

#[cfg(feature = "tui")]
fn pane_title(title: impl AsRef<str>, pane: TuiFocusPane, focus: TuiFocusPane) -> String {
    if pane == focus {
        format!("> {}", title.as_ref())
    } else {
        title.as_ref().to_string()
    }
}

#[cfg(feature = "tui")]
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "Stopped" | "Completed")
}

#[cfg(feature = "tui")]
fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

#[cfg(feature = "tui")]
fn option_value<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(feature = "tui")]
fn levels_value(value: &Option<Vec<f64>>) -> String {
    value
        .as_ref()
        .filter(|levels| !levels.is_empty())
        .map(|levels| {
            levels
                .iter()
                .map(|level| format!("{level:.5}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(feature = "tui")]
fn time_value(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(feature = "tui")]
fn detail_line(label: impl Into<String>, value: impl Into<String>) -> ratatui::text::Line<'static> {
    use ratatui::{
        style::{Color, Style},
        text::{Line, Span},
    };

    Line::from(vec![
        Span::styled(
            format!("{}: ", label.into()),
            Style::default().fg(Color::Gray),
        ),
        Span::raw(value.into()),
    ])
}

#[cfg(feature = "tui")]
fn insight_detail_lines(
    insight: Option<&RuntimeInsightSnapshot>,
) -> Vec<ratatui::text::Line<'static>> {
    let Some(insight) = insight else {
        return vec![ratatui::text::Line::from(
            "Select an active insight to inspect it.",
        )];
    };

    let mut lines = vec![
        detail_line("ID", insight.insight_id.clone()),
        detail_line("Symbol", insight.symbol.clone()),
        detail_line("State", insight.state.clone()),
        detail_line("Side", insight.side.clone()),
        detail_line("Strategy", insight.strategy_type.clone()),
        detail_line("Order", option_value(insight.order_id.as_deref())),
        detail_line(
            "Close order",
            option_value(insight.close_order_id.as_deref()),
        ),
        detail_line(
            "Qty",
            option_value(insight.quantity.map(|value| format!("{value:.4}"))),
        ),
        detail_line(
            "Contracts",
            option_value(insight.contracts.map(|value| format!("{value:.4}"))),
        ),
        detail_line(
            "Type",
            format!("{} / {}", insight.order_type, insight.order_class),
        ),
        detail_line(
            "Limit/Stop",
            format!(
                "{} / {}",
                option_value(insight.limit_price.map(|value| format!("{value:.5}"))),
                option_value(insight.stop_price.map(|value| format!("{value:.5}")))
            ),
        ),
        detail_line("TP", levels_value(&insight.take_profit_levels)),
        detail_line("SL", levels_value(&insight.stop_loss_levels)),
        detail_line(
            "Filled/Close",
            format!(
                "{} / {}",
                option_value(insight.filled_price.map(|value| format!("{value:.5}"))),
                option_value(insight.close_price.map(|value| format!("{value:.5}")))
            ),
        ),
        detail_line("Confidence", insight.confidence.to_string()),
        detail_line("Updated", time_value(insight.updated_at)),
    ];

    if !insight.state_history.is_empty() {
        lines.push(ratatui::text::Line::from("State history"));
        for entry in &insight.state_history {
            let message = entry
                .message
                .as_deref()
                .filter(|message| !message.is_empty())
                .unwrap_or("-");
            lines.push(detail_line(
                entry.at.format("%H:%M:%S").to_string(),
                format!("{} {}", entry.state, message),
            ));
        }
    }

    lines
}

#[cfg(feature = "tui")]
fn run_tui(
    snapshot: Arc<RwLock<RuntimeSnapshot>>,
    strategy_stop_requested: Arc<AtomicBool>,
    tui_close_requested: Arc<AtomicBool>,
    render_interval: Duration,
) -> std::io::Result<()> {
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use ratatui::{
        layout::{Constraint, Direction, Layout},
        style::{Color, Modifier, Style},
        text::{Line, Span},
        widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph, Wrap},
    };

    let mut terminal = ratatui::init();
    let mut ui_state = TuiUiState::default();
    let result = (|| -> std::io::Result<()> {
        while !tui_close_requested.load(Ordering::Relaxed) {
            let current = snapshot
                .read()
                .map(|snapshot| snapshot.clone())
                .unwrap_or_default();
            normalise_ui_state(&mut ui_state, &current);

            terminal.draw(|frame| {
                let area = frame.area();
                let rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(5),
                        Constraint::Length(5),
                        Constraint::Min(10),
                        Constraint::Length(7),
                    ])
                    .split(area);

                let title = format!(
                    "AQE {} - {} [{}]",
                    current.mode.as_str(),
                    current.strategy_name,
                    current.status
                );
                let header = Paragraph::new(vec![
                    Line::from(vec![
                        Span::styled("Strategy: ", Style::default().fg(Color::Gray)),
                        Span::raw(current.strategy_id.clone()),
                    ]),
                    Line::from(vec![
                        Span::styled("Broker: ", Style::default().fg(Color::Gray)),
                        Span::raw(current.broker_status.clone()),
                        Span::raw("  "),
                        Span::styled("Data Feed: ", Style::default().fg(Color::Gray)),
                        Span::raw(current.datafeed_status.clone()),
                        Span::raw("  "),
                        Span::styled("AQS: ", Style::default().fg(Color::Gray)),
                        Span::raw(current.aqs_sync_status.clone()),
                    ]),
                    Line::from(
                        "Tab focus | m/i/v/e quick focus | w watch variable | Enter/Right open | Left/Backspace parent | q quit",
                    ),
                ])
                .block(Block::default().title(title).borders(Borders::ALL));
                frame.render_widget(header, rows[0]);

                let progress = current
                    .backtest_progress
                    .as_ref()
                    .map(|progress| progress.progress_pct.clamp(0.0, 100.0))
                    .unwrap_or(0.0);
                let progress_label = current
                    .backtest_progress
                    .as_ref()
                    .map(|progress| {
                        format!(
                            "{:.1}%  {}/{} steps  streams={}",
                            progress.progress_pct,
                            progress.processed_steps,
                            progress.total_steps,
                            progress.stream_count
                        )
                    })
                    .unwrap_or_else(|| "live run".to_string());
                let gauge = Gauge::default()
                    .block(Block::default().title("Progress").borders(Borders::ALL))
                    .gauge_style(Style::default().fg(Color::Cyan))
                    .percent(progress.round() as u16)
                    .label(progress_label);
                frame.render_widget(gauge, rows[1]);

                let columns = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Percentage(34),
                        Constraint::Percentage(33),
                        Constraint::Percentage(33),
                    ])
                    .split(rows[2]);

                let metrics = current
                    .metrics
                    .as_ref()
                    .map(|metrics| {
                        if metrics.summary_lines.is_empty() {
                            vec![
                                Line::from(format!("Equity: {:.2}", metrics.final_equity)),
                                Line::from(format!("Return: {:.2}%", metrics.total_return_pct)),
                                Line::from(format!("Trades: {}", metrics.total_trades)),
                                Line::from(format!(
                                    "Open positions: {}",
                                    metrics.open_positions_count
                                )),
                                Line::from(format!("Open insights: {}", metrics.open_insights_count)),
                            ]
                        } else {
                            metrics
                                .summary_lines
                                .iter()
                                .cloned()
                                .map(Line::from)
                                .collect()
                        }
                    })
                    .unwrap_or_else(|| vec![Line::from("No metrics yet")]);
                frame.render_widget(
                    Paragraph::new(metrics)
                        .scroll((ui_state.metrics_scroll as u16, 0))
                        .wrap(Wrap { trim: true })
                        .block(
                            Block::default()
                                .title(pane_title("Metrics", TuiFocusPane::Metrics, ui_state.focus))
                                .borders(Borders::ALL),
                        ),
                    columns[0],
                );

                let insight_rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
                    .split(columns[1]);
                let insight_items = current
                    .active_insights
                    .iter()
                    .map(|insight| {
                        ListItem::new(format!(
                            "{} {} {} {}",
                            insight.symbol,
                            insight.side,
                            insight.state,
                            short_id(&insight.insight_id)
                        ))
                    })
                    .collect::<Vec<_>>();
                let mut insight_list_state = ListState::default();
                if !current.active_insights.is_empty() {
                    insight_list_state.select(Some(ui_state.insight_index));
                }
                frame.render_stateful_widget(
                    List::new(insight_items)
                        .block(
                            Block::default()
                                .title(pane_title(
                                    "Active Insights",
                                    TuiFocusPane::Insights,
                                    ui_state.focus,
                                ))
                                .borders(Borders::ALL),
                        )
                        .highlight_style(
                            Style::default()
                                .bg(Color::Cyan)
                                .fg(Color::Black)
                                .add_modifier(Modifier::BOLD),
                        )
                        .highlight_symbol("> "),
                    insight_rows[0],
                    &mut insight_list_state,
                );
                let insight_detail =
                    insight_detail_lines(current.active_insights.get(ui_state.insight_index));
                frame.render_widget(
                    Paragraph::new(insight_detail)
                        .wrap(Wrap { trim: true })
                        .block(Block::default().title("Insight Detail").borders(Borders::ALL)),
                    insight_rows[1],
                );

                let variable_rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Percentage(64), Constraint::Percentage(36)])
                    .split(columns[2]);

                let (variable_scope, variable_title) =
                    variable_scope(&current.variables, &ui_state.variable_path);
                let variable_items = variable_scope
                    .iter()
                    .map(|variable| {
                        let marker = if variable.children.is_empty() {
                            "  "
                        } else {
                            "> "
                        };
                        let truncated = if variable.truncated { " *" } else { "" };
                        ListItem::new(format!(
                            "{}{}: {}{}",
                            marker, variable.key, variable.value, truncated
                        ))
                    })
                    .collect::<Vec<_>>();
                let mut variable_list_state = ListState::default();
                if !variable_scope.is_empty() {
                    variable_list_state.select(Some(ui_state.variable_index));
                }
                frame.render_stateful_widget(
                    List::new(variable_items)
                        .block(
                            Block::default()
                                .title(pane_title(
                                    variable_title,
                                    TuiFocusPane::Variables,
                                    ui_state.focus,
                                ))
                                .borders(Borders::ALL),
                        )
                        .highlight_style(
                            Style::default()
                                .bg(Color::Cyan)
                                .fg(Color::Black)
                                .add_modifier(Modifier::BOLD),
                        )
                        .highlight_symbol("> "),
                    variable_rows[0],
                    &mut variable_list_state,
                );

                let mut watch_list_state = ListState::default();
                if !ui_state.watch_paths.is_empty() {
                    watch_list_state.select(Some(ui_state.watch_index));
                }
                frame.render_stateful_widget(
                    List::new(watch_items(&current.variables, &ui_state.watch_paths))
                        .block(
                            Block::default()
                                .title(pane_title(
                                    "Watch",
                                    TuiFocusPane::Watch,
                                    ui_state.focus,
                                ))
                                .borders(Borders::ALL),
                        )
                        .highlight_style(
                            Style::default()
                                .bg(Color::Cyan)
                                .fg(Color::Black)
                                .add_modifier(Modifier::BOLD),
                        )
                        .highlight_symbol("> "),
                    variable_rows[1],
                    &mut watch_list_state,
                );

                let mut visible_events = current.events.clone();
                visible_events.extend(pending_global_log_events());
                let max_events_scroll = visible_events.len().saturating_sub(EVENTS_VISIBLE_ROWS);
                ui_state.events_scroll = ui_state.events_scroll.min(max_events_scroll);
                let log_items = visible_events
                    .iter()
                    .rev()
                    .skip(ui_state.events_scroll)
                    .take(EVENTS_VISIBLE_ROWS)
                    .map(|event| {
                        if event.level == "log" {
                            ListItem::new(Line::from(Span::raw(event.message.clone())))
                        } else {
                            ListItem::new(Line::from(vec![
                                Span::styled(
                                    format!("{} ", event.created_at.format("%H:%M:%S")),
                                    Style::default().fg(Color::DarkGray),
                                ),
                                Span::styled(
                                    format!("[{}] ", event.level),
                                    Style::default().add_modifier(Modifier::BOLD),
                                ),
                                Span::raw(event.message.clone()),
                            ]))
                        }
                    })
                    .collect::<Vec<_>>();
                frame.render_widget(
                    List::new(log_items).block(
                        Block::default()
                            .title(pane_title(
                                format!("Events (dropped={})", current.dropped_events),
                                TuiFocusPane::Events,
                                ui_state.focus,
                            ))
                            .borders(Borders::ALL),
                    ),
                    rows[3],
                );
            })?;

            if event::poll(render_interval)? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => {
                                if is_terminal_status(&current.status) {
                                    tui_close_requested.store(true, Ordering::Relaxed);
                                } else {
                                    let already_requested =
                                        strategy_stop_requested.swap(true, Ordering::Relaxed);
                                    if already_requested {
                                        push_global_tui_event(
                                            "warn",
                                            "Stop already requested; waiting for strategy teardown to finish. Press q again once the run has stopped to close the TUI.",
                                        );
                                    } else {
                                        push_global_tui_event(
                                            "warn",
                                            "Stop requested from TUI; waiting for strategy teardown. Press q again once the run has stopped to close the TUI.",
                                        );
                                    }
                                }
                            }
                            KeyCode::Tab => {
                                ui_state.focus = ui_state.focus.next();
                            }
                            KeyCode::BackTab => {
                                ui_state.focus = ui_state.focus.previous();
                            }
                            KeyCode::Char('m') => {
                                ui_state.focus = TuiFocusPane::Metrics;
                            }
                            KeyCode::Char('i') => {
                                ui_state.focus = TuiFocusPane::Insights;
                            }
                            KeyCode::Char('v') => {
                                ui_state.focus = TuiFocusPane::Variables;
                            }
                            KeyCode::Char('e') => {
                                ui_state.focus = TuiFocusPane::Events;
                            }
                            KeyCode::Char('w') if ui_state.focus == TuiFocusPane::Variables => {
                                if let Some(path) = variable_key_path(
                                    &current.variables,
                                    &ui_state.variable_path,
                                    ui_state.variable_index,
                                ) {
                                    toggle_watch_path(&mut ui_state.watch_paths, path);
                                    clamp_index(
                                        &mut ui_state.watch_index,
                                        ui_state.watch_paths.len(),
                                    );
                                }
                            }
                            KeyCode::Char('w') if ui_state.focus == TuiFocusPane::Watch => {
                                if !ui_state.watch_paths.is_empty() {
                                    ui_state.watch_paths.remove(ui_state.watch_index);
                                    clamp_index(
                                        &mut ui_state.watch_index,
                                        ui_state.watch_paths.len(),
                                    );
                                }
                            }
                            KeyCode::Up => match ui_state.focus {
                                TuiFocusPane::Metrics => {
                                    ui_state.metrics_scroll =
                                        ui_state.metrics_scroll.saturating_sub(1);
                                }
                                TuiFocusPane::Insights => move_index(
                                    &mut ui_state.insight_index,
                                    current.active_insights.len(),
                                    -1,
                                ),
                                TuiFocusPane::Variables => {
                                    let (scope, _) =
                                        variable_scope(&current.variables, &ui_state.variable_path);
                                    move_index(&mut ui_state.variable_index, scope.len(), -1);
                                }
                                TuiFocusPane::Watch => move_index(
                                    &mut ui_state.watch_index,
                                    ui_state.watch_paths.len(),
                                    -1,
                                ),
                                TuiFocusPane::Events => {
                                    ui_state.events_scroll =
                                        ui_state.events_scroll.saturating_sub(1);
                                }
                            },
                            KeyCode::Down => match ui_state.focus {
                                TuiFocusPane::Metrics => {
                                    let max_scroll = current
                                        .metrics
                                        .as_ref()
                                        .map(|metrics| {
                                            metrics.summary_lines.len().saturating_sub(1)
                                        })
                                        .unwrap_or(0);
                                    ui_state.metrics_scroll =
                                        (ui_state.metrics_scroll + 1).min(max_scroll);
                                }
                                TuiFocusPane::Insights => move_index(
                                    &mut ui_state.insight_index,
                                    current.active_insights.len(),
                                    1,
                                ),
                                TuiFocusPane::Variables => {
                                    let (scope, _) =
                                        variable_scope(&current.variables, &ui_state.variable_path);
                                    move_index(&mut ui_state.variable_index, scope.len(), 1);
                                }
                                TuiFocusPane::Watch => move_index(
                                    &mut ui_state.watch_index,
                                    ui_state.watch_paths.len(),
                                    1,
                                ),
                                TuiFocusPane::Events => {
                                    let event_count =
                                        current.events.len() + pending_global_log_events().len();
                                    let max_scroll =
                                        event_count.saturating_sub(EVENTS_VISIBLE_ROWS);
                                    ui_state.events_scroll =
                                        (ui_state.events_scroll + 1).min(max_scroll);
                                }
                            },
                            KeyCode::PageUp if ui_state.focus == TuiFocusPane::Events => {
                                ui_state.events_scroll =
                                    ui_state.events_scroll.saturating_sub(EVENTS_VISIBLE_ROWS);
                            }
                            KeyCode::PageDown if ui_state.focus == TuiFocusPane::Events => {
                                let event_count =
                                    current.events.len() + pending_global_log_events().len();
                                let max_scroll = event_count.saturating_sub(EVENTS_VISIBLE_ROWS);
                                ui_state.events_scroll =
                                    (ui_state.events_scroll + EVENTS_VISIBLE_ROWS).min(max_scroll);
                            }
                            KeyCode::Home if ui_state.focus == TuiFocusPane::Events => {
                                ui_state.events_scroll = 0;
                            }
                            KeyCode::End if ui_state.focus == TuiFocusPane::Events => {
                                let event_count =
                                    current.events.len() + pending_global_log_events().len();
                                ui_state.events_scroll =
                                    event_count.saturating_sub(EVENTS_VISIBLE_ROWS);
                            }
                            KeyCode::Enter | KeyCode::Right
                                if ui_state.focus == TuiFocusPane::Variables =>
                            {
                                let (scope, _) =
                                    variable_scope(&current.variables, &ui_state.variable_path);
                                if scope
                                    .get(ui_state.variable_index)
                                    .is_some_and(|node| !node.children.is_empty())
                                {
                                    ui_state.variable_path.push(ui_state.variable_index);
                                    ui_state.variable_index = 0;
                                }
                            }
                            KeyCode::Backspace | KeyCode::Left
                                if ui_state.focus == TuiFocusPane::Variables =>
                            {
                                if ui_state.variable_path.pop().is_some() {
                                    ui_state.variable_index = 0;
                                }
                            }
                            KeyCode::Backspace | KeyCode::Delete
                                if ui_state.focus == TuiFocusPane::Watch =>
                            {
                                if !ui_state.watch_paths.is_empty() {
                                    ui_state.watch_paths.remove(ui_state.watch_index);
                                    clamp_index(
                                        &mut ui_state.watch_index,
                                        ui_state.watch_paths.len(),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    })();
    ratatui::restore();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_tui_arg_disables_tui() {
        let config = TuiConfig::from_args(["strategy", "--no-tui", "--tui"]);
        assert!(!config.enabled);
        assert!(config.forced);
    }

    #[test]
    fn runtime_telemetry_tracks_dropped_events() {
        let mut telemetry = RuntimeTelemetry::default();
        telemetry.event_capacity = 2;
        telemetry.push_event("info", "one");
        telemetry.push_event("info", "two");
        telemetry.push_event("info", "three");
        telemetry.update_snapshot(|snapshot| {
            snapshot.strategy_name = "test".to_string();
        });
        let snapshot = telemetry.snapshot.read().unwrap().clone();
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.dropped_events, 1);
        assert_eq!(snapshot.events[0].message, "two");
    }

    #[test]
    fn summarises_large_values() {
        let summary = summarise_value(&Value::String("x".repeat(VARIABLE_VALUE_LIMIT + 10)));
        assert!(summary.truncated);
        assert!(summary.value.ends_with("..."));
    }

    #[test]
    fn summarises_nested_variables_for_browsing() {
        let summary = summarise_value(&serde_json::json!({
            "alpha": {
                "state": "ready",
                "scores": [1, 2, 3]
            }
        }));

        assert_eq!(summary.value, "{1 fields}");
        assert_eq!(summary.children[0].key, "alpha");
        assert_eq!(summary.children[0].value, "{2 fields}");
        assert_eq!(summary.children[0].children[0].key, "scores");
        assert_eq!(summary.children[0].children[0].value, "[3 items]");
    }

    #[cfg(feature = "tui")]
    #[test]
    fn terminal_status_allows_tui_close() {
        assert!(is_terminal_status("Stopped"));
        assert!(is_terminal_status("Completed"));
        assert!(!is_terminal_status("Running"));
        assert!(!is_terminal_status("Stopping"));
        assert!(!is_terminal_status("Initialised"));
    }

    #[cfg(feature = "tui")]
    #[test]
    fn watch_paths_resolve_nested_variables_by_key() {
        let summary = summarise_value(&serde_json::json!({
            "alpha": {
                "state": "ready",
                "scores": [1, 2, 3]
            }
        }));
        let variables = summary.children;
        let path = vec!["alpha".to_string(), "scores".to_string(), "[1]".to_string()];

        let watched = find_variable_by_key_path(&variables, &path).unwrap();
        assert_eq!(format_variable_path(&path), "alpha.scores[1]");
        assert_eq!(watched.value, "2");
    }
}
