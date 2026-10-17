use super::*;
use ratatui::{Terminal, backend::TestBackend};

fn fixture() -> RuntimeSnapshot {
    RuntimeSnapshot {
        mode: StrategyMode::Live,
        strategy_name: "Three Amigos preview".into(),
        status: "Running".into(),
        broker_status: "connected".into(),
        datafeed_status: "connected".into(),
        insight_counts: [("Filled".into(), 140)].into(),
        active_insights: vec![RuntimeInsightSnapshot {
            insight_id: "selected-insight".into(),
            symbol: "BTCUSD".into(),
            state: "Filled".into(),
            side: "Buy".into(),
            parent_id: Some("parent-insight".into()),
            state_history: vec![RuntimeInsightStateSnapshot {
                at: Utc::now(),
                state: "Filled".into(),
                kind: "StateTransition".into(),
                message: Some("Broker confirmed fill".into()),
            }],
            ..Default::default()
        }],
        sync: Some(RuntimeSyncSnapshot {
            transport: "Embedded hosted gateway".into(),
            runtime_id: "runtime-test".into(),
            run_id: "run-test".into(),
            publisher: Default::default(),
            sampled_at: Utc::now(),
        }),
        ..Default::default()
    }
}

fn render(snapshot: &RuntimeSnapshot, state: &mut TuiUiState, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| render_dashboard(frame, snapshot, state))
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

#[test]
fn live_and_backtest_layouts_render_at_small_and_large_sizes() {
    let mut snapshot = fixture();
    for mode in [StrategyMode::Live, StrategyMode::Backtest] {
        snapshot.mode = mode;
        for (width, height) in [(1, 1), (40, 12), (80, 24), (160, 48)] {
            for focus in [
                TuiFocusPane::Metrics,
                TuiFocusPane::Insights,
                TuiFocusPane::Inspector,
                TuiFocusPane::Variables,
                TuiFocusPane::Watch,
                TuiFocusPane::Events,
            ] {
                let mut state = TuiUiState {
                    focus,
                    ..Default::default()
                };
                render(&snapshot, &mut state, width, height);
                state.diagnostics = true;
                render(&snapshot, &mut state, width, height);
            }
        }
    }
}

#[test]
fn live_header_replaces_backtest_gauge_and_shows_preview_total() {
    let text = render(&fixture(), &mut TuiUiState::default(), 180, 50);
    assert!(text.contains("Snapshot age:"));
    assert!(text.contains("Active Insights 1/140"));
    assert!(!text.contains("Progress"));
}

#[test]
fn diagnostics_do_not_conflate_local_ack_with_remote_commit() {
    let text = render(
        &fixture(),
        &mut TuiUiState {
            diagnostics: true,
            ..Default::default()
        },
        140,
        40,
    );
    assert!(text.contains("run-test"));
    assert!(text.contains("does not confirm cloud persistence"));
    assert!(text.contains("reconciliation: not observed"));
}

#[test]
fn inspector_can_reach_history_and_scroll_is_clamped() {
    let mut state = TuiUiState {
        focus: TuiFocusPane::Inspector,
        inspector_scroll: 4096,
        ..Default::default()
    };
    let text = render(&fixture(), &mut state, 80, 24);
    assert!(text.contains("Broker confirmed fill"));
    assert!(state.inspector_scroll < 100);
}

#[test]
fn selected_id_survives_reordering_and_resets_when_removed() {
    let mut snapshot = fixture();
    let mut state = TuiUiState::default();
    normalise_ui_state(&mut state, &snapshot);
    state.inspector_scroll = 3;
    snapshot.active_insights.insert(
        0,
        RuntimeInsightSnapshot {
            insight_id: "another".into(),
            ..Default::default()
        },
    );
    normalise_ui_state(&mut state, &snapshot);
    assert_eq!(state.insight_index, 1);
    assert_eq!(state.inspector_scroll, 3);
    snapshot.active_insights.pop();
    normalise_ui_state(&mut state, &snapshot);
    assert_eq!(state.selected_insight_id.as_deref(), Some("another"));
    assert_eq!(state.inspector_scroll, 0);
}

#[test]
fn long_unterminated_logs_are_bounded_and_unicode_safe() {
    let mut writer = TuiLogWriter::default();
    writer.write_all("💹".repeat(100_000).as_bytes()).unwrap();
    assert!(writer.line_buffer.len() <= LOG_LINE_LIMIT);
    assert!(writer.truncated);
    let text = bounded_text(&"💹".repeat(10_000));
    assert!(text.len() < LOG_LINE_LIMIT + 40);
    assert!(text.ends_with("[truncated]"));
    assert_eq!(bounded_text("\u{1b}[31mError\u{1b}[0m"), "Error");
    assert_eq!(bounded_text("\u{1b}]0;unsafe title\u{7}Safe"), "Safe");
}

#[test]
fn nested_variables_have_a_total_node_budget() {
    fn count(node: &RuntimeVariableSnapshot) -> usize {
        1 + node.children.iter().map(count).sum::<usize>()
    }
    let value = serde_json::json!(vec![vec![vec!["large value"; 64]; 64]; 4]);
    let summary = summarise_value(&value);
    assert!(count(&summary) <= VARIABLE_NODE_LIMIT);
    assert!(summary.truncated);
}
