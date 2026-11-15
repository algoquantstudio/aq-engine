use super::*;
use crate::node::ConnectionEndpoint;
use std::collections::{BTreeSet, HashSet, VecDeque};

fn node(id: &str, kind: NodeType, label: &str) -> Node {
    let mut node = StrategyMeta::create_strategy_node(label);
    node.id = id.into();
    node.node_type = kind;
    node.label = label.into();
    node.inputs.clear();
    node.outputs.clear();
    node.source_file = Some(format!("src/{id}.rs"));
    node
}

fn edge(from: &str, to: &str, port: &str) -> Connection {
    Connection {
        from: ConnectionEndpoint {
            node_id: from.into(),
            port: "out".into(),
        },
        to: ConnectionEndpoint {
            node_id: to.into(),
            port: port.into(),
        },
    }
}

// Reference of the previous owned traversal, deliberately kept independent of ReachableGraph.
fn reference_sort(nodes: &[Node], edges: &[Connection]) -> Result<Vec<String>, String> {
    let mut reachable = HashSet::new();
    if let Some(root) = nodes
        .iter()
        .find(|node| node.node_type == NodeType::Strategy)
    {
        reachable.insert(root.id.clone());
    }
    loop {
        let before = reachable.len();
        for conn in edges {
            if reachable.contains(&conn.from.node_id) {
                reachable.insert(conn.to.node_id.clone());
            }
            if conn.to.port == "insights_pipes" && reachable.contains(&conn.to.node_id) {
                reachable.insert(conn.from.node_id.clone());
            }
        }
        if reachable.len() == before {
            break;
        }
    }
    let nodes: Vec<_> = nodes
        .iter()
        .filter(|node| reachable.contains(&node.id))
        .cloned()
        .collect();
    let edges: Vec<_> = edges
        .iter()
        .filter(|conn| {
            reachable.contains(&conn.from.node_id) && reachable.contains(&conn.to.node_id)
        })
        .cloned()
        .collect();
    let mut degrees: HashMap<String, usize> =
        nodes.iter().map(|node| (node.id.clone(), 0)).collect();
    let mut next: HashMap<String, Vec<String>> = HashMap::new();
    for conn in &edges {
        if let Some(degree) = degrees.get_mut(&conn.to.node_id) {
            *degree += 1;
            next.entry(conn.from.node_id.clone())
                .or_default()
                .push(conn.to.node_id.clone());
        }
    }
    let mut queue: VecDeque<_> = nodes
        .iter()
        .filter(|node| degrees[&node.id] == 0)
        .map(|node| node.id.clone())
        .collect();
    let mut sorted = Vec::new();
    while let Some(id) = queue.pop_front() {
        sorted.push(id.clone());
        for next_id in next.get(&id).into_iter().flatten() {
            let degree = degrees.get_mut(next_id).unwrap();
            *degree -= 1;
            if *degree == 0 {
                queue.push_back(next_id.clone());
            }
        }
    }
    if sorted.len() != nodes.len() {
        return Err("Cycle detected in strategy graph".into());
    }
    Ok(sorted)
}

#[test]
fn indexed_codegen_graph_matches_reference_traversal_and_cycle_errors() {
    let mut nodes = vec![node("root", NodeType::Strategy, "Root")];
    for id in ["a", "b", "c", "orphan"] {
        nodes.push(node(id, NodeType::Pipe, id));
    }
    let possibilities = [
        edge("root", "a", "in"),
        edge("root", "b", "in"),
        edge("a", "c", "in"),
        edge("b", "c", "in"),
        edge("c", "a", "in"),
        edge("c", "b", "insights_pipes"),
        edge("root", "missing", "in"),
        edge("missing", "b", "in"),
        edge("a", "c", "in"),
        edge("orphan", "orphan", "in"),
    ];
    // Covers duplicate edges, diamonds, reachable/disconnected cycles, dangling endpoints,
    // reverse compositions and different saved node/edge orders.
    for mask in 0..1usize << possibilities.len() {
        let mut edges: Vec<_> = possibilities
            .iter()
            .enumerate()
            .filter(|(bit, _)| mask & (1 << bit) != 0)
            .map(|(_, edge)| edge.clone())
            .collect();
        if mask % 2 == 0 {
            edges.reverse();
        }
        nodes[1..].rotate_left(1);
        let graph = ReachableGraph::new(&nodes, &edges);
        assert_eq!(
            graph
                .toposort()
                .map(|nodes| nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>()),
            reference_sort(&nodes, &edges),
            "edge subset {mask}"
        );
    }
    nodes.retain(|node| node.node_type != NodeType::Strategy);
    assert!(
        ReachableGraph::new(&nodes, &possibilities)
            .toposort()
            .unwrap()
            .is_empty()
    );
}

fn reference_scopes<'a>(
    direct: impl IntoIterator<Item = (&'a str, &'a str)>,
    successors: &HashMap<&'a str, Vec<&'a str>>,
) -> HashMap<&'a str, BTreeSet<&'a str>> {
    let mut allowed: HashMap<&str, Vec<&str>> = HashMap::new();
    for (pipe, alpha) in direct {
        allowed.entry(pipe).or_default().push(alpha);
    }
    loop {
        let mut changed = false;
        let pipes: HashSet<_> = allowed
            .keys()
            .copied()
            .chain(successors.keys().copied())
            .collect();
        for pipe in pipes {
            let Some(parent) = allowed.get(pipe).cloned() else {
                continue;
            };
            for next in successors.get(pipe).into_iter().flatten() {
                let entry = allowed.entry(*next).or_default();
                for alpha in &parent {
                    if !entry.contains(alpha) {
                        entry.push(alpha);
                        changed = true;
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }
    allowed
        .into_iter()
        .map(|(pipe, alphas)| (pipe, alphas.into_iter().collect()))
        .collect()
}

#[test]
fn queued_codegen_scopes_match_fixed_point_for_merges_cycles_and_duplicate_edges() {
    let direct = [("a", "Zeta"), ("b", "Alpha"), ("a", "Zeta")];
    let edges = [
        ("a", "b"),
        ("b", "c"),
        ("c", "a"),
        ("a", "d"),
        ("b", "d"),
        ("d", "d"),
        ("d", "e"),
        ("c", "e"),
        ("a", "b"),
    ];
    for mask in 0..1usize << edges.len() {
        let mut successors: HashMap<&str, Vec<&str>> = HashMap::new();
        for (bit, (from, to)) in edges.iter().enumerate() {
            if mask & (1 << bit) != 0 {
                successors.entry(from).or_default().push(to);
            }
        }
        let expected = reference_scopes(direct, &successors);
        assert_eq!(inherit_alpha_scopes(direct, &successors), expected);
        assert_eq!(
            inherit_alpha_scopes(direct.into_iter().rev(), &successors),
            expected
        );
    }
}

fn composed_meta() -> StrategyMeta {
    let mut meta = StrategyMeta::new("Scoped Composition");
    meta.nodes.push(node("root", NodeType::Strategy, "Root"));
    meta.nodes.push(node("alpha-z", NodeType::Alpha, "Zeta"));
    meta.nodes.push(node("alpha-a", NodeType::Alpha, "Alpha"));
    for id in [
        "left", "right", "parent", "child-a", "child-b", "tail", "free",
    ] {
        meta.nodes.push(node(id, NodeType::Pipe, "Custom Pipe"));
    }
    meta.connections = vec![
        edge("root", "alpha-z", "on_bar"),
        edge("root", "alpha-a", "on_bar"),
        edge("alpha-z", "left", "in"),
        edge("alpha-a", "right", "in"),
        edge("left", "parent", "in"),
        edge("right", "parent", "in"),
        edge("child-b", "parent", "insights_pipes"),
        edge("child-a", "parent", "insights_pipes"),
        // Acyclic construction edges can still induce a cycle in scope propagation.
        edge("child-b", "parent", "in"),
        edge("parent", "tail", "in"),
        edge("root", "free", "in"),
    ];
    meta
}

#[test]
fn generated_codegen_source_is_stable_and_preserves_custom_composition_order() {
    let meta = composed_meta();
    let source = generate_main_rs(&meta).unwrap();
    let canonical =
        ".allowed_alphas(vec![\"Alpha\".to_string(), \"Zeta\".to_string()].into_iter().collect())";
    for id in ["parent", "child_a", "child_b", "tail"] {
        let line = source
            .lines()
            .find(|line| line.contains(&format!("let pipe_{id} =")))
            .unwrap();
        assert!(line.contains(canonical), "{line}");
    }
    assert!(source.contains("CustomPipe::new(vec![pipe_child_b, pipe_child_a])"));
    assert!(!source.contains("ctx.add_pipe(pipe_child_a)"));
    assert!(!source.contains("ctx.add_pipe(pipe_child_b)"));
    assert!(source.find("let pipe_child_b =").unwrap() < source.find("let pipe_parent =").unwrap());
    assert!(
        !source
            .lines()
            .find(|line| line.contains("let pipe_free ="))
            .unwrap()
            .contains("allowed_alphas")
    );
    for _ in 0..32 {
        assert_eq!(generate_main_rs(&meta).unwrap(), source);
    }

    let mut duplicate = meta.clone();
    duplicate.connections.push(edge("alpha-z", "left", "in"));
    assert_eq!(generate_main_rs(&duplicate).unwrap(), source);
    let mut disconnected = meta;
    disconnected
        .nodes
        .push(node("unreachable", NodeType::Pipe, "Unused"));
    disconnected
        .connections
        .push(edge("unreachable", "unreachable", "in"));
    assert_eq!(generate_main_rs(&disconnected).unwrap(), source);
}

#[test]
#[ignore = "manual paired alpha-scope scaling benchmark; run with --ignored --nocapture"]
fn codegen_scope_scaling_benchmark() {
    use std::{hint::black_box, time::Instant};
    for count in [64, 256, 1024] {
        let names: Vec<_> = (0..count).map(|n| format!("pipe-{n}")).collect();
        let mut successors: HashMap<&str, Vec<&str>> = HashMap::new();
        for pair in names.windows(2) {
            successors
                .entry(pair[0].as_str())
                .or_default()
                .push(pair[1].as_str());
        }
        let direct = [
            (names[0].as_str(), "Zeta"),
            (names[count / 2].as_str(), "Alpha"),
        ];
        let expected = reference_scopes(direct, &successors);
        assert_eq!(inherit_alpha_scopes(direct, &successors), expected);
        let mut reference_ns = Vec::new();
        let mut indexed_ns = Vec::new();
        for sample in 0..5 {
            let mut measure_reference = || {
                let start = Instant::now();
                black_box(reference_scopes(black_box(direct), black_box(&successors)));
                reference_ns.push(start.elapsed().as_nanos());
            };
            let mut measure_indexed = || {
                let start = Instant::now();
                black_box(inherit_alpha_scopes(
                    black_box(direct),
                    black_box(&successors),
                ));
                indexed_ns.push(start.elapsed().as_nanos());
            };
            if sample % 2 == 0 {
                measure_reference();
                measure_indexed();
            } else {
                measure_indexed();
                measure_reference();
            }
        }
        reference_ns.sort_unstable();
        indexed_ns.sort_unstable();
        eprintln!(
            "codegen alpha scope chain pipes={count} reference_median_ns={} queue_median_ns={} speedup={:.2}",
            reference_ns[2],
            indexed_ns[2],
            reference_ns[2] as f64 / indexed_ns[2] as f64
        );
    }
}
