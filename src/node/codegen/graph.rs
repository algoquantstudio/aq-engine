use crate::node::{Connection, Node, NodeType};
use std::collections::{HashMap, VecDeque};

/// Find all nodes reachable from the Strategy root node.
pub(super) fn get_reachable_nodes(
    nodes: &[Node],
    connections: &[Connection],
) -> std::collections::HashSet<String> {
    let mut reachable = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();

    // Find strategy root
    if let Some(root) = nodes.iter().find(|n| n.node_type == NodeType::Strategy) {
        reachable.insert(root.id.clone());
        queue.push_back(root.id.clone());
    }

    // Normal outward adjacency list.
    let mut adj: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    // Reverse adjacency for nested pipe composition (`child_pipe -> parent_pipe.insights_pipes`).
    let mut reverse_pipe_children: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for conn in connections {
        adj.entry(conn.from.node_id.clone())
            .or_default()
            .push(conn.to.node_id.clone());

        if conn.to.port == "insights_pipes" {
            reverse_pipe_children
                .entry(conn.to.node_id.clone())
                .or_default()
                .push(conn.from.node_id.clone());
        }
    }

    while let Some(curr) = queue.pop_front() {
        if let Some(neighbors) = adj.get(&curr) {
            for next in neighbors {
                if reachable.insert(next.clone()) {
                    queue.push_back(next.clone());
                }
            }
        }

        if let Some(children) = reverse_pipe_children.get(&curr) {
            for child in children {
                if reachable.insert(child.clone()) {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    reachable
}

/// Topological sort of the node graph. Returns node IDs in execution order.
pub(super) fn toposort(nodes: &[Node], connections: &[Connection]) -> Result<Vec<String>, String> {
    let mut in_degrees: HashMap<String, usize> = HashMap::new();
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();

    for node in nodes {
        in_degrees.insert(node.id.clone(), 0);
        adj.insert(node.id.clone(), vec![]);
    }

    for conn in connections {
        if !in_degrees.contains_key(&conn.to.node_id) {
            continue;
        }
        *in_degrees.entry(conn.to.node_id.clone()).or_insert(0) += 1;
        adj.entry(conn.from.node_id.clone())
            .or_default()
            .push(conn.to.node_id.clone());
    }

    let mut queue = VecDeque::new();
    for node in nodes {
        if in_degrees.get(&node.id).copied().unwrap_or_default() == 0 {
            queue.push_back(node.id.clone());
        }
    }

    let mut sorted = Vec::new();
    while let Some(curr) = queue.pop_front() {
        sorted.push(curr.clone());
        if let Some(neighbors) = adj.get(&curr) {
            for next in neighbors {
                if let Some(deg) = in_degrees.get_mut(next) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(next.clone());
                    }
                }
            }
        }
    }

    if sorted.len() != nodes.len() {
        return Err("Cycle detected in strategy graph".to_string());
    }

    Ok(sorted)
}
