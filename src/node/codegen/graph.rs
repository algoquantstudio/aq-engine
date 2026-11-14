use crate::node::{Connection, Node, NodeType};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// Borrow the reachable graph and index node IDs once for all code generation passes.
/// Node/connection order is significant for registration and composed pipe arguments.
pub(super) struct ReachableGraph<'a> {
    pub nodes: Vec<&'a Node>,
    pub connections: Vec<&'a Connection>,
    pub by_id: HashMap<&'a str, &'a Node>,
}

impl<'a> ReachableGraph<'a> {
    pub fn new(nodes: &'a [Node], connections: &'a [Connection]) -> Self {
        let reachable = get_reachable_nodes(nodes, connections);
        let nodes: Vec<_> = nodes
            .iter()
            .filter(|node| reachable.contains(node.id.as_str()))
            .collect();
        let connections = connections
            .iter()
            .filter(|conn| {
                reachable.contains(conn.from.node_id.as_str())
                    && reachable.contains(conn.to.node_id.as_str())
            })
            .collect();
        let mut by_id = HashMap::with_capacity(nodes.len());
        for node in &nodes {
            // Match the previous linear lookup's first occurrence for duplicate IDs.
            by_id.entry(node.id.as_str()).or_insert(*node);
        }
        Self {
            nodes,
            connections,
            by_id,
        }
    }

    /// Stable Kahn traversal: roots follow saved node order, successors follow edge order.
    pub fn toposort(&self) -> Result<Vec<&'a Node>, String> {
        let mut in_degrees: HashMap<&str, usize> = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), 0))
            .collect();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for conn in &self.connections {
            let Some(degree) = in_degrees.get_mut(conn.to.node_id.as_str()) else {
                continue;
            };
            *degree += 1;
            adj.entry(conn.from.node_id.as_str())
                .or_default()
                .push(conn.to.node_id.as_str());
        }

        let mut queue = VecDeque::new();
        for node in &self.nodes {
            if in_degrees[node.id.as_str()] == 0 {
                queue.push_back(node.id.as_str());
            }
        }
        let mut sorted = Vec::with_capacity(self.nodes.len());
        while let Some(curr) = queue.pop_front() {
            sorted.push(self.by_id[curr]);
            if let Some(neighbors) = adj.get(curr) {
                for next in neighbors {
                    let degree = in_degrees.get_mut(next).expect("indexed edge target");
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(*next);
                    }
                }
            }
        }
        if sorted.len() != self.nodes.len() {
            return Err("Cycle detected in strategy graph".to_string());
        }
        Ok(sorted)
    }
}

/// Include outward paths and reverse composition edges, even through dangling endpoints,
/// preserving the existing reachability and malformed-graph error behavior.
fn get_reachable_nodes<'a>(nodes: &'a [Node], connections: &'a [Connection]) -> HashSet<&'a str> {
    let mut reachable = HashSet::new();
    let mut queue = VecDeque::new();
    if let Some(root) = nodes
        .iter()
        .find(|node| node.node_type == NodeType::Strategy)
    {
        reachable.insert(root.id.as_str());
        queue.push_back(root.id.as_str());
    }
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for conn in connections {
        adj.entry(conn.from.node_id.as_str())
            .or_default()
            .push(conn.to.node_id.as_str());
        if conn.to.port == "insights_pipes" {
            adj.entry(conn.to.node_id.as_str())
                .or_default()
                .push(conn.from.node_id.as_str());
        }
    }
    while let Some(curr) = queue.pop_front() {
        if let Some(neighbors) = adj.get(curr) {
            for next in neighbors {
                if reachable.insert(*next) {
                    queue.push_back(*next);
                }
            }
        }
    }
    reachable
}

/// Propagate each new (pipe, alpha) pair once, including cycles induced by reversed
/// composition edges. Sorted sets make source independent of hash iteration order.
pub(super) fn inherit_alpha_scopes<'a>(
    direct: impl IntoIterator<Item = (&'a str, &'a str)>,
    successors: &HashMap<&'a str, Vec<&'a str>>,
) -> HashMap<&'a str, BTreeSet<&'a str>> {
    let mut allowed: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    let mut queue = VecDeque::new();
    for (pipe, alpha) in direct {
        if allowed.entry(pipe).or_default().insert(alpha) {
            queue.push_back((pipe, alpha));
        }
    }
    while let Some((pipe, alpha)) = queue.pop_front() {
        if let Some(next_pipes) = successors.get(pipe) {
            for next in next_pipes {
                if allowed.entry(*next).or_default().insert(alpha) {
                    queue.push_back((*next, alpha));
                }
            }
        }
    }
    allowed
}
