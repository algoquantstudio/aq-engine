#[test]
fn test_universe_parse() {
    let json = std::fs::read_to_string("/Users/moustaphadiaby/TradingStrategies/aqs1/Test1.aqmeta").unwrap();
    let meta: crate::node::StrategyMeta = serde_json::from_str(&json).unwrap();
    let sorted = crate::node::codegen::toposort(&meta.nodes, &meta.connections).unwrap();
    println!("SORTED NODES: {:?}", sorted);
    for node_id in &sorted {
        let node = meta.nodes.iter().find(|n| n.id == *node_id).unwrap();
        if node.node_type == crate::node::NodeType::Universe {
            println!("FOUND UNIVERSE {:?}", node.inputs);
        }
    }
}
