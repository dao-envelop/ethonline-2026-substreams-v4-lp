fn main() {
    // One binding module per contract. The manager ABI is taken from VolatileLPManager because it is the
    // widest: it carries everything BaseLPManager and SingletonNFTOwned declare plus its own events, so
    // the same bindings decode logs from the stable and open-volatile products too.
    for (name, abi, out) in [
        ("LpManager", "abi/lp_manager.json", "src/abi/lp_manager.rs"),
        ("LpFactory", "abi/lp_factory.json", "src/abi/lp_factory.rs"),
        ("PoolManager", "abi/pool_manager.json", "src/abi/pool_manager.rs"),
    ] {
        substreams_ethereum::Abigen::new(name, abi)
            .unwrap_or_else(|e| panic!("load {abi}: {e}"))
            .generate()
            .unwrap_or_else(|e| panic!("generate {name}: {e}"))
            .write_to_file(out)
            .unwrap_or_else(|e| panic!("write {out}: {e}"));
    }

    prost_build::compile_protos(
        &[
            "proto/envelop/lp/v1/lp.proto",
            // Entity changes for graph_out. Generated here rather than taken from the
            // `substreams-entity-change` crate — see the header of that file for why.
            "proto/sf/substreams/sink/entity/v1/entity.proto",
        ],
        &["proto/"],
    )
    .unwrap();
}
