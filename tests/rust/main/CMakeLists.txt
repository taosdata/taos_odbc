add_test(NAME rust_test
  COMMAND env RUST_BACKTRACE=1 cargo run --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR})

