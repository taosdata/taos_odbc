add_custom_target(rust_test ALL
    COMMAND env RUST_BACKTRACE=1 cargo build --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR}
    VERBATIM)

add_test(NAME rust_test
    COMMAND env RUST_BACKTRACE=1 cargo run --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR})

add_test(NAME cargo_test
    COMMAND env RUST_BACKTRACE=1 cargo test --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR})

add_test(NAME Vrust_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/debug/main)

