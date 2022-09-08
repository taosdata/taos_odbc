configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/node_test.js"
    "${CMAKE_CURRENT_BINARY_DIR}/node_test.js"
    @ONLY)

add_test(NAME node_test
    COMMAND node_test.js)

