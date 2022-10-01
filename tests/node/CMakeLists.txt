configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/node_test.js"
    "${CMAKE_CURRENT_BINARY_DIR}/node_test.js"
    @ONLY)

add_test(NAME node_test
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/node_test.js)

add_test(NAME Vnode_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh node ${CMAKE_CURRENT_BINARY_DIR}/node_test.js)

