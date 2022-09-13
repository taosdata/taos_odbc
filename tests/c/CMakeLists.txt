add_executable(api_test api_test.c)

target_link_libraries(api_test odbc dl taos)
add_dependencies(api_test taos_odbc)

add_test(NAME api_test
    COMMAND api_test)

add_test(NAME Vapi_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/api_test)

