add_executable(basic basic.c)

target_link_libraries(basic taos_odbc_a)
target_include_directories(basic PRIVATE
    ${CMAKE_SOURCE_DIR}/inc)

add_dependencies(basic taos_odbc_a)

add_test(NAME basic
    COMMAND basic)

add_test(NAME Vbasic
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/basic)

