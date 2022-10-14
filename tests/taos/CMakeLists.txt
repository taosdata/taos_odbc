add_executable(taos_test taos_test.c)
add_dependencies(taos_test ex_cjson)

target_include_directories(taos_test PRIVATE ${CJSON_INSTALL_PATH}/include)
target_link_directories(taos_test PRIVATE ${CJSON_INSTALL_PATH}/lib)
# NOTE: cJSON_xxx exported in `libtaos.so`
target_link_libraries(taos_test cjson taos)

add_test(NAME taos_test
    COMMAND taos_test)

add_test(NAME Vtaos_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/taos_test)

