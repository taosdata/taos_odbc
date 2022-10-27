add_executable(api_test
    $<TARGET_OBJECTS:common_obj>
    api_test.c)

target_link_libraries(api_test odbc dl)

add_test(NAME api_test
    COMMAND api_test)

add_test(NAME Vapi_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/api_test)

add_executable(edge_cases_test edge_cases_test.c)

target_link_libraries(edge_cases_test odbc)

add_test(NAME edge_cases_test
    COMMAND edge_cases_test)

add_test(NAME Vedge_cases_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/edge_cases_test)

add_executable(odbc_test
    $<TARGET_OBJECTS:common_obj>
    odbc_test.c)
add_dependencies(odbc_test ex_cjson)

target_include_directories(odbc_test PRIVATE ${CJSON_INSTALL_PATH}/include)
target_link_directories(odbc_test PRIVATE ${CJSON_INSTALL_PATH}/lib)
target_link_libraries(odbc_test odbc cjson)

add_test(NAME odbc_test
    COMMAND odbc_test)

add_test(NAME Vodbc_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/odbc_test)

if (ENABLE_MYSQL_TEST)
  add_test(NAME mysql_odbc_test
      COMMAND odbc_test --dsn MYSQL_ODBC_DSN)

  add_test(NAME Vmysql_odbc_test
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/odbc_test --dsn MYSQL_ODBC_DSN)
endif ()

