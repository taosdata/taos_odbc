include(ExternalProject)
ExternalProject_Add(cjson
    GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
    GIT_TAG v1.7.15
    GIT_SHALLOW TRUE
    PREFIX "$ENV{HOME}/.taos_odbc/cjson"
    CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=$ENV{HOME}/.taos_odbc/.install"
    )

add_subdirectory(parser)
add_subdirectory(utils)

list(APPEND odbc_SOURCES desc.c)
list(APPEND odbc_SOURCES enums.c)
list(APPEND odbc_SOURCES env.c)
list(APPEND odbc_SOURCES errs.c)
list(APPEND odbc_SOURCES conn.c)
list(APPEND odbc_SOURCES odbc.c)
list(APPEND odbc_SOURCES stmt.c)

add_library(odbc_obj OBJECT ${odbc_SOURCES})
target_include_directories(odbc_obj PRIVATE
    ${CMAKE_SOURCE_DIR}/inc)
set_property(TARGET odbc_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(taos_odbc_a STATIC
    $<TARGET_OBJECTS:odbc_obj>
    $<TARGET_OBJECTS:utils_obj>
    $<TARGET_OBJECTS:parser_obj>)
target_link_libraries(taos_odbc_a pthread taos)

add_library(taos_odbc SHARED
    $<TARGET_OBJECTS:odbc_obj>
    $<TARGET_OBJECTS:utils_obj>
    $<TARGET_OBJECTS:parser_obj>)
add_dependencies(taos_odbc cjson)
target_link_libraries(taos_odbc pthread taos)
set_target_properties(taos_odbc PROPERTIES
    VERSION 0.1)
install(TARGETS taos_odbc)

add_subdirectory(tests)

