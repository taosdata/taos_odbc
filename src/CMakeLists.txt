set(INTERNAL_INC_PATH ${CMAKE_CURRENT_SOURCE_DIR}/inc)

add_subdirectory(core)
add_subdirectory(parser)
add_subdirectory(utils)

list(APPEND odbc_SOURCES odbc.c)

add_library(odbc_obj OBJECT ${odbc_SOURCES})
target_include_directories(odbc_obj PRIVATE
    ${INTERNAL_INC_PATH}
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET odbc_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

# FIXME:
# set(CMAKE_REQUIRED_LINK_OPTIONS ${ODBC_LINK_OPTIONS} ${ODBCINST_LINK_OPTIONS})

add_library(taos_odbc_a STATIC
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:core_obj>
    $<TARGET_OBJECTS:odbc_obj>
    $<TARGET_OBJECTS:utils_obj>
    $<TARGET_OBJECTS:parser_obj>)
target_link_libraries(taos_odbc_a pthread taos odbcinst)

add_library(taos_odbc SHARED
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:core_obj>
    $<TARGET_OBJECTS:odbc_obj>
    $<TARGET_OBJECTS:utils_obj>
    $<TARGET_OBJECTS:parser_obj>)
# add_dependencies(taos_odbc cjson)
target_link_libraries(taos_odbc pthread taos odbcinst)

set_target_properties(taos_odbc PROPERTIES
    VERSION 0.1)
install(TARGETS taos_odbc)

add_subdirectory(tests)

