list(APPEND core_SOURCES desc.c)
list(APPEND core_SOURCES env.c)
list(APPEND core_SOURCES errs.c)
list(APPEND core_SOURCES conn.c)
list(APPEND core_SOURCES stmt.c)

add_library(core_obj OBJECT ${core_SOURCES})
target_include_directories(core_obj PRIVATE
    ${INTERNAL_INC_PATH}
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET core_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

