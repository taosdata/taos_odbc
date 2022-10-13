list(APPEND common_SOURCES enums.c)

add_library(common_obj OBJECT ${common_SOURCES})
target_include_directories(common_obj PRIVATE
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET common_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

