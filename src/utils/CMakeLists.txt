set(utils_SOURCES
    utils.c)

add_library(utils_obj OBJECT ${utils_SOURCES})
target_include_directories(utils_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET utils_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

