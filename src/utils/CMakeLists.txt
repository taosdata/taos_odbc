set(utils_SOURCES
    log.c)

add_library(utils_obj OBJECT ${utils_SOURCES})
target_include_directories(utils_obj PRIVATE
    ${CMAKE_SOURCE_DIR}/inc)
set_property(TARGET utils_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

