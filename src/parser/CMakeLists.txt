parser_gen(parser)

set(parser_SOURCES
    parser_tab.c)

add_library(parser_obj OBJECT ${parser_SOURCES})
target_include_directories(parser_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

