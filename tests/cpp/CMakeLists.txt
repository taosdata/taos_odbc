add_executable(SQLBindCol       SQLBindCol_ref.cpp)

target_link_libraries(SQLBindCol odbc)

add_test(NAME SQLBindCol
    COMMAND SQLBindCol)

add_test(NAME VSQLBindCol
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/SQLBindCol)

