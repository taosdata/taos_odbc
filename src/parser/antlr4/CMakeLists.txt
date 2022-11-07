###############################################################################
# MIT License
#
# Copyright (c) 2022 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

add_custom_target(Expr DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/Expr_done)
add_custom_command(OUTPUT
    ${CMAKE_CURRENT_BINARY_DIR}/Expr_done
    ${CMAKE_CURRENT_BINARY_DIR}/ExprBaseListener.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprBaseVisitor.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprLexer.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprListener.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprParser.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprVisitor.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS Expr.g4
    COMMAND java -cp ${TAOS_ODBC_LOCAL_REPO}/antlr4/src/ex_antlr4/tool/target/antlr4-4.11.0-complete.jar org.antlr.v4.Tool
                 -Dlanguage=Cpp
                 -visitor
                 -o ${CMAKE_CURRENT_BINARY_DIR}
                 Expr.g4
    COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/Expr_done)
add_dependencies(Expr ex_antlr4_lib)

set(Expr_SOURCES
    ${CMAKE_CURRENT_BINARY_DIR}/ExprBaseListener.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprBaseVisitor.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprLexer.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprListener.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprParser.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ExprVisitor.cpp
    )

add_library(Expr_obj OBJECT ${Expr_SOURCES})
target_include_directories(Expr_obj PRIVATE
    ${TAOS_ODBC_LOCAL_INSTALL}/include/antlr4-runtime
    ${CMAKE_CURRENT_BINARY_DIR}
    ${INTERNAL_INC_PATH})
set_property(TARGET Expr_obj PROPERTY POSITION_INDEPENDENT_CODE 1)
add_dependencies(Expr_obj ex_antlr4_lib Expr)
target_compile_options(Expr_obj PRIVATE -Wno-error=unused-parameter)

