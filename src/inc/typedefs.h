/*
 * MIT License
 *
 * Copyright (c) 2022 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _typedefs_h_
#define _typedefs_h_

typedef struct charset_conv_s            charset_conv_t;
typedef struct charset_conv_mgr_s        charset_conv_mgr_t;

typedef struct columns_args_s            columns_args_t;
typedef struct columns_s                 columns_t;

typedef struct column_meta_s             column_meta_t;

typedef struct connection_cfg_s          connection_cfg_t;

typedef struct conn_s                    conn_t;

typedef struct data_s                    data_t;
typedef struct data_conv_result_s        data_conv_result_t;

typedef struct descriptor_s              descriptor_t;
typedef struct desc_s                    desc_t;
typedef struct desc_record_s             desc_record_t;

typedef struct env_s                     env_t;

typedef struct errs_s                    errs_t;

typedef struct parser_token_s            parser_token_t;
typedef struct parser_param_s            parser_param_t;

typedef struct primarykeys_args_s        primarykeys_args_t;
typedef struct primarykeys_s             primarykeys_t;

typedef struct stmt_s                    stmt_t;
typedef struct stmt_get_data_args_s      stmt_get_data_args_t;

typedef struct stmt_base_s               stmt_base_t;

typedef struct tables_args_s             tables_args_t;
typedef struct tables_s                  tables_t;

typedef struct tls_s                     tls_t;

typedef struct tsdb_stmt_s               tsdb_stmt_t;
typedef struct tsdb_params_s             tsdb_params_t;
typedef struct tsdb_binds_s              tsdb_binds_t;
typedef struct tsdb_res_s                tsdb_res_t;
typedef struct tsdb_fields_s             tsdb_fields_t;
typedef struct tsdb_rows_block_s         tsdb_rows_block_t;
typedef struct tsdb_data_s               tsdb_data_t;

typedef struct typesinfo_s               typesinfo_t;

#endif // _typedefs_h_

