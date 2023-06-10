/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
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

#ifndef _helpers_h_
#define _helpers_h_

#include "taos_odbc_config.h"

#include "os_port.h"

#include "logger.h"

#include <inttypes.h>
#include <stdio.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <string.h>
#endif

EXTERN_C_BEGIN

const char *tod_strptime(const char *s, const char *format, struct tm *tm) FA_HIDDEN;
uintptr_t tod_get_current_thread_id(void) FA_HIDDEN;
uintptr_t tod_get_current_process_id(void) FA_HIDDEN;
const char* tod_get_format_current_local_timestamp_ms(char *s, size_t n) FA_HIDDEN;
const char* tod_get_format_current_local_timestamp_us(char *s, size_t n) FA_HIDDEN;

#ifdef _WIN32
#define tod_strcasecmp      _stricmp
#define tod_strncasecmp     _strnicmp
#else
#define tod_strcasecmp      strcasecmp
#define tod_strncasecmp     strncasecmp
#endif

EXTERN_C_END

#endif // _helpers_h_

