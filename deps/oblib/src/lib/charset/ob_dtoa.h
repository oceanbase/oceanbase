/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_OBMYSQL_OB_DTOA_
#define OCEANBASE_LIB_OBMYSQL_OB_DTOA_

#include "lib/charset/ob_mysql_global.h"

#ifdef __cplusplus
extern "C" {
#endif

//================= from m_string.h ================

/* Conversion routines */
typedef enum { OB_GCVT_ARG_FLOAT, OB_GCVT_ARG_DOUBLE } ob_gcvt_arg_type;

//==================================================

double ob_strtod(const char* str, char** end, int* error);
size_t ob_fcvt(double x, int precision, int width, char* to, ob_bool* error);
size_t ob_gcvt(double x, ob_gcvt_arg_type type, int width, char* to, ob_bool* error);
size_t ob_gcvt_opt(double x, ob_gcvt_arg_type type, int width, char* to, ob_bool* error, ob_bool use_oracle_mode);
size_t ob_gcvt_strict(double x, ob_gcvt_arg_type type, int width, char* to, ob_bool* error, ob_bool use_oracle_mode,
    ob_bool use_force_e_format);

#ifdef __cplusplus
}
#endif

#endif /* OCEANBASE_LIB_OBMYSQL_OB_DTOA_ */
