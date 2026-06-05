/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OBMYSQL_OB_DTOA_
#define OCEANBASE_LIB_OBMYSQL_OB_DTOA_

#include "obcharset/ob_mysql_global.h"

#ifdef	__cplusplus
extern "C" {
#endif

//================= from m_string.h ================

/* Conversion routines */
typedef enum
{
  OB_GCVT_ARG_FLOAT,
  OB_GCVT_ARG_DOUBLE
} ob_gcvt_arg_type;

//==================================================

double ob_strtod(const char *str, char **end, int *error);
size_t ob_fcvt(double x, int precision, int width, char *to, bool *error);
size_t ob_gcvt(double x, ob_gcvt_arg_type type, int width, char *to, bool *error);
// If is_binary_double is false, the behavior at this time is consistent
// with mysql, and mysql mode needs to be used
size_t ob_gcvt_opt(double x, ob_gcvt_arg_type type, int width, char *to, bool *error,
                   bool use_oracle_mode, bool is_binary_double);
size_t ob_gcvt_strict(double x, ob_gcvt_arg_type type, int width, char *to, bool *error,
                      bool use_oracle_mode, bool is_binary_double,
                      bool use_force_e_format);

#ifdef	__cplusplus
}
#endif

#endif /* OCEANBASE_LIB_OBMYSQL_OB_DTOA_ */

