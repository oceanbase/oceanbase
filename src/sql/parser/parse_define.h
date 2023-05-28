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

#ifndef OCEANBASE_SQL_PARSER_PARSE_DEFINE_
#define OCEANBASE_SQL_PARSER_PARSE_DEFINE_

#include "lib/utility/ob_macro_utils.h"

#define NULL_PTR(x)        (OB_UNLIKELY(NULL == (x)))

static const int64_t OB_MAX_PARSER_INT16_VALUE = 32738;
//errno keep consistency with  with ob_define.h
static const int32_t OB_PARSER_ERR_NO_MEMORY = -4013;
static const int32_t OB_PARSER_ERR_UNEXPECTED = -4016;
static const int32_t OB_PARSER_ERR_EMPTY_QUERY = -5253;
static const int32_t OB_PARSER_ERR_PARSE_SQL = -5001;
static const int32_t OB_PARSER_ERR_SIZE_OVERFLOW = -4019;
static const int32_t OB_PARSER_SUCCESS = 0;
static const int32_t OB_PARSER_ERR_ILLEGAL_NAME = -5018;
static const int32_t OB_PARSER_ERR_TOO_BIG_DISPLAYWIDTH = -5205;
static const int32_t OB_PARSER_ERR_STR_LITERAL_TOO_LONG = -9644;
static const int32_t OB_PARSER_ERR_NOT_VALID_ROUTINE_NAME = -5980;
static const int32_t OB_PARSER_ERR_MUST_RETURN_SELF = -9646;
static const int32_t OB_PARSER_ERR_ONLY_FUNC_CAN_PIPELINED = -9600;
static const int32_t OB_PARSER_ERR_NO_ATTR_FOUND = -9650;
static const int32_t OB_PARSER_ERR_NON_INT_LITERAL = -9605;
static const int32_t OB_PARSER_ERR_NUMERIC_OR_VALUE_ERROR = -5677;
static const int32_t OB_PARSER_ERR_NON_INTEGRAL_NUMERIC_LITERAL = -9670;
static const int32_t OB_PARSER_ERR_UNDECLARED_VAR = -5543;
static const int32_t OB_PARSER_ERR_UNSUPPORTED = -4007;
static const int32_t OB_PARSER_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK = -9748;
#endif /*OCEANBASE_SQL_PARSER_PARSE_DEFINE_*/
