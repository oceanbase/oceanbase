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

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_JSON_PARAM_TYPE_H
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_JSON_PARAM_TYPE_H
namespace oceanbase
{
namespace sql
{

// json_query
/* process empty or error */
typedef enum JsnQueryType {
  JSN_QUERY_ERROR,              // 0
  JSN_QUERY_NULL,               // 1
  JSN_QUERY_EMPTY,              // 2
  JSN_QUERY_EMPTY_ARRAY,        // 3
  JSN_QUERY_EMPTY_OBJECT,       // 4
  JSN_QUERY_IMPLICIT,           // 5
  JSN_QUERY_RESPONSE_COUNT,     // 6
} JsnQueryType;

/* process on mismatch { error : 0, null : 1, implicit : 2 }*/
typedef enum JsnQueryMisMatch {
  JSN_QUERY_MISMATCH_ERROR,        // 0
  JSN_QUERY_MISMATCH_NULL,         // 1
  JSN_QUERY_MISMATCH_IMPLICIT,     // 2
  JSN_QUERY_MISMATCH_DOT,          // 3
  JSN_QUERY_MISMATCH_COUNT,        // 4
} JsnQueryMisMatch;

/* process on error */
typedef enum JsnQueryOnError {
  JSN_QUERY_ERROR_ON_ERROR = 0,
  JSN_QUERY_NULL_ON_ERROR = 1,
  JSN_QUERY_EMPTY_ON_ERROR = 2,
  JSN_QUERY_EMPTY_ARRAY_ON_ERROR = 3,
  JSN_QUERY_EMPTY_OBJECT_ON_ERROR = 4,
  JSN_QUERY_DEFAULT_ON_ERROR = 5
} JsnQueryOnError;

/* process on error */
typedef enum JsnQueryOnEmpty {
  JSN_QUERY_ERROR_ON_EMPTY = 0,
  JSN_QUERY_NULL_ON_EMPTY = 1,
  JSN_QUERY_EMPTY_ON_EMPTY = 2,
  JSN_QUERY_EMPTY_ARRAY_ON_EMPTY = 3,
  JSN_QUERY_EMPTY_OBJECT_ON_EMPTY = 4,
  JSN_QUERY_DEFAULT_ON_EMPTY = 5
} JsnQueryOnEmpty;

/* process wrapper type */
typedef enum JsnQueryWrapper {
  JSN_QUERY_WITHOUT_WRAPPER,                     // 0
  JSN_QUERY_WITHOUT_ARRAY_WRAPPER,               // 1
  JSN_QUERY_WITH_WRAPPER,                        // 2
  JSN_QUERY_WITH_ARRAY_WRAPPER,                  // 3
  JSN_QUERY_WITH_UNCONDITIONAL_WRAPPER,          // 4
  JSN_QUERY_WITH_CONDITIONAL_WRAPPER,            // 5
  JSN_QUERY_WITH_UNCONDITIONAL_ARRAY_WRAPPER,    // 6
  JSN_QUERY_WITH_CONDITIONAL_ARRAY_WRAPPER,      // 7
  JSN_QUERY_WRAPPER_IMPLICIT ,                   // 8
  JSN_QUERY_WRAPPER_COUNT,                       // 9
} JsnQueryWrapper;

/* process on scalars { allow : 0, disallow : 1, implicit : 2 }*/
typedef enum JsnQueryScalar {
  JSN_QUERY_SCALARS_ALLOW,       // 0
  JSN_QUERY_SCALARS_DISALLOW,    // 1
  JSN_QUERY_SCALARS_IMPLICIT,    // 2
  JSN_QUERY_SCALARS_COUNT       // 3
} JsnQueryScalar;

/* pretty ascii 0 : null 1 : yes */
typedef enum JsnQueryAsc {
  OB_JSON_PRE_ASC_EMPTY,       // 0
  OB_JSON_PRE_ASC_SET,         // 1
  OB_JSON_PRE_ASC_COUNT       // 2
} JsnQueryAsc;

// json query clause position
typedef enum JsnQueryClause {
  JSN_QUE_DOC = 0,
  JSN_QUE_PATH = 1,
  JSN_QUE_RET = 2,
  JSN_QUE_TRUNC = 3,
  JSN_QUE_SCALAR = 4,
  JSN_QUE_PRETTY = 5,
  JSN_QUE_ASCII = 6,
  JSN_QUE_WRAPPER = 7,
  JSN_QUE_ERROR = 8,
  JSN_QUE_EMPTY = 9,
  JSN_QUE_MISMATCH = 10,
  JSN_QUE_PARAM_NUM = 11,
} JsnQueryClause;

typedef enum JsnQueryOpt {
  JSN_QUE_TRUNC_OPT,      // 0
  JSN_QUE_SCALAR_OPT,     // 1
  JSN_QUE_PRETTY_OPT,    // 2
  JSN_QUE_ASCII_OPT,     // 3
  JSN_QUE_WRAPPER_OPT,    // 4
  JSN_QUE_ERROR_OPT,     // 5
  JSN_QUE_EMPTY_OPT,  // 6
  JSN_QUE_MISMATCH_OPT, // 7
} JsnQueryOpt;

// json_value
/* process empty or error */
typedef enum JsnValueType {
  JSN_VALUE_ERROR,    // 0
  JSN_VALUE_NULL,     // 1
  JSN_VALUE_DEFAULT,  // 2
  JSN_VALUE_IMPLICIT, // 3
} JsnValueType;
/* process mismatch type { MISSING : 4 (1), EXTRA : 5 (2), TYPE : 6 (4), EMPTY : 7 (0)} make diff with mismatch type  */
typedef enum JsnValueClause {
  JSN_VAL_DOC = 0,
  JSN_VAL_PATH = 1,
  JSN_VAL_RET = 2,
  JSN_VAL_TRUNC = 3,
  JSN_VAL_ASCII = 4,
  JSN_VAL_EMPTY = 5,
  JSN_VAL_EMPTY_DEF = 6,
  JSN_VAL_ERROR = 7,
  JSN_VAL_ERROR_DEF = 8,
  JSN_VAL_MISMATCH = 9
} JsnValueClause;

typedef enum JsnValueOpt {
  JSN_VAL_TRUNC_OPT,      // 0
  JSN_VAL_ASCII_OPT,     // 1
  JSN_VAL_EMPTY_OPT,     // 2
  JSN_VAL_ERROR_OPT,  // 3
} JsnValueOpt;

/* process on error */
typedef enum JsnValueOnError {
  JSN_VALUE_ERROR_ON_ERROR = 0,
  JSN_VALUE_NULL_ON_ERROR = 1,
  JSN_VALUE_LITERAL_ON_ERROR = 2,
  JSN_VALUE_DEFAULT_ON_ERROR = 3
} JsnValueOnError;

/* process on empty */
typedef enum JsnValueOnEmpty {
  JSN_VALUE_ERROR_ON_EMPTY = 0,
  JSN_VALUE_NULL_ON_EMPTY = 1,
  JSN_VALUE_LITERAL_ON_EMPTY = 2,
  JSN_VALUE_DEFAULT_ON_EMPTY = 3
} JsnValueOnEmpty;

// multi-mode table
typedef enum MTableParamNum {
  COL_ORDINALITY_PARAM_NUM = 2,
  JS_EXIST_COL_PARAM_NUM = 5,
  JS_QUERY_COL_PARAM_NUM = 7,
  XML_EXTRACT_COL_PARAM_NUM = 5,

} MTableParamNum;
typedef enum JsnTablePubClause {
  JT_DOC = 0,
  JT_PATH = 1,
  JT_ON_ERROR = 2,
  JT_COL_NODE = 3,
  JT_ALIAS = 4,
  JT_PARAM_NUM = 5
} JsnTablePubClause;

// EXIST COL, QUERY COL, VALUE COL
typedef enum JtColClause {
  JT_COL_NAME = 0,
  JT_COL_TYPE = 1,
  JT_COL_TRUNCATE = 2,
  JT_EXISTS_COL_PATH = 3,
  JT_EXISTS_COL_ON_ERR = 4,
  JT_QUERY_COL_SCALAR = 3,
  JT_QUERY_COL_WRAPPER = 4,
  JT_QUERY_COL_PATH = 5,
  JT_QUERY_ON_ERROR = 6,
  JT_VAL_TRUNCATE = 2,
  JT_VAL_PATH = 3,
} JtColClause;

typedef enum JtOnErrorNode {
  JT_ERROR_OPT = 0,
  JT_EMPTY_OPT = 1,
  JT_MISMATCH_OPT = 2,
} JtOnErrorNode;

typedef enum XmlTablePubClause {
  XT_NS = 0,
  XT_PATH = 1,
  XT_DOC = 2,
  XT_ON_ERROR = 3,
  XT_COL_NODE = 4,
  XT_ALIAS = 5,
  XT_PARAM_NUM = 6
} XmlTablePubClause;

// when is_input_quoted_ == 1
// first node is error_node, second node is empty_node
// else first node is empty_node, second node is error node
typedef enum XTColClauseOpt {
  XT_COL_ERROR_FIRST_OPT = 0,
  XT_COL_ERROR_FIRST_DEFAULT_VAL = 1,
  XT_COL_ERROR_SECOND_OPT = 2,
  XT_COL_ERROR_SECOND_DEFAULT_VAL = 3,
  XT_COL_MISMATCH = 4
} XTColClauseOpt;
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_JSON_PARAM_TYPE_H */