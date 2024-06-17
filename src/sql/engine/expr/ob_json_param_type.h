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
// modify JsnQueryOpt if modify JsnQueryClause
typedef enum JsnQueryClause {
  JSN_QUE_DOC,      // [0:json_text]
  JSN_QUE_PATH,     // [1:json_path]
  JSN_QUE_RET,      // [2:returning_type]
  JSN_QUE_TRUNC,    // [3:truncate]
  JSN_QUE_SCALAR,   // [4:scalars]
  JSN_QUE_PRETTY,   // [5:pretty]
  JSN_QUE_ASCII,    // [6:ascii]
  JSN_QUE_WRAPPER,  // [7:wrapper]
  JSN_QUE_ASIS,     // [8:asis]
  JSN_QUE_ERROR,    // [9:error_type]
  JSN_QUE_EMPTY,    // [10:empty_type]
  JSN_QUE_MISMATCH, // [11:mismatch]
  JSN_QUE_MULTIVALUE, // [12:multivalue]

  JSN_QUE_MAX, // end
} JsnQueryClause;

typedef enum JsnQueryOpt {
  JSN_QUE_TRUNC_OPT,     // 0
  JSN_QUE_SCALAR_OPT,    // 1
  JSN_QUE_PRETTY_OPT,    // 2
  JSN_QUE_ASCII_OPT,     // 3
  JSN_QUE_WRAPPER_OPT,   // 4
  JSN_QUE_ASIS_OPT,      // 5
  JSN_QUE_ERROR_OPT,     // 6
  JSN_QUE_EMPTY_OPT,     // 7
  JSN_QUE_MISMATCH_OPT,  // 8
  JSN_QUE_MULTIVALUE_OPT,// 9
  JSN_QUE_MAX_OPT, // end
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
  JSN_VAL_DOC,      // 0
  JSN_VAL_PATH,     // 1
  JSN_VAL_RET,      // 2
  JSN_VAL_TRUNC,      // 3
  JSN_VAL_ASCII,     // 4
  JSN_VAL_EMPTY,    // 5
  JSN_VAL_EMPTY_DEF,     // 6
  JSN_VAL_ERROR,    // 7
  JSN_VAL_ERROR_DEF,     // 8
  JSN_VAL_MISMATCH  // 9
} JsnValueClause;


typedef enum JsnValueOpt {
  JSN_VAL_TRUNC_OPT,      // 0
  JSN_VAL_ASCII_OPT,     // 1
  JSN_VAL_EMPTY_OPT,     // 2
  JSN_VAL_ERROR_OPT,  // 3
} JsnValueOpt;

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_JSON_PARAM_TYPE_H */
