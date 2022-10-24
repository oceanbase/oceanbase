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

#ifndef OCEANBASE_COMMON_ACTION_FLAG_H__
#define OCEANBASE_COMMON_ACTION_FLAG_H__

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObActionFlag
{
public:
  static const int64_t OP_MIN_OBJ = 1;
  static const int64_t OP_MAX_OBJ = 2;
  static const int64_t OP_NOP = 3;

  // @todo remove following
  static const int64_t OP_MH_ACTIVE             = 4;
  static const int64_t OP_READ                  = 5;
  static const int64_t OP_UPDATE                = 6;
  static const int64_t OP_INSERT                = 7;
  static const int64_t OP_DEL_ROW               = 8;
  static const int64_t OP_RT_TABLE_TYPE         = 9;
  static const int64_t OP_RT_TABLE_INDEX_TYPE   = 10;
  static const int64_t OP_ROW_DOES_NOT_EXIST    = 11;
  static const int64_t OP_END_FLAG              = 12;
  static const int64_t OP_SYS_DATE              = 13;
  static const int64_t OP_DEL_TABLE             = 14;
  static const int64_t OP_ROW_EXIST             = 16;
  static const int64_t OP_END_ROW               = 17;
  static const int64_t OP_VALID                 = 18;
  static const int64_t OP_NEW_ADD               = 19;
  static const int64_t OP_DEFAULT_NOW_FLAG      = 20;
  static const int64_t OP_USE_OB_SEM            = 21;
  static const int64_t OP_USE_DB_SEM            = 22;
  static const int64_t OP_LOCK_ROW              = 23;

  static const int64_t OP_RETURN_UPDATE_RESULT  = 0x0000000100000000;
  static const int64_t OP_ACTION_FLAG_LOW_MASK  = 0x00000000ffffffff;

  // serialize ext obj type
  static const int64_t BASIC_PARAM_FIELD        = 50;
  static const int64_t END_PARAM_FIELD          = 51;
  static const int64_t TABLE_PARAM_FIELD        = 52;
  static const int64_t ROW_KEY_FIELD            = 53;
  static const int64_t TABLE_NAME_FIELD         = 54;
  static const int64_t COLUMN_PARAM_FIELD       = 55;
  static const int64_t SORT_PARAM_FIELD         = 56;
  static const int64_t LIMIT_PARAM_FIELD        = 57;
  static const int64_t SELECT_CLAUSE_WHERE_FIELD = 58;
  static const int64_t MUTATOR_PARAM_FIELD      = 59;
  static const int64_t TABLET_RANGE_FIELD       = 60;
  static const int64_t OBDB_SEMANTIC_FIELD      = 61;
  static const int64_t GROUPBY_PARAM_FIELD      = 62;
  static const int64_t GROUPBY_GRO_COLUMN_FIELD = 63;
  static const int64_t GROUPBY_RET_COLUMN_FIELD = 64;
  static const int64_t GROUPBY_AGG_COLUMN_FIELD = 65;
  static const int64_t UPDATE_COND_PARAM_FIELD  = 66;
  static const int64_t UPDATE_COND_FIELD        = 67;
  static const int64_t RESERVE_PARAM_FIELD      = 68;
  /// extention field add when ms 0.3
  static const int64_t SELECT_CLAUSE_RETURN_INFO_FIELD = 69;
  static const int64_t GROUPBY_CLAUSE_RETURN_INFO_FIELD = 70;

  static const int64_t SELECT_CLAUSE_COMP_COLUMN_FIELD = 71;
  static const int64_t GROUPBY_CLAUSE_COMP_COLUMN_FIELD = 72;

  /// static const int64_t SELECT_CLAUSE_WHERE_FILED= 58;
  static const int64_t GROUPBY_CLAUSE_HAVING_FIELD = 73;

  /// topk param
  static const int64_t TOPK_PARAM_FIELD        = 74;

  static const int64_t PREFETCH_PARAM_FIELD     = 75;
  static const int64_t MUTATOR_TYPE_FIELD       = 76;
  /// obscanner meta param
  static const int64_t META_PARAM_FIELD         = 80;
  /// add for SQL
  static const int64_t SQL_PROJECT_PARAM_FIELD      = 81;
  static const int64_t SQL_FILTER_PARAM_FIELD       = 82;
  static const int64_t SQL_LIMIT_PARAM_FIELD        = 83;
  static const int64_t SQL_SCALAR_AGG_PARAM_FIELD   = 84;
  static const int64_t SQL_GROUP_BY_PARAM_FIELD     = 85;
  static const int64_t SQL_GROUP_SORT_PARAM_FIELD   = 86;
  /// end extention field add when ms 0.3
  static const int64_t NEWRANGE_PARAM_FIELD     = 87;
  static const int64_t FORMED_ROW_KEY_FIELD     = 88;
  static const int64_t TABLET_LOCATION_FIELD    = 89;
  // add for SQL
  static const int64_t SQL_DATA_VERSION        = 90;
  static const int64_t DML_TYPE_FIELD          = 91;
  // new scanner basic 2
  static const int64_t NEW_SCANNER_BASIC_PARAM_FIELD_2 = 92;
  static const int64_t TRANS_ID_PARAM_FIELD = 93;
  // for ObPostfixExpression::serialize
  static const int64_t EXPR_OPERATOR = 94;
  static const int64_t EXPR_QUESTIONMARK = 95;
  static const int64_t EXPR_SYSTEM_VARIABLE = 96;
  static const int64_t EXPR_USER_VARIABLE = 97;
};
} /* common */
} /* oceanbase */

#endif
