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

#ifndef OCEANBASE_SQL_OB_SQL_DEFINE_H_
#define OCEANBASE_SQL_OB_SQL_DEFINE_H_

#include "lib/container/ob_fixed_array.h"

namespace oceanbase {
namespace share {
namespace schema {
struct ObSchemaObjVersion;
}
}  // namespace share
namespace sql {
const int64_t OB_SQL_MAX_CHILD_OPERATOR_NUM = 16;
const int64_t OB_MIN_PARALLEL_TASK_COUNT = 13;   // min task count for one worker
const int64_t OB_MAX_PARALLEL_TASK_COUNT = 100;  // max task count for one worker
const int64_t OB_MIN_MARCO_COUNT_IN_TASK = 1;    // min macro blocks for one worker
const int64_t OB_INVAILD_PARALLEL_TASK_COUNT = -1;
const int64_t OB_EXPECTED_TASK_LOAD = 100;  // MB, one task will get 100MB data from disk
const int64_t OB_GET_MACROS_COUNT_BY_QUERY_RANGE = 1;
const int64_t OB_GET_BLOCK_RANGE = 2;
const int64_t OB_BROADCAST_THRESHOLD = 100;
const int64_t OB_PARTITION_COUNT_PRE_SQL = 16;

typedef common::ObFixedArray<share::schema::ObSchemaObjVersion, common::ObIAllocator> DependenyTableStore;

// ob_rowkey_info.h oceanbase::common::ObOrderType also defined
enum ObOrderDirection {
  NULLS_FIRST_ASC = 0,  //,Forward, NULLs first
  NULLS_LAST_ASC,       // Forward, NULLs last
  NULLS_FIRST_DESC,     // Backward, NULLs first
  NULLS_LAST_DESC,      // Backward, NULLs last
  UNORDERED,            // no order
  MAX_DIR,              // invalid
};

inline bool is_null_first(ObOrderDirection order_direction)
{
  return NULLS_FIRST_ASC == order_direction || NULLS_FIRST_DESC == order_direction;
}

extern ObOrderDirection default_asc_direction();
extern ObOrderDirection default_desc_direction();

inline bool is_ascending_direction(const ObOrderDirection direction)
{
  return (NULLS_FIRST_ASC == direction || NULLS_LAST_ASC == direction);
}

inline bool is_descending_direction(const ObOrderDirection direction)
{
  return (NULLS_FIRST_DESC == direction || NULLS_LAST_DESC == direction);
}

enum ObJoinType {
  UNKNOWN_JOIN = 0,
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  RIGHT_OUTER_JOIN,
  FULL_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  RIGHT_SEMI_JOIN,
  LEFT_ANTI_JOIN,
  RIGHT_ANTI_JOIN,
  CONNECT_BY_JOIN,  // used for hierarchical query
  MAX_JOIN_TYPE
};

enum SlaveMappingType {
  SM_NONE = 0,
  SM_PWJ_HASH_HASH,
  SM_PPWJ_HASH_HASH,
  SM_PPWJ_BCAST_NONE,
  SM_PPWJ_NONE_BCAST,
};

enum PathType { INVALID = 0, ACCESS, JOIN, SUBQUERY, FAKE_CTE_TABLE_ACCESS, FUNCTION_TABLE_ACCESS, TEMP_TABLE_ACCESS };

enum ObNameTypeClass {
  OB_TABLE_NAME_CLASS =
      0,  // table name, database names, table alias names ,affected by system variable lower_case_table_names
  OB_COLUMN_NAME_CLASS =
      1,  // column name,column alias name. index name , stored using lettercase and comparisons are case insensitive
  OB_USER_NAME_CLASS =
      2,  // user names, tenant names and other names, stored using lettercase and comparisons are case sensitive
};

enum ObMatchAgainstMode { NATURAL_LANGUAGE_MODE = 0, BOOLEAN_MODE = 1 };

#define IS_JOIN(type)                                                                             \
  (((type) == PHY_MERGE_JOIN) || ((type) == PHY_NESTED_LOOP_JOIN) || ((type) == PHY_HASH_JOIN) || \
      ((type) == PHY_BLOCK_BASED_NESTED_LOOP_JOIN))

#define IS_OUTER_JOIN(join_type) \
  ((join_type) == LEFT_OUTER_JOIN || (join_type) == RIGHT_OUTER_JOIN || (join_type) == FULL_OUTER_JOIN)

#define IS_SEMI_ANTI_JOIN(join_type)                                                                   \
  ((join_type) == LEFT_SEMI_JOIN || (join_type) == RIGHT_SEMI_JOIN || (join_type) == LEFT_ANTI_JOIN || \
      (join_type) == RIGHT_ANTI_JOIN)

#define IS_SEMI_JOIN(join_type) ((join_type) == LEFT_SEMI_JOIN || (join_type) == RIGHT_SEMI_JOIN)

#define IS_ANTI_JOIN(join_type) ((join_type) == LEFT_ANTI_JOIN || (join_type) == RIGHT_ANTI_JOIN)

#define IS_LEFT_SEMI_ANTI_JOIN(join_type) ((join_type) == LEFT_SEMI_JOIN || (join_type) == LEFT_ANTI_JOIN)

#define IS_RIGHT_SEMI_ANTI_JOIN(join_type) ((join_type) == RIGHT_SEMI_JOIN || (join_type) == RIGHT_ANTI_JOIN)

#define IS_OUTER_OR_CONNECT_BY_JOIN(join_type) (IS_OUTER_JOIN(join_type) || CONNECT_BY_JOIN == join_type)

#define IS_DUMMY_PHY_OPERATOR(op_type) ((op_type == PHY_MONITORING_DUMP))

#define IS_LEFT_STYLE_JOIN(join_type)                                                                  \
  ((join_type) == LEFT_SEMI_JOIN || (join_type) == LEFT_ANTI_JOIN || (join_type) == LEFT_OUTER_JOIN || \
      (join_type) == FULL_OUTER_JOIN)

#define IS_RIGHT_STYLE_JOIN(join_type)                                                                    \
  ((join_type) == RIGHT_SEMI_JOIN || (join_type) == RIGHT_ANTI_JOIN || (join_type) == RIGHT_OUTER_JOIN || \
      (join_type) == FULL_OUTER_JOIN)

#define IS_SET_PHY_OP(type)                                                                       \
  (((type) == PHY_MERGE_UNION) || ((type) == PHY_HASH_UNION) || ((type) == PHY_HASH_INTERSECT) || \
      ((type) == PHY_MERGE_INTERSECT) || ((type) == PHY_HASH_EXCEPT) || ((type) == PHY_MERGE_EXCEPT))

inline ObJoinType get_opposite_join_type(ObJoinType type)
{
  ObJoinType oppo_type = UNKNOWN_JOIN;
  switch (type) {
    case INNER_JOIN:
      oppo_type = INNER_JOIN;
      break;
    case LEFT_OUTER_JOIN:
      oppo_type = RIGHT_OUTER_JOIN;
      break;
    case RIGHT_OUTER_JOIN:
      oppo_type = LEFT_OUTER_JOIN;
      break;
    case FULL_OUTER_JOIN:
      oppo_type = FULL_OUTER_JOIN;
      break;
    case LEFT_SEMI_JOIN:
      oppo_type = RIGHT_SEMI_JOIN;
      break;
    case RIGHT_SEMI_JOIN:
      oppo_type = LEFT_SEMI_JOIN;
      break;
    case LEFT_ANTI_JOIN:
      oppo_type = RIGHT_ANTI_JOIN;
      break;
    case RIGHT_ANTI_JOIN:
      oppo_type = LEFT_ANTI_JOIN;
      break;
    case CONNECT_BY_JOIN:
      oppo_type = CONNECT_BY_JOIN;
      break;
    default:
      break;
  }
  return oppo_type;
}

inline const char* ob_join_type_str(ObJoinType join_type)
{
  const char* ret = "UNKNOWN TYPE";
  static const char* join_type_str_st[] = {"UNKNOWN JOIN",
      "JOIN",
      "OUTER JOIN",
      "RIGHT OUTER JOIN",
      "FULL OUTER JOIN",
      "SEMI JOIN",
      "RIGHT SEMI JOIN",
      "ANTI JOIN",
      "RIGHT ANTI JOIN",
      "CONNECT BY"};

  if (OB_LIKELY(join_type >= UNKNOWN_JOIN) && OB_LIKELY(join_type <= CONNECT_BY_JOIN)) {
    ret = join_type_str_st[join_type];
  }
  return ret;
}

enum ObTableLocationType {
  OB_TBL_LOCATION_UNINITIALIZED = 0,
  OB_TBL_LOCATION_LOCAL,
  OB_TBL_LOCATION_REMOTE,
  OB_TBL_LOCATION_DISTRIBUTED,
  OB_TBL_LOCATION_ALL  // like EXPRESSION, match all
};

enum ObRepartitionType {
  OB_REPARTITION_NO_REPARTITION = 0,        // no partition
  OB_REPARTITION_ONE_SIDE_ONE_LEVEL,        // none partition and one level partition
  OB_REPARTITION_ONE_SIDE_TWO_LEVEL,        // none partition and two level partition
  OB_REPARTITION_BOTH_SIDE_ONE_LEVEL,       // both one level partition
  OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST,  // repartition by first level partition of two level partition
  OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB,    // repartition by second level partition of two level partition
};

enum ObRepartitionScope {
  OB_REPARTITION_NONE_SIDE = 0,
  OB_REPARTITION_LEFT_SIDE,
  OB_REPARTITION_RIGHT_SIDE,
  OB_REPARTITION_BOTH_SIDE,
};

enum ObPhyPlanType {
  OB_PHY_PLAN_UNINITIALIZED = 0,
  OB_PHY_PLAN_LOCAL,
  OB_PHY_PLAN_REMOTE,
  OB_PHY_PLAN_DISTRIBUTED,
  OB_PHY_PLAN_UNCERTAIN
};

inline const char* ob_plan_type_str(ObPhyPlanType plan_type)
{
  const char* ret = "UNKNOWN TYPE";
  static const char* plan_type_str_st[] = {
      "UNINITIALIZED",
      "LOCAL",
      "REMOTE",
      "DISTRIBUTED",
  };

  if (OB_LIKELY(plan_type >= OB_PHY_PLAN_UNINITIALIZED) && OB_LIKELY(plan_type <= OB_PHY_PLAN_DISTRIBUTED)) {
    ret = plan_type_str_st[plan_type];
  }
  return ret;
}

enum ExplainType {
  EXPLAIN_UNINITIALIZED = 0,
  EXPLAIN_OUTLINE,
  EXPLAIN_EXTENDED,
  EXPLAIN_PARTITIONS,
  EXPLAIN_TRADITIONAL,
  EXPLAIN_JSON,
  EXPLAIN_BASIC,
  EXPLAIN_PLANREGRESS,
  EXPLAIN_EXTENDED_NOADDR,
  EXPLAIN_DBLINK_STMT,
};

enum OutlineType { OUTLINE_TYPE_UNINIT = 0, USED_HINT, OUTLINE_DATA };

enum ObPlanLocationType { UNINITIALIZED = 0, LOCAL, REMOTE, DISTRIBUTED };

struct ObPQDistributeMethod {
#define PQ_DIST_METHOD_DEF(DEF)                  \
  DEF(NONE, )                                    \
  DEF(PARTITION, )                               \
  DEF(RANDOM, )                                  \
  DEF(RANDOM_LOCAL, )                            \
  DEF(HASH, )                                    \
  DEF(BROADCAST, )                               \
                                                 \
  /* distribute in two level: SQC && PX worker*/ \
  /* BROADCAST for SQC, RANDOM for PX worker */  \
  DEF(BC2HOST, )                                 \
  DEF(SM_BROADCAST, )                            \
  DEF(PARTITION_HASH, )                          \
  DEF(DROP, )                                    \
  DEF(PARTITION_RANDOM, )                        \
  DEF(MAX_VALUE, )  // represents pull to local

  DECLARE_ENUM(Type, type, PQ_DIST_METHOD_DEF, static);

  static ObPQDistributeMethod::Type get_print_dist(ObPQDistributeMethod::Type method)
  {
    ObPQDistributeMethod::Type print_method = ObPQDistributeMethod::MAX_VALUE;
    if (ObPQDistributeMethod::Type::SM_BROADCAST == method) {
      print_method = ObPQDistributeMethod::Type::BROADCAST;
    } else if (ObPQDistributeMethod::Type::PARTITION_HASH == method) {
      print_method = ObPQDistributeMethod::Type::PARTITION;
    } else {
      print_method = method;
    }
    return print_method;
  }
};

struct ObUsePxHint {
#define USE_PX_METHOD_DEF(DEF) \
  DEF(DISABLE, )               \
  DEF(ENABLE, )                \
  DEF(NOT_SET, )               \
  DEF(INVALID, )               \
  DEF(MAX_VALUE, )
  DECLARE_ENUM(Type, type, USE_PX_METHOD_DEF, static);
};

struct ObUseRewriteHint {
#define USE_REWRITE_DEF(DEF) \
  DEF(NOT_SET, )             \
  DEF(NO_EXPAND, )           \
  DEF(USE_CONCAT, )          \
  DEF(V_MERGE, )             \
  DEF(NO_V_MERGE, )          \
  DEF(UNNEST, )              \
  DEF(NO_UNNEST, )           \
  DEF(PLACE_GROUPBY, )       \
  DEF(NO_PLACE_GROUPBY, )    \
  DEF(NO_PRED_DEDUCE, )      \
  DEF(MAX_VALUE, )
  DECLARE_ENUM(Type, type, USE_REWRITE_DEF, static);
};

enum PartitionFilterType {
  Uninitialized,
  Forbidden,
  OneLevelPartitionKey,
  TwoLevelPartitionKey,
  WholePartitionKey,
};

enum DistinctType { T_DISTINCT_NONE = 0, T_HASH_DISTINCT = 1, T_MERGE_DISTINCT = 2 };

enum class ObPDMLOption { NOT_SPECIFIED = -1, ENABLE, DISABLE, MAX_VALUE };

enum OrderingFlag {
  NOT_MATCH = 0,
  JOIN_MATCH = 1,
  GROUP_MATCH = 1 << 1,
  WINFUNC_MATCH = 1 << 2,
  DISTINCT_MATCH = 1 << 3,
  SET_MATCH = 1 << 4,
  ORDERBY_MATCH = 1 << 5,
  POTENTIAL_MATCH = 1 << 6
};

enum OrderingCheckScope {
  NOT_CHECK = 0,
  CHECK_GROUP = 1,
  CHECK_WINFUNC = 1 << 1,
  CHECK_DISTINCT = 1 << 2,
  CHECK_SET = 1 << 3,
  CHECK_ORDERBY = 1 << 4,
  CHECK_ALL = (1 << 5) - 1
};

enum ObExecuteMode {
  EXECUTE_INVALID = 0,
  EXECUTE_INNER,
  EXECUTE_LOCAL,
  EXECUTE_REMOTE,
  EXECUTE_DIST,
  EXECUTE_PS_PREPARE,  // prepare statement local server
  EXECUTE_PS_EXECUTE,  // prepare statement local server
  EXECUTE_PS_FETCH,
};

enum PartitionIdCalcType {
  CALC_NORMAL = 0,             // calc both part id and subpart id
  CALC_IGNORE_FIRST_PART = 1,  // only calc subpart id
  CALC_IGNORE_SUB_PART = 2     // only calc part id
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_SQL_DEFINE_H_ */
