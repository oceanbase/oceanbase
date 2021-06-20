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

#ifdef LOG_OP_DEF
LOG_OP_DEF(LOG_OP_INVALID, "INVALID") /* 0 */
LOG_OP_DEF(LOG_PROJECT, "PROJECT")
LOG_OP_DEF(LOG_LIMIT, "LIMIT")
LOG_OP_DEF(LOG_FILTER, "FILTER")
LOG_OP_DEF(LOG_GROUP_BY, "GROUP BY")
LOG_OP_DEF(LOG_ORDER_BY, "ORDER BY")
LOG_OP_DEF(LOG_SORT, "SORT")
LOG_OP_DEF(LOG_TABLE_SCAN, "TABLE SCAN")
LOG_OP_DEF(LOG_MV_TABLE_SCAN, "MV TABLE SCAN")
LOG_OP_DEF(LOG_JOIN, "JOIN")
LOG_OP_DEF(LOG_INDEX_SCAN, "INDEX SCAN")
LOG_OP_DEF(LOG_SUBPLAN_SCAN, "SUBPLAN SCAN")
LOG_OP_DEF(LOG_SUBPLAN_FILTER, "SUBPLAN FILTER")
LOG_OP_DEF(LOG_EXCHANGE, "EXCHANGE")
LOG_OP_DEF(LOG_HAVING, "HAVING")
LOG_OP_DEF(LOG_FOR_UPD, "FOR UPDATE")
LOG_OP_DEF(LOG_DISTINCT, "DISTINCT")
LOG_OP_DEF(LOG_SET, "SET")
LOG_OP_DEF(LOG_UPDATE, "UPDATE")
LOG_OP_DEF(LOG_DELETE, "DELETE")
LOG_OP_DEF(LOG_INSERT, "INSERT")
LOG_OP_DEF(LOG_EXPR_VALUES, "EXPRESSION")
LOG_OP_DEF(LOG_VALUES, "VALUES")
LOG_OP_DEF(LOG_MATERIAL, "MATERIAL")
LOG_OP_DEF(LOG_WINDOW_FUNCTION, "WINDOW FUNCTION")
LOG_OP_DEF(LOG_SELECT_INTO, "SELECT INTO")
LOG_OP_DEF(LOG_TOPK, "TOPK")
LOG_OP_DEF(LOG_APPEND, "APPEND")
LOG_OP_DEF(LOG_COUNT, "COUNT")
LOG_OP_DEF(LOG_MERGE, "MERGE")
LOG_OP_DEF(LOG_GRANULE_ITERATOR, "GRANULE ITERATOR")
LOG_OP_DEF(LOG_TABLE_LOOKUP, "TABLE LOOKUP")
LOG_OP_DEF(LOG_JOIN_FILTER, "JOIN FILTER")
LOG_OP_DEF(LOG_CONFLICT_ROW_FETCHER, "CONFLICT ROW FETCHER")
LOG_OP_DEF(LOG_SEQUENCE, "SEQUENCE")
LOG_OP_DEF(LOG_MONITORING_DUMP, "MONITORING DUMP")
LOG_OP_DEF(LOG_FUNCTION_TABLE, "FUNCTION_TABLE")
LOG_OP_DEF(LOG_UNPIVOT, "UNPIVOT")
LOG_OP_DEF(LOG_LINK, "LINK")
LOG_OP_DEF(LOG_TEMP_TABLE_INSERT, "TEMP TABLE INSERT")
LOG_OP_DEF(LOG_TEMP_TABLE_ACCESS, "TEMP TABLE ACCESS")
LOG_OP_DEF(LOG_TEMP_TABLE_TRANSFORMATION, "TEMP TABLE TRANSFORMATION")
LOG_OP_DEF(LOG_INSERT_ALL, "INSERT ALL")
/* end of logical operator type */
LOG_OP_DEF(LOG_OP_END, "OP_DEF_END")
#endif /*LOG_OP_DEF*/

#ifndef OCEANBASE_SQL_OB_LOG_OPERATOR_FACTORY_H
#define OCEANBASE_SQL_OB_LOG_OPERATOR_FACTORY_H
#include "lib/allocator/ob_allocator.h"
#include "lib/list/ob_obj_store.h"
namespace oceanbase {
namespace sql {
namespace log_op_def {
/* @note: append only */
enum ObLogOpType {
#define LOG_OP_DEF(type, name) type,
#include "sql/optimizer/ob_log_operator_factory.h"
#undef LOG_OP_DEF
};

extern const char* get_op_name(ObLogOpType type);

inline bool instance_of_log_table_scan(ObLogOpType type)
{
  return type == LOG_TABLE_SCAN || type == LOG_MV_TABLE_SCAN;
}

}  // namespace log_op_def
class ObLogicalOperator;

enum JoinAlgo { INVALID_JOIN_ALGO, NESTED_LOOP_JOIN, MERGE_JOIN, HASH_JOIN, BLK_NESTED_LOOP_JOIN };

enum AggregateAlgo { AGGREGATE_UNINITIALIZED = 0, MERGE_AGGREGATE, HASH_AGGREGATE, SCALAR_AGGREGATE };

enum SetAlgo { INVALID_SET_ALGO, MERGE_SET, HASH_SET };

enum JoinDistAlgo {
  DIST_INVALID_METHOD = 0,
  DIST_PULL_TO_LOCAL = (1UL),
  DIST_HASH_HASH = (1UL << 1),
  DIST_BROADCAST_NONE = (1UL << 2),
  DIST_NONE_BROADCAST = (1UL << 3),
  DIST_BC2HOST_NONE = (1UL << 4),
  DIST_PARTITION_NONE = (1UL << 5),
  DIST_NONE_PARTITION = (1UL << 6),
  DIST_PARTITION_WISE = (1UL << 7),
  // only for set operator
  DIST_RANDOM_NONE = (1UL << 8),
  DIST_NONE_RANDOM = (1UL << 9)
};

#define ADD_JOIN_DIST_METHOD_WITHOUT_BC2HOST(method) \
  do {                                               \
    add_join_dist_flag(method, DIST_PULL_TO_LOCAL);  \
    add_join_dist_flag(method, DIST_HASH_HASH);      \
    add_join_dist_flag(method, DIST_BROADCAST_NONE); \
    add_join_dist_flag(method, DIST_NONE_BROADCAST); \
    add_join_dist_flag(method, DIST_PARTITION_NONE); \
    add_join_dist_flag(method, DIST_NONE_PARTITION); \
    add_join_dist_flag(method, DIST_PARTITION_WISE); \
  } while (0);

#define REMOVE_PX_SPECIFIC_JOIN_DIST_METHOD(method)     \
  do {                                                  \
    remove_join_dist_flag(method, DIST_HASH_HASH);      \
    remove_join_dist_flag(method, DIST_BROADCAST_NONE); \
    remove_join_dist_flag(method, DIST_NONE_BROADCAST); \
    remove_join_dist_flag(method, DIST_BC2HOST_NONE);   \
    remove_join_dist_flag(method, DIST_NONE_RANDOM);    \
    remove_join_dist_flag(method, DIST_RANDOM_NONE);    \
  } while (0);

#define UPDATE_CURRENT_JOIN_DIST_METHOD(method, cost, v_method, v_cost)      \
  do {                                                                       \
    if ((DIST_INVALID_METHOD == method || cost > v_cost) && 0 != v_method) { \
      method = v_method;                                                     \
      cost = v_cost;                                                         \
    }                                                                        \
  } while (0);

#define REMOVE_PX_PARALLEL_DFO_DIST_METHOD(method)      \
  do {                                                  \
    remove_join_dist_flag(method, DIST_HASH_HASH);      \
    remove_join_dist_flag(method, DIST_BROADCAST_NONE); \
    remove_join_dist_flag(method, DIST_NONE_BROADCAST); \
    remove_join_dist_flag(method, DIST_NONE_RANDOM);    \
    remove_join_dist_flag(method, DIST_RANDOM_NONE);    \
  } while (0);

inline const char* JoinAlgo_to_hint_str(JoinAlgo algo)
{
  static const char* algo_str[] = {"INVALID", "USE_NL", "USE_MERGE", "USE_HASH"};
  return algo_str[algo <= HASH_JOIN ? algo : INVALID_JOIN_ALGO];
}
class ObLogPlan;
class ObLogOperatorFactory {
public:
  explicit ObLogOperatorFactory(common::ObIAllocator& allocator);
  ~ObLogOperatorFactory()
  {
    destory();
  }
  ObLogicalOperator* allocate(ObLogPlan& plan, log_op_def::ObLogOpType type);
  void destory();

private:
  common::ObIAllocator& allocator_;
  common::ObObjStore<ObLogicalOperator*, common::ObIAllocator&> op_store_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_LOG_OPERATOR_FACTORY_H
