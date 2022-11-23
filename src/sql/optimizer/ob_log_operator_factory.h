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
LOG_OP_DEF(LOG_LIMIT, "LIMIT")
LOG_OP_DEF(LOG_GROUP_BY, "GROUP BY")
LOG_OP_DEF(LOG_ORDER_BY, "ORDER BY")
LOG_OP_DEF(LOG_SORT, "SORT")
LOG_OP_DEF(LOG_TABLE_SCAN, "TABLE SCAN")
LOG_OP_DEF(LOG_JOIN, "JOIN")
LOG_OP_DEF(LOG_SUBPLAN_SCAN, "SUBPLAN SCAN") // 10
LOG_OP_DEF(LOG_SUBPLAN_FILTER, "SUBPLAN FILTER")
LOG_OP_DEF(LOG_EXCHANGE, "EXCHANGE")
LOG_OP_DEF(LOG_FOR_UPD, "FOR UPDATE")
LOG_OP_DEF(LOG_DISTINCT, "DISTINCT")
LOG_OP_DEF(LOG_SET, "SET")
LOG_OP_DEF(LOG_UPDATE, "UPDATE")
LOG_OP_DEF(LOG_DELETE, "DELETE")
LOG_OP_DEF(LOG_INSERT, "INSERT")
LOG_OP_DEF(LOG_EXPR_VALUES, "EXPRESSION")  // 20
LOG_OP_DEF(LOG_VALUES, "VALUES")
LOG_OP_DEF(LOG_MATERIAL, "MATERIAL")
LOG_OP_DEF(LOG_WINDOW_FUNCTION, "WINDOW FUNCTION")
LOG_OP_DEF(LOG_SELECT_INTO, "SELECT INTO")
LOG_OP_DEF(LOG_TOPK, "TOPK")
LOG_OP_DEF(LOG_COUNT, "COUNT")
LOG_OP_DEF(LOG_MERGE, "MERGE")
LOG_OP_DEF(LOG_GRANULE_ITERATOR, "GRANULE ITERATOR")
LOG_OP_DEF(LOG_TABLE_LOOKUP, "TABLE LOOKUP")
LOG_OP_DEF(LOG_JOIN_FILTER, "JOIN FILTER")
LOG_OP_DEF(LOG_SEQUENCE, "SEQUENCE")
LOG_OP_DEF(LOG_MONITORING_DUMP, "MONITORING DUMP")
LOG_OP_DEF(LOG_FUNCTION_TABLE, "FUNCTION_TABLE")
LOG_OP_DEF(LOG_UNPIVOT, "UNPIVOT")
LOG_OP_DEF(LOG_LINK, "LINK")
LOG_OP_DEF(LOG_TEMP_TABLE_INSERT, "TEMP TABLE INSERT")
LOG_OP_DEF(LOG_TEMP_TABLE_ACCESS, "TEMP TABLE ACCESS")
LOG_OP_DEF(LOG_TEMP_TABLE_TRANSFORMATION, "TEMP TABLE TRANSFORMATION")
LOG_OP_DEF(LOG_INSERT_ALL, "INSERT ALL")
LOG_OP_DEF(LOG_ERR_LOG, "ERROR LOGGING")
LOG_OP_DEF(LOG_STAT_COLLECTOR, "STAT COLLECTOR")
/* end of logical operator type */
LOG_OP_DEF(LOG_OP_END, "OP_DEF_END")
#endif /*LOG_OP_DEF*/

#ifndef OCEANBASE_SQL_OB_LOG_OPERATOR_FACTORY_H
#define OCEANBASE_SQL_OB_LOG_OPERATOR_FACTORY_H
#include "lib/allocator/ob_allocator.h"
#include "lib/list/ob_obj_store.h"
namespace oceanbase
{
namespace sql
{
namespace log_op_def
{
/* @note: append only */
enum ObLogOpType
{
#define LOG_OP_DEF(type, name) type,
#include "sql/optimizer/ob_log_operator_factory.h"
#undef LOG_OP_DEF
};

extern const char *get_op_name(ObLogOpType type);

}
class ObLogicalOperator;

enum JoinAlgo
{
  INVALID_JOIN_ALGO = 0,
  NESTED_LOOP_JOIN = (1UL),
  MERGE_JOIN = (1UL << 1),
  HASH_JOIN = (1UL << 2)
};

enum AggregateAlgo
{
  AGGREGATE_UNINITIALIZED = 0,
  MERGE_AGGREGATE,
  HASH_AGGREGATE,
  SCALAR_AGGREGATE
};

enum SetAlgo
{
  INVALID_SET_ALGO = 0,
  MERGE_SET,
  HASH_SET
};

enum DistAlgo
{
  DIST_INVALID_METHOD = 0,
  DIST_BASIC_METHOD = (1UL), // represent local/remote join method
  DIST_PULL_TO_LOCAL = (1UL << 1),
  DIST_HASH_HASH = (1UL << 2),
  DIST_BROADCAST_NONE = (1UL << 3),
  DIST_NONE_BROADCAST = (1UL << 4),
  DIST_BC2HOST_NONE = (1UL << 5),
  DIST_PARTITION_NONE = (1UL << 6),
  DIST_NONE_PARTITION = (1UL << 7),
  DIST_NONE_ALL = (1UL << 8), // all side is allowed for EXPRESSION/DAS
  DIST_ALL_NONE = (1UL << 9),
  DIST_PARTITION_WISE = (1UL << 10),
  DIST_MAX_JOIN_METHOD = (1UL << 11), // represents max join method
  // only for set operator
  DIST_SET_RANDOM = (1UL << 12)
};

inline DistAlgo get_dist_algo(int64_t method)
{
  if (method & DIST_BASIC_METHOD) {
    return DIST_BASIC_METHOD;
  } else if (method & DIST_PULL_TO_LOCAL) {
    return DIST_PULL_TO_LOCAL;
  } else if (method & DIST_HASH_HASH) {
    return DIST_HASH_HASH;
  } else if (method & DIST_BROADCAST_NONE) {
    return DIST_BROADCAST_NONE;
  } else if (method & DIST_NONE_BROADCAST) {
    return DIST_NONE_BROADCAST;
  } else if (method & DIST_BC2HOST_NONE) {
    return DIST_BC2HOST_NONE;
  } else if (method & DIST_PARTITION_NONE) {
    return DIST_PARTITION_NONE;
  } else if (method & DIST_NONE_PARTITION) {
    return DIST_NONE_PARTITION;
  } else if (method & DIST_PARTITION_WISE) {
    return DIST_PARTITION_WISE;
  } else if (method & DIST_NONE_ALL) {
    return DIST_NONE_ALL;
  } else if (method & DIST_ALL_NONE) {
    return DIST_ALL_NONE;        
  } else if (method & DIST_SET_RANDOM) {
    return DIST_SET_RANDOM;
  } else {
    return DIST_INVALID_METHOD;
  }
}

inline DistAlgo get_opposite_distributed_type(DistAlgo dist_type)
{
  DistAlgo oppo_type = DistAlgo::DIST_INVALID_METHOD;
  switch (dist_type) {
  case DistAlgo::DIST_BASIC_METHOD:
    oppo_type = DistAlgo::DIST_BASIC_METHOD;
    break;
  case DistAlgo::DIST_PULL_TO_LOCAL:
    oppo_type = DistAlgo::DIST_PULL_TO_LOCAL;
    break;
  case DistAlgo::DIST_PARTITION_WISE:
    oppo_type = DistAlgo::DIST_PARTITION_WISE;
    break;
  case DistAlgo::DIST_HASH_HASH:
    oppo_type = DistAlgo::DIST_HASH_HASH;
    break;
  case DistAlgo::DIST_BROADCAST_NONE:
    oppo_type = DistAlgo::DIST_NONE_BROADCAST;
    break;
  case DistAlgo::DIST_NONE_BROADCAST:
    oppo_type = DistAlgo::DIST_BROADCAST_NONE;
    break;
  case DistAlgo::DIST_NONE_PARTITION:
    oppo_type = DistAlgo::DIST_PARTITION_NONE;
    break;
  case DistAlgo::DIST_PARTITION_NONE:
    oppo_type = DistAlgo::DIST_NONE_PARTITION;
    break;
  case DistAlgo::DIST_ALL_NONE:
    oppo_type = DistAlgo::DIST_NONE_ALL;
    break;  
  case DistAlgo::DIST_NONE_ALL:
    oppo_type = DistAlgo::DIST_ALL_NONE;
    break;    
  case DistAlgo::DIST_SET_RANDOM:
    oppo_type = DistAlgo::DIST_SET_RANDOM;
    break;
  default:
    break;
  }
  return oppo_type;
}

// Window function distribution
enum class WinDistAlgo
{
  NONE = 0,
  HASH = 1, // hash distribute
  RANGE = 2, // range distribute
  LIST = 3 // range + random distribute
};

#define ADD_DIST_METHOD_WITHOUT_BC2HOST(method) \
  do {add_join_dist_flag(method, DIST_PULL_TO_LOCAL);         \
  add_join_dist_flag(method, DIST_HASH_HASH);                 \
  add_join_dist_flag(method, DIST_BROADCAST_NONE);            \
  add_join_dist_flag(method, DIST_NONE_BROADCAST);            \
  add_join_dist_flag(method, DIST_PARTITION_NONE);            \
  add_join_dist_flag(method, DIST_NONE_PARTITION);            \
  add_join_dist_flag(method, DIST_PARTITION_WISE);            \
  } while(0);

#define REMOVE_PX_SPECIFIC_DIST_METHOD(method) \
  do {remove_join_dist_flag(method, DIST_HASH_HASH);         \
  remove_join_dist_flag(method, DIST_BROADCAST_NONE);        \
  remove_join_dist_flag(method, DIST_NONE_BROADCAST);        \
  remove_join_dist_flag(method, DIST_BC2HOST_NONE);          \
  remove_join_dist_flag(method, DIST_NONE_RANDOM);           \
  remove_join_dist_flag(method, DIST_RANDOM_NONE);           \
  } while(0);

#define UPDATE_CURRENT_JOIN_DIST_METHOD(method, cost, v_method, v_cost) \
  do { if ((DIST_INVALID_METHOD == method || cost > v_cost) && 0 != v_method) {      \
    method = v_method;                                             \
    cost = v_cost;}                                                \
  } while(0);

#define REMOVE_PX_PARALLEL_DFO_DIST_METHOD(method) \
  do {remove_join_dist_flag(method, DIST_HASH_HASH);         \
  remove_join_dist_flag(method, DIST_BROADCAST_NONE);        \
  remove_join_dist_flag(method, DIST_NONE_BROADCAST);        \
  remove_join_dist_flag(method, DIST_NONE_RANDOM);           \
  remove_join_dist_flag(method, DIST_RANDOM_NONE);           \
  } while(0);

class ObLogPlan;
class ObLogOperatorFactory
{
public:
  explicit ObLogOperatorFactory(common::ObIAllocator &allocator);
  ~ObLogOperatorFactory() { destory(); }
  ObLogicalOperator *allocate(ObLogPlan &plan, log_op_def::ObLogOpType type);
  void destory();
private:
  common::ObIAllocator &allocator_;
  common::ObObjStore<ObLogicalOperator *, common::ObIAllocator &> op_store_;
};
}
}
#endif // OCEANBASE_SQL_OB_LOG_OPERATOR_FACTORY_H
