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

#ifndef OCEANBASE_SQL_OB_LOGICAL_OPERATOR_H
#define OCEANBASE_SQL_OB_LOGICAL_OPERATOR_H

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/json/ob_json.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/rewrite/ob_equal_analysis.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_opt_est_sel.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_raw_expr_check_dep.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_fd_item.h"

namespace oceanbase {
namespace sql {
class planText;
struct partition_location {
  int64_t partition_id;
  // server id
  TO_STRING_KV(K(partition_id));
};
static const char ID[] = "ID";
static const char OPERATOR[] = "OPERATOR";
static const char NAME[] = "NAME";
static const char ROWS[] = "EST.ROWS";
static const char COST[] = "COST";
static const char OPCOST[] = "OP_COST";

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_EXPRS(exprs, type)                                                       \
  {                                                                                            \
    int64_t N = -1;                                                                            \
    if (OB_FAIL(ret)) {                           /* Do nothing */                             \
    } else if (OB_FAIL(BUF_PRINTF(#exprs "("))) { /* Do nothing */                             \
    } else if (FALSE_IT(N = exprs.count())) {     /* Do nothing */                             \
    } else if (N == 0) {                                                                       \
      ret = BUF_PRINTF("nil");                                                                 \
    } else {                                                                                   \
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                                        \
        if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                                       \
        } else if (OB_ISNULL(exprs.at(i))) {                                                   \
          ret = OB_ERR_UNEXPECTED;                                                             \
        } else if (OB_FAIL(exprs.at(i)->get_name(buf, buf_len, pos, type))) { /*Do nothing */  \
        } else if (OB_FAIL(BUF_PRINTF("]"))) {                                /* Do nothing */ \
        } else if (i < N - 1) {                                                                \
          ret = BUF_PRINTF(", ");                                                              \
        } else { /* Do nothing */                                                              \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
    if (OB_SUCC(ret)) { /* Do nothing */                                                       \
      ret = BUF_PRINTF(")");                                                                   \
    } else { /* Do nothing */                                                                  \
    }                                                                                          \
  }

#define EXPLAIN_PRINT_EXPR(expr, type)                             \
  {                                                                \
    if (OB_FAIL(ret)) {                          /* Do nothing */  \
    } else if (OB_FAIL(BUF_PRINTF(#expr "("))) { /* Do nothing */  \
    } else if (OB_ISNULL(expr)) {                                  \
      ret = BUF_PRINTF("nil");                                     \
    } else if (OB_FAIL(expr->get_name(buf, buf_len, pos, type))) { \
      LOG_WARN("print expr name failed", K(ret));                  \
    } else { /*Do nothing*/                                        \
    }                                                              \
    if (OB_SUCC(ret)) { /* Do nothing */                           \
      ret = BUF_PRINTF(")");                                       \
    } else { /* Do nothing */                                      \
    }                                                              \
  }

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_SORT_KEYS(exprs, type)                                                         \
  {                                                                                                  \
    int64_t N = -1;                                                                                  \
    if (OB_FAIL(ret)) {                           /* Do nothing */                                   \
    } else if (OB_FAIL(BUF_PRINTF(#exprs "("))) { /* Do nothing */                                   \
    } else if (FALSE_IT(N = exprs.count())) {                                                        \
    } else if (N == 0) {                                                                             \
      BUF_PRINTF("nil");                                                                             \
    } else {                                                                                         \
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                                              \
        if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                                             \
        } else if (OB_ISNULL(exprs.at(i).expr_)) {                                                   \
          ret = common::OB_ERR_UNEXPECTED;                                                           \
        } else if (OB_FAIL(exprs.at(i).expr_->get_name(buf, buf_len, pos, type))) { /* Do nothing */ \
        } else if (exprs.at(i).is_descending()) {                                                    \
          ret = BUF_PRINTF(", DESC");                                                                \
        } else { /* Do nothing */                                                                    \
        }                                                                                            \
        if (OB_FAIL(ret)) {                    /* Do nothing */                                      \
        } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                                      \
        } else if (i < N - 1) {                                                                      \
          ret = BUF_PRINTF(", ");                                                                    \
        } else { /* Do nothing */                                                                    \
        }                                                                                            \
      }                                                                                              \
    }                                                                                                \
    if (OB_SUCC(ret)) { /* Do nothing */                                                             \
      ret = BUF_PRINTF(")");                                                                         \
    } else { /* Do nothing */                                                                        \
    }                                                                                                \
  }

/**
 *  Print log info with exec params
 */
#define EXPLAIN_PRINT_EXEC_PARAMS(exec_params, type)                                       \
  {                                                                                        \
    int64_t N = -1;                                                                        \
    if (OB_FAIL(ret)) {                                 /* Do nothing */                   \
    } else if (OB_FAIL(BUF_PRINTF(#exec_params "("))) { /* Do nothing */                   \
    } else if (FALSE_IT(N = exec_params.count())) {     /* Do nothing */                   \
    } else if (N == 0) {                                                                   \
      ret = BUF_PRINTF("nil");                                                             \
    } else {                                                                               \
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                                    \
        if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                                   \
        } else if (OB_ISNULL(exec_params.at(i).second)) {                                  \
          ret = OB_ERR_UNEXPECTED;                                                         \
        } else if (OB_FAIL(exec_params.at(i).second->get_name(buf, buf_len, pos, type))) { \
        } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                            \
        } else if (i < N - 1) {                                                            \
          ret = BUF_PRINTF(", ");                                                          \
        } else { /* Do nothing */                                                          \
        }                                                                                  \
      }                                                                                    \
    }                                                                                      \
    if (OB_SUCC(ret)) {                                                                    \
      ret = BUF_PRINTF(")");                                                               \
    } else { /* Do nothing */                                                              \
    }                                                                                      \
  }

/**
 *  Print log info with idxs
 */
#define EXPLAIN_PRINT_IDXS(idxs)                                             \
  {                                                                          \
    ObSEArray<int64_t, 4, common::ModulePageAllocator, true> arr;            \
    int64_t N = -1;                                                          \
    if (OB_FAIL(ret)) {                          /* Do nothing */            \
    } else if (OB_FAIL(BUF_PRINTF(#idxs "("))) { /* Do nothing */            \
    } else if (OB_FAIL(idxs.to_array(arr))) {    /* Do nothing */            \
    } else if (FALSE_IT(N = arr.count())) {      /* Do nothing */            \
    } else if (N == 0) {                                                     \
      ret = BUF_PRINTF("nil");                                               \
    } else {                                                                 \
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                      \
        if (OB_FAIL(BUF_PRINTF("["))) {                     /* Do nothing */ \
        } else if (OB_FAIL(BUF_PRINTF("%lu", arr.at(i)))) { /* Do nothing */ \
        } else if (OB_FAIL(BUF_PRINTF("]"))) {              /* Do nothing */ \
        } else if (i < N - 1) {                                              \
          ret = BUF_PRINTF(", ");                                            \
        } else { /* Do nothing */                                            \
        }                                                                    \
      }                                                                      \
    }                                                                        \
    if (OB_SUCC(ret)) { /* Do nothing */                                     \
      ret = BUF_PRINTF(")");                                                 \
    } else { /* Do nothing */                                                \
    }                                                                        \
  }

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_MERGE_DIRECTIONS(directions)                      \
  {                                                                     \
    int64_t N = -1;                                                     \
    if (OB_FAIL(ret)) {                                /* Do nothing */ \
    } else if (OB_FAIL(BUF_PRINTF(#directions "("))) { /* Do nothing */ \
    } else if (FALSE_IT(N = directions.count())) {     /* Do nothing */ \
    } else if (N == 0) {                                                \
      ret = BUF_PRINTF("nil");                                          \
    } else {                                                            \
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                 \
        if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                \
        } else if (is_descending_direction(directions.at(i))) {         \
          ret = BUF_PRINTF("DESC");                                     \
        } else {                                                        \
          ret = BUF_PRINTF("ASC");                                      \
        }                                                               \
        if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                \
        } else if (i < N - 1) {                                         \
          ret = BUF_PRINTF(", ");                                       \
        } else { /* Do nothing */                                       \
        }                                                               \
      }                                                                 \
    }                                                                   \
    if (OB_SUCC(ret)) {                                                 \
      ret = BUF_PRINTF(")");                                            \
    } else { /* Do nothing */                                           \
    }                                                                   \
  }

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type)                                       \
  {                                                                                     \
    int64_t N = -1;                                                                     \
    if (OB_FAIL(ret)) {                               /* Do nothing */                  \
    } else if (OB_FAIL(BUF_PRINTF(#sort_keys "("))) { /* Do nothing */                  \
    } else if (FALSE_IT(N = sort_keys.count())) {     /* Do nothing */                  \
    } else if (0 == N) {                                                                \
      ret = BUF_PRINTF("nil");                                                          \
    } else {                                                                            \
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                                 \
        if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                                \
        } else if (OB_ISNULL(sort_keys.at(i).expr_)) {                                  \
          ret = OB_ERR_UNEXPECTED;                                                      \
        } else if (OB_FAIL(sort_keys.at(i).expr_->get_name(buf, buf_len, pos, type))) { \
          LOG_WARN("fail to get name", K(i), K(ret));                                   \
        } else if (OB_FAIL(BUF_PRINTF(", "))) { /* Do nothing */                        \
        } else if (is_ascending_direction(sort_keys.at(i).order_type_)) {               \
          ret = BUF_PRINTF("ASC");                                                      \
        } else {                                                                        \
          ret = BUF_PRINTF("DESC");                                                     \
        }                                                                               \
        if (OB_FAIL(ret)) {                    /* Do nothing */                         \
        } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                         \
        } else if (i < N - 1) {                                                         \
          ret = BUF_PRINTF(", ");                                                       \
        } else { /* Do nothing */                                                       \
        }                                                                               \
      }                                                                                 \
    }                                                                                   \
    if (OB_SUCC(ret)) {                                                                 \
      ret = BUF_PRINTF(")");                                                            \
    } else { /* Do nothing */                                                           \
    }                                                                                   \
  }

/**
 * these operator never generate expr
 */

#define IS_EXPR_PASSBY_OPER(type)                                                  \
  (log_op_def::LOG_GRANULE_ITERATOR == (type) || log_op_def::LOG_LINK == (type) || \
      log_op_def::LOG_EXCHANGE == (type) || log_op_def::LOG_MONITORING_DUMP == (type))

struct FilterCompare {
  FilterCompare(common::ObIArray<ObExprSelPair>& predicate_selectivities)
      : predicate_selectivities_(predicate_selectivities)
  {}
  bool operator()(ObRawExpr* left, ObRawExpr* right)
  {
    return (get_selectivity(left) < get_selectivity(right));
  }
  double get_selectivity(ObRawExpr* expr);

  common::ObIArray<ObExprSelPair>& predicate_selectivities_;
};

class AdjustSortContext {
public:
  bool has_exchange_;
  // count the exchange, in-out will be add 2
  int64_t exchange_cnt_;

  AdjustSortContext() : has_exchange_(false), exchange_cnt_(0)
  {}
};

class AllocGIContext {
public:
  enum GIState {
    GIS_NORMAL = 0,
    GIS_IN_PARTITION_WISE,
    GIS_PARTITION_WITH_AFFINITY,
    GIS_PARTITION,
  };

public:
  explicit AllocGIContext(int64_t parallel)
      : alloc_gi_(false),
        tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
        state_(GIS_NORMAL),
        pw_op_ptr_(nullptr),
        exchange_op_above_count_(0),
        multi_child_op_above_count_in_dfo_(0),
        parallel_(parallel),
        partition_count_(0),
        hash_part_(false),
        slave_mapping_type_(SM_NONE),
        is_del_upd_insert_(false),
        is_valid_for_gi_(false),
        enable_gi_partition_pruning_(false)
  {}
  ~AllocGIContext()
  {
    dfo_table_dop_stack_.reset();
  }
  bool managed_by_gi();
  bool is_in_partition_wise_state();
  bool is_in_pw_affinity_state();
  bool is_partition_gi()
  {
    return GIS_PARTITION == state_;
  };
  void set_in_partition_wise_state(ObLogicalOperator* op_ptr);
  bool try_set_out_partition_wise_state(ObLogicalOperator* op_ptr);
  bool is_op_set_pw(ObLogicalOperator* op_ptr);
  int set_pw_affinity_state();
  void enable_gi_partition_pruning()
  {
    enable_gi_partition_pruning_ = true;
  }
  void reset_info();
  GIState get_state();
  void add_exchange_op_count()
  {
    exchange_op_above_count_++;
  }
  bool exchange_above()
  {
    return 0 != exchange_op_above_count_;
  }
  void delete_exchange_op_count()
  {
    exchange_op_above_count_--;
  }
  void add_multi_child_op_count()
  {
    multi_child_op_above_count_in_dfo_++;
  }
  bool multi_child_op_above()
  {
    return 0 != multi_child_op_above_count_in_dfo_;
  }
  void delete_multi_child_op_count()
  {
    multi_child_op_above_count_in_dfo_--;
  }
  void reset_state()
  {
    state_ = GIS_NORMAL;
  }
  void set_force_partition()
  {
    state_ = GIS_PARTITION;
  }
  bool force_partition()
  {
    return GIS_PARTITION == state_;
  }
  int push_current_dfo_dop(int64_t dop);
  int pop_current_dfo_dop();
  inline bool is_in_slave_mapping()
  {
    return SlaveMappingType::SM_NONE != slave_mapping_type_;
  }
  TO_STRING_KV(K(alloc_gi_), K(tablet_size_), K(state_), K(exchange_op_above_count_));
  bool alloc_gi_;
  int64_t tablet_size_;
  GIState state_;
  ObLogicalOperator* pw_op_ptr_;
  int64_t exchange_op_above_count_;
  int64_t multi_child_op_above_count_in_dfo_;
  int64_t parallel_;
  int64_t partition_count_;
  bool hash_part_;
  SlaveMappingType slave_mapping_type_;
  bool is_del_upd_insert_;
  bool is_valid_for_gi_;
  common::ObSEArray<int64_t, 16> dfo_table_dop_stack_;
  bool enable_gi_partition_pruning_;
};

class ObAllocGIInfo {
public:
  ObAllocGIInfo()
      : state_(AllocGIContext::GIS_NORMAL),
        pw_op_ptr_(nullptr),
        multi_child_op_above_count_in_dfo_(0),
        enable_gi_partition_pruning_(false)
  {}
  TO_STRING_KV(K(state_), K(pw_op_ptr_));
  virtual ~ObAllocGIInfo() = default;
  void set_info(AllocGIContext& ctx)
  {
    state_ = ctx.state_;
    pw_op_ptr_ = ctx.pw_op_ptr_;
    multi_child_op_above_count_in_dfo_ = ctx.multi_child_op_above_count_in_dfo_;
    enable_gi_partition_pruning_ = ctx.enable_gi_partition_pruning_;
  }
  void get_info(AllocGIContext& ctx)
  {
    ctx.state_ = state_;
    ctx.pw_op_ptr_ = pw_op_ptr_;
    ctx.multi_child_op_above_count_in_dfo_ = multi_child_op_above_count_in_dfo_;
    ctx.enable_gi_partition_pruning_ = enable_gi_partition_pruning_;
  }
  AllocGIContext::GIState state_;
  ObLogicalOperator* pw_op_ptr_;
  int64_t multi_child_op_above_count_in_dfo_;
  bool enable_gi_partition_pruning_;
};

class AllocMDContext {
public:
  AllocMDContext() : org_op_id_(0){};
  ~AllocMDContext() = default;
  int64_t org_op_id_;
};

class AllocExchContext {
public:
  enum DistrStat {
    UNINITIALIZED = 0,
    LOCAL,
    REMOTE,
    DISTRIBUTED,
    MATCH_ALL,
  };
  explicit AllocExchContext(int64_t parallel)
      : group_push_down_replaced_exprs_(),
        plan_type_(UNINITIALIZED),
        parallel_(parallel),
        exchange_allocated_(false),
        servers_(),
        weak_part_exprs_(),
        sharding_conds_()
  {}
  virtual ~AllocExchContext()
  {}
  void add_exchange_type(DistrStat status);  // for calc plan type
  DistrStat get_plan_type() const
  {
    return plan_type_;
  }

  TO_STRING_KV(K(plan_type_), K(parallel_), K(servers_));

  common::ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 4, common::ModulePageAllocator, true>
      group_push_down_replaced_exprs_;
  DistrStat plan_type_;
  int64_t parallel_;
  bool exchange_allocated_;
  common::ObSEArray<common::ObAddr, 16> servers_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> weak_part_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> sharding_conds_;
};

struct ObExchangeInfo {
  struct HashExpr {
    HashExpr() : expr_(NULL)
    {}
    HashExpr(ObRawExpr* expr, const ObObjMeta& cmp_type) : expr_(expr), cmp_type_(cmp_type)
    {}

    TO_STRING_KV(K_(expr), K_(cmp_type));

    ObRawExpr* expr_;

    // Compare type of %expr_ when compare with other values.
    // Objects should convert to %cmp_type_ before calculate hash value.
    //
    // Only type_ and cs_type_ of %cmp_type_ are used right now.
    ObObjMeta cmp_type_;
  };
  ObExchangeInfo()
      : is_task_order_(false),
        slice_count_(1),
        repartition_type_(OB_REPARTITION_NO_REPARTITION),
        repartition_ref_table_id_(OB_INVALID_ID),
        repartition_table_name_(),
        calc_part_id_expr_(NULL),
        repartition_keys_(),
        repartition_sub_keys_(),
        repartition_func_exprs_(),
        keep_ordering_(false),
        dist_method_(ObPQDistributeMethod::MAX_VALUE),
        unmatch_row_dist_method_(ObPQDistributeMethod::MAX_VALUE),
        px_dop_(0),
        px_single_(false),
        pdml_pkey_(false),
        slave_mapping_type_(SlaveMappingType::SM_NONE)
  {}
  virtual ~ObExchangeInfo()
  {}
  void reset();
  int clone(ObExchangeInfo& exch_info);
  int set_repartition_info(const common::ObIArray<ObRawExpr*>& repart_keys,
      const common::ObIArray<ObRawExpr*>& repart_sub_keys, const common::ObIArray<ObRawExpr*>& repart_func_exprs);
  int init_calc_part_id_expr(ObOptimizerContext& opt_ctx);
  void set_keep_ordering(bool keep_ordering)
  {
    keep_ordering_ = keep_ordering;
  }
  void set_slave_mapping_type(SlaveMappingType slave_mapping_type)
  {
    slave_mapping_type_ = slave_mapping_type;
  }
  bool is_repart_exchange() const
  {
    return OB_REPARTITION_NO_REPARTITION != repartition_type_;
  }
  bool is_no_repart_exchange() const
  {
    return OB_REPARTITION_NO_REPARTITION == repartition_type_;
  }
  bool is_two_level_repart() const
  {
    return OB_REPARTITION_ONE_SIDE_TWO_LEVEL == repartition_type_;
  }
  bool is_keep_order() const
  {
    return keep_ordering_;
  }
  bool is_task_order() const
  {
    return is_task_order_;
  }
  uint64_t hash(uint64_t seed) const;

  bool is_pq_hash_dist() const
  {
    return ObPQDistributeMethod::HASH == dist_method_;
  }
  bool is_pq_broadcast_dist() const
  {
    return ObPQDistributeMethod::BROADCAST == dist_method_;
  }
  bool is_pq_pkey() const
  {
    return ObPQDistributeMethod::PARTITION == dist_method_;
  }
  bool is_pq_dist() const
  {
    return dist_method_ < ObPQDistributeMethod::MAX_VALUE;
  }
  SlaveMappingType get_slave_mapping_type() const
  {
    return slave_mapping_type_;
  }

  int append_hash_dist_expr(const common::ObIArray<ObRawExpr*>& exprs);

  bool is_task_order_;
  int64_t slice_count_;
  ObRepartitionType repartition_type_;
  int64_t repartition_ref_table_id_;
  ObString repartition_table_name_;  // just for print plan
  ObRawExpr* calc_part_id_expr_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> repartition_keys_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> repartition_sub_keys_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> repartition_func_exprs_;
  bool keep_ordering_;
  common::ObSEArray<HashExpr, 4, common::ModulePageAllocator, true> hash_dist_exprs_;
  ObPQDistributeMethod::Type dist_method_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;

  // degree of parallelism, <= 0 means no restrict
  int64_t px_dop_;
  // PX worker must on single thread
  bool px_single_;
  // pdml pkey
  bool pdml_pkey_;
  // slave mapping exchange
  SlaveMappingType slave_mapping_type_;

  TO_STRING_KV(K_(is_task_order), K_(slice_count), K_(repartition_type), K_(repartition_ref_table_id),
      K_(repartition_table_name), K_(calc_part_id_expr), K_(repartition_keys), K_(repartition_sub_keys),
      K_(hash_dist_exprs), "dist_method", ObPQDistributeMethod::get_type_string(dist_method_), K_(px_dop),
      K_(px_single), K_(repartition_func_exprs), K_(keep_ordering), K_(pdml_pkey), K_(slave_mapping_type));

private:
  DISALLOW_COPY_AND_ASSIGN(ObExchangeInfo);
};

/**
 *  Expr and its producer operator
 */
class ExprProducer {
public:
  ExprProducer()
      : expr_(NULL),
        producer_branch_(common::OB_INVALID_ID),
        consumer_id_(common::OB_INVALID_ID),
        producer_id_(common::OB_INVALID_ID),
        is_shared_(false)
  {}
  ExprProducer(const ObRawExpr* expr, int64_t consumer_id)
      : expr_(expr),
        producer_branch_(common::OB_INVALID_ID),
        consumer_id_(consumer_id),
        producer_id_(common::OB_INVALID_ID),
        is_shared_(false)
  {}

  ExprProducer(const ObRawExpr* expr, int64_t consumer_id, int64_t producer_id)
      : expr_(expr),
        producer_branch_(common::OB_INVALID_ID),
        consumer_id_(consumer_id),
        producer_id_(producer_id),
        is_shared_(false)
  {}

  ExprProducer(const ExprProducer& other)
      : expr_(other.expr_),
        producer_branch_(other.producer_branch_),
        consumer_id_(other.consumer_id_),
        producer_id_(other.producer_id_),
        is_shared_(other.is_shared_)
  {}

  ExprProducer& operator=(const ExprProducer& other)
  {
    expr_ = other.expr_;
    producer_branch_ = other.producer_branch_;
    consumer_id_ = other.consumer_id_;
    producer_id_ = other.producer_id_;
    is_shared_ = other.is_shared_;
    return *this;
  }
  bool not_produced() const
  {
    return common::OB_INVALID_ID == producer_branch_;
  }
  TO_STRING_KV(K_(consumer_id), K_(producer_id), K_(producer_branch), K(is_shared_), KPC_(expr));

  const ObRawExpr* expr_;
  uint64_t producer_branch_;
  uint64_t consumer_id_;
  uint64_t producer_id_;
  bool is_shared_;
};

struct ObAllocExprContext {
  ObAllocExprContext(ObIAllocator& allocator)
      : expr_producers_(), group_replaced_exprs_(), not_produced_exprs_(allocator)
  {}
  ~ObAllocExprContext();

  int set_group_replaced_exprs(common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& exprs)
  {
    return group_replaced_exprs_.assign(exprs);
  }
  int find(const ObRawExpr* expr, ExprProducer*& producer);

  int add(const ExprProducer& producer);

  DISALLOW_COPY_AND_ASSIGN(ObAllocExprContext);

  hash::ObHashMap<uint64_t, int64_t> expr_map_;
  common::ObSEArray<ExprProducer, 4, common::ModulePageAllocator, true> expr_producers_;
  common::ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 4, common::ModulePageAllocator, true> group_replaced_exprs_;
  ObRawExprUniqueSet not_produced_exprs_;
};

struct ObPxPipeBlockingCtx {
  // Look through the pipe (may be blocked by block operator), ended with a exchange operator or not.
  class PipeEnd {
  public:
    PipeEnd() : exch_(false)
    {}
    explicit PipeEnd(const bool is_exch) : exch_(is_exch)
    {}
    void set_exch(bool is_exch)
    {
      exch_ = is_exch;
    }
    bool is_exch() const
    {
      return exch_;
    }
    TO_STRING_KV(K_(exch));

  private:
    bool exch_;
  };

  // %in_ filed of OpCtx include the operator it self.
  // e.g. Opctx of SORT:
  //
  //   EXCH
  //    |
  //   MERGE GBY
  //    |
  //    |   out_: is exch
  //    |<----- viewpoint of OpCtx (above operator)
  //    |   in_: is not exch (blocked by sort operator)
  //    |   has_dfo_below_: true
  //   SORT
  //    |
  //   EXCH
  struct OpCtx {
    OpCtx() : out_(), in_(), has_dfo_below_(false), dfo_depth_(-1)
    {}

    PipeEnd out_;
    PipeEnd in_;

    bool has_dfo_below_;
    int64_t dfo_depth_;

    TO_STRING_KV(K_(in), K_(out), K_(has_dfo_below), K_(dfo_depth));
  };

  explicit ObPxPipeBlockingCtx(common::ObIAllocator& alloc);
  ~ObPxPipeBlockingCtx();

  OpCtx* alloc();
  TO_STRING_KV(K(op_ctxs_));

private:
  common::ObIAllocator& alloc_;
  common::ObArray<OpCtx*> op_ctxs_;
};

typedef common::ObList<common::ObString, common::ObIAllocator> ObStringList;
typedef common::ObList<common::ObString, common::ObIAllocator>::iterator ObStringListIter;
typedef common::ObList<common::ObString, common::ObIAllocator>::const_iterator ObStringListConstIter;
typedef common::ObIArray<common::ObString> ObStringIArray;
typedef common::ObIArray<ObRawExpr*> ObRawExprIArray;
typedef common::ObIArray<OrderItem> ObOrderItemIArray;
typedef common::ObArray<ObRawExpr*> ObRawExprArray;

class GenLinkStmtContext;
class ObLinkStmt {
public:
  explicit ObLinkStmt(common::ObIAllocator& alloc, const ObRawExprIArray& output_exprs)
      : link_ctx_(NULL),
        alloc_(alloc),
        root_output_exprs_(output_exprs),
        select_strs_(alloc),
        from_strs_(alloc),
        where_strs_(alloc),
        groupby_strs_(alloc),
        having_strs_(alloc),
        orderby_strs_(alloc),
        tmp_buf_(NULL),
        tmp_buf_len_(-1),
        is_inited_(false),
        is_distinct_(false)
  {}
  virtual ~ObLinkStmt()
  {}
  int64_t to_string(char* buf, const int64_t buf_len) const;
  /**
   * link op must not have child link op now, even if cluster A has a dblink to B,
   * and cluster B has a dblink to C.
   * sql in cluster A can access cluster C using synonym or view in cluster B,
   * but cluster A can not get schema info from C because he has no privilege.
   * under precondition above, we can use single ObLinkStmt for the traverse
   * operation, even if there are mutli link ops in plan.
   * but we need add some functions, such as init / reset.
   * the pre() of link op should call init(), and post() call reset().
   * other ops will fill relevant member if ObLinkStmt is inited, or
   * do nothing if not inited.
   */
  int init(GenLinkStmtContext* link_ctx);
  void reset();
  bool is_inited() const
  {
    return is_inited_;
  }

  int get_first_table_blank(ObStringList& strs, ObStringListIter& iter) const;
  int gen_stmt_fmt(char* buf, int32_t buf_len, int32_t& pos) const;
  int get_nl_param_columns(ObRawExprIArray& param_exprs, ObRawExprIArray& access_exprs, ObRawExprIArray& param_columns);

  bool is_select_strs_empty() const
  {
    return select_strs_.empty();
  }
  bool is_same_output_exprs_ignore_seq(const ObRawExprIArray& output_exprs);
  int append_nl_param_idx(int64_t param_idx);
  int try_fill_select_strs(const ObRawExprIArray& exprs);
  int force_fill_select_strs();
  int append_select_strs(const ObRawExprIArray& param_columns);
  int set_select_distinct();
  int fill_from_set(ObSelectStmt::SetOperator set_type, bool is_distinct);
  int fill_from_strs(const TableItem& table_item);
  int fill_from_strs(const ObLinkStmt& sub_stmt);
  int fill_from_strs(const ObLinkStmt& sub_stmt, const common::ObString& alias_name);
  int fill_from_strs(ObJoinType join_type, const ObRawExprIArray& join_conditions, const ObRawExprIArray& join_filters,
      const ObRawExprIArray& pushdown_filters);
  int fill_where_strs(const ObRawExprIArray& exprs, bool skip_nl_param = false);
  int fill_where_rownum(const ObRawExpr* expr);
  int fill_groupby_strs(const ObRawExprIArray& exprs);
  int fill_having_strs(const ObRawExprIArray& exprs);
  int fill_orderby_strs(const ObOrderItemIArray& order_items, const ObRawExprIArray& output_exprs);
  int32_t get_total_size() const;

private:
  int fill_exprs(
      const ObRawExprIArray& exprs, const common::ObString& sep, ObStringList& strs, bool skip_nl_param = false);
  int fill_exprs(const ObRawExprIArray& exprs, const common::ObString& sep, ObStringList& strs, ObStringListIter& iter,
      bool skip_nl_param = false);
  int fill_expr(const ObRawExpr* expr, const common::ObString& sep, ObStringList& strs);
  int fill_expr(const ObRawExpr* expr, const common::ObString& sep, ObStringList& strs, ObStringListIter& iter);
  int32_t get_total_size(const ObStringList& str_list, const common::ObString& clause) const;
  int32_t get_total_size(const ObStringIArray& str_array, const common::ObString& clause) const;
  int32_t get_total_size(const common::ObString& str, const common::ObString& clause) const;
  int merge_string_list(
      const ObStringList& str_array, const common::ObString& clause, char* buf, int32_t buf_len, int32_t& pos) const;
  int append_string(
      const common::ObString str, const common::ObString& clause, char* buf, int32_t buf_len, int32_t& pos) const;
  int append_string(const common::ObString str, char* buf, int32_t buf_len, int32_t& pos) const;
  const common::ObString& join_type_str(ObJoinType join_type) const;
  const common::ObString& set_type_str(ObSelectStmt::SetOperator set_type) const;
  const common::ObString& from_clause_str() const;
  int64_t get_order_item_index(const ObRawExpr* order_item_expr, const ObRawExprIArray& output_exprs);
  int expr_in_nl_param(ObRawExpr* expr, bool& in_nl_param);
  int do_expr_in_nl_param(ObRawExpr* expr, bool& in_nl_param);
  int get_nl_param_columns(ObRawExpr* param_expr, ObRawExprIArray& access_exprs, ObRawExprIArray& param_columns);

public:
  GenLinkStmtContext* link_ctx_;
  common::ObIAllocator& alloc_;
  const ObRawExprIArray& root_output_exprs_;
  ObStringList select_strs_;
  // now we only consider join / table scan for form_strs,
  // the algorithm is described in add_from_str(),
  // you can see why we use ObDList for from_strs, not ObArray.
  ObStringList from_strs_;
  // where_strs stores table scan range condition and filters only,
  // the join conditions and filters are stored in from_strs.
  ObStringList where_strs_;
  ObStringList groupby_strs_;
  ObStringList having_strs_;
  ObStringList orderby_strs_;
  common::ObString limit_str_;
  common::ObString offset_str_;
  char* tmp_buf_;
  int32_t tmp_buf_len_;
  bool is_inited_;
  bool is_distinct_;

public:
  static const common::ObString TABLE_BLANK_;
  static const common::ObString JOIN_ON_;
  static const common::ObString LEFT_BRACKET_;
  static const common::ObString RIGHT_BRACKET_;
  static const common::ObString SEP_DOT_;
  static const common::ObString SEP_COMMA_;
  static const common::ObString SEP_AND_;
  static const common::ObString SEP_SPACE_;
  static const common::ObString UNION_ALL_;
  static const common::ObString ORDER_ASC_;
  static const common::ObString ORDER_DESC_;
  static const common::ObString ROWNUM_;
  static const common::ObString LESS_EQUAL_;
  static const common::ObString SELECT_CLAUSE_;
  static const common::ObString SELECT_DIS_CLAUSE_;
  static const common::ObString FROM_CLAUSE_;
  static const common::ObString WHERE_CLAUSE_;
  static const common::ObString GROUPBY_CLAUSE_;
  static const common::ObString HAVING_CLAUSE_;
  static const common::ObString ORDERBY_CLAUSE_;
  static const common::ObString LIMIT_CLAUSE_;
  static const common::ObString OFFSET_CLAUSE_;
  static const common::ObString NULL_STR_;
};

class GenLinkStmtContext {
public:
  GenLinkStmtContext() : dblink_id_(OB_INVALID_ID), link_stmt_(NULL), nl_param_idxs_()
  {}
  uint64_t dblink_id_;
  ObLinkStmt* link_stmt_;
  // param_store index for nl_params.
  ObArray<int64_t> nl_param_idxs_;
};

class Path;
class ObLogPlan;
class ObLogicalOperator {
public:
  friend class ObLogExprValues;
  friend class ObLogFunctionTable;
  typedef common::ObBitSet<common::OB_MAX_BITSET_SIZE> PPDeps;
  /**
   *  Child operator related constant definition
   */
  static const int64_t first_child = 0;
  static const int64_t second_child = 1;
  static const int64_t third_child = 2;
  static const int64_t fourth_child = 3;
  static const int64_t TOPN_LIMIT_COUNT = 30000;
  static constexpr double TOPN_ROWS_RATIO = .2;
  static const int64_t SEQUENTIAL_EXECUTION_THRESHOLD = 1000;

  ObLogicalOperator(ObLogPlan& plan);
  virtual ~ObLogicalOperator();

  /**
   *  Get operator type
   */
  inline log_op_def::ObLogOpType get_type() const
  {
    return type_;
  }

  /**
   *  Set operator type
   */
  inline void set_type(log_op_def::ObLogOpType type)
  {
    type_ = type;
  }

  /**
   *  Get the number of child operators
   */
  inline int64_t get_num_of_child() const
  {
    return child_.count();
  }
  inline int64_t get_child_id() const
  {
    return child_id_;
  }
  inline void set_child_id(const int64_t idx)
  {
    child_id_ = idx;
  }
  /**
   *  Attache to a logical plan tree
   */
  inline void set_plan(ObLogPlan& plan)
  {
    my_plan_ = &plan;
  }
  /**
   *  Attache to a logical plan tree
   */
  inline void set_stmt(ObDMLStmt* stmt)
  {
    stmt_ = stmt;
  }

  /**
   *  Get the logical plan tree
   */
  virtual inline ObLogPlan* get_plan()
  {
    return my_plan_;
  }
  virtual inline const ObLogPlan* get_plan() const
  {
    return my_plan_;
  }

  /**
   *  Get the logical plan tree
   */
  inline ObDMLStmt* get_stmt()
  {
    return stmt_;
  }
  inline const ObDMLStmt* get_stmt() const
  {
    return stmt_;
  }
  /**
   *  Set my parent operator
   */
  inline ObLogicalOperator* get_parent()
  {
    return parent_;
  }

  int get_parent(ObLogicalOperator* root, ObLogicalOperator*& parent);

  virtual int append_not_produced_exprs(ObRawExprUniqueSet& not_produced_exprs) const;
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& not_produced_exprs) const;

  void set_parent(ObLogicalOperator* parent);

  /*
   * set parent based on child relationship
   */
  int adjust_parent_child_relationship();
  /**
   *  Get the cardinality
   */
  inline double get_card() const
  {
    return card_;
  }

  /**
   *  Set the cardinality
   */
  inline void set_card(double card)
  {
    card_ = card;
  }

  inline double get_width() const
  {
    return width_;
  }
  void set_width(double width)
  {
    width_ = width;
  }

  inline int64_t get_plan_depth() const
  {
    return plan_depth_;
  }
  void set_plan_depth(int64_t plan_depth)
  {
    plan_depth_ = plan_depth;
  }

  PxOpSizeFactor get_px_est_size_factor() const
  {
    return px_est_size_factor_;
  }
  PxOpSizeFactor& get_px_est_size_factor()
  {
    return px_est_size_factor_;
  }

  inline ObEstSelInfo* get_est_sel_info()
  {
    return est_sel_info_;
  }

  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
  {
    re_est = false;
    UNUSED(parent);
    UNUSED(need_row_count);
    return OB_SUCCESS;
  }

  /**
   *  Get the output expressions
   */
  inline common::ObIArray<ObRawExpr*>& get_output_exprs()
  {
    return output_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_output_exprs() const
  {
    return output_exprs_;
  }

  /**
   *  Get the filter expressions
   */
  inline common::ObIArray<ObRawExpr*>& get_filter_exprs()
  {
    return filter_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_filter_exprs() const
  {
    return filter_exprs_;
  }

  /**
   *  Get the pushdown filter expressions
   */
  inline common::ObIArray<ObRawExpr*>& get_pushdown_filter_exprs()
  {
    return pushdown_filter_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_pushdown_filter_exprs() const
  {
    return pushdown_filter_exprs_;
  }

  /**
   *  Get the filter expressions
   */
  inline common::ObIArray<ObRawExpr*>& get_startup_exprs()
  {
    return startup_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_startup_exprs() const
  {
    return startup_exprs_;
  }

  /**
   *  Get a specified child operator
   */
  inline ObLogicalOperator* get_child(const int64_t index) const
  {
    return OB_LIKELY(index >= 0 && index < child_.count()) ? child_.at(index) : NULL;
  }

  /**
   *  Set a specified child operator
   */
  void set_child(const int64_t idx, ObLogicalOperator* child_op);

  /**
   *  Add a child operator at the next available position
   */
  int add_child(ObLogicalOperator* child_op);

  /**
   *  Set the first child operator
   */
  void set_left_child(ObLogicalOperator* child_op)
  {
    if (NULL != child_op) {
      child_op->child_id_ = 0;
    }
    set_child(first_child, child_op);
  }

  /**
   *  Set the second child operator
   */
  void set_right_child(ObLogicalOperator* child_op)
  {
    if (NULL != child_op) {
      child_op->child_id_ = 1;
    }
    set_child(second_child, child_op);
  }

  int reset_number_of_parent();
  int aggregate_number_of_parent();
  int need_copy_plan_tree(bool& need_copy);

  /**
   *  Get the optimization cost up to this point
   */
  inline const double& get_cost() const
  {
    return cost_;
  }

  inline double& get_cost()
  {
    return cost_;
  }

  /*
   * get optimization cost for current operator
   */
  inline const double& get_op_cost() const
  {
    return op_cost_;
  }

  inline double& get_op_cost()
  {
    return op_cost_;
  }

  /**
   *  Set the optimization cost
   */
  inline void set_cost(double cost)
  {
    cost_ = cost;
  }

  inline void set_op_cost(double op_cost)
  {
    op_cost_ = op_cost;
  }

  inline void set_branch_id(uint64_t branch_id)
  {
    branch_id_ = branch_id;
  }

  inline void set_id(uint64_t id)
  {
    id_ = id;
  }

  inline uint64_t get_op_id()
  {
    return op_id_;
  }
  inline void set_op_id(uint64_t op_id)
  {
    op_id_ = op_id;
  }
  inline bool is_partition_wise()
  {
    return is_partition_wise_;
  }
  inline void set_is_partition_wise(bool is_partition_wise)
  {
    is_partition_wise_ = is_partition_wise;
  }
  inline bool get_is_block_gi_allowed()
  {
    return is_block_gi_allowed_;
  }
  inline void set_is_block_gi_allowed(bool is_block_gi_allowed)
  {
    is_block_gi_allowed_ = is_block_gi_allowed;
  }

  /*
   * get cur_op's output ordering
   */
  inline common::ObIArray<OrderItem>& get_op_ordering()
  {
    return op_ordering_;
  }

  inline const common::ObIArray<OrderItem>& get_op_ordering() const
  {
    return op_ordering_;
  }

  inline const ObFdItemSet& get_fd_item_set() const
  {
    return NULL == fd_item_set_ ? empty_fd_item_set_ : *fd_item_set_;
  }

  int check_order_unique(bool& order_unique) const;

  void set_est_sel_info(ObEstSelInfo* sel_info)
  {
    est_sel_info_ = sel_info;
  }

  void set_fd_item_set(const ObFdItemSet* other)
  {
    fd_item_set_ = other;
  }

  void set_table_set(const ObRelIds* table_set)
  {
    table_set_ = table_set;
  }

  inline const ObRelIds& get_table_set() const
  {
    return NULL == table_set_ ? empty_table_set_ : *table_set_;
  }

  int set_op_ordering(const common::ObIArray<OrderItem>& op_ordering);

  void reset_op_ordering()
  {
    op_ordering_.reset();
  }

  inline common::ObIArray<OrderItem>& get_local_ordering()
  {
    return op_local_ordering_;
  }

  int set_local_ordering(const common::ObIArray<OrderItem>& op_local_ordering);

  void reset_local_ordering()
  {
    op_local_ordering_.reset();
  }

  int get_ordering_input_equal_sets(EqualSets& ordering_in_eset) const;

  int get_sharding_input_equal_sets(EqualSets& sharding_in_eset) const;

  int get_input_const_exprs(common::ObIArray<ObRawExpr*>& const_exprs) const;

  inline void set_sharding_output_equal_sets(const EqualSets* equal_sets)
  {
    sharding_output_equal_sets_ = equal_sets;
  }
  inline void set_ordering_output_equal_sets(const EqualSets* equal_sets)
  {
    ordering_output_equal_sets_ = equal_sets;
  }
  inline const EqualSets& get_ordering_output_equal_sets() const
  {
    return NULL == ordering_output_equal_sets_ ? empty_expr_sets_ : *ordering_output_equal_sets_;
  }
  inline const EqualSets& get_sharding_output_equal_sets() const
  {
    return NULL == sharding_output_equal_sets_ ? get_ordering_output_equal_sets() : *sharding_output_equal_sets_;
  }
  inline common::ObIArray<ObRawExpr*>& get_output_const_exprs()
  {
    return const_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_output_const_exprs() const
  {
    return const_exprs_;
  }
  /*
   * get cur_op's expected_ordering
   */
  inline common::ObIArray<OrderItem>& get_expected_ordering()
  {
    return expected_ordering_;
  }

  int set_expected_ordering(const common::ObIArray<OrderItem>& expected_ordering);

  inline void set_is_at_most_one_row(bool is_at_most_one_row)
  {
    is_at_most_one_row_ = is_at_most_one_row;
  }
  inline bool get_is_at_most_one_row()
  {
    return is_at_most_one_row_;
  }
  inline bool get_is_at_most_one_row() const
  {
    return is_at_most_one_row_;
  }

  void* get_traverse_ctx() const
  {
    return traverse_ctx_;
  }
  void set_traverse_ctx(void* ctx)
  {
    traverse_ctx_ = ctx;
  }

  // clear plan tree's traverse ctx
  void clear_all_traverse_ctx();
  template <typename Allocator>
  int init_all_traverse_ctx(Allocator& alloc);

  inline int64_t get_interesting_order_info() const
  {
    return interesting_order_info_;
  }
  inline void set_interesting_order_info(int64_t info)
  {
    interesting_order_info_ = info;
  }
  inline bool has_any_interesting_order_info_flag() const
  {
    return interesting_order_info_ > 0;
  }
  /*
   * re set op's ordering according child's ordering
   */
  virtual int transmit_op_ordering();

  /**
   * set whether op is local order
   */
  virtual int transmit_local_ordering();

  /**
   *  Copy an operator without copying its child
   */
  virtual int copy_without_child(ObLogicalOperator*& out) = 0;

  int clone(ObLogicalOperator*& other);

  /**
   *  Set a series of properties for the operator
   */
  virtual int set_properties()
  {
    return common::OB_SUCCESS;
  }

  /**
   *  Do explain collect width operation(pre)
   */
  int explain_collect_width_pre(void* ctx);

  /**
   *  Do explain collect width operation(post)
   */
  int explain_collect_width_post(void* ctx);

  /**
   *  Do explain write buffer operation(pre)
   */
  int explain_write_buffer_pre(void* ctx);

  int explain_index_selection_info_pre(void* ctx);

  /**
   *  Do explain write buffer operation(pre)
   */
  int explain_write_buffer_post(void* ctx);

  /**
   *  Do explain write buffer with output & filter exprs
   */
  int explain_write_buffer_output_pre(void* ctx);

  /**
   *  Do explain write buffer with outline
   */
  int explain_write_buffer_outline_pre(void* ctx);
  void reset_outline_state()
  {
    is_added_outline_ = false;
  }

  /**
   *  Do pre-child-traverse operation
   */
  int do_pre_traverse_operation(const TraverseOp& op, void* ctx);

  /**
   *  Do post-child-traverse operation
   */
  int do_post_traverse_operation(const TraverseOp& op, void* ctx);

  /*
   *  set topn
   */
  int set_sort_topn();
  /**
   *  Rerturn a JSON object of the operator
   *
   *  'buf' is used when calling 'to_string()' internally.
   */
  virtual int to_json(char* buf, const int64_t buf_len, int64_t& pos, json::Value*& ret_val);

  /**
   *  Get predefined operator name
   */
  virtual const char* get_name() const
  {
    return log_op_def::get_op_name(type_);
  }

  /**
   * Get the length of the operator name
   */
  inline virtual int32_t get_explain_name_length() const
  {
    return ((int32_t)strlen(get_name()));
  }

  virtual int get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos)
  {
    int ret = common::OB_SUCCESS;
    ret = BUF_PRINTF("%s", get_name());
    return ret;
  }

  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return common::OB_SUCCESS;
  }

  virtual int explain_index_selection_info(char* buf, int64_t& buf_len, int64_t& pos)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return ret;
  }

  /**
   *  Allocate output expr pre-traverse
   */
  virtual int allocate_expr_pre(ObAllocExprContext& ctx);

  virtual int reordering_project_columns();

  int get_next_producer_id(ObLogicalOperator* node, uint64_t& producer_id);

  int check_param_expr_should_be_added(const ObRawExpr* param_expr, bool& should_add);

  int add_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& exprs);

  int add_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& exprs, uint64_t producer_id);

  int add_exprs_to_ctx(
      ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& exprs, uint64_t producer_id, uint64_t consumer_id);

  int add_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObColumnRefRawExpr*>& exprs);

  int extract_specific_exprs(const ObIArray<ObRawExpr*>& exprs, ObIArray<ExprProducer>* ctx,
      ObIArray<ObRawExpr*>& fix_producer_exprs, ObIArray<ObRawExpr*>& shard_exprs);
  int extract_specific_exprs(ObRawExpr* expr, ObIArray<ExprProducer>* ctx, ObIArray<ObRawExpr*>& fix_producer_exprs,
      ObIArray<ObRawExpr*>& shard_exprs);
  bool is_fix_producer_expr(const ObRawExpr& expr);

  bool is_shared_expr(const ObIArray<ExprProducer>* ctx, const ObRawExpr* expr);

  int find_consumer_id_for_shared_expr(const ObIArray<ExprProducer>* ctx, const ObRawExpr* expr, uint64_t& consumer_id);
  /**
   *  Allocate output expr post-traverse
   *
   *  The default implementation will iterate over the context to see if there are
   *  more exprs that can be produced - ie. the exprs that they depend on have been
   *  produced. An expression will be categorized into filter, join predicates or
   *  output expressions and placed at proper location once it is produced.
   */
  virtual int allocate_expr_post(ObAllocExprContext& ctx);

  int expr_can_be_produced(const ObRawExpr* expr, ObAllocExprContext& gen_expr_ctx, bool& can_be_produced);

  int expr_has_been_produced(const ObRawExpr* expr, ObAllocExprContext& gen_expr_ctx, bool& has_been_produced);

  virtual int compute_const_exprs();

  virtual int compute_equal_set();

  virtual int compute_fd_item_set();

  virtual int deduce_const_exprs_and_ft_item_set(ObFdItemSet& fd_item_set);

  virtual int compute_op_ordering();

  virtual int compute_op_interesting_order_info();

  virtual int compute_table_set();

  virtual int compute_one_row_info();

  virtual int init_est_sel_info();

  virtual int est_cost()
  {
    return OB_SUCCESS;
  }

  /**
   * @brief compute_property
   * convert property fields from a path into a logical operator
   */
  virtual int compute_property(Path* path);

  /**
   * @brief compute_property
   * compute properties of an operator.
   * called by non-terminal operator
   * 1. ordering, 2. unique sets, 3. equal_sets
   * 4. est_sel_info, 5. cost, card, width
   */
  virtual int compute_property();

  bool share_property() const
  {
    return type_ != log_op_def::LOG_EXCHANGE && type_ != log_op_def::LOG_GRANULE_ITERATOR &&
           type_ != log_op_def::LOG_MONITORING_DUMP && type_ != log_op_def::LOG_SORT &&
           type_ != log_op_def::LOG_SUBPLAN_FILTER && type_ != log_op_def::LOG_MATERIAL &&
           type_ != log_op_def::LOG_JOIN_FILTER;
  }

  /*
   * re_calc_cost: children's cost + op_cost
   */
  virtual int re_calc_cost();

  /*
   *  move useless sort operator, set merge sort,task order,prefix sort
   */
  int adjust_sort_operator(AdjustSortContext* adjust_sort_ctx);
  /*
   * set prefix sort, merge, task order
   */
  int inner_adjust_sort_operator(AdjustSortContext* adjust_sort_ctx, bool& need_remove);
  /*
   * remove log sort(current node)
   */
  int remove_sort();

  /**
   *  Add an array of exprs into context
   *
   *  This helper function add all the expressions into an array of ExprProducer
   *  (should be moved to some util files)
   */
  int push_down_limit(AllocExchContext* ctx, ObRawExpr* limit_count_expr, ObRawExpr* limit_offset_expr,
      bool should_push_limit, bool is_fetch_with_ties, ObLogicalOperator*& exchange_point);
  /**
   *  Allocate a pair of exchange nodes above this operator
   */
  int allocate_exchange_nodes_above(bool is_remote, AllocExchContext& ctx, ObExchangeInfo& exch_info);
  /**
   *  Pre-traverse function for allocating granule iterator
   */
  virtual int allocate_granule_pre(AllocGIContext& ctx);
  /**
   *	Post-traverse function for allocating granule iterator
   */
  virtual int allocate_granule_post(AllocGIContext& ctx);
  /**
   *	Allocate granule iterator above this operator
   */
  virtual int allocate_granule_nodes_above(AllocGIContext& ctx);
  /**
   *	Set granule iterator affinity
   */
  virtual int set_granule_nodes_affinity(AllocGIContext& ctx, int64_t child_index);
  /*
   * Allocate gi above all the table scans below this operator.
   */
  int allocate_gi_recursively(AllocGIContext& ctx);
  /**
   * Allocate m dump operator.
   */
  int allocate_monitoring_dump_node_above(uint64_t flags, uint64_t line_id);

  int gen_location_constraint(void* ctx);

  /**
   * Generate a table's location constraint for pdml index maintain op
   */
  int get_tbl_loc_cons_for_pdml_index(LocationConstraint& loc_cons);
  /**
   * Generate a table's location constraint for table scan op
   */
  int get_tbl_loc_cons_for_scan(LocationConstraint& loc_cons);

  /**
   * @brief Generate a table's location constraint for insert op
   */
  int get_tbl_loc_cons_for_insert(LocationConstraint& loc_cons, bool& is_multi_part_dml);

  /**
   *  Allocate a pair of exchange nodes between this operator and the index'th
   *  child
   */
  int allocate_exchange_nodes_below(int64_t index, AllocExchContext& ctx, ObExchangeInfo& exch_info);

  int allocate_grouping_style_exchange_below(AllocExchContext* ctx, common::ObIArray<ObRawExpr*>& partition_exprs);

  int allocate_stmt_order_by_above(ObLogicalOperator*& top);

  int simplify_ordered_exprs(const ObIArray<OrderItem>& candi_sort_key, ObIArray<OrderItem>& simplified_sort_key);

  int simplify_ordered_exprs(const ObIArray<ObRawExpr*>& candi_exprs, ObIArray<ObRawExpr*>& simplified_exprs);

  int simplify_exprs(const ObIArray<ObRawExpr*>& candi_exprs, ObIArray<ObRawExpr*>& simplified_exprs) const;

  int check_need_sort_above_node(const ObIArray<OrderItem>& expected_order_items, bool& need_sort) const;
  int check_need_sort_above_node(const common::ObIArray<ObRawExpr*>& expected_order_exprs,
      const common::ObIArray<ObOrderDirection>* expected_order_directions, bool& need_sort) const;
  int check_need_sort_below_node(
      const int64_t index, const common::ObIArray<OrderItem>& expected_order_items, bool& need);

  // check whether we need allocate sort, mainly for group/distinct clause
  int check_need_sort_for_grouping_op(
      const int64_t index, const common::ObIArray<OrderItem>& order_items, bool& need_sort);
  int check_need_sort_for_grouping_op(const int64_t index, const common::ObIArray<ObRawExpr*>& order_expr,
      const ObIArray<ObOrderDirection>& directions, bool& need_sort);

  int check_if_is_prefix(const common::ObIArray<ObRawExpr*>* order_exprs,
      const common::ObIArray<OrderItem>* order_items, ObLogicalOperator* op, bool& is_prefix);
  // whether operator keep input(child at idx) ordering
  //@param[out]: keep which idx child's ordering.
  // and it's special for granule iterator
  bool is_keep_input_ordering();

  int get_table_scan(ObLogicalOperator*& tsc, uint64_t table_id);

  int get_order_item_by_plan(ObIArray<OrderItem>& order_items);

  // get operator ordering with multi-partitions
  // If no parts ordering, return NULL. This is used for check whether sort needed.
  static const common::ObIArray<OrderItem>* get_op_ordering_in_parts(ObLogicalOperator* op);

  int check_and_allocate_material(const int64_t index);

  /**
   * Check if a exchange need rescan is below me.
   */
  int check_exchange_rescan(bool& need_rescan);

  /**
   * Check if has exchange below.
   */
  int check_has_exchange_below(bool& has_exchange) const;
  /**
   *  Post-traverse function for allocating output expressions
   */
  virtual int allocate_exchange_post(AllocExchContext* ctx) = 0;
  int reselect_duplicate_table_replica();
  virtual int get_duplicate_table_replica(ObIArray<ObAddr>& valid_addrs);
  int set_duplicate_table_replica(const ObIArray<ObAddr>& addrs);
  virtual int set_duplicate_table_replica(const ObAddr& addr);
  int should_use_sequential_execution(bool& use_seq_exec) const;

  int check_if_match_repart(const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& src_join_key,
      const common::ObIArray<ObRawExpr*>& target_join_key, const ObLogicalOperator& target_child, bool& is_match);
  int check_is_table_scan(const ObLogicalOperator& op, bool& is_table_scan);

  int get_repartition_table_info(
      ObLogicalOperator& op, ObString& table_name, uint64_t& table_id, uint64_t& ref_table_id);

  int compute_sharding_and_allocate_exchange(AllocExchContext* ctx, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& hash_left_keys, const ObIArray<ObRawExpr*>& hash_right_keys,
      const ObIArray<ObExprCalcType>& hash_calc_types, const ObIArray<ObRawExpr*>& left_keys,
      const ObIArray<ObRawExpr*>& right_keys, ObLogicalOperator& left_child, ObLogicalOperator& right_child,
      const JoinDistAlgo best_method, const SlaveMappingType slave_mapping_type, const ObJoinType join_type,
      ObShardingInfo& sharding_info);

  int update_sharding_conds(ObIArray<ObRawExpr*>& sharding_conds, const JoinDistAlgo best_method,
      const ObJoinType join_type, const ObExchangeInfo& left_exch_info, const ObExchangeInfo& right_exch_info);

  int compute_repartition_distribution_info(const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& src_keys,
      const ObIArray<ObRawExpr*>& target_keys, ObLogicalOperator& target_child, ObExchangeInfo& exch_info,
      ObShardingInfo& sharding_info);

  int compute_hash_distribution_info(const ObIArray<ObRawExpr*>& left_keys, const ObIArray<ObRawExpr*>& right_keys,
      const ObIArray<ObExprCalcType>& calc_types, ObExchangeInfo& l_exch_info, ObExchangeInfo& r_exch_info,
      ObShardingInfo& sharding_info);

  int compute_repartition_func_info(const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& src_join_key,
      const common::ObIArray<ObRawExpr*>& target_join_key, const ObShardingInfo& target_sharding,
      ObRawExprFactory& expr_factory, ObExchangeInfo& exch_info);

  int get_repartition_keys(const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& src_keys,
      const common::ObIArray<ObRawExpr*>& target_keys, const common::ObIArray<ObRawExpr*>& target_part_keys,
      common::ObIArray<ObRawExpr*>& src_part_keys);

  int choose_best_distribution_method(
      AllocExchContext& ctx, uint64_t method, bool sm_hint, JoinDistAlgo& best_method, SlaveMappingType& sm_type) const;
  int should_use_slave_mapping(bool& enable_sm) const;
  int get_hash_hash_cost(double& cost) const;
  int get_broadcast_cost(int64_t child_idx, AllocExchContext& ctx, double& cost) const;
  int get_repartition_cost(int64_t child_idx, double& cost) const;
  int get_b2host_cost(int64_t child_idx, AllocExchContext& ctx, double& cost) const;

  virtual int allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info);

  // Operator is block operator if outputting data after all children's data processed.
  //   block operator: group by, sort, material ...
  //   non block operator: limit, join, ...
  virtual bool is_block_op() const
  {
    return false;
  }
  // Non block operator may start outputting data after some children's data processed,
  // is_block_input() indicate those children.
  // e.g.: first child of hash join is block input, second child is not block input.
  virtual bool is_block_input(const int64_t child_idx) const
  {
    UNUSED(child_idx);
    return is_block_op();
  }

  virtual bool is_consume_child_1by1() const
  {
    return false;
  }

  // PX pipe blocking phase add material operator to DFO, to make sure DFO can be scheduled
  // in consumer/producer threads model. top-down stage markup the output side has block
  // operator or not. bottom-up stage markup the input side and add material operator in the
  // following case:
  //   1. operator got more then one input with out block operator.
  //   2. no block operator between two DFO.
  //
  // Notice: We do not mark block operator directly in traverse context, we marking whether
  // data can streaming to an exchange operator instead.
  virtual int px_pipe_blocking_pre(ObPxPipeBlockingCtx& ctx);
  virtual int px_pipe_blocking_post(ObPxPipeBlockingCtx& ctx);

  // generate local order plan for merge sort receive operator, but for top exchange, exchang-in is exchange for coord
  // so it need to forbit generating local order plan
  // pre function to tag top exchange flag and post function to remove tag top exchange flag
  virtual int set_exchange_cnt_pre(AdjustSortContext* ctx);
  virtual int set_exchange_cnt_post(AdjustSortContext* ctx);
  virtual int allocate_link_post();
  int allocate_link_node_above(int64_t child_idx);

  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx);

  /**
   *  Start plan tree traverse
   *
   *  For each traverse operation, we provide a opportunity of operation both
   *  before and after accessing the children. This should be sufficient to
   *  support both top-down and bottom-up traversal.
   */
  int do_plan_tree_traverse(const TraverseOp& operation, void* ctx);

  int should_allocate_gi_for_dml(bool& is_valid);
  /**
   *  Get the operator id
   */
  uint64_t get_operator_id() const
  {
    return id_;
  }

  /**
   *  Mark this operator as the root of the plan
   */
  void mark_is_plan_root()
  {
    is_plan_root_ = true;
  }

  void set_is_plan_root(bool is_root)
  {
    is_plan_root_ = is_root;
  }

  /**
   *  Check if this operator is the root of a logical plan
   */
  bool is_plan_root() const
  {
    return is_plan_root_;
  }

  /**
   *  Mark an expr as 'produced'
   */
  int mark_expr_produced(
      ObRawExpr* expr, uint64_t branch_id, uint64_t producer_id, ObAllocExprContext& gen_expr_ctx, bool& found);
  /**
   *  Get the operator's hash value
   */
  virtual uint64_t hash(uint64_t seed) const;
  /**
   *  Generate hash value for output exprs
   */
  inline uint64_t hash_output_exprs(uint64_t seed) const
  {
    return ObOptimizerUtil::hash_exprs(seed, output_exprs_);
  }

  /**
   *  Generate hash value for filter exprs
   */
  inline uint64_t hash_filter_exprs(uint64_t seed) const
  {
    return ObOptimizerUtil::hash_exprs(seed, filter_exprs_);
  }

  inline uint64_t hash_pushdown_filter_exprs(uint64_t seed) const
  {
    return ObOptimizerUtil::hash_exprs(seed, pushdown_filter_exprs_);
  }

  inline uint64_t hash_startup_exprs(uint64_t seed) const
  {
    return ObOptimizerUtil::hash_exprs(seed, startup_exprs_);
  }

  /**
   *  Add an expr to output_exprs_
   */
  int add_expr_to_output(const ObRawExpr* expr);

  /**
   *  Add all select expr to the output of the root operator
   */
  int add_plan_root_exprs(ObAllocExprContext& ctx);
  /**
   *  Get operator's sharding information
   */
  ObShardingInfo& get_sharding_info()
  {
    return sharding_info_;
  }
  const ObShardingInfo& get_sharding_info() const
  {
    return sharding_info_;
  }

  bool is_dml_operator() const
  {
    return (log_op_def::LOG_UPDATE == type_ || log_op_def::LOG_DELETE == type_ || log_op_def::LOG_INSERT == type_ ||
            log_op_def::LOG_INSERT_ALL == type_);
  }
  bool is_duplicated_checker_op() const
  {
    return log_op_def::LOG_CONFLICT_ROW_FETCHER == type_;
  }
  bool is_expr_operator() const
  {
    return (log_op_def::LOG_EXPR_VALUES == type_ || log_op_def::LOG_VALUES == type_);
  }
  /*
   * replace agg expr generated by group by push down during allocating exchange
   */
  int replace_generated_agg_expr(const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs);
  // set merge_sort for exchange
  int set_merge_sort(ObLogicalOperator* op, AdjustSortContext* adjust_sort_ctx, bool& need_remove);

  // check if this operator is some kind of table scan.
  // the default return value is false
  // Note: The default table scan operator ObLogTableScan overrides this
  //       method to return true.
  //       IT'S RECOMMENDED THAT any table scan operator derive from ObLogTableScan,
  //       or you MAY NEED TO CHANGE EVERY CALLER of this method.
  virtual bool is_table_scan() const
  {
    return false;
  }

  int allocate_material(const int64_t index);
  bool is_scan_operator(log_op_def::ObLogOpType type);

  // Find the first operator through the child operators recursively
  // Found: return OB_SUCCESS, set %op
  // Not found: return OB_SUCCESS, set %op to NULL
  int find_first_recursive(const log_op_def::ObLogOpType type, ObLogicalOperator*& op);
  // the operator use these interfaces to allocate gi
  int pw_allocate_granule_pre(AllocGIContext& ctx);
  int pw_allocate_granule_post(AllocGIContext& ctx);

  int allocate_distinct_below(int64_t index, ObIArray<ObRawExpr*>& distinct_exprs, const AggregateAlgo algo);

  int allocate_sort_below(const int64_t index, const ObIArray<OrderItem>& order_keys);

  int allocate_topk_below(const int64_t index, ObSelectStmt* stmt);

  int allocate_material_below(const int64_t index);

  int allocate_limit_below(const int64_t index, ObRawExpr* limit_expr);

  int allocate_group_by_below(
      int64_t index, ObIArray<ObRawExpr*>& group_by_exprs, const AggregateAlgo algo, const bool from_pivot);

  int check_match_remote_sharding(const ObAddr& server, bool& is_match) const;

  static int check_match_server_addr(const ObPhyTableLocationInfo& phy_loc_info, const ObAddr& server, bool& is_match);

  int print_operator_for_use_join_filter(planText& plan_text);

  int compute_sharding_equal_sets(const ObIArray<ObRawExpr*>& sharding_conds);

  int prune_weak_part_exprs(
      const AllocExchContext& ctx, const ObIArray<ObRawExpr*>& input_exprs, ObIArray<ObRawExpr*>& output_exprs);

  int prune_weak_part_exprs(const AllocExchContext& ctx, const ObIArray<ObRawExpr*>& left_input_exprs,
      const ObIArray<ObRawExpr*>& right_input_exprs, ObIArray<ObRawExpr*>& left_output_exprs,
      ObIArray<ObRawExpr*>& right_output_exprs);

  bool check_weak_part_expr(const ObRawExpr* expr, const ObIArray<ObRawExpr*>& weak_part_exprs);

  int compute_basic_sharding_info(AllocExchContext* ctx, bool& is_basic);
  int compute_recursive_cte_sharding_info(AllocExchContext* ctx, ObLogicalOperator* op, bool& is_basic);
  int check_contain_fake_cte_table(ObLogicalOperator* child, bool& contain_rcte);
  static int compute_basic_sharding_info(
      ObIArray<ObShardingInfo*>& input_shardings, ObShardingInfo& output_sharding, bool& is_basic);
  int gen_calc_part_id_expr(uint64_t table_id, uint64_t ref_table_id, ObRawExpr*& expr);

  // dblink
  void set_dblink_id(uint64_t dblink_id)
  {
    dblink_id_ = dblink_id;
  }
  uint64_t get_dblink_id() const
  {
    return dblink_id_;
  }

  int check_is_uncertain_plan();

  int get_part_exprs(uint64_t table_id, uint64_t ref_table_id, share::schema::ObPartitionLevel& part_level,
      ObRawExpr*& part_expr, ObRawExpr*& subpart_expr);

  int check_fulfill_cut_ratio_condition(int64_t dop, double ndv, bool& is_fulfill);

public:
  /* child operators */
  ObSEArray<ObLogicalOperator*, 16, common::ModulePageAllocator, true> child_;
  // ObLogicalOperator *child_[max_num_of_child];

protected:
  enum TraverseType { JOIN_METHOD, MATERIAL_NL, LEADING, PQ_DIST, PQ_MAP };
  enum JoinTreeType { INVALID_TYPE, LEFT_DEEP, RIGHT_DEEP, BUSHY };

  /**
   *  Check if all expresionss are produced by some operator.
   */
  int all_expr_produced(common::ObIArray<ExprProducer>* ctx, bool& all_produced);

  /**
   *  Print the footer of the explain plan table
   */
  int print_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

  /**
   *  Interface for operators to print their specific info in the plan table footer
   */
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    UNUSED(type);
    return ret;
  }

  /**
   *  Interface for operators to print their specific info in the plan table footer
   */
  virtual int print_plan_head_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    UNUSED(type);
    return ret;
  }

  //    virtual int print_outline(char *buf, int64_t &buf_len, int64_t &pos, bool is_oneline)
  virtual int print_outline(planText& plan)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(plan);
    return ret;
  }

  virtual int print_operator_for_outline(planText& plan)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(plan);
    return ret;
  }

  virtual int is_used_join_type_hint(JoinAlgo join_algo, bool& is_used)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(join_algo);
    UNUSED(is_used);
    return ret;
  }

  virtual int is_used_in_leading_hint(bool& is_used)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(is_used);
    return ret;
  }

  int traverse_join_plan_ldr(planText& plan_text, TraverseType traverse_type, JoinTreeType join_tree_type);
  int traverse_join_plan_pre(
      planText& plan_text, TraverseType traverse_type, JoinTreeType join_tree_type, bool is_need_print = true);
  int print_qb_name(planText& plan_text);

  /**
   *  Allocate dummy access if the current output is empty
   */
  int allocate_dummy_access();

  /**
   *  Allocate dummy output if the current output is empty
   */
  virtual int allocate_dummy_output();

  /**
   *  Allocate dummy output and access if the current output is empty
   */
  int allocate_dummy_output_access();

  int project_pruning_pre();
  void project_pruning(common::ObIArray<ObRawExpr*>& exprs, PPDeps& deps, const char* reason = nullptr);

  int check_output_dep_common(ObRawExprCheckDep& checker);
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker)
  {
    (void)(checker);
    return common::OB_SUCCESS;
  }
  virtual int check_output_dep(common::ObIArray<ObRawExpr*>& child_output, PPDeps& deps);

  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs);
  int replace_exprs_action(const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs,
      common::ObIArray<ObRawExpr*>& dest_exprs);
  int replace_expr_action(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs, ObRawExpr*& dest_expr);
  int make_order_keys(common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<OrderItem>& items);

  static int explain_print_partitions(
      const common::ObIArray<int64_t>& partitions, const bool two_level, char* buf, int64_t& buf_len, int64_t& pos);

  int check_sharding_compatible_with_reduce_expr(const ObShardingInfo& sharding,
      const common::ObIArray<ObRawExpr*>& reduce_exprs, const EqualSets& equal_sets, bool& compatible) const;

  int get_percentage_value(ObRawExpr* percentage_expr, double& percentage, bool& is_null_value);

  int check_need_sort_for_local_order(const int64_t index, const common::ObIArray<OrderItem>* order_items, bool& need);

protected:
  int alloc_partition_id_expr(uint64_t table_id, ObAllocExprContext& ctx, ObPseudoColumnRawExpr*& partition_id_expr);

  void add_join_dist_flag(uint64_t& flags, JoinDistAlgo method) const
  {
    flags |= method;
  }

  void remove_join_dist_flag(uint64_t& flags, JoinDistAlgo method) const
  {
    flags &= ~method;
  }

  bool has_join_dist_flag(uint64_t flags, JoinDistAlgo method) const
  {
    return !!(flags & method);
  }

  void clear_join_dist_flag(uint64_t& flags) const
  {
    flags = 0;
  }

protected:
  log_op_def::ObLogOpType type_;
  ObLogPlan* my_plan_;                                                                // the entry point of the plan
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> output_exprs_;  // expressions produced
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> filter_exprs_;  // filtering exprs.
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> pushdown_filter_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> startup_exprs_;  // startup filtering exprs.
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> const_exprs_;

  const EqualSets* ordering_output_equal_sets_;
  const EqualSets* sharding_output_equal_sets_;
  const ObFdItemSet* fd_item_set_;
  const ObRelIds* table_set_;
  ObEstSelInfo* est_sel_info_;

  ObDMLStmt* stmt_;
  uint64_t id_;  // operator 0-based depth-first id
  uint64_t branch_id_;
  uint64_t op_id_;

private:
  /**
   *  Numbering the operator
   */
  int numbering_operator_pre(NumberingCtx& ctx);
  void numbering_operator_post(NumberingCtx& ctx);
  /**
   *  allocate md operator
   */
  int alloc_md_post(AllocMDContext& ctx);
  /**
   * Numbering px transmit op with dfo id
   * for Explain display
   */
  int numbering_exchange_pre(NumberingExchangeCtx& ctx);
  int numbering_exchange_post(NumberingExchangeCtx& ctx);
  /**
   * Estimate size level for different operator when px
   */
  int px_estimate_size_factor_pre();
  int px_estimate_size_factor_post();
  /**
   * for px rescan function
   */
  int px_rescan_pre();
  int mark_child_exchange_rescanable();
  int check_subplan_filter_child_exchange_rescanable();

  bool has_expr_as_input(const ObRawExpr* expr) const;

  /**
   *  Re-order the filter expressions according to their selectivity
   *
   *  Re-order the filter expression vector so that the more selective predicate will
   *  be evaluated earlier.
   */
  int reorder_filter_exprs();
  int is_need_print_join_method(const planText& plan_text, JoinAlgo join_algo, bool& is_need);
  int is_need_print_operator_for_leading(const planText& plan_text, bool& is_need);
  int print_operator_for_use_join(planText& plan_text);
  int print_operator_for_leading(planText& plan_text);
  int inner_set_merge_sort(ObLogicalOperator* producer, ObLogicalOperator* consumer, ObLogicalOperator* op_sort,
      bool& need_remove, bool global_order);

  // private function, just used for allocating join filter node.
  int check_allocate_bf_condition(bool& allocate);
  int set_join_condition_to_bf(ObLogicalOperator* filter, bool is_create);
  // get the table dop for this dfo
  int calc_current_dfo_table_dop(ObLogicalOperator* root, int64_t& table_dop, bool& found_base_table);

private:
  int64_t child_id_;           // parent child
  ObLogicalOperator* parent_;  // parent operator
  bool is_plan_root_;          // plan root operator
protected:
  double cost_;     // cost up to this point
  double op_cost_;  // cost for this operator
  double card_;     // cardinality
  double width_;    // average row width
  ObShardingInfo sharding_info_;
  bool is_added_outline_;  // used for outline
  bool is_added_leading_outline_;
  bool is_added_leading_hints_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> expected_ordering_;
  // temporary traverse context pointer, maybe modified in every plan_tree_traverse
  void* traverse_ctx_;
  ObPwjConstraint strict_pwj_constraint_;
  ObPwjConstraint non_strict_pwj_constraint_;
  bool is_partition_wise_;
  bool is_block_gi_allowed_;
  PxOpSizeFactor px_est_size_factor_;
  // dblink
  uint64_t dblink_id_;
  int64_t plan_depth_;

private:
  // unify to use get, set and reset function instead of using op_ordering_
  bool is_at_most_one_row_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> op_ordering_;
  // denotes current op will deliver local order data
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> op_local_ordering_;
  const ObRawExprSets& empty_expr_sets_;
  const ObFdItemSet& empty_fd_item_set_;
  const ObRelIds& empty_table_set_;
  /*
   * number of parent node during the optimization of first-phase
   * this variable is for special purpose, do not use this variable for other cases
   */
  int64_t num_of_parent_;
  int64_t interesting_order_info_;
};

template <typename Allocator>
int ObLogicalOperator::init_all_traverse_ctx(Allocator& alloc)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_OPT_LOG(WARN, "NULL child", K(ret));
    } else if (OB_FAIL(get_child(i)->init_all_traverse_ctx(alloc))) {
      SQL_OPT_LOG(WARN, "init all traverse ctx failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(traverse_ctx_ = alloc.alloc())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_OPT_LOG(WARN, "allocate traverse context failed", K(ret));
    }
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOGICAL_OPERATOR_H
