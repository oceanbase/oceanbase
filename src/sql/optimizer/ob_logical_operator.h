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
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_raw_expr_check_dep.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_fd_item.h"
#include "sql/monitor/ob_sql_plan.h"
#include "sql/monitor/ob_plan_info_manager.h"
#include "sql/optimizer/ob_px_resource_analyzer.h"

namespace oceanbase
{
namespace sql
{
struct JoinFilterInfo;
struct EstimateCostInfo;
struct ObSqlPlanItem;
struct PlanText;
class ObPxResourceAnalyzer;
struct partition_location
{
  int64_t partition_id;
  // server id
  TO_STRING_KV(K(partition_id));
};

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_EXPRS(exprs, type)                                                    \
{                                                                                           \
  int64_t N = -1;                                                                           \
  if (OB_FAIL(ret)) { /* Do nothing */                                                      \
  } else if (OB_FAIL(BUF_PRINTF(#exprs"("))) { /* Do nothing */                             \
  } else if (FALSE_IT(N = exprs.count())) { /* Do nothing */                                \
  } else if (N == 0) {                                                                      \
    ret = BUF_PRINTF("nil");                                                                \
  } else {                                                                                  \
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                                       \
      if(OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                                       \
      } else if (OB_ISNULL(exprs.at(i))) {                                                  \
        ret = OB_ERR_UNEXPECTED;                                                            \
      } else if (OB_FAIL(exprs.at(i)                                                        \
                         ->get_name(buf, buf_len, pos, type))) { /*Do nothing */            \
      } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                               \
      } else if (i < N - 1) {                                                               \
        ret = BUF_PRINTF(", ");                                                             \
      } else { /* Do nothing */ }                                                           \
    }                                                                                       \
  }                                                                                         \
  if (OB_SUCC(ret)) { /* Do nothing */                                                      \
    ret = BUF_PRINTF(")");                                                                  \
  } else { /* Do nothing */ }                                                               \
}

#define EXPLAIN_PRINT_EXPR(expr, type)                                                      \
{                                                                                           \
  if (OB_FAIL(ret)) { /* Do nothing */                                                      \
  } else if (OB_FAIL(BUF_PRINTF(#expr"("))) { /* Do nothing */                              \
  } else if (OB_ISNULL(expr)) {                                                             \
    ret = BUF_PRINTF("nil");                                                                \
  } else if (OB_FAIL(expr->get_name(buf, buf_len, pos, type))) {                            \
    LOG_WARN("print expr name failed", K(ret));                                             \
  } else { /*Do nothing*/ }                                                                 \
  if (OB_SUCC(ret)) { /* Do nothing */                                                      \
    ret = BUF_PRINTF(")");                                                                  \
  } else { /* Do nothing */ }                                                               \
}

/**
 *  Print log info with exec params
 */
#define EXPLAIN_PRINT_EXEC_EXPRS(exec_params, type)                         \
{                                                                           \
  int64_t N = -1;                                                           \
  if (OB_FAIL(ret)) { /* Do nothing */                                      \
  } else if (OB_FAIL(BUF_PRINTF(#exec_params"("))) { /* Do nothing */       \
  } else if (FALSE_IT(N = exec_params.count())) { /* Do nothing */          \
  } else if (N == 0) {                                                      \
    ret = BUF_PRINTF("nil");                                                \
  } else {                                                                  \
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                       \
      if (OB_ISNULL(exec_params.at(i))) {                                   \
        ret = common::OB_ERR_UNEXPECTED;                                    \
      } else if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */               \
      } else if (OB_ISNULL(exec_params.at(i)->get_ref_expr())) {            \
        ret = OB_ERR_UNEXPECTED;                                            \
      } else if (OB_FAIL(exec_params.at(i)->get_ref_expr()                  \
                         ->get_name(buf, buf_len, pos, type))) {            \
      } else if (OB_FAIL(BUF_PRINTF("("))) { /* Do nothing */               \
      } else if (OB_FAIL(exec_params.at(i)                                  \
                         ->get_name(buf, buf_len, pos, type))) {            \
      } else if (OB_FAIL(BUF_PRINTF(")"))) { /* Do nothing */               \
      } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */               \
      } else if (i < N - 1) {                                               \
        ret = BUF_PRINTF(", ");                                             \
      } else { /* Do nothing */ }                                           \
    }                                                                       \
  }                                                                         \
  if (OB_SUCC(ret)) {                                                       \
    ret = BUF_PRINTF(")");                                                  \
  } else { /* Do nothing */ }                                               \
}

/**
 *  Print log info with idxs
 */
#define EXPLAIN_PRINT_IDXS(idxs)                                                \
{                                                                               \
  ObSEArray<int64_t, 4, common::ModulePageAllocator, true> arr;                 \
  int64_t N = -1;                                                               \
  if (OB_FAIL(ret)) { /* Do nothing */                                          \
  } else if (OB_FAIL(BUF_PRINTF(#idxs"("))) { /* Do nothing */                  \
  } else if (OB_FAIL(idxs.to_array(arr))) { /* Do nothing */                    \
  } else if (FALSE_IT(N = arr.count())) { /* Do nothing */                      \
  } else if (N == 0) {                                                          \
    ret = BUF_PRINTF("nil");                                                    \
  } else {                                                                      \
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                           \
      if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                          \
      } else if (OB_FAIL(BUF_PRINTF("%lu", arr.at(i)))) { /* Do nothing */      \
      } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                   \
      } else if (i < N - 1) {                                                   \
        ret = BUF_PRINTF(", ");                                                 \
      } else { /* Do nothing */ }                                               \
    }                                                                           \
  }                                                                             \
  if (OB_SUCC(ret)) { /* Do nothing */                                          \
    ret = BUF_PRINTF(")");                                                      \
  } else { /* Do nothing */ }                                                   \
}

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_MERGE_DIRECTIONS(directions)                                \
{                                                                                 \
  int64_t N = -1;                                                                 \
  if (OB_FAIL(ret)) { /* Do nothing */                                            \
  } else if (OB_FAIL(BUF_PRINTF(#directions"("))) { /* Do nothing */              \
  } else if (FALSE_IT(N = directions.count())) { /* Do nothing */                 \
  } else if (N == 0) {                                                            \
    ret = BUF_PRINTF("nil");                                                      \
  } else {                                                                        \
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                             \
      if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                            \
      } else if (is_descending_direction(directions.at(i))) {                     \
        ret = BUF_PRINTF("DESC");                                                 \
      } else {                                                                    \
        ret = BUF_PRINTF("ASC");                                                  \
      }                                                                           \
      if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */                            \
      } else if (i < N - 1) {                                                     \
        ret = BUF_PRINTF(", ");                                                   \
      } else { /* Do nothing */ }                                                 \
    }                                                                             \
  }                                                                               \
  if (OB_SUCC(ret)) {                                                             \
    ret = BUF_PRINTF(")");                                                        \
  } else { /* Do nothing */ }                                                     \
}

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type)                         \
{                                                                         \
  int64_t N = -1;                                                         \
  if (OB_FAIL(ret)) { /* Do nothing */                                    \
  } else if (OB_FAIL(BUF_PRINTF(#sort_keys"("))) { /* Do nothing */       \
  } else if (FALSE_IT(N = sort_keys.count())) { /* Do nothing */          \
  } else if (0 == N) {                                                    \
    ret = BUF_PRINTF("nil");                                              \
  } else {                                                                \
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                     \
      if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                    \
      } else if(OB_ISNULL(sort_keys.at(i).expr_)) {                       \
        ret = OB_ERR_UNEXPECTED;                                          \
      } else if (OB_FAIL(sort_keys.at(i).expr_                            \
                         ->get_name(buf, buf_len, pos, type))) {          \
        LOG_WARN("fail to get name", K(i), K(ret));                       \
      } else if (OB_FAIL(BUF_PRINTF(", "))) { /* Do nothing */            \
      } else if (is_ascending_direction(sort_keys.at(i).order_type_)) {   \
        ret = BUF_PRINTF("ASC");                                          \
      } else {                                                            \
        ret = BUF_PRINTF("DESC");                                         \
      }                                                                   \
      if (OB_FAIL(ret)) { /* Do nothing */                                \
      } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */             \
      } else if(i < N - 1) {                                              \
        ret = BUF_PRINTF(", ");                                           \
      } else { /* Do nothing */ }                                         \
    }                                                                     \
  }                                                                       \
  if (OB_SUCC(ret)) {                                                     \
    ret = BUF_PRINTF(")");                                                \
  } else { /* Do nothing */ }                                             \
}

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_POPULAR_VALUES(popular_values)                      \
{                                                                         \
  int64_t N = -1;                                                         \
  if (OB_FAIL(ret)) { /* Do nothing */                                    \
  } else if (OB_FAIL(BUF_PRINTF("popular_values("))) { /* Do nothing */   \
  } else if (FALSE_IT(N = popular_values.count())) { /* Do nothing */     \
  } else if (0 == N) {                                                    \
    ret = BUF_PRINTF("nil");                                              \
  } else {                                                                \
    if (OB_FAIL(BUF_PRINTF("["))) { /* Do nothing */                      \
    }                                                                     \
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {                     \
      if (OB_FAIL(popular_values.at(i).print_sql_literal(                 \
                  buf, buf_len, pos))) {                                  \
        LOG_WARN("fail to get name", K(i), K(ret));                       \
      }                                                                   \
      if (OB_FAIL(ret)) { /* Do nothing */                                \
      } else if(i < N - 1) {                                              \
        ret = BUF_PRINTF(",");                                            \
      } else { /* Do nothing */ }                                         \
    }                                                                     \
    if (OB_FAIL(ret)) { /* Do nothing */                                  \
    } else if (OB_FAIL(BUF_PRINTF("]"))) { /* Do nothing */               \
    }                                                                     \
  }                                                                       \
  if (OB_SUCC(ret)) {                                                     \
    ret = BUF_PRINTF(")");                                                \
  } else { /* Do nothing */ }                                             \
}

/**
 * these operator never generate expr
 */

#define IS_EXPR_PASSBY_OPER(type) (log_op_def::LOG_GRANULE_ITERATOR == (type)    \
                                   || log_op_def::LOG_MONITORING_DUMP == (type)) \

/**
 * these operator never generate expr except for its fixed expr
 */
#define IS_OUTPUT_EXPR_PASSBY_OPER(type) (log_op_def::LOG_SORT == (type))

struct FilterCompare
{
  FilterCompare(common::ObIArray<ObExprSelPair> &predicate_selectivities)
      : predicate_selectivities_(predicate_selectivities)
  {
  }
  bool operator()(ObRawExpr *left, ObRawExpr *right)
  {
    return (get_selectivity(left) < get_selectivity(right));
  }
  double get_selectivity(ObRawExpr *expr);

  common::ObIArray<ObExprSelPair> &predicate_selectivities_;
};

typedef std::pair<double, ObRawExpr *> ObExprRankPair;

struct ObExprRankPairCompare
{
  ObExprRankPairCompare() {};
  bool operator()(ObExprRankPair &left, ObExprRankPair &right)
  {
    return left.first < right.first;
  }
};

class AdjustSortContext
{
 public:
  bool has_exchange_;
  // count the exchange, in-out will be add 2
  int64_t exchange_cnt_;

  AdjustSortContext()
      : has_exchange_(false), exchange_cnt_(0)
  {}
};

class AllocGIContext
{
public:
  enum GIState {
    GIS_NORMAL = 0,
    GIS_IN_PARTITION_WISE,
    GIS_PARTITION_WITH_AFFINITY,
    GIS_PARTITION,
    GIS_AFFINITY,
  };
public:
  explicit AllocGIContext() :
  	alloc_gi_(false),
		tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
		state_(GIS_NORMAL),
		pw_op_ptr_(nullptr),
		exchange_op_above_count_(0),
		multi_child_op_above_count_in_dfo_(0),
		partition_count_(0),
    hash_part_(false),
    slave_mapping_type_(SM_NONE),
    is_valid_for_gi_(false)
  {
  }
  ~AllocGIContext()
  {
  }
  bool managed_by_gi();
  bool is_in_partition_wise_state();
  bool is_in_pw_affinity_state();
  bool is_partition_gi() { return GIS_PARTITION == state_; };
  void set_in_partition_wise_state(ObLogicalOperator *op_ptr);
  bool is_in_affinity_state();
  void set_in_affinity_state(ObLogicalOperator *op_ptr);
  bool try_set_out_partition_wise_state(ObLogicalOperator *op_ptr);
  bool is_op_set_pw(ObLogicalOperator *op_ptr);
  int set_pw_affinity_state();
  void reset_info();
  GIState get_state();
  void add_exchange_op_count() { exchange_op_above_count_++; }
  bool exchange_above() { return 0 != exchange_op_above_count_; }
  void delete_exchange_op_count() { exchange_op_above_count_--; }
  void add_multi_child_op_count() { multi_child_op_above_count_in_dfo_++; }
  bool multi_child_op_above() { return 0 != multi_child_op_above_count_in_dfo_; }
  void delete_multi_child_op_count() { multi_child_op_above_count_in_dfo_--; }
  void reset_state() { state_ = GIS_NORMAL; }
  void set_force_partition() { state_ = GIS_PARTITION; }
  bool force_partition() { return GIS_PARTITION == state_; }
  // MANUAL_TABLE_DOP情况下，exchange operator在 alloc_gi_pre才能够被调用
  int push_current_dfo_dop(int64_t dop);
  // MANUAL_TABLE_DOP情况下，exchange operator在 alloc_gi_post才能够被调用
  int pop_current_dfo_dop();
  inline bool is_in_slave_mapping() { return SlaveMappingType::SM_NONE != slave_mapping_type_; }
  TO_STRING_KV(K(alloc_gi_),
							 K(tablet_size_),
							 K(state_),
							 K(exchange_op_above_count_));
  bool alloc_gi_;
  int64_t tablet_size_;
  GIState state_;
  ObLogicalOperator *pw_op_ptr_;
  int64_t exchange_op_above_count_;
  // 当前dfo的root op到当前处理op路径上多支算子的个数
  int64_t multi_child_op_above_count_in_dfo_;
  // 记录了当前GI直系TSC的分区数
  int64_t partition_count_;
  // 记录了当前GI直系TSC的是否是hash/key分区表
  bool hash_part_;
  SlaveMappingType slave_mapping_type_;
  bool is_valid_for_gi_;
};

class ObAllocGIInfo
{
public:
  ObAllocGIInfo () :
    state_(AllocGIContext::GIS_NORMAL),
    pw_op_ptr_(nullptr),
    multi_child_op_above_count_in_dfo_(0)
  {
  }
  TO_STRING_KV(K(state_),
               K(pw_op_ptr_));
  virtual ~ObAllocGIInfo() = default;
  void set_info(AllocGIContext &ctx)
  {
    state_ = ctx.state_;
    pw_op_ptr_ = ctx.pw_op_ptr_;
    multi_child_op_above_count_in_dfo_ = ctx.multi_child_op_above_count_in_dfo_;
  }
  void get_info(AllocGIContext &ctx)
  {
    ctx.state_ = state_;
    ctx.pw_op_ptr_ = pw_op_ptr_;
    ctx.multi_child_op_above_count_in_dfo_ = multi_child_op_above_count_in_dfo_;
  }
  AllocGIContext::GIState state_;
  ObLogicalOperator *pw_op_ptr_;
  int64_t multi_child_op_above_count_in_dfo_;
};

class AllocOpContext
{
public:
  AllocOpContext() :
      visited_map_(), disabled_op_set_(), gen_temp_op_id_(false), next_op_id_(0) {}
  ~AllocOpContext();

  int init();
  int visit(uint64_t op_id, uint8_t flag);
  bool is_visited(uint64_t op_id, uint8_t flag);

  // For the `blocking` hint, there are different insertion levels. We need to ensure that
  // when multiple insertion levels exist, material ops will not be inserted repeatedly.
  hash::ObHashMap<uint8_t, ObSEArray<uint64_t, 8>> visited_map_; // key:flag, value:op ids
  hash::ObHashSet<uint64_t> disabled_op_set_;
  bool gen_temp_op_id_;
  uint64_t next_op_id_;
};

class AllocBloomFilterContext
{
public:
  TO_STRING_KV(K_(filter_id));
  AllocBloomFilterContext() : filter_id_(0) {};
  ~AllocBloomFilterContext() = default;
  int64_t filter_id_;
};

struct ObExchangeInfo
{
  struct HashExpr
  {
    HashExpr() : expr_(NULL) {}
    HashExpr(ObRawExpr *expr, const ObObjMeta &cmp_type) : expr_(expr), cmp_type_(cmp_type) {}

    TO_STRING_KV(K_(expr), K_(cmp_type));

    ObRawExpr *expr_;

    // Compare type of %expr_ when compare with other values.
    // Objects should convert to %cmp_type_ before calculate hash value.
    //
    // Only type_ and cs_type_ of %cmp_type_ are used right now.
    ObObjMeta cmp_type_;
  };
  ObExchangeInfo()
  : is_remote_(false),
    is_task_order_(false),
    is_merge_sort_(false),
    is_sort_local_order_(false),
    sort_keys_(),
    slice_count_(1),
    repartition_type_(OB_REPARTITION_NO_REPARTITION),
    repartition_ref_table_id_(OB_INVALID_ID),
    repartition_table_id_(OB_INVALID_ID),
    repartition_table_name_(),
    repartition_keys_(),
    repartition_sub_keys_(),
    repartition_func_exprs_(),
    calc_part_id_expr_(NULL),
    hash_dist_exprs_(),
    popular_values_(),
    dist_method_(ObPQDistributeMethod::LOCAL), // pull to local
    unmatch_row_dist_method_(ObPQDistributeMethod::LOCAL),
    null_row_dist_method_(ObNullDistributeMethod::NONE),
    slave_mapping_type_(SlaveMappingType::SM_NONE),
    strong_sharding_(NULL),
    weak_sharding_(),
    need_null_aware_shuffle_(false),
    is_rollup_hybrid_(false),
    is_wf_hybrid_(false),
    wf_hybrid_aggr_status_expr_(NULL),
    wf_hybrid_pby_exprs_cnt_array_(),
    may_add_interval_part_(MayAddIntervalPart::NO),
    sample_type_(NOT_INIT_SAMPLE_TYPE),
    parallel_(ObGlobalHint::UNSET_PARALLEL),
    server_cnt_(0),
    server_list_()
  {
    repartition_table_id_ = 0;
  }

  virtual ~ObExchangeInfo() {}
  bool is_pq_hash_dist() const { return ObPQDistributeMethod::HASH == dist_method_; }
  bool is_pq_broadcast_dist() const { return ObPQDistributeMethod::BROADCAST == dist_method_; }
  bool is_pq_pkey() const { return ObPQDistributeMethod::PARTITION == dist_method_; }
  bool is_pq_range() const { return ObPQDistributeMethod::RANGE == dist_method_; }
  bool is_pq_local() const { return dist_method_ == ObPQDistributeMethod::LOCAL; }
  bool is_pq_random() const { return dist_method_ == ObPQDistributeMethod::RANDOM; }
  bool is_pq_pkey_hash() const { return dist_method_ == ObPQDistributeMethod::PARTITION_HASH;  }
  bool is_pq_pkey_rand() const { return dist_method_ == ObPQDistributeMethod::PARTITION_RANDOM; }
  bool need_exchange() const { return dist_method_ != ObPQDistributeMethod::NONE; }
  int init_calc_part_id_expr(ObOptimizerContext &opt_ctx);
  void set_calc_part_id_expr(ObRawExpr *expr) { calc_part_id_expr_ = expr; }
  int append_hash_dist_expr(const common::ObIArray<ObRawExpr *> &exprs);
  int assign(ObExchangeInfo &other);

  bool is_remote_;
  bool is_task_order_;
  bool is_merge_sort_;
  bool is_sort_local_order_;
  common::ObSEArray<OrderItem, 4> sort_keys_;
  int64_t slice_count_;
  ObRepartitionType repartition_type_;
  // repart target ref table id in pkey shuffle
  // the same as table_scan's get_ref_table_id
  int64_t repartition_ref_table_id_;
  // repart target table location id in pkey shuffle
  // the same as table_scan's get_table_id
  int64_t repartition_table_id_;
  ObString repartition_table_name_;
  common::ObSEArray<ObRawExpr*, 4> repartition_keys_;
  common::ObSEArray<ObRawExpr*, 4> repartition_sub_keys_;
  common::ObSEArray<ObRawExpr*, 4> repartition_func_exprs_;
  ObRawExpr *calc_part_id_expr_;
  common::ObSEArray<HashExpr, 4> hash_dist_exprs_;
  // for hybrid hash distr
  common::ObSEArray<ObObj, 20> popular_values_;
  ObPQDistributeMethod::Type dist_method_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;
  ObNullDistributeMethod::Type null_row_dist_method_;
  SlaveMappingType slave_mapping_type_;
  // for repart transmit with pkey range
  ObSEArray<uint64_t, 4> repart_all_tablet_ids_;
  // for hash-hash, re-partition and broadcast, remember the consumer exchange sharding
  ObShardingInfo *strong_sharding_;
  ObSEArray<ObShardingInfo*, 4> weak_sharding_;
  // for null aware anti join
  bool need_null_aware_shuffle_;
  bool is_rollup_hybrid_;
  bool is_wf_hybrid_;
  ObRawExpr *wf_hybrid_aggr_status_expr_;
  // pby exprs cnt of every wf for wf hybrid dist
  common::ObSEArray<int64_t, 4> wf_hybrid_pby_exprs_cnt_array_;
  MayAddIntervalPart may_add_interval_part_;
  // sample type for range distribution or partition range distribution
  ObPxSampleType sample_type_;
  int64_t parallel_;
  int64_t server_cnt_;
  common::ObSEArray<common::ObAddr, 4> server_list_;

  TO_STRING_KV(K_(is_remote),
               K_(is_task_order),
               K_(is_merge_sort),
               K_(is_sort_local_order),
               K_(sort_keys),
               K_(slice_count),
               K_(repartition_type),
               K_(repartition_ref_table_id),
               K_(repartition_table_name),
               K_(calc_part_id_expr),
               K_(repartition_keys),
               K_(repartition_sub_keys),
               K_(hash_dist_exprs),
               "dist_method", ObPQDistributeMethod::get_type_string(dist_method_),
               K_(repartition_func_exprs),
               K_(repart_all_tablet_ids),
               K_(slave_mapping_type),
               K_(need_null_aware_shuffle),
               K_(is_rollup_hybrid),
               K_(is_wf_hybrid),
               K_(wf_hybrid_pby_exprs_cnt_array),
               K_(may_add_interval_part),
               K_(sample_type),
               K_(parallel),
               K_(server_cnt),
               K_(server_list));
private:
  DISALLOW_COPY_AND_ASSIGN(ObExchangeInfo);
};

/**
 *  Expr and its producer operator
 */
class ExprProducer
{
public:
  ExprProducer()
      : expr_(NULL),
      producer_branch_(common::OB_INVALID_ID),
      consumer_id_(common::OB_INVALID_ID),
      producer_id_(common::OB_INVALID_ID)
  {
  }
  ExprProducer(ObRawExpr* expr, int64_t consumer_id)
      : expr_(expr),
      producer_branch_(common::OB_INVALID_ID),
      consumer_id_(consumer_id),
      producer_id_(common::OB_INVALID_ID)
  {
  }

  ExprProducer(ObRawExpr* expr,
               int64_t consumer_id,
               int64_t producer_id)
      : expr_(expr),
        producer_branch_(common::OB_INVALID_ID),
        consumer_id_(consumer_id),
        producer_id_(producer_id)
  {
  }

  ExprProducer(const ExprProducer &other)
      : expr_(other.expr_),
      producer_branch_(other.producer_branch_),
      consumer_id_(other.consumer_id_),
      producer_id_(other.producer_id_)
  {
  }

  ExprProducer &operator=(const ExprProducer &other)
  {
    expr_ = other.expr_;
    producer_branch_ = other.producer_branch_;
    consumer_id_ = other.consumer_id_;
    producer_id_ = other.producer_id_;
    return *this;
  }
  TO_STRING_KV(K_(consumer_id), K_(producer_id), K_(producer_branch), KPC_(expr));

  ObRawExpr *expr_;
  // 一般情况下可以忽略这个变量，简单把它理解成一个 bool 变量，
  // 表示表达式是否生成过，不影响理解整个表达式架构
  // 如果实在绕不开这个值，需要彻底理解它， @溪峰
  uint64_t producer_branch_;

  // 一个计划中有很多表达式，每个表达式一定有两个属性：
  //  - 谁负责计算/生成这个表达式
  //  - 谁负责消费这个表达式
  //
  // 在 OB 中：
  //  - producer id 表示表达式的生成者 id
  //  - consumer id 表示表达式的消费者算子 id
  //
  // - 对于普通表达式(如 c1+1 ) ，在 allocate_expr_pre 阶段
  //   首次遇到这些表达式时，就会设置上它们的 consumer id 和 producer id 为当前算子，
  // - 对于 fix expr，allocate_expr_pre 阶段首次遇到这些表达式时，只会设置 consumer id
  //   而 producer id 则延迟到后面的算子去生成（后面一定能遇到一个有能力生成这个表达式的算子）
  //
  // 表达式生成的过程非常绕，这里有一篇文档帮助你入门：
  //  -
  // 如果看完还不明白，请直接 @溪峰
  uint64_t consumer_id_;
  uint64_t producer_id_;
};

struct ObAllocExprContext
{
  ObAllocExprContext()
      : expr_producers_()
  {
  }
  ~ObAllocExprContext();
  int find(const ObRawExpr *expr, ExprProducer *&producer);

  int add(const ExprProducer &producer);

  int add_flattern_expr(const ObRawExpr* expr);

  int get_expr_ref_cnt(const ObRawExpr* expr, int64_t &ref_cnt);

  DISALLOW_COPY_AND_ASSIGN(ObAllocExprContext);

  // record producers expr index to search producer by expr quickly
  //  {key => ExprProducer.expr_ , value => index of expr_producers_}
  hash::ObHashMap<uint64_t, int64_t> expr_map_;
  // record each expr and its children expr reference count in expr producers
  //  {key => flattern expr, value => reference count }
  hash::ObHashMap<uint64_t, int64_t> flattern_expr_map_;
  common::ObSEArray<ExprProducer, 16, common::ModulePageAllocator, true> expr_producers_;
  // Exprs that cannot be used to extract shared child exprs
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> inseparable_exprs_;
};

struct ObPxPipeBlockingCtx
{
  // Look through the pipe (may be blocked by block operator), ended with a exchange operator or not.
  class PipeEnd {
  public:
    PipeEnd() : exch_(false) {}
    explicit PipeEnd(const bool is_exch) : exch_(is_exch) {}
    void set_exch(bool is_exch) { exch_ = is_exch; }
    bool is_exch() const { return exch_; }
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
  struct OpCtx
  {
    OpCtx() : out_(), in_(), has_dfo_below_(false), dfo_depth_(-1) {}

    PipeEnd out_;
    PipeEnd in_;

    bool has_dfo_below_;
    int64_t dfo_depth_;

    TO_STRING_KV(K_(in), K_(out), K_(has_dfo_below), K_(dfo_depth));
  };

  explicit ObPxPipeBlockingCtx(common::ObIAllocator &alloc);
  ~ObPxPipeBlockingCtx();

  OpCtx *alloc();
  TO_STRING_KV(K(op_ctxs_));

private:
  common::ObIAllocator &alloc_;
  common::ObArray<OpCtx *> op_ctxs_;
};

struct ObBatchExecParamCtx
{
  struct ExecParam
  {
    ExecParam() : expr_(NULL), branch_id_(0) {}

    ExecParam(ObRawExpr *expr, uint64_t branch_id)
      : expr_(expr), branch_id_(branch_id) {}

    ExecParam(const ExecParam &other)
      : expr_(other.expr_),
      branch_id_(other.branch_id_) {}

    ExecParam &operator=(const ExecParam &other)
    {
      expr_ = other.expr_;
      branch_id_ = other.branch_id_;
      return *this;
    }

    ObRawExpr *expr_;
    uint64_t branch_id_;

    TO_STRING_KV(K_(expr), K_(branch_id));
  };
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> params_idx_;
  common::ObSEArray<ExecParam, 8, common::ModulePageAllocator, true> exec_params_;
};

struct ObErrLogDefine
{
  ObErrLogDefine() :
    is_err_log_(false),
    err_log_database_name_(),
    err_log_table_name_(),
    reject_limit_(0),
    err_log_value_exprs_(),
    err_log_column_names_()
  {
  }
  bool is_err_log_;
  ObString err_log_database_name_;
  ObString err_log_table_name_; // now not support insert all
  uint64_t reject_limit_;
  // error logging the value expr of the column to be inserted
  ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> err_log_value_exprs_;
  //  the name of error logging table column which will be inserted
  ObSEArray<ObString, 4, common::ModulePageAllocator, true> err_log_column_names_;
};

class Path;
class ObLogPlan;
class ObLogicalOperator
{
public:
  friend class ObLogExprValues;
  friend class ObLogFunctionTable;
  typedef common::ObBitSet<common::OB_MAX_BITSET_SIZE> PPDeps;

  struct PartInfo {
    PartInfo() : part_id_(OB_INVALID_INDEX), subpart_id_(OB_INVALID_INDEX) {}
    int64_t part_id_;
    int64_t subpart_id_;
    TO_STRING_KV(K(part_id_), K(subpart_id_));
  };
  /**
   *  Child operator related constant definition
   */
  static const int64_t first_child = 0;
  static const int64_t second_child = 1;
  static const int64_t third_child = 2;
  static const int64_t fourth_child = 3;

  ObLogicalOperator(ObLogPlan &plan);
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
  /**
   *  Attache to a logical plan tree
   */
  inline void set_plan(ObLogPlan &plan) { my_plan_ = &plan; }

  /**
   *  Get the logical plan tree
   */
  virtual inline ObLogPlan *get_plan() { return my_plan_; }
  virtual inline const ObLogPlan *get_plan() const { return my_plan_; }

  /**
   *  Get the logical plan tree
   */
  const ObDMLStmt *get_stmt() const;
  /**
   *  Set my parent operator
   */
  inline ObLogicalOperator *get_parent()
  {
    return parent_;
  }

  inline const ObLogicalOperator *get_parent() const
  {
    return parent_;
  }

  int get_parent(ObLogicalOperator *root, ObLogicalOperator *&parent);
  /**
   * 目前优化器使用两阶段来生成计划:
   * 第一阶段是生成join order和其他算子
   * 第二阶段是分配exchange节点和处理其他逻辑(各种plan_traverse_loop)
   * 在第一阶段，为了不拷贝plan tree,节约优化器内存使用, 优化器只会设置孩子节点，不会设置父亲节点, 也就是说所有的parent_都是NULL，
   * 所以在第一阶段的任何时候都不能通过调用get_parent接口来获取父亲节点(可以通过传入root节点来获取父亲节点)。
   * 在第二阶段，因为很多地方用到了get_parent接口，所以在第一阶段结束的时候，我们根据孩子节点把parent设置正确，这样在第二阶段可以调用get_parent接口来获取父节点。
   * 关于第一阶段为啥不设置parent的原因:
   * 考虑如下例子，在我们分配group by算子的时候，对于当前一个计划，通常会有hash group和merge group两种方式，因为一个孩子节点只能存在一个parent，
   * 所以我们必须要拷贝当前的一份计划，然后在这两个计划上分别分配hash group by和merge group by，并且设置相应的parent，这种拷贝方式会占用大量的内存空间，
   * 如果不维护parent这个结构，就可以避免这种计划拷贝。
   */
  void set_parent(ObLogicalOperator *parent);
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

  /**
   * 算子生成的行的平均行长（以字节为单位）
   */
  inline double get_width() const { return width_; }
  void set_width(double width) { width_ = width; }
  virtual int est_width();

  /**
   * 算子在计划树中的深度，顶层算子为0
   */
  inline int64_t get_plan_depth() const { return plan_depth_; }
  void set_plan_depth(int64_t plan_depth) { plan_depth_ = plan_depth; }

  PxOpSizeFactor get_px_est_size_factor() const { return px_est_size_factor_; }
  PxOpSizeFactor &get_px_est_size_factor() { return px_est_size_factor_; }

  /**
   *  Get the output expressions
   */
  inline common::ObIArray<ObRawExpr *> &get_output_exprs()
  {
    return output_exprs_;
  }
  inline const common::ObIArray<ObRawExpr *> &get_output_exprs() const
  {
    return output_exprs_;
  }

  /**
   *  Get the filter expressions
   */
  inline common::ObIArray<ObRawExpr *> &get_filter_exprs()
  {
    return filter_exprs_;
  }
  inline const common::ObIArray<ObRawExpr *> &get_filter_exprs() const
  {
    return filter_exprs_;
  }

  /**
   *  Get the pushdown filter expressions
   */
  inline common::ObIArray<ObRawExpr *> &get_pushdown_filter_exprs()
  {
    return pushdown_filter_exprs_;
  }
  inline const common::ObIArray<ObRawExpr *> &get_pushdown_filter_exprs() const
  {
    return pushdown_filter_exprs_;
  }

  /**
   *  Get the filter expressions
   */
  inline common::ObIArray<ObRawExpr *> &get_startup_exprs()
  {
    return startup_exprs_;
  }
  inline const common::ObIArray<ObRawExpr *> &get_startup_exprs() const
  {
    return startup_exprs_;
  }
  inline int add_startup_exprs(const common::ObIArray<ObRawExpr *> &exprs)
  {
    return append(startup_exprs_, exprs);
  }

  /**
   *  Get a specified child operator
   */
  inline ObLogicalOperator *get_child(const int64_t index) const
  {
    return OB_LIKELY(index >= 0 && index < child_.count()) ? child_.at(index) : NULL;
  }

  inline ObIArray<ObLogicalOperator*> &get_child_list()
  {
    return child_;
  }

  inline const ObIArray<ObLogicalOperator*> &get_child_list() const
  {
    return child_;
  }

  /**
   *  Set a specified child operator
   */
  void set_child(const int64_t idx, ObLogicalOperator *child_op);

  /**
   *  Add a child operator at the next available position
   */
  int add_child(ObLogicalOperator *child_op);

  int add_child(const common::ObIArray<ObLogicalOperator*> &child_ops);

  /**
   *  Set the first child operator
   */
  void set_left_child(ObLogicalOperator *child_op)
  {
    set_child(first_child, child_op);
  }

  /**
   *  Set the second child operator
   */
  void set_right_child(ObLogicalOperator *child_op)
  {
    set_child(second_child, child_op);
  }

  /**
   *  Get the optimization cost up to this point
   */
  inline const double &get_cost() const
  {
    return cost_;
  }

  inline double &get_cost()
  {
    return cost_;
  }

  /*
   * get optimization cost for current operator
   */
  inline const double &get_op_cost() const
  {
    return op_cost_;
  }

  inline double &get_op_cost()
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

  inline uint64_t get_op_id() const { return op_id_; }
  inline void set_op_id(uint64_t op_id) { op_id_ = op_id; }
  inline bool is_partition_wise() const { return is_partition_wise_; }
  inline void set_is_partition_wise(bool is_partition_wise)
  { is_partition_wise_ = is_partition_wise; }
  inline bool is_fully_partition_wise() const
  {
    return is_partition_wise() && !exchange_allocated_;
  }

  inline bool is_partial_partition_wise() const
  {
    return is_partition_wise() && exchange_allocated_;
  }
  inline bool is_exchange_allocated() const
  {
    return exchange_allocated_;
  }
  inline void set_exchange_allocated(bool exchange_allocated)
  {
    exchange_allocated_ = exchange_allocated;
  }
  virtual bool is_gi_above() const { return false; }
  inline void set_phy_plan_type(ObPhyPlanType phy_plan_type)
  {
    phy_plan_type_ = phy_plan_type;
  }
  inline ObPhyPlanType get_phy_plan_type() const
  {
    return phy_plan_type_;
  }
  inline void set_location_type(ObPhyPlanType location_type)
  {
    location_type_ = location_type;
  }
  inline ObPhyPlanType get_location_type() const
  {
    return location_type_;
  }
  /*
   * get cur_op's output ordering
   */
  inline common::ObIArray<OrderItem> &get_op_ordering()
  {
    return op_ordering_;
  }

  inline const common::ObIArray<OrderItem> &get_op_ordering() const
  {
    return op_ordering_;
  }

  inline common::ObIArray<common::ObAddr> &get_server_list()
  {
    return server_list_;
  }
  inline const common::ObIArray<common::ObAddr> &get_server_list() const
  {
    return server_list_;
  }

  inline const ObFdItemSet& get_fd_item_set() const
  {
    return NULL == fd_item_set_ ? empty_fd_item_set_ : *fd_item_set_;
  }

  void set_fd_item_set(const ObFdItemSet *other)
  {
    fd_item_set_ = other;
  }

  void set_table_set(const ObRelIds *table_set)
  {
    table_set_ = table_set;
  }

  inline const ObRelIds& get_table_set() const
  {
    return NULL == table_set_ ? empty_table_set_ : *table_set_;
  }

  int set_op_ordering(const common::ObIArray<OrderItem> &op_ordering);

  void reset_op_ordering()
  {
    op_ordering_.reset();
  }

  int get_input_const_exprs(common::ObIArray<ObRawExpr *> &const_exprs) const;

  int get_input_equal_sets(EqualSets &ordering_in_eset) const;

  inline void set_output_equal_sets(const EqualSets *equal_sets)
  {
    output_equal_sets_ = equal_sets;
  }
  inline const EqualSets &get_output_equal_sets() const
  {
    return NULL == output_equal_sets_ ?
          empty_expr_sets_ : *output_equal_sets_;
  }
  inline common::ObIArray<ObRawExpr *> &get_output_const_exprs()
  {
    return output_const_exprs_;
  }
  inline const common::ObIArray<ObRawExpr *> &get_output_const_exprs() const
  {
    return output_const_exprs_;
  }
  /*
   * get cur_op's expected_ordering
   */
  inline void set_is_local_order(bool is_local_order) { is_local_order_ = is_local_order; }
  inline bool get_is_local_order() const { return is_local_order_; }
  inline void set_is_range_order(bool is_range_order) { is_range_order_ = is_range_order; }
  inline bool get_is_range_order() const { return is_range_order_; }

  inline void set_is_at_most_one_row(bool is_at_most_one_row) { is_at_most_one_row_ = is_at_most_one_row; }
  inline bool get_is_at_most_one_row() { return is_at_most_one_row_; }
  inline bool get_is_at_most_one_row() const { return is_at_most_one_row_; }

  void *get_traverse_ctx() const { return traverse_ctx_; }
  void set_traverse_ctx(void *ctx) { traverse_ctx_ = ctx; }

  // clear plan tree's traverse ctx
  void clear_all_traverse_ctx();
  template <typename Allocator>
  int init_all_traverse_ctx(Allocator &alloc);

  inline int64_t get_interesting_order_info() const { return interesting_order_info_; }
  inline void set_interesting_order_info(int64_t info) { interesting_order_info_ = info; }
  inline bool has_any_interesting_order_info_flag() const { return interesting_order_info_ > 0; }

  /**
   *  Do pre-child-traverse operation
   */
  int do_pre_traverse_operation(const TraverseOp &op, void *ctx);

  /**
   *  Do post-child-traverse operation
   */
  int do_post_traverse_operation(const TraverseOp &op, void *ctx);

  /**
   *  Get predefined operator name
   */
  virtual const char *get_name() const
  {
    return log_op_def::get_op_name(type_);
  }

  virtual int get_explain_name_internal(char *buf, const int64_t buf_len, int64_t &pos)
  {
    int ret = common::OB_SUCCESS;
    ret = BUF_PRINTF("%s", get_name());
    return ret;
  }

  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return common::OB_SUCCESS;
  }

  int collecte_inseparable_exprs(ObAllocExprContext &ctx);

  virtual int allocate_expr_pre(ObAllocExprContext &ctx);

  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs);

  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed);

  int contain_my_fixed_expr(const ObRawExpr *expr, bool &is_contain);
  int get_next_producer_id(ObLogicalOperator *node,
                           uint64_t &producer_id);
  int add_exprs_to_op_and_ctx(ObAllocExprContext &ctx,
                              const ObIArray<ObRawExpr*> &exprs);
  int add_exprs_to_op(const ObIArray<ObRawExpr*> &exprs);
  int add_exprs_to_ctx(ObAllocExprContext &ctx,
                       const ObIArray<ObRawExpr*> &exprs);
  int build_and_put_pack_expr(ObIArray<ObRawExpr*> &output_exprs);
  int build_and_put_into_outfile_expr(const ObSelectIntoItem *into_item,
                                      ObIArray<ObRawExpr*> &output_exprs);
  int put_into_outfile_expr(ObRawExpr *into_expr);
  int add_exprs_to_ctx(ObAllocExprContext &ctx,
                       const ObIArray<ObRawExpr*> &exprs,
                       uint64_t producer_id);
  int add_expr_to_ctx(ObAllocExprContext &ctx,
                      ObRawExpr* epxr,
                      uint64_t producer_id);

  bool can_update_producer_id_for_shared_expr(const ObRawExpr *expr);

  int extract_non_const_exprs(const ObIArray<ObRawExpr*> &input_exprs,
                              ObIArray<ObRawExpr*> &non_const_exprs);
  int check_need_pushdown_expr(const bool producer_id,
                               bool &need_pushdown);
  int check_can_pushdown_expr(const ObRawExpr *expr, bool &can_pushdown);
  int get_pushdown_producer_id(const ObRawExpr *expr, uint64_t &producer_id);

  int force_pushdown_exprs(ObAllocExprContext &ctx);
  int get_pushdown_producer_id(uint64_t &producer_id);

  int extract_shared_exprs(const ObIArray<ObRawExpr*> &exprs,
                           ObAllocExprContext &ctx,
                           ObIArray<ObRawExpr*> &shard_exprs);
  int extract_shared_exprs(ObRawExpr *expr,
                           ObAllocExprContext &ctx,
                           int64_t parent_ref_cnt,
                           ObIArray<ObRawExpr*> &shard_exprs);

  int find_consumer_id_for_shared_expr(const ObIArray<ExprProducer> *ctx,
                                       const ObRawExpr *expr,
                                       uint64_t &consumer_id);
  int find_producer_id_for_shared_expr(const ObRawExpr *expr,
                                       uint64_t &producer_id);
  /**
   *  Allocate output expr post-traverse
   *
   *  The default implementation will iterate over the context to see if there are
   *  more exprs that can be produced - ie. the exprs that they depend on have been
   *  produced. An expression will be categorized into filter, join predicates or
   *  output expressions and placed at proper location once it is produced.
   */
  virtual int allocate_expr_post(ObAllocExprContext &ctx);

  int expr_can_be_produced(const ObRawExpr *expr,
                           ObAllocExprContext &expr_ctx,
                           bool &can_be_produced);

  int expr_has_been_produced(const ObRawExpr *expr,
                             ObAllocExprContext &expr_ctx,
                             bool &has_been_produced);

  bool is_child_output_exprs(const ObRawExpr *expr) const;
  virtual int compute_const_exprs();

  virtual int compute_equal_set();

  virtual int compute_fd_item_set();

  virtual int deduce_const_exprs_and_ft_item_set(ObFdItemSet &fd_item_set);

  virtual int compute_op_ordering();

  virtual int compute_op_interesting_order_info();

  virtual int compute_op_parallel_and_server_info();

  virtual int compute_table_set();

  virtual int compute_one_row_info();

  virtual int compute_sharding_info();

  virtual int compute_plan_type();

  virtual int compute_op_other_info();

  virtual int compute_pipeline_info();
  bool is_pipelined_plan() const { return is_pipelined_plan_; }
  bool is_nl_style_pipelined_plan() const { return is_nl_style_pipelined_plan_; }

  virtual int est_cost()
  { return OB_SUCCESS; }

  int re_est_cost(EstimateCostInfo &param, double &card, double &cost);
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost);

  /**
   * @brief compute_property
   * convert property fields from a path into a logical operator
   */
  virtual int compute_property(Path *path);

  /**
   * @brief compute_property
   * compute properties of an operator.
   * called by non-terminal operator
   * 1. ordering, 2. unique sets, 3. equal_sets
   * 4. est_sel_info, 5. cost, card, width
   */
  virtual int compute_property();

  int check_property_valid() const;
  int compute_normal_multi_child_parallel_and_server_info();
  int set_parallel_and_server_info_for_match_all();
  int get_limit_offset_value(ObRawExpr *percent_expr,
                             ObRawExpr *limit_expr,
                             ObRawExpr *offset_expr,
                             double &limit_percent,
                             int64_t &limit_count,
                             int64_t &offset_count);

  /**
   *  Pre-traverse function for allocating granule iterator
   */
  virtual int allocate_granule_pre(AllocGIContext &ctx);
  /**
   *	Post-traverse function for allocating granule iterator
   */
  virtual int allocate_granule_post(AllocGIContext &ctx);
  /**
   *	Allocate granule iterator above this operator
   */
  virtual int allocate_granule_nodes_above(AllocGIContext &ctx);
  /**
   *	Set granule iterator affinity
   */
  virtual int set_granule_nodes_affinity(AllocGIContext &ctx, int64_t child_index);
  /*
   * Allocate gi above all the table scans below this operator.
   */
  int allocate_gi_recursively(AllocGIContext &ctx);
  /**
   * Allocate op for monitering
   */
  int alloc_op_pre(AllocOpContext& ctx);
  int alloc_op_post(AllocOpContext& ctx);
  int find_rownum_expr_recursively(const uint64_t &op_id, ObLogicalOperator *&rownum_op,
                                   const ObRawExpr *expr);
  int find_rownum_expr(const uint64_t &op_id, ObLogicalOperator *&rownum_op,
                       const ObIArray<ObRawExpr *> &exprs);
  int find_rownum_expr(const uint64_t &op_id, ObLogicalOperator *&rownum_op);
  int gen_temp_op_id(AllocOpContext& ctx);
  int recursively_disable_alloc_op_above(AllocOpContext& ctx);
  int alloc_nodes_above(AllocOpContext& ctx, const uint64_t &flags);
  int allocate_material_node_above();
  int allocate_monitoring_dump_node_above(uint64_t flags, uint64_t line_id);

  virtual int gen_location_constraint(void *ctx);

  /**
   * Generate a table's location constraint for pdml index maintain op
   */
  int get_tbl_loc_cons_for_pdml_index(LocationConstraint &loc_cons);
  /**
   * Generate a table's location constraint for table scan op
   */
  int get_tbl_loc_cons_for_scan(LocationConstraint &loc_cons);
  // generate a table location constraint for duplicate table's replica selection
  int get_dup_replica_cons_for_scan(ObDupTabConstraint &dup_rep_cons, bool &found_dup_con);
  /**
   * @brief Generate a table's location constraint for insert op
   */
  int get_tbl_loc_cons_for_insert(LocationConstraint &loc_cons, bool &is_multi_part_dml);

  int get_table_scan(ObLogicalOperator *&tsc, uint64_t table_id);

  int find_all_tsc(common::ObIArray<ObLogicalOperator*> &tsc_ops, ObLogicalOperator *root);
  /**
   * Check if a exchange need rescan is below me.
   */
  int check_exchange_rescan(bool &need_rescan);

  /**
   * Check if has exchange below.
   */
  int check_has_exchange_below(bool &has_exchange) const;
  /**
   * Allocate runtime filter operator.
   */
  int allocate_runtime_filter_for_hash_join(AllocBloomFilterContext &ctx);

  static int check_is_table_scan(const ObLogicalOperator &op,
                                 bool &is_table_scan);

  // Operator is block operator if outputting data after all children's data processed.
  //   block operator: group by, sort, material ...
  //   non block operator: limit, join, ...
  virtual bool is_block_op() const { return false; }
  // Non block operator may start outputting data after some children's data processed,
  // is_block_input() indicate those children.
  // e.g.: first child of hash join is block input, second child is not block input.
  virtual bool is_block_input(const int64_t child_idx) const { UNUSED(child_idx); return is_block_op(); }
  // 表示一次只迭代一个child的数据，收完一个child的数据才开始收下一个
  virtual bool is_consume_child_1by1() const { return false; }

  // PX pipe blocking phase add material operator to DFO, to make sure DFO can be scheduled
  // in consumer/producer threads model. top-down stage markup the output side has block
  // operator or not. bottom-up stage markup the input side and add material operator in the
  // following case:
  //   1. operator got more then one input with out block operator.
  //   2. no block operator between two DFO.
  //
  // Notice: We do not mark block operator directly in traverse context, we marking whether
  // data can streaming to an exchange operator instead.
  virtual int px_pipe_blocking_pre(ObPxPipeBlockingCtx &ctx);
  virtual int px_pipe_blocking_post(ObPxPipeBlockingCtx &ctx);
  virtual int has_block_parent_for_shj(bool &has_shj);

  virtual int allocate_startup_expr_post();
  virtual int allocate_startup_expr_post(int64_t child_idx);
  /**
   *  Start plan tree traverse
   *
   *  For each traverse operation, we provide a opportunity of operation both
   *  before and after accessing the children. This should be sufficient to
   *  support both top-down and bottom-up traversal.
   */
  int do_plan_tree_traverse(const TraverseOp &operation, void *ctx);

  int should_allocate_gi_for_dml(bool &is_valid);
  /**
   *  Get the operator id
   */
  uint64_t get_operator_id() const { return id_; }

  /**
   *  Mark this operator as the root of the plan
   */
  void mark_is_plan_root() { is_plan_root_ = true; }

  void set_is_plan_root(bool is_root) { is_plan_root_ = is_root; }

  /**
   *  Check if this operator is the root of a logical plan
   */
  bool is_plan_root() const { return is_plan_root_; }

  /**
   *  Mark an expr as 'produced'
   */
  int mark_expr_produced(ObRawExpr *expr,
                         uint64_t branch_id,
                         uint64_t producer_id,
                         ObAllocExprContext &gen_expr_ctx);
  /**
   *  Get the operator's hash value
   */
  virtual uint64_t hash(uint64_t seed) const;
  /**
   *  Generate hash value for output exprs
   */
  inline uint64_t hash_output_exprs(uint64_t seed) const
  { return ObOptimizerUtil::hash_exprs(seed, output_exprs_); }

  /**
   *  Generate hash value for filter exprs
   */
  inline uint64_t hash_filter_exprs(uint64_t seed) const
  { return ObOptimizerUtil::hash_exprs(seed, filter_exprs_); }

  inline uint64_t hash_pushdown_filter_exprs(uint64_t seed) const
  { return ObOptimizerUtil::hash_exprs(seed, pushdown_filter_exprs_); }

  inline uint64_t hash_startup_exprs(uint64_t seed) const
  { return ObOptimizerUtil::hash_exprs(seed, startup_exprs_); }
  int adjust_plan_root_output_exprs();
  int set_plan_root_output_exprs();
  inline ObShardingInfo *get_strong_sharding() { return strong_sharding_; }
  inline const ObShardingInfo *get_strong_sharding() const { return strong_sharding_; }
  inline void set_strong_sharding(ObShardingInfo* strong_sharding) { strong_sharding_ = strong_sharding; }

  int check_stmt_can_be_packed(const ObDMLStmt *stmt, bool &need_pack);
  inline ObIArray<ObShardingInfo*> &get_weak_sharding() { return weak_sharding_; }

  inline const ObIArray<ObShardingInfo*> &get_weak_sharding() const { return weak_sharding_; }
  inline int set_weak_sharding(ObIArray<ObShardingInfo*> &weak_sharding) {
    return weak_sharding_.assign(weak_sharding);
  }
  inline bool get_contains_fake_cte() const { return contain_fake_cte_; }
  inline void set_contains_fake_cte(bool contain_fake_cte)
  {
    contain_fake_cte_ = contain_fake_cte;
  }
  inline bool get_contains_pw_merge_op() const { return contain_pw_merge_op_; }
  inline void set_contains_pw_merge_op(bool contain_pw_merge_op)
  {
    contain_pw_merge_op_ = contain_pw_merge_op;
  }
  inline bool get_contains_match_all_fake_cte() const { return contain_match_all_fake_cte_; }
  inline void set_contains_match_all_fake_cte(bool contain_match_all_fake_cte)
  {
    contain_match_all_fake_cte_ = contain_match_all_fake_cte;
  }
  inline bool get_contains_das_op() const { return contain_das_op_; }
  inline void set_contains_das_op(bool contain_das_op)
  {
    contain_das_op_ = contain_das_op;
  }

  inline int64_t get_inherit_sharding_index() const { return inherit_sharding_index_; }
  inline void set_inherit_sharding_index(int64_t inherit_sharding_index)
  {
    inherit_sharding_index_ = inherit_sharding_index;
  }

  inline bool need_osg_merge() const { return need_osg_merge_; }
  inline void set_need_osg_merge(bool v)
  {
    need_osg_merge_ = v;
  }

  bool is_dml_operator() const
  {
    return (log_op_def::LOG_UPDATE == type_
            || log_op_def::LOG_DELETE == type_
            || log_op_def::LOG_INSERT == type_
            || log_op_def::LOG_INSERT_ALL == type_
            || log_op_def::LOG_MERGE == type_);
  }
  bool is_expr_operator() const
  {
    return (log_op_def::LOG_EXPR_VALUES == type_ || log_op_def::LOG_VALUES == type_);
  }
  bool is_osg_operator() const
  {
    return log_op_def::LOG_OPTIMIZER_STATS_GATHERING == type_;
  }
  /*
   * replace exprs which will be returned by get_op_exprs during allocating expr
   */
  int replace_op_exprs(ObRawExprReplacer &replacer);

  // check if this operator is some kind of table scan.
  // the default return value is false
  // Note: The default table scan operator ObLogTableScan overrides this
  //       method to return true.
  //       IT'S RECOMMENDED THAT any table scan operator derive from ObLogTableScan,
  //       or you MAY NEED TO CHANGE EVERY CALLER of this method.
  virtual bool is_table_scan() const { return false; }

  int allocate_material(const int64_t index);

  // Find the first operator through the child operators recursively
  // Found: return OB_SUCCESS, set %op
  // Not found: return OB_SUCCESS, set %op to NULL
  int find_first_recursive(const log_op_def::ObLogOpType type, ObLogicalOperator *&op);
  // the operator use these interfaces to allocate gi
  int pw_allocate_granule_pre(AllocGIContext &ctx);
  int pw_allocate_granule_post(AllocGIContext &ctx);

  // dblink
  void set_dblink_id(uint64_t dblink_id) { dblink_id_ = dblink_id; }
  uint64_t get_dblink_id() const { return dblink_id_; }

  int check_sharding_compatible_with_reduce_expr(const common::ObIArray<ObRawExpr*> &reduce_exprs,
                                                 bool &compatible) const;
  inline bool is_local() const
  {
    return (NULL != strong_sharding_ && strong_sharding_->is_local());
  }
  inline bool is_remote() const
  {
    return (NULL != strong_sharding_ && strong_sharding_->is_remote());
  }
  inline bool is_match_all() const
  {
    return (NULL != strong_sharding_ && strong_sharding_->is_match_all());
  }
  inline bool is_distributed() const
  {
    return (NULL != strong_sharding_ && strong_sharding_->is_distributed())
            || !weak_sharding_.empty();
  }
  inline bool is_sharding() const
  {
    return is_remote() || is_distributed();
  }
  inline bool is_single() const
  {
    return is_local() || is_remote() || is_match_all();
  }
  inline ObShardingInfo* get_sharding() const
  {
    ObShardingInfo *ret_sharding = NULL;
    if (NULL != strong_sharding_) {
      ret_sharding = strong_sharding_;
    } else if (!weak_sharding_.empty()) {
      ret_sharding = weak_sharding_.at(0);
    }
    return ret_sharding;
  }
  int get_part_column_exprs(const uint64_t table_id,
                            const uint64_t ref_table_id,
                            ObIArray<ObRawExpr *> &part_cols) const;
  inline void set_parallel(int64_t parallel) { parallel_ = parallel; }
  inline int64_t get_parallel() const { return parallel_; }
  inline void set_op_parallel_rule(OpParallelRule op_parallel_rule) { op_parallel_rule_ = op_parallel_rule; }
  inline OpParallelRule get_op_parallel_rule() const { return op_parallel_rule_; }
  inline int64_t get_available_parallel() const { return available_parallel_; }
  inline void set_available_parallel(int64_t available_parallel) { available_parallel_ = available_parallel; }
  inline void set_server_cnt(int64_t count) { server_cnt_ = count; }
  inline int64_t get_server_cnt() const { return server_cnt_; }

  void set_late_materialization(bool need_mater) { need_late_materialization_ = need_mater; }
  bool need_late_materialization() const { return need_late_materialization_; }
  /**
   *  Re-order the filter expressions according to their selectivity
   *
   *  Re-order the filter expression vector so that the more selective predicate will
   *  be evaluated earlier.
   */
  int reorder_filter_exprs();
  int reorder_filters_exprs(common::ObIArray<ObExprSelPair> &predicate_selectivities,
                            ObIArray<ObRawExpr *> &filters_exprs);

  int find_shuffle_join_filter(bool &find) const;
  int has_window_function_below(bool &has_win_func) const;
  int get_pushdown_op(log_op_def::ObLogOpType op_type, const ObLogicalOperator *&op) const;

  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item);

  virtual int print_outline_data(PlanText &plan_text);

  virtual int print_used_hint(PlanText &plan_text);

  int print_outline_table(PlanText &plan_text, const TableItem *table_item) const;
  /**
   * pure partition wise:
   *   match multiple children and all children meet strict partition wise
   *   e.g, original pw join and pw set(not union all)
   *
   * affinite partition wise:
   *   match multiple children and all children meet partition wise with affinity
   *   e.g, union all with the same data distribution node for all children
   *
   * extended partition wise:
   *   match multiple children and all children have already with hash-hash distribution
   *   and no need additional exchange allocated
   *   e.g, union all hash-hash and none-none hash join
   */
  inline bool is_pure_partition_wise()
  {
    return is_partition_wise_ && !exchange_allocated_ &&
           NULL != get_sharding() &&
           get_sharding()->is_distributed_with_table_location_and_partitioning();
  }
  inline bool is_affinite_partition_wise()
  {
    return is_partition_wise_ && exchange_allocated_ &&
           NULL != get_sharding() &&
           get_sharding()->is_distributed_with_table_location_and_partitioning();
  }
  inline bool is_extended_partition_wise()
  {
    return is_partition_wise_ && exchange_allocated_ &&
           NULL != get_sharding() &&
           get_sharding()->is_distributed_without_table_location_with_partitioning();
  }
  int collect_batch_exec_param_pre(void* ctx);
  int collect_batch_exec_param_post(void* ctx);
  int collect_batch_exec_param(void* ctx,
                               const ObIArray<ObExecParamRawExpr*> &exec_params,
                               ObIArray<ObExecParamRawExpr *> &left_above_params,
                               ObIArray<ObExecParamRawExpr *> &right_above_params);
  // 生成 partition id 表达式
  int generate_pseudo_partition_id_expr(ObOpPseudoColumnRawExpr *&expr);
  int pick_out_startup_filters();
  int check_contain_false_startup_filter(bool &contain_false);

  // Make the operator in state of output data.
  // 1. If this operator is single-child and non-block, then open its child.
  // 2. If this operator is single-child and block, then open and close its child.
  // 3. If this operator has multiple children, open all children by default and
  //    operators should override this function according to their unique execution logic.
  // append_map = false means it's in the progress of LogSet searching for child
  //   with max running thread/group count, and we will not modify max_count or max_map.
  virtual int open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG);
  // Make the operator in state that all data has been outputted already.
  virtual int close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG);
  int find_max_px_resource_child(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG, int64_t start_idx);

public:
  ObSEArray<ObLogicalOperator *, 16, common::ModulePageAllocator, true> child_;
  ObSEArray<ObPCParamEqualInfo, 4, common::ModulePageAllocator, true> equal_param_constraints_;
  ObSEArray<ObPCConstParamInfo, 4, common::ModulePageAllocator, true> const_param_constraints_;
  ObSEArray<ObExprConstraint, 4, common::ModulePageAllocator, true> expr_constraints_;
protected:
  enum TraverseType
  {
    JOIN_METHOD,
    MATERIAL_NL,
    LEADING,
    PQ_DIST,
    PQ_MAP
  };
  enum JoinTreeType
  {
    INVALID_TYPE,
    LEFT_DEEP,
    RIGHT_DEEP,
    BUSHY
  };

  int project_pruning_pre();
  virtual int check_output_dependance(common::ObIArray<ObRawExpr *> &child_output, PPDeps &deps);
  void do_project_pruning(common::ObIArray<ObRawExpr *> &exprs,
                          PPDeps &deps);
  int try_add_remove_const_exprs();
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer);
  int replace_exprs_action(ObRawExprReplacer &replacer, common::ObIArray<ObRawExpr*> &dest_exprs);
  int replace_expr_action(ObRawExprReplacer &replacer, ObRawExpr *&dest_expr);

  int explain_print_partitions(ObTablePartitionInfo &table_partition_info,
                               char *buf, int64_t &buf_len, int64_t &pos);
  static int explain_print_partitions(const ObIArray<ObLogicalOperator::PartInfo> &part_infos,
                                      const bool two_level, char *buf,
                                      int64_t &buf_len, int64_t &pos);

  int check_op_orderding_used_by_parent(bool &used);
protected:

  void add_dist_flag(uint64_t &flags, DistAlgo method) const {
    flags |= method;
  }

  void remove_dist_flag(uint64_t &flags, DistAlgo method) const {
    flags &= ~method;
  }

  bool has_dist_flag(uint64_t flags, DistAlgo method) const {
    return !!(flags & method);
  }

  void clear_dist_flag(uint64_t &flags) const {
    flags = 0;
  }

  int find_table_scan(ObLogicalOperator* root_op,
                    uint64_t table_id,
                    ObLogicalOperator* &scan_op,
                    bool& table_scan_has_exchange,
                    bool &has_px_coord);

  int check_sort_key_can_pushdown_to_tsc(
      ObLogicalOperator *op,
      common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> &effective_sk_exprs,
      uint64_t table_id, ObLogicalOperator *&scan_op, bool &table_scan_has_exchange,
      bool &has_px_coord, int64_t &effective_sk_cnt);

protected:

  log_op_def::ObLogOpType type_;
  ObLogPlan *my_plan_;                     // the entry point of the plan
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> output_exprs_;    // expressions produced
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> filter_exprs_;        // filtering exprs.
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> pushdown_filter_exprs_; // pushdown filtering exprs，用于计算条件下推的nl join的join keys
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> startup_exprs_;        // startup filtering exprs.

  common::ObSEArray<ObRawExpr *, 16, common::ModulePageAllocator, true> output_const_exprs_;
  const EqualSets *output_equal_sets_;
  const ObFdItemSet *fd_item_set_;
  const ObRelIds *table_set_;

  uint64_t id_;                        // operator 0-based depth-first id
  uint64_t branch_id_;
  uint64_t op_id_;

private:

  /**
   *  Numbering the operator
   */
  int numbering_operator_pre(NumberingCtx &ctx);
  void numbering_operator_post(NumberingCtx &ctx);
  /**
   * Numbering px transmit op with dfo id
   * for Explain display
   */
  int numbering_exchange_pre(NumberingExchangeCtx &ctx);
  int numbering_exchange_post(NumberingExchangeCtx &ctx);
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
  int inner_set_merge_sort(
    ObLogicalOperator *producer,
    ObLogicalOperator *consumer,
    ObLogicalOperator *op_sort,
    bool &need_remove,
    bool global_order);
  //private function, just used for allocating join filter node.
  int allocate_partition_join_filter(const ObIArray<JoinFilterInfo> &infos,
                                     int64_t &filter_id);
  int allocate_normal_join_filter(const ObIArray<JoinFilterInfo> &infos,
                                  int64_t &filter_id);
  int create_runtime_filter_info(
      ObLogicalOperator *op,
      ObLogicalOperator *join_filter_create_op,
      ObLogicalOperator *join_filter_use_op,
      double join_filter_rate);
  int add_join_filter_info(
      ObLogicalOperator *join_filter_create_op,
      ObLogicalOperator *join_filter_use_op,
      ObRawExpr *join_filter_expr,
      RuntimeFilterType type);
  int add_partition_join_filter_info(
      ObLogicalOperator *join_filter_create_op,
      RuntimeFilterType type);
  int generate_runtime_filter_expr(
      ObLogicalOperator *op,
      ObLogicalOperator *join_filter_create_op,
      ObLogicalOperator *join_filter_use_op,
      double join_filter_rate,
      RuntimeFilterType type);
  int cal_runtime_filter_compare_func(
      ObLogJoinFilter *join_filter_use,
      ObRawExpr *join_use_expr,
      ObRawExpr *join_create_expr);
  int check_can_extract_query_range_by_rf(
    ObLogicalOperator *scan_node,
    ObLogicalOperator *join_filter_create_op,
    ObLogicalOperator *join_filter_use_op,
    bool &can_extract_query_range,
    ObIArray<int64_t> &prefix_col_idxs);
  int try_prepare_rf_query_range_info(
    ObLogicalOperator *join_filter_create_op,
    ObLogicalOperator *join_filter_use_op,
    ObLogicalOperator *scan_node,
    const JoinFilterInfo &info);

  int check_sort_key_can_pushdown_to_tsc_detail(ObLogicalOperator *op,
                                                ObRawExpr *candidate_sk_expr, uint64_t table_id,
                                                ObLogicalOperator *&scan_op, bool &find_table_scan,
                                                bool &table_scan_has_exchange, bool &has_px_coord);
  int check_sort_key_can_pushdown_to_tsc_for_gby(ObLogicalOperator *op,
                                                ObRawExpr *candidate_sk_expr, uint64_t table_id,
                                                ObLogicalOperator *&scan_op, bool &find_table_scan,
                                                bool &table_scan_has_exchange, bool &has_px_coord);
  int check_sort_key_can_pushdown_to_tsc_for_winfunc(ObLogicalOperator *op,
                                                 ObRawExpr *candidate_sk_expr, uint64_t table_id,
                                                 ObLogicalOperator *&scan_op, bool &find_table_scan,
                                                 bool &table_scan_has_exchange, bool &has_px_coord);
  int check_sort_key_can_pushdown_to_tsc_for_join(ObLogicalOperator *op,
                                                ObRawExpr *candidate_sk_expr, uint64_t table_id,
                                                ObLogicalOperator *&scan_op, bool &find_table_scan,
                                                bool &table_scan_has_exchange, bool &has_px_coord);


  /* manual set dop for each dfo */
  int refine_dop_by_hint();
  int check_has_temp_table_access(ObLogicalOperator *cur, bool &has_temp_table_access);

  int find_px_for_batch_rescan(const log_op_def::ObLogOpType, int64_t op_id, bool &find);
  int find_nested_dis_rescan(bool &find, bool nested);
  int add_op_exprs(ObRawExpr* expr);
  // alloc mat for sync in output
  int need_alloc_material_for_shared_hj(ObLogicalOperator &curr_op, bool &need_alloc);
  // alloc mat for sync in intput
  int need_alloc_material_for_push_down_wf(ObLogicalOperator &curr_op, bool &need_alloc);
  int check_need_parallel_valid(int64_t need_parallel) const;
  virtual int get_card_without_filter(double &card);
  virtual int check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index);
private:
  ObLogicalOperator *parent_;                           // parent operator
  bool is_plan_root_;                                // plan root operator
protected:
  double cost_;                                  // cost up to this point
  double op_cost_;                               // cost for this operator
  double card_;                                  // cardinality
  double width_;                                 // average row width
  // temporary traverse context pointer, maybe modified in every plan_tree_traverse
  void *traverse_ctx_;
  // 严格的partition wise join约束, 要求分组内的基表分区在逻辑上和物理上相同
  ObPwjConstraint strict_pwj_constraint_;
  // 非严格的partition wise join约束, 要求分组内的基表分区在物理上相同
  ObPwjConstraint non_strict_pwj_constraint_;
  //依赖的表的物理分布
  //ObLocationConstraint如果只有一个元素, 则仅需要约束该location type是否一致；
  //                    如果有多个元素，则需要校验每一项location对应的物理分布是否一样
  ObLocationConstraint location_constraint_;
  // 用于处理multi part dml的约束抽取
  // multi_part_location_constraint_仅仅只能有一个约束元素
  ObLocationConstraint multi_part_location_constraint_;
  bool exchange_allocated_;
  ObPhyPlanType phy_plan_type_;
  ObPhyPlanType location_type_;
  bool is_partition_wise_;
  PxOpSizeFactor px_est_size_factor_;
  // dblink
  uint64_t dblink_id_;
  int64_t plan_depth_;
  bool contain_fake_cte_;
  bool contain_pw_merge_op_;
  bool contain_das_op_;
  bool contain_match_all_fake_cte_;
  ObShardingInfo *strong_sharding_;
  common::ObSEArray<ObShardingInfo*, 8, common::ModulePageAllocator, true> weak_sharding_;
  bool is_pipelined_plan_;
  bool is_nl_style_pipelined_plan_;
  bool is_at_most_one_row_;
  bool is_local_order_;
  bool is_range_order_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> op_ordering_;
  const ObRawExprSets &empty_expr_sets_;
  const ObFdItemSet &empty_fd_item_set_;
  const ObRelIds &empty_table_set_;
  int64_t interesting_order_info_;  // 记录算子的序在stmt中的哪些地方用到 e.g. join, group by, order by
  int64_t parallel_;
  OpParallelRule op_parallel_rule_;
  int64_t available_parallel_;  // parallel degree used by serial op to enable parallel again
  int64_t server_cnt_;
  ObSEArray<common::ObAddr, 8, common::ModulePageAllocator, true> server_list_;
  bool need_late_materialization_;
  // all non_const exprs for this op, generated by allocate_expr_pre and used by project pruning
  ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> op_exprs_;
  // Used to indicate which child node the current sharding inherits from
  int64_t inherit_sharding_index_;
  // wether has allocated a osg_gather.
  bool need_osg_merge_;
  int64_t max_px_thread_branch_;
  int64_t max_px_group_branch_;
};

template <typename Allocator>
int ObLogicalOperator::init_all_traverse_ctx(Allocator &alloc)
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

// json table default value struct
struct ObColumnDefault
{
public:
  ObColumnDefault()
    : column_id_(common::OB_NOT_EXIST_COLUMN_ID),
      default_error_expr_(nullptr),
      default_empty_expr_(nullptr)
  {}
  ObColumnDefault(int64_t column_id)
    : column_id_(column_id),
      default_error_expr_(nullptr),
      default_empty_expr_(nullptr)
  {}
  void reset()
  {
    column_id_ = common::OB_NOT_EXIST_COLUMN_ID;
    default_error_expr_ = nullptr;
    default_empty_expr_ = nullptr;
  }

  TO_STRING_KV(K_(column_id), KPC_(default_error_expr), KPC_(default_empty_expr));
  int64_t column_id_;
  ObRawExpr* default_error_expr_;
  ObRawExpr* default_empty_expr_;
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOGICAL_OPERATOR_H
