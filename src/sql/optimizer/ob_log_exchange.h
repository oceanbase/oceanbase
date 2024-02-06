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

#ifndef OCEANBASE_SQL_OB_LOG_EXCHANGE_H
#define OCEANBASE_SQL_OB_LOG_EXCHANGE_H
#include "lib/allocator/page_arena.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/engine/px/ob_px_basic_info.h"
namespace oceanbase
{
namespace sql
{
class ObLogExchange : public ObLogicalOperator
{
  typedef common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> RepartColumnExprs;
public:
  ObLogExchange(ObLogPlan &plan)
      : ObLogicalOperator(plan),
      is_producer_(false),
      is_rescanable_(false),
      dfo_id_(common::OB_INVALID_ID),
      px_id_(common::OB_INVALID_ID),
      expected_worker_count_(0),
      is_remote_(false),
      is_task_order_(false),
      is_merge_sort_(false),
      is_sort_local_order_(false),
      is_rollup_hybrid_(false),
      is_wf_hybrid_(false),
      wf_hybrid_aggr_status_expr_(NULL),
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
      dist_method_(ObPQDistributeMethod::LOCAL), // pull to local
      unmatch_row_dist_method_(ObPQDistributeMethod::LOCAL),
      null_row_dist_method_(ObNullDistributeMethod::NONE),
      slave_mapping_type_(SlaveMappingType::SM_NONE),
      gi_info_(),
      px_batch_op_id_(OB_INVALID_ID),
      px_batch_op_type_(log_op_def::LOG_OP_INVALID),
      partition_id_expr_(NULL),
      random_expr_(NULL),
      need_null_aware_shuffle_(false),
      is_old_unblock_mode_(true),
      sample_type_(NOT_INIT_SAMPLE_TYPE),
      in_server_cnt_(0)
  {
    repartition_table_id_ = 0;
  }
  virtual ~ObLogExchange()
  {}
  virtual const char *get_name() const;
  const common::ObIArray<OrderItem> &get_sort_keys() const { return sort_keys_; }
  common::ObIArray<OrderItem> &get_sort_keys() { return sort_keys_; }
  inline void set_to_consumer() { is_producer_ = false; }
  inline void set_to_producer() { is_producer_ = true; }
  inline bool is_producer() const { return is_producer_; }
  inline bool is_consumer() const { return !is_producer_; }
  inline bool is_px_producer() const { return is_producer_ && !is_remote_; }
  inline bool is_px_consumer() const { return !is_producer_ && !is_remote_; }
  inline bool is_px_coord() const { return is_px_consumer() && is_rescanable(); }
  inline void set_rescanable(bool rescan) { is_rescanable_ = rescan; }
  inline bool is_rescanable() const { return is_rescanable_; }
  inline void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  inline void set_px_id(int64_t px_id) { px_id_ = px_id; }
  inline int64_t get_dfo_id() const { return dfo_id_; }
  inline int64_t get_px_id() const { return px_id_; }
  inline bool is_px_dfo_root() const
  { return dfo_id_ != common::OB_INVALID_ID && px_id_ != common::OB_INVALID_ID; }
  inline bool get_is_remote() const { return is_remote_; }
  inline bool is_merge_sort() const { return is_merge_sort_; }
  inline bool is_sort_local_order() const { return is_sort_local_order_; }
  inline bool is_block_op() const { return is_sort_local_order_; }
  virtual int get_explain_name_internal(char *buf,
                                        const int64_t buf_len,
                                        int64_t &pos);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int set_exchange_info(const ObExchangeInfo &exch_info);
  const common::ObIArray<ObRawExpr *> &get_repart_keys() const {return repartition_keys_;}
  const common::ObIArray<ObRawExpr *> &get_repart_sub_keys() const {return repartition_sub_keys_;}
  const common::ObIArray<ObRawExpr *> &get_repart_func_exprs() const {return repartition_func_exprs_;}
  const common::ObIArray<ObExchangeInfo::HashExpr> &get_hash_dist_exprs() const {return hash_dist_exprs_;}
  const common::ObIArray<common::ObObj> *get_popular_values() const {return &popular_values_;}
  const ObRawExpr *get_calc_part_id_expr() { return calc_part_id_expr_; }
  ObRepartitionType get_repartition_type() const {return repartition_type_;}
  int64_t get_repartition_ref_table_id() const {return repartition_ref_table_id_;}
  int64_t get_repartition_table_id() const {return repartition_table_id_;}
  int64_t get_slice_count() const {return slice_count_; }
  bool is_repart_exchange() const { return OB_REPARTITION_NO_REPARTITION != repartition_type_; }
  bool is_pq_hash_dist() const { return ObPQDistributeMethod::HASH == dist_method_; }
  bool is_pq_broadcast_dist() const { return ObPQDistributeMethod::BROADCAST == dist_method_; }
  bool is_pq_pkey() const { return ObPQDistributeMethod::PARTITION == dist_method_; }
  bool is_pq_dist() const { return dist_method_ < ObPQDistributeMethod::LOCAL; }
  bool is_pq_local() const { return dist_method_ == ObPQDistributeMethod::LOCAL; }
  bool is_pq_random() const { return dist_method_ == ObPQDistributeMethod::RANDOM; }
  bool is_pq_pkey_hash() const { return dist_method_ == ObPQDistributeMethod::PARTITION_HASH;  }
  bool is_pq_pkey_rand() const { return dist_method_ == ObPQDistributeMethod::PARTITION_RANDOM; }
  bool is_pq_pkey_range() const { return dist_method_ == ObPQDistributeMethod::PARTITION_RANGE;}
  bool is_pq_range() const { return dist_method_ == ObPQDistributeMethod::RANGE; }
  ObPQDistributeMethod::Type get_dist_method() const { return dist_method_; }
  ObPQDistributeMethod::Type get_unmatch_row_dist_method() const { return unmatch_row_dist_method_; }
  ObNullDistributeMethod::Type get_null_row_dist_method() const { return null_row_dist_method_; }
  bool is_px_single() const { return is_single(); }
  void set_expected_worker_count(int64_t c) { expected_worker_count_ = c; }
  int64_t get_expected_worker_count() const { return expected_worker_count_; }
  virtual int px_pipe_blocking_pre(ObPxPipeBlockingCtx &ctx) override;
  virtual int px_pipe_blocking_post(ObPxPipeBlockingCtx &ctx) override;
  virtual int allocate_granule_post(AllocGIContext &ctx) override;
  virtual int allocate_granule_pre(AllocGIContext &ctx) override;
  virtual uint64_t hash(uint64_t seed) const override;
  bool is_task_order() const { return is_task_order_; }
  virtual int compute_op_ordering() override;
  virtual int compute_op_parallel_and_server_info() override;
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(int64_t parallel, double child_card, double &op_cost);
  const ObIArray<uint64_t> &get_repart_all_tablet_ids() const { return repart_all_tablet_ids_; }
  virtual int compute_sharding_info() override;
  virtual int compute_plan_type() override;
  SlaveMappingType get_slave_mapping_type() { return slave_mapping_type_; }
  bool is_slave_mapping() const { return SlaveMappingType::SM_NONE != slave_mapping_type_; }
  void set_px_batch_op_id(int64_t id) { px_batch_op_id_ = id; }
  void set_px_batch_op_type(log_op_def::ObLogOpType px_batch_op_type)
  { px_batch_op_type_ = px_batch_op_type; }
  int64_t get_px_batch_op_id() { return px_batch_op_id_; }

  void set_rollup_hybrid(bool is_rollup_hybrid) { is_rollup_hybrid_ = is_rollup_hybrid; }
  bool is_rollup_hybrid() { return is_rollup_hybrid_; }

  void set_wf_hybrid(bool is_wf_hybrid) { is_wf_hybrid_ = is_wf_hybrid; }
  bool is_wf_hybrid() { return is_wf_hybrid_; }
  void set_wf_hybrid_aggr_status_expr(ObRawExpr *wf_hybrid_aggr_status_expr)
  {
    wf_hybrid_aggr_status_expr_ = wf_hybrid_aggr_status_expr;
  }
  ObRawExpr *get_wf_hybrid_aggr_status_expr() { return wf_hybrid_aggr_status_expr_; }

  common::ObIArray<int64_t> &get_wf_hybrid_pby_exprs_cnt_array()
  {
    return wf_hybrid_pby_exprs_cnt_array_;
  }

  log_op_def::ObLogOpType get_px_batch_op_type() { return px_batch_op_type_;}
  common::ObIArray<ObTableLocation> &get_pruning_table_locations() { return table_locations_; }
  common::ObIArray<int64_t> &get_bloom_filter_ids() { return filter_id_array_; }
  int gen_px_pruning_table_locations();
  int allocate_startup_expr_post()override;
  void set_old_unblock_mode(bool old_unblock_mode) { is_old_unblock_mode_ = old_unblock_mode; }
  bool is_old_unblock_mode() { return is_old_unblock_mode_; }
  void set_partition_id_expr(ObOpPseudoColumnRawExpr *expr) { partition_id_expr_ = expr; }
  ObOpPseudoColumnRawExpr *get_partition_id_expr() { return partition_id_expr_; }
  bool need_null_aware_shuffle() const { return need_null_aware_shuffle_; }
  void set_need_null_aware_shuffle(const bool need_null_aware_shuffle)
                    { need_null_aware_shuffle_ = need_null_aware_shuffle; }
  void set_sample_type(ObPxSampleType type) { sample_type_ = type; }
  ObPxSampleType get_sample_type() { return sample_type_; }

  void set_random_expr(ObRawExpr *expr) { random_expr_ = expr; }
  ObRawExpr *get_random_expr() const { return random_expr_; }
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  int get_plan_special_expr_info(PlanText &plan_text,
                                 ObSqlPlanItem &plan_item);
  int get_plan_distribution(PlanText &plan_text,
                            ObSqlPlanItem &plan_item);
  int print_annotation_keys(char *buf,
                            int64_t &buf_len,
                            int64_t &pos,
                            ExplainType type,
                            const ObIArray<ObRawExpr *> &keys);
  inline void set_in_server_cnt(int64_t in_server_cnt) {  in_server_cnt_ = in_server_cnt;  }
  inline int64_t get_in_server_cnt() {  return in_server_cnt_;  }
private:
  int prepare_px_pruning_param(ObLogicalOperator *op, int64_t &count,
      common::ObIArray<const ObDMLStmt *> &stmts, common::ObIArray<int64_t> &drop_expr_idxs);
  int add_px_table_location(ObLogicalOperator *op,
      common::ObIArray<ObTableLocation> &table_locations,
      common::ObIArray<int64_t> &drop_expr_idxs,
      const common::ObIArray<const ObDMLStmt *> &stmts,
      int64_t &cur_idx);
  int find_need_drop_expr_idxs(ObLogicalOperator *op,
      common::ObIArray<int64_t> &drop_expr_idxs,
      log_op_def::ObLogOpType type);
  int find_table_location_exprs(const common::ObIArray<int64_t> &drop_expr_idxs,
      const common::ObIArray<ObRawExpr *> &filters,
      common::ObIArray<ObRawExpr *> &exprs, bool &has_exec_param);
  int check_expr_is_need(const ObRawExpr *expr,
      const common::ObIArray<int64_t> &drop_expr_idxs,
      bool &is_need);
private:
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

private:
  // the 'partition key' expressions
  bool is_producer_;                                    /* true if the exchange the producer */
  bool is_rescanable_; /* true if this is exchange receive and can be rescan  */
  int64_t dfo_id_; // 在 CG 之前就给 dfo 定下 id
  int64_t px_id_; // 在 CG 之前就给多个 px 的 plan 定下每个 px 的 id
  int64_t expected_worker_count_; // 仅供 QC 节点使用，其余 exchange 节点均为 0

  bool is_remote_; /* true if the exchange is remote single-server */
  bool is_task_order_; // true if the input data is task order
  bool is_merge_sort_; // true if need merge sort for partition data
  bool is_sort_local_order_; // true if need local order sort
  bool is_rollup_hybrid_;  // for adaptive rollup pushdown
  bool is_wf_hybrid_;  // for adaptive window function pushdown
  ObRawExpr *wf_hybrid_aggr_status_expr_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> wf_hybrid_pby_exprs_cnt_array_;
  common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> sort_keys_;

  int64_t slice_count_;//对于重分发之外的exchange, slice_count均为1
  ObRepartitionType repartition_type_;
  int64_t repartition_ref_table_id_;
  int64_t repartition_table_id_;
  ObString repartition_table_name_; //just for print plan
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> repart_all_tablet_ids_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> repartition_keys_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> repartition_sub_keys_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> repartition_func_exprs_;
  ObRawExpr *calc_part_id_expr_;
  common::ObSEArray<ObExchangeInfo::HashExpr, 4, common::ModulePageAllocator, true> hash_dist_exprs_;
  common::ObSEArray<ObObj, 20, common::ModulePageAllocator, true> popular_values_; // for hybrid hash distr

  ObPQDistributeMethod::Type dist_method_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;
  ObNullDistributeMethod::Type null_row_dist_method_;
  SlaveMappingType slave_mapping_type_;

  //granule info
  ObAllocGIInfo gi_info_;
  // px batch rescan drive op
  int64_t px_batch_op_id_;
  log_op_def::ObLogOpType px_batch_op_type_;
  ObOpPseudoColumnRawExpr *partition_id_expr_;

  // produce random number, added in %sort_keys_ of range distribution to splitting big range.
  ObRawExpr *random_expr_;

  common::ObSEArray<ObTableLocation, 4, common::ModulePageAllocator, true> table_locations_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> filter_id_array_;
  // new shuffle method for non-preserved side in naaj
  // broadcast 1st line && null join key
  bool need_null_aware_shuffle_;
  bool is_old_unblock_mode_;
  // -for pkey range/range
  ObPxSampleType sample_type_;
  // -end pkey range/range
  int64_t in_server_cnt_; // for producer, need use exchange in server cnt to compute cost
  DISALLOW_COPY_AND_ASSIGN(ObLogExchange);
};
} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_EXCHANGE_H
