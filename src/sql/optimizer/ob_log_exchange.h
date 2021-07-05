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

namespace oceanbase {
namespace sql {
class ObLogExchange : public ObLogicalOperator {
  typedef common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> RepartColumnExprs;

public:
  ObLogExchange(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        is_producer_(false),
        is_remote_(true),
        is_merge_sort_(false),
        is_local_order_(false),
        is_rescanable_(false),
        dfo_id_(common::OB_INVALID_ID),
        px_id_(common::OB_INVALID_ID),
        expected_worker_count_(0),
        sort_keys_(),
        exch_info_(),
        gi_info_()
  {}
  virtual ~ObLogExchange()
  {}
  virtual int est_cost() override;
  virtual const char* get_name() const;
  int set_sort_keys(const common::ObIArray<OrderItem>& order_keys);
  const common::ObIArray<OrderItem>& get_sort_keys() const
  {
    return sort_keys_;
  }
  common::ObIArray<OrderItem>& get_sort_keys()
  {
    return sort_keys_;
  }
  inline void set_to_consumer()
  {
    is_producer_ = false;
  }
  inline void set_to_producer()
  {
    is_producer_ = true;
  }
  inline bool is_producer() const
  {
    return is_producer_;
  }
  inline bool is_consumer() const
  {
    return !is_producer_;
  }
  inline bool is_px_producer() const
  {
    return is_producer_ && !is_remote_;
  }
  inline bool is_px_consumer() const
  {
    return !is_producer_ && !is_remote_;
  }
  inline void set_rescanable(bool rescan)
  {
    is_rescanable_ = rescan;
  }
  inline bool is_rescanable() const
  {
    return is_rescanable_;
  }
  inline void set_is_remote(bool is_remote)
  {
    is_remote_ = is_remote;
  }
  inline void set_task_order(bool task_order)
  {
    exch_info_.is_task_order_ = task_order;
  }
  inline void set_is_merge_sort(bool is_merge_sort)
  {
    is_merge_sort_ = is_merge_sort;
  }
  inline void set_dfo_id(int64_t dfo_id)
  {
    dfo_id_ = dfo_id;
  }
  inline void set_px_id(int64_t px_id)
  {
    px_id_ = px_id;
  }
  inline int64_t get_dfo_id() const
  {
    return dfo_id_;
  }
  inline int64_t get_px_id() const
  {
    return px_id_;
  }
  inline bool is_px_dfo_root() const
  {
    return dfo_id_ != common::OB_INVALID_ID && px_id_ != common::OB_INVALID_ID;
  }
  inline bool get_is_remote() const
  {
    return is_remote_;
  }
  inline bool is_task_order() const
  {
    return exch_info_.is_task_order_;
  }
  inline bool is_keep_order() const
  {
    return exch_info_.keep_ordering_;
  }
  inline bool is_pdml_pkey() const
  {
    return exch_info_.pdml_pkey_;
  }
  inline bool is_merge_sort() const
  {
    return is_merge_sort_;
  }
  inline bool is_block_op() const
  {
    return is_local_order_;
  }
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int32_t get_explain_name_length() const;
  virtual int get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos);
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int set_exchange_info(ObExchangeInfo& exch_info);
  virtual int transmit_op_ordering();
  const common::ObIArray<ObRawExpr*>& get_repart_keys() const
  {
    return exch_info_.repartition_keys_;
  }
  const common::ObIArray<ObRawExpr*>& get_repart_sub_keys() const
  {
    return exch_info_.repartition_sub_keys_;
  }
  const common::ObIArray<ObRawExpr*>& get_repart_func_exprs() const
  {
    return exch_info_.repartition_func_exprs_;
  }
  const common::ObIArray<ObExchangeInfo::HashExpr>& get_hash_dist_exprs() const
  {
    return exch_info_.hash_dist_exprs_;
  }
  const ObRawExpr* get_calc_part_id_expr()
  {
    return exch_info_.calc_part_id_expr_;
  }
  ObRepartitionType get_repartition_type() const
  {
    return exch_info_.repartition_type_;
  }
  int64_t get_repartition_table_id() const
  {
    return exch_info_.repartition_ref_table_id_;
  }
  int64_t get_slice_count() const
  {
    return exch_info_.slice_count_;
  }
  bool is_repart_exchange() const
  {
    return exch_info_.is_repart_exchange();
  }
  bool is_pq_dist() const
  {
    return exch_info_.is_pq_dist();
  }
  ObPQDistributeMethod::Type get_dist_method() const
  {
    return exch_info_.dist_method_;
  }
  ObPQDistributeMethod::Type get_unmatch_row_dist_method() const
  {
    return exch_info_.unmatch_row_dist_method_;
  }
  bool is_px_single() const
  {
    return exch_info_.px_single_;
  }
  int64_t get_px_dop() const
  {
    return exch_info_.px_dop_;
  }
  void set_expected_worker_count(int64_t c)
  {
    expected_worker_count_ = c;
  }
  int64_t get_expected_worker_count() const
  {
    return expected_worker_count_;
  }
  virtual int px_pipe_blocking_pre(ObPxPipeBlockingCtx& ctx) override;
  virtual int px_pipe_blocking_post(ObPxPipeBlockingCtx& ctx) override;
  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  uint64_t hash(uint64_t seed) const;
  bool is_local_order() const
  {
    return is_local_order_;
  }
  void set_local_order(bool local_order)
  {
    is_local_order_ = local_order;
  }
  virtual int compute_op_ordering() override;
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  SlaveMappingType get_slave_mapping_type()
  {
    return exch_info_.get_slave_mapping_type();
  }
  bool is_slave_mapping() const
  {
    return SlaveMappingType::SM_NONE != exch_info_.get_slave_mapping_type();
  }
  int update_sharding_conds(AllocExchContext& ctx);

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int print_plan_head_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type) override;

  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs) override;

private:
  // the 'partition key' expressions
  bool is_producer_;    /* true if the exchange the producer */
  bool is_remote_;      /* true if the exchange is remote single-server */
  bool is_merge_sort_;  // true if need merge sort for partition data
  bool is_local_order_; /* true if the input data is local ordering */
  bool is_rescanable_;  /* true if this is exchange receive and can be rescan  */
  int64_t dfo_id_;
  int64_t px_id_;
  int64_t expected_worker_count_;
  common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> sort_keys_;

  // repart info
  ObExchangeInfo exch_info_;

  // granule info
  ObAllocGIInfo gi_info_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExchange);
};
}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_EXCHANGE_H
