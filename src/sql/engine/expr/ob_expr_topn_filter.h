/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"
#include "sql/engine/px/p2p_datahub/ob_pushdown_topn_filter_msg.h"

namespace oceanbase
{
namespace sql
{

class ObDynamicFilterExecutor;

using ObRuntimeFilterParams = common::ObSEArray<common::ObDatum, 4>;

class ObExprTopNFilterContext final: public ObExprOperatorCtx
{
public:
  enum class FilterState
  {
    NOT_READY = 0,
    CHECK_READY = 1,
    ENABLE = 2,
    DYNAMIC_DISABLE = 3,
  };

  ObExprTopNFilterContext()
      : ObExprOperatorCtx(), topn_filter_msg_(nullptr), topn_filter_key_(), cmp_funcs_(),
        start_time_(0), ready_time_(0), filter_count_(0), total_count_(0), check_count_(0),
        n_times_(0), n_rows_(0), slide_window_(total_count_), flag_(0),
        state_(FilterState::NOT_READY), row_selector_(nullptr)
  {
    is_first_ = true;
  }
  virtual ~ObExprTopNFilterContext();

public:
  inline bool need_reset_in_rescan() override final
  {
    return true;
  }
  void reset_for_rescan();
  inline bool dynamic_disable() override final
  {
    return slide_window_.dynamic_disable();
  }
  inline void collect_monitor_info(const int64_t filtered_rows_count,
                                   const int64_t check_rows_count,
                                   const int64_t total_rows_count) override final
  {
    filter_count_ += filtered_rows_count;
    check_count_ += check_rows_count;
    total_count_ += total_rows_count;
    if (FilterState::NOT_READY == state_) {
      n_times_ += 1;
      n_rows_ += total_rows_count;
      if (n_rows_ > ROW_COUNT_CHECK_INTERVAL || n_times_ > EVAL_TIME_CHECK_INTERVAL) {
        state_ = FilterState::CHECK_READY;
        n_times_ = 0;
        n_rows_ = 0;
      }
    }
  }

  inline bool is_data_version_updated(int64_t old_version) override
  {
    bool bool_ret = false;
    if (OB_NOT_NULL(topn_filter_msg_)) {
      bool_ret = topn_filter_msg_->get_current_data_version() > old_version;
    }
    return bool_ret;
  }

  inline void reset_monitor_info() {
    filter_count_ = 0;
    total_count_ = 0;
    check_count_ = 0;
    n_times_ = 0;
    n_rows_ = 0;
    ready_time_ = 0;
  }

  inline void collect_sample_info(const int64_t filter_count,
                                  const int64_t total_count) override final
  {
    (void)slide_window_.update_slide_window_info(filter_count, total_count);
  }

  template <typename... Args>
  int state_machine(const ObExpr &expr, ObEvalCtx &ctx, Args &&... args);

private:
  inline int bypass(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  inline int bypass(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                    const int64_t batch_size);
  inline int bypass(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                     const EvalBound &bound);
  inline int bypass(const ObExpr &expr, ObEvalCtx &ctx,ObDynamicFilterExecutor &dynamic_filter,
                    ObRuntimeFilterParams &params, bool &is_data_prepared);

  inline int do_process(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  inline int do_process(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                       const int64_t batch_size);
  inline int do_process(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                        const EvalBound &bound);
  inline int do_process(const ObExpr &expr, ObEvalCtx &ctx, ObDynamicFilterExecutor &dynamic_filter,
                        ObRuntimeFilterParams &params, bool &is_data_prepared);
  int check_filter_ready();

public:
  ObPushDownTopNFilterMsg *topn_filter_msg_;
  ObP2PDhKey topn_filter_key_;
  // TODO XUNSI: if the sort key(from left table) is join key (of right table),
  // and the type of them are different, then the cmp_func should be prepared.
  // Not support yet
  ObCmpFuncs cmp_funcs_;
  int64_t start_time_;
  int64_t ready_time_;
  int64_t filter_count_;
  int64_t total_count_;
  int64_t check_count_;
  int64_t n_times_;
  int64_t n_rows_;
  ObAdaptiveFilterSlideWindow slide_window_;

  union
  {
    uint64_t flag_;
    struct
    {
      bool is_first_ : 1;
      int64_t reserved_ : 63;
    };
  };
  FilterState state_;
  uint16_t *row_selector_;
public:
  static const int64_t ROW_COUNT_CHECK_INTERVAL;
  static const int64_t EVAL_TIME_CHECK_INTERVAL;
};

class ObExprTopNFilter final : public ObExprOperator
{
public:
  explicit ObExprTopNFilter(common::ObIAllocator &alloc)
      : ObExprOperator(alloc, T_OP_PUSHDOWN_TOPN_FILTER, "PUSHDOWN_TOPN_FILTER", MORE_THAN_ZERO,
                       VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
  {}
  ~ObExprTopNFilter()
  {}
  int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  inline bool need_rt_ctx() const override
  {
    return true;
  }

public:
  static int eval_topn_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_topn_filter_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                    const int64_t batch_size);
  static int eval_topn_filter_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                     const EvalBound &bound);

  static int prepare_storage_white_filter_data(const ObExpr &expr,
                                               ObDynamicFilterExecutor &dynamic_filter,
                                               ObEvalCtx &eval_ctx, ObRuntimeFilterParams &params,
                                               bool &is_data_prepared);

  static int update_storage_white_filter_data(const ObExpr &expr,
                                              ObDynamicFilterExecutor &dynamic_filter,
                                              ObEvalCtx &eval_ctx, ObRuntimeFilterParams &params,
                                              bool &is_update);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTopNFilter);
};

} // end namespace sql
} // end namespace oceanbase
