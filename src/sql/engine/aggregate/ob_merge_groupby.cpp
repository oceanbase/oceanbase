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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "lib/utility/utility.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
// REGISTER_PHY_OPERATOR(ObMergeGroupBy, PHY_MERGE_GROUP_BY);

class ObMergeGroupBy::ObMergeGroupByCtx : public ObGroupByCtx {
public:
  explicit ObMergeGroupByCtx(ObExecContext& exec_ctx)
      : ObGroupByCtx(exec_ctx), last_input_row_(NULL), is_end_(false), cur_output_group_id(-1), first_output_group_id(0)
  {}
  virtual void destroy()
  {
    ObGroupByCtx::destroy();
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupByCtx);

private:
  const ObNewRow* last_input_row_;
  bool is_end_;
  // added to support groupby with rollup
  int64_t cur_output_group_id;
  int64_t first_output_group_id;

  friend class ObMergeGroupBy;
};

ObMergeGroupBy::ObMergeGroupBy(ObIAllocator& alloc) : ObGroupBy(alloc)
{}

ObMergeGroupBy::~ObMergeGroupBy()
{}

int ObMergeGroupBy::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObMergeGroupByCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObMergeGroupBy::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupBy::init_group_by(ctx))) {
    LOG_WARN("init group by failed", K(ret));
  } else {
    ObMergeGroupByCtx* groupby_ctx = GET_PHY_OPERATOR_CTX(ObMergeGroupByCtx, ctx, get_id());
    if (OB_ISNULL(groupby_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group by ctx is NULL", K(ret));
    } else {
      groupby_ctx->get_aggr_func().set_sort_based_gby();
    }
  }
  return ret;
}

int ObMergeGroupBy::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeGroupByCtx* groupby_ctx = NULL;
  if (OB_FAIL(ObGroupBy::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(groupby_ctx = GET_PHY_OPERATOR_CTX(ObMergeGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("groupby_ctx is null");
  } else {
    groupby_ctx->aggr_func_.reuse();
    groupby_ctx->last_input_row_ = NULL;
    groupby_ctx->is_end_ = false;
    groupby_ctx->cur_output_group_id = -1;
    groupby_ctx->first_output_group_id = 0;
  }
  return ret;
}

int ObMergeGroupBy::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  return ret;
}

int ObMergeGroupBy::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeGroupByCtx* groupby_ctx = NULL;
  int64_t stop_output_group_id = group_col_idxs_.count();
  int64_t col_count = group_col_idxs_.count() + rollup_col_idxs_.count();
  if (OB_ISNULL(child_op_) || OB_ISNULL(groupby_ctx = GET_PHY_OPERATOR_CTX(ObMergeGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ObMergeGroupByCtx failed", K_(child_op));
  } else if (has_rollup_ && groupby_ctx->cur_output_group_id >= groupby_ctx->first_output_group_id &&
             groupby_ctx->cur_output_group_id >= stop_output_group_id) {
    // output roll-up results here
    if (OB_FAIL(rollup_and_calc_results(groupby_ctx->cur_output_group_id, groupby_ctx))) {
      LOG_WARN("failed to rollup and calculate results", K(groupby_ctx->cur_output_group_id), K(ret));
    } else {
      --groupby_ctx->cur_output_group_id;
    }
  } else {
    // output group by results here
    if (groupby_ctx->is_end_) {
      ret = OB_ITER_END;
    } else if (NULL == groupby_ctx->last_input_row_) {
      // get the first input row
      if (OB_FAIL(child_op_->get_next_row(ctx, groupby_ctx->last_input_row_))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (has_rollup_ && OB_FAIL(groupby_ctx->aggr_func_.rollup_init(col_count))) {
        LOG_WARN("failed to initialize roll up", K(ret));
      }
    }
    int64_t group_id = 0;
    if (OB_SUCC(ret) && has_rollup_) {
      group_id = col_count;
    }
    if (OB_SUCC(ret) && groupby_ctx->last_input_row_ != NULL) {
      if (OB_FAIL(groupby_ctx->aggr_func_.prepare(*groupby_ctx->last_input_row_, group_id))) {
        LOG_WARN("failed to init aggr cells", K(ret));
      } else {
        groupby_ctx->last_input_row_ = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      bool same_group = false;
      const ObNewRow* input_row = NULL;
      const ObRowStore::StoredRow* stored_row = NULL;
      ObSQLSessionInfo* my_session = groupby_ctx->exec_ctx_.get_my_session();
      const ObTimeZoneInfo* tz_info = (my_session != NULL) ? my_session->get_timezone_info() : NULL;
      bool is_break = false;
      int64_t first_diff_pos = 0;
      while (OB_SUCC(ret) && !is_break && OB_SUCC(child_op_->get_next_row(ctx, input_row))) {
        if (OB_FAIL(try_check_status(ctx))) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(groupby_ctx->aggr_func_.get_cur_row(stored_row, group_id))) {
          LOG_WARN("fail to get cur row from aggr_func", K(ret));
        } else if (OB_ISNULL(stored_row) || OB_ISNULL(input_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the stored row is NULL");
        } else if (OB_FAIL(is_same_group(*stored_row, *input_row, same_group, first_diff_pos))) {
          LOG_WARN("failed to check group", K(ret));
        } else if (same_group) {
          if (OB_FAIL(groupby_ctx->aggr_func_.process(*input_row, tz_info, group_id))) {
            LOG_WARN("failed to calc aggr", K(ret));
          } else if (mem_size_limit_ > 0 && mem_size_limit_ < groupby_ctx->aggr_func_.get_used_mem_size()) {
            ret = OB_EXCEED_MEM_LIMIT;
            LOG_WARN("merge group by has exceeded the mem limit",
                K_(mem_size_limit),
                "aggr mem size",
                groupby_ctx->aggr_func_.get_used_mem_size());
          }
        } else if (OB_FAIL(rollup_and_calc_results(group_id, groupby_ctx))) {
          // ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to rollup and calculate results", K(group_id), K(ret));
        } else {
          groupby_ctx->last_input_row_ = input_row;
          is_break = true;
          if (has_rollup_) {
            groupby_ctx->first_output_group_id = first_diff_pos + 1;
            groupby_ctx->cur_output_group_id = group_id - 1;
          }
        }
      }  // end while
      if (OB_ITER_END == ret) {
        // the last group
        groupby_ctx->is_end_ = true;
        if (OB_FAIL(rollup_and_calc_results(group_id, groupby_ctx))) {
          LOG_WARN("failed to rollup and calculate results", K(group_id), K(ret));
        } else if (has_rollup_) {
          groupby_ctx->first_output_group_id = 0;
          groupby_ctx->cur_output_group_id = group_id - 1;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &groupby_ctx->get_cur_row();
  }
  return ret;
}

int ObMergeGroupBy::rollup_and_calc_results(const int64_t group_id, ObMergeGroupByCtx* groupby_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t col_count = group_col_idxs_.count() + rollup_col_idxs_.count();
  if (OB_UNLIKELY(OB_ISNULL(groupby_ctx) || group_id < 0 || group_id > col_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(group_id), K(ret));
  } else {
    ObSQLSessionInfo* my_session = groupby_ctx->exec_ctx_.get_my_session();
    const ObTimeZoneInfo* tz_info = (my_session != NULL) ? my_session->get_timezone_info() : NULL;
    // for: SELECT GROUPING(z0_test0) FROM Z0CASE GROUP BY z0_test0, ROLLUP(z0_test0);
    bool set_grouping = true;
    if (group_id > group_col_idxs_.count()) {
      int64_t rollup_id = rollup_col_idxs_[group_id - group_col_idxs_.count() - 1].index_;
      for (int64_t i = 0; set_grouping && i < group_col_idxs_.count(); ++i) {
        if (rollup_id == group_col_idxs_[i].index_) {
          set_grouping = false;
        }
      }
    }
    if (has_rollup_ && group_id > 0 &&
        OB_FAIL(groupby_ctx->aggr_func_.rollup_process(tz_info,
            group_id - 1,
            group_id,
            group_id <= group_col_idxs_.count() ? group_col_idxs_[group_id - 1].index_
                                                : rollup_col_idxs_[group_id - group_col_idxs_.count() - 1].index_,
            set_grouping))) {
      // ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to rollup aggregation results", K(ret));
    } else if (OB_FAIL(groupby_ctx->aggr_func_.get_result(groupby_ctx->get_cur_row(), tz_info, group_id))) {
      LOG_WARN("failed to get aggr result", K(group_id), K(ret));
    } else if (OB_FAIL(groupby_ctx->aggr_func_.reuse_group(group_id))) {
      LOG_WARN("failed to reuse group", K(group_id), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
