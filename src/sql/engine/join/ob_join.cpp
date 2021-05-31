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

#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/object/ob_obj_compare.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/join/ob_join.h"
#include "sql/parser/ob_item_type_str.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
const int64_t ObJoin::DUMMY_OUPUT = -1;
const int64_t ObJoin::UNUSED_POS = -2;
ObJoin::ObJoinCtx::ObJoinCtx(ObExecContext& ctx)
    : ObPhyOperatorCtx(ctx),
      left_row_(NULL),
      right_row_(NULL),
      last_left_row_(NULL),
      last_right_row_(NULL),
      left_row_joined_(false)
{}

ObJoin::ObJoin(ObIAllocator& alloc)
    : ObDoubleChildrenPhyOperator(alloc),
      join_type_(UNKNOWN_JOIN),
      equal_join_conds_(),
      other_join_conds_(),
      pump_row_desc_(alloc),
      root_row_desc_(alloc),
      pseudo_column_row_desc_(alloc),
      sort_siblings_columns_(alloc),
      connect_by_prior_exprs_(),
      sort_siblings_exprs_(),
      connect_by_root_exprs_(),
      is_nocycle_(false)
{}

ObJoin::~ObJoin()
{
  reset();
}

void ObJoin::reset()
{
  join_type_ = UNKNOWN_JOIN;
  equal_join_conds_.reset();
  other_join_conds_.reset();
  pump_row_desc_.reset();
  root_row_desc_.reset();
  pseudo_column_row_desc_.reset();
  sort_siblings_columns_.reset();
  connect_by_prior_exprs_.reset();
  sort_siblings_exprs_.reset();
  connect_by_root_exprs_.reset();
  is_nocycle_ = false;
  ObDoubleChildrenPhyOperator::reset();
}

void ObJoin::reuse()
{
  reset();
}

int ObJoin::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (UNKNOWN_JOIN == (OB_I(t1) join_type_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("join type is needed", K(ret));
  } else if (OB_FAIL(init_op_ctx(exec_ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_ISNULL(GET_PHY_OPERATOR_CTX(ObJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get join ctx", K(ret));
  }
  return ret;
}

int ObJoin::get_left_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (NULL != join_ctx.last_left_row_) {
    join_ctx.left_row_ = join_ctx.last_left_row_;
    join_ctx.last_left_row_ = NULL;
  } else {
    join_ctx.left_row_joined_ = false;
    if (OB_FAIL(OB_I(t1) left_op_->get_next_row(join_ctx.exec_ctx_, join_ctx.left_row_))) {
      join_ctx.left_row_ = NULL;
    } else {
      LOG_DEBUG("Success to get left row, ", K(*join_ctx.left_row_));
    }
  }
  return ret;
}

int ObJoin::get_right_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (NULL != join_ctx.last_right_row_) {
    join_ctx.right_row_ = join_ctx.last_right_row_;
    join_ctx.last_right_row_ = NULL;
  } else {
    if (OB_FAIL(OB_I(t1) right_op_->get_next_row(join_ctx.exec_ctx_, join_ctx.right_row_))) {
      join_ctx.right_row_ = NULL;
    } else {
      LOG_DEBUG("Success to get right row, ", K(*join_ctx.right_row_));
    }
  }
  return ret;
}

int ObJoin::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObJoinCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get join ctx", K(ret));
  } else {
    join_ctx->left_row_ = NULL;
    join_ctx->right_row_ = NULL;
    join_ctx->last_left_row_ = NULL;
    join_ctx->last_right_row_ = NULL;
    join_ctx->left_row_joined_ = false;
    if (OB_FAIL(ObDoubleChildrenPhyOperator::rescan(exec_ctx))) {
      LOG_WARN("failed to call parent rescan", K(ret));
    } else {
    }
  }
  return ret;
}

int ObJoin::get_next_left_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  join_ctx.left_row_joined_ = false;
  if (OB_FAIL(OB_I(t1) left_op_->get_next_row(join_ctx.exec_ctx_, join_ctx.left_row_))) {
    join_ctx.left_row_ = NULL;
  } else {
    LOG_DEBUG("Success to get next left row, ", K(*join_ctx.left_row_));
  }
  return ret;
}

int ObJoin::get_next_right_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) right_op_->get_next_row(join_ctx.exec_ctx_, join_ctx.right_row_))) {
    join_ctx.right_row_ = NULL;
  } else {
    LOG_DEBUG("Success to get next right row, ", K(*join_ctx.right_row_));
  }
  return ret;
}

int ObJoin::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("failed to create operator context", K(ret), K_(id));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create operator context", K(ret), K_(id));
  } else {
    bool need_create_cells = false;
    if (IS_SEMI_ANTI_JOIN(join_type_)) {
      need_create_cells = need_copy_row_for_compute();
    } else {
      need_create_cells = true;
    }
    if (OB_FAIL(init_cur_row(*op_ctx, need_create_cells))) {
      LOG_WARN("failed to init current row", K(ret));
    }
  }
  return ret;
}

int ObJoin::set_join_type(const ObJoinType join_type)
{
  join_type_ = join_type;
  return OB_SUCCESS;
}

int ObJoin::add_equijoin_condition(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(equal_join_conds_, expr);
}

int ObJoin::add_other_join_condition(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(other_join_conds_, expr);
}

int ObJoin::join_rows(ObJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* left_row = join_ctx.left_row_;
  const ObNewRow* right_row = join_ctx.right_row_;
  ObNewRow& cur_row = join_ctx.cur_row_;
  row = NULL;
  if (OB_ISNULL(left_row) || OB_ISNULL(right_row) || OB_ISNULL(left_row->cells_) || OB_ISNULL(right_row->cells_) ||
      OB_ISNULL(cur_row.cells_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left / right row is null, or left row / right row / cur row cells is null", K(ret));
  } else if (OB_I(t1)(cur_row.count_ < left_row->projector_size_ + right_row->projector_size_)) {
    ret = OB_ERR_UNEXPECTED;
    // cur_row.count_ may contains const content,
    LOG_WARN("join row column count is not match with left and right row",
        K(ret),
        K_(cur_row.count),
        K_(left_row->projector_size),
        K_(right_row->projector_size));
  } else {
    if (NULL == left_row->projector_) {
      MEMCPY(cur_row.cells_, left_row->cells_, left_row->get_count());
    } else {
      for (int64_t i = 0; i < left_row->get_count(); ++i) {
        cur_row.cells_[i] = left_row->get_cell(i);
      }
    }

    if (NULL == right_row->projector_) {
      MEMCPY(cur_row.cells_ + left_row->get_count(), right_row->cells_, right_row->get_count());
    } else {
      for (int64_t i = 0; i < right_row->get_count(); ++i) {
        cur_row.cells_[left_row->get_count() + i] = right_row->get_cell(i);
      }
    }
    row = &cur_row;
  }
  return ret;
}

int ObJoin::left_join_rows(ObJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* left_row = join_ctx.left_row_;
  ObNewRow& cur_row = join_ctx.cur_row_;
  row = NULL;
  if (OB_ISNULL(left_row) || OB_ISNULL(right_op_) || OB_ISNULL(left_row->cells_) || OB_ISNULL(cur_row.cells_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left row or right op is null, or left row / cur row cells is null", K(ret));
  } else {
    const int64_t right_row_size = right_op_->get_projector_size();
    if (OB_I(t1)(right_row_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right row column count is invalid", K(ret), K(right_row_size));
    } else if ((cur_row.count_ < left_row->get_count() + right_row_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join row column count is not match with left and right row",
          K(ret),
          K_(cur_row.count),
          K_(left_row->projector_size),
          K(right_row_size));
    } else {
      int64_t i = 0;
      for (i = 0; i < left_row->projector_size_; ++i) {
        cur_row.cells_[i] = left_row->cells_[left_row->projector_[i]];
      }
      for (i = 0; i < right_row_size; ++i) {
        cur_row.cells_[left_row->projector_size_ + i].set_null();
      }
      row = &cur_row;
    }
  }
  return ret;
}

int ObJoin::right_join_rows(ObJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* right_row = join_ctx.right_row_;
  ObNewRow& cur_row = join_ctx.cur_row_;
  row = NULL;
  if (OB_ISNULL(left_op_) || OB_ISNULL(right_row) || OB_ISNULL(right_row->cells_) || OB_ISNULL(cur_row.cells_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left row or right op is null, or right row / cur row cells is null", K(ret));
  } else {
    const int64_t left_row_size = left_op_->get_projector_size();
    if (OB_I(t1)(left_row_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row column count is invalid", K(ret));
    } else if ((cur_row.count_ < left_row_size + right_row->get_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join row column count is not match with left and right row",
          K(ret),
          K_(cur_row.count),
          K(left_row_size),
          K_(right_row->projector_size));
    } else {
      int64_t i = 0;
      for (i = 0; i < left_row_size; ++i) {
        cur_row.cells_[i].set_null();
      }
      for (i = 0; i < right_row->projector_size_; ++i) {
        cur_row.cells_[left_row_size + i] = right_row->cells_[right_row->projector_[i]];
      }
      row = &cur_row;
    }
  }
  return ret;
}

int ObJoin::calc_equal_conds(
    ObJoinCtx& join_ctx, int64_t& equal_cmp, const common::ObIArray<int64_t>* merge_directions) const
{
  int ret = OB_SUCCESS;
  equal_cmp = 0;
  if (equal_join_conds_.get_size() > 0) {
    ObSQLSessionInfo* my_session = join_ctx.exec_ctx_.get_my_session();
    const ObTimeZoneInfo* tz_info = OB_ISNULL(my_session) ? NULL : my_session->get_timezone_info();
    ObCastMode cast_mode = CM_NONE;
    ObCollationType collation_connection = CS_TYPE_INVALID;
    int64_t tz_offset = INVALID_TZ_OFF;
    if (OB_ISNULL(my_session) || OB_ISNULL(my_phy_plan_) || OB_ISNULL(join_ctx.left_row_) ||
        OB_ISNULL(join_ctx.right_row_)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("session or phy plan or left / right row is null", K(ret));
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(my_phy_plan_, my_session, cast_mode))) {
      LOG_WARN("failed to get cast_mode", K(ret));
    } else if (OB_FAIL(my_session->get_collation_connection(collation_connection))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
      LOG_WARN("get tz offset failed", K(ret));
    } else {
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(my_session);
      ObCastCtx cast_ctx(join_ctx.calc_buf_, &dtc_params, cast_mode, collation_connection);
      int i = 0;
      int64_t col1 = OB_INVALID_INDEX;
      int64_t col2 = OB_INVALID_INDEX;
      bool is_null_safe = false;
      ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, true, tz_offset, default_null_pos());
      DLIST_FOREACH(node, equal_join_conds_)
      {
        if (OB_ISNULL(node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("node or node expr is null", K(ret));
        } else if (node->is_equijoin_cond(col1, col2, cmp_ctx.cmp_type_, cmp_ctx.cmp_cs_type_, is_null_safe)) {
          // The character set saved by cmp_ctx.cmp_cs_type_ needs to be
          // used for type conversion, otherwise there is no setting. MySQL
          // mode will use utf8mb4_general_ci as default
          cast_ctx.dest_collation_ = cmp_ctx.cmp_cs_type_;
          const ObObj* cell1 = NULL;
          const ObObj* cell2 = NULL;
          if (col1 < col2) {
            // left table's join column in '=''s left
            if (OB_UNLIKELY(col1 >= join_ctx.left_row_->count_ || col2 < join_ctx.left_row_->count_)) {
              ret = OB_ARRAY_OUT_OF_RANGE;
              LOG_WARN("column index is out of range",
                  K(ret),
                  K(col1),
                  K(col2),
                  "left row count",
                  join_ctx.left_row_->count_,
                  "right row count",
                  join_ctx.right_row_->count_);
            } else {
              cell1 = &(join_ctx.left_row_->cells_[col1]);
              cell2 = &(join_ctx.right_row_->cells_[col2 - join_ctx.left_row_->count_]);
            }
          } else {
            // right table's join column in '=''s right
            if (OB_UNLIKELY(col1 < join_ctx.left_row_->count_ || col2 >= join_ctx.left_row_->count_)) {
              ret = OB_ARRAY_OUT_OF_RANGE;
              LOG_WARN("column index is out of range",
                  K(ret),
                  K(col1),
                  K(col2),
                  "left row count",
                  join_ctx.left_row_->count_,
                  "right row count",
                  join_ctx.right_row_->count_);
            } else {
              cell1 = &(join_ctx.left_row_->cells_[col2]);
              cell2 = &(join_ctx.right_row_->cells_[col1 - join_ctx.left_row_->count_]);
            }
          }
          // cell1 and cell2 is address of cells element, so we need NOT judge NULL.
          if (cell1->is_null() && cell2->is_null()) {
            equal_cmp = is_null_safe ? 0 : -1;
          } else {
            ObObj result;
            if (OB_I(t1)
                    OB_FAIL(ObRelationalExprOperator::compare(result, *cell1, *cell2, cmp_ctx, cast_ctx, CO_CMP))) {
              LOG_WARN("failed to compare expr", K(ret), K(cell1), K(cell2));
            } else if (0 != (equal_cmp = result.get_int()) && NULL != merge_directions) {
              equal_cmp *= merge_directions->at(i);
            }
          }
          ++i;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid equijoin condition", K(ret));
        }
        if (0 != equal_cmp) {
          // we use DLIST_FOREACH MACRO to loop, so we use break here.
          break;
        }
      }
    }
  }
  return ret;
}

int ObJoin::calc_other_conds(ObJoinCtx& join_ctx, bool& is_match) const
{
  int ret = OB_SUCCESS;
  is_match = true;
  if (other_join_conds_.get_size() > 0) {
    ObExprCtx expr_ctx;
    ObSQLSessionInfo* my_session = join_ctx.exec_ctx_.get_my_session();
    if (OB_ISNULL(my_session) || OB_ISNULL(my_phy_plan_) || OB_ISNULL(join_ctx.left_row_) ||
        OB_ISNULL(join_ctx.right_row_)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("session or phy plan or left / right row is null", K(ret));
    } else if (OB_FAIL(wrap_expr_ctx(join_ctx.exec_ctx_, expr_ctx))) {
      LOG_WARN("fail to wrap expr ctx", K(ret));
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(my_phy_plan_, my_session, expr_ctx.cast_mode_))) {
      LOG_WARN("failed to get cast_mode", K(ret));
    } else {
      ObObj calc_ret;
      ObSqlExpression* expr = NULL;
      DLIST_FOREACH(node, other_join_conds_)
      {
        if (OB_ISNULL(node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("node or node expr is null", K(ret));
        } else if (OB_FAIL(OB_I(t1) node->calc(expr_ctx, *join_ctx.left_row_, *join_ctx.right_row_, calc_ret))) {
          LOG_WARN("failed to calc other cond expr", K(ret), K(expr));
        } else if (OB_FAIL(ObObjEvaluator::is_true(calc_ret, is_match))) {
          LOG_WARN("failed to get calc result", K(ret), K(calc_ret));
        } else {
          if (!is_match) {
            // we use DLIST_FOREACH MACRO to loop, so we use break here.
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObJoin::set_pump_row_desc(const common::ObIArray<int64_t>& pump_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pump_row_desc_.init(pump_row_desc.count()))) {
    LOG_WARN("fail to init fix arrary", K(pump_row_desc.count()), K(ret));
  }
  ARRAY_FOREACH(pump_row_desc, i)
  {
    if (OB_FAIL(pump_row_desc_.push_back(pump_row_desc.at(i)))) {
      LOG_WARN("fail to push back idx", K(i), K(ret));
    }
  }
  return ret;
}

int ObJoin::set_sort_siblings_columns(const common::ObIArray<ObSortColumn>& sort_columns)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_siblings_columns_.init(sort_columns.count()))) {
    LOG_WARN("fail to init sort columns", K(sort_columns), K(ret));
  }
  ARRAY_FOREACH(sort_columns, i)
  {
    if (OB_FAIL(sort_siblings_columns_.push_back(sort_columns.at(i)))) {
      LOG_WARN("fail to push back sort column", K(i), K(ret));
    }
  }
  return ret;
}

int ObJoin::init_pseudo_column_row_desc()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pseudo_column_row_desc_.init(CONNECT_BY_PSEUDO_COLUMN_CNT))) {
    LOG_WARN("fail to init ", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < CONNECT_BY_PSEUDO_COLUMN_CNT; ++i) {
    if (OB_FAIL(pseudo_column_row_desc_.push_back(UNUSED_POS))) {
      LOG_WARN("fail to push back pos", K(ret));
    }
  }
  return ret;
}

int ObJoin::add_pseudo_column(const ObPseudoColumnRawExpr* expr, int64_t pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || pos < 0 || pseudo_column_row_desc_.count() != CONNECT_BY_PSEUDO_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(pos), KPC(expr), K(pseudo_column_row_desc_.count()), K(ret));
  } else {
    ObItemType expr_type = expr->get_expr_type();
    switch (expr_type) {
      case T_LEVEL:
        pseudo_column_row_desc_[LEVEL] = pos;
        break;
      case T_CONNECT_BY_ISLEAF:
        pseudo_column_row_desc_[CONNECT_BY_ISLEAF] = pos;
        break;
      case T_CONNECT_BY_ISCYCLE:
        pseudo_column_row_desc_[CONNECT_BY_ISCYCLE] = pos;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", KPC(expr), K(ret));
    }
  }

  return ret;
}

int ObJoin::set_root_row_desc(const common::ObIArray<int64_t>& root_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_row_desc_.init(root_row_desc.count()))) {
    LOG_WARN("fail to init fix arrary", K(root_row_desc.count()), K(ret));
  }
  ARRAY_FOREACH(root_row_desc, i)
  {
    if (OB_FAIL(root_row_desc_.push_back(root_row_desc.at(i)))) {
      LOG_WARN("fail to push back idx", K(i), K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObJoin)
{
  int ret = OB_SUCCESS;

  ret = ObDoubleChildrenPhyOperator::serialize(buf, buf_len, pos);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_I(t1) serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(join_type_)))) {
      LOG_WARN("failed to encode join type", K(ret));
    } else if (OB_FAIL(OB_I(t3) serialization::encode_vi64(buf, buf_len, pos, equal_join_conds_.get_size()))) {
      LOG_WARN("failed to encode equal join conds count", K(ret));
    } else {
      DLIST_FOREACH(node, equal_join_conds_)
      {
        if (OB_ISNULL(node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("node or node expr is null", K(ret));
        } else if (OB_FAIL(OB_I(t5) node->serialize(buf, buf_len, pos))) {
          LOG_WARN("failed to serialize equal join conds", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_I(t7) serialization::encode_vi64(buf, buf_len, pos, other_join_conds_.get_size()))) {
      LOG_WARN("failed to encode equal join conds count", K(ret));
    } else {
      DLIST_FOREACH(node, other_join_conds_)
      {
        if (OB_ISNULL(node)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("node or node expr is null", K(ret));
        } else if (OB_FAIL(OB_I(t9) node->serialize(buf, buf_len, pos))) {
          LOG_WARN("failed to serialize equal join conds", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pump_row_desc_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize pump_row_desc", K(ret));
    } else if (OB_FAIL(root_row_desc_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize root_row_desc", K(ret));
    } else if (OB_FAIL(pseudo_column_row_desc_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize pseudo column row desc", K(ret));
    } else if (OB_FAIL(sort_siblings_columns_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize order siblings expr desc", K(ret));
    } else if (OB_FAIL(serialize_dlist(connect_by_prior_exprs_, buf, buf_len, pos))) {
      LOG_WARN("fail to serialize dist", K(ret));
    } else if (OB_FAIL(serialize_dlist(sort_siblings_exprs_, buf, buf_len, pos))) {
      LOG_WARN("fail to serialize dist", K(ret));
    } else if (OB_FAIL(serialize_dlist(connect_by_root_exprs_, buf, buf_len, pos))) {
      LOG_WARN("fail to serialize dist", K(ret));
    } else {
      OB_UNIS_ENCODE(is_nocycle_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObJoin)
{
  int ret = OB_SUCCESS;
  int64_t cond_count = 0;
  int64_t join_type = -1;

  ret = ObDoubleChildrenPhyOperator::deserialize(buf, data_len, pos);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_I(t1) serialization::decode_vi64(buf, data_len, pos, &join_type))) {
      LOG_WARN("decode join type fail", K(ret));
    } else {
      join_type_ = ObJoinType(join_type);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_I(t3) serialization::decode_vi64(buf, data_len, pos, &cond_count))) {
      LOG_WARN("decode equal join conds counts fail", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cond_count; i++) {
        ObSqlExpression* expr = NULL;
        if (OB_FAIL(OB_I(t5) ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
          LOG_WARN("failed to make sql expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to make sql expr", K(ret));
        } else if (OB_FAIL(OB_I(t7) add_equijoin_condition(expr))) {
          LOG_WARN("failed to add equal join conds to array", K(ret));
        } else if (OB_FAIL(OB_I(t9) expr->deserialize(buf, data_len, pos))) {
          LOG_WARN("falied to deserialize expr", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_I(t11) serialization::decode_vi64(buf, data_len, pos, &cond_count))) {
      LOG_WARN("decode other join conds count fail", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cond_count; i++) {
        ObSqlExpression* expr = NULL;
        if (OB_FAIL(OB_I(t13) ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
          LOG_WARN("failed to make sql expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to make sql expr", K(ret));
        } else if (OB_FAIL(OB_I(t15) add_other_join_condition(expr))) {
          LOG_WARN("failed to add other join conds to array", K(ret));
        } else if (OB_FAIL(OB_I(t17) expr->deserialize(buf, data_len, pos))) {
          LOG_WARN("falied to deserialize expr", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pump_row_desc_.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize pump_row_desc", K(ret));
    } else if (OB_FAIL(root_row_desc_.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize root_row_desc", K(ret));
    } else if (OB_FAIL(pseudo_column_row_desc_.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize pseudo_column_row_desc", K(ret));
    } else if (OB_FAIL(sort_siblings_columns_.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize sort_siblings_expr_row_desc", K(ret));
    } else {
      OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, connect_by_prior_exprs_, my_phy_plan_);
      OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, sort_siblings_exprs_, my_phy_plan_);
      OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, connect_by_root_exprs_, my_phy_plan_);
      OB_UNIS_DECODE(is_nocycle_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObJoin)
{
  int64_t len = 0;

  len += ObDoubleChildrenPhyOperator::get_serialize_size();

  OB_UNIS_ADD_LEN(join_type_);
  OB_UNIS_ADD_LEN(equal_join_conds_.get_size());

  DLIST_FOREACH_NORET(expr, equal_join_conds_)
  {
    if (OB_ISNULL(expr)) {
      len = -1;
      LOG_WARN("expr is null", "ret", OB_BAD_NULL_ERROR);
    } else {
      OB_UNIS_ADD_LEN(*expr);
    }
  }

  OB_UNIS_ADD_LEN(other_join_conds_.get_size());
  DLIST_FOREACH_NORET(expr, other_join_conds_)
  {
    if (OB_ISNULL(expr)) {
      len = -1;
      LOG_WARN("expr is null", "ret", OB_BAD_NULL_ERROR);
    } else {
      OB_UNIS_ADD_LEN(*expr);
    }
  }

  OB_UNIS_ADD_LEN(pump_row_desc_);
  OB_UNIS_ADD_LEN(root_row_desc_);
  OB_UNIS_ADD_LEN(pseudo_column_row_desc_);
  OB_UNIS_ADD_LEN(sort_siblings_columns_);
  len += get_dlist_serialize_size(connect_by_prior_exprs_);
  len += get_dlist_serialize_size(sort_siblings_exprs_);
  len += get_dlist_serialize_size(connect_by_root_exprs_);
  OB_UNIS_ADD_LEN(is_nocycle_);
  return len;
}

}  // namespace sql
}  // namespace oceanbase
