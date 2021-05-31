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
#include "sql/engine/set/ob_merge_set_operator.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
using namespace common;
namespace sql {
const int64_t ObMergeSetOperator::UNUSED_POS = -2;
const int64_t ObMergeSetOperator::ObMergeSetOperatorCtx::OB_ROW_BUF_SIZE = OB_MAX_ROW_LENGTH;
ObMergeSetOperator::ObMergeSetOperator(common::ObIAllocator& alloc)
    : ObSetOperator(alloc), set_directions_(alloc), cte_pseudo_column_row_desc_(alloc), map_array_(alloc)
{}

ObMergeSetOperator::~ObMergeSetOperator()
{}

void ObMergeSetOperator::reset()
{
  set_directions_.reset();
  cte_pseudo_column_row_desc_.reset();
  map_array_.reset();
  ObSetOperator::reset();
}

void ObMergeSetOperator::reuse()
{
  set_directions_.reuse();
  cte_pseudo_column_row_desc_.reuse();
  map_array_.reuse();
  ObSetOperator::reuse();
}

int ObMergeSetOperator::init(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSetOperator::init(count))) {
    LOG_WARN("failed to init set operator", K(count), K(ret));
  } else if (OB_FAIL(init_set_directions(count))) {
    LOG_WARN("failed to init_set_directions", K(count), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObMergeSetOperator::init_cte_pseudo_column()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cte_pseudo_column_row_desc_.init(CTE_PSEUDO_COLUMN_CNT))) {
    LOG_WARN("init cte_pseudo_column_row_desc_ failed");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < CTE_PSEUDO_COLUMN_CNT; ++i) {
      if (OB_FAIL(cte_pseudo_column_row_desc_.push_back(ObMergeSetOperator::UNUSED_POS))) {
        LOG_WARN("fail to push back pos", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeSetOperator::set_map_array(const ObIArray<int64_t>& map_array)
{
  return map_array_.assign(map_array);
}

int ObMergeSetOperator::strict_compare(const ObNewRow& row1, const ObNewRow& row2, int& cmp) const
{
  int ret = OB_SUCCESS;

  cmp = 0;
  if (row1.is_invalid() || row2.is_invalid() || row1.get_count() != row2.get_count() ||
      row1.get_count() != cs_types_.count() || row1.get_count() != set_directions_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(row1.get_count()),
        K(row2.get_count()),
        "collation types",
        cs_types_.count(),
        "set_directions",
        set_directions_.count(),
        K(ret));
  }
  if (0 != map_array_.count()) {
    if (map_array_.count() != row1.get_count() || map_array_.count() != row2.get_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(map_array_), K(row1), K(row2));
    } else {
      int64_t pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < row1.get_count(); ++i) {
        pos = map_array_.at(i);
        if ((cmp = row1.get_cell(pos).compare(row2.get_cell(pos), cs_types_.at(pos))) != 0) {
          // take consideration of order_type of columns
          cmp *= set_directions_.at(i);
          break;
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row1.get_count(); ++i) {
      if ((cmp = row1.get_cell(i).compare(row2.get_cell(i), cs_types_.at(i))) != 0) {
        // take consideration of order_type of columns
        cmp *= set_directions_.at(i);
        break;
      }
    }
  }
  return ret;
}

int ObMergeSetOperator::strict_compare(const ObNewRow& row1, const ObNewRow& row2, bool& equal) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;

  if (OB_FAIL(strict_compare(row1, row2, cmp))) {
    LOG_WARN("strict compare row failed", K(row1), K(row2));
  } else {
    equal = (cmp == 0);
  }
  return ret;
}

int ObMergeSetOperator::do_strict_distinct(
    ObPhyOperator& child_op, ObExecContext& ctx, const ObNewRow& compare_row, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool equal = false;
  bool is_break = false;
  ObMergeSetOperatorCtx* set_ctx = GET_PHY_OPERATOR_CTX(ObMergeSetOperatorCtx, ctx, get_id());
  if (nullptr == set_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get physical operator", K(ctx), K_(id));
  }
  while (OB_SUCC(ret) && !is_break && OB_SUCC(child_op.get_next_row(ctx, row))) {
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null");
    } else if (OB_UNLIKELY(set_ctx->get_need_skip_init_row())) {
      set_ctx->set_need_skip_init_row(false);
      is_break = true;
    } else if (OB_FAIL(strict_compare(compare_row, *row, equal))) {
      LOG_WARN("strict compare with last_row failed", K(ret), K(compare_row), K(*row));
    } else if (!equal) {
      is_break = true;
    }
  }
  return ret;
}

int ObMergeSetOperator::do_strict_distinct(
    ObPhyOperator& child_op, ObExecContext& ctx, const ObNewRow& compare_row, const ObNewRow*& row, int& cmp) const
{
  int ret = OB_SUCCESS;
  bool is_break = false;
  while (OB_SUCC(ret) && !is_break && OB_SUCC(child_op.get_next_row(ctx, row))) {
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null");
    } else if (OB_FAIL(strict_compare(compare_row, *row, cmp))) {
      LOG_WARN("strict compare with last_row failed", K(ret), K(compare_row), K(*row));
    } else if (0 != cmp) {
      is_break = true;
    }
  }
  return ret;
}

int ObMergeSetOperator::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeSetOperatorCtx* set_ctx = NULL;

  if (OB_ISNULL(get_child(FIRST_CHILD))) {
    ret = OB_NOT_INIT;
    LOG_WARN("left op is null");
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init operator context", K(ret));
  } else if (OB_ISNULL(set_ctx = GET_PHY_OPERATOR_CTX(ObMergeSetOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get physical operator", K(ctx), K_(id));
  } else if (is_distinct()) {
    if (OB_FAIL(set_ctx->alloc_last_row_buf(get_child(FIRST_CHILD)->get_output_count()))) {
      LOG_WARN("failed to alloc last row buffer", K(ret), "column_count", get_child(FIRST_CHILD)->get_output_count());
    }
  }
  return ret;
}

int ObMergeSetOperator::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeSetOperatorCtx* set_ctx = NULL;
  if (OB_ISNULL(set_ctx = GET_PHY_OPERATOR_CTX(ObMergeSetOperatorCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    set_ctx->free_last_row_buf();
  }
  return ret;
}

int ObMergeSetOperator::add_set_direction(ObOrderDirection direction_)
{

  int32_t set_direction = (is_ascending_direction(direction_) ? static_cast<int32_t>(CMP_DIRECTION_ASC)
                                                              : static_cast<int32_t>(CMP_DIRECTION_DESC));
  return set_directions_.push_back(set_direction);
}

int64_t ObMergeSetOperator::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_DISTINCT,
      distinct_,
      "collation_types",
      cs_types_,
      "set_directions_",
      set_directions_,
      "cte_pseido_column_",
      cte_pseudo_column_row_desc_,
      "map_array_",
      map_array_);
  return pos;
}

int ObMergeSetOperator::add_cte_pseudo_column(const ObPseudoColumnRawExpr* expr, int64_t pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || pos < 0 || cte_pseudo_column_row_desc_.count() != ObCTEPseudoColumn::CTE_PSEUDO_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(pos), KPC(expr), K(cte_pseudo_column_row_desc_.count()), K(ret));
  } else {
    ObItemType expr_type = expr->get_expr_type();
    switch (expr_type) {
      case T_CTE_SEARCH_COLUMN:
        cte_pseudo_column_row_desc_[ObCTEPseudoColumn::CTE_SEARCH] = pos;
        break;
      case T_CTE_CYCLE_COLUMN:
        cte_pseudo_column_row_desc_[ObCTEPseudoColumn::CTE_CYCLE] = pos;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", KPC(expr), K(ret));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE(ObMergeSetOperator)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(distinct_);
  OB_UNIS_ENCODE(cs_types_);
  if (OB_SUCC(ret)) {
    ret = ObPhyOperator::serialize(buf, buf_len, pos);
  }
  OB_UNIS_ENCODE(set_directions_);
  OB_UNIS_ENCODE(cte_pseudo_column_row_desc_);
  OB_UNIS_ENCODE(map_array_);
  OB_UNIS_ENCODE(child_num_);
  return ret;
}

OB_DEF_DESERIALIZE(ObMergeSetOperator)
{
  int ret = OB_SUCCESS;
  child_num_ = 2;  // for compatibility, set a default two child op value
  bool is_distinct = false;
  OB_UNIS_DECODE(is_distinct);
  OB_UNIS_DECODE(cs_types_);
  if (OB_SUCC(ret)) {
    ret = ObPhyOperator::deserialize(buf, data_len, pos);
  }
  OB_UNIS_DECODE(set_directions_);
  OB_UNIS_DECODE(cte_pseudo_column_row_desc_);
  OB_UNIS_DECODE(map_array_);
  OB_UNIS_DECODE(child_num_);
  if (OB_SUCC(ret)) {
    set_distinct(is_distinct);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMergeSetOperator)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(distinct_);
  OB_UNIS_ADD_LEN(cs_types_);
  len += ObPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(set_directions_);
  OB_UNIS_ADD_LEN(cte_pseudo_column_row_desc_);
  OB_UNIS_ADD_LEN(map_array_);
  OB_UNIS_ADD_LEN(child_num_);
  return len;
}

}  // namespace sql
}  // namespace oceanbase
