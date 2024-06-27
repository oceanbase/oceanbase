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
#include "ob_pushdown_filter.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/column_store/ob_column_store_util.h"
#include "storage/lob/ob_lob_manager.h"
#include "sql/engine/expr/ob_expr_topn_filter.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObPushdownFilterFactory::PDFilterAllocFunc ObPushdownFilterFactory::PD_FILTER_ALLOC[PushdownFilterType::MAX_FILTER_TYPE] =
{
  ObPushdownFilterFactory::alloc<ObPushdownBlackFilterNode, BLACK_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownWhiteFilterNode, WHITE_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownAndFilterNode, AND_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownOrFilterNode, OR_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownDynamicFilterNode, DYNAMIC_FILTER>,
  ObPushdownFilterFactory::alloc<ObPushdownSampleFilterNode, SAMPLE_FILTER>
};

ObPushdownFilterFactory::FilterExecutorAllocFunc ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[PushdownExecutorType::MAX_EXECUTOR_TYPE] =
{
  ObPushdownFilterFactory::alloc<ObBlackFilterExecutor, ObPushdownBlackFilterNode, BLACK_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObWhiteFilterExecutor, ObPushdownWhiteFilterNode, WHITE_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObAndFilterExecutor, ObPushdownAndFilterNode, AND_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObOrFilterExecutor, ObPushdownOrFilterNode, OR_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObDynamicFilterExecutor, ObPushdownDynamicFilterNode, DYNAMIC_FILTER_EXECUTOR>,
  ObPushdownFilterFactory::alloc<ObSampleFilterExecutor, ObPushdownSampleFilterNode, SAMPLE_FILTER_EXECUTOR>
};

ObDynamicFilterExecutor::PreparePushdownDataFunc ObDynamicFilterExecutor::PREPARE_PD_DATA_FUNCS
    [DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE] = {
  ObExprJoinFilter::prepare_storage_white_filter_data,
  ObExprTopNFilter::prepare_storage_white_filter_data,
};

ObDynamicFilterExecutor::UpdatePushdownDataFunc ObDynamicFilterExecutor::UPDATE_PD_DATA_FUNCS
    [DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE] = {
  nullptr,
  ObExprTopNFilter::update_storage_white_filter_data,
};

OB_SERIALIZE_MEMBER(ObPushdownFilterNode, type_, n_child_, col_ids_);
OB_SERIALIZE_MEMBER((ObPushdownAndFilterNode,ObPushdownFilterNode), is_runtime_filter_root_node_);
OB_SERIALIZE_MEMBER((ObPushdownOrFilterNode,ObPushdownFilterNode));
OB_SERIALIZE_MEMBER((ObPushdownBlackFilterNode,ObPushdownFilterNode),
                    column_exprs_, filter_exprs_, assist_exprs_, mono_);
OB_DEF_SERIALIZE(ObPushdownWhiteFilterNode)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObPushdownWhiteFilterNode, ObPushdownFilterNode));
  LST_DO_CODE(OB_UNIS_ENCODE,
              expr_,
              op_type_,
              column_exprs_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPushdownWhiteFilterNode)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObPushdownWhiteFilterNode, ObPushdownFilterNode));
  LST_DO_CODE(OB_UNIS_DECODE,
              expr_,
              op_type_,
              column_exprs_);
  if (OB_SUCC(ret) && nullptr != expr_ && column_exprs_.empty()) {
    if (OB_UNLIKELY(nullptr != column_exprs_.get_data())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected inited column exprs array", K(ret));
    } else if (OB_FAIL(column_exprs_.init(1))) {
      LOG_WARN("failed to init column exprs array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_->arg_cnt_; i++) {
      ObExpr *e = expr_->args_[i];
      if (nullptr != e && e->type_ == T_REF_COLUMN) {
        if (OB_FAIL(column_exprs_.push_back(e))) {
          LOG_WARN("failed to push back col expr", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushdownWhiteFilterNode)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObPushdownWhiteFilterNode, ObPushdownFilterNode));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              expr_,
              op_type_,
              column_exprs_);
  return len;
}

OB_SERIALIZE_MEMBER((ObPushdownDynamicFilterNode, ObPushdownWhiteFilterNode), col_idx_,
                    is_first_child_, is_last_child_, val_meta_,
                    dynamic_filter_type_ // FARM COMPAT WHITELIST for prepare_data_func_type_
);

int ObPushdownBlackFilterNode::merge(ObIArray<ObPushdownFilterNode*> &merged_node)
{
  int ret = OB_SUCCESS;
  int64_t merge_expr_count = 0;
  for (int64_t i = 0; i < merged_node.count(); i++) {
    merge_expr_count += static_cast<ObPushdownBlackFilterNode *>(merged_node.at(i))->get_filter_expr_count();
  }
  if (0 < filter_exprs_.count()) {
    common::ObArray<ObExpr *> tmp_expr;
    if (OB_FAIL(tmp_expr.assign(filter_exprs_))) {
      LOG_WARN("failed to assign filter exprs", K(ret));
    } else if (FALSE_IT(filter_exprs_.reuse())) {
    } else if (OB_FAIL(filter_exprs_.init(tmp_expr.count() + merge_expr_count))) {
      LOG_WARN("failed to init filter exprs", K(ret));
    } else if (OB_FAIL(filter_exprs_.assign(tmp_expr))) {
      LOG_WARN("failed to assign filter exprs", K(ret));
    }
  } else if (OB_FAIL(filter_exprs_.init(1 + merge_expr_count))) {
    LOG_WARN("failed to init exprs", K(ret));
  } else if (OB_FAIL(filter_exprs_.push_back(tmp_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < merged_node.count(); ++i) {
    ObPushdownBlackFilterNode *black_node = static_cast<ObPushdownBlackFilterNode*>(merged_node.at(i));
    if (!black_node->filter_exprs_.empty()) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < black_node->filter_exprs_.count(); idx++) {
        if (OB_FAIL(filter_exprs_.push_back(black_node->filter_exprs_.at(idx)))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    } else if (OB_ISNULL(black_node->tmp_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: exprs must be only one", K(ret));
    } else if (OB_FAIL(filter_exprs_.push_back(black_node->tmp_expr_))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObPushdownBlackFilterNode::postprocess()
{
  int ret = OB_SUCCESS;
  if (0 == filter_exprs_.count()) {
    // 没有merge
    OZ(filter_exprs_.init(1));
    OZ(filter_exprs_.push_back(tmp_expr_));
  }
  return ret;
}

const common::ObCmpOp ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[] = {
  CO_EQ,  // WHITE_OP_EQ
  CO_LE,  // WHITE_OP_LE
  CO_LT,  // WHITE_OP_LT
  CO_GE,  // WHITE_OP_GE
  CO_GT,  // WHITE_OP_GT
  CO_NE,  // WHITE_OP_NE
  CO_MAX, // WHITE_OP_BT
  CO_MAX, // WHITE_OP_IN
  CO_MAX, // WHITE_OP_NU
  CO_MAX  // WHITE_OP_NN
};

int ObPushdownWhiteFilterNode::set_op_type(const ObRawExpr &raw_expr)
{
  int ret = OB_SUCCESS;
  const ObItemType type = raw_expr.get_expr_type();
  switch (type) {
    case T_OP_EQ:
      op_type_ = WHITE_OP_EQ;
      break;
    case T_OP_LE:
      op_type_ = WHITE_OP_LE;
      break;
    case T_OP_LT:
      op_type_ = WHITE_OP_LT;
      break;
    case T_OP_GE:
      op_type_ = WHITE_OP_GE;
      break;
    case T_OP_GT:
      op_type_ = WHITE_OP_GT;
      break;
    case T_OP_NE:
      op_type_ = WHITE_OP_NE;
      break;
    case T_OP_BTW:
      op_type_ = WHITE_OP_BT;
      break;
    case T_OP_IN:
      op_type_ = WHITE_OP_IN;
      break;
    case T_OP_IS:
      op_type_ = WHITE_OP_NU;
      break;
    case T_OP_IS_NOT:
      op_type_ = WHITE_OP_NN;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObPushdownDynamicFilterNode::set_op_type(const ObRawExpr &raw_expr)
{
  int ret = OB_SUCCESS;
  if (T_OP_RUNTIME_FILTER == raw_expr.get_expr_type()) {
    const RuntimeFilterType type = raw_expr.get_runtime_filter_type();
    switch (type) {
      case RANGE:
        op_type_ = WHITE_OP_BT;
        dynamic_filter_type_ = JOIN_RUNTIME_FILTER;
        break;
      case IN:
        op_type_ = WHITE_OP_IN;
        dynamic_filter_type_ = JOIN_RUNTIME_FILTER;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  } else if (T_OP_PUSHDOWN_TOPN_FILTER == raw_expr.get_expr_type()) {
    // for topn pushdown filter, we can not sure whether is ascding or not
    // so we set the real optype in ObExprTopNFilter::prepare_storage_white_filter_data
    dynamic_filter_type_ = PD_TOPN_FILTER;
  }
  return ret;
}

int ObPushdownWhiteFilterNode::get_filter_val_meta(common::ObObjMeta &obj_meta) const
{
  int ret = OB_SUCCESS;
  bool is_datum_column_found = false;
  if (OB_UNLIKELY(nullptr == expr_ || 2 != expr_->arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), KP_(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_datum_column_found && i < expr_->arg_cnt_; i++) {
    if (OB_ISNULL(expr_->args_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null expr arguments", K(ret), K(i));
    } else if (expr_->args_[i]->type_ == T_REF_COLUMN) {
      // skip column reference expr
      continue;
    } else {
      is_datum_column_found = true;
      obj_meta = expr_->args_[i]->obj_meta_;
      if (obj_meta.is_decimal_int()) {
        obj_meta.set_stored_precision(MIN(OB_MAX_DECIMAL_PRECISION,
                                          expr_->args_[i]->datum_meta_.precision_));
      }
    }
  }
  if (OB_SUCC(ret) && !is_datum_column_found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Datum column not found", K(ret));
  }
  return ret;
}

int ObPushdownFilterConstructor::is_white_mode(const ObRawExpr* raw_expr, bool &is_white)
{
  int ret = OB_SUCCESS;
  bool need_check = true;
  const ObRawExpr *child = nullptr;
  const ObItemType item_type = raw_expr->get_expr_type();
  is_white = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null argument", K(ret));
  } else if (1 >= raw_expr->get_param_count()) {
    need_check = false;
  } else if (OB_ISNULL(child = raw_expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected first child expr: nullptr", K(ret));
  } else if (ObRawExpr::EXPR_COLUMN_REF != child->get_expr_class()) {
    need_check = false;
  } else if (T_OP_IS == item_type || T_OP_IS_NOT == item_type) {
    if (2 == raw_expr->get_param_count()
         && (child = raw_expr->get_param_expr(1))
         && child->is_const_expr()
         && child->get_result_meta().is_null()) {
      is_white = true;
    }
    need_check = false;
  } else if (static_cg_.get_cur_cluster_version() < CLUSTER_VERSION_4_3_1_0 && T_OP_IN == item_type) {
    need_check = false;
  } else {
    if (static_cg_.get_cur_cluster_version() < CLUSTER_VERSION_4_3_0_0) {
      const ObObjMeta &col_meta = child->get_result_meta();
      for (int64_t i = 1; OB_SUCC(ret) && need_check && i < raw_expr->get_param_count(); i++) {
        if (OB_ISNULL(child = raw_expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child expr", K(ret), K(i));
        } else {
          const ObObjMeta &param_meta = child->get_result_meta();
          need_check = child->is_const_expr();
          if (need_check && !param_meta.is_null()) {
            const ObCmpOp cmp_op = sql::ObRelationalExprOperator::get_cmp_op(raw_expr->get_expr_type());
            obj_cmp_func cmp_func = nullptr;
            need_check = ObObjCmpFuncs::can_cmp_without_cast(col_meta, param_meta, cmp_op, cmp_func);
          }
        }
      }
    } else {
      const ObRawExpr *param_exprs = T_OP_IN == item_type ? raw_expr->get_param_expr(1) : raw_expr;
      int64_t i = T_OP_IN == item_type ? 0 : 1;
      const ObExprResType &col_type = child->get_result_type();
      for (; OB_SUCC(ret) && need_check && i < param_exprs->get_param_count(); i++) {
        if (OB_ISNULL(child = param_exprs->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child expr", K(ret), K(i));
        } else {
          const ObExprResType &param_type = child->get_result_type();
          need_check = child->is_const_expr();
          if (need_check && !param_type.is_null()) {
            const ObCmpOp cmp_op = sql::ObRelationalExprOperator::get_cmp_op(raw_expr->get_expr_type());
            need_check = sql::ObRelationalExprOperator::can_cmp_without_cast(col_type, param_type, cmp_op);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_check) {
    switch (item_type) {
      case T_OP_EQ:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:
      case T_OP_IN:
        is_white = true;
        break;
      default:
        break;
    }
  }
  return ret;
}

int ObPushdownFilterConstructor::create_black_filter_node(
    ObRawExpr *raw_expr,
    ObPushdownFilterNode *&filter_node)
{
  int ret = OB_SUCCESS;
  ObExpr *expr = nullptr;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  ObPushdownBlackFilterNode *black_filter_node = nullptr;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null raw expr", K(ret));
  } else if (OB_FAIL(static_cg_.generate_rt_expr(*raw_expr, expr))) {
    LOG_WARN("failed to generate rt expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(raw_expr, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(factory_.alloc(PushdownFilterType::BLACK_FILTER, 0, filter_node))) {
    LOG_WARN("failed t o alloc pushdown filter", K(ret));
  } else if (OB_ISNULL(filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("black filter node is null", K(ret));
  } else if (FALSE_IT(black_filter_node = static_cast<ObPushdownBlackFilterNode*>(filter_node))) {
  } else if (OB_FAIL(get_black_filter_monotonicity(raw_expr, column_exprs, black_filter_node))) {
    LOG_WARN("failed to get black filter monotonicity", K(ret));
  } else if (0 < column_exprs.count()) {
    if (OB_FAIL(black_filter_node->col_ids_.init(column_exprs.count()))) {
      LOG_WARN("failed to init col ids", K(ret));
    } else if (OB_FAIL(black_filter_node->column_exprs_.init(column_exprs.count()))) {
      LOG_WARN("failed to init column exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      ObRawExpr *sub_raw_expr = column_exprs.at(i);
      ObExpr *sub_expr = nullptr;
      ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr*>(sub_raw_expr);
      if (OB_FAIL(static_cg_.generate_rt_expr(*sub_raw_expr, sub_expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(black_filter_node->col_ids_.push_back(ref_expr->get_column_id()))) {
        LOG_WARN("failed to push back column id", K(ret));
      } else if (OB_FAIL(black_filter_node->column_exprs_.push_back(sub_expr))) {
        LOG_WARN("failed to push back column expr", K(ret));
      }
    }
  } else {
    // 常量表达式
  }
  if (OB_SUCC(ret)) {
    black_filter_node->tmp_expr_ = expr;
    LOG_DEBUG("[PUSHDOWN] black_filter_node", K(*raw_expr), K(*expr), K(black_filter_node->col_ids_));
  }
  return ret;
}

int ObPushdownFilterConstructor::get_black_filter_monotonicity(
    const ObRawExpr *raw_expr,
    common::ObIArray<ObRawExpr *> &column_exprs,
    ObPushdownBlackFilterNode *black_filter_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(black_filter_node) || OB_ISNULL(op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected status", K(ret), K(raw_expr), K(black_filter_node), K(op_));
  } else if (1 == column_exprs.count()) {
    ObSEArray<ObRawExpr*, 2> tmp_exprs;
    PushdownFilterMonotonicity mono;
    if (OB_FAIL(op_->get_filter_monotonicity(raw_expr, static_cast<ObColumnRefRawExpr *>(column_exprs.at(0)),
                                             mono, tmp_exprs))) {
      LOG_WARN("Failed to get filter monotonicity", K(ret), KPC(raw_expr), K(column_exprs));
    } else if (OB_UNLIKELY(mono < MON_NON || mono > MON_EQ_DESC)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter monotonicity", K(mono));
    } else if (FALSE_IT(black_filter_node->mono_ = mono)) {
    } else if (tmp_exprs.count() == 0) {
    } else if (tmp_exprs.count() != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected expr count", K(ret), K(tmp_exprs));
    } else if (OB_FAIL(black_filter_node->assist_exprs_.init(2))) {
      LOG_WARN("Failed to init assist exprs", K(ret));
    } else if (OB_FAIL(static_cg_.generate_rt_exprs(tmp_exprs, black_filter_node->assist_exprs_))) {
      LOG_WARN("Failed to generate rt exprs", K(ret), K(tmp_exprs));
    }
    LOG_TRACE("[PUSHDOWN] check black filter monotonicity", K(ret), KPC(raw_expr), K(column_exprs), KPC(black_filter_node));
  }
  return ret;
}

// For ObPushdownWhiteFilterNode, always has col_idx = 0 and column_exprs.count() = 1.
// For ObPushdownDynamicFilterNode, possibly column_exprs.count() is greater than 1,
// and col_idx ranges from 0 to column_exprs.count() - 1
template <typename ClassT, PushdownFilterType type>
int ObPushdownFilterConstructor::create_white_or_dynamic_filter_node(
    ObRawExpr *raw_expr,
    ObPushdownFilterNode *&filter_node,
    int64_t col_idx/*default=0*/)
{
  int ret = OB_SUCCESS;
  ObExpr *expr = nullptr;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  ClassT *white_filter_node = nullptr;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null raw expr", K(ret));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null alloc", K(ret));
  } else if (OB_FAIL(static_cg_.generate_rt_expr(*raw_expr, expr))) {
    LOG_WARN("Failed to generate rt expr", K(ret));
  } else if (DYNAMIC_FILTER == type &&
      OB_FAIL(column_exprs.assign(static_cast<ObOpRawExpr *>(raw_expr)->get_param_exprs()))) {
    // for runtime filter, the param_exprs must be column_exprs
    LOG_WARN("Failed copy assign", K(ret));
  } else if (DYNAMIC_FILTER != type && OB_FAIL(ObRawExprUtils::extract_column_exprs(raw_expr, column_exprs))) {
    LOG_WARN("Failed to extract column exprs", K(ret));
  } else if (col_idx >= column_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected white filter expr, col_idx greater than column exprs count", K(col_idx), K(column_exprs.count()));
  } else if (OB_FAIL(factory_.alloc(type, 0, filter_node))) {
    LOG_WARN("Failed t o alloc pushdown filter", K(ret));
  } else if (OB_ISNULL(filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("White filter node is null", K(ret));
  } else if (FALSE_IT(white_filter_node = static_cast<ClassT *>(filter_node))) {
  } else if (OB_FAIL(white_filter_node->set_op_type(*raw_expr))) {
    LOG_WARN("Failed to set white filter op type", K(ret), K(type), K(raw_expr->get_expr_type()),
             K(raw_expr->get_runtime_filter_type()));
  } else if (OB_FAIL(white_filter_node->column_exprs_.init(column_exprs.count()))) {
    LOG_WARN("failed to init column exprs", K(ret), K(type));
  } else {
    if (DYNAMIC_FILTER == type) {
      ObPushdownDynamicFilterNode *dynamic_node =
          static_cast<ObPushdownDynamicFilterNode *>(filter_node);
      dynamic_node->set_col_idx(col_idx);
      dynamic_node->set_first_child(col_idx == 0);
    }
    ObExpr *sub_expr = nullptr;
    ObRawExpr *sub_raw_expr = column_exprs.at(col_idx);
    ObColumnRefRawExpr *sub_ref_expr = static_cast<ObColumnRefRawExpr *>(sub_raw_expr);
    if (OB_FAIL(white_filter_node->col_ids_.init(column_exprs.count()))) {
      LOG_WARN("Failed to init col ids", K(ret));
    } else if (OB_FAIL(white_filter_node->col_ids_.push_back(sub_ref_expr->get_column_id()))) {
      LOG_WARN("Failed to push back col id", K(ret));
    } else if (OB_FAIL(static_cg_.generate_rt_expr(*sub_raw_expr, sub_expr))) {
      LOG_WARN("failed to generate rt expr", K(ret));
    } else if (OB_FAIL(white_filter_node->column_exprs_.push_back(sub_expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    white_filter_node->expr_ = expr;
    LOG_DEBUG("[PUSHDOWN] white_filter_node", K(*raw_expr), K(*expr), K(white_filter_node->col_ids_));
  }
  return ret;
}

int ObPushdownFilterConstructor::merge_filter_node(
    ObPushdownFilterNode *dst,
    ObPushdownFilterNode *other,
    ObIArray<ObPushdownFilterNode*> &merged_node,
    bool &merged)
{
  int ret = OB_SUCCESS;
  merged = false;
  if (OB_ISNULL(other) || OB_ISNULL(dst) || dst == other) {
  } else if (dst->get_type() == other->get_type()) {
    if (dst->get_type() == PushdownFilterType::BLACK_FILTER
        && is_array_equal(dst->get_col_ids(), other->get_col_ids())
        && !static_cast<ObPushdownBlackFilterNode *>(dst)->is_monotonic()
        && !static_cast<ObPushdownBlackFilterNode *>(other)->is_monotonic())
    {
      if (OB_FAIL(merged_node.push_back(other))) {
        LOG_WARN("failed to push back", K(ret));
      }
      merged = true;
      LOG_DEBUG("[PUSHDOWN] merged filter", K(dst->get_col_ids()));
    }
  }
  return ret;
}

int ObPushdownFilterConstructor::deduplicate_filter_node(
    ObIArray<ObPushdownFilterNode*> &filter_nodes, uint32_t &n_node)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < filter_nodes.count() - 1 && OB_SUCC(ret); ++i) {
    ObArray<ObPushdownFilterNode*> merged_node;
    for (int64_t j = i + 1; j < filter_nodes.count() && OB_SUCC(ret); ++j) {
      bool merged = false;
      if (OB_FAIL(merge_filter_node(filter_nodes.at(i), filter_nodes.at(j), merged_node, merged))) {
        LOG_WARN("failed to merge filter node", K(ret));
      } else if (merged) {
        filter_nodes.at(j) = nullptr;
        --n_node;
      }
    }
    if (OB_SUCC(ret) && 0 < merged_node.count()) {
      if (OB_ISNULL(filter_nodes.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter node is null", K(ret), K(i));
      } else if (OB_FAIL(filter_nodes.at(i)->merge(merged_node))) {
        LOG_WARN("failed to merge filter node", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilterConstructor::generate_and_deduplicate(
    const ObIArray<ObRawExpr*> &exprs,
    ObArray<ObPushdownFilterNode*> &filter_nodes,
    uint32_t &valid_nodes,
    const bool need_dedup)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *raw_expr = exprs.at(i);
    ObPushdownFilterNode *sub_filter_node = nullptr;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw expr is null", K(ret));
    } else if (OB_FAIL(generate(raw_expr, sub_filter_node))) {
      LOG_WARN("failed to generate filter node from raw expr", K(ret));
    } else if (OB_FAIL(filter_nodes.push_back(sub_filter_node))) {
      LOG_WARN("failed to push back filter node", K(ret));
    }
  }
  valid_nodes = (uint32_t)filter_nodes.count();
  if (OB_SUCC(ret) &&
      need_dedup &&
      OB_FAIL(deduplicate_filter_node(filter_nodes, valid_nodes))) {
    LOG_WARN("failed to deduplicate filter node", K(ret));
  }
  return ret;
}

/*
filter([t1.c1 <= 500], [RF_RANGE_FILTER(t1.c1, t1.c2)])
Think about this scene, the tsc has 2 filters, one is normal filter and another is runtime filter,
the pushdown filter tree is the left one. In this situation,
we can remove the NO.2 and node(with is_runtime_filter_root_node_=true),
thus the the pushdown filter tree can be transformed as the right one.

              and(NO.1, root)                                          and(NO.1, root)
           /       \                                              /     |     \
        white      and(NO.2, runtime filter root node)  ---->   white  dynamic dynamic
          c1      /      \                                       c1      c1      c2
              dynamic  dynamic
                 c1       c2

Note that, only the runtime filter root node is a child node, it should be removed.
If the runtime filter root node is the root of filter tree, do not remove it.

        and(runtime filter root node    !!!              !!!
      /      \                          !!! DO NOT remove!!!
    dynamic  dynamic                    !!! DO NOT remove!!!
      c1       c2                       !!! DO NOT remove!!!
                                        !!!              !!!

*/

int ObPushdownFilterConstructor::remove_runtime_filter_root_node(
    ObArray<ObPushdownFilterNode*> &filter_nodes,
    uint32_t &valid_nodes)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < filter_nodes.count() && OB_SUCC(ret); ++i) {
    ObPushdownFilterNode *filter_node = filter_nodes.at(i);
    if (OB_NOT_NULL(filter_node)) {
      if (PushdownFilterType::AND_FILTER == filter_node->get_type()) {
        ObPushdownAndFilterNode *and_node = static_cast<ObPushdownAndFilterNode *>(filter_node);
        if (and_node->is_runtime_filter_root_node_) {
          for (int j = 0; j < and_node->n_child_ && OB_SUCC(ret); ++j) {
            if (OB_FAIL(filter_nodes.push_back(and_node->childs_[j]))) {
              LOG_WARN("failed to push back");
            }
          }
          if (OB_SUCC(ret)) {
            // remove and node, add childs_node of and node
            // filter_node is allocate from exe_ctx.allocator(an arena), not need to free
            filter_nodes.at(i) = nullptr;
            valid_nodes += and_node->n_child_ - 1;
          }
        }
      }
    }
  }
  return ret;
}

/*
When runtime filter pushdown as white filter, probability it is need to split multi cols.
1. runtime filter with single column:
                                dynamic
rf_expr(c1)          --->          c1

2. runtime filter with multiple columns:
                                    and(runtime filter root node)
rf_expr(c1, c2, c3)  --->      /     |     \
                           dynamic dynamic dynamic
                              c1      c2     c3
*/

int ObPushdownFilterConstructor::split_multi_cols_runtime_filter(
    ObOpRawExpr *raw_expr,
    ObPushdownFilterNode *&filter_tree)
{
  int ret = OB_SUCCESS;
  ObPushdownFilterNode *and_filter_node = nullptr;
  ObArray<ObPushdownFilterNode*> tmp_filter_nodes;
  for (int64_t i = 0; i < raw_expr->get_children_count() && OB_SUCC(ret); ++i) {
    ObRawExpr *child_expr = raw_expr->get_param_exprs().at(i);
    ObPushdownFilterNode *sub_filter_node = nullptr;
    if (OB_ISNULL(child_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw expr is null", K(ret));
    } else if (OB_FAIL((
        create_white_or_dynamic_filter_node<ObPushdownDynamicFilterNode, PushdownFilterType::DYNAMIC_FILTER>(
            raw_expr, sub_filter_node, i)))) {
      LOG_WARN("failed to generate filter node from raw expr", K(ret));
    } else if (OB_FAIL(tmp_filter_nodes.push_back(sub_filter_node))) {
      LOG_WARN("failed to push back filter node", K(ret));
    } else if (i == raw_expr->get_children_count() - 1) {
      static_cast<ObPushdownDynamicFilterNode *>(sub_filter_node)->set_last_child(true);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(factory_.alloc(PushdownFilterType::AND_FILTER, tmp_filter_nodes.count(), and_filter_node))) {
      LOG_WARN("failed t o alloc pushdown filter node", K(ret));
    } else if (OB_ISNULL(and_filter_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null filter node", K(ret));
    } else {
      static_cast<ObPushdownAndFilterNode *>(and_filter_node)->is_runtime_filter_root_node_ = true;
      uint32_t n_node = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_filter_nodes.count(); ++i) {
        if (OB_FAIL(tmp_filter_nodes.at(i)->postprocess())) {
          LOG_WARN("failed to postprocess tmp_filter", K(ret));
        } else {
          and_filter_node->childs_[n_node] = tmp_filter_nodes.at(i);
          ++n_node;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(and_filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is null", K(ret));
  } else {
    filter_tree = and_filter_node;
    OZ(and_filter_node->postprocess());
  }
  return ret;
}

int ObPushdownFilterConstructor::generate(ObRawExpr *raw_expr, ObPushdownFilterNode *&filter_tree)
{
  int ret = OB_SUCCESS;
  bool is_white = false;
  ObItemType op_type = T_INVALID;
  ObPushdownFilterNode *filter_node = nullptr;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null parameter", K(ret), KP(raw_expr), KP(alloc_));
  // join runtime filter only in column store can be pushdown as white filter
  // topn runtime filter can be pushdown as white filter both in row store and column store
  } else if ((use_column_store_ || T_OP_PUSHDOWN_TOPN_FILTER == raw_expr->get_expr_type())
             && raw_expr->is_white_runtime_filter_expr()) {
    // only in column store, the runtime filter can be pushdown as white filter
    ObOpRawExpr *op_raw_expr = static_cast<ObOpRawExpr *>(raw_expr);
    if (op_raw_expr->get_children_count() > 1) {
      if (OB_FAIL(split_multi_cols_runtime_filter(op_raw_expr, filter_node))) {
        LOG_WARN("Failed to split_multi_cols_runtime_filter", K(ret), K(raw_expr->get_expr_type()));
      }
    } else if (OB_FAIL((create_white_or_dynamic_filter_node<ObPushdownDynamicFilterNode, PushdownFilterType::DYNAMIC_FILTER>(
        raw_expr, filter_node)))) {
      LOG_WARN("Failed to create dynamic pushdown filter node", K(ret), K(raw_expr->get_expr_type()));
    } else {
      static_cast<ObPushdownDynamicFilterNode *>(filter_node)->set_last_child(true);
    }
  } else if (OB_FAIL(is_white_mode(raw_expr, is_white))) {
    LOG_WARN("Failed to get filter type", K(ret));
  } else if (is_white) {
    if (OB_FAIL((create_white_or_dynamic_filter_node<ObPushdownWhiteFilterNode, PushdownFilterType::WHITE_FILTER>(
        raw_expr, filter_node)))) {
      LOG_WARN("Failed to create white pushdown filter node", K(ret), K(raw_expr->get_expr_type()));
    }
  } else if (FALSE_IT(op_type = raw_expr->get_expr_type())) {
  } else if (T_OP_OR == op_type || T_OP_AND == op_type) {
    uint32_t valid_nodes;
    int64_t children = raw_expr->get_param_count();
    ObArray<ObRawExpr*> children_exprs;
    ObArray<ObPushdownFilterNode*> tmp_filter_nodes;
    for (int64_t i = 0; OB_SUCC(ret) && i < children; i++) {
      if (OB_FAIL(children_exprs.push_back(raw_expr->get_param_expr(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_and_deduplicate(children_exprs, tmp_filter_nodes, valid_nodes, T_OP_AND == op_type))) {
      LOG_WARN("Failed to generate filter node from exprs", K(ret));
    } else if (OB_UNLIKELY(0 >= valid_nodes)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected generated valid nodes", K(ret), K(valid_nodes));
    } else if (1 == valid_nodes) {
      filter_node = tmp_filter_nodes.at(0);
    } else {
      if (T_OP_OR == op_type) {
        if (OB_FAIL(factory_.alloc(PushdownFilterType::OR_FILTER, valid_nodes, filter_node))) {
          LOG_WARN("Failed t o alloc or pushdown filter node", K(ret), K(valid_nodes));
        }
      } else if (OB_FAIL(factory_.alloc(PushdownFilterType::AND_FILTER, valid_nodes, filter_node))) {
        LOG_WARN("Failed t o alloc or pushdown filter node", K(ret), K(valid_nodes));
      }
      uint32_t n_node = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_filter_nodes.count(); ++i) {
        if (nullptr != tmp_filter_nodes.at(i)) {
          if (n_node < valid_nodes) {
            if (OB_FAIL(tmp_filter_nodes.at(i)->postprocess())) {
              LOG_WARN("failed to postprocess tmp_filter", K(ret));
            } else {
              filter_node->childs_[n_node] = tmp_filter_nodes.at(i);
              ++n_node;
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpeced status: valid node count", K(ret), K(n_node), K(valid_nodes));
          }
        }
      }
    }
  } else if (OB_FAIL(create_black_filter_node(raw_expr, filter_node))) {
    LOG_WARN("Failed to create black pushdown filter node", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is null", K(ret));
  } else {
    filter_tree = filter_node;
  }
  return ret;
}

int ObPushdownFilterConstructor::apply(
    ObIArray<ObRawExpr*> &exprs, ObPushdownFilterNode *&filter_tree)
{
  int ret = OB_SUCCESS;
  uint32_t valid_nodes;
  ObPushdownFilterNode *filter_node = nullptr;
  ObArray<ObPushdownFilterNode*> tmp_filter_nodes;
  if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected status: alloc is null", K(ret), KP(alloc_));
  } else if (OB_FAIL(generate_and_deduplicate(exprs, tmp_filter_nodes, valid_nodes, true))) {
    LOG_WARN("Failed to generate filter node from exprs", K(ret));
  } else if (OB_UNLIKELY(0 >= valid_nodes)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected generated valid nodes", K(ret), K(valid_nodes));
  } else if (1 == valid_nodes) {
    filter_node = tmp_filter_nodes.at(0);
  } else if (OB_FAIL(remove_runtime_filter_root_node(tmp_filter_nodes, valid_nodes))) {
    LOG_WARN("failed to remove_runtime_filter_root_node");
  } else if (OB_FAIL(factory_.alloc(PushdownFilterType::AND_FILTER, valid_nodes, filter_node))) {
    LOG_WARN("failed t o alloc pushdown filter node", K(ret));
  } else if (OB_ISNULL(filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter node", K(ret));
  } else {
    uint32_t n_node = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_filter_nodes.count(); ++i) {
      if (nullptr != tmp_filter_nodes.at(i)) {
        if (n_node < valid_nodes) {
          if (OB_FAIL(tmp_filter_nodes.at(i)->postprocess())) {
            LOG_WARN("failed to postprocess tmp_filter", K(ret));
          } else {
            filter_node->childs_[n_node] = tmp_filter_nodes.at(i);
            ++n_node;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeced status: valid node count", K(ret), K(n_node), K(valid_nodes));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is null", K(ret));
  } else {
    filter_tree = filter_node;
    OZ(filter_node->postprocess());
  }
  return ret;
}

int ObPushdownFilterFactory::alloc(PushdownFilterType type, uint32_t n_child, ObPushdownFilterNode *&pd_filter)
{
  int ret = OB_SUCCESS;
  if (!(BLACK_FILTER <= type && type < MAX_FILTER_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter type", K(ret), K(type));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null allocator", K(ret));
  } else if (OB_ISNULL(ObPushdownFilterFactory::PD_FILTER_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPushdownFilterFactory::PD_FILTER_ALLOC[type](*alloc_, n_child, pd_filter))) {
    LOG_WARN("fail to alloc pushdown filter", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, PushdownFilterType type>
int ObPushdownFilterFactory::alloc(common::ObIAllocator &alloc, uint32_t n_child, ObPushdownFilterNode *&pd_filter)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc pushdown filter", K(ret));
  } else {
    pd_filter = new(buf) ClassT(alloc);
    if (0 < n_child) {
      void *tmp_buf = alloc.alloc(n_child * sizeof(ObPushdownFilterNode*));
      if (OB_ISNULL(tmp_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc pushdown filter", K(ret));
      } else {
        pd_filter->childs_ = reinterpret_cast<ObPushdownFilterNode**>(tmp_buf);
      }
      pd_filter->n_child_ = n_child;
    } else {
      pd_filter->childs_ = nullptr;
    }
    pd_filter->set_type(type);
  }
  return ret;
}

int ObPushdownFilterFactory::alloc(
    PushdownExecutorType type,
    uint32_t n_child,
    ObPushdownFilterNode &filter_node,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = OB_SUCCESS;
  if (!(BLACK_FILTER_EXECUTOR <= type && type < MAX_EXECUTOR_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filter type", K(ret), K(type));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null allocator", K(ret));
  } else if (OB_ISNULL(ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[type](*alloc_, n_child, filter_node, filter_executor, op))) {
    LOG_WARN("fail to alloc pushdown filter", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, typename FilterNodeT, PushdownExecutorType type>
int ObPushdownFilterFactory::alloc(
    common::ObIAllocator &alloc,
    uint32_t n_child,
    ObPushdownFilterNode &filter_node,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc pushdown filter", K(ret), K(sizeof(ClassT)));
  } else {
    filter_executor = new(buf) ClassT(alloc, *static_cast<FilterNodeT*>(&filter_node), op);
    if (0 < n_child) {
      void *tmp_buf = alloc.alloc(n_child * sizeof(ObPushdownFilterExecutor*));
      if (OB_ISNULL(tmp_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc pushdown filter", K(ret));
      }
      filter_executor->set_childs(n_child, reinterpret_cast<ObPushdownFilterExecutor**>(tmp_buf));
    }
    filter_executor->set_type(type);
  }
  return ret;
}

int ObPushdownFilter::serialize_pushdown_filter(
    char *buf,
    int64_t buf_len,
    int64_t &pos,
    ObPushdownFilterNode *pd_storage_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pd_storage_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pd filter is null", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, pd_storage_filter->type_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, pd_storage_filter->n_child_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(pd_storage_filter->serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    for (int64_t i = 0; i < pd_storage_filter->n_child_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(serialize_pushdown_filter(
                  buf, buf_len, pos, pd_storage_filter->childs_[i]))) {
        LOG_WARN("failed to serialize pushdown storage filter", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilter::deserialize_pushdown_filter(
    ObPushdownFilterFactory filter_factory,
    const char *buf,
    int64_t data_len,
    int64_t &pos,
    ObPushdownFilterNode *&pd_storage_filter)
{
  int ret = OB_SUCCESS;
  int32_t filter_type;
  uint32_t child_cnt = 0;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &filter_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else {
    if (OB_FAIL(filter_factory.alloc(
                static_cast<PushdownFilterType>(filter_type), child_cnt, pd_storage_filter))) {
      LOG_WARN("failed to allocate filter", K(ret));
    } else if (OB_FAIL(pd_storage_filter->deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize", K(ret));
    } else if (0 < child_cnt) {
      for (uint32_t i = 0; i < child_cnt && OB_SUCC(ret); ++i) {
        ObPushdownFilterNode *sub_pd_storage_filter = nullptr;
        if (OB_FAIL(deserialize_pushdown_filter(
                    filter_factory, buf, data_len, pos, sub_pd_storage_filter))) {
          LOG_WARN("failed to deserialize child", K(ret));
        } else {
          pd_storage_filter->childs_[i] = sub_pd_storage_filter;
        }
      }
      if (pd_storage_filter->n_child_ != child_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child count is not match", K(ret), K(pd_storage_filter->n_child_), K(child_cnt));
      }
    }
  }
  return ret;
}

int64_t ObPushdownFilter::get_serialize_pushdown_filter_size(
    ObPushdownFilterNode *pd_filter_node)
{
  int64_t len = 0;
  if (OB_NOT_NULL(pd_filter_node)) {
    len += serialization::encoded_length_vi32(pd_filter_node->type_);
    len += serialization::encoded_length(pd_filter_node->n_child_);
    len += pd_filter_node->get_serialize_size();
    for (int64_t i = 0; i < pd_filter_node->n_child_; ++i) {
      len += get_serialize_pushdown_filter_size(pd_filter_node->childs_[i]);
    }
  } else {
    int ret = OB_SUCCESS;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pushdown filter is null", K(ret));
  }
  return len;
}

OB_DEF_SERIALIZE(ObPushdownFilter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_tree_)) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, false))) {
      LOG_WARN("fail to encode op type", K(ret));
    }
  } else {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, true))) {
      LOG_WARN("fail to encode op type", K(ret));
    } else if (OB_FAIL(serialize_pushdown_filter(buf, buf_len, pos, filter_tree_))) {
      LOG_WARN("failed to serialize pushdown filter", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPushdownFilter)
{
  int ret = OB_SUCCESS;
  bool has_filter = false;
  filter_tree_ = nullptr;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, has_filter))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (has_filter) {
    ObPushdownFilterFactory filter_factory(&alloc_);
    if (OB_FAIL(deserialize_pushdown_filter(filter_factory, buf, data_len, pos, filter_tree_))) {
      LOG_WARN("failed to deserialize pushdown filter", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushdownFilter)
{
  int64_t len = 0;
  if (OB_ISNULL(filter_tree_)) {
    len += serialization::encoded_length(false);
  } else {
    len += serialization::encoded_length(true);
    len += get_serialize_pushdown_filter_size(filter_tree_);
  }
  return len;
}

//--------------------- start filter executor ----------------------------
int ObPushdownFilterExecutor::find_evaluated_datums(
    ObExpr *expr, const ObIArray<ObExpr*> &calc_exprs, ObIArray<ObExpr*> &eval_exprs)
{
  int ret = OB_SUCCESS;
  if (is_contain(calc_exprs, expr)) {
    if (OB_FAIL(eval_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
    for (uint32_t i = 0; i < expr->arg_cnt_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(find_evaluated_datums(expr->args_[i], calc_exprs, eval_exprs))) {
        LOG_WARN("failed to find evaluated datums", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilterExecutor::find_evaluated_datums(
    ObIArray<ObExpr*> &src_exprs,
    const ObIArray<ObExpr*> &calc_exprs,
    ObIArray<ObExpr*> &eval_exprs)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < src_exprs.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(find_evaluated_datums(src_exprs.at(i), calc_exprs, eval_exprs))) {
      LOG_WARN("failed to find evaluated datums", K(ret));
    }
  }
  return ret;
}

int ObPushdownFilterExecutor::init_bitmap(const int64_t row_count, common::ObBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(filter_bitmap_)) {
    if (OB_FAIL(filter_bitmap_->reserve(row_count))) {
      LOG_WARN("Failed to expand size of filter bitmap", K(ret));
    } else if (FALSE_IT(filter_bitmap_->reuse(is_logic_and_node()))) {
    }
  } else {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for filter bitmap", K(ret));
    } else if (FALSE_IT(filter_bitmap_ = new (buf) ObBitmap(allocator_))) {
    } else if (OB_FAIL(filter_bitmap_->init(row_count, is_logic_and_node()))) {
      LOG_WARN("Failed to init result bitmap", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bitmap = filter_bitmap_;
  }
  return ret;
}

int ObPushdownFilterExecutor::init_filter_param(
    const common::ObIArray<share::schema::ObColumnParam *> &col_params,
    const common::ObIArray<int32_t> &output_projector,
    const bool need_padding)
{
  int ret = OB_SUCCESS;
  const ObIArray<uint64_t> &col_ids = get_col_ids();
  const int64_t col_count = col_ids.count();
  if (is_filter_node()) {
    if (0 == col_count) {
    } else if (OB_FAIL(init_array_param(col_params_, col_count))) {
      LOG_WARN("Fail to init col params", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(col_offsets_, col_count))) {
      LOG_WARN("Fail to init col offsets", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(default_datums_, col_count))) {
      LOG_WARN("Fail to init default datums", K(ret), K(col_count));
    } else {
      const share::schema::ObColumnParam *col_param = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
        int32_t idx = OB_INVALID_INDEX;
        for (int32_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == idx && j < output_projector.count(); ++j) {
          if (OB_UNLIKELY(output_projector.at(j) >= col_params.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected col projector", K(ret), K(output_projector.at(j)), K(col_params.count()));
          } else if (col_ids.at(i) == col_params.at(output_projector.at(j))->get_column_id()) {
            idx = output_projector.at(j);
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected idx found", K(ret));
        } else if (OB_FAIL(col_offsets_.push_back(idx))) {
          LOG_WARN("failed to push back col offset", K(ret));
        } else {
          col_param = nullptr;
          blocksstable::ObStorageDatum default_datum;
          const common::ObObj &def_cell = col_params.at(idx)->get_orig_default_value();
          const common::ObObjMeta &obj_meta = col_params.at(idx)->get_meta_type();
          if (need_padding && obj_meta.is_fixed_len_char_type()) {
            col_param = col_params.at(idx);
          } else if (obj_meta.is_lob_storage() || obj_meta.is_decimal_int()) {
            col_param = col_params.at(idx);
          }
          if (OB_FAIL(col_params_.push_back(col_param))) {
            LOG_WARN("failed to push back col param", K(ret));
          } else if (!def_cell.is_nop_value()) {
            if (OB_FAIL(default_datum.from_obj(def_cell))) {
              LOG_WARN("convert obj to datum failed", K(ret), K(col_params_.count()), K(def_cell));
            } else if (obj_meta.is_lob_storage() && !def_cell.is_null()) {
              // lob def value must have no lob header when not null
              // When do lob pushdown, should add lob header for default value
              ObString data = default_datum.get_string();
              ObString out;
              if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, data, out))) {
                LOG_WARN("failed to fill lob header for column", K(ret), K(idx), K(def_cell), K(data));
              } else {
                default_datum.set_string(out);
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(default_datums_.push_back(default_datum))) {
            LOG_WARN("Fail to push back default datum", K(ret));
          }
        }
      }
    }
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < n_child_; i++) {
      if (OB_NOT_NULL(childs_[i]) &&
          OB_FAIL(childs_[i]->init_filter_param(col_params, output_projector, need_padding))) {
        LOG_WARN("Failed to init pushdown filter param", K(ret), K(i), KP(childs_[i]));
      }
    }
  }

  if (OB_SUCC(ret)) {
    n_cols_ = col_count;
  }
  return ret;
}

int ObPushdownFilterExecutor::init_co_filter_param(const ObTableIterParam &iter_param, const bool need_padding)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *read_info =  nullptr;
  const common::ObIArray<int32_t> *access_cgs = nullptr;
  const common::ObIArray<ObExpr *> *cg_exprs = nullptr;
  const ObIArray<uint64_t> &col_ids = get_col_ids();
  const int64_t col_count = col_ids.count();
  if (OB_UNLIKELY(!iter_param.is_valid() || nullptr == (read_info = iter_param.get_read_info())
                  || nullptr == read_info->get_cg_idxs())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(iter_param), KPC(iter_param.get_read_info()));
  } else if (is_filter_node()) {
    if (0 == col_count) {
      if (OB_FAIL(init_array_param(cg_idxs_, 1))) {
        LOG_WARN("Fail to init cg idxs", K(ret), K(col_count));
      } else if (OB_FAIL(cg_idxs_.push_back(OB_CS_VIRTUAL_CG_IDX))) {
        LOG_WARN("Failed to push back cg idx", K(ret));
      }
    } else if (OB_FAIL(init_array_param(col_params_, col_count))) {
      LOG_WARN("Fail to init col params", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(col_offsets_, col_count))) {
      LOG_WARN("Fail to init col offsets", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(cg_col_offsets_, col_count))) {
      LOG_WARN("Fail to init cg col offsets", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(cg_idxs_, col_count))) {
      LOG_WARN("Fail to init col offsets", K(ret), K(col_count));
    } else if (OB_FAIL(init_array_param(default_datums_, col_count))) {
      LOG_WARN("Fail to init default datums", K(ret), K(col_count));
    } else if (FALSE_IT(cg_exprs = get_cg_col_exprs())) {
    } else if (nullptr != cg_exprs && OB_FAIL(cg_col_exprs_.assign(*cg_exprs))) {
      LOG_WARN("Fail to assign cg exprs", K(ret), KPC(cg_exprs));
    } else {
      access_cgs = read_info->get_cg_idxs();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
        int32_t col_pos = -1;
        int32_t cg_idx = -1;
        const share::schema::ObColumnParam *col_param = nullptr;
        const common::ObIArray<share::schema::ObColumnParam *> &col_params = *read_info->get_columns();
        const common::ObIArray<ObColDesc> &cols_desc = read_info->get_columns_desc();
        for (col_pos = 0; OB_SUCC(ret) && col_pos < cols_desc.count(); col_pos++) {
          if (col_ids.at(i) == cols_desc.at(col_pos).col_id_) {
            cg_idx = access_cgs->at(col_pos);
            col_param = col_params.at(col_pos);
            break;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(0 > col_pos || 0 > cg_idx || nullptr == col_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected result", K(ret), K(col_pos), K(cg_idx), K(col_ids.at(i)));
        } else if (OB_FAIL(col_offsets_.push_back(col_pos))) {
          LOG_WARN("failed to push back col offset", K(ret));
        } else if (OB_FAIL(cg_col_offsets_.push_back(0))) { // TODO: fix this later, use col index when multi cols contained in cg table
          LOG_WARN("failed to push back col offset", K(ret));
        } else if (OB_FAIL(cg_idxs_.push_back(cg_idx))) {
          LOG_WARN("Failed to push back cg idx", K(ret), K(cg_idx));
        } else {
          const share::schema::ObColumnParam *cg_col_param = nullptr;
          blocksstable::ObStorageDatum default_datum;
          const common::ObObj &def_cell = col_param->get_orig_default_value();
          const common::ObObjMeta &obj_meta = col_param->get_meta_type();
          if (need_padding && obj_meta.is_fixed_len_char_type()) {
            cg_col_param = col_param;
          } else if (obj_meta.is_lob_storage() || obj_meta.is_decimal_int()) {
            cg_col_param = col_param;
          }

          if (OB_FAIL(col_params_.push_back(cg_col_param))) {
            LOG_WARN("failed to push back col param", K(ret));
          } else if (!def_cell.is_nop_value()) {
            if (OB_FAIL(default_datum.from_obj(def_cell))) {
              LOG_WARN("convert obj to datum failed", K(ret), K(col_params_.count()), K(def_cell));
            } else if (obj_meta.is_lob_storage() && !def_cell.is_null()) {
              // lob def value must have no lob header when not null
              // When do lob pushdown, should add lob header for default value
              ObString data = default_datum.get_string();
              ObString out;
              if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, data, out))) {
                LOG_WARN("failed to fill lob header for column", K(ret), K(col_pos), K(def_cell), K(data));
              } else {
                default_datum.set_string(out);
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(default_datums_.push_back(default_datum))) {
            LOG_WARN("Fail to push back default datum", K(ret));
          }
          LOG_DEBUG("[COLUMNSTORE] extract one cg idx", K(ret), KPC(read_info), K(col_pos), K(cg_idx), KPC(cg_col_param));
        }
      }
      LOG_DEBUG("[COLUMNSTORE] cons cg idxs", K(ret), K_(cg_idxs), K(col_ids), K_(col_params), K_(col_offsets), K_(cg_col_offsets));
    }
  } else {
    // TODO: @yuxiaozhe.yxz rewrite cg_col_exprs_ in rescan
    for (uint32_t i = 0; OB_SUCC(ret) && i < n_child_; i++) {
      if (OB_NOT_NULL(childs_[i]) &&
          OB_FAIL(childs_[i]->init_co_filter_param(iter_param, need_padding))) {
        LOG_WARN("Failed to init pushdown filter param", K(ret), K(i), KP(childs_[i]));
      }
    }
  }

  if (OB_SUCC(ret)) {
    n_cols_ = col_count;
  }
  return ret;
}

int ObPushdownFilterExecutor::set_cg_param(const common::ObIArray<uint32_t> &cg_idxs, const common::ObIArray<ObExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((cg_idxs_.assign(cg_idxs)))) {
    LOG_WARN("Failed to assign cg_idxs", K(ret), K(cg_idxs), K(cg_idxs_));
  } else if (!exprs.empty() && OB_FAIL(cg_col_exprs_.assign(exprs))) {
    LOG_WARN("Failed to assign cg_exprs", K(ret), K(exprs));
  }
  return ret;
}

int ObPushdownFilterExecutor::execute(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &filter_info,
    blocksstable::ObIMicroBlockRowScanner *micro_scanner,
    const bool use_vectorize)
{
  int ret = OB_SUCCESS;
  common::ObBitmap *result = nullptr;
  if (OB_UNLIKELY(filter_info.start_ < 0 || filter_info.count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(filter_info.start_), K(filter_info.count_));
  } else if (OB_FAIL(init_bitmap(filter_info.count_, result))) {
    LOG_WARN("Failed to get filter bitmap", K(ret));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  } else if (nullptr != parent && OB_FAIL(parent->prepare_skip_filter())) {
    LOG_WARN("Failed to check parent blockscan", K(ret));
  } else if (is_filter_node()) {
    if (OB_FAIL(do_filter(parent, filter_info, micro_scanner, use_vectorize, *result))) {
      LOG_WARN("Fail to do filter", K(ret));
    }
  } else if (is_logic_op_node()) {
    if (OB_UNLIKELY(get_child_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected child count of filter executor", K(ret), K(get_child_count()), KP(this));
    } else {
      sql::ObPushdownFilterExecutor **children = get_childs();
      if (parent != nullptr && parent->is_logic_and_node() && this->is_logic_and_node()) {
        MEMCPY(result->get_data(), parent->get_result()->get_data(), filter_info.count_);
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < get_child_count(); i++) {
        const common::ObBitmap *child_result = nullptr;
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(children[i]->execute(this, filter_info, micro_scanner, use_vectorize))) {
          LOG_WARN("Failed to filter micro block", K(ret), K(i), KP(children[i]));
        } else if (OB_ISNULL(child_result = children[i]->get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected get null filter bitmap", K(ret));
        } else {
          if (is_logic_and_node()) {
            if (OB_FAIL(result->bit_and(*child_result))) {
              LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
            } else if (result->is_all_false()) {
              break;
            }
          } else  {
            if (OB_FAIL(result->bit_or(*child_result))) {
              LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
            } else if (result->is_all_true()) {
              break;
            }
          }
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported filter executor type", K(ret), K(get_type()));
  }
  return ret;
}

int ObPushdownFilterExecutor::execute_skipping_filter(ObBoolMask &bm)
{
  int ret = OB_SUCCESS;
  if (is_filter_node()) {
    bm = filter_bool_mask_;
  } else if (is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && i < get_child_count(); i++) {
      ObBoolMask child_bm;
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null child filter", K(ret));
      } else if (OB_FAIL(children[i]->execute_skipping_filter(child_bm))) {
        LOG_WARN("Fail to execute skipping filter", K(ret), K(i), KP(children[i]));
      } else if (0 == i) {
        bm = child_bm;
      } else if (is_logic_and_node()) {
        bm = bm & child_bm;
        if (bm.is_always_false()) {
          break;
        }
      } else {
        bm = bm | child_bm;
        if (bm.is_always_true()) {
          break;
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not supported filter executor type", K(ret), K(get_type()));
  }
  return ret;
}

void ObPushdownFilterExecutor::clear()
{
  if (is_filter_white_node()) {
    static_cast<ObWhiteFilterExecutor*>(this)->clear_in_datums();
  } else if (is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = get_childs();
    for (uint32_t i = 0; i < get_child_count(); ++i) {
      children[i]->clear();
    }
  }
}

bool ObPushdownFilterExecutor::check_sstable_index_filter()
{
  bool is_needed_to_do_filter = true;
  if (is_filter_constant()) {
    is_needed_to_do_filter = false;
    filter_bitmap_->reuse(is_filter_always_true());
  }
  return is_needed_to_do_filter;
}

int ObPushdownFilterExecutor::do_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &filter_info,
    blocksstable::ObIMicroBlockRowScanner *micro_scanner,
    const bool use_vectorize,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  bool is_needed_to_do_filter = check_sstable_index_filter();
  if (!is_needed_to_do_filter) {
  } else if (is_filter_dynamic_node()
             && OB_FAIL(static_cast<ObDynamicFilterExecutor *>(this)->check_runtime_filter(
                    parent, is_needed_to_do_filter))) {
    LOG_WARN("Failed to check runtime filter", K(ret), KPC(this));
  } else if (is_filter_dynamic_node() && !is_needed_to_do_filter) {
    ObDynamicFilterExecutor *dynamic_filter = static_cast<ObDynamicFilterExecutor *>(this);
    dynamic_filter->filter_on_bypass(parent);
  } else if (OB_LIKELY(nullptr != micro_scanner)) {
    if (OB_FAIL(micro_scanner->filter_pushdown_filter(parent, this, filter_info,
        use_vectorize, result_bitmap))) {
      LOG_WARN("Fail to pushdown filter to micro scanner", K(ret));
    } else if (is_filter_dynamic_node()) {
      ObDynamicFilterExecutor *dynamic_filter = static_cast<ObDynamicFilterExecutor *>(this);
      dynamic_filter->filter_on_success(parent);
    }
    if (parent) {
      parent->clear_skipped_rows();
    }
  } else if (OB_UNLIKELY(!is_filter_black_node() || !use_vectorize)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter node", K(ret), KPC(this));
  } else if ((OB_FAIL((static_cast<ObBlackFilterExecutor*>(this))->filter_batch(parent,
      0, filter_info.count_, result_bitmap)))) {
    LOG_WARN("failed to filter batch", K(ret));
  }
  return ret;
}

int ObPushdownFilterExecutor::pull_up_common_node(
    const common::ObIArray<uint32_t> &filter_indexes,
    ObPushdownFilterExecutor *&common_filter_executor)
{
  int ret = OB_SUCCESS;
  ObPushdownFilterExecutor *new_filter_executor = nullptr;
  common_filter_executor = nullptr;
  if (OB_UNLIKELY(filter_indexes.count() >= n_child_
                   || filter_indexes.count() <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments when pull up common filter node",
              K(ret), K(filter_indexes), K_(n_child));
  } else if (OB_FAIL(build_new_sub_filter_tree(filter_indexes, new_filter_executor))) {
    LOG_WARN("Failed to build new sub filter tree", K(filter_indexes));
  } else {
    const uint32_t base_idx = filter_indexes.at(0);
    uint32_t pos = base_idx + 1;
    for (uint32_t i = pos; i < n_child_; ++i) {
      if (common::is_contain(filter_indexes, i)) {
        set_child(i, nullptr);
      } else {
        set_child(pos++, childs_[i]);
      }
    }
    n_child_ = n_child_ - filter_indexes.count() + 1;
    set_child(base_idx, new_filter_executor);
    common_filter_executor = new_filter_executor;
  }
  return ret;
}

int ObPushdownFilterExecutor::build_new_sub_filter_tree(
    const common::ObIArray<uint32_t> &filter_indexes,
    ObPushdownFilterExecutor *&new_filter_executor)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPushdownFilterNode*, 4> tmp_filter_nodes;
  new_filter_executor = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_indexes.count(); ++i) {
    ObPushdownFilterExecutor *filter_executor = childs_[filter_indexes.at(i)];
    ObPushdownFilterNode* filter_node = nullptr;
    switch (filter_executor->get_type()) {
      case BLACK_FILTER_EXECUTOR: {
        filter_node = &(static_cast<ObBlackFilterExecutor *>(filter_executor)->get_filter_node());
        break;
      }
      case WHITE_FILTER_EXECUTOR:
      case DYNAMIC_FILTER_EXECUTOR: {
        filter_node = &(static_cast<ObWhiteFilterExecutor *>(filter_executor)->get_filter_node());
        break;
      }
      case AND_FILTER_EXECUTOR: {
        filter_node = &(static_cast<ObAndFilterExecutor *>(filter_executor)->get_filter_node());
        break;
      }
      case OR_FILTER_EXECUTOR: {
        filter_node = &(static_cast<ObOrFilterExecutor *>(filter_executor)->get_filter_node());
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected filter executor type", K(ret), K(filter_executor->get_type()));
        break;
    }
    if (OB_SUCC(ret) && (OB_FAIL(tmp_filter_nodes.push_back(filter_node)))) {
      LOG_WARN("Failed to push back filter node", K(ret), KPC(filter_node), K(tmp_filter_nodes));
    }
  }
  if (OB_SUCC(ret)) {
    ObPushdownFilterFactory factory_(&allocator_);
    ObPushdownFilterNode *filter_node = nullptr;
    ObPushdownFilterExecutor *filter_executor = nullptr;
    uint32_t child_cnt = tmp_filter_nodes.count();
    if (AND_FILTER_EXECUTOR == get_type()) {
      if (OB_FAIL(factory_.alloc(AND_FILTER, child_cnt, filter_node))) {
        LOG_WARN("Failed to alloc filter node", K(ret), K(get_type()));
      } else if (OB_FAIL(factory_.alloc(AND_FILTER_EXECUTOR, child_cnt, *filter_node, filter_executor, op_))) {
        LOG_WARN("Failed to alloc filter executor", K(ret), K(get_type()));
      }
    } else {
      if (OB_FAIL(factory_.alloc(OR_FILTER, child_cnt, filter_node))) {
        LOG_WARN("Failed to alloc filter node", K(ret), K(get_type()));
      } else if (OB_FAIL(factory_.alloc(OR_FILTER_EXECUTOR, child_cnt, *filter_node, filter_executor, op_))) {
        LOG_WARN("Failed to alloc filter executor", K(ret), K(get_type()));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < child_cnt; ++i) {
        filter_node->childs_[i] = tmp_filter_nodes[i];
        filter_executor->set_child(i, childs_[filter_indexes.at(i)]);
      }
      new_filter_executor = filter_executor;
    }
  }
  return ret;
}

template<typename T>
int ObPushdownFilterExecutor::init_array_param(common::ObFixedArray<T, common::ObIAllocator> &param, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(param.clear())) {
  } else if (OB_FAIL(param.reserve(size))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WARN("Failed to init params", K(ret));
    } else {
      param.reset();
      if (OB_FAIL(param.init(size))) {
        LOG_WARN("Failed to init params", K(ret), K(size));
      }
    }
  }
  return ret;
}

ObPushdownFilterExecutor::ObPushdownFilterExecutor(common::ObIAllocator &alloc,
                                                   ObPushdownOperator &op,
                                                   PushdownExecutorType type)
  : type_(type), need_check_row_filter_(false), filter_tree_status_(ObCommonFilterTreeStatus::NONE_FILTER),
    n_cols_(0), n_child_(0), cg_iter_idx_(INVALID_CG_ITER_IDX), skipped_rows_(0), childs_(nullptr),
    filter_bitmap_(nullptr), col_params_(alloc), col_offsets_(alloc), cg_col_offsets_(alloc), default_datums_(alloc),
    cg_idxs_(alloc), cg_col_exprs_(alloc), allocator_(alloc), op_(op), is_rewrited_(false), filter_bool_mask_()
{}

ObPushdownFilterExecutor::~ObPushdownFilterExecutor()
{
  if (nullptr != filter_bitmap_) {
    filter_bitmap_->~ObBitmap();
    allocator_.free(filter_bitmap_);
  }
  filter_bitmap_ = nullptr;
  col_params_.reset();
  col_offsets_.reset();
  cg_col_offsets_.reset();
  default_datums_.reset();
  cg_idxs_.reset();
  cg_col_exprs_.reset();
  for (uint32_t i = 0; i < n_child_; i++) {
    if (OB_NOT_NULL(childs_[i])) {
      childs_[i]->~ObPushdownFilterExecutor();
      childs_[i] = nullptr;
    }
  }
  n_child_ = 0;
  cg_iter_idx_ = INVALID_CG_ITER_IDX;
  need_check_row_filter_ = false;
  is_rewrited_ = false;
}

DEF_TO_STRING(ObPushdownFilterExecutor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), K_(need_check_row_filter), K_(n_cols),
       K_(n_child), KP_(childs), KP_(filter_bitmap),
       K_(col_params), K_(default_datums), K_(col_offsets),
       K_(cg_col_offsets), K_(cg_idxs), K_(cg_col_exprs),
       K_(is_rewrited), K_(filter_bool_mask));
  J_OBJ_END();
  return pos;
}

int ObPushdownFilterExecutor::prepare_skip_filter()
{
  int ret = OB_SUCCESS;
  need_check_row_filter_ = false;
  if (OB_ISNULL(filter_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  } else if (PushdownExecutorType::AND_FILTER_EXECUTOR == type_) {
    need_check_row_filter_ = !filter_bitmap_->is_all_true();
  } else if (PushdownExecutorType::OR_FILTER_EXECUTOR == type_) {
    need_check_row_filter_ = !filter_bitmap_->is_all_false();
  }

  return ret;
}

// 初始化需要被清理的标记
int ObAndFilterExecutor::init_evaluated_datums()
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret); ++i) {
    if (OB_FAIL(childs_[i]->init_evaluated_datums())) {
      LOG_WARN("failed to filter child", K(ret));
    }
  }
  return ret;
}

int ObOrFilterExecutor::init_evaluated_datums()
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret); ++i) {
    if (OB_FAIL(childs_[i]->init_evaluated_datums())) {
      LOG_WARN("failed to filter child", K(ret));
    }
  }
  return ret;
}

ObPhysicalFilterExecutor::~ObPhysicalFilterExecutor()
{
  if (nullptr != eval_infos_) {
    allocator_.free(eval_infos_);
    eval_infos_ = nullptr;
  }
  if (nullptr != datum_eval_flags_) {
    allocator_.free(datum_eval_flags_);
    datum_eval_flags_ = nullptr;
  }
}

int ObPhysicalFilterExecutor::filter(blocksstable::ObStorageDatum *datums, int64_t col_cnt, const sql::ObBitVector &skip_bit, bool &filtered)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObExpr *> *column_exprs = get_cg_col_exprs();
  if (OB_UNLIKELY(col_cnt != column_exprs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected status: column count not match", K(ret), K(col_cnt));
  } else {
    ObEvalCtx &eval_ctx = op_.get_eval_ctx();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs->count(); ++i) {
      ObDatum &expr_datum = column_exprs->at(i)->locate_datum_for_write(eval_ctx);
      if (OB_FAIL(expr_datum.from_storage_datum(datums[i], column_exprs->at(i)->obj_datum_map_))) {
        LOG_WARN("Failed to convert object from datum", K(ret), K(datums[i]));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(filter(eval_ctx, skip_bit, filtered))) {
      LOG_WARN("Failed to calc filter", K(ret));
    }
  }
  return ret;
}

// 根据calc expr来设置每个列（空集）对应的清理Datum
// 这里将clear的datum放在filter node是为了更精准处理，其实只有涉及到的表达式清理即可，其他不需要清理
// 还有类似空集需要清理
int ObPhysicalFilterExecutor::init_evaluated_datums()
{
  int ret = OB_SUCCESS;
  const int32_t cur_eval_info_cnt = n_eval_infos_;
  n_eval_infos_ = 0;
  n_datum_eval_flags_ = 0;
  ObSEArray<ObExpr*, 4> eval_exprs;
  ObIArray<ObExpr*> *filter_exprs = nullptr;
  ObSEArray<ObExpr*, 1> white_filter_exprs;
  if (is_filter_black_node()) {
    filter_exprs = &(static_cast<ObBlackFilterExecutor *>(this)->get_filter_node().filter_exprs_);
  } else {
    if (OB_FAIL(white_filter_exprs.push_back(static_cast<ObWhiteFilterExecutor *>(this)->get_filter_node().expr_))) {
      LOG_WARN("Failed to push back filter expr", K(ret));
    } else {
      filter_exprs = &white_filter_exprs;
    }
  }

  if (OB_FAIL(ret)){
  } else if (OB_FAIL(find_evaluated_datums(*filter_exprs, op_.expr_spec_.calc_exprs_, eval_exprs))) {
    LOG_WARN("failed to find evaluated datums", K(ret));
  } else if (0 < eval_exprs.count()) {
    if (OB_FAIL(init_eval_param(cur_eval_info_cnt, eval_exprs.count()))) {
       LOG_WARN("failed to reuse filter param", K(ret));
    }
    FOREACH_CNT_X(e, eval_exprs, OB_SUCC(ret)) {
      eval_infos_[n_eval_infos_++] = &(*e)->get_eval_info(op_.get_eval_ctx());
      if (op_.is_vectorized() && (*e)->is_batch_result()) {
        datum_eval_flags_[n_datum_eval_flags_++] = &(*e)->get_evaluated_flags(op_.get_eval_ctx());
      }
    }
    if (OB_SUCC(ret)) {
      clear_evaluated_infos();
    }
  }
  return ret;
}

int ObPhysicalFilterExecutor::init_eval_param(const int32_t cur_eval_info_cnt, const int64_t eval_expr_cnt)
{
  int ret = OB_SUCCESS;
  if (eval_expr_cnt > cur_eval_info_cnt) {
    if (nullptr != eval_infos_) {
      allocator_.free(eval_infos_);
      eval_infos_ = nullptr;
    }
    if (nullptr != datum_eval_flags_) {
      allocator_.free(datum_eval_flags_);
      datum_eval_flags_ = nullptr;
    }
  }
  if (nullptr == eval_infos_) {
    if (OB_ISNULL(eval_infos_ = static_cast<ObEvalInfo **>(allocator_.alloc(
                  eval_expr_cnt * sizeof(ObEvalInfo*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate eval infos", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == datum_eval_flags_) {
    if (OB_ISNULL(datum_eval_flags_ = static_cast<ObBitVector **>(allocator_.alloc(
                  eval_expr_cnt * sizeof(ObBitVector*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate eval flags", K(ret));
    }
  }
  return ret;
}

void ObPhysicalFilterExecutor::clear_evaluated_flags()
{
  if (op_.is_vectorized()) {
    for (int i = 0; i < n_datum_eval_flags_; i++) {
      datum_eval_flags_[i]->unset(op_.get_eval_ctx().get_batch_idx());
    }
  } else {
    for (int i = 0; i < n_eval_infos_; i++) {
      eval_infos_[i]->clear_evaluated_flag();
    }
  }
}

void ObPhysicalFilterExecutor::clear_evaluated_infos()
{
  for (int i = 0; i < n_eval_infos_; i++) {
    eval_infos_[i]->clear_evaluated_flag();
  }
}

int ObWhiteFilterExecutor::init_evaluated_datums()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), KPC(filter_.expr_));
  } else if (WHITE_OP_IN == filter_.get_op_type()) {
    if (OB_FAIL(init_in_eval_datums())) {
      LOG_WARN("Failed to init eval datums for WHITE_OP_IN filter", K(ret));
    }
  } else if (OB_FAIL(init_compare_eval_datums())) {
    LOG_WARN("Failed to init eval datums for compare white filter", K(ret));
  }
  LOG_DEBUG("[PUSHDOWN], white pushdown filter inited datum params", K(datum_params_));
  return ret;
}

int ObWhiteFilterExecutor::init_compare_eval_datums()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  ObObjMeta col_obj_meta;
  ObObjMeta param_obj_meta;
  bool is_ref_column_found = false;
  if (OB_UNLIKELY(filter_.expr_->arg_cnt_ < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), KPC(filter_.expr_));
  } else if (OB_FAIL(ObPhysicalFilterExecutor::init_evaluated_datums())) {
    LOG_WARN("Failed to init evaluated datums", K(ret));
  } else if (OB_FAIL(init_array_param(datum_params_, filter_.expr_->arg_cnt_))) {
    LOG_WARN("Failed to alloc params", K(ret));
  } else {
    null_param_contained_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_.expr_->arg_cnt_; i++) {
      if (OB_ISNULL(filter_.expr_->args_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null expr arguments", K(ret), K(i));
      } else if (filter_.expr_->args_[i]->type_ == T_REF_COLUMN) {
        is_ref_column_found = true;
        col_obj_meta = filter_.expr_->args_[i]->obj_meta_;
        // skip column reference expr
        continue;
      } else {
        ObDatum *datum = NULL;
        if (OB_FAIL(filter_.expr_->args_[i]->eval(eval_ctx, datum))) {
          LOG_WARN("evaluate filter arg expr failed", K(ret), K(i));
        } else if (OB_FAIL(datum_params_.push_back(*datum))) {
          LOG_WARN("Failed to push back datum", K(ret));
        } else if (is_null_param(*datum, param_obj_meta)) {
          null_param_contained_ = true;
        } else {
          param_obj_meta = filter_.expr_->args_[i]->obj_meta_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_ref_column_found)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected no ref column found", K(ret));
    } else {
      cmp_func_ = get_datum_cmp_func(col_obj_meta, param_obj_meta);
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::init_in_eval_datums()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  ObObjMeta col_obj_meta;
  ObObjMeta param_obj_meta;
  if (OB_UNLIKELY(filter_.expr_->arg_cnt_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), KPC(filter_.expr_));
  } else if (OB_UNLIKELY(nullptr == filter_.expr_->args_[0] ||
                         T_REF_COLUMN != filter_.expr_->args_[0]->type_ ||
                         nullptr == filter_.expr_->args_[1] ||
                         0 >= filter_.expr_->inner_func_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter expr", K(ret), KPC(filter_.expr_), KP(filter_.expr_->args_[0]), KP(filter_.expr_->args_[1]));
  } else if (OB_FAIL(ObPhysicalFilterExecutor::init_evaluated_datums())) {
    LOG_WARN("Failed to init evaluated datums", K(ret));
  } else if (OB_FAIL(init_array_param(datum_params_, filter_.expr_->inner_func_cnt_))) {
    LOG_WARN("Failed to alloc params", K(ret));
  } else if (OB_FAIL(init_param_set(filter_.expr_->inner_func_cnt_, filter_.expr_->args_[1]->args_[0]))) {
    LOG_WARN("Failed to init datum set", K(ret));
  } else {
    col_obj_meta = filter_.expr_->args_[0]->obj_meta_;
    null_param_contained_ = false;
    ObDatum *datum = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < filter_.expr_->inner_func_cnt_; ++i) {
      const ObExpr *cur_arg = filter_.expr_->args_[1]->args_[i];
      if (OB_ISNULL(cur_arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null arg", K(ret), K(cur_arg));
      } else if (i == 0) {
        param_obj_meta = cur_arg->obj_meta_;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cur_arg->eval(eval_ctx, datum))) {
          LOG_WARN("Evaluate filter arg expr failed", K(ret), K(i));
        } else if (is_null_param(*datum, param_obj_meta)) {
          // skip null in filter IN
        } else if (OB_FAIL(add_to_param_set_and_array(*datum, cur_arg))) {
          LOG_WARN("Failed to add param to set", K(ret), KPC(datum), K(cur_arg));
        }
      }
    }
    if (datum_params_.count() == 0) {
      null_param_contained_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    bool mock_equal = false;
    ObDatumComparator cmp(filter_.expr_->args_[1]->args_[0]->basic_funcs_->null_first_cmp_, ret, mock_equal);
    lib::ob_sort(datum_params_.begin(), datum_params_.end(), cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to sort datums", K(ret));
    } else {
      cmp_func_ = get_datum_cmp_func(col_obj_meta, param_obj_meta);
      cmp_func_rev_ = get_datum_cmp_func(param_obj_meta, col_obj_meta);
      // When initializing a parameter set, the corresponding hash and comparison functions of the parameter type are used.
      // However, during subsequent exist checks, comparison is done between the parameter and the column.
      // Therefore, it is necessary to convert the corresponding function types.
      param_set_.set_hash_and_cmp_func(filter_.expr_->args_[0]->basic_funcs_->murmur_hash_v2_, cmp_func_rev_);
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::init_param_set(const int64_t count, const ObExpr *param_arg)
{
  int ret = OB_SUCCESS;
  if (param_set_.created()) {
    param_set_.destroy();
  }
  if (OB_FAIL(param_set_.create(count * 2))) {
    LOG_WARN("Failed to create hash set", K(ret), K(count));
  } else {
    param_set_.set_hash_and_cmp_func(param_arg->basic_funcs_->murmur_hash_v2_, param_arg->basic_funcs_->null_first_cmp_);
  }
  return ret;
}

int ObWhiteFilterExecutor::add_to_param_set_and_array(const ObDatum &datum, const ObExpr *cur_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_set_.set_refactored(datum))) {
    if (OB_UNLIKELY(ret != OB_HASH_EXIST)) {
      LOG_WARN("Failed to insert object into hashset", K(ret), K(datum));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(datum_params_.push_back(datum))) {
    LOG_WARN("Failed to add datum to datum array", K(ret), K(datum));
  }
  return ret;
}

int ObWhiteFilterExecutor::exist_in_datum_set(const ObDatum &datum, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (param_set_.count() > 0) {
    if (OB_FAIL(param_set_.exist_refactored(datum, is_exist))) {
      LOG_WARN("Failed to search datum in param set", K(ret), K(datum));
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::exist_in_datum_array(const ObDatum &datum, bool &is_exist, const int64_t offset) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (datum_params_.count() > 0) {
    ObDatumComparator cmp(cmp_func_rev_, ret, is_exist);
    std::lower_bound(datum_params_.begin() + offset, datum_params_.end(), datum, cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to search datum in param array", K(ret), K(datum));
    }
  }
  return ret;
}

bool ObWhiteFilterExecutor::is_cmp_op_with_null_ref_value() const
{
  const bool is_cmp_op =
      get_op_type() >= WHITE_OP_EQ
      && get_op_type() <= WHITE_OP_NE;
  const bool has_single_null_ref_value =
      1 == get_datums().count()
      && get_datums().at(0).is_null();
  return is_cmp_op && has_single_null_ref_value;
}

int ObWhiteFilterExecutor::filter(ObEvalCtx &eval_ctx, const sql::ObBitVector &skip_bit, bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  if (!op_.enable_rich_format_) {
    ObDatum *cmp_res = nullptr;
    if (OB_FAIL(filter_.expr_->eval(eval_ctx, cmp_res))) {
      LOG_WARN("Failed to eval", K(ret));
    } else {
      filtered = is_row_filtered(*cmp_res);
    }
  } else {
    const int64_t batch_idx = eval_ctx.get_batch_idx();
    EvalBound eval_bound(eval_ctx.get_batch_size(), batch_idx, batch_idx + 1, false);
    if (OB_FAIL(filter_.expr_->eval_vector(eval_ctx, skip_bit, eval_bound))) {
      LOG_WARN("Failed to eval vector", K(ret));
    } else {
      ObIVector *res = filter_.expr_->get_vector(eval_ctx);
      filtered = !res->is_true(batch_idx);
    }
  }
  clear_evaluated_flags();
  return ret;
}

ObBlackFilterExecutor::~ObBlackFilterExecutor()
{
  if (nullptr != skip_bit_) {
    allocator_.free(skip_bit_);
    skip_bit_ = nullptr;
  }
}

int ObBlackFilterExecutor::filter(ObEvalCtx &eval_ctx, const sql::ObBitVector &skip_bit, bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  const bool enable_rich_format = op_.enable_rich_format_;
  if (!enable_rich_format) {
    ObDatum *cmp_res = nullptr;
    FOREACH_CNT_X(e, filter_.filter_exprs_, OB_SUCC(ret) && !filtered) {
      if (OB_FAIL((*e)->eval(eval_ctx, cmp_res))) {
        LOG_WARN("failed to filter child", K(ret));
      } else {
        filtered = is_row_filtered(*cmp_res);
      }
    }
  } else {
    const int64_t batch_idx = eval_ctx.get_batch_idx();
    EvalBound eval_bound(eval_ctx.get_batch_size(), batch_idx, batch_idx + 1, false);
    FOREACH_CNT_X(e, filter_.filter_exprs_, OB_SUCC(ret) && !filtered) {
      if (OB_FAIL((*e)->eval_vector(eval_ctx, skip_bit, eval_bound))) {
        LOG_WARN("Failed to evaluate vector", K(ret));
      } else {
        ObIVector *res = (*e)->get_vector(eval_ctx);
        filtered = !res->is_true(batch_idx);
      }
    }
  }
  clear_evaluated_flags();
  return ret;
}

int ObBlackFilterExecutor::filter(blocksstable::ObStorageDatum &datum, const sql::ObBitVector &skip_bit, bool &ret_val)
{
  return ObPhysicalFilterExecutor::filter(&datum, 1, skip_bit, ret_val);
}

int ObBlackFilterExecutor::judge_greater_or_less(
    blocksstable::ObStorageDatum &datum,
    const sql::ObBitVector &skip_bit,
    const bool is_greater,
    bool &ret_val)
{
  int ret = OB_SUCCESS;
  ret_val = false;
  sql::ObExpr *assist_expr = nullptr;
  sql::ObExpr *column_expr = nullptr;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  const common::ObIArray<ObExpr *> *column_exprs = get_cg_col_exprs();
  if (OB_UNLIKELY(nullptr == column_exprs || column_exprs->count() != 1 ||
                  filter_.assist_exprs_.count() != 2 ||
                  filter_.mono_ < MON_NON || filter_.mono_ > MON_EQ_DESC)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter status", K(ret), KPC(column_exprs), K_(filter));
  } else if (FALSE_IT(assist_expr = is_greater ? filter_.assist_exprs_.at(0) : filter_.assist_exprs_.at(1))) {
  } else if (FALSE_IT(column_expr = column_exprs->at(0))) {
  } else if (OB_ISNULL(assist_expr) || OB_ISNULL(column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect null expr", K(ret), K(filter_.assist_exprs_), KPC(column_exprs));
  } else {
    ObDatum &expr_datum = column_expr->locate_datum_for_write(eval_ctx);
    if (OB_FAIL(expr_datum.from_storage_datum(datum, column_expr->obj_datum_map_))) {
      LOG_WARN("Failed to convert from datum", K(ret), K(datum));
    } else if (!op_.enable_rich_format_) {
      ObDatum *cmp_res = nullptr;
      if (OB_FAIL(assist_expr->eval(eval_ctx, cmp_res))) {
        LOG_WARN("failed to filter child", K(ret));
      } else {
        ret_val = !is_row_filtered(*cmp_res);
      }
    } else {
      const int64_t batch_idx = eval_ctx.get_batch_idx();
      EvalBound eval_bound(eval_ctx.get_batch_size(), batch_idx, batch_idx + 1, true);
      if (OB_FAIL(assist_expr->eval_vector(eval_ctx, skip_bit, eval_bound))) {
        LOG_WARN("Failed to evaluate vector", K(ret));
      } else {
        ObIVector *res = assist_expr->get_vector(eval_ctx);
        ret_val = res->is_true(batch_idx);
      }
    }
    clear_evaluated_flags();
    if (op_.is_vectorized() && assist_expr->is_batch_result()) {
      assist_expr->get_evaluated_flags(eval_ctx).unset(eval_ctx.get_batch_idx());
    } else {
      assist_expr->get_eval_info(eval_ctx).clear_evaluated_flag();
    }
    LOG_DEBUG("check judge greater or less status", K(expr_datum), K(datum), KPC(column_expr), KPC(assist_expr), K(is_greater));
  }
  return ret;
}

int ObBlackFilterExecutor::get_datums_from_column(common::ObIArray<blocksstable::ObSqlDatumInfo> &datum_infos)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  FOREACH_CNT_X(e, filter_.column_exprs_, OB_SUCC(ret)) {
    if (OB_FAIL(datum_infos.push_back(
                blocksstable::ObSqlDatumInfo((*e)->locate_batch_datums(eval_ctx), (*e))))) {
      LOG_WARN("fail to push back datum ifno", K(ret));
    }
  }
  return ret;
}

// mask filter datums, set %bit_vec to 1 if datums filtered
typedef void (*MarkFilterdDatumsFunc)(const ObDatum *datums,
                                        const uint64_t *values,
                                        const int64_t size,
                                        ObBitVector &bit_vec);

void mark_filtered_datums(const ObDatum *datums,
                          const uint64_t *values,
                          const int64_t size,
                          ObBitVector &bit_vec)
{
  bit_vec.reset(size);
  for (int64_t i = 0; i < size; i++) {
    if (datums[i].is_null() || 0 == values[i]) {
      bit_vec.set(i);
    }
  }
}

extern void mark_filtered_datums_simd(const ObDatum *datums,
                                      const uint64_t *values,
                                      const int64_t size,
                                      ObBitVector &bit_vec);

MarkFilterdDatumsFunc get_mark_filterd_datums_func()
{
  return blocksstable::is_avx512_valid()
      ? mark_filtered_datums_simd
      : mark_filtered_datums;
}

MarkFilterdDatumsFunc mark_filtered_datums_func = get_mark_filterd_datums_func();

int ObBlackFilterExecutor::eval_exprs_batch(ObBitVector &skip, const int64_t bsize)
{
  int ret = OB_SUCCESS;
  clear_evaluated_infos();
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  const bool enable_rich_format = op_.enable_rich_format_;
  FOREACH_CNT_X(e, filter_.column_exprs_, OB_SUCC(ret)) {
    (*e)->get_eval_info(eval_ctx).projected_ = true;
  }
  FOREACH_CNT_X(e, filter_.filter_exprs_, OB_SUCC(ret) && !skip.is_all_true(bsize)) {
    if (enable_rich_format) {
      if (OB_FAIL((*e)->eval_vector(eval_ctx, skip, bsize, skip.is_all_false(bsize)))) {
        LOG_WARN("evaluate batch failed", K(ret));
      } else {
        ObIVector *res = (*e)->get_vector(eval_ctx);
        if (VectorFormat::VEC_FIXED == res->get_format()) {
          ObFixedLengthBase *fixed_length_base = static_cast<ObFixedLengthBase *>(res);
          const bool has_null = fixed_length_base->has_null();
          const char *data = fixed_length_base->get_data();
          sql::ObBitVector *nulls = fixed_length_base->get_nulls();
          common::ObBitmap::filter(
              has_null, reinterpret_cast<uint8_t *>(nulls->data_), reinterpret_cast<const uint64_t *>(data),
              bsize, reinterpret_cast<uint8_t *>(skip.data_));
        } else {
          for (int64_t i = 0; i < bsize; i++) {
            if (!skip.at(i) && !res->is_true(i)) {
              skip.set(i);
            }
          }
        }
      }
    } else {
      if (OB_FAIL((*e)->eval_batch(eval_ctx, skip, bsize))) {
       LOG_WARN("evaluate batch failed", K(ret));
      } else if (!(*e)->is_batch_result()) {
        const ObDatum &d = (*e)->locate_expr_datum(eval_ctx);
        if (is_row_filtered(d)) {
          skip.set_all(bsize);
        }
      } else {
        const ObDatum *datums = (*e)->locate_batch_datums(eval_ctx);
        if (mark_filtered_datums_simd == mark_filtered_datums_func
            && (*e)->get_eval_info(eval_ctx).point_to_frame_) {
          char bit_vec_mem[ObBitVector::memory_size(bsize)];
          ObBitVector *tmp_vec = to_bit_vector(bit_vec_mem);
          mark_filtered_datums_func(datums,
                                   reinterpret_cast<uint64_t *>((*e)->get_rev_buf(eval_ctx)),
                                  bsize,
                                  *tmp_vec);
          skip.bit_calculate(skip, *tmp_vec, bsize,
                             [](uint64_t l, uint64_t r) { return l | r; });
        } else {
          for (int64_t i = 0; i < bsize; i++) {
            if (!skip.at(i) && is_row_filtered(datums[i])) {
              skip.set(i);
            }
          }
        }
      }
    }
  }
  clear_evaluated_infos();
  return ret;
}

int ObBlackFilterExecutor::filter_batch(
    ObPushdownFilterExecutor *parent,
    const int64_t start,
    const int64_t end,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t bsize = end - start;
  if (OB_UNLIKELY(start >= end || bsize > op_.get_batch_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid batch row idx", K(ret), K(start), K(end), K(op_.get_batch_size()));
  } else if (nullptr == skip_bit_) {
    if (OB_ISNULL(skip_bit_ = to_bit_vector(
                (char *)(allocator_.alloc(ObBitVector::memory_size(op_.get_batch_size())))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc skip_bit", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr != parent && parent->need_check_row_filter()) {
      if (OB_FAIL(parent->get_result()->to_bits_mask(start, end, parent->is_logic_and_node(),
                                                      reinterpret_cast<uint8_t *>(skip_bit_->data_)))) {
        LOG_WARN("failed to transform bytes mask to bit mask", K(start), K(end), KPC(parent->get_result()));
      }
    } else {
      skip_bit_->init(bsize);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(eval_exprs_batch(*skip_bit_, bsize))) {
        LOG_WARN("failed to eval batch or", K(ret));
      } else if (FALSE_IT(skip_bit_->bit_not(bsize))) {
      } else if (OB_FAIL(result_bitmap.from_bits_mask(start, end, reinterpret_cast<uint8_t *>(skip_bit_->data_)))) {
        LOG_WARN("failed to set filter result bitmap", K(start), K(end));
      }
    }
  }
  LOG_DEBUG("[PUSHDOWN] microblock black pushdown filter batch row", K(ret), K(start), K(end),
            "filtered_cnt", bsize - skip_bit_->accumulate_bit_cnt(bsize), "popcnt", result_bitmap.popcnt());
  return ret;
}


void ObDynamicFilterExecutor::filter_on_bypass(ObPushdownFilterExecutor* parent_filter)
{
  ObPushdownDynamicFilterNode &dynamic_filter_node =
      static_cast<ObPushdownDynamicFilterNode &>(filter_);
  int64_t total_rows_count = 0;
  int64_t filter_count = 0;
  int64_t check_count = 0;

  if (!dynamic_filter_node.is_first_child()) {
  } else {
    total_rows_count = filter_bitmap_->size();
    if (parent_filter) {
      total_rows_count = parent_filter->get_result()->popcnt();
    }
    if (is_filter_always_false() || DynamicFilterAction::FILTER_ALL == filter_action_) {
      // is_filter_always_false is set by skip index
      // filter_action_ is set by runtime filter msg
      filter_count = total_rows_count;
      check_count = total_rows_count;
    }
    if (OB_NOT_NULL(runtime_filter_ctx_)) {
      runtime_filter_ctx_->collect_monitor_info(filter_count, check_count, total_rows_count);
    }
  }
  if (dynamic_filter_node.is_last_child()) {
    if (OB_NOT_NULL(runtime_filter_ctx_)) {
      runtime_filter_ctx_->collect_sample_info(filter_count, total_rows_count);
    }
  }
}

void ObDynamicFilterExecutor::filter_on_success(ObPushdownFilterExecutor* parent_filter)
{
  int64_t total_rows_count = filter_bitmap_->size();
  int64_t filtered_rows_count = total_rows_count - filter_bitmap_->popcnt();
  if (parent_filter) {
    const int64_t skipped_rows_count = parent_filter->get_skipped_rows();
    total_rows_count -= skipped_rows_count;
    if (parent_filter->is_logic_and_node()) {
      filtered_rows_count -= skipped_rows_count;
    }
  }
  ObPushdownDynamicFilterNode &dynamic_filter_node =
      static_cast<ObPushdownDynamicFilterNode &>(filter_);
  if (!dynamic_filter_node.is_first_child()) {
    total_rows_count = 0;
  }
  if (OB_NOT_NULL(runtime_filter_ctx_)) {
    runtime_filter_ctx_->collect_monitor_info(filtered_rows_count, total_rows_count,
                                              total_rows_count);
    if (dynamic_filter_node.is_last_child()) {
      runtime_filter_ctx_->collect_sample_info(filtered_rows_count, total_rows_count);
    }
  }
}

int ObDynamicFilterExecutor::init_evaluated_datums()
{
  int ret = OB_SUCCESS;
  if (is_data_prepared_ && OB_NOT_NULL(runtime_filter_ctx_)
      && runtime_filter_ctx_->need_reset_in_rescan()) {
    is_data_prepared_ = false;
    batch_cnt_ = 0;
    datum_params_.clear();
  }
  return ret;
}

int ObDynamicFilterExecutor::check_runtime_filter(ObPushdownFilterExecutor *parent_filter,
                                                  bool &is_needed)
{
  int ret = OB_SUCCESS;
  is_needed = false;
  if (is_first_check_) {
    locate_runtime_filter_ctx();
    is_first_check_ = false;
  }
  // If data has prepared, and need continuous update(such as topn runtime filter)
  // we check whether the data in runtime filter has a new version and then update it.
  // If the data has not prepared, we check whether the runtime filter is ready and
  // get data from it.
  if (is_data_prepared() && is_data_version_updated() && OB_FAIL(try_updating_data())) {
    LOG_WARN("Failed to updating data");
  } else if (!is_data_prepared() /*&& (0 == ((batch_cnt_++) % DEFAULT_CHECK_INTERVAL))*/ &&
      OB_FAIL(try_preparing_data())) {
    LOG_WARN("Failed to try preparing data", K_(is_data_prepared));
  } else {
    if (!is_data_prepared()) {
      filter_bitmap_->reuse(true);
    } else {
      if (DynamicFilterAction::PASS_ALL == filter_action_) {
        filter_bitmap_->reuse(true);
      } else if (DynamicFilterAction::FILTER_ALL == filter_action_) {
        // bitmap is inited as all false, do not fill false again
      } else if (DynamicFilterAction::DO_FILTER == filter_action_) {
        is_needed = !runtime_filter_ctx_->dynamic_disable();
        if (!is_needed) {
          filter_bitmap_->reuse(true);
        }
      }
    }
  }
  return ret;
}

void ObDynamicFilterExecutor::locate_runtime_filter_ctx()
{
  const uint64_t op_id = get_filter_node().expr_->expr_ctx_id_;
  ObEvalCtx &eval_ctx = op_.get_eval_ctx();
  runtime_filter_ctx_ = static_cast<ObExprOperatorCtx *>(eval_ctx.exec_ctx_.get_expr_op_ctx(op_id));
}

int ObDynamicFilterExecutor::try_preparing_data()
{
  int ret = OB_SUCCESS;
  ObRuntimeFilterParams runtime_filter_params;
  DynamicFilterType dynamic_filter_type =
      static_cast<ObPushdownDynamicFilterNode &>(filter_).get_dynamic_filter_type();
  if (dynamic_filter_type >= DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid func type", K(ret), K(dynamic_filter_type));
  } else {
    ret = PREPARE_PD_DATA_FUNCS[dynamic_filter_type](
        *filter_.expr_, *this, op_.get_eval_ctx(), runtime_filter_params, is_data_prepared_);
  }
  if (OB_FAIL(ret)) {
  } else if (is_data_prepared_) {
    if (OB_FAIL(datum_params_.assign(runtime_filter_params))) {
      LOG_WARN("Failed to assing params for white filter", K(runtime_filter_params));
    } else {
      // runtime filter with null equal condition will not be pushed down as white filter,
      // so it's not need to check null params.
      // check_null_params();
    }
  }
  return ret;
}

int ObDynamicFilterExecutor::try_updating_data()
{
  int ret = OB_SUCCESS;
  bool is_update = false;
  ObRuntimeFilterParams runtime_filter_params;
  DynamicFilterType dynamic_filter_type =
      static_cast<ObPushdownDynamicFilterNode &>(filter_).get_dynamic_filter_type();
  if (dynamic_filter_type >= DynamicFilterType::MAX_DYNAMIC_FILTER_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid func type", K(ret), K(dynamic_filter_type));
  } else {
    ret = UPDATE_PD_DATA_FUNCS[dynamic_filter_type](
        *filter_.expr_, *this, op_.get_eval_ctx(), runtime_filter_params, is_update);
  }
  if (OB_FAIL(ret)) {
  } else if (is_update) {
    if (OB_FAIL(datum_params_.assign(runtime_filter_params))) {
      LOG_WARN("Failed to assing params for white filter", K(runtime_filter_params));
    }
  }
  return ret;
}

inline bool ObDynamicFilterExecutor::is_data_version_updated()
{
  bool bool_ret = false;
  if (!get_filter_node().need_continuous_update()) {
  } else if (OB_NOT_NULL(runtime_filter_ctx_)) {
    bool_ret = runtime_filter_ctx_->is_data_version_updated(stored_data_version_);
  }
  return bool_ret;
}

//--------------------- end filter executor ----------------------------


//--------------------- start filter executor constructor ----------------------------
template<typename CLASST, PushdownExecutorType type>
int ObFilterExecutorConstructor::create_filter_executor(
    ObPushdownFilterNode *filter_tree,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = OB_SUCCESS;
  ObPushdownFilterExecutor *tmp_filter_executor = nullptr;
  if (OB_ISNULL(filter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter tree is null", K(ret));
  } else if (OB_FAIL(factory_.alloc(type, filter_tree->n_child_, *filter_tree, tmp_filter_executor, op))) {
    LOG_WARN("failed to alloc pushdown filter", K(ret), K(filter_tree->n_child_));
  } else {
    filter_executor = static_cast<CLASST*>(tmp_filter_executor);
    for (int64_t i = 0; i < filter_tree->n_child_ && OB_SUCC(ret); ++i) {
      ObPushdownFilterExecutor *sub_filter_executor = nullptr;
      if (OB_FAIL(apply(filter_tree->childs_[i], sub_filter_executor, op))) {
        LOG_WARN("failed to apply filter node", K(ret));
      } else if (OB_ISNULL(sub_filter_executor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub filter executor is null", K(ret));
      } else if (OB_ISNULL(filter_executor->get_childs())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("childs is null", K(ret));
      } else {
        filter_executor->set_child(i, sub_filter_executor);
      }
    }
  }
  return ret;
}

int ObFilterExecutorConstructor::apply(
    ObPushdownFilterNode *filter_tree,
    ObPushdownFilterExecutor *&filter_executor,
    ObPushdownOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is null", K(ret));
  } else {
    switch(filter_tree->get_type()) {
      case BLACK_FILTER: {
        ret = create_filter_executor<ObBlackFilterExecutor, BLACK_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case WHITE_FILTER: {
        ret = create_filter_executor<ObWhiteFilterExecutor, WHITE_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case DYNAMIC_FILTER: {
        ret = create_filter_executor<ObDynamicFilterExecutor, DYNAMIC_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case AND_FILTER: {
        ret = create_filter_executor<ObAndFilterExecutor, AND_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case OR_FILTER: {
        ret = create_filter_executor<ObOrFilterExecutor, OR_FILTER_EXECUTOR>(filter_tree, filter_executor, op);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected filter type", K(ret));
        break;
    }
  }
  return ret;
}

ObPushdownExprSpec::ObPushdownExprSpec(ObIAllocator &alloc)
  : calc_exprs_(alloc),
    access_exprs_(alloc),
    max_batch_size_(0),
    pushdown_filters_(alloc),
    pd_storage_flag_(0),
    pd_storage_filters_(alloc),
    pd_storage_aggregate_output_(alloc),
    ext_file_column_exprs_(alloc),
    ext_column_convert_exprs_(alloc),
    trans_info_expr_(nullptr),
    auto_split_filter_type_(OB_INVALID_ID),
    auto_split_expr_(nullptr),
    auto_split_params_(alloc)
{
}

OB_DEF_SERIALIZE(ObPushdownExprSpec)
{
  int ret = OB_SUCCESS;
  ExprFixedArray fake_filters_before_index_back(CURRENT_CONTEXT->get_allocator());
  ObPushdownFilter fake_pd_storage_index_back_filters(CURRENT_CONTEXT->get_allocator());
  LST_DO_CODE(OB_UNIS_ENCODE,
              calc_exprs_,
              access_exprs_,
              max_batch_size_,
              pushdown_filters_,
              fake_filters_before_index_back, //mock a fake filters to compatible with 4.0
              pd_storage_flag_.pd_flag_,
              pd_storage_filters_,
              fake_pd_storage_index_back_filters, //mock a fake filters to compatible with 4.0
              pd_storage_aggregate_output_,
              ext_file_column_exprs_,
              ext_column_convert_exprs_,
              trans_info_expr_,
              auto_split_filter_type_,
              auto_split_expr_,
              auto_split_params_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPushdownExprSpec)
{
  int ret = OB_SUCCESS;
  ExprFixedArray fake_filters_before_index_back(CURRENT_CONTEXT->get_allocator());
  ObPushdownFilter fake_pd_storage_index_back_filters(CURRENT_CONTEXT->get_allocator());
  LST_DO_CODE(OB_UNIS_DECODE,
              calc_exprs_,
              access_exprs_,
              max_batch_size_,
              pushdown_filters_,
              fake_filters_before_index_back, //mock a fake filters to compatible with 4.0
              pd_storage_flag_.pd_flag_,
              pd_storage_filters_,
              fake_pd_storage_index_back_filters, //mock a fake filters to compatible with 4.0
              pd_storage_aggregate_output_,
              ext_file_column_exprs_,
              ext_column_convert_exprs_,
              trans_info_expr_,
              auto_split_filter_type_,
              auto_split_expr_,
              auto_split_params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushdownExprSpec)
{
  int64_t len = 0;
  ExprFixedArray fake_filters_before_index_back(CURRENT_CONTEXT->get_allocator());
  ObPushdownFilter fake_pd_storage_index_back_filters(CURRENT_CONTEXT->get_allocator());
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              calc_exprs_,
              access_exprs_,
              max_batch_size_,
              pushdown_filters_,
              fake_filters_before_index_back, //mock a fake filters to compatible with 4.0
              pd_storage_flag_.pd_flag_,
              pd_storage_filters_,
              fake_pd_storage_index_back_filters, //mock a fake filters to compatible with 4.0
              pd_storage_aggregate_output_,
              ext_file_column_exprs_,
              ext_column_convert_exprs_,
              trans_info_expr_,
              auto_split_filter_type_,
              auto_split_expr_,
              auto_split_params_);
  return len;
}

ObPushdownOperator::ObPushdownOperator(
    ObEvalCtx &eval_ctx,
    const ObPushdownExprSpec &expr_spec,
    const bool enable_rich_format)
  : pd_storage_filters_(nullptr),
    eval_ctx_(eval_ctx),
    expr_spec_(expr_spec),
    enable_rich_format_(enable_rich_format)
{
}

int ObPushdownOperator::init_pushdown_storage_filter()
{
  int ret = OB_SUCCESS;
  if (expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
    ObFilterExecutorConstructor filter_exec_constructor(&eval_ctx_.exec_ctx_.get_allocator());
    if (OB_NOT_NULL(expr_spec_.pd_storage_filters_.get_pushdown_filter())) {
      if (OB_FAIL(filter_exec_constructor.apply(expr_spec_.pd_storage_filters_.get_pushdown_filter(),
                                                pd_storage_filters_,
                                                *this))) {
        LOG_WARN("failed to create filter executor", K(ret));
      } else if (OB_ISNULL(pd_storage_filters_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter executor is null", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownOperator::reset_trans_info_datum()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr_spec_.trans_info_expr_)) {
    if (enable_rich_format_) {
      if (OB_FAIL(expr_spec_.trans_info_expr_->init_vector(
                  eval_ctx_,
                  VectorFormat::VEC_UNIFORM,
                  expr_spec_.trans_info_expr_->is_batch_result() ? expr_spec_.max_batch_size_ : 1))) {
        LOG_WARN("Fail to init vector", K(ret), K(expr_spec_.max_batch_size_));
      }
    }
    if (OB_SUCC(ret)) {
      if (expr_spec_.trans_info_expr_->is_batch_result()) {
        ObDatum *datums = expr_spec_.trans_info_expr_->locate_datums_for_update(eval_ctx_, expr_spec_.max_batch_size_);
        for (int64_t i = 0; i < expr_spec_.max_batch_size_; i++) {
          datums[i].set_null();
        }
      } else {
        ObDatum &datum = expr_spec_.trans_info_expr_->locate_datum_for_write(eval_ctx_);
        datum.set_null();
      }
    }
  }
  return ret;
}

int ObPushdownOperator::write_trans_info_datum(blocksstable::ObDatumRow &out_row)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr_spec_.trans_info_expr_) &&
      OB_NOT_NULL(out_row.trans_info_)) {
    ObDatum &datum = expr_spec_.trans_info_expr_->locate_datum_for_write(eval_ctx_);
    char *dst_ptr = const_cast<char *>(datum.ptr_);
    int64_t pos = 0;
    if (OB_ISNULL(dst_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (OB_FAIL(databuff_memcpy(dst_ptr,
                                       ObDASWriteBuffer::DAS_ROW_TRANS_STRING_SIZE,
                                       pos,
                                       strlen(out_row.trans_info_),
                                       out_row.trans_info_))) {
      LOG_WARN("fail to copy trans info to datum", K(ret));
    } else {
      datum.pack_ = pos;
      // out_row.trans_info_ must be reset to nullptr to prevent affecting the next row
      out_row.trans_info_ = nullptr;
    }
  }
  return ret;
}

int ObPushdownOperator::clear_datum_eval_flag()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT(e, expr_spec_.calc_exprs_) {
    if ((*e)->is_batch_result()) {
      (*e)->get_evaluated_flags(eval_ctx_).unset(eval_ctx_.get_batch_idx());
    } else {
      (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
    }
  }
  return ret;
}

int ObPushdownOperator::clear_evaluated_flag()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT(e, expr_spec_.calc_exprs_) {
    (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
  }
  return ret;
}

int ObPushdownOperator::deep_copy(const sql::ObExprPtrIArray *exprs, const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == exprs || batch_idx >= expr_spec_.max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exprs), K(batch_idx), K(expr_spec_.max_batch_size_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs->count(); i++) {
      char *ptr = nullptr;
      sql::ObExpr *e = exprs->at(i);
      if (OBJ_DATUM_STRING == e->obj_datum_map_) {
        ObDatum &datum = e->locate_expr_datum(eval_ctx_, batch_idx);
        if (!datum.null_) {
          if (OB_ISNULL(ptr = e->get_str_res_mem(eval_ctx_, datum.len_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            MEMMOVE(const_cast<char *>(ptr), datum.ptr_, datum.len_);
            datum.ptr_ = ptr;
          }
        }
      }
    }
  }
  return ret;
}

PushdownFilterInfo::~PushdownFilterInfo()
{
  reset();
}

void PushdownFilterInfo::reset()
{
  if (nullptr != allocator_) {
    if (nullptr != datum_buf_) {
      allocator_->free(datum_buf_);
      datum_buf_ = nullptr;
    }
    if (nullptr != cell_data_ptrs_) {
      allocator_->free(cell_data_ptrs_);
      cell_data_ptrs_ = nullptr;
    }
    if (nullptr != row_ids_) {
      allocator_->free(row_ids_);
      row_ids_ = nullptr;
    }
    if (nullptr != len_array_) {
      allocator_->free(len_array_);
      len_array_ = nullptr;
    }
    if (nullptr != ref_bitmap_) {
      ref_bitmap_->~ObBitmap();
      allocator_->free(ref_bitmap_);
      ref_bitmap_ = nullptr;
    }
    if (nullptr != skip_bit_) {
      allocator_->free(skip_bit_);
      skip_bit_ = nullptr;
    }
    allocator_ = nullptr;
  }
  filter_ = nullptr;
  param_ = nullptr;
  context_ = nullptr;
  is_inited_ = false;
  is_pd_filter_ = false;
  is_pd_to_cg_ = false;
  start_ = -1;
  count_ = 0;
  col_capacity_ = 0;
  batch_size_ = 0;
  col_datum_buf_.reset();
}

void PushdownFilterInfo::reuse()
{
  is_pd_to_cg_ = false;
  filter_ = nullptr;
  param_ = nullptr;
  context_ = nullptr;
  start_ = -1;
  count_ = 0;
}

int PushdownFilterInfo::init(const storage::ObTableIterParam &iter_param, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  void *len_array_buf = nullptr;
  int64_t out_col_cnt = iter_param.get_out_col_cnt();
  is_pd_filter_ = iter_param.enable_pd_filter();
  allocator_ = &alloc;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init store pushdown filter", K(ret), K(iter_param));
  } else if (nullptr == iter_param.pushdown_filter_) {
    // nothing to do without filter exprs
  } else if (OB_ISNULL((buf = alloc.alloc(sizeof(blocksstable::ObStorageDatum) * out_col_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for pushdown filter col buf", K(ret), K(out_col_cnt));
  } else if (FALSE_IT(datum_buf_ = new (buf) blocksstable::ObStorageDatum[out_col_cnt]())) {
  } else {
    filter_ = iter_param.pushdown_filter_;
    col_capacity_ = out_col_cnt;
  }

  if (OB_SUCC(ret) && (iter_param.vectorized_enabled_ || iter_param.enable_pd_aggregate() ||
                       (nullptr != iter_param.op_ && iter_param.op_->enable_rich_format_))) {
    batch_size_ = iter_param.vectorized_enabled_ ? iter_param.op_->get_batch_size() : storage::AGGREGATE_STORE_BATCH_SIZE;
    if (OB_FAIL(col_datum_buf_.init(batch_size_, alloc))) {
      LOG_WARN("fail to init tmp col datum buf", K(ret));
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(char *) * batch_size_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cell data ptr", K(ret), K(batch_size_));
    } else if (FALSE_IT(cell_data_ptrs_ = reinterpret_cast<const char **>(buf))) {
    } else if (OB_ISNULL(skip_bit_ = to_bit_vector(alloc.alloc(ObBitVector::memory_size(batch_size_))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc skip bit", K(ret), K_(batch_size));
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(int32_t) * batch_size_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc row_ids", K(ret), K(batch_size_));
    } else if (OB_ISNULL(len_array_buf = alloc.alloc(sizeof(uint32_t) * batch_size_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc len_array_buf", K(ret), K_(batch_size));
    } else {
      skip_bit_->init(batch_size_);
      row_ids_ = reinterpret_cast<int32_t *>(buf);
      len_array_ = reinterpret_cast<uint32_t *>(len_array_buf);
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_inited_ = true;
  }
  return ret;
}

int PushdownFilterInfo::init_bitmap(const int64_t row_count, common::ObBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(ref_bitmap_)) {
    if (OB_FAIL(ref_bitmap_->reserve(row_count))) {
      LOG_WARN("Failed to expand size of filter bitmap", K(ret));
    } else if (FALSE_IT(ref_bitmap_->reuse())) {
    }
  } else {
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObBitmap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for filter bitmap", K(ret));
    } else if (FALSE_IT(ref_bitmap_ = new (buf) common::ObBitmap(*allocator_))) {
    } else if (OB_FAIL(ref_bitmap_->init(row_count))) {
      LOG_WARN("Failed to init result bitmap", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bitmap = ref_bitmap_;
  }
  return ret;
}

int PushdownFilterInfo::get_col_datum(ObDatum *&datums) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(col_datum_buf_.get_col_datum(batch_size_, datums))) {
    LOG_WARN("Failed to get col datum from tmp datum buf", K(ret));
  }
  return ret;
}

void PushdownFilterInfo::TmpColDatumBuf::reset()
{
  if (nullptr != allocator_) {
    if (nullptr != datums_) {
      allocator_->free(datums_);
    }
    if (nullptr != data_buf_) {
      allocator_->free(data_buf_);
    }
  }
  datums_ = nullptr;
  data_buf_ = nullptr;
  batch_size_ = 0;
  allocator_ = nullptr;
}

int PushdownFilterInfo::TmpColDatumBuf::init(const int64_t batch_size, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != allocator_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret), K_(batch_size), K_(allocator));
  } else {
    datums_ = nullptr;
    data_buf_ = nullptr;
    batch_size_ = batch_size;
    allocator_ = &allocator;
  }
  return ret;
}

int PushdownFilterInfo::TmpColDatumBuf::get_col_datum(const int64_t batch_size, ObDatum *&datums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp col datum buf not inited", K(ret));
  } else if (OB_UNLIKELY(batch_size != batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid call to get col datum", K(ret), K_(batch_size), K(batch_size), KP_(allocator));
  } else if (nullptr != datums_) {
    datums = datums_;
    int64_t datum_ptr_offset = 0;
    for (int64_t i = 0; i < batch_size; ++i) {
      datums_[i].ptr_ = data_buf_ + datum_ptr_offset;
      datum_ptr_offset += common::OBJ_DATUM_NUMBER_RES_SIZE;
    }
  } else {
    char *datum_buf = nullptr;
    data_buf_ = nullptr;
    if (OB_ISNULL(datum_buf = static_cast<char *>(allocator_->alloc(batch_size * sizeof(ObDatum))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for tmp col datum", K(ret), K(batch_size));
    } else if (OB_ISNULL(data_buf_ = static_cast<char *>(allocator_->alloc(batch_size * common::OBJ_DATUM_NUMBER_RES_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for tmp col datum", K(ret), K(batch_size));
    } else {
      datums_ = new (datum_buf) ObDatum[batch_size];
      int64_t datum_ptr_offset = 0;
      for (int64_t i = 0; i < batch_size; ++i) {
        datums_[i].ptr_ = data_buf_ + datum_ptr_offset;
        datum_ptr_offset += common::OBJ_DATUM_NUMBER_RES_SIZE;
      }
      datums = datums_;
    }
  }
  return ret;
}

}
}
