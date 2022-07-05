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

#define USING_LOG_PREFIX SQL_CG
#include "ob_expr_generator_impl.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/expr/ob_expr_regexp.h"
#include "sql/engine/expr/ob_expr_in.h"
#include "sql/engine/expr/ob_expr_like.h"
#include "sql/engine/expr/ob_expr_field.h"
#include "sql/engine/expr/ob_expr_strcmp.h"
#include "sql/engine/expr/ob_expr_abs.h"
#include "sql/engine/expr/ob_expr_arg_case.h"
#include "sql/engine/expr/ob_expr_oracle_decode.h"
#include "sql/engine/expr/ob_expr_to_type.h"
#include "sql/engine/expr/ob_iter_expr_index_scan.h"
#include "sql/engine/expr/ob_iter_expr_set_operation.h"
#include "sql/engine/expr/ob_expr_random.h"
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_rownum.h"
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/expr/ob_expr_nullif.h"
#include "sql/engine/expr/ob_expr_cast.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObExprGeneratorImpl::ObExprGeneratorImpl(
    int16_t cur_regexp_op_count, int16_t cur_like_op_count, uint32_t* next_expr_id, ObColumnIndexProvider& idx_provider)
    : sql_expr_(NULL),
      cur_regexp_op_count_(cur_regexp_op_count),
      cur_like_op_count_(cur_like_op_count),
      column_idx_provider_(idx_provider),
      inner_alloc_(),
      inner_factory_(inner_alloc_),
      factory_(inner_factory_)
{
  factory_.set_next_expr_id(next_expr_id);
}

ObExprGeneratorImpl::ObExprGeneratorImpl(ObExprOperatorFactory& factory, int16_t cur_regexp_op_count,
    int16_t cur_like_op_count, uint32_t* next_expr_id, ObColumnIndexProvider& idx_provider)
    : sql_expr_(NULL),
      cur_regexp_op_count_(cur_regexp_op_count),
      cur_like_op_count_(cur_like_op_count),
      column_idx_provider_(idx_provider),
      inner_alloc_(),
      inner_factory_(inner_alloc_),
      factory_(factory)
{
  factory_.set_next_expr_id(next_expr_id);
}

ObExprGeneratorImpl::~ObExprGeneratorImpl()
{}

int ObExprGeneratorImpl::generate(ObRawExpr& raw_expr, ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;

  sql_expr_ = &expr;
  int64_t count = 0;
  if (OB_FAIL(ObRawExprUtils::get_item_count(&raw_expr, count))) {
    OB_LOG(WARN, "fail to get raw expr_count", K(ret));
  } else if (OB_FAIL(expr.set_item_count(count))) {
    OB_LOG(WARN, "fail to init item count", K(ret), K(count));
  } else if (ObSqlExpressionUtil::should_gen_postfix_expr() && OB_FAIL(raw_expr.postorder_accept(*this))) {
    LOG_WARN("failed to postorder accept", K(ret), K(raw_expr));
  } else if (OB_FAIL(generate_infix_expr(raw_expr))) {
    LOG_WARN("failed to generate infix expr", K(ret));
  } else if (OB_FAIL(expr.generate_idx_for_regexp_ops(cur_regexp_op_count_))) {
    LOG_WARN("failed to generate_idx_for_regexp_ops", K(ret));
  } else if (OB_FAIL(gen_fast_expr(raw_expr))) {
    LOG_WARN("gen fast expr failed", K(ret));
  }

  return ret;
}

int ObExprGeneratorImpl::generate(ObRawExpr& raw_expr, ObIterExprOperator*& out_expr)
{
  int ret = OB_SUCCESS;
  if (!raw_expr.is_domain_index_func()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr is unexpected", K(raw_expr));
  } else if (OB_FAIL(raw_expr.postorder_accept(*this))) {
    LOG_WARN("failed to postorder accept", K(ret), K(raw_expr));
  } else if (iter_expr_desc_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter_expr_desc count is unexptected", K(iter_expr_desc_.count()));
  } else {
    out_expr = iter_expr_desc_.at(0);
  }

  return ret;
}

int ObExprGeneratorImpl::generate_infix_expr(ObRawExpr& raw_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 64> visited_exprs;
    sql_expr_->start_gen_infix_exr();
    auto& exprs = sql_expr_->get_infix_expr().get_exprs();
    if (OB_FAIL(raw_expr.do_visit(*this))) {
      LOG_WARN("expr visit failed", K(ret), K(raw_expr));
    } else if (exprs.count() > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should add one expr per visit", K(ret), K(raw_expr));
    } else if (1 == exprs.count()) {
      if (OB_FAIL(visited_exprs.push_back(&raw_expr))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_child_infix_expr(raw_expr, exprs.count() - 1, visited_exprs))) {
      LOG_WARN("add child infix expr failed", K(ret), K(raw_expr));
    }
  }

  if (OB_SUCC(ret)) {
    auto& exprs = sql_expr_->get_infix_expr().get_exprs();
    if (exprs.count() > 1 && T_REF_COLUMN == exprs.at(0).get_item_type()) {
      bool is_all_ref = true;
      FOREACH_CNT_X(e, exprs, is_all_ref)
      {
        if (T_REF_COLUMN != e->get_item_type()) {
          is_all_ref = false;
        }
      }
      if (!is_all_ref) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unreasonable expression generated, must be bug", K(raw_expr), K(exprs), K(lbt()));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObExprGeneratorImpl::add_child_infix_expr(
    ObRawExpr& raw_expr, const int64_t item_pos, ObIArray<ObRawExpr*>& visited_exprs)
{
  int ret = OB_SUCCESS;
  OZ(check_stack_overflow());
  // %item_pos may be negative: expr item not generated by parent.
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sql_expr_) || item_pos >= sql_expr_->get_infix_expr().get_exprs().count() ||
             visited_exprs.count() != sql_expr_->get_infix_expr().get_exprs().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item_pos), K(visited_exprs.count()));
  } else {
    auto& exprs = sql_expr_->get_infix_expr().get_exprs();
    const int64_t start_pos = exprs.count();
    if (OB_FAIL(infix_visit_child(raw_expr, visited_exprs))) {
      LOG_WARN("visit child of raw expr failed", K(ret), K(raw_expr));
    } else if (exprs.count() != visited_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr count not equal to visited raw expr count", K(ret));
    } else {
      const int64_t end_pos = exprs.count();
      if (item_pos >= 0) {
        auto& item = exprs.at(item_pos);
        item.set_param_idx(static_cast<uint16_t>(start_pos));
        item.set_param_num(static_cast<uint16_t>(end_pos - start_pos));
      }

      for (int64_t i = start_pos; i < end_pos && OB_SUCC(ret); i++) {
        ObRawExpr* e = visited_exprs.at(i);
        if (OB_ISNULL(e)) {
          LOG_WARN("NULL raw expr pointer", K(ret));
        } else if (OB_FAIL(add_child_infix_expr(*e, i, visited_exprs))) {
          LOG_WARN("add child infix expr failed", K(ret), K(*e));
        }
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::infix_visit_child(ObRawExpr& raw_expr, ObIArray<ObRawExpr*>& visited_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    auto& exprs = sql_expr_->get_infix_expr().get_exprs();

    int64_t param_cnt = raw_expr.get_param_count();
    if (raw_expr.skip_visit_child() || skip_child(raw_expr)) {
      param_cnt = 0;
    }
    ObRawExpr* ref_expr = NULL;
    if (raw_expr.is_column_ref_expr()) {
      // for generated column, we should visit the dependant expr
      ObColumnRefRawExpr* col_ref = static_cast<ObColumnRefRawExpr*>(&raw_expr);
      if (col_ref->is_generated_column() && NULL != col_ref->get_dependant_expr()) {
        int64_t idx = OB_INVALID_INDEX;
        if (OB_FAIL(column_idx_provider_.get_idx(col_ref, idx))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            idx = OB_INVALID_INDEX;
          } else {
            LOG_WARN("get index failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (col_ref->has_flag(IS_COLUMNLIZED) && OB_INVALID_INDEX != idx) {
            // do nothing for columnlized col ref
          } else {
            ref_expr = col_ref->get_dependant_expr();
          }
        }
      }
    }
    // visit the ref expr or child expr
    for (int64_t i = 0; (i < param_cnt || NULL != ref_expr) && OB_SUCC(ret); i++) {
      const int64_t cnt_bak = exprs.count();
      ObRawExpr* e = NULL != ref_expr ? ref_expr : raw_expr.get_param_expr(i);
      if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr returned", K(ret));
      } else if (OB_FAIL(e->do_visit(*this))) {
        LOG_WARN("expr visit failed", K(ret), K(*e));
      } else {
        const int64_t new_expr_cnt = exprs.count() - cnt_bak;
        if (new_expr_cnt > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should add one expr per visit", K(ret), K(*e));
        } else if (1 == new_expr_cnt) {
          if (OB_FAIL(visited_exprs.push_back(e))) {
            LOG_WARN("array push back failed", K(ret));
          }
        } else {
          // Some raw expr do not generate infix expr item, pull it's child here.
          // e.g.: T_OP_ROW, T_FUN_COUNT
          if (OB_FAIL(infix_visit_child(*e, visited_exprs))) {
            LOG_WARN("visit child of raw expr failed", K(ret), K(*e));
          }
        }
      }
      if (NULL != ref_expr) {
        break;
      }
    }
  }

  return ret;
}

int ObExprGeneratorImpl::visit(ObConstRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  int64_t idx = OB_INVALID_INDEX;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED) && OB_SUCCESS == column_idx_provider_.get_idx(&expr, idx) &&
             OB_INVALID_INDEX != idx) {
    /**
     * Use value in row if possible, %idx is also checked here when got IS_COLUMNLIZED flag.
     * Because the const expr may be shared, see:
     *
        select * from t1 where t1.a in (select t1.a from t1 as t2 union select t1.a from t1 as t3);
        ==============================================
        |ID|OPERATOR             |NAME|EST. ROWS|COST|
        ----------------------------------------------
        |0 |SUBPLAN FILTER       |    |2        |264 |
        |1 | TABLE SCAN          |t1  |3        |37  |
        |2 | MERGE UNION DISTINCT|    |6        |75  |
        |3 |  TABLE SCAN         |t2  |3        |37  |
        |4 |  TABLE SCAN         |t3  |3        |37  |
        ==============================================

        Outputs & filters:
        -------------------------------------
          0 - output([t1.a], [t1.b], [t1.c]), filter([t1.a = ANY(subquery(1))]),
              exec_params_([t1.a]), onetime_exprs_(nil), init_plan_idxs_(nil)
          1 - output([t1.a], [t1.b], [t1.c]), filter(nil),
              access([t1.a], [t1.b], [t1.c]), partitions(p0)
          2 - output([UNION(?, ?)]), filter(nil)
          3 - output([?]), filter(nil),
              access([t2.__pk_increment]), partitions(p0)
          4 - output([?]), filter(nil),
              access([t3.__pk_increment]), partitions(p0)

      Output of operator 3 and output of operator 4 is the same question mark expr, will be
      marked columnlized in operator 3 CG. In operator 4 CG, the expr is still columnlized
      but get %idx is invalid.
    */

    if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("failed to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret), K(item));
    }
  } else if (OB_FAIL(item.assign(expr.get_value()))) {
    LOG_WARN("failed to assign const value", K(ret));
  } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
    LOG_WARN("failed to add expr item", K(ret));
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObVarRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  int64_t idx = OB_INVALID_INDEX;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (OB_FAIL(item.assign(static_cast<ObItemType>(expr.get_data_type())))) {
    LOG_WARN("failed to assign const value", K(ret));
  } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
    LOG_WARN("failed to add expr item", K(ret));
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObQueryRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("failed to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret), K(item));
    }
  } else {
    ObExprOperator* op = NULL;
    if (OB_FAIL(factory_.alloc(expr.get_expr_type(), op))) {
      LOG_WARN("fail to alloc expr_op", K(ret));
    } else if (OB_UNLIKELY(NULL == op)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc expr operator failed", "expr type", get_type_name(expr.get_expr_type()));
    } else {
      ObExprSubQueryRef* subquery_op = static_cast<ObExprSubQueryRef*>(op);
      bool result_is_scalar = (expr.get_output_column() == 1 && !expr.is_set());
      subquery_op->set_result_is_scalar(result_is_scalar);
      subquery_op->set_result_type(expr.get_result_type());
      if (result_is_scalar) {
        subquery_op->set_scalar_result_type(expr.get_result_type());
      }
      OZ(subquery_op->init_row_desc(expr.get_column_types().count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_column_types().count(); ++i) {
        ObDataType type;
        type.set_meta_type(expr.get_column_types().at(i));
        type.set_accuracy(expr.get_column_types().at(i).get_accuracy());
        OZ(subquery_op->get_row_desc().push_back(type));
      }

      // ref_id in expr means which child is located in ObSubPlanFilter
      if (OB_SUCC(ret)) {
        LOG_DEBUG("convert unary current subquery ref id is", K(expr.get_ref_id()));
        subquery_op->set_subquery_idx(expr.get_ref_id() - 1);
        if (OB_FAIL(item.assign(op))) {
          LOG_WARN("assign sql item failed", K(ret));
        } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
          LOG_WARN("add expr item to sql expr failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObColumnRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(column_idx_provider_.get_idx(&expr, col_idx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      col_idx = OB_INVALID_INDEX;
    } else {
      LOG_WARN("get index failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED) && OB_INVALID_INDEX != col_idx) {
    if (OB_FAIL(item.set_column(col_idx))) {
      LOG_WARN("failed to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret));
    }
  } else if (expr.is_generated_column() && expr.get_dependant_expr() != NULL) {
    if (!sql_expr_->is_gen_infix_expr()) {
      if (OB_FAIL(expr.get_dependant_expr()->postorder_accept(*this))) {
        LOG_WARN("failed to postorder accept", K(ret), KPC(expr.get_dependant_expr()));
      }
    } else {
      // do nothing for infix expr generation, especially processed in infix_visit_child()
    }
  } else if (expr.is_unpivot_mocked_column()) {
    // do nothing
  } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == expr.get_column_id() && expr.get_dependant_expr() != NULL) {
    if (!sql_expr_->is_gen_infix_expr()) {
      if (OB_FAIL(expr.get_dependant_expr()->postorder_accept(*this))) {
        LOG_WARN("failed to postorder accept", K(ret), KPC(expr.get_dependant_expr()));
      }
    } else {
      // do nothing
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("all basic column should have been generated", K(expr), K(&expr), K(col_idx));
  }
  return ret;
}

int ObExprGeneratorImpl::visit_simple_op(ObNonTerminalRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator* op = NULL;
  ObExprOperator* old_op = NULL;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("fail to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret), K(item), K(expr));
    }
  } else if (OB_FAIL(factory_.alloc(expr.get_expr_type(), op))) {
    LOG_WARN("fail to alloc expr_op", K(ret));
  } else if (OB_UNLIKELY(NULL == op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc expr op", K(ret), N_TYPE, expr.get_expr_type());
  } else if (OB_ISNULL(old_op = expr.get_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is null");
  } else {
    op->set_real_param_num(static_cast<int32_t>(expr.get_param_count()));
    op->set_row_dimension(ObExprOperator::NOT_ROW_DIMENSION);
    op->set_result_type(expr.get_result_type());
    // calc part id expr may got T_OP_ROW child, it's meaningless to set its input type.
    if (OB_LIKELY(T_FUN_SYS_CALC_PARTITION_ID != expr.get_expr_type()) &&
        OB_FAIL(op->set_input_types(expr.get_input_types()))) {
      LOG_WARN("fail copy input types", K(ret));
    } else if (OB_FAIL(item.assign(op))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret));
    } else {
      switch (expr.get_expr_type()) {
        case T_FUN_SYS_ORA_DECODE: {
          ObExprOracleDecode* decode_op = static_cast<ObExprOracleDecode*>(op);
          ret = visit_decode_expr(expr, decode_op);
          break;
        }
        case T_FUN_SYS_GREATEST_INNER:
        case T_FUN_SYS_LEAST_INNER:
        case T_FUN_SYS_GREATEST:
        case T_FUN_SYS_LEAST: {
          ObMinMaxExprOperator* minmax_op = static_cast<ObMinMaxExprOperator*>(op);
          ret = visit_minmax_expr(expr, minmax_op);
          break;
        }
        case T_FUN_SYS_FIELD: {
          ObExprField* field = static_cast<ObExprField*>(op);
          ret = visit_field_expr(expr, field);
          break;
        }
        case T_FUN_SYS_STRCMP: {
          ObExprStrcmp* strcmp_op = static_cast<ObExprStrcmp*>(op);
          ret = visit_strcmp_expr(expr, strcmp_op);
          break;
        }
        case T_OP_ABS: {
          ObExprAbs* abs_op = static_cast<ObExprAbs*>(op);
          ret = visit_abs_expr(expr, abs_op);
          break;
        }
        case T_FUN_SYS_TO_TYPE: {
          ObExprToType* to_type = static_cast<ObExprToType*>(op);
          to_type->set_expect_type(expr.get_result_type().get_type());
          break;
        }
        case T_FUN_SYS_RAND: {
          ObExprRandom* rand_op = static_cast<ObExprRandom*>(op);
          ret = visit_random_expr(static_cast<ObOpRawExpr&>(expr), rand_op);
          break;
        }
        case T_FUN_COLUMN_CONV: {
          ObExprColumnConv* column_conv_op = static_cast<ObExprColumnConv*>(op);
          ret = visit_column_conv_expr(expr, column_conv_op);
          break;
        }
        case T_FUN_ENUM_TO_STR:
        case T_FUN_ENUM_TO_INNER_TYPE:
        case T_FUN_SET_TO_STR:
        case T_FUN_SET_TO_INNER_TYPE: {
          ObExprTypeToStr* enum_set_op = static_cast<ObExprTypeToStr*>(op);
          ret = visit_enum_set_expr(expr, enum_set_op);
          break;
        }
        case T_FUN_UDF: {
          ret = OB_NOT_SUPPORTED;
          break;
        }
        case T_FUN_SYS_INTERVAL: {
          ObExprInterval* enum_set_op = static_cast<ObExprInterval*>(op);
          ret = visit_fun_interval(expr, enum_set_op);
          break;
        }
        case T_FUN_NORMAL_UDF: {
          ObExprDllUdf* normal_udf_op = static_cast<ObExprDllUdf*>(op);
          ret = visit_normal_udf_expr(expr, normal_udf_op);
          break;
        }
        case T_FUN_SYS_ROWNUM: {
          ObExprRowNum* rownum_op = static_cast<ObExprRowNum*>(op);
          rownum_op->set_op_id(static_cast<ObSysFunRawExpr&>(expr).get_op_id());
          break;
        }
        case T_FUN_SYS_NULLIF: {
          ObExprNullif* nullif_expr = static_cast<ObExprNullif*>(op);
          ret = visit_nullif_expr(expr, nullif_expr);
          break;
        }
        case T_FUN_SYS_CAST: {
          ObExprCast* cast_op = static_cast<ObExprCast*>(op);
          const bool is_implicit = expr.has_flag(IS_INNER_ADDED_EXPR);
          cast_op->set_implicit_cast(is_implicit);
          LOG_DEBUG("cast debug, explicit or implicit", K(ret), K(is_implicit));
          break;
        }
        default: {
          break;
        }
      }  // end of switch
    }
  }
  return ret;
}

inline int ObExprGeneratorImpl::visit_regex_expr(ObOpRawExpr& expr, ObExprRegexp*& regexp_op)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*>& param_exprs = expr.get_param_exprs();
  if (OB_ISNULL(regexp_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("regexpr_op is NULL");
  } else if (OB_UNLIKELY(2 != param_exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("regex op should have 2 arguments", K(param_exprs.count()));
  } else {
    const ObRawExpr* value_expr = param_exprs.at(0);
    const ObRawExpr* pattern_expr = param_exprs.at(1);
    if (OB_ISNULL(value_expr) || OB_ISNULL(pattern_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer");
    } else {
      // The value && pattern are const, it is calculated in pre_calculate
      regexp_op->set_value_is_const(value_expr->has_flag(IS_CONST) || value_expr->has_flag(IS_CONST_EXPR));
      regexp_op->set_pattern_is_const(pattern_expr->has_flag(IS_CONST) || pattern_expr->has_flag(IS_CONST_EXPR));
    }
  }
  return ret;
}

inline int ObExprGeneratorImpl::visit_like_expr(ObOpRawExpr& expr, ObExprLike*& like_op)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*>& param_exprs = expr.get_param_exprs();
  if (OB_ISNULL(like_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("like_op is NULL");
  } else if (OB_UNLIKELY(3 != param_exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("like op should have 3 arguments", K(param_exprs.count()));
  } else {
    const ObRawExpr* text_expr = param_exprs.at(0);
    const ObRawExpr* pattern_expr = param_exprs.at(1);
    const ObRawExpr* escape_expr = param_exprs.at(2);
    if (OB_ISNULL(pattern_expr) || OB_ISNULL(text_expr) || OB_ISNULL(escape_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer", K(pattern_expr), K(text_expr), K(escape_expr));
    } else {
      like_op->set_text_is_literal(!text_expr->has_flag(IS_EXEC_PARAM) && !text_expr->has_flag(CNT_EXEC_PARAM) &&
                                   (text_expr->has_flag(IS_CONST) || text_expr->has_flag(IS_CONST_EXPR)));
      like_op->set_pattern_is_literal(!pattern_expr->has_flag(IS_EXEC_PARAM) &&
                                      !pattern_expr->has_flag(CNT_EXEC_PARAM) &&
                                      (pattern_expr->has_flag(IS_CONST) || pattern_expr->has_flag(IS_CONST_EXPR)));
      like_op->set_escape_is_literal(!escape_expr->has_flag(IS_EXEC_PARAM) && !escape_expr->has_flag(CNT_EXEC_PARAM) &&
                                     (escape_expr->has_flag(IS_CONST) || escape_expr->has_flag(IS_CONST_EXPR)));
      if (!is_oracle_mode() && !like_op->is_escape_literal()) {
        ret = INCORRECT_ARGUMENTS_TO_ESCAPE;
        LOG_WARN("escape argument of like expr in mysql mode must be const", K(ret), K(*escape_expr));
      }
    }
  }
  return ret;
}

inline int ObExprGeneratorImpl::visit_in_expr(ObOpRawExpr& expr, ObExprInOrNotIn*& in_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != expr.get_param_exprs().count() || OB_ISNULL(expr.get_param_expr(0)) ||
                  OB_ISNULL(expr.get_param_expr(1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected", K(expr.get_param_count()), K(expr.get_param_expr(0)), K(expr.get_param_expr(1)));
  } else if (T_OP_ROW != expr.get_param_expr(1)->get_expr_type()) {
    // in subquery -> =ANY(subquery) has been rewrite in resolver
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in subquery has been rewrite in  resolver", K(expr));
  } else if (T_REF_QUERY == expr.get_param_expr(0)->get_expr_type()
             // output column == 1 The subplan filter is responsible for iterating the data
             && expr.get_param_expr(0)->get_output_column() > 1) {
    // like (select 1, 2) in ((1,2), (3,4))
    ObQueryRefRawExpr* left_ref = static_cast<ObQueryRefRawExpr*>(expr.get_param_expr(0));
    int64_t column_count = left_ref->get_output_column();
    in_op->set_row_dimension(column_count);
    in_op->set_real_param_num(1 + expr.get_param_expr(1)->get_param_count());
    in_op->set_param_is_subquery();
  } else if (T_OP_ROW == expr.get_param_expr(0)->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_expr(1)->get_param_count(); ++i) {
      if (T_OP_ROW != expr.get_param_expr(1)->get_param_expr(i)->get_expr_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the right expr should be row of row", K(i), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // like (a,b) in ((1, 2), (3, 4))
      in_op->set_row_dimension(static_cast<int32_t>(expr.get_param_expr(0)->get_param_count()));
      in_op->set_real_param_num(static_cast<int32_t>(1 + expr.get_param_expr(1)->get_param_count()));
      // we need some extra work for performance, but with strict conditions:
      // all params after "in" / "not in" are all const, all same type, and all same cs type.
      // all const null params will not be concerned.
      ObRawExpr* param1 = expr.get_param_expr(1);
      int64_t param_count = param1->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < in_op->get_row_dimension(); ++j) {
          if (OB_ISNULL(param1->get_param_expr(i)->get_param_expr(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param expr is null", K(i), K(j), K(ret));
          }
        }
      }
      // for row_type in left_param of EXPR IN
      // if min_cluster_version < 3.1, do not check params can use hash optimizition
      if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) {
        bool param_all_const = true;
        bool param_all_same_type = true;
        bool param_all_same_cs_type = true;
        bool param_all_is_ext = true;
        bool param_all_same_cs_level = true;
        for (int64_t j = 0; OB_SUCC(ret) && j < in_op->get_row_dimension(); ++j) {
          param_all_const &= (param1->get_param_expr(0)->get_param_expr(j)->has_const_or_const_expr_flag()
                              && !param1->get_param_expr(0)->get_param_expr(j)
                                                           ->has_flag(IS_EXEC_PARAM));
          ObObjType first_obj_type = param1->get_param_expr(0)->get_param_expr(j)->get_data_type();
          ObObjType cur_obj_type = ObMaxType;
          ObCollationType first_obj_cs_type = param1->get_param_expr(0)->get_param_expr(j)->get_collation_type();
          ObCollationType cur_obj_cs_type = CS_TYPE_INVALID;
          ObCollationLevel first_obj_cs_level = param1->get_param_expr(0)->get_param_expr(j)->get_collation_level();
          ObCollationLevel cur_obj_cs_level = CS_LEVEL_INVALID;
          param_all_is_ext &= (ObExtendType == first_obj_type);
          for (int64_t i = 1; OB_SUCC(ret) && i < param_count; ++i) {
            cur_obj_type = param1->get_param_expr(i)->get_param_expr(j)->get_data_type();
            cur_obj_cs_type = param1->get_param_expr(i)->get_param_expr(j)->get_collation_type();
            cur_obj_cs_level = param1->get_param_expr(i)->get_param_expr(j)->get_collation_level();
            if (ObNullType == first_obj_type) {
              first_obj_type = cur_obj_type;
              first_obj_cs_type = cur_obj_cs_type;
            }
            if (ObNullType != first_obj_type && ObNullType != cur_obj_type) {
              param_all_const &= (param1->get_param_expr(i)->get_param_expr(j)->has_const_or_const_expr_flag()
                                 && !param1->get_param_expr(i)->get_param_expr(j)
                                                            ->has_flag(IS_EXEC_PARAM));
              param_all_same_type &= (first_obj_type == cur_obj_type);
              param_all_same_cs_type &= (first_obj_cs_type == cur_obj_cs_type);
              param_all_same_cs_level &= (first_obj_cs_level == cur_obj_cs_level);
            }
            param_all_is_ext &= (ObExtendType == first_obj_type);
          }
        }
        in_op->set_param_all_const(param_all_const);
        in_op->set_param_all_same_type(param_all_same_type);
        in_op->set_param_all_same_cs_type(
            share::is_oracle_mode() ? param_all_same_cs_type : (param_all_same_cs_type &= param_all_same_cs_level));
        in_op->set_param_is_ext_type_oracle(param_all_is_ext);
      }
    }
  } else {
    // like a in (1, 2, 3)
    in_op->set_row_dimension(1);
    in_op->set_real_param_num(static_cast<int32_t>(1 + expr.get_param_expr(1)->get_param_count()));
    // we need some extra work for performance, but with strict conditions:
    // all params after "in" / "not in" are all const, all same type, and all same cs type.
    // all const null params will not be concerned.
    ObRawExpr* param1 = expr.get_param_expr(1);
    int64_t param_count = param1->get_param_count();
    for (int i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      if (OB_ISNULL(param1->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(i), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool param_all_const = (param1->get_param_expr(0)->has_const_or_const_expr_flag()
                             && !param1->get_param_expr(0)->has_flag(IS_EXEC_PARAM));
      bool param_all_same_type = true;
      bool param_all_same_cs_type = true;
      bool param_all_is_ext = true;
      bool param_all_same_cs_level = true;
      ObObjType first_obj_type = param1->get_param_expr(0)->get_data_type();
      ObObjType cur_obj_type = ObMaxType;
      ObCollationType first_obj_cs_type = param1->get_param_expr(0)->get_collation_type();
      ObCollationType cur_obj_cs_type = CS_TYPE_INVALID;
      ObCollationLevel first_obj_cs_level = param1->get_param_expr(0)->get_collation_level();
      ObCollationLevel cur_obj_cs_level = CS_LEVEL_INVALID;
      param_all_is_ext &= (ObExtendType == first_obj_type);
      for (int i = 1; OB_SUCC(ret) && i < param_count; ++i) {
        cur_obj_type = param1->get_param_expr(i)->get_data_type();
        cur_obj_cs_type = param1->get_param_expr(i)->get_collation_type();
        cur_obj_cs_level = param1->get_param_expr(i)->get_collation_level();
        if (ObNullType == first_obj_type) {
          first_obj_type = cur_obj_type;
          first_obj_cs_type = cur_obj_cs_type;
        }
        if (ObNullType != first_obj_type && ObNullType != cur_obj_type) {
          param_all_const &= (param1->get_param_expr(i)->has_const_or_const_expr_flag()
                             && !param1->get_param_expr(i)->has_flag(IS_EXEC_PARAM));
          param_all_same_type &= (first_obj_type == cur_obj_type);
          param_all_same_cs_type &= (first_obj_cs_type == cur_obj_cs_type);
          param_all_same_cs_level &= (first_obj_cs_level == cur_obj_cs_level);
        }
        param_all_is_ext &= (ObExtendType == first_obj_type);
      }
      in_op->set_param_all_const(param_all_const);
      in_op->set_param_all_same_type(param_all_same_type);
      in_op->set_param_all_same_cs_type(
          share::is_oracle_mode() ? param_all_same_cs_type : (param_all_same_cs_type &= param_all_same_cs_level));
      in_op->set_param_is_ext_type_oracle(param_all_is_ext);
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_decode_expr(ObNonTerminalRawExpr& expr, ObExprOracleDecode* decode_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(decode_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decode expr op is null", K(ret));
  } else {
    // decode(col, cond1, val1, cond2, val2, ......, condN, valN, def_val)
    // cmp type of decode is always equal to cond1, or varchar if cond1 is const null.
    // res type of decode is always euqal to val1, or varchar if val1 is const null.
    bool cond_all_same_meta = true;
    bool val_all_same_meta = true;
    int64_t param_count = expr.get_param_count();
    for (int i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      if (OB_ISNULL(expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("decode expr param is null", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObObjType cond_type = expr.get_param_expr(0)->get_data_type();
      ObObjType val_type = expr.get_param_expr(param_count - 1)->get_data_type();
      ObCollationType cond_cs_type = expr.get_param_expr(0)->get_collation_type();
      ObCollationType val_cs_type = expr.get_param_expr(param_count - 1)->get_collation_type();
      for (int i = 1; OB_SUCC(ret) && i < param_count - 1; i += 2) {
        cond_all_same_meta = cond_all_same_meta &&
                             decode_op->can_compare_directly(cond_type, expr.get_param_expr(i)->get_data_type()) &&
                             (cond_cs_type == expr.get_param_expr(i)->get_collation_type());
        val_all_same_meta = val_all_same_meta && (val_type == expr.get_param_expr(i + 1)->get_data_type()) &&
                            (val_cs_type == expr.get_param_expr(i + 1)->get_collation_type());
      }
      decode_op->set_cond_all_same_meta(cond_all_same_meta);
      decode_op->set_val_all_same_meta(val_all_same_meta);
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_minmax_expr(ObNonTerminalRawExpr& expr, ObMinMaxExprOperator* minmax_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(minmax_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minmax expr op is null", K(ret));
  } else {
    bool need_cast = true;
    if (OB_FAIL(set_need_cast(expr, need_cast))) {
      LOG_WARN("get cast info failed", K(ret), K(expr));
    } else {
      minmax_op->set_need_cast(need_cast);
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_field_expr(ObNonTerminalRawExpr& expr, ObExprField* field_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(field_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("field expr op is null", K(ret));
  } else {
    bool need_cast = false;
    if (OB_FAIL(set_need_cast(expr, need_cast))) {
      LOG_WARN("get cast info failed", K(ret), K(expr));
    } else {
      field_op->set_need_cast(need_cast);
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_strcmp_expr(ObNonTerminalRawExpr& expr, ObExprStrcmp* strcmp_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(strcmp_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("strcmp_op expr op is null", K(ret));
  } else if (strcmp_op->set_cmp_func(ObVarcharType, ObVarcharType)) {
    LOG_WARN("set cmp func failed", K(ret), K(expr), K(*strcmp_op));
  }
  return ret;
}

int ObExprGeneratorImpl::visit_abs_expr(ObNonTerminalRawExpr& expr, ObExprAbs* abs_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(abs_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("abs_op expr op is null", K(ret));
  } else if (expr.get_param_count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. invalid argument count", K(ret), K(expr.get_param_count()));
  } else if (OB_ISNULL(expr.get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. param expr is null", K(ret), K(expr));
  } else {
    ret = abs_op->set_func(expr.get_param_expr(0)->get_data_type());
  }
  return ret;
}

int ObExprGeneratorImpl::visit_column_conv_expr(ObRawExpr& expr, ObBaseExprColumnConv* column_conv_op)
{
  int ret = OB_SUCCESS;
  ObExprOperator* old_op = NULL;
  ObExprColumnConv* column_conv_old = NULL;
  ObSysFunRawExpr& raw_column_conv_expr = static_cast<ObSysFunRawExpr&>(expr);
  if (OB_ISNULL(column_conv_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_conv_op is null", K(ret));
  } else if (OB_ISNULL(old_op = raw_column_conv_expr.get_op()) ||
             OB_UNLIKELY(T_FUN_COLUMN_CONV != (old_op->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid old op", K(raw_column_conv_expr), K(ret));
  } else if (OB_ISNULL(column_conv_old = static_cast<ObExprColumnConv*>(old_op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to static cast ObExprOperator * to ObExprColumnConv *", K(raw_column_conv_expr), K(ret));
  } else if (column_conv_old->get_str_values().count() > 0 &&
             OB_FAIL(column_conv_op->deep_copy_str_values(column_conv_old->get_str_values()))) {
    LOG_WARN("failed to deep_copy_str_values", K(raw_column_conv_expr), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

inline int ObExprGeneratorImpl::visit_fun_interval(ObNonTerminalRawExpr& expr, ObExprInterval* fun_interval)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fun_interval)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fun_interval expr op is null", K(ret));
  } else {
    static const int64_t MYSQL_BINARY_SEARCH_BOUND = 8;
    const int64_t num = expr.get_param_count();
    if (num < 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. invalid argument count", K(ret), K(expr.get_param_count()));
    } else {
      // checking input parameters:
      // if the number of input parameters is more then 8 and all the parameters are const and not null, we will do a
      // binary search during calc otherwise, use sequential search
      bool use_binary_search = true;
      if (num > MYSQL_BINARY_SEARCH_BOUND) {
        for (int64_t i = 0; i < num; i++) {
          LOG_DEBUG("visit interval ",
              K(expr.get_param_expr(i)->has_const_or_const_expr_flag()),
              K(expr.get_param_expr(i)->is_not_null()));
          if (!expr.get_param_expr(i)->has_const_or_const_expr_flag() || !expr.get_param_expr(i)->is_not_null()) {
            use_binary_search = false;
            break;
          }
        }
      } else {
        use_binary_search = false;
      }
      fun_interval->set_use_binary_search(use_binary_search);
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_enum_set_expr(ObNonTerminalRawExpr& expr, ObExprTypeToStr* enum_set_op)
{
  int ret = OB_SUCCESS;
  ObExprOperator* old_op = NULL;
  if (OB_ISNULL(enum_set_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("enum_set_op is null", K(ret));
  } else if (OB_ISNULL(old_op = expr.get_op()) || OB_UNLIKELY(!IS_ENUM_SET_OP(old_op->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid old op", K(expr), K(ret));
  } else {
    ObExprTypeToStr* type_to_str = static_cast<ObExprTypeToStr*>(old_op);
    if (OB_FAIL(enum_set_op->deep_copy_str_values(type_to_str->get_str_values()))) {
      LOG_WARN("failed to deep_copy_str_values", K(expr), K(ret));
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_normal_udf_expr(ObNonTerminalRawExpr& expr, ObExprDllUdf* normal_udf_op)
{
  int ret = OB_SUCCESS;
  ObNormalDllUdfRawExpr& fun_sys = static_cast<ObNormalDllUdfRawExpr&>(expr);
  // used to check the old op exist or not
  ObExprOperator* old_op = NULL;
  if (OB_ISNULL(normal_udf_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("enum_set_op is null", K(ret));
  } else if (OB_ISNULL(old_op = expr.get_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid old op", K(expr), K(ret));
  } else if (OB_FAIL(normal_udf_op->set_udf_meta(fun_sys.get_udf_meta()))) {
    LOG_WARN("failed to set udf to expr", K(ret));
  } else if (OB_FAIL(normal_udf_op->init_udf(fun_sys.get_param_exprs()))) {
    LOG_WARN("failed to init udf", K(ret));
  } else {
    LOG_DEBUG("set udf meta to expr", K(fun_sys.get_udf_meta()));
  }
  return ret;
}

int ObExprGeneratorImpl::visit_nullif_expr(ObNonTerminalRawExpr& expr, ObExprNullif*& nullif_expr)
{
  // for oracle compat
  int ret = OB_SUCCESS;
  if (expr.get_param_expr(0)->is_const_expr()) {
    ObConstRawExpr* c_expr = static_cast<ObConstRawExpr*>(expr.get_param_expr(0));
    ObObjType type0 = c_expr->get_expr_obj_meta().get_type();
    if (ObNullType == type0) {
      nullif_expr->set_first_param_flag(false);
    }
  } else if (expr.get_param_expr(0)->has_const_or_const_expr_flag()) {
    nullif_expr->set_first_param_flag(false);
  } else {
  }
  return ret;
}

int ObExprGeneratorImpl::set_need_cast(ObNonTerminalRawExpr& expr, bool& need_cast)
{
  int ret = OB_SUCCESS;
  bool all_are_numeric = true;
  bool all_same_type = true;
  int64_t param_count = expr.get_param_count();
  need_cast = true;
  for (int i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    if (OB_ISNULL(expr.get_param_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("minmax expr param is null", K(ret), K(i));
    } else {
      ObObjType type = expr.get_param_expr(i)->get_data_type();
      const ObObjMeta& cur_meta = expr.get_param_expr(i)->get_result_type().get_obj_meta();
      const ObObjMeta& first_meta = expr.get_param_expr(0)->get_result_type().get_obj_meta();
      // no need to test expr.get_param_expr(0) for NULL here.
      if (all_same_type && (!(first_meta == cur_meta) || ob_is_enumset_tc(type))) {
        all_same_type = false;
      }
      if (all_are_numeric && (!ob_is_accurate_numeric_type(type))) {
        all_are_numeric = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    need_cast = !(all_same_type || all_are_numeric);
  }
  return ret;
}

int ObExprGeneratorImpl::visit_relational_expr(ObNonTerminalRawExpr& expr, ObRelationalExprOperator* relational_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(relational_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relational expr op is null", K(ret));
  } else if (expr.get_param_count() == 2) {
    int64_t param_count = expr.get_param_count();
    int64_t input_types_count = expr.get_input_types().count();
    if (OB_UNLIKELY(param_count != input_types_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param count should be equal to input_types_count", K(param_count), K(input_types_count), K(ret));
    } else {
      /*
       * loop of size 2. Seems reasonable  to unroll it to get a better perf.
       *
       * But, well, do not bother yourself. It is up to gcc.
       */
      for (int i = 0; OB_SUCC(ret) && i < param_count; ++i) {
        if (OB_ISNULL(expr.get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("relational expr param is null", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        ObObjType left_operand_type = expr.get_input_types().at(0).get_calc_type();
        ObObjType right_operand_type = expr.get_input_types().at(1).get_calc_type();
        ret = relational_op->set_cmp_func(left_operand_type, right_operand_type);
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit_argcase_expr(ObNonTerminalRawExpr& expr, ObExprArgCase* argcase_op)
{
  int ret = OB_SUCCESS;
  const ObCaseOpRawExpr& argcase_expr = static_cast<ObCaseOpRawExpr&>(expr);
  if (OB_ISNULL(argcase_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argcase expr op is null", K(ret));
  } else if (argcase_expr.get_param_count() < 2 || OB_ISNULL(argcase_expr.get_arg_param_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. invalid params", K(argcase_expr.get_param_count()), K(ret));
  } else {
    const ObObjMeta& case_meta = argcase_expr.get_arg_param_expr()->get_result_type();
    bool need_cast = true;
    bool all_are_numeric = ob_is_accurate_numeric_type(case_meta.get_type());
    bool all_same_type = true;
    int64_t loop = argcase_expr.get_when_expr_size();
    for (int i = 0; OB_SUCC(ret) && i < loop; i++) {
      if (OB_ISNULL(argcase_expr.get_when_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("argcase expr param is null", K(ret), K(i));
      } else {
        const ObObjMeta& param_meta = argcase_expr.get_when_param_expr(i)->get_result_type();
        if (all_same_type && !ObSQLUtils::is_same_type_for_compare(case_meta, param_meta)) {
          all_same_type = false;
        }
        if (all_are_numeric && (!ob_is_accurate_numeric_type(param_meta.get_type()))) {
          all_are_numeric = false;
        }
      }
    }  // end for
    if (OB_SUCC(ret)) {
      need_cast = !(all_same_type || all_are_numeric);
      argcase_op->set_need_cast(need_cast);
    }
  }
  return ret;
}

// (a,b,c) binary_op (x, y, z)
inline int ObExprGeneratorImpl::visit_maybe_row_expr(ObOpRawExpr& expr, ObExprOperator*& op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != expr.get_param_exprs().count() || OB_ISNULL(expr.get_param_expr(0)) ||
                  OB_ISNULL(expr.get_param_expr(1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected", K(expr.get_param_count()), K(expr.get_param_expr(0)), K(expr.get_param_expr(1)));
  } else if (T_OP_ROW == expr.get_param_expr(0)->get_expr_type() ||
             T_OP_ROW == expr.get_param_expr(1)->get_expr_type()) {
    if (T_OP_ROW != expr.get_param_expr(0)->get_expr_type() || T_OP_ROW != expr.get_param_expr(1)->get_expr_type() ||
        expr.get_param_expr(0)->get_param_count() != expr.get_param_expr(1)->get_param_count()) {
      ObExprOperatorType op_type = expr.get_expr_type();
      if (lib::is_oracle_mode() && (T_OP_EQ == op_type || T_OP_NE == op_type || T_OP_IN == op_type) &&
          T_OP_ROW == expr.get_param_expr(0)->get_expr_type() && 0 < expr.get_param_expr(0)->get_param_count() &&
          T_OP_ROW != expr.get_param_expr(0)->get_param_expr(0)->get_expr_type() &&
          T_OP_ROW == expr.get_param_expr(1)->get_expr_type() &&
          expr.get_param_expr(0)->get_param_count() == expr.get_param_expr(1)->get_param_expr(0)->get_param_count()) {
        op->set_real_param_num(2);
        op->set_row_dimension(static_cast<int32_t>(expr.get_param_expr(0)->get_param_count()));
      } else {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
      }
    } else {
      op->set_real_param_num(2);
      op->set_row_dimension(static_cast<int32_t>(expr.get_param_expr(0)->get_param_count()));
    }
  } else {
    op->set_real_param_num(static_cast<int32_t>(expr.get_param_count()));
    op->set_row_dimension(ObExprOperator::NOT_ROW_DIMENSION);
    ObRelationalExprOperator* relational_op = static_cast<ObRelationalExprOperator*>(op);
    ret = visit_relational_expr(expr, relational_op);
  }
  return ret;
}

inline int ObExprGeneratorImpl::visit_subquery_cmp_expr(ObOpRawExpr& expr, ObSubQueryRelationalExpr*& subquery_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != expr.get_param_count() || !expr.has_flag(CNT_SUB_QUERY) || OB_ISNULL(expr.get_param_expr(0)) ||
                  OB_ISNULL(expr.get_param_expr(1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected",
        K(expr.get_param_count()),
        K(expr.get_param_expr(0)),
        K(expr.get_param_expr(1)),
        K(expr.has_flag(CNT_SUB_QUERY)));
  } else {
    subquery_op->set_subquery_key(expr.get_subquery_key());

    if (T_OP_ROW == expr.get_param_expr(0)->get_expr_type()) {
      // OB_ASSERT(expr.get_param_expr(1)->has_flag(IS_SUB_QUERY));
      subquery_op->set_real_param_num(static_cast<int32_t>(expr.get_param_expr(0)->get_param_count()) + 1);
      subquery_op->set_left_is_iter(false);
      subquery_op->set_right_is_iter(true);
    } else if (expr.get_param_expr(0)->has_flag(IS_SUB_QUERY) &&
               static_cast<ObQueryRefRawExpr*>(expr.get_param_expr(0))->get_output_column() > 1) {
      if (T_OP_ROW == expr.get_param_expr(1)->get_expr_type()) {
        subquery_op->set_real_param_num(static_cast<int32_t>(expr.get_param_expr(1)->get_param_count()) + 1);
        subquery_op->set_left_is_iter(true);
        subquery_op->set_right_is_iter(false);
      } else {
        // OB_ASSERT(expr.get_param_expr(1)->has_flag(IS_SUB_QUERY));
        subquery_op->set_real_param_num(static_cast<int32_t>(expr.get_param_count()));
        subquery_op->set_left_is_iter(true);
        subquery_op->set_right_is_iter(true);
      }
    } else {
      // OB_ASSERT(expr.get_param_expr(1)->has_flag(IS_SUB_QUERY));
      subquery_op->set_real_param_num(static_cast<int32_t>(expr.get_param_count()));
      subquery_op->set_left_is_iter(false);
      subquery_op->set_right_is_iter(true);
    }
  }
  return ret;
}

inline int ObExprGeneratorImpl::visit_random_expr(ObOpRawExpr& expr, ObExprRandom* rand_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rand_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decode expr op is null", K(ret));
  } else {
    int64_t num_param = expr.get_param_exprs().count();
    if (num_param > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected number of arguments", K(num_param));
    } else if (num_param == 1) {
      rand_op->set_seed_const(expr.get_param_expr(0)->has_const_or_const_expr_flag());
    } else {
      rand_op->set_seed_const(true);
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("failed to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret), K(expr));
    }
  } else if (T_OP_ROW == expr.get_expr_type()) {
    // skip
  } else {
    ObExprOperator* op = NULL;
    ObExprOperatorType type = expr.get_expr_type();
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100) {
      switch (expr.get_expr_type()) {
        case T_OP_AGG_ADD: {
          type = T_OP_ADD;
          break;
        }
        case T_OP_AGG_MINUS: {
          type = T_OP_MINUS;
          break;
        }
        case T_OP_AGG_MUL: {
          type = T_OP_MUL;
          break;
        }
        case T_OP_AGG_DIV: {
          type = T_OP_DIV;
          break;
        }
        default: {
        }
      };
      if (type != expr.get_expr_type()) {
        LOG_DEBUG("replace agg arithmetic op to arithmetic op for compatibility", K(type), K(expr.get_expr_type()));
      }
    }
    if (OB_FAIL(factory_.alloc(type, op))) {
      LOG_WARN("fail to alloc expr_op", K(ret));
    } else if (OB_UNLIKELY(NULL == op)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc expr op", "expr type", get_type_name(expr.get_expr_type()), K(expr.get_expr_type()));
    } else {
      if (T_OP_REGEXP == expr.get_expr_type()) {
        ObExprRegexp* regexp_op = static_cast<ObExprRegexp*>(op);
        ret = visit_regex_expr(expr, regexp_op);
      } else if (T_OP_LIKE == expr.get_expr_type()) {
        ObExprLike* like_op = static_cast<ObExprLike*>(op);
        ret = visit_like_expr(expr, like_op);
      } else if (T_OP_MULTISET == expr.get_expr_type()) {
        ret = OB_NOT_SUPPORTED;
      } else if (T_OP_COLL_PRED == expr.get_expr_type()) {
        ObExprCollPred* ms_op = static_cast<ObExprCollPred*>(op);
        const ObCollPredRawExpr& ms_expr = static_cast<ObCollPredRawExpr&>(expr);
        ms_op->set_ms_type(ms_expr.get_multiset_type());
        ms_op->set_ms_modifier(ms_expr.get_multiset_modifier());
      } else if (T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type()) {
        ObExprInOrNotIn* in_op = static_cast<ObExprInOrNotIn*>(op);
        ret = visit_in_expr(expr, in_op);
      } else if (MAYBE_ROW_OP(expr.get_expr_type())) {
        ret = visit_maybe_row_expr(expr, op);
      } else if (IS_SUBQUERY_COMPARISON_OP(expr.get_expr_type())) {
        ObSubQueryRelationalExpr* subquery_op = static_cast<ObSubQueryRelationalExpr*>(op);
        ret = visit_subquery_cmp_expr(expr, subquery_op);
      } else {
        op->set_real_param_num(static_cast<int32_t>(expr.get_param_count()));
        op->set_row_dimension(ObExprOperator::NOT_ROW_DIMENSION);
      }
      if (OB_SUCC(ret)) {
        op->set_result_type(expr.get_result_type());
        if (OB_FAIL(op->set_input_types(expr.get_input_types()))) {
          LOG_WARN("fail copy input types", K(ret));
        } else if (OB_FAIL(item.assign(op))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
          LOG_WARN("failed to add expr item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObCaseOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  bool need_calc = true;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      // if an expr has been marked with IS_COLUMNLIZED but get invalid idx,
      // then it most likely be a const shared expr and need to calculate again
      if (ret == OB_ENTRY_NOT_EXIST && idx == OB_INVALID_INDEX &&
          (expr.has_flag(IS_CONST) || expr.has_flag(IS_CONST_EXPR))) {
        ret = OB_SUCCESS;
        expr.clear_flag(IS_COLUMNLIZED);
        LOG_TRACE("need to recalculate const expr", K(expr));
      } else if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("get index failed", K(ret), K(expr), K(idx));
      }
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("failed to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret));
    } else {
      need_calc = false;
    }
  }
  if (OB_SUCC(ret) && need_calc) {
    ObExprOperator *op = NULL;
    if (OB_FAIL(factory_.alloc(expr.get_expr_type(), op))) {
      LOG_WARN("fail to alloc expr_op", K(ret));
    } else if (OB_UNLIKELY(NULL == op)) {
      LOG_WARN("failed to alloc expr op", K(expr.get_expr_type()));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      op->set_real_param_num(static_cast<int32_t>(expr.get_param_count()));
      op->set_row_dimension(ObExprOperator::NOT_ROW_DIMENSION);
      op->set_result_type(expr.get_result_type());
      if (expr.is_arg_case()) {
        // @todo
        // ObExprArgCase* arg_case = dynamic_cast<ObExprArgCase*>(op);
        // OB_ASSERT(arg_case);
        // arg_case->set_null_equals_null(expr.get_is_decode_func());
      }
      if (T_OP_ARG_CASE == expr.get_expr_type()) {
        ObExprArgCase* argcase_op = static_cast<ObExprArgCase*>(op);
        ret = visit_argcase_expr(expr, argcase_op);
      }
      if (OB_SUCC(ret) && OB_FAIL(op->set_input_types(expr.get_input_types()))) {
        LOG_WARN("fail copy input types", K(ret));
      } else if (OB_FAIL(item.assign(op))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
        LOG_WARN("failed to add expr item", K(ret));
      }
    }
  }
  return ret;
}

// ObAggFunRawExpr is visited twice to generate %post_expr_ and %infix_expr_.
// some property of %aggr_expr is produced in the last visit (aggr_expr->is_gen_infix_expr()).
int ObExprGeneratorImpl::visit(ObAggFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("fail to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret), K(expr));
    }
  } else if (ObSqlExpression::EXPR_TYPE_AGGREGATE != sql_expr_->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected aggr_expr", K(ret), K(sql_expr_->get_type()), K(*sql_expr_), K(expr));
  } else {
    ObAggregateExpression* aggr_expr = static_cast<ObAggregateExpression*>(sql_expr_);
    aggr_expr->set_aggr_func(expr.get_expr_type(), expr.is_param_distinct());
    aggr_expr->set_collation_type(expr.get_collation_type());
    aggr_expr->set_accuracy(expr.get_accuracy());
    int64_t col_count = (T_FUN_JSON_OBJECTAGG == expr.get_expr_type()) ?  2 : 1;
    aggr_expr->set_real_param_col_count(col_count);
    aggr_expr->set_all_param_col_count(col_count);
    const ObIArray<ObRawExpr*>& real_param_exprs = expr.get_real_param_exprs();
    if (aggr_expr->is_gen_infix_expr() && OB_FAIL(aggr_expr->init_aggr_cs_type_count(real_param_exprs.count()))) {
      LOG_WARN("failed to init aggr cs type count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); i++) {
      if (OB_ISNULL(real_param_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("real param expr is null", K(ret), K(i));
      } else {
        if (aggr_expr->is_gen_infix_expr() &&
            OB_FAIL(aggr_expr->add_aggr_cs_type(real_param_exprs.at(i)->get_collation_type()))) {
          LOG_WARN("add cs type fail", K(ret));
        }
      }
      if (OB_SUCC(ret) && real_param_exprs.at(i)->is_column_ref_expr()) {
        // if the input expr is a column, we should set the column name as the expr name.
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(real_param_exprs.at(i));
        const ObString& real_expr_name =
            col_expr->get_alias_column_name().empty() ? col_expr->get_column_name() : col_expr->get_alias_column_name();
        real_param_exprs.at(i)->set_expr_name(real_expr_name);
      }
    }
    if ((T_FUN_COUNT == expr.get_expr_type() && expr.get_real_param_count() > 1) ||
        (T_FUN_APPROX_COUNT_DISTINCT == expr.get_expr_type() && expr.get_real_param_count() > 1) ||
        (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == expr.get_expr_type() && expr.get_real_param_count() > 1) ||
        T_FUN_GROUP_CONCAT == expr.get_expr_type() || T_FUN_AGG_UDF == expr.get_expr_type() ||
        T_FUN_GROUP_RANK == expr.get_expr_type() || T_FUN_GROUP_DENSE_RANK == expr.get_expr_type() ||
        T_FUN_GROUP_PERCENT_RANK == expr.get_expr_type() || T_FUN_GROUP_CUME_DIST == expr.get_expr_type() ||
        T_FUN_GROUP_PERCENTILE_CONT == expr.get_expr_type() || T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type() ||
        T_FUN_MEDIAN == expr.get_expr_type() || T_FUN_KEEP_SUM == expr.get_expr_type() ||
        T_FUN_KEEP_MAX == expr.get_expr_type() || T_FUN_KEEP_MIN == expr.get_expr_type() ||
        T_FUN_KEEP_COUNT == expr.get_expr_type() || T_FUN_KEEP_WM_CONCAT == expr.get_expr_type() || 
        (T_FUN_JSON_OBJECTAGG == expr.get_expr_type() && expr.get_real_param_count() > 1)) {
      ObExprOperator* op = NULL;
      if (OB_FAIL(factory_.alloc(T_OP_AGG_PARAM_LIST, op))) {
        LOG_WARN("fail to alloc expr_op", K(ret));
      } else if (OB_UNLIKELY(NULL == op)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to alloc expr op", "expr type", get_type_name(T_OP_AGG_PARAM_LIST));
      } else {
        op->set_row_dimension(static_cast<int32_t>(expr.get_param_count()));
        op->set_real_param_num(col_count);
        op->set_result_type(expr.get_result_type());
        if (OB_FAIL(item.assign(op))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
          LOG_WARN("failed to add expr item", K(ret));
        } else {
          aggr_expr->set_real_param_col_count(expr.get_real_param_count());
          aggr_expr->set_all_param_col_count(expr.get_param_count());
          if (OB_SUCCESS == ret &&
              (T_FUN_GROUP_CONCAT == expr.get_expr_type() || T_FUN_GROUP_RANK == expr.get_expr_type() ||
                  T_FUN_GROUP_DENSE_RANK == expr.get_expr_type() || T_FUN_GROUP_PERCENT_RANK == expr.get_expr_type() ||
                  T_FUN_GROUP_CUME_DIST == expr.get_expr_type() ||
                  T_FUN_GROUP_PERCENTILE_CONT == expr.get_expr_type() ||
                  T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type() || T_FUN_MEDIAN == expr.get_expr_type() ||
                  T_FUN_KEEP_MAX == expr.get_expr_type() || T_FUN_KEEP_MIN == expr.get_expr_type() ||
                  T_FUN_KEEP_SUM == expr.get_expr_type() || T_FUN_KEEP_COUNT == expr.get_expr_type() ||
                  T_FUN_KEEP_WM_CONCAT == expr.get_expr_type())) {
            ObConstRawExpr* sep_expr = static_cast<ObConstRawExpr*>(expr.get_separator_param_expr());
            // set separator
            if (NULL != sep_expr) {
              ObPostExprItem sep_item;
              sep_item.set_accuracy(sep_expr->get_accuracy());
              if (OB_FAIL(sep_item.assign(sep_expr->get_value()))) {
                LOG_WARN("failed to assign const value", K(ret));
              } else if (OB_FAIL(aggr_expr->add_separator_param_expr_item(sep_item, sep_expr))) {
                LOG_WARN("failed to add sep expr item", K(ret));
              }
            } else {
            }

            if (OB_SUCC(ret) && aggr_expr->is_gen_infix_expr()) {
              // Child expr may set IS_COLUMNLIZED flag by %row_desc.add_column(), we need to
              // revert this. Since child is visited after parent in infix expression generation,
              // Error will be reported if child is visited with incorrect IS_COLUMNLIZED flag.
              ObSEArray<ObRawExpr*, 16> columnlized_exprs;
              const ObIArray<OrderItem>& sort_keys = expr.get_order_items();
              int64_t N = sort_keys.count();
              int64_t sort_idx = OB_INVALID_INDEX;
              RowDesc row_desc;
              if (OB_FAIL(row_desc.init())) {
                LOG_WARN("fail to init row desc", K(ret));
              } else if (N > 0 && OB_FAIL(aggr_expr->init_sort_column_count(N))) {
                LOG_WARN("fail to init sort column count", K(ret));
              } else if (N > 0 && OB_FAIL(aggr_expr->init_sort_extra_infos_(N))) {
                LOG_WARN("fail to init sort extra infos", K(ret));
              } else {
                for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
                  ObRawExpr* e = expr.get_param_expr(i);
                  if (OB_ISNULL(e)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("NULL expr returned", K(ret), K(i));
                  } else if (!e->has_flag(IS_COLUMNLIZED) && OB_FAIL(columnlized_exprs.push_back(e))) {
                    LOG_WARN("array push back failed", K(ret));
                  } else if (OB_FAIL(row_desc.add_column(e))) {
                    if (OB_HASH_EXIST == ret) {
                      ret = OB_SUCCESS;
                    } else {
                      LOG_WARN("fail to add param expr to row desc", K(ret));
                    }
                  }
                }
              }
              // add sort columns
              for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
                const ObRawExpr* raw_expr = sort_keys.at(i).expr_;
                if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
                  if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
                    continue;  // sort by const value, just ignore
                  } else {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_ERROR("sort column should have be columnlized", K(*raw_expr));
                  }
                } else if (OB_FAIL(row_desc.get_idx(raw_expr, sort_idx))) {
                  LOG_ERROR("failed to find column", K(*raw_expr), K(raw_expr));
                } else if (OB_FAIL(aggr_expr->add_sort_column(
                               sort_idx, raw_expr->get_collation_type(), sort_keys.at(i).is_ascending()))) {
                  LOG_WARN("failed to add typed sort column", K(ret), K(sort_idx), K(raw_expr->get_data_type()));
                } else {
                  ObOpSchemaObj op_schema_obj(ITEM_TO_OBJ_TYPE(raw_expr->get_data_type()), sort_keys.at(i).order_type_);
                  if (OB_FAIL(aggr_expr->get_sort_extra_infos().push_back(op_schema_obj))) {
                    LOG_WARN("failed to push back ObOpSchemaObj", K(ret));
                  } else {
                    /*do nothing*/
                  }
                }
              }  // end for

              FOREACH(e, columnlized_exprs)
              {
                if ((*e)->has_flag(IS_COLUMNLIZED)) {
                  (*e)->clear_flag(IS_COLUMNLIZED);
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObWinFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("fail to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("fail to add expr item", K(ret), K(expr));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("all window expr should have been generated", K(expr), K(&expr));
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObPseudoColumnRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("fail to set column", K(ret), K(expr), K(&expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret), K(expr));
    }
  } else if (T_PDML_PARTITION_ID == expr.get_expr_type()) {
    ObExprOperator* pdml_partition_id_op = NULL;
    LOG_TRACE("alloc pdml partition id expr phy operator", K(expr));
    if (OB_FAIL(factory_.alloc(expr.get_expr_type(), pdml_partition_id_op))) {
      LOG_WARN("failed to alloc expr", K(get_type_name(expr.get_expr_type())));
    } else if (OB_ISNULL(pdml_partition_id_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ob pdml partition id expr operator is null", K(ret));
    } else {
      pdml_partition_id_op->set_result_type(expr.get_result_type());
      pdml_partition_id_op->set_real_param_num(expr.get_param_count());
      LOG_TRACE("alloc pdml partition id expr operator successfully", K(*pdml_partition_id_op));
      if (OB_FAIL(item.assign(pdml_partition_id_op))) {
        LOG_WARN("failed to assign pdml partition id expr operator", K(ret));
      } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
        LOG_WARN("failed to add expr item", K(ret));
      } else {
        // do nothing
      }
    }
    LOG_TRACE("alloc pdml partition id expr operator", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("all pseudo column expr should have been generated", K(expr), K(&expr));
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (OB_FAIL(visit_simple_op(expr))) {
    LOG_ERROR("visit simple op failed", K(ret));
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObSetOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item;
  item.set_accuracy(expr.get_accuracy());
  if (OB_ISNULL(sql_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_expr_ is NULL");
  } else if (expr.has_flag(IS_COLUMNLIZED)) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(&expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(item.set_column(idx))) {
      LOG_WARN("failed to set column", K(ret), K(expr));
    } else if (OB_FAIL(sql_expr_->add_expr_item(item, &expr))) {
      LOG_WARN("failed to add expr item", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("all set expr should have been generated", K(expr), K(&expr));
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObSetIterRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObIterExprOperator* left_iter = NULL;
  ObIterExprOperator* right_iter = NULL;
  ObSetOpIterExpr* set_iter = NULL;
  int64_t iter_cnt = iter_expr_desc_.count();
  if (OB_FAIL(factory_.alloc(expr.get_expr_type(), set_iter))) {
    LOG_WARN("alloc set operation iterator expr failed", K(ret));
  } else if (OB_UNLIKELY(iter_cnt < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child expr desc is invalid", K(iter_cnt));
  } else if (OB_FAIL(iter_expr_desc_.pop_back(right_iter))) {
    LOG_WARN("pop back iter expr desc to right iter failed", K(ret));
  } else if (OB_FAIL(iter_expr_desc_.pop_back(left_iter))) {
    LOG_WARN("pop back iter expr desc to left iter failed", K(ret));
  } else {
    set_iter->set_left_iter(left_iter);
    set_iter->set_right_iter(right_iter);
    ret = iter_expr_desc_.push_back(set_iter);
  }
  return ret;
}

int ObExprGeneratorImpl::visit(ObRowIterRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObIndexScanIterExpr* iter_expr = NULL;
  if (OB_FAIL(factory_.alloc(expr.get_expr_type(), iter_expr))) {
    LOG_WARN("alloc row iterator expr failed", K(ret));
  } else if (OB_ISNULL(iter_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter_expr is null");
  } else {
    iter_expr->set_iter_idx(expr.get_iter_idx());
    if (OB_FAIL(iter_expr_desc_.push_back(iter_expr))) {
      LOG_WARN("store iter expr to iter expr desc failed", K(ret));
    }
  }
  return ret;
}

bool ObExprGeneratorImpl::skip_child(ObRawExpr& expr)
{
  return expr.has_flag(IS_COLUMNLIZED);
}

int ObExprGeneratorImpl::gen_fast_expr(ObRawExpr& raw_expr)
{
  int ret = OB_SUCCESS;
  if (raw_expr.get_expr_type() == T_FUN_COLUMN_CONV && !raw_expr.has_flag(IS_COLUMNLIZED)) {
    if (OB_ISNULL(raw_expr.get_param_expr(4))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw expr param is null");
    } else if (raw_expr.get_param_expr(4)->is_column_ref_expr() || raw_expr.get_param_expr(4)->is_const_expr()) {
      if (OB_FAIL(gen_fast_column_conv_expr(raw_expr))) {
        LOG_WARN("gen fast column conv expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::gen_fast_column_conv_expr(ObRawExpr& raw_expr)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* data_type_expr = NULL;
  const ObRawExpr* collaction_type_expr = NULL;
  const ObRawExpr* accuracy_expr = NULL;
  const ObRawExpr* not_null_expr = NULL;
  const ObRawExpr* value_expr = NULL;
  ObFastColumnConvExpr* fast_conv_expr = NULL;
  if (OB_UNLIKELY(raw_expr.get_expr_type() != T_FUN_COLUMN_CONV) ||
      raw_expr.get_param_count() < ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO ||
      OB_ISNULL(data_type_expr = raw_expr.get_param_expr(0)) ||
      OB_ISNULL(collaction_type_expr = raw_expr.get_param_expr(1)) ||
      OB_ISNULL(accuracy_expr = raw_expr.get_param_expr(2)) || OB_ISNULL(not_null_expr = raw_expr.get_param_expr(3)) ||
      OB_ISNULL(value_expr = raw_expr.get_param_expr(4))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw expr type is invalid", K(raw_expr));
  } else if (OB_UNLIKELY(!data_type_expr->is_const_expr()) || OB_UNLIKELY(!collaction_type_expr->is_const_expr()) ||
             OB_UNLIKELY(!accuracy_expr->is_const_expr()) || OB_UNLIKELY(!not_null_expr->is_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column convert expr is invalid", K(ret), K(raw_expr));
  } else if (OB_FAIL(factory_.create_fast_expr(T_FUN_COLUMN_CONV, fast_conv_expr))) {
    LOG_WARN("allocate fast column conv expr failed", K(ret));
  } else if (OB_ISNULL(fast_conv_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fast column conv expr is null");
  } else if (OB_UNLIKELY(!value_expr->is_column_ref_expr()) && OB_UNLIKELY(!value_expr->is_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value expr is invalid", KPC(value_expr));
  } else if (value_expr->is_column_ref_expr()) {
    // value from column
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(column_idx_provider_.get_idx(value_expr, idx))) {
      LOG_WARN("value index is invalid", KPC(value_expr));
    } else {
      fast_conv_expr->set_column(idx);
    }
  } else {
    const ObConstRawExpr* const_value_expr = static_cast<const ObConstRawExpr*>(value_expr);
    if (OB_FAIL(fast_conv_expr->set_const_value(const_value_expr->get_value()))) {
      LOG_WARN("get value index from const value expr failed", KPC(const_value_expr), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // Handling the type of fast column convert
    ObExprResType column_type;
    int32_t data_type = 0;
    int32_t coll_type = 0;
    int64_t accuracy_type = 0;
    int8_t is_nullable = 0;
    ObString column_info = ObString::make_string("");
    const ObConstRawExpr* const_data_type_expr = static_cast<const ObConstRawExpr*>(data_type_expr);
    const ObConstRawExpr* const_coll_type_expr = static_cast<const ObConstRawExpr*>(collaction_type_expr);
    const ObConstRawExpr* const_accuracy_expr = static_cast<const ObConstRawExpr*>(accuracy_expr);
    const ObConstRawExpr* const_not_null_expr = static_cast<const ObConstRawExpr*>(not_null_expr);
    if (OB_FAIL(const_data_type_expr->get_value().get_int32(data_type))) {
      LOG_WARN("get int from data type expr failed", K(ret), KPC(const_data_type_expr));
    } else if (OB_FAIL(const_coll_type_expr->get_value().get_int32(coll_type))) {
      LOG_WARN("get int from collation type expr failed", K(ret), KPC(const_coll_type_expr));
    } else if (OB_FAIL(const_accuracy_expr->get_value().get_int(accuracy_type))) {
      LOG_WARN("get int from accuracy expr failed", K(ret), KPC(const_accuracy_expr));
    } else if (OB_FAIL(const_not_null_expr->get_value().get_tinyint(is_nullable))) {
      LOG_WARN("get int from const not null expr failed", K(ret), KPC(const_not_null_expr));
    } else if (OB_FAIL(visit_column_conv_expr(raw_expr, fast_conv_expr))) {
      LOG_WARN("fail to visit column conv expr", K(raw_expr), K(ret));
    } else {
      if (raw_expr.get_param_count() >= ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO) {
        ObRawExpr* ci_expr = raw_expr.get_param_expr(5);
        CK(NULL != ci_expr);
        CK(ci_expr->is_const_expr());
        OZ(static_cast<const ObConstRawExpr*>(ci_expr)->get_value().get_string(column_info));
      }
    }

    if (OB_SUCC(ret)) {
      ObObjType dst_type = static_cast<ObObjType>(data_type);
      dst_type = ObLobType == dst_type ? ObLongTextType : dst_type;
      column_type.set_type(dst_type);
      column_type.set_collation_type(static_cast<ObCollationType>(coll_type));
      column_type.set_accuracy(accuracy_type);
      column_type.set_result_flag(is_nullable ? 0 : OB_MYSQL_NOT_NULL_FLAG);
      fast_conv_expr->set_column_type(column_type);
      fast_conv_expr->set_value_accuracy(value_expr->get_accuracy());
      if (OB_FAIL(fast_conv_expr->set_column_info(column_info))) {
        LOG_WARN("fail to set column info", K(column_info), K(ret));
      } else {
        sql_expr_->set_fast_expr(fast_conv_expr);
      }
    }
  }
  return ret;
}

int ObExprGeneratorImpl::generate_expr_operator(ObRawExpr& raw_expr, ObExprOperatorFetcher& fetcher)
{
  int ret = OB_SUCCESS;
  sql_expr_ = &fetcher;
  fetcher.op_ = NULL;
  ObItemType type = raw_expr.get_expr_type();
  if (IS_EXPR_OP(type) && !IS_AGGR_FUN(type)) {
    raw_expr.clear_flag(IS_COLUMNLIZED);
    // no expr operator for set expr
    if (!(type > T_OP_SET && type <= T_OP_EXCEPT)) {
      OZ(raw_expr.do_visit(*this));
    }
  }
  sql_expr_ = NULL;
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase
