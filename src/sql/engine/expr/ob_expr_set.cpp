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
#include "sql/engine/expr/ob_expr_set.h"
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"
#include "pl/ob_pl_allocator.h"
#include "sql/engine/expr/ob_expr_multiset.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
}
}

ObExprSet::ObExprSet(ObIAllocator &alloc)
  :ObFuncExprOperator(alloc, T_FUN_SYS_SET, N_SET, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSet::~ObExprSet()
{
}

int ObExprSet::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_SET) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
  }
  return ret;
}

int ObExprSet::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (type1.is_ext()) {
      type.set_ext();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      type.set_calc_type(type1.get_calc_type());
      type.set_result_flag(NOT_NULL_FLAG);
      type.set_udt_id(type1.get_udt_id());
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
              K(ret), K(type1));
    }
  } else {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
              K(ret), K(type1));
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObExprSet::eval_coll(const ObObj &obj, ObExecContext &ctx, pl::ObPLNestedTable *&coll)
{
  int ret = OB_SUCCESS;
  pl::ObPLCollection *c1 = reinterpret_cast<pl::ObPLCollection *>(obj.get_ext());
  ObIAllocator &allocator = ctx.get_allocator();
  ObIAllocator *collection_allocator = NULL;
  ObObj *data_arr = NULL;
  int64_t elem_count = 0;

  if (OB_ISNULL(c1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("union udt failed due to null udt", K(ret), K(obj));
  } else if (pl::PL_NESTED_TABLE_TYPE != c1->get_type()) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
    K(ret), K(c1->get_type()));
  } else if (0 > c1->get_count() || !(c1->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("union udt failed due to null udt", K(ret), K(c1->get_count()), K(c1->is_inited()));
  } else {
    coll = static_cast<pl::ObPLNestedTable*>(allocator.alloc(sizeof(pl::ObPLNestedTable)));
    if (OB_ISNULL(coll)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(ret));
    } else {
      coll = new(coll)pl::ObPLNestedTable(c1->get_id());
      collection_allocator =
                  static_cast<ObIAllocator*>(allocator.alloc(sizeof(pl::ObPLCollAllocator)));
      collection_allocator = new(collection_allocator)pl::ObPLCollAllocator(coll);
      if (OB_ISNULL(collection_allocator)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc pl collection allocator failed.", K(ret));
      } else {
        if (OB_FAIL(ObExprMultiSet::calc_ms_one_distinct(collection_allocator,
                                                          reinterpret_cast<ObObj *>(c1->get_data()),
                                                          c1->get_count(),
                                                          data_arr,
                                                          elem_count))) {
          LOG_WARN("failed to distinct nest table", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to calc set operator", K(ret), K(data_arr));
  }
  else if (0 != elem_count && OB_ISNULL(data_arr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected result.", K(elem_count), K(data_arr), K(ret));
  } else {
    coll->set_allocator(collection_allocator);
    coll->set_type(c1->get_type());
    coll->set_id(c1->get_id());
    coll->set_is_null(c1->is_null());
    coll->set_element_type(c1->get_element_type());
    coll->set_column_count(c1->get_column_count());
    coll->set_not_null(c1->is_not_null());
    coll->set_count(elem_count);
    coll->set_first(elem_count > 0 ? 1 : OB_INVALID_ID);
    coll->set_last(elem_count > 0 ? elem_count : OB_INVALID_ID);
    coll->set_data(data_arr);
  }
  return ret;
}
#endif

int ObExprSet::cg_expr(
    ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK (1 == rt_expr.arg_cnt_);
  OX (rt_expr.eval_func_ = calc_set);
  return ret;
}

int ObExprSet::calc_set(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(expr, ctx, expr_datum);
#else
  if (lib::is_oracle_mode() && ObExtendType == expr.args_[0]->datum_meta_.type_) {
    ObDatum *datum = NULL;
    pl::ObPLNestedTable *coll = NULL;
    ObObj *obj = NULL;
    ObObjMeta obj_meta;
    obj_meta.set_ext();
    CK (OB_NOT_NULL(obj = static_cast<ObObj *>(
        ctx.exec_ctx_.get_allocator().alloc(sizeof(ObObj)))));
    OZ (expr.args_[0]->eval(ctx, datum));
    OX (datum->to_obj(*obj, obj_meta));
    OZ (eval_coll(*obj, ctx.exec_ctx_, coll));
    CK (OB_NOT_NULL(coll));
    OX (obj->set_extend(reinterpret_cast<int64_t>(coll), coll->get_type()));
    OZ (expr_datum.from_obj(*obj));
    //Collection constructed here must be recorded and destructed at last
    if (OB_NOT_NULL(coll) &&
        OB_NOT_NULL(coll->get_allocator())) {
      common::ObIAllocator *collection_allocator = dynamic_cast<pl::ObPLCollAllocator*>(coll->get_allocator());
      if (OB_NOT_NULL(collection_allocator)) {
        int tmp_ret = OB_SUCCESS;
        auto &exec_ctx = ctx.exec_ctx_;
        if (OB_ISNULL(exec_ctx.get_pl_ctx())) {
          tmp_ret = exec_ctx.init_pl_ctx();
        }
        if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(exec_ctx.get_pl_ctx())) {
          tmp_ret = exec_ctx.get_pl_ctx()->add(*obj);
        }
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("fail to collect pl collection allocator, may be exist memory issue", K(tmp_ret));
        }
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
#endif
  return ret;
}

