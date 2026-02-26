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
#include "sql/engine/expr/ob_expr_pl_associative_index.h"
#include "sql/ob_spi.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObExprPLAssocIndex, ObExprOperator),
                    info_.for_write_,
                    info_.out_of_range_set_err_,
                    info_.parent_expr_type_,
                    info_.is_index_by_varchar_);

ObExprPLAssocIndex::ObExprPLAssocIndex(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_PL_ASSOCIATIVE_INDEX, N_PL_ASSOCIATIVE_INDEX, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                  false, INTERNAL_IN_ORACLE_MODE),
    info_()
{
}

ObExprPLAssocIndex::~ObExprPLAssocIndex()
{
}

void ObExprPLAssocIndex::reset()
{
  ObExprOperator::reset();
  info_ = Info();
}

int ObExprPLAssocIndex::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_FUN_PL_ASSOCIATIVE_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    info_ = static_cast<const ObExprPLAssocIndex &>(other).info_;
  }
  return ret;
}

int ObExprPLAssocIndex::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(type1);
  UNUSED(type2);
  type.set_int();
  type.set_precision(DEFAULT_SCALE_FOR_INTEGER);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  return ret;
}

int ObExprPLAssocIndex::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  const ObPLAssocIndexRawExpr &assoc_idx_expr = static_cast<const ObPLAssocIndexRawExpr &>(raw_expr);
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(rt_expr.arg_cnt_));
  } else {
    Info info;
    info.for_write_ = assoc_idx_expr.get_write();
    info.out_of_range_set_err_ = assoc_idx_expr.get_out_of_range_set_err();
    info.parent_expr_type_ = assoc_idx_expr.get_parent_type();
    info.is_index_by_varchar_ = assoc_idx_expr.is_index_by_varchar();

    rt_expr.extra_ = info.v_;
    rt_expr.eval_func_ = &eval_assoc_idx;
  }
  return ret;
}

#ifndef OB_BUILD_ORACLE_PL
int ObExprPLAssocIndex::eval_assoc_idx(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSEDx(expr, ctx, expr_datum);
  return ret;
}
#else
int ObExprPLAssocIndex::do_eval_assoc_index(int64_t &assoc_idx,
                                            ObExecContext &exec_ctx,
                                            const Info &info,
                                            pl::ObPLAssocArray &assoc_array_ref,
                                            const common::ObObj &key)
{
  return do_eval_assoc_index(assoc_idx,
                             exec_ctx.get_my_session(),
                             info,
                             assoc_array_ref,
                             key,
                             exec_ctx.get_allocator());
}

int ObExprPLAssocIndex::do_eval_assoc_index(int64_t &assoc_idx,
                                            ObSQLSessionInfo *session,
                                            const Info &info,
                                            pl::ObPLAssocArray &assoc_array_ref,
                                            const common::ObObj &key,
                                            ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  pl::ObPLAssocArray *assoc_array = &assoc_array_ref;
  int64_t index = OB_INVALID_INDEX;
  int64_t search_end = OB_INVALID_INDEX;

  if (assoc_array->get_count() > 0 && OB_ISNULL(assoc_array->get_key())) { // it`s opt when bulk collect & key is pls_integer
    if (!key.is_integer_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid associative array", K(*assoc_array), K(key), K(ret));
    } else if (info.for_write_) {
      if (OB_FAIL(assoc_array->reserve_assoc_key())) {
        LOG_WARN("failed to reserve_assoc_key", K(ret), K(info));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(assoc_array->search_key(key, index, search_end, assoc_array->get_count()))) {
    LOG_WARN("failed to search key", K(ret), K(key), K(index), K(search_end));
  }

  if (OB_FAIL(ret)) {
  } else if (info.for_write_) {
    if (OB_INVALID_INDEX == index) {
      ObObj new_key;
      if (OB_FAIL(deep_copy_obj(*assoc_array->get_allocator(), key, new_key))) {
        LOG_WARN("failed to copy key", K(ret), K(key), K(new_key), KPC(assoc_array));
      } else if (OB_FAIL(ObSPIService::spi_extend_assoc_array(session->get_effective_tenant_id(),
                                                              session->get_pl_context()->get_current_ctx(),
                                                              allocator,
                                                              *assoc_array,
                                                              1))) {
        int tmp_ret = pl::ObUserDefinedType::destruct_objparam(*assoc_array->get_allocator(), new_key, session);
        LOG_WARN("failed to extend assoc array", K(ret), K(tmp_ret), KPC(assoc_array));
      } else if (OB_FAIL(assoc_array->set_key(assoc_array->get_count() - 1, new_key))) {
        LOG_ERROR("failed to set new key", K(ret), K(key), K(new_key), KPC(assoc_array));
      } else if (OB_FAIL(assoc_array->insert_sort(key, assoc_array->get_count() - 1, search_end, assoc_array->get_count() - 1))) {
        LOG_ERROR("failed to insert new key", K(ret), K(key), K(new_key), KPC(assoc_array));
      } else if (OB_FAIL(assoc_array->update_first_last(OB_INVALID_INDEX == search_end ? 0 : search_end))) {
        LOG_WARN("failed to update first last", K(ret), K(search_end));
      } else {
        assoc_idx = assoc_array->get_count();
      }
    } else { // old Key
      bool is_deleted = false;
      assoc_idx = index + 1;
      if (OB_FAIL(assoc_array->is_elem_deleted(index, is_deleted))) {
        LOG_WARN("failed to test element deleted.", K(ret));
      } else if (is_deleted) {
        if (assoc_array->get_element_desc().is_composite_type()) { // renew a composite memmory
          if (OB_FAIL(ObSPIService::spi_new_coll_element(assoc_array->get_id(),
                                                         assoc_array->get_allocator(),
                                                         session->get_pl_context()->get_current_ctx(),
                                                         assoc_array->get_data() + index))) {
            LOG_WARN("failed to new coll element", K(ret), KPC(assoc_array), K(index));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(assoc_array->update_first_last(search_end))) {
          LOG_WARN("failed to update first last", K(ret), K(search_end));
        }
      }
    }
  } else {
    if (OB_INVALID_INDEX != index) {
      assoc_idx = index + 1;
    } else if (NULL == assoc_array->get_key() && key.get_int() <= assoc_array->get_count()) {
      assoc_idx = key.get_int();
    } else {
      if (info.out_of_range_set_err_) {
        ret = OB_READ_NOTHING;
        LOG_WARN("key of associative not exists", K(key), KPC(assoc_array), K(ret));
      } else {
        // pl collection的构建有时候会走到这里，out of range的话默认属性是赋值为NULL
        // 例如 aa('e')  ===> NULL, 如果aa是关联数组，e不是一个key
        // 假设a的索引是 b, c,  g,   那么a.exists('e')和a.next('e')不能返回相同的值。
        // next和prior需要返回三种不同的值，一是小于first，返回-1， 二是大于next，返回-2，
        // 三是中间值，但不存在，这个时候需要看prior和next返回最接近的前一个值索引位置。
        /*enum parent_expr_type {
          EXPR_UNKNOWN = -1,
          EXPR_PRIOR,
          EXPR_NEXT,
          EXPR_EXISTS,
        }; */
        const pl::parent_expr_type type = info.parent_expr_type_;
        int64_t cnt = 0, index = 0;
        if (pl::parent_expr_type::EXPR_EXISTS == type) {
          assoc_idx = OB_INVALID_INDEX;
        } else if (pl::parent_expr_type::EXPR_NEXT == type
                   && OB_INVALID_INDEX == assoc_array->get_last()) {
          assoc_idx = pl::ObPLCollection::IndexRangeType::LARGE_THAN_LAST;
        } else if (pl::parent_expr_type::EXPR_PRIOR == type
                   && OB_INVALID_INDEX == assoc_array->get_first()) {
          assoc_idx = pl::ObPLCollection::IndexRangeType::LESS_THAN_FIRST;
        } else if (pl::parent_expr_type::EXPR_NEXT == type
                  || pl::parent_expr_type::EXPR_PRIOR == type) {
          int64_t fidx = assoc_array->get_first() - 1;
          int64_t lidx = assoc_array->get_last() - 1;
          const ObObj &first = assoc_array->get_key()[fidx];
          const ObObj &last = assoc_array->get_key()[lidx];
          if (key < first) {
            assoc_idx = pl::ObPLCollection::IndexRangeType::LESS_THAN_FIRST;
          } else if (key > last) {
            assoc_idx = pl::ObPLCollection::IndexRangeType::LARGE_THAN_LAST;
          } else {
            if (OB_INVALID_INDEX == search_end
                && first == last // must not equal, otherwise, it's a bug
                && key < assoc_array->get_key()[assoc_array->get_sort()[search_end]]) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("index out of range, associative array", K(ret), K(key), KPC(assoc_array), K(search_end));
            } else {
              // A little tricky here:
              // We need calc NEXT property in ObjAccess expression again, so we get prior of current index,
              // then ObjAccess can got current index. the PRIOR do the same thing.
              if (pl::parent_expr_type::EXPR_NEXT == type) {
                index = assoc_array->get_sort()[search_end - 1];
              } else {
                index = assoc_array->get_sort()[search_end];
              }
              assoc_idx = index + 1;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprPLAssocIndex::eval_assoc_idx(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *array;
  ObDatum *key;
  ObObj key_obj;
  Info info;
  int64_t assoc_idx = 0;

  info.v_ = expr.extra_;
  if (OB_FAIL(expr.eval_param_value(ctx, array, key))) {
    LOG_WARN("failed to eval param value", K(ret), K(info));
  } else if (OB_FAIL(key->to_obj(key_obj, expr.args_[1]->obj_meta_, expr.args_[1]->obj_datum_map_))) {
    LOG_WARN("failed to obj", K(ret), K(key_obj), KPC(key));
  } else if (key_obj.is_null()) {
    if (pl::parent_expr_type::EXPR_UNKNOWN == info.parent_expr_type_) {
      ObString msg("NULL index table key value");
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, msg.length(), msg.ptr());
    } else if (pl::parent_expr_type::EXPR_NEXT != info.parent_expr_type_) {
      expr_datum.set_int(pl::ObPLCollection::IndexRangeType::LESS_THAN_FIRST);
    } else if (expr.args_[1]->obj_meta_.is_null() || !info.is_index_by_varchar_) { // null
      expr_datum.set_int(pl::ObPLCollection::IndexRangeType::LARGE_THAN_LAST);
    } else { // '' empty string
      expr_datum.set_int(pl::ObPLCollection::IndexRangeType::LESS_THAN_FIRST);
    }
  } else {
    pl::ObPLAssocArray *assoc_array = nullptr;
    if (info.for_write_) {
      pl::ObPlCompiteWrite *composite_write = reinterpret_cast<pl::ObPlCompiteWrite *>(array->extend_obj_->get_ext());
      if (OB_ISNULL(composite_write)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected composite write obj", K(ret));
      } else {
        assoc_array = reinterpret_cast<pl::ObPLAssocArray *>(composite_write->value_addr_);
      }
    } else {
      assoc_array = reinterpret_cast<pl::ObPLAssocArray *>(array->extend_obj_->get_ext());
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(assoc_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected assoc array", K(ret));
    } else if (OB_FAIL(do_eval_assoc_index(assoc_idx, ctx.exec_ctx_, info, *assoc_array, key_obj))) {
      LOG_WARN("failed to eval assoc index", K(ret), K(info), KPC(assoc_array), K(key_obj));
    } else {
      expr_datum.set_int(assoc_idx);
    }
  }
  return ret;
}
#endif

}  // namespace sql
}  // namespace oceanbase


