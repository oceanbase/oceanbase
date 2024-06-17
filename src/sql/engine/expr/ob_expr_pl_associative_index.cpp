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
#include "sql/engine/ob_exec_context.h"
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

#ifdef OB_BUILD_ORACLE_PL
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
  if (assoc_array->get_count() >0 && OB_ISNULL(assoc_array->get_key())) {
    //如果下标是int类型，并且是BULK的数据，那么key_为空，这是一个优化
    if (!key.is_integer_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid associative array", K(*assoc_array), K(key), K(ret));
    } else if (info.for_write_) {
      OZ (reserve_assoc_key(*assoc_array));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }

  int64_t index = OB_INVALID_INDEX;
  if (OB_SUCC(ret) && OB_NOT_NULL(assoc_array->get_key())) {
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == index && i < assoc_array->get_count(); ++i) {
      const ObObj &cur_obj = assoc_array->get_key()[i];
      if (!cur_obj.can_compare(key)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not compare", K(cur_obj), K(key), K(i), K(ret));
      } else if (cur_obj.is_string_type() && cur_obj.get_collation_type() != key.get_collation_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not compare", K(cur_obj), K(key), K(i), K(ret));
      } else if (0 == cur_obj.compare(key)) {
        index = i;
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret)) {
    if (info.for_write_) {
      if (OB_INVALID_INDEX == index) {
        pl::ObPLExecCtx *pl_exec_ctx = session->get_pl_context()->get_current_ctx();
        if (OB_FAIL(ObSPIService::spi_extend_assoc_array(session->get_effective_tenant_id(),
                                                         pl_exec_ctx, allocator, *assoc_array, 1))) {
          LOG_WARN("failed to spi_set_collection_data", K(*assoc_array), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t pos = OB_INVALID_INDEX == index ? assoc_array->get_count() : index + 1; //pos是传给ObjAccess使用的下标，从1开始
        assoc_idx = pos;

        if (OB_INVALID_INDEX == index) { //如果是一个新的key，需要插入key和sort
          ObObj new_key;
          if (OB_FAIL(deep_copy_obj(*assoc_array->get_allocator(), key, new_key))) {
            LOG_WARN("failed to copy key", K(pos), K(key), K(*assoc_array), K(ret));
          } else if (OB_FAIL(assoc_array->set_key(pos - 1, new_key))) {
            LOG_WARN("failed to set key", K(pos), K(key), K(*assoc_array), K(ret));
          } else {
            //找到新插入key的index
            int64_t pre_idx = OB_INVALID_INDEX;
            int64_t sort_idx = OB_INVALID_INDEX == assoc_array->get_first() ? OB_INVALID_INDEX : assoc_array->get_first() - 1;
            while (OB_SUCC(ret) && OB_INVALID_INDEX != sort_idx) {
              ObObj *current_key = assoc_array->get_key(sort_idx);
              if (!key.can_compare(*current_key)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("key can not be compared",
                         K(ret), K(assoc_array), K(sort_idx), K(key), K(*current_key),
                         K(index), K(pos), K(info));
              } else if (key < *current_key) {
                break;
              } else if (key > *current_key) {
                //do nothing
              } else{
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("key exists, but not found", K(assoc_array), K(sort_idx), K(key), K(*current_key), K(ret));
              }
              pre_idx = sort_idx;
              sort_idx = assoc_array->get_sort(sort_idx);
            }

            //重排sort数组
            if (OB_SUCC(ret)) {
              //如果是首元素，修改first指针，否则修改前项指针
              if (OB_INVALID_INDEX == pre_idx) {
                assoc_array->set_first(pos);
              } else {
                OZ (assoc_array->set_sort(pre_idx, pos - 1), K(pre_idx), K(pos - 1), K(ret));
              }
              //修改自己的后项指针
              OZ (assoc_array->set_sort(pos - 1, sort_idx), K(pos - 1), K(sort_idx), K(ret));
              //如果是末元素，修改last指针
              if (OB_SUCC(ret)) {
                OB_INVALID_INDEX == sort_idx ? assoc_array->set_last(pos) : (void)NULL;
              }
            }
          }
        } else {
          // 这儿处理一个场景，a('a') := 1; a('b'):=2; a('c'):=3; a.delete('a'); a('a'):=1;
          // 这种场景下，删除会将first，last更新一次，后面的赋值需要在这里重新更新first，last
          bool is_deleted = false;
          if (OB_FAIL(assoc_array->is_elem_deleted(index, is_deleted))) {
            LOG_WARN("failed to test element deleted.", K(ret));
          } else if (is_deleted) {
            if (!assoc_array->get_element_desc().is_composite_type()) {
              // do nothing
            } else {
              pl::ObPLExecCtx *pl_exec_ctx = session->get_pl_context()->get_current_ctx();
              const pl::ObUserDefinedType *type = NULL;
              const pl::ObCollectionType *collection_type = NULL;
              int64_t ptr = 0;
              int64_t init_size = OB_INVALID_SIZE;
              ObObj* row = assoc_array->get_data() + index;
              CK (OB_NOT_NULL(pl_exec_ctx));
              CK (OB_NOT_NULL(row));
              OZ (pl_exec_ctx->get_user_type(assoc_array->get_id(), type));
              CK (OB_NOT_NULL(type));
              CK (type->is_collection_type());
              CK (OB_NOT_NULL(collection_type = static_cast<const pl::ObCollectionType*>(type)));
              OZ (collection_type->get_element_type().newx(*assoc_array->get_allocator(), pl_exec_ctx, ptr));
              if (OB_SUCC(ret) && collection_type->get_element_type().is_collection_type()) {
                pl::ObPLCollection *collection = NULL;
                CK (OB_NOT_NULL(collection = reinterpret_cast<pl::ObPLCollection*>(ptr)));
                OX (collection->set_count(0));
              }
              OZ (collection_type->get_element_type().get_size(pl::PL_TYPE_INIT_SIZE, init_size));
              OX (row->set_extend(ptr, collection_type->get_element_type().get_type(), init_size));
            }
            if (OB_SUCC(ret)) {
              if (OB_INVALID_INDEX != assoc_array->get_first()) {
                const ObObj &cur_obj = assoc_array->get_key()[index];
                const ObObj &first = assoc_array->get_key()[assoc_array->get_first() - 1];
                if (cur_obj < first) {
                  assoc_array->set_first(index + 1);
                }
              } else {
                assoc_array->set_first(index + 1);
              }
              if (OB_INVALID_INDEX != assoc_array->get_last()) {
                const ObObj &cur_obj = assoc_array->get_key()[index];
                const ObObj &last = assoc_array->get_key()[assoc_array->get_last() - 1];
                if (cur_obj > last) {
                  assoc_array->set_last(index + 1);
                }
              } else {
                assoc_array->set_last(index + 1);
              }
            }
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
            } else if (first != last) { // 相等时, 上面的两个分中必然会进入一个，因为first和las相等
              ObObj head;
              ObObj next;
              index = fidx;
              // 确定中间值的索引
              do {
                head = assoc_array->get_key()[index];
                next = assoc_array->get_key()[assoc_array->get_sort()[index]];
                if (key >= first && key <= next) {
                  if (pl::parent_expr_type::EXPR_NEXT == type) {
                    // do nothing
                  } else {
                    index = assoc_array->get_sort()[index];
                  }
                  break;
                }
                index = assoc_array->get_sort()[index];
                cnt++;
              } while (cnt < assoc_array->get_count() - 1 && OB_INVALID_INDEX != index);
              if (cnt < assoc_array->get_count() - 1) {
                assoc_idx = index + 1;
              } else {
                ret = OB_ERROR_OUT_OF_RANGE;
                LOG_WARN("index out of range, associative array", K(key), K(*assoc_array), K(ret));
              }
            } else {
              // do nothing
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprPLAssocIndex::reserve_assoc_key(pl::ObPLAssocArray &assoc_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(assoc_array.get_allocator()) || 0 == assoc_array.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid associative array", K(assoc_array), K(ret));
  } else if (NULL != assoc_array.get_key() || NULL != assoc_array.get_sort()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid associative array to reserve", K(assoc_array), K(ret));
  } else { /*do nothing*/ }

#define RESERVE_ASSOC_ARRAY(TYPE, PROPERTY) \
  do { \
    if (OB_SUCC(ret)) { \
      TYPE *addr = static_cast<TYPE *>(assoc_array.get_allocator()->alloc(sizeof(TYPE) * assoc_array.get_count())); \
      if (OB_ISNULL(addr)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("alloc failed", K(assoc_array), K(assoc_array.get_count()), K(sizeof(TYPE)), K(ret)); \
      } else { \
        assoc_array.set_##PROPERTY(addr); \
      } \
    } \
  } while(0)

  RESERVE_ASSOC_ARRAY(ObObj, key);

  RESERVE_ASSOC_ARRAY(int64_t, sort);

  for (int64_t i = 0; OB_SUCC(ret) && i < assoc_array.get_count(); ++i) {
    assoc_array.get_key(i)->set_int32(i + 1);
    assoc_array.set_sort(i, i + 1);
  }
  assoc_array.set_sort(assoc_array.get_count() - 1, OB_INVALID_INDEX);
  assoc_array.set_first(1); //first和last从1开始
  assoc_array.set_last(assoc_array.get_count());

  return ret;
}
#endif

int ObExprPLAssocIndex::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  const auto &assoc_idx_expr = static_cast<const ObPLAssocIndexRawExpr &>(raw_expr);
  CK(2 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    Info info;
    info.for_write_ = assoc_idx_expr.get_write();
    info.out_of_range_set_err_ = assoc_idx_expr.get_out_of_range_set_err();
    info.parent_expr_type_ = assoc_idx_expr.get_parent_type();
    info.is_index_by_varchar_ = assoc_idx_expr.is_index_by_varchar();

    rt_expr.extra_ = info.v_;
    rt_expr.eval_func_ = &eval_assoc_idx;
  }
  return ret;
};

int ObExprPLAssocIndex::eval_assoc_idx(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(expr, ctx, expr_datum);
#else
  ObDatum *array;
  ObDatum *key;
  ObObj key_obj;
  Info info;
  info.v_ = expr.extra_;

  OZ(expr.eval_param_value(ctx, array, key));
  OZ(key->to_obj(key_obj, expr.args_[1]->obj_meta_, expr.args_[1]->obj_datum_map_));

  if (OB_SUCC(ret)
      && key_obj.is_null()
      && pl::parent_expr_type::EXPR_UNKNOWN == info.parent_expr_type_) {
    ObString msg("NULL index table key value");
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, msg.length(), msg.ptr());
  }
  if (OB_FAIL(ret)) {
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
    pl::ObPLAssocArray *assoc_array = reinterpret_cast<pl::ObPLAssocArray *>(
        array->extend_obj_->get_ext());
    CK(NULL != assoc_array);
    int64_t assoc_idx = 0;
    OZ(do_eval_assoc_index(assoc_idx, ctx.exec_ctx_, info, *assoc_array, key_obj));
    if (OB_SUCC(ret)) {
      expr_datum.set_int(assoc_idx);
    }
  }
#endif
  return ret;
}

}  // namespace sql
}  // namespace oceanbase


