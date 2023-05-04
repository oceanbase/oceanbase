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
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"
#include "pl/ob_pl.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprObjAccess::ExtraInfo::ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
    get_attr_func_(0),
    param_idxs_(alloc),
    access_idx_cnt_(0),
    for_write_(false),
    property_type_(pl::ObCollectionType::INVALID_PROPERTY),
    coll_idx_(OB_INVALID_INDEX),
    extend_size_(0)
{
}

OB_SERIALIZE_MEMBER(ObExprObjAccess::ExtraInfo,
                    get_attr_func_,
                    param_idxs_,
                    access_idx_cnt_,
                    for_write_,
                    property_type_,
                    coll_idx_,
                    extend_size_);

OB_SERIALIZE_MEMBER((ObExprObjAccess, ObExprOperator),
                    info_.get_attr_func_,
                    info_.param_idxs_,
                    info_.access_idx_cnt_,
                    info_.for_write_,
                    info_.property_type_,
                    info_.coll_idx_);
                    // extend_size_ is not needed here, we got extend size from result_type_

ObExprObjAccess::ObExprObjAccess(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_OBJ_ACCESS_REF, N_OBJ_ACCESS, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                   INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE),
    info_(alloc, T_OBJ_ACCESS_REF)
{
}

ObExprObjAccess::~ObExprObjAccess()
{
}

void ObExprObjAccess::ExtraInfo::reset()
{
  get_attr_func_ = 0;
  param_idxs_.reset();
  access_idx_cnt_ = 0;
  for_write_ = false;
  property_type_  = pl::ObCollectionType::INVALID_PROPERTY;
  coll_idx_ = OB_INVALID_INDEX;
  extend_size_ = 0;
}

void ObExprObjAccess::reset()
{
  info_.reset();
  ObExprOperator::reset();
}

int ObExprObjAccess::ExtraInfo::deep_copy(common::ObIAllocator &allocator,
                               const ObExprOperatorType type,
                               ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ExtraInfo &other = *static_cast<ExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    OZ(other.assign(*this));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::assign(const ObExprObjAccess::ExtraInfo &other)
{
  int ret = OB_SUCCESS;
  get_attr_func_ = other.get_attr_func_;
  access_idx_cnt_ = other.access_idx_cnt_;
  for_write_ = other.for_write_;
  property_type_ = other.property_type_;
  coll_idx_ = other.coll_idx_;
  extend_size_ = other.extend_size_;
  OZ(param_idxs_.assign(other.param_idxs_));
  return ret;
}

int ObExprObjAccess::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OBJ_ACCESS_REF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprObjAccess &other_expr = static_cast<const ObExprObjAccess &>(other);
    OZ(info_.assign(other_expr.info_));
  }
  return ret;
}

#define GET_VALID_INT64_PARAM(obj) \
  do { \
    if (OB_SUCC(ret)) { \
      int64_t param_value = 0; \
      if (obj.is_integer_type() || obj.is_ext()) { \
        param_value = obj.get_int(); \
      } else if (obj.is_number()) { \
        if (!obj.get_number().is_valid_int64(param_value)) { \
          number::ObNumber number = obj.get_number(); \
          if (OB_FAIL(number.round(0))) { \
            LOG_WARN("failed to round number", K(ret), K(number)); \
          } else if (!number.is_valid_int64(param_value)) { \
            ret = OB_ARRAY_OUT_OF_RANGE; \
            LOG_WARN("array index is out of range", K(ret), K(number)); \
          } \
        } \
      } else if (obj.is_null()) { \
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR; \
        LOG_WARN("ORA-06502: PL/SQL: numeric or value error: NULL index table key value",\
                 K(ret), K(obj), K(i)); \
      } else { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN("obj param is invalid type", K(obj), K(i)); \
      } \
      if (OB_SUCC(ret) && OB_FAIL(param_array.push_back(param_value))) { \
        LOG_WARN("store param array failed", K(ret), K(i)); \
      } \
    } \
  } while (0)

int ObExprObjAccess::ExtraInfo::init_param_array(const ParamStore &param_store,
                                                 const ObObj *objs_stack,
                                                 int64_t param_num,
                                                 ParamArray &param_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_idxs_.count(); ++i) {
    CK (param_idxs_.at(i) >= 0 && param_idxs_.at(i) < param_store.count());
    if (OB_SUCC(ret)) {
      const ObObjParam &obj_param = param_store.at(param_idxs_.at(i));
      GET_VALID_INT64_PARAM(obj_param);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    const ObObj &obj = objs_stack[i];
    GET_VALID_INT64_PARAM(obj);
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::update_coll_first_last(
  const ParamStore &param_store, const ObObj *objs_stack, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  bool found = false;

#define SEARCH_AND_UPDATE_COLL(get_elem, count) \
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < count; ++i) { \
    const ObObj &obj = get_elem; \
    if (obj.is_pl_extend() \
        && (pl::PL_NESTED_TABLE_TYPE == obj.get_meta().get_extend_type() \
            || pl::PL_VARRAY_TYPE == obj.get_meta().get_extend_type())) { \
      found = true; \
      pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection *>(obj.get_ext()); \
      CK (OB_NOT_NULL(coll)); \
      OX (coll->set_first(OB_INVALID_INDEX)); \
      OX (coll->set_last(OB_INVALID_INDEX)); \
      OX (found = true); \
    } \
  }

  SEARCH_AND_UPDATE_COLL(objs_stack[i], param_num);
  SEARCH_AND_UPDATE_COLL(param_store.at(i), param_store.count());

#undef SEARCH_AND_UPDATE_COLL
  return ret;
}

int ObExprObjAccess::calc_result(ObObj &result,
                                 const ObObj *objs_stack,
                                 int64_t param_num,
                                 const ParamStore &param_store) const
{
  return info_.calc(result,
                    get_result_type(),
                    get_result_type().get_extend_size(),
                    param_store,
                    objs_stack,
                    param_num);
}
int ObExprObjAccess::ExtraInfo::calc(ObObj &result,
                                     const ObObjMeta &res_type,
                                     const int32_t extend_size,
                                     const ParamStore &param_store,
                                     const common::ObObj *params,
                                     int64_t param_num) const
{
  int ret = OB_SUCCESS;
  typedef int32_t (*GetAttr)(int64_t, int64_t [], int64_t *);
  GetAttr get_attr = reinterpret_cast<GetAttr>(get_attr_func_);
  ParamArray param_array;
  CK (OB_NOT_NULL(get_attr));
  OZ (init_param_array(param_store, params, param_num, param_array));

  if (OB_SUCC(ret)) {
    int64_t *param_ptr = const_cast<int64_t *>(param_array.head());
    int64_t attr_addr = 0;
    OZ (get_attr(param_array.count(), param_ptr, &attr_addr));
    if (OB_FAIL(ret)) {
      if (OB_NOT_INIT == ret && pl::ObCollectionType::EXISTS_PROPERTY == property_type_) {
        ret = OB_SUCCESS;
        result.set_tinyint(0);
      }
    } else if (OB_UNLIKELY(0 >= attr_addr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get attribute failed", K(ret), K(attr_addr));
    } else if (for_write_ || res_type.is_ext()) {
      // 当对collection元素赋值的时候，强制设置collection的first和last为invalid
      // 在获取first，last的时候，会根据这个标记进行更新。
      // 主要是因为collection有删除操作的时候，而又赋值了，需要一个地方给出提示
      // 这儿就是一个标记，需要明确的，这可能会导致first last暂时不准确。但get_first会自动更新
      // 所有需要使用first，last的地方应该调用get_first这样的函数，而不能直接使用first的值
      // assoc array 不需要处理，因为在accociative_index里面更新了first，last
      if (for_write_ && OB_INVALID_INDEX != coll_idx_) {
        if (0 != param_array.at(coll_idx_)) {
          pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection*>(param_array.at(coll_idx_));
          if (pl::PL_NESTED_TABLE_TYPE == coll->get_type()
              || pl::PL_VARRAY_TYPE == coll->get_type()) {
            OX (coll->set_first(OB_INVALID_INDEX));
            OX (coll->set_last(OB_INVALID_INDEX));
          }
        }
      }
      OX(result.set_extend(attr_addr, res_type.get_extend_type(), extend_size));
    } else {
      ObObj *datum = reinterpret_cast<ObObj*>(attr_addr);
      if (ObMaxType == datum->get_type()) { //means has been deleted
        ret = OB_READ_NOTHING;
        LOG_WARN("accessing deleted element, no data found", K(ret), KPC(datum), K(result));
      } else if (OB_FAIL(result.apply(*datum))) {
        LOG_WARN("apply failed", K(ret), KPC(datum), K(result), K(res_type));
      }

      if (OB_SUCC(ret)) {
        if ((ObLongTextType == result.get_meta().get_type()
            && res_type.is_lob_locator())
        || (result.get_meta().is_lob_locator() && ObLongTextType == res_type.get_type())) {
        } else if (!result.is_null()
                  && result.get_meta().get_type() != res_type.get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("obj access result meta not equel to expr type",
            K(ret), KPC(datum), K(result), K(res_type));
        }
      }
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::from_raw_expr(const ObObjAccessRawExpr &raw_access)
{
  int ret = 0;
  CK(0 != raw_access.get_get_attr_func_addr());
  if (OB_SUCC(ret)) {
    extend_size_ = raw_access.get_result_type().get_extend_size();
    get_attr_func_ = raw_access.get_get_attr_func_addr();
    for_write_ = raw_access.for_write();
    property_type_ = raw_access.get_property();
    access_idx_cnt_ = raw_access.get_access_idxs().count();
    coll_idx_ = OB_INVALID_INDEX;
    if (raw_access.get_access_idxs().at(0).elem_type_.is_collection_type()) {
      coll_idx_ = raw_access.get_access_idxs().at(0).var_index_;
    }
    OZ(param_idxs_.init(raw_access.get_var_indexs().count()));
    OZ(param_idxs_.assign(raw_access.get_var_indexs()));
  }
  return ret;
}

int ObExprObjAccess::cg_expr(ObExprCGCtx &op_cg_ctx,
                             const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  ExtraInfo *info = OB_NEWx(ExtraInfo, (&alloc), alloc, T_OBJ_ACCESS_REF);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    const ObObjAccessRawExpr &raw_access = static_cast<const ObObjAccessRawExpr &>(raw_expr);
    CK(0 != raw_access.get_get_attr_func_addr());
    if (OB_SUCC(ret)) {
      info->extend_size_ = raw_expr.get_result_type().get_extend_size();
      info->get_attr_func_ = raw_access.get_get_attr_func_addr();
      info->for_write_ = raw_access.for_write();
      info->property_type_ = raw_access.get_property();
      info->access_idx_cnt_ = raw_access.get_access_idxs().count();
      int64_t coll_idx = OB_INVALID_INDEX;
      if (raw_access.get_access_idxs().at(0).elem_type_.is_collection_type()) {
        coll_idx = raw_access.get_access_idxs().at(0).var_index_;
      }
      info->coll_idx_ = coll_idx;
      OZ(info->param_idxs_.init(raw_access.get_var_indexs().count()));
      OZ(info->param_idxs_.assign(raw_access.get_var_indexs()));
    }
    if (OB_SUCC(ret)) {
      rt_expr.extra_info_ = info;
      rt_expr.eval_func_ = eval_obj_access;
    }
  }
  return ret;
}

int ObExprObjAccess::eval_obj_access(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const ExtraInfo *info = static_cast<const ExtraInfo *>(expr.extra_info_);
  const ParamStore &param_store = ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
  OZ(expr.eval_param_value(ctx));
  ObObj params[expr.arg_cnt_];
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    const ObExpr *e = expr.args_[i];
    const ObDatum &d = e->locate_expr_datum(ctx);
    OZ(d.to_obj(params[i], e->obj_meta_, e->obj_datum_map_));
  }

  ObObj result;
  OZ(info->calc(result,
                expr.obj_meta_,
                info->extend_size_,
                param_store,
                params,
                expr.arg_cnt_));

  OZ(expr_datum.from_obj(result, expr.obj_datum_map_));
  if (is_lob_storage(result.get_type())) {
    OZ (ob_adjust_lob_datum(result, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), expr_datum));
  }
  return ret;
}



}  // namespace sql
}  // namespace oceanbase
