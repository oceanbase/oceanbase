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
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/hash/ob_hashmap.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_allocator.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObExprCollPred, ObExprOperator),
                    ms_type_,
                    ms_modifier_);

typedef hash::ObHashMap<ObObj, int64_t, common::hash::NoPthreadDefendMode> LocalNTSHashMap;

ObExprCollPred::ObExprCollPred(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_OP_COLL_PRED, N_COLL_PRED, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
    ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID),
    ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID)
{
}

ObExprCollPred::~ObExprCollPred()
{
}

void ObExprCollPred::reset()
{
  ms_type_ = ObMultiSetType::MULTISET_TYPE_INVALID;
  ms_modifier_ = ObMultiSetModifier::MULTISET_MODIFIER_INVALID;
  ObExprOperator::reset();
}

int ObExprCollPred::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_COLL_PRED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprCollPred &other_expr = static_cast<const ObExprCollPred &>(other);
    ms_type_ = other_expr.ms_type_;
    ms_modifier_ = other_expr.ms_modifier_;
  }
  return ret;
}

int ObExprCollPred::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    type.set_type(ObTinyIntType);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("multiset operator not support non udt type", K(type1), K(type2), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiset operator non udt type");
  }

  return ret;
}

#define FILL_HASH_MAP(map_name, coll, map_cnt) \
do { \
  if (OB_FAIL(map_name.create(coll->get_actual_count(), ObModIds::OB_SQL_HASH_SET))) { \
    LOG_WARN("fail create hash map", K(coll->get_actual_count()), K(ret)); \
  } else { \
    const ObObj *elem = NULL; \
    bool is_del = false; \
    ObObj *data_arr = static_cast<ObObj*>(coll->get_data()); \
    for (int64_t i = 0; OB_SUCC(ret) && i < coll->get_count(); ++i) {\
      if (OB_FAIL(coll->is_elem_deleted(i, is_del))) { \
        LOG_WARN("failed to check is collection elem deleted", K(ret)); \
      } else if (!is_del) { \
        elem = data_arr + i;\
        ret = map_name.set_refactored(*elem, i);\
        if (OB_HASH_EXIST == ret) {\
          ret = OB_SUCCESS;\
          continue;\
        } else if (OB_SUCC(ret)) {\
          map_cnt++;\
        } else {\
          LOG_WARN("insert elem into hashmap failed.", K(ret));\
        }\
      } \
    }\
  }\
} while(0)

int ObExprCollPred::compare_obj(const ObObj &obj1, const ObObj &obj2, ObCompareCtx &cmp_ctx) {
  UNUSED(cmp_ctx);
  return obj1 == obj2 ? 0 : 1;
}

int ObExprCollPred::calc_collection_is_contained_without_null(const pl::ObPLCollection *c1,
                                                     const pl::ObPLCollection *c2,
                                                     int64_t tz_offset,
                                                     const ObExprCalcType calc_type,
                                                     CollectionPredRes &result)
{
  int ret = OB_SUCCESS;
  obj_cmp_func lt_cmp_fp;
  const ObObjType type1 = c1->get_element_type().get_obj_type();
  const ObObjType type2 = c2->get_element_type().get_obj_type();
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  if (OB_FAIL(ObObjCmpFuncs::get_cmp_func(tc1, tc2, CO_LT, lt_cmp_fp))) {
    LOG_WARN("get cmp func failed", K(type1), K(type2), K(tc1), K(tc2), K(ret));
  } else {
    ObCompareCtx cmp_ctx(calc_type.get_type(),
                       calc_type.get_collation_type(),
                       false,
                       tz_offset,
                       default_null_pos());
    struct udtComparer{
      udtComparer(ObCompareCtx &cmp_ctx, common::obj_cmp_func cmp_func) :
      cmp_ctx_(cmp_ctx),
      cmp_func_(cmp_func) {}

      bool operator()(const ObObj *&e1, const ObObj *&e2) {
        int cmpRes = cmp_func_(*e1, *e2, cmp_ctx_);
        return cmpRes;
      }
      ObCompareCtx &cmp_ctx_;
      common::obj_cmp_func cmp_func_;
    };
    udtComparer uc(cmp_ctx, lt_cmp_fp);
    common::ObArray<const ObObj *> c1_copy, c2_copy;
    bool del_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < c1->get_count(); ++i) {
      const ObObj *elem = reinterpret_cast<const ObObj*>(c1->get_data()) + i;
      CK (OB_NOT_NULL(elem));
      if (OB_SUCC(ret)) {
        if (elem->is_null()) {
        } else if (c1->is_elem_deleted(i, del_flag)) {
          LOG_WARN("failed to test if element is deleted", K(*elem), K(ret), K(i));
        } else {
          if (!del_flag) {
            OZ (c1_copy.push_back(elem));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < c2->get_count(); ++i) {
      const ObObj *elem = reinterpret_cast<const ObObj*>(c2->get_data()) + i;
      CK (OB_NOT_NULL(elem));
      if (OB_SUCC(ret)) {
        if (elem->is_null()) {
        } else if (c2->is_elem_deleted(i, del_flag)) {
          LOG_WARN("failed to test if element is deleted", K(*elem), K(ret), K(i));
        } else {
          if (!del_flag) {
            OZ (c2_copy.push_back(elem));
          }
        }
      }
    }
    int64_t c1_cnt = c1_copy.count();
    int64_t c2_cnt = c2_copy.count();
    if (c1_cnt > 0 && c2_cnt > 0) {
      result = COLL_PRED_TRUE;
      if (c1_cnt > c2_cnt) {
        result = COLL_PRED_FALSE;
      } else {
        const ObObj **first = &c1_copy.at(0);
        std::sort(first, first + c1_cnt, uc);
        first = &c2_copy.at(0);
        std::sort(first, first + c2_cnt, uc);
        for (int64_t i = 0; OB_SUCC(ret) && i < c1_copy.count(); ++i) {
          if (0 != compare_obj(*(c1_copy.at(i)), *(c2_copy.at(i)), cmp_ctx)) {
            result = COLL_PRED_FALSE;
            break;
          }
        }
      }
    } else {
      if (0 == c1_cnt && 0 == c2_cnt) {
        result = COLL_PRED_BOTH_COLL_ZERO;
      } else if (0 == c1_cnt) {
        result = COLL_PRED_FIRST_COLL_ZERO;
      } else if (0 == c2_cnt) {
        result = COLL_PRED_SECOND_COLL_ZERO;
      }
    }
  }

  return ret;
}

int ObExprCollPred::calc_is_submultiset(const ObObj &obj1,
                                     const ObObj &obj2,
                                     int64_t tz_offset,
                                     const ObExprCalcType calc_type,
                                     CollectionPredRes &result)
{
  int ret = OB_SUCCESS;
  result = COLL_PRED_FALSE;
  if (obj1.get_meta().is_ext() && obj2.get_meta().is_ext()) {
    pl::ObPLCollection *c1 = reinterpret_cast<pl::ObPLCollection *>(obj1.get_ext());
    pl::ObPLCollection *c2 = reinterpret_cast<pl::ObPLCollection *>(obj2.get_ext());
    int64_t c1_cnt = c1->get_actual_count();
    int64_t c2_cnt = c2->get_actual_count();
    // 1) return true when:
    // 1. obj1 is not null and contains no row, return true whatever obj2 is.
    // for example obj nest_type := nest_type();
    // 2. obj1 and obj2 not null, obj1 does not contain any null element, obj1 elem one-to-one
    // mapping to obj2 element.

    // 2) return null when:
    // 1. obj1 is null
    // 2. obj2 is null, and obj1 is not null and not empty.
    // 3. obj1 contain null element, and any non null is element is one-to-one mapping to obj2 elem
    // for example:  nt1(1,2,3,4,5), nt2(1,2,NULL), nt3(1,7,null); nt2 submultiset of nt1 => null
    // nt3 submultiset nt1 => false

    // 3) return false when:
    // if none of above conditions occur, return false;
    if (OB_ISNULL(c1) || OB_ISNULL(c2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("union udt failed due to null udt", K(ret), K(obj1), K(obj2));
    } else if (pl::PL_NESTED_TABLE_TYPE != c1->get_type()
              || pl::PL_NESTED_TABLE_TYPE != c2->get_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support udt union except nested table", K(ret), K(c1), K(c2));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "udt union except nested table");
    } else if ((c1->get_element_type().get_obj_type() != c2->get_element_type().get_obj_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt union failed due to uninited", K(ret));
    } else if (0 == c1_cnt && (c1->is_inited())) {
      result = COLL_PRED_TRUE;
    } else if (!c1->is_inited()) {
      result = COLL_PRED_NULL; // null
    } else if (!c2->is_inited()) {
      result = COLL_PRED_NULL;
    } else if (c1_cnt > c2_cnt) {
      result = COLL_PRED_FALSE;
    } else if (calc_collection_is_contained_without_null(c1, c2, tz_offset, calc_type, result)) {
      LOG_WARN("faile to calc multiset without null and delete val", K(ret));
    } else {
      // 去除了null和delete数据之后比较，如果是包含关系，但是c1含有null，置结果为null.
      if (COLL_PRED_TRUE == result && c1->is_contain_null_val()) {
        result = COLL_PRED_NULL;
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprCollPredInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              ms_type_,
              ms_modifier_,
              tz_offset_,
              result_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprCollPredInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              ms_type_,
              ms_modifier_,
              tz_offset_,
              result_type_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprCollPredInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              ms_type_,
              ms_modifier_,
              tz_offset_,
              result_type_);
  return len;
}

int ObExprCollPredInfo::deep_copy(common::ObIAllocator &allocator,
                         const ObExprOperatorType type,
                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprCollPredInfo &other = *static_cast<ObExprCollPredInfo *>(copied_info);
  other.ms_type_ = ms_type_;
  other.ms_modifier_ = ms_modifier_;
  other.tz_offset_ = tz_offset_;
  other.result_type_ = result_type_;
  return ret;
}

template <typename RE>
int ObExprCollPredInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  ObCollPredRawExpr &set_expr 
       = const_cast<ObCollPredRawExpr &> (static_cast<const ObCollPredRawExpr&>(raw_expr));
  ms_type_ = set_expr.get_multiset_type();
  ms_modifier_ = set_expr.get_multiset_modifier();
  return ret;
}

int ObExprCollPred::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (2 != rt_expr.arg_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong param num for coll pred", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(expr_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret)); 
  } else {
    ObIAllocator &alloc = *expr_cg_ctx.allocator_;
    const ObCollPredRawExpr &fun_sys = static_cast<const ObCollPredRawExpr &>(raw_expr);
    ObExprCollPredInfo *info = OB_NEWx(ObExprCollPredInfo, (&alloc), alloc, T_OP_COLL_PRED);
    if (NULL == info) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      OZ(info->from_raw_expr(fun_sys));
      const ObTimeZoneInfo *tz_info = get_timezone_info(expr_cg_ctx.session_);
      int64_t tz_offset = 0;
      if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
        LOG_WARN("failed to get tz offset", K(ret));
      } else {
        info->tz_offset_ = tz_offset;
        info->result_type_ = result_type_;
        rt_expr.extra_info_ = info;
        rt_expr.eval_func_ = eval_coll_pred;
      }
    }
  }
  return ret;
}

int ObExprCollPred::eval_coll_pred(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *datum1 = nullptr;
  ObDatum *datum2 = nullptr;
  ObObj result;
  ObObj obj1;
  ObObj obj2;
  const ObExprCollPredInfo *info = static_cast<ObExprCollPredInfo *>(expr.extra_info_);
  if (OB_FAIL(expr.eval_param_value(ctx, datum1, datum2))) {
    LOG_WARN("failed to eval params", K(ret));
  } else if (lib::is_oracle_mode()) {
    if (OB_FAIL(datum1->to_obj(obj1, expr.args_[0]->obj_meta_))) {
      LOG_WARN("failed to convert to obj", K(ret));
    } else if (OB_FAIL(datum2->to_obj(obj2, expr.args_[1]->obj_meta_))) {
      LOG_WARN("failed to convert to obj", K(ret));
    } else {
      switch (info->ms_type_)
      {
      case MULTISET_TYPE_SUBMULTISET: {
        CollectionPredRes pred_res = COLL_PRED_INVALID;
        if (OB_FAIL(calc_is_submultiset(obj1, obj2, info->tz_offset_,
                                        info->result_type_.get_calc_meta(), pred_res))) {
          LOG_WARN("failed to call submultiset", K(ret));
        } else {
          if (COLL_PRED_NULL == pred_res) {
            result.set_null();
          } else {
            if (pred_res ^ (MULTISET_MODIFIER_NOT == info->ms_modifier_)) {
              result.set_tinyint(1);
            } else {
              result.set_tinyint(0);
            }
          }
        }
      }
        break;
      case MULTISET_TYPE_MEMBER_OF: {
        if (obj2.get_meta().is_ext()) {
          pl::ObPLCollection *c2 = reinterpret_cast<pl::ObPLCollection *>(obj2.get_ext());
          if (OB_NOT_NULL(c2) && 0 < c2->get_actual_count()) {
            ObObj *elem = static_cast<ObObj*>(c2->get_data());
            ObObj mem_cast;
            const ObObj * res_obj1 = &obj1;
            ObCollationType cast_coll_type = elem->get_collation_type();
            ObCastMode cp_cast_mode;
            if (OB_FAIL(ObSQLUtils::get_default_cast_mode(ctx.exec_ctx_.get_my_session(),
                                                          cp_cast_mode))) {
              LOG_WARN("failed to get default cast mode", K(ret));
            }
            const ObDataTypeCastParams dtc_params 
                   = ObBasicSessionInfo::create_dtc_params(ctx.exec_ctx_.get_my_session());
            ObCastCtx cast_ctx(&ctx.exec_ctx_.get_allocator(),
                               &dtc_params,
                               get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                               cp_cast_mode,
                               cast_coll_type,
                               nullptr);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ObObjCaster::to_type(elem->get_type(), cast_ctx,
                                                    obj1, mem_cast, res_obj1))) {
              LOG_WARN("failed to cast member to collection elem type", K(ret));
            } else {
              LocalNTSHashMap dmap_c;
              int64_t res_cnt = 0;
              FILL_HASH_MAP(dmap_c, c2, res_cnt);
              if (OB_SUCC(ret)) {
                if (OB_NOT_NULL(dmap_c.get(*res_obj1)) ^ (MULTISET_MODIFIER_NOT == info->ms_modifier_)) {
                  result.set_tinyint(1);
                } else {
                  result.set_tinyint(0);
                }
              }
            }
          }
        }
      }
        break;
      case MULTISET_TYPE_IS_SET: {
        if (obj1.get_meta().is_ext()) {
          pl::ObPLCollection *c1 = reinterpret_cast<pl::ObPLCollection *>(obj1.get_ext());
          if (OB_NOT_NULL(c1)) {
            LocalNTSHashMap dmap_c;
            int64_t count = c1->get_actual_count();
            int res_cnt = 0;
            if (0 == count) {
              // empty nest table is a set
              result.set_tinyint(1);
            } else if (!c1->is_inited()) {
              result.set_null();
            } else {
              FILL_HASH_MAP(dmap_c, c1, res_cnt);
              if (OB_SUCC(ret)) {
                if ((count == res_cnt) ^ (MULTISET_MODIFIER_NOT == info->ms_modifier_)) {
                  result.set_tinyint(1);
                } else {
                  result.set_tinyint(0);
                }
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null collection structure", K(ret));
          }
        }
      }
        break;
      case MULTISET_TYPE_EMPTY: {
        if (obj1.get_meta().is_ext()) {
          pl::ObPLCollection *c1 = reinterpret_cast<pl::ObPLCollection *>(obj1.get_ext());
          int32_t pred_res = -1;
          if (OB_NOT_NULL(c1)) {
            if (c1->is_nested_table() || c1->is_varray()) {
              if (!c1->is_inited()) {
                result.set_null();
              } else if (0 < c1->get_actual_count()) {
                pred_res = 1;
              } else {
                pred_res = 0;
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support other type except nest table and varray", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "other type except nest table and varray");
            }
            if (OB_SUCC(ret) && -1 != pred_res) {
              if (pred_res ^ (MULTISET_MODIFIER_NOT == info->ms_modifier_)) {
                result.set_tinyint(0);
              } else {
                result.set_tinyint(1);
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null collection struct", K(ret));
          }
        }
      }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown collection predicate condition", K(ret));
        break;
      }
      if (OB_SUCC(ret)) {
        OZ(res.from_obj(result, expr.obj_datum_map_));
        OZ(expr.deep_copy_datum(ctx, res));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
