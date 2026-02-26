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
#include "sql/engine/expr/ob_expr_multiset.h"
#include "src/pl/ob_pl.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObExprCollPred, ObExprOperator),
                    ms_type_,
                    ms_modifier_);

typedef hash::ObHashMap<ObObj, int64_t, common::hash::NoPthreadDefendMode> LocalNTSHashMap;

static const ObString MEMBER_OF_EXPR_NAME = ObString("MEMBER OF");

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
  int64_t count = coll->get_actual_count(); \
  if (OB_FAIL(map_name.create(std::max(count, 1L), ObModIds::OB_SQL_HASH_SET))) { \
    LOG_WARN("fail create hash map", K(count), K(ret)); \
  } else if (0 == count) { \
    /* do nothing */ \
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
        lib::ob_sort(first, first + c1_cnt, uc);
        first = &c2_copy.at(0);
        lib::ob_sort(first, first + c2_cnt, uc);
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

int ObExprCollPred::calc_is_submultiset(ObExecContext &exec_ctx,
                                        const ObObj &obj1,
                                        const ObObj &obj2,
                                        CollectionPredRes &result)
{
  int ret = OB_SUCCESS;

  static const ObString EXPR_NAME = "SUBMULTISET";

  // LHS = common_part(C) + unique_part(Ul) + null_part(Nl)
  // RHS = common_part(C) + unique_part(Ur) + null_part(Nr)
  // assume both LHS and RHS are not empty
  // (LHS or RHS is NULL or empty is handled by C++ code)
  // LHS submultiset RHS =
  //    TRUE: if and only if |Ul| = 0 and |Nl| = 0 <=> |LHS| = |C|
  //    FALSE: if and only if some element in LHS can't be mapped to RHS, which is |Ul| > |Nr|
  //    NULL: otherwise, we have |Ul| <= |Nr|, so all elements in LHS can be mapped to RHS, and the result is NULL
  static constexpr char SUBMULTISET_PL[] =
      "declare\n"
      "cmp boolean := :result;\n"
      "lhs_count pls_integer := :lhs.count;\n"
      "rhs_count pls_integer := :rhs.count;\n"
      "lhs_null pls_integer := 0;\n"  // Nl
      "rhs_null pls_integer := 0;\n"  // Nr
      "matched pls_integer := 0;\n"  // C
      "null_reuslt boolean := NULL;\n"
      "begin\n"
      "for j in :rhs.first..:rhs.last loop\n"
      "  if :rhs.exists(j) and (:rhs(j)=:rhs(j)) is null then\n"
      "    rhs_null := rhs_null + 1;\n"
      "    :rhs.delete(j);\n"
      "  end if;\n"
      "end loop;\n"
      "for i in :lhs.first..:lhs.last loop\n"
      "  if not :lhs.exists(i) then continue; end if;\n"
      "  if (:lhs(i)=:lhs(i)) is null then\n"
      "    lhs_null := lhs_null + 1;\n"
      "    continue;\n"
      "  end if;\n"
      "  for j in :rhs.first..:rhs.last loop\n"
      "    if not :rhs.exists(j) then continue; end if;\n"
      "    if :lhs(i) = :rhs(j) then\n"
      "      matched := matched + 1;\n"
      "      :rhs.delete(j);\n"
      "      exit;\n"
      "    end if;\n"
      "  end loop;\n"
      "end loop;\n"
      "\n"
      "if lhs_count = matched then\n"
      "  :result := TRUE;\n"
      "elsif lhs_count - matched - lhs_null > rhs_null then\n"
      "  :result := FALSE;\n"
      "else\n"
      "  :result := null_reuslt;\n"
      "end if;\n"
      "end;";

  pl::ObPLComposite *lhs = nullptr;
  pl::ObPLComposite *rhs = nullptr;

  if (!obj1.is_ext() || !obj2.is_ext()) {
    ret = OB_ERR_CALL_WRONG_ARG;

    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, EXPR_NAME.length(), EXPR_NAME.ptr());
    LOG_WARN("failed to calc_is_submultiset",
             K(ret), K(obj1), K(obj2));
  } else {
    lhs = reinterpret_cast<pl::ObPLComposite*>(obj1.get_ext());
    rhs = reinterpret_cast<pl::ObPLComposite*>(obj2.get_ext());
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL operand for SUBMULTISET",
             K(ret), KPC(lhs), KPC(rhs), K(obj1), K(obj2));
  } else if (pl::PL_NESTED_TABLE_TYPE != lhs->get_type()
               || pl::PL_NESTED_TABLE_TYPE != rhs->get_type()
               || lhs->get_id() != rhs->get_id()) {
    ret = OB_ERR_CALL_WRONG_ARG;

    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, EXPR_NAME.length(), EXPR_NAME.ptr());
    LOG_WARN("failed to calc_is_submultiset",
             K(ret), KPC(lhs), KPC(rhs), K(obj1), K(obj2));
  } else {
    pl::ObPLCollection *left = static_cast<pl::ObPLCollection*>(lhs);
    pl::ObPLCollection *right = static_cast<pl::ObPLCollection*>(rhs);

    if (0 == left->get_actual_count() && !left->is_collection_null()) {
      result = COLL_PRED_TRUE;
    } else if (left->is_collection_null() || right->is_collection_null()) {
      result = COLL_PRED_NULL;
    } else if (left->get_actual_count() > right->get_actual_count()) {
      result = COLL_PRED_FALSE;
    } else {
      ObObj res;
      res.set_bool(false);

      ObArenaAllocator alloc;
      ParamStore params((ObWrapperAllocator(alloc)));
      ObBitSet<> out_args;

      bool tmp_result = false;

      ObObj obj2_copy;
      DEFER(pl::ObUserDefinedType::destruct_obj(obj2_copy));

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(alloc, obj2, obj2_copy))) {
        LOG_WARN("failed to deep_copy_obj", K(ret), K(obj2), K(obj2_copy));
      } else if (OB_FAIL(params.push_back(res))) {
        LOG_WARN("failed to push back result",
                 K(ret), K(res), K(params));
      } else if (FALSE_IT(params.at(0).set_param_meta())) {
        // unreachable
      } else if (OB_FAIL(params.push_back(obj1))) {
        LOG_WARN("failed to push back coll",
                 K(ret), K(obj1), KPC(left), K(params));
      } else if (FALSE_IT(params.at(1).set_udt_id(left->get_id()))) {
        // unreachable
      } else if (FALSE_IT(params.at(1).set_param_meta())) {
        // unreachable
      } else if (OB_FAIL(params.push_back(obj2_copy))) {
        LOG_WARN("failed to push back val",
                  K(ret), K(obj2_copy), KPC(right), K(params));
      } else if (FALSE_IT(params.at(2).set_udt_id(right->get_id()))) {
        // unreachable
      } else if (FALSE_IT(params.at(2).set_param_meta())) {
        // unreachable
      } else if (OB_FAIL(ObExprMultiSet::eval_composite_relative_anonymous_block(exec_ctx,
                                                                                 SUBMULTISET_PL,
                                                                                 params,
                                                                                 out_args))) {
        LOG_WARN("failed to execute PS anonymous bolck",
                  K(ret), K(obj1), K(obj2_copy), KPC(left), KPC(right),
                  K(res), K(params));
      } else if (out_args.num_members() != 2 || !out_args.has_member(0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected out args",
                  K(ret), K(obj1), K(obj2_copy), KPC(left), KPC(right),
                  K(res), K(params));
      } else if (params.at(0).is_null()) {
        result = COLL_PRED_NULL;
      } else if (OB_FAIL(params.at(0).get_bool(tmp_result))) {
        LOG_WARN("failed to get result",
                  K(ret), K(obj1), K(obj2_copy), KPC(left), KPC(right),
                  K(res), K(params));
      } else {
        result = tmp_result ? COLL_PRED_TRUE : COLL_PRED_FALSE;
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
        pl::ObPLComposite *lhs = nullptr;

        if (obj1.is_null()) {
          pred_res = COLL_PRED_NULL;
        } else if (obj1.is_ext()
                     && OB_NOT_NULL(lhs = reinterpret_cast<pl::ObPLComposite*>(obj1.get_ext()))
                     && pl::PL_NESTED_TABLE_TYPE == lhs->get_type()
                     && 0 == reinterpret_cast<pl::ObPLCollection*>(lhs)->get_actual_count()) {
          pred_res = COLL_PRED_TRUE;
        } else if (obj2.is_null()) {
          pred_res = COLL_PRED_NULL;
        } else if (OB_FAIL(calc_is_submultiset(ctx.exec_ctx_, obj1, obj2, pred_res))) {
          LOG_WARN("failed to call submultiset", K(ret));
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (COLL_PRED_NULL == pred_res) {
          result.set_null();
        } else {
          if (pred_res ^ (MULTISET_MODIFIER_NOT == info->ms_modifier_)) {
            result.set_tinyint(1);
          } else {
            result.set_tinyint(0);
          }
        }
      }
        break;
      case MULTISET_TYPE_MEMBER_OF: {
        if (!obj2.is_pl_extend()
              || (pl::PL_NESTED_TABLE_TYPE != obj2.get_meta().get_extend_type()
                    && pl::PL_VARRAY_TYPE != obj2.get_meta().get_extend_type()
                    && pl::PL_ASSOCIATIVE_ARRAY_TYPE != obj2.get_meta().get_extend_type())) {
          ret = OB_ERR_CALL_WRONG_ARG;

          LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG,
                         MEMBER_OF_EXPR_NAME.length(), MEMBER_OF_EXPR_NAME.ptr());
          LOG_WARN("failed to eval MEMBER OF", K(ret), K(obj2));
        } else {
          pl::ObPLCollection *c2 = reinterpret_cast<pl::ObPLCollection *>(obj2.get_ext());
          if (OB_NOT_NULL(c2)) {
            if (ObTinyIntType == obj1.get_type()
                  && ObTinyIntType != c2->get_element_type().get_obj_type()) {
              ret = OB_ERR_CALL_WRONG_ARG;

              LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG,
                            MEMBER_OF_EXPR_NAME.length(), MEMBER_OF_EXPR_NAME.ptr());
              LOG_WARN("failed to eval MEMBER OF, BOOLEAN cannot convert to other types", K(ret), K(obj1), K(obj2));
            } else if (c2->is_of_composite()) {
              ObObj res;
              bool tmp_result = false;

              if (OB_FAIL(eval_member_of_composite(ctx.exec_ctx_, obj1, obj2, res))) {
                LOG_WARN("failed to eval_member_of_composite", K(ret), K(obj1), K(obj2), K(res));
              } else if (res.is_null()) {
                result.set_null();
              } else if (OB_FAIL(res.get_bool(tmp_result))){
                LOG_WARN("failed to get bool result from res", K(ret), K(res), K(tmp_result));
              } else if (MULTISET_MODIFIER_NOT == info->ms_modifier_) {
                result.set_bool(!tmp_result);
              } else {
                result.set_bool(tmp_result);
              }
            } else if (c2->is_collection_null()) {
              result.set_null();
            } else if (0 == c2->get_actual_count()) {
              result.set_bool(MULTISET_MODIFIER_NOT == info->ms_modifier_);
            } else if (obj1.is_null()) {
              result.set_null();
            } else {
              ObObj mem_cast;
              const ObObj *res_obj1 = &obj1;
              ObCollationType cast_coll_type = c2->get_element_type().get_collation_type();
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
              } else if (OB_FAIL(ObObjCaster::to_type(c2->get_element_type().get_obj_type(),
                                                      cast_ctx,
                                                      obj1,
                                                      mem_cast,
                                                      res_obj1))) {
                LOG_WARN("failed to cast member to collection elem type", K(ret));
              } else {
                LocalNTSHashMap dmap_c;
                int64_t res_cnt = 0;
                FILL_HASH_MAP(dmap_c, c2, res_cnt);
                if (OB_SUCC(ret)) {
                  if (OB_NOT_NULL(dmap_c.get(*res_obj1))) {
                    result.set_bool(MULTISET_MODIFIER_NOT != info->ms_modifier_);
                  } else {
                    bool has_null = false;

                    for (int64_t i = 0; OB_SUCC(ret) && i < c2->get_count(); ++i) {
                      ObObj *curr = c2->get_data() + i;

                      CK (OB_NOT_NULL(curr));

                      if (OB_SUCC(ret) && curr->is_null()) {
                        has_null = true;
                        break;
                      }
                    }

                    if (OB_FAIL(ret)) {
                      // do nothing
                    } else if (has_null) {
                      result.set_null();
                    } else {
                      result.set_bool(MULTISET_MODIFIER_NOT == info->ms_modifier_);
                    }
                  }
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
            ObObj res;
            bool tmp_result;

            if (c1->is_collection_null()) {
              result.set_null();
            } else if (c1->is_of_composite()) {
              if (OB_FAIL(eval_is_set_composit(ctx.exec_ctx_, obj1, res))) {
                LOG_WARN("failed to eval_is_set_composit", K(ret), K(obj1), K(res));
              } else if (OB_FAIL(res.get_bool(tmp_result))) {
                LOG_WARN("failed to get bool result", K(ret), K(res));
              } else {
                result.set_bool(MULTISET_MODIFIER_NOT == info->ms_modifier_
                                    ? !tmp_result
                                    : tmp_result);
              }
            } else {
              LocalNTSHashMap dmap_c;
              int64_t count = c1->get_actual_count();
              int res_cnt = 0;
              if (0 == count) {
                // empty nest table is a set
                result.set_tinyint(MULTISET_MODIFIER_NOT != info->ms_modifier_);
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

int ObExprCollPred::eval_member_of_composite(ObExecContext &exec_ctx,
                                             const ObObj &val,
                                             const ObObj &coll,
                                             ObObj &res)
{
  int ret = OB_SUCCESS;

  static constexpr char MEMBER_OF_PL[] =
      "declare\n"
      "coll_count pls_integer := :coll.count;\n"
      "cmp boolean;\n"
      "null_result boolean := NULL;\n"
      "begin\n"
      "if (:val = :val) is null then\n"
      "  :result := null_result;\n"
      "  return;\n"
      "end if;\n"
      ":result := FALSE;\n"
      "for idx in :coll.first..:coll.last loop\n"
      "  if not :coll.exists(idx) then continue; end if;\n"
      "  if :coll(idx) is NULL then\n"
      "    :result := null_result;\n"
      "    continue;\n"
      "  end if;\n"
      "  cmp := (:val = :coll(idx));\n"
      "  if cmp then\n"
      "    :result := TRUE;\n"
      "    exit;\n"
      "  elsif :result is not NULL then\n"
      "    :result := cmp;\n"
      "  end if;\n"
      "end loop;\n"
      "end;";

  pl::ObPLCollection *collection = reinterpret_cast<pl::ObPLCollection *>(coll.get_ext());
  pl::ObPLComposite *value = nullptr;

  ObArenaAllocator alloc;
  ParamStore params((ObWrapperAllocator(alloc)));
  ObBitSet<> out_args;

  ObObj result;
  result.set_bool(false);

  bool tmp_result = false;

  CK (OB_NOT_NULL(exec_ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(collection));

  // 1st, check if the collection is null, if so, the result is NULL
  // 2nd, check if the collection is empty, if so, the result is FALSE
  // 3rd, check if the val is null, if so, the result is NULL
  // otherwise, iterate the collection
  // this order is important
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (collection->is_collection_null()) {
    res.set_null();
  } else if (0 == collection->get_actual_count()) {
    // Oracle Doc: The return value is NULL if expr is null or if the nested table is empty.
    // but actually, if the collection is empty, then the result is alway FALSE in Oracle 19c
    res.set_bool(false);
  } else if (val.is_null()) {
    res.set_null();
  } else if (!val.is_ext() ||
        OB_ISNULL(value = reinterpret_cast<pl::ObPLComposite*>(val.get_ext())) ||
        value->get_id() != collection->get_element_type().get_udt_id()) {
    ret = OB_ERR_CALL_WRONG_ARG;

    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG,
                   MEMBER_OF_EXPR_NAME.length(), MEMBER_OF_EXPR_NAME.ptr());
    LOG_WARN("failed to eval MEMBER OF", K(ret), K(val), K(coll));
  } else {
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(params.push_back(coll))) {
      LOG_WARN("failed to push back coll",
                K(ret), K(val), K(coll), K(params));
    } else if (FALSE_IT(params.at(0).set_udt_id(collection->get_id()))) {
      // unreachable
    } else if (FALSE_IT(params.at(0).set_param_meta())) {
      // unreachable
    } else if (OB_FAIL(params.push_back(val))) {
      LOG_WARN("failed to push back val",
              K(ret), K(val), K(coll), K(params));
    } else if (FALSE_IT(params.at(1).set_udt_id(value->get_id()))) {
      // unreachable
    } else if (FALSE_IT(params.at(1).set_param_meta())) {
      // unreachable
    } else if (OB_FAIL(params.push_back(result))) {
      LOG_WARN("failed to push back result",
              K(ret), K(val), K(coll), K(result), K(params));
    } else if (FALSE_IT(params.at(2).set_param_meta())) {
      // unreachable
    } else if (OB_FAIL(ObExprMultiSet::eval_composite_relative_anonymous_block(exec_ctx,
                                                                              MEMBER_OF_PL,
                                                                              params,
                                                                              out_args))) {
      LOG_WARN("failed to execute PS anonymous bolck", K(ret), K(val), K(coll), K(params));
    } else if (out_args.num_members() != 1 || !out_args.has_member(2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected out args",
              K(ret), K(val), K(coll), K(params), K(out_args));
    } else if (params.at(2).is_null()) {
      res.set_null();
    } else if (OB_FAIL(params.at(2).get_bool(tmp_result))) {
      LOG_WARN("failed to get result",
              K(ret), K(val), K(coll), K(params), K(result), K(res));
    } else {
      res.set_bool(tmp_result);
    }
  }

  return ret;
}

int ObExprCollPred::eval_is_set_composit(ObExecContext &exec_ctx,
                                         const ObObj &obj,
                                         ObObj &res)
{
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_ORACLE_PL

  static constexpr char IS_SET_PL[] =
      "declare\n"
      "has_null boolean := FALSE;\n"
      "cmp boolean;\n"
      "begin\n"
      ":result := TRUE;\n"
      "if :coll.count > 0 then\n"
      "for i in :coll.first..:coll.last loop\n"
      "  if not :coll.exists(i) then continue; end if;\n"
      "  cmp := :coll(i) member of :buf;\n"
      "  if cmp then\n"
      "    :result := FALSE;\n"
      "    exit;\n"
      "  elsif (:coll(i) = :coll(i)) is null then\n"
      "    if has_null then\n"
      "       :result := FALSE;\n"
      "       exit;\n"
      "    else\n"
      "      has_null := TRUE;\n"
      "    end if;\n"
      "  else\n"
      "    :buf.extend(1);\n"
      "    :buf(:buf.count) := :coll(i);\n"
      "  end if;\n"
      "end loop;\n"
      "end if;\n"
      "end;";

  pl::ObPLCollection *collection = reinterpret_cast<pl::ObPLCollection *>(obj.get_ext());

  ObArenaAllocator alloc;
  ParamStore params((ObWrapperAllocator(alloc)));

  ObObj result;
  result.set_bool(false);

  ObObj buf_obj;
  pl::ObPLNestedTable buf;
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(pl::PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  pl::ObPLAllocator1 tmp_coll_alloc(pl::PL_MOD_IDX::OB_PL_COLLECTION, &tmp_alloc);
  OZ (tmp_coll_alloc.init(nullptr));

  CK (OB_NOT_NULL(GCTX.pl_engine_));
  CK (OB_NOT_NULL(exec_ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(collection));
  CK (collection->is_nested_table());
  OX (buf.set_id(collection->get_id()));
  OX (buf.set_allocator(&tmp_coll_alloc));
  OX (buf.set_element_desc(collection->get_element_desc()));
  OX (buf.set_inited());
  OX (buf_obj.set_extend(reinterpret_cast<int64_t>(&buf), obj.get_meta().get_extend_type()));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(params.push_back(result))) {
    LOG_WARN("failed to push back result obj", K(ret), K(obj), K(params));
  } else if (OB_FAIL(params.push_back(obj))) {
    LOG_WARN("failed to push back collection obj", K(ret), K(result), K(params));
  } else if (OB_FAIL(params.push_back(buf_obj))) {
    LOG_WARN("failed to push back buf obj", K(ret), K(result), K(params));
  } else {
    params.at(0).set_param_meta();

    params.at(1).set_udt_id(collection->get_id());
    params.at(1).set_param_meta();

    params.at(2).set_udt_id(collection->get_id());
    params.at(2).set_param_meta();
  }

  if (OB_SUCC(ret)) {
    ObBitSet<> out_args;

    if (OB_FAIL(ObExprMultiSet::eval_composite_relative_anonymous_block(exec_ctx,
                                                                        IS_SET_PL,
                                                                        params,
                                                                        out_args))) {
      LOG_WARN("failed to execute PS anonymous bolck", K(ret), K(obj), K(params));
    } else if (out_args.num_members() != 2 || !out_args.has_member(0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected out args",
               K(ret), K(obj), K(params), K(out_args));
    } else {
      res = params.at(0);
    }
  }

  int temp_ret = pl::ObUserDefinedType::destruct_obj(params.at(2));
  if (OB_SUCCESS != temp_ret) {
    LOG_WARN("failed to destruct obj", K(temp_ret));
  }
  ret = OB_SUCCESS == ret ? temp_ret : ret;

#endif // OB_BUILD_ORACLE_PL

  return ret;
}

} // namespace sql
}  // namespace oceanbase
