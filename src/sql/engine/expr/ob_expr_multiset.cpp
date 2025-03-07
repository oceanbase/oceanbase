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
#include "sql/engine/expr/ob_expr_multiset.h"
#include "src/pl/ob_pl.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObExprMultiSet, ObExprOperator),
                    ms_type_,
                    ms_modifier_);

typedef hash::ObHashMap<ObObj, int64_t, common::hash::NoPthreadDefendMode> LocalNTSHashMap;

ObExprMultiSet::ObExprMultiSet(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_OP_MULTISET, N_MULTISET, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
    ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID),
    ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID)
{
}

ObExprMultiSet::~ObExprMultiSet()
{
}

void ObExprMultiSet::reset()
{
  ms_type_ = ObMultiSetType::MULTISET_TYPE_INVALID;
  ms_modifier_ = ObMultiSetModifier::MULTISET_MODIFIER_INVALID;
  ObExprOperator::reset();
}

int ObExprMultiSet::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_MULTISET) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprMultiSet &other_expr = static_cast<const ObExprMultiSet &>(other);
    ms_type_ = other_expr.ms_type_;
    ms_modifier_ = other_expr.ms_modifier_;
  }
  return ret;
}

int ObExprMultiSet::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (type1.is_ext() && type2.is_ext()) {
      type.set_ext();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      type.set_calc_type(type1.get_calc_type());
      type.set_result_flag(NOT_NULL_FLAG);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("multiset operator not support non udt type", K(type1), K(type2), K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this function in MySql mode");
    LOG_WARN("multiset operator not support non udt type", K(type1), K(type2), K(ret));
  }

  return ret;
}

  #define FILL_ELEM(ca, dst, offset, allocator) \
  do { \
    const ObObj *elem = NULL; \
    int64_t i = 0; \
    int64_t cnt = 0; \
    for (; OB_SUCC(ret) && i < ca->get_count(); ++i) { \
      bool is_del = false; \
      if (OB_FAIL(ca->is_elem_deleted(i, is_del))) { \
        LOG_WARN("failed to test collection elem is deleted", K(ret)); \
      } else if (!is_del) { \
        CK (cnt < ca->get_actual_count()); \
        if (OB_SUCC(ret)) { \
          elem = static_cast<ObObj *>(ca->get_data()) + i; \
          dst[cnt + offset].set_null();       \
          if (OB_NOT_NULL(elem)) { \
            if (elem->is_pl_extend() &&  \
                elem->get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) { \
              if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(allocator, *elem, dst[cnt + offset], true))) { \
                LOG_WARN("fail to fill obobj", K(*elem), K(ret)); \
              } \
            } else if (OB_FAIL(deep_copy_obj(allocator, *elem, dst[cnt + offset]))) { \
              LOG_WARN("fail to fill obobj", K(*elem), K(ret)); \
            } \
            if (OB_SUCC(ret)) {  \
              cnt++; \
            } \
          } else { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("get null collection element", K(ret), K(i)); \
          } \
        } else { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("collection element count illegal", K(ret)); \
        } \
      } \
    } \
  } while(0)

  #define FILL_ELEM_DISTICT(ca, d_map, allow_dup) \
  do { \
    const ObObj *elem = NULL; \
    int64_t i = 0; \
    for (; OB_SUCC(ret) && i < ca->get_count(); ++i) { \
      bool is_del = false; \
      if (OB_FAIL(ca->is_elem_deleted(i, is_del))) { \
        LOG_WARN("failed to test collection elem is deleted", K(ret)); \
      } else if (!is_del) { \
        elem = static_cast<ObObj *>(ca->get_data()) + i; \
        ret = d_map.set_refactored(*elem, 1); \
        if (OB_HASH_EXIST == ret) { \
          if (allow_dup) { \
            int64_t *dup_cnt = const_cast<int64_t *>(d_map.get(*elem)); \
            if (OB_NOT_NULL(dup_cnt)) { \
              *dup_cnt = *dup_cnt + 1; \
              ret = OB_SUCCESS; \
            } else { \
              ret = OB_ERR_UNEXPECTED; \
              LOG_WARN("get hash map value failed", K(ret), K(*elem)); \
            } \
          } else { \
            ret = OB_SUCCESS; \
          } \
        } else if (OB_FAIL(ret)) { \
          LOG_WARN("insert elem into hashmap failed.", K(ret)); \
        } \
      } \
    }\
  } while(0)

int ObExprMultiSet::calc_ms_intersect(common::ObIAllocator *coll_allocator,
                                      pl::ObPLCollection *c1,
                                      pl::ObPLCollection *c2,
                                      ObObj *&data_arr,
                                      int64_t &elem_count,
                                      ObMultiSetType ms_type,
                                      ObMultiSetModifier ms_modifier)
{
  return calc_ms_impl(coll_allocator, c1, c2, data_arr, elem_count, ms_type, ms_modifier);
}

int ObExprMultiSet::calc_ms_except(common::ObIAllocator *coll_allocator,
                                   pl::ObPLCollection *c1,
                                   pl::ObPLCollection *c2,
                                   ObObj *&data_arr,
                                   int64_t &elem_count,
                                   ObMultiSetType ms_type,
                                   ObMultiSetModifier ms_modifier)
{
  return calc_ms_impl(coll_allocator, c1, c2, data_arr, elem_count, ms_type, ms_modifier);
}

int ObExprMultiSet::calc_ms_impl(common::ObIAllocator *coll_allocator,
                                   pl::ObPLCollection *c1,
                                   pl::ObPLCollection *c2,
                                   ObObj *&data_arr,
                                   int64_t &elem_count,
                                   ObMultiSetType ms_type,
                                   ObMultiSetModifier ms_modifier)
{
  int ret = OB_SUCCESS;
  if (MULTISET_MODIFIER_ALL == ms_modifier) {
    if (OB_FAIL(calc_ms_all_impl(coll_allocator, c1, c2, data_arr,
                                 elem_count, true, ms_type, ms_modifier))) {
      LOG_WARN("failed to calc except all", K(ret));
    }
  } else if (MULTISET_MODIFIER_DISTINCT == ms_modifier) {
    if (OB_FAIL(calc_ms_distinct_impl(coll_allocator, c1, c2, data_arr,
                                      elem_count, ms_type, ms_modifier))) {
      LOG_WARN("failed to calc except distinct", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected multiest except modifier", K(ms_modifier), K(ms_type));
  }
  return ret;
}

int ObExprMultiSet::calc_ms_one_distinct(common::ObIAllocator *coll_allocator,
                                   ObObj *input_data,
                                   int64_t count,
                                   ObObj *&data_arr,
                                   int64_t &elem_count)
{
  int ret = OB_SUCCESS;
  LocalNTSHashMap dmap_c;
  int res_cnt = 0;
  int64_t real_count = 0;
  ObObj objs[count];
  for (int64_t i = 0; i < count; ++i) {
    // skip deleted element
    if (ObMaxType != input_data[i].get_type()) {
      objs[real_count] = input_data[i];
      real_count += 1;
    }
  }
  if (0 == real_count) {
    // do nothing
  } else if (OB_ISNULL(objs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs array is null", K(count), K(real_count));
  } else {
    if (OB_FAIL(dmap_c.create(real_count, ObModIds::OB_SQL_HASH_SET))) {
      LOG_WARN("fail create hash map", K(real_count), K(ret));
    } else {
      const ObObj *elem = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < real_count; ++i) {
        elem = objs + i;
        ret = dmap_c.set_refactored(*elem, i);
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          continue;
        } else if (OB_SUCC(ret)) {
          if (i != res_cnt) {
            objs[res_cnt] = *elem;
          }
          res_cnt++;
        } else {
          LOG_WARN("insert elem into hashmap failed.", K(ret));
        }
      }
    }
    CK (res_cnt > 0);
    if (OB_SUCC(ret)) {
      data_arr = static_cast<ObObj *>(coll_allocator->alloc(res_cnt * sizeof(ObObj)));
      if (OB_ISNULL(data_arr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate result obobj array failed, size is: ", K(res_cnt));
      } else {
        for (int64_t i = 0; i < res_cnt; ++i) {
          data_arr[i].set_null();
          if (objs[i].is_pl_extend() &&
              objs[i].get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
            if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(*coll_allocator, objs[i], data_arr[i], true))) {
              LOG_WARN("copy obobj failed.", K(ret));
            }
          } else if (OB_FAIL(deep_copy_obj(*coll_allocator, objs[i], data_arr[i]))) {
            LOG_WARN("copy obobj failed.", K(ret));
          }
        }
      }
      elem_count = res_cnt;
    }
  }
  return ret;
}

int ObExprMultiSet::calc_ms_distinct_impl(common::ObIAllocator *coll_allocator,
                                   pl::ObPLCollection *c1,
                                   pl::ObPLCollection *c2,
                                   ObObj *&data_arr,
                                   int64_t &elem_count,
                                   ObMultiSetType ms_type,
                                   ObMultiSetModifier ms_modifier)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  ObObj *tmp_res = NULL;
  if (OB_FAIL(calc_ms_all_impl(&tmp_alloc, c1, c2, tmp_res,
                               elem_count, false, ms_type, ms_modifier))) {
    LOG_WARN("calc intersect or except failed.", K(ret));
  } else if (OB_FAIL(calc_ms_one_distinct(coll_allocator,
                                          tmp_res,
                                          elem_count,
                                          data_arr,
                                          elem_count))) {
    LOG_WARN("calc distinct failed.", K(ret));
  }
  for (int64_t i = 0; i < elem_count; ++i) {
    if (tmp_res[i].is_pl_extend() &&
        tmp_res[i].get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
      pl::ObUserDefinedType::destruct_obj(tmp_res[i], nullptr);
    }
  }
  return ret;
}

int ObExprMultiSet::calc_ms_all_impl(common::ObIAllocator *coll_allocator,
                                   pl::ObPLCollection *c1,
                                   pl::ObPLCollection *c2,
                                   ObObj *&data_arr,
                                   int64_t &elem_count,
                                   bool allow_dup,
                                   ObMultiSetType ms_type,
                                   ObMultiSetModifier ms_modifier)
{
  UNUSED(ms_modifier);
  int ret = OB_SUCCESS;
  int64_t cnt1 = c1->get_actual_count();
  int64_t i = 0, index = 0;
  LocalNTSHashMap dmap_c2;
  if (0 == cnt1) {
    elem_count = 0;
  } else if ( 0 == c2->get_actual_count()) {
    if (MULTISET_TYPE_INTERSECT == ms_type) {
      elem_count = 0;
    } else if (MULTISET_TYPE_EXCEPT == ms_type) {
      data_arr =
       static_cast<ObObj *>(coll_allocator->alloc(c1->get_actual_count() * sizeof(ObObj)));
      FILL_ELEM(c1, data_arr, 0, *coll_allocator);
      OX (elem_count = c1->get_actual_count());
    }
  } else {
    if (OB_FAIL(dmap_c2.create(c2->get_actual_count(), ObModIds::OB_SQL_HASH_SET))) {
      LOG_WARN("fail create hash map", K(c2->get_actual_count()), K(ret));
    } else {
      FILL_ELEM_DISTICT(c2, dmap_c2, allow_dup);
    }
    if (OB_SUCC(ret)) {
      const ObObj *elem = NULL;
      data_arr =
       static_cast<ObObj *>(coll_allocator->alloc(c1->get_actual_count() * sizeof(ObObj)));
      if (OB_ISNULL(data_arr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("cann't alloc memory", K(c1->get_actual_count()), K(c1->get_count()));
      } else {
        int64_t *dup_cnt = NULL;
        bool is_del = false;
        for (i = 0; OB_SUCC(ret) && i < c1->get_count(); ++i) {
          if (OB_FAIL(c1->is_elem_deleted(i, is_del))) {
            LOG_WARN("failed to check collection elem deleted", K(ret));
          } else if (!is_del) {
            elem = static_cast<ObObj *>(c1->get_data()) + i;
        #define COPY_ELEM(iscopy) \
        do{ \
          if (iscopy) { \
            data_arr[index].set_null();  \
            if (elem->is_pl_extend() && elem->get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) { \
              OZ (pl::ObUserDefinedType::deep_copy_obj(*coll_allocator, *elem, data_arr[index], true)); \
            } else {  \
              OZ (common::deep_copy_obj(*coll_allocator, *elem, data_arr[index])); \
            }  \
            ++index; \
          }\
        } while(0)

        /* The except ALL keyword instructs Oracle to return all elements in nested_table1 that are
          not in nested_table2. For example, if a particular element occurs m times in
          nested_table1 and n times in nested_table2, then the result will have (m-n)
          occurrences of the element if m >n and 0 occurrences if m<=n.
        */
        /* The intersect ALL keyword instructs Oracle to return all common occurrences of elements
          that are in the two input nested tables, including duplicate common values and
          duplicate common NULL occurrences. For example, if a particular value occurs m
          times in nested_table1 and n times in nested_table2, then the result would
          contain the element min(m,n) times.
        */
            if (allow_dup) {
              dup_cnt = const_cast<int64_t *>(dmap_c2.get(*elem));
              if (MULTISET_TYPE_EXCEPT == ms_type) {
                if (OB_ISNULL(dup_cnt)) {
                  COPY_ELEM(true);
                } else {
                  if (OB_UNLIKELY(0 > *dup_cnt)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("unexpected collection elem count in hash map", K(ret), K(*dup_cnt));
                  } else if (0 != *dup_cnt) {
                    *dup_cnt -= 1;
                  } else {
                    COPY_ELEM(true);
                  }
                }
              } else {
                if (OB_ISNULL(dup_cnt)) {
                  // do nothing
                } else {
                  if (OB_UNLIKELY(0 > *dup_cnt)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("unexpected collection elem count in hash map", K(ret), K(*dup_cnt));
                  } else if (0 != *dup_cnt) {
                    COPY_ELEM(true);
                    *dup_cnt -= 1;
                  } else {
                  }
                }
              }
            } else {
              if (OB_ISNULL(dmap_c2.get(*elem))) {
                COPY_ELEM(MULTISET_TYPE_EXCEPT == ms_type);
              } else {
                COPY_ELEM(MULTISET_TYPE_INTERSECT == ms_type);
              }
            }
          }
        }
        elem_count = index;
      }
    }
  }
  return ret;
}

int ObExprMultiSet::append_collection(ObObj *buffer, int64_t buffer_size, int64_t &pos,
                                      pl::ObPLCollection *c,
                                      ObIAllocator &coll_alloc,
                                      bool keep_deleted_elem)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(buffer));
  CK (OB_NOT_NULL(c));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (buffer_size - pos < c->get_count()) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size not enough", K(ret), K(buffer_size), K(pos), KPC(c));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < c->get_count(); ++i) {
      ObObj *curr = c->get_data() + i;
      buffer[pos].reset();

      CK (OB_NOT_NULL(curr));

      if (OB_SUCC(ret)) {
        if (ObMaxType == curr->get_type() && !keep_deleted_elem) {
          // do nothing
        } else {
          if (curr->is_pl_extend() && pl::PL_REF_CURSOR_TYPE != curr->get_meta().get_extend_type()) {
            if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(coll_alloc, *curr, buffer[pos], true))) {
              LOG_WARN("failed to deep_copy_obj", K(ret), K(i), KPC(c));
            }
          } else if (OB_FAIL(deep_copy_obj(coll_alloc, *curr, buffer[pos]))) {
            LOG_WARN("failed to deep_copy_obj", K(ret), K(i), KPC(c));
          }

          if (OB_SUCC(ret)) {
            pos += 1;
          }
        }
      }
    }
  }

  return ret;
}

int ObExprMultiSet::calc_ms_union(common::ObIAllocator *coll_allocator,
                                  pl::ObPLCollection *c1,
                                  pl::ObPLCollection *c2,
                                  ObObj *&data_arr,
                                  int64_t &elem_count,
                                  ObMultiSetType ms_type,
                                  ObMultiSetModifier ms_modifier)
{
  int ret = OB_SUCCESS;
  int64_t count = c1->get_count() + c2->get_count();
  int64_t alloc_size = 0;
  LocalNTSHashMap distinct_map;
  if (0 == count) {
    elem_count = 0;
  } else {
    ObObj *tmp_res = NULL;
    alloc_size = count * sizeof(ObObj);
    tmp_res = static_cast<ObObj*>(coll_allocator->alloc(alloc_size));
    if (OB_ISNULL(tmp_res)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(alloc_size));
    } else {
      for (int64_t i = 0; i < count; ++i) {
        new(&tmp_res[i])ObObj();
      }
      if (OB_SUCC(ret)) {
        if (MULTISET_MODIFIER_ALL == ms_modifier) {
          elem_count = 0;

          if (OB_FAIL(append_collection(tmp_res, count, elem_count, c1, *coll_allocator, true))) {
            LOG_WARN("failed append_collection to c1", K(ret), K(count), K(elem_count), KPC(c1));
          } else if (OB_FAIL(append_collection(tmp_res, count, elem_count, c2, *coll_allocator, false))) {
            LOG_WARN("failed append_collection to c2", K(ret), K(count), K(elem_count), KPC(c2));
          } else {
            data_arr = tmp_res;
          }
        } else if (MULTISET_MODIFIER_DISTINCT == ms_modifier) {
          ObArenaAllocator tmp_alloc;
          FILL_ELEM(c1, tmp_res, 0, tmp_alloc);
          FILL_ELEM(c2, tmp_res, c1->get_actual_count(), tmp_alloc);
          if (OB_FAIL(calc_ms_one_distinct(coll_allocator,
                                          tmp_res, count,
                                          data_arr, elem_count))) {
            LOG_WARN("calc multiset union distinc failed.", K(ret));
          }
          for (int64_t i = 0; i < count; ++i) {
            if (tmp_res[i].is_pl_extend() &&
                tmp_res[i].get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
              pl::ObUserDefinedType::destruct_obj(tmp_res[i], nullptr);
            }
          }
          coll_allocator->free(tmp_res);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected multiset modifier", K(ms_modifier), K(ms_type));
        }
      }
    }
  }
  return ret;
}
int ObExprMultiSet::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (2 != rt_expr.arg_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong param num for multi set", K(ret), K(rt_expr.arg_cnt_));
  } else {
    ObIAllocator &alloc = *expr_cg_ctx.allocator_;
    const ObMultiSetRawExpr &fun_sys = static_cast<const ObMultiSetRawExpr &>(raw_expr);
    ObExprMultiSetInfo *info = OB_NEWx(ObExprMultiSetInfo, (&alloc), alloc, T_OP_MULTISET);
    if (NULL == info) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      OZ(info->from_raw_expr(fun_sys));
      rt_expr.extra_info_ = info;
    }
    rt_expr.eval_func_ = eval_multiset;
  }
  return ret;
}

int ObExprMultiSet::eval_multiset_composite(ObExecContext &exec_ctx,
                                            const common::ObObj &obj1,
                                            const common::ObObj &obj2,
                                            common::ObObj &result,
                                            ObMultiSetType ms_type,
                                            ObMultiSetModifier ms_modifier) {
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_ORACLE_PL

  static constexpr char MULTISET_EXCEPT_PL[] =
      "declare\n"
      "lhs_count pls_integer := :lhs.count;\n"
      "rhs_count pls_integer := :rhs.count;\n"
      "buffer_count pls_integer := :buffer.count;\n"
      "result_count pls_integer := :result.count;\n"
      "cmp boolean;\n"
      "cmp1 boolean;\n"
      "cmp2 boolean;\n"
      "begin\n"
      "if lhs_count > 0 then\n"
      "  if rhs_count > 0 then\n"
      "    for i in :rhs.first..:rhs.last loop\n"
      "      if not :rhs.exists(i) then continue; end if;\n"
      "      cmp1 := (:rhs(i) = :rhs(i));\n"
      "        for j in :lhs.first..:lhs.last loop\n"
      "          if not :lhs.exists(j) then continue; end if;\n"
      "          cmp := (:rhs(i) = :lhs(j));\n"
      "          if cmp then\n"
      "            :lhs.delete(j);\n"
      "            exit;\n"
      "          elsif cmp is NULL then\n"
      "            cmp2 := (:lhs(j) = :lhs(j));\n"
      "            if (cmp1 is NULL) or (cmp2 is NULL) then\n"
      "              if (cmp1 is NULL) and (cmp2 is NULL) then\n"
      "                :lhs.delete(j);\n"
      "                exit;\n"
      "              end if;\n"
      "            end if;\n"
      "          end if;\n"
      "      end loop;\n"
      "    end loop;\n"
      "  end if;\n"
      "  for i in :lhs.first..:lhs.last loop\n"
      "    if not :lhs.exists(i) then continue; end if;\n"
      "    :result.extend(1);\n"
      "    :result(:result.count) := :lhs(i);\n"
      "  end loop;\n"
      "end if;\n"
      "end;";

  static constexpr char MULTISET_INTERSECT_PL[] =
      "declare\n"
      "lhs_count pls_integer := :lhs.count;\n"
      "rhs_count pls_integer := :rhs.count;\n"
      "buffer_count pls_integer := :buffer.count;\n"
      "result_count pls_integer := :result.count;\n"
      "lhs_cnt pls_integer := 0;\n"
      "rhs_cnt pls_integer := 0;\n"
      "begin\n"
      "if lhs_count = 0 or rhs_count = 0 then return; end if;\n"
      "for i in :buffer.first..:buffer.last loop\n"
      "if not :buffer.exists(i) then continue; end if;\n"
      "  lhs_cnt := 0;\n"
      "  rhs_cnt := 0;\n"
      "  for j in :lhs.first..:lhs.last loop\n"
      "    if not :lhs.exists(j) then continue; end if;\n"
      "    if (:buffer(i) is NULL) or (:lhs(j) is NULL) then\n"
      "      if (:buffer(i) is NULL) and (:lhs(j) is NULL) then\n"
      "        lhs_cnt := lhs_cnt+1;\n"
      "        :lhs.delete(j);\n"
      "      end if;\n"
      "    elsif :buffer(i) = :lhs(j) or (:buffer(i) = :lhs(j)) is NULL then\n"
      "      lhs_cnt := lhs_cnt+1;\n"
      "      :lhs.delete(j);\n"
      "    end if;\n"
      "  end loop;\n"
      "\n"
      "  for j in :rhs.first..:rhs.last loop\n"
      "    if not :rhs.exists(j) then continue; end if;\n"
      "    if (:buffer(i) is NULL) or (:rhs(j) is NULL) then\n"
      "      if (:buffer(i) is NULL) and (:rhs(j) is NULL) then\n"
      "        rhs_cnt := rhs_cnt+1;\n"
      "        :rhs.delete(j);\n"
      "      end if;\n"
      "    elsif :buffer(i) = :rhs(j) or (:buffer(i) = :rhs(j)) is NULL then\n"
      "      rhs_cnt := rhs_cnt+1;\n"
      "      :rhs.delete(j);\n"
      "    end if;\n"
      "  end loop;\n"
      "\n"
      "  for j in 1..least(lhs_cnt, rhs_cnt) loop\n"
      "    :result.extend(1);\n"
      "    :result(:result.count) := :buffer(i);\n"
      "  end loop;\n"
      "end loop;\n"
      "end;";

  static constexpr char MULTISET_UNION_PL[] =
      "declare\n"
      "lhs_count pls_integer := :lhs.count;\n"
      "rhs_count pls_integer := :rhs.count;\n"
      "buffer_count pls_integer := :buffer.count;\n"
      "result_count pls_integer := :result.count;\n"
      "begin\n"
      "if :lhs.count > 0 then\n"
      "  for idx in :lhs.first..:lhs.last loop\n"
      "    if not :lhs.exists(idx) then continue; end if;\n"
      "    :result.extend(1);\n"
      "    :result(:result.count) := :lhs(idx);\n"
      "  end loop;\n"
      "end if;\n"
      "\n"
      "if :rhs.count > 0 then\n"
      "  for idx in :rhs.first..:rhs.last loop\n"
      "    if not :rhs.exists(idx) then continue; end if;\n"
      "    :result.extend(1);\n"
      "    :result(:result.count) := :rhs(idx);\n"
      "  end loop;\n"
      "end if;\n"
      "end;";

  static constexpr char DISTINCT_PL[] =
      "declare\n"
      "null_element boolean := FALSE;\n"
      "cmp boolean;\n"
      "begin\n"
      "if :coll.count = 0 then return; end if;\n"
      "for idx in :coll.first..:coll.last loop\n"
      "  if not :coll.exists(idx) then continue; end if;\n"
      "  cmp := :coll(idx) member of :result;\n"
      "  if cmp is not NULL then\n"
      "    if cmp then\n"
      "      null;\n"
      "    else\n"
      "      :result.extend(1);\n"
      "      :result(:result.count) := :coll(idx);\n"
      "    end if;\n"
      "  elsif (:coll(idx) = :coll(idx)) is null then\n"
      "    if not null_element then\n"
      "      null_element := TRUE;\n"
      "      :result.extend(1);\n"
      "      :result(:result.count) := :coll(idx);\n"
      "    end if;\n"
      "  end if;\n"
      "end loop;\n"
      "end;";

#define DECLARE_NESTED_TABLE(name, coll)                                         \
  pl::ObPLNestedTable name;                                                      \
  pl::ObPLAllocator1 name##_alloc(pl::PL_MOD_IDX::OB_PL_COLLECTION, &tmp_alloc); \
  OZ (name##_alloc.init(nullptr));                                               \
  ObObj name##_obj;                                                              \
  if (OB_SUCC(ret)) {                                                            \
    (name).set_id((coll)->get_id());                                             \
    (name).set_allocator(&name##_alloc);                                         \
    (name).set_element_desc((coll)->get_element_desc());                         \
    (name).set_inited();                                                         \
    (name##_obj)                                                                 \
        .set_extend(reinterpret_cast<int64_t>(&(name)),                          \
                    (coll)->get_type());                                         \
  }                                                                              \
  DEFER(pl::ObUserDefinedType::destruct_obj((name##_obj)));

#define DEDUP_COLLECTION(obj, params, result)                                  \
  do {                                                                         \
    if (OB_FAIL(ret)) {                                                        \
    } else if (OB_FAIL((params).push_back(obj))) {                             \
      LOG_WARN("failed to push back coll obj", K(ret), K(obj), K(params));     \
    } else if (OB_FAIL((params).push_back(result))) {                          \
      LOG_WARN("failed to push back result obj", K(ret), K(result),            \
               K(params));                                                     \
    } else {                                                                   \
      (params).at(0).set_udt_id(coll_udt_id);                                  \
      (params).at(0).set_param_meta();                                         \
      (params).at(1).set_udt_id(coll_udt_id);                                  \
      (params).at(1).set_param_meta();                                         \
    }                                                                          \
    if (OB_FAIL(ret)) {                                                        \
    } else if (OB_FAIL(eval_composite_relative_anonymous_block(exec_ctx,       \
                                                               DISTINCT_PL,    \
                                                               (params),       \
                                                               out_args))) {   \
      LOG_WARN("failed to execute PS anonymous bolck", K(params),              \
               K(out_args));                                                   \
    } else if (1 != out_args.num_members() || !out_args.has_member(1)) {       \
      ret = OB_ERR_UNEXPECTED;                                                 \
      LOG_WARN("unexpected out param for DISTINCT_PL", K(ret), K(out_args),    \
               K(params));                                                     \
    } else {                                                                   \
      (result) = (params).at(1);                                               \
    }                                                                          \
  } while (0)

  pl::ObPLCollection *c1 =
      reinterpret_cast<pl::ObPLCollection *>(obj1.get_ext());
  pl::ObPLCollection *c2 =
      reinterpret_cast<pl::ObPLCollection *>(obj2.get_ext());

  const char *multiset_pl = nullptr;

  pl::ObPLNestedTable *res_coll = nullptr;
  pl::ObPLAllocator1 *res_alloc = nullptr;

  CK (OB_NOT_NULL(c1));
  CK (OB_NOT_NULL(c2));
  CK (c1->is_nested_table());
  CK (c2->is_nested_table());

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (MULTISET_TYPE_EXCEPT == ms_type) {
    multiset_pl = MULTISET_EXCEPT_PL;
  } else if (MULTISET_TYPE_INTERSECT == ms_type) {
    multiset_pl = MULTISET_INTERSECT_PL;
  } else if (MULTISET_TYPE_UNION == ms_type) {
    multiset_pl = MULTISET_UNION_PL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected MULTISET type", K(ret), K(ms_type), K(ms_modifier));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (c1->get_id() != c2->get_id()) {
    static const ObString EXPR_NAME = "MULTISET";
    ret = OB_ERR_CALL_WRONG_ARG;

    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, EXPR_NAME.length(), EXPR_NAME.ptr());
    LOG_WARN("failed to eval MULTISET", K(ret), KPC(c1), KPC(c2), K(ms_type),
             K(ms_modifier));
  } else if (OB_ISNULL(res_coll = static_cast<pl::ObPLNestedTable *>(
                           exec_ctx.get_allocator().alloc(
                               sizeof(pl::ObPLNestedTable))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for result collection", K(ret));
  } else if (OB_ISNULL(res_coll =
                           new (res_coll) pl::ObPLNestedTable(c1->get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct result collection",
             K(ret), KPC(c1), KPC(res_coll));
  } else if (OB_ISNULL(res_alloc = static_cast<pl::ObPLAllocator1 *>(
                           exec_ctx.get_allocator().alloc(
                               sizeof(pl::ObPLAllocator1))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for result collection allocator", K(ret));
  } else if (OB_ISNULL(res_alloc =
                           new (res_alloc) pl::ObPLAllocator1(pl::PL_MOD_IDX::OB_PL_COLLECTION, &exec_ctx.get_allocator()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct result collection allocator", K(ret));
  } else if (OB_FAIL(res_alloc->init(nullptr))) {
    LOG_WARN("failed to init result collection allocator", K(ret));
  } else if (FALSE_IT(res_coll->set_allocator(res_alloc))) {
    // unreachable
  } else {
    const uint64_t coll_udt_id = c1->get_id();
    ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(pl::PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    DECLARE_NESTED_TABLE(lhs, c1);
    DECLARE_NESTED_TABLE(rhs, c2);
    DECLARE_NESTED_TABLE(buffer, c1);
    DECLARE_NESTED_TABLE(res, c1);
    DECLARE_NESTED_TABLE(res_distinct, c1);

    pl::ObPLNestedTable *eval_result = nullptr;

    ObArenaAllocator alloc;
    ParamStore params((ObWrapperAllocator(alloc)));
    ParamStore lhs_distinct_params((ObWrapperAllocator(alloc)));
    ParamStore rhs_distinct_params((ObWrapperAllocator(alloc)));
    ParamStore result_distinct_params((ObWrapperAllocator(alloc)));

    ObBitSet<> out_args;

    if (MULTISET_MODIFIER_DISTINCT == ms_modifier) {
      DEDUP_COLLECTION(obj1, lhs_distinct_params, lhs_obj);
      DEDUP_COLLECTION(obj2, rhs_distinct_params, rhs_obj);
    } else {
      if (OB_FAIL(lhs.deep_copy(c1, nullptr))) {
        LOG_WARN("failed too deep copy c1", K(ret), K(lhs), KPC(c1));
      } else if (OB_FAIL(rhs.deep_copy(c2, nullptr))) {
        LOG_WARN("failed too deep copy c2", K(ret), K(rhs), KPC(c2));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(buffer.deep_copy(reinterpret_cast<pl::ObPLNestedTable*>(lhs_obj.get_ext()), nullptr))) {
      LOG_WARN("failed to deep copy lhs to buffer", K(lhs_obj), K(buffer));
    } else if (OB_FAIL(params.push_back(lhs_obj))) {
      LOG_WARN("failed to push back lhs_obj", K(ret), K(lhs_obj), K(params));
    } else if (OB_FAIL(params.push_back(rhs_obj))) {
      LOG_WARN("failed to push back rhs_obj", K(ret), K(rhs_obj), K(params));
    } else if (OB_FAIL(params.push_back(buffer_obj))) {
      LOG_WARN("failed to push back buffer_obj", K(ret), K(buffer_obj), K(params));
    } else if (OB_FAIL(params.push_back(res_obj))) {
      LOG_WARN("failed to push back res_obj", K(ret), K(res_obj), K(params));
    } else {
      params.at(0).set_udt_id(coll_udt_id);
      params.at(0).set_param_meta();
      params.at(1).set_udt_id(coll_udt_id);
      params.at(1).set_param_meta();
      params.at(2).set_udt_id(coll_udt_id);
      params.at(2).set_param_meta();
      params.at(3).set_udt_id(coll_udt_id);
      params.at(3).set_param_meta();
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(eval_composite_relative_anonymous_block(exec_ctx, multiset_pl, params, out_args))) {
      LOG_WARN("failed to execute PS anonymous bolck",
               K(multiset_pl), K(params), K(out_args));
    } else if (!out_args.has_member(3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected out args", K(ret), K(params), K(out_args));
    } else if (MULTISET_MODIFIER_DISTINCT == ms_modifier) {
      ObObj all_result = params.at(3);
      DEDUP_COLLECTION(all_result, result_distinct_params, res_distinct_obj);
      OX (eval_result = reinterpret_cast<pl::ObPLNestedTable*>(res_distinct_obj.get_ext()));
    } else {
      eval_result = reinterpret_cast<pl::ObPLNestedTable*>(params.at(3).get_ext());
    }

    CK (OB_NOT_NULL(eval_result));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(res_coll->deep_copy(eval_result, nullptr))) {
      LOG_WARN("failed too deep copy eval_result", K(ret), KPC(eval_result), K(res_coll));
    } else {
      result.set_extend(reinterpret_cast<int64_t>(res_coll), res_coll->get_type());
    }
  }

#undef DECLARE_NESTED_TABLE

#endif // OB_BUILD_ORACLE_PL

  return ret;
}



int ObExprMultiSet::eval_composite_relative_anonymous_block(ObExecContext &exec_ctx,
                                                            const char *pl,
                                                            ParamStore &params,
                                                            ObBitSet<> &out_args) {
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(GCTX.pl_engine_));
  CK (OB_NOT_NULL(exec_ctx.get_sql_ctx()));

  if (OB_SUCC(ret)) {
    bool is_inner_mock_backup = false;
    bool is_ps_backup = exec_ctx.get_sql_ctx()->is_prepare_protocol_;
    bool is_pre_exec_backup = exec_ctx.get_sql_ctx()->is_pre_execute_;

    exec_ctx.get_sql_ctx()->is_prepare_protocol_ = true;
    exec_ctx.get_sql_ctx()->is_pre_execute_ = true;

    if (OB_NOT_NULL(exec_ctx.get_pl_stack_ctx())) {
      is_inner_mock_backup = exec_ctx.get_pl_stack_ctx()->get_is_inner_mock();
      exec_ctx.get_pl_stack_ctx()->set_is_inner_mock(true);
    }

    DEFER(exec_ctx.get_sql_ctx()->is_prepare_protocol_ = is_ps_backup);
    DEFER(exec_ctx.get_sql_ctx()->is_pre_execute_ = is_pre_exec_backup);
    DEFER(if (OB_NOT_NULL(exec_ctx.get_pl_stack_ctx())) { exec_ctx.get_pl_stack_ctx()->set_is_inner_mock(is_inner_mock_backup); });

    out_args.reuse();

    CREATE_WITH_TEMP_CONTEXT(lib::ContextParam().set_mem_attr(MTL_ID(),
                                                              GET_PL_MOD_STRING(pl::OB_PL_MULTISET),
                                                              ObCtxIds::DEFAULT_CTX_ID)) {
      if (OB_FAIL(GCTX.pl_engine_->execute(exec_ctx,
                                          params,
                                          OB_INVALID_ID,
                                          pl,
                                          out_args))) {
        LOG_WARN("failed to execute PS anonymous bolck",
                 K(ret), K(pl), K(params), K(out_args));
      }
    }
  }

  return ret;
}

int ObExprMultiSet::eval_multiset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(expr, ctx, res);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support udt", K(ret));
#else
  ObDatum *datum1 = nullptr;
  ObDatum *datum2 = nullptr;
  ObObj result;
  const ObExprMultiSetInfo *info = static_cast<ObExprMultiSetInfo *>(expr.extra_info_);
  if (OB_FAIL(expr.eval_param_value(ctx, datum1, datum2))) {
    LOG_WARN("failed to eval params", K(ret));
  } else if (lib::is_oracle_mode()
             && ObExtendType == expr.args_[0]->datum_meta_.type_
             && ObExtendType == expr.args_[1]->datum_meta_.type_) {
    ObObj obj1;
    ObObj obj2;
    if (OB_FAIL(datum1->to_obj(obj1, expr.args_[0]->obj_meta_))) {
      LOG_WARN("failed to convert to obj", K(ret));
    } else if (OB_FAIL(datum2->to_obj(obj2, expr.args_[1]->obj_meta_))) {
      LOG_WARN("failed to convert to obj", K(ret));
    } else {
      pl::ObPLCollection *c1 = reinterpret_cast<pl::ObPLCollection *>(obj1.get_ext());
      pl::ObPLCollection *c2 = reinterpret_cast<pl::ObPLCollection *>(obj2.get_ext());
      ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
      pl::ObPLAllocator1 *collection_allocator = NULL;
      ObObj *data_arr = NULL;
      int64_t elem_count = -1;
      pl::ObPLNestedTable *coll =
              static_cast<pl::ObPLNestedTable*>(allocator.alloc(sizeof(pl::ObPLNestedTable)));
      if (OB_ISNULL(coll)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed.", K(ret));
      } else if (OB_ISNULL(c1) || OB_ISNULL(c2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("union udt failed due to null udt", K(ret), K(obj1), K(obj2));
      } else if (pl::PL_NESTED_TABLE_TYPE != c1->get_type()
                || pl::PL_NESTED_TABLE_TYPE != c2->get_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "udt union except nested table");
        LOG_WARN("not support udt union except nested table", K(ret), K(c1), K(c2));
      } else if (c1->get_element_type().get_obj_type() != c2->get_element_type().get_obj_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt union failed due to uninited", K(ret), KPC(c1), KPC(c2));
      } else if (!c1->is_inited() || !c2->is_inited()) {
        // if has uninit collection, result is uninit, so do nothing ...
      } else if (c1->is_of_composite()) {
        if (OB_FAIL(eval_multiset_composite(ctx.exec_ctx_,
                                            obj1,
                                            obj2,
                                            result,
                                            info->ms_type_,
                                            info->ms_modifier_))) {
          LOG_WARN("failed to eval_multiset_composit",
                   K(ret), K(obj1), K(obj2), K(result));
        } else {
          coll = reinterpret_cast<pl::ObPLNestedTable*>(result.get_ext());
        }
      } else {
        coll = new(coll)pl::ObPLNestedTable(c1->get_id());
        collection_allocator =
                    static_cast<pl::ObPLAllocator1*>(allocator.alloc(sizeof(pl::ObPLAllocator1)));
        if (OB_ISNULL(collection_allocator)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc pl collection allocator failed.", K(ret));
        } else {
          collection_allocator = new(collection_allocator)pl::ObPLAllocator1(pl::PL_MOD_IDX::OB_PL_COLLECTION, &allocator);
          OZ (collection_allocator->init(nullptr));
          if (OB_SUCC(ret)) {
            switch (info->ms_type_) {
              case MULTISET_TYPE_UNION:
                if (OB_FAIL(calc_ms_union(collection_allocator,
                                          c1, c2, data_arr, elem_count,
                                          info->ms_type_, info->ms_modifier_))) {
                  LOG_WARN("calc multiset union failed.", K(ret));
                }
                break;
              case MULTISET_TYPE_INTERSECT:
                if (OB_FAIL(calc_ms_intersect(collection_allocator,
                                              c1, c2, data_arr, elem_count,
                                              info->ms_type_, info->ms_modifier_))) {
                  LOG_WARN("calc multiset union failed.", K(ret));
                }
                break;
              case MULTISET_TYPE_EXCEPT:
                if (OB_FAIL(calc_ms_except(collection_allocator,
                                          c1, c2, data_arr, elem_count,
                                          info->ms_type_, info->ms_modifier_))) {
                  LOG_WARN("calc multiset union failed.", K(ret));
                }
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unknown multiset operation type", K(info->ms_type_),
                                                K(info->ms_modifier_), K(ret));
                break;
            }
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to calc multiset operator", K(ret), K(data_arr));
        } else if (elem_count > 0 && OB_ISNULL(data_arr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result.", K(elem_count), K(data_arr), K(ret));
        } else {
          coll->set_allocator(collection_allocator);
          coll->set_type(c1->get_type());
          coll->set_id(c1->get_id());
          coll->set_is_null(c1->is_null());
          coll->set_element_desc(c1->get_element_desc());
          coll->set_column_count(c1->get_column_count());
          coll->set_not_null(c1->is_not_null());
          coll->set_count(elem_count);
          coll->set_first(elem_count > 0 ? 1 : OB_INVALID_ID);
          coll->set_last(elem_count > 0 ? elem_count : OB_INVALID_ID);
          coll->set_data(data_arr, elem_count);
          result.set_extend(reinterpret_cast<int64_t>(coll), coll->get_type());
        }
      }

      OZ(res.from_obj(result, expr.obj_datum_map_));
      //Collection constructed here must be recorded and destructed at last
      if (OB_NOT_NULL(coll) && OB_NOT_NULL(coll->get_allocator())) {
        int tmp_ret = OB_SUCCESS;
        if (OB_ISNULL(ctx.exec_ctx_.get_pl_ctx())) {
          tmp_ret = ctx.exec_ctx_.init_pl_ctx();
        }
        if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(ctx.exec_ctx_.get_pl_ctx())) {
          tmp_ret = ctx.exec_ctx_.get_pl_ctx()->add(result);
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

OB_DEF_SERIALIZE(ObExprMultiSetInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              ms_type_,
              ms_modifier_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprMultiSetInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              ms_type_,
              ms_modifier_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprMultiSetInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              ms_type_,
              ms_modifier_);
  return len;
}

int ObExprMultiSetInfo::deep_copy(common::ObIAllocator &allocator,
                         const ObExprOperatorType type,
                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprMultiSetInfo &other = *static_cast<ObExprMultiSetInfo *>(copied_info);
  other.ms_type_ = ms_type_;
  other.ms_modifier_ = ms_modifier_;
  return ret;
}

template <typename RE>
int ObExprMultiSetInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  ObMultiSetRawExpr &set_expr 
       = const_cast<ObMultiSetRawExpr &> (static_cast<const ObMultiSetRawExpr&>(raw_expr));
  ms_type_ = set_expr.get_multiset_type();
  ms_modifier_ = set_expr.get_multiset_modifier();
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
