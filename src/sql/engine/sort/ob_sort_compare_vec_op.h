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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_COMPARE_VEC_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_COMPARE_VEC_OP_H_

#include "sql/engine/sort/ob_sort_vec_op_chunk.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{
struct SortKeyColResult
{
public:
  SortKeyColResult(ObIAllocator &allocator);
  ~SortKeyColResult();
  void reset();
  int init(int64_t max_batch_size);
  bool is_null(const int64_t idx) const;
  const char *get_payload(const int64_t idx) const;
  ObLength get_length(const int64_t idx) const;

public:
  union
  {
    struct
    {
      uint32_t has_null_ : 1;
      uint32_t reserved_ : 7;
    };
    uint8_t flag_;
  };
  VectorFormat format_;
  ObIAllocator &allocator_;
  const char *data_;
  const sql::ObBitVector *nulls_;
  sql::ObBitVector *uniform_fmt_nulls_;
  const char **payload_array_;
  ObLength *len_array_;
};

class CompareBase
{
public:
  enum EncodeSortKeyState
  {
    DISABLE = -1,
    ENABLE,
    FALLBACK_TO_DISABLE,
  };
  CompareBase(ObIAllocator &allocator);
  ~CompareBase();
  int init(const ObIArray<ObExpr *> *cmp_sk_exprs, const RowMeta *sk_row_meta,
           const RowMeta *addon_row_meta, const ObIArray<ObSortFieldCollation> *cmp_sort_collations,
           ObExecContext *exec_ctx, bool enable_encode_sortkey);
  bool is_inited() const
  {
    return nullptr != cmp_sort_collations_;
  }
  // interface required by ObBinaryHeap
  int get_error_code()
  {
    return ret_;
  }
  void reset()
  {
    this->~CompareBase();
  }
  int fast_check_status();
  int64_t get_cnt()
  {
    return cnt_;
  }
  // for hash_based sort of partition by in window function
  void set_cmp_range(const int64_t cmp_start, const int64_t cmp_end)
  {
    cmp_start_ = cmp_start;
    cmp_end_ = cmp_end;
  }
  void set_sort_key_col_result_list(const SortKeyColResult *col_result_list)
  {
    sk_col_result_list_ = col_result_list;
  }
  void fallback_to_disable_encode_sortkey()
  {
    encode_sk_state_ = CompareBase::FALLBACK_TO_DISABLE;
  }

protected:
  int init_cmp_sort_key(const ObIArray<ObExpr *> *cmp_sk_exprs,
                        const ObIArray<ObSortFieldCollation> *sort_collations);

public:
  ObIAllocator &allocator_;
  int ret_;
  const ObIArray<ObExpr *> *cmp_sk_exprs_;
  const RowMeta *sk_row_meta_;
  const RowMeta *addon_row_meta_;
  const ObIArray<ObSortFieldCollation> *cmp_sort_collations_;
  const SortKeyColResult *sk_col_result_list_;
  common::ObFixedArray<NullSafeRowCmpFunc, common::ObIAllocator> cmp_funcs_;
  ObExecContext *exec_ctx_;
  EncodeSortKeyState encode_sk_state_;
  int64_t cmp_count_;
  int64_t cmp_start_;
  int64_t cmp_end_;
  int64_t cnt_;
};

template <typename Store_Row, bool has_addon>
class GeneralCompare : public CompareBase
{
public:
  using SortVecOpChunk = ObSortVecOpChunk<Store_Row, has_addon>;
  GeneralCompare(ObIAllocator &allocator) : CompareBase(allocator)
  {}
  // compare function for quick sort.
  bool operator()(const Store_Row *l, const Store_Row *r);
  // compare function for in-memory merge sort
  bool operator()(Store_Row **l, Store_Row **r);
  // compare function for external merge sort
  bool operator()(const SortVecOpChunk *l, const SortVecOpChunk *r);
  bool operator()(const Store_Row *r, ObEvalCtx &eval_ctx);
  int with_ties_cmp(const Store_Row *r, ObEvalCtx &eval_ctx);
  int with_ties_cmp(const Store_Row *l, const Store_Row *r);

protected:
  int compare(const Store_Row *l, const Store_Row *r, const RowMeta *row_meta);
  int compare(const Store_Row *r, ObEvalCtx &eval_ctx, const RowMeta *row_meta);
};

template<VecValueTypeClass vec_tc, bool null_first>
struct FixedCmpFunc
{
  using CType = RTCType<vec_tc>;
  OB_INLINE static int cmp(const void *l_v, const bool l_null, const void *r_v, const bool r_null)
  {
    int cmp_ret = 0;
    if (OB_UNLIKELY(l_null) && OB_UNLIKELY(r_null)) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l_null)) {
      cmp_ret = null_first ? -1 : 1;
    } else if (OB_UNLIKELY(r_null)) {
      cmp_ret = null_first ? 1 : -1;
    } else {
      cmp_ret = *(reinterpret_cast<const CType*>(l_v)) == *(reinterpret_cast<const CType*>(r_v))
        ? 0
        : (*(reinterpret_cast<const CType*>(l_v)) < *(reinterpret_cast<const CType*>(r_v)) ? -1 : 1);
    }
    return cmp_ret;
  }
};

template <typename Store_Row, bool has_addon>
class FixedCompare : public CompareBase
{
  typedef int (*CmpFunc) (const void*, const bool, const void*, const bool);

public:
  using SortVecOpChunk = ObSortVecOpChunk<Store_Row, has_addon>;
  FixedCompare(ObIAllocator &allocator) : CompareBase(allocator), basic_cmp_funcs_(allocator)
  {}
  ~FixedCompare()
  {
    basic_cmp_funcs_.reset();
  }
  int init_basic_cmp_func(const ObIArray<ObExpr *> &cmp_sk_exprs,
                          const ObIArray<ObSortFieldCollation> &cmp_sort_collations);
  int init(const ObIArray<ObExpr *> *cmp_sk_exprs, const RowMeta *sk_row_meta,
           const RowMeta *addon_row_meta, const ObIArray<ObSortFieldCollation> *cmp_sort_collations,
           ObExecContext *exec_ctx, bool enable_encode_sortkey);
  // compare function for quick sort.
  bool operator()(const Store_Row *l, const Store_Row *r);
  // compare function for in-memory merge sort
  bool operator()(Store_Row **l, Store_Row **r);
  // compare function for external merge sort
  bool operator()(const SortVecOpChunk *l, const SortVecOpChunk *r);
  bool operator()(const Store_Row *r, ObEvalCtx &eval_ctx);
  int with_ties_cmp(const Store_Row *r, ObEvalCtx &eval_ctx);
  int with_ties_cmp(const Store_Row *l, const Store_Row *r);

protected:
  int compare(const Store_Row *l, const Store_Row *r, const RowMeta *row_meta);
  int compare(const Store_Row *r, ObEvalCtx &eval_ctx, const RowMeta *row_meta);

private:
  common::ObFixedArray<CmpFunc, common::ObIAllocator> basic_cmp_funcs_;
};

} // end namespace sql
} // end namespace oceanbase

#include "ob_sort_compare_vec_op.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_COMPARE_VEC_OP_H_ */
