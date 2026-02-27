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

#ifndef OCEANBASE_STORAGE_DDL_OB_DDL_SORT_PROVIDER_H_
#define OCEANBASE_STORAGE_DDL_OB_DDL_SORT_PROVIDER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_se_array.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"

namespace oceanbase
{
namespace storage
{

template <typename Store_Row>
struct ObInvertedIndexCompare
{
public:
  ObInvertedIndexCompare();
  ~ObInvertedIndexCompare() {};

  // 基于 ddl_table_schema 的列，准备比较信息
  int init(const ObDDLTableSchema *ddl_table_schema, const sql::RowMeta *row_meta, const bool enable_encode_sortkey, const bool rowkey_only = false);
  void reset();
  void reuse();
  int get_error_code() const { return ret_; }
  void fallback_to_disable_encode_sortkey() {}
  int compare(const Store_Row *l, const ObIArray<ObDatum> &r_datums);

  bool operator()(const Store_Row *l, const Store_Row *r);
  bool operator()(const sql::ObSortVecOpChunk<Store_Row, false> *l,
                  const sql::ObSortVecOpChunk<Store_Row, false> *r);
  bool operator()(const Store_Row *l, const ObIArray<ObDatum> &r_datums);
  bool operator()(const ObIArray<ObDatum> &l_datums, const Store_Row *r);

private:
  struct CmpInfo
  {
  public:
    CmpInfo()
    : col_idx_(0),
      obj_type_(common::ObObjType::ObNullType),
      cs_type_(common::ObCollationType::CS_TYPE_INVALID),
      precision_(common::PRECISION_UNKNOWN_YET),
      scale_(common::SCALE_UNKNOWN_YET),
      cmp_func_(nullptr)
    {}
    ~CmpInfo() = default;

  public:
    int32_t col_idx_;
    common::ObObjType obj_type_;
    common::ObCollationType cs_type_;
    common::ObPrecision precision_;
    common::ObScale scale_;
    common::ObDatumCmpFuncType cmp_func_;
    TO_STRING_KV(K_(col_idx), K_(obj_type), K_(cs_type), K_(precision), K_(scale));
  };

private:
  int ret_;
  const sql::RowMeta *row_meta_;
  common::ObArray<CmpInfo> cmp_infos_;
  bool enable_encode_sortkey_;
};

// 提供 DDL 排序实现的线程级复用

class ObDDLSortProvider final
{
public:
  using StoreRow = sql::ObSortKeyStore<false>;
  using Compare = ObInvertedIndexCompare<StoreRow>;
  using SortImpl = sql::ObStorageVecSortImpl<Compare, StoreRow, false>;
  using ChunkType = sql::ObSortVecOpChunk<StoreRow, false>;

public:
  ObDDLSortProvider();
  ~ObDDLSortProvider();
  int init(ObDDLIndependentDag *ddl_dag);
  int get_sort_impl(SortImpl *&sort_impl);
  int get_sort_impl(const int64_t slice_id, SortImpl *&sort_impl);
  int finish_sort_impl(const int64_t slice_id);
  int get_final_merge_ways(int64_t &final_merge_ways);
  int set_sort_handle_status(const int64_t slice_id, bool is_in_use);
  void destroy();
  const sql::RowMeta &get_sk_row_meta() const { return sk_row_meta_; }
  static int build_row_meta_from_schema(const ObDDLTableSchema &ddl_schema, ObIAllocator &allocator, sql::RowMeta &sk_row_meta);
  static int build_full_row_meta_from_schema(const ObDDLTableSchema &ddl_schema, const bool enable_encode_sortkey, ObIAllocator &rowmeta_allocator, sql::RowMeta &full_row_meta);
  TO_STRING_KV(K_(is_inited), KP_(ddl_dag), K(sort_impl_map_.size()), K(slice_sort_impl_map_.size()), K_(sk_row_meta), K_(tempstore_read_alignment_size), K_(enable_encode_sortkey));
private:
  struct SortHandle
  {
  public:
    SortHandle() : mem_ctx_(nullptr), impl_(nullptr), compare_(nullptr), slice_decider_(nullptr), in_use_(false) {}
    bool is_valid() const { return OB_NOT_NULL(mem_ctx_) && OB_NOT_NULL(impl_) && OB_NOT_NULL(compare_); }
    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, "SortHandle(mem_ctx=%p, impl=%p, compare=%p, slice_decider=%p, in_use=%d)",
                              mem_ctx_.ref_context(), impl_, compare_, slice_decider_, in_use_);
      return pos;
    }

  public:
    lib::MemoryContext mem_ctx_;
    SortImpl *impl_;
    Compare *compare_;
    ObSortChunkSliceDecider<Compare, StoreRow> *slice_decider_;
    bool in_use_;
  };

  struct SetCallback
  {
  public:
    SetCallback(bool is_in_use);
    ~SetCallback() = default;
    void operator()(common::hash::HashMapPair<int64_t, SortHandle *> &pair);

  public:
    int ret_code_;
    bool is_concurrent_;
    bool is_in_use_; // true: try to set in use, false: try to set not in use
  };

private:
  int build_row_meta_from_schema(const ObDDLTableSchema &ddl_schema);
  int create_sort_handle(const bool need_slice_decider, ObDDLSortProvider::SortHandle *&handle);
  void clean_sort_handle(ObDDLSortProvider::SortHandle *&handle);
  int init_sort_impl(const bool need_slice_decider, ObDDLSortProvider::SortHandle *&handle);
  int reuse_sort_impl(ObDDLSortProvider::SortHandle *handle);

private:
  bool is_inited_;
  ObDDLIndependentDag *ddl_dag_;
  ObMonitorNode op_monitor_info_;
  sql::RowMeta sk_row_meta_;
  ObArenaAllocator rowmeta_allocator_;
  int64_t tempstore_read_alignment_size_;
  // Use pointer to SortHandle because SortImpl holds a reference to mem_ctx_, and storing SortHandle objects directly
  // would invalidate the memory context reference
  common::hash::ObHashMap<uint64_t, SortHandle *> sort_impl_map_;
  common::hash::ObHashMap<int64_t, SortHandle *> slice_sort_impl_map_;
  bool enable_encode_sortkey_;
  // Reuse queue for completed slice sort handles
  common::ObSEArray<SortHandle *, 64> reuse_queue_;
  lib::ObMutex reuse_queue_lock_;
};

// -------------------------------- ObInvertedIndexCompare 模板实现 --------------------------------
template <typename Store_Row>
ObInvertedIndexCompare<Store_Row>::ObInvertedIndexCompare()
  : ret_(OB_SUCCESS),
    row_meta_(nullptr),
    cmp_infos_(),
    enable_encode_sortkey_(false)
  {}

template <typename Store_Row>
int ObInvertedIndexCompare<Store_Row>::init(const ObDDLTableSchema *ddl_table_schema, const sql::RowMeta *row_meta, const bool enable_encode_sortkey, const bool rowkey_only)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(ddl_table_schema) || OB_ISNULL(row_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ddl_table_schema is null or row_meta is null", K(ret), KP(ddl_table_schema), KP(row_meta));
  } else if (OB_UNLIKELY(ddl_table_schema->column_items_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ddl_table_schema is null or column_items is empty", K(ret));
  } else {
    row_meta_ = row_meta;
    enable_encode_sortkey_ = enable_encode_sortkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_table_schema->column_items_.count(); ++i) {
      const ObColumnSchemaItem &col_item = ddl_table_schema->column_items_.at(i);
      if (rowkey_only && !col_item.is_rowkey_column_) {
        continue;
      }
      CmpInfo info;
      info.col_idx_ = static_cast<int32_t>(i) + (enable_encode_sortkey ? 1 : 0);
      info.obj_type_ = col_item.col_type_.get_type();
      info.cs_type_ = col_item.col_type_.get_collation_type();
      info.precision_ = col_item.col_accuracy_.get_precision();
      info.scale_ = col_item.col_accuracy_.get_scale();
      info.cmp_func_ = common::ObDatumFuncs::get_nullsafe_cmp_func(
          info.obj_type_, info.obj_type_, common::NULL_LAST, info.cs_type_,
          info.scale_, lib::is_oracle_mode(), false /*has_lob_header*/,
          info.precision_, info.precision_);
      if (OB_ISNULL(info.cmp_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cmp_func is null", K(ret), K(i));
      } else if (OB_FAIL(cmp_infos_.push_back(info))) {
        LOG_WARN("failed to push back cmp_info", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && cmp_infos_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no column found", K(ret));
    }
  }
  return ret;
}

template <typename Store_Row>
void ObInvertedIndexCompare<Store_Row>::reset()
{
  ret_ = OB_SUCCESS;
  row_meta_ = nullptr;
  cmp_infos_.reset();
  enable_encode_sortkey_ = false;
}

template <typename Store_Row>
void ObInvertedIndexCompare<Store_Row>::reuse()
{
  ret_ = OB_SUCCESS;
  row_meta_ = nullptr;
  cmp_infos_.reuse();
  enable_encode_sortkey_ = false;
}

template <typename Store_Row>
bool ObInvertedIndexCompare<Store_Row>::operator()(const Store_Row *l, const Store_Row *r)
{
  int ret = ret_;
  bool fallback = false;
  bool is_l_less_than_r = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r) || OB_ISNULL(row_meta_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l), KP(r), KP(row_meta_));
  } else if (enable_encode_sortkey_) {
    const ObDatum l_datum = l->get_datum(*row_meta_, 0);
    const ObDatum r_datum = r->get_datum(*row_meta_, 0);
    if (OB_LIKELY(!l_datum.is_null() && !r_datum.is_null())) {
      int cmp = MEMCMP(l_datum.ptr_, r_datum.ptr_, min(l_datum.len_, r_datum.len_));
      is_l_less_than_r = cmp != 0 ? (cmp < 0) : (l_datum.len_ < r_datum.len_);
    } else {
      fallback = true;
    }
  }
  if (OB_SUCC(ret) && (!enable_encode_sortkey_ || fallback)) {
    int cmp = 0;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < cmp_infos_.count(); ++i) {
      const CmpInfo &info = cmp_infos_.at(i);
      const ObDatum l_datum = l->get_datum(*row_meta_, info.col_idx_);
      const ObDatum r_datum = r->get_datum(*row_meta_, info.col_idx_);
      if (OB_ISNULL(info.cmp_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cmp_func is null", K(ret), K(i));
      } else if (OB_FAIL(info.cmp_func_(l_datum, r_datum, cmp))) {
        LOG_WARN("failed to compare datum", K(ret), K(cmp), K(l_datum), K(r_datum));
      }
    }
    if (OB_SUCC(ret) && cmp < 0) {
      is_l_less_than_r = true;
    }
  }
  ret_ = ret;
  return is_l_less_than_r;
}

template <typename Store_Row>
bool ObInvertedIndexCompare<Store_Row>::operator()(
    const sql::ObSortVecOpChunk<Store_Row, false> *l,
    const sql::ObSortVecOpChunk<Store_Row, false> *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l), KP(r));
  } else {
    less = operator()(r->sk_row_, l->sk_row_);
  }
  return less;
}

template <typename Store_Row>
int ObInvertedIndexCompare<Store_Row>::compare(const Store_Row *l, const ObIArray<ObDatum> &r_datums)
{
  int &ret = ret_;
  int cmp = 0;
  const int64_t col_cnt = cmp_infos_.count();
  // l_datums and r_datums can be different in length.
  int cmp_length = r_datums.count() > col_cnt ? col_cnt : r_datums.count();
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_UNLIKELY(col_cnt < cmp_length) || OB_ISNULL(l)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(r_datums.count()), K(col_cnt), KP(l));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < cmp_length; ++i) {
      const ObDatum l_datum = l->get_datum(*row_meta_, cmp_infos_.at(i).col_idx_);
      const CmpInfo &info = cmp_infos_.at(i);
      if (OB_FAIL(info.cmp_func_(l_datum, r_datums.at(i), cmp))) {
        LOG_WARN("failed to compare datum", K(ret), K(cmp), K(l_datum), K(r_datums.at(i)));
      }
    }
  }
  return cmp;
}

template <typename Store_Row>
bool ObInvertedIndexCompare<Store_Row>::operator()(
    const ObIArray<ObDatum> &l_datums,
    const Store_Row *r)
{
  int &ret = ret_;
  bool is_less = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(r));
  } else {
    int cmp = compare(r, l_datums);
    if (OB_SUCC(ret)) {
      is_less = cmp > 0;
    }
  }
  return is_less;
}

template <typename Store_Row>
bool ObInvertedIndexCompare<Store_Row>::operator()(
    const Store_Row *l,
    const ObIArray<ObDatum> &r_datums)
{
  int &ret = ret_;
  bool is_less = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l));
  } else {
    int cmp = compare(l, r_datums);
    if (OB_SUCC(ret)) {
      is_less = cmp < 0;
    }
  }
  return is_less;
}


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_DDL_SORT_PROVIDER_H_
