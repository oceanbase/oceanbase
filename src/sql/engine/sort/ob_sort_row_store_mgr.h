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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_SORT_ROW_STORE_MGR_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_SORT_ROW_STORE_MGR_H_

#include "sql/engine/basic/ob_temp_row_store.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace sql
{

template <typename Store_Row, bool has_addon>
class ObSortRowStoreMgr
{
public:
  explicit ObSortRowStoreMgr(ObIAllocator &allocator);
  ~ObSortRowStoreMgr();

  // 基于RowMeta的初始化接口
  int init(const RowMeta &sk_row_meta,
           const RowMeta *addon_row_meta,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const common::ObCompressorType compressor_type,
           const int64_t mem_limit,
           const bool enable_dump,
           const bool enable_trunc = false,
           int64_t tempstore_read_alignment_size = 0);

  // 现有的基于表达式的初始化接口
  int init(const common::ObIArray<ObExpr *> &sk_exprs,
           const common::ObIArray<ObExpr *> *addon_exprs,
           int64_t batch_size,
           bool need_callback,
           int64_t extra_sk_size,
           int64_t extra_addon_size,
           int64_t tenant_id,
           const int64_t mem_limit,
           const bool enable_dump,
           ObCompressorType compress_type = NONE_COMPRESSOR);

  int add_batch(const common::ObIArray<ObExpr *> &sk_exprs,
                const common::ObIArray<ObExpr *> *addon_exprs,
                ObEvalCtx &eval_ctx,
                const ObBatchRows &input_brs,
                int64_t &stored_row_cnt,
                Store_Row **sk_rows,
                Store_Row **addon_rows,
                int64_t &inmem_row_size,
                const int64_t start_pos = 0);

  int add_batch(const common::ObFixedArray<ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
                const common::ObFixedArray<ObIVector *, common::ObIAllocator> *addon_vec_ptrs,
                const ObBitVector &skip,
                const int64_t batch_size,
                int64_t &stored_rows_cnt,
                Store_Row **sk_rows,
                Store_Row **addon_rows,
                int64_t &inmem_row_size);

  int add_batch(const common::ObFixedArray<ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
                const common::ObFixedArray<ObIVector *, common::ObIAllocator> *addon_vec_ptrs,
                const uint16_t selector[],
                const int64_t size,
                Store_Row **sk_rows,
                Store_Row **addon_rows,
                int64_t &inmem_row_size);

  void set_dir_id(int64_t dir_id);
  void set_callback(ObSqlMemoryCallback *callback);
  void set_io_event_observer(ObIOEventObserver *observer);

  void reset();
  void reuse();

  // Add individual row methods for backward compatibility
  int add_sk_row(const ObCompactRow *sk_row, ObCompactRow *&sk_store_row);
  int add_addon_row(const ObCompactRow *addon_row, ObCompactRow *&addon_store_row);

  const RowMeta *get_sk_row_meta() const { return sk_row_meta_; }
  const RowMeta *get_addon_row_meta() const { return addon_row_meta_; }
  int64_t get_row_cnt() const { return sk_store_.get_row_cnt(); }
  int64_t get_mem_hold() const { return sk_store_.get_mem_hold() + addon_store_.get_mem_hold(); }
  int64_t get_file_size() const { return sk_store_.get_file_size() + addon_store_.get_file_size(); }

  ObTempRowStore &get_sk_store() { return sk_store_; }
  ObTempRowStore &get_addon_store() { return addon_store_; }

  void set_sk_row_meta(const RowMeta *meta) { sk_row_meta_ = meta; }
  void set_addon_row_meta(const RowMeta *meta) { addon_row_meta_ = meta; }

  bool is_inited() const { return is_inited_; }

private:
  ObIAllocator &allocator_;
  ObTempRowStore sk_store_;
  ObTempRowStore addon_store_;
  const RowMeta *sk_row_meta_;
  const RowMeta *addon_row_meta_;
  bool is_inited_;
};

template <typename Store_Row, bool has_addon>
ObSortRowStoreMgr<Store_Row, has_addon>::ObSortRowStoreMgr(ObIAllocator &allocator)
  : allocator_(allocator),
    sk_store_(&allocator),
    addon_store_(&allocator),
    sk_row_meta_(nullptr),
    addon_row_meta_(nullptr),
    is_inited_(false)
{}

template <typename Store_Row, bool has_addon>
ObSortRowStoreMgr<Store_Row, has_addon>::~ObSortRowStoreMgr()
{
  reset();
}

template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::init(
    const RowMeta &sk_row_meta,
    const RowMeta *addon_row_meta,
    const int64_t max_batch_size,
    const lib::ObMemAttr &mem_attr,
    const common::ObCompressorType compressor_type,
    const int64_t mem_limit,
    const bool enable_dump,
    const bool enable_trunc,
    int64_t tempstore_read_alignment_size)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "init twice", K(ret));
  } else {
    // 初始化 sk_store_
    if (OB_FAIL(sk_store_.init(sk_row_meta, max_batch_size, mem_attr, mem_limit,
                               enable_dump, compressor_type, enable_trunc,
                               tempstore_read_alignment_size))) {
      SQL_ENG_LOG(WARN, "failed to init sk_store_", K(ret));
    }
    // 初始化 addon_store_ (如果需要)
    else if (has_addon && OB_NOT_NULL(addon_row_meta)) {
      if (OB_FAIL(addon_store_.init(*addon_row_meta, max_batch_size, mem_attr, mem_limit,
                                    enable_dump, compressor_type, enable_trunc,
                                    tempstore_read_alignment_size))) {
        SQL_ENG_LOG(WARN, "failed to init addon_store_", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // 保存RowMeta指针
      sk_row_meta_ = &sk_row_meta;
      addon_row_meta_ = has_addon ? addon_row_meta : nullptr;
      is_inited_ = true;
      SQL_ENG_LOG(INFO, "ObSortRowStoreMgr init success", K(has_addon), K(max_batch_size));
    }
  }

  return ret;
}

// 现有的基于表达式的初始化方法
template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::init(
    const common::ObIArray<ObExpr *> &sk_exprs,
    const common::ObIArray<ObExpr *> *addon_exprs,
    int64_t batch_size,
    bool need_callback,
    int64_t extra_sk_size,
    int64_t extra_addon_size,
    int64_t tenant_id,
    const int64_t mem_limit,
    const bool enable_dump,
    ObCompressorType compress_type)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "init twice", K(ret));
  } else {
    const bool enable_trunc = true;
    const bool reorder_fixed_expr = true;
    int64_t tempstore_read_alignment_size = ObTempBlockStore::get_read_alignment_size_config(tenant_id);
    lib::ObMemAttr mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA);

    if (OB_FAIL(sk_store_.init(sk_exprs, batch_size, mem_attr, mem_limit, enable_dump,
                               extra_sk_size, compress_type, reorder_fixed_expr, enable_trunc,
                               tempstore_read_alignment_size))) {
      SQL_ENG_LOG(WARN, "failed to init sk store", K(ret));
    } else {
      sk_row_meta_ = &sk_store_.get_row_meta();
      if (has_addon && OB_NOT_NULL(addon_exprs)) {
        if (OB_FAIL(addon_store_.init(*addon_exprs, batch_size, mem_attr, mem_limit, enable_dump,
                                      extra_addon_size, compress_type, reorder_fixed_expr,
                                      enable_trunc, tempstore_read_alignment_size))) {
          SQL_ENG_LOG(WARN, "failed to init addon store", K(ret));
        } else {
          addon_row_meta_ = &addon_store_.get_row_meta();
        }
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      SQL_ENG_LOG(INFO, "ObSortRowStoreMgr init success", K(has_addon), K(batch_size));
    }
  }

  return ret;
}

template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::add_batch(
    const common::ObIArray<ObExpr *> &sk_exprs,
    const common::ObIArray<ObExpr *> *addon_exprs,
    ObEvalCtx &eval_ctx,
    const ObBatchRows &input_brs,
    int64_t &stored_row_cnt,
    Store_Row **sk_rows,
    Store_Row **addon_rows,
    int64_t &inmem_row_size,
    const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  inmem_row_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(sk_store_.add_batch(sk_exprs, eval_ctx, input_brs, stored_row_cnt,
                                         reinterpret_cast<ObCompactRow **>(sk_rows), start_pos))) {
    SQL_ENG_LOG(WARN, "failed to add batch to sk store", K(ret));
  } else if (has_addon && OB_NOT_NULL(addon_exprs) &&
             OB_FAIL(addon_store_.add_batch(*addon_exprs, eval_ctx, input_brs, stored_row_cnt,
                                            reinterpret_cast<ObCompactRow **>(addon_rows), start_pos))) {
    SQL_ENG_LOG(WARN, "failed to add batch to addon store", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_row_cnt; i++) {
      inmem_row_size += sk_rows[i]->get_row_size();
      if (has_addon) {
        sk_rows[i]->set_addon_ptr(addon_rows[i], *sk_row_meta_);
        inmem_row_size += addon_rows[i]->get_row_size();
      }
    }
  }

  return ret;
}

template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::add_batch(
    const common::ObFixedArray<ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
    const common::ObFixedArray<ObIVector *, common::ObIAllocator> *addon_vec_ptrs,
    const ObBitVector &skip,
    const int64_t batch_size,
    int64_t &stored_row_cnt,
    Store_Row **sk_rows,
    Store_Row **addon_rows,
    int64_t &inmem_row_size)
{
  int ret = OB_SUCCESS;
  inmem_row_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(sk_store_.add_batch(sk_vec_ptrs, skip, batch_size, stored_row_cnt,
                                         reinterpret_cast<ObCompactRow **>(sk_rows)))) {
    SQL_ENG_LOG(WARN, "failed to add batch to sk store", K(ret));
  } else if (has_addon && OB_NOT_NULL(addon_vec_ptrs) &&
             OB_FAIL(addon_store_.add_batch(*addon_vec_ptrs, skip, batch_size, stored_row_cnt,
                                            reinterpret_cast<ObCompactRow **>(addon_rows)))) {
    SQL_ENG_LOG(WARN, "failed to add batch to addon store", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_row_cnt; i++) {
      inmem_row_size += sk_rows[i]->get_row_size();
      if (has_addon) {
        sk_rows[i]->set_addon_ptr(addon_rows[i], *sk_row_meta_);
        inmem_row_size += addon_rows[i]->get_row_size();
      }
    }
  }

  return ret;
}

template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::add_batch(
    const common::ObFixedArray<ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
    const common::ObFixedArray<ObIVector *, common::ObIAllocator> *addon_vec_ptrs,
    const uint16_t selector[],
    const int64_t batch_size,
    Store_Row **sk_rows,
    Store_Row **addon_rows,
    int64_t &inmem_row_size)
{
  int ret = OB_SUCCESS;
  inmem_row_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(sk_store_.add_batch(sk_vec_ptrs, selector, batch_size,
                                         reinterpret_cast<ObCompactRow **>(sk_rows)))) {
    SQL_ENG_LOG(WARN, "failed to add batch to sk store", K(ret));
  } else if (has_addon && OB_NOT_NULL(addon_vec_ptrs) &&
             OB_FAIL(addon_store_.add_batch(*addon_vec_ptrs, selector, batch_size,
                                            reinterpret_cast<ObCompactRow **>(addon_rows)))) {
    SQL_ENG_LOG(WARN, "failed to add batch to addon store", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      inmem_row_size += sk_rows[i]->get_row_size();
      if (has_addon) {
        sk_rows[i]->set_addon_ptr(addon_rows[i], *sk_row_meta_);
        inmem_row_size += addon_rows[i]->get_row_size();
      }
    }
  }

  return ret;
}

template <typename Store_Row, bool has_addon>
void ObSortRowStoreMgr<Store_Row, has_addon>::set_dir_id(int64_t dir_id)
{
  sk_store_.set_dir_id(dir_id);
  if (has_addon) {
    addon_store_.set_dir_id(dir_id);
  }
}

template <typename Store_Row, bool has_addon>
void ObSortRowStoreMgr<Store_Row, has_addon>::set_callback(ObSqlMemoryCallback *callback)
{
  sk_store_.set_callback(callback);
  if (has_addon) {
    addon_store_.set_callback(callback);
  }
}

template <typename Store_Row, bool has_addon>
void ObSortRowStoreMgr<Store_Row, has_addon>::set_io_event_observer(ObIOEventObserver *observer)
{
  sk_store_.set_io_event_observer(observer);
  if (has_addon) {
    addon_store_.set_io_event_observer(observer);
  }
}

template <typename Store_Row, bool has_addon>
void ObSortRowStoreMgr<Store_Row, has_addon>::reset()
{
  sk_store_.reset();
  addon_store_.reset();
  sk_row_meta_ = nullptr;
  addon_row_meta_ = nullptr;
  is_inited_ = false;
}

template <typename Store_Row, bool has_addon>
void ObSortRowStoreMgr<Store_Row, has_addon>::reuse()
{
  sk_store_.reset();
  if (has_addon) {
    addon_store_.reset();
  }
}

template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::add_sk_row(const ObCompactRow *sk_row, ObCompactRow *&sk_store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else {
    ret = sk_store_.add_row(sk_row, sk_store_row);
  }
  return ret;
}

template <typename Store_Row, bool has_addon>
int ObSortRowStoreMgr<Store_Row, has_addon>::add_addon_row(const ObCompactRow *addon_row, ObCompactRow *&addon_store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else if (!has_addon) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "addon not enabled", K(ret));
  } else {
    ret = addon_store_.add_row(addon_row, addon_store_row);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_SORT_ROW_STORE_MGR_H_ */
