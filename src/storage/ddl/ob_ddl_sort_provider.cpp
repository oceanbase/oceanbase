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

#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/rc/ob_rc.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ddl/ob_ddl_sort_provider.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::storage;


ObDDLSortProvider::ObDDLSortProvider()
  : is_inited_(false),
    ddl_dag_(nullptr),
    op_monitor_info_(),
    sk_row_meta_(),
    rowmeta_allocator_(ObMemAttr(MTL_ID(), "DDLSortRowMeta")),
    tempstore_read_alignment_size_(0),
    sort_impl_map_(),
    slice_sort_impl_map_(),
    enable_encode_sortkey_(false),
    reuse_queue_(),
    reuse_queue_lock_(common::ObLatchIds::DDL_SORT_PROVIDER_LOCK)
{
}

ObDDLSortProvider::~ObDDLSortProvider()
{
  destroy();
}

int ObDDLSortProvider::init(ObDDLIndependentDag *ddl_dag)
{
  int ret = OB_SUCCESS;
  const ObDDLTableSchema *ddl_table_schema = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLSortProvider init twice", K(ret));
  } else if (OB_ISNULL(ddl_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_dag));
  } else if (OB_FAIL(sort_impl_map_.create(64, ObMemAttr(MTL_ID(), "DDLSortMap")))) {
    LOG_WARN("create sort impl map failed", K(ret));
  } else if (OB_FAIL(slice_sort_impl_map_.create(64, ObMemAttr(MTL_ID(), "DDLSliceSortMap")))) {
    LOG_WARN("create slice sort impl map failed", K(ret));
  } else if (OB_FAIL(ddl_dag->get_sort_ddl_table_schema(ddl_table_schema))) {
    LOG_WARN("get sort ddl table schema failed", K(ret));
  } else if (OB_ISNULL(ddl_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, ddl table schema is null", K(ret));
  } else if (OB_FAIL(ddl_dag->check_enable_encode_sortkey(enable_encode_sortkey_))) {
    LOG_WARN("failed to check enable encode sortkey", K(ret));
  } else if (OB_FAIL(build_full_row_meta_from_schema(*ddl_table_schema, enable_encode_sortkey_, rowmeta_allocator_, sk_row_meta_))) {
    LOG_WARN("build full row meta failed", K(ret));
  } else {
    ddl_dag_ = ddl_dag;
    tempstore_read_alignment_size_ = ObTempBlockStore::get_read_alignment_size_config(MTL_ID());
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObDDLSortProvider::init_sort_impl(const bool need_slice_decider, ObDDLSortProvider::SortHandle *&handle)
{
  int ret = OB_SUCCESS;
  const int64_t max_batch_size = ObDDLIndependentDag::DEFAULT_ROW_BUFFER_SIZE;
  const ObCompressorType compressor_type = ddl_dag_->get_ddl_table_schema().table_item_.compress_type_;
  // Use typedef to avoid macro parsing issues with template parameters containing comma
  using SliceDeciderType = ObSortChunkSliceDecider<Compare, StoreRow>;
  const ObPxTabletRange * slice_range = nullptr;
  const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids = ddl_dag_->get_ls_tablet_ids();
  if (need_slice_decider) {
    if (ls_tablet_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, ls tablet ids count is not 1", K(ret), K(ls_tablet_ids.count()));
    } else {
      const ObTabletID &tablet_id = ls_tablet_ids.at(0).second;
      ObDDLTabletContext *tablet_context = nullptr;
      if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id, tablet_context))) {
        LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
      } else if (OB_ISNULL(tablet_context)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, tablet context is null", K(ret), K(tablet_id));
      } else if (OB_ISNULL(handle->slice_decider_ = OB_NEWx(SliceDeciderType, &(handle->mem_ctx_)->get_malloc_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc slice decider failed", K(ret));
      } else if (tablet_context->get_inverted_final_sample_range().range_cut_.count() > 0) {
        slice_range = &tablet_context->get_inverted_final_sample_range();
      } else {
        slice_range = nullptr;//no slice range
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(handle->slice_decider_->init(slice_range, handle->compare_))) {
        LOG_WARN("init slice decider failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(handle->impl_->init(handle->compare_,
                                  &sk_row_meta_,
                                  nullptr /*addon_row_meta*/,
                                  max_batch_size,
                                  ObMemAttr(MTL_ID(), "DDLSortImpl", ObCtxIds::WORK_AREA),
                                  compressor_type,
                                  false /*enable_trunc*/,
                                  tempstore_read_alignment_size_,
                                  MTL_ID(),
                                  enable_encode_sortkey_,
                                  handle->slice_decider_ /*slice_decider*/))) {
    LOG_WARN("sort impl init failed", K(ret));
  }
  return ret;
}

int ObDDLSortProvider::get_final_merge_ways(int64_t &final_merge_ways)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider not init", K(ret));
  } else {
    const int64_t MAX_MERGE_MEMORY_PER_THREAD = 16 * 1024 * 1024; // 16MB
    final_merge_ways = MAX_MERGE_MEMORY_PER_THREAD / max(tempstore_read_alignment_size_, ObTempBlockStore::BLOCK_CAPACITY);
    final_merge_ways = std::max(2L, final_merge_ways);
  }
  return ret;
}

int ObDDLSortProvider::build_row_meta_from_schema(const ObDDLTableSchema &ddl_schema, ObIAllocator &rowmeta_allocator, sql::RowMeta &sk_row_meta)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_cnt = ddl_schema.table_item_.rowkey_column_num_;
  const ObIArray<ObColumnSchemaItem> &column_items = ddl_schema.column_items_;
  ObArray<sql::ColMetaInfo> expr_infos;
  if (OB_UNLIKELY(rowkey_cnt <= 0 || column_items.count() < rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey schema", K(ret), K(rowkey_cnt), K(column_items.count()));
  } else if (OB_FAIL(expr_infos.reserve(rowkey_cnt))) {
    LOG_WARN("reserve expr infos failed", K(ret), K(rowkey_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    const ObColumnSchemaItem &column_item = column_items.at(i);
    if (is_fixed_length(column_item.col_type_.get_type())) {
      int16_t fixed_length = get_type_fixed_length(column_item.col_type_.get_type());
      if (OB_FAIL(expr_infos.push_back(sql::ColMetaInfo(true /*is_fixed*/, fixed_length)))) {
        LOG_WARN("push back col meta info failed", K(ret), K(i));
      }
    } else {
      if (OB_FAIL(expr_infos.push_back(sql::ColMetaInfo(false /*is_fixed*/, 0 /*fixed_len*/)))) {
        LOG_WARN("push back col meta info failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int32_t extra_size = StoreRow::get_extra_size(true /*is_sort_key*/);
    if (OB_FAIL(sk_row_meta.init(expr_infos, extra_size,
                                  false /*reorder_fixed_expr*/,
                                  &rowmeta_allocator))) {
      LOG_WARN("init row meta failed", K(ret), K(rowkey_cnt), K(extra_size));
    }
  }
  return ret;
}

/* make all column in row become a sort key */
int ObDDLSortProvider::build_full_row_meta_from_schema(const ObDDLTableSchema &ddl_schema, const bool enable_encode_sortkey, ObIAllocator &rowmeta_allocator, sql::RowMeta &full_row_meta)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_cnt = ddl_schema.table_item_.rowkey_column_num_;
  const ObIArray<ObColumnSchemaItem> &column_items = ddl_schema.column_items_;
  const int64_t info_cnt = column_items.count() + (enable_encode_sortkey ? 1 : 0);
  ObArray<sql::ColMetaInfo> expr_infos;
  if (OB_UNLIKELY(rowkey_cnt <= 0 || column_items.count() < rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey schema", K(ret), K(rowkey_cnt), K(column_items.count()));
  } else if (OB_FAIL(expr_infos.reserve(info_cnt))) {
    LOG_WARN("reserve expr infos failed", K(ret), K(column_items.count()), K(info_cnt));
  } else if (enable_encode_sortkey) {
    if (OB_FAIL(expr_infos.push_back(sql::ColMetaInfo(false /*is_fixed*/, 0 /*fixed_len*/)))) {
      LOG_WARN("push back col meta info failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
    const ObColumnSchemaItem &column_item = column_items.at(i);
    if (is_fixed_length(column_item.col_type_.get_type())) {
      int16_t fixed_length = get_type_fixed_length(column_item.col_type_.get_type());
      if (OB_FAIL(expr_infos.push_back(sql::ColMetaInfo(true /*is_fixed*/, fixed_length)))) {
        LOG_WARN("push back col meta info failed", K(ret), K(i));
      }
    } else {
      if (OB_FAIL(expr_infos.push_back(sql::ColMetaInfo(false /*is_fixed*/, 0 /*fixed_len*/)))) {
        LOG_WARN("push back col meta info failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int32_t extra_size = StoreRow::get_extra_size(true /*is_sort_key*/);
    if (OB_FAIL(full_row_meta.init(expr_infos, extra_size,
                                  false /*reorder_fixed_expr*/,
                                  &rowmeta_allocator))) {
      LOG_WARN("init row meta failed", K(ret), "col_cnt", column_items.count(), K(extra_size));
    }
  }
  return ret;
}

int ObDDLSortProvider::get_sort_impl(SortImpl *&sort_impl)
{
  int ret = OB_SUCCESS;
  sort_impl = nullptr;
  const uint64_t tid = GETTID();
  ObDDLSortProvider::SortHandle *handle = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider not init", K(ret));
  } else if (OB_FAIL(sort_impl_map_.get_refactored(tid, handle))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get sort impl from map failed", K(ret), K(tid));
    } else {
      if (OB_FAIL(create_sort_handle(false/*need_slice_decider*/, handle))) {
        LOG_WARN("create sort handle failed", K(ret), K(tid));
      } else if (OB_FAIL(sort_impl_map_.set_refactored(tid, handle, 0 /*overwrite*/))) {
        LOG_WARN("put sort impl failed", K(ret), K(tid));
        clean_sort_handle(handle);
      } else {
        sort_impl = handle->impl_;
      }
    }
  } else if (OB_ISNULL(handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort handle is null", K(ret), K(tid));
  } else {
    handle->impl_->reset();
    if (OB_FAIL(init_sort_impl(false/*need_slice_decider*/, handle))) {
      LOG_WARN("init sort impl failed", K(ret));
    } else {
      sort_impl = handle->impl_;
    }
  }
  return ret;
}

int ObDDLSortProvider::get_sort_impl(const int64_t slice_id, SortImpl *&sort_impl)
{
  int ret = OB_SUCCESS;
  sort_impl = nullptr;
  ObDDLSortProvider::SortHandle *handle = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider not init", K(ret));
  } else if (OB_FAIL(slice_sort_impl_map_.get_refactored(slice_id, handle))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get sort impl from map failed", K(ret), K(slice_id));
    } else {
      //overwrite the ret_code
      // Try to get a reusable handle from the reuse queue first
      {
        lib::ObMutexGuard guard(reuse_queue_lock_);
        if (!reuse_queue_.empty()) {
          handle = reuse_queue_.at(reuse_queue_.count() - 1);
          reuse_queue_.pop_back();
          LOG_INFO("reuse sort handle from queue", K(slice_id), K(reuse_queue_.count()));
        }
      }

      if (OB_ISNULL(handle) && OB_FAIL(create_sort_handle(true/*need_slice_decider*/, handle))) {
        LOG_WARN("create sort handle failed", K(ret), K(slice_id));
      } else if (OB_FAIL(slice_sort_impl_map_.set_refactored(slice_id, handle, 0 /*overwrite*/))) {
        LOG_WARN("put sort impl failed", K(ret), K(slice_id));
        clean_sort_handle(handle);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_sort_handle_status(slice_id, true/*is_in_use*/))) {
      LOG_WARN("set sort handle in use failed", K(ret), K(slice_id));
    } else {
      sort_impl = handle->impl_;
    }
  }
  return ret;
}

// SetCallback 实现
ObDDLSortProvider::SetCallback::SetCallback(bool is_in_use)
  : ret_code_(OB_SUCCESS), is_concurrent_(false), is_in_use_(is_in_use) {}

void ObDDLSortProvider::SetCallback::operator()(common::hash::HashMapPair<int64_t, ObDDLSortProvider::SortHandle *> &pair)
{
  SortHandle *handle = pair.second;
  if (OB_ISNULL(handle)) {
    ret_code_ = OB_ERR_UNEXPECTED;
  } else if (handle->in_use_ == is_in_use_) { // we expected the status is different from the actual status
    // 检测到并发访问：该 handle 已经被另一个线程使用
    ret_code_ = OB_ERR_UNEXPECTED;
    is_concurrent_ = true;
  } else {
    // 设置 in_use_ 标志
    handle->in_use_ = is_in_use_;
  }
}

int ObDDLSortProvider::set_sort_handle_status(const int64_t slice_id, bool is_in_use)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider not init", K(ret));
  } else {
    SetCallback callback(is_in_use);
    if (OB_FAIL(slice_sort_impl_map_.atomic_refactored(slice_id, callback))) {
      if (OB_HASH_NOT_EXIST == ret) {
        LOG_WARN("sort handle not found in map", K(ret), K(slice_id));
      } else {
        LOG_WARN("atomic operation failed", K(ret), K(slice_id));
      }
    } else if (OB_FAIL(callback.ret_code_)) {
      // 回调函数中检测到错误（包括并发访问）
      if (callback.is_concurrent_) {
        LOG_WARN("concurrent access detected: sort handle is already in use", K(ret), K(slice_id));
      } else {
        LOG_WARN("callback failed: sort handle is null", K(ret), K(slice_id));
      }
    }
  }
  return ret;
}

int ObDDLSortProvider::finish_sort_impl(const int64_t slice_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider not init", K(ret));
  } else if (!slice_sort_impl_map_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice sort impl map not created", K(ret));
  } else {
    SortHandle *handle = nullptr;
    if (OB_FAIL(slice_sort_impl_map_.get_refactored(slice_id, handle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("sort impl not found for slice_id", K(slice_id));
      } else {
        LOG_WARN("get sort impl from map failed", K(ret), K(slice_id));
      }
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort handle is null", K(ret), K(slice_id));
    } else {
      if (OB_FAIL(reuse_sort_impl(handle))) {
        LOG_WARN("reuse sort impl failed", K(ret), K(slice_id));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(slice_sort_impl_map_.erase_refactored(slice_id))) {
          LOG_WARN("erase sort impl from map failed", K(tmp_ret), K(slice_id));
        }
      }
    }
  }
  return ret;
}

int ObDDLSortProvider::reuse_sort_impl(ObDDLSortProvider::SortHandle *handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("provider not init", K(ret));
  } else if (OB_ISNULL(handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(handle));
  } else {
    handle->impl_->reset();
    if (OB_NOT_NULL(handle->slice_decider_)) {
      handle->slice_decider_->reuse();
    }
    //since we can reuse slice_decider
    if (OB_FAIL(init_sort_impl(false/*need_slice_decider*/, handle))) {
      LOG_WARN("init sort impl failed, handle will be cleaned up in destroy()", K(ret));
    } else {
      lib::ObMutexGuard guard(reuse_queue_lock_);
      if (OB_FAIL(reuse_queue_.push_back(handle))) {
        LOG_WARN("push handle to reuse queue failed, handle will be cleaned up in destroy()", K(ret));
      } else {
        LOG_INFO("add sort handle to reuse queue", K(reuse_queue_.count()));
      }
    }
  }
  return ret;
}

int ObDDLSortProvider::create_sort_handle(const bool need_slice_decider, SortHandle *&handle)
{
  int ret = OB_SUCCESS;
  handle = nullptr;
  lib::MemoryContext mem_ctx = nullptr;
  lib::ContextParam param;
  param.set_mem_attr(ObMemAttr(MTL_ID(), "DDLSortRow", ObCtxIds::WORK_AREA)).set_properties(lib::ALLOC_THREAD_SAFE);
  // Use ROOT_CONTEXT instead of CURRENT_CONTEXT to avoid thread-local lifetime issues.
  // The memory context may be destroyed in a different thread than it was created.
  const ObDDLTableSchema *ddl_table_schema = nullptr;
  if (OB_FAIL(ddl_dag_->get_sort_ddl_table_schema(ddl_table_schema))) {
    LOG_WARN("get sort ddl table schema failed", K(ret));
  } else if (OB_ISNULL(ddl_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, ddl table schema is null", K(ret));
  } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_ctx, param))) {
    LOG_WARN("create memory context failed", K(ret));
  } else if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory context", K(ret));
  } else if (OB_ISNULL(handle = OB_NEWx(SortHandle, &mem_ctx->get_malloc_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc sort handle failed", K(ret));
  } else if (OB_FALSE_IT(handle->mem_ctx_ = mem_ctx)) {
  } else if (OB_FALSE_IT(mem_ctx = nullptr)) {
    //TODO: ly435438: all sort impl should share one compare instancea
  } else if (OB_ISNULL(handle->compare_ = OB_NEWx(Compare, &(handle->mem_ctx_)->get_malloc_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc compare failed", K(ret));
  } else if (OB_FAIL(handle->compare_->init(ddl_table_schema, &sk_row_meta_, enable_encode_sortkey_))) {
    LOG_WARN("init compare failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(handle->impl_ = OB_NEWx(SortImpl, &(handle->mem_ctx_)->get_malloc_allocator(), handle->mem_ctx_, op_monitor_info_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc sort impl failed", K(ret));
    } else {
      handle->impl_->reset();
      if (OB_FAIL(init_sort_impl(need_slice_decider, handle))) {
        LOG_WARN("init sort impl failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    clean_sort_handle(handle);
    if (OB_NOT_NULL(mem_ctx.ref_context())) {
      DESTROY_CONTEXT(mem_ctx);
    }
  }
  return ret;
}

void ObDDLSortProvider::destroy()
{
  if (sort_impl_map_.created()) {
    common::hash::ObHashMap<uint64_t, SortHandle *>::iterator it;
    for (it = sort_impl_map_.begin(); it != sort_impl_map_.end(); ++it) {
      SortHandle *&handle = it->second;
      clean_sort_handle(handle);
    }
    sort_impl_map_.destroy();
  }
  if (slice_sort_impl_map_.created()) {
    common::hash::ObHashMap<int64_t, SortHandle *>::iterator it;
    for (it = slice_sort_impl_map_.begin(); it != slice_sort_impl_map_.end(); ++it) {
      SortHandle *&handle = it->second;
      clean_sort_handle(handle);
    }
    slice_sort_impl_map_.destroy();
  }
  // Clean up reuse queue
  {
    lib::ObMutexGuard guard(reuse_queue_lock_);
    for (int64_t i = 0; i < reuse_queue_.count(); ++i) {
      SortHandle *handle = reuse_queue_.at(i);
      clean_sort_handle(handle);
    }
    reuse_queue_.reset();
  }
  sk_row_meta_.reset();
  tempstore_read_alignment_size_ = 0;
  ddl_dag_ = nullptr;
  enable_encode_sortkey_ = false;
  is_inited_ = false;
}

void ObDDLSortProvider::clean_sort_handle(SortHandle *&handle)
{
  lib::MemoryContext mem_ctx = OB_NOT_NULL(handle) ? handle->mem_ctx_ : nullptr;
  if (OB_NOT_NULL(handle) && OB_NOT_NULL(mem_ctx)) {
    if (nullptr != handle->impl_) {
      handle->impl_->~SortImpl();
      mem_ctx->get_malloc_allocator().free(handle->impl_);
      handle->impl_ = nullptr;
    }
    if (nullptr != handle->compare_) {
      handle->compare_->~Compare();
      mem_ctx->get_malloc_allocator().free(handle->compare_);
      handle->compare_ = nullptr;
    }
    if (nullptr != handle->slice_decider_) {
      handle->slice_decider_->~ObSortChunkSliceDecider<Compare, StoreRow>();
      mem_ctx->get_malloc_allocator().free(handle->slice_decider_);
      handle->slice_decider_ = nullptr;
    }
    handle->~SortHandle();
    mem_ctx->get_malloc_allocator().free(handle);
    handle = nullptr;
    DESTROY_CONTEXT(mem_ctx);
    mem_ctx = nullptr;
   }
}
