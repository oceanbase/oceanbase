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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_table_compactor.h"
#include "observer/table_load/ob_table_load_general_table_compactor.h"
#include "observer/table_load/ob_table_load_merger.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_mem_compactor.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "observer/table_load/ob_table_load_multiple_heap_table_compactor.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "observer/table_load/ob_table_load_parallel_merge_table_compactor.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace storage;

/**
 * ObTableLoadTableCompactResult
 */

ObTableLoadTableCompactResult::ObTableLoadTableCompactResult()
  : allocator_("TLD_TCResult"), tablet_result_map_(64)
{
  allocator_.set_tenant_id(MTL_ID());
  all_table_array_.set_tenant_id(MTL_ID());
}

ObTableLoadTableCompactResult::~ObTableLoadTableCompactResult()
{
  reset();
}

void ObTableLoadTableCompactResult::reset()
{
  for (int64_t i = 0; i < all_table_array_.count(); ++i) {
    ObIDirectLoadPartitionTable *table = all_table_array_.at(i);
    table->~ObIDirectLoadPartitionTable();
    allocator_.free(table);
  }
  all_table_array_.reset();
  allocator_.reset();
  tablet_result_map_.reset();
}

int ObTableLoadTableCompactResult::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_result_map_.init("TLD_TCResult", MTL_ID()))) {
    LOG_WARN("fail to init link hash map", KR(ret));
  }
  return ret;
}

int ObTableLoadTableCompactResult::add_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table));
  } else {
    const ObTabletID &tablet_id = table->get_tablet_id();
    ObTableLoadTableCompactTabletResult *tablet_result = nullptr;
    if (OB_FAIL(tablet_result_map_.get(tablet_id, tablet_result))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get", KR(ret));
      } else {
        if (OB_FAIL(tablet_result_map_.create(tablet_id, tablet_result))) {
          LOG_WARN("fail to create", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_result->table_array_.push_back(table))) {
        LOG_WARN("fail to push back", KR(ret));
      } else if (OB_FAIL(all_table_array_.push_back(table))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_NOT_NULL(tablet_result)) {
      tablet_result_map_.revert(tablet_result);
    }
  }
  return ret;
}

void ObTableLoadTableCompactResult::release_all_table_data()
{
  for (int64_t i = 0; i < all_table_array_.count(); ++i) {
    ObIDirectLoadPartitionTable *table = all_table_array_.at(i);
    table->release_data();
  }
}

/**
 * ObTableLoadTableCompactConfig
 */

int ObTableLoadTableCompactConfigMainTable::handle_table_compact_success()
{
  // notify merger
  return merger_->handle_table_compact_success();
}

int ObTableLoadTableCompactConfigMainTable::get_tables(common::ObIArray<storage::ObIDirectLoadPartitionTable *> &table_array,
               common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadTransStore *> trans_store_array;
  trans_store_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(store_ctx_->get_committed_trans_stores(trans_store_array))) {
    LOG_WARN("fail to get committed trans stores", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_store_array.count(); ++i) {
    ObTableLoadTransStore *trans_store = trans_store_array.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < trans_store->session_store_array_.count(); ++j) {
      const ObTableLoadTransStore::SessionStore *session_store = trans_store->session_store_array_.at(j);
      for (int64_t k = 0; OB_SUCC(ret) && k < session_store->partition_table_array_.count(); ++k) {
        ObIDirectLoadPartitionTable *table = session_store->partition_table_array_.at(k);
        ObDirectLoadExternalTable *external_table = nullptr;
        ObDirectLoadExternalTable *copied_external_table = nullptr;
        if (OB_ISNULL(external_table = dynamic_cast<ObDirectLoadExternalTable *>(table))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table", KR(ret), K(i), KPC(table));
        } else if (OB_ISNULL(copied_external_table =
                               OB_NEWx(ObDirectLoadExternalTable, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new external table", KR(ret));
        } else if (OB_FAIL(copied_external_table->copy(*external_table))) {
          LOG_WARN("fail to copy external table", KR(ret));
        } else if (OB_FAIL(table_array.push_back(copied_external_table))) {
          LOG_WARN("fail to add tablet table", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != copied_external_table) {
            copied_external_table->~ObDirectLoadExternalTable();
            copied_external_table = nullptr;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    store_ctx_->clear_committed_trans_stores();
  }
  return ret;
}

int ObTableLoadTableCompactConfigMainTable::init(ObTableLoadStoreCtx *store_ctx, ObTableLoadMerger &merger)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx));
  } else {
    store_ctx_ = store_ctx;
    merger_ = &merger;
  }
  return ret;
}

ObTableLoadTableCompactConfigLobIdTable::ObTableLoadTableCompactConfigLobIdTable() : merger_(nullptr)
{

}


ObTableLoadTableCompactConfigLobIdTable::~ObTableLoadTableCompactConfigLobIdTable()
{

}
int ObTableLoadTableCompactConfigLobIdTable::init(ObTableLoadMerger &merger)
{
  int ret = OB_SUCCESS;
  merger_ = &merger;
  is_sort_lobid_ = true;
  return ret;
}

int ObTableLoadTableCompactConfigLobIdTable::handle_table_compact_success()
{
  // notify merger
  return merger_->handle_lob_id_compact_success();
}

int ObTableLoadTableCompactConfigLobIdTable::get_tables(common::ObIArray<storage::ObIDirectLoadPartitionTable *> &table_array,
               common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  FOREACH_X(item, merger_->get_merge_ctx().get_table_builder_map(), OB_SUCC(ret)) {
    if (OB_FAIL(item->second->get_tables(table_array, allocator))) {
      LOG_WARN("fail to get tables", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadTableCompactCtx
 */

ObTableLoadTableCompactCtx::ObTableLoadTableCompactCtx()
  : store_ctx_(nullptr), compact_config_(nullptr)
{
}

ObTableLoadTableCompactCtx::~ObTableLoadTableCompactCtx()
{
}

int ObTableLoadTableCompactCtx::init(ObTableLoadStoreCtx *store_ctx, ObTableLoadTableCompactConfig *compact_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == store_ctx || nullptr == compact_config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx), KP(compact_config));
  } else {
    if (OB_FAIL(result_.init())) {
      LOG_WARN("fail to init result", KR(ret));
    } else {
      store_ctx_ = store_ctx;
      compact_config_ = compact_config;
    }
  }
  return ret;
}

bool ObTableLoadTableCompactCtx::is_valid() const
{
  return nullptr != store_ctx_ && nullptr != compact_config_;
}

int ObTableLoadTableCompactCtx::new_compactor(ObTableLoadTableCompactorHandle &compactor_handle)
{
  int ret = OB_SUCCESS;
  compactor_handle.reset();
  ObTableLoadTableCompactor *compactor = nullptr;
  obsys::ObWLockGuard guard(rwlock_);
  {
    ObMemAttr attr(MTL_ID(), "TLD_Compactor");
    if (compact_config_->is_sort_lobid_) {
      compactor = OB_NEW(ObTableLoadMemCompactor, attr);
    } else {
      if (store_ctx_->is_multiple_mode_) {
        if (store_ctx_->table_data_desc_.is_heap_table_) {
          compactor = OB_NEW(ObTableLoadMultipleHeapTableCompactor, attr);
        } else {
          compactor = OB_NEW(ObTableLoadMemCompactor, attr);
        }
      } else {
        // 有主键表不排序
        compactor = OB_NEW(ObTableLoadParallelMergeTableCompactor, attr);
      }
    }
    if (OB_ISNULL(compactor)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTableCompactor", KR(ret));
    } else if (OB_FAIL(compactor_handle_.set_compactor(compactor))) {
      LOG_WARN("fail to set compactor", KR(ret));
    } else {
      compactor_handle = compactor_handle_;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != compactor) {
        OB_DELETE(ObTableLoadTableCompactor, attr, compactor);
        compactor = nullptr;
      }
    }
  }
  return ret;
}

void ObTableLoadTableCompactCtx::release_compactor()
{
  ObTableLoadTableCompactorHandle compactor_handle;
  {
    obsys::ObWLockGuard guard(rwlock_);
    compactor_handle = compactor_handle_;
    compactor_handle_.reset();
  }
}

int ObTableLoadTableCompactCtx::get_compactor(ObTableLoadTableCompactorHandle &compactor_handle)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(rwlock_);
  compactor_handle = compactor_handle_;
  return ret;
}

int ObTableLoadTableCompactCtx::start()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactorHandle compactor_handle;
  if (OB_FAIL(new_compactor(compactor_handle))) {
    LOG_WARN("fail to new compactor", KR(ret));
  } else if (OB_UNLIKELY(!compactor_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid compactor handle", KR(ret), K(compactor_handle));
  } else {
    ObTableLoadTableCompactor *compactor = compactor_handle.get_compactor();
    if (OB_FAIL(compactor->init(this))) {
      LOG_WARN("fail to init compactor", KR(ret));
    } else if (OB_FAIL(compactor->start())) {
      LOG_WARN("fail to start compactor", KR(ret));
    }
  }
  return ret;
}

void ObTableLoadTableCompactCtx::stop()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactorHandle compactor_handle;
  if (OB_FAIL(get_compactor(compactor_handle))) {
    LOG_WARN("fail to get compactor", KR(ret));
  } else if (compactor_handle.is_valid()) {
    ObTableLoadTableCompactor *compactor = compactor_handle.get_compactor();
    compactor->stop();
  }
}

int ObTableLoadTableCompactCtx::handle_table_compact_success()
{
  int ret = OB_SUCCESS;
  // release compactor
  release_compactor();

  if (compact_config_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("compact_config_  is nullptr", KR(ret));
  } else if (OB_FAIL(compact_config_->handle_table_compact_success())) {
    LOG_WARN("fail to handle_table_compact_success", KR(ret));
  }
  return ret;
}

/**
 * ObTableLoadTableCompactor
 */

ObTableLoadTableCompactor::ObTableLoadTableCompactor()
  : compact_ctx_(nullptr), ref_cnt_(0), is_inited_(false)
{
}

ObTableLoadTableCompactor::~ObTableLoadTableCompactor()
{
}

int ObTableLoadTableCompactor::init(ObTableLoadTableCompactCtx *compact_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == compact_ctx || !compact_ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(compact_ctx));
  } else {
    compact_ctx_ = compact_ctx;
    if (OB_FAIL(inner_init())) {
      LOG_WARN("fail to inner init", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

/**
 * ObTableLoadTableCompactorHandle
 */

ObTableLoadTableCompactorHandle &ObTableLoadTableCompactorHandle::operator =(const ObTableLoadTableCompactorHandle &other)
{
  if (this != &other) {
    reset();
    if (OB_NOT_NULL(other.compactor_)) {
      compactor_ = other.compactor_;
      compactor_->inc_ref();
    }
  }
  return *this;
}

void ObTableLoadTableCompactorHandle::reset()
{
  if (nullptr != compactor_) {
    const int64_t ref_cnt = compactor_->dec_ref();
    if (0 == ref_cnt) {
      ObMemAttr attr(MTL_ID(), "TLD_Compactor");
      OB_DELETE(ObTableLoadTableCompactor, attr, compactor_);
    }
    compactor_ = nullptr;
  }
}

bool ObTableLoadTableCompactorHandle::is_valid() const
{
  return nullptr != compactor_;
}

int ObTableLoadTableCompactorHandle::set_compactor(ObTableLoadTableCompactor *compactor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compactor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(compactor));
  } else {
    reset();
    compactor_ = compactor;
    compactor_->inc_ref();
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
