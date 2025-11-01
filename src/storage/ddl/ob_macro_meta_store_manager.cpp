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

#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

ObMacroMetaStoreManager::ObMacroMetaStoreManager()
  : allocator_(ObMemAttr(MTL_ID(), "MbMetaStoreMgr")), mutex_(ObLatchIds::COLUMN_STORE_DDL_RESCAN_LOCK), dir_id_(-1)
{

}

ObMacroMetaStoreManager::~ObMacroMetaStoreManager()
{
  for (int64_t i = 0; i < store_items_.count(); ++i) {
    ObMacroMetaTempStore *&cur_store = store_items_.at(i).macro_meta_store_;
    if (nullptr != cur_store) {
      cur_store->~ObMacroMetaTempStore();
      cur_store = nullptr;
    }
  }
  store_items_.reset();
}

int ObMacroMetaStoreManager::add_macro_meta_store(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, const int64_t lob_start_seq, blocksstable::ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  if (dir_id_ < 0) {
    ObMutexGuard guard(mutex_);
    if (dir_id_ < 0) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id_))) {
        LOG_WARN("alloc dir id failed", K(MTL_ID()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_store_item(tablet_id, cg_idx, parallel_idx, lob_start_seq, dir_id_, macro_meta_store))) {
      LOG_WARN("add macro meta store failed", K(ret), K(dir_id_));
    }
  }
  return ret;
}

int ObMacroMetaStoreManager::add_macro_meta_store(const int64_t cg_idx, const int64_t parallel_idx, const int64_t dir_id, ObMacroMetaTempStore *&macro_meta_store)
{
  static ObTabletID dummy_tablet_id;
  return add_store_item(dummy_tablet_id, cg_idx, parallel_idx, 0/*lob_start_seq*/, dir_id, macro_meta_store);
}

int ObMacroMetaStoreManager::add_store_item(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, const int64_t lob_start_seq, const int64_t dir_id, ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  macro_meta_store = nullptr;
  if (OB_UNLIKELY(cg_idx < 0 || parallel_idx < 0 || dir_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(parallel_idx), K(dir_id));
  } else {
    ObMutexGuard guard(mutex_);
    StoreItem item;
    item.tablet_id_ = tablet_id;
    item.cg_idx_ = cg_idx;
    item.parallel_idx_ = parallel_idx;
    item.lob_start_seq_ = lob_start_seq;
    if (OB_ISNULL(item.macro_meta_store_ = OB_NEWx(ObMacroMetaTempStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(item.macro_meta_store_->init(dir_id))) {
      LOG_WARN("init macro meta store failed", K(ret), K(dir_id));
    } else if (OB_FAIL(store_items_.push_back(item))) {
      LOG_WARN("push back store item failed", K(ret));
    } else {
      macro_meta_store = item.macro_meta_store_;
    }
    if (OB_FAIL(ret) && nullptr != item.macro_meta_store_) {
      item.macro_meta_store_->~ObMacroMetaTempStore();
      allocator_.free(item.macro_meta_store_);
      item.macro_meta_store_ = nullptr;
    }
  }
  return ret;
}

int ObMacroMetaStoreManager::get_sorted_macro_meta_stores(const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores)
{
  static ObTabletID dummy_tablet_id;
  return get_sorted_macro_meta_stores(dummy_tablet_id, cg_idx, macro_meta_stores);
}

int ObMacroMetaStoreManager::get_sorted_macro_meta_stores(const ObTabletID &tablet_id, const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores)
{
  int ret = OB_SUCCESS;
  macro_meta_stores.reset();
  ObArray<StoreItem> temp_items;
  if (OB_UNLIKELY(cg_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < store_items_.count(); ++i) {
      if (cg_idx == store_items_.at(i).cg_idx_ && tablet_id == store_items_.at(i).tablet_id_) {
        if (OB_FAIL(temp_items.push_back(store_items_.at(i)))) {
          LOG_WARN("push back store item failed", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && temp_items.count() > 0) {
    struct {
      bool operator () (const StoreItem &a, const StoreItem &b) { return a.lob_start_seq_ == b.lob_start_seq_ ? a.parallel_idx_ < b.parallel_idx_ : a.lob_start_seq_ < b.lob_start_seq_; }
    } ParallelIdxCmp;
    lib::ob_sort(temp_items.begin(), temp_items.end(), ParallelIdxCmp);
    if (OB_FAIL(macro_meta_stores.reserve(temp_items.count()))) {
      LOG_WARN("reserve macro meta stores failed", K(ret), K(temp_items.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_items.count(); ++i) {
      ObMacroMetaTempStore *cur_store = temp_items.at(i).macro_meta_store_;
      if (OB_ISNULL(cur_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current macro meta store is null", K(ret), K(i), K(temp_items.at(i)));
      } else if (OB_FAIL(macro_meta_stores.push_back(temp_items.at(i)))) {
        LOG_WARN("push back macro meta store failed", K(ret), K(i), KPC(cur_store));
      }
    }
  }
  return ret;
}

int ObMacroMetaStoreManager::get_macro_meta_store(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, blocksstable::ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  macro_meta_store = nullptr;
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; i < store_items_.count(); ++i) {
    if (tablet_id == store_items_.at(i).tablet_id_ &&
        cg_idx == store_items_.at(i).cg_idx_ &&
        parallel_idx == store_items_.at(i).parallel_idx_) {
      macro_meta_store = store_items_.at(i).macro_meta_store_;
      break;
    }
  }
  if (nullptr == macro_meta_store) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}