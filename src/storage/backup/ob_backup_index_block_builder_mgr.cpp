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

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_backup_index_block_builder_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_mds_schema_helper.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace backup
{

// ObBackupTaskIndexRebuilderMgr

ObBackupTaskIndexRebuilderMgr::ObBackupTaskIndexRebuilderMgr()
  : is_inited_(false),
    item_list_(),
    index_builder_mgr_(NULL),
    device_handle_array_(),
    cur_table_key_(),
    index_block_rebuilder_(NULL)
{
}

ObBackupTaskIndexRebuilderMgr::~ObBackupTaskIndexRebuilderMgr()
{
  reset();
}

int ObBackupTaskIndexRebuilderMgr::init(const common::ObIArray<ObBackupProviderItem> &item_list,
    ObBackupTabletIndexBlockBuilderMgr *index_builder_mgr, common::ObIArray<ObIODevice *> &device_handle_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (2 != device_handle_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("device handle is NULL", K(ret), K(device_handle_array));
  } else if (OB_FAIL(item_list_.assign(item_list))) {
    LOG_WARN("failed to assign list", K(ret));
  } else if (OB_FAIL(device_handle_array_.assign(device_handle_array))) {
    LOG_WARN("failed to assign device handle array", K(ret), K(device_handle_array));
  } else {
    index_builder_mgr_ = index_builder_mgr;
    cur_table_key_.reset();
    index_block_rebuilder_ = NULL;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTaskIndexRebuilderMgr::reset()
{
  if (OB_NOT_NULL(index_block_rebuilder_)) {
    index_block_rebuilder_->~ObIndexBlockRebuilder();
    mtl_free(index_block_rebuilder_);
    index_block_rebuilder_ = NULL;
  }
}

int ObBackupTaskIndexRebuilderMgr::prepare_index_block_rebuilder_if_need(
    const ObBackupProviderItem &item, const int64_t *task_idx, bool &is_opened)
{
  int ret = OB_SUCCESS;
  is_opened = false;
  bool is_first = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  LOG_WARN("backup task index rebuilder mgr do not init", K(ret));
  } else if (OB_FAIL(check_is_first_item_for_table_key_(item, is_first))) {
    LOG_WARN("failed to check is first item for table key" ,K(ret), K(item));
  } else if (!is_first) {
    // do nothing
  } else {
    ObIndexBlockRebuilder *rebuilder = NULL;
    const ObITable::TableKey &table_key = item.get_table_key();
    if (OB_FAIL(prepare_index_block_rebuilder_(item, task_idx, device_handle_array_, rebuilder))) {
      LOG_WARN("failed to prepare index block rebuilder", K(ret), K(item));
    } else {
      cur_table_key_ = table_key;
      index_block_rebuilder_ = rebuilder;
      is_opened = true;
    }
  }
  return ret;
}

int ObBackupTaskIndexRebuilderMgr::get_index_block_rebuilder(
    const storage::ObITable::TableKey &table_key, blocksstable::ObIndexBlockRebuilder *&rebuilder)
{
  int ret = OB_SUCCESS;
  rebuilder = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task index rebuilder mgr do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(table_key));
  } else if (table_key != cur_table_key_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur table key not equal to table key", K(ret), K(table_key), K_(cur_table_key));
  } else if (OB_ISNULL(index_block_rebuilder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index block rebuilder is null", K(ret), K(table_key));
  } else {
    rebuilder = index_block_rebuilder_;
  }
  return ret;
}

int ObBackupTaskIndexRebuilderMgr::close_index_block_rebuilder_if_need(const ObBackupProviderItem &item, bool &is_closed)
{
  int ret = OB_SUCCESS;
  is_closed = false;
  bool is_last = false;
  const common::ObTabletID &tablet_id = item.get_tablet_id();
  const ObITable::TableKey &table_key = item.get_table_key();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task index rebuilder mgr do not init", K(ret));
  } else if (OB_FAIL(check_is_last_item_for_table_key_(item, is_last))) {
    LOG_WARN("failed to check is last item for table key", K(ret), K(item));
  } else if (!is_last) {
    // do nothing
  } else if (cur_table_key_ != table_key) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur table key not equal to table key", K(ret), K(table_key), K_(cur_table_key));
  } else if (OB_ISNULL(index_block_rebuilder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index block rebuilder should not be null", K(ret));
  } else if (OB_FAIL(index_block_rebuilder_->close())) {
    LOG_WARN("failed to close rebuilder", K(ret));
  } else {
    LOG_INFO("succeed to close index block rebuilder", K(tablet_id), K(table_key));
    is_closed = true;
  }
  if (OB_FAIL(ret) || is_last) {
    if (OB_NOT_NULL(index_block_rebuilder_)) {
      index_block_rebuilder_->~ObIndexBlockRebuilder();
      mtl_free(index_block_rebuilder_);
      index_block_rebuilder_ = NULL;
    }
  }
  return ret;
}

int ObBackupTaskIndexRebuilderMgr::check_is_last_item_for_table_key(const ObBackupProviderItem &item, bool &is_last)
{
  return check_is_last_item_for_table_key_(item, is_last);
}

int ObBackupTaskIndexRebuilderMgr::prepare_index_block_rebuilder_(
    const ObBackupProviderItem &item,
    const int64_t *task_idx,
    common::ObIArray<ObIODevice *> &device_handle_array,
    ObIndexBlockRebuilder *&rebuilder)
{
  int ret = OB_SUCCESS;
  ObSSTableIndexBuilder *sstable_index_builder = NULL;
  const common::ObTabletID &tablet_id = item.get_tablet_id();
  const storage::ObITable::TableKey &table_key = item.get_table_key();
  void *buf = NULL;
  if (!item.is_valid() || 2 != device_handle_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(item), K(device_handle_array));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObIndexBlockRebuilder), ObModIds::BACKUP))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(rebuilder = new (buf) ObIndexBlockRebuilder)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(get_sstable_index_builder_ptr_(tablet_id, table_key, sstable_index_builder))) {
    LOG_WARN("failed to get sstable index builder ptr", K(ret), K(tablet_id), K(table_key));
  } else if (OB_ISNULL(sstable_index_builder)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index builder should not be null", K(ret));
  } else if (OB_FAIL(rebuilder->init(*sstable_index_builder,
                                     task_idx,
                                     table_key.is_ddl_merge_sstable()/*is_ddl_merge*/,
                                     &device_handle_array))) {
    LOG_WARN("failed to init index block rebuilder", K(ret), KPC(sstable_index_builder));
  } else {
    LOG_INFO("succeed to prepare index block rebuilder", K(tablet_id), K(table_key));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rebuilder)) {
      rebuilder->~ObIndexBlockRebuilder();
      mtl_free(rebuilder);
      rebuilder = NULL;
    }
  }
  return ret;
}

int ObBackupTaskIndexRebuilderMgr::get_sstable_index_builder_ptr_(const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key, ObSSTableIndexBuilder *&sstable_index_builder)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_builder_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index builder mgr should not be null", K(ret));
  } else if (OB_FAIL(index_builder_mgr_->get_sstable_index_builder(
        tablet_id, table_key, sstable_index_builder))) {
    LOG_WARN("failed to get sstable index builder", K(ret), K(tablet_id), K(table_key));
  }
  return ret;
}

int ObBackupTaskIndexRebuilderMgr::check_is_first_item_for_table_key_(
    const ObBackupProviderItem &item, bool &is_first)
{
  int ret = OB_SUCCESS;
  is_first = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.count(); i++) {
    const ObBackupProviderItem &tmp_item = item_list_.at(i);
    if (tmp_item == item) {
      is_first = true;
      break;
    } else if (tmp_item.get_table_key() == item.get_table_key()) {
      is_first = false;
      break;
    } else {
      continue;
    }
  }
  return ret;
}

int ObBackupTaskIndexRebuilderMgr::check_is_last_item_for_table_key_(
    const ObBackupProviderItem &item, bool &is_last)
{
  int ret = OB_SUCCESS;
  is_last = false;
  for (int64_t i = item_list_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    const ObBackupProviderItem &tmp_item = item_list_.at(i);
    if (tmp_item == item) {
      is_last = true;
      break;
    } else if (tmp_item.get_table_key() == item.get_table_key()) {
      is_last = false;
      break;
    } else {
      continue;
    }
  }
  return ret;
}

// ObBackupTabletIndexBlockBuilderMgr

ObBackupTabletIndexBlockBuilderMgr::ObBackupTabletIndexBlockBuilderMgr()
  : is_inited_(false),
    mutex_(),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    sstable_builder_map_()
{
}

ObBackupTabletIndexBlockBuilderMgr::~ObBackupTabletIndexBlockBuilderMgr()
{
  reset();
}

int ObBackupTabletIndexBlockBuilderMgr::init(
    const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup index block builder mgr is init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(sstable_builder_map_.create(BUCKET_NUM, lib::ObMemAttr(tenant_id, ObModIds::BACKUP)))) {
    LOG_WARN("failed to create sstable builder map", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTabletIndexBlockBuilderMgr::reset()
{
  reuse();
}

void ObBackupTabletIndexBlockBuilderMgr::reuse()
{
  ObMutexGuard guard(mutex_);
  if (!sstable_builder_map_.created()) {
  } else {
    FOREACH(iter, sstable_builder_map_) {
      ObBackupTabletSSTableIndexBuilderMgr *mgr = iter->second;
      mgr->~ObBackupTabletSSTableIndexBuilderMgr();
      mtl_free(mgr);
      mgr = NULL;
    }
    sstable_builder_map_.reuse();
  }
}

int ObBackupTabletIndexBlockBuilderMgr::prepare_sstable_index_builders(
    const common::ObTabletID &tablet_id, const common::ObIArray<storage::ObITable::TableKey> &table_keys,
    const bool is_major_compaction_mview_dep_tablet)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup index block builder mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else {
    ObMutexGuard guard(mutex_);
    ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;
    int32_t hash_ret = sstable_builder_map_.get_refactored(tablet_id, mgr);
    if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : hash_ret;
      LOG_WARN("tablet mgr already init", K(ret));
    } else {
      void *buf = NULL;
      mgr = NULL;

      if (OB_ISNULL(buf = mtl_malloc(sizeof(ObBackupTabletSSTableIndexBuilderMgr), ObModIds::BACKUP))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KP(buf));
      } else if (FALSE_IT(mgr = new (buf) ObBackupTabletSSTableIndexBuilderMgr)) {
      } else if (OB_FAIL(mgr->init(tenant_id_, tablet_id, table_keys, is_major_compaction_mview_dep_tablet))) {
        LOG_WARN("failed to init backup tablet sstable index builder mgr", K(ret), K(tablet_id), K(table_keys));
      } else if (OB_FAIL(sstable_builder_map_.set_refactored(tablet_id, mgr))) {
        LOG_WARN("failed to set tablet sstable index builder mgr into map", K(ret), K(tablet_id));
      } else {
        LOG_INFO("[INDEX_BUILDER_MGR] prepare sstable index builders", K(tablet_id), K(table_keys));
        mgr = NULL;
      }

      if (OB_NOT_NULL(mgr)) {
        mgr->~ObBackupTabletSSTableIndexBuilderMgr();
        mtl_free(mgr);
        mgr = NULL;
      }
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::open_sstable_index_builder(
    const common::ObTabletID &tablet_id, const ObTabletHandle &tablet_handle,
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup index block builder mgr do not init", K(ret), K(tablet_id));
  } else if (!tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id), K(table_key));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, mgr))) {
      LOG_WARN("failed to get sstable index builder", K(ret), K(tablet_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup tablet sstable index builder mgr should not be null", K(ret));
    } else if (OB_FAIL(mgr->add_sstable_index_builder(ls_id_, tablet_handle, table_key, sstable))) {
      LOG_WARN("failed to add sstable index builder", K(ret), K(tablet_id), K(table_key), KPC(sstable));
    } else {
      LOG_INFO("[INDEX_BUILDER_MGR] open sstable index builder", K(tablet_id), K(table_key));
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::get_sstable_index_builder_mgr(
    const common::ObTabletID &tablet_id, ObBackupTabletSSTableIndexBuilderMgr *&builder_mgr)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup index block builder mgr do not init", K(ret), K(tablet_id));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, mgr))) {
      LOG_WARN("failed to get sstable index builder", K(ret), K(tablet_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup tablet sstable index builder mgr should not be null", K(ret));
    } else {
      builder_mgr = mgr;
      LOG_INFO("[INDEX_BUILDER_MGR] get sstable index builder", K(tablet_id), KP(mgr));
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::get_sstable_index_builder(const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableIndexBuilder *&sstable_index_builder)
{
  int ret = OB_SUCCESS;
  sstable_index_builder = NULL;
  ObBackupTabletSSTableIndexBuilderMgr *builder_mgr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id), K(table_key));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, builder_mgr))) {
      LOG_WARN("failed to get tablet table key mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(builder_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table key mgr should not be NULL", K(ret));
    } else if (OB_FAIL(builder_mgr->get_sstable_index_builder(table_key, sstable_index_builder))) {
      LOG_WARN("failed to get copy table key info", K(ret), K(tablet_id), K(table_key));
    } else {
      LOG_INFO("[INDEX_BUILDER_MGR] get sstable index builder", K(tablet_id), K(table_key));
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::check_sstable_index_builder_mgr_exist(
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObBackupTabletSSTableIndexBuilderMgr *builder_mgr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id), K(table_key));
  } else {
    ObMutexGuard guard(mutex_);
    blocksstable::ObSSTableIndexBuilder *index_builder = NULL;
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, builder_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        exist = false;
      } else {
        LOG_WARN("failed to get tablet table key mgr", K(ret), K(tablet_id));
      }
    } else if (OB_FAIL(builder_mgr->get_sstable_index_builder(table_key, index_builder))) {
      LOG_WARN("failed to get sstable index builder", K(ret), K(table_key));
    } else if (OB_ISNULL(index_builder)) {
      exist = false;
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::get_sstable_merge_result(const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableMergeRes *&merge_res)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableIndexBuilder *sstable_index_builder = NULL;
  ObBackupTabletSSTableIndexBuilderMgr *builder_mgr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id), K(table_key));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, builder_mgr))) {
      LOG_WARN("failed to get tablet table key mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(builder_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table key mgr should not be NULL", K(ret));
    } else if (OB_FAIL(builder_mgr->get_sstable_merge_result(table_key, merge_res))) {
      LOG_WARN("failed to get copy table key info", K(ret), K(tablet_id), K(table_key));
    } else {
      LOG_INFO("[INDEX_BUILDER_MGR] get sstable merge result", K(tablet_id), K(table_key), KPC(merge_res));
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::close_sstable_index_builder(
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key, ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *builder_mgr = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, builder_mgr))) {
      LOG_WARN("failed to get tablet table key mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(builder_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table key mgr should not be NULL", K(ret));
    } else if (OB_FAIL(builder_mgr->close_sstable_index_builder(table_key, device_handle))) {
      LOG_WARN("failed to get sstable index builders", K(ret), K(tablet_id), K(table_key));
    } else {
      LOG_INFO("[INDEX_BUILDER_MGR] close sstable index builder", K(tablet_id), K(table_key));
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::free_sstable_index_builder(
    const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *builder_mgr = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha table info mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(sstable_builder_map_.get_refactored(tablet_id, builder_mgr))) {
      LOG_WARN("failed to get tablet table key mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(builder_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table key mgr should not be NULL", K(ret));
    } else if (OB_FAIL(builder_mgr->free_sstable_index_builder(table_key))) {
      LOG_WARN("failed to get sstable index builders", K(ret), K(tablet_id), K(table_key));
    } else {
      LOG_INFO("[INDEX_BUILDER_MGR] close sstable index builder", K(tablet_id), K(table_key));
    }
  }
  return ret;
}

int ObBackupTabletIndexBlockBuilderMgr::remove_sstable_index_builder(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBackupTabletSSTableIndexBuilderMgr *mgr = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup index block builder mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tablet_id));
  } else {
    ObMutexGuard guard(mutex_);
    FLOG_INFO("[INDEX_BUILDER_MGR] remove sstable index builder", K(tablet_id));
    if (OB_FAIL(sstable_builder_map_.erase_refactored(tablet_id, &mgr))) {
      LOG_WARN("failed to erase backup tablet sstable index builder mgr", K(ret), K(tablet_id));
    } else if (OB_ISNULL(mgr)) {
      //do nothing
    } else {
      mgr->~ObBackupTabletSSTableIndexBuilderMgr();
      mtl_free(mgr);
      mgr = nullptr;
    }
  }
  return ret;
}

// ObBackupTabletSSTableIndexBuilderMgr

ObBackupTabletSSTableIndexBuilderMgr::ObBackupTabletSSTableIndexBuilderMgr()
  : is_inited_(false),
    mutex_(),
    tablet_id_(),
    table_keys_(),
    builders_(),
    merge_results_(),
    local_reuse_map_(),
    is_major_compaction_mview_dep_tablet_(false)
{
}

ObBackupTabletSSTableIndexBuilderMgr::~ObBackupTabletSSTableIndexBuilderMgr()
{
  reset();
}

int ObBackupTabletSSTableIndexBuilderMgr::init(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const common::ObIArray<storage::ObITable::TableKey> &table_key_array, const bool is_major_compaction_mview_dep_tablet)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(tenant_id, ObModIds::BACKUP);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet sstable index builder mgr init twice", K(ret), K(tablet_id));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet id is not valid", K(ret), K(tablet_id));
  } else if (FALSE_IT(table_keys_.set_attr(mem_attr))) {
  } else if (OB_FAIL(table_keys_.assign(table_key_array))) {
    LOG_WARN("failed to assign table keys", K(ret));
  } else {
    builders_.set_attr(mem_attr);
    merge_results_.set_attr(mem_attr);
    if (OB_FAIL(builders_.prepare_allocate(table_key_array.count()))) {
      LOG_WARN("failed to reserve table keys", K(ret));
    } else if (OB_FAIL(merge_results_.prepare_allocate(table_key_array.count()))) {
      LOG_WARN("failed to reserve merge res", K(ret));
    } else if (is_major_compaction_mview_dep_tablet && OB_FAIL(local_reuse_map_.create(BUCKET_NUM, mem_attr))) {
      LOG_WARN("failed to create local reuse map", K(ret));
    } else {
      tablet_id_ = tablet_id;
      is_major_compaction_mview_dep_tablet_ = is_major_compaction_mview_dep_tablet;
      LOG_INFO("init backup tablet sstable index builder mgr", K(tablet_id), K(is_major_compaction_mview_dep_tablet));
      is_inited_ = true;
    }
  }
  return ret;
}

void ObBackupTabletSSTableIndexBuilderMgr::reset()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ARRAY_FOREACH(builders_, idx) {
    ObSSTableIndexBuilder *index_builder = builders_.at(idx);
    if (OB_NOT_NULL(index_builder)) {
      index_builder->~ObSSTableIndexBuilder();
      mtl_free(index_builder);
      builders_.at(idx) = NULL;
    }
  }
  builders_.reset();
}

int ObBackupTabletSSTableIndexBuilderMgr::add_sstable_index_builder(
    const share::ObLSID &ls_id, const ObTabletHandle &tablet_handle,
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  int64_t idx = -1;
  ObWholeDataStoreDesc data_store_desc;
  ObSSTableIndexBuilder *index_builder = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(table_key));
  } else if (OB_FAIL(get_table_key_idx_(table_key, idx))) {
    LOG_WARN("failed to get table key idx", K(table_key));
  } else if (OB_FAIL(prepare_data_store_desc_(ls_id, tablet_handle, table_key, sstable, data_store_desc))) {
    LOG_WARN("failed to prepare data store desc", K(ret), K(ls_id), K(tablet_handle), K(table_key));
  } else if (OB_FAIL(alloc_sstable_index_builder_(table_key, data_store_desc.get_desc(), index_builder))) {
    LOG_WARN("failed to alloc sstable index builder", K(ret), K(table_key));
  } else {
    builders_.at(idx) = index_builder;
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::get_sstable_index_builder(
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableIndexBuilder *&index_builder)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  index_builder = NULL;
  int64_t idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(table_key));
  } else if (OB_FAIL(get_table_key_idx_(table_key, idx))) {
    LOG_WARN("failed to get table key idx", K(table_key));
  } else {
    index_builder = builders_.at(idx);
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::get_sstable_merge_result(
    const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableMergeRes *&merge_res)
{
  int ret = OB_SUCCESS;
  merge_res = NULL;
  ObMutexGuard guard(mutex_);
  int64_t idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(table_key));
  } else if (OB_FAIL(get_table_key_idx_(table_key, idx))) {
    LOG_WARN("failed to get table key idx", K(table_key));
  } else {
    merge_res = &merge_results_.at(idx);
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::get_sstable_index_builders(
    common::ObIArray<blocksstable::ObSSTableIndexBuilder *> &builders)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (OB_FAIL(builders.assign(builders_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::close_sstable_index_builder(
    const storage::ObITable::TableKey &table_key, ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  blocksstable::ObSSTableMergeRes sstable_merge_res;
  ObMutexGuard guard(mutex_);
  ObSSTableIndexBuilder *index_builder = NULL;
  if (OB_FAIL(get_table_key_idx_(table_key, idx))) {
    LOG_WARN("failed to get table key idx", K(ret), K(table_key));
  } else if (OB_ISNULL(index_builder = builders_.at(idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index builder should not be null", K(ret));
  } else if (OB_FAIL(close_sstable_index_builder_(index_builder, device_handle, sstable_merge_res))) {
    LOG_WARN("failed to close sstable index builder", K(ret), KP(index_builder), KP(device_handle));
  } else if (OB_FAIL(merge_results_.at(idx).assign(sstable_merge_res))) {
    LOG_WARN("failed to assign res", K(ret), K(sstable_merge_res));
  } else {
    LOG_INFO("close sstable index builders", K_(tablet_id), K(table_key), K(idx), K(sstable_merge_res));
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::free_sstable_index_builder(const storage::ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  ObMutexGuard guard(mutex_);
  ObSSTableIndexBuilder *index_builder = NULL;
  if (OB_FAIL(get_table_key_idx_(table_key, idx))) {
    LOG_WARN("failed to get table key idx", K(ret), K(table_key));
  } else if (OB_ISNULL(index_builder = builders_.at(idx))) {
    // do nothing
  } else {
    index_builder->~ObSSTableIndexBuilder();
    mtl_free(index_builder);
    builders_.at(idx) = NULL;
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::get_merge_type_(
    const storage::ObITable::TableKey &table_key, compaction::ObMergeType &merge_type)
{
  int ret = OB_SUCCESS;
  merge_type = compaction::ObMergeType::INVALID_MERGE_TYPE;

  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table key is not valid", K(ret), K(table_key));
  } else if (table_key.is_major_sstable()) {
    merge_type = compaction::ObMergeType::MAJOR_MERGE;
  } else if (table_key.is_minor_sstable()) {
    merge_type = compaction::ObMergeType::MINOR_MERGE;
  } else if (table_key.is_ddl_dump_sstable()) {
    merge_type = compaction::ObMergeType::MAJOR_MERGE;
  } else if (table_key.is_mds_mini_sstable()) {
    merge_type = compaction::ObMergeType::MDS_MINI_MERGE;
  } else if (table_key.is_mds_minor_sstable()) {
    merge_type = compaction::ObMergeType::MDS_MINOR_MERGE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key is not expected", K(ret), K(table_key));
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::prepare_data_store_desc_(const share::ObLSID &ls_id,
    const ObTabletHandle &tablet_handle, const storage::ObITable::TableKey &table_key,
    blocksstable::ObSSTable *sstable, blocksstable::ObWholeDataStoreDesc &data_store_desc)
{
  int ret = OB_SUCCESS;
  data_store_desc.reset();
  compaction::ObMergeType merge_type;
  ObTablet *tablet = NULL;

  if (!ls_id.is_valid() || !tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id), K(tablet_handle));
  } else if (OB_FAIL(get_merge_type_(table_key, merge_type))) {
    LOG_WARN("failed to get merge type", K(ret), K(table_key));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else {
    if (is_mds_merge(merge_type)) {
      const ObStorageSchema *storage_schema = ObMdsSchemaHelper::get_instance().get_storage_schema();
      if (OB_ISNULL(storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage schema is null", K(ret), KP(storage_schema));
      } else if (OB_UNLIKELY(!storage_schema->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mds storage schema is invalid", K(ret), KP(storage_schema), KPC(storage_schema));
      } else if (OB_FAIL(data_store_desc.init(false/* is ddl */,
                                              *storage_schema,
                                              ls_id,
                                              tablet->get_tablet_id(),
                                              merge_type,
                                              tablet->get_snapshot_version(),
                                              0/*cluster_version*/,
                                              false/*micro_index_clustered*/,
                                              tablet->get_transfer_seq(),
                                              table_key.get_end_scn()))) {
        LOG_WARN("failed to init static desc", K(ret), KPC(storage_schema));
      }
    } else {
      ObArenaAllocator allocator;
      ObStorageSchema *storage_schema = NULL;
      ObSSTableMetaHandle sstable_meta_handle;
      if (OB_FAIL(tablet->load_storage_schema(allocator, storage_schema))) {
        LOG_WARN("failed to load storage schema failed", K(ret));
      } else if (OB_FAIL(sstable->get_meta(sstable_meta_handle))) {
        LOG_WARN("failed to get meta", K(ret), KPC(sstable));
      } else {
        const uint16_t cg_idx = table_key.get_column_group_id();
        const ObStorageColumnGroupSchema *cg_schema = NULL;
        if (table_key.is_cg_sstable()) {
          if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= storage_schema->get_column_group_count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected cg idx", K(ret), K(cg_idx), KPC(storage_schema));
          } else {
            cg_schema = &storage_schema->get_column_groups().at(cg_idx);
          }
        }
        if (FAILEDx(data_store_desc.init(false/* is ddl */,
                                        *storage_schema,
                                        ls_id,
                                        tablet->get_tablet_id(),
                                        merge_type,
                                        tablet->get_snapshot_version(),
                                        0/*cluster_version*/,
                                        false/*micro_index_clustered*/,
                                        tablet->get_transfer_seq(),
                                        table_key.get_end_scn(),
                                        cg_schema,
                                        cg_idx))) {
          LOG_WARN("failed to init index store desc for column store table", K(ret), K(cg_idx), K(cg_schema));
        } else {
          int64_t column_cnt = sstable_meta_handle.get_sstable_meta().get_basic_meta().column_cnt_;
          if (OB_FAIL(data_store_desc.get_col_desc().mock_valid_col_default_checksum_array(column_cnt))) {
            LOG_WARN("fail to mock valid col default checksum array", K(ret));
          }
        }
      }
      ObTabletObjLoadHelper::free(allocator, storage_schema);
    }
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::get_table_key_idx_(
    const storage::ObITable::TableKey &table_key, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  ARRAY_FOREACH_X(table_keys_, i, cnt, OB_SUCC(ret)) {
    const storage::ObITable::TableKey &tmp = table_keys_.at(i);
    if (table_key == tmp) {
      idx = i;
      break;
    }
  }
  if (-1 == idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("entry not exist", K(ret), K_(table_keys));
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::alloc_sstable_index_builder_(
    const storage::ObITable::TableKey &table_key,
    const ObDataStoreDesc &data_store_desc,
    ObSSTableIndexBuilder *&index_builder)
{
  int ret = OB_SUCCESS;
  index_builder = NULL;
  void *buf = NULL;
  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table key is not valid", K(ret));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(blocksstable::ObSSTableIndexBuilder), ObModIds::BACKUP))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_ISNULL(index_builder = new (buf) blocksstable::ObSSTableIndexBuilder(false/* not use writer buffer*/))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(index_builder->init(data_store_desc, ObSSTableIndexBuilder::DISABLE))) {
    LOG_WARN("failed to init index builder", K(ret), K(data_store_desc));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(index_builder)) {
    index_builder->~ObSSTableIndexBuilder();
    mtl_free(index_builder);
    index_builder = NULL;
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::close_sstable_index_builder_(
    ObSSTableIndexBuilder *index_builder, ObIODevice *device_handle,
    ObSSTableMergeRes &sstable_merge_res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_builder) || OB_ISNULL(device_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key is not expected", K(ret), KP(index_builder), KP(device_handle));
  } else if (OB_FAIL(index_builder->close(sstable_merge_res,
                                          OB_DEFAULT_MACRO_BLOCK_SIZE/*nested_size*/,
                                          0/*nested_offset*/,
                                          nullptr,
                                          device_handle))) {
    LOG_WARN("failed to close sstable index builder", K(ret));
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::insert_place_holder_macro_index(
    const blocksstable::ObLogicMacroBlockId &logic_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObBackupMacroBlockIndex tmp_index;
  int32_t hash_ret = local_reuse_map_.get_refactored(logic_id, tmp_index);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!is_major_compaction_mview_dep_tablet_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not major compaction mview dep tablet, should not call this", K(ret));
  } else if (OB_HASH_NOT_EXIST != hash_ret) {
    LOG_WARN("macro index already exist, do nothing", K(ret), K(logic_id));
  } else {
    ObBackupMacroBlockIndex macro_index;
    macro_index.reset();
    if (OB_FAIL(local_reuse_map_.set_refactored(logic_id, macro_index))) {
      LOG_WARN("failed to set macro index", K(ret), K(logic_id), K(macro_index));
    } else {
      LOG_INFO("insert place holder macro index", K(logic_id));
    }
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::update_logic_id_to_macro_index(
    const blocksstable::ObLogicMacroBlockId &logic_id, const ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObBackupMacroBlockIndex tmp_index;
  int32_t hash_ret = local_reuse_map_.get_refactored(logic_id, tmp_index);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!is_major_compaction_mview_dep_tablet_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not major compaction mview dep tablet, should not call this", K(ret));
  } else if (!logic_id.is_valid() || !macro_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id), K(macro_index));
  } else {
    if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("previous logic id do not exist", K(ret), K(hash_ret), K(logic_id));
    } else if (OB_FAIL(local_reuse_map_.set_refactored(logic_id, macro_index, 1))) {
      LOG_WARN("failed to set macro index", K(ret));
    } else {
      LOG_INFO("update logic id to macro index", K(logic_id), K(macro_index));
    }
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::check_place_holder_macro_index_exist(
    const blocksstable::ObLogicMacroBlockId &logic_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObMutexGuard guard(mutex_);
  ObBackupMacroBlockIndex tmp_index;
  int32_t hash_ret = local_reuse_map_.get_refactored(logic_id, tmp_index);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!is_major_compaction_mview_dep_tablet_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not major compaction mview dep tablet, should not call this", K(ret));
  } else if (OB_HASH_NOT_EXIST == hash_ret) {
    exist = false;
  } else if (!tmp_index.is_valid()) {
    exist = true;
  }
  return ret;
}

int ObBackupTabletSSTableIndexBuilderMgr::check_real_macro_index_exist(
    const blocksstable::ObLogicMacroBlockId &logic_id, bool &exist, ObBackupMacroBlockIndex &index)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObMutexGuard guard(mutex_);
  ObBackupMacroBlockIndex tmp_index;
  int32_t hash_ret = local_reuse_map_.get_refactored(logic_id, tmp_index);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("builder mgr do not init", K(ret));
  } else if (!is_major_compaction_mview_dep_tablet_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not major compaction mview dep tablet, should not call this", K(ret));
  } else if (OB_HASH_NOT_EXIST == hash_ret) {
    exist = false;
  } else if (!tmp_index.is_valid()) {
    exist = false;
  } else {
    exist = true;
    index = tmp_index;
  }
  LOG_INFO("check macro index exist", K(logic_id), K(exist), K(index));
  return ret;
}

}
}
