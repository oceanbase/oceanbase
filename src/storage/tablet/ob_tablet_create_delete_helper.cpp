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

#include "storage/tablet/ob_tablet_create_delete_helper.h"

#include "lib/ob_abort.h"
#include "lib/worker.h"
#include "lib/utility/utility.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_id_set.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"
#include "observer/omt/ob_tenant_config_mgr.h"

#define USING_LOG_PREFIX STORAGE

#define PRINT_CREATE_ARG(arg) (ObSimpleBatchCreateTabletArg(arg))

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::palf;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{
int ObTabletCreateDeleteHelper::get_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_TIME_US = 10;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const int64_t begin_time = ObClockGenerator::getClock();
  int64_t current_time = 0;

  while (OB_SUCC(ret)) {
    ret = t3m->get_tablet(WashTabletPriority::WTP_HIGH, key, handle);
    if (OB_SUCC(ret)) {
      break;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_DEBUG("tablet does not exist", K(ret), K(key));
    } else if (OB_ITEM_NOT_SETTED == ret) {
      current_time = ObClockGenerator::getClock();
      if (current_time - begin_time > timeout_us) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("continuously meet item not set error", K(ret), K(key),
            K(begin_time), K(current_time), K(timeout_us));
      } else {
        ret = OB_SUCCESS;
        ob_usleep(SLEEP_TIME_US);
      }
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key));
    }
  }
  return ret;
}

int ObTabletCreateDeleteHelper::check_and_get_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const int64_t timeout_us,
    const ObMDSGetTabletMode mode,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;

  if (OB_FAIL(get_tablet(key, handle, timeout_us))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_DEBUG("tablet does not exist", K(ret), K(key), K(mode));
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key), K(mode));
    }
  } else if (OB_ISNULL(tablet = handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(handle));
  } else if (tablet->is_ls_inner_tablet()) {
    // no need to check ls inner tablet, do nothing
  } else if (ObMDSGetTabletMode::READ_WITHOUT_CHECK == mode) {
    // no checking
  } else if (ObMDSGetTabletMode::READ_ALL_COMMITED == mode) {
    if (OB_UNLIKELY(snapshot_version != ObTransVersion::MAX_TRANS_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("read all committed mode should only pass max scn", K(ret), K(key), K(mode), K(snapshot_version));
    } else if (OB_FAIL(tablet->check_tablet_status_for_read_all_committed())) {
      LOG_WARN("failed to check tablet status", K(ret), K(key));
    }
  } else if (ObMDSGetTabletMode::READ_READABLE_COMMITED == mode) {
    if (OB_FAIL(tablet->check_new_mds_with_cache(snapshot_version, timeout_us))) {
      LOG_WARN("failed to check status for new mds", K(ret), K(mode), K(snapshot_version), K(timeout_us));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret), K(key), K(mode));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::check_status_for_new_mds(
    ObTablet &tablet,
    const int64_t snapshot_version,
    const int64_t timeout_us,
    ObTabletStatusCache &tablet_status_cache)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  share::SCN snapshot;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTabletStatus tablet_status(ObTabletStatus::MAX);
  bool is_committed = false;

  auto func = [&user_data](const ObTabletCreateDeleteMdsUserData &data) -> int {
    int ret = OB_SUCCESS;
    if (OB_FAIL(user_data.assign(data))) {
      LOG_WARN("failed to copy", K(ret));
    }
    return ret;
  };

  // get latest, then check transfer scn
  // if read snapshot > transfer scn:
  // if tx is not committed, we will return OB_SCHEMA_EAGAIN and let upper layer retry
  // if tx is committed, tablet is deleted for sure and we will return OB_TABLET_NOT_EXIST
  if (OB_FAIL(tablet.get_latest<ObTabletCreateDeleteMdsUserData>(func, is_committed, 0/*read_seq*/))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet creation has not been committed, or has been roll backed", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get snapshot", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (FALSE_IT(tablet_status = user_data.tablet_status_)) {
  } else if (OB_FAIL(snapshot.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert snapshot", K(ret), K(snapshot_version));
  } else if (ObTabletStatus::TRANSFER_OUT == tablet_status
      && OB_FAIL(check_read_snapshot_by_transfer_scn(user_data.transfer_scn_, is_committed, snapshot))) {
    LOG_WARN("failed to check read snapshot by transfer scn", K(ret), K(ls_id), K(tablet_id), K(user_data), K(is_committed), K(snapshot));
  } else if (OB_FAIL(tablet.get_snapshot<ObTabletCreateDeleteMdsUserData>(
      func,
      snapshot,
      0/*read_seq*/,
      timeout_us))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet creation has not been committed, or has been roll backed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_ERR_SHARED_LOCK_CONFLICT == ret) {
      LOG_WARN("tablet transaction is in commit progress", K(ret), K(ls_id), K(tablet_id), K(snapshot), K(timeout_us));
    } else {
      LOG_WARN("failed to get snapshot", KR(ret), K(ls_id), K(tablet_id), K(snapshot), K(timeout_us));
    }
  } else if (OB_FAIL(check_read_snapshot_by_commit_version(tablet,
      user_data.create_commit_version_,
      user_data.delete_commit_version_,
      snapshot_version,
      user_data.tablet_status_))) {
    LOG_WARN("failed to check read snapshot by commit version", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
  }

  if (OB_SUCC(ret) && ObTabletStatus::TRANSFER_OUT != tablet_status && user_data.is_valid()) {
    tablet_status_cache.set_value(user_data);
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_by_transfer_scn(
    const share::SCN &transfer_scn,
    const bool is_committed,
    const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;

  if (!is_committed) {
    if (snapshot > transfer_scn) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("read snapshot is bigger than transfer scn, should retry", K(ret), K(snapshot), K(transfer_scn));
    }
  } else if (is_committed) {
    if (snapshot > transfer_scn) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("read snapshot is bigger than transfer scn after transfer out committed, should retry on target ls",
          K(ret), K(snapshot), K(transfer_scn));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_by_commit_version(
    ObTablet &tablet,
    const int64_t create_commit_version,
    const int64_t delete_commit_version,
    const int64_t snapshot_version,
    const ObTabletStatus &tablet_status)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  ObTabletStatus infered_tablet_status = tablet_status;
  if (snapshot_version == ObTransVersion::MAX_TRANS_VERSION) {
    // do nothing
  } else if (OB_UNLIKELY(create_commit_version == ObTransVersion::INVALID_TRANS_VERSION)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create tablet trans version is invalid",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
  } else if (snapshot_version < create_commit_version) {
    // read snapshot is smaller than create tablet trans version,
    // no previous committed transaction
    infered_tablet_status = ObTabletStatus::MAX;
    LOG_INFO("tablet status is set to MAX because read snapshot is smaller than create trans version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
  } else if (delete_commit_version == ObTransVersion::INVALID_TRANS_VERSION) {
    // delete commit version is not valid, no delete transaction committed
  } else if (snapshot_version < delete_commit_version) {
    // read snapshot is smaller than delete tablet trans version,
    // previous transaction is create tablet/transfer in create tablet, so tablet status is NORMAL
    infered_tablet_status = ObTabletStatus::NORMAL;
    LOG_INFO("tablet status is set to NORMAL because read snapshot is smaller than delete trans version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(delete_commit_version));
  } else {
    // snapshot_version >= user_data.delete_commit_version_
    // do nothing
  }

  if (OB_FAIL(ret)) {
  } else if (ObTabletStatus::NORMAL == infered_tablet_status || ObTabletStatus::TRANSFER_IN == infered_tablet_status) {
    if (OB_UNLIKELY(tablet.is_empty_shell())) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet is empty shell", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
    }
  } else if (ObTabletStatus::MAX == infered_tablet_status) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("no valid tablet status can be read", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
  } else {
    ret = OB_TABLET_NOT_EXIST;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::create_tmp_tablet(
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(ls_service->get_ls(key.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("fail to get ls", K(ret), "ls_id", key.ls_id_);
  } else if (OB_FAIL(t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, key, allocator, ls_handle, handle))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::prepare_create_msd_tablet()
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_FAIL(t3m->get_mstx_tablet_creator().throttle_tablet_creation())) {
    LOG_WARN("fail to prepare full tablet", K(ret));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::push_msd_tablet_to_queue(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_FAIL(t3m->get_mstx_tablet_creator().push_tablet_to_queue(handle))) {
    LOG_WARN("fail to push full tablet to queue", K(ret));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::create_msd_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(ls_service->get_ls(key.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("fail to get ls", K(ret), "ls_id", key.ls_id_);
  } else if (OB_FAIL(t3m->create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle))) {
    LOG_WARN("fail to create multi source data tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::acquire_msd_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(t3m->acquire_msd_tablet(WashTabletPriority::WTP_HIGH, key, handle))) {
    LOG_WARN("fail to acquire multi source data tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::acquire_tmp_tablet(
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(t3m->acquire_tmp_tablet(WashTabletPriority::WTP_HIGH, key, allocator, handle))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::acquire_tablet_from_pool(
    const ObTabletPoolType &type,
    const ObTabletMapKey &key,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(t3m->acquire_tablet_from_pool(type, WashTabletPriority::WTP_HIGH, key, handle))) {
    LOG_WARN("fail to acquire tablet from pool", K(ret), K(key), K(type));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::create_sstable_for_migrate(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(sizeof(ObSSTable));
  ObSSTable *sstable = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate sstable memory", K(ret));
  } else if (OB_ISNULL(sstable = new (buf) ObSSTable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new sstable", K(ret));
  } else if (OB_FAIL(create_sstable(param, allocator, *sstable))) {
    LOG_WARN("fail to create sstable", K(ret));
  } else if (OB_FAIL(table_handle.set_sstable(sstable, &allocator))) {
    LOG_WARN("fail to set table handle", K(ret), KPC(sstable));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::create_sstable(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator,
    blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(sstable.init(param, &allocator))) {
    LOG_WARN("fail to init sstable", K(ret), K(param));
  }
  return ret;
}

bool ObTabletCreateDeleteHelper::is_pure_data_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() == 1 && is_contain(tablet_ids, data_tablet_id);
}

bool ObTabletCreateDeleteHelper::is_mixed_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() > 1 && is_contain(tablet_ids, data_tablet_id);
}

bool ObTabletCreateDeleteHelper::is_pure_aux_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && !info.is_create_bind_hidden_tablets_;
}

bool ObTabletCreateDeleteHelper::is_pure_hidden_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && info.is_create_bind_hidden_tablets_;
}

int ObTabletCreateDeleteHelper::check_need_create_empty_major_sstable(
    const ObTableSchema &table_schema,
    bool &need_create_sstable)
{
  int ret = OB_SUCCESS;
  need_create_sstable = false;
  if (OB_UNLIKELY(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_schema));
  } else {
    need_create_sstable = !(table_schema.is_user_hidden_table() || (table_schema.is_index_table() && !table_schema.can_read_index()));
  }
  return ret;
}

void ObTabletCreateDeleteHelper::print_memtables_for_table(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  common::ObSArray<storage::ObITable *> memtables;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_handle is not valid", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_memtables(memtables, true))) {
    LOG_WARN("failed to get_memtables", K(ret), K(tablet_handle));
  } else {
    LOG_INFO("memtables print", K(memtables), K(tablet_handle));
  }
}

int ObTabletCreateDeleteHelper::build_create_sstable_param(
    const ObTableSchema &table_schema,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!table_schema.is_valid()
      || !tablet_id.is_valid()
      || OB_INVALID_VERSION == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_schema), K(snapshot_version));
  } else if (OB_FAIL(table_schema.get_encryption_id(param.encrypt_id_))) {
    LOG_WARN("fail to get_encryption_id", K(ret), K(table_schema));
  } else {
    param.master_key_id_ = table_schema.get_master_key_id();
    MEMCPY(param.encrypt_key_, table_schema.get_encrypt_key_str(), table_schema.get_encrypt_key_len());

    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    param.table_key_.tablet_id_ = tablet_id;
    param.table_key_.version_range_.snapshot_version_ = snapshot_version;
    param.max_merged_trans_version_ = snapshot_version;

    param.schema_version_ = table_schema.get_schema_version();
    param.create_snapshot_version_ = 0;
    param.progressive_merge_round_ = table_schema.get_progressive_merge_round();
    param.progressive_merge_step_ = 0;

    param.table_mode_ = table_schema.get_table_mode_struct();
    param.index_type_ = table_schema.get_index_type();
    param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();
    param.root_row_store_type_ = (ObRowStoreType::ENCODING_ROW_STORE == table_schema.get_row_store_type()
        ? ObRowStoreType::SELECTIVE_ENCODING_ROW_STORE : table_schema.get_row_store_type());
    param.latest_row_store_type_ = table_schema.get_row_store_type();
    param.data_index_tree_height_ = 0;
    param.index_blocks_cnt_ = 0;
    param.data_blocks_cnt_ = 0;
    param.micro_block_cnt_ = 0;
    param.use_old_macro_block_count_ = 0;
    param.data_checksum_ = 0;
    param.occupy_size_ = 0;
    param.ddl_scn_.set_min();
    param.filled_tx_scn_.set_min();
    param.original_size_ = 0;
    param.ddl_scn_.set_min();
    param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    if (OB_FAIL(table_schema.get_store_column_count(param.column_cnt_, true/*is_full*/))) {
      LOG_WARN("fail to get stored col cnt of table schema", K(ret), K(table_schema));
    } else if (FALSE_IT(param.column_cnt_ += multi_version_col_cnt)) {
    } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_,
        param.column_checksums_))) {
      LOG_WARN("fail to fill column checksum for empty major", K(ret), K(param));
    }
  }

  return ret;
}

ObSimpleBatchCreateTabletArg::ObSimpleBatchCreateTabletArg(const ObBatchCreateTabletArg &arg)
  : arg_(arg)
{
}

int64_t ObSimpleBatchCreateTabletArg::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObSimpleBatchCreateTabletArg");
    J_COLON();
    J_KV("id", arg_.id_,
         "major_frozen_scn", arg_.major_frozen_scn_,
         "total_tablet_cnt", arg_.get_tablet_count());
    J_COMMA();

    BUF_PRINTF("tablets");
    J_COLON();
    J_OBJ_START();
    for (int64_t i = 0; i < arg_.tablets_.count(); ++i) {
      const ObCreateTabletInfo &info = arg_.tablets_.at(i);
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      BUF_PRINTF("[%ld] [", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF("] ");
      J_KV("data_tablet_id", info.data_tablet_id_,
           "tablet_ids", info.tablet_ids_,
           "compat_mode", info.compat_mode_,
           "is_create_bind_hidden_tablets", info.is_create_bind_hidden_tablets_);
    }
    J_NEWLINE();
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}
} // namespace storage
} // namespace oceanbase
