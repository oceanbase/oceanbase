/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_tenant_storage_meta_service.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "observer/omt/ob_tenant.h"
#include "storage/ls/ob_ls.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "meta_store/ob_shared_storage_obj_meta.h"
#endif

namespace oceanbase
{
using namespace compaction;
namespace storage
{

ObTenantStorageMetaService::ObTenantStorageMetaService()
  : is_inited_(false),
    is_started_(false),
    is_shared_storage_(false),
    ckpt_slog_handler_(),
    slogger_(),
    shared_object_rwriter_(),
    shared_object_raw_rwriter_(),
    wait_gc_map_lock_(),
    wait_gc_tablet_arr_map_()
{}

int ObTenantStorageMetaService::mtl_init(ObTenantStorageMetaService *&meta_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_service->init())) {
    LOG_WARN("fail to init ObTenantStorageMetaService", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::init()
{
  int ret = OB_SUCCESS;
  const int64_t MAP_BUCKET_CNT = 1024;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else if (OB_FAIL(slogger_.init(SERVER_STORAGE_META_SERVICE.get_slogger_manager(), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("failed to init slogger", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.init(slogger_))) {
    LOG_WARN("fail to init tenant checkpoint slog hander", K(ret));
  } else if (OB_FAIL(shared_object_rwriter_.init())) {
    LOG_WARN("fail to init shared block rwriter", K(ret));
  } else if (OB_FAIL(shared_object_raw_rwriter_.init())) {
    LOG_WARN("fail to init shared block raw rwriter", K(ret));
  } else if (OB_FAIL(wait_gc_tablet_arr_map_.create(MAP_BUCKET_CNT, lib::ObMemAttr(MTL_ID(), "WaitGCTabMap")))) {
    LOG_WARN("fail to create wait gc tablet array map", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantStorageMetaService::start()
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();
  uint64_t macro_block_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(slogger_.start())) {
    LOG_WARN("fail to start slogger", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.start())) {
    LOG_WARN("fail to start tenant checkpoint slog handler", K(ret));
  } else if (OB_FAIL(seq_generator_.init(is_shared_storage_, ckpt_slog_handler_))) {
    LOG_WARN("fail to seq generator", K(ret));
  } else if (OB_FAIL(seq_generator_.start())) {
    LOG_WARN("fail to seq generator", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(seq_generator_.get_private_object_seq(macro_block_id))) {
    LOG_WARN("failed to get_private_object_seq", KR(ret));
  } else if (is_shared_storage_) {
    // for macro check in observer start
    MTL(checkpoint::ObTabletGCService*)->set_mtl_start_max_block_id(macro_block_id);
#endif
  }
  if (OB_SUCC(ret)) {
    is_started_ = true;
  }
  FLOG_INFO("finish start ObTenantStorageMetaService", K(ret));
  return ret;
}

void ObTenantStorageMetaService::stop()
{
  if (IS_INIT) {
    slogger_.stop();
    ckpt_slog_handler_.stop();
    seq_generator_.stop();
  }
}

void ObTenantStorageMetaService::wait()
{
  if (IS_INIT) {
    slogger_.wait();
    ckpt_slog_handler_.wait();
    seq_generator_.stop();
  }
}

void ObTenantStorageMetaService::destroy()
{
  if (IS_INIT) {
    int ret = OB_SUCCESS;
    const lib::ObMemAttr attr(MTL_ID(), "WaitGCTabArr");
    for (WaitGCTabletArrayMap::iterator iter = wait_gc_tablet_arr_map_.begin();
         iter !=  wait_gc_tablet_arr_map_.end();
         iter++) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("WaitGCTabletArrayInfo is null", K(ret), K(iter->first));
      } else {
        OB_DELETE(WaitGCTabletArray, attr, iter->second);
      }
    }
    wait_gc_tablet_arr_map_.destroy();
  }
  slogger_.destroy();
  ckpt_slog_handler_.destroy();
  seq_generator_.destroy();
  shared_object_rwriter_.reset();
  shared_object_raw_rwriter_.reset();
  is_shared_storage_ = false;
  is_started_ = false;
  is_inited_ = false;
}

int ObTenantStorageMetaService::get_meta_block_list(
    ObIArray<blocksstable::MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(ckpt_slog_handler_.get_meta_block_list(meta_block_list))) {
      LOG_WARN("fail to get meta block list", K(ret));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::write_checkpoint(const ObTenantSlogCheckpointWorkflow::Type ckpt_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.write_checkpoint(ckpt_type))) {
    LOG_WARN("fail to write checkpoint", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::prepare_create_ls(const ObLSMeta &meta, int64_t &ls_epoch)
{
  int ret = OB_SUCCESS;
  ls_epoch = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_ && OB_FAIL(ckpt_slog_handler_.create_tenant_ls_item(meta.ls_id_, ls_epoch))) {
    LOG_WARN("fail to create tenant super block ls item", K(ret), K(meta));
  } else {
    HEAP_VAR(ObLSMeta, tmp_meta) {
      tmp_meta = meta;
      tmp_meta.set_ls_epoch(ls_epoch);
      ObCreateLSPrepareSlog slog_entry(tmp_meta);
      ObStorageLogParam log_param;
      log_param.data_ = &slog_entry;
      log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                              ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);
      if (OB_FAIL(slogger_.write_log(log_param))) {
          LOG_WARN("fail to write remove ls slog", K(log_param));
      }
    }
    if (OB_FAIL(ret) && is_shared_storage_) {
      LOG_WARN("fail to write create ls prepare slog", K(ret), K(meta));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ckpt_slog_handler_.update_tenant_ls_item(meta.ls_id_, ls_epoch, ObLSItemStatus::CREATE_ABORT))) {
        LOG_WARN("fail to update tenant super block ls item", K(tmp_ret), K(ret), K(meta.ls_id_), K(ls_epoch));
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::commit_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(ckpt_slog_handler_.update_tenant_ls_item(ls_id, ls_epoch, ObLSItemStatus::CREATED))) {
    LOG_WARN("fail to update tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
#endif
  } else {
    share::ObLSID tmp_ls_id = ls_id;
    ObCreateLSCommitSLog slog_entry(tmp_ls_id);
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_COMMIT);
    if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write create ls commit slog", K(log_param));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::abort_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(ckpt_slog_handler_.update_tenant_ls_item(ls_id, ls_epoch, ObLSItemStatus::CREATE_ABORT))) {
    LOG_WARN("fail to update tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
#endif
  } else {
    share::ObLSID tmp_ls_id = ls_id;
    ObCreateLSAbortSLog slog_entry(tmp_ls_id);
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_ABORT);
    if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write create ls abort slog", K(log_param));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::delete_ls(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(ckpt_slog_handler_.update_tenant_ls_item(ls_id, ls_epoch, ObLSItemStatus::DELETED))) {
    LOG_WARN("fail to update tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
#endif
  } else {
    share::ObLSID tmp_ls_id = ls_id;
    ObDeleteLSLog slog_entry(tmp_ls_id);
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_DELETE_LS);
    if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write remove ls slog", K(log_param));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::delete_ls_item(const share::ObLSID ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.delete_tenant_ls_item(ls_id, ls_epoch))) {
    LOG_WARN("fail to delete tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
  }
  return ret;
}

int ObTenantStorageMetaService::update_ls_meta(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObLSMetaLog slog_entry(ls_meta);
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_UPDATE_LS);
    if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write update ls slog", K(log_param), K(ret));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::update_dup_table_meta(
    const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_table_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObDupTableCkptLog slog_entry;
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_UPDATE_DUP_TABLE_LS);
    if (OB_FAIL(slog_entry.init(dup_table_meta))) {
      LOG_WARN("init slog entry failed", K(ret),K(dup_table_meta));
    } else if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write dup table meta slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::batch_update_tablet(const ObIArray<ObUpdateTabletLog> &slog_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support for shared-storage", K(ret));
  } else {
    ObSArray<ObStorageLogParam> param_arr;
    param_arr.set_attr(ObMemAttr(MTL_ID(), "BatchUpdateTab"));
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(
        ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET);
    if (OB_FAIL(param_arr.reserve(slog_arr.count()))) {
      LOG_WARN("fail to reserve memory for slog param arr", K(ret), K(slog_arr.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < slog_arr.count(); i++) {
      log_param.data_ =(ObIBaseStorageLogEntry*)(&(slog_arr.at(i)));
      if (OB_FAIL(param_arr.push_back(log_param))) {
        LOG_WARN("fail to push back slog param", K(ret), K(log_param));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(slogger_.write_log(param_arr))) {
      LOG_WARN("fail to batch write slog", K(ret), K(param_arr.count()));
    }
  }

  return ret;
}

int ObTenantStorageMetaService::update_tablet(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObTabletID &tablet_id,
    const ObUpdateTabletPointerParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(param));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync_block())) { // make sure that all data or meta written on the macro block is flushed
    LOG_WARN("fail to fsync_block", K(ret));
  } else {
    const ObTabletMapKey tablet_key(ls_id, tablet_id);
    ObUpdateTabletLog slog_entry(ls_id, tablet_id, param, ls_epoch);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_UNLIKELY(!slog_entry.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid slog entry", K(ret), K(slog_entry), K(ls_epoch), K(ls_id), K(tablet_id), K(param));
    } else if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write slog for creating tablet", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::remove_tablet(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletHandle &tablet_handle,
    const int64_t last_gc_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet", K(ret), K(tablet_handle));
  } else {
    const common::ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
    const ObMetaDiskAddr &tablet_addr = tablet_handle.get_obj()->get_tablet_addr();
    if (OB_UNLIKELY(!ls_id.is_valid() || ls_epoch < 0|| !tablet_id.is_valid() || !tablet_addr.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(ls_id), K(ls_epoch), K(tablet_id), K(tablet_addr));
    } else if (!is_shared_storage_ && OB_FAIL(write_remove_tablet_slog_for_sn(ls_id, tablet_id))) {
      LOG_WARN("fail to write remove tablet slog", K(ret), K(ls_id), K(ls_epoch), K(tablet_id));
    } else if (is_shared_storage_ && OB_FAIL(write_remove_tablet_slog_for_ss(ls_id,
                                                                             ls_epoch,
                                                                             tablet_id,
                                                                             *tablet_handle.get_obj(),
                                                                             last_gc_version))) {
      LOG_WARN("fail to write remove tablet slog", K(ret), K(ls_id), K(ls_epoch), K(tablet_id));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::batch_remove_tablet(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const ObIArray<ObMetaDiskAddr> &tablet_addrs,
    const ObIArray<int64_t> &last_gc_versions)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || ls_epoch < 0 || tablet_ids.count() != tablet_addrs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(ls_epoch), K(tablet_ids.count()), K(tablet_addrs.count()));
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode() && last_gc_versions.count() != tablet_ids.count()))  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last_gc_versions", K(ret), K(last_gc_versions.count()), K(tablet_ids.count()));
  } else if (OB_FAIL(batch_write_remove_tablet_slog(ls_id, ls_epoch, tablet_ids, tablet_addrs, last_gc_versions))) {
    LOG_WARN("fail to write remove tablets slog", K(ret), K(ls_id), K(ls_epoch));
  }
  return ret;
}

int ObTenantStorageMetaService::write_empty_shell_tablet(
    const uint64_t data_version,
    const int64_t ls_epoch,
    ObTablet *tablet,
    ObMetaDiskAddr &tablet_addr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret), K(tablet_addr));
  } else if (OB_UNLIKELY(!tablet->is_empty_shell())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("the tablet is not empty shell", K(ret), K(tablet));
  } else {
    const ObTabletMapKey tablet_key(tablet->get_tablet_meta().ls_id_, tablet->get_tablet_meta().tablet_id_);
    ObEmptyShellTabletLog slog_entry(data_version,
                                     tablet->get_tablet_meta().ls_id_,
                                     tablet->get_tablet_meta().tablet_id_,
                                     ls_epoch,
                                     tablet);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_UNLIKELY(!slog_entry.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid slog entry", K(ret), K(slog_entry), K(ls_epoch), KPC(tablet));
    } else if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write slog for empty shell tablet", K(ret), K(log_param));
    } else {
      tablet_addr = log_param.disk_addr_;
    }
  }
  return ret;
}

int ObTenantStorageMetaService::add_snapshot(const ObTenantSnapshotMeta &tenant_snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.add_snapshot(tenant_snapshot))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::delete_snapshot(const share::ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.delete_snapshot(snapshot_id))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::swap_snapshot(const ObTenantSnapshotMeta &tenant_snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.swap_snapshot(tenant_snapshot))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::clone_ls(
    observer::ObStartupAccelTaskHandler* startup_accel_handler,
    const blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.clone_ls(startup_accel_handler, tablet_meta_entry))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::read_from_disk(
    const ObMetaDiskAddr &addr,
    const int64_t ls_epoch,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  char *read_buf = nullptr;
  const int64_t read_buf_len = addr.size();
  if (ObMetaDiskAddr::DiskType::FILE == addr.type()) {
    if (OB_UNLIKELY(is_shared_storage_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared-storage not support DiskType::FILE", K(ret));
    } else if (OB_FAIL(ckpt_slog_handler_.read_empty_shell_file(addr, allocator, buf, buf_len))) {
      LOG_WARN("fail to read empty shell", K(ret), K(addr), K(buf), K(buf_len));
    }
  } else {
    if (OB_FAIL(read_from_share_blk(addr, ls_epoch, allocator, buf, buf_len))) {
      LOG_WARN("fail to read from share block", K(ret), K(addr), K(ls_epoch), K(buf), K(buf_len));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::update_hidden_sys_tenant_super_block_to_real(omt::ObTenant &sys_tenant)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!common::is_sys_tenant(sys_tenant.id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only sys tenant can call this method", K(ret), K(sys_tenant.id()));
  } else if (OB_FAIL(ckpt_slog_handler_.update_hidden_sys_tenant_super_block_to_real(sys_tenant))) {
    LOG_WARN("failed to update hidden sys tenant super block to real", K(ret), K(sys_tenant.id()));
  }
  return ret;
}

int ObTenantStorageMetaService::update_real_sys_tenant_super_block_to_hidden(omt::ObTenant &sys_tenant)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!common::is_sys_tenant(sys_tenant.id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only sys tenant can call this method", K(ret), K(sys_tenant.id()));
  } else if (OB_FAIL(ckpt_slog_handler_.update_real_sys_tenant_super_block_to_hidden(sys_tenant))) {
    LOG_WARN("failed to update hidden sys tenant super block to real", K(ret), K(sys_tenant.id()));
  }
  return ret;
}

int ObTenantStorageMetaService::read_from_share_blk(
    const ObMetaDiskAddr &addr,
    const int64_t ls_epoch,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  ObSharedObjectReadHandle read_handle(allocator);
  ObSharedObjectReadInfo read_info;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
  read_info.addr_ = addr;
  read_info.ls_epoch_ = ls_epoch; /* ls_epoch for share storage */
  if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, read_handle))) {
    LOG_WARN("fail to read tablet from macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("fail to wait for read handle", K(ret));
  } else if (OB_FAIL(read_handle.get_data(allocator, buf, buf_len))) {
    LOG_WARN("fail to get data from read handle", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTenantStorageMetaService::ObLSItemIterator::get_next_ls_item(
      storage::ObLSItem &item)
{
  int ret = OB_SUCCESS;
  if (idx_ == tenant_super_block_.ls_cnt_) {
    ret = OB_ITER_END;
  } else {
    item = tenant_super_block_.ls_item_arr_[idx_++];
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTenantStorageMetaService::inner_get_blocks_for_tablet_(
    const ObMetaDiskAddr &tablet_addr,
    const int64_t ls_epoch,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids/*OUT*/) const
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "GetTabletBLocks"));
  storage::ObTablet tablet;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  // TODO (gaishun.gs): remove debug log tmp arr once get tablet block ids is stable
  const int64_t max_print_id_count = 30;
  ObSEArray<blocksstable::MacroBlockId, max_print_id_count> tmp_print_arr;

  if (OB_FAIL(MTL(ObTenantStorageMetaService*)->read_from_disk(tablet_addr, ls_epoch, allocator, buf, buf_len))) {
    LOG_WARN("fail to read tablet buf from disk", K(ret), K(tablet_addr), K(ls_epoch));
  } else if (FALSE_IT(tablet.set_tablet_addr(tablet_addr))) {
  } else if (OB_FAIL(tablet.deserialize_for_replay(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize tablet", K(ret), KP(buf), K(buf_len));
  } else if (!tablet.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("talbet is invalid", K(ret), K(tablet), K(tablet_addr));
  } else {
    bool in_memory = true;
    ObTabletMacroInfo *macro_info = nullptr;
    storage::ObMacroInfoIterator macro_iter;
    storage::ObTabletBlockInfo block_info;

    if (OB_FAIL(tablet.load_macro_info(ls_epoch, allocator, macro_info, in_memory))) {
      LOG_WARN("fail to load macro info", K(ret));
    } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
      LOG_WARN("fail to init macro iterator", K(ret), KPC(macro_info));
    } else {
      while (OB_SUCC(ret)) {
        block_info.reset();
        if (OB_FAIL(macro_iter.get_next(block_info))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next block info", KR(ret), K(block_info));
          } else {
            ret = OB_SUCCESS;
            if (0 != tmp_print_arr.count()) {
#ifndef OB_BUILD_PACKAGE
              FLOG_INFO("iter get blocks", K(ret), K(ls_epoch), K(tablet_addr), K(is_shared), K(tmp_print_arr));
#endif
              tmp_print_arr.reuse();
            }
            break;
          }
        } else if (!block_info.macro_id_.is_private_data_or_meta() &&
                   !block_info.macro_id_.is_shared_data_or_meta()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected storage_object_type detected", K(ret), K(block_info));
        } else if (!is_shared &&
            block_info.macro_id_.is_private_data_or_meta() &&
            OB_FAIL(block_ids.push_back(block_info.macro_id_))) {
          LOG_WARN("fail to push private macro id", K(ret), K(block_info));
        } else if (is_shared &&
            block_info.macro_id_.is_shared_data_or_meta() &&
            OB_FAIL(block_ids.push_back(block_info.macro_id_))) {
          LOG_WARN("fail to push shared macro id", K(ret), K(block_info));
        } else if (OB_FAIL(tmp_print_arr.push_back(block_info.macro_id_))) {
          LOG_WARN("fail to push shared macro id", K(ret), K(block_info));
        } else if (max_print_id_count == tmp_print_arr.count()) {
#ifndef OB_BUILD_PACKAGE
          FLOG_INFO("iter get blocks", K(ret), K(ls_epoch), K(tablet_addr), K(is_shared), K(tmp_print_arr));
#endif
          tmp_print_arr.reuse();
        }
      }
      if (OB_NOT_NULL(macro_info) && !in_memory) {
        macro_info->reset();
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::get_blocks_from_private_tablet_meta(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletID &tablet_id,
    const int64_t tablet_version,
    const int32_t tablet_private_transfer_epoch,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr tablet_addr;
  blocksstable::MacroBlockId object_id;
  blocksstable::ObStorageObjectOpt opt;
  const int64_t object_size = OB_DEFAULT_MACRO_BLOCK_SIZE;

  opt.set_ss_private_tablet_meta_object_opt(ls_id.id(), tablet_id.id(), tablet_version, tablet_private_transfer_epoch);
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get object id", K(ret), K(opt));
  } else if (OB_FAIL(tablet_addr.set_block_addr(object_id, 0/*offset*/, object_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    LOG_WARN("fail to set initial tablet meta addr", K(ret), K(tablet_addr));
  } else if (OB_FAIL(inner_get_blocks_for_tablet_(tablet_addr, ls_epoch, is_shared, block_ids))) {
    LOG_WARN("fail to deserialize tablet", K(ret), K(tablet_addr), K(ls_epoch), K(is_shared), K(block_ids));
  }
  return ret;
}

int ObTenantStorageMetaService::get_shared_blocks_for_tablet(
    const ObTabletID &tablet_id,
    const int64_t tablet_version,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  // FIXME: re-impl
  return OB_NOT_SUPPORTED;
}

int ObTenantStorageMetaService::inner_get_gc_tablet_scn_arr_(
    const blocksstable::ObStorageObjectOpt &opt,
    ObGCTabletMetaInfoList &gc_tablet_scn_arr) const
{
  int ret = OB_SUCCESS;
  gc_tablet_scn_arr.tablet_version_arr_.reuse();
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "InnerGetGCScn"));
  bool is_exist = false;

  if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, 0/*do not need ls_epoch*/, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(opt));
  } else if (!is_exist) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_TRACE("entry not exist", K(ret), K(opt));
  } else if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
      opt, allocator, MTL_ID(), 0/*do not need ls_epoch*/, gc_tablet_scn_arr))) {
    LOG_WARN("failed to get object: gc_tablet_sc_arr", K(ret), K(opt), K(MTL_ID()), K(gc_tablet_scn_arr));
  }
  return ret;
}
#endif

int ObTenantStorageMetaService::get_wait_gc_tablet_items(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    common::ObIArray<ObPendingFreeTabletItem> &items)
{
  int ret = OB_SUCCESS;
  const WaitGCTabletArrayKey key(ls_id, ls_epoch);
  WaitGCTabletArray *tablet_array = nullptr;
  items.reuse();
  {
    lib::ObMutexGuard guard(wait_gc_map_lock_);
    if (OB_FAIL(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array)) && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get wait gc tablet array from map", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_HASH_NOT_EXIST == ret) {
      tablet_array = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get wait gc tablet array from map", K(ret), K(key));
    }
  } else if (OB_ISNULL(tablet_array)) {
    ret = OB_ERR_UNEXPECTED; // get_refactored successfully, but array_info = nullptr
    LOG_WARN("tablet array is nullptr", K(ret), K(key));
  } else {
    lib::ObMutexGuard guard(tablet_array->lock_);
    if (OB_FAIL(items.assign(tablet_array->wait_gc_tablet_arr_.items_))) {
      LOG_WARN("fail to assign array", K(ret), K(tablet_array->wait_gc_tablet_arr_.items_));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::delete_wait_gc_tablet_items(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObIArray<ObPendingFreeTabletItem> &items)
{
  int ret = OB_SUCCESS;
  const WaitGCTabletArrayKey key(ls_id, ls_epoch);
  // We can split the tablet_ids array due to following reasons:
  // 1. batch delete tablet items doesn't need atomic semantic, they can be written in different log items
  // 2. log item batch header count is int16_t type, we can't over the limit
  const int64_t MAX_ARRAY_SIZE = 32000;
  const int64_t total_cnt = items.count();
  ObSEArray<ObPendingFreeTabletItem, 16> cur_items;
  int64_t finish_cnt = 0;
  int64_t cur_cnt = 0;
  WaitGCTabletArray *tablet_array = nullptr;
  {
    lib::ObMutexGuard guard(wait_gc_map_lock_);
    if (OB_FAIL(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array))) {
      LOG_WARN("fail to wait gc tablet array", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_HASH_NOT_EXIST == ret) {
      tablet_array = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get wait gc tablet array", K(ret), K(key));
    }
  } else if (OB_ISNULL(tablet_array)) {
    ret = OB_ERR_UNEXPECTED; // get_refactored successfully, but tablet array = nullptr
    LOG_WARN("array info is nullptr", K(ret), K(key));
  }
  while (OB_SUCC(ret) && finish_cnt < total_cnt) {
    cur_items.reset();
    cur_cnt = MIN(MAX_ARRAY_SIZE, total_cnt - finish_cnt);
    if (OB_FAIL(cur_items.reserve(cur_cnt))) {
      STORAGE_REDO_LOG(WARN, "reserve array fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    }
    for (int64_t i = finish_cnt; OB_SUCC(ret) && i < finish_cnt + cur_cnt; ++i) {
      const ObPendingFreeTabletItem &item = items.at(i);
      if (OB_FAIL(cur_items.push_back(item))) {
        STORAGE_REDO_LOG(WARN, "push back tablet item fail", K(ret), K(cur_cnt), K(total_cnt),
            K(finish_cnt), K(i), K(item));
      }
    }
    if (FAILEDx(safe_batch_write_gc_tablet_slog(ls_id, ls_epoch, cur_items, *tablet_array))) {
      STORAGE_REDO_LOG(WARN, "inner write log fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    } else {
      finish_cnt += cur_cnt;
    }
  }
  return ret;
}

int ObTenantStorageMetaService::safe_batch_write_gc_tablet_slog(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObIArray<ObPendingFreeTabletItem> &items,
    WaitGCTabletArray &tablet_array)
{
  int ret = OB_SUCCESS;
  const int64_t item_count = items.count();
  const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                             ObRedoLogSubType::OB_REDO_LOG_GC_TABLET);
  ObSArray<ObGCTabletLog> slog_array;
  ObSArray<ObStorageLogParam> param_array;
  const ObIArray<ObPendingFreeTabletItem> &arr = tablet_array.wait_gc_tablet_arr_.items_;
  common::ObSArray<ObPendingFreeTabletItem> tmp;
  if (OB_FAIL(slog_array.reserve(item_count))) {
    LOG_WARN("failed to reserve for slog array", K(ret), K(item_count));
  } else if (OB_FAIL(param_array.reserve(item_count))) {
    LOG_WARN("failed to reserve for param array", K(ret), K(item_count));
  } else if (OB_FAIL(tmp.reserve(arr.count()))) {
    LOG_WARN("failed to reserve for tmp array", K(ret), K(arr.count()));
  } else {
    lib::ObMutexGuard guard(tablet_array.lock_);
    int64_t delete_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && delete_cnt < item_count && i < arr.count(); ++i) {
      const ObPendingFreeTabletItem &item = arr.at(i);
      if (OB_UNLIKELY(!item.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet item", K(ret), K(ls_id), K(ls_epoch), K(item));
      } else if (has_exist_in_array(items, item)) {
        delete_cnt++;
        ObGCTabletLog slog_entry(ls_id,
                                 ls_epoch,
                                 item.tablet_id_,
                                 item.tablet_meta_version_,
                                 item.status_,
                                 item.tablet_private_transfer_epoch_);
        if (OB_FAIL(slog_array.push_back(slog_entry))) {
          LOG_WARN("fail to push slog entry into slog array", K(ret), K(slog_entry), K(i));
        }
      } else if (OB_FAIL(tmp.push_back(item))) {
        LOG_WARN("failed to push_back", K(ret), K(tmp), K(arr), K(items));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_cnt; i++) {
      ObStorageLogParam log_param(cmd, &slog_array[i]);
      if (OB_FAIL(param_array.push_back(log_param))) {
        LOG_WARN("fail to push log param into param array", K(ret), K(log_param), K(i));
      }
    }
    if (OB_FAIL(ret)) {
      // error occurred
    } else if (items.count() != delete_cnt) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("deleting item(s) do not all exist in pending_free_arr", K(ret), K(items), K(arr), K(tmp));
    } else if (OB_FAIL(slogger_.write_log(param_array))) {
      LOG_WARN("fail to write slog for batch deleting tablet", K(ret), K(param_array));
    } else if (OB_FAIL(tablet_array.wait_gc_tablet_arr_.items_.assign(tmp))) {
      LOG_WARN("fail to sync delete tablet item", K(ret), K(tmp));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE

#endif

int ObTenantStorageMetaService::get_ls_items_by_status(
    const storage::ObLSItemStatus status,
    ObIArray<storage::ObLSItem> &ls_items)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    ls_items.reuse();
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    HEAP_VAR(ObLSItemIterator, ls_item_iter, tenant->get_super_block()) {
      ObLSItem ls_item;
      while (OB_SUCC(ls_item_iter.get_next_ls_item(ls_item))) {
        if (status == ls_item.status_ &&
            OB_FAIL(ls_items.push_back(ls_item))) {
          LOG_WARN("failed to push back tenant_item", K(ret), K(ls_item), K(ls_items), K(ls_item_iter));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tenant items by status", K(ret), K(ls_item), K(ls_items), K(ls_item_iter));
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::write_remove_tablet_slog_for_sn(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDeleteTabletLog slog_entry(ls_id, tablet_id);
  ObStorageLogParam log_param;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
  log_param.data_ = &slog_entry;
  if (OB_FAIL(slogger_.write_log(log_param))) {
    LOG_WARN("fail to write remove tablet slog", K(ret), K(log_param));
  }
  return ret;
}

int ObTenantStorageMetaService::write_remove_tablet_slog_for_ss(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletID &tablet_id,
    const ObTablet &tablet,
    const int64_t last_gc_version)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData data;
  const ObMetaDiskAddr &tablet_addr = tablet.get_tablet_addr();
  GCTabletType gc_type = GCTabletType::TransferOut;
  WaitGCTabletArray *tablet_array = nullptr;
  if (OB_UNLIKELY(last_gc_version < -1 || last_gc_version >= static_cast<int64_t>(tablet_addr.block_id().meta_version_id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last_gc_version", K(ret), K(last_gc_version), K(tablet_addr));
  } else if (OB_FAIL(tablet.get_tablet_status(share::SCN::max_scn(), data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      gc_type = GCTabletType::CreateAbort;
      LOG_INFO("tablet_status is not commit", KR(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get CreateDeleteMdsUserData", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (ObTabletStatus::DELETED == data.tablet_status_) {
    gc_type = GCTabletType::DropTablet;
  }
  if (OB_SUCC(ret)) {
    const WaitGCTabletArrayKey key(ls_id, ls_epoch);
    lib::ObMutexGuard guard(wait_gc_map_lock_);
    if (OB_FAIL(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // overwrite return code
        const lib::ObMemAttr attr(MTL_ID(), "WaitGCTabArr");
        if (OB_ISNULL(tablet_array = OB_NEW(WaitGCTabletArray, attr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", K(ret), K(key));
        } else if (OB_FAIL(wait_gc_tablet_arr_map_.set_refactored(key, tablet_array))) {
          LOG_WARN("fail to set wait gc tablet array info", K(ret), K(key));
          OB_DELETE(WaitGCTabletArray, attr, tablet_array);
        }
      } else {
        LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
      }
    }
  }
  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(tablet_array->lock_);
    const ObPendingFreeTabletItem tablet_item(tablet_id,
                                              tablet_addr.block_id().meta_version_id(),
                                              ObPendingFreeTabletStatus::WAIT_GC,
                                              ObTimeUtility::fast_current_time(),
                                              gc_type,
                                              tablet_addr.block_id().meta_private_transfer_epoch(),
                                              last_gc_version);
    ObDeleteTabletLog slog_entry(ls_id,
                                 tablet_id,
                                 ls_epoch,
                                 tablet_item.tablet_meta_version_,
                                 tablet_item.status_,
                                 tablet_item.free_time_,
                                 tablet_item.gc_type_,
                                 tablet_item.tablet_private_transfer_epoch_,
                                 tablet_item.last_gc_version_);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
    log_param.data_ = &slog_entry;
    if (has_exist_in_array(tablet_array->wait_gc_tablet_arr_.items_, tablet_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet_item has existed in wait gc tablet arr", K(ret), K(tablet_array), K(tablet_item));
    } else if (OB_FAIL(slogger_.write_log(log_param))) {
      LOG_WARN("fail to write remove tablet slog", K(ret), K(log_param));
    } else if (OB_FAIL(tablet_array->wait_gc_tablet_arr_.items_.push_back(tablet_item))) {
      LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
    } else {
      FLOG_INFO("succeed to remove tablet", K(ret), K(tablet_item));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::batch_write_remove_tablet_slog(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObIArray<ObTabletID> &tablet_ids,
    const common::ObIArray<ObMetaDiskAddr> &tablet_addrs,
    const common::ObIArray<int64_t> &last_gc_versions)
{
  int ret = OB_SUCCESS;
  // We can split the tablet_ids array due to following reasons:
  // 1. batch remove tablets doesn't need atomic semantic, they can be written in different log items
  // 2. log item batch header count is int16_t type, we can't over the limit
  const int64_t MAX_ARRAY_SIZE = 32000;
  const int64_t total_cnt = tablet_ids.count();
  ObSEArray<TabletInfo, 16> current_tablet_arr;
  int64_t finish_cnt = 0;
  int64_t cur_cnt = 0;
  int64_t item_arr_expand_cnt = total_cnt;
  if (OB_UNLIKELY(is_shared_storage_ && last_gc_versions.count() != tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last_gc_versions", K(ret), K(is_shared_storage_), K(last_gc_versions.count()), K(tablet_ids.count()));
  }

  while (OB_SUCC(ret) && finish_cnt < total_cnt) {
    current_tablet_arr.reset();
    cur_cnt = MIN(MAX_ARRAY_SIZE, total_cnt - finish_cnt);
    if (OB_FAIL(current_tablet_arr.reserve(cur_cnt))) {
      STORAGE_REDO_LOG(WARN, "reserve array fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    }
    for (int64_t i = finish_cnt; OB_SUCC(ret) && i < finish_cnt + cur_cnt; ++i) {
      int64_t last_gc_version = is_shared_storage_ ? last_gc_versions.at(i) : -1;
      TabletInfo info(tablet_ids.at(i), tablet_addrs.at(i), last_gc_version);
      if (OB_FAIL(current_tablet_arr.push_back(info))) {
        STORAGE_REDO_LOG(WARN, "push back tablet info fail", K(ret), K(cur_cnt), K(total_cnt),
            K(finish_cnt), K(i), K(tablet_ids.at(i)), K(tablet_addrs.at(i)));
      }
    }
    if (OB_FAIL(ret)){
    } else if (!is_shared_storage_ && OB_FAIL(safe_batch_write_remove_tablet_slog_for_sn(ls_id, current_tablet_arr))) {
      STORAGE_REDO_LOG(WARN, "inner write log fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    } else if (is_shared_storage_ && OB_FAIL(safe_batch_write_remove_tablet_slog_for_ss(ls_id, ls_epoch, current_tablet_arr, item_arr_expand_cnt))) {
      STORAGE_REDO_LOG(WARN, "inner write log fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    } else {
      item_arr_expand_cnt = 0;
      finish_cnt += cur_cnt;
    }
  }
  return ret;
}

int ObTenantStorageMetaService::safe_batch_write_remove_tablet_slog_for_sn(
    const ObLSID &ls_id,
    const common::ObIArray<TabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_count = tablet_infos.count();
  const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
  ObSArray<ObDeleteTabletLog> slog_array;
  ObSArray<ObStorageLogParam> param_array;
  const bool need_write = (tablet_count > 0);

  if (!need_write) {
  } else if (OB_FAIL(slog_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for slog array", K(ret), K(tablet_count));
  } else if (OB_FAIL(param_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for param array", K(ret), K(tablet_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; ++i) {
      const ObTabletID &tablet_id = tablet_infos.at(i).tablet_id_;
      ObDeleteTabletLog slog_entry(ls_id, tablet_id);
      if (OB_UNLIKELY(!tablet_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet id is invalid", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(slog_array.push_back(slog_entry))) {
        LOG_WARN("fail to push slog entry into slog array", K(ret), K(slog_entry), K(i));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
      ObStorageLogParam log_param(cmd, &slog_array[i]);
      if (OB_FAIL(param_array.push_back(log_param))) {
        LOG_WARN("fail to push log param into param array", K(ret), K(log_param), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!need_write) {
  } else if (OB_FAIL(slogger_.write_log(param_array))) {
    LOG_WARN("fail to write slog for batch deleting tablet", K(ret), K(param_array));
  }
  return ret;
}

int ObTenantStorageMetaService::safe_batch_write_remove_tablet_slog_for_ss(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObIArray<TabletInfo> &tablet_infos,
    const int64_t item_arr_expand_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_count = tablet_infos.count();
  if (tablet_count > 0) {
    WaitGCTabletArray *tablet_array = nullptr;
    ObLSPendingFreeTabletArray tmp_pending_free_array;
    const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                               ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
    ObSArray<ObDeleteTabletLog> slog_array;
    ObSArray<ObStorageLogParam> param_array;
    const WaitGCTabletArrayKey key(ls_id, ls_epoch);
    {
      lib::ObMutexGuard guard(wait_gc_map_lock_);
      if (OB_FAIL(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // overwrite return code
          const lib::ObMemAttr attr(MTL_ID(), "WaitGCTabArr");
          if (OB_ISNULL(tablet_array = OB_NEW(WaitGCTabletArray, attr))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mem", K(ret), K(key));
          } else if (OB_FAIL(wait_gc_tablet_arr_map_.set_refactored(key, tablet_array))) {
            LOG_WARN("fail to set wait gc tablet array info", K(ret), K(key));
            OB_DELETE(WaitGCTabletArray, attr, tablet_array);
          }
        } else {
          LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(tablet_array) && item_arr_expand_cnt > 0) {
        const uint64_t cnt = tablet_array->wait_gc_tablet_arr_.items_.count() + item_arr_expand_cnt;
        if (OB_FAIL(tablet_array->wait_gc_tablet_arr_.items_.reserve(cnt))) {
          LOG_WARN("failed to reserve for pending free tablet array", K(ret), K(cnt), K(item_arr_expand_cnt));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tmp_pending_free_array.items_.reserve(tablet_count))) {
      LOG_WARN("failed to reserve for pending free tablet array", K(ret), K(tablet_count));
    } else if (OB_FAIL(slog_array.reserve(tablet_count))) {
      LOG_WARN("failed to reserve for slog array", K(ret), K(tablet_count));
    } else if (OB_FAIL(param_array.reserve(tablet_count))) {
      LOG_WARN("failed to reserve for param array", K(ret), K(tablet_count));
    } else {
      lib::ObMutexGuard guard(tablet_array->lock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; ++i) {
        const TabletInfo &tablet_info = tablet_infos.at(i);
        const ObTabletID &tablet_id = tablet_info.tablet_id_;
        const ObMetaDiskAddr &tablet_addr = tablet_info.tablet_addr_;
        const int64_t last_gc_version = tablet_info.last_gc_version_;
        const ObPendingFreeTabletItem tablet_item(tablet_id,
                                                  tablet_addr.block_id().meta_version_id(),
                                                  ObPendingFreeTabletStatus::WAIT_GC,
                                                  INT64_MAX /* delete_time */,
                                                  GCTabletType::DropLS,
                                                  tablet_addr.block_id().meta_private_transfer_epoch(),
                                                  last_gc_version);
        ObDeleteTabletLog slog_entry(ls_id,
                                     tablet_id,
                                     ls_epoch,
                                     tablet_item.tablet_meta_version_,
                                     tablet_item.status_,
                                     tablet_item.free_time_,
                                     tablet_item.gc_type_,
                                     tablet_item.tablet_private_transfer_epoch_,
                                     tablet_item.last_gc_version_);
        if (OB_UNLIKELY(!tablet_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet info is invalid", K(ret), K(tablet_info), K(tablet_item.tablet_meta_version_));
        } else if (has_exist_in_array(tmp_pending_free_array.items_, tablet_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet_item has existed in pending free tablet arr", K(ret), K(tmp_pending_free_array), K(tablet_item));
        } else if (OB_FAIL(tmp_pending_free_array.items_.push_back(tablet_item))) {
          LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
        } else if (OB_FAIL(slog_array.push_back(slog_entry))) {
          LOG_WARN("fail to push slog entry into slog array", K(ret), K(slog_entry), K(i));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
        ObStorageLogParam log_param(cmd, &slog_array[i]);
        if (OB_FAIL(param_array.push_back(log_param))) {
          LOG_WARN("fail to push log param into param array", K(ret), K(log_param), K(i));
        }
      }
      if (FAILEDx(slogger_.write_log(param_array))) {
        LOG_WARN("fail to write slog for batch deleting tablet", K(ret), K(param_array));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; ++i) {
          const ObPendingFreeTabletItem &tablet_item = tmp_pending_free_array.items_.at(i);
          if (OB_FAIL(tablet_array->wait_gc_tablet_arr_.items_.push_back(tablet_item))) {
            LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::replay_wait_gc_tablet_array(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObLSPendingFreeTabletArray deleting_tablets;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for non-shared storage mode", K(ret));
  } else if (OB_FAIL(deleting_tablets.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize delete tablets", K(ret));
  } else {
    const lib::ObMemAttr attr(MTL_ID(), "WaitGCTabArr");
    const WaitGCTabletArrayKey key(deleting_tablets.ls_id_, deleting_tablets.ls_epoch_);
    WaitGCTabletArray *tablet_array = nullptr;
    lib::ObMutexGuard guard(wait_gc_map_lock_);
    if (OB_FAIL(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array)) && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get wait gc tablet array", K(ret), K(key));
    } else if (OB_SUCC(ret) || OB_NOT_NULL(tablet_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in rebooting, wait gc tablet array should be nullptr", K(key), KPC(tablet_array));
    } else if (OB_ISNULL(tablet_array = OB_NEW(WaitGCTabletArray, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(ret), K(key));
    } else if (OB_FAIL(tablet_array->wait_gc_tablet_arr_.assign(deleting_tablets))) {
      LOG_WARN("fail to assign tablet array", K(ret), K(key), K(deleting_tablets), KPC(tablet_array));
    } else if (OB_FAIL(wait_gc_tablet_arr_map_.set_refactored(key, tablet_array))) {
      LOG_WARN("fail to set wait gc tablet array", K(ret), K(key));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(tablet_array)) {
      OB_DELETE(WaitGCTabletArray, attr, tablet_array);
    }
  }
  return ret;
}

int ObTenantStorageMetaService::replay_apply_wait_gc_tablet_items(
    ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet &wait_gc_set,
    ObTenantCheckpointSlogHandler::ReplayGCTabletSet &gc_set)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for non-shared storage mode", K(ret));
  } else {
    const lib::ObMemAttr attr(MTL_ID(), "WaitGCTabArr");
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    HEAP_VAR(ObTenantSuperBlock, tenant_super_block, tenant->get_super_block()) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < tenant_super_block.ls_cnt_; ++idx) {
        const ObLSItem &ls_item = tenant_super_block.ls_item_arr_[idx];
        const WaitGCTabletArrayKey key(ls_item.ls_id_, ls_item.epoch_);
        ObLSPendingFreeTabletArray wait_gc_tablets;
        WaitGCTabletArray *tmp_tablet_array = nullptr;
        WaitGCTabletArray *tablet_array = nullptr;
        wait_gc_tablets.ls_id_ = ls_item.ls_id_;
        wait_gc_tablets.ls_epoch_ = ls_item.epoch_;
        if (OB_FAIL(get_wait_gc_tablet_items_except_gc_set(ls_item.ls_id_, ls_item.epoch_, gc_set, wait_gc_set, wait_gc_tablets.items_))) {
          LOG_WARN("fail to get wait gc tablet items except gc set", K(ret), K(ls_item));
        } else if (OB_FAIL(add_wait_gc_set_items_except_gc_set(ls_item.ls_id_, ls_item.epoch_, gc_set, wait_gc_set, wait_gc_tablets.items_))) {
          LOG_WARN("fail to add wait gc set items except gc set", K(ret), K(ls_item));
        }
        {
          lib::ObMutexGuard guard(wait_gc_map_lock_);
          if (FAILEDx(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array)) && OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("fail to get wait gc tablet array", K(ret), K(key));
          } else if (OB_SUCC(ret)) {
            // nothing to do.
          } else if (OB_ISNULL(tmp_tablet_array = OB_NEW(WaitGCTabletArray, attr))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mem", K(ret), K(key));
          } else {
            tablet_array = tmp_tablet_array;
            ret = OB_SUCCESS;
          }
          if (FAILEDx(tablet_array->wait_gc_tablet_arr_.assign(wait_gc_tablets))) {
            LOG_WARN("fail to assign tablet array", K(ret), K(key), K(wait_gc_tablets), KPC(tablet_array));
          } else if (OB_NOT_NULL(tmp_tablet_array) && OB_FAIL(wait_gc_tablet_arr_map_.set_refactored(key, tablet_array))) {
            LOG_WARN("fail to set wait gc tablet array", K(ret), K(key));
          }
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(tmp_tablet_array)) {
          OB_DELETE(WaitGCTabletArray, attr, tmp_tablet_array);
        }
      }
    } // HEAP_VAR
  }
  return ret;
}

int ObTenantStorageMetaService::get_wait_gc_tablet_items_except_gc_set(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTenantCheckpointSlogHandler::ReplayGCTabletSet &gc_set,
    ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet &wait_gc_set,
    common::ObIArray<ObPendingFreeTabletItem> &final_items)
{
  int ret = OB_SUCCESS;
  common::ObSArray<ObPendingFreeTabletItem> items;
  if (OB_FAIL(get_wait_gc_tablet_items(ls_id, ls_epoch, items))) {
    LOG_WARN("fail to get wait gc tablet items", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < items.count(); ++j) {
      const ObPendingFreeTabletItem &tablet_item = items.at(j);
      const ObGCTabletLog gc_log(ls_id,
                                 ls_epoch,
                                 tablet_item.tablet_id_,
                                 tablet_item.tablet_meta_version_,
                                 tablet_item.status_,
                                 tablet_item.tablet_private_transfer_epoch_);
      const ObDeleteTabletLog del_log(ls_id,
                                      tablet_item.tablet_id_,
                                      ls_epoch,
                                      tablet_item.tablet_meta_version_,
                                      tablet_item.status_,
                                      tablet_item.free_time_,
                                      tablet_item.gc_type_,
                                      tablet_item.tablet_private_transfer_epoch_,
                                      tablet_item.last_gc_version_);
      if (OB_FAIL(gc_set.exist_refactored(gc_log)) && OB_HASH_NOT_EXIST != ret) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS; // just skip, nothing to do.
        } else {
          LOG_WARN("fail to check exist in gc set", K(ret), K(gc_log));
        }
      } else if (OB_FAIL(final_items.push_back(tablet_item))) {
        LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
      } else if (OB_FAIL(wait_gc_set.exist_refactored(del_log)) && OB_HASH_EXIST != ret) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // just skip, nothing to do.
        } else {
          LOG_WARN("fail to check exist in wait gc set", K(ret), K(del_log));
        }
      } else if (OB_FAIL(wait_gc_set.erase_refactored(del_log))) {
        LOG_WARN("fail to erase from wait gc set", K(ret), K(del_log));
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::add_wait_gc_set_items_except_gc_set(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTenantCheckpointSlogHandler::ReplayGCTabletSet &gc_set,
    const ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet &wait_gc_set,
    common::ObIArray<ObPendingFreeTabletItem> &final_items)
{
  int ret = OB_SUCCESS;
  for (ObTenantCheckpointSlogHandler::ReplayWaitGCTabletSet::const_iterator iter = wait_gc_set.begin();
       iter != wait_gc_set.end();
       ++iter) {
    const ObDeleteTabletLog &log = iter->first;
    if (log.ls_id_ == ls_id && log.ls_epoch_ == ls_epoch) {
      const ObPendingFreeTabletItem tablet_item(log.tablet_id_,
                                                log.tablet_meta_version_,
                                                log.status_,
                                                log.free_time_,
                                                log.gc_type_,
                                                log.tablet_private_transfer_epoch_,
                                                log.last_gc_version_);
      const ObGCTabletLog gc_log(ls_id,
                                 ls_epoch,
                                 tablet_item.tablet_id_,
                                 tablet_item.tablet_meta_version_,
                                 tablet_item.status_,
                                 tablet_item.tablet_private_transfer_epoch_);
      if (OB_FAIL(gc_set.exist_refactored(gc_log)) && OB_HASH_NOT_EXIST != ret) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS; // just skip, nothing to do.
        } else {
          LOG_WARN("fail to check exist in gc set", K(ret), K(gc_log));
        }
      } else if (OB_FAIL(final_items.push_back(tablet_item))) {
        LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::get_max_meta_version_from_wait_gc_map(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletID &tablet_id,
    /*out*/ int64_t &max_meta_version)
{
  int ret = OB_SUCCESS;
  max_meta_version = 0;

  if (!GCTX.is_shared_storage_mode()) {
    // do nothing
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageMetaSerivce has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id or tablet id", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(ls_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls epoch", K(ret), K(ls_epoch));
  } else {
    WaitGCTabletArray *tablet_array = nullptr;
    const WaitGCTabletArrayKey key(ls_id, ls_epoch);
    {
      // hold map's lock
      lib::ObMutexGuard guard(wait_gc_map_lock_);
      if (OB_FAIL(wait_gc_tablet_arr_map_.get_refactored(key, tablet_array))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
        }
      } else if (OB_ISNULL(tablet_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null tablet array", K(ret), K(key), KP(tablet_array));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(tablet_array)) {
      // hold array's lock
      lib::ObMutexGuard guard(tablet_array->lock_);
      const ObIArray<ObPendingFreeTabletItem> &pending_free_arr = tablet_array->wait_gc_tablet_arr_.items_;
      for (int64_t i = 0; OB_SUCC(ret) && i < pending_free_arr.count(); ++i) {
        const ObPendingFreeTabletItem &item = pending_free_arr.at(i);
        if (item.tablet_id_ == tablet_id) {
          max_meta_version = MAX(max_meta_version, item.tablet_meta_version_);
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
