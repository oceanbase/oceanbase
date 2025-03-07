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

#include "ob_tenant_storage_meta_persister.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "observer/omt/ob_tenant.h"
#ifdef OB_BUILD_SHARED_STORAGE
#endif

namespace oceanbase
{
using namespace omt;
using namespace blocksstable;
namespace storage
{

int ObTenantStorageMetaPersister::init(
    const bool is_shared_storage,
    ObStorageLogger &slogger,
    ObTenantCheckpointSlogHandler &ckpt_slog_handler)
{
  int ret = OB_SUCCESS;

  const int64_t MEM_LIMIT = 512UL << 20;
  lib::ObMemAttr attr(MTL_ID(), "TntMetaPersist");
  const int64_t MAP_BUCKET_CNT = 256;
  lib::ObMemAttr map_attr(MTL_ID(), "PendingFreeMap");

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantStorageMetaPersister has inited", K(ret));
  } else if (OB_FAIL(allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE, attr, MEM_LIMIT))) {
    LOG_WARN("fail to init fifo allocator", K(ret));
  } else if (OB_FAIL(pending_free_tablet_arr_map_.create(MAP_BUCKET_CNT, map_attr))) {
    LOG_WARN("fail to create pending_free_tablet_arr_map", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    slogger_ = &slogger;
    ckpt_slog_handler_ = &ckpt_slog_handler;
    is_inited_ = true;
  }
  return ret;
}

void ObTenantStorageMetaPersister::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    for (PendingFreeTabletArrayMap::iterator iter = pending_free_tablet_arr_map_.begin();
         iter !=  pending_free_tablet_arr_map_.end(); iter++) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("PendingFreeTabletArrayInfo is null", K(ret), K(iter->first));
      } else {
        ob_delete(iter->second);
      }
    }
    pending_free_tablet_arr_map_.destroy();
    slogger_ = nullptr;
    ckpt_slog_handler_ = nullptr;
    allocator_.reset();
    is_inited_ = false;
  }
}

int ObTenantStorageMetaPersister::prepare_create_ls(const ObLSMeta &meta, int64_t &ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    ls_epoch = 0;
    if (OB_FAIL(write_prepare_create_ls_slog_(meta))) {
      LOG_WARN("fail to write prepare create ls slog", K(ret), K(meta));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_prepare_create_ls_(meta, ls_epoch))) {
      LOG_WARN("fail to prepare create ls", K(ret), K(meta));
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::commit_create_ls(
    const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_commit_create_ls_slog_(ls_id))) {
      LOG_WARN("fail to write commit create ls slog", K(ret), K(ls_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_commit_create_ls_(ls_id, ls_epoch))) {
      LOG_WARN("fail to commit create ls", K(ret), K(ls_id), K(ls_epoch));
    }
#endif
  }
  return ret;

}
int ObTenantStorageMetaPersister::abort_create_ls(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_abort_create_ls_slog_(ls_id))) {
      LOG_WARN("fail to write abort create ls slog", K(ret), K(ls_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_abort_create_ls_(ls_id, ls_epoch))) {
      LOG_WARN("fail to abort create ls", K(ret), K(ls_id), K(ls_epoch));
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::delete_ls(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_delete_ls_slog_(ls_id))) {
      LOG_WARN("fail to write delete ls slog", K(ret), K(ls_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_delete_ls_(ls_id, ls_epoch))) {
      LOG_WARN("fail to delete ls", K(ret), K(ls_id), K(ls_epoch));
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::update_ls_meta(const int64_t ls_epoch, const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_update_ls_meta_slog_(ls_meta))) {
      LOG_WARN("fail to write update ls meta slog", K(ret), K(ls_meta));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_write_ls_meta_(ls_epoch, ls_meta))) {
      LOG_WARN("fail to wirte ls meta", K(ret), K(ls_meta));
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::update_dup_table_meta(
    const int64_t ls_epoch,
    const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_table_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_update_dup_table_meta_slog_(dup_table_meta))) {
      LOG_WARN("fail to write update ls meta slog", K(ret), K(dup_table_meta));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_write_dup_table_meta_(ls_epoch, dup_table_meta))) {
      LOG_WARN("fail to wirte ls meta", K(ret), K(dup_table_meta));
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::update_tenant_preallocated_seqs(
    const ObTenantMonotonicIncSeqs &preallocated_seqs)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support for shared-nothing", K(ret));
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    lib::ObMutexGuard guard(super_block_lock_);
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    ObTenantSuperBlock tenant_super_block = tenant->get_super_block();
    tenant_super_block.preallocated_seqs_ = preallocated_seqs;

    if (OB_FAIL(ss_write_tenant_super_block_(tenant_super_block))) {
      LOG_WARN("fail to write tenant super block", K(ret), K(tenant_super_block));
    } else {
      tenant->set_tenant_super_block(tenant_super_block);
      FLOG_INFO("update tenant preallocated seqs", K(ret), K(tenant_super_block));
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::batch_update_tablet(const ObIArray<ObUpdateTabletLog> &slog_arr)
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
    } else if (OB_FAIL(slogger_->write_log(param_arr))) {
      LOG_WARN("fail to batch write slog", K(ret), K(param_arr.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < param_arr.count(); i++) {
        const ObStorageLogParam &log_param = param_arr.at(i);
        const ObUpdateTabletLog *slog = reinterpret_cast<const ObUpdateTabletLog*>(log_param.data_);
        const ObTabletMapKey tablet_key(slog->ls_id_, slog->tablet_id_);
        do {
          if (OB_FAIL(ckpt_slog_handler_->report_slog(tablet_key, log_param.disk_addr_))) {
            if (OB_ALLOCATE_MEMORY_FAILED != ret) {
              LOG_WARN("fail to report slog", K(ret), K(tablet_key), K(log_param));
            } else if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
              LOG_WARN("fail to report slog due to memory limit", K(ret), K(tablet_key), K(log_param));
            }
          }
        } while (OB_ALLOCATE_MEMORY_FAILED == ret);
      }
    }
  }

  return ret;
}

int ObTenantStorageMetaPersister::update_tablet(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObTabletID &tablet_id,
    const ObMetaDiskAddr &tablet_addr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !tablet_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(tablet_addr));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(write_update_tablet_slog_(ls_id, tablet_id, tablet_addr))) {
      LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(tablet_addr));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_update_tablet_(ls_id, ls_epoch,  tablet_id, tablet_addr))) {
      LOG_WARN("fail to update tablet", K(ret), K(tablet_id), K(tablet_addr));
    }
#endif

  }
  return ret;
}

int ObTenantStorageMetaPersister::write_active_tablet_array(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (!is_shared_storage_) {
    // do nothing for shared-nothing
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    ObMetaDiskAddr addr;
    ObLSTabletAddrIterator tablet_iter;
    ObTabletMapKey tablet_key;
    ObLSActiveTabletArray active_tablet_arr;
    if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
      LOG_WARN("fail to build ls tablet iter", K(ret), KPC(ls));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_iter.get_next_tablet_addr(tablet_key, addr))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next tablet", K(ret));
        }
      } else if (OB_UNLIKELY(!tablet_key.is_valid() || !addr.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet key or addr is invalid", K(ret), K(tablet_key), K(addr));
      } else if (!addr.is_disked()) {
        FLOG_INFO("skip MEM and NONE type", K(ret), K(tablet_key), K(addr));
      } else if (OB_FAIL(active_tablet_arr.items_.push_back(ObActiveTabletItem(tablet_key.tablet_id_, addr.block_id().fourth_id())))) {
        LOG_WARN("fail to push back active tablet item", K(ret), K(tablet_key), K(addr));
      }
    }
    if (OB_SUCC(ret) && active_tablet_arr.items_.count() > 0) {
      if (OB_FAIL(ss_write_active_tablet_array_(ls->get_ls_id(), ls->get_ls_epoch(), active_tablet_arr))) {
        LOG_WARN("fail to write active tablet array", K(ret), K(ls->get_ls_id()));
      }
    }
#endif
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_empty_shell_tablet(ObTablet *tablet, ObMetaDiskAddr &tablet_addr)
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
    ObEmptyShellTabletLog slog_entry(tablet->get_tablet_meta().ls_id_,
                                     tablet->get_tablet_meta().tablet_id_,
                                     tablet);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_FAIL(slogger_->write_log(log_param))) {
      LOG_WARN("fail to write slog for empty shell tablet", K(ret), K(log_param));
    } else if (OB_FAIL(ckpt_slog_handler_->report_slog(tablet_key, log_param.disk_addr_))) {
      LOG_WARN("fail to report slog", K(ret), K(tablet_key));
    } else {
      tablet_addr = log_param.disk_addr_;
    }
  }
  return ret;
}

int ObTenantStorageMetaPersister::remove_tablet(
    const share::ObLSID &ls_id, const int64_t ls_epoch,
    const ObTabletHandle &tablet_handle)
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

    if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !tablet_addr.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(tablet_addr));
    } else if (!is_shared_storage_) {
      if (OB_FAIL(write_remove_tablet_slog_(ls_id, tablet_id))) {
        LOG_WARN("fail to write remove tablet slog", K(ret), K(ls_id), K(tablet_id));
      }
    } else {
#ifdef OB_BUILD_SHARED_STORAGE
      if (OB_FAIL(ss_remove_tablet_(ls_id, ls_epoch,  tablet_handle))) {
        LOG_WARN("fail to remove tablet", K(ret), K(tablet_id), K(tablet_addr));
      }
#endif
    }
  }
  return ret;
}

int ObTenantStorageMetaPersister::remove_tablets(
    const share::ObLSID &ls_id, const int64_t ls_epoch,
    const ObIArray<common::ObTabletID> &tablet_ids, const ObIArray<ObMetaDiskAddr> &tablet_addrs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(write_remove_tablets_slog_(ls_id, tablet_ids))) {
      LOG_WARN("fail to write remove tablets slog", K(ret), K(ls_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_batch_remove_ls_tablets(ls_id, ls_epoch, tablet_ids, tablet_addrs, true/*delete_current_version*/))) {
      LOG_WARN("fail to remove tablets", K(ret), K(ls_id));
    }
#endif
  }
  return ret;

}


//=================================== SLOG ==============================================//
int ObTenantStorageMetaPersister::write_prepare_create_ls_slog_(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  ObCreateLSPrepareSlog slog_entry(ls_meta);
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                          ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);
  if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN("fail to write remove ls slog", K(log_param));
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_commit_create_ls_slog_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id = ls_id;
  ObCreateLSCommitSLog slog_entry(tmp_ls_id);
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                          ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_COMMIT);
  if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN("fail to write create ls commit slog", K(log_param));
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_abort_create_ls_slog_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id = ls_id;
  ObCreateLSAbortSLog slog_entry(tmp_ls_id);
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_ABORT);
  if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN("fail to write create ls abort slog", K(log_param));
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_delete_ls_slog_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id = ls_id;
  ObDeleteLSLog slog_entry(tmp_ls_id);
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_DELETE_LS);
  if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN("fail to write remove ls slog", K(log_param));
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_update_ls_meta_slog_(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  ObLSMetaLog slog_entry(ls_meta);
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                          ObRedoLogSubType::OB_REDO_LOG_UPDATE_LS);
  if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN("fail to write update ls slog", K(log_param), K(ret));
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_update_dup_table_meta_slog_(
    const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_table_meta)
{
  int ret = OB_SUCCESS;
  ObDupTableCkptLog slog_entry;
  ObStorageLogParam log_param;
  log_param.data_ = &slog_entry;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                          ObRedoLogSubType::OB_REDO_LOG_UPDATE_DUP_TABLE_LS);
  if (OB_FAIL(slog_entry.init(dup_table_meta))) {
    LOG_WARN("init slog entry failed", K(ret),K(dup_table_meta));
  } else if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN( "fail to write dup table meta slog", K(ret), K(log_param));
  }
  return ret;
}
int ObTenantStorageMetaPersister::write_update_tablet_slog_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMetaDiskAddr &disk_addr)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey tablet_key(ls_id, tablet_id);
  if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync_block())) { // make sure that all data or meta written on the macro block is flushed
    LOG_WARN("fail to fsync_block", K(ret));
  } else {
    ObUpdateTabletLog slog_entry(ls_id, tablet_id, disk_addr);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_FAIL(slogger_->write_log(log_param))) {
      LOG_WARN("fail to write slog for creating tablet", K(ret), K(log_param));
    } else {
      do {
        if (OB_FAIL(ckpt_slog_handler_->report_slog(tablet_key, log_param.disk_addr_))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret) {
            LOG_WARN("fail to report slog", K(ret), K(tablet_key));
          } else if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
            LOG_WARN("fail to report slog due to memory limit", K(ret), K(tablet_key));
          }
        }
      } while (OB_ALLOCATE_MEMORY_FAILED == ret);
    }
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_remove_tablet_slog_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDeleteTabletLog slog_entry(ls_id, tablet_id);
  ObStorageLogParam log_param;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
  log_param.data_ = &slog_entry;
  if (OB_FAIL(slogger_->write_log(log_param))) {
    LOG_WARN("fail to write remove tablet slog", K(ret), K(log_param));
  }
  return ret;
}

int ObTenantStorageMetaPersister::write_remove_tablets_slog_(
    const ObLSID &ls_id, const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  // We can split the tablet_ids array due to following reasons:
  // 1. batch remove tablets doesn't need atomic semantic, they can be written in different log items
  // 2. log item batch header count is int16_t type, we can't over the limit
  const int64_t MAX_ARRAY_SIZE = 32000;
  const int64_t total_cnt = tablet_ids.count();
  ObSEArray<ObTabletID, 16> current_tablet_arr;
  int64_t finish_cnt = 0;
  int64_t cur_cnt = 0;
  while (OB_SUCC(ret) && finish_cnt < total_cnt) {
    current_tablet_arr.reset();
    cur_cnt = MIN(MAX_ARRAY_SIZE, total_cnt - finish_cnt);

    if (OB_FAIL(current_tablet_arr.reserve(cur_cnt))) {
      STORAGE_REDO_LOG(WARN, "reserve array fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    }
    for (int64_t i = finish_cnt; OB_SUCC(ret) && i < finish_cnt + cur_cnt; ++i) {
      if (OB_FAIL(current_tablet_arr.push_back(tablet_ids.at(i)))) {
        STORAGE_REDO_LOG(WARN, "push back tablet id fail", K(ret), K(cur_cnt), K(total_cnt),
            K(finish_cnt), K(i));
      }
    }
    if (OB_FAIL(ret)){
    } else if (OB_FAIL(safe_batch_write_remove_tablets_slog_(ls_id, current_tablet_arr))){
      STORAGE_REDO_LOG(WARN, "inner write log fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    } else {
      finish_cnt += cur_cnt;
    }
  }

  return ret;
}

int ObTenantStorageMetaPersister::safe_batch_write_remove_tablets_slog_(
    const ObLSID &ls_id, const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_count = tablet_ids.count();
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
      const ObTabletID &tablet_id = tablet_ids.at(i);
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
  } else if (OB_FAIL(slogger_->write_log(param_array))) {
    LOG_WARN("fail to write slog for batch deleting tablet", K(ret), K(param_array));
  }

  return ret;
}

int ObTenantStorageMetaPersister::get_items_from_pending_free_tablet_array(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    ObIArray<ObPendingFreeTabletItem> &items)
{
  int ret = OB_SUCCESS;
  items.reuse();
  PendingFreeTabletArrayKey key(ls_id, ls_epoch);
  PendingFreeTabletArrayInfo *array_info = nullptr;
  {
    lib::ObMutexGuard guard(peding_free_map_lock_);
    if (OB_FAIL(pending_free_tablet_arr_map_.get_refactored(key, array_info))) {
      LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_HASH_NOT_EXIST == ret) {
      array_info = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pending free tablet array info from map", K(ret), K(key));
    }
  } else if (OB_ISNULL(array_info)) {
    ret = OB_ERR_UNEXPECTED; // get_refactored successfully, but array_info = nullptr
    LOG_WARN("array info is nullptr", K(ret), K(key));
  } else {
    lib::ObMutexGuard guard(array_info->lock_);
    if (OB_FAIL(items.assign(array_info->pending_free_tablet_arr_.items_))) {
      LOG_WARN("fail to assign array", K(ret), K(array_info->pending_free_tablet_arr_.items_));
    }
  }
  return ret;
}
int ObTenantStorageMetaPersister::delete_items_from_pending_free_tablet_array(
    const ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObIArray<ObPendingFreeTabletItem> &items)
{
  int ret = OB_SUCCESS;

  PendingFreeTabletArrayKey key(ls_id, ls_epoch);
  PendingFreeTabletArrayInfo *array_info = nullptr;
  {
    lib::ObMutexGuard guard(peding_free_map_lock_);
    if (OB_FAIL(pending_free_tablet_arr_map_.get_refactored(key, array_info))) {
      LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
    }
  } // guard peding_free_map_lock_

  if (OB_FAIL(ret)) {
    if (OB_HASH_NOT_EXIST == ret) {
      array_info = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
    }
  } else if (OB_ISNULL(array_info)) {
    ret = OB_ERR_UNEXPECTED; // get_refactored successfully, but array_info = nullptr
    LOG_WARN("array info is nullptr", K(ret), K(key));
  } else {
    lib::ObMutexGuard guard(array_info->lock_);
    const ObIArray<ObPendingFreeTabletItem> &arr = array_info->pending_free_tablet_arr_.items_;
    common::ObSArray<ObPendingFreeTabletItem> tmp;
    tmp.reserve(arr.count());
    int64_t delete_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); i++) {
      if (has_exist_in_array(items, arr.at(i))) {
        delete_cnt++;
      } else if (OB_FAIL(tmp.push_back(arr.at(i)))) {
        LOG_WARN("failed to push_back", K(ret), K(tmp), K(arr), K(items));
      }
    }
    if (OB_FAIL(ret)) {
      // error occurred
    } else if (items.count() == delete_cnt) {
      // all deleted
      if (OB_FAIL(array_info->pending_free_tablet_arr_.items_.assign(tmp))) {
        LOG_WARN("failed to sync delete op to pending_free_items_arr", K(ret), K(tmp), K(arr), K(items));
      }
    } else if (items.count() != delete_cnt) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("deleting item(s) do not all exist in pending_free_arr", K(ret), K(items), K(arr), K(tmp));
    }
  }
  return ret;
}

//=================================== Shared-Storage =============================================//


#ifdef OB_BUILD_SHARED_STORAGE

int ObTenantStorageMetaPersister::ss_prepare_create_ls_(const ObLSMeta &meta, int64_t &ls_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tenant_ls_item_(meta.ls_id_, ls_epoch))) {
    LOG_WARN("fail to create tenant super block ls item", K(ret), K(meta));
  } else if (OB_FAIL(ss_write_ls_meta_(ls_epoch, meta))) {
    LOG_WARN("fail to write ls meta", K(ret), K(ls_epoch), K(meta));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_commit_create_ls_(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_tenant_ls_item_(ls_id, ls_epoch, ObLSItemStatus::CREATED))) {
    LOG_WARN("fail to update tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_abort_create_ls_(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_tenant_ls_item_(ls_id, ls_epoch, ObLSItemStatus::CREATE_ABORT))) {
    LOG_WARN("fail to update tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_delete_ls_(const share::ObLSID &ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_tenant_ls_item_(ls_id, ls_epoch, ObLSItemStatus::DELETED))) {
    LOG_WARN("fail to update tenant super block ls item", K(ret), K(ls_id), K(ls_epoch));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_update_tablet_(
    const ObLSID ls_id, const int64_t ls_epoch,
    const common::ObTabletID &tablet_id, const ObMetaDiskAddr &tablet_addr)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_private_tablet_meta_current_verison_object_opt(ls_id.id(), tablet_id.id());
  ObPrivateTabletCurrentVersion current_version;
  current_version.tablet_addr_ = tablet_addr;

  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, current_version, allocator_, MTL_ID(), ls_epoch))) {
    LOG_WARN("fail to write private tablet current version", K(ret), K(ls_epoch), K(current_version));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_delete_tablet_current_version_(
  const ObTabletID &tablet_id,
  const ObLSID &ls_id,
  const uint64_t ls_epoch)
{
  int ret = OB_SUCCESS;

  MacroBlockId current_version_block_id;
  ObTenantFileManager *file_manager = nullptr;
  ObStorageObjectOpt current_version_opt;

  current_version_opt.set_ss_private_tablet_meta_current_verison_object_opt(ls_id.id(), tablet_id.id());
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get file manager", K(ret), K(MTL_ID()), KP(file_manager));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(current_version_opt, current_version_block_id))) {
    LOG_WARN("fail to get object id", K(ret), K(current_version_opt), K(current_version_block_id));
  } else if (OB_FAIL(file_manager->delete_file(current_version_block_id, ls_epoch))) {
    LOG_WARN("fail to delete file", K(ret), K(current_version_block_id), K(ls_epoch));
  }

  return ret;
}

int ObTenantStorageMetaPersister::ss_check_and_delete_tablet_current_version(
  const ObTabletID &tablet_id,
  const ObLSID &ls_id,
  const uint64_t ls_epoch,
  const int64_t deleted_tablet_version,
  const int64_t deleted_tablet_transfer_seq,
  ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;

  ObPrivateTabletCurrentVersion latest_addr;
  ObStorageObjectOpt current_version_opt;
  current_version_opt.set_ss_private_tablet_meta_current_verison_object_opt(ls_id.id(), tablet_id.id());
  if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(current_version_opt, allocator, MTL_ID(), ls_epoch, latest_addr))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("this tablet has been deleted and current_version has been deleted either",
        K(ret), K(ls_id), K(tablet_id), K(deleted_tablet_version), K(current_version_opt));
    } else {
      LOG_WARN("fail to read cur version",
        K(ret), K(ls_id), K(tablet_id), K(deleted_tablet_version), K(current_version_opt));
    }
  } else if (latest_addr.tablet_addr_.block_id().meta_transfer_seq() > deleted_tablet_transfer_seq) {
    // newer tablet_transfer_seq need not process
  } else if (latest_addr.tablet_addr_.block_id().meta_transfer_seq() < deleted_tablet_transfer_seq) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet transfer_seq in rebooting should not larger than curr_version", K(ret), K(deleted_tablet_transfer_seq), K(latest_addr));
  } else if (latest_addr.tablet_addr_.block_id().meta_version_id() > deleted_tablet_version) {
    // may be transfer_in tablet, need not delete current_version tablet;
  } else if (latest_addr.tablet_addr_.block_id().meta_version_id() < deleted_tablet_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet version in current_version is the last version of tablet, should equal to the version in pending_free_arr",
        K(ret), K(ls_id), K(tablet_id), K(deleted_tablet_version), K(latest_addr));
  } else {
    uint64_t retry_count = 0;
    while (OB_FAIL(ss_delete_tablet_current_version_(tablet_id, ls_id, ls_epoch))) {
      LOG_WARN("try delete tablet current version failed", K(ret), K(tablet_id), K(ls_id), K(ls_epoch), K(retry_count));
      retry_count++;
      usleep(1000 * 1000); // sleep 1s for each time
      if (retry_count > 60) { // 1min
        LOG_ERROR("failed to delete tablet current version", K(ret), K(tablet_id), K(ls_id), K(ls_epoch), K(retry_count));
        break;
      }
    }
  }

  return ret;
}

int ObTenantStorageMetaPersister::ss_remove_tablet_(
    const ObLSID ls_id, const int64_t ls_epoch,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;

  const common::ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
  const ObMetaDiskAddr &tablet_addr = tablet_handle.get_obj()->get_tablet_addr();
  lib::ObMemAttr attr(MTL_ID(), "PendingFreeInfo");
  PendingFreeTabletArrayKey key(ls_id, ls_epoch);
  PendingFreeTabletArrayInfo *array_info = nullptr;

  ObTabletCreateDeleteMdsUserData data;
  // create_empty_shell may delete old tablet whose process like transfer out
  GCTabletType gc_type = GCTabletType::TransferOut;
  uint64_t seq = 0;
  if (OB_FAIL(tablet_handle.get_obj()->get_tablet_status(share::SCN::max_scn(),
          data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      gc_type = GCTabletType::CreateAbort;
      STORAGE_LOG(INFO, "tablet_status is not commit", KR(ret), K(ls_id), K(tablet_id));
    } else {
      STORAGE_LOG(WARN, "failed to get CreateDeleteMdsUserData", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (ObTabletStatus::DELETED == data.tablet_status_) {
    gc_type = GCTabletType::DropTablet;
  }

  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(peding_free_map_lock_);
    if (OB_FAIL(pending_free_tablet_arr_map_.get_refactored(key, array_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        array_info = nullptr;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(array_info)) {
      if (OB_ISNULL(array_info = OB_NEW(PendingFreeTabletArrayInfo, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem", K(ret), K(key));
      } else if (OB_FAIL(pending_free_tablet_arr_map_.set_refactored(key, array_info))) {
        OB_DELETE(PendingFreeTabletArrayInfo, attr, array_info);
        LOG_WARN("fail to set pending free tablet array info", K(ret), K(key));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObTenantFileManager *file_manager = nullptr;
    MacroBlockId current_version_block_id;
    ObStorageObjectOpt opt;
    opt.set_ss_private_tablet_meta_current_verison_object_opt(ls_id.id(), tablet_id.id());

    ObLSPendingFreeTabletArray tmp_array;
    lib::ObMutexGuard guard(array_info->lock_);
    const int64_t curr_t = ObTimeUtility::fast_current_time();
    const ObPendingFreeTabletItem tablet_item(tablet_id, tablet_addr.block_id().meta_version_id(),
        ObPendingFreeTabletStatus::WAIT_GC, curr_t, gc_type, tablet_addr.block_id().meta_transfer_seq());
    if (OB_FAIL(tmp_array.assign(array_info->pending_free_tablet_arr_))) {
      LOG_WARN("fail to assign pending free tablet array", K(ret));
    } else if (has_exist_in_array(tmp_array.items_, tablet_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet_item has existed in pending free tablet arr", K(ret), K(tmp_array), K(tablet_item));
    } else if (OB_FAIL(tmp_array.items_.push_back(tablet_item))) {
      LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
    } else if (OB_FAIL(ss_write_pending_free_tablet_array_(ls_id, ls_epoch, tmp_array))) {
      LOG_WARN("fail to write_pending_free_tablet_array", K(ret), K(ls_id), K(ls_epoch), K(tablet_item));
    } else if (OB_FAIL(array_info->pending_free_tablet_arr_.items_.push_back(tablet_item))) {
      LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
    } else {
      uint64_t retry_count = 0;
      // make sure delete current_version after updating pending_free_talet_arr
      while (OB_FAIL(ss_delete_tablet_current_version_(tablet_id, ls_id, ls_epoch))) {
        // Q: Why we delete current_version after writing pending_free_tablet_arr ?
        // A: 1. If we deleted current_version firstly,
        //       the deleted tablet may be replay during reboot (pending_free_tablet_arr may not be written down).
        //       When loading the tablet, the current version is not exist, which had been deleted;
        //    2. The active_tablet_arr only record the tablets persisted.
        //       Though we write pending_free_tablet_arr firstly, the un-deleted current_version do no affect to the transfer-in tablet.
        LOG_WARN("failed to delete tablet current version", K(ret), K(tablet_id), K(ls_id), K(ls_epoch), K(retry_count));
        retry_count++;
        usleep(2 * 1000);
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaPersister::create_tenant_ls_item_(
    const ObLSID ls_id, int64_t &ls_epoch)
{
  int ret = OB_SUCCESS;
  uint64_t inf_seq = 0;
  // have to get macro_seq before get sputer_block_lock
  // update preallocate.. need the lock either
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(inf_seq))) {
    LOG_WARN("fail to get tenant_object_seq", K(ret));
  } else {
    lib::ObMutexGuard guard(super_block_lock_);
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    ObTenantSuperBlock tenant_super_block = tenant->get_super_block();

    int64_t i = 0;
    for (; i < tenant_super_block.ls_cnt_; i++) {
      const ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      if (ls_id == item.ls_id_ && item.status_ != ObLSItemStatus::CREATE_ABORT &&
          item.status_ != ObLSItemStatus::DELETED) {
        break;
      }
    }
    if (OB_UNLIKELY(i != tenant_super_block.ls_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls item already exist", K(ret), "ls_item", tenant_super_block.ls_item_arr_[i]);
    } else if (OB_UNLIKELY(ObTenantSuperBlock::MAX_LS_COUNT == i)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("too many ls", K(ret), K(ls_id), K(tenant_super_block));
    } else {
      ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      tenant_super_block.ls_cnt_ = i + 1;
      item.ls_id_ = ls_id;
      item.min_macro_seq_ = inf_seq;
      item.max_macro_seq_ = UINT64_MAX;
      item.status_ = ObLSItemStatus::CREATING;
      item.epoch_ = tenant_super_block.auto_inc_ls_epoch_++;
      ls_epoch = item.epoch_;
      if (!item.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected new ls_item", K(ret), K(item));
      } else if (OB_FAIL(ss_write_tenant_super_block_(tenant_super_block))) {
        LOG_WARN("fail to write tenant super block", K(ret), K(tenant_super_block));
      } else {
        tenant->set_tenant_super_block(tenant_super_block);
        FLOG_INFO("create tenant ls item", K(ret), K(item), K(tenant_super_block), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    FLOG_INFO("create tenant ls item failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObTenantStorageMetaPersister::update_tenant_ls_item_(
    const ObLSID ls_id, const int64_t ls_epoch, const ObLSItemStatus status)
{
  int ret = OB_SUCCESS;

  uint64_t sup_seq = 0;
  if (ObLSItemStatus::DELETED == status) {
    // have to get macro_seq before get sputer_block_lock
    // update preallocate.. need the lock either
    if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(sup_seq))) {
      LOG_WARN("fail to get tenant_object_seq", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    lib::ObMutexGuard guard(super_block_lock_);
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    ObTenantSuperBlock tenant_super_block = tenant->get_super_block();
    int64_t i = 0;
    for (; i < tenant_super_block.ls_cnt_; i++) {
      const ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      if (ls_id == item.ls_id_ && ls_epoch == item.epoch_) {
        break;
      }
    }
    if (OB_UNLIKELY(i == tenant_super_block.ls_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls not exist", K(ret), K(ls_id), K(ls_epoch), K(status));
    } else {
      const ObLSItem old_item = tenant_super_block.ls_item_arr_[i];
      ObLSItem &new_item = tenant_super_block.ls_item_arr_[i];
      new_item.status_ = status;
      if (ObLSItemStatus::DELETED == status) {
        // update the supremum seq of the deleted_ls_item
        new_item.max_macro_seq_ = sup_seq;
      }
      if (!new_item.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected new ls_item", K(ret), K(new_item), K(old_item));
      } else if (OB_FAIL(ss_write_tenant_super_block_(tenant_super_block))) {
        LOG_WARN("fail to write tenant super block", K(ret), K(tenant_super_block));
      } else {
        tenant->set_tenant_super_block(tenant_super_block);
        FLOG_INFO("update tenant super block ls item", K(ret), K(old_item), K(new_item), K(tenant_super_block), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    FLOG_INFO("update tenant ls item failed", K(ret), K(ls_id), K(ls_epoch), K(status));
  }
  return ret;
}


int ObTenantStorageMetaPersister::delete_tenant_ls_item_(
  const share::ObLSID ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(super_block_lock_);
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
  ObTenantSuperBlock tenant_super_block = tenant->get_super_block();

  HEAP_VAR(ObTenantSuperBlock, tmp_super_block) {
    tmp_super_block = tenant_super_block;
    tmp_super_block.ls_cnt_ = 0;
    bool is_delete_hit = false;
    for (int64_t i = 0; i < tenant_super_block.ls_cnt_; i++) {
      const ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      if (ls_id == item.ls_id_ && ls_epoch == item.epoch_) {
        if (ObLSItemStatus::DELETED == item.status_
            || ObLSItemStatus::CREATE_ABORT == item.status_) {
          is_delete_hit = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("delete ls_item whose status is not equal to deleted", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block), K(tmp_super_block));
        }
      } else {
        tmp_super_block.ls_item_arr_[tmp_super_block.ls_cnt_++] = item;
      }
    }
    if (OB_FAIL(ret)) {
      // error occurred
    } else if (OB_LIKELY(is_delete_hit)) {
      if (OB_FAIL(ss_write_tenant_super_block_(tmp_super_block))) {
        LOG_WARN("fail to write tenant super block", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block), K(tmp_super_block));
      } else {
        tenant->set_tenant_super_block(tmp_super_block);
        FLOG_INFO("update tenant super block ls item (delete)", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block), K(tmp_super_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls not exist", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block));
    }
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_write_tenant_super_block_(
    const ObTenantSuperBlock &tenant_super_block)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_epoch = MTL_EPOCH_ID();
  ObStorageObjectOpt opt;
  opt.set_ss_tenant_level_meta_object_opt(
      ObStorageObjectType::TENANT_SUPER_BLOCK, tenant_super_block.tenant_id_, tenant_epoch);
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, tenant_super_block, allocator_, MTL_ID(), 0/*ls_epoch*/))) {
    LOG_WARN("fail to write tenant super block", K(ret), K(tenant_epoch), K(tenant_super_block));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_write_ls_meta_(const int64_t ls_epoch, const ObLSMeta &meta)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_META, meta.ls_id_.id());
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, meta, allocator_, MTL_ID(), ls_epoch))) {
    LOG_WARN("fail to write ls meta", K(ret), K(ls_epoch), K(meta));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_write_dup_table_meta_(
    const int64_t ls_epoch,
    const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &meta)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_DUP_TABLE_META, meta.ls_id_.id());
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, meta, allocator_, MTL_ID(), ls_epoch))) {
    LOG_WARN("fail to write dup table meta", K(ret), K(ls_epoch), K(meta));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_write_active_tablet_array_(
    const ObLSID ls_id, const int64_t ls_epoch,
    const ObLSActiveTabletArray &active_tablet_array)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_ACTIVE_TABLET_ARRAY, ls_id.id());
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, active_tablet_array, allocator_, MTL_ID(), ls_epoch))) {
    LOG_WARN("fail to write active tablet array", K(ret), K(ls_id), K(ls_epoch), K(active_tablet_array));
  }
  return ret;
}


int ObTenantStorageMetaPersister::ss_write_pending_free_tablet_array_(
     const ObLSID ls_id, const int64_t ls_epoch, const ObLSPendingFreeTabletArray &pending_free_arry)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_PENDING_FREE_TABLET_ARRAY, ls_id.id());
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, pending_free_arry, allocator_, MTL_ID(), ls_epoch))) {
    LOG_WARN("fail to write pending_free_tablet_array", K(ret), K(ls_id), K(ls_epoch));
  }
  return ret;
}

int ObTenantStorageMetaPersister::ss_batch_remove_ls_tablets(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObIArray<common::ObTabletID> &tablet_id_arr,
    const ObIArray<ObMetaDiskAddr> &tablet_addr_arr,
    const bool delete_current_version)
{
  int ret = OB_SUCCESS;

  /* 0. argument check */
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || ls_epoch < 0 || tablet_id_arr.count() != tablet_addr_arr.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(ls_epoch), K(tablet_id_arr), K(tablet_addr_arr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_arr.count(); ++i) {
      if (!tablet_id_arr.at(i).is_valid() || !tablet_addr_arr.at(i).is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(ls_id), K(ls_epoch), K(i), K(tablet_id_arr), K(tablet_addr_arr));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // error occurred
  } else {
    const lib::ObMemAttr attr(MTL_ID(), "PendingFreeInfo");
    PendingFreeTabletArrayKey key(ls_id, ls_epoch);
    PendingFreeTabletArrayInfo *array_info = nullptr;

    { /* 1. get pending_free_tablet_arr or create it */
      lib::ObMutexGuard guard(peding_free_map_lock_);
      if (OB_FAIL(pending_free_tablet_arr_map_.get_refactored(key, array_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          array_info = nullptr;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(array_info)) {
        if (OB_ISNULL(array_info = OB_NEW(PendingFreeTabletArrayInfo, attr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", K(ret), K(key));
        } else if (OB_FAIL(pending_free_tablet_arr_map_.set_refactored(key, array_info))) {
          OB_DELETE(PendingFreeTabletArrayInfo, attr, array_info);
          LOG_WARN("fail to set pending free tablet array info", K(ret), K(key));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObLSPendingFreeTabletArray tmp_pending_free_array;
      lib::ObMutexGuard guard(array_info->lock_);
      if (OB_FAIL(tmp_pending_free_array.assign(array_info->pending_free_tablet_arr_))) {
        LOG_WARN("fail to assign pending free tablet array", K(ret));
      }

      if (OB_SUCC(ret)){
        /* 2. construct new pending_free_table_arr (tmp_pending_free_array) */
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_arr.count(); i++) {
          common::ObTabletID tablet_id = tablet_id_arr.at(i);
          ObMetaDiskAddr tablet_addr = tablet_addr_arr.at(i);

          ObPendingFreeTabletItem tablet_item(tablet_id, tablet_addr.block_id().meta_version_id(),
              ObPendingFreeTabletStatus::WAIT_GC, INT64_MAX /* delete_time */ , GCTabletType::DropLS,
              tablet_addr.block_id().meta_transfer_seq());
          if (has_exist_in_array(tmp_pending_free_array.items_, tablet_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet_item has existed in pending free tablet arr", K(ret), K(tmp_pending_free_array), K(tablet_item));
          } else if (OB_FAIL(tmp_pending_free_array.items_.push_back(tablet_item))) {
            LOG_WARN("fail to push back tablet item", K(ret), K(tablet_item));
          }
        }
        /*
          3. persist new pending_free_tablet_arr, and update array_info.
            Avoiding seperated IO, do not delete current_version for each tablet in there, which will be deleted later.
        */
        if (OB_FAIL(ret)) {
          // error occurred
        } else if (OB_FAIL(ss_write_pending_free_tablet_array_(ls_id, ls_epoch, tmp_pending_free_array))) {
          LOG_WARN("fail to write_pending_free_tablet_array", K(ret), K(ls_id), K(ls_epoch), K(tmp_pending_free_array));
        } else if (OB_FAIL(array_info->pending_free_tablet_arr_.items_.assign(tmp_pending_free_array.items_))) {
          LOG_WARN("fail to push back tablet item", K(ret), K(tmp_pending_free_array));
        } else if (delete_current_version) {
          /*
            if delete_current_version is true,
            make sure delete current_version after updating pending_free_talet_arr
          */
          for (int64_t i = 0; i < tablet_id_arr.count(); i++) {
            common::ObTabletID tablet_id = tablet_id_arr.at(i);
            uint64_t retry_count = 0;
            while (OB_FAIL(ss_delete_tablet_current_version_(tablet_id, ls_id, ls_epoch))) {
              LOG_WARN("fail to delete tablet_current_version", K(ret), K(tablet_id), K(ls_id), K(ls_epoch), K(retry_count));
              retry_count++;
              usleep(2 * 1000);
            }
          } // end for
        }// end persist
      }
    }
  }

  return ret;
}

int ObTenantStorageMetaPersister::ss_replay_ls_pending_free_arr(
  ObArenaAllocator &allocator,
  const ObLSID &ls_id,
  const uint64_t ls_epoch)
{
  int ret = OB_SUCCESS;

  ObStorageObjectOpt deleting_opt;
  ObLSPendingFreeTabletArray deleting_tablets;
  PendingFreeTabletArrayInfo* info = nullptr;
  const PendingFreeTabletArrayKey key(ls_id, ls_epoch);

  deleting_opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_PENDING_FREE_TABLET_ARRAY, ls_id.id());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || ls_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(ls_epoch));
  } else if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
        deleting_opt, allocator, MTL_ID(), ls_epoch, deleting_tablets))) {
    LOG_WARN("fail to get deleting tablets", K(ret), K(deleting_opt), K(ls_id), K(ls_epoch));
  } else if (OB_FAIL(pending_free_tablet_arr_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      info = nullptr;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pending free tablet array info", K(ret), K(key));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in rebooting, pending_arr_tablet_info should be nullptr", K(key));
  }

  if (OB_FAIL(ret)) {
  } else {
    const lib::ObMemAttr attr(MTL_ID(), "PendingFreeInfo");
    lib::ObMutexGuard guard(peding_free_map_lock_);
    if (OB_ISNULL(info = OB_NEW(PendingFreeTabletArrayInfo, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(ret), K(key));
    } else if (OB_FAIL(info->pending_free_tablet_arr_.assign(deleting_tablets))) {
      OB_DELETE(PendingFreeTabletArrayInfo, attr, info);
      LOG_WARN("fail to assign pending_free_arr", K(ret), K(key), K(deleting_tablets), K(info->pending_free_tablet_arr_));
    } else if (OB_FAIL(pending_free_tablet_arr_map_.set_refactored(key, info))) {
      OB_DELETE(PendingFreeTabletArrayInfo, attr, info);
      LOG_WARN("fail to set pending free tablet array info", K(ret), K(key));
    }
  }

  return ret;
}

#endif


} // namespace storage
} // namespace oceanbase
