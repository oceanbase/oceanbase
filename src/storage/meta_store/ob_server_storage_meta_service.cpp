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

#include "observer/omt/ob_tenant.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
namespace storage
{

ObServerStorageMetaService &ObServerStorageMetaService::get_instance()
{
  static ObServerStorageMetaService instance_;
  return instance_;
}
ObServerStorageMetaService::ObServerStorageMetaService()
  : is_inited_(false),
    is_started_(false),
    is_shared_storage_(false),
    slogger_mgr_(),
    server_slogger_(nullptr),
    ckpt_slog_handler_() {}

int ObServerStorageMetaService::init(const bool is_shared_storage)
{
  int ret = OB_SUCCESS;
  const char *data_dir = OB_FILE_SYSTEM_ROUTER.get_sstable_dir();
  const char *log_dir = is_shared_storage ? data_dir : OB_FILE_SYSTEM_ROUTER.get_slog_dir();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else if (OB_FAIL(slogger_mgr_.init(log_dir, data_dir, ObLogConstants::MAX_LOG_FILE_SIZE, OB_FILE_SYSTEM_ROUTER.get_slog_file_spec()))) {
    LOG_WARN("fail to init slogger manager", K(ret));
  } else if (OB_FAIL(slogger_mgr_.get_server_slogger(server_slogger_))) {
    LOG_WARN("fail to get server slogger", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.init(server_slogger_))) {
    LOG_WARN("fail to init server checkpoint slog hander", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    is_inited_ = true;
  }
  return ret;
}

int ObServerStorageMetaService::start()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(slogger_mgr_.start())) {
    LOG_WARN("fail to start slogger mgr", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.start())) {
    LOG_WARN("fail to start replay", K(ret));
  } else if (!is_shared_storage_ && OB_FAIL(OB_SERVER_BLOCK_MGR.first_mark_device())) { // mark must after finish replay slog
    LOG_WARN("fail to first mark device", K(ret));
  } else if (OB_FAIL(try_write_checkpoint_for_compat())) {
    LOG_WARN("fail to try write checkpoint for compat", K(ret));
  } else if (OB_FAIL(start_complete_and_online_ls())) {
    LOG_ERROR("fail to start complete and online ls", KR(ret));
  } else {
    ATOMIC_STORE(&is_started_, true);
  }
  const int64_t cost_time_us = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("finish start server storage meta service", K(ret), K(cost_time_us));
  return ret;
}

void ObServerStorageMetaService::stop()
{
  if (IS_INIT) {
    if (!is_shared_storage_) {
      slogger_mgr_.stop();
      ckpt_slog_handler_.stop();
    }
  }
}
void ObServerStorageMetaService::wait()
{
  if (IS_INIT) {
    if (!is_shared_storage_) {
      slogger_mgr_.wait();
      ckpt_slog_handler_.wait();
    }
  }
}
void ObServerStorageMetaService::destroy()
{
  slogger_mgr_.destroy();
  server_slogger_ = nullptr;
  ckpt_slog_handler_.destroy();
  is_inited_ = false;
}

int ObServerStorageMetaService::get_meta_block_list(
    ObIArray<blocksstable::MacroBlockId> &meta_block_list) const
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

int ObServerStorageMetaService::get_reserved_size(int64_t &reserved_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(slogger_mgr_.get_reserved_size(reserved_size))) {
      LOG_WARN("fail to get reserved size", K(ret));
    }
  } else {
    reserved_size = 0;
  }
  return ret;
}

int ObServerStorageMetaService::get_server_slogger(ObStorageLogger *&slogger) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    slogger = server_slogger_;
  } else {
    slogger = nullptr;
  }
  return ret;
}

int ObServerStorageMetaService::prepare_create_tenant(const omt::ObTenantMeta &meta, int64_t &epoch)
{
  int ret = OB_SUCCESS;
  epoch = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_STORAGE_OBJECT_MGR.alloc_tenant_epoch(meta.unit_.tenant_id_, epoch))  {
    LOG_WARN("fail to allocate tenant epoch", K(ret), K(meta.unit_.tenant_id_));
#endif
  } else {
    ObStorageLogParam log_param;
    const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
        ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_PREPARE);
    omt::ObTenantMeta tmp_meta = meta;
    tmp_meta.epoch_ = epoch;
    ObCreateTenantPrepareLog log_entry(tmp_meta);
    log_param.data_ = &log_entry;
    log_param.cmd_ = cmd;
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("failed to write put tenant slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObServerStorageMetaService::commit_create_tenant(
    const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObStorageLogParam log_param;
    const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
        ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_COMMIT);
    uint64_t tid = tenant_id;
    ObCreateTenantCommitLog log_entry(tid);
    log_param.data_ = &log_entry;
    log_param.cmd_ = cmd;
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("failed to write slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObServerStorageMetaService::abort_create_tenant(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(OB_STORAGE_OBJECT_MGR.create_super_block_tenant_item(
          tenant_id, epoch, ObTenantCreateStatus::CREATE_ABORT))) {
    LOG_WARN("fail to create super block tenant item", K(ret), K(tenant_id), K(epoch));
#endif
  } else {
    ObStorageLogParam log_param;
    const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
        ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_ABORT);
    uint64_t tid = tenant_id;
    ObCreateTenantAbortLog log_entry(tid);
    log_param.data_ = &log_entry;
    log_param.cmd_ = cmd;
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("failed to write slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObServerStorageMetaService::prepare_delete_tenant(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObStorageLogParam log_param;
    const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
        ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_PREPARE);
    uint64_t tid = tenant_id;
    ObDeleteTenantPrepareLog log_entry(tid);
    log_param.data_ = &log_entry;
    log_param.cmd_ = cmd;
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("failed to write slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObServerStorageMetaService::commit_delete_tenant(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(OB_STORAGE_OBJECT_MGR.create_super_block_tenant_item(
          tenant_id, epoch, ObTenantCreateStatus::DELETED))) {
    LOG_WARN("fail to create super block tenant item", K(ret), K(tenant_id), K(epoch));
#endif
  } else {
    ObStorageLogParam log_param;
    const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
        ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_COMMIT);
    uint64_t tid = tenant_id;
    ObDeleteTenantCommitLog log_entry(tid);
    log_param.data_ = &log_entry;
    log_param.cmd_ = cmd;
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("failed to write slog", K(ret), K(log_param));
    }
  }
  return ret;
}

// Concurrency security is guaranteed by the ObMultiTenant,
// although ObTenantStorageMetaPerister also update the tenant super block,
// but it must they must occur after this, so it don't need a lock here.
int ObServerStorageMetaService::update_tenant_super_block(
    const int64_t tenant_epoch, const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(super_block));
  } else {
    ObUpdateTenantSuperBlockLog slog_entry(*const_cast<ObTenantSuperBlock*>(&super_block));
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_SUPER_BLOCK);
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("fail to write tenant super block slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObServerStorageMetaService::update_tenant_unit(
    const int64_t tenant_epoch, const ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObStorageLogParam log_param;
    int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
        ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_UNIT);
    ObUpdateTenantUnitLog log_entry(*const_cast<ObUnitInfoGetter::ObTenantConfig*>(&unit));
    log_param.data_ = &log_entry;
    log_param.cmd_ = cmd;
    if (OB_FAIL(server_slogger_->write_log(log_param))) {
      LOG_WARN("failed to write tenant unit slog", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObServerStorageMetaService::clear_tenant_log_dir(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_clog_dir[MAX_PATH_SIZE] = {0};
  char tenant_slog_dir[MAX_PATH_SIZE] = {0};
  bool exist = true;

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  // in logservice mode, we don't need to clean tenant clog dir
  // cause there is no clog dir in ob server`s disk
  if (GCONF.enable_logservice) { // do nothing
  } else
#endif
  if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.get_tenant_clog_dir(tenant_id, tenant_clog_dir))) {
    LOG_WARN("fail to get tenant clog dir", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_clog_dir, exist))) {
    LOG_WARN("fail to check exist", K(ret));
  } else if (exist) {
    // defense code begin
    int tmp_ret = OB_SUCCESS;
    bool directory_empty = true;
    if (OB_TMP_FAIL(FileDirectoryUtils::is_empty_directory(tenant_clog_dir, directory_empty))) {
      LOG_WARN("fail to check directory whether is empty", KR(tmp_ret), K(tenant_clog_dir));
    }
    if (!directory_empty) {
      LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "clog directory must be empty when delete tenant", K(tenant_clog_dir));
    }
    // defense code end
    if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(tenant_clog_dir))) {
      LOG_WARN("fail to delete clog dir", K(ret), K(tenant_clog_dir));
    }
  }

  if (OB_SUCC(ret) && !is_shared_storage_) {
    if (OB_FAIL(slogger_mgr_.get_tenant_slog_dir(tenant_id, tenant_slog_dir))) {
      LOG_WARN("fail to get tenant slog dir", K(ret));
    } else if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_slog_dir, exist))) {
      LOG_WARN("fail to check exist", K(ret));
    } else if (exist) {
      if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(tenant_slog_dir))) {
        LOG_WARN("fail to delete slog dir", K(ret), K(tenant_slog_dir));
      }
    }
  }
  return ret;
}


int ObServerStorageMetaService::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.write_checkpoint(is_force))) {
    LOG_WARN("fail to write checkpoint", K(ret));
  }
  return ret;
}

int ObServerStorageMetaService::ObTenantItemIterator::init()
{
  OB_STORAGE_OBJECT_MGR.get_server_super_block_by_copy(server_super_block_);
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObServerStorageMetaService::ObTenantItemIterator::get_next_tenant_item(
      storage::ObTenantItem &item)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (idx_ == server_super_block_.body_.tenant_cnt_) {
    ret = OB_ITER_END;
  } else {
    item = server_super_block_.body_.tenant_item_arr_[idx_++];
  }
  return ret;
}

int ObServerStorageMetaService::get_tenant_items_by_status(
    const storage::ObTenantCreateStatus status,
    ObIArray<storage::ObTenantItem> &tenant_items/*OUT*/) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (storage::ObTenantCreateStatus::MAX == status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    HEAP_VAR(ObTenantItemIterator, tenant_item_iter) {
      tenant_item_iter.init();
      tenant_items.reuse();
      ObTenantItem tenant_item;
      while (OB_SUCC(tenant_item_iter.get_next_tenant_item(tenant_item))) {
        if (tenant_item.status_ == status &&
            OB_FAIL(tenant_items.push_back(tenant_item))) {
          LOG_WARN("failed to push back tenant_item", K(ret), K(tenant_item), K(tenant_items));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tenant items by status", K(ret), K(tenant_item), K(tenant_items));
      }
    }
  }
  return ret;
}

int ObServerStorageMetaService::try_write_checkpoint_for_compat()
{
  int ret = OB_SUCCESS;
  common::ObArray<omt::ObTenantMeta> tenant_metas;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, omt is nullptr", K(ret));
  } else if (OB_FAIL(omt->get_tenant_metas_for_ckpt(tenant_metas))) {
    LOG_WARN("fail to get tenant metas", K(ret), KP(omt));
  } else {
    bool need_svr_ckpt = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_metas.size(); ++i) {
      ObTenantSuperBlock &super_block = tenant_metas.at(i).super_block_;
      if (!super_block.is_old_version()) {
        // nothing to do.
      } else {
        MTL_SWITCH(super_block.tenant_id_) {
          if (OB_FAIL(MTL(ObTenantStorageMetaService*)->write_checkpoint(ObTenantSlogCheckpointWorkflow::COMPAT_UPGRADE))) {
            LOG_WARN("fail to write tenant slog checkpoint", K(ret));
          } else {
            // we don't write checkpoint or update super_block for hidden tenant
            // so it is necessary to update version here
            if (super_block.is_hidden_) {
              super_block.version_ = ObTenantSuperBlock::TENANT_SUPER_BLOCK_VERSION;
              omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
              tenant->set_tenant_super_block(super_block);
            }
            need_svr_ckpt = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (need_svr_ckpt && OB_FAIL(ckpt_slog_handler_.write_checkpoint(true/*is_force*/))) {
        LOG_WARN("fail to write server checkpoint", K(ret));
      }
    }
  }
  return ret;
}

int ObServerStorageMetaService::start_complete_and_online_ls() const
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, omt is nullptr", K(ret));
  } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
    const uint64_t &tenant_id = tenant_ids.at(i);
    MTL_SWITCH(tenant_id) {
      common::ObSharedGuard<ObLSIterator> ls_iter;
      ObLS *ls = nullptr;
      ObLSTabletService *ls_tablet_svr = nullptr;
      if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("failed to get ls iter", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(ls_iter->get_next(ls))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next ls", K(ret));
            }
          } else if (nullptr == ls) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls is null", K(ret));
          } else if (OB_FAIL(ls->finish_storage_meta_replay())) {
            LOG_WARN("finish replay failed", K(ret), KPC(ls));
          }
        }
        if (OB_ITER_END == ret) {
          ObTransferService *transfer_service = nullptr;
          if (OB_FAIL(MTL(ObLSService*)->gc_ls_after_replay_slog())) {
            LOG_WARN("fail to gc ls after replay slog", K(ret));
          } else if (OB_FAIL(MTL(ObLSService*)->online_ls())) {
            LOG_WARN("fail enable replay clog", K(ret));
          } else if (OB_ISNULL(transfer_service = (MTL(ObTransferService *)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("transfer service should not be NULL", K(ret), KP(transfer_service));
          } else {
            transfer_service->wakeup();
          }
        }
      }
    }
  }
  FLOG_INFO("storage meta service start complete and enable replay clog", K(ret));
  return ret;
}

} // namespace storage
} // namespace oceanbase
