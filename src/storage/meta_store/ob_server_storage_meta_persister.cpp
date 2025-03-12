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

#include "ob_server_storage_meta_persister.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
using namespace omt;
using namespace blocksstable;
namespace storage
{

int ObServerStorageMetaPersister::init(const bool is_shared_storage, ObStorageLogger *server_slogger)
{
  int ret = OB_SUCCESS;
  const int64_t MEM_LIMIT = 512UL << 20;
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "SvrMetaPersist");

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else if (OB_FAIL(allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE, attr, MEM_LIMIT))) {
    LOG_WARN("fail to init fifo allocator", K(ret));
  } else {
    server_slogger_ = server_slogger;
    is_shared_storage_ = is_shared_storage;
    is_inited_ = true;
  }
  return ret;
}

void ObServerStorageMetaPersister::destroy()
{
  server_slogger_ = nullptr;
  allocator_.reset();
  is_inited_ = false;
}

int ObServerStorageMetaPersister::prepare_create_tenant(const ObTenantMeta &meta, int64_t &epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    epoch = 0;
    if (OB_FAIL(write_prepare_create_tenant_slog_(meta))) {
      LOG_WARN("fail to write prepare create tenant slog", K(ret), K(meta));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_prepare_create_tenant_(meta, epoch))) {
      LOG_WARN("fail to prepare create tenant", K(ret), K(meta));
    }
#endif
  }
  return ret;
}

int ObServerStorageMetaPersister::commit_create_tenant(
    const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_commit_create_tenant_slog_(tenant_id))) {
      LOG_WARN("fail to write commit create tenant slog", K(ret), K(tenant_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_commit_create_tenant_(tenant_id, epoch))) {
      LOG_WARN("fail to commit create tenant", K(ret), K(tenant_id), K(epoch));
    }
#endif
  }
  return ret;
}

int ObServerStorageMetaPersister::abort_create_tenant(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_abort_create_tenant_slog_(tenant_id))) {
      LOG_WARN("fail to write abort create tenant slog", K(ret), K(tenant_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_abort_create_tenant_(tenant_id, epoch))) {
      LOG_WARN("fail to abort create tenant", K(ret), K(tenant_id), K(epoch));
    }
#endif
  }
  return ret;
}

int ObServerStorageMetaPersister::prepare_delete_tenant(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_prepare_delete_tenant_slog_(tenant_id))) {
      LOG_WARN("fail to write prepare delete tenant slog", K(ret), K(tenant_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_prepare_delete_tenant_(tenant_id, epoch))) {
      LOG_WARN("fail to prepare delete tenant", K(ret), K(tenant_id), K(epoch));
    }
#endif
  }
  return ret;
}

int ObServerStorageMetaPersister::commit_delete_tenant(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_commit_delete_tenant_slog_(tenant_id))) {
      LOG_WARN("fail to write commit delete tenant slog", K(ret), K(tenant_id));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_commit_delete_tenant_(tenant_id, epoch))) {
      LOG_WARN("fail to commit delete tenant", K(ret), K(tenant_id), K(epoch));
    }
#endif
  }
  return ret;
}

// Concurrency security is guaranteed by the ObMultiTenant,
// although ObTenantStorageMetaPerister also update the tenant super block,
// but it must they must occur after this, so it don't need a lock here.
int ObServerStorageMetaPersister::update_tenant_super_block(
    const int64_t tenant_epoch, const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_update_tenant_super_block_slog_(super_block))) {
      LOG_WARN("fail to write update tenant super block slog", K(ret), K(super_block));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_write_tenant_super_block_(tenant_epoch, super_block))) {
      LOG_WARN("fail to wirte tenant super block", K(ret), K(super_block));
    }
#endif
  }
  return ret;
}

int ObServerStorageMetaPersister::update_tenant_unit(
    const int64_t tenant_epoch, const ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_)  {
    if (OB_FAIL(write_update_tenant_unit_slog_(unit))) {
      LOG_WARN("fail to write update tenant unit slog", K(ret), K(unit));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_write_unit_config_(tenant_epoch, unit))) {
      LOG_WARN("fail to wirte unit config", K(ret), K(unit));
    }
#endif
  }
  return ret;
}

int ObServerStorageMetaPersister::clear_tenant_log_dir(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_clog_dir[MAX_PATH_SIZE] = {0};
  char tenant_slog_dir[MAX_PATH_SIZE] = {0};
  bool exist = true;

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
    if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_slogger_manager().get_tenant_slog_dir(tenant_id, tenant_slog_dir))) {
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


int ObServerStorageMetaPersister::write_prepare_create_tenant_slog_(const ObTenantMeta &meta)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_PREPARE);
  ObCreateTenantPrepareLog log_entry(*const_cast<ObTenantMeta*>(&meta));
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write put tenant slog", K(ret), K(log_param));
  }

  return ret;
}

int ObServerStorageMetaPersister::write_commit_create_tenant_slog_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_COMMIT);
  ObCreateTenantCommitLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}
int ObServerStorageMetaPersister::write_abort_create_tenant_slog_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_ABORT);
  ObCreateTenantAbortLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}

int ObServerStorageMetaPersister::write_prepare_delete_tenant_slog_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_PREPARE);
  ObDeleteTenantPrepareLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}

int ObServerStorageMetaPersister::write_commit_delete_tenant_slog_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_COMMIT);
  ObDeleteTenantCommitLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}

int ObServerStorageMetaPersister::write_update_tenant_super_block_slog_(
    const ObTenantSuperBlock &super_block)
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

int ObServerStorageMetaPersister::write_update_tenant_unit_slog_(const ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_UNIT);
  ObUpdateTenantUnitLog log_entry(*const_cast<ObUnitInfoGetter::ObTenantConfig*>(&unit));
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write tenant unit slog", K(ret), K(log_param));
  }

  return ret;
}


#ifdef OB_BUILD_SHARED_STORAGE
int ObServerStorageMetaPersister::ss_prepare_create_tenant_(
    const ObTenantMeta &meta, int64_t &epoch)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = meta.unit_.tenant_id_;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.create_super_block_tenant_item(tenant_id, epoch))) {
    LOG_WARN("fail to create tenant item", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_FAIL(ss_write_tenant_super_block_(epoch, meta.super_block_)))) {
    LOG_WARN("fail to write tenant super block", K(ret), K(epoch), K(meta));
  } else if (OB_FAIL(ss_write_unit_config_(epoch, meta.unit_))) {
    LOG_WARN("fail to write unit config", K(ret), K(epoch), K(meta));
  }
  return ret;
}

int ObServerStorageMetaPersister::ss_write_tenant_super_block_(
    const int64_t tenant_epoch,
    const ObTenantSuperBlock &tenant_super_block)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_tenant_level_meta_object_opt(
      ObStorageObjectType::TENANT_SUPER_BLOCK, tenant_super_block.tenant_id_, tenant_epoch);
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, tenant_super_block, allocator_, OB_SERVER_TENANT_ID, 0/*ls_epoch*/))) {
    LOG_WARN("fail to write tenant super block", K(ret), K(tenant_epoch));
  }
  return ret;
}

int ObServerStorageMetaPersister::ss_write_unit_config_(
  const int64_t tenant_epoch,
  const share::ObUnitInfoGetter::ObTenantConfig &unit_config)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_tenant_level_meta_object_opt(
      ObStorageObjectType::TENANT_UNIT_META, unit_config.tenant_id_, tenant_epoch);
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, unit_config, allocator_, OB_SERVER_TENANT_ID, 0/*ls_epoch*/))) {
    LOG_WARN("fail to write tenant super block", K(ret), K(tenant_epoch));
  }
  return ret;
}

int ObServerStorageMetaPersister::ss_commit_create_tenant_(
    const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.update_super_block_tenant_item(
      tenant_id, epoch, ObTenantCreateStatus::CREATED))) {
    LOG_WARN("fail to update tenant item", K(ret), K(tenant_id));
  }
  return ret;
}

int ObServerStorageMetaPersister::ss_abort_create_tenant_(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.update_super_block_tenant_item(
      tenant_id, epoch, ObTenantCreateStatus::CREATE_ABORT))) {
    LOG_WARN("fail to update tenant item", K(ret), K(tenant_id), K(epoch));
  }
  return ret;
}

int ObServerStorageMetaPersister::ss_prepare_delete_tenant_(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.update_super_block_tenant_item(
      tenant_id, epoch, ObTenantCreateStatus::DELETING))) {
    LOG_WARN("fail to update tenant item", K(ret), K(tenant_id), K(epoch));
  }
  return ret;
}

int ObServerStorageMetaPersister::ss_commit_delete_tenant_(const uint64_t tenant_id, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.update_super_block_tenant_item(
      tenant_id, epoch, ObTenantCreateStatus::DELETED))) {
    LOG_WARN("fail to update tenant item", K(ret), K(tenant_id), K(epoch));
  }
  return ret;
}

#endif

} // namespace storage
} // namespace oceanbase
