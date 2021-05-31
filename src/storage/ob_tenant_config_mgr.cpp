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
#include "lib/oblog/ob_log_module.h"
#include "ob_tenant_config_mgr.h"
#include "blocksstable/slog/ob_storage_log_struct.h"
#include "storage/ob_partition_log.h"
#include "blocksstable/ob_store_file.h"
#include "observer/omt/ob_tenant_node_balancer.h"

using namespace oceanbase;
using namespace storage;
using namespace share;
using namespace blocksstable;
using namespace common;
using namespace lib;
using namespace memtable;

ObTenantConfigMgr::ObTenantConfigMgr()
    : is_inited_(false),
      tenant_units_(),
      mutex_(common::ObLatchIds::CONFIG_LOCK),
      lock_(common::ObLatchIds::CONFIG_LOCK)
{}

ObTenantConfigMgr::~ObTenantConfigMgr()
{
  destroy();
}

ObTenantConfigMgr& ObTenantConfigMgr::get_instance()
{
  static ObTenantConfigMgr instance;
  return instance;
}

int ObTenantConfigMgr::enable_write_log()
{
  int ret = OB_SUCCESS;
  LOG_INFO("enable write tenant config mgr log");
  if (OB_FAIL(ObIRedoModule::enable_write_log())) {
    LOG_WARN("Failed to enable write log", K(ret));
  } else if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().update_tenant(tenant_units_, true /*is_local*/))) {
    LOG_WARN("Failed to update tenant", K(ret), K(tenant_units_));
  } else {
    LOG_INFO("succ to update tenant", K_(tenant_units));
  }
  return ret;
}

int ObTenantConfigMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_TENANT_CONFIG, this))) {
    LOG_WARN("failed to register redo module", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObTenantConfigMgr::destroy()
{
  is_inited_ = false;
  enable_write_log_ = false;
  tenant_units_.destroy();
}

int ObTenantConfigMgr::write_tenant_units(const TenantUnits& tenant_units)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!enable_write_log_) {
    ret = OB_EAGAIN;
    LOG_INFO("not able to write slog now", K(ret));
  } else if (!is_tenant_changed(tenant_units_, tenant_units)) {
    LOG_INFO("tenant units not changed, no need to write", K(tenant_units));
  } else {
    {
      TCWLockGuard lock_guard(lock_);
      if (OB_FAIL(tenant_units_.reserve(tenant_units.count()))) {
        LOG_WARN("failed to reserve ", K(ret), K(tenant_units));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(SLOGGER.begin(OB_LOG_WRITE_TENANT_CONFIG))) {
        LOG_WARN("failed to begin slog", K(ret));
      } else {
        int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_TENANT_CONFIG, REDO_LOG_UPDATE_TENANT_CONFIG);
        ObUpdateTenantConfigLogEntry log_entry(*const_cast<TenantUnits*>(&tenant_units));
        const ObStorageLogAttribute log_attr(OB_SYS_TENANT_ID, OB_VIRTUAL_DATA_FILE_ID);
        int64_t lsn = 0;
        if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
          LOG_WARN("failed to write modify table store log", K(ret), K(log_entry));
        } else if (OB_FAIL(SLOGGER.commit(lsn))) {
          LOG_WARN("failed to commit slog", K(ret), K(lsn));
        } else {
          TCWLockGuard lock_guard(lock_);
          if (OB_FAIL(tenant_units_.assign(tenant_units))) {
            LOG_ERROR("failed to assign tenant units", K(ret), K(tenant_units));
            ob_abort();
          }
        }

        if (OB_FAIL(ret)) {
          if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
            LOG_WARN("create sstable logger abort error", K(tmp_ret));
          } else {
            LOG_WARN("failed to complete sstables", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// single thread, no need to hold lock
int ObTenantConfigMgr::load_tenant_units(const TenantUnits& tenant_units)
{
  int ret = OB_SUCCESS;
  tenant_units_.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(tenant_units_.reserve(tenant_units.count()))) {
    LOG_WARN("failed to reserve for tenant units", K(ret), K(tenant_units.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_units.count(); i++) {
      if (OB_FAIL(tenant_units_.push_back(tenant_units.at(i)))) {
        LOG_WARN("failed to push back tenant unit", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().update_tenant(tenant_units_, true /*is_local*/))) {
      LOG_WARN("Failed to update tenant", K(ret), K(tenant_units));
    } else {
      LOG_INFO("succ to update tenant info", K(tenant_units));
    }
  }
  return ret;
}

int ObTenantConfigMgr::get_tenant_units(TenantUnits& tenant_units)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    tenant_units.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_units_.count(); i++) {
      if (OB_FAIL(tenant_units.push_back(tenant_units_.at(i)))) {
        LOG_WARN("failed to push back tenant unit", K(ret));
      }
    }
  }
  return ret;
}

// Don't call this, please call ObCompatModeGetter::get_tenant_compat_mode
int ObTenantConfigMgr::get_compat_mode(const int64_t tenant_id, ObWorker::CompatMode& compat_mode)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  bool found = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; !found && i < tenant_units_.count(); ++i) {
      if (tenant_id == tenant_units_.at(i).tenant_id_) {
        compat_mode = tenant_units_.at(i).mode_;
        found = true;
      }
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

// single thread, no need to hold lock
int ObTenantConfigMgr::replay(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  const char* buf = param.buf_;
  const int64_t len = param.buf_len_;
  ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_REDO_LOG_TENANT_CONFIG != main_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
      case REDO_LOG_UPDATE_TENANT_CONFIG: {
        if (OB_FAIL(replay_update_tenant_config(buf, len))) {
          LOG_WARN("failed to replay create sstable", K(ret), K(param));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown subtype", K(ret), K(sub_type), K(param));
      }
    }
  }

  return ret;
}

int ObTenantConfigMgr::replay_update_tenant_config(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  TenantUnits tenant_units;
  int64_t pos = 0;
  ObUpdateTenantConfigLogEntry log_entry(tenant_units);
  tenant_units_.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("TenantConfigmgr is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(tenant_units_.reserve(tenant_units.count()))) {
    LOG_WARN("failed to reserve for tenant units", K(ret), K(tenant_units.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_units.count(); i++) {
      if (OB_FAIL(tenant_units_.push_back(tenant_units.at(i)))) {
        LOG_WARN("failed to push back tenant unit", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succeed to replay update tenant config", K_(tenant_units));
      if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().update_tenant(tenant_units_, true /*is_local*/))) {
        LOG_WARN("Failed to update tenant", K(ret), K_(tenant_units));
      } else {
        LOG_INFO("succ to update tenant info", K(tenant_units));
      }
    }
  }
  return ret;
}

int ObTenantConfigMgr::parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRedoLogMainType main_type = OB_REDO_LOG_TABLE_MGR;
  int32_t sub_type = 0;

  ObIRedoModule::parse_subcmd(subcmd, main_type, sub_type);  // this func has no ret
  if (NULL == buf || len <= 0 || NULL == stream) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(buf), K(len), K(stream));
  } else if (OB_REDO_LOG_TENANT_CONFIG != main_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
      case REDO_LOG_UPDATE_TENANT_CONFIG: {
        TenantUnits tenant_units;
        ObUpdateTenantConfigLogEntry entry(tenant_units);
        if (OB_FAIL(entry.deserialize(buf, len, pos))) {
          LOG_WARN("Fail to deserialize update tenant config log entry, ", K(ret));
        } else if (0 > fprintf(stream, "update tenant config.\n%s\n", to_cstring(entry))) {
          ret = OB_IO_ERROR;
          LOG_WARN("failed to update tenant config log", K(ret), K(entry));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown subtype", K(ret), K(sub_type));
      }
    }
  }

  return OB_SUCCESS;
}

bool ObTenantConfigMgr::is_tenant_changed(const TenantUnits& old_tenants, const TenantUnits& new_tenants)
{
  bool is_changed = false;
  int64_t index = 0;
  // omt_->set_synced() needs to be called, even when both are 0
  if (old_tenants.count() != new_tenants.count() || (0 == new_tenants.count() && 0 == old_tenants.count())) {
    is_changed = true;
  } else {
    while (index < old_tenants.count()) {
      if (old_tenants.at(index) == new_tenants.at(index)) {
        index++;
      } else {
        is_changed = true;
        break;
      }
    }
  }
  return is_changed;
}
