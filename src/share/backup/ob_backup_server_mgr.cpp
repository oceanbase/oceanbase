// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//         http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE
#include "ob_backup_server_mgr.h"

using namespace oceanbase;
using namespace share;
using namespace lib;

ObBackupServerMgr::ObBackupServerMgr()
  : is_inited_(false),
    mtx_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    server_op_(),
    unit_op_(),
    server_status_array_(),
    zone_array_()
{
}

int ObBackupServerMgr::init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit> units;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(server_op_.init(&sql_proxy))) {
    LOG_WARN("fail to init server op", K(ret));
  } else if (OB_FAIL(server_op_.get(server_status_array_))) {
    LOG_WARN("fail to get server status info", K(ret));
  } else if (OB_FAIL(unit_op_.init(sql_proxy))) {
    LOG_WARN("fail to init unit table operator", K(ret));
  } else if (OB_FAIL(unit_op_.get_units_by_tenant(tenant_id, units))) {
    LOG_WARN("fail to get units by tenant", K(ret), K(tenant_id));
  } else if (units.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units must not be empty", K(ret), K(tenant_id));
  } else {
    zone_array_.reset();
    ARRAY_FOREACH_X(units, i, cur, OB_SUCC(ret)) {
      const share::ObUnit &unit = units.at(i);
      if (OB_FAIL(zone_array_.push_back(unit.zone_))) {
        LOG_WARN("fail to push back zone", K(ret), K(unit));
      }
    }
    if (OB_SUCC(ret)) {
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupServerMgr::get_alive_servers(
    const bool force_update, const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (force_update) {
    ObMutexGuard guard(mtx_);
    if (OB_FAIL(update_server_())) {
      LOG_WARN("failed to update server", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObMutexGuard guard(mtx_);
    ARRAY_FOREACH(server_status_array_, i) {
      if (server_status_array_[i].zone_ == zone && server_status_array_.at(i).is_alive()) {
        if (OB_FAIL(server_list.push_back(server_status_array_[i].server_))) {
          LOG_WARN("push back to server_list failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupServerMgr::get_alive_servers(
    const bool force_update, common::ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (force_update) {
    ObMutexGuard guard(mtx_);
    if (OB_FAIL(update_zone_())) {
      LOG_WARN("failed to update zone", K(ret));
    } else if (OB_FAIL(update_server_())) {
      LOG_WARN("failed to update server", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObMutexGuard guard(mtx_);
    ARRAY_FOREACH(server_status_array_, i) {
      if (OB_FAIL(server_list.push_back(server_status_array_.at(i).server_))) {
        LOG_WARN("failed to push server server", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupServerMgr::get_zone_list(const bool force_update, ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (force_update) {
    ObMutexGuard guard(mtx_);
    if (OB_FAIL(update_zone_())) {
      LOG_WARN("failed to update zone", K(ret));
    }
  }

  ObMutexGuard guard(mtx_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append(zone_list, zone_array_))) {
    LOG_WARN("failed to append zone list", K(zone_array_));
  }
  return ret;
}

int ObBackupServerMgr::get_server_status(
    const common::ObAddr &server, const bool force_update, ObServerStatus &server_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (force_update) {
    ObMutexGuard guard(mtx_);
    if (OB_FAIL(update_server_())) {
      LOG_WARN("failed to update server", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObMutexGuard guard(mtx_);
    const ObServerStatus *status_ptr = nullptr;
    if (OB_FAIL(find_(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (OB_ISNULL(status_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      server_status = *status_ptr;
    }
  }
  return ret;
}

int ObBackupServerMgr::is_server_exist(const common::ObAddr &server, const bool force_update, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (force_update) {
    ObMutexGuard guard(mtx_);
    if (OB_FAIL(update_server_())) {
      LOG_WARN("failed update server", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObMutexGuard guard(mtx_);
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find_(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
      ret = OB_SUCCESS;
      exist = false;
    } else if (OB_ISNULL(status_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObBackupServerMgr::update_zone_()
{
  int ret = OB_SUCCESS;
  zone_array_.reset();
  common::ObArray<share::ObUnit> units;
  if (OB_FAIL(unit_op_.get_units_by_tenant(tenant_id_, units))) {
    LOG_WARN("fail to init unit table operator", K(ret), K(tenant_id_));
  } else if (units.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units must not be empty", K(ret), K(tenant_id_));
  }
  ARRAY_FOREACH_X(units, i, cur, OB_SUCC(ret)) {
    const share::ObUnit &unit = units.at(i);
    if (OB_FAIL(zone_array_.push_back(unit.zone_))) {
      LOG_WARN("fail to push back zone", K(ret), K(unit));
    }
  }
  return ret;
}

int ObBackupServerMgr::update_server_()
{
  int ret = OB_SUCCESS;
  server_status_array_.reset();
  if (OB_FAIL(server_op_.get(server_status_array_))) {
    LOG_WARN("failed to get server status array", K(ret));
  }
  return ret;
}

int ObBackupServerMgr::find_(const ObAddr &server, const ObServerStatus *&status) const
{
  int ret = OB_SUCCESS;
  status = NULL;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; i < server_status_array_.count() && !find; ++i) {
      if (server_status_array_[i].server_ == server) {
        status = &server_status_array_[i];
        find = true;
      }
    }
    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("server not exist", K(server), K(ret));
    }
  }
  return ret;
}