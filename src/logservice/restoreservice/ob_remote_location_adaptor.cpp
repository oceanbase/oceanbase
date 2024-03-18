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

#define USING_LOG_PREFIX CLOG
#include "lib/utility/ob_macro_utils.h"
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"         // ObTimeUtility
#include "storage/tx_storage/ob_ls_map.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_ls_id.h"                     // ObLSID
#include "common/ob_role.h"                 // ObRole
#include "logservice/palf_handle_guard.h"   // PalfHandleGuard
#include "share/scn.h"   // SCN
#include "share/restore/ob_ls_restore_status.h" // ObLSRestoreStatus
#include "share/restore/ob_log_restore_source.h"  // ObLogRestoreSourceItem
#include "share/restore/ob_log_restore_source_mgr.h"  // ObLogRestoreSourceMgr
#include "share/rc/ob_tenant_base.h"                  // ObTenantRole
#include "storage/ls/ob_ls.h"                   // ObLS
#include "storage/tx_storage/ob_ls_service.h"   // ObLSService
#include "ob_remote_location_adaptor.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::storage;
ObRemoteLocationAdaptor::ObRemoteLocationAdaptor() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  last_refresh_ts_(OB_INVALID_TIMESTAMP),
  ls_svr_(NULL)
{}

ObRemoteLocationAdaptor::~ObRemoteLocationAdaptor()
{
  destroy();
}

int ObRemoteLocationAdaptor::init(const uint64_t tenant_id, ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRemoteLocationAdaptor init twice", K(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_svr));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    last_refresh_ts_ = common::ObTimeUtility::fast_current_time();
    inited_ = true;
  }
  return ret;
}

void ObRemoteLocationAdaptor::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  last_refresh_ts_ = OB_INVALID_TIMESTAMP;
  ls_svr_ = NULL;
}

int ObRemoteLocationAdaptor::update_upstream(share::ObLogRestoreSourceItem &source, bool &source_exist)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  const int64_t cur_ts = common::ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRemoteLocationAdaptor not init", K(ret));
  } else if (! is_user_tenant(tenant_id_)) {
    // not user tenant, just skip
  } else if (is_tenant_primary_()) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      LOG_INFO("primary tenant, just skip");
    }
  } else if (OB_FAIL(get_source_(source, source_exist))) {
    LOG_WARN("get source failed", K_(tenant_id));
  } else if (OB_FAIL(do_update_(source_exist, source))) {
    LOG_WARN("do update failed", K(source_exist), K(source));
  } else {
    last_refresh_ts_ = common::ObTimeUtility::fast_current_time();
  }
  return ret;
}

bool ObRemoteLocationAdaptor::is_tenant_primary_()
{
  return MTL_GET_TENANT_ROLE_CACHE() == share::ObTenantRole::PRIMARY_TENANT;
}

int ObRemoteLocationAdaptor::do_update_(const bool is_add_source, const share::ObLogRestoreSourceItem &item)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLogRestoreHandler *restore_handler = NULL;
  if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get log stream iter failed");
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter is NULL", K(iter));
  } else {
    while (OB_SUCC(ret)) {
      ls = NULL;
      bool need_update = false;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter ls get next failed");
        } else {
          LOG_TRACE("iter to end");
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", K(ls));
      } else if (OB_FAIL(check_replica_status_(*ls, need_update))) {
        LOG_WARN("check replica status failed", K(ret), K(ls));
      } else if (! need_update) {
        // just skip
      } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get restore_handler failed", KPC(ls));
      } else if (is_add_source) {
        ret = add_source_(item, *restore_handler);
      } else {
        ret = clean_source_(*restore_handler);
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRemoteLocationAdaptor::get_source_(share::ObLogRestoreSourceItem &item, bool &source_exist)
{
  int ret = OB_SUCCESS;
  share::ObLogRestoreSourceMgr mgr;
  source_exist = false;
  if (OB_FAIL(mgr.init(MTL_ID(), GCTX.sql_proxy_))) {
    LOG_WARN("ObLogRestoreSourceMgr init failed", K(ret));
  } else if (OB_FAIL(mgr.get_source(item)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("get source failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    source_exist = false;
  } else {
    source_exist = true;
  }
  return ret;
}

int ObRemoteLocationAdaptor::check_replica_status_(ObLS &ls, bool &need_update)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus restore_status;
  ObLogRestoreHandler *restore_handler = NULL;
  if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get restore_handler failed", K(ret), K(ls));
  } else if (! restore_handler->need_update_source()) {
    need_update = false;
  } else if (OB_FAIL(ls.get_restore_status(restore_status))) {
    LOG_WARN("get restore status failed", K(ret), K(ls));
  } else {
    need_update = restore_status.can_restore_log();
  }
  return ret;
}

int ObRemoteLocationAdaptor::clean_source_(ObLogRestoreHandler &restore_handler)
{
  return restore_handler.clean_source();
}

int ObRemoteLocationAdaptor::add_source_(const share::ObLogRestoreSourceItem &item,
    ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log restore source item", K(ret), K(item));
  } else if (is_location_log_source_type(item.type_)) {
    ret = add_location_source_(item, restore_handler);
  } else if (is_service_log_source_type(item.type_)) {
    ret = add_service_source_(item, restore_handler);
  } else if (is_raw_path_log_source_type(item.type_)) {
    ret = add_rawpath_source_(item, restore_handler);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObRemoteLocationAdaptor::add_location_source_(const share::ObLogRestoreSourceItem &item,
    ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  if (OB_FAIL(ObLogRestoreSourceMgr::get_backup_dest(item, dest))) {
    LOG_WARN("get backup dest failed", K(ret), K(item));
  } else if (OB_FAIL(restore_handler.add_source(dest, item.until_scn_))) {
    LOG_WARN("add ObBackupDest source failed", K(ret), K(dest), K(item));
  }
  return ret;
}

int ObRemoteLocationAdaptor::add_service_source_(const share::ObLogRestoreSourceItem &item,
    ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  /*
  common::ObAddr addr;
  if (OB_FAIL(addr.parse_from_string(ObString(item.value_.ptr())))) {
    LOG_WARN("addr parse from string failed", K(ret), K(item));
  } else if (OB_FAIL(restore_handler.add_source(addr, item.until_scn_))) {
    LOG_WARN("add ObAddr source failed", K(ret), K(addr), K(item));
  }
  clean_source_(restore_handler);
  common::ObAddr addr(common::ObAddr::IPV4, "127.0.0.1", 42959);
  ret = restore_handler.add_source(addr, item.until_scn_);
  */
  RestoreServiceAttr service_attr;
  ObSqlString value;
  if (OB_FAIL(value.assign(item.value_))) {
    LOG_WARN("string assign failed", K(item));
  } else if (OB_FAIL(service_attr.parse_service_attr_from_str(value))) {
    LOG_WARN("parse service attr failed", K(value));
  } else if (OB_UNLIKELY(!service_attr.is_valid())) {
    ret = OB_INVALID_DEST_ID;
  } else if (OB_FAIL(restore_handler.add_source(service_attr, item.until_scn_))) {
    LOG_WARN("add source failed", K(item), K(service_attr));
  }
  return ret;
}

int ObRemoteLocationAdaptor::add_rawpath_source_(const share::ObLogRestoreSourceItem &item,
    ObLogRestoreHandler &restore_handler)
{
  int ret = OB_SUCCESS;
  SMART_VAR(logservice::DirArray, dir_array) {
    ObSqlString tmp_str;
    char *token = nullptr;
    char *saveptr = nullptr;

    if (OB_FAIL(tmp_str.assign(item.value_))) {
      LOG_WARN("fail to parse rawpath value", K(item));
    } else {
      token = tmp_str.ptr();
      for (char *str = token; OB_SUCC(ret); str = nullptr) {
        ObBackupDest dest;
        SMART_VAR(logservice::DirInfo, dir_info) {
          char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
          token = ::STRTOK_R(str, ",", &saveptr);
          if (nullptr == token) {
            break;
          } else if (OB_FAIL(dest.set(token))) {
            LOG_WARN("fail to set dest", K(token));
          } else if (OB_FALSE_IT(dir_info.first = dest.get_root_path())) {
          } else if (OB_FAIL(dest.get_storage_info()->get_storage_info_str(storage_info_str, sizeof(storage_info_str)))) {
            LOG_WARN("fail to get storage info str", K(dest));
          } else if (OB_FALSE_IT(dir_info.second = storage_info_str)) {
          } else if (OB_FAIL(dir_array.push_back(dir_info))) {
            LOG_WARN("fail to push backup dir_array", K(ret), K(dir_array));
          }
        }
      }
    }
    if ((OB_SUCC(ret)) && OB_FAIL(restore_handler.add_source(dir_array, item.until_scn_))) {
      LOG_WARN("fail to add rawpath source", K(ret), K(item), K(dir_array));
    } else {
      LOG_INFO("add rawpath source", K(ret), K(dir_array.count()), K(dir_array));
    }
  }
  return ret;
}
} // namespace logservice
} // namespace oceanbase
