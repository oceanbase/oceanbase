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

#define USING_LOG_PREFIX SHARE
#include "lib/restore/ob_storage.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_struct.h"
#include "share/restore/ob_log_archive_source.h"
#include "ob_log_archive_source_mgr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
using namespace oceanbase::share;
int ObLogArchiveSourceMgr::init(const uint64_t tenant_id, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLogArchiveSourceMgr already init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(! is_user_tenant(tenant_id)) || OB_ISNULL(proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(proxy));
  } else if (OB_FAIL(table_operator_.init(tenant_id, proxy))) {
    LOG_WARN("table_operator_ init failed", K(ret), K(tenant_id), K(proxy));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObLogArchiveSourceMgr::update_recovery_until_ts(const int64_t recovery_until_ts_ns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogArchiveSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == recovery_until_ts_ns)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(recovery_until_ts_ns));
  } else {
    ObLogArchiveSourceItem item(tenant_id_,
                                OB_DEFAULT_LOG_ARCHIVE_SOURCE_ID,
                                recovery_until_ts_ns);
    if (OB_FAIL(table_operator_.update_source_until_ts(item))) {
      LOG_WARN("table_operator_ update_source_until_ts failed", K(ret), K(recovery_until_ts_ns));
    } else {
      LOG_INFO("update log archive source recovery until ts succ", K(recovery_until_ts_ns));
    }
  }
  return ret;
}

int ObLogArchiveSourceMgr::delete_source()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogArchiveSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(table_operator_.delete_source())) {
    LOG_WARN("table_operator_ delete_source failed", K(ret));
  } else {
    LOG_INFO("delete log archive source succ");
  }
  return ret;
}

int ObLogArchiveSourceMgr::add_service_source(const int64_t recovery_until_ts_ns,
    const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogArchiveSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(! addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObLogArchiveSourceMgr::add_location_source(const int64_t recovery_until_ts_ns,
    const ObString &archive_dest)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  char dest_buf[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogArchiveSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(archive_dest.empty() || recovery_until_ts_ns <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(archive_dest), K(recovery_until_ts_ns));
  } else if (OB_FAIL(dest.set(archive_dest.ptr()))) {
    // use backup dest to manage oss key
    LOG_WARN("set backup dest failed", K(ret), K(archive_dest));
  } else if (OB_FAIL(dest.get_backup_dest_str(dest_buf, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("get backup dest str failed", K(ret), K(dest));
  } else {
    ObLogArchiveSourceItem item(tenant_id_,
                                OB_DEFAULT_LOG_ARCHIVE_SOURCE_ID,
                                ObLogArchiveSourceType::LOCATION,
                                ObString(dest_buf),
                                recovery_until_ts_ns);
    if (OB_FAIL(table_operator_.insert_source(item))) {
      LOG_WARN("table_operator_ insert_source failed", K(ret), K(item));
    } else {
      LOG_INFO("add location source succ", K(recovery_until_ts_ns), K(archive_dest));
    }
  }
  return ret;
}

int ObLogArchiveSourceMgr::add_rawpath_source(const int64_t recovery_until_ts_ns, const DirArray &array)
{
  return OB_NOT_SUPPORTED;
}

int ObLogArchiveSourceMgr::get_source(ObLogArchiveSourceItem &item)
{
  int ret = OB_SUCCESS;
  // only support src_id 1
  item.tenant_id_ = tenant_id_;
  item.id_ = OB_DEFAULT_LOG_ARCHIVE_SOURCE_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogArchiveSourceMgr not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(table_operator_.get_source(item))) {
    LOG_WARN("table_operator_ get_source failed", K(ret));
  } else {
    LOG_TRACE("get_source succ", K(item));
  }
  return ret;
}

int ObLogArchiveSourceMgr::get_backup_dest(const ObLogArchiveSourceItem &item, ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! item.is_valid() || ! is_location_log_source_type(item.type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if OB_FAIL(dest.set(item.value_)) {
    LOG_WARN("backup dest set failed", K(ret), K(item));
  }
  return ret;
}
