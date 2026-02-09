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
#include "share/backup/ob_backup_util.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"

using namespace oceanbase;
using namespace share;
//TODO(xingzhi): move parse_str_to_array of backup_clean to here after mannual clean realized
int ObBackupUtil::parse_str_to_array(const char *str, ObIArray<uint64_t> &array)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", str))) {
    LOG_WARN("failed to printf executor_tenant_id", KR(ret), K(str));
  } else {
    char *token = nullptr;
    char *save_ptr = nullptr;
    char *p_end = nullptr;
    token = strtok_r(tmp_str, ",", &save_ptr);
    while (OB_SUCC(ret) && nullptr != token) {
      uint64_t tenant_id = OB_INVALID_ID;
      if (OB_FAIL(ob_strtoull(token, p_end, tenant_id))) {
        LOG_WARN("failed to get tenant id from token", KR(ret), K(token));
      } else if ('\0' == *p_end) {
        if (OB_FAIL(array.push_back(tenant_id))) {
          LOG_WARN("failed to push back tenant id", KR(ret), K(tenant_id));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("set tenant id error", KR(ret), K(token));
      }
      token = STRTOK_R(nullptr, ",", &save_ptr);
      p_end = nullptr;
    }
  }
  return ret;
}

int ObBackupUtil::get_ls_ids_from_traverse(
  const ObBackupPath &path,
  const common::ObObjectStorageInfo *storage_info,
  ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  const int64_t warn_threshold = 2 * 1000 * 1000L; // 2s threshold
  common::ObTimeGuard time_guard("get_ls_ids_from_traverse", warn_threshold);
  ObBackupIoAdapter util;
  common::ObArray<common::ObIODirentEntry> d_entrys;
  common::ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  char logstream_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };

  if (OB_ISNULL(storage_info) || path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(databuff_printf(logstream_prefix, sizeof(logstream_prefix), "%s", "logstream_"))) {
    LOG_WARN("failed to set log stream prefix", KR(ret));
  } else if (OB_FAIL(prefix_op.init(logstream_prefix, strlen(logstream_prefix)))) {
    LOG_WARN("failed to init dir prefix", KR(ret), K(logstream_prefix));
  } else {
    time_guard.click("before list_directories");
    if (OB_FAIL(util.list_directories(path.get_obstr(), storage_info, prefix_op))) {
      LOG_WARN("failed to list directories", KR(ret), K(path), KP(storage_info));
    } else {
      time_guard.click("parse_ls_ids");
      common::ObIODirentEntry tmp_entry;
      share::ObLSID ls_id;
      for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
        int64_t id_val = 0;
        tmp_entry = d_entrys.at(i);
        if (OB_ISNULL(tmp_entry.name_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file name is null", KR(ret));
        } else if (OB_FAIL(parse_ls_id(tmp_entry.name_, id_val))) {
          LOG_WARN("failed to parse ls id", KR(ret));
        } else if (FALSE_IT(ls_id = id_val)) {
        } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
          LOG_WARN("failed to push back ls_ids", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_ls_ids_from_traverse completed", K(ls_ids.count()), K(time_guard));
  }
  return ret;
}

int ObBackupUtil::parse_ls_id(const char *dir_name, int64_t &id_val)
{
  int ret = OB_SUCCESS;
  id_val = 0;
  const char *LOGSTREAM = "logstream";
  char *token = NULL;
  char *saved_ptr = NULL;
  char tmp_name[OB_MAX_URI_LENGTH] = { 0 };
  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dir_name));
  } else if (OB_FAIL(databuff_printf(tmp_name, sizeof(tmp_name), "%s", dir_name))) {
    LOG_WARN("failed to set dir name", KR(ret), KP(dir_name));
  } else {
    token = tmp_name;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "_", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(LOGSTREAM, token, strlen(LOGSTREAM))) {
        continue;
      } else if (OB_FAIL(ob_atoll(token, id_val))) {
        LOG_WARN("invalid number", KR(ret), K(token), K(id_val));
      }
    }
  }

  return ret;
}
