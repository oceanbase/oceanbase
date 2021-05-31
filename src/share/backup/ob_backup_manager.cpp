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

#define USING_LOG_PREFIX SERVER

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_backup_lease_info_mgr.h"
#include "ob_backup_operator.h"
#include "ob_backup_manager.h"
#include "ob_backup_struct.h"
using namespace oceanbase;
using namespace share;
using namespace common;

static const char* BASE_BACKUP_VERSION_STR = "base_backup_version";  // only used for restore
static ObBackupInfoSimpleItem backup_info_item_list[] = {
    {"backup_dest", ""},
    {"backup_status", "STOP"},  // ObBackupInfoStatus::STOP
    {"backup_type", ""},
    {"backup_snapshot_version", ""},
    {"backup_schema_version", ""},
    {"backup_data_version", ""},
    {"backup_set_id", "0"},  // OB_START_BACKUP_SET_ID =1
    {"incarnation", "1"},    // OB_START_INCARNATION=1
    {"backup_task_id", "0"},
    {"detected_backup_region", ""},
    {"log_archive_status", "STOP"},  // STOP
    {"backup_scheduler_leader", ""},
    {"backup_encryption_mode", ""},
    {"backup_passwd", ""},
    {"last_delete_expired_data_snapshot", ""},
};

ObBackupInfoItem::ObBackupInfoItem(ObBackupInfoItem::ItemList& list, const char* name, const char* value)
    : name_(name), value_(value)
{
  list.add_last(this);
}

ObBackupInfoItem::ObBackupInfoItem(const ObBackupInfoItem& item) : name_(item.name_), value_(item.value_)
{}

ObBackupInfoItem::ObBackupInfoItem() : name_(NULL), value_()
{}

ObBackupInfoItem& ObBackupInfoItem::operator=(const ObBackupInfoItem& item)
{
  name_ = item.name_;
  value_ = item.value_;
  return *this;
}

int ObBackupInfoItem::update(common::ObISQLClient& sql_client, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::update_info_item(sql_client, tenant_id, *this))) {
    LOG_WARN("update item failed", K(ret), K(tenant_id), "item", *this);
  }
  return ret;
}

int ObBackupInfoItem::update(ObBackupItemTransUpdater& updater, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else if (OB_FAIL(updater.update(tenant_id, *this))) {
    LOG_WARN("update item failed", K(ret), K(tenant_id), K(*this));
  }
  return ret;
}

int ObBackupInfoItem::get_int_value(int64_t& value) const
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  char* endptr = NULL;
  value = 0;

  if (value_.is_empty()) {
    tmp = 0;
  } else {
    tmp = strtoll(value_.ptr(), &endptr, 0);
    if ('\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("invalid data, is not int value", K(ret), K(tmp), K(value_));
    } else {
      value = tmp;
    }
  }
  return ret;
}

int ObBackupInfoItem::set_value(const int64_t value)
{
  int ret = OB_SUCCESS;
  if (value < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info set value get invalid argument", K(ret), K(value));
  } else if (0 == value) {
    if (OB_FAIL(set_value(""))) {
      LOG_WARN("failed to set value", K(ret), K(value));
    }
  } else {
    int strlen = sprintf(value_.ptr(), "%ld", value);
    if (strlen <= 0 || strlen > common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set value", K(ret), K(value), K(strlen));
    }
  }
  return ret;
}

int ObBackupInfoItem::set_value(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set value get invalid argument", K(ret), KP(buf));
  } else {
    const int64_t len = strlen(buf);
    if (len > OB_INNER_TABLE_DEFAULT_VALUE_LENTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set value buf length is unexpected", K(ret), K(len));
    } else {
      STRNCPY(value_.ptr(), buf, len);
      value_.ptr()[len] = '\0';
      LOG_DEBUG("set value", K(buf), K(strlen(buf)), K(value_));
    }
  }
  return ret;
}

int ObBackupInfoItem::insert(common::ObISQLClient& sql_client, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::insert_info_item(sql_client, tenant_id, *this))) {
    LOG_WARN("update item failed", K(ret), K(tenant_id), "item", *this);
  }
  return ret;
}

ObBackupItemTransUpdater::ObBackupItemTransUpdater() : started_(false), success_(false)
{}

ObBackupItemTransUpdater::~ObBackupItemTransUpdater()
{
  if (started_) {
    int ret = end(success_);
    if (OB_FAIL(ret)) {
      LOG_WARN("end transaction failed", K(ret));
    }
  }
}

int ObBackupItemTransUpdater::start(ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (started_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transaction already started", K(ret));
  } else if (OB_FAIL(trans_.start(&sql_proxy))) {
    LOG_WARN("start transaction failed", K(ret));
    started_ = false;
  } else {
    started_ = true;
    success_ = true;
  }
  return ret;
}

int ObBackupItemTransUpdater::update(const uint64_t tenant_id, const ObBackupInfoItem& item)
{
  int ret = OB_SUCCESS;
  if (!started_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transaction not started", K(ret));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else {
    if (OB_FAIL(ObTenantBackupInfoOperation::update_info_item(trans_, tenant_id, item))) {
      LOG_WARN("update backup info failed", K(ret), K(tenant_id), "item", item);
      success_ = false;
    }
  }
  return ret;
}

int ObBackupItemTransUpdater::load(const uint64_t tenant_id, ObBackupInfoItem& item, const bool need_lock)
{
  int ret = OB_SUCCESS;
  if (!started_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transaction not started", K(ret));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(trans_, tenant_id, item, need_lock))) {
    LOG_WARN("load backup item failed", K(ret), K(tenant_id), "item", item);
    success_ = false;
  }
  return ret;
}

int ObBackupItemTransUpdater::end(const bool commit)
{
  int ret = OB_SUCCESS;
  // can be end if ::start() fail, do not check started_ here.
  if (started_) {
    if (OB_FAIL(trans_.end(commit && success_))) {
      LOG_WARN("transaction end failed", K(ret), K(commit), K(success_));
    }
    started_ = false;
  }
  return ret;
}

#define INIT_ITEM(field, value_def) field##_(list_, #field, value_def)
#define CONSTRUCT_BASE_BACKUP_INFO()                                                                               \
  INIT_ITEM(backup_dest, ""), INIT_ITEM(backup_status, ""), INIT_ITEM(backup_type, ""),                            \
      INIT_ITEM(backup_snapshot_version, ""), INIT_ITEM(backup_schema_version, ""),                                \
      INIT_ITEM(backup_data_version, ""), INIT_ITEM(backup_set_id, ""), INIT_ITEM(incarnation, ""),                \
      INIT_ITEM(backup_task_id, ""), INIT_ITEM(detected_backup_region, ""), INIT_ITEM(backup_encryption_mode, ""), \
      INIT_ITEM(backup_passwd, "")

ObBaseBackupInfo::ObBaseBackupInfo() : tenant_id_(), CONSTRUCT_BASE_BACKUP_INFO()
{}

ObBaseBackupInfo::ObBaseBackupInfo(const ObBaseBackupInfo& other) : CONSTRUCT_BASE_BACKUP_INFO()
{
  *this = other;
}

#undef CONSTRUCT_BASE_BACKUP_INFO
#undef INIT_ITEM

ObBaseBackupInfo& ObBaseBackupInfo::operator=(const ObBaseBackupInfo& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ObBackupInfoItem* it = list_.get_first();
  const ObBackupInfoItem* o_it = other.list_.get_first();
  while (it != list_.get_header() && o_it != other.list_.get_header()) {
    if (NULL == it || NULL == o_it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null item", K(ret), KP(it), KP(o_it));
      break;
    }
    *it = *o_it;
    it = it->get_next();
    o_it = o_it->get_next();
  }
  return *this;
}

void ObBaseBackupInfo::reset()
{
  int ret = OB_SUCCESS;
  tenant_id_ = OB_INVALID_ID;
  ObBackupInfoItem* it = list_.get_first();
  while (it != list_.get_header()) {
    if (NULL == it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null item", K(ret), KP(it));
      break;
    }
    it->value_.reset();
    it = it->get_next();
  }
}

DEF_TO_STRING(ObBaseBackupInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KO("tenant_id", tenant_id_);
  J_KO("backup_dest", backup_dest_);
  J_KO("backup_status", backup_status_);
  // TODO  backup finish it later

  DLIST_FOREACH_NORET(it, list_)
  {
    J_KO(it->name_, *it);
  }
  J_OBJ_END();
  return pos;
}

bool ObBaseBackupInfo::is_valid() const
{
  bool is_valid = true;
  // TODO backup fix it later
  return is_valid;
}

bool ObBaseBackupInfo::is_empty() const
{
  return backup_status_.value_.is_empty();
}

ObBackupInfoManager::ObBackupInfoManager() : inited_(false), tenant_ids_(), proxy_(NULL)
{}

ObBackupInfoManager::~ObBackupInfoManager()
{}

void ObBackupInfoManager::reset()
{
  tenant_ids_.reset();
  proxy_ = NULL;
}

int ObBackupInfoManager::init(const common::ObIArray<uint64_t>& tenant_ids, common::ObMySQLProxy& proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant ids should not be empty", K(ret), K(tenant_ids));
  } else if (OB_FAIL(tenant_ids_.assign(tenant_ids))) {
    LOG_WARN("fail to assign tenant ids", K(ret), K(tenant_ids));
  } else {
    proxy_ = &proxy;
    inited_ = true;
  }
  return ret;
}

int ObBackupInfoManager::init(const uint64_t tenant_id, common::ObMySQLProxy& proxy)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init backup info manager get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
    LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
  } else if (OB_FAIL(init(tenant_ids, proxy))) {
    LOG_WARN("failed to init", K(ret), K(tenant_ids));
  }
  return ret;
}

int ObBackupInfoManager::get_backup_info(
    const uint64_t tenant_id, ObBackupItemTransUpdater& updater, ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  ObBaseBackupInfo tmp_info;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_backup_info(tenant_id, updater, tmp_info))) {
    LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
  } else if (OB_FAIL(convert_info_to_struct(tmp_info, info))) {
    LOG_WARN("failed to convert info to struct", K(ret), K(tmp_info));
  }

  return ret;
}

int ObBackupInfoManager::update_backup_info(
    const uint64_t tenant_id, const ObBaseBackupInfoStruct& info, ObBackupItemTransUpdater& updater)
{
  int ret = OB_SUCCESS;
  ObBaseBackupInfo tmp_info;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(convert_struct_to_info(info, tmp_info))) {
    LOG_WARN("failed to convert info to struct", K(ret), K(info));
  } else if (OB_FAIL(update_backup_info(tenant_id, tmp_info, updater))) {
    LOG_WARN("failed to update backup info", K(ret), K(tmp_info));
  }
  return ret;
}

int ObBackupInfoManager::get_backup_info(
    const uint64_t tenant_id, ObBackupItemTransUpdater& updater, ObBaseBackupInfo& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(updater.load(tenant_id, info.backup_dest_, false /*no need lock*/))) {
    LOG_WARN("failed to load backup dest", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_data_version_))) {
    LOG_WARN("failed to load backup data version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_schema_version_))) {
    LOG_WARN("failed to load backup schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_set_id_))) {
    LOG_WARN("failed to load backup set id", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_snapshot_version_))) {
    LOG_WARN("failed to load backup snapshot version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_status_))) {
    LOG_WARN("failed to load backup status", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_type_))) {
    LOG_WARN("failed to load backup type", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.detected_backup_region_))) {
    LOG_WARN("failed to load backup schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.incarnation_))) {
    LOG_WARN("failed to load backup incarnation", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_task_id_))) {
    LOG_WARN("failed to load backup task id", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_encryption_mode_))) {
    LOG_WARN("failed to load backup encryption mode", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.load(tenant_id, info.backup_passwd_))) {
    LOG_WARN("failed to load backup passwd", K(ret), K(tenant_id));
  } else {
    info.tenant_id_ = tenant_id;
    LOG_INFO("get backup info", K(info));
  }
  return ret;
}

int ObBackupInfoManager::update_backup_info(
    const uint64_t tenant_id, const ObBaseBackupInfo& info, ObBackupItemTransUpdater& updater)
{
  // TODO() consider with backup dest
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update backup info get invalid argument", K(ret), K(info));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_dest_))) {
    LOG_WARN("failed to update backup dest", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_data_version_))) {
    LOG_WARN("failed to update backup data version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_schema_version_))) {
    LOG_WARN("failed to update backup schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_set_id_))) {
    LOG_WARN("failed to update backup set id", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_snapshot_version_))) {
    LOG_WARN("failed to update backup snapshot version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_status_))) {
    LOG_WARN("failed to update backup status", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_type_))) {
    LOG_WARN("failed to update backup type", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.detected_backup_region_))) {
    LOG_WARN("failed to update backup schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.incarnation_))) {
    LOG_WARN("failed to update backup incarnation", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_task_id_))) {
    LOG_WARN("failed to update backup task id", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_encryption_mode_))) {
    LOG_WARN("failed to update backup encryption mode", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.update(tenant_id, info.backup_passwd_))) {
    LOG_WARN("failed to update backup passwd", K(ret), K(tenant_id));
  } else {
    LOG_INFO("update backup info", K(info));
  }
  return ret;
}

int ObBackupInfoManager::check_can_update(
    const ObBaseBackupInfoStruct& src_info, const ObBaseBackupInfoStruct& dest_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (!src_info.is_valid() || !dest_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src info or dest info is invalid", K(ret), K(src_info), K(dest_info));
  } else if (OB_FAIL(check_can_update_(src_info.backup_status_.status_, dest_info.backup_status_.status_))) {
    LOG_WARN("failed to check can update", K(ret), K(src_info), K(dest_info));
  }
  return ret;
}

int ObBackupInfoManager::get_tenant_count(int64_t& tenant_count) const
{
  int ret = OB_SUCCESS;
  tenant_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manger do not init", K(ret));
  } else {
    tenant_count = tenant_ids_.count();
  }
  return ret;
}

int ObBackupInfoManager::get_backup_info(ObBaseBackupInfo& info)
{
  return get_backup_info(info.tenant_id_, info);
}

int ObBackupInfoManager::get_backup_info(const uint64_t tenant_id, ObBaseBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObBackupItemTransUpdater updater;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(tenant_id), K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id), K(tenant_ids_));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    info.tenant_id_ = tenant_id;
    if (OB_FAIL(get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupInfoManager::get_backup_info(common::ObIArray<ObBaseBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  infos.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
      ObBaseBackupInfo tmp_info;
      const uint64_t tenant_id = tenant_ids_.at(i);
      if (OB_FAIL(get_backup_info(tenant_id, tmp_info))) {
        LOG_WARN("failed to get backup info", K(ret), K(tenant_id), K(tmp_info));
      } else if (OB_FAIL(infos.push_back(tmp_info))) {
        LOG_WARN("failed to push backup info into array", K(ret), K(tenant_id), K(tmp_info));
      }
    }
  }
  return ret;
}

int ObBackupInfoManager::get_backup_info(common::ObIArray<ObBaseBackupInfoStruct>& infos)
{
  int ret = OB_SUCCESS;
  infos.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
      ObBaseBackupInfo tmp_info;
      ObBaseBackupInfoStruct tmp_info_struct;
      const uint64_t tenant_id = tenant_ids_.at(i);
      if (OB_FAIL(get_backup_info(tenant_id, tmp_info))) {
        LOG_WARN("failed to get backup info", K(ret), K(tenant_id), K(tmp_info));
      } else if (OB_FAIL(convert_info_to_struct(tmp_info, tmp_info_struct))) {
        LOG_WARN("failed to convert info to struct", K(ret), K(tmp_info));
      } else if (OB_FAIL(infos.push_back(tmp_info_struct))) {
        LOG_WARN("failed to push backup info into array", K(ret), K(tenant_id), K(tmp_info_struct));
      }
    }
  }
  return ret;
}

int ObBackupInfoManager::convert_info_to_struct(const ObBaseBackupInfo& info, ObBaseBackupInfoStruct& info_struct)
{
  int ret = OB_SUCCESS;
  info_struct.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(info));
  } else if (!info.backup_dest_.value_.is_empty() &&
             OB_FAIL(info_struct.backup_dest_.assign(info.backup_dest_.value_.ptr()))) {
    LOG_WARN("failed to assign backup dest", K(ret), K(info));
  } else if (!info.detected_backup_region_.value_.is_empty() &&
             OB_FAIL(info_struct.detected_backup_region_.assign(info.detected_backup_region_.value_.ptr()))) {
    LOG_WARN("failed to assign detected backup region", K(ret), K(info));
  } else if (!info.backup_type_.value_.is_empty() &&
             OB_FAIL(info_struct.backup_type_.set_backup_type(info.backup_type_.value_.ptr()))) {
    LOG_WARN("failed to set backup type", K(ret), K(info));
  } else if (!info.backup_status_.value_.is_empty() &&
             OB_FAIL(info_struct.backup_status_.set_info_backup_status(info.backup_status_.value_.ptr()))) {
    LOG_WARN("failed to assign backup status", K(ret), K(info));
  } else if (OB_FAIL(info.backup_set_id_.get_int_value(info_struct.backup_set_id_))) {
    LOG_WARN("failed to get info_struct.backup_set_id_", K(ret), K(info));
  } else if (OB_FAIL(info.incarnation_.get_int_value(info_struct.incarnation_))) {
    LOG_WARN("failed to get info_struct.incarnation_", K(ret), K(info));
  } else if (OB_FAIL(info.backup_snapshot_version_.get_int_value(info_struct.backup_snapshot_version_))) {
    LOG_WARN("failed to get info_struct.backup_snapshot_version_", K(ret), K(info));
  } else if (OB_FAIL(info.backup_schema_version_.get_int_value(info_struct.backup_schema_version_))) {
    LOG_WARN("failed to get info_struct.backup_schema_version_", K(ret), K(info));
  } else if (OB_FAIL(info.backup_data_version_.get_int_value(info_struct.backup_data_version_))) {
    LOG_WARN("failed to get info_struct.backup_data_version_", K(ret), K(info));
  } else if (OB_FAIL(info.backup_task_id_.get_int_value(info_struct.backup_task_id_))) {
    LOG_WARN("failed to get info_struct.backup_task_id_", K(ret), K(info));
  } else if (FALSE_IT(info_struct.encryption_mode_ =
                          ObBackupEncryptionMode::parse_str(info.backup_encryption_mode_.get_value_ptr()))) {
  } else if (!ObBackupEncryptionMode::is_valid(info_struct.encryption_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid encryption mode", K(ret), K(info), K(info_struct));
  } else if (OB_FAIL(info_struct.passwd_.assign(info.backup_passwd_.get_value_ptr()))) {
    LOG_WARN("failed to get passwd", K(ret), K(info));
  } else {
    info_struct.tenant_id_ = info.tenant_id_;
  }
  return ret;
}

int ObBackupInfoManager::convert_struct_to_info(const ObBaseBackupInfoStruct& info_struct, ObBaseBackupInfo& info)
{
  // TODO backup fix it later
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (!info_struct.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(info));
  } else {
    info.tenant_id_ = info_struct.tenant_id_;
    if (OB_FAIL(info.backup_set_id_.set_value(info_struct.backup_set_id_))) {
      LOG_WARN("failed to set backup set id", K(ret), K(info_struct));
    } else if (OB_FAIL(info.incarnation_.set_value(info_struct.incarnation_))) {
      LOG_WARN("failed to set backup incarnation", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_snapshot_version_.set_value(info_struct.backup_snapshot_version_))) {
      LOG_WARN("failed to set backup snapshot version", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_schema_version_.set_value(info_struct.backup_schema_version_))) {
      LOG_WARN("failed to set backup schema version", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_data_version_.set_value(info_struct.backup_data_version_))) {
      LOG_WARN("failed to set backup data version", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_dest_.set_value(info_struct.backup_dest_.ptr()))) {
      LOG_WARN("failed to set backup dest", K(ret), K(info_struct));
    } else if (OB_FAIL(info.detected_backup_region_.set_value(info_struct.detected_backup_region_.ptr()))) {
      LOG_WARN("failed to set backup region", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_type_.set_value(info_struct.backup_type_.get_backup_type_str()))) {
      LOG_WARN("failed to set backup type", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_status_.set_value(info_struct.backup_status_.get_info_backup_status_str()))) {
      LOG_WARN("failed to set backup status", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_task_id_.set_value(info_struct.backup_task_id_))) {
      LOG_WARN("failed to set backup task id", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_encryption_mode_.set_value(
                   ObBackupEncryptionMode::to_str(info_struct.encryption_mode_)))) {
      LOG_WARN("failed to set encryption mode", K(ret), K(info_struct));
    } else if (OB_FAIL(info.backup_passwd_.set_value(info_struct.passwd_.ptr()))) {
      LOG_WARN("failed to set backup passwd", K(ret), K(info_struct));
    }
  }
  return ret;
}

int ObBackupInfoManager::find_tenant_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; i < tenant_ids_.count() && !found; ++i) {
      if (tenant_id == tenant_ids_.at(i)) {
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find tenant id", K(ret), K(tenant_id), K(tenant_ids_));
    }
  }
  return ret;
}

int ObBackupInfoManager::get_backup_info_without_trans(const uint64_t tenant_id, ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  ObBaseBackupInfo tmp_info;
  tmp_info.tenant_id_ = tenant_id;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_base_backup_info(*proxy_, tmp_info))) {
    LOG_WARN("failed to load base backup info", K(ret), K(tmp_info));
  } else if (OB_FAIL(convert_info_to_struct(tmp_info, info))) {
    LOG_WARN("failed to convert info to struct", K(ret), K(tmp_info), K(info));
  }
  return ret;
}

int ObBackupInfoManager::get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(tenant_ids.assign(tenant_ids_))) {
    LOG_WARN("failed to assign tenant ids", K(ret), K(tenant_ids_));
  }
  return ret;
}

int ObBackupInfoManager::get_detected_region(const uint64_t tenant_id, ObIArray<ObRegion>& detected_region)
{
  int ret = OB_SUCCESS;
  const char* name = "detected_backup_region";
  ObBackupInfoItem detected_region_item;
  detected_region_item.name_ = name;
  const bool need_lock = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObTenantBackupInfoOperation::load_info_item(*proxy_, tenant_id, detected_region_item, need_lock))) {
    LOG_WARN("failed to detected info", K(ret), K(tenant_id));
  } else {
    ObString detected_region_str;
    ObString detected_regions_str(detected_region_item.value_.ptr());
    bool split_end = false;
    ObRegion region;
    while (!split_end && OB_SUCC(ret)) {
      detected_region_str = detected_regions_str.split_on(',');
      if (detected_region_str.empty() && NULL == detected_region_str.ptr()) {
        split_end = true;
        detected_region_str = detected_regions_str;
      }
      region = detected_region_str.trim();
      if (!region.is_empty()) {
        if (OB_FAIL(detected_region.push_back(region))) {
          LOG_WARN("failed to push detected region into array", K(ret), K(region));
        }
      }
    }
  }
  return ret;
}

int ObBackupInfoManager::get_backup_status(
    const uint64_t tenant_id, common::ObISQLClient& trans, ObBackupInfoStatus& status)
{
  int ret = OB_SUCCESS;
  status.reset();
  ObBackupInfoItem backup_status_item;
  const char* name = "backup_status";
  backup_status_item.name_ = name;
  const bool need_lock = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(trans, tenant_id, backup_status_item, need_lock))) {
    LOG_WARN("failed to detected info", K(ret), K(tenant_id));
  } else if (OB_FAIL(status.set_info_backup_status(backup_status_item.get_value_ptr()))) {
    LOG_WARN("failed to set backup info status", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupInfoManager::get_backup_scheduler_leader(
    const uint64_t tenant_id, common::ObISQLClient& trans, common::ObAddr& scheduler_leader, bool& has_leader)
{
  int ret = OB_SUCCESS;
  scheduler_leader.reset();
  ObBackupInfoItem scheduler_leader_item;
  const char* name = "backup_scheduler_leader";
  scheduler_leader_item.name_ = name;
  const bool need_lock = true;
  has_leader = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(trans, tenant_id, scheduler_leader_item, need_lock))) {
    LOG_WARN("failed to detected info", K(ret), K(tenant_id));
  } else if (scheduler_leader_item.value_.is_empty()) {
    has_leader = false;
    LOG_INFO("scheduler leader not exist");
  } else if (OB_FAIL(scheduler_leader.parse_from_cstring(scheduler_leader_item.get_value_ptr()))) {
    LOG_WARN("failed to set backup scheduler leader", K(ret), K(scheduler_leader_item));
  }
  return ret;
}

int ObBackupInfoManager::update_backup_scheduler_leader(
    const uint64_t tenant_id, const common::ObAddr& scheduler_leader, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObBackupInfoItem scheduler_leader_item;
  const char* name = "backup_scheduler_leader";
  scheduler_leader_item.name_ = name;
  char scheduler_leader_str[MAX_IP_PORT_LENGTH] = "";

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (!scheduler_leader.is_valid()) {
    if (OB_FAIL(scheduler_leader_item.set_value(""))) {
      LOG_WARN("failed to set scheduler leader", K(ret), K(tenant_id), K(scheduler_leader));
    }
  } else if (OB_FAIL(scheduler_leader.ip_port_to_string(scheduler_leader_str, MAX_IP_PORT_LENGTH))) {
    LOG_WARN("failed to add addr to buf", K(ret), K(scheduler_leader));
  } else if (OB_FAIL(scheduler_leader_item.set_value(scheduler_leader_str))) {
    LOG_WARN("failed to set backup scheduler leader", K(ret), K(scheduler_leader), K(tenant_id));
  } else if (OB_FAIL(scheduler_leader_item.update(trans, tenant_id))) {
    LOG_WARN("failed to update scheduler leader", K(ret), K(tenant_id), K(scheduler_leader));
  }
  return ret;
}

int ObBackupInfoManager::clean_backup_scheduler_leader(const uint64_t tenant_id, const common::ObAddr& scheduler_leader)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tenant_id != OB_SYS_TENANT_ID || !scheduler_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(scheduler_leader));
  } else if (OB_FAIL(
                 ObTenantBackupInfoOperation::clean_backup_scheduler_leader(*proxy_, tenant_id, scheduler_leader))) {
    LOG_WARN("failed to clean backup scheduler leader", K(ret));
  }

  return ret;
}

int ObBackupInfoManager::insert_restore_tenant_base_backup_version(
    const uint64_t tenant_id, const int64_t major_version)
{
  int ret = OB_SUCCESS;
  ObBackupInfoItem base_backup_version_item;
  const int64_t base_backup_version = major_version + 1;
  const char* name = "base_backup_version";
  base_backup_version_item.name_ = name;
  const bool need_lock = false;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (base_backup_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert restore tenant base backup version get invalid argument",
        K(ret),
        K(major_version),
        K(base_backup_version));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(
                 *proxy_, tenant_id, base_backup_version_item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to detected info", K(ret), K(tenant_id));
    }
  }

  if (OB_BACKUP_INFO_NOT_EXIST == ret) {
    // overwrite ret
    if (OB_FAIL(base_backup_version_item.set_value(base_backup_version))) {
      LOG_WARN("failed to set backup scheduler leader", K(ret), K(base_backup_version_item), K(tenant_id));
    } else if (OB_FAIL(base_backup_version_item.insert(*proxy_, tenant_id))) {
      LOG_WARN("failed to update scheduler leader", K(ret), K(tenant_id), K(base_backup_version_item));
    }
  }

  return ret;
}

int ObBackupInfoManager::get_job_id(int64_t& job_id)
{
  int ret = OB_SUCCESS;
  ObBackupInfoItem job_id_item;
  const char* name = "job_id";
  job_id_item.name_ = name;
  const int64_t FIRST_JOB_ID = 1;
  const bool need_lock = false;
  // TODO : need to do in one transaction
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert job id get invalid argument", K(ret));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(*proxy_, OB_SYS_TENANT_ID, job_id_item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      if (OB_FAIL(job_id_item.set_value(FIRST_JOB_ID))) {
        LOG_WARN("failed to set job id", K(ret));
      } else if (OB_FAIL(job_id_item.insert(*proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to update job id", K(ret));
      } else if (FALSE_IT(job_id = FIRST_JOB_ID)) {
        // do nothing
      }
    }
  } else {
    int64_t tmp_job_id = 0;
    if (OB_FAIL(job_id_item.get_int_value(tmp_job_id))) {
      LOG_WARN("failed to get int value", K(ret));
    } else if (OB_FAIL(job_id_item.set_value(tmp_job_id + 1))) {
      LOG_WARN("failed to set value", K(ret));
    } else if (OB_FAIL(job_id_item.update(*proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to update job id", K(ret));
    } else if (FALSE_IT(job_id = tmp_job_id + 1)) {
      // do nothing
    }
  }
  return ret;
}

int ObBackupInfoManager::get_base_backup_version(
    const uint64_t tenant_id, common::ObISQLClient& trans, int64_t& base_backup_version)
{
  int ret = OB_SUCCESS;
  base_backup_version = 0;
  ObBackupInfoItem base_backup_version_item;
  const char* name = "base_backup_version";
  base_backup_version_item.name_ = name;
  const bool need_lock = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObTenantBackupInfoOperation::load_info_item(trans, tenant_id, base_backup_version_item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to detected info", K(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (base_backup_version_item.value_.is_empty()) {
    // do nothing
  } else if (OB_FAIL(base_backup_version_item.get_int_value(base_backup_version))) {
    LOG_WARN("failed to get base backup version", K(ret), K(base_backup_version_item));
  }
  return ret;
}

int ObBackupInfoManager::is_backup_started(bool& is_started)
{
  int ret = OB_SUCCESS;
  is_started = false;
  ObMySQLTransaction trans;
  ObBackupInfoStatus status;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(trans.start(proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_status(OB_SYS_TENANT_ID, trans, status))) {
      LOG_WARN("failed to get backup status", K(ret));
    } else {
      is_started = !status.is_stop_status();
    }
  }
  return ret;
}

int ObBackupInfoManager::get_last_delete_expired_data_snapshot(
    const uint64_t tenant_id, common::ObISQLClient& trans, int64_t& last_delete_expired_data_snapshot)
{
  int ret = OB_SUCCESS;
  last_delete_expired_data_snapshot = 0;
  ObBackupInfoItem last_delete_expired_snaphost_item;
  const char* name = "last_delete_expired_data_snapshot";
  last_delete_expired_snaphost_item.name_ = name;
  const bool need_lock = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(
                 trans, tenant_id, last_delete_expired_snaphost_item, need_lock))) {
    LOG_WARN("failed to detected info", K(ret), K(tenant_id));
  } else if (last_delete_expired_snaphost_item.value_.is_empty()) {
    // do nothing
  } else if (OB_FAIL(last_delete_expired_snaphost_item.get_int_value(last_delete_expired_data_snapshot))) {
    LOG_WARN("failed to get int value", K(ret), K(last_delete_expired_snaphost_item));
  }
  return ret;
}

int ObBackupInfoManager::update_last_delete_expired_data_snapshot(
    const uint64_t tenant_id, const int64_t last_delete_expired_data_snapshot, common::ObISQLClient& trans)
{
  int ret = OB_SUCCESS;
  ObBackupInfoItem last_delete_expired_snaphost_item;
  const char* name = "last_delete_expired_data_snapshot";
  last_delete_expired_snaphost_item.name_ = name;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(last_delete_expired_snaphost_item.set_value(last_delete_expired_data_snapshot))) {
    LOG_WARN("failed to set backup scheduler leader", K(ret), K(last_delete_expired_data_snapshot), K(tenant_id));
  } else if (OB_FAIL(last_delete_expired_snaphost_item.update(trans, tenant_id))) {
    LOG_WARN("failed to update scheduler leader", K(ret), K(tenant_id), K(last_delete_expired_snaphost_item));
  }
  return ret;
}

int ObBackupInfoManager::check_can_update_(
    const ObBackupInfoStatus::BackupStatus& src_status, const ObBackupInfoStatus::BackupStatus& dest_status)
{
  int ret = OB_SUCCESS;
  if (src_status > ObBackupInfoStatus::MAX || dest_status > ObBackupInfoStatus::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src or dest status is invalid", K(ret), K(src_status), K(dest_status));
  } else {
    switch (src_status) {
      case ObBackupInfoStatus::PREPARE:
        if (ObBackupInfoStatus::SCHEDULE != dest_status && ObBackupInfoStatus::CANCEL != dest_status &&
            ObBackupInfoStatus::STOP != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupInfoStatus::SCHEDULE:
        if (ObBackupInfoStatus::SCHEDULE != dest_status && ObBackupInfoStatus::DOING != dest_status &&
            ObBackupInfoStatus::CANCEL != dest_status && ObBackupInfoStatus::STOP != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupInfoStatus::DOING:
        if (ObBackupInfoStatus::DOING != dest_status && ObBackupInfoStatus::CLEANUP != dest_status &&
            ObBackupInfoStatus::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupInfoStatus::CLEANUP:
        if (ObBackupInfoStatus::CLEANUP != dest_status && ObBackupInfoStatus::STOP != dest_status &&
            ObBackupInfoStatus::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupInfoStatus::CANCEL:
        if (ObBackupInfoStatus::CANCEL != dest_status && ObBackupInfoStatus::CLEANUP != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupInfoStatus::STOP:
        if (ObBackupInfoStatus::STOP != dest_status && ObBackupInfoStatus::PREPARE != dest_status &&
            ObBackupInfoStatus::SCHEDULE != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupInfoStatus::MAX:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
        break;
    }
  }
  return ret;
}

ObBackupInfoSimpleItem::ObBackupInfoSimpleItem() : name_(""), value_("")
{}

ObBackupInfoSimpleItem::ObBackupInfoSimpleItem(const char* name, const char* value) : name_(name), value_(value)
{}

ObBackupInfoChecker::ObBackupInfoChecker() : is_inited_(false), sql_proxy_(nullptr)
{}

ObBackupInfoChecker::~ObBackupInfoChecker()
{}

int ObBackupInfoChecker::init(common::ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sql_proxy));
  } else {
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupInfoChecker::check(
    const common::ObIArray<uint64_t>& tenant_ids, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  int64_t count = tenant_ids.count();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    if (OB_FAIL(backup_lease_service.check_lease())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(check(tenant_id))) {
      LOG_WARN("failed to check tenant backup info", K(ret), K(i), K(tenant_id));
    }
  }

  return ret;
}

int ObBackupInfoChecker::check(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t status_line_count = 0;
  common::ObArray<ObBackupInfoSimpleItem> new_items;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_status_line_count_(tenant_id, status_line_count))) {
    LOG_WARN("failed to get_status_line_count_", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_new_items_(*sql_proxy_, tenant_id, false /*for update*/, new_items))) {
    LOG_WARN("failed to get new items", K(ret), K(tenant_id));
  } else if (new_items.empty() && 0 != status_line_count) {
    // do no thing
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (0 == status_line_count) {
      if (OB_FAIL(insert_log_archive_status_(trans, tenant_id))) {
        LOG_WARN("failed to insert log archive srtatus", K(ret), K(tenant_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_new_items_(trans, tenant_id, true /*for update*/, new_items))) {
      LOG_WARN("failed to get new items", K(ret), K(tenant_id));
    } else if (OB_FAIL(insert_new_items_(trans, tenant_id, new_items))) {
      LOG_WARN("failed to insert new items", K(ret), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {  // commit
        LOG_WARN("failed to commit", K(ret), K(tenant_id));
      } else {
        FLOG_INFO("succeed to add backup info items", K(ret), K(tenant_id), K(new_items));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {  // rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObBackupInfoChecker::get_item_count_(const uint64_t tenant_id, int64_t& item_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    item_count = 0;

    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id=%lu and name not in ('%s')",
            share::OB_ALL_TENANT_BACKUP_INFO_TNAME,
            tenant_id,
            BASE_BACKUP_VERSION_STR))) {
      LOG_WARN("failed to init backup info sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", item_count, int64_t);
    }
  }
  return ret;
}

int ObBackupInfoChecker::get_status_line_count_(const uint64_t tenant_id, int64_t& status_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    status_count = 0;

    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id=%lu ",
            share::OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
            tenant_id))) {
      LOG_WARN("failed to init backup info sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", status_count, int64_t);
    }
  }
  return ret;
}

int ObBackupInfoChecker::get_new_items_(common::ObISQLClient& trans, const uint64_t tenant_id, const bool for_update,
    common::ObIArray<ObBackupInfoSimpleItem>& items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char* for_update_str = for_update ? "for update" : "";

  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
    char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
    int real_length = 0;
    int64_t read_count = 0;
    items.reset();

    if (OB_FAIL(sql.assign_fmt("select name from %s where tenant_id=%lu %s",
            OB_ALL_TENANT_BACKUP_INFO_TNAME,
            tenant_id,
            for_update_str))) {
      LOG_WARN("failed to init backup info sql", K(ret));
    } else if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else {
      const int64_t count = ARRAYSIZEOF(backup_info_item_list);
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (OB_FAIL(items.push_back(backup_info_item_list[i]))) {
          LOG_WARN("failed to add items", K(ret), K(i));
        }
      }
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }

      EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name_str, sizeof(name_str), real_length);
      const int64_t count = items.count();
      bool found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (0 == strcmp(name_str, items.at(i).name_)) {
          if (OB_FAIL(items.remove(i))) {
            LOG_WARN("failed to remove item", K(ret), K(i));
          } else {
            LOG_TRACE("remove exist item", K(ret), K(i), K(name_str));
          }
          found = true;
          break;
        }
      }

      if (OB_SUCC(ret) && !found) {
        LOG_DEBUG("unkown item", K(ret), K(sql), K(tenant_id), K(name_str), K(items));
      }
    }
  }
  return ret;
}

int ObBackupInfoChecker::insert_new_items_(
    common::ObMySQLTransaction& trans, const uint64_t tenant_id, common::ObIArray<ObBackupInfoSimpleItem>& items)
{
  int ret = OB_SUCCESS;
  const int64_t count = items.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(insert_new_item_(trans, tenant_id, items.at(i)))) {
      LOG_WARN("failed to insert new item", K(ret), K(tenant_id), "item", items.at(i));
    }
  }

  return ret;
}

int ObBackupInfoChecker::insert_new_item_(
    common::ObMySQLTransaction& trans, const uint64_t tenant_id, ObBackupInfoSimpleItem& item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (OB_FAIL(sql.assign_fmt("insert into %s(tenant_id, name, value) values(%lu, '%s', '%s')",
          OB_ALL_TENANT_BACKUP_INFO_TNAME,
          tenant_id,
          item.name_,
          item.value_))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(item));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    LOG_INFO("[LOG_ARCHIVE] insert_new_item_", K(sql));
  }

  return ret;
}

int ObBackupInfoChecker::insert_log_archive_status_(common::ObMySQLTransaction& trans, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (OB_FAIL(sql.assign_fmt(
          "insert into %s(%s,%s,%s,%s,%s, %s) values(%lu, %ld, 0,usec_to_time(0), usec_to_time(0), 'STOP') ",
          OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
          OB_STR_TENANT_ID,
          OB_STR_INCARNATION,
          OB_STR_LOG_ARCHIVE_ROUND,
          OB_STR_MIN_FIRST_TIME,
          OB_STR_MAX_NEXT_TIME,
          OB_STR_STATUS,
          tenant_id,
          OB_START_INCARNATION))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    LOG_INFO("[LOG_ARCHIVE] insert_log_archive_status_", K(sql));
  }
  return ret;
}
