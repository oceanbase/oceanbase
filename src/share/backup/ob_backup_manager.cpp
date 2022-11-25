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

static const char *BASE_BACKUP_VERSION_STR = "base_backup_version"; // only used for restore
static ObBackupInfoSimpleItem backup_info_item_list[] = {
    {"backup_dest", ""},
    {"backup_backup_dest", ""},
    {"backup_status", "STOP"}, // ObBackupInfoStatus::STOP
    {"backup_type", ""},
    {"backup_snapshot_version", ""},
    {"backup_schema_version", ""},
    {"backup_data_version", ""},
    {"backup_set_id", "0"}, // OB_START_BACKUP_SET_ID =1
    {"incarnation", "1"}, // OB_START_INCARNATION=1
    {"backup_task_id", "0"},
    {"detected_backup_region", ""},
    {"log_archive_status", "STOP"}, // STOP
    {"backup_scheduler_leader", ""},
    {"backup_encryption_mode", ""},
    {"backup_passwd", ""},
    {"enable_auto_backup_archivelog", ""},
    {"delete_obsolete_backup_snapshot", ""},
    {"delete_obsolete_backup_backup_snapshot", ""},
};


ObBackupInfoItem::ObBackupInfoItem(ObBackupInfoItem::ItemList &list,
                               const char *name, const char *value)
    : name_(name), value_(value)
{
  list.add_last(this);
}

ObBackupInfoItem::ObBackupInfoItem(const ObBackupInfoItem &item)
    : name_(item.name_), value_(item.value_)
{
}

ObBackupInfoItem::ObBackupInfoItem()
  :name_(NULL), value_()
{
}

ObBackupInfoItem &ObBackupInfoItem::operator =(const ObBackupInfoItem &item)
{
  name_ = item.name_;
  value_ = item.value_;
  return *this;
}

int ObBackupInfoItem::update(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id)
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

int ObBackupInfoItem::update(
    ObBackupItemTransUpdater &updater,
    const uint64_t tenant_id)
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

int ObBackupInfoItem::get_int_value(int64_t &value) const
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  char *endptr = NULL;
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

int ObBackupInfoItem::set_value(const char *buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set value get invalid argument", K(ret), KP(buf));
  } else {
    const int64_t len = strlen(buf);
    if (len >= OB_INNER_TABLE_DEFAULT_VALUE_LENTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer len is unexpected", K(ret), K(len));
    } else {
      STRNCPY(value_.ptr(), buf, len);
      value_.ptr()[len] = '\0';
      LOG_DEBUG("set value", K(buf), K(strlen(buf)), K(value_));
    }
  }
  return ret;
}

int ObBackupInfoItem::insert(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id)
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

ObBackupItemTransUpdater::ObBackupItemTransUpdater()
    : started_(false), success_(false)
{
}

ObBackupItemTransUpdater::~ObBackupItemTransUpdater()
{
  if (started_) {
    int ret = end(success_);
    if (OB_FAIL(ret)) {
      LOG_WARN("end transaction failed", K(ret));
    }
  }
}

int ObBackupItemTransUpdater::start(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (started_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transaction already started", K(ret));
  } else if (OB_FAIL(trans_.start(&sql_proxy, tenant_id))) {
    LOG_WARN("start transaction failed", K(ret), K(tenant_id));
    started_ = false;
  } else {
    started_ = true;
    success_ = true;
  }
  return ret;
}

int ObBackupItemTransUpdater::update(
    const uint64_t tenant_id,
    const ObBackupInfoItem &item)
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

int ObBackupItemTransUpdater::load(
    const uint64_t tenant_id,
    ObBackupInfoItem &item,
    const bool need_lock)
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
#define CONSTRUCT_BASE_BACKUP_INFO() \
  INIT_ITEM(backup_dest, ""),                             \
  INIT_ITEM(backup_backup_dest, ""),                             \
  INIT_ITEM(backup_status, ""),                           \
  INIT_ITEM(backup_type, ""),                             \
  INIT_ITEM(backup_snapshot_version, ""),                 \
  INIT_ITEM(backup_schema_version, ""),                   \
  INIT_ITEM(backup_data_version, ""),                     \
  INIT_ITEM(backup_set_id, ""),                           \
  INIT_ITEM(incarnation, ""),                             \
  INIT_ITEM(backup_task_id, ""),                          \
  INIT_ITEM(detected_backup_region, ""),                          \
  INIT_ITEM(backup_encryption_mode, ""),                          \
  INIT_ITEM(backup_passwd, "")

ObBaseBackupInfo::ObBaseBackupInfo()
  : tenant_id_(),
    CONSTRUCT_BASE_BACKUP_INFO()
{
}

ObBaseBackupInfo::ObBaseBackupInfo(const ObBaseBackupInfo &other)
  : CONSTRUCT_BASE_BACKUP_INFO()
{
  *this = other;
}

#undef CONSTRUCT_BASE_BACKUP_INFO
#undef INIT_ITEM

ObBaseBackupInfo &ObBaseBackupInfo::operator = (const ObBaseBackupInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ObBackupInfoItem *it = list_.get_first();
  const ObBackupInfoItem *o_it = other.list_.get_first();
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
  ObBackupInfoItem *it = list_.get_first();
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

bool ObBaseBackupInfo::is_empty() const
{
  return backup_status_.value_.is_empty();
}


ObBackupInfoManager::ObBackupInfoManager()
  : inited_(false),
    tenant_ids_(), proxy_(NULL)
{
}

ObBackupInfoManager::~ObBackupInfoManager()
{
}

void ObBackupInfoManager::reset()
{
  tenant_ids_.reset();
  proxy_ = NULL;
}

int ObBackupInfoManager::init(
    const common::ObIArray<uint64_t> &tenant_ids,
    common::ObMySQLProxy &proxy)
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

int ObBackupInfoManager::init(
    const uint64_t tenant_id,
    common::ObMySQLProxy &proxy)
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


int ObBackupInfoManager::check_can_update(
    const ObBaseBackupInfoStruct &src_info,
    const ObBaseBackupInfoStruct &dest_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (!src_info.is_valid() || !dest_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src info or dest info is invalid", K(ret), K(src_info), K(dest_info));
  } else if (OB_FAIL(check_can_update_(src_info.backup_status_.status_,
      dest_info.backup_status_.status_))) {
    LOG_WARN("failed to check can update", K(ret), K(src_info), K(dest_info));
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

int ObBackupInfoManager::get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids)
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

int ObBackupInfoManager::get_backup_scheduler_leader(
    const uint64_t tenant_id,
    common::ObISQLClient &trans,
    common::ObAddr &scheduler_leader,
    bool &has_leader)
{
  int ret = OB_SUCCESS;
  scheduler_leader.reset();
  ObBackupInfoItem scheduler_leader_item;
  const char *name = "backup_scheduler_leader";
  scheduler_leader_item.name_ = name;
  const bool need_lock = true;
  has_leader = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(find_tenant_id(tenant_id))) {
    LOG_WARN("failed to find tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(trans,
      tenant_id, scheduler_leader_item, need_lock))) {
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
    const uint64_t tenant_id,
    const common::ObAddr &scheduler_leader,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObBackupInfoItem scheduler_leader_item;
  const char *name = "backup_scheduler_leader";
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

int ObBackupInfoManager::clean_backup_scheduler_leader(
    const uint64_t tenant_id,
    const common::ObAddr &scheduler_leader)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tenant_id != OB_SYS_TENANT_ID || !scheduler_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(scheduler_leader));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::clean_backup_scheduler_leader(*proxy_,
      tenant_id, scheduler_leader))) {
    LOG_WARN("failed to clean backup scheduler leader", K(ret));
  }

  return ret;
}

int ObBackupInfoManager::get_job_id(int64_t& job_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupInfoItem job_id_item;
  const char *name = "job_id";
  job_id_item.name_ = name;
  const int64_t FIRST_JOB_ID = 1;
  const bool need_lock = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup info manager do not init", K(ret));
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(ObTenantBackupInfoOperation::load_info_item(
        trans, OB_SYS_TENANT_ID, job_id_item, need_lock))) {
      if (OB_BACKUP_INFO_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(job_id_item.set_value(FIRST_JOB_ID))) {
          LOG_WARN("failed to set job id", K(ret));
        } else if (OB_FAIL(job_id_item.insert(trans, OB_SYS_TENANT_ID))) {
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
      } else if (OB_FAIL(job_id_item.update(trans, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to update job id", K(ret));
      } else if (FALSE_IT(job_id = tmp_job_id + 1)) {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true/*commit*/))) {
        LOG_WARN("failed to commit", K(ret));
      } else {
        LOG_INFO("succeed to get_job_id", K(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false/*commit*/)))) {
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupInfoManager::check_can_update_(
    const ObBackupInfoStatus::BackupStatus &src_status,
    const ObBackupInfoStatus::BackupStatus &dest_status)
{
  int ret = OB_SUCCESS;
  if (src_status > ObBackupInfoStatus::MAX
      || dest_status > ObBackupInfoStatus::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src or dest status is invalid", K(ret), K(src_status), K(dest_status));
  } else {
    switch (src_status) {
    case ObBackupInfoStatus::PREPARE :
      if (ObBackupInfoStatus::SCHEDULE != dest_status
          && ObBackupInfoStatus::CANCEL != dest_status
          && ObBackupInfoStatus::STOP != dest_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      }
      break;
    case ObBackupInfoStatus::SCHEDULE :
      if (ObBackupInfoStatus::SCHEDULE != dest_status
          && ObBackupInfoStatus::DOING != dest_status
          && ObBackupInfoStatus::CANCEL != dest_status
          && ObBackupInfoStatus::STOP != dest_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      }
      break;
    case ObBackupInfoStatus::DOING :
      if (ObBackupInfoStatus::DOING != dest_status
          && ObBackupInfoStatus::CLEANUP != dest_status
          && ObBackupInfoStatus::CANCEL != dest_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      }
      break;
    case ObBackupInfoStatus::CLEANUP :
      if (ObBackupInfoStatus::CLEANUP != dest_status
          && ObBackupInfoStatus::STOP != dest_status
          && ObBackupInfoStatus::CANCEL != dest_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      }
      break;
    case ObBackupInfoStatus::CANCEL :
      if (ObBackupInfoStatus::CANCEL != dest_status
          && ObBackupInfoStatus::CLEANUP != dest_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      }
      break;
    case ObBackupInfoStatus::STOP :
      if (ObBackupInfoStatus::STOP != dest_status
          && ObBackupInfoStatus::PREPARE != dest_status
          && ObBackupInfoStatus::SCHEDULE != dest_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      }
      break;
    case ObBackupInfoStatus::MAX :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not update backup info", K(ret), K(src_status), K(dest_status));
      break;
    }
  }
  return ret;
}

ObBackupInfoSimpleItem::ObBackupInfoSimpleItem()
  : name_(""), value_("")
{
}

ObBackupInfoSimpleItem::ObBackupInfoSimpleItem(const char *name, const char *value)
  : name_(name), value_(value)
{
}

ObBackupInfoChecker::ObBackupInfoChecker()
  : is_inited_(false),
    sql_proxy_(nullptr),
    inner_table_version_(OB_BACKUP_INNER_TABLE_VMAX)
{
}

ObBackupInfoChecker::~ObBackupInfoChecker()
{
}

int ObBackupInfoChecker::init(common::ObMySQLProxy *sql_proxy,
    const share::ObBackupInnerTableVersion &inner_table_version)
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
    inner_table_version_ = inner_table_version;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupInfoChecker::check(const common::ObIArray<uint64_t> &tenant_ids,
    share::ObIBackupLeaseService &backup_lease_service)
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
  } else if (OB_FAIL(get_new_items_(*sql_proxy_, tenant_id, false/*for update*/, new_items))) {
    LOG_WARN("failed to get new items", K(ret), K(tenant_id));
  } else if (new_items.empty()
      && (0 != status_line_count || inner_table_version_ > OB_BACKUP_INNER_TABLE_V1)) {
    // do no thing
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (0 == status_line_count && OB_BACKUP_INNER_TABLE_V1 == inner_table_version_) {
      if (OB_FAIL(insert_log_archive_status_(trans, tenant_id))) {
        LOG_WARN("failed to insert log archive srtatus", K(ret), K(tenant_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_new_items_(trans, tenant_id, true/*for update*/, new_items))) {
      LOG_WARN("failed to get new items", K(ret), K(tenant_id));
    } else if (OB_FAIL(insert_new_items_(trans, tenant_id, new_items))) {
      LOG_WARN("failed to insert new items", K(ret), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {//commit
        LOG_WARN("failed to commit", K(ret), K(tenant_id));
      } else {
        FLOG_INFO("succeed to add backup info items", K(ret), K(tenant_id), K(new_items));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans.end((false)))) {//rollback
        LOG_WARN("failed to rollback", K(ret), K(tmp_ret), K(tenant_id));
      }
    }
  }

  return ret;
}


int ObBackupInfoChecker::get_item_count_(const uint64_t tenant_id, int64_t &item_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    item_count = 0;
  
    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id=%lu and name not in ('%s')",
        share::OB_ALL_BACKUP_INFO_TNAME, tenant_id, BASE_BACKUP_VERSION_STR))) {
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

int ObBackupInfoChecker::get_status_line_count_(const uint64_t tenant_id, int64_t &status_count)
{
  int ret = OB_SUCCESS;

  // TODO: to remove.

#if 0
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    status_count = 0;
  
    if (OB_FAIL(sql.assign_fmt("select count(*) as count from %s where tenant_id=%lu ",
        share::OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME, tenant_id))) {
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
#endif
  return ret;
}

int ObBackupInfoChecker::get_new_items_(common::ObISQLClient &trans,
    const uint64_t tenant_id, const bool for_update,
    common::ObIArray<ObBackupInfoSimpleItem> &items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *for_update_str = for_update? "for update": "";

  SMART_VAR(ObMySQLProxy::ReadResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
    char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
    int real_length = 0;
    int64_t read_count = 0;
    items.reset();
  
    if (OB_FAIL(sql.assign_fmt("select name from %s where tenant_id=%lu %s",
        OB_ALL_BACKUP_INFO_TNAME, tenant_id, for_update_str))) {
      LOG_WARN("failed to init backup info sql", K(ret));
    } else if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else {
      const int64_t count = ARRAYSIZEOF(backup_info_item_list);
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const ObBackupInfoSimpleItem &item = backup_info_item_list[i];
        if (inner_table_version_ > OB_BACKUP_INNER_TABLE_V1
            && (0 == strcmp("log_archive_status", item.name_))) {
          // no need prepare these item for new version inner table
          // TODO(zeyong): remove backup_scheduler_leader after backup lease upgrade
        } else if (OB_FAIL(items.push_back(item))) {
          LOG_WARN("failed to add items", K(ret), K(i));
        }
      }
    }
  
    while(OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "result is NULL", K(ret), K(sql));
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
        LOG_DEBUG("unknown item", K(ret), K(sql), K(tenant_id),  K(name_str), K(items));
      }
    }
  }
  return ret;
}

int ObBackupInfoChecker::insert_new_items_(common::ObMySQLTransaction &trans,
    const uint64_t tenant_id, common::ObIArray<ObBackupInfoSimpleItem> &items)
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

int ObBackupInfoChecker::insert_new_item_(common::ObMySQLTransaction &trans,
    const uint64_t tenant_id, ObBackupInfoSimpleItem &item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (OB_FAIL(sql.assign_fmt("insert into %s(tenant_id, name, value) values(%lu, '%s', '%s')",
      OB_ALL_BACKUP_INFO_TNAME, tenant_id, item.name_, item.value_))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(item));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    FLOG_INFO("[LOG_ARCHIVE] insert_new_item_", K(sql));
  }

  return ret;
}


int ObBackupInfoChecker::insert_log_archive_status_(
    common::ObMySQLTransaction &trans, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // TODO: to remove.
#if 0
  ObSqlString sql;
  int64_t affected_rows = -1;

  if (OB_FAIL(sql.assign_fmt(
      "insert into %s(%s,%s,%s,%s,%s, %s) values(%lu, %ld, 0,usec_to_time(0), usec_to_time(0), 'STOP') ",
      OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME, OB_STR_TENANT_ID, OB_STR_INCARNATION,
      OB_STR_LOG_ARCHIVE_ROUND, OB_STR_MIN_FIRST_TIME, OB_STR_MAX_NEXT_TIME, OB_STR_STATUS,
      tenant_id, OB_START_INCARNATION))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    LOG_INFO("[LOG_ARCHIVE] insert_log_archive_status_", K(inner_table_version_), K(sql));
  }
#endif
  return ret;
}




