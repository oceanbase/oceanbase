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
#include "share/ob_zone_info.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_storage_format.h"
#include "share/ob_zone_table_operation.h"
#include "share/config/ob_server_config.h"
#include "common/ob_zone_type.h"

namespace oceanbase
{
using namespace common;
namespace share
{

ObZoneInfoItem::ObZoneInfoItem(ObZoneInfoItem::ItemList &list,
                               const char *name, int64_t value, const char *info)
    : name_(name), value_(value), info_(info)
{
  list.add_last(this);
}

ObZoneInfoItem::ObZoneInfoItem(const ObZoneInfoItem &item)
    : name_(item.name_), value_(item.value_), info_(item.info_)
{
}

ObZoneInfoItem &ObZoneInfoItem::operator =(const ObZoneInfoItem &item)
{
  name_ = item.name_;
  value_ = item.value_;
  info_ = item.info_;
  return *this;
}

int ObZoneInfoItem::update(common::ObISQLClient &sql_client, const common::ObZone &zone,
                           const int64_t value, const Info &info)
{
  ObZoneInfoItem tmp = *this;
  tmp.value_ = value;
  tmp.info_ = info;
  int ret = OB_SUCCESS;
  // %zone can be empty for global info
  // %value and %info can be arbitrary values.
  if (!tmp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tmp));
  } else if (OB_FAIL(ObZoneTableOperation::update_info_item(sql_client, zone, tmp))) {
    LOG_WARN("update item failed", K(ret), K(zone), "item", tmp);
  } else {
    *this = tmp;
  }
  return ret;
}

int ObZoneInfoItem::update(common::ObISQLClient &sql_client, const common::ObZone &zone,
                           const int64_t value)
{
  // %zone can be empty
  // %value can be arbitrary values.
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else if (OB_FAIL(update(sql_client, zone, value, ""))) {
    LOG_WARN("update item failed", K(ret), K(zone), K(value));
  }
  return ret;
}

int ObZoneInfoItem::update(ObZoneItemTransUpdater &updater, const common::ObZone &zone,
                           const int64_t value, const Info &info)
{
  int ret = OB_SUCCESS;
  // %zone can be empty for global info
  // %value and %info can be arbitrary values.
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else if (OB_FAIL(updater.update(zone, *this, value, info))) {
    LOG_WARN("update item failed", K(ret), K(zone), K(value), K(info));
  }
  return ret;
}

int ObZoneInfoItem::update(ObZoneItemTransUpdater &updater, const common::ObZone &zone,
                           const int64_t value)
{
  // %zone can be empty
  // %value can be arbitrary values.
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else if (OB_FAIL(update(updater, zone, value, ""))) {
    LOG_WARN("update item failed", K(ret), K(zone), K(value));
  }
  return ret;
}

ObZoneItemTransUpdater::ObZoneItemTransUpdater()
    : started_(false), success_(false), alloc_(ObModIds::OB_ZONE_TABLE_UPDATER)
{
}

ObZoneItemTransUpdater::~ObZoneItemTransUpdater()
{
  if (started_) {
    int ret = end(success_);
    if (OB_FAIL(ret)) {
      LOG_WARN("end transaction failed", K(ret));
    }
  }
}

int ObZoneItemTransUpdater::start(ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (started_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transaction already started", K(ret));
  } else if (OB_FAIL(trans_.start(&sql_proxy, OB_SYS_TENANT_ID))) {
    LOG_WARN("start transaction failed", K(ret));
    started_ = false;
  } else {
    started_ = true;
    success_ = true;
  }
  return ret;
}

int ObZoneItemTransUpdater::update(const ObZone &zone, ObZoneInfoItem &item,
                                   const int64_t value, const ObZoneInfoItem::Info &info)
{
  int ret = OB_SUCCESS;
  void *mem = NULL;
  // %value and %info can be arbitrary values.
  if (!started_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transaction not started", K(ret));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (NULL == (mem = alloc_.alloc(sizeof(ObZoneInfoItem) + PTR_OFFSET))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret), "size", sizeof(ObZoneInfoItem) + PTR_OFFSET);
  } else {
    ObZoneInfoItem tmp = item;
    tmp.value_ = value;
    tmp.info_ = info;
    if (OB_FAIL(ObZoneTableOperation::update_info_item(trans_, zone, tmp))) {
      LOG_WARN("update zone item failed", K(ret), K(zone), "item", tmp);
      success_ = false;
      alloc_.free(mem);
      mem = NULL;
    } else {
      ObZoneInfoItem **ptr = static_cast<ObZoneInfoItem **>(mem);
      *ptr = &item;
      new(static_cast<char *>(mem) + PTR_OFFSET)ObZoneInfoItem(
          list_, item.name_, item.value_, item.info_.ptr());
      item = tmp;
    }
  }
  return ret;
}

int ObZoneItemTransUpdater::end(const bool commit)
{
  int ret = OB_SUCCESS;
  // can be end if ::start() fail, do not check started_ here.
  if (started_) {
    bool rollback_item_value = !commit;
    if (OB_FAIL(trans_.end(commit))) {
      LOG_WARN("transaction end failed", K(ret), K(commit));
      rollback_item_value = true;
    }
    if (rollback_item_value) {
      // store item to backup value
      int tmp_ret = OB_SUCCESS; // do not overwrite ret
      while (OB_SUCCESS == tmp_ret && !list_.is_empty()) {
        ObZoneInfoItem *item = list_.get_last();
        if (NULL == item) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null item", K(tmp_ret));
        } else {
          ObZoneInfoItem **ptr = reinterpret_cast<ObZoneInfoItem **>(
                                     reinterpret_cast<char *>(item) - PTR_OFFSET);
          if (NULL == ptr || NULL == *ptr) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null ptr or *ptr", K(tmp_ret), KP(ptr));
          } else {
            **ptr = *item;
            list_.remove_last();
          }
        }
      }
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
    list_.reset();
    started_ = false;
  }
  return ret;
}

#define INIT_ITEM(field, int_def, info_def) field##_(list_, #field, int_def, info_def)

// NOTE: c++11 delegating constructor can be used to avoid this macro
#define CONSTRUCT_GLOBAL_INFO() \
    INIT_ITEM(cluster, 0, GCONF.cluster),               \
    INIT_ITEM(privilege_version, 0, ""),                \
    INIT_ITEM(config_version, 0, ""),                   \
    INIT_ITEM(lease_info_version, 0, ""),               \
    INIT_ITEM(time_zone_info_version, 0, ""),           \
    INIT_ITEM(storage_format_version, (OB_STORAGE_FORMAT_VERSION_MAX - 1), "")

ObGlobalInfo::ObGlobalInfo()
    : CONSTRUCT_GLOBAL_INFO()
{
}

ObGlobalInfo::ObGlobalInfo(const ObGlobalInfo &other)
    : CONSTRUCT_GLOBAL_INFO()
{
  *this = other;
}

#undef CONSTRUCT_GLOBAL_INFO

ObGlobalInfo &ObGlobalInfo::operator = (const ObGlobalInfo &other)
{
  int ret = OB_SUCCESS;
  ObZoneInfoItem *it = list_.get_first();
  const ObZoneInfoItem *o_it = other.list_.get_first();
  while (OB_SUCCESS == ret && it != list_.get_header() && o_it != other.list_.get_header()) {
    if (NULL == it || NULL == o_it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null item", K(ret), KP(it), KP(o_it));
    } else {
      *it = *o_it;
      it = it->get_next();
      o_it = o_it->get_next();
    }
  }
  return *this;
}

void ObGlobalInfo::reset()
{
  this->~ObGlobalInfo();
  new(this) ObGlobalInfo();
}

DEF_TO_STRING(ObGlobalInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  DLIST_FOREACH_NORET(it, list_) {
    J_KO(it->name_, *it);
  }
  J_OBJ_END();
  return pos;
}

#define CONSTRUCT_ZONE_INFO() \
  INIT_ITEM(status, ObZoneStatus::INACTIVE, ObZoneStatus::get_status_str(ObZoneStatus::INACTIVE)),\
  INIT_ITEM(region, 0, DEFAULT_REGION_NAME),                 \
  INIT_ITEM(idc, 0, ""),                                     \
  INIT_ITEM(zone_type, ObZoneType::ZONE_TYPE_READWRITE, zone_type_to_str(ObZoneType::ZONE_TYPE_READWRITE)), \
  INIT_ITEM(recovery_status, \
            ObZoneInfo::RECOVERY_STATUS_NORMAL, \
            ObZoneInfo::get_recovery_status_str(ObZoneInfo::RECOVERY_STATUS_NORMAL)), \
  INIT_ITEM(storage_type, ObZoneInfo::STORAGE_TYPE_LOCAL, \
            ObZoneInfo::get_storage_type_str(ObZoneInfo::STORAGE_TYPE_LOCAL))

ObZoneInfo::ObZoneInfo() : zone_(), CONSTRUCT_ZONE_INFO()
{
}

ObZoneInfo::ObZoneInfo(const ObZoneInfo &other) : CONSTRUCT_ZONE_INFO()
{
  *this = other;
}

#undef CONSTRUCT_ZONE_INFO
#undef INIT_ITEM

ObZoneInfo &ObZoneInfo::operator = (const ObZoneInfo &other)
{
  int ret = OB_SUCCESS;

  zone_ = other.zone_;

  ObZoneInfoItem *it = list_.get_first();
  const ObZoneInfoItem *o_it = other.list_.get_first();
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

void ObZoneInfo::reset()
{
  this->~ObZoneInfo();
  new(this) ObZoneInfo();
}

DEF_TO_STRING(ObZoneInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KO("zone", zone_);
  J_KO("region", region_);
  J_KO("status", status_);
  J_KO("idc", idc_);
  J_KO("zone_type", zone_type_);
  J_KO("recovery_status", recovery_status_);
  J_KO("storage_type", storage_type_);

  DLIST_FOREACH_NORET(it, list_) {
    J_KO(it->name_, *it);
  }
  J_OBJ_END();
  return pos;
}

int ObZoneInfo::get_region(common::ObRegion &region) const
{
  int ret = OB_SUCCESS;
  region = DEFAULT_REGION_NAME;
  if (OB_FAIL(region.assign(region_.info_.ptr()))) {
    LOG_WARN("get_region assign failed", K(ret));
  }
  return ret;
}

bool ObGlobalInfo::is_valid() const
{
  bool is_valid = true;
  if (privilege_version_ < 0 || config_version_ < 0) {
    is_valid = false;
  }
  return is_valid;
}

const char *ObZoneInfo::get_status_str() const
{
  return ObZoneStatus::get_status_str(static_cast<ObZoneStatus::Status>(status_.value_));
}

const char *ObZoneInfo::get_recovery_status_str(const RecoveryStatus status)
{
  const char *str = nullptr;
  const char *str_array[] = {"NORMAL", "SUSPEND"};
  STATIC_ASSERT(RECOVERY_STATUS_MAX == ARRAYSIZEOF(str_array), "status count mismatch");
  if (status < RECOVERY_STATUS_NORMAL || status >= RECOVERY_STATUS_MAX) {
    str = nullptr;
  } else {
    str = str_array[status];
  }
  return str;
}

ObZoneInfo::RecoveryStatus ObZoneInfo::get_recovery_status(const char* status_str)
{
  ObZoneInfo::RecoveryStatus status = RECOVERY_STATUS_MAX;
  const char *str_array[] = {"NORMAL", "SUSPEND"};
  STATIC_ASSERT(RECOVERY_STATUS_MAX == ARRAYSIZEOF(str_array), "status count mismatch");
  for (int64_t i = 0; i < ARRAYSIZEOF(str_array); i++) {
    if (0 == STRCMP(str_array[i], status_str)) {
      status = static_cast<RecoveryStatus>(i);
    }
  }
  return status;
}

const char *ObZoneInfo::get_storage_type_str(const ObZoneInfo::StorageType storage_type)
{
  const char *str = nullptr;
  const char *str_array[] = {"LOCAL"};
  STATIC_ASSERT(STORAGE_TYPE_MAX == ARRAYSIZEOF(str_array), "storage type count mismatch");
  if (storage_type < STORAGE_TYPE_LOCAL || storage_type >= STORAGE_TYPE_MAX) {
    str = nullptr;
  } else {
    str = str_array[storage_type];
  }
  return str;
}

ObZoneInfo::StorageType ObZoneInfo::get_storage_type(const char* storage_type_str)
{
  ObZoneInfo::StorageType storage_type = STORAGE_TYPE_MAX;
  const char *str_array[] = {"LOCAL"};
  STATIC_ASSERT(STORAGE_TYPE_MAX == ARRAYSIZEOF(str_array), "storage type count mismatch");
  for (int64_t i = 0; i < ARRAYSIZEOF(str_array); i++) {
    if (0 == STRCMP(str_array[i], storage_type_str)) {
      storage_type = static_cast<StorageType>(i);
    }
  }
  return storage_type;
}

bool ObZoneInfo::is_valid() const
{
  bool is_valid = true;
  if (zone_.is_empty() || status_ < ObZoneStatus::INACTIVE || status_ >= ObZoneStatus::UNKNOWN
      || region_.info_.is_empty()) {
    is_valid = false;
  }
  return is_valid;
}

bool ObZoneInfo::can_switch_to_leader_while_daily_merge() const
{
  bool bret = true;
  // ObZoneStatus::Status inactive_status = ObZoneStatus::INACTIVE;
  // if (status_ == inactive_status) {
  //   bret = false;  //stop zone
  // } else if (is_in_merge()) {
  //   bret = false;  //in merging
  // }
  return bret;
}

} // end namespace share
} // end namespace oceanbase
