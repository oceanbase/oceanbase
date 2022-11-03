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
#include "share/ob_zone_merge_info.h"
#include "share/ob_zone_merge_table_operator.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_storage_format.h"
#include "share/config/ob_server_config.h"
#include "common/ob_zone_type.h"

namespace oceanbase
{
namespace share
{
template <typename T>
T first_param_percnetage(const T first, const T second)
{
  T percentage = 100;
  const T sum = first + second;
  if (0 != sum) {
    percentage = first * 100 / sum;
  }
  return percentage;
}
///////////////////////////////////////////////////////////////////////////////
ObMergeInfoItem::ObMergeInfoItem(
    ObMergeInfoItem::ItemList &list,
    const char *name,
    int64_t value,
    const bool need_update)
    : name_(name), value_(value < 0 ? 0 : value), need_update_(need_update)
{
  list.add_last(this);
}

ObMergeInfoItem::ObMergeInfoItem(const ObMergeInfoItem &item)
    : name_(item.name_), value_(item.value_), need_update_(item.need_update_)
{
}

void ObMergeInfoItem::assign_value(const ObMergeInfoItem &item)
{
  name_ = item.name_;
  value_ = item.value_;
}

ObMergeInfoItem &ObMergeInfoItem::operator =(const ObMergeInfoItem &item)
{
  name_ = item.name_;
  value_ = item.value_;
  return *this;
}
///////////////////////////////////////////////////////////////////////////////
#define INIT_ITEM(field, int_def, update_def) field##_(list_, #field, int_def, update_def)
///////////////////////////////////////////////////////////////////////////////
#define CONSTRUCT_ZONE_MERGE_INFO() \
  INIT_ITEM(is_merging, 0, false),                              \
  INIT_ITEM(broadcast_scn, 1, false),                       \
  INIT_ITEM(last_merged_scn, OB_MERGED_VERSION_INIT, false),\
  INIT_ITEM(last_merged_time, 0, false),                        \
  INIT_ITEM(all_merged_scn, 1, false),                      \
  INIT_ITEM(merge_start_time, 0, false),                        \
  INIT_ITEM(merge_status, 0, false),                            \
  INIT_ITEM(frozen_scn, 1, false)

ObZoneMergeInfo::ObZoneMergeInfo()
  : tenant_id_(OB_INVALID_TENANT_ID), 
    zone_(),
    CONSTRUCT_ZONE_MERGE_INFO(),
    start_merge_fail_times_(0)
{
}

int ObZoneMergeInfo::assign(const ObZoneMergeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    zone_ = other.zone_;
    start_merge_fail_times_ = other.start_merge_fail_times_;
    ObMergeInfoItem *it = list_.get_first();
    const ObMergeInfoItem *o_it = other.list_.get_first();
    while (OB_SUCC(ret) && (it != list_.get_header()) && (o_it != other.list_.get_header())) {
      if (NULL == it || NULL == o_it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null item", KR(ret), KP(it), KP(o_it));
      } else {
        *it = *o_it;
        it = it->get_next();
        o_it = o_it->get_next();
      }
    }
  }
  return ret;
} 

int ObZoneMergeInfo::assign_value(
    const ObZoneMergeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    zone_ = other.zone_;
    start_merge_fail_times_ = other.start_merge_fail_times_;
    ObMergeInfoItem *it = list_.get_first();
    const ObMergeInfoItem *o_it = other.list_.get_first();
    while (OB_SUCC(ret) && (it != list_.get_header()) && (o_it != other.list_.get_header())) {
      if (NULL == it || NULL == o_it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null item", KR(ret), KP(it), KP(o_it));
      } else {
        it->assign_value(*o_it);
        it = it->get_next();
        o_it = o_it->get_next();
      }
    }
  }
  return ret;
} 

void ObZoneMergeInfo::reset()
{
  this->~ObZoneMergeInfo();
  new(this) ObZoneMergeInfo();
}

const char *ObZoneMergeInfo::get_merge_status_str(const MergeStatus status)
{
  const char *str = "UNKNOWN";
  const char *str_array[] = { "IDLE", "MERGING", "CHECKSUM" };
  STATIC_ASSERT(MERGE_STATUS_MAX == ARRAYSIZEOF(str_array), "status count mismatch");
  if (status < 0 || status >= MERGE_STATUS_MAX) {
    LOG_WARN("invalid merge status", K(status));
  } else {
    str = str_array[status];
  }
  return str;
}

ObZoneMergeInfo::MergeStatus ObZoneMergeInfo::get_merge_status(const char* merge_status_str)
{
  ObZoneMergeInfo::MergeStatus status = MERGE_STATUS_MAX;
  const char *str_array[] = { "IDLE", "MERGING", "CHECKSUM" };
  STATIC_ASSERT(MERGE_STATUS_MAX == ARRAYSIZEOF(str_array), "status count mismatch");
  for (int64_t i = 0; i < ARRAYSIZEOF(str_array); i++) {
    if (0 == STRCMP(str_array[i], merge_status_str)) {
      status = static_cast<MergeStatus>(i);
    }
  }
  return status;
}

bool ObZoneMergeInfo::is_merged(const int64_t broadcast_version) const
{
  return (broadcast_version == broadcast_scn_)
         && (broadcast_scn_ == last_merged_scn_);
}

bool ObZoneMergeInfo::is_valid() const
{
  bool is_valid = true;
  if ((broadcast_scn_ < 0) 
     || (last_merged_scn_ < 0) 
     || (last_merged_time_ < 0) 
     || (merge_start_time_ < 0) 
     || (all_merged_scn_ < 0)) {
    is_valid = false;
  }
  return is_valid;
}

bool ObZoneMergeInfo::is_in_merge() const
{
  return is_merging_;
}

///////////////////////////////////////////////////////////////////////////////
#define CONSTRUCT_GLOBAL_MERGE_INFO() \
  INIT_ITEM(cluster, 0, false),                     \
  INIT_ITEM(frozen_scn, 1, false),             \
  INIT_ITEM(global_broadcast_scn, 1, false),    \
  INIT_ITEM(last_merged_scn, 1, false),         \
  INIT_ITEM(is_merge_error, 0, false),              \
  INIT_ITEM(merge_status, 0, false),                \
  INIT_ITEM(error_type, 0, false),                  \
  INIT_ITEM(suspend_merging, 0, false),             \
  INIT_ITEM(merge_start_time, 0, false),            \
  INIT_ITEM(last_merged_time, 0, false)

ObGlobalMergeInfo::ObGlobalMergeInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    CONSTRUCT_GLOBAL_MERGE_INFO()
{
}

void ObGlobalMergeInfo::reset()
{
  this->~ObGlobalMergeInfo();
  new(this) ObGlobalMergeInfo();
}

bool ObGlobalMergeInfo::is_last_merge_complete() const
{
  return last_merged_scn_ == global_broadcast_scn_;
}

bool ObGlobalMergeInfo::is_in_merge() const
{
  return (last_merged_scn_ != global_broadcast_scn_)
         && (!suspend_merging_);
}

bool ObGlobalMergeInfo::is_merge_error() const
{
  return (is_merge_error_ > 0);
}

bool ObGlobalMergeInfo::is_in_verifying_status() const
{
  return (ObZoneMergeInfo::MERGE_STATUS_VERIFYING == merge_status_);
}

bool ObGlobalMergeInfo::is_valid() const
{
  bool is_valid = true;
  if ((OB_INVALID_TENANT_ID == tenant_id_)
      || (frozen_scn_ < 0)
      || (global_broadcast_scn_ < 0)
      || (last_merged_scn_ < 0)) {
    is_valid = false;
  }
  return is_valid;
}

int ObGlobalMergeInfo::assign(
    const ObGlobalMergeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ObMergeInfoItem *it = list_.get_first();
    const ObMergeInfoItem *o_it = other.list_.get_first();
    while ((it != list_.get_header()) && (o_it != other.list_.get_header())) {
      if (NULL == it || NULL == o_it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null item", KR(ret), KP(it), KP(o_it));
      } else {
        *it = *o_it;
        it = it->get_next();
        o_it = o_it->get_next();
      }
    }
  }
  return ret;
}

int ObGlobalMergeInfo::assign_value(
    const ObGlobalMergeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ObMergeInfoItem *it = list_.get_first();
    const ObMergeInfoItem *o_it = other.list_.get_first();
    while ((it != list_.get_header()) && (o_it != other.list_.get_header())) {
      if (NULL == it || NULL == o_it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null item", KR(ret), KP(it), KP(o_it));
      } else {
        it->assign_value(*o_it);
        it = it->get_next();
        o_it = o_it->get_next();
      }
    }
  }
  return ret;
}
///////////////////////////////////////////////////////////////////////////////

int64_t ObMergeProgress::get_merged_tablet_percentage() const
{
  return first_param_percnetage(merged_tablet_cnt_, unmerged_tablet_cnt_);
}

int64_t ObMergeProgress::get_merged_data_percentage() const
{
  return first_param_percnetage(merged_data_size_, unmerged_data_size_);
}

} // end namespace share
} // end namespace oceanbase
