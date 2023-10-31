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
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_column_schema.h"

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
    const SCN &scn,
    const bool need_update)
    : name_(name), is_scn_(true), scn_(scn),
      value_(-1), need_update_(need_update)
{
  list.add_last(this);
}

ObMergeInfoItem::ObMergeInfoItem(
    ObMergeInfoItem::ItemList &list,
    const char *name,
    const int64_t value,
    const bool need_update)
    : name_(name), is_scn_(false), scn_(),
      value_(value), need_update_(need_update)
{
  list.add_last(this);
}

ObMergeInfoItem::ObMergeInfoItem(const ObMergeInfoItem &item)
    : name_(item.name_), is_scn_(item.is_scn_), scn_(),
      value_(item.value_), need_update_(item.need_update_)
{
  scn_ = item.scn_;
}

bool ObMergeInfoItem::is_valid() const
{
  bool is_valid = false;
  if (is_scn_) {
    is_valid = scn_.is_valid();
  } else {
    is_valid = (value_ >= 0);
  }
  return is_valid && (NULL != name_);
}

void ObMergeInfoItem::assign_value(const ObMergeInfoItem &item)
{
  name_ = item.name_;
  is_scn_ = item.is_scn_;
  scn_ = item.scn_;
  value_ = item.value_;
}

void ObMergeInfoItem::set_val(const int64_t value, const bool need_update)
{
  is_scn_ = false;
  value_ = ((value < 0) ? 0: value);
  need_update_ = need_update;
}

void ObMergeInfoItem::set_scn(const SCN &scn, const bool need_update)
{
  is_scn_ = true;
  scn_ = scn;
  need_update_ = need_update;
}

int ObMergeInfoItem::set_scn(const uint64_t scn_val)
{
  int ret = OB_SUCCESS;
  is_scn_ = true;
  if (OB_FAIL(scn_.convert_for_inner_table_field(scn_val))) {
    LOG_WARN("fail to convert uint64_t to SCN", KR(ret), K(scn_val));
  }
  return ret;
}

int ObMergeInfoItem::set_scn(const uint64_t scn_val, const bool need_update)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_scn(scn_val))) {
    LOG_WARN("fail to set scn val", KR(ret), K(scn_val));
  } else {
    need_update_ = need_update;
  }
  return ret;
}

ObMergeInfoItem &ObMergeInfoItem::operator =(const ObMergeInfoItem &item)
{
  name_ = item.name_;
  is_scn_ = item.is_scn_;
  scn_ = item.scn_;
  value_ = item.value_;
  need_update_ = item.need_update_;
  return *this;
}
///////////////////////////////////////////////////////////////////////////////
#define INIT_VAL_ITEM(field, int_def, update_def) field##_(list_, #field, int_def, update_def)
#define INIT_SCN_ITEM(field, scn_def, update_def) field##_(list_, #field, scn_def, update_def)
///////////////////////////////////////////////////////////////////////////////
#define CONSTRUCT_ZONE_MERGE_INFO() \
  INIT_VAL_ITEM(is_merging, 0, false),                       \
  INIT_SCN_ITEM(broadcast_scn, SCN::base_scn(), false),      \
  INIT_SCN_ITEM(last_merged_scn, SCN::base_scn(), false),    \
  INIT_VAL_ITEM(last_merged_time, 0, false),                 \
  INIT_SCN_ITEM(all_merged_scn, SCN::base_scn(), false),     \
  INIT_VAL_ITEM(merge_start_time, 0, false),                 \
  INIT_VAL_ITEM(merge_status, 0, false),                     \
  INIT_SCN_ITEM(frozen_scn, SCN::base_scn(), false)

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

bool ObZoneMergeInfo::is_valid() const
{
  bool is_valid = true;
  if ((!broadcast_scn_.is_valid())
     || (!last_merged_scn_.is_valid())
     || (last_merged_time_.get_value() < 0)
     || (merge_start_time_.get_value() < 0)
     || (!all_merged_scn_.is_valid())
     || (!frozen_scn_.is_valid())) {
    is_valid = false;
  }
  return is_valid;
}

///////////////////////////////////////////////////////////////////////////////
#define CONSTRUCT_GLOBAL_MERGE_INFO() \
  INIT_VAL_ITEM(cluster, 0, false),                              \
  INIT_SCN_ITEM(frozen_scn, SCN::base_scn(), false),             \
  INIT_SCN_ITEM(global_broadcast_scn, SCN::base_scn(), false),   \
  INIT_SCN_ITEM(last_merged_scn, SCN::base_scn(), false),        \
  INIT_VAL_ITEM(is_merge_error, 0, false),                       \
  INIT_VAL_ITEM(merge_status, 0, false),                         \
  INIT_VAL_ITEM(error_type, 0, false),                           \
  INIT_VAL_ITEM(suspend_merging, 0, false),                      \
  INIT_VAL_ITEM(merge_start_time, 0, false),                     \
  INIT_VAL_ITEM(last_merged_time, 0, false)

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
  return last_merged_scn() == global_broadcast_scn();
}

bool ObGlobalMergeInfo::is_in_merge() const
{
  return last_merged_scn() != global_broadcast_scn();
}

bool ObGlobalMergeInfo::is_merge_error() const
{
  return (is_merge_error_.get_value() > 0);
}

bool ObGlobalMergeInfo::is_in_verifying_status() const
{
  return (ObZoneMergeInfo::MERGE_STATUS_VERIFYING == merge_status_.get_value());
}

bool ObGlobalMergeInfo::is_valid() const
{
  bool is_valid = true;
  if ((OB_INVALID_TENANT_ID == tenant_id_)
      || (!frozen_scn_.is_valid())
      || (!global_broadcast_scn_.is_valid())
      || (!last_merged_scn_.is_valid())) {
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

} // end namespace share
} // end namespace oceanbase
