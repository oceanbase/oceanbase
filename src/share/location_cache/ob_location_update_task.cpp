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

#define USING_LOG_PREFIX SHARE_LOCATION

#include "share/location_cache/ob_location_update_task.h"
#include "share/location_cache/ob_ls_location_service.h"
#include "share/location_cache/ob_tablet_ls_service.h"

namespace oceanbase
{
using namespace common;
namespace share
{
int ObLSLocationUpdateTask::init(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const bool renew_for_tenant,
    const int64_t add_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_CLUSTER_ID == cluster_id
      || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else {
    cluster_id_ = cluster_id;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    renew_for_tenant_ = renew_for_tenant;
    add_timestamp_ = add_timestamp;
  }
  return ret;
}

void ObLSLocationUpdateTask::reset()
{
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  renew_for_tenant_ = false;
  add_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObLSLocationUpdateTask::is_valid() const
{
  return OB_INVALID_CLUSTER_ID != cluster_id_
      && !is_virtual_tenant_id(tenant_id_)
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && OB_INVALID_TIMESTAMP != add_timestamp_;
}

int ObLSLocationUpdateTask::assign(const ObLSLocationUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    cluster_id_ = other.cluster_id_;
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    renew_for_tenant_ = other.renew_for_tenant_;
    add_timestamp_ = other.add_timestamp_;
  }
  return ret;
}

int64_t ObLSLocationUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  hash_val = murmurhash(&renew_for_tenant_, sizeof(renew_for_tenant_), hash_val);
  return hash_val;
}

bool ObLSLocationUpdateTask::operator ==(const ObLSLocationUpdateTask &other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (this == &other) { // same pointer
    equal = true;
  } else {
    equal = (cluster_id_ == other.cluster_id_
        && tenant_id_ == other.tenant_id_
        && ls_id_ == other.ls_id_
        && renew_for_tenant_ == other.renew_for_tenant_);
  }
  return equal;
}

bool ObLSLocationUpdateTask::operator !=(const ObLSLocationUpdateTask &other) const
{
  return !(*this == other);
}

bool ObLSLocationUpdateTask::compare_without_version(
    const ObLSLocationUpdateTask &other) const
{
  return (*this == other);
}

int ObLSLocationUpdateTask::assign_when_equal(
    const ObLSLocationUpdateTask &other)
{
  UNUSED(other);
  return OB_NOT_SUPPORTED;
}

int ObTabletLSUpdateTask::init(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t add_timestamp)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  tablet_id_ = tablet_id;
  add_timestamp_ = add_timestamp;
  return ret;
}

int ObTabletLSUpdateTask::assign(const ObTabletLSUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    tablet_id_ = other.tablet_id_;
    add_timestamp_ = other.add_timestamp_;
  }
  return ret;
}

void ObTabletLSUpdateTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_id_.reset();
  add_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObTabletLSUpdateTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && tablet_id_.is_valid()
      && OB_INVALID_TIMESTAMP != add_timestamp_;
}

int64_t ObTabletLSUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

bool ObTabletLSUpdateTask::operator ==(const ObTabletLSUpdateTask &other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (this == &other) { // same pointer
    equal = true;
  } else {
    equal = (tenant_id_ == other.tenant_id_
        && tablet_id_ == other.tablet_id_);
  }
  return equal;
}

bool ObTabletLSUpdateTask::operator !=(const ObTabletLSUpdateTask &other) const
{
  return !(*this == other);
}

bool ObTabletLSUpdateTask::compare_without_version(
    const ObTabletLSUpdateTask &other) const
{
  return (*this == other);
}

int ObTabletLSUpdateTask::assign_when_equal(
    const ObTabletLSUpdateTask &other)
{
  UNUSED(other);
  return OB_NOT_SUPPORTED;
}

ObLSLocationTimerTask::ObLSLocationTimerTask(
    ObLSLocationService &ls_loc_service)
    : ls_loc_service_(ls_loc_service)
{
}

void ObLSLocationTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_loc_service_.renew_all_ls_locations())) {
    LOG_WARN("fail to renew_all_ls_locations", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(ls_loc_service_.schedule_ls_timer_task())) {
    LOG_WARN("fail to schedule ls location timer task", KR(ret));
  }
}

ObLSLocationByRpcTimerTask::ObLSLocationByRpcTimerTask(
    ObLSLocationService &ls_loc_service)
    : ls_loc_service_(ls_loc_service)
{
}

void ObLSLocationByRpcTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_loc_service_.renew_all_ls_locations_by_rpc())) {
    LOG_WARN("fail to renew_all_ls_location by rpc", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(ls_loc_service_.schedule_ls_by_rpc_timer_task())) {
    LOG_WARN("fail to schedule ls location by rpc timer task", KR(ret));
  }
}

ObDumpLSLocationCacheTimerTask::ObDumpLSLocationCacheTimerTask(
    ObLSLocationService &ls_loc_service)
    : ls_loc_service_(ls_loc_service)
{
}

void ObDumpLSLocationCacheTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_loc_service_.dump_cache())) {
    LOG_WARN("fail to dump ls location cache", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(ls_loc_service_.schedule_dump_cache_timer_task())) {
    LOG_WARN("fail to schedule dump ls location cache timer task", KR(ret));
  }
}

int ObVTableLocUpdateTask::init(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t add_timestamp)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  table_id_ = table_id;
  add_timestamp_ = add_timestamp;
  return ret;
}

int ObVTableLocUpdateTask::assign(const ObVTableLocUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    add_timestamp_ = other.add_timestamp_;
  }
  return ret;
}

void ObVTableLocUpdateTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = OB_INVALID_ID;
  add_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObVTableLocUpdateTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && OB_INVALID_ID != table_id_
      && OB_INVALID_TIMESTAMP != add_timestamp_;
}

int64_t ObVTableLocUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  return hash_val;
}

bool ObVTableLocUpdateTask::operator ==(const ObVTableLocUpdateTask &other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (this == &other) { // same pointer
    equal = true;
  } else {
    equal = (tenant_id_ == other.tenant_id_
        && table_id_ == other.table_id_);
  }
  return equal;
}

bool ObVTableLocUpdateTask::operator !=(const ObVTableLocUpdateTask &other) const
{
  return !(*this == other);
}

bool ObVTableLocUpdateTask::compare_without_version(
    const ObVTableLocUpdateTask &other) const
{
  return (*this == other);
}

int ObVTableLocUpdateTask::assign_when_equal(
    const ObVTableLocUpdateTask &other)
{
  UNUSED(other);
  return OB_NOT_SUPPORTED;
}

ObClearTabletLSCacheTimerTask::ObClearTabletLSCacheTimerTask(
    ObTabletLSService &tablet_ls_service)
    : tablet_ls_service_(tablet_ls_service)
{
}

void ObClearTabletLSCacheTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_ls_service_.clear_expired_cache())) {
    LOG_WARN("fail to clear expired cache", KR(ret));
  }
}

} // end namespace share
} // end namespace oceanbase
