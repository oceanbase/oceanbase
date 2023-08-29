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

#include "share/location_cache/ob_location_struct.h"
#include "share/config/ob_server_config.h" // GCONF
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/net/ob_addr.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
OB_SERIALIZE_MEMBER(ObLSReplicaLocation,
    server_,
    role_,
    sql_port_,
    replica_type_,
    property_,
    restore_status_,
    proposal_id_);

OB_SERIALIZE_MEMBER(ObLSLocationCacheKey,
    cluster_id_,
    tenant_id_,
    ls_id_);

OB_SERIALIZE_MEMBER(ObLSLocation,
    cache_key_,
    replica_locations_,
    renew_time_);

OB_SERIALIZE_MEMBER(ObTabletLocation,
    tenant_id_,
    tablet_id_,
    replica_locations_,
    renew_time_);

OB_SERIALIZE_MEMBER(ObTabletLSKey,
    tenant_id_,
    tablet_id_);

OB_SERIALIZE_MEMBER(ObTabletLSCache,
    cache_key_,
    ls_id_,
    renew_time_);

ObLSReplicaLocation::ObLSReplicaLocation()
    : server_(),
      role_(FOLLOWER),
      sql_port_(OB_INVALID_INDEX),
      replica_type_(REPLICA_TYPE_FULL),
      property_(),
      restore_status_(),
      proposal_id_(OB_INVALID_ID)
{
}

void ObLSReplicaLocation::reset()
{
  server_.reset();
  role_ = FOLLOWER;
  sql_port_ = OB_INVALID_INDEX;
  replica_type_ = REPLICA_TYPE_FULL;
  property_.reset();
  restore_status_ = ObLSRestoreStatus::Status::RESTORE_NONE;
  proposal_id_ = OB_INVALID_ID;
}

bool ObLSReplicaLocation::is_valid() const
{
  return server_.is_valid()
      && OB_INVALID_INDEX != sql_port_
      && ObReplicaTypeCheck::is_replica_type_valid(replica_type_)
      && property_.is_valid()
      && proposal_id_ >= 0;
}

bool ObLSReplicaLocation::operator==(const ObLSReplicaLocation &other) const
{
  return server_ == other.server_
      && role_ == other.role_
      && sql_port_ == other.sql_port_
      && replica_type_ == other.replica_type_
      && property_ == other.property_
      && restore_status_ == other.restore_status_
      && proposal_id_ == other.proposal_id_;
}

bool ObLSReplicaLocation::operator!=(const ObLSReplicaLocation &other) const
{
  return !(*this == other);
}

int ObLSReplicaLocation::assign(const ObLSReplicaLocation &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    server_ = other.server_;
    role_ = other.role_;
    sql_port_ = other.sql_port_;
    replica_type_ = other.replica_type_;
    property_ = other.property_;
    restore_status_ = other.restore_status_;
    proposal_id_ = other.proposal_id_;
  }
  return ret;
}

int ObLSReplicaLocation::init(
    const common::ObAddr &server,
    const common::ObRole &role,
    const int64_t &sql_port,
    const common::ObReplicaType &replica_type,
    const common::ObReplicaProperty &property,
    const ObLSRestoreStatus &restore_status,
    const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid()
      || OB_INVALID_INDEX == sql_port
      || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)
      || !property.is_valid()
      || proposal_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObLSReplicaLocation init failed", KR(ret),
             K(server), K(role), K(sql_port), K(replica_type), K(property),
             K(restore_status), K(proposal_id));
  } else {
    server_ = server;
    role_ = role;
    sql_port_ = sql_port;
    replica_type_ = replica_type;
    property_ = property;
    restore_status_ = restore_status;
    proposal_id_ = proposal_id;
  }
  return ret;
}

int ObLSReplicaLocation::init_without_check(
    const common::ObAddr &server,
    const common::ObRole &role,
    const int64_t &sql_port,
    const common::ObReplicaType &replica_type,
    const common::ObReplicaProperty &property,
    const ObLSRestoreStatus &restore_status,
    const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  server_ = server;
  role_ = role;
  sql_port_ = sql_port;
  replica_type_ = replica_type;
  property_ = property;
  restore_status_ = restore_status;
  proposal_id_ = proposal_id;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSLeaderLocation,
    key_,
    location_);

int ObLSLeaderLocation::init(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID ls_id,
    const common::ObAddr &server,
    const common::ObRole &role,
    const int64_t &sql_port,
    const common::ObReplicaType &replica_type,
    const common::ObReplicaProperty &property,
    const ObLSRestoreStatus &restore_status,
    const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_.init(cluster_id, tenant_id, ls_id))) {
    LOG_WARN("fail to init key", KR(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(location_.init(
    server, role, sql_port, replica_type, property, restore_status, proposal_id))) {
    LOG_WARN("fail to init key", KR(ret), K(server), K(role), K(sql_port),
             K(replica_type), K(property), K(restore_status), K(proposal_id));
  }
  return ret;
}

int ObLSLeaderLocation::assign(const ObLSLeaderLocation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_.assign(other.key_))) {
    LOG_WARN("fail to assign key", KR(ret), K(other));
  } else if (OB_FAIL(location_.assign(other.location_))) {
    LOG_WARN("fail to assign location", KR(ret), K(other));
  }
  return ret;
}

void ObLSLeaderLocation::reset()
{
  key_.reset();
  location_.reset();
}

bool ObLSLeaderLocation::is_valid() const
{
  return key_.is_valid() && location_.is_valid();
}

ObLSLocation::ObLSLocation()
    : ObLink(),
      cache_key_(),
      renew_time_(0),
      last_access_ts_(0),
      replica_locations_()
{
}

ObLSLocation::ObLSLocation(common::ObIAllocator &allocator)
    : ObLink(),
      cache_key_(),
      renew_time_(0),
      last_access_ts_(0),
      replica_locations_(
          common::OB_MALLOC_NORMAL_BLOCK_SIZE,
          common::ModulePageAllocator(allocator))
{
}

ObLSLocation::~ObLSLocation()
{
}

int64_t ObLSLocation::size() const
{
  return sizeof(*this) + replica_locations_.get_data_size();
}

int ObLSLocation::deep_copy(const ObLSLocation &ls_location)
{
 int ret = OB_SUCCESS;
 if (this != &ls_location) {
   this->cache_key_ = ls_location.cache_key_;
   this->renew_time_ = ls_location.renew_time_;
   if (OB_FAIL(this->replica_locations_.assign(ls_location.replica_locations_))) {
     LOG_WARN("ls location deep copy error", K(ret), K(*this), K(ls_location));
   }
 }
 return ret;
}

int ObLSLocation::init(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t renew_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id) || renew_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_id), K(renew_time));
  } else if (OB_FAIL(cache_key_.init(cluster_id, tenant_id, ls_id))) {
    LOG_WARN("cache key init error", K(ret), K(tenant_id), K(ls_id));
  } else {
    renew_time_ = renew_time;
    last_access_ts_ = 0;
    replica_locations_.reset();
    ObLink::reset();
  }
  return ret;
}

int ObLSLocation::init_fake_location()
{
  int ret = OB_SUCCESS;
  reset();
  ObLSReplicaLocation replica_location;
  const char *VIRTUAL_IP = GCONF.use_ipv6 ? "::" : "0.0.0.0";
  uint32_t VIRTUAL_PORT = 0;
  ObAddr server;
  ObReplicaProperty property;
  ObLSRestoreStatus restore_status;
  if (OB_UNLIKELY(!server.set_ip_addr(VIRTUAL_IP, VIRTUAL_PORT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to set ip addr", KR(ret), K(VIRTUAL_IP), K(VIRTUAL_PORT));
  } else if (OB_FAIL(replica_location.init_without_check(
      server,
      FOLLOWER,
      VIRTUAL_PORT,
      REPLICA_TYPE_FULL,
      property,
      restore_status,
      0 /*proposal_id*/))) {
    LOG_WARN("fail to init replica location", KR(ret), K(server),
        K(FOLLOWER), K(VIRTUAL_PORT), K(REPLICA_TYPE_FULL), K(property), K(restore_status));
  } else if (OB_FAIL(replica_locations_.push_back(replica_location))) {
    LOG_WARN("fail to add replica location", KR(ret), K(replica_location));
  } else {
    LOG_INFO("success to init fake location", KPC(this));
  }
  return ret;
}

void ObLSLocation::reset()
{
  cache_key_.reset();
  renew_time_ = 0;
  last_access_ts_ = 0;
  replica_locations_.reset();
  ObLink::reset();
}

int ObLSLocation::assign(const ObLSLocation &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    cache_key_ = other.cache_key_;
    renew_time_ = other.renew_time_;
    if (OB_FAIL(replica_locations_.assign(other.replica_locations_))) {
      LOG_WARN("Failed to assign replica locations", KR(ret), K(other));
    }
  }
  return ret;
}

bool ObLSLocation::is_valid() const
{
  return cache_key_.is_valid()
      && renew_time_ > 0;
}

bool ObLSLocation::is_same_with(const ObLSLocation &other) const
{
  bool bret = true;
  if (cache_key_ != other.cache_key_
      || replica_locations_.count() != other.replica_locations_.count()) {
    bret = false;
  } else {
    ARRAY_FOREACH_X(replica_locations_, idx, cnt, bret) {
      if (replica_locations_.at(idx) != other.replica_locations_.at(idx)) {
        bret = false;
      }
    }
  }
  return bret;
}

bool ObLSLocation::operator==(const ObLSLocation &other) const
{
  return is_same_with(other)
      && (last_access_ts_ == other.last_access_ts_)
      && (renew_time_ == other.renew_time_);
}

bool ObLSLocation::operator!=(const ObLSLocation &other) const
{
  return !(*this == other);
}

int ObLSLocation::get_replica_count(int64_t &full_replica_cnt, int64_t &readonly_replica_cnt)
{
  int ret = OB_SUCCESS;
  full_replica_cnt = 0;
  readonly_replica_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < replica_locations_.count(); ++i) {
    const ObLSReplicaLocation &replica = replica_locations_.at(i);
    if (REPLICA_TYPE_FULL == replica.get_replica_type()) {
      full_replica_cnt++;
    } else if (REPLICA_TYPE_READONLY == replica.get_replica_type()) {
      readonly_replica_cnt++;
    }
  }
  return ret;
}

int ObLSLocation::get_leader(common::ObAddr &leader) const
{
  int ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  leader.reset();
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "self", *this);
  } else {
      ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
      if (replica_locations_.at(i).is_strong_leader()) {
        leader = replica_locations_.at(i).get_server();
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObLSLocation::get_leader(ObLSReplicaLocation &leader) const
{
  int ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  leader.reset();
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "self", *this);
  } else {
      ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
      if (replica_locations_.at(i).is_strong_leader()) {
        if(OB_FAIL(leader.assign(replica_locations_.at(i)))) {
          LOG_WARN("fail to assign leader", KR(ret), "leader", replica_locations_.at(i));
        }
      }
    }
  }
  return ret;
}

int ObLSLocation::alloc_new_location(
    common::ObIAllocator &allocator,
    ObLSLocation *&new_location)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLSLocation)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc location", KR(ret));
  } else {
    new_location = new (buf) ObLSLocation(allocator);
  }
  return ret;
}

int ObLSLocation::add_replica_location(const ObLSReplicaLocation &replica_location)
{
  int ret = OB_SUCCESS;
  if (!replica_location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica location", KR(ret), K(replica_location));
  } else if (OB_FAIL(replica_locations_.push_back(replica_location))) {
    LOG_WARN("fail to do add replica location", KR(ret), K(replica_location));
  }
  return ret;
}

int ObLSLocation::merge_leader_from(const ObLSLocation &new_location)
{
  int ret = OB_SUCCESS;
  ObLSReplicaLocation new_leader;
  int tmp_ret = new_location.get_leader(new_leader);
  if (OB_LS_LOCATION_LEADER_NOT_EXIST != tmp_ret && OB_SUCCESS != tmp_ret) {
    ret = tmp_ret;
    LOG_WARN("fail to get leader from new location", KR(ret), K(new_location));
  } else if (OB_LS_LOCATION_LEADER_NOT_EXIST == tmp_ret) {
    // no valid leader exist, just skip
  } else {
    ObLSReplicaLocation *old_leader = NULL;
    ObLSReplicaLocation *exist_replica = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_locations_.count(); i++) {
      ObLSReplicaLocation &replica = replica_locations_.at(i);
      if (replica.is_strong_leader()) {
        if (OB_NOT_NULL(old_leader)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplicate leader exist", KR(ret), K(replica), KPC(old_leader));
        } else {
          old_leader = &replica;
        }
      }
      if (OB_SUCC(ret) && replica.get_server() == new_leader.get_server()) {
        if (OB_NOT_NULL(exist_replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplidate replica exist", KR(ret), K(replica), KPC(exist_replica));
        } else {
          exist_replica = &replica;
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(old_leader) || old_leader->get_proposal_id() < new_leader.get_proposal_id()) {
      // megre leader when:
      // 1. leader not exist
      // 2. old leader has smaller proposal id
      FLOG_INFO("[LS_LOCATION] leader will be changed",
                K(new_leader), KPC(old_leader), KPC(exist_replica), "old_location", *this);
      if (OB_ISNULL(exist_replica)) {
        if (OB_FAIL(add_replica_location(new_leader))) {
          LOG_WARN("fail to add replica", KR(ret), K(new_leader));
        }
      } else if (OB_FAIL(exist_replica->assign(new_leader))) {
        LOG_WARN("fail to assign new leader", KR(ret), K(new_leader));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < replica_locations_.count(); i++) {
        ObLSReplicaLocation &replica = replica_locations_.at(i);
        if (replica.get_server() != new_leader.get_server()) {
          replica.set_role(FOLLOWER);
          replica.set_proposal_id(0);
        }
      } // end for
      if (OB_SUCC(ret) && get_renew_time() < new_location.get_renew_time()) {
        (void) set_renew_time(new_location.get_renew_time());
      }
    }
  }
  return ret;
}

ObTabletLocation::ObTabletLocation()
    : tenant_id_(OB_INVALID_TENANT_ID),
      tablet_id_(),
      renew_time_(0),
      replica_locations_()
{
}

ObTabletLocation::ObTabletLocation(common::ObIAllocator &allocator)
    : tenant_id_(OB_INVALID_TENANT_ID),
      tablet_id_(),
      renew_time_(0),
      replica_locations_(
        common::OB_MALLOC_NORMAL_BLOCK_SIZE,
        common::ModulePageAllocator(allocator))
{
}

ObTabletLocation::~ObTabletLocation()
{
}

int ObTabletLocation::init(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t renew_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id) || renew_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(tablet_id), K(renew_time));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    renew_time_ = renew_time;
    replica_locations_.reset();
  }
  return ret;
}

int64_t ObTabletLocation::size() const
{
  return sizeof(*this) + replica_locations_.get_data_size();
}

void ObTabletLocation::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  renew_time_ = 0;
  tablet_id_.reset();
  replica_locations_.reset();
}

int ObTabletLocation::assign(const ObTabletLocation &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    renew_time_ = other.renew_time_;
    tablet_id_ = other.tablet_id_;
    if (OB_FAIL(replica_locations_.assign(other.replica_locations_))) {
      LOG_WARN("Failed to assign replica locations", KR(ret), K(other));
    }
  }
  return ret;
}

bool ObTabletLocation::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && tablet_id_.is_valid()
      && renew_time_ > 0;
}

bool ObTabletLocation::operator==(const ObTabletLocation &other) const
{
  bool equal = true;
  if (!is_valid() || !other.is_valid()) {
    equal = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (replica_locations_.count() != other.replica_locations_.count()) {
    equal = false;
  } else {
    for (int64_t i = 0; i < replica_locations_.count(); ++i) {
      if (replica_locations_.at(i) != other.replica_locations_.at(i)) {
        equal = false;
        break;
      }
    }
    equal = equal
        && (tenant_id_ == other.tenant_id_)
        && (tablet_id_ == other.tablet_id_)
        && (renew_time_ == other.renew_time_);
  }
  return equal;
}

bool ObTabletLocation::operator!=(const ObTabletLocation &other) const
{
  return !(*this == other);
}

int ObTabletLocation::get_leader(common::ObAddr &leader) const
{
  int ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  leader.reset();
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "self", *this);
  } else {
      ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
      if (replica_locations_.at(i).is_strong_leader()) {
        leader = replica_locations_.at(i).get_server();
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObTabletLocation::get_leader(ObLSReplicaLocation &leader) const
{
  int ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  leader.reset();
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "self", *this);
  } else {
      ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LS_LOCATION_LEADER_NOT_EXIST == ret) {
      if (replica_locations_.at(i).is_strong_leader()) {
        if(OB_FAIL(leader.assign(replica_locations_.at(i)))) {
          LOG_WARN("fail to assign leader", KR(ret), "leader", replica_locations_.at(i));
        }
      }
    }
  }
  return ret;
}

int ObTabletLocation::add_replica_location(const ObLSReplicaLocation &replica_location)
{
  int ret = OB_SUCCESS;
  if (!replica_location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica location", KR(ret), K(replica_location));
  } else if (OB_FAIL(replica_locations_.push_back(replica_location))) {
    LOG_WARN("fail to do add replica location", KR(ret), K(replica_location));
  }
  return ret;
}


int ObTabletLocation::alloc_new_location(
    common::ObIAllocator &allocator,
    ObTabletLocation *&new_location)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTabletLocation)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc location", KR(ret));
  } else {
    new_location = new (buf) ObTabletLocation(allocator);
  }
  return ret;
}

int ObTabletLocation::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObTabletLocation *pvalue = new (buf) ObTabletLocation();
    if (OB_FAIL(pvalue->assign(*this))) {
      LOG_WARN("fail to assign ObTabletLocation", KR(ret), KP(this), KP(pvalue));
    } else {
      value = pvalue;
    }
  }
  return ret;
}

ObTabletLSCache::ObTabletLSCache()
    : cache_key_(),
      ls_id_(),
      renew_time_(0)
{
}

int64_t ObTabletLSCache::size() const
{
  return sizeof(*this);
}

void ObTabletLSCache::reset()
{
  cache_key_.reset();
  ls_id_.reset();
  renew_time_ = 0;
}

int ObTabletLSCache::assign(const ObTabletLSCache &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    cache_key_ = other.cache_key_;
    ls_id_ = other.ls_id_;
    renew_time_ = other.renew_time_;
  }
  return ret;
}

bool ObTabletLSCache::is_valid() const
{
  return cache_key_.is_valid()
      && ls_id_.is_valid()
      && renew_time_ > 0;
}

bool ObTabletLSCache::mapping_is_same_with(const ObTabletLSCache &other) const
{
  return cache_key_ == other.cache_key_
      && ls_id_ == other.ls_id_;
}

bool ObTabletLSCache::operator==(const ObTabletLSCache &other) const
{
  return mapping_is_same_with(other)
      && renew_time_ == other.renew_time_;
}

bool ObTabletLSCache::operator!=(const ObTabletLSCache &other) const
{
  return !(*this == other);
}

int ObTabletLSCache::init(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const int64_t renew_time,
    const int64_t row_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_key_.init(tenant_id, tablet_id))) {
    LOG_WARN("fail to init ObTabletLSCache", KR(ret), K(tenant_id), K(tablet_id));
  } else {
    ls_id_ = ls_id;
    renew_time_ = renew_time;
    ObLink::reset();
  }
  return ret;
}

ObLSLocationCacheKey::ObLSLocationCacheKey()
    : cluster_id_(OB_INVALID_CLUSTER_ID),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_()
{
}

ObLSLocationCacheKey::ObLSLocationCacheKey(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID ls_id)
    : cluster_id_(cluster_id), tenant_id_(tenant_id), ls_id_(ls_id)
{
}

int ObLSLocationCacheKey::init(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID ls_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id)
      || cluster_id == OB_INVALID_CLUSTER_ID
      || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls location cache key init error", K(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else {
    cluster_id_ = cluster_id;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}

int ObLSLocationCacheKey::assign(const ObLSLocationCacheKey &other)
{
  int ret = OB_SUCCESS;
  cluster_id_ = other.cluster_id_;
  tenant_id_ = other.tenant_id_;
  ls_id_ = other.ls_id_;
  return ret;
}

void ObLSLocationCacheKey::reset()
{
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
}

bool ObLSLocationCacheKey::operator ==(const ObLSLocationCacheKey &other) const
{
  return cluster_id_ == other.cluster_id_
      && tenant_id_ == other.tenant_id_
      && ls_id_ == other.ls_id_;
}

bool ObLSLocationCacheKey::operator !=(const ObLSLocationCacheKey &other) const
{
  return !(*this == other);
}

bool ObLSLocationCacheKey::is_valid() const
{
  return OB_INVALID_CLUSTER_ID != cluster_id_
      && OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid();
}

uint64_t ObLSLocationCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  return hash_val;
}

int ObTabletLSKey::init(const uint64_t tenant_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_id with tenant_id", KR(ret), K(tenant_id), K(tablet_id));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
  }
  return ret;
}

void ObTabletLSKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_id_.reset();
}

bool ObTabletLSKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && tablet_id_.is_valid();
}

uint64_t ObTabletLSKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

bool ObTabletLSKey::operator ==(const ObTabletLSKey &other) const
{
  return tenant_id_ == other.tenant_id_
      && tablet_id_ == other.tablet_id_;
}

//TODO: Reserved for tableapi. Need remove.
bool ObTabletLSCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObTabletLSCacheKey &other_key
      = reinterpret_cast<const ObTabletLSCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_
      && tablet_id_ == other_key.tablet_id_;
}

bool ObTabletLSCacheKey::operator !=(const ObIKVCacheKey &other) const
{
  return !(*this == other);
}

bool ObTabletLSCacheKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && tablet_id_.is_valid();
}

uint64_t ObTabletLSCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

int ObTabletLSCacheKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len < size()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObTabletLSCacheKey *pkey = new (buf) ObTabletLSCacheKey();
    *pkey = *this;
    key = pkey;
  }
  return ret;
}

bool ObLocationServiceUtility::treat_sql_as_timeout(const int error_code)
{
  bool bool_ret = false;
  switch(error_code) {
  case OB_CONNECT_ERROR:
  case OB_TIMEOUT:
  case OB_WAITQUEUE_TIMEOUT:
  case OB_SESSION_NOT_FOUND:
  case OB_TRANS_TIMEOUT:
  case OB_TRANS_STMT_TIMEOUT:
  case OB_TRANS_UNKNOWN:
  case OB_GET_LOCATION_TIME_OUT: {
      bool_ret = true;
      break;
    }
  default: {
      bool_ret = false;
    }
  }
  return bool_ret;
}

ObVTableLocationCacheKey::ObVTableLocationCacheKey()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    table_id_(common::OB_INVALID_ID)
{
}

ObVTableLocationCacheKey::ObVTableLocationCacheKey(
    const uint64_t tenant_id,
    const uint64_t table_id)
    : tenant_id_(tenant_id),
      table_id_(table_id)
{
}

ObVTableLocationCacheKey::~ObVTableLocationCacheKey()
{
}

bool ObVTableLocationCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObVTableLocationCacheKey &other_key =
      reinterpret_cast<const ObVTableLocationCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_
      && table_id_ == other_key.table_id_;
}

bool ObVTableLocationCacheKey::operator !=(const ObIKVCacheKey &other) const
{
  const ObVTableLocationCacheKey &other_key =
      reinterpret_cast<const ObVTableLocationCacheKey &>(other);
  return tenant_id_ != other_key.tenant_id_
      || table_id_ != other_key.table_id_;
}

uint64_t ObVTableLocationCacheKey::get_tenant_id() const
{
  return common::OB_SYS_TENANT_ID;
}

uint64_t ObVTableLocationCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  return hash_val;
}

int64_t ObVTableLocationCacheKey::size() const
{
  return sizeof(*this);
}

int ObVTableLocationCacheKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len < size()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObVTableLocationCacheKey *pkey = new (buf) ObVTableLocationCacheKey();
    *pkey = *this;
    key = pkey;
  }
  return ret;
}

int ObLocationKVCacheValue::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObLocationKVCacheValue *pvalue = new (buf) ObLocationKVCacheValue();
    pvalue->size_ = size_;
    pvalue->buffer_ = buf + sizeof(*this);
    MEMCPY(pvalue->buffer_, buffer_, size_);
    value = pvalue;
  }
  return ret;
}

void ObLocationKVCacheValue::reset()
{
  size_ = 0;
  buffer_ = NULL;
}

ObLocationSem::ObLocationSem() : cur_count_(0), max_count_(0), cond_()
{
  cond_.init(ObWaitEventIds::LOCATION_CACHE_COND_WAIT);
}

ObLocationSem::~ObLocationSem()
{}

void ObLocationSem::set_max_count(const int64_t max_count)
{
  cond_.lock();
  max_count_ = max_count;
  cond_.unlock();
  LOG_INFO("location cache fetch location concurrent max count changed", K(max_count));
}

int ObLocationSem::acquire(const int64_t abs_timeout_us)
{
  // when we change max_count to small value, cur_count > max_count is possible
  int ret = OB_SUCCESS;
  const int64_t default_wait_time_ms = 1000;
  int64_t wait_time_ms = default_wait_time_ms;
  bool has_wait = false;
  cond_.lock();
  if (max_count_ <= 0 || cur_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid max_count", K(ret), K_(max_count), K_(cur_count));
  } else {
    while (OB_SUCC(ret) && cur_count_ >= max_count_) {
      if (abs_timeout_us > 0) {
        wait_time_ms = (abs_timeout_us - ObTimeUtility::current_time()) / 1000 + 1;  // 1ms at least
        if (wait_time_ms <= 0) {
          ret = OB_TIMEOUT;
        }
      } else {
        wait_time_ms = default_wait_time_ms;
      }

      if (OB_SUCC(ret)) {
        if (wait_time_ms > INT32_MAX) {
          wait_time_ms = INT32_MAX;
          const bool force_print = true;
          LOG_DEBUG("wait time is longer than INT32_MAX", K(wait_time_ms), K(abs_timeout_us));
        }
        has_wait = true;
        cond_.wait(static_cast<int32_t>(wait_time_ms));
      }
    }

    if (has_wait) {
      EVENT_INC(LOCATION_CACHE_WAIT);
    }

    if (OB_SUCC(ret)) {
      ++cur_count_;
    }
  }
  cond_.unlock();
  return ret;
}

int ObLocationSem::release()
{
  int ret = OB_SUCCESS;
  cond_.lock();
  if (cur_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cur_count", K(ret), K_(cur_count));
  } else {
    --cur_count_;
  }
  cond_.signal();
  cond_.unlock();
  return ret;
}

} // end namespace share
} // end namespace oceanbase
