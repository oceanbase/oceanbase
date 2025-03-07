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

#include "ob_unit_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
const char *const ObUnit::unit_status_strings[ObUnit::UNIT_STATUS_MAX] = {
  "ACTIVE",
  "DELETING",
};

ObUnit::Status ObUnit::str_to_unit_status(const ObString &str)
{
  ObUnit::Status unit_status = UNIT_STATUS_MAX;
  if (str.empty()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "str is empty", K(str));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(unit_status_strings); ++i) {
      if (0 == str.case_compare(unit_status_strings[i])) {
        unit_status = static_cast<ObUnit::Status>(i);
        break;
      }
    }
  }
  return unit_status;
}

ObUnit::ObUnit()
{
  reset();
}

void ObUnit::reset()
{
  unit_id_ = OB_INVALID_ID;
  resource_pool_id_ = OB_INVALID_ID;
  unit_group_id_ = OB_INVALID_ID;
  zone_.reset();
  server_.reset();
  migrate_from_server_.reset();
  is_manual_migrate_ = false;
  status_ = UNIT_STATUS_MAX;
  replica_type_ = REPLICA_TYPE_FULL;
}

int ObUnit::assign(const ObUnit& that)
{
  int ret = OB_SUCCESS;
  if (this == &that) {
    //skip
  } else if (OB_FAIL(zone_.assign(that.zone_))) {
    LOG_WARN("zone_ assign failed", KR(ret), K(that.zone_));
  } else {
    unit_id_ = that.unit_id_;
    resource_pool_id_ = that.resource_pool_id_;
    unit_group_id_ = that.unit_group_id_;
    server_ = that.server_;
    migrate_from_server_ = that.migrate_from_server_;
    is_manual_migrate_ = that.is_manual_migrate_;
    status_ = that.status_;
    replica_type_ = that.replica_type_;
  }
  return ret;
}

bool ObUnit::is_valid() const
{
  // it's ok for migrate_from_server to be invalid
  return OB_INVALID_ID != unit_id_
         && OB_INVALID_ID != resource_pool_id_
         && OB_INVALID_ID != unit_group_id_
         && !zone_.is_empty()
         && server_.is_valid()
         && server_ != migrate_from_server_
         && (status_ >= UNIT_STATUS_ACTIVE && status_ < UNIT_STATUS_MAX)
         && ObReplicaTypeCheck::is_replica_type_valid(replica_type_);
}

int ObUnit::get_unit_status_str(const char *&status_str) const
{
  int ret = OB_SUCCESS;
  if (status_ >= UNIT_STATUS_MAX || status_ < UNIT_STATUS_ACTIVE) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(status_));
  } else {
    status_str = unit_status_strings[status_];
  }
  return ret;
}

DEF_TO_STRING(ObUnit)
{
  int64_t pos = 0;
  J_KV(K_(unit_id),
       K_(resource_pool_id),
       K_(unit_group_id),
       K_(zone),
       K_(server),
       K_(migrate_from_server),
       K_(is_manual_migrate),
       K_(status),
       K_(replica_type));
  return pos;
}

OB_SERIALIZE_MEMBER(ObUnit,
                    unit_id_,
                    resource_pool_id_,
                    unit_group_id_,
                    zone_,
                    server_,
                    migrate_from_server_,
                    is_manual_migrate_,
                    status_,
                    replica_type_);

int ObUnitInfo::assign(const ObUnitInfo &other)
{
  int ret = OB_SUCCESS;
  unit_ = other.unit_;
  config_ = other.config_;
  if (OB_FAIL(copy_assign(pool_, other.pool_))) {
    SHARE_LOG(WARN, "failed to assign pool_", K(ret));
  }
  return ret;
}

ObTenantServers::ObTenantServers()
    : tenant_id_(OB_INVALID_TENANT_ID),
      servers_(),
      renew_time_(0)
{
  servers_.set_label("TntSrvArr");
}

ObTenantServers::~ObTenantServers()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  renew_time_ = 0;
  servers_.destroy();
}

int ObTenantServers::assign(const ObTenantServers &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(servers_.assign(other.servers_))) {
      LOG_WARN("assign failed", KR(ret), "servers", other.servers_);
    } else {
      tenant_id_ = other.tenant_id_;
      renew_time_ = other.renew_time_;
    }
  }
  return ret;
}

int ObTenantServers::init_or_insert_server(
    const uint64_t tenant_id,
    const common::ObAddr &server,
    const common::ObAddr &migrate_server,
    const int64_t renew_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !server.is_valid()
      || renew_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input parameter", KR(ret),
        K(tenant_id), K(server), K(renew_time));
  } else if (!is_valid()) {
    tenant_id_ = tenant_id;
    renew_time_ = renew_time;
  } else if (tenant_id != tenant_id_) {
    ret = OB_CONFLICT_VALUE;
    LOG_WARN("already initialized; tenant mismatch", KR(ret),
        "tenant to be inserted", tenant_id,
        "tenant already inserted", tenant_id_);
  }

  // insert_server will ensure that
  // there are no duplicates in the servers of tenant_servers
  if (FAILEDx(insert_server_(server))) {
    LOG_WARN("failed to insert server to tenant_servers", KR(ret), K(server));
  } else if (migrate_server.is_valid()) {
    if (OB_FAIL(insert_server_(migrate_server))) {
      LOG_WARN("failed to insert migrate_server to tenant_servers", KR(ret), K(migrate_server));
    }
  }
  return ret;
}

void ObTenantServers::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  renew_time_ = 0;
  servers_.reset();
}

bool ObTenantServers::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
         && !servers_.empty()
         && renew_time_ > 0;
}

int ObTenantServers::insert_server_(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the server is invalid", KR(ret), K(server));
  } else if (has_exist_in_array(servers_, server)) {
    // server exist
  } else if (OB_FAIL(servers_.push_back(server))) {
    LOG_WARN("push_back failed", KR(ret), K(server));
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
