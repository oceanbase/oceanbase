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

#include "common/ob_member.h"

namespace oceanbase
{
namespace common
{
ObMember::ObMember() : timestamp_(OB_INVALID_TIMESTAMP), flag_(0)
{
}

ObMember::ObMember(const common::ObAddr &server,
                   const int64_t timestamp)
    : server_(server),
      timestamp_(timestamp)
{
  flag_ = 0;
}

const common::ObAddr &ObMember::get_server() const
{
  return server_;
}

int64_t ObMember::get_timestamp() const
{
  return timestamp_;
}

int64_t ObMember::get_flag() const
{
  return flag_;
}

void ObMember::reset()
{
  server_.reset();
  timestamp_ = OB_INVALID_TIMESTAMP;
  flag_ = 0;
}

ObMember &ObMember::operator=(const ObMember &rhs)
{
  server_ = rhs.server_;
  timestamp_ = rhs.timestamp_;
  flag_ = rhs.flag_;
  return *this;
}

int ObMember::assign(const ObMember &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  return ret;
}

bool ObMember::is_valid() const
{
  // timestamp_ could be OB_INVALID_TIMESTAMP
  return server_.is_valid();
}

bool ObMember::is_migrating() const
{
  return (flag_ >> MIGRATING_FLAG_BIT) & 1U;
}

void ObMember::set_migrating()
{
  flag_ |= (1UL << MIGRATING_FLAG_BIT);
}

void ObMember::reset_migrating()
{
  flag_ &= ~(1UL << MIGRATING_FLAG_BIT);
}

OB_SERIALIZE_MEMBER(ObMember, server_, timestamp_, flag_);

bool ObReplicaMember::is_readonly_replica() const
{
  return REPLICA_TYPE_READONLY == replica_type_;
}

void ObReplicaMember::reset()
{
  ObMember::reset();
  replica_type_ = REPLICA_TYPE_FULL;
  region_ = DEFAULT_REGION_NAME;
  memstore_percent_ = 100;
}

bool ObReplicaMember::is_valid() const
{
  return ObMember::is_valid()
         && ObReplicaTypeCheck::is_replica_type_valid(replica_type_)
         && !region_.is_empty()
         && memstore_percent_ <= 100
         && memstore_percent_ >= 0;
}

common::ObReplicaType ObReplicaMember::get_replica_type() const
{
  return replica_type_;
}

int ObReplicaMember::set_replica_type(const common::ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  if (!ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    replica_type_ = replica_type;
  }
  return ret;
}

const common::ObRegion &ObReplicaMember::get_region() const
{
  return region_;
}

int ObReplicaMember::set_member(const ObMember &member)
{
 int ret = OB_SUCCESS;

 if (!member.is_valid()) {
   ret = OB_INVALID_ARGUMENT;
   COMMON_LOG(WARN, "invalid args", K(ret), K(member));
 } else if (OB_FAIL(ObMember::assign(member))) {
   COMMON_LOG(WARN, "failed to assign member", K(ret), K(member));
 }
 return ret;
}

int ObReplicaMember::set_region(const common::ObRegion &region)
{
  int ret = OB_SUCCESS;
  if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    region_ = region;
  }
  return ret;
}

ObReplicaMember &ObReplicaMember::operator=(const ObReplicaMember &rhs)
{
  server_ = rhs.server_;
  timestamp_ = rhs.timestamp_;
  flag_ = rhs.flag_;
  replica_type_ = rhs.replica_type_;
  region_ = rhs.region_;
  memstore_percent_ = rhs.memstore_percent_;
  return *this;
}

OB_SERIALIZE_MEMBER((ObReplicaMember, ObMember), replica_type_, region_, memstore_percent_);

} // namespace common
} // namespace oceanbase
