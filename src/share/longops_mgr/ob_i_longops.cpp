// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE

#include "ob_i_longops.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

ObILongopsKey::ObILongopsKey()
  : tenant_id_(OB_INVALID_ID),
    sid_(OB_INVALID_ID)
{
  MEMSET(name_, 0, sizeof(name_));
  MEMSET(target_, 0 ,sizeof(target_));
}

int64_t ObILongopsKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&sid_, sizeof(sid_), hash_val);
  hash_val = murmurhash(name_, sizeof(name_), hash_val);
  hash_val = murmurhash(target_, sizeof(target_), hash_val);
  return hash_val;
}

bool ObILongopsKey::operator==(const ObILongopsKey &other) const
{
  return tenant_id_ == other.tenant_id_ &&
         sid_ == other.sid_ &&
         (0 == MEMCMP(name_, other.name_, sizeof(name_))) &&
         (0 == MEMCMP(target_, other.target_, sizeof(target_)));
}

bool ObILongopsKey::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ &&
         '\0' != name_[0] &&
         '\0' != target_[0];
}

ObLongopsValue::ObLongopsValue()
  : trace_id_(), sid_(OB_INVALID_ID), tenant_id_(OB_INVALID_ID), start_time_(-1), finish_time_(-1), elapsed_seconds_(0),
    time_remaining_(0), percentage_(0), last_update_time_(0), op_name_(), target_(), message_()
{
}

ObLongopsValue &ObLongopsValue::operator=(const ObLongopsValue &other)
{
  if (this != &other) {
    trace_id_ = other.trace_id_;
    sid_ = other.sid_;
    tenant_id_ = other.tenant_id_;
    start_time_ = other.start_time_;
    finish_time_ = other.finish_time_;
    elapsed_seconds_ = other.elapsed_seconds_;
    time_remaining_ = other.time_remaining_;
    percentage_ = other.percentage_;
    last_update_time_ = other.last_update_time_;
    MEMCPY(op_name_, other.op_name_, sizeof(op_name_));
    MEMCPY(target_, other.target_, sizeof(target_));
    MEMCPY(message_, other.message_, sizeof(message_));
  }
  return *this;
}

void ObLongopsValue::reset()
{
  trace_id_.reset();
  tenant_id_ = OB_INVALID_ID;
  start_time_ = -1;
  finish_time_ = -1;
  elapsed_seconds_ = 0;
  time_remaining_ = 0;
  percentage_ = 0;
  last_update_time_ = 0;
  memset(op_name_, 0, sizeof(op_name_));
  memset(target_, 0, sizeof(target_));
  memset(message_, 0, sizeof(message_));
}