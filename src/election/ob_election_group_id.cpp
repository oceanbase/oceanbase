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

#include "ob_election_group_id.h"
#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "share/ob_errno.h"
#include "lib/utility/utility.h"
#include "common/ob_member_list.h"
#include "ob_election_async_log.h"

namespace oceanbase {
using namespace common;

namespace election {
OB_SERIALIZE_MEMBER(ObElectionGroupId, server_, create_time_, hash_val_);

int ObElectionGroupId::init(const ObAddr& server, const int64_t create_time)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TIMESTAMP == create_time) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid arguemnt", K(ret), K(server), K(create_time));
  } else {
    server_ = server;
    create_time_ = create_time;
    hash_val_ = cal_hash_();
  }
  return ret;
}

void ObElectionGroupId::reset()
{
  server_.reset();
  create_time_ = OB_INVALID_TIMESTAMP;
  hash_val_ = 0;
}

bool ObElectionGroupId::is_valid() const
{
  return (server_.is_valid() && (OB_INVALID_TIMESTAMP != create_time_));
}

uint64_t ObElectionGroupId::hash() const
{
  return hash_val_;
}

uint64_t ObElectionGroupId::cal_hash_() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&server_, sizeof(server_), hash_val);
  hash_val = murmurhash(&create_time_, sizeof(create_time_), hash_val);
  return hash_val;
}

ObElectionGroupId& ObElectionGroupId::operator=(const ObElectionGroupId& other)
{
  if (this != &other) {
    server_ = other.get_server();
    create_time_ = other.get_create_time();
    hash_val_ = other.hash();
  }
  return *this;
}

int ObElectionGroupId::compare(const ObElectionGroupId& other) const
{
  int cmp_ret = 0;

  if (hash_val_ > other.hash()) {
    cmp_ret = 1;
  } else if (hash_val_ < other.hash()) {
    cmp_ret = -1;
  } else if (other.get_server() < server_) {
    cmp_ret = 1;
  } else if (server_ < other.get_server()) {
    cmp_ret = -1;
  } else if (create_time_ > other.get_create_time()) {
    cmp_ret = 1;
  } else if (create_time_ < other.get_create_time()) {
    cmp_ret = -1;
  } else {
    // do nothing
  }

  return cmp_ret;
}

}  // namespace election
}  // namespace oceanbase
