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

#include "ob_cascad_member.h"

namespace oceanbase
{
using namespace common;

namespace share
{
ObCascadMember::ObCascadMember() : server_(),
                                   cluster_id_(common::INVALID_CLUSTER_ID)
{}

ObCascadMember::ObCascadMember(const common::ObAddr &server,
                               const int64_t cluster_id)
    : server_(server),
      cluster_id_(cluster_id)
{}

OB_SERIALIZE_MEMBER(ObCascadMember, server_, cluster_id_);

void ObCascadMember::reset()
{
  server_.reset();
  cluster_id_ = common::INVALID_CLUSTER_ID;
}

ObCascadMember &ObCascadMember::operator=(const ObCascadMember &rhs)
{
  server_ = rhs.server_;
  cluster_id_ = rhs.cluster_id_;
  return *this;
}

bool ObCascadMember::is_valid() const
{
  return server_.is_valid();
}

int ObCascadMember::set_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    server_ = server;
  }
  return ret;
}

int64_t ObCascadMember::hash() const
{
  return (server_.hash() | (cluster_id_ << 32));
}

} // namespace share
} // namespace oceanbase
