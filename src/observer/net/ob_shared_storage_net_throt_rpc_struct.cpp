/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/net/ob_shared_storage_net_throt_rpc_struct.h"

#define USING_LOG_PREFIX RPC

namespace oceanbase
{

namespace obrpc
{
// for the serialize need of rpc
OB_SERIALIZE_MEMBER(ObSharedDeviceResource, key_, type_, value_);
OB_SERIALIZE_MEMBER(ObSharedDeviceResourceArray, array_);
OB_SERIALIZE_MEMBER(ObSSNTKey, addr_, key_);
OB_SERIALIZE_MEMBER(ObSSNTValue, predicted_resource_, assigned_resource_, expire_time_);
OB_SERIALIZE_MEMBER(ObSSNTResource, ops_, ips_, iops_, obw_, ibw_, iobw_, tag_);

OB_SERIALIZE_MEMBER(ObSSNTEndpointArg, addr_, storage_keys_, expire_time_);
OB_SERIALIZE_MEMBER(ObSSNTSetRes, res_);
int ObSSNTSetRes::assign(const ObSSNTSetRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    this->res_ = other.res_;
  }
  return OB_SUCCESS;
}
}  // namespace obrpc
}  // namespace oceanbase