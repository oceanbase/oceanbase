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

#define USING_LOG_PREFIX STORAGE

#include "storage/tx_storage/ob_get_ls_replica_checkpoint_info.h"

#include "lib/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace obrpc
{

OB_SERIALIZE_MEMBER(ObGetLSReplicaCheckpointInfoArg, tenant_id_, ls_id_);

bool ObGetLSReplicaCheckpointInfoArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
         && ls_id_.is_valid();
}

int ObGetLSReplicaCheckpointInfoArg::init(
    const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}

int ObGetLSReplicaCheckpointInfoArg::assign(
    const ObGetLSReplicaCheckpointInfoArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetLSReplicaCheckpointInfoRes, tenant_id_, ls_id_, checkpoint_scn_);

bool ObGetLSReplicaCheckpointInfoRes::is_valid() const
{

  return is_valid_tenant_id(tenant_id_)
         && ls_id_.is_valid()
         && checkpoint_scn_.is_valid();
}

int ObGetLSReplicaCheckpointInfoRes::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !ls_id.is_valid()
                  || !checkpoint_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(checkpoint_scn));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    checkpoint_scn_ = checkpoint_scn;
  }
  return ret;
}

int ObGetLSReplicaCheckpointInfoRes::assign(
    const ObGetLSReplicaCheckpointInfoRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    checkpoint_scn_ = other.checkpoint_scn_;
  }
  return ret;
}

} // namespace obrpc
} // namespace oceanbase
