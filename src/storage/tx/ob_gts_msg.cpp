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

#include "share/ob_define.h"
#include "ob_gts_msg.h"
#include "ob_gts_rpc.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;

namespace transaction
{
// ObGtsRequest
OB_SERIALIZE_MEMBER(ObGtsRequest, tenant_id_, srr_.mts_, range_size_, sender_);
// ObGtsErrResponse
OB_SERIALIZE_MEMBER(ObGtsErrResponse, tenant_id_, srr_.mts_, status_, sender_);

int ObGtsRequest::init(const uint64_t tenant_id, const MonotonicTs srr, const int64_t range_size,
    const ObAddr &sender)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || !srr.is_valid() || 0 >= range_size || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(srr), K(range_size), K(sender));
  } else {
    tenant_id_ = tenant_id;
    srr_ = srr;
    range_size_ = range_size;
    sender_ = sender;
  }
  return ret;
}

bool ObGtsRequest::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && srr_.is_valid() && range_size_ > 0 &&
    sender_.is_valid();
}

//leader may be invalid, the validity check does not need to check this field
int ObGtsErrResponse::init(const uint64_t tenant_id, const MonotonicTs srr, const int status,
    const ObAddr &sender)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || !srr.is_valid() || !sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(srr), K(status), K(sender));
  } else {
    tenant_id_ = tenant_id;
    srr_ = srr;
    status_ = status;
    sender_ = sender;
  }
  return ret;
}

bool ObGtsErrResponse::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && srr_.is_valid() && OB_SUCCESS != status_ && sender_.is_valid();
}

} // transaction
} // oceanbase
