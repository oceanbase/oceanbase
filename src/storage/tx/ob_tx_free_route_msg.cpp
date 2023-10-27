/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_tx_free_route_msg.h"
namespace oceanbase {
namespace transaction {
OB_SERIALIZE_MEMBER(ObTxFreeRouteCheckAliveMsg, request_id_, req_sess_id_, tx_sess_id_, tx_id_, sender_, receiver_);
OB_SERIALIZE_MEMBER(ObTxFreeRouteCheckAliveRespMsg, request_id_, req_sess_id_, tx_id_, sender_, receiver_, ret_);

OB_DEF_SERIALIZE_SIZE(ObTxFreeRoutePushState)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, tx_id_, logic_clock_, dynamic_offset_, parts_offset_, extra_offset_, buf_);
  return len;
}

OB_DEF_SERIALIZE(ObTxFreeRoutePushState)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, tx_id_, logic_clock_, dynamic_offset_, parts_offset_, extra_offset_, buf_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTxFreeRoutePushState)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, tx_id_, logic_clock_, dynamic_offset_, parts_offset_, extra_offset_, buf_);
  // deep copy buf_
  void *tmp_buf = ob_malloc(buf_.length(), lib::ObMemAttr(tenant_id_, "TxnFreeRoute"));
  if (OB_ISNULL(tmp_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(tmp_buf, buf_.ptr(), buf_.length());
    buf_ = ObString(buf_.length(), (char*)tmp_buf);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTxFreeRoutePushStateResp, ret_);

}
}
