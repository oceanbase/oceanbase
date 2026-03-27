/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX TRANS
#include "ob_mock_request_msg.h"

namespace oceanbase
{
namespace memtable
{
OB_SERIALIZE_MEMBER(ObFakeRequestMsg, type_, send_addr_);
OB_DEF_SERIALIZE_SIZE(ObFakeWriteRequestMsg)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, type_, send_addr_);
  OB_UNIS_ADD_LEN(*tx_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, key_, value_, expire_ts_, inc_seq_, tx_param_);
  return len;
}

OB_DEF_SERIALIZE(ObFakeWriteRequestMsg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, type_, send_addr_);
  OB_UNIS_ENCODE(*tx_);
  LST_DO_CODE(OB_UNIS_ENCODE, key_, value_, expire_ts_, inc_seq_, tx_param_);
  return ret;
}

OB_DEF_DESERIALIZE(ObFakeWriteRequestMsg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, type_, send_addr_);
  OB_UNIS_DECODE(*tx_);
  LST_DO_CODE(OB_UNIS_DECODE, key_, value_, expire_ts_, inc_seq_, tx_param_);
  return ret;
}
// OB_SERIALIZE_MEMBER(ObFakeWriteRequestMsg, type_, send_addr_, *tx_, key_, value_, expire_ts_, tx_param_);
OB_SERIALIZE_MEMBER(ObFakeWriteRequestRespMsg, type_, send_addr_, tx_id_, key_, value_, exec_result_, ret_);
} // memtable
} // oceanbase