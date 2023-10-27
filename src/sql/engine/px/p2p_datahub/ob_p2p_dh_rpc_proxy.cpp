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
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

OB_DEF_SERIALIZE(ObPxP2PDatahubArg)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(msg_));
  OB_UNIS_ENCODE(msg_->get_msg_type());
  OB_UNIS_ENCODE(*msg_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPxP2PDatahubArg)
{
  int ret =  OB_SUCCESS;
  CK(OB_NOT_NULL(CURRENT_CONTEXT));
  ObP2PDatahubMsgBase::ObP2PDatahubMsgType msg_type = ObP2PDatahubMsgBase::NOT_INIT;
  OB_UNIS_DECODE(msg_type);
  ObIAllocator &allocator = CURRENT_CONTEXT->get_arena_allocator();
  if (OB_FAIL(PX_P2P_DH.alloc_msg(allocator, msg_type, msg_))) {
    LOG_WARN("fail to alloc msg", K(ret));
  } else {
    OB_UNIS_DECODE(*msg_);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(msg_)) {
    // DECODE failed, must destroy msg.
    msg_->destroy();
    msg_ = nullptr;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxP2PDatahubArg)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN, msg_->get_msg_type(), *msg_);
  return len;
}

void ObPxP2PDatahubArg::destroy_arg()
{
  if (OB_NOT_NULL(msg_)) {
    msg_->destroy();
    msg_ = nullptr;
  }
}

OB_SERIALIZE_MEMBER(ObPxP2PDatahubMsgResponse, rc_);

OB_SERIALIZE_MEMBER(ObPxP2PClearMsgArg, p2p_dh_ids_, px_seq_id_);
