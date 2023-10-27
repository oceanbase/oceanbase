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
#ifndef OB_P2P_DH_RPC_PROXY_H
#define OB_P2P_DH_RPC_PROXY_H

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
namespace sql {

class ObP2PDatahubMsgBase;
struct ObPxP2PDatahubArg
{
  OB_UNIS_VERSION(1);
public:
  ObPxP2PDatahubArg() : msg_(nullptr) {}
  void destroy_arg();
public:
  ObP2PDatahubMsgBase *msg_;
  TO_STRING_KV(KP(msg_));
};

struct ObPxP2PDatahubMsgResponse
{
  OB_UNIS_VERSION(1);
public:
  ObPxP2PDatahubMsgResponse() : rc_(0) {}
public:
  int rc_;
  TO_STRING_KV(K_(rc));
};

struct ObPxP2PClearMsgArg
{
  OB_UNIS_VERSION(1);
public:
  ObPxP2PClearMsgArg() : p2p_dh_ids_(), px_seq_id_(0) {}
public:
  ObSArray<int64_t> p2p_dh_ids_;
  int64_t px_seq_id_;
  TO_STRING_KV(K(p2p_dh_ids_));
};

}
namespace obrpc {

class ObP2PDhRpcProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(ObP2PDhRpcProxy);
  RPC_AP(PR5 send_p2p_dh_message, OB_PX_P2P_DH_MSG, (sql::ObPxP2PDatahubArg), sql::ObPxP2PDatahubMsgResponse);
  RPC_AP(PR5 clear_dh_msg, OB_PX_CLAER_DH_MSG, (sql::ObPxP2PClearMsgArg), sql::ObPxP2PDatahubMsgResponse);
};

}
}

#endif
