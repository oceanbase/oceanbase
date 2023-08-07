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

#ifndef OCEANBASE_TRANSACTION_TEST_OB_FAKE_TX_RPC_
#define OCEANBASE_TRANSACTION_TEST_OB_FAKE_TX_RPC_
#include "msg_bus.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_rpc.h"
#include "storage/tx/ob_location_adapter.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "storage/tx/ob_tx_free_route.h"
namespace oceanbase {
using namespace share;
namespace transaction {
struct TxMsgCallbackMsg {
private:
  TxMsgCallbackMsg(const ObTxMsg &msg,
                   const ObAddr &addr) {
    orig_msg_type_ = msg.type_;
    sender_ls_id_ = msg.sender_;
    receiver_ls_id_ = msg.receiver_;
    epoch_ = msg.epoch_;
    tx_id_ = msg.tx_id_;
    receiver_addr_ = addr;
    request_id_ = msg.request_id_;
  }
public:
  TO_STRING_KV(K_(orig_msg_type),
               K_(sender_ls_id),
               K_(receiver_ls_id),
               K_(epoch),
               K_(tx_id),
               K_(receiver_addr),
               K_(request_id));
  TxMsgCallbackMsg() {}
  TxMsgCallbackMsg(const ObTxMsg &msg,
                   const ObAddr &addr,
                   const obrpc::ObTxRpcRollbackSPResult &r)
    :TxMsgCallbackMsg(msg, addr)
  {
    type_ = MSG_TYPE::SAVEPOINT_ROLLBACK;
    sp_rollback_result_ = r;
  }
  TxMsgCallbackMsg(const ObTxMsg &msg,
                   const ObAddr &addr,
                   const obrpc::ObTransRpcResult &r)
    :TxMsgCallbackMsg(msg, addr)
  {
    type_ = MSG_TYPE::NORMAL;
    tx_rpc_result_ = r;
  }
#define SERIALIZE_TX_TEST_MSG_CB_(ACT)                          \
  LST_DO_CODE(ACT, type_, orig_msg_type_,                       \
              sender_ls_id_, receiver_ls_id_,                   \
              epoch_, tx_id_, receiver_addr_, request_id_,      \
              sp_rollback_result_,                              \
              tx_rpc_result_);
  int serialize(SERIAL_PARAMS) const {
    int ret = OB_SUCCESS;
    SERIALIZE_TX_TEST_MSG_CB_(OB_UNIS_ENCODE);
    return ret;
  }
  int deserialize(DESERIAL_PARAMS) {
    int ret = OB_SUCCESS;
    SERIALIZE_TX_TEST_MSG_CB_(OB_UNIS_DECODE);
    return ret;
  }
  int64_t get_serialize_size() const {
    int64_t len = 0;
    SERIALIZE_TX_TEST_MSG_CB_(OB_UNIS_ADD_LEN);
    return len;
  }

  enum MSG_TYPE { SAVEPOINT_ROLLBACK = 10,
                  NORMAL = 100 };
  int type_;
  int16_t orig_msg_type_;
  ObLSID sender_ls_id_;
  ObLSID receiver_ls_id_;
  int64_t epoch_;
  ObTransID tx_id_;
  ObAddr receiver_addr_;
  int64_t request_id_;
  obrpc::ObTxRpcRollbackSPResult sp_rollback_result_;
  ObTransRpcResult tx_rpc_result_;
};

class ObFakeTransRpc : public ObITransRpc {
public:
  ObFakeTransRpc(MsgBus *msg_bus,
                 const ObAddr &my_addr,
                 ObILocationAdapter *location_adapter)
    : addr_(my_addr), msg_bus_(msg_bus), location_adapter_(location_adapter) {}
  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy() {}
  int post_msg(const ObAddr &server, ObTxMsg &msg)
  {
    int ret = OB_SUCCESS;
    int64_t size = msg.get_serialize_size() + 1 /*for msg category*/ + sizeof(int16_t) /* for tx_msg.type_ */;
    char *buf = (char*)ob_malloc(size, ObNewModIds::TEST);
    buf[0] = 0; // 0 not callback msg
    int64_t pos = 1;
    int16_t msg_type = msg.type_;
    OZ(serialization::encode(buf, size, pos, msg_type));
    OZ(msg.serialize(buf, size, pos));
    ObString msg_str(size, buf);
    TRANS_LOG(INFO, "post_msg", "msg_ptr", OB_P(buf), K(msg), "receiver", server);
    OZ(msg_bus_->send_msg(msg_str, addr_, server));
    return ret;
  }
  int post_msg(const share::ObLSID &p, ObTxMsg &msg)
  {
    int ret = OB_SUCCESS;
    common::ObAddr leader;
    OZ(location_adapter_->nonblock_get_leader(0, 0, p, leader));
    OZ(post_msg(leader, msg));
    return ret;
  }
  int post_msg(const ObAddr &server, const ObTxFreeRouteMsg &msg) override
  {
    int ret = OB_SUCCESS;
    int64_t size = msg.get_serialize_size() + 1 /*for msg category*/ + sizeof(int16_t) /* for tx_msg.type_ */;
    char *buf = (char*)ob_malloc(size, ObNewModIds::TEST);
    buf[0] = 0; // not callback msg
    int64_t pos = 1;
    int16_t msg_type = msg.type_;
    OZ(serialization::encode(buf, size, pos, msg_type));
    OZ(msg.serialize(buf, size, pos));
    ObString msg_str(size, buf);
    TRANS_LOG(INFO, "post_tx_free_route_msg", "msg_ptr", OB_P(buf), K(msg), "receiver", server);
    OZ(msg_bus_->send_msg(msg_str, addr_, server));
    return ret;
  }
  int sync_access(const ObAddr &server,
                          const ObTxFreeRoutePushState &msg,
                          ObTxFreeRoutePushStateResp &result) override
  {
    int ret = OB_SUCCESS;
    int64_t size = msg.get_serialize_size() + 1 + sizeof(int16_t);
    char *buf = (char*)ob_malloc(size, ObNewModIds::TEST);
    buf[0] = 0; // not callback msg
    int64_t pos = 1;
    int16_t msg_type = TX_MSG_TYPE::TX_FREE_ROUTE_PUSH_STATE;
    OZ(serialization::encode(buf, size, pos, msg_type));
    OZ(msg.serialize(buf, size, pos));
    ObString msg_str(size, buf);
    TRANS_LOG(INFO, "sync send tx push_state", "msg_ptr", OB_P(buf), K(msg), "receiver", server);
    ObString resp;
    OZ(msg_bus_->sync_send_msg(msg_str, addr_, server, resp));
    pos = 0;
    OZ(result.deserialize(resp.ptr(), resp.length(), pos));
    return ret;
  }
  int ask_tx_state_for_4377(const ObAskTxStateFor4377Msg &msg,
                            ObAskTxStateFor4377RespMsg &resp)
  {
    return OB_SUCCESS;
  }
  template<class MSG_RESULT_T>
  int send_msg_callback(const ObAddr &recv,
                        const ObTxMsg &msg,
                        MSG_RESULT_T &rslt);
private:
  ObAddr addr_;
  MsgBus *msg_bus_;
  ObILocationAdapter *location_adapter_;
};

template<class MSG_RESULT_T>
int ObFakeTransRpc::send_msg_callback(const ObAddr &recv,
                                           const ObTxMsg &msg,
                                           MSG_RESULT_T &rslt)
{
  int ret = OB_SUCCESS;
  TxMsgCallbackMsg rmsg(msg, addr_, rslt);
  int64_t size = rmsg.get_serialize_size() + 1 /*  for msg_category */;
  char *buf = (char*)ob_malloc(size, ObNewModIds::TEST);
  buf[0] = 1;// callback
  int64_t pos = 1;
  OZ(rmsg.serialize(buf, size, pos));
  ObString str(size, buf);
  TRANS_LOG(INFO, "send_msg_callback", K(recv), "msg_ptr", OB_P(buf), "orig_msg", msg);
  OZ(msg_bus_->send_msg(str, addr_, recv));
  return ret;
}

} // transaction
} // oceanbase

#endif //OCEANBASE_TRANSACTION_TEST_OB_FAKE_TX_RPC_
