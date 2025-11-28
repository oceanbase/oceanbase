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

#ifndef OCEANBASE_TRANSACTION_OB_TX_SBY_READ_DEFINE
#define OCEANBASE_TRANSACTION_OB_TX_SBY_READ_DEFINE

#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

namespace transaction
{

/**************************************************
 *   Basic Structure
 * ************************************************/

union ObTxSbyStateFlag
{
  uint64_t flag_val_;
  struct FlagBit
  {
    uint64_t infer_by_root_ : 1;
    uint64_t infer_by_transfer_prepare_ : 1;
    uint64_t ls_not_exist_ : 1;
    uint64_t reserverd_bit_ : 61;

    void reset()
    {
      infer_by_root_ = 0;
      infer_by_transfer_prepare_ = 0;
      ls_not_exist_ = 0;
      reserverd_bit_ = 0;
    }

    TO_STRING_KV(K(infer_by_root_), K(infer_by_transfer_prepare_), K(ls_not_exist_));
  } flag_bit_;
};

class ObTxSbyStateInfo
{
  OB_UNIS_VERSION(1);

public:
  ObTxSbyStateInfo() { reset(); }

  bool is_valid() const;
  bool is_empty() const;
  bool is_useful() const;
  bool is_confirmed() const;

  void init(const share::ObLSID &ls_id)
  {
    if (!part_id_.is_valid()) {
      part_id_ = ls_id;
    }
  }
  void fill(const share::ObLSID &ls_id,
            const ObTxState &tx_state,
            const share::SCN &trans_version,
            const share::SCN ls_readable_scn,
            const share::SCN &snapshot,
            const bool is_transfer_prepare = false);

  void set_ls_not_exist(const share::ObLSID &ls_id);

  int get_confirmed_state(ObTxCommitData::TxDataState &infer_trx_state,
                          share::SCN &infer_trx_version) const;
  int infer_by_userful_info(const share::SCN read_snapshot,
                            const bool filter_unreadable_prepare_trx,
                            ObTxCommitData::TxDataState &infer_trx_state,
                            share::SCN &infer_trx_version) const;
  int try_to_update(const ObTxSbyStateInfo &other,  const bool allow_update_by_other_ls);
  void reset();

  TO_STRING_KV(K(part_id_),
               K(tx_state_),
               K(trans_version_),
               K(ls_readable_scn_),
               K(max_applied_snapshot_),
               K(src_addr_),
               K(flag_.flag_bit_));

public:
  // durable
  share::ObLSID part_id_;

  ObTxState tx_state_;
  share::SCN trans_version_;
  share::SCN ls_readable_scn_;

  share::SCN max_applied_snapshot_;

  common::ObAddr src_addr_;
  ObTxSbyStateFlag flag_;
};

typedef common::ObSEArray<ObTxSbyStateInfo, 3> CtxLocalSbyStateInfoArray;

typedef common::ObSEArray<ObTxSbyStateInfo, 2> CtxPartSbyStateInfoArray;

class ObTxSbyAskOrigin : public common::ObDLinkBase<ObTxSbyAskOrigin>
{
public:
  share::SCN read_snapshot_;
  share::ObLSID ori_ls_id_;
  ObAddr ori_addr_;

public:
  static const int64_t MAX_WAIT_ASK_ORIGIN_COUNT = 512;

  TO_STRING_KV(K(read_snapshot_), K(ori_ls_id_), K(ori_addr_));

  void reset()
  {
    read_snapshot_.reset();
    ori_ls_id_.reset();
    ori_addr_.reset();
  }

  ObTxSbyAskOrigin() { reset(); }
  ObTxSbyAskOrigin(const share::SCN & read_snapshot, const share::ObLSID &ls_id, const ObAddr & addr):
    read_snapshot_(read_snapshot),ori_ls_id_(ls_id),ori_addr_(addr)
  {}
};

typedef common::ObDList<ObTxSbyAskOrigin> CtxSbyAskOriginList;

/**************************************************
 *   MSG
 * ************************************************/

// union ObTxSbyStateFlag
// {
//   uint64_t flag_val_;
//   struct FlagBit
//   {
//     uint64_t infer_by_transfer_prepare_ : 1;
//     uint64_t reserverd_bit_ : 63;
//   };
// };

enum class ObTxSbyMsgType
{
  UNKNOWN = 0,
  ASK_UPSTREAM = 1,
  ASK_DOWNSTREAM = 2,
  ASK_REPLICA = 3,
  STATE_RESULT = 4,
  MAX = 5,

};

static bool is_valid_sby_msg_type(const ObTxSbyMsgType &msg_type)
{
  return msg_type > ObTxSbyMsgType::UNKNOWN && msg_type < ObTxSbyMsgType::MAX;
}

static const char *sby_msg_type_to_str(const ObTxSbyMsgType &msg_type)
{
  const char *type_str = "INVALID";
  switch (msg_type) {
  case ObTxSbyMsgType::UNKNOWN: {
    type_str = "UNKNOWN";
    break;
  }
  case ObTxSbyMsgType::ASK_UPSTREAM: {
    type_str = "ASK_UPSTREAM";
    break;
  }
  case ObTxSbyMsgType::ASK_DOWNSTREAM: {
    type_str = "ASK_DOWNSTREAM";
    break;
  }
  case ObTxSbyMsgType::ASK_REPLICA: {
    type_str = "ASK_REPLICA";
    break;
  }
  case ObTxSbyMsgType::STATE_RESULT: {
    type_str = "STATE_RESULT";
    break;
  }
  case ObTxSbyMsgType::MAX: {
    break;
  }
  };
  return type_str;
}

class ObTxSbyBaseMsg
{
  OB_UNIS_VERSION(1);

public:
  ObTxSbyBaseMsg(const ObTxSbyMsgType &msg_type)
      : msg_type_(msg_type), tenant_id_(OB_INVALID_TENANT_ID), cluster_version_(0), tx_id_(),
        msg_snapshot_(share::SCN::invalid_scn()), src_ls_id_(), src_addr_(), dst_ls_id_(),
        dst_addr_()
  {}

  VIRTUAL_TO_STRING_KV("msg_type",
                       sby_msg_type_to_str(msg_type_),
                       K(tenant_id_),
                       K(cluster_version_),
                       K(tx_id_),
                       K(msg_snapshot_),
                       K(src_ls_id_),
                       K(src_addr_),
                       K(dst_ls_id_),
                       K(dst_addr_));

public:
  virtual bool is_valid() const = 0;

  void fill_common_header(const uint64_t tenant_id,
                          const int64_t cluster_version,
                          const ObTransID tx_id,
                          const share::SCN msg_snapshot,
                          const share::ObLSID src_ls_id,
                          const ObAddr &src_addr,
                          const share::ObLSID dst_ls_id,
                          const ObAddr &dst_addr)
  {
    tenant_id_ = tenant_id;
    cluster_version_ = cluster_version;
    tx_id_ = tx_id;
    msg_snapshot_ = msg_snapshot;

    src_ls_id_ = src_ls_id;
    src_addr_ = src_addr;
    dst_ls_id_ = dst_ls_id;
    dst_addr_ = dst_addr;
  }

public:
  ObTxSbyMsgType msg_type_;

  uint64_t tenant_id_;
  int64_t cluster_version_;
  ObTransID tx_id_;
  share::SCN msg_snapshot_; // read snapshot

  share::ObLSID src_ls_id_;
  ObAddr src_addr_;
  share::ObLSID dst_ls_id_;
  ObAddr dst_addr_;
};

class ObTxSbyAskUpstreamReq : public ObTxSbyBaseMsg
{
  OB_UNIS_VERSION(1);

public:
  ObTxSbyAskUpstreamReq() : ObTxSbyBaseMsg(ObTxSbyMsgType::ASK_UPSTREAM), ori_ls_id_(), ori_addr_()
  {}

  bool is_valid() const;

  INHERIT_TO_STRING_KV("base_header", ObTxSbyBaseMsg, K(ori_ls_id_), K(ori_addr_));

public:
  share::ObLSID ori_ls_id_;
  ObAddr ori_addr_;
};

class ObTxSbyAskDownstreamReq : public ObTxSbyBaseMsg
{
  OB_UNIS_VERSION(1);

public:
  ObTxSbyAskDownstreamReq()
      : ObTxSbyBaseMsg(ObTxSbyMsgType::ASK_DOWNSTREAM), root_ls_id_(), root_addr_(),
        exec_epoch_(-1), transfer_epoch_(-1)
  {}
  bool is_valid() const;

  INHERIT_TO_STRING_KV("base_header",
                       ObTxSbyBaseMsg,
                       K(root_ls_id_),
                       K(root_addr_),
                       K(exec_epoch_),
                       K(transfer_epoch_));

public:
  share::ObLSID root_ls_id_;
  ObAddr root_addr_;
  int64_t exec_epoch_;
  int64_t transfer_epoch_;
};

// class ObTxSbyAskReplicaReq : public ObTxSbyBaseMsg
// {
//   OB_UNIS_VERSION(1);
//
// public:
//   ObTxSbyAskReplicaReq() : ObTxSbyBaseMsg(ObTxSbyMsgType::ASK_REPLICA) {}
//   bool is_valid() const;
//
//   INHERIT_TO_STRING_KV("base_header",
//                        ObTxSbyBaseMsg,
//                        K(upstream_ls_id_),
//                        K(epoch_),
//                        K(transfer_epoch_),
//                        K(target_ls_id_),
//                        K(target_addr_));
//
// public:
//   share::ObLSID upstream_ls_id_;
//   int64_t epoch_;
//   int64_t transfer_epoch_;
//   share::ObLSID target_ls_id_;
//   ObAddr target_addr_;
// };

// to root
class ObTxSbyStateResultMsg : public ObTxSbyBaseMsg
{
  OB_UNIS_VERSION(1);

public:
  ObTxSbyStateResultMsg()
      : ObTxSbyBaseMsg(ObTxSbyMsgType::STATE_RESULT), src_state_info_(), downstream_parts_(),
        from_root_(false)
  {}
  bool is_valid() const;

  INHERIT_TO_STRING_KV("base_header",
                       ObTxSbyBaseMsg,
                       K(src_state_info_),
                       K(downstream_parts_),
                       K(from_root_));

public:
  ObTxSbyStateInfo src_state_info_;
  share::ObLSArray downstream_parts_;
  bool from_root_;
};

} // namespace transaction

namespace obrpc
{
class ObTxSbyRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObTxSbyRpcProxy);

  RPC_AP(PR3 post_msg, OB_TX_SBY_ASK_UPSTREAM_REQ, (transaction::ObTxSbyAskUpstreamReq));
  RPC_AP(PR3 post_msg, OB_TX_SBY_ASK_DOWNSTREAM_REQ, (transaction::ObTxSbyAskDownstreamReq));
  // RPC_AP(PR3 post_msg, OB_TX_SBY_ASK_REPLICA_REQ, (transaction::ObTxSbyAskReplicaReq));
  RPC_AP(PR3 post_msg, OB_TX_SBY_STATE_RESULT, (transaction::ObTxSbyStateResultMsg));
};

#define TX_SBY_P(name, pcode)                                               \
  class Ob##name##P : public ObRpcProcessor<ObTxSbyRpcProxy::ObRpc<pcode> > \
  {                                                                         \
  public:                                                                   \
    Ob##name##P() {}                                                        \
                                                                            \
  protected:                                                                \
    int process();                                                          \
                                                                            \
  private:                                                                  \
    DISALLOW_COPY_AND_ASSIGN(Ob##name##P);                                  \
  };

TX_SBY_P(TxSbyAskUpstreamReq, OB_TX_SBY_ASK_UPSTREAM_REQ)
TX_SBY_P(TxSbyAskDownstreamReq, OB_TX_SBY_ASK_DOWNSTREAM_REQ)
// TX_SBY_P(TxSbyAskReplicaReq, OB_TX_SBY_ASK_REPLICA_REQ)
TX_SBY_P(TxSbyStateResult, OB_TX_SBY_STATE_RESULT)

class ObTxSbyRpc
{
public:
  int init(rpc::frame::ObReqTransport *req_transport, const oceanbase::common::ObAddr &addr);

  template <typename SbyMsgType>
  int post_msg(const common::ObAddr dst, const SbyMsgType &msg);

private:
  obrpc::ObTxSbyRpcProxy proxy_;
};

template <typename SbyMsgType>
int ObTxSbyRpc::post_msg(const common::ObAddr dst, const SbyMsgType &msg)
{
  int ret = OB_SUCCESS;
  if (!dst.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid msg or addr", K(dst), K(msg));
  } else if (OB_FAIL(proxy_.to(dst).by(MTL_ID()).post_msg(msg, nullptr))) {
    TRANS_LOG(WARN, "post msg error", K(ret), K(msg));
  }
  return ret;
}
} // namespace obrpc

namespace transaction
{}

} // namespace oceanbase

#endif
