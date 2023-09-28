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

#ifndef OCEANBASE_TRANSACTION_OB_TX_MSG_
#define OCEANBASE_TRANSACTION_OB_TX_MSG_

#include "share/scn.h"
#include "share/ob_define.h"
#include "ob_trans_define.h"
#include "share/rpc/ob_batch_proxy.h"
namespace oceanbase
{
namespace transaction
{
    enum TX_MSG_TYPE
    {
      TX_UNKNOWN               = 0,
      /* for xa */
      SUBPREPARE               = 1,
      SUBPREPARE_RESP          = 2,
      SUBCOMMIT                = 3,
      SUBCOMMIT_RESP           = 4,
      SUBROLLBACK              = 5,
      SUBROLLBACK_RESP         = 6,
      /* for trans */
      TX_COMMIT                = 20,
      TX_COMMIT_RESP           = 21,
      TX_ABORT                 = 22,
      /* for 2PC */
      TX_2PC_PREPARE_REDO_REQ     = 40,
      TX_2PC_PREPARE_REDO_RESP    = 41,
      TX_2PC_PREPARE_VERSION_REQ  = 42,
      TX_2PC_PREPARE_VERSION_RESP = 43,
      TX_2PC_PREPARE_REQ       = 44,
      TX_2PC_PREPARE_RESP      = 45,
      TX_2PC_PRE_COMMIT_REQ    = 46,
      TX_2PC_PRE_COMMIT_RESP   = 47,
      TX_2PC_COMMIT_REQ        = 48,
      TX_2PC_COMMIT_RESP       = 49,
      TX_2PC_ABORT_REQ         = 50,
      TX_2PC_ABORT_RESP        = 51,
      TX_2PC_CLEAR_REQ         = 52,
      TX_2PC_CLEAR_RESP        = 53,
      /* for others */
      ROLLBACK_SAVEPOINT       = 60,
      KEEPALIVE                = 61,
      KEEPALIVE_RESP           = 62,
      /* for standby read */
      ASK_STATE                = 63,
      ASK_STATE_RESP           = 64,
      COLLECT_STATE            = 65,
      COLLECT_STATE_RESP       = 66,
      // rollback savepoint resp
      ROLLBACK_SAVEPOINT_RESP  = 67,
      /* for txn free route  */
      TX_FREE_ROUTE_PUSH_STATE       = 80,
      TX_FREE_ROUTE_CHECK_ALIVE      = 81,
      TX_FREE_ROUTE_CHECK_ALIVE_RESP = 82,
    };

    struct ObTxMsg : public obrpc::ObIFill
    {
      explicit ObTxMsg(const int16_t msg_type = TX_UNKNOWN) :
                    type_(msg_type),
                    cluster_version_(0),
                    tenant_id_(OB_INVALID_TENANT_ID),
                    tx_id_(),
                    receiver_(share::ObLSID::INVALID_LS_ID),
                    epoch_(-1),
                    sender_addr_(),
                    sender_(share::ObLSID::INVALID_LS_ID),
                    request_id_(-1),
                    timestamp_(ObTimeUtility::current_time()),
                    cluster_id_(OB_INVALID_CLUSTER_ID)
      {}
      ~ObTxMsg() {}
      int16_t type_;
      int64_t cluster_version_;
      uint64_t tenant_id_;
      ObTransID tx_id_;
      share::ObLSID receiver_;
      /* the target participant's born epoch, used to verify its health */
      int64_t epoch_;
      /* useful when send rsp to sender */
      ObAddr sender_addr_;
      share::ObLSID sender_;
      int64_t request_id_;
      int64_t timestamp_;
      int64_t cluster_id_;
      VIRTUAL_TO_STRING_KV(K_(type),
                           K_(cluster_version),
                           K_(tenant_id),
                           K_(tx_id),
                           K_(receiver),
                           K_(sender),
                           K_(sender_addr),
                           K_(epoch),
                           K_(request_id),
                           K_(timestamp),
                           K_(cluster_id));
      OB_UNIS_VERSION_V(1);
    public:
      virtual bool is_valid() const;
      share::ObLSID get_receiver() const { return receiver_; }
      int64_t get_epoch() const { return epoch_; }
      ObAddr get_sender_addr() const { return sender_addr_; }
      share::ObLSID get_sender() const { return sender_; }
      ObTransID get_trans_id() const { return tx_id_; }
      int16_t get_msg_type() const { return type_; }
      int64_t get_request_id() const { return request_id_; }
      int64_t get_timestamp() const { return timestamp_; }
      uint64_t get_tenant_id() const { return tenant_id_; }
      int64_t get_cluster_id() const { return cluster_id_; }
      int64_t get_cluster_version() const { return cluster_version_; }
      virtual int fill_buffer(char* buf, int64_t size, int64_t &filled_size) const override
      {
        filled_size = 0;
        return serialize(buf, size, filled_size);
      }
      virtual int64_t get_req_size() const override { return get_serialize_size(); }
    };

    // for XA
    struct ObTxSubPrepareMsg : public ObTxMsg {
      ObTxSubPrepareMsg() :
          ObTxMsg(SUBPREPARE),
          expire_ts_(OB_INVALID_TIMESTAMP),
          xid_(),
          parts_()
      {}
      int64_t expire_ts_;
      ObXATransID xid_;
      share::ObLSArray parts_;
      common::ObString app_trace_info_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(expire_ts), K_(xid), K_(parts),
          K_(app_trace_info));
      OB_UNIS_VERSION(1);
    };

    struct ObTxSubPrepareRespMsg : public ObTxMsg {
      ObTxSubPrepareRespMsg() :
          ObTxMsg(SUBPREPARE_RESP)
      {}
      int ret_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(ret));
      OB_UNIS_VERSION(1);
    };

    struct ObTxSubCommitMsg : public ObTxMsg {
      ObTxSubCommitMsg() :
          ObTxMsg(SUBCOMMIT),
          xid_()
      {}
      ObXATransID xid_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K(xid_));
      OB_UNIS_VERSION(1);
    };

    struct ObTxSubCommitRespMsg : public ObTxMsg {
      ObTxSubCommitRespMsg() :
          ObTxMsg(SUBCOMMIT_RESP)
      {}
      int ret_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(ret));
      OB_UNIS_VERSION(1);
    };

    struct ObTxSubRollbackMsg : public ObTxMsg {
      ObTxSubRollbackMsg() :
          ObTxMsg(SUBROLLBACK),
          xid_()
      {}
      ObXATransID xid_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K(xid_));
      OB_UNIS_VERSION(1);
    };

    struct ObTxSubRollbackRespMsg : public ObTxMsg {
      ObTxSubRollbackRespMsg() :
          ObTxMsg(SUBROLLBACK_RESP)
      {}
      int ret_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(ret));
      OB_UNIS_VERSION(1);
    };

    // for trans
    struct ObTxCommitMsg : public ObTxMsg {
      ObTxCommitMsg() :
          ObTxMsg(TX_COMMIT),
          expire_ts_(OB_INVALID_TIMESTAMP),
          parts_()
      { commit_start_scn_.set_max(); }
      int64_t expire_ts_;
      share::SCN commit_start_scn_;
      share::ObLSArray parts_;
      common::ObString app_trace_info_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(expire_ts), K_(commit_start_scn), K_(parts), K_(app_trace_info));
      OB_UNIS_VERSION(1);
    };
    struct ObTxCommitRespMsg : public ObTxMsg {
      ObTxCommitRespMsg() :
          ObTxMsg(TX_COMMIT_RESP)
      {}
      share::SCN commit_version_;
      int ret_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(ret), K_(commit_version));
      OB_UNIS_VERSION(1);
    };

    struct ObTxAbortMsg : public ObTxMsg {
      ObTxAbortMsg() :
          ObTxMsg(TX_ABORT)
      {}
      int reason_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(reason));
      OB_UNIS_VERSION(1);
    };

    struct ObTxRollbackSPMsg : public ObTxMsg {
      ObTxRollbackSPMsg() :
          ObTxMsg(ROLLBACK_SAVEPOINT),
          savepoint_(),
          op_sn_(-1),
          //todo:后续branch_id使用方式确定后，需要相应修改
          branch_id_(-1),
          tx_ptr_(NULL),
          flag_(USE_ASYNC_RESP)
      {}
      ~ObTxRollbackSPMsg() {
        if (OB_NOT_NULL(tx_ptr_)) {
          tx_ptr_->~ObTxDesc();
          ob_free((void*)tx_ptr_);
          tx_ptr_ = NULL;
        }
      }
      ObTxSEQ savepoint_;
      int64_t op_sn_;
      //todo:后期设计中操作编号是否等于branch_id
      int64_t branch_id_;
      const ObTxDesc *tx_ptr_;
      uint8_t flag_;
      bool use_async_resp() const { return (flag_ & USE_ASYNC_RESP) !=0; }
      const static uint8_t USE_ASYNC_RESP = 0x01;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg,
                           K_(savepoint), K_(op_sn), K_(branch_id), K_(flag),
                           KP_(tx_ptr));
      OB_UNIS_VERSION(1);
    };

    struct ObTxRollbackSPRespMsg : public ObTxMsg {
      ObTxRollbackSPRespMsg() :
      ObTxMsg(ROLLBACK_SAVEPOINT_RESP),
      ret_(-1),
      orig_epoch_(0)
      {}
      ~ObTxRollbackSPRespMsg() {
        ret_ = -1;
        orig_epoch_ = 0;
      }
      int ret_;
      int64_t orig_epoch_;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(ret), K_(orig_epoch));
      OB_UNIS_VERSION(1);
    };

    struct ObTxKeepaliveMsg : public ObTxMsg {
      ObTxKeepaliveMsg() :
          ObTxMsg(KEEPALIVE)
      {}
      int64_t status_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(status));
      OB_UNIS_VERSION(1);
    };

    struct ObTxKeepaliveRespMsg : public ObTxMsg {
      ObTxKeepaliveRespMsg() :
          ObTxMsg(KEEPALIVE_RESP)
      {}
      int64_t status_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(status));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPrepareReqMsg : public ObTxMsg
    {
    public:
      Ob2pcPrepareReqMsg() :
          ObTxMsg(TX_2PC_PREPARE_REQ),
          upstream_(share::ObLSID::INVALID_LS_ID)
      {}
    public:
      share::ObLSID upstream_;
      ObString app_trace_info_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(upstream));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPrepareRespMsg : public ObTxMsg
    {
    public:
      Ob2pcPrepareRespMsg() :
          ObTxMsg(TX_2PC_PREPARE_RESP),
          prepare_info_array_()
      {}
    public:
      share::SCN prepare_version_;
      ObLSLogInfoArray prepare_info_array_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(prepare_version), K_(prepare_info_array));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPreCommitReqMsg : public ObTxMsg
    {
    public:
      Ob2pcPreCommitReqMsg() :
          ObTxMsg(TX_2PC_PRE_COMMIT_REQ)
      {}
    public:
      share::SCN commit_version_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(commit_version));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPreCommitRespMsg : public ObTxMsg
    {
    public:
      Ob2pcPreCommitRespMsg() :
          ObTxMsg(TX_2PC_PRE_COMMIT_RESP)
      {}
    public:
      //set commit_version when the root participant 
      //which recover from prepare log recive a pre_commit response 
      //because the coord_state_ will be set as pre_commit
      share::SCN commit_version_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(commit_version));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcCommitReqMsg : public ObTxMsg
    {
    public:
      Ob2pcCommitReqMsg() :
          ObTxMsg(TX_2PC_COMMIT_REQ),
          prepare_info_array_()
      {}
    public:
      share::SCN commit_version_;
      ObLSLogInfoArray prepare_info_array_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(commit_version), K_(prepare_info_array));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcCommitRespMsg : public ObTxMsg
    {
    public:
      Ob2pcCommitRespMsg() : ObTxMsg(TX_2PC_COMMIT_RESP) {}

    public:
      bool is_valid() const;
      share::SCN commit_version_;
      share::SCN commit_log_scn_;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(commit_version), K_(commit_log_scn));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcAbortReqMsg : public ObTxMsg
    {
    public:
      Ob2pcAbortReqMsg() :
        ObTxMsg(TX_2PC_ABORT_REQ),
        upstream_(share::ObLSID::INVALID_LS_ID)
      {}
    public:
      bool is_valid() const;
      share::ObLSID upstream_;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(upstream));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcAbortRespMsg : public ObTxMsg
    {
    public:
      Ob2pcAbortRespMsg() :
          ObTxMsg(TX_2PC_ABORT_RESP)
      {}
    public:
      bool is_valid() const;
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcClearReqMsg : public ObTxMsg
    {
    public:
      Ob2pcClearReqMsg() :
          ObTxMsg(TX_2PC_CLEAR_REQ)
      {}
    public:
      bool is_valid() const;
      share::SCN max_commit_log_scn_;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(max_commit_log_scn));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcClearRespMsg : public ObTxMsg
    {
    public:
      Ob2pcClearRespMsg() :
          ObTxMsg(TX_2PC_CLEAR_RESP)
      {}
    public:
      bool is_valid() const;
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPrepareRedoReqMsg : public ObTxMsg
    {
    public:
      Ob2pcPrepareRedoReqMsg() :
          ObTxMsg(TX_2PC_PREPARE_REDO_REQ),
          xid_(),
          upstream_(share::ObLSID::INVALID_LS_ID)
      {}
    public:
      ObXATransID xid_;
      share::ObLSID upstream_;
      ObString app_trace_info_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(xid), K_(upstream));
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPrepareRedoRespMsg : public ObTxMsg
    {
    public:
      Ob2pcPrepareRedoRespMsg() :
          ObTxMsg(TX_2PC_PREPARE_REDO_RESP)
      {}
    public:
      bool is_valid() const;
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPrepareVersionReqMsg : public ObTxMsg
    {
    public:
      Ob2pcPrepareVersionReqMsg() :
          ObTxMsg(TX_2PC_PREPARE_VERSION_REQ)
      {}
    public:
      bool is_valid() const;
      OB_UNIS_VERSION(1);
    };

    struct Ob2pcPrepareVersionRespMsg : public ObTxMsg
    {
    public:
      Ob2pcPrepareVersionRespMsg() :
          ObTxMsg(TX_2PC_PREPARE_VERSION_RESP),
          prepare_info_array_()
      {}
    public:
      share::SCN prepare_version_;
      ObLSLogInfoArray prepare_info_array_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(prepare_version), K_(prepare_info_array));
      OB_UNIS_VERSION(1);
    };

    struct ObAskStateMsg : public ObTxMsg
    {
    public:
      ObAskStateMsg() :
          ObTxMsg(ASK_STATE),
          snapshot_()
      {}
    public:
      share::SCN snapshot_;

      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(snapshot));
      OB_UNIS_VERSION(1);
    };

    struct ObAskStateRespMsg : public ObTxMsg
    {
    public:
      ObAskStateRespMsg() :
          ObTxMsg(ASK_STATE_RESP),
          state_info_array_()
      {}
    public:
      ObStateInfoArray state_info_array_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(state_info_array));
      OB_UNIS_VERSION(1);
    };

    struct ObCollectStateMsg : public ObTxMsg
    {
    public:
      ObCollectStateMsg() :
          ObTxMsg(COLLECT_STATE),
          snapshot_()
      {}
    public:
      share::SCN snapshot_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(snapshot));
      OB_UNIS_VERSION(1);
    };

    struct ObCollectStateRespMsg : public ObTxMsg
    {
    public:
      ObCollectStateRespMsg() :
          ObTxMsg(COLLECT_STATE_RESP),
          state_info_()
      {}
    public:
      ObStateInfo state_info_;
      bool is_valid() const;
      INHERIT_TO_STRING_KV("txMsg", ObTxMsg, K_(state_info));
      OB_UNIS_VERSION(1);
    };

    class ObTxMsgTypeChecker
    {
    public:
      static bool is_valid_msg_type(const int16_t msg_type)
      {
        return ((0 <= msg_type && 6 >= msg_type)
            || (20 <= msg_type && 22 >= msg_type)
            || (40 <= msg_type && 49 >= msg_type)
            || (50 <= msg_type && 53 >= msg_type)
            || (60 <= msg_type && 67 >= msg_type));
      }

      static bool is_2pc_msg_type(const int16_t msg_type)
      {
        return (msg_type >= TX_2PC_PREPARE_REQ && msg_type <= TX_2PC_CLEAR_RESP)
          || (TX_2PC_PREPARE_REDO_REQ <= msg_type && TX_2PC_PREPARE_VERSION_RESP >= msg_type);
      }
    };


   struct ObAskTxStateFor4377Msg
   {
   public:
     ObAskTxStateFor4377Msg() :
       tx_id_(),
       ls_id_() {}
   public:
     ObTransID tx_id_;
     share::ObLSID ls_id_;
     bool is_valid() const;
     TO_STRING_KV(K_(tx_id), K_(ls_id));
     OB_UNIS_VERSION(1);
   };

   struct ObAskTxStateFor4377RespMsg
   {
   public:
     ObAskTxStateFor4377RespMsg() :
       is_alive_(false),
       ret_(OB_SUCCESS) {}
   public:
     bool is_alive_;
     int ret_;
     bool is_valid() const;
     TO_STRING_KV(K_(is_alive), K_(ret));
     OB_UNIS_VERSION(1);
   };
}
}

#endif // OCEANBASE_TRANSACTION_OB_TX_MSG_
