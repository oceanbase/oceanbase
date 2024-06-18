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

#include "share/detect/ob_detect_callback.h"
#include "share/detect/ob_detect_manager.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"

namespace oceanbase {
namespace common {

const int64_t DM_INTERRUPT_MSG_MAX_LENGTH = 128;

ObIDetectCallback::ObIDetectCallback(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states)
    : ref_count_(0), d_node_()
{
  int ret = OB_SUCCESS;
  peer_states_.set_attr(ObMemAttr(tenant_id, "DmCbStArr"));
  if (OB_FAIL(peer_states_.assign(peer_states))) {
    alloc_succ_ = false;
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "[DM] failed to assign peer_states");
  } else {
    alloc_succ_ = true;
  }
}

int ObIDetectCallback::atomic_set_finished(const common::ObAddr &addr, ObTaskState *state)
{
  int ret = OB_SEARCH_NOT_FOUND;
  ARRAY_FOREACH_NORET(peer_states_, idx) {
    if (peer_states_.at(idx).peer_addr_ == addr) {
      ObTaskState old_val = (ObTaskState)ATOMIC_SET(
          (int32_t*)&peer_states_.at(idx).peer_state_,
          (int32_t)ObTaskState::FINISHED);
      if (OB_NOT_NULL(state)) {
        *state = old_val;
      }
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObIDetectCallback::atomic_set_running(const common::ObAddr &addr)
{
  int ret = OB_SEARCH_NOT_FOUND;
  ARRAY_FOREACH_NORET(peer_states_, idx) {
    if (peer_states_.at(idx).peer_addr_ == addr) {
      ATOMIC_SET((int32_t*)&peer_states_.at(idx).peer_state_,
          (int32_t)ObTaskState::RUNNING);
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int64_t ObIDetectCallback::inc_ref_count(int64_t count)
{
  return ATOMIC_AAF(&ref_count_, count);
}
int64_t ObIDetectCallback::dec_ref_count()
{
  return ATOMIC_SAF(&ref_count_, 1);
}

class ObDmInterruptQcCall
{
public:
  ObDmInterruptQcCall(const common::ObAddr &from_svr_addr, sql::ObDfo &dfo,
      int err,
      int64_t timeout_ts,
      bool need_set_not_alive) : from_svr_addr_(from_svr_addr), dfo_(dfo), err_(err),
      need_interrupt_(false), timeout_ts_(timeout_ts)
  {
    need_interrupt_ = true;
  }
  ~ObDmInterruptQcCall() = default;
  void operator() (hash::HashMapPair<ObInterruptibleTaskID,
      ObInterruptCheckerNode *> &entry);
  int mock_sqc_finish_msg(sql::ObPxSqcMeta &sqc);
public:
  const common::ObAddr &from_svr_addr_;
  sql::ObDfo &dfo_;
  int err_;
  bool need_interrupt_;
  int64_t timeout_ts_;
};

void ObDmInterruptQcCall::operator()(hash::HashMapPair<ObInterruptibleTaskID,
    ObInterruptCheckerNode *> &entry)
{
  UNUSED(entry);
  common::ObIArray<sql::ObPxSqcMeta> &sqcs= dfo_.get_sqcs();
  ARRAY_FOREACH_NORET(sqcs, i) {
    sql::ObPxSqcMeta &sqc = sqcs.at(i);
    if (sqc.get_exec_addr() == from_svr_addr_ && !sqc.is_thread_finish()) {
      if (sqc.is_ignore_vtable_error()) {
        // mock sqc finish if only visit virtual table
        mock_sqc_finish_msg(sqc);
      } else {
        // no longer need to report
        sqc.set_need_report(false);
        // mark for rollback trans
        sqc.set_interrupt_by_dm(true);
      }
      break;
    }
  }
}

int ObDmInterruptQcCall::mock_sqc_finish_msg(sql::ObPxSqcMeta &sqc)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlBasicChannel *ch = reinterpret_cast<dtl::ObDtlBasicChannel *>(
      sqc.get_qc_channel());
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "[DM] ch is unexpected", K(ret));
  } else {
    MTL_SWITCH(ch->get_tenant_id()) {
      ObPxFinishSqcResultMsg finish_msg;
      finish_msg.rc_ = err_;
      finish_msg.dfo_id_ = sqc.get_dfo_id();
      finish_msg.sqc_id_ = sqc.get_sqc_id();
      dtl::ObDtlMsgHeader header;
      header.nbody_ = static_cast<int32_t>(finish_msg.get_serialize_size());
      header.type_ = static_cast<int16_t>(finish_msg.get_type());
      int64_t need_size = header.get_serialize_size() + finish_msg.get_serialize_size();
      dtl::ObDtlLinkedBuffer *buffer = nullptr;
      if (OB_ISNULL(buffer = ch->alloc_buf(need_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "[DM] alloc buffer failed", K(ret));
      } else {
        auto buf = buffer->buf();
        auto size = buffer->size();
        auto &pos = buffer->pos();
        buffer->set_data_msg(false);
        buffer->timeout_ts() = timeout_ts_;
        buffer->set_msg_type(dtl::ObDtlMsgType::FINISH_SQC_RESULT);
        if (OB_FAIL(common::serialization::encode(buf, size, pos, header))) {
          LIB_LOG(WARN, "[DM] fail to encode buffer", K(ret));
        } else if (OB_FAIL(common::serialization::encode(buf, size, pos, finish_msg))) {
          LIB_LOG(WARN, "[DM] serialize RPC channel message fail", K(ret));
        } else if (FALSE_IT(buffer->size() = pos)) {
        } else if (FALSE_IT(pos = 0)) {
        } else if (FALSE_IT(buffer->tenant_id() = ch->get_tenant_id())) {
        } else if (OB_FAIL(ch->attach(buffer))) {
          LIB_LOG(WARN, "[DM] fail to feedup buffer", K(ret));
        } else if (FALSE_IT(ch->free_buffer_count())) {
        } else {
          need_interrupt_ = false;
        }
      }
      if (NULL != buffer) {
        ch->free_buffer_count();
      }
    }
  }
  return ret;
}

ObQcDetectCB::ObQcDetectCB(uint64_t tenant_id,
    const ObIArray<ObPeerTaskState> &peer_states,
    const ObInterruptibleTaskID &tid, sql::ObDfo &dfo,
    const ObIArray<sql::dtl::ObDtlChannel *> &dtl_channels)
    : ObIDetectCallback(tenant_id, peer_states), tid_(tid), dfo_(dfo)
{
  // if ObIDetectCallback constructed succ
  if (alloc_succ_) {
    int ret = OB_SUCCESS;
    dtl_channels_.set_attr(ObMemAttr(tenant_id, "DmCbDtlArr"));
    if (OB_FAIL(dtl_channels_.assign(dtl_channels))) {
      alloc_succ_ = false;
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "[DM] failed to assign dtl_channels_");
    } else {
      alloc_succ_ = true;
    }
  }
}

void ObQcDetectCB::destroy()
{
  ObIDetectCallback::destroy();
  dtl_channels_.reset();
}

int ObQcDetectCB::do_callback()
{
  int ret = OB_SUCCESS;
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  if (OB_NOT_NULL(manager)) {
    ObDmInterruptQcCall call(from_svr_addr_, dfo_, OB_RPC_CONNECT_ERROR, timeout_ts_, false);
    if (OB_FAIL(manager->get_map().atomic_refactored(tid_, call))) {
      ret = ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : ret;
      LIB_LOG(WARN, "[DM] fail to set need report, qc already exits", K(tid_), K_(trace_id));
    } else if (call.need_interrupt_) {
      ObInterruptCode int_code(OB_RPC_CONNECT_ERROR,
                               GETTID(),
                               from_svr_addr_,
                               "Dm interrupt qc");
      if (OB_FAIL(manager->interrupt(tid_, int_code))) {
        LIB_LOG(WARN, "[DM] fail to send interrupt message", K(int_code), K(tid_), K_(trace_id));
      }
    }
  }
  LIB_LOG(WARN, "[DM] interrupt qc", K(tid_), K_(trace_id));
  return ret;
}

int ObQcDetectCB::atomic_set_finished(const common::ObAddr &addr, ObTaskState *state)
{
  int ret = OB_SEARCH_NOT_FOUND;
  for (int i = 0; i < get_peer_states().count(); ++i) {
    if (get_peer_states().at(i).peer_addr_ == addr) {
      ATOMIC_SET((int32_t*)&get_peer_states().at(i).peer_state_, (int32_t)ObTaskState::FINISHED);
      if (OB_NOT_NULL(state)) {
        sql::dtl::ObDtlRpcChannel* dtl_rpc_channel = static_cast<sql::dtl::ObDtlRpcChannel*>(dtl_channels_.at(i));
        if (dtl_rpc_channel->recv_sqc_fin_res()) {
          *state = ObTaskState::FINISHED;
        } else {
          *state = ObTaskState::RUNNING;
        }
      }
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObSqcDetectCB::do_callback()
{
  int ret = OB_SUCCESS;
  ObInterruptCode int_code(OB_RPC_CONNECT_ERROR,
                                 GETTID(),
                                 from_svr_addr_,
                                 "Dm interrupt sqc");
  if (OB_FAIL(ObGlobalInterruptManager::getInstance()->interrupt(tid_, int_code))) {
    LIB_LOG(WARN, "[DM] fail to send interrupt message", K(int_code), K(tid_), K_(trace_id));
  }
  LIB_LOG(WARN, "[DM] interrupt sqc", K(tid_), K_(trace_id));
  return ret;
}

int ObSingleDfoDetectCB::do_callback()
{
  int ret = OB_SUCCESS;
  int clean_ret = OB_E(EventTable::EN_ENABLE_CLEAN_INTERM_RES) OB_SUCCESS;
  if (OB_SUCC(clean_ret)) {
    dtl::ObDTLIntermResultManager *interm_res_manager = MTL(dtl::ObDTLIntermResultManager*);
    if (OB_ISNULL(interm_res_manager)) {
      // ignore ret
      LIB_LOG(WARN, "[DM] single dfo erase_interm_result_info, but interm_res_manager is null",
                    K(ret), K(key_), K_(trace_id));
    } else {
      ret = interm_res_manager->erase_interm_result_info(key_, false);
      ret = ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : ret;
      LIB_LOG(WARN, "[DM] single dfo erase_interm_result_info", K(ret), K(key_), K_(trace_id));
    }
  }
  return ret;
}

int ObTempTableDetectCB::do_callback()
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultManager *interm_res_manager = MTL(dtl::ObDTLIntermResultManager*);
  if (OB_ISNULL(interm_res_manager)) {
    // ignore ret
    LIB_LOG(WARN, "[DM] temp table erase_interm_result_info, but interm_res_manager is null",
                  K(ret), K(key_), K_(trace_id));
  } else {
    ret = interm_res_manager->erase_interm_result_info(key_, false);
    ret = ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : ret;
    LIB_LOG(WARN, "[DM] temp table erase_interm_result_info", K(ret), K(key_), K_(trace_id));
  }
  return ret;
}

int ObP2PDataHubDetectCB::do_callback()
{
  int ret = OB_SUCCESS;
  ObP2PDatahubMsgBase *msg = nullptr;
  bool is_erased = false;
  ret = PX_P2P_DH.erase_msg_if(key_, msg, is_erased, false/* need unregister dm */);
  ret = ret == OB_HASH_NOT_EXIST ? OB_SUCCESS : ret;
  LIB_LOG(WARN, "[DM] p2p dh erase p2p msg", K(ret), K(key_), K_(trace_id), K(is_erased));
  return ret;
}

} // end namespace common
} // end namespace oceanbase
