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

#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_msg.h"
#include "lib/rc/context.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "share/ob_rpc_share.h"
#include "share/detect/ob_detect_manager_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::obrpc;

ObP2PDatahubManager &ObP2PDatahubManager::instance()
{
  static ObP2PDatahubManager the_p2p_dh_mgr;
  return the_p2p_dh_mgr;
}

int ObP2PDatahubManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("no need to init twice datahub manager", K(ret));
  } else if (OB_FAIL(map_.create(BUCKET_NUM,
      "PxP2PDhMgrKey",
      "PxP2PDhMgrNode"))) {
    LOG_WARN("create hash table failed", K(ret));
  } else if (OB_FAIL(share::init_obrpc_proxy(p2p_dh_proxy_))) {
    LOG_WARN("fail to init obrpc proxy", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObP2PDatahubManager::destroy()
{
  if (IS_INIT) {
    map_.destroy();
  }
}

int ObP2PDatahubManager::process_msg(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObP2PDatahubMsgBase *new_msg = nullptr;
  bool need_free = false;
  if (!msg.is_valid_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", K(ret), K(msg.get_msg_type()));
  } else if (OB_FAIL(deep_copy_msg(msg, new_msg))) {
    need_free = true;
    LOG_WARN("fail to copy msg", K(ret));
  } else if (OB_ISNULL(new_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new msg", K(ret));
  } else if (OB_FAIL(new_msg->process_msg_internal(need_free))) {
    LOG_WARN("fail to process msg", K(ret));
  }
  if (need_free && OB_NOT_NULL(new_msg)) {
    new_msg->destroy();
    ob_free(new_msg);
    new_msg = nullptr;
  }
  return ret;
}

template<typename T>
int ObP2PDatahubManager::alloc_msg(int64_t tenant_id, T *&msg_ptr)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  ObMemAttr attr(tenant_id, "PxP2PDhMsg", common::ObCtxIds::DEFAULT_CTX_ID);
  if (OB_ISNULL(ptr = (ob_malloc(sizeof(T), attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for p2p dh msg", K(ret));
  } else {
    msg_ptr = new(ptr) T();
  }
  return ret;
}

template<typename T>
int ObP2PDatahubManager::alloc_msg(
    common::ObIAllocator &allocator,
    T *&msg_ptr)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  if (OB_ISNULL(ptr = (allocator.alloc(sizeof(T))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for p2p dh msg", K(ret));
  } else {
    msg_ptr = new(ptr) T();
  }
  return ret;
}

int ObP2PDatahubManager::alloc_msg(
    common::ObIAllocator &allocator,
    ObP2PDatahubMsgBase::ObP2PDatahubMsgType type,
    ObP2PDatahubMsgBase *&msg_ptr)
{
  int ret = OB_SUCCESS;
  switch(type) {
    case ObP2PDatahubMsgBase::BLOOM_FILTER_MSG: {
      ObRFBloomFilterMsg *bf_ptr = nullptr;
      if (OB_FAIL(alloc_msg<ObRFBloomFilterMsg>(allocator, bf_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else {
        msg_ptr = bf_ptr;
      }
      break;
    }
    case ObP2PDatahubMsgBase::RANGE_FILTER_MSG: {
      ObRFRangeFilterMsg *range_ptr = nullptr;
      if (OB_FAIL(alloc_msg<ObRFRangeFilterMsg>(allocator, range_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else {
        msg_ptr = range_ptr;
      }
      break;
    }
    case ObP2PDatahubMsgBase::IN_FILTER_MSG: {
      ObRFInFilterMsg *in_ptr = nullptr;
      if (OB_FAIL(alloc_msg<ObRFInFilterMsg>(allocator, in_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else {
        msg_ptr = in_ptr;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(type), K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(msg_ptr)) {
    msg_ptr->set_msg_type(type);
  }
  return ret;
}

void ObP2PDatahubManager::free_msg(ObP2PDatahubMsgBase *&msg)
{
  if (OB_NOT_NULL(msg)) {
    msg->destroy();
    ob_free(msg);
    msg = nullptr;
  }
}

int ObP2PDatahubManager::deep_copy_msg(ObP2PDatahubMsgBase &msg, ObP2PDatahubMsgBase *&new_msg)
{
  return msg.deep_copy_msg(new_msg);
}

int ObP2PDatahubManager::P2PMsgMergeCall::operator() (common::hash::HashMapPair<ObP2PDhKey,
    ObP2PDatahubMsgBase *> &entry)
{
  int ret = OB_SUCCESS;
  if (!dh_msg_.is_active()) {
    entry.second->set_is_active(false);
  } else if (!dh_msg_.is_empty() && (OB_FAIL(entry.second->merge(dh_msg_)))) {
    LOG_WARN("fail to merge dh msg", K(ret_), K(entry.first));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(entry.second->process_receive_count(dh_msg_))) {
    LOG_WARN("fail to process receive count", K(ret));
  }
  need_free_ = true;
  return ret;
}

int ObP2PDatahubManager::send_local_msg(ObP2PDatahubMsgBase *msg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("msg is null", K(ret));
  } else {
    ObP2PDhKey dh_key(msg->get_p2p_datahub_id(),
        msg->get_px_seq_id(),
        msg->get_task_id(),
        ObTimeUtility::current_time(), msg->get_timeout_ts());
    if (OB_FAIL(map_.set_refactored(dh_key, msg))) {
      LOG_TRACE("fail to insert p2p dh msg", K(ret), K(dh_key));
    } else {
      msg->set_is_ready(true);
    }
  }
  return ret;
}

int ObP2PDatahubManager::atomic_get_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg)
{
  int ret = OB_SUCCESS;
  P2PMsgGetCall call(msg);
  if (OB_FAIL(map_.read_atomic(dh_key, call))) {
    LOG_TRACE("fail to get p2p msg in PX_P2P_DH", K(ret));
  } else if (OB_SUCCESS != call.ret_) {
    ret = call.ret_;
    LOG_TRACE("fail to get p2p msg in PX_P2P_DH", K(ret));
  }
  return ret;
}

int ObP2PDatahubManager::set_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.set_refactored(dh_key, msg))) {
    LOG_WARN("fail to insert p2p dh msg", K(ret));
  }
  return ret;
}

int ObP2PDatahubManager::erase_msg(ObP2PDhKey &dh_key,
    ObP2PDatahubMsgBase *&msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.erase_refactored(dh_key, &msg))) {
    LOG_TRACE("fail to erase from map", K(ret));
  }
  return ret;
}

int ObP2PDatahubManager::erase_msg_if(ObP2PDhKey &dh_key,
    ObP2PDatahubMsgBase *&msg, bool& is_erased, bool need_unreg_dm)
{
  int ret = OB_SUCCESS;
  P2PMsgEraseIfCall erase_if_call;
  if (OB_FAIL(map_.erase_if(dh_key, erase_if_call, is_erased, &msg))) {
    LOG_TRACE("fail to erase if from map", K(ret));
  } else if (is_erased && OB_NOT_NULL(msg)) {
    if (need_unreg_dm) {
      ObDetectManagerUtils::p2p_datahub_unregister_check_item_from_dm(
          msg->get_register_dm_info().detectable_id_, msg->get_dm_cb_node_seq_id());
    }
    PX_P2P_DH.free_msg(msg);
  } else {
    // If erase failed, means other threads still referencing the msg.
    // If the caller is an RPC thread, the clearing task will be delegated to DM;
    // If the caller is DM, the retry policy is utilized to ensure that the message is deleted.
    ret = OB_EAGAIN;
    LOG_WARN("failed to erase msg, other threads still referencing it", K(dh_key), K(need_unreg_dm));
  }
  return ret;
}

int ObP2PDatahubManager::generate_p2p_dh_id(int64_t &p2p_dh_id)
{
  int ret = OB_SUCCESS;
  // generate p2p dh id
  // |    <16>     |      <28>     |     20
  //    server_id       timestamp     sequence
  if (!is_valid_server_id(GCTX.server_id_)) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server id is unexpected", K(ret));
  } else {
    const uint64_t svr_id = GCTX.server_id_;
    int64_t ts = (common::ObTimeUtility::current_time() / 1000000) << 20;
    int64_t seq_id = ATOMIC_AAF(&p2p_dh_id_, 1);
    p2p_dh_id = (ts & 0x0000FFFFFFFFFFFF) | (svr_id << 48) | seq_id;
  }
  return ret;
}

int ObP2PDatahubManager::send_p2p_msg(
    ObP2PDatahubMsgBase &msg,
    ObPxSQCProxy &sqc_proxy)
{
  int ret = OB_SUCCESS;
  int64_t p2p_dh_id = msg.get_p2p_datahub_id();
  ObPxSQCProxy::SQCP2PDhMap &dh_map = sqc_proxy.get_p2p_dh_map();
  ObSArray<ObAddr> *target_addrs = nullptr;
  if (OB_FAIL(dh_map.get_refactored(p2p_dh_id, target_addrs))) {
    LOG_WARN("fail to get dh map", K(ret));
  } else if (OB_ISNULL(target_addrs) || target_addrs->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected target addrs", K(ret));
  } else if (target_addrs->count() == 1 &&
             GCTX.self_addr() == target_addrs->at(0) &&
             1 == msg.get_msg_receive_expect_cnt()) {
    ObP2PDatahubMsgBase *new_msg = nullptr;
    if (OB_FAIL(deep_copy_msg(msg, new_msg))) {
      LOG_WARN("fail to copy msg", K(ret));
    } else if (OB_ISNULL(new_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpeceted new msg", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObP2PDatahubMsgGuard guard(new_msg);
      if (OB_FAIL(send_local_msg(new_msg))) {
        // set failed, which means final_msg is not exists in dh map, let it go
        guard.release();
        if (ret == OB_HASH_EXIST) {
        // it's ok.
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to do local msg send", K(ret));
        }
        if (OB_NOT_NULL(new_msg)) {
          new_msg->destroy();
          ob_free(new_msg);
        }
      } else {
        ObP2PDhKey dh_key(new_msg->get_p2p_datahub_id(),
            new_msg->get_px_seq_id(),
            new_msg->get_task_id(),
            ObTimeUtility::current_time(), new_msg->get_timeout_ts());
        int reg_ret = ObDetectManagerUtils::p2p_datahub_register_check_item_into_dm(
            new_msg->get_register_dm_info(), dh_key, new_msg->get_dm_cb_node_seq_id());
        if (OB_SUCCESS != reg_ret) {
          LOG_WARN("[DM] failed to register check item to dm", K(reg_ret));
        }
        LOG_TRACE("[DM] p2p dh register check item to dm", K(reg_ret), K(new_msg->get_register_dm_info()),
            K(dh_key), K(new_msg->get_dm_cb_node_seq_id()), K(new_msg));
      }
    }
  } else if (OB_FAIL(msg.broadcast(*target_addrs, p2p_dh_proxy_))) {
    LOG_WARN("fail to do broadcast");
  }
  return ret;
}

int ObP2PDatahubManager::send_local_p2p_msg(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(send_local_msg(&msg))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to send local msg", K(ret));
    }
  }
  return ret;
}

void ObP2PDatahubManager::P2PMsgGetCall::operator() (common::hash::HashMapPair<ObP2PDhKey,
    ObP2PDatahubMsgBase *> &entry)
{
  dh_msg_ = entry.second;
  if (OB_NOT_NULL(dh_msg_)) {
    dh_msg_->inc_ref_count();
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dh_msg_ is null", K(ret));
  }
}

bool ObP2PDatahubManager::P2PMsgEraseIfCall::operator() (common::hash::HashMapPair<ObP2PDhKey,
    ObP2PDatahubMsgBase *> &entry)
{
  bool need_erase = false;
  if (OB_NOT_NULL(entry.second)) {
    // only if the ref count is 1, we can decrease ref count to 0 and erase it from map
    if (1 == entry.second->cas_ref_count(1, 0)) {
      need_erase = true;
    }
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dh_msg_ is null", K(ret));
  }
  return need_erase;
}

int ObP2PDatahubManager::P2PMsgSetCall::operator() (const common::hash::HashMapPair<ObP2PDhKey,
    ObP2PDatahubMsgBase *> &entry)
{
  // entry.second == &dh_msg_
  // 1. register into dm
  // 2. do dh_msg_.regenerate()
  UNUSED(entry);
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  if (OB_FAIL(OB_E(EventTable::EN_PX_P2P_MSG_REG_DM_FAILED) OB_SUCCESS)) {
    LOG_WARN("p2p msg reg dm failed by design", K(ret));
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ret_ = ret;
    return ret;
  }
#endif
  if (OB_FAIL(ObDetectManagerUtils::p2p_datahub_register_check_item_into_dm(
      dh_msg_.get_register_dm_info(), dh_key_, dh_msg_.get_dm_cb_node_seq_id()))) {
    LOG_WARN("[DM] failed to register check item to dm", K(dh_msg_.get_register_dm_info()),
        K(dh_key_), K(dh_msg_.get_dm_cb_node_seq_id()));
  } else {
    succ_reg_dm_ = true;
    LOG_TRACE("[DM] rf register check item to dm", K(dh_msg_.get_register_dm_info()),
        K(dh_key_), K(dh_msg_.get_dm_cb_node_seq_id()));
    if (OB_FAIL(dh_msg_.regenerate())) {
      LOG_WARN("failed to do regen_call", K(dh_key_));
    }
  }
  if (OB_FAIL(ret)) {
    (void) revert();
  }
  ret_ = ret;
  return ret;
}

void ObP2PDatahubManager::P2PMsgSetCall::revert()
{
  if (succ_reg_dm_) {
    (void) ObDetectManagerUtils::p2p_datahub_unregister_check_item_from_dm(
        dh_msg_.get_register_dm_info().detectable_id_, dh_msg_.get_dm_cb_node_seq_id());
  }
}
