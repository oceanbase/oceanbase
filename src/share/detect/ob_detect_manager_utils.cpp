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

#include "share/detect/ob_detect_manager_utils.h"
#include "share/detect/ob_detect_manager.h"
#include "sql/engine/px/ob_dfo.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/ob_px_scheduler.h"

using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

namespace oceanbase {
namespace common {

int ObDetectManagerUtils::generate_detectable_id(ObDetectableId &detectable_id, uint64_t tenant_id) {
  return ObDetectableIdGen::instance().generate_detectable_id(detectable_id, tenant_id);
}

void ObDetectManagerUtils::prepare_register_dm_info(common::ObRegisterDmInfo &register_dm_info,
                                                    ObPxSqcHandler *handler)
{
  register_dm_info.detectable_id_ = handler->get_sqc_init_arg().sqc_.get_px_detectable_ids().qc_detectable_id_;
  register_dm_info.addr_ = handler->get_sqc_init_arg().sqc_.get_qc_addr();
}

int ObDetectManagerUtils::qc_register_detectable_id_into_dm(ObDetectableId &detectable_id,
                                                            bool &register_detectable_id,
                                                            uint64_t tenant_id,
                                                            ObPxCoordInfo& coord_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDetectManagerUtils::generate_detectable_id(detectable_id, tenant_id))) {
    LIB_LOG(WARN, "[DM] failed to generate_detectable_id", K(tenant_id));
  } else if (FALSE_IT(coord_info.qc_detectable_id_ = detectable_id)) {
  } else {
    ObDetectManager* dm = MTL(ObDetectManager*);
    if (OB_ISNULL(dm)) {
      // ignore ret
      LIB_LOG(WARN, "[DM] dm is null", K(tenant_id));
    } else if (OB_FAIL(dm->register_detectable_id(detectable_id))) {
      LIB_LOG(WARN, "[DM] QC fail to register detectable_id", K(ret), K(detectable_id), K(tenant_id));
    } else {
      register_detectable_id = true;
    }
    LIB_LOG(TRACE, "[DM] QC register detectable_id_", K(ret), K(detectable_id), K(tenant_id));
  }
  return ret;
}

void ObDetectManagerUtils::qc_unregister_detectable_id_from_dm(const ObDetectableId &detectable_id,
                                                               bool &register_detectable_id)
{
  if (register_detectable_id) {
    int ret = OB_SUCCESS;
    ObDetectManager* dm = MTL(ObDetectManager*);
    if (OB_ISNULL(dm)) {
      LIB_LOG(WARN, "[DM] dm is null", K(detectable_id));
    } else {
      ret = dm->unregister_detectable_id(detectable_id);
    }
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LIB_LOG(WARN, "[DM] QC failed to unregister_detectable_id", K(ret), K(detectable_id));
    }
    LIB_LOG(TRACE, "[DM] QC unregister detectable_id_", K(ret), K(detectable_id));
    register_detectable_id = false;
  }
}

int ObDetectManagerUtils::qc_register_check_item_into_dm(ObDfo &dfo,
                                                         const ObIArray<ObPeerTaskState> &peer_states,
                                                         const ObIArray<ObDtlChannel *> &dtl_channels)
{
  int ret = OB_SUCCESS;
  ObDetectableId sqc_detectable_id = dfo.get_px_detectable_ids().sqc_detectable_id_;
  ObQcDetectCB *cb = nullptr;
  uint64_t node_sequence_id = 0;
  ObDetectManager* dm = MTL(ObDetectManager*);
  if (OB_ISNULL(dm)) {
    // ignore ret
    LIB_LOG(WARN, "[DM] dm is null", K(sqc_detectable_id));
  } else if (OB_SUCC(dm->register_check_item(
      sqc_detectable_id, cb, node_sequence_id, true,
      peer_states, dfo.get_interrupt_id().query_interrupt_id_, dfo, dtl_channels))) {
    dfo.set_detect_cb(cb);
    dfo.set_node_sequence_id(node_sequence_id);
  } else {
    LIB_LOG(WARN, "[DM] qc failed register check item", K(sqc_detectable_id));
  }
  return ret;
}

void ObDetectManagerUtils::qc_unregister_check_item_from_dm(sql::ObDfo *dfo, ObDetectManager* dm)
{
  int ret = OB_SUCCESS;
  ObIDetectCallback *cb = dfo->get_detect_cb();
  const common::ObDetectableId &sqc_detectable_id = dfo->get_px_detectable_ids().sqc_detectable_id_;
  if (OB_NOT_NULL(cb)) {
    // only if ref_count_ decrease to 0, unregister cb
    if (0 == cb->dec_ref_count()) {
      if (OB_ISNULL(dm)) {
        ObDetectManager* dm = MTL(ObDetectManager*);
        if (OB_ISNULL(dm)) {
          LIB_LOG(WARN, "[DM] dm is null", K(sqc_detectable_id));
        } else {
          int ret = dm->unregister_check_item(sqc_detectable_id, dfo->get_node_sequence_id());
          if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
            LIB_LOG(WARN, "[DM] qc failed to unregister_check_item ", K(ret),
                K(sqc_detectable_id), K(dfo->get_node_sequence_id()));
          }
        }
      } else {
        int ret = dm->unregister_check_item(sqc_detectable_id, dfo->get_node_sequence_id());
        if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
          LIB_LOG(WARN, "[DM] qc failed to unregister_check_item ", K(ret),
              K(sqc_detectable_id), K(dfo->get_node_sequence_id()));
        }
      }
    }
    dfo->set_detect_cb(nullptr);
  }
}

void ObDetectManagerUtils::qc_unregister_all_check_items_from_dm(const ObIArray<ObDfo *> &dfos)
{
  int ret = OB_SUCCESS;
  ObDetectManager* dm = MTL(ObDetectManager*);
  if (OB_ISNULL(dm)) {
    // ignore ret
    LIB_LOG(WARN, "[DM] dm is null");
  } else {
    for (int64_t idx = 0; idx < dfos.count(); ++idx) {
      ObDfo *dfo = dfos.at(idx);
      if (OB_NOT_NULL(dfo)) {
        ObDetectManagerUtils::qc_unregister_check_item_from_dm(dfo, dm);
      }
    }
  }
}

int ObDetectManagerUtils::sqc_register_into_dm(ObPxSqcHandler *sqc_handler, ObPxSqcMeta &sqc)
{
  int ret = OB_SUCCESS;
  const ObDetectableId &sqc_detectable_id = sqc.get_px_detectable_ids().sqc_detectable_id_;
  const ObDetectableId &qc_detectable_id = sqc.get_px_detectable_ids().qc_detectable_id_;
  // 1.register sqc's state as running, MUST be success or qc be interrupted by dm,
  // must be success or sql being interrupted unexpectedly
  ObDetectManager* dm = MTL(ObDetectManager*);
  if (OB_ISNULL(dm)) {
    // ignore ret
    LIB_LOG(WARN, "[DM] dm is null");
  } else if (OB_FAIL(dm->register_detectable_id(sqc_detectable_id))) {
    LIB_LOG(WARN, "[DM] sqc failed to register_detectable_id", K(ret), K(sqc_detectable_id), K(qc_detectable_id));
  } else {
  // 2.register a check item for detecting qc,
  // can be failed and not return error code, only drop the ability to detect qc
    ObSEArray<ObPeerTaskState, 1> peer_states;
    if (OB_SUCCESS != peer_states.push_back(ObPeerTaskState(sqc.get_qc_addr()))) {
      LIB_LOG(WARN, "[DM] failed to push_back", K(ret), K(sqc_detectable_id), K(qc_detectable_id));
    } else {
      uint64_t node_sequence_id = 0;
      ObSqcDetectCB *cb = nullptr;
      const ObInterruptibleTaskID &px_interrupt_id = sqc.get_interrupt_id().px_interrupt_id_;
      int reg_ret = dm->register_check_item(
                    qc_detectable_id, cb, node_sequence_id, false, peer_states, px_interrupt_id);
      if (OB_SUCCESS != reg_ret) {
        LIB_LOG(WARN, "[DM] sqc failed to register_check_item", K(reg_ret), K(sqc_detectable_id), K(qc_detectable_id));
      } else {
        // for unregister check item in ObPxSqcHandler::destroy_sqc()
        sqc_handler->set_node_sequence_id(node_sequence_id);
      }
    }
  }
  LIB_LOG(TRACE, "[DM] sqc register dm", K(ret), K(sqc_detectable_id), K(qc_detectable_id));
  return ret;
}

void ObDetectManagerUtils::sqc_unregister_detectable_id_from_dm(const common::ObDetectableId &detectable_id)
{
  int ret = OB_SUCCESS;
  ObDetectManager* dm = MTL(ObDetectManager*);
  if (OB_ISNULL(dm)) {
    LIB_LOG(WARN, "[DM] dm is null", K(detectable_id));
  } else {
    ret = dm->unregister_detectable_id(detectable_id);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LIB_LOG(WARN, "[DM] failed to unregister_detectable_id ", K(ret), K(detectable_id));
    }
  }
  LIB_LOG(TRACE, "[DM] sqc unregister detectable_id_", K(ret), K(detectable_id));
}

void ObDetectManagerUtils::sqc_unregister_check_item_from_dm(const ObDetectableId &detectable_id,
                                                             const uint64_t &node_sequence_id)
{
  int ret = OB_SUCCESS;
  ObDetectManager* dm = MTL(ObDetectManager*);
  if (OB_ISNULL(dm)) {
    LIB_LOG(WARN, "[DM] dm is null", K(detectable_id));
  } else {
    ret = dm->unregister_check_item(detectable_id, node_sequence_id);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LIB_LOG(WARN, "[DM] sqc failed to unregister_check_item ",
                K(ret), K(detectable_id), K(node_sequence_id));
    }
  }
}

int ObDetectManagerUtils::single_dfo_register_check_item_into_dm(const common::ObRegisterDmInfo &register_dm_info,
                                                                 const ObDTLIntermResultKey &key,
                                                                 ObDTLIntermResultInfo *result_info)
{
  int ret = OB_SUCCESS;
  if (register_dm_info.is_valid()) {
    uint64_t node_sequence_id = 0;
    common::ObSingleDfoDetectCB *cb = nullptr;
    ObSEArray<ObPeerTaskState, 1> peer_states;
    if (OB_FAIL(peer_states.push_back(ObPeerTaskState{register_dm_info.addr_}))) {
      LIB_LOG(WARN, "[DM] failed to push_back to peer_states", K(ret), K(register_dm_info), K(key));
    }
    if (OB_SUCC(ret)) {
      uint64_t tenant_id = register_dm_info.detectable_id_.tenant_id_;
      MTL_SWITCH(tenant_id) {
        ObDetectManager* dm = MTL(ObDetectManager*);
        if (OB_ISNULL(dm)) {
          // ignore ret
          LIB_LOG(WARN, "[DM] dm is null", K(tenant_id));
        } else if (OB_FAIL(dm->register_check_item(
                          register_dm_info.detectable_id_, cb, node_sequence_id, false,
                          peer_states, key))) {
          LIB_LOG(WARN, "[DM] single dfo register_check_item failed ", K(ret), K(register_dm_info), K(key));
        } else {
          result_info->unregister_dm_info_.detectable_id_ = register_dm_info.detectable_id_;
          result_info->unregister_dm_info_.node_sequence_id_ = node_sequence_id;
        }
      }
    }
  }
  return ret;
}

int ObDetectManagerUtils::temp_table_register_check_item_into_dm(const common::ObDetectableId &qc_detectable_id,
                                                                 const common::ObAddr &qc_addr,
                                                                 const dtl::ObDTLIntermResultKey &dtl_int_key,
                                                                 ObDTLIntermResultInfo *&row_store)
{
  int ret = OB_SUCCESS;
  uint64_t node_sequence_id = 0;
  common::ObTempTableDetectCB *cb = nullptr;
  ObSEArray<ObPeerTaskState, 1> peer_states;
  if (OB_FAIL(peer_states.push_back(ObPeerTaskState{qc_addr}))) {
    LIB_LOG(WARN, "[DM] failed to push_back", K(ret), K(qc_detectable_id),
        K(qc_addr), K(dtl_int_key));
  }
  if (OB_SUCC(ret)) {
    ObDetectManager* dm = MTL(ObDetectManager*);
    if (OB_ISNULL(dm)) {
      // ignore ret
      LIB_LOG(WARN, "[DM] dm is null", K(qc_detectable_id));
    } else if (OB_FAIL(dm->register_check_item(
          qc_detectable_id, cb, node_sequence_id, false,
          peer_states, dtl_int_key))) {
      LIB_LOG(WARN, "[DM] temp table register_check_item failed ", K(ret),
          K(qc_detectable_id), K(qc_addr), K(dtl_int_key));
    } else {
      row_store->unregister_dm_info_.detectable_id_ = qc_detectable_id;
      row_store->unregister_dm_info_.node_sequence_id_ = node_sequence_id;
    }
  }
  return ret;
}

void ObDetectManagerUtils::intern_result_unregister_check_item_from_dm(ObDTLIntermResultInfo *result_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_info) && result_info->unregister_dm_info_.is_valid()) {
    uint64_t tenant_id = result_info->unregister_dm_info_.detectable_id_.tenant_id_;
    MTL_SWITCH(tenant_id) {
      ObDetectManager* dm = MTL(ObDetectManager*);
      if (OB_ISNULL(dm)) {
        LIB_LOG(WARN, "[DM] dm is null", K(tenant_id));
      } else {
        ret = dm->unregister_check_item(result_info->unregister_dm_info_.detectable_id_,
                                                          result_info->unregister_dm_info_.node_sequence_id_);
        if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
          LIB_LOG(WARN, "[DM] failed to unregister_check_item ", K(ret), K(result_info->unregister_dm_info_));
        }
      }
    }
  }
}

int ObDetectManagerUtils::p2p_datahub_register_check_item_into_dm(const common::ObRegisterDmInfo &register_dm_info,
    const sql::ObP2PDhKey &p2p_key, uint64_t &node_sequence_id)
{
  int ret = OB_SUCCESS;
  ObP2PDataHubDetectCB *cb = nullptr;
  const common::ObDetectableId &qc_detectable_id = register_dm_info.detectable_id_;
  const common::ObAddr &qc_addr = register_dm_info.addr_;

  ObSEArray<ObPeerTaskState, 1> peer_states;
  if (OB_FAIL(peer_states.push_back(ObPeerTaskState{qc_addr}))) {
    LIB_LOG(WARN, "[DM] failed to push_back", K(ret), K(qc_detectable_id),
        K(qc_addr), K(p2p_key));
  } else {
    ObDetectManager* dm = MTL(ObDetectManager*);
    if (OB_ISNULL(dm)) {
      // ignore ret
      LIB_LOG(WARN, "[DM] dm is null", K(qc_detectable_id));
    } else if (OB_SUCC(dm->register_check_item(
        qc_detectable_id, cb, node_sequence_id, false,
        peer_states, p2p_key))) {
    } else {
      LIB_LOG(WARN, "[DM] p2p datahub failed register check item", K(qc_detectable_id));
    }
  }
  return ret;
}

void ObDetectManagerUtils::p2p_datahub_unregister_check_item_from_dm(const common::ObDetectableId &detectable_id,
    uint64_t node_sequence_id)
{
  int ret = OB_SUCCESS;
  if (!detectable_id.is_invalid() && 0 != node_sequence_id) {
    uint64_t tenant_id = detectable_id.tenant_id_;
    MTL_SWITCH(tenant_id) {
      ObDetectManager* dm = MTL(ObDetectManager*);
      if (OB_ISNULL(dm)) {
        LIB_LOG(WARN, "[DM] dm is null", K(tenant_id));
      } else {
        ret = dm->unregister_check_item(detectable_id, node_sequence_id);
        if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
          LIB_LOG(WARN, "[DM] failed to unregister_check_item ", K(ret), K(detectable_id));
        }
      }
    }
  }
}

} // end namespace common
} // end namespace oceanbase
