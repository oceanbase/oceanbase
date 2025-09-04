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
#pragma once

#include "lib/net/ob_addr.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "share/detect/ob_detectable_id.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"
#include "lib/container/ob_se_array.h"
#include "lib/list/ob_dlink_node.h"

namespace oceanbase {

namespace sql {
class ObDfo;
class ObPxSqcMeta;
class ObSQLSessionInfo;
namespace dtl {
class ObDtlChannel;
}
}

namespace common {

enum class ObTaskState
{
  RUNNING = 0,
  FINISHED = 1,
};

struct ObPeerTaskState {
  ObPeerTaskState() : peer_addr_(), peer_state_(ObTaskState::RUNNING) {}
  explicit ObPeerTaskState(const common::ObAddr &addr) : peer_addr_(addr), peer_state_(ObTaskState::RUNNING) {}
  ObPeerTaskState(const common::ObAddr &addr, const ObTaskState &state) : peer_addr_(addr), peer_state_(state) {}
  void operator=(const ObPeerTaskState &other)
  {
    peer_addr_ = other.peer_addr_;
    peer_state_ = other.peer_state_;
  }
  inline int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, len, pos, "peer_state: (%d)", peer_state_);
    pos += peer_addr_.to_string(buf + pos, len - pos);
    return pos;
  }
  common::ObAddr peer_addr_;
  ObTaskState peer_state_;
};

#define DETECT_CALLBACK_TYPE(ACT)                                                                  \
  ACT(VIRTUAL, = 0)                                                                                \
  ACT(QC_DETECT_CB, )                                                                              \
  ACT(SQC_DETECT_CB, )                                                                             \
  ACT(SINGLE_DFO_DETECT_CB, )                                                                      \
  ACT(TEMP_TABLE_DETECT_CB, )                                                                      \
  ACT(P2P_DATAHUB_DETECT_CB, )                                                                     \
  ACT(DAS_REMOTE_TASK_DETECT_CB, )                                                                 \
  ACT(REMOTE_SQL_EXECUTION_DETECT_CB, )                                                            \
  ACT(MAX_TYPE, )

DECLARE_ENUM(DetectCallBackType, detect_callback_type, DETECT_CALLBACK_TYPE);

// detectable id with activate time, used for delay detect
class ObDetectableIdDNode : public common::ObDLinkBase<ObDetectableIdDNode>
{
public:
  ObDetectableIdDNode() : detectable_id_(), activate_tm_(0) {}
  ObDetectableId detectable_id_;
  int64_t activate_tm_;
  TO_STRING_KV(K_(detectable_id), K_(activate_tm));
};

class ObIDetectCallback : public common::ObDLinkBase<ObIDetectCallback>
{
public:
  // constructor for pass peer_states from derived class
  ObIDetectCallback(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states);
  virtual void destroy()
  {
    peer_states_.reset();
  }
  virtual int do_callback() = 0;
  virtual const char *get_type() const = 0;
  virtual bool reentrant() const { return false; }

  ObIArray<ObPeerTaskState> &get_peer_states() { return peer_states_; }

  // set peer state to finished and get the old state
  virtual int atomic_set_finished(const common::ObAddr &addr, ObTaskState *state=nullptr);
  // if do_callback failed, reset state to running for next detect loop
  virtual int atomic_set_running(const common::ObAddr &addr);
  int64_t get_ref_count() { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count(int64_t count = 1);
  int64_t dec_ref_count();

  void set_from_svr_addr(const common::ObAddr &from) { from_svr_addr_ = from; }
  const common::ObAddr &get_from_svr_addr() { return from_svr_addr_; }

  void set_trace_id(const common::ObCurTraceId::TraceId &trace_id) { trace_id_ = trace_id; }
  const common::ObCurTraceId::TraceId & get_trace_id() { return trace_id_; }

  bool alloc_succ() { return alloc_succ_; }
  uint64_t get_sequence_id() { return sequence_id_; }
  void set_sequence_id(uint64_t v) { sequence_id_ = v; }
  void set_executed() { executed_ = true; }
  bool is_executed() { return executed_; }
  inline int64_t to_string(char *buf, const int64_t len) const { return 0; }
private:
  int64_t ref_count_;
  ObSEArray<ObPeerTaskState, 8, common::ModulePageAllocator, true> peer_states_;
protected:
  common::ObAddr from_svr_addr_; // in which server the task is detected as finished
  common::ObCurTraceId::TraceId trace_id_;
  bool alloc_succ_;
  uint64_t sequence_id_;
  bool executed_;
public:
  ObDetectableIdDNode d_node_; // used for delay detect
};

class ObQcDetectCB : public ObIDetectCallback
{
public:
  ObQcDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states, const ObInterruptibleTaskID &tid, sql::ObDfo &dfo,
      const ObIArray<sql::dtl::ObDtlChannel *> &dtl_channels);
  void destroy() override;
  int do_callback() override;
  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::QC_DETECT_CB);
  }
  // this DetectCallback is reentrant, because several sqcs can be detected as finished and should set_need_report(false)
  bool reentrant() const override { return true; }
  int atomic_set_finished(const common::ObAddr &addr, ObTaskState *state=nullptr) override;
private:
  ObInterruptibleTaskID tid_;
  sql::ObDfo &dfo_;
  int64_t timeout_ts_;
  ObSEArray<sql::dtl::ObDtlChannel *, 8, common::ModulePageAllocator, true> dtl_channels_;
};

class ObSqcDetectCB : public ObIDetectCallback
{
public:
  ObSqcDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states, const ObInterruptibleTaskID &tid)
      : ObIDetectCallback(tenant_id, peer_states), tid_(tid) {}

  int do_callback() override;
  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::SQC_DETECT_CB);
  }
private:
  ObInterruptibleTaskID tid_;
};

class ObSingleDfoDetectCB : public ObIDetectCallback
{
public:
  ObSingleDfoDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states, const sql::dtl::ObDTLIntermResultKey &key)
    : ObIDetectCallback(tenant_id, peer_states), key_(key) {}

  int do_callback() override;
  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::SINGLE_DFO_DETECT_CB);
  }
private:
  sql::dtl::ObDTLIntermResultKey key_;
};

class ObTempTableDetectCB : public ObIDetectCallback
{
public:
  ObTempTableDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states, const sql::dtl::ObDTLIntermResultKey &key)
      : ObIDetectCallback(tenant_id, peer_states), key_(key) {}

  int do_callback() override;
  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::TEMP_TABLE_DETECT_CB);
  }
private:
  sql::dtl::ObDTLIntermResultKey key_;
};

class ObP2PDataHubDetectCB : public ObIDetectCallback
{
public:
  ObP2PDataHubDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states, const sql::ObP2PDhKey &key)
      : ObIDetectCallback(tenant_id, peer_states), key_(key) {}

  int do_callback() override;
  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::P2P_DATAHUB_DETECT_CB);
  }
private:
  sql::ObP2PDhKey key_;
};


class ObDASRemoteTaskDetectCB : public ObIDetectCallback
{
  public:
  ObDASRemoteTaskDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states, sql::DASTCBInfo key, const ObInterruptibleTaskID &tid)
      : ObIDetectCallback(tenant_id, peer_states), key_(key), tid_(tid) {}

  int do_callback() override;
  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::DAS_REMOTE_TASK_DETECT_CB);
  }
private:
  sql::DASTCBInfo key_;
  ObInterruptibleTaskID tid_;
};

class ObRemoteSqlDetectCB : public ObIDetectCallback
{
public:
  ObRemoteSqlDetectCB(uint64_t tenant_id, const ObIArray<ObPeerTaskState> &peer_states,
                      ObSQLSessionInfo *session)
      : ObIDetectCallback(tenant_id, peer_states), session_(session)
  {}

  const char *get_type() const override
  {
    return get_detect_callback_type_string(DetectCallBackType::REMOTE_SQL_EXECUTION_DETECT_CB);
  }

  int do_callback() override;

private:
  ObSQLSessionInfo *session_;
};

} // end namespace common
} // end namespace oceanbase
