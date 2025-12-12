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

#ifndef OCEANBASE_MEMTABLE_DEADLOCK_ADAPTER_OB_LOCAL_EXECUTION_DEADLOCK_CALLBACK_H
#define OCEANBASE_MEMTABLE_DEADLOCK_ADAPTER_OB_LOCAL_EXECUTION_DEADLOCK_CALLBACK_H
#include "share/ob_define.h"
#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/ob_ls_id.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_seq.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"

namespace oceanbase
{
namespace memtable
{
class RowHolderMapper;

class DeadLockBlockCallBack {
public:
  DeadLockBlockCallBack(RowHolderMapper &mapper, uint64_t hash)
  : mapper_(mapper),
  hash_(hash) {}
  int operator()(ObIArray<share::detector::ObDependencyHolder> &resource_array, bool &need_remove);
private:
  RowHolderMapper &mapper_;
  uint64_t hash_;
};

class LocalDeadLockCollectCallBack {
public:
  static constexpr int64_t NODE_KEY_BUFFER_MAX_LENGTH = rpc::ObLockWaitNode::KEY_BUFFER_SIZE;
  LocalDeadLockCollectCallBack(const transaction::ObTransID &self_trans_id,
                               const char *node_key_buffer,
                               const transaction::SessionIDPair sess_id_pair,
                               const int64_t ls_id,
                               const uint64_t tablet_id,
                               const transaction::ObTxSEQ &blocked_holder_tx_hold_seq);
  int operator()(const share::detector::ObDependencyHolder &, share::detector::ObDetectorUserReportInfo &info);
private:
  transaction::ObTransID self_trans_id_;
  char node_key_buffer_[NODE_KEY_BUFFER_MAX_LENGTH];
  transaction::SessionIDPair sess_id_pair_;
  const int64_t ls_id_;
  const uint64_t tablet_id_;
  const transaction::ObTxSEQ blocked_holder_tx_hold_seq_;
  ObCurTraceId::TraceId trace_id_;
};

class LocalExecutionWaitingForRowFillVirtualInfoOperation
{
public:
  LocalExecutionWaitingForRowFillVirtualInfoOperation(RowHolderMapper &mapper,
                                                      const share::ObLSID ls_id,
                                                      const uint64_t hash)
  : mapper_(mapper),
  ls_id_(ls_id),
  hash_(hash) {}
  LocalExecutionWaitingForRowFillVirtualInfoOperation(const LocalExecutionWaitingForRowFillVirtualInfoOperation &) = default;
  LocalExecutionWaitingForRowFillVirtualInfoOperation &operator=(const LocalExecutionWaitingForRowFillVirtualInfoOperation &) = delete;
  ~LocalExecutionWaitingForRowFillVirtualInfoOperation() {}
  int operator()(const bool need_fill_conflict_actions_flag,
                 char *buffer,/*to_string buffer*/
                 const int64_t buffer_len,/*to_string buffer length*/
                 int64_t &pos,/*to_string current position*/
                 share::detector::DetectorNodeInfoForVirtualTable &virtual_info/*virtual info to fill*/);
  TO_STRING_KV(KP(this), K_(hash));
private:
  RowHolderMapper &mapper_;
  share::ObLSID ls_id_;
  uint64_t hash_;
};

class LocalExecutionWaitingForTransFillVirtualInfoOperation
{
public:
  LocalExecutionWaitingForTransFillVirtualInfoOperation(share::ObLSID ls_id,
                                                        transaction::ObTransID conflict_tx_id,
                                                        transaction::ObTxSEQ conflict_tx_seq)
  : ls_id_(ls_id),
  conflict_tx_id_(conflict_tx_id),
  conflict_tx_seq_(conflict_tx_seq) {}
  LocalExecutionWaitingForTransFillVirtualInfoOperation(const LocalExecutionWaitingForTransFillVirtualInfoOperation &) = default;
  LocalExecutionWaitingForTransFillVirtualInfoOperation &operator=(const LocalExecutionWaitingForTransFillVirtualInfoOperation &) = default;
  ~LocalExecutionWaitingForTransFillVirtualInfoOperation() {}
  int operator()(const bool need_fill_conflict_actions_flag,
                 char *buffer,/*to_string buffer*/
                 const int64_t buffer_len,/*to_string buffer length*/
                 int64_t &pos,/*to_string current position*/
                 share::detector::DetectorNodeInfoForVirtualTable &virtual_info/*virtual info to fill*/);
  TO_STRING_KV(KP(this), K_(conflict_tx_id), K_(conflict_tx_seq));
private:
  share::ObLSID ls_id_;
  transaction::ObTransID conflict_tx_id_;
  transaction::ObTxSEQ conflict_tx_seq_;
};

class RemoteExecutionSideNodeDeadLockCollectCallBack {
public:
  static constexpr int64_t NODE_KEY_BUFFER_MAX_LENGTH = rpc::ObLockWaitNode::KEY_BUFFER_SIZE;
  static constexpr int64_t QUERY_SQL_BUFFER_MAX_LENGTH = 256;
  RemoteExecutionSideNodeDeadLockCollectCallBack(const transaction::ObTransID &self_trans_id,
                               const char *node_key_buffer,
                               const ObString &query_sql,
                               const transaction::SessionIDPair sess_id_pair,
                               const int64_t ls_id,
                               const uint64_t tablet_id,
                               const transaction::ObTxSEQ &blocked_holder_tx_hold_seq);
  int operator()(const share::detector::ObDependencyHolder &, share::detector::ObDetectorUserReportInfo &info);
private:
  transaction::ObTransID self_trans_id_;
  char node_key_buffer_[NODE_KEY_BUFFER_MAX_LENGTH];
  transaction::SessionIDPair sess_id_pair_;
  const int64_t ls_id_;
  const uint64_t tablet_id_;
  const transaction::ObTxSEQ blocked_holder_tx_hold_seq_;
  ObCurTraceId::TraceId trace_id_;
  char query_sql_buffer_[QUERY_SQL_BUFFER_MAX_LENGTH];
};

}
}
#endif