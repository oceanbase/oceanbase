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

#ifndef STROAGE_TX_DEADLOCK_ADAPTER_OB_REMOTE_EXECUTION_DEADLOCK_CALLBACK_H
#define STROAGE_TX_DEADLOCK_ADAPTER_OB_REMOTE_EXECUTION_DEADLOCK_CALLBACK_H
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/memtable/ob_row_conflict_info.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"

namespace oceanbase
{
namespace transaction
{

class ObTransOnDetectOperation
{
public:
  ObTransOnDetectOperation(const SessionIDPair sess_id_pair,
                           const ObTransID &trans_id)
  : sess_id_pair_(sess_id_pair),
  trans_id_(trans_id) {}
  ~ObTransOnDetectOperation() {}
  int operator()(const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
                 const int64_t self_idx);
  TO_STRING_KV(KP(this), K_(sess_id_pair), K_(trans_id));
private:
  SessionIDPair sess_id_pair_;
  ObTransID trans_id_;
};

class RemoteDeadLockCollectCallBack {
public:
  RemoteDeadLockCollectCallBack() : sess_id_pair_(), trace_id_(), row_conflict_info_array_() {}
  RemoteDeadLockCollectCallBack(const RemoteDeadLockCollectCallBack &) = delete;
  RemoteDeadLockCollectCallBack &operator=(const RemoteDeadLockCollectCallBack &) = delete;
  RemoteDeadLockCollectCallBack(const SessionIDPair sess_id_pair)
  : sess_id_pair_(sess_id_pair), row_conflict_info_array_() {
    trace_id_ = *ObCurTraceId::get_trace_id();
  }
  int assign(const RemoteDeadLockCollectCallBack &rhs) {
    sess_id_pair_ = rhs.sess_id_pair_;
    trace_id_ = rhs.trace_id_;
    return row_conflict_info_array_.assign(rhs.row_conflict_info_array_);
  }
  int operator()(const share::detector::ObDependencyHolder &blocked_hodler,
                 share::detector::ObDetectorUserReportInfo &info);
  int generate_resource_info_(ObRowConflictInfo *conflict_info, share::detector::ObDetectorUserReportInfo &info);
  TO_STRING_KV(K_(sess_id_pair), K_(trace_id), K_(row_conflict_info_array))
public:
  SessionIDPair sess_id_pair_;
  ObCurTraceId::TraceId trace_id_;
  ObArray<storage::ObRowConflictInfo> row_conflict_info_array_;
};

class ObTransDeadLockRemoteExecutionFillVirtualInfoOperation
{
public:
  ObTransDeadLockRemoteExecutionFillVirtualInfoOperation() = default;
  ObTransDeadLockRemoteExecutionFillVirtualInfoOperation(const ObTransDeadLockRemoteExecutionFillVirtualInfoOperation &) = delete;
  ObTransDeadLockRemoteExecutionFillVirtualInfoOperation &operator=(const ObTransDeadLockRemoteExecutionFillVirtualInfoOperation &) = delete;
  ~ObTransDeadLockRemoteExecutionFillVirtualInfoOperation() {}
  int init(const ObIArray<ObRowConflictInfo> &conflict_info_array) {
    return conflict_info_array_.assign(conflict_info_array);
  }
  int operator()(const bool need_fill_conflict_actions_flag,
                 char *buffer,/*to_string buffer*/
                 const int64_t buffer_len,/*to_string buffer length*/
                 int64_t &pos,/*to_string current position*/
                 share::detector::DetectorNodeInfoForVirtualTable &virtual_info/*virtual info to fill*/);
  int assign(const ObTransDeadLockRemoteExecutionFillVirtualInfoOperation &rhs) {
    return conflict_info_array_.assign(rhs.conflict_info_array_);
  }
  TO_STRING_KV(KP(this), K_(conflict_info_array));
private:
  ObArray<ObRowConflictInfo> conflict_info_array_;
};

}
}
#endif