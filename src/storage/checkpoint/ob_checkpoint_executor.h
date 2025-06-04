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

#ifndef OCEANBASE_STORAGE_OB_CHECKPOINT_EXECUTOR_H_
#define OCEANBASE_STORAGE_OB_CHECKPOINT_EXECUTOR_H_

#include "lib/lock/ob_spin_rwlock.h"           // SpinRWLock
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"
#include "share/scn.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_ss_checkpoint_util.h"
#endif

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{

struct ObCheckpointVTInfo
{
  share::SCN rec_scn;
  int service_type;

  TO_STRING_KV(
    K(rec_scn),
    K(service_type)
  );
};

struct CheckpointDiagnoseInfo
{
  CheckpointDiagnoseInfo() { reset(); }
  ~CheckpointDiagnoseInfo() { reset(); }
  share::SCN checkpoint_;
  share::SCN min_rec_scn_;
  logservice::ObLogBaseType log_type_;
  TO_STRING_KV(K(checkpoint_),
               K(min_rec_scn_),
               K(log_type_));
  void reset() {
    checkpoint_.reset();
    min_rec_scn_.reset();
    log_type_ = logservice::ObLogBaseType::INVALID_LOG_BASE_TYPE;
  }
};

class ObCheckpointExecutor
{
public:
  ObCheckpointExecutor();
  virtual ~ObCheckpointExecutor();

  int init(ObLS *ls, logservice::ObILogHandler *ob_loghandler);
  void reset();
  void start();
  void online();
  void offline();

  // services register into checkpoint_executor by logservice frame
  int register_handler(const logservice::ObLogBaseType &type, logservice::ObICheckpointSubHandler *handler);
  void unregister_handler(const logservice::ObLogBaseType &type);

  // calculate clog checkpoint and update in ls_meta
  int update_clog_checkpoint();
  void add_server_event_history_for_update_clog_checkpoint(
      const SCN &checkpoint_scn,
      const char *service_type);

  // the service will flush and advance checkpoint
  // after flush, checkpoint_scn will be equal or greater than recycle_scn
  int advance_checkpoint_by_flush(const share::SCN input_recycle_scn);

  // for __all_virtual_checkpoint
  int get_checkpoint_info(ObIArray<ObCheckpointVTInfo> &checkpoint_array);

  int64_t get_cannot_recycle_log_size();

  void get_min_rec_scn(int &log_type, share::SCN &min_rec_scn) const;

  int diagnose(CheckpointDiagnoseInfo &diagnose_info) const;

  int traversal_flush() const;

private:
  int check_need_flush_(const SCN max_decided_scn, const SCN recycle_scn);
  int calculate_recycle_scn_(const SCN max_decided_scn, SCN &recycle_scn);
  int calculate_min_recycle_scn_(const palf::LSN clog_checkpoint_lsn, SCN &min_recycle_scn);
  int calculate_expected_recycle_scn_(const palf::LSN clog_checkpoint_lsn, SCN &expected_recycle_scn);

private:
  static const int64_t CLOG_GC_PERCENT = 60;
  static const int64_t ADD_SERVER_HISTORY_INTERVAL = 10 * 60 * 1000 * 1000; // 10 min

  ObLS *ls_;
  logservice::ObILogHandler *loghandler_;
  logservice::ObICheckpointSubHandler *handlers_[logservice::ObLogBaseType::MAX_LOG_BASE_TYPE];
  // be used to avoid checkpoint concurrently,
  // no need to protect handlers_[] because ls won't be destroyed(hold lshandle)
  // when the public interfaces are invoked
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;
  RWLock rwlock_;
  RWLock rwlock_for_update_clog_checkpoint_;

  bool update_checkpoint_enabled_;
  int64_t reuse_recycle_scn_times_;

  palf::LSN prev_clog_checkpoint_lsn_;
  share::SCN prev_recycle_scn_;

  int64_t update_clog_checkpoint_times_;
  int64_t last_add_server_history_time_;
#ifdef OB_BUILD_SHARED_STORAGE
  CkptChangeRecord ckpt_change_record_;
#endif
};


}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
#endif
