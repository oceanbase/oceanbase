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

  // the service will flush and advance checkpoint
  // after flush, checkpoint_scn will be equal or greater than recycle_scn
  int advance_checkpoint_by_flush(
      share::SCN recycle_scn = share::SCN::invalid_scn());

  // for __all_virtual_checkpoint
  int get_checkpoint_info(ObIArray<ObCheckpointVTInfo> &checkpoint_array);

  int64_t get_cannot_recycle_log_size();

  void get_min_rec_scn(int &log_type, share::SCN &min_rec_scn) const;

  int diagnose(CheckpointDiagnoseInfo &diagnose_info) const;

  int traversal_flush() const;

private:
  static const int64_t CLOG_GC_PERCENT = 60;

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
};

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
#endif
