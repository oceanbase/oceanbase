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

#include "lib/lock/ob_spin_lock.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{

struct ObCheckpointVTInfo
{
  int64_t rec_log_ts;
  int service_type;

  TO_STRING_KV(
    K(rec_log_ts),
    K(service_type)
  );
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
  // after flush, checkpoint_log_ts will be equal or greater than recycle_ts
  int advance_checkpoint_by_flush(int64_t recycle_ts = 0);

  // for __all_virtual_checkpoint
  int get_checkpoint_info(ObIArray<ObCheckpointVTInfo> &checkpoint_array);

  // avoid need replay too mang logs
  bool need_flush();

  bool is_wait_advance_checkpoint();

  void set_wait_advance_checkpoint(int64_t checkpoint_log_ts);

  int64_t get_cannot_recycle_log_size();

private:
  static const int64_t CLOG_GC_PERCENT = 60;
  static const int64_t MAX_NEED_REPLAY_CLOG_INTERVAL = (int64_t)60 * 60 * 1000 * 1000 * 1000; //ns

  ObLS *ls_;
  logservice::ObILogHandler *loghandler_;
  logservice::ObICheckpointSubHandler *handlers_[logservice::ObLogBaseType::MAX_LOG_BASE_TYPE];
  // be used to avoid checkpoint concurrently,
  // no need to protect handlers_[] because ls won't be destroyed(hold lshandle)
  // when the public interfaces are invoked
  common::ObSpinLock lock_;

  // avoid frequent freeze when clog_used_over_threshold
  bool wait_advance_checkpoint_;
  int64_t last_set_wait_advance_checkpoint_time_;
  bool update_checkpoint_enabled_;
};

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
#endif
