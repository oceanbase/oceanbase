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

#ifndef OCEANBASE_LOG_FETCHER_LS_CTX_ADDITIONAL_INFO_H_
#define OCEANBASE_LOG_FETCHER_LS_CTX_ADDITIONAL_INFO_H_

#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"
#include "logservice/common_util/ob_log_ls_define.h"
#include "lib/lock/ob_small_spin_lock.h"  // ObByteLock
#include "logservice/logfetcher/ob_log_ls_progress.h"

namespace oceanbase
{
namespace logfetcher
{
// 前向声明，避免循环依赖
class LSFetchCtx;

struct PartTransDispatchInfo
{
  PartTransDispatchInfo() :
    last_dispatch_log_lsn_(),
    current_checkpoint_(OB_INVALID_VERSION),
    pending_task_count_(0),
    task_count_in_queue_(0),
    next_task_type_("INVALID"),
    next_trans_log_lsn_(),
    next_trans_committed_(false),
    next_trans_ready_to_commit_(false),
    next_trans_global_version_(OB_INVALID_VERSION)
  {}

  palf::LSN   last_dispatch_log_lsn_;
  int64_t     current_checkpoint_;
  int64_t     pending_task_count_;        // The total number of tasks waiting, both in the queue and in the Map
  int64_t     task_count_in_queue_;       // Number of queued tasks

  const char  *next_task_type_;
  palf::LSN   next_trans_log_lsn_;
  bool        next_trans_committed_;
  bool        next_trans_ready_to_commit_;
  int64_t     next_trans_global_version_;

  TO_STRING_KV(
      K_(last_dispatch_log_lsn),
      K_(current_checkpoint),
      K_(pending_task_count),
      K_(task_count_in_queue),
      K_(next_task_type),
      K_(next_trans_log_lsn),
      K_(next_trans_committed),
      K_(next_trans_ready_to_commit),
      K_(next_trans_global_version));
};

class ObILogFetcherLSCtxAddInfo
{
public:
  virtual ~ObILogFetcherLSCtxAddInfo() {}

  virtual int init(
      const logservice::TenantLSID &ls_id,
      const int64_t start_commit_version) = 0;

  /// get tps info of current LS
  virtual double get_tps() = 0;

  /// get dispatch progress and dispatch info of current LS
  virtual int get_dispatch_progress(
      const share::ObLSID &ls_id,
      int64_t &progress,
      PartTransDispatchInfo &dispatch_info) = 0;

  /// sync/dispatch ready tasks to downstream
  /// This method should be called after reading logs to dispatch ready tasks
  /// @param [in] stop_flag  stop flag
  /// @param [out] pending_task_count  number of pending tasks
  /// @retval OB_SUCCESS  success
  /// @retval OB_IN_STOP_STATE  in stop state
  /// @retval other error codes  failed
  virtual int sync(volatile bool &stop_flag, int64_t &pending_task_count, LSProgress &progress, int64_t &last_sync_progress) = 0;

  virtual int offline(volatile bool &stop_flag) = 0;
};
} // namespace logfetcher
} // namespace oceanbase

#endif
