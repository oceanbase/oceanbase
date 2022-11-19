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

#ifndef OCEANBASE_LOGSERVICE_OB_RESTORE_TASK_H_
#define OCEANBASE_LOGSERVICE_OB_RESTORE_TASK_H_

#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"                   // ObLSID
#include "logservice/palf/lsn.h"              // LSN
#include <cstdint>
namespace oceanbase
{
namespace logservice
{
// The granularity for Restore Service to schedule log fetch.
// ObFetchLogTask is produced by Restore Service and consumed by FetchLogWorkers.
//
// For one ls, more than one tasks can be generated and consumed in parallel,
// which gives to ability to catch up in physical standby and enhance the log fetch speed in physical restore.
class ObFetchLogTask : public common::ObLink
{
public:
  enum Status
  {
    NORMAL = 0,   // 正常状态
    FINISH = 1,   // 该任务包含日志范围完成
    STALE = 2,    // 该任务已经过时(切主导致任务过时, 暂时未使用TODO)
    TO_END = 3,   // 已经拉到任务指定终点
  };

public:
  ObFetchLogTask(const share::ObLSID &id,
                 const int64_t pre_log_ts,
                 const palf::LSN &lsn,
                 const int64_t size,
                 const int64_t proposal_id);

  bool is_valid() const;
  bool is_finish() const { return Status::FINISH == status_; }
  bool is_to_end() const { return Status::TO_END == status_; }
  void mark_to_end() { status_ = Status::TO_END; }
  int update_cur_lsn_ts(const palf::LSN &lsn, const int64_t max_submit_ts, const int64_t max_fetch_ts);
  void set_to_end() { status_ = Status::TO_END; }
  void set_stale() { status_ = Status::STALE; }
  TO_STRING_KV(K_(id), K_(proposal_id), K_(pre_log_ts), K_(start_lsn), K_(cur_lsn), K_(end_lsn),
      K_(max_fetch_log_ts), K_(max_submit_log_ts), K_(status));

public:
  share::ObLSID id_;
  // to distinguish stale tasks which is generated in previous leader
  int64_t proposal_id_;
  int64_t pre_log_ts_;    // heuristic log ts to locate piece, may be imprecise one
  palf::LSN start_lsn_;
  palf::LSN cur_lsn_;
  palf::LSN end_lsn_;
  int64_t max_fetch_log_ts_;     // 拉取日志最大log ts
  int64_t max_submit_log_ts_;    // 提交日志最大log ts
  Status status_;
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_RESTORE_TASK_H_ */
