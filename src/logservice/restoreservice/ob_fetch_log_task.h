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
#include "ob_remote_log_iterator.h"           // Iterator
#include "share/ob_ls_id.h"                   // ObLSID
#include "logservice/palf/lsn.h"              // LSN
#include "share/scn.h"              // SCN
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
  ObFetchLogTask(const share::ObLSID &id,
                 const share::SCN &pre_scn,
                 const palf::LSN &lsn,
                 const int64_t size,
                 const int64_t proposal_id,
                 const int64_t version);

  ~ObFetchLogTask() { reset(); }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(id), K_(proposal_id), K_(version), K_(pre_scn), K_(start_lsn),
      K_(cur_lsn), K_(end_lsn), K_(max_fetch_scn), K_(max_submit_scn), K_(iter));

public:
  share::ObLSID id_;
  // to distinguish stale tasks which is generated in previous leader
  int64_t proposal_id_;
  int64_t version_;
  share::SCN pre_scn_;    // heuristic log scn to locate piece, may be imprecise one
  palf::LSN start_lsn_;
  palf::LSN cur_lsn_;
  palf::LSN end_lsn_;
  share::SCN max_fetch_scn_;     // 拉取日志最大scn
  share::SCN max_submit_scn_;    // 提交日志最大scn
  ObRemoteLogGroupEntryIterator iter_;
};

struct FetchLogTaskCompare final
{
public:
  FetchLogTaskCompare() {}
  ~FetchLogTaskCompare() {}
  bool operator()(const ObFetchLogTask *left, const ObFetchLogTask *right)
  {
    return left->start_lsn_ < right->start_lsn_;
  }
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_RESTORE_TASK_H_ */
