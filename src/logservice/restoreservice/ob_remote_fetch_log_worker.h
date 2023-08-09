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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_WORKER_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_WORKER_H_

#include "lib/queue/ob_lighty_queue.h"                      // ObLightyQueue
#include "common/ob_queue_thread.h"                         // ObCond
#include "share/ob_thread_pool.h"                           // ObThreadPool
#include "share/ob_ls_id.h"                                 // ObLSID
#include "ob_remote_log_iterator.h"                         // ObRemoteLogGroupEntryIterator
#include "logservice/ob_log_external_storage_handler.h"     // ObLogExternalStorageHandler

namespace oceanbase
{
namespace storage
{
class ObLSService;
}

namespace share
{
class SCN;
}

namespace palf
{
struct LSN;
class LogGroupEntry;
}

namespace logservice
{
class ObFetchLogTask;
class ObRemoteSourceGuard;
class ObRemoteLogParent;
class ObLogRestoreService;
class ObLogRestoreAllocator;
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;
// Remote fetch log worker
class ObRemoteFetchWorker : public share::ObThreadPool
{
public:
  ObRemoteFetchWorker();
  ~ObRemoteFetchWorker();

  int init(const uint64_t tenant_id,
      ObLogRestoreAllocator *allocator,
      ObLogRestoreService *restore_service,
      storage::ObLSService *ls_svr);
  void destroy();
  int start();
  void stop();
  void wait();
  void signal();
public:
  // submit fetch log task
  //
  // @retval OB_SIZE_OVERFLOW    task num more than queue limit
  // @retval other code          unexpected error
  int submit_fetch_log_task(ObFetchLogTask *task);

  int modify_thread_count(const int64_t log_restore_concurrency);
  int get_thread_count(int64_t &thread_count) const;

private:
  void run1();
  void do_thread_task_();
  int handle_single_task_();
  int handle_fetch_log_task_(ObFetchLogTask *task);
  bool need_fetch_log_(const share::ObLSID &id);
  void mark_if_to_end_(ObFetchLogTask &task, const share::SCN &upper_limit_scn, const share::SCN &scn);
  void try_update_location_info_(const ObFetchLogTask &task, ObRemoteLogGroupEntryIterator &iter);

  int push_submit_array_(ObFetchLogTask &task);

  int try_retire_(ObFetchLogTask *&task);
  void inner_free_task_(ObFetchLogTask &task);
  bool is_fatal_error_(const int ret_code) const;
  void report_error_(const ObLSID &id,
                     const int ret_code,
                     const palf::LSN &lsn,
                     const ObLogRestoreErrorContext::ErrorType &error_type);
  int64_t calcuate_thread_count_(const int64_t log_restore_concurrency);
private:
  bool inited_;
  uint64_t tenant_id_;
  ObLogRestoreService *restore_service_;
  storage::ObLSService *ls_svr_;
  common::ObLightyQueue task_queue_;
  ObLogRestoreAllocator *allocator_;
  ObLogExternalStorageHandler log_ext_handler_;

  common::ObCond cond_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteFetchWorker);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_WORKER_H_ */
