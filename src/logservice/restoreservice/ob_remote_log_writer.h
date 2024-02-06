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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_WRITER_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_WRITER_H_

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_ls_id.h"
#include "share/ob_thread_pool.h"                           // ObThreadPool
#include "ob_log_restore_define.h"

namespace oceanbase
{
namespace storage
{
class ObLSService;
}
namespace share
{
class ObLSID;
class SCN;
}
namespace palf
{
class LSN;
}
namespace logservice
{
class ObFetchLogTask;
class ObRemoteFetchWorker;
class ObLogRestoreService;
class ObRemoteLogWriter : public share::ObThreadPool
{
public:
  ObRemoteLogWriter();
  virtual ~ObRemoteLogWriter();

public:
  int init(const uint64_t tenant_id,
      storage::ObLSService *ls_svr,
      ObLogRestoreService *restore_service,
      ObRemoteFetchWorker *worker);
  void destroy();
  int start();
  void stop();
  void wait();

private:
  void run1();
  void do_thread_task_();
  int foreach_ls_(const share::ObLSID &id);
  int submit_entries_(ObFetchLogTask &task);
  int submit_log_(const share::ObLSID &id, const int64_t proposal_id, const palf::LSN &lsn,
      const share::SCN &scn, const char *buf, const int64_t buf_size);
  int try_retire_(ObFetchLogTask *&task);
  void inner_free_task_(ObFetchLogTask &task);
  void report_error_(const share::ObLSID &id,
                     const int ret_code,
                     const palf::LSN &lsn,
                     const ObLogRestoreErrorContext::ErrorType &error_type);

private:
  bool inited_;
  uint64_t tenant_id_;
  storage::ObLSService *ls_svr_;
  ObLogRestoreService *restore_service_;
  ObRemoteFetchWorker *worker_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogWriter);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_WRITER_H_ */
