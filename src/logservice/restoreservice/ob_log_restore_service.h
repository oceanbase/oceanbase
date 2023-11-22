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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_SERVICE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_SERVICE_H_

#include "lib/utility/ob_macro_utils.h"
#include "rpc/frame/ob_req_transport.h"                     // ObReqTransport
#include "share/ob_thread_pool.h"                           // ObThreadPool
#include "ob_remote_fetch_log.h"                            // ObRemoteFetchLogImpl
#include "ob_log_restore_rpc.h"                             // ObLogResSvrRpc
#include "ob_remote_fetch_log_worker.h"                     // ObRemoteFetchWorker
#include "ob_remote_location_adaptor.h"                     // ObRemoteLocationAdaptor
#include "ob_remote_error_reporter.h"                       // ObRemoteErrorReporter
#include "ob_log_restore_allocator.h"                       // ObLogRestoreAllocator
#include "ob_log_restore_scheduler.h"                       // ObLogRestoreScheduler
#include "ob_log_restore_net_driver.h"                      // ObLogRestoreNetDriver
#include "ob_log_restore_archive_driver.h"                  // ObLogRestoreArchiveDriver
#include "ob_remote_log_writer.h"                           // ObRemoteLogWriter

namespace oceanbase
{
namespace share
{
class ObLSID;
}

namespace storage
{
class ObLSService;
}

namespace logservice
{
class ObLogService;
using oceanbase::share::ObLSID;
using oceanbase::storage::ObLSService;
// Work in physical restore and physical standby,
// provide the ability to fetch log from remote cluster and backups
class ObLogRestoreService : public share::ObThreadPool
{
  const int64_t SCHEDULE_INTERVAL = 1000 * 1000L;   // 1s
  const int64_t UPDATE_RESTORE_UPPER_LIMIT_INTERVAL = 100 * 1000L;  // 100ms
  const int64_t PRIMARY_THREAD_RUN_INTERVAL = 1000 * 1000L;   // 1s
  const int64_t STANDBY_THREAD_RUN_INTERVAL = 100 * 1000L;  // 100ms
public:
  ObLogRestoreService();
  ~ObLogRestoreService();

public:
  ObLogResSvrRpc *get_log_restore_proxy() { return &proxy_; }

public:
  int init(rpc::frame::ObReqTransport *transport,
           ObLSService *ls_svr,
           ObLogService *log_service);
  void destroy();
  int start();
  void stop();
  void wait();
  void signal();
  ObLogRestoreAllocator *get_log_restore_allocator() { return &allocator_;}

private:
  void run1();
  void do_thread_task_();
  int update_upstream_(share::ObLogRestoreSourceItem &source, bool &source_exist);
  void schedule_fetch_log_(share::ObLogRestoreSourceItem &source);
  void schedule_resource_(const share::ObLogRestoreSourceType &source_type);
  void clean_resource_();
  void report_error_();
  void update_restore_upper_limit_();
  void set_compressor_type_();
  void refresh_error_context_();

private:
  bool inited_;
  ObLSService *ls_svr_;
  ObLogResSvrRpc proxy_;
  ObRemoteLocationAdaptor location_adaptor_;
  ObLogRestoreArchiveDriver archive_driver_;
  ObLogRestoreNetDriver net_driver_;
  ObRemoteFetchLogImpl fetch_log_impl_;
  ObRemoteFetchWorker fetch_log_worker_;
  ObRemoteLogWriter writer_;
  ObRemoteErrorReporter error_reporter_;
  ObLogRestoreAllocator allocator_;
  ObLogRestoreScheduler scheduler_;
  common::ObCond cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreService);
};
} // namespace logservice
} // namespace oceanbase
#endif
