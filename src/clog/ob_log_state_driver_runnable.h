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

#ifndef OCEANBASE_CLOG_OB_LOG_STATE_DRIVER_RUNNABLE_
#define OCEANBASE_CLOG_OB_LOG_STATE_DRIVER_RUNNABLE_

#include "share/ob_thread_pool.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace election {
class ObIElectionMgr;
}

namespace clog {
class ObLogStateDriverRunnable : public share::ObThreadPool {
public:
  ObLogStateDriverRunnable();
  virtual ~ObLogStateDriverRunnable();

public:
  int init(storage::ObPartitionService* partition_service, election::ObIElectionMgr* election_mgr);
  void destroy();
  void run1();

private:
  void state_driver_loop();
  void check_can_start_service_();

private:
  storage::ObPartitionService* partition_service_;
  election::ObIElectionMgr* election_mgr_;
  bool already_disk_error_;
  bool is_inited_;
  bool can_start_service_;
  int64_t last_check_time_for_keepalive_;
  int64_t last_check_time_for_replica_state_;
  int64_t last_check_time_for_broadcast_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStateDriverRunnable);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_STATE_DRIVER_RUNNABLE_H_
