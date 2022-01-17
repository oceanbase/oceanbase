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

#ifndef OCEANBASE_CLOG_OB_LOG_EVENT_TASK_V2_H_
#define OCEANBASE_CLOG_OB_LOG_EVENT_TASK_V2_H_

#include "storage/transaction/ob_time_wheel.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObLogEventScheduler;
class ObLogStateEventTaskV2 : public common::ObTimeWheelTask {
public:
  ObLogStateEventTaskV2();
  virtual ~ObLogStateEventTaskV2();

public:
  int init(const common::ObPartitionKey& partition_key, ObLogEventScheduler* event_scheduler,
      storage::ObPartitionService* partition_service);
  void destroy();
  uint64_t hash() const;
  void runTimerTask();
  bool is_inited()
  {
    return is_inited_;
  }
  int set_expected_ts(const int64_t delay);

private:
  static const int64_t MAX_EVENT_EXECUTE_DEPLAY_TS = 100 * 1000;

  bool is_inited_;
  common::ObPartitionKey partition_key_;
  ObLogEventScheduler* event_scheduler_;
  storage::ObPartitionService* partition_service_;
  int64_t expected_ts_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStateEventTaskV2);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_EVENT_TASK_V2_H_
