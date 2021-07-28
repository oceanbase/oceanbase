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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_TASK_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_TASK_H_

#include "lib/ob_define.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase {
namespace share {
struct TSILocationRateLimit {
public:
  const int64_t ONE_SECOND_US = 1 * 1000 * 1000L;  // 1s
public:
  TSILocationRateLimit() : cnt_(0), start_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~TSILocationRateLimit()
  {}
  void reset();
  int64_t calc_wait_ts(const int64_t cnt, const int64_t exec_ts, const int64_t frequency);
  TO_STRING_KV(K_(cnt), K_(start_ts));

public:
  int64_t cnt_;
  int64_t start_ts_;
};

struct TSILocationStatistics {
public:
  TSILocationStatistics() : suc_cnt_(0), fail_cnt_(0), total_exec_us_(0), total_wait_us_(0)
  {}
  ~TSILocationStatistics()
  {}
  void reset();
  void calc(const int ret, const int64_t exec_us, const int64_t wait_us, const int64_t cnt);
  int64_t get_total_cnt() const;
  TO_STRING_KV(K_(suc_cnt), K_(fail_cnt), K_(total_exec_us), K_(total_wait_us));

public:
  int64_t suc_cnt_;
  int64_t fail_cnt_;
  uint64_t total_exec_us_;
  uint64_t total_wait_us_;
};

// For Sender of ObPartitionLocationUpdater
class ObPartitionBroadcastTask : public common::ObDLinkBase<ObPartitionBroadcastTask> {
public:
  OB_UNIS_VERSION(1);
  friend class ObPartitionLocationUpdater;

public:
  ObPartitionBroadcastTask()
      : table_id_(common::OB_INVALID_ID),
        partition_id_(common::OB_INVALID_ID),
        partition_cnt_(common::OB_INVALID_ID),
        timestamp_(common::OB_INVALID_TIMESTAMP)
  {}
  explicit ObPartitionBroadcastTask(
      const int64_t table_id, const int64_t partition_id, const int64_t partition_cnt, const int64_t timestamp)
      : table_id_(table_id), partition_id_(partition_id), partition_cnt_(partition_cnt), timestamp_(timestamp)
  {}
  virtual ~ObPartitionBroadcastTask()
  {}

  int init(const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt, const int64_t timestamp);
  int assign(const ObPartitionBroadcastTask& other);
  void reset();
  bool is_valid() const;

  virtual int64_t hash() const;
  virtual bool operator==(const ObPartitionBroadcastTask& other) const;

  uint64_t get_group_id() const;
  bool is_barrier() const;
  bool need_process_alone() const;
  virtual bool compare_without_version(const ObPartitionBroadcastTask& other) const;
  bool need_assign_when_equal() const;
  int assign_when_equal(const ObPartitionBroadcastTask& other);

  uint64_t get_table_id() const
  {
    return table_id_;
  }
  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  int64_t get_partition_cnt() const
  {
    return partition_cnt_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }

  TO_STRING_KV(K_(table_id), K_(partition_id), K_(partition_cnt), K_(timestamp));

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;  // won't serialize/deserialize
  int64_t timestamp_;
};

// For Receiver of ObPartitionLocationUpdater
class ObPartitionUpdateTask : public common::ObDLinkBase<ObPartitionUpdateTask> {
public:
  friend class ObPartitionLocationUpdater;

public:
  ObPartitionUpdateTask()
      : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID), timestamp_(common::OB_INVALID_TIMESTAMP)
  {}
  explicit ObPartitionUpdateTask(const int64_t table_id, const int64_t partition_id, const int64_t timestamp)
      : table_id_(table_id), partition_id_(partition_id), timestamp_(timestamp)
  {}
  virtual ~ObPartitionUpdateTask()
  {}

  int init(const uint64_t table_id, const int64_t partition_id, const int64_t timestamp);
  void reset();
  bool is_valid() const;
  int assign(const ObPartitionUpdateTask& other);

  virtual int64_t hash() const;
  virtual bool operator==(const ObPartitionUpdateTask& other) const;

  uint64_t get_group_id() const;
  bool is_barrier() const;
  bool need_process_alone() const;
  virtual bool compare_without_version(const ObPartitionUpdateTask& other) const;
  bool need_assign_when_equal() const;
  int assign_when_equal(const ObPartitionUpdateTask& other);

  uint64_t get_table_id() const
  {
    return table_id_;
  }
  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  int64_t get_timestamp() const
  {
    return timestamp_;
  }

  TO_STRING_KV(K_(table_id), K_(partition_id), K_(timestamp));

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t timestamp_;
};

}  // namespace share
}  // namespace oceanbase

#endif
