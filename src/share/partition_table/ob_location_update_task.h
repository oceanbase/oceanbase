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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_LOCATION_UPDATE_TASK_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_LOCATION_UPDATE_TASK_H_

#include "lib/queue/ob_dedup_queue.h"
#include "share/partition_table/ob_partition_location_cache.h"

namespace oceanbase {
namespace share {
class ObPartitionLocationCache;

struct TSILocationCacheStatistics {
public:
  TSILocationCacheStatistics()
      : suc_cnt_(0), fail_cnt_(0), sql_suc_cnt_(0), sql_fail_cnt_(0), total_wait_us_(0), total_exec_us_(0)
  {}
  void reset();
  void calc(ObLocationCacheQueueSet::Type type, bool sql_renew, int64_t succ_cnt, int64_t fail_cnt, int64_t wait_us,
      int64_t exec_us);
  void calc(ObLocationCacheQueueSet::Type type, int ret, bool sql_renew, int64_t wait_us, int64_t exec_us);
  void dump();

public:
  int64_t suc_cnt_;
  int64_t fail_cnt_;
  int64_t sql_suc_cnt_;
  int64_t sql_fail_cnt_;
  uint64_t total_wait_us_;
  uint64_t total_exec_us_;
  ObLocationCacheQueueSet::Type type_;
};
// partition location update task
class ObLocationUpdateTask : public common::IObDedupTask {
public:
  static const int64_t WAIT_PROCESS_WARN_TIME = 3 * 1000 * 1000;  // 3s
  ObLocationUpdateTask(ObPartitionLocationCache& loc_cache, const volatile bool& is_stopped, const uint64_t table_id,
      const int64_t partition_id, const int64_t add_timestamp, const int64_t cluster_id);
  ObLocationUpdateTask(ObPartitionLocationCache& loc_cache, const volatile bool& is_stopped, const uint64_t table_id,
      const int64_t partition_id, const int64_t add_timestamp, const int64_t cluster_id, const bool force_sql_renew);
  virtual ~ObLocationUpdateTask();

  bool is_valid() const;
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline int64_t get_partition_id() const
  {
    return partition_id_;
  }
  inline uint64_t get_tenant_id() const
  {
    return extract_tenant_id(table_id_);
  }
  inline int64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  virtual int64_t hash() const;
  virtual bool operator==(const common::IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  virtual common::IObDedupTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();
  bool need_discard() const;

  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(add_timestamp), K_(cluster_id), K_(force_sql_renew));

private:
  ObPartitionLocationCache& loc_cache_;
  const volatile bool& is_stopped_;

  uint64_t table_id_;
  int64_t partition_id_;
  int64_t add_timestamp_;
  int64_t cluster_id_;
  bool force_sql_renew_;

  DISALLOW_COPY_AND_ASSIGN(ObLocationUpdateTask);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
