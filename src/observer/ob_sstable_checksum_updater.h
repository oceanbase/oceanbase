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

#ifndef OCEANBASE_OBSERVER_OB_SSTABLE_CHECKSUM_UPDATER_H_
#define OCEANBASE_OBSERVER_OB_SSTABLE_CHECKSUM_UPDATER_H_

#include "lib/container/ob_se_array.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/thread/thread_mgr_interface.h"
#include "common/ob_partition_key.h"
#include "ob_uniq_task_queue.h"

namespace oceanbase {
namespace observer {

class ObSSTableChecksumUpdater;

enum ObSSTableChecksumUpdateType {
  UPDATE_COLUMN_CHECKSUM,
  UPDATE_DATA_CHECKSUM,
  UPDATE_ALL,
};

inline bool need_update_column_checksum(const ObSSTableChecksumUpdateType update_type)
{
  return ObSSTableChecksumUpdateType::UPDATE_ALL == update_type ||
         ObSSTableChecksumUpdateType::UPDATE_COLUMN_CHECKSUM == update_type;
}

inline bool need_update_data_checksum(const ObSSTableChecksumUpdateType update_type)
{
  return ObSSTableChecksumUpdateType::UPDATE_ALL == update_type ||
         ObSSTableChecksumUpdateType::UPDATE_DATA_CHECKSUM == update_type;
}

inline bool is_valid_checksum_update_type(const ObSSTableChecksumUpdateType update_type)
{
  return UPDATE_COLUMN_CHECKSUM <= update_type && UPDATE_ALL >= update_type;
}

class ObSSTableChecksumUpdateTask : public common::ObDLinkBase<ObSSTableChecksumUpdateTask> {
public:
  friend class ObSSTableChecksumUpdater;
  ObSSTableChecksumUpdateTask();
  virtual ~ObSSTableChecksumUpdateTask() = default;
  int set_info(const common::ObPartitionKey& pkey, const uint64_t sstable_id, const int sstable_type,
      const bool is_remove, const int64_t timestamp, const ObSSTableChecksumUpdateType update_type,
      const bool need_batch);
  virtual int64_t hash() const;
  virtual bool operator==(const ObSSTableChecksumUpdateTask& other) const;
  virtual bool operator!=(const ObSSTableChecksumUpdateTask& other) const;
  bool is_valid() const;
  virtual bool compare_without_version(const ObSSTableChecksumUpdateTask& other) const;
  uint64_t get_group_id() const
  {
    return pkey_.get_tenant_id();
  }
  bool is_barrier() const;
  bool need_process_alone() const
  {
    return false;
  }
  int64_t get_add_timestamp() const
  {
    return add_timestamp_;
  }
  TO_STRING_KV(K_(pkey), K_(sstable_id), K_(sstable_type), K_(is_remove));

private:
  common::ObPartitionKey pkey_;
  uint64_t sstable_id_;  // if OB_INVALID_ID == sstable_, we will report information of whole partition
  int sstable_type_;
  bool is_remove_;
  bool need_batch_;
  int64_t add_timestamp_;
  ObSSTableChecksumUpdateType update_type_;
};

typedef ObUniqTaskQueue<ObSSTableChecksumUpdateTask, ObSSTableChecksumUpdater> ObSSTableChecksumTaskQueue;

class ObSSTableChecksumUpdater : public lib::TGRunnable {
public:
  const static int64_t UPDATER_THREAD_CNT = 8;
  const static int64_t MINI_MODE_UPDATER_THREAD_CNT = 1;

public:
  ObSSTableChecksumUpdater();
  virtual ~ObSSTableChecksumUpdater()
  {
    destroy();
  }
  int init();
  virtual int batch_process_tasks(const common::ObIArray<ObSSTableChecksumUpdateTask>& tasks, bool& stopped);
  virtual int process_barrier(const ObSSTableChecksumUpdateTask& task, bool& stopped);
  int add_task(const common::ObPartitionKey& pkey, const bool is_remove = false,
      const ObSSTableChecksumUpdateType update_type = ObSSTableChecksumUpdateType::UPDATE_ALL,
      const uint64_t sstable_id = common::OB_INVALID_ID, const int sstable_type = -1,
      const bool task_need_batch = true);
  void stop();
  void wait();
  void destroy();
  void run1() override;

private:
  int reput_to_queue(const common::ObIArray<ObSSTableChecksumUpdateTask>& tasks);
  void try_submit_task(int64_t& time_to_wait);
  void add_tasks_to_queue();

private:
  const static int64_t EXPECT_BATCH_SIZE = 30;
  const static int64_t MAX_WAIT_TIME_US = 3000 * 1000L;  // 3s
  const static int64_t COND_WAIT_US = 200 * 1000L;       // 200ms
  const static int64_t MAX_PARTITION_CNT = 1000000;
  const static int64_t MINI_MODE_MAX_PARTITION_CNT = 100000;
  bool is_inited_;
  bool stopped_;
  ObSSTableChecksumTaskQueue task_queue_;
  common::ObThreadCond cond_;
  common::ObSEArray<ObSSTableChecksumUpdateTask, EXPECT_BATCH_SIZE> buffered_tasks_;
  int tg_id_;
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_SSTABLE_CHECKSUM_UPDATER_H_
