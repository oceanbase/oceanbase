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

#ifndef OCEANBASE_OBSERVER_OB_TENANT_PARTITION_META_TABLE_UPDATER_H_
#define OCEANBASE_OBSERVER_OB_TENANT_PARTITION_META_TABLE_UPDATER_H_

#include "lib/container/ob_se_array.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_dedup_queue.h"
#include "common/ob_partition_key.h"
#include "ob_uniq_task_queue.h"

namespace oceanbase {
namespace observer {

class ObPGPartitionMTUpdater;

enum ObPGPartitionMTUpdateType { INSERT_ON_UPDATE, DELETE };

class ObPGPartitionMTUpdateTask : public common::ObDLinkBase<ObPGPartitionMTUpdateTask> {
public:
  friend class ObPGPartitionMTUpdater;
  const static common::ObPartitionKey sync_pt_key_;
  ObPGPartitionMTUpdateTask();
  virtual ~ObPGPartitionMTUpdateTask() = default;
  int init(const common::ObPartitionKey& pkey, const ObPGPartitionMTUpdateType type, const int64_t version,
      const common::ObAddr& addr);
  virtual int64_t hash() const;
  virtual bool operator==(const ObPGPartitionMTUpdateTask& other) const;
  virtual bool operator!=(const ObPGPartitionMTUpdateTask& other) const;
  bool is_valid() const;
  virtual bool compare_without_version(const ObPGPartitionMTUpdateTask& other) const;
  uint64_t get_group_id() const
  {
    return pkey_.get_tenant_id();
  }
  int64_t get_version() const
  {
    return version_;
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
  TO_STRING_KV(K_(pkey), K_(add_timestamp), K_(update_type), K_(version));

private:
  common::ObPartitionKey pkey_;
  int64_t add_timestamp_;
  ObPGPartitionMTUpdateType update_type_;
  int64_t version_;
  common::ObAddr svr_;
};

typedef ObUniqTaskQueue<ObPGPartitionMTUpdateTask, ObPGPartitionMTUpdater> PartitionMetaTableTaskQueue;

class ObPGPartitionMTUpdater {
public:
  ObPGPartitionMTUpdater()
  {
    reset();
  }
  virtual ~ObPGPartitionMTUpdater()
  {
    destroy();
  }
  int init();
  void reset();
  void stop();
  void wait();
  void destroy();
  int add_task(const common::ObPartitionKey& pkey, const ObPGPartitionMTUpdateType type, const int64_t version,
      const common::ObAddr& addr);
  virtual int batch_process_tasks(const common::ObIArray<ObPGPartitionMTUpdateTask>& tasks, bool& stopped);
  int sync_pg_pt(const int64_t version);
  virtual int process_barrier(const ObPGPartitionMTUpdateTask& task, bool& stopped);

public:
  static ObPGPartitionMTUpdater& get_instance();

private:
  void add_tasks_to_queue_(const ObPGPartitionMTUpdateTask& task);
  int do_sync_pt_finish_(const int64_t version);

private:
  static const int64_t UPDATER_THREAD_CNT = 8;
  static const int64_t MINI_MODE_UPDATER_THREAD_CNT = 1;

private:
  bool is_inited_;
  bool is_running_;
  PartitionMetaTableTaskQueue task_queue_;
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_TENANT_PARTITION_META_TABLE_UPDATER_H_
