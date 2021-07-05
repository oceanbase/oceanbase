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

#ifndef OCEANBASE_OBSERVER_OB_PARTITION_TABLE_UPDATER_H_
#define OCEANBASE_OBSERVER_OB_PARTITION_TABLE_UPDATER_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "ob_uniq_task_queue.h"
namespace oceanbase {
namespace common {
class ObAddr;
}
namespace share {
class ObPartitionReplica;
}
namespace observer {

// fill partition replica
class ObIPartitionReplicaFiller {
public:
  ObIPartitionReplicaFiller()
  {}
  ~ObIPartitionReplicaFiller()
  {}

  // return OB_PARTITION_NOT_EXIST for partition not found.
  // return OB_INVALID_PARTITION for partition not valid.
  virtual int fill_partition_replica(const common::ObPartitionKey& part_key, share::ObPartitionReplica& replica) = 0;

  virtual const common::ObAddr& get_self_addr() = 0;
};

class ObPartitionTableUpdater;

class ObPTUpdateRoleTask : public common::ObDLinkBase<ObPTUpdateRoleTask> {
public:
  friend class ObPartitionTableUpdater;

public:
  ObPTUpdateRoleTask() : pkey_(), data_version_(0), first_submit_time_(0)
  {}
  virtual ~ObPTUpdateRoleTask();

  int set_update_partition_role_task(
      const common::ObPartitionKey& pkey, const int64_t data_version, const int64_t first_submit_time);
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }

  bool is_valid() const;
  // this func is used to check if this task can be executed using batch mode
  bool need_process_alone() const;
  void check_task_status() const;

  virtual int64_t hash() const;
  virtual bool operator==(const ObPTUpdateRoleTask& other) const;
  bool operator<(const ObPTUpdateRoleTask& other) const;
  virtual bool compare_without_version(const ObPTUpdateRoleTask& other) const;
  uint64_t get_group_id() const
  {
    return pkey_.get_tenant_id();
  }
  bool is_barrier() const
  {
    return false;
  }
  TO_STRING_KV(K_(pkey), K_(data_version), K_(first_submit_time));

private:
  common::ObPartitionKey pkey_;
  int64_t data_version_;
  // no task with the same pkey shall exist in the task queue,
  // this first_submit_time_ private member is used to
  // record the timestamp the first time this task is pushed back to the task queue
  int64_t first_submit_time_;
};

// partition table update task
class ObPTUpdateTask : public common::ObDLinkBase<ObPTUpdateTask> {
public:
  friend class ObPartitionTableUpdater;
  friend class TestBatchProcessQueue_test_task_Test;
  friend class TestBatchProcessQueue_test_single_update_Test;
  const static common::ObPartitionKey report_merge_finish_pkey_;
  const static common::ObPartitionKey sync_pt_key_;

  ObPTUpdateTask() : part_key_(), data_version_(0), first_submit_time_(0), is_remove_(false), with_role_(false)
  {}

  virtual ~ObPTUpdateTask();

  int set_update_task(const common::ObPartitionKey& part_key, const int64_t data_version,
      const int64_t first_submit_time, const bool with_role);
  int set_remove_task(const common::ObPartitionKey& part_key, const int64_t first_submit_time);

  bool is_valid() const;
  // this func is used to check if this task can be executed using batch mode
  bool need_process_alone() const;
  void check_task_status() const;

  virtual int64_t hash() const;
  virtual bool operator==(const ObPTUpdateTask& other) const;
  bool operator<(const ObPTUpdateTask& other) const;
  virtual bool compare_without_version(const ObPTUpdateTask& other) const;
  uint64_t get_group_id() const
  {
    return part_key_.get_tenant_id();
  }
  bool is_barrier() const;
  static bool is_barrier(const common::ObPartitionKey& pkey);

  TO_STRING_KV(K_(part_key), K_(data_version), K_(first_submit_time), K_(is_remove), K_(with_role));

private:
  const static int64_t rpc_task_group_id_;

  common::ObPartitionKey part_key_;
  int64_t data_version_;
  // no task with the same pkey shall exist in the task queue,
  // this first_submit_time_ private member is used to
  // record the timestamp the first time this task is pushed back to the task queue
  int64_t first_submit_time_;
  bool is_remove_;
  bool with_role_;
};

typedef ObUniqTaskQueue<ObPTUpdateTask, ObPartitionTableUpdater> ObPTUpdateTaskQueue;
typedef ObUniqTaskQueue<ObPTUpdateRoleTask, ObPartitionTableUpdater> ObPTUpdateRoleTaskQueue;

class ObPartitionTableUpdater {
public:
  friend class TestBatchProcessQueue_test_single_update_Test;
  friend class TestBatchProcessQueue_test_update_process_Test;
  friend class TestBatchProcessQueue_test_reput_Test;
  friend class TestBatchProcessQueue_test_reput2_Test;
  const static int64_t UPDATE_USER_TABLE_CNT = 7;
  const static int64_t MINI_MODE_UPDATE_USER_TABLE_CNT = 1;
  const static int64_t UPDATER_TENANT_SPACE_THREAD_CNT = 1;
  const static int64_t UPDATER_THREAD_CNT = 1;
  const static int64_t MAX_PARTITION_CNT = 1000000;
  const static int64_t MINI_MODE_MAX_PARTITION_CNT = 200000;
  const static int64_t MAX_PARTITION_REPORT_CONCURRENCY = 8;
  const static int64_t SYS_RETRY_INTERVAL_US = 200 * 1000;      // 200ms
  const static int64_t RETRY_INTERVAL_US = 1L * 1000 * 1000;    // 1 seconds.
  const static int64_t SLOW_UPDATE_TIME_US = 20 * 1000 * 1000;  // 20s

  ObPartitionTableUpdater()
      : inited_(false),
        stopped_(false),
        core_table_queue_(),
        sys_table_queue_(),
        tenant_space_queue_(),
        user_table_queue_()
  {}
  virtual ~ObPartitionTableUpdater()
  {
    destroy();
  }

  int init();

  virtual int batch_process_tasks(const common::ObIArray<ObPTUpdateTask>& tasks, bool& stopped);
  virtual int async_update(const common::ObPartitionKey& key, const bool with_role);
  virtual int sync_update(const common::ObPartitionKey& key);

  int process_barrier(const ObPTUpdateTask& task, bool& stopped);
  virtual int async_remove(const common::ObPartitionKey& key);

  virtual int sync_pt(const int64_t version);
  virtual int sync_merge_finish(const int64_t version);

  virtual int async_update_partition_role(const common::ObPartitionKey& pkey);
  virtual int process_barrier(const ObPTUpdateRoleTask& task, bool& stopped);
  virtual int batch_process_tasks(const common::ObIArray<ObPTUpdateRoleTask>& tasks, bool& stopped);

  void stop();
  void wait();
  void destroy();
  int64_t get_partition_table_updater_user_queue_size() const
  {
    return user_table_queue_.task_count();
  }
  int64_t get_partition_table_updater_tenant_space_queue_size() const
  {
    return tenant_space_queue_.task_count();
  }
  int64_t get_partition_table_updater_sys_queue_size() const
  {
    return sys_table_queue_.task_count();
  }
  int64_t get_partition_table_updater_core_queue_size() const
  {
    return core_table_queue_.task_count();
  }
  static int throttle(const bool is_sys_table, const int return_code, const int64_t execute_time_us, bool& stopped);

private:
  int get_pg_key(const common::ObPartitionKey& key, common::ObPGKey& pg_key);
  int get_queue(const common::ObPartitionKey& key, ObPTUpdateTaskQueue*& queue);
  int direct_execute(ObPTUpdateTask& task);
  virtual int add_task(const common::ObPartitionKey& key, const int64_t data_version, const int64_t first_submit_time,
      const bool with_role);
  virtual int add_update_partition_role_task(
      const common::ObPartitionKey& pkey, const int64_t data_version, const int64_t first_submit_time);
  virtual int do_report_merge_finish(const int64_t data_version);
  virtual int do_sync_pt_finish(const int64_t version);
  int do_rpc_task(const ObPTUpdateTask& task);
  int reput_to_queue(const common::ObIArray<ObPTUpdateTask>& tasks, const int64_t success_idx = common::OB_INVALID_ID);
  int reput_to_queue(const common::ObIArray<ObPTUpdateRoleTask>& tasks);

  int check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool& has_dropped);
  int do_batch_execute(const common::ObIArray<ObPTUpdateTask>& tasks,
      const common::ObIArray<share::ObPartitionReplica>& replicas, const bool with_role);
  int do_batch_execute(const common::ObIArray<share::ObPartitionReplica>& tasks, const common::ObRole new_role);

private:
  bool inited_;
  bool stopped_;
  ObPTUpdateTaskQueue core_table_queue_;
  ObPTUpdateTaskQueue sys_table_queue_;
  ObPTUpdateTaskQueue tenant_space_queue_;
  ObPTUpdateTaskQueue user_table_queue_;
  // this update_role_queue_ is used only
  // by user partition(with respect to the sys tenant and sys tables of user tenants)
  ObPTUpdateRoleTaskQueue update_role_queue_;
};
}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_PARTITION_TABLE_UPDATER_H_
