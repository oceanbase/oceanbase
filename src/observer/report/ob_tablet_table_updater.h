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

#ifndef OCEANBASE_OBSERVER_OB_TABLET_TABLE_UPDATER_H_
#define OCEANBASE_OBSERVER_OB_TABLET_TABLE_UPDATER_H_

#include "observer/ob_uniq_task_queue.h"   // for ObIUniqTaskQueueTask
#include "common/ob_tablet_id.h"           // for ObTablet
#include "share/ob_ls_id.h"                // for ObLSID

namespace oceanbase
{
namespace common
{
class ObAddr;
class ObTabletID;
}
namespace share
{
class ObLSID;
class ObTabletReplica;
class ObTabletTableOperator;
struct ObTabletReplicaChecksumItem;
}
namespace observer
{
class ObService;
class ObTabletTableUpdater;
struct TSITabletTableUpdatStatistics
{
public:
  TSITabletTableUpdatStatistics() { reset(); }
  void reset();
  void calc(int64_t succ_cnt,
            int64_t fail_cnt,
            int64_t remove_task_cnt,
            int64_t update_task_cnt,
            int64_t wait_us,
            int64_t exec_us);
  void dump();
private:
  int64_t suc_cnt_;
  int64_t fail_cnt_;
  int64_t remove_task_cnt_;
  int64_t update_task_cnt_;
  int64_t total_wait_us_;
  int64_t total_exec_us_;
};
class ObTabletTableUpdateTask : public ObIUniqTaskQueueTask<ObTabletTableUpdateTask>
{
public:
  friend class ObTabletTableUpdater;

  ObTabletTableUpdateTask()
      : tenant_id_(OB_INVALID_TENANT_ID),
        ls_id_(),
        tablet_id_(),
        add_timestamp_(OB_INVALID_TIMESTAMP){}
  explicit ObTabletTableUpdateTask(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t add_timestamp)
      : tenant_id_(tenant_id),
        ls_id_(ls_id),
        tablet_id_(tablet_id),
        add_timestamp_(add_timestamp) {}
  virtual ~ObTabletTableUpdateTask();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t add_timestamp);
  void reset();
  // operator-related functions for ObTabletTableUpdateTask
  bool is_valid() const;
  void check_task_status() const;
  int assign(const ObTabletTableUpdateTask &other);
  virtual bool operator==(const ObTabletTableUpdateTask &other) const;

  // get-related functions for member in ObTabletTableUpdateTask
  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }

  // other functions
  bool need_process_alone() const { return false; }
  uint64_t get_group_id() const { return tenant_id_; }
  int64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  bool compare_without_version(
      const ObTabletTableUpdateTask &other) const;
  inline bool need_assign_when_equal() const { return false; }
  inline int assign_when_equal(const ObTabletTableUpdateTask &other)
  {
    UNUSED(other);
    return common::OB_NOT_SUPPORTED;
  }
  // TODO: need to realize barrier related functions
  bool is_barrier() const;

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(add_timestamp));
private:
  const int64_t TABLET_CHECK_INTERVAL = 2 * 3600 * 1000L * 1000L; //2 hour
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t add_timestamp_;
};

typedef ObUniqTaskQueue<ObTabletTableUpdateTask, ObTabletTableUpdater> ObTabletTableUpdateTaskQueue;
typedef ObUniqTaskQueue<ObTabletTableUpdateTask, ObTabletTableUpdater> ObTabletTableRemoveTaskQueue;
typedef ObArray<ObTabletTableUpdateTask> UpdateTaskList;
typedef ObArray<ObTabletTableUpdateTask> RemoveTaskList;

class ObTabletTableUpdater
{
public:
  ObTabletTableUpdater()
      : inited_(false),
        stopped_(true),
        ob_service_(nullptr),
        tablet_operator_(nullptr),
        update_queue_() {}
  virtual ~ObTabletTableUpdater() { destroy(); }
  int init(ObService &ob_service,
           share::ObTabletTableOperator &tablet_operator);
  inline bool is_inited() const { return inited_; }
  void stop();
  void wait();
  void destroy();

  // async update tablets - add task to queue
  // @param [in] tenant_id, to report which tenant's tablet
  // @param [in] ls_id, to report which ls
  // @param [in] tablet_id, to report which tablet
  int async_update(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);

  // batch_process_tasks - divide tasks into different group, and do reput when failed
  // @parma [in] tasks, tasks to process
  // @param [in] stopped, whether this process is working or stopped
  int batch_process_tasks(
      const common::ObIArray<ObTabletTableUpdateTask> &tasks,
      bool &stopped);

  int64_t get_tablet_table_updater_update_queue_size() const { return update_queue_.task_count(); }
  // TODO: need to realize barrier related functions
  int process_barrier(const ObTabletTableUpdateTask &task, bool &stopped);

private:
  // generate_tasks_ - split batch_tasks into update_tasks and remove_tasks
  // @parma [in] batch_tasks, input tasks
  // @parma [out] update_tablet_replicas, generated update replicas
  // @parma [out] remove_tablet_replicas, generated remove replicas
  // @parma [out] update_tablet_checksums, generated update tablet checksums
  // @parma [out] update_tablet_tasks, generated update tasks
  // @parma [out] remove_tablet_tasks, generated remove tasks
  int generate_tasks_(
      const ObIArray<ObTabletTableUpdateTask> &batch_tasks,
      ObArray<share::ObTabletReplica> &update_tablet_replicas,
      ObArray<share::ObTabletReplica> &remove_tablet_replicas,
      ObArray<share::ObTabletReplicaChecksumItem> &update_tablet_checksums,
      UpdateTaskList &update_tablet_tasks,
      RemoveTaskList &remove_tablet_tasks);

  // do_batch_update - the real action to update a batch of tasks
  // @parma [in] start_time, the time to start this execution
  // @parma [in] tasks, batch of tasks to execute
  // @parma [in] replicas, related replica to each task
  // @parma [in] checksums, related checksum to each task
  int do_batch_update_(
      const int64_t start_time,
      const ObIArray<ObTabletTableUpdateTask> &tasks,
      const ObIArray<share::ObTabletReplica> &replicas,
      const ObIArray<share::ObTabletReplicaChecksumItem> &checksums);

  // do_batch_remove - the real action to remove a batch of tasks
  // @parma [in] start_time, the time to start this execution
  // @parma [in] tasks, batch of tasks to execute
  // @parma [in] replicas, related replica to each task
  int do_batch_remove_(
      const int64_t start_time,
      const ObIArray<ObTabletTableUpdateTask> &tasks,
      const ObIArray<share::ObTabletReplica> &replicas);

  // add_update_task - add a task to task_queue
  // @parma [in] task, task to add
  int add_task_(const ObTabletTableUpdateTask &task);

  // throttle - wait a certain time before reput task to queue
  // @param [in] return_code, pre-procedure's running result
  // @parma [in] execute_time_us, execute time of pre-procedure
  int throttle_(
      const int return_code,
      const int64_t execute_time_us);

  // reput_to_update_queue - reput tasks to update_queue when failure occurs
  // @param [in] tasks, tasks to reput to queue
  int reput_to_queue_(
    const common::ObIArray<ObTabletTableUpdateTask> &tasks);

  // TODO: no need to check tenant dropped after ObTabletTableUpdater has been moved into MTL
  int check_tenant_status_(
      const uint64_t tenant_id,
      bool &tenant_dropped,
      bool &schema_not_ready);
private:
  const int64_t MINI_MODE_UPDATE_TASK_THREAD_CNT = 1;
  const int64_t UPDATE_TASK_THREAD_CNT = 7;
  const int64_t MINI_MODE_UPDATE_QUEUE_SIZE = 5 * 10000;
  const int64_t UPDATE_QUEUE_SIZE = 10 * 10000;
  bool inited_;
  bool stopped_;
  ObService *ob_service_;
  share::ObTabletTableOperator *tablet_operator_;
  ObTabletTableUpdateTaskQueue update_queue_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableUpdater);
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_TABLET_TABLE_UPDATER_H_
