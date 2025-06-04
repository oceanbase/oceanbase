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
class ObCompactionLocalityCache;
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
      : need_diagnose_(false),
        ls_id_(),
        tablet_id_(),
        add_timestamp_(OB_INVALID_TIMESTAMP),
        start_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObTabletTableUpdateTask(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t add_timestamp)
      : need_diagnose_(false),
        ls_id_(ls_id),
        tablet_id_(tablet_id),
        add_timestamp_(add_timestamp),
        start_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObTabletTableUpdateTask(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id)
      : need_diagnose_(false),
        ls_id_(ls_id),
        tablet_id_(tablet_id),
        add_timestamp_(OB_INVALID_TIMESTAMP),
        start_timestamp_(OB_INVALID_TIMESTAMP) {}
  virtual ~ObTabletTableUpdateTask();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t add_timestamp,
      const bool need_diagnose = false);
  void reset();
  // operator-related functions for ObTabletTableUpdateTask
  bool is_valid() const;
  void check_task_status() const;
  int assign(const ObTabletTableUpdateTask &other);
  virtual bool operator==(const ObTabletTableUpdateTask &other) const;
  virtual void set_start_timestamp() override;
  virtual int64_t get_start_timestamp() const override;
  virtual bool need_diagnose() const override { return need_diagnose_; }

  // get-related functions for member in ObTabletTableUpdateTask
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }

  // other functions
  bool need_process_alone() const { return false; }
  uint64_t get_group_id() const { return ls_id_.id(); }
  int64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  bool compare_without_version(
      const ObTabletTableUpdateTask &other) const;
  // TODO: need to realize barrier related functions
  bool is_barrier() const;

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(need_diagnose), K_(add_timestamp), K_(start_timestamp));
private:
  const int64_t TABLET_CHECK_INTERVAL = 2 * 3600 * 1000L * 1000L; //2 hour
  bool need_diagnose_; // task for compaction need diagnose
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t add_timestamp_;
  int64_t start_timestamp_;
};

typedef ObUniqTaskQueue<ObTabletTableUpdateTask, ObTabletTableUpdater> ObTabletTableUpdateTaskQueue;
typedef ObUniqTaskQueue<ObTabletTableUpdateTask, ObTabletTableUpdater> ObTabletTableRemoveTaskQueue;
typedef ObArray<ObTabletTableUpdateTask> UpdateTaskList;
typedef ObArray<ObTabletTableUpdateTask> RemoveTaskList;

class ObTabletTableUpdater
{
public:
  ObTabletTableUpdater()
      : is_inited_(false),
        is_stop_(true),
        tenant_id_(OB_INVALID_TENANT_ID),
        update_queue_() {}
  virtual ~ObTabletTableUpdater() { destroy(); }
  static int mtl_init(ObTabletTableUpdater *&tablet_table_updater);
  int init();
  inline bool is_inited() const { return is_inited_; }
  void stop();
  void wait();
  void destroy();

  int submit_tablet_update_task(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool need_diagnose = false);

  // async update tablets - add task to queue
  // @param [in] ls_id, to report which ls
  // @param [in] tablet_id, to report which tablet
  int async_update(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool need_diagnose = false);

  // batch_process_tasks - divide tasks into different group, and do reput when failed
  // @parma [in] tasks, tasks to process
  // @param [in] stopped, whether this process is working or stopped
  int batch_process_tasks(
      const common::ObIArray<ObTabletTableUpdateTask> &tasks,
      bool &stopped);

  int64_t get_tablet_table_updater_update_queue_size() const { return update_queue_.task_count(); }
  // TODO: need to realize barrier related functions
  int process_barrier(const ObTabletTableUpdateTask &task, bool &stopped);
  int set_thread_count();
  // for diagnose
  int check_exist(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      bool &exist);
  int check_processing_exist(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      bool &exist);
  int diagnose_existing_task(
      ObIArray<ObTabletTableUpdateTask> &waiting_tasks,
      ObIArray<ObTabletTableUpdateTask> &processing_tasks);
private:
  int64_t cal_thread_count_();
  void diagnose_batch_tasks_(
      const ObIArray<ObTabletTableUpdateTask> &batch_tasks,
      const int error_code);

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

  int check_tenant_status_(
      const uint64_t tenant_id,
      bool &tenant_dropped,
      bool &tenant_not_ready);

  // prepare_locality_cache - init ls locality cache for share storage
  void prepare_locality_cache_(
      share::ObCompactionLocalityCache &locality_cache,
      bool &locality_is_valid);

  // check_remove_task_ - check whether the replica in inner table should be removed
  void check_remove_task_(
      const share::ObLSID &ls_id,
      const bool is_ls_not_exist,
      const bool locality_is_valid,
      share::ObCompactionLocalityCache &locality_cache,
      bool &is_remove_task);

  // push_task_info_ - add update / remove task to array
  int push_task_info_(
      const ObTabletTableUpdateTask &task,
      const share::ObTabletReplica &replica,
      ObArray<share::ObTabletReplica> &replicas,
      ObArray<ObTabletTableUpdateTask> &task_list);
private:
  const int64_t MINI_MODE_UPDATE_TASK_THREAD_CNT = 1;
  const int64_t MIN_UPDATE_TASK_THREAD_CNT = 2;
  const int64_t MAX_UPDATE_TASK_THREAD_CNT = 7;
  const double UPDATE_TASK_THREAD_RATIO = 0.2;
  const int64_t MINI_MODE_UPDATE_QUEUE_SIZE = 5 * 10000;
  const int64_t UPDATE_QUEUE_SIZE = 10 * 10000;
  const int64_t DIAGNOSE_MAX_BATCH_COUNT = 3;
  bool is_inited_;
  bool is_stop_;
  uint64_t tenant_id_;
  ObTabletTableUpdateTaskQueue update_queue_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableUpdater);
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_TABLET_TABLE_UPDATER_H_
