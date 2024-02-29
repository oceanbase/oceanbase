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

#ifndef OCEANBASE_OBSERVER_OB_LS_TABLE_UPDATER
#define OCEANBASE_OBSERVER_OB_LS_TABLE_UPDATER

#include "observer/ob_uniq_task_queue.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace observer
{
class ObLSTableUpdateTask : public ObIUniqTaskQueueTask<ObLSTableUpdateTask>
{
public:
  ObLSTableUpdateTask()
      : tenant_id_(OB_INVALID_TENANT_ID),
        ls_id_(),
        inner_table_only_(false),
        add_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObLSTableUpdateTask(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const bool inner_table_only,
      const int64_t add_timestamp)
      : tenant_id_(tenant_id),
        ls_id_(ls_id),
        inner_table_only_(inner_table_only),
        add_timestamp_(add_timestamp) {}
  virtual ~ObLSTableUpdateTask() {}
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const bool inner_table_only,
      const int64_t add_timestamp);
  int assign(const ObLSTableUpdateTask &other);
  virtual void reset();
  virtual bool is_barrier() const { return false; }
  virtual bool need_process_alone() const { return true; }
  virtual bool is_valid() const;
  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  virtual bool operator==(const ObLSTableUpdateTask &other) const;
  virtual bool operator!=(const ObLSTableUpdateTask &other) const;
  virtual bool compare_without_version(const ObLSTableUpdateTask &other) const;
  virtual uint64_t get_group_id() const { return tenant_id_; }
  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline share::ObLSID get_ls_id() const { return ls_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }
  inline bool is_inner_table_only() const { return inner_table_only_; }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(add_timestamp));
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  // 1. For sys tenant, when inner_table_only_ = true, task will be add to meta_tenant_queue_. Otherwise, task will be add to sys_tenant_queue_.
  // 2. For other tenants, inner_table_only_ is useless.
  bool inner_table_only_;
  int64_t add_timestamp_;
};

class ObLSTableUpdater;
typedef ObUniqTaskQueue<ObLSTableUpdateTask,
    ObLSTableUpdater> ObLSTableUpdateTaskQueue;

class ObLSTableUpdateQueueSet
{
public:
  ObLSTableUpdateQueueSet(ObLSTableUpdater *updater);
  virtual ~ObLSTableUpdateQueueSet();
  int init();
  int add_task(const ObLSTableUpdateTask &task);
  int set_thread_count(const int64_t thread_cnt);
  void stop();
  void wait();
private:
  const int64_t MINI_MODE_UPDATE_THREAD_CNT = 1;
  const int64_t USER_TENANT_UPDATE_THREAD_CNT = 7;
  const int64_t LST_TASK_QUEUE_SIZE = 100;
  const int64_t USER_TENANT_QUEUE_SIZE = 10000;
  const int64_t MINI_MODE_USER_TENANT_QUEUE_SIZE = 1000;
  bool inited_;
  ObLSTableUpdater *updater_;
  ObLSTableUpdateTaskQueue sys_tenant_queue_;
  ObLSTableUpdateTaskQueue meta_tenant_queue_;
  ObLSTableUpdateTaskQueue user_tenant_queue_;
};

class ObLSTableUpdater
{
public:
  ObLSTableUpdater()
      : inited_(false), stopped_(true), update_queue_set_(this) {}
  virtual ~ObLSTableUpdater() {}
  virtual int async_update(const uint64_t tenant_id, const share::ObLSID &ls_id);
  int batch_process_tasks(
      const common::ObIArray<ObLSTableUpdateTask> &tasks,
      bool &stopped);
  int process_barrier(const ObLSTableUpdateTask &task, bool &stopped);
  int init();
  void stop();
  void wait();
  void throttle(
      const share::ObLSID &ls_id,
      const int return_code,
      const int64_t execute_time_us,
      bool &stopped);

 private:
  const static int64_t SYS_RETRY_INTERVAL_US = 10 * 1000;     // 10ms
  const static int64_t RETRY_INTERVAL_US = 10 * 1000;         // 10ms
  const static int64_t SLOW_UPDATE_TIME_US = 20* 1000 * 1000; // 20s
  int check_tenant_status_(
      const uint64_t tenant_id,
      bool &tenant_dropped,
      bool &schema_not_ready);
  bool inited_;
  bool stopped_;
  ObLSTableUpdateQueueSet update_queue_set_;
};

} // end namespace observer
} // end namespace oceanbase
#endif
