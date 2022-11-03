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

#ifndef OCEANBASE_OBSERVER_OB_TABLET_CHECKSUM_UPDATER_H_
#define OCEANBASE_OBSERVER_OB_TABLET_CHECKSUM_UPDATER_H_

#include "observer/ob_uniq_task_queue.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

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
struct ObTabletReplicaChecksumItem;
}

namespace observer
{
class ObService;
class ObTabletChecksumUpdater;
class ObTabletChecksumUpdateTask : public ObIUniqTaskQueueTask<ObTabletChecksumUpdateTask>
{
public:
  friend class ObTabletChecksumUpdater;

  ObTabletChecksumUpdateTask();
  ObTabletChecksumUpdateTask(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t add_timestamp);
  virtual ~ObTabletChecksumUpdateTask();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t add_timestamp);
  void reset();
  bool is_valid() const;
  int64_t hash() const;
  void check_task_status() const;
  int assign(const ObTabletChecksumUpdateTask &other);
  virtual bool operator==(const ObTabletChecksumUpdateTask &other) const;
  virtual bool operator!=(const ObTabletChecksumUpdateTask &other) const;
  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }
  bool compare_without_version(const ObTabletChecksumUpdateTask& other) const;
  uint64_t get_group_id() const { return tenant_id_; }
  bool need_process_alone() const { return false; }
  virtual bool need_assign_when_equal() const { return false; }
  inline int assign_when_equal(const ObTabletChecksumUpdateTask &other)
  {
    UNUSED(other);
    return common::OB_NOT_SUPPORTED;
  }
  bool is_barrier() const { return false; }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(add_timestamp));
private:
  const int64_t TABLET_CHECK_INTERVAL = 120l * 1000 * 1000; //2 minutes
  int64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t add_timestamp_;
};


// TODO impl Checker to clean the abandoned task
typedef ObUniqTaskQueue<ObTabletChecksumUpdateTask, ObTabletChecksumUpdater> ObTabletChecksumTaskQueue;
typedef ObArray<ObTabletChecksumUpdateTask> UpdateTabletChecksumTaskList;
typedef ObArray<ObTabletChecksumUpdateTask> RemoveTabletChecksumTaskList;

// TODO impl MTL Module
class ObTabletChecksumUpdater
{
public:
  ObTabletChecksumUpdater();
  virtual ~ObTabletChecksumUpdater();
  int init(ObService &ob_service);
  inline bool is_inited() const { return inited_; }
  void stop();
  void wait();
  void destroy();
  int async_update(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int batch_process_tasks(
      const common::ObIArray<ObTabletChecksumUpdateTask> &tasks,
      bool &stopped);
  int process_barrier(const ObTabletChecksumUpdateTask &task, bool &stopped);
private:
  int add_task_(const ObTabletChecksumUpdateTask &task);
  int reput_to_queue_(const ObIArray<ObTabletChecksumUpdateTask> &tasks);
  int generate_tasks_(
      const ObIArray<ObTabletChecksumUpdateTask> &tasks,
      ObIArray<share::ObTabletReplicaChecksumItem> &update_tablet_items,
      UpdateTabletChecksumTaskList &update_tasks,
      RemoveTabletChecksumTaskList &remove_tasks);
  int do_batch_update_(
    const int64_t start_time,
    const ObIArray<ObTabletChecksumUpdateTask> &tasks,
    const ObIArray<share::ObTabletReplicaChecksumItem> &items);
  int do_batch_remove_(
    const int64_t start_time,
    const ObIArray<ObTabletChecksumUpdateTask> &tasks);
  
  int throttle_(const int return_code, const int64_t execute_time_us);
private:
  const int64_t MINI_MODE_UPDATE_TASK_THREAD_CNT = 1;
  const int64_t UPDATE_TASK_THREAD_CNT = 7;
  const int64_t MINI_MODE_TASK_QUEUE_SIZE = 20 * 10000;
  const int64_t TASK_QUEUE_SIZE = 100 * 10000;
  bool inited_;
  bool stopped_;
  ObService *ob_service_;
  ObTabletChecksumTaskQueue task_queue_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletChecksumUpdater);
};


} // observer
} // oceanbase
#endif // OCEANBASE_OBSERVER_OB_TABLET_CHECKSUM_UPDATER_H_