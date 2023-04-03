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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_TASK_RUNNER_NOTIFIER_SERVICE_
#define OCEANBASE_SQL_EXECUTOR_OB_TASK_RUNNER_NOTIFIER_SERVICE_

#include "sql/executor/ob_task_runner_notifier.h"
#include "lib/net/ob_addr.h"
#include "sql/executor/ob_slice_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace sql
{
class ObTask;
class ObTaskInfo;
class ObTaskRunnerNotifierService
{
public:
  class ObKillTaskRunnerNotifier
  {
  public:
    ObKillTaskRunnerNotifier() : ret_(common::OB_ERR_UNEXPECTED) {}
    virtual ~ObKillTaskRunnerNotifier() {}
    void operator()(common::hash::HashMapPair<ObTaskID, ObTaskRunnerNotifier*> &entry);
    int get_ret() { return ret_; }
  private:
    int ret_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObKillTaskRunnerNotifier);
  };

  class Guard
  {
  public:
    Guard(const ObTaskID &task_id, ObTaskRunnerNotifier *notifier);
    ~Guard();
  private:
    const ObTaskID task_id_;
  };

  ObTaskRunnerNotifierService();
  virtual ~ObTaskRunnerNotifierService();

  static int build_instance();
  static ObTaskRunnerNotifierService *get_instance();
  static int register_notifier(const ObTaskID &key,
                               ObTaskRunnerNotifier *notifier);
  static int unregister_notifier(const ObTaskID &key);
  static int kill_task_runner(const ObTaskID &key, bool *is_running = NULL);

  void reset();
  int init();
private:
  static const int64_t NOTIFIER_MAP_BUCKET_SIZE = 1024;

  static ObTaskRunnerNotifierService *instance_;

  int set_notifier(const ObTaskID &key, ObTaskRunnerNotifier *notifier);
  int erase_notifier(const ObTaskID &key);
  template<class _callback> int atomic(const ObTaskID &key, _callback &callback);

  bool inited_;
  common::hash::ObHashMap<ObTaskID, ObTaskRunnerNotifier*> notifier_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskRunnerNotifierService);
};

}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_RUNNER_NOTIFIER_SERVICE_ */

