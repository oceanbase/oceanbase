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

#ifndef OCEANBASE_STORAGE_OB_DDL_HEART_BEAT_TASK_H
#define OCEANBASE_STORAGE_OB_DDL_HEART_BEAT_TASK_H

#include "observer/ob_server_struct.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace storage
{
class ObDDLHeartBeatTaskInfo final
{
public:
  TO_STRING_KV(K_(task_id),
               K_(tenant_id));
  ObDDLHeartBeatTaskInfo() : task_id_(0), tenant_id_(OB_INVALID_ID) {};
  ObDDLHeartBeatTaskInfo(int64_t task_id, uint64_t tenant_id) : task_id_(task_id), tenant_id_(tenant_id) {}
  ~ObDDLHeartBeatTaskInfo() = default;
  inline int64_t get_task_id() {return task_id_;}
  inline uint64_t get_tenant_id() {return tenant_id_;}
  inline void set_task_id(int64_t task_id) {task_id_ = task_id;}
  inline void set_tenant_id(uint64_t tenant_id) {tenant_id_ = tenant_id;}
private:
  int64_t task_id_;
  uint64_t tenant_id_;
};
class ObDDLHeartBeatTaskContainer final
{
public:
  static ObDDLHeartBeatTaskContainer &get_instance();
  ObDDLHeartBeatTaskContainer();
  ~ObDDLHeartBeatTaskContainer();
  int init();
  int set_register_task_id(const int64_t task_id, const uint64_t tenant_id);
  int remove_register_task_id(const int64_t task_id, const uint64_t tenant_id);
  int send_task_status_to_rs();
private:
  static const int64_t BUCKET_LOCK_BUCKET_CNT = 10243L;
  static const int64_t RETRY_COUNT = 3L;
  static const int64_t RETRY_TIME_INTERVAL = 100L;
  common::hash::ObHashMap<rootserver::ObDDLTaskID, uint64_t> register_tasks_;
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
};

class ObRedefTableHeartBeatTask : public common::ObTimerTask
{
public:
  ObRedefTableHeartBeatTask();
  virtual ~ObRedefTableHeartBeatTask() = default;
  int init(const int tg_id);
  virtual void runTimerTask() override;
private:
  int send_task_status_to_rs();
private:
  const static int64_t HEARTBEAT_INTERVAL = 30L * 1000L * 1000L;//30s
  bool is_inited_;
};

inline ObDDLHeartBeatTaskContainer &ObDDLHeartBeatTaskContainer::get_instance()
{
  static ObDDLHeartBeatTaskContainer THE_ONE;
  return THE_ONE;
}

}  // end of namespace observer
}  // end of namespace oceanbase

#define OB_DDL_HEART_BEAT_TASK_CONTAINER (::oceanbase::storage::ObDDLHeartBeatTaskContainer::get_instance())

#endif /*_OCEANBASE_STORAGE_OB_DDL_HEART_BEAT_TASK_H_ */