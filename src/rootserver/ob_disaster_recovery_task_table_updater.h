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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_TABLE_UPDATER
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_TABLE_UPDATER

#include "observer/ob_uniq_task_queue.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"               // ObMySQLProxy
#include "rootserver/ob_disaster_recovery_task.h"         // for ObDRTaskType

namespace oceanbase
{
namespace rootserver
{
class ObDRTaskTableUpdateTask : public observer::ObIUniqTaskQueueTask<ObDRTaskTableUpdateTask>
{
public:
  ObDRTaskTableUpdateTask()
      : tenant_id_(OB_INVALID_TENANT_ID),
        ls_id_(),
        task_type_(),
        task_id_(),
        task_key_(),
        ret_code_(OB_SUCCESS),
        need_clear_server_data_in_limit_(false),
        need_record_event_(true),
        ret_comment_(ObDRTaskRetComment::MAX),
        add_timestamp_(OB_INVALID_TIMESTAMP) {}
  explicit ObDRTaskTableUpdateTask(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObDRTaskType &task_type,
      const share::ObTaskId &task_id,
      const ObDRTaskKey &task_key,
      const int ret_code,
      const bool need_clear_server_data_in_limit,
      const bool need_record_event,
      const ObDRTaskRetComment &ret_comment,
      const int64_t add_timestamp)
      : tenant_id_(tenant_id),
        ls_id_(ls_id),
        task_type_(task_type),
        task_id_(task_id),
        task_key_(),
        ret_code_(ret_code),
        need_clear_server_data_in_limit_(need_clear_server_data_in_limit),
        need_record_event_(need_record_event),
        ret_comment_(ret_comment),
        add_timestamp_(add_timestamp) { task_key_ = task_key; }
  virtual ~ObDRTaskTableUpdateTask() {}
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObDRTaskType &task_type,
      const share::ObTaskId &task_id,
      const ObDRTaskKey &task_key,
      const int ret_code,
      const bool need_clear_server_data_in_limit,
      const bool need_record_event,
      const ObDRTaskRetComment &ret_comment,
      const int64_t add_timestamp);
  int assign(const ObDRTaskTableUpdateTask &other);
  virtual void reset();
  virtual bool is_valid() const;
  virtual int64_t hash() const { return task_id_.hash(); };
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  virtual bool operator==(const ObDRTaskTableUpdateTask &other) const;
  virtual bool operator!=(const ObDRTaskTableUpdateTask &other) const;
  virtual bool compare_without_version(const ObDRTaskTableUpdateTask &other) const;

  virtual uint64_t get_group_id() const { return tenant_id_; }
  inline int64_t get_tenant_id() const { return tenant_id_; }
  inline share::ObLSID get_ls_id() const { return ls_id_; }
  inline rootserver::ObDRTaskType get_task_type() const { return task_type_; }
  inline share::ObTaskId get_task_id() const { return task_id_; }
  inline int64_t get_add_timestamp() const { return add_timestamp_; }
  inline ObDRTaskKey get_task_key() const { return task_key_; }
  inline int get_ret_code() const { return ret_code_; }
  inline bool get_need_clear_server_data_in_limit() const { return need_clear_server_data_in_limit_; }
  inline bool get_need_record_event() const { return need_record_event_; }
  inline ObDRTaskRetComment get_ret_comment() const { return ret_comment_; }

  // unused functions
  virtual bool is_barrier() const { return false; }
  virtual bool need_process_alone() const { return true; }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(task_type), K_(task_id),
               K_(task_key), K_(ret_code), K_(need_clear_server_data_in_limit),
               K_(need_record_event), K_(ret_comment), K_(add_timestamp));
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  rootserver::ObDRTaskType task_type_;
  share::ObTaskId task_id_;
  ObDRTaskKey task_key_;
  int ret_code_;
  bool need_clear_server_data_in_limit_;
  bool need_record_event_;
  ObDRTaskRetComment ret_comment_;
  int64_t add_timestamp_;
};

class ObDRTaskTableUpdater;
typedef observer::ObUniqTaskQueue<ObDRTaskTableUpdateTask,
    ObDRTaskTableUpdater> ObDRTaskTableUpdateTaskQueue;

class ObDRTaskTableUpdater
{
public:
  ObDRTaskTableUpdater()
      : inited_(false),
        stopped_(true),
        update_queue_(),
        sql_proxy_(nullptr),
        task_mgr_(nullptr) {}
  virtual ~ObDRTaskTableUpdater() { destroy(); }
  virtual int async_update(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObDRTaskType &task_type,
      const ObDRTaskKey &task_key,
      const int ret_code,
      const bool need_clear_server_data_in_limit,
      const share::ObTaskId &task_id,
      const bool need_record_event,
      const ObDRTaskRetComment &ret_comment);
  int batch_process_tasks(
      const common::ObIArray<ObDRTaskTableUpdateTask> &tasks,
      bool &stopped);
  int process_barrier(const ObDRTaskTableUpdateTask &task, bool &stopped);
  inline bool is_inited() const { return inited_; }
  int init(common::ObMySQLProxy *sql_proxy, ObDRTaskMgr *task_mgr);
  int start();
  void stop();
  void wait();
  void destroy();

private:
  int check_inner_stat_();
  int process_task_(
      const ObDRTaskTableUpdateTask &task);

private:
  const int64_t MINI_MODE_UPDATE_THREAD_CNT = 1;
  const int64_t UPDATE_THREAD_CNT = 1;
  const int64_t TASK_QUEUE_SIZE = 10000;
  bool inited_;
  bool stopped_;
  ObDRTaskTableUpdateTaskQueue update_queue_;
  common::ObMySQLProxy *sql_proxy_;
  ObDRTaskMgr *task_mgr_;
};

} // end namespace observer
} // end namespace oceanbase
#endif
