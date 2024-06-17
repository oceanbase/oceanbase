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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_SCHEDULE_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_SCHEDULE_TASK_H_

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_refered_map.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "ob_backup_base_job.h"

namespace oceanbase
{

namespace rootserver
{
class ObBackupTaskScheduler;
class ObBackupTaskQueue;

class ObBackupServerStatKey final
{
public: 
  ObBackupServerStatKey()
    : type_(BackupJobType::BACKUP_JOB_MAX),
      addr_(),
      hash_value_() {}
  ~ObBackupServerStatKey() {}
public:
  bool is_valid() const;
  bool operator==(const ObBackupServerStatKey &that) const;
  bool operator!=(const ObBackupServerStatKey &that) const { return !(*this == that); }
  ObBackupServerStatKey &operator=(const ObBackupServerStatKey &that);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  int init(const common::ObAddr &addr, const BackupJobType type);
  int init(const ObBackupServerStatKey &that);
  TO_STRING_KV(K_(type), K_(addr), K_(hash_value));
private:
  uint64_t inner_hash() const;
private:
  BackupJobType  type_;
  common::ObAddr addr_;
  uint64_t hash_value_;
};

struct ObServerBackupScheduleTaskStat 
{
  ObServerBackupScheduleTaskStat()
    : key_(),
      in_schedule_task_cnt_(0)
  {
  }

  const ObBackupServerStatKey &get_key() const
  {
    return key_;
  }
  void set_key(const ObBackupServerStatKey &key)
  {
    key_ = key;
  }

  bool is_valid() const  // %addr_ can be invalid
  {
    return in_schedule_task_cnt_ >= 0 && in_schedule_task_cnt_ >= 0;
  }

  TO_STRING_KV(K_(key), K_(in_schedule_task_cnt));

  ObBackupServerStatKey key_;
  // backup and validate task count executed on observer parallelly
  int64_t in_schedule_task_cnt_;
};

struct ObTenantBackupScheduleTaskStat {
  ObTenantBackupScheduleTaskStat()
    : tenant_id_(common::OB_INVALID_ID),
      task_cnt_(0)
  {
  }

  const uint64_t &get_key() const
  {
    return tenant_id_;
  }
  void set_key(const uint64_t key)
  {
    tenant_id_ = key;
  }

  bool is_valid()
  {
    return common::OB_INVALID_ID != tenant_id_ && task_cnt_ >= 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(task_cnt));

  uint64_t tenant_id_;
  int64_t task_cnt_;
};

typedef common::hash::ObReferedMap<ObBackupServerStatKey, ObServerBackupScheduleTaskStat> ObServerBackupScheduleTaskStatMap;
typedef common::hash::ObReferedMap<uint64_t, ObTenantBackupScheduleTaskStat> ObTenantBackupScheduleTaskStatMap;

// used to be the key of the map to indicate one specific task，the ls_id_,key_2_ ……
// key_4_ can be choosed by any as you want
class ObBackupScheduleTaskKey final
{
public:
  friend class ObBackupScheduleTask;
public:
  ObBackupScheduleTaskKey()
      : tenant_id_(-1),
        job_id_(-1),
        task_id_(-1),
        ls_id_(-1),
        type_(BackupJobType::BACKUP_JOB_MAX),
        hash_value_(0)
  {
  }
  virtual ~ObBackupScheduleTaskKey()
  {
  }

public:
  bool is_valid() const;
  bool operator==(const ObBackupScheduleTaskKey &that) const;
  bool operator!=(const ObBackupScheduleTaskKey &that) const { return !(*this == that); }
  ObBackupScheduleTaskKey &operator=(const ObBackupScheduleTaskKey &that);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  int init(const uint64_t tenant_id,
           const uint64_t job_id,
           const uint64_t task_id_,
           const uint64_t ls_id,
           const BackupJobType type);
  int init(const ObBackupScheduleTaskKey &that);
  BackupJobType get_key_type() const { return type_; }

  TO_STRING_KV(K_(tenant_id),
               K_(job_id),
               K_(task_id),
               K_(ls_id),
               K_(type));
private:
  uint64_t inner_hash() const;
private:
  uint64_t tenant_id_;
  uint64_t job_id_;
  uint64_t task_id_;
  uint64_t ls_id_;
  BackupJobType type_;
  uint64_t hash_value_;
};

// ObBackupTask is the based calss
// derived class include Backup，BackupBackup and so on
class ObBackupScheduleTask : public common::ObDLinkBase<ObBackupScheduleTask>
{
public:
  friend class ObBackupTaskScheduler;
  friend class ObBackupTaskQueue;

public:
  typedef common::hash::ObHashMap<ObBackupScheduleTaskKey,
                                  ObBackupScheduleTask *,
                                  common::hash::NoPthreadDefendMode> TaskMap;

public:
  ObBackupScheduleTask()
    : task_key_(),
      trace_id_(),
      optional_servers_(),
      dst_(),
      generate_time_(common::ObTimeUtility::current_time()),
      schedule_time_(0),
      executor_time_(0)
  {
  }
  virtual ~ObBackupScheduleTask()
  {
  }

public:
  // pure interfaces related to task cheduler
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const = 0;
  virtual int64_t get_deep_copy_size() const = 0;
  // interfaces related to execution
  virtual bool can_execute_on_any_server() const = 0;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const = 0;
  virtual int cancel(obrpc::ObSrvRpcProxy &rpc_proxy) const = 0;
  // check backup task can remote execute 
  virtual bool can_cross_machine_exec() { return false; };
private:
  // write inner table to update task dst, trace id and advnace task status to doing
  virtual int do_update_dst_and_doing_status_(common::ObISQLClient &sql_proxy, common::ObAddr &dst, share::ObTaskId &trace_id) = 0;
public:
  int build_from_res(const obrpc::ObBackupTaskRes &res, const BackupJobType &type);
  int build(const ObBackupScheduleTaskKey &key, const share::ObTaskId &trace_id, const share::ObBackupTaskStatus &status, const common::ObAddr &dst);
  int update_dst_and_doing_status(common::ObMySQLProxy &sql_proxy);
  int set_schedule(const common::ObAddr &server);
  void clear_schedule();
  bool in_schedule() const { return schedule_time_ > 0; }
  // for copy
  int deep_copy(const ObBackupScheduleTask &that);
  int init_task_key(const ObBackupScheduleTaskKey &key) { return task_key_.init(key); }
  uint64_t get_tenant_id() const { return task_key_.tenant_id_; }
  uint64_t get_job_id() const { return task_key_.job_id_; }
	void set_trace_id(share::ObTaskId trace_id) { trace_id_ = trace_id; }
  const share::ObTaskId &get_trace_id() const { return trace_id_; }
  void set_dst(const common::ObAddr &server) { dst_ = server; }
  const common::ObAddr &get_dst() const { return dst_; }
  const ObBackupScheduleTaskKey &get_task_key() const { return task_key_; }
  int set_optional_servers(const ObIArray<share::ObBackupServer> &servers);
  const ObIArray<share::ObBackupServer> &get_optional_servers() const { return optional_servers_; }
  void set_generate_time(int64_t now) { generate_time_ = now; }
  const int64_t &get_generate_time() const { return generate_time_; }
  void set_executor_time(int64_t now) { executor_time_ = now; }
  const int64_t &get_executor_time() const { return executor_time_; }
  void set_schedule_time(int64_t now) { schedule_time_ = now; }
  const int64_t &get_schedule_time() const { return schedule_time_; }
  const uint64_t &get_task_id() const { return task_key_.task_id_; }
  const uint64_t &get_ls_id() const { return task_key_.ls_id_; }
  const BackupJobType &get_type() const { return task_key_.type_; }
  const share::ObBackupTaskStatus &get_status() const { return status_; }
public:
  /* disallow copy constructor and operator= */
  ObBackupScheduleTask(const ObBackupScheduleTask &) = delete;
  ObBackupScheduleTask &operator=(const ObBackupScheduleTask &) = delete;
  TO_STRING_KV(K_(task_key), K_(trace_id), K_(dst), K_(status), K_(optional_servers), K_(generate_time),
      K_(schedule_time), K_(executor_time));
private:
  ObBackupScheduleTaskKey task_key_;  // the key for map to indicate a specific task
  share::ObTaskId trace_id_;
  // server which has replica. for data backup job, server can't be strong leader replica's server
  ObSEArray<share::ObBackupServer, OB_MAX_MEMBER_NUMBER> optional_servers_;
  common::ObAddr dst_; // choosed server
  share::ObBackupTaskStatus status_;
  int64_t generate_time_; // time of generating task
  int64_t schedule_time_; // time of choosing task dst
  int64_t executor_time_; // time of sending to dst
};

// backup data task

class ObBackupDataBaseTask : public ObBackupScheduleTask
{
public:
  ObBackupDataBaseTask();
  virtual ~ObBackupDataBaseTask() {}
public:
  virtual bool can_execute_on_any_server() const final override { return false; }
  virtual int cancel(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
  virtual int build(const share::ObBackupJobAttr &job_attr, const share::ObBackupSetTaskAttr &set_task_attr,
      const share::ObBackupLSTaskAttr &ls_attr);
  int deep_copy(const ObBackupDataBaseTask &that);
  int set_optional_servers_(const ObIArray<common::ObAddr> &black_servers);
private:
  virtual int do_update_dst_and_doing_status_(common::ObISQLClient &sql_proxy, common::ObAddr &dst,
      share::ObTaskId &trace_id) final override;
  virtual bool execute_on_sys_server_() const { return false; }
  bool check_replica_in_black_server_(const share::ObLSReplica &replica, const ObIArray<common::ObAddr> &black_servers);
public:
  INHERIT_TO_STRING_KV("ObBackupScheduleTask", ObBackupScheduleTask, K_(incarnation_id), K_(backup_set_id),
      K_(backup_type), K_(backup_date), K_(ls_id), K_(turn_id), K_(retry_id), K_(start_scn),
      K_(backup_user_ls_scn), K_(end_scn), K_(backup_path), K_(backup_status));
protected:
  int64_t incarnation_id_;
  int64_t backup_set_id_;
  share::ObBackupType backup_type_;
  int64_t backup_date_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  share::SCN start_scn_;
  share::SCN backup_user_ls_scn_;
  share::SCN end_scn_;
  share::ObBackupPathString backup_path_;
  share::ObBackupStatus backup_status_;
  bool is_only_calc_stat_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataBaseTask);
};

class ObBackupDataLSTask : public ObBackupDataBaseTask
{
public:
  ObBackupDataLSTask() {}
  virtual ~ObBackupDataLSTask() {}
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override;
  virtual int64_t get_deep_copy_size() const override;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataLSTask);
};

class ObBackupComplLogTask final: public ObBackupDataBaseTask
{
public:
  ObBackupComplLogTask() {}
  virtual ~ObBackupComplLogTask() {}
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override;
  virtual int64_t get_deep_copy_size() const override;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
  virtual int build(const share::ObBackupJobAttr &job_attr, const share::ObBackupSetTaskAttr &set_task_attr,
      const share::ObBackupLSTaskAttr &ls_attr);
private:
  int calc_start_replay_scn_(const share::ObBackupJobAttr &job_attr, const share::ObBackupSetTaskAttr &set_task_attr,
      const share::ObBackupLSTaskAttr &ls_attr, share::SCN &scn);
  bool execute_on_sys_server_() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupComplLogTask); 
};

class ObBackupBuildIndexTask final : public ObBackupDataBaseTask
{
public:
  ObBackupBuildIndexTask() {}
  virtual ~ObBackupBuildIndexTask() {}
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override;
  virtual int64_t get_deep_copy_size() const override;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
private:
  bool execute_on_sys_server_() const override { return true; }
  DISALLOW_COPY_AND_ASSIGN(ObBackupBuildIndexTask);
};

class ObBackupDataLSMetaTask final : public ObBackupDataLSTask
{
public:
  ObBackupDataLSMetaTask() {}
  virtual ~ObBackupDataLSMetaTask() {}
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override;
  virtual int64_t get_deep_copy_size() const override;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataLSMetaTask);
};

class ObBackupDataLSMetaFinishTask final : public ObBackupDataLSTask
{
public:
  ObBackupDataLSMetaFinishTask() {}
  virtual ~ObBackupDataLSMetaFinishTask() {}
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override;
  virtual int64_t get_deep_copy_size() const override;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataLSMetaFinishTask);
};

class ObBackupCleanLSTask : public ObBackupScheduleTask
{
public:
  ObBackupCleanLSTask();
  virtual ~ObBackupCleanLSTask();
public:
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override;
  virtual int64_t get_deep_copy_size() const override;
  // interfaces related to execution
  virtual bool can_execute_on_any_server() const override;
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
  virtual int cancel(obrpc::ObSrvRpcProxy &rpc_proxy) const override;
private:
  virtual int do_update_dst_and_doing_status_(common::ObISQLClient &sql_proxy, common::ObAddr &dst, share::ObTaskId &trace_id) final override;
  int set_optional_servers_();
public:
  int build(const share::ObBackupCleanTaskAttr &task_attr, const share::ObBackupCleanLSTaskAttr &ls_attr);
  INHERIT_TO_STRING_KV("ObBackupScheduleTask", ObBackupScheduleTask, K_(job_id), K_(incarnation_id), K_(id), K_(round_id),
               K_(task_type), K_(ls_id), K_(dest_id), K_(backup_path));
private:
  int64_t job_id_;
  uint64_t incarnation_id_;
  int64_t id_;
  uint64_t round_id_;
  share::ObBackupCleanTaskType::TYPE task_type_;
  share::ObLSID ls_id_;
  int64_t dest_id_;
  share::ObBackupPathString backup_path_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanLSTask); 
};

}  // namespace rootserver
}  // end namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_SCHEDULE_TASK_H_
