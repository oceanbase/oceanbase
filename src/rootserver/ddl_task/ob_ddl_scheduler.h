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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_DDL_SCHEDULER_H_

#include "share/ob_ddl_task_executor.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_column_redefinition_task.h"
#include "rootserver/ddl_task/ob_constraint_task.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"
#include "rootserver/ddl_task/ob_ddl_retry_task.h"
#include "rootserver/ddl_task/ob_drop_index_task.h"
#include "rootserver/ddl_task/ob_drop_primary_key_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_modify_autoinc_task.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "rootserver/ob_thread_idling.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace common
{
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace rootserver
{
class ObRootService;

class ObDDLTaskQueue
{
public:
  ObDDLTaskQueue();
  virtual ~ObDDLTaskQueue();
  int init(const int64_t bucket_num);
  bool has_set_stop() const { return ATOMIC_LOAD(&stop_); }
  void set_stop(bool stop) { ATOMIC_STORE(&stop_, stop); }
  int push_task(ObDDLTask *task);
  int get_next_task(ObDDLTask *&task);
  int remove_task(ObDDLTask *task);
  int add_task_to_last(ObDDLTask *task);
  template<typename F>
  int modify_task(const ObDDLTaskKey &task_key, F &&op);
  template<typename F>
  int modify_task(const ObDDLTaskID &task_id, F &&op);
  template<typename F>
  int get_task(const ObDDLTaskKey &task_key, F &&op);
  int update_task_copy_deps_setting(const ObDDLTaskID &task_id,
                                    const bool is_copy_constraints,
                                    const bool is_copy_indexes,
                                    const bool is_copy_triggers,
                                    const bool is_copy_foreign_keys,
                                    const bool is_ignore_errors);
  int update_task_process_schedulable(const ObDDLTaskID &task_id);
  int update_task_ret_code(const ObDDLTaskID &task_id, const int ret_code);
  int abort_task(const ObDDLTaskID &task_id);
  int64_t get_task_cnt() const { return task_list_.get_size(); }
  void destroy();
private:
  typedef common::ObDList<ObDDLTask> TaskList;
  typedef common::hash::ObHashMap<ObDDLTaskKey, ObDDLTask *,
          common::hash::NoPthreadDefendMode> TaskKeyMap;
  typedef common::hash::ObHashMap<ObDDLTaskID, ObDDLTask *,
          common::hash::NoPthreadDefendMode> TaskIdMap;
  TaskList task_list_;
  TaskKeyMap task_map_;
  TaskIdMap task_id_map_;
  common::ObSpinLock lock_;
  bool stop_;
  bool is_inited_;
};

class ObDDLTaskHeartBeatMananger final
{
public:
  ObDDLTaskHeartBeatMananger();
  ~ObDDLTaskHeartBeatMananger();
  int init();
  int update_task_active_time(const ObDDLTaskID &task_id);
  int remove_task(const ObDDLTaskID &task_id);
  int get_inactive_ddl_task_ids(ObArray<ObDDLTaskID>& remove_task_ids);
private:
  static const int64_t BUCKET_LOCK_BUCKET_CNT = 10243L;
  common::hash::ObHashMap<ObDDLTaskID, int64_t> register_task_time_;
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
};
struct ObPrepareAlterTableArgParam final
{
public:
  ObPrepareAlterTableArgParam() :
    session_id_(common::OB_INVALID_ID),
    sql_mode_(0),
    tz_info_wrap_(),
    allocator_(lib::ObLabel("PrepAlterTblArg")),
    nls_formats_{}
  {}
  ~ObPrepareAlterTableArgParam() = default;
  int init(const uint64_t session_id,
          const ObSQLMode &sql_mode,
          const ObString &ddl_stmt_str,
          const ObString &orig_table_name,
          const ObString &orig_database_name,
          const ObString &target_database_name,
          const ObTimeZoneInfo &tz_info,
          const ObTimeZoneInfoWrap &tz_info_wrap,
          const ObString *nls_formats);
  bool is_valid() const
  {
    return OB_INVALID_ID != session_id_ &&
            !orig_table_name_.empty() &&
            !orig_database_name_.empty() &&
            !target_database_name_.empty();
  }
  int set_nls_formats(const common::ObString *nls_formats);
  TO_STRING_KV(K_(session_id),
                K_(sql_mode),
                K_(ddl_stmt_str),
                K_(orig_table_name),
                K_(orig_database_name),
                K_(target_database_name),
                K_(tz_info_wrap),
                "nls_formats", common::ObArrayWrap<ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX));
public:
  uint64_t session_id_;
  ObSQLMode sql_mode_;
  common::ObString ddl_stmt_str_;
  common::ObString orig_table_name_;
  common::ObString orig_database_name_;
  common::ObString target_database_name_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObArenaAllocator allocator_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
};

class ObRedefCallback
{
public:
  ObRedefCallback() : infos_(nullptr) {}
  virtual ~ObRedefCallback() = default;

  virtual int modify_info(ObTableRedefinitionTask &redef_task,
                          ObDDLTaskQueue &task_queue,
                          ObISQLClient &trans);
  virtual int update_redef_task_info(ObTableRedefinitionTask& redef_task) = 0;
  virtual int update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                      ObDDLTaskQueue &ddl_task_queue) = 0;
protected:
  common::hash::ObHashMap<ObString, bool> *infos_;
};

class ObAbortRedefCallback : public ObRedefCallback
{
public:
  ObAbortRedefCallback() = default;
  virtual ~ObAbortRedefCallback() = default;
  virtual int update_redef_task_info(ObTableRedefinitionTask& redef_task) override;
  virtual int update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                      ObDDLTaskQueue &ddl_task_queue) override;
};

class ObCopyTableDepCallback : public ObRedefCallback
{
public:
  ObCopyTableDepCallback() = default;
  virtual ~ObCopyTableDepCallback() = default;
  virtual int update_redef_task_info(ObTableRedefinitionTask& redef_task) override;
  virtual int update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                      ObDDLTaskQueue &ddl_task_queue) override;
  int set_infos(common::hash::ObHashMap<ObString, bool> *infos);
};

class ObFinishRedefCallback : public ObRedefCallback
{
public:
  ObFinishRedefCallback() = default;
  virtual ~ObFinishRedefCallback() = default;
  virtual int update_redef_task_info(ObTableRedefinitionTask& redef_task) override;
  virtual int update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                      ObDDLTaskQueue &ddl_task_queue) override;
};

class ObUpdateSSTableCompleteStatusCallback : public ObRedefCallback
{
public:
  ObUpdateSSTableCompleteStatusCallback()
    : ret_code_(OB_SUCCESS)
  {}
  ~ObUpdateSSTableCompleteStatusCallback() = default;
  virtual int update_redef_task_info(ObTableRedefinitionTask& redef_task) override;
  virtual int update_task_info_in_queue(ObTableRedefinitionTask& redef_task,
                                      ObDDLTaskQueue &ddl_task_queue) override;
  int set_ret_code(const int ret_code);
private:
  int ret_code_;
};

/*
 * the only scheduler for all ddl tasks executed in root service
 *
 * each category of ddl request has an unique task type.
 * every ddl task has its record in an inner table(__all_ddl_task_status),
 * which will be used to recover or cleanup the task when the root server has switched
 */
class ObDDLScheduler : public lib::TGRunnable
{
public:
  ObDDLScheduler();
  virtual ~ObDDLScheduler();
  int init(ObRootService *root_service);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void run1() override;

  int create_ddl_task(
      const ObCreateDDLTaskParam &param,
      common::ObISQLClient &proxy,
      ObDDLTaskRecord &task_record);

  int schedule_ddl_task(
      const ObDDLTaskRecord &task_record);
  int recover_task();
  int remove_inactive_ddl_task();

  int destroy_task();

  int on_column_checksum_calc_reply(
      const common::ObTabletID &tablet_id,
      const ObDDLTaskKey &task_key,
      const int ret_code);

  int on_sstable_complement_job_reply(
      const common::ObTabletID &tablet_id,
      const ObDDLTaskKey &task_key,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int ret_code,
      const ObDDLTaskInfo &addition_info);

  int on_ddl_task_finish(
      const ObDDLTaskID &parent_task_id,
      const ObDDLTaskKey &task_key,
      const int ret_code,
      const ObCurTraceId::TraceId &parent_task_trace_id);

  int notify_update_autoinc_end(
      const ObDDLTaskKey &task_key,
      const uint64_t autoinc_val,
      const int ret_code);
  int get_task_record(const ObDDLTaskID &task_id,
                      ObISQLClient &trans,
                      ObDDLTaskRecord &task_record,
                      common::ObIAllocator &allocator);
  int modify_redef_task(const ObDDLTaskID &task_id, ObRedefCallback &cb);
  int abort_redef_table(const ObDDLTaskID &task_id);

  int copy_table_dependents(const ObDDLTaskID &task_id,
                            const bool is_copy_constraints,
                            const bool is_copy_indexes,
                            const bool is_copy_triggers,
                            const bool is_copy_foreign_keys,
                            const bool is_ignore_errors);
  int finish_redef_table(const ObDDLTaskID &task_id);
  int start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res);
  int update_ddl_task_active_time(const ObDDLTaskID &task_id);
  int prepare_alter_table_arg(const ObPrepareAlterTableArgParam &param,
                              const ObTableSchema *target_table_schema,
                              obrpc::ObAlterTableArg &alter_table_arg);
private:
  class DDLIdling : public ObThreadIdling
  {
  public:
    explicit DDLIdling(volatile bool &stop): ObThreadIdling(stop) {}
    virtual ~DDLIdling() {}
    virtual int64_t get_idle_interval_us() override { return 1000L * 1000L; }
  };
  class DDLScanTask : private common::ObTimerTask
  {
  public:
    explicit DDLScanTask(ObDDLScheduler &ddl_scheduler): ddl_scheduler_(ddl_scheduler) {}
    virtual ~DDLScanTask() {};
    int schedule(int tg_id);
  private:
    void runTimerTask() override;
  private:
    static const int64_t DDL_TASK_SCAN_PERIOD = 60 * 1000L * 1000L; // 60s
    ObDDLScheduler &ddl_scheduler_;
  };

  class HeartBeatCheckTask : public common::ObTimerTask
  {
  public:
    explicit HeartBeatCheckTask(ObDDLScheduler &ddl_scheduler): ddl_scheduler_(ddl_scheduler) {}
    virtual ~HeartBeatCheckTask() {};
    int schedule(int tg_id);
  private:
    void runTimerTask() override;
  private:
    static const int64_t DDL_TASK_CHECK_PERIOD = 30 * 1000L * 1000L; // 30s
    ObDDLScheduler &ddl_scheduler_;
  };
private:
  int insert_task_record(
      common::ObISQLClient &proxy,
      ObDDLTask &ddl_task,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);
  template<typename T>
  int alloc_ddl_task(T *&ddl_task);
  void free_ddl_task(ObDDLTask *ddl_task);
  void destroy_all_tasks();
  int inner_schedule_ddl_task(ObDDLTask *ddl_task,
                              const ObDDLTaskRecord &task_record);
  int create_build_index_task(
      common::ObISQLClient &proxy,
      const share::schema::ObTableSchema *data_table_schema,
      const share::schema::ObTableSchema *index_schema,
      const int64_t parallelism,
      const int64_t parent_task_id,
      const int64_t consumer_group_id,
      const obrpc::ObCreateIndexArg *create_index_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);
  int create_constraint_task(
      common::ObISQLClient &proxy,
      const share::schema::ObTableSchema *table_schema,
      const int64_t constraint_id,
      const share::ObDDLType ddl_type,
      const int64_t schema_version,
      const obrpc::ObAlterTableArg *arg,
      const int64_t parent_task_id,
      const int64_t consumer_group_id,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int create_table_redefinition_task(
      common::ObISQLClient &proxy,
      const share::ObDDLType &type,
      const share::schema::ObTableSchema *src_schema,
      const share::schema::ObTableSchema *dest_schema,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const int64_t task_id,
      const obrpc::ObAlterTableArg *alter_table_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int create_drop_primary_key_task(
      common::ObISQLClient &proxy,
      const share::ObDDLType &type,
      const ObTableSchema *src_schema,
      const ObTableSchema *dest_schema,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const int64_t task_id,
      const obrpc::ObAlterTableArg *alter_table_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int create_column_redefinition_task(
      common::ObISQLClient &proxy,
      const share::ObDDLType &type,
      const share::schema::ObTableSchema *src_schema,
      const share::schema::ObTableSchema *dest_schema,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const int64_t task_id,
      const obrpc::ObAlterTableArg *alter_table_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int create_modify_autoinc_task(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t table_id,
      const int64_t schema_version,
      const int64_t consumer_group_id,
      const int64_t task_id,
      const obrpc::ObAlterTableArg *alter_table_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int create_drop_index_task(
      common::ObISQLClient &proxy,
      const share::schema::ObTableSchema *index_schema,
      const int64_t parent_task_id,
      const int64_t consumer_group_id,
      const obrpc::ObDropIndexArg *drop_index_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);
  
  int create_ddl_retry_task(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const uint64_t object_id,
      const int64_t schema_version,
      const int64_t consumer_group_id,
      const share::ObDDLType &type,
      const obrpc::ObDDLArg *arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int create_recover_restore_table_task(
      common::ObISQLClient &proxy,
      const share::ObDDLType &type,
      const share::schema::ObTableSchema *src_schema,
      const share::schema::ObTableSchema *dest_schema,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const int64_t task_id,
      const obrpc::ObAlterTableArg *alter_table_arg,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  int schedule_build_index_task(
      const ObDDLTaskRecord &task_record);
  int schedule_drop_primary_key_task(const ObDDLTaskRecord &task_record);
  int schedule_table_redefinition_task(const ObDDLTaskRecord &task_record);
  int schedule_constraint_task(const ObDDLTaskRecord &task_record);
  int schedule_column_redefinition_task(const ObDDLTaskRecord &task_record);
  int schedule_modify_autoinc_task(const ObDDLTaskRecord &task_record);
  int schedule_drop_index_task(const ObDDLTaskRecord &task_record);
  int schedule_ddl_retry_task(const ObDDLTaskRecord &task_record);
  int schedule_recover_restore_table_task(const ObDDLTaskRecord &task_record);
  int add_sys_task(ObDDLTask *task);
  int remove_sys_task(ObDDLTask *task);
  int add_task_to_longops_mgr(ObDDLTask *ddl_task);
  int remove_task_from_longops_mgr(ObDDLTask *ddl_task);
  int remove_ddl_task(ObDDLTask *ddl_task);

private:
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  bool is_inited_;
  bool is_started_;
  int tg_id_;
  ObRootService *root_service_;
  bool idle_stop_;
  DDLIdling idler_;
  common::ObConcurrentFIFOAllocator allocator_;
  ObDDLTaskQueue task_queue_;
  ObDDLTaskHeartBeatMananger manager_reg_heart_beat_task_;
  DDLScanTask scan_task_;
  HeartBeatCheckTask heart_beat_check_task_;
};

template<typename T>
int ObDDLScheduler::alloc_ddl_task(T *&ddl_task)
{
  int ret = OB_SUCCESS;
  ddl_task = nullptr;
  void *tmp_buf = nullptr;
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RS_LOG(WARN, "alloc ddl task failed", K(ret));
  } else {
    ddl_task = new (tmp_buf) T;
  }
  return ret;
}


} // end namespace rootserver
} // end namespace oceanbase


#endif /* OCEANBASE_ROOTSERVER_OB_DDL_SCHEDULER_H_ */
