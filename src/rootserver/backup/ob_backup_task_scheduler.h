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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_TASK_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_TASK_SCHEDULER_H_

#include "ob_backup_schedule_task.h"
#include "ob_backup_lease_service.h"
#include "lib/lock/ob_thread_cond.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_thread_idling.h"
#include "share/ob_srv_rpc_proxy.h"
namespace oceanbase 
{
namespace lib
{
class ObMutex;
}
namespace rootserver 
{
class ObZoneManager;
class ObBackupTaskScheduler;
class ObBackupService;
class ObBackupTaskSchedulerQueue 
{
public:
  typedef common::ObDList<ObBackupScheduleTask> TaskList;
  typedef common::hash::ObHashMap<ObBackupScheduleTaskKey, 
                                  ObBackupScheduleTask *, 
                                  common::hash::NoPthreadDefendMode> TaskMap;
  ObBackupTaskSchedulerQueue();
  virtual ~ObBackupTaskSchedulerQueue();

  int init(ObTenantBackupScheduleTaskStatMap &tenant_stat_map,
           ObServerBackupScheduleTaskStatMap &server_stat_map,
           ObZoneManager &zone_manager,
	         ObBackupService &backup_mgr,
           const int64_t bucket_num, 
           obrpc::ObSrvRpcProxy *rpc_proxy,
           ObBackupTaskScheduler *task_scheduler,
           const int64_t max_size,
           common::ObMySQLProxy &sql_proxy,
           ObBackupLeaseService &lease_service);

  // try to add task in queue
  // return OB_ENTRY_EXIST if insert a task which already in queue
  // return OB_SUCCESS if add new task
  int push_task(const ObBackupScheduleTask &task);

  // try to pop task to excute
  // get one task from wait_list_, for per task choosing a server to execute,
  // then set to scheduler state and move to schedule_list;
  // return OB_SUCCESS or assign NULL to task, if no task can be scheduled
  int pop_task(ObBackupScheduleTask *&output_task, common::ObArenaAllocator &allocator);
  int execute_over(const ObBackupScheduleTask &task, const int execute_ret);
  // remove task 
  // When finished, task memory will be released and %task can not be used again.
  int reload_task(const ObArray<ObBackupScheduleTask *> &need_reload_tasks);
  int cancel_tasks(const BackupJobType &type, const uint64_t job_id, const uint64_t tenant_id);
  int cancel_tasks(const BackupJobType &type, const uint64_t tenant_id);
  int check_task_exist(const ObBackupScheduleTaskKey key, bool &is_exist);
  int get_all_tasks(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks);
  int get_schedule_tasks(ObIArray<ObBackupScheduleTask *> &schedule_tasks, ObArenaAllocator &allocator);
  void reset();
  int dump_statistics();
  int64_t get_task_cnt() const { return task_map_.size(); }
private:
  virtual int get_backup_region_and_zone_(ObIArray<share::ObBackupZone> &backup_zone,
                                          ObIArray<share::ObBackupRegion> &backup_region);
  virtual int get_all_servers_(const ObIArray<share::ObBackupZone> &backup_zone,
                               const ObIArray<share::ObBackupRegion> &backup_region,
                               ObIArray<share::ObBackupServer> &servers);
  virtual int get_all_zones_(const ObIArray<share::ObBackupZone> &backup_zone,
                             const ObIArray<share::ObBackupRegion> &backup_region,
                             ObIArray<share::ObBackupZone> &zones);
  int choose_dst_(const ObBackupScheduleTask &task, 
                  const ObIArray<share::ObBackupServer> &servers,
                  ObAddr &dst,
                  bool &can_schedule);
  int get_alternative_servers_(const ObBackupScheduleTask &task, 
                               const ObIArray<share::ObBackupServer> &servers,
                               ObArray<ObAddr> &alternative_servers);
  int check_server_can_become_dst_(const ObAddr &server, const BackupJobType &type, bool &can_dst);
  int get_dst_(const ObIArray<ObAddr> &alternative_servers, common::ObAddr &dst);
  int set_server_stat_(const ObAddr &dst, const BackupJobType &type);
  int set_tenant_stat_(const int64_t &tenant_id);
  int clean_server_ref_(const ObAddr &dst, const BackupJobType &type);
  int clean_tenant_ref_(const int64_t &tenant_id);
  int clean_task_map(const ObBackupScheduleTaskKey &task_key);
  // get in schedule task
  // used to detect whether the finished task is scheduled from this queue,
  // Set task to nullptr if in schedule task not found
  int get_schedule_task_(const ObBackupScheduleTask &input_task, ObBackupScheduleTask *&task);
  int get_wait_task_(const ObBackupScheduleTask &input_task, ObBackupScheduleTask *&task);
  // remove task from the scheduler_list and wait_list
  int remove_task_(ObBackupScheduleTask *task, const bool &in_schedule);
  int check_push_unique_task_(const ObBackupScheduleTask &task);

  int64_t get_task_cnt_() const { return task_map_.size(); }
  int64_t get_wait_task_cnt_() const { return wait_list_.get_size(); }
  int64_t get_in_schedule_task_cnt_() const { return schedule_list_.get_size(); }

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  int64_t max_size_;
  // Count the number of tasks per tenant. key: tenant_id, value :struct for task_cnt
  ObTenantBackupScheduleTaskStatMap *tenant_stat_map_;
  // Count the number of tasks per server, key: server_addr value :struct for statistical information
  ObServerBackupScheduleTaskStatMap *server_stat_map_;
  common::ObFIFOAllocator task_allocator_;
  // task in wait_list waiting to schedule
  TaskList wait_list_;
  // task in schedule_list has scheduled
  TaskList schedule_list_;
  // key: task_key value: task
  TaskMap task_map_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  ObZoneManager *zone_mgr_;
  ObBackupService *backup_service_;
  common::ObMySQLProxy *sql_proxy_; 
  ObBackupLeaseService *lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTaskSchedulerQueue);
};

class ObBackupSchedulerIdling : public ObThreadIdling {
public:
  explicit ObBackupSchedulerIdling(volatile bool &stop) : ObThreadIdling(stop) {}
  virtual int64_t get_idle_interval_us();
};

class ObBackupTaskScheduler : public ObRsReentrantThread 
{
public:
  const static int64_t MAX_BACKUP_TASK_QUEUE_LIMIT = 1 << 20;
  const static int64_t CONCURRENCY_LIMIT_INTERVAL = 10 * 60 * 1000000L;  // 10min
  const static int64_t BACKUP_TASK_CONCURRENCY = 1;
  const static int64_t BACKUP_SERVER_DATA_LIMIT_INTERVAL = 20 * 60 * 1000000; // 60 min;
  const static int64_t TASK_SCHEDULER_STATISTIC_INTERVAL = 1000000L; // 1s

public:
  ObBackupTaskScheduler();

  int init(ObZoneManager *zone_mgr_,
           obrpc::ObSrvRpcProxy *rpc_proxy,
           ObBackupService *backup_mgr,
           common::ObMySQLProxy &sql_proxy,
           ObBackupLeaseService &lease_service);

  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  void stop();
  void wakeup();
  int idle() const;
public:
  // add_task() will nerver block
  // Return OB_ENTRY_EXIST if the task already exist in the scheduler
  // (Caller should ignore OB_ENTRY_EXIST and continue add task)
  // Return OB_SISE_OVERFLOW if queue already full. (caller should stop adding task)
  virtual int add_task(const ObBackupScheduleTask &task);
  virtual int check_task_exist(const ObBackupScheduleTaskKey key, bool &is_exist);
  // call when task execute finish
  // remove task from scheduler
  virtual int execute_over(const ObBackupScheduleTask &input_task, const int &execute_ret);
  virtual int get_all_tasks(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks);
  virtual int cancel_tasks(const BackupJobType &type, const uint64_t job_id, const uint64_t tenant_id);
  int cancel_tasks(const BackupJobType &type, const uint64_t tenant_id);
  int reuse();
  int reload_task_(int64_t &last_reload_task_ts, bool &reload_flag);
  share::schema::ObMultiVersionSchemaService &get_schema_service() { return *schema_service_; }
private:
  // Send task to execute.
  int execute_task_(const ObBackupScheduleTask &task);
  int do_execute_(const ObBackupScheduleTask &task);
  void dump_statistics_(int64_t &last_dump_time);
  int check_alive_(int64_t &last_check_task_on_server_ts, bool &reload_flag);
  int pop_and_send_task_();
  
private:
  bool is_inited_;
  mutable ObBackupSchedulerIdling idling_;
  lib::ObMutex scheduler_mtx_;
  // Count the number of tasks per tenant. key: tenant_id, value :struct for task_cnt
  ObTenantBackupScheduleTaskStatMap tenant_stat_map_;
  // Count the number of tasks per server, key: server_addr value :struct for statistical information
  ObServerBackupScheduleTaskStatMap server_stat_map_;
  ObBackupTaskSchedulerQueue queue_;
  // scheduler's self server addr
  common::ObAddr self_;
  ObZoneManager *zone_mgr_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupService *backup_service_;
  ObBackupLeaseService *lease_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTaskScheduler);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_TASK_SCHEDULER_H_