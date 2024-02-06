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

#ifndef SRC_OBSERVER_OB_DBMS_SCHED_JOB_MASTER_H_
#define SRC_OBSERVER_OB_DBMS_SCHED_JOB_MASTER_H_

#include "ob_dbms_sched_job_rpc_proxy.h"
#include "ob_dbms_sched_job_utils.h"
#include "ob_dbms_sched_table_operator.h"

#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/task/ob_timer.h"
#include "lib/container/ob_iarray.h"

#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"

#include "observer/dbms_job/ob_dbms_job_utils.h"
#include "rootserver/ob_ddl_service.h"


namespace oceanbase
{

namespace dbms_scheduler
{
class ObDBMSSchedJobThread : public ObSimpleThreadPool
{
public:
  ObDBMSSchedJobThread() {}
  virtual ~ObDBMSSchedJobThread() {}
private:
  virtual void handle(void *task);
};

class ObDBMSSchedJobKey : public common::ObLink
{
public:
  ObDBMSSchedJobKey(
    uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const common::ObString &job_name,
    uint64_t execute_at, uint64_t delay,
    bool check_job)
  : tenant_id_(tenant_id),
    is_oracle_tenant_(is_oracle_tenant),
    job_id_(job_id),
    job_name_(),
    execute_at_(execute_at),
    delay_(delay),
    check_job_(check_job) {
      job_name_.assign_buffer(job_name_buf_, JOB_NAME_MAX_SIZE);
      job_name_.write(job_name.ptr(), job_name.length());
    }

  virtual ~ObDBMSSchedJobKey() {}

  static constexpr int64_t JOB_NAME_MAX_SIZE = 128;
  OB_INLINE uint64_t get_job_id_with_tenant() const { return common::combine_two_ids(tenant_id_, job_id_); }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_job_id() const { return job_id_; }
  OB_INLINE common::ObString &get_job_name() { return job_name_; }
  OB_INLINE uint64_t get_execute_at() const { return execute_at_;}
  OB_INLINE uint64_t get_delay() const { return delay_; }

  OB_INLINE bool is_check() { return check_job_; }

  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_job_id(uint64_t job_id) { job_id_ = job_id; }

  OB_INLINE void set_execute_at(uint64_t execute_at) { execute_at_ = execute_at; }
  OB_INLINE void set_delay(uint64_t delay) { delay_ = delay; }

  OB_INLINE void set_check_job(bool check_job) { check_job_ = check_job; }

  OB_INLINE uint64_t get_adjust_delay() const
  {
    uint64_t now = ObTimeUtility::current_time();
    return (execute_at_ < now) ? 0 : (execute_at_ - now);
  }

  OB_INLINE bool is_valid()
  {
    return job_id_ != OB_INVALID_ID && tenant_id_ != OB_INVALID_ID;
  }

  bool is_oracle_tenant() { return is_oracle_tenant_; }

  TO_STRING_KV(
    K_(tenant_id),
    K_(is_oracle_tenant),
    K_(job_id),
    K_(job_name),
    K_(execute_at),
    K_(delay),
    K_(check_job));

private:
  uint64_t tenant_id_;
  bool is_oracle_tenant_;
  uint64_t job_id_;
  char job_name_buf_[JOB_NAME_MAX_SIZE];
  common::ObString job_name_;
  uint64_t execute_at_;
  uint64_t delay_;

  bool check_job_; // for check job update ...
};

class ObDBMSSchedJobTask : public ObTimerTask
{
public:
  typedef common::ObSortedVector<ObDBMSSchedJobKey *> WaitVector;
  typedef WaitVector::iterator WaitVectorIterator;

  ObDBMSSchedJobTask()
    : inited_(false),
      job_key_(NULL),
      ready_queue_(NULL),
      wait_vector_(0, NULL, ObModIds::VECTOR),
      lock_(common::ObLatchIds::DBMS_SCHEDULER_TASK_LOCK) {}

  virtual ~ObDBMSSchedJobTask() {}

  int init();
  int start(dbms_job::ObDBMSJobQueue *ready_queue);
  int stop();
  int destroy();

  void runTimerTask();

  int scheduler(ObDBMSSchedJobKey *job_key);
  int add_new_job(ObDBMSSchedJobKey *job_key);
  int immediately(ObDBMSSchedJobKey *job_key);

  inline static bool compare_job_key(
    const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs);
  inline static bool equal_job_key(
    const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs);

private:
  bool inited_;
  ObDBMSSchedJobKey *job_key_;
  dbms_job::ObDBMSJobQueue *ready_queue_;
  WaitVector wait_vector_;

  ObSpinLock lock_;
  ObTimer timer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSSchedJobTask);
};

class ObDBMSSchedJobMaster
{
public:
  ObDBMSSchedJobMaster()
    : inited_(false),
      stoped_(false),
      running_(false),
      trace_id_(NULL),
      rand_(),
      schema_service_(NULL),
      job_rpc_proxy_(NULL),
      self_addr_(),
      lock_(common::ObLatchIds::DBMS_SCHEDULER_MASTER_LOCK),
      alive_jobs_() {}

  virtual ~ObDBMSSchedJobMaster() { alive_jobs_.destroy(); };

  static ObDBMSSchedJobMaster &get_instance();

  bool is_inited() { return inited_; }

  int init(rootserver::ObUnitManager *unit_mgr,
           common::ObISQLClient *sql_client,
           share::schema::ObMultiVersionSchemaService *schema_service);

  int start();
  int stop();
  int scheduler();
  int destroy();

  int alloc_job_key(
    ObDBMSSchedJobKey *&job_key,
    uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const common::ObString &job_name,
    uint64_t execute_at, uint64_t delay,
    bool check_job = false);

  int server_random_pick(int64_t tenant_id, common::ObString &pick_zone, ObAddr &server);
  int get_execute_addr(ObDBMSSchedJobInfo &job_info, common::ObAddr &execute_addr);

  int check_all_tenants();
  int check_new_jobs(uint64_t tenant_id, bool is_oracle_tenant);
  int register_new_jobs(uint64_t tenant_id, bool is_oracle_tenant, ObIArray<ObDBMSSchedJobInfo> &job_infos);
  int register_job(ObDBMSSchedJobInfo &job_info, ObDBMSSchedJobKey *job_key = NULL, bool ignore_nextdate = false);

  int scheduler_job(ObDBMSSchedJobKey *job_key);

private:
  const static int MAX_READY_JOBS_CAPACITY = 1024 * 1024;
  const static int MIN_SCHEDULER_INTERVAL = 20 * 1000 * 1000;

  bool inited_;
  bool stoped_;
  bool running_;

  const uint64_t *trace_id_;

  common::ObRandom rand_; // for random pick server
  rootserver::ObUnitManager *unit_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_; // for got all tenant info
  obrpc::ObDBMSSchedJobRpcProxy *job_rpc_proxy_;

  common::ObAddr self_addr_;
  dbms_job::ObDBMSJobQueue ready_queue_;
  ObDBMSSchedJobTask scheduler_task_;
  ObDBMSSchedJobThread scheduler_thread_;
  ObDBMSSchedTableOperator table_operator_;

  common::ObSpinLock lock_;
  common::ObArenaAllocator allocator_;

  common::hash::ObHashSet<uint64_t> alive_jobs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSSchedJobMaster);
};

} //end for namespace dbms_scheduler
} //end for namespace oceanbase

#endif /* SRC_OBSERVER_OB_DBMS_SCHED_JOB_MASTER_H_ */
