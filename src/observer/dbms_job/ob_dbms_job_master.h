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

#ifndef SRC_OBSERVER_OB_DBMS_JOB_MASTER_H_
#define SRC_OBSERVER_OB_DBMS_JOB_MASTER_H_

#include "ob_dbms_job_rpc_proxy.h"
#include "ob_dbms_job_utils.h"

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

#include "rootserver/ob_ddl_service.h"


namespace oceanbase
{

namespace dbms_job
{
class ObDBMSJobThread : public ObSimpleThreadPool
{
  virtual void handle(void *task);
};


class ObDBMSJobKey : public common::ObLink
{
public:
  ObDBMSJobKey(
    uint64_t tenant_id, uint64_t job_id,
    uint64_t execute_at, uint64_t delay,
    bool check_job, bool check_new, bool check_new_tenant)
  : tenant_id_(tenant_id),
    job_id_(job_id),
    execute_at_(execute_at),
    delay_(delay),
    check_job_(check_job),
    check_new_(check_new),
    check_new_tenant_(check_new_tenant) {}

  virtual ~ObDBMSJobKey() {}

  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_job_id() const { return job_id_; }
  OB_INLINE uint64_t get_execute_at() const { return execute_at_;}
  OB_INLINE uint64_t get_delay() const { return delay_; }

  OB_INLINE bool is_check() { return check_job_ || check_new_ || check_new_tenant_; }
  OB_INLINE bool is_check_new() { return check_new_; }
  OB_INLINE bool is_check_new_tenant() { return check_new_tenant_; }

  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_job_id(uint64_t job_id) { job_id_ = job_id; }

  OB_INLINE void set_execute_at(uint64_t execute_at) { execute_at_ = execute_at; }
  OB_INLINE void set_delay(uint64_t delay) { delay_ = delay; }

  OB_INLINE void set_check_job(bool check_job) { check_job_ = check_job; }
  OB_INLINE void set_check_new(bool check_new) { check_new_ = check_new; }
  OB_INLINE void set_check_new_tenant(bool check_new) { check_new_tenant_ = check_new; }

  OB_INLINE uint64_t get_adjust_delay() const
  {
    uint64_t now = ObTimeUtility::current_time();
    return (execute_at_ < now) ? 0 : (execute_at_ - now);
  }

  OB_INLINE bool is_valid()
  {
    return job_id_ != OB_INVALID_ID && tenant_id_ != OB_INVALID_ID;
  }

  TO_STRING_KV(
    K_(check_job), K_(check_new), K_(check_new_tenant),
    K_(execute_at), K_(delay), K_(job_id), K_(tenant_id));

private:
  uint64_t tenant_id_;
  uint64_t job_id_; // for check_new, job_id is max job id in current tenant
  uint64_t execute_at_;
  uint64_t delay_;

  bool check_job_; // for check job update ...
  bool check_new_; // for check new job coming ...
  bool check_new_tenant_; // for check new tenant ...
};

class ObDBMSJobTask : public ObTimerTask
{
public:
  typedef common::ObSortedVector<ObDBMSJobKey *> WaitVector;
  typedef WaitVector::iterator WaitVectorIterator;

  ObDBMSJobTask()
    : inited_(false),
      job_key_(NULL),
      ready_queue_(NULL),
      wait_vector_(0, NULL, ObModIds::VECTOR),
      lock_(common::ObLatchIds::DBMS_JOB_TASK_LOCK) {}

  virtual ~ObDBMSJobTask() {}

  int init(ObDBMSJobQueue *ready_queue);
  int start();
  int stop();
  int destroy();

  void runTimerTask();

  int scheduler(ObDBMSJobKey *job_key);
  int add_new_job(ObDBMSJobKey *job_key);
  int immediately(ObDBMSJobKey *job_key);

  inline static bool compare_job_key(
    const ObDBMSJobKey *lhs, const ObDBMSJobKey *rhs);
  inline static bool equal_job_key(
    const ObDBMSJobKey *lhs, const ObDBMSJobKey *rhs);

private:
  bool inited_;
  ObDBMSJobKey *job_key_;
  ObDBMSJobQueue *ready_queue_;
  WaitVector wait_vector_;

  ObSpinLock lock_;
  ObTimer timer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSJobTask);
};

class ObDBMSJobMaster
{
public:
  ObDBMSJobMaster()
    : inited_(false),
      stoped_(false),
      running_(false),
      trace_id_(NULL),
      rand_(),
      schema_service_(NULL),
      job_rpc_proxy_(NULL),
      self_addr_(),
      ready_queue_(),
      scheduler_task_(),
      scheduler_thread_(),
      job_utils_(),
      lock_(common::ObLatchIds::DBMS_JOB_MASTER_LOCK),
      allocator_("DBMSJobMaster"),
      alive_jobs_() {}

  virtual ~ObDBMSJobMaster() { alive_jobs_.destroy(); };

  static ObDBMSJobMaster &get_instance();

  bool is_inited() { return inited_; }

  int init(common::ObISQLClient *sql_client,
           share::schema::ObMultiVersionSchemaService *schema_service);

  int start();
  int stop();
  int scheduler();
  int destroy();

  int alloc_job_key(
    ObDBMSJobKey *&job_key,
    uint64_t tenant_id, uint64_t job_id,
    uint64_t execute_at, uint64_t delay,
    bool check_job = false, bool check_new = false, bool check_new_tenant = false);

  int server_random_pick(int64_t tenant_id, common::ObString &pick_zone, ObAddr &server);
  int get_all_servers(int64_t tenant_id, ObString &pick_zone, ObIArray<ObAddr> &servers);
  int get_execute_addr(ObDBMSJobInfo &job_info, common::ObAddr &execute_addr);

  int register_check_tenant_job();
  int load_and_register_all_jobs(ObDBMSJobKey *job_key = NULL);
  int load_and_register_new_jobs(uint64_t tenant_id, ObDBMSJobKey *job_key = NULL);
  int register_jobs(
    uint64_t tenant_id, common::ObIArray<ObDBMSJobInfo> &job_infos, ObDBMSJobKey *job_key = NULL);
  int register_job(ObDBMSJobInfo &job_info, ObDBMSJobKey *job_key = NULL, bool ignore_nextdate = false);

  int scheduler_job(ObDBMSJobKey *job_key, bool is_retry = false);

private:
  const static int MAX_READY_JOBS_CAPACITY = (1 << 20);
  const static int MIN_SCHEDULER_INTERVAL = 5 * 1000 * 1000;

  bool inited_;
  bool stoped_;
  bool running_;

  const uint64_t *trace_id_;

  common::ObRandom rand_; // for random pick server
  share::schema::ObMultiVersionSchemaService *schema_service_; // for got all tenant info
  obrpc::ObDBMSJobRpcProxy *job_rpc_proxy_;

  common::ObAddr self_addr_;
  ObDBMSJobQueue ready_queue_;
  ObDBMSJobTask scheduler_task_;
  ObDBMSJobThread scheduler_thread_;
  ObDBMSJobUtils job_utils_;

  common::ObSpinLock lock_;
  common::ObArenaAllocator allocator_;

  common::hash::ObHashSet<uint64_t> alive_jobs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSJobMaster);
};

} //end for namespace dbms_job
} //end for namespace oceanbase

#endif /* SRC_OBSERVER_OB_DBMS_JOB_MASTER_H_ */
