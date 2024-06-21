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
    uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const common::ObString &job_name)
  : tenant_id_(tenant_id),
    is_oracle_tenant_(is_oracle_tenant),
    job_id_(job_id),
    job_name_() {
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
  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_job_id(uint64_t job_id) { job_id_ = job_id; }
  OB_INLINE void set_execute_at(uint64_t execute_at) { execute_at_ = execute_at; }
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
    K_(execute_at));

private:
  uint64_t tenant_id_;
  bool is_oracle_tenant_;
  uint64_t job_id_;
  char job_name_buf_[JOB_NAME_MAX_SIZE];
  common::ObString job_name_;
  uint64_t execute_at_;
};

class ObDBMSSchedJobTask
{
public:
  typedef common::ObSortedVector<ObDBMSSchedJobKey *> WaitVector;
  typedef WaitVector::iterator WaitVectorIterator;

  ObDBMSSchedJobTask()
    : inited_(false),
      allocator_(NULL),
      wait_vector_(0, NULL, ObModIds::VECTOR) {}

  virtual ~ObDBMSSchedJobTask() {}

  int init();
  int start(common::ObVSliceAlloc *allocator);
  int stop();
  int destroy();

  int scheduler(ObDBMSSchedJobKey *job_key);
  int add_new_job(ObDBMSSchedJobKey *job_key);
  WaitVector &wait_vector() { return wait_vector_; }

  inline static bool compare_job_key(
    const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs);
  inline static bool equal_job_key(
    const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs);

private:
  bool inited_;
  common::ObVSliceAlloc *allocator_;
  WaitVector wait_vector_;
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
      allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "DbmsScheduler"), OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_),
      alive_jobs_() {}

  virtual ~ObDBMSSchedJobMaster() { alive_jobs_.destroy(); };

  class JobIdByTenant
  {
  public:
    JobIdByTenant()
        : tenant_id_(OB_INVALID_TENANT_ID),
          job_id_(OB_INVALID_ID) {}
    JobIdByTenant(const int64_t tenant_id, const int64_t job_id)
      : tenant_id_(tenant_id),
        job_id_(job_id) {}
    ~JobIdByTenant() {}
    bool operator ==(const JobIdByTenant &other) const
    {
      return tenant_id_ == other.tenant_id_ && job_id_ == other.job_id_;
    }
    bool operator !=(const JobIdByTenant &other) const
    {
      return !(*this == other);
    }
    uint64_t hash() const
    {
      uint64_t hash_val = 0;

      hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
      hash_val = murmurhash(&job_id_, sizeof(job_id_), hash_val);

      return hash_val;
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
    void set_job_id(int64_t job_id) { job_id_ = job_id; }
    int64_t get_tenant_id() { return tenant_id_; }
    int64_t get_job_id() { return job_id_; }
    TO_STRING_KV(K_(tenant_id),
      K_(job_id));
  private:
    int64_t tenant_id_;
    int64_t job_id_;
  };

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
    uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const common::ObString &job_name);
  void free_job_key(ObDBMSSchedJobKey *&job_key);

  int server_random_pick_from_zone_list(int64_t tenant_id, common::ObIArray<common::ObZone> &zone_list, ObAddr &server);
  int get_execute_addr(ObDBMSSchedJobInfo &job_info, common::ObAddr &execute_addr);

  int check_all_tenants();
  int check_new_jobs(uint64_t tenant_id, bool is_oracle_tenant);
  int register_new_jobs(uint64_t tenant_id, bool is_oracle_tenant, ObIArray<ObDBMSSchedJobInfo> &job_infos);
  int register_job(ObDBMSSchedJobKey *job_key, int64_t next_date);
  int scheduler_job(ObDBMSSchedJobKey *job_key);
  int64_t calc_next_date(ObDBMSSchedJobInfo &job_info);
  int64_t run_job(ObDBMSSchedJobInfo &job_info, ObDBMSSchedJobKey *job_key, int64_t next_date);

private:
  const static int MAX_READY_JOBS_CAPACITY = 1024 * 1024;
  const static int MIN_SCHEDULER_INTERVAL = 1 * 1000 * 1000;
  const static int CHECK_NEW_INTERVAL = 20 * 1000 * 1000;
  const static int DEFAULT_ZONE_SIZE = 4;
  const static int FILTER_ZONE_SIZE = 1;
  const static int DEFALUT_SERVER_SIZE = 16;

  bool inited_;
  bool stoped_;
  bool running_;

  const uint64_t *trace_id_;

  common::ObRandom rand_; // for random pick server
  rootserver::ObUnitManager *unit_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_; // for got all tenant info
  obrpc::ObDBMSSchedJobRpcProxy *job_rpc_proxy_;

  common::ObAddr self_addr_;
  ObDBMSSchedJobTask scheduler_task_;
  ObDBMSSchedJobThread scheduler_thread_;
  ObDBMSSchedTableOperator table_operator_;

  common::ObBlockAllocMgr block_alloc_;
  common::ObVSliceAlloc allocator_;

  common::hash::ObHashSet<JobIdByTenant> alive_jobs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSSchedJobMaster);
};

} //end for namespace dbms_scheduler
} //end for namespace oceanbase

#endif /* SRC_OBSERVER_OB_DBMS_SCHED_JOB_MASTER_H_ */
