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

#define USING_LOG_PREFIX RS

#include "ob_dbms_sched_job_master.h"

#include "lib/time/Time.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/container/ob_array.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/profile/ob_trace_id.h"

#include "share/partition_table/ob_partition_location.h"
#include "share/ob_all_server_tracer.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace share;
using namespace share::schema;
using namespace rootserver;
using namespace obutil;
using namespace obrpc;

namespace dbms_scheduler
{

int ObDBMSSchedJobTask::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(inited_));
  } else if (OB_FAIL(timer_.init())) {
    LOG_WARN("fail to init timer", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObDBMSSchedJobTask::start(dbms_job::ObDBMSJobQueue *ready_queue)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job task not inited", K(ret), K(inited_));
  } else if (OB_ISNULL(ready_queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(ready_queue));
  }
  ready_queue_ = ready_queue;
  OZ (timer_.start());
  return ret;
}

int ObDBMSSchedJobTask::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job task not inited", K(ret), K(inited_));
  } else {
    timer_.cancel(*this);
    timer_.stop();
    timer_.wait();
    wait_vector_.clear();
    job_key_ = NULL;
    ready_queue_ = NULL;
  }
  return ret;
}

int ObDBMSSchedJobTask::destroy()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("scheduler task not inited", K(ret), K(inited_));
  } else {
    timer_.destroy();
  }
  return ret;
}

void ObDBMSSchedJobTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job task not init", K(ret), K(inited_));
  } else if (OB_ISNULL(job_key_)
          || OB_ISNULL(ready_queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(job_key_), K(ready_queue_));
  } else if (OB_FAIL(ready_queue_->push(job_key_, 0))) {
    LOG_WARN("fail to push ready job to queue", K(ret), K(*job_key_));
  } else {
    job_key_ = NULL;
    if (wait_vector_.count() > 0) {
      job_key_ = wait_vector_[0];
      if (OB_FAIL(wait_vector_.remove(wait_vector_.begin()))) {
        job_key_ = NULL;
        LOG_WARN("fail to remove job_id from sorted vector", K(ret));
      } else if (OB_ISNULL(job_key_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(job_key_));
      } else if (OB_FAIL(timer_.schedule(*this, job_key_->get_adjust_delay()))) {
        LOG_WARN("fail to schedule task", K(ret), K(*job_key_));
      }
    }
  }
  LOG_DEBUG("JobKEYS INFO HEADER ==== ", KPC(job_key_), K(wait_vector_.count()));
  int i = 0;
  for (WaitVectorIterator iter = wait_vector_.begin();
          OB_SUCC(ret) && iter != wait_vector_.end(); ++iter, ++i) {
    ObDBMSSchedJobKey *job = *iter;
    LOG_DEBUG("JobKEYS INFO ELEMENT ====", K(i), KPC(job));
  }
  return;
}

int ObDBMSSchedJobTask::scheduler(ObDBMSSchedJobKey *job_key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job task not init", K(ret));
  } else if (OB_ISNULL(job_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr for job id", K(ret), KPC(job_key));
  } else if (!job_key->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job id is invalid", K(ret), KPC(job_key));
  } else if (0 == job_key->get_delay()) {
    OZ (immediately(job_key), KPC(job_key));
  } else {
    OZ (add_new_job(job_key), KPC(job_key));
  }

  return ret;
}

int ObDBMSSchedJobTask::add_new_job(ObDBMSSchedJobKey *new_job_key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job not init", K(ret));
  } else if (OB_ISNULL(new_job_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), KPC(new_job_key));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_ISNULL(job_key_)) {
      job_key_ = new_job_key;
      OZ (timer_.schedule(*this, job_key_->get_delay()));
    } else if (new_job_key->get_execute_at() >= job_key_->get_execute_at()) {
      WaitVectorIterator iter;
      ObDBMSSchedJobKey *replace_job_key = NULL;
      OZ (wait_vector_.replace(new_job_key, iter, compare_job_key, equal_job_key, replace_job_key));
    } else {
      WaitVectorIterator iter;
      OX (timer_.cancel(*this));
      OZ (wait_vector_.insert(job_key_, iter, compare_job_key));
      OX (job_key_ = new_job_key);
      OZ (timer_.schedule(*this, job_key_->get_delay()));
    }
  }
  return ret;
}

int ObDBMSSchedJobTask::immediately(ObDBMSSchedJobKey *job_key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job not init", K(ret), K(inited_));
  } else if (OB_ISNULL(job_key) || OB_ISNULL(ready_queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(job_key), K(ready_queue_));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(ready_queue_->push(job_key, 0))) {
      LOG_WARN("fail to push ready job to queue", K(ret), K(*job_key));
    }
  }
  return ret;
}

bool ObDBMSSchedJobTask::compare_job_key(const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs)
{
  return lhs->get_execute_at() < rhs->get_execute_at()
    || (lhs->get_execute_at() == rhs->get_execute_at() && lhs->get_job_id() < rhs->get_job_id())
    || (lhs->get_execute_at() == rhs->get_execute_at() && lhs->get_job_id() == rhs->get_job_id() &&
        lhs->get_tenant_id() < rhs->get_tenant_id());
}

bool ObDBMSSchedJobTask::equal_job_key(const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id() &&
         lhs->get_job_id() == rhs->get_job_id() &&
         lhs->get_execute_at() == rhs->get_execute_at();
}

void ObDBMSSchedJobThread::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobMaster *master = NULL;
  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(task));
  } else if (OB_ISNULL(master = static_cast<ObDBMSSchedJobMaster *>(task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("master is null", K(ret), K(master), K(task));
  } else if (OB_FAIL(master->scheduler())) {
    LOG_ERROR("fail to run dbms sched job master", K(ret));
  }
  return;
}

ObDBMSSchedJobMaster &ObDBMSSchedJobMaster::get_instance()
{
  static ObDBMSSchedJobMaster master_;
  return master_;
}

int ObDBMSSchedJobMaster::init(ObUnitManager *unit_mgr,
                          ObISQLClient *sql_client,
                          ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dbms sched job master already inited", K(ret), K(inited_));
  } else if (OB_ISNULL(unit_mgr)
          || OB_ISNULL(sql_client)
          || OB_ISNULL(schema_service)
          || OB_ISNULL(GCTX.dbms_sched_job_rpc_proxy_)
          ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(unit_mgr), K(sql_client), K(schema_service));
  } else if (FALSE_IT(ready_queue_.set_limit(MAX_READY_JOBS_CAPACITY))) {
    // do-nothing
  } else if (OB_FAIL(scheduler_task_.init())) {
    LOG_WARN("fail to init ready queue", K(ret));
  } else if (OB_FAIL(scheduler_thread_.init(1, 1))) {
    LOG_WARN("fail to init scheduler pool", K(ret));
  } else if (OB_FAIL(table_operator_.init(sql_client))) {
    LOG_WARN("fail to init action record", K(ret));
  } else if (OB_FAIL(alive_jobs_.create(1024))) {
    LOG_WARN("failed to create job hash set", K(ret));
  } else if (OB_ISNULL(ObCurTraceId::get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trace id is null", K(ret));
  } else {
    trace_id_ = ObCurTraceId::get();
    self_addr_ = GCONF.self_addr_;
    unit_mgr_ = unit_mgr;
    schema_service_ = schema_service;
    job_rpc_proxy_ = GCTX.dbms_sched_job_rpc_proxy_;
    inited_ = true;
  }
  LOG_INFO("dbms sched job master inited!", K(ret));
  return ret;
}

int ObDBMSSchedJobMaster::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet!", K(ret), K(inited_));
  } else if (running_) {
    // alreay running , do nothing ...
  } else if (OB_FAIL(scheduler_thread_.push(static_cast<void *>(this)))) {
    LOG_WARN("fail to start scheduler thread", K(ret));
  } else if (OB_FAIL(scheduler_task_.start(&ready_queue_))) {
    LOG_WARN("fail to start ready queue", K(ret));
  }
  LOG_INFO("dbms sched job master started", K(ret));
  return ret;
}

int ObDBMSSchedJobMaster::stop()
{
  int ret = OB_SUCCESS;
  stoped_ = true;
  while (running_) {
    sleep(1);
  }
  scheduler_task_.stop();
  ready_queue_.clear();
  alive_jobs_.clear();
  stoped_ = false;
  LOG_INFO("dbms sched job master stoped", K(ret));
  return ret;
}

int ObDBMSSchedJobMaster::scheduler()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init yet", K(ret));
  } else if (OB_ISNULL(trace_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null trace_id_ ptr", K(ret), K(trace_id_));
  } else {
    ObCurTraceId::set(trace_id_);
    running_ = true;
    LOG_INFO("NOTICE: DBMS Sched Job master start running!", K(ret), K(running_));
    lib::set_thread_name("DBMS_SCHEDULER");
    while (OB_SUCC(ret) && !stoped_) {
      ObLink* ptr = NULL;
      int64_t timeout = MIN_SCHEDULER_INTERVAL;
      ObDBMSSchedJobKey *job_key = NULL;
      if (REACH_TIME_INTERVAL(MIN_SCHEDULER_INTERVAL)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = check_all_tenants())) {
          LOG_WARN("fail to check all tenants", K(tmp_ret));
        }
      }
      if (OB_FAIL(ready_queue_.pop(ptr, timeout))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("dbms sched job master wait timeout, no entry", K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("fail to pop dbms sched job ready queue", K(ret), K(timeout));
        }
      } else if (OB_ISNULL(job_key = static_cast<ObDBMSSchedJobKey *>(ptr)) || !job_key->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected error, invalid job key found in ready queue!", K(ret), KPC(job_key));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = scheduler_job(job_key))) {
          LOG_WARN("fail to scheduler single dbms sched job", K(ret), K(tmp_ret), KPC(job_key));
        } else {
          LOG_INFO("success to scheduler single dbms sched job", K(ret), K(tmp_ret), KPC(job_key));
        }
      }
    }
    LOG_INFO("NOTICE: DBMS Sched Job master end running!", K(ret), K(running_));
    running_ = false;
  }
  return ret;
}

int ObDBMSSchedJobMaster::scheduler_job(ObDBMSSchedJobKey *job_key)
{
  int ret = OB_SUCCESS;

  ObAddr execute_addr;
  ObDBMSSchedJobInfo job_info;
  bool can_running = false;

  CK (OB_LIKELY(inited_));
  CK (OB_NOT_NULL(job_key));
  CK (OB_LIKELY(job_key->is_valid()));

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to scheduler job", K(ret), KPC(job_key));
  } else {
    ObArenaAllocator allocator;
    OZ (table_operator_.get_dbms_sched_job_info(
      job_key->get_tenant_id(), job_key->is_oracle_tenant(), job_key->get_job_id(), job_key->get_job_name(), allocator, job_info));

    if (OB_FAIL(ret) || !job_info.valid()) {
      int tmp = alive_jobs_.erase_refactored(job_key->get_job_id_with_tenant());
      if (tmp != OB_SUCCESS) {
        LOG_ERROR("failed delete invalid job from hash set", K(tmp), K(ret), K(job_info), KPC(job_key));
      } else {
        LOG_INFO("delete invalid job from hash set", K(tmp), K(ret), K(job_info), KPC(job_key));
      }
      allocator_.free(job_key); // sql proxy error
    } else{
      bool ignore_nextdate = false;
      if (!job_key->is_check() && !job_info.is_running() && !job_info.is_broken() && !job_info.is_disabled()) {
        bool can_running = false;
        OZ (table_operator_.check_job_can_running(job_info.get_tenant_id(), can_running));
        if (OB_SUCC(ret) && can_running) {
          OZ (get_execute_addr(job_info, execute_addr));
          OZ (table_operator_.update_for_start(
            job_info.get_tenant_id(), job_info));
          OZ (job_rpc_proxy_->run_dbms_sched_job(
            job_key->get_tenant_id(), job_key->is_oracle_tenant(), job_key->get_job_id(), job_key->get_job_name(), execute_addr, self_addr_));
        } else {
          LOG_INFO("avoid duplicate job", K(ret), K(job_info), K(can_running));
        }
        ignore_nextdate = true;
      }
      int tmp_ret = OB_SUCCESS;
      // always add job to queue. we need this to check job status changes.
      if (OB_SUCCESS != (tmp_ret = register_job(job_info, job_key, ignore_nextdate))) {
        LOG_WARN("failed to register job to job queue", K(tmp_ret), K(job_info));
        int tmp = alive_jobs_.erase_refactored(job_info.get_job_id_with_tenant());
        if (tmp != OB_SUCCESS) {
          LOG_ERROR("failed delete invalid job from hash set", K(tmp), K(job_info));
        } else {
          LOG_WARN("delete register failed job from hash set", K(job_info));
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::destroy()
{
  ready_queue_.destroy();
  scheduler_task_.destroy();
  scheduler_thread_.destroy();
  allocator_.clear();
  return OB_SUCCESS;
}

int ObDBMSSchedJobMaster::alloc_job_key(
  ObDBMSSchedJobKey *&job_key,
  uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const ObString &job_name,
  uint64_t execute_at, uint64_t delay,
  bool check_job)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  void *ptr = NULL;
  job_key = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObDBMSSchedJobKey)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(ptr));
  } else if (OB_ISNULL(job_key =
    new(ptr)ObDBMSSchedJobKey(tenant_id, is_oracle_tenant, job_id, job_name,
                         execute_at, delay,
                         check_job))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init scheduler job id", K(ret), K(tenant_id));
  }
  return ret;
}

int ObDBMSSchedJobMaster::get_execute_addr(ObDBMSSchedJobInfo &job_info, ObAddr &execute_addr)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret), K(inited_));
  } else if (!job_info.valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dbms sched job info is invalid", K(ret), K(job_info));
  }
  OZ (server_random_pick(job_info.get_tenant_id(), job_info.get_zone(), execute_addr));
  return ret;
}

int ObDBMSSchedJobMaster::server_random_pick(int64_t tenant_id, ObString &pick_zone, ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_info = NULL;
  common::ObArray<common::ObZone> zone_list;
  common::ObArray<ObAddr> total_server;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret), K(inited_));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ or unit_mgr_ is null", KR(ret), KP(schema_service_), KP(unit_mgr_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), KP(tenant_info));
  } else if (OB_FAIL(tenant_info->get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      common::ObZone zone = zone_list.at(i);
      common::ObArray<ObAddr> server_list;
      if (pick_zone.empty() || 0 == pick_zone.case_compare(zone.str())) {
        if (OB_FAIL(SVR_TRACER.get_alive_servers(zone, server_list))) {
          LOG_WARN("fail to get zone server list", KR(ret), K(zone));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); j++) {
            if (OB_FAIL(total_server.push_back(server_list.at(j)))) {
              LOG_WARN("fail to push server to total", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && 0 == total_server.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not find an alive server", K(ret), K(total_server), K(pick_zone), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    ObAddr pick;
    bool is_alive = false;
    bool is_active = false;
    bool on_server = false;
    int64_t pos = rand_.get(0,255) % total_server.count();
    int64_t cnt = 0;
    do {
      pos = (pos + 1) % total_server.count();
      pick = total_server.at(pos);
      if (OB_FAIL(SVR_TRACER.check_server_alive(pick, is_alive))) {
        LOG_WARN("fail to check server alive", KR(ret), K(pick));
      } else if (OB_FAIL(SVR_TRACER.check_server_active(pick, is_active))) {
        LOG_WARN("fail to check server active", KR(ret), K(pick));
      } else if (OB_FAIL(unit_mgr_->check_tenant_on_server(tenant_id, pick, on_server))) {
        LOG_WARN("fail to check tenant on server", KR(ret), K(tenant_id), K(pick));
      } else {
        if (is_alive && is_active && on_server) {
          break;
        }
        cnt++;
      }
    } while (cnt < total_server.count());
    if (cnt >= total_server.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find a alive server", K(ret), K(cnt), K(total_server.count()));
    } else {
      LOG_INFO("get a addr for execute", K(ret), K(tenant_id), K(pick));
      server = pick;
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::check_all_tenants()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSEArray<uint64_t, 32> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job not init yet", K(ret), K(inited_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get all tenant ids", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_ids.count(); ++i) {
      const ObTenantSchema *tenant_schema = NULL;
      OZ (schema_guard.get_tenant_info(tenant_ids.at(i), tenant_schema));
      CK (OB_NOT_NULL(tenant_schema));
      if (OB_SUCC(ret)) {
        uint64_t data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_ids.at(i), data_version))) {
          LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
        } else if (MOCK_DATA_VERSION <= data_version) {
          //add default job class
          OZ (table_operator_.register_default_job_class(tenant_ids.at(i)));
          OZ (table_operator_.purge_run_detail_histroy(tenant_ids.at(i)));
        }
        OZ (check_new_jobs(tenant_ids.at(i), tenant_schema->is_oracle_tenant()));
      }
      ret = OB_SUCCESS; // one tenant failed should not affect other
    }
  }
  LOG_INFO("check all tenants", K(ret));
  return ret;
}

int ObDBMSSchedJobMaster::check_new_jobs(uint64_t tenant_id, bool is_oracle_tenant)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDBMSSchedJobInfo, 16> job_infos;
  ObArenaAllocator allocator;
  OZ (table_operator_.get_dbms_sched_job_infos_in_tenant(tenant_id, is_oracle_tenant, allocator, job_infos));
  OZ (register_new_jobs(tenant_id, is_oracle_tenant, job_infos));
  LOG_INFO("check new jobs", K(ret), K(tenant_id), K(is_oracle_tenant), K(job_infos));
  return ret;
}

int ObDBMSSchedJobMaster::register_new_jobs(uint64_t tenant_id, bool is_oracle_tenant, ObIArray<ObDBMSSchedJobInfo> &job_infos)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobInfo job_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < job_infos.count(); i++) {
    job_info = job_infos.at(i);
    if (job_info.valid()) {
      int tmp = alive_jobs_.exist_refactored(job_info.get_job_id_with_tenant());
      if (OB_HASH_EXIST == tmp) {
        // do nothing ...
      } else if (OB_HASH_NOT_EXIST) {
        OZ (register_job(job_info));
        OZ (alive_jobs_.set_refactored(job_info.get_job_id_with_tenant()));
        LOG_INFO("register new job", K(ret), K(tenant_id), K(job_info));
      } else {
        LOG_ERROR("dbms sched job master check job exist failed", K(ret), K(job_info));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::register_job(
  ObDBMSSchedJobInfo &job_info, ObDBMSSchedJobKey *job_key, bool ignore_nextdate)
{
  int ret = OB_SUCCESS;

  int64_t execute_at = -1;
  int64_t delay = -1;
  bool check_job = false;
  int64_t now = ObTimeUtility::current_time();

  CK (OB_LIKELY(inited_));
  CK (job_info.valid());
  OZ (table_operator_.check_job_timeout(job_info));
  OZ (table_operator_.check_auto_drop(job_info));
  if (OB_FAIL(ret)) {
  } else if (job_info.is_broken() || job_info.is_disabled()) {
    execute_at = now + MIN_SCHEDULER_INTERVAL;
    delay = MIN_SCHEDULER_INTERVAL; // every MIN_SCHEDULER_INTERVAL check job status
    check_job = true;
  } else {
    OZ (table_operator_.calc_execute_at(job_info, execute_at, delay, ignore_nextdate));
    if (OB_FAIL(ret) || delay < 0) {
      ret = OB_SUCCESS;
      execute_at = now + MIN_SCHEDULER_INTERVAL;
      delay = MIN_SCHEDULER_INTERVAL;
      check_job = true;
    } else if (delay > MIN_SCHEDULER_INTERVAL) {
      // job may run later, but we need check job update.
      execute_at = now + MIN_SCHEDULER_INTERVAL;
      delay = MIN_SCHEDULER_INTERVAL;
      check_job = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(job_key)) {
    OZ (alloc_job_key(
      job_key,
      job_info.get_tenant_id(),
      job_info.is_oracle_tenant(),
      job_info.get_job_id(),
      job_info.get_job_name(),
      execute_at,
      delay,
      check_job));
    CK (OB_NOT_NULL(job_key));
    CK (job_key->is_valid());
  } else {
    CK (job_key->get_tenant_id() == job_info.get_tenant_id());
    CK (job_key->get_job_id() == job_info.get_job_id());
    CK (job_key->get_job_name() == job_info.get_job_name());
    OX (job_key->set_execute_at(execute_at));
    OX (job_key->set_delay(delay));
    OX (job_key->set_check_job(check_job));
  }
  OZ (scheduler_task_.scheduler(job_key));
  if (OB_FAIL(ret) && OB_NOT_NULL(job_key)) {
    allocator_.free(job_key);
  }
  LOG_INFO("register dbms sched job", K(ret), K(job_info), KPC(job_key), K(ignore_nextdate));

  return ret;
}

} // end for namespace dbms_scheduler
} // end for namespace oceanbase
