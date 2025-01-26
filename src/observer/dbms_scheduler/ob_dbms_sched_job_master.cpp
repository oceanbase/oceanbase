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

#define USING_LOG_PREFIX SERVER

#include "ob_dbms_sched_job_master.h"
#include "share/ob_primary_zone_util.h"//ObPrimaryZoneUtil

#include "rootserver/ob_root_service.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#define TO_TS(second) (1000000L * second)
#define IS_SPLIT_TENANT_DATA_VERSION ((GET_MIN_CLUSTER_VERSION() > MOCK_CLUSTER_VERSION_4_2_5_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_4_0))
namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace share;
using namespace share::schema;
using namespace rootserver;
using namespace obutil;
using namespace obrpc;
using namespace storage;

namespace dbms_scheduler
{

int ObDBMSSchedJobMaster::init(common::ObMySQLProxy *sql_proxy,
                          ObMultiVersionSchemaService *schema_service,
                          uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dbms sched job master already inited", K(ret), K(inited_));
  } else if (OB_ISNULL(sql_proxy)
          || OB_ISNULL(schema_service)
          || OB_ISNULL(GCTX.dbms_sched_job_rpc_proxy_)
          ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(sql_proxy), K(schema_service));
  } else if (OB_FAIL(unit_operator_.init(*sql_proxy))) {
      LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(table_operator_.init(sql_proxy))) {
    LOG_WARN("fail to init action record", K(ret));
  } else if (OB_FAIL(alive_jobs_.create(1024, ObMemAttr(tenant_id, "DbmsSched_Job")))) {
    LOG_WARN("failed to create job hash set", K(ret));
  } else if (OB_ISNULL(ObCurTraceId::get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trace id is null", K(ret));
  } else {
    tenant_server_cache_.reset();
    self_addr_ = GCONF.self_addr_;
    schema_service_ = schema_service;
    job_rpc_proxy_ = GCTX.dbms_sched_job_rpc_proxy_;
    tenant_id_ = tenant_id;
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
  } else {
    stoped_ = false;
  }
  LOG_INFO("dbms sched job master started", K(ret));
  return ret;
}

int ObDBMSSchedJobMaster::stop()
{
  int ret = OB_SUCCESS;
  stoped_ = true;
  LOG_INFO("dbms sched job master begin stop", K(ret));
  return ret;
}

void ObDBMSSchedJobMaster::switch_to_leader()
{
  is_leader_ = true;
}
void ObDBMSSchedJobMaster::switch_to_follower()
{
  is_leader_ = false;
}

int64_t ObDBMSSchedJobMaster::calc_next_date(ObDBMSSchedJobInfo &job_info)
{
  int64_t ret = 0;
  int64_t next_date = 0;
  if (job_info.is_mview_job()
      && !job_info.get_interval().empty()
      && (0 != job_info.get_interval().case_compare("null"))) {
    int64_t next_date_ts = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObMViewSchedJobUtils::calc_date_expression(job_info, next_date_ts))) {
      LOG_WARN("failed to calc date expression", KR(ret), K(job_info));
      // error code is ignored
      next_date = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
    } else {
      next_date = next_date_ts;
    }
  } else if (OB_FAIL(ObDBMSSchedJobUtils::calc_dbms_sched_repeat_expr(job_info, next_date))) {
    next_date = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
    LOG_WARN("failed to calc next date", KR(ret), K(job_info));
  }
  return next_date;
}

int64_t ObDBMSSchedJobMaster::run_job(ObDBMSSchedJobInfo &job_info, ObDBMSSchedJobKey *job_key, int64_t next_date)
{
  int ret = OB_SUCCESS;
  ObAddr execute_addr;
  if (OB_FAIL((get_execute_addr(job_info, execute_addr)))) {
    LOG_WARN("failed to get execute addr, retry soon", K(ret), K(job_info));
  } else if (ObTimeUtility::current_time() > job_info.get_end_date()) {
    LOG_INFO("job reach end date, not running", K(job_info));
  } else if (OB_FAIL(table_operator_.update_for_start(job_info.get_tenant_id(), job_info, next_date))) {
    LOG_WARN("failed to update for start", K(ret), K(job_info), KPC(job_key));
  } else if (OB_FAIL(job_rpc_proxy_->run_dbms_sched_job(job_key->get_tenant_id(),
      job_key->is_oracle_tenant(),
      job_key->get_job_id(),
      job_key->get_job_name(),
      execute_addr,
      self_addr_,
      job_info.is_olap_async_job() ? share::OBCG_OLAP_ASYNC_JOB : share::OBCG_DBMS_SCHED_JOB))) {
    LOG_WARN("failed to run dbms sched job", K(ret), K(job_info), KPC(job_key));
    if (is_server_down_error(ret)) {
      int tmp = OB_SUCCESS;
      if (OB_SUCCESS != (tmp = table_operator_.update_for_rollback(job_info))) {
        LOG_WARN("update for end failed for send rpc failed job", K(tmp), K(job_info), KPC(job_key));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::scheduler()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init yet", K(ret));
  } else {
    while (OB_SUCC(ret) && !stoped_) {
      bool is_leader = is_leader_;
      ObLink* ptr = NULL;
      ObDBMSSchedJobKey *job_key = NULL;
      int tmp_ret = OB_SUCCESS;
      if (!IS_SPLIT_TENANT_DATA_VERSION) {
        ob_usleep(MIN_SCHEDULER_INTERVAL, true);
      } else {
        if (is_leader && TC_REACH_TIME_INTERVAL(CHECK_NEW_INTERVAL)) {
          if (OB_SUCCESS != (tmp_ret = check_tenant())) {
            LOG_WARN("fail to check tenant", K(tmp_ret));
          }
        }
        if (is_leader && TC_REACH_TIME_INTERVAL(UPDATE_SERVER_CACHE_INTERVAL)) {
          if (OB_SUCCESS != (tmp_ret = update_tenant_server_cache())) {
            LOG_WARN("fail to update server", K(tmp_ret));
          }
        }
        if (is_leader && TC_REACH_TIME_INTERVAL(PURGE_RUN_DETAIL_INTERVAL)) {
          if (OB_SUCCESS != (tmp_ret = purge_run_detail())) {
            LOG_WARN("fail to purge run detail", K(tmp_ret));
          }
        }
        if (!is_leader) {
          clear_wait_vector();
          alive_jobs_.clear();
        }

        if (wait_vector_.count() == 0) {
          ob_usleep(MIN_SCHEDULER_INTERVAL, true);
        } else if (OB_ISNULL(job_key = wait_vector_[0]) || !job_key->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected error, invalid job key found in ready queue!", K(ret), KPC(job_key));
        } else {
          int64_t delay = job_key->get_execute_at() - ObTimeUtility::current_time();
          if (delay > MIN_SCHEDULER_INTERVAL) {
            ob_usleep(MIN_SCHEDULER_INTERVAL, true);
          } else {
            ob_usleep(max(0, delay), true);
            common::ObCurTraceId::TraceId job_trace_id;
            job_trace_id.init(GCONF.self_addr_);
            ObTraceIdGuard trace_id_guard(job_trace_id);
            if (OB_SUCCESS != (tmp_ret = wait_vector_.remove(wait_vector_.begin()))) {
              LOG_WARN("fail to remove job_id from sorted vector", K(ret));
            } else if (OB_SUCCESS != (tmp_ret = scheduler_job(job_key))) {
              LOG_WARN("fail to scheduler single dbms sched job", K(ret), K(tmp_ret));
            } else {
              LOG_INFO("success to scheduler single dbms sched job", K(ret), K(tmp_ret));
            }
          }
        }
      }
    }
    clear_wait_vector();
    alive_jobs_.clear();
    LOG_INFO("dbms sched job master stoped", K(ret));
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
    ObArenaAllocator allocator("DBMSSchedTmp");
    OZ (table_operator_.get_dbms_sched_job_info(
      job_key->get_tenant_id(), job_key->is_oracle_tenant(), job_key->get_job_id(), job_key->get_job_name(), allocator, job_info));

    const int64_t now = ObTimeUtility::current_time();
    int64_t next_check_date = now + MIN_SCHEDULER_INTERVAL;
    if (OB_FAIL(ret) || !job_info.valid()) {
      free_job_key(job_key);
      job_key = NULL;
      LOG_INFO("free invalid job", K(job_info));
    } else if (job_info.is_running()) {
      LOG_INFO("job is running now, retry later", K(job_info));
      if (now > job_info.get_this_date() + TO_TS(job_info.get_max_run_duration())) {
        if (OB_FAIL(table_operator_.update_for_timeout(job_info))) {
          LOG_WARN("update for end failed for timeout job", K(ret));
        } else {
          LOG_WARN("job is timeout, force update for end", K(job_info), K(now));
        }
      }
    } else if (job_info.is_killed()) {
      free_job_key(job_key);
      job_key = NULL;
      int tmp = OB_SUCCESS;
      if (OB_SUCCESS != (tmp = table_operator_.update_for_kill(job_info))) {
        LOG_WARN("update for stop failed", K(tmp), K(job_info));
      } else {
        LOG_WARN("update for stop job", K(job_info));
      }
    } else if (job_info.is_disabled() || job_info.is_broken()) {
      free_job_key(job_key);
      job_key = NULL;
      LOG_INFO("free disable/broken job", K(job_info));
    } else if (now > job_info.get_end_date()) {
      int tmp = OB_SUCCESS;
      if (OB_SUCCESS != (tmp = table_operator_.update_for_enddate(job_info))) {
        LOG_WARN("update for end failed for auto drop job", K(tmp), K(job_info));
      } else {
        LOG_WARN("update for end for expired job", K(job_info), K(now));
      }
      free_job_key(job_key);
      job_key = NULL;
      LOG_INFO("free enddate job", K(job_info));
    } else if (now < job_info.get_next_date()) {
        next_check_date = min(job_info.get_next_date(), now + CHECK_NEW_INTERVAL);
    } else {
      bool can_running = false;
      if (OB_FAIL(table_operator_.check_job_can_running(job_info.get_tenant_id(), alive_jobs_.size(), can_running))) {
        LOG_WARN("failed to check job can running, retry later", K(ret));
      } else if (!can_running) {
        LOG_INFO("job concurrency reach limit, retry later", K(ret), K(job_info), K(can_running));
      } else if (now > job_info.get_next_date() + TO_TS(job_info.get_max_run_duration())) {
        LOG_WARN("job maybe missed, ignore it", K(now), K(job_info));
        int64_t new_next_date = calc_next_date(job_info);
        int tmp = OB_SUCCESS;
        if (OB_SUCCESS != (tmp = table_operator_.update_for_missed(job_info))) {
          LOG_WARN("update for end failed for missed job", K(tmp));
        } else if (OB_SUCCESS != (tmp = table_operator_.update_next_date(job_info.get_tenant_id(), job_info, new_next_date))){
          LOG_WARN("update next date failed", K(tmp), K(job_info));
        } else {
          next_check_date = min(new_next_date, now + CHECK_NEW_INTERVAL);
        }
      } else {
        int64_t new_next_date = calc_next_date(job_info);
        if (OB_FAIL(run_job(job_info, job_key, new_next_date))) {
          LOG_WARN("failed to run job", K(ret), K(job_info), KPC(job_key));
        } else {
          next_check_date = min(new_next_date, now + CHECK_NEW_INTERVAL);
          next_check_date = min(next_check_date, now + TO_TS(job_info.get_max_run_duration()));
        }
      }
    }
    int tmp = OB_SUCCESS;
    if (OB_NOT_NULL(job_key) && OB_SUCCESS != (tmp = register_job(job_key, next_check_date))) {
      LOG_WARN("failed to register job", K(tmp), K(job_info));
      free_job_key(job_key);
      job_key = NULL;
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::destroy()
{
  allocator_.destroy();
  inited_ = false;
  stoped_ = true;
  is_leader_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  return OB_SUCCESS;
}

int ObDBMSSchedJobMaster::alloc_job_key(
  ObDBMSSchedJobKey *&job_key,
  uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const ObString &job_name)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  job_key = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObDBMSSchedJobKey)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(ptr));
  } else if (OB_ISNULL(job_key =
    new(ptr)ObDBMSSchedJobKey(tenant_id, is_oracle_tenant, job_id, job_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init scheduler job id", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(alive_jobs_.set_refactored(job_id))) {
      LOG_WARN("faile to add job to alive_jobs", K(ret), K(tenant_id), K(job_id));
      allocator_.free(job_key);
      job_key = NULL;
    }
  }
  return ret;
}

void ObDBMSSchedJobMaster::free_job_key(ObDBMSSchedJobKey *&job_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(job_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job_key is null", K(ret));
  } else {
    OZ (alive_jobs_.erase_refactored(job_key->get_job_id()));
    allocator_.free(job_key);
    job_key = NULL;
  }
}

int ObDBMSSchedJobMaster::get_execute_addr(ObDBMSSchedJobInfo &job_info, ObAddr &execute_addr)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = job_info.get_tenant_id();
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_info = NULL;
  common::ObSEArray<common::ObZone, DEFAULT_ZONE_SIZE> all_zone_list;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret), K(inited_));
  } else if (!job_info.valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dbms sched job info is invalid", K(ret), K(job_info));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ is null", KR(ret), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), KP(tenant_info));
  } else if (OB_FAIL(tenant_info->get_zone_list(all_zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(job_info.get_zone())) {
      ObSEArray<ObZone, DEFAULT_ZONE_SIZE> primary_zone_list;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(*tenant_info, primary_zone_list))) {
        LOG_WARN("failed to get tenant primary zone array", KR(tmp_ret));
      } else if (OB_TMP_FAIL(server_random_pick_from_zone_list(tenant_id, primary_zone_list, execute_addr))) {
        LOG_WARN("get execute addr from primary zone failed",
            KR(tmp_ret), K(tenant_id), K(primary_zone_list));
      }
      if (tmp_ret != OB_SUCCESS) {
        if (OB_FAIL(server_random_pick_from_zone_list(tenant_id, all_zone_list, execute_addr))) {
          LOG_WARN("get execute addr from all zone failed",
              KR(ret), K(tenant_id), K(execute_addr));
        }
      }
    } else if (0 == job_info.get_zone().compare("RANDOM")) {
      if (OB_FAIL(server_random_pick_from_zone_list(tenant_id, all_zone_list, execute_addr))) {
        LOG_WARN("get execute addr from random failed",
            KR(ret), K(tenant_id), K(execute_addr));
      }
    } else {
      common::ObSEArray<common::ObZone, FILTER_ZONE_SIZE> filter_zone_list;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_zone_list.count(); ++i) {
        common::ObZone zone = all_zone_list.at(i);
        if (0 == job_info.get_zone().case_compare(zone.str())) {
          if (OB_FAIL(filter_zone_list.push_back(zone))) {
            LOG_WARN("failed to push back to filter_zone_list",
              KR(ret), K(job_info.get_tenant_id()), K(job_info.get_zone()));
            }
        }
      }
      if (OB_SUCC(ret)) {
        if (filter_zone_list.empty()) {
          if(OB_FAIL(table_operator_.update_for_zone_not_exist(job_info))) {
            LOG_WARN("update not zone info failed",KR(ret), K(job_info.get_tenant_id()), K(job_info.get_zone()));
          }
        } else if (OB_FAIL(server_random_pick_from_zone_list(tenant_id, filter_zone_list, execute_addr))) {
          LOG_WARN("get execute addr from filter zone failed",
            KR(ret), K(job_info.get_tenant_id()), K(job_info.get_zone()), K(execute_addr));
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::server_random_pick_from_zone_list(int64_t tenant_id, common::ObIArray<common::ObZone> &zone_list, ObAddr &server)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObAddr, DEFALUT_SERVER_SIZE> total_server;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret), K(inited_));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (0 == tenant_server_cache_.count() && OB_FAIL(update_tenant_server_cache())) {
    LOG_WARN("update tenant server list failed", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_server_cache_.count(); ++i) {
      common::ObZone zone;
      ObAddr server = tenant_server_cache_.at(i);
      int tmp_ret = OB_SUCCESS;
      bool is_active = false;
      bool is_service = false;
      if (OB_TMP_FAIL(SVR_TRACER.get_server_zone(server, zone))) {
        LOG_WARN("fail to get zone server list", KR(tmp_ret), K(zone));
      } else if (OB_TMP_FAIL(SVR_TRACER.check_server_active(server, is_active))) {
        LOG_WARN("fail to check server active", KR(tmp_ret), K(server));
      } else if (OB_TMP_FAIL(SVR_TRACER.check_in_service(server, is_service))) {
        LOG_WARN("fail to check server service", KR(tmp_ret), K(server));
      } else if (is_active && is_service && has_exist_in_array(zone_list, zone)) {
        if(OB_FAIL(total_server.push_back(server))) {
          LOG_WARN("fail to push server to total", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && 0 == total_server.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not find an alive server", K(ret), K(total_server), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    int64_t pos = rand_.get(0,65536) % total_server.count();
    server = total_server.at(pos);
  }
  return ret;
}
int ObDBMSSchedJobMaster::check_tenant()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job not init yet", K(ret), K(inited_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else {
    const ObTenantSchema *tenant_schema = NULL;
    OZ (schema_guard.get_tenant_info(tenant_id_, tenant_schema));
    CK (OB_NOT_NULL(tenant_schema));
    bool is_tenant_standby = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAllTenantInfoProxy::is_standby_tenant(GCTX.sql_proxy_, tenant_id_, is_tenant_standby))) {
      LOG_WARN("check is standby tenant failed", K(ret), K(tenant_id_));
    } else if (is_tenant_standby) {
      clear_wait_vector();
      alive_jobs_.clear();
      LOG_INFO("tenant is standby, not check new jobs, and remove exist jobs", K(tenant_id_));
    } else {
      OZ (check_new_jobs(tenant_id_, tenant_schema->is_oracle_tenant()));
    }
  }
  LOG_INFO("check all tenants", K(ret));
  return ret;
}

int ObDBMSSchedJobMaster::check_new_jobs(uint64_t tenant_id, bool is_oracle_tenant)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDBMSSchedJobInfo, 12> job_infos;
  ObArenaAllocator allocator("DBMSSchedTmp");
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
    if (job_info.valid() && !job_info.is_disabled() && !job_info.is_broken()) {
      int tmp = alive_jobs_.exist_refactored(job_info.get_job_id());
      if (OB_HASH_EXIST == tmp) {
        // do nothing ...
        LOG_DEBUG("job exist", K(alive_jobs_));
      } else if (OB_HASH_NOT_EXIST == tmp) {
        ObDBMSSchedJobKey *job_key = NULL;
        if (OB_FAIL(alloc_job_key(
          job_key,
          job_info.get_tenant_id(),
          job_info.is_oracle_tenant(),
          job_info.get_job_id(),
          job_info.get_job_name()))) {
          LOG_WARN("failed to alloc job key", K(ret), K(job_info));
        } else if (OB_FAIL(register_job(job_key, ObTimeUtility::current_time()))) {
          LOG_WARN("failed to register job", K(ret), K(job_info));
          free_job_key(job_key);
          job_key = NULL;
        }
        LOG_INFO("register new job", K(ret), K(tenant_id), K(job_info));
      } else {
        LOG_ERROR("dbms sched job master check job exist failed", K(tmp), K(job_info));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::register_job(ObDBMSSchedJobKey *job_key, int64_t next_date)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(job_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job key is null", K(ret));
  } else if (next_date == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("next date should not be 0", K(ret), KPC(job_key), K(next_date));
  } else {
    job_key->set_execute_at(next_date);
    common::ObSortedVector<ObDBMSSchedJobKey *>::iterator iter;
    ObDBMSSchedJobKey *replace_job_key = NULL;
    OZ (wait_vector_.replace(job_key, iter, compare_job_key, equal_job_key, replace_job_key));
  }
  return ret;
}

int ObDBMSSchedJobMaster::update_tenant_server_cache()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched master not inited", K(ret), K(inited_));
  } else {
    ObArray<ObUnit> units;
    if (OB_FAIL(unit_operator_.get_units_by_tenant(gen_user_tenant_id(tenant_id_), units))) {
      LOG_WARN("failed to get tenant unit", KR(ret), K(tenant_id_));
    } else {
      common::ObArray<ObAddr> server_list;
      ObServerInfoInTable server_info;
      for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
        const ObUnit &unit = units.at(i);
        ObAddr server = unit.server_;
        if (OB_UNLIKELY(!server.is_valid())) {
          // skip
        } else if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
          LOG_WARN("fail to execute get_server_info", KR(ret), K(server));
        } else if (!server_info.in_service() || !server_info.is_active() || server_info.is_stopped()) {
          // skip
        } else if (!has_exist_in_array(server_list, server) && OB_FAIL(server_list.push_back(server))) {
          LOG_WARN("fail to execute push_back", KR(ret), K(server));
        }
      }
      if (OB_SUCC(ret)) {
        tenant_server_cache_.reuse();
        for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
           if(OB_FAIL(tenant_server_cache_.push_back(server_list.at(i)))) {
              LOG_WARN("fail to execute push_back", KR(ret), K(tenant_server_cache_));
           }
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobMaster::purge_run_detail()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job not init yet", K(ret), K(inited_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (DATA_VERSION_SUPPORT_JOB_CLASS(data_version)) {
    if (OB_FAIL(job_rpc_proxy_->purge_run_detail(tenant_id_, self_addr_))) {
      LOG_WARN("failed to run dbms sched job", K(ret), K(tenant_id_), K(self_addr_));
    } else {
      LOG_INFO("send purge run detail rpc finish", K(ret), K(tenant_id_));
    }
  }
  return ret;
}

void ObDBMSSchedJobMaster::clear_wait_vector()
{
  common::ObSortedVector<ObDBMSSchedJobKey *>::iterator iter;
  for (iter = wait_vector_.begin(); iter != wait_vector_.end(); ++iter) {
    ObDBMSSchedJobKey *job_key = *iter;
    allocator_.free(job_key);
  }
  wait_vector_.clear();
}
bool ObDBMSSchedJobMaster::compare_job_key(const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs)
{
  return lhs->get_execute_at() < rhs->get_execute_at()
    || (lhs->get_execute_at() == rhs->get_execute_at() && lhs->get_job_id() < rhs->get_job_id())
    || (lhs->get_execute_at() == rhs->get_execute_at() && lhs->get_job_id() == rhs->get_job_id() &&
        lhs->get_tenant_id() < rhs->get_tenant_id());
}

bool ObDBMSSchedJobMaster::equal_job_key(const ObDBMSSchedJobKey *lhs, const ObDBMSSchedJobKey *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id() &&
         lhs->get_job_id() == rhs->get_job_id() &&
         lhs->get_execute_at() == rhs->get_execute_at();
}

} // end for namespace dbms_scheduler
} // end for namespace oceanbase
