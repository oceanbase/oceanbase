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

#define USING_LOG_PREFIX TRANS

#include "share/inner_table/ob_inner_table_schema_constants.h"  // OB_ALL_WEAK_READ_SERVICE_TNAME
#include "storage/ob_partition_service.h"                       // ObIPartitionGroupGuard
#include "clog/ob_partition_log_service.h"                      // ObPartitionService
#include "lib/mysqlclient/ob_mysql_result.h"                    // ObMySQLResult
#include "ob_weak_read_util.h"

#include "ob_tenant_weak_read_cluster_service.h"

#define CLUSTER_STAT(level, fmt, args...) \
  TRANS_LOG(level, "[WRS] [TENANT_WEAK_READ_SERVICE] [CLUSTER_SERVICE] " fmt, ##args);
#define CLUSTER_ISTAT(fmt, args...) CLUSTER_STAT(INFO, fmt, ##args)
#define CLUSTER_WSTAT(fmt, args...) CLUSTER_STAT(WARN, fmt, ##args)
#define CLUSTER_DSTAT(fmt, args...) CLUSTER_STAT(DEBUG, fmt, ##args)

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace clog;
using namespace share;
using namespace common::sqlclient;

namespace transaction {

ObTenantWeakReadClusterService::ObTenantWeakReadClusterService()
    : inited_(false),
      wrs_pkey_(),
      ps_(NULL),
      mysql_proxy_(NULL),
      in_service_(false),
      can_update_version_(false),
      start_service_tstamp_(0),
      leader_epoch_(0),
      skipped_server_count_(0),
      last_print_skipped_server_tstamp_(0),
      error_count_for_change_leader_(0),
      last_error_tstamp_for_change_leader_(0),
      all_valid_server_count_(0),
      current_version_(0),
      min_version_(0),
      max_version_(0),
      cluster_version_mgr_(),
      rwlock_()
{}

ObTenantWeakReadClusterService::~ObTenantWeakReadClusterService()
{
  destroy();
}

int ObTenantWeakReadClusterService::init(const uint64_t tenant_id, ObPartitionService& ps, ObMySQLProxy& mysql_proxy)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = combine_id(tenant_id, OB_ALL_WEAK_READ_SERVICE_TID);
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(wrs_pkey_.init(table_id, 0, 0))) {
    LOG_WARN("init wrs table partition key fail", KR(ret), K(table_id), K(tenant_id), K(wrs_pkey_));
  } else {
    ps_ = &ps;
    mysql_proxy_ = &mysql_proxy;
    in_service_ = false;
    can_update_version_ = false;
    start_service_tstamp_ = 0;
    leader_epoch_ = 0;
    skipped_server_count_ = 0;
    last_print_skipped_server_tstamp_ = 0;
    error_count_for_change_leader_ = 0;
    last_error_tstamp_for_change_leader_ = 0;
    all_valid_server_count_ = 0;
    current_version_ = 0;
    min_version_ = 0;
    max_version_ = 0;
    cluster_version_mgr_.reset(tenant_id);
    inited_ = true;
    CLUSTER_ISTAT("init succ", K(tenant_id), K(wrs_pkey_));
  }
  return ret;
}

void ObTenantWeakReadClusterService::destroy()
{
  CLUSTER_ISTAT("destroy", "tenant_id", wrs_pkey_.get_tenant_id(), K(in_service_), K(leader_epoch_));

  stop_service();

  inited_ = false;
  wrs_pkey_.reset();
  ps_ = NULL;
  mysql_proxy_ = NULL;
  in_service_ = false;
  can_update_version_ = false;
  start_service_tstamp_ = 0;
  leader_epoch_ = 0;
  skipped_server_count_ = 0;
  last_print_skipped_server_tstamp_ = 0;
  error_count_for_change_leader_ = 0;
  last_error_tstamp_for_change_leader_ = 0;
  all_valid_server_count_ = 0;
  current_version_ = 0;
  min_version_ = 0;
  max_version_ = 0;
  cluster_version_mgr_.reset(OB_INVALID_ID);
}

int ObTenantWeakReadClusterService::check_leader_info_(int64_t& leader_epoch) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* part = NULL;
  ObRole role = INVALID_ROLE;
  ObIPartitionLogService* pls = NULL;
  if (OB_ISNULL(ps_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ps_->get_partition(wrs_pkey_, guard))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      // WRS partition not exist
      ret = OB_NOT_MASTER;
    } else {
      LOG_WARN("get WRS partition fail", KR(ret), K(wrs_pkey_));
    }
  } else if (OB_ISNULL(part = guard.get_partition_group()) || OB_ISNULL(pls = part->get_log_service())) {
    // partition exist
    ret = OB_NOT_MASTER;
  } else if (OB_FAIL(pls->get_role_and_leader_epoch(role, leader_epoch))) {
    LOG_WARN("partition log service get_role_and_leader_epoch fail", KR(ret), K(wrs_pkey_));
  } else if (!is_strong_leader(role)) {
    // not Leader
    ret = OB_NOT_MASTER;
    leader_epoch = 0;
  } else {
    // get leader info success
  }
  return ret;
}

// Query valid server count SQL
#define QUERY_ALL_SERVER_COUNT_SQL "select count(1) as cnt from __all_server where status = 'active'"

void ObTenantWeakReadClusterService::update_valid_server_count_()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    if (OB_ISNULL(mysql_proxy_)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(sql.assign_fmt(QUERY_ALL_SERVER_COUNT_SQL))) {
      LOG_WARN("generate QUERY_ALL_SERVER_COUNT_SQL fail", KR(ret));
    } else if (OB_FAIL(mysql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("execute sql read fail", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql fail", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        LOG_WARN("query valid server count fail", KR(ret), K(sql));
        ret = OB_ERR_UNEXPECTED;
      } else {
        LOG_WARN("iterate next result fail", KR(ret), K(sql));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "cnt", all_valid_server_count_, int64_t);
    }
  }
}

#define QUERY_CLUSTER_VERSION_SQL "select min_version, max_version from %s where level_id = %d and level_value = ''"

int ObTenantWeakReadClusterService::query_cluster_version_range_(
    int64_t& cur_min_version, int64_t& cur_max_version, bool& record_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    uint64_t tenant_id = wrs_pkey_.get_tenant_id();
    // record exist as default
    record_exist = true;
    if (OB_ISNULL(mysql_proxy_)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(sql.assign_fmt(QUERY_CLUSTER_VERSION_SQL, OB_ALL_WEAK_READ_SERVICE_TNAME, WRS_LEVEL_CLUSTER))) {
      LOG_WARN("generate QUERY_CLUSTER_VERSION_SQL fail", KR(ret));
    } else if (OB_FAIL(mysql_proxy_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql read fail", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql fail", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        // record not exist
        cur_min_version = 0;
        cur_max_version = 0;
        ret = OB_SUCCESS;
        CLUSTER_ISTAT("no CLUSTER record in WRS table", K(tenant_id));
        record_exist = false;
      } else {
        LOG_WARN("iterate next result fail", KR(ret), K(sql));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "min_version", cur_min_version, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "max_version", cur_max_version, int64_t);
      record_exist = true;
    }

    if (OB_SUCCESS == ret) {
      CLUSTER_ISTAT("query CLUSTER version range succ", K(tenant_id), K(cur_min_version), K(cur_max_version));
    }
  }
  return ret;
}

#define INSERT_CLUSTER_VERSION_SQL \
  " \
    insert into %s (level_id, level_value, level_name, min_version, max_version) \
    values (%d, '', '%s', %ld, %ld)\
"

#define UPDATE_CLUSTER_VERSION_SQL \
  " \
    update %s set min_version=%ld, max_version=%ld \
    where level_id = %d and level_value = '' and min_version = %ld and max_version = %ld \
"

int ObTenantWeakReadClusterService::build_update_version_sql_(const int64_t last_min_version,
    const int64_t last_max_version, const int64_t new_min_version, const int64_t new_max_version,
    const bool record_exist, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  if (record_exist) {
    // do UPDATE if record exist
    if (OB_FAIL(sql.assign_fmt(UPDATE_CLUSTER_VERSION_SQL,
            OB_ALL_WEAK_READ_SERVICE_TNAME,
            new_min_version,
            new_max_version,
            WRS_LEVEL_CLUSTER,
            last_min_version,
            last_max_version))) {
      LOG_WARN("generate update cluster weak read version sql fail", KR(ret), K(sql));
    }
  } else {
    // do INSERT if record not exist
    if (OB_FAIL(sql.assign_fmt(INSERT_CLUSTER_VERSION_SQL,
            OB_ALL_WEAK_READ_SERVICE_TNAME,
            WRS_LEVEL_CLUSTER,
            wrs_level_to_str(WRS_LEVEL_CLUSTER),
            new_min_version,
            new_max_version))) {
      LOG_WARN("generate insert cluster weak read version sql fail", KR(ret), K(sql));
    }
  }

  CLUSTER_DSTAT("build update cluster version sql", KR(ret), K(sql));
  return ret;
}

int ObTenantWeakReadClusterService::persist_version_if_need_(const int64_t last_min_version,
    const int64_t last_max_version, const int64_t new_min_version, const int64_t new_max_version,
    const bool record_exist, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t tenant_id = wrs_pkey_.get_tenant_id();
  int64_t begin_ts = ObTimeUtility::current_time();
  static const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;
  // check if need update
  // NOTE:min_version <= max_version
  if (new_min_version > last_min_version && new_min_version >= last_max_version) {
    if (OB_ISNULL(mysql_proxy_)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(build_update_version_sql_(
                   last_min_version, last_max_version, new_min_version, new_max_version, record_exist, sql))) {
      LOG_WARN("build update version sql fail",
          KR(ret),
          K(sql),
          K(last_min_version),
          K(last_max_version),
          K(new_min_version),
          K(new_max_version),
          K(record_exist));
    } else if (OB_FAIL(mysql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute update cluster weak read version sql fail", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    }
    // If the number of affected rows is 0, it means that the update condition is not met.
    // In this case, do PERSIST version failed, and function caller need to try again.
    //
    // @NOTE: If this happens, it means that there are concurrent transactions that are being modified,
    // and there is a high probability of two "cluster leaders".
    // It is possible that the old leader has not stepped down, and the new leader is taking over.
    // The external module needs to perform self-check immediately, or force resignation, or retry to take over.
    else if (OB_UNLIKELY(0 == affected_rows)) {
      ret = OB_NEED_RETRY;
      LOG_WARN("update cluster version sql affects no row, other server is updating cluster version"
               ", need retry",
          KR(ret),
          K(tenant_id),
          K(affected_rows),
          K(sql));
    } else if (OB_UNLIKELY(affected_rows != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error, update cluster version sql affects multiple rows or no rows",
          KR(ret),
          K(tenant_id),
          K(affected_rows),
          K(sql));
    } else {
      int64_t total_time = ObTimeUtility::current_time() - begin_ts;
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        CLUSTER_ISTAT("persist CLUSTER version range succ",
            K(tenant_id),
            K(new_min_version),
            K(new_max_version),
            K(last_min_version),
            K(last_max_version),
            K(total_time));
      }
    }
  }
  if (OB_SUCCESS != ret) {
    ATOMIC_INC(&error_count_for_change_leader_);
    ATOMIC_STORE(&last_error_tstamp_for_change_leader_, ObTimeUtility::current_time());
  }
  return ret;
}

int ObTenantWeakReadClusterService::start_service()
{
  int ret = OB_SUCCESS;
  int64_t leader_epoch = 0;
  int64_t begin_ts = ObTimeUtility::current_time();
  int64_t after_lock_time = 0, begin_query_ts = 0, begin_persist_ts = 0, end_ts = 0;
  const int64_t max_stale_time = ObWeakReadUtil::max_stale_time_for_weak_consistency(wrs_pkey_.get_tenant_id());
  const int64_t tenant_id = wrs_pkey_.get_tenant_id();

  CLUSTER_ISTAT("begin start service", K(tenant_id), K(is_in_service()), K_(can_update_version));

  // write lock
  WLockGuard guard(rwlock_);

  after_lock_time = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_in_service())) {
    // allready in service
    LOG_INFO("CLUSTER weak read service has been started",
        K(tenant_id),
        K(leader_epoch_),
        K(current_version_),
        K(min_version_),
        K(max_version_),
        K(start_service_tstamp_));
  } else if (OB_FAIL(check_leader_info_(leader_epoch))) {
    if (OB_NOT_MASTER == ret) {
      // not Leader
    } else {
      LOG_WARN("check leader info fail", KR(ret), K(wrs_pkey_), K(leader_epoch));
    }
  } else if (OB_UNLIKELY(leader_epoch <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("WRS leader epoch is invalid", KR(ret), K(leader_epoch));
  } else {
    int64_t cur_min_version = 0, cur_max_version = 0;
    bool record_exist = false;

    begin_query_ts = ObTimeUtility::current_time();

    // query cluster weak read version range in WRS table
    if (OB_FAIL(query_cluster_version_range_(cur_min_version, cur_max_version, record_exist))) {
      LOG_WARN("query cluster version range from WRS table fail", KR(ret));
    } else {
      begin_persist_ts = ObTimeUtility::current_time();

      // new weak read version delay should smaller than max_stale_time
      int64_t new_version = std::max(cur_max_version, ObTimeUtility::current_time() - max_stale_time);
      int64_t new_min_version = new_version;
      int64_t new_max_version = generate_max_version_(new_min_version);
      int64_t affected_rows = 0;

      // do persist
      if (OB_FAIL(persist_version_if_need_(
              cur_min_version, cur_max_version, new_min_version, new_max_version, record_exist, affected_rows))) {
        LOG_WARN("persist version if need fail",
            KR(ret),
            K(wrs_pkey_),
            K(cur_min_version),
            K(cur_max_version),
            K(new_min_version),
            K(new_max_version),
            K(max_stale_time),
            K(record_exist),
            K(affected_rows),
            K(error_count_for_change_leader_),
            K(last_error_tstamp_for_change_leader_));
      } else {
        // init version
        min_version_ = new_min_version;
        max_version_ = new_max_version;
        ATOMIC_STORE(&current_version_, new_version);

        // weak read service start success
        leader_epoch_ = leader_epoch;
        ATOMIC_STORE(&in_service_, true);
        ATOMIC_STORE(&start_service_tstamp_, ObTimeUtility::current_time());
      }
    }
  }

  if (OB_SUCCESS == ret) {
    // update the active server number after service start,
    // which will be useful in subsequent updates
    update_valid_server_count_();
  }

  end_ts = ObTimeUtility::current_time();
  CLUSTER_ISTAT("start service done",
      KR(ret),
      K(tenant_id),
      K_(in_service),
      K_(leader_epoch),
      K_(current_version),
      "delta",
      end_ts - current_version_,
      K_(min_version),
      K_(max_version),
      K(max_stale_time),
      K_(all_valid_server_count),
      "total_time",
      end_ts - begin_ts,
      "wlock_time",
      after_lock_time - begin_ts,
      "check_leader_time",
      0 == begin_query_ts ? 0 : begin_query_ts - after_lock_time,
      "query_version_time",
      0 == begin_persist_ts ? 0 : begin_persist_ts - begin_query_ts,
      "persist_version_time",
      0 == begin_persist_ts ? 0 : end_ts - begin_persist_ts);

  return ret;
}

void ObTenantWeakReadClusterService::stop_service()
{
  // write lock
  WLockGuard guard(rwlock_);

  if (is_in_service()) {
    stop_service_impl_();
  }
}

int ObTenantWeakReadClusterService::stop_service_if_leader_info_match(const int64_t target_leader_epoch)
{
  int ret = OB_SUCCESS;
  // write lock
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (is_in_service() && target_leader_epoch == leader_epoch_) {
    stop_service_impl_();
  }
  return ret;
}

void ObTenantWeakReadClusterService::get_serve_info(bool& in_service, int64_t& leader_epoch) const
{
  RLockGuard guard(rwlock_);

  in_service = in_service_;
  leader_epoch = leader_epoch_;
}

int ObTenantWeakReadClusterService::get_version(int64_t& version) const
{
  int64_t min_version = 0, max_version = 0;
  return get_version(version, min_version, max_version);
}

int ObTenantWeakReadClusterService::get_version(int64_t& version, int64_t& min_version, int64_t& max_version) const
{
  int ret = OB_SUCCESS;
  static const int64_t GET_CLUSTER_VERSION_RDLOCK_TIMEOUT = 100;
  int64_t ret_version = 0;
  int64_t rdlock_wait_time = GET_CLUSTER_VERSION_RDLOCK_TIMEOUT;

  // Lock wait time is necessary to prevent 'deadlock'
  //
  // Because the thread that calls this function is the tenant's worker thread, and when the service
  // is started and the write lock is added, SQL (query and update internal tables) needs to be executed,
  // and it will also rely on worker thread resources at this time.
  // If the read lock is forced here, it may appear that all worker threads are trying to add a read lock
  // and waiting, and the thread holding the write lock is waiting for the worker thread resources,
  // so a "deadlock" occurs.
  //
  // If the attempt to add a read lock fails, the outermost caller is required to retry
  // and release the worker thread resources
  if (OB_FAIL(rwlock_.rdlock(rdlock_wait_time))) {
    CLUSTER_ISTAT("try rdlock conflict when get CLUSTER weak read version, need retry",
        KR(ret),
        "tenant_id",
        wrs_pkey_.get_tenant_id(),
        K(is_in_service()));
    // Attempt to add read lock failed
    ret = OB_NEED_RETRY;
  } else {
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
    }
    // Check if in service first
    else if (OB_UNLIKELY(!is_in_service())) {
      // NOT in service
      ret = OB_NOT_IN_SERVICE;
    } else {
      // get current version if in service
      ret_version = ATOMIC_LOAD(&current_version_);
      min_version = ATOMIC_LOAD(&min_version_);
      max_version = ATOMIC_LOAD(&max_version_);

      // check leader info again, if not leader, return NOT MASTER
      int64_t cur_leader_epoch = 0;
      if (OB_FAIL(check_leader_info_(cur_leader_epoch))) {
        if (OB_NOT_MASTER == ret) {
          // NOT Leader
        } else {
          LOG_WARN("check WRS leader info fail", KR(ret));
        }
      } else if (cur_leader_epoch != leader_epoch_) {
        ret = OB_NOT_MASTER;
        CLUSTER_ISTAT("WRS leader changed when get CLUSTER version, need stop CLUSTER weak read service",
            "tenant_id",
            wrs_pkey_.get_tenant_id(),
            K(cur_leader_epoch),
            K(leader_epoch_));
      } else {
        // check leader info before and after, return version if consistent
        version = ret_version;
      }
    }

    // release lock
    (void)rwlock_.unlock();
  }

  return ret;
}

bool ObTenantWeakReadClusterService::need_print_skipped_server()
{
  bool need_print = false;
  int64_t current_time = ObTimeUtility::current_time();
  if ((current_time - last_print_skipped_server_tstamp_) > PRINT_CLUSTER_SERVER_INFO_INTERVAL) {
    need_print = true;
    ATOMIC_STORE(&last_print_skipped_server_tstamp_, current_time);
  }
  return need_print;
}

bool ObTenantWeakReadClusterService::check_can_update_version_()
{
  const bool old_can_update_version = can_update_version_;

  // weak read service start time
  const int64_t start_service_time = ObTimeUtility::current_time() - start_service_tstamp_;
  // server count allready registered
  const int64_t registered_server_count = cluster_version_mgr_.get_server_count();

  // If the number of valid servers is valid, and the number of currently registered servers is
  // greater than or equal to the number of valid servers, all servers are considered to
  // have reported their status, and the version number can be calculated.
  if (all_valid_server_count_ > 0 && registered_server_count >= all_valid_server_count_) {
    can_update_version_ = true;

    // print log in first update version
    if (!old_can_update_version) {
      CLUSTER_ISTAT("can update version while all valid servers are registered",
          "tenant_id",
          wrs_pkey_.get_tenant_id(),
          K(registered_server_count),
          K_(all_valid_server_count),
          K_(in_service),
          K_(start_service_tstamp),
          K(start_service_time),
          K_(current_version),
          K_(min_version),
          K_(max_version));
    }
  } else if (start_service_time > FORCE_UPDATE_VERSION_TIME_AFTER_START_SERVICE) {
    // otherwise, after starting the service for a period of time,
    // it is mandatory to think that the version number can be updated
    can_update_version_ = true;

    // print log in first update version
    if (!old_can_update_version) {
      CLUSTER_WSTAT("force to update version while not all valid servers registered",
          "tenant_id",
          wrs_pkey_.get_tenant_id(),
          K(registered_server_count),
          K_(all_valid_server_count),
          K_(in_service),
          K_(start_service_tstamp),
          K(start_service_time),
          K_(current_version),
          K_(min_version),
          K_(max_version));
    }
  }

  if (!can_update_version_) {
    CLUSTER_DSTAT("can not update version",
        "tenant_id",
        wrs_pkey_.get_tenant_id(),
        K(registered_server_count),
        K_(all_valid_server_count),
        K_(in_service),
        K_(start_service_tstamp),
        K(start_service_time),
        K_(current_version),
        K_(min_version),
        K_(max_version));
  }

  return can_update_version_;
}

int ObTenantWeakReadClusterService::update_version(int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t new_version = 0;
  int64_t cur_leader_epoch = 0;
  int64_t skipped_server_count = 0;
  bool need_print = false;

  // read lock
  RLockGuard guard(rwlock_);

  need_print = need_print_skipped_server();

  if (OB_UNLIKELY(!is_in_service())) {
    // not in service
    ret = OB_NOT_IN_SERVICE;
  } else if (OB_FAIL(check_leader_info_(cur_leader_epoch))) {
    if (OB_NOT_MASTER == ret) {
      // NOT Leader
    } else {
      LOG_WARN("check WRS leader info fail", KR(ret));
    }
  } else if (OB_UNLIKELY(cur_leader_epoch != leader_epoch_)) {
    ret = OB_NOT_MASTER;
    CLUSTER_ISTAT("WRS leader changed when update CLUSTER version, need stop CLUSTER weak read service",
        "tenant_id",
        wrs_pkey_.get_tenant_id(),
        K(cur_leader_epoch),
        K(leader_epoch_));
  } else if (!check_can_update_version_()) {
    // check if can update version or not
  } else if (OB_UNLIKELY((new_version = compute_version_(skipped_server_count, need_print)) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid CLUSTER weak read version", K(new_version), KR(ret));
  } else {
    ATOMIC_STORE(&skipped_server_count_, skipped_server_count);
    // if new version exceeds the weak read version range([min, max)), do PERSIST
    // NOTE:Note that min_version maybe equal to max_version
    if (new_version > min_version_ && new_version >= max_version_) {
      int64_t new_min_version = new_version;
      int64_t new_max_version = generate_max_version_(new_min_version);
      // weak read record should be inserted in start service, and following update version only include UPDATE SQL
      bool record_exist = true;

      // do persist if new version bigger than old max version
      if (OB_FAIL(persist_version_if_need_(
              min_version_, max_version_, new_min_version, new_max_version, record_exist, affected_rows))) {
        LOG_WARN("persist CLUSTER weak read version if need fail",
            KR(ret),
            K(wrs_pkey_),
            K(min_version_),
            K(max_version_),
            K(new_min_version),
            K(new_max_version),
            K(record_exist),
            K(affected_rows),
            K(error_count_for_change_leader_),
            K(last_error_tstamp_for_change_leader_));
      } else {
        // init version
        min_version_ = new_min_version;
        max_version_ = new_max_version;
      }
    }

    if (OB_SUCCESS == ret && new_version > current_version_) {
      ATOMIC_STORE(&current_version_, new_version);
      CLUSTER_DSTAT("update version",
          "tenant_id",
          wrs_pkey_.get_tenant_id(),
          K_(current_version),
          K_(min_version),
          K_(max_version),
          K_(in_service),
          K_(leader_epoch));
    }
  }
  return ret;
}

int64_t ObTenantWeakReadClusterService::compute_version_(int64_t& skipped_servers, bool need_print) const
{
  /// min weak read version delay should not smaller than max_stale_time
  int64_t base_version = ObWeakReadUtil::generate_min_weak_read_version(wrs_pkey_.get_tenant_id());
  // weak read version should increase monotonically
  base_version = std::max(current_version_, base_version);
  return cluster_version_mgr_.get_version(base_version, skipped_servers, need_print);
}

bool ObTenantWeakReadClusterService::is_service_master() const
{
  int ret = OB_SUCCESS;
  bool is_master = false;
  ;
  int64_t cur_leader_epoch = 0;
  if (OB_FAIL(check_leader_info_(cur_leader_epoch))) {
    is_master = false;
  } else {
    is_master = true;
  }
  return is_master;
}

void ObTenantWeakReadClusterService::self_check()
{
  int ret = OB_SUCCESS;
  bool in_service = false;
  int64_t serve_leader_epoch = 0;
  int64_t cur_leader_epoch = 0;
  bool need_stop_service = false;
  bool need_start_service = false;
  bool need_change_leader = false;
  uint64_t tenant_id = wrs_pkey_.get_tenant_id();
  static const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;

  get_serve_info(in_service, serve_leader_epoch);

  // check if self is wrs Leader
  if (OB_FAIL(check_leader_info_(cur_leader_epoch))) {
    if (OB_NOT_MASTER == ret) {
      // not wrs leader
    } else {
      LOG_WARN("check WRS leader info fail", KR(ret));
    }
  }

  if (OB_NOT_MASTER == ret) {
    if (in_service) {
      need_stop_service = true;
      CLUSTER_ISTAT("[SELF_CHECK] current server is not WRS leader. need stop CLUSTER weak read service",
          K(tenant_id),
          K(serve_leader_epoch),
          K(cur_leader_epoch),
          K(wrs_pkey_),
          K(in_service),
          K_(can_update_version),
          K(start_service_tstamp_),
          K(error_count_for_change_leader_),
          K(last_error_tstamp_for_change_leader_));
    }
    ret = OB_SUCCESS;
  } else if (OB_SUCCESS == ret) {
    if (need_force_change_leader_()) {
      // need force change leader
      need_change_leader = true;
      CLUSTER_ISTAT("[SELF_CHECK] WRS leader occur too many errors, need force change leader",
          K(tenant_id),
          K(serve_leader_epoch),
          K(cur_leader_epoch),
          K(wrs_pkey_),
          K(in_service),
          K_(can_update_version),
          K(start_service_tstamp_),
          K(error_count_for_change_leader_),
          K(last_error_tstamp_for_change_leader_));
    } else if (!in_service) {
      need_start_service = true;
      CLUSTER_ISTAT("[SELF_CHECK] current server is WRS leader, need start CLUSTER weak read service",
          K(tenant_id),
          K(serve_leader_epoch),
          K(cur_leader_epoch),
          K(wrs_pkey_),
          K(in_service),
          K_(can_update_version),
          K(start_service_tstamp_),
          K(error_count_for_change_leader_),
          K(last_error_tstamp_for_change_leader_));
    } else if (cur_leader_epoch == serve_leader_epoch) {
      // wrs is in normal
    } else {
      // Leader switch need restart service, stop and start again
      need_stop_service = true;
      need_start_service = true;
      CLUSTER_ISTAT("[SELF_CHECK] WRS leader epoch changed, need stop and restart CLUSTER weak read service",
          K(tenant_id),
          K(serve_leader_epoch),
          K(cur_leader_epoch),
          K(wrs_pkey_),
          K(in_service),
          K_(can_update_version),
          K(start_service_tstamp_),
          K(error_count_for_change_leader_),
          K(last_error_tstamp_for_change_leader_));
    }
  }

  if (OB_SUCCESS == ret) {
    if (need_change_leader) {
      if (OB_FAIL(force_change_leader_())) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("not find appropriate server to change leader", KR(ret), K(tenant_id));
        } else {
          LOG_WARN("weak read service force CHANGE LEADER failed", KR(ret), K(tenant_id));
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (need_stop_service) {
      // stop service with appointed epoch, do dothing if epoch not match
      if (OB_FAIL(stop_service_if_leader_info_match(serve_leader_epoch))) {
        LOG_WARN("stop CLUSTER weak read service fail", KR(ret), K(serve_leader_epoch));
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (need_start_service) {
      if (OB_FAIL(start_service())) {
        if (OB_NOT_MASTER == ret || OB_NEED_RETRY == ret) {
          LOG_WARN("start CLUSTER weak read service fail, retry next time", KR(ret), K(tenant_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("start CLUSTER weak read service fail", KR(ret), K(tenant_id));
        }
      }
    }
  }

  if (REACH_TIME_INTERVAL(PRINT_INTERVAL) || need_stop_service || need_start_service || need_change_leader) {
    CLUSTER_ISTAT("[SELF_CHECK] done",
        KR(ret),
        K(tenant_id),
        K(need_start_service),
        K(need_stop_service),
        K(need_change_leader),
        K(is_in_service()),
        K_(can_update_version),
        K(cur_leader_epoch),
        K(start_service_tstamp_),
        K(error_count_for_change_leader_),
        K(last_error_tstamp_for_change_leader_));
  } else {
    CLUSTER_DSTAT("[SELF_CHECK] done",
        KR(ret),
        K(tenant_id),
        K(need_start_service),
        K(need_stop_service),
        K(need_change_leader),
        K(is_in_service()),
        K_(can_update_version),
        K(cur_leader_epoch),
        K(start_service_tstamp_),
        K(error_count_for_change_leader_),
        K(last_error_tstamp_for_change_leader_));
  }
}

bool ObTenantWeakReadClusterService::need_force_change_leader_()
{
  int bool_ret = false;
  if (REACH_TIME_INTERVAL(ERROR_STATISTIC_INTERVAL_FOR_CHANGE_LEADER)) {
    int64_t current_time = ObTimeUtility::current_time();
    int64_t start_service_tstamp = ATOMIC_LOAD(&start_service_tstamp_);
    int64_t leader_alive_tstamp = current_time - start_service_tstamp;
    int64_t error_static = ATOMIC_LOAD(&error_count_for_change_leader_);
    int64_t last_error_tstamp = ATOMIC_LOAD(&last_error_tstamp_for_change_leader_);
    int64_t last_error_interval = current_time - last_error_tstamp;
    uint64_t tenant_id = wrs_pkey_.get_tenant_id();

    if (error_static > MAX_ERROR_THRESHOLD_FOR_CHANGE_LEADER &&
        leader_alive_tstamp > LEADER_ALIVE_THRESHOLD_FOR_CHANGE_LEADER &&
        last_error_interval < LAST_ERROR_TSTAMP_INTERVAL_FOR_CHANGE_LEADER) {
      bool_ret = true;
      LOG_WARN("too many errors occur, need change weak read service partition leader",
          K(wrs_pkey_),
          K(error_static),
          K(start_service_tstamp),
          K(leader_alive_tstamp),
          K(tenant_id),
          K(bool_ret));
    }
    // reset change leader info
    reset_change_leader_info_();
  }
  return bool_ret;
}

void ObTenantWeakReadClusterService::reset_change_leader_info_()
{
  ATOMIC_STORE(&error_count_for_change_leader_, 0);
  ATOMIC_STORE(&last_error_tstamp_for_change_leader_, 0);
}

int ObTenantWeakReadClusterService::force_change_leader_() const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* part = NULL;
  common::ObMemberList member_list;
  common::ObAddr candidate;
  common::ObAddr leader;
  uint64_t tenant_id = wrs_pkey_.get_tenant_id();
  if (OB_ISNULL(ps_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ps_->get_partition(wrs_pkey_, guard))) {
    LOG_WARN("get partition failed", K(wrs_pkey_), K(tenant_id), KR(ret));
  } else if (OB_ISNULL(part = guard.get_partition_group())) {
    // wrs partition not exist
    ret = OB_NOT_MASTER;
  } else if (OB_FAIL(part->get_leader_curr_member_list(member_list))) {
    LOG_WARN("get leader curr member list failed", K(tenant_id), K(wrs_pkey_), KR(ret));
  } else if (OB_FAIL(part->get_leader(leader))) {
    LOG_WARN("get leader failed", K(tenant_id), K(wrs_pkey_), KR(ret));
  } else if (OB_FAIL(get_candidate_server_(leader, member_list, candidate))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get candidate server failed", K(tenant_id), K(wrs_pkey_), KR(ret));
    } else {
      LOG_INFO("not find change server candidate", K(tenant_id), K(wrs_pkey_), KR(ret));
    }
  } else if (OB_FAIL(verify_candidate_server_(candidate))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("verify candidate server failed", K(tenant_id), K(wrs_pkey_), KR(ret));
    } else {
      LOG_INFO("not find change server candidate", K(tenant_id), K(wrs_pkey_), KR(ret));
    }
  } else if (OB_FAIL(ps_->change_leader(wrs_pkey_, candidate))) {
    LOG_WARN("change leader failed", K(tenant_id), K(leader), KR(ret));
  } else {
    LOG_INFO("weak read service partition change leader success", K(tenant_id), K(candidate), KR(ret));
  }
  return ret;
}

int ObTenantWeakReadClusterService::verify_candidate_server_(const common::ObAddr& server) const
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  common::ObPartitionArray pkey_array;
  common::ObSArray<common::ObAddr> dist_server_list;
  common::ObSArray<common::ObAddrSArray> candidate_list_array;
  common::ObSArray<obrpc::CandidateStatusList> candidate_status_array;
  uint64_t tenant_id = wrs_pkey_.get_tenant_id();
  if (OB_ISNULL(ps_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pkey_array.push_back(wrs_pkey_))) {
    LOG_WARN("pkey array push back failed", K(wrs_pkey_), K(tenant_id), KR(ret));
  } else if (OB_FAIL(dist_server_list.push_back(server))) {
    LOG_WARN("server list push back failed", K(server), K(wrs_pkey_), K(tenant_id), KR(ret));
  } else if (OB_FAIL(ps_->get_dst_candidates_array(
                 pkey_array, dist_server_list, candidate_list_array, candidate_status_array))) {
    LOG_WARN("get dst candidates array failed", K(wrs_pkey_), K(server), K(tenant_id), KR(ret));
  } else {
    for (int64_t index = 0; !can_change && OB_SUCC(ret) && index < candidate_list_array.count(); index++) {
      const common::ObAddrSArray& addr_array = candidate_list_array.at(index);
      for (int i = 0; !can_change && i < addr_array.count(); i++) {
        const common::ObAddr& candidate = addr_array.at(i);
        if (server == candidate) {
          can_change = true;
          LOG_INFO(
              "get appropriate server to CHANGE LEADER", K(server), K(wrs_pkey_), K(can_change), K(tenant_id), KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !can_change) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTenantWeakReadClusterService::get_candidate_server_(
    const common::ObAddr& self, const common::ObMemberList& member_list, common::ObAddr& candidate) const
{
  int ret = OB_SUCCESS;
  bool candidate_found = false;
  common::ObAddr server;
  common::ObRegion region;
  common::ObRegion leader_region = DEFAULT_REGION_NAME;
  uint64_t tenant_id = wrs_pkey_.get_tenant_id();
  if (OB_ISNULL(ps_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ps_->get_server_region(self, leader_region))) {
    LOG_WARN("get server region failed", K(server), K(tenant_id), KR(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && !candidate_found && index < member_list.get_member_number(); index++) {
      if (OB_FAIL(member_list.get_server_by_index(index, server))) {
        LOG_WARN("get server by index failed", K(index), K(tenant_id), KR(ret));
      } else if (OB_FAIL(ps_->get_server_region(server, region))) {
        LOG_WARN("get server region failed", K(server), K(tenant_id), K(index), KR(ret));
      } else if (server == self) {
        // skip
      } else if (region == leader_region) {
        candidate = server;
        candidate_found = true;
        LOG_INFO("get CHANGE LEADER candidate succ", K(server), K(region), KR(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !candidate_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

void ObTenantWeakReadClusterService::stop_service_impl_()
{
  CLUSTER_ISTAT("stop CLUSTER weak read service",
      "tenant_id",
      wrs_pkey_.get_tenant_id(),
      K_(in_service),
      K_(leader_epoch),
      K_(can_update_version),
      K_(current_version),
      K_(min_version),
      K_(max_version));

  ATOMIC_STORE(&leader_epoch_, 0);
  ATOMIC_STORE(&in_service_, false);

  can_update_version_ = false;
  current_version_ = 0;
  min_version_ = 0;
  max_version_ = 0;
  skipped_server_count_ = 0;
  cluster_version_mgr_.reset(wrs_pkey_.get_tenant_id());
}

int ObTenantWeakReadClusterService::update_server_version(const common::ObAddr& addr, const int64_t version,
    const int64_t valid_part_count, const int64_t total_part_count, const int64_t generate_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t cur_leader_epoch = 0;
  bool is_new_server = false;
  int64_t rdlock_wait_time = PROCESS_CLUSTER_HEARTBEAT_RPC_RDLOCK_TIMEOUT;

  // rpc worker can not hang, overtime should be set
  if (OB_FAIL(rwlock_.rdlock(rdlock_wait_time))) {
    CLUSTER_ISTAT("try rdlock conflict when tenant weak read service update server version, need retry",
        KR(ret),
        "tenant_id",
        wrs_pkey_.get_tenant_id(),
        K(addr),
        K(version),
        K(valid_part_count),
        K(total_part_count),
        K(generate_timestamp),
        K(wrs_pkey_),
        K(in_service_),
        K(can_update_version_));
    // try lock fail
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else {
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
    }
    // check if in service first
    else if (OB_UNLIKELY(!is_in_service())) {
      ret = OB_NOT_IN_SERVICE;
    }
    // check if self is wrs Leader
    else if (OB_FAIL(check_leader_info_(cur_leader_epoch))) {
      if (OB_NOT_MASTER == ret) {
        // not wrs leader
      } else {
        LOG_WARN("check leader info fail", KR(ret));
      }
    } else if (cur_leader_epoch != leader_epoch_) {
      ret = OB_NOT_MASTER;
      CLUSTER_ISTAT("WRS leader changed when update server version, need stop CLUSTER weak read service",
          "tenant_id",
          wrs_pkey_.get_tenant_id(),
          K(cur_leader_epoch),
          K(leader_epoch_));
    }
    // update server version
    else if (OB_FAIL(cluster_version_mgr_.update_server_version(
                 addr, version, valid_part_count, total_part_count, generate_timestamp, is_new_server))) {
      LOG_WARN("cluster version mgr update server version fail",
          KR(ret),
          K(addr),
          K(version),
          K(valid_part_count),
          K(generate_timestamp));
    } else {
      if (is_new_server) {
        CLUSTER_ISTAT("[UPDATE_SERVER_VERSION] new server registered",
            "tenant_id",
            wrs_pkey_.get_tenant_id(),
            K(addr),
            K(version),
            K(valid_part_count),
            K(total_part_count),
            K(generate_timestamp));
      }

      CLUSTER_DSTAT("[UPDATE_SERVER_VERSION] ",
          "tenant_id",
          wrs_pkey_.get_tenant_id(),
          K(addr),
          K(version),
          K(valid_part_count),
          K(total_part_count),
          K(generate_timestamp));
    }
    // release lock
    (void)rwlock_.unlock();
  }
  return ret;
}

int64_t ObTenantWeakReadClusterService::get_cluster_registered_server_count() const
{
  return cluster_version_mgr_.get_server_count();
}

int64_t ObTenantWeakReadClusterService::get_cluster_skipped_server_count() const
{
  return ATOMIC_LOAD(&skipped_server_count_);
}

}  // namespace transaction
}  // namespace oceanbase
