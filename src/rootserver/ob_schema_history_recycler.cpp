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
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_mgr.h"
#include "rootserver/ob_schema_history_recycler.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_all_server_tracer.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace share::schema;

ObRecycleSchemaValue::ObRecycleSchemaValue()
    : is_deleted_(false),
      max_schema_version_(OB_INVALID_VERSION), record_cnt_(0)
{}

ObRecycleSchemaValue::~ObRecycleSchemaValue()
{
}

int ObRecycleSchemaValue::assign(const ObRecycleSchemaValue &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    is_deleted_ = other.is_deleted_;
    max_schema_version_ = other.max_schema_version_;
    record_cnt_ = other.record_cnt_;
  }
  return ret;
}

ObRecycleSchemaValue &ObRecycleSchemaValue::operator=(const ObRecycleSchemaValue &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("fail to assign ObRecycleSchemaValue", K(ret), K(other));
  }
  return *this;
}

void ObRecycleSchemaValue::reset()
{
  is_deleted_ = false;
  max_schema_version_ = OB_INVALID_VERSION;
  record_cnt_ = 0;
}

bool ObRecycleSchemaValue::is_valid() const
{
  return max_schema_version_ > 0 && record_cnt_ > 0;
}

int64_t ObSchemaHistoryRecyclerIdling::get_idle_interval_us()
{
  int64_t idle_time = DEFAULT_SCHEMA_HISTORY_RECYCLE_INTERVAL;
  if (0 != GCONF.schema_history_recycle_interval
      && !GCONF.in_upgrade_mode()) {
    idle_time = GCONF.schema_history_recycle_interval;
  }
  return idle_time;
}

ObSchemaHistoryRecycler::ObSchemaHistoryRecycler()
  : inited_(false), idling_(stop_), schema_service_(NULL),
    /*freeze_info_mgr_(NULL),*/ zone_mgr_(NULL), sql_proxy_(NULL), recycle_schema_versions_()
{
}

ObSchemaHistoryRecycler::~ObSchemaHistoryRecycler()
{
  if (!stop_) {
    stop();
    wait();
  }
}

int ObSchemaHistoryRecycler::init(
    ObMultiVersionSchemaService &schema_service,
    //ObFreezeInfoManager &freeze_info_manager,
    ObZoneManager &zone_manager,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const int schema_history_recycler_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(create(schema_history_recycler_thread_cnt, "SchemaRec"))) {
    LOG_WARN("create thread failed", KR(ret), K(schema_history_recycler_thread_cnt));
  } else if (OB_FAIL(recycle_schema_versions_.create(hash::cal_next_prime(BUCKET_NUM),
                     "RecScheHisMap", "RecScheHisMap"))) {
    LOG_WARN("fail to init hashmap", KR(ret));
  } else {
    schema_service_ = &schema_service;
    //freeze_info_mgr_ = &freeze_info_manager;
    zone_mgr_ = &zone_manager;
    sql_proxy_ = &sql_proxy;
    inited_ = true;
  }
  return ret;
}

int ObSchemaHistoryRecycler::idle()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  }
  return ret;
}

void ObSchemaHistoryRecycler::wakeup()
{
  if (!inited_) {
    LOG_WARN_RET(common::OB_NOT_INIT, "not init");
  } else {
    idling_.wakeup();
  }
}

void ObSchemaHistoryRecycler::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObSchemaHistoryRecycler::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("schema history recycler stopped", KR(ret));
  } else if (0 == GCONF.schema_history_recycle_interval
             || GCONF.in_upgrade_mode()) {
    ret = OB_CANCELED;
    LOG_WARN("schema history recycler should stopped", KR(ret));
  }
  return ret;
}

int ObSchemaHistoryRecycler::check_stop()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (GCTX.is_standby_cluster()) {
    ret = OB_CANCELED;
    LOG_WARN("schema history recycler should stopped", KR(ret));
  }
  return ret;
}

void ObSchemaHistoryRecycler::run3()
{
  LOG_INFO("[SCHEMA_RECYCLE] schema history recycler start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(recycle_schema_versions_.clear())) {
    LOG_WARN("fail to clear recycle schema version map", KR(ret));
  } else {
    while (!stop_) {
      ObCurTraceId::init(GCTX.self_addr());
      LOG_INFO("[SCHEMA_RECYCLE] recycle schema history start");
      if (OB_FAIL(try_recycle_schema_history())) {
        LOG_WARN("fail to recycle schema history", KR(ret));
      }
      LOG_INFO("[SCHEMA_RECYCLE] recycle schema history finish", KR(ret));
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
      idle();
    }
  }
  LOG_INFO("[SCHEMA_RECYCLE] schema history recycler quit");
  return;
}

bool ObSchemaHistoryRecycler::is_valid_recycle_schema_version(
     const int64_t recycle_schema_version)
{
  return recycle_schema_version > OB_CORE_SCHEMA_VERSION
         && ObSchemaService::is_formal_version(recycle_schema_version);
}

int ObSchemaHistoryRecycler::get_recycle_schema_versions(
    const obrpc::ObGetRecycleSchemaVersionsArg &arg,
    obrpc::ObGetRecycleSchemaVersionsResult &result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else {
    result.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tenant_ids_.count(); i++) {
      share::TenantIdAndSchemaVersion recycle_schema_version;
      const uint64_t tenant_id = arg.tenant_ids_.at(i);
      recycle_schema_version.tenant_id_ = tenant_id;
      recycle_schema_version.schema_version_ = OB_INVALID_VERSION;
      if (OB_FAIL(recycle_schema_versions_.get_refactored(
          tenant_id, recycle_schema_version.schema_version_))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get recyle schema version", KR(ret), K(tenant_id));
        } else {
          recycle_schema_version.schema_version_ = OB_INVALID_VERSION;
          ret = OB_SUCCESS; // overwrite ret
        }
      }
      if (FAILEDx(result.recycle_schema_versions_.push_back(recycle_schema_version))) {
        LOG_WARN("fail to push back recycle schema version",
                 KR(ret), K(tenant_id), K(recycle_schema_version));
      }
    }
    if (OB_SUCC(ret) && arg.tenant_ids_.count() != result.recycle_schema_versions_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result cnt not match", KR(ret), K(arg), K(result));
    }
  }
  return ret;
}


int ObSchemaHistoryRecycler::try_recycle_schema_history()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get schema_guard", KR(ret));
  } else if (OB_FAIL(calc_recycle_schema_versions(tenant_ids))) {
    LOG_WARN("fail to fetch recycle schema version", KR(ret));
  } else if (GCTX.is_standby_cluster()) {
    // standby cluster only calc recycle schema versions
  } else if (OB_FAIL(try_recycle_schema_history(tenant_ids))) {
    LOG_WARN("fail to recycle schema history", KR(ret));
  }
  return ret;
}

int ObSchemaHistoryRecycler::try_recycle_schema_history(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      bool skip = true;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(check_can_skip_tenant(tenant_id, skip))) {
        LOG_WARN("fail to check tenant can skip", KR(ret), K(tenant_id));
      } else if (skip) {
        // pass
      } else {
        int64_t recycle_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(recycle_schema_versions_.get_refactored(tenant_id, recycle_schema_version))) {
          LOG_WARN("fail to get recycle schema version",
                   KR(ret), K(tenant_id), K(recycle_schema_version));
        } else if (is_valid_recycle_schema_version(recycle_schema_version)) {
          if (OB_FAIL(try_recycle_schema_history(tenant_id, recycle_schema_version))) {
            LOG_WARN("fail to recycle schema history by tenant",
                     KR(ret), K(tenant_id), K(recycle_schema_version));
          }
        }
        ret = OB_SUCCESS; // ignore tenant's failure
      }
    }
  }
  return ret;
}


int ObSchemaHistoryRecycler::check_can_skip_tenant(
    const uint64_t tenant_id,
    bool &skip)
{
  int ret = OB_SUCCESS;
  skip = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // TODO: (yanmu.ztl)
    // Additional schema history of system tenant should be recycled:
    // 1. Other tenant's schema history(except tenant schema history and system table's schema history) generated before schema split.
    skip = true;
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObSimpleTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get schema_guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(ret), K(tenant_id));
    } else if (!tenant_schema->is_normal()) {
      skip = true;
      LOG_INFO("tenant is not in normal stat", K(ret), KPC(tenant_schema));
    }
  }
  return ret;
}

// get recycle schema version by tenant from:
// 1. min used schema_version from schema_service by server.
// 2. min used schema_version from storage by server.
int ObSchemaHistoryRecycler::get_recycle_schema_version_by_server(
    const common::ObIArray<uint64_t> &tenant_ids,
    common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  obrpc::ObGetMinSSTableSchemaVersionArg arg;
  ObZone zone;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(arg.tenant_id_arg_list_.assign(tenant_ids))) {
    LOG_WARN("fail to assign arg", KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, server_list))) {
    LOG_WARN("fail to get server_list", KR(ret));
  } else {
    rootserver::ObGetMinSSTableSchemaVersionProxy proxy_batch(
        *(GCTX.srv_rpc_proxy_), &obrpc::ObSrvRpcProxy::get_min_sstable_schema_version);
    const int64_t timeout_ts = GCONF.rpc_timeout;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
      const ObAddr &addr = server_list.at(i);
      if (OB_FAIL(proxy_batch.call(addr, timeout_ts, arg))) {
        LOG_WARN("fail to call async batch rpc", KR(ret), K(addr), K(arg));
      }
    }
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS; // always wait all
    if (OB_TMP_FAIL(proxy_batch.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy_batch.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
        int res_ret = return_code_array.at(i);
        const ObAddr &addr = proxy_batch.get_dests().at(i);
        if (OB_SUCCESS != res_ret) {
          ret = res_ret;
          LOG_WARN("rpc execute failed", KR(ret), K(addr));
        } else {
          const obrpc::ObGetMinSSTableSchemaVersionRes *result = proxy_batch.get_results().at(i);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", KR(ret));
          } else if (result->ret_list_.count() != arg.tenant_id_arg_list_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cnt not match", KR(ret),
                     "tenant_cnt", arg.tenant_id_arg_list_.count(),
                     "result_cnt", result->ret_list_.count());
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < result->ret_list_.count(); j++) {
              int64_t schema_version = result->ret_list_.at(j);
              uint64_t tenant_id = arg.tenant_id_arg_list_.at(j);
              if (FAILEDx(fill_recycle_schema_versions(
                  tenant_id, schema_version, recycle_schema_versions))) {
                LOG_WARN("fail to fill recycle schema versions",
                         KR(ret), K(tenant_id), K(schema_version));
              }
              LOG_INFO("[SCHEMA_RECYCLE] get recycle schema version from observer",
                       KR(ret), K(addr), K(tenant_id), K(schema_version));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaHistoryRecycler::get_recycle_schema_version_for_ddl(
    const common::ObIArray<uint64_t> &tenant_ids,
    common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(check_inner_stat())) {
        LOG_WARN("fail to check inner stat", KR(ret));
      } else if (OB_UNLIKELY(OB_ISNULL(sql_proxy_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mysql proxy ptr is null", KR(ret));
      } else if (OB_FAIL(sql.assign_fmt(
              "SELECT tenant_id, min(schema_version) as min_schema_version"
              " FROM %s WHERE tenant_id = %ld group by tenant_id",
              OB_ALL_DDL_TASK_STATUS_TNAME, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
        LOG_WARN("fail to append sql", KR(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_UNLIKELY(OB_ISNULL(result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          int64_t schema_version = OB_INVALID_VERSION;
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "min_schema_version", schema_version, int64_t);
          if (FAILEDx(fill_recycle_schema_versions(
                  tenant_id, schema_version, recycle_schema_versions))) {
            LOG_WARN("fail to fill recycle schema versions",
                KR(ret), K(tenant_id), K(schema_version));
          }
          LOG_INFO("[SCHEMA_RECYCLE] get recycle schema version from __all_ddl_task_status",
              KR(ret), K(tenant_id), K(schema_version));
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

// get recycle schema version by tenant from:
// 1. schema_history_expire_time.
// 2. reserved major version.
// 3. backup reserved schema_version.
int ObSchemaHistoryRecycler::get_recycle_schema_version_by_global_stat(
    const common::ObIArray<uint64_t> &tenant_ids,
    common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    bool is_standby = GCTX.is_standby_cluster();
    if (OB_SUCC(ret)) {
      // step 1. calc by schema_history_expire_time
      int64_t conf_expire_time = GCONF.schema_history_expire_time;
      int64_t expire_time = ObTimeUtility::current_time() - conf_expire_time;
      ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
      if (OB_ISNULL(schema_status_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status_proxy is null", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
        const uint64_t tenant_id = tenant_ids.at(i);
        int64_t expire_schema_version = OB_INVALID_VERSION;
        ObRefreshSchemaStatus schema_status;
        if (!is_standby) {
          schema_status.tenant_id_ = tenant_id;  // use strong read
        } else {
          if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, schema_status))) {
            LOG_WARN("fail to get refresh schema status", KR(ret), K(tenant_id));
          }
        }
        if (FAILEDx(schema_service_->get_schema_version_by_timestamp(
                    schema_status, tenant_id, expire_time, expire_schema_version))) {
          LOG_WARN("fail to get schema version by timestamp",
                   KR(ret), K(schema_status), K(tenant_id), K(expire_time));
        } else if (OB_FAIL(fill_recycle_schema_versions(
                   tenant_id, expire_schema_version, recycle_schema_versions))) {
          LOG_WARN("fail to fill recycle schema versions",
                   KR(ret), K(tenant_id), K(expire_schema_version));
        }
        LOG_INFO("[SCHEMA_RECYCLE] get recycle schema version by timestamp", KR(ret),
                 K(schema_status), K(tenant_id), K(conf_expire_time), K(expire_time),
                 K(expire_schema_version));
      }
    }
    if (OB_SUCC(ret)) {
      // step 2. calc by major version
      const int64_t MIN_RESERVED_MAJOR_SCN_NUM = 2;
      int64_t reserved_num = MIN_RESERVED_MAJOR_SCN_NUM;

      if (OB_FAIL(check_inner_stat())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else {
        SCN spec_frozen_scn;
        for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
          const uint64_t tenant_id = tenant_ids.at(i);
          ObFreezeInfoProxy freeze_info_proxy(tenant_id);
          ObGlobalMergeInfo global_info;
          ObArray<ObFreezeInfo> frozen_status_arr;
          TenantIdAndSchemaVersion schema_version;

          if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*sql_proxy_, tenant_id, global_info))) {
            LOG_WARN("fail to get global merge info", KR(ret), K(tenant_id));
          } else if (!global_info.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid global merge info", KR(ret), K(global_info));
          } else if (OB_FAIL(freeze_info_proxy.get_frozen_info_less_than(*sql_proxy_, global_info.last_merged_scn(),
              frozen_status_arr))) {
            LOG_WARN("fail to get all freeze info", KR(ret), K(global_info));
          } else if (frozen_status_arr.count() < reserved_num + 1) {
            ret = OB_EAGAIN;
            LOG_WARN("not exist enough frozen_scn to reserve", KR(ret), K(reserved_num), K(frozen_status_arr));
          } else if (FALSE_IT(spec_frozen_scn = frozen_status_arr.at(reserved_num).frozen_scn_)) {
          } else if (OB_FAIL(freeze_info_proxy.get_freeze_schema_info(*sql_proxy_, tenant_id,
                     spec_frozen_scn, schema_version))) {
            LOG_WARN("fail to get freeze schema info", KR(ret), K(tenant_id), K(spec_frozen_scn));
          } else if (!schema_version.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema version is invalid", KR(ret), K(tenant_id), K(spec_frozen_scn));
          } else {
            int64_t specific_schema_version = schema_version.schema_version_;
            if (OB_FAIL(fill_recycle_schema_versions(
                tenant_id, specific_schema_version, recycle_schema_versions))) {
              LOG_WARN("fail to fill recycle schema versions",
                       KR(ret), K(tenant_id), K(specific_schema_version));
            }
            LOG_INFO("[SCHEMA_RECYCLE] get recycle schema version by major version",
                     KR(ret), K(tenant_id), K(spec_frozen_scn), K(specific_schema_version));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      // step 4. restore point
      // int64_t schema_version = 0;
      // for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      //   const uint64_t tenant_id = tenant_ids.at(i);
      //   if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      //     FETCH_ENTITY(TENANT_SPACE, tenant_id) {
      //       storage::ObTenantFreezeInfoMgr *freeze_info_mgr = nullptr;
      //       if (OB_ISNULL(freeze_info_mgr = MTL_GET(ObTenantFreezeInfoMgr*))) {
      //         ret = OB_ERR_UNEXPECTED;
      //         LOG_WARN("MTL_GET unexpected null ObTenantFreezeInfoMgr", K(ret), K(tenant_id));
      //       } else if (OB_FAIL(freeze_info_mgr->get_restore_point_min_schema_version(tenant_id, schema_version))) {
      //         LOG_WARN("failed to get restore point min_schema_version", K(ret), K(tenant_id));
      //       } else if (INT64_MAX != schema_version && OB_FAIL(fill_recycle_schema_versions(
      //           tenant_id, schema_version, recycle_schema_versions))) {
      //         LOG_WARN("fail to fill recycle schema versions", KR(ret), K(tenant_id), K(schema_version));
      //       }
      //       LOG_INFO("[SCHEMA_RECYCLE] get recycle schema version by restore point",
      //            KR(ret), K(tenant_id), K(schema_version));
      //     }
      //   }
      // }
    }
  }
  return ret;
}

int ObSchemaHistoryRecycler::update_recycle_schema_versions(
    const common::ObIArray<uint64_t> &tenant_ids,
    common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);

      int64_t old_value = OB_INVALID_VERSION;
      if (OB_FAIL(recycle_schema_versions_.get_refactored(tenant_id, old_value))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get last recycle schema version", KR(ret), K(tenant_id), K(old_value));
        } else {
          old_value = OB_INVALID_VERSION;
          ret = OB_SUCCESS; // overwrite ret
        }
      }

      int64_t new_value = OB_INVALID_VERSION;
      if (FAILEDx(recycle_schema_versions.get_refactored(tenant_id, new_value))) {
        LOG_WARN("fail to get last recycle schema version", KR(ret), K(tenant_id), K(old_value));
      } else if (new_value >= old_value) {
        // can't fallback recycle schema version
        int overwrite = 1;
        if (OB_FAIL(recycle_schema_versions_.set_refactored(tenant_id, new_value, overwrite))) {
          LOG_WARN("fal to set recycle schema version", KR(tenant_id), K(old_value), K(new_value));
        }
      }

      if (OB_SUCC(ret) && is_valid_recycle_schema_version(new_value)) {
        if (new_value > old_value) {
          LOG_INFO("[SCHEMA_RECYCLE] recycle schema version changed",
                   K(tenant_id), K(new_value), K(old_value));
          ROOTSERVICE_EVENT_ADD("schema_recycler", "change_recycle_schema_version",
                                "tenant_id", tenant_id,
                                "recycle_schema_version", new_value,
                                "last_recycle_schema_version", old_value);
        } else if (new_value < old_value) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("[SCHEMA_RECYCLE] can't fallback recycle schema version",
                    KR(ret), K(tenant_id), K(new_value), K(old_value));
        } else {}
      }
    }
  }
  return ret;
}

int ObSchemaHistoryRecycler::fill_recycle_schema_versions(
    const uint64_t tenant_id,
    const int64_t schema_version,
    common::hash::ObHashMap<uint64_t, int64_t> &recycle_schema_versions)
{
  int ret = OB_SUCCESS;
  int64_t store_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(recycle_schema_versions.get_refactored(tenant_id, store_schema_version))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get schema_version", KR(ret), K(tenant_id), K(schema_version));
    } else if (OB_FAIL(recycle_schema_versions.set_refactored(
                       tenant_id, schema_version))) { // overwrite ret
      LOG_WARN("fail to set schema_version", KR(ret), K(tenant_id), K(schema_version));
    }
  } else if (store_schema_version > schema_version) {
    // schema_version which is OB_INVALID_VERSION means fetch recycle schema version error.
    // in such situation, we should not recycle such tenant's schema history in this round.
    int overwrite = 1;
    if (OB_FAIL(recycle_schema_versions.set_refactored(
                tenant_id, schema_version, overwrite))) {
      LOG_WARN("fail to set schema_version", KR(ret), K(tenant_id), K(schema_version));
    }
  }
  return ret;
}


int ObSchemaHistoryRecycler::calc_recycle_schema_versions(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<uint64_t, int64_t> recycle_schema_versions;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(recycle_schema_versions.create(
             hash::cal_next_prime(BUCKET_NUM),
             "RecScheHisTmp", "RecScheHisTmp"))) {
    LOG_WARN("fail to init hashmap", KR(ret));
  } else if (OB_FAIL(get_recycle_schema_version_by_server(
             tenant_ids, recycle_schema_versions))) {
    LOG_WARN("fail to get recycle schema version by server", KR(ret));
  } else if (OB_FAIL(get_recycle_schema_version_for_ddl(tenant_ids, recycle_schema_versions))) {
    LOG_WARN("fail to get recycle schema version for ddl", KR(ret));
  } else if (OB_FAIL(get_recycle_schema_version_by_global_stat(
             tenant_ids, recycle_schema_versions))) {
    LOG_WARN("fail to get recycle schema version by global stat", KR(ret));
  } else if (OB_FAIL(update_recycle_schema_versions(
             tenant_ids, recycle_schema_versions))) {
    LOG_WARN("fail to update recycle schema versions", KR(ret));
  }
  return ret;
}


int ObSchemaHistoryRecycler::try_recycle_schema_history(
    const uint64_t tenant_id,
    const int64_t recycle_schema_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (!is_valid_recycle_schema_version(recycle_schema_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("fail to get recycle schema version", KR(ret), K(tenant_id), K(recycle_schema_version));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else {
    int64_t start_ts = ObTimeUtility::current_time();
    LOG_INFO("[SCHEMA_RECYCLE] recycle schema history by tenant start",
             KR(ret), K(tenant_id), K(recycle_schema_version));
#define RECYCLE_FIRST_SCHEMA(RECYCLE_MODE, SCHEMA_TYPE, TNAME, KEY_NAME) \
  if (OB_SUCC(ret)) { \
    ObRecycleSchemaExecutor::RecycleMode mode = ObRecycleSchemaExecutor::RECYCLE_MODE;\
    ObRecycleSchemaExecutor executor(tenant_id, \
                                     recycle_schema_version, \
                                     TNAME, \
                                     #KEY_NAME, \
                                     mode, \
                                     sql_proxy_, \
                                     this); \
    if (OB_FAIL(executor.execute())) { \
      LOG_WARN("fail to recycle schema history", \
               KR(ret), "type", #SCHEMA_TYPE, \
               K(tenant_id), K(recycle_schema_version)); \
    } \
  }

#define RECYCLE_SECOND_SCHEMA(SCHEMA_TYPE, TNAME, KEY_NAME, SECOND_KEY_NAME) \
  if (OB_SUCC(ret)) { \
    ObSecondRecycleSchemaExecutor executor(tenant_id, \
                                     recycle_schema_version, \
                                     TNAME, \
                                     #KEY_NAME, \
                                     #SECOND_KEY_NAME, \
                                     sql_proxy_, \
                                     this); \
    if (OB_FAIL(executor.execute())) { \
      LOG_WARN("fail to recycle schema history", \
               KR(ret), "type", #SCHEMA_TYPE, \
               K(tenant_id), K(recycle_schema_version)); \
    } \
  }

#define RECYCLE_THIRD_SCHEMA(SCHEMA_TYPE, TNAME, KEY_NAME, SECOND_KEY_NAME, THIRD_KEY_NAME) \
  if (OB_SUCC(ret)) { \
    ObThirdRecycleSchemaExecutor executor(tenant_id, \
                                 recycle_schema_version, \
                                 TNAME, \
                                 #KEY_NAME, \
                                 #SECOND_KEY_NAME, \
                                 #THIRD_KEY_NAME, \
                                 sql_proxy_, \
                                 this); \
    if (OB_FAIL(executor.execute())) { \
      LOG_WARN("fail to recycle schema history", \
               KR(ret), "type", #SCHEMA_TYPE, \
               K(tenant_id), K(recycle_schema_version)); \
    } \
  }

    // TODO:(yanmu.ztl) implement later
    // 1.tenant
    // 2.__all_ddl_operation
    // 3.table_priv/databse_priv
    // 4.udf
    // 5.audit
    // 6.dblink
    // 7.user-role
    // 8.PL
    // 9.ols

    // ---------------------------- table related ------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_ONLY, table, OB_ALL_TABLE_HISTORY_TNAME, table_id);

    RECYCLE_SECOND_SCHEMA(column, OB_ALL_COLUMN_HISTORY_TNAME, table_id, column_id);

    RECYCLE_FIRST_SCHEMA(RECYCLE_ONLY, part_info, OB_ALL_PART_INFO_HISTORY_TNAME, table_id);
    RECYCLE_SECOND_SCHEMA(part, OB_ALL_PART_HISTORY_TNAME, table_id, part_id);
    RECYCLE_SECOND_SCHEMA(def_sub_part, OB_ALL_DEF_SUB_PART_HISTORY_TNAME, table_id, sub_part_id);
    RECYCLE_THIRD_SCHEMA(sub_part, OB_ALL_SUB_PART_HISTORY_TNAME, table_id, part_id, sub_part_id);

    RECYCLE_SECOND_SCHEMA(constraint, OB_ALL_CONSTRAINT_HISTORY_TNAME, table_id, constraint_id);

    RECYCLE_FIRST_SCHEMA(RECYCLE_ONLY, foreign_key, OB_ALL_FOREIGN_KEY_HISTORY_TNAME, foreign_key_id);
    RECYCLE_THIRD_SCHEMA(foreign_key, OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME,
                         foreign_key_id, child_column_id, parent_column_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_ONLY, tablet, OB_ALL_TABLET_TO_TABLE_HISTORY_TNAME, tablet_id);
    ret = OB_SUCCESS; // overwrite ret

    RECYCLE_SECOND_SCHEMA(column_group, OB_ALL_COLUMN_GROUP_HISTORY_TNAME, table_id, column_group_id);
    RECYCLE_SECOND_SCHEMA(column_group_mapping, OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME, table_id, column_group_id);
    ret = OB_SUCCESS; // overwrite ret
    // ----------------------------- database ----------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, database, OB_ALL_DATABASE_HISTORY_TNAME,
                         database_id);
    ret = OB_SUCCESS; // overwrite ret

    // ----------------------------- tablegroup --------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_ONLY, tablegroup, OB_ALL_TABLEGROUP_HISTORY_TNAME, tablegroup_id);
    // tablegroup's partition will be recycled as table's partition
    ret = OB_SUCCESS; // overwrite ret

    // ----------------------------- user/role/proxy ---------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, user, OB_ALL_USER_HISTORY_TNAME, user_id);
    // TODO: should be tested
    //RECYCLE_SECOND_SCHEMA(role_grantee, OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TNAME, grantee_id, role_id);
    RECYCLE_SECOND_SCHEMA(proxy, OB_ALL_USER_PROXY_INFO_HISTORY_TNAME, client_user_id, proxy_user_id);
    RECYCLE_THIRD_SCHEMA(proxy_role, OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TNAME, client_user_id, proxy_user_id, role_id);
    ret = OB_SUCCESS; // overwrite ret

    // ---------------------------- outline ------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, outline, OB_ALL_OUTLINE_HISTORY_TNAME, outline_id);
    ret = OB_SUCCESS; // overwrite ret

    // ---------------------------- synonym ------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, synonym, OB_ALL_SYNONYM_HISTORY_TNAME, synonym_id);
    ret = OB_SUCCESS; // overwrite ret


    // ---------------------------- package/routine ---------------------------------
    /* TODO(yanmu.ztl): package、routine、udt should be tested.
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, package, OB_ALL_PACKAGE_HISTORY_TNAME, package_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, routine, OB_ALL_ROUTINE_HISTORY_TNAME, routine_id);
    RECYCLE_SECOND_SCHEMA(routine, OB_ALL_ROUTINE_PARAM_HISTORY_TNAME, routine_id, sequence);
    ret = OB_SUCCESS; // overwrite ret

    // --------------------------- udt related -------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, udt, OB_ALL_TYPE_HISTORY_TNAME, type_id);
    RECYCLE_SECOND_SCHEMA(udt, OB_ALL_TYPE_ATTR_HISTORY_TNAME, type_id, attribute);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, udt, OB_ALL_COLL_TYPE_HISTORY_TNAME, coll_type_id);
    RECYCLE_SECOND_SCHEMA(udt, OB_ALL_TENANT_OBJECT_TYPE_TNAME, object_type_id, type);
    ret = OB_SUCCESS; // overwrite ret
    */

    // -------------------------- sequence -----------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, sequence, OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME,
                         sequence_id);
    ret = OB_SUCCESS; // overwrite ret

    // -------------------------- system variable ----------------------------------
    // global variable only supports system variable,
    // so system variable schema history is only compressed but not recycled.
    {
      ObSystemVariableRecycleSchemaExecutor executor(tenant_id,
                                                     recycle_schema_version,
                                                     OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
                                                     sql_proxy_,
                                                     this);
      if (OB_FAIL(executor.execute())) { // overwrite ret
        LOG_WARN("fail to recycle schema history",
                 KR(ret), "type", "system_variable",
                 K(tenant_id), K(recycle_schema_version));
      }
      ret = OB_SUCCESS;
    }

    // keystore can't be recycled
    //RECYCLE_FIRST_SCHEMA(NONE, keystore, OB_ALL_TENANT_KEYSTORE_HISTORY_TNAME, keystore_id);

    // ---------------------------- ols ----------------------------------------------------
    /*
    // TODO(yanmu.ztl): should be tested
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, label_se_policy, OB_ALL_TENANT_OLS_POLICY_HISTORY_TNAME,
                         label_se_policy_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, label_se_component, OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TNAME,
                         label_se_component_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, label_se_label, OB_ALL_TENANT_OLS_LABEL_HISTORY_TNAME,
                         label_se_label_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, label_se_user_level, OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TNAME,
                         label_se_user_level_id);
    ret = OB_SUCCESS; // overwrite ret
    */

    // ---------------------------- tablespace --------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, tablespace, OB_ALL_TENANT_TABLESPACE_HISTORY_TNAME,
                         tablespace_id);
    ret = OB_SUCCESS; // overwrite ret

    // ---------------------------- trigger ----------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, trigger, OB_ALL_TENANT_TRIGGER_HISTORY_TNAME,
                         trigger_id);
    ret = OB_SUCCESS; // overwrite ret

    // --------------------------- profile -----------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, profile, OB_ALL_TENANT_PROFILE_HISTORY_TNAME,
                         profile_id);
    ret = OB_SUCCESS; // overwrite ret

    // -------------------------- object priv --------------------------------------------
    // (RECYCLE_AND_COMPRESS)
    // 1. sys object priv history won't be recycle.
    // 2. user object priv history will be recycled and compressed.
    {
      ObObjectPrivRecycleSchemaExecutor executor(tenant_id,
                                                 recycle_schema_version,
                                                 OB_ALL_TENANT_OBJAUTH_HISTORY_TNAME,
                                                 sql_proxy_,
                                                 this);
      if (OB_FAIL(executor.execute())) { // overwrite ret
        LOG_WARN("fail to recycle schema history",
                 KR(ret), "type", "object_priv",
                 K(tenant_id), K(recycle_schema_version));
      }
      ret = OB_SUCCESS;
    }

    // --------------------------- rls ---------------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, rls_policy, OB_ALL_RLS_POLICY_HISTORY_TNAME,
                         rls_policy_id);
    RECYCLE_SECOND_SCHEMA(rls_sec_column, OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TNAME,
                          rls_policy_id, column_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, rls_group, OB_ALL_RLS_GROUP_HISTORY_TNAME,
                         rls_group_id);
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, rls_context, OB_ALL_RLS_CONTEXT_HISTORY_TNAME,
                         rls_context_id);
    ret = OB_SUCCESS; // overwrite ret

    // --------------------------- column priv ---------------------------------------------------
    RECYCLE_FIRST_SCHEMA(RECYCLE_AND_COMPRESS, column_priv, OB_ALL_COLUMN_PRIVILEGE_HISTORY_TNAME,
                         priv_id);
    ret = OB_SUCCESS; // overwrite ret

#undef RECYCLE_FIRST_SCHEMA
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    ROOTSERVICE_EVENT_ADD("schema_recycler", "batch_recycle_by_tenant",
                          "tenant_id", tenant_id,
                          "recycle_schema_version", recycle_schema_version,
                          "cost", cost_ts);
    LOG_INFO("[SCHEMA_RECYCLE] recycle schema history by tenant end",
             KR(ret), K(tenant_id), K(recycle_schema_version), K(cost_ts));
  }
  return ret;
}

ObFirstSchemaKey::ObFirstSchemaKey()
  : first_schema_id_(OB_INVALID_ID)
{
}

ObFirstSchemaKey::~ObFirstSchemaKey()
{
}

bool ObFirstSchemaKey::operator==(const ObFirstSchemaKey &other) const
{
  return first_schema_id_ == other.first_schema_id_;
}

bool ObFirstSchemaKey::operator!=(const ObFirstSchemaKey &other) const
{
  return first_schema_id_ != other.first_schema_id_;
}

ObFirstSchemaKey &ObFirstSchemaKey::operator=(const ObFirstSchemaKey &other)
{
  assign(other);
  return *this;
}

bool ObFirstSchemaKey::operator<(const ObFirstSchemaKey &other) const
{
  return first_schema_id_ < other.first_schema_id_;
}

int ObFirstSchemaKey::assign(const ObFirstSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    first_schema_id_ = other.first_schema_id_;
  }
  return ret;
}

void ObFirstSchemaKey::reset()
{
  first_schema_id_ = OB_INVALID_ID;
}

bool ObFirstSchemaKey::is_valid() const
{
  return first_schema_id_ != OB_INVALID_ID;
}

uint64_t ObFirstSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&first_schema_id_, sizeof(first_schema_id_), hash_val);
  return hash_val;
}

ObSecondSchemaKey::ObSecondSchemaKey()
  : ObFirstSchemaKey(), second_schema_id_(OB_INVALID_ID)
{
}

ObSecondSchemaKey::~ObSecondSchemaKey()
{
}

bool ObSecondSchemaKey::operator==(const ObSecondSchemaKey &other) const
{
  return first_schema_id_ == other.first_schema_id_
         && second_schema_id_ == other.second_schema_id_;
}

bool ObSecondSchemaKey::operator!=(const ObSecondSchemaKey &other) const
{
  return first_schema_id_ != other.first_schema_id_
         || second_schema_id_ != other.second_schema_id_;
}

ObSecondSchemaKey &ObSecondSchemaKey::operator=(const ObSecondSchemaKey &other)
{
  assign(other);
  return *this;
}

bool ObSecondSchemaKey::operator<(const ObSecondSchemaKey &other) const
{
  return first_schema_id_ < other.first_schema_id_
         || (first_schema_id_ == other.first_schema_id_
             && second_schema_id_ < other.second_schema_id_);
}

int ObSecondSchemaKey::assign(const ObSecondSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    first_schema_id_ = other.first_schema_id_;
    second_schema_id_ = other.second_schema_id_;
  }
  return ret;
}

void ObSecondSchemaKey::reset()
{
  first_schema_id_ = common::OB_INVALID_ID;
  second_schema_id_ = common::OB_INVALID_ID;
}

bool ObSecondSchemaKey::is_valid() const
{
  return first_schema_id_ != common::OB_INVALID_ID
         && second_schema_id_ != common::OB_INVALID_ID;
}

uint64_t ObSecondSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&first_schema_id_, sizeof(first_schema_id_), hash_val);
  hash_val = murmurhash(&second_schema_id_, sizeof(second_schema_id_), hash_val);
  return hash_val;
}

ObThirdSchemaKey::ObThirdSchemaKey()
  : ObSecondSchemaKey(),
    third_schema_id_(OB_INVALID_ID)
{
}

ObThirdSchemaKey::~ObThirdSchemaKey()
{
}

bool ObThirdSchemaKey::operator==(const ObThirdSchemaKey &other) const
{
  return first_schema_id_ == other.first_schema_id_
         && second_schema_id_ == other.second_schema_id_
         && third_schema_id_ == other.third_schema_id_;
}

bool ObThirdSchemaKey::operator!=(const ObThirdSchemaKey &other) const
{
  return first_schema_id_ != other.first_schema_id_
         || second_schema_id_ != other.second_schema_id_
         || third_schema_id_ != other.third_schema_id_;
}

ObThirdSchemaKey &ObThirdSchemaKey::operator=(const ObThirdSchemaKey &other)
{
  assign(other);
  return *this;
}

bool ObThirdSchemaKey::operator<(const ObThirdSchemaKey &other) const
{
  return first_schema_id_ < other.first_schema_id_
         || (first_schema_id_ == other.first_schema_id_
             && second_schema_id_ < other.second_schema_id_)
         || (first_schema_id_ == other.first_schema_id_
             && second_schema_id_ == other.second_schema_id_
             && third_schema_id_ < other.third_schema_id_);
}

int ObThirdSchemaKey::assign(const ObThirdSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    first_schema_id_ = other.first_schema_id_;
    second_schema_id_ = other.second_schema_id_;
    third_schema_id_ = other.third_schema_id_;
  }
  return ret;
}

void ObThirdSchemaKey::reset()
{
  first_schema_id_ = common::OB_INVALID_ID;
  second_schema_id_ = common::OB_INVALID_ID;
  third_schema_id_ = common::OB_INVALID_ID;
}

bool ObThirdSchemaKey::is_valid() const
{
  return first_schema_id_ != common::OB_INVALID_ID
         && second_schema_id_ != common::OB_INVALID_ID
         && third_schema_id_ != common::OB_INVALID_ID;
}

uint64_t ObThirdSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&first_schema_id_, sizeof(first_schema_id_), hash_val);
  hash_val = murmurhash(&second_schema_id_, sizeof(second_schema_id_), hash_val);
  hash_val = murmurhash(&third_schema_id_, sizeof(third_schema_id_), hash_val);
  return hash_val;
}

ObIRecycleSchemaExecutor::ObIRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char* table_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler)
    : tenant_id_(tenant_id), schema_version_(schema_version),
      table_name_(table_name), sql_proxy_(sql_proxy), recycler_(recycler)
{
}

ObIRecycleSchemaExecutor::~ObIRecycleSchemaExecutor()
{
}

bool ObIRecycleSchemaExecutor::need_recycle(RecycleMode mode)
{
  return RECYCLE_ONLY == mode || RECYCLE_AND_COMPRESS == mode;
}

bool ObIRecycleSchemaExecutor::need_compress(RecycleMode mode)
{
  return COMPRESS_ONLY == mode || RECYCLE_AND_COMPRESS == mode;
}

int ObIRecycleSchemaExecutor::check_stop()
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(tenant_id), K_(schema_version));
  } else {
    ret = recycler_->check_stop();
  }
  return ret;
}

int ObIRecycleSchemaExecutor::execute()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("[SCHEMA_RECYCLE] recycle schema history by table start",
           K(ret), K_(tenant_id), K_(table_name), K_(schema_version));
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (OB_FAIL(fill_schema_history_map())) {
    LOG_WARN("fail to fill schema history", K(ret), K_(tenant_id), K_(schema_version));
  } else if (OB_FAIL(recycle_schema_history())) {
    LOG_WARN("fail to recycle  schema history", K(ret), K_(tenant_id), K_(schema_version));
  } else if (OB_FAIL(compress_schema_history())) {
    LOG_WARN("fail to compress  schema history", K(ret), K_(tenant_id), K_(schema_version));
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("[SCHEMA_RECYCLE] recycle schema history by table end",
           K(ret), K_(tenant_id), K_(table_name), K_(schema_version), K(cost_ts));
  ROOTSERVICE_EVENT_ADD("schema_recycler", "batch_recycle_by_table",
                        "tenant_id", tenant_id_,
                        "recycle_schema_version", schema_version_,
                        "table_name", table_name_,
                        "cost", cost_ts);
  return ret;
}

ObRecycleSchemaExecutor::ObRecycleSchemaExecutor(
  const uint64_t tenant_id,
  const int64_t schema_version,
  const char* table_name,
  const char* schema_key_name,
  const RecycleMode mode,
  common::ObMySQLProxy *sql_proxy,
  ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_key_name_(schema_key_name), mode_(mode), schema_history_map_()
{
}

ObRecycleSchemaExecutor::~ObRecycleSchemaExecutor()
{
}

bool ObRecycleSchemaExecutor::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_SYS_TENANT_ID == tenant_id_
      || !ObSchemaService::is_formal_version(schema_version_)
      || OB_ISNULL(table_name_)
      || OB_ISNULL(schema_key_name_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(recycler_)
      || NONE == mode_) {
    bret = false;
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(bret), K_(tenant_id), K_(schema_version), K_(mode),
             KP_(table_name), KP_(schema_key_name), KP_(sql_proxy), KP_(recycler));
  }
  return bret;
}

int ObRecycleSchemaExecutor::execute()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("[SCHEMA_RECYCLE] recycle schema history by table start",
           K(ret), K_(tenant_id), K_(table_name), K_(schema_version));
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (OB_FAIL(fill_schema_history_map())) {
    LOG_WARN("fail to fill schema history", K(ret), K_(tenant_id), K_(schema_version));
  } else if (need_recycle(mode_) && OB_FAIL(recycle_schema_history())) {
    LOG_WARN("fail to recycle  schema history", K(ret), K_(tenant_id), K_(schema_version));
  } else if (need_compress(mode_) && OB_FAIL(compress_schema_history())) {
    LOG_WARN("fail to compress  schema history", K(ret), K_(tenant_id), K_(schema_version));
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("[SCHEMA_RECYCLE] recycle schema history by table end",
           K(ret), K_(tenant_id), K_(table_name), K_(schema_version), K(cost_ts));
  ROOTSERVICE_EVENT_ADD("schema_recycler", "batch_recycle_by_table",
                        "tenant_id", tenant_id_,
                        "recycle_schema_version", schema_version_,
                        "table_name", table_name_,
                        "cost", cost_ts);
  return ret;
}

int ObRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("select %s, schema_version, is_deleted from %s "
                                    "where tenant_id = 0 and schema_version <= %ld "
                                    "order by tenant_id, %s, schema_version "
                                    "limit %ld, %ld",
                                    schema_key_name_, table_name_,
                                    schema_version_, schema_key_name_, start_idx,
                                    SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObFirstSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, schema_key_name_, cur_key.first_schema_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
  }
  return ret;
}

#define DEFINE_FILL_SCHEMA_HISTORY_MAP(EXECUTOR, KEY) \
int EXECUTOR::fill_schema_history_map() \
{ \
  int ret = OB_SUCCESS; \
  /*TODO:(yanmu.ztl) double check total cnt*/ \
  if (OB_FAIL(check_stop())) { \
    LOG_WARN("schema history recycler is stopped", K(ret)); \
  } else if (OB_FAIL(schema_history_map_.create(hash::cal_next_prime(BUCKET_NUM), \
                                                "ScheHisRecMap", "ScheHisRecMap"))) { \
    LOG_WARN("fail to create map", K(ret)); \
  } else { \
    KEY key; \
    ObRecycleSchemaValue value; \
    int64_t start_idx = 0; \
    int64_t total_recycle_cnt = 0; \
    while (OB_SUCC(ret)) { \
      ObSqlString sql; \
      SMART_VAR(ObMySQLProxy::MySQLResult, res) { \
        common::sqlclient::ObMySQLResult *result = NULL; \
        int64_t start_ts = ObTimeUtility::current_time(); \
        if (OB_FAIL(check_stop())) { \
          LOG_WARN("schema history recycler is stopped", K(ret)); \
        } else if (OB_FAIL(gen_fill_schema_history_sql(start_idx, sql))) { \
          LOG_WARN("fail to gen sql", K(ret), K_(tenant_id)); \
        } else if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) { \
          LOG_WARN("execute sql failed", K(ret), K_(tenant_id), K(sql)); \
        } else if (NULL == (result = res.get_result())) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("failed to get sql result", K(ret)); \
        } else if (OB_FAIL(result->next())) { \
          if (OB_ITER_END != ret) { \
            LOG_WARN("fail to iter", K(ret)); \
          } \
        } else { \
          int64_t cost_ts = ObTimeUtility::current_time() - start_ts; \
          LOG_INFO("[SCHEMA_RECYCLE] gather recycle schema history", \
                   K(ret), "table_name", table_name_, K(cost_ts), K(sql)); \
          do { \
            KEY cur_key; \
            ObRecycleSchemaValue cur_value; \
            if (OB_FAIL(retrieve_schema_history(*result, cur_key, cur_value))) { \
              LOG_WARN("fail to retrieve schema history", K(ret)); \
            } else if (key.is_valid() && value.is_valid() && cur_key < key) { \
              ret = OB_ERR_UNEXPECTED; \
              LOG_WARN("cur_key is less than last_key", K(ret), K(cur_key), \
                       K(cur_value), K(key), K(value)); \
            } else if (cur_key != key) { \
              /*store last key result*/ \
              if (key.is_valid() && OB_FAIL(fill_schema_history(key, value))) { \
                LOG_WARN("fail to store_schema history", K(ret), K(key), K(value)); \
              } else if (OB_FAIL(fill_schema_history_key(cur_key, key))) { \
                LOG_WARN("fail to fill schema history key", K(ret), K(cur_key)); \
              } else { \
                if (value.is_deleted_) { \
                  total_recycle_cnt += value.record_cnt_; \
                } \
                value.reset(); \
              } \
            } else if (cur_value.max_schema_version_ < value.max_schema_version_) { \
              /*keys are the same, schema_version must be monotonically increasing*/ \
              ret = OB_ERR_UNEXPECTED; \
              LOG_WARN("cur_value is less", K(ret), K(cur_key), \
                       K(cur_value), K(key), K(value)); \
            } \
            if (OB_SUCC(ret)) { \
              value.max_schema_version_ = cur_value.max_schema_version_; \
              value.is_deleted_ = cur_value.is_deleted_; \
              value.record_cnt_++; \
            } \
          } while (OB_SUCC(ret) && OB_SUCC(result->next())); \
          if (OB_ITER_END == ret) { \
            /*overwrite ret*/ \
            ret = OB_SUCCESS; \
            start_idx += SCHEMA_HISTORY_BATCH_FETCH_NUM; \
            if (total_recycle_cnt >= TOTAL_RECYCLE_RECORD_CNT) { \
              /* Because of performance of limit statement */ \
              /* (limit statement may take tens of seconds when row count of table reaches 50 million), */ \
              /* it may take quite a long time to analyze all the schema history. */ \
              /* And schema history recycle may never be executed if error occur when analysis is still running. */ \
              /* So, we recylce history immediately when recycle schema history count reaches threshold. */ \
              ret = OB_ITER_END; \
            } \
          } else { \
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret; \
            LOG_WARN("fail to iter", K(ret)); \
          } \
        } \
      } \
    } \
    if (OB_ITER_END == ret) { \
      ret = OB_SUCCESS; \
    } \
    if (OB_SUCC(ret) && key.is_valid()) { /*add last*/ \
      if (OB_FAIL(fill_schema_history(key, value))) { \
        LOG_WARN("fail to store_schema history", K(ret), K(key), K(value)); \
      } \
    } \
  } \
  return ret; \
}
DEFINE_FILL_SCHEMA_HISTORY_MAP(ObRecycleSchemaExecutor, ObFirstSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_MAP(ObSecondRecycleSchemaExecutor, ObSecondSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_MAP(ObThirdRecycleSchemaExecutor, ObThirdSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_MAP(ObSystemVariableRecycleSchemaExecutor, ObSystemVariableSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_MAP(ObObjectPrivRecycleSchemaExecutor, ObObjectPrivSchemaKey);
#undef DEFINE_FILL_SCHEMA_HISTORY_MAP

#define DEFINE_FILL_SCHEMA_HISTORY_KEY(EXECUTOR, KEY) \
int EXECUTOR::fill_schema_history_key(const KEY &cur_key, KEY &key) \
{ \
  int ret = OB_SUCCESS; \
  key = cur_key; \
  return ret; \
}
DEFINE_FILL_SCHEMA_HISTORY_KEY(ObRecycleSchemaExecutor, ObFirstSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_KEY(ObSecondRecycleSchemaExecutor, ObSecondSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_KEY(ObThirdRecycleSchemaExecutor, ObThirdSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_KEY(ObObjectPrivRecycleSchemaExecutor, ObObjectPrivSchemaKey);
#undef DEFINE_FILL_SCHEMA_HISTORY_KEY

#define DEFINE_FILL_SCHEMA_HISTORY_FUNC(EXECUTOR, KEY) \
int EXECUTOR::fill_schema_history(\
    const KEY &key,\
    const ObRecycleSchemaValue &value)\
{\
  int ret = OB_SUCCESS;\
  if (OB_FAIL(check_stop())) {\
    LOG_WARN("schema history recycler is stopped", K(ret));\
  } else if (!key.is_valid() || !value.is_valid()) {\
    ret = OB_INVALID_ARGUMENT;\
    LOG_WARN("invalid arg", K(ret), K(key), K(value));\
  } else {\
    ObRecycleSchemaValue new_value;\
    ret = schema_history_map_.get_refactored(key, new_value);\
    if (OB_HASH_NOT_EXIST == ret) {\
      ret = OB_SUCCESS;\
      new_value = value;\
    } else if (OB_SUCCESS == ret) {\
      if (new_value.max_schema_version_ >= value.max_schema_version_) {\
        ret = OB_ERR_UNEXPECTED;\
        LOG_WARN("invalid value", K(key), K(new_value), K(value));\
      } else {\
        new_value.max_schema_version_ = value.max_schema_version_;\
      }\
    } else {\
      LOG_WARN("fail to get value", K(ret), K(key));\
    }\
    if (FAILEDx(schema_history_map_.set_refactored(key, new_value))) { \
      LOG_WARN("fail to set new value", K(key), K(new_value)); \
    } else { \
      LOG_DEBUG("key and value", K(ret), K(key), K(new_value)); \
    } \
  } \
  return ret; \
}
DEFINE_FILL_SCHEMA_HISTORY_FUNC(ObRecycleSchemaExecutor, ObFirstSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_FUNC(ObSecondRecycleSchemaExecutor, ObSecondSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_FUNC(ObThirdRecycleSchemaExecutor, ObThirdSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_FUNC(ObSystemVariableRecycleSchemaExecutor, ObSystemVariableSchemaKey);
DEFINE_FILL_SCHEMA_HISTORY_FUNC(ObObjectPrivRecycleSchemaExecutor, ObObjectPrivSchemaKey);
#undef DEFINE_FILL_SCHEMA_HISTORY_FUNC

#define DEFINE_RECYCLE_SCHEMA_HISTORY(EXECUTOR, KEY) \
int EXECUTOR::recycle_schema_history() \
{ \
  int ret = OB_SUCCESS; \
  if (OB_FAIL(check_stop())) { \
    LOG_WARN("schema history recycler is stopped", K(ret)); \
  } else { \
    ObArray<KEY> dropped_schema_keys; \
    int64_t record_cnt = 0; \
    FOREACH_X(it, schema_history_map_, OB_SUCC(ret)) { \
      KEY key = (*it).first; \
      ObRecycleSchemaValue value = (*it).second; \
      if (OB_FAIL(check_stop())) { \
        LOG_WARN("schema history recycler is stopped", K(ret)); \
      } else if (!value.is_deleted_) { \
        /*skip*/ \
      } else if (OB_FAIL(dropped_schema_keys.push_back(key))) { \
        LOG_WARN("fail to push back schema_id", K(ret), K(key), K(value)); \
      } else { \
        LOG_DEBUG("try recycle schema history", \
                  K(ret), "table_name", table_name_, K(key), K(value)); \
        record_cnt += value.record_cnt_; \
        if (record_cnt < BATCH_RECYCLE_RECORD_CNT \
            && dropped_schema_keys.count() < BATCH_SCHEMA_RECYCLE_CNT) { \
          /*skip*/ \
        } else if (OB_FAIL(batch_recycle_schema_history(dropped_schema_keys))) { \
          LOG_WARN("batch recycle schema history failed", K(ret)); \
        } else { \
          LOG_INFO("[SCHEMA_RECYCLE] batch recycle schema history", K(ret), \
                   K_(tenant_id), K_(schema_version), "table_name", table_name_, \
                   "schema_cnt", dropped_schema_keys.count(), K(record_cnt)); \
          dropped_schema_keys.reset(); \
          record_cnt = 0; \
        } \
      } \
    } \
    if (OB_SUCC(ret) && dropped_schema_keys.count() > 0) { \
      if (OB_FAIL(batch_recycle_schema_history(dropped_schema_keys))) { \
        LOG_WARN("batch recycle schema history failed", K(ret)); \
      } else { \
        LOG_INFO("[SCHEMA_RECYCLE] batch recycle schema history", K(ret), \
                 K_(tenant_id), K_(schema_version), "table_name", table_name_, \
                 "schema_cnt", dropped_schema_keys.count(), K(record_cnt)); \
      } \
    } \
  } \
  return ret; \
}
DEFINE_RECYCLE_SCHEMA_HISTORY(ObRecycleSchemaExecutor, ObFirstSchemaKey)
DEFINE_RECYCLE_SCHEMA_HISTORY(ObSecondRecycleSchemaExecutor, ObSecondSchemaKey);
DEFINE_RECYCLE_SCHEMA_HISTORY(ObThirdRecycleSchemaExecutor, ObThirdSchemaKey);
DEFINE_RECYCLE_SCHEMA_HISTORY(ObObjectPrivRecycleSchemaExecutor, ObObjectPrivSchemaKey);
#undef DEFINE_RECYCLE_SCHEMA_HISTORY

int ObRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const ObIArray<ObFirstSchemaKey> &dropped_schema_keys,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (dropped_schema_keys.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where (tenant_id, %s) in ( ",
                                table_name_, schema_key_name_))) {
      LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); i++) {
      const ObFirstSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (0, %ld)", 0 == i ? "" : ",",
                                        key.first_schema_id_))) {
        LOG_WARN("fail to append fmt", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(") and schema_version <= %ld", schema_version_))) {
      LOG_WARN("fail to append fmt", K(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

#define DEFINE_BATCH_RECYCLE_SCHEMA_HISTORY(EXECUTOR, KEY) \
int EXECUTOR::batch_recycle_schema_history( \
    const ObIArray<KEY> &dropped_schema_keys) \
{ \
  int ret = OB_SUCCESS; \
  ObSqlString sql; \
  int64_t affected_rows = 0; \
  if (OB_FAIL(gen_batch_recycle_schema_history_sql(dropped_schema_keys, sql))) { \
    LOG_WARN("fail to gen sql", K(ret)); \
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) { \
    LOG_WARN("fail to execute sql", K(ret), K_(tenant_id), K_(schema_version)); \
  } else if (is_zero_row(affected_rows)) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("affected_rows is zero", K(ret), K_(tenant_id), K_(schema_version)); \
  } else { \
    LOG_INFO("[SCHEMA_RECYCLE] batch_recycle_schema_history", \
             K(ret), K_(tenant_id), K(affected_rows), K(sql)); \
  } \
  return ret; \
}
DEFINE_BATCH_RECYCLE_SCHEMA_HISTORY(ObRecycleSchemaExecutor, ObFirstSchemaKey)
DEFINE_BATCH_RECYCLE_SCHEMA_HISTORY(ObSecondRecycleSchemaExecutor, ObSecondSchemaKey);
DEFINE_BATCH_RECYCLE_SCHEMA_HISTORY(ObThirdRecycleSchemaExecutor, ObThirdSchemaKey);
DEFINE_BATCH_RECYCLE_SCHEMA_HISTORY(ObObjectPrivRecycleSchemaExecutor, ObObjectPrivSchemaKey);
#undef DEFINE_BATCH_RECYCLE_SCHEMA_HISTORY

int ObRecycleSchemaExecutor::gen_batch_compress_schema_history_sql(
    const ObIArray<ObFirstCompressSchemaInfo> &compress_schema_infos,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (compress_schema_infos.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and ( ",
                                table_name_))) {
      LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < compress_schema_infos.count(); i++) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (%s = %ld and schema_version < %ld)",
                                        0 == i ? "" : "or", schema_key_name_,
                                        compress_schema_infos.at(i).key_.first_schema_id_,
                                        compress_schema_infos.at(i).max_schema_version_))) {
        LOG_WARN("fail to append fmt", K(ret), "schema_info", compress_schema_infos.at(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", K(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

#define DEFINE_COMPRESS_SCHEMA_HISTORY(EXECUTOR, KEY, INFO) \
int EXECUTOR::compress_schema_history() \
{ \
  int ret = OB_SUCCESS; \
  if (OB_FAIL(check_stop())) { \
    LOG_WARN("schema history recycler is stopped", K(ret)); \
  } else { \
    ObArray<INFO> compress_schema_infos; \
    int64_t record_cnt = 0; \
    FOREACH_X(it, schema_history_map_, OB_SUCC(ret)) { \
      KEY key = (*it).first; \
      ObRecycleSchemaValue value = (*it).second; \
      INFO info; \
      info.key_ = key; \
      info.max_schema_version_ = value.max_schema_version_; \
      if (OB_FAIL(check_stop())) { \
        LOG_WARN("schema history recycler is stopped", K(ret)); \
      } else if (value.is_deleted_ || value.record_cnt_ < COMPRESS_RECORD_THREHOLD) { \
        /*skip*/ \
      } else if (value.max_schema_version_ > schema_version_) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN("invalid compresss value", K(ret), \
                 K(info), K_(tenant_id), K_(schema_version)); \
      } else if (OB_FAIL(compress_schema_infos.push_back(info))) { \
        LOG_WARN("fail to push back schema_id", K(ret), K(key), K(value)); \
      } else { \
        LOG_INFO("try compress schema history", \
                 K(ret), "table_name", table_name_, K(key), K(value)); \
        record_cnt += value.record_cnt_; \
        if (record_cnt < BATCH_COMPRESS_RECORD_CNT \
            && compress_schema_infos.count() < BATCH_COMPRESS_SCHEMA_CNT) { \
          /*skip*/ \
        } else if (OB_FAIL(batch_compress_schema_history(compress_schema_infos))) { \
          LOG_WARN("batch compress schema history failed", K(ret)); \
        } else { \
          LOG_INFO("[SCHEMA_RECYCLE] batch compress schema history", K(ret), \
                   K_(tenant_id), K_(schema_version), "table_name", table_name_, \
                   "schema_cnt", compress_schema_infos.count(), K(record_cnt)); \
          compress_schema_infos.reset(); \
          record_cnt = 0; \
        } \
      } \
    } \
    if (OB_SUCC(ret) && compress_schema_infos.count() > 0) { \
      if (OB_FAIL(batch_compress_schema_history(compress_schema_infos))) { \
        LOG_WARN("batch compress schema history failed", K(ret)); \
      } else { \
        LOG_INFO("[SCHEMA_RECYCLE] batch compress schema history", K(ret), \
                 K_(tenant_id), K_(schema_version), "table_name", table_name_, \
                 "schema_cnt", compress_schema_infos.count(), K(record_cnt)); \
      } \
    } \
  } \
  return ret; \
}
DEFINE_COMPRESS_SCHEMA_HISTORY(ObRecycleSchemaExecutor,
                               ObFirstSchemaKey,
                               ObFirstCompressSchemaInfo);
DEFINE_COMPRESS_SCHEMA_HISTORY(ObSystemVariableRecycleSchemaExecutor,
                               ObSystemVariableSchemaKey,
                               ObSystemVariableCompressSchemaInfo);
DEFINE_COMPRESS_SCHEMA_HISTORY(ObObjectPrivRecycleSchemaExecutor,
                               ObObjectPrivSchemaKey,
                               ObObjectPrivCompressSchemaInfo);
#undef DEFINE_COMPRESS_SCHEMA_HISTORY

#define BATCH_COMPRESS_SCHEMA_HISTORY(EXECUTOR, INFO) \
int EXECUTOR::batch_compress_schema_history( \
    const ObIArray<INFO> &compress_schema_infos) \
{ \
  int ret = OB_SUCCESS; \
  ObSqlString sql; \
  int64_t affected_rows = 0; \
  if (OB_FAIL(gen_batch_compress_schema_history_sql(compress_schema_infos, sql))) { \
    LOG_WARN("fail to gen sql", K(ret)); \
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) { \
    LOG_WARN("fail to execute sql", K(ret), K_(tenant_id), K_(schema_version)); \
  } else if (is_zero_row(affected_rows)) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("affected_rows is zero", K(ret), K_(tenant_id), K_(schema_version)); \
  } else { \
    LOG_INFO("[SCHEMA_RECYCLE] batch_compress_schema_history", \
             K(ret), K_(tenant_id), K(affected_rows), K(sql)); \
  } \
  return ret; \
}
BATCH_COMPRESS_SCHEMA_HISTORY(ObRecycleSchemaExecutor, ObFirstCompressSchemaInfo);
BATCH_COMPRESS_SCHEMA_HISTORY(ObSystemVariableRecycleSchemaExecutor, ObSystemVariableCompressSchemaInfo);
BATCH_COMPRESS_SCHEMA_HISTORY(ObObjectPrivRecycleSchemaExecutor, ObObjectPrivCompressSchemaInfo);
#undef BATCH_COMPRESS_SCHEMA_HISTORY

ObSecondRecycleSchemaExecutor::ObSecondRecycleSchemaExecutor(
  const uint64_t tenant_id,
  const int64_t schema_version,
  const char* table_name,
  const char* schema_key_name,
  const char* second_schema_key_name,
  common::ObMySQLProxy *sql_proxy,
  ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_key_name_(schema_key_name), second_schema_key_name_(second_schema_key_name),
      schema_history_map_()
{
}

ObSecondRecycleSchemaExecutor::~ObSecondRecycleSchemaExecutor()
{
}

bool ObSecondRecycleSchemaExecutor::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_SYS_TENANT_ID == tenant_id_
      || !ObSchemaService::is_formal_version(schema_version_)
      || OB_ISNULL(table_name_)
      || OB_ISNULL(schema_key_name_)
      || OB_ISNULL(second_schema_key_name_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(recycler_)) {
    bret = false;
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(bret), K_(tenant_id), K_(schema_version),
             KP_(table_name), KP_(schema_key_name), KP_(second_schema_key_name),
             KP_(sql_proxy), KP_(recycler));
  }
  return bret;
}

int ObSecondRecycleSchemaExecutor::compress_schema_history()
{
  int ret = OB_SUCCESS;
  /*do nothing*/
  return ret;
}

int ObSecondRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("select %s, %s, schema_version, is_deleted from %s "
                                    "where tenant_id = 0 and schema_version <= %ld "
                                    "order by tenant_id, %s, %s, schema_version "
                                    "limit %ld, %ld",
                                    schema_key_name_, second_schema_key_name_,
                                    table_name_, schema_version_,
                                    schema_key_name_, second_schema_key_name_,
                                    start_idx, SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObSecondRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObSecondSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, schema_key_name_, cur_key.first_schema_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, second_schema_key_name_,
                            cur_key.second_schema_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
  }
  return ret;
}

int ObSecondRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const ObIArray<ObSecondSchemaKey> &dropped_schema_keys,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (dropped_schema_keys.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where (tenant_id, %s, %s) in ( ",
                                table_name_, schema_key_name_, second_schema_key_name_))) {
      LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); i++) {
      const ObSecondSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (0, %ld, %ld)", 0 == i ? "" : ",",
                                        key.first_schema_id_,
                                        key.second_schema_id_))) {
        LOG_WARN("fail to append fmt", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(") and schema_version <= %ld", schema_version_))) {
      LOG_WARN("fail to append fmt", K(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

ObThirdRecycleSchemaExecutor::ObThirdRecycleSchemaExecutor(
  const uint64_t tenant_id,
  const int64_t schema_version,
  const char* table_name,
  const char* schema_key_name,
  const char* second_schema_key_name,
  const char* third_schema_key_name,
  common::ObMySQLProxy *sql_proxy,
  ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_key_name_(schema_key_name), second_schema_key_name_(second_schema_key_name),
      third_schema_key_name_(third_schema_key_name), schema_history_map_()
{
}

ObThirdRecycleSchemaExecutor::~ObThirdRecycleSchemaExecutor()
{
}

bool ObThirdRecycleSchemaExecutor::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_SYS_TENANT_ID == tenant_id_
      || !ObSchemaService::is_formal_version(schema_version_)
      || OB_ISNULL(table_name_)
      || OB_ISNULL(schema_key_name_)
      || OB_ISNULL(second_schema_key_name_)
      || OB_ISNULL(third_schema_key_name_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(recycler_)) {
    bret = false;
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(bret), K_(tenant_id), K_(schema_version),
             KP_(table_name), KP_(schema_key_name), KP_(second_schema_key_name),
             KP_(third_schema_key_name), KP_(sql_proxy), KP_(recycler));
  }
  return bret;
}

int ObThirdRecycleSchemaExecutor::compress_schema_history()
{
  int ret = OB_SUCCESS;
  /*do nothing*/
  return ret;
}

int ObThirdRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("select %s, %s, %s, schema_version, is_deleted from %s "
                                    "where tenant_id = 0 and schema_version <= %ld "
                                    "order by tenant_id, %s, %s, %s, schema_version "
                                    "limit %ld, %ld",
                                    schema_key_name_, second_schema_key_name_,
                                    third_schema_key_name_, table_name_,
                                    schema_version_, schema_key_name_,
                                    second_schema_key_name_, third_schema_key_name_,
                                    start_idx, SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObThirdRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObThirdSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, schema_key_name_, cur_key.first_schema_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, second_schema_key_name_,
                            cur_key.second_schema_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, third_schema_key_name_,
                            cur_key.third_schema_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
  }
  return ret;
}

int ObThirdRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const ObIArray<ObThirdSchemaKey> &dropped_schema_keys,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (dropped_schema_keys.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where (tenant_id, %s, %s, %s) in ( ",
                                table_name_, schema_key_name_, second_schema_key_name_,
                                third_schema_key_name_))) {
      LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); i++) {
      const ObThirdSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (0, %ld, %ld, %ld)", 0 == i ? "" : ",",
                                        key.first_schema_id_,
                                        key.second_schema_id_,
                                        key.third_schema_id_))) {
        LOG_WARN("fail to append fmt", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(") and schema_version <= %ld", schema_version_))) {
      LOG_WARN("fail to append fmt", K(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

ObSystemVariableSchemaKey::ObSystemVariableSchemaKey()
  : zone_(), name_()
{
}

ObSystemVariableSchemaKey::~ObSystemVariableSchemaKey()
{
}

bool ObSystemVariableSchemaKey::operator==(const ObSystemVariableSchemaKey &other) const
{
  return 0 == zone_.compare(other.zone_) && 0 == name_.compare(other.name_);
}

bool ObSystemVariableSchemaKey::operator!=(const ObSystemVariableSchemaKey &other) const
{
  return 0 != zone_.compare(other.zone_) || 0 != name_.compare(other.name_);
}

bool ObSystemVariableSchemaKey::operator<(const ObSystemVariableSchemaKey &other) const
{
  bool bret = false;
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  int cmp = ObCharset::strcmp(cs_type, zone_, other.zone_);
  if (cmp < 0) {
    bret = true;
  } else if (cmp > 0) {
    bret = false;
  } else if (ObCharset::strcmp(cs_type, name_, other.name_) < 0) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

ObSystemVariableSchemaKey &ObSystemVariableSchemaKey::operator=(const ObSystemVariableSchemaKey &other)
{
  assign(other);
  return *this;
}

// shallow copy
int ObSystemVariableSchemaKey::assign(const ObSystemVariableSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    zone_.assign_ptr(other.zone_.ptr(), other.zone_.length());
    name_.assign_ptr(other.name_.ptr(), other.name_.length());
  }
  return ret;
}

void ObSystemVariableSchemaKey::reset()
{
  zone_.reset();
  name_.reset();
}

bool ObSystemVariableSchemaKey::is_valid() const
{
  return !name_.empty();
}

uint64_t ObSystemVariableSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, zone_, hash_val);
  hash_val = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, name_, hash_val);
  return hash_val;
}

ObSystemVariableRecycleSchemaExecutor::ObSystemVariableRecycleSchemaExecutor(
  const uint64_t tenant_id,
  const int64_t schema_version,
  const char* table_name,
  common::ObMySQLProxy *sql_proxy,
  ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_history_map_(), allocator_()
{
}

ObSystemVariableRecycleSchemaExecutor::~ObSystemVariableRecycleSchemaExecutor()
{
}

bool ObSystemVariableRecycleSchemaExecutor::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_SYS_TENANT_ID == tenant_id_
      || !ObSchemaService::is_formal_version(schema_version_)
      || OB_ISNULL(table_name_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(recycler_)) {
    bret = false;
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(bret), K_(tenant_id), K_(schema_version),
             KP_(table_name), KP_(sql_proxy), KP_(recycler));
  }
  return bret;
}

int ObSystemVariableRecycleSchemaExecutor::recycle_schema_history()
{
  int ret = OB_SUCCESS;
  /*do nothing*/
  return ret;
}

int ObSystemVariableRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("select zone, name, schema_version, is_deleted from %s "
                                    "where tenant_id = 0 and schema_version <= %ld "
                                    "order by tenant_id, zone, name, schema_version "
                                    "limit %ld, %ld",
                                    table_name_, schema_version_,
                                    start_idx, SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObSystemVariableRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObSystemVariableSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else {
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "zone", cur_key.zone_);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "name", cur_key.name_);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
    if (OB_SUCC(ret)) {
      if (cur_key.zone_.empty()) {
        cur_key.zone_.reset();
      }
      if (cur_key.name_.empty()) {
        cur_key.name_.reset();
      }
    }
  }
  return ret;
}

int ObSystemVariableRecycleSchemaExecutor::fill_schema_history_key(
    const ObSystemVariableSchemaKey &cur_key,
    ObSystemVariableSchemaKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator_, cur_key.zone_, key.zone_))) {
    LOG_WARN("fail to write string", K(ret), K(cur_key));
  } else if (OB_FAIL(ob_write_string(allocator_, cur_key.name_, key.name_))) {
    LOG_WARN("fail to write string", K(ret), K(cur_key));
  }
  return ret;
}

int ObSystemVariableRecycleSchemaExecutor::gen_batch_compress_schema_history_sql(
    const ObIArray<ObSystemVariableCompressSchemaInfo> &compress_schema_infos,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", K(ret));
  } else if (compress_schema_infos.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and ( ",
                                table_name_))) {
      LOG_WARN("fail to assign sql", K(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < compress_schema_infos.count(); i++) {
      const ObSystemVariableCompressSchemaInfo &info = compress_schema_infos.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (zone = '%.*s' and name = '%.*s' and schema_version < %ld)",
                                        0 == i ? "" : "or",
                                        info.key_.zone_.length(),
                                        info.key_.zone_.ptr(),
                                        info.key_.name_.length(),
                                        info.key_.name_.ptr(),
                                        info.max_schema_version_))) {
        LOG_WARN("fail to append fmt", K(ret), K(info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", K(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

ObObjectPrivSchemaKey::ObObjectPrivSchemaKey()
  : obj_id_(OB_INVALID_ID),
    obj_type_(OB_INVALID_ID),
    col_id_(OB_INVALID_ID),
    grantor_id_(OB_INVALID_ID),
    grantee_id_(OB_INVALID_ID),
    priv_id_(OB_INVALID_ID)
{
}

ObObjectPrivSchemaKey::~ObObjectPrivSchemaKey()
{
}

bool ObObjectPrivSchemaKey::operator==(const ObObjectPrivSchemaKey &other) const
{
  return obj_id_ == other.obj_id_
         && obj_type_ == other.obj_type_
         && col_id_ == other.col_id_
         && grantor_id_ == other.grantor_id_
         && grantee_id_ == other.grantee_id_
         && priv_id_ == other.priv_id_;
}

bool ObObjectPrivSchemaKey::operator!=(const ObObjectPrivSchemaKey &other) const
{
  return obj_id_ != other.obj_id_
         || obj_type_ != other.obj_type_
         || col_id_ != other.col_id_
         || grantor_id_ != other.grantor_id_
         || grantee_id_ != other.grantee_id_
         || priv_id_ != other.priv_id_;
}

bool ObObjectPrivSchemaKey::operator<(const ObObjectPrivSchemaKey &other) const
{
  bool bret = false;
  if (obj_id_ != other.obj_id_) {
    bret = obj_id_ < other.obj_id_;
  } else if (obj_type_ != other.obj_type_) {
    bret = obj_type_ < other.obj_type_;
  } else if (col_id_ != other.col_id_) {
    bret = col_id_ < other.col_id_;
  } else if (grantor_id_ != other.grantor_id_) {
    bret = grantor_id_ < other.grantor_id_;
  } else if (grantee_id_ != other.grantee_id_) {
    bret = grantee_id_ < other.grantee_id_;
  } else if (priv_id_ != other.priv_id_) {
    bret = priv_id_ < other.priv_id_;
  } else {
    bret = false;
  }
  return bret;
}

ObObjectPrivSchemaKey &ObObjectPrivSchemaKey::operator=(const ObObjectPrivSchemaKey &other)
{
  assign(other);
  return *this;
}

int ObObjectPrivSchemaKey::assign(const ObObjectPrivSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    obj_id_ = other.obj_id_;
    obj_type_ = other.obj_type_;
    col_id_ = other.col_id_;
    grantor_id_ = other.grantor_id_;
    grantee_id_ = other.grantee_id_;
    priv_id_ = other.priv_id_;
  }
  return ret;
}

void ObObjectPrivSchemaKey::reset()
{
  obj_id_ = OB_INVALID_ID;
  obj_type_ = OB_INVALID_ID;
  col_id_ = OB_INVALID_ID;
  grantor_id_ = OB_INVALID_ID;
  grantee_id_ = OB_INVALID_ID;
  priv_id_ = OB_INVALID_ID;
}

bool ObObjectPrivSchemaKey::is_valid() const
{
  return obj_id_ != OB_INVALID_ID
         && obj_type_ != OB_INVALID_ID
         && col_id_ != OB_INVALID_ID
         && grantor_id_ != OB_INVALID_ID
         && grantee_id_ != OB_INVALID_ID
         && priv_id_ != OB_INVALID_ID;
}

uint64_t ObObjectPrivSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&obj_id_, sizeof(obj_id_), hash_val);
  hash_val = murmurhash(&obj_type_, sizeof(obj_type_), hash_val);
  hash_val = murmurhash(&col_id_, sizeof(col_id_), hash_val);
  hash_val = murmurhash(&grantor_id_, sizeof(grantor_id_), hash_val);
  hash_val = murmurhash(&grantee_id_, sizeof(grantee_id_), hash_val);
  hash_val = murmurhash(&priv_id_, sizeof(priv_id_), hash_val);
  return hash_val;
}

ObObjectPrivRecycleSchemaExecutor::ObObjectPrivRecycleSchemaExecutor(
  const uint64_t tenant_id,
  const int64_t schema_version,
  const char* table_name,
  common::ObMySQLProxy *sql_proxy,
  ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_history_map_()
{
}

ObObjectPrivRecycleSchemaExecutor::~ObObjectPrivRecycleSchemaExecutor()
{
}

bool ObObjectPrivRecycleSchemaExecutor::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_SYS_TENANT_ID == tenant_id_
      || !ObSchemaService::is_formal_version(schema_version_)
      || OB_ISNULL(table_name_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(recycler_)) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", K(bret), K_(tenant_id), K_(schema_version),
             KP_(table_name), KP_(sql_proxy), KP_(recycler));
  }
  return bret;
}

int ObObjectPrivRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
    "select obj_id, objtype, col_id, grantor_id, grantee_id, priv_id, schema_version, is_deleted "
    "from %s where tenant_id = 0 and schema_version <= %ld "
    "order by obj_id, objtype, col_id, grantor_id, grantee_id, priv_id, schema_version "
    "limit %ld, %ld",
    table_name_, schema_version_,
    start_idx, SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObObjectPrivRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObObjectPrivSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "obj_id",         cur_key.obj_id_,               int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "objtype",        cur_key.obj_type_,             int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "col_id",         cur_key.col_id_,               int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "grantor_id",     cur_key.grantor_id_,           int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "grantee_id",     cur_key.grantee_id_,           int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "priv_id",        cur_key.priv_id_,              int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted",     cur_value.is_deleted_,         bool);
  }
  return ret;
}

int ObObjectPrivRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const common::ObIArray<ObObjectPrivSchemaKey> &dropped_schema_keys,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (dropped_schema_keys.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt(
        " delete from %s where schema_version <= %ld"
        " and (tenant_id, obj_id, objtype, col_id, grantor_id, grantee_id, priv_id) in ( ",
         table_name_, schema_version_))) {
      LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); i++) {
      const ObObjectPrivSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (0, %ld, %ld, %ld, %ld, %ld, %ld)", 0 == i ? "" : ",",
                                        key.obj_id_,
                                        key.obj_type_,
                                        key.col_id_,
                                        key.grantor_id_,
                                        key.grantee_id_,
                                        key.priv_id_))) {
        LOG_WARN("fail to append fmt", KR(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", KR(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

int ObObjectPrivRecycleSchemaExecutor::gen_batch_compress_schema_history_sql(
    const ObIArray<ObObjectPrivCompressSchemaInfo> &compress_schema_infos,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (compress_schema_infos.count() <= 0) {
    // skip
  } else {
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and ( ",
                                table_name_))) {
      LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < compress_schema_infos.count(); i++) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s (obj_id  = %ld "
                                        "and objtype = %ld "
                                        "and col_id  = %ld "
                                        "and grantor_id = %ld "
                                        "and grantee_id = %ld "
                                        "and priv_id = %ld "
                                        "and schema_version < %ld)",
                                        0 == i ? "" : "or",
                                        compress_schema_infos.at(i).key_.obj_id_,
                                        compress_schema_infos.at(i).key_.obj_type_,
                                        compress_schema_infos.at(i).key_.col_id_,
                                        compress_schema_infos.at(i).key_.grantor_id_,
                                        compress_schema_infos.at(i).key_.grantee_id_,
                                        compress_schema_infos.at(i).key_.priv_id_,
                                        compress_schema_infos.at(i).max_schema_version_))) {
        LOG_WARN("fail to append fmt", KR(ret), "schema_info", compress_schema_infos.at(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", KR(ret), K_(tenant_id), K_(schema_version));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
