/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/ob_define.h"
#include "lib/profile/ob_trace_id.h"
#include "rootserver/ob_bootstrap.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_ddl_sql_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"

#include <thread>

namespace oceanbase
{
namespace rootserver
{

int batch_create_schema_local(uint64_t tenant_id,
                              ObDDLService &ddl_service,
                              ObIArray<ObTableSchema> &table_schemas,
                              const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  if (begin < 0 || begin >= end || end > table_schemas.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin), K(end), "table count", table_schemas.count());
  } else {
    ObDDLOperator ddl_operator(ddl_service.get_schema_service(), ddl_service.get_sql_proxy());
    ObMySQLTransaction trans(true);
    if (OB_FAIL(trans.start(&ddl_service.get_sql_proxy(), tenant_id))) {
      LOG_WARN("start transaction failed", KR(ret));
    } else {
      for (int64_t idx = begin;idx < end && OB_SUCC(ret); idx++) {
        ObTableSchema &table = table_schemas.at(idx);
        const ObString *ddl_stmt = NULL;
        bool need_sync_schema_version = !(ObSysTableChecker::is_sys_table_index_tid(table.get_table_id()) ||
                                          is_sys_lob_table(table.get_table_id()));
        int64_t start_time = ObTimeUtility::current_time();
        if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt,
                                              need_sync_schema_version,
                                              false))) {
          LOG_WARN("add table schema failed", K(ret),
              "table_id", table.get_table_id(),
              "table_name", table.get_table_name());
        } else {
          int64_t end_time = ObTimeUtility::current_time();
          LOG_INFO("add table schema succeed", K(idx),
              "table_id", table.get_table_id(),
              "table_name", table.get_table_name(), "core_table", is_core_table(table.get_table_id()), "cost", end_time-start_time);
        }
      }
    }
    if (trans.is_started()) {
      const bool is_commit = (OB_SUCCESS == ret);
      int tmp_ret = trans.end(is_commit);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end trans failed", K(tmp_ret), K(is_commit));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      } else {
      }
    }
  }

  const int64_t now = ObTimeUtility::current_time();
  LOG_INFO("batch create schema finish", K(ret), "table_count", end - begin, "total_time_used", now - begin_time);
  //BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int parallel_create_table_schema(uint64_t tenant_id, ObDDLService &ddl_service, ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  int64_t begin = 0;
  int64_t batch_count = table_schemas.count() / 16;
  const int64_t MAX_RETRY_TIMES = 10;
  int64_t finish_cnt = 0;
  std::vector<std::thread> ths;
  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
    if (table_schemas.count() == (i + 1) || (i + 1 - begin) >= batch_count) {
      std::thread th([&, begin, i, cur_trace_id] () {
        int ret = OB_SUCCESS;
        ObCurTraceId::set(*cur_trace_id);
        int64_t retry_times = 1;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(batch_create_schema_local(tenant_id, ddl_service, table_schemas, begin, i + 1))) {
            LOG_WARN("batch create schema failed", K(ret), "table count", i + 1 - begin);
            // bugfix:
            if (retry_times <= MAX_RETRY_TIMES) {
              retry_times++;
              ret = OB_SUCCESS;
              LOG_INFO("schema error while create table, need retry", KR(ret), K(retry_times));
              usleep(1 * 1000 * 1000L); // 1s
            }
          } else {
            ATOMIC_AAF(&finish_cnt, i + 1 - begin);
            break;
          }
        }
        LOG_INFO("worker job", K(begin), K(i), K(i-begin), K(ret));
      });
      ths.push_back(std::move(th));
      if (OB_SUCC(ret)) {
        begin = i + 1;
      }
    }
  }
  for (auto &th : ths) {
    th.join();
  }
  if (finish_cnt != table_schemas.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel_create_table_schema fail", K(finish_cnt), K(table_schemas.count()), K(ret), K(tenant_id));
  }
  return ret;
}

int ObBootstrap::create_all_schema(ObDDLService &ddl_service,
                                   ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("start create all schemas", "table count", table_schemas.count());
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", K(table_schemas), K(ret));
  } else {
    // persist __all_core_table's schema in inner table, which is only used for sys views.
    HEAP_VAR(ObTableSchema, core_table) {
       ObArray<ObTableSchema> tmp_tables;
      if (OB_FAIL(share::ObInnerTableSchema::all_core_table_schema(core_table))) {
        LOG_WARN("fail to construct __all_core_table's schema", KR(ret), K(core_table));
      } else if (OB_FAIL(tmp_tables.push_back(core_table))) {
        LOG_WARN("fail to push back __all_core_table's schema", KR(ret), K(core_table));
      } else if (OB_FAIL(batch_create_schema_local(OB_SYS_TENANT_ID, ddl_service, tmp_tables, 0, 1))) {
        LOG_WARN("fail to create __all_core_table's schema", KR(ret), K(core_table));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parallel_create_table_schema(OB_SYS_TENANT_ID, ddl_service, table_schemas))) {
      LOG_WARN("create_all_schema", K(ret));
    }
  }
  LOG_INFO("end create all schemas", K(ret), "table count", table_schemas.count(),
           "time_used", ObTimeUtility::current_time() - begin_time);
  return ret;
}

/*
int ObDDLService::create_sys_table_schemas(
    ObDDLOperator &ddl_operator,
    ObMySQLTransaction &trans,
    common::ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || tables.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy), KP_(schema_service));
  } else if (OB_FAIL(parallel_create_table_schema(tables.at(0).get_tenant_id(), *this, tables))) {
    LOG_WARN("create_sys_table_schemas", K(ret));
  }
  return ret;
}
*/


} // end rootserver

namespace share
{
namespace schema
{
common::SpinRWLock lock_for_schema_version;
int ObSchemaServiceSQLImpl::gen_new_schema_version(
    uint64_t tenant_id,
    int64_t refreshed_schema_version,
    int64_t &schema_version)
{
  SpinWLockGuard guard(lock_for_schema_version);
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const int64_t version_cnt = 1;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(gen_tenant_new_schema_version_(tenant_id, refreshed_schema_version, version_cnt, schema_version))) {
    LOG_WARN("fail to gen schema version", KR(ret), K(tenant_id), K(refreshed_schema_version));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("new schema version", K(tenant_id), K(schema_version));
  }
  return ret;
}
}
}

} // end oceanbase
