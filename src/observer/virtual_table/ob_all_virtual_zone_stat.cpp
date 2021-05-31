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
#include "ob_all_virtual_zone_stat.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_zone_table_operation.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace observer {
ObAllVirtualZoneStat::ZoneStat::ZoneStat() : zone_info_(), server_count_(0), resource_pool_count_(0), unit_count_(0)
{}

bool ObAllVirtualZoneStat::ZoneStat::is_valid() const
{
  return zone_info_.is_valid() && server_count_ >= 0 && resource_pool_count_ >= 0 && unit_count_ >= 0;
}

void ObAllVirtualZoneStat::ZoneStat::reset()
{
  zone_info_.reset();
  server_count_ = 0;
  resource_pool_count_ = 0;
  unit_count_ = 0;
}

ObAllVirtualZoneStat::ObAllVirtualZoneStat()
    : inited_(false), zone_stats_(), cluster_name_(), schema_service_(NULL), sql_proxy_(NULL)
{
  cluster_name_.reset();
}
ObAllVirtualZoneStat::~ObAllVirtualZoneStat()
{}

int ObAllVirtualZoneStat::init(share::schema::ObMultiVersionSchemaService& schema_service, ObMySQLProxy* sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sql_proxy));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = sql_proxy;
    cluster_name_.reset();
    inited_ = true;
  }
  return ret;
}

int ObAllVirtualZoneStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_));
  } else if (!start_to_read_) {
    zone_stats_.reset();
    cluster_name_.reset();
    const ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_ZONE_STAT_TID);
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("get_table_schema failed", KT(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(ret));
    } else if (OB_FAIL(get_all_stat())) {
      LOG_WARN("fail to get all_stat");
    } else {
      ObArray<Column> columns;
      for (int64_t i = 0; i < zone_stats_.count(); i++) {
        columns.reuse();
        if (OB_FAIL(get_full_row(table_schema, zone_stats_.at(i), columns))) {
          LOG_WARN("get_full_row failed", "table_schema", *table_schema, K(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("project_row failed", K(columns), K(ret), K_(cur_row));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("add_row failed", K(cur_row_), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }  // end if (!start_to_read_)
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualZoneStat::get_all_stat()
{
  int ret = OB_SUCCESS;
  const bool can_weak_read = GCTX.is_started_and_can_weak_read();
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, can_weak_read);
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret), K(inited_));
    } else if (
        OB_FAIL(sql.assign_fmt(
            // Disable hash group by temporary. (bug #16337196)
            // FIXME : use hash group by again after hash group by optimized.
            "select v2.zone, v2.merge_status as is_merging, v2.status, v3.count as server_count, v5.count as "
            "resource_pool_count,"
            "v4.count as unit_count, v2.cluster, v2.region, v2.idc, v2.zone_type from "
            "(select /*+ NO_USE_HASH_AGGREGATION */ v1.zone as zone, group_concat(v1.merge_status)  as merge_status, "
            "group_concat(v1.status) as status,"
            "group_concat(v1.cluster) as cluster,group_concat(v1.region) as region, group_concat(v1.idc) as idc,"
            "group_concat(v1.zone_type) as zone_type from (select zone, case when name = 'merge_status' then info "
            "else null end as merge_status, case when name = 'status' then info else null end as status, case when "
            "name='idc' then info else null end as idc, case when name = 'zone_type' then info else null end as "
            "zone_type, case when name = 'cluster' then info else null end as cluster, case when name = 'region' "
            "then info else null end as region from __all_zone)v1 group by zone) v2 left join "
            "(select /*+ NO_USE_HASH_AGGREGATION */ zone, count(*) as count from __all_server group by zone) v3 on "
            "v3.zone = v2.zone left join "
            "(select /*+ NO_USE_HASH_AGGREGATION */ zone, count(*) count from __all_unit group by zone) v4 on v4.zone "
            "=v2.zone left join "
            "(select /*+ NO_USE_HASH_AGGREGATION */ zone, count(distinct resource_pool_id) count from __all_unit group "
            "by zone) v5 on v5.zone=v2.zone"))) {
      LOG_WARN("fail to assigh fmt", K(ret), K(sql));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", K(ret), K(sql), K(can_weak_read));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid result", K(ret));
    } else {
      ObString zone;
      ObString status;
      ObString region;
      ObString cluster;
      ObString idc;
      ObString zone_type;
      ObString merge_status;
      ZoneStat zone_stat;
      ObString null_value;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        zone_stat.reset();
        zone.reset();
        status.reset();
        region.reset();
        cluster.reset();
        merge_status.reset();
        idc.reset();
        zone_type.reset();
        EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "cluster", cluster, true, false, null_value);

        if (OB_FAIL(ret)) {
          // nothing todo
        } else if (!cluster.empty()) {
          if (OB_FAIL(cluster_name_.assign(cluster))) {
            LOG_WARN("fail to assign string", K(ret));
          }
        } else {
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "zone", zone, true, false, null_value);
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "zone", zone, true, false, null_value);
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "is_merging", merge_status, true, false, null_value);
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "status", status, true, false, null_value);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "server_count", zone_stat.server_count_, int64_t);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "resource_pool_count", zone_stat.resource_pool_count_, int64_t);
          EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "unit_count", zone_stat.unit_count_, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "region", region, true, false, null_value);
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "idc", idc, true, false, null_value);
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "zone_type", zone_type, true, false, null_value);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(zone_stat.zone_info_.zone_.assign(zone))) {
            LOG_WARN("fail to assign string", K(ret));
          } else if (OB_FAIL(zone_stat.zone_info_.status_.info_.assign(status))) {
            LOG_WARN("fail to assign string", K(ret));
          } else if (OB_FAIL(zone_stat.zone_info_.region_.info_.assign(region))) {
            LOG_WARN("fail to assign string", K(ret));
          } else if (OB_FAIL(zone_stat.zone_info_.idc_.info_.assign(idc))) {
            LOG_WARN("fail to assign string", K(ret));
          } else if (OB_FAIL(zone_stat.zone_info_.merge_status_.info_.assign(merge_status))) {
            LOG_WARN("fail to assign string", K(ret));
          } else if (OB_FAIL(zone_stat.zone_info_.zone_type_.info_.assign(zone_type))) {
            LOG_WARN("fail to assign string", K(ret));
          } else if (OB_FAIL(zone_stats_.push_back(zone_stat))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObAllVirtualZoneStat::get_full_row(const ObTableSchema* table, const ZoneStat& zone_stat, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  char* zone = NULL;
  int32_t n = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (!zone_stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid zone_stat", K(zone_stat), K(ret));
  } else if (NULL == (zone = static_cast<char*>(allocator_->alloc(MAX_ZONE_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc zone buf failed", "size", MAX_ZONE_LENGTH, K(ret));
  } else {
    if (0 > (n = snprintf(zone, MAX_ZONE_LENGTH, "%s", zone_stat.zone_info_.zone_.ptr())) || n >= MAX_ZONE_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN(
          "snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src len", strlen(zone_stat.zone_info_.zone_.ptr()), K(ret));
    } else {
      bool is_merging = !(ObZoneInfo::MERGE_STATUS_IDLE ==
                          ObZoneInfo::get_merge_status(zone_stat.zone_info_.merge_status_.info_.ptr()));
      ADD_COLUMN(set_varchar, table, "zone", zone, columns);
      ADD_COLUMN(set_int, table, "is_merging", is_merging, columns);
      ADD_COLUMN(set_varchar, table, "status", zone_stat.zone_info_.status_.info_.ptr(), columns);
      ADD_COLUMN(set_int, table, "server_count", zone_stat.server_count_, columns);
      ADD_COLUMN(set_int, table, "resource_pool_count", zone_stat.resource_pool_count_, columns);
      ADD_COLUMN(set_int, table, "unit_count", zone_stat.unit_count_, columns);
      ADD_COLUMN(set_varchar, table, "cluster", cluster_name_.ptr(), columns);
      ADD_COLUMN(set_varchar, table, "region", zone_stat.zone_info_.region_.info_.ptr(), columns);
      ADD_COLUMN(set_int, table, "spare1", -1, columns);
      ADD_COLUMN(set_int, table, "spare2", -1, columns);
      ADD_COLUMN(set_int, table, "spare3", -1, columns);
      ADD_COLUMN(set_varchar, table, "spare4", zone_stat.zone_info_.idc_.info_.ptr(), columns);
      ADD_COLUMN(set_varchar, table, "spare5", zone_stat.zone_info_.zone_type_.info_.ptr(), columns);
      ADD_COLUMN(set_varchar, table, "spare6", "", columns);
    }

    if (OB_FAIL(ret)) {
      allocator_->free(zone);
      zone = NULL;
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
