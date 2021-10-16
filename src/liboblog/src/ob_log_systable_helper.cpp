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

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "ob_log_systable_helper.h"

#include "common/ob_role.h"                                    // LEADER
#include "common/ob_partition_key.h"                           // ObPartitionKey

#include "ob_log_config.h"                                     // ObLogConfig, TCONF
#include "share/inner_table/ob_inner_table_schema_constants.h" // OB_***_TNAME
#include "ob_log_utils.h"
#include "share/ob_cluster_version.h"                          // GET_MIN_CLUSTER_VERSION

#define GET_DATA(type, index, val, val_str) \
    do { \
      if (OB_SUCCESS == ret) {\
        bool is_null_value = false; \
        int64_t idx = (index); \
        if (OB_FAIL(get_##type(idx, (val), is_null_value)) || is_null_value) { \
          LOG_ERROR("get " val_str " fail", KR(ret), K(idx), K(is_null_value)); \
          ret = (OB_SUCCESS == ret ? OB_INVALID_DATA : ret); \
        } \
      } \
    } while (0)

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace liboblog
{

bool ISQLStrategy::g_is_replica_type_info_valid = true;

bool is_cluster_version_be_equal_or_greater_than_200_()
{
  bool bool_ret = true;

  // ob version: 2_0_0
  bool_ret = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2000);

  return bool_ret;
}

bool is_cluster_version_be_equal_or_greater_than_220_()
{
  bool bool_ret = true;

  // ob version: 2_2_0
  bool_ret = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200);

  return bool_ret;
}

////////////////////////////////////// QueryClogHistorySQLStrategy /////////////////////////////////
QueryClogHistorySQLStrategy::QueryClogHistorySQLStrategy() :
    inited_(false),
    pkey_(),
    log_id_(OB_INVALID_ID),
    tstamp_(OB_INVALID_TIMESTAMP),
    query_by_log_id_(false)
{
}

QueryClogHistorySQLStrategy::~QueryClogHistorySQLStrategy()
{
  destroy();
}

void QueryClogHistorySQLStrategy::destroy()
{
  inited_ = false;

  pkey_.reset();
  log_id_ = OB_INVALID_ID;
  tstamp_ = OB_INVALID_TIMESTAMP;
  query_by_log_id_ = false;
}

int QueryClogHistorySQLStrategy::init_by_log_id_query(const ObPartitionKey &pkey,
    const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else {
    pkey_ = pkey;
    log_id_ = log_id;
    query_by_log_id_ = true;

    inited_ = true;
  }

  return ret;
}

int QueryClogHistorySQLStrategy::init_by_tstamp_query(const ObPartitionKey &pkey,
    const int64_t tstamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else {
    pkey_ = pkey;
    tstamp_ = tstamp;
    query_by_log_id_ = false;

    inited_ = true;
  }

  return ret;
}

int QueryClogHistorySQLStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Always use __all_virtual_server_log_meta for clusters greater than or equal to version 2_2_0
    bool is_all_virtual_server_log_meta_valid = is_cluster_version_be_equal_or_greater_than_220_();
    const char *table_name = NULL;

    if (is_all_virtual_server_log_meta_valid) {
      table_name = OB_ALL_VIRTUAL_SERVER_LOG_META_TNAME;
    } else {
      table_name = OB_ALL_CLOG_HISTORY_INFO_V2_TNAME;
    }

    if (is_all_virtual_server_log_meta_valid) {
      // After __all_clog_history_info_v2 is split into individual tenants, __all_virtual_server_log_meta is queried to specify the tenant_id, to ensure query efficiency
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT "
          "start_log_id, "
          "end_log_id, "
          "svr_ip, "
          "svr_port "
          "FROM %s "
          "WHERE tenant_id=%lu "
          "AND table_id=%lu "
          "AND partition_idx=%ld "
          "AND partition_cnt=%d ",
          table_name,
          pkey_.get_tenant_id(),
          pkey_.table_id_,
          pkey_.get_partition_id(),
          pkey_.get_partition_cnt()))) {
        LOG_ERROR("build sql fail", KR(ret), K(pos), K(table_name), K(pkey_), "buf_size", mul_statement_buf_len, K(sql_buf));
      }
    } else {
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT "
          "start_log_id, "
          "end_log_id, "
          "svr_ip, "
          "svr_port "
          "FROM %s "
          "WHERE table_id=%lu "
          "AND partition_idx=%ld "
          "AND partition_cnt=%d ",
          table_name,
          pkey_.table_id_,
          pkey_.get_partition_id(),
          pkey_.get_partition_cnt()))) {
        LOG_ERROR("build sql fail", KR(ret), K(pos), K(table_name), K(pkey_), "buf_size", mul_statement_buf_len, K(sql_buf));
      }
    }

   if (OB_SUCC(ret)) {
      // query based on log id
      if (query_by_log_id_) {
        if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
                "AND (%lu<=end_log_id OR %lu=end_log_id) "
                "ORDER BY gmt_create;",
                log_id_,
                OB_INVALID_ID))) {
          LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
        }
      } else {
        // query based on timestamp
        if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
                "AND (%ld<=end_log_timestamp OR %ld=end_log_timestamp) "
                "ORDER BY gmt_create;",
                tstamp_,
                OB_INVALID_TIMESTAMP))) {
          LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
        }
      }
    }
  }

  return ret;
}

////////////////////////////////////// QueryMetaInfo /////////////////////////////////
QueryMetaInfoSQLStrategy::QueryMetaInfoSQLStrategy() :
    inited_(false),
    pkey_(),
    only_query_leader_(false)
{
}

QueryMetaInfoSQLStrategy::~QueryMetaInfoSQLStrategy()
{
  destroy();
}

void QueryMetaInfoSQLStrategy::destroy()
{
  inited_ = false;

  pkey_.reset();
  only_query_leader_ = false;
}

int QueryMetaInfoSQLStrategy::init(const ObPartitionKey &pkey, bool only_query_leader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else {
    pkey_ = pkey;
    only_query_leader_ = only_query_leader;

    inited_ = true;
  }

  return ret;
}

int QueryMetaInfoSQLStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  int64_t leader_role = common::LEADER;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else  {
    // Cluster greater than or equal to version 2_0_0, always use __all_virtual_partition_location
    bool is_partition_location_table_valid = is_cluster_version_be_equal_or_greater_than_200_();
    // Generate different SQL according to version
    bool is_replica_type_info_valid = ATOMIC_LOAD(&g_is_replica_type_info_valid);
    const char *table_name = NULL;

    if (is_partition_location_table_valid) {
      table_name = "__all_virtual_partition_location";
    } else {
      table_name = is_inner_table(pkey_.table_id_) ? "__all_root_table" : "__all_meta_table";
    }

    if (is_replica_type_info_valid) {
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "svr_ip, svr_port, role, replica_type "
          "FROM %s "
          "where tenant_id=%lu AND table_id=%lu AND partition_id=%ld",
          table_name,
          extract_tenant_id(pkey_.table_id_),
          pkey_.table_id_,
          pkey_.get_partition_id()))) {
          LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
      }
    } else {
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "svr_ip, svr_port, role "
          "FROM %s "
          "where tenant_id=%lu AND table_id=%lu AND partition_id=%ld",
          table_name,
          extract_tenant_id(pkey_.table_id_),
          pkey_.table_id_,
          pkey_.get_partition_id()))) {
          LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
      }
    }

    // If only the leader info is queried, add the leader query condition
    if (OB_SUCCESS == ret && only_query_leader_) {
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos, " and role=%ld", leader_role))) {
        LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos, ";"))) {
        LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
      }
    }
  }

  return ret;
}

////////////////////////////////////// QueryAllServerInfo /////////////////////////////////
int QueryAllServerInfoStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "svr_ip, svr_port, status, zone "
          "FROM %s",
          OB_ALL_SERVER_TNAME))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
  } else {
    // succ
  }

  return ret;
}

////////////////////////////////////// QueryAllZoneInfo /////////////////////////////////
int QueryAllZoneInfoStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *region = "region";

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "zone, info "
          "FROM %s "
          "WHERE name = \'%s\'",
          OB_ALL_ZONE_TNAME, region))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
  } else {
    // succ
  }

  return ret;
}

////////////////////////////////////// QueryAllZoneTypeInfo /////////////////////////////////
int QueryAllZoneTypeStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *zone_type = "zone_type";

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
          "zone, info "
          "FROM %s "
          "WHERE name = \'%s\'",
          OB_ALL_ZONE_TNAME, zone_type))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), "buf_size", mul_statement_buf_len, K(sql_buf));
  } else {
    // succ
  }
  return ret;
}

////////////////////////////////////// QueryClusterId /////////////////////////////////
int QueryClusterIdStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *query_sql = "show parameters like 'cluster_id'";

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "%s", query_sql))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), K(query_sql), "buf_size", mul_statement_buf_len, K(sql_buf));
  } else {
    // succ
  }

  return ret;
}

///////////////////////// QueryObserverVersionStrategy /////////////////////////
int QueryObserverVersionStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *query_sql = "select distinct(value) from __all_virtual_sys_parameter_stat where name='min_observer_version';";

  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "%s", query_sql))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), K(query_sql), "buf_size", mul_statement_buf_len, K(sql_buf));
  } else {
    // succ
  }

  return ret;
}

///////////////////////// QueryTimeZoneInfoVersionStrategy/////////////////////////
int QueryTimeZoneInfoVersionStrategy::build_sql_statement(char *sql_buf,
    const int64_t mul_statement_buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const char *query_sql = NULL;
  const bool need_query_tenant_timezone_version = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260);

  if (need_query_tenant_timezone_version) {
    query_sql = "select value from __all_virtual_sys_stat where name='current_timezone_version' and tenant_id=";
  } else {
    query_sql = "select value from __all_zone where name='time_zone_info_version';";
  }
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(mul_statement_buf_len <=0)) {
    LOG_ERROR("invalid argument", K(sql_buf), K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos,
          "%s", query_sql))) {
    LOG_ERROR("build sql fail", KR(ret), K(pos), K(query_sql), "buf_size", mul_statement_buf_len, K(sql_buf));
  } else {
    if (need_query_tenant_timezone_version) {
      if (OB_FAIL(databuff_printf(sql_buf, mul_statement_buf_len, pos, "%lu;", tenant_id_))) {
        LOG_ERROR("build tenant_id sql fail", KR(ret), K(pos), K(query_sql),
            "buf_size", mul_statement_buf_len, K(sql_buf), K(tenant_id_));
      }
    }
  }

  return ret;
}

IObLogSysTableHelper::BatchSQLQuery::BatchSQLQuery() :
    inited_(false),
    enable_multiple_statement_(false),
    mul_statement_buf_(NULL),
    mul_statement_buf_len_(0),
    pos_(0),
    batch_sql_count_(0)
{
  single_statement_buf_[0] = '\0';
}

IObLogSysTableHelper::BatchSQLQuery::~BatchSQLQuery()
{
  destroy();
}

int IObLogSysTableHelper::BatchSQLQuery::init(const int64_t mul_statement_buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(mul_statement_buf_len <= 0)) {
    LOG_ERROR("invalid arguments", K(mul_statement_buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(mul_statement_buf_ = static_cast<char *>(ob_malloc(
            mul_statement_buf_len, ObModIds::OB_LOG_BATCH_SQL_QUERY)))) {
    LOG_ERROR("mul_statement_buf_ is null", K(mul_statement_buf_), K(mul_statement_buf_len));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    enable_multiple_statement_ = true;
    mul_statement_buf_len_ = mul_statement_buf_len;

    pos_ = 0;
    batch_sql_count_ = 0;

    inited_ = true;
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::init(ISQLStrategy *strategy)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(strategy)) {
    LOG_ERROR("strategy is null", K(strategy));
    ret = OB_INVALID_ARGUMENT;
  } else {
    enable_multiple_statement_ = false;
    single_statement_buf_[0] = '\0';

    pos_ = 0;
    batch_sql_count_ = 0;

    inited_ = true;

    if (OB_FAIL(do_sql_aggregate(strategy))) {
      LOG_ERROR("do_sql_aggregate fail", KR(ret));
    } else if (OB_FAIL(init_sql())) {
      LOG_ERROR("init_sql fail", KR(ret));
    } else {
      // succ
    }
  }

  return ret;
}

void IObLogSysTableHelper::BatchSQLQuery::destroy()
{
  MySQLQueryBase::destroy();

  inited_ = false;

  enable_multiple_statement_ = false;
  if (NULL != mul_statement_buf_) {
    ob_free(mul_statement_buf_);
    mul_statement_buf_ = NULL;
  }
  mul_statement_buf_len_ = 0;
  single_statement_buf_[0] = '\0';

  pos_ = 0;
  batch_sql_count_ = 0;
}

void IObLogSysTableHelper::BatchSQLQuery::reset()
{
  MySQLQueryBase::destroy();

  pos_ = 0;
  batch_sql_count_ = 0;
}

int IObLogSysTableHelper::BatchSQLQuery::do_sql_aggregate(ISQLStrategy *strategy)
{
  int ret = OB_SUCCESS;
  int64_t sql_len = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(strategy)) {
    LOG_ERROR("strategy is null", K(strategy));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (enable_multiple_statement_) {
      if (OB_FAIL(strategy->build_sql_statement(mul_statement_buf_ + pos_, mul_statement_buf_len_ - pos_, sql_len))) {
        LOG_ERROR("strategy build_sql_statement fail", KR(ret), K(mul_statement_buf_), K(mul_statement_buf_len_),
            K(pos_), K(sql_len));
      }
    } else {
      if (OB_FAIL(strategy->build_sql_statement(single_statement_buf_, sizeof(single_statement_buf_),
              sql_len))) {
        LOG_ERROR("strategy build_sql_statement fail", KR(ret), K(single_statement_buf_),
            "buf_len", sizeof(single_statement_buf_), K(sql_len));
      }
    }

    if (OB_SUCC(ret)) {
      pos_ += sql_len;
      ++batch_sql_count_;
    }
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::init_sql()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else  {
    if (enable_multiple_statement_) {
      if (OB_FAIL(MySQLQueryBase::init(mul_statement_buf_, pos_))) {
        LOG_ERROR("init MySQLQueryBase fail", KR(ret), K(mul_statement_buf_), K(pos_));
      }
    } else {
      if (OB_FAIL(MySQLQueryBase::init(single_statement_buf_, pos_))) {
        LOG_ERROR("init MySQLQueryBase fail", KR(ret), K(single_statement_buf_), K(pos_));
      }
    }
  }

  return ret;
}

template <typename RecordsType>
int IObLogSysTableHelper::BatchSQLQuery::get_records_tpl_(RecordsType &records, const char *event,
    int64_t &record_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(next_result())) {
    // OB_NEED_RETRY indicates that a retry is required
    LOG_ERROR("next_result fail", KR(ret), K(event), "mysql_error_code", get_mysql_err_code(),
        "mysql_err_msg", get_mysql_err_msg());
  } else {
    record_count = 0;

    while (OB_SUCC(ret)) {
      // fetch next row
      if (OB_FAIL(next_row())) {
        if (OB_ITER_END == ret) {
          // End of iteration
        } else {
          // OB_NEED_RETRY indicates that a retry is required
          LOG_ERROR("get next row fail", KR(ret), K(event),
              "mysql_error_code", get_mysql_err_code(), "mysql_err_msg", get_mysql_err_msg());
        }
      }
      // Parsing data from the rows
      else if (OB_FAIL(parse_record_from_row_(records))) {
        LOG_ERROR("parse records from row data fail", KR(ret), K(event));
      } else {
        record_count++;
      }
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}


int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ClogHistoryRecordArray &records)
{
  int ret = OB_SUCCESS;
  ObString ip_str;
  ClogHistoryRecord record;

  // Store tmp data.
  int64_t port = 0;
  int64_t index = -1;

  index++;
  GET_DATA(uint, index, record.start_log_id_, "start log id");

  index++;
  GET_DATA(uint, index, record.end_log_id_, "end log id");

  index++;
  GET_DATA(varchar, index, ip_str, "server ip");

  index++;
  GET_DATA(int, index, port, "server port");

  if (OB_SUCCESS == ret) {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(record.svr_ip_, sizeof(record.svr_ip_), pos,
        "%.*s", ip_str.length(), ip_str.ptr()))) {
      LOG_ERROR("save ip address fail", K(pos), "size", sizeof(record.svr_ip_), K(ip_str));
    } else {
      record.svr_port_ = static_cast<int32_t>(port);
    }

    if (OB_SUCCESS == ret && OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(ClogHistoryRecordArray& records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryClogHistory", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(MetaRecordArray& records)
{
  int ret = OB_SUCCESS;
  bool is_replica_type_info_valid = ATOMIC_LOAD(&ISQLStrategy::g_is_replica_type_info_valid);
  ObString ip_str;
  MetaRecord record;

  int64_t port = 0;
  int64_t role = 0;
  int64_t replica_type = -1;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, ip_str, "server ip");

  index++;
  GET_DATA(int, index, port, "server port");

  index++;
  GET_DATA(int, index, role, "role");

  // Get replica_type only for high version observer
  if (is_replica_type_info_valid) {
    index++;
    GET_DATA(int, index, replica_type, "replica type");
  }

  if (OB_SUCCESS == ret) {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(record.svr_ip_, sizeof(record.svr_ip_), pos,
        "%.*s", ip_str.length(), ip_str.ptr()))) {
      LOG_ERROR("save ip address fail", KR(ret), K(pos),
          "buf_size", sizeof(record.svr_ip_), K(ip_str));
    } else {
      record.svr_port_ = static_cast<int32_t>(port);
      record.role_ = role;
      if (is_replica_type_info_valid) {
        record.replica_type_ = static_cast<ObReplicaType>(replica_type);
      } else {
        // Low version default REPLICA_TYPE_FULL
        record.replica_type_ = REPLICA_TYPE_FULL;
      }
    }

    if (OB_SUCCESS == ret && OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(MetaRecordArray& records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryMetaTable", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(bool &has_leader, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  MetaRecordArray records;
  MetaRecord rec;
  has_leader = false;
  leader.reset();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  }
  // The query Leader is also based on Meta Table query, just get the first row of Meta Table query result directly
  else if (OB_FAIL(get_records(records))) {
    LOG_ERROR("get records fail while query leader info", KR(ret),
        "mysql_error_code", get_mysql_err_code(),
        "mysql_err_msg", get_mysql_err_msg());
  } else if (records.count() <= 0) {
    // Returning an empty record means there is no leader
    has_leader = false;
    leader.reset();
  }
  // Take only the first record
  else if (OB_FAIL(records.at(0, rec))) {
    LOG_ERROR("get record from array fail", KR(ret), K(records.count()), K(records));
  } else if (OB_UNLIKELY(!common::is_strong_leader(static_cast<common::ObRole>(rec.role_)))) {
    LOG_ERROR("server role is not leader", K(rec));
    ret = OB_ERR_UNEXPECTED;
  } else if (! leader.set_ip_addr(rec.svr_ip_, rec.svr_port_)) {
    LOG_ERROR("set_ip_addr fail", K(leader), K(rec));
    ret = OB_ERR_UNEXPECTED;
  } else {
    has_leader = true;
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(AllServerRecordArray& records)
{
  int ret = OB_SUCCESS;
  ObString ip_str;
  ObString status;
  ObString zone;
  AllServerRecord record;
  int64_t port = 0;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, ip_str, "server ip");

  index++;
  GET_DATA(int, index, port, "server port");

  index++;
  GET_DATA(varchar, index, status, "status");

  index++;
  GET_DATA(varchar, index, zone, "zone");

  if (OB_SUCCESS == ret) {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(record.svr_ip_, sizeof(record.svr_ip_), pos,
        "%.*s", ip_str.length(), ip_str.ptr()))) {
      LOG_ERROR("save ip address fail", KR(ret), K(pos),
          "buf_size", sizeof(record.svr_ip_), K(ip_str));
    } else if (OB_FAIL(share::ObServerStatus::str2display_status(status.ptr(), record.status_))) {
      LOG_ERROR("str2display_status fail", KR(ret), K(status.ptr()), K(status));
    } else if (OB_FAIL(record.zone_.assign(zone.ptr()))) {
      LOG_ERROR("zone assign fail", KR(ret), K(zone.ptr()), K(zone));
    } else {
      record.svr_port_ = static_cast<int32_t>(port);
    }

    LOG_DEBUG("query all server info record", KR(ret), K(record));

    if (OB_SUCCESS == ret && OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(AllServerRecordArray& records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryAllServer", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(AllZoneRecordArray& records)
{
  int ret = OB_SUCCESS;
  ObString zone;
  ObString region;
  AllZoneRecord record;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, zone, "zone");

  index++;
  GET_DATA(varchar, index, region, "region");

  if (OB_SUCCESS == ret) {
    if (OB_FAIL(record.zone_.assign(zone.ptr()))) {
      LOG_ERROR("zone assign fail", KR(ret), K(zone.ptr()), K(zone));
    } else if (OB_FAIL(record.region_.assign(region.ptr()))) {
      LOG_ERROR("region assign fail", KR(ret), K(region.ptr()), K(region));
    }
    LOG_DEBUG("query all zone info record", KR(ret), K(record));

    if (OB_SUCCESS == ret && OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(AllZoneRecordArray& records)
{
  int64_t record_count = 0;
  int ret = get_records_tpl_(records, "QueryAllZone", record_count);

  if (OB_SUCCESS == ret && 0 == record_count) {
    // Query the __all_zone table, if no record is retrieved, the region information is not available and a low version of the observer is connected.
    ret = OB_ITEM_NOT_SETTED;
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(AllZoneTypeRecordArray& records)
{
  int ret = OB_SUCCESS;
  ObString zone;
  ObString zone_type_str;
  AllZoneTypeRecord record;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, zone, "zone");

  index++;
  GET_DATA(varchar, index, zone_type_str, "zone_type");

  if (OB_SUCCESS == ret) {
    if (OB_FAIL(record.zone_.assign(zone.ptr()))) {
      LOG_ERROR("zone assign fail", KR(ret), K(zone.ptr()), K(zone));
    } else {
      record.zone_type_ = str_to_zone_type(zone_type_str.ptr());
    }
    LOG_DEBUG("query all zone info record", KR(ret), K(record));

    if (OB_SUCCESS == ret && OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(AllZoneTypeRecordArray& records)
{
  int64_t record_count = 0;
  int ret = get_records_tpl_(records, "QueryAllZoneType", record_count);

  if (OB_SUCCESS == ret && 0 == record_count) {
    // Query the __all_zone table, if no record is retrieved, the zone_type information is not available and a low version of the observer is connected.
    ret = OB_ITEM_NOT_SETTED;
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ClusterInfo &record)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  const char *column_name = "value";
  // Get  value of column `value`
  // FIXME: it is assumed that show parameters like 'cluster_id' will only return one row, and value must be the value of cluster_id
  if (OB_FAIL(get_column_index(column_name, index))) {
    LOG_ERROR("get_column_index fail", KR(ret), K(column_name), K(index));
  } else {
    GET_DATA(int, index, record.cluster_id_, "value");
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(ClusterInfo &record)
{
  int64_t record_count = 0;
  int ret = get_records_tpl_(record, "QueryClusterInfo", record_count);

  if (OB_SUCCESS == ret && 0 == record_count) {
    LOG_ERROR("no cluster id info records, unexcepted error", K(record_count), K(svr_));
    ret = OB_ITEM_NOT_SETTED;
  }
  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ObServerVersionInfoArray &records)
{
  int ret = OB_SUCCESS;
  ObServerVersionInfo record;
  ObString server_version_str;
  uint64_t server_version;
  int64_t index = -1;

  index++;
  GET_DATA(varchar, index, server_version_str, "value");

  if (OB_FAIL(ObClusterVersion::get_version(server_version_str, server_version))) {
    LOG_ERROR("ObClusterVersion get_version fail", KR(ret), K(server_version_str), K(server_version));
  } else {
    record.server_version_ = server_version;

    if (OB_FAIL(records.push_back(record))) {
      LOG_ERROR("push back record fail", KR(ret), K(record), K(records));
    }
  }

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(ObServerVersionInfoArray& records)
{
  int64_t record_count = 0;
  return get_records_tpl_(records, "QueryClusterVersion", record_count);
}

int IObLogSysTableHelper::BatchSQLQuery::parse_record_from_row_(ObServerTZInfoVersionInfo &record)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;

  index++;
  GET_DATA(int, index, record.timezone_info_version_, "value");

  return ret;
}

int IObLogSysTableHelper::BatchSQLQuery::get_records(ObServerTZInfoVersionInfo &record)
{
  int ret = OB_SUCCESS;
  int64_t record_count = 0;

  ret = get_records_tpl_(record, "QueryTimeZoneInfoVersion", record_count);

  if (0 == record_count) {
    record.is_timezone_info_version_exist_ = false;
  }

  return ret;
}

//////////////////////////////////////// ObLogSysTableHelper ////////////////////////////////////////

ObLogSysTableHelper::ObLogSysTableHelper() :
    inited_(false),
    svr_provider_(NULL),
    max_thread_num_(0),
    mysql_conns_(NULL),
    next_svr_idx_array_(NULL),
    thread_counter_(0)
{}

ObLogSysTableHelper::~ObLogSysTableHelper()
{
  destroy();
}

int ObLogSysTableHelper::init(SvrProvider &svr_provider,
    const int64_t access_systable_helper_thread_num,
    const char *mysql_user,
    const char *mysql_password,
    const char *mysql_db)
{
  int ret = OB_SUCCESS;
  ObLogMySQLConnector *conn_array = NULL;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(access_systable_helper_thread_num <= 0)
      || OB_ISNULL(mysql_user)
      || OB_ISNULL(mysql_password)
      || OB_ISNULL(mysql_db)) {
    LOG_ERROR("invalid arguments", K(access_systable_helper_thread_num), K(mysql_user),
        K(mysql_password), K(mysql_db));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t max_thread_num = access_systable_helper_thread_num;
    int64_t sql_conn_size = static_cast<int64_t>(sizeof(ObLogMySQLConnector) * max_thread_num);
    int64_t next_svr_idx_array_size = static_cast<int64_t>(sizeof(int64_t) * max_thread_num);
    int64_t buf_size = sql_conn_size + next_svr_idx_array_size;
    void *buf = ob_malloc(buf_size, ObModIds::OB_LOG_MYSQL_CONNECTOR);

    if (OB_ISNULL(buf)) {
      LOG_ERROR("alloc buffer fail", K(buf_size), K(max_thread_num));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      conn_array = static_cast<ObLogMySQLConnector *>(buf);
      next_svr_idx_array_ = reinterpret_cast<int64_t *>(static_cast<char *>(buf) + sql_conn_size);
      int64_t svr_count = svr_provider.get_server_count();

      if (svr_count <= 0) {
        svr_count = 1;
      }

      for (int64_t i = 0; i < max_thread_num; ++i) {
        new(conn_array + i) ObLogMySQLConnector();
        // Ensure that requests from different threads are balanced across servers
        next_svr_idx_array_[i] = i % svr_count;
      }

      (void)snprintf(mysql_user_, sizeof(mysql_user_), "%s", mysql_user);
      (void)snprintf(mysql_password_, sizeof(mysql_password_), "%s", mysql_password);
      (void)snprintf(mysql_db_, sizeof(mysql_db_), "%s", mysql_db);

      svr_provider_ = &svr_provider;
      mysql_conns_ = conn_array;
      max_thread_num_ = max_thread_num;
      thread_counter_ = 0;
      inited_ = true;

      LOG_INFO("init systable helper succ", K(mysql_user_), K(mysql_password_), K(mysql_db_),
          K(access_systable_helper_thread_num));
    }
  }

  return ret;
}

void ObLogSysTableHelper::destroy()
{
  inited_ = false;

  if (NULL != mysql_conns_) {
    for (int64_t idx = 0, cnt = max_thread_num_; (idx < cnt); ++idx) {
      mysql_conns_[idx].~ObLogMySQLConnector();
    }

    ob_free(mysql_conns_);
    mysql_conns_ = NULL;
  }

  svr_provider_ = NULL;
  max_thread_num_ = 0;
  thread_counter_ = 0;
  mysql_conns_ = NULL;
  next_svr_idx_array_ = NULL;    // FIXME：next_svr_idx_array_ shares a piece of memory with mysql_conns_ and does not need to be freed here
  mysql_user_[0] = '\0';
  mysql_password_[0] = '\0';
  mysql_db_[0] = '\0';

  LOG_INFO("destroy systable helper succ");
}

int ObLogSysTableHelper::do_query_and_handle_when_query_error_occurred_(BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY != ret) {
      LOG_ERROR("do_query_ fail", KR(ret));
    } else {
      const int err_code = query.get_mysql_err_code();
      const char *mysql_err_msg = NULL;

      if (0 == err_code) {
        // need retry
      } else {
        if (OB_ISNULL(mysql_err_msg = query.get_mysql_err_msg())) {
          LOG_ERROR("mysql_err_msg is null", K(mysql_err_msg));
          ret = OB_ERR_UNEXPECTED;
        } else {
          const char *ptr = NULL;

          if (ER_BAD_FIELD_ERROR == err_code) {
           // handler if column not exist
            ptr = strstr(mysql_err_msg, "replica_type");

            if (NULL == ptr) {
              LOG_ERROR("ptr is null, unexpected column is not found", K(ptr), K(err_code), K(mysql_err_msg));
              ret = OB_NEED_RETRY;
            } else {
              LOG_DEBUG("column info", K(ptr), K(mysql_err_msg));
              handle_column_not_found_when_query_meta_info_();
            }
          } else {
            LOG_ERROR("do query fail", K(err_code), K(mysql_err_msg));
            ret = OB_NEED_RETRY;
          }
        }
      }
    }
  }

  return ret;
}

void ObLogSysTableHelper::handle_column_not_found_when_query_meta_info_()
{
  // The replica_type field is not available and will not be requested in the future
  ATOMIC_STORE(&ISQLStrategy::g_is_replica_type_info_valid, false);
  LOG_INFO("'replica_type' is not availalbe in meta table. would not request 'replica_type'",
      "g_is_replica_type_info_valid", ISQLStrategy::g_is_replica_type_info_valid);
}

int ObLogSysTableHelper::query_with_multiple_statement(BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init_sql())) {
    LOG_ERROR("query init_sql fail", KR(ret));
  } else if (OB_FAIL(do_query_and_handle_when_query_error_occurred_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("query_with_multiple_statement fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogSysTableHelper::query_all_server_info(AllServerRecordArray &records)
{
  int ret = OB_SUCCESS;

  BatchSQLQuery query;
  QueryAllServerInfoStrategy query_server_strategy;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_server_strategy))) {
    LOG_ERROR("init all server info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_all_server_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_all_server_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(records))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_all_server_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_all_server_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_all_server_info", KR(ret), "query_svr", query.get_server(),
      "record_count", records.count(), K(records));

  return ret;
}

int ObLogSysTableHelper::query_all_zone_info(AllZoneRecordArray &records)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryAllZoneInfoStrategy query_zone_strategy;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_zone_strategy))) {
    LOG_ERROR("init all zone info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_all_zone_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_all_zone_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(records))) {
    if (OB_ITEM_NOT_SETTED == ret) {
      LOG_WARN("'region' is not availalbe in __all_zone table. would not request 'region'", KR(ret));
    } else if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_all_zone_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_all_zone_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_all_zone_info", KR(ret), "query_svr", query.get_server(),
      "record_count", records.count(), K(records));

  return ret;
}

int ObLogSysTableHelper::query_all_zone_type(AllZoneTypeRecordArray &records)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryAllZoneTypeStrategy query_zone_type_strategy;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_zone_type_strategy))) {
    LOG_ERROR("init all zone info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_all_zone_type fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_all_zone_type fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(records))) {
    if (OB_ITEM_NOT_SETTED == ret) {
      LOG_WARN("'region' is not availalbe in __all_zone table. would not request 'zone_type'", KR(ret));
    } else if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_all_zone_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_all_zone_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_DEBUG("query_all_zone_type", KR(ret), "query_svr", query.get_server(),
      "record_count", records.count(), K(records));
  return ret;
}

int ObLogSysTableHelper::query_cluster_info(ClusterInfo &record)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryClusterIdStrategy query_cluster_id_strategy;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_cluster_id_strategy))) {
    LOG_ERROR("init cluster info query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_cluster_info fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_cluster_info fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(record))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_cluster_info, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_cluster_info", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  LOG_INFO("query_cluster_info", KR(ret), K(record), "query_svr", query.get_server());

  return ret;
}

int ObLogSysTableHelper::query_cluster_min_observer_version(uint64_t &min_observer_version)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryObserverVersionStrategy query_observer_version_strategy;
  ObServerVersionInfoArray records;
  min_observer_version = OB_INVALID_ID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_observer_version_strategy))) {
    LOG_ERROR("init ObserverVersion query fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_cluster_min_observer_version fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_cluster_min_observer_version fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(records))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_cluster_min_observer_version, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_cluster_min_observer_version", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  }

  if (OB_SUCC(ret)) {
    // Get minimum version of servers in cluster
    for (int64_t idx = 0; OB_SUCC(ret) && idx < records.count(); ++idx) {
      uint64_t server_version = records.at(idx).server_version_;
      if (OB_INVALID_ID == min_observer_version) {
        min_observer_version = server_version;
      } else {
        min_observer_version = std::min(min_observer_version, server_version);
      }
    }
  }

  LOG_INFO("query_cluster_min_observer_version", KR(ret), K(min_observer_version), K(records), "query_svr", query.get_server());

  return ret;
}

int ObLogSysTableHelper::query_timezone_info_version(const uint64_t tenant_id,
    int64_t &timezone_info_version)
{
  int ret = OB_SUCCESS;
  BatchSQLQuery query;
  QueryTimeZoneInfoVersionStrategy query_timezone_info_version_strategy(tenant_id);
  ObServerTZInfoVersionInfo record;
  timezone_info_version = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.init(&query_timezone_info_version_strategy))) {
    LOG_ERROR("init QueryTimeZoneInfoVersionquery fail", KR(ret));
  } else if (OB_FAIL(do_query_(query))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("do query_timezone_info_version fail, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("do query_timezone_info_version fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else if (OB_FAIL(query.get_records(record))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("get_records fail while query_timezone_info_version, need retry", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    } else {
      LOG_ERROR("get_records fail while query_timezone_info_version", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }
  } else {
    if (false == record.is_timezone_info_version_exist_) {
      // 1. 1451 version ob cluster __all_zone table does not have timezone_info_version, query is empty
      // 2. 226 versions of the ob cluster have the timezone table split into tenants, if the tenants are not imported, then the query is not available
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      timezone_info_version = record.timezone_info_version_;
    }
  }

  LOG_INFO("query_timezone_info_version", KR(ret), K(timezone_info_version), K(record), "query_svr", query.get_server());

  return ret;
}

int64_t ObLogSysTableHelper::thread_index_()
{
  static __thread int64_t index = -1;
  return index < 0 ? (index = ATOMIC_FAA(&thread_counter_, 1)) : index;
}

int ObLogSysTableHelper::do_query_(MySQLQueryBase &query)
{
  int ret = OB_SUCCESS;
  int64_t tid = thread_index_();
  static __thread int64_t last_change_server_tstamp = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mysql_conns_) || OB_ISNULL(next_svr_idx_array_)) {
    LOG_ERROR("invalid mysql_conns_ or next_svr_idx_array_", K(mysql_conns_),
        K(next_svr_idx_array_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(svr_provider_)) {
    LOG_ERROR("invalid svr provider or config", K(svr_provider_));
    ret = OB_ERR_UNEXPECTED;
  }
  // Check if the maximum number of threads to access the systable helper is exceeded
  // FIXME: we cache a mysql connector for each access thread.
  // If the number of access threads exceeds the maximum number of threads prepared, an error should be reported.
  else if (OB_UNLIKELY(tid >= max_thread_num_)) {
    LOG_ERROR("thread index is larger than systable helper's max thread number", K(tid),
        K(max_thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool done = false;
    ObLogMySQLConnector &conn = mysql_conns_[tid];
    // Get the next server index of the corresponding Connector connection
    int64_t &next_svr_idx = next_svr_idx_array_[tid];

    // Check if should force a server switch before querying
    // There is currently a periodic connection reset mechanism to avoid exceptions on one server, e.g. if the query result is wrong
    if (need_change_server_(last_change_server_tstamp, conn, tid, next_svr_idx)) {
      if (conn.is_inited()) {
        conn.destroy();
      }
    }

    if (svr_provider_->get_server_count() <= 0) {
      LOG_WARN("no server available to query", K(svr_provider_->get_server_count()));
      ret = OB_NEED_RETRY;
    } else {
      for (int64_t retry_svr_cnt = 0;
          OB_SUCCESS == ret && ! done && retry_svr_cnt <= svr_provider_->get_server_count();
          ++retry_svr_cnt) {
        // init connection
        if (! conn.is_inited()) {
          // get index of next server
          int64_t svr_idx = get_next_svr_idx_(next_svr_idx, svr_provider_->get_server_count());
          // switch to next server
          if (OB_FAIL(change_to_next_server_(svr_idx, conn))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              // The server list has changed, the required server does not exist, and the current query exits.
              LOG_WARN("server list changed, next_svr_idx does not exist. quit current query",
                  K(svr_idx), K(next_svr_idx), K(retry_svr_cnt),
                  "server_count", svr_provider_->get_server_count());
            } else if (OB_NEED_RETRY == ret) {
              LOG_ERROR("connect to server fail, need switch server", KR(ret), K(svr_idx),
                  K(next_svr_idx), K(retry_svr_cnt),
                  "server_count", svr_provider_->get_server_count());
            } else {
              LOG_ERROR("connect to server fail", KR(ret), K(svr_idx), K(next_svr_idx),
                  K(retry_svr_cnt), "server_count", svr_provider_->get_server_count());
            }
          } else {
            last_change_server_tstamp = ObTimeUtility::current_time();
          }
        }

        // execute query
        if (OB_SUCCESS == ret) {
          if (OB_FAIL(conn.query(query))) {
            if (OB_NEED_RETRY == ret) {
              LOG_WARN("query fail on current server, need retry to change server", KR(ret),
                  "cur_server", query.get_server(), K(next_svr_idx), K(retry_svr_cnt),
                  "server_count", svr_provider_->get_server_count(),
                  "mysql_error_code", query.get_mysql_err_code(),
                  "mysql_error_msg", query.get_mysql_err_msg());
            } else {
              LOG_ERROR("query fail", KR(ret), K(retry_svr_cnt), K(next_svr_idx));
            }
          } else {
            done = true;
          }
        }

        // In case of query failure, reset the connection and retry to the next server next time
        if (OB_SUCCESS != ret) {
          if (conn.is_inited()) {
            conn.destroy();
          }
        }

        // Retry next round
        if (OB_NEED_RETRY == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_ENTRY_NOT_EXIST == ret) {
      // The server list has changed
      ret = OB_SUCCESS;
    }

    if ((OB_SUCC(ret)) && !done) {
      ret = OB_NEED_RETRY;
    }
  }

  return ret;
}

bool ObLogSysTableHelper::need_change_server_(
    const int64_t last_change_server_tstamp,
    const ObLogMySQLConnector &conn,
    const int64_t tid,
    const int64_t next_svr_idx)
{
  bool bool_ret = false;
  static const int64_t PRINT_CONN_SERVER_INTERVAL = 10 * _SEC_;
  const int64_t sql_server_change_interval = TCONF.sql_server_change_interval_sec * _SEC_;
  int64_t cur_time = ObTimeUtility::current_time();

  if (REACH_TIME_INTERVAL(PRINT_CONN_SERVER_INTERVAL)) {
    LOG_INFO("[STAT] [SYSTABLE_HELPER] [QUERY_SQL_SERVER]", K(tid), "svr", conn.get_server(),
        K(next_svr_idx), "last_change_server_tstamp", TS_TO_STR(last_change_server_tstamp));
  }

  if (cur_time - last_change_server_tstamp >= sql_server_change_interval) {
    bool_ret = true;
    LOG_INFO("[STAT] [SYSTABLE_HELPER] [NEED_CHANGE_SQL_SERVER]", K(tid), "svr", conn.get_server(),
        K(next_svr_idx), "last_change_server_tstamp", TS_TO_STR(last_change_server_tstamp));
  }

  return bool_ret;
}

int64_t ObLogSysTableHelper::get_next_svr_idx_(int64_t &next_svr_idx, const int64_t total_svr_count)
{
  // Get index while advancing index value
  int64_t svr_idx = next_svr_idx++;
  if (svr_idx >= total_svr_count) {
    // Corrected values to avoid overflow
    svr_idx = 0;
    next_svr_idx = 1;
  }
  return svr_idx;
}

int ObLogSysTableHelper::change_to_next_server_(const int64_t svr_idx, ObLogMySQLConnector &conn)
{
  int ret = OB_SUCCESS;
  // update connection
  ObAddr svr;
  MySQLConnConfig conn_config;
  int mysql_connect_timeout_sec = TCONF.mysql_connect_timeout_sec;
  int mysql_query_timeout_sec = TCONF.mysql_query_timeout_sec;
  int64_t cluster_id = OB_INVALID_ID;
  const bool enable_ssl_client_authentication = (1 == TCONF.ssl_client_authentication);

  if (OB_ISNULL(svr_provider_)) {
    LOG_ERROR("invalid svr provider", K(svr_provider_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(svr_provider_->get_server(cluster_id, svr_idx, svr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // exit on server exhaustion
    } else {
      LOG_ERROR("get server from svr provider fail", KR(ret), K(svr_idx),
          K(svr_provider_->get_server_count()));
    }
  } else if (OB_FAIL(conn_config.reset(svr, mysql_user_, mysql_password_, mysql_db_,
      mysql_connect_timeout_sec, mysql_query_timeout_sec))) {
    LOG_ERROR("reset mysql config fail", KR(ret), K(svr), K(mysql_user_), K(mysql_password_),
        K(mysql_db_), K(mysql_connect_timeout_sec), K(mysql_query_timeout_sec));
  } else {
    LOG_INFO("connect to next mysql server", "cur_server", conn.get_server(),
        "next_server", svr, "next_svr_idx", svr_idx,
        "server_count", svr_provider_->get_server_count());

    // destroy connection
    if (conn.is_inited()) {
      conn.destroy();
    }

    if (OB_FAIL(conn.init(conn_config, enable_ssl_client_authentication))) {
      if (OB_NEED_RETRY == ret) {
        LOG_WARN("init mysql connector fail, need retry", KR(ret), K(svr), K(svr_idx),
            K(conn_config), K(enable_ssl_client_authentication));
      } else {
        LOG_ERROR("init mysql connector fail", KR(ret), K(svr), K(svr_idx), K(conn_config));
      }
    } else {
      // connection init success
    }
  }
  return ret;
}

int ObLogSysTableHelper::reset_connection()
{
  int ret = OB_SUCCESS;
  int64_t tid = thread_index_();
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mysql_conns_) || OB_ISNULL(next_svr_idx_array_)) {
    LOG_ERROR("invalid mysql_conns_ or next_svr_idx_array_", K(mysql_conns_),
        K(next_svr_idx_array_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(tid >= max_thread_num_)) {
    LOG_ERROR("thread index is larger than systable helper's max thread number", K(tid),
        K(max_thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObLogMySQLConnector &conn = mysql_conns_[tid];
    const int64_t next_svr_idx = next_svr_idx_array_[tid];

    if (conn.is_inited()) {
      LOG_INFO("reset connection", K(tid), "current_server", conn.get_server(), K(next_svr_idx));
      conn.destroy();
    }
  }

  return ret;
}

}
}
