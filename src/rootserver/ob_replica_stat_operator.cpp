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
#include "ob_replica_stat_operator.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;

ObReplicaStatIterator::ObReplicaStatIterator() : inited_(false), sql_proxy_(NULL), result_(NULL)
{}

ObReplicaStatIterator::~ObReplicaStatIterator()
{
  close();
}

int ObReplicaStatIterator::init(common::ObMySQLProxy& proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    sql_proxy_ = &proxy;
    inited_ = true;
  }
  return ret;
}

int ObReplicaStatIterator::open(uint64_t tenant_id, int64_t replica_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString query_text;
  // the default timeout is short when replica number is large,
  // we need to give an estimation for the timeout value based on the replica number
  // however a upper limit of the timeout value is set to 120s to avoid hanging the balancer thread
  static const int64_t us_per_replica = 100;  // 100 us per replica
  int64_t esti_timeout = std::min(120 * 1000L * 1000L, std::max(10 * 1000L * 1000L, us_per_replica * replica_cnt));
  if (OB_FAIL(query_text.assign_fmt("SELECT /*+ OB_QUERY_TIMEOUT(%ld) */ svr_ip, svr_port, table_id, "
                                    " partition_idx, partition_cnt, sstable_read_rate, "
                                    " sstable_read_bytes_rate, sstable_write_rate, sstable_write_bytes_rate, "
                                    " log_write_rate, log_write_bytes_rate, memtable_bytes, cpu_utime_rate, "
                                    " cpu_stime_rate, net_in_rate, net_in_bytes_rate, net_out_rate, net_out_bytes_rate"
                                    " FROM %s where tenant_id = %lu",
          esti_timeout,
          OB_ALL_VIRTUAL_PARTITION_INFO_TNAME,
          tenant_id))) {
    LOG_WARN("failed to format query text", K(ret), K(tenant_id));
  } else {
    LOG_INFO("query stat", K(query_text));
    if (OB_FAIL(sql_proxy_->read(res_, query_text.ptr()))) {
      LOG_WARN("execute sql failed", K(query_text), K(ret));
    } else if (NULL == (result_ = res_.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql failed", K(query_text), K(ret));
    }
  }
  return ret;
}

int ObReplicaStatIterator::cons_replica_stat(const sqlclient::ObMySQLResult& res, ObReplicaStat& replica_stat)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;  // dummy, used to fill the output argument
  UNUSED(tmp_real_str_len);
  char svr_ip[OB_IP_STR_BUFF] = "";
  int64_t svr_port = 0;
  uint64_t table_id = OB_INVALID_ID;
  int64_t partition_idx = 0;
  // int64_t partition_cnt = 0;
  EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
  EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
  replica_stat.server_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
  EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(res, "partition_idx", partition_idx, int64_t);
  // EXTRACT_INT_FIELD_MYSQL(res, "partition_cnt", partition_cnt, int64_t);
  replica_stat.part_key_.init(table_id, partition_idx, 0 /* partition_cnt */);

  EXTRACT_DOUBLE_FIELD_MYSQL(res, "sstable_read_rate", replica_stat.sstable_read_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "sstable_read_bytes_rate", replica_stat.sstable_read_bytes_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "sstable_write_rate", replica_stat.sstable_write_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "sstable_write_bytes_rate", replica_stat.sstable_write_bytes_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "log_write_rate", replica_stat.log_write_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "log_write_bytes_rate", replica_stat.log_write_bytes_rate_, double);
  EXTRACT_INT_FIELD_MYSQL(res, "memtable_bytes", replica_stat.memtable_bytes_, int64_t);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "cpu_utime_rate", replica_stat.cpu_utime_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "cpu_stime_rate", replica_stat.cpu_stime_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "net_in_rate", replica_stat.net_in_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "net_in_bytes_rate", replica_stat.net_in_bytes_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "net_out_rate", replica_stat.net_out_rate_, double);
  EXTRACT_DOUBLE_FIELD_MYSQL(res, "net_out_bytes_rate", replica_stat.net_out_bytes_rate_, double);
  return ret;
}

int ObReplicaStatIterator::next(ObReplicaStat& replica_stat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("result next failed", K(ret));
    }
  } else if (OB_FAIL(cons_replica_stat(*result_, replica_stat))) {
    LOG_WARN("build replica statistics failed", K(ret));
  }
  return ret;
}

int ObReplicaStatIterator::close()
{
  int ret = OB_SUCCESS;
  if (NULL != result_) {
    ret = result_->close();
  }
  return ret;
}

int ObReplicaStatUpdater::insert_stat(common::ObISQLClient& sql_client, ObReplicaStat& replica_stat)
{
  int ret = OB_SUCCESS;
  char svr_ip[OB_IP_STR_BUFF] = "";
  ObSqlString sql;
  ObAddr& server = replica_stat.server_;
  ObPartitionKey& p = replica_stat.part_key_;
  ObReplicaStat& s = replica_stat;  // shorter
  if (false == server.ip_to_string(svr_ip, sizeof(svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else if (OB_FAIL(sql.assign_fmt(
                 "INSERT INTO %s "
                 "(svr_ip,svr_port,tenant_id,table_id,"
                 "partition_idx,partition_cnt,frozen_version,freeze_status,"
                 "frozen_timestamp,partition_state,merged_version,active_version,"
                 "sstable_read_bytes_rate,sstable_write_rate,sstable_write_bytes_rate,net_out_bytes_rate,"
                 "log_write_rate,log_write_bytes_rate,sstable_read_rate,cpu_utime_rate,"
                 "cpu_stime_rate,net_in_rate,net_in_bytes_rate,net_out_rate,"
                 "memtable_bytes) VALUES ("
                 "'%s', %d, %lu, %lu,"
                 "%ld, %d, '1-0-0', 'COMMIT_SUCCEED',"
                 "0, 'L_WORKING', '1-0-0', '1-0-0',"
                 "%lf, %lf, %lf, %lf,"
                 "%lf, %lf, %lf, %lf,"
                 "%lf, %lf, %lf, %lf,"
                 "%ld)",
                 OB_ALL_VIRTUAL_PARTITION_INFO_TNAME,
                 svr_ip,
                 server.get_port(),
                 p.get_tenant_id(),
                 p.get_table_id(),
                 p.get_partition_id(),
                 p.get_partition_cnt(),
                 s.sstable_read_bytes_rate_,
                 s.sstable_write_rate_,
                 s.sstable_write_bytes_rate_,
                 s.net_out_bytes_rate_,
                 s.log_write_rate_,
                 s.log_write_bytes_rate_,
                 s.sstable_read_rate_,
                 s.cpu_utime_rate_,
                 s.cpu_stime_rate_,
                 s.net_in_rate_,
                 s.net_in_bytes_rate_,
                 s.net_out_rate_,
                 s.memtable_bytes_))) {
    LOG_WARN("fail format query text", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
    } else {
      LOG_TRACE("execute sql success", K(sql));
    }
  }
  return ret;
}

int ObReplicaStatUpdater::migrate_stat(common::ObISQLClient& sql_client, const common::ObPartitionKey& p,
    const common::ObAddr& from_svr, const common::ObAddr& to_svr)
{
  int ret = OB_SUCCESS;
  char from_svr_ip[OB_IP_STR_BUFF] = "";
  char to_svr_ip[OB_IP_STR_BUFF] = "";
  ObSqlString sql;
  if (false == from_svr.ip_to_string(from_svr_ip, sizeof(from_svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(from_svr));
  } else if (false == to_svr.ip_to_string(to_svr_ip, sizeof(to_svr_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(to_svr));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s "
                                    "SET svr_ip = '%s', svr_port = %d  WHERE svr_ip = '%s' AND svr_port = %d AND "
                                    " tenant_id = %lu AND table_id = %lu AND "
                                    " partition_idx = %ld",
                 OB_ALL_VIRTUAL_PARTITION_INFO_TNAME,
                 to_svr_ip,
                 to_svr.get_port(),
                 from_svr_ip,
                 from_svr.get_port(),
                 p.get_tenant_id(),
                 p.get_table_id(),
                 p.get_partition_id()))) {
    LOG_WARN("fail format query text", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (!(is_double_row(affected_rows) || is_single_row(affected_rows) || is_zero_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
    } else {
      // LOG_INFO("execute sql success", K(sql));
    }
  }
  return ret;
}

int ObReplicaStatUpdater::delete_stat(
    common::ObISQLClient& sql_client, const common::ObPartitionKey& part_key, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  UNUSED(sql_client);
  UNUSED(part_key);
  UNUSED(server);
  return ret;
}
