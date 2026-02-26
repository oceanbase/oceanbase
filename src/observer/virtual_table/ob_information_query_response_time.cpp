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

#include "ob_information_query_response_time.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {
ObInfoSchemaQueryResponseTimeTable::ObInfoSchemaQueryResponseTimeTable()
 : ObVirtualTableScannerIterator(), 
 addr_(NULL),
 ipstr_(),
 port_(0),
 time_collector_(),
 utility_iter_(0),
 sql_type_iter_(0)
{
}

ObInfoSchemaQueryResponseTimeTable::~ObInfoSchemaQueryResponseTimeTable()
{
  reset();
}

void ObInfoSchemaQueryResponseTimeTable::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_ = NULL;
  port_ = 0;
  ipstr_.reset();
  start_to_read_ = false;
  time_collector_.flush();
  utility_iter_ = 0;
  sql_type_iter_ = 0;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaQueryResponseTimeTable::set_ip(common::ObAddr* addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObInfoSchemaQueryResponseTimeTable::init(ObIAllocator *allocator, common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_to_read_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret));
  } else {
    allocator_ = allocator;
    addr_ = &addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObInfoSchemaQueryResponseTimeTable::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_ip(addr_))) {
    SERVER_LOG(WARN, "can't get ip", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObInfoSchemaQueryResponseTimeTable::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

bool ObInfoSchemaQueryResponseTimeTable::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    return true;
  }
  return false;
}

int ObInfoSchemaQueryResponseTimeTable::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    if (utility_iter_ == 0 && sql_type_iter_ == 0) {
      observer::ObTenantQueryRespTimeCollector *t_query_resp_time_collector = MTL(observer::ObTenantQueryRespTimeCollector *);
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(t_query_resp_time_collector)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "t_query_resp_time_collector should not be null", K(ret));
      } else if (OB_FAIL(t_query_resp_time_collector->get_sum_value(time_collector_))) {
        SERVER_LOG(WARN, "failed to get sum value",K(ret), K(MTL_ID()));
      } else if (OB_FAIL(process_row_data(row, cells))){
        SERVER_LOG(WARN, "process row data of time collector failed", K(MTL_ID()), K(ret));
      }
    } else if (utility_iter_ == time_collector_.utility().bound_count() && sql_type_iter_ == static_cast<int32_t>(RespTimeSqlType::END) -1 ){
      ret = OB_ITER_END;
    } else {
      if (utility_iter_ == time_collector_.utility().bound_count()) {
        sql_type_iter_ ++;
        utility_iter_ = 0;
      }
      if (OB_FAIL(process_row_data(row, cells))){
        SERVER_LOG(WARN, "process row data of time collector failed", K(MTL_ID()), K(ret));
      } 
    }
  }
  return ret;
}
static inline const char *sql_type_to_string(RespTimeSqlType type)
{
  switch(type)
  {
    case(RespTimeSqlType::select_sql): return "SELECT";
    case(RespTimeSqlType::insert_sql): return "INSERT";
    case(RespTimeSqlType::delete_sql): return "DELETE";
    case(RespTimeSqlType::update_sql): return "UPDATE";
    case(RespTimeSqlType::replace_sql): return "REPLACE";
    case(RespTimeSqlType::commit_sql): return "COMMIT";
    case(RespTimeSqlType::other_sql): return "OTHER";
    case(RespTimeSqlType::tableapi_select_sql): return "TABLEAPI SELECT";
    case(RespTimeSqlType::tableapi_insert_sql): return "TABLEAPI INSERT";
    case(RespTimeSqlType::tableapi_delete_sql): return "TABLEAPI DELETE";
    case(RespTimeSqlType::tableapi_update_sql): return "TABLEAPI UPDATE";
    case(RespTimeSqlType::tableapi_replace_sql): return "TABLEAPI REPLACE";
    case(RespTimeSqlType::tableapi_query_and_mutate_sql): return "TABLEAPI QUERY AND MUTATE";
    case(RespTimeSqlType::tableapi_other_sql): return "TABLEAPI OTHER";
    case(RespTimeSqlType::hbase_scan_sql): return "HBASE SCAN";
    case(RespTimeSqlType::hbase_put_sql): return "HBASE PUT";
    case(RespTimeSqlType::hbase_delete_sql): return "HBASE DELETE";
    case(RespTimeSqlType::hbase_append_sql): return "HBASE APPEND";
    case(RespTimeSqlType::hbase_increment_sql): return "HBASE INCREMENT";
    case(RespTimeSqlType::hbase_check_and_put_sql): return "HBASE CHECK AND PUT";
    case(RespTimeSqlType::hbase_check_and_mutate_sql): return "HBASE CHECK AND MUTATE";
    case(RespTimeSqlType::hbase_check_and_delete_sql): return "HBASE CHECK AND DELETE";
    case(RespTimeSqlType::hbase_hybrid_batch_sql): return "HBASE HYBRID BATCH";
    case(RespTimeSqlType::inner_sql): return "INNER SQL";
    case(RespTimeSqlType::redis_lindex): return "REDIS LINDEX";
    case(RespTimeSqlType::redis_lset): return "REDIS LSET";
    case(RespTimeSqlType::redis_lrange): return "REDIS LRANGE";
    case(RespTimeSqlType::redis_ltrim): return "REDIS LTRIM";
    case(RespTimeSqlType::redis_lpush): return "REDIS LPUSH";
    case(RespTimeSqlType::redis_lpushx): return "REDIS LPUSHX";
    case(RespTimeSqlType::redis_rpush): return "REDIS RPUSH";
    case(RespTimeSqlType::redis_rpushx): return "REDIS RPUSHX";
    case(RespTimeSqlType::redis_lpop): return "REDIS LPOP";
    case(RespTimeSqlType::redis_rpop): return "REDIS RPOP";
    case(RespTimeSqlType::redis_lrem): return "REDIS LREM";
    case(RespTimeSqlType::redis_rpoplpush): return "REDIS RPOPLPUSH";
    case(RespTimeSqlType::redis_linsert): return "REDIS LINSERT";
    case(RespTimeSqlType::redis_llen): return "REDIS LLEN";
    case(RespTimeSqlType::redis_sdiff): return "REDIS SDIFF";
    case(RespTimeSqlType::redis_sdiffstore): return "REDIS SDIFFSTORE";
    case(RespTimeSqlType::redis_sinter): return "REDIS SINTER";
    case(RespTimeSqlType::redis_sinterstore): return "REDIS SINTERSTORE";
    case(RespTimeSqlType::redis_sunion): return "REDIS SUNION";
    case(RespTimeSqlType::redis_sunionstore): return "REDIS SUNIONSTORE";
    case(RespTimeSqlType::redis_sadd): return "REDIS SADD";
    case(RespTimeSqlType::redis_scard): return "REDIS SCARD";
    case(RespTimeSqlType::redis_sismember): return "REDIS SISMEMBER";
    case(RespTimeSqlType::redis_smembers): return "REDIS SMEMBERS";
    case(RespTimeSqlType::redis_smove): return "REDIS SMOVE";
    case(RespTimeSqlType::redis_spop): return "REDIS SPOP";
    case(RespTimeSqlType::redis_srandmember): return "REDIS SRANDMEMBER";
    case(RespTimeSqlType::redis_srem): return "REDIS SREM";
    case(RespTimeSqlType::redis_zadd): return "REDIS ZADD";
    case(RespTimeSqlType::redis_zcard): return "REDIS ZCARD";
    case(RespTimeSqlType::redis_zrem): return "REDIS ZREM";
    case(RespTimeSqlType::redis_zincrby): return "REDIS ZINCRBY";
    case(RespTimeSqlType::redis_zscore): return "REDIS ZSCORE";
    case(RespTimeSqlType::redis_zrank): return "REDIS ZRANK";
    case(RespTimeSqlType::redis_zrevrank): return "REDIS ZREVRANK";
    case(RespTimeSqlType::redis_zrange): return "REDIS ZRANGE";
    case(RespTimeSqlType::redis_zrevrange): return "REDIS ZREVRANGE";
    case(RespTimeSqlType::redis_zremrangebyrank): return "REDIS ZREMRANGEBYRANK";
    case(RespTimeSqlType::redis_zcount): return "REDIS ZCOUNT";
    case(RespTimeSqlType::redis_zrangebyscore): return "REDIS ZRANGEBYSCORE";
    case(RespTimeSqlType::redis_zrevrangebyscore): return "REDIS ZREVRANGEBYSCORE";
    case(RespTimeSqlType::redis_zremrangebyscore): return "REDIS ZREMRANGEBYSCORE";
    case(RespTimeSqlType::redis_zinterstore): return "REDIS ZINTERSTORE";
    case(RespTimeSqlType::redis_zunionstore): return "REDIS ZUNIONSTORE";
    case(RespTimeSqlType::redis_hset): return "REDIS HSET";
    case(RespTimeSqlType::redis_hmset): return "REDIS HMSET";
    case(RespTimeSqlType::redis_hsetnx): return "REDIS HSETNX";
    case(RespTimeSqlType::redis_hget): return "REDIS HGET";
    case(RespTimeSqlType::redis_hmget): return "REDIS HMGET";
    case(RespTimeSqlType::redis_hgetall): return "REDIS HGETALL";
    case(RespTimeSqlType::redis_hvals): return "REDIS HVALS";
    case(RespTimeSqlType::redis_hkeys): return "REDIS HKEYS";
    case(RespTimeSqlType::redis_hexists): return "REDIS HEXISTS";
    case(RespTimeSqlType::redis_hdel): return "REDIS HDEL";
    case(RespTimeSqlType::redis_hincrby): return "REDIS HINCRBY";
    case(RespTimeSqlType::redis_hincrbyfloat): return "REDIS HINCRBYFLOAT";
    case(RespTimeSqlType::redis_hlen): return "REDIS HLEN";
    case(RespTimeSqlType::redis_getset): return "REDIS GETSET";
    case(RespTimeSqlType::redis_setbit): return "REDIS SETBIT";
    case(RespTimeSqlType::redis_incr): return "REDIS INCR";
    case(RespTimeSqlType::redis_incrby): return "REDIS INCRBY";
    case(RespTimeSqlType::redis_decr): return "REDIS DECR";
    case(RespTimeSqlType::redis_decrby): return "REDIS DECRBY";
    case(RespTimeSqlType::redis_append): return "REDIS APPEND";
    case(RespTimeSqlType::redis_bitcount): return "REDIS BITCOUNT";
    case(RespTimeSqlType::redis_get): return "REDIS GET";
    case(RespTimeSqlType::redis_getbit): return "REDIS GETBIT";
    case(RespTimeSqlType::redis_getrange): return "REDIS GETRANGE";
    case(RespTimeSqlType::redis_incrbyfloat): return "REDIS INCRBYFLOAT";
    case(RespTimeSqlType::redis_mget): return "REDIS MGET";
    case(RespTimeSqlType::redis_mset): return "REDIS MSET";
    case(RespTimeSqlType::redis_set): return "REDIS SET";
    case(RespTimeSqlType::redis_psetex): return "REDIS PSETEX";
    case(RespTimeSqlType::redis_setex): return "REDIS SETEX";
    case(RespTimeSqlType::redis_setnx): return "REDIS SETNX";
    case(RespTimeSqlType::redis_setrange): return "REDIS SETRANGE";
    case(RespTimeSqlType::redis_strlen): return "REDIS STRLEN";
    case(RespTimeSqlType::redis_ttl): return "REDIS TTL";
    case(RespTimeSqlType::redis_pttl): return "REDIS PTTL";
    case(RespTimeSqlType::redis_expire): return "REDIS EXPIRE";
    case(RespTimeSqlType::redis_pexpire): return "REDIS PEXPIRE";
    case(RespTimeSqlType::redis_expireat): return "REDIS EXPIREAT";
    case(RespTimeSqlType::redis_pexpireat): return "REDIS PEXPIREAT";
    case(RespTimeSqlType::redis_del): return "REDIS DEL";
    case(RespTimeSqlType::redis_exists): return "REDIS EXISTS";
    case(RespTimeSqlType::redis_type): return "REDIS TYPE";
    case(RespTimeSqlType::redis_persist): return "REDIS PERSIST";
    default: return "";
  }
}
int ObInfoSchemaQueryResponseTimeTable::process_row_data(ObNewRow *&row, ObObj* cells)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  if (OB_SUCC(ret)){
    uint64_t cell_idx = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
      uint64_t col_id = output_column_ids_.at(j);
      switch (col_id){
        case TENANT_ID:{
          cells[cell_idx].set_int(MTL_ID());
          break;
        }
        case SVR_IP: {
          cells[cell_idx].set_varchar(ipstr_);
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[cell_idx].set_int(port_);
          break;
        }
        case QUERY_RESPPONSE_TIME:{
          cells[cell_idx].set_int(time_collector_.utility().bound(utility_iter_));
          break;
        }
         case COUNT: {
          int64_t val = 0;
          if (OB_FAIL(time_collector_.get_count_val(static_cast<RespTimeSqlType>(sql_type_iter_), utility_iter_, val))) {
            SERVER_LOG(WARN, "failed to get count val", K(ret), K(utility_iter_), K(sql_type_iter_));
          } else {
            cells[cell_idx].set_int(val);
          }
          break;
        }
        case TOTAL: {
          int64_t val = 0;
          if (OB_FAIL(time_collector_.get_total_time_val(static_cast<RespTimeSqlType>(sql_type_iter_), utility_iter_, val))) {
            SERVER_LOG(WARN, "failed to get count val", K(ret), K(utility_iter_), K(sql_type_iter_));
          } else {
            cells[cell_idx].set_int(val);
          }
          break;
        }
        case SQL_TYPE: {
          const char* sql_type = sql_type_to_string(static_cast<RespTimeSqlType>(sql_type_iter_));
          if (strcmp(sql_type, "") == 0) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid sql type, please check sql_type_to_string func", K(ret), K(sql_type_iter_), K(sql_type));
          } else {
            cells[cell_idx].set_varchar(sql_type);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
          break;
        }
      }

      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
    utility_iter_++;
  }
  return ret;
}

void ObInfoSchemaQueryResponseTimeTable::release_last_tenant()
{
  time_collector_.flush();
  utility_iter_ = 0;
  sql_type_iter_ = 0;
}

}  // namespace observer
}  // namespace oceanbase
