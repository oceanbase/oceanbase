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

#ifndef _OB_TABLE_RPC_PROCESSOR_UTIL_H
#define _OB_TABLE_RPC_PROCESSOR_UTIL_H 1
#include "lib/stat/ob_diagnose_info.h"
#include "share/table/ob_table.h"
#include "sql/monitor/ob_exec_stat.h"
#include "share/table/ob_table_config_util.h"
#include "observer/table/ob_table_process_type.h"

namespace oceanbase
{
namespace observer
{
#define RECORD_STAT(process_type, stat_prefix, stmt_name)                            \
  case ObTableProccessType::process_type:                                            \
    EVENT_INC(stat_prefix##_COUNT);                                                  \
    EVENT_ADD(stat_prefix##_TIME, elapsed_us);                                       \
    COLLECT_RESPONSE_TIME(                                                           \
        enable_response_time_stats, sql::stmt::stmt_name, process_type, elapsed_us); \
    break;

#define RECORD_STAT_WITH_ROW(process_type, stat_prefix, stmt_name)                   \
  case ObTableProccessType::process_type:                                            \
    EVENT_INC(stat_prefix##_COUNT);                                                  \
    EVENT_ADD(stat_prefix##_TIME, elapsed_us);                                       \
    EVENT_ADD(stat_prefix##_ROW, rows);                                              \
    COLLECT_RESPONSE_TIME(                                                           \
        enable_response_time_stats, sql::stmt::stmt_name, process_type, elapsed_us); \
    break;

#define RECORD_STAT_END()                                                                         \
  default:                                                                                        \
    SERVER_LOG_RET(                                                                               \
        WARN, OB_ERR_UNEXPECTED, "unknow process type", K(process_type), K(elapsed_us), K(rows)); \
    break;

#define SET_AUDIT_SQL_STRING(op_type)                          \
  static const char op_type##_name[] = "table api: " #op_type; \
  audit_record.sql_ = const_cast<char *>(op_type##_name);      \
  audit_record.sql_len_ = sizeof(op_type##_name)

#define COLLECT_RESPONSE_TIME_false(stmt_type, process_type, elapsed_us)
#define COLLECT_RESPONSE_TIME_true(stmt_type, process_type, elapsed_us)              \
  do {                                                                               \
    observer::ObTenantQueryRespTimeCollector *collector =                            \
        MTL(observer::ObTenantQueryRespTimeCollector *);                             \
    if (OB_NOT_NULL(collector)) {                                                    \
      collector->collect(ObTableHistogramType{stmt_type, process_type}, elapsed_us); \
    }                                                                                \
  } while (0);

#define COLLECT_RESPONSE_TIME(enable, stmt_type, process_type, elapsed_us) \
  do {                                                                     \
    if (enable) {                                                          \
      COLLECT_RESPONSE_TIME_true(stmt_type, process_type, elapsed_us);     \
    } else {                                                               \
      COLLECT_RESPONSE_TIME_false(stmt_type, process_type, elapsed_us);    \
    }                                                                      \
  } while (0);

class ObTableRpcProcessorUtil
{
public:
  OB_INLINE static bool is_require_rerouting_err(const int err)
  {
    // rerouting: whether client should refresh location cache and retry
    // Now, following the same logic as in ../mysql/ob_query_retry_ctrl.cpp
    return is_master_changed_error(err)
      || is_server_down_error(err)
      || is_partition_change_error(err)
      || is_server_status_error(err)
      || is_unit_migrate(err)
      || is_transaction_rpc_timeout_err(err)
      || is_has_no_readable_replica_err(err)
      || is_select_dup_follow_replic_err(err)
      || is_trans_stmt_need_retry_error(err)
      || is_snapshot_discarded_err(err);
  }

  OB_INLINE static bool need_do_move_response(const int err, const obrpc::ObRpcPacket &rpc_pkt)
  {
    return is_require_rerouting_err(err)
      && ObKVFeatureModeUitl::is_rerouting_enable()
      && rpc_pkt.require_rerouting();
  }

  OB_INLINE static void record_stat(
      const int32_t process_type,
      int64_t elapsed_us,
      int64_t rows,
      bool enable_response_time_stats)
  {
    switch (process_type) {
      // table single mutate
      RECORD_STAT(TABLE_API_SINGLE_INSERT, TABLEAPI_INSERT, T_KV_INSERT)
      RECORD_STAT(TABLE_API_SINGLE_GET, TABLEAPI_RETRIEVE, T_KV_GET)
      RECORD_STAT(TABLE_API_SINGLE_DELETE, TABLEAPI_DELETE, T_KV_DELETE)
      RECORD_STAT(TABLE_API_SINGLE_UPDATE, TABLEAPI_UPDATE, T_KV_UPDATE)
      RECORD_STAT(TABLE_API_SINGLE_INSERT_OR_UPDATE, TABLEAPI_INSERT_OR_UPDATE, T_KV_INSERT_OR_UPDATE)
      RECORD_STAT(TABLE_API_SINGLE_PUT, TABLEAPI_PUT, T_KV_PUT)
      RECORD_STAT(TABLE_API_SINGLE_REPLACE, TABLEAPI_REPLACE, T_KV_REPLACE)
      RECORD_STAT(TABLE_API_SINGLE_INCREMENT, TABLEAPI_INCREMENT, T_KV_INCREMENT)
      RECORD_STAT(TABLE_API_SINGLE_APPEND, TABLEAPI_APPEND, T_KV_APPEND)

      // table batch mutate
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_INSERT, TABLEAPI_MULTI_INSERT, T_KV_MULTI_INSERT)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_GET, TABLEAPI_MULTI_RETRIEVE, T_KV_MULTI_GET)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_DELETE, TABLEAPI_MULTI_DELETE, T_KV_MULTI_DELETE)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_UPDATE, TABLEAPI_MULTI_UPDATE, T_KV_MULTI_UPDATE)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_INSERT_OR_UPDATE, TABLEAPI_MULTI_INSERT_OR_UPDATE, T_KV_MULTI_INSERT_OR_UPDATE)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_PUT, TABLEAPI_MULTI_PUT, T_KV_MULTI_PUT)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_REPLACE, TABLEAPI_MULTI_REPLACE, T_KV_MULTI_REPLACE)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_INCREMENT, TABLEAPI_MULTI_INCREMENT, T_KV_MULTI_INCREMENT)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_APPEND, TABLEAPI_MULTI_APPEND, T_KV_MULTI_APPEND)
      RECORD_STAT_WITH_ROW(TABLE_API_BATCH_RETRIVE, TABLEAPI_BATCH_RETRIEVE, T_KV_MULTI_GET)
      case ObTableProccessType::TABLE_API_BATCH_HYBRID:
        EVENT_INC(TABLEAPI_BATCH_HYBRID_COUNT);
        EVENT_ADD(TABLEAPI_BATCH_HYBRID_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, rows); // @todo row count for each type
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_OTHER, TABLE_API_BATCH_HYBRID, elapsed_us);
        break;

      // hbase mutate
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_DELETE, HBASEAPI_DELETE, T_HBASE_DELETE)
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_PUT, HBASEAPI_PUT, T_HBASE_PUT)
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_CHECK_AND_DELETE, HBASEAPI_CHECK_DELETE, T_HBASE_CHECK_AND_DELETE)
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_CHECK_AND_PUT, HBASEAPI_CHECK_PUT, T_HBASE_CHECK_AND_PUT)
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_INCREMENT, HBASEAPI_INCREMENT, T_HBASE_INCREMENT)
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_APPEND, HBASEAPI_APPEND, T_HBASE_APPEND)
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_HYBRID, HBASEAPI_HYBRID, T_HBASE_OTHER)

      // table query
      RECORD_STAT_WITH_ROW(TABLE_API_TABLE_QUERY, TABLEAPI_QUERY, T_KV_QUERY)

      // hbase query
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_QUERY, HBASEAPI_SCAN, T_HBASE_SCAN)

      // table query async
      RECORD_STAT_WITH_ROW(TABLE_API_TABLE_QUERY_ASYNC, TABLEAPI_QUERY, T_KV_QUERY)

      // hbase query sync
      RECORD_STAT_WITH_ROW(TABLE_API_HBASE_QUERY_ASYNC, HBASEAPI_SCAN, T_HBASE_SCAN)

      // table query_and_mutate
      RECORD_STAT_WITH_ROW(TABLE_API_QUERY_AND_MUTATE, TABLEAPI_QUERY_AND_MUTATE, T_KV_QUERY_AND_MUTATE)

      // table check_and_insert_up
      RECORD_STAT(TABLE_API_CHECK_AND_INSERT_UP, TABLEAPI_CHECK_AND_INSERT_UP, T_KV_QUERY_AND_MUTATE)
      RECORD_STAT_WITH_ROW(TABLE_API_MULTI_CHECK_AND_INSERT_UP, TABLEAPI_MULTI_CHECK_AND_INSERT_UP, T_KV_QUERY_AND_MUTATE)

      // group trigger
      RECORD_STAT_WITH_ROW(TABLE_API_GROUP_TRIGGER, TABLEAPI_GROUP_TRIGGER, T_KV_OTHER)

      // redis
      RECORD_STAT(TABLE_API_REDIS_LINDEX, REDISAPI_LINDEX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LSET, REDISAPI_LSET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LRANGE, REDISAPI_LRANGE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LTRIM, REDISAPI_LTRIM, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LPUSH, REDISAPI_LPUSH, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LPUSHX, REDISAPI_LPUSHX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_RPUSH, REDISAPI_RPUSH, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_RPUSHX, REDISAPI_RPUSHX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LPOP, REDISAPI_LPOP, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_RPOP, REDISAPI_RPOP, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LREM, REDISAPI_LREM, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_RPOPLPUSH, REDISAPI_RPOPLPUSH, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LINSERT, REDISAPI_LINSERT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_LLEN, REDISAPI_LLEN, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SDIFF, REDISAPI_SDIFF, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SDIFFSTORE, REDISAPI_SDIFFSTORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SINTER, REDISAPI_SINTER, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SINTERSTORE, REDISAPI_SINTERSTORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SUNION, REDISAPI_SUNION, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SUNIONSTORE, REDISAPI_SUNIONSTORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SADD, REDISAPI_SADD, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SCARD, REDISAPI_SCARD, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SISMEMBER, REDISAPI_SISMEMBER, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SMEMBERS, REDISAPI_SMEMBERS, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SMOVE, REDISAPI_SMOVE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SPOP, REDISAPI_SPOP, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SRANDMEMBER, REDISAPI_SRANDMEMBER, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SREM, REDISAPI_SREM, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZADD, REDISAPI_ZADD, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZCARD, REDISAPI_ZCARD, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZREM, REDISAPI_ZREM, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZINCRBY, REDISAPI_ZINCRBY, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZSCORE, REDISAPI_ZSCORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZRANK, REDISAPI_ZRANK, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZREVRANK, REDISAPI_ZREVRANK, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZRANGE, REDISAPI_ZRANGE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZREVRANGE, REDISAPI_ZREVRANGE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZREMRANGEBYRANK, REDISAPI_ZREMRANGEBYRANK, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZCOUNT, REDISAPI_ZCOUNT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZRANGEBYSCORE, REDISAPI_ZRANGEBYSCORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZREVRANGEBYSCORE, REDISAPI_ZREVRANGEBYSCORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZREMRANGEBYSCORE, REDISAPI_ZREMRANGEBYSCORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZINTERSTORE, REDISAPI_ZINTERSTORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_ZUNIONSTORE, REDISAPI_ZUNIONSTORE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HSET, REDISAPI_HSET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HMSET, REDISAPI_HMSET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HSETNX, REDISAPI_HSETNX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HGET, REDISAPI_HGET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HMGET, REDISAPI_HMGET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HGETALL, REDISAPI_HGETALL, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HVALS, REDISAPI_HVALS, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HKEYS, REDISAPI_HKEYS, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HEXISTS, REDISAPI_HEXISTS, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HDEL, REDISAPI_HDEL, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HINCRBY, REDISAPI_HINCRBY, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HINCRBYFLOAT, REDISAPI_HINCRBYFLOAT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_HLEN, REDISAPI_HLEN, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_GETSET, REDISAPI_GETSET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SETBIT, REDISAPI_SETBIT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_INCR, REDISAPI_INCR, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_INCRBY, REDISAPI_INCRBY, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_DECR, REDISAPI_DECR, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_DECRBY, REDISAPI_DECRBY, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_APPEND, REDISAPI_APPEND, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_BITCOUNT, REDISAPI_BITCOUNT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_GET, REDISAPI_GET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_GETBIT, REDISAPI_GETBIT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_GETRANGE, REDISAPI_GETRANGE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_INCRBYFLOAT, REDISAPI_INCRBYFLOAT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_MGET, REDISAPI_MGET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_MSET, REDISAPI_MSET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SET, REDISAPI_SET, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_PSETEX, REDISAPI_PSETEX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SETEX, REDISAPI_SETEX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SETNX, REDISAPI_SETNX, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_SETRANGE, REDISAPI_SETRANGE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_STRLEN, REDISAPI_STRLEN, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_TTL, REDISAPI_TTL, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_PTTL, REDISAPI_PTTL, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_EXPIRE, REDISAPI_EXPIRE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_PEXPIRE, REDISAPI_PEXPIRE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_EXPIREAT, REDISAPI_EXPIREAT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_PEXPIREAT, REDISAPI_PEXPIREAT, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_DEL, REDISAPI_DEL, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_EXISTS, REDISAPI_EXISTS, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_TYPE, REDISAPI_TYPE, T_REDIS)
      RECORD_STAT(TABLE_API_REDIS_PERSIST, REDISAPI_PERSIST, T_REDIS)
      RECORD_STAT_END()
    }
  }

  static int negate_htable_timestamp(table::ObITableEntity &entity);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableRpcProcessorUtil);
  ObTableRpcProcessorUtil() = delete;
  ~ObTableRpcProcessorUtil() = delete;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROCESSOR_UTIL_H */
