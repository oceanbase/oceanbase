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
#define USING_LOG_PREFIX SQL
#include "ob_query_response_time.h"
#include "lib/oblog/ob_log.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {

ObRSTUtility::ObRSTUtility():base_(0)
{
  max_dec_value_ = MILLION;
  for(int i= 0; OB_QRT_POSITIVE_LENGTH > i; ++i) {
    max_dec_value_ *= 10;
  }
  setup(OB_QRT_DEFAULT_BASE);
}

ObRSTUtility::~ObRSTUtility()
{}

int ObRSTUtility::setup(uint32_t base)
{
  int ret = OB_SUCCESS;
  if (base < 2) {
    ret = OB_INVALID_ARGUMENT;
  } else if (base != base_) {
    base_= base;

    uint64_t value = MILLION;
    negative_count_= 0;
    while (value > 0) {
      negative_count_ += 1;
      value /= base_;
    }
    negative_count_ -= 1;

    value = MILLION;
    positive_count_ = 0;
    while (value < max_dec_value_) {
      positive_count_ += 1;
      value *= base_;
    }
    bound_count_ = negative_count_ + positive_count_;

    value = MILLION;
    for (uint i = 0; i < negative_count_; ++i) {
      value /= base_;
      bound_[negative_count_ - i - 1] = value;
    }

    value = MILLION;
    for (uint i = 0; i < positive_count_; ++i) {
      bound_[negative_count_ + i] = value;
      value *= base_;
    }
  }
  return 0;
}

int QueryRespTimeInfo::collect(const int64_t pos, const uint64_t resp_time)
{
  int ret = OB_SUCCESS;
  ATOMIC_INC(&count_[pos]);
  ATOMIC_FAA(&total_time_[pos], resp_time);
  return ret;
}

uint64_t QueryRespTimeInfo::get_count_val(const int64_t pos)
{
  return ATOMIC_LOAD(&count_[pos]);
}

uint64_t QueryRespTimeInfo::get_total_time_val(const int64_t pos)
{
  return ATOMIC_LOAD(&total_time_[pos]);
}

ObRespTimeInfoCollector::ObRespTimeInfoCollector()
{
  flush();
}

int ObRespTimeInfoCollector::setup(uint32_t base)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(utility_.setup(base))) {
    LOG_WARN("failed to setup utility",K(ret), K(base));
  }
  return ret;
}

int ObRespTimeInfoCollector::collect(const sql::stmt::StmtType sql_type, const bool is_inner_sql, const uint64_t resp_time)
{
  int ret = OB_SUCCESS;
  int pos = -1;
  for (int i = 0; i < utility_.bound_count(); i++) {
    if(utility_.bound(i) > resp_time) {
      pos = i;
      break;
    }
  }
  if (pos < 0 || pos >= utility_.bound_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pos in utility", K(ret), K(pos));
  } else if (is_inner_sql) {
    if (OB_FAIL(inner_sql_info_.collect(pos, resp_time))) {
      LOG_WARN("inner sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
    }
  } else {
    switch (sql_type) {
      case sql::stmt::T_SELECT : {
        if (OB_FAIL(select_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("select info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_INSERT : {
        if (OB_FAIL(insert_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("insert info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_DELETE : {
        if (OB_FAIL(delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("delete info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_UPDATE : {
        if (OB_FAIL(update_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("update info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_REPLACE : {
        if (OB_FAIL(replace_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("replace info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_XA_COMMIT : {
        if (OB_FAIL(commit_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("commit info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_GET :
      case sql::stmt::T_KV_QUERY :
      case sql::stmt::T_KV_MULTI_GET : {
        if (OB_FAIL(tableapi_select_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi select sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_INSERT_OR_UPDATE :
      case sql::stmt::T_KV_MULTI_INSERT_OR_UPDATE :
      case sql::stmt::T_KV_INSERT :
      case sql::stmt::T_KV_MULTI_INSERT :
      case sql::stmt::T_KV_PUT :
      case sql::stmt::T_KV_MULTI_PUT : {
        if (OB_FAIL(tableapi_insert_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi insert sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_DELETE :
      case sql::stmt::T_KV_MULTI_DELETE : {
        if (OB_FAIL(tableapi_delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi delete sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_UPDATE :
      case sql::stmt::T_KV_APPEND :
      case sql::stmt::T_KV_INCREMENT :
      case sql::stmt::T_KV_MULTI_UPDATE :
      case sql::stmt::T_KV_MULTI_INCREMENT :
      case sql::stmt::T_KV_MULTI_APPEND : {
        if (OB_FAIL(tableapi_update_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi update sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_REPLACE :
      case sql::stmt::T_KV_MULTI_REPLACE : {
        if (OB_FAIL(tableapi_replace_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi replace sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_QUERY_AND_MUTATE : {
        if (OB_FAIL(tableapi_query_and_mutate_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi query and mutate sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_OTHER : {
        if (OB_FAIL(tableapi_other_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi other sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_SCAN : {
        if (OB_FAIL(hbase_scan_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase scan sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_PUT : {
        if (OB_FAIL(hbase_put_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase put sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_DELETE : {
        if (OB_FAIL(hbase_delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase delete sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_APPEND : {
        if (OB_FAIL(hbase_append_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase append sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_INCREMENT : {
        if (OB_FAIL(hbase_increment_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase increment sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_CHECK_AND_PUT : {
        if (OB_FAIL(hbase_check_and_put_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase check and put sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_CHECK_AND_MUTATE : {
        if (OB_FAIL(hbase_check_and_mutate_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase check and mutate sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_CHECK_AND_DELETE : {
        if (OB_FAIL(hbase_check_and_delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase check and delete sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_HYBRID_BATCH : {
        if (OB_FAIL(hbase_hybrid_batch_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase bybrid batch sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      default: {
        if (OB_FAIL(other_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("other info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
    }
  }
  return ret;
}

int ObRespTimeInfoCollector::collect_redis(const ObTableProccessType redis_type, int pos, const uint64_t resp_time)
{
  int ret = OB_SUCCESS;
  if (pos < 0 || pos >= utility_.bound_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pos in utility", K(ret), K(pos));
  } else {
    switch (redis_type) {
      case ObTableProccessType::TABLE_API_REDIS_LINDEX : {
        if (OB_FAIL(redis_lindex_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lindex info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LSET : {
        if (OB_FAIL(redis_lset_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lset info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LRANGE : {
        if (OB_FAIL(redis_lrange_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lrange info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LTRIM : {
        if (OB_FAIL(redis_ltrim_info_.collect(pos, resp_time))) {
          LOG_WARN("redis ltrim info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LPUSH : {
        if (OB_FAIL(redis_lpush_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lpush info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LPUSHX : {
        if (OB_FAIL(redis_lpushx_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lpushx info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_RPUSH : {
        if (OB_FAIL(redis_rpush_info_.collect(pos, resp_time))) {
          LOG_WARN("redis rpush info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_RPUSHX : {
        if (OB_FAIL(redis_rpushx_info_.collect(pos, resp_time))) {
          LOG_WARN("redis rpushx info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LPOP : {
        if (OB_FAIL(redis_lpop_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lpop info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_RPOP : {
        if (OB_FAIL(redis_rpop_info_.collect(pos, resp_time))) {
          LOG_WARN("redis rpop info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LREM : {
        if (OB_FAIL(redis_lrem_info_.collect(pos, resp_time))) {
          LOG_WARN("redis lrem info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_RPOPLPUSH : {
        if (OB_FAIL(redis_rpoplpush_info_.collect(pos, resp_time))) {
          LOG_WARN("redis rpoplpush info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LINSERT : {
        if (OB_FAIL(redis_linsert_info_.collect(pos, resp_time))) {
          LOG_WARN("redis linsert info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_LLEN : {
        if (OB_FAIL(redis_llen_info_.collect(pos, resp_time))) {
          LOG_WARN("redis llen info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SDIFF : {
        if (OB_FAIL(redis_sdiff_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sdiff info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SDIFFSTORE : {
        if (OB_FAIL(redis_sdiffstore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sdiffstore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SINTER : {
        if (OB_FAIL(redis_sinter_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sinter info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SINTERSTORE : {
        if (OB_FAIL(redis_sinterstore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sinterstore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SUNION : {
        if (OB_FAIL(redis_sunion_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sunion info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SUNIONSTORE : {
        if (OB_FAIL(redis_sunionstore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sunionstore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SADD : {
        if (OB_FAIL(redis_sadd_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sadd info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SCARD : {
        if (OB_FAIL(redis_scard_info_.collect(pos, resp_time))) {
          LOG_WARN("redis scard info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SISMEMBER : {
        if (OB_FAIL(redis_sismember_info_.collect(pos, resp_time))) {
          LOG_WARN("redis sismember info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SMEMBERS : {
        if (OB_FAIL(redis_smembers_info_.collect(pos, resp_time))) {
          LOG_WARN("redis smembers info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SMOVE : {
        if (OB_FAIL(redis_smove_info_.collect(pos, resp_time))) {
          LOG_WARN("redis smove info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SPOP : {
        if (OB_FAIL(redis_spop_info_.collect(pos, resp_time))) {
          LOG_WARN("redis spop info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SRANDMEMBER : {
        if (OB_FAIL(redis_srandmember_info_.collect(pos, resp_time))) {
          LOG_WARN("redis srandmember info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SREM : {
        if (OB_FAIL(redis_srem_info_.collect(pos, resp_time))) {
          LOG_WARN("redis srem info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZADD : {
        if (OB_FAIL(redis_zadd_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zadd info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZCARD : {
        if (OB_FAIL(redis_zcard_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zcard info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZREM : {
        if (OB_FAIL(redis_zrem_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrem info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZINCRBY : {
        if (OB_FAIL(redis_zincrby_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zincrby info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZSCORE : {
        if (OB_FAIL(redis_zscore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zscore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZRANK : {
        if (OB_FAIL(redis_zrank_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrank info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZREVRANK : {
        if (OB_FAIL(redis_zrevrank_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrevrank info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZRANGE : {
        if (OB_FAIL(redis_zrange_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrange info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZREVRANGE : {
        if (OB_FAIL(redis_zrevrange_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrevrange info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZREMRANGEBYRANK : {
        if (OB_FAIL(redis_zremrangebyrank_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zremrangebyrank info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZCOUNT : {
        if (OB_FAIL(redis_zcount_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zcount info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZRANGEBYSCORE : {
        if (OB_FAIL(redis_zrangebyscore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrangebyscore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZREVRANGEBYSCORE : {
        if (OB_FAIL(redis_zrevrangebyscore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zrevrangebyscore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZREMRANGEBYSCORE : {
        if (OB_FAIL(redis_zremrangebyscore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zremrangebyscore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZINTERSTORE : {
        if (OB_FAIL(redis_zinterstore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zinterstore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_ZUNIONSTORE : {
        if (OB_FAIL(redis_zunionstore_info_.collect(pos, resp_time))) {
          LOG_WARN("redis zunionstore info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HSET : {
        if (OB_FAIL(redis_hset_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hset info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HMSET : {
        if (OB_FAIL(redis_hmset_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hmset info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HSETNX : {
        if (OB_FAIL(redis_hsetnx_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hsetnx info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HGET : {
        if (OB_FAIL(redis_hget_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hget info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HMGET : {
        if (OB_FAIL(redis_hmget_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hmget info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HGETALL : {
        if (OB_FAIL(redis_hgetall_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hgetall info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HVALS : {
        if (OB_FAIL(redis_hvals_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hvals info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HKEYS : {
        if (OB_FAIL(redis_hkeys_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hkeys info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HEXISTS : {
        if (OB_FAIL(redis_hexists_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hexists info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HDEL : {
        if (OB_FAIL(redis_hdel_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hdel info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HINCRBY : {
        if (OB_FAIL(redis_hincrby_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hincrby info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HINCRBYFLOAT : {
        if (OB_FAIL(redis_hincrbyfloat_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hincrbyfloat info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_HLEN : {
        if (OB_FAIL(redis_hlen_info_.collect(pos, resp_time))) {
          LOG_WARN("redis hlen info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_GETSET : {
        if (OB_FAIL(redis_getset_info_.collect(pos, resp_time))) {
          LOG_WARN("redis getset info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SETBIT : {
        if (OB_FAIL(redis_setbit_info_.collect(pos, resp_time))) {
          LOG_WARN("redis setbit info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_INCR : {
        if (OB_FAIL(redis_incr_info_.collect(pos, resp_time))) {
          LOG_WARN("redis incr info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_INCRBY : {
        if (OB_FAIL(redis_incrby_info_.collect(pos, resp_time))) {
          LOG_WARN("redis incrby info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_DECR : {
        if (OB_FAIL(redis_decr_info_.collect(pos, resp_time))) {
          LOG_WARN("redis decr info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_DECRBY : {
        if (OB_FAIL(redis_decrby_info_.collect(pos, resp_time))) {
          LOG_WARN("redis decrby info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_APPEND : {
        if (OB_FAIL(redis_append_info_.collect(pos, resp_time))) {
          LOG_WARN("redis append info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_BITCOUNT : {
        if (OB_FAIL(redis_bitcount_info_.collect(pos, resp_time))) {
          LOG_WARN("redis bitcount info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_GET : {
        if (OB_FAIL(redis_get_info_.collect(pos, resp_time))) {
          LOG_WARN("redis get info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_GETBIT : {
        if (OB_FAIL(redis_getbit_info_.collect(pos, resp_time))) {
          LOG_WARN("redis getbit info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_GETRANGE : {
        if (OB_FAIL(redis_getrange_info_.collect(pos, resp_time))) {
          LOG_WARN("redis getrange info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_INCRBYFLOAT : {
        if (OB_FAIL(redis_incrbyfloat_info_.collect(pos, resp_time))) {
          LOG_WARN("redis incrbyfloat info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_MGET : {
        if (OB_FAIL(redis_mget_info_.collect(pos, resp_time))) {
          LOG_WARN("redis mget info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_MSET : {
        if (OB_FAIL(redis_mset_info_.collect(pos, resp_time))) {
          LOG_WARN("redis mset info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SET : {
        if (OB_FAIL(redis_set_info_.collect(pos, resp_time))) {
          LOG_WARN("redis set info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_PSETEX : {
        if (OB_FAIL(redis_psetex_info_.collect(pos, resp_time))) {
          LOG_WARN("redis psetex info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SETEX : {
        if (OB_FAIL(redis_setex_info_.collect(pos, resp_time))) {
          LOG_WARN("redis setex info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SETNX : {
        if (OB_FAIL(redis_setnx_info_.collect(pos, resp_time))) {
          LOG_WARN("redis setnx info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_SETRANGE : {
        if (OB_FAIL(redis_setrange_info_.collect(pos, resp_time))) {
          LOG_WARN("redis setrange info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_STRLEN : {
        if (OB_FAIL(redis_strlen_info_.collect(pos, resp_time))) {
          LOG_WARN("redis strlen info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_TTL : {
        if (OB_FAIL(redis_ttl_info_.collect(pos, resp_time))) {
          LOG_WARN("redis ttl info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_PTTL : {
        if (OB_FAIL(redis_pttl_info_.collect(pos, resp_time))) {
          LOG_WARN("redis pttl info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_EXPIRE : {
        if (OB_FAIL(redis_expire_info_.collect(pos, resp_time))) {
          LOG_WARN("redis expire info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_PEXPIRE : {
        if (OB_FAIL(redis_pexpire_info_.collect(pos, resp_time))) {
          LOG_WARN("redis pexpire info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_EXPIREAT : {
        if (OB_FAIL(redis_expireat_info_.collect(pos, resp_time))) {
          LOG_WARN("redis expireat info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_PEXPIREAT : {
        if (OB_FAIL(redis_pexpireat_info_.collect(pos, resp_time))) {
          LOG_WARN("redis pexpireat info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_DEL : {
        if (OB_FAIL(redis_del_info_.collect(pos, resp_time))) {
          LOG_WARN("redis del info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_EXISTS : {
        if (OB_FAIL(redis_exists_info_.collect(pos, resp_time))) {
          LOG_WARN("redis exists info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_TYPE : {
        if (OB_FAIL(redis_type_info_.collect(pos, resp_time))) {
          LOG_WARN("redis type info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case ObTableProccessType::TABLE_API_REDIS_PERSIST : {
        if (OB_FAIL(redis_persist_info_.collect(pos, resp_time))) {
          LOG_WARN("redis persist info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      default: {
        if (OB_FAIL(other_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("other info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
    }
  }
  return ret;
}

int ObRespTimeInfoCollector::collect(const ObTableHistogramType table_his_type, const uint64_t resp_time)
{
  int ret = OB_SUCCESS;
  int pos = -1;
  for (int i = 0; i < utility_.bound_count(); i++) {
    if(utility_.bound(i) > resp_time) {
      pos = i;
      break;
    }
  }
  if (pos < 0 || pos >= utility_.bound_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pos in utility", K(ret), K(pos));
  } else {
    switch (table_his_type.sql_type_) {
      case sql::stmt::T_KV_GET :
      case sql::stmt::T_KV_QUERY :
      case sql::stmt::T_KV_MULTI_GET : {
        if (OB_FAIL(tableapi_select_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi select sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_INSERT_OR_UPDATE :
      case sql::stmt::T_KV_MULTI_INSERT_OR_UPDATE :
      case sql::stmt::T_KV_INSERT :
      case sql::stmt::T_KV_MULTI_INSERT :
      case sql::stmt::T_KV_PUT :
      case sql::stmt::T_KV_MULTI_PUT : {
        if (OB_FAIL(tableapi_insert_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi insert sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_DELETE :
      case sql::stmt::T_KV_MULTI_DELETE : {
        if (OB_FAIL(tableapi_delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi delete sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_UPDATE :
      case sql::stmt::T_KV_APPEND :
      case sql::stmt::T_KV_INCREMENT :
      case sql::stmt::T_KV_MULTI_UPDATE :
      case sql::stmt::T_KV_MULTI_INCREMENT :
      case sql::stmt::T_KV_MULTI_APPEND : {
        if (OB_FAIL(tableapi_update_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi update sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_REPLACE :
      case sql::stmt::T_KV_MULTI_REPLACE : {
        if (OB_FAIL(tableapi_replace_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi replace sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_QUERY_AND_MUTATE : {
        if (OB_FAIL(tableapi_query_and_mutate_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi query and mutate sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_KV_OTHER : {
        if (OB_FAIL(tableapi_other_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("tableapi other sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_SCAN : {
        if (OB_FAIL(hbase_scan_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase scan sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_PUT : {
        if (OB_FAIL(hbase_put_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase put sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_DELETE : {
        if (OB_FAIL(hbase_delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase delete sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_APPEND : {
        if (OB_FAIL(hbase_append_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase append sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_INCREMENT : {
        if (OB_FAIL(hbase_increment_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase increment sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_CHECK_AND_PUT : {
        if (OB_FAIL(hbase_check_and_put_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase check and put sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_CHECK_AND_DELETE : {
        if (OB_FAIL(hbase_check_and_delete_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase check and delete sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_HBASE_HYBRID_BATCH : {
        if (OB_FAIL(hbase_hybrid_batch_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("hbase bybrid batch sql info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
      case sql::stmt::T_REDIS : {
        ret = collect_redis(table_his_type.table_type_, pos, resp_time);
        break;
      }
      default: {
        if (OB_FAIL(other_sql_info_.collect(pos, resp_time))) {
          LOG_WARN("other info failed to collect resp time", K(ret), K(pos), K(resp_time), K(utility_.bound_count()));
        }
        break;
      }
    }
  }
  return ret;
}

int ObRespTimeInfoCollector::flush(int64_t base /*=OB_INVALID_ID*/)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (!tenant_config.is_valid()) {
  } else {
    if (OB_FAIL(setup(base == OB_INVALID_ID ? tenant_config->query_response_time_range_base : base))) {
      LOG_WARN("failed to setup utility", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < OB_QRT_OVERALL_COUNT + 1; i++) {
#define DEF_RESP_TIME_SQL_TYPE(name)           \
  ATOMIC_SET(&name##_info_.count_[i], 0);      \
  ATOMIC_SET(&name##_info_.total_time_[i], 0);
#include "observer/mysql/ob_query_response_time.h"
#undef DEF_RESP_TIME_SQL_TYPE
  }
  return 0;
}

int ObRespTimeInfoCollector::get_count_val(
  const RespTimeSqlType resp_time_sql_type,
  const int32_t pos,
  int64_t &val)
{
  int ret = OB_SUCCESS;
  if (pos < 0 || pos >= utility_.bound_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pos in utility", K(ret), K(pos));
  } else {
    switch (resp_time_sql_type) {
#define DEF_RESP_TIME_SQL_TYPE(name)           \
      case name : {                                \
        val = name##_info_.get_count_val(pos);     \
        break;                                     \
      }
#include "observer/mysql/ob_query_response_time.h"
#undef DEF_RESP_TIME_SQL_TYPE
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid RespTimeSqlType", K(ret), K(resp_time_sql_type));
        break;
      }
    }
  }
  return ret;
}

int ObRespTimeInfoCollector::get_total_time_val(
  const RespTimeSqlType resp_time_sql_type,
  const int32_t pos,
  int64_t &val)
{
  int ret = OB_SUCCESS;
  if (pos < 0 || pos >= utility_.bound_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pos in utility", K(ret), K(pos));
  } else {
    switch (resp_time_sql_type) {
#define DEF_RESP_TIME_SQL_TYPE(name)                \
      case name : {                                     \
        val = name##_info_.get_total_time_val(pos);     \
        break;                                          \
      }
#include "observer/mysql/ob_query_response_time.h"
#undef DEF_RESP_TIME_SQL_TYPE
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid RespTimeSqlType", K(ret), K(resp_time_sql_type));
        break;
      }
    }
  }
  return ret;
}

int ObTenantQueryRespTimeCollector::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantQueryRespTimeCollector has been inited", K(ret));
  } else {
    flush_config_version_ = 0;
    multi_collector_.set_tenant_id(MTL_ID());
    multi_collector_.set_attr(ObMemAttr(MTL_ID(), "RespTimeColl"));
    multi_collector_.prepare_allocate(multi_ways_count_); // Memory is allocated in advance and objects are constructed in advance.
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    LOG_INFO("succ to init ObTenantQueryRespTimeCollector", K(MTL_ID()));
  }
  return ret;
}

void ObTenantQueryRespTimeCollector::destroy()
{
  multi_collector_.destroy();
  flush_config_version_ = 0;
  is_inited_ = false;
  LOG_INFO("succ to destroy ObTenantQueryRespTimeCollector", K(MTL_ID()));
}

int ObTenantQueryRespTimeCollector::mtl_init(ObTenantQueryRespTimeCollector *&t_resp_time_collector)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t_resp_time_collector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant query response time collector is null", K(ret));
  } else if (OB_FAIL(t_resp_time_collector->init())) {
    LOG_WARN("failed to init tenant query response time collector", K(ret));
  }
  return ret;
}

void ObTenantQueryRespTimeCollector::mtl_destroy(ObTenantQueryRespTimeCollector *&t_resp_time_collector)
{
  if (nullptr != t_resp_time_collector) {
    t_resp_time_collector->destroy();
    common::ob_delete(t_resp_time_collector);
    t_resp_time_collector = nullptr;
  }
}

int ObTenantQueryRespTimeCollector::collect(const sql::stmt::StmtType sql_type, const bool is_inner_sql, const uint64_t resp_time)
{
  int ret = OB_SUCCESS;
  const size_t pos = std::abs(GETTID()) % multi_ways_count_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(multi_collector_.at(pos).collect(sql_type, is_inner_sql, resp_time))) {
    LOG_WARN("failed to collect response time",K(ret), K(pos), K(sql_type), K(resp_time), K(is_inner_sql));
  }

  return ret;
}

int ObTenantQueryRespTimeCollector::collect(const ObTableHistogramType table_his_type, const uint64_t resp_time)
{
  int ret = OB_SUCCESS;
  const size_t pos = std::abs(GETTID()) % multi_ways_count_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(multi_collector_.at(pos).collect(table_his_type, resp_time))) {
    LOG_WARN("failed to collect response time",K(ret), K(pos), K(table_his_type));
  }

  return ret;
}

int ObTenantQueryRespTimeCollector::get_sum_value(ObRespTimeInfoCollector &total_collector)
{
  int ret = OB_SUCCESS;
  RLockGuard rlock_guard(rwlock_);
  if (OB_FAIL(total_collector.flush(multi_collector_.at(0).utility().base()))) {
    LOG_WARN("failed to flush total collector", K(ret), K(MTL_ID()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_ways_count_; i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < total_collector.utility().bound_count(); j++) {
#define DEF_RESP_TIME_SQL_TYPE(name)           \
        total_collector.name##_info_.count_[j] +=  \
                multi_collector_.at(i).name##_info_.get_count_val(j); \
        total_collector.name##_info_.total_time_[j] +=  \
                multi_collector_.at(i).name##_info_.get_total_time_val(j);
#include "observer/mysql/ob_query_response_time.h"
#undef DEF_RESP_TIME_SQL_TYPE
      }

    }
  }
  return ret;
}


int ObTenantQueryRespTimeCollector::flush()
{
  int ret = OB_SUCCESS;
  WLockGuard wlock_guard(rwlock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_ways_count_; i++) {
    if (OB_FAIL(multi_collector_.at(i).flush())) {
      LOG_WARN("failed to flush resp time info collector", K(ret), K(i));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
