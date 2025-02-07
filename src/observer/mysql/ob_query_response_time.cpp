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
