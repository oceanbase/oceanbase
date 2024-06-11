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

#ifdef DEF_RESP_TIME_SQL_TYPE
DEF_RESP_TIME_SQL_TYPE(select_sql)
DEF_RESP_TIME_SQL_TYPE(insert_sql)
DEF_RESP_TIME_SQL_TYPE(delete_sql)
DEF_RESP_TIME_SQL_TYPE(update_sql)
DEF_RESP_TIME_SQL_TYPE(replace_sql)
DEF_RESP_TIME_SQL_TYPE(commit_sql)
DEF_RESP_TIME_SQL_TYPE(other_sql)
DEF_RESP_TIME_SQL_TYPE(inner_sql)
#endif // DEF_RESP_TIME_SQL_TYPE

#ifndef OB_QUERY_RESPONSE_TIME_
#define OB_QUERY_RESPONSE_TIME_

#include "share/ob_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/ob_stmt_type.h"
#include "lib/lock/ob_tc_rwlock.h"
namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
class ObDatabaseSchema;

}  // namespace schema
}  // namespace share

namespace observer {

static const int32_t OB_QRT_POSITIVE_LENGTH = 7;
static const int32_t OB_QRT_NEGATIVE_LENGTH = 7;

// Defaute query reqponse time range base
static const uint32_t OB_QRT_DEFAULT_BASE = 10;

static const int32_t OB_QRT_POSITIVE_COUNT = (int32_t)(3.32192809 * OB_QRT_POSITIVE_LENGTH);
static const int32_t OB_QRT_NEGATIVE_COUNT = (int32_t)(3.32192809 * OB_QRT_NEGATIVE_LENGTH);
static const int32_t OB_QRT_OVERALL_COUNT = (int32_t)(OB_QRT_POSITIVE_COUNT + 1 + OB_QRT_NEGATIVE_COUNT);

static const uint64_t MILLION = (uint64_t)(1000 * 1000);

const char* const QUERY_RESPPONSE_TIME_STATS = "query_response_time_stats";
const char* const QUERY_RESPPONSE_TIME_FLUSH = "query_response_time_flush";

class ObRSTUtility {
public:
  ObRSTUtility();
  virtual ~ObRSTUtility();
  int setup(uint32_t base);

  uint base() const {return base_;};
  uint negative_count() const { return negative_count_; };
  uint positive_count() const { return positive_count_; };
  uint bound_count()const { return bound_count_; };
  uint64_t max_dec_value() const { return max_dec_value_; };
  uint64_t bound(uint index) const { return bound_[index]; };

private:
  uint base_;
  uint negative_count_;
  uint positive_count_;
  uint bound_count_;
  uint64_t max_dec_value_; 
  uint64_t bound_[OB_QRT_OVERALL_COUNT];
};

struct QueryRespTimeInfo {
  int collect(const int64_t pos, const uint64_t resp_time);
  uint64_t get_count_val(const int64_t pos);
  uint64_t get_total_time_val(const int64_t pos);
  uint64_t count_[OB_QRT_OVERALL_COUNT];
  uint64_t total_time_[OB_QRT_OVERALL_COUNT];
};

enum RespTimeSqlType {
#define DEF_RESP_TIME_SQL_TYPE(name) name,
#include "observer/mysql/ob_query_response_time.h"
#undef DEF_RESP_TIME_SQL_TYPE
  END,
};

class ObRespTimeInfoCollector {
public:
  ObRespTimeInfoCollector();
  ~ObRespTimeInfoCollector() = default;
  int setup(uint32_t base);
  int collect(const sql::stmt::StmtType sql_type, const bool is_inner_sql, const uint64_t resp_time);
  int flush(int64_t base = OB_INVALID_ID);
  const ObRSTUtility &utility() const {return utility_;}
  int get_count_val(const RespTimeSqlType resp_time_sql_type, const int32_t pos, int64_t &val);
  int get_total_time_val(const RespTimeSqlType resp_time_sql_type, const int32_t pos, int64_t &val);
  TO_STRING_KV(K(utility_.base()));
public:
#define DEF_RESP_TIME_SQL_TYPE(name)   \
  QueryRespTimeInfo name##_info_;
#include "observer/mysql/ob_query_response_time.h"
#undef DEF_RESP_TIME_SQL_TYPE
private:
  ObRSTUtility utility_;
};

class ObTenantQueryRespTimeCollector {
public:
  ObTenantQueryRespTimeCollector(): multi_collector_(),rwlock_(),flush_config_version_(0),multi_ways_count_(64) {};
  ~ObTenantQueryRespTimeCollector() {};
  int init();
  void destroy();
  static int mtl_init(ObTenantQueryRespTimeCollector *&t_resp_time_collector);
  static void mtl_destroy(ObTenantQueryRespTimeCollector *&t_resp_time_collector);

  int collect(const sql::stmt::StmtType sql_type, const bool is_inner_sql, const uint64_t resp_time);
  int get_sum_value(ObRespTimeInfoCollector &total_collector);
  int resize();
  int flush();
  int64_t get_flush_config_version() const { return flush_config_version_; };
  void set_flush_config_version(int64_t version) { ATOMIC_SET(&flush_config_version_, version); };

private:
  typedef common::RWLock::RLockGuard RLockGuard;
  typedef common::RWLock::WLockGuard WLockGuard;
  ObArray<ObRespTimeInfoCollector> multi_collector_;
  common::RWLock rwlock_; //for leader revoke/takeover submit log
  int64_t flush_config_version_;
  const int64_t multi_ways_count_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantQueryRespTimeCollector);
}; // end of class ObTenantQueryRespTimeCollector

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_QUERY_RESPONSE_TIME_ */
//// end of header file
