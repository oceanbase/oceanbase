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

#ifndef OB_QUERY_RESPONSE_TIME_
#define OB_QUERY_RESPONSE_TIME_

#include "share/ob_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
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
  int setup(uint base);

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

class ObRSTTimeCollector {
public:

  ObRSTUtility utility_;
  uint32_t count_[OB_QRT_OVERALL_COUNT];
  uint64_t total_[OB_QRT_OVERALL_COUNT];

public:
  ObRSTTimeCollector();
  ~ObRSTTimeCollector();
  int setup(uint base) {return utility_.setup(base);};
  uint32_t count(uint index) {return count_[index];};
  uint64_t total(uint index) {return total_[index];};
  uint bound_count() {return utility_.bound_count();}
  uint64_t bound(uint index) {return utility_.bound(index);}
  void print_time(char* buffer, std::size_t buffer_size, const char* format, uint64_t value);
  int flush();
  int collect(uint64_t time);
};

class ObRSTCollector{
public:
  static ObRSTCollector& get_instance();
  common::hash::ObHashMap<uint64_t /*tenant id*/, ObRSTTimeCollector*> collector_map_;
  int init();
  int control_query_response_time(uint64_t tenant_id, const ObString& is_enable);
  int flush_query_response_time(uint64_t tenant_id, const ObString& is_enable);
  int collect_query_response_time(uint64_t tenant_id_, uint64_t time);

private:
  ObRSTCollector();
  ~ObRSTCollector();
  bool inited_;
  int free_query_response_time(uint64_t tenant_id);
  int enable_query_response_time(uint64_t tenant_id);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_QUERY_RESPONSE_TIME_ */
//// end of header file
