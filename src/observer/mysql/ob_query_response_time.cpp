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

#include "ob_query_response_time.h"
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

int ObRSTUtility::setup(uint base)
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

ObRSTTimeCollector::ObRSTTimeCollector()
{
  flush();
}
ObRSTTimeCollector::~ObRSTTimeCollector()
{}

int ObRSTTimeCollector::flush()
{
  for (int i = 0; i < OB_QRT_OVERALL_COUNT + 1; i++) {
    ATOMIC_SET(&count_[i], 0);
    ATOMIC_SET(&total_[i], 0);
  }
  return 0;
}

int ObRSTTimeCollector::collect(uint64_t time)
{
  int i = 0;
  for (int count = utility_.bound_count(); count > i; ++i) {
    if(utility_.bound(i) > time) {
      ATOMIC_INC(&count_[i]);
      ATOMIC_FAA(&total_[i],time);
      break;
    }
  }
  return 0;
}

ObRSTCollector::ObRSTCollector():inited_(false)
{}

ObRSTCollector::~ObRSTCollector()
{
  inited_ = false;
  collector_map_.clear();
}

ObRSTCollector& ObRSTCollector::get_instance()
{
  static ObRSTCollector collector_instance_;
  if (!collector_instance_.inited_){
    collector_instance_.init();
  }
  return collector_instance_;
}

int ObRSTCollector::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (!collector_map_.created() && OB_FAIL(collector_map_.create(
      common::OB_MAX_SERVER_TENANT_CNT, ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "create time collector map failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObRSTCollector::collect_query_response_time(uint64_t tenant_id, uint64_t time)
{
  int ret = OB_SUCCESS;
  ObRSTTimeCollector* time_collector;
  if (OB_FAIL(collector_map_.get_refactored(tenant_id, time_collector))) {
    SERVER_LOG(WARN, "time collector of the tenant does not exist", K(tenant_id), K(time), K(ret));
  } else {
    if(OB_FAIL(time_collector->collect(time))) {
      SERVER_LOG(WARN, "time collector of the tenant collect time failed", K(tenant_id), K(time), K(ret));
    }
  }
  return ret;
}

int ObRSTCollector::flush_query_response_time(uint64_t tenant_id,const ObString& is_enable)
{
  int ret = OB_SUCCESS;
  bool is_enable_value = false;
  bool is_valid = false;
  is_enable_value = ObConfigBoolParser::get(is_enable.ptr(), is_valid);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid bool str", K(ret), K(is_enable), K(tenant_id));
    } else if (is_enable_value) {
      ObRSTTimeCollector* time_collector;
      if (!tenant_config->query_response_time_stats) {
        SERVER_LOG(WARN, "query_response_time_stats already turn off", K(ret), K(tenant_id));
      } else if (OB_FAIL(collector_map_.get_refactored(tenant_id, time_collector))){
        SERVER_LOG(WARN, "time collector of the tenant does not exist", K(ret), K(tenant_id));
      } else if (OB_FAIL(time_collector->setup(tenant_config->query_response_time_range_base))) {
        SERVER_LOG(WARN, "time collector of the tenant set range base failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(time_collector->flush())) {
        SERVER_LOG(WARN, "time collector of the tenant flush failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObRSTCollector::enable_query_response_time(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    ObRSTTimeCollector* time_collector;
    if (OB_SUCC(collector_map_.get_refactored(tenant_id, time_collector))) {
      SERVER_LOG(INFO, "time collector of the tenant is existed", K(ret), K(tenant_id));
    } else if (OB_FAIL(collector_map_.set_refactored(tenant_id, new ObRSTTimeCollector()))) {
      SERVER_LOG(WARN, "create time collector of the tenant failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObRSTCollector::free_query_response_time(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    ObRSTTimeCollector* time_collector;
    if (OB_FAIL(collector_map_.get_refactored(tenant_id, time_collector))) {
      if(OB_HASH_NOT_EXIST == ret){
        SERVER_LOG(WARN, "time collector of the tenant does not exist", K(ret), K(tenant_id));
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "query time collector of the tenant failed", K(ret), K(tenant_id));
      }
    } else if (OB_FAIL(collector_map_.erase_refactored(tenant_id))) {
      SERVER_LOG(WARN, "erase the time collector failed", K(tenant_id));
    }
  }
  return ret;
}

int ObRSTCollector::control_query_response_time(uint64_t tenant_id, const ObString& is_enable)
{
  int ret = OB_SUCCESS;
  bool is_enable_value = false;
  bool is_valid = false;
  is_enable_value = ObConfigBoolParser::get(is_enable.ptr(), is_valid);
  if (is_enable_value) {
    if (OB_FAIL(enable_query_response_time(tenant_id))) {
      SERVER_LOG(WARN, "enable the query response time failed", K(ret), K(tenant_id));
    }
  } else if (OB_FAIL(free_query_response_time(tenant_id))) {
    SERVER_LOG(WARN, "free the query response time failed", K(ret), K(tenant_id));
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
