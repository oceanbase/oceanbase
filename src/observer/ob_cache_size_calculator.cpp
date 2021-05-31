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

#include "ob_cache_size_calculator.h"

#include "share/cache/ob_cache_name_define.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace observer {
ObCacheSizeCalculator::ObCacheSizeCalculator() : inited_(false), partition_service_(NULL), schema_service_(NULL)
{}

ObCacheSizeCalculator::~ObCacheSizeCalculator()
{}

int ObCacheSizeCalculator::init(ObPartitionService& partition_service, ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    partition_service_ = &partition_service;
    schema_service_ = &schema_service;
    const int thread_count = 1;
    set_thread_count(thread_count);
    inited_ = true;
  }
  return ret;
}

void ObCacheSizeCalculator::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("CacheCalculator");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("cache size calculator thread start");
    while (!has_set_stop()) {
      int64_t loc_cache_size = 0;
      int64_t schema_cache_size = 0;
      if (OB_FAIL(calc_loc_cache_size(loc_cache_size))) {
        LOG_WARN("calc_loc_cache_size failed", K(ret));
      } else if (OB_FAIL(calc_schema_cache_size(schema_cache_size))) {
        LOG_WARN("calc_schema_cache_size failed", K(ret));
      } else if (OB_FAIL(set_cache_size(loc_cache_size, schema_cache_size))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("set_cache_size failed", K(ret), K(loc_cache_size), K(schema_cache_size));
        }
      }

      int64_t left_time_to_sleep = RENEW_CACHE_SIZE_INTERVAL_US;
      const int64_t sleep_interval = 10000;  // 10ms
      while (!has_set_stop() && left_time_to_sleep > 0) {
        int64_t time_to_sleep = std::min(left_time_to_sleep, sleep_interval);
        usleep(sleep_interval);
        left_time_to_sleep -= time_to_sleep;
      }
    }
    LOG_INFO("cache sfie calculator thread exit");
  }
}

int ObCacheSizeCalculator::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    LOG_WARN("start cache size calculator thread failed", K(ret));
  }
  return ret;
}

void ObCacheSizeCalculator::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!has_set_stop()) {
    share::ObThreadPool::stop();
  }
}

int ObCacheSizeCalculator::calc_loc_cache_size(int64_t& cache_size)
{
  int ret = OB_SUCCESS;
  int64_t partition_count = 0;
  int64_t avg_cache_item_size = 0;
  cache_size = 0;
  if (!GCTX.is_standby_cluster_and_started()) {
    cache_size = 0;
  } else {
    if (OB_FAIL(partition_service_->get_partition_count(partition_count))) {
      LOG_WARN("get_partition_count failed", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_avg_cache_item_size(
                   OB_SYS_TENANT_ID, OB_LOCATION_CACHE_NAME, avg_cache_item_size))) {
      LOG_WARN("get_avg_cache_item_size failed", K(ret), "cache_name", OB_LOCATION_CACHE_NAME);
    } else {
      cache_size = normalize_cache_size(partition_count * avg_cache_item_size);
      LOG_DEBUG("calcucate location cache size succeed", K(cache_size), K(partition_count), K(avg_cache_item_size));
    }
  }
  return ret;
}

int ObCacheSizeCalculator::calc_schema_cache_size(int64_t& cache_size)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  int64_t schema_item_count = 0;
  int64_t avg_cache_item_size = 0;
  cache_size = 0;
  if (!GCTX.is_standby_cluster_and_started()) {
    cache_size = 0;
  } else {
    if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_schema_count(schema_item_count))) {
      LOG_WARN("get_schema_count failed", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_avg_cache_item_size(
                   OB_SYS_TENANT_ID, OB_SCHEMA_CACHE_NAME, avg_cache_item_size))) {
      LOG_WARN("get_avg_cache_item_size failed", K(ret), "cache_name", OB_SCHEMA_CACHE_NAME);
    } else {
      cache_size = normalize_cache_size(schema_item_count * avg_cache_item_size);
      LOG_DEBUG("calcucate schema cache size succeed", K(cache_size), K(schema_item_count), K(avg_cache_item_size));
    }
  }
  return ret;
}

int ObCacheSizeCalculator::set_cache_size(const int64_t loc_cache_size, const int64_t schema_cache_size)
{
  int ret = OB_SUCCESS;
  int64_t cur_loc_cache_size = 0;
  int64_t cur_schema_cache_size = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (loc_cache_size < 0 || schema_cache_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(loc_cache_size), K(schema_cache_size));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_hold_size(
                 OB_SYS_TENANT_ID, OB_LOCATION_CACHE_NAME, cur_loc_cache_size))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get location cache hold size failed", K(ret));
    }
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_hold_size(
                 OB_SYS_TENANT_ID, OB_SCHEMA_CACHE_NAME, cur_schema_cache_size))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get schema cache hold size failed", K(ret));
    }
  } else {
    if (cur_loc_cache_size != loc_cache_size) {
      if (OB_FAIL(ObKVGlobalCache::get_instance().set_hold_size(
              OB_SYS_TENANT_ID, OB_LOCATION_CACHE_NAME, loc_cache_size))) {
        LOG_WARN("set location cache hold size failed", K(ret), K(loc_cache_size));
      } else {
        LOG_INFO("set location cache hold size succeed", K(loc_cache_size));
      }
    }

    if (OB_SUCC(ret) && cur_schema_cache_size != schema_cache_size) {
      if (OB_FAIL(ObKVGlobalCache::get_instance().set_hold_size(
              OB_SYS_TENANT_ID, OB_SCHEMA_CACHE_NAME, schema_cache_size))) {
        LOG_WARN("set schema cache hold size failed", K(ret), K(schema_cache_size));
      } else {
        LOG_INFO("set schema cache hold size succeed", K(schema_cache_size));
      }
    }
  }
  return ret;
}

int64_t ObCacheSizeCalculator::normalize_cache_size(const int64_t cache_size)
{
  int64_t normalized_size = OB_MALLOC_BIG_BLOCK_SIZE *
                            (cache_size / (OB_MALLOC_BIG_BLOCK_SIZE * (100 - MEM_BLOCK_FRAGMENT_PERCENT) / 100) + 2);
  if (normalized_size < MIN_HOLD_SIZE) {
    normalized_size = MIN_HOLD_SIZE;
  }
  if (normalized_size > MAX_HOLD_SIZE) {
    normalized_size = MAX_HOLD_SIZE;
  }
  return normalized_size;
}

}  // end namespace observer
}  // end namespace oceanbase
