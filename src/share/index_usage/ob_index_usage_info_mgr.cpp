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
#include "ob_index_usage_info_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_tenant_schema_service.h"

#define USING_LOG_PREFIX SERVER
using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

const char *OB_INDEX_USAGE_MANAGER = "IndexUsageMgr";

void ObIndexUsageOp::operator()(common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &data)
{
  if (ObIndexUsageOpMode::UPDATE == op_mode_) {
    ATOMIC_INC(&data.second.total_exec_count_);
    data.second.last_used_time_ = current_time_;
  } else if (ObIndexUsageOpMode::RESET == op_mode_) {
    old_info_ = data.second;
    data.second.reset();
  }
}

ObIndexUsageInfoMgr::ObIndexUsageInfoMgr()
    : is_inited_(false), is_enabled_(false), is_sample_mode_(true), max_entries_(30000),
      current_time_(common::ObClockGenerator::getClock()),
      min_tenant_data_version_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      hashmap_count_(0),
      index_usage_map_(nullptr), report_task_(), refresh_conf_task_(), allocator_() {}

ObIndexUsageInfoMgr::~ObIndexUsageInfoMgr()
{
  destroy();
}

int ObIndexUsageInfoMgr::mtl_init(ObIndexUsageInfoMgr *&index_usage_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(index_usage_mgr->init(tenant_id))) {
    LOG_WARN("ObIndexUsageInfoMgr init failed", K(ret), K(tenant_id));
  }
  return ret;
}

uint64_t ObIndexUsageInfoMgr::calc_hashmap_count(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t hashmap_count = 0;

  ObTenantBase * tenant = MTL_CTX();
  int64_t tenant_min_cpu = tenant != NULL ? std::max((int)tenant->unit_min_cpu(), 1) : 1;
  int64_t tenant_min_thread_cnt = tenant_min_cpu * DEFAULT_CPU_QUATO_CONCURRENCY;
  int64_t tenant_memory_limit = lib::get_tenant_memory_limit(tenant_id);
  int64_t hashmap_memory = tenant_min_thread_cnt * ONE_HASHMAP_MEMORY;
  int64_t hashmap_tenant_memory_limit = tenant_memory_limit * 2 / 1000;
  int64_t hashmap_memory_limit = hashmap_memory < hashmap_tenant_memory_limit ? hashmap_memory : hashmap_tenant_memory_limit;

  hashmap_count = hashmap_memory_limit / ONE_HASHMAP_MEMORY + 1;

  LOG_TRACE("success to get hash map count",
    K(tenant_min_cpu), K(tenant_min_thread_cnt), K(tenant_memory_limit),
    K(hashmap_memory), K(hashmap_tenant_memory_limit), K(hashmap_memory_limit), K(hashmap_count));
  return hashmap_count;
}

int ObIndexUsageInfoMgr::create_hash_map(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  hashmap_count_ = calc_hashmap_count(tenant_id);
  void *tmp_ptr = allocator_.alloc(sizeof(ObIndexUsageHashMap) * hashmap_count_) ;
  if (NULL == tmp_ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for hashmap", K(ret), K(hashmap_count_));
  } else {
    index_usage_map_ = new (tmp_ptr) ObIndexUsageHashMap[hashmap_count_];
    const ObMemAttr attr(tenant_id, OB_INDEX_USAGE_MANAGER);
    for (int64_t i = 0 ; OB_SUCC(ret) && i < hashmap_count_; ++i) {
      ObIndexUsageHashMap *hashmap = index_usage_map_ + i;
      if (OB_FAIL(hashmap->create(DEFAULT_MAX_HASH_BUCKET_CNT, attr))) {
        LOG_WARN("create hash map failed", K(ret));
      }
    }
  }
  return ret;
}

void ObIndexUsageInfoMgr::destroy_hash_map()
{
  if (OB_NOT_NULL(index_usage_map_)) {
    for (int i = 0 ; i < hashmap_count_; ++i) {
      ObIndexUsageHashMap *hashmap = index_usage_map_ + i;
      hashmap->~ObIndexUsageHashMap();
    }
    allocator_.free(index_usage_map_);
    index_usage_map_ = nullptr;
  }
}

int ObIndexUsageInfoMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObMemAttr attr(tenant_id, OB_INDEX_USAGE_MANAGER);

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (!is_user_tenant(tenant_id)) {
    LOG_INFO("index monitoring only for user tenant", K(tenant_id));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_NORMAL_BLOCK_SIZE, attr))) {
    LOG_WARN("init allocator failed", K(ret));
  } else if (OB_FAIL(create_hash_map(tenant_id))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FALSE_IT(refresh_config())) {
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    LOG_TRACE("success to init ObIndexUsageInfoMgr", K(tenant_id_));
  }
  return ret;
}

void ObIndexUsageInfoMgr::destroy()
{
  if (is_inited_) {
    // cancel report task
    if (report_task_.get_is_inited()) {
      bool is_exist = true;
      if (TG_TASK_EXIST(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_, is_exist) == OB_SUCCESS && is_exist) {
        TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
        TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
        report_task_.destroy();
      }
    }
    if (refresh_conf_task_.get_is_inited()) {
      bool is_exist = true;
      if (TG_TASK_EXIST(MTL(omt::ObSharedTimer *)->get_tg_id(), refresh_conf_task_, is_exist) == OB_SUCCESS && is_exist) {
        TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), refresh_conf_task_);
        TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), refresh_conf_task_);
        refresh_conf_task_.destroy();
      }
    }
    is_inited_ = false;
    is_enabled_ = false;
    destroy_hash_map();
    allocator_.reset();
  }
}

int ObIndexUsageInfoMgr::start()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // report index usage
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_, INDEX_USAGE_REPORT_INTERVAL, true))) {
      LOG_WARN("failed to schedule index usage report task", K(ret));
    } else if (OB_FAIL(report_task_.init(this))) {
      LOG_WARN("fail to init report task", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer *)->get_tg_id(), refresh_conf_task_, INDEX_USAGE_REFRESH_CONF_INTERVAL, true))) {
      LOG_WARN("failed to schedule index usage refresh conf task", K(ret));
    } else if (OB_FAIL(refresh_conf_task_.init((this)))) {
      LOG_WARN("fail to init refresh conf task", K(ret));
    } else {
      LOG_TRACE("success to start ObIndexUsageInfoMgr", K(tenant_id_));
    }
  }
  return ret;
}

void ObIndexUsageInfoMgr::stop()
{
  if (OB_LIKELY(report_task_.get_is_inited())) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
  }
  if (OB_LIKELY(refresh_conf_task_.get_is_inited())) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), refresh_conf_task_);
  }
}

void ObIndexUsageInfoMgr::wait()
{
  if (OB_LIKELY(report_task_.get_is_inited())) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), report_task_);
  }
  if (OB_LIKELY(refresh_conf_task_.get_is_inited())) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), refresh_conf_task_);
  }
}

bool ObIndexUsageInfoMgr::sample_filterd(const uint64_t random_num)
{
  bool is_filtered = true;
  if (!is_sample_mode_) {
    is_filtered = false;
  } else {
    const int seed = random_num % 20;
    const double sample_ratio = SAMPLE_RATIO * 1.0 / 100;
    const int cycle = sample_ratio < 1 ? 20 * sample_ratio : 20;
    for (int i = 0; i < cycle; ++i) {
      if (i == seed) {
        is_filtered = false;
        break;
      }
    }
  }
  return is_filtered;
}

void ObIndexUsageInfoMgr::update(const uint64_t tenant_id, const uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  uint64_t random_num = common::ObClockGenerator::getClock();
  if (!is_inited_ || !is_enabled_) {
    // do nothing
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || index_table_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(index_table_id));
  } else if (min_tenant_data_version_ < DATA_VERSION_4_3_0_0) {
    // skip
  } else if (sample_filterd(random_num)) {
    // skip
  } else {
    ObIndexUsageKey key(index_table_id);
    ObIndexUsageOp update_op(ObIndexUsageOpMode::UPDATE, current_time_);
    int idx = random_num % hashmap_count_;
    if (is_inner_object_id(index_table_id)) {
      // do nothing
    } else if (OB_SUCC(index_usage_map_[idx].read_atomic(key, update_op))) {
      // key exists, update success
    } else if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
      // key not exist, insert new one
      ObIndexUsageInfo new_info;
      new_info.total_exec_count_ = 1;
      new_info.last_used_time_ = current_time_;
      if (max_entries_ <= index_usage_map_[idx].size()) {
        LOG_TRACE("index usage hashmap reaches max entries");
      } else if (OB_FAIL(index_usage_map_[idx].set_or_update(key, new_info, update_op))) {
        LOG_WARN("failed to set or update index-usage map", K(ret));
      }
    } else {
      LOG_WARN("failed to update index-usage map", K(ret));
    }
  }
}

void ObIndexUsageInfoMgr::refresh_config()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_LIKELY(tenant_config.is_valid())) {
    max_entries_ = tenant_config->_iut_max_entries.get();
    is_enabled_ = tenant_config->_iut_enable;
    is_sample_mode_ = tenant_config->_iut_stat_collection_type.get_value_string().case_compare("SAMPLED") == 0;
  }
}


} // namespace share
} // namespace oceanbase