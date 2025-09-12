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
 *
 *  TransCtx: Context for Distributed Transactions
 */

#include "ob_cdc_mem_mgr.h"
#include "ob_log_utils.h"
#include "ob_log_instance.h"
#include "ob_log_config.h"                // ObLogConfig
#include "ob_cdc_malloc_sample_info.h"    // ObCDCMallocSampleInfo

#define USING_LOG_PREFIX OBLOG

namespace oceanbase
{
namespace libobcdc
{
ObCDCMemController &ObCDCMemController::get_instance()
{
  static ObCDCMemController mem_controller;
  return mem_controller;
}

ObCDCMemController::ObCDCMemController()
  : enable_hard_mem_limit_(false),
    hard_mem_limit_threshold_(0),
    hard_mem_limit_(0)
{}

void ObCDCMemController::reset()
{
  enable_hard_mem_limit_ = false;
  hard_mem_limit_threshold_ = 0;
  hard_mem_limit_ = 0;
}

int ObCDCMemController::init(const ObLogConfig &config)
{
  int ret = OB_SUCCESS;
  enable_hard_mem_limit_ = (1 == config.enable_hard_mem_limit);
  refresh_hard_mem_limit_(config);
  return ret;
}

void ObCDCMemController::configure(const ObLogConfig &config)
{
  refresh_hard_mem_limit_(config);
}

void ObCDCMemController::refresh_hard_mem_limit_(const ObLogConfig &config)
{
  _LOG_INFO("[CONFIG] enable_hard_mem_limit=%ld, hard_mem_limit_threshold=%ld", config.enable_hard_mem_limit.get(), config.hard_mem_limit_threshold.get());
  if (enable_hard_mem_limit_) {
    const int64_t memory_limit = config.memory_limit.get();
    const int64_t hard_mem_limit_threshold = config.hard_mem_limit_threshold.get();
    const int64_t hard_mem_limit = memory_limit * hard_mem_limit_threshold / 100;
    if (hard_mem_limit_ != hard_mem_limit) {
      lib::set_memory_limit(hard_mem_limit);
      _LOG_INFO("[MEM_LIMIT] config changed, old_limit: %s, new_limit: %s",
          SIZE_TO_STR(hard_mem_limit_), SIZE_TO_STR(hard_mem_limit));
      hard_mem_limit_threshold_ = hard_mem_limit_threshold;
      hard_mem_limit_ = hard_mem_limit;
    }
  }
}

void ObCDCMemController::handle_when_alloc_fail(const char* label, bool &hang_on_alloc_fail)
{
  const int64_t print_interval = 10 * _SEC_;
  hang_on_alloc_fail = true;
  if (REACH_TIME_INTERVAL(print_interval)) {
    LOG_INFO("memory alloc failed, will retry until success", KCSTRING(label));
  }
  ob_usleep(100);
}

void ObCDCMemController::print_mem_stat()
{
  int ret = OB_SUCCESS;
  static const int64_t BUF_LEN = 64LL << 10;
  static char print_buf[BUF_LEN];
  int64_t pos = CHUNK_MGR.to_string(print_buf, BUF_LEN);
  // print CHUNK_MGR info for cached memory
  _LOG_INFO("%.*s", static_cast<int>(pos), print_buf);
  lib::ObMallocAllocator *mallocator = lib::ObMallocAllocator::get_instance();
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  if (OB_ISNULL(mallocator)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "mallocator is NULL, can not print_tenant_memory_usage");
  } else if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant_mgr is NULL, can not print_tenant_memory_usage for each tenant");
  } else {
    std::vector<uint64_t> tenant_ids;
    if (TCTX.enable_filter_sys_tenant_) {
      //.print sys tenant memory usage here
      mallocator->print_tenant_memory_usage(OB_SYS_TENANT_ID);
      mallocator->print_tenant_ctx_memory_usage(OB_SYS_TENANT_ID);
    }
    mallocator->print_tenant_memory_usage(OB_SERVER_TENANT_ID);
    mallocator->print_tenant_ctx_memory_usage(OB_SERVER_TENANT_ID);

    if (OB_FAIL(tenant_mgr->get_all_tenant_ids(tenant_ids))) {
      LOG_ERROR("get_all_tenant_ids failed", KR(ret));
    } else {
      for (uint64_t tenant_id: tenant_ids) {
        mallocator->print_tenant_memory_usage(tenant_id);
        mallocator->print_tenant_ctx_memory_usage(tenant_id);
      }
    }
  }
}

void ObCDCMemController::dump_malloc_sample()
{
  int ret = OB_SUCCESS;
  static int64_t last_print_ts = 0;
  lib::ObMallocSampleMap malloc_sample_map;
  ObCDCMallocSampleInfo sample_info;
  const int64_t cur_time = get_timestamp();
  const int64_t print_interval = TCONF.print_mod_memory_usage_interval.get();

  if (OB_LIKELY(last_print_ts + print_interval > cur_time)) {
  } else if (OB_FAIL(malloc_sample_map.create(1000, "MallocInfoMap", "MallocInfoMap"))) {
    LOG_WARN("init malloc_sample_map failed", KR(ret));
  } else if (OB_FAIL(ObMemoryDump::get_instance().load_malloc_sample_map(malloc_sample_map))) {
    LOG_WARN("load_malloc_sample_map failed", KR(ret));
  } else if (OB_FAIL(sample_info.init(malloc_sample_map))) {
    LOG_ERROR("init ob_cdc_malloc_sample_info failed", KR(ret));
  } else {
    const static int64_t top_mem_usage_mod_print_num = 5;
    const int64_t print_mod_memory_usage_threshold = TCONF.print_mod_memory_usage_threshold.get();
    const char *print_mod_memory_usage_label = TCONF.print_mod_memory_usage_label;
    sample_info.print_topk(top_mem_usage_mod_print_num);
    sample_info.print_with_filter(print_mod_memory_usage_label, print_mod_memory_usage_threshold);
    last_print_ts = cur_time;
  }
}

} // end of namespace libobcdc
} // end of namespace oceanbase
