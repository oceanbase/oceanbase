/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SQL_QRR
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/thread_mgr.h"
#include "lib/rc/ob_rc.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/alloc/alloc_func.h"
#include "observer/ob_server.h"
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "sql/udr/ob_udr_mgr.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
namespace sql
{

void UDRBackupRecoveryGuard::backup()
{
  is_prepare_protocol_ = sql_ctx_.is_prepare_protocol_;
  cur_sql_ = sql_ctx_.cur_sql_;
  mode_ = pc_ctx_.mode_;
}

void UDRBackupRecoveryGuard::recovery()
{
  sql_ctx_.is_prepare_protocol_ = is_prepare_protocol_;
  sql_ctx_.cur_sql_ = cur_sql_;
  pc_ctx_.is_rewrite_sql_ = false;
  pc_ctx_.def_name_ctx_ = nullptr;
  pc_ctx_.mode_ = mode_;
  if (!is_prepare_protocol_) {
    const_cast<ObString &>(pc_ctx_.raw_sql_) = cur_sql_;
    pc_ctx_.is_parameterized_execute_ = false;
    pc_ctx_.fp_result_.parameterized_params_.reuse();
  }
}

UDRTmpAllocatorGuard::~UDRTmpAllocatorGuard()
{
  if (inited_) {
    DESTROY_CONTEXT(mem_context_);
  }
}

int UDRTmpAllocatorGuard::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(tenant_id, "QueryRewriteSql", ObCtxIds::DEFAULT_CTX_ID)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL)
       .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("failed to create memory entity", K(ret));
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create memory entity", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

ObIAllocator* UDRTmpAllocatorGuard::get_allocator()
{
  return inited_ ? &mem_context_->get_arena_allocator() : nullptr;
}

void ObUDRRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(rule_mgr_->tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret), K(rule_mgr_->tenant_id_));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    // do nothing
  } else if (OB_NOT_NULL(rule_mgr_) && rule_mgr_->inited_) {
    LOG_INFO("run rewrite rule refresh task", K(rule_mgr_->tenant_id_));
    if (OB_FAIL(rule_mgr_->sync_rule_from_inner_table())) {
      LOG_WARN("failed to sync rule from inner table", K(ret));
    }
  }
}

ObUDRMgr::~ObUDRMgr()
{
  if (inited_) {
    destroy();
  }
}

int ObUDRMgr::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObPCMemPctConf default_conf;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rewrite rule mgr init twice", K(ret));
  } else if (OB_FAIL(sql_service_.init(&ObServer::get_instance().get_mysql_proxy()))) {
    LOG_WARN("failed to init rewrite rule sql service", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
    LOG_WARN("failed to create tg", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("failed to start tg", K(ret));
  } else if (OB_FAIL(init_mem_context(tenant_id))) {
    LOG_WARN("failed to init mem context", K(ret));
  } else if (OB_FAIL(rule_item_mgr_.init(tenant_id, *inner_allocator_))) {
    LOG_WARN("failed to init rule item manager", K(ret));
  } else {
    refresh_task_.rule_mgr_ = this;
    if (OB_FAIL(TG_SCHEDULE(tg_id_, refresh_task_, ObUDRRefreshTask::REFRESH_INTERVAL, true))) {
      LOG_WARN("failed to schedule refresh task", K(ret));
    } else {
      tenant_id_ = tenant_id;
      inited_ = true;
      destroyed_ = false;
    }
  }

  if (OB_FAIL(ret) && !inited_) {
    destroy();
  }
  return ret;
}

int ObUDRMgr::init_mem_context(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, "RewriteRuleMgr", common::ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem entity is null", K(ret));
    } else {
      inner_allocator_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

int ObUDRMgr::mtl_init(ObUDRMgr* &node_list)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = lib::current_resource_owner_id();
  if (OB_FAIL(node_list->init(tenant_id))) {
    LOG_WARN("failed to init event list", K(ret));
  }
  return ret;
}

void ObUDRMgr::mtl_stop(ObUDRMgr* &udr_mgr)
{
  if (NULL != udr_mgr && udr_mgr->inited_) {
    TG_CANCEL(udr_mgr->tg_id_, udr_mgr->refresh_task_);
    TG_STOP(udr_mgr->tg_id_);
  }
}

void ObUDRMgr::destroy()
{
  if (!destroyed_) {
    inited_ = false;
    destroyed_ = true;
    TG_DESTROY(tg_id_);
    rule_item_mgr_.destroy();
    if (OB_LIKELY(nullptr != inner_allocator_)) {
      inner_allocator_ = nullptr;
    }
    if (OB_LIKELY(nullptr != mem_context_)) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
}

int ObUDRMgr::insert_rule(ObUDRInfo &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_service_.insert_rule(arg))) {
    LOG_WARN("failed to create rewrite rule", K(ret), K(arg));
  }
  return ret;
}

int ObUDRMgr::alter_rule_status(ObUDRInfo &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_service_.alter_rule_status(arg))) {
    LOG_WARN("failed to alter rewrite rule status", K(ret), K(arg));
  }
  return ret;
}

int ObUDRMgr::remove_rule(ObUDRInfo &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_service_.remove_rule(arg))) {
    LOG_WARN("failed to remove rewrite rule", K(ret), K(arg));
  }
  return ret;
}

int ObUDRMgr::get_udr_item(const ObUDRContext &rule_ctx,
                           ObUDRItemMgr::UDRItemRefGuard &item_guard,
                           PatternConstConsList *cst_cons_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rule_item_mgr_.get_udr_item(rule_ctx, item_guard, cst_cons_list))) {
    LOG_DEBUG("failed to get rewrite rule item", K(ret), K(rule_ctx));
  }
  return ret;
}

int ObUDRMgr::fuzzy_check_by_pattern_digest(const uint64_t pattern_digest, bool &is_exists)
{
  int ret = OB_SUCCESS;
  is_exists = false;
  if (OB_FAIL(rule_item_mgr_.fuzzy_check_by_pattern_digest(pattern_digest, is_exists))) {
    LOG_WARN("failed to fuzzy check by pattern digest", K(ret), K(pattern_digest));
  }
  return ret;
}

int ObUDRMgr::sync_rule_from_inner_table()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  LOG_DEBUG("sync rule from inner table", K(tenant_id_), K(rule_version_));
  int64_t max_rule_version = OB_INVALID_VERSION;
  ObSEArray<ObUDRInfo, 32> rule_infos;
  UDRTmpAllocatorGuard alloc_guard;
  ObIAllocator* allocator = nullptr;
  if (OB_FAIL(sql_service_.fetch_max_rule_version(tenant_id_, max_rule_version))) {
    LOG_WARN("failed to fetch max rule version", K(ret));
  } else if (rule_version_ >= max_rule_version) {
    LOG_TRACE("local version is latest, don't need refresh", K(tenant_id_), K(rule_version_), K(max_rule_version));
  } else if (OB_FAIL(alloc_guard.init(tenant_id_))) {
    LOG_WARN("failed to init allocator guard", K(ret));
  } else if (OB_ISNULL(allocator = alloc_guard.get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null allocator", K(ret));
  } else {
    LOG_INFO("local version is not latest, need refresh", K(tenant_id_), K(rule_version_), K(max_rule_version));
    if (OB_FAIL(sql_service_.clean_up_items_marked_for_deletion(tenant_id_))) {
      LOG_WARN("failed to clean up items marked for deletion", K(ret));
    } else if (OB_FAIL(sql_service_.get_need_sync_rule_infos(*allocator, tenant_id_, rule_version_, rule_infos))) {
      LOG_WARN("failed to get need sync rule infos", K(ret), K(rule_version_), K(max_rule_version));
    } else if (rule_infos.empty()) {
      // do nothing
      LOG_TRACE("rule infos is empty", K(tenant_id_), K(rule_version_), K(max_rule_version));
    } else if (OB_FAIL(rule_item_mgr_.sync_local_cache_rules(rule_infos))) {
      LOG_WARN("failed to sync local cache rules", K(ret));
    } else {
      set_rule_version(max_rule_version);
    }
  }
  return ret;
}

} // namespace sql end
} // namespace oceanbase end
