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

#include "lib/utility/ob_print_utils.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "rpc/obrpc/ob_rpc_stat.h"             // RpcStatService
#include "observer/omt/ob_multi_tenant.h"      // ObMultiTenant
#include "observer/omt/ob_tenant.h"            // ObTenant
#include "share/config/ob_server_config.h"     // GCONF
#include "observer/ob_server_struct.h"         // GCTX
#include "share/ob_tenant_mgr.h"

namespace oceanbase
{
namespace lib
{
  int get_max_thread_num() {
    return 0 == GCONF._ob_max_thread_num ? INT32_MAX : static_cast<int32_t>(GCONF._ob_max_thread_num);
  }
}

namespace rpc
{
RpcStatService *get_stat_srv_by_tenant_id(uint64_t tenant_id)
{
  omt::ObMultiTenant *omt = GCTX.omt_;
  omt::ObTenant *tenant = nullptr;
  RpcStatService *srv = nullptr;
  if ((nullptr != omt) && (OB_SUCCESS == GCTX.omt_->get_tenant(tenant_id, tenant)) && (nullptr != tenant)) {
    srv = &(tenant->rpc_stat_info_->rpc_stat_srv_);
  }
  return srv;
}
}

namespace obrpc
{
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

} // namespace obrpc

namespace common
{
using namespace oceanbase::obrpc;

ObVirtualTenantManager::ObVirtualTenantManager()
  : tenant_map_(NULL),
    allocator_(ObModIds::OB_TENANT_INFO),
    memattr_(OB_SERVER_TENANT_ID, ObModIds::OB_TENANT_INFO),
    is_inited_(false)
{
}

ObVirtualTenantManager::~ObVirtualTenantManager()
{
  destroy();
}

ObVirtualTenantManager &ObVirtualTenantManager::get_instance()
{
  static ObVirtualTenantManager instance_;
  return instance_;
}

int ObVirtualTenantManager::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(init_tenant_map_())) {
    COMMON_LOG(WARN, "Fail to init tenant map, ", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

int ObVirtualTenantManager::init_tenant_map_()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (NULL == (buf = (char*) allocator_.alloc(sizeof(ObTenantBucket) * BUCKET_NUM))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "Fail to allocate memory, ", K(ret));
  } else {
    tenant_map_ = new (buf) ObTenantBucket[BUCKET_NUM];
  }
  return ret;
}

void ObVirtualTenantManager::destroy()
{
  tenant_map_ = NULL;
  allocator_.reset();
  is_inited_ = false;
}

int ObVirtualTenantManager::get_all_tenant_id(ObIArray<uint64_t> &key) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_NUM; ++i) {
      ObTenantBucket &bucket = tenant_map_[i];
      SpinRLockGuard guard(bucket.lock_);
      DLIST_FOREACH(iter, bucket.info_list_) {
        if (true == iter->is_loaded_) {
          if (OB_FAIL(key.push_back(iter->tenant_id_))) {
            COMMON_LOG(WARN, "Fail to add key to array, ", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObVirtualTenantManager::add_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) ||
             OB_UNLIKELY(!is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket &bucket = tenant_map_[pos];
    SpinWLockGuard guard(bucket.lock_);
    ObTenantInfo *node = NULL;
    if (OB_SUCC(bucket.get_the_node(tenant_id, node))) {
      //tenant is exist do nothing
    } else {
      ret = OB_SUCCESS;
      ObTenantInfo *info = OB_NEW(ObTenantInfo, memattr_);
      if (OB_ISNULL(info)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "Fail to alloc ObTenantInfo", K(ret));
      } else {
        info->reset();
        info->tenant_id_ = tenant_id;
        if (OB_UNLIKELY(!bucket.info_list_.add_last(info))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
          ob_delete(info);
        }
      }
    }
  }
  return ret;
}

int ObVirtualTenantManager::del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) ||
             OB_UNLIKELY(!is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket &bucket = tenant_map_[pos];
    SpinWLockGuard guard(bucket.lock_);
    ObTenantInfo *node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant has not exist", K(tenant_id), K(ret));
    } else {
      ObTenantInfo *info = NULL;
      info = bucket.info_list_.remove(node);
      if (NULL == info) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "The info is null, ", K(ret));
      } else {
        ob_delete(info);
        COMMON_LOG(INFO, "del_tenant succeed", K(tenant_id));
      }
    }
  }
  return ret;
}

bool ObVirtualTenantManager::has_tenant(const uint64_t tenant_id) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_INVALID_ID != tenant_id &&
             is_virtual_tenant_id(tenant_id)) {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket &bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo *node = NULL;
    if (OB_SUCC(bucket.get_the_node(tenant_id, node))) {
      if (NULL != node && true == node->is_loaded_) {
        bool_ret = true;
      }
    }
  }
  return bool_ret;
}

int ObVirtualTenantManager::set_tenant_mem_limit(
    const uint64_t tenant_id,
    const int64_t lower_limit,
    const int64_t upper_limit)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
             || OB_UNLIKELY(lower_limit < 0)
             || OB_UNLIKELY(upper_limit < 0)
             || OB_UNLIKELY(!is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id),
                     K(lower_limit), K(upper_limit));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket &bucket = tenant_map_[pos];
    SpinWLockGuard guard(bucket.lock_); // It should be possible to change to a read lock here, this lock is a structural lock, it is not appropriate to borrow
    ObTenantInfo *node = NULL;
    int64_t kv_cache_mem = 0;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else {
      node->mem_lower_limit_ = lower_limit;
      node->mem_upper_limit_ = upper_limit;
      if (OB_FAIL(get_kv_cache_mem_(tenant_id,
                                    kv_cache_mem))) {
        COMMON_LOG(WARN, "fail to get kv cache mem", K(ret), K(tenant_id));
      }
      node->is_loaded_ = true;
    }

    if (OB_SUCC(ret)) {
      COMMON_LOG(INFO, "set tenant mem limit",
                 "tenant id", tenant_id,
                 "mem_lower_limit", lower_limit,
                 "mem_upper_limit", upper_limit,
                 "mem_tenant_limit", get_tenant_memory_limit(node->tenant_id_),
                 "mem_tenant_hold", get_tenant_memory_hold(node->tenant_id_),
                 "kv_cache_mem", kv_cache_mem);
    }
  }
  return ret;
}

int ObVirtualTenantManager::get_tenant_mem_limit(
    const uint64_t tenant_id,
    int64_t &lower_limit,
    int64_t &upper_limit) const
{
  int ret = OB_SUCCESS;
  lower_limit = 0;
  upper_limit = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) ||
             OB_UNLIKELY(!is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket &bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo *node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      ret = OB_NOT_REGISTERED;
    } else {
      lower_limit = node->mem_lower_limit_;
      upper_limit = node->mem_upper_limit_;
    }
  }
  return ret;
}

int ObVirtualTenantManager::get_kv_cache_mem_(
    const uint64_t tenant_id,
    int64_t &kv_cache_mem)
{
  int ret = OB_SUCCESS;
  ObTenantResourceMgrHandle resource_handle;
  if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id,
                                                                    resource_handle))) {
    COMMON_LOG(WARN, "fail to get resource mgr", K(ret), K(tenant_id));
  } else {
    kv_cache_mem = resource_handle.get_memory_mgr()->get_cache_hold();
  }
  return ret;
}

int ObVirtualTenantManager::print_tenant_usage(
    char *print_buf,
    int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObTenantInfo *head = NULL;
  ObTenantInfo *node = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_NUM; ++i) {
    ObTenantBucket &bucket = tenant_map_[i];
    SpinRLockGuard guard(bucket.lock_);
    head = bucket.info_list_.get_header();
    node = bucket.info_list_.get_first();
    while (head != node && NULL != node && OB_SUCC(ret)) {
      ret = print_tenant_usage_(*node,
                                print_buf,
                                buf_len,
                                pos);
      node = node->get_next();
    }
  }
  return ret;
}

int ObVirtualTenantManager::print_tenant_usage_(
    ObTenantInfo &node,
    char *print_buf,
    int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  lib::ObMallocAllocator *mallocator = lib::ObMallocAllocator::get_instance();
  int64_t kv_cache_mem = 0;
  if (OB_FAIL(get_kv_cache_mem_(node.tenant_id_,
                                kv_cache_mem))) {
    COMMON_LOG(WARN, "get tenant kv cache mem error", K(ret), K(node.tenant_id_));
  } else {
    ret = databuff_printf(print_buf, buf_len, pos,
                          "[TENANT_MEMORY] "
                          "tenant_id=% '9ld "
                          "mem_tenant_limit=% '15ld "
                          "mem_tenant_hold=% '15ld "
                          "kv_cache_mem=% '15ld\n",
                          node.tenant_id_,
                          get_tenant_memory_limit(node.tenant_id_),
                          get_tenant_memory_hold(node.tenant_id_),
                          kv_cache_mem);
  }
  if (!OB_ISNULL(mallocator)) {
    mallocator->print_tenant_memory_usage(node.tenant_id_);
    mallocator->print_tenant_ctx_memory_usage(node.tenant_id_);
  }
  return ret;
}

void ObVirtualTenantManager::reload_config()
{
  // only reload the config of tenant memstore.
  // virtual tenant no need reload_config
}

ObVirtualTenantManager::ObTenantInfo::ObTenantInfo()
  :	tenant_id_(INT64_MAX),
    mem_lower_limit_(0),
    mem_upper_limit_(0),
    is_loaded_(false)
{
}

void ObVirtualTenantManager::ObTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID; // i64 max as invalid.
  mem_lower_limit_ = 0;
  mem_upper_limit_ = 0;
  is_loaded_ = false;
}

int64_t ObTenantCpuShare::calc_px_pool_share(uint64_t tenant_id, int64_t min_cpu)
{
  int64_t share = 3;
  int ret = OB_SUCCESS;
  oceanbase::omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    share = 3;
    COMMON_LOG(ERROR, "fail get tenant config. share default to 3", K(share));
  } else {
    share = std::max(3L, min_cpu * tenant_config->px_workers_per_cpu_quota);
  }
  return share;
}

} // namespace common
} // namespace oceanbase
