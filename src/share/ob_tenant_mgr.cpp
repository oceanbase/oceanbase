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
#include "lib/alloc/malloc_hook.h"
#include "share/ob_tenant_mgr.h"
#include "share/rc/ob_context.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_service.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server.h"

int64_t get_virtual_memory_used()
{
  constexpr int BUFFER_SIZE = 128;
  char buf[BUFFER_SIZE];
  snprintf(buf, BUFFER_SIZE, "/proc/%d/status", getpid());
  std::ifstream status(buf);
  int64_t used = 0;
  while (status && 0 == used) {
    status >> buf;
    if (strncmp(buf, "VmSize:", BUFFER_SIZE) == 0) {
      status >> buf;
      used = std::stoi(buf);
    } else {
      status.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
  }
  return used * 1024;
}

namespace oceanbase {
namespace obrpc {
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;
typedef ObMemstoreAllocatorMgr::TAllocator ObTenantMemstoreAllocator;

DEF_TO_STRING(ObTenantFreezeArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(freeze_type));
  return pos;
}

OB_SERIALIZE_MEMBER(ObTenantFreezeArg, tenant_id_, freeze_type_, try_frozen_version_);

int ObTenantMgrRpcCb::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTenantManager::get_instance().rpc_callback())) {
    COMMON_LOG(WARN, "rpc callback failed", K(ret));
  }
  return ret;
}

void ObTenantMgrRpcCb::on_timeout()
{
  int ret = OB_SUCCESS;
  COMMON_LOG(INFO, "Tenant mgr major freeze request timeout");
  if (OB_FAIL(ObTenantManager::get_instance().rpc_callback())) {
    COMMON_LOG(WARN, "rpc callback failed", K(ret));
  }
}

int64_t ObTenantMgrP::minor_freeze_token_ = ObTenantMgrP::MAX_CONCURRENT_MINOR_FREEZING;

int ObTenantMgrP::process()
{
  int ret = OB_SUCCESS;
  ObTenantManager& tenant_mgr = ObTenantManager::get_instance();

  if (storage::MINOR_FREEZE == arg_.freeze_type_) {
    // get freeze token
    int64_t oldv = ATOMIC_LOAD(&minor_freeze_token_);
    bool finish = false;
    while (oldv > 0 && !finish) {
      int64_t newv = oldv - 1;
      finish = (oldv == (newv = ATOMIC_VCAS(&minor_freeze_token_, oldv, newv)));
      oldv = newv;
    }

    if (!finish) {
      COMMON_LOG(
          INFO, "fail to do minor freeze due to no token left", "tenant_id", arg_.tenant_id_, K_(minor_freeze_token));
    } else {
      bool marked = false;
      // set freezing mark for tenant
      if (OB_FAIL(tenant_mgr.set_tenant_freezing(arg_.tenant_id_, marked))) {
        COMMON_LOG(WARN, "fail to set tenant freezing", K_(arg), K(ret));
      } else if (marked) {
        bool rollback_freeze_cnt = false;
        if (OB_FAIL(partition_service_->minor_freeze(arg_.tenant_id_))) {
          rollback_freeze_cnt = true;
          COMMON_LOG(WARN, "fail to minor freeze", K_(arg), K(ret));
        } else {
          COMMON_LOG(INFO, "finish tenant minor freeze", K_(arg), K(ret));
        }
        // clear freezing mark for tenant
        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(
                OB_SUCCESS != (tmp_ret = tenant_mgr.unset_tenant_freezing(arg_.tenant_id_, rollback_freeze_cnt)))) {
          COMMON_LOG(WARN, "unset tenant freezing mark failed", K_(arg), K(tmp_ret));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      } else {
        COMMON_LOG(WARN, "previous minor freeze exist, skip this time", K_(arg));
      }
      // turn over freeze token
      ATOMIC_INC(&minor_freeze_token_);
    }
  } else if (storage::MAJOR_FREEZE == arg_.freeze_type_) {
    uint64_t tenant_id = arg_.tenant_id_;
    common::ObAddr rs_addr;
    Int64 frozen_version;
    ObRetryMajorInfo retry_major_info = tenant_mgr.get_retry_major_info();
    retry_major_info.tenant_id_ = tenant_id;
    retry_major_info.major_version_.version_ = arg_.try_frozen_version_;

    if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
      COMMON_LOG(WARN, "get master root service address failed", K(ret));
    } else if (!rs_addr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid rootserver addr", K(ret), K(rs_addr));
    } else if (OB_FAIL(rpc_proxy_->to(rs_addr).get_frozen_version(frozen_version))) {
      COMMON_LOG(WARN, "get_frozen_version failed", K(rs_addr), K(ret));
    } else {
      bool need_major = true;

      if (arg_.try_frozen_version_ > 0) {
        if (OB_UNLIKELY(arg_.try_frozen_version_ > frozen_version + 1)) {
          ret = OB_ERR_UNEXPECTED;
          need_major = false;
          COMMON_LOG(WARN,
              "wrong frozen version",
              K(ret),
              K(tenant_id),
              K(rs_addr),
              K(frozen_version),
              K(arg_.try_frozen_version_));
        } else if (arg_.try_frozen_version_ <= frozen_version) {
          need_major = false;
        } else {  // retry_major_info.major_version_.version_ == frozen_version + 1
          need_major = true;
        }
      } else if (!tenant_mgr.tenant_need_major_freeze(tenant_id)) {
        need_major = false;
      }

      if (!need_major) {
        retry_major_info.reset();
      } else {
        ObRootMajorFreezeArg arg;
        arg.try_frozen_version_ = frozen_version + 1;
        arg.launch_new_round_ = true;
        arg.svr_ = MYADDR;
        arg.tenant_id_ = arg_.tenant_id_;
        retry_major_info.major_version_.version_ = arg.try_frozen_version_;

        COMMON_LOG(INFO, "root_major_freeze", K(rs_addr), K(arg));

        if (OB_FAIL(rpc_proxy_->to(rs_addr).root_major_freeze(arg))) {
          COMMON_LOG(WARN, "fail to major freeze", K(arg), K(ret));
        } else {
          retry_major_info.reset();
        }
      }
    }

    tenant_mgr.set_retry_major_info(retry_major_info);
    COMMON_LOG(INFO, "finish tenant manager major freeze", K(ret), K(rs_addr));
  } else {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "unknown freeze type", K(arg_), K(ret));
  }
  return ret;
}

}  // namespace obrpc

namespace common {
using namespace oceanbase::obrpc;
using namespace oceanbase::storage;

void get_tenant_ids(uint64_t* ids, int cap, int& cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  omt::ObMultiTenant* omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    if (ObTenantManager::get_instance().is_inited()) {
      common::ObArray<uint64_t> tmp_ids;
      if (OB_FAIL(ObTenantManager::get_instance().get_all_tenant_id(tmp_ids))) {
        SERVER_LOG(WARN, "get tenant id error", K(ret));
      } else {
        for (auto it = tmp_ids.begin(); OB_SUCC(ret) && it != tmp_ids.end() && cnt < cap; it++) {
          ids[cnt++] = *it;
        }
      }
    } else {
      if (cnt < cap) {
        ids[cnt++] = OB_SYS_TENANT_ID;
      }
      if (cnt < cap) {
        ids[cnt++] = OB_SERVER_TENANT_ID;
      }
    }
  } else {
    omt::TenantIdList tmp_ids(nullptr, ObModIds::OMT);
    omt->get_tenant_ids(tmp_ids);
    for (auto it = tmp_ids.begin(); OB_SUCC(ret) && it != tmp_ids.end() && cnt < cap; it++) {
      ids[cnt++] = *it;
    }
  }
}

void ObTenantMgrTimerTask::runTimerTask()
{
  COMMON_LOG(INFO, "====== tenant manager timer task ======");
  int ret = OB_SUCCESS;
  ObTenantManager& tenant_mgr = ObTenantManager::get_instance();
  if (OB_FAIL(tenant_mgr.check_and_do_freeze_mixed())) {
    COMMON_LOG(WARN, "check and do minor freeze failed", K(ret));
  }
}

void ObPrintTenantMemstoreUsage::runTimerTask()
{
  COMMON_LOG(INFO, "=== Run print tenant memstore usage task ===");
  ObTenantManager& tenant_mgr = ObTenantManager::get_instance();
  tenant_mgr.print_tenant_usage();
  ObObjFreeListList::get_freelists().dump();
}

ObTenantInfo::ObTenantInfo()
    : tenant_id_(INT64_MAX),
      mem_lower_limit_(0),
      mem_upper_limit_(0),
      mem_memstore_limit_(0),
      is_loaded_(false),
      is_freezing_(false),
      last_freeze_clock_(0),
      frozen_version_(0),
      freeze_cnt_(0),
      last_halt_ts_(0)
{
  memset(disk_used_, 0, OB_MAX_MACRO_BLOCK_TYPE * sizeof(uint64_t));
}

void ObTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;  // i64 max as invalid.
  mem_memstore_limit_ = 0;
  mem_lower_limit_ = 0;
  mem_upper_limit_ = 0;
  is_loaded_ = false;
  is_freezing_ = false;
  frozen_version_ = 0;
  freeze_cnt_ = 0;
  last_halt_ts_ = 0;
}

int ObTenantInfo::update_frozen_version(int64_t frozen_version)
{
  int ret = OB_SUCCESS;

  if (frozen_version > frozen_version_) {
    frozen_version_ = frozen_version;
    freeze_cnt_ = 0;
  }

  return ret;
}

int64_t ObTenantInfo::mem_memstore_left() const
{
  uint64_t memstore_hold = get_tenant_memory_hold(tenant_id_, ObCtxIds::MEMSTORE_CTX_ID);
  return max(0, mem_memstore_limit_ - (int64_t)memstore_hold);
}

ObTenantManager::ObTenantManager()
    : tenant_map_(NULL),
      tenant_pool_(),
      allocator_(ObModIds::OB_TENANT_INFO),
      memattr_(default_memattr),
      freeze_task_(),
      print_task_(),
      rpc_proxy_(),
      tenant_mgr_cb_(),
      svr_rpc_proxy_(NULL),
      common_rpc_proxy_(NULL),
      rs_mgr_(NULL),
      self_(),
      config_(NULL),
      all_tenants_freeze_trigger_(INT64_MAX),
      all_tenants_memstore_limit_(INT64_MAX),
      is_inited_(false),
      retry_major_info_(),
      allocator_mgr_(NULL)
{}

ObTenantManager::~ObTenantManager()
{
  destroy();
}

ObTenantManager& ObTenantManager::get_instance()
{
  static ObTenantManager instance_;
  return instance_;
}

// this init is for obproxy only
int ObTenantManager::init(const int64_t tenant_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(tenant_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(tenant_cnt), K(ret));
  } else if (OB_FAIL(init_tenant_map(tenant_cnt))) {
    COMMON_LOG(WARN, "Fail to init tenant map, ", K(ret));
  }
  if (OB_SUCC(ret)) {
    allocator_mgr_ = &ObMemstoreAllocatorMgr::get_instance();
    is_inited_ = true;
  } else if (OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}
// in observer, we only use this init func
int ObTenantManager::init(const ObAddr& self, obrpc::ObSrvRpcProxy& rpc_proxy,
    obrpc::ObCommonRpcProxy& common_rpc_proxy, const share::ObRsMgr& rs_mgr, rpc::frame::ObReqTransport* req_transport,
    ObServerConfig* config, const int64_t tenant_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!self.is_valid()) || OB_UNLIKELY(NULL == req_transport) || OB_UNLIKELY(NULL == config) ||
             OB_UNLIKELY(tenant_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(self), K(req_transport), K(config), K(tenant_cnt), K(ret));
  } else if (OB_FAIL(init_tenant_map(tenant_cnt))) {
    COMMON_LOG(WARN, "Fail to init tenant map, ", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = rpc_proxy_.init(req_transport, self))) {
      COMMON_LOG(WARN, "fail to init rpc proxy", K(ret));
    } else {
      self_ = self;
      svr_rpc_proxy_ = &rpc_proxy;
      common_rpc_proxy_ = &common_rpc_proxy;
      rs_mgr_ = &rs_mgr;
      config_ = config;
      ATOMIC_STORE(&all_tenants_freeze_trigger_,
          config_->get_server_memory_avail() * config_->get_global_freeze_trigger_percentage() / 100);
      ATOMIC_STORE(&all_tenants_memstore_limit_,
          config_->get_server_memory_avail() * config_->get_global_memstore_limit_percentage() / 100);
      // make sure a major freeze request can be sent the first time after start
      allocator_mgr_ = &ObMemstoreAllocatorMgr::get_instance();
      is_inited_ = true;
    }
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

int ObTenantManager::init_tenant_map(const int64_t tenant_cnt)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (OB_UNLIKELY(tenant_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(tenant_cnt), K(ret));
  } else if (NULL == (buf = (char*)allocator_.alloc(
                          (sizeof(ObTenantInfo*) * tenant_cnt) + sizeof(ObTenantInfo) * tenant_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "Fail to allocate memory, ", K(ret));
  } else if (OB_FAIL(tenant_pool_.init(tenant_cnt, buf))) {
    COMMON_LOG(WARN, "Fail to init tenant pool, ", K(ret));
  } else {
    buf += (sizeof(ObTenantInfo*) * tenant_cnt);
    ObTenantInfo* info = new (buf) ObTenantInfo[tenant_cnt];
    for (int64_t idx = 0; idx < tenant_cnt && OB_SUCC(ret); ++idx) {
      info[idx].reset();
      if (OB_FAIL(tenant_pool_.push(&(info[idx])))) {
        COMMON_LOG(WARN, "Fail to push info to pool, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == (buf = (char*)allocator_.alloc(sizeof(ObTenantBucket) * BUCKET_NUM))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "Fail to allocate memory, ", K(ret));
      } else {
        tenant_map_ = new (buf) ObTenantBucket[BUCKET_NUM];
      }
    }
  }
  return ret;
}

void ObTenantManager::destroy()
{
  tenant_map_ = NULL;
  tenant_pool_.destroy();
  allocator_.reset();
  rpc_proxy_.destroy();
  self_.reset();
  config_ = NULL;
  all_tenants_freeze_trigger_ = INT64_MAX;
  all_tenants_memstore_limit_ = INT64_MAX;
  is_inited_ = false;
}

int ObTenantManager::print_tenant_node(
    ObTenantInfo& node, char* print_buf, int64_t buf_len, int64_t& pos, int64_t& total_active_memstore_hold)
{
  int ret = OB_SUCCESS;
  lib::ObMallocAllocator* mallocator = lib::ObMallocAllocator::get_instance();
  int64_t mem_active_memstore_used = 0;
  int64_t mem_total_memstore_used = 0;
  int64_t mem_total_memstore_hold = 0;
  int64_t minor_freeze_trigger_limit = 0;
  int64_t max_mem_memstore_can_get_now = 0;
  int64_t kv_cache_mem = 0;

  if (OB_FAIL(get_tenant_mem_usage(
          node.tenant_id_, mem_active_memstore_used, mem_total_memstore_used, mem_total_memstore_hold))) {
    COMMON_LOG(WARN, "fail to get mem usage", K(ret), K(node.tenant_id_));
  } else {
    total_active_memstore_hold += mem_active_memstore_used;
    if (OB_FAIL(get_tenant_minor_freeze_trigger(node.tenant_id_,
            node.mem_memstore_limit_,
            max_mem_memstore_can_get_now,
            kv_cache_mem,
            minor_freeze_trigger_limit))) {
      COMMON_LOG(WARN, "get tenant minor freeze trigger error", K(ret), K(node.tenant_id_));
    } else {
      ret = databuff_printf(print_buf,
          buf_len,
          pos,
          "[TENANT_MEMSTORE] "
          "tenant_id=% '9ld "
          "active_memstore_used=% '15ld "
          "total_memstore_used=% '15ld "
          "total_memstore_hold=% '15ld "
          "minor_freeze_trigger_limit=% '15ld "
          "memstore_limit=% '15ld "
          "mem_tenant_limit=% '15ld "
          "mem_tenant_hold=% '15ld "
          "mem_memstore_used=% '15ld "
          "kv_cache_mem=% '15ld "
          "max_mem_memstore_can_get_now=% '15ld\n",
          node.tenant_id_,
          mem_active_memstore_used,
          mem_total_memstore_used,
          mem_total_memstore_hold,
          minor_freeze_trigger_limit,
          node.mem_memstore_limit_,
          get_tenant_memory_limit(node.tenant_id_),
          get_tenant_memory_hold(node.tenant_id_),
          get_tenant_memory_hold(node.tenant_id_, ObCtxIds::MEMSTORE_CTX_ID),
          kv_cache_mem,
          max_mem_memstore_can_get_now);
    }
  }

  if (!OB_ISNULL(mallocator)) {
    mallocator->print_tenant_memory_usage(node.tenant_id_);
    mallocator->print_tenant_ctx_memory_usage(node.tenant_id_);
  }

  return ret;
}

int ObTenantManager::print_tenant_usage()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_FAIL(print_mutex_.trylock())) {
    // Guaranteed serial printing
    // do-nothing
  } else {
    static const int64_t BUF_LEN = 64LL << 10;
    static char print_buf[BUF_LEN] = "";
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(print_buf,
            BUF_LEN,
            pos,
            "=== TENANTS MEMSTORE INFO ===\n"
            "all_tenants_memstore_used=% '15ld "
            "all_tenants_memstore_limit=% '15ld\n",
            allocator_mgr_->get_all_tenants_memstore_used(),
            all_tenants_memstore_limit_))) {
    } else {
      ObTenantInfo* head = NULL;
      ObTenantInfo* node = NULL;
      int64_t total_active_memstore_hold = 0;
      omt::ObMultiTenant* omt = GCTX.omt_;
      if (OB_ISNULL(omt)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_NUM; ++i) {
          ObTenantBucket& bucket = tenant_map_[i];
          SpinRLockGuard guard(bucket.lock_);
          head = bucket.info_list_.get_header();
          node = bucket.info_list_.get_first();
          while (head != node && NULL != node && OB_SUCC(ret)) {
            ret = print_tenant_node(*node, print_buf, BUF_LEN, pos, total_active_memstore_hold);
            node = node->get_next();
          }
        }
      } else {
        omt::TenantIdList ids(nullptr, ObModIds::OMT);
        omt->get_tenant_ids(ids);
        for (omt::TenantIdList::iterator it = ids.begin(); OB_SUCC(ret) && it != ids.end(); it++) {
          uint64_t id = *it;
          ObTenantBucket& bucket = tenant_map_[id % BUCKET_NUM];
          SpinWLockGuard guard(bucket.lock_);
          ObTenantInfo* node = NULL;
          if (OB_FAIL(bucket.get_the_node(id, node))) {
            // tenant is not exist do nothing
            ret = OB_SUCCESS;
            lib::ObMallocAllocator* mallocator = lib::ObMallocAllocator::get_instance();
            if (!OB_ISNULL(mallocator)) {
              mallocator->print_tenant_memory_usage(id);
              mallocator->print_tenant_ctx_memory_usage(id);
            }
          } else {
            ret = print_tenant_node(*node, print_buf, BUF_LEN, pos, total_active_memstore_hold);
          }
        }
      }

      if (OB_SUCC(ret)) {
        ret = databuff_printf(print_buf,
            BUF_LEN,
            pos,
            "[TENANT_MEMSTORE] "
            "total_active_memstore_hold=% '15ld "
            "all_tenants_freeze_trigger=% '15ld\n",
            total_active_memstore_hold,
            all_tenants_freeze_trigger_);
      }

      if (OB_SIZE_OVERFLOW == ret) {
        // If the buffer is not enough, truncate directly
        ret = OB_SUCCESS;
        print_buf[BUF_LEN - 2] = '\n';
        print_buf[BUF_LEN - 1] = '\0';
      }
      if (OB_SUCCESS == ret) {
        _COMMON_LOG(INFO, "====== tenants memstore info ======\n%s", print_buf);
      }

      // print global chunk freelist

      int64_t memory_used = get_virtual_memory_used();
      _COMMON_LOG(INFO,
          "[CHUNK_MGR] free=%ld pushes=%ld pops=%ld limit=%'15ld hold=%'15ld used=%'15ld"
          " freelist_hold=%'15ld maps=%'15ld unmaps=%'15ld large_maps=%'15ld large_unmaps=%'15ld"
          " memalign=%d virtual_memory_used=%'15ld\n",
          CHUNK_MGR.get_free_chunk_count(),
          CHUNK_MGR.get_free_chunk_pushes(),
          CHUNK_MGR.get_free_chunk_pops(),
          CHUNK_MGR.get_limit(),
          CHUNK_MGR.get_hold(),
          CHUNK_MGR.get_used(),
          CHUNK_MGR.get_freelist_hold(),
          CHUNK_MGR.get_maps(),
          CHUNK_MGR.get_unmaps(),
          CHUNK_MGR.get_large_maps(),
          CHUNK_MGR.get_large_unmaps(),
          0,
          memory_used);
    }
    print_mutex_.unlock();
  }

  return ret;
}

int ObTenantManager::get_all_tenant_id(ObIArray<uint64_t>& key) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_NUM; ++i) {
      ObTenantBucket& bucket = tenant_map_[i];
      SpinRLockGuard guard(bucket.lock_);
      DLIST_FOREACH(iter, bucket.info_list_)
      {
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

int ObTenantManager::add_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinWLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_SUCC(bucket.get_the_node(tenant_id, node))) {
      // tenant is exist do nothing
    } else {
      ObTenantInfo* info = NULL;
      if (OB_FAIL(tenant_pool_.pop(info))) {
        COMMON_LOG(WARN, "Fail to pop info from pool, ", K(ret));
      } else {
        info->reset();
        info->tenant_id_ = tenant_id;
        if (OB_UNLIKELY(!bucket.info_list_.add_last(info))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
          if (OB_SUCCESS != (tmp_ret = tenant_pool_.push(info))) {
            COMMON_LOG(WARN, "Fail to push collects to pool, ", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantManager::del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinWLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant has not exist", K(tenant_id), K(ret));
    } else {
      ObTenantInfo* info = NULL;
      info = bucket.info_list_.remove(node);
      if (NULL == info) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "The info is null, ", K(ret));
      } else {
        if (OB_FAIL(tenant_pool_.push(info))) {
          COMMON_LOG(WARN, "Fail to push collect to pool, ", K(ret));
        } else {
          COMMON_LOG(INFO, "del_tenant succeed", K(tenant_id));
        }
      }
    }
  }
  return ret;
}

bool ObTenantManager::has_tenant(const uint64_t tenant_id) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_INVALID_ID != tenant_id) {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_SUCC(bucket.get_the_node(tenant_id, node))) {
      if (NULL != node && true == node->is_loaded_) {
        bool_ret = true;
      }
    }
  }
  return bool_ret;
}

int ObTenantManager::set_tenant_freezing(const uint64_t tenant_id, bool& success)
{
  int ret = OB_SUCCESS;
  ObTenantMemstoreAllocator* tenant_allocator = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (OB_FAIL(allocator_mgr_->get_tenant_memstore_allocator(tenant_id, tenant_allocator))) {
      COMMON_LOG(WARN, "failed to get_tenant_memstore_allocator", K(tenant_id), K(ret));
    } else if (NULL == tenant_allocator) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "got tenant memstore allocator is NULL", K(tenant_id), K(ret));
    } else {
      // Do not judge is_loaded_, and do minor freeze even when the tenant information is not refreshed when the machine
      // is restarted
      success = ATOMIC_BCAS(&node->is_freezing_, false, true);
      if (success) {
        node->last_freeze_clock_ = tenant_allocator->get_retire_clock();
        ATOMIC_AAF(&node->freeze_cnt_, 1);
      }
    }
  }
  return ret;
}

int ObTenantManager::unset_tenant_freezing(uint64_t tenant_id, const bool rollback_freeze_cnt)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else {
      // Do not judge is_loaded_, and do minor freeze even when the tenant information is not refreshed when the machine
      // is restarted
      if (rollback_freeze_cnt) {
        if (ATOMIC_AAF(&node->freeze_cnt_, -1) < 0) {
          node->freeze_cnt_ = 0;
        }
      }
      ATOMIC_SET(&node->is_freezing_, false);
    }
  }
  return ret;
}

int ObTenantManager::set_tenant_freeze_clock(const uint64_t tenant_id, const int64_t freeze_clock)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else {
      node->last_freeze_clock_ = freeze_clock;
      COMMON_LOG(INFO, "set tenant freeze clock", K(freeze_clock), K(node->last_freeze_clock_));
    }
  }
  return ret;
}

int ObTenantManager::set_tenant_mem_limit(
    const uint64_t tenant_id, const int64_t lower_limit, const int64_t upper_limit)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(lower_limit < 0) ||
             OB_UNLIKELY(upper_limit < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(lower_limit), K(upper_limit));
  } else {
    if ((NULL != config_) && (((int64_t)(config_->memstore_limit_percentage)) > 100 ||
                                 ((int64_t)(config_->memstore_limit_percentage)) <= 0 ||
                                 ((int64_t)(config_->freeze_trigger_percentage)) > 100 ||
                                 ((int64_t)(config_->freeze_trigger_percentage)) <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN,
          "memstore limit percent in ObServerConfig is invaild",
          "memstore limit percent",
          (int64_t)config_->memstore_limit_percentage,
          "minor freeze trigger percent",
          (int64_t)config_->freeze_trigger_percentage,
          K(ret));
    } else {
      uint64_t pos = tenant_id % BUCKET_NUM;
      ObTenantBucket& bucket = tenant_map_[pos];
      SpinWLockGuard guard(bucket.lock_);  // It should be possible to change to a read lock here, this lock is a
                                           // structural lock, it is not appropriate to borrow
      ObTenantInfo* node = NULL;
      int64_t mem_minor_freeze_trigger_limit = 0;
      int64_t max_mem_memstore_can_get_now = 0;
      int64_t kv_cache_mem = 0;
      if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
        COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
      } else if (NULL == node) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
      } else {
        node->mem_lower_limit_ = lower_limit;
        node->mem_upper_limit_ = upper_limit;
        if (NULL != config_) {
          int64_t tmp_var = upper_limit / 100;
          node->mem_memstore_limit_ = tmp_var * config_->memstore_limit_percentage;
          if (OB_FAIL(get_tenant_minor_freeze_trigger(tenant_id,
                  node->mem_memstore_limit_,
                  max_mem_memstore_can_get_now,
                  kv_cache_mem,
                  mem_minor_freeze_trigger_limit))) {
            COMMON_LOG(WARN, "fail to get minor freeze trigger", K(ret), K(tenant_id));
          }
        }
        node->is_loaded_ = true;
      }

      if (OB_SUCC(ret)) {
        COMMON_LOG(INFO,
            "set tenant mem limit",
            "tenant id",
            tenant_id,
            "mem_lower_limit",
            lower_limit,
            "mem_upper_limit",
            upper_limit,
            "mem_memstore_limit",
            node->mem_memstore_limit_,
            "mem_minor_freeze_trigger_limit",
            mem_minor_freeze_trigger_limit,
            "mem_tenant_limit",
            get_tenant_memory_limit(node->tenant_id_),
            "mem_tenant_hold",
            get_tenant_memory_hold(node->tenant_id_),
            "mem_memstore_used",
            get_tenant_memory_hold(node->tenant_id_, ObCtxIds::MEMSTORE_CTX_ID),
            "kv_cache_mem",
            kv_cache_mem,
            "max_mem_memstore_can_get_now",
            max_mem_memstore_can_get_now);
      }
    }
  }
  return ret;
}

int ObTenantManager::get_tenant_mem_limit(const uint64_t tenant_id, int64_t& lower_limit, int64_t& upper_limit) const
{
  int ret = OB_SUCCESS;
  lower_limit = 0;
  upper_limit = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
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

int ObTenantManager::get_tenant_memstore_cond(const uint64_t tenant_id, int64_t& active_memstore_used,
    int64_t& total_memstore_used, int64_t& minor_freeze_trigger, int64_t& memstore_limit, int64_t& freeze_cnt)
{
  int ret = OB_SUCCESS;
  int64_t unused = 0;

  active_memstore_used = 0;
  total_memstore_used = 0;
  minor_freeze_trigger = 0;
  memstore_limit = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (OB_FAIL(get_tenant_mem_usage(tenant_id, active_memstore_used, unused, total_memstore_used))) {
      COMMON_LOG(WARN, "failed to get tenant mem usage", K(ret), K(tenant_id));
    } else {
      if (OB_FAIL(get_tenant_minor_freeze_trigger(tenant_id, node->mem_memstore_limit_, minor_freeze_trigger))) {
        COMMON_LOG(WARN, "fail to get minor freeze trigger", K(ret), K(tenant_id));
      }
      memstore_limit = node->mem_memstore_limit_;
      freeze_cnt = node->freeze_cnt_;
    }
  }
  return ret;
}

int ObTenantManager::get_tenant_minor_freeze_trigger(
    const uint64_t tenant_id, const int64_t mem_memstore_limit, int64_t& minor_freeze_trigger)
{
  int64_t not_used = 0;
  int64_t not_used2 = 0;

  return get_tenant_minor_freeze_trigger(tenant_id, mem_memstore_limit, not_used, not_used2, minor_freeze_trigger);
}

static inline bool is_add_overflow(int64_t first, int64_t second, int64_t& res)
{
  if (first + second < 0) {
    return true;
  } else {
    res = first + second;
    return false;
  }
}

int ObTenantManager::get_tenant_minor_freeze_trigger(const uint64_t tenant_id, const int64_t mem_memstore_limit,
    /* Now the maximum memory size that the memstore module can preempt and obtain */
    int64_t& max_mem_memstore_can_get_now, int64_t& kv_cache_mem, int64_t& minor_freeze_trigger)
{
  int ret = OB_SUCCESS;
  ObTenantResourceMgrHandle resource_handle;
  if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id, resource_handle))) {
    COMMON_LOG(WARN, "fail to get resource mgr", K(ret), K(tenant_id));
    ret = OB_SUCCESS;
    minor_freeze_trigger = config_->freeze_trigger_percentage / 100 * mem_memstore_limit;
  } else if (OB_UNLIKELY(NULL == config_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "config_ is nullptr", K(ret), K(tenant_id));
  } else {
    int64_t tenant_mem_limit = get_tenant_memory_limit(tenant_id);
    int64_t tenant_mem_hold = get_tenant_memory_hold(tenant_id);
    int64_t tenant_memstore_hold = get_tenant_memory_hold(tenant_id, ObCtxIds::MEMSTORE_CTX_ID);
    kv_cache_mem = resource_handle.get_memory_mgr()->get_cache_hold();

    bool is_overflow = false;
    if (tenant_mem_limit < tenant_mem_hold) {
      is_overflow = true;
      COMMON_LOG(WARN,
          "tenant_mem_limit is smaller than tenant_mem_hold",
          K(tenant_mem_limit),
          K(tenant_mem_hold),
          K(tenant_id));
    } else if (is_add_overflow(
                   tenant_mem_limit - tenant_mem_hold, tenant_memstore_hold, max_mem_memstore_can_get_now)) {
      is_overflow = true;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        COMMON_LOG(WARN,
            "max memstore can get is overflow",
            K(tenant_mem_limit),
            K(tenant_mem_hold),
            K(tenant_memstore_hold),
            K(tenant_id));
      }
    } else if (OB_SERVER_TENANT_ID != tenant_id &&
               is_add_overflow(max_mem_memstore_can_get_now, kv_cache_mem, max_mem_memstore_can_get_now)) {
      is_overflow = true;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        COMMON_LOG(WARN,
            "max memstore can get is overflow",
            K(tenant_mem_limit),
            K(tenant_mem_hold),
            K(tenant_memstore_hold),
            K(kv_cache_mem),
            K(tenant_id));
      }
    }

    int64_t min = mem_memstore_limit;
    if (!is_overflow) {
      min = MIN(mem_memstore_limit, max_mem_memstore_can_get_now);
    }

    if (min < 100) {
      minor_freeze_trigger = config_->freeze_trigger_percentage * min / 100;
    } else {
      minor_freeze_trigger = min / 100 * config_->freeze_trigger_percentage;
    }
  }

  return ret;
}

int ObTenantManager::get_tenant_mem_usage(
    const uint64_t tenant_id, int64_t& active_memstore_used, int64_t& total_memstore_used, int64_t& total_memstore_hold)
{
  int ret = OB_SUCCESS;
  ObTenantMemstoreAllocator* tenant_allocator = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(allocator_mgr_->get_tenant_memstore_allocator(tenant_id, tenant_allocator))) {
    COMMON_LOG(WARN, "failed to get_tenant_memstore_allocator", K(ret), K(tenant_id));
  } else if (NULL == tenant_allocator) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "tenant memstore allocator is NULL", K(ret), K(tenant_id));
  } else {
    active_memstore_used = tenant_allocator->get_mem_active_memstore_used();
    total_memstore_used = tenant_allocator->get_mem_total_memstore_used();
    total_memstore_hold = get_tenant_memory_hold(tenant_id, ObCtxIds::MEMSTORE_CTX_ID);
  }

  return ret;
}

int ObTenantManager::get_tenant_memstore_limit(const uint64_t tenant_id, int64_t& mem_limit)
{
  int ret = OB_SUCCESS;
  mem_limit = INT64_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;

    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      ret = OB_SUCCESS;
      mem_limit = INT64_MAX;
      COMMON_LOG(WARN, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      mem_limit = INT64_MAX;
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else {
      mem_limit = node->mem_memstore_limit_;
    }
  }
  return ret;
}

int ObTenantManager::check_tenant_out_of_memstore_limit(const uint64_t tenant_id, bool& is_out_of_mem)
{
  int ret = OB_SUCCESS;
  const int64_t check_memstore_limit_interval = 1 * 1000 * 1000;
  static __thread uint64_t last_tenant_id = OB_INVALID_TENANT_ID;
  static __thread int64_t last_check_timestamp = 0;
  static __thread bool last_result = false;
  int64_t current_time = ObTimeUtility::fast_current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (!last_result && tenant_id == last_tenant_id &&
             current_time - last_check_timestamp < check_memstore_limit_interval) {
    // Check once when the last memory burst or tenant_id does not match or the interval reaches the threshold
    is_out_of_mem = false;
  } else if (config_->enable_global_freeze_trigger && tenant_id >= OB_USER_TENANT_ID &&
             allocator_mgr_->get_all_tenants_memstore_used() >= ATOMIC_LOAD(&all_tenants_memstore_limit_)) {
    is_out_of_mem = true;
  } else {
    int64_t mem_active_memstore_used = 0;
    int64_t mem_total_memstore_used = 0;
    int64_t mem_total_memstore_hold = 0;
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;

    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      ret = OB_SUCCESS;
      is_out_of_mem = false;
      COMMON_LOG(WARN, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      is_out_of_mem = false;
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (OB_FAIL(get_tenant_mem_usage(
                   node->tenant_id_, mem_active_memstore_used, mem_total_memstore_used, mem_total_memstore_hold))) {
      COMMON_LOG(WARN, "fail to get mem usage", K(ret), K(node->tenant_id_));
    } else {
      is_out_of_mem = (mem_total_memstore_hold > node->mem_memstore_limit_);
    }
    last_check_timestamp = current_time;
  }

  if (OB_SUCC(ret)) {
    last_result = is_out_of_mem;
    last_tenant_id = tenant_id;
  }
  return ret;
}

bool ObTenantManager::is_rp_pending_log_too_large(const uint64_t tenant_id, const int64_t pending_replay_mutator_size)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else {
      // The limit of the tenant may not be loaded during the restart process, and the global limit is used.
      int64_t mem_rp_pending_log_limit = 0;
      int64_t all_tenants_memstore_used = allocator_mgr_->get_all_tenants_memstore_used();
      if (node->mem_upper_limit_ > 0) {
        mem_rp_pending_log_limit = node->mem_memstore_left();
      } else {
        mem_rp_pending_log_limit = max(0, all_tenants_memstore_limit_ - all_tenants_memstore_used);
      }
      mem_rp_pending_log_limit >>= 3;  // Estimate the size of memstore based on 8 times expansion
      bool_ret = (pending_replay_mutator_size >= mem_rp_pending_log_limit);

      if (bool_ret && REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        COMMON_LOG(WARN,
            "replay engine pending log too large",
            K(tenant_id),
            K(mem_rp_pending_log_limit),
            K(pending_replay_mutator_size),
            "mem_upper_limit",
            node->mem_upper_limit_,
            K(all_tenants_memstore_limit_),
            K(all_tenants_memstore_used));
      }
    }
  }
  return bool_ret;
}

int ObTenantManager::add_tenant_disk_used(const uint64_t tenant_id, const int64_t size, const int16_t attr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    {
      uint64_t pos = tenant_id % BUCKET_NUM;
      ObTenantBucket& bucket = tenant_map_[pos];
      SpinRLockGuard guard(bucket.lock_);
      ObTenantInfo* node = NULL;
      if (OB_SUCCESS != (tmp_ret = bucket.get_the_node(tenant_id, node))) {
        COMMON_LOG(WARN, "fail to get the node", K(tenant_id), K(tmp_ret));
      } else if (NULL == node) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
      } else {
        if (attr >= OB_MAX_MACRO_BLOCK_TYPE || attr < 0) {
          ret = OB_INVALID_MACRO_BLOCK_TYPE;
          COMMON_LOG(WARN, "invalid type of macro block", K(tenant_id), K(ret));
        } else {
          (void)ATOMIC_AAF(&node->disk_used_[attr], size);
        }
      }
    }
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      AddDiskUsed adder(attr);
      adder.size_ = size;
      if (OB_FAIL(add_tenant_and_used(tenant_id, adder))) {
        COMMON_LOG(WARN, "fail to add disk used", K(tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int ObTenantManager::subtract_tenant_disk_used(const uint64_t tenant_id, const int64_t size, const int16_t attr)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else {
      if (attr >= OB_MAX_MACRO_BLOCK_TYPE || attr < 0) {
        ret = OB_INVALID_MACRO_BLOCK_TYPE;
        COMMON_LOG(WARN, "invalid type of macro block", K(tenant_id), K(ret));
      } else {
        (void)ATOMIC_SAF(&node->disk_used_[attr], size);
      }
    }
  }
  return ret;
}

int ObTenantManager::get_tenant_disk_used(const uint64_t tenant_id, int64_t& disk_used, int16_t attr)
{
  int ret = OB_SUCCESS;
  disk_used = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else {
      if (attr >= OB_MAX_MACRO_BLOCK_TYPE || attr < 0) {
        ret = OB_INVALID_MACRO_BLOCK_TYPE;
        COMMON_LOG(WARN, "invalid type of macro block", K(tenant_id), K(ret));
      } else {
        disk_used = node->disk_used_[attr];
      }
    }
  }
  return ret;
}

int ObTenantManager::get_tenant_disk_total_used(const uint64_t tenant_id, int64_t& disk_used)
{
  int ret = OB_SUCCESS;
  disk_used = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else {
      const int64_t max_count = OB_MAX_MACRO_BLOCK_TYPE;
      for (int64_t i = 0; i < max_count; i++) {
        disk_used += node->disk_used_[i];
      }
    }
  }
  return ret;
}

bool ObTenantManager::is_major_freeze_turn(int64_t freeze_cnt)
{
  // Major freeze can only be done after restarting the scan
  // Prevent multiple major freezes during restart.
  const int64_t minor_freeze_times = config_->minor_freeze_times;
  return (freeze_cnt >= minor_freeze_times && NULL != GCTX.par_ser_ && GCTX.par_ser_->is_scan_disk_finished());
}

int ObTenantManager::do_major_freeze_if_previous_failure_exist(bool& triggered)
{
  int ret = OB_SUCCESS;

  if (get_retry_major_info().is_valid()) {
    COMMON_LOG(INFO, "A major freeze is needed due to previous failure");
    if (OB_FAIL(post_freeze_request(
            retry_major_info_.tenant_id_, MAJOR_FREEZE, get_retry_major_info().major_version_.major_))) {
      COMMON_LOG(WARN, "major freeze failed", K(ret));
    }
    triggered = true;
  }

  return ret;
}

int ObTenantManager::do_minor_freeze(const int64_t mem_active_memstore_used, const ObTenantInfo& node, bool& triggered)
{
  int ret = OB_SUCCESS;
  int64_t mem_minor_freeze_trigger = 0;
  int64_t max_mem_memstore_can_get_now = 0;
  int64_t kv_cache_mem = 0;

  if (OB_FAIL(get_tenant_minor_freeze_trigger(node.tenant_id_,
          node.mem_memstore_limit_,
          max_mem_memstore_can_get_now,
          kv_cache_mem,
          mem_minor_freeze_trigger))) {
    COMMON_LOG(WARN, "fail to get minor freeze trigger in a minor freeze", K(ret), K(node.tenant_id_));
  } else {
    COMMON_LOG(INFO,
        "A minor freeze is needed",
        "mem_active_memstore_used_",
        mem_active_memstore_used,
        "mem_minor_freeze_trigger_limit_",
        mem_minor_freeze_trigger,
        "mem_tenant_limit",
        get_tenant_memory_limit(node.tenant_id_),
        "mem_tenant_hold",
        get_tenant_memory_hold(node.tenant_id_),
        "mem_memstore_used",
        get_tenant_memory_hold(node.tenant_id_, ObCtxIds::MEMSTORE_CTX_ID),
        "kv_cache_mem",
        kv_cache_mem,
        "max_mem_memstore_can_get_now",
        max_mem_memstore_can_get_now,
        "tenant_id",
        node.tenant_id_);
  }

  if (OB_FAIL(post_freeze_request(node.tenant_id_, MINOR_FREEZE, node.frozen_version_))) {
    COMMON_LOG(WARN, "minor freeze failed", K(node.tenant_id_), K(ret));
  } else {
    triggered = true;
  }

  return ret;
}

int ObTenantManager::do_major_freeze(const uint64_t tenant_id, const int64_t try_frozen_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(post_freeze_request(tenant_id, MAJOR_FREEZE, try_frozen_version))) {
    COMMON_LOG(WARN, "major freeze failed", K(ret), K(tenant_id));
  }

  return ret;
}

int ObTenantManager::get_global_frozen_version(int64_t& frozen_version)
{
  int ret = OB_SUCCESS;

  common::ObAddr rs_addr;
  Int64 tmp_frozen_version;
  if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    COMMON_LOG(WARN, "get master root service address failed", K(ret));
  } else if (!rs_addr.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid rootserver addr", K(ret), K(rs_addr));
  } else if (OB_FAIL(common_rpc_proxy_->to(rs_addr).get_frozen_version(tmp_frozen_version))) {
    COMMON_LOG(WARN, "get_frozen_version failed", K(rs_addr), K(ret));
  } else {
    frozen_version = tmp_frozen_version;
  }

  return ret;
}

int ObTenantManager::check_and_do_freeze_mixed()
{
  bool upgrade_mode = GCONF.in_major_version_upgrade_mode();
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool triggered = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(upgrade_mode)) {
    // skip trigger freeze while upgrading
  } else {
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = do_major_freeze_if_previous_failure_exist(triggered)))) {
      COMMON_LOG(WARN, "fail to do major freeze due to previous failure", K(tmp_ret));
    }

    bool major_triggered = triggered;
    bool need_major = false;
    uint64_t major_tenant_id = OB_INVALID_TENANT_ID;
    int64_t curr_frozen_version = 0;
    int64_t frozen_version = 0;

    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_frozen_version(frozen_version)))) {
      COMMON_LOG(WARN, "fail to get global frozen version", K(tmp_ret));
    }

    for (int64_t i = 0; i < BUCKET_NUM; ++i) {
      ObTenantBucket& bucket = tenant_map_[i];
      SpinRLockGuard guard(bucket.lock_);
      DLIST_FOREACH_NORET(iter, bucket.info_list_)
      {
        if (iter->is_loaded_) {
          int64_t mem_active_memstore_used = 0;
          int64_t mem_total_memstore_used = 0;
          int64_t mem_total_memstore_hold = 0;
          int64_t mem_minor_freeze_trigger = 0;

          if (OB_FAIL(get_tenant_minor_freeze_trigger(
                  iter->tenant_id_, iter->mem_memstore_limit_, mem_minor_freeze_trigger))) {
            COMMON_LOG(WARN, "fail to get minor freeze trigger", K(ret), K(iter->tenant_id_));
          } else {
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_tenant_mem_usage(iter->tenant_id_,
                                               mem_active_memstore_used,
                                               mem_total_memstore_used,
                                               mem_total_memstore_hold)))) {
              COMMON_LOG(WARN, "fail to get mem usage", K(ret), K(iter->tenant_id_));
            } else if (0 != frozen_version && OB_FAIL(iter->update_frozen_version(frozen_version))) {
              COMMON_LOG(WARN, "fail to update frozen version", K(ret), K(frozen_version), K(*iter));
            } else {
              if (mem_active_memstore_used > mem_minor_freeze_trigger) {
                bool finished = false;
                if (!major_triggered && !need_major && is_major_freeze_turn(iter->freeze_cnt_)) {
                  COMMON_LOG(INFO,
                      "A major freeze is needed",
                      "mem_active_memstore_used_",
                      mem_active_memstore_used,
                      "mem_minor_freeze_trigger_limit_",
                      mem_minor_freeze_trigger,
                      "tenant_id",
                      iter->tenant_id_);

                  major_tenant_id = iter->tenant_id_;
                  curr_frozen_version = iter->frozen_version_;
                  need_major = true;
                }

                if (OB_UNLIKELY(
                        OB_SUCCESS != (tmp_ret = do_minor_freeze(mem_active_memstore_used, *iter, triggered)))) {
                  COMMON_LOG(WARN, "fail to do minor freeze", K(tmp_ret), K(iter->tenant_id_));
                }
              }

              if (mem_total_memstore_hold > mem_minor_freeze_trigger) {
                // There is an unreleased memstable
                COMMON_LOG(INFO,
                    "tenant have inactive memstores",
                    K(mem_active_memstore_used),
                    K(mem_total_memstore_used),
                    K(mem_total_memstore_hold),
                    "mem_minor_freeze_trigger_limit_",
                    mem_minor_freeze_trigger,
                    "tenant_id",
                    iter->tenant_id_);
                ObTenantMemstoreAllocator* tenant_allocator = NULL;
                int tmp_ret = OB_SUCCESS;
                if (OB_SUCCESS !=
                    (tmp_ret = allocator_mgr_->get_tenant_memstore_allocator(iter->tenant_id_, tenant_allocator))) {
                } else {
                  char frozen_mt_info[DEFAULT_BUF_LENGTH];
                  tenant_allocator->log_frozen_memstore_info(frozen_mt_info, sizeof(frozen_mt_info));
                  COMMON_LOG(INFO, "oldest frozen memtable", "list", frozen_mt_info);
                }
              }

              // When the memory is tight, try to abort the warm-up to release memstore
              int64_t mem_danger_limit =
                  iter->mem_memstore_limit_ - ((iter->mem_memstore_limit_ - mem_minor_freeze_trigger) >> 2);
              if (mem_total_memstore_hold > mem_danger_limit) {
                int64_t curr_ts = ObTimeUtility::current_time();
                if (curr_ts - iter->last_halt_ts_ > 10L * 1000L * 1000L) {
                  if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = svr_rpc_proxy_->to(self_).halt_all_prewarming_async(
                                                     iter->tenant_id_, NULL)))) {
                    COMMON_LOG(WARN, "fail to halt prewarming", K(tmp_ret), K(iter->tenant_id_));
                  } else {
                    iter->last_halt_ts_ = curr_ts;
                  }
                }
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_major && OB_INVALID_TENANT_ID != major_tenant_id) {
        triggered = true;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = do_major_freeze(major_tenant_id, curr_frozen_version + 1)))) {
          COMMON_LOG(WARN, "fail to do major freeze", K(tmp_ret));
        }
      }

      if (!triggered) {
        if (OB_SUCCESS != (tmp_ret = check_and_do_freeze_by_total_limit())) {
          COMMON_LOG(WARN, "check_and_do_freeze_by_total_limit failed", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

// This check is triggered only if the mixed check does not trigger freeze, including two cases:
// 1. The active of all tenants did not exceed their limit, but the total exceeded.
// 2. During the downtime and restart, the limit of the tenant has not been loaded yet, and the total active exceeds the
// total limit.
int ObTenantManager::check_and_do_freeze_by_total_limit()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t freeze_cnt = 0;
  int64_t frozen_version = 0;
  int64_t total_active_memstore_used = 0;
  int64_t max_active_memstore_used = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (NULL == GCTX.par_ser_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "GCTX.par_ser_ is NULL", K(ret));
  } else if (!config_->enable_global_freeze_trigger) {
    // The entire process is controlled by enable_global_freeze_trigger
    // If you can't start up during the downtime and restart, you need to turn on the configuration item for temporary
    // processing
  } else {
    for (int64_t i = 0; i < BUCKET_NUM; ++i) {
      ObTenantBucket& bucket = tenant_map_[i];
      SpinRLockGuard guard(bucket.lock_);
      DLIST_FOREACH_NORET(iter, bucket.info_list_)
      {
        int64_t mem_active_memstore_used = 0;
        int64_t mem_total_memstore_used = 0;
        int64_t mem_total_memstore_hold = 0;

        if (OB_UNLIKELY(
                OB_SUCCESS !=
                (tmp_ret = get_tenant_mem_usage(
                     iter->tenant_id_, mem_active_memstore_used, mem_total_memstore_used, mem_total_memstore_hold)))) {
          COMMON_LOG(WARN, "fail to get mem usage", K(tmp_ret), K(iter->tenant_id_));
        } else {
          total_active_memstore_used += mem_active_memstore_used;
          if (mem_active_memstore_used > max_active_memstore_used) {
            // find the max tenant to do minor freeze
            max_active_memstore_used = mem_active_memstore_used;
            tenant_id = iter->tenant_id_;
            frozen_version = iter->frozen_version_;
            freeze_cnt = iter->freeze_cnt_;
          }
        }
      }
    }

    if (total_active_memstore_used >= all_tenants_freeze_trigger_) {
      // After the overall trigger is exceeded, a dump will be triggered if it is during a downtime restart
      // If the downtime restart has been completed and the configuration is open to dump, the dump will be triggered,
      // otherwise it will trigger major freeze
      storage::ObFreezeType freeze_type = is_major_freeze_turn(freeze_cnt) ? MAJOR_FREEZE : MINOR_FREEZE;

      if (MINOR_FREEZE == freeze_type) {
        if (OB_INVALID_TENANT_ID != tenant_id) {
          COMMON_LOG(INFO,
              "A minor freeze is needed by total limit",
              K(total_active_memstore_used),
              "server_memory_avail",
              config_->get_server_memory_avail(),
              K_(all_tenants_freeze_trigger),
              K(tenant_id));

          if (OB_FAIL(post_freeze_request(tenant_id, MINOR_FREEZE, frozen_version))) {
            COMMON_LOG(WARN,
                "trigger minor freeze failed",
                K(tenant_id),
                K(total_active_memstore_used),
                K_(all_tenants_freeze_trigger),
                K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN,
              "no tenant to do minor freeze",
              K(total_active_memstore_used),
              K_(all_tenants_freeze_trigger),
              K(ret));
        }
      } else {
        COMMON_LOG(INFO,
            "A major freeze is needed by total limit",
            K(total_active_memstore_used),
            "server_memory_avail",
            config_->get_server_memory_avail(),
            K_(all_tenants_freeze_trigger));

        if (OB_FAIL(post_freeze_request(OB_INVALID_TENANT_ID, MAJOR_FREEZE, frozen_version + 1))) {
          COMMON_LOG(WARN, "trigger global major freeze failed", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObTenantManager::tenant_need_major_freeze(uint64_t tenant_id)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;

  int64_t mem_active_memstore_used = 0;
  int64_t mem_total_memstore_used = 0;
  int64_t mem_total_memstore_hold = 0;
  int64_t minor_freeze_trigger_limit = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id && ((int64_t)tenant_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id) {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinRLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_FAIL(bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(WARN, "This tenant not exist", K(tenant_id), K(ret));
    } else if (NULL == node) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Unexpected error", K(tenant_id), K(ret));
    } else if (false == node->is_loaded_) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(INFO, "This tenant not exist", K(tenant_id), K(ret));
    } else if (OB_FAIL(get_tenant_mem_usage(
                   node->tenant_id_, mem_active_memstore_used, mem_total_memstore_used, mem_total_memstore_hold))) {
      COMMON_LOG(WARN, "fail to get mem usage", K(ret), K(node->tenant_id_));
    } else {
      if (OB_FAIL(get_tenant_minor_freeze_trigger(
              node->tenant_id_, node->mem_memstore_limit_, minor_freeze_trigger_limit))) {
        COMMON_LOG(WARN, "get tenant minor freeze trigger error", K(ret), K(node->tenant_id_));
      } else {
        if (mem_active_memstore_used > minor_freeze_trigger_limit) {
          COMMON_LOG(INFO,
              "A major freeze is needed",
              "mem_active_memstore_used_",
              mem_active_memstore_used,
              "mem_minor_freeze_trigger_limit_",
              minor_freeze_trigger_limit,
              "tenant_id",
              node->tenant_id_);
          bool_ret = true;
        }
      }
    }
  } else {  // OB_INVALID_ID == tenant_id
    int64_t total_active_memstore_used = 0;
    for (int64_t i = 0; i < BUCKET_NUM; ++i) {
      ObTenantBucket& bucket = tenant_map_[i];
      SpinRLockGuard guard(bucket.lock_);
      DLIST_FOREACH_NORET(iter, bucket.info_list_)
      {
        if (OB_FAIL(get_tenant_mem_usage(
                iter->tenant_id_, mem_active_memstore_used, mem_total_memstore_used, mem_total_memstore_hold))) {
          COMMON_LOG(WARN, "fail to get mem usage", K(ret), K(iter->tenant_id_));
        } else {
          total_active_memstore_used += mem_active_memstore_used;
        }
      }
    }
    if (total_active_memstore_used >= all_tenants_freeze_trigger_) {
      COMMON_LOG(INFO, "A major freeze is needed", K(total_active_memstore_used), K_(all_tenants_freeze_trigger));
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObTenantManager::register_timer_task(int tg_id)
{
  int ret = OB_SUCCESS;
  const bool is_repeated = true;
  const int64_t trigger_interval = 2 * 1000000;  // 2s
  const int64_t print_delay = 10 * 1000000;      // 10s
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_SUCCESS != (ret = TG_SCHEDULE(tg_id, freeze_task_, trigger_interval, is_repeated))) {
    COMMON_LOG(WARN, "fail to schedule major freeze task of tenant manager", K(ret));
  } else if (OB_SUCCESS != (ret = TG_SCHEDULE(tg_id, print_task_, print_delay, is_repeated))) {
    COMMON_LOG(WARN, "fail to schedule print task of tenant manager", K(ret));
  }
  return ret;
}

int ObTenantManager::post_freeze_request(
    const uint64_t tenant_id, const storage::ObFreezeType freeze_type, const int64_t try_frozen_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else {
    ObTenantFreezeArg arg;
    arg.tenant_id_ = tenant_id;
    arg.freeze_type_ = freeze_type;
    arg.try_frozen_version_ = try_frozen_version;
    COMMON_LOG(INFO, "post major freeze in tenant manager", K(arg));
    if (OB_FAIL(rpc_proxy_.to(self_).post_freeze_request(arg, &tenant_mgr_cb_))) {
      COMMON_LOG(WARN, "fail to post freeze request", K(arg), K(ret));
    }
    COMMON_LOG(INFO, "after major freeze in tenant manager");
  }
  return ret;
}

int ObTenantManager::rpc_callback()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else {
    COMMON_LOG(INFO, "call back of tenant mgr major freeze request");
  }
  return ret;
}

void ObTenantManager::reload_config()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (NULL == config_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "config_ shouldn't be null here", K(ret), KP(config_));
  } else if (((int64_t)(config_->memstore_limit_percentage)) > 100 ||
             ((int64_t)(config_->memstore_limit_percentage)) <= 0 ||
             ((int64_t)(config_->freeze_trigger_percentage)) > 100 ||
             ((int64_t)(config_->freeze_trigger_percentage)) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN,
        "memstore limit percent in ObServerConfig is invalid",
        "memstore limit percent",
        (int64_t)config_->memstore_limit_percentage,
        "minor freeze trigger percent",
        (int64_t)config_->freeze_trigger_percentage,
        K(ret));
  } else {
    for (int64_t i = 0; i < BUCKET_NUM; i++) {
      ObTenantBucket& bucket = tenant_map_[i];
      SpinWLockGuard guard(bucket.lock_);  // It should be possible to change to a read lock here, this lock is a
                                           // structural lock, it is not appropriate to borrow
      DLIST_FOREACH_NORET(iter, bucket.info_list_)
      {
        if (true == iter->is_loaded_) {
          int64_t tmp_var = iter->mem_upper_limit_ / 100;
          iter->mem_memstore_limit_ = tmp_var * config_->memstore_limit_percentage;
          tmp_var = iter->mem_memstore_limit_ / 100;
        }
      }
    }
    ATOMIC_STORE(&all_tenants_freeze_trigger_,
        config_->get_server_memory_avail() * config_->get_global_freeze_trigger_percentage() / 100);
    ATOMIC_STORE(&all_tenants_memstore_limit_,
        config_->get_server_memory_avail() * config_->get_global_memstore_limit_percentage() / 100);
  }
  if (OB_SUCCESS == ret) {
    COMMON_LOG(INFO,
        "reload config for tenant manager",
        "new memstore limit percent",
        (int64_t)config_->memstore_limit_percentage,
        "new minor freeze trigger percent",
        (int64_t)config_->freeze_trigger_percentage,
        "new get_global_memstore_limit_percentage()",
        (int64_t)config_->get_global_memstore_limit_percentage(),
        "new get_global_freeze_trigger_percentage()",
        (int64_t)config_->get_global_freeze_trigger_percentage());
  }
}

template <class _callback>
int ObTenantManager::add_tenant_and_used(const uint64_t tenant_id, _callback& callback)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "tenant manager not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    uint64_t pos = tenant_id % BUCKET_NUM;
    ObTenantBucket& bucket = tenant_map_[pos];
    SpinWLockGuard guard(bucket.lock_);
    ObTenantInfo* node = NULL;
    if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, node))) {
      COMMON_LOG(INFO, "This tenant has exist", K(tenant_id), K(ret));
      callback(node);
    } else {
      ObTenantInfo* info = NULL;
      if (OB_FAIL(tenant_pool_.pop(info))) {
        COMMON_LOG(WARN, "Fail to pop info from pool, ", K(ret));
      } else {
        info->reset();
        info->tenant_id_ = tenant_id;
        callback(info);
        if (OB_UNLIKELY(!bucket.info_list_.add_last(info))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
          if (OB_SUCCESS != (tmp_ret = tenant_pool_.push(info))) {
            COMMON_LOG(WARN, "Fail to push collects to pool, ", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int64_t ObTenantCpuShare::calc_px_pool_share(uint64_t tenant_id, int64_t cpu_count)
{
  UNUSED(tenant_id);
  /* Follow cpu_count * concurrency * 0.1 as the default value
   * But make sure to allocate at least 3 threads to the px pool,
   * When the calculated default value is less than 3, it is forced to be set to 3
   *
   * Why should it be at least 3? This is to make mysqltest as much as possible
   * Can pass. When encountering a general right deep tree in mysqltest, 3 threads
   * To ensure that the scheduling is successful, 2 will time out.
   */
  return std::max(
      3L, cpu_count * static_cast<int64_t>(static_cast<double>(GCONF.px_workers_per_cpu_quota.get()) * 0.1));
}

}  // namespace common
}  // namespace oceanbase
