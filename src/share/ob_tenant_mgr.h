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

#ifndef OCEANBASE_COMMON_OB_TENANT_MGR_
#define OCEANBASE_COMMON_OB_TENANT_MGR_

#include <fstream>
#include "share/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/queue/ob_fixed_queue.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "common/storage/ob_freeze_define.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/ob_i_tenant_mgr.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

int64_t get_virtual_memory_used();

namespace oceanbase {
namespace common {
class ObMemstoreAllocatorMgr;
}

namespace storage {
class ObPartitionService;
};

namespace obrpc {
class ObSrvRpcProxy;
struct ObTenantFreezeArg {
  uint64_t tenant_id_;
  storage::ObFreezeType freeze_type_;
  int64_t try_frozen_version_;

  DECLARE_TO_STRING;
  OB_UNIS_VERSION(1);
};

class ObTenantMgrRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObTenantMgrRpcProxy);
  RPC_AP(@PR5 post_freeze_request, OB_TENANT_MGR, (ObTenantFreezeArg));
};

class ObTenantMgrP : public ObRpcProcessor<obrpc::ObTenantMgrRpcProxy::ObRpc<OB_TENANT_MGR> > {
public:
  ObTenantMgrP(
      obrpc::ObCommonRpcProxy* rpc_proxy, const share::ObRsMgr* rs_mgr, storage::ObPartitionService* partition_service)
      : rpc_proxy_(rpc_proxy), rs_mgr_(rs_mgr), partition_service_(partition_service)
  {}
  virtual ~ObTenantMgrP()
  {
    rpc_proxy_ = NULL;
    rs_mgr_ = NULL;
    partition_service_ = NULL;
  }

  const static int64_t MAX_CONCURRENT_MINOR_FREEZING = 10;

protected:
  int process();

private:
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  const share::ObRsMgr* rs_mgr_;
  storage::ObPartitionService* partition_service_;
  // used to control the max concurrent number of minor freezing
  static int64_t minor_freeze_token_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantMgrP);
};

class ObTenantMgrRpcCb : public ObTenantMgrRpcProxy::AsyncCB<OB_TENANT_MGR> {
public:
  ObTenantMgrRpcCb()
  {}
  virtual ~ObTenantMgrRpcCb()
  {}

public:
  int process();
  void on_timeout();
  rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB* newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObTenantMgrRpcCb();
    }
    return newcb;
  }
  void set_args(const ObTenantFreezeArg& arg)
  {
    UNUSED(arg);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantMgrRpcCb);
};

}  // end namespace obrpc

namespace common {

class ObTenantMgrTimerTask : public ObTimerTask {
public:
  ObTenantMgrTimerTask()
  {}
  virtual ~ObTenantMgrTimerTask()
  {}

public:
  virtual void runTimerTask();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantMgrTimerTask);
};

class ObPrintTenantMemstoreUsage : public ObTimerTask {
public:
  ObPrintTenantMemstoreUsage()
  {}
  virtual ~ObPrintTenantMemstoreUsage()
  {}

public:
  virtual void runTimerTask();

private:
  DISALLOW_COPY_AND_ASSIGN(ObPrintTenantMemstoreUsage);
};

struct ObRetryMajorInfo {
  uint64_t tenant_id_;
  common::ObVersion major_version_;

  ObRetryMajorInfo() : tenant_id_(UINT64_MAX), major_version_()
  {}
  bool is_valid() const
  {
    return UINT64_MAX != tenant_id_;
  }
  void reset()
  {
    tenant_id_ = UINT64_MAX;
    major_version_.reset();
  }

  TO_STRING_KV(K_(tenant_id), K_(major_version));
};
class ObTenantInfo : public ObDLinkBase<ObTenantInfo> {
public:
  ObTenantInfo();
  virtual ~ObTenantInfo()
  {}

public:
  uint64_t tenant_id_;
  int64_t mem_lower_limit_;
  int64_t mem_upper_limit_;
  // mem_memstore_limit will be checked when **leader** partitions
  // perform writing operation (select for update is included)
  int64_t mem_memstore_limit_;
  uint64_t disk_used_[OB_MAX_MACRO_BLOCK_TYPE];
  bool is_loaded_;
  bool is_freezing_;
  int64_t last_freeze_clock_;
  int64_t frozen_version_;
  int64_t freeze_cnt_;
  int64_t last_halt_ts_;  // Avoid frequent execution of abort preheating

  int update_frozen_version(int64_t frozen_version);
  int64_t mem_memstore_left() const;
  void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantInfo);
};

class ObServerConfig;

class ObTenantManager : public ObITenantMgr {
public:
  // this init is for obproxy only
  int init(const int64_t tenant_cnt = OB_ONLY_SYS_TENANT_COUNT);
  // in observer we only use this init func
  int init(const ObAddr& self, obrpc::ObSrvRpcProxy& rpc_proxy, obrpc::ObCommonRpcProxy& common_rpc_proxy,
      const share::ObRsMgr& rs_mgr, rpc::frame::ObReqTransport* req_transport, ObServerConfig* config,
      const int64_t tenant_cnt = OB_DEFAULT_TENANT_COUNT);
  bool is_inited() const
  {
    return is_inited_;
  }
  void destroy();
  static ObTenantManager& get_instance();
  virtual int get_all_tenant_id(ObIArray<uint64_t>& key) const;
  int add_tenant(const uint64_t tenant_id);
  int del_tenant(const uint64_t tenant_id);
  virtual bool has_tenant(const uint64_t tenant_id) const;
  int set_tenant_freezing(const uint64_t tenant_id, bool& success);
  int unset_tenant_freezing(const uint64_t tenant_id, const bool rollback_freeze_cnt);
  int set_tenant_freeze_clock(const uint64_t tenant_id, const int64_t freeze_clock);
  int set_tenant_mem_limit(const uint64_t tenant_id, const int64_t lower_limit, const int64_t upper_limit);
  virtual int get_tenant_mem_limit(const uint64_t tenant_id, int64_t& lower_limit, int64_t& upper_limit) const;
  int get_tenant_memstore_cond(const uint64_t tenant_id, int64_t& active_memstore_used, int64_t& total_memstore_used,
      int64_t& minor_freeze_trigger, int64_t& memstore_limit, int64_t& freeze_cnt);
  int get_tenant_memstore_limit(const uint64_t tenant_id, int64_t& mem_limit);
  int get_tenant_mem_usage(const uint64_t tenant_id, int64_t& active_memstore_used, int64_t& total_memstore_used,
      int64_t& total_memstore_hold);
  int get_tenant_minor_freeze_trigger(
      const uint64_t tenant_id, const int64_t mem_memstore_limit, int64_t& minor_freeze_trigger);
  int get_tenant_minor_freeze_trigger(const uint64_t tenant_id, const int64_t mem_memstore_limit,
      int64_t& max_mem_memstore_can_get_now, int64_t& kvcache_mem, int64_t& minor_freeze_trigger);
  // this is used to check mem_memstore_limit_
  int check_tenant_out_of_memstore_limit(const uint64_t tenant_id, bool& is_out_of_mem);
  int add_tenant_active_trans_mem_size(const uint64_t tenant_id, const int64_t size);
  bool is_rp_pending_log_too_large(const uint64_t tenant_id, const int64_t pending_replay_mutator_size);
  int add_tenant_disk_used(const uint64_t tenant_id, const int64_t size, const int16_t attr);
  int subtract_tenant_disk_used(const uint64_t tenant_id, const int64_t size, const int16_t attr);
  int get_tenant_disk_used(const uint64_t tenant_id, int64_t& disk_used, const int16_t attr);
  int get_tenant_disk_total_used(const uint64_t tenant_id, int64_t& disk_total_used);
  // this check if a major freeze is needed, and returns the tenant
  // whose memstore is out of range
  bool tenant_need_major_freeze(uint64_t tenant_id);
  // check which tenant is out of range, and do minor free to it
  int check_and_do_freeze_mixed();
  int register_timer_task(int tg_id);
  int post_freeze_request(
      const uint64_t tenant_id, const storage::ObFreezeType freeze_type, const int64_t try_frozen_version);
  int rpc_callback();
  void reload_config();
  int print_tenant_usage();
  const ObRetryMajorInfo& get_retry_major_info() const
  {
    return retry_major_info_;
  }
  void set_retry_major_info(const ObRetryMajorInfo& retry_major_info)
  {
    retry_major_info_ = retry_major_info;
  }

private:
  static const int64_t BUCKET_NUM = 1373;

private:
  ObTenantManager();
  virtual ~ObTenantManager();
  int init_tenant_map(const int64_t tenant_cnt);
  template <class _callback>
  int add_tenant_and_used(const uint64_t tenant_id, _callback& callback);
  bool is_major_freeze_turn(int64_t freeze_cnt);
  int do_major_freeze_if_previous_failure_exist(bool& triggered);
  int do_minor_freeze(const int64_t mem_active_memstore_used, const ObTenantInfo& node, bool& triggered);
  int do_major_freeze(const uint64_t tenant_id, const int64_t try_frozen_version);
  int get_global_frozen_version(int64_t& frozen_version);
  int check_and_do_freeze_by_total_limit();
  int print_tenant_node(
      ObTenantInfo& node, char* print_buf, int64_t buf_len, int64_t& pos, int64_t& total_active_memstore_hold);

  struct ObTenantBucket {
    ObDList<ObTenantInfo> info_list_;
    SpinRWLock lock_;
    ObTenantBucket() : info_list_(), lock_()
    {}
    int get_the_node(const uint64_t tenant_id, ObTenantInfo*& node)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      ObTenantInfo* head = info_list_.get_header();
      node = info_list_.get_first();
      while (head != node && NULL != node) {
        if (tenant_id == node->tenant_id_) {
          ret = OB_SUCCESS;
          break;
        } else {
          node = node->get_next();
        }
      }
      return ret;
    }
  };
  struct AddDiskUsed {
    explicit AddDiskUsed(int16_t attr) : size_(0), attr_(attr)
    {}
    void operator()(ObTenantInfo* info)
    {
      (void)ATOMIC_AAF(&info->disk_used_[attr_], size_);
    }
    int64_t size_;
    int16_t attr_;
  };
  ObTenantBucket* tenant_map_;
  ObFixedQueue<ObTenantInfo> tenant_pool_;
  ObArenaAllocator allocator_;
  ObMemAttr memattr_;
  ObTenantMgrTimerTask freeze_task_;
  ObPrintTenantMemstoreUsage print_task_;
  obrpc::ObTenantMgrRpcProxy rpc_proxy_;
  obrpc::ObTenantMgrRpcCb tenant_mgr_cb_;
  obrpc::ObSrvRpcProxy* svr_rpc_proxy_;
  obrpc::ObCommonRpcProxy* common_rpc_proxy_;
  const share::ObRsMgr* rs_mgr_;
  ObAddr self_;
  // bool is_processing_; There is no concurrent thread processing, temporarily delete this parameter.
  ObServerConfig* config_;
  int64_t all_tenants_freeze_trigger_;
  int64_t all_tenants_memstore_limit_;
  bool is_inited_;
  ObRetryMajorInfo retry_major_info_;
  common::ObMemstoreAllocatorMgr* allocator_mgr_;
  lib::ObMutex print_mutex_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantManager);
};

class ObTenantCpuShare {
public:
  /* Return value: The number of px threads assigned to tenant_id tenant */
  static int64_t calc_px_pool_share(uint64_t tenant_id, int64_t cpu_count);
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_TENANT_MGR_
