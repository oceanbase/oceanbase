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

#pragma once
#include "lib/hash/ob_hashset.h"
#include "lib/ob_errno.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/signal/ob_signal_worker.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/function/ob_function.h"           // ObFunction
#include "logservice/palf/log_block_pool_interface.h"
#include "observer/ob_signal_handle.h"
#include "observer/ob_srv_deliver.h"
#include "lib/utility/ob_defer.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/ob_request.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/frame/ob_req_transport.h"
#include "logservice/palf/log_rpc_macros.h"
#include "logservice/palf/log_rpc_processor.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/palf_env.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/ob_arbitration_service.h"
#include "lib/net/ob_addr.h"
#include "share/ob_rpc_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "share/ob_local_device.h"
#include "share/ob_occam_timer.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "mittest/palf_cluster/rpc/palf_cluster_rpc_processor.h"
#include "mittest/palf_cluster/logservice/log_service.h"
#include <map>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::palf;
using namespace oceanbase::share;

uint32_t get_local_addr(const char *dev_name);
std::string get_local_ip();

struct LossConfig
{
  ObAddr src_;
  int loss_rate_;
  LossConfig()
    : src_(), loss_rate_(0)
  {}
  LossConfig(const ObAddr &src, const int loss_rate)
    : src_(src), loss_rate_(loss_rate)
  {}
  TO_STRING_KV(K_(src), K_(loss_rate));
};

class ObLogServerReqQueueHandler : public rpc::frame::ObiReqQHandler
{
public:
  ObLogServerReqQueueHandler()
    : log_service_(NULL) { }
  virtual ~ObLogServerReqQueueHandler();
  int init(palfcluster::LogService *log_service);

  int onThreadCreated(obsys::CThread *);
  int onThreadDestroy(obsys::CThread *);

  bool handlePacketQueue(ObRequest *req, void *args);
private:
  palfcluster::LogService *log_service_;
};

class ObLogDeliver : public rpc::frame::ObReqQDeliver, public lib::TGTaskHandler
{
public:
	ObLogDeliver(rpc::frame::ObiReqQHandler &qhandler)
      : rpc::frame::ObReqQDeliver(qhandler),
        palf_env_impl_(NULL),
        tg_id_(0),
        blacklist_(),
        rpc_loss_config_(),
        is_stopped_(true),
        tenant_base_(NULL),
        req_queue_(NULL) {}
  ~ObLogDeliver() { destroy(); }
  int init();
  int set_tenant_base(ObTenantBase *base, palfcluster::LogService *log_service)
  {
    tenant_base_ = base;
    log_service_ = log_service;
    return OB_SUCCESS;
  }
  void destroy();
  int deliver(rpc::ObRequest &req);
  int start();
  void stop();
  int wait();
  void handle(void *task);
	void set_need_drop_packet(const bool need_drop_packet) { need_drop_packet_ = need_drop_packet; }
  void block_net(const ObAddr &src);
  void unblock_net(const ObAddr &src);
  void set_rpc_loss(const ObAddr &src, const int loss_rate);
  void reset_rpc_loss(const ObAddr &src);
  bool need_filter_packet_by_blacklist(const ObAddr &address);
  bool need_drop_by_loss_config(const ObAddr &addr);
  void get_loss_config(const ObAddr &src, bool &exist, LossConfig &loss_config);

private:
  void init_all_propocessor_();
  typedef std::function<int(ObReqProcessor *&)> Func;

  Func funcs_[MAX_PCODE];
  template <typename PROCESSOR>
  void register_rpc_propocessor_(int pcode)
  {
    auto func = [](ObReqProcessor *&ptr) -> int {
      int ret = OB_SUCCESS;
      if (NULL == (ptr = OB_NEW(PROCESSOR, "SimpleLogSvr"))) {
        SERVER_LOG(WARN, "allocate memory failed");
      } else if (OB_FAIL(ptr->init())) {
      } else {
      }
      return ret;
    };
    funcs_[pcode] = func;
    SERVER_LOG(INFO, "register_rpc_propocessor_ success", K(pcode));
  }

  int create_queue_thread(int tg_id, const char *thread_name, observer::QueueThread *&qthread);
  int handle_req_(rpc::ObRequest &req);
private:
  mutable common::RWLock lock_;
  bool is_inited_;
	PalfEnvImpl *palf_env_impl_;
  palfcluster::LogService *log_service_;
  int tg_id_;
	bool need_drop_packet_;
  hash::ObHashSet<ObAddr> blacklist_;
  common::ObSEArray<LossConfig, 4> rpc_loss_config_;
  bool is_stopped_;
  ObTenantBase *tenant_base_;
  observer::QueueThread *req_queue_;
};

class ObSimpleLogServer
{
public:
  ObSimpleLogServer()
    : req_handler_(),
      deliver_(req_handler_),
      handler_(deliver_),
      transport_(NULL)
  {
  }
  ~ObSimpleLogServer()
  {
    if (OB_NOT_NULL(allocator_)) {
      ob_delete(allocator_);
    }
    if (OB_NOT_NULL(io_device_)) {
      ob_delete(io_device_);
    }
  }
  bool is_valid() {return NULL != palf_env_;}
  int simple_init(const std::string &cluster_name,
                  const common::ObAddr &addr,
                  const int64_t node_id,
                  const bool is_bootstrap);
  int simple_start(const bool is_bootstrap);
  int simple_close(const bool is_shutdown);
  int simple_restart(const std::string &cluster_name, const int64_t node_idx);
  PalfEnv *get_palf_env() { return palf_env_; }
  const std::string& get_clog_dir() const { return clog_dir_; }
  common::ObAddr get_addr() const { return addr_; }
  ObTenantBase *get_tenant_base() const { return tenant_base_; }
  palfcluster::LogService *get_log_service() { return &log_service_; }
  logservice::ObServerLogBlockMgr *get_log_block_pool() { return &log_block_pool_; }
	// Nowdat, not support drop packet from specificed address
	void set_need_drop_packet(const bool need_drop_packet) { deliver_.set_need_drop_packet(need_drop_packet);}
  void block_net(const ObAddr &src) { deliver_.block_net(src); }
  void unblock_net(const ObAddr &src) { deliver_.unblock_net(src); }
  void set_rpc_loss(const ObAddr &src, const int loss_rate) { deliver_.set_rpc_loss(src, loss_rate); }
  void reset_rpc_loss(const ObAddr &src) { deliver_.reset_rpc_loss(src); }
  TO_STRING_KV(K_(node_id), K_(addr), KP(palf_env_));
protected:
  int init_io_(const std::string &cluster_name);
  int init_network_(const common::ObAddr &addr, const bool is_bootstrap);
  int init_log_service_(const std::string &cluster_name);
  int init_memory_dump_timer_();

private:
  int64_t node_id_;
  common::ObAddr addr_;
  rpc::frame::ObNetEasy net_;
  ObLogServerReqQueueHandler req_handler_;
  ObLogDeliver deliver_;
  obrpc::ObRpcHandler handler_;
  ObRandom rand_;
  PalfEnv *palf_env_;
  ObTenantBase *tenant_base_;
  ObMalloc malloc_mgr_;
  ObLocalDevice *io_device_;
  static const int64_t MAX_IOD_OPT_CNT = 5;
  ObIODOpt iod_opt_array_[MAX_IOD_OPT_CNT];
  ObIODOpts iod_opts_;
  std::string clog_dir_;
  ObOccamTimer timer_;
  ObOccamTimerTaskRAIIHandle timer_handle_;
  palfcluster::LogService log_service_;
  ObTenantMutilAllocator *allocator_;
  rpc::frame::ObReqTransport *transport_;
  ObLSService ls_service_;
  ObLocationService location_service_;
  logservice::ObServerLogBlockMgr log_block_pool_;
  common::ObMySQLProxy sql_proxy_;
};

} // end unittest
} // oceanbase