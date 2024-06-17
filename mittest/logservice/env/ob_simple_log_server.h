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
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/ob_errno.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/signal/ob_signal_worker.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/function/ob_function.h"           // ObFunction
#include "observer/ob_signal_handle.h"
#include "lib/utility/ob_defer.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/ob_request.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/frame/ob_req_transport.h"
#include "logservice/logrpc/ob_log_rpc_processor.h"
#include "logservice/palf/log_rpc_macros.h"
#include "logservice/palf/log_rpc_processor.h"
#include "logservice/palf/palf_env.h"
#include "logservice/ob_arbitration_service.h"
#include "mock_election.h"
#include "mock_ob_locality_manager.h"
#include "mock_ob_meta_reporter.h"
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
#include "logservice/ob_net_keepalive_adapter.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include <memory>
#include <map>
#include "share/ob_tenant_mem_limit_getter.h"

namespace oceanbase
{
namespace unittest
{
class ObLogDeliver;
}

namespace unittest
{
using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::palf;
using namespace oceanbase::share;

class MockNetKeepAliveAdapter : public logservice::IObNetKeepAliveAdapter
{
public:
  MockNetKeepAliveAdapter() : log_deliver_(NULL) {}
  ~MockNetKeepAliveAdapter() { log_deliver_ = NULL; }
  int init(unittest::ObLogDeliver *log_deliver);
  bool in_black_or_stopped(const common::ObAddr &server) override final;
  bool is_server_stopped(const common::ObAddr &server) override final;
  bool in_black(const common::ObAddr &server) override final;
  int get_last_resp_ts(const common::ObAddr &server, int64_t &last_resp_ts) override final;
private:
  unittest::ObLogDeliver *log_deliver_;
};
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

class ObMittestBlacklist {
public:
  int init(const common::ObAddr &self);
  void block_net(const ObAddr &src);
  void unblock_net(const ObAddr &src);
  void block_pcode(const ObRpcPacketCode &pcode);
  void unblock_pcode(const ObRpcPacketCode &pcode);
  bool need_filter_packet_by_blacklist(const ObAddr &address);
  bool need_filter_packet_by_pcode_blacklist(const ObRpcPacketCode &pcode);
	void set_need_drop_packet(const bool need_drop_packet) { need_drop_packet_ = need_drop_packet; }
  void set_rpc_loss(const ObAddr &src, const int loss_rate);
  void reset_rpc_loss(const ObAddr &src);
  bool need_drop_by_loss_config(const ObAddr &addr);
  void get_loss_config(const ObAddr &src, bool &exist, LossConfig &loss_config);
  TO_STRING_KV(K_(blacklist), K_(rpc_loss_config));
protected:
  hash::ObHashSet<ObAddr> blacklist_;
  hash::ObHashSet<int64_t> pcode_blacklist_;
	bool need_drop_packet_;
  common::ObSEArray<LossConfig, 4> rpc_loss_config_;
  common::ObAddr self_;
};

class ObSimpleLogServer;
class ObLooper : public share::ObThreadPool {
public:
  static constexpr int64_t INTERVAL_US = 1000*1000;
  ObLooper();
  virtual ~ObLooper();
public:
  int init(ObSimpleLogServer *log_server);
  void destroy();
  void run1();
private:
  void log_loop_();
private:
  ObSimpleLogServer *log_server_;
  int64_t run_interval_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLooper);
};

class ObLogDeliver : public rpc::frame::ObReqDeliver, public lib::TGTaskHandler, public ObMittestBlacklist
{
public:
	ObLogDeliver()
      : rpc::frame::ObReqDeliver(),
        palf_env_impl_(NULL),
        tg_id_(0),
        is_stopped_(true) {}
  ~ObLogDeliver() { destroy(true); }
  int init() override final {return OB_SUCCESS;}
  int init(const common::ObAddr &self, const bool is_bootstrap);
  void destroy(const bool is_shutdown);
  int deliver(rpc::ObRequest &req);
  int start();
  void stop();
  int wait();
  void handle(void *task);

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
  int handle_req_(rpc::ObRequest &req);
private:
  mutable common::RWLock lock_;
  bool is_inited_;
	PalfEnvImpl *palf_env_impl_;
  int tg_id_;
  bool is_stopped_;
  int64_t node_id_;
};

class MockElectionAlloc
{
public:
  typedef common::LinkHashNode<palf::LSKey> Node;
  static MockElection *alloc_value() { return NULL; }
  static void free_value(MockElection *mock_election)
  {
    ob_free(mock_election);
    mock_election = NULL;
  }
  static Node *alloc_node(MockElection *val)
  {
    UNUSED(val);
    ObMemAttr attr(1, ObNewModIds::OB_ELECTION);
    Node* node = (Node*)ob_malloc(sizeof(Node), attr);
    new(node) Node();
    return node;
  }
  static void free_node(Node *node)
  {
    node->~Node();
    ob_free(node);
    node = NULL;
  }
};

typedef common::ObLinkHashMap<palf::LSKey, MockElection, MockElectionAlloc> MockElectionMap;
class ObISimpleLogServer
{
public:
  ObISimpleLogServer() {}
  virtual ~ObISimpleLogServer() {}
  virtual bool is_valid() const = 0;
  virtual IPalfEnvImpl *get_palf_env() = 0;
  virtual void revert_palf_env(IPalfEnvImpl *palf_env) = 0;
  virtual const std::string& get_clog_dir() const = 0;
  virtual common::ObAddr get_addr() const = 0;
  virtual ObTenantBase *get_tenant_base() const = 0;
  virtual logservice::ObLogFlashbackService *get_flashback_service() = 0;
	virtual void set_need_drop_packet(const bool need_drop_packet) = 0;
  virtual void block_net(const ObAddr &src) = 0;
  virtual void unblock_net(const ObAddr &src) = 0;
  virtual void block_pcode(const ObRpcPacketCode &pcode) = 0;
  virtual void unblock_pcode(const ObRpcPacketCode &pcode) = 0;
  virtual void set_rpc_loss(const ObAddr &src, const int loss_rate) = 0;
  virtual void reset_rpc_loss(const ObAddr &src) = 0;
  virtual int simple_init(const std::string &cluster_name,
                          const common::ObAddr &addr,
                          const int64_t node_id,
                          LogMemberRegionMap *region_map,
                          const bool is_bootstrap) = 0;
  virtual int simple_start(const bool is_bootstrap) = 0;
  virtual int simple_close(const bool is_shutdown) = 0;
  virtual int simple_restart(const std::string &cluster_name, const int64_t node_idx) = 0;
  virtual ILogBlockPool *get_block_pool() = 0;
  virtual ObILogAllocator *get_allocator() = 0;
  virtual int update_disk_opts(const PalfDiskOptions &opts) = 0;
  virtual int get_disk_opts(PalfDiskOptions &opts) = 0;
  virtual int get_palf_env(PalfEnv *&palf_env) = 0;
  virtual bool is_arb_server() const {return false;};
  virtual int64_t get_node_id() = 0;
  virtual int create_mock_election(const int64_t palf_id, MockElection *&mock_election) = 0;
  virtual int remove_mock_election(const int64_t palf_id) = 0;
  virtual int set_leader(const int64_t palf_id, const common::ObAddr &leader, const int64_t new_epoch = 0) = 0;
  virtual int update_server_log_disk(const int64_t log_disk_size) = 0;
  virtual MockObLocalityManager *get_locality_manager() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObSimpleLogServer : public ObISimpleLogServer
{
public:
  ObSimpleLogServer()
    : handler_(deliver_),
      transport_(NULL),
      batch_rpc_transport_(NULL),
      high_prio_rpc_transport_(NULL)
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
  int simple_init(const std::string &cluster_name,
                  const common::ObAddr &addr,
                  const int64_t node_id,
                  LogMemberRegionMap *region_map,
                  const bool is_bootstrap) override final;
  int simple_start(const bool is_bootstrap) override final;
  int simple_close(const bool is_shutdown) override final;
  int simple_restart(const std::string &cluster_name, const int64_t node_idx) override final;
public:
  int64_t get_node_id() {return node_id_;}
  ILogBlockPool *get_block_pool() override final
  {return &log_block_pool_;};
  ObILogAllocator *get_allocator() override final
  { return allocator_; }
  virtual int update_disk_opts(const PalfDiskOptions &opts) override final;
  virtual int get_disk_opts(PalfDiskOptions &opts) override final
  {
    ObSpinLockGuard guard(log_disk_lock_);
    opts = disk_opts_;
    return OB_SUCCESS;
  }
  virtual int try_resize();
  virtual int get_palf_env(PalfEnv *&palf_env)
  { palf_env = palf_env_; return OB_SUCCESS;}
  virtual void revert_palf_env(IPalfEnvImpl *palf_env) { UNUSED(palf_env); }
  bool is_valid() const override final {return NULL != palf_env_;}
  IPalfEnvImpl *get_palf_env() override final
  { return palf_env_->get_palf_env_impl(); }
  const std::string& get_clog_dir() const override final
  { return clog_dir_; }
  common::ObAddr get_addr() const override final
  { return addr_; }
  ObTenantBase *get_tenant_base() const override final
  { return tenant_base_; }
  logservice::ObLogFlashbackService *get_flashback_service() override final
  { return log_service_.get_flashback_service(); }
	// Nowdat, not support drop packet from specificed address
	void set_need_drop_packet(const bool need_drop_packet) override final
  { deliver_.set_need_drop_packet(need_drop_packet);}
  void block_net(const ObAddr &src) override final
  { deliver_.block_net(src); }
  void unblock_net(const ObAddr &src) override final
  { deliver_.unblock_net(src); }
  void block_pcode(const ObRpcPacketCode &pcode) override final
  { deliver_.block_pcode(pcode); }
  void unblock_pcode(const ObRpcPacketCode &pcode) override final
  { deliver_.unblock_pcode(pcode); }
  void set_rpc_loss(const ObAddr &src, const int loss_rate) override final
  { deliver_.set_rpc_loss(src, loss_rate); }
  void reset_rpc_loss(const ObAddr &src) override final
  { deliver_.reset_rpc_loss(src); }
  int create_mock_election(const int64_t palf_id, MockElection *&mock_election) override final
  {
    int ret = OB_SUCCESS;
    mock_election = NULL;
    void *buf = NULL;
    ObMemAttr attr(1, ObNewModIds::OB_ELECTION);
    if (OB_ISNULL(buf = ob_malloc(sizeof(MockElection), attr))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "ob_malloc failed", K(palf_id));
    } else if (FALSE_IT(mock_election = new (buf) MockElection)) {
    } else if (OB_FAIL(mock_election->init(palf_id, addr_))) {
      SERVER_LOG(WARN, "mock_election->init failed", K(palf_id), K_(addr));
    } else if (OB_FAIL(mock_election_map_.insert_and_get(palf::LSKey(palf_id), mock_election))) {
      SERVER_LOG(WARN, "create_mock_election failed", K(palf_id));
    } else {
      SERVER_LOG(INFO, "create_mock_election success", K(palf_id), K_(addr), KP(mock_election));
    }
    if (OB_FAIL(ret) && NULL != mock_election) {
      ob_free(mock_election);
      mock_election = NULL;
    }
    return ret;
  }
  int remove_mock_election(const int64_t palf_id) override final
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mock_election_map_.del(palf::LSKey(palf_id))) && OB_ENTRY_NOT_EXIST != ret) {
      SERVER_LOG(WARN, "del failed", K(palf_id));
    } else {
      ret = OB_SUCCESS;
      SERVER_LOG(INFO, "remove_mock_election success", K(palf_id), K_(addr));
    }
    return ret;
  }
  int set_leader(const int64_t palf_id, const common::ObAddr &leader, const int64_t new_epoch = 0)
  {
    int ret = OB_SUCCESS;
    MockElection *mock_election= NULL;
    if (OB_FAIL(mock_election_map_.get(palf::LSKey(palf_id), mock_election))) {
      SERVER_LOG(WARN, "get failed", K(palf_id));
    } else if (OB_FAIL(mock_election->set_leader(leader, new_epoch))) {
      SERVER_LOG(WARN, "set_leader failed", K(palf_id), KP(mock_election), K(leader), K(new_epoch));
    }
    if (OB_NOT_NULL(mock_election)) {
      mock_election_map_.revert(mock_election);
    }
    return ret;
  }
  int update_server_log_disk(const int64_t log_disk_size);
  MockObLocalityManager *get_locality_manager() { return &mock_locality_manager_; }
  TO_STRING_KV(K_(node_id), K_(addr), KP(palf_env_));

protected:
  int init_io_(const std::string &cluster_name);
  int init_network_(const common::ObAddr &addr, const bool is_bootstrap);
  int init_log_service_();
  int init_memory_dump_timer_();
  int update_tenant_log_disk_size_(const uint64_t tenant_id,
                                   const int64_t old_log_disk_size,
                                   const int64_t new_log_disk_size,
                                   int64_t &allowed_log_disk_size);
  int update_disk_opts_no_lock_(const PalfDiskOptions &opts);
  int init_log_kv_cache_();


private:
  int64_t node_id_;
  common::ObAddr addr_;
  rpc::frame::ObNetEasy net_;
  obrpc::ObRpcHandler handler_;
  ObLogDeliver deliver_;
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
  logservice::ObLogService log_service_;
  ObTenantMutilAllocator *allocator_;
  rpc::frame::ObReqTransport *transport_;
  rpc::frame::ObReqTransport *batch_rpc_transport_;
  rpc::frame::ObReqTransport *high_prio_rpc_transport_;
  ObLSService ls_service_;
  ObLocationService location_service_;
  MockMetaReporter reporter_;
  logservice::ObServerLogBlockMgr log_block_pool_;
  common::ObMySQLProxy sql_proxy_;
  MockNetKeepAliveAdapter *net_keepalive_;
  ObSrvRpcProxy srv_proxy_;
  logservice::coordinator::ObFailureDetector detector_;
  MockElectionMap mock_election_map_;
  // ObTenantUnit以及__all_unit_configs
  ObSpinLock log_disk_lock_;
  // 本地已生效日志盘规格
  palf::PalfDiskOptions disk_opts_;
  // 内部表中记录日志盘规格
  palf::PalfDiskOptions inner_table_disk_opts_;
  ObLooper looper_;
  MockObLocalityManager mock_locality_manager_;
  obrpc::ObBatchRpc batch_rpc_;
  int batch_rpc_tg_id_;
};

} // end unittest
} // oceanbase
