// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_PALF_CLUSTER_LOG_CLIENT_H_
#define OCEANBASE_PALF_CLUSTER_LOG_CLIENT_H_

#include "share/ob_thread_pool.h"
#include "share/ob_occam_timer.h"
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/lock/ob_tc_rwlock.h"              // RWLock
#include "common/ob_member_list.h"              // common::ObMemberList
#include "storage/ob_locality_manager.h"        // ObLocalityManager
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_log_service.h"
#include "mittest/palf_cluster/rpc/palf_cluster_rpc_req.h"              // ProbeMsg
#include "mittest/palf_cluster/rpc/palf_cluster_rpc_proxy.h"              // ProbeMsg

namespace oceanbase
{

namespace palf
{
class PalfEnv;
}

namespace obrpc
{
class PalfClusterRpcProxy;
}

namespace palfcluster
{
class LogService;

class MockAppendCb : public logservice::AppendCb
{
public:
  MockAppendCb()
  : log_size_(0),
    is_called_(true)
  { }

  int on_success() override final;

  int on_failure() override final
  {
    ATOMIC_STORE(&is_called_, true);
    return OB_SUCCESS;
  }

  void reset()
  {
    ATOMIC_STORE(&is_called_, false);
  }

  bool is_called() const
  {
    return ATOMIC_LOAD(&is_called_);
  }

public:
  int64_t log_size_;
  bool is_called_;
};

class MockRemoteAppendCb : public logservice::AppendCb
{
public:
  MockRemoteAppendCb()
  : rpc_proxy_(NULL),
    client_addr_(),
    is_called_(true)
  { }

  ~MockRemoteAppendCb()
  {
    rpc_proxy_ = NULL;
    client_addr_.reset();
  }

  int init(obrpc::PalfClusterRpcProxy *rpc_proxy,
           const common::ObAddr &self)
  {
    int ret = OB_SUCCESS;
    if (rpc_proxy == NULL) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      rpc_proxy_ = rpc_proxy;
      self_ = self;
    }
    return ret;
  }

  int pre_submit(const common::ObAddr &client_addr,
                 const int64_t palf_id,
                 const int64_t client_id)
  {
    int ret = OB_SUCCESS;
    if (is_called()) {
      reset();
      client_addr_ = client_addr;
      palf_id_ = palf_id;
      client_id_ = client_id;
    } else {
      ret = OB_EAGAIN;
    }
    return ret;
  }

  int on_success() override final
  {
    int ret = OB_SUCCESS;
    ATOMIC_STORE(&is_called_, true);
    AppendCb::reset();
    if (OB_NOT_NULL(rpc_proxy_) && client_addr_.is_valid()) {
      // notify called
      const int64_t RPC_TIMEOUT_US = 1 * 1000 * 1000;
      SubmitLogCmdResp resp(self_, palf_id_, client_id_);
      static obrpc::ObLogRpcCB<obrpc::OB_LOG_SUBMIT_LOG_CMD_RESP> cb;                                                                  \
      if (OB_FAIL(rpc_proxy_->to(client_addr_).timeout(RPC_TIMEOUT_US).trace_time(true).
          max_process_handler_time(RPC_TIMEOUT_US).by(MTL_ID()).send_submit_log_resp(resp, &cb))) {
        CLOG_LOG(ERROR, "send_submit_log_resp failed", KR(ret), K(resp));
      }
    }
    return OB_SUCCESS;
  }

  int on_failure() override final
  {
    ATOMIC_STORE(&is_called_, true);
    if (OB_NOT_NULL(rpc_proxy_) && client_addr_.is_valid()) {
      //notify called
    }
    return OB_SUCCESS;
  }

  void reset()
  {
    ATOMIC_STORE(&is_called_, false);
  }

  bool is_called() const
  {
    return ATOMIC_LOAD(&is_called_);
  }

  obrpc::PalfClusterRpcProxy *rpc_proxy_;
  common::ObAddr client_addr_;
  common::ObAddr self_;
  int64_t palf_id_;
  int64_t client_id_;
  bool is_called_;
};

class LogRemoteClient
{
public:
  LogRemoteClient()
  : thread_(),
    th_id_(0),
    log_size_(0),
    rpc_proxy_(NULL),
    self_(),
    dst_(),
    palf_id_(0),
    cond_(),
    is_returned_(true),
    last_submit_ts_(0),
    avg_rt_(-1),
    is_inited_(false) {}

  int init_and_create(const int64_t th_id,
           const int64_t log_size,
           obrpc::PalfClusterRpcProxy *rpc_proxy,
           const common::ObAddr &self,
           const common::ObAddr &dst,
           const int64_t palf_id)
  {
    int ret = OB_SUCCESS;
    cond_.init(ObWaitEventIds::REBALANCE_TASK_MGR_COND_WAIT);
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (th_id < 0 || log_size <= 0 ||
        rpc_proxy == NULL || false == self.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (0 != pthread_create(&thread_, NULL, do_submit, this)){
      PALF_LOG(ERROR, "create thread fail", K(thread_));
    } else {
      th_id_ = th_id;
      log_size_ = log_size;
      rpc_proxy_ = rpc_proxy;
      self_ = self;
      dst_ = dst;
      palf_id_ = palf_id;
    }
    return ret;
  }

  void join()
  {
    pthread_join(thread_, NULL);
  }

  static void* do_submit(void *arg)
  {
    int ret = OB_SUCCESS;
    const int64_t NBYTES = 40000;
    const int64_t RPC_TIMEOUT_US = 1 * 1000 * 1000;
    char BUFFER[NBYTES];
    memset(BUFFER, 'a', NBYTES);

    LogRemoteClient *client = static_cast<LogRemoteClient *>(arg);
    palf::LogWriteBuf write_buf;
    write_buf.push_back(BUFFER, client->log_size_);

    SubmitLogCmd req(client->self_, client->palf_id_, client->th_id_, write_buf);

    while (true) {
      // const bool is_timeout = (common::ObTimeUtility::current_time() - client->last_submit_ts_) > 500 * 1000;
      // if (client->can_submit())
      static obrpc::ObLogRpcCB<obrpc::OB_LOG_SUBMIT_LOG_CMD> cb;
      if (OB_FAIL(client->rpc_proxy_->to(client->dst_).timeout(RPC_TIMEOUT_US).trace_time(true). \
          max_process_handler_time(RPC_TIMEOUT_US).by(MTL_ID()).send_submit_log_cmd(req, &cb))) {
        PALF_LOG(WARN, "send_submit_log_cmd fail", K(req));
      } else {
        client->has_submit();
        ObThreadCondGuard guard(client->cond_);
        client->cond_.wait();
      }
    }
    return NULL;
  }

  bool can_submit() const
  {
    return ATOMIC_LOAD(&is_returned_);
  }

  void has_submit()
  {
    ATOMIC_STORE(&is_returned_, false);
    last_submit_ts_ = common::ObTimeUtility::current_time();
  }

  void has_returned();

public:
  pthread_t thread_;
  int64_t th_id_;
  int64_t log_size_;
  obrpc::PalfClusterRpcProxy *rpc_proxy_;
  common::ObAddr self_;
  common::ObAddr dst_;
  int64_t palf_id_;
  mutable common::ObThreadCond cond_;
  bool is_returned_;
  int64_t last_submit_ts_;
  int64_t avg_rt_;
  bool is_inited_;
};

class ObLogClient
{
public:
  ObLogClient();
  virtual ~ObLogClient();
  int init(const common::ObAddr &self,
           const int64_t palf_id,
           obrpc::PalfClusterRpcProxy *rpc_proxy,
           palfcluster::LogService *log_service);
  void destroy();
  int create_palf_replica(const common::ObMemberList &member_list,
                          const int64_t replica_num,
                          const int64_t leader_idx);
  int submit_append_log_task(const int64_t thread_num, const int64_t log_size);
  int submit_log(const common::ObAddr &client_addr, const int64_t client_id, const palf::LogWriteBuf &log_buf);
  int do_submit();

  share::ObLSID get_ls_id() const { return share::ObLSID(palf_id_); }
  logservice::ObLogHandler *get_log_handler() { return &log_handler_;}
  TO_STRING_KV(K_(palf_id));
private:
  static const int64_t MAX_THREAD_NUM = 10;
  static const int64_t REMOTE_APPEND_CB_CNT = 4000;
  MockRemoteAppendCb remote_append_cb_list[REMOTE_APPEND_CB_CNT];
  common::ObAddr self_;
  int64_t palf_id_;
  obrpc::PalfClusterRpcProxy *rpc_proxy_;
  logservice::ObLogHandler log_handler_;
  common::ObSpinLock lock_;
  int64_t log_size_;
  logservice::coordinator::ElectionPriorityImpl election_priority_;
  int64_t total_num_;
  palfcluster::LogService *log_service_;
  int64_t client_number_;
  int64_t worker_number_;
  bool is_inited_;
};


typedef common::ObLinearHashMap<share::ObLSID, palfcluster::ObLogClient*> LogClientMap;

} // palfcluster
} // oceanbase

#endif