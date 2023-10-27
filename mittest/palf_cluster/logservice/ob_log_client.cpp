// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX CLOG
#include "ob_log_client.h"
#include "logservice/palf/palf_env.h"
#include "logservice/palf/palf_handle.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/thread/ob_thread_name.h"        // set_thread_name
#include "lib/function/ob_function.h"         // ObFunction
#include "mittest/palf_cluster/rpc/palf_cluster_rpc_proxy.h"            // RpcProxy
#include "mittest/palf_cluster/logservice/log_service.h"            // LogService

namespace oceanbase
{
using namespace common;
using namespace palf;
namespace palfcluster
{
int64_t c_append_cnt = 0;
int64_t c_rt = 0;

ObLogClient::ObLogClient()
    : self_(),
      palf_id_(-1),
      rpc_proxy_(NULL),
      log_handler_(),
      lock_(),
      log_size_(-1),
      election_priority_(),
      total_num_(0),
      is_inited_(false)
  {}

ObLogClient::~ObLogClient()
{
  destroy();
}

void ObLogClient::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    rpc_proxy_ = NULL;
    palf_id_ = -1;
    log_handler_.destroy();
    log_size_ = -1;
  }
}

int ObLogClient::init(const common::ObAddr &self,
                      const int64_t palf_id,
                      obrpc::PalfClusterRpcProxy *rpc_proxy,
                      LogService *log_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogClient has been inited", K(ret));
  } else if (false == self.is_valid()|| OB_ISNULL(rpc_proxy) || palf_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(self), K(palf_id), K(rpc_proxy));
  } else {
    for (int i = 0; i < REMOTE_APPEND_CB_CNT; i++) {
      if (OB_FAIL(remote_append_cb_list[i].init(rpc_proxy, self))) {
        CLOG_LOG(WARN, "init", K(ret), K(self), K(palf_id), K(rpc_proxy));
      }
    }
    self_ = self;
    palf_id_ = palf_id;
    rpc_proxy_ = rpc_proxy;
    log_service_ = log_service;
    is_inited_ = true;
  }

  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  CLOG_LOG(INFO, "ObLogClient init finished", K(ret), K(self));
  return ret;
}

int ObLogClient::create_palf_replica(const common::ObMemberList &member_list,
                                     const int64_t replica_num,
                                     const int64_t leader_idx)
{
  int ret = OB_SUCCESS;
  if(IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogClient has not been inited", K(ret));
  } else if (false == member_list.is_valid() || replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    share::ObLSID ls_id(palf_id_);
    share::ObTenantRole tenant_role(share::ObTenantRole::PRIMARY_TENANT);
    common::ObAddr leader;
    member_list.get_server_by_index(leader_idx, leader);

    LSN init_lsn(0);
    common::ObILogAllocator *alloc_mgr = NULL;
    palf::PalfBaseInfo palf_base_info;
    palf_base_info.generate_by_default();
    common::GlobalLearnerList learner_list;
    if (OB_FAIL(log_service_->create_ls(ls_id, ObReplicaType::REPLICA_TYPE_FULL,
        tenant_role, palf_base_info, true, log_handler_))) {
      CLOG_LOG(WARN, "create_ls failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(log_handler_.set_initial_member_list(member_list, replica_num, learner_list))) {
      CLOG_LOG(WARN, "set_initial_member_list failed", K(ret), K_(palf_id));
    } else {
      CLOG_LOG(ERROR, "create_palf_replica success", K(ret), K_(palf_id), K(member_list), K(replica_num), K(leader_idx));
    }
  }
  return ret;
}

static void *append_fn(void *arg)
{
  ObLogClient *client = reinterpret_cast<ObLogClient *>(arg);
  client->do_submit();
  return (void *)0;
}

int ObLogClient::submit_append_log_task(const int64_t thread_num, const int64_t log_size)
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t unused_pid;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_handler_.get_role(role, unused_pid))) {
  } else if (role != LEADER) {
    CLOG_LOG(ERROR, "client is not leader");
  } else if (OB_FAIL(lock_.trylock())) {
    CLOG_LOG(ERROR, "another client running", K(ret));
  } else {
    log_size_ = log_size;
    client_number_ = thread_num;
    worker_number_ = (client_number_ >= MAX_THREAD_NUM)? MAX_THREAD_NUM: client_number_;
    pthread_t tids[MAX_THREAD_NUM];

    CLOG_LOG(ERROR, "start submit_log", K_(log_size), K(thread_num), K(worker_number_));
    for (int64_t i = 0; i < worker_number_; i++) {
      pthread_create(&tids[i], NULL, append_fn, this);
    }

    for (int64_t i = 0; i < worker_number_; i++) {
      pthread_join(tids[i], NULL);
    }
    lock_.unlock();
  }
  return ret;
}

int ObLogClient::do_submit()
{
  int ret = OB_SUCCESS;
  const int64_t NBYTES = 40000;
  char BUFFER[NBYTES];
  memset(BUFFER, 'a', NBYTES);
  const int64_t CB_ARRAY_NUM = (client_number_ >= worker_number_)? client_number_ / worker_number_: 1;
  MockAppendCb *cb_array = new MockAppendCb[CB_ARRAY_NUM];
  LSN lsn;
  SCN log_scn;
  while (true) {
    for (int i = 0; i < CB_ARRAY_NUM; i++)
    {
      if (cb_array[i].is_called()) {
        cb_array[i].reset();
        const int64_t log_size = (log_size_ > 0)? log_size_: ObRandom::rand(100, 1024);
        cb_array[i].log_size_ = log_size;
        ret = log_handler_.append(BUFFER, log_size, SCN::min_scn(), true, &cb_array[i], lsn, log_scn);
        if (OB_SUCCESS != ret) {
          (void) cb_array[i].on_success();
        }
      }
    }
  }
  while (true) {
    sleep(10);
  }
  return ret;
}

int MockAppendCb::on_success()
{
  ATOMIC_STORE(&is_called_, true);
  return OB_SUCCESS;
}

int ObLogClient::submit_log(const common::ObAddr &client_addr, const int64_t client_id, const palf::LogWriteBuf &log_buf)
{
  int ret = OB_SUCCESS;
  LSN lsn;
  SCN log_scn;
  const char *buf = log_buf.write_buf_[0].buf_;
  const int64_t buf_len = log_buf.write_buf_[0].buf_len_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(remote_append_cb_list[client_id].pre_submit(client_addr, palf_id_, client_id))) {
    // CLOG_LOG(WARN, "append_cb init failed", K(ret));
  } else if (OB_FAIL(log_handler_.append(buf, buf_len, SCN::min_scn(), true, &(remote_append_cb_list[client_id]), lsn, log_scn))) {
    CLOG_LOG(WARN, "append failed", K(ret));
  }
  return OB_SUCCESS;
}

void LogRemoteClient::has_returned()
{
  const int64_t tmp_rt = common::ObTimeUtility::current_time() - last_submit_ts_;

  ATOMIC_STORE(&is_returned_, true);
  ObThreadCondGuard guard(cond_);
  cond_.signal();

  ATOMIC_INC(&c_append_cnt);
  ATOMIC_FAA(&c_rt, tmp_rt);

  if (REACH_TIME_INTERVAL(1000 *1000)) {
    int64_t l_append_cnt = ATOMIC_LOAD(&c_append_cnt);
    if (l_append_cnt == 0) l_append_cnt = 1;
    int64_t l_rt = ATOMIC_LOAD(&c_rt);

    ATOMIC_STORE(&c_append_cnt, 0);
    ATOMIC_STORE(&c_rt, 0);

    CLOG_LOG_RET(ERROR, OB_SUCCESS, "result:", K(l_append_cnt), K(l_rt), "avg_rt", l_rt/l_append_cnt);
  }
}
} // end namespace palfcluster
} // end namespace oceanbase