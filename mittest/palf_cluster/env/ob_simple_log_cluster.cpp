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

#include "ob_simple_log_cluster.h"
#include "common/ob_member_list.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "ob_mittest_utils.h"
#include "lib/alloc/memory_dump.h"
#include "lib/alloc/alloc_func.cpp"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/allocator/ob_libeasy_mem_pool.h"
#include <algorithm>

namespace oceanbase
{
namespace unittest
{
ObSimpleLogServer* ObSimpleLogCluster::log_server_;
int64_t ObSimpleLogCluster::node_cnt_ = 1;
common::ObMemberList ObSimpleLogCluster::node_list_ = ObMemberList();
bool ObSimpleLogCluster::is_started_ = false;
char ObSimpleLogCluster::sig_buf_[sizeof(ObSignalWorker) + sizeof(observer::ObSignalHandle)];
ObSignalWorker *ObSimpleLogCluster::sig_worker_ = new (sig_buf_) ObSignalWorker();
observer::ObSignalHandle *ObSimpleLogCluster::signal_handle_ = new (sig_worker_ + 1) observer::ObSignalHandle();

void ObSimpleLogCluster::SetUpTestCase()
{
  SERVER_LOG(INFO, "SetUpTestCase");
  int ret = OB_SUCCESS;
  if (!is_started_) {
    ret = start();
  }
  ASSERT_EQ(ret, OB_SUCCESS);
}

void ObSimpleLogCluster::TearDownTestCase()
{
  SERVER_LOG(INFO, "TearDownTestCase", K(node_cnt_));
  int ret = OB_SUCCESS;

  ret = close();
  ASSERT_EQ(ret, OB_SUCCESS);
  ob_delete(log_server_);
}

int ObSimpleLogCluster::start()
{
  int ret = OB_SUCCESS;
  ObTenantMutilAllocatorMgr::get_instance().init();
  ObMemoryDump::get_instance().init();
  // set easy allocator for watching easy memory holding
  easy_pool_set_allocator(ob_easy_realloc);
  ev_set_allocator(ob_easy_realloc);
  lib::set_memory_limit(100L * 1000L * 1000L * 1000L);
  if (sig_worker_ != nullptr && OB_FAIL(sig_worker_->start())) {
    SERVER_LOG(ERROR, "Start signal worker error", K(ret));
  } else if (signal_handle_ != nullptr && OB_FAIL(signal_handle_->start())) {
    SERVER_LOG(ERROR, "Start signal handle error", K(ret));
  } else if (OB_FAIL(generate_sorted_server_list_(node_cnt_))) {
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node_cnt_; i++) {
      auto svr = OB_NEW(ObSimpleLogServer, "TestBase");
      common::ObAddr server;
      if (OB_FAIL(node_list_.get_server_by_index(i, server))) {
      } else if (OB_FAIL(svr->simple_init(test_name_, server, 1, true))) {
        SERVER_LOG(WARN, "simple_init failed", K(ret), K(i), K_(node_list));
      } else if (OB_FAIL(svr->simple_start(true))) {
        SERVER_LOG(WARN, "simple_start failed", K(ret), K(i), K_(node_list));
      } else {
        log_server_ = svr;
      }
    }
    is_started_ = true;
    SERVER_LOG(INFO, "ObSimpleLogCluster started", K(ret), K_(node_cnt), K_(node_list));
  }
  return ret;
}

int ObSimpleLogCluster::close()
{
  int ret = OB_SUCCESS;
  ret = log_server_->simple_close(true);
  if (OB_FAIL(ret)) {
    SERVER_LOG(WARN, "simple_close failed", K(ret));
  }
  return ret;
}

int ObSimpleLogCluster::generate_sorted_server_list_(const int64_t node_cnt)
{
  int ret = OB_SUCCESS;
  // each get_rpc_port calling will get two available ports,
  // so just get node_cnt / 2 + 1 times
  const int64_t get_port_cnt = node_cnt / 2 + 1;
  for (int i = 0; i < get_port_cnt; i++) {
    int server_fd = 0;
    const std::string local_ip = get_local_ip();
    // const int64_t port = get_rpc_port(server_fd);
    const int64_t port = RPC_PORT;
    common::ObAddr addr;
    if (0 == port) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get_rpc_port failed", K(ret), K(port));
      break;
    } else if (local_ip == "") {
      ret = OB_ERR_UNEXPECTED;
    } else if (false == addr.set_ip_addr(local_ip.c_str(), port)) {
      SERVER_LOG(ERROR, "set_ip_addr failed", K(local_ip.c_str()), K(port), K(addr));
    } else if (OB_FAIL(node_list_.add_server(addr))) {
      PALF_LOG(WARN, "add_server failed", K(ret));
    } else if (false == addr.set_ip_addr(local_ip.c_str(), port + 1)) {
      SERVER_LOG(ERROR, "set_ip_addr failed", K(local_ip.c_str()), K(port), K(addr));
    } else if (node_list_.get_member_number() < node_cnt && OB_FAIL(node_list_.add_server(addr))) {
      PALF_LOG(WARN, "add_server failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    SERVER_LOG(INFO, "simple log cluster node_list", K_(node_list));
  }
  return ret;
}

} // end unittest
} // end oceanbase