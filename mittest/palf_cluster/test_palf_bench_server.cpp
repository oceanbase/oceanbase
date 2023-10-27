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

#include <gtest/gtest.h>
#include "io/easy_connection.h"
#include "lib/file/file_directory_utils.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include <cstdio>
#include <signal.h>
#include "lib/utility/ob_defer.h"
#include "share/ob_errno.h"
#define private public
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_rpc_processor.h"
#include "env/ob_simple_log_cluster.h"

#undef private

const std::string TEST_NAME = "palf_cluster_bench_server";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{

char *level;
int thread_num_arg = 100;
int nbytes_arg = 500;
int palf_group_number_arg = 1;
int replica_num_arg = 3;

ObAddr server1(ObAddr::VER::IPV4, "SERVER_IP1", ObSimpleLogCluster::RPC_PORT);
ObAddr server2(ObAddr::VER::IPV4, "SERVER_IP2", ObSimpleLogCluster::RPC_PORT);
ObAddr server3(ObAddr::VER::IPV4, "SERVER_IP3", ObSimpleLogCluster::RPC_PORT);

class TestPalfClusterBenchServer : public ObSimpleLogCluster
{
public:
  std::vector<int64_t> palf_id_list_;
  std::vector<common::ObAddr> palf_leader_list_;
  std::vector<common::ObMemberList> palf_group_list_;
  std::vector<common::ObAddr> server_list_;
public:
  TestPalfClusterBenchServer() : ObSimpleLogCluster()
  {
    server_list_.push_back(server1);
    server_list_.push_back(server2);
    server_list_.push_back(server3);
  }

  int local_submit_log(const int64_t thread_num, const int64_t log_size)
  {
    int ret = OB_SUCCESS;
    palfcluster::LogService *log_service = get_log_server()->get_log_service();
    obrpc::PalfClusterRpcProxy *rpc_proxy = NULL;
    const int64_t RPC_TIMEOUT_US = 500 * 1000 * 1000;
    palfcluster::LogClientMap *log_clients = nullptr;
    palfcluster::ObLogClient *log_client;
    int64_t palf_id = 1;
    const share::ObLSID ls_id(palf_id);
    ObAddr self;

    if (OB_ISNULL(log_service)) {
      CLOG_LOG(ERROR, "get_log_service failed");
    } else if (nullptr == (log_clients = log_service->get_log_client_map())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_log_clients failed", K(ret));
    } else {
      int64_t unused_id;
      common::ObRole role = ObRole::INVALID_ROLE;
      while (true) {
        role = ObRole::INVALID_ROLE;
        if (OB_FAIL(log_clients->get(ls_id, log_client))) {
          CLOG_LOG(ERROR, "get_log_client failed", K(ret), K(palf_id));
          usleep(1000 * 1000);
        } else if (OB_FAIL(log_client->get_log_handler()->get_role(role, unused_id))) {
          CLOG_LOG(ERROR, "get_role fail", K(palf_id));
        } else if (role == ObRole::LEADER) {
          break;
          CLOG_LOG(ERROR, "switch_to_leader success", K(palf_id));
        } else {
          usleep(100* 1000);
        }
      }

      if (OB_FAIL(log_client->submit_append_log_task(thread_num, log_size))) {
        CLOG_LOG(ERROR, "submit_log failed", K(ret));
      }
    }
    PALF_LOG(INFO, "end test_palf_bench");

    LOG_STDOUT("submit_log success\n");
    return ret;
  }

};

std::string ObSimpleLogCluster::test_name_ = TEST_NAME;

TEST_F(TestPalfClusterBenchServer, multiple_replica)
{
  int ret = OB_SUCCESS;
  OB_LOGGER.set_log_level("WDIAG");
  // server mode
  if (OB_FAIL(local_submit_log(oceanbase::unittest::thread_num_arg, oceanbase::unittest::nbytes_arg))) {
    CLOG_LOG(ERROR, "local_submit_log failed");
  }
  while (true) {
    usleep(1000 * 1000);
  }
}

} // end unittest
} // end oceanbase

// Notes: How to write a new module integrate test case in logservice?
// 1. cp test_ob_simple_log_basic_func.cpp test_ob_simple_log_xxx.cpp
// 2. modify const string TEST_NAME, class name and log file name in
// test_ob_simple_log_xxx.cpp
// 3. add ob_unittest_clog() item and set label for test_ob_simple_log_xxx in
// unittest/cluster/CMakeFiles.txt
// 4. write new TEST_F

int main(int argc, char **argv)
{
  if (argc > 1) {
    oceanbase::unittest::thread_num_arg = strtol(argv[1], NULL, 10);
    oceanbase::unittest::nbytes_arg = strtol(argv[2], NULL, 10);
  }
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}