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

const std::string TEST_NAME = "palf_cluster_bench_client";
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


class TestObPalfClusterBench : public ObSimpleLogCluster
{
public:
  std::vector<int64_t> palf_id_list_;
  std::vector<common::ObAddr> palf_leader_list_;
  std::vector<common::ObMemberList> palf_group_list_;
  std::vector<common::ObAddr> server_list_;
public:
  TestObPalfClusterBench() : ObSimpleLogCluster()
  {
    server_list_.push_back(server1);
    server_list_.push_back(server2);
    server_list_.push_back(server3);
  }

  int create_palf_group()
  {
    int ret = OB_SUCCESS;
    const int64_t replica_num = oceanbase::unittest::replica_num_arg;
    palfcluster::LogService *log_service = get_log_server()->get_log_service();
    obrpc::PalfClusterRpcProxy *rpc_proxy = NULL;
    const int64_t RPC_TIMEOUT_US = 5 * 1000 * 1000;
    ObAddr self;
    ObMemberList memberlist;
    for (int i = 0; i < replica_num; i++) {
      memberlist.add_server(server_list_[i]);
    }

    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_log_service failed");
    } else if (FALSE_IT(rpc_proxy = log_service->get_rpc_proxy())) {
    } else if (FALSE_IT(self = log_service->get_self())) {
    } else {
      for (int i = 0; i < oceanbase::unittest::palf_group_number_arg; i++) {
        const int64_t leader_index = i % replica_num;
        ObAddr leader;
        memberlist.get_server_by_index(leader_index, leader);
        palf_leader_list_.push_back(leader);
        for (int j = 0; j < replica_num; j++) {
          palfcluster::LogCreateReplicaCmd req(self, i + 1, memberlist, replica_num, leader_index);
          if (OB_FAIL(rpc_proxy->to(server_list_[j]).timeout(RPC_TIMEOUT_US).trace_time(true).
              max_process_handler_time(RPC_TIMEOUT_US).by(MTL_ID()).send_create_replica_cmd(req, NULL))) {
            CLOG_LOG(ERROR, "create_replica failed", KR(ret), K(req), K(server_list_[j]));
            break;
          }
        }
      }
      LOG_STDOUT("create_replica success\n");
    }
    return ret;
  }
};

std::string ObSimpleLogCluster::test_name_ = TEST_NAME;

TEST_F(TestObPalfClusterBench, multiple_replica)
{
  int ret = OB_SUCCESS;
  // client mode
  OB_LOGGER.set_log_level("ERROR");
  if (OB_FAIL(create_palf_group())) {
    CLOG_LOG(ERROR, "create_palf_group failed", K(ret));
  } else {
    sleep(3);
    if (OB_FAIL(create_palf_group())) {
      CLOG_LOG(ERROR, "create_palf_group failed", K(ret));
    }
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
    oceanbase::unittest::palf_group_number_arg = strtol(argv[3], NULL, 10);
    oceanbase::unittest::replica_num_arg = strtol(argv[4], NULL, 10);
  }
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
