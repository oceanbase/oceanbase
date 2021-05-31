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

#include "election/ob_election_rpc.h"
#include "common/ob_clock_generator.h"
#include "mock_ob_election_mgr.h"
#include "share/ob_errno.h"
#include "mock_ob_election_rpc.h"

namespace oceanbase {
using namespace common;
using namespace election;
namespace unittest {
static const int64_t WARN_THRESHOLD = 1000000;

int64_t ElectionRpcTaskFactory::alloc_count_ = 0;
int64_t ElectionRpcTaskFactory::release_count_ = 0;

ElectionRpcTask* ElectionRpcTaskFactory::alloc()
{
  ElectionRpcTask* task = NULL;

  if (NULL == (task = rp_alloc(ElectionRpcTask, ObModIds::OB_ELECTION))) {
    ELECT_LOG(WARN, "ElectionRpcTask alloc failed.", "task", OB_P(task));
  } else {
    ATOMIC_FAA(&alloc_count_, 1);
  }

  return task;
}

void ElectionRpcTaskFactory::release(ElectionRpcTask* task)
{
  if (NULL == task) {
    ELECT_LOG(WARN, "election rpc task is NULL");
  } else {
    rp_free(task, ObModIds::OB_ELECTION);
    ATOMIC_FAA(&release_count_, 1);
    task = NULL;
  }
}

int MockObElectionRpc::init(ObIElectionMgr* election_mgr, const ObAddr& self)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = S2MQueueThread::init(4, 100000, false, false, self))) {
    ELECT_LOG(WARN, "S2MQueueThread init error.", K(ret));
  } else {
    election_mgr_ = election_mgr;
    inited_ = true;
  }

  return ret;
}

void MockObElectionRpc::handle(void* task, void* pdata)
{
  int ret = OB_SUCCESS;
  UNUSED(pdata);
  obrpc::ObElectionRpcResult result;

  ELECT_LOG(INFO, "MockObTransRpc handle");
  ElectionRpcTask* rpc_task = static_cast<ElectionRpcTask*>(task);
  MockObElectionMgr* election_mgr = static_cast<MockObElectionMgr*>(election_mgr_);
  // ELECT_LOG(INFO, "idx_", K(election_mgr_->idx_));
  if (!inited_) {
    ELECT_LOG(WARN, "MockObElectionMgr not inited");
  } else if (NULL == election_mgr_) {
    ELECT_LOG(WARN, "MockObElectionMgr is null");
  } else if (NULL == rpc_task) {
    ELECT_LOG(WARN, "ElectionRpcTask rpc task is null");
  } else if (OB_SUCCESS != (ret = election_mgr->handle_election_msg(rpc_task->msgbuf_, result))) {
    ELECT_LOG(WARN, "handle election message error", K(ret));
  } else {
    ELECT_LOG(INFO, "handle election message");
    const int64_t used_time = ObClockGenerator::getClock() - rpc_task->timestamp_;
    if (used_time > WARN_THRESHOLD) {
      ELECT_LOG(INFO, "handle transaction message success", "server", rpc_task->server_, K(used_time));
    }
  }
  if (NULL != rpc_task) {
    ElectionRpcTaskFactory::release(rpc_task);
    rpc_task = NULL;
  }
}

}  // namespace unittest
}  // namespace oceanbase
