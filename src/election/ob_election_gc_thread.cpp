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

#include "ob_election_gc_thread.h"
#include "ob_election_group_mgr.h"
#include "ob_election_async_log.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace election {
ObElectionGCThread::ObElectionGCThread() : eg_mgr_(NULL), is_inited_(false)
{}

ObElectionGCThread::~ObElectionGCThread()
{
  destroy();
}

int ObElectionGCThread::init(ObElectionGroupMgr* eg_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    ELECT_ASYNC_LOG(WARN, "init twice", KR(ret));
  } else if (NULL == eg_mgr) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", KR(ret), K(eg_mgr));
  } else {
    eg_mgr_ = eg_mgr;
    is_inited_ = true;
  }
  ELECT_ASYNC_LOG(INFO, "ObElectionGCThread init finished", KR(ret));
  return ret;
}

int ObElectionGCThread::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionGCThread is not inited", K(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ELECT_ASYNC_LOG(ERROR, "ObElectionGCThread failed to start", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObElectionGCThread::stop()
{
  ObThreadPool::stop();
  ELECT_ASYNC_LOG(INFO, "ObElectionGCThread stop");
}

void ObElectionGCThread::wait()
{
  ObThreadPool::wait();
  ELECT_ASYNC_LOG(INFO, "ObElectionGCThread wait");
}

void ObElectionGCThread::destroy()
{
  (void)stop();
  (void)wait();
  is_inited_ = false;
  eg_mgr_ = NULL;
}

int ObElectionGCThread::exec_gc_loop_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(eg_mgr_->exec_gc_loop())) {
    ELECT_ASYNC_LOG(WARN, "exec_gc_loop failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObElectionGCThread::run1()
{
  ELECT_ASYNC_LOG(INFO, "ObElectionGCThread start to run");
  lib::set_thread_name("ObElectionGCThread");
  while (!has_set_stop()) {
    const int64_t begin_ts = ObClockGenerator::getClock();
    (void)exec_gc_loop_();
    const int64_t end_ts = ObClockGenerator::getClock();
    ELECT_ASYNC_LOG(INFO, "ObElectionGCThread loop cost", "cost time", end_ts - begin_ts);
    usleep(ELECT_GC_INTERVAL);
  }
  return;
}

}  // namespace election
}  // namespace oceanbase
