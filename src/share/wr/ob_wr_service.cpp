/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX WR

#include "share/wr/ob_wr_service.h"
#include "share/wr/ob_wr_task.h"
#include "share/scn.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_errno.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{

ObWorkloadRepositoryService::ObWorkloadRepositoryService() : is_inited_(false), wr_timer_task_()
{}

int ObWorkloadRepositoryService::replay(
    const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  UNUSED(buffer);
  UNUSED(nbytes);
  UNUSED(lsn);
  UNUSED(scn);
  return ret;
}

share::SCN ObWorkloadRepositoryService::get_rec_scn()
{
  return SCN::max_scn();
}

int ObWorkloadRepositoryService::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObWorkloadRepositoryService::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wr_timer_task_.init())) {
    LOG_WARN("failed to init wr timer", K(ret), K_(wr_timer_task));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObWorkloadRepositoryService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("wr service not init", K(ret));
  } else if (OB_FAIL(wr_timer_task_.start())) {
    LOG_WARN("failed to start wr timer thread", K(ret));
  }
  return ret;
}

void ObWorkloadRepositoryService::stop()
{
  wr_timer_task_.stop();
}

void ObWorkloadRepositoryService::wait()
{
  wr_timer_task_.wait();
}

void ObWorkloadRepositoryService::destroy()
{
  wr_timer_task_.destroy();
}

int ObWorkloadRepositoryService::inner_switch_to_leader()
{
  int ret = OB_SUCCESS;
  // schedule wr timer task
  // TODO(roland.qk): need to cancel wr task first?
  if (OB_FAIL(wr_timer_task_.schedule_one_task())) {
    LOG_WARN("failed to schedule wr timer task", K(ret));
  } else {
    LOG_INFO("current observer is leader, start to dispatch workload repository snapshot timer",
        KPC(this));
  }
  return ret;
}

int ObWorkloadRepositoryService::inner_switch_to_follower()
{
  int ret = OB_SUCCESS;
  // cancel previous wr timer task
  wr_timer_task_.cancel_current_task();
  LOG_INFO("stop to execute workload repository snapshot timer", KPC(this));
  return ret;
}

void ObWorkloadRepositoryService::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("failed to switch leader", K(ret));
  }
}

int ObWorkloadRepositoryService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_leader())) {
    LOG_WARN("failed to switch leader", K(ret));
  }
  return ret;
}

int ObWorkloadRepositoryService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("failed to switch leader", K(ret));
  }
  return ret;
}

int ObWorkloadRepositoryService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_leader())) {
    LOG_WARN("failed to switch leader", K(ret));
  }
  return ret;
}

int ObWorkloadRepositoryService::cancel_current_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("wr service not init", K(ret));
  } else if (FALSE_IT(wr_timer_task_.cancel_current_task())) {
  }
  return ret;
}

int ObWorkloadRepositoryService::schedule_new_task(const int64_t interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("wr service not init", K(ret));
  } else if (OB_FAIL(wr_timer_task_.schedule_one_task(interval))) {
    LOG_WARN("failed to schedule a new task of wr timer thread", K(ret), K(interval));
  }
  return ret;
}

}  // end of namespace share
}  // end of namespace oceanbase
