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

#include "ob_trans_migrate_worker.h"
#include "ob_trans_timer.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase {
using namespace common;

namespace transaction {

int ObTransMigrateWorker::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTransMigrateWorker inited twice", KR(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(lib::TGDefIDs::TransMigrate, *this))) {
    TRANS_LOG(WARN, "thread pool init error", K(ret));
  } else {
    is_inited_ = true;
    TRANS_LOG(INFO, "ObTransMigrateWorker inited success", KP(this));
  }

  return ret;
}

int ObTransMigrateWorker::push(void* task)
{
  return TG_PUSH_TASK(lib::TGDefIDs::TransMigrate, task);
}

int ObTransMigrateWorker::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransMigrateWorker is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObTransMigrateWorker is already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObTransMigrateWorker start success", KP(this));
  }

  return ret;
}

void ObTransMigrateWorker::stop()
{
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransMigrateWorker is not inited");
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTransMigrateWorker already has been stopped");
  } else {
    TG_STOP(lib::TGDefIDs::TransMigrate);
    is_running_ = false;
    TRANS_LOG(INFO, "ObTransMigrateWorker stop success");
  }
}

void ObTransMigrateWorker::wait()
{
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransMigrateWorker is not inited");
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransMigrateWorker is running");
  } else {
    TRANS_LOG(INFO, "ObTransMigrateWorker wait success");
  }
}

void ObTransMigrateWorker::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "ObTransMigrateWorker destroyed", KP(this));
  }
}

void ObTransMigrateWorker::handle(void* task)
{
  int ret = OB_SUCCESS;

  if (NULL == task) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    ObPrepareChangingLeaderTask* tmp_task = static_cast<ObPrepareChangingLeaderTask*>(task);
    if (OB_FAIL(tmp_task->run())) {
      // task's memory was freed in run function
      TRANS_LOG(WARN, "prepare changing leader failed", KR(ret));
    }
  }
}

}  // namespace transaction
}  // namespace oceanbase
