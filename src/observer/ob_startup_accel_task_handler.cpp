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

#define USING_LOG_PREFIX SERVER
#include "observer/ob_startup_accel_task_handler.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
const int64_t ObStartupAccelTaskHandler::MAX_QUEUED_TASK_NUM = 128;
const int64_t ObStartupAccelTaskHandler::MAX_THREAD_NUM = 64;

ObStartupAccelTaskHandler::ObStartupAccelTaskHandler()
  : is_inited_(false),
    tg_id_(-1),
    task_allocator_()
{}

ObStartupAccelTaskHandler::~ObStartupAccelTaskHandler()
{
  destroy();
}

int ObStartupAccelTaskHandler::init(ObStartupAccelType accel_type)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr = ObMemAttr(SERVER_ACCEL == accel_type ? OB_SERVER_TENANT_ID : MTL_ID(),
                                 "StartupTask",
                                 ObCtxIds::DEFAULT_CTX_ID);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStartupAccelTaskHandler has already been inited", K(ret));
  } else if (OB_FAIL(task_allocator_.init(lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("fail to init tenant tiny allocator", K(ret));
  } else if (SERVER_ACCEL == accel_type &&
      OB_FAIL(TG_CREATE(lib::TGDefIDs::StartupAccelHandler, tg_id_))) {
    LOG_WARN("lib::TGDefIDs::StartupAccelHandler tg create", K(ret));
  } else if (TENANT_ACCEL == accel_type &&
      OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StartupAccelHandler, tg_id_))) {
    LOG_WARN("lib::TGDefIDs::StartupAccelHandler tg create", K(ret));
  } else {
    is_inited_ = true;
    accel_type_ = accel_type;
  }
  return ret;
}

int64_t ObStartupAccelTaskHandler::get_thread_cnt()
{
  int64_t thread_cnt = 1;
  if (lib::is_mini_mode()) {
    thread_cnt = 1;
  } else {
    if (SERVER_ACCEL == accel_type_) {
      thread_cnt = common::get_cpu_count();
    } else {
      thread_cnt = MTL_CPU_COUNT();
    }
  }

  return std::min(MAX_THREAD_NUM, thread_cnt);
}

int ObStartupAccelTaskHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStartupAccelTaskHandler not inited", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER(tg_id_, *this))) {
    LOG_WARN("tg set handler failed", K(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, get_thread_cnt()))) {
    LOG_WARN("tg set thread cnt failed", K(ret), K(tg_id_), K(get_thread_cnt()));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start ObStartupAccelTaskHandler", K(ret));
  }
  return ret;
}

void ObStartupAccelTaskHandler::stop()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
  }
}

void ObStartupAccelTaskHandler::wait()
{
  if (IS_INIT) {
    TG_WAIT(tg_id_);
  }
}

void ObStartupAccelTaskHandler::destroy()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
    task_allocator_.reset();
    is_inited_ = false;
  }
}

int ObStartupAccelTaskHandler::push_task(ObStartupAccelTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStartupAccelTaskHandler not inited", K(ret));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    LOG_WARN("fail to push startup accel task", K(ret), KPC(task));
  }
  return ret;
}

void ObStartupAccelTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (NULL == task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", K(ret));
  } else {
    ObStartupAccelTask *startup_task = static_cast<ObStartupAccelTask *>(task);
    if (OB_FAIL(startup_task->execute())) {
      LOG_WARN("fail to execute startup task", K(ret), KPC(startup_task));
    }
    startup_task->~ObStartupAccelTask();
    task_allocator_.free(startup_task);
  }
}

} // observer
} // oceanbase
