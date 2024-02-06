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
#include "observer/ob_server_startup_task_handler.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase
{
namespace observer
{
const int64_t ObServerStartupTaskHandler::MAX_QUEUED_TASK_NUM = 128;
const int64_t ObServerStartupTaskHandler::MAX_THREAD_NUM = 64;

ObServerStartupTaskHandler::ObServerStartupTaskHandler()
  : is_inited_(false),
    tg_id_(-1),
    task_allocator_()
{}

ObServerStartupTaskHandler::~ObServerStartupTaskHandler()
{
  destroy();
}

int ObServerStartupTaskHandler::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerStartupTaskHandler has already been inited", K(ret));
  } else if (OB_FAIL(task_allocator_.init(lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE, ObMemAttr(OB_SERVER_TENANT_ID, "StartupTask", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init tenant tiny allocator", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::SvrStartupHandler, tg_id_))) {
    LOG_WARN("lib::TGDefIDs::SvrStartupHandler tg create", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObServerStartupTaskHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerStartupTaskHandler not inited", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("fail to start ObServerStartupTaskHandler", K(ret));
  }
  return ret;
}

void ObServerStartupTaskHandler::stop()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
  }
}

void ObServerStartupTaskHandler::wait()
{
  if (IS_INIT) {
    TG_WAIT(tg_id_);
  }
}

void ObServerStartupTaskHandler::destroy()
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

int ObServerStartupTaskHandler::push_task(ObServerStartupTask *task)
{
	int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerStartupTaskHandler not inited", K(ret));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    LOG_WARN("fail to push server startup task", K(ret), KPC(task));
  }
  return ret;
}

void ObServerStartupTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (NULL == task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", K(ret));
  } else {
    ObServerStartupTask *startup_task = static_cast<ObServerStartupTask *>(task);
    if (OB_FAIL(startup_task->execute())) {
      LOG_WARN("fail to execute startup task", K(ret), KPC(startup_task));
    }
    startup_task->~ObServerStartupTask();
    task_allocator_.free(startup_task);
  }
}

} // observer
} // oceanbase
