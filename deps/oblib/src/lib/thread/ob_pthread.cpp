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

#include "lib/thread/threads.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase;
using namespace oceanbase::lib;

extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg)
{
  int ret = OB_SUCCESS;
  ObPThread *thread = NULL;
  // Temporarily set expect_run_wrapper to NULL for creating normal thread
  IRunWrapper *expect_run_wrapper = Threads::get_expect_run_wrapper();
  Threads::get_expect_run_wrapper() = NULL;
  DEFER(Threads::get_expect_run_wrapper() = expect_run_wrapper);
  OB_LOG(INFO, "ob_pthread_create start");
  if (OB_ISNULL(thread = OB_NEW(ObPThread, SET_USE_500("PThread"), start_routine, arg))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else if (OB_FAIL(thread->start())) {
    OB_LOG(WARN, "failed to start thread", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(thread)) {
      OB_DELETE(ObPThread, SET_USE_500("PThread"), thread);
    }
  } else {
    ATOMIC_STORE(ptr, thread);
    OB_LOG(INFO, "ob_pthread_create succeed", KP(thread));
  }
  return ret;
}
void ob_pthread_join(void *ptr)
{
  if (OB_NOT_NULL(ptr)) {
    ObPThread *thread = (ObPThread*) ptr;
    thread->wait();
    OB_LOG(INFO, "ob_pthread_join succeed", KP(thread));
    OB_DELETE(ObPThread, SET_USE_500("PThread"), thread);
  }
}

int ob_pthread_tryjoin_np(void *ptr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ptr)) {
    ObPThread *thread = (ObPThread*) ptr;
    if (OB_SUCC(thread->try_wait())) {
      OB_LOG(INFO, "ob_pthread_tryjoin_np succeed", KP(thread));
      OB_DELETE(ObPThread, SET_USE_500("PThread"), thread);
    }
  }
  return ret;
}

pthread_t ob_pthread_get_pth(void *ptr)
{
  pthread_t pth = 0;
  if (OB_NOT_NULL(ptr)) {
    ObPThread *thread = (ObPThread*) ptr;
    pth = thread->get_pthread(0);
  }
  return pth;
}
} /* extern "C" */