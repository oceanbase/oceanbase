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

#include "co_thread_mgr.h"
#include "lib/coro/co_thread.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

CoThreadMgr::CoThreadMgr()
{}

CoThreadMgr::~CoThreadMgr()
{}

int CoThreadMgr::add_thread(CoThread& th)
{
  return th_list_.add_last(&th);
}

int CoThreadMgr::start()
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH(curr, th_list_)
  {
    if (OB_FAIL(curr->start())) {
      break;
    }
  }
  if (OB_FAIL(ret)) {
    stop();
    wait();
  }
  return ret;
}

void CoThreadMgr::stop()
{
  DLIST_FOREACH_NORET(curr, th_list_)
  {
    curr->stop();
  }
}

void CoThreadMgr::wait()
{
  DLIST_FOREACH_NORET(curr, th_list_)
  {
    curr->wait();
  }
}
