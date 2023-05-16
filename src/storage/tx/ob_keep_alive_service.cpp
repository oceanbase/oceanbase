// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_handler.h"
#include "storage/tx/ob_keep_alive_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace share;
using namespace logservice;

namespace transaction
{

int ObKeepAliveService::mtl_init(ObKeepAliveService *& ka)
{
  return ka->init();
}

int ObKeepAliveService::init()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  TRANS_LOG(INFO, "[Keep Alive] init");

  lib::ThreadPool::set_run_wrapper(MTL_CTX());

  return ret;
}

int ObKeepAliveService::start()
{
  int ret = OB_SUCCESS;

  TRANS_LOG(INFO, "[Keep Alive] start");
  if (OB_FAIL(lib::ThreadPool::start())) {
    TRANS_LOG(WARN, "[Keep Alive] start keep alive thread failed", K(ret));
  } else {
    // TRANS_LOG(INFO, "[Keep Alive] start keep alive thread succeed", K(ret));
  }

  return ret;
}

void ObKeepAliveService::stop()
{
  TRANS_LOG(INFO, "[Keep Alive] stop");
  lib::ThreadPool::stop();
}

void ObKeepAliveService::wait()
{
  TRANS_LOG(INFO, "[Keep Alive] wait");
  lib::ThreadPool::wait();
}

void ObKeepAliveService::destroy()
{
  TRANS_LOG(INFO, "[Keep Alive] destroy");
  lib::ThreadPool::destroy();
  reset();
}

void ObKeepAliveService::reset()
{
  need_print_ = false;

}

void ObKeepAliveService::run1()
{
  int ret = OB_SUCCESS;
  int64_t start_time_us = 0;
  int64_t time_used = 0;
  lib::set_thread_name("TxKeepAlive");

  // int64_t loop_cnt = 0;
  while (!has_set_stop()) {
    start_time_us = ObTimeUtility::current_time();

    if (REACH_TIME_INTERVAL(KEEP_ALIVE_PRINT_INFO_INTERVAL)) {
      // TRANS_LOG(INFO, "[Keep Alive LOOP]", K(loop_cnt));
      need_print_ = true;
      // loop_cnt = 0;
    }

    if (OB_FAIL(scan_all_ls_())) {
      TRANS_LOG(WARN, "[Keep Alive] scan all ls failed", K(ret));
    }

    // loop_cnt += 1;
    need_print_ = false;

    time_used = ObTimeUtility::current_time() - start_time_us;
    if (time_used < KEEP_ALIVE_INTERVAL) {
      usleep(KEEP_ALIVE_INTERVAL - time_used);
    }
  }
}

int ObKeepAliveService::scan_all_ls_()
{
  int ret = OB_SUCCESS;
  int iter_ret = OB_SUCCESS;

  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *iter_ptr = nullptr;
  ObLS *cur_ls_ptr = nullptr;

  int64_t ls_cnt = 0;

  if (OB_ISNULL(MTL(ObLSService *)) || OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLS::LSGetMod::STORAGE_MOD))
      || !ls_iter_guard.is_valid()) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    TRANS_LOG(WARN, "[Keep Alive] get ls iter failed", K(ret), KP(MTL(ObLSService *)));
  } else if (OB_ISNULL(iter_ptr = ls_iter_guard.get_ptr())) {
    TRANS_LOG(WARN, "[Keep Alive] ls iter_ptr is nullptr", KP(iter_ptr));
  } else {
    iter_ret = OB_SUCCESS;
    cur_ls_ptr = nullptr;
    while (OB_SUCCESS == (iter_ret = iter_ptr->get_next(cur_ls_ptr)) && OB_NOT_NULL(cur_ls_ptr)) {
      if (cur_ls_ptr->get_keep_alive_ls_handler()->try_submit_log()) {
        TRANS_LOG(WARN, "[Keep Alive] try submit keep alive log failed", K(ret));
      }
      if (need_print_) {
        ls_cnt += 1;
        cur_ls_ptr->get_keep_alive_ls_handler()->print_stat_info();
      }
    }
  }

  if (need_print_) {
    TRANS_LOG(INFO, "[Keep Alive Loop] ", "LS_CNT", ls_cnt);
  }
  return ret;
}

}
}
