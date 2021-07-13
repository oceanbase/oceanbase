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

#ifndef OCEANBASE_CLOG_OB_LOG_CALLBACK_THREAD_POOL_
#define OCEANBASE_CLOG_OB_LOG_CALLBACK_THREAD_POOL_

#include <cstdlib>
#include "lib/net/ob_addr.h"
#include "lib/thread/thread_mgr_interface.h"
#include "ob_log_callback_handler.h"

namespace oceanbase {
namespace clog {
class ObLogCallbackHandler;
class ObLogCallbackThreadPool : public lib::TGTaskHandler {
public:
  ObLogCallbackThreadPool() : is_inited_(false), handler_(NULL)
  {}
  virtual ~ObLogCallbackThreadPool()
  {
    destroy();
  }

public:
  int init(int tg_id, ObLogCallbackHandler* handler, const common::ObAddr& self_addr);
  void destroy();
  // virtual void *on_begin();
  virtual void handle(void* task);

private:
  // Adaptive thread pool's parameters
  const int64_t LEAST_THREAD_NUM = 8;
  const int64_t ESTIMATE_TS = 80000;
  const int64_t EXPAND_RATE = 70;
  const int64_t SHRINK_RATE = 35;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogCallbackThreadPool);

private:
  bool is_inited_;
  ObLogCallbackHandler* handler_;
  int tg_id_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CALLBACK_THREAD_POOL_
