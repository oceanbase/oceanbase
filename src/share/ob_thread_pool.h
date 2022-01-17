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

#ifndef OB_THREAD_POOL_H
#define OB_THREAD_POOL_H

#include "lib/coro/co_user_thread.h"
#include "lib/thread/thread_pool.h"
#include "share/ob_worker.h"

namespace oceanbase {
namespace share {

class ObThreadPool : public lib::ThreadPool {
public:
  void run0() override
  {
    // Create worker for current thread.
    ObWorker worker;
    run1();
  }
};

}  // namespace share
}  // namespace oceanbase

#endif /* OB_THREAD_POOL_H */
