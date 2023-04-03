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

#ifndef OCEANBASE_TRANSACTION_OB_TS_WORKER_
#define OCEANBASE_TRANSACTION_OB_TS_WORKER_

#include "lib/utility/utility.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace transaction
{
class ObTsResponseTask;
class ObTsMgr;
class ObTsWorker : public lib::TGTaskHandler
{
public:
  ObTsWorker() : is_inited_(false), use_local_worker_(false), ts_mgr_(NULL), tg_id_(-1) {}
  ~ObTsWorker() {}
  int init(ObTsMgr *ts_mgr, const bool use_local_worker = false);
  void stop();
  void wait();
  void destroy();
public:
  int push_task(const uint64_t tenant_id, ObTsResponseTask *task);
  void handle(void *task);
public:
  static const int64_t MAX_TASK_NUM = 10240;
private:
  bool is_inited_;
  bool use_local_worker_;
  ObTsMgr *ts_mgr_;
  int tg_id_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TS_WORKER_
