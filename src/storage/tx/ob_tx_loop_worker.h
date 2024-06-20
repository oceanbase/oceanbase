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

#ifndef OCEANBASE_TRANSACTION_KEEP_ALIVE_SERVICE_
#define OCEANBASE_TRANSACTION_KEEP_ALIVE_SERVICE_

#include "lib/thread/thread_pool.h"

#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase {

namespace share
{
class ObLSID;
};

namespace storage
{
class ObLS;
}

namespace transaction 
{

class ObTxLoopWorker : public lib::ThreadPool
{
public:
  // keep alive
  const static int64_t LOOP_INTERVAL = 100 * 1000;                            // 100ms
  const static int64_t KEEP_ALIVE_PRINT_INFO_INTERVAL = 5 * 60 * 1000 * 1000; // 5min
  const static int64_t TX_GC_INTERVAL = 5 * 1000 * 1000;                     // 5s
  const static int64_t TX_RETAIN_CTX_GC_INTERVAL = 5 * 1000 * 1000;           // 5s
  const static int64_t TX_START_WORKING_RETRY_INTERVAL = 5 * 1000 * 1000;  //5s
public:
  ObTxLoopWorker() { reset(); }
  ~ObTxLoopWorker() {}
  static int mtl_init(ObTxLoopWorker *&ka);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  void reset();

  virtual void run1();

private:
  int scan_all_ls_(bool can_tx_gc, bool can_gc_retain_ctx, bool can_check_and_retry_start_working);
  void do_keep_alive_(ObLS *ls, const share::SCN &min_start_scn, MinStartScnStatus status); // 100ms
  void do_tx_gc_(ObLS *ls, share::SCN &min_start_scn, MinStartScnStatus &status);     // 15s
  void update_max_commit_ts_();
  void do_retain_ctx_gc_(ObLS * ls);  // 15s
  void do_start_working_retry_(ObLS * ls);

private:
  int64_t last_tx_gc_ts_;
  int64_t last_retain_ctx_gc_ts_;
  int64_t last_check_start_working_retry_ts_;
};


} // namespace transaction
} // namespace oceanbase

#endif
