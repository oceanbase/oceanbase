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

#ifndef _OCEABASE_OBSERVER_OMT_OB_WORKER_POOL_H_
#define _OCEABASE_OBSERVER_OMT_OB_WORKER_POOL_H_

#include <stdint.h>
#include "lib/queue/ob_fixed_queue.h"
#include "observer/ob_srv_xlator.h"

namespace oceanbase {

namespace rpc {
namespace frame {
class ObReqTranslator;
}
}  // namespace rpc

namespace omt {

class ObThWorker;
class ObIWorkerProcessor;

// This class isn't thread safe.
class ObWorkerPool {
  static const int64_t MAX_WORKER_CNT = 10240;
  typedef common::ObFixedQueue<ObThWorker> WorkerArray;

public:
  explicit ObWorkerPool(ObIWorkerProcessor& procor);
  virtual ~ObWorkerPool();

  int init(int64_t init_cnt, int64_t idle_cnt, int64_t max_cnt);
  void destroy();

  ObThWorker* alloc();
  void free(ObThWorker* worker);

  void set_max(int64_t v);

private:
  int create_worker(ObThWorker*& worker);
  void destroy_worker(ObThWorker* worker);

private:
  bool is_inited_;
  int64_t init_cnt_;
  int64_t idle_cnt_;
  int64_t max_cnt_;
  int64_t worker_cnt_;
  WorkerArray workers_;
  ObIWorkerProcessor& procor_;
};  // end of class ObWorkerPool

}  // end of namespace omt
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_WORKER_POOL_H_ */
