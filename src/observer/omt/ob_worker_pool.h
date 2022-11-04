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
#include "lib/queue/ob_link_queue.h"
#include "observer/ob_srv_xlator.h"

namespace oceanbase
{

namespace rpc
{
namespace frame
{
class ObReqTranslator;
}
}

namespace omt
{

class ObThWorker;

// This class isn't thread safe.
class ObWorkerPool
{
public:
  class Queue
  {
  public:
    Queue() {}
    virtual ~Queue() {}
    int push(ObThWorker *worker);
    int pop(ObThWorker *&worker);
  private:
    common::ObLinkQueue queue_;
  };

public:
  explicit ObWorkerPool();
  virtual ~ObWorkerPool();

  int init(int64_t init_cnt, int64_t idle_cnt);
  void destroy();

  ObThWorker *alloc();
  void free(ObThWorker *worker);

private:
  int create_worker(ObThWorker *&worker);
  void destroy_worker(ObThWorker *worker);

private:
  bool is_inited_;
  int64_t init_cnt_;
  int64_t idle_cnt_;
  int64_t worker_cnt_;
  Queue workers_;
}; // end of class ObWorkerPool

} // end of namespace omt
} // end of namespace oceanbase


#endif /* _OCEABASE_OBSERVER_OMT_OB_WORKER_POOL_H_ */
