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

#include "lib/queue/ob_lighty_queue.h"
#include "lib/thread/thread_pool.h"

using oceanbase::common::LightyQueue;
using oceanbase::lib::ThreadPool;

class Productor : public ThreadPool {
public:
  Productor(LightyQueue& q, int th_cnt) : q_(q)
  {
    set_thread_count(th_cnt);
  }

private:
  void run1() override;

private:
  LightyQueue& q_;
};  // end of class Productor

class Consumer : public ThreadPool {
public:
  Consumer(LightyQueue& q, int th_cnt) : q_(q), cnt_(0)
  {
    set_thread_count(th_cnt);
  }

  uint64_t get_cnt() const
  {
    return cnt_;
  }

private:
  void run1() override;

private:
  LightyQueue& q_;
  uint64_t cnt_;
};  // end of class Consumer
