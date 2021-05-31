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

#include "co_set_sched.h"
#include "lib/coro/co_local_storage.h"
#include "lib/coro/routine.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

CoSetSched::CoSetSched(PrepFuncT prep, PostFuncT post) : sets_(), max_setid_(), prep_(prep), post_(post), cs_()
{}

// Simple implementation without priority control.
CoRoutine* CoSetSched::get_next_runnable(int64_t& waittime)
{
  CoRoutine* w = CoBaseSched::get_next_runnable(waittime);
  if (nullptr == w) {
    int64_t waittime0 = 0;
    for (int i = 0; nullptr == w && i < max_setid_; i++) {
      if (sets_[i] != nullptr) {
        w = sets_[i]->get_next_runnable(waittime0);
        waittime = std::min(waittime, waittime0);
      }
    }
  }
  if (w != nullptr) {
    cs_++;
  }
  return w;
}

void CoSetSched::add_runnable(CoBaseSched::Worker& w)
{
  Worker& worker = static_cast<Worker&>(w);
  sets_[worker.get_setid()]->add_worker(worker);
}

int CoSetSched::prepare()
{
  // 0 is the default setid.
  int ret = OB_SUCCESS;
  if (OB_SUCC(create_set(0)) && prep_ != nullptr) {
    ret = prep_();
  }
  return ret;
}

void CoSetSched::postrun()
{
  // remove the remover created in prepare stage.
  remove_finished_workers();

  for (int i = 0; i < max_setid_; i++) {
    if (sets_[i] != nullptr) {
      __libc_free(sets_[i]);
    }
  }

  if (post_) {
    post_();
  }

  // call thread exiting callbacks.
  const int num = ATOMIC_LOAD(&exit_cb_num_);
  for (int i = num - 1; i >= 0; --i) {
    exit_cbs_[i]();
  }
}

void* CoSetSched::allocator_alloc(ObIAllocator& allocator, size_t size)
{
  return allocator.alloc(size);
}

CoSetSched::Worker::Worker(CoSetSched& sched, FuncT func, ObIAllocator* allocator)
    : CoBaseSched::Worker(sched), setid_(0), owned_(false), func_(func), allocator_(allocator), sched_(sched)
{}

void CoSetSched::Worker::run()
{
  if (func_) {
    func_();
  }
  // remove other finished workers, it won't destory self.
  sched_.remove_finished_workers();
}

CoSetSched::Set::Set() : setid_(-1), runnables_()
{}

void CoSetSched::Set::add_worker(Worker& worker)
{
  worker.add_to_runnable(runnables_);
}

CoSetSched::Worker* CoSetSched::Set::get_next_runnable(int64_t& waittime)
{
  Worker* w = nullptr;
  LinkedNode* node = runnables_.remove_first();
  if (nullptr == node) {
    waittime = INT64_MAX;
  } else {
    w = static_cast<Worker*>(node->get_data());
  }
  return w;
}
