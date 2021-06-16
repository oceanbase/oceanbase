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

#ifndef OB_THREAD_MGR_H_
#define OB_THREAD_MGR_H_

#include "lib/thread/thread_mgr.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace lib {
namespace TGDefIDs {
enum OBTGDefIDEnum {
  OB_START = TGDefIDs::LIB_END - 1,
#define TG_DEF(id, ...) id,
#include "share/ob_thread_define.h"
#undef TG_DEF
  OB_END,
};
}  // namespace TGDefIDs
}  // end of namespace lib

namespace share {
using lib::ITG;
using lib::TGType;
template <enum TGType type>
class ObTG;

class MyObThreadPool : public share::ObThreadPool {
public:
  void run1() override
  {
    runnable_->set_thread_idx(get_thread_idx());
    runnable_->run1();
  }
  lib::TGRunnable* runnable_ = nullptr;
};

template <>
class ObTG<TGType::OB_THREAD_POOL> : public ITG {
public:
  ObTG(lib::ThreadCountPair pair) : thread_cnt_(pair.get_thread_cnt())
  {}
  ~ObTG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return thread_cnt_;
  }
  int set_runnable(lib::TGRunnable& runnable) override
  {
    int ret = common::OB_SUCCESS;
    if (th_ != nullptr) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      th_ = new (buf_) MyObThreadPool();
      th_->runnable_ = &runnable;
    }
    return ret;
  }
  int start() override
  {
    int ret = common::OB_SUCCESS;
    if (nullptr == th_) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (nullptr == th_->runnable_) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      th_->runnable_->set_stop(false);
      th_->set_thread_count(thread_cnt_);
      ret = th_->start();
    }
    return ret;
  }
  void stop() override
  {
    if (th_ != nullptr) {
      th_->runnable_->set_stop(true);
      th_->stop();
    }
  }
  void wait() override
  {
    if (th_ != nullptr) {
      th_->wait();
      destroy();
    }
  }
  void destroy()
  {
    if (th_ != nullptr) {
      th_->destroy();
      th_->~MyObThreadPool();
      th_ = nullptr;
    }
  }

private:
  char buf_[sizeof(MyObThreadPool)];
  MyObThreadPool* th_ = nullptr;
  int thread_cnt_;
};

}  // end of namespace share

BIND_TG_CLS(lib::TGType::OB_THREAD_POOL, share::ObTG<lib::TGType::OB_THREAD_POOL>);

}  // end of namespace oceanbase

#endif  // OB_THREAD_MGR_H_
