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

#ifndef OBLIB_THREAD_MGR_H
#define OBLIB_THREAD_MGR_H

#include "lib/ob_define.h"
#include "lib/ob_running_mode.h"
#include "lib/thread/thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/ob_async_task_queue.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/task/ob_timer.h"

namespace oceanbase {
namespace lib {

using common::ObDedupQueue;

class ThreadCountPair {
public:
  ThreadCountPair(int64_t cnt, int64_t mini_mode_cnt) : cnt_(cnt), mini_mode_cnt_(mini_mode_cnt)
  {}
  int64_t get_thread_cnt() const
  {
    return !is_mini_mode() ? cnt_ : mini_mode_cnt_;
  }

private:
  int64_t cnt_;
  int64_t mini_mode_cnt_;
};

namespace TGDefIDs {
enum TGDefIDEnum {
  LIB_START = -1,
#define TG_DEF(id, ...) id,
#include "lib/thread/thread_define.h"
#undef TG_DEF
  LIB_END,
  END = 256,
};
}  // namespace TGDefIDs

enum class TGScope {
  INVALID,
  TG_STATIC,
  TG_DYNAMIC,
};

enum class TGType {
  INVALID,
  THREAD_POOL,
  OB_THREAD_POOL,
  TIMER,
  TIMER_GROUP,
  QUEUE_THREAD,
  DEDUP_QUEUE,
  ASYNC_TASK_QUEUE,
};

class TGCommonAttr {
public:
  const char* name_;
  const char* desc_;
  enum TGScope scope_;
  enum TGType type_;
  TO_STRING_KV(K_(name), K_(desc), K_(scope), K_(type));
};

class ITG;
typedef std::function<ITG*()> CreateFunc;
extern CreateFunc create_funcs_[TGDefIDs::END];
extern void init_create_func();
extern void lib_init_create_func();
extern bool create_func_inited_;

class ITG {
public:
  virtual ~ITG()
  {}
  virtual int64_t thread_cnt() = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;

  virtual int set_runnable(TGRunnable& runnable)
  {
    UNUSED(runnable);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int set_handler(TGTaskHandler& handler)
  {
    UNUSED(handler);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int set_adaptive_strategy(const common::ObAdaptiveStrategy& strategy)
  {
    UNUSED(strategy);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int push_task(void* task)
  {
    UNUSED(task);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int64_t get_queue_num()
  {
    OB_LOG(ERROR, "unexpected invoke");
    return 0;
  }
  virtual int push_task(const common::IObDedupTask& task)
  {
    UNUSED(task);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int push_task(share::ObAsyncTask& task)
  {
    UNUSED(task);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int schedule(common::ObTimerTask& task, const int64_t delay, bool repeate = false)
  {
    UNUSEDx(task, delay, repeate);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int schedule(int idx, common::ObTimerTask& task, const int64_t delay, bool repeate = false)
  {
    UNUSEDx(idx, task, delay, repeate);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int task_exist(const common::ObTimerTask& task, bool& exist)
  {
    UNUSEDx(task, exist);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int cancel(const common::ObTimerTask& task)
  {
    UNUSED(task);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int cancel_all()
  {
    return common::OB_NOT_SUPPORTED;
  }
  TGCommonAttr attr_;
};

template <enum TGType type>
class TG;

class MyThreadPool : public lib::ThreadPool {
public:
  void run1() override
  {
    runnable_->set_thread_idx(get_thread_idx());
    runnable_->run1();
  }
  TGRunnable* runnable_ = nullptr;
};

template <>
class TG<TGType::THREAD_POOL> : public ITG {
public:
  TG(ThreadCountPair pair) : thread_cnt_(pair.get_thread_cnt())
  {}
  ~TG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return thread_cnt_;
  }
  int set_runnable(TGRunnable& runnable)
  {
    int ret = common::OB_SUCCESS;
    if (th_ != nullptr) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      th_ = new (buf_) MyThreadPool();
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
      th_->~MyThreadPool();
      th_ = nullptr;
    }
  }

private:
  char buf_[sizeof(MyThreadPool)];
  MyThreadPool* th_ = nullptr;
  int thread_cnt_;
};

class MySimpleThreadPool : public common::ObSimpleThreadPool {
public:
  void run1() override
  {
    handler_->set_thread_cnt(get_thread_count());
    handler_->set_thread_idx(get_thread_idx());
    common::ObSimpleThreadPool::run1();
  }
  void handle(void* task) override
  {
    handler_->handle(task);
  }
  TGTaskHandler* handler_ = nullptr;
};

template <>
class TG<TGType::QUEUE_THREAD> : public ITG {
public:
  TG(ThreadCountPair pair, const int64_t task_num_limit)
      : thread_num_(pair.get_thread_cnt()), task_num_limit_(task_num_limit)
  {}
  ~TG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return thread_num_;
  }
  int set_handler(TGTaskHandler& handler)
  {
    int ret = common::OB_SUCCESS;
    if (qth_ != nullptr) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      qth_ = new (buf_) MySimpleThreadPool();
      qth_->handler_ = &handler;
      ret = qth_->init(thread_num_, task_num_limit_, attr_.name_);
    }
    return ret;
  }
  int start() override
  {
    int ret = common::OB_SUCCESS;
    return ret;
  }
  void stop() override
  {
    if (qth_ != nullptr) {
      qth_->stop();
    }
  }
  void wait() override
  {
    if (qth_ != nullptr) {
      qth_->wait();
      destroy();
    }
  }
  int64_t get_queue_num() override
  {
    int64_t num = 0;
    if (qth_ != nullptr) {
      num = qth_->get_queue_num();
    }
    return num;
  }
  int push_task(void* task) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(qth_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = qth_->push(task);
    }
    return ret;
  }
  int set_adaptive_strategy(const common::ObAdaptiveStrategy& strategy) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(qth_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = qth_->set_adaptive_strategy(strategy);
    }
    return ret;
  }
  void destroy()
  {
    if (qth_ != nullptr) {
      qth_->destroy();
      qth_->~MySimpleThreadPool();
      qth_ = nullptr;
    }
  }

private:
  char buf_[sizeof(MySimpleThreadPool)];
  MySimpleThreadPool* qth_ = nullptr;
  int64_t thread_num_;
  int64_t task_num_limit_;
};

template <>
class TG<TGType::DEDUP_QUEUE> : public ITG {
public:
  TG(ThreadCountPair pair, const int64_t queue_size = ObDedupQueue::TASK_QUEUE_SIZE,
      const int64_t task_map_size = ObDedupQueue::TASK_MAP_SIZE,
      const int64_t total_mem_limit = ObDedupQueue::TOTAL_LIMIT,
      const int64_t hold_mem_limit = ObDedupQueue::HOLD_LIMIT, const int64_t page_size = ObDedupQueue::PAGE_SIZE,
      const char* label = nullptr)
      : thread_num_(pair.get_thread_cnt()),
        queue_size_(queue_size),
        task_map_size_(task_map_size),
        total_mem_limit_(total_mem_limit),
        hold_mem_limit_(hold_mem_limit),
        page_size_(page_size),
        label_(label)
  {}
  ~TG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return thread_num_;
  }
  int start() override
  {
    int ret = common::OB_SUCCESS;
    if (qth_ != nullptr) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      qth_ = new (buf_) common::ObDedupQueue();
      if (OB_FAIL(qth_->init(
              thread_num_, attr_.name_, queue_size_, task_map_size_, total_mem_limit_, hold_mem_limit_, page_size_))) {
      } else {
        qth_->set_label(label_);
      }
    }
    return ret;
  }
  void stop() override
  {
    if (qth_ != nullptr) {
      qth_->stop();
    }
  }
  void wait() override
  {
    if (qth_ != nullptr) {
      qth_->wait();
      destroy();
    }
  }
  int push_task(const common::IObDedupTask& task) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(qth_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = qth_->add_task(task);
    }
    return ret;
  }
  int64_t get_queue_num() override
  {
    int64_t num = 0;
    if (qth_ != nullptr) {
      num = qth_->task_count();
    }
    return num;
  }
  void destroy()
  {
    if (qth_ != nullptr) {
      qth_->destroy();
      qth_->~ObDedupQueue();
      qth_ = nullptr;
    }
  }

private:
  char buf_[sizeof(common::ObDedupQueue)];
  common::ObDedupQueue* qth_ = nullptr;
  int32_t thread_num_;
  const int64_t queue_size_;
  const int64_t task_map_size_;
  const int64_t total_mem_limit_;
  const int64_t hold_mem_limit_;
  const int64_t page_size_;
  const char* label_;
};

template <>
class TG<TGType::TIMER> : public ITG {
public:
  ~TG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return 1;
  }
  int start() override
  {
    int ret = common::OB_SUCCESS;
    if (timer_ != nullptr) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      timer_ = new (buf_) common::ObTimer();
      if (OB_FAIL(timer_->init(attr_.name_))) {
        OB_LOG(WARN, "init failed", K(ret));
      }
    }
    return ret;
  }
  void stop() override
  {
    if (timer_ != nullptr) {
      timer_->stop();
    }
  }
  void wait() override
  {
    if (timer_ != nullptr) {
      timer_->wait();
      destroy();
    }
  }
  int schedule(common::ObTimerTask& task, const int64_t delay, bool repeate = false) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(timer_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = timer_->schedule(task, delay, repeate);
    }
    return ret;
  }
  int task_exist(const common::ObTimerTask& task, bool& exist) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(timer_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = timer_->task_exist(task, exist);
    }
    return ret;
  }
  int cancel(const common::ObTimerTask& task) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(timer_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = timer_->cancel(task);
    }
    return ret;
  }
  int cancel_all() override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(timer_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      timer_->cancel_all();
    }
    return ret;
  }
  void destroy()
  {
    if (timer_ != nullptr) {
      timer_->destroy();
      timer_->~ObTimer();
      timer_ = nullptr;
    }
  }

private:
  char buf_[sizeof(common::ObTimer)];
  common::ObTimer* timer_ = nullptr;
};

template <>
class TG<TGType::ASYNC_TASK_QUEUE> : public ITG {
public:
  TG(lib::ThreadCountPair pair, const int64_t queue_size) : thread_cnt_(pair.get_thread_cnt()), queue_size_(queue_size)
  {}
  ~TG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return thread_cnt_;
  }
  int start() override
  {
    int ret = common::OB_SUCCESS;
    if (qth_ != nullptr) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      qth_ = new (buf_) share::ObAsyncTaskQueue();
      if (OB_FAIL(qth_->init(thread_cnt_, queue_size_, attr_.name_))) {
      } else {
        ret = qth_->start();
      }
    }
    return ret;
  }
  void stop() override
  {
    if (qth_ != nullptr) {
      qth_->stop();
    }
  }
  void wait() override
  {
    if (qth_ != nullptr) {
      qth_->wait();
      destroy();
    }
  }
  int push_task(share::ObAsyncTask& task) override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(qth_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = qth_->push(task);
    }
    return ret;
  }
  void destroy()
  {
    if (qth_ != nullptr) {
      qth_->destroy();
      qth_->~ObAsyncTaskQueue();
      qth_ = nullptr;
    }
  }

private:
  char buf_[sizeof(share::ObAsyncTaskQueue)];
  share::ObAsyncTaskQueue* qth_ = nullptr;
  int thread_cnt_;
  int64_t queue_size_;
};

template <>
class TG<TGType::TIMER_GROUP> : public ITG {
  static constexpr int MAX_CNT = 32;
  using TimerType = TG<TGType::TIMER>;

public:
  TG(ThreadCountPair pair) : cnt_(pair.get_thread_cnt())
  {}
  ~TG()
  {
    destroy();
  }
  int64_t thread_cnt() override
  {
    return cnt_;
  }
  int start() override
  {
    int ret = common::OB_SUCCESS;
    if (is_inited_) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      if (cnt_ >= MAX_CNT) {
        ret = common::OB_ERR_UNEXPECTED;
      }
      for (int i = 0; OB_SUCC(ret) && i < cnt_; i++) {
        auto* tmp = new (buf_ + sizeof(TimerType) * i) TimerType();
        tmp->attr_ = attr_;
        if (OB_FAIL(tmp->start())) {
        } else {
          timers_[i] = tmp;
        }
      }
      is_inited_ = true;
    }
    return ret;
  }
  void stop() override
  {
    if (is_inited_) {
      for (int i = 0; i < cnt_; i++) {
        if (timers_[i] != nullptr) {
          timers_[i]->stop();
        }
      }
    }
  }
  void wait() override
  {
    if (is_inited_) {
      for (int i = 0; i < cnt_; i++) {
        if (timers_[i] != nullptr) {
          timers_[i]->wait();
        }
      }
      destroy();
    }
  }
  int schedule(int idx, common::ObTimerTask& task, const int64_t delay, bool repeate = false) override
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited_) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (idx >= cnt_) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(timers_[idx])) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = timers_[idx]->schedule(task, delay, repeate);
    }
    return ret;
  }
  void destroy()
  {
    if (is_inited_) {
      for (int i = 0; i < cnt_; i++) {
        if (timers_[i] != nullptr) {
          timers_[i]->~TimerType();
          timers_[i] = nullptr;
        }
      }
      is_inited_ = false;
    }
  }

private:
  char buf_[sizeof(TimerType) * MAX_CNT];
  TimerType* timers_[MAX_CNT] = {nullptr};
  const int cnt_ = 0;
  bool is_inited_ = false;
};

template <enum TGType type>
class TGCLSMap;
#define BIND_TG_CLS(type, CLS_) \
  namespace lib {               \
  template <>                   \
  class TGCLSMap<type> {        \
  public:                       \
    using CLS = CLS_;           \
  };                            \
  }
}  // end of namespace lib

BIND_TG_CLS(TGType::THREAD_POOL, TG<TGType::THREAD_POOL>);
BIND_TG_CLS(TGType::QUEUE_THREAD, TG<TGType::QUEUE_THREAD>);
BIND_TG_CLS(TGType::DEDUP_QUEUE, TG<TGType::DEDUP_QUEUE>);
BIND_TG_CLS(TGType::TIMER, TG<TGType::TIMER>);
BIND_TG_CLS(TGType::TIMER_GROUP, TG<TGType::TIMER_GROUP>);
BIND_TG_CLS(TGType::ASYNC_TASK_QUEUE, TG<TGType::ASYNC_TASK_QUEUE>);

namespace lib {

class TGMgr {
private:
  TGMgr();
  ~TGMgr();

public:
  static TGMgr& instance()
  {
    static TGMgr mgr;
    return mgr;
  }
  int alloc_tg_id();
  void free_tg_id(int tg_id);
  int create_tg(int tg_def_id, int& tg_id)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!create_func_inited_)) {
      init_create_func();
      create_func_inited_ = true;
    }
    ITG* tg = nullptr;
    auto func = create_funcs_[tg_def_id];
    if (!func) {
      // Also need to account for id without do anything
      alloc_tg_id();
    } else if (FALSE_IT(tg_id = alloc_tg_id())) {
    } else if (tg_id < 0) {
      ret = common::OB_SIZE_OVERFLOW;
    } else if (OB_ISNULL(tg = func())) {
      ret = common::OB_INIT_FAIL;
    } else {
      tgs_[tg_id] = tg;
      OB_LOG(INFO, "create tg succeed", "tg_id", tg_id, "tg", tg, "thread_cnt", tg->thread_cnt(), K(tg->attr_));
    }
    return ret;
  }
  void destroy_tg(int tg_id);

private:
  static constexpr int MAX_ID = 1024;
  common::ObLatchMutex lock_;
  ABitSet bs_;
  char bs_buf_[ABitSet::buf_len(MAX_ID)];

public:
  ITG* tgs_[MAX_ID] = {nullptr};
  int default_tg_id_map_[TGDefIDs::END] = {-1};
};

template <typename Func, bool return_void = false>
class FWrap {
public:
  int operator()(Func func)
  {
    return func();
  }
};

template <typename Func>
class FWrap<Func, true> {
public:
  int operator()(Func func)
  {
    func();
    return common::OB_SUCCESS;
  }
};

#define TG_INVOKE(tg_id, func, args...)                                                                               \
  ({                                                                                                                  \
    int ret = common::OB_SUCCESS;                                                                                     \
    if (OB_UNLIKELY(-1 == tg_id)) {                                                                                   \
      ret = common::OB_ERR_UNEXPECTED;                                                                                \
      OB_LOG(ERROR, "invalid tg id");                                                                                 \
    } else {                                                                                                          \
      lib::ITG* tg =                                                                                                  \
          TG_MGR                                                                                                      \
              .tgs_[static_cast<int>(tg_id) < static_cast<int>(lib::TGDefIDs::END) ? TG_MGR.default_tg_id_map_[tg_id] \
                                                                                   : tg_id];                          \
      if (OB_ISNULL(tg)) {                                                                                            \
        ret = common::OB_ERR_UNEXPECTED;                                                                              \
      } else {                                                                                                        \
        auto f = [&]() { return tg->func(args); };                                                                    \
        ret = lib::FWrap<decltype(f), std::is_void<decltype(f())>::value>()(f);                                       \
      }                                                                                                               \
    }                                                                                                                 \
    ret;                                                                                                              \
  })

// tenant scope
#define TG_MGR (lib::TGMgr::instance())
#define TG_CREATE(tg_def_id, tg_id) TG_MGR.create_tg(tg_def_id, tg_id)
#define TG_DESTROY(tg_id) TG_MGR.destroy_tg(tg_id)
#define TG_SET_RUNNABLE(tg_id, args...) TG_INVOKE(tg_id, set_runnable, args)
#define TG_SET_HANDLER(tg_id, args...) TG_INVOKE(tg_id, set_handler, args)
#define TG_START(tg_id)                             \
  ({                                                \
    const char* tg_name = nullptr;                  \
    if (TG_MGR.tgs_[tg_id] != nullptr) {            \
      tg_name = TG_MGR.tgs_[tg_id]->attr_.name_;    \
    }                                               \
    OB_LOG(INFO, "start tg", K(tg_id), K(tg_name)); \
    int ret = TG_INVOKE(tg_id, start);              \
    ret;                                            \
  })
#define TG_SET_ADAPTIVE_STRATEGY(tg_id, args...) TG_INVOKE(tg_id, set_adaptive_strategy, args)
#define TG_PUSH_TASK(tg_id, args...) TG_INVOKE(tg_id, push_task, args)
#define TG_GET_QUEUE_NUM(tg_id) TG_INVOKE(tg_id, get_queue_num)
#define TG_WAIT_R(tg_id) TG_INVOKE(tg_id, wait)
#define TG_WAIT(tg_id)              \
  do {                              \
    int r = TG_INVOKE(tg_id, wait); \
    UNUSED(r);                      \
  } while (0)
#define TG_STOP_R(tg_id) TG_INVOKE(tg_id, stop)
#define TG_STOP(tg_id)              \
  do {                              \
    int r = TG_INVOKE(tg_id, stop); \
    UNUSED(r);                      \
  } while (0)
#define TG_CANCEL_R(tg_id, args...) TG_INVOKE(tg_id, cancel, args)
#define TG_CANCEL(tg_id, args...)           \
  do {                                      \
    int r = TG_INVOKE(tg_id, cancel, args); \
    UNUSED(r);                              \
  } while (0)
#define TG_CANCEL_ALL(tg_id) TG_INVOKE(tg_id, cancel_all)
#define TG_TASK_EXIST(tg_id, args...) TG_INVOKE(tg_id, task_exist, args)
#define TG_SCHEDULE(tg_id, args...) TG_INVOKE(tg_id, schedule, args)
#define TG_EXIST(tg_id) (TG_MGR.tgs_[tg_id] != nullptr)
#define TG_SET_RUNNABLE_AND_START(tg_id, args...) \
  ({                                              \
    int ret = OB_SUCCESS;                         \
    ret = TG_SET_RUNNABLE(tg_id, args);           \
    if (OB_SUCC(ret)) {                           \
      ret = TG_START(tg_id);                      \
    }                                             \
    ret;                                          \
  })
#define TG_SET_HANDLER_AND_START(tg_id, args...) \
  ({                                             \
    int ret = OB_SUCCESS;                        \
    ret = TG_SET_HANDLER(tg_id, args);           \
    if (OB_SUCC(ret)) {                          \
      ret = TG_START(tg_id);                     \
    }                                            \
    ret;                                         \
  })

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* OBLIB_THREAD_MGR_H */
