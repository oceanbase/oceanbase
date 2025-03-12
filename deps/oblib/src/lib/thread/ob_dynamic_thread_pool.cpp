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

#include "ob_dynamic_thread_pool.h"
#include "lib/thread/thread_mgr.h"
#include "lib/allocator/ob_sql_mem_leak_checker.h"

extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
}

namespace oceanbase
{
namespace common
{

ObDynamicThreadInfo::ObDynamicThreadInfo()
  : tid_(nullptr),
    idx_(-1),
    pool_(NULL),
    is_stop_(false),
    is_alive_(false),
    error_thread_(false)
{
}



ObDynamicThreadPool::ObDynamicThreadPool()
  : is_inited_(false),
    is_stop_(false),
    thread_num_(0),
    task_queue_(),
    cond_(),
    need_idle_(true),
    start_thread_num_(0),
    stop_thread_num_(0),
    thread_name_(nullptr)
{
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    thread_infos_[i].idx_ = i;
    thread_infos_[i].pool_ = this;
  }
}

ObDynamicThreadPool::~ObDynamicThreadPool()
{
  if (!is_stop_) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ObDynamicThreadPool is destruction before stop");
    is_stop_ = true;
  }
  destroy();
}

int ObDynamicThreadPool::init(const char* thread_name)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(task_queue_.init(MAX_TASK_NUM))) {
    COMMON_LOG(WARN, "failed to init task queue", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DYNAMIC_THREAD_POOL_COND_WAIT))) {
    STORAGE_LOG(WARN, "failed to init cond", K(ret));
  } else if (OB_FAIL(task_thread_cond_.init(ObWaitEventIds::DYNAMIC_THREAD_POOL_COND_WAIT))) {
    STORAGE_LOG(WARN, "failed to init cond", K(ret));
  }

  if (OB_SUCC(ret)) {
    thread_name_ = thread_name;
    is_inited_ = true;
    is_stop_ = false;
    ATOMIC_STORE(&thread_num_ , 0);

    if (OB_FAIL(start())) {
      COMMON_LOG(WARN, "failed to start dynamic thread pool", K(ret));
      is_stop_ = true;
      is_inited_ = false;
    } else {
      COMMON_LOG(INFO, "succeed to init dynamic thread pool", K(ret));
    }
  }
  return ret;
}

int ObDynamicThreadPool::set_task_thread_num(const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  const int64_t cur_thread_num = ATOMIC_LOAD(&thread_num_);

  if (thread_num < 0 || thread_num > MAX_THREAD_NUM) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid thread num", K(ret), K(thread_num));
  } else if (cur_thread_num != thread_num) {
    COMMON_LOG(INFO, "set dynamic thread pool thread num", K(cur_thread_num), K(thread_num));
    ATOMIC_STORE(&thread_num_ , thread_num);
    wakeup();
  }
  return ret;
}

void ObDynamicThreadPool::destroy()
{
  is_stop_ = true;
  if (is_inited_) {
    stop();
    wait();
    is_inited_ = false;
    COMMON_LOG(INFO, "thread pool is destroyed", K(*this));
  }
}

void ObDynamicThreadPool::stop()
{
  is_stop_ = true;
  wakeup();
}

void ObDynamicThreadPool::wakeup()
{
  ObThreadCondGuard guard(cond_);
  need_idle_ = false;
  cond_.broadcast();
}

void ObDynamicThreadPool::task_thread_idle()
{
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(task_thread_cond_);
  if (OB_SUCCESS != (tmp_ret = task_thread_cond_.wait(DEFAULT_CHECK_TIME_MS))) {
    if (OB_TIMEOUT != tmp_ret) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to idle", K(tmp_ret));
    }
  }
}

void ObDynamicThreadPool::run1()
{
  int tmp_ret = OB_SUCCESS;
  const uint64_t idx = get_thread_idx();
  if (OB_NOT_NULL(thread_name_)) {
    lib::set_thread_name(thread_name_, idx);
  }
  while (!is_stop_) {
    if (OB_SUCCESS != (tmp_ret = check_thread_status())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to check_thread_status", K(tmp_ret));
    }
    ObThreadCondGuard guard(cond_);
    if (need_idle_) {
      if (OB_SUCCESS != (tmp_ret = cond_.wait(DEFAULT_CHECK_TIME_MS))) {
        if (OB_TIMEOUT != tmp_ret) {
          STORAGE_LOG_RET(WARN, tmp_ret, "failed to idle", K(tmp_ret));
        }
      }
    }
    need_idle_ = true;
  }
  if (OB_SUCCESS != (tmp_ret = stop_all_threads())) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to stop all threads", K(tmp_ret));
  }
}

int ObDynamicThreadPool::check_thread_status()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t thread_num = ATOMIC_LOAD(&thread_num_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    ObDynamicThreadInfo &thread_info = thread_infos_[i];
    if (i < thread_num) {
      if (!thread_info.is_alive_) {
        if (OB_SUCCESS != (tmp_ret = start_thread(thread_info))) {
          COMMON_LOG(WARN, "failed to start thread", K(tmp_ret));
        }
      }
    } else {
      if (thread_info.is_alive_) {
        if (OB_SUCCESS != (tmp_ret = stop_thread(thread_info))) {
          COMMON_LOG(WARN, "failed to start thread", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObDynamicThreadPool::stop_all_threads()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();

  COMMON_LOG(INFO, "start stop all thread", KP(this));
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    ObDynamicThreadInfo &thread_info = thread_infos_[i];
    thread_info.is_stop_ = true;
  }
  task_thread_cond_.broadcast();

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    ObDynamicThreadInfo &thread_info = thread_infos_[i];
    if (thread_info.is_alive_) {
      if (OB_SUCCESS != (tmp_ret = stop_thread(thread_info))) {
        COMMON_LOG(WARN, "failed to start thread", K(tmp_ret));
        ret = OB_SUCC(ret)? tmp_ret : ret;
      }
    }
  }

  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;

  COMMON_LOG(INFO, "finish stop all thread", KP(this), K(cost_ts));
  return ret;
}

int ObDynamicThreadPool::start_thread(ObDynamicThreadInfo &thread_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (thread_info.is_alive_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "thread is alive, cannot start twice", K(ret), K(thread_info));
  } else if (thread_info.error_thread_) {
    ret = OB_ERR_SYS;
    COMMON_LOG(ERROR, "this thread failed during startup, cannot start again", K(ret), K(thread_info));
  } else {
    thread_info.is_stop_ = false;
    if (OB_FAIL(ob_pthread_create(&thread_info.tid_, task_thread_func, &thread_info))) {
      thread_info.error_thread_ = true;
      COMMON_LOG(ERROR, "failed to create thread", K(ret), K(thread_info));
    } else {
      thread_info.is_alive_ = true;
      ++start_thread_num_;
      COMMON_LOG(INFO, "succeed to start thread", K(thread_info));
    }
  }

  return ret;
}

int ObDynamicThreadPool::stop_thread(ObDynamicThreadInfo &thread_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!thread_info.is_alive_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "thread is not alive, cannot stop", K(ret), K(thread_info));
  } else {
    thread_info.is_stop_ = true;
    ob_pthread_join(thread_info.tid_);
    thread_info.is_alive_ = false;
    ++stop_thread_num_;
    COMMON_LOG(INFO, "succeed to stop thread", K(thread_info));
  }

  return ret;
}

int ObDynamicThreadPool::pop_task(ObDynamicThreadTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (is_stop_) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(task_queue_.pop(task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to pop task", K(ret));
    }
  }

  return ret;
}

int ObDynamicThreadPool::add_task(ObDynamicThreadTask *task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (is_stop_) {
    ret = OB_IN_STOP_STATE;
    COMMON_LOG(WARN, "thread pool is stopped", K(ret));
  } else if (OB_FAIL(task_queue_.push(task))) {
    COMMON_LOG(WARN, "failed to push task", K(ret));
  } else {
    ObThreadCondGuard guard(task_thread_cond_);
    task_thread_cond_.signal();
  }
  return ret;
}

void *ObDynamicThreadPool::task_thread_func(void *data)
{
  int tmp_ret = OB_SUCCESS;

  if (NULL == data) {
    tmp_ret = OB_ERR_SYS;
    COMMON_LOG_RET(ERROR, tmp_ret, "data must not null", K(tmp_ret), K(data));
  } else {
    ObDynamicThreadInfo *thread_info = reinterpret_cast<ObDynamicThreadInfo *>(data);
    ObDynamicThreadTask *task = NULL;

    while (!thread_info->is_stop_) {
      task = NULL;
      if (NULL != thread_info->pool_) {
        if (OB_SUCCESS != (tmp_ret = thread_info->pool_->pop_task(task))) {
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            thread_info->pool_->task_thread_idle();
          } else if (OB_IN_STOP_STATE == tmp_ret) {
            break;
          } else {
            COMMON_LOG_RET(WARN, tmp_ret, "failed to pop task", K(tmp_ret));
          }
        } else if (OB_SUCCESS != (tmp_ret = task->process(thread_info->is_stop_))) {
          COMMON_LOG_RET(WARN, tmp_ret, "failed to process task", K(tmp_ret), K(*thread_info));
        }
      }
    }
    COMMON_LOG(INFO, "task thread exits", K(*thread_info));
  }
  return NULL;
}

int ObSimpleDynamicThreadPool::init(const int64_t thread_num, const char* name, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  name_ = name;
  max_thread_cnt_ = thread_num;
  tenant_id_ = tenant_id;
  if (min_thread_cnt_ < 0) {
    // has not set adaptive thread
    min_thread_cnt_ = thread_num;
  }
  ret = set_thread_count(thread_num);
  return ret;
}

void ObSimpleDynamicThreadPool::stop()
{
  IGNORE_RETURN ObSimpleThreadPoolDynamicMgr::get_instance().unbind(this);
  lib::ThreadPool::stop();
}

void ObSimpleDynamicThreadPool::destroy()
{
  if (min_thread_cnt_ < max_thread_cnt_) {
    IGNORE_RETURN ObSimpleThreadPoolDynamicMgr::get_instance().unbind(this);
  }
  int64_t ref_cnt = 0;
  while ((ref_cnt = get_ref_cnt()) > 0) {
    if (REACH_TIME_INTERVAL(1000L * 1000L)) {
      COMMON_LOG(INFO, "wait tenant io manager quit", K(*this), K(ref_cnt));
    }
    ob_usleep((useconds_t)10L * 1000L);
  }
  lib::ThreadPool::stop();
  lib::ThreadPool::wait();
}

ObSimpleDynamicThreadPool::~ObSimpleDynamicThreadPool()
{
  destroy();
}

int ObSimpleDynamicThreadPool::set_adaptive_thread(int64_t min_thread_num, int64_t max_thread_num)
{
  int ret = OB_SUCCESS;
  if (min_thread_num < 0 || max_thread_num <= 0
      || min_thread_num > MAX_THREAD_NUM || max_thread_num > MAX_THREAD_NUM
      || min_thread_num > max_thread_num) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "set_adaptive_thread failed", KP(this), K(min_thread_num), K(max_thread_num));
  } else if (OB_FAIL((ObSimpleThreadPoolDynamicMgr::get_instance().bind(this)))) {
    COMMON_LOG(WARN, "bind dynamic mgr failed");
  } else {
    min_thread_cnt_ = min_thread_num;
    max_thread_cnt_ = max_thread_num;
    COMMON_LOG(INFO, "set adaptive thread success", KP(this), K(min_thread_num), K(max_thread_num));
  }
  return ret;
}

int ObSimpleDynamicThreadPool::set_max_thread_count(int64_t max_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (max_thread_cnt > MAX_THREAD_NUM || max_thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "set_adaptive_thread failed", KP(this), K(max_thread_cnt));
  } else {
    update_threads_lock_.lock();
    bool has_set_adaptive_thread = (max_thread_cnt_ > min_thread_cnt_);
    bool has_set_adaptive_thread_new = (max_thread_cnt > min_thread_cnt_);
    if (!has_set_adaptive_thread_new && OB_FAIL(lib::ThreadPool::set_thread_count(max_thread_cnt))) {
      COMMON_LOG(ERROR, "set thread cnt failed", KP(this), K(max_thread_cnt));
    }
    if (OB_SUCC(ret)) {
      max_thread_cnt_ = max_thread_cnt;
      COMMON_LOG(INFO, "set max thread count", K(*this), K(max_thread_cnt));
    }
    update_threads_lock_.unlock();
    if (has_set_adaptive_thread != has_set_adaptive_thread_new) {
      if (has_set_adaptive_thread_new
           && OB_FAIL(ObSimpleThreadPoolDynamicMgr::get_instance().bind(this))) {
        COMMON_LOG(WARN, "bind thread pool to dynamic mgr failed", KP(this));
      } else if (!has_set_adaptive_thread_new
                 && OB_FAIL(ObSimpleThreadPoolDynamicMgr::get_instance().unbind(this))) {
        COMMON_LOG(WARN, "unbind thread pool to dynamic mgr failed", KP(this));
      }
    }
  }
  return ret;
}

int ObSimpleDynamicThreadPool::set_thread_count_and_try_recycle(int64_t cnt)
{
  int ret = OB_SUCCESS;
  ret = Threads::do_set_thread_count(cnt, true/*async_recycle*/);
  if (OB_SUCC(ret)) {
    ret = Threads::try_thread_recycle();
  }
  return ret;
}

void ObSimpleDynamicThreadPool::try_expand_thread_count()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard time_guard("expand_thread", 20000);
  int64_t queue_size = 0;
  if (min_thread_cnt_ >= max_thread_cnt_) {
    // not set adaptive thread, do nothing
  } else if ((queue_size = get_queue_num()) <= 0) {
    // unneed to expand thread count
  } else if (OB_SUCC(update_threads_lock_.trylock())) {
    int64_t cur_thread_count = get_thread_count();
    int inc_cnt = 0;
    if (cur_thread_count > 0) {
      inc_cnt = queue_size / cur_thread_count;
    } else {
      inc_cnt = queue_size;
    }
    inc_cnt = min(inc_cnt, max_thread_cnt_ - cur_thread_count);
    if (inc_cnt > 0) {
      DISABLE_SQL_MEMLEAK_GUARD;
      COMMON_LOG(INFO, "expand thread count", KP(this), K_(max_thread_cnt), K(cur_thread_count), K(inc_cnt), K(queue_size));
      if (is_server_tenant(tenant_id_)) {
        // temporarily reset ob_get_tenant_id() and run_wrapper
        // avoid the newly created thread to use the memory or run_wrapper of user tenant
        lib::IRunWrapper *run_wrapper = lib::Threads::get_expect_run_wrapper();
        lib::Threads::get_expect_run_wrapper() = NULL;
        DEFER(lib::Threads::get_expect_run_wrapper() = run_wrapper);
        ObResetThreadTenantIdGuard guard;
        ret = set_thread_count_and_try_recycle(cur_thread_count + inc_cnt);
      } else {
        ret = set_thread_count_and_try_recycle(cur_thread_count + inc_cnt);
      }
      if (OB_FAIL(ret)) {
        COMMON_LOG(ERROR, "set thread count failed", KP(this), K(cur_thread_count), K(inc_cnt));
      }
    }
    update_threads_lock_.unlock();
  }
}

void ObSimpleDynamicThreadPool::try_inc_thread_count(int64_t cnt)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard time_guard("inc_thread", 20000);
  if (min_thread_cnt_ >= max_thread_cnt_) {
    // not set adaptive thread, do nothing
  } else if (OB_SUCC(update_threads_lock_.trylock())) {
    int64_t cur_thread_count = get_thread_count();
    int64_t new_thread_count = cur_thread_count + cnt;
    new_thread_count = max(new_thread_count, min_thread_cnt_);
    new_thread_count = min(new_thread_count, max_thread_cnt_);
    COMMON_LOG(INFO, "try inc thread count", K(*this), K(cur_thread_count), K(cnt), K(new_thread_count));
    if (new_thread_count != cur_thread_count) {
      if (OB_FAIL(set_thread_count_and_try_recycle(new_thread_count))) {
        COMMON_LOG(ERROR, "set thread count failed", K(*this), K(cur_thread_count), K(cnt), K(new_thread_count));
      } else {
        COMMON_LOG(INFO, "inc thread count", K(*this), K(cur_thread_count), K(cnt), K(new_thread_count));
      }
    }
    update_threads_lock_.unlock();
  }
}

int ObSimpleDynamicThreadPool::set_thread_count(int64_t n_threads)
{
  int ret = OB_SUCCESS;
  if (min_thread_cnt_ < max_thread_cnt_) {
    ret = lib::ThreadPool::set_thread_count(min_thread_cnt_);
  } else {
    ret = lib::ThreadPool::set_thread_count(n_threads);
  }
  return ret;
}

ObSimpleThreadPoolDynamicMgr &ObSimpleThreadPoolDynamicMgr::get_instance()
{
  static ObSimpleThreadPoolDynamicMgr instance;
  return instance;
}
int ObSimpleThreadPoolDynamicMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::QUEUE_THREAD_MANAGER, *this))) {
    COMMON_LOG(WARN, "start thread failed");
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSimpleThreadPoolDynamicMgr::stop()
{
  if (is_inited_) {
    TG_STOP(lib::TGDefIDs::QUEUE_THREAD_MANAGER);
  }
}

void ObSimpleThreadPoolDynamicMgr::wait()
{
  if (is_inited_) {
    TG_WAIT(lib::TGDefIDs::QUEUE_THREAD_MANAGER);
  }
}

void ObSimpleThreadPoolDynamicMgr::destroy()
{
  if (is_inited_) {
    TG_DESTROY(lib::TGDefIDs::QUEUE_THREAD_MANAGER);
    is_inited_ = false;
  }
}

ObSimpleThreadPoolDynamicMgr::~ObSimpleThreadPoolDynamicMgr()
{
  destroy();
}

void ObSimpleThreadPoolDynamicMgr::run1()
{
  lib::set_thread_name("qth_mgr");
  int64_t last_access_ts = 0;
  int64_t last_idle_ts = 0;
  uint64_t loop_cnt = 0;
  while (!has_set_stop()) {
    // check global simple thread pool
    ObArray<ObSimpleDynamicThreadPool *> dec_pools;
    {
      SpinRLockGuard guard(simple_thread_pool_list_lock_);
      for (int i = 0; i < simple_thread_pool_list_.count(); i++) {
        ObSimpleThreadPoolStat &pool_stat = simple_thread_pool_list_.at(i);
        ObSimpleDynamicThreadPool *pool = pool_stat.pool_;
        int64_t current_time = ObTimeUtility::current_time();
        if (OB_LIKELY(pool_stat.is_valid())) {
          int64_t interval = current_time - pool_stat.last_check_time_;
          int64_t idle = pool->get_threads_idle_time() - pool_stat.last_idle_time_;
          if (idle > interval
              && current_time - pool_stat.last_shrink_time_ > SHRINK_INTERVAL_US
              && pool->get_thread_count() == pool_stat.last_thread_count_
              && pool_stat.last_thread_count_ > pool->get_min_thread_cnt()) {
            int64_t queue_size = pool->get_queue_num();
            int tmp_ret = OB_SUCCESS;
            pool_stat.last_shrink_time_ = current_time;
            if (OB_TMP_FAIL(dec_pools.push_back(pool))) {
              COMMON_LOG_RET(ERROR, tmp_ret, "push dec pool failed", KP(pool));
            } else {
              pool->inc_ref();
            }
            current_time = ObTimeUtility::current_time();
          }
        }
        pool_stat.last_check_time_ = current_time;
        pool_stat.last_idle_time_ = pool->get_threads_idle_time();
        pool_stat.last_thread_count_ = pool->get_thread_count();
        pool->try_expand_thread_count();
        if (OB_UNLIKELY(loop_cnt % 100 == 0)) { // print each 20s
          int64_t qsize = pool->get_queue_num();
          int64_t current_idle_time = pool->get_threads_idle_time();
          SHARE_LOG(INFO, "[thread pool stat]", K(pool_stat), K(*pool), K(qsize), K(current_idle_time));
        }
      }
    }
    for (int i = 0; i < dec_pools.count(); i++) {
      dec_pools[i]->try_inc_thread_count(-1);
      dec_pools[i]->dec_ref();
    }
    ++loop_cnt;
    ob_usleep(CHECK_INTERVAL_US);
  }
}

int ObSimpleThreadPoolDynamicMgr::bind(ObSimpleDynamicThreadPool *pool)
{
  int ret = OB_SUCCESS;
  ObSimpleThreadPoolStat pool_stat(pool);

  SpinWLockGuard guard(simple_thread_pool_list_lock_);
  if (OB_FAIL(simple_thread_pool_list_.push_back(pool_stat))) {
    COMMON_LOG(WARN, "bind simple thread pool faild", KP(pool));
  } else {
    pool->has_bind_ = true;
    COMMON_LOG(INFO, "bind simple thread pool success", K(*pool));
  }
  return ret;
}

int ObSimpleThreadPoolDynamicMgr::unbind(ObSimpleDynamicThreadPool *pool)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
     COMMON_LOG(WARN, "unbind pool failed");
  } else if (!pool->has_bind_) {
    // do-nothing
  } else {
    SpinWLockGuard guard(simple_thread_pool_list_lock_);
    int64_t idx = -1;
    for (int64_t i = 0; (i < simple_thread_pool_list_.count()) && (-1 == idx); ++i) {
      ObSimpleThreadPoolStat &pool_stat = simple_thread_pool_list_.at(i);
      if (pool_stat.pool_ == pool) {
        idx = i;
      }
    }
    if ((-1 != idx) && OB_FAIL(simple_thread_pool_list_.remove(idx))) {
      COMMON_LOG(WARN, "failed to remove simple_thread_pool", K(ret), K(idx), KP(pool));
    } else {
      pool->has_bind_ = false;
      COMMON_LOG(INFO, "try to unbind simple thread pool", K(*pool), K(idx));
    }
  }
  return ret;
}

int64_t ObSimpleThreadPoolDynamicMgr::get_pool_num() const
{
  return simple_thread_pool_list_.size();
}

} // namespace common
} // namespace oceanbase
