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

#ifndef OCEANBASE_SHARE_OB_CACHE_TEST_UTILS_H_
#define OCEANBASE_SHARE_OB_CACHE_TEST_UTILS_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/ob_define.h"
#include "share/ob_thread_pool.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/cache/ob_kvcache_hazard_version.h"
namespace oceanbase
{
using namespace lib;
namespace common
{

template<int64_t SIZE>
struct TestKVCacheKey: public ObIKVCacheKey
{
  TestKVCacheKey(void)
      : v_(0), tenant_id_(0)
  {
    memset(buf_, 0, sizeof(buf_));
  }
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  virtual uint64_t hash() const
  {
    return v_;
  }
  virtual int64_t size() const
  {
    return sizeof(*this);
  }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  uint64_t v_;
  uint64_t tenant_id_;
  char buf_[SIZE > sizeof(v_) ? SIZE - sizeof(v_) : 0];
};

template<int64_t SIZE>
struct TestKVCacheValue: public ObIKVCacheValue
{
  TestKVCacheValue(void)
      : v_(0)
  {
    memset(buf_, 0, sizeof(buf_));
  }
  virtual int64_t size() const
  {
    return sizeof(*this);
  }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  uint64_t v_;
  char buf_[SIZE > sizeof(v_) ? SIZE - sizeof(v_) : 0];
};


template<int64_t SIZE>
bool TestKVCacheKey<SIZE>::operator ==(const ObIKVCacheKey &other) const
{
  const TestKVCacheKey &other_key = reinterpret_cast<const TestKVCacheKey &>(other);
  return v_ == other_key.v_ && tenant_id_ ==  other_key.tenant_id_;
}

template<int64_t SIZE>
int TestKVCacheKey<SIZE>::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  TestKVCacheKey<SIZE> *pkey = NULL;
  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    pkey = new (buf) TestKVCacheKey<SIZE>();
    pkey->v_ = v_;
    pkey->tenant_id_ = tenant_id_;
    key = pkey;
  }
  return ret;
}

template<int64_t SIZE>
int TestKVCacheValue<SIZE>::deep_copy(
  char *buf,
  const int64_t buf_len,
  ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  TestKVCacheValue<SIZE> *pvalue = NULL;
  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    pvalue = new (buf) TestKVCacheValue<SIZE>();
    pvalue->v_ = v_;
    value = pvalue;
  }
  return ret;
}



struct AllocBuf
{
  void *ptr_;
  AllocBuf *next_;
};

class ObCacheTestTask;
class ObICacheTestStat
{
public:
  virtual void add_task(ObCacheTestTask *task) = 0;
  virtual ObCacheTestTask *pop_oppo_task(ObCacheTestTask *task) = 0;
  virtual void inc_fail_count() = 0;
};

class ObCacheTestTask
{
public:
  ObCacheTestTask(const int64_t tenant_id, const bool is_alloc,
      const int64_t alloc_size, const int64_t alloc_count,
      ObICacheTestStat *stat)
    : tenant_id_(tenant_id), is_alloc_(is_alloc),
      alloc_size_(alloc_size), alloc_count_(alloc_count),
      next_(NULL), stat_(stat), alloc_list_(NULL)
  {
  }

  virtual ~ObCacheTestTask() {}

  virtual int process()
  {
    int ret = OB_SUCCESS;
    if (is_alloc_) {
      ObMemAttr attr;
      attr.tenant_id_ = tenant_id_;
      attr.label_ = "CacheTestTask"; //xxx
      for (int64_t i = 0; i < alloc_count_; ++i) {
        void *ptr = ob_malloc(alloc_size_, attr);
        if (NULL == ptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "ob_malloc failed", K(ret), K_(alloc_size));
          ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
          ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);
          break;
        } else {
          append_alloc_list(ptr);
        }
      }

      if (OB_SUCC(ret)) {
        stat_->add_task(this);
      }
    } else {
      ObCacheTestTask *oppo_task = stat_->pop_oppo_task(this);
      if (NULL == oppo_task) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "oppo_task not exist", K(ret), "this", *this);
      } else {
        int64_t free_count = 0;
        AllocBuf *buf = oppo_task->alloc_list_;
        AllocBuf *next = NULL;
        while (NULL != buf) {
          next = buf->next_;
          ob_free(buf->ptr_);
          buf = next;
          ++free_count;
        }

        if (free_count != alloc_count_) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "free_count != alloc_count_", K(ret), K(free_count), K_(alloc_count));
        }

        ob_free(oppo_task);
      }
    }

    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "task process failed", K(ret));
      ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
      ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);
      stat_->inc_fail_count();
    }

    return ret;
  }
  void set_next(ObCacheTestTask *task) { next_ = task; }
  void append_alloc_list(void *ptr)
  {
    AllocBuf *buf = new (ptr) AllocBuf();
    buf->ptr_ = ptr;
    buf->next_ = alloc_list_;
    alloc_list_ = buf;
  }
  TO_STRING_KV(K_(tenant_id), K_(is_alloc), K_(alloc_size), K_(alloc_count));
public:
  uint64_t tenant_id_;
  bool is_alloc_;  // alloc or free
  int64_t alloc_size_;
  int64_t alloc_count_;
  ObCacheTestTask *next_;
  ObICacheTestStat *stat_;
  AllocBuf *alloc_list_;
};

class CacheTestStat : public ObICacheTestStat
{
public:
  CacheTestStat() : fail_count_(0), task_list_(NULL) {}
  virtual ~CacheTestStat() {}

  virtual void inc_fail_count() { ATOMIC_AAF(&fail_count_, 1); }
  virtual int64_t get_fail_count() const { return ATOMIC_LOAD(&fail_count_); }
  virtual void add_task(ObCacheTestTask *task)
  {
    if (NULL != task) {
      task->next_ = task_list_;
      task_list_ = task;
    }
  }
  virtual ObCacheTestTask *pop_oppo_task(ObCacheTestTask *task)
  {
    ObCacheTestTask *oppo_task = NULL;
    ObCacheTestTask *prev_task = NULL;
    ObCacheTestTask *cur_task = NULL;
    if (NULL != task && !task->is_alloc_) {
      cur_task = task_list_;
      while (NULL != cur_task) {
        if (cur_task->is_alloc_ && cur_task->tenant_id_ == task->tenant_id_
            && cur_task->alloc_size_ == task->alloc_size_ && cur_task->alloc_count_ == task->alloc_count_) {
          oppo_task = cur_task;
          // delete it from list
          if (NULL != prev_task) {
            prev_task->next_ = cur_task->next_;
            cur_task->next_ = NULL;
          } else {
            task_list_ = cur_task->next_;
            cur_task->next_ = NULL;
          }
          break;
        }
        cur_task = cur_task->next_;
        prev_task = cur_task;
      }
    }
    return oppo_task;
  }
private:
  int64_t fail_count_;
  ObCacheTestTask *task_list_;
};

class ObAllocatorStress : public share::ObThreadPool
{
public:
  ObAllocatorStress() : inited_(false), stat_(), queue_() {}
  virtual ~ObAllocatorStress() {}

  int init()
  {
    int ret = OB_SUCCESS;
    if (inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice", K(ret));
    } else if (OB_FAIL(queue_.init(1024))) {
      COMMON_LOG(WARN, "queue init failed", K(ret));
    } else {
      inited_ = true;
    }
    return ret;
  }

  virtual void run1()
  {

    // UNUSED(arg);
    int ret = OB_SUCCESS;
    COMMON_LOG(INFO, "allocator stress thread start");
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not init", K(ret));
    } else {
      // will process all task before exit
      while (!has_set_stop() || OB_ENTRY_NOT_EXIST != ret) {
        ObCacheTestTask *task = NULL;
        if (OB_FAIL(pop(task))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            COMMON_LOG(WARN, "pop task failed", K(ret));
          }
        } else {
          if (OB_FAIL(task->process())) {
            COMMON_LOG(WARN, "task process failed", K(ret));
          } else {
            COMMON_LOG(INFO, "task process succeed", "task", *task);
          }
          ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
          ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);

          ObMallocAllocator::get_instance()->print_tenant_memory_usage(task->tenant_id_);
          ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(task->tenant_id_);
        }
      }
    }
    COMMON_LOG(INFO, "allocator stress thread end");
  }

  int add_task(const ObCacheTestTask &task)
  {
    int ret = OB_SUCCESS;
    const int64_t buf_size = sizeof(task);
    void *ptr = NULL;
    if (NULL == (ptr = ob_malloc(buf_size, ObNewModIds::TEST))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "ob_malloc failed", K(ret), K(buf_size));
    } else {
      ObCacheTestTask *copy_task = new (ptr) ObCacheTestTask(
          task.tenant_id_, task.is_alloc_, task.alloc_size_, task.alloc_count_, task.stat_);
      if (OB_FAIL(queue_.push(copy_task))) {
        COMMON_LOG(WARN, "push task failed", K(ret));
      }
    }
    return ret;
  }

  int pop(ObCacheTestTask *&task)
  {
    int ret = OB_SUCCESS;
    void *vp = NULL;
    const int64_t timeout = 1000 * 1000;
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN,"not init", K(ret));
    } else {
      ret = queue_.pop(vp, timeout);
      if (OB_FAIL(ret)) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          COMMON_LOG(WARN, "queue pop failed", K(ret));
        }
      } else {
        task = static_cast<ObCacheTestTask *>(vp);
      }
    }
    return ret;
  }

  int64_t get_fail_count() { return stat_.get_fail_count(); }
  CacheTestStat *get_stat() { return &stat_; }
private:
  bool inited_;
  CacheTestStat stat_;
  common::ObLightyQueue queue_;
};

template<int64_t K_SIZE, int64_t V_SIZE>
class ObCacheStress : public share::ObThreadPool
{
public:
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  ObCacheStress() : inited_(false), tenant_id_(OB_INVALID_ID),
      put_count_(0), fail_count_(0), cache_()  {}
  virtual ~ObCacheStress() {}

  int init(const uint64_t tenant_id, int64_t index)
  {
    int ret = OB_SUCCESS;
    char cache_name[1024];
    snprintf(cache_name, 1024, "%s_%ld", "test", index);
    if (inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice", K(ret));
    } else if (OB_FAIL(cache_.init(cache_name))) {
      COMMON_LOG(WARN, "cache init failed", K(ret));
    } else {
      tenant_id_ = tenant_id;
      inited_ = true;
    }
    return ret;
  }
  virtual void run1()
  {

    // UNUSED(arg);
    int ret = OB_SUCCESS;
    COMMON_LOG(INFO, "cache stress thread start");
    TestKey key;
    TestValue value;
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not init", K(ret));
    } else {
      while (!has_set_stop()) {
        key.tenant_id_ = tenant_id_;
        key.v_ = put_count_;
        if (OB_FAIL(cache_.put(key, value))) {
          COMMON_LOG(WARN, "cache put failed", K(ret));
        } else {
          const TestValue *get_value = NULL;
          ObKVCacheHandle handle;
          if (OB_FAIL(cache_.get(key, get_value, handle))) {
            COMMON_LOG(WARN, "cache get failed", K(ret));
          }
        }
        ++put_count_;
        if (OB_FAIL(ret)) {
          ++fail_count_;
        }
      }
    }
    COMMON_LOG(INFO, "cache stress thread exit");
  }

  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_put_count() const { return put_count_; }
  int64_t get_fail_count() const { return fail_count_; }
private:
  bool inited_;
  uint64_t tenant_id_;
  int64_t put_count_;
  int64_t fail_count_;
  ObKVCache<TestKey, TestValue> cache_;
};

template<int64_t K_SIZE, int64_t V_SIZE>
class ObCacheGetStress : public share::ObThreadPool
{
public:
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  ObCacheGetStress()
    : inited_(false), tenant_id_(OB_INVALID_ID),
      kv_cnt_(0), hit_cnt_(0), total_cnt_(0), fail_cnt_(0), cache_()
  {
  }

  int init(const uint64_t tenant_id, const int64_t kv_cnt)
  {
    int ret = OB_SUCCESS;
    if (inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice", K(ret));
    } else if (OB_INVALID_ID == tenant_id || kv_cnt <= 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(kv_cnt));
    } else if (OB_FAIL(cache_.init("test_cache"))) {
      COMMON_LOG(WARN, "cache init failed", K(ret));
    } else {
      // put kv pairs to cache
      TestKey key;
      TestValue value;
      for (int64_t i = 0; OB_SUCC(ret) && i < kv_cnt; ++i) {
        key.tenant_id_ = tenant_id;;
        key.v_ = i;
        if (OB_FAIL(cache_.put(key, value))) {
          COMMON_LOG(WARN, "put failed", K(ret));
        }
      }
      tenant_id_ = tenant_id;
      kv_cnt_ = kv_cnt;
      hit_cnt_ = 0;
      total_cnt_ = 0;
      fail_cnt_ = 0;
      inited_ = true;
    }
    return ret;
  }

  // will create monitor thread print hit ratio per second
  virtual void set_thread_count(const int64_t thread_count)
  {
    // extra thread for monitor thread
    share::ObThreadPool::set_thread_count(static_cast<int32_t>(thread_count + 1));
  }

  virtual void run1()
  {

    // int64_t thread_id = (int64_t)(arg);
    int64_t thread_id = (int64_t)(this->get_thread_idx());
    if (0 == thread_id) {
      do_monitor();
    } else {
      do_work();
    }
  }

  ObKVCache<TestKey, TestValue> &get_cache()
  {
    return cache_;
  }
  double get_hit_ratio()
  {
    int ret = OB_SUCCESS;
    int64_t hit_cnt = 0;
    TestKey key;
    const TestValue *pvalue = NULL;
    ObKVCacheHandle handle;
    for (int64_t i = 0; i < kv_cnt_; ++i) {
      key.tenant_id_ = tenant_id_;
      key.v_ = i;
      if (OB_FAIL(cache_.get(key, pvalue, handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          COMMON_LOG(ERROR, "get failed", K(ret));
        }
      } else {
        ++hit_cnt;
      }
    }
    return (double)hit_cnt / (double)kv_cnt_;
  }
private:
  void do_monitor()
  {
    while (!has_set_stop()) {
      double hit_ratio = 0;
      const int64_t hit_cnt = ATOMIC_LOAD(&hit_cnt_);
      const int64_t total_cnt = ATOMIC_LOAD(&total_cnt_);
      ATOMIC_STORE(&hit_cnt_, 0);
      ATOMIC_STORE(&total_cnt_, 0);
      if (total_cnt > 0) {
        hit_ratio = (double)hit_cnt / (double)total_cnt;
      }
      COMMON_LOG(INFO, "get stress stat", K(hit_ratio));
      sleep(1);
    }
  }

  void do_work()
  {
    int ret = OB_SUCCESS;
    TestKey key;
    const TestValue *pvalue = NULL;
    ObKVCacheHandle handle;
    while (!has_set_stop()) {
      for (int64_t i = 0; i < kv_cnt_ && !has_set_stop(); ++i) {
        const int64_t get_cnt = i < (int64_t)((double)kv_cnt_ * 0.8) ? 1 : 4;
        key.tenant_id_ = tenant_id_;
        key.v_ = i;
        for (int64_t i = 0; i < get_cnt; ++i) {
          ATOMIC_INC(&total_cnt_);
          if (OB_FAIL(cache_.get(key, pvalue, handle))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              COMMON_LOG(ERROR, "get failed", K(ret));
            } else {
              ATOMIC_INC(&fail_cnt_);
            }
          } else {
            ATOMIC_INC(&hit_cnt_);
          }
        }
      }
    }
  }

  bool inited_;
  uint64_t tenant_id_;
  int64_t kv_cnt_;
  int64_t hit_cnt_;
  int64_t total_cnt_;
  int64_t fail_cnt_;
  ObKVCache<TestKey, TestValue> cache_;
};

template<int64_t K_SIZE, int64_t V_SIZE>
class ObWorkingSetStress : public share::ObThreadPool
{
public:
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  ObWorkingSetStress() : inited_(false), tenant_id_(OB_INVALID_ID),
      put_count_(0), fail_count_(0)  {}
  virtual ~ObWorkingSetStress() {}

  int init(const uint64_t tenant_id, const bool only_put)
  {
    int ret = OB_SUCCESS;
    if (inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice", K(ret));
    } else if (OB_FAIL(cache_.init("test_cache"))) {
      COMMON_LOG(WARN, "cache init failed", K(ret));
    } else if (OB_FAIL(ws_.init(tenant_id, cache_))) {
      COMMON_LOG(WARN, "init ws failed", K(ret), K(tenant_id));
    } else {
      tenant_id_ = tenant_id;
      put_count_ = 0;
      fail_count_ = 0;
      only_put_ = only_put;
      pcache_ = &ws_;
      inited_ = true;
    }
    return ret;
  }

  int init(const uint64_t tenant_id, ObKVCache<TestKey, TestValue> &cache, const bool use_ws, const int64_t start_key = 0)
  {
    int ret = OB_SUCCESS;
    if (inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice", K(ret));
    } else {
      tenant_id_ = tenant_id;
      put_count_ = 0;
      fail_count_ = 0;
      only_put_ = false;
      pcache_ = &cache;
      start_key_ = start_key;
      if (use_ws) {
        if (OB_FAIL(ws_.init(tenant_id, cache))) {
          COMMON_LOG(WARN, "init ws failed", K(ret), K(tenant_id));
        } else {
          pcache_ = &ws_;
        }
      }
      if (OB_SUCC(ret)) {
        inited_ = true;
      }
    }
    return ret;
  }

  virtual void run1()
  {

    // const int64_t thread_id = (int64_t)(arg);
    const int64_t thread_id = this->get_thread_idx();
    const int64_t count = this->get_thread_count();
    int64_t put_count = 0;
    int64_t fail_count = 0;
    int ret = OB_SUCCESS;
    COMMON_LOG(INFO, "working set stress thread start");
    TestKey key;
    TestValue value;
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not init", K(ret));
    } else {
      while (!has_set_stop()) {
        key.tenant_id_ = tenant_id_;
        key.v_ = start_key_ + thread_id + put_count * count;
        if (OB_FAIL(pcache_->put(key, value))) {
          COMMON_LOG(WARN, "cache put failed", K(ret));
        } else if (!only_put_) {
          const TestValue *get_value = NULL;
          ObKVCacheHandle handle;
          if (OB_FAIL(pcache_->get(key, get_value, handle))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              COMMON_LOG(WARN, "cache get failed", K(ret));
            }
          }
        }
        ++put_count;
        if (OB_FAIL(ret)) {
          ++fail_count;
          ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);
          ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);

        }
      }
    }
    ATOMIC_AAF(&put_count_, put_count);
    ATOMIC_AAF(&fail_count_, fail_count);
    COMMON_LOG(INFO, "working set stress thread exit");
  }

  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_put_count() const { return put_count_; }
  int64_t get_fail_count() const { return fail_count_; }
  int64_t get_used() const { return ws_.working_set_->get_used(); }
  int64_t get_limit() const { return ws_.working_set_->get_limit(); }
private:
  bool inited_;
  uint64_t tenant_id_;
  int64_t put_count_;
  int64_t fail_count_;
  bool only_put_;
  ObIKVCache<TestKey, TestValue> *pcache_;
  ObKVCache<TestKey, TestValue> cache_;
  ObCacheWorkingSet<TestKey, TestValue> ws_;
  int64_t start_key_;
};

class TestNode : public ObKVCacheHazardNode{
public:
  TestNode()
    : id_(0) 
  {}
  virtual void retire() {COMMON_LOG(INFO, "TestNode(HazardNode) retire", K(this->get_version()), K(id_));}
private:
  int64_t id_;
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_CACHE_TEST_UTILS_H_
