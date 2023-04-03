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

#ifndef OCEANBASE_SHARE_OB_CACHE_GET_STRESSOR_H_
#define OCEANBASE_SHARE_OB_CACHE_GET_STRESSOR_H_

#include "share/ob_define.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_thread_pool.h"

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

template<int64_t K_SIZE, int64_t V_SIZE>
class ObCacheGetStressor : public share::ObThreadPool
{
public:
  typedef TestKVCacheKey<K_SIZE> TestKey;
  typedef TestKVCacheValue<V_SIZE> TestValue;

  ObCacheGetStressor()
  {
  }

  virtual ~ObCacheGetStressor()
  {
  }

  int init(ObKVCache<TestKey, TestValue> &cache,
      const uint64_t tenant_id, const int64_t index, const int64_t count,
      const int64_t kv_count)
  {
    cache_ = &cache;
    tenant_id_ = tenant_id;
    index_ = index;
    count_ = count;
    kv_count_ = kv_count;
    return OB_SUCCESS;
  }
  virtual void run1()
  {

    // UNUSED(arg);
    //int ret = OB_SUCCESS;
    COMMON_LOG(INFO, "cache get stress thread start");
    /*
    TestKey key;
    key.tenant_id_ = tenant_id_;
    const TestValue *get_value = NULL;
    ObKVCacheHandle handle;
    while (!has_set_stop()) {
      for (int64_t i = 0; i < kv_count_; i += count_) {
        key.v_ = i;
        if (OB_FAIL(cache_->get(key, get_value, handle))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            COMMON_LOG(WARN, "get failed", K(ret));
          }
        }
      }
    }
    */
    while (!has_set_stop()) {
      ObMemAttr attr;
      attr.label_ = "CacheGetStress";
      attr.tenant_id_ = tenant_id_;
      void *buf = ob_malloc(16 * 1024, attr);
      ob_free(buf);
    }
  }

  static int make_cache_full(ObKVCache<TestKey, TestValue> &cache, const uint64_t tenant_id, const int64_t kv_count)
  {
    int ret = OB_SUCCESS;
    TestKey key;
    TestValue value;
    key.tenant_id_ = tenant_id;
    for (int64_t i = 0; i < kv_count; ++i) {
      key.v_ = i;
      const TestValue *get_value = NULL;
      ObKVCacheHandle handle;
      if (OB_FAIL(cache.get(key, get_value, handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          COMMON_LOG(WARN, "get failed", K(ret));
        } else if (OB_FAIL(cache.put(key, value))) {
          COMMON_LOG(WARN, "put failed", K(ret));
          ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id);
          ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id);
        }
      }
    }
    return ret;
  }

private:
  ObKVCache<TestKey, TestValue> *cache_;
  uint64_t tenant_id_;
  int64_t index_;
  int64_t count_;
  int64_t kv_count_;
};
}
}
#endif //OCEANBASE_SHARE_OB_CACHE_GET_STRESSOR_H_
