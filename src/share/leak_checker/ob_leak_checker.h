/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_SHARE_OB_LEAK_CHECKER_H_
#define OCEANBASE_SHARE_OB_LEAK_CHECKER_H_
#include "lib/hash/ob_hashmap.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{

template <typename Key, typename Value>
class ObBaseLeakChecker
{
  typedef common::ObLinearHashMap<Key, Value> tenant_leak_checker_t;
  struct Printer
  {
    bool operator()(const Key &k, const Value &v)
    {
      bool ret = true;
      COMMON_LOG(INFO, "LEAK_CHECKER ",
                 "key:", k,
                 "value:", v);
      return ret;
    }
  };
public:
  ObBaseLeakChecker();
  ~ObBaseLeakChecker();
  int init(const uint64_t tenant_id);
  void reset();
  void record(const Key &k, const Value &v, const int64_t max_cnt=INT64_MAX);
  void release(const Key &k, Value &value);
  template <typename Function> int for_each(Function &fn);
  void print();
  bool is_empty()
  { return (ATOMIC_LOAD(&total_size_) == 0); }
private:
  static constexpr int MEMORY_LIMIT = 128L << 20;
  static constexpr int MAP_SIZE_LIMIT = MEMORY_LIMIT / sizeof(Value);
  tenant_leak_checker_t checker_info_;
  int64_t total_size_;
};

template<typename Key, typename Value>
ObBaseLeakChecker<Key, Value>::ObBaseLeakChecker()
{
  total_size_ = 0;
}

template<typename Key, typename Value>
ObBaseLeakChecker<Key, Value>::~ObBaseLeakChecker()
{
  reset();
}

template<typename Key, typename Value>
int ObBaseLeakChecker<Key, Value>::init(const uint64_t tenant_id)
{
  ObMemAttr attr(tenant_id, "leakChecker", ObCtxIds::DEFAULT_CTX_ID);
  int ret = checker_info_.init(attr);
  if (OB_FAIL(ret)) {
    COMMON_LOG(ERROR, "failed to create hashmap", K(ret));
  } else {
    COMMON_LOG(INFO, "leak checker init succ");
  }
  return ret;
}

template<typename Key, typename Value>
void ObBaseLeakChecker<Key, Value>::reset()
{
  checker_info_.reset();
  total_size_ = 0;
}

template<typename Key, typename Value>
void ObBaseLeakChecker<Key, Value>::record(const Key &k, const Value &v, const int64_t max_cnt)
{
  INIT_SUCC(ret);
  if (total_size_ < OB_MIN(MAP_SIZE_LIMIT, max_cnt)) {
    if (OB_FAIL(checker_info_.insert(k, v))) {
      COMMON_LOG(WARN, "Fail to register leak info", K(ret), K(k), K(v));
    } else {
      ATOMIC_INC(&total_size_);
    }
  }
}

template<typename Key, typename Value>
void ObBaseLeakChecker<Key, Value>::release(const Key &k, Value &value)
{
  INIT_SUCC(ret);
  if (OB_FAIL(checker_info_.erase(k, value))) {
    COMMON_LOG(WARN, "Fail to unregister leak info", K(ret), K(k));
  } else {
    ATOMIC_DEC(&total_size_);
  }
}

template<typename Key, typename Value>
void ObBaseLeakChecker<Key, Value>::print()
{
  Printer fn;
  for_each(fn);
}

template <typename Key, typename Value>
template <typename Function>
int ObBaseLeakChecker<Key, Value>::for_each(Function &fn)
{
  int ret = OB_SUCCESS;
  ret = checker_info_.for_each(fn);
  return ret;
}

}  // storage
}  // oceanbase
#endif  // OCEANBASE_SHARE_OB_LEAK_CHECKER_H_
