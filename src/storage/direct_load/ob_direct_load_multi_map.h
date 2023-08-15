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
#ifndef OB_DIRECT_LOAD_MULTI_MAP_H_
#define OB_DIRECT_LOAD_MULTI_MAP_H_

#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_array.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{

template<class Key, class Value>
class ObDirectLoadMultiMapNoLock
{
  typedef common::hash::ObHashMap<Key, common::ObArray<Value> *, common::hash::NoPthreadDefendMode> MapType;
  typedef common::hash::HashMapPair<Key, common::ObArray<Value> *> MapTypePair;
public:
  ObDirectLoadMultiMapNoLock()
  {
  }

  int init()
  {
    return map_.create(1024, "TLD_multi_map", "TLD_multi_map", MTL_ID());
  }

  virtual ~ObDirectLoadMultiMapNoLock()
  {
    destroy();
  }

  int add(const Key &key, const Value &value)
  {
    int ret = common::OB_SUCCESS;
    common::ObArray<Value> *bag = nullptr;
    ret = map_.get_refactored(key, bag);
    if (ret == common::OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      bag = OB_NEW(common::ObArray<Value>, "TLD_MM_bag", OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("TLD_MM_bagi", MTL_ID()));
      if (OB_FAIL(map_.set_refactored(key, bag))) {
        STORAGE_LOG(WARN, "fail to put bag", KR(ret));
      }
    } else if (ret != OB_SUCCESS) {
      STORAGE_LOG(WARN, "fail to get bag", KR(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(bag->push_back(value))) {
        STORAGE_LOG(WARN, "fail to push back value", KR(ret));
      }
    }
    return ret;
  }

  int get_all_key(common::ObIArray<Key> &keys)
  {
    int ret = OB_SUCCESS;
    auto fn = [&keys] (MapTypePair &p) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(keys.push_back(p.first))) {
        STORAGE_LOG(WARN, "fail to push key", KR(ret));
      }
      return ret;
    };
    if (OB_FAIL(map_.foreach_refactored(fn))) {
      STORAGE_LOG(WARN, "fail to traverse map", KR(ret));
    }
    return ret;
  }

  int get(const Key &key, common::ObIArray<Value> &out_bag)
  {
    int ret = OB_SUCCESS;
    common::ObArray<Value> *bag = nullptr;
    if (OB_FAIL(map_.get_refactored(key, bag))) {
      if (ret == common::OB_HASH_NOT_EXIST) {
        bag = nullptr;
        ret = common::OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "fail to get bag", KR(ret));
      }
    }
    if (bag != nullptr) {
      for (int64_t i = 0; OB_SUCC(ret) && i < bag->count(); i ++) {
        if (OB_FAIL(out_bag.push_back(bag->at(i)))) {
          STORAGE_LOG(WARN, "fail to push item", KR(ret));
        }
      }
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMultiMapNoLock);
  void destroy()
  {
    auto fn = [] (MapTypePair &p) {
      int ret = OB_SUCCESS;
      if (p.second != nullptr) {
        p.second->~ObArray<Value>();
        ob_free(p.second);
      }
      return ret;
    };
    map_.foreach_refactored(fn);
  }

private:
  // data members
  MapType map_;
};

template<class Key, class Value>
class ObDirectLoadMultiMap
{
public:
  ObDirectLoadMultiMap()
  {
  }

  int init()
  {
    return multi_map_.init();
  }

  virtual ~ObDirectLoadMultiMap()
  {
  }

  int add(const Key &key, const Value &value)
  {
    lib::ObMutexGuard guard(mutex_);
    return multi_map_.add(key, value);
  }

  int get_all_key(common::ObIArray<Key> &keys)
  {
    lib::ObMutexGuard guard(mutex_);
    return multi_map_.get_all_key(keys);
  }

  int get(const Key &key, common::ObIArray<Value> &out_bag)
  {
    lib::ObMutexGuard guard(mutex_);
    return multi_map_.get(key, out_bag);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMultiMap);

private:
  // data members
  ObDirectLoadMultiMapNoLock<Key, Value> multi_map_;
  lib::ObMutex mutex_;
};





}
}

#endif /* OB_DIRECT_LOAD_MULTI_MAP_H_ */
