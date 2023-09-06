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

#ifndef  OCEANBASE_COMMON_HASH_HASHMAP_
#define  OCEANBASE_COMMON_HASH_HASHMAP_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <new>
#include <pthread.h>
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_serialization.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
template <class _key_type, class _value_type>
struct HashMapTypes
{
  typedef HashMapPair<_key_type, _value_type> pair_type;
  typedef typename HashTableTypes<pair_type>::AllocType AllocType;
};

template <class _key_type, class _value_type>
struct hashmap_preproc
{
  typedef typename HashMapTypes<_key_type, _value_type>::pair_type pair_type;
  _value_type &operator()(pair_type &pair) const
  {
    return pair.second;
  };
  char buf[0];
};

template <class _key_type,
          class _value_type,
          class _defendmode = LatchReadWriteDefendMode,
          class _hashfunc = hash_func<_key_type>,
          class _equal = equal_to<_key_type>,
          class _allocer = SimpleAllocer<typename HashMapTypes<_key_type, _value_type>::AllocType>,
          template <class> class _bucket_array = NormalPointer,
          class _bucket_allocer = oceanbase::common::ObMalloc,
          int64_t EXTEND_RATIO = 1>
class ObHashMap
{
  typedef typename HashMapTypes<_key_type, _value_type>::pair_type pair_type;
  typedef hashmap_preproc<_key_type, _value_type> preproc;
public:
  typedef ObHashTable<_key_type, pair_type, _hashfunc, _equal, pair_first<pair_type>, _allocer, _defendmode, _bucket_array, _bucket_allocer, EXTEND_RATIO>
  hashtable;
  typedef typename hashtable::bucket_iterator bucket_iterator;
  typedef typename hashtable::iterator iterator;
  typedef typename hashtable::const_iterator const_iterator;
public:
  ObHashMap() : ht_()
  {
    // default
    ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_HASH_BUCKET);
    bucket_allocer_.set_attr(attr);
  };
  ~ObHashMap()
  {
  };
public:
  bucket_iterator bucket_begin()
  {
    return ht_.bucket_begin();
  };
  bucket_iterator bucket_end()
  {
    return ht_.bucket_end();
  };
  iterator begin()
  {
    return ht_.begin();
  };
  const_iterator begin() const
  {
    return ht_.begin();
  };
  iterator end()
  {
    return ht_.end();
  };
  const_iterator end() const
  {
    return ht_.end();
  };
  int64_t size() const
  {
    return ht_.size();
  }
  int64_t bucket_count() const
  {
    return ht_.get_bucket_count();
  }
  bool empty() const
  {
    return 0 == ht_.size();
  }
  inline bool created() const
  {
    return ht_.created();
  }

  int create(int64_t bucket_num,
             const ObMemAttr &bucket_attr,
             const ObMemAttr &node_attr)
  {
    allocer_.set_attr(node_attr);
    bucket_allocer_.set_attr(bucket_attr);
    return ht_.create(cal_next_prime(bucket_num), &allocer_, &bucket_allocer_);
  }
  int create(int64_t bucket_num,
             const ObMemAttr &bucket_attr)
  {
    return create(bucket_num, bucket_attr, bucket_attr);
  }
  int create(int64_t bucket_num, const lib::ObLabel &bucket_label,
             const lib::ObLabel &node_label = ObModIds::OB_HASH_NODE, uint64_t tenant_id = OB_SERVER_TENANT_ID,
             uint64_t ctx_id = ObCtxIds::DEFAULT_CTX_ID)
  {
    allocer_.set_attr(ObMemAttr(tenant_id, node_label, ctx_id));
    bucket_allocer_.set_attr(ObMemAttr(tenant_id, bucket_label, ctx_id));
    return ht_.create(cal_next_prime(bucket_num), &allocer_, &bucket_allocer_);
  };
  int create(int64_t bucket_num, _allocer *allocer, const lib::ObLabel &bucket_label,
             const lib::ObLabel &node_label = ObModIds::OB_HASH_NODE)
  {
    allocer_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, node_label));
    bucket_allocer_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, bucket_label));
    return ht_.create(cal_next_prime(bucket_num), allocer, &bucket_allocer_);
  };
  int create(int64_t bucket_num, _allocer *allocer, _bucket_allocer *bucket_allocer)
  {
    return ht_.create(cal_next_prime(bucket_num), allocer, bucket_allocer);
  };
  int destroy()
  {
    int ret = ht_.destroy();
    return ret;
  };
  int clear()
  {
    int ret = ht_.clear();
    return ret;
  };
  int reuse()
  {
    return clear();
  };
  _allocer &get_local_allocer() {return allocer_;};
  _bucket_allocer &get_local_bucket_allocer() {return bucket_allocer_;};
  inline int get_refactored(const _key_type &key, _value_type &value, const int64_t timeout_us = 0) const
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    if (OB_FAIL(pair.init(key, value))) {
      HASH_WRITE_LOG(HASH_WARNING, "init pair failed, ret=%d", ret);
    } else {
      if (OB_SUCC(const_cast<hashtable &>(ht_).get_refactored(key, pair, timeout_us))) {
        if (OB_FAIL(copy_assign(value, pair.second))) {
          HASH_WRITE_LOG(HASH_FATAL, "copy assign failed, ret=%d", ret);
        }
      }
    }
    return ret;
  };
  inline const _value_type *get(const _key_type &key) const
  {
    const _value_type *ret = NULL;
    const pair_type *pair = NULL;
    if (OB_SUCCESS == const_cast<hashtable &>(ht_).get_refactored(key, pair)
        && NULL != pair) {
      ret = &(pair->second);
    }
    return ret;
  };
  // flag: 0 shows that do not cover existing object
  inline _value_type *get(_key_type &key)
  {
    const _value_type *ret = get(const_cast<const _key_type&>(key));
    return const_cast<_value_type*>(ret);
  }
  template <typename _callback = void>
  int set_refactored(const _key_type &key, const _value_type &value, int flag = 0,
                 int broadcast = 0, int overwrite_key = 0, _callback *callback = nullptr)
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    if (OB_FAIL(pair.init(key, value))) {
      HASH_WRITE_LOG(HASH_WARNING, "init pair failed, ret=%d", ret);
    } else {
      ret = ht_.set_refactored(key, pair, flag, broadcast, overwrite_key, callback);
    }
    return ret;
  };
  template <class _callback>
  int atomic_refactored(const _key_type &key, _callback &callback)
  {
    //return ht_.atomic(key, callback, preproc_);
    return ht_.atomic_refactored(key, callback);
  };

  // this operation will rdlock on bucket, if there is writing in callback, keep atomic by yourself.
  template <class _callback>
  int read_atomic(const _key_type &key, _callback &callback)
  {
    return ht_.read_atomic(key, callback);
  }
  // thread safe scan, will add read lock to the bucket, the modification to the value is forbidden
  //
  // @param callback
  // @return OB_SUCCESS for success, other for error
  template<class _callback>
  int foreach_refactored(_callback &callback) const
  {
    return ht_.foreach_refactored(callback);
  }
  int erase_refactored(const _key_type &key, _value_type *value = NULL)
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    if (NULL != value) {
      if (OB_FAIL(ht_.erase_refactored(key, &pair))) {
      } else if (OB_FAIL(copy_assign(*value, pair.second))) {
        HASH_WRITE_LOG(HASH_FATAL, "copy assign failed, ret=%d", ret);
      }
    } else {
      ret = ht_.erase_refactored(key);
    }
    return ret;
  };
  // 该原子操作在bucket上添加的写锁,
  // 如果节点存在，调用 callback 进行修改，如果节点不存在，插入该节点
  //
  // 返回值：
  //   OB_SUCCESS 表示成功
  //   其它 表示出错
  template <class _callback>
  int set_or_update(const _key_type &key, const _value_type &value,
                    _callback &callback)
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    if (OB_FAIL(pair.init(key, value))) {
      HASH_WRITE_LOG(HASH_WARNING, "init pair failed, ret=%d", ret);
    } else {
      ret = ht_.set_or_update(key, pair, callback);
    }
    return ret;
  };
  // erase key value pair if pred is met
  // thread safe erase, will add write lock to the bucket
  // return value:
  //   OB_SUCCESS for success
  //   OB_HASH_NOT_EXIST for node not exists
  //   others for error
  template<class _pred>
  int erase_if(const _key_type &key, _pred &pred, bool &is_erased, _value_type *value = NULL)
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    if (NULL != value) {
      if (OB_FAIL(ht_.erase_if(key, pred, is_erased, &pair))) {
      } else if (is_erased && OB_FAIL(copy_assign(*value, pair.second))) {
        HASH_WRITE_LOG(HASH_FATAL, "copy assign failed, ret=%d", ret);
      }
    } else {
      ret = ht_.erase_if(key, pred, is_erased);
    }
    return ret;
  }
  template <class _archive>
  int serialization(_archive &archive)
  {
    return ht_.serialization(archive);
  };
  template <class _archive>
  int deserialization(_archive &archive)
  {
    return ht_.deserialization(archive, &allocer_);
  };
private:
  preproc preproc_;
  _allocer allocer_;
  _bucket_allocer bucket_allocer_;
  hashtable ht_;

  DISALLOW_COPY_AND_ASSIGN(ObHashMap);
};
}//namespace hash
}//namespace common
}//namespace oceanbase
#endif //OCEANBASE_COMMON_HASH_HASHMAP_
