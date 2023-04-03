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

#ifndef OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_
#define OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_

#include "lib/hash_func/ob_hash_func.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/list/ob_intrusive_list.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
// A hash map usable by obproxy.
//
// This class depends on the @c DLL class from @c ob_intrusive_list.h. It assumes it can uses instances of
// that class to store chains of elements.


// @code
// bool equal (Key lhs, Key rhs);
// bool equal (Key key, Value const* value);
// @endcode
//
// Example for @c ObMysqlServerSession keyed by the origin server IP address.
//
// @code
// struct Hasher {
//   typedef sockaddr const &Key;
//   typedef ObMysqlServerSession Value;
//   typedef ObDLList(ObMysqlServerSession, ip_hash_link_) ListHead;
//
//   static uint64_t hash(Key key) { return do_hash(key); }
//   static sockaddr const &key(Value const *value) { return &value->ip_.sa_ }
//   static bool equal(Key lhs, Key rhs) { return ops_ip_addr_port_eq(lhs, rhs); }
//   // Alternatively
//   // static uint64_t hash(Key key);
//   // static Key key(Value const *value);
//   // static bool equal(Key lhs, Key rhs);
// };
// @endcode
//
// In @c ObMysqlServerSession is the definition
//
// @code
// LINK(ObMysqlServerSession, ip_hash_link_);
// @endcode
//
// which creates the internal links used by @c ObBuildInHashMap.
//
template <typename H, int64_t bucket_num = 16>
class ObBuildInHashMap
{
public:
  // Make embedded types easier to use by importing them to the class namespace.
  typedef H Hasher; // Rename and promote.
  typedef typename Hasher::Key Key; // Key type.
  typedef typename Hasher::Value Value; // Stored value (element) type.
  typedef typename Hasher::ListHead ListHead; // Anchor for value chain.

  struct ObBuildInBucket
  {
    ListHead chain_; // Chain of elements.
    int64_t count_; // # of elements in chain.

    // Internal chain for iteration.
    //
    // Iteration is tricky because it needs to skip over empty buckets and detect end of buckets.
    // Both of these are difficult inside the iterator without excess data. So we chain the
    // non-empty buckets and let the iterator walk that. This makes end detection easy and
    // iteration on sparse data fast. If we make it a doubly linked list adding and removing buckets
    // is very fast as well.
    LINK(ObBuildInBucket, link_);

    ObBuildInBucket() : count_(0)
    {
      memset(&link_, 0, sizeof(link_));
    }
  };
  // Make available to nested classes statically.
  // We must reach inside the link hackery because we're in a template and
  // must use typename. Older compilers don't handle typename outside of
  // template context so if we put typename in the base definition it won't
  // work in non-template classes.
  typedef DLL<ObBuildInBucket, typename ObBuildInBucket::Link_link_>
  BucketChain; // Anchor for bucket chain

  // Standard iterator for walking the map.
  // This iterates over all elements.
  // @internal Iterator is end if value_ is NULL.
  struct iterator
  {
    Value *value_; // Current location.
    ObBuildInBucket *bucket_; // Current bucket;

    iterator() : value_(NULL), bucket_(NULL) {}
    iterator &operator ++ ()
    {
      if (NULL != value_) {
        if (NULL == (value_ = ListHead::next(value_))) { // end of bucket, next bucket.
          if (NULL != (bucket_ = BucketChain::next(bucket_))) { // found non-empty next bucket.
            value_ = bucket_->chain_.head_;
            if (OB_ISNULL(value_)) {
              OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "NULL value", KP_(value)); // if bucket is in chain, must be non-empty.
            }
          }
        }
      }
      return *this;
    }
    Value &operator * () { return *value_; }
    Value *operator -> () { return value_; }
    bool operator == (iterator const &that) { return bucket_ == that.bucket_ && value_ == that.value_; }
    bool operator != (iterator const &that) { return !(*this == that); }

  protected:
    // Internal iterator constructor.
    iterator(ObBuildInBucket *b, Value *v) : value_(v), bucket_(b) {}
    friend class ObBuildInHashMap;
  };

public:
  ObBuildInHashMap() : count_(0) {}

  // Remove all values from the map.
  // The values are not cleaned up. The values are not touched in this method,
  // therefore it is safe to destroy them first and then @c clear this map.
  void reset()
  {
    memset(this, 0, sizeof(ObBuildInHashMap));
  }

  // Get the number of elements in the map.
  int64_t count() const { return count_; }

  iterator begin()
  {
    // Get the first non-empty bucket, if any.
    ObBuildInBucket *b = bucket_chain_.head_;
    return (NULL != b && NULL != b->chain_.head_)
           ? iterator(b, b->chain_.head_) : end();
  }

  iterator end() { return iterator(NULL, NULL); }


  // put a key value pair into HashMap
  // @retval OB_SUCCESS for success
  // @retval OB_HASH_EXIST when key already exist
  int set_refactored(Value *value)
  {
    int ret = OB_SUCCESS;
    Key key = Hasher::key(value);
    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];

    if (! bucket.chain_.in(value)) {
      bucket.chain_.push(value);
      ++count_;
      // not empty, put it on the non-empty list.
      if (1 == ++(bucket.count_)) {
        bucket_chain_.push(&bucket);
      }
    } else {
      ret = OB_HASH_EXIST;
    }

    return ret;
  }

  // @retval OB_SUCCESS for success
  // @retval OB_HASH_NOT_EXIST for key not exist
  int get_refactored(Key key, Value *&value) const
  {
    int ret = OB_HASH_NOT_EXIST;
    const ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
    Value *v = bucket.chain_.head_;
    while (NULL != v && ! Hasher::equal(key, Hasher::key(v))) {
      v = ListHead::next(v);
    }
    if (NULL != (value = v)) {
      ret = OB_SUCCESS;;
    }
    return ret;
  }

  Value *get(Key key) const
  {
    const ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
    Value *v = bucket.chain_.head_;
    while (NULL != v && ! Hasher::equal(key, Hasher::key(v))) {
      v = ListHead::next(v);
    }
    return v;
  }

  Value *remove(const Key key)
  {
    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
    Value *v = bucket.chain_.head_;
    while (NULL != v && ! Hasher::equal(key, Hasher::key(v))) {
      v = ListHead::next(v);
    }
    if (NULL != v) {
      bucket.chain_.remove(v);
      --count_;
      // if it's now empty, take it out of the non-empty bucket chain.
      if (0 == --(bucket.count_)) {
        bucket_chain_.remove(&bucket);
      }
    }
    return v;
  }

  // @retval OB_SUCCESS for success
  // @retval OB_HASH_NOT_EXIST for key not exist
  int erase_refactored(const Key key)
  {
    int ret = OB_SUCCESS;
    Value *v = remove(key);
    if (NULL == v) {
      ret = OB_HASH_NOT_EXIST;
    }
    return ret;
  }

private:
  ObBuildInBucket buckets_[bucket_num];
  int64_t count_; // of elements stored in the map.
  BucketChain bucket_chain_;
};
} // namespace hash
} // namespace common
} // namespace oceanbase
#endif //OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_
