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

#ifndef OCEANBASE_TRANSACTION_OB_CLOG_ENCRYPT_INFO_
#define OCEANBASE_TRANSACTION_OB_CLOG_ENCRYPT_INFO_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_trans_define.h"
#include "share/ob_encryption_util.h"
#include "share/ob_encryption_struct.h"


namespace oceanbase
{
using namespace common;

namespace transaction
{

struct ObSerializeEncryptMeta : public share::ObEncryptMeta
{
  ObSerializeEncryptMeta() : share::ObEncryptMeta() {}
  OB_UNIS_VERSION(1);
};

struct ObEncryptMetaCache
{
  int64_t table_id_;
  int64_t local_index_id_;
  ObSerializeEncryptMeta meta_;
  ObEncryptMetaCache() : table_id_(OB_INVALID_ID), local_index_id_(OB_INVALID_ID), meta_() {}
  inline int64_t real_table_id() const { return local_index_id_ != OB_INVALID_ID ? local_index_id_ : table_id_; }
  TO_STRING_KV(K_(meta), K_(table_id), K_(local_index_id));
  OB_UNIS_VERSION(1);
};

struct ObTxEncryptMeta
{
  ObTxEncryptMeta() : table_id_(OB_INVALID_ID), prev_(NULL), next_(NULL) {}
  int store_encrypt_meta(uint64_t table_id, const share::ObEncryptMeta &meta);
  bool is_valid() const { return ((table_id_ != OB_INVALID_ID) && meta_.is_valid()); }
  bool contain(const uint64_t &table_id) const { return table_id_ == table_id; }
  //for memtable compare
  bool is_memtable_equal(const share::ObEncryptMeta &other)
  {
    //now, we only take table_key_、master_key_version_、encrypt_algorithm_ into consideration.
    return (meta_.table_key_.get_content() == other.table_key_.get_content() &&
            meta_.master_key_version_ == other.master_key_version_ &&
            meta_.encrypt_algorithm_ == other.encrypt_algorithm_);
  }
  int assign(const ObTxEncryptMeta &other);
  void reset()
  {
    table_id_ = OB_INVALID_ID;
    meta_.reset();
    prev_ = NULL;
    next_ = NULL;
  }
  //table_id is used as encrypt_index
  uint64_t table_id_;
  share::ObEncryptMeta meta_;
  //for node in hash
  ObTxEncryptMeta *prev_;
  ObTxEncryptMeta *next_;

  TO_STRING_KV(K_(table_id), K_(meta));
  OB_UNIS_VERSION(1);
};

class ObEncryptMetaAlloc
{
public:
  static ObTxEncryptMeta* alloc_value()
  {
    ObTxEncryptMeta *it = nullptr;
    void *buf = ob_malloc(sizeof(ObTxEncryptMeta), "ClogEncrypt");
    if (nullptr != buf) {
      it = new(buf) ObTxEncryptMeta();
    }
    return it;
  }
  static void free_value(ObTxEncryptMeta *v)
  {
    if (NULL != v) {
      ob_free(v);
      v = NULL;
    }
  }
};

//this hashMap is just for clog encryption to save encryption data
//without lock protect
template<typename Key,
         typename Value,
         typename AllocHandle,
         class _hashfunc = common::hash::hash_func<Key>,
         int64_t BUCKETS_CNT = 10>
class ObSimpleHashMap
{
public:
  ObSimpleHashMap() : is_inited_(false), total_cnt_(0)
  {
    OB_ASSERT(BUCKETS_CNT > 0);
  }
  ~ObSimpleHashMap() { destroy(); }
  int64_t count() const { return ATOMIC_LOAD(&total_cnt_); }
  int init()
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      TRANS_LOG(WARN, "ObSimpleHashMap init twice");
      ret = OB_INIT_TWICE;
    } else {
      is_inited_ = true;
    }
    return ret;
  }

  void reset()
  {
    if (is_inited_) {
      Value *curr = nullptr;
      Value *next = nullptr;
      for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
        curr = buckets_[i].next_;
        while (OB_NOT_NULL(curr)) {
          next = curr->next_;
          del_from_bucket_(i, curr);
          // dec ref and free curr value
          curr = next;
        }
        // reset bucket
        buckets_[i].reset();
      }
      total_cnt_ = 0;
      is_inited_ = false;
    }
  }

  void destroy() { reset(); }

  int insert_and_get(const Key &key, const Value &value, Value **old_value)
  { return insert_(key, value, old_value); }
  int insert(const Key &key, const Value &value)
  { return insert_(key, value, NULL); }
  int insert_(const Key &key, const Value &value, Value **old_value)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_val = 0;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObSimpleHashMap not init", K(ret));
    } else if (OB_FAIL(hashfunc_(key, hash_val))) {
      TRANS_LOG(WARN, "do hash failed", K(ret));
    } else {
      uint64_t pos = hash_val % BUCKETS_CNT;
      Value *curr = buckets_[pos].next_;
      while (OB_NOT_NULL(curr)) {
        if (curr->contain(key)) {
          break;
        } else {
          curr = curr->next_;
        }
      }
      if (OB_ISNULL(curr)) {
        Value *new_value = NULL;
        if (NULL == (new_value = alloc_handle_.alloc_value())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          new_value->assign(value);
          if (NULL != buckets_[pos].next_) {
            buckets_[pos].next_->prev_ = new_value;
          }
          new_value->next_ = buckets_[pos].next_;
          new_value->prev_ = NULL;
          buckets_[pos].next_ = new_value;
          ATOMIC_INC(&total_cnt_);
        }
      } else {
        ret = OB_ENTRY_EXIST;
        if (old_value) {
          *old_value = curr;
        }
      }
    }
    return ret;
  }

  int del(const Key &key)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_val = 0;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObTransHashMap not init", K(ret));
    } else if (OB_FAIL(hashfunc_(key, hash_val))) {
      TRANS_LOG(WARN, "do hash failed", K(ret));
    } else {
      int64_t pos = hash_val % BUCKETS_CNT;
      Value *curr = buckets_[pos].next_;
      while (OB_NOT_NULL(curr)) {
        if (curr->contain(key)) {
          break;
        } else {
          curr = curr->next_;
        }
      }
      if (OB_ISNULL(curr)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        del_from_bucket_(pos, curr);
      }
    }
    return ret;
  }

  int get(const Key &key, Value *&value)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_val = 0;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObTransHashMap not init", K(ret), K(key));
    } else if (OB_FAIL(hashfunc_(key, hash_val))) {
      TRANS_LOG(WARN, "do hash failed", K(ret));
    } else {
      Value *tmp_value = NULL;
      int64_t pos = hash_val % BUCKETS_CNT;

      tmp_value = buckets_[pos].next_;
      while (OB_NOT_NULL(tmp_value)) {
        if (tmp_value->contain(key)) {
          value = tmp_value;
          break;
        } else {
          tmp_value = tmp_value->next_;
        }
      }

      if (OB_ISNULL(tmp_value)) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }

  class ObEncryptIterator
  {
  public:
    ObEncryptIterator(const ObSimpleHashMap* hm, int64_t bucket_pos, Value *node) :
        hm_(hm), bucket_pos_(bucket_pos), node_(node) {}
    Value* operator ->() const
    {
      Value *p = NULL;
      if (OB_ISNULL(node_)) {
        HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "node is null, backtrace=%s", lbt());
      } else {
        p = node_;
      }
      return p;
    }
    ObEncryptIterator &operator ++()
    {
      if (OB_ISNULL(hm_)) {
        HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "hashmap is null, backtrace=%s", lbt());
      } else if (NULL != node_ && NULL != (node_ = node_->next_)) {
        // do nothing
      } else {
        for (int64_t i = bucket_pos_ + 1; i < BUCKETS_CNT; i++) {
          if (NULL != (node_ = hm_->buckets_[i].next_)) {
            bucket_pos_ = i;
            break;
          }
        }
        if (NULL == node_) {
          bucket_pos_ = BUCKETS_CNT;
        }
      }
      return *this;
    }

    bool operator !=(const ObEncryptIterator &iter) const
    {
      return node_ != iter.node_;
    }
  private:
    //for iterator
    const ObSimpleHashMap *hm_;
    int64_t bucket_pos_;
    Value *node_;
  };

  ObEncryptIterator begin()
  {
    int64_t bucket_pos = 0;
    Value *node = NULL;
    while (NULL == node && bucket_pos < BUCKETS_CNT) {
      node = buckets_[bucket_pos].next_;
      if (NULL == node) {
        ++bucket_pos;
      }
    }
    return ObEncryptIterator(this, bucket_pos, node);
  }

  ObEncryptIterator end()
  {
    return ObEncryptIterator(this, BUCKETS_CNT, NULL);
  }

private:
  void del_from_bucket_(const int64_t pos, Value *curr)
  {
    if (curr == buckets_[pos].next_) {
      if (NULL == curr->next_) {
        buckets_[pos].next_ = NULL;
      } else {
        buckets_[pos].next_ = curr->next_;
        curr->next_->prev_ = curr->prev_;
      }
    } else {
      curr->prev_->next_ = curr->next_;
      if (NULL != curr->next_) {
        curr->next_->prev_ = curr->prev_;
      }
    }
    curr->prev_ = NULL;
    curr->next_ = NULL;
    alloc_handle_.free_value(curr);
    ATOMIC_DEC(&total_cnt_);
  }

private:
  struct ObSimpleHashHeader
  {
    Value *next_;
    ObSimpleHashHeader() : next_(NULL) {}
    ~ObSimpleHashHeader() { destroy(); }
    //maybe we can add init() in the future
    void reset()
    {
      next_ = NULL;
    }
    void destroy()
    {
      reset();
    }
  };
private:
  bool is_inited_;
  ObSimpleHashHeader buckets_[BUCKETS_CNT];
  int64_t total_cnt_;
  AllocHandle alloc_handle_;
  _hashfunc hashfunc_;
};

typedef ObSimpleHashMap<uint64_t, ObTxEncryptMeta, ObEncryptMetaAlloc> ObTxEncryptMap;

class ObCLogEncryptInfo
{
public:
  ObCLogEncryptInfo() : encrypt_map_(nullptr),
                        cached_encrypt_meta_(nullptr),
                        cached_table_id_(OB_INVALID_ID),
                        is_inited_(false) {}
  ~ObCLogEncryptInfo() { destroy(); }
  int init();
  bool is_inited() const { return is_inited_; }
  virtual void destroy();
  bool is_valid() const;
  void reset();
  int get_encrypt_info(const uint64_t table_id, ObTxEncryptMeta *&meta) const;
  int store(uint64_t table_id, const share::ObEncryptMeta &meta);
  int store_and_get_old(const uint64_t table_id, const ObTxEncryptMeta &new_meta, ObTxEncryptMeta **old_meta);
  int remove(uint64_t table_id);
  int add_clog_encrypt_info(const ObCLogEncryptInfo &rhs);
  ObTxEncryptMap* get_info() { return encrypt_map_; }
  bool has_encrypt_meta() const;
  int replace_tenant_id(const uint64_t real_tenant_id);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int decrypt_table_key();
  OB_UNIS_VERSION(1);
private:
  int init_encrypt_meta_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObCLogEncryptInfo);
private:
  ObTxEncryptMap *encrypt_map_;
  mutable ObTxEncryptMeta *cached_encrypt_meta_;
  mutable uint64_t cached_table_id_;
  bool is_inited_;
};

}//transaction
}//oceanbase

#endif
