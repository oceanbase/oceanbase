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

#include "ob_kvcache_map.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase {
namespace common {
ObKVCacheMap::ObKVCacheMap() : is_inited_(false), bucket_num_(0), buckets_(NULL), store_(NULL)
{
  bucket_allocator_.set_label(ObNewModIds::OB_KVSTORE_CACHE);
}

ObKVCacheMap::~ObKVCacheMap()
{}

int ObKVCacheMap::init(const int64_t bucket_num, ObKVCacheStore* store)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCacheMap has been inited, ", K(ret));
  } else if (0 >= bucket_num || NULL == store) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments, ", K(bucket_num), K(store), K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::KV_CACHE_BUCKET_LOCK, ObNewModIds::OB_KVSTORE_CACHE))) {
    COMMON_LOG(WARN, "Fail to init bucket lock, ", K(bucket_num), K(ret));
  } else {
    const int64_t bucket_cnt =
        bucket_num % Bucket::BUCKET_SIZE == 0 ? bucket_num / Bucket::BUCKET_SIZE : bucket_num / Bucket::BUCKET_SIZE + 1;
    if (OB_ISNULL(buckets_ = static_cast<Bucket**>(bucket_allocator_.alloc(sizeof(Bucket*) * bucket_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to allocate bucket array", K(ret), K(bucket_cnt));
    } else {
      Bucket* bucket = NULL;
      MEMSET(buckets_, 0, sizeof(Bucket*) * bucket_cnt);
      for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt; ++i) {
        if (OB_ISNULL(bucket = static_cast<Bucket*>(bucket_allocator_.alloc(sizeof(Bucket))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "failed to allocate bucket", K(ret), K(i), K(bucket_cnt));
        } else {
          memset(bucket, 0, sizeof(Bucket));
          buckets_[i] = bucket;
        }
      }
    }
    if (OB_SUCC(ret)) {
      bucket_num_ = bucket_num;
      store_ = store;
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObKVCacheMap::destroy()
{
  if (NULL != buckets_) {
    if (is_inited_) {
      for (int64_t i = 0; i < bucket_num_; i++) {
        Node*& iter = get_bucket_node(i);
        while (iter != NULL) {
          Node* tmp = iter;
          iter = iter->next_;
          tmp->inst_->node_allocator_.free(tmp);
          tmp = NULL;
        }
        iter = NULL;
      }
    }
    const int64_t bucket_cnt = bucket_num_ % Bucket::BUCKET_SIZE == 0 ? bucket_num_ / Bucket::BUCKET_SIZE
                                                                      : bucket_num_ / Bucket::BUCKET_SIZE + 1;
    for (int64_t i = 0; i < bucket_cnt; ++i) {
      if (NULL != buckets_[i]) {
        bucket_allocator_.free(buckets_[i]);
        buckets_[i] = NULL;
      }
    }
    bucket_allocator_.free(buckets_);
    buckets_ = NULL;
  }
  bucket_lock_.destroy();
  bucket_num_ = 0;
  store_ = NULL;
  is_inited_ = false;
}

int ObKVCacheMap::put(ObKVCacheInst& inst, const ObIKVCacheKey& key, const ObKVCachePair* kvpair,
    ObKVMemBlockHandle* mb_handle, bool overwrite)
{
  int ret = OB_SUCCESS;

  Node* insert_node = NULL;
  Node* iter = NULL;
  Node* prev = NULL;
  bool is_overwrite = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(NULL == kvpair) || OB_UNLIKELY(NULL == mb_handle)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(kvpair), KP(mb_handle), K(ret));
  } else {
    uint64_t hash_code = key.hash() + inst.cache_id_;
    uint64_t bucket_pos = hash_code % bucket_num_;

    ObBucketWLockGuard guard(bucket_lock_, bucket_pos);
    Node*& bucket_ptr = get_bucket_node(bucket_pos);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(WARN, "Fail to lock bucket, ", K(bucket_pos), K(ret));
    } else {
      if (NULL != (iter = bucket_ptr)) {
        while (NULL != iter && OB_SUCC(ret)) {
          if (!store_->add_handle_ref(iter->mb_handle_, iter->seq_num_)) {
            // remove expired kv-pairs
            internal_map_erase(prev, iter, bucket_pos);
          } else {
            // fragment node collection
            if (iter->inst_->node_allocator_.is_fragment(iter)) {
              internal_map_replace(prev, iter, bucket_pos);
            }

            if (hash_code == iter->hash_code_ && key == *(iter->key_)) {
              // found the same key
              if (overwrite) {
                (void)ATOMIC_SAF(&iter->mb_handle_->kv_cnt_, 1);
                (void)ATOMIC_SAF(&iter->mb_handle_->get_cnt_, iter->get_cnt_);
                insert_node = iter;
                is_overwrite = true;
              } else {
                ret = OB_ENTRY_EXIST;
              }
              store_->de_handle_ref(iter->mb_handle_);
              break;
            }

            store_->de_handle_ref(iter->mb_handle_);
            prev = iter;
            iter = iter->next_;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (NULL == insert_node) {
          void* buf = NULL;
          if (NULL == (buf = inst.node_allocator_.alloc(sizeof(Node)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            COMMON_LOG(ERROR, "Fail to allocate memory, ", K(ret), "size", sizeof(Node));
          } else {
            insert_node = new (buf) Node();
            if (NULL == prev) {
              bucket_ptr = insert_node;
            }
            insert_node->next_ = NULL;
          }
        }

        if (OB_SUCC(ret)) {
          if (prev != NULL) {
            prev->next_ = insert_node;
          }

          insert_node->inst_ = &inst;
          insert_node->get_cnt_++;
          insert_node->mb_handle_ = mb_handle;
          insert_node->key_ = kvpair->key_;
          insert_node->value_ = kvpair->value_;
          insert_node->hash_code_ = hash_code;
          insert_node->seq_num_ = mb_handle->handle_ref_.get_seq_num();

          (void)ATOMIC_AAF(&mb_handle->kv_cnt_, 1);
          (void)ATOMIC_AAF(&mb_handle->get_cnt_, 1);
          (void)ATOMIC_AAF(&mb_handle->recent_get_cnt_, 1);

          if (!is_overwrite) {
            (void)ATOMIC_AAF(&inst.status_.kv_cnt_, 1);
          }
          inst.status_.total_put_cnt_.inc();
        }
      }
    }
  }

  return ret;
}

int ObKVCacheMap::get(
    const int64_t cache_id, const ObIKVCacheKey& key, const ObIKVCacheValue*& pvalue, ObKVMemBlockHandle*& out_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    const uint64_t tenant_id = key.get_tenant_id();
    bool need_modify = false;
    uint64_t hash_code = key.hash() + cache_id;
    uint64_t bucket_pos = hash_code % bucket_num_;
    Node* iter = NULL;
    Node* prev = NULL;

    // add read lock and read
    {
      ObBucketRLockGuard rd_guard(bucket_lock_, bucket_pos);
      if (OB_FAIL(rd_guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(bucket_pos), K(ret));
      } else {
        if (NULL != (iter = get_bucket_node(bucket_pos))) {

          while (NULL != iter && OB_SUCC(ret)) {
            if (!store_->add_handle_ref(iter->mb_handle_, iter->seq_num_)) {
              // garbage node
              need_modify = true;
            } else {
              if (hash_code == iter->hash_code_ && key == *(iter->key_)) {
                break;
              }
              // The handle ref must be deref
              store_->de_handle_ref(iter->mb_handle_);
            }
            iter = iter->next_;
          }

          if (NULL == iter) {
            ret = OB_ENTRY_NOT_EXIST;
          }

          if (OB_SUCC(ret)) {
            pvalue = iter->value_;
            out_handle = iter->mb_handle_;
            if (LRU == out_handle->policy_) {
              need_modify = need_modify_cache(iter->get_cnt_, out_handle->get_cnt_, out_handle->kv_cnt_);
            }
            out_handle->get_cnt_++;
            out_handle->recent_get_cnt_++;
            iter->get_cnt_++;
          }
        } else {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
    }

    if (OB_SUCC(ret)) {
      out_handle->inst_->status_.total_hit_cnt_.inc();
      if (need_modify) {
        // need add write lock and do some modification
        ObBucketWLockGuard wr_guard(bucket_lock_, bucket_pos);
        if (OB_FAIL(wr_guard.get_ret())) {
          COMMON_LOG(WARN, "Fail to add write lock, ", K(wr_guard.get_ret()));
        } else {
          if (NULL != (iter = get_bucket_node(bucket_pos))) {
            prev = NULL;
            Node* move_node = NULL;

            while (NULL != iter && OB_SUCC(ret)) {
              if (!store_->add_handle_ref(iter->mb_handle_, iter->seq_num_)) {
                // remove expire node
                internal_map_erase(prev, iter, bucket_pos);
              } else {
                // fragment node collection
                if (iter->inst_->node_allocator_.is_fragment(iter)) {
                  internal_map_replace(prev, iter, bucket_pos);
                }

                if (iter->mb_handle_ == out_handle && LRU == out_handle->policy_) {
                  if (hash_code == iter->hash_code_ && key == *(iter->key_)) {
                    move_node = iter;
                  }
                }

                // The handle ref must be deref
                store_->de_handle_ref(iter->mb_handle_);
                prev = iter;
                iter = iter->next_;
              }
            }

            if (OB_NOT_NULL(move_node)) {
              internal_data_move(move_node, LFU);
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObKVCacheMap::erase(ObKVCacheInst& inst, const ObIKVCacheKey& key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!inst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(ret));
  } else {
    bool found = false;
    uint64_t hash_code = key.hash() + inst.cache_id_;
    uint64_t bucket_pos = hash_code % bucket_num_;
    ObBucketWLockGuard guard(bucket_lock_, bucket_pos);

    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(WARN, "Fail to lock bucket, ", K(bucket_pos), K(ret));
    } else {
      Node* iter = get_bucket_node(bucket_pos);
      if (NULL != iter) {
        Node* prev = NULL;

        while (NULL != iter && OB_SUCC(ret)) {
          if (!store_->add_handle_ref(iter->mb_handle_, iter->seq_num_)) {
            internal_map_erase(prev, iter, bucket_pos);
          } else {
            // fragment node collection
            if (iter->inst_->node_allocator_.is_fragment(iter)) {
              internal_map_replace(prev, iter, bucket_pos);
            }

            if (&inst == iter->inst_ && key == *(iter->key_)) {
              // found the key
              ObKVMemBlockHandle* mb_handle = iter->mb_handle_;
              (void)ATOMIC_SAF(&mb_handle->kv_cnt_, 1);
              (void)ATOMIC_SAF(&mb_handle->get_cnt_, iter->get_cnt_);

              store_->de_handle_ref(iter->mb_handle_);
              internal_map_erase(prev, iter, bucket_pos);
              found = true;
              break;
            } else {
              store_->de_handle_ref(iter->mb_handle_);
              prev = iter;
              iter = iter->next_;
            }
          }
        }

        if (!found) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }

  return ret;
}

int ObKVCacheMap::erase_all()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); i++) {
      ObBucketWLockGuard guard(bucket_lock_, i);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(i), K(ret));
      } else {
        Node*& bucket_ptr = get_bucket_node(i);
        Node* iter = get_bucket_node(i);
        while (NULL != iter) {
          Node* tmp = iter;
          ObKVCacheInst* inst = iter->inst_;
          iter = iter->next_;
          tmp->inst_->node_allocator_.free(tmp);
          if (NULL != inst) {
            (void)ATOMIC_SAF(&inst->status_.kv_cnt_, 1);
          }
          tmp = NULL;
        }
        bucket_ptr = NULL;
      }
    }
  }
  return ret;
}

int ObKVCacheMap::erase_all(const int64_t cache_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument ", K(cache_id), K(ret));
  } else {
    for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); i++) {
      ObBucketWLockGuard guard(bucket_lock_, i);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(i), K(ret));
      } else {
        Node* iter = get_bucket_node(i);
        if (NULL != iter) {
          Node* prev = NULL;
          while (NULL != iter) {
            if (iter->inst_->cache_id_ == cache_id) {
              internal_map_erase(prev, iter, i);
            } else {
              prev = iter;
              iter = iter->next_;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObKVCacheMap::erase_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); i++) {
      ObBucketWLockGuard guard(bucket_lock_, i);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(i), K(ret));
      } else {
        Node* iter = get_bucket_node(i);
        if (NULL != iter) {
          Node* prev = NULL;
          while (NULL != iter) {
            if (iter->inst_->tenant_id_ == tenant_id) {
              internal_map_erase(prev, iter, i);
            } else {
              prev = iter;
              iter = iter->next_;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObKVCacheMap::erase_tenant_cache(const uint64_t tenant_id, const int64_t cache_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument ", K(cache_id), K(ret));
  } else {
    for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); i++) {
      ObBucketWLockGuard guard(bucket_lock_, i);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(i), K(ret));
      } else {
        Node* iter = get_bucket_node(i);
        if (NULL != iter) {
          Node* prev = NULL;
          while (NULL != iter) {
            if (iter->inst_->tenant_id_ == tenant_id && iter->inst_->cache_id_ == cache_id) {
              internal_map_erase(prev, iter, i);
            } else {
              prev = iter;
              iter = iter->next_;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObKVCacheMap::clean_garbage_node(int64_t& start_pos, const int64_t clean_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(start_pos < 0) || OB_UNLIKELY(start_pos >= bucket_num_) || OB_UNLIKELY(clean_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(start_pos), K_(bucket_num), K(clean_num), K(ret));
  } else {
    int64_t clean_start_pos = start_pos % bucket_num_;
    int64_t clean_end_pos = MIN(clean_num + clean_start_pos, bucket_num_);
    for (int64_t i = clean_start_pos; i < clean_end_pos && OB_SUCC(ret); i++) {
      ObBucketWLockGuard guard(bucket_lock_, i);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(i), K(ret));
      } else {
        Node* iter = get_bucket_node(i);
        if (NULL != iter) {
          Node* prev = NULL;
          while (NULL != iter) {
            if (!store_->add_handle_ref(iter->mb_handle_, iter->seq_num_)) {
              internal_map_erase(prev, iter, i);
            } else {
              store_->de_handle_ref(iter->mb_handle_);
              // don't replace in wash task, put it in single task
              // if (iter->inst_->node_allocator_.is_fragment(iter)) {
              //  internal_map_replace(prev, iter, i);
              //}
              prev = iter;
              iter = iter->next_;
            }
          }
        }
      }
    }

    start_pos = clean_end_pos >= bucket_num_ ? 0 : clean_end_pos;
  }

  return ret;
}

int ObKVCacheMap::replace_fragment_node(int64_t& start_pos, const int64_t replace_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(start_pos < 0) || OB_UNLIKELY(start_pos >= bucket_num_) || OB_UNLIKELY(replace_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(start_pos), K_(bucket_num), K(replace_num), K(ret));
  } else {
    int64_t replace_start_pos = start_pos % bucket_num_;
    int64_t replace_end_pos = MIN(replace_num + replace_start_pos, bucket_num_);
    for (int64_t i = replace_start_pos; i < replace_end_pos && OB_SUCC(ret); i++) {
      ObBucketWLockGuard guard(bucket_lock_, i);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "Fail to lock bucket, ", K(i), K(ret));
      } else {
        Node* iter = get_bucket_node(i);
        if (NULL != iter) {
          Node* prev = NULL;
          while (NULL != iter) {
            if (iter->inst_->node_allocator_.is_fragment(iter)) {
              internal_map_replace(prev, iter, i);
            }
            prev = iter;
            iter = iter->next_;
          }
        }
      }
    }

    start_pos = replace_end_pos >= bucket_num_ ? 0 : replace_end_pos;
  }
  return ret;
}

int ObKVCacheMap::multi_get(
    const int64_t cache_id, const int64_t pos, common::ObList<Node, common::ObArenaAllocator>& list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos >= bucket_num_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), K(pos), K_(bucket_num), K(ret));
  } else {
    ObBucketWLockGuard guard(bucket_lock_, pos);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(WARN, "Fail to lock bucket, ", K(pos), K(ret));
    } else {
      Node* iter = get_bucket_node(pos);
      if (NULL != iter) {
        Node* prev = NULL;
        while (NULL != iter) {
          if (!store_->add_handle_ref(iter->mb_handle_, iter->seq_num_)) {
            internal_map_erase(prev, iter, pos);
            if (NULL == iter) {
              break;
            }
          } else {
            if (cache_id == iter->inst_->cache_id_) {
              list.push_back(*iter);
            }
            store_->de_handle_ref(iter->mb_handle_);
            if (iter->inst_->node_allocator_.is_fragment(iter)) {
              internal_map_replace(prev, iter, pos);
            }

            prev = iter;
            iter = iter->next_;
          }
        }
      }
    }
  }

  return ret;
}

void ObKVCacheMap::internal_map_erase(Node*& prev, Node*& iter, const uint64_t bucket_pos)
{
  if (NULL != iter) {
    ObKVCacheInst* inst = iter->inst_;
    if (NULL == prev) {
      Node*& bucket_ptr = get_bucket_node(bucket_pos);
      if (NULL == iter->next_) {
        bucket_ptr->inst_->node_allocator_.free(bucket_ptr);
        bucket_ptr = NULL;
        iter = NULL;
      } else {
        bucket_ptr = iter->next_;
        iter->inst_->node_allocator_.free(iter);
        iter = bucket_ptr;
      }
    } else {
      prev->next_ = iter->next_;
      iter->inst_->node_allocator_.free(iter);
      iter = prev->next_;
    }

    if (NULL != inst) {
      (void)ATOMIC_SAF(&inst->status_.kv_cnt_, 1);
    }
  }
}

void ObKVCacheMap::internal_map_replace(Node*& prev, Node*& iter, const uint64_t bucket_pos)
{
  if (NULL != iter) {
    Node* node = NULL;
    void* buf = NULL;
    if (NULL != (buf = iter->inst_->node_allocator_.alloc(sizeof(Node)))) {
      node = new (buf) Node();
      *node = *iter;
      if (NULL == prev) {
        get_bucket_node(bucket_pos) = node;
      } else {
        prev->next_ = node;
      }
      iter->inst_->node_allocator_.free(iter);
      iter = node;
    }
  }
}

void ObKVCacheMap::internal_data_move(Node* iter, const enum ObKVCachePolicy policy)
{
  const ObIKVCacheKey* old_key = iter->key_;
  const ObIKVCacheValue* old_value = iter->value_;
  ObKVCachePair* new_kvpair = NULL;
  ObKVMemBlockHandle* mb_handle = iter->mb_handle_;
  ObKVMemBlockHandle* new_mb_handle = NULL;

  if (NULL != old_key && NULL != old_value) {
    if (OB_SUCCESS == store_->store(*iter->inst_, *old_key, *old_value, new_kvpair, new_mb_handle, policy)) {
      (void)ATOMIC_SAF(&mb_handle->kv_cnt_, 1);
      (void)ATOMIC_SAF(&mb_handle->get_cnt_, iter->get_cnt_);

      (void)ATOMIC_AAF(&new_mb_handle->kv_cnt_, 1);
      (void)ATOMIC_AAF(&new_mb_handle->get_cnt_, iter->get_cnt_);
      (void)ATOMIC_AAF(&new_mb_handle->recent_get_cnt_, 1);
      iter->mb_handle_ = new_mb_handle;
      iter->key_ = new_kvpair->key_;
      iter->value_ = new_kvpair->value_;
      iter->seq_num_ = new_mb_handle->handle_ref_.get_seq_num();
      // dec ref of old handle since we already inc ref outside
      store_->de_handle_ref(iter->mb_handle_);
    }
  }
}

ObKVCacheMap::Node*& ObKVCacheMap::get_bucket_node(const int64_t idx)
{
  const int64_t bucket_idx = idx / Bucket::BUCKET_SIZE;
  return buckets_[bucket_idx]->nodes_[idx & (Bucket::BUCKET_SIZE - 1)];
}

}  // end namespace common
}  // end namespace oceanbase
