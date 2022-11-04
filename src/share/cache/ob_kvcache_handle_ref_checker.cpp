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

#include "share/cache/ob_kvcache_handle_ref_checker.h"
#include "share/cache/ob_kv_storecache.h"


namespace oceanbase
{
namespace common
{

/*
 * ---------------------------------------------- ObKVCacheHandleRefChecker::Key ----------------------------------------------
 */

ObKVCacheHandleRefChecker::Key::Key()
  : handle_(nullptr)
{
}

ObKVCacheHandleRefChecker::Key::Key(const ObKVCacheHandle &cache_handle)
  : handle_(&cache_handle)
{
}

uint64_t ObKVCacheHandleRefChecker::Key::hash() const
{
  uint64_t hash = 0;
  if (nullptr != handle_) {
    hash = murmurhash(&handle_, sizeof(handle_), hash);
  }
  return hash;
}

bool ObKVCacheHandleRefChecker::Key::operator== (const Key &other) const
{
  return handle_ == other.handle_;
}

/*
 * ---------------------------------------------- ObKVCacheHandleRefChecker::Value ----------------------------------------------
 */

ObKVCacheHandleRefChecker::Value::Value()
  : tenant_id_(OB_INVALID_TENANT_ID),
    cache_id_(INVALID_CACHE_ID),
    bt_()
{
  MEMSET(bt_, 0, sizeof(bt_));
}

ObKVCacheHandleRefChecker::Value::Value(const Value &other)
{
  *this = other;
}

uint64_t ObKVCacheHandleRefChecker::Value::hash() const
{
  uint64_t hash = 0;
  hash = murmurhash(bt_, sizeof(bt_), hash);
  return hash;
}

bool ObKVCacheHandleRefChecker::Value::operator== (const Value &other) const
{
  return (tenant_id_ == other.tenant_id_) && (cache_id_ == other.cache_id_) && (0 == STRNCMP(bt_, other.bt_, sizeof(bt_)));
}

ObKVCacheHandleRefChecker::Value & ObKVCacheHandleRefChecker::Value::operator= (const Value &other)
{
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    cache_id_ = other.cache_id_;
    MEMCPY(bt_, other.bt_, sizeof(bt_));
  }
  return *this;
}

/*
 * ---------------------------------------------- ObKVCacheHandleRefChecker ----------------------------------------------
 */

const char ObKVCacheHandleRefChecker::ALL_CACHE_NAME[MAX_CACHE_NAME_LENGTH] = "all_cache";

ObKVCacheHandleRefChecker::ObKVCacheHandleRefChecker()
  : cache_id_(INVALID_CACHE_ID),
    handle_ref_bt_info_(),
    is_inited_(false)
{
  INIT_SUCC(ret);
  if (OB_FAIL(handle_ref_bt_info_.create(HANDLE_BT_MAP_BUCKET_NUM, "CACHE_CHECKER_M", "CACHE_CHECKER_M"))) {
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail to create handle ref info", K(ret));
  } else {
    is_inited_ = true;
  }
}

ObKVCacheHandleRefChecker::~ObKVCacheHandleRefChecker()
{
  reset();
}

ObKVCacheHandleRefChecker &ObKVCacheHandleRefChecker::get_instance()
{
  static ObKVCacheHandleRefChecker cache_handle_ref_checker_;
  return cache_handle_ref_checker_;
}

void ObKVCacheHandleRefChecker::reset()
{
  cache_id_ = INVALID_CACHE_ID;
  handle_ref_bt_info_.reuse();
  is_inited_ = false;
}

void ObKVCacheHandleRefChecker::handle_ref_inc(const ObKVCacheHandle &cache_handle)
{
  INIT_SUCC(ret);
  Key key(cache_handle);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] The ObKVCaheHandleRefChecker is not inited", K(ret));
  } else if (INVALID_CACHE_ID == cache_id_ || !key.is_valid() || nullptr == cache_handle.mb_handle_
      || (cache_id_ != MAX_CACHE_NUM && cache_id_ != cache_handle.mb_handle_->inst_->cache_id_)) {  // TODO: @lvling filter tenant id
  } else {
    Value value;
    value.tenant_id_ = cache_handle.mb_handle_->inst_->tenant_id_;
    value.cache_id_= cache_handle.mb_handle_->inst_->cache_id_;
    lbt(value.bt_, sizeof(value.bt_));
    if (OB_FAIL(handle_ref_bt_info_.set_refactored(key, value))) {
      COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail to record handle ref backtrace info", K(ret), K(key), K(value));
    }
  }
}

void ObKVCacheHandleRefChecker::handle_ref_de(const ObKVCacheHandle &cache_handle)
{
  INIT_SUCC(ret);
  Key key(cache_handle);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] The ObKVCacheHandleRefChecker is not inited", K(ret));
  } else if (!key.is_valid() || nullptr == cache_handle.mb_handle_ || INVALID_CACHE_ID == cache_id_) {
  } else {
    if (OB_FAIL(handle_ref_bt_info_.erase_refactored(key))) {
      if (OB_HASH_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail to erase handle ref backtrace info", K(ret), K(key), KPC(cache_handle.mb_handle_));
      }
    }
  }
}

int ObKVCacheHandleRefChecker::set_cache_id(const int64_t cache_id)
{
  INIT_SUCC(ret);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] The ObKVCacheHandleRefChecker is not inited", K(ret));
  } else if (cache_id < INVALID_CACHE_ID || cache_id > MAX_CACHE_NUM) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Invalid argument", K(ret), K(cache_id));
  } else {
    cache_id_ = cache_id;
    handle_ref_bt_info_.reuse();
  }
  return ret;
}

int ObKVCacheHandleRefChecker::get_aggregate_bt_info(hash::ObHashMap<Value, int64_t> &bt_info)
{
  INIT_SUCC(ret);
  bt_info.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] The ObKVCacheHanleRefChecker is not inited", K(ret));
  } else if (INVALID_CACHE_ID == cache_id_) {
  } else {
    for (hash::ObHashMap<Key, Value>::bucket_iterator bucket_iter = handle_ref_bt_info_.bucket_begin() ; 
        OB_SUCC(ret) && bucket_iter != handle_ref_bt_info_.bucket_end() ; ++bucket_iter) {
      hash::ObHashMap<Key, Value>::hashtable::bucket_lock_cond blk(*bucket_iter);
      hash::ObHashMap<Key, Value>::hashtable::readlocker locker(blk.lock());
      for (hash::ObHashMap<Key, Value>::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin() ; 
          OB_SUCC(ret) && node_iter != bucket_iter->node_end() ; ++node_iter) {
        int64_t bt_count = 0;
        if (OB_FAIL(bt_info.get_refactored(node_iter->second, bt_count))) {
          if (OB_HASH_NOT_EXIST == ret) {
            bt_count = 1;
            if (OB_FAIL(bt_info.set_refactored(node_iter->second, bt_count))) {
              COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail to set aggregated info", K(ret));
            }
          } else {
            COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail to get aggregated info", K(ret), K(node_iter->second));
          }
        } else if (OB_FAIL(bt_info.set_refactored(node_iter->second, bt_count+1, 1, 0, 1))) {
          COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail wo update aggregated info", K(ret), K(node_iter->second), K(bt_count));
        }
      }
    }
  }
  return ret;
}


}  // common
}  // oceanbase