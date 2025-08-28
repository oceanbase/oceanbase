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
#include "ob_storage_leak_checker.h"
#include "lib/utility/ob_sort.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/rc/ob_tenant_base.h"
#include "share/io/ob_io_define.h"


namespace oceanbase
{
namespace storage
{

/*
 * ---------------------------------------------- ObStorageCheckerKey ----------------------------------------------
 */
ObStorageCheckerKey::ObStorageCheckerKey()
  : handle_(nullptr)
{
}

ObStorageCheckerKey::ObStorageCheckerKey(const void *handle)
  : handle_(handle)
{
}

int ObStorageCheckerKey::hash(uint64_t &hash_value) const
{
  int ret = OB_SUCCESS;
  hash_value = 0;
  if (nullptr != handle_) {
    hash_value = murmurhash(&handle_, sizeof(handle_), hash_value);
  } else {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "[STORAGE-CHECKER] handle is null, cannot get hash value", K(ret));
  }
  return ret;
}

bool ObStorageCheckerKey::operator== (const ObStorageCheckerKey &other) const
{
  return handle_ == other.handle_;
}


/*
 * ---------------------------------------------- ObStorageCheckerValue ----------------------------------------------
 */
ObStorageCheckerValue::ObStorageCheckerValue()
  : tenant_id_(OB_INVALID_TENANT_ID),
    check_id_(ObStorageCheckID::INVALID_ID),
    bt_()
{
}

ObStorageCheckerValue::ObStorageCheckerValue(const ObStorageCheckerValue &other)
{
  *this = other;
}

int ObStorageCheckerValue::hash(uint64_t &hash_value) const
{
  int ret = OB_SUCCESS;
  hash_value = murmurhash(bt_, sizeof(bt_), hash_value);
  return ret;
}

bool ObStorageCheckerValue::operator== (const ObStorageCheckerValue &other) const
{
  return tenant_id_ == other.tenant_id_
      && check_id_ == other.check_id_
      && 0 == STRNCMP(bt_, other.bt_, sizeof(bt_));
}

ObStorageCheckerValue & ObStorageCheckerValue::operator= (const ObStorageCheckerValue &other)
{
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    check_id_ = other.check_id_;
    MEMCPY(bt_, other.bt_, sizeof(bt_));
  }
  return *this;
}


/*
 * ---------------------------------------------- ObStorageLeakChecker ----------------------------------------------
 */
const char ObStorageLeakChecker::ALL_CACHE_NAME[MAX_CACHE_NAME_LENGTH] = "all_cache";
const char ObStorageLeakChecker::IO_HANDLE_CHECKER_NAME[MAX_CACHE_NAME_LENGTH] = "io_handle";
const char ObStorageLeakChecker::ITER_CHECKER_NAME[MAX_CACHE_NAME_LENGTH] = "storage_iter";
ObStorageLeakChecker ObStorageLeakChecker::instance_;

ObStorageLeakChecker::ObStorageLeakChecker()
    : checker_info_()
{
  INIT_SUCC(ret);
  if (OB_FAIL(checker_info_.create(HANDLE_BT_MAP_BUCKET_NUM, "STRG_CHECKER_M", "STRG_CHECKER_M"))) {
    COMMON_LOG(WARN, "[STORAGE-CHECKER] Fail to create handle ref info", K(ret));
  }
}

ObStorageLeakChecker::~ObStorageLeakChecker()
{
  reset();
}

void ObStorageLeakChecker::reset()
{
  checker_info_.reuse();
}

OB_NOINLINE void ObStorageLeakChecker::inner_handle_hold(
    ObStorageCheckedObjectBase* handle, const ObStorageCheckID check_id)
{
  INIT_SUCC(ret);
  ObStorageCheckerKey key(handle);
  ObStorageCheckerValue value;
  if (OB_UNLIKELY(OB_ISNULL(handle) || !is_valid_check_id(check_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "[STORAGE-CHECKER] Invalid argument", K(ret), K(handle), K(check_id));
  } else if (checker_info_.size() > MAP_SIZE_LIMIT) {
  } else {
    value.check_id_ = check_id;
    value.tenant_id_ = MTL_ID();
    lbt(value.bt_, sizeof(value.bt_));
    if (OB_FAIL(checker_info_.set_refactored(key, value))) {
      COMMON_LOG(WARN, "[STORAGE-CHECKER] Fail to record backtrace", K(ret), K(key), K(value));
    } else {
      handle->is_traced_ = true;
    }
  } 
  COMMON_LOG(DEBUG, "[STORAGE-CHECKER] handle hold details", K(ret), K(check_id),
             KP(handle), K(check_id), K(key), K(value));
}

OB_NOINLINE void ObStorageLeakChecker::inner_handle_reset(
    ObStorageCheckedObjectBase* handle, const ObStorageCheckID check_id)
{
  INIT_SUCC(ret);
  ObStorageCheckerKey key(handle);
  if (OB_UNLIKELY(nullptr == handle || !is_valid_check_id(check_id))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "[STORAGE-CHECKER] Invalid argument", K(ret), KP(handle), K(check_id));
  } else {
    if (OB_FAIL(checker_info_.erase_refactored(key))) {
      if (OB_HASH_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "[STORAGE-CHECKER] Fail to erase cache handle backtrace", K(ret), K(key));
      }
    }
    handle->is_traced_ = false;
  }
  COMMON_LOG(DEBUG, "[STORAGE-CHECKER] handle reset details", K(ret), K(check_id), KP(handle), K(check_id), K(key));
}

int ObStorageLeakChecker::get_aggregate_bt_info(hash::ObHashMap<ObStorageCheckerValue, int64_t> &bt_info)
{
  INIT_SUCC(ret);
  bt_info.reuse();
  for (hash::ObHashMap<ObStorageCheckerKey, ObStorageCheckerValue>::bucket_iterator bucket_iter = checker_info_.bucket_begin() ; 
      OB_SUCC(ret) && bucket_iter != checker_info_.bucket_end() ; ++bucket_iter) {
    hash::ObHashMap<ObStorageCheckerKey, ObStorageCheckerValue>::hashtable::bucket_lock_cond blk(*bucket_iter);
    hash::ObHashMap<ObStorageCheckerKey, ObStorageCheckerValue>::hashtable::readlocker locker(blk.lock());
    for (hash::ObHashMap<ObStorageCheckerKey, ObStorageCheckerValue>::hashtable::hashbucket::const_iterator node_iter = bucket_iter->node_begin() ; 
        OB_SUCC(ret) && node_iter != bucket_iter->node_end() ; ++node_iter) {
      int64_t bt_count = 0;
      if (OB_FAIL(bt_info.get_refactored(node_iter->second, bt_count))) {
        if (OB_HASH_NOT_EXIST == ret) {
          bt_count = 1;
          if (OB_FAIL(bt_info.set_refactored(node_iter->second, bt_count))) {
            COMMON_LOG(WARN, "[STORAGE-CHECKER] Fail to set aggregated info", K(ret));
          }
        } else {
          COMMON_LOG(WARN, "[STORAGE-CHECKER] Fail to get aggregated info", K(ret), K(node_iter->second));
        }
      } else if (OB_FAIL(bt_info.set_refactored(node_iter->second, bt_count+1, 1, 0, 1))) {
        COMMON_LOG(WARN, "[STORAGE-CHECKER] Fail wo update aggregated info", K(ret), K(node_iter->second), K(bt_count));
      }
    }
  }
  return ret;
}


}  // common
}  // oceanbase
