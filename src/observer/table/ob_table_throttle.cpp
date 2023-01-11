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
#define USING_LOG_PREFIX SERVER
#include "ob_table_throttle.h"
#include "ob_table_utils.h"
#include "ob_table_hotkey_kvcache.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace table
{

/**
 * -----------------------------------Singleton ObTableHotKeyThrottle-----------------------------------
 */
ObTableHotKeyThrottle::ObTableHotKeyThrottle()
    :throttle_keys_(),
     allocator_(),
     random_()
{
}

int64_t ObTableHotKeyThrottle::inited_ = 0;
ObTableHotKeyThrottle *ObTableHotKeyThrottle::instance_ = NULL;
const int64_t ObTableHotKeyThrottle::THROTTLE_PERCENTAGE_UNIT;
const int64_t ObTableHotKeyThrottle::THROTTLE_BUCKET_NUM;
constexpr double ObTableHotKeyThrottle::THROTTLE_MAINTAIN_PERCENTAGE;
constexpr double ObTableHotKeyThrottle::KV_HOTKEY_STAT_RATIO;

int ObTableHotKeyThrottle::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(throttle_keys_.create(THROTTLE_BUCKET_NUM, ObModIds::OB_TABLE_THROTTLE, ObModIds::OB_TABLE_THROTTLE))) {
    LOG_WARN("Fail to create throttle map", K(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_WARN("fail to init throttle allocator", K(ret));
  } else {
    allocator_.set_label(ObModIds::OB_TABLE_THROTTLE);
  }
  return ret;
}

void ObTableHotKeyThrottle::clear()
{
  for (ThrottleKeyHashMap::iterator iter = throttle_keys_.begin();
       iter != throttle_keys_.end(); ++iter) {
    ObTableThrottleKey *key = iter->first;
    throttle_keys_.erase_refactored(iter->first);
    allocator_.free(key);
  }
}

void ObTableHotKeyThrottle::destroy()
{
  clear();
  throttle_keys_.destroy();
  inited_ = false;
}

ObTableHotKeyThrottle &ObTableHotKeyThrottle::get_instance()
{
  ObTableHotKeyThrottle *instance = NULL;
  while(OB_UNLIKELY(inited_ < 2)) {
    if (ATOMIC_BCAS(&inited_, 0, 1)) {
      instance = OB_NEW(ObTableHotKeyThrottle, ObModIds::OB_TABLE_THROTTLE);
      if (OB_LIKELY(OB_NOT_NULL(instance))) {
        if (common::OB_SUCCESS != instance->init()) {
          LOG_WARN("failed to init ObTableHotKeyThrottle instance");
          OB_DELETE(ObTableHotKeyThrottle, ObModIds::OB_TABLE_THROTTLE, instance);
          instance = NULL;
          ATOMIC_BCAS(&inited_, 1, 0);
        } else {
          instance_ = instance;
          (void)ATOMIC_BCAS(&inited_, 1, 2);
        }
      } else {
        (void)ATOMIC_BCAS(&inited_, 1, 0);
      }
    }
  }
  return *(ObTableHotKeyThrottle *)instance_;
}


/**
  * @param [in]   hotkey point to source table hotkey
  * @param [out]  throttle_hotkey return a target throttle hotkey
  * create_throttle_hotkey will reuse memory of hotkey rowstr
  * NOT DEEP COPY
  */
int ObTableHotKeyThrottle::create_throttle_hotkey(const ObTableHotKey &hotkey, ObTableThrottleKey &throttle_hotkey)
{
  int ret = OB_SUCCESS;

  throttle_hotkey.tenant_id_ = hotkey.tenant_id_;
  throttle_hotkey.table_id_ = hotkey.table_id_;
  throttle_hotkey.partition_id_ = hotkey.partition_id_;
  throttle_hotkey.key_ = ObString(hotkey.rowkey_.size(), hotkey.rowkey_.length(), hotkey.rowkey_.ptr());
  return ret;
}

/**
  * @param [in]   hotkey source throttle hotkey
  * DEEP COPY hotkey if it is not in map
  */
int ObTableHotKeyThrottle::try_add_throttle_key(ObTableThrottleKey &hotkey)
{
  int ret = OB_SUCCESS;
  ObTableThrottleKey *new_key = nullptr;

  int64_t cur_throttle = 0;
  if (NULL == hotkey.key_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key in throttle-key is null", K(ret), K(hotkey));
  } else {
    if (OB_FAIL(throttle_keys_.get_refactored(&hotkey, cur_throttle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // key is not in throttle map
        if (OB_FAIL(hotkey.deep_copy(allocator_, new_key))) {
          LOG_WARN("fail to deep copy hotkey", K(hotkey), K(ret));
        } else if (OB_FAIL(throttle_keys_.set_refactored(new_key, THROTTLE_PERCENTAGE_UNIT))) {
          LOG_WARN("fail to add hotkey into map", K(ret));
        }
      } else {
        LOG_WARN("fail to get hotkey from throttle map", K(ret));
      }
    } else {
      cur_throttle += THROTTLE_PERCENTAGE_UNIT;
      cur_throttle = cur_throttle > 100 ? 100 : cur_throttle;
      ThrottleKeyHashMapCallBack updater(cur_throttle);
      if (OB_FAIL(throttle_keys_.atomic_refactored(&hotkey, updater))) {
        LOG_WARN("fail to set hotkey to map", K(hotkey), K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(new_key)) {
    allocator_.free(new_key);
  }

  return ret;
}

/**
  * @param [in]   key throttle key
  * if a key should be block then return OB_KILLED_BY_THROTTLING
  * if a key should not be block then return OB_SUCCESS
  * NOT DEEP COPY
  */
int ObTableHotKeyThrottle::check_need_reject_common(const ObTableThrottleKey &key)
{
  int ret = OB_SUCCESS;
  int64_t throttle = 0;

  if (OB_FAIL(throttle_keys_.get_refactored(const_cast<ObTableThrottleKey *>(&key), throttle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // do nothing (rowkey not in hotkey)
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get key from throttle map", K(ret));
    }
  } else {
    if (random_.get(0, 100) < throttle) {
      ret = OB_KILLED_BY_THROTTLING;
    } else {
      // do nothing (hotkey but not block)
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/**
  * @param [in]   allocator ObArenaAllocator from outside
  * @param [in]   tenant_id tenant id
  * @param [in]   table_id  table id
  * @param [in]   rowkey    executor rowkey
  * @param [in]   entity_type KV or HKV
  * if a key should be block then return OB_KILLED_BY_THROTTLING
  * if a key should not be block then return OB_SUCCESS
  */
int ObTableHotKeyThrottle::check_need_reject_req(ObArenaAllocator &allocator,
                                                 uint64_t tenant_id,
                                                 uint64_t table_id,
                                                 const ObRowkey &rowkey,
                                                 ObTableEntityType entity_type)
{
  int ret = OB_SUCCESS;
  ObTableThrottleKey throttle_key;
  ObRowkey copy_key = rowkey;
  ObString rowkey_string;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;

  // check whether need to reject hotkey request
  if (throttle_threshold <= 0) {
    // do nothing
  } else if (ObTableEntityType::ET_HKV == entity_type && OB_FAIL(ObTableHotKeyMgr::get_instance().transform_hbase_rowkey(copy_key))) {
    LOG_WARN("fail to transform hbase hotkey", K(ret), K(rowkey));
  } else if (OB_FAIL(ObTableUtils::set_rowkey_collation(copy_key))) {
      LOG_WARN("fail to set rowkey collation", K(copy_key), K(ret));
  } else if (OB_FAIL(ObTableHotKeyMgr::get_instance().rowkey_serialize(allocator, copy_key, rowkey_string))) {
    LOG_WARN("fail to get string of rowkey", K(ret), K(copy_key), K(rowkey));
  } else {
    throttle_key.tenant_id_ = tenant_id;
    throttle_key.table_id_ = table_id;
    throttle_key.key_ = rowkey_string;
    if (OB_FAIL(check_need_reject_common(throttle_key))) {
      // reject hotkey or not, OB_SUCCESS and OB_KILLED_BY_THROTTLING are accepted
      if (OB_KILLED_BY_THROTTLING != ret) {
        LOG_WARN("fail to check throttle of rowkey", K(throttle_key), K(ret));
      }
    }
  }

  // free temp rowkey string memory
  if (OB_NOT_NULL(rowkey_string.ptr())) {
    allocator.free(rowkey_string.ptr());
  }
  return ret;
}

/**
  * @param [in]   batch_operation batch operation
  * @param [in]   entity_type KV or HKV
  * @param [out]  result if all the key is the same then set result to true
  * if a key should be block then return OB_KILLED_BY_THROTTLING
  * if a key should not be block then return OB_SUCCESS
  * NOT DEEP COPY
  */
int ObTableHotKeyThrottle::is_same_key(const ObTableBatchOperation &batch_operation, ObTableEntityType entity_type, bool &result)
{
  int ret = OB_SUCCESS;
  if (ObTableEntityType::ET_HKV == entity_type) {
    // for hbase
    ObRowkey rowkey = const_cast<ObITableEntity&>(batch_operation.at(0).entity()).get_rowkey();
    ObObj rowkey_obj;
    bool same_key = true;
    // get the first rowkey and compare to the rest of operation
    if (rowkey.get_obj_cnt() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("key for hbase is empty", K(ret));
    } else {
      rowkey_obj = rowkey.get_obj_ptr()[0];
    }
    for (int64_t i = 1; OB_SUCCESS == ret && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_op = batch_operation.at(i);
      rowkey = const_cast<ObITableEntity&>(table_op.entity()).get_rowkey();
      if (rowkey.get_obj_cnt() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("key for hbase is empty", K(ret));
      } else {
        if (rowkey_obj != rowkey.get_obj_ptr()[0]) {
          same_key = false;
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      result =  same_key;
    }
  } else if (ObTableEntityType::ET_KV == entity_type || ObTableEntityType::ET_DYNAMIC == entity_type) {
    // for tableapi
    ObRowkey rowkey = const_cast<ObITableEntity&>(batch_operation.at(0).entity()).get_rowkey();
    bool same_key = true;
    for (int64_t i = 1; OB_SUCCESS == ret && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_op = batch_operation.at(i);
      if (rowkey != const_cast<ObITableEntity&>(table_op.entity()).get_rowkey()) {
        same_key = false;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      result = same_key;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown entity type", K(entity_type));
  }

  return ret;
}

/**
  * @param [in]   allocator   ObArenaAllocator from outside
  * @param [in]   batch_operation batch operation
  * @param [in]   tenant_id   operation's tenant id
  * @param [in]   table_id    operation's table id
  * @param [in]   entity_type KV or HKV
  * check whether a batch request need to reject
  */
int ObTableHotKeyThrottle::check_need_reject_batch_req(ObArenaAllocator &allocator, const ObTableBatchOperation &batch_operation,
                                                       uint64_t tenant_id, uint64_t table_id, ObTableEntityType entity_type)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;
  ObString rowkey_string;
  bool same_key = false;

  if (throttle_threshold <= 0 || !batch_operation.is_same_type()){
    // do nothing
  } else if (batch_operation.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch operation", K(batch_operation), K(ret));
  } else if (is_same_key(batch_operation, entity_type, same_key)) {
    LOG_WARN("fail to check whether keys from batch_operation are the same", K(batch_operation), K(ret));
  } else if (same_key) {
    ObTableHotKeyThrottle &throttle = ObTableHotKeyThrottle::get_instance();
    ObTableThrottleKey throttle_key;
    ObRowkey rowkey = const_cast<ObITableEntity&>(batch_operation.at(0).entity()).get_rowkey();

    if (ObTableEntityType::ET_HKV == entity_type && OB_FAIL(ObTableHotKeyMgr::get_instance().transform_hbase_rowkey(rowkey))) {
      LOG_WARN("fail to transform hbase hotkey", K(ret), K(rowkey));
    } else if (OB_FAIL(ObTableUtils::set_rowkey_collation(rowkey))) {
      LOG_WARN("fail to set rowkey collation", K(rowkey), K(ret));
    } else if (OB_FAIL(ObTableHotKeyMgr::get_instance().rowkey_serialize(allocator, rowkey, rowkey_string))) {
      LOG_WARN("fail to get string of rowkey", K(rowkey), K(ret));
    } else {
      throttle_key.tenant_id_ = tenant_id;
      throttle_key.table_id_ = table_id;
      throttle_key.key_ = rowkey_string;
      if (OB_FAIL(throttle.check_need_reject_common(throttle_key))) {
        // reject hotkey or not, OB_SUCCESS and OB_KILLED_BY_THROTTLING are accepted
        if (OB_KILLED_BY_THROTTLING != ret) {
          LOG_WARN("fail to check throttle of rowkey", K(ret));
        }
      }
    }
  }

  if (OB_NOT_NULL(rowkey_string.ptr())) {
    allocator.free(rowkey_string.ptr());
  }

  return ret;
}

/**
  * @param [in]   allocator ObArenaAllocator from outside
  * @param [in]   tenant_id tenant id
  * @param [in]   table_id  table id
  * @param [in]   query     query
  * @param [in]   entity_type KV or HKV
  * if a key should be block then return OB_KILLED_BY_THROTTLING
  * if a key should not be block then return OB_SUCCESS
  */
int ObTableHotKeyThrottle::check_need_reject_query(ObArenaAllocator &allocator,
                                                   uint64_t tenant_id,
                                                   uint64_t table_id,
                                                   const ObTableQuery &query,
                                                   ObTableEntityType entity_type)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;
  ObTableHotKeyMgr &hotket_mgr = ObTableHotKeyMgr::get_instance();

  if (throttle_threshold <= 0 || query.get_scan_ranges().count() != 1) {
    // do nothing
  } else {
    ObRowkey start_key = query.get_scan_ranges().at(0).start_key_;
    ObRowkey end_key = query.get_scan_ranges().at(0).end_key_;

    if (ObTableEntityType::ET_HKV == entity_type &&
        (OB_FAIL(hotket_mgr.transform_hbase_rowkey(start_key)) || OB_FAIL(hotket_mgr.transform_hbase_rowkey(end_key)))) {
      LOG_WARN("fail to transform hbase hotkey", K(ret), K(start_key), K(end_key));
    } else if (start_key != end_key) {
      // do nothing, only deal with start_key == end_key
    } else if (OB_FAIL(ObTableUtils::set_rowkey_collation(start_key))) {
      LOG_WARN("fail to set rowkey collation", K(start_key), K(ret));
    } else {
      ObTableThrottleKey throttle_key;
      ObString rowkey_string;

      // check whether need to reject hotkey request
      if (OB_FAIL(hotket_mgr.rowkey_serialize(allocator, start_key, rowkey_string))) {
        LOG_WARN("fail to get string of rowkey", K(ret), K(start_key));
      } else {
        throttle_key.tenant_id_ = tenant_id;
        throttle_key.table_id_ = table_id;
        throttle_key.key_ = rowkey_string;
        if (OB_FAIL(check_need_reject_common(throttle_key))) {
          // reject hotkey or not, OB_SUCCESS and OB_KILLED_BY_THROTTLING are accepted
          if (OB_KILLED_BY_THROTTLING != ret) {
            LOG_WARN("fail to check throttle of rowkey", K(ret));
          }
        }
      }
      // free temp rowkey string memory
      if (OB_NOT_NULL(rowkey_string.ptr())) {
        allocator.free(rowkey_string.ptr());
      }
    }
  }
  return ret;
}

/**
  * @param [in]   epoch epoch that need to refresh
  * used to refresh ObTableHotKeyThrottle, remove old hotkey
  */
int ObTableHotKeyThrottle::refresh(uint64_t epoch)
{
  int ret = OB_SUCCESS;

  int64_t cur_throttle = 0;
  int64_t get_cnt = 0;
  ObTableThrottleKey *throttle_key = nullptr;
  const ObTableHotKeyCacheValue *hotkey_value = nullptr;

  for (ThrottleKeyHashMap::iterator iter = throttle_keys_.begin();
       iter != throttle_keys_.end() && OB_SUCC(ret); ++iter) {
    if (OB_ISNULL(throttle_key = iter->first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unpected null throttle key", K(epoch), K(ret));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(throttle_key->tenant_id_));
      int64_t throttle_threshold = tenant_config->kv_hotkey_throttle_threshold;

      // construct tablehotkey to get the latest cnt from kvcache
      ObTableHotKey hotkey(throttle_key->tenant_id_, throttle_key->table_id_, 0, epoch,
                           HotKeyType::TABLE_HOTKEY_ALL, ObString(throttle_key->key_.size(),
                           throttle_key->key_.length(), throttle_key->key_.ptr()));

      if (OB_FAIL(ObTableHotKeyMgr::get_instance().get_hotkey_cache().get_without_put(hotkey, hotkey_value, 1, get_cnt))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get key count from cache", K(ret));
        }
      }

      if (OB_ENTRY_NOT_EXIST == ret || (OB_SUCC(ret) && static_cast<double>(get_cnt) < static_cast<double>(throttle_threshold) * KV_HOTKEY_STAT_RATIO)) {
        // ret OB_ENTRY_NOT_EXIST means that this key is not hotkey
        ret = OB_SUCCESS;
        cur_throttle = iter->second;
        if (cur_throttle - THROTTLE_PERCENTAGE_UNIT <= 0) {
          if (OB_FAIL(throttle_keys_.erase_refactored(throttle_key))) {
            LOG_WARN("Could not delete key from map", K(ret));
          } else {
            allocator_.free(throttle_key);
          }
        } else {
          cur_throttle -= THROTTLE_PERCENTAGE_UNIT;
          ThrottleKeyHashMapCallBack updater(cur_throttle);
          if (OB_FAIL(throttle_keys_.atomic_refactored(throttle_key, updater))) {
            LOG_WARN("fail to set renew throttle to map",K(hotkey), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

} /* namespace table */
} /* namespace oceanbase */