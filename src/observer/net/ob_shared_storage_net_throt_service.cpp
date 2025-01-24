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

#include "ob_shared_storage_net_throt_service.h"
#include "observer/ob_srv_network_frame.h"
#include "src/rootserver/ob_root_service.h"
#include "src/share/backup/ob_backup_connectivity.h"
#include "src/share/object_storage/ob_zone_storage_table_operation.h"
#define USING_LOG_PREFIX RS

namespace oceanbase
{
namespace rootserver
{
ObSharedStorageNetThrotManager::ObSharedStorageNetThrotManager() : is_inited_(false), lock_()
{}

ObSharedStorageNetThrotManager::~ObSharedStorageNetThrotManager()
{
  destroy();
}

void ObSharedStorageNetThrotManager::destroy()
{
  ObSpinLockGuard guard(lock_);
  if (OB_LIKELY(is_inited_)) {
    is_inited_ = false;
    endpoint_infos_map_.clear();
    endpoint_infos_map_.destroy();
    for (ObBucketThrotMap::iterator it = bucket_throt_map_.begin(); it != bucket_throt_map_.end(); ++it) {
      ObQuotaPlanMap *elem_ptr = it->second;
      if (OB_NOT_NULL(elem_ptr)) {
        for (ObQuotaPlanMap ::iterator quota_it = elem_ptr->begin(); quota_it != elem_ptr->end(); ++quota_it) {
          ob_delete(quota_it->second);
        }
        elem_ptr->clear();
        ob_delete(elem_ptr);
      }
    }
    bucket_throt_map_.clear();
    bucket_throt_map_.destroy();
  }
}

int ObSharedStorageNetThrotManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSharedStorageNetThrotManager has been inited", K(ret));
  } else if (OB_FAIL(endpoint_infos_map_.created())) {
    LOG_WARN("endpoint_infos_map_ should not init multiple times", K(ret));
  } else if (OB_FAIL(endpoint_infos_map_.create(7, "SSNT_SERVICE"))) {
    LOG_WARN("fail create ObSharedStorageNetThrotManager's endpoint_infos_map_", K(ret));
  } else if (bucket_throt_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("SSNT's bucket_throt_map_ should not init multiple times", K(ret));
  } else if (OB_FAIL(bucket_throt_map_.create(7, "BUCKET_MAP"))) {
    LOG_WARN("fail create ObSharedStorageNetThrotManager's bucket_throt_map", K(ret));
  } else if (OB_FAIL(storage_key_limit_map_.created())) {
    LOG_WARN("storage_key_limit_map_ should not init multiple times", K(ret));
  } else if (OB_FAIL(storage_key_limit_map_.create(7, "SSNT_SERVICE"))) {
    LOG_WARN("fail create ObSharedStorageNetThrotManager's storage_key_limit_map_", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ObSharedStorageNetThrotManager init ok");
  }
  return ret;
}
int ObSharedStorageNetThrotManager::register_endpoint(const ObSSNTEndpointArg &endpoint_storage_infos)
{
  int ret = OB_SUCCESS;
  const ObAddr &addr = endpoint_storage_infos.addr_;
  const ObSEArray<ObTrafficControl::ObStorageKey, 1> &storage_keys_ = endpoint_storage_infos.storage_keys_;
  const int64_t expire_time = endpoint_storage_infos.expire_time_;
  const ObEndpointInfos storageInfos(storage_keys_, expire_time);
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(endpoint_infos_map_.set_refactored(addr, storageInfos, /*flag*/ 1))) {
    LOG_WARN("add or update endpoint_infos_map_ failed", K(ret));
  } else {
    for (int64_t i = 0; i < storage_keys_.count(); ++i) {
      const ObTrafficControl::ObStorageKey &storage_key = storage_keys_.at(i);
      ObQuotaPlanMap *quota_plan_map = nullptr;
      if (OB_HASH_NOT_EXIST == bucket_throt_map_.get_refactored(storage_key, quota_plan_map) &&
          nullptr == quota_plan_map) {
        quota_plan_map = OB_NEW(obrpc::ObQuotaPlanMap, "SSNT_SERVICE");
        ObSSNTKey key(addr, storage_key);
        ObSSNTValue *value = OB_NEW(obrpc::ObSSNTValue, "SSNT_SERVICE");
        if (OB_ISNULL(quota_plan_map)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new quota_plan_map", K(ret));
        } else if (OB_FAIL(quota_plan_map->created())) {
          ret = OB_INIT_TWICE;
          LOG_WARN("quota_plan_map has been created", K(ret));
        } else if (OB_FAIL(quota_plan_map->create(10, "SSNT_SERVICE"))) {
          LOG_WARN("fail to creat quota_plan_map", K(ret));
        } else if (OB_ISNULL(value)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new value", K(ret));
        } else if (OB_SUCCESS != quota_plan_map->set_refactored(key, value, /*flag*/ 1)) {
          LOG_WARN("fail to set quota_plan_map", K(ret));
        } else if (OB_FAIL(bucket_throt_map_.set_refactored(storage_key, quota_plan_map, /*flag*/ 1))) {
          LOG_WARN("fail to set bucket_throt_map", K(ret));
        }
        if (ret != OB_SUCCESS) {
          LOG_WARN("SSNT:register predict resource failed", K(ret));
          ob_delete(value);
          ob_delete(quota_plan_map);
        }
      }
    }
  }
  return ret;
}

// Cleaning up expired storage limits and expired addresses.
int ObSharedStorageNetThrotManager::clear_expired_infos()
{
  int ret = OB_SUCCESS;
  const int64_t current_time = ObTimeUtility::current_time();
  ObSpinLockGuard guard(lock_);

  if (OB_SUCCESS != OB_IO_MANAGER.get_tc().gc_tenant_infos()) {
    LOG_WARN("SSNT:failed to gc tenant infos for IO MANAGER", K(ret));
  }

  // clean up expired storages
  if (storage_key_limit_map_.size() == 0) {
  } else if (REACH_TIME_INTERVAL(1 * 60 * 1000L * 1000L)){ // 1min
    common::ObSEArray <ObTrafficControl::ObStorageKey, 4> delete_storage_keys;
    obrpc::ObStorageKeyLimitMap::iterator it = storage_key_limit_map_.begin();
    for (; it != storage_key_limit_map_.end(); ++it) {
      if (it->second.expire_time_ < current_time) {
        delete_storage_keys.push_back(it->first);
      }
    }
    for (int i = 0; i < delete_storage_keys.count(); ++i) {
      if (OB_SUCCESS != storage_key_limit_map_.erase_refactored(delete_storage_keys.at(i))) {
        LOG_WARN("SSNT:failed to erase SSNT key", K(ret), K(delete_storage_keys.at(i)));
      }
    }
  }

  // clean up expired addresses
  if (endpoint_infos_map_.size() == 0 && bucket_throt_map_.size() == 0) {
  } else if (endpoint_infos_map_.size() == 0 || bucket_throt_map_.size() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("endpoint infos and storage infos not match",
        K(ret),
        K(endpoint_infos_map_.size()),
        K(bucket_throt_map_.size()));
    endpoint_infos_map_.clear();
    for (ObBucketThrotMap::iterator it = bucket_throt_map_.begin(); it != bucket_throt_map_.end(); ++it) {
      ObQuotaPlanMap *elem_ptr = it->second;
      if (OB_NOT_NULL(elem_ptr)) {
        for (ObQuotaPlanMap::iterator quota_it = elem_ptr->begin(); quota_it != elem_ptr->end(); ++quota_it) {
          ob_delete(quota_it->second);
        }
        elem_ptr->clear();
        ob_delete(elem_ptr);
      }
    }
    bucket_throt_map_.clear();
  } else {
    ObEndpointRegMap::iterator it = endpoint_infos_map_.begin();
    ObBucketThrotMap::iterator iter = bucket_throt_map_.begin();
    common::ObSEArray<ObAddr, 10> delete_addrs;
    common::ObSEArray<ObTrafficControl::ObStorageKey, 10> delete_storage_keys;
    // clear ObAddr
    for (; it != endpoint_infos_map_.end(); ++it) {
      ObEndpointInfos &endpoint_info = it->second;
      if (endpoint_info.expire_time_ < current_time) {
        delete_addrs.push_back(it->first);
      }
    }
    for (int i = 0; i < delete_addrs.count(); ++i) {
      if (OB_SUCCESS != endpoint_infos_map_.erase_refactored(delete_addrs.at(i))) {
        LOG_WARN("SSNT:failed to erase addr", K(ret), K(delete_addrs.at(i)));
      }
    }

    // clear ObStorageKey
    for (; iter != bucket_throt_map_.end(); ++iter) {
      ObQuotaPlanMap *quota_plan_map = iter->second;
      if (OB_ISNULL(quota_plan_map)) {
        // badcase
        ret = OB_INVALID_CONFIG;
        LOG_WARN("value is nullptr", K(ret));
        delete_storage_keys.push_back(iter->first);
      } else if (quota_plan_map->size() == 0) {
        delete_storage_keys.push_back(iter->first);
      } else {
        ObQuotaPlanMap::iterator quota_it = quota_plan_map->begin();
        common::ObSEArray<ObSSNTKey, 10> delete_SSNT_keys;
        for (; quota_it != quota_plan_map->end(); ++quota_it) {
          ObSSNTKey &key = quota_it->first;
          ObSSNTValue *value = quota_it->second;
          if (OB_ISNULL(value)) {
            delete_SSNT_keys.push_back(key);
          } else if (value->expire_time_ < current_time) {
            ob_delete(value);
            delete_SSNT_keys.push_back(key);
          }
        }
        for (int i = 0; i < delete_SSNT_keys.count(); ++i) {
          if (OB_SUCCESS != quota_plan_map->erase_refactored(delete_SSNT_keys.at(i))) {
            LOG_WARN("SSNT:failed to erase SSNT key", K(ret), K(delete_SSNT_keys.at(i)));
          }
        }
      }
      if (OB_NOT_NULL(quota_plan_map) && quota_plan_map->size() == 0) {
        delete_storage_keys.push_back(iter->first);
      }
    }
    if (OB_FAIL(clear_storage_key(delete_storage_keys))) {
      LOG_WARN("SSNT:failed to clear storage key", K(ret));
    }
  }
  return ret;
}

int ObSharedStorageNetThrotManager::clear_storage_key(
    const ObSEArray<ObTrafficControl::ObStorageKey, 10> &delete_storage_keys)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < delete_storage_keys.count(); ++i) {
    ObQuotaPlanMap *quota_plan_map = nullptr;
    if (OB_SUCCESS != bucket_throt_map_.get_refactored(delete_storage_keys.at(i), quota_plan_map)) {
      LOG_WARN("SSNT:failed to get quota_plan_map", K(ret), K(delete_storage_keys.at(i)));
    } else if (OB_ISNULL(quota_plan_map)) {
      bucket_throt_map_.erase_refactored(delete_storage_keys.at(i));
    } else {
      for (ObQuotaPlanMap::iterator quota_it = quota_plan_map->begin(); quota_it != quota_plan_map->end(); ++quota_it) {
        ob_delete(quota_it->second);
      }
      quota_plan_map->clear();
      ob_delete(quota_plan_map);
      bucket_throt_map_.erase_refactored(delete_storage_keys.at(i));
    }
  }
  return ret;
}

int ObSharedStorageNetThrotManager::collect_predict_resource(const int64_t &expire_time)
{
  int ret = OB_SUCCESS;
  ObSharedStorageNetThrotPredictProxy proxy_batch(
      *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::shared_storage_net_throt_predict);
  const int64_t timeout_ts = GCONF.rpc_timeout;
  const int64_t current_time = ObTimeUtility::current_time();
  // RPC2.1: RS asks observer for the required quotas of all storages
  {
    ObSpinLockGuard guard(lock_);
    obrpc::ObEndpointRegMap::iterator it = endpoint_infos_map_.begin();
    for (; OB_SUCC(ret) && it != endpoint_infos_map_.end(); ++it) {
      ObSSNTEndpointArg arg(it->first, it->second.storage_keys_, it->second.expire_time_);
      if (OB_FAIL(proxy_batch.call(arg.addr_, timeout_ts, arg))) {
        LOG_WARN("SSNT:failed to async call rpc", K(ret));
      }
    }
  }
  ObArray<int> return_code_array;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(proxy_batch.wait_all(return_code_array))) {
    LOG_WARN("SSNT's RPC wait batch result failed", KR(tmp_ret), KR(ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (OB_FAIL(ret)) {
  } else if (return_code_array.count() != endpoint_infos_map_.size()) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("SSNT:cnt not match",
        K(ret),
        "return_count",
        return_code_array.count(),
        "server_cnt",
        endpoint_infos_map_.size());
  } else if (OB_FAIL(proxy_batch.check_return_cnt(return_code_array.count()))) {
    LOG_WARN(
        "cnt not match", KR(ret), "return_cnt", return_code_array.count(), "server_cnt", endpoint_infos_map_.size());
  } else {
    ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.size(); ++i) {
      const ObAddr &addr = proxy_batch.get_dests().at(i);
      if (OB_SUCCESS != return_code_array.at(i)) {
        LOG_WARN("SSNT:rpc execute failed", KR(ret), K(addr));
      } else {
        const obrpc::ObSharedDeviceResourceArray *result = proxy_batch.get_results().at(i);
        if (OB_ISNULL(result)) {
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "SSNT:result should not be NULL");  // ignore error
        } else if (OB_FAIL(register_or_update_predict_resource(addr, result, expire_time))) {
          LOG_WARN("fail to register or update predict resource", K(ret), K(result));
        }
      }
    }
  }

  return ret;
}

int ObSharedStorageNetThrotManager::register_or_update_predict_resource(
    const ObAddr &addr, const obrpc::ObSharedDeviceResourceArray *predict_resources, const int64_t &expire_time)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(predict_resources)) {
    LOG_WARN_RET(OB_INVALID_CONFIG, "SSNT:predict_resources should not be NULL");  // ignore error
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < predict_resources->array_.count(); ++i) {
      const ObTrafficControl::ObStorageKey &storage_key = predict_resources->array_[i].key_;
      const ResourceType &type = predict_resources->array_[i].type_;
      ObQuotaPlanMap *quota_plan_map = nullptr;
      int tmp_ret = bucket_throt_map_.get_refactored(storage_key, quota_plan_map);
      if ((OB_HASH_NOT_EXIST == tmp_ret && nullptr == quota_plan_map) ) {
        LOG_WARN("not find storage_key in bucket_throt_map", K(storage_key), K(bucket_throt_map_.size()), K(tmp_ret), K(ret));
      } else if (OB_SUCCESS == tmp_ret && nullptr == quota_plan_map) {
        LOG_WARN("bucket_throt_map register key, but not register value", K(tmp_ret), K(ret), K(storage_key));
      } else if (OB_SUCCESS == tmp_ret && nullptr != quota_plan_map) {
        // update
        ObSSNTKey key(addr, storage_key);
        ObSSNTValue *value = nullptr;
        if (OB_HASH_NOT_EXIST == quota_plan_map->get_refactored(key, value)) {
          value = OB_NEW(obrpc::ObSSNTValue, "SSNT_SERVICE");
          if (OB_ISNULL(value)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new value", K(ret));
          } else if (OB_FAIL(assign_type_value(value, type, predict_resources->array_[i].value_, expire_time))) {
            LOG_WARN("fail to assign type value", K(ret));
          } else if (OB_FAIL(quota_plan_map->set_refactored(key, value))) {
            LOG_WARN("fail to set quota_plan_map", K(ret));
          }
          if (ret != OB_SUCCESS) {
            LOG_WARN("SSNT:update predict resource failed", K(ret));
            ob_delete(value);
          }
        } else if (OB_FAIL(assign_type_value(value, type, predict_resources->array_[i].value_, expire_time))) {
          LOG_WARN("fail to assign type value", K(ret));
        } else if (OB_FAIL(quota_plan_map->set_refactored(key, value, /*flag*/ 1))) {
          LOG_WARN("fail to set quota_plan_map", K(ret));
        }
      } else {
        // bad case: unregister in bucket_throt_map, but register in quota_plan_map, unexpected
        LOG_WARN("fail to register or update predict resource", K(tmp_ret), K(ret), K(storage_key));
      }
    }
  }
  return ret;
}

int ObSharedStorageNetThrotManager::assign_type_value(
    ObSSNTValue *value, const ResourceType &type, const int64_t &val, const int64_t &expire_time)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("value is nullptr", K(ret));
  } else {
    value->expire_time_ = expire_time;
    switch (type) {
      case ResourceType::ops:
        value->predicted_resource_.ops_ = val;
        break;
      case ResourceType::ips:
        value->predicted_resource_.ips_ = val;
        break;
      case ResourceType::iops:
        value->predicted_resource_.iops_ = val;
        break;
      case ResourceType::obw:
        value->predicted_resource_.obw_ = val;
        break;
      case ResourceType::ibw:
        value->predicted_resource_.ibw_ = val;
        break;
      case ResourceType::iobw:
        value->predicted_resource_.iobw_ = val;
        break;
      case ResourceType::tag:
        value->predicted_resource_.tag_ = val;
        break;
      default:
        break;
        // do nothing
    }
  }
  return ret;
}

int ObSharedStorageNetThrotManager::update_quota_plan()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  obrpc::ObBucketThrotMap::iterator it = bucket_throt_map_.begin();
  int tmp_ret = OB_SUCCESS;
  for (; it != bucket_throt_map_.end(); ++it) {
    if (OB_TMP_FAIL(update_one_storage_quota_plan(it->first, it->second))) {
      LOG_WARN("SSNT:update quota plan failed", K(ret), K(tmp_ret), K(it->first));
    }
  }
  return ret;
}
int ObSharedStorageNetThrotManager::update_one_storage_quota_plan(
    const ObTrafficControl::ObStorageKey &storage_key, ObQuotaPlanMap *quota_plan_map)
{
  int ret = OB_SUCCESS;
  int64_t max_iops = 0;
  int64_t max_bandwidth = 0;
  if (OB_ISNULL(quota_plan_map)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("quota_plan_map is nullptr", K(ret));
  } else if (OB_FAIL(get_storage_iops_and_bandwidth_limit(storage_key, max_iops, max_bandwidth))) {
    LOG_WARN("SSNT:get storage iops and bandwidth limit failed", K(ret), K(storage_key), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(cal_iops_quota(max_iops, quota_plan_map))) {
    LOG_WARN("SSNT:cal iops failed", K(ret), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(cal_bw_quota(max_bandwidth, &ObSSNTResource::obw_, quota_plan_map))) {
    LOG_WARN("SSNT:cal obw failed", K(ret), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(cal_bw_quota(max_bandwidth, &ObSSNTResource::ibw_, quota_plan_map))) {
    LOG_WARN("SSNT:cal ibw failed", K(ret), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(cal_iobw_quota(quota_plan_map))) {
    LOG_WARN("SSNT:cal iobw failed", K(ret), K(max_iops), K(max_bandwidth));
  }
  return ret;
}

int ObSharedStorageNetThrotManager::cal_iops_quota(const int64_t limit, ObQuotaPlanMap *quota_plan_map)
{
  int ret = OB_SUCCESS;
  int64_t remain_limit = limit;
  int64_t valid_count = 0;
  if (OB_ISNULL(quota_plan_map)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("quota_plan_map is nullptr", K(ret));
  } else {
    obrpc::ObQuotaPlanMap::iterator iter = quota_plan_map->begin();
    int64_t predict_elements_size = quota_plan_map->size() * 2;
    int64_t predict_elements[predict_elements_size];
    memset(predict_elements, 0, sizeof(predict_elements));
    for (int64_t i = 0; iter != quota_plan_map->end() && i < quota_plan_map->size(); ++iter, ++i) {
      ObSSNTValue *value = iter->second;
      if (OB_ISNULL(value)) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("value is nullptr", K(ret));
      } else if (OB_UNLIKELY((value->predicted_resource_.ops_ < 0) && (value->assigned_resource_.ops_ >= 0) &&
                             (value->predicted_resource_.ips_ < 0) && (value->assigned_resource_.ips_ >= 0))) {
        // bad case: a endpoint's predicted resource is -1, but it has assigned resource.
        // In this situation, we give it asked assigned resource.
        remain_limit -= value->assigned_resource_.ops_;
        remain_limit -= value->assigned_resource_.ips_;
      } else if (OB_UNLIKELY((value->predicted_resource_.ops_ < 0) && (value->assigned_resource_.ops_ >= 0))) {
        // as above
        remain_limit -= value->assigned_resource_.ops_;
        predict_elements[valid_count++] = value->predicted_resource_.ips_;
      } else if (OB_UNLIKELY((value->predicted_resource_.ips_ < 0) && (value->assigned_resource_.ips_ >= 0))) {
        // as above
        remain_limit -= value->assigned_resource_.ips_;
        predict_elements[valid_count++] = value->predicted_resource_.ops_;
      } else {
        predict_elements[valid_count++] = value->predicted_resource_.ops_;
        predict_elements[valid_count++] = value->predicted_resource_.ips_;
      }
    }
    // cal the baseline_resource and extra_resource
    int64_t baseline_resource = 0;
    int64_t extra_resource = 0;
    if (remain_limit <= 0) {
      remain_limit = 0;
    }
    lib::ob_sort(predict_elements, predict_elements + valid_count);
    int64_t average = 0;
    bool is_break = false;
    for (int64_t i = 0; (!is_break) && i < valid_count; ++i) {
      average = remain_limit / (valid_count - i);
      if (predict_elements[i] >= average) {
        remain_limit = 0;
        is_break = true;
      } else {
        remain_limit -= predict_elements[i];
      }
    }
    baseline_resource = average;
    int64_t remain_average = 0;
    int64_t remain_quota = 0;
    if (OB_UNLIKELY(0 == valid_count)) {
      LOG_WARN("SSNT Service: cal assigned resource failed, valid_count == 0", K(valid_count));
    } else {
      remain_average = remain_limit / valid_count;
      remain_quota = remain_limit % valid_count;
    }
    for (iter = quota_plan_map->begin(); iter != quota_plan_map->end(); ++iter) {
      ObSSNTValue *value = iter->second;
      if (OB_UNLIKELY(value->predicted_resource_.ops_ == -1) && OB_UNLIKELY(value->predicted_resource_.ips_ == -1)) {
      } else if (OB_UNLIKELY(value->predicted_resource_.ops_ == -1)) {
        int64_t temp_resource =
            value->predicted_resource_.ips_ > baseline_resource ? baseline_resource : value->predicted_resource_.ips_;
        value->assigned_resource_.ips_ = temp_resource + remain_average;
      } else if (OB_UNLIKELY(value->predicted_resource_.ips_ == -1)) {
        int64_t temp_resource =
            value->predicted_resource_.ops_ > baseline_resource ? baseline_resource : value->predicted_resource_.ops_;
        value->assigned_resource_.ops_ = temp_resource + remain_average;
      } else {
        int64_t temp_resource =
            value->predicted_resource_.ops_ > baseline_resource ? baseline_resource : value->predicted_resource_.ops_;
        value->assigned_resource_.ops_ = temp_resource + remain_average;
        temp_resource =
            value->predicted_resource_.ips_ > baseline_resource ? baseline_resource : value->predicted_resource_.ips_;
        value->assigned_resource_.ips_ = temp_resource + remain_average;
      }
      if (iter == quota_plan_map->begin()) {
        value->assigned_resource_.ops_ += remain_quota;
      }
      value->assigned_resource_.iops_ = value->assigned_resource_.ops_ + value->assigned_resource_.ips_;
    }
  }
  return ret;
}

int ObSharedStorageNetThrotManager::cal_bw_quota(
    const int64_t limit, int64_t ObSSNTResource::*member_ptr, ObQuotaPlanMap *quota_plan_map)
{
  int ret = OB_SUCCESS;
  int64_t remain_limit = limit;
  int64_t valid_count = 0;
  if (OB_ISNULL(quota_plan_map)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("quota_plan_map is nullptr", K(ret));
  } else if (OB_ISNULL(member_ptr)) {
    ret = OB_NULL_CHECK_ERROR;
    LOG_ERROR("member_ptr is nullptr", K(ret));
  } else {
    obrpc::ObQuotaPlanMap::iterator iter = quota_plan_map->begin();
    int64_t predict_elements[quota_plan_map->size()];
    memset(predict_elements, 0, sizeof(predict_elements));
    for (int64_t i = 0; iter != quota_plan_map->end() && i < quota_plan_map->size(); ++iter, ++i) {
      ObSSNTValue *value = iter->second;
      if (OB_ISNULL(value)) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("value is nullptr", K(ret));
      } else if (OB_UNLIKELY(
                     (value->predicted_resource_.*member_ptr < 0) && (value->assigned_resource_.*member_ptr >= 0))) {
        remain_limit -= value->assigned_resource_.*member_ptr;
      } else {
        predict_elements[valid_count++] = value->predicted_resource_.*member_ptr;
      }
    }
    // cal the baseline_resource and extra_resource
    int64_t baseline_resource = 0;
    int64_t extra_resource = 0;
    if (remain_limit <= 0) {
      remain_limit = 0;
    }
    lib::ob_sort(predict_elements, predict_elements + valid_count);
    int64_t average = 0;
    bool is_break = false;
    for (int64_t i = 0; (!is_break) && i < valid_count; ++i) {
      average = remain_limit / (valid_count - i);
      if (predict_elements[i] >= average) {
        remain_limit = 0;
        is_break = true;
      } else {
        remain_limit -= predict_elements[i];
      }
    }
    baseline_resource = average;
    int64_t remain_average = 0;
    int64_t remain_quota = 0;
    if (OB_UNLIKELY(0 == valid_count)) {
      LOG_WARN("SSNT Service: cal assigned resource failed, valid_count == 0", K(valid_count));
    } else {
      remain_average = remain_limit / valid_count;
      remain_quota = remain_limit % valid_count;
    }
    for (iter = quota_plan_map->begin(); iter != quota_plan_map->end(); ++iter) {
      ObSSNTValue *value = iter->second;
      if (OB_UNLIKELY(value->predicted_resource_.*member_ptr == -1)) {
      } else if (OB_ISNULL(value)) {
      } else {
        int64_t temp_resource = value->predicted_resource_.*member_ptr > baseline_resource
                                    ? baseline_resource
                                    : value->predicted_resource_.*member_ptr;
        value->assigned_resource_.*member_ptr = temp_resource + remain_average;
      }
    }
    if (quota_plan_map->size() > 0) {
      ObSSNTValue *value = quota_plan_map->begin()->second;
      value->assigned_resource_.*member_ptr += remain_quota;
    }
  }
  return ret;
}

int ObSharedStorageNetThrotManager::cal_iobw_quota(ObQuotaPlanMap *quota_plan_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(quota_plan_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quota_plan_map is nullptr", K(ret));
  } else {
    obrpc::ObQuotaPlanMap::iterator iter = quota_plan_map->begin();
    for (; iter != quota_plan_map->end(); ++iter) {
      ObSSNTValue *value = iter->second;
      if (OB_ISNULL(value)) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("value is nullptr", K(ret));
      } else {
        value->assigned_resource_.iobw_ = value->assigned_resource_.ibw_ + value->assigned_resource_.obw_;
      }
    }
  }
  return ret;
}
int ObSharedStorageNetThrotManager::commit_quota_plan()
{
  int ret = OB_SUCCESS;
  ObSharedStorageNetThrotSetProxy proxy_batch(
      *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::shared_storage_net_throt_set);
  const int64_t timeout_ts = GCONF.rpc_timeout;
  ObArray<int> return_code_array;
  ObSEArray<std::pair<ObAddr, obrpc::ObSharedDeviceResourceArray>, 7> send_rpc_arr;
  ObSpinLockGuard guard(lock_);
  {
    obrpc::ObEndpointRegMap::iterator iter = endpoint_infos_map_.begin();
    for (; iter != endpoint_infos_map_.end(); ++iter) {  // commit to each observer
      obrpc::ObSharedDeviceResourceArray arg;
      const ObAddr &addr = iter->first;
      const ObSEArray<ObTrafficControl::ObStorageKey, 1> &storage_keys_ = iter->second.storage_keys_;
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < storage_keys_.count(); ++i) {
        const ObTrafficControl::ObStorageKey &storage_key = storage_keys_[i];
        ObQuotaPlanMap *quota_plan_map = nullptr;
        if (OB_TMP_FAIL(bucket_throt_map_.get_refactored(storage_key, quota_plan_map))) {
          LOG_WARN("SSNT:get quota plan map failed", K(ret), K(tmp_ret), K(storage_key));
        } else if (OB_ISNULL(quota_plan_map)) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("SSNT:get quota plan map failed", K(ret), K(tmp_ret), K(storage_key));
        } else {
          obrpc::ObQuotaPlanMap::iterator it = quota_plan_map->begin();
          for (; it != quota_plan_map->end(); ++it) {
            const ObSSNTKey &key = it->first;
            ObSSNTValue *value = it->second;
            if (key.addr_.compare(addr) != 0) {
            } else if (key.key_ != storage_key) {
            } else if (OB_ISNULL(value)) {
            } else if (OB_TMP_FAIL(get_predict(key.key_, *value, arg))) {
              LOG_WARN("SSNT:get predict failed", K(ret), K(key), K(arg));
            }
          }
        }
      }
      if (OB_TMP_FAIL(send_rpc_arr.push_back(std::pair<ObAddr, obrpc::ObSharedDeviceResourceArray>(addr, arg)))) {
        LOG_WARN("SSNT:push back rpc info failed", K(ret), K(tmp_ret), K(addr), K(arg));
      }
    }
  }
  for (int64_t i = 0; i < send_rpc_arr.count(); ++i) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy_batch.call(send_rpc_arr.at(i).first, timeout_ts, send_rpc_arr.at(i).second))) {
      LOG_WARN("SSNT:fail to call async batch rpc", KR(ret), K(tmp_ret), "addr", send_rpc_arr.at(i).first, "arg", send_rpc_arr.at(i).second);
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(proxy_batch.wait_all(return_code_array))) {
    LOG_WARN("SSNT:fail to wait all async batch rpc", KR(ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (OB_FAIL(ret)) {
  } else if (return_code_array.count() != endpoint_infos_map_.size()) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("SSNT:cnt not match",
        K(ret),
        "return_count",
        return_code_array.count(),
        "kv_count",
        endpoint_infos_map_.size());
  } else if (OB_FAIL(proxy_batch.check_return_cnt(return_code_array.count()))) {
    LOG_WARN(
        "cnt not match", KR(ret), "return_cnt", return_code_array.count(), "server_cnt", endpoint_infos_map_.size());
  } else {
    for (int64_t i = 0; i < return_code_array.count(); ++i) {
      if (OB_SUCCESS != return_code_array.at(i)) {
        const ObAddr &addr = proxy_batch.get_dests().at(i);
        LOG_WARN("rpc execute failed", KR(ret), K(addr));
      }
    }
  }
  return ret;
}
int ObSharedStorageNetThrotManager::get_predict(const oceanbase::common::ObTrafficControl::ObStorageKey &storage_key,
    const obrpc::ObSSNTValue &value, obrpc::ObSharedDeviceResourceArray &arg)
{
  int ret = OB_SUCCESS;
  arg.array_.push_back(
      obrpc::ObSharedDeviceResource(storage_key, obrpc::ResourceType::ops, value.assigned_resource_.ops_));
  arg.array_.push_back(
      obrpc::ObSharedDeviceResource(storage_key, obrpc::ResourceType::ips, value.assigned_resource_.ips_));
  arg.array_.push_back(
      obrpc::ObSharedDeviceResource(storage_key, obrpc::ResourceType::iops, value.assigned_resource_.iops_));
  arg.array_.push_back(
      obrpc::ObSharedDeviceResource(storage_key, obrpc::ResourceType::obw, value.assigned_resource_.obw_));
  arg.array_.push_back(
      obrpc::ObSharedDeviceResource(storage_key, obrpc::ResourceType::ibw, value.assigned_resource_.ibw_));
  arg.array_.push_back(
      obrpc::ObSharedDeviceResource(storage_key, obrpc::ResourceType::iobw, value.assigned_resource_.iobw_));
  return ret;
}
int ObSharedStorageNetThrotManager::register_or_update_storage_key_limit(
    const ObTrafficControl::ObStorageKey &storage_key)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageKeyLimit limit;
  limit.expire_time_ = ObTimeUtility::current_time() + 1L * 60L * 60L * 1000L * 1000L;  // 1 hour
  if (storage_key.get_category() == ObStorageInfoType::ALL_ZONE_STORAGE) {
    common::ObArray<oceanbase::share::ObZoneStorageTableInfo> storage_infos;
    if (OB_TMP_FAIL(share::ObStorageInfoOperator::get_zone_storage_table_info(GCTX.root_service_->get_sql_proxy(), storage_infos))) {
      LOG_WARN("failed to get storage infos", K(ret), K(tmp_ret), K(storage_key));
    } else if (storage_infos.count() == 0) {
      LOG_ERROR("get storage infos failed", K(ret), K(tmp_ret), K(storage_key));
    } else {
      bool is_end = false;
      for (int64_t i = 0; !is_end && i < storage_infos.count(); ++i) {
        if (storage_infos.at(i).storage_id_ == storage_key.get_storage_id()) {
          limit.max_iops_ = storage_infos.at(i).max_iops_;
          limit.max_bw_ = storage_infos.at(i).max_bandwidth_;
          is_end = true;
        }
      }
    }
  } else if (storage_key.get_category() == ObStorageInfoType::ALL_BACKUP_STORAGE_INFO ||
             storage_key.get_category() == ObStorageInfoType::ALL_RESTORE_INFO) {
    if (OB_TMP_FAIL(share::ObBackupStorageInfoOperator::get_restore_shared_storage_limit(
            storage_key, limit.max_iops_, limit.max_bw_))) {
      LOG_WARN("failed to get storage limit", K(storage_key), K(limit));
    }
  } else if (is_unlimited_category(storage_key.get_category())) {
    // ignore ret
    limit.max_iops_ = INT64_MAX;
    limit.max_bw_ = INT64_MAX;
  } else {
    LOG_WARN("unknown storage key", K(storage_key));
  }
  if (tmp_ret != OB_SUCCESS) {
    // inner sql failed, do nothing
  } else if (OB_FAIL(storage_key_limit_map_.set_refactored(storage_key, limit, /*flag*/ 1))) {  // update or insert
    LOG_WARN("failed to set storage key limit", K(ret), K(tmp_ret), K(storage_key), K(limit));
  }
  return ret;
}

int ObSharedStorageNetThrotManager::is_unlimited_category(const ObStorageInfoType category) const
{
  return category == ObStorageInfoType::ALL_DDL_STORAGE_INFO ||
         category == ObStorageInfoType::ALL_EXTERNAL_STORAGE_INFO ||
         category == ObStorageInfoType::ALL_EXPORT_STORAGE_INFO ||
         category == ObStorageInfoType::ALL_SQL_AUDIT_STORAGE_INFO ||
         category == ObStorageInfoType::ALL_OTHER_STORAGE_INFO;
}

int ObSharedStorageNetThrotManager::get_storage_iops_and_bandwidth_limit(
    const ObTrafficControl::ObStorageKey &storage_key, int64_t &max_iops, int64_t &max_bandwidth)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageKeyLimit limit;
  if (OB_TMP_FAIL(register_or_update_storage_key_limit(storage_key))) {
    LOG_WARN("register or get storage limit failed", K(storage_key), K(limit), K(ret), K(tmp_ret));
  }
  if (OB_FAIL(storage_key_limit_map_.get_refactored(storage_key, limit))) {
    max_iops = 200;
    max_bandwidth = 25 * 1000L * 1000L; // 25MBtyes
    if (REACH_TIME_INTERVAL(1 * 60 * 1000L * 1000L)) {  // 1min
      LOG_WARN("failed to get storage limit", K(storage_key), K(limit), K(max_iops), K(max_bandwidth), K(ret));
    }
  } else if (!limit.is_valid()) {
    max_iops = 20;
    max_bandwidth = 2500L * 1000L; // 2.5MBtyes
  } else {
    max_iops = limit.max_iops_;
    max_bandwidth = limit.max_bw_;
  }
  return ret;
}

int ObSSNTAllocService::init(const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;
  tg_id_ = lib::TGDefIDs::IngressService;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(quota_manager_.init())) {
    LOG_ERROR("failed to init quota manager", KR(ret));
  } else {
    is_inited_ = true;
    cluster_id_ = cluster_id;
    LOG_INFO("[SSNT_SERVICE] ObSSNTService init success", K(tg_id_), K(cluster_id_));
  }
  return ret;
}

int ObSSNTAllocService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, QUOTA_SERVICE_INTERVAL_US, true))) {
    LOG_WARN("ObSSNTService TG_SCHEDULE failed", K(ret), K(tg_id_));
  } else {
    ATOMIC_STORE(&is_stop_, false);
    LOG_INFO("[SSNT_SERVICE] ObSSNTService start success", K(tg_id_));
  }
  return ret;
}
void ObSSNTAllocService::stop()
{
  ATOMIC_STORE(&is_stop_, true);
}

void ObSSNTAllocService::wait()
{}

void ObSSNTAllocService::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    is_inited_ = false;
    ATOMIC_STORE(&is_stop_, true);
    ATOMIC_STORE(&is_leader_, false);
    stop();
    wait();
    quota_manager_.destroy();
    LOG_INFO("[SSNT_SERVICE] ObSSNTService destroy success", K(tg_id_));
  }
}
int ObSSNTAllocService::register_endpoint(const ObSSNTEndpointArg &endpoint_storage_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_leader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[SSNT_SERVICE] ObSSNTAllocService not leader", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[SSNT_SERVICE] ObSSNTAllocService thread is stopped", KR(ret), KP(this));
  } else if (OB_FAIL(quota_manager_.register_endpoint(endpoint_storage_infos))) {
    LOG_WARN("[SSNT_SERVICE] ObSSNTAllocService register endpoint failed", KR(ret), KP(this));
  }
  return ret;
}

int ObSSNTAllocService::get_follower_register_arg(obrpc::ObSSNTEndpointArg &arg)
{
  int ret = OB_SUCCESS;
  arg.expire_time_ = ObTimeUtility::current_time() + ENDPOINT_EXPIRE_INTERVAL_US;
  arg.addr_ = GCTX.self_addr();
  common::hash::ObHashSet<ObTrafficControl::ObStorageKey> storage_set;
  if (storage_set.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage set is created", KR(ret));
  } else if (OB_FAIL(storage_set.create(4))) {
    LOG_WARN("failed to create storage set", KR(ret));
  }
  struct GetStorageKey
  {
  public:
    GetStorageKey(common::hash::ObHashSet<ObTrafficControl::ObStorageKey> &storage_set) : storage_set_(storage_set)
    {}
    int operator()(common::hash::HashMapPair<oceanbase::common::ObTrafficControl::ObIORecordKey,
        common::ObTrafficControl::ObSharedDeviceIORecord> &entry)
    {
      if (OB_HASH_NOT_EXIST == storage_set_.exist_refactored(entry.first.id_)) {
        storage_set_.set_refactored(entry.first.id_);
      }
      return OB_SUCCESS;
    }
    common::hash::ObHashSet<ObTrafficControl::ObStorageKey> &storage_set_;
  };
  GetStorageKey fn(storage_set);
  ObIOManager::get_instance().get_tc().foreach_record(fn);
  for (common::hash::ObHashSet<ObTrafficControl::ObStorageKey>::iterator it = storage_set.begin();
       it != storage_set.end();
       ++it) {
    arg.storage_keys_.push_back(it->first);
  }
  return ret;
}
int ObSSNTAllocService::follower_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  obrpc::ObSSNTEndpointArg arg;
  ObAddr rs_addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret), K(is_inited_));
  } else if (OB_TMP_FAIL(quota_manager_.clear_expired_infos())) {
    COMMON_LOG(INFO, "SSNT:clear addr failed", K(ret), K(tmp_ret), K(is_inited_));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(cluster_id_, rs_addr))) {
    COMMON_LOG(ERROR, "failed to get root server address", K(ret), K(cluster_id_), K(rs_addr));
  } else if (OB_FAIL(get_follower_register_arg(arg))) {
    COMMON_LOG(WARN, "get follower register arg failed", K(ret));
  } else if (arg.storage_keys_.count() == 0) { // do nothing
  } else if (OB_FAIL(endpoint_rpc_proxy_.to(rs_addr).timeout(500 * 1000).shared_storage_net_throt_register(arg))) {
    COMMON_LOG(WARN, "SSNT:endpoint register failed", K(ret), K(rs_addr), K(arg));
  } else {
    expire_time_ = arg.expire_time_;
  }
  return ret;
}

int ObSSNTAllocService::leader_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObSSNTAllocService is not inited", K(ret));
  } else if (OB_FAIL(quota_manager_.collect_predict_resource(expire_time_))) {
    LOG_WARN("SSNT:update observer predicted resource failed", K(ret));
  } else if (OB_FAIL(quota_manager_.update_quota_plan())) {
    LOG_WARN("SSNT:update quota plan failed", K(ret));
  } else if (OB_FAIL(quota_manager_.commit_quota_plan())) {
    LOG_WARN("SSNT:assign quota resource failed", K(ret));
  }
  return ret;
}
void ObSSNTAllocService::runTimerTask()
{
  // for follower register task
  follower_task();
  // for leader
  if (is_leader()) {
    leader_task();
  }
}

void ObSSNTAllocService::switch_to_follower_forcedly()
{
  ATOMIC_STORE(&is_leader_, false);
}

int ObSSNTAllocService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_leader_, true);
  return ret;
}

int ObSSNTAllocService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_leader_, false);
  return ret;
}

int ObSSNTAllocService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (!is_leader()) {
    ATOMIC_STORE(&is_leader_, true);
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
