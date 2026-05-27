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
 *
 * Persistent storage for serialized DELETE payload in update-split-merge flow.
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_update_split_merge_storager.h"
#include "ob_log_store_service.h"                   // IObStoreService
#include "ob_log_instance.h"                        // TCTX
#include "ob_log_trans_stat_mgr.h"                  // UpdateSplitMergeStatInfo

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

int UpdateSplitMergeKey::get_key(std::string &key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("update_split_merge store key is not valid", KR(ret), KPC(this));
  } else {
    // Tenant is isolated at cf granularity (merge_cf_handle is per-tenant),
    // so we do not encode tenant_id into the key string. Matches lob_aux convention.
    key.append(std::to_string(commit_version_));
    key.append("_");
    key.append(std::to_string(trans_id_.get_id()));
    key.append("_");
    key.append(std::to_string(trace_id_raw_));
  }
  return ret;
}

ObCDCUpdateSplitMergeStorager::ObCDCUpdateSplitMergeStorager() :
    is_inited_(false),
    store_service_(nullptr),
    stat_(nullptr)
{
}

ObCDCUpdateSplitMergeStorager::~ObCDCUpdateSplitMergeStorager()
{
  destroy();
}

int ObCDCUpdateSplitMergeStorager::init(IObStoreService *store_service, UpdateSplitMergeStatInfo &stat)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCUpdateSplitMergeStorager has been initialized", KR(ret));
  } else if (OB_ISNULL(store_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("store_service is null", KR(ret));
  } else {
    store_service_ = store_service;
    stat_ = &stat;
    is_inited_ = true;
    LOG_INFO("ObCDCUpdateSplitMergeStorager init succ");
  }

  return ret;
}

void ObCDCUpdateSplitMergeStorager::destroy()
{
  if (is_inited_) {
    store_service_ = nullptr;
    stat_ = nullptr;
    is_inited_ = false;
  }
}

int ObCDCUpdateSplitMergeStorager::put(
    const UpdateSplitMergeKey &key,
    const char *data,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  void *cf_handle = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCUpdateSplitMergeStorager has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!key.is_valid()) || OB_ISNULL(data) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(key), K(data_len));
  } else if (OB_FAIL(get_cf_handle_(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->put(cf_handle, key_str, ObSlice(data, data_len)))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("[MERGE_STORE][PUT] store_service_ put fail", KR(ret), K(key),
          KCSTRING(key_str.c_str()), K(data_len));
    }
  } else {
    stat_->add_storager_disk_put(data_len);
    LOG_DEBUG("[MERGE_STORE][PUT] succ", K(key), KCSTRING(key_str.c_str()), K(data_len));
  }

  return ret;
}

int ObCDCUpdateSplitMergeStorager::get(
    ObIAllocator &allocator,
    const UpdateSplitMergeKey &key,
    const char *&data,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  std::string value;
  char *data_ptr = nullptr;
  void *cf_handle = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCUpdateSplitMergeStorager has not been initialized", KR(ret));
  } else if (OB_FAIL(get_cf_handle_(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->get(cf_handle, key_str, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("[MERGE_STORE][GET] get failed", KR(ret), K(key));
    }
  } else if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("data is empty", KR(ret), K(key));
  } else if (OB_ISNULL(data_ptr = static_cast<char*>(allocator.alloc(value.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc fail", KR(ret), "size", value.length(), K(key));
  } else {
    MEMCPY(data_ptr, value.c_str(), value.length());
    data = data_ptr;
    data_len = value.length();
    stat_->inc_storager_disk_hit();
  }

  return ret;
}

int ObCDCUpdateSplitMergeStorager::del(const UpdateSplitMergeKey &key)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  void *cf_handle = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCUpdateSplitMergeStorager has not been initialized", KR(ret));
  } else if (OB_FAIL(get_cf_handle_(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->del(cf_handle, key_str))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("del failed", KR(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    LOG_DEBUG("[MERGE_STORE][DEL] succ", K(key), KCSTRING(key_str.c_str()));
  }

  return ret;
}

int ObCDCUpdateSplitMergeStorager::get_cf_handle_(const uint64_t tenant_id, void *&cf_handle)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = nullptr;
  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is null", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(cf_handle = tenant->get_merge_cf_handle())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("merge_cf_handle is null, tenant may not be initialized for update-split-merge",
        KR(ret), K(tenant_id));
  }
  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
