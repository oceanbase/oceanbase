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

#include "ob_all_virtual_tablet_local_cache.h"
#include "share/ob_server_struct.h"
#include "share/ash/ob_di_util.h"

using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

ObAllVirtualTabletLocalCache::ObAllVirtualTabletLocalCache()
    : tenant_id_(OB_INVALID_TENANT_ID),
      cur_idx_(0),
      tablet_policy_status_arr_()
{
  ip_buf_[0] = '\0';
}

ObAllVirtualTabletLocalCache::~ObAllVirtualTabletLocalCache()
{
  reset();
}

void ObAllVirtualTabletLocalCache::reset()
{
  ip_buf_[0] = '\0';
  tenant_id_ = OB_INVALID_TENANT_ID;
  cur_idx_ = 0;
  tablet_policy_status_arr_.reset();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTabletLocalCache::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAllVirtualTabletLocalCache::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", KR(ret));
  }
  return ret;
}

int ObAllVirtualTabletLocalCache::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  ObAddr addr = GCTX.self_addr();
  const int64_t col_count = output_column_ids_.count();
  if (OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", KR(ret), K(cells));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  }

#ifdef OB_BUILD_SHARED_STORAGE
  ObStorageCachePolicyService *scp_service = nullptr;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(scp_service = MTL(ObStorageCachePolicyService *))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObStorageCachePolicyService is NULL", KR(ret));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObSSMicroCache is NULL", KR(ret));
  } else {
    if (MTL_ID() != tenant_id_) {
      tenant_id_ = MTL_ID();
      if (is_user_tenant(tenant_id_)) {
        const hash::ObHashMap<int64_t, PolicyStatus> &tablet_status_map =
            scp_service->get_tablet_status_map();
        if (OB_FAIL(copy_tablet_status_map_(tablet_status_map))) {
          SERVER_LOG(WARN, "fail to copy tablet status", KR(ret), KPC(scp_service));
        }
      } else {  // only process user tenant
        ret = OB_ITER_END;
      }
    }

    common::ObTabletID tablet_id;
    PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
    ObSSTabletCacheInfo tablet_micro_cache_info;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(cur_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur_idx_ is invalid", KR(ret), K(cur_idx_));
    } else if (cur_idx_ >= tablet_policy_status_arr_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet_id = tablet_policy_status_arr_.at(cur_idx_).first;
      policy_status = tablet_policy_status_arr_.at(cur_idx_).second;
      if (OB_UNLIKELY(!tablet_id.is_valid()
          || !ObStorageCachePolicyStatus::is_valid(policy_status))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid tablet_id or policy status", KR(ret),
            K(cur_idx_), K(tablet_id), K(policy_status));
      } else if (OB_FAIL(micro_cache->get_tablet_cache_info(tablet_id, tablet_micro_cache_info))) {
        SERVER_LOG(WARN, "fail to get micro cache info", KR(ret),
            K(cur_idx_), K(tablet_id), K(policy_status));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case SVR_IP: {
        if (addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret), K(addr));
        }
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(addr.get_port());
        break;
      }
      case TENANT_ID: {
        cells[i].set_int(tenant_id_);
        break;
      }
      case TABLET_ID: {
        cells[i].set_int(tablet_id.id());
        break;
      }
      case STORAGE_CACHE_POLICY: {
        const char *policy_str = nullptr;
        if (OB_FAIL(ObStorageCachePolicyStatus::safely_get_str(policy_status, policy_str))) {
          SERVER_LOG(WARN, "storage cache policy status is NULL", KR(ret), K(policy_status));
        } else if (OB_ISNULL(policy_str)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "storage cache policy status is NULL", KR(ret), K(policy_status));
        } else {
          cells[i].set_varchar(policy_str);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case CACHED_DATA_SIZE: {
        cells[i].set_int(tablet_micro_cache_info.get_valid_size());
        break;
      }
      case CACHE_HIT_COUNT: {
        cells[i].set_int(-1);
        break;
      }
      case CACHE_MISS_COUNT: {
        cells[i].set_int(-1);
        break;
      }
      case CACHE_HIT_SIZE: {
        cells[i].set_int(-1);
        break;
      }
      case CACHE_MISS_SIZE: {
        cells[i].set_int(-1);
        break;
      }
      case INFO: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      } // end switch
    } // end for
    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  }
#endif
  return ret; 
}

class ObCopyTabletStatusFunc
{
public:
  ObCopyTabletStatusFunc(TabletPolicyStatusArray &tablet_policy_status_arr)
      : tablet_policy_status_arr_(tablet_policy_status_arr)
  {}

  int operator()(const hash::HashMapPair<int64_t, PolicyStatus> &entry)
  {
    int ret = OB_SUCCESS;
    const int64_t tablet_id = entry.first;
    const PolicyStatus policy_status = entry.second;
    if (OB_UNLIKELY(tablet_id == common::ObTabletID::INVALID_TABLET_ID
        || !ObStorageCachePolicyStatus::is_valid(policy_status))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "entry in ObStorageCachePolicyService::tablet_status_map_ is invalid",
          KR(ret), K(tablet_id), K(policy_status));
    } else if (OB_FAIL(tablet_policy_status_arr_.push_back(std::pair<int64_t, PolicyStatus>(
        tablet_id, policy_status)))) {
      SERVER_LOG(WARN, "fail to store entry", KR(ret), K(tablet_policy_status_arr_.count()));
    }
    return ret;
  }

private:
  TabletPolicyStatusArray &tablet_policy_status_arr_;
};

int ObAllVirtualTabletLocalCache::copy_tablet_status_map_(
    const hash::ObHashMap<int64_t, PolicyStatus> &tablet_status_map)
{
  int ret = OB_SUCCESS;
  tablet_policy_status_arr_.reset();
  cur_idx_ = 0;
  ObCopyTabletStatusFunc func(tablet_policy_status_arr_);
  if (tablet_status_map.size() > 0 && OB_FAIL(tablet_status_map.foreach_refactored(func))) {
    SERVER_LOG(WARN, "fail to iterate tablets tasks map", KR(ret), K(tablet_status_map.size()));
  }
  return ret;
}

void ObAllVirtualTabletLocalCache::release_last_tenant()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_policy_status_arr_.reset();
}

bool ObAllVirtualTabletLocalCache::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_;
}

} // namespace observer
} // namespace oceanbase