/**
 * Copyright (c) 2026 OceanBase
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

#include "ob_all_virtual_proxy_ls_location.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObAllVirtualProxyLSLocation::ObAllVirtualProxyLSLocation()
    : ObAllVirtualProxyBaseIterator(),
      is_inited_(false),
      tenant_ids_(),
      next_location_idx_(0),
      next_replica_idx_(0),
      ls_locations_()
{
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

ObAllVirtualProxyLSLocation::~ObAllVirtualProxyLSLocation()
{
}

int ObAllVirtualProxyLSLocation::inner_open()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  tenant_ids_.reset();
  next_location_idx_ = 0;
  next_replica_idx_ = 0;
  ls_locations_.reset();
  if (OB_ISNULL(allocator_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualProxyLSLocation not init", KR(ret), K_(allocator), KP(GCTX.location_service_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(effective_tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid effective_tenant_id", KR(ret), K_(effective_tenant_id));
  } else if (OB_FAIL(parse_key_ranges_())) {
    LOG_WARN("fail to parse key ranges", KR(ret), K_(key_ranges));
  } else if (OB_FAIL(prepare_ls_locations_())) {
    LOG_WARN("fail to prepare log stream locations", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualProxyLSLocation::parse_key_ranges_()
{
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  const ObObj *start_key_obj_ptr = NULL;
  const ObObj *end_key_obj_ptr = NULL;
  for (int64_t i = 0; (OB_SUCC(ret)) && (i < key_ranges_.count()); ++i) {
    start_key = key_ranges_.at(i).start_key_;
    end_key = key_ranges_.at(i).end_key_;
    if (OB_UNLIKELY(ROW_KEY_COUNT != start_key.get_obj_cnt()
        || ROW_KEY_COUNT != end_key.get_obj_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "row key count not match");
      LOG_WARN("row key count not match", KR(ret), K(ROW_KEY_COUNT),
          "start key count", start_key.get_obj_cnt(), "end key count", end_key.get_obj_cnt());
    } else {
      start_key_obj_ptr = start_key.get_obj_ptr();
      end_key_obj_ptr = end_key.get_obj_ptr();
      if (OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey objects are null", KR(ret), KP(start_key_obj_ptr), KP(end_key_obj_ptr));
      } else if (OB_UNLIKELY(!start_key_obj_ptr[TENANT_ID_IDX].is_int()
          || !end_key_obj_ptr[TENANT_ID_IDX].is_int()
          || start_key_obj_ptr[TENANT_ID_IDX] != end_key_obj_ptr[TENANT_ID_IDX]
          || start_key_obj_ptr[TENANT_ID_IDX].get_int() <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_id must be specified as an equality or IN predicate");
        LOG_WARN("invalid tenant_id range", KR(ret),
            "start tenant id", start_key_obj_ptr[TENANT_ID_IDX],
            "end tenant id", end_key_obj_ptr[TENANT_ID_IDX]);
      } else {
        const uint64_t tenant_id = static_cast<uint64_t>(start_key_obj_ptr[TENANT_ID_IDX].get_int());
        if (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_) {
          // ret = OB_SUCCESS, warn and skip this table without failing the query.
          LOG_USER_WARN(OB_ERR_NO_PRIVILEGE, "sys tenant");
        } else if (OB_FAIL(add_tenant_id_(tenant_id))) {
          LOG_WARN("fail to add tenant id", KR(ret), K(tenant_id));
        }
      }
    }
  } // end for key_ranges_
  return ret;
}

int ObAllVirtualProxyLSLocation::add_tenant_id_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  for (int64_t i = 0; !is_exist && i < tenant_ids_.count(); ++i) {
    is_exist = tenant_id == tenant_ids_.at(i);
  }
  if (!is_exist && OB_FAIL(tenant_ids_.push_back(tenant_id))) {
    LOG_WARN("fail to push back tenant id", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObAllVirtualProxyLSLocation::prepare_ls_locations_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSLocation, 4> tenant_locations;
  ls_locations_.reset();
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location service is not initialized", KR(ret), KP(GCTX.location_service_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = tenant_ids_.at(i);
    tenant_locations.reset();
    if (OB_FAIL(GCTX.location_service_->get_tenant_ls_locations(tenant_id, tenant_locations))) {
      LOG_WARN("fail to get tenant log stream locations", KR(ret), K(tenant_id));
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tenant_locations.count(); ++idx) {
      const ObLSLocation &location = tenant_locations.at(idx);
      if (OB_UNLIKELY(!location.is_valid() || tenant_id != location.get_tenant_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid log stream location", KR(ret), K(tenant_id), K(location));
      } else if (OB_FAIL(ls_locations_.push_back(location))) {
        LOG_WARN("fail to push back log stream location", KR(ret), K(location));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyLSLocation::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool has_row = false;
  if (!is_inited_) {
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret) && !has_row && next_location_idx_ < ls_locations_.count()) {
    const ObLSLocation &location = ls_locations_.at(next_location_idx_);
    if (next_replica_idx_ >= location.get_replica_locations().count()) {
      ++next_location_idx_;
      next_replica_idx_ = 0;
    } else {
      const ObLSReplicaLocation &replica = location.get_replica_locations().at(next_replica_idx_++);
      if (OB_FAIL(fill_row_(location, replica))) {
        LOG_WARN("fail to fill row", KR(ret), K(location), K(replica));
      } else {
        has_row = true;
      }
    }
  }
  if (OB_SUCC(ret) && !has_row) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualProxyLSLocation::fill_row_(
    const share::ObLSLocation &location,
    const share::ObLSReplicaLocation &replica)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  ObCollationType coll_type =
      ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", KR(ret));
  }
  for (int64_t i = 0; (OB_SUCC(ret)) && (i < col_count); ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID: {
        cells[i].set_int(static_cast<int64_t>(location.get_tenant_id()));
        break;
      }
      case LS_ID: {
        cells[i].set_int(location.get_ls_id().id());
        break;
      }
      case SVR_IP: {
        if (OB_UNLIKELY(!replica.get_server().ip_to_string(ip_buf_, OB_IP_STR_BUFF))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to convert ip to string", KR(ret), K(replica));
        } else {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(coll_type);
        }
        break;
      }
      case SQL_PORT: {
        cells[i].set_int(replica.get_sql_port());
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(replica.get_server().get_port());
        break;
      }
      case ROLE: {
        cells[i].set_int(static_cast<int64_t>(replica.get_role()));
        break;
      }
      case REPLICA_TYPE: {
        cells[i].set_int(static_cast<int64_t>(replica.get_replica_type()));
        break;
      }
      case PROPOSAL_ID: {
        cells[i].set_int(replica.get_proposal_id());
        break;
      }
      case SPARE1:
      case SPARE2:
      case SPARE3: {
        cells[i].set_int(0);
        break;
      }
      case SPARE4:
      case SPARE5:
      case SPARE6: {
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column id", KR(ret), K(i), K(col_id));
        break;
      }
    }
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
