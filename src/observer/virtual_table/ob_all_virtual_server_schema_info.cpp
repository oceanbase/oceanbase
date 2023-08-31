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

# define USING_LOG_PREFIX SERVER
#include "ob_all_virtual_server_schema_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace observer
{
int ObAllVirtualServerSchemaInfo::inner_open()
{
  int ret = OB_SUCCESS;
  idx_ = 0;
  share::schema::ObSchemaGetterGuard guard;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", KR(ret), K_(effective_tenant_id));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    // all tenant's schema is visible in sys tenant
    if (OB_FAIL(guard.get_tenant_ids(tenant_ids_))) {
      LOG_WARN("fail to get tenant_ids", KR(ret));
    }
  } else {
    // user/meta tenant can see its own schema
    bool is_exist = false;
    if (OB_FAIL(guard.check_tenant_exist(effective_tenant_id_, is_exist))) {
      LOG_WARN("fail to check tenant exist", KR(ret), K_(effective_tenant_id));
    } else if (is_exist && OB_FAIL(tenant_ids_.push_back(effective_tenant_id_))) {
      LOG_WARN("fail to push back effective_tenant_id", KR(ret), K_(effective_tenant_id));
    }
  }
  return ret;
}

int ObAllVirtualServerSchemaInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (idx_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else if (OB_INVALID_TENANT_ID == tenant_ids_[idx_]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", K(ret));
  } else {
    const uint64_t tenant_id = tenant_ids_[idx_];
    const ObAddr &addr = GCTX.self_addr();
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    int64_t received_schema_version = OB_INVALID_VERSION;
    int64_t schema_count = OB_INVALID_ID;
    int64_t schema_size = OB_INVALID_ID;
    share::schema::ObSchemaGetterGuard schema_guard;
    if (false == addr.ip_to_string(ip_buffer_, sizeof(ip_buffer_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to convert ip to string", K(ret), K(addr));
    } else if (OB_FAIL(schema_service_.get_tenant_refreshed_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(schema_service_.get_tenant_received_broadcast_version(tenant_id, received_schema_version))) {
      LOG_WARN("fail to get tenant receieved schema version", K(ret), K(tenant_id), K(received_schema_version));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(tmp_ret), K(tenant_id));
      } else if (OB_SUCCESS != (tmp_ret = schema_guard.get_schema_count(tenant_id, schema_count))) {
        LOG_WARN("fail to get schema count", K(tmp_ret), K(tenant_id));
      }
    }

    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: { // svr_ip
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_buffer_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: { // svr_port
          cur_row_.cells_[i].set_int(static_cast<int64_t>(addr.get_port()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: { // tenant_id
          cur_row_.cells_[i].set_int(static_cast<int64_t>(tenant_id));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 3: { // refreshed_schema_version
          cur_row_.cells_[i].set_int(refreshed_schema_version);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 4: { // received_schema_version
          cur_row_.cells_[i].set_int(received_schema_version);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 5: { // schema_count
          cur_row_.cells_[i].set_int(schema_count);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 6: { // schema_size
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = schema_guard.get_schema_size(tenant_id, schema_size))) {
            cur_row_.cells_[i].set_int(OB_INVALID_ID);
            LOG_WARN("fail to get schema size", K(tmp_ret), K(tenant_id));
          } else {
            cur_row_.cells_[i].set_int(schema_size);
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 7: { // min_schema_version
          cur_row_.cells_[i].set_int(OB_INVALID_VERSION);
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid col_id", K(ret), K(col_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      idx_++;
    }
  }
  return ret;
}
}
}
