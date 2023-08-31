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
#include "ob_all_virtual_schema_memory.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace observer
{
int ObAllVirtualSchemaMemory::inner_open()
{
  int ret = OB_SUCCESS;
  const ObAddr &addr = GCTX.self_addr();

  if (false == addr.ip_to_string(ip_buffer_, sizeof(ip_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", KR(ret), K(addr));
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", KR(ret), K_(effective_tenant_id));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(schema_service_.get_schema_store_tenants(tenant_ids_))) {
      LOG_WARN("fail to get schema store tenants", KR(ret));
    }
  } else {
    // user/meta tenant can see its own schema
    if (schema_service_.check_schema_store_tenant_exist(effective_tenant_id_)) {
      if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id_))) {
        LOG_WARN("fail to push back effective_tenant_id", KR(ret), K_(effective_tenant_id));
      }
    }
  }
  return ret;
}

int ObAllVirtualSchemaMemory::get_next_tenant_mem_info(ObSchemaMemory &schema_mem) {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (mem_idx_ >= schema_mem_infos_.count()) {
    do {
      schema_mem_infos_.reset();
      if (++tenant_idx_ >= tenant_ids_.count()) {
        ret = OB_ITER_END;
      } else {
        uint64_t tenant_id = tenant_ids_[tenant_idx_];
        if (OB_INVALID_TENANT_ID == tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tenant_id", KR(ret), K(tenant_idx_));
        //ignore single failture
        } else if (OB_SUCCESS != (tmp_ret = schema_service_.get_tenant_mem_info(tenant_id, schema_mem_infos_))) {
          LOG_WARN("fail to get tenant mem info", KR(tmp_ret), K(tenant_id));
          schema_mem_infos_.reset();
        } else {
          mem_idx_ = 0;
        }
      }
    } while (0 == schema_mem_infos_.count() && OB_SUCC(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(mem_idx_ < 0 || mem_idx_ >= schema_mem_infos_.count())) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("mem_idx_ out of range", KR(ret), K(mem_idx_));
    } else {
      schema_mem = schema_mem_infos_[mem_idx_++];
    }
  }
  return ret;
}

int ObAllVirtualSchemaMemory::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaMemory schema_mem;

  if (OB_FAIL(get_next_tenant_mem_info(schema_mem))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next tenant_mem_info", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t pos = schema_mem.get_pos();
    const uint64_t tenant_id = schema_mem.get_tenant_id();
    const int64_t used_schema_mgr_cnt = schema_mem.get_used_schema_mgr_cnt();
    const int64_t free_schema_mgr_cnt = schema_mem.get_free_schema_mgr_cnt();
    const int64_t mem_used = schema_mem.get_mem_used();
    const int64_t mem_total = schema_mem.get_mem_total();
    const int64_t allocator_idx = schema_mem.get_allocator_idx();

    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP: {
          cur_row_.cells_[i].set_varchar(common::ObString::make_string(ip_buffer_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(GCTX.self_addr().get_port()));
          break;
        }
        case TENANT_ID: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(tenant_id));
          break;
        }
        case ALLOCATOR_TYPE: {
          cur_row_.cells_[i].set_varchar( 0 == pos ? "current" : "another");
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case USED_SCHEMA_MGR_CNT: {
          cur_row_.cells_[i].set_int(used_schema_mgr_cnt);
          break;
        }
        case FREE_SCHEMA_MGR_CNT: {
          cur_row_.cells_[i].set_int(free_schema_mgr_cnt);
          break;
        }
        case MEM_USED: {
          cur_row_.cells_[i].set_int(mem_used);
          break;
        }
        case MEM_TOTAL: {
          cur_row_.cells_[i].set_int(mem_total);
          break;
        }
        case ALLOCATOR_IDX: {
          cur_row_.cells_[i].set_int(allocator_idx);
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid col_id", KR(ret), K(col_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}
} /* namespace observer */
} /* namespace oceanbase */
