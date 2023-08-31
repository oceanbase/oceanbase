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
#include "observer/virtual_table/ob_all_virtual_schema_slot.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_schema_mgr_cache.h"

namespace oceanbase
{
namespace observer
{
void ObAllVirtualSchemaSlot::reset(common::ObIAllocator &allocator, common::ObIArray<ObSchemaSlot> &tenant_slot_infos) 
{
  const char *ptr = NULL;
  common::ObString str;
  int ret = OB_SUCCESS;
  int len = tenant_slot_infos.count();

  for (int64_t i = 0; i < len && OB_SUCC(ret); ++i) {
    ptr = ((tenant_slot_infos.at(i)).get_mod_ref_infos()).ptr();
    if (OB_NOT_NULL(ptr)) {
      allocator.free(const_cast<char*>(ptr));
    }
    (tenant_slot_infos.at(i)).reset();
  }
  tenant_slot_infos.reset();
}

int ObAllVirtualSchemaSlot::inner_open()
{
  const ObAddr &addr = GCTX.self_addr();
  int ret = OB_SUCCESS;
  
  if (false == addr.ip_to_string(ip_buffer_, sizeof(ip_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", KR(ret), K(addr));
  } else if (OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", KR(ret), K_(effective_tenant_id));
  } else if(is_sys_tenant(effective_tenant_id_)) {
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

int ObAllVirtualSchemaSlot::get_next_tenant_slot_info(ObSchemaSlot &schema_slot) {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", KR(ret));
  } else if (slot_idx_ >= schema_slot_infos_.count()) {
    do {
      reset(*allocator_, schema_slot_infos_);
      if (++tenant_idx_ >= tenant_ids_.count()) {
        ret = OB_ITER_END;
      } else {
        uint64_t tenant_id = tenant_ids_[tenant_idx_];
        if (OB_INVALID_TENANT_ID == tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tenant_id", KR(ret), K(tenant_idx_));
        // ignore single failture
        } else if (OB_SUCCESS != (tmp_ret = schema_service_.get_tenant_slot_info(*allocator_, tenant_id, schema_slot_infos_))) {
          LOG_WARN("fail to get tenant slot info", KR(tmp_ret), K(tenant_id));
          reset(*allocator_, schema_slot_infos_);
        } else {
          slot_idx_ = 0;
        }
      }
    } while (0 == schema_slot_infos_.count() && OB_SUCC(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(slot_idx_ < 0 || slot_idx_ >= schema_slot_infos_.count())) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("slot_idx_ out of range", KR(ret), K(slot_idx_));
    } else {
      schema_slot = schema_slot_infos_[slot_idx_++];
    }
  }
  return ret;
}

int ObAllVirtualSchemaSlot::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaSlot schema_slot;

  if (OB_FAIL(get_next_tenant_slot_info(schema_slot))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next tenant_info", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = schema_slot.get_tenant_id();
    const int64_t slot_id = schema_slot.get_slot_id();
    const int64_t total_ref_cnt = schema_slot.get_ref_cnt();
    const int64_t schema_version = schema_slot.get_schema_version();
    const int64_t schema_count = schema_slot.get_schema_count();
    const int64_t allocator_idx = schema_slot.get_allocator_idx();
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
        case SLOT_ID: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(slot_id));
          break;
        }
        case SCHEMA_VERSION: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(schema_version));
          break;
        }
        case SCHEMA_COUNT: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(schema_count));
          break;
        }
        case REF_CNT: { 
          cur_row_.cells_[i].set_int(static_cast<int64_t>(total_ref_cnt));
          break;
        }
        case REF_INFO: {
          if (OB_NOT_NULL(schema_slot.get_mod_ref_infos().ptr())) {
            cur_row_.cells_[i].set_varchar(schema_slot.get_mod_ref_infos());
          } else {
            cur_row_.cells_[i].set_varchar("");
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
          break;
        }
        case ALLOCATOR_IDX: {
          cur_row_.cells_[i].set_int(static_cast<int64_t>(allocator_idx));
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
