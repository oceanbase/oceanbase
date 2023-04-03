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

#include "ob_all_virtual_tenant_ctx_memory_info.h"

#include "lib/alloc/memory_dump.h"

namespace oceanbase
{
using namespace common;

namespace observer
{
ObAllVirtualTenantCtxMemoryInfo::ObAllVirtualTenantCtxMemoryInfo()
    : has_start_(false)
{
}

ObAllVirtualTenantCtxMemoryInfo::~ObAllVirtualTenantCtxMemoryInfo()
{
  reset();
}

int ObAllVirtualTenantCtxMemoryInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

void ObAllVirtualTenantCtxMemoryInfo::reset()
{
  has_start_ = false;
}

int ObAllVirtualTenantCtxMemoryInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  int tenant_cnt = 0;
  if (has_start_) {
    // do nothing
  } else {
    get_tenant_ids(tenant_ids_, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);
    for (int i = 0; i < tenant_cnt; ++i) {
      uint64_t tenant_id = tenant_ids_[i];
      for (int ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
        auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
        if (NULL == ta) {
          ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                                      ctx_id);
        }
        if (OB_ISNULL(ta)) {
          // do nothing
        } else {
          ret = add_row(tenant_id, ctx_id, ta->get_hold(), ta->get_used(), ta->get_limit());
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      has_start_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualTenantCtxMemoryInfo::add_row(uint64_t tenant_id, int64_t ctx_id, int64_t hold, int64_t used, int64_t limit)
{
  int ret = OB_SUCCESS;
  ObObj *cells = nullptr;
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID: {
          cells[i].set_int(tenant_id);
          break;
        }
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(GCONF.self_addr_.get_port());
          break;
        }
        case CTX_ID: {
          cells[i].set_int(ctx_id);
          break;
        }
        case CTX_NAME: {
          cells[i].set_varchar(get_global_ctx_info().get_ctx_name(ctx_id));
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case HOLD: {
          cells[i].set_int(hold);
          break;
        }
        case USED: {
          cells[i].set_int(used);
          break;
        }
        case LIMIT: {
          cells[i].set_int(limit);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
          break;
        }
      }
    } // iter column end
    if (OB_SUCC(ret)) {
      // scanner最大支持64M，因此暂不考虑溢出的情况
      if (OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

} // observer
} // oceanbase
