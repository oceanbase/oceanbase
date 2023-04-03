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

#include "observer/virtual_table/ob_all_virtual_tenant_memstore_info.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantMemstoreInfo::ObAllVirtualTenantMemstoreInfo()
    : ObVirtualTableScannerIterator(),
      current_pos_(0),
      addr_()
{
}

ObAllVirtualTenantMemstoreInfo::~ObAllVirtualTenantMemstoreInfo()
{
  reset();
}

void ObAllVirtualTenantMemstoreInfo::reset()
{
  current_pos_ = 0;
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTenantMemstoreInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_) {
    ObObj *cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_ID;
      char ip_buf[common::OB_IP_STR_BUFF];
      omt::ObMultiTenant *omt = GCTX.omt_;
      omt::TenantIdList current_ids(nullptr, ObModIds::OMT);
      if (OB_ISNULL(omt)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "omt is null", K(ret));
      } else {
        omt->get_tenant_ids(current_ids);
      }
      // does not check ret code, we need iter all the tenant.
      for (int64_t i = 0; i < current_ids.size(); ++i) {
        tenant_id = current_ids.at(i);
        int64_t active_span = 0;
        int64_t memstore_used = 0;
        int64_t freeze_trigger = 0;
        int64_t memstore_limit = 0;
        int64_t freeze_cnt = 0;
        if (is_virtual_tenant_id(tenant_id)
            || (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_)) {
          continue;
        }
        MTL_SWITCH(tenant_id) {
          storage::ObTenantFreezer *freezer = nullptr;
          if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
          } else if (OB_FAIL(freezer->get_tenant_memstore_cond(active_span,
                                                               memstore_used,
                                                               freeze_trigger,
                                                               memstore_limit,
                                                               freeze_cnt))) {
            SERVER_LOG(WARN, "fail to get memstore used", K(ret), K(tenant_id));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch (col_id) {
              case SERVER_IP:
                if (!addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
                  STORAGE_LOG(ERROR, "ip to string failed");
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  cells[i].set_varchar(ip_buf);
                  cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                }
                break;
              case SERVER_PORT:
                cells[i].set_int(addr_.get_port());
                break;
              case TENANT_ID:
                cells[i].set_int(tenant_id);
                break;
              case ACTIVE_SPAN:
                cells[i].set_int(active_span);
                break;
              case FREEZE_TRIGGER:
                cells[i].set_int(freeze_trigger);
                break;
              case FREEZE_CNT:
                cells[i].set_int(freeze_cnt);
                break;
              case MEMSTORE_USED:
                cells[i].set_int(memstore_used);
                break;
              case MEMSTORE_LIMIT:
                cells[i].set_int(memstore_limit);
                break;
              default:
                // abnormal column id
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "unexpected column id", K(ret));
                break;
            }
          }
          if (OB_SUCCESS == ret
              && OB_SUCCESS != (ret = scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          }
        }
      }
      // always start to read, event it failed.
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  // always get next row, if we have start to read.
  if (start_to_read_) {
    if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
