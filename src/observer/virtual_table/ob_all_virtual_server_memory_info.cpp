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

#include "observer/virtual_table/ob_all_virtual_server_memory_info.h"
#include "observer/ob_server.h"
#include "share/ob_tenant_mgr.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace observer {

ObAllVirtualServerMemoryInfo::ObAllVirtualServerMemoryInfo()
    : ObVirtualTableScannerIterator(), tenant_mgr_(NULL), current_pos_(0), addr_()
{}

ObAllVirtualServerMemoryInfo::~ObAllVirtualServerMemoryInfo()
{
  reset();
}

void ObAllVirtualServerMemoryInfo::reset()
{
  tenant_mgr_ = NULL;
  current_pos_ = 0;
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualServerMemoryInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_ || NULL == tenant_mgr_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or tenant_mgr_ shouldn't be NULL", K(allocator_), K(tenant_mgr_), K(ret));
  } else if (!start_to_read_) {
    ObObj* cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_ID;
      char ip_buf[common::OB_IP_STR_BUFF];
      common::ObArray<uint64_t> keys;
      int64_t memory_hold = lib::get_memory_hold();
      int64_t memory_limit = lib::get_memory_limit();
      int64_t system_reserved = GCONF.get_reserved_server_memory();
      int64_t freeze_trigger = GCONF.get_server_memory_avail() * GCONF.get_global_freeze_trigger_percentage() / 100;
      int64_t memstore_limit = GCONF.get_server_memory_avail() * GCONF.get_global_memstore_limit_percentage() / 100;
      int64_t server_active_memstore_used = 0;
      int64_t server_total_memstore_used = 0;
      if (OB_FAIL(tenant_mgr_->get_all_tenant_id(keys))) {
        SERVER_LOG(WARN, "get tenant id error", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
        tenant_id = keys.at(i);
        int64_t active_memstore_used = 0;
        int64_t total_memstore_used = 0;
        int64_t major_freeze_trigger = 0;
        int64_t memstore_limit = 0;
        int64_t unused = 0;
        if (OB_SUCCESS !=
            (ret = tenant_mgr_->get_tenant_memstore_cond(
                 tenant_id, active_memstore_used, total_memstore_used, major_freeze_trigger, memstore_limit, unused))) {
          SERVER_LOG(WARN, "fail to get memstore used", K(ret));
        } else {
          server_active_memstore_used += active_memstore_used;
          server_total_memstore_used += total_memstore_used;
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case SERVER_IP:
            (void)addr_.ip_to_string(ip_buf, sizeof(ip_buf));
            cells[i].set_varchar(ip_buf);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          case SERVER_PORT:
            cells[i].set_int(addr_.get_port());
            break;
          case SERVER_MEMORY_HOLD:
            cells[i].set_int(memory_hold);
            break;
          case SERVER_MEMORY_LIMIT:
            cells[i].set_int(memory_limit);
            break;
          case SYSTEM_RESERVED:
            cells[i].set_int(system_reserved);
            break;
          case ACTIVE_MEMSTORE_USED:
            cells[i].set_int(server_active_memstore_used);
            break;
          case TOTAL_MEMSTORE_USED:
            cells[i].set_int(server_total_memstore_used);
            break;
          case MAJOR_FREEZE_TRIGGER:
            cells[i].set_int(freeze_trigger);
            break;
          case MEMSTORE_LIMIT:
            cells[i].set_int(memstore_limit);
            break;
          default:
            // abnormal column id
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(ret));
            break;
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
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

}  // namespace observer
}  // namespace oceanbase
