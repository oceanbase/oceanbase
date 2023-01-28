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

#include "ob_all_virtual_memory_info.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/alloc/memory_dump.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"

namespace oceanbase
{
using namespace common;

namespace observer
{
ObAllVirtualMemoryInfo::ObAllVirtualMemoryInfo()
    : ObVirtualTableScannerIterator(),
      col_count_(0),
      has_start_(false)
{
}

ObAllVirtualMemoryInfo::~ObAllVirtualMemoryInfo()
{
  reset();
}

int ObAllVirtualMemoryInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

void ObAllVirtualMemoryInfo::reset()
{
  col_count_ = 0;
  has_start_ = false;
}

int ObAllVirtualMemoryInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!has_start_) {
    col_count_ = output_column_ids_.count();
    ObObj *cells = cur_row_.cells_;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      auto add_row = [&](uint64_t tenant_id, int64_t ctx_id,
                         const char *mod_name,
                         int64_t hold, int64_t used, int64_t count) {
        int ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
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
          case MOD_TYPE: {
              ObString mod_type;
              mod_type = ObString::make_string("user");
              cells[i].set_varchar(mod_type);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
          case MOD_ID: {
              // MOD_ID废弃，为兼容工具而保留
              cells[i].set_int(0);
              break;
            }
          case LABEL:
          case MOD_NAME: {
              cells[i].set_varchar(mod_name);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
          case ZONE: {
              cells[i].set_varchar(GCONF.zone);
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
          case COUNT: {
              cells[i].set_int(count);
              break;
            }
          case ALLOC_COUNT: {
              cells[i].set_int(0);
              break;
            }
          case FREE_COUNT: {
              cells[i].set_int(0);
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
        return ret;
      };
      int tenant_cnt = 0;

      // sys tenant show all tenant memory info
      if (is_sys_tenant(effective_tenant_id_)) {
        get_tenant_ids(tenant_ids_, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);
      } else {
        // user tenant show self tenant memory info
        tenant_ids_[0] = effective_tenant_id_;
        tenant_cnt = 1;
      }

      for (int tenant_idx = 0; OB_SUCC(ret) && tenant_idx < tenant_cnt; tenant_idx++) {
        uint64_t tenant_id = tenant_ids_[tenant_idx];
        for (int ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
          auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
          if (nullptr == ta) {
            ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                                        ctx_id);
          }
          if (nullptr == ta) {
            continue;
          }
          if (OB_SUCC(ret)) {
            ret = ta->iter_label([&](lib::ObLabel &label, LabelItem *l_item)
              {
                return add_row(tenant_id, ctx_id, label.str_, l_item->hold_, l_item->used_, l_item->count_);
              });
          }
        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        has_start_ = true;
      }
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

} // namespace observer
} // namespace oceanbase
