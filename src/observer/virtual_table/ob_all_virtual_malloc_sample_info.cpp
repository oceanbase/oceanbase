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

#include "ob_all_virtual_malloc_sample_info.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/alloc/memory_dump.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{
ObMallocSampleInfo::ObMallocSampleInfo()
    : ObVirtualTableScannerIterator(),
      col_count_(0),
      opened_(false)
{}

ObMallocSampleInfo::~ObMallocSampleInfo()
{
  reset();
}

int ObMallocSampleInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}
void ObMallocSampleInfo::reset()
{
  col_count_ = 0;
  opened_ = false;
}
int ObMallocSampleInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!opened_) {
    col_count_ = output_column_ids_.count();
    if (OB_FAIL(malloc_sample_map_.create(1000, "MallocInfoMap", "MallocInfoMap"))) {
      SERVER_LOG(WARN, "create memory info map failed", K(ret));
    } else {
      ret = ObMemoryDump::get_instance().load_malloc_sample_map(malloc_sample_map_);
      if (OB_SUCC(ret)) {
        it_ = malloc_sample_map_.begin();
        opened_ = true;
      }
    }
  }

  for (; OB_SUCC(ret) && it_ != malloc_sample_map_.end(); ++it_) {
    if (is_sys_tenant(effective_tenant_id_) || effective_tenant_id_ == it_->first.tenant_id_) {
      if (OB_FAIL(fill_row(row))) {
        SERVER_LOG(WARN, "failed to fill row", K(ret));
      }
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (it_ != malloc_sample_map_.end()) {
      ++it_;
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}
int ObMallocSampleInfo::fill_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
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
      case TENANT_ID: {
          cells[i].set_int(it_->first.tenant_id_);
          break;
        }
      case CTX_ID: {
          cells[i].set_int(it_->first.ctx_id_);
          break;
        }
      case MOD_NAME: {
          cells[i].set_varchar(it_->first.label_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
      case BACKTRACE: {
          IGNORE_RETURN parray(bt_, sizeof(bt_), (int64_t*)it_->first.bt_, AOBJECT_BACKTRACE_COUNT);
          cells[i].set_varchar(bt_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
      }
      case CTX_NAME: {
          cells[i].set_varchar(get_global_ctx_info().get_ctx_name(it_->first.ctx_id_));
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
      case ALLOC_COUNT: {
          cells[i].set_int(it_->second.alloc_count_);
          break;
        }
      case ALLOC_BYTES: {
          cells[i].set_int(it_->second.alloc_bytes_);
          break;
        }
      default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

}
}
