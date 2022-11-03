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

#include "observer/virtual_table/ob_all_virtual_server_blacklist.h"

namespace oceanbase
{
namespace observer
{
using namespace storage;
using namespace common;
using namespace share;
ObAllVirtualServerBlacklist::ObAllVirtualServerBlacklist()
{
  reset();
}

ObAllVirtualServerBlacklist::~ObAllVirtualServerBlacklist()
{
  reset();
}

void ObAllVirtualServerBlacklist::reset()
{
  ObVirtualTableScannerIterator::reset();
  ready_to_read_ = false;
  self_addr_.reset();
  memset(self_ip_buf_, 0, common::OB_IP_STR_BUFF);
  memset(dst_ip_buf_, 0, common::OB_IP_STR_BUFF);
  info_iter_.reset();
}

int ObAllVirtualServerBlacklist::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    SERVER_LOG(WARN, "invalid argument", KP_(allocator));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = ObServerBlacklist::get_instance().iterate_blacklist_info(info_iter_))) {
    SERVER_LOG(WARN, "iterate info error", K(ret));
  } else if (OB_SUCCESS != (ret = info_iter_.set_ready())) {
    SERVER_LOG(WARN, "iterate info begin error", K(ret));
  } else {
    ready_to_read_ = true;
  }

  return ret;
}

int ObAllVirtualServerBlacklist::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  ObBlacklistInfo bl_info;
  ObDstServerInfo dst_info;

  if (!ready_to_read_ && OB_FAIL(prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare to read error.", K(ret), K_(start_to_read));
  } else if (OB_SUCCESS != (ret = info_iter_.get_next(bl_info))) {
    if (OB_ITER_END == ret) {
      SERVER_LOG(DEBUG, "iterate info iter end", K(ret));
    } else {
      SERVER_LOG(WARN, "get next info error.", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    dst_info = bl_info.get_dst_info();

    if (OB_SUCC(ret)) {
      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
        uint64_t col_id = output_column_ids_.at(cell_idx);
        switch(col_id) {
          case SVR_IP: {
            (void) self_addr_.ip_to_string(self_ip_buf_, common::OB_IP_STR_BUFF);
            cells[cell_idx].set_varchar(self_ip_buf_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(self_addr_.get_port());
            break;
          }
          case DST_IP: {
            (void) bl_info.get_addr().ip_to_string(dst_ip_buf_, common::OB_IP_STR_BUFF);
            cells[cell_idx].set_varchar(dst_ip_buf_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DST_PORT: {
            cells[cell_idx].set_int(bl_info.get_addr().get_port());
            break;
          }
          case IS_IN_BLACKLIST: {
            cells[cell_idx].set_bool(dst_info.is_in_blacklist_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                       K(output_column_ids_), K(col_id));
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}
}/* ns observer*/
}/* ns oceanbase */
