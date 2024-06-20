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

#include "observer/virtual_table/ob_all_virtual_tracepoint_info.h"
#include "observer/ob_server_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace observer
{

ObAllTracepointInfo::ObAllTracepointInfo(): addr_(NULL)
{
}

ObAllTracepointInfo::~ObAllTracepointInfo()
{
}

void ObAllTracepointInfo::reset()
{
  addr_ = NULL;
}

int ObAllTracepointInfo::get_rows_from_tracepoint_info_list()
{
  int ret = OB_SUCCESS;
  ObString addr_ip;
  ObObj *cells = NULL;
  if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, addr_ip))) {
    SERVER_LOG(ERROR, "get server ip failed", K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else {
    DLIST_FOREACH_NORET(cur_tp_event, EventTable::global_item_list()) {
      for (int64_t cell_idx = 0; OB_SUCC(ret) &&
                    cell_idx < output_column_ids_.count(); ++cell_idx) {
        const uint64_t column_id = output_column_ids_.at(cell_idx);
        switch (column_id) {
          case ObAllTracepointInfo::INSPECT_COLUMN::SVR_IP: {
            cells[cell_idx].set_varchar(addr_ip);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::SVR_PORT: {
            cells[cell_idx].set_int(addr_->get_port());
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_NO: {
            cells[cell_idx].set_int(cur_tp_event->item_.no_);
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_NAME: {
            cells[cell_idx].set_varchar(cur_tp_event->item_.name_);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_DESCRIBE: {
            cells[cell_idx].set_varchar(cur_tp_event->item_.describe_);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_FREQUENCY: {
            cells[cell_idx].set_int(cur_tp_event->item_.trigger_freq_);
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_ERROR_CODE: {
            cells[cell_idx].set_int(cur_tp_event->item_.error_code_);
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_OCCUR: {
            cells[cell_idx].set_int(cur_tp_event->item_.occur_);
            break;
          }
          case ObAllTracepointInfo::INSPECT_COLUMN::TP_MATCH: {
            cells[cell_idx].set_int(cur_tp_event->item_.cond_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(cell_idx),
                K_(output_column_ids), K(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      }
    }
  }
  return ret;
}

int ObAllTracepointInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      SERVER_LOG(WARN, "fill scanner failed", K(ret));
    } else {
      start_to_read_ = true;
    }
  }

  if (OB_SUCC(ret) && start_to_read_) {
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

int ObAllTracepointInfo::fill_scanner()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or addr_ is null", K_(allocator), K_(addr), K(ret));
  } else {
    if (OB_FAIL(get_rows_from_tracepoint_info_list())) {
      SERVER_LOG(WARN, "get rows from tracepoint_info_list failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
