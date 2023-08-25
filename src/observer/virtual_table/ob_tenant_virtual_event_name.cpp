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

#include "ob_tenant_virtual_event_name.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

ObTenantVirtualEventName::ObTenantVirtualEventName()
    : ObVirtualTableScannerIterator(),
    event_iter_(0),
    tenant_id_(OB_INVALID_ID)
{
}

ObTenantVirtualEventName::~ObTenantVirtualEventName()
{
  reset();
}

void ObTenantVirtualEventName::reset()
{
  ObVirtualTableScannerIterator::reset();
  event_iter_ = 0;
  tenant_id_ = OB_INVALID_ID;
  for (int64_t i = 0; i  < OB_ROW_MAX_COLUMNS_COUNT; i++) {
    cells_[i].reset();
  }
}

int ObTenantVirtualEventName::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K_(tenant_id), K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    cur_row_.cells_ = cells_;
    cur_row_.count_ = reserved_column_cnt_;

    if (event_iter_ >= WAIT_EVENTS_TOTAL) {
      ret = OB_ITER_END;
    }

    if (OB_SUCC(ret)) {
      uint64_t cell_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch(col_id) {
           case TENANT_ID: {
            cells_[cell_idx].set_int(tenant_id_);
            break;
          }
          case EVENT_ID: {
            cells_[cell_idx].set_int(OB_WAIT_EVENTS[event_iter_].event_id_);
            break;
          }
          case EVENT_NO: {
            cells_[cell_idx].set_int(event_iter_);
            break;
          }
          case NAME: {
            cells_[cell_idx].set_varchar(OB_WAIT_EVENTS[event_iter_].event_name_);
            cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DISPLAY_NAME: {
            cells_[cell_idx].set_varchar(OB_WAIT_EVENTS[event_iter_].event_name_);
            cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case PARAMETER1: {
            cells_[cell_idx].set_varchar(OB_WAIT_EVENTS[event_iter_].param1_);
            cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case PARAMETER2: {
            cells_[cell_idx].set_varchar(OB_WAIT_EVENTS[event_iter_].param2_);
            cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case PARAMETER3: {
            cells_[cell_idx].set_varchar(OB_WAIT_EVENTS[event_iter_].param3_);
            cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case WAIT_CLASS_ID: {
            cells_[cell_idx].set_int(OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_iter_].wait_class_].wait_class_id_);
            break;
          }
          case WAIT_CLASS_NO: {
            cells_[cell_idx].set_int(OB_WAIT_EVENTS[event_iter_].wait_class_);
            break;
          }
          case WAIT_CLASS: {
            cells_[cell_idx].set_varchar(OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_iter_].wait_class_].wait_class_);
            cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                       K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      event_iter_++;
      row = &cur_row_;
    }
  }
  return ret;
}

} /* namespace observer */
} /* namespace oceanbase */
