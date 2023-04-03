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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_virtual_trace_log.h"
#include "lib/string/ob_sql_string.h"
#include "lib/trace/ob_trace_event.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

ObVirtualTraceLog::ObVirtualTraceLog()
    : ObVirtualTableScannerIterator()
{
}

ObVirtualTraceLog::~ObVirtualTraceLog()
{
  reset();
}

void ObVirtualTraceLog::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObVirtualTraceLog::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      LOG_WARN( "fail to fill scanner", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN( "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObVirtualTraceLog::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN( "allocator is NULL", K(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN( "session_ is NULL", K(ret));
  } else if (OB_FAIL(fill_trace_buf())) {
    LOG_WARN("fail fill trace buf", K(ret));
  }
  return ret;
}

int ObVirtualTraceLog::fill_trace_buf()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObTraceEventRecorder *trace_buf = session_->get_trace_buf();
    if (NULL != trace_buf) {
      int64_t N = trace_buf->count();
      char buf1[512];
      char buf2[32];
      int64_t pos = 0;
      int64_t prev_ts = 0;
      ObString str;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        const ObTraceEvent &ev = trace_buf->get_event(i);
        if (ev.id_ <= OB_ID(__PAIR_NAME_BEGIN__)) {
          continue;
        }
        int cell_idx = 0;
        for (int j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
          int64_t col_id = output_column_ids_.at(j);
          switch(col_id) {
            case TITLE: {
              cells[cell_idx].set_varchar(ObString::make_string(name::get_description(ev.id_)));
              cells[cell_idx].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            };
            case KEY_VALUE: {
              if (ev.yson_end_pos_ > ev.yson_beg_pos_) {
                pos = 0;
                (void)::oceanbase::yson::databuff_print_elements(buf1, 512, pos,
                                                                 trace_buf->get_buffer() + ev.yson_beg_pos_, ev.yson_end_pos_-ev.yson_beg_pos_);
                str.assign_ptr(const_cast<const char*>(buf1), static_cast<int32_t>(pos));
                cells[cell_idx].set_varchar(str);
              } else {
                // no value
                cells[cell_idx].set_varchar("");
              }
              cells[cell_idx].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            };
            case TIME: {
              pos = 0;
              if (prev_ts == 0) {
                prev_ts = ev.timestamp_;
              }
              pos = snprintf(buf2, 32, "%ld", ev.timestamp_-prev_ts);
              prev_ts = ev.timestamp_;
              str.assign_ptr(const_cast<const char*>(buf2), static_cast<int32_t>(pos));
              cells[cell_idx].set_varchar(str);
              cells[cell_idx].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            };
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(ERROR, "invalid column id", K(ret), K(cell_idx), K(j),
                         K(output_column_ids_), K(col_id));
              break;
            }
          }
          if (OB_SUCC(ret)) {
            cell_idx++;
          }
        }
        if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN( "fail to add row", K(ret), K(cur_row_));
        }
      } // end for
    } else {
      session_->clear_trace_buf(); //避免下次打开trace log后show时会被show出来
    }
  }
  return ret;
}

}
}
