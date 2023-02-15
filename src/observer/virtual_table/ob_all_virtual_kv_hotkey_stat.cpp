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

#include "ob_all_virtual_kv_hotkey_stat.h"
#include "observer/table/ob_table_throttle_manager.h"
#include "observer/ob_server.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

namespace oceanbase {
namespace observer {

ObAllVirtualKvHotKeyStat::ObAllVirtualKvHotKeyStat()
    : ObVirtualTableScannerIterator(),
      inited_(false), closed_(false), idx(0),
      arena_allocator_(ObModIds::OB_TABLE_HOTKEY),
      virtual_tbl_mgr_()
{
  virtual_tbl_mgr_.set_allocator(&arena_allocator_);
}

ObAllVirtualKvHotKeyStat::~ObAllVirtualKvHotKeyStat()
{
}

int ObAllVirtualKvHotKeyStat::init()
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (!OBSERVER.get_self().ip_to_string(svr_ip_, sizeof(svr_ip_))) {
    ret = OB_BUF_NOT_ENOUGH;
    SERVER_LOG(WARN, "svr ip buffer is not enough", K(ret));
  } else if (OB_FAIL(virtual_tbl_mgr_.init())) {
    SERVER_LOG(WARN, "could not init virtual table manager", K(ret));
  } else if (OB_FAIL(virtual_tbl_mgr_.set_iterator())) {
    SERVER_LOG(WARN, "could not set virtual table manager", K(ret));
  } else {
    inited_ = true;
    svr_port_ = OBSERVER.get_self().get_port();
  }

  return ret;
}

int ObAllVirtualKvHotKeyStat::inner_open()
{
  int ret = OB_SUCCESS;
  if(closed_) {
    // do nothing
  } else if (OB_FAIL(init())) {
    SERVER_LOG(WARN, "fail to init hotkey stat virtual table", K(ret));
  }
  return ret;
}

int ObAllVirtualKvHotKeyStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (closed_) {
    ret = OB_ITER_END;
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualKvHotKeyStat has not been inited, ", K(ret));
  } else if (OB_FAIL(fill_cells())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to fill cells, ", K(ret));
    }
  } else {
    row = &cur_row_;
  }

  return ret;
}

int ObAllVirtualKvHotKeyStat::inner_close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAllVirtualKvHotKeyStat::fill_cells()
{
  int ret = OB_SUCCESS;
  if (virtual_tbl_mgr_.is_end()) {
    ret = OB_ITER_END;
  } else {
    ObObj *cells = cur_row_.cells_;
    ObTableStatHotkey &info = virtual_tbl_mgr_.inner_get_next_row();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch(column_id) {
        case TENANT_ID: {
          cells[i].set_int(info.tenant_id_);
          break;
        }
        case DATABASE_ID: {
          cells[i].set_int(info.database_id_);
          break;
        }
        case PARTITION_ID: {
          cells[i].set_int(info.partition_id_);
          break;
        }
        case TABLE_ID: {
          cells[i].set_int(info.table_id_);
          break;
        }
        case SVR_IP: {
          cells[i].set_varchar(svr_ip_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(svr_port_);
          break;
        }
        case HOTKEY: {
          cells[i].set_varchar(to_hotkey_string(info.hotkey_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case HOTKEY_TYPE: {
          switch (info.hotkey_type_) {
            case HotKeyType::TABLE_HOTKEY_ALL: {
              cells[i].set_varchar("TABLE_HOTKEY_ALL");
              break;
            }
            case HotKeyType::TABLE_HOTKEY_INSERT: {
              cells[i].set_varchar("TABLE_HOTKEY_INSERT");
              break;
            }
            case HotKeyType::TABLE_HOTKEY_DELETE: {
              cells[i].set_varchar("TABLE_HOTKEY_DELETE");
              break;
            }
            case HotKeyType::TABLE_HOTKEY_UPDATE: {
              cells[i].set_varchar("TABLE_HOTKEY_UPDATE");
              break;
            }
            case HotKeyType::TABLE_HOTKEY_QUERY: {
              cells[i].set_varchar("TABLE_HOTKEY_QUERY");
              break;
            }
            default: {
              cells[i].set_varchar("INVALID");
            }
          }
          break;
        }
        case HOTKEY_FREQ: {
          cells[i].set_int(info.hotkey_freq_);
          break;
        }
        case THROTTLE_PERCENT:{
          cells[i].set_int(info.throttle_percent);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id, ", K(ret), K(column_id));
        }
      }
    }
  }
  ++idx;

  return ret;
}

void ObAllVirtualKvHotKeyStat::print_rowkey_range_value(ObObj rowkey, char* buf, int64_t buf_len, int64_t& pos)
{
  if (rowkey.is_string_type()) {
    // for Unicode character set
    rowkey.print_str_with_repeat(buf, buf_len, pos);
  } else {
    rowkey.print_format(buf, buf_len, pos);
  }
}

char* ObAllVirtualKvHotKeyStat::to_hotkey_string(ObRowkey rowkey)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey set into hotkey stat virtual table", K(rowkey), K(ret));
  } else {
    int64_t obj_cnt = rowkey.get_obj_cnt();
    common::ObObj* obj_ptr = rowkey.get_obj_ptr();

    memset(hotkey_buffer_, 0, STAT_HOTKEY_BUFFER_LEN);

    for (int i = 0; OB_SUCC(ret) && i < obj_cnt; ++i) {
      if (pos < STAT_HOTKEY_BUFFER_LEN) {
        if (!obj_ptr[i].is_max_value() && !obj_ptr[i].is_min_value()) {
          print_rowkey_range_value(obj_ptr[i], hotkey_buffer_, STAT_HOTKEY_BUFFER_LEN, pos);
        } else if (obj_ptr[i].is_min_value()) {
          if (OB_FAIL(databuff_printf(hotkey_buffer_, STAT_HOTKEY_BUFFER_LEN, pos, "MIN"))) {
            // do nothing
          }
        } else {
          if (OB_FAIL(databuff_printf(hotkey_buffer_, STAT_HOTKEY_BUFFER_LEN, pos, "MAX"))) {
            // do nothing
          }
        }
        if (OB_SUCC(ret)) {
          if (i < obj_cnt - 1) {
            if (OB_FAIL(databuff_printf(hotkey_buffer_, STAT_HOTKEY_BUFFER_LEN, pos, ","))) {
              // do nothing
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && ret != OB_SIZE_OVERFLOW) {
    LOG_WARN("unpected error when transforming rowkey", K(rowkey), K(ret));
  }
  return hotkey_buffer_;
}

void ObAllVirtualKvHotKeyStat::set_closed(bool closed)
{
  closed_ = closed;
}

} /* namespace observer */
} /* namespace oceanbase */
