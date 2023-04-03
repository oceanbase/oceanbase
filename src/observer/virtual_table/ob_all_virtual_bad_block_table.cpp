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

#include "ob_all_virtual_bad_block_table.h"

#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObVirtualBadBlockTable::ObVirtualBadBlockTable()
  : is_inited_(false), cursor_(0), addr_(), bad_block_infos_()
{
}

ObVirtualBadBlockTable::~ObVirtualBadBlockTable()
{
}

int ObVirtualBadBlockTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(cells) || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "Not inited", KP(cur_row_.cells_), K(is_inited_), K(ret));
  } else if (cursor_ >= bad_block_infos_.count()) {
    row = NULL;
    ret = OB_ITER_END;
  } else {
    const blocksstable::ObBadBlockInfo &bad_block_info = bad_block_infos_[cursor_];
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id){
        case SVR_IP: {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case DISK_ID: {
          cells[i].set_int(bad_block_info.disk_id_);
          break;
        }
        case STORE_FILE_PATH: {
          cells[i].set_varchar(bad_block_info.store_file_path_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MACRO_BLOCK_INDEX: {
          cells[i].set_int(bad_block_info.macro_block_id_.block_index());
          break;
        }
        case ERROR_TYPE: {
          cells[i].set_int(bad_block_info.error_type_);
          break;
        }
        case ERROR_MSG: {
          cells[i].set_varchar(bad_block_info.error_msg_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case CHECK_TIME: {
          cells[i].set_timestamp(bad_block_info.check_time_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id, ", K(ret), K(i), K(output_column_ids_), K(col_id));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    ++cursor_;
  }
  return ret;
}

void ObVirtualBadBlockTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  cursor_ = 0;
  addr_.reset();
  bad_block_infos_.reset();
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

int ObVirtualBadBlockTable::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Invalid argument, ", K(ret), K(addr));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.get_bad_block_infos(bad_block_infos_))) {
    SERVER_LOG(WARN, "Fail to get bad block infos", K(ret));
  } else {
    addr_ = addr;
    MEMSET(ip_buf_, 0, sizeof(ip_buf_));
    if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ip to string failed", K(ret), K(addr_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

}// namespace observer
}// namespace oceanbase


