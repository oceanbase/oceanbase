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

#include "observer/virtual_table/ob_all_virtual_table_modifications.h"
#include "storage/memtable/ob_memtable.h"
#include "observer/ob_server.h"
#include "storage/ob_table_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualTableModifications::ObAllVirtualTableModifications()
    : ObVirtualTableScannerIterator(), addr_(), infos_(), info_idx_(0), is_inited_(false)
{}

ObAllVirtualTableModifications::~ObAllVirtualTableModifications()
{
  reset();
}

void ObAllVirtualTableModifications::reset()
{
  addr_.reset();
  infos_.reset();
  info_idx_ = 0;
  is_inited_ = false;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTableModifications::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualTableModifications cannot init twice", K(ret));
  } else if (OB_FAIL(ObSSTableMergeInfoMgr::get_instance().get_modification_infos(infos_))) {
    SERVER_LOG(WARN, "Failed to get modification infos", K(ret));
  } else {
    info_idx_ = 0;
    is_inited_ = true;
  }

  return ret;
}

int ObAllVirtualTableModifications::get_next_table_info(ObTableModificationInfo*& info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualTableModifications is not inited", K(ret));
  } else if (info_idx_ >= infos_.count()) {
    ret = OB_ITER_END;
  } else {
    info = &infos_.at(info_idx_);
    info_idx_++;
  }

  return ret;
}

int ObAllVirtualTableModifications::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObTableModificationInfo* info = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualTableModifications not inited", K(ret), K_(is_inited));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_table_info(info))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next table modification info", K(ret));
    }
  } else if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "Unexpected null info", K(ret), K_(info_idx), K_(infos));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(extract_tenant_id(info->table_id_));
          break;
        case TABLE_ID:
          cur_row_.cells_[i].set_int(info->table_id_);
          break;
        case PARTITION_ID:
          cur_row_.cells_[i].set_int(info->partition_id_);
          break;
        case INSERT_ROW_COUNT:
          cur_row_.cells_[i].set_int(info->insert_row_count_);
          break;
        case UPDATE_ROW_COUNT:
          cur_row_.cells_[i].set_int(info->update_row_count_);
          break;
        case DELETE_ROW_COUNT:
          cur_row_.cells_[i].set_int(info->delete_row_count_);
          break;
        case MAX_SNAPSHOT_VERSION:
          cur_row_.cells_[i].set_int(info->max_snapshot_version_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
