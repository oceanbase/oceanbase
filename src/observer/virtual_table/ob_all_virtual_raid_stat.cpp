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

#define USING_LOG_PREFIX STORAGE
#include "observer/ob_server.h"
#include "observer/virtual_table/ob_all_virtual_raid_stat.h"

using namespace oceanbase;
using namespace observer;
using namespace blocksstable;

ObDiskStat::ObDiskStat()
  : disk_idx_(-1),
    install_seq_(-1),
    create_ts_(-1),
    finish_ts_(-1),
    percent_(-1)
{
  alias_name_[0] = '\0';
  status_ = "";
}

ObDiskStats::ObDiskStats()
  : disk_stats_(),
    data_num_(0),
    parity_num_(0)
{
}

void ObDiskStats::reset()
{
  disk_stats_.reset();
  data_num_ = 0;
  parity_num_ = 0;
}

ObAllVirtualRaidStat::ObAllVirtualRaidStat()
  : disk_stats_(),
    cur_idx_(-1),
    addr_()
{
  ip_buf_[0] = '\0';
}

ObAllVirtualRaidStat::~ObAllVirtualRaidStat()
{
}

int ObAllVirtualRaidStat::init(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;

  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(addr));
  } else if (!addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to set ip buf", K(ret));
  } else {
    cur_idx_ = 0;
    addr_ = addr;
    start_to_read_ = true;
  }

  return ret;
}

void ObAllVirtualRaidStat::reset()
{
  disk_stats_.reset();
  cur_idx_ = -1;
  start_to_read_ = false;
}

int ObAllVirtualRaidStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret), K(start_to_read_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid pointer", K(allocator_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (cur_idx_ >= disk_stats_.disk_stats_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObDiskStat &stat = disk_stats_.disk_stats_.at(cur_idx_);
    const int64_t col_count = output_column_ids_.count();
    ++cur_idx_;

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case DISK_INDEX:
          cur_row_.cells_[i].set_int(stat.disk_idx_);
          break;
        case INSTALL_SEQ:
          cur_row_.cells_[i].set_int(stat.install_seq_);
          break;
        case DATA_NUM:
          cur_row_.cells_[i].set_int(disk_stats_.data_num_);
          break;
        case PARITY_NUM:
          cur_row_.cells_[i].set_int(disk_stats_.parity_num_);
          break;
        case CREATE_TS:
          cur_row_.cells_[i].set_int(stat.create_ts_);
          break;
        case FINISH_TS:
          cur_row_.cells_[i].set_int(stat.finish_ts_);
          break;
        case ALIAS_NAME:
          cur_row_.cells_[i].set_varchar(stat.alias_name_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case STATUS:
          cur_row_.cells_[i].set_varchar(stat.status_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case PERCENT:
          cur_row_.cells_[i].set_int(stat.percent_);
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

