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

#include "ob_all_virtual_partition_table_store_stat.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_pg_storage.h"
#include "share/ob_errno.h"

namespace oceanbase {
using namespace storage;
using namespace common;
namespace observer {
ObAllVirtualPartitionTableStoreStat::ObAllVirtualPartitionTableStoreStat()
    : stat_(), stat_iter_(), partition_service_(NULL), is_inited_(false)
{
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(rowkey_prefix_info_, 0, sizeof(rowkey_prefix_info_));
}

ObAllVirtualPartitionTableStoreStat::~ObAllVirtualPartitionTableStoreStat()
{
  reset();
}

int ObAllVirtualPartitionTableStoreStat::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualPartitionTableStoreStat has been inited", K(ret));
  } else if (OB_FAIL(stat_iter_.open())) {
    SERVER_LOG(WARN, "Open iterator fail", K(ret));
  } else {
    stat_.reset();
    partition_service_ = partition_service;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualPartitionTableStoreStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualPartitionTableStoreStat has not been inited", K(ret));
  } else if (OB_FAIL(stat_iter_.get_next_stat(stat_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get stat info", K(ret));
    }
  } else if (OB_FAIL(fill_cells(stat_))) {
    STORAGE_LOG(WARN, "Fail to fill cells, ", K(ret), K(stat_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

void ObAllVirtualPartitionTableStoreStat::reset()
{
  ObVirtualTableScannerIterator::reset();
  stat_.reset();
  stat_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(rowkey_prefix_info_, 0, sizeof(rowkey_prefix_info_));
  partition_service_ = NULL;
  is_inited_ = false;
}

int ObAllVirtualPartitionTableStoreStat::fill_cells(const ObTableStoreStat& stat)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  if (!stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(stat));
  } else {
    if (OB_FAIL(get_rowkey_prefix_info(stat.pkey_))) {
      if (OB_PARTITION_NOT_EXIST != ret) {
        SERVER_LOG(WARN, "get_rowkey_prefix_info failed", K(stat));
      }
      ret = OB_SUCCESS;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cells[i].set_int(stat.pkey_.get_tenant_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // table_id
          cells[i].set_int(stat.pkey_.get_table_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // partition_id
          cells[i].set_int(stat.pkey_.get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // row_cache_hit_count
          cells[i].set_int(stat.row_cache_hit_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // row_cache_miss_count
          cells[i].set_int(stat.row_cache_miss_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // row_cache_put_count
          cells[i].set_int(stat.row_cache_put_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // bf_filter_count
          cells[i].set_int(stat.bf_filter_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // bf_empty_read_count
          cells[i].set_int(stat.bf_empty_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // bf_access_count
          cells[i].set_int(stat.bf_access_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // block_cache_hit_count
          cells[i].set_int(stat.block_cache_hit_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // block_cache_miss_count
          cells[i].set_int(stat.block_cache_miss_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // access_row_count
          cells[i].set_int(stat.access_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // outout_row_count
          cells[i].set_int(stat.output_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // fuse_row_cache_hit_count
          cells[i].set_int(stat.fuse_row_cache_hit_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // fuse_row_cache_miss_count
          cells[i].set_int(stat.fuse_row_cache_miss_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // fuse_row_cache_put_count
          cells[i].set_int(stat.fuse_row_cache_put_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // single_get_call_count
          cells[i].set_int(stat.single_get_stat_.call_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          // single_get_output_row_count
          cells[i].set_int(stat.single_get_stat_.output_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          // multi_get_call_count
          cells[i].set_int(stat.multi_get_stat_.call_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 21:
          // multi_get_output_row_count
          cells[i].set_int(stat.multi_get_stat_.output_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 22:
          // index_back_call_count
          cells[i].set_int(stat.index_back_stat_.call_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 23:
          // index_back_output_row_count
          cells[i].set_int(stat.index_back_stat_.output_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 24:
          // single_scan_call_count
          cells[i].set_int(stat.single_scan_stat_.call_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 25:
          // single_scan_output_row_count
          cells[i].set_int(stat.single_scan_stat_.output_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 26:
          // multi_scan_call_count
          cells[i].set_int(stat.multi_scan_stat_.call_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 27:
          // multi_scan_output_row_count
          cells[i].set_int(stat.multi_scan_stat_.output_row_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 28:
          // exist_row and effect read
          cells[i].set_int(stat.exist_row_.effect_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 29:
          // exist_row and empty read
          cells[i].set_int(stat.exist_row_.empty_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 30:
          // get_row and effect read
          cells[i].set_int(stat.get_row_.effect_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 31:
          // get_row and empty read
          cells[i].set_int(stat.get_row_.empty_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 32:
          // scan_row and effect read
          cells[i].set_int(stat.scan_row_.effect_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 33:
          // scan_row and empty read
          cells[i].set_int(stat.scan_row_.empty_read_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 34:
          // rowkey prefix read info
          cells[i].set_varchar(rowkey_prefix_info_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
      }
    }
  }
  return ret;
}

int ObAllVirtualPartitionTableStoreStat::get_rowkey_prefix_info(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pg_guard;
  storage::ObIPartitionGroup* pg;
  storage::ObPartitionPrefixAccessStat prefix_stat;
  snprintf(rowkey_prefix_info_, COLUMN_DEFAULT_LENGTH, "[]");
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "Partition service is NULL");
  } else if (OB_FAIL(partition_service_->get_partition(pkey, pg_guard))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      SERVER_LOG(WARN, "partition_service get partition failed", K(pkey));
    }
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObIPartitionGroup is NULL", K(pkey));
  } else if (OB_FAIL(pg->get_pg_storage().get_partition_access_stat(pkey, prefix_stat))) {
    SERVER_LOG(WARN, "failed to get partition access stat", K(ret), K(pkey));
  } else {
    (void)prefix_stat.to_string(rowkey_prefix_info_, COLUMN_DEFAULT_LENGTH);
  }
  return ret;
}

} /* namespace observer */
} /* namespace oceanbase */
