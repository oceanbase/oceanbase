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

#include "ob_all_virtual_partition_sstable_merge_info.h"
#include "share/ob_errno.h"
#include "storage/ob_sstable.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace storage;
using namespace common;
namespace observer {
ObAllVirtualPartitionSSTableMergeInfo::ObAllVirtualPartitionSSTableMergeInfo()
    : merge_info_(), merge_info_iter_(), is_inited_(false)
{}
ObAllVirtualPartitionSSTableMergeInfo::~ObAllVirtualPartitionSSTableMergeInfo()
{
  reset();
}

int ObAllVirtualPartitionSSTableMergeInfo::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualPartitionSSTableMergeInfo has been inited, ", K(ret));
  } else if (OB_FAIL(merge_info_iter_.open())) {
    SERVER_LOG(WARN, "Fail to open merge info iter, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMergeInfo::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualPartitionSSTableMergeInfo has been inited, ", K(ret));
  } else if (OB_FAIL(merge_info_iter_.get_next_merge_info(merge_info_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next merge info, ", K(ret));
    }
  } else if (OB_FAIL(fill_cells(&merge_info_))) {
    STORAGE_LOG(WARN, "Fail to fill cells, ", K(ret), K(merge_info_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMergeInfo::fill_cells(ObSSTableMergeInfo* sstable_merge_info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  if (OB_ISNULL(sstable_merge_info)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(sstable_merge_info));

  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case VERSION:
          // version
          if (OB_SUCC(sstable_merge_info->version_.version_to_string(version_buf_, sizeof(version_buf_)))) {
            cells[i].set_varchar(version_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case SVR_IP:
          // svr_ip
          if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case SVR_PORT:
          // svr_port
          cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
          break;
        case TENANT_ID:
          // tenant_id
          cells[i].set_int(extract_tenant_id(sstable_merge_info->table_id_));
          break;
        case TABLE_ID:
          // table_id
          cells[i].set_int(sstable_merge_info->table_id_);
          break;
        case PARTITION_ID:
          // partition_id
          cells[i].set_int(sstable_merge_info->partition_id_);
          break;
        case MERGE_TYPE: {
          // merge_type
          const char* merge_type_str = nullptr;
          if (sstable_merge_info->is_complement_) {
            merge_type_str = "mini complement";
          } else {
            merge_type_str = merge_type_to_str(sstable_merge_info->merge_type_);
          }
          cells[i].set_varchar(merge_type_str);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SNAPSHOT_VERSION:
          cells[i].set_int(sstable_merge_info->snapshot_version_);
          break;
        case TABLE_TYPE:
          // table_type
          cells[i].set_int(sstable_merge_info->table_type_);
          break;
        case MAJOR_TABLE_ID:
          // major_table_id
          cells[i].set_int(sstable_merge_info->major_table_id_);
          break;
        case MERGE_START_TIME:
          // merge_start_time
          cells[i].set_int(sstable_merge_info->merge_start_time_);
          break;
        case MERGE_FINISH_TIME:
          // merge_finish_time
          cells[i].set_int(sstable_merge_info->merge_finish_time_);
          break;
        case MERGE_COST_TIME:
          // merge_cost_time
          cells[i].set_int(sstable_merge_info->merge_cost_time_);
          break;
        case ESTIMATE_COST_TIME:
          // estimate_cost_time
          cells[i].set_int(sstable_merge_info->estimate_cost_time_);
          break;
        case OCCUPY_SIZE:
          // occupy_size
          cells[i].set_int(sstable_merge_info->occupy_size_);
          break;
        case MACRO_BLOCK_COUNT:
          // macro_block_count
          cells[i].set_int(sstable_merge_info->macro_block_count_);
          break;
        case USE_OLD_MACRO_BLOCK_COUNT:
          // use_old_macro_block_count
          cells[i].set_int(sstable_merge_info->use_old_macro_block_count_);
          break;
        case BUILD_BLOOMFILTER_COUNT:
          // build_bloomfilter_count
          cells[i].set_int(sstable_merge_info->macro_bloomfilter_count_);
          break;
        case TOTAL_ROW_COUNT:
          // total_row_count
          cells[i].set_int(sstable_merge_info->total_row_count_);
          break;
        case DELETE_ROW_COUNT:
          // delete_row_count
          cells[i].set_int(sstable_merge_info->delete_row_count_);
          break;
        case INSERT_ROW_COUNT:
          // insert_row_count
          cells[i].set_int(sstable_merge_info->insert_row_count_);
          break;
        case UPDATE_ROW_COUNT:
          // update_row_count
          cells[i].set_int(sstable_merge_info->update_row_count_);
          break;
        case BASE_ROW_COUNT:
          // base_row_count
          cells[i].set_int(sstable_merge_info->base_row_count_);
          break;
        case USE_BASE_ROW_COUNT:
          // use_base_row_count
          cells[i].set_int(sstable_merge_info->use_base_row_count_);
          break;
        case MEMTABLE_ROW_COUNT:
          // memtable_row_count
          cells[i].set_int(sstable_merge_info->memtable_row_count_);
          break;
        case PURGED_ROW_COUNT:
          // PURGED_ROW_COUNT
          cells[i].set_int(sstable_merge_info->purged_row_count_);
          break;
        case OUTPUT_ROW_COUNT:
          // output_row_count
          cells[i].set_int(sstable_merge_info->output_row_count_);
          break;
        case MERGE_LEVEL:
          // merge_level
          cells[i].set_int(sstable_merge_info->merge_level_);
          break;
        case REWRITE_MACRO_OLD_MICRO_BLOCK_COUNT:
          // rewrite_macro_old_micro_block_count
          cells[i].set_int(sstable_merge_info->rewrite_macro_old_micro_block_count_);
          break;
        case REWRITE_MACRO_TOTAL_MICRO_BLOCK_COUNT:
          // rewrite_macor_total_micro_block_count
          cells[i].set_int(sstable_merge_info->rewrite_macro_total_micro_block_count_);
          break;
        case TOTAL_CHILD_TASK:
          // total_child_task
          cells[i].set_int(sstable_merge_info->total_child_task_);
          break;
        case FINISH_CHILD_TASK:
          // finish_child_task
          cells[i].set_int(sstable_merge_info->finish_child_task_);
          break;
        case STEP_MERGE_PERCENTAGE: {
          // step merge percentage
          if (sstable_merge_info->version_.major_ >= sstable_merge_info->step_merge_start_version_ &&
              sstable_merge_info->version_.major_ < sstable_merge_info->step_merge_end_version_) {
            if (0 != (sstable_merge_info->step_merge_end_version_ - sstable_merge_info->step_merge_start_version_)) {
              cells[i].set_int(
                  (sstable_merge_info->version_.major_ - sstable_merge_info->step_merge_start_version_ + 1) * 100 /
                  (sstable_merge_info->step_merge_end_version_ - sstable_merge_info->step_merge_start_version_));
            } else {
              cells[i].set_int(0);
            }
          } else {
            cells[i].set_int(100);
          }
          break;
        }
        case MERGE_PERCENTAGE: {
          // merge percentage
          if (sstable_merge_info->total_child_task_ > 0) {
            cells[i].set_int((sstable_merge_info->finish_child_task_ * 100) / sstable_merge_info->total_child_task_);
          } else {
            cells[i].set_int(0);
          }
          break;
        }
        case ERROR_CODE:
          cells[i].set_int(sstable_merge_info->error_code_);
          break;
        case TABLE_COUNT: {
          cells[i].set_int(sstable_merge_info->table_count_);
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
      }
    }
  }

  return ret;
}
void ObAllVirtualPartitionSSTableMergeInfo::reset()
{
  ObVirtualTableScannerIterator::reset();
  merge_info_iter_.reset();
  memset(version_buf_, 0, sizeof(version_buf_));
}

} /* namespace observer */
} /* namespace oceanbase */
