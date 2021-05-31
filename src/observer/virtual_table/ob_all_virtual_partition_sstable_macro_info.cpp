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

#include "ob_all_virtual_partition_sstable_macro_info.h"
#include "storage/ob_partition_storage.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace storage;
using namespace common;
using namespace share;
using namespace share::schema;
namespace observer {

ObAllVirtualPartitionSSTableMacroInfo::ObAllVirtualPartitionSSTableMacroInfo()
    : partition_service_(NULL),
      partition_iter_(NULL),
      curr_partition_(NULL),
      curr_sstable_(NULL),
      tables_handler_(),
      sstable_cursor_(0),
      macro_iter_(),
      sstables_(),
      macro_desc_(),
      is_inited_(false)
{}

ObAllVirtualPartitionSSTableMacroInfo::~ObAllVirtualPartitionSSTableMacroInfo()
{
  reset();
}

int ObAllVirtualPartitionSSTableMacroInfo::init(ObPartitionService& partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualPartitionSSTableMergeInfo has been inited, ", K(ret));
  } else if (!ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string failed", K(ret));
  } else {
    partition_service_ = &partition_service;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMacroInfo::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualPartitionSSTableMergeInfo not inited, ", K(ret));
  } else {
    if (!start_to_read_) {
      if (NULL == (partition_iter_ = partition_service_->alloc_pg_partition_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        sstable_cursor_ = 0;
        macro_iter_.reset();
        start_to_read_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      while (OB_SUCC(ret) && (OB_ITER_END == (ret = macro_iter_.get_next_macro_block(macro_desc_)))) {
        ret = OB_SUCCESS;
        if (OB_FAIL(get_next_sstable())) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "get_next_sstable failed", K(ret));
          }
        } else {
          macro_iter_.reset();
          if (OB_FAIL(curr_sstable_->scan_macro_block_totally(macro_iter_))) {
            SERVER_LOG(WARN, "Fail to scan macro block, ", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(gen_row(row))) {
          SERVER_LOG(WARN, "gen_row failed", K(ret));
        }
      }
    }
  }

  return ret;
}

void ObAllVirtualPartitionSSTableMacroInfo::reset()
{
  key_ranges_.reset();
  if (NULL != partition_service_ && NULL != partition_iter_) {
    partition_service_->revert_pg_partition_iter(partition_iter_);
    partition_iter_ = NULL;
  }
  curr_partition_ = NULL;
  tables_handler_.reset();
  sstables_.reset();
  macro_iter_.reset();
  sstable_cursor_ = 0;
  curr_sstable_ = NULL;
  memset(objs_, 0, sizeof(objs_));
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualPartitionSSTableMacroInfo::set_key_ranges(const ObIArray<ObNewRange>& key_ranges)
{
  int ret = OB_SUCCESS;
  if (key_ranges.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid key_ranges", K(ret), K(key_ranges));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      const ObNewRange& key_range = key_ranges.at(i);
      ObNewRange range = key_range;
      range.start_key_.set_length(range.start_key_.length() - 1);
      range.end_key_.set_length(range.end_key_.length() - 1);
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();
      if (key_range.start_key_.ptr()[key_range.start_key_.length() - 1].is_max_value()) {
        range.border_flag_.unset_inclusive_start();
      }
      if (key_range.end_key_.ptr()[key_range.end_key_.length() - 1].is_min_value()) {
        range.border_flag_.unset_inclusive_end();
      }
      if (OB_FAIL(key_ranges_.push_back(range))) {
        SERVER_LOG(WARN, "push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMacroInfo::gen_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(curr_partition_->get_partition_key().get_tenant_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // table_id
          cur_row_.cells_[i].set_int(curr_sstable_->get_meta().index_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // partition_id
          cur_row_.cells_[i].set_int(curr_partition_->get_partition_key().get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // data_version
          cur_row_.cells_[i].set_int(curr_sstable_->get_meta().data_version_);
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // base_version
          cur_row_.cells_[i].set_int(curr_sstable_->get_base_version());
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // multi version start
          cur_row_.cells_[i].set_int(curr_sstable_->get_multi_version_start());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          cur_row_.cells_[i].set_int(curr_sstable_->get_snapshot_version());
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // macro_idx_in_sstable
          cur_row_.cells_[i].set_int(macro_desc_.block_idx_);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // major_table_id
          cur_row_.cells_[i].set_int(curr_partition_->get_partition_key().get_table_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // macro_data_version
          cur_row_.cells_[i].set_int(macro_desc_.data_version_);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // macro_idx_in_data_file
          cur_row_.cells_[i].set_int(macro_desc_.macro_block_ctx_.get_macro_block_id().block_index());
          break;
        // TODO(): need add ofs file id to virtual table
        case OB_APP_MIN_COLUMN_ID + 13:
          // data_seq_
          cur_row_.cells_[i].set_int(macro_desc_.data_seq_);
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // row_count
          cur_row_.cells_[i].set_int(macro_desc_.row_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // occupy_size
          cur_row_.cells_[i].set_int(macro_desc_.occupy_size_);
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // micro_block_count
          cur_row_.cells_[i].set_int(macro_desc_.micro_block_count_);
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // data_checksum
          cur_row_.cells_[i].set_int(macro_desc_.data_checksum_);
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // schema_version
          cur_row_.cells_[i].set_int(macro_desc_.schema_version_);
          break;
        case OB_APP_MIN_COLUMN_ID + 19: {
          if (macro_desc_.range_.to_plain_string(range_buf_, sizeof(range_buf_)) >= 0) {
            cur_row_.cells_[i].set_varchar(range_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            // if error occur, set to null
            cur_row_.cells_[i].set_null();
          }
          break;
          case OB_APP_MIN_COLUMN_ID + 20:
            // row_count_delta
            cur_row_.cells_[i].set_int(macro_desc_.row_count_delta_);
            break;
        }
        case OB_APP_MIN_COLUMN_ID + 21: {
          blocksstable::ObMacroDataSeq macro_data_seq(macro_desc_.data_seq_);
          if (macro_data_seq.is_lob_block()) {
            cur_row_.cells_[i].set_varchar(ObString::make_string("lob_block"));
          } else {
            cur_row_.cells_[i].set_varchar(ObString::make_string("data_block"));
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 22: {
          // compressor name
          if (!macro_desc_.full_meta_.is_valid() || OB_ISNULL(macro_desc_.full_meta_.schema_->compressor_)) {
            cur_row_.cells_[i].set_varchar(ObString::make_string("unknown"));
          } else {
            cur_row_.cells_[i].set_varchar(ObString::make_string(macro_desc_.full_meta_.schema_->compressor_));
          }
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMacroInfo::get_next_sstable()
{
  int ret = OB_SUCCESS;
  bool need_ignore = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else {
    while (OB_SUCC(ret) && need_ignore) {
      while (OB_SUCC(ret) && sstable_cursor_ >= sstables_.count()) {
        sstable_cursor_ = 0;
        tables_handler_.reset();
        if (OB_FAIL(partition_iter_->get_next(curr_partition_))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "Fail to get next partition, ", K(ret));
          }
        } else if (NULL == curr_partition_) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "The curr partition is NULL, ", K(ret));
        } else {
          ObIPartitionStorage* partition_storage = curr_partition_->get_storage();
          sstables_.reuse();
          if (OB_FAIL(partition_storage->get_all_tables(tables_handler_))) {
            SERVER_LOG(WARN, "Fail to get ssstores, ", K(ret));
          } else if (OB_FAIL(tables_handler_.get_all_sstables(sstables_))) {
            SERVER_LOG(WARN, "failed to get all sstables", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        curr_sstable_ = sstables_.at(sstable_cursor_);
        if (OB_FAIL(check_need_ignore(need_ignore))) {
          SERVER_LOG(WARN, "check_need_ignore failed", K(ret));
        } else {
          ++sstable_cursor_;
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMacroInfo::check_need_ignore(bool& need_ignore)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(gen_sstable_range(range))) {
    SERVER_LOG(WARN, "gen_sstable_range failed", K(ret));
  } else {
    need_ignore = true;
    for (int64_t i = 0; i < key_ranges_.count() && need_ignore; ++i) {
      if (key_ranges_.at(i).include(range)) {
        need_ignore = false;
      }
    }
  }
  return ret;
}

int ObAllVirtualPartitionSSTableMacroInfo::gen_sstable_range(ObNewRange& range)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else {
    int index = 0;
    objs_[index].set_varchar(ip_buf_);
    objs_[index].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
    index++;
    objs_[index++].set_int(ObServerConfig::get_instance().self_addr_.get_port());
    objs_[index++].set_int(curr_partition_->get_partition_key().get_tenant_id());
    objs_[index++].set_int(curr_sstable_->get_meta().index_id_);
    objs_[index++].set_int(curr_partition_->get_partition_key().get_partition_id());
    objs_[index++].set_int(curr_sstable_->get_meta().data_version_);
    ObRowkey rowkey(objs_, index);
    if (OB_FAIL(
            range.build_range(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_TID), rowkey))) {
      SERVER_LOG(WARN, "build_range failed", K(ret), K(rowkey));
    }
  }
  return ret;
}

} /* namespace observer */
} /* namespace oceanbase */
