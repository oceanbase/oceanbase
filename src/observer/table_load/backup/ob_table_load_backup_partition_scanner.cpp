/**
 * Copyright (c) 2023 OceanBase
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
#include "observer/table_load/backup/ob_table_load_backup_partition_scanner.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;
using namespace blocksstable;

ObTableLoadBackupPartScanner::ObTableLoadBackupPartScanner()
  : allocator_("TLD_BPS"),
    lob_header_allocator_("TLD_BPSLob"),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    buf_(nullptr),
    lob_buf_(nullptr),
    block_idx_(-1),
    block_start_idx_(-1),
    block_end_idx_(-1),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  lob_header_allocator_.set_tenant_id(MTL_ID());
  column_ids_.set_tenant_id(MTL_ID());
  lob_col_buf_.set_tenant_id(MTL_ID());
  lob_col_buf_size_.set_tenant_id(MTL_ID());
}

ObTableLoadBackupPartScanner::~ObTableLoadBackupPartScanner()
{
  reset();
}

void ObTableLoadBackupPartScanner::reset()
{
  storage_info_.reset();
  backup_version_ = ObTableLoadBackupVersion::INVALID;
  if (buf_ != nullptr)  {
    allocator_.free(buf_);
    buf_ = nullptr;
  }
  if (lob_buf_ != nullptr)  {
    allocator_.free(lob_buf_);
    lob_buf_ = nullptr;
  }
  for (int64_t i = 0; i < lob_col_buf_.count(); i++) {
    if (lob_col_buf_[i] != nullptr) {
      allocator_.free(lob_col_buf_[i]);
      lob_col_buf_[i] = nullptr;
    }
  }
  lob_col_buf_.reset();
  lob_col_buf_size_.reset();
  lob_macro_block_idx_map_.destroy();
  block_idx_ = -1;
  block_start_idx_ = -1;
  block_end_idx_ = -1;
  macro_block_scanner_.reset();
  allocator_.reset();
  lob_header_allocator_.reset();
  is_inited_ = false;
}

int ObTableLoadBackupPartScanner::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    if (block_idx_ == -1) {
      if (OB_FAIL(switch_next_macro_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to switch next macro block", KR(ret), K(block_idx_));
        }
      } else {
        ret = macro_block_scanner_.get_next_row(row);
      }
    } else {
      if (OB_FAIL(macro_block_scanner_.get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_macro_block())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch next macro block", KR(ret));
            }
          } else {
            ret = macro_block_scanner_.get_next_row(row);
          }
        }
      }
    }
    if (OB_SUCC(ret) && lob_col_buf_.count() > 0) {
      if (OB_FAIL(read_lob_data(row))) {
        LOG_WARN("fail to get lob data", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::inner_init(
    const ObTableLoadBackupVersion &backup_version,
    const ObBackupStorageInfo &storage_info,
    const ObSchemaInfo &schema_info,
    int64_t subpart_count,
    int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  backup_version_ = backup_version;
  if (OB_FAIL(schema_info_.assign(schema_info))) {
    LOG_WARN("fail to assign schema_info_", KR(ret), K(schema_info));
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    LOG_WARN("fail to assign storage_info_", KR(ret));
  } else if (OB_FAIL(lob_macro_block_idx_map_.create(bucket_num, "TLD_MacroMap", "TLD_MacroMap", MTL_ID()))) {
    LOG_WARN("fail to create macro_block_idx_map_", KR(ret));
  } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(SSTABLE_BLOCK_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_ISNULL(lob_buf_ = static_cast<char*>(allocator_.alloc(SSTABLE_BLOCK_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_FAIL(init_macro_block_index(subpart_count, subpart_idx))) {
    LOG_WARN("fail to init macro block list", KR(ret));
  } else if (OB_FAIL(init_lob_col_buf())) {
    LOG_WARN("fail to init lob col buf", KR(ret));
  }
  return ret;
}

int ObTableLoadBackupPartScanner::locate_subpart_macro_block(
    const int64_t total_macro_block_count,
    const int64_t subpart_count,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  if (total_macro_block_count > 0) {
    int64_t valid_subpart_count = subpart_count;
    if (total_macro_block_count < subpart_count * MIN_SUBPART_MACRO_BLOCK_COUNT) {
      // 宏块数目太少, 分不出subpart_count份, 重新计算能分出几份
      valid_subpart_count =  MAX(total_macro_block_count / MIN_SUBPART_MACRO_BLOCK_COUNT, 1);
    }
    const int64_t count_per_subpart = total_macro_block_count / valid_subpart_count;
    const int64_t remain_count = total_macro_block_count - count_per_subpart * valid_subpart_count;
    // 比如16个宏块分成5份: 4 3 3 3 3
    // count_per_subpart = 3
    // remain_count = 1
    // [0, 4) [4, 7) [7, 10) [10, 13) [13, 16)
    if (subpart_idx < valid_subpart_count) {
      if (subpart_idx < remain_count) {
        block_start_idx_ = subpart_idx * (count_per_subpart + 1);
        block_end_idx_ = block_start_idx_ + (count_per_subpart + 1);
      } else {
        block_start_idx_ = subpart_idx * count_per_subpart + remain_count;
        block_end_idx_ = block_start_idx_ + count_per_subpart;
      }
    } else {
      // invalid subpart, do nothing
    }
  } else {
    // emprt partition, do nothing
  }
  return ret;
}

int ObTableLoadBackupPartScanner::init_lob_col_buf()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_info_.column_desc_.count(); i++) {
    char *empty_buf = nullptr;
    if (OB_FAIL(lob_col_buf_.push_back(empty_buf))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(lob_col_buf_size_.push_back(0))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::switch_next_macro_block()
{
  int ret = OB_SUCCESS;
  if (block_start_idx_ == block_end_idx_) {
    ret = OB_ITER_END; // empty subpart
  } else if (block_idx_ == -1) {
    block_idx_ = block_start_idx_;
  } else {
    if (block_idx_ < block_end_idx_) {
      ++block_idx_;
    }
    if (block_idx_ >= block_end_idx_) {
      ret = OB_ITER_END; // iter end
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    int64_t read_size = 0;
    if (OB_FAIL(read_macro_block_data(block_idx_, false/*is_lob_block*/, buf_, read_size))) {
      LOG_WARN("fail to read macro block data", KR(ret), K(block_idx_));
    } else {
      macro_block_scanner_.reset();
      if (OB_FAIL(macro_block_scanner_.init(buf_, read_size, backup_version_, &schema_info_, &column_ids_))) {
        LOG_WARN("fail to init macro block scanner", KR(ret), K(block_idx_));
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::read_lob_data(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  lob_header_allocator_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < row->count_; i++) {
    ObObj &col = row->get_cell(i);
    if (col.is_lob() || col.is_json() || col.is_geometry()) {
      if (col.is_outrow()) {
        const ObBackupLobData *lob_data = reinterpret_cast<const ObBackupLobData *>(col.get_string_ptr());
        const int64_t direct_block_cnt = MIN(lob_data->get_direct_cnt(), static_cast<int64_t>(lob_data->idx_cnt_));
        int64_t pos = 0;
        if (lob_col_buf_[i] == nullptr || lob_col_buf_size_[i] < lob_data->byte_size_) {
          if (lob_col_buf_[i] != nullptr) {
            allocator_.free(lob_col_buf_[i]);
            lob_col_buf_[i] = nullptr;
          }
          if (OB_ISNULL(lob_col_buf_[i] = static_cast<char*>(allocator_.alloc(lob_data->byte_size_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret), K(i), KPC(lob_data));
          } else {
            lob_col_buf_size_[i] = lob_data->byte_size_;
          }
        }
        // 读取直接索引，微块存储的是lob数据
        for (int64_t j = 0; OB_SUCC(ret) && j < direct_block_cnt; j++) {
          const ObBackupLobIndex &lob_index = lob_data->lob_idx_[j];
          if (OB_FAIL(fill_lob_buf(lob_index, lob_col_buf_[i], lob_col_buf_size_[i], pos))) {
            LOG_WARN("fail to fill lob buf", KR(ret), K(i), K(j));
          }
        }
        // 读取一层间接索引，微块存储的是lob_index
        char *indirect_index_buf = nullptr;
        for (int64_t j = direct_block_cnt; OB_SUCC(ret) && j < lob_data->idx_cnt_; j++) {
          const ObBackupLobIndex &lob_index = lob_data->lob_idx_[j];
          int64_t indirect_index_pos = 0;
          if (OB_ISNULL(indirect_index_buf) && OB_ISNULL(indirect_index_buf = static_cast<char*>(lob_header_allocator_.alloc(SSTABLE_BLOCK_BUF_SIZE)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else if (OB_FAIL(fill_lob_buf(lob_index, indirect_index_buf, SSTABLE_BLOCK_BUF_SIZE, indirect_index_pos))) {
            LOG_WARN("fail to fill lob buf", KR(ret));
          } else {
            const ObBackupLobIndex *indirect_lob_index = nullptr;
            int64_t cur_pos = 0;
            while (OB_SUCC(ret) && cur_pos < indirect_index_pos) {
              if (OB_UNLIKELY(cur_pos + sizeof(ObBackupLobIndex)) > indirect_index_pos) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected second index buf", KR(ret), K(cur_pos), K(indirect_index_pos), K(sizeof(ObBackupLobIndex)));
              } else {
                indirect_lob_index = reinterpret_cast<const ObBackupLobIndex *>(indirect_index_buf + cur_pos);
                if (OB_FAIL(fill_lob_buf(*indirect_lob_index, lob_col_buf_[i], lob_col_buf_size_[i], pos))) {
                  LOG_WARN("fail to fill lob buf", KR(ret));
                } else {
                  cur_pos += sizeof(ObBackupLobIndex);
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(pos != lob_data->byte_size_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", KR(ret), K(lob_data->byte_size_), K(pos));
          } else {
            col.v_.string_ = lob_col_buf_[i];
            col.val_len_ = pos;
          }
        }
        if (indirect_index_buf != nullptr) {
          ob_free(indirect_index_buf);
          indirect_index_buf = nullptr;
        }
      }
      if (OB_SUCC(ret)) {
        // 对于非text类型，比如json类型，补充lob_header并设置成json类型，跳过obj_cast里的json解析过程
        // 对于text类型，设置成varchar类型，如果字符集不同，obj_cast的varchar_to_text过程需要做内存拷贝，这里再补充lob_header，就变成了二次拷贝
        if (col.is_lob()) {
          col.meta_.set_varchar();
        } else {
          ObString src_obj(col.val_len_, col.v_.string_);
          ObString dest_obj;
          if (OB_FAIL(ObLobManager::fill_lob_header(lob_header_allocator_, src_obj, dest_obj))) {
            LOG_WARN("fail to fill lob header", KR(ret));
          } else {
            col.set_lob_value(col.get_type(), dest_obj.ptr(), dest_obj.length());
            col.set_has_lob_header();
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner::fill_lob_buf(
    const ObBackupLobIndex &lob_index,
    char *&buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObBackupLogicMacroBlockId &macro_block_id = lob_index.logic_macro_id_;
  int64_t block_idx;
  int64_t read_size = 0;
  ObTableLoadBackupSSTableBlockReader lob_reader;
  if (OB_FAIL(lob_macro_block_idx_map_.get_refactored(macro_block_id.data_seq_, block_idx))) {
    LOG_WARN("fail to get refactored", KR(ret), K(macro_block_id));
  } else if (OB_FAIL(read_macro_block_data(block_idx, true/*is_lob_block*/, lob_buf_, read_size))) {
    LOG_WARN("fail to read macro block data", KR(ret), K(block_idx));
  } else if (OB_FAIL(lob_reader.init(lob_buf_, read_size, backup_version_))) {
    LOG_WARN("fail to init sstable block reader", KR(ret), K(block_idx));
  } else {
    const ObMicroBlockData *micro_block_data = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(lob_reader.get_next_micro_block(micro_block_data))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("fail to get next micro block", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        int64_t data_size = micro_block_data->get_buf_size() - sizeof(ObLobMicroBlockHeader);
        if (OB_UNLIKELY(buf_size - pos < data_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected buf size", KR(ret), K(pos), K(buf_size), K(data_size));
        } else {
          std::memcpy(buf + pos, micro_block_data->get_buf() + sizeof(ObLobMicroBlockHeader), data_size);
          pos += data_size;
        }
      }
    }
  }
  return ret;
}

bool ObTableLoadBackupPartScanner::is_lob_block(const int64_t macro_block_id) const
{
  const uint64_t BIT_DATA_SEQ = 32;
  const uint64_t BIT_PARALLEL_IDX = 11;
  const uint64_t BIT_SPLIT_FLAG = 1;
  return ((macro_block_id >> (BIT_DATA_SEQ + BIT_PARALLEL_IDX + BIT_SPLIT_FLAG)) & 1) == 1;
}

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
