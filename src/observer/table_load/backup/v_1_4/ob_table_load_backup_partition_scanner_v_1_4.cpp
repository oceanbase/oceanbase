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

#include "observer/table_load/backup/v_1_4/ob_table_load_backup_partition_scanner_v_1_4.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_scanner_v_1_4.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;

int ObTableLoadBackupPartScanner_V_1_4::init(const ObBackupStorageInfo &storage_info,
                                             const ObIArray<int64_t> &column_ids,
                                             const ObString &data_path,
                                             const ObString &meta_path,
                                             int64_t subpart_count,
                                             int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_UNLIKELY(!storage_info.is_valid() || column_ids.empty() || data_path.empty() ||
                         meta_path.empty() || subpart_count <= 0 || subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(storage_info), K(column_ids), K(data_path), K(meta_path), K(subpart_count), K(subpart_idx));
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    LOG_WARN("fail to assign storage_info_", KR(ret));
  } else if (OB_FAIL(column_ids_.assign(column_ids))) {
    LOG_WARN("fail to assign column_ids_", KR(ret), K(column_ids));
  } else if (OB_FAIL(ob_write_string(allocator_, data_path, data_path_))) {
    LOG_WARN("fail to ob_write_string", KR(ret), K(data_path));
  } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(MACRO_BLOCK_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_FAIL(init_macro_block_list(meta_path))) {
    LOG_WARN("fail to init_macro_block_list", KR(ret));
  } else if (OB_FAIL(locate_subpart_macro_block(subpart_count, subpart_idx))) {
    LOG_WARN("fail to locate subpart macro block", KR(ret), K(subpart_count), K(subpart_idx));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObTableLoadBackupPartScanner_V_1_4::reset()
{
  storage_info_.reset();
  column_ids_.reset();
  data_path_.reset();
  if (buf_ != nullptr)  {
    allocator_.free(buf_);
    buf_ = nullptr;
  }
  macro_block_list_.reset();
  block_idx_ = -1;
  block_start_idx_ = -1;
  block_end_idx_ = -1;
  allocator_.reset();
  is_inited_ = false;
}

int ObTableLoadBackupPartScanner_V_1_4::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (macro_block_list_.count() == 0) {
    ret = OB_ITER_END;
  } else if (block_idx_ == -1) {
    if (OB_FAIL(switch_next_macro_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to switch_next_macro_block", KR(ret), K(block_idx_));
      }
    } else {
      ret = scanner_.get_next_row(row);
    }
  } else {
    if (OB_FAIL(scanner_.get_next_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(switch_next_macro_block())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to switch_next_macro_block", KR(ret));
          }
        } else {
          ret = scanner_.get_next_row(row);
        }
      }
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner_V_1_4::init_macro_block_list(const ObString &meta_path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  int64_t file_length = 0;
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*spart_list",
                              meta_path.length(), meta_path.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(buf));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::get_file_length(ObString(pos, buf),
                                                                &storage_info_,
                                                                file_length))) {
    LOG_WARN("fail to get_file_length", K(ret), K(file_length), K(ObString(pos, buf)));
  } else if (file_length > 0) {
    char *file_buf = nullptr;
    int64_t read_size = 0;
    if (OB_ISNULL(file_buf = static_cast<char*>(allocator_.alloc(file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(file_length));
    } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf),
                                                                   &storage_info_,
                                                                   file_buf,
                                                                   file_length,
                                                                   read_size))) {
      LOG_WARN("fail to read_single_file", KR(ret), K(file_length), K(ObString(pos, buf)));
    } else {
      const char *p = nullptr;
      ObString file_str(read_size, file_buf);
      ObString macro_block_id;
      do {
        p = file_str.find('\n');
        if (OB_NOT_NULL(p)) {
          macro_block_id = file_str.split_on(p);
        } else {
          macro_block_id = file_str;
        }
        if (!macro_block_id.empty() && OB_FAIL(macro_block_list_.push_back(macro_block_id))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      } while (OB_SUCC(ret) && OB_NOT_NULL(p));
    }
    if (file_buf != nullptr) {
      allocator_.free(file_buf);
      file_buf = nullptr;
    }
  }

  return ret;
}

int ObTableLoadBackupPartScanner_V_1_4::locate_subpart_macro_block(int64_t subpart_count, int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  const int64_t total_macro_block_count = macro_block_list_.count();
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

int ObTableLoadBackupPartScanner_V_1_4::switch_next_macro_block()
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
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_macro_block_scanner())) {
      LOG_WARN("fail to init_macro_block_scanner", KR(ret), K(block_idx_));
    }
  }
  return ret;
}

int ObTableLoadBackupPartScanner_V_1_4::init_macro_block_scanner()
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s",
                                     data_path_.length(),
                                     data_path_.ptr(),
                                     macro_block_list_[block_idx_].length(),
                                     macro_block_list_[block_idx_].ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf),
                                                         &storage_info_,
                                                         buf_,
                                                         MACRO_BLOCK_BUF_SIZE,
                                                         read_size))) {
    LOG_WARN("fail to read_single_file", KR(ret), K(ObString(pos, buf)));
  } else {
    scanner_.reset();
    if (OB_FAIL(scanner_.init(buf_, MACRO_BLOCK_BUF_SIZE, &column_ids_))) {
      LOG_WARN("fail to init scanner_", KR(ret), K(block_idx_));
    }
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
