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
#include "observer/table_load/backup/ob_table_load_logical_backup_partition_scanner.h"
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

ObTableLoadLogicalBackupPartScanner::ObTableLoadLogicalBackupPartScanner()
{
  data_macro_block_index_.set_tenant_id(MTL_ID());
  lob_macro_block_index_.set_tenant_id(MTL_ID());
}

ObTableLoadLogicalBackupPartScanner::~ObTableLoadLogicalBackupPartScanner()
{
  reset();
}

void ObTableLoadLogicalBackupPartScanner::reset()
{
  data_path_.reset();
  backup_set_id_.reset();
  data_macro_block_index_.reset();
  lob_macro_block_index_.reset();
}

int ObTableLoadLogicalBackupPartScanner::init(
    const ObTableLoadBackupVersion &backup_version,
    const ObBackupStorageInfo &storage_info,
    const ObSchemaInfo &schema_info,
    const ObIArray<int64_t> &column_ids,
    const ObString &data_path,
    const ObString &meta_path,
    const ObString &backup_set_id,
    const int64_t subpart_count,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_UNLIKELY(!storage_info.is_valid() || column_ids.empty() || data_path.empty() || meta_path.empty() ||
                         backup_set_id.empty() || subpart_count <= 0 || subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(storage_info), K(column_ids), K(data_path), K(meta_path), K(backup_set_id),
        K(subpart_count), K(subpart_idx));
  } else {
    if (OB_FAIL(column_ids_.assign(column_ids))) {
      LOG_WARN("fail to assign column_ids_", KR(ret), K(column_ids));
    } else if (OB_FAIL(ob_write_string(allocator_, data_path, data_path_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(data_path));
    } else if (OB_FAIL(ob_write_string(allocator_, meta_path, meta_path_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(meta_path));
    } else if (OB_FAIL(ob_write_string(allocator_, backup_set_id, backup_set_id_))) {
      LOG_WARN("fail to ob_write_string", KR(ret), K(backup_set_id));
    } else if (OB_FAIL(inner_init(backup_version, storage_info, schema_info, subpart_count, subpart_idx))) {
      LOG_WARN("fail to inner init", KR(ret), K(backup_set_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadLogicalBackupPartScanner::init_macro_block_index(
  const int64_t subpart_count,
  const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  int64_t file_length = 0;
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*spart_list",
                              meta_path_.length(),
                              meta_path_.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(buf));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::get_file_length(ObString(pos, buf),
                                                                &storage_info_,
                                                                file_length))) {
    LOG_WARN("fail to get_file_length", K(ret), K(file_length), K(ObString(pos, buf)));
  } else if (file_length > 0) {
    char *file_buf = nullptr;
    int64_t read_size = 0;
    if (OB_ISNULL(file_buf = static_cast<char*>(allocator_.alloc(file_length + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(file_length));
    } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf),
                                                                   &storage_info_,
                                                                   file_buf,
                                                                   file_length,
                                                                   read_size))) {
      LOG_WARN("fail to read single file", KR(ret), K(file_length), K(ObString(pos, buf)));
    } else {
      const char *p = nullptr;
      file_buf[read_size] = '\0';
      ObString file_str(read_size, file_buf);
      ObString block_file_name;
      do {
        p = file_str.find('\n');
        if (OB_NOT_NULL(p)) {
          block_file_name = file_str.split_on(p);
        } else {
          block_file_name = file_str;
        }
        // block_file_name version_{marco_block_id}
        if (!block_file_name.empty()) {
          const char *block_id_ptr = nullptr;
          if (OB_ISNULL(block_id_ptr = block_file_name.find('_'))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected block file name", KR(ret), K(block_file_name));
          } else {
            block_id_ptr = block_id_ptr + 1;
            ObString block_id_str(block_file_name.length() - (block_id_ptr - block_file_name.ptr()), block_id_ptr);
            int64_t block_id = std::stoll(block_id_str.ptr());
            if (is_lob_block(block_id)) {
              if (OB_FAIL(lob_macro_block_idx_map_.set_refactored(block_id, lob_macro_block_index_.size(), 1))) {
                LOG_WARN("fail to set refactored", KR(ret));
              } else if (OB_FAIL(lob_macro_block_index_.push_back(block_file_name))) {
                LOG_WARN("fail to push back", KR(ret));
              }
            } else {
              if (OB_FAIL(data_macro_block_index_.push_back(block_file_name))) {
                LOG_WARN("fail to push back", KR(ret));
              }
            }
          }
        }
      } while (OB_SUCC(ret) && OB_NOT_NULL(p));
    }
    if (file_buf != nullptr) {
      allocator_.free(file_buf);
      file_buf = nullptr;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(locate_subpart_macro_block(data_macro_block_index_.count(), subpart_count, subpart_idx))) {
        LOG_WARN("fail to locate subpart macro block", KR(ret), K(data_macro_block_index_.count()), K(subpart_count), K(subpart_idx));
      } else {
        LOG_INFO("init macro block index result", KR(ret), K(block_idx_), K(block_start_idx_), K(block_end_idx_), K(data_macro_block_index_.count()), K(lob_macro_block_index_.count()));
      }
    }
  }
  return ret;
}

int ObTableLoadLogicalBackupPartScanner::read_macro_block_data(
    const int64_t block_idx,
    const bool is_lob_block,
    char *&data_buf,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s",
                              data_path_.length(),
                              data_path_.ptr(),
                              is_lob_block ? lob_macro_block_index_[block_idx].length() : data_macro_block_index_[block_idx].length(),
                              is_lob_block ? lob_macro_block_index_[block_idx].ptr() : data_macro_block_index_[block_idx].ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(data_path_), K(block_idx), K(is_lob_block));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf),
                                                                 &storage_info_,
                                                                 data_buf,
                                                                 SSTABLE_BLOCK_BUF_SIZE,
                                                                 read_size))) {
    LOG_WARN("fail to read single file", KR(ret), K(ObString(pos, buf)));
  }
  return ret;
}

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
