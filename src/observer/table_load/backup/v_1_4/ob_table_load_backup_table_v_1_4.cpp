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

#include "observer/table_load/backup/v_1_4/ob_table_load_backup_table_v_1_4.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_partition_scanner_v_1_4.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_util_v_1_4.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include <cstring>

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;

int ObTableLoadBackupTable_V_1_4::init(const ObBackupStorageInfo *storage_info, const ObString &path)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(storage_info == nullptr || path.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(path));
  } else if (OB_FAIL(storage_info_.assign(*storage_info))) {
    LOG_WARN("fail to assign", KR(ret));
  } else if (OB_FAIL(parse_path(path))) {
    LOG_WARN("fail to parse path", KR(ret), K(path));
  } else if (OB_FAIL(get_column_ids())) {
    LOG_WARN("fail to get_column_ids", KR(ret));
  } else if (OB_FAIL(get_partitions())) {
    LOG_WARN("fail to get_partitions", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadBackupTable_V_1_4::scan(int64_t part_idx, ObNewRowIterator *&iter, ObIAllocator &allocator,
                                       int64_t subpart_count, int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  int64_t data_pos = 0;
  int64_t meta_pos = 0;
  char data_buf[OB_MAX_URI_LENGTH];
  char meta_buf[OB_MAX_URI_LENGTH];
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(part_idx < 0 || part_idx >= part_list_.count() || subpart_count <= 0 ||
                         subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(part_idx), K(part_list_.count()), K(subpart_count),
             K(subpart_idx));
  } else if (OB_FAIL(databuff_printf(data_buf, OB_MAX_URI_LENGTH, data_pos, "%.*s%.*s/%.*s/",
                                     data_path_.length(), data_path_.ptr(),
                                     part_list_[part_idx].length(), part_list_[part_idx].ptr(),
                                     table_id_.length(), table_id_.ptr()))) {
    LOG_WARN("fail to fill data_buf", KR(ret), K(data_pos), K(part_list_[part_idx]), K(table_id_));
  } else if (OB_FAIL(databuff_printf(meta_buf, OB_MAX_URI_LENGTH, meta_pos, "%.*s%.*s/%.*s/",
                                     meta_path_.length(), meta_path_.ptr(),
                                     part_list_[part_idx].length(), part_list_[part_idx].ptr(),
                                     table_id_.length(), table_id_.ptr()))) {
    LOG_WARN("fail to fill meta_buf", KR(ret), K(meta_pos), K(part_list_[part_idx]), K(table_id_));
  } else {
    ObTableLoadBackupPartScanner_V_1_4 *scanner = nullptr;
    if (OB_ISNULL(scanner = OB_NEWx(ObTableLoadBackupPartScanner_V_1_4, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(scanner->init(storage_info_, column_ids_,
                                     ObString(data_pos, data_buf), ObString(meta_pos, meta_buf),
                                     subpart_count, subpart_idx))) {
      LOG_WARN("fail to init iter", KR(ret), K(table_id_), K(subpart_count), K(subpart_idx));
    } else {
      iter = scanner;
    }
    if (OB_FAIL(ret) && scanner != nullptr) {
      scanner->~ObTableLoadBackupPartScanner_V_1_4();
      allocator.free(scanner);
      scanner = nullptr;
    }
  }

  return ret;
}

bool ObTableLoadBackupTable_V_1_4::is_valid() const
{
  return is_inited_;
}

int ObTableLoadBackupTable_V_1_4::parse_path(const ObString &path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(storage_info_.get_type() != OB_STORAGE_OSS)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport storage type", KR(ret), K(storage_info_));
  } else {
    int64_t pos = 0;
    char buf[OB_MAX_URI_LENGTH];
    if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s",
                                path.length(), path.ptr()))) {
      LOG_WARN("fail to fill buf", KR(ret), K(pos), K(path));
    } else {
      if (buf[pos - 1] != '/') {
        buf[pos++] = '/';
      }
      if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), data_path_))) {
        LOG_WARN("fail to ob_write_string", KR(ret));
      } else {
        ObString str(pos, buf);
        while (OB_SUCC(ret)) {
          ObString tmp_str = str.split_on(str.reverse_find('/'));
          if (!str.empty()) {
            if (OB_FAIL(ob_write_string(allocator_, str, table_id_))) {
              LOG_WARN("fail to ob_write_string", KR(ret));
            } else {
              break;
            }
          } else {
            str = tmp_str;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObString pattern("base_data_");
        char *match_ptr = nullptr;
        if (OB_ISNULL(match_ptr = strstr(buf, pattern.ptr()))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not match pattern", KR(ret), K(pattern));
        } else {
          pos -= pattern.length();
          MEMMOVE(match_ptr, match_ptr + pattern.length(), pos - (match_ptr - buf));
          if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), meta_path_, true))) {
            LOG_WARN("fail to ob_write_string", KR(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLoadBackupTable_V_1_4::get_column_ids()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  char *file_buf = nullptr;
  int64_t read_size = 0;
  int64_t file_length = 0;
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s_definition",
                              meta_path_.length(), meta_path_.ptr(),
                              table_id_.length(), table_id_.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(meta_path_), K(table_id_));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::get_file_length(ObString(pos, buf),
                                                                &storage_info_,
                                                                file_length))) {
    LOG_WARN("fail to get_file_length", KR(ret), K(ObString(pos, buf)));
  } else if (OB_ISNULL(file_buf = static_cast<char*>(allocator_.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(file_length));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf),
                                                                 &storage_info_,
                                                                 file_buf,
                                                                 file_length,
                                                                 read_size))) {
    LOG_WARN("fail to read_single_file", KR(ret), K(ObString(pos, buf)));
  } else if (OB_FAIL(ObTableLoadBackupUtil_V_1_4::get_column_ids_from_create_table_sql(ObString(read_size, file_buf), column_ids_))) {
    LOG_WARN("fail to get_column_ids_from_create_table_sql", K(ret));
  }
  if (file_buf != nullptr) {
    allocator_.free(file_buf);
    file_buf = nullptr;
  }

  return ret;
}

int ObTableLoadBackupTable_V_1_4::get_partitions()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadBackupFileUtil::list_directories(meta_path_, &storage_info_, part_list_, allocator_))) {
    LOG_WARN("fail to list_directories", KR(ret), K(meta_path_));
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
