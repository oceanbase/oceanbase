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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_partition_scanner.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_util.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "observer/table_load/ob_table_load_schema.h"
#include <cstring>

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{
using namespace common;
using namespace share;
using namespace share::schema;

bool backup_version_cmp_func(const ObString &left, const ObString &right)
{
  return atoi(left.ptr()) > atoi(right.ptr());
}

int ObTableLoadBackupTable_V_1_4::init(
    const ObBackupStorageInfo *storage_info,
    const ObString &path,
    const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(storage_info == nullptr || path.empty() || table_schema == nullptr || !table_schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_info), K(path), KP(table_schema));
  } else if (lib::is_oracle_mode()) {   // 1.4x只支持mysql租户
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct load from backup data of mysql tenant to oracle tenant is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data of mysql tenant to oracle tenant is");
  } else if (OB_FAIL(storage_info_.assign(*storage_info))) {
    LOG_WARN("fail to assign", KR(ret));
  } else if (FALSE_IT(schema_info_.is_heap_table_ = table_schema->is_heap_table())) {
  } else if (OB_FAIL(table_schema->get_column_ids(schema_info_.column_desc_))) {
    LOG_WARN("fail to get columns ids", KR(ret));
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
                                     backup_table_id_.length(), backup_table_id_.ptr()))) {
    LOG_WARN("fail to fill data_buf", KR(ret), K(data_pos), K(part_list_[part_idx]), K(backup_table_id_));
  } else if (OB_FAIL(databuff_printf(meta_buf, OB_MAX_URI_LENGTH, meta_pos, "%.*s%.*s/%.*s/",
                                     meta_path_.length(), meta_path_.ptr(),
                                     part_list_[part_idx].length(), part_list_[part_idx].ptr(),
                                     backup_table_id_.length(), backup_table_id_.ptr()))) {
    LOG_WARN("fail to fill meta_buf", KR(ret), K(meta_pos), K(part_list_[part_idx]), K(backup_table_id_));
  } else {
    ObTableLoadBackupPartScanner *scanner = nullptr;
    if (OB_ISNULL(scanner = OB_NEWx(ObTableLoadBackupPartScanner, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(scanner->init(storage_info_, schema_info_, column_ids_,
                                     ObString(data_pos, data_buf), ObString(meta_pos, meta_buf),
                                     subpart_count, subpart_idx))) {
      LOG_WARN("fail to init iter", KR(ret), K(backup_table_id_), K(subpart_count), K(subpart_idx));
    } else {
      iter = scanner;
    }
    if (OB_FAIL(ret) && scanner != nullptr) {
      scanner->~ObTableLoadBackupPartScanner();
      allocator.free(scanner);
      scanner = nullptr;
    }
  }

  return ret;
}

int ObTableLoadBackupTable_V_1_4::parse_path(const ObString &path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(storage_info_.get_type() != OB_STORAGE_OSS && storage_info_.get_type() != OB_STORAGE_FILE)) {
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
      /*
      path路径为: xxxx/backup_version/tenant_id/backup_table_id/
      meta_path路径为: xxxx/backup_version/tenant_id/backup_table_id/
      data_path路径为: xxxx/base_data_{full_backup_version}/tenant_id/backup_table_id/
      */
      if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), meta_path_, true))) {
        LOG_WARN("fail to ob_write_string", KR(ret));
      } else {
        const int64_t backup_table_id_idx = 0;
        const int64_t backup_tenant_id_idx = 1;
        const int64_t backup_version_idx = 2;
        const int64_t need_split_count = backup_version_idx + 1;
        ObArray<ObString> split_result;
        ObString str(pos, buf);
        if (OB_FAIL(ObTableLoadBackupFileUtil::split_reverse(str, '/', split_result, need_split_count, true/*ignore_empty*/))) {
          LOG_WARN("fail to split reverse", KR(ret), K(split_result));
        } else if (OB_UNLIKELY(split_result.count() != need_split_count)) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("invalid backup destination", KR(ret), K(split_result.count()), K(need_split_count));
        } else if (OB_FAIL(ob_write_string(allocator_, split_result[backup_table_id_idx], backup_table_id_))) {
          LOG_WARN("fail to ob_write_string", KR(ret), K(split_result[backup_table_id_idx]));
        } else if (OB_FAIL(ob_write_string(allocator_, split_result[backup_tenant_id_idx], backup_tenant_id_))) {
          LOG_WARN("fail to ob_write_string", KR(ret), K(split_result[backup_tenant_id_idx]));
        } else {
          pos = str.length() + 1;
          buf[pos] = '\0';
          ObString dir_path(pos, buf);
          ObArray<ObString> dir_list;
          ObArray<ObString> backup_version_list;
          dir_list.set_tenant_id(MTL_ID());
          backup_version_list.set_tenant_id(MTL_ID());
          ObString pattern("base_data_");
          if (OB_FAIL(ObTableLoadBackupFileUtil::list_directories(dir_path, &storage_info_, dir_list, allocator_))) {
            LOG_WARN("fail to list directories", KR(ret), K(dir_path));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < dir_list.count(); i++) {
            char *match_ptr = nullptr;
            if (OB_NOT_NULL(match_ptr = strstr(dir_list[i].ptr(), pattern.ptr()))) {
              if (OB_FAIL(backup_version_list.push_back(ObString(dir_list[i].length() - pattern.length(), match_ptr + pattern.length())))) {
                LOG_WARN("fail to push back", KR(ret));
              }
            }
          }
          if (OB_UNLIKELY(backup_version_list.empty())) {
            ret = OB_INVALID_BACKUP_DEST;
            LOG_WARN("not file base data directory", KR(ret), K(dir_path), K(dir_list));
          } else {
            ob_sort(backup_version_list.begin(), backup_version_list.end(), backup_version_cmp_func);
            if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s/%.*s/%.*s/",
                                        pattern.length(), pattern.ptr(),
                                        backup_version_list[0].length(), backup_version_list[0].ptr(),
                                        backup_tenant_id_.length(), backup_tenant_id_.ptr(),
                                        backup_table_id_.length(), backup_table_id_.ptr()))) {
              LOG_WARN("fail to fill buf", KR(ret), K(pos), K(path));
            } else if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), data_path_, true))) {
              LOG_WARN("fail to ob_write_string", KR(ret));
            }
          }
        }
      }
    }
  }
  LOG_INFO("parse path", KR(ret), K(path), K(backup_table_id_), K(backup_tenant_id_), K(data_path_), K(meta_path_));
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
                              backup_table_id_.length(), backup_table_id_.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(meta_path_), K(backup_table_id_));
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
  } else if (OB_FAIL(ObTableLoadBackupUtil::get_column_ids_from_create_table_sql(ObString(read_size, file_buf), column_ids_))) {
    LOG_WARN("fail to get_column_ids_from_create_table_sql", K(ret));
  } else if (OB_UNLIKELY(schema_info_.column_desc_.count() != (column_ids_.count() + (schema_info_.is_heap_table_ ? 1 : 0)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct load from 1.4x backup data, column count not match is not supported", KR(ret), K(schema_info_.is_heap_table_), K(schema_info_.column_desc_.count()), K(column_ids_.count()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data, column count not match is");
  } else {
    LOG_INFO("get column ids", K(schema_info_), K(column_ids_));
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
  } else {
    LOG_INFO("success to get partitions", K(part_list_));
  }

  return ret;
}

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
