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
#include "observer/table_load/backup/ob_table_load_physical_backup_table.h"
#include "observer/table_load/backup/ob_table_load_physical_backup_partition_scanner_v1.h"
#include "observer/table_load/backup/ob_table_load_physical_backup_partition_scanner_v2.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include <cstring>

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;
using namespace share::schema;

ObTableLoadPhysicalBackupTable::ObTableLoadPhysicalBackupTable()
  : allocator_("TLD_PhyBT"),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  part_list_.set_tenant_id(MTL_ID());
}

int ObTableLoadPhysicalBackupTable::init(
    const ObTableLoadBackupVersion &backup_version,
    const ObBackupStorageInfo *storage_info,
    const ObString &path,
    const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_logical_backup_version(backup_version) || storage_info == nullptr || path.empty() || table_schema == nullptr || !table_schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(backup_version), KP(storage_info), K(path), KP(table_schema));
  } else {
    if (OB_FAIL(storage_info_.assign(*storage_info))) {
      LOG_WARN("fail to assign", KR(ret));
    } else if (OB_FAIL(parse_path(path))) {
      LOG_WARN("fail to parse path", KR(ret), K(path));
    } else if (OB_FAIL(check_support_for_tenant())) {
      LOG_WARN("fail to check support for tenant", KR(ret));
    } else if (OB_FAIL(get_partitions())) {
      LOG_WARN("fail to get_partitions", KR(ret));
    } else if (OB_FAIL(init_schema_info(table_schema))) {
      LOG_WARN("fail to init schema info", KR(ret));
    } else {
      backup_version_ = backup_version;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadPhysicalBackupTable::scan(
    int64_t part_idx,
    ObNewRowIterator *&iter,
    ObIAllocator &allocator,
    int64_t subpart_count,
    int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(part_idx < 0 || part_idx >= part_list_.count() || subpart_count <= 0 || subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(part_idx), K(part_list_.count()), K(subpart_count),
        K(subpart_idx));
  } else {
    if (backup_version_ == ObTableLoadBackupVersion::V_2_X_PHY) {
       ObTableLoadPhysicalBackupPartScannerV1 *scanner = nullptr;
      if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s/", data_path_.length(), data_path_.ptr(), part_list_[part_idx].length(), part_list_[part_idx].ptr()))) {
        LOG_WARN("fail to fill data_buf", KR(ret), K(pos), K(part_list_[part_idx]));
      } else if (OB_ISNULL(scanner = OB_NEWx(ObTableLoadPhysicalBackupPartScannerV1, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret));
      } else if (OB_FAIL(scanner->init(backup_version_, storage_info_, schema_info_, ObString(pos, buf), backup_set_id_,
                                      backup_table_id_, subpart_count, subpart_idx))) {
        LOG_WARN("fail to init iter", KR(ret), K(backup_version_), K(data_path_), K(backup_set_id_), K(subpart_count), K(subpart_idx));
      } else {
        iter = scanner;
      }
      if (OB_FAIL(ret) && scanner != nullptr) {
        scanner->~ObTableLoadPhysicalBackupPartScannerV1();
        allocator.free(scanner);
        scanner = nullptr;
      }
    } else {
      ObTableLoadPhysicalBackupPartScannerV2 *scanner = nullptr;
      if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s/major_data/", data_path_.length(), data_path_.ptr(), part_list_[part_idx].length(), part_list_[part_idx].ptr()))) {
        LOG_WARN("fail to fill data_buf", KR(ret), K(pos), K(part_list_[part_idx]));
      } else if (OB_ISNULL(scanner = OB_NEWx(ObTableLoadPhysicalBackupPartScannerV2, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret));
      } else if (OB_FAIL(scanner->init(backup_version_, storage_info_, schema_info_, ObString(pos, buf), backup_set_id_,
                                      backup_table_id_, subpart_count, subpart_idx))) {
        LOG_WARN("fail to init iter", KR(ret), K(backup_version_), K(data_path_), K(backup_set_id_), K(subpart_count), K(subpart_idx));
      } else {
        iter = scanner;
      }
      if (OB_FAIL(ret) && scanner != nullptr) {
        scanner->~ObTableLoadPhysicalBackupPartScannerV2();
        allocator.free(scanner);
        scanner = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadPhysicalBackupTable::check_support_for_tenant()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[OB_MAX_URI_LENGTH];
  char *file_buf = nullptr;
  int64_t read_size = 0;
  int64_t file_length = 0;
  if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*stenant_diagnose_info", meta_path_.length(), meta_path_.ptr()))) {
    LOG_WARN("fail to fill buf", KR(ret), K(pos), K(meta_path_));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::get_file_length(ObString(pos, buf), &storage_info_, file_length))) {
    LOG_WARN("fail to get file length", KR(ret), K(ObString(pos, buf)));
  } else if (OB_ISNULL(file_buf = static_cast<char*>(allocator_.alloc(file_length + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(file_length));
  } else if (OB_FAIL(ObTableLoadBackupFileUtil::read_single_file(ObString(pos, buf), &storage_info_, file_buf, file_length, read_size))) {
    LOG_WARN("fail to read single file", KR(ret), K(ObString(pos, buf)));
  } else {
    // compat_mode:紧跟着0或1，0代表mysql租户，1代表oracle租户
    ObString pattern("compat_mode:");
    bool has_match = false;
    file_buf[read_size] = '\0';
    ObString file_content(read_size, file_buf);
    while (OB_SUCC(ret) && !has_match && !file_content.empty()) {
      char *match_ptr = nullptr;
      ObString line = file_content.split_on('\n');
      if (line.empty()) {
        line = file_content;
        file_content.reset();
      } else {
        buf[file_content.ptr() - buf - 1] = '\0';
      }
      if (OB_ISNULL(match_ptr = strstr(line.ptr(), pattern.ptr()))) {
        // do nothing
      } else {
        has_match = true;
        int64_t compat_mode_idx = match_ptr - line.ptr() + pattern.length();
        if (OB_UNLIKELY(compat_mode_idx >= line.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("compat mode idx is illegal", KR(ret), K(compat_mode_idx), K(line.length()));
        } else if (OB_UNLIKELY(line[compat_mode_idx] != '0' && line[compat_mode_idx] != '1')) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("compat mode is illegal", KR(ret), K(compat_mode_idx), K(line[compat_mode_idx]));
        } else {
          if (lib::is_mysql_mode()) {
            if (line[compat_mode_idx] != '0') {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("direct load from backup data of oracle tenant to mysql tenant is not supported", KR(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data of oracle tenant to mysql tenant is");
            }
          } else {
            if (line[compat_mode_idx] != '1') {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("direct load from backup data of mysql tenant to oracle tenant is not supported", KR(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data of mysql tenant to oracle tenant is");
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!has_match) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cannot find compat mode", KR(ret), K(ObString(read_size, file_buf)), K(pattern));
      }
    }
  }
  return ret;
}

int ObTableLoadPhysicalBackupTable::init_schema_info(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_schema->get_column_ids(schema_info_.column_desc_))) {
    // 运维保证源表和目标表的column schema一致，column id不一定一致, 这一期不从备份获取源表的column schema，直接用目标表的column schema
    LOG_WARN("fail to get columns ids", KR(ret));
  } else if (table_schema->is_table_with_hidden_pk_column()) {
    schema_info_.is_heap_table_ = true;
    const ObPartitionKeyInfo &part_key_info = table_schema->get_partition_key_info();
    const ObPartitionKeyInfo &subpart_key_info = table_schema->get_subpartition_key_info();
    ObArray<uint64_t> column_ids;
    if (part_key_info.get_size() > 0) {
      if (OB_FAIL(part_key_info.get_column_ids(column_ids))) {
        LOG_WARN("fail to get partition columns ids", KR(ret));
      } else if (subpart_key_info.get_size() > 0) {
        if (OB_FAIL(subpart_key_info.get_column_ids(column_ids))) {
          LOG_WARN("fail to get subpartition columns ids", KR(ret));
        }
      }
    }
    schema_info_.partkey_count_ = column_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_info_.column_desc_.count(); i++) {
      ObSchemaColumnInfo column_info;
      if (OB_FAIL(schema_info_.column_info_.push_back(column_info))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        bool has_match = false;
        for (int64_t j = 0; !has_match && j < column_ids.count(); j++) {
          if (schema_info_.column_desc_[i].col_id_ == column_ids[j]) {
            has_match = true;
            schema_info_.column_info_[i].partkey_idx_ = j;
            schema_info_.column_info_[i].is_partkey_ = true;
          }
        }
      }
    }
  }
  LOG_INFO("init schema info", KR(ret), K(schema_info_));
  return ret;
}

int ObTableLoadPhysicalBackupTable::parse_path(const ObString &path)
{
  // xxxx/tenant_id/data/backup_set_XX_full_YY/data/backup_table_id/, XX为id, YY为日期
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
      if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), data_path_, true))) {
        LOG_WARN("fail to ob_write_string", KR(ret));
      } else {
        const int64_t backup_table_id_idx = 0;
        const int64_t backup_set_id_idx = 2;
        const int64_t need_split_count = backup_set_id_idx + 1;
        ObArray<ObString> split_result;
        ObString str(pos, buf);
        if (OB_FAIL(ObTableLoadBackupFileUtil::split_reverse(str, '/', split_result, need_split_count, true/*ignore_empty*/))) {
          LOG_WARN("fail to split reverse", KR(ret), K(split_result));
        } else if (OB_UNLIKELY(split_result.count() != need_split_count)) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("invalid backup destination", KR(ret), K(split_result.count()), K(need_split_count));
        } else if (OB_UNLIKELY(1 != sscanf(split_result[backup_table_id_idx].ptr(), "%ld", &backup_table_id_))) {
          LOG_WARN("fail to get backup table id", KR(ret), K(split_result[backup_table_id_idx]));
        } else {
          char backup_set_id_buf[32];
          int itemsParsed = sscanf(split_result[backup_set_id_idx].ptr(), "backup_set_%31[^_]", backup_set_id_buf);
          if (OB_UNLIKELY(itemsParsed != 1)) {
            ret = OB_INVALID_BACKUP_DEST;
            LOG_WARN("invalid backup destination", KR(ret), K(split_result[backup_set_id_idx]));
          } else if (OB_FAIL(ob_write_string(allocator_, ObString(backup_set_id_buf), backup_set_id_))) {
            LOG_WARN("fail to ob_write_string", KR(ret));
          } else {
            // meta_path_形式为 xxxx/tenant_id/data/backup_set_XX_full_YY/backup_{backup_set_id_}/
            int64_t meta_path_start_idx = pos;
            for (int64_t i = 0; i < backup_set_id_idx; i++) {
              meta_path_start_idx -= split_result[i].length() + 1;
            }
            if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, meta_path_start_idx, "backup_%.*s/", backup_set_id_.length(), backup_set_id_.ptr()))) {
              LOG_WARN("fail to fill buf", KR(ret), K(meta_path_start_idx), K(backup_set_id_));
            } else if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), meta_path_, true))) {
              LOG_WARN("fail to ob_write_string", KR(ret));
            }
          }
        }
      }
    }
  }
  LOG_INFO("parse path", KR(ret), K(path), K(data_path_), K(meta_path_), K(backup_table_id_), K(backup_set_id_));
  return ret;
}

int ObTableLoadPhysicalBackupTable::get_partitions()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadBackupFileUtil::list_directories(data_path_, &storage_info_, part_list_, allocator_))) {
    LOG_WARN("fail to list_directories", KR(ret), K(data_path_));
  } else {
     LOG_INFO("success to get partitions", K(part_list_));
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
