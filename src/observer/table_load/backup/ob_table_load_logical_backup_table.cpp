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

#include "observer/table_load/backup/ob_table_load_logical_backup_table.h"
#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "observer/table_load/ob_table_load_schema.h"
#include <cstring>
#include <vector>
#include <regex>

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;
using namespace share::schema;

bool backup_version_cmp_func(const ObString &left, const ObString &right)
{
  return std::atoi(left.ptr()) > std::atoi(right.ptr());
}

ObTableLoadLogicalBackupTable::ObTableLoadLogicalBackupTable()
  : allocator_("TLD_LogBT"),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  column_ids_.set_tenant_id(MTL_ID());
  part_list_.set_tenant_id(MTL_ID());
}

int ObTableLoadLogicalBackupTable::init(
    const ObTableLoadBackupVersion &backup_version,
    const ObBackupStorageInfo *storage_info,
    const ObString &path,
    const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_logical_backup_version(backup_version) || storage_info == nullptr || path.empty() || table_schema == nullptr || !table_schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(backup_version), KP(storage_info), K(path), KP(table_schema));
  } else {
    backup_version_ = backup_version;
    if (OB_FAIL(storage_info_.assign(*storage_info))) {
      LOG_WARN("fail to assign", KR(ret));
    } else if (OB_FAIL(check_support_for_tenant())) {
      LOG_WARN("fail to check support for tenant", KR(ret));
    } else if (FALSE_IT(schema_info_.is_heap_table_ = table_schema->is_table_with_hidden_pk_column())) {
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
  }
  return ret;
}

int ObTableLoadLogicalBackupTable::scan(
    int64_t part_idx,
    ObNewRowIterator *&iter,
    ObIAllocator &allocator,
    int64_t subpart_count,
    int64_t subpart_idx)
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
    ObTableLoadLogicalBackupPartScanner *scanner = nullptr;
    if (OB_ISNULL(scanner = OB_NEWx(ObTableLoadLogicalBackupPartScanner, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(scanner->init(backup_version_, storage_info_, schema_info_, column_ids_,
                                     ObString(data_pos, data_buf), ObString(meta_pos, meta_buf), backup_set_id_,
                                     subpart_count, subpart_idx))) {
      LOG_WARN("fail to init iter", KR(ret), K(backup_version_), K(subpart_count), K(subpart_idx));
    } else {
      iter = scanner;
    }
    if (OB_FAIL(ret) && scanner != nullptr) {
      scanner->~ObTableLoadLogicalBackupPartScanner();
      allocator.free(scanner);
      scanner = nullptr;
    }
  }
  return ret;
}

int ObTableLoadLogicalBackupTable::check_support_for_tenant()
{
  int ret = OB_SUCCESS;
  if (backup_version_ == ObTableLoadBackupVersion::V_1_4) {
    if (lib::is_oracle_mode()) {   // 1.4x只支持mysql租户
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct load from backup data of mysql tenant to oracle tenant is not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load from backup data of mysql tenant to oracle tenant is");
    }
  }
  return ret;
}

int ObTableLoadLogicalBackupTable::init_schema_info(const ObTableSchema *table_schema)
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

int ObTableLoadLogicalBackupTable::parse_path(const ObString &path)
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
        } else if (OB_FAIL(ob_write_string(allocator_, split_result[backup_version_idx], backup_set_id_))) {
          LOG_WARN("fail to ob_write_string", KR(ret), K(split_result[backup_version_idx]));
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
            for (int64_t i = 0; i < backup_version_list.size(); i++) {
              if (atoi(backup_version_list[i].ptr()) > atoi(backup_set_id_.ptr())) {
                // do nothing
              } else if (OB_FAIL(databuff_printf(buf, OB_MAX_URI_LENGTH, pos, "%.*s%.*s/%.*s/%.*s/",
                                                 pattern.length(), pattern.ptr(),
                                                 backup_version_list[i].length(), backup_version_list[i].ptr(),
                                                 backup_tenant_id_.length(), backup_tenant_id_.ptr(),
                                                 backup_table_id_.length(), backup_table_id_.ptr()))) {
                LOG_WARN("fail to fill buf", KR(ret), K(pos), K(path));
              } else if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), data_path_, true))) {
                LOG_WARN("fail to ob_write_string", KR(ret));
              } else {
                break;
              }
            }
          }
        }
      }
    }
  }
  LOG_INFO("parse path", KR(ret), K(path), K(backup_version_), K(backup_table_id_), K(backup_tenant_id_), K(backup_set_id_), K(data_path_), K(meta_path_));
  return ret;
}

int ObTableLoadLogicalBackupTable::get_column_ids()
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
  } else if (OB_FAIL(get_column_ids_from_create_table_sql(ObString(read_size, file_buf), column_ids_))) {
    LOG_WARN("fail to get column ids from create table sql", K(ret));
  } else if (OB_UNLIKELY(schema_info_.column_desc_.count() != (column_ids_.count() + (schema_info_.is_heap_table_ ? 1 : 0)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct load from logical backup data, column count not match is not supported", KR(ret), K(schema_info_.is_heap_table_), K(schema_info_.column_desc_.count()), K(column_ids_.count()));
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

int ObTableLoadLogicalBackupTable::get_partitions()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadBackupFileUtil::list_directories(meta_path_, &storage_info_, part_list_, allocator_))) {
    LOG_WARN("fail to list_directories", KR(ret), K(meta_path_));
  } else {
    LOG_INFO("success to get partitions", K(part_list_));
  }
  return ret;
}

int ObTableLoadLogicalBackupTable::get_column_ids_from_create_table_sql(
    const ObString &sql,
    ObIArray<int64_t> &column_ids)
{
  int ret = OB_SUCCESS;

  //split to lines
  ObArray<char *> lines;
  ObArenaAllocator allocator;
  lines.set_tenant_id(MTL_ID());
  allocator.set_tenant_id(MTL_ID());
  char *sql_str = nullptr;
  if (OB_ISNULL(sql_str = static_cast<char *>(allocator.alloc(sql.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    memcpy(sql_str, sql.ptr(), sql.length());
    sql_str[sql.length()] = '\0';
    char *save_ptr = nullptr;
    char *token = strtok_r(sql_str, "\n", &save_ptr);
    while (OB_SUCC(ret) && token != NULL) {
      if (OB_FAIL(lines.push_back(token))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        token = strtok_r(NULL, "\n", &save_ptr);
      }
    }
  }

  // get column lines and pk
  std::vector<std::string> column_lines;
  char *pk = nullptr;
  if (OB_SUCC(ret)) {
    for (int64_t i = 1; i < lines.count(); i ++) {
      char *pos = strcasestr(lines[i], "primary key");
      char *comment = strcasestr(lines[i], "comment ");
      if (pos != nullptr && comment == nullptr) {
        pk = pos;
        break;
      } else {
        column_lines.push_back(lines[i]);
      }
    }
  }

  // regex search column_ids and pk
  if (OB_SUCC(ret)) {
    std::vector<std::string> pks;
    if (pk != nullptr) {
      std::smatch m;
      std::string cur = pk;
      while (OB_SUCC(ret) && std::regex_search(cur, m, std::regex("([\"`])([^\"`]+)([\"`])"))) {
        pks.push_back(m[2].str());
        cur = m.suffix();
      }
    }

    std::vector<std::pair<std::string, int64_t>> column_defs;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_lines.size(); i ++) {
      std::smatch m;
      if (std::regex_search(column_lines[i], m, std::regex("([\"`])([^\"`]+)([\"`])"))) {
        std::string column_name = m[2].str();
        if (std::regex_search(column_lines[i], m, std::regex("id ([0-9]+)", std::regex_constants::icase))) {
          std::string id_str = m[1].str();
          char *endstr = nullptr;
          int64_t id = strtoll(id_str.c_str(), &endstr, 10);
          column_defs.push_back(std::make_pair(column_name, id));
        }
      }
    }

    // put pk first
    for (int64_t i = 0; OB_SUCC(ret) && i < pks.size(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < column_defs.size(); j++) {
        if (pks[i] == column_defs[j].first) {
          if (OB_FAIL(column_ids.push_back(column_defs[j].second))) {
            LOG_WARN("fail to push back", KR(ret));
          }
          break;
        }
      }
    }

    // add remain columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_defs.size(); i++) {
      bool flag = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); j++) {
        if (column_ids.at(j) == column_defs[i].second) {
          flag = true;
          break;
        }
      }
      if (!flag) {
        if (OB_FAIL(column_ids.push_back(column_defs[i].second))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
