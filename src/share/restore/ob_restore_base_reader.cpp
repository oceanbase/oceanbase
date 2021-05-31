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

#define USING_LOG_PREFIX STORAGE

#include "share/restore/ob_restore_uri_parser.h"
#include "ob_restore_base_reader.h"
#include "lib/restore/ob_storage.h"

namespace oceanbase {
using namespace common;

namespace share {
namespace restore {

ObRestoreBaseReader::ObRestoreBaseReader(ObRestoreArgs& args)
    : args_(args), allocator_(ObModIds::RESTORE), is_inited_(false)
{
  common_path_.reset();
}

int ObRestoreBaseReader::init(const ObString& oss_uri)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already inited", K(ret));
  } else if (OB_FAIL(ObRestoreURIParser::parse(oss_uri, args_))) {
    OB_LOG(WARN, "fail parse oss_uri", K(oss_uri), K(ret));
  } else if (OB_FAIL(ObRestoreURIParserHelper::set_data_version(args_))) {
    OB_LOG(WARN, "fail set data version", K(args_), K(ret));
  } else if (!args_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(args_), K(ret));
  } else if (OB_FAIL(
                 ObStoragePathUtil::generate_logic_backup_uri_dir_path(ObString::make_string(args_.get_uri_header()),
                     ObString::make_string(args_.get_cluster_name()),
                     args_.curr_data_version_,
                     args_.tenant_id_,
                     common_path_))) {
    OB_LOG(WARN, "fail to generate logic backup uri path", K(ret), K(args_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObRestoreBaseReader::get_create_unit_stmts(ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_RESOURCE_UNIT_DEFINITION, false /* allow_not_exist*/, stmts))) {
    OB_LOG(WARN, "fail to get resource unit definition", K(ret));
  }
  return ret;
}

int ObRestoreBaseReader::get_create_pool_stmts(ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_RESOURCE_POOL_DEFINITION, false /* allow_not_exist*/, stmts))) {
    OB_LOG(WARN, "fail to get resource pool definition", K(ret));
  }
  return ret;
}

int ObRestoreBaseReader::get_create_tenant_stmt(ObString& stmt)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> stmts;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_CREATE_TENANT_DEFINITION, false /* allow_not_exist*/, stmts))) {
    OB_LOG(WARN, "fail to get tenant definition", K(ret));
  } else if (stmts.count() <= 0 || stmts.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "the count is invalid", K(stmts.count()), K(ret));
  } else {
    stmt = stmts.at(0);
  }
  return ret;
}

int ObRestoreBaseReader::get_create_tablegroup_stmts(ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();
  ObArray<ObString> tablegroup_ids;
  ObString tablegroup_id_name;
  char tablegroup_name[OB_MAX_TABLEGROUP_NAME_LENGTH];
  char* create_tablegroup_buf = NULL;
  uint64_t tablegroup_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_TABLEGROUP_IDS_LIST, false /* allow_not_exist*/, tablegroup_ids))) {
    OB_LOG(WARN, "fail to get tablegroup_ids_list", K(ret));
  }

  for (int64_t idx = 0; idx < tablegroup_ids.count() && OB_SUCC(ret); ++idx) {
    tablegroup_id_name = tablegroup_ids.at(idx);
    tablegroup_id = 0;
    if (2 != sscanf(tablegroup_id_name.ptr(), "%lu:%s", &tablegroup_id, tablegroup_name)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to get tablegroup_id and tablegroup_name", K(tablegroup_id_name));
    } else {
      ObStorageUtil util(true /*need retry*/);
      int64_t file_length = 0;
      ObStoragePath path;
      if (OB_FAIL(ObStoragePathUtil::generate_tablegroup_definition_file_path(
              common_path_.get_obstring(), tablegroup_id, path))) {
        OB_LOG(WARN, "fail to generate tablegroup definition file path", K(ret), K(common_path_), K(tablegroup_name));
      } else if (OB_FAIL(util.get_file_length(path.get_obstring(), args_.get_storage_info(), file_length))) {
        OB_LOG(WARN, "fail to get file length", K(ret), K(args_), K(path), K(args_));
      } else if (OB_UNLIKELY(0 >= file_length)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "tablegroup definition file is empty", K(ret), K(path), K(args_));
      } else {
        const int64_t text_file_length = file_length + 1;
        if (OB_ISNULL(create_tablegroup_buf = static_cast<char*>(allocator_.alloc(text_file_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(WARN, "fail to allocate memory", K(ret), K(text_file_length), K(path), K(args_));
        } else if (OB_FAIL(util.read_single_text_file(
                       path.get_obstring(), args_.get_storage_info(), create_tablegroup_buf, text_file_length))) {
          OB_LOG(WARN, "fail to read object from oss", K(ret), K(path), K(args_), K(text_file_length));
        } else if (strlen(create_tablegroup_buf) <= 0) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "read_size is invalid", K(ret), K(strlen(create_tablegroup_buf)));
        } else if (OB_FAIL(stmts.push_back(ObString::make_string(create_tablegroup_buf)))) {
          OB_LOG(WARN, "fail to push backup create tablegroup definition", K(ret), K(path), K(create_tablegroup_buf));
        } else {
          OB_LOG(INFO, "get restore create tablegroup stmt", K(create_tablegroup_buf));
        }
      }  // else
    }
  }  // for
  return ret;
}

int ObRestoreBaseReader::get_create_foreign_key_stmts(common::ObIArray<common::ObString>& stmts)
{
  return get_create_stmts_for_solo_backup_object(OB_FOREIGN_KEY_IDS_LIST, OB_FOREIGN_KEY_DEFINITION, stmts);
}

int ObRestoreBaseReader::get_create_stmts_for_solo_backup_object(
    const char* object_ids_list_str, const char* object_definition_str, ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();
  ObArray<ObString> object_ids;
  uint64_t object_id = 0;
  char* create_object_buf = NULL;
  ObStorageUtil util(true /*need retry*/);
  ObStoragePath path;
  int64_t file_length = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_ISNULL(object_ids_list_str) || OB_ISNULL(object_definition_str)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object ids list is NULL");
  } else if (OB_FAIL(get_one_object_from_oss(object_ids_list_str, false /* allow_not_exist*/, object_ids))) {
    OB_LOG(WARN, "fail to get object_ids_list", K(ret));
  }

  for (int64_t idx = 0; idx < object_ids.count() && OB_SUCC(ret); ++idx) {
    object_id = 0;
    path.reset();
    if (OB_ISNULL(object_ids.at(idx).ptr())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object_id string is null", K(ret), K(idx));
    } else if (1 != sscanf(object_ids.at(idx).ptr(), "%lu", &object_id)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to get object_id", K(ret), K(object_ids.at(idx)));
    } else if (OB_FAIL(ObStoragePathUtil::generate_simple_object_file_path(
                   common_path_.get_obstring(), object_definition_str, object_id, path))) {
      OB_LOG(WARN, "fail to generate object definition file path", K(ret), K(common_path_), K(object_id));
    } else if (OB_FAIL(util.get_file_length(path.get_obstring(), args_.get_storage_info(), file_length))) {
      OB_LOG(WARN, "fail to get file length", K(ret), K(args_), K(path), K(args_));
    } else if (OB_FAIL(file_length <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object definition file is empty", K(ret), K(path), K(args_));
    } else if (FALSE_IT(file_length++)) {
    } else if (OB_ISNULL(create_object_buf = static_cast<char*>(allocator_.alloc(file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate memory", K(ret), K(file_length), K(path), K(args_));
    } else if (OB_FAIL(util.read_single_text_file(
                   path.get_obstring(), args_.get_storage_info(), create_object_buf, file_length))) {
      OB_LOG(WARN, "fail to read object from oss", K(ret), K(path), K(args_), K(file_length));
    } else if (strlen(create_object_buf) <= 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "read_size is invalid", K(ret), K(strlen(create_object_buf)));
    } else if (OB_FAIL(stmts.push_back(ObString::make_string(create_object_buf)))) {
      OB_LOG(WARN, "fail to push backup create object definition", K(ret), K(path), K(create_object_buf));
    } else {
      OB_LOG(INFO, "get restore create object stmt", K(create_object_buf));
    }
  }
  return ret;
}

int ObRestoreBaseReader::get_create_database_stmts(ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_CREATE_DATABASE_DEFINITION, false /* allow_not_exist*/, stmts))) {
    OB_LOG(WARN, "fail to get database definition", K(ret));
  }
  return ret;
}

int ObRestoreBaseReader::get_create_user_stmts(ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_CREATE_USER_DEINITION, false /* allow_not_exist*/, stmts))) {
    OB_LOG(WARN, "fail to get user definition", K(ret));
  }
  return ret;
}

int ObRestoreBaseReader::get_direct_executable_stmts(
    const char* direct_executable_definition, ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_ISNULL(direct_executable_definition)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "direct_executable_definition is NULL", K(ret));
  } else {
    stmts.reset();
    if (OB_FAIL(get_one_object_from_oss(direct_executable_definition, false /* allow_not_exist*/, stmts))) {
      OB_LOG(WARN, "fail to get definition", "definition", *direct_executable_definition, K(ret));
    }
  }
  return ret;
}

int ObRestoreBaseReader::get_recycle_objects(common::ObIArray<schema::ObRecycleObject>& objects)
{
  int ret = OB_SUCCESS;
  char* read_buf = NULL;
  int64_t read_size = 0;
  int64_t pos = 0;
  ObStorageUtil util(true /*need retry*/);
  ObStoragePath path;
  bool is_exist = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss reader is not inited", K(ret));
  } else if (OB_FAIL(path.init(common_path_.get_obstring()))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(common_path_));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_RECYCLE_OBJECT_LIST)))) {
    OB_LOG(WARN, "fail to join last name", K(ret), K(common_path_));
  } else if (OB_FAIL(util.is_exist(path.get_obstring(), args_.get_storage_info(), is_exist))) {
    OB_LOG(WARN, "fail to check file exist", K(ret), K(path), K(args_), K(common_path_));
  } else if (!is_exist) {
    OB_LOG(INFO, "recycle object list do not exist");
  } else if (OB_FAIL(read_one_file(path, allocator_, read_buf, read_size))) {
    OB_LOG(WARN, "fail to read partition meta", K(ret), K(args_));
  } else if (0 == read_size) {
    OB_LOG(INFO, "no recyclebin objects, skip it", K(ret), K(args_));
  } else {
    while (OB_SUCC(ret) && pos < read_size) {
      schema::ObRecycleObject recycle_object;
      if (OB_FAIL(recycle_object.deserialize(read_buf, read_size, pos))) {
        OB_LOG(WARN, "failed to deserialize recycle object", K(ret), K(read_buf), K(pos), K(read_size));
      } else if (OB_FAIL(objects.push_back(recycle_object))) {
        OB_LOG(WARN, "failed to push recycle object into array", K(ret), K(recycle_object));
      }
    }
  }
  return ret;
}
int ObRestoreBaseReader::get_create_data_table_stmts(ObIArray<ObString>& stmts)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> table_id_strs;
  const bool allow_not_exist = false;
  const bool is_index = false;
  uint64_t table_id = 0;
  ObString stmt;
  stmts.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_DATA_TABLE_IDS_LIST, allow_not_exist, table_id_strs))) {
    OB_LOG(WARN, "fail to get table ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_id_strs.count(); ++i) {
      const ObString& table_id_str = table_id_strs.at(i);
      if (OB_FAIL(get_create_table_stmt(table_id_str, is_index, stmt, table_id))) {
        LOG_WARN("failed to get create table stmt", K(ret), K(i), K(table_id_str));
      } else if (OB_FAIL(stmts.push_back(stmt))) {
        LOG_WARN("failed to push back stmts", K(ret));
      } else {
        LOG_INFO("get create table stmt", K(stmt));
      }
    }
  }
  return ret;
}

int ObRestoreBaseReader::get_create_synonym_stmts(common::ObIArray<common::ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();
  ObArray<ObString> synonym_ids;
  uint64_t synonym_id = 0;
  char* create_synonym_buf = NULL;
  ObStorageUtil util(true /*need retry*/);
  ObStoragePath path;
  int64_t file_length = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_SYNONYM_IDS_LIST, false /* allow_not_exists */, synonym_ids))) {
    OB_LOG(WARN, "fail to get synonym_ids_list", K(ret));
  }

  for (int64_t idx = 0; idx < synonym_ids.count() && OB_SUCC(ret); ++idx) {
    synonym_id = 0;
    path.reset();
    if (OB_ISNULL(synonym_ids.at(idx).ptr())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "synonym_id string is null", K(ret), K(idx));
    } else if (1 != sscanf(synonym_ids.at(idx).ptr(), "%lu", &synonym_id)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to get synonym_id", K(ret), K(synonym_ids.at(idx)));
    } else if (OB_FAIL(ObStoragePathUtil::generate_simple_object_file_path(common_path_.get_obstring(),
                   ObString::make_string(OB_CREATE_SYNONYM_DEFINITION),
                   synonym_id,
                   path))) {
      OB_LOG(WARN, "fail to generate synonym definition file path", K(ret), K(common_path_), K(synonym_id));
    } else if (OB_FAIL(util.get_file_length(path.get_obstring(), args_.get_storage_info(), file_length))) {
      OB_LOG(WARN, "fail to get file length", K(ret), K(args_), K(path), K(args_));
    } else if (OB_FAIL(file_length <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "synonym definition file is empty", K(ret), K(path), K(args_));
    } else if (FALSE_IT(file_length++)) {
    } else if (OB_ISNULL(create_synonym_buf = static_cast<char*>(allocator_.alloc(file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate memory", K(ret), K(file_length), K(path), K(args_));
    } else if (OB_FAIL(util.read_single_text_file(
                   path.get_obstring(), args_.get_storage_info(), create_synonym_buf, file_length))) {
      OB_LOG(WARN, "fail to read object from oss", K(ret), K(path), K(args_), K(file_length));
    } else if (strlen(create_synonym_buf) <= 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "read_size is invalid", K(ret), K(strlen(create_synonym_buf)));
    } else if (OB_FAIL(stmts.push_back(ObString::make_string(create_synonym_buf)))) {
      OB_LOG(WARN, "fail to push backup create synonym definition", K(ret), K(path), K(create_synonym_buf));
    } else {
      OB_LOG(INFO, "get restore create synonym stmt", K(create_synonym_buf));
    }
  }
  return ret;
}

int ObRestoreBaseReader::get_create_index_table_stmts(
    ObIArray<ObString>& stmts, common::hash::ObHashSet<uint64_t>& dropped_index_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> index_id_strs;
  ObArray<ObString> dropped_index_id_strs;
  const bool is_index = true;
  uint64_t table_id = 0;
  ObString stmt;

  stmts.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(
                 get_one_object_from_oss(OB_DROPPED_TABLE_ID_LIST, true /* allow_not_exist*/, dropped_index_id_strs))) {
    OB_LOG(WARN, "failed to get dropped index object from oss", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_INDEX_TABLE_IDS_LIST, false /* allow_not_exist*/, index_id_strs))) {
    LOG_WARN("failed to get index  one object from oss", K(ret));
  } else if (OB_FAIL(dropped_index_ids.create(dropped_index_id_strs.count() + 1))) {
    OB_LOG(WARN, "failed to create dropped index id set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_id_strs.count(); ++i) {
      const ObString& index_id_str = index_id_strs.at(i);
      if (OB_FAIL(get_create_table_stmt(index_id_str, is_index, stmt, table_id))) {
        LOG_WARN("failed to get create table stmt", K(ret), K(i), K(index_id_str));
      } else if (OB_FAIL(stmts.push_back(stmt))) {
        LOG_WARN("failed to push back stmts", K(ret));
      } else {
        LOG_INFO("get create index stmt", K(stmt));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_index_id_strs.count(); ++i) {
      const ObString& index_id_str = dropped_index_id_strs.at(i);
      if (OB_FAIL(get_create_table_stmt(index_id_str, is_index, stmt, table_id))) {
        LOG_WARN("failed to get create table stmt", K(ret), K(i), K(index_id_str));
      } else if (OB_FAIL(dropped_index_ids.set_refactored(table_id))) {
        LOG_WARN("failed to set dropped index ids", K(ret), K(table_id), K(stmt));
      } else {
        LOG_INFO("get create dropped index id", K(table_id), K(index_id_str), K(stmt));
      }
    }
  }
  return ret;
}

int ObRestoreBaseReader::get_create_all_timezone_stmts(common::ObIArray<common::ObString>& stmts)
{
  int ret = OB_SUCCESS;
  stmts.reset();
  bool allow_not_exist = true;
  ObStorageUtil util(true /*need retry*/);
  ObStoragePath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(get_one_object_from_oss(OB_TIMEZONE_INFO_DEFINITION, allow_not_exist, stmts))) {
    OB_LOG(WARN, "fail to get all timezone info", K(ret));
  } else {
    OB_LOG(DEBUG, "get create all timezone info stmts succeed", K(stmts.count()));
  }
  return ret;
}

int ObRestoreBaseReader::get_create_table_stmt(
    const common::ObString& table_id_name, const bool is_index, common::ObString& stmt, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_table_id = 0;
  table_id = 0;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  char* create_table_buf = NULL;
  stmt.reset();

  if (is_index) {
    if (3 != sscanf(table_id_name.ptr(), "%lu:%lu:%s", &data_table_id, &table_id, table_name)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to get table_id and table_name", K(table_id_name), K(ret));
    }
  } else {
    if (2 != sscanf(table_id_name.ptr(), "%lu:%s", &data_table_id, table_name)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to get table_id and table_name", K(table_id_name));
    } else {
      table_id = data_table_id;
    }
  }

  if (OB_SUCC(ret)) {
    ObStorageUtil util(true /*need retry*/);
    int64_t file_length = 0;
    ObStoragePath path;
    if (OB_FAIL(ObStoragePathUtil::generate_table_definition_file_path(
            common_path_.get_obstring(), data_table_id, table_id, path))) {
      OB_LOG(WARN, "fail to generate table definition file path", K(ret), K(common_path_), K(table_name));
    } else if (OB_FAIL(util.get_file_length(path.get_obstring(), args_.get_storage_info(), file_length))) {
      OB_LOG(WARN, "fail to get file length", K(ret), K(args_), K(path), K(args_));
    } else if (OB_FAIL(file_length <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "table definition file is empty", K(ret), K(path), K(args_));
    } else {
      const int64_t text_file_length = file_length + 1;
      if (OB_ISNULL(create_table_buf = static_cast<char*>(allocator_.alloc(text_file_length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to allocate memory", K(ret), K(text_file_length), K(path), K(args_));
      } else if (OB_FAIL(util.read_single_text_file(
                     path.get_obstring(), args_.get_storage_info(), create_table_buf, text_file_length))) {
        OB_LOG(WARN, "fail to read object from oss", K(ret), K(path), K(args_), K(text_file_length));
      } else if (strlen(create_table_buf) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "read_size is invalid", K(ret), K(strlen(create_table_buf)));
      } else {
        stmt = ObString::make_string(create_table_buf);
        OB_LOG(INFO, "get restore create table stmt", K(create_table_buf));
      }
    }  // else
  }
  return ret;
}

int ObRestoreBaseReader::get_one_object_from_oss(
    const char* last_name, const bool allow_not_exist, common::ObIArray<common::ObString>& stmts)
{
  int ret = OB_SUCCESS;
  ObStoragePath path;
  char* read_buf = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_ISNULL(last_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(last_name), K(ret));
  } else if (OB_FAIL(path.init(common_path_.get_obstring()))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(common_path_));
  } else if (OB_FAIL(path.join(ObString::make_string(last_name)))) {
    OB_LOG(WARN, "fail to join last name", K(ret), K(common_path_), K(last_name));
  }

  if (OB_SUCC(ret)) {
    ObStorageUtil util(true /*need retry*/);
    int64_t file_length = 0;
    bool is_exist = false;
    if (OB_FAIL(util.is_exist(path.get_obstring(), args_.get_storage_info(), is_exist))) {
      OB_LOG(WARN, "fail to check file exist", K(ret), K(path), K(args_), K(common_path_));
    } else if (!is_exist) {
      if (!allow_not_exist) {
        ret = OB_FILE_NOT_EXIST;
        OB_LOG(WARN, "file not exist", K(ret), K(path));
      } else {
        OB_LOG(INFO, "file not exist", K(path));
      }
    } else {
      if (OB_FAIL(util.get_file_length(path.get_obstring(), args_.get_storage_info(), file_length))) {
        OB_LOG(WARN, "fail to get file length", K(ret), K(path), K(args_), K(common_path_));
      } else if (file_length <= 0) {  // data_table_ids_list and index_table_ids_list is possible empty
        OB_LOG(INFO, "this file is empty", K(ret), K(path), K(args_));
      } else {
        const int64_t text_file_length = file_length + 1;
        if (OB_ISNULL(read_buf = static_cast<char*>(allocator_.alloc(text_file_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(WARN, "fail to allocate memory", K(ret), K(text_file_length), K(path), K(args_));
        } else if (OB_FAIL(util.read_single_text_file(
                       path.get_obstring(), args_.get_storage_info(), read_buf, text_file_length))) {
          OB_LOG(WARN, "fail to read single text file", K(ret), K(path), K(args_), K(text_file_length));
        }
      }
    }
    OB_LOG(DEBUG, "get buf from oss", K(is_exist), K(read_buf));
  }
  // get stmt from buf
  if (OB_SUCC(ret) && NULL != read_buf) {
    char* saveptr = NULL;
    char* line = strtok_r(read_buf, "\n", &saveptr);
    ObString tmp_str;
    while (line != NULL && OB_SUCC(ret)) {
      tmp_str.assign_ptr(line, static_cast<int32_t>(strlen(line)));
      if (OB_FAIL(stmts.push_back(tmp_str))) {
        OB_LOG(WARN, "fail to push back string", K(tmp_str), K(ret));
      } else {
        OB_LOG(INFO, "get restroe line", K(line), K(strlen(line)), K(tmp_str));
        line = strtok_r(NULL, "\n", &saveptr);
      }
    }
  }
  return ret;
}

int ObRestoreBaseReader::read_one_file(
    const ObStoragePath& path, ObIAllocator& allocator, char*& buf, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need retry*/);
  int64_t file_len = 0;
  read_size = 0;
  int64_t buffer_len = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (!path.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(path), K(ret));
  } else if (OB_FAIL(util.get_file_length(path.get_obstring(), args_.get_storage_info(), file_len))) {
    OB_LOG(WARN, "fail to get file len", K(ret), K(path), K(args_));
  } else if (file_len <= 0) {  // part_list is possible empty, just return success
    OB_LOG(INFO, "this file is empty", K(path), K(args_), K(file_len));
  } else {
    buffer_len = file_len + 1;  // last for '\0'
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "fail to alloc memory", K(ret), K(file_len), K(buffer_len));
    } else if (OB_FAIL(
                   util.read_single_file(path.get_obstring(), args_.get_storage_info(), buf, file_len, read_size))) {
      OB_LOG(WARN, "fail to read all data", K(ret), K(path));
    } else if (OB_UNLIKELY(read_size != file_len)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "read size is invalid", K(ret), K(read_size), K(file_len), K(buffer_len));
    } else {
      buf[file_len] = '\0';
    }
  }
  return ret;
}

}  // namespace restore
}  // namespace share
}  // namespace oceanbase
