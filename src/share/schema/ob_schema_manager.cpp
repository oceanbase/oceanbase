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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_schema_manager.h"
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/file/file_directory_utils.h"
#include "lib/string/ob_sql_string.h"
#include "common/object/ob_obj_type.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_unit_getter.h"

namespace oceanbase {
namespace share {
namespace schema {
using namespace std;
using namespace common;
using namespace common::hash;
/*-----------------------------------------------------------------------------
 *  ObSchemaManager
 *-----------------------------------------------------------------------------*/
ObSchemaManager::ObSchemaManager()
    : schema_magic_(OB_SCHEMA_MAGIC_NUMBER),
      code_version_(OB_SCHEMA_CODE_VERSION),
      schema_version_(0),
      n_alloc_tables_(0),
      global_alloc_size_(0),
      table_infos_(0, NULL, ObModIds::OB_SCHEMA_TABLE_INFOS),
      index_table_infos_(0, NULL, ObModIds::OB_SCHEMA_INDEX_TABLE_INFOS),
      database_infos_(0, NULL, ObModIds::OB_SCHEMA_DATABASE_INFOS),
      tablegroup_infos_(0, NULL, ObModIds::OB_SCHEMA_TABLEGROUP_INFOS),
      table_id_map_(ObModIds::OB_SCHEMA_TABLE_ID_MAP),
      table_name_map_(ObModIds::OB_SCHEMA_TABLE_NAME_MAP),
      index_name_map_(ObModIds::OB_SCHEMA_TABLE_NAME_MAP),
      database_name_map_(ObModIds::OB_SCHEMA_DATABASE_NAME_MAP),
      tablegroup_name_map_(ObModIds::OB_SCHEMA_TABLEGROUP_NAME_MAP),
      max_used_table_id_map_(),
      use_global_allocator_(false),
      allocator_(local_allocator_),
      priv_mgr_(allocator_),
      pos_in_user_schemas_(-1)
{}

ObSchemaManager::ObSchemaManager(ObMemfragRecycleAllocator& global_allocator)
    : schema_magic_(OB_SCHEMA_MAGIC_NUMBER),
      code_version_(OB_SCHEMA_CODE_VERSION),
      schema_version_(0),
      n_alloc_tables_(0),
      global_alloc_size_(0),
      table_infos_(0, NULL, ObModIds::OB_SCHEMA_TABLE_INFOS),
      index_table_infos_(0, NULL, ObModIds::OB_SCHEMA_INDEX_TABLE_INFOS),
      database_infos_(0, NULL, ObModIds::OB_SCHEMA_DATABASE_INFOS),
      tablegroup_infos_(0, NULL, ObModIds::OB_SCHEMA_TABLEGROUP_INFOS),
      table_id_map_(ObModIds::OB_SCHEMA_TABLE_ID_MAP),
      table_name_map_(ObModIds::OB_SCHEMA_TABLE_NAME_MAP),
      index_name_map_(ObModIds::OB_SCHEMA_TABLE_NAME_MAP),
      database_name_map_(ObModIds::OB_SCHEMA_DATABASE_NAME_MAP),
      tablegroup_name_map_(ObModIds::OB_SCHEMA_TABLEGROUP_NAME_MAP),
      max_used_table_id_map_(),
      use_global_allocator_(true),
      allocator_(global_allocator),
      priv_mgr_(allocator_),
      pos_in_user_schemas_(-1)
{}

int ObSchemaManager::assign(const ObSchemaManager& schema, const bool full_copy)
{
  // TODO: remove it later
  UNUSED(full_copy);
#define ASSIGN_FIELD(x)                        \
  if (OB_SUCC(ret)) {                          \
    if (OB_FAIL(x.assign(schema.x))) {         \
      LOG_WARN("assign " #x "failed", K(ret)); \
    }                                          \
  }

  int ret = OB_SUCCESS;
  if (this != &schema) {
    ASSIGN_FIELD(table_infos_);
    ASSIGN_FIELD(index_table_infos_);
    ASSIGN_FIELD(database_infos_);
    ASSIGN_FIELD(tablegroup_infos_);
    ASSIGN_FIELD(table_id_map_);
    ASSIGN_FIELD(table_name_map_);
    ASSIGN_FIELD(index_name_map_);
    ASSIGN_FIELD(database_name_map_);
    ASSIGN_FIELD(tablegroup_name_map_);
    ASSIGN_FIELD(priv_mgr_);
  }

  if (OB_SUCC(ret)) {
    ObHashMap<uint64_t, uint64_t>::const_iterator const_iter = schema.max_used_table_id_map_.begin();
    int hash_ret = 0;
    max_used_table_id_map_.clear();
    for (; OB_SUCCESS == ret && const_iter != max_used_table_id_map_.end(); ++const_iter) {
      if (HASH_INSERT_SUCC != (hash_ret = max_used_table_id_map_.set(const_iter->first, const_iter->second))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to insert into max_used_table_id_map", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    schema_magic_ = schema.schema_magic_;
    code_version_ = schema.code_version_;
    (void)ATOMIC_SET(&schema_version_, schema.schema_version_);
    n_alloc_tables_ = schema.n_alloc_tables_;
    global_alloc_size_ = (*const_cast<ObSchemaManager*>(&schema)).get_allocator().get_all_alloc_size();
  }

  pos_in_user_schemas_ = schema.pos_in_user_schemas_;
#undef ASSIGN_FIELD
  return ret;
}

ObSchemaManager::~ObSchemaManager()
{
  table_id_map_.destroy();
  table_name_map_.destroy();
  index_name_map_.destroy();
  database_name_map_.destroy();
  tablegroup_name_map_.destroy();
  max_used_table_id_map_.destroy();
}

int ObSchemaManager::init(const bool is_init_sys_tenant)
{
  int ret = OB_SUCCESS;
  if (!use_global_allocator_) {
    if (OB_SUCCESS !=
        (ret = local_allocator_.init(ObModIds::OB_SCHEMA_MANAGER_LOCAL_ALLOCATOR, NULL, SCHEMA_MEM_EXPIRE_TIME))) {
      LOG_WARN("Init local_allocator_ error", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(priv_mgr_.init(is_init_sys_tenant))) {
    LOG_WARN("Init priv_mgr_ error", K(is_init_sys_tenant), K(ret));
  } else if (OB_FAIL(max_used_table_id_map_.create(
                 OB_SCHEMA_MGR_MAX_USED_TID_MAP_BUCKET_NUM, ObModIds::ObModIds::OB_SCHEMA_MAX_USED_TABLE_ID_MAP))) {
    LOG_WARN("failed to create max_used_table_id_map", K(ret));
  } else {
  }
  return ret;
}

void ObSchemaManager::reset()
{
  schema_magic_ = OB_SCHEMA_MAGIC_NUMBER;
  code_version_ = OB_SCHEMA_CODE_VERSION;
  (void)ATOMIC_SET(&schema_version_, 0);
  n_alloc_tables_ = 0;
  global_alloc_size_ = 0;

  table_id_map_.clear();
  table_name_map_.clear();
  index_name_map_.clear();
  database_name_map_.clear();
  tablegroup_name_map_.clear();
  max_used_table_id_map_.clear();
  table_infos_.reset();
  index_table_infos_.reset();
  database_infos_.reset();
  tablegroup_infos_.reset();
  priv_mgr_.reset();
  pos_in_user_schemas_ = -1;
}

ObMemfragRecycleAllocator& ObSchemaManager::get_allocator()
{
  return allocator_;
}

int ObSchemaManager::copy_schema_infos(const ObSchemaManager& schema_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_infos_.assign(schema_manager.table_infos_))) {
    LOG_WARN("failed to assign table_infos", K(ret), "size", schema_manager.table_infos_.size());
  } else if (OB_FAIL(database_infos_.assign(schema_manager.database_infos_))) {
    LOG_WARN("failed to assign database_infos", K(ret), "size", schema_manager.database_infos_.size());
  } else if (OB_FAIL(tablegroup_infos_.assign(schema_manager.tablegroup_infos_))) {
    LOG_WARN("failed to assign tablegroup_infos", K(ret), "size", schema_manager.tablegroup_infos_.size());
  } else if (OB_FAIL(priv_mgr_.copy_priv_infos(schema_manager.priv_mgr_))) {
    LOG_WARN("failed to copy_priv_infos", K(ret));
  }
  return ret;
}

int ObSchemaManager::deep_copy(const ObSchemaManager& schema_manager)
{
  int ret = OB_SUCCESS;
  reset();
  schema_magic_ = schema_manager.schema_magic_;
  code_version_ = schema_manager.code_version_;
  (void)ATOMIC_SET(&schema_version_, schema_manager.schema_version_);
  // skip n_alloc_tables_, as it's the increamental for new schema mgr
  // n_alloc_tables_ = schema_manager.n_alloc_tables_;

  if (OB_FAIL(priv_mgr_.deep_copy(schema_manager.priv_mgr_))) {
    LOG_WARN("deep copy priv_mgr failed", K(ret));
  }

  for (const_table_iterator table_iter = schema_manager.table_begin();
       OB_SUCCESS == ret && table_iter != schema_manager.table_end();
       ++table_iter) {
    if (OB_ISNULL(*table_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*table_iter is null", K(ret));
    } else if (OB_FAIL(add_new_table_schema(**table_iter))) {
      LOG_WARN("add_new_table_schema failed", "table_schema", **table_iter, K(ret));
    }
  }
  for (const_database_iterator db_iter = schema_manager.database_begin();
       OB_SUCCESS == ret && db_iter != schema_manager.database_end();
       ++db_iter) {
    if (OB_ISNULL(*db_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*db_iter is null", K(ret));
    } else if (OB_FAIL(add_new_database_schema(**db_iter))) {
      LOG_WARN("add_new_database_schema failed", "db_schema", **db_iter, K(ret));
    }
  }
  for (const_tablegroup_iterator tg_iter = schema_manager.tablegroup_begin();
       OB_SUCCESS == ret && tg_iter != schema_manager.tablegroup_end();
       ++tg_iter) {
    if (OB_ISNULL(*tg_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*tg_iter is null", K(ret));
    } else if (OB_FAIL(add_new_tablegroup_schema(**tg_iter))) {
      LOG_WARN("add_new_tablegroup_schema failed", "tg_schema", **tg_iter, K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos_in_user_schemas_ = schema_manager.pos_in_user_schemas_;
  }

  return ret;
}

int ObSchemaManager::column_can_be_dropped(
    const uint64_t table_id, const uint64_t column_id, bool& can_be_dropped) const
{
  int ret = OB_SUCCESS;
  can_be_dropped = true;
  int64_t size = OB_MAX_INDEX_PER_TABLE;
  uint64_t idx_tid_array[OB_MAX_INDEX_PER_TABLE];
  const ObTableSchema* table_schema = NULL;
  bool is_rowkey = false;

  if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (table_schema = get_table_schema(table_id))) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("failed to get table schema,", K(table_id), K(ret));
  } else if (OB_FAIL(table_schema->get_rowkey_info().is_rowkey_column(column_id, is_rowkey))) {
    LOG_WARN("failed to check is_rowkey_column,", K(column_id), K(ret));
  } else if (is_rowkey) {
    can_be_dropped = false;
  } else if (OB_FAIL(table_schema->get_index_tid_array(idx_tid_array, size))) {
    LOG_WARN("failed to get index tid array,", K(ret));
  } else {
    bool is_stop = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < size && !is_stop; ++i) {
      if (NULL == (table_schema = get_table_schema(idx_tid_array[i]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get index table schema, ", "index_table_id", idx_tid_array[i], K(ret));
      } else if (OB_FAIL(table_schema->get_rowkey_info().is_rowkey_column(column_id, is_rowkey))) {
        LOG_WARN("failed to check is_rowkey_column,", K(column_id), K(ret));
      } else if (is_rowkey) {
        can_be_dropped = false;
        is_stop = true;
      }
    }
  }
  return ret;
}

int64_t ObSchemaManager::get_total_column_count() const
{
  int ret = OB_SUCCESS;
  int64_t column_count = 0;
  for (const_table_iterator it = table_infos_.begin(); OB_SUCC(ret) && it != table_end(); it++) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("*it is null", K(ret));
    } else {
      column_count += (*it)->get_column_count();
    }
  }
  return column_count;
}

int ObSchemaManager::check_table_exist(const uint64_t table_id, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;

  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(table_id), K(ret));
  } else {
    ObTableSchema* table_schema = NULL;
    if (HASH_EXIST != table_id_map_.get(table_id, table_schema)) {
      table_schema = NULL;
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get table schema return NULL, ", K(table_id), K(ret));
    } else {
      exist = true;
    }
  }

  return ret;
}

int ObSchemaManager::check_table_exist(const uint64_t tenant_id, const ObString& database_name,
    const ObString& table_name, const bool is_index, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;

  if (OB_INVALID_ID == tenant_id || database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: ", K(tenant_id), K(database_name), K(table_name), K(ret));
  } else {
    uint64_t database_id = OB_INVALID_ID;
    bool db_exist = false;
    if (OB_FAIL(check_database_exist(tenant_id, database_name, database_id, db_exist))) {
      LOG_WARN("Check database exist error", K(ret));
    } else if (!db_exist) {
      exist = false;
    } else {
      if (is_index) {
        if (OB_FAIL(check_index_exist(tenant_id, database_id, table_name, exist))) {
          LOG_WARN("Check index exist error", K(tenant_id), K(database_id), K(table_name), K(ret));
        }
      } else {
        if (OB_FAIL(check_table_exist(tenant_id, database_id, table_name, exist))) {
          LOG_WARN("Check table exist error", K(tenant_id), K(database_id), K(table_name), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::check_table_exist(const uint64_t tenant_id, const uint64_t database_id, const ObString& table_name,
    const bool is_index, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: ", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else {
    if (is_index) {
      if (OB_FAIL(check_index_exist(tenant_id, database_id, table_name, exist))) {
        LOG_WARN("Check index exist error", K(tenant_id), K(database_id), K(table_name), K(ret));
      }
    } else {
      if (OB_FAIL(check_table_exist(tenant_id, database_id, table_name, exist))) {
        LOG_WARN("Check table exist error", K(tenant_id), K(database_id), K(table_name), K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::check_database_exist(
    const uint64_t tenant_id, const ObString& database_name, uint64_t& database_id, bool& is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(tenant_id), "database_name", to_cstring(database_name), K(ret));
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_ERROR("fail to get_tenant_name_case_mode", K(tenant_id), K(ret));
  } else {
    ObDatabaseSchemaHashWrapper database_wrapper(tenant_id, mode, database_name);
    ObDatabaseSchema* database_schema = NULL;
    if (HASH_EXIST != database_name_map_.get(database_wrapper, database_schema)) {
      database_schema = NULL;
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get tablegroup schema return NULL, ", K(tenant_id), K(database_name), K(ret));
    } else {
      database_id = database_schema->get_database_id();
      is_exist = true;
    }
  }

  return ret;
}

int ObSchemaManager::check_tablegroup_exist(
    const uint64_t tenant_id, const ObString& tablegroup_name, uint64_t& tablegroup_id, bool& is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(tablegroup_name), K(ret));
  } else {
    const ObTablegroupSchemaHashWrapper tablegroup_wrapper(tenant_id, tablegroup_name);
    ObTablegroupSchema* tablegroup_schema = NULL;
    if (HASH_EXIST != tablegroup_name_map_.get(tablegroup_wrapper, tablegroup_schema)) {
      tablegroup_schema = NULL;
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get tablegroup schema return NULL, ", K(tenant_id), K(tablegroup_name), K(ret));
    } else {
      tablegroup_id = tablegroup_schema->get_tablegroup_id();
      is_exist = true;
    }
  }

  return ret;
}

const ObColumnSchemaV2* ObSchemaManager::get_column_schema(const uint64_t table_id, const uint64_t column_id) const
{
  const ObColumnSchemaV2* column = NULL;
  if (table_infos_.count() > 0) {
    const ObTableSchema* table_schema = get_table_schema(table_id);
    if (OB_ISNULL(table_schema)) {
      LOG_WARN("failed to find table schema:", K(table_id));
    } else {
      column = table_schema->get_column_schema(column_id);
      if (OB_ISNULL(column)) {
        LOG_WARN("failed to find column schema, ", K(column_id), K(table_id));
      }
    }
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManager::get_column_schema(const uint64_t table_id, const ObString& column_name) const
{
  const ObColumnSchemaV2* column = NULL;
  if (table_infos_.count() > 0) {
    const ObTableSchema* table_schema = get_table_schema(table_id);
    if (OB_ISNULL(table_schema)) {
      LOG_WARN("failed to find table schema:", K(table_id));
    } else {
      column = table_schema->get_column_schema(column_name);
      if (OB_ISNULL(column)) {
        LOG_WARN("failed to find column schema, ", K(column_name), K(table_id));
      }
    }
  }
  return column;
}

const ObColumnSchemaV2* ObSchemaManager::get_column_schema(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& table_name, const ObString& column_name, const bool is_index) const
{
  const ObColumnSchemaV2* column_schema = NULL;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    LOG_WARN("invalid arguments:", K(tenant_id), K(database_id), "table_name", to_cstring(table_name));
  } else if (table_infos_.count() <= 0) {
    LOG_WARN("there is no user tables so far when get column schema of table",
        "table_name",
        to_cstring(table_name),
        "table_infos_count",
        table_infos_.count());
  } else {
    const ObTableSchema* table_schema = get_table_schema(tenant_id, database_id, table_name, is_index);
    if (OB_ISNULL(table_schema)) {
      LOG_WARN("failed to get table schema, ", "table_name", to_cstring(table_name), K(tenant_id), K(database_id));
    } else {
      if (table_schema->is_index_table()) {
        LOG_WARN("table schema should not be index table", K(table_schema));
      } else {
        column_schema = table_schema->get_column_schema(column_name);
        if (OB_ISNULL(column_schema)) {
          LOG_WARN("failed to get column schema, ", K(table_name), K(column_name));
        }
      }
    }
  }
  return column_schema;
}

const ObTableSchema* ObSchemaManager::get_table_schema(
    const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name, const bool is_index) const
{
  const ObTableSchema* schema = NULL;
  if (is_index) {
    schema = get_index_schema(tenant_id, database_id, table_name);
  } else {
    schema = get_table_schema(tenant_id, database_id, table_name);
  }
  return schema;
}

const ObTableSchema* ObSchemaManager::get_table_schema(const uint64_t tenant_id, const common::ObString& database_name,
    const common::ObString& table_name, const bool is_index) const
{
  const ObTableSchema* schema = NULL;
  if (is_index) {
    schema = get_index_schema(tenant_id, database_name, table_name);
  } else {
    schema = get_table_schema(tenant_id, database_name, table_name);
  }
  return schema;
}

const ObTableSchema* ObSchemaManager::get_table_schema(const uint64_t table_id) const
{
  ObTableSchema* table = NULL;
  if (table_infos_.count() <= 0) {
    LOG_WARN("there is no user tables so far when get table schema of ,",
        K(table_id),
        "table_infos.count",
        table_infos_.count());
  } else if (HASH_EXIST != table_id_map_.get(table_id, table)) {
    table = NULL;
  }
  return table;
}

const ObTableSchema* ObSchemaManager::get_table_schema(
    const uint64_t tenant_id, const uint64_t database_id, const ObString& table_name) const
{
  ObTableSchema* table_schema = NULL;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(table_name));
  } else if (table_infos_.count() <= 0) {
    LOG_WARN("there is no user tables so far when get table schema",
        K(tenant_id),
        K(database_id),
        K(table_name),
        "table_infos_count",
        table_infos_.count());
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_ERROR("fail to get_tenant_name_case_mode", K(tenant_id), K(ret));
  } else {
    const ObTableSchemaHashWrapper table_name_wrapper(tenant_id, database_id, mode, table_name);
    if (HASH_EXIST != table_name_map_.get(table_name_wrapper, table_schema)) {
      table_schema = NULL;
    }
  }
  return table_schema;
}

const ObTableSchema* ObSchemaManager::get_table_schema(
    const uint64_t tenant_id, const ObString& database_name, const ObString& table_name) const
{
  const ObTableSchema* table_schema = NULL;
  if (OB_INVALID_ID == tenant_id || database_name.empty() || table_name.empty()) {
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(table_name));
  } else if (table_infos_.count() <= 0) {
    LOG_WARN("there is no user tables so far when get table schema",
        K(tenant_id),
        K(database_name),
        K(table_name),
        "table_infos_count",
        table_infos_.count());
  } else {
    int tmp_ret = OB_SUCCESS;
    uint64_t database_id = OB_INVALID_ID;
    if (OB_SUCCESS != (tmp_ret = get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("failed to get database id, ", K(tenant_id), K(database_name), K(tmp_ret));
    } else if (NULL == (table_schema = get_table_schema(tenant_id, database_id, table_name))) {
      LOG_WARN("failed to get table schema, ", K(tenant_id), K(database_id), K(table_name), K(tmp_ret));
    }
  }
  return table_schema;
}

const ObTableSchema* ObSchemaManager::get_index_schema(
    const uint64_t tenant_id, const uint64_t database_id, const ObString& index_name) const
{
  ObTableSchema* index_schema = NULL;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || index_name.empty()) {
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(index_name));
  } else if (table_infos_.count() <= 0) {
    LOG_WARN("there is no user tables so far when get table schema",
        K(tenant_id),
        K(database_id),
        K(index_name),
        "table_infos_count",
        table_infos_.count());
  } else {
    const ObIndexSchemaHashWrapper index_schema_wrapper(tenant_id, database_id, index_name);
    if (HASH_EXIST != index_name_map_.get(index_schema_wrapper, index_schema)) {
      index_schema = NULL;
    } else if (NULL != index_schema && !index_schema->is_index_table()) {
      index_schema = NULL;
      LOG_WARN("schema is not index", K(index_schema));
    }
  }
  return index_schema;
}

const ObTableSchema* ObSchemaManager::get_index_schema(
    const uint64_t tenant_id, const ObString& database_name, const ObString& index_name) const
{
  const ObTableSchema* index_schema = NULL;
  if (OB_INVALID_ID == tenant_id || database_name.empty() || index_name.empty()) {
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(index_name));
  } else if (table_infos_.count() <= 0) {
    LOG_WARN("there is no user tables so far when get table schema",
        K(tenant_id),
        K(database_name),
        K(index_name),
        "table_infos_count",
        table_infos_.count());
  } else {
    int tmp_ret = OB_SUCCESS;
    uint64_t database_id = OB_INVALID_ID;
    if (OB_SUCCESS != (tmp_ret = get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("failed to get database id, ", K(tenant_id), K(database_name), K(tmp_ret));
    } else if (NULL == (index_schema = get_index_schema(tenant_id, database_id, index_name))) {
      LOG_WARN("failed to get table schema, ", K(tenant_id), K(database_id), K(index_name), K(tmp_ret));
    } else if (!index_schema->is_index_table()) {
      index_schema = NULL;
      LOG_WARN("schema is not index", K(index_schema));
    }
  }
  return index_schema;
}

int ObSchemaManager::get_tenant_name_case_mode(const uint64_t tenant_id, ObNameCaseMode& mode) const
{
  int ret = OB_SUCCESS;
  mode = OB_NAME_CASE_INVALID;
  const ObTenantSchema* schema = NULL;
  if (NULL == (schema = get_tenant_info(tenant_id))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("fail to get tenant info", K(tenant_id), K(ret));
  } else {
    ObNameCaseMode local_mode = schema->get_name_case_mode();
    if (local_mode <= OB_NAME_CASE_INVALID || local_mode >= OB_NAME_CASE_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid name case mode of tenant", K(tenant_id), K(local_mode), K(ret));
    } else {
      mode = local_mode;
    }
  }
  return ret;
}

const ObTableSchema* ObSchemaManager::get_index_table_schema(
    const uint64_t data_table_id, const ObString& index_name) const
{
  int ret = OB_SUCCESS;
  char buffer[OB_MAX_TABLE_NAME_BUF_LENGTH];
  ObDataBuffer data_buffer(buffer, sizeof(buffer));
  const ObTableSchema* data_table_schema = NULL;
  const ObTableSchema* index_table_schema = NULL;
  ObString index_table_name;
  if (OB_FAIL(ObTableSchema::build_index_table_name(data_buffer, data_table_id, index_name, index_table_name))) {
    LOG_WARN("build_index_table_name failed", K(data_table_id), K(index_name), K(ret));
  } else if (NULL == (data_table_schema = get_table_schema(data_table_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("data table schema not exist", K(data_table_id), K(ret));
  } else if (NULL ==
             (index_table_schema = get_index_schema(
                  data_table_schema->get_tenant_id(), data_table_schema->get_database_id(), index_table_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("index table schema not exist", K(data_table_id), K(index_name), K(ret));
  }

  return index_table_schema;
}

const ObTableSchema* ObSchemaManager::get_index_table_schema(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& data_table_name, const ObString& index_name) const
{
  int ret = OB_SUCCESS;
  char buffer[OB_MAX_TABLE_NAME_BUF_LENGTH];
  ObDataBuffer data_buffer(buffer, sizeof(buffer));
  const ObTableSchema* data_table_schema = NULL;
  const ObTableSchema* index_table_schema = NULL;
  ObString index_table_name;
  if (NULL == (data_table_schema = get_table_schema(tenant_id, database_id, data_table_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("data table schema not exist", K(tenant_id), K(database_id), K(data_table_name), K(ret));
  } else if (OB_FAIL(ObTableSchema::build_index_table_name(
                 data_buffer, data_table_schema->get_table_id(), index_name, index_table_name))) {
    LOG_WARN(
        "build_index_table_name failed", "data_table_id", data_table_schema->get_table_id(), K(index_name), K(ret));
  } else if (NULL == (index_table_schema = get_index_schema(tenant_id, database_id, index_table_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("index table schema not exist", K(tenant_id), K(database_id), K(index_table_name), K(ret));
  }

  return index_table_schema;
}

int ObSchemaManager::get_table_schemas_in_tenant(
    const uint64_t tenant_id, ObIArray<const ObTableSchema*>& table_schema_array) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);
  const_table_iterator tenant_table_begin = table_infos_.lower_bound(tenant_table_id_lower, compare_tenant_table_id);
  bool is_stop = false;
  for (const_table_iterator table_schema_iter = tenant_table_begin;
       OB_SUCCESS == ret && table_schema_iter != table_end() && !is_stop;
       table_schema_iter++) {
    if (NULL == (table_schema = *table_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get table schema, ", K(table_schema), K(ret));
    } else if (tenant_id == table_schema->get_tenant_id()) {
      if (OB_FAIL(table_schema_array.push_back(table_schema))) {
        LOG_WARN("failed to push back table schema:", K(ret));
      }
    } else if (tenant_id != table_schema->get_tenant_id()) {
      is_stop = true;
    }
  }
  return ret;
}

int ObSchemaManager::get_table_ids_in_tenant(const uint64_t tenant_id, ObIArray<uint64_t>& table_ids) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> table_schemas;
  if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("get_table_schemas_in_tenant failed", K(tenant_id), K(ret));
  } else {
    FOREACH_CNT_X(table, table_schemas, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(*table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("*table is null", K(ret));
      } else if (OB_FAIL(table_ids.push_back((*table)->get_table_id()))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::get_table_schemas_in_database(
    const uint64_t tenant_id, const uint64_t database_id, ObIArray<const ObTableSchema*>& table_schema_array) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  bool is_exist = false;
  if (OB_FAIL(check_database_exist(database_id, is_exist))) {
    LOG_WARN("failed to check database exist, ", K(database_id), K(ret));
  } else if (!is_exist) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database is not exist, ", K(database_id), K(ret));
  } else {
    ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);
    const_table_iterator tenant_table_begin = table_infos_.lower_bound(tenant_table_id_lower, compare_tenant_table_id);
    bool is_stop = false;
    for (const_table_iterator table_schema_iter = tenant_table_begin;
         OB_SUCCESS == ret && table_schema_iter != table_end() && !is_stop;
         table_schema_iter++) {
      if (NULL == (table_schema = *table_schema_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid table schema pointer, ", K(table_schema), K(ret));
      } else if (database_id == table_schema->get_database_id()) {
        if (tenant_id != table_schema->get_tenant_id()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant id is invalid", K(tenant_id), K(database_id), K(ret));
        } else if (OB_FAIL(table_schema_array.push_back(table_schema))) {
          LOG_WARN("failed to push back table schema:", K(ret));
        }
      } else if (tenant_id != table_schema->get_tenant_id()) {
        is_stop = true;
      }
    }
  }
  return ret;
}

int ObSchemaManager::get_table_ids_in_database(
    const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<uint64_t>& table_id_array) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  bool is_exist = false;
  if (OB_FAIL(check_database_exist(database_id, is_exist))) {
    LOG_WARN("failed to check database[ exist, ", K(database_id), K(ret));
  } else if (!is_exist) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database is not exist, ", K(database_id), K(ret));
  } else {
    bool is_stop = false;
    ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);
    const_table_iterator tenant_table_begin = table_infos_.lower_bound(tenant_table_id_lower, compare_tenant_table_id);
    for (const_table_iterator table_schema_iter = tenant_table_begin;
         OB_SUCCESS == ret && table_schema_iter != table_end() && !is_stop;
         table_schema_iter++) {
      if (NULL == (table_schema = *table_schema_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid table schema point, ", K(table_schema), K(ret));
      } else if (tenant_id != table_schema->get_tenant_id()) {
        is_stop = true;
      } else if (database_id == table_schema->get_database_id()) {
        if (OB_FAIL(table_id_array.push_back(table_schema->get_table_id()))) {
          LOG_WARN("failed to push back table id:", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::get_table_ids_in_tablegroup(
    const uint64_t tenant_id, const uint64_t tablegroup_id, ObIArray<uint64_t>& table_id_array) const
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> table_schemas;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or invalid tablegroup_id", K(tenant_id), K(tablegroup_id), K(ret));
  } else if (OB_FAIL(get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("get_table_schemas_in_tablegroup failed", K(tenant_id), K(tablegroup_id), K(ret));
  } else {
    FOREACH_CNT_X(table_schema, table_schemas, OB_SUCCESS == ret)
    {
      if (NULL == *table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null");
      } else if (OB_FAIL(table_id_array.push_back((*table_schema)->get_table_id()))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::get_table_schemas_in_tablegroup(
    const uint64_t tenant_id, const uint64_t tablegroup_id, ObIArray<const ObTableSchema*>& table_schemas) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  bool is_exist = false;
  if (OB_FAIL(check_tablegroup_exist(tablegroup_id, is_exist))) {
    LOG_WARN("failed to check tablegroup exist, ", K(tablegroup_id), K(ret));
  } else if (!is_exist) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("tablegroup is not exist, ", K(tablegroup_id), K(ret));
  } else {
    bool is_stop = false;
    ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);
    const_table_iterator tenant_table_begin = table_infos_.lower_bound(tenant_table_id_lower, compare_tenant_table_id);
    for (const_table_iterator table_schema_iter = tenant_table_begin;
         OB_SUCCESS == ret && table_schema_iter != table_end() && !is_stop;
         table_schema_iter++) {
      if (NULL == (table_schema = *table_schema_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid table schema point, ", K(table_schema), K(ret));
      } else if (tenant_id != table_schema->get_tenant_id()) {
        is_stop = true;
      } else if (tablegroup_id == table_schema->get_tablegroup_id()) {
        if (OB_FAIL(table_schemas.push_back(table_schema))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::check_database_exists_in_tablegroup(
    const uint64_t tenant_id, const uint64_t tablegroup_id, bool& not_empty) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* database_schema = NULL;
  bool is_exist = false;
  if (OB_FAIL(check_tablegroup_exist(tablegroup_id, is_exist))) {
    LOG_WARN("failed to check tablegroup exist, ", K(tablegroup_id), K(ret));
  } else if (!is_exist) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("tablegroup is not exist, ", K(tablegroup_id), K(ret));
  } else {
    ObTenantDatabaseId tenant_database_id_lower(tenant_id, OB_MIN_ID);
    const_database_iterator tenant_database_begin =
        database_infos_.lower_bound(tenant_database_id_lower, compare_tenant_database_id);
    bool is_stop = false;
    for (const_database_iterator database_schema_iter = tenant_database_begin;
         OB_SUCCESS == ret && database_schema_iter != database_end() && !is_stop;
         database_schema_iter++) {
      if (NULL == (database_schema = *database_schema_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid database schema point, ", K(database_schema), K(ret));
      } else if (tenant_id != database_schema->get_tenant_id()) {
        is_stop = true;
      } else if (tablegroup_id == database_schema->get_default_tablegroup_id()) {
        not_empty = true;
        is_stop = true;
      }
    }
  }
  return ret;
}

int ObSchemaManager::batch_get_next_table(
    const ObTenantTableId tenant_table_id, const int64_t get_size, ObIArray<ObTenantTableId>& table_array) const
{
  int ret = OB_SUCCESS;
  table_array.reset();
  const_table_iterator tenant_table_begin = table_infos_.upper_bound(tenant_table_id, compare_tenant_table_id_up);
  ObTenantTableId tmp_tenant_table_id;
  const ObTableSchema* table_schema = NULL;
  const_table_iterator table_schema_iter = tenant_table_begin;
  for (; OB_SUCC(ret) && table_array.count() < get_size && table_schema_iter != table_end(); table_schema_iter++) {
    if (NULL == (table_schema = *table_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid table schema point,  ", K(table_schema), K(ret));
    } else {
      tmp_tenant_table_id.tenant_id_ = table_schema->get_tenant_id();
      tmp_tenant_table_id.table_id_ = table_schema->get_table_id();
      if (OB_FAIL(table_array.push_back(tmp_tenant_table_id))) {
        LOG_WARN("failed to push back tenant table id:", K(ret));
      }
    }
  }

  if (OB_SUCCESS == ret && table_end() == table_schema_iter) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObSchemaManager::del_databases(const hash::ObHashSet<uint64_t>& database_id_set)
{
  int ret = OB_SUCCESS;
  if (database_id_set.size() > 0) {
    for (hash::ObHashSet<uint64_t>::const_iterator it = database_id_set.begin();
         OB_SUCCESS == ret && it != database_id_set.end();
         it++) {
      ObTenantDatabaseId id(extract_tenant_id(it->first), it->first);
      if (OB_FAIL(del_database(id))) {
        LOG_WARN("failed to remove database", K(ret), K(id));
      } else {
        LOG_INFO("succ to remove database", K(id));
      }
    }
  }
  return ret;
}

int ObSchemaManager::del_tablegroups(const hash::ObHashSet<uint64_t>& tablegroup_id_set)
{
  int ret = OB_SUCCESS;
  if (tablegroup_id_set.size() > 0) {
    for (hash::ObHashSet<uint64_t>::const_iterator it = tablegroup_id_set.begin();
         OB_SUCCESS == ret && it != tablegroup_id_set.end();
         it++) {
      ObTenantTablegroupId id(extract_tenant_id(it->first), it->first);
      if (OB_FAIL(del_tablegroup(id))) {
        LOG_WARN("failed to remove tablegroup", K(ret), K(id));
      } else {
        LOG_INFO("succ to remove tablegroup", K(id));
      }
    }
  }
  return ret;
}

int ObSchemaManager::del_tables(const hash::ObHashSet<uint64_t>& table_id_set)
{
  int ret = OB_SUCCESS;
  if (table_id_set.size() > 0) {
    for (hash::ObHashSet<uint64_t>::const_iterator it = table_id_set.begin();
         OB_SUCCESS == ret && it != table_id_set.end();
         it++) {
      ObTenantTableId id(extract_tenant_id(it->first), it->first);
      if (OB_FAIL(del_table(id))) {
        LOG_WARN("failed to remove table", K(ret), K(id));
      } else {
        LOG_INFO("succ to remove table", K(id));
      }
    }
  }
  return ret;
}

int ObSchemaManager::del_schemas_in_tenants(const hash::ObHashSet<uint64_t>& tenant_id_set)
{
  int ret = OB_SUCCESS;
  if (tenant_id_set.size() > 0) {
    for (hash::ObHashSet<uint64_t>::const_iterator it = tenant_id_set.begin();
         OB_SUCCESS == ret && it != tenant_id_set.end();
         ++it) {
      if (OB_FAIL(del_schemas_in_tenant(it->first))) {
        LOG_WARN("del_schemas_in_tenant failed", "tenant", it->first, K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::update_max_used_table_ids(const ObIArray<uint64_t>& max_used_table_ids)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(max_used_table_id, max_used_table_ids, OB_SUCCESS == ret)
  {
    if (OB_ISNULL(max_used_table_id) || OB_INVALID_ID == *max_used_table_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("max_used table id should not be invalid", K(ret));
    } else {
      uint64_t tenant_id = extract_tenant_id(*max_used_table_id);
      int hash_ret = 0;
      if (0 == extract_pure_id(*max_used_table_id)) {
        if (-1 == (hash_ret = max_used_table_id_map_.erase(tenant_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to erase entry in max_used_table_id map", K(hash_ret), K(ret));
        }
      } else if (-1 == (hash_ret = max_used_table_id_map_.set(tenant_id, *max_used_table_id, 1 /*overwrite*/))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set max_used_table_id_map_", KT(*max_used_table_id), K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::get_database_schemas_in_tenant(
    const uint64_t tenant_id, ObIArray<const ObDatabaseSchema*>& database_schema_array) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* database_schema = NULL;
  ObTenantDatabaseId tenant_database_id_lower(tenant_id, OB_MIN_ID);
  const_database_iterator tenant_database_begin =
      database_infos_.lower_bound(tenant_database_id_lower, compare_tenant_database_id);
  bool is_stop = false;
  for (const_database_iterator database_schema_iter = tenant_database_begin;
       OB_SUCCESS == ret && database_schema_iter != database_end() && !is_stop;
       database_schema_iter++) {
    if (NULL == (database_schema = *database_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid got database schema pointer,  ", K(database_schema), K(ret));
    } else if (tenant_id == database_schema->get_tenant_id()) {
      if (OB_FAIL(database_schema_array.push_back(database_schema))) {
        LOG_WARN("failed to push back database schema:", K(ret));
      }
    } else if (tenant_id != database_schema->get_tenant_id()) {
      is_stop = true;
    }
  }
  return ret;
}

int ObSchemaManager::get_tablegroup_schemas_in_tenant(
    const uint64_t tenant_id, ObArray<const ObTablegroupSchema*>& tablegroup_schema_array) const
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  ObTenantTablegroupId tenant_tablegroup_id_lower(tenant_id, OB_MIN_ID);
  const_tablegroup_iterator tenant_tablegroup_begin =
      tablegroup_infos_.lower_bound(tenant_tablegroup_id_lower, compare_tenant_tablegroup_id);
  bool is_stop = false;
  for (const_tablegroup_iterator tablegroup_schema_iter = tenant_tablegroup_begin;
       OB_SUCCESS == ret && tablegroup_schema_iter != tablegroup_end() && !is_stop;
       tablegroup_schema_iter++) {
    if (NULL == (tablegroup_schema = *tablegroup_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid got tablegroup schema pointer, ", K(tablegroup_schema), K(ret));
    } else if (tenant_id == tablegroup_schema->get_tenant_id()) {
      if (OB_FAIL(tablegroup_schema_array.push_back(tablegroup_schema))) {
        LOG_WARN("failed to push back tablegroup schema:", K(ret));
      }
    } else if (tenant_id != tablegroup_schema->get_tenant_id()) {
      is_stop = true;
    }
  }
  return ret;
}

// these two can reverse the schema pointer array
int ObSchemaManager::get_tablegroup_ids_in_tenant(
    const uint64_t tenant_id, ObArray<uint64_t>& tablegroup_id_array) const
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  ObTenantTablegroupId tenant_tablegroup_id_lower(tenant_id, OB_MIN_ID);
  const_tablegroup_iterator tenant_tablegroup_begin =
      tablegroup_infos_.lower_bound(tenant_tablegroup_id_lower, compare_tenant_tablegroup_id);
  bool is_stop = false;
  for (const_tablegroup_iterator tablegroup_schema_iter = tenant_tablegroup_begin;
       OB_SUCCESS == ret && tablegroup_schema_iter != tablegroup_end() && !is_stop;
       tablegroup_schema_iter++) {
    if (NULL == (tablegroup_schema = *tablegroup_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid got tablegroup schema pointer, ", K(tablegroup_schema), K(ret));
    } else if (tenant_id == tablegroup_schema->get_tenant_id()) {
      if (OB_SUCCESS != (ret = tablegroup_id_array.push_back(tablegroup_schema->get_tablegroup_id()))) {
        LOG_WARN("failed to push back tablegroup schema:", K(ret));
      }
    } else if (tenant_id != tablegroup_schema->get_tenant_id()) {
      is_stop = true;
    }
  }
  return ret;
}

int ObSchemaManager::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> tables;
  ObArray<const ObDatabaseSchema*> databases;
  ObArray<const ObTablegroupSchema*> tablegroups;

  // delete tables of tenant
  if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("get_table_schemas_in_tenant failed", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      const ObTableSchema* table_schema = tables.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tables.at(i) is null", K(ret));
      } else {
        ObTenantTableId tenant_table_id(tenant_id, table_schema->get_table_id());
        if (OB_FAIL(del_table(tenant_table_id))) {
          LOG_WARN("del_table failed", K(tenant_table_id), K(ret));
        }
      }
    }
  }

  // delete databases of tenant
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_database_schemas_in_tenant(tenant_id, databases))) {
    LOG_WARN("get_database_schemas_in_tenant failed", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < databases.count(); ++i) {
      const ObDatabaseSchema* database_schema = databases.at(i);
      if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("database_schema is bull", K(ret));
      } else {
        ObTenantDatabaseId tenant_database_id(tenant_id, database_schema->get_database_id());
        if (OB_FAIL(del_database(tenant_database_id))) {
          LOG_WARN("del_database failed", K(tenant_database_id), K(ret));
        }
      }
    }
  }

  // delete tablegroups of tenant
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    LOG_WARN("get_tablegroup_schemas_in_tenant failed", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablegroups.count(); ++i) {
      const ObTablegroupSchema* tablegroup_schema = tablegroups.at(i);
      if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablegroup_schema is null", K(ret));
      } else {
        ObTenantTablegroupId tenant_tablegroup_id(tenant_id, tablegroup_schema->get_tablegroup_id());
        if (OB_FAIL(del_tablegroup(tenant_tablegroup_id))) {
          LOG_WARN("del_tablegroup failed", K(tenant_tablegroup_id), K(ret));
        }
      }
    }
  }

  return ret;
}

// TODO  we did not constuct a index (tenant_id,database_id) ->ObDatabaseSchema *
// if performance is not satisfatory
const ObDatabaseSchema* ObSchemaManager::get_database_schema(const uint64_t tenant_id, const uint64_t database_id) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* database_schema = NULL;
  ObTenantDatabaseId tenant_database_id_lower(tenant_id, OB_MIN_ID);
  const_database_iterator tenant_database_begin =
      database_infos_.lower_bound(tenant_database_id_lower, compare_tenant_database_id);
  bool is_stop = false;
  for (const_database_iterator database_schema_iter = tenant_database_begin;
       OB_SUCCESS == ret && database_schema_iter != database_end() && !is_stop;
       database_schema_iter++) {
    if (NULL == (database_schema = *database_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid got database schema pointer, ", K(database_schema), K(ret));
    } else if (database_id == database_schema->get_database_id()) {
      if (tenant_id != database_schema->get_tenant_id()) {
        LOG_WARN("tenant id is invalid", K(tenant_id), K(database_id), K(ret));
        database_schema = NULL;
      }
      is_stop = true;
    } else {
      database_schema = NULL;
    }
  }
  return database_schema;
}

const ObDatabaseSchema* ObSchemaManager::get_database_schema(const uint64_t database_id) const
{
  return get_database_schema(extract_tenant_id(database_id), database_id);
}

const ObDatabaseSchema* ObSchemaManager::get_database_schema(
    const uint64_t tenant_id, const ObString& database_name) const
{
  ObDatabaseSchema* database_schema = NULL;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    LOG_WARN("invalid argument", K(tenant_id), K(database_name));
  } else if (database_infos_.count() <= 0) {
    LOG_WARN("there is no database so far when get table schema",
        K(tenant_id),
        K(database_name),
        "table_infos_count",
        database_infos_.count());
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_ERROR("fail to get_tenant_name_case_mode", K(tenant_id), K(ret));
  } else {
    const ObDatabaseSchemaHashWrapper database_name_wrapper(tenant_id, mode, database_name);
    if (HASH_EXIST != database_name_map_.get(database_name_wrapper, database_schema)) {
      database_schema = NULL;
    }
  }
  return database_schema;
}

// TODO  we did not constuct a index (tenant_id,tablegroup_id) ->ObTablegroupSchema *
// if performance is not satisfatory
const ObTablegroupSchema* ObSchemaManager::get_tablegroup_schema(
    const uint64_t tenant_id, const uint64_t tablegroup_id) const
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  ObTenantTablegroupId tenant_tablegroup_id_lower(tenant_id, OB_MIN_ID);
  const_tablegroup_iterator tenant_tablegroup_begin =
      tablegroup_infos_.lower_bound(tenant_tablegroup_id_lower, compare_tenant_tablegroup_id);
  bool is_stop = false;
  for (const_tablegroup_iterator tablegroup_schema_iter = tenant_tablegroup_begin;
       OB_SUCCESS == ret && tablegroup_schema_iter != tablegroup_end() && !is_stop;
       tablegroup_schema_iter++) {
    if (NULL == (tablegroup_schema = *tablegroup_schema_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid got tablegroup schema pointer,  ", K(tablegroup_schema), K(ret));
    } else if (tablegroup_id == tablegroup_schema->get_tablegroup_id()) {
      if (tenant_id != tablegroup_schema->get_tenant_id()) {
        LOG_WARN("tenant id is invalid", K(tenant_id), K(tablegroup_id), K(ret));
        tablegroup_schema = NULL;
      }
      is_stop = true;
    } else {
      tablegroup_schema = NULL;
    }
  }
  return tablegroup_schema;
}

const ObTablegroupSchema* ObSchemaManager::get_tablegroup_schema(const uint64_t tablegroup_id) const
{
  return get_tablegroup_schema(extract_tenant_id(tablegroup_id), tablegroup_id);
}

int ObSchemaManager::add_new_table_schema_array(const ObArray<ObTableSchema>& schema_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schema_array.count() && OB_SUCCESS == ret; ++i) {
    const ObTableSchema& table_schema = schema_array.at(i);
    if (OB_FAIL(add_new_table_schema(table_schema))) {
      LOG_WARN("add_new_table_schema", K(table_schema), K(ret));
    } else {
      n_alloc_tables_++;
    }
  }
  return ret;
}

int ObSchemaManager::add_new_table_schema_array(const ObArray<ObTableSchema*>& schema_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schema_array.count() && OB_SUCCESS == ret; ++i) {
    const ObTableSchema* table_schema = schema_array.at(i);
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(ret));
    } else if (OB_FAIL(add_new_table_schema(*table_schema))) {
      LOG_WARN("add_new_table_schema", "table_schema", *table_schema, K(ret));
    } else {
      n_alloc_tables_++;
    }
  }
  return ret;
}

int ObSchemaManager::add_new_database_schema_array(const ObArray<ObDatabaseSchema>& schema_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schema_array.count() && OB_SUCCESS == ret; ++i) {
    const ObDatabaseSchema& database_schema = schema_array.at(i);
    if (OB_FAIL(add_new_database_schema(database_schema))) {
      LOG_WARN("add_new_database_schema failed", K(database_schema), K(ret));
    }
  }
  return ret;
}

int ObSchemaManager::add_new_tablegroup_schema_array(const ObArray<ObTablegroupSchema>& schema_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schema_array.count() && OB_SUCCESS == ret; ++i) {
    const ObTablegroupSchema& tablegroup_schema = schema_array.at(i);
    if (OB_FAIL(add_new_tablegroup_schema(tablegroup_schema))) {
      LOG_WARN("add_new_tablegroup_schema failed", K(tablegroup_schema), K(ret));
    }
  }
  return ret;
}

// convert the new table schema into old one and insert it into the schema_manager
int ObSchemaManager::add_new_table_schema(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  int64_t convert_size = 0;
  ObTableSchema* new_schema = NULL;
  char* buffer = NULL;
  const bool is_replace = false;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;

  convert_size = table_schema.get_convert_size() + sizeof(ObDataBuffer);
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (OB_FAIL(get_tenant_name_case_mode(table_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get tenant name case mode", "tenant_id", table_schema.get_tenant_id(), K(ret));
  } else if (NULL == (buffer = static_cast<char*>(allocator_.alloc(convert_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory, ", K(convert_size), K(ret));
  } else {
    ObDataBuffer* databuf = NULL;
    if (NULL == (databuf = new (buffer + sizeof(table_schema))
                        ObDataBuffer(buffer + sizeof(table_schema) + sizeof(ObDataBuffer),
                            convert_size - sizeof(table_schema) - sizeof(ObDataBuffer)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to new ObDataBuffer, ", K(ret));
    } else if (NULL == (new_schema = new (buffer) ObTableSchema(databuf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to new table schema, ", K(ret));
    } else {
      if (OB_FAIL((*new_schema).assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        new_schema->set_name_case_mode(mode);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(add_table_schema(new_schema, is_replace))) {
        LOG_WARN("failed to add table schema, ", K(ret));
      } else {
        uint64_t table_id = new_schema->get_table_id();
        LOG_INFO("Success to add table schema, ", KT(table_id));
      }
    }
  }

  return ret;
}

int ObSchemaManager::add_new_database_schema(const ObDatabaseSchema& database_schema)
{
  int ret = OB_SUCCESS;
  int64_t convert_size = 0;
  ObDatabaseSchema* new_schema = NULL;
  char* buffer = NULL;
  const bool is_replace = false;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;

  convert_size = database_schema.get_convert_size() + sizeof(ObDataBuffer);
  if (!database_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new database schema is invalid", K(ret));
  } else if (OB_FAIL(get_tenant_name_case_mode(database_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get tenant name case mode", "tenant_id", database_schema.get_tenant_id(), K(ret));
  } else if (NULL == (buffer = static_cast<char*>(allocator_.alloc(convert_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory for database, ", K(convert_size), K(ret));
  } else {
    ObDataBuffer* databuf = NULL;
    if (NULL == (databuf = new (buffer + sizeof(database_schema))
                        ObDataBuffer(buffer + sizeof(database_schema) + sizeof(ObDataBuffer),
                            convert_size - sizeof(database_schema) - sizeof(ObDataBuffer)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to new ObDataBuffer, ", K(ret));
    } else if (NULL == (new_schema = new (buffer) ObDatabaseSchema(databuf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to new database schema, ", K(ret));
    } else {
      *new_schema = database_schema;
      new_schema->set_name_case_mode(mode);
      if (OB_FAIL(add_database_schema(new_schema, is_replace))) {
        LOG_WARN("failed to add database schema, ", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaManager::add_new_tablegroup_schema(const ObTablegroupSchema& tablegroup_schema)
{
  int ret = OB_SUCCESS;
  int64_t convert_size = 0;
  ObTablegroupSchema* new_schema = NULL;
  char* buffer = NULL;
  const bool is_replace = false;

  convert_size = tablegroup_schema.get_convert_size() + sizeof(ObDataBuffer);
  if (!tablegroup_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new tablegroup schema is invalid", K(ret));
  } else if (NULL == (buffer = static_cast<char*>(allocator_.alloc(convert_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory for tablegroup, ", K(convert_size), K(ret));
  } else {
    ObDataBuffer* databuf = NULL;
    if (NULL == (databuf = new (buffer + sizeof(tablegroup_schema))
                        ObDataBuffer(buffer + sizeof(tablegroup_schema) + sizeof(ObDataBuffer),
                            convert_size - sizeof(tablegroup_schema) - sizeof(ObDataBuffer)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to new ObDataBuffer, ", K(ret));
    } else if (NULL == (new_schema = new (buffer) ObTablegroupSchema(databuf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to new tablegroup schema, ", K(ret));
    } else {
      *new_schema = tablegroup_schema;
      if (OB_FAIL(add_tablegroup_schema(new_schema, is_replace))) {
        LOG_WARN("failed to add tablegroup schema, ", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaManager::do_table_name_check(const ObTableSchema* old_table_schema, const ObTableSchema* new_table_schema)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (OB_ISNULL(old_table_schema) || OB_ISNULL(new_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(old_table_schema), K(new_table_schema));
  } else {
    const ObString& old_table_name = old_table_schema->get_table_name_str();
    const ObString& new_table_name = new_table_schema->get_table_name_str();
    const uint64_t old_table_id = old_table_schema->get_table_id();
    const uint64_t new_table_id = new_table_schema->get_table_id();

    // when table renamed(e.g.,using AlTER TABLE RENAME sql), we should delete old
    //(table_name->ObTableSchema*) kv pair in table_name_map_. Otherwise when we
    // insert new(table_name->ObTableSchema*) kv pair later, the old one can not be
    // overwrited and the schema will become inconsistent.
    if (old_table_id != new_table_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("old_table_id != new_table_id, never run here, bug.", K(old_table_id), K(new_table_id), K(ret));
    } else if (old_table_name != new_table_name) {
      LOG_INFO("table renamed, ",
          "old_table_name",
          to_cstring(old_table_name),
          "new_table_name",
          to_cstring(new_table_name));
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(get_tenant_name_case_mode(new_table_schema->get_tenant_id(), mode))) {
        LOG_WARN("fail to get tenant name case mode", K(new_table_id), K(ret));
      } else {
        ObTableSchemaHashWrapper table_wrapper(old_table_schema->get_tenant_id(),
            old_table_schema->get_database_id(),
            mode,
            old_table_schema->get_table_name_str());
        if (HASH_EXIST != (hash_ret = table_name_map_.erase(table_wrapper))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to delete table from table name hashmap, ",
              "old_table_name",
              to_cstring(old_table_name),
              K(hash_ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::do_database_name_check(
    const ObDatabaseSchema* old_db_schema, const ObDatabaseSchema* new_db_schema)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (OB_ISNULL(old_db_schema) || OB_ISNULL(new_db_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(old_db_schema), K(new_db_schema));
  } else {
    const ObString& old_db_name = old_db_schema->get_database_name_str();
    const ObString& new_db_name = new_db_schema->get_database_name_str();
    const uint64_t old_db_id = old_db_schema->get_database_id();
    const uint64_t new_db_id = new_db_schema->get_database_id();

    if (old_db_id != new_db_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("old_database_id != new_database_id, never run here, bug.", K(old_db_id), K(new_db_id), K(ret));
    } else if (old_db_name != new_db_name) {
      LOG_INFO("database renamed, ",
          "old_database_name",
          to_cstring(old_db_name),
          "new_database_name",
          to_cstring(new_db_name));

      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(get_tenant_name_case_mode(new_db_schema->get_tenant_id(), mode))) {
        LOG_WARN("fail to get tenant name case mode", K(new_db_id), K(ret));
      } else {
        ObDatabaseSchemaHashWrapper db_wrapper(
            old_db_schema->get_tenant_id(), mode, old_db_schema->get_database_name_str());
        if (HASH_EXIST != (hash_ret = database_name_map_.erase(db_wrapper))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to delete database from database name hashmap, ",
              "old_database_name",
              to_cstring(old_db_name),
              K(hash_ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::do_tablegroup_name_check(
    const ObTablegroupSchema* old_tg_schema, const ObTablegroupSchema* new_tg_schema)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (OB_ISNULL(old_tg_schema) || OB_ISNULL(new_tg_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(old_tg_schema), K(new_tg_schema));
  } else {
    const ObString& old_tg_name = old_tg_schema->get_tablegroup_name();
    const ObString& new_tg_name = new_tg_schema->get_tablegroup_name();
    const uint64_t old_tg_id = old_tg_schema->get_tablegroup_id();
    const uint64_t new_tg_id = new_tg_schema->get_tablegroup_id();

    if (old_tg_id != new_tg_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("old_tablesapce_id != new_tablegroup_id,"
                " never run here, bug.",
          K(old_tg_id),
          K(new_tg_id),
          K(ret));
    } else if (old_tg_name != new_tg_name) {
      LOG_INFO("tablegroup renamed, ",
          "old_tablegroup_name",
          to_cstring(old_tg_name),
          "new_tablegroup_name",
          to_cstring(new_tg_name));

      ObTablegroupSchemaHashWrapper tg_wrapper(old_tg_schema->get_tenant_id(), old_tg_schema->get_tablegroup_name());
      if (HASH_EXIST != (hash_ret = tablegroup_name_map_.erase(tg_wrapper))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to delete tablegroup from tablegroup name hashmap, ",
            "old_tg_name",
            to_cstring(old_tg_name),
            K(hash_ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::add_table_schema(ObTableSchema* table_schema, const bool is_replace)
{
  int ret = OB_SUCCESS;
  table_iterator table_iterator = NULL;
  ObTableSchema* replaced_table_schema = NULL;
  ObTableSchema* replaced_index_table_schema = NULL;

  if (OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid parameters, ", K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(table_infos_.replace(
            table_schema, table_iterator, compare_table_schema, equal_table_schema, replaced_table_schema))) {
      LOG_ERROR("failed to put table schema into table vector, ", K(ret));
    } else if (table_schema->is_index_table()) {
      if (OB_FAIL(index_table_infos_.replace(
              table_schema, table_iterator, compare_table_schema, equal_table_schema, replaced_index_table_schema))) {
        // can not alloc here  table_schema has been inserted into table_infos
        LOG_ERROR("failed to put table schema into index table vector, ",
            "index_table_id",
            table_schema->get_table_id(),
            K(ret));
      } else {
        SHARE_SCHEMA_LOG(DEBUG,
            "succ to put table schema into index table vector,",
            "index_table_id",
            table_schema->get_table_id(),
            K(ret));
      }
    }

    if (NULL != replaced_table_schema) {
      if (OB_FAIL(do_table_name_check(replaced_table_schema, table_schema))) {
        const ObString& replaced_table_name = replaced_table_schema->get_table_name_str();
        const ObString& new_table_name = table_schema->get_table_name_str();
        LOG_WARN("fail to do table name check,",
            "replaced_table_name",
            to_cstring(replaced_table_name),
            "new_table_name",
            to_cstring(new_table_name),
            K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(replaced_table_schema) && is_replace) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("replaced_table_schema should not be NULL, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int id_hash_ret = OB_SUCCESS;
      int name_hash_ret = OB_SUCCESS;
      if (HASH_INSERT_SUCC != (id_hash_ret = table_id_map_.set(table_schema->get_table_id(), table_schema, 1)) &&
          HASH_OVERWRITE_SUCC != id_hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to build table id hashmap, ", "table_id", table_schema->get_table_id());
      } else {
        if (table_schema->is_index_table()) {
          ObIndexSchemaHashWrapper index_name_wrapper(
              table_schema->get_tenant_id(), table_schema->get_database_id(), table_schema->get_table_name_str());
          if (HASH_INSERT_SUCC != (name_hash_ret = index_name_map_.set(index_name_wrapper, table_schema, 1)) &&
              HASH_OVERWRITE_SUCC != name_hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to build index name hashmap, ", "index_name", table_schema->get_table_name());
          }
        } else {
          ObNameCaseMode mode = OB_NAME_CASE_INVALID;
          if (OB_FAIL(get_tenant_name_case_mode(table_schema->get_tenant_id(), mode))) {
            LOG_ERROR("fail to get_tenant_name_case_mode", "tenant_id", table_schema->get_tenant_id(), K(ret));
          } else {
            ObTableSchemaHashWrapper table_name_wrapper(table_schema->get_tenant_id(),
                table_schema->get_database_id(),
                mode,
                table_schema->get_table_name_str());
            if (HASH_INSERT_SUCC != (name_hash_ret = table_name_map_.set(table_name_wrapper, table_schema, 1)) &&
                HASH_OVERWRITE_SUCC != name_hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to build table name hashmap, ", "table_name", table_schema->get_table_name());
            }
          }
        }
      }

      if (table_infos_.count() != table_id_map_.item_count() ||
          table_id_map_.item_count() != (table_name_map_.item_count() + index_name_map_.item_count())) {
        LOG_WARN("table info is non-consistent, try to rebuild it, ",
            "table_infos_count",
            table_infos_.count(),
            " table_id_map_item_count",
            table_id_map_.item_count(),
            "table_name_map_item_count",
            table_name_map_.item_count(),
            "index_name_map_item_count",
            index_name_map_.item_count(),
            K(id_hash_ret),
            K(name_hash_ret),
            "table_id",
            table_schema->get_table_id(),
            "table_name",
            table_schema->get_table_name());
        int build_ret = OB_SUCCESS;
        if (OB_SUCCESS != (build_ret = build_table_hashmap())) {
          LOG_ERROR("build_table_hashmap fail", K(build_ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::add_database_schema(ObDatabaseSchema* database_schema, const bool is_replace)
{
  int ret = OB_SUCCESS;
  database_iterator database_iterator = NULL;
  ObDatabaseSchema* replaced_database_schema = NULL;

  if (OB_ISNULL(database_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid parameters, database_schema is NULL.", K(ret));
  } else {
    if (OB_FAIL(database_infos_.replace(database_schema,
            database_iterator,
            compare_database_schema,
            equal_database_schema,
            replaced_database_schema))) {
      LOG_ERROR("failed to put database schema into database vector, ", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (NULL != replaced_database_schema) {
        if (OB_FAIL(do_database_name_check(replaced_database_schema, database_schema))) {
          const ObString& replaced_database_name = replaced_database_schema->get_database_name_str();
          const ObString& new_database_name = database_schema->get_database_name_str();
          LOG_WARN("fail to do database name check, ",
              "replaced_database_name",
              to_cstring(replaced_database_name),
              "new_database_name",
              to_cstring(new_database_name),
              K(ret));
        }
      } else if (OB_ISNULL(replaced_database_schema)) {
        if (is_replace) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("replaced_database_schema should not be NULL, ", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      int name_hash_ret = OB_SUCCESS;
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(get_tenant_name_case_mode(database_schema->get_tenant_id(), mode))) {
        LOG_WARN("fail to get tenant name case mode", "tenant_id", database_schema->get_tenant_id(), K(ret));
      } else {
        ObDatabaseSchemaHashWrapper database_wrapper(
            database_schema->get_tenant_id(), mode, database_schema->get_database_name_str());
        if (HASH_INSERT_SUCC != (name_hash_ret = database_name_map_.set(database_wrapper, database_schema, 1)) &&
            HASH_OVERWRITE_SUCC != name_hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("failed to build database name hashmap, ", "table_name", database_schema->get_database_name());
        }
      }

      if (database_infos_.count() != database_name_map_.item_count()) {
        LOG_WARN("database info is non-consistent, try to rebuild it,",
            " database_infos_count",
            database_infos_.count(),
            "database_name_map_item_count",
            database_name_map_.item_count(),
            K(name_hash_ret),
            "tenant_id",
            database_schema->get_tenant_id(),
            "database_id",
            database_schema->get_database_id(),
            "database_name",
            database_schema->get_database_name());
        int build_ret = OB_SUCCESS;
        if (OB_SUCCESS != (build_ret = build_database_hashmap())) {
          LOG_ERROR("failed to build database hashmap, ", K(build_ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::add_tablegroup_schema(ObTablegroupSchema* tablegroup_schema, const bool is_replace)
{
  int ret = OB_SUCCESS;
  tablegroup_iterator tablegroup_iterator = NULL;
  ObTablegroupSchema* replaced_tablegroup_schema = NULL;

  if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument, tablegroup_schema is NULL.", K(ret));
  } else {
    if (OB_FAIL(tablegroup_infos_.replace(tablegroup_schema,
            tablegroup_iterator,
            compare_tablegroup_schema,
            equal_tablegroup_schema,
            replaced_tablegroup_schema))) {
      LOG_ERROR("failed to put tablegroup schema into tablegroup vector, ", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (NULL != replaced_tablegroup_schema) {
        if (OB_FAIL(do_tablegroup_name_check(replaced_tablegroup_schema, tablegroup_schema))) {
          const ObString& replaced_tablegroup_name = replaced_tablegroup_schema->get_tablegroup_name();
          const ObString& new_tablegroup_name = tablegroup_schema->get_tablegroup_name();
          LOG_WARN("fail to do tablegroup name check, ",
              "replaced_tablegroup_name",
              to_cstring(replaced_tablegroup_name),
              "new_tablegroup_name",
              to_cstring(new_tablegroup_name),
              K(ret));
        }
      } else if (OB_ISNULL(replaced_tablegroup_schema)) {
        if (is_replace) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("replaced_tablegroup_schema should not be NULL, ", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      int name_hash_ret = OB_SUCCESS;
      ObTablegroupSchemaHashWrapper tablegroup_wrapper(
          tablegroup_schema->get_tenant_id(), tablegroup_schema->get_tablegroup_name());
      if (HASH_INSERT_SUCC != (name_hash_ret = tablegroup_name_map_.set(tablegroup_wrapper, tablegroup_schema, 1)) &&
          HASH_OVERWRITE_SUCC != name_hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to build tablegroup name hashmap, ",
            "table_name",
            to_cstring(tablegroup_schema->get_tablegroup_name()));
      }

      if (tablegroup_infos_.count() != tablegroup_name_map_.item_count()) {
        LOG_WARN("tablegroup info is non-consistent, try to rebuild it, ",
            "tablegroup_infos_count",
            tablegroup_infos_.count(),
            "tablegroup_map_item_count",
            tablegroup_name_map_.item_count(),
            K(name_hash_ret),
            "tablegroup_id",
            tablegroup_schema->get_tablegroup_id(),
            "tablegroup_name",
            to_cstring(tablegroup_schema->get_tablegroup_name()));
        int build_ret = OB_SUCCESS;
        if (OB_SUCCESS != (build_ret = build_tablegroup_hashmap())) {
          LOG_ERROR("failed to build tablegroup hashmap, ", K(build_ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::cons_table_to_index_relation(const hash::ObHashSet<uint64_t>* data_table_id_set /* = NULL*/)
{
  int ret = OB_SUCCESS;
  /**
   * if NULL == data_table_id_set, build all data tables
   */
  if (NULL == data_table_id_set) {
    if (OB_FAIL(cons_all_table_to_index_relation())) {
      LOG_WARN("failed to cons all table to index relation,", K(ret));
    }
  } else {
    if (OB_FAIL(cons_part_table_to_index_relation(*data_table_id_set))) {
      LOG_WARN("failed to cons part table to index relation,", K(ret));
    }
  }

  return ret;
}

int ObSchemaManager::get_can_read_index_array(
    uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool with_mv) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = get_table_schema(table_id);
  int64_t index_count = size;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get table schema for table  ", K(table_id));
  } else if (OB_FAIL(table_schema->get_index_tid_array(index_tid_array, index_count))) {
    LOG_WARN("fail to get index tid array:", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t can_read_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
      const ObTableSchema* index_tschema = get_table_schema(index_tid_array[i]);
      if (OB_ISNULL(index_tschema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cannot get table schema for table  ", "index_table_id", index_tid_array[i], K(ret));
      } else if (index_tschema->can_read_index() && index_tschema->is_index_visible()) {
        if (!(!with_mv && index_tschema->is_materialized_view())) {
          index_tid_array[can_read_count++] = index_tid_array[i];
        }
      }
    }
    size = can_read_count;
  }
  return ret;
}

int ObSchemaManager::get_can_write_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = get_table_schema(table_id);
  int64_t index_count = size;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get table schema for table ", K(table_id));
  } else if (OB_FAIL(table_schema->get_index_tid_array(index_tid_array, index_count))) {
    LOG_WARN("fail to get index tid array:", K(ret));
  }

  int64_t can_write_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
    const ObTableSchema* index_tschema = get_table_schema(index_tid_array[i]);
    if (OB_ISNULL(index_tschema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not get table schema for table ", "index_table_id", index_tid_array[i]);
    } else {
      ++can_write_count;
    }
  }
  size = can_write_count;
  return ret;
}

void ObSchemaManager::print_info(bool verbose /*= false*/) const
{
  int64_t database_num = database_infos_.count();
  int64_t tablegroup_num = tablegroup_infos_.count();
  int64_t table_num = table_infos_.count();
  int64_t all_alloc_size = allocator_.get_all_alloc_size();
  int64_t cur_alloc_size = allocator_.get_cur_alloc_size();

  LOG_WARN("dump schema manager info:",
      K(schema_version_),
      K(n_alloc_tables_),
      K(all_alloc_size),
      K(cur_alloc_size),
      K(database_num),
      K(tablegroup_num),
      K(table_num));

  if (verbose) {
    for (int i = 0; i < database_infos_.count(); ++i) {
      ObDatabaseSchema* database_schema = database_infos_.at(i);
      if (NULL != database_schema) {
        database_schema->print_info();
      }
    }
    for (int i = 0; i < tablegroup_infos_.count(); ++i) {
      ObTablegroupSchema* tablegroup_schema = tablegroup_infos_.at(i);
      if (NULL != tablegroup_schema) {
        tablegroup_schema->print_info();
      }
    }
    for (int i = 0; i < table_infos_.count(); ++i) {
      ObTableSchema* table_schema = table_infos_.at(i);
      if (NULL != table_schema) {
        table_schema->print_info(verbose);
      }
    }
    priv_mgr_.print_priv_infos();
  }
}

//------private functions
bool ObSchemaManager::compare_table_schema(const ObTableSchema* lhs, const ObTableSchema* rhs)
{
  return (lhs->get_tenant_table_id() < rhs->get_tenant_table_id());
}

bool ObSchemaManager::compare_tenant_table_id(const ObTableSchema* lhs, const ObTenantTableId& tenant_table_id)

{
  return NULL != lhs ? (lhs->get_tenant_table_id() < tenant_table_id) : false;
}

bool ObSchemaManager::compare_tenant_table_id_up(const ObTenantTableId& tenant_table_id, const ObTableSchema* lhs)

{
  return NULL != lhs ? (tenant_table_id < lhs->get_tenant_table_id()) : false;
}

bool ObSchemaManager::equal_table_schema(const ObTableSchema* lhs, const ObTableSchema* rhs)
{
  return NULL != lhs ? (lhs->get_tenant_table_id() == rhs->get_tenant_table_id()) : false;
}

bool ObSchemaManager::equal_tenant_table_id(const ObTableSchema* lhs, const ObTenantTableId& tenant_table_id)
{
  return NULL != lhs ? (lhs->get_tenant_table_id() == tenant_table_id) : false;
}

bool ObSchemaManager::compare_database_schema(const ObDatabaseSchema* lhs, const ObDatabaseSchema* rhs)
{
  return NULL != lhs ? (lhs->get_tenant_database_id() < rhs->get_tenant_database_id()) : false;
}

bool ObSchemaManager::compare_tenant_database_id(
    const ObDatabaseSchema* lhs, const ObTenantDatabaseId& tenant_database_id)

{
  return NULL != lhs ? (lhs->get_tenant_database_id() < tenant_database_id) : false;
}

bool ObSchemaManager::compare_tenant_database_id_up(
    const ObTenantDatabaseId& tenant_database_id, const ObDatabaseSchema* lhs)

{
  return (tenant_database_id < lhs->get_tenant_database_id());
}

bool ObSchemaManager::equal_database_schema(const ObDatabaseSchema* lhs, const ObDatabaseSchema* rhs)
{
  return (lhs->get_tenant_database_id() == rhs->get_tenant_database_id());
}

bool ObSchemaManager::equal_tenant_database_id(
    const ObDatabaseSchema* lhs, const ObTenantDatabaseId& tenant_database_id)
{
  return (lhs->get_tenant_database_id() == tenant_database_id);
}

bool ObSchemaManager::compare_tablegroup_schema(const ObTablegroupSchema* lhs, const ObTablegroupSchema* rhs)
{
  return (NULL != lhs && NULL != rhs) ? (lhs->get_tenant_tablegroup_id() < rhs->get_tenant_tablegroup_id()) : false;
}

bool ObSchemaManager::compare_tenant_tablegroup_id(
    const ObTablegroupSchema* lhs, const ObTenantTablegroupId& tenant_tablegroup_id)

{
  return NULL != lhs ? (lhs->get_tenant_tablegroup_id() < tenant_tablegroup_id) : false;
}

bool ObSchemaManager::compare_tenant_tablegroup_id_up(
    const ObTenantTablegroupId& tenant_tablegroup_id, const ObTablegroupSchema* lhs)

{
  return NULL != lhs ? (tenant_tablegroup_id < lhs->get_tenant_tablegroup_id()) : false;
}

bool ObSchemaManager::equal_tablegroup_schema(const ObTablegroupSchema* lhs, const ObTablegroupSchema* rhs)
{
  return (NULL != lhs && NULL != rhs) ? (lhs->get_tenant_tablegroup_id() == rhs->get_tenant_tablegroup_id()) : false;
}

bool ObSchemaManager::equal_tenant_tablegroup_id(
    const ObTablegroupSchema* lhs, const ObTenantTablegroupId& tenant_tablegroup_id)
{
  return NULL != lhs ? (lhs->get_tenant_tablegroup_id() == tenant_tablegroup_id) : false;
}

int ObSchemaManager::check_database_exist(uint64_t database_id, bool& is_exist) const
{
  // TODO  low efficiency
  int ret = OB_SUCCESS;
  is_exist = false;
  const ObDatabaseSchema* database_schema = NULL;
  if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ", K(database_id), K(ret));
  } else {
    bool is_stop = false;
    for (const_database_iterator database_schema_iter = database_begin();
         OB_SUCCESS == ret && database_schema_iter != database_end() && !is_stop;
         database_schema_iter++) {
      database_schema = NULL;
      if (NULL == (database_schema = *database_schema_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid got database schema pointer,  ", K(database_schema), K(ret));
      } else if (database_id == database_schema->get_database_id()) {
        is_exist = true;
        is_stop = true;
      }
    }
  }
  return ret;
}

int ObSchemaManager::check_tablegroup_exist(uint64_t tablegroup_id, bool& is_exist) const
{
  // TODO  low efficiency
  int ret = OB_SUCCESS;
  is_exist = false;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid , ", K(tablegroup_id), K(ret));
  } else {
    bool is_stop = false;
    for (const_tablegroup_iterator tablegroup_schema_iter = tablegroup_begin();
         OB_SUCCESS == ret && tablegroup_schema_iter != tablegroup_end() && !is_stop;
         tablegroup_schema_iter++) {
      tablegroup_schema = NULL;
      if (NULL == (tablegroup_schema = *tablegroup_schema_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid got tablegroup schema pointer,  ", K(tablegroup_schema), K(ret));
      } else if (tablegroup_id == tablegroup_schema->get_tablegroup_id()) {
        is_exist = true;
        is_stop = true;
      }
    }
  }
  return ret;
}

int ObSchemaManager::del_database(const ObTenantDatabaseId database_to_delete)
{
  int ret = OB_SUCCESS;
  ObDatabaseSchema* schema_to_del = NULL;

  if (OB_FAIL(database_infos_.remove_if(
          database_to_delete, compare_tenant_database_id, equal_tenant_database_id, schema_to_del))) {
    LOG_WARN("fail to remove database, ",
        "tenant_id",
        database_to_delete.tenant_id_,
        "database_id",
        database_to_delete.database_id_,
        K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed database schema return NULL,  ", K(schema_to_del));
  }

  int hash_ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(get_tenant_name_case_mode(schema_to_del->get_tenant_id(), mode))) {
      LOG_WARN("fail to get tenant name case mode", "tenant_id", schema_to_del->get_tenant_id(), K(ret));
    } else {
      ObDatabaseSchemaHashWrapper schema_wrapper(
          schema_to_del->get_tenant_id(), mode, schema_to_del->get_database_name_str());
      if (HASH_EXIST != (hash_ret = database_name_map_.erase(schema_wrapper))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed delete database from database name hashmap, ",
            "database_name",
            to_cstring(schema_to_del->get_database_name_str()),
            K(hash_ret));
      }
    }
  }

  if (database_infos_.count() != database_name_map_.item_count()) {
    LOG_WARN("database info is non-consistent, try to rebuild it, ",
        "database_infos_count",
        database_infos_.count(),
        "database_name_map_item_count",
        database_name_map_.item_count());
    int build_ret = OB_SUCCESS;
    if (OB_SUCCESS != (build_ret = build_database_hashmap())) {
      LOG_WARN("build_database_hashmap fail", K(ret));
    }
  }

  return ret;
}

int ObSchemaManager::del_tablegroup(const ObTenantTablegroupId tablegroup_to_delete)
{
  int ret = OB_SUCCESS;
  ObTablegroupSchema* schema_to_del = NULL;

  if (OB_FAIL(tablegroup_infos_.remove_if(
          tablegroup_to_delete, compare_tenant_tablegroup_id, equal_tenant_tablegroup_id, schema_to_del))) {
    LOG_WARN("fail to remove tablegroup, ",
        "tenant_id",
        tablegroup_to_delete.tenant_id_,
        "tablegroup_id",
        tablegroup_to_delete.tablegroup_id_,
        K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed tablegroup schema return NULL, ", K(schema_to_del));
  }

  int hash_ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ObTablegroupSchemaHashWrapper tablegroup_schema_wrapper(
        schema_to_del->get_tenant_id(), schema_to_del->get_tablegroup_name());
    if (HASH_EXIST != (hash_ret = tablegroup_name_map_.erase(tablegroup_schema_wrapper))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete tablegroup from tablegroup name hashmap,",
          "tablegroup_name",
          to_cstring(schema_to_del->get_tablegroup_name()),
          K(hash_ret));
    }
  }

  if (tablegroup_infos_.count() != tablegroup_name_map_.item_count()) {
    LOG_WARN("tablegroup info is non-consistent, try to rebuild it,",
        "tablegroup_infos_count",
        tablegroup_infos_.count(),
        "tablegroup_name_map_item_count",
        tablegroup_name_map_.item_count());
    int build_ret = OB_SUCCESS;
    if (OB_SUCCESS != (build_ret = build_database_hashmap())) {
      LOG_WARN("build_database_hashmap fail", K(ret));
    }
  }
  return ret;
}

int ObSchemaManager::del_table(const ObTenantTableId table_to_delete)
{
  int ret = OB_SUCCESS;
  ObTableSchema* schema_to_del = NULL;
  ObTableSchema* index_schema_to_del = NULL;

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(table_infos_.remove_if(
                 table_to_delete, compare_tenant_table_id, equal_tenant_table_id, schema_to_del))) {
    LOG_WARN("failed to remove table schema, ",
        "tenant_id",
        table_to_delete.tenant_id_,
        "table_id",
        table_to_delete.table_id_,
        K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed table schema return NULL, ",
        "tenant_id",
        table_to_delete.tenant_id_,
        "table_id",
        table_to_delete.table_id_,
        K(ret));
  } else if (schema_to_del->is_index_table()) {
    if (OB_SUCCESS != (ret = index_table_infos_.remove_if(
                           table_to_delete, compare_tenant_table_id, equal_tenant_table_id, index_schema_to_del))) {
      LOG_WARN("failed to remove table schema, ",
          "tenant_id",
          table_to_delete.tenant_id_,
          "table_id",
          table_to_delete.table_id_,
          K(ret));
    } else if (OB_ISNULL(index_schema_to_del)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("removed index table schema return NULL, ",
          "tenant_id",
          table_to_delete.tenant_id_,
          "table_id",
          table_to_delete.table_id_,
          K(ret));
    }
  }

  int hash_ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (HASH_EXIST != (hash_ret = table_id_map_.erase(schema_to_del->get_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete table from table id hashmap, ", "table_id", schema_to_del->get_table_id(), K(hash_ret));
    } else {
      if (schema_to_del->is_index_table()) {
        ObIndexSchemaHashWrapper index_schema_wrapper(
            schema_to_del->get_tenant_id(), schema_to_del->get_database_id(), schema_to_del->get_table_name_str());
        if (HASH_EXIST != (hash_ret = index_name_map_.erase(index_schema_wrapper))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed delete index from index name hashmap, ",
              "index_name",
              schema_to_del->get_table_name(),
              K(hash_ret));
        }
      } else {
        ObNameCaseMode mode = OB_NAME_CASE_INVALID;
        if (OB_FAIL(get_tenant_name_case_mode(schema_to_del->get_tenant_id(), mode))) {
          LOG_ERROR("fail to get_tenant_name_case_mode", "tenant_id", schema_to_del->get_tenant_id(), K(ret));
        } else {
          ObTableSchemaHashWrapper table_schema_wrapper(schema_to_del->get_tenant_id(),
              schema_to_del->get_database_id(),
              mode,
              schema_to_del->get_table_name_str());
          if (HASH_EXIST != (hash_ret = table_name_map_.erase(table_schema_wrapper))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed delete table from table name hashmap, ",
                "table_name",
                schema_to_del->get_table_name(),
                K(hash_ret));
          }
        }
      }
    }
  }

  if (table_infos_.count() != table_id_map_.item_count() ||
      table_id_map_.item_count() != (table_name_map_.item_count() + index_name_map_.item_count())) {
    LOG_WARN("table info is non-consistent, try to rebuild it, ",
        "table_infos_count",
        table_infos_.count(),
        "table_id_map_item_count",
        table_id_map_.item_count(),
        "table_name_map_item_count",
        table_name_map_.item_count(),
        "index_name_map_item_count",
        index_name_map_.item_count());
    int build_ret = OB_SUCCESS;
    if (OB_SUCCESS != (build_ret = build_table_hashmap())) {
      LOG_ERROR("failed to build table hashmap, ", K(build_ret));
    }
  }
  return ret;
}

int ObSchemaManager::build_table_hashmap()
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;

  table_id_map_.clear();
  table_name_map_.clear();
  index_name_map_.clear();
  bool is_stop = false;
  for (const_table_iterator it = table_begin(); OB_SUCC(ret) && it != table_end() && !is_stop; ++it) {
    table_schema = *it;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table_schema should not be null");
    } else if (HASH_INSERT_SUCC !=
               table_id_map_.set(table_schema->get_table_id(), const_cast<ObTableSchema*>(table_schema))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed build table id hashmap, ", "table_id", table_schema->get_table_id());
    } else {
      if (table_schema->is_index_table()) {
        ObIndexSchemaHashWrapper index_wrapper(
            table_schema->get_tenant_id(), table_schema->get_database_id(), table_schema->get_table_name_str());
        if (HASH_INSERT_SUCC != index_name_map_.set(index_wrapper, const_cast<ObTableSchema*>(table_schema))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed build index name hashmap, ", "index_name", table_schema->get_table_name(), K(ret));
        }
      } else {
        ObNameCaseMode mode = OB_NAME_CASE_INVALID;
        if (OB_FAIL(get_tenant_name_case_mode(table_schema->get_tenant_id(), mode))) {
          LOG_ERROR("fail to get_tenant_name_case_mode", "tenant_id", table_schema->get_tenant_id(), K(ret));
        } else {
          ObTableSchemaHashWrapper table_wrapper(
              table_schema->get_tenant_id(), table_schema->get_database_id(), mode, table_schema->get_table_name_str());
          int ret_tmp = OB_SUCCESS;
          ret_tmp = table_name_map_.set(table_wrapper, const_cast<ObTableSchema*>(table_schema));
          if (HASH_INSERT_SUCC != ret_tmp) {
            LOG_WARN("hashmap error, ", K(ret_tmp), K(lbt()), K(table_schema));
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed build table name hashmap, ", "table_name", table_schema->get_table_name(), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::build_database_hashmap()
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* database_schema = NULL;
  database_name_map_.clear();

  for (const_database_iterator it = database_begin(); OB_SUCC(ret) && it != database_end(); ++it) {
    if (NULL == (database_schema = *it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("database_schema should not be null, ", K(ret));
    } else {
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(get_tenant_name_case_mode(database_schema->get_tenant_id(), mode))) {
        LOG_WARN("fail to get tenant name case mode", "tenant_id", database_schema->get_tenant_id(), K(ret));
      } else {
        ObDatabaseSchemaHashWrapper database_wrapper(
            database_schema->get_tenant_id(), mode, database_schema->get_database_name_str());
        if (HASH_INSERT_SUCC !=
            database_name_map_.set(database_wrapper, const_cast<ObDatabaseSchema*>(database_schema))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed build database name hashmap,",
              "tenant_id",
              database_schema->get_tenant_id(),
              "database_name",
              database_schema->get_database_name());
        }
      }
    }
  }  // end for
  return ret;
}

int ObSchemaManager::build_tablegroup_hashmap()
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  tablegroup_name_map_.clear();

  for (const_tablegroup_iterator it = tablegroup_begin(); OB_SUCCESS == ret && it != tablegroup_end(); it++) {
    if (NULL == (tablegroup_schema = *it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablegroup_schema should not be null, ", K(ret));
    } else {
      ObTablegroupSchemaHashWrapper tablegroup_wrapper(
          tablegroup_schema->get_tenant_id(), tablegroup_schema->get_tablegroup_name());
      if (HASH_INSERT_SUCC !=
          tablegroup_name_map_.set(tablegroup_wrapper, const_cast<ObTablegroupSchema*>(tablegroup_schema))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed build tablegroup name hashmap, ",
            "tenant_id",
            tablegroup_schema->get_tenant_id(),
            "tablegroup_name",
            tablegroup_schema->get_tablegroup_name());
      }
    }
  }  // end for
  return ret;
}

int ObSchemaManager::cons_all_table_to_index_relation()
{
  int ret = OB_SUCCESS;
  const ObTableSchema* data_table_schema = NULL;
  const ObTableSchema* index_table_schema = NULL;

  for (const_table_iterator index_table_schema_iter = index_table_infos_.begin();
       OB_SUCCESS == ret && index_table_schema_iter != index_table_infos_.end();
       ++index_table_schema_iter) {
    if (NULL == (index_table_schema = *index_table_schema_iter) || !index_table_schema->is_index_table()) {
      LOG_WARN("invalid ",
          K(index_table_schema),
          "table_type",
          (NULL != index_table_schema) ? index_table_schema->get_table_type() : -1);
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (data_table_schema = get_table_schema(index_table_schema->get_data_table_id()))) {
      LOG_WARN("failed to get data table schema, ",
          "data_table_id",
          index_table_schema->get_data_table_id(),
          "index_table_id",
          index_table_schema->get_table_id());
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(
                   const_cast<ObTableSchema*>(data_table_schema)->add_index_tid(index_table_schema->get_table_id()))) {
      LOG_WARN("fail to add index to table schema:", K(ret));
    }
  }

  return ret;
}

int ObSchemaManager::cons_part_table_to_index_relation(const hash::ObHashSet<uint64_t>& data_table_id_set)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* data_table_schema = NULL;
  const ObTableSchema* index_table_schema = NULL;

  for (hash::ObHashSet<uint64_t>::const_iterator it = data_table_id_set.begin();
       OB_SUCCESS == ret && it != data_table_id_set.end();
       ++it) {
    if (NULL == (data_table_schema = get_table_schema(it->first))) {
      LOG_WARN("failed to get table schema, ", "table_id", it->first);
      ret = OB_ERR_UNEXPECTED;
    } else if (data_table_schema->is_user_table()) {
      const_cast<ObTableSchema*>(data_table_schema)->reset_index_tid_array();
    }
  }

  for (const_table_iterator index_table_schema_iter = index_table_infos_.begin();
       OB_SUCCESS == ret && index_table_schema_iter != index_table_infos_.end();
       ++index_table_schema_iter) {
    if (NULL == (index_table_schema = *index_table_schema_iter) || !index_table_schema->is_index_table()) {
      LOG_WARN("invalid ",
          K(index_table_schema),
          "table_type",
          (NULL != index_table_schema) ? index_table_schema->get_table_type() : -1);
      ret = OB_ERR_UNEXPECTED;
    } else if (HASH_EXIST == data_table_id_set.exist(index_table_schema->get_data_table_id())) {
      if (NULL == (data_table_schema = get_table_schema(index_table_schema->get_data_table_id()))) {
        LOG_WARN("failed to get data table schema, ", "table_id", index_table_schema->get_data_table_id());
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(const_cast<ObTableSchema*>(data_table_schema)
                             ->add_index_tid(index_table_schema->get_table_id()))) {
        LOG_WARN("fail to add index to table schema:", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaManager::check_table_exist(
    const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_ERROR("fail to get_tenant_name_case_mode", K(tenant_id), K(ret));
  } else {
    ObTableSchemaHashWrapper table_wrapper(tenant_id, database_id, mode, table_name);
    ObTableSchema* table_schema = NULL;
    if (HASH_EXIST != table_name_map_.get(table_wrapper, table_schema)) {
      table_schema = NULL;
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get table schema return NULL", K(ret), K(tenant_id), K(database_id), K(table_name));
    } else {
      if (!table_schema->is_index_table()) {
        exist = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table cannot be index", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::check_index_exist(
    const uint64_t tenant_id, const uint64_t database_id, const common::ObString& index_name, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;

  ObIndexSchemaHashWrapper index_wrapper(tenant_id, database_id, index_name);
  ObTableSchema* index_schema = NULL;
  if (HASH_EXIST != index_name_map_.get(index_wrapper, index_schema)) {
    index_schema = NULL;
    exist = false;
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get table schema return NULL", K(ret), K(tenant_id), K(database_id), K(index_name));
  } else {
    if (index_schema->is_index_table()) {
      exist = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index_schema is not a index table", K(ret));
    }
  }
  return ret;
}

int ObSchemaManager::get_database_id(uint64_t tenant_id, const ObString& database_name, uint64_t& database_id) const
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;

  if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), "database_name", to_cstring(database_name), K(ret));
  } else if ((database_name.length() == strlen(OB_SYS_DATABASE_NAME)) &&
             (0 == STRCASECMP(database_name.ptr(), OB_SYS_DATABASE_NAME))) {
    database_id = combine_id(tenant_id, OB_SYS_DATABASE_ID);
  } else {
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
      LOG_WARN("fail to get tenant name case mode", K(tenant_id), K(ret));
    } else {
      ObDatabaseSchemaHashWrapper database_wrapper(tenant_id, mode, database_name);
      ObDatabaseSchema* database_schema = NULL;
      if (HASH_EXIST != database_name_map_.get(database_wrapper, database_schema)) {
        database_schema = NULL;
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("failed to get database schema, ", K(tenant_id), "database_name", to_cstring(database_name), K(ret));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR(
            "get tablegroup schema return NULL, ", K(tenant_id), "database_name", to_cstring(database_name), K(ret));
      } else {
        database_id = database_schema->get_database_id();
      }
    }
  }

  return ret;
}

int ObSchemaManager::get_database_name(
    const uint64_t tenant_id, const uint64_t database_id, ObString& database_name) const
{
  int ret = OB_ERR_BAD_DATABASE;
  if (database_id == combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID)) {
    database_name.assign_ptr(OB_SYS_DATABASE_NAME, static_cast<ObString::obstr_size_t>(strlen(OB_SYS_DATABASE_NAME)));
    ret = OB_SUCCESS;
  } else {
    const ObDatabaseSchema* ds = get_database_schema(tenant_id, database_id);
    if (OB_ISNULL(ds)) {
      SQL_RESV_LOG(WARN, "database not exist", K(database_id));
      ret = OB_ERR_BAD_DATABASE;
    } else {
      ret = OB_SUCCESS;
      database_name = ds->get_database_name_str();
    }
  }
  return ret;
}

int ObSchemaManager::get_tablegroup_id(
    uint64_t tenant_id, const ObString& tablegroup_name, uint64_t& tablegroup_id) const
{
  int ret = OB_SUCCESS;
  tablegroup_id = OB_INVALID_ID;

  if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(tenant_id), "tablegroup_name", to_cstring(tablegroup_name), K(ret));
  } else {
    const ObTablegroupSchemaHashWrapper tablegroup_wrapper(tenant_id, tablegroup_name);
    ObTablegroupSchema* tablegroup_schema = NULL;
    if (HASH_EXIST != tablegroup_name_map_.get(tablegroup_wrapper, tablegroup_schema)) {
      tablegroup_schema = NULL;
      ret = OB_TABLEGROUP_NOT_EXIST;
      LOG_WARN(
          "failed to get tablegroup schema, ", K(tenant_id), "tablegroup_name", to_cstring(tablegroup_name), K(ret));
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR(
          "get tablegroup schema return NULL, ", K(tenant_id), "tablegroup_name", to_cstring(tablegroup_name), K(ret));
    } else {
      tablegroup_id = tablegroup_schema->get_tablegroup_id();
    }
  }

  return ret;
}

int ObSchemaManager::get_tenant_id(const common::ObString& tenant_name, uint64_t& tenant_id) const
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  if (OB_FAIL(priv_mgr_.get_tenant_id(tenant_name, tenant_id))) {
    LOG_WARN("get_tenant_id failed", K(ret), K(tenant_name));
  }
  return ret;
}

int ObSchemaManager::add_tenant_info(const ObTenantSchema& tenant_schema)
{
  return priv_mgr_.add_new_tenant_info(tenant_schema);
}

const ObTenantSchema* ObSchemaManager::get_tenant_info(const uint64_t tenant_id) const
{
  return priv_mgr_.get_tenant_info(tenant_id);
}

const ObTenantSchema* ObSchemaManager::get_tenant_info(const common::ObString& tenant_name) const
{
  return priv_mgr_.get_tenant_info(tenant_name);
}

int ObSchemaManager::get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids) const
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  for (ObPrivManager::ConstTenantInfoIter tenant_iter = priv_mgr_.tenant_info_begin();
       OB_SUCCESS == ret && tenant_iter != priv_mgr_.tenant_info_end();
       ++tenant_iter) {
    ObTenantSchema* tenant_schema = *tenant_iter;
    if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_schema is nnull", K(ret));
    } else if (OB_FAIL(tenant_ids.push_back(tenant_schema->get_tenant_id()))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

const ObUserInfo* ObSchemaManager::get_user_info(const ObTenantUserId& user_key) const
{
  return priv_mgr_.get_user_info(user_key);
}

const ObDBPriv* ObSchemaManager::get_db_priv(const ObOriginalDBKey& db_priv_key) const
{
  return priv_mgr_.get_db_priv(db_priv_key);
}

const ObTablePriv* ObSchemaManager::get_table_priv(const ObTablePrivSortKey& table_priv_key) const
{
  return priv_mgr_.get_table_priv(table_priv_key);
}

int ObSchemaManager::check_user_access(const ObUserLoginInfo& login_info, ObSessionPrivInfo& session_priv) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(priv_mgr_.check_user_access(login_info, session_priv))) {
    LOG_WARN("No privilege to login", K(login_info), K(ret));
  } else if (!login_info.db_.empty()) {
    uint64_t database_id = OB_INVALID_ID;
    bool is_exist = false;
    if (OB_FAIL(check_database_exist(session_priv.tenant_id_, login_info.db_, database_id, is_exist))) {
      LOG_WARN("Check database exist failed", "tenant_id", session_priv.tenant_id_, "database", login_info.db_, K(ret));
    } else if (!is_exist) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("Database not exist", K(ret));
    } else {
    }
  }
  return ret;
}

int ObSchemaManager::check_db_access(ObSessionPrivInfo& s_priv, const ObString& database_name) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  uint64_t database_id = OB_INVALID_ID;
  if (OB_FAIL(check_database_exist(s_priv.tenant_id_, database_name, database_id, is_exist))) {
    LOG_WARN("fail to check database exist.", K(ret), K(s_priv.tenant_id_), K(database_name));
  } else if (is_exist) {
    if (OB_FAIL(priv_mgr_.check_db_access(s_priv, database_name, s_priv.db_priv_set_))) {
      LOG_WARN("fail to check db access", K(database_name), K(ret));
    }
  } else {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database not exist", K(ret), K(database_name), K(s_priv));
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
  }
  return ret;
}

int ObSchemaManager::check_priv(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs) const
{
  return priv_mgr_.check_priv(session_priv, stmt_need_privs);
}

int ObSchemaManager::check_priv_or(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs) const
{
  return priv_mgr_.check_priv_or(session_priv, stmt_need_privs);
}

int ObSchemaManager::check_table_show(
    const ObSessionPrivInfo& s_priv, const ObString& db, const ObString& table, bool& allow_show) const
{
  return priv_mgr_.check_table_show(s_priv, db, table, allow_show);
}

int ObSchemaManager::verify_db_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv) const
{
  int ret = OB_SUCCESS;
  const ObString& db_name = need_priv.db_;
  const ObDatabaseSchema* db_schema = get_database_schema(tenant_id, db_name);
  if (NULL != db_schema) {
    if (db_schema->is_read_only()) {
      ret = OB_ERR_DB_READ_ONLY;
      LOG_USER_ERROR(OB_ERR_DB_READ_ONLY, db_name.length(), db_name.ptr());
      SHARE_SCHEMA_LOG(
          WARN, "database is read only, can't not execute this statment", K(need_priv), K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObSchemaManager::verify_table_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv) const
{
  int ret = OB_SUCCESS;
  const ObString& db_name = need_priv.db_;
  const ObString& table_name = need_priv.table_;
  const ObTableSchema* table_schema = get_table_schema(tenant_id, db_name, table_name);
  if (NULL != table_schema) {
    if (table_schema->is_read_only()) {
      ret = OB_ERR_TABLE_READ_ONLY;
      LOG_USER_ERROR(OB_ERR_TABLE_READ_ONLY, db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
      SHARE_SCHEMA_LOG(WARN, "table is read only, can't not execute this statment", K(need_priv), K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObSchemaManager::verify_read_only(const uint64_t tenant_id, const ObStmtNeedPrivs& stmt_need_privs) const
{
  int ret = OB_SUCCESS;
  const ObStmtNeedPrivs::NeedPrivs& need_privs = stmt_need_privs.need_privs_;
  for (int i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
    const ObNeedPriv& need_priv = need_privs.at(i);
    switch (need_priv.priv_level_) {
      case OB_PRIV_USER_LEVEL: {
        // we do not check user priv level only check table and db
        break;
      }
      case OB_PRIV_DB_LEVEL: {
        if (OB_FAIL(verify_db_read_only(tenant_id, need_priv))) {
          SHARE_SCHEMA_LOG(WARN, "database is read only, can't not execute this statement");
        }
        break;
      }
      case OB_PRIV_TABLE_LEVEL: {
        if (OB_FAIL(verify_db_read_only(tenant_id, need_priv))) {
          SHARE_SCHEMA_LOG(WARN, "db is read only, can't not execute this statement");
        } else if (OB_FAIL(verify_table_read_only(tenant_id, need_priv))) {
          SHARE_SCHEMA_LOG(WARN, "table is read only, can't not execute this statement");
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "unknown privilege level", K(need_priv), K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaManager::print_table_definition(
    uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info) const
{
  // TODO(: refactor this function):consider index_position in
  int ret = OB_SUCCESS;

  const ObTableSchema* table_schema = NULL;
  if (NULL == (table_schema = get_table_schema(table_id))) {
    ret = OB_TABLE_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(table_id));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CREATE TABLE `%s` (\n", table_schema->get_table_name()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
  } else {
    if (OB_FAIL(print_table_definition_columns(*table_schema, buf, buf_len, pos, tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print columns", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_rowkeys(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print rowkeys", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_indexes(*table_schema, buf, buf_len, pos, true))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indexes", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_indexes(*table_schema, buf, buf_len, pos, false))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indexes", K(ret), K(*table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    } else if (OB_FAIL(print_table_definition_table_options(*table_schema, buf, buf_len, pos, false))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table options", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_partition_options(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print partition options", K(ret), K(*table_schema));
    }
  }
  return ret;
}

int ObSchemaManager::print_table_definition_columns(const ObTableSchema& table_schema, char* buf,
    const int64_t& buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator it_begin = table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = table_schema.column_end();
  bool is_first_col = true;
  ObColumnSchemaV2* col = NULL;
  for (; OB_SUCCESS == ret && it_begin != it_end; it_begin++) {
    col = *it_begin;
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col is null", K(ret));
    } else if (col->is_shadow_column()) {
      // do nothing
    } else if (col->is_hidden()) {
      // do nothing
    } else {
      SHARE_SCHEMA_LOG(DEBUG, "print_table_definition_columns", KPC(col));
      if (true == is_first_col) {
        is_first_col = false;
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print enter", K(ret));
        }
      }
      const ObLengthSemantics default_length_semantics = LS_INVALIED;  // force print full length_semantics
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "  `%s` ", col->get_column_name()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column", K(ret), K(*col));
      } else if (OB_FAIL(ob_sql_type_str(col->get_meta_type(),
                     col->get_accuracy(),
                     col->get_extended_type_info(),
                     default_length_semantics,
                     buf,
                     buf_len,
                     pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get data type str", K(ret), K(col->get_data_type()));
      }
      // zerofill, only for int, float, decimal
      if (OB_SUCCESS == ret && col->is_zero_fill()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " zerofill"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print zerofill", K(ret), K(*col));
        }
      }
      // string collation, e.g. CHARACTER SET latin1 COLLATE latin1_bin
      if (OB_SUCCESS == ret && ob_is_string_type(col->get_data_type())) {
        if (col->get_charset_type() == CHARSET_BINARY) {
          // nothing to do
        } else {
          if (col->get_charset_type() == table_schema.get_charset_type() &&
              col->get_collation_type() == table_schema.get_collation_type()) {
            // nothing to do
          } else {
            if (CHARSET_INVALID != col->get_charset_type() && CHARSET_BINARY != col->get_charset_type()) {
              if (OB_FAIL(databuff_printf(
                      buf, buf_len, pos, " CHARACTER SET %s", ObCharset::charset_name(col->get_charset_type())))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print character set", K(ret), K(*col));
              }
            }
          }
          if (OB_SUCCESS == ret && !ObCharset::is_default_collation(col->get_collation_type()) &&
              CS_TYPE_INVALID != col->get_collation_type() && CS_TYPE_BINARY != col->get_collation_type()) {
            if (OB_FAIL(databuff_printf(
                    buf, buf_len, pos, " COLLATE %s", ObCharset::collation_name(col->get_collation_type())))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print collate", K(ret), K(*col));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (!col->is_nullable()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOT NULL"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
          }
        } else if (!share::is_oracle_mode() && ObTimestampType == col->get_data_type()) {
          // only timestamp need to print null;
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NULL"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print NULL", K(ret));
          }
        }
      }
      // if column is not permit null and default value does not specify , don't  display DEFAULT NULL
      if (OB_SUCC(ret)) {
        if (share::is_oracle_mode()) {
          if (!(ObNullType == col->get_cur_default_value().get_type() && !col->is_nullable()) &&
              col->is_default_expr_v2_column()) {
            ObString default_value = col->get_cur_default_value().get_string();
            if (OB_FAIL(
                    databuff_printf(buf, buf_len, pos, " DEFAULT %.*s", default_value.length(), default_value.ptr()))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(default_value), K(ret));
            }
          }
        } else if (!(ObNullType == col->get_cur_default_value().get_type() && !col->is_nullable()) &&
                   !col->is_autoincrement()) {
          if (IS_DEFAULT_NOW_OBJ(col->get_cur_default_value())) {
            int16_t scale = col->get_data_scale();
            if (0 == scale) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT %s", N_UPPERCASE_CUR_TIMESTAMP))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT now()", K(ret));
              }
            } else {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT %s(%d)", N_UPPERCASE_CUR_TIMESTAMP, scale))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT now()", K(ret));
              }
            }
          } else {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT", K(ret));
            } else {
              ObObj default_value = col->get_cur_default_value();
              default_value.set_scale(col->get_data_scale());
              if (OB_FAIL(default_value.print_varchar_literal(buf, buf_len, pos, tz_info))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCCESS == ret && col->is_on_update_current_timestamp()) {
        int16_t scale = col->get_data_scale();
        if (0 == scale) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " %s", N_UPDATE_CURRENT_TIMESTAMP))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print on update current_tiemstamp", K(ret));
          }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " %s(%d)", N_UPDATE_CURRENT_TIMESTAMP, scale))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print on update current_tiemstamp", K(ret));
          }
        }
      }
      if (OB_SUCCESS == ret && col->is_autoincrement()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " AUTO_INCREMENT"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print auto_increment", K(ret));
        }
      }
      if (OB_SUCCESS == ret && 0 < strlen(col->get_comment())) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " COMMENT '%s'", col->get_comment()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print comment", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::print_table_definition_indexes(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos, bool is_unique_index) const
{
  int ret = OB_SUCCESS;
  uint64_t index_tid_array[OB_MAX_INDEX_PER_TABLE] = {OB_INVALID_ID};
  int64_t index_size = OB_MAX_INDEX_PER_TABLE;
  if (OB_FAIL(table_schema.get_index_tid_array(index_tid_array, index_size))) {
    SHARE_SCHEMA_LOG(WARN, "get index tid array failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_size; i++) {
    const ObTableSchema* index_schema = get_table_schema(index_tid_array[i]);
    if (NULL == index_schema) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(ERROR, "invalid index table id", "index_table_id", index_tid_array[i]);
    } else if (OB_SUCC(ret)) {
      ObString index_name;
      ObStringBuf allocator;

      //  get the original short index name
      if (OB_FAIL(ObTableSchema::get_index_name(allocator,
              table_schema.get_table_id(),
              ObString::make_string(index_schema->get_table_name()),
              index_name))) {
        SHARE_SCHEMA_LOG(WARN, "get index table name failed");
      } else if ((is_unique_index && index_schema->is_unique_index()) ||
                 (!is_unique_index && !index_schema->is_unique_index())) {
        if (index_schema->is_unique_index()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  UNIQUE KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print UNIQUE KEY", K(ret));
          }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print KEY", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%.*s` (", index_name.length(), index_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print index name", K(ret), K(index_name));
        } else {
          // index columns contain rowkeys of base table, but no need to show them.
          const int64_t index_column_num = index_schema->get_index_column_number();
          const ObRowkeyInfo& index_rowkey_info = index_schema->get_rowkey_info();
          int64_t rowkey_count = index_rowkey_info.get_size();
          ObColumnSchemaV2 last_col;
          bool is_valid_col = false;
          for (int64_t k = 0; OB_SUCC(ret) && k < index_column_num; k++) {
            const ObRowkeyColumn* rowkey_column = index_rowkey_info.get_column(k);
            const ObColumnSchemaV2* col = NULL;
            if (NULL == rowkey_column) {
              ret = OB_SCHEMA_ERROR;
              SHARE_SCHEMA_LOG(WARN, "fail to get rowkey column", K(ret));
            } else if (NULL == (col = get_column_schema(index_tid_array[i], rowkey_column->column_id_))) {
              ret = OB_SCHEMA_ERROR;
              SHARE_SCHEMA_LOG(WARN, "fail to get column schema", K(ret));
            } else if (!col->is_shadow_column()) {
              if (is_valid_col) {
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%s`, ", last_col.get_column_name()))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(last_col));
                }
              }
              last_col = *col;
              is_valid_col = true;
            } else {
              is_valid_col = false;
            }
          }

          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%s`)", last_col.get_column_name()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(last_col));
          }

          // show storing columns in index
          if (OB_SUCC(ret)) {
            int64_t column_count = index_schema->get_column_count();
            if (column_count > rowkey_count) {
              bool first_storing_column = true;
              for (ObTableSchema::const_column_iterator row_col = index_schema->column_begin();
                   OB_SUCCESS == ret && NULL != row_col && row_col != index_schema->column_end();
                   row_col++) {
                int64_t k = 0;
                const ObRowkeyInfo& tab_rowkey_info = table_schema.get_rowkey_info();
                int64_t tab_rowkey_count = tab_rowkey_info.get_size();
                for (; k < tab_rowkey_count; k++) {
                  const ObRowkeyColumn* rowkey_column = tab_rowkey_info.get_column(k);
                  if (NULL != *row_col && rowkey_column->column_id_ == (*row_col)->get_column_id()) {
                    break;
                  }
                }
                if (k == tab_rowkey_count && NULL != *row_col && !(*row_col)->get_rowkey_position()) {
                  if (first_storing_column) {
                    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " STORING ("))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print STORING(", K(ret));
                    }
                    first_storing_column = false;
                  }
                  if (OB_SUCC(ret)) {
                    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%s`, ", (*row_col)->get_column_name()))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K((*row_col)->get_column_name()));
                    }
                  }
                }
              }
              if (OB_SUCCESS == ret && !first_storing_column) {
                pos -= 2;  // fallback to the col name
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
                }
              }
            }
          }  // end of storing columns

          // print partititon table index type
          if (OB_SUCCESS == ret && table_schema.is_partitioned_table()) {
            ObIndexType index_type_ = index_schema->get_index_type();
            const ObPartitionOption& part_opt = table_schema.get_part_expr();
            if (part_opt.get_part_num() > 1) {
              if (INDEX_TYPE_NORMAL_LOCAL == index_type_ || INDEX_TYPE_UNIQUE_LOCAL == index_type_) {
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, " LOCAL "))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print local index", K(ret), K(table_schema));
                }
              } else if (INDEX_TYPE_NORMAL_GLOBAL == index_type_ || INDEX_TYPE_UNIQUE_GLOBAL == index_type_) {
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, " GLOBAL "))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print global index", K(ret), K(table_schema));
                }
              } else {
                // do nothing
              }
            }
          }
          // print index options
          if (OB_SUCC(ret)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print space", K(ret));
            } else if (OB_FAIL(print_table_definition_table_options(*index_schema, buf, buf_len, pos, false))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print index options", K(ret), K(*index_schema));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaManager::print_table_definition_rowkeys(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
  if (!table_schema.is_no_pk_table() && rowkey_info.get_size() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  PRIMARY KEY ("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print PRIMARY KEY(", K(ret));
    }

    bool is_first_col = true;
    for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); j++) {
      const ObColumnSchemaV2* col = NULL;
      if (NULL == (col = get_column_schema(table_schema.get_table_id(), rowkey_info.get_column(j)->column_id_))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        SHARE_SCHEMA_LOG(WARN, "fail to get column", "column_id", rowkey_info.get_column(j)->column_id_);
      } else if (!col->is_shadow_column()) {
        if (true == is_first_col) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%s`", col->get_column_name()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name()));
          } else {
            is_first_col = false;
          }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", `%s`", col->get_column_name()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name()));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    }
  }
  return ret;
}

int ObSchemaManager::print_table_definition_table_options(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos, bool is_for_table_status) const
{
  const bool is_index_tbl = table_schema.is_index_table();
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret && !is_index_tbl && !is_for_table_status) {
    uint64_t auto_increment = 0;
    if (OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_value(table_schema.get_tenant_id(),
            table_schema.get_table_id(),
            table_schema.get_autoinc_column_id(),
            auto_increment))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get auto_increment value", K(ret));
    } else if (auto_increment > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT = %lu ", auto_increment))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print auto increment", K(ret), K(auto_increment), K(table_schema));
      }
    }
  }

  if (OB_SUCCESS == ret && !is_for_table_status && !is_index_tbl &&
      CHARSET_INVALID != table_schema.get_charset_type()) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "DEFAULT CHARSET = %s ", ObCharset::charset_name(table_schema.get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_for_table_status && !is_index_tbl &&
      CS_TYPE_INVALID != table_schema.get_collation_type() &&
      false == ObCharset::is_default_collation(table_schema.get_charset_type(), table_schema.get_collation_type())) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "COLLATE = %s ", ObCharset::collation_name(table_schema.get_collation_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print collate", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && table_schema.is_compressed()) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_index_tbl ? "COMPRESSION '%s' " : "COMPRESSION = '%s' ",
            table_schema.get_compress_func_name()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print compress method", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.get_expire_info().length() > 0 &&
      NULL != table_schema.get_expire_info().ptr()) {
    const ObString expire_str = table_schema.get_expire_info();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "EXPIRE_INFO = (%.*s) ", expire_str.length(), expire_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print expire info", K(ret), K(expire_str));
    }
  }
  // ObSchemaManager is obsolete, but it is still used in storage single test.
  // It is no problem to comment out the following code directly.
  /*
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "REPLICA_NUM = %ld ",
                                             table_schema.get_replica_num()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print replica num", K(ret), K(table_schema));
    }
  }
  */
  if (OB_SUCCESS == ret && table_schema.get_block_size() >= 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_index_tbl ? "BLOCK_SIZE %ld " : "BLOCK_SIZE = %ld ",
            table_schema.get_block_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print block size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "USE_BLOOM_FILTER = %s ", table_schema.is_use_bloomfilter() ? "TRUE" : "FALSE"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print use bloom filter", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && common::OB_INVALID_ID != table_schema.get_tablegroup_id()) {
    const ObTablegroupSchema* tablegroup_schema = get_tablegroup_schema(table_schema.get_tablegroup_id());
    if (NULL != tablegroup_schema) {
      const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name();
      if (tablegroup_name.length() > 0 && NULL != tablegroup_name.ptr()) {
        if (OB_FAIL(databuff_printf(
                buf, buf_len, pos, "TABLEGROUP = '%.*s' ", tablegroup_name.length(), tablegroup_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup", K(ret), K(tablegroup_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "tablegroup name is null");
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "tablegroup schema is null");
    }
  }

  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.get_progressive_merge_num() > 0) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "PROGRESSIVE_MERGE_NUM = %ld ", table_schema.get_progressive_merge_num()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_for_table_status && table_schema.get_comment_str().length() > 0) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, is_index_tbl ? "COMMENT '%s' " : "COMMENT = '%s' ", table_schema.get_comment()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print comment", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    pos -= 1;
    buf[pos] = '\0';  // remove trailer space
  }
  return ret;
}

static int print_partition_func(ObSqlString& disp_part_str, const ObString& func_expr)
{
  int ret = OB_SUCCESS;
  // replace `key_v2' with `key'
  static const int32_t len1 = strlen("key_v2");
  if (func_expr.length() >= len1 && 0 == strncasecmp(func_expr.ptr(), "key_v2", len1)) {
    if (OB_FAIL(disp_part_str.append_fmt("partition by key%.*s", func_expr.length() - len1, func_expr.ptr() + len1))) {
      SHARE_SCHEMA_LOG(WARN, "fail to append diaplay partition expr", K(ret), K(func_expr));
    }
  } else {
    if (OB_FAIL(disp_part_str.append_fmt("partition by %.*s", func_expr.length(), func_expr.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to append diaplay partition expr", K(ret), K(func_expr));
    }
  }
  return ret;
}

int ObSchemaManager::print_table_definition_partition_options(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (table_schema.is_partitioned_table()) {
    const ObPartitionOption& part_opt = table_schema.get_part_expr();
    ObString disp_part_fun_expr_str;
    ObSqlString disp_part_str;
    if (OB_FAIL(print_partition_func(disp_part_str, part_opt.get_part_func_expr_str()))) {
      SHARE_SCHEMA_LOG(WARN, "failed to print part func", K(ret));
    } else if (FALSE_IT(disp_part_fun_expr_str = disp_part_str.string())) {
      // will not reach here
    } else if (OB_FAIL(databuff_printf(
                   buf, buf_len, pos, " %.*s", disp_part_fun_expr_str.length(), disp_part_fun_expr_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf partition expr", K(ret), K(part_opt));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " partitions %ld", part_opt.get_part_num()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf partition number", K(ret), K(part_opt));
    }
  }
  return ret;
}

int ObSchemaManager::print_view_definiton(uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;

  if (buf && buf_len > 0) {
    const ObTableSchema* table_schema = NULL;
    if (NULL == (table_schema = get_table_schema(table_id))) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(table_id));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   "CREATE VIEW `%s` AS %.*s",
                   table_schema->get_table_name(),
                   table_schema->get_view_schema().get_view_definition_str().length(),
                   table_schema->get_view_schema().get_view_definition_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is bull", K(ret));
  }
  return ret;
}

int ObSchemaManager::print_database_definiton(
    uint64_t database_id, bool if_not_exists, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t mark_pos = 0;
  const ObDatabaseSchema* database_schema = NULL;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(buf_len));
  }

  if (OB_SUCC(ret)) {
    if (NULL == (database_schema = get_database_schema(extract_tenant_id(database_id), database_id))) {
      ret = OB_ERR_BAD_DATABASE;
      SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(database_id));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   "CREATE DATABASE %s`%.*s`",
                   true == if_not_exists ? "IF NOT EXISTS " : "",
                   database_schema->get_database_name_str().length(),
                   database_schema->get_database_name_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print database definition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    mark_pos = pos;
  }
  if (OB_SUCCESS == ret && CHARSET_INVALID != database_schema->get_charset_type()) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            " DEFAULT CHARACTER SET = %s",
            ObCharset::charset_name(database_schema->get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(*database_schema));
    }
  }
  if (OB_SUCCESS == ret && CS_TYPE_INVALID != database_schema->get_collation_type() &&
      !ObCharset::is_default_collation(database_schema->get_collation_type())) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            " DEFAULT COLLATE = %s",
            ObCharset::collation_name(database_schema->get_collation_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default collate", K(ret), K(*database_schema));
    }
  }
  // ObSchemaManager has been deprecated, but it is still used in the storage single test.
  // There is no problem directly commenting the code below.
  /*
  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REPLICA_NUM = %ld",
                                             database_schema->get_replica_num()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print replica num", K(ret), K(*database_schema));
    }
  }
  */
  if (OB_SUCCESS == ret && database_schema->get_primary_zone().length() > 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            " PRIMARY_ZONE = %.*s",
            database_schema->get_primary_zone().length(),
            database_schema->get_primary_zone().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print primary zone", K(ret), K(*database_schema));
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t tablegroup_id = database_schema->get_default_tablegroup_id();
    if (common::OB_INVALID_ID != tablegroup_id) {
      const ObTablegroupSchema* tablegroup_schema = get_tablegroup_schema(tablegroup_id);
      if (NULL != tablegroup_schema) {
        const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name();
        if (!tablegroup_name.empty()) {
          if (OB_FAIL(databuff_printf(
                  buf, buf_len, pos, " DEFAULT TABLEGROUP = %.*s", tablegroup_name.length(), tablegroup_name.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup", K(ret), K(tablegroup_name));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "tablegroup name is empty");
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "tablegroup schema is null");
      }
    }
  }
  if (OB_SUCCESS == ret && pos > mark_pos) {
    buf[pos] = '\0';  // remove trailer dot and space
  }
  return ret;
}

int ObSchemaManager::print_tenant_definition(
    uint64_t tenant_id, common::ObMySQLProxy* sql_proxy, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(buf_len));
  }

  if (OB_SUCC(ret)) {
    if (NULL == (tenant_schema = get_tenant_info(tenant_id))) {
      ret = OB_TENANT_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "Unknow tenant", K(ret), K(tenant_id));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   "CREATE TENANT %.*s ",
                   tenant_schema->get_tenant_name_str().length(),
                   tenant_schema->get_tenant_name_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tenant name", K(ret), K(tenant_schema->get_tenant_name_str()));
    }
  }

  if (OB_SUCCESS == ret && CHARSET_INVALID != tenant_schema->get_charset_type()) {  // charset
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "charset=%s, ", ObCharset::charset_name(tenant_schema->get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(tenant_schema->get_charset_type()));
    }
  }
  if (OB_SUCCESS == ret && 0 < tenant_schema->get_primary_zone().length()) {  // primary_zone
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            "primary_zone=\'%.*s\', ",
            tenant_schema->get_primary_zone().length(),
            tenant_schema->get_primary_zone().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print primary_zone", K(ret), K(tenant_schema->get_primary_zone()));
    }
  }
  if (OB_SUCC(ret)) {  // resource_pool_list
    ObUnitInfoGetter ui_getter;
    typedef ObSEArray<ObResourcePool, 10> PoolList;
    PoolList pool_list;
    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "sql proxy is null", K(ret));
    } else if (OB_FAIL(ui_getter.init(*sql_proxy))) {
      SHARE_SCHEMA_LOG(WARN, "init unit getter fail", K(ret));
    } else if (OB_FAIL(ui_getter.get_pools_of_tenant(tenant_id, pool_list))) {
      SHARE_SCHEMA_LOG(WARN, "faile to get pool list", K(tenant_id));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "resource_pool_list("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret));
    } else {
      int64_t i = 0;
      for (i = 0; OB_SUCC(ret) && i < pool_list.count() - 1; i++) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%s\', ", pool_list.at(i).name_.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret), K(pool_list.at(i)));
        }
      }
      if (OB_SUCCESS == ret && pool_list.count() > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%s\')", pool_list.at(i).name_.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret), K(pool_list.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObSchemaManager::deep_copy_obj(const ObObj& src, ObObj& dest) const
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (size > 0) {
    if (NULL == (buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
      LOG_WARN("Fail to deep copy obj, ", K(ret));
    }
  } else {
    dest = src;
  }

  return ret;
}

}  // end namespace schema
}  // end of namespace share
}  // end namespace oceanbase
