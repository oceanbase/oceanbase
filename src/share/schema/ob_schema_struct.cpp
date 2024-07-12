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
#include "ob_schema_struct.h"
#include "ob_schema_mgr.h"        // ObSimpleTableSchemaV2
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"
#include "lib/number/ob_number_v2.h"
#include "common/ob_zone_type.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_primary_zone_util.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_parser.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_root_utils.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_part_mgr_util.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/ob_encryption_util.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;
using namespace sql;
using namespace rootserver;

bool is_normal_partition(const PartitionType partition_type)
{
  return PARTITION_TYPE_NORMAL == partition_type;
}

bool is_hidden_partition(const PartitionType partition_type)
{
  return PARTITION_TYPE_SPLIT_SOURCE == partition_type
         || PARTITION_TYPE_SPLIT_DESTINATION == partition_type
         || PARTITION_TYPE_MERGE_SOURCE == partition_type
         || PARTITION_TYPE_MERGE_DESTINATION == partition_type;
}

bool check_normal_partition(const ObCheckPartitionMode check_partition_mode)
{
  return (CHECK_PARTITION_NORMAL_FLAG & check_partition_mode) > 0;
}

bool check_hidden_partition(const ObCheckPartitionMode check_partition_mode)
{
  return (CHECK_PARTITION_HIDDEN_FLAG & check_partition_mode) > 0;
}

lib::Worker::CompatMode get_worker_compat_mode(const ObCompatibilityMode &mode)
{
  lib::Worker::CompatMode worker_mode = lib::Worker::CompatMode::MYSQL;
  if (ObCompatibilityMode::ORACLE_MODE == mode) {
    worker_mode = lib::Worker::CompatMode::ORACLE;
  }
  return worker_mode;
}

int ObSchemaIdVersion::init(
    const uint64_t schema_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == schema_id
      || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_id/schema_version is invalid",
             KR(ret), K(schema_id), K(schema_version));
  } else {
    schema_id_ = schema_id;
    schema_version_ = schema_version;
  }
  return ret;
}

void ObSchemaIdVersion::reset()
{
  schema_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
}

bool ObSchemaIdVersion::is_valid() const
{
  return OB_INVALID_ID != schema_id_ && schema_version_ > 0;
}

int ObSchemaVersionGenerator::init(
    const int64_t start_version,
    const int64_t end_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_version <= 0
      || end_version <= 0
      || start_version > end_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(start_version), K(end_version));
  } else if (OB_FAIL(ObIDGenerator::init(SCHEMA_VERSION_INC_STEP,
                                         static_cast<uint64_t>(start_version),
                                         static_cast<uint64_t>(end_version)))) {
    LOG_WARN("fail to init id generator", KR(ret), K(start_version), K(end_version));
  }
  return ret;
}

int ObSchemaVersionGenerator::next_version(int64_t &current_version)
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  if (OB_FAIL(ObIDGenerator::next(id))) {
    LOG_WARN("fail to get next id", KR(ret));
  } else {
    current_version = static_cast<int64_t>(id);
  }
  return ret;
}

int ObSchemaVersionGenerator::get_start_version(int64_t &start_version) const
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  if (OB_FAIL(ObIDGenerator::get_start_id(id))) {
    LOG_WARN("fail to get start id", KR(ret));
  } else {
    start_version = static_cast<int64_t>(id);
  }
  return ret;
}

int ObSchemaVersionGenerator::get_current_version(int64_t &current_version) const
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  if (OB_FAIL(ObIDGenerator::get_current_id(id))) {
    LOG_WARN("fail to get current id", KR(ret));
  } else {
    current_version = static_cast<int64_t>(id);
  }
  return ret;
}

int ObSchemaVersionGenerator::get_end_version(int64_t &end_version) const
{
  int ret = OB_SUCCESS;
  uint64_t id = OB_INVALID_ID;
  if (OB_FAIL(ObIDGenerator::get_end_id(id))) {
    LOG_WARN("fail to get end id", KR(ret));
  } else {
    end_version = static_cast<int64_t>(id);
  }
  return ret;
}

int ObSchemaVersionGenerator::get_version_cnt(int64_t &version_cnt) const
{
  int ret = OB_SUCCESS;
  uint64_t id_cnt = OB_INVALID_ID;
  if (OB_FAIL(ObIDGenerator::get_id_cnt(id_cnt))) {
    LOG_WARN("fail to get id cnt", KR(ret));
  } else {
    version_cnt = static_cast<int64_t>(id_cnt);
  }
  return ret;
}

uint64_t ObSysTableChecker::TableNameWrapper::hash() const
{
  uint64_t hash_ret = 0;
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::ObCharset::hash(cs_type, table_name_, hash_ret);
  return hash_ret;
}

bool ObSysTableChecker::TableNameWrapper::operator ==(const TableNameWrapper &rv) const
{
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  return (database_id_ == rv.database_id_)
         && (name_case_mode_ == rv.name_case_mode_)
         && (0 == common::ObCharset::strcmp(cs_type, table_name_, rv.table_name_));
}

ObSysTableChecker::ObSysTableChecker()
    : tenant_space_table_id_map_(),
      sys_table_name_map_(),
      tenant_space_sys_table_num_(0),
      allocator_(),
      is_inited_(false)
{
}

ObSysTableChecker::~ObSysTableChecker()
{
  destroy();
}

ObSysTableChecker &ObSysTableChecker::instance()
{
  static ObSysTableChecker tenant_space_table_checker;
  return tenant_space_table_checker;
}

int ObSysTableChecker::is_tenant_space_table_id(const uint64_t table_id, bool &is_tenant_space_table)
{
  return instance().check_tenant_space_table_id(table_id, is_tenant_space_table);
}

int ObSysTableChecker::is_sys_table_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &table_name,
    bool &is_sys_table_name)
{
  return instance().check_sys_table_name(tenant_id, database_id, table_name, is_sys_table_name);
}

int ObSysTableChecker::is_inner_table_exist(
    const uint64_t tenant_id,
    const ObSimpleTableSchemaV2 &table,
    bool &exist)
{
  return instance().check_inner_table_exist(tenant_id, table, exist);
}

int ObSysTableChecker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // do nothing
  } else if (OB_FAIL(tenant_space_table_id_map_.create(TABLE_BUCKET_NUM,
                                                 ObModIds::OB_TENANT_SPACE_TABLE_ID_SET,
                                                 ObModIds::OB_TENANT_SPACE_TABLE_ID_SET))) {
    LOG_WARN("fail to create tenant_space_table_id_map", K(ret));
  } else if (OB_FAIL(sys_table_name_map_.create(TABLE_BUCKET_NUM,
                                                ObModIds::OB_SYS_TABLE_NAME_MAP,
                                                ObModIds::OB_SYS_TABLE_NAME_MAP))) {
    LOG_WARN("fail to create sys_table_name_map", K(ret));
  } else if (OB_FAIL(init_tenant_space_table_id_map())) {
    LOG_WARN("fail to init table id map", K(ret));
  } else if (OB_FAIL(init_sys_table_name_map())) {
    LOG_WARN("fail to init table name map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSysTableChecker::init_tenant_space_table_id_map()
{
  int ret = OB_SUCCESS;
  tenant_space_sys_table_num_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    if (OB_FAIL(tenant_space_table_id_map_.set_refactored(tenant_space_tables[i]))) {
      LOG_WARN("fail to set tenant space table_id", K(ret), K(tenant_space_tables[i]));
    } else if (is_sys_table(tenant_space_tables[i])) {
      // Including tenant-level system table indexes
      tenant_space_sys_table_num_++;
    }
  }
  return ret;
}

int ObSysTableChecker::init_sys_table_name_map()
{
  int ret = OB_SUCCESS;
  const schema_create_func all_core_table_schema_creator[]
      = { &share::ObInnerTableSchema::all_core_table_schema, NULL};
  const schema_create_func *creator_ptr_array[] = {
    all_core_table_schema_creator, share::core_table_schema_creators,
    share::sys_table_schema_creators, share::virtual_table_schema_creators,
    share::sys_view_schema_creators, NULL };

  ObTableSchema table_schema;
  ObNameCaseMode mode = OB_ORIGIN_AND_INSENSITIVE;
  ObString table_name;
  for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
      OB_SUCCESS == ret && NULL != *creator_ptr_ptr; ++creator_ptr_ptr) {
    for (const schema_create_func *creator_ptr = *creator_ptr_ptr;
        OB_SUCCESS == ret && NULL != *creator_ptr; ++creator_ptr) {
      table_schema.reset();
      if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_WARN("create table schema failed", K(ret));
      } else if (OB_FAIL(ob_write_string(table_schema.get_table_name(), table_name))) {
        LOG_WARN("fail to write table name", K(ret), K(table_schema));
      } else {
        uint64_t database_id = table_schema.get_database_id();
        TableNameWrapper table(database_id, mode, table_name);
        uint64_t key = table.hash();
        TableNameWrapperArray *value = NULL;
        ret = sys_table_name_map_.get_refactored(key, value);
        if (OB_HASH_NOT_EXIST == ret) {
          void *buffer = NULL;
          if (OB_ISNULL(buffer = allocator_.alloc(sizeof(TableNameWrapperArray)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("fail to alloc mem", K(ret));
          } else if (FALSE_IT(value = new (buffer) TableNameWrapperArray(
                              ObModIds::OB_TABLE_NAME_WRAPPER_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
          } else if (OB_FAIL(value->push_back(table))) {
            LOG_WARN("fail to push back tables", K(ret), K(key), K(table));
          } else if (OB_FAIL(sys_table_name_map_.set_refactored(key, value))) {
            LOG_WARN("fail to set table name array", K(ret), K(key), K(table));
          } else {
            LOG_INFO("set tenant space table name", K(key), K(table), "strlen", table_name.length());
          }
        } else if (OB_SUCCESS == ret) {
          if (OB_ISNULL(value)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("value is null", K(ret), K(key), K(table));
          } else if (value->count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("num not match", K(ret), K(key), K(table));
          } else if (OB_FAIL(value->push_back(table))) {
            LOG_WARN("fail to push back tables", K(ret), K(key), K(table));
          } else {
            LOG_INFO("duplicate tenant space table name", K(key), K(table));
          }
        } else {
          LOG_WARN("fail to get table name array", K(ret), K(key), K(table));
        }
      }
    }
  }
  return ret;
}

int ObSysTableChecker::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
  } else if (OB_FAIL(tenant_space_table_id_map_.destroy())) {
    LOG_ERROR("fail to destroy tenant_space_table_id_map", K(ret));
  } else {
    FOREACH(it, sys_table_name_map_) {
      TableNameWrapperArray *array = it->second;
      if (OB_NOT_NULL(array)) {
        array->reset();
        array = NULL;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sys_table_name_map_.destroy())) {
      LOG_ERROR("fail to destroy sys_table_name_map", K(ret));
    }
  }
  return ret;
}

int ObSysTableChecker::check_tenant_space_table_id(const uint64_t table_id, bool &is_tenant_space_table)
{
  int ret = OB_SUCCESS;
  uint64_t pure_table_id = table_id;
  is_tenant_space_table = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret));
  } else if (!is_inner_table(table_id)) {
    // skip
  } else {
    ret = tenant_space_table_id_map_.exist_refactored(pure_table_id);
    if (OB_HASH_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
      is_tenant_space_table = (OB_HASH_EXIST == ret);
      ret = OB_SUCCESS;
    } else {
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("fail to check table_id exist", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObSysTableChecker::check_sys_table_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &table_name,
    bool &is_system_table)
{
  int ret = OB_SUCCESS;
  is_system_table = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret));
  } else if (table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is empty", K(ret));
  } else if (!is_sys_database_id(database_id)) {
    is_system_table = false;
  } else {
    ObNameCaseMode mode = OB_ORIGIN_AND_INSENSITIVE;
    const TableNameWrapper table(database_id, mode, table_name);
    uint64_t key = table.hash();
    TableNameWrapperArray *value = NULL;
    if (OB_FAIL(sys_table_name_map_.get_refactored(key, value))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to check table_name exist", K(ret), K(key), K(table_name));
      } else {
        is_system_table = false;
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(value) || value->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table name array should not be empty", K(ret), K(key), K(table_name));
    } else {
      for (int64_t i = 0; !is_system_table && i < value->count(); i++) {
        is_system_table = (value->at(i) == table);
      }
    }
    LOG_TRACE("check sys table name", K(ret), K(key), K(table), "strlen", table_name.length());
  }
  return ret;
}

int ObSysTableChecker::check_inner_table_exist(
    const uint64_t tenant_id,
    const ObSimpleTableSchemaV2 &table,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  bool is_tenant_table = false;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  const int64_t table_id = table.get_table_id();
  const int64_t database_id = table.get_database_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret));
  } else if (!is_inner_table(table_id)
             || table.get_tenant_id() != tenant_id
             || !is_sys_database_id(database_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", KR(ret), K(tenant_id), K(table_id), K(database_id));
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, is_tenant_table))) {
    LOG_WARN("fail to check if table_id in tenant space", KR(ret), K(table_id));
  } else if (!is_tenant_table) {
    // case 1: sys table in sys tenant only
    exist = is_sys_tenant(tenant_id);
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret), K(tenant_id));
  } else {
    const bool is_oracle_mode = lib::Worker::CompatMode::ORACLE == compat_mode;
    // case 2: sys table in tenant space
    if (is_oceanbase_sys_database_id(database_id)) {
      if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
        // case 2.1: sys/meta tenant has all inner tables in oceanbase.
        exist = true;
      } else if (is_oracle_mode) {
        // case 2.2: oracle tenant has non cluster private inner tables in oceanbase.
        // mysql sys view in oracle tenant is not accessable.
        exist = !is_mysql_sys_view_table(table_id) && !is_cluster_private_tenant_table(table_id);
      } else {
        // case 2.3: mysql tenant has non cluster private inner tables in oceanbase.
        exist = !is_cluster_private_tenant_table(table_id);
      }
    } else {
      // information_schema、mysql、sys
      if (is_oracle_mode) {
        // case 2.4: In Oracle tenant mode, there is no need to add MySQL related internal tables
        exist = is_oracle_sys_database_id(database_id);
      } else {
        // case 2.5: In the MySQL tenant mode, there is no need to add Oracle related internal tables,
        exist = is_mysql_sys_database_id(database_id);
      }
    }
  }
  return ret;
}

/* -- hard code info for sys table indexes -- */
bool ObSysTableChecker::is_sys_table_index_tid(const int64_t index_id)
{
  bool bret = false;
  switch (index_id) {
#define SYS_INDEX_TABLE_ID_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef SYS_INDEX_TABLE_ID_SWITCH
    {
      bret = true;
      break;
    }
    default : {
      bret = false;
      break;
    }
  }
  return bret;
}

bool ObSysTableChecker::is_sys_table_has_index(const int64_t table_id)
{
  bool bret = false;
  switch (table_id) {
#define SYS_INDEX_DATA_TABLE_ID_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef SYS_INDEX_DATA_TABLE_ID_SWITCH
    {
      bret = true;
      break;
    }
    default : {
      bret = false;
      break;
    }
  }
  return bret;
}

int ObSysTableChecker::fill_sys_index_infos(ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const int64_t table_id = table.get_table_id();
  if (ObSysTableChecker::is_sys_table_has_index(table_id)
      && table.get_index_tid_count() <= 0) {
    ObArray<uint64_t> index_tids;
    if (OB_FAIL(get_sys_table_index_tids(table_id, index_tids))) {
      LOG_WARN("fail to get index tids", KR(ret), K(table_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_tids.count(); i++) {
      const int64_t index_id = index_tids.at(i);
      if (OB_INVALID_ID == index_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sys table's index_id", KR(ret), K(table_id));
      } else if (OB_FAIL(table.add_simple_index_info(ObAuxTableMetaInfo(
                         index_id,
                         USER_INDEX,
                         INDEX_TYPE_NORMAL_LOCAL)))) {
        LOG_WARN("fail to add simple_index_info", KR(ret), K(table_id), K(index_id));
      }
    } // end for
  }
  return ret;
}


int ObSysTableChecker::get_sys_table_index_tids(
    const int64_t table_id,
    ObIArray<uint64_t> &index_tids)
{
  int ret = OB_SUCCESS;
  index_tids.reset();
  switch (table_id) {
#define SYS_INDEX_DATA_TABLE_ID_TO_INDEX_IDS_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_IDS_SWITCH
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data table id", KR(ret), K(table_id));
      break;
    }
  }
  return ret;
}

int ObSysTableChecker::append_sys_table_index_schemas(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (ObSysTableChecker::is_sys_table_has_index(data_table_id)) {
    HEAP_VAR(ObTableSchema, index_schema) {
      switch (data_table_id) {
#define SYS_INDEX_DATA_TABLE_ID_TO_INDEX_SCHEMAS_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef SYS_INDEX_DATA_TABLE_ID_TO_INDEX_SCHEMAS_SWITCH
        default : {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("data table is invalid", KR(ret), K(data_table_id));
          break;
        }
      }
    } // end HEAP_VAR
  }
  return ret;
}

int ObSysTableChecker::append_table_(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &index_schema,
    common::ObIArray<share::schema::ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (!is_sys_tenant(tenant_id) && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, index_schema))) {
    LOG_WARN("fail to construct full table", KR(ret), K(tenant_id), "data_table_id", index_schema.get_data_table_id());
  } else if (OB_FAIL(tables.push_back(index_schema))) {
    LOG_WARN("fail to push back index", KR(ret), K(tenant_id), "data_table_id", index_schema.get_data_table_id());
  }
  return ret;
}

int ObSysTableChecker::add_sys_table_index_ids(
    const uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
#define ADD_SYS_INDEX_ID
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef ADD_SYS_INDEX_ID
  }
  return ret;
}

/* ------------------------------------------ */

bool ObSysTableChecker::is_cluster_private_tenant_table(const uint64_t table_id)
{
  bool bret = false;
  uint64_t pure_id = table_id;
  switch (pure_id) {
#define CLUSTER_PRIVATE_TABLE_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef CLUSTER_PRIVATE_TABLE_SWITCH
    {
      bret = true;
      break;
    }
    default : {
      bret = false;
      break;
    }
  }
  return bret;
}

int ObSysTableChecker::ob_write_string(const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  int64_t len = src.length();
  if (NULL == (buf = allocator_.alloc(len))) {
    dst.assign(NULL, 0);
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret), K(len));
  } else {
    MEMCPY(buf, src.ptr(), len);
    dst.assign_ptr(reinterpret_cast<char *>(buf), static_cast<ObString::obstr_size_t>(len));
  }
  return ret;
}

ObRefreshSchemaInfo::ObRefreshSchemaInfo(const ObRefreshSchemaInfo &other)
{
  (void) assign(other);
}

int ObRefreshSchemaInfo::assign(const ObRefreshSchemaInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  schema_version_ = other.schema_version_;
  sequence_id_ = other.sequence_id_;
  return ret;
}

void ObRefreshSchemaInfo::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  schema_version_ = common::OB_INVALID_VERSION;
  sequence_id_ = common::OB_INVALID_ID;
}

// In schema split mode:
// 1. The observer is started in the old mode, and RS pushes the sequence_id at the last stage of do_restart. At this time,
//  the sequence_id of the heartbeat received by the observer may be an illegal value;
// 2. After RS restarts, if DDL has not been done, the broadcast (tenant_id, schema_version) is an illegal value.
bool ObRefreshSchemaInfo::is_valid() const
{
  return true;
}

OB_SERIALIZE_MEMBER(ObRefreshSchemaInfo, tenant_id_, schema_version_, sequence_id_);

OB_SERIALIZE_MEMBER(ObSchemaObjVersion, object_id_, version_, object_type_);

void ObDropTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  schema_version_ = OB_INVALID_VERSION;
}

bool ObDropTenantInfo::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && OB_INVALID_VERSION != schema_version_;
}

ObSysParam::ObSysParam()
{
  reset();
}

ObSysParam::~ObSysParam()
{
}

int ObSysParam::init(const uint64_t tenant_id,
                     const ObString &name,
                     int64_t data_type,
                     const ObString &value,
                     const ObString &min_val,
                     const ObString &max_val,
                     const ObString &info,
                     int64_t flags)
{
  return init(tenant_id, "", name, data_type, value, min_val, max_val, info, flags);
}

int ObSysParam::init(const uint64_t tenant_id,
                     const common::ObZone &zone,
                     const ObString &name,
                     int64_t data_type,
                     const ObString &value,
                     const ObString &min_val,
                     const ObString &max_val,
                     const ObString &info,
                     int64_t flags)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  zone_ = zone;
  data_type_ = data_type;
  flags_ = flags;
  int64_t pos = 0;
  if (OB_INVALID == tenant_id || OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid or some variable is null", K(name), K(ret));
  } else if (OB_FAIL(databuff_printf(name_, OB_MAX_SYS_PARAM_NAME_LENGTH, pos, "%.*s", name.length(),
                                    name.ptr()))) {
    LOG_WARN("failed to print name", K(name), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(value_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", value.length(),
                                    value.ptr()))) {
    LOG_WARN("failed to print value", K(value), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(min_val_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", min_val.length(),
                                    min_val.ptr()))) {
    LOG_WARN("failed to print min_val", K(min_val), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(max_val_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", max_val.length(),
                                    max_val.ptr()))) {
    LOG_WARN("failed to print max_val", K(max_val), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(info_, OB_MAX_SYS_PARAM_INFO_LENGTH, pos, "%.*s", info.length(),
                                    info.ptr()))) {
    LOG_WARN("failed to print info", K(info), K(ret));
  } else {/*do nothing*/}
  return ret;
}

void ObSysParam::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  zone_.reset();
  MEMSET(name_, 0, sizeof(name_));
  data_type_ = 0;
  MEMSET(value_, 0, sizeof(value_));
  MEMSET(min_val_, 0, sizeof(min_val_));
  MEMSET(max_val_, 0, sizeof(max_val_));
  MEMSET(info_, 0, sizeof(info_));
  flags_ = 0;
}

int64_t ObSysParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(zone),
       K_(name),
       K_(data_type),
       K_(value),
       K_(info),
       K_(flags));
  return pos;
}

ObSysVariableSchema::ObSysVariableSchema()
  : ObSchema()
{
  reset();
}

ObSysVariableSchema::ObSysVariableSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSysVariableSchema::~ObSysVariableSchema()
{
}

int ObSysVariableSchema::assign(const ObSysVariableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    error_ret_ = src_schema.error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    schema_version_ = src_schema.schema_version_;
    read_only_ = src_schema.read_only_;
    name_case_mode_ = src_schema.name_case_mode_;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.get_sysvar_count(); ++i) {
      const ObSysVarSchema *sysvar = src_schema.get_sysvar_schema(i);
      if (sysvar != NULL) {
        if (OB_FAIL(add_sysvar_schema(*sysvar))) {
          LOG_WARN("add sysvar schema failed", K(ret), K(*sysvar));
        }
      }
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return ret;
}

bool ObSysVariableSchema::is_valid() const
{
  return ObSchema::is_valid() && OB_INVALID_ID != tenant_id_ && schema_version_ > 0;
}

void ObSysVariableSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  read_only_ = false;
  name_case_mode_ = OB_NAME_CASE_INVALID;
  memset(sysvar_array_, 0, sizeof(sysvar_array_));
  ObSchema::reset();
}

int64_t ObSysVariableSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  for (int64_t i = 0; i < get_sysvar_count(); ++i) {
    const ObSysVarSchema *sysvar = get_sysvar_schema(i);
    if (sysvar != NULL) {
      convert_size += sizeof(ObSysVarSchema);
      convert_size += sysvar->get_convert_size();
    }
  }
  return convert_size;
}

OB_DEF_DESERIALIZE(ObSysVariableSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, schema_version_, read_only_, name_case_mode_);

  if (OB_SUCC(ret)) {
    int64_t count = 0;
    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
    } else if (pos == data_len) {
      //do nothing
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("Fail to decode sys var count", K(ret));
    } else {
      ObSysVarSchema sys_var;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        sys_var.reset();
        if (OB_FAIL(sys_var.deserialize(buf, data_len, pos))) {
          LOG_WARN("Fail to deserialize sys var", K(ret));
        } else if (OB_FAIL(add_sysvar_schema(sys_var))) {
          LOG_WARN("Fail to add sys var", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSysVariableSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, schema_version_, read_only_, name_case_mode_);

  int64_t var_amount = ObSysVarFactory::ALL_SYS_VARS_COUNT;
  len += serialization::encoded_length_vi64(var_amount);

  for (int64_t i = 0; i < var_amount; i++) {
    if (NULL != sysvar_array_[i]) {
      len += sysvar_array_[i]->get_serialize_size();
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObSysVariableSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              schema_version_,
              read_only_,
              name_case_mode_);

  if (OB_SUCC(ret)) {
    int64_t var_amount = ObSysVarFactory::ALL_SYS_VARS_COUNT;
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, var_amount))) {
      LOG_WARN("Fail to encode sys var count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; i++) {
      if (OB_ISNULL(sysvar_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sysvar_array_ element is null", K(ret));
      } else if (OB_FAIL(sysvar_array_[i]->serialize(buf, buf_len, pos))) {
        LOG_WARN("Fail to serialize sys var", K(ret));
      }
    }
  }
  return ret;
}

int ObSysVariableSchema::add_sysvar_schema(const ObSysVarSchema &sysvar_schema)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObSysVarSchema *tmp_sysvar_schema = NULL;
  ObSysVarClassType var_id = ObSysVarFactory::find_sys_var_id_by_name(sysvar_schema.get_name(), true);
  int64_t var_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(SYS_VAR_INVALID == var_id)) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_TRACE("system variable is unknown", K(sysvar_schema));
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(var_id, var_idx))) {
    if (ret != OB_SYS_VARS_MAYBE_DIFF_VERSION) { // If the error is caused by a different version, just ignore it
      LOG_WARN("calc system variable store index failed", K(ret));
    } else {
      ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
      LOG_INFO("system variable maybe come from diff version", "name", sysvar_schema.get_name());
    }
  } else if (OB_UNLIKELY(var_idx < 0) || OB_UNLIKELY(var_idx >= get_sysvar_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("system variable index is invalid", K(ret), K(var_idx), K(get_sysvar_count()));
  } else if (OB_ISNULL(ptr = alloc(sizeof(ObSysVarSchema)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc sysvar schema failed", K(sizeof(ObSysVarSchema)));
  } else {
    tmp_sysvar_schema = new(ptr) ObSysVarSchema(allocator_);
    if (OB_FAIL(tmp_sysvar_schema->assign(sysvar_schema))) {
      LOG_WARN("fail to assign sysvar", KR(ret), K(sysvar_schema));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!tmp_sysvar_schema->is_valid())) {
      ret = tmp_sysvar_schema->get_err_ret();
      LOG_WARN("sysvar schema is invalid", K(ret));
    } else if (sysvar_array_[var_idx] == NULL) {
      sysvar_array_[var_idx] = tmp_sysvar_schema;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sysvar key-value is duplicated");
    }
  }
  if (OB_SUCC(ret)) {
    if (0 == sysvar_schema.get_name().case_compare(OB_SV_READ_ONLY)) {
      read_only_ = static_cast<bool> ((sysvar_schema.get_value())[0] - '0');
    } else if (0 == sysvar_schema.get_name().case_compare(OB_SV_LOWER_CASE_TABLE_NAMES)) {
      name_case_mode_ = static_cast<ObNameCaseMode>((sysvar_schema.get_value())[0] - '0');
    }
  }
  return ret;
}

int ObSysVariableSchema::load_default_system_variable(bool is_sys_tenant)
{
  int ret = OB_SUCCESS;
  ObString value;
  ObSysVarSchema sysvar;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObSysVariables::get_amount(); ++i) {
    sysvar.reset();
    if (is_sys_tenant && ObCharset::case_insensitive_equal(ObSysVariables::get_name(i), OB_SV_LOWER_CASE_TABLE_NAMES)) {
      value = ObString::make_string("2");
    } else {
      value = ObSysVariables::get_value(i);
    }
    sysvar.set_tenant_id(get_tenant_id());
    sysvar.set_data_type(ObSysVariables::get_type(i));
    sysvar.set_flags(ObSysVariables::get_flags(i));
    sysvar.set_schema_version(get_schema_version());
    if (OB_FAIL(sysvar.set_name(ObSysVariables::get_name(i)))) {
      LOG_WARN("set sysvar schema name failed", K(ObSysVariables::get_name(i)), K(ret));
    } else if (OB_FAIL(sysvar.set_value(value))) {
      LOG_WARN("set sysvar value failed", K(ret), K(value));
    } else if (OB_FAIL(sysvar.set_min_val(ObSysVariables::get_min(i)))) {
      LOG_WARN("set sysvar schema min value failed", K(ret), K(ObSysVariables::get_min(i)));
    } else if (OB_FAIL(sysvar.set_max_val(ObSysVariables::get_max(i)))) {
      LOG_WARN("set sysvar schema max value failed", K(ret), K(ObSysVariables::get_max(i)));
    } else if (OB_FAIL(sysvar.set_info(ObSysVariables::get_info(i)))) {
      LOG_WARN("set sysvar schema info failed", K(ret), K(ObSysVariables::get_info(i)));
    } else if (OB_FAIL(add_sysvar_schema(sysvar))) {
      LOG_WARN("add sysvar schema failed", K(ret));
    }
  }
  return ret;
}

int64_t ObSysVariableSchema::get_real_sysvar_count() const
{
  int64_t sysvar_cnt = 0;
  for (int64_t i = 0; i < get_sysvar_count(); ++i) {
    if (sysvar_array_[i] != NULL) {
      ++sysvar_cnt;
    }
  }
  return sysvar_cnt;
}

int ObSysVariableSchema::get_sysvar_schema(const ObString &name, const ObSysVarSchema *&sysvar_schema) const
{
  int ret = OB_SUCCESS;
  ObSysVarClassType var_id = ObSysVarFactory::find_sys_var_id_by_name(name, false);
  if (SYS_VAR_INVALID == var_id) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
  } else {
    ret = get_sysvar_schema(var_id, sysvar_schema);
  }
  return ret;
}

int ObSysVariableSchema::get_sysvar_schema(ObSysVarClassType var_id, const ObSysVarSchema *&sysvar_schema) const
{
  int ret = OB_SUCCESS;
  int64_t var_idx = OB_INVALID_INDEX;
  if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(var_id, var_idx))) {
    LOG_WARN("calc system variable store index failed", K(ret));
  } else if (OB_UNLIKELY(var_idx < 0) || OB_UNLIKELY(var_idx >= get_sysvar_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("system variable index is invalid", K(var_idx));
  } else {
    sysvar_schema = get_sysvar_schema(var_idx);
    if (OB_ISNULL(sysvar_schema)) {
      ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    }
  }
  return ret;
}

ObSysVarSchema *ObSysVariableSchema::get_sysvar_schema(int64_t idx)
{
  ObSysVarSchema *ret = NULL;
  if (idx >= 0 && idx < get_sysvar_count()) {
    ret = sysvar_array_[idx];
  }
  return ret;
}

const ObSysVarSchema *ObSysVariableSchema::get_sysvar_schema(int64_t idx) const
{
  const ObSysVarSchema *ret = NULL;
  if (idx >= 0 && idx < get_sysvar_count()) {
    ret = sysvar_array_[idx];
  }
  return ret;
}

int ObSysVariableSchema::get_oracle_mode(bool &is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  is_oracle_mode = false;
  const ObSysVarSchema *sysvar_schema = nullptr;
  if (OB_FAIL(get_sysvar_schema(SYS_VAR_OB_COMPATIBILITY_MODE, sysvar_schema))) {
    LOG_WARN("failed to get ob_compatibility_mode", K(ret));
  } else if (0 == (sysvar_schema->get_value()).case_compare("1")) {
    is_oracle_mode = true;
  }
  return ret;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTenantSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObSchema::ObSchema()
    : buffer_(this), error_ret_(OB_SUCCESS), is_inner_allocator_(false), allocator_(NULL)
{
}

ObSchema::ObSchema(common::ObIAllocator *allocator)
    : buffer_(this), error_ret_(OB_SUCCESS), is_inner_allocator_(false), allocator_(allocator)
{
}

ObSchema::~ObSchema()
{
  if (is_inner_allocator_) {
    common::ObArenaAllocator *arena = static_cast<common::ObArenaAllocator *>(allocator_);
    OB_DELETE(ObArenaAllocator, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA, arena);
    allocator_ = NULL;
  }
  buffer_ = NULL;
}

void ObSchema::reset_allocator()
{
  if (!is_inner_allocator_) {
    if (allocator_) {
      allocator_->reset();
      allocator_ = NULL;
    }
  }
}

common::ObIAllocator *ObSchema::get_allocator()
{
  if (NULL == allocator_) {
    if (!THIS_WORKER.has_req_flag()) {
      if (NULL == (allocator_ = OB_NEW(ObArenaAllocator, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA))) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "Fail to new allocator.");
      } else {
        is_inner_allocator_ = true;
      }
    } else if (NULL!= schema_stack_allocator()) {
      allocator_ = schema_stack_allocator();
    } else {
      allocator_ = &THIS_WORKER.get_allocator();
    }
  }
  return allocator_;
}

void *ObSchema::alloc(int64_t size)
{
  void *ret = NULL;
  ObIAllocator *allocator = get_allocator();
  if (NULL == allocator) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Fail to get allocator.");
  } else {
    ret = allocator->alloc(size);
  }

  return ret;
}

void ObSchema::free(void *ptr)
{
  if (NULL != ptr) {
    if (NULL != allocator_) {
      allocator_->free(ptr);
    }
  }
}

int ObSchema::zone_array2str(const common::ObIArray<common::ObZone> &zone_list,
                             char *str, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObString> zone_ptr_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
    const common::ObZone &this_zone = zone_list.at(i);
    common::ObString zone_ptr(this_zone.size(), this_zone.ptr());
    if (OB_FAIL(zone_ptr_array.push_back(zone_ptr))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(string_array2str(zone_ptr_array, str, buf_size))) {
    LOG_WARN("fail to convert Obstring array to C style string", K(ret));
  } else {} // good
  return ret;
}

int ObSchema::set_primary_zone_array(
    const common::ObIArray<ObZoneScore> &src,
    common::ObIArray<ObZoneScore> &dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  int64_t count = src.count();
  if (OB_FAIL(dst.reserve(count))) {
    LOG_WARN("fail to reserve array", KR(ret), K(count));
  }
  for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
    const ObZoneScore &zone_score = src.at(i);
    ObString str;
    if (OB_FAIL(deep_copy_str(zone_score.zone_, str))) {
      LOG_WARN("deep copy str failed", K(ret));
    } else if (OB_FAIL(dst.push_back(
            ObZoneScore(str, zone_score.score_)))) {
      LOG_WARN("fail to push back", K(ret));
      for (int64_t j = 0; j < dst.count(); ++j) {
        free(dst.at(j).zone_.ptr());
      }
      free(str.ptr());
    } else {} // ok
  }
  return ret;
}

int ObSchema::string_array2str(const common::ObIArray<common::ObString> &string_array,
                               char *str, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(str), K(buf_size));
  } else {
    MEMSET(str, 0, static_cast<uint32_t>(buf_size));
    int64_t nwrite = 0;
    int64_t n = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < string_array.count(); ++i) {
      n = snprintf(str + nwrite, static_cast<uint32_t>(buf_size - nwrite),
          "%s%s", to_cstring(string_array.at(i)), (i != string_array.count() - 1) ? ";" : "");
      if (n <= 0 || n >= buf_size - nwrite) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf failed", K(ret));
      } else {
        nwrite += n;
      }
    }
  }
  return ret;
}

int ObSchema::str2string_array(const char *str,
                               common::ObIArray<common::ObString> &string_array) const
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  while (OB_SUCC(ret)) {
    item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ";", &save_ptr);
    if (NULL != item_str) {
      if (OB_FAIL(string_array.push_back(ObString::make_string(item_str)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObSchema::deep_copy_str(const char *src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (OB_SUCCESS != error_ret_) {
    ret = error_ret_;
    LOG_WARN("There has error in this schema, ", K(ret));
  } else if (OB_ISNULL(src)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The src is NULL, ", K(ret));
  } else {
    int64_t len = strlen(src) + 1;
    if (NULL == (buf = static_cast<char*>(alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(len), K(ret));
    } else {
      MEMCPY(buf, src, len-1);
      buf[len-1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
    }
  }

  return ret;
}

int ObSchema::deep_copy_str(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (OB_SUCCESS != error_ret_) {
    ret = error_ret_;
    LOG_WARN("There has error in this schema, ", K(ret));
  } else {
    if (src.length() > 0) {
      int64_t len = src.length() + 1;
      if (NULL == (buf = static_cast<char*>(alloc(len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory, ", K(len), K(ret));
      } else {
        MEMCPY(buf, src.ptr(), len-1);
        buf[len - 1] = '\0';
        dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
      }
    } else {
      dest.reset();
    }
  }

  return ret;
}

int ObSchema::deep_copy_obj(const ObObj &src, ObObj &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (OB_SUCCESS != error_ret_) {
    ret = error_ret_;
    LOG_WARN("There has error in this schema, ", K(ret));
  } else {
    if (size > 0) {
      if (NULL == (buf = static_cast<char*>(alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
      } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))){
        LOG_WARN("Fail to deep copy obj, ", K(ret));
      }
    } else {
      dest = src;
    }
  }

  return ret;
}

int ObSchema::deep_copy_string_array(const ObIArray<ObString> &src_array,
                                     ObArrayHelper<ObString> &dst_array)
{
  int ret = OB_SUCCESS;
  if (NULL != dst_array.get_base_address()) {
    free(dst_array.get_base_address());
    dst_array.reset();
  }
  const int64_t alloc_size = src_array.count() * static_cast<int64_t>(sizeof(ObString));
  void *buf = NULL;
  if (src_array.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(alloc_size), K(ret));
  } else {
    dst_array.init(src_array.count(), static_cast<ObString *>(buf));
    for (int64_t i = 0; OB_SUCC(ret) && i < src_array.count(); ++i) {
      ObString str;
      if (OB_FAIL(deep_copy_str(src_array.at(i), str))) {
        LOG_WARN("deep_copy_str failed", K(ret));
      } else if (OB_FAIL(dst_array.push_back(str))) {
        LOG_WARN("push_back failed", K(ret));
        // free memory avoid memory leak
        for (int64_t j = 0; j < dst_array.count(); ++j) {
          free(dst_array.at(j).ptr());
        }
        free(str.ptr());
      }
    }
  }

  if (OB_SUCCESS != ret && NULL != buf) {
    free(buf);
  }
  return ret;
}

int ObSchema::add_string_to_array(const ObString &str,
                                  ObArrayHelper<ObString> &str_array)
{
  int ret = OB_SUCCESS;
  const int64_t extend_cnt = STRING_ARRAY_EXTEND_CNT;
  int64_t alloc_size = 0;
  void *buf = NULL;
  // if not init, alloc memory and init it
  if (!str_array.check_inner_stat()) {
    alloc_size = extend_cnt * static_cast<int64_t>(sizeof(ObString));
    if (NULL == (buf = alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(alloc_size), K(ret));
    } else {
      str_array.init(extend_cnt, static_cast<ObString *>(buf));
    }
  }

  if (OB_SUCC(ret)) {
    ObString temp_str;
    if (OB_FAIL(deep_copy_str(str, temp_str))) {
      LOG_WARN("deep_copy_str failed", K(ret));
    } else {
      // if full, extend it
      if (str_array.capacity() == str_array.count()) {
        alloc_size = (str_array.count() + extend_cnt) * static_cast<int64_t>(sizeof(ObString));
        if (NULL == (buf = alloc(alloc_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc failed", K(alloc_size), K(ret));
        } else {
          ObArrayHelper<ObString> new_array(
              str_array.count() + extend_cnt, static_cast<ObString *>(buf));
          if (OB_FAIL(new_array.assign(str_array))) {
            LOG_WARN("assign failed", K(ret));
          } else {
            free(str_array.get_base_address());
            str_array = new_array;
          }
        }
        if (OB_SUCCESS != ret && NULL != buf) {
          free(buf);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(str_array.push_back(temp_str))) {
          LOG_WARN("push_back failed", K(ret));
          free(temp_str.ptr());
        }
      }
    }
  }
  return ret;
}

int ObSchema::serialize_string_array(char *buf, const int64_t buf_len, int64_t &pos,
                                     const ObArrayHelper<ObString> &str_array) const
{
  int ret = OB_SUCCESS;
  int64_t temp_pos = pos;
  const int64_t count = str_array.count();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count))) {
    LOG_WARN("serialize count failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < str_array.count(); ++i) {
      if (OB_FAIL(str_array.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("serialize string failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    pos = temp_pos;
  }
  return ret;
}

int ObSchema::deserialize_string_array(const char *buf, const int64_t data_len, int64_t &pos,
                                       common::ObArrayHelper<common::ObString> &str_array)
{
  int ret = OB_SUCCESS;
  int64_t temp_pos = pos;
  int64_t count = 0;
  str_array.reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("deserialize count failed", K(ret));
  } else if (0 == count) {
    //do nothing
  } else {
    void *array_buf = NULL;
    const int64_t alloc_size = count * static_cast<int64_t>(sizeof(ObString));
    if (NULL == (array_buf = alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(alloc_size), K(ret));
    } else {
      str_array.init(count, static_cast<ObString *>(array_buf));
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObString str;
        ObString copy_str;
        if (OB_FAIL(str.deserialize(buf, data_len, pos))) {
          LOG_WARN("string deserialize failed", K(ret));
        } else if (OB_FAIL(deep_copy_str(str, copy_str))) {
          LOG_WARN("deep_copy_str failed", K(ret));
        } else if (OB_FAIL(str_array.push_back(copy_str))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    pos = temp_pos;
  }
  return ret;
}

int64_t ObSchema::get_string_array_serialize_size(
      const ObArrayHelper<ObString> &str_array) const
{
  int64_t serialize_size = 0;
  const int64_t count = str_array.count();
  serialize_size += serialization::encoded_length_vi64(count);
  for (int64_t i = 0; i < count; ++i) {
    serialize_size += str_array.at(i).get_serialize_size();
  }
  return serialize_size;
}

void ObSchema::reset_string(ObString &str)
{
  if (NULL != str.ptr() && 0 != str.length()) {
    free(str.ptr());
  }
  str.reset();
}

void ObSchema::reset_string_array(ObArrayHelper<ObString> &str_array)
{
  if (NULL != str_array.get_base_address()) {
    for (int64_t i = 0; i < str_array.count(); ++i) {
      ObString &this_str = str_array.at(i);
      reset_string(this_str);
    }
    free(str_array.get_base_address());
  }
  str_array.reset();
}

const char *ObSchema::extract_str(const ObString &str) const
{
  return str.empty() ? "" : str.ptr();
}

void ObSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  if (is_inner_allocator_ && NULL != allocator_) {
    //It's better to invoke the reset methods of allocator if the ObIAllocator has reset function
    ObArenaAllocator *arena = static_cast<ObArenaAllocator*>(allocator_);
    arena->reuse();
  }
}

template <class T>
int ObSchema::preserve_array(T** &array, int64_t &array_capacity, const int64_t &preserved_capacity) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= preserved_capacity)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("preserved capacity should greater than 0", KR(ret), K(preserved_capacity));
  } else if (OB_NOT_NULL(array) || OB_UNLIKELY(0 != array_capacity)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to preserve when array is not null or capacity is not zero", KR(ret), KP(array), K(array_capacity));
  } else if (OB_ISNULL(array = static_cast<T**>(alloc(sizeof(T*) * preserved_capacity)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for partition arrary", KR(ret));
  } else {
    array_capacity = preserved_capacity;
  }
  return ret;
}
common::ObCollationType ObSchema::get_cs_type_with_cmp_mode(const ObNameCaseMode mode)
{
  common::ObCollationType cs_type = common::CS_TYPE_INVALID;

  if (OB_ORIGIN_AND_INSENSITIVE == mode || OB_LOWERCASE_AND_INSENSITIVE == mode) {
    cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (OB_ORIGIN_AND_SENSITIVE == mode){
    cs_type = common::CS_TYPE_UTF8MB4_BIN;
  } else {
    SHARE_SCHEMA_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid ObNameCaseMode value", K(mode));
  }
  return cs_type;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTenantSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
static const char *tenant_display_status_strs[] = {
  "NORMAL",
  "CREATING",
  "DROPPING",
  "RESTORE",
  "CREATING_STANDBY",
};

const char *ob_tenant_status_str(const ObTenantStatus status)
{
  STATIC_ASSERT(ARRAYSIZEOF(tenant_display_status_strs) == TENANT_STATUS_MAX,
                "type string array size mismatch with enum tenant status count");
  const char *str = NULL;
  if (status >= 0 && status < TENANT_STATUS_MAX) {
    str = tenant_display_status_strs[status];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid tenant status", K(status));
  }
  return str;
}

int get_tenant_status(const ObString &str, ObTenantStatus &status)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    status = TENANT_STATUS_MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_display_status_strs); ++i) {
      if (0 == str.case_compare(tenant_display_status_strs[i])) {
        status = static_cast<ObTenantStatus>(i);
        break;
      }
    }
    if (TENANT_STATUS_MAX == status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("display status str not found", K(ret), K(str));
    }
  }
  return ret;
}

bool is_tenant_normal(ObTenantStatus &status)
{
  return TENANT_STATUS_NORMAL == status;
}

bool is_tenant_restore(ObTenantStatus &status)
{
  return TENANT_STATUS_RESTORE == status || TENANT_STATUS_CREATING_STANDBY == status;
}

bool is_creating_standby_tenant_status(ObTenantStatus &status)
{
  return TENANT_STATUS_CREATING_STANDBY == status;
}

ObTenantSchema::ObTenantSchema()
  : ObSchema()
{
  reset();
}

ObTenantSchema::ObTenantSchema(ObIAllocator *allocator)
  : ObSchema(allocator),
    zone_list_(),
    zone_replica_attr_array_(),
    primary_zone_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator))
{
  reset();
}

ObTenantSchema::ObTenantSchema(const ObTenantSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObTenantSchema::~ObTenantSchema()
{
}

ObTenantSchema& ObTenantSchema::operator =(const ObTenantSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_schema_version(src_schema.schema_version_);
    set_locked(src_schema.locked_);
    set_read_only(src_schema.read_only_);
    set_collation_type(src_schema.get_collation_type());
    set_charset_type(src_schema.get_charset_type());
    set_name_case_mode(src_schema.name_case_mode_);
    set_default_tablegroup_id(src_schema.default_tablegroup_id_);
    set_compatibility_mode(src_schema.compatibility_mode_);
    set_drop_tenant_time(src_schema.drop_tenant_time_);
    set_status(src_schema.status_);
    set_in_recyclebin(src_schema.in_recyclebin_);
    set_arbitration_service_status(src_schema.arbitration_service_status_);
    if (OB_FAIL(set_tenant_name(src_schema.tenant_name_))) {
      LOG_WARN("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_zone_list(src_schema.zone_list_))) {
      LOG_WARN("set_zone_list failed", K(ret));
    } else if (OB_FAIL(set_primary_zone(src_schema.primary_zone_))) {
      LOG_WARN("set_primary_zone failed", K(ret));
    } else if (OB_FAIL(set_comment(src_schema.comment_))) {
      LOG_WARN("set_comment failed", K(ret));
    } else if (OB_FAIL(set_locality(src_schema.locality_str_))) {
      LOG_WARN("set locality failed", K(ret));
    } else if (OB_FAIL(set_zone_replica_attr_array(src_schema.zone_replica_attr_array_))) {
      LOG_WARN("set zone replica attr array failed", K(ret));
    } else if (OB_FAIL(set_primary_zone_array(src_schema.primary_zone_array_))) {
      LOG_WARN("fail to set primary zone array", K(ret));
    } else if (OB_FAIL(set_previous_locality(src_schema.previous_locality_str_))) {
      LOG_WARN("fail to set previous locality", K(ret));
    } else if (OB_FAIL(set_default_tablegroup_name(src_schema.default_tablegroup_name_))) {
      LOG_WARN("set default tablegroup name failed", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObTenantSchema::assign(const ObTenantSchema &src_schema)
{
  int ret = OB_SUCCESS;
  *this = src_schema;
   ret = get_err_ret();
  return ret;
}

bool ObTenantSchema::is_valid() const
{
  return ObSchema::is_valid() && OB_INVALID_ID != tenant_id_ && schema_version_ > 0;
}

void ObTenantSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  reset_string(tenant_name_);
  locked_ = false;
  read_only_ = false;
  charset_type_ = ObCharset::get_default_charset();
  collation_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
  name_case_mode_ = OB_NAME_CASE_INVALID;
  reset_string(comment_);
  default_tablegroup_id_ = OB_INVALID_ID;
  reset_string(default_tablegroup_name_);
  compatibility_mode_ = ObCompatibilityMode::OCEANBASE_MODE;
  drop_tenant_time_ = OB_INVALID_TIMESTAMP;
  status_ = TENANT_STATUS_NORMAL;
  in_recyclebin_ = false;
  arbitration_service_status_ = ObArbitrationServiceStatus::DISABLED;
  reset_physical_location_info();
  ObSchema::reset();
}

void ObTenantSchema::reset_physical_location_info()
{
  reset_string_array(zone_list_);
  reset_string(primary_zone_);
  reset_string(locality_str_);
  reset_string(previous_locality_str_);
  reset_zone_replica_attr_array();
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    free(primary_zone_array_.at(i).zone_.ptr());
  }
  primary_zone_array_.reset();

}

void ObTenantSchema::reset_alter_tenant_attributes()
{
  //reset no need attributes, such as:
  //read_only_

  //reset attributes standby can modify by self, such as:
  //locked_, comment_, default_tablegroup_id_, default_tablegroup_name_ and some location informations.

  //if the attributes are can not change after create, those are no need reset, such as:
  //compatibility_mode_, charset_type_, collation_type_, name_case_mode_

  //if the attributes must be synced from primary, those are no need reset, such as:
  //tenant_id_, schema_version_, tenant_name_, drop_tenant_time_, status_, in_recyclebin_
  locked_ = false;
  read_only_ = false;
  reset_string(comment_);
  default_tablegroup_id_ = OB_INVALID_ID;
  reset_string(default_tablegroup_name_);
  reset_physical_location_info();
}


int64_t ObTenantSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += tenant_name_.length() + 1;
  convert_size += zone_list_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < zone_list_.count(); ++i) {
    convert_size += zone_list_.at(i).length() + 1;
  }
  convert_size += zone_replica_attr_array_.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    convert_size += zone_replica_attr_array_.at(i).get_convert_size();
  }
  convert_size += primary_zone_.length() + 1;
  convert_size += comment_.length() + 1;
  convert_size += locality_str_.length() + 1;
  convert_size += primary_zone_array_.get_data_size();
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    convert_size += primary_zone_array_.at(i).zone_.length() + 1;
  }
  convert_size += previous_locality_str_.length() + 1;
  convert_size += default_tablegroup_name_.length() + 1;
  return convert_size;
}

void ObTenantSchema::reset_zone_replica_attr_array()
{
  if (NULL != zone_replica_attr_array_.get_base_address()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
      SchemaZoneReplicaAttrSet &zone_locality = zone_replica_attr_array_.at(i);
      reset_string_array(zone_locality.zone_set_);
      SchemaReplicaAttrArray &full_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_full_replica_attr_array());
      if (nullptr != full_attr_set.get_base_address()) {
        free(full_attr_set.get_base_address());
        full_attr_set.reset();
      }
      SchemaReplicaAttrArray &logonly_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_logonly_replica_attr_array());
      if (nullptr != logonly_attr_set.get_base_address()) {
        free(logonly_attr_set.get_base_address());
        logonly_attr_set.reset();
      }
      SchemaReplicaAttrArray &readonly_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_readonly_replica_attr_array());
      if (nullptr != readonly_attr_set.get_base_address()) {
        free(readonly_attr_set.get_base_address());
        readonly_attr_set.reset();
      }
      SchemaReplicaAttrArray &encryption_logonly_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_encryption_logonly_replica_attr_array());
      if (nullptr != encryption_logonly_attr_set.get_base_address()) {
        free(encryption_logonly_attr_set.get_base_address());
        encryption_logonly_attr_set.reset();
      }
    }
    free(zone_replica_attr_array_.get_base_address());
    zone_replica_attr_array_.reset();
  }
}

int ObTenantSchema::set_specific_replica_attr_array(
    SchemaReplicaAttrArray &this_schema_set,
    const common::ObIArray<ReplicaAttr> &src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.count();
  if (count > 0) {
    const int64_t size = count * static_cast<int64_t>(sizeof(share::ReplicaAttr));
    void *ptr = nullptr;
    if (nullptr == (ptr = alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(size));
    } else if (FALSE_IT(this_schema_set.init(count, static_cast<ReplicaAttr *>(ptr), count))) {
      // shall never by here
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
        const share::ReplicaAttr &src_replica_attr = src.at(i);
        ReplicaAttr *dst_replica_attr = &this_schema_set.at(i);
        if (nullptr == (dst_replica_attr = new (dst_replica_attr) ReplicaAttr(
                src_replica_attr.num_, src_replica_attr.memstore_percent_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("placement new return nullptr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTenantSchema::set_zone_replica_attr_array(
    const common::ObIArray<SchemaZoneReplicaAttrSet> &src)
{
  int ret = OB_SUCCESS;
  reset_zone_replica_attr_array();
  const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  void *buf = NULL;
  if (src.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(ret), K(alloc_size));
  } else {
    zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet *>(buf), src.count());
    // call construct func in advance to avoid core status
    //
    ARRAY_NEW_CONSTRUCT(SchemaZoneReplicaAttrSet, zone_replica_attr_array_);
    for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
      const SchemaZoneReplicaAttrSet &src_replica_attr_set = src.at(i);
      SchemaZoneReplicaAttrSet *this_schema_set = &zone_replica_attr_array_.at(i);
      if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_encryption_logonly_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_encryption_logonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(deep_copy_string_array(src_replica_attr_set.zone_set_, this_schema_set->zone_set_))) {
        LOG_WARN("fail to copy schema replica attr set zone set", K(ret));
      } else {
        this_schema_set->zone_ = src_replica_attr_set.zone_;
      }
    }
  }
  return ret;
}

int ObTenantSchema::set_zone_replica_attr_array(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &src)
{
  int ret = OB_SUCCESS;
  reset_zone_replica_attr_array();
  const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  void *buf = NULL;
  if (src.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(ret), K(alloc_size));
  } else {
    zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet *>(buf), src.count());
    // call construct func in advance to avoid core status
    //
    ARRAY_NEW_CONSTRUCT(SchemaZoneReplicaAttrSet, zone_replica_attr_array_);
    for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
      const share::ObZoneReplicaAttrSet &src_replica_attr_set = src.at(i);
      SchemaZoneReplicaAttrSet *this_schema_set = &zone_replica_attr_array_.at(i);
      if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
              static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_encryption_logonly_replica_attr_array()),
              src_replica_attr_set.replica_attr_set_.get_encryption_logonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else {
        common::ObArray<common::ObString> zone_set_ptrs;
        for (int64_t j = 0; OB_SUCC(ret) && j < src_replica_attr_set.zone_set_.count(); ++j) {
          const common::ObZone &zone = src_replica_attr_set.zone_set_.at(j);
          if (OB_FAIL(zone_set_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
             LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(deep_copy_string_array(zone_set_ptrs, this_schema_set->zone_set_))) {
          LOG_WARN("fail to copy schema replica attr set zone set", K(ret));
        } else {
          this_schema_set->zone_ = src_replica_attr_set.zone_;
        }
      }
    }
  }
  return ret;
}

int ObTenantSchema::get_zone_replica_attr_array(
    ZoneLocalityIArray &locality) const
{
  int ret = OB_SUCCESS;
  locality.reset();
  for (int64_t i = 0; i < zone_replica_attr_array_.count() && OB_SUCC(ret); ++i) {
    const SchemaZoneReplicaAttrSet &schema_replica_attr_set = zone_replica_attr_array_.at(i);
    ObZoneReplicaAttrSet zone_replica_attr_set;
    for (int64_t j = 0; OB_SUCC(ret) && j < schema_replica_attr_set.zone_set_.count(); ++j) {
      if (OB_FAIL(zone_replica_attr_set.zone_set_.push_back(schema_replica_attr_set.zone_set_.at(j)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_replica_attr_set.replica_attr_set_.assign(
            schema_replica_attr_set.replica_attr_set_))) {
      LOG_WARN("fail to set replica attr set", K(ret));
    } else {
      zone_replica_attr_set.zone_ = schema_replica_attr_set.zone_;
      if (OB_FAIL(locality.push_back(zone_replica_attr_set))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantSchema::get_zone_replica_attr_array_inherit(
    ObSchemaGetterGuard &schema_guard,
    ZoneLocalityIArray &locality) const
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  locality.reset();
  if (OB_FAIL(get_zone_replica_attr_array(locality))) {
    LOG_WARN("fail to get zone replica attr array inherit", K(ret));
  }
  return ret;
}

int64_t ObTenantSchema::get_full_replica_num() const
{
  int64_t num = 0;
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    num += zone_replica_attr_array_.at(i).get_full_replica_num();
  }
  return num;
}

int64_t ObTenantSchema::get_all_replica_num() const
{
  int64_t num = 0;
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    num += zone_replica_attr_array_.at(i).get_specific_replica_num();
  }
  return num;
}

int ObTenantSchema::get_paxos_replica_num(
    share::schema::ObSchemaGetterGuard &schema_guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  num = 0;
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    num += zone_replica_attr_array_.at(i).get_paxos_replica_num();
  }
  return ret;
}

// The tenant actually does not need to pass in the schema_guard to get the zone_list. In order to be consistent
// with the TableSchema and DatabaseSchema interfaces, the get_zone_list for the tenantSchema also passes in the schema_guard.
int ObTenantSchema::get_zone_list(
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<common::ObZone> &zone_list) const
{
  UNUSED(schema_guard);
  return get_zone_list(zone_list);
}

int ObTenantSchema::get_zone_list(
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  common::ObZone tmp_zone;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list_.count(); ++i) {
    tmp_zone.reset();
    const common::ObString &zone_ptr = zone_list_.at(i);
    if (OB_FAIL(tmp_zone.assign(zone_ptr.ptr()))) {
      LOG_WARN("fail to assign zone", K(ret), K(zone_ptr));
    } else if (OB_FAIL(zone_list.push_back(tmp_zone))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObTenantSchema::set_zone_list(
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  // The length of the string in zone_list_ptrs directly points to the ObZone of zone_list.
  common::ObArray<common::ObString> zone_list_ptrs;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
    const common::ObZone &zone = zone_list.at(i);
    if (OB_FAIL(zone_list_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_string_array(zone_list_ptrs, zone_list_))) {
    LOG_WARN("fail to copy zone list", K(ret), K(zone_list_ptrs));
  } else {} // no more to do
  return ret;
}

OB_DEF_SERIALIZE(ObTenantSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_, schema_version_, tenant_name_,
              primary_zone_, locked_, comment_, charset_type_,
              collation_type_, name_case_mode_, read_only_,
              locality_str_, previous_locality_str_);
  if (OB_FAIL(ret)) {
    LOG_WARN("func_SERIALIZE failed", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, zone_list_))) {
    LOG_WARN("serialize_string_array failed", K(ret));
  } else {} // no more to do
  LST_DO_CODE(OB_UNIS_ENCODE,
              default_tablegroup_id_,
              default_tablegroup_name_,
              compatibility_mode_,
              drop_tenant_time_,
              status_,
              in_recyclebin_,
              arbitration_service_status_);

  LOG_INFO("serialize schema",
           K_(tenant_id), K_(schema_version), K_(tenant_name),
           K_(primary_zone), K_(locked), K_(comment),
           K_(charset_type), K_(collation_type), K_(name_case_mode),
           K_(locality_str), K_(primary_zone_array), K_(default_tablegroup_id),
           K_(default_tablegroup_name), K_(drop_tenant_time), K_(in_recyclebin),
           K_(arbitration_service_status), K(ret));
  return ret;
}

OB_DEF_DESERIALIZE(ObTenantSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, schema_version_, tenant_name_,
              primary_zone_, locked_, comment_, charset_type_, collation_type_, name_case_mode_,
              read_only_, locality_str_, previous_locality_str_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, zone_list_))) {
    LOG_WARN("deserialize_string_array failed", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              default_tablegroup_id_,
              default_tablegroup_name_,
              compatibility_mode_,
              drop_tenant_time_,
              status_,
              in_recyclebin_,
              arbitration_service_status_);

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(set_tenant_name(tenant_name_))) {
    LOG_WARN("set_tenant_name failed", K(ret));
  } else if (OB_FAIL(set_primary_zone(primary_zone_))) {
    LOG_WARN("set_primary_zone failed", K(ret));
  } else if (OB_FAIL(set_comment(comment_))) {
    LOG_WARN("set_comment failed", K(ret));
  } else if (OB_FAIL(set_locality(locality_str_))) {
    LOG_WARN("set_locality failed", K(ret));
  } else if (OB_FAIL(set_default_tablegroup_name(default_tablegroup_name_))) {
    LOG_WARN("set default_tablegroup_name failed", K(ret));
  } else if (OB_FAIL(set_previous_locality(previous_locality_str_))) {
    // parse and set zone and region replica num array
    if (locality_str_.length() > 0 && zone_list_.count() > 0) {
      rootserver::ObLocalityDistribution locality_dist;
      ObArray<share::ObZoneReplicaAttrSet> zone_replica_attr_array;
      if (OB_FAIL(locality_dist.init())) {
        SHARE_SCHEMA_LOG(WARN, "fail to init locality dist", K(ret));
      } else if (OB_FAIL(locality_dist.parse_locality(
              locality_str_, zone_list_))) {
        SHARE_SCHEMA_LOG(WARN, "fail to parse locality", K(ret));
      } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_attr_array))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get zone region replica num array", K(ret));
      } else if (OB_FAIL(set_zone_replica_attr_array(zone_replica_attr_array))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set zone replica num array", K(ret));
      } else {} // no more to do, good
    } else {} // no need to parse locality
    // parse and set primary zone array
    if (OB_FAIL(ret)) {
    } else if (primary_zone_.length() > 0 && zone_list_.count() > 0) {
      ObPrimaryZoneUtil primary_zone_util(primary_zone_);
      if (OB_FAIL(primary_zone_util.init())) {
        SHARE_SCHEMA_LOG(WARN, "fail to init primary zone util", K(ret));
      } else if (OB_FAIL(primary_zone_util.check_and_parse_primary_zone())) {
        SHARE_SCHEMA_LOG(WARN, "fail to check and parse primary zone", K(ret));
      } else if (OB_FAIL(set_primary_zone_array(primary_zone_util.get_zone_array()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set primary zone array", K(ret));
      } else {} // set primary zone array success
    } else {} // no need to parse primary zone
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTenantSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, schema_version_, tenant_name_,
              primary_zone_, locked_, comment_, charset_type_, collation_type_, name_case_mode_,
              read_only_, locality_str_, previous_locality_str_,
              default_tablegroup_id_, default_tablegroup_name_,
              compatibility_mode_, drop_tenant_time_, status_, in_recyclebin_, arbitration_service_status_);
  len += get_string_array_serialize_size(zone_list_);
  return len;
}

int ObTenantSchema::get_primary_zone_score(
    const common::ObZone &zone,
    int64_t &zone_score) const
{
  int ret = OB_SUCCESS;
  zone_score = INT64_MAX;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_UNLIKELY(primary_zone_.empty())
             || OB_UNLIKELY(primary_zone_array_.count() <= 0)) {
    // zone score set to INT64_MAX above
  } else {
    for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
      if (zone == primary_zone_array_.at(i).zone_) {
        zone_score = primary_zone_array_.at(i).score_;
        break;
      } else {} // go no find
    }
  }
  return ret;
}

int ObTenantSchema::set_primary_zone_array(
    const common::ObIArray<ObZoneScore> &primary_zone_array)
{
  return ObSchema::set_primary_zone_array(primary_zone_array, primary_zone_array_);
}

int ObTenantSchema::get_primary_zone_inherit(
    ObSchemaGetterGuard &schema_guard,
    ObPrimaryZone &primary_zone) const
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  primary_zone.reset();
  if (OB_FAIL(primary_zone.set_primary_zone_array(get_primary_zone_array()))) {
    LOG_WARN("fail to set primary zone array", K(ret));
  } else if (!get_primary_zone().empty()) {
    if (OB_FAIL(primary_zone.set_primary_zone(get_primary_zone()))) {
      LOG_WARN("fail to set primary zone", K(ret));
    }
  } else {
    if (OB_FAIL(primary_zone.set_primary_zone(OB_RANDOM_PRIMARY_ZONE))) {
      LOG_WARN("fail to set primary zone", K(ret));
    }
  }
  return ret;
}


void ObSysVarSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  name_.reset();
  data_type_ = ObNullType;
  value_.reset();
  min_val_.reset();
  max_val_.reset();
  schema_version_ = 0;
  info_.reset();
  zone_.reset();
  flags_ = 0;
  ObSchema::reset();
}

int64_t ObSysVarSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += name_.length() + 1;
  convert_size += value_.length() + 1;
  convert_size += min_val_.length() + 1;
  convert_size += max_val_.length() + 1;
  convert_size += info_.length() + 1;
  convert_size += zone_.size();
  return convert_size;
}

ObSysVarSchema::ObSysVarSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

int ObSysVarSchema::assign(const ObSysVarSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    if (!src_schema.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("src schema is invalid", K(ret));
    } else if (OB_FAIL(set_name(src_schema.get_name()))) {
      LOG_WARN("set sysvar name failed", K(ret));
    } else if (OB_FAIL(set_value(src_schema.get_value()))) {
      LOG_WARN("set sysvar value failed", K(ret));
    } else if (OB_FAIL(set_min_val(src_schema.get_min_val()))) {
      LOG_WARN("set sysvar min val failed", K(ret));
    } else if (OB_FAIL(set_max_val(src_schema.get_max_val()))) {
      LOG_WARN("set sysvar max val failed", K(ret));
    } else if (OB_FAIL(set_info(src_schema.get_info()))) {
      LOG_WARN("set sysvar info failed", K(ret));
    } else if (OB_FAIL(set_zone(src_schema.get_zone()))) {
      LOG_WARN("set sysvar zone failed", K(ret));
    } else {
      set_data_type(src_schema.get_data_type());
      set_schema_version(src_schema.get_schema_version());
      set_flags(src_schema.get_flags());
      set_tenant_id(src_schema.get_tenant_id());
    }
    error_ret_ = ret;
  }
  return ret;
}

int ObSysVarSchema::get_value(ObIAllocator *allocator, const ObDataTypeCastParams &dtc_params, ObObj &value) const
{
  int ret = OB_SUCCESS;
  if (ob_is_string_type(data_type_)) {
    value.set_string(data_type_, value_);
    value.set_collation_type(ObCharset::get_system_collation());
  } else if (ObBasicSysVar::is_null_value(value_, flags_)) {
    value.set_null();
  } else {
    ObObj var_value;
    var_value.set_varchar(value_);
    ObCastCtx cast_ctx(allocator, &dtc_params, CM_NONE, ObCharset::get_system_collation());
    ObObj casted_val;
    const ObObj *res_val = NULL;
    if (OB_FAIL(ObObjCaster::to_type(data_type_, cast_ctx, var_value, casted_val, res_val))) {
      _LOG_WARN("failed to cast object, ret=%d cell=%s from_type=%s to_type=%s",
                 ret, to_cstring(var_value), ob_obj_type_str(var_value.get_type()), ob_obj_type_str(data_type_));
    } else if (OB_ISNULL(res_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("casted success, but res_val is NULL", K(ret), K(var_value), K_(data_type));
    } else {
      value = *res_val;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSysVarSchema,
                    tenant_id_,
                    name_,
                    data_type_,
                    value_,
                    min_val_,
                    max_val_,
                    info_,
                    zone_,
                    schema_version_,
                    flags_);

// Used to compare whether the hard-coded content is consistent with the schema, and check the parts except value, zone,
// and schema_version
bool ObSysVarSchema::is_equal_except_value(const ObSysVarSchema &other) const
{
  bool bret = false;
  if (data_type_ == other.data_type_
      && flags_ == other.flags_
      && 0 == name_.compare(other.name_)
      && 0 == min_val_.compare(other.min_val_)
      && 0 == max_val_.compare(other.max_val_)
      && 0 == info_.compare(other.info_)) {
    bret = true;
  }
  return bret;
}

bool ObSysVarSchema::is_equal_for_add(const ObSysVarSchema &other) const
{
  bool bret = false;
  if (is_equal_except_value(other)
      && 0 == value_.compare(other.value_)
      && zone_ == other.zone_) {
    bret = true;
  }
  return bret;
}
/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObDatabaseSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObDatabaseSchema::ObDatabaseSchema()
  : ObSchema()
{
  reset();
}

ObDatabaseSchema::ObDatabaseSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObDatabaseSchema::ObDatabaseSchema(const ObDatabaseSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObDatabaseSchema::~ObDatabaseSchema()
{
}

ObDatabaseSchema &ObDatabaseSchema::operator =(const ObDatabaseSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_database_id(src_schema.database_id_);
    set_schema_version(src_schema.schema_version_);
    set_charset_type(src_schema.charset_type_);
    set_collation_type(src_schema.collation_type_);
    set_name_case_mode(src_schema.name_case_mode_);
    set_read_only(src_schema.read_only_);
    set_default_tablegroup_id(src_schema.default_tablegroup_id_);
    set_in_recyclebin(src_schema.is_in_recyclebin());

    if (OB_FAIL(set_database_name(src_schema.database_name_))) {
      LOG_WARN("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_comment(src_schema.comment_))) {
      LOG_WARN("set_comment failed", K(ret));
    } else if (OB_FAIL(set_default_tablegroup_name(src_schema.default_tablegroup_name_))) {
      LOG_WARN("set_comment failed", K(ret));
    } else {} // no more to do

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObDatabaseSchema::assign(const ObDatabaseSchema &src_schema) {
  int ret = OB_SUCCESS;
  *this = src_schema;
  ret = get_err_ret();
  return ret;
}

int64_t ObDatabaseSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += database_name_.length() + 1;
  convert_size += comment_.length() + 1;
  return convert_size;
}

bool ObDatabaseSchema::is_valid() const
{
  return ObSchema::is_valid() && common::OB_INVALID_ID != tenant_id_
      && common::OB_INVALID_ID != database_id_ && schema_version_ > 0;
}

void ObDatabaseSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  reset_string(database_name_);
  reset_string(comment_);
  charset_type_ = common::CHARSET_INVALID;
  collation_type_ = common::CS_TYPE_INVALID;
  name_case_mode_ = OB_NAME_CASE_INVALID;
  read_only_ = false;
  default_tablegroup_id_ = OB_INVALID_ID;
  reset_string(default_tablegroup_name_);
  in_recyclebin_ = false;
  ObSchema::reset();
}

int ObDatabaseSchema::get_zone_replica_attr_array_inherit(
    share::schema::ObSchemaGetterGuard &schema_guard,
    ZoneLocalityIArray &locality) const
{
  int ret = OB_SUCCESS;
  locality.reset();
  const ObTenantSchema *tenant_schema = NULL;
  common::ObArray<share::ObZoneReplicaAttrSet> tenant_locality;
  if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(database_id_), K(tenant_id_));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(database_id_), K(tenant_id_), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(tenant_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_locality.count(); ++i) {
      if (OB_FAIL(locality.push_back(tenant_locality.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObDatabaseSchema::get_paxos_replica_num(
    share::schema::ObSchemaGetterGuard &schema_guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  num = 0;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(database_id_), K(tenant_id_));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(database_id_), K(tenant_id_), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret) {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get valid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_paxos_replica_num();
      }
    }
  }
  return ret;
}

int ObDatabaseSchema::get_zone_list(
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  zone_list.reset();
  if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(database_id_), K(tenant_id_));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(database_id_), K(tenant_id_), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else {} // no more to do
  return ret;
}

OB_DEF_SERIALIZE(ObDatabaseSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_,
              database_id_, schema_version_, database_name_,
              comment_, charset_type_, collation_type_, name_case_mode_, read_only_,
              default_tablegroup_id_, default_tablegroup_name_, in_recyclebin_);
  if (OB_FAIL(ret)) {
    LOG_WARN("func_SERIALIZE failed", K(ret));
  } else {} // no more to do
  return ret;
}

OB_DEF_DESERIALIZE(ObDatabaseSchema)
{
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString comment;
  ObString default_tablegroup_name;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_,
              database_id_, schema_version_, database_name,
              comment, charset_type_, collation_type_, name_case_mode_, read_only_,
              default_tablegroup_id_, default_tablegroup_name, in_recyclebin_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(set_database_name(database_name))) {
    LOG_WARN("set_tenant_name failed", K(ret));
  } else if (OB_FAIL(set_comment(comment))) {
    LOG_WARN("set_comment failed", K(ret));
  } else if (OB_FAIL(set_default_tablegroup_name(default_tablegroup_name))) {
    LOG_WARN("set_comment failed", K(ret));
  } else {} // no more to do
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatabaseSchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_, database_id_, schema_version_,
              database_name_,
              comment_, charset_type_, collation_type_,
              name_case_mode_, read_only_, default_tablegroup_id_,
              default_tablegroup_name_, in_recyclebin_);

  return len;
}

int ObDatabaseSchema::get_first_primary_zone_inherit(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
    common::ObZone &first_primary_zone) const
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObDatabaseSchema::get_primary_zone_inherit(
    ObSchemaGetterGuard &schema_guard,
    ObPrimaryZone &primary_zone) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_primary_zone = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_;
  primary_zone.reset();
  if (!use_tenant_primary_zone) {
    const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, simple_tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
    } else {
      use_tenant_primary_zone = simple_tenant->is_restore();
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(database_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(database_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_primary_zone_inherit(schema_guard, primary_zone))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(database_id_), K(tenant_id_));
    }
  }
  return ret;
}
/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObLocality-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
void ObLocality::reset_zone_replica_attr_array()
{
  if (NULL != schema_ && NULL != zone_replica_attr_array_.get_base_address()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
      SchemaZoneReplicaAttrSet &zone_locality = zone_replica_attr_array_.at(i);
      schema_->reset_string_array(zone_locality.zone_set_);
      SchemaReplicaAttrArray &full_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_full_replica_attr_array());
      if (nullptr != full_attr_set.get_base_address()) {
        schema_->free(full_attr_set.get_base_address());
        full_attr_set.reset();
      }
      SchemaReplicaAttrArray &logonly_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_logonly_replica_attr_array());
      if (nullptr != logonly_attr_set.get_base_address()) {
        schema_->free(logonly_attr_set.get_base_address());
        logonly_attr_set.reset();
      }
      SchemaReplicaAttrArray &readonly_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_readonly_replica_attr_array());
      if (nullptr != readonly_attr_set.get_base_address()) {
        schema_->free(readonly_attr_set.get_base_address());
        readonly_attr_set.reset();
      }
      SchemaReplicaAttrArray &encryption_logonly_attr_set
        = static_cast<SchemaReplicaAttrArray &>(zone_locality.replica_attr_set_.get_encryption_logonly_replica_attr_array());
      if (nullptr != encryption_logonly_attr_set.get_base_address()) {
        schema_->free(encryption_logonly_attr_set.get_base_address());
        encryption_logonly_attr_set.reset();
      }
    }
    schema_->free(zone_replica_attr_array_.get_base_address());
    zone_replica_attr_array_.reset();
  }
}

int ObLocality::set_specific_replica_attr_array(
    SchemaReplicaAttrArray &this_schema_set,
    const common::ObIArray<ReplicaAttr> &src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.count();
  if (count > 0) {
    const int64_t size = count * static_cast<int64_t>(sizeof(share::ReplicaAttr));
    void *ptr = nullptr;
    if (nullptr == (ptr = schema_->alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(size));
    } else if (FALSE_IT(this_schema_set.init(count, static_cast<ReplicaAttr *>(ptr), count))) {
      // shall never by here
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
        const share::ReplicaAttr &src_replica_attr = src.at(i);
        ReplicaAttr *dst_replica_attr = &this_schema_set.at(i);
        if (nullptr == (dst_replica_attr = new (dst_replica_attr) ReplicaAttr(
                src_replica_attr.num_, src_replica_attr.memstore_percent_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("placement new return nullptr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocality::set_zone_replica_attr_array(const common::ObIArray<SchemaZoneReplicaAttrSet> &src)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else {
    reset_zone_replica_attr_array();
    const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
    void *buf = NULL;
    if (src.count() <= 0) {
      // do nothing
    } else if (NULL == (buf = schema_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(alloc_size));
    } else {
      zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet *>(buf), src.count());
      // call construct func in advance to avoid core status
      //
      ARRAY_NEW_CONSTRUCT(SchemaZoneReplicaAttrSet, zone_replica_attr_array_);
      for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
        const SchemaZoneReplicaAttrSet &src_replica_attr_set = src.at(i);
        SchemaZoneReplicaAttrSet *this_schema_set = &zone_replica_attr_array_.at(i);
        if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_encryption_logonly_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_encryption_logonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(schema_->deep_copy_string_array(
                src_replica_attr_set.zone_set_, this_schema_set->zone_set_))) {
          LOG_WARN("fail to copy schema replica attr set zone set", K(ret));
        } else {
          this_schema_set->zone_ = src_replica_attr_set.zone_;
        }
      }
    }
  }
  return ret;
}

int ObLocality::set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet> &src)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else {
    reset_zone_replica_attr_array();
    const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
    void *buf = NULL;
    if (src.count() <= 0) {
      // do nothing
    } else if (NULL == (buf = schema_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(alloc_size));
    } else {
      zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet *>(buf), src.count());
      // call construct func in advance to avoid core status
      //
      ARRAY_NEW_CONSTRUCT(SchemaZoneReplicaAttrSet, zone_replica_attr_array_);
      for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
        const share::ObZoneReplicaAttrSet &src_replica_attr_set = src.at(i);
        SchemaZoneReplicaAttrSet *this_schema_set = &zone_replica_attr_array_.at(i);
        if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                static_cast<SchemaReplicaAttrArray &>(this_schema_set->replica_attr_set_.get_encryption_logonly_replica_attr_array()),
                src_replica_attr_set.replica_attr_set_.get_encryption_logonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else {
          common::ObArray<common::ObString> zone_set_ptrs;
          for (int64_t j = 0; OB_SUCC(ret) && j < src_replica_attr_set.zone_set_.count(); ++j) {
            const common::ObZone &zone = src_replica_attr_set.zone_set_.at(j);
            if (OB_FAIL(zone_set_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
               LOG_WARN("fail to push back", K(ret));
            } else {} // no more to do
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(schema_->deep_copy_string_array(zone_set_ptrs, this_schema_set->zone_set_))) {
            LOG_WARN("fail to copy schema replica attr set zone set", K(ret));
          } else {
            this_schema_set->zone_ = src_replica_attr_set.zone_;
          }
        }
      }
    }
  }
  return ret;
}

int ObLocality::assign(const ObLocality &other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else if (OB_FAIL(schema_->deep_copy_str(other.locality_str_, locality_str_))) {
    LOG_WARN("fail to assign locality info", K(ret));
  } else if (OB_FAIL(set_zone_replica_attr_array(other.zone_replica_attr_array_))) {
    LOG_WARN("set zone replica attr array failed", K(ret));
  }
  return ret;
}

int ObLocality::set_locality_str(const ObString &other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else if (OB_FAIL(schema_->deep_copy_str(other, locality_str_))) {
    LOG_WARN("fail to assign locality info", K(ret));
  }
  return ret;
}

int64_t ObLocality::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += zone_replica_attr_array_.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    convert_size += zone_replica_attr_array_.at(i).get_convert_size();
  }
  convert_size += locality_str_.length() + 1;
  return convert_size;
}

void ObLocality::reset()
{
  if (OB_ISNULL(schema_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid schema info", K(schema_));
  } else {
    reset_zone_replica_attr_array();
    if (!OB_ISNULL(locality_str_.ptr())) {
      schema_->free(locality_str_.ptr());
    }
    locality_str_.reset();
  }
}

OB_DEF_SERIALIZE(ObLocality)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, locality_str_);
  if (OB_FAIL(ret)) {
    LOG_WARN("func_SERIALIZE failed", K(ret));
  } else {} // no more to do
  return ret;
}

OB_DEF_DESERIALIZE(ObLocality)
{
  int ret = OB_SUCCESS;
  ObString locality;
  LST_DO_CODE(OB_UNIS_DECODE, locality);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema_ info", K(ret), K(schema_));
  } else if (OB_FAIL(schema_->deep_copy_str(locality, locality_str_))) {
    LOG_WARN("fail to deep copy str", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLocality)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, locality_str_);
  return len;
}


/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObPrimaryZone-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObPrimaryZone::ObPrimaryZone()
  : allocator_(NULL),
    primary_zone_str_(),
    primary_zone_array_()
{
}

ObPrimaryZone::ObPrimaryZone(common::ObIAllocator &alloc)
  : allocator_(&alloc),
    primary_zone_str_(),
    primary_zone_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator_))
{
}

void ObPrimaryZone::set_allocator(common::ObIAllocator *allocator)
{
  if (OB_NOT_NULL(allocator)) {
    allocator_ = allocator;
    primary_zone_array_.set_block_allocator(ModulePageAllocator(*allocator_));
  }
}

int ObPrimaryZone::set_primary_zone_array(
    const common::ObIArray<ObZoneScore> &primary_zone_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(allocator));
  } else {
    primary_zone_array_.reset();
    int64_t count = primary_zone_array.count();
    if (OB_FAIL(primary_zone_array_.reserve(count))) {
      LOG_WARN("fail to reserve array", KR(ret), K(count));
    }
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      const ObZoneScore &zone_score = primary_zone_array.at(i);
      ObString str;
      if (OB_FAIL(ob_write_string(*allocator_, zone_score.zone_, str, true))) {
        LOG_WARN("deep copy str failed", KR(ret));
      } else if (OB_FAIL(primary_zone_array_.push_back(
                  ObZoneScore(str, zone_score.score_)))) {
        LOG_WARN("fail to push back", KR(ret));
        for (int64_t j = 0; j < primary_zone_array_.count(); ++j) {
          allocator_->free(primary_zone_array_.at(j).zone_.ptr());
        }
        allocator_->free(str.ptr());
      } else {} // ok
    }
  }
  return ret;
}

int ObPrimaryZone::set_primary_zone(const ObString &zone)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP_(allocator));
  } else if (OB_FAIL(ob_write_string(*allocator_, zone, primary_zone_str_, true))) {
    LOG_WARN("fail to assign primary zone", K(ret));
  }
  return ret;
}

int ObPrimaryZone::assign(const ObPrimaryZone &other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP_(allocator));
  } else if (OB_FAIL(set_primary_zone(other.primary_zone_str_))) {
    LOG_WARN("fail to assign primary zone", K(ret));
  } else if (OB_FAIL(set_primary_zone_array(other.primary_zone_array_))) {
    LOG_WARN("fail to set primary zone array", K(ret));
  }
  return ret;
}

int64_t ObPrimaryZone::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += primary_zone_str_.length() + 1;
  convert_size += primary_zone_array_.get_data_size();
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    convert_size += primary_zone_array_.at(i).zone_.length() + 1;
  }
  return convert_size;
}

void ObPrimaryZone::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    if (!OB_ISNULL(primary_zone_str_.ptr())) {
      allocator_->free(primary_zone_str_.ptr());
    }
    for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
      if (!OB_ISNULL(primary_zone_array_.at(i).zone_.ptr())) {
        allocator_->free(primary_zone_array_.at(i).zone_.ptr());
      }
    }
  }
  primary_zone_str_.reset();
  primary_zone_array_.reset();
}


OB_DEF_SERIALIZE(ObPrimaryZone)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, primary_zone_str_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPrimaryZone)
{
  int ret = OB_SUCCESS;
  ObString primary_zone;
  LST_DO_CODE(OB_UNIS_DECODE, primary_zone);
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, primary_zone, primary_zone_str_))) {
    LOG_WARN("fail to deep copy str", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPrimaryZone)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, primary_zone_str_);
  return len;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObPartitionSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObPartitionSchema::ObPartitionSchema()
    : ObSchema()
{
  reset();
}

ObPartitionSchema::ObPartitionSchema(common::ObIAllocator *allocator)
    : ObSchema(allocator),
      part_option_(allocator),
      sub_part_option_(allocator)
{
  reset();
}

ObPartitionSchema::ObPartitionSchema(const ObPartitionSchema &src_schema)
    : ObSchema()
{
  reset();
  *this = src_schema;
}

ObPartitionSchema::~ObPartitionSchema()
{
}

ObPartitionSchema &ObPartitionSchema::operator =(const ObPartitionSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    if (OB_FAIL(assign_partition_schema(src_schema))) {
      LOG_WARN("failed to assign partition schema", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

void ObPartitionSchema::reuse_partition_schema()
{
  // Note: Do not directly call the reset of the base class
  // Here will reset the allocate in ObSchema, which will affect other variables
  // very dangerous
  part_level_ = PARTITION_LEVEL_ZERO;
  part_option_.reuse();
  sub_part_option_.reuse();
  partition_array_ = NULL;
  partition_array_capacity_ = 0;
  partition_num_ = 0;
  def_subpartition_array_ = NULL;
  def_subpartition_num_ = 0;
  def_subpartition_array_capacity_ = 0;
  partition_schema_version_ = 0;
  partition_status_ = PARTITION_STATUS_ACTIVE;
  sub_part_template_flags_ = 0;
  hidden_partition_array_ = NULL;
  hidden_partition_array_capacity_ = 0;
  hidden_partition_num_ = 0;
  transition_point_.reset();
  interval_range_.reset();
}

int ObPartitionSchema::assign_partition_schema(const ObPartitionSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reuse_partition_schema();
    part_level_ = src_schema.part_level_;
    partition_schema_version_ = src_schema.partition_schema_version_;
    partition_status_ = src_schema.partition_status_;
    sub_part_template_flags_ = src_schema.sub_part_template_flags_;
    if (OB_SUCC(ret)) {
      part_option_ = src_schema.part_option_;
      if (OB_FAIL(part_option_.get_err_ret())) {
        LOG_WARN("fail to assign part_option", K(ret),
                 K_(part_option), K(src_schema.part_option_));
      }
    }

    if (OB_SUCC(ret)) {
      sub_part_option_ = src_schema.sub_part_option_;
      if (OB_FAIL(sub_part_option_.get_err_ret())) {
        LOG_WARN("fail to assign sub_part_option", K(ret),
                 K_(sub_part_option), K(src_schema.sub_part_option_));
      }
    }

#define ASSIGN_PARTITION_ARRAY(PART_NAME) \
    if (OB_SUCC(ret)) { \
      int64_t partition_num = src_schema.PART_NAME##_num_; \
      if (partition_num > 0) { \
        if (OB_FAIL(preserve_array(PART_NAME##_array_, PART_NAME##_array_capacity_, partition_num))) { \
          LOG_WARN("Fail to preserve "#PART_NAME" array", KR(ret), KP(PART_NAME##_array_), K(PART_NAME##_array_capacity_), K(partition_num)); \
        } else if (OB_ISNULL(src_schema.PART_NAME##_array_)) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("src_schema."#PART_NAME"_array_ is null", K(ret)); \
        } \
      } \
      ObPartition *partition = NULL; \
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) { \
        partition = src_schema.PART_NAME##_array_[i]; \
        if (OB_ISNULL(partition)) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("the partition is null", K(ret)); \
        } else if (OB_FAIL(add_partition(*partition))) { \
          LOG_WARN("Fail to add partition", K(ret), K(i)); \
        } \
      } \
    }
    ASSIGN_PARTITION_ARRAY(partition);
    ASSIGN_PARTITION_ARRAY(hidden_partition);
#undef ASSIGN_PARTITION_ARRAY
    //def subpartitions array
    if (OB_SUCC(ret)) {
      int64_t def_subpartition_num = src_schema.def_subpartition_num_;
      if (def_subpartition_num > 0) {
        if(OB_FAIL(preserve_array(def_subpartition_array_, def_subpartition_array_capacity_, def_subpartition_num))) {
          LOG_WARN("fail to preserve def_subpartition_array", KR(ret), KP(def_subpartition_array_), K(def_subpartition_array_capacity_), K(def_subpartition_num));
        } else if (OB_ISNULL(src_schema.def_subpartition_array_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src_schema.def_subpartition_array_ is null", K(ret));
        }
      }
      ObSubPartition *subpartition = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < def_subpartition_num; i++) {
        subpartition = src_schema.def_subpartition_array_[i];
        if (OB_ISNULL(subpartition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the partition is null", K(ret));
        } else if (OB_FAIL(add_def_subpartition(*subpartition))) {
          LOG_WARN("Fail to add partition", K(ret), K(i));
        }
      }
    }

    if (FAILEDx(set_transition_point(src_schema.get_transition_point()))) {
      LOG_WARN("fail to set transition point", K(ret));
    } else if (OB_FAIL(set_interval_range(src_schema.get_interval_range()))) {
      LOG_WARN("fail to set interval range", K(ret));
    }
  }

  return ret;
}

int ObPartitionSchema::try_assign_part_array(
    const share::schema::ObPartitionSchema &that)
{
  int ret = OB_SUCCESS;
  if (nullptr != partition_array_) {
    for (int64_t i = 0; i < partition_num_; ++i) {
      ObPartition *this_part = partition_array_[i];
      if (nullptr != this_part) {
        this_part->~ObPartition();
        free(this_part);
        this_part = nullptr;
      }
    }
    free(partition_array_);
    partition_array_ = nullptr;
    partition_num_ = 0;
  }
  part_level_ = that.get_part_level();
  part_option_ = that.get_part_option();
  if (OB_FAIL(part_option_.get_err_ret())) {
    LOG_WARN("fail to assign part option", K(ret), K(part_option_));
  } else {
    int64_t partition_num = that.get_partition_num();
    if (partition_num > 0) {
      partition_array_ = static_cast<ObPartition **>(alloc(sizeof(ObPartition *)
          * partition_num));
      if (OB_ISNULL(partition_array_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for partition_array_", K(ret));
      } else if (OB_ISNULL(that.get_part_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("that partition_array_ is null", K(ret));
      } else {
        partition_array_capacity_ = partition_num;
      }
    }
    ObPartition *partition = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      partition = that.get_part_array()[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition is null", K(ret));
      } else if (partition->get_part_name().empty()) {
        if (OB_FAIL(add_partition(*partition))) {
          LOG_WARN("Fail to add partition", K(ret));
        }
      } else if (OB_FAIL(add_partition(*partition))) {
        LOG_WARN("Fail to add partition", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    error_ret_ = ret;
  }
  return ret;
}

int ObPartitionSchema::try_assign_def_subpart_array(
    const share::schema::ObPartitionSchema &that)
{
  int ret = OB_SUCCESS;
  if (nullptr != def_subpartition_array_) {
    for (int64_t i = 0; i < def_subpartition_num_; ++i) {
      ObSubPartition *this_part = def_subpartition_array_[i];
      if (nullptr != this_part) {
        this_part->~ObSubPartition();
        free(this_part);
        this_part = nullptr;
      }
    }
    free(def_subpartition_array_);
    def_subpartition_array_ = nullptr;
    def_subpartition_num_ = 0;
  }
  part_level_ = that.get_part_level();
  sub_part_option_ = that.get_sub_part_option();
  if (OB_FAIL(sub_part_option_.get_err_ret())) {
    LOG_WARN("fail to assign part option", K(ret), K(sub_part_option_));
  } else {
    int64_t def_subpartition_num = that.get_def_subpartition_num();
    if (def_subpartition_num > 0) {
      def_subpartition_array_ = static_cast<ObSubPartition **>(
          alloc(sizeof(ObSubPartition *) * def_subpartition_num));
      if (OB_ISNULL(def_subpartition_array_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for def_subpartition_array_", K(ret), K(def_subpartition_num));
      } else if (OB_ISNULL(that.get_def_subpart_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("that def_subpartition_array_ is null", K(ret));
      } else {
        def_subpartition_array_capacity_ = def_subpartition_num;
      }
    }
    ObSubPartition *subpartition = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < def_subpartition_num; i++) {
      subpartition = that.get_def_subpart_array()[i];
      if (OB_ISNULL(subpartition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition is null", K(ret));
      } else if (OB_FAIL(add_def_subpartition(*subpartition))) {
        LOG_WARN("Fail to add partition", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    error_ret_ = ret;
  }
  return ret;
}

int ObPartitionSchema::try_generate_hash_part()
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == part_level_) {
    // skip
  } else if (OB_NOT_NULL(get_part_array())) {
    // skip
  } else if (is_hash_like_part()) {
    bool is_oracle_mode = false;
    const int64_t BUF_SIZE = OB_MAX_PARTITION_NAME_LENGTH;
    char buf[BUF_SIZE];
    const int64_t &first_part_num = get_first_part_num();
    if (OB_UNLIKELY(first_part_num <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part_option is invalid", KR(ret), KPC(this));
    } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check if oracle mode", KR(ret), KPC(this));
    } else if (OB_FAIL(preserve_array(partition_array_, partition_array_capacity_, first_part_num))) {
      LOG_WARN("fail to preserve partition array", KR(ret), KP(partition_array_), K(partition_array_capacity_), K(first_part_num));
    } else {
      ObPartition part;
      for (int64_t i = 0; OB_SUCC(ret) && i < first_part_num; i++) {
        ObString part_name;
        part.reset();
        MEMSET(buf, 0, BUF_SIZE);
        if (OB_FAIL(ObPartitionSchema::gen_hash_part_name(
            i, FIRST_PART, is_oracle_mode, buf, BUF_SIZE, NULL, NULL))) {
          LOG_WARN("fail to get part name", KR(ret), K(i));
        } else if (FALSE_IT(part_name.assign_ptr(buf, static_cast<int32_t>(strlen(buf))))) {
        } else if (OB_FAIL(part.set_part_name(part_name))) {
          LOG_WARN("fail to set part name", KR(ret), K(part_name));
        } else if (OB_FAIL(add_partition(part))) {
          LOG_WARN("fail to add partition", KR(ret), K(ret));
        }
      } // end for
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_array should not be null", KR(ret), KPC(this));
  }
  return ret;
}

int ObPartitionSchema::try_generate_hash_subpart(bool &generated)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const int64_t part_num = get_partition_num();
  ObPartition **part_array = get_part_array();
  const int64_t def_subpart_num = get_def_sub_part_num();
  int64_t all_partition_num = 0;
  generated = false;
  if (PARTITION_LEVEL_TWO != part_level_) {
    // skip
  } else if (!is_hash_like_subpart()) {
    // skip
  } else if (OB_FAIL(get_all_partition_num(
             ObCheckPartitionMode::CHECK_PARTITION_MODE_NORMAL, all_partition_num))) {
    LOG_WARN("fail to get partition num", KR(ret), K(all_partition_num));
  } else if (all_partition_num > 0) {
    // skip, this means each part has no subpartitions.
  } else if (OB_ISNULL(part_array) || part_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_array is null or part_num is invalid",
             KR(ret), KP(part_array), K(part_num));
  } else if (def_subpart_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("def_subpart_num is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check if oracle mode", KR(ret), KPC(this));
  } else {
    const int64_t BUF_SIZE = OB_MAX_PARTITION_NAME_LENGTH;
    char buf[BUF_SIZE];
    ObSubPartition subpart;
    // 1. try generate def_sub_part_array()
    if (OB_ISNULL(get_def_subpart_array())) {
      if (OB_FAIL(preserve_array(def_subpartition_array_, def_subpartition_array_capacity_, def_subpart_num))) {
        LOG_WARN("fail to preserve def subpartition array", KR(ret), KP(def_subpartition_array_), K(def_subpartition_array_capacity_), K(def_subpart_num));
      }
      for (int64_t j = 0; j < def_subpart_num && OB_SUCC(ret); j++) {
        MEMSET(buf, 0, BUF_SIZE);
        ObString sub_part_name;
        subpart.reset();
        if (OB_FAIL(gen_hash_part_name(j, TEMPLATE_SUB_PART,
                    is_oracle_mode, buf, BUF_SIZE, NULL, NULL))) {
          LOG_WARN("fail to get def subpart name", KR(ret), K(j));
        } else if (FALSE_IT(sub_part_name.assign_ptr(buf, static_cast<int32_t>(strlen(buf))))) {
        } else if (OB_FAIL(subpart.set_part_name(sub_part_name))) {
          LOG_WARN("set subpart name failed", KR(ret), K(sub_part_name), KPC(this));
        } else if (OB_FAIL(add_def_subpartition(subpart))) {
          LOG_WARN("failed to add partition", KR(ret), K(subpart));
        } else {
          generated = true;
        }
      } // end for
    }
    // 2. generate hash subpart by template
    if (FAILEDx(try_generate_subpart_by_template(generated))) {
      LOG_WARN("fail to generate subpart by template", KR(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::try_generate_subpart_by_template(bool &generated)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const int64_t part_num = get_partition_num();
  ObPartition **part_array = get_part_array();
  const int64_t def_subpart_num = get_def_subpartition_num();
  ObSubPartition **def_subpart_array = get_def_subpart_array();
  int64_t all_partition_num = 0;
  generated = false;
  if (PARTITION_LEVEL_TWO != get_part_level()) {
    // skip
  } else if (OB_FAIL(get_all_partition_num(
             ObCheckPartitionMode::CHECK_PARTITION_MODE_NORMAL, all_partition_num))) {
    LOG_WARN("fail to get partition num", KR(ret), K(all_partition_num));
  } else if (all_partition_num > 0) {
    // skip, this means each part has no subpartitions.
  } else if (OB_ISNULL(part_array) || part_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_array is null or part_num is invalid",
             KR(ret), KP(part_array), K(part_num));
  } else if (OB_ISNULL(def_subpart_array) || def_subpart_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("def_subpart_array is null or def_subpart_num is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check if oracle mode", KR(ret), KPC(this));
  } else {
    const int64_t BUF_SIZE = OB_MAX_PARTITION_NAME_LENGTH;
    char buf[BUF_SIZE];
    ObSubPartition subpart;
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      ObPartition *part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(i), K(part_num), KPC(this));
      } else if (part->get_subpartition_num() > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition num should be 0", KR(ret), KPC(part));
      } else if (OB_FAIL(part->preserve_subpartition(def_subpart_num))) {
        LOG_WARN("fail to preserve subpartition", KR(ret), K(def_subpart_num));
      } else {
        part->set_sub_part_num(def_subpart_num);
        for (int64_t j = 0; j < def_subpart_num && OB_SUCC(ret); j++) {
          MEMSET(buf, 0, BUF_SIZE);
          int64_t pos = 0;
          ObString sub_part_name;
          subpart.reset();
          if (OB_ISNULL(def_subpart_array[j])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), K(i), K(j), K(def_subpart_num), KPC(this));
          } else if (OB_FAIL(subpart.assign(*def_subpart_array[j]))) {
            LOG_WARN("fail to assign subpart", KR(ret));
          } else if (OB_FAIL(databuff_printf(buf, BUF_SIZE, pos, "%s%s%s",
                     part->get_part_name().ptr(), is_oracle_mode ? "S" : "s",
                     def_subpart_array[j]->get_part_name().ptr()))) {
            LOG_WARN("part name is too long", KR(ret), KPC(part), K(subpart));
          } else if (FALSE_IT(sub_part_name.assign_ptr(buf, static_cast<int32_t>(strlen(buf))))) {
          } else if (OB_FAIL(subpart.set_part_name(sub_part_name))) {
            LOG_WARN("set subpart name failed", KR(ret), K(sub_part_name), KPC(this));
          } else if (OB_FAIL(part->add_partition(subpart))) {
            LOG_WARN("failed to add partition", KR(ret), K(subpart));
          } else {
            generated = true;
          }
        } // end for subpart
      }
    } // end for part
    if (OB_FAIL(ret)) {
      generated = false;
    }
  }
  return ret;
}

int ObPartitionSchema::try_init_partition_idx()
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = get_part_level();
  if (PARTITION_LEVEL_ZERO == part_level) {
    // skip
  } else {
    bool part_idx_valid = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_partition_num(); i++) {
      ObPartition *part = get_part_array()[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", KR(ret), KP(part), K(i));
      } else if (0 == i) {
        part_idx_valid = (part->get_part_idx() >= 0);
      } else if ((part_idx_valid && part->get_part_idx() < 0)
                 || (!part_idx_valid && part->get_part_idx() >= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all part_idx should be valid or not", KR(ret), K(i), KPC(part));
      }
      if (OB_SUCC(ret)) {
        if (!part_idx_valid) {
          part->set_part_idx(i);
        }
        if (PARTITION_LEVEL_TWO == part_level) {
          bool subpart_idx_valid = false;
          for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); j++) {
            ObSubPartition *subpart = part->get_subpart_array()[j];
            if (OB_ISNULL(subpart)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("subpart is null", KR(ret), K(j), KPC(part));
            } else if (0 == j) {
              subpart_idx_valid = (subpart->get_sub_part_idx() >= 0);
            } else if ((subpart_idx_valid && subpart->get_sub_part_idx() < 0)
                       || (!subpart_idx_valid && subpart->get_sub_part_idx() > 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("all subpart_idx should be valid or not", KR(ret), K(j), KPC(part));
            }
            if (OB_SUCC(ret) && !subpart_idx_valid) {
              subpart->set_sub_part_idx(j);
            }
          } // end for iterate subpart
        }
      }
    } // end for iterate part
  }
  return ret;
}

int ObPartitionSchema::get_max_part_id(int64_t &part_id) const
{
  int ret = OB_SUCCESS;

  if (PARTITION_LEVEL_ZERO == part_level_) {
    part_id = get_object_id();
  } else if (OB_ISNULL(partition_array_)
             || partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_array is null or partition_num is invalid",
             KR(ret), KP_(partition_array), K_(partition_num));
  } else {
    int64_t max_part_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
      const ObPartition *part = partition_array_[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", KR(ret), K(i));
      } else {
        max_part_id = max(max_part_id, part->get_part_id());
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (max_part_id < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max_part_id is invalid", KR(ret), K(max_part_id));
      } else {
        part_id = max_part_id;
      }
    }
  }
  return ret;
}

int ObPartitionSchema::get_max_part_idx(int64_t &part_idx, const bool skip_external_table_default_partition) const
{
  int ret = OB_SUCCESS;
  if (skip_external_table_default_partition && !is_external_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("skip default partition under non-external table is invalid", K(ret));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    // for alter table partition by
    part_idx = 0;
  } else if (OB_ISNULL(partition_array_)
             || partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_array is null or partition_num is invalid",
            KR(ret), KP_(partition_array), K_(partition_num));
  } else {
    int64_t max_part_idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
      const ObPartition *part = partition_array_[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", KR(ret), K(i));
      } else {
        if (skip_external_table_default_partition && ObPartitionUtils::is_default_list_part(*part)) {
          if (partition_num_ == 1) {
            max_part_idx = 0;
          }
        } else {
          max_part_idx = max(max_part_idx, part->get_part_idx());
        }
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (max_part_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max_part_idx is invalid", KR(ret), K(max_part_idx));
      } else {
        part_idx = max_part_idx;
      }
    }
  }
  return ret;
}

bool ObPartitionSchema::is_valid() const
{
  return ObSchema::is_valid();
}

void ObPartitionSchema::reset()
{
  reuse_partition_schema();
  ObSchema::reset();
}

int64_t ObPartitionSchema::get_def_sub_part_num() const
{
  int64_t num = 0;
  if (PARTITION_LEVEL_TWO != part_level_) {
    num = OB_INVALID_ID;
  } else {
    num = sub_part_option_.get_part_num();
  }
  return num;
}

int64_t ObPartitionSchema::get_all_part_num() const
{
  int64_t num = 1;
  switch (part_level_) {
    case PARTITION_LEVEL_ZERO: {
      break;
    }
    case PARTITION_LEVEL_ONE: {
      num = get_first_part_num();
      break;
    }
    case PARTITION_LEVEL_TWO: {
      num = 0;
      int64_t partition_num = get_partition_num();
      int ret = OB_SUCCESS;
      if (partition_num <= 0) {
        // resolver may get_all_part_num() with incomplete schema(hash like - * partitioned table)
        LOG_WARN("partition num should greator than 0", K(ret), K(partition_num));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
          const ObPartition *partition = get_part_array()[i];
          if (OB_ISNULL(partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", K(ret), K(i));
          } else {
            num += partition->get_sub_part_num();
          }
        }
      }
      num = OB_FAIL(ret) ? -1 : num;
      break;
    }
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid partition level", K_(part_level));
      break;
    }
  }
  return num;
}

int64_t ObPartitionSchema::get_first_part_num(
    const ObCheckPartitionMode check_partition_mode) const
{
  int64_t part_num = 0;
  if (check_normal_partition(check_partition_mode)) {
    part_num += get_first_part_num();
  }
  if (check_hidden_partition(check_partition_mode)) {
    part_num += get_hidden_partition_num();
  }
  return part_num;
}

int ObPartitionSchema::get_all_partition_num(
    const ObCheckPartitionMode check_partition_mode,
    int64_t &part_num) const
{
  int ret = OB_SUCCESS;
  part_num = 0;
  switch (part_level_) {
    case PARTITION_LEVEL_ZERO: {
      // non-partitioned table won't have hidden partitions.
      if (check_normal_partition(check_partition_mode)) {
        part_num = 1;
      }
      break;
    }
    case PARTITION_LEVEL_ONE: {
      part_num = get_first_part_num(check_partition_mode);
      break;
    }
    case PARTITION_LEVEL_TWO: {
      // partititon_array may contain subpartition_array/hidden_subpartition_array
      for (int64_t i = 0; OB_SUCC(ret) && i < get_partition_num(); i++) {
        const ObPartition *part = get_part_array()[i];
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", KR(ret), K(i));
        } else {
          if (check_normal_partition(check_partition_mode)) {
            part_num += part->get_subpartition_num();
          }
          if (check_hidden_partition(check_partition_mode)) {
            part_num += part->get_hidden_subpartition_num();
          }
        }
      } // end for
      // hidden_partition_array only have hidden_subpartition_array
      if (check_hidden_partition(check_partition_mode)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < get_hidden_partition_num(); i++) {
          const ObPartition *part = get_hidden_part_array()[i];
          if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), K(i));
          } else {
            part_num += part->get_hidden_subpartition_num();
          }
        } // end for
      }
      break;
    }
    default: {
      LOG_WARN("invalid partition level", K_(part_level));
      break;
    }
  }
  return ret;
}

int ObPartitionSchema::add_def_subpartition(const ObSubPartition &subpartition)
{
  int ret = OB_SUCCESS;
  ObSubPartition *local = OB_NEWx(ObSubPartition, (get_allocator()), (get_allocator()));
  if (NULL == local) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(local->assign(subpartition))) {
    LOG_WARN("failed to assign partition", K(ret));
  } else if (OB_FAIL(inner_add_partition(*local,
                     def_subpartition_array_,
                     def_subpartition_array_capacity_,
                     def_subpartition_num_))) {
    LOG_WARN("add subpartition failed", K(subpartition), K(ret));
  }
  return ret;
}

int ObPartitionSchema::check_part_name(const ObPartition &partition)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; ++i) {
    const ObString &part_name = partition.get_part_name();
    if (common::ObCharset::case_insensitive_equal(part_name,
                                                  partition_array_[i]->get_part_name())) {
      ret = OB_ERR_SAME_NAME_PARTITION;
      LOG_WARN("part name is duplicate", K(ret), K(partition), K(i), "exists partition", partition_array_[i]);
      LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, part_name.length(), part_name.ptr());
    }
  }
  return ret;
}

int ObPartitionSchema::add_partition(const ObPartition &partition)
{
  int ret = OB_SUCCESS;
  ObPartition *new_part = OB_NEWx(ObPartition, (get_allocator()), (get_allocator()));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory", K(ret));
  } else if (OB_FAIL(new_part->assign(partition))) {
    LOG_WARN("failed to assign partition", K(ret));
  } else if (OB_FAIL(inner_add_partition(*new_part))) {
    LOG_WARN("add partition failed", KPC(new_part), K(ret));
  } else {
    LOG_TRACE("add partition succ", K(ret), K(partition), KPC(new_part));
  }
  return ret;
}

int ObPartitionSchema::inner_add_partition(const ObPartition &part)
{
  int ret = OB_SUCCESS;
  if (part.is_hidden_partition()) {
    if (OB_FAIL(inner_add_partition(part,
                                    hidden_partition_array_,
                                    hidden_partition_array_capacity_,
                                    hidden_partition_num_))) {
      LOG_WARN("add hidden partition failed", K(ret), K(part));
    }
  } else {
    if (OB_FAIL(inner_add_partition(part,
                                    partition_array_,
                                    partition_array_capacity_,
                                    partition_num_))) {
      LOG_WARN("add partition failed", K(ret), K(part));
    }
  }
  return ret;
}

template<typename T>
int ObPartitionSchema::inner_add_partition(const T &part, T **&part_array,
                                               int64_t &part_array_capacity,
                                               int64_t &part_num)
{
  int ret = common::OB_SUCCESS;
  if (0 == part_array_capacity) {
    if (NULL == (part_array = static_cast<T **>(alloc(sizeof(T *) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition arrary");
    } else {
      part_array_capacity = DEFAULT_ARRAY_CAPACITY;
    }
  } else if (part_num >= part_array_capacity) {
    int64_t new_size = 2 * part_array_capacity;
    T **tmp = NULL;
    if (NULL == (tmp = static_cast<T **>(alloc((sizeof(T *) * new_size))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition array", K(new_size));
    } else {
      MEMCPY(tmp, part_array, sizeof(T *) * part_num);
      MEMSET(tmp + part_num, 0, sizeof(T *) * (new_size - part_num));
      free(part_array);
      part_array = tmp;
      part_array_capacity = new_size;
    }
  }
  if (OB_SUCC(ret)) {
    part_array[part_num] = const_cast<T *>(&part);
    ++part_num;
  }
  return ret;
}

int ObPartitionSchema::serialize_partitions(char *buf,
    const int64_t data_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, partition_num_))) {
    LOG_WARN("Fail to encode partition count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
    if (OB_ISNULL(partition_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_array_ element is null", K(ret));
    } else if (OB_FAIL(partition_array_[i]->serialize(buf, data_len, pos))) {
      LOG_WARN("Fail to serialize partition", K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::serialize_def_subpartitions(char *buf,
    const int64_t data_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, def_subpartition_num_))) {
    LOG_WARN("Fail to encode subpartition count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < def_subpartition_num_; i++) {
    if (OB_ISNULL(def_subpartition_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("def_subpartition_array_ element is null", K(ret));
    } else if (OB_FAIL(def_subpartition_array_[i]->serialize(buf, data_len, pos))) {
      LOG_WARN("Fail to serialize partition", K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::deserialize_partitions(const char *buf,
    const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("Fail to decode partition count", K(ret));
  } else {
    ObPartition partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      partition.reset();
      if (OB_FAIL(partition.deserialize(buf, data_len, pos))) {
        LOG_WARN("Fail to deserialize partition", K(ret));
      } else if (OB_FAIL(add_partition(partition))) {
        LOG_WARN("Fail to add partition", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionSchema::deserialize_def_subpartitions(const char *buf,
    const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("Fail to decode partition count", K(ret));
  } else {
    ObSubPartition subpartition;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      subpartition.reset();
      if (OB_FAIL(subpartition.deserialize(buf, data_len, pos))) {
        LOG_WARN("Fail to deserialize subpartition", K(ret));
      } else if (OB_FAIL(add_def_subpartition(subpartition))) {
        LOG_WARN("Fail to add subpartition", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionSchema::set_transition_point(const ObRowkey &transition_point)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (transition_point.get_obj_cnt() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transition_point cannot have more than one obj", KR(ret), K(transition_point));
  } else if (OB_FAIL(transition_point.deep_copy(transition_point_, *allocator))) {
    LOG_WARN("Fail to deep copy transition_point_", K(ret));
  } else { }
  return ret;
}

int ObPartitionSchema::get_transition_point_str(char *buf, const int64_t buf_size, int64_t &len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is NULL", K(buf), KR(ret));
  } else if (buf_size < OB_MAX_B_HIGH_BOUND_VAL_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf_size is less OB_MAX_B_HIGH_BOUND_VAL_LENGTH", K(buf_size), KR(ret));
  } else {
    MEMSET(buf, 0, buf_size);
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    bool is_oracle_mode = false;
    const uint64_t tenant_id = get_tenant_id();
    if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
               is_oracle_mode, transition_point_, buf,
               buf_size, len, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::set_transition_point_with_hex_str(
    const common::ObString &transition_point_hex)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  char *serialize_buf = NULL;
  int64_t pos = 0;
  const int64_t hex_length = transition_point_hex.length();
  int64_t serialize_len = hex_length / 2;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if ((hex_length % 2) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hex str length should be even", K(ret));
  } else if (OB_UNLIKELY(NULL == (serialize_buf = static_cast<char *>(allocator->alloc(
                         serialize_len))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for default value buffer failed", K(ret), K(serialize_len));
  } else if (OB_UNLIKELY(hex_length != str_to_hex(
       transition_point_hex.ptr(), static_cast<int32_t>(hex_length),
       serialize_buf, static_cast<int32_t>(serialize_len)))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("Failed to get hex_str buf", K(ret));
  } else if (OB_FAIL(transition_point_.deserialize(*allocator, serialize_buf, serialize_len, pos))) {
    LOG_WARN("Failed to deserialize transition point", K(ret));
  } else { }//do nothing
  return ret;
}

int ObPartitionSchema::set_interval_range(const ObRowkey &interval_range)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (interval_range.get_obj_cnt() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("interval_range cannot have more than one obj", KR(ret), K(interval_range));
  } else if (OB_FAIL(interval_range.deep_copy(interval_range_, *allocator))) {
    LOG_WARN("Fail to deep copy interval_range_", K(ret));
  } else { }
  return ret;
}

int ObPartitionSchema::get_interval_range_str(char *buf, const int64_t buf_size, int64_t &len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is NULL", K(buf), KR(ret));
  } else if (buf_size < OB_MAX_B_HIGH_BOUND_VAL_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf_size is less OB_MAX_B_HIGH_BOUND_VAL_LENGTH", K(buf_size), KR(ret));
  } else {
    MEMSET(buf, 0, buf_size);
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    bool is_oracle_mode = false;
    const uint64_t tenant_id = get_tenant_id();
    if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
               is_oracle_mode, interval_range_, buf,
               buf_size, len, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::set_interval_range_with_hex_str(
    const common::ObString &interval_range_hex)
{
  int ret = OB_SUCCESS;
  char *serialize_buf = NULL;
  ObIAllocator *allocator = get_allocator();
  int64_t pos = 0;
  const int64_t hex_length = interval_range_hex.length();
  int64_t serialize_len = hex_length / 2;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if ((hex_length % 2) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hex str length should be even", K(ret));
  } else if (OB_UNLIKELY(NULL == (serialize_buf = static_cast<char *>(allocator->alloc(
                         serialize_len))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for default value buffer failed", K(ret), K(serialize_len));
  } else if (OB_UNLIKELY(hex_length != str_to_hex(
       interval_range_hex.ptr(), static_cast<int32_t>(hex_length),
       serialize_buf, static_cast<int32_t>(serialize_len)))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("Failed to get hex_str buf", K(ret));
  } else if (OB_FAIL(interval_range_.deserialize(*allocator, serialize_buf, serialize_len, pos))) {
    LOG_WARN("Failed to deserialize interval range", K(ret));
  } else { }//do nothing
  return ret;
}

int ObPartitionSchema::get_interval_parted_range_part_num(uint64_t &part_num) const
{
  int ret = OB_SUCCESS;
  const ObPartition *p = NULL;
  part_num = 0;
  if (is_interval_part()) {
    for (int64_t i = 0; i < partition_num_ && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(p = partition_array_[i])) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("Do not access the null partition object", K(ret), K(i), K(partition_num_));
      } else {
        const ObRowkey &transition_point = get_transition_point();
        const ObRowkey &high_bound_val = p->get_high_bound_val();
        if (high_bound_val <= transition_point) {
          part_num++;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect interval partition table", K(ret));
  }
  return ret;
}

int64_t ObPartitionSchema::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(part_level),
       K_(part_option),
       K_(sub_part_option),
       K_(partition_num),
       K_(def_subpartition_num),
       K_(partition_status),
       K_(partition_schema_version),
       "partition_array", ObArrayWrap<ObPartition *>(partition_array_, partition_num_),
       "def_subpartition_array", ObArrayWrap<ObSubPartition *>(def_subpartition_array_, def_subpartition_num_),
       "hidden_partition_array",
       ObArrayWrap<ObPartition *>(hidden_partition_array_, hidden_partition_num_),
       K_(sub_part_template_flags),
       K_(transition_point),
       K_(interval_range));
  J_OBJ_END();
  return pos;
}

int ObPartitionSchema::get_tablet_and_object_id(
    common::ObTabletID &tablet_id,
    common::ObObjectID &object_id) const
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = get_part_level();
  if (OB_UNLIKELY(!has_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported table type", KR(ret));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    tablet_id = get_tablet_id();
    object_id = get_object_id();
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported part type", KR(ret), K(part_level));
  }
  LOG_TRACE("partition schema get tablet and object id",
             "object_id", get_object_id(),
             "tablet_id", get_tablet_id());
  return ret;
}

int ObPartitionSchema::get_tablet_and_object_id_by_index(
    const int64_t part_idx,
    const int64_t subpart_idx,
    ObTabletID &tablet_id,
    ObObjectID &object_id,
    ObObjectID &first_level_part_id) const
{
  int ret = OB_SUCCESS;
  const ObPartition *partition = NULL;
  ObPartitionLevel part_level = get_part_level();
  if (part_level >= PARTITION_LEVEL_MAX
      || PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", KR(ret), K(part_level));
  } else if (!has_tablet()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("There are no tablets in virtual table and view", KR(ret));
  } else if (OB_FAIL(get_partition_by_partition_index(
             part_idx, CHECK_PARTITION_MODE_NORMAL, partition))) {
    LOG_WARN("fail to get partition by part_idx", KR(ret), K(part_idx));
  } else if (OB_ISNULL(partition)){
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("partition not exist", KR(ret), K(part_idx));
  } else {
    tablet_id = partition->get_tablet_id();
    object_id = partition->get_part_id();
    first_level_part_id = OB_INVALID_ID;
    ObSubPartition **subpartition_array = partition->get_subpart_array();
    int64_t subpartition_num = partition->get_subpartition_num();
    if (PARTITION_LEVEL_TWO != part_level || subpart_idx < 0) {
      // skip
    } else if (OB_ISNULL(subpartition_array) || subpartition_num <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpart_array is null ord subpartition_num is invalid",
               K(ret), KP(subpartition_array), K(subpartition_num));
    } else if (subpart_idx >= subpartition_num) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("subpartition not exist", KR(ret), K(part_idx), K(subpart_idx));
    } else {
      const ObSubPartition *subpartition = subpartition_array[subpart_idx];
      tablet_id = subpartition->get_tablet_id();
      first_level_part_id = object_id;
      object_id = subpartition->get_sub_part_id();
    }
  }
  return ret;
}

int ObPartitionSchema::gen_hash_part_name(const int64_t part_idx,
                                          const ObHashNameType name_type,
                                          const bool need_upper_case,
                                          char* buf,
                                          const int64_t buf_size,
                                          int64_t *pos,
                                          const ObPartition *partition)
{
  int ret = OB_SUCCESS;
  if (NULL != pos) {
    *pos = 0;
  }
  int64_t part_name_size = 0;
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_size));
  } else if (OB_UNLIKELY(part_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid subpart id", K(part_idx), K(ret));
  } else if (FIRST_PART == name_type) {
    if (need_upper_case) {
      part_name_size += snprintf(buf, buf_size, "P%ld", part_idx);
    } else {
      part_name_size += snprintf(buf, buf_size, "p%ld", part_idx);
    }
  } else if (TEMPLATE_SUB_PART == name_type) {
    if (need_upper_case) {
      part_name_size += snprintf(buf, buf_size, "P%ld", part_idx);
    } else {
      part_name_size += snprintf(buf, buf_size, "p%ld", part_idx);
    }
  } else if (INDIVIDUAL_SUB_PART == name_type) {
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null partition", K(ret));
    } else {
      part_name_size += snprintf(buf, buf_size, "%s", partition->get_part_name().ptr());
      if (need_upper_case) {
        part_name_size += snprintf(buf + part_name_size, buf_size - part_name_size, "SP%ld", part_idx);
      } else {
        part_name_size += snprintf(buf + part_name_size, buf_size - part_name_size, "sp%ld", part_idx);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hash name type", K(ret), K(name_type));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(part_name_size <= 0 || part_name_size >= buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pname size is invalid", K(ret), K(part_name_size), K(buf_size));
  } else if (NULL != pos) {
    *pos = part_name_size;
  }
  return ret;
}

int ObPartitionSchema::get_partition_by_part_id(
    const int64_t part_id,
    const ObCheckPartitionMode check_partition_mode,
    const ObPartition *&partition) const
{
  int ret = OB_SUCCESS;
  int64_t partition_index = OB_INVALID_INDEX;
  partition = NULL;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_id));
  } else if (partition_num_ < 0
             || hidden_partition_num_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition num is invalid", KR(ret), K(part_id),
             K_(partition_num), K_(hidden_partition_num));
  } else if (OB_ISNULL(partition_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition array", K(ret));
  } else if (OB_FAIL(get_partition_index_by_id(part_id,
                                               check_partition_mode,
                                               partition_index))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("partition not exist", KR(ret), K(part_id), K(check_partition_mode));
    } else {
      LOG_WARN("failed to get partition index by id", K(ret), K(part_id));
    }
  } else if (OB_FAIL(get_partition_by_partition_index(partition_index,
                                                      check_partition_mode,
                                                      partition))) {
    LOG_WARN("fail to get partition by partition_index",
             KR(ret), K(part_id), K(check_partition_mode), K(partition_index));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(partition) && partition->get_part_id() != part_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition", K(ret), KPC(partition), K(part_id));
  } else {
    // partition maybe null
  }
  return ret;
}

int ObPartitionSchema::get_partition_index_loop(
    const int64_t part_id,
    const ObCheckPartitionMode check_partition_mode,
    int64_t &partition_index) const
{
  int ret = OB_SUCCESS;
  bool finded = false;
  partition_index = 0;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_id));
  }

  if (OB_SUCC(ret) && !finded && check_normal_partition(check_partition_mode)) {
    if (OB_ISNULL(partition_array_) || partition_num_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition array", K(ret), K(part_id), K_(partition_num));
    }
    for (int64_t i = 0; !finded && i < partition_num_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partiion", K(ret), K(i), K(part_id));
      } else if (part_id == partition_array_[i]->get_part_id()) {
        partition_index += i;
        finded = true;
      }
    } // end for
    if (OB_SUCC(ret) && !finded) {
      partition_index += partition_num_;
    }
  }

  if (OB_SUCC(ret) && !finded && check_hidden_partition(check_partition_mode)) {
    if (OB_ISNULL(hidden_partition_array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition array", K(ret), K(part_id));
    }
    for (int64_t i = 0; !finded && i < hidden_partition_num_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(hidden_partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partiion", K(ret), K(i), K(part_id));
      } else if (part_id == hidden_partition_array_[i]->get_part_id()) {
        partition_index += i;
        finded = true;
      }
    } // end for
    if (OB_SUCC(ret) && !finded) {
      partition_index += hidden_partition_num_;
    }
  }

  if (OB_SUCC(ret) && !finded) {
    partition_index = OB_INVALID_INDEX;
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find partition", K(ret), K(part_id));
  }
  return ret;
}

int ObPartitionSchema::get_partition_by_partition_index(
    const int64_t partition_index,
    const ObCheckPartitionMode check_partition_mode,
    const ObPartition *&partition) const
{
  int ret = OB_SUCCESS;
  partition = NULL;
  const int64_t part_num = check_normal_partition(check_partition_mode) ?
                           get_partition_num() : 0;
  const int64_t hidden_part_num = check_hidden_partition(check_partition_mode) ?
                                  get_hidden_partition_num() : 0;
  const int64_t total_part_num =  part_num + hidden_part_num;
  if (0 <= partition_index && part_num > partition_index) {
    if (OB_ISNULL(get_part_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_array is null", KR(ret), K(partition_index));
    } else {
      partition = get_part_array()[partition_index];
    }
  } else if (part_num <= partition_index
             && total_part_num > partition_index) {
    if (OB_ISNULL(get_hidden_part_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hidden_partition_array is null", KR(ret), K(partition_index));
    } else {
      partition = get_hidden_part_array()[partition_index - part_num];
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition index", KR(ret), K(partition_index));
  }
  return ret;
}

int ObPartitionSchema::get_subpart_info(
    const int64_t part_id,
    ObSubPartition **&subpart_array,
    int64_t &subpart_num,
    int64_t &subpartition_num) const
{
  int ret = OB_SUCCESS;
  const ObPartition *part = NULL;
  const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
  if (OB_FAIL(get_partition_by_part_id(
              part_id, mode, part))) {
    LOG_WARN("fail to get partition", K(ret), K(part_id));
  } else if (OB_ISNULL(part)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get partition", K(ret), K(part_id));
  } else {
    subpart_array = part->get_subpart_array();
    subpart_num = part->get_sub_part_num();
    subpartition_num = part->get_subpartition_num();
  }
  if (OB_FAIL(ret)) {
  } else if (subpart_num != subpartition_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart_num not match", K(ret), K(part_id),
             K(subpart_num), K(subpartition_num));
  } else if (OB_ISNULL(subpart_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart_array is null", K(ret), K(part_id));
  }
  return ret;
}


int ObPartitionSchema::get_partition_index_by_id(
    const int64_t part_id,
    const ObCheckPartitionMode check_partition_mode,
    int64_t &partition_index) const
{
  int ret = OB_SUCCESS;
  partition_index = OB_INVALID_INDEX;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_id is invalid", KR(ret), K(part_id));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    if (!check_normal_partition(check_partition_mode) || get_object_id() != part_id) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("non-partitioned table only have one normal partition",
               KR(ret), K(part_id), K(check_partition_mode));
    } else {
      partition_index = 0;
    }
  } else if (0 == partition_schema_version_
             && is_hash_like_part()
             && check_normal_partition(check_partition_mode)) {
    // Here is an optimizition, hash like part/subpart doesn't support reorganize yet, and parition_id will be allocated continuously.
    int64_t part_idx = OB_INVALID_INDEX;
    if (OB_ISNULL(partition_array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_array is null", KR(ret));
    } else if (FALSE_IT(part_idx = part_id - partition_array_[0]->get_part_id())) {
    } else if (part_idx < 0 || part_idx >= partition_num_) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("part not exist", KR(ret), K(part_id), K(part_idx),
               K(partition_num_), K(check_partition_mode));
    } else if (partition_array_[part_idx]->get_part_id() != part_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id not match", KR(ret), K(part_idx), K(part_id), KPC(partition_array_[part_idx]));
    } else {
      partition_index = part_idx;
    }
  } else {
    // The divided partition can only be traversed
    if (OB_FAIL(get_partition_index_loop(part_id,
                                         check_partition_mode,
                                         partition_index))) {
      LOG_WARN("failed to get partition index loop", KR(ret), K(part_id));
    }
  }
  return ret;
}

int ObPartitionSchema::mock_list_partition_array()
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = get_table_id();
  bool is_oracle_mode = false;
  if (!is_virtual_table(table_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only virtual table need mock partition array", KR(ret), K(table_id));
  } else if (!is_list_part()
             || PARTITION_LEVEL_ONE != get_part_level()
             || 1 != get_first_part_num()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid part option", KR(ret), K(table_id), K_(part_option));
  } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_id));
  } else {
    reset_partition_array();
    ObPartition partition;
    char buf[OB_MAX_PARTITION_NAME_LENGTH] = {'\0'};
    // inner table use pure schema id as part_id
    const int64_t part_id = table_id;
    const char* part_name_str  = is_oracle_mode ?
                                 ORACLE_NON_PARTITIONED_TABLE_PART_NAME :
                                 MYSQL_NON_PARTITIONED_TABLE_PART_NAME;
    ObString part_name(strlen(part_name_str), part_name_str);

    partition.set_tenant_id(get_tenant_id());
    partition.set_table_id(table_id);
    partition.set_part_id(part_id);
    partition.set_schema_version(get_schema_version());
    // part_name
    if (OB_FAIL(partition.set_part_name(part_name))) {
      LOG_WARN("fail to set part name", KR(ret), K(table_id), K(part_name));
    }
    // list_row_values
    if (OB_SUCC(ret)) {
      ObNewRow row;
      ObObj obj;
      obj.set_max_value();
      row.assign(&obj, 1);
      if (OB_FAIL(partition.add_list_row(row))) {
        LOG_WARN("add row failed", KR(ret), K(table_id));
      } else if (OB_FAIL(add_partition(partition))) {
        LOG_WARN("fail to add partition", KR(ret), K(partition));
      }
    }
  }
  return ret;
}

int ObPartitionSchema::get_partition_by_name(const ObString &name, const ObPartition *&part) const
{
  int ret = OB_SUCCESS;
  part = nullptr;
  const ObPartitionLevel part_level = this->get_part_level();
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  ObPartitionSchemaIter iter(*this, check_partition_mode);
  ObPartitionSchemaIter::Info info;
  if (PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_UNKNOWN_PARTITION;
    LOG_WARN("could not get partition on nonpartitioned table", KR(ret), K(part_level));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END == ret) {
          ret = OB_UNKNOWN_PARTITION;
          LOG_WARN("could not find the partition by given name", KR(ret), K(name), KPC(this));
        } else {
          LOG_WARN("unexpected erro happened when get partition by name", KR(ret));
        }
      } else if (OB_ISNULL(info.part_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info.part_ is null", KR(ret), KPC(this));
      } else if (ObCharset::case_insensitive_equal(name, info.part_->get_part_name())) {
        part = info.part_;
        break;
      }
    }
  }
  return ret;
}

int ObPartitionSchema::get_subpartition_by_name(const ObString &name, const ObPartition *&part, const ObSubPartition *&subpart) const
{
  int ret = OB_SUCCESS;
  part = nullptr;
  subpart = nullptr;
  const ObPartitionLevel part_level = this->get_part_level();
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  ObPartitionSchemaIter iter(*this, check_partition_mode);
  ObPartitionSchemaIter::Info info;
  if (PARTITION_LEVEL_TWO != part_level) {
    ret = OB_UNKNOWN_SUBPARTITION;
    LOG_WARN("could not get subpartition on not composite partition table", KR(ret), K(part_level));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END == ret) {
          //subpart not exist errno is same with the part right now
          ret = OB_UNKNOWN_SUBPARTITION;
          LOG_WARN("could not find the subpartition by given name", KR(ret), K(name), KPC(this));
        } else {
          LOG_WARN("unexpected erro happened when get subpartition by name", KR(ret));
        }
      } else if (OB_ISNULL(info.part_) || OB_ISNULL(info.partition_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info.part_ or info.partition_ is null", KR(ret), KP(info.part_), KP(info.partition_), KPC(this));
      } else if (ObCharset::case_insensitive_equal(name, info.partition_->get_part_name())) {
        part = info.part_;
        subpart = reinterpret_cast<const ObSubPartition*>(info.partition_);
        break;
      }
    }
  }
  return ret;
}

int ObPartitionSchema::check_partition_duplicate_with_name(const ObString &name) const
{
  int ret = OB_SUCCESS;
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  ObPartitionSchemaIter iter(*this, check_partition_mode);
  ObPartitionSchemaIter::Info info;
  const ObPartitionLevel part_level = this->get_part_level();
  if (PARTITION_LEVEL_ZERO == part_level) {
    //nonpartitioned tabel doesn't have any partition
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("unexpected erro happened when get check partition duplicate with name", KR(ret));
        }
      } else if (OB_ISNULL(info.part_) || OB_ISNULL(info.partition_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info.part_ is null", KR(ret), KP(info.part_), KP(info.partition_), KPC(this));
      } else if (ObCharset::case_insensitive_equal(name, info.part_->get_part_name())
        || ObCharset::case_insensitive_equal(name, info.partition_->get_part_name())) {
        ret = OB_DUPLICATE_OBJECT_NAME_EXIST;
        LOG_WARN("there is a partition or subpartition have the same name", KR(ret), KPC(info.part_), KPC(info.partition_));
        break;
      }
    }
  }
  return ret;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTablegroupSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObTablegroupSchema::ObTablegroupSchema()
    : ObPartitionSchema(),
      tenant_id_(OB_INVALID_TENANT_ID),
      tablegroup_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      tablegroup_name_(),
      comment_(),
      sharding_(),
      part_func_expr_num_(OB_INVALID_INDEX),
      sub_part_func_expr_num_(OB_INVALID_INDEX),
      split_partition_name_(),
      split_high_bound_val_(),
      split_list_row_values_()
{
  ObIAllocator *allocator = get_allocator();
}

ObTablegroupSchema::ObTablegroupSchema(common::ObIAllocator *allocator)
    : ObPartitionSchema(allocator),
      tenant_id_(OB_INVALID_TENANT_ID),
      tablegroup_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      tablegroup_name_(),
      comment_(),
      sharding_(),
      part_func_expr_num_(OB_INVALID_INDEX),
      sub_part_func_expr_num_(OB_INVALID_INDEX),
      split_partition_name_(),
      split_high_bound_val_(),
      split_list_row_values_()
{
}

ObTablegroupSchema::ObTablegroupSchema(const ObTablegroupSchema &other)
    : ObPartitionSchema(),
      tenant_id_(OB_INVALID_TENANT_ID),
      tablegroup_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      tablegroup_name_(),
      comment_(),
      sharding_(),
      part_func_expr_num_(OB_INVALID_INDEX),
      sub_part_func_expr_num_(OB_INVALID_INDEX),
      split_partition_name_(),
      split_high_bound_val_(),
      split_list_row_values_()
{
  if (this != &other) {
    ObIAllocator *allocator = get_allocator();
    *this = other;
  }
}

ObTablegroupSchema::~ObTablegroupSchema()
{
}

ObObjectID ObTablegroupSchema::get_object_id() const
{
  return static_cast<ObObjectID>(get_tablegroup_id());
}

ObTabletID ObTablegroupSchema::get_tablet_id() const
{
  return static_cast<ObTabletID>(common::ObTabletID::INVALID_TABLET_ID);
}

int ObTablegroupSchema::assign(const ObTablegroupSchema &src_schema)
{
  int ret = OB_SUCCESS;
  *this = src_schema;
   ret = get_err_ret();
  return ret;
}

ObTablegroupSchema &ObTablegroupSchema::operator =(const ObTablegroupSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    ObPartitionSchema::operator=(src_schema);
    if (OB_SUCCESS == error_ret_) {
      int ret = OB_SUCCESS;
      tenant_id_ = src_schema.tenant_id_;
      tablegroup_id_ = src_schema.tablegroup_id_;
      schema_version_ = src_schema.schema_version_;
      part_func_expr_num_ = src_schema.part_func_expr_num_;
      sub_part_func_expr_num_ = src_schema.sub_part_func_expr_num_;

      if (OB_FAIL(deep_copy_str(src_schema.tablegroup_name_, tablegroup_name_))) {
        LOG_WARN("Fail to deep copy tablegroup name, ", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
        LOG_WARN("Fail to deep copy comment, ", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.split_partition_name_, split_partition_name_))) {
        LOG_WARN("fail to deep copy split partition name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.sharding_, sharding_))) {
        LOG_WARN("fail to deep copy split partition name", K(ret));
      } else if (OB_FAIL(src_schema.split_high_bound_val_.deep_copy(split_high_bound_val_, *get_allocator()))) {
        LOG_WARN("fail to deep copy split row key", K(ret));
      } else if (OB_FAIL(src_schema.split_list_row_values_.deep_copy(split_list_row_values_, *get_allocator()))) {
        LOG_WARN("failed to deep copy split list value", K(ret));
      }
      if (OB_FAIL(ret)) {
        error_ret_ = ret;
      }
    }
  }
  return *this;
}

int64_t ObTablegroupSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += tablegroup_name_.length() + 1;
  convert_size += comment_.length() + 1;
  convert_size += sharding_.length() + 1;
  convert_size += part_option_.get_convert_size() - sizeof(part_option_);
  convert_size += sub_part_option_.get_convert_size() - sizeof(sub_part_option_);
  convert_size += split_partition_name_.length() + 1;
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  partition_array_, partition_num_);
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  def_subpartition_array_, def_subpartition_num_);
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  hidden_partition_array_, hidden_partition_num_);
  convert_size += transition_point_.get_deep_copy_size();
  convert_size += interval_range_.get_deep_copy_size();
  return convert_size;
}

bool ObTablegroupSchema::is_valid() const
{
  return ObSchema::is_valid() && OB_INVALID_ID != tenant_id_
      && OB_INVALID_ID != tablegroup_id_ && schema_version_ > 0;
}

void ObTablegroupSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  tablegroup_name_.reset();
  comment_.reset();
  part_func_expr_num_ = OB_INVALID_INDEX;
  sub_part_func_expr_num_ = OB_INVALID_INDEX;
  split_partition_name_.reset();
  split_high_bound_val_.reset();
  split_list_row_values_.reset();
  sharding_.reset();
  ObPartitionSchema::reset();
}

OB_DEF_SERIALIZE(ObTablegroupSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              tablegroup_id_,
              schema_version_,
              tablegroup_name_,
              comment_,
              part_level_,
              part_option_,
              sub_part_option_,
              part_func_expr_num_,
              sub_part_func_expr_num_,
              partition_schema_version_,
              partition_status_,
              split_partition_name_);

  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(ObSchemaUtils::serialize_partition_array(
                  partition_array_, partition_num_,
                  buf, buf_len, pos))) {
        LOG_WARN("failed to serialize partitions", K(ret));
      } else { }
    }
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_TWO == part_level_) {
     if (OB_FAIL(ObSchemaUtils::serialize_partition_array(
                 def_subpartition_array_, def_subpartition_num_,
                 buf, buf_len, pos))) {
        LOG_WARN("failed to serialize subpartitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                sub_part_template_flags_);
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(ObSchemaUtils::serialize_partition_array(
                  hidden_partition_array_, hidden_partition_num_,
                  buf, buf_len, pos))){
        LOG_WARN("failed to serialize hidden partitions", K(ret));
      } else { }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                sharding_);
  }
  LOG_TRACE("serialize tablegroup schema", K(*this));

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTablegroupSchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              tablegroup_id_,
              schema_version_,
              tablegroup_name_,
              comment_,
              part_level_,
              part_option_,
              sub_part_option_,
              part_func_expr_num_,
              sub_part_func_expr_num_,
              partition_schema_version_,
              partition_status_,
              split_partition_name_);

  if (PARTITION_LEVEL_ONE <= part_level_) {
    len += ObSchemaUtils::get_partition_array_serialize_size(
           partition_array_, partition_num_);
    len += ObSchemaUtils::get_partition_array_serialize_size(
           hidden_partition_array_, hidden_partition_num_);
  }
  if (PARTITION_LEVEL_TWO == part_level_) {
    len += ObSchemaUtils::get_partition_array_serialize_size(
           def_subpartition_array_, def_subpartition_num_);
  }

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              sub_part_template_flags_);

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              sharding_);
  return len;
}

OB_DEF_DESERIALIZE(ObTablegroupSchema)
{
  int ret = OB_SUCCESS;
  ObString tablegroup_name;
  ObString comment;
  ObString split_partition_name;

  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              tablegroup_id_,
              schema_version_,
              tablegroup_name,
              comment,
              part_level_,
              part_option_,
              sub_part_option_,
              part_func_expr_num_,
              sub_part_func_expr_num_,
              partition_schema_version_,
              partition_status_,
              split_partition_name);

  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(deserialize_partitions(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize partitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_TWO == part_level_) {
      if (OB_FAIL(deserialize_def_subpartitions(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize subpartitions", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    //skip
  } else if (OB_FAIL(deep_copy_str(tablegroup_name, tablegroup_name_))) {
    LOG_WARN("Fail to deep copy tablegroup name, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(comment, comment_))) {
    LOG_WARN("Fail to deep copy comment, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(split_partition_name, split_partition_name_))) {
    LOG_WARN("fail to deep copy split partition name", K(ret), K(split_partition_name));
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                sub_part_template_flags_);
  }
  // hidden partition array
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(deserialize_partitions(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize hidden partitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                sharding_);
  }
  LOG_WARN("serialize tablegroup schema", K(*this));
  return ret;
}

int64_t ObTablegroupSchema::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(tablegroup_id),
       K_(schema_version),
       K_(tablegroup_name),
       K_(comment),
       K_(part_level),
       K_(part_option),
       K_(sub_part_option),
       K_(part_func_expr_num),
       K_(sub_part_func_expr_num),
       K_(partition_num),
       K_(def_subpartition_num),
       K_(error_ret),
       K_(partition_status),
       "partition_array", ObArrayWrap<ObPartition *>(partition_array_, partition_num_),
       "def_subpartition_array", ObArrayWrap<ObSubPartition *>(def_subpartition_array_, def_subpartition_num_),
       "hidden_partition_array",
       ObArrayWrap<ObPartition *>(hidden_partition_array_, hidden_partition_num_),
       K_(split_high_bound_val),
       K_(split_list_row_values),
       K_(sub_part_template_flags),
       K_(sharding));
  J_OBJ_END();
  return pos;
}

int ObTablegroupSchema::get_first_primary_zone_inherit(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
    common::ObZone &first_primary_zone) const
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObTablegroupSchema::get_zone_list(
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  zone_list.reset();
  if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else {} // no more to do
  return ret;
}

int ObTablegroupSchema::check_is_duplicated(
    share::schema::ObSchemaGetterGuard &guard,
    bool &is_duplicated) const
{
  int ret = OB_SUCCESS;
  UNUSED(guard);
  is_duplicated = false;
  return ret;
}

//TODO: These functions can be extracted; as an implementation of locality/primary_zone
int ObTablegroupSchema::get_zone_replica_attr_array_inherit(
    ObSchemaGetterGuard &schema_guard,
    ZoneLocalityIArray &locality) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_locality = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_;
  locality.reset();
  if (!use_tenant_locality) {
    const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, simple_tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
    } else {
      use_tenant_locality = simple_tenant->is_restore();
    }
  }
  if (OB_FAIL(ret)) {
  } else {
   const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array_inherit(schema_guard, locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret), K(tablegroup_id_), K(tenant_id_));
    }
  }
  return ret;
}

int ObTablegroupSchema::get_locality_str_inherit(
    share::schema::ObSchemaGetterGuard &guard,
    const common::ObString *&locality_str) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_locality = OB_SYS_TENANT_ID != tenant_id_ && GCTX.is_standby_cluster();
  locality_str = nullptr;
  if (!use_tenant_locality) {
    const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
    if (OB_FAIL(guard.get_tenant_info(tenant_id_, simple_tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
    } else {
      use_tenant_locality = simple_tenant->is_restore();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (use_tenant_locality
             || nullptr == locality_str
             || locality_str->empty()) {
    const ObSimpleTenantSchema *tenant_schema = nullptr;
    if (OB_FAIL(guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
    } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
    } else {
      locality_str = &tenant_schema->get_locality_str();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == locality_str || locality_str->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality str should not be null or empty", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTablegroupSchema::get_primary_zone_inherit(
    ObSchemaGetterGuard &schema_guard,
    ObPrimaryZone &primary_zone) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_primary_zone = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_;
  primary_zone.reset();
  if (!use_tenant_primary_zone) {
    const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, simple_tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
    } else {
      use_tenant_primary_zone = simple_tenant->is_restore();
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_primary_zone_inherit(schema_guard, primary_zone))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(tablegroup_id_), K(tenant_id_));
    }
  }
  return ret;
}

int ObTablegroupSchema::check_is_readonly_at_all(
    ObSchemaGetterGuard &schema_guard,
    const common::ObZone &zone,
    const common::ObRegion &region,
    bool &readonly_at_all) const
{
  UNUSED(region);
  int ret = OB_SUCCESS;
  readonly_at_all = false;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count(); ++i) {
      if (zone == zone_locality.at(i).zone_) {
        readonly_at_all = (OB_ALL_SERVER_CNT == zone_locality.at(i).get_readonly_replica_num());
        break;
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::get_full_replica_num(
    share::schema::ObSchemaGetterGuard &guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count(); ++i) {
      num += zone_locality.at(i).get_full_replica_num();
    }
  }
  return ret;
}

int ObTablegroupSchema::get_paxos_replica_num(
    share::schema::ObSchemaGetterGuard &schema_guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  const ObTenantSchema *tenant_schema = NULL;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret) {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get valid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_paxos_replica_num();
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::get_all_replica_num(
    share::schema::ObSchemaGetterGuard &guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count(); ++i) {
      num += zone_locality.at(i).get_specific_replica_num();
    }
  }
  return ret;
}

int ObTablegroupSchema::calc_part_func_expr_num(int64_t &part_func_expr_num) const
{
  int ret = OB_SUCCESS;
  part_func_expr_num = part_func_expr_num_;
  return ret;
}

int ObTablegroupSchema::calc_subpart_func_expr_num(int64_t &subpart_func_expr_num) const
{
  int ret = OB_SUCCESS;
  subpart_func_expr_num = sub_part_func_expr_num_;
  return ret;
}

int ObTablegroupSchema::check_if_oracle_compat_mode(bool &is_oracle_mode) const

{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  const int64_t tablegroup_id = get_tablegroup_id();
  is_oracle_mode = false;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

  if (is_sys_tablegroup_id(tablegroup_id)) {
    is_oracle_mode = false;
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (lib::Worker::CompatMode::ORACLE == compat_mode) {
    is_oracle_mode = true;
  } else if (lib::Worker::CompatMode::MYSQL == compat_mode) {
    is_oracle_mode = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compat_mode should not be INVALID.", KR(ret), K(tenant_id), K_(tablegroup_id));
  }
  return ret;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObPartitionOption-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObPartitionOption::ObPartitionOption()
    : ObSchema(),
      part_func_type_(PARTITION_FUNC_TYPE_HASH),
      part_func_expr_(),
      part_num_(1),
      auto_part_(false),
      auto_part_size_(-1)
{
}

ObPartitionOption::ObPartitionOption(ObIAllocator *allocator)
    : ObSchema(allocator),
      part_func_type_(PARTITION_FUNC_TYPE_HASH),
      part_func_expr_(),
      part_num_(1),
      auto_part_(false),
      auto_part_size_(-1)
{
}

ObPartitionOption::~ObPartitionOption()
{
}

ObPartitionOption::ObPartitionOption(const ObPartitionOption &expr)
    : ObSchema(), part_func_type_(PARTITION_FUNC_TYPE_HASH), part_num_(1),
      auto_part_(false), auto_part_size_(-1)
{
  *this = expr;
}

ObPartitionOption &ObPartitionOption::operator =(const ObPartitionOption &expr)
{
  if (this != &expr) {
    reset();
    int ret = OB_SUCCESS;

    part_num_ = expr.part_num_;
    part_func_type_ = expr.part_func_type_;
    auto_part_ = expr.auto_part_;
    auto_part_size_ = expr.auto_part_size_;
    if (OB_FAIL(deep_copy_str(expr.part_func_expr_, part_func_expr_))) {
      LOG_WARN("Fail to deep copy part func expr, ", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }

  }
  return *this;
}

int64_t ObPartitionOption::assign(const ObPartitionOption &src_part)
{
  int ret = OB_SUCCESS;
  *this = src_part;
  ret = get_assign_ret();
  return ret;
}

bool ObPartitionOption::operator ==(const ObPartitionOption &expr) const
{
  return (part_func_type_ == expr.part_func_type_)
      && (part_num_ == expr.part_num_)
      && (part_func_expr_ == expr.part_func_expr_)
      && (auto_part_ == expr.auto_part_)
      && (auto_part_size_ == expr.auto_part_size_);
}

bool ObPartitionOption::operator !=(const ObPartitionOption &expr) const
{
  return !(*this == expr);
}

void ObPartitionOption::reset()
{
  part_func_type_ = PARTITION_FUNC_TYPE_HASH;
  part_num_ = 1;
  reset_string(part_func_expr_);
  reset_string(interval_start_);
  reset_string(part_interval_);
  auto_part_ = false;
  auto_part_size_ = -1;
  ObSchema::reset();
}

void ObPartitionOption::reuse()
{
  part_func_type_ = PARTITION_FUNC_TYPE_HASH;
  part_num_ = 1;
  reset_string(part_func_expr_);
  reset_string(interval_start_);
  reset_string(part_interval_);
  auto_part_ = false;
  auto_part_size_ = 0;
  ObSchema::reset();
}

int64_t ObPartitionOption::get_convert_size() const
{
  return sizeof(*this) + part_func_expr_.length() + 1;
}

bool ObPartitionOption::is_valid() const
{
  return ObSchema::is_valid() && part_num_ > 0;
}

OB_DEF_SERIALIZE(ObPartitionOption)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, part_func_type_,
              part_func_expr_, part_num_,
              auto_part_, auto_part_size_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPartitionOption)
{
  int ret = OB_SUCCESS;
  ObString part_func_expr;

  LST_DO_CODE(OB_UNIS_DECODE, part_func_type_,
              part_func_expr, part_num_,
              auto_part_, auto_part_size_);

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(part_func_expr, part_func_expr_))) {
    LOG_WARN("Fail to deep copy part_func_expr, ", K(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPartitionOption)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, part_func_type_,
              part_func_expr_, part_num_,
              auto_part_, auto_part_size_);
  return len;
}

ObBasePartition::ObBasePartition()
  : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID),
    part_id_(common::OB_INVALID_INDEX),
    schema_version_(OB_INVALID_VERSION), name_(),
    high_bound_val_(), status_(PARTITION_STATUS_ACTIVE),
    projector_(NULL),
    projector_size_(0),
    part_idx_(OB_INVALID_INDEX),
    is_empty_partition_name_(false),
    tablespace_id_(common::OB_INVALID_ID),
    partition_type_(PARTITION_TYPE_NORMAL),
    low_bound_val_(),
    tablet_id_(),
    external_location_(),
    split_source_tablet_id_()
{ }

ObBasePartition::ObBasePartition(common::ObIAllocator *allocator)
  : ObSchema(allocator), tenant_id_(common::OB_INVALID_ID),
    table_id_(common::OB_INVALID_ID),
    part_id_(common::OB_INVALID_INDEX),
    schema_version_(OB_INVALID_VERSION), name_(),
    high_bound_val_(),
    schema_allocator_(*allocator),
    list_row_values_(SCHEMA_MALLOC_BLOCK_SIZE,
                     common::ModulePageAllocator(schema_allocator_)),
    status_(PARTITION_STATUS_ACTIVE),
    projector_(NULL),
    projector_size_(0),
    part_idx_(OB_INVALID_INDEX),
    is_empty_partition_name_(false),
    tablespace_id_(common::OB_INVALID_ID),
    partition_type_(PARTITION_TYPE_NORMAL),
    low_bound_val_(),
    tablet_id_(),
    external_location_(),
    split_source_tablet_id_()
{ }

void ObBasePartition::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  part_id_ = -1;
  tablet_id_.reset();
  split_source_tablet_id_.reset();
  schema_version_ = OB_INVALID_VERSION;
  status_ = PARTITION_STATUS_ACTIVE;
  projector_ = NULL;
  projector_size_ = 0;
  part_idx_ = OB_INVALID_INDEX;
  high_bound_val_.reset();
  low_bound_val_.reset();
  list_row_values_.reset();
  part_idx_ = OB_INVALID_INDEX;
  is_empty_partition_name_ = false;
  tablespace_id_ = OB_INVALID_ID;
  partition_type_ = PARTITION_TYPE_NORMAL;
  name_.reset();
  ObSchema::reset();
  external_location_.reset();
}

int ObBasePartition::assign(const ObBasePartition & src_part)
{
  int ret = OB_SUCCESS;
  if (this != &src_part) {
    reset();
    tenant_id_ = src_part.tenant_id_;
    table_id_ = src_part.table_id_;
    tablet_id_ = src_part.tablet_id_;
    split_source_tablet_id_ = src_part.split_source_tablet_id_;
    part_id_ = src_part.part_id_;
    schema_version_ = src_part.schema_version_;
    status_ = src_part.status_;
    part_idx_ = src_part.part_idx_;
    is_empty_partition_name_ = src_part.is_empty_partition_name_;
    tablespace_id_ = src_part.tablespace_id_;
    partition_type_ = src_part.partition_type_;
    if (OB_FAIL(deep_copy_str(src_part.name_, name_))) {
      LOG_WARN("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_part.external_location_, external_location_))) {
      LOG_WARN("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(set_high_bound_val(src_part.high_bound_val_))) {
      LOG_WARN("Fail to deep copy high_bound_val_", K(ret));
    } else if (OB_FAIL(set_low_bound_val(src_part.low_bound_val_))) {
      LOG_WARN("Fail to deep copy low_bound_val_", K(ret));
    } else {
      int64_t count = src_part.list_row_values_.count();
      if (OB_FAIL(list_row_values_.reserve(count))) {
        LOG_WARN("fail to reserve se array", K(ret), K(count));
      } else {
        ObNewRow tmp_row;
        ObIAllocator *allocator = get_allocator();
        for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
          const ObNewRow &row = src_part.list_row_values_.at(i);
          if (OB_FAIL(ob_write_row(*allocator, row, tmp_row))) {
            LOG_WARN("Fail to write row", K(ret));
          } else if (OB_FAIL(list_row_values_.push_back(tmp_row))) {
            LOG_WARN("Fail to push back row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

bool ObBasePartition::list_part_func_layout(
     const ObBasePartition *lhs,
     const ObBasePartition *rhs)
{
  bool bool_ret = false;
  /*
   * The default partition of the list partition mode, in the current code, some assumptions are placed
   * at the end of the partition/subpartition array.
   * The list_row_values of the default partition is filled in with the max value of a single element.
   * Therefore, the comparison rules are as follows:
   * 1. The number of rows is small and put back
   * 2. When the number of rows is the same, the row value at the corresponding position is compared.
   * Ensure that the default partition is placed in the last partition
   */
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs should not be null", KP(lhs), KP(rhs));
  } else if (lhs->get_list_row_values().count() < rhs->get_list_row_values().count()) {
    bool_ret = false;
  } else if (lhs->get_list_row_values().count() > rhs->get_list_row_values().count()) {
    bool_ret = true;
  } else {
    bool finish = false;
    for (int64_t i = 0; !finish && i < lhs->get_list_row_values().count(); ++i) {
      const common::ObNewRow &l_row = lhs->get_list_row_values().at(i);
      const common::ObNewRow &r_row = rhs->get_list_row_values().at(i);
      int cmp = 0;
      if (OB_SUCCESS != ObRowUtil::compare_row(l_row, r_row, cmp)) {
        LOG_ERROR_RET(OB_INVALID_ARGUMENT, "l or r is invalid");
        finish = true;
      } else if (cmp < 0) {
        bool_ret = true;
        finish = true;
      } else if (cmp > 0) {
        bool_ret = false;
        finish = true;
      } else {} // go on next
    }
    if (!finish) {
      bool_ret = (lhs->get_part_id() < rhs->get_part_id());
    }
  }
  return bool_ret;
}

int ObBasePartition::set_low_bound_val(const ObRowkey &low_bound_val)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (OB_FAIL(low_bound_val.deep_copy(low_bound_val_, *allocator))) {
    LOG_WARN("Fail to deep copy low_bound_val_", K(ret));
  } else { }
  return ret;
}

int ObBasePartition::set_high_bound_val(const ObRowkey &high_bound_val)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (OB_FAIL(high_bound_val.deep_copy(high_bound_val_, *allocator))) {
    LOG_WARN("Fail to deep copy high_bound_val_", K(ret));
  } else { }
  return ret;
}

int ObBasePartition::set_list_vector_values_with_hex_str(
    const common::ObString &list_vector_vals_hex)
{
  int ret = OB_SUCCESS;
  const int obj_capacity = 1024;
  HEAP_VARS_2((char[OB_MAX_B_HIGH_BOUND_VAL_LENGTH], serialize_buf),
              (ObObj[obj_capacity], obj_array)) {
    ObIAllocator *allocator = get_allocator();
    int64_t pos = 0;
    const int64_t hex_length = list_vector_vals_hex.length();
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Allocator is NULL", K(ret));
    } else if ((hex_length % 2) != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Hex str length should be even", K(ret));
    } else if (OB_UNLIKELY(hex_length != str_to_hex(
         list_vector_vals_hex.ptr(), static_cast<int32_t>(hex_length),
         serialize_buf, OB_MAX_B_HIGH_BOUND_VAL_LENGTH))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("Failed to get hex_str buf", K(ret));
    }

    if (OB_SUCC(ret)) {
      int64_t size = 0;
      if (OB_FAIL(serialization::decode_vi64(serialize_buf, hex_length, pos, &size))) {
        LOG_WARN("fail to decode vi64", K(ret));
      }
      ObNewRow row;
      ObNewRow tmp_row;
      row.assign(obj_array, obj_capacity);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i ++) {
        row.count_ = obj_capacity; // reset count for sparse row
        if (OB_FAIL(row.deserialize(serialize_buf, hex_length, pos))) {
          LOG_WARN("fail to deserialize row", K(ret));
        } else if (OB_FAIL(ob_write_row(*allocator, row, tmp_row))) {
          LOG_WARN("fail to write row", K(ret));
        } else if (OB_FAIL(list_row_values_.push_back(tmp_row))) {
          LOG_WARN("fail to push back tmp_row", K(ret));
        }
      }
      // Sort each point in the partition list according to the rules
      if (OB_SUCC(ret)) {
        InnerPartListVectorCmp part_list_vector_op;
        lib::ob_sort(list_row_values_.begin(), list_row_values_.end(), part_list_vector_op);
        if (OB_FAIL(part_list_vector_op.get_ret())) {
          LOG_WARN("fail to sort list row values", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObBasePartition::set_high_bound_val_with_hex_str(
    const common::ObString &high_bound_val_hex)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator local_allocator("HighBoundV");
  char *serialize_buf = NULL;
  ObIAllocator *allocator = get_allocator();
  int64_t pos = 0;
  const int64_t hex_length = high_bound_val_hex.length();
  const int64_t seri_length = hex_length / 2;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if ((hex_length % 2) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hex str length should be even", K(ret));
  } else if (OB_ISNULL(serialize_buf = static_cast<char*>(local_allocator.alloc(seri_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(seri_length));
  } else if (OB_UNLIKELY(hex_length != str_to_hex(
       high_bound_val_hex.ptr(), static_cast<int32_t>(hex_length),
       serialize_buf, static_cast<int32_t>(seri_length)))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("Failed to get hex_str buf", K(ret));
  } else if (OB_FAIL(high_bound_val_.deserialize(*allocator, serialize_buf, seri_length, pos))) {
    LOG_WARN("Failed to deserialize high bound val", K(ret));
  } else { }//do nothing
  return ret;
}

int ObBasePartition::convert_character_for_range_columns_part(
    const ObCollationType &to_collation)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator", K(ret));
  } else if (low_bound_val_.get_obj_cnt() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("defensive code, unexpected error", K(ret), K(low_bound_val_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < high_bound_val_.get_obj_cnt(); i++) {
      ObObj &obj = high_bound_val_.get_obj_ptr()[i];
      const ObObjMeta &obj_meta = obj.get_meta();
      if (obj_meta.is_lob()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err, lob column can not be part key", K(ret), K(obj_meta));
      } else if (ObDDLUtil::check_can_convert_character(obj_meta)) {
        ObString dst_string;
        if (OB_FAIL(ObCharset::charset_convert(*allocator, obj.get_string(), obj.get_collation_type(),
                                               to_collation, dst_string))) {
          LOG_WARN("charset convert failed", K(ret), K(obj), K(to_collation));
        } else {
          obj.set_string(obj.get_type(), dst_string);
          obj.set_collation_type(to_collation);
        }
      }
    }
  }
  return ret;
}

int ObBasePartition::convert_character_for_list_columns_part(
    const ObCollationType &to_collation)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list_row_values_.count(); i++) {
      common::ObNewRow &row = list_row_values_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < row.get_count(); j++) {
        ObObj &obj = row.get_cell(j);
        const ObObjMeta &obj_meta = obj.get_meta();
        if (obj_meta.is_lob()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err, lob column can not be part key", K(ret), K(obj_meta));
        } else if (ObDDLUtil::check_can_convert_character(obj_meta)) {
          ObString dst_string;
          if (OB_FAIL(ObCharset::charset_convert(*allocator, obj.get_string(), obj.get_collation_type(),
                                               to_collation, dst_string))) {
          LOG_WARN("charset convert failed", K(ret), K(obj), K(to_collation));
          } else {
            obj.set_string(obj.get_type(), dst_string);
            obj.set_collation_type(to_collation);
          }
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObBasePartition)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, table_id_, part_id_,
              schema_version_, name_, high_bound_val_, status_);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_vi64(buf,
                                                buf_len,
                                                pos,
                                                list_row_values_.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < list_row_values_.count(); i ++) {
    if (OB_FAIL(list_row_values_.at(i).serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to encode row", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              part_idx_,
              is_empty_partition_name_,
              tablespace_id_,
              partition_type_,
              low_bound_val_,
              tablet_id_,
              external_location_,
              split_source_tablet_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasePartition)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("BasePart");
  ObRowkey high_bound_val;
  ObRowkey low_bound_val;
  ObString name;
  ObObj sub_interval_start;
  ObObj sub_part_interval;

  if (OB_SUCC(ret)) {
    void *tmp_buf = NULL;
    ObObj *array = NULL;
    if (OB_ISNULL(tmp_buf = allocator.alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER * 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret));
    } else if (OB_ISNULL(array = new (tmp_buf) ObObj[OB_MAX_ROWKEY_COLUMN_NUMBER * 2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new obj array", KR(ret));
    } else {
      high_bound_val.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      low_bound_val.assign(&(array[OB_MAX_ROWKEY_COLUMN_NUMBER]), OB_MAX_ROWKEY_COLUMN_NUMBER);
    }
  }

  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, table_id_, part_id_,
              schema_version_, name);
  if (FAILEDx(high_bound_val.deserialize(buf, data_len, pos, true))) {
    LOG_WARN("fail to deserialize high_bound_val", KR(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE, status_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(name, name_))) {
    LOG_WARN("Fail to deep copy name, ", K(ret), K_(name));
  } else if (OB_FAIL(set_high_bound_val(high_bound_val))) {
    LOG_WARN("Fail to deep copy high_bound_val", K(ret), K(high_bound_val));
  } else { }//do nothing

  if (OB_SUCC(ret) && pos < data_len) {
    int64_t size = 0;
    void *tmp_buf = NULL;
    ObObj *obj_array = NULL;
    const int obj_capacity = 1024;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &size))) {
      LOG_WARN("fail to decode vi64", K(ret));
    } else if (OB_ISNULL(tmp_buf = allocator.alloc(sizeof(ObObj) * obj_capacity))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret));
    } else if (OB_ISNULL(obj_array = new (tmp_buf) ObObj[obj_capacity])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new obj array", KR(ret));
    } else {
      ObNewRow row;
      ObNewRow tmp_row;
      row.assign(obj_array, obj_capacity);
      ObIAllocator *allocator = get_allocator();
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i ++) {
        row.count_ = obj_capacity; // reset count for sparse row
        if (OB_FAIL(row.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize row", K(ret));
        } else if (OB_FAIL(ob_write_row(*allocator, row, tmp_row))) {
          LOG_WARN("fail to write row", K(ret));
        } else if (OB_FAIL(list_row_values_.push_back(tmp_row))) {
          LOG_WARN("fail to push back tmp_row", K(ret));
        }
      }
    }
  }

  LST_DO_CODE(OB_UNIS_DECODE,
              part_idx_,
              is_empty_partition_name_,
              tablespace_id_,
              partition_type_);
  if (FAILEDx(low_bound_val.deserialize(buf, data_len, pos, true))) {
    LOG_WARN("fail to deserialze low_bound_val", KR(ret));
  }
  ObString external_location;
  LST_DO_CODE(OB_UNIS_DECODE, tablet_id_, external_location, split_source_tablet_id_);
  if (OB_SUCC(ret) && OB_FAIL(set_low_bound_val(low_bound_val))) {
    LOG_WARN("Fail to deep copy low_bound_val", K(ret), K(low_bound_val));
  } else if (OB_FAIL(deep_copy_str(external_location, external_location_))) {
    LOG_WARN("Fail to deep copy location ", K(ret), K_(external_location));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasePartition)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, table_id_, part_id_,
      schema_version_, name_, high_bound_val_, status_,
      part_idx_, is_empty_partition_name_,
      tablespace_id_, partition_type_, low_bound_val_, tablet_id_, external_location_, split_source_tablet_id_);
  len += serialization::encoded_length_vi64(list_row_values_.count());
  for (int64_t i = 0; i < list_row_values_.count(); i ++) {
    len += list_row_values_.at(i).get_serialize_size();
  }
  return len;
}

int64_t ObBasePartition::get_deep_copy_size() const
{
  int64_t deep_copy_size = name_.length() + 1;
  deep_copy_size += high_bound_val_.get_deep_copy_size();
  deep_copy_size += low_bound_val_.get_deep_copy_size();
  int64_t list_row_size = list_row_values_.get_data_size();
  for (int64_t i = 0; i < list_row_values_.count(); i ++) {
    list_row_size += list_row_values_.at(i).get_deep_copy_size();
  }
  deep_copy_size += list_row_size * 2 - 1;
  deep_copy_size += external_location_.length() + 1;
  return deep_copy_size;
}

bool ObBasePartition::less_than(const ObBasePartition *lhs, const ObBasePartition *rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else {
    ObNewRow lrow;
    lrow.cells_ = const_cast<ObObj*>(lhs->high_bound_val_.get_obj_ptr());
    lrow.count_ = lhs->high_bound_val_.get_obj_cnt();
    lrow.projector_ = lhs->projector_;
    lrow.projector_size_ = lhs->projector_size_;
    ObNewRow rrow;
    rrow.cells_ = const_cast<ObObj*>(rhs->high_bound_val_.get_obj_ptr());
    rrow.count_ = rhs->high_bound_val_.get_obj_cnt();
    rrow.projector_ = rhs->projector_;
    rrow.projector_size_ = rhs->projector_size_;
    int cmp = 0;
    if (OB_SUCCESS != ObRowUtil::compare_row(lrow, rrow, cmp)) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs is invalid", K(lrow), K(rrow), K(lhs), K(rhs));
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

bool ObBasePartition::range_like_func_less_than(const ObBasePartition *lhs, const ObBasePartition *rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else {
    ObNewRow lrow;
    lrow.cells_ = const_cast<ObObj*>(lhs->high_bound_val_.get_obj_ptr());
    lrow.count_ = lhs->high_bound_val_.get_obj_cnt();
    lrow.projector_ = lhs->projector_;
    lrow.projector_size_ = lhs->projector_size_;
    ObNewRow rrow;
    rrow.cells_ = const_cast<ObObj*>(rhs->high_bound_val_.get_obj_ptr());
    rrow.count_ = rhs->high_bound_val_.get_obj_cnt();
    rrow.projector_ = rhs->projector_;
    rrow.projector_size_ = rhs->projector_size_;
    int cmp = 0;
    if (OB_SUCCESS != ObRowUtil::compare_row(lrow, rrow, cmp)) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs is invalid", K(lrow), K(rrow), K(lhs), K(rhs));
    } else if (0 == cmp) {
      bret = lhs->get_part_id() < rhs->get_part_id();
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

bool ObBasePartition::hash_like_func_less_than(const ObBasePartition *lhs, const ObBasePartition *rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else {
    int cmp  = static_cast<int32_t>(lhs->get_part_idx() - rhs->get_part_idx());
    if (0 == cmp) {
      bret = lhs->get_part_id() < rhs->get_part_id();
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

////////////////////////////////////
ObPartition::ObPartition()
    : ObBasePartition()
{
  reset();
}

ObPartition::ObPartition(ObIAllocator *allocator)
    : ObBasePartition(allocator)
{
  reset();
}

void ObPartition::reset()
{
  sub_part_num_ = 0;
  sub_interval_start_.reset();
  sub_part_interval_.reset();
  subpartition_num_ = 0;
  subpartition_array_capacity_ = 0;
  subpartition_array_ = NULL;
  hidden_subpartition_num_ = 0;
  hidden_subpartition_array_capacity_ = 0;
  hidden_subpartition_array_ = NULL;
  ObBasePartition::reset();
}

int ObPartition::clone(common::ObIAllocator &allocator, ObPartition *&dst) const
{
  int ret = OB_SUCCESS;
  dst = NULL;

  ObPartition *new_part = OB_NEWx(ObPartition, (&allocator));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(ret));
  } else if (OB_FAIL(new_part->assign(*this))) {
    LOG_ERROR("assign failed", K(ret));
  } else {
    dst = new_part;
  }

  return ret;
}

int ObPartition::assign(const ObPartition & src_part)
{
  int ret = OB_SUCCESS;
  if (this != &src_part) {
    reset();
    if (OB_FAIL(ObBasePartition::assign(src_part))) {
      LOG_WARN("Failed to assign ObBasePartition", K(ret));
    } else if (OB_FAIL(deep_copy_obj(src_part.sub_interval_start_, sub_interval_start_))) {
      LOG_WARN("Fail to deep copy sub_interval_start_", K(ret));
    } else if (OB_FAIL(deep_copy_obj(src_part.sub_part_interval_, sub_part_interval_))) {
      LOG_WARN("Fail to deep copy sub_part_interval_", K(ret));
    } else {
      sub_part_num_ = src_part.sub_part_num_;
#define ASSIGN_SUBPARTITION_ARRAY(SUBPART_NAME) \
      if (OB_SUCC(ret) && src_part.SUBPART_NAME##_num_ > 0) { \
        int64_t subpartition_num = src_part.SUBPART_NAME##_num_; \
        if (OB_FAIL(preserve_array(SUBPART_NAME##_array_, SUBPART_NAME##_array_capacity_, subpartition_num))) { \
          LOG_WARN("Fail to preserve "#SUBPART_NAME" array", KR(ret), KP(SUBPART_NAME##_array_), K(SUBPART_NAME##_array_capacity_), K(subpartition_num)); \
        } else if (OB_ISNULL(src_part.SUBPART_NAME##_array_)) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN(#SUBPART_NAME"_array_ is null", KR(ret)); \
        } \
        ObSubPartition *subpartition = NULL; \
        for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; i++) { \
          subpartition = src_part.SUBPART_NAME##_array_[i]; \
          if (OB_ISNULL(subpartition)) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("the subpartition is null", KR(ret)); \
          } else if (OB_FAIL(add_partition(*subpartition))) { \
            LOG_WARN("Fail to add subpartition", KR(ret), K(i)); \
          } \
        } \
      }
      ASSIGN_SUBPARTITION_ARRAY(subpartition);
      ASSIGN_SUBPARTITION_ARRAY(hidden_subpartition);
#undef ASSIGN_SUBPARTITION_ARRAY
    }//do nothing
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPartition)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObBasePartition));
  LST_DO_CODE(OB_UNIS_ENCODE,
              sub_part_num_,
              sub_interval_start_,
              sub_part_interval_);
  if (FAILEDx(ObSchemaUtils::serialize_partition_array(
              subpartition_array_,
              subpartition_num_,
              buf, buf_len, pos))) {
    LOG_WARN("fail to seriablize subpartition array", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::serialize_partition_array(
                     hidden_subpartition_array_,
                     hidden_subpartition_num_,
                     buf, buf_len, pos))) {
    LOG_WARN("fail to seriablize hidden subpartition array", KR(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPartition)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObBasePartition));
  ObObj sub_interval_start;
  ObObj sub_part_interval;
  LST_DO_CODE(OB_UNIS_DECODE,
              sub_part_num_,
              sub_interval_start,
              sub_part_interval);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_obj(sub_interval_start, sub_interval_start_))) {
    LOG_WARN("Fail to deep copy sub_interval_start", K(ret), K(sub_interval_start));
  } else if (OB_FAIL(deep_copy_obj(sub_part_interval, sub_part_interval_))) {
    LOG_WARN("Fail to deep copy sub_part_interval", K(ret), K(sub_part_interval));
  } else if (OB_FAIL(deserialize_subpartition_array(buf, data_len, pos))) { // subpartition_array_
    LOG_WARN("fail to deserialize subpartition_array", KR(ret));
  } else if (OB_FAIL(deserialize_subpartition_array(buf, data_len, pos))) { // hidden_subpartition_array_
    LOG_WARN("fail to deserialize hidden_subpartition_array", KR(ret));
  } else {}//do nothing
  return ret;
}

int ObPartition::deserialize_subpartition_array(
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (data_len > 0) {
    int64_t count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("Fail to decode partition count", K(ret));
    } else {
      ObSubPartition subpartition;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        subpartition.reset();
        if (OB_FAIL(subpartition.deserialize(buf, data_len, pos))) {
          LOG_WARN("Fail to deserialize subpartition", K(ret));
        } else if (OB_FAIL(add_partition(subpartition))) {
          LOG_WARN("Fail to add subpartition", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPartition)
{
  int64_t len = ObBasePartition::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              sub_part_num_,
              sub_interval_start_,
              sub_part_interval_);

  len += ObSchemaUtils::get_partition_array_serialize_size(
         subpartition_array_, subpartition_num_);
  len += ObSchemaUtils::get_partition_array_serialize_size(
         hidden_subpartition_array_, hidden_subpartition_num_);
  return len;
}

int64_t ObPartition::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += ObBasePartition::get_deep_copy_size();
  convert_size += sub_interval_start_.get_deep_copy_size();
  convert_size += sub_part_interval_.get_deep_copy_size();
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  subpartition_array_, subpartition_num_);
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  hidden_subpartition_array_, hidden_subpartition_num_);
  return convert_size;
}

int ObPartition::add_partition(const ObSubPartition &subpartition)
{
  int ret = OB_SUCCESS;
  ObSubPartition *local = OB_NEWx(ObSubPartition, (get_allocator()), (get_allocator()));
  if (NULL == local) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(local->assign(subpartition))) {
    LOG_WARN("failed to assign partition", K(ret));
  } else if (subpartition.is_hidden_partition()) {
    if (OB_FAIL(inner_add_partition(*local,
                                    hidden_subpartition_array_,
                                    hidden_subpartition_array_capacity_,
                                    hidden_subpartition_num_))) {
      LOG_WARN("add subpartition failed", K(subpartition), K(ret));
    }
  } else {
    if (OB_FAIL(inner_add_partition(*local,
                                    subpartition_array_,
                                    subpartition_array_capacity_,
                                    subpartition_num_))) {
      LOG_WARN("add subpartition failed", K(subpartition), K(ret));
    }
  }
  return ret;
}

int ObPartition::inner_add_partition(
    const ObSubPartition &part,
    ObSubPartition **&part_array,
    int64_t &part_array_capacity,
    int64_t &part_num)
{
  int ret = common::OB_SUCCESS;
  if (0 == part_array_capacity) {
    if (NULL == (part_array = static_cast<ObSubPartition **>(
                 alloc(sizeof(ObSubPartition *) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition arrary");
    } else {
      part_array_capacity = DEFAULT_ARRAY_CAPACITY;
    }
  } else if (part_num >= part_array_capacity) {
    int64_t new_size = 2 * part_array_capacity;
    ObSubPartition **tmp = NULL;
    if (NULL == (tmp = static_cast<ObSubPartition **>(
                 alloc((sizeof(ObSubPartition *) * new_size))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition array", K(new_size));
    } else {
      MEMCPY(tmp, part_array, sizeof(ObSubPartition *) * part_num);
      MEMSET(tmp + part_num, 0, sizeof(ObSubPartition *) * (new_size - part_num));
      free(part_array);
      part_array = tmp;
      part_array_capacity = new_size;
    }
  }
  if (OB_SUCC(ret)) {
    part_array[part_num] = const_cast<ObSubPartition *>(&part);
    ++part_num;
  }
  return ret;
}

int ObPartition::get_max_sub_part_idx(int64_t &sub_part_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(subpartition_array_)
      || subpartition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP_(subpartition_array), K_(subpartition_num));
  } else {
    int64_t max_sub_part_idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num_; i++) {
      const ObSubPartition *subpart = subpartition_array_[i];
      if (OB_ISNULL(subpart)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", KR(ret), K(i));
      } else {
        max_sub_part_idx = max(max_sub_part_idx, subpart->get_sub_part_idx());
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (max_sub_part_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max_sub_part_idx is invalid", KR(ret), K(max_sub_part_idx));
      } else {
        sub_part_idx = max_sub_part_idx;
      }
    }
  }
  return ret;
}

int ObPartition::preserve_subpartition(const int64_t &capacity) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(preserve_array(subpartition_array_, subpartition_array_capacity_, capacity))) {
    LOG_WARN("fail to preserve subpartition array", KR(ret), KP(subpartition_array_), K(subpartition_array_capacity_), K(capacity));
  }
  return ret;
}

int ObPartition::get_normal_subpartition_index_by_id(const int64_t subpart_id,
                                      int64_t &subpartition_index) const
{
  int ret = OB_SUCCESS;
  subpartition_index = OB_INVALID_INDEX;
  bool finded = false;
  if (OB_INVALID_ID == subpart_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpart_id is invalid", KR(ret), K(subpart_id));
  } else {
    if (OB_ISNULL(subpartition_array_) || OB_UNLIKELY(subpartition_num_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid subpartition array", KR(ret), K(subpartition_array_), K(subpartition_num_));
    }
    for (int64_t i = 0; !finded && i < subpartition_num_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(subpartition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid subpartition", KR(ret), K(i));
      } else if (subpart_id == subpartition_array_[i]->get_sub_part_id()) {
        subpartition_index = i;
        finded = true;
      }
    }
  }
  if (OB_SUCC(ret) && !finded) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find subpartition index", KR(ret), K(subpart_id));
  }
  return ret;
}

int ObPartition::get_normal_subpartition_by_subpartition_index(const int64_t subpartition_index,
                                                const ObSubPartition *&subpartition) const
{
  int ret = OB_SUCCESS;
  subpartition = nullptr;
  const int64_t subpart_num = subpartition_num_;
  if (0 <= subpartition_index && subpart_num > subpartition_index) {
    if (OB_ISNULL(get_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition array is null", KR(ret), K(subpartition_index));
    } else {
      subpartition = get_subpart_array()[subpartition_index];
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid subpartition index", KR(ret), K(subpartition_index));
  }
  return ret;
}
ObSubPartition::ObSubPartition()
  : ObBasePartition()
{
  reset();
}

ObSubPartition::ObSubPartition(ObIAllocator *allocator)
  : ObBasePartition(allocator)
{
  reset();
}

void ObSubPartition::reset()
{
  subpart_id_ = OB_INVALID_INDEX;
  subpart_idx_ = OB_INVALID_INDEX;
  ObBasePartition::reset();
}

int ObSubPartition::clone(common::ObIAllocator &allocator, ObSubPartition *&dst) const
{
  int ret = OB_SUCCESS;
  dst = NULL;

  ObSubPartition *new_part = OB_NEWx(ObSubPartition, (&allocator));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(ret));
  } else if (OB_FAIL(new_part->assign(*this))) {
    LOG_ERROR("assign failed", K(ret));
  } else {
    dst = new_part;
  }

  return ret;
}

bool ObSubPartition::less_than(const ObSubPartition *lhs, const ObSubPartition *rhs)
{
  bool b_ret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else if (lhs->get_part_id() < rhs->get_part_id()) {
    b_ret = true;
  } else if (lhs->get_part_id() == rhs->get_part_id()) {
    ObNewRow lrow;
    lrow.cells_ = const_cast<ObObj*>(lhs->high_bound_val_.get_obj_ptr());
    lrow.count_ = lhs->high_bound_val_.get_obj_cnt();
    lrow.projector_ = lhs->projector_;
    lrow.projector_size_ = lhs->projector_size_;
    ObNewRow rrow;
    rrow.cells_ = const_cast<ObObj*>(rhs->high_bound_val_.get_obj_ptr());
    rrow.count_ = rhs->high_bound_val_.get_obj_cnt();
    rrow.projector_ = rhs->projector_;
    rrow.projector_size_ = rhs->projector_size_;
    int cmp = 0;
    if (OB_SUCCESS != ObRowUtil::compare_row(lrow, rrow, cmp)) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs is invalid");
    } else {
      b_ret = (cmp < 0);
    }
  } else { }//do nothing
  return b_ret;
}

int ObSubPartition::assign(const ObSubPartition &src_part)
{
  int ret = OB_SUCCESS;
  if (this != &src_part) {
    reset();
    if (OB_FAIL(ObBasePartition::assign(src_part))) {//Must assign base class first
      LOG_WARN("Failed to assign ObBasePartition", K(ret));
    } else {
      subpart_id_ = src_part.subpart_id_;
      subpart_idx_ = src_part.subpart_idx_;
    }
  }
  return ret;
}

bool ObSubPartition::key_match(const ObSubPartition &other) const
{
  return get_table_id() == other.get_table_id()
         && get_part_id() == other.get_part_id()
         && get_sub_part_id() == other.get_sub_part_id();
}

OB_DEF_SERIALIZE(ObSubPartition)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObBasePartition));
  LST_DO_CODE(OB_UNIS_ENCODE,
              subpart_id_,
              subpart_idx_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSubPartition)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObBasePartition));
  LST_DO_CODE(OB_UNIS_DECODE,
              subpart_id_,
              subpart_idx_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSubPartition)
{
  int64_t len = ObBasePartition::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              subpart_id_,
              subpart_idx_);
  return len;
}

int64_t ObSubPartition::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += ObBasePartition::get_deep_copy_size();
  return convert_size;
}

bool ObSubPartition::hash_like_func_less_than(const ObSubPartition *lhs, const ObSubPartition *rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else {
    int cmp  = static_cast<int32_t>(lhs->get_part_idx() - rhs->get_part_idx());
    if (0 == cmp) {
      cmp = static_cast<int32_t>(lhs->get_sub_part_idx() - rhs->get_sub_part_idx());
      bret = (cmp < 0);
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}
///////////////////////////////////////////////////////////////////////////////////////
int ObPartitionUtils::check_param_valid_(
    const share::schema::ObTableSchema &table_schema,
    RelatedTableInfo *related_table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
   if (!table_schema.has_tablet()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema has no tablet", KR(ret), K(tenant_id), K(table_id),
             "table_type", table_schema.get_table_type(),
             "index_type", table_schema.get_index_type());
  } else if (OB_ISNULL(related_table)) {
    // skip
  } else if (!related_table->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("related_table is invalid", KR(ret),
             KP(related_table->related_tids_), KP(related_table->related_map_));
  } else if (related_table->related_tids_->count() <= 0) {
    // skip
  } else {
    ObSchemaGetterGuard *guard = related_table->guard_;
    const uint64_t data_table_id = table_schema.get_data_table_id();
    if (table_schema.is_global_index_table()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't purning global index table with other tables",
               KR(ret), K(tenant_id), K(table_id));
    } else {
      // 1. get data table schema
      const ObTableSchema *data_schema = NULL;
      const bool is_index = table_schema.is_storage_local_index_table();
      if (!is_index) {
        data_schema = &table_schema;
      } else { // local index
        bool finded = false;
        const uint64_t data_table_id = table_schema.get_data_table_id();
        for (int64_t i = 0; OB_SUCC(ret) && !finded && i < related_table->related_tids_->count(); i++) {
          if (table_schema.get_data_table_id() == related_table->related_tids_->at(i)) {
            finded = true;
          }
        }
        if (OB_SUCC(ret) && !finded) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("local index's data table not exist in related tids", KR(ret),
                   "index_id", table_id, K(data_table_id), "related_tids",
                   ObArrayWrap<uint64_t>(related_table->related_tids_->get_data(),
                                         related_table->related_tids_->count()));
        }
        if (FAILEDx(guard->get_table_schema(
            tenant_id, data_table_id, data_schema))) {
          LOG_WARN("fail to get data table schema", KR(ret), K(tenant_id), K(data_table_id));
        } else if (OB_ISNULL(data_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("data table schema not exist", KR(ret), K(tenant_id), K(data_table_id));
        }
      }
      // 2. check data table is correspond to related_table
      if (OB_SUCC(ret)) {
        // FIXME: Can be optimized if tids arrays are sorted.
        const ObIArray<ObAuxTableMetaInfo> &simple_index_infos = data_schema->get_simple_index_infos();
        // if table_schema is local index, check its existence in data table schema.
        bool index_exist = !is_index;
        for (int64_t i = 0; OB_SUCC(ret) && i < related_table->related_tids_->count(); i++) {
          const uint64_t related_tid =
            share::is_oracle_mapping_real_virtual_table(related_table->related_tids_->at(i)) ?
                  ObSchemaUtils::get_real_table_mappings_tid(related_table->related_tids_->at(i))
                  : related_table->related_tids_->at(i);
          bool finded = false;
          for (int64_t j = 0; !finded && OB_SUCC(ret) && j < simple_index_infos.count(); j++) {
            const ObAuxTableMetaInfo &index_info = simple_index_infos.at(j);
            if (is_index_local_storage(index_info.index_type_)
                && related_tid == index_info.table_id_) {
              finded = true;
            }
            if (!index_exist && table_id == index_info.table_id_) {
              index_exist = true;
            }
          } // end for simple_index_infos
          if (OB_SUCC(ret) && !finded && data_schema->has_mlog_table()) {
            finded = (related_tid == data_schema->get_mlog_tid());
          }
          if (OB_SUCC(ret) && !finded && related_tid != data_table_id) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("local index not exist", KR(ret), K(table_id));
          }
        } // end for related_tids
        if (OB_SUCC(ret) && !index_exist) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("local index not exist in data table's index_infos", KR(ret),
                   "index_id", table_id, K(data_table_id));
        }
      }
    }
  }
  return ret;
}

// check_param_valid_() should be run first
int ObPartitionUtils::fill_tablet_and_object_ids_(
    const bool fill_tablet_id,
    const int64_t part_idx,
    const common::ObIArray<PartitionIndex> &partition_indexes,
    const share::schema::ObTableSchema &table_schema,
    RelatedTableInfo *related_table,
    common::ObIArray<common::ObTabletID> &tablet_ids,
    common::ObIArray<common::ObObjectID> &object_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_indexes.count(); i++) {
    const PartitionIndex &index =  partition_indexes.at(i);
    const uint64_t src_table_id = table_schema.get_table_id();
    ObTabletID src_tablet_id;
    ObObjectID src_object_id;
    ObObjectID src_first_level_part_id;
    // part_idx is valid when dealing with composited-partitioned table
    int64_t actual_part_idx = part_idx >= 0 ? part_idx : index.get_part_idx();
    int64_t actual_subpart_idx = index.get_subpart_idx();
    if (OB_FAIL(table_schema.get_tablet_and_object_id_by_index(
        actual_part_idx, actual_subpart_idx,
        src_tablet_id, src_object_id, src_first_level_part_id))) {
      LOG_WARN("fail to get tablet and object id", KR(ret), K(part_idx), K(index));
    } else if (fill_tablet_id && OB_FAIL(tablet_ids.push_back(src_tablet_id))) {
      LOG_WARN("fail to push back tablet_id", KR(ret), K(src_tablet_id));
    } else if (OB_FAIL(object_ids.push_back(src_object_id))) {
      LOG_WARN("fail to push back tablet_id", KR(ret), K(src_object_id));
    } else if (OB_NOT_NULL(related_table) && fill_tablet_id) {
      ObSchemaGetterGuard *guard = related_table->guard_;
      // Won't set related_table if dealing with first part in composited-partitioned tables.
      for (int64_t j = 0; OB_SUCC(ret) && j < related_table->related_tids_->count(); j++) {
        const uint64_t related_table_id = related_table->related_tids_->at(j);
        ObTabletID related_tablet_id;
        ObObjectID related_object_id;
        ObObjectID related_first_level_part_id;
        const ObSimpleTableSchemaV2 *related_schema = NULL;
        if (OB_FAIL(guard->get_simple_table_schema(tenant_id, related_table_id, related_schema))) {
          LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id), K(related_table_id));
        } else if (OB_ISNULL(related_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", KR(ret), K(tenant_id), K(related_table_id));
        } else if (OB_FAIL(related_schema->get_tablet_and_object_id_by_index(
                   actual_part_idx, actual_subpart_idx,
                   related_tablet_id, related_object_id, related_first_level_part_id))) {
          LOG_WARN("fail to get tablet and object id", KR(ret), K(part_idx), K(index));
        } else if (OB_FAIL(related_table->related_map_->add_related_tablet_id(
                   src_tablet_id, related_table_id, related_tablet_id, related_object_id, related_first_level_part_id))) {
          LOG_WARN("fail to add related tablet info", KR(ret),
                   K(src_table_id), K(src_tablet_id), K(src_object_id),
                   K(related_table_id), K(related_tablet_id), K(related_object_id));
        }
      } // end for related tids
    }
  } // end for partition_indexes
  return ret;
}

int ObPartitionUtils::get_tablet_and_object_id(
    const share::schema::ObTableSchema &table_schema,
    common::ObTabletID &tablet_id,
    common::ObObjectID &object_id,
    RelatedTableInfo *related_table /*= NULL*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_param_valid_(table_schema, related_table))) {
    LOG_WARN("fail to check param", KR(ret), K(table_schema), KP(related_table));
  } else if (OB_FAIL(table_schema.get_tablet_and_object_id(tablet_id, object_id))) {
    LOG_WARN("fail to get tablet id and object id", KR(ret), K(table_schema));
  } else if (OB_NOT_NULL(related_table)) {
    ObSchemaGetterGuard *guard = related_table->guard_;
    const uint64_t tenant_id = table_schema.get_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < related_table->related_tids_->count(); i++) {
      const uint64_t related_table_id =
          share::is_oracle_mapping_real_virtual_table(related_table->related_tids_->at(i)) ?
                ObSchemaUtils::get_real_table_mappings_tid(related_table->related_tids_->at(i))
                : related_table->related_tids_->at(i);
      const ObSimpleTableSchemaV2 *related_schema = NULL;
      ObTabletID related_tablet_id;
      ObObjectID related_object_id;
      if (OB_FAIL(guard->get_simple_table_schema(tenant_id,
                  related_table_id, related_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(related_table_id));
      } else if (OB_ISNULL(related_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(tenant_id), K(related_table_id));
      } else if (OB_FAIL(related_schema->get_tablet_and_object_id(
                 related_tablet_id, related_object_id))) {
        LOG_WARN("fail to get tablet id and object id", KR(ret), K(related_table_id));
      } else if (OB_FAIL(related_table->related_map_->add_related_tablet_id(
                 tablet_id, related_table_id, related_tablet_id, related_object_id, OB_INVALID_ID))) {
        LOG_WARN("fail to add related tablet info", KR(ret),
                 "src_table_id", table_schema.get_table_id(),
                 "src_tablet_id", tablet_id, "src_object_id", object_id,
                 K(related_table_id), K(related_tablet_id), K(related_object_id));
      }
    } // end for
  }
  return ret;
}

int ObPartitionUtils::get_tablet_and_part_id(
    const share::schema::ObTableSchema &table_schema,
    const common::ObNewRange &range,
    common::ObIArray<common::ObTabletID> &tablet_ids,
    common::ObIArray<common::ObObjectID> &part_ids,
    RelatedTableInfo *related_table /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartitionIndex, DEFAULT_PARTITION_INDEX_NUM> partition_indexes;
  ObPartitionLevel part_level = table_schema.get_part_level();
  const uint64_t table_id = table_schema.get_table_id();
  if (OB_FAIL(check_param_valid_(table_schema, related_table))) {
    LOG_WARN("fail to check param", KR(ret), K(table_schema), KP(related_table));
  } else if (PARTITION_LEVEL_ONE == part_level
             || PARTITION_LEVEL_TWO == part_level) {
    ObPartition * const* part_array = table_schema.get_part_array();
    const int64_t part_num = table_schema.get_partition_num();
    if (table_schema.is_hash_like_part()) {
      if (OB_FAIL(ObPartitionUtils::get_hash_tablet_and_part_id_(
                  range, part_array, part_num, partition_indexes))) {
        LOG_WARN("fail to fill hash tablet_id and part_id",
                 KR(ret), K(range), K(table_id));
      }
    } else if (table_schema.is_range_part()) {
      if (OB_FAIL(ObPartitionUtils::get_range_tablet_and_part_id_(
                  range, part_array, part_num, partition_indexes))) {
        LOG_WARN("fail to fill range tablet_id and part_id",
                 KR(ret), K(range), K(table_id));
      }
    } else if (table_schema.is_list_part()) {
      if (OB_FAIL(ObPartitionUtils::get_list_tablet_and_part_id_(
                  range, part_array, part_num, partition_indexes))) {
        LOG_WARN("fail to fill list tablet_id and part_id",
                 KR(ret), K(range), K(table_id));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not suppored part option", KR(ret), K(table_id),
               "part_option", table_schema.get_part_option());
    }
    const bool fill_tablet_id = (PARTITION_LEVEL_ONE == part_level);
    if (FAILEDx(fill_tablet_and_object_ids_(
        fill_tablet_id, OB_INVALID_INDEX /*part_idx*/,
        partition_indexes, table_schema, related_table,
        tablet_ids, part_ids))) {
      LOG_WARN("fail to fill tablet and part_ids", KR(ret), K(fill_tablet_id),
               K(table_id), K(partition_indexes));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported part level", KR(ret), K(table_id), K(part_level));
  }
  LOG_TRACE("table schema get tablet and part id", K(table_id), K(tablet_ids), K(part_ids), K(partition_indexes));
  return ret;
}

int ObPartitionUtils::get_tablet_and_part_id(
    const share::schema::ObTableSchema &table_schema,
    const common::ObNewRow &row,
    common::ObTabletID &tablet_id,
    common::ObObjectID &part_id,
    RelatedTableInfo *related_table /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartitionIndex, 1> partition_indexes;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObObjectID, 1> part_ids;
  ObPartitionLevel part_level = table_schema.get_part_level();
  const uint64_t table_id = table_schema.get_table_id();
  tablet_id.reset();
  part_id = OB_INVALID_ID;
  if (OB_FAIL(check_param_valid_(table_schema, related_table))) {
    LOG_WARN("fail to check param", KR(ret), K(table_schema), KP(related_table));
  } else if (PARTITION_LEVEL_ONE == part_level
             || PARTITION_LEVEL_TWO == part_level) {
    ObPartition * const* part_array = table_schema.get_part_array();
    const int64_t part_num = table_schema.get_partition_num();
    if (table_schema.is_hash_like_part()) {
      if (OB_FAIL(ObPartitionUtils::get_hash_tablet_and_part_id_(
                  row, part_array, part_num, partition_indexes))) {
        LOG_WARN("fail to fill hash tablet_id and part_id",
                 KR(ret), K(row), K(table_id));
      }
    } else if (table_schema.is_range_part()) {
      if (OB_FAIL(ObPartitionUtils::get_range_tablet_and_part_id_(
                  row, part_array, part_num, partition_indexes))) {
        LOG_WARN("fail to fill range tablet_id and part_id",
                 KR(ret), K(row), K(table_id));
      }
    } else if (table_schema.is_list_part()) {
      if (OB_FAIL(ObPartitionUtils::get_list_tablet_and_part_id_(
                  row, part_array, part_num, partition_indexes))) {
        LOG_WARN("fail to fill list tablet_id and part_id",
                 KR(ret), K(row), K(table_id));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not suppored part option", KR(ret), K(table_id),
               "part_option", table_schema.get_part_option());
    }
    const bool fill_tablet_id = (PARTITION_LEVEL_ONE == part_level);
    if (FAILEDx(fill_tablet_and_object_ids_(
        fill_tablet_id, OB_INVALID_INDEX /*part_idx*/,
        partition_indexes, table_schema, related_table,
        tablet_ids, part_ids))) {
      LOG_WARN("fail to fill tablet and part_ids", KR(ret), K(fill_tablet_id),
               K(table_id), K(partition_indexes));
    } else if (1 < part_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_ids count is invalid", KR(ret), K(part_ids));
    } else if (part_ids.count() > 0) {
      part_id = part_ids.at(0);
      if (!fill_tablet_id) {
        // skip
      } else if (1 != tablet_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_ids count is invalid", KR(ret), K(tablet_ids));
      } else {
        tablet_id = tablet_ids.at(0);
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported part level", KR(ret), K(table_id), K(part_level));
  }
  LOG_TRACE("table schema get tablet and part id", K(table_id), K(tablet_ids), K(part_ids), K(partition_indexes));
  return ret;
}

int ObPartitionUtils::get_tablet_and_subpart_id(
      const share::schema::ObTableSchema &table_schema,
      const common::ObPartID &part_id,
      const common::ObNewRange &range,
      common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<common::ObObjectID> &subpart_ids,
      RelatedTableInfo *related_table /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartitionIndex, DEFAULT_PARTITION_INDEX_NUM> partition_indexes;
  ObPartitionLevel part_level = table_schema.get_part_level();
  const uint64_t table_id = table_schema.get_table_id();
  const ObPartition *partition = NULL;
  int64_t part_idx = OB_INVALID_ID;
  if (OB_FAIL(check_param_valid_(table_schema, related_table))) {
    LOG_WARN("fail to check param", KR(ret), K(table_schema), KP(related_table));
  } else if (PARTITION_LEVEL_TWO != part_level) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported part level", KR(ret), K(part_level));
  } else if (OB_FAIL(table_schema.get_partition_index_by_id(
             part_id, CHECK_PARTITION_MODE_NORMAL, part_idx))) {
    LOG_WARN("fail to get part_idx by part_id", KR(ret), K(part_id));
  } else if (OB_FAIL(table_schema.get_partition_by_partition_index(
             part_idx, CHECK_PARTITION_MODE_NORMAL, partition))) {
    LOG_WARN("fail to get partition by part_idx", KR(ret), K(part_idx));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition not exist", KR(ret), K(part_id), K(part_idx));
  } else {
    ObSubPartition * const* subpartition_array = partition->get_subpart_array();
    int64_t subpartition_num = partition->get_subpartition_num();
    if (table_schema.is_hash_like_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_hash_tablet_and_subpart_id_(
                  part_id, range,
                  subpartition_array, subpartition_num,
                  partition_indexes))) {
        LOG_WARN("fail to fill hash tablet_id and subpart_id",
                 KR(ret), K(part_id), K(range), K(table_id));
      }
    } else if (table_schema.is_range_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_range_tablet_and_subpart_id_(
                  part_id, range,
                  subpartition_array, subpartition_num,
                  partition_indexes))) {
        LOG_WARN("fail to fill range tablet_id and subpart_id",
                 KR(ret), K(part_id), K(range), K(table_id));
      }
    } else if (table_schema.is_list_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_list_tablet_and_subpart_id_(
                  part_id, range,
                  subpartition_array, subpartition_num,
                  partition_indexes))) {
        LOG_WARN("fail to fill list tablet_id and subpart_id",
                 KR(ret), K(part_id), K(range), K(table_id));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported subpart option", KR(ret), K(table_id),
               "subpart_option", table_schema.get_sub_part_option());
    }
    const bool fill_tablet_id = true;
    if (FAILEDx(fill_tablet_and_object_ids_(
        fill_tablet_id, part_idx, partition_indexes, table_schema,
        related_table, tablet_ids, subpart_ids))) {
      LOG_WARN("fail to fill tablet and subpart_ids", KR(ret),
               K(fill_tablet_id), K(table_id), K(partition_indexes));
    }
    LOG_TRACE("table schema get tablet and subpart id",
               K(table_id), K(tablet_ids), K(subpart_ids));
  }
  return ret;
}

int ObPartitionUtils::get_tablet_and_subpart_id(
    const share::schema::ObTableSchema &table_schema,
    const common::ObPartID &part_id,
    const common::ObNewRow &row,
    common::ObTabletID &tablet_id,
    common::ObObjectID &subpart_id,
    RelatedTableInfo *related_table /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartitionIndex, 1> partition_indexes;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObObjectID, 1> subpart_ids;
  ObPartitionLevel part_level = table_schema.get_part_level();
  const uint64_t table_id = table_schema.get_table_id();
  tablet_id.reset();
  subpart_id = OB_INVALID_ID;
  const ObPartition *partition = NULL;
  int64_t part_idx = OB_INVALID_ID;
  if (OB_FAIL(check_param_valid_(table_schema, related_table))) {
    LOG_WARN("fail to check param", KR(ret), K(table_schema), KP(related_table));
  } else if (PARTITION_LEVEL_TWO != part_level) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported part level", KR(ret), K(part_level));
  } else if (OB_FAIL(table_schema.get_partition_index_by_id(
             part_id, CHECK_PARTITION_MODE_NORMAL, part_idx))) {
    LOG_WARN("fail to get part_idx by part_id", KR(ret), K(part_id));
  } else if (OB_FAIL(table_schema.get_partition_by_partition_index(
             part_idx, CHECK_PARTITION_MODE_NORMAL, partition))) {
    LOG_WARN("fail to get partition by part_idx", KR(ret), K(part_idx));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition not exist", KR(ret), K(part_id), K(part_idx));
  } else {
    ObSubPartition * const* subpartition_array = partition->get_subpart_array();
    int64_t subpartition_num = partition->get_subpartition_num();
    if (table_schema.is_hash_like_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_hash_tablet_and_subpart_id_(
                  part_id, row,
                  subpartition_array, subpartition_num,
                  partition_indexes))) {
        LOG_WARN("fail to fill hash tablet_id and subpart_id",
                 KR(ret), K(part_id), K(row), K(table_id));
      }
    } else if (table_schema.is_range_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_range_tablet_and_subpart_id_(
                  part_id, row,
                  subpartition_array, subpartition_num,
                  partition_indexes))) {
        LOG_WARN("fail to fill range tablet_id and subpart_id",
                 KR(ret), K(part_id), K(row), K(table_id));
      }
    } else if (table_schema.is_list_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_list_tablet_and_subpart_id_(
                  part_id, row,
                  subpartition_array, subpartition_num,
                  partition_indexes))) {
        LOG_WARN("fail to fill list tablet_id and subpart_id",
                 KR(ret), K(part_id), K(row), K(table_id));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported subpart option", KR(ret), K(table_id),
               "subpart_option", table_schema.get_sub_part_option());
    }
    const bool fill_tablet_id = true;
    if (FAILEDx(fill_tablet_and_object_ids_(
        fill_tablet_id, part_idx, partition_indexes, table_schema,
        related_table, tablet_ids, subpart_ids))) {
      LOG_WARN("fail to fill tablet and subpart_ids", KR(ret),
               K(fill_tablet_id), K(table_id), K(partition_indexes));
    } else if (1 < subpart_ids.count()
               || subpart_ids.count() != tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpart_ids/tablet_ids count is invalid",
               KR(ret), K(subpart_ids), K(tablet_ids));
    } else if (subpart_ids.count() > 0) {
      subpart_id = subpart_ids.at(0);
      tablet_id = tablet_ids.at(0);
    }
    LOG_TRACE("table schema get tablet and subpart id",
               K(table_id), K(tablet_ids), K(subpart_ids));
  }
  return ret;
}

int ObPartitionUtils::get_all_tablet_and_part_id_(
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  if (OB_UNLIKELY(
      OB_ISNULL(partition_array)
      || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
             KR(ret), KP(partition_array), K(partition_num));
  } else {
    const ObPartition *partition = NULL;
    for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < partition_num; part_idx++) {
      if (OB_ISNULL(partition = partition_array[part_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(part_idx));
      } else if (OB_FAIL(indexes.push_back(PartitionIndex(part_idx, OB_INVALID_INDEX)))) {
        LOG_WARN("fail to push back part_idx", KR(ret), K(part_idx));
      }
    } // end for
  }
  return ret;
}

int ObPartitionUtils::get_all_tablet_and_subpart_id_(
    const ObPartID &part_id,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      OB_ISNULL(subpartition_array)
      || subpartition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP(subpartition_array), K(subpartition_num));
  } else {
    const ObSubPartition *subpartition = NULL;
    for (int64_t subpart_idx = 0; OB_SUCC(ret) && subpart_idx < subpartition_num; subpart_idx++) {
      if (OB_ISNULL(subpartition = subpartition_array[subpart_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition is null", KR(ret), K(subpart_idx));
      } else if (OB_UNLIKELY(static_cast<ObPartID>(subpartition->get_part_id()) != part_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_id not match", KR(ret), KPC(subpartition), K(part_id));
      } else if (!subpartition->get_tablet_id().is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_id", KR(ret), KPC(subpartition), K(part_id));
      } else if (OB_FAIL(indexes.push_back(PartitionIndex(OB_INVALID_INDEX, subpart_idx)))) {
        LOG_WARN("fail to push back subpart_idx", KR(ret), K(subpart_idx));
      }
    } // end for
  }
  return ret;
}

int ObPartitionUtils::get_range_tablet_and_part_id_(
    const ObPartition &start_bound,
    const ObPartition &end_bound,
    const ObBorderFlag &border_flag,
    ObPartition * const *partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  int64_t start_idx = OB_INVALID_INDEX;
  int64_t end_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(
      OB_ISNULL(partition_array)
      || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
             KR(ret), KP(partition_array), K(partition_num));
  } else if (OB_FAIL(get_start_(partition_array, partition_num, start_bound, start_idx))) {
    LOG_WARN("fail to get start idx", KR(ret), K(start_bound), K(partition_num));
  } else if (OB_FAIL(get_end_(partition_array, partition_num, border_flag, end_bound, end_idx))) {
    LOG_WARN("fail to get end idx", KR(ret), K(end_bound), K(border_flag), K(partition_num));
  } else if (OB_UNLIKELY(
             start_idx < 0
             || end_idx >= partition_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid start_idx or end_idx", KR(ret), K(start_idx), K(end_idx),
             K(partition_num), K(start_bound), K(end_bound));
  } else {
    const ObPartition *partition = NULL;
    for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; i++) {
      if (OB_ISNULL(partition = partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(i));
      } else if (OB_FAIL(indexes.push_back(PartitionIndex(i, OB_INVALID_INDEX)))) {
        LOG_WARN("fail to push back part_idx", KR(ret), K(i));
      }
    } // end for
  }
  return ret;
}

int ObPartitionUtils::get_range_tablet_and_subpart_id_(
    const ObSubPartition &start_bound,
    const ObSubPartition &end_bound,
    const ObBorderFlag &border_flag,
    const ObPartID &part_id,
    ObSubPartition * const *subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  int64_t start_idx = OB_INVALID_INDEX;
  int64_t end_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(
      OB_ISNULL(subpartition_array)
      || subpartition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP(subpartition_array), K(subpartition_num));
  } else if (OB_FAIL(get_start_(subpartition_array, subpartition_num, start_bound, start_idx))) {
    LOG_WARN("fail to get start idx", KR(ret), K(start_bound), K(subpartition_num));
  } else if (OB_FAIL(get_end_(subpartition_array, subpartition_num, border_flag, end_bound, end_idx))) {
    LOG_WARN("fail to get end idx", KR(ret), K(end_bound), K(border_flag), K(subpartition_num));
  } else if (OB_UNLIKELY(
             start_idx < 0
             || end_idx >= subpartition_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid start_idx or end_idx", KR(ret), K(start_idx), K(end_idx),
             K(subpartition_num), K(start_bound), K(end_bound));
  } else {
    const ObSubPartition *subpartition = NULL;
    for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; i++) {
      if (OB_ISNULL(subpartition = subpartition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition is null", KR(ret), K(i));
      } else if (OB_UNLIKELY(static_cast<ObPartID>(subpartition->get_part_id()) != part_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_id not match", KR(ret), KPC(subpartition), K(part_id));
      } else if (OB_UNLIKELY(!subpartition->get_tablet_id().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_id is invalid", KR(ret), KPC(subpartition));
      } else if (OB_FAIL(indexes.push_back(PartitionIndex(OB_INVALID_INDEX, i)))) {
        LOG_WARN("fail to push back subpart_idx", KR(ret), K(i));
      }
    } // end for
  }
  return ret;
}

int ObPartitionUtils::get_hash_tablet_and_part_id_(
    const common::ObNewRange &range,
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  const ObRowkey &start_key = range.get_start_key();
  if (OB_UNLIKELY(
      OB_ISNULL(partition_array)
      || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
             KR(ret), KP(partition_array), K(partition_num));
  } else if (!range.is_single_rowkey()
             || 1 != start_key.get_obj_cnt()
             || ObIntType != start_key.get_obj_ptr()[0].get_type()) {
    if (OB_FAIL(get_all_tablet_and_part_id_(
        partition_array, partition_num, indexes))) {
      LOG_WARN("fail to get all tablet and part_id", KR(ret));
    }
  } else {
    int64_t val = 0;
    int64_t part_idx = OB_INVALID_INDEX;
    const ObPartition *partition = NULL;
    if (OB_FAIL(start_key.get_obj_ptr()[0].get_int(val))) {
      LOG_WARN("Failed to get int val", KR(ret), K(start_key));
    } else if (OB_UNLIKELY(val < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("val is invalid", KR(ret), K(val), K(partition_num));
    } else if (OB_FAIL(calc_hash_part_idx(val, partition_num, part_idx))) {
      LOG_WARN("failed to calc hash part idx", KR(ret), K(val), K(partition_num));
    } else if (OB_UNLIKELY(part_idx >= partition_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid", KR(ret), K(val), K(part_idx), K(partition_num));
    } else if (OB_ISNULL(partition = partition_array[part_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(part_idx));
    } else if (OB_UNLIKELY(partition->get_part_idx() != part_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_idx not match", KR(ret), KPC(partition), K(part_idx));
    } else if (OB_FAIL(indexes.push_back(PartitionIndex(part_idx, OB_INVALID_INDEX)))) {
      LOG_WARN("fail to push back index", KR(ret), K(part_idx));
    }
  }
  return ret;
}

int ObPartitionUtils::get_range_tablet_and_part_id_(
    const common::ObNewRange &range,
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;

  ObPartition start_tmp;
  start_tmp.high_bound_val_ = range.start_key_;

  ObPartition end_tmp;
  end_tmp.high_bound_val_ = range.end_key_;

  return get_range_tablet_and_part_id_(start_tmp, end_tmp,
                                       range.border_flag_,
                                       partition_array, partition_num,
                                       indexes);
}

int ObPartitionUtils::get_list_tablet_and_part_id_(
    const common::ObNewRange &range,
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  if (OB_UNLIKELY(
      OB_ISNULL(partition_array)
      || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
              KR(ret), KP(partition_array), K(partition_num));
  } else if (!range.is_single_rowkey()) {
    if (OB_FAIL(get_all_tablet_and_part_id_(
        partition_array, partition_num, indexes))) {
      LOG_WARN("fail to get all tablet and part_id", KR(ret));
    }
  } else {
    ObNewRow row;
    row.cells_ = const_cast<ObObj*>(range.start_key_.get_obj_ptr());
    row.count_ = range.start_key_.get_obj_cnt();
    if (OB_FAIL(get_list_tablet_and_part_id_(
        row, partition_array, partition_num, indexes))) {
      LOG_WARN("fail to get list tablet and part_id", KR(ret), K(row));
    }
  }
  return ret;
}

int ObPartitionUtils::get_hash_tablet_and_part_id_(
    const common::ObNewRow &row,
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
   const ObObj *obj = NULL;
  if (OB_UNLIKELY(
      OB_ISNULL(partition_array)
      || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
             KR(ret), KP(partition_array), K(partition_num));
  } else if (OB_UNLIKELY(1 != row.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(row), KR(ret));
  } else if (FALSE_IT(obj = &(row.get_cell(0)))) {
  } else if (OB_UNLIKELY(!obj->is_int() && !obj->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(row), KR(ret));
  } else {
    // Hash the null value to partition 0
    int64_t val = obj->is_int() ? obj->get_int() : 0;
    int64_t part_idx = OB_INVALID_INDEX;
    const ObPartition *partition = NULL;
    if (OB_UNLIKELY(val < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("val is invalid", KR(ret), K(val), K(partition_num));
    } else if (OB_FAIL(calc_hash_part_idx(val, partition_num, part_idx))) {
      LOG_WARN("failed to calc hash part idx", KR(ret), K(partition_num), K(val));
    } else if (OB_UNLIKELY(part_idx >= partition_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_idx is invalid", KR(ret), K(val), K(part_idx), K(partition_num));
    } else if (OB_ISNULL(partition = partition_array[part_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(part_idx));
    } else if (OB_UNLIKELY(partition->get_part_idx() != part_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_idx not match", KR(ret), KPC(partition), K(part_idx));
    } else if (OB_FAIL(indexes.push_back(PartitionIndex(part_idx, OB_INVALID_INDEX)))) {
      LOG_WARN("fail to push back index", KR(ret), K(part_idx));
    }
  }
  return ret;
}

int ObPartitionUtils::get_range_tablet_and_part_id_(
    const common::ObNewRow &row,
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  ObPartition start_tmp;
  start_tmp.high_bound_val_.assign(row.cells_, row.count_);
  start_tmp.projector_ = row.projector_;
  start_tmp.projector_size_ = row.projector_size_;

  ObPartition end_tmp;
  end_tmp.high_bound_val_.assign(row.cells_, row.count_);
  end_tmp.projector_ = row.projector_;
  end_tmp.projector_size_ = row.projector_size_;

  ObBorderFlag border_flag;
  border_flag.set_inclusive_start();
  border_flag.set_inclusive_end();

  if (OB_FAIL(get_range_tablet_and_part_id_(start_tmp, end_tmp,
                                            border_flag,
                                            partition_array, partition_num,
                                            indexes))) {
    LOG_WARN("fail to get range tablet and part_id",
             KR(ret), K(start_tmp), K(end_tmp), K(border_flag));
  }
  return ret;
}

int ObPartitionUtils::get_list_tablet_and_part_id_(
    const common::ObNewRow &row,
    ObPartition * const* partition_array,
    const int64_t partition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      OB_ISNULL(partition_array)
      || partition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
             KR(ret), KP(partition_array), K(partition_num));
  } else {
    int64_t part_idx = OB_INVALID_INDEX;
    int64_t default_value_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == part_idx && i < partition_num; i++) {
      const ObIArray<common::ObNewRow> &list_row_values = partition_array[i]->get_list_row_values();
      for (int64_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == part_idx && j < list_row_values.count(); j++) {
        const ObNewRow &list_row = list_row_values.at(j);
        if (row == list_row) {
          part_idx = i;
        }
      } // end for
      if (list_row_values.count() == 1
          && list_row_values.at(0).get_count() >= 1
          && list_row_values.at(0).get_cell(0).is_max_value()) {
        // calc default value position
        default_value_idx = i;
      }
    } // end dor

    if (OB_SUCC(ret)) {
      const ObPartition *partition = NULL;
      part_idx = OB_INVALID_INDEX == part_idx ? default_value_idx : part_idx;
      if (OB_UNLIKELY(OB_INVALID_INDEX == part_idx)) {
        // return invalid part_id/tablet_id if partition not found.
        LOG_TRACE("partition not found", KR(ret), K(row));
      } else if (OB_ISNULL(partition = partition_array[part_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(part_idx));
      } else if (OB_FAIL(indexes.push_back(PartitionIndex(part_idx, OB_INVALID_INDEX)))) {
        LOG_WARN("fail to push back part_idx", KR(ret), K(part_idx));
      }
    }
  }
  return ret;
}

int ObPartitionUtils::get_hash_tablet_and_subpart_id_(
    const common::ObPartID &part_id,
    const common::ObNewRange &range,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  const ObRowkey &start_key = range.get_start_key();
  if (OB_UNLIKELY(
      OB_ISNULL(subpartition_array)
      || subpartition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP(subpartition_array), K(subpartition_num));
  } else if (OB_UNLIKELY(
             !range.is_single_rowkey()
             || 1 != start_key.get_obj_cnt()
             || ObIntType != start_key.get_obj_ptr()[0].get_type())) {
    if (OB_FAIL(get_all_tablet_and_subpart_id_(
        part_id, subpartition_array, subpartition_num, indexes))) {
      LOG_WARN("fail to get all tablet and subpart_id", KR(ret), K(part_id));
    }
  } else {
    int64_t val = 0;
    int64_t subpart_idx = OB_INVALID_INDEX;
    const ObSubPartition *subpartition = NULL;
    if (OB_FAIL(start_key.get_obj_ptr()[0].get_int(val))) {
      LOG_WARN("Failed to get int val", KR(ret), K(start_key));
    } else if (OB_UNLIKELY(val < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("val is invalid", KR(ret), K(val), K(subpartition_num));
    } else if (OB_FAIL(calc_hash_part_idx(val, subpartition_num, subpart_idx))) {
      LOG_WARN("failed to calc hash subpart idx", KR(ret), K(val), K(subpartition_num));
    } else if (OB_UNLIKELY(subpart_idx >= subpartition_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpart_idx is invalid", KR(ret), K(val), K(subpart_idx), K(subpartition_num));
    } else if (OB_ISNULL(subpartition = subpartition_array[subpart_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition is null", KR(ret), K(subpart_idx));
    } else if (OB_UNLIKELY(
               static_cast<ObPartID>(subpartition->get_part_id()) != part_id
               || subpartition->get_sub_part_idx() != subpart_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id or subpart_idx not match", KR(ret), KPC(subpartition), K(part_id), K(subpart_idx));
    } else if (OB_UNLIKELY(!subpartition->get_tablet_id().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet_id", KR(ret), KPC(subpartition), K(subpart_idx));
    } else if (OB_FAIL(indexes.push_back(PartitionIndex(OB_INVALID_INDEX, subpart_idx)))) {
      LOG_WARN("fail to push back subpart_idx", KR(ret), K(subpart_idx));
    }
  }
  return ret;
}

int ObPartitionUtils::get_range_tablet_and_subpart_id_(
    const common::ObPartID &part_id,
    const common::ObNewRange &range,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;

  ObSubPartition start_tmp;
  start_tmp.part_id_ = part_id;
  start_tmp.high_bound_val_ = range.start_key_;

  ObSubPartition end_tmp;
  end_tmp.part_id_ = part_id;
  end_tmp.high_bound_val_ = range.end_key_;

  return get_range_tablet_and_subpart_id_(start_tmp, end_tmp,
                                          range.border_flag_, part_id,
                                          subpartition_array, subpartition_num,
                                          indexes);
}

int ObPartitionUtils::get_list_tablet_and_subpart_id_(
    const common::ObPartID &part_id,
    const common::ObNewRange &range,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      OB_ISNULL(subpartition_array)
      || subpartition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP(subpartition_array), K(subpartition_num));
  } else if (!range.is_single_rowkey()) {
    if (OB_FAIL(get_all_tablet_and_subpart_id_(
        part_id, subpartition_array, subpartition_num, indexes))) {
      LOG_WARN("fail to get all tablet and subpart_id", KR(ret), K(part_id));
    }
  } else {
    ObTabletID tablet_id;
    ObObjectID subpart_id;
    ObNewRow row;
    row.cells_ = const_cast<ObObj*>(range.start_key_.get_obj_ptr());
    row.count_ = range.start_key_.get_obj_cnt();
    if (OB_FAIL(get_list_tablet_and_subpart_id_(part_id, row,
                                                subpartition_array, subpartition_num,
                                                indexes))) {
      LOG_WARN("fail to get list tablet and part_id", KR(ret), K(row));
    }
  }
  return ret;
}

int ObPartitionUtils::get_hash_tablet_and_subpart_id_(
    const common::ObPartID &part_id,
    const common::ObNewRow &row,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  if (OB_UNLIKELY(
      OB_ISNULL(subpartition_array)
      || subpartition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP(subpartition_array), K(subpartition_num));
  } else if (OB_UNLIKELY(
             1 != row.get_count()
             || (!row.get_cell(0).is_int() && !row.get_cell(0).is_null()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(row), KR(ret));
  } else {
    // Hash the null value to subpartition 0
    int64_t val = row.get_cell(0).is_int() ? row.get_cell(0).get_int() : 0;
    int64_t subpart_idx = OB_INVALID_INDEX;
    const ObSubPartition *subpartition = NULL;
    if (OB_UNLIKELY(val < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("val is invalid", KR(ret), K(val), K(subpartition_num));
    } else if (OB_FAIL(calc_hash_part_idx(val, subpartition_num, subpart_idx))) {
      LOG_WARN("failed to calc hash subpart idx", KR(ret), K(subpartition_num), K(val));
    } else if (OB_UNLIKELY(subpart_idx >= subpartition_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpart_idx is invalid", KR(ret), K(val), K(subpart_idx), K(subpartition_num));
    } else if (OB_ISNULL(subpartition = subpartition_array[subpart_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition is null", KR(ret), K(subpart_idx));
    } else if (OB_UNLIKELY(
               static_cast<ObPartID>(subpartition->get_part_id()) != part_id
               || subpartition->get_sub_part_idx() != subpart_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id or subpart_idx not match", KR(ret), KPC(subpartition), K(part_id), K(subpart_idx));
    } else if (OB_UNLIKELY(!subpartition->get_tablet_id().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet_id", KR(ret), KPC(subpartition), K(subpart_idx));
    } else if (OB_FAIL(indexes.push_back(PartitionIndex(OB_INVALID_INDEX, subpart_idx)))) {
      LOG_WARN("fail to push back subpart_idx", KR(ret), K(subpart_idx));
    }
  }
  return ret;
}

int ObPartitionUtils::get_range_tablet_and_subpart_id_(
    const common::ObPartID &part_id,
    const common::ObNewRow &row,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  ObSubPartition start_tmp;
  start_tmp.part_id_ = part_id;
  start_tmp.high_bound_val_.assign(row.cells_, row.count_);
  start_tmp.projector_ = row.projector_;
  start_tmp.projector_size_ = row.projector_size_;

  ObSubPartition end_tmp;
  end_tmp.part_id_ = part_id;
  end_tmp.high_bound_val_.assign(row.cells_, row.count_);
  end_tmp.projector_ = row.projector_;
  end_tmp.projector_size_ = row.projector_size_;

  ObBorderFlag border_flag;
  border_flag.set_inclusive_start();
  border_flag.set_inclusive_end();

  if (OB_FAIL(get_range_tablet_and_subpart_id_(start_tmp, end_tmp,
                                               border_flag, part_id,
                                               subpartition_array, subpartition_num,
                                               indexes))) {
    LOG_WARN("fail to get range tablet and subpart_id",
             KR(ret), K(start_tmp), K(end_tmp), K(border_flag));
  }
  return ret;
}

int ObPartitionUtils::get_list_tablet_and_subpart_id_(
    const common::ObPartID &part_id,
    const common::ObNewRow &row,
    ObSubPartition * const* subpartition_array,
    const int64_t subpartition_num,
    common::ObIArray<PartitionIndex> &indexes)
{
  int ret = OB_SUCCESS;
  indexes.reset();
  if (OB_UNLIKELY(
      OB_ISNULL(subpartition_array)
      || subpartition_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition_array is null or subpartition_num is invalid",
             KR(ret), KP(subpartition_array), K(subpartition_num));
  } else {
    int64_t subpart_idx = OB_INVALID_INDEX;
    int64_t default_value_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == subpart_idx && i < subpartition_num; i++) {
      const ObIArray<common::ObNewRow> &list_row_values = subpartition_array[i]->get_list_row_values();
      for (int64_t j = 0; OB_SUCC(ret) && OB_INVALID_INDEX == subpart_idx && j < list_row_values.count(); j++) {
        const ObNewRow &list_row = list_row_values.at(j);
        if (row == list_row) {
          subpart_idx = i;
        }
      } // end for
      if (list_row_values.count() == 1
          && list_row_values.at(0).get_count() >= 1
          && list_row_values.at(0).get_cell(0).is_max_value()) {
        // calc default value position
        default_value_idx = i;
      }
    } // end dor

    if (OB_SUCC(ret)) {
      const ObSubPartition *subpartition = NULL;
      subpart_idx = OB_INVALID_INDEX == subpart_idx ? default_value_idx : subpart_idx;
      if (OB_UNLIKELY(OB_INVALID_INDEX == subpart_idx)) {
        // return invalid subpart_id/tablet_id if subpartition not found.
        LOG_TRACE("subpartition not found", KR(ret), K(row));
      } else if (OB_ISNULL(subpartition = subpartition_array[subpart_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition is null", KR(ret), K(subpart_idx));
      } else if (OB_UNLIKELY(static_cast<ObPartID>(subpartition->get_part_id()) != part_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_id not match", KR(ret), KPC(subpartition), K(part_id));
      } else if (OB_UNLIKELY(!subpartition->get_tablet_id().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_id", KR(ret), KPC(subpartition), K(subpart_idx));
      } else if (OB_FAIL(indexes.push_back(PartitionIndex(OB_INVALID_INDEX, subpart_idx)))) {
        LOG_WARN("fail to push back subpart_idx", KR(ret), K(subpart_idx));
      }
    }
  }
  return ret;
}
///////////////////////////////////////////////////////////////////////////////////////

// used by SQL
int ObPartitionUtils::calc_hash_part_idx(const uint64_t val,
                                         const int64_t part_num,
                                         int64_t &partition_idx)
{
  int ret = OB_SUCCESS;
  int64_t N = 0;
  int64_t powN = 0;
  const static int64_t max_part_num_log2 = 64;
  // This function is used by SQL. Should ensure SQL runs in MySQL mode when query sys table.
  if (lib::is_oracle_mode()) {
    //
    // It will not be a negative number, so use forced conversion instead of floor
    N = static_cast<int64_t>(std::log(part_num) / std::log(2));
    if (N >= max_part_num_log2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is too big", K(N), K(part_num), K(val));
    } else {
      powN = (1ULL << N);
      partition_idx = val & (powN - 1); //pow(2, N));
      if (partition_idx + powN < part_num && (val & powN) == powN) {
        partition_idx += powN;
      }
    }
  } else {
    partition_idx = val % part_num;
  }
  return ret;
}

bool ObPartitionUtils::is_default_list_part(const ObPartition &part)
{
  bool is_default = false;
  if (part.get_list_row_values().count() == 1
      && part.get_list_row_values().at(0).get_count() >= 1
      && part.get_list_row_values().at(0).get_cell(0).is_max_value()) {
    is_default = true;
  }
  return is_default;
}

///special case: char and varchar && oracle mode int and numberic
bool ObPartitionUtils::is_types_equal_for_partition_check(
     const bool is_oracle_mode,
     const common::ObObjType &type1,
     const common::ObObjType &type2)
{
  bool is_equal = false;
  if (type1 == type2) {
    is_equal = true;
  } else if ((common::ObCharType == type1 || common::ObVarcharType == type1)
              && (common::ObCharType == type2 || common::ObVarcharType == type2)) {
    is_equal = true;
  } else if (is_oracle_mode) {
    if ((common::ObIntType == type1 || common::ObNumberType == type1)
        && (common::ObIntType == type2 || common::ObNumberType == type2)) {
      is_equal = true;
    } else if ((common::ObNumberFloatType == type1 || common::ObNumberType == type1)
               && (common::ObNumberFloatType == type2 || common::ObNumberType == type2)) {
      is_equal = true;
    } else if (ob_is_nstring_type(type1) && ob_is_nstring_type(type2)) {
      is_equal = true;
    } else {
      is_equal = false;
    }
  } else {
    is_equal = false;
  }
  return is_equal;
}

int ObPartitionUtils::convert_rows_to_sql_literal(
    const bool is_oracle_mode,
    const common::ObIArray<common::ObNewRow>& rows,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool print_collation,
    const common::ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;

  for (int64_t j = 0; OB_SUCC(ret) && j < rows.count(); j ++) {
    if (0 != j) {
      if (OB_FAIL(BUF_PRINTF(","))) {
        LOG_WARN("Failed to add comma", K(ret));
      }
    }
    const common::ObNewRow &row = rows.at(j);
    if (OB_SUCC(ret) && row.get_count() > 1) {
      if (OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("Failed to add comma", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
      const ObObj &tmp_obj = row.get_cell(i);
      if (0 != i) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("Failed to add comma", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (tmp_obj.is_timestamp_ltz()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("timestamp ltz should not be values here", K(tmp_obj), K(ret));
      } else if (tmp_obj.is_max_value()) {
        BUF_PRINTF("%s", "DEFAULT");
      } else if (tmp_obj.is_string_type()) {
        if (OB_FAIL(tmp_obj.print_varchar_literal(buf, buf_len, pos))) {
          LOG_WARN("Failed to print sql literal", K(ret));
        } else if (!print_collation) {
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " collate %s",
                           ObCharset::collation_name(tmp_obj.get_collation_type())))) {
          LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
        }
      } else if (is_oracle_mode
                 && (tmp_obj.get_meta().is_otimestamp_type() || tmp_obj.is_datetime())) {
        if (OB_FAIL(print_oracle_datetime_literal(tmp_obj, buf, buf_len, pos, tz_info))) {
          LOG_WARN("Failed to print_oracle_datetime_literal", K(tmp_obj), K(ret));
        }
      } else if (tmp_obj.is_year()) {
        if (OB_FAIL(ObTimeConverter::year_to_str(tmp_obj.get_year(), buf, buf_len, pos))) {
          LOG_WARN("Failed to year_to_str", K(tmp_obj), K(ret));
        }
      } else if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos, tz_info))) {
        LOG_WARN("Failed to print sql literal", K(ret));
      } else { }
    }
    if (OB_SUCC(ret) && row.get_count() > 1) {
      if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("Failed to add comma", K(ret));
      }
    }
  }
  return ret;
}


int ObPartitionUtils::convert_rowkey_to_sql_literal(
    const bool is_oracle_mode,
    const ObRowkey &rowkey,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool print_collation,
    const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  if (!rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid rowkey", K(rowkey), K(ret));
  } else {
    const ObObj *objs = rowkey.get_obj_ptr();
    if (NULL == objs) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("objs is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
      const ObObj &tmp_obj = objs[i];
      if (0 != i) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("Failed to add comma", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (tmp_obj.is_timestamp_ltz()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("timestamp ltz should not be values here", K(tmp_obj), K(ret));
      } else if (tmp_obj.is_max_value()) {
        BUF_PRINTF("%s", "MAXVALUE");
      } else if (tmp_obj.is_string_type()) {
        if (OB_FAIL(tmp_obj.print_varchar_literal(buf, buf_len, pos))) {
          LOG_WARN("Failed to print sql literal", K(ret));
        } else if (!print_collation) {
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " collate %s",
                           ObCharset::collation_name(tmp_obj.get_collation_type())))) {
          LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
        }
      } else if (is_oracle_mode
                 && (tmp_obj.get_meta().is_otimestamp_type() || tmp_obj.is_datetime())) {
        if (OB_FAIL(print_oracle_datetime_literal(tmp_obj, buf, buf_len, pos, tz_info))) {
          LOG_WARN("Failed to print_oracle_datetime_literal", K(tmp_obj), K(ret));
        }
      } else if (tmp_obj.is_year()) {
        if (OB_FAIL(ObTimeConverter::year_to_str(tmp_obj.get_year(), buf, buf_len, pos))) {
          LOG_WARN("Failed to year_to_str", K(tmp_obj), K(ret));
        }
      } else if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos, tz_info))) {
        LOG_WARN("Failed to print sql literal", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionUtils::print_oracle_datetime_literal(const common::ObObj &tmp_obj,
    char *buf, const int64_t buf_len, int64_t &pos, const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  if (tmp_obj.get_meta().is_otimestamp_type() || tmp_obj.is_datetime()) {
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos > buf_len)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("buf should not be null", K(buf), K(buf_len), K(pos), K(ret));
    } else {
      switch (tmp_obj.get_type()) {
        case ObObjType::ObDateTimeType: {
          ObObjPrintParams params(tz_info);
          params.beginning_space_ = 1;
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TO_DATE("))) {
            LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
          } else if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos, params))) {
            LOG_WARN("Failed to print sql literal", K(ret));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"))) {
            LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
          }
          break;
        }
        case ObObjType::ObTimestampNanoType:
        case ObTimestampTZType: {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "Timestamp "))) {
            LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
          } else if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos, tz_info))) {
            LOG_WARN("Failed to print sql literal", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("it should not arrive here", K(tmp_obj), K(lbt()), K(ret));
          break;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it should not arrive here", K(tmp_obj), K(lbt()), K(ret));
  }
  return ret;
}

int ObPartitionUtils::convert_rows_to_hex(
    const common::ObIArray<common::ObNewRow>& rows,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ConvertRow");
  char *serialize_buf = NULL;
  int64_t seri_pos = 0;
  int64_t seri_length = 8;
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i++) {
    seri_length += rows.at(i).get_serialize_size();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(serialize_buf = static_cast<char*>(allocator.alloc(seri_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(seri_length));
  } else if (OB_FAIL(serialization::encode_vi64(serialize_buf, seri_length, seri_pos, rows.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i ++) {
    if (OB_FAIL(rows.at(i).serialize(serialize_buf, seri_length, seri_pos))) {
      LOG_WARN("fail to encode row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(hex_print(serialize_buf, seri_pos, buf, buf_len, pos))) {
      LOG_WARN("Failed to print hex", K(ret), K(seri_pos), K(buf_len));
    } else { }//do nothing
  }
  return ret;
}

int ObPartitionUtils::convert_rowkey_to_hex(
    const ObRowkey &rowkey,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ConvertRowkey");
  char *serialize_buf = NULL;
  int64_t seri_pos = 0;
  int64_t seri_length = rowkey.get_serialize_size();
  if (OB_ISNULL(serialize_buf = static_cast<char*>(allocator.alloc(seri_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(seri_length));
  } else if (OB_FAIL(rowkey.serialize(serialize_buf, seri_length, seri_pos))) {
    LOG_WARN("Failed to serialize rowkey", K(rowkey), K(ret));
  } else if (OB_FAIL(hex_print(serialize_buf, seri_pos, buf, buf_len, pos))) {
    LOG_WARN("Failed to print hex", K(ret), K(seri_pos), K(buf_len));
  } else { }//do nothing
  return ret;
}

int ObPartitionUtils::set_low_bound_val_by_interval_range_by_innersql(
    const bool is_oracle_mode,
    ObPartition &p,
    const ObRowkey &interval_range_val)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SetLowBV");
  char *high_bound_val_str = static_cast<char *>(allocator.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
  char *interval_range_str = static_cast<char *>(allocator.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
  int64_t high_bound_val_len = 0;
  int64_t interval_range_len = 0;
  ObCommonSqlProxy *sql_proxy = GCTX.ddl_oracle_sql_proxy_;
  ObSqlString sql_string;
  if (OB_ISNULL(high_bound_val_str) || OB_ISNULL(interval_range_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("high_bound_val_str is null", KR(ret), K(high_bound_val_str), K(interval_range_str));
  } else {
    MEMSET(high_bound_val_str, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    MEMSET(interval_range_str, 0, OB_MAX_B_HIGH_BOUND_VAL_LENGTH);
    ObTimeZoneInfo tz_info;
    tz_info.set_offset(0);
    const ObObj *high_bound_objs = p.get_high_bound_val().get_obj_ptr();
    if (p.get_high_bound_val().get_obj_cnt() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey is invalid", KR(ret), K(p), K(interval_range_val));
    } else if (OB_ISNULL(high_bound_objs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr NULL", KR(ret), K(p), K(interval_range_val));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(p.get_tenant_id(), tz_info.get_tz_map_wrap()))) {
      LOG_WARN("get tenant timezone map failed", KR(ret), K(p.get_tenant_id()));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
               is_oracle_mode,
               p.get_high_bound_val(), high_bound_val_str,
               OB_MAX_B_HIGH_BOUND_VAL_LENGTH,
               high_bound_val_len, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), KR(ret));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
               is_oracle_mode,
               interval_range_val, interval_range_str,
               OB_MAX_B_HIGH_BOUND_VAL_LENGTH,
               interval_range_len, false, &tz_info))) {
      LOG_WARN("Failed to convert rowkey to sql text", K(tz_info), KR(ret));
    } else if (ObDateTimeType == high_bound_objs[0].get_type()) {
      if (OB_FAIL(sql_string.append_fmt("SELECT TO_DATE(%.*s) - %.*s FROM DUAL",
                                                static_cast<int>(high_bound_val_len),
                                                high_bound_val_str,
                                                static_cast<int>(interval_range_len),
                                                interval_range_str))) {
        LOG_WARN("fail to append format", KR(ret));
      }
    } else if (ObTimestampNanoType == high_bound_objs[0].get_type()) {
      if(OB_FAIL(sql_string.append_fmt("SELECT TO_TIMESTAMP(%.*s) - %.*s FROM DUAL",
                                                static_cast<int>(high_bound_val_len),
                                                high_bound_val_str,
                                                static_cast<int>(interval_range_len),
                                                interval_range_str))) {
        LOG_WARN("fail to append format", KR(ret));
      }
    } else if (OB_FAIL(sql_string.append_fmt("SELECT %.*s - %.*s FROM DUAL",
                                             static_cast<int>(high_bound_val_len),
                                             high_bound_val_str,
                                             static_cast<int>(interval_range_len),
                                             interval_range_str))) {
      LOG_WARN("fail to append format", KR(ret));
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        static int64_t ROW_KEY_CNT = 1;
        ObObj obj_array[ROW_KEY_CNT];
        obj_array[0].reset();
        ObObj &low_bound = obj_array[0];
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy->read(res, p.get_tenant_id(), sql_string.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(sql_string.ptr()), K(p), K(interval_range_val),
                   K(high_bound_objs[0].get_type()));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("execute sql failed", KR(ret), K(p.get_tenant_id()), K(sql_string));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("iterate next result fail", KR(ret), K(sql_string));
        } else if (OB_FAIL(result->get_obj((int64_t)0, low_bound))) {
          LOG_WARN("failed to get obj", KR(ret));
        } else {
          ObRowkey low_bound_val;
          low_bound_val.reset();
          low_bound_val.assign(obj_array, ROW_KEY_CNT);
          if (OB_FAIL(p.set_low_bound_val(low_bound_val))) {
            LOG_WARN("fail to set low bound val", K(p), KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionUtils::check_interval_partition_table(
    const ObRowkey &transition_point,
    const ObRowkey &interval_range)
{
  int ret = OB_SUCCESS;

  const stmt::StmtType stmt_type = stmt::T_NONE;
  SMART_VARS_4((ObExprCtx, expr_ctx),
               (ObArenaAllocator, local_allocator),
               (ObExecContext, exec_ctx, local_allocator),
               (ObSQLSessionInfo, session_info)) {

    OZ (session_info.init(0, 0, &local_allocator, NULL));
    OX (session_info.set_time_zone(ObString("+8:00"), true, true));
    OZ (session_info.load_default_sys_variable(false, false));
    OZ (session_info.load_default_configs_in_pc());
    OX (exec_ctx.set_my_session(&session_info));

    if (OB_SUCC(ret)) {
      ObNewRow tmp_row;
      RowDesc row_desc;
      ObObj temp_obj;
      ParamStore dummy_params;

      OZ (ObSQLUtils::wrap_expr_ctx(stmt_type, exec_ctx, exec_ctx.get_allocator(), expr_ctx));
      ObExprOperatorFactory expr_op_factory(exec_ctx.get_allocator());
      ObRawExprFactory raw_expr_factory(exec_ctx.get_allocator());
      ObExprGeneratorImpl expr_gen(expr_op_factory, 0, 0, NULL, row_desc);
      ObSqlExpression sql_expr(exec_ctx.get_allocator());
      expr_ctx.cast_mode_ = CM_WARN_ON_FAIL; //always set to WARN_ON_FAIL to allow calculate

      ObConstRawExpr *transition_expr = NULL;
      ObConstRawExpr *interval_expr = NULL;

      OZ (ObRawExprUtils::build_const_obj_expr(raw_expr_factory, transition_point.get_obj_ptr()[0], transition_expr));
      OZ (ObRawExprUtils::build_const_obj_expr(raw_expr_factory, interval_range.get_obj_ptr()[0], interval_expr));

      OZ (interval_expr->formalize(exec_ctx.get_my_session()));
      CK (interval_expr->is_const_expr());

      OZ (sql::ObPartitionExecutorUtils::check_transition_interval_valid(stmt::StmtType::T_NONE,
                                                          exec_ctx,
                                                          transition_expr,
                                                          interval_expr));
      // // OZ (expr_gen.generate(*interval_expr, sql_expr));
      // // OZ (sql_expr.calc(expr_ctx, tmp_row, temp_obj));
      // OZ (ObSQLUtils::calc_simple_expr_without_row(stmt::StmtType::T_NONE, &session_info,
      //                              interval_expr, temp_obj, &dummy_params, exec_ctx.get_allocator()));

      // if (OB_SUCC(ret)) {
      //   if (temp_obj.is_zero()) {
      //     ret = OB_ERR_INTERVAL_CANNOT_BE_ZERO;
      //     LOG_WARN("interval can not be zero");
      //   }
      // }
      // if (OB_SUCC(ret)) {
      //   if ((transition_expr->get_data_type() == ObDateTimeType
      //       ||transition_expr->get_data_type() == ObTimestampNanoType)
      //     && (interval_expr->get_data_type() == ObIntervalYMType)) {
      //     if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type, exec_ctx, exec_ctx.get_allocator(), expr_ctx))) {
      //       LOG_WARN("Failed to wrap expr exec_ctx", K(ret));
      //     } else {
      //       ObOpRawExpr *add_expr = NULL;
      //       ObRawExpr *tmp_expr = transition_expr;
      //       ObObj temp_obj;
      //       // ObExprOperatorFactory expr_op_factory(exec_ctx.get_allocator());
      //       // ObRawExprFactory raw_expr_factory(exec_ctx.get_allocator());
      //       // ObExprGeneratorImpl expr_gen(expr_op_factory, 0, 0, NULL, row_desc);
      //       // ObSqlExpression sql_expr(exec_ctx.get_allocator());
      //       // expr_ctx.cast_mode_ = CM_WARN_ON_FAIL; //always set to WARN_ON_FAIL to allow calculate
      //       // EXPR_SET_CAST_CTX_MODE(expr_ctx);
      //       // tmp_expr = transition_expr;
      //       // for (int i = 0; OB_SUCC(ret) && i < n; i ++) {
      //       //   OX (expr = NULL);
      //       //   OZ (raw_expr_factory.create_raw_expr(T_OP_ADD, expr));
      //       //   if (NULL != expr) {
      //       //     OZ (expr->set_param_exprs(tmp_expr, interval_expr));
      //       //     OX (tmp_expr = expr);
      //       //   }
      //       // }
      //       // OZ (expr_gen.generate(*expr, sql_expr));
      //       // if (OB_SUCC(ret) && OB_FAIL(sql_expr.calc(expr_ctx, tmp_row, temp_obj))) {
      //       //   ret = OB_ERR_INVALID_INTERVAL_HIGH_BOUNDS;
      //       //   LOG_WARN("fail to calc value", K(ret), K(*expr));
      //       // }

      //       // For the interval of year and month, it needs to be added up to 11 times to judge whether it is legal
      //       for (int i = 0; OB_SUCC(ret) && i < 11; i ++) {
      //         OX (add_expr = NULL);
      //         OZ (raw_expr_factory.create_raw_expr(T_OP_ADD, add_expr));
      //         if (NULL != add_expr) {
      //           OZ (add_expr->set_param_exprs(tmp_expr, interval_expr));
      //           OX (tmp_expr = add_expr);
      //         }
      //       }
      //       OZ (add_expr->formalize(exec_ctx.get_my_session()));
      //       OZ (ObSQLUtils::calc_simple_expr_without_row(stmt::StmtType::T_NONE,
      //                                     exec_ctx.get_my_session(),
      //                                     add_expr, temp_obj,
      //                                     &dummy_params, exec_ctx.get_allocator()));
      //       if (OB_ERR_DAY_OF_MONTH_RANGE == ret) {
      //         ret = OB_ERR_INVALID_INTERVAL_HIGH_BOUNDS;
      //         LOG_WARN("fail to calc value", K(ret), KPC(add_expr));
      //       }
      //     }
      //   }
      // }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMVRefreshInfo,
    refresh_method_,
    refresh_mode_,
    start_time_,
    next_time_expr_,
    exec_env_,
    parallel_);

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObViewSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObViewSchema::ObViewSchema()
    : ObSchema(),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false),
      materialized_(false),
      character_set_client_(CHARSET_INVALID),
      collation_connection_(CS_TYPE_INVALID),
      container_table_id_(OB_INVALID_ID)
{
}

ObViewSchema::ObViewSchema(ObIAllocator *allocator)
    : ObSchema(allocator),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false),
      materialized_(false),
      character_set_client_(CHARSET_INVALID),
      collation_connection_(CS_TYPE_INVALID),
      container_table_id_(OB_INVALID_ID)
{
}

ObViewSchema::~ObViewSchema()
{
}

ObViewSchema::ObViewSchema(const ObViewSchema &src_schema)
    : ObSchema(),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false),
      materialized_(false),
      character_set_client_(CHARSET_INVALID),
      collation_connection_(CS_TYPE_INVALID),
      container_table_id_(OB_INVALID_ID),
      mv_refresh_info_(nullptr)
{
  *this = src_schema;
}

ObViewSchema &ObViewSchema::operator =(const ObViewSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;

    view_check_option_ = src_schema.view_check_option_;
    view_is_updatable_ = src_schema.view_is_updatable_;
    materialized_ = src_schema.materialized_;
    character_set_client_ = src_schema.character_set_client_;
    collation_connection_ = src_schema.collation_connection_;
    container_table_id_ = src_schema.container_table_id_;
    mv_refresh_info_ = src_schema.mv_refresh_info_;

    if (OB_FAIL(deep_copy_str(src_schema.view_definition_, view_definition_))) {
      LOG_WARN("Fail to deep copy view definition, ", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObViewSchema::operator==(const ObViewSchema &other) const
{
  return view_definition_ == other.view_definition_
      && view_check_option_ == other.view_check_option_
      && view_is_updatable_ == other.view_is_updatable_
      && materialized_ == other.materialized_
      && character_set_client_ == other.character_set_client_
      && collation_connection_ == other.collation_connection_
      && container_table_id_ == other.container_table_id_
      && mv_refresh_info_ == other.mv_refresh_info_;
}

bool ObViewSchema::operator!=(const ObViewSchema &other) const
{
  return !(*this == other);
}

int64_t ObViewSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(*this);
  convert_size += view_definition_.length() + 1;

  return convert_size;
}

bool ObViewSchema::is_valid() const
{
  return ObSchema::is_valid() && !view_definition_.empty();
}

void ObViewSchema::reset()
{
  reset_string(view_definition_);
  view_check_option_ = VIEW_CHECK_OPTION_NONE;
  view_is_updatable_ = false;
  materialized_ = false;
  character_set_client_ = CHARSET_INVALID;
  collation_connection_ = CS_TYPE_INVALID;
  container_table_id_ = OB_INVALID_ID;
  mv_refresh_info_ = nullptr;
  ObSchema::reset();
}

OB_DEF_SERIALIZE(ObViewSchema)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              view_definition_,
              view_check_option_,
              view_is_updatable_,
              materialized_,
              character_set_client_,
              collation_connection_,
              container_table_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObViewSchema)
{
  int ret = OB_SUCCESS;
  ObString definition;

  LST_DO_CODE(OB_UNIS_DECODE,
              definition,
              view_check_option_,
              view_is_updatable_,
              materialized_,
              character_set_client_,
              collation_connection_,
              container_table_id_);

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(definition, view_definition_))) {
    LOG_WARN("Fail to deep copy view definition, ", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObViewSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              view_definition_,
              view_check_option_,
              view_is_updatable_,
              materialized_,
              character_set_client_,
              collation_connection_,
              container_table_id_);
  return len;
}

const char *ob_view_check_option_str(const ViewCheckOption option)
{
  const char *ret = "invalid";
  const char *option_ptr[] =
  { "none", "local", "cascaded" };
  if (option >= 0 && option < VIEW_CHECK_OPTION_MAX) {
    ret = option_ptr[option];
  }
  return ret;
}

const char *ob_index_status_str(ObIndexStatus status)
{
  const char *ret = "invalid";
  const char *status_ptr[] = {
               "not_found",
               "unavailable",
               "available",
               "unique_checking",
               "unique_inelegible",
               "index_error",
               "restore_index_error",
               "unusable" };
  if (status >= 0 && status < INDEX_STATUS_MAX) {
    ret = status_ptr[status];
  }
  return ret;
}

ObTenantTableId &ObTenantTableId::operator =(const ObTenantTableId &tenant_table_id)
{
  tenant_id_ = tenant_table_id.tenant_id_;
  table_id_ = tenant_table_id.table_id_;
  return *this;
}

/*************************For managing Privileges****************************/
//ObTenantUserId
OB_SERIALIZE_MEMBER(ObTenantUserId,
                    tenant_id_,
                    user_id_);

//ObTenantUrObjId
OB_SERIALIZE_MEMBER(ObTenantUrObjId,
                    tenant_id_,
                    grantee_id_,
                    obj_id_,
                    obj_type_,
                    col_id_);

//ObPrintPrivSet
DEF_TO_STRING(ObPrintPrivSet)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = BUF_PRINTF("\"");
  if ((priv_set_ & OB_PRIV_ALTER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_ALTER,");
  }
  if ((priv_set_ & OB_PRIV_CREATE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_USER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE_USER,");
  }
  if ((priv_set_ & OB_PRIV_DELETE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_DELETE,");
  }
  if ((priv_set_ & OB_PRIV_DROP) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_DROP,");
  }
  if ((priv_set_ & OB_PRIV_GRANT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_GRANT_OPTION,");
  }
  if ((priv_set_ & OB_PRIV_INSERT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_INSERT,");
  }
  if ((priv_set_ & OB_PRIV_UPDATE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_UPDATE,");
  }
  if ((priv_set_ & OB_PRIV_SELECT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SELECT,");
  }
  if ((priv_set_ & OB_PRIV_INDEX) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_INDEX,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_VIEW) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE_VIEW,");
  }
  if ((priv_set_ & OB_PRIV_SHOW_VIEW) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SHOW_VIEW,");
  }
  if ((priv_set_ & OB_PRIV_SHOW_DB) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SHOW_DB,");
  }
  if ((priv_set_ & OB_PRIV_SUPER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SUPER,");
  }
  if ((priv_set_ & OB_PRIV_SUPER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_PROCESS,");
  }
  if ((priv_set_ & OB_PRIV_BOOTSTRAP) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_BOOTSTRAP,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_SYNONYM) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("CREATE_SYNONYM,");
  }
  if ((priv_set_ & OB_PRIV_AUDIT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_AUDIT,");
  }
  if ((priv_set_ & OB_PRIV_COMMENT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_COMMENT,");
  }
  if ((priv_set_ & OB_PRIV_LOCK) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_LOCK,");
  }
  if ((priv_set_ & OB_PRIV_RENAME) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_RENAME,");
  }
  if ((priv_set_ & OB_PRIV_REFERENCES) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_REFERENCES,");
  }
  if ((priv_set_ & OB_PRIV_FLASHBACK) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_FLASHBACK,");
  }
  if ((priv_set_ & OB_PRIV_READ) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_READ,");
  }
  if ((priv_set_ & OB_PRIV_WRITE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_WRITE,");
  }
  if ((priv_set_ & OB_PRIV_FILE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_FILE,");
  }
  if ((priv_set_ & OB_PRIV_ALTER_TENANT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_ALTER_TENANT,");
  }
  if ((priv_set_ & OB_PRIV_ALTER_SYSTEM) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_ALTER_SYSTEM,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_RESOURCE_POOL) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE_RESOURCE_POOL,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_RESOURCE_UNIT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE_RESOURCE_UNIT,");
  }
  if ((priv_set_ & OB_PRIV_REPL_SLAVE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" REPLICATION SLAVE,");
  }
  if ((priv_set_ & OB_PRIV_REPL_CLIENT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" REPLICATION CLIENT,");
  }
  if ((priv_set_ & OB_PRIV_DROP_DATABASE_LINK) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" DROP DATABASE LINK,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_DATABASE_LINK) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" CREATE DATABASE LINK,");
  }
  if ((priv_set_ & OB_PRIV_EXECUTE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" EXECUTE,");
  }
  if ((priv_set_ & OB_PRIV_ALTER_ROUTINE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" ALTER ROUTINE,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_ROUTINE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" CREATE ROUTINE,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_TABLESPACE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" CREATE TABLESPACE,");
  }
  if ((priv_set_ & OB_PRIV_SHUTDOWN) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" SHUTDOWN,");
  }
  if ((priv_set_ & OB_PRIV_RELOAD) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF(" RELOAD,");
  }
  if (OB_SUCCESS == ret && pos > 1) {
    pos--; //Delete last ','
  }
  ret = BUF_PRINTF("\"");
  return pos;
}

//ObPrintSysPrivSet
DEF_TO_STRING(ObPrintPackedPrivArray)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = BUF_PRINTF("\"");
  if (packed_priv_array_.count() > 0 ){
    ret = BUF_PRINTF("%ld", packed_priv_array_[0]);
    /*FOREACH(it, packed_priv_array_) {
      if ((*it) != NULL) {
        ret = BUF_PRINTF("%lld", *(*it));
      }
      if (OB_SUCCESS == ret && pos > 1) {
      pos--; //Delete last ','
      }
    }
    if ((packed_priv_array_[0] & OB_ORA_SYS_PRIV_CREATE_SESS) && OB_SUCCESS == ret) {
      ret = BUF_PRINTF("CREATE SESSION,");
    }
    if ((sys_priv_set_[0] & OB_ORA_SYS_PRIV_EXEMPT_RED_PLY) && OB_SUCCESS == ret) {
      ret = BUF_PRINTF("EXEMPT REDACTION POLICY,");
    }
    if (OB_SUCCESS == ret && pos > 1) {
      pos--; //Delete last ','
    }*/
  }

  ret = BUF_PRINTF("\"");
  return pos;
}

//ObPriv
int ObPriv::assign(const ObPriv &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    schema_version_ = other.schema_version_;
    priv_set_ = other.priv_set_;
    if (OB_FAIL(set_priv_array(other.priv_array_))) {
      LOG_WARN("assgin priv array failed", K(ret));
    }
  }
  return ret;
}

void ObPriv::reset()
{
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  priv_set_ = 0;
  priv_array_.reset();
}

int64_t ObPriv::get_convert_size() const
{
  int64_t convert_size = sizeof(ObPriv);
  convert_size += priv_array_.get_data_size();
  return convert_size;
}

OB_SERIALIZE_MEMBER(ObPriv,
                    tenant_id_,
                    user_id_,
                    schema_version_,
                    priv_set_,
                    priv_array_);

void ObProxyInfo::reset()
{
  user_id_ = OB_INVALID_ID;
  proxy_flags_ = 0;
  credential_type_ = 0;
  role_ids_ = NULL;
  role_id_cnt_ = 0;
  role_id_capacity_ = 0;
}
int ObProxyInfo::assign(const ObProxyInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    user_id_ = other.user_id_;
    proxy_flags_ = other.proxy_flags_;
    credential_type_ = other.credential_type_;
    if (other.role_id_cnt_ != 0) {
      if (OB_ISNULL(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else {
        uint64_t tmp_role_id_capacity = 0;
        uint64_t tmp_role_id_cnt = 0;
        uint64_t *tmp_role_ids = static_cast<uint64_t*>(allocator_->alloc(sizeof(uint64_t) * other.role_id_cnt_));
        if (NULL == tmp_role_ids) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for role_ids", K(ret));
        } else {
          MEMSET(tmp_role_ids, 0, sizeof(uint64_t) * other.role_id_cnt_);
          tmp_role_id_capacity = other.role_id_cnt_;
          for (int64_t i = 0; OB_SUCC(ret) && i < other.role_id_cnt_; ++i) {
            uint64_t role_id = other.get_role_id_by_idx(i);
            tmp_role_ids[tmp_role_id_cnt++] = role_id;
          }
        }

        if (OB_SUCC(ret)) {
          role_ids_ = tmp_role_ids;
          role_id_capacity_ = tmp_role_id_capacity;
          role_id_cnt_ = tmp_role_id_cnt;
        }
      }
    }
  }
  return ret;
}

int ObProxyInfo::add_role_id(const uint64_t role_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  }else if (0 == role_id_capacity_) {
    if (NULL == (role_ids_ = static_cast<uint64_t*>(
        allocator_->alloc(sizeof(uint64_t) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for array_.", K(ret));
    } else {
      role_id_capacity_ = DEFAULT_ARRAY_CAPACITY;
      MEMSET(role_ids_, 0, sizeof(uint64_t*) * DEFAULT_ARRAY_CAPACITY);
    }
  } else if (role_id_cnt_ >= role_id_capacity_) {
    int64_t tmp_size = 2 * role_id_capacity_;
    uint64_t *tmp = NULL;
    if (NULL == (tmp = static_cast<uint64_t*>(
        allocator_->alloc(sizeof(uint64_t) * tmp_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for array_, ", K(tmp_size), K(ret));
    } else {
      MEMCPY(tmp, role_ids_, sizeof(uint64_t) * role_id_capacity_);
      free(role_ids_);
      role_ids_ = tmp;
      role_id_capacity_ = tmp_size;
    }
  }

  if (OB_SUCC(ret)) {
    role_ids_[role_id_cnt_++] = role_id;
  }
  return ret;
}

uint64_t ObProxyInfo::get_role_id_by_idx(const int64_t idx) const
{
  uint64_t role_id = OB_INVALID_ID;
  if (idx < 0 || idx >= role_id_cnt_) {
    role_id = OB_INVALID_ID;
  } else {
    role_id = role_ids_[idx];
  }
  return role_id;
}

OB_DEF_SERIALIZE(ObProxyInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              user_id_,
              proxy_flags_,
              credential_type_);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, role_id_cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < role_id_cnt_; ++i) {
      LST_DO_CODE(OB_UNIS_ENCODE, role_ids_[i]);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObProxyInfo)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE,
              user_id_,
              proxy_flags_,
              credential_type_);

  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(role_id_cnt_);
    if (OB_SUCC(ret)) {
      if (role_id_cnt_ == 0) {
        role_ids_ = NULL;
        role_id_capacity_ = 0;
      } else {
        role_ids_ = static_cast<uint64_t*>(allocator_->alloc(sizeof(uint64_t) * role_id_cnt_));
        if (NULL == role_ids_) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for role ids", K(ret));
        } else {
          MEMSET(role_ids_, 0, sizeof(uint64_t) * role_id_cnt_);
          role_id_capacity_ = role_id_cnt_;
          for (int64_t i = 0; OB_SUCC(ret) && i < role_id_cnt_; ++i) {
            uint64_t role_id = OB_INVALID_ID;
            LST_DO_CODE(OB_UNIS_DECODE, role_id);
            role_ids_[i] = role_id;
          }
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObProxyInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              user_id_,
              proxy_flags_,
              credential_type_,
              role_id_cnt_);
  len += role_id_cnt_ * sizeof(uint64_t);
  return len;
}

int ObUserFlags::assign(const ObUserFlags &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input ObUserFlags is invalid", K(ret), K(other));
  } else {
    flags_ = other.flags_;
  }
  return ret;
}

ObUserFlags& ObUserFlags::operator=(const ObUserFlags &other)
{
  if (this != &other) {
    flags_ = other.flags_;
  }
  return *this;
}

bool ObUserFlags::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(proxy_activated_flag_ < 0 || proxy_activated_flag_ >= PROXY_ACTIVATED_MAX)) {
    bret = false;
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObUserFlags,flags_);

//ObUserInfo
ObUserInfo::ObUserInfo(ObIAllocator *allocator)
  : ObSchema(allocator), ObPriv(allocator),
    user_name_(), host_name_(), passwd_(), info_(), locked_(false),
    ssl_type_(ObSSLType::SSL_TYPE_NOT_SPECIFIED), ssl_cipher_(), x509_issuer_(),
    x509_subject_(), type_(OB_USER),
    grantee_id_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                      common::ModulePageAllocator(*allocator)),
    role_id_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                   common::ModulePageAllocator(*allocator)),
    profile_id_(OB_INVALID_ID), password_last_changed_timestamp_(OB_INVALID_TIMESTAMP),
    role_id_option_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                          common::ModulePageAllocator(*allocator)),
    max_connections_(0),
    max_user_connections_(0),
    proxied_user_info_(NULL),
    proxied_user_info_capacity_(0),
    proxied_user_info_cnt_(0),
    proxy_user_info_(NULL),
    proxy_user_info_capacity_(0),
    proxy_user_info_cnt_(0),
    user_flags_()
{
}

ObUserInfo::~ObUserInfo()
{
}

int ObUserInfo::assign_proxy_info_array_(ObProxyInfo **src_arr,
                                        const uint64_t src_cnt,
                                        const uint64_t src_capacity,
                                        ObProxyInfo **&tar_arr,
                                        uint64_t &tar_cnt,
                                        uint64_t &tar_capacity)

{
  int ret = OB_SUCCESS;
  UNUSED(src_capacity);
  ObProxyInfo **tmp_arr = NULL;
  uint64_t tmp_cnt = 0;
  uint64_t tmp_capacity = 0;
  if (src_cnt == 0) {
    //do nothing
  } else if (NULL == (tmp_arr = static_cast<ObProxyInfo**>(
              alloc(sizeof(ObProxyInfo*) * src_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory for array_.", K(src_cnt), KR(ret));
  } else {
    MEMSET(tmp_arr, 0, sizeof(ObProxyInfo*) * src_cnt);
    tmp_capacity = src_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_cnt; i++) {
      const ObProxyInfo *src_info = src_arr[i];
      ObProxyInfo *tar_info = NULL;
      if (OB_ISNULL(src_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (OB_ISNULL(tar_info = OB_NEWx(ObProxyInfo, get_allocator(), get_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(get_allocator()));
      } else if (OB_FAIL(tar_info->assign(*src_info))) {
        LOG_WARN("failed to assign proxy info", K(ret));
      } else {
        tmp_arr[tmp_cnt++] = tar_info;
      }
    }

    if (OB_SUCC(ret)) {
      tar_arr = tmp_arr;
      tar_capacity = tmp_capacity;
      tar_cnt = tmp_cnt;
    }
  }
  return ret;
}

int ObUserInfo::assign(const ObUserInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FALSE_IT(locked_ = other.locked_)) {
    } else if (OB_FALSE_IT(ssl_type_ = other.ssl_type_)) {
    } else if (OB_FAIL(deep_copy_str(other.user_name_, user_name_))) {
      LOG_WARN("Fail to deep copy user_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.host_name_, host_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.passwd_, passwd_))) {
      LOG_WARN("Fail to deep copy passwd", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.info_, info_))) {
      LOG_WARN("Fail to deep copy info", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.ssl_cipher_, ssl_cipher_))) {
      LOG_WARN("Fail to deep copy ssl_cipher", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.x509_issuer_, x509_issuer_))) {
      LOG_WARN("Fail to deep copy x509_issuer", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.x509_subject_, x509_subject_))) {
      LOG_WARN("Fail to deep copy ssl_subject", K(ret));
    } else if (OB_FAIL(grantee_id_array_.assign(other.grantee_id_array_))) {
      LOG_WARN("Fail to assign grantee_id_array ", K(ret));
    } else if (OB_FAIL(role_id_array_.assign(other.role_id_array_))) {
      LOG_WARN("Fail to assign role_id_array", K(ret));
    } else if (OB_FAIL(role_id_option_array_.assign(other.role_id_option_array_))) {
      LOG_WARN("Fail to assign role_id_option_array", K(ret));
    } else {
      type_ = other.type_;
      profile_id_ = other.profile_id_;
      password_last_changed_timestamp_ = other.password_last_changed_timestamp_;
      max_connections_ = other.max_connections_;
      max_user_connections_ = other.max_user_connections_;
      if (OB_FAIL(assign_proxy_info_array_(other.proxied_user_info_,
                                           other.proxied_user_info_cnt_,
                                           other.proxied_user_info_capacity_,
                                           proxied_user_info_,
                                           proxied_user_info_cnt_,
                                           proxied_user_info_capacity_))) {
        LOG_WARN("assign proxy info array", K(ret));
      } else if (OB_FAIL(assign_proxy_info_array_(other.proxy_user_info_,
                                           other.proxy_user_info_cnt_,
                                           other.proxy_user_info_capacity_,
                                           proxy_user_info_,
                                           proxy_user_info_cnt_,
                                           proxy_user_info_capacity_))) {
        LOG_WARN("assign proxy info array", K(ret));
      }
      if (OB_SUCC(ret)) {
        user_flags_ = other.user_flags_;
      }
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return ret;
}

bool ObUserInfo::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

int ObUserInfo::add_proxy_info_(ObProxyInfo **&arr, uint64_t &capacity, uint64_t &cnt, const ObProxyInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  if (0 == capacity) {
    if (NULL == (arr = static_cast<ObProxyInfo**>(
        alloc(sizeof(ObProxyInfo*) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory for array_.", KR(ret));
    } else {
      capacity = DEFAULT_ARRAY_CAPACITY;
      MEMSET(arr, 0, sizeof(ObProxyInfo*) * DEFAULT_ARRAY_CAPACITY);
    }
  } else if (cnt >= capacity) {
    int64_t tmp_size = 2 * capacity;
    ObProxyInfo **tmp = NULL;
    if (NULL == (tmp = static_cast<ObProxyInfo**>(
        alloc(sizeof(ObProxyInfo*) * tmp_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory for array_, ", K(tmp_size), KR(ret));
    } else {
      MEMCPY(tmp, arr, sizeof(ObProxyInfo*) * capacity);
      free(arr);
      arr = tmp;
      capacity = tmp_size;
    }
  }

  if (OB_SUCC(ret)) {
    ObProxyInfo *info = OB_NEWx(ObProxyInfo, get_allocator(), get_allocator());
    if (NULL == info) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(get_allocator()));
    } else if (OB_FAIL(info->assign(proxy_info))) {
      LOG_WARN("failed to assign proxy info", K(ret));
    } else {
      arr[cnt++] = info;
    }
  }
  return ret;
}

int ObUserInfo::add_proxied_user_info(const ObProxyInfo& proxy_info)
{
  return add_proxy_info_(proxied_user_info_, proxied_user_info_capacity_, proxied_user_info_cnt_, proxy_info);
}

int ObUserInfo::add_proxy_user_info(const ObProxyInfo& proxy_info)
{
  return add_proxy_info_(proxy_user_info_, proxy_user_info_capacity_, proxy_user_info_cnt_, proxy_info);
}

const ObProxyInfo* ObUserInfo::get_proxied_user_info_by_idx(uint64_t idx) const
{
  const ObProxyInfo *proxy_info = NULL;
  if (idx < 0 || idx >= proxied_user_info_cnt_) {
    proxy_info = NULL;
  } else {
    proxy_info = proxied_user_info_[idx];
  }
  return proxy_info;
}

ObProxyInfo* ObUserInfo::get_proxied_user_info_by_idx_for_update(uint64_t idx)
{
  ObProxyInfo *proxy_info = NULL;
  if (idx < 0 || idx >= proxied_user_info_cnt_) {
    proxy_info = NULL;
  } else {
    proxy_info = proxied_user_info_[idx];
  }
  return proxy_info;
}

const ObProxyInfo* ObUserInfo::get_proxy_user_info_by_idx(uint64_t idx) const
{
  const ObProxyInfo *proxy_info = NULL;
  if (idx < 0 || idx >= proxy_user_info_cnt_) {
    proxy_info = NULL;
  } else {
    proxy_info = proxy_user_info_[idx];
  }
  return proxy_info;
}

ObProxyInfo* ObUserInfo::get_proxy_user_info_by_idx_for_update(uint64_t idx)
{
  ObProxyInfo *proxy_info = NULL;
  if (idx < 0 || idx >= proxy_user_info_cnt_) {
    proxy_info = NULL;
  } else {
    proxy_info = proxy_user_info_[idx];
  }
  return proxy_info;
}

void ObUserInfo::reset()
{
  user_name_.reset();
  host_name_.reset();
  passwd_.reset();
  info_.reset();
  ssl_type_ = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
  ssl_cipher_.reset();
  x509_issuer_.reset();
  x509_subject_.reset();
  type_ = OB_USER;
  grantee_id_array_.reset();
  role_id_array_.reset();
  role_id_option_array_.reset();
  proxied_user_info_ = NULL;
  proxied_user_info_cnt_ = 0;
  proxied_user_info_capacity_ = 0;
  proxy_user_info_ = NULL;
  proxy_user_info_cnt_ = 0;
  proxy_user_info_capacity_ = 0;
  profile_id_ = OB_INVALID_ID;
  password_last_changed_timestamp_ = OB_INVALID_TIMESTAMP;
  max_connections_ = 0;
  max_user_connections_ = 0;
  proxied_user_info_ = NULL;
  proxied_user_info_cnt_ = 0;
  proxied_user_info_capacity_ = 0;
  proxy_user_info_ = NULL;
  proxy_user_info_cnt_ = 0;
  proxy_user_info_capacity_ = 0;
  user_flags_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObUserInfo::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObUserInfo) - sizeof(ObPriv);
  convert_size += user_name_.length() + 1;
  convert_size += host_name_.length() + 1;
  convert_size += passwd_.length() + 1;
  convert_size += info_.length() + 1;
  convert_size += ssl_cipher_.length() + 1;
  convert_size += x509_issuer_.length() + 1;
  convert_size += x509_subject_.length() + 1;
  convert_size += grantee_id_array_.get_data_size();
  convert_size += role_id_array_.get_data_size();
  convert_size += role_id_option_array_.get_data_size();
  convert_size += proxied_user_info_cnt_ * sizeof(ObProxyInfo*);
  convert_size += proxy_user_info_cnt_ * sizeof(ObProxyInfo*);
  for (int64_t i = 0; i < proxied_user_info_cnt_; i++) {
    if (OB_NOT_NULL(proxied_user_info_[i])) {
      convert_size += proxied_user_info_[i]->get_convert_size();
    }
  }
  for (int64_t i = 0; i < proxy_user_info_cnt_; i++) {
    if (OB_NOT_NULL(proxy_user_info_[i])) {
      convert_size += proxy_user_info_[i]->get_convert_size();
    }
  }
  return convert_size;
}

OB_DEF_SERIALIZE(ObUserInfo)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE,
              user_name_,
              host_name_,
              passwd_,
              info_,
              locked_,
              ssl_type_,
              ssl_cipher_,
              x509_issuer_,
              x509_subject_,
              type_,
              grantee_id_array_,
              role_id_array_,
              profile_id_,
              password_last_changed_timestamp_,
              role_id_option_array_,
              max_connections_,
              max_user_connections_);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, proxied_user_info_cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < proxied_user_info_cnt_; ++i) {
      if (OB_ISNULL(proxied_user_info_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        LST_DO_CODE(OB_UNIS_ENCODE, *proxied_user_info_[i]);
      }
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, proxy_user_info_cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < proxy_user_info_cnt_; ++i) {
      if (OB_ISNULL(proxy_user_info_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        LST_DO_CODE(OB_UNIS_ENCODE, *proxy_user_info_[i]);
      }
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, user_flags_);
  }
  return ret;
}

int ObUserInfo::deserialize_proxy_info_array_(ObProxyInfo **&arr, uint64_t &cnt, uint64_t &capacity,
                                            const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("ProxyInfo");
  OB_UNIS_DECODE(cnt);
  if (OB_FAIL(ret)) {
  } else if (cnt == 0) {
    capacity = 0;
    arr = NULL;
  } else if (NULL == (arr = static_cast<ObProxyInfo**>(
            alloc(sizeof(ObProxyInfo*) * cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory for array_.", KR(ret));
  } else {
    MEMSET(arr, 0, sizeof(ObProxyInfo*) * cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
      ObProxyInfo proxy_info(&tmp_allocator);
      LST_DO_CODE(OB_UNIS_DECODE, proxy_info);
      if (OB_SUCC(ret)) {
        ObProxyInfo *info = OB_NEWx(ObProxyInfo, get_allocator(), get_allocator());
        if (NULL == info) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(get_allocator()));
        } else if (OB_FAIL(info->assign(proxy_info))) {
          LOG_WARN("failed to assign proxy info", K(ret));
        } else {
          arr[i] = info;
        }
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObUserInfo)
{
  int ret = OB_SUCCESS;
  ObString user_name;
  ObString host_name;
  ObString passwd;
  ObString info;
  ObString ssl_cipher;
  ObString x509_issuer;
  ObString x509_subject;

  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE,
              user_name,
              host_name,
              passwd,
              info,
              locked_,
              ssl_type_,
              ssl_cipher,
              x509_issuer,
              x509_subject);

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(user_name, user_name_))) {
    LOG_WARN("Fail to deep copy", K(user_name), K(ret));
  } else if (OB_FAIL(deep_copy_str(host_name, host_name_))) {
    LOG_WARN("Fail to deep copy", K(host_name), K(ret));
  } else if (OB_FAIL(deep_copy_str(passwd, passwd_))) {
    LOG_WARN("Fail to deep copy", K(passwd), K(ret));
  } else if (OB_FAIL(deep_copy_str(info, info_))) {
    LOG_WARN("Fail to deep copy", K(passwd), K(ret));
  } else if (OB_FAIL(deep_copy_str(ssl_cipher, ssl_cipher_))) {
    LOG_WARN("Fail to deep copy", K(ssl_cipher), K(ret));
  } else if (OB_FAIL(deep_copy_str(x509_issuer, x509_issuer_))) {
    LOG_WARN("Fail to deep copy", K(x509_issuer), K(ret));
  } else if (OB_FAIL(deep_copy_str(x509_subject, x509_subject_))) {
    LOG_WARN("Fail to deep copy", K(x509_subject), K(ret));
  } else if (OB_SUCC(ret) && pos < data_len) {
    LST_DO_CODE(OB_UNIS_DECODE,
        type_,
        grantee_id_array_,
        role_id_array_,
        profile_id_,
        password_last_changed_timestamp_,
        role_id_option_array_,
        max_connections_,
        max_user_connections_);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(deserialize_proxy_info_array_(proxied_user_info_, proxied_user_info_cnt_, proxied_user_info_capacity_,
                                                buf, data_len, pos))) {
        LOG_WARN("deserialize proxi info array failed", K(ret));
      } else if (OB_FAIL(deserialize_proxy_info_array_(proxy_user_info_, proxy_user_info_cnt_, proxy_user_info_capacity_,
                                                buf, data_len, pos))) {
        LOG_WARN("deserialize proxi info array failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, user_flags_);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deserialize_proxy_info_array_(proxied_user_info_, proxied_user_info_cnt_, proxied_user_info_capacity_,
                                              buf, data_len, pos))) {
      LOG_WARN("deserialize proxi info array failed", K(ret));
    } else if (OB_FAIL(deserialize_proxy_info_array_(proxy_user_info_, proxy_user_info_cnt_, proxy_user_info_capacity_,
                                              buf, data_len, pos))) {
      LOG_WARN("deserialize proxi info array failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, user_flags_);
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObUserInfo)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              user_name_,
              host_name_,
              passwd_,
              info_,
              locked_,
              ssl_type_,
              ssl_cipher_,
              x509_issuer_,
              x509_subject_,
              type_,
              profile_id_,
              password_last_changed_timestamp_,
              max_connections_,
              max_user_connections_);
  len += grantee_id_array_.get_serialize_size();
  len += role_id_array_.get_serialize_size();
  len += role_id_option_array_.get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, proxied_user_info_cnt_);
  for (int64_t i = 0; i < proxied_user_info_cnt_; ++i) {
    if (OB_NOT_NULL(proxied_user_info_[i])) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, *proxied_user_info_[i]);
    }
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, proxy_user_info_cnt_);
  for (int64_t i = 0; i < proxy_user_info_cnt_; ++i) {
    if (OB_NOT_NULL(proxy_user_info_[i])) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, *proxy_user_info_[i]);
    }
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, user_flags_);
  return len;
}

int ObUserInfo::add_role_id(
    const uint64_t id,
    const uint64_t admin_option,
    const uint64_t disable_flag)
{
  int ret = OB_SUCCESS;
  uint64_t option = 0;
  set_admin_option(option, admin_option);
  set_disable_flag(option, disable_flag);
  OZ (role_id_array_.push_back(id));
  OZ (role_id_option_array_.push_back(option));
  return ret;
}

bool ObUserInfo::role_exists(
  const uint64_t role_id,
  const uint64_t option) const
{
  bool exists = false;
  for (int64_t i = 0; !exists && i < get_role_count(); ++i) {
    if (role_id == get_role_id_array().at(i)
       && (option == NO_OPTION ||
          option == get_admin_option(get_role_id_option_array().at(i)))) {
      exists = true;
    }
  }
  return exists;
}

int ObUserInfo::get_seq_by_role_id(
    uint64_t role_id,
    uint64_t &seq) const
{
  int ret = OB_SUCCESS;
  bool exists = false;
  seq = OB_INVALID_ID;
  for (uint64_t i = 0; !exists && i < get_role_count(); ++i) {
    if (role_id == get_role_id_array().at(i)) {
      exists = true;
      seq = i;
    }
  }
  if (!exists) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObUserInfo::get_nth_role_option(
    uint64_t nth,
    uint64_t &option) const
{
  int ret = OB_SUCCESS;
  option = NO_OPTION;
  if (nth >= get_role_count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    option = get_role_id_option_array().at(nth);
  }
  return ret;
}

//ObDBPriv
ObDBPriv& ObDBPriv::operator=(const ObDBPriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
    } else {
      sort_ = other.sort_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObDBPriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObDBPriv::reset()
{
  db_.reset();
  sort_ = 0;
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObDBPriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObDBPriv) - sizeof(ObPriv);
  convert_size += db_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObDBPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, db_, sort_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDBPriv)
{
  int ret = OB_SUCCESS;
  ObString db;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, db, sort_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WARN("Fail to deep copy user_name", K(db), K(ret));
  } else {}
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDBPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, sort_);
  return len;
}

//ObTablePriv
ObTablePriv& ObTablePriv::operator=(const ObTablePriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.table_, table_))) {
      LOG_WARN("Fail to deep copy table", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObTablePriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObTablePriv::reset()
{
  db_.reset();
  table_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObTablePriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObTablePriv) - sizeof(ObPriv);
  convert_size += db_.length() + 1;
  convert_size += table_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObTablePriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, db_, table_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTablePriv)
{
  int ret = OB_SUCCESS;
  ObString db;
  ObString table;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, db, table);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WARN("Fail to deep copy user_name", K(db), K(ret));
  } else if (OB_FAIL(deep_copy_str(table, table_))) {
    LOG_WARN("Fail to deep copy user_name", K(table), K(ret));
  } else {}
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTablePriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, table_);
  return len;
}

//ObRoutinePriv

int ObRoutinePriv::assign(const ObRoutinePriv &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.routine_, routine_))) {
      LOG_WARN("Fail to deep copy table", K(ret));
    } else {
      routine_type_ = other.routine_type_;
      error_ret_ = other.error_ret_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return ret;
}

bool ObRoutinePriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid() && routine_type_ != 0;
}

void ObRoutinePriv::reset()
{
  db_.reset();
  routine_.reset();
  routine_type_ = 0;
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObRoutinePriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObRoutinePriv) - sizeof(ObPriv);
  convert_size += db_.length() + 1;
  convert_size += routine_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObRoutinePriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, db_, routine_, routine_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRoutinePriv)
{
  int ret = OB_SUCCESS;
  ObString db;
  ObString routine;
  int64_t routine_type;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, db, routine, routine_type);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WARN("Fail to deep copy user_name", K(db), K(ret));
  } else if (OB_FAIL(deep_copy_str(routine, routine_))) {
    LOG_WARN("Fail to deep copy user_name", K(routine_), K(ret));
  } else {}
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRoutinePriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, routine_, routine_type_);
  return len;
}

int ObColumnPriv::assign(const ObColumnPriv &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.table_, table_))) {
      LOG_WARN("Fail to deep copy table", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.column_, column_))) {
      LOG_WARN("Fail to deep copy table", K(ret));
    } else {
      priv_id_ = other.priv_id_;
      error_ret_ = other.error_ret_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return ret;
}

bool ObColumnPriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid() && priv_id_ != OB_INVALID_ID;
}

void ObColumnPriv::reset()
{
  db_.reset();
  table_.reset();
  column_.reset();
  priv_id_ = 0;
  ObPriv::reset();
  ObSchema::reset();
}

int64_t ObColumnPriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObColumnPriv) - sizeof(ObPriv);
  convert_size += db_.length() + 1;
  convert_size += table_.length() + 1;
  convert_size += column_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObColumnPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, db_, table_, column_, priv_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObColumnPriv)
{
  int ret = OB_SUCCESS;
  ObString db;
  ObString table;
  ObString column;
  uint64_t priv_id;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, db, table, column, priv_id);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WARN("Fail to deep copy user_name", K(db), K(ret));
  } else if (OB_FAIL(deep_copy_str(table, table_))) {
    LOG_WARN("Fail to deep copy user_name", K(table), K(ret));
  } else if (OB_FAIL(deep_copy_str(column, column_))) {
    LOG_WARN("Fail to deep copy user_name", K(column), K(ret));
  } else {
    priv_id_ = priv_id;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObColumnPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, table_, column_, priv_id_);
  return len;
}

//ObObjPriv
ObObjPriv& ObObjPriv::operator=(const ObObjPriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else {
      error_ret_ = other.error_ret_;
      obj_id_ = other.obj_id_;
      obj_type_ = other.obj_type_;
      col_id_ = other.col_id_;
      grantor_id_ = other.grantor_id_;
      grantee_id_ = other.grantee_id_;
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObObjPriv::is_valid() const
{
  return ObSchema::is_valid()
         && tenant_id_ != common::OB_INVALID_ID
         && obj_id_ != common::OB_INVALID_ID
         && obj_type_ != common::OB_INVALID_ID
         && col_id_ != common::OB_INVALID_ID
         && grantor_id_ != common::OB_INVALID_ID
         && grantee_id_ != common::OB_INVALID_ID;
}

void ObObjPriv::reset()
{
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObObjPriv::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  return convert_size;
}

OB_DEF_SERIALIZE(ObObjPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, obj_id_, obj_type_, col_id_, grantor_id_, grantee_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObObjPriv)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, obj_id_, obj_type_, col_id_, grantor_id_, grantee_id_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObObjPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, obj_id_, obj_type_, col_id_, grantor_id_, grantee_id_);
  return len;
}

//ObSysPriv
ObSysPriv& ObSysPriv::operator=(const ObSysPriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    }
    error_ret_ = other.error_ret_;
    grantee_id_= other.grantee_id_;
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObSysPriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObSysPriv::reset()
{
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObSysPriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObSysPriv) - sizeof(ObPriv);
  return convert_size;
}

OB_DEF_SERIALIZE(ObSysPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, grantee_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSysPriv)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, grantee_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSysPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, grantee_id_);
  return len;
}

int ObNeedPriv::deep_copy(const ObNeedPriv &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  priv_level_ = other.priv_level_;
  priv_set_ = other.priv_set_;
  is_sys_table_ = other.is_sys_table_;
  is_for_update_ = other.is_for_update_;
  priv_check_type_ = other.priv_check_type_;
  obj_type_ = other.obj_type_;
  check_any_column_priv_ = other.check_any_column_priv_;
  if (OB_FAIL(ob_write_string(allocator, other.db_, db_))) {
    LOG_WARN("Fail to deep copy db", K_(db), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.table_, table_))) {
    LOG_WARN("Fail to deep copy table", K_(table), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.columns_.count(); i++) {
      ObString tmp_column;
      if (OB_FAIL(ob_write_string(allocator, other.columns_.at(i), tmp_column))) {
        LOG_WARN("ob write string failed", K(ret));
      } else if (OB_FAIL(columns_.push_back(tmp_column))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObStmtNeedPrivs::deep_copy(const ObStmtNeedPrivs &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  need_privs_.reset();
  if (OB_FAIL(need_privs_.reserve(other.need_privs_.count()))) {
    LOG_WARN("fail to reserve need prives size", K(ret), K(other.need_privs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.need_privs_.count(); ++i) {
    const ObNeedPriv &priv_other = other.need_privs_.at(i);
    ObNeedPriv priv_new;
    if (OB_FAIL(priv_new.deep_copy(priv_other, allocator))) {
      LOG_WARN("Fail to deep copy ObNeedPriv", K(priv_new), K(ret));
    } else {
      need_privs_.push_back(priv_new);
    }
  }
  return ret;
}

ObOraNeedPriv::ObOraNeedPriv(ObIAllocator &allocator)
  : db_name_(),
    grantee_id_(common::OB_INVALID_ID),
    obj_id_(common::OB_INVALID_ID),
    obj_level_(common::OB_INVALID_ID),
    obj_type_(common::OB_INVALID_ID),
    check_flag_(false),
    obj_privs_(0),
    owner_id_(common::OB_INVALID_ID),
    col_id_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
{}

int ObOraNeedPriv::deep_copy(const ObOraNeedPriv &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  obj_id_ = other.obj_id_;
  obj_level_ = other.obj_level_;
  obj_type_ = other.obj_type_;
  obj_privs_ = other.obj_privs_;
  grantee_id_ = other.grantee_id_;
  check_flag_ = other.check_flag_;
  owner_id_ = other.owner_id_;
  col_id_array_.reset();
  col_id_array_.set_block_allocator(ModulePageAllocator(allocator));
  if (OB_FAIL(ob_write_string(allocator, other.db_name_, db_name_))) {
    LOG_WARN("Fail to deep copy db", K_(db_name), K(ret));
  }
  if (OB_SUCC(ret)) {
    OZ (col_id_array_.reserve(other.col_id_array_.count()));
    for (int i = 0; OB_SUCC(ret) && i < other.col_id_array_.count(); ++i) {
      OZ (col_id_array_.push_back(other.col_id_array_.at(i)));
    }
  }
  return ret;
}

bool ObOraNeedPriv::same_col_id_array(const ObOraNeedPriv &other)
{
  bool ret = true;
  if (col_id_array_.count() != other.col_id_array_.count()) {
    ret = false;
  } else {
    for (int i = 0; true == ret && i < col_id_array_.count(); ++i) {
      const uint64_t id = col_id_array_.at(i);
      if (other.col_id_array_.end() ==
          std::find(other.col_id_array_.begin(), other.col_id_array_.end(), id)) {
        ret = false;
      }
    }
  }
  return ret;
}

bool ObOraNeedPriv::same_obj(const ObOraNeedPriv &other)
{
  if (grantee_id_ == other.grantee_id_
      && obj_id_ == other.obj_id_
      && obj_level_ == other.obj_level_
      && obj_type_ == other.obj_type_
      && check_flag_ == other.check_flag_
      && same_col_id_array(other))
    return true;
  else
    return false;
}

int ObStmtOraNeedPrivs::deep_copy(const ObStmtOraNeedPrivs &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  need_privs_.reset();
  if (OB_FAIL(need_privs_.reserve(other.need_privs_.count()))) {
    LOG_WARN("fail to reserve need prives size", K(ret), K(other.need_privs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.need_privs_.count(); ++i) {
    const ObOraNeedPriv &priv_other = other.need_privs_.at(i);
    ObOraNeedPriv priv_new;
    if (OB_FAIL(priv_new.deep_copy(priv_other, allocator))) {
      LOG_WARN("Fail to deep copy ObNeedPriv", K(priv_new), K(ret));
    } else {
      need_privs_.push_back(priv_new);
    }
  }
  return ret;
}

// ObOraNeedPriv &ObOraNeedPriv::operator=(const ObOraNeedPriv &src)
// {
//   if (this != &src) {
//     obj_id_ = src.obj_id_;
//     col_id_ = src.col_id_;
//     obj_type_ = src.obj_type_;
//     obj_privs_ = src.obj_privs_;
//   }

//   return *this;
// }

const char *PART_TYPE_STR[PARTITION_FUNC_TYPE_MAX + 1] =
{
  "hash",
  "key",
  "key",
  "range",
  "range columns",
  "list",
  "list columns",
  "range",
  "unknown"
};

int get_part_type_str(const bool is_oracle_mode,
                      ObPartitionFuncType type,
                      common::ObString &str)
{
  int ret = common::OB_SUCCESS;
  if (type >= PARTITION_FUNC_TYPE_MAX) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid partition function type", K(type));
  } else {
    if (is_oracle_mode) {
      if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == type
         || PARTITION_FUNC_TYPE_INTERVAL == type) {
        type = PARTITION_FUNC_TYPE_RANGE;
      } else if (PARTITION_FUNC_TYPE_LIST_COLUMNS == type) {
        type = PARTITION_FUNC_TYPE_LIST;
      }
    }
    str = common::ObString::make_string(PART_TYPE_STR[type]);
  }
  return ret;
}

const char *OB_PRIV_LEVEL_STR[OB_PRIV_MAX_LEVEL] =
{
  "INVALID_LEVEL",
  "USER_LEVEL",
  "DB_LEVEL",
  "TABLE_LEVEL",
  "DB_ACCESS_LEVEL"
};

const char *ob_priv_level_str(const ObPrivLevel grant_level)
{
  const char *ret = "Unknown";
  if (grant_level < OB_PRIV_MAX_LEVEL && grant_level > OB_PRIV_INVALID_LEVEL) {
    ret = OB_PRIV_LEVEL_STR[grant_level];
  }
  return ret;
}

//ObTableType=>const char* ;
const char *ob_table_type_str(ObTableType type)
{
  const char *type_ptr = "UNKNOWN";
  switch (type) {
  case SYSTEM_TABLE: {
      type_ptr = "SYSTEM TABLE";
      break;
    }
  case SYSTEM_VIEW: {
      type_ptr = "SYSTEM VIEW";
      break;
    }
  case VIRTUAL_TABLE: {
      type_ptr = "VIRTUAL TABLE";
      break;
    }
  case USER_TABLE: {
      type_ptr = "USER TABLE";
      break;
    }
  case USER_VIEW: {
      type_ptr = "USER VIEW";
      break;
    }
  case USER_INDEX: {
      type_ptr = "USER INDEX";
      break;
    }
  case TMP_TABLE: {
      type_ptr = "TMP TABLE";
      break;
    }
  case MATERIALIZED_VIEW: {
      type_ptr = "MATERIALIZED VIEW";
      break;
    }
  case TMP_TABLE_ORA_SESS: {
      type_ptr = "TMP TABLE ORA SESS";
      break;
    }
  case TMP_TABLE_ORA_TRX: {
      type_ptr = "TMP TABLE ORA TRX";
      break;
    }
  case TMP_TABLE_ALL: {
      type_ptr = "TMP TABLE ALL";
      break;
    }
  case AUX_VERTIAL_PARTITION_TABLE: {
      type_ptr = "AUX VERTIAL PARTITION TABLE";
      break;
    }
  case AUX_LOB_PIECE: {
      type_ptr = "AUX LOB PIECE";
      break;
    }
  case AUX_LOB_META: {
      type_ptr = "AUX LOB META";
      break;
    }
  case EXTERNAL_TABLE: {
      type_ptr = "EXTERNAL TABLE";
      break;
    }
  case MATERIALIZED_VIEW_LOG: {
      type_ptr = "MATERIALIZED VIEW LOG";
      break;
    }
  default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unkonw table type", K(type));
      break;
    }
  }
  return type_ptr;
}

//ObTableType => mysql table type str : SYSTEM VIEW, BASE TABLE, VIEW
const char *ob_mysql_table_type_str(ObTableType type)
{
  const char *type_ptr = "UNKNOWN";
  switch (type) {
    case SYSTEM_TABLE:
    case USER_TABLE:
      type_ptr = "BASE TABLE";
      break;
    case USER_VIEW:
      type_ptr = "VIEW";
      break;
    case SYSTEM_VIEW:
      type_ptr = "SYSTEM VIEW";
      break;
    case VIRTUAL_TABLE:
      type_ptr = "VIRTUAL TABLE";
      break;
    case USER_INDEX:
      type_ptr = "USER INDEX";
      break;
    case TMP_TABLE:
      type_ptr = "TMP TABLE";
      break;
    default:
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unkonw table type", K(type));
      break;
  }
  return type_ptr;
}

ObTableType get_inner_table_type_by_id(const uint64_t tid) {
  if (!is_inner_table(tid)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "tid is not inner table", K(tid));
  }
  ObTableType type = MAX_TABLE_TYPE;
  if (is_sys_table(tid)) {
    type = SYSTEM_TABLE;
  } else if (is_virtual_table(tid)) {
    type = VIRTUAL_TABLE;
  } else if (is_sys_view(tid)) {
    type = SYSTEM_VIEW;
  } else {
    // MAX_TABLE_TYPE;
  }
  return type;
}

bool is_mysql_tmp_table(const ObTableType table_type)
{
  return ObTableType::TMP_TABLE == table_type;
}

bool is_view_table(const ObTableType table_type)
{
  return ObTableType::USER_VIEW == table_type
         || ObTableType::SYSTEM_VIEW == table_type
         || ObTableType::MATERIALIZED_VIEW == table_type;
}

bool is_index_table(const ObTableType table_type)
{
  return ObTableType::USER_INDEX == table_type;
}

bool is_aux_lob_meta_table(const ObTableType table_type)
{
  return ObTableType::AUX_LOB_META == table_type;
}

bool is_aux_lob_piece_table(const ObTableType table_type)
{
  return ObTableType::AUX_LOB_PIECE == table_type;
}

bool is_aux_lob_table(const ObTableType table_type)
{
  return is_aux_lob_meta_table(table_type) || is_aux_lob_piece_table(table_type);
}

bool is_mlog_table(const ObTableType table_type)
{
  return (ObTableType::MATERIALIZED_VIEW_LOG == table_type);
}

const char *schema_type_str(const ObSchemaType schema_type)
{
  const char *str = "";
  if (TENANT_SCHEMA == schema_type) {
    str = "tenant_schema";
  } else if (USER_SCHEMA == schema_type) {
    str = "user_schema";
  } else if (DATABASE_SCHEMA == schema_type) {
    str = "database_schema";
  } else if (TABLEGROUP_SCHEMA == schema_type) {
    str = "tablegroup_schema";
  } else if (TABLE_SCHEMA == schema_type) {
    str = "table_schema";
  } else if (DATABASE_PRIV == schema_type) {
    str = "database_priv";
  } else if (TABLE_PRIV == schema_type) {
    str = "table_priv";
  } else if (ROUTINE_PRIV == schema_type) {
    str = "routine_priv";
  } else if (OUTLINE_SCHEMA == schema_type) {
    str = "outline_schema";
  } else if (SYNONYM_SCHEMA == schema_type) {
    str = "synonym_schema";
  } else if (UDF_SCHEMA == schema_type) {
    str = "udf_schema";
  } else if (UDT_SCHEMA == schema_type) {
    str = "udt_schema";
  } else if (SEQUENCE_SCHEMA == schema_type) {
    str = "sequence_schema";
  } else if (LABEL_SE_POLICY_SCHEMA == schema_type) {
    str = "label_se_policy_schema";
  } else if (LABEL_SE_COMPONENT_SCHEMA == schema_type) {
    str = "label_se_component_schema";
  } else if (LABEL_SE_LABEL_SCHEMA == schema_type) {
    str = "label_se_label_schema";
  } else if (LABEL_SE_USER_LEVEL_SCHEMA == schema_type) {
    str = "label_se_user_level_schema";
  } else if (PROFILE_SCHEMA == schema_type) {
    str = "profile_schema";
  } else if (DBLINK_SCHEMA == schema_type) {
    str = "dblink_schema";
  } else if (FK_SCHEMA == schema_type) {
    str = "fk_schema";
  }
  return str;
}

bool is_normal_schema(const ObSchemaType schema_type)
{
  return schema_type == TENANT_SCHEMA ||
      schema_type == USER_SCHEMA ||
      schema_type == DATABASE_SCHEMA ||
      schema_type == TABLEGROUP_SCHEMA ||
      schema_type == TABLE_SCHEMA ||
      schema_type == OUTLINE_SCHEMA ||
      schema_type == ROUTINE_SCHEMA ||
      schema_type == SYNONYM_SCHEMA ||
      schema_type == PACKAGE_SCHEMA ||
      schema_type == TRIGGER_SCHEMA ||
      schema_type == SEQUENCE_SCHEMA ||
      schema_type == UDF_SCHEMA ||
      schema_type == UDT_SCHEMA ||
      schema_type == SYS_VARIABLE_SCHEMA ||
      schema_type == TABLE_SIMPLE_SCHEMA ||
      schema_type == KEYSTORE_SCHEMA ||
      schema_type == TABLESPACE_SCHEMA ||
      schema_type == LABEL_SE_POLICY_SCHEMA ||
      schema_type == LABEL_SE_COMPONENT_SCHEMA ||
      schema_type == LABEL_SE_LABEL_SCHEMA ||
      schema_type == LABEL_SE_USER_LEVEL_SCHEMA ||
      schema_type == PROFILE_SCHEMA ||
      schema_type == DBLINK_SCHEMA ||
      schema_type == MOCK_FK_PARENT_TABLE_SCHEMA ||
      false;
}

#if 0
//------Funcs of outlineinfo-----//
ObTenantOutlineId &ObTenantOutlineId::operator =(const ObTenantOutlineId &tenant_outline_id)
{
  tenant_id_ = tenant_outline_id.tenant_id_;
  outline_id_ = tenant_outline_id.outline_id_;
  return *this;
}
#endif

bool ObFixedParam::has_equal_value(const ObObj &other_value) const
{
  bool is_equal = false;
  if (value_.get_type() != other_value.get_type()) {
    is_equal = false;
  } else {
    is_equal = value_ == other_value;
  }
  return is_equal;
}

bool ObFixedParam::is_equal(const ObFixedParam &other_param) const
{
  bool is_equal = false;
  if (has_equal_value(other_param.value_)) {
    is_equal = offset_ == other_param.offset_;
  }
  return is_equal;
}

ObMaxConcurrentParam::ObMaxConcurrentParam(common::ObIAllocator *allocator,
                                           const common::ObMemAttr &attr) :
      allocator_(allocator),
      concurrent_num_(UNLIMITED),
      outline_content_(),
      mem_attr_(attr),
      fixed_param_store_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                         ObWrapperAllocatorWithAttr(allocator, attr)),
      sql_text_()
{
}

ObMaxConcurrentParam::~ObMaxConcurrentParam()
{
}

int ObMaxConcurrentParam::destroy()
{
  int ret = OB_SUCCESS;

  //reset outline_content
  if (outline_content_.ptr() != NULL) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("allocator is NULL", K(ret));
    } else {
      allocator_->free(outline_content_.ptr());
    }
  }
  outline_content_.reset();

  //reset fixed_param_store
  if (fixed_param_store_.count() != 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("allocator is NULL", K(ret));
    } else {
      for (int64_t i = 0; i < fixed_param_store_.count(); ++i) {
        fixed_param_store_.at(i).offset_ = OB_INVALID_INDEX;
        ObObj &cur_value = fixed_param_store_.at(i).value_;
        if (cur_value.need_deep_copy()) {
          if (OB_ISNULL(cur_value.get_data_ptr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("ptr is NULL", K(ret), K(cur_value));
          } else {
            allocator_->free(const_cast<void *>(cur_value.get_data_ptr()));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    fixed_param_store_.destroy();
    concurrent_num_ = UNLIMITED;
    //allocator_ = NULL;
  }

  //reset sql_text
  if (OB_SUCC(ret) && sql_text_.ptr() != NULL) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("allocator is NULL", K(ret));
    } else {
      allocator_->free(sql_text_.ptr());
    }
  }
  sql_text_.reset();

  return ret;
}

int ObMaxConcurrentParam::match_fixed_param(const ParamStore &const_param_store,
                                            bool &is_match) const
{
  int ret = OB_SUCCESS;
  is_match = false;
  bool is_same = true;
  if (const_param_store.count() < fixed_param_store_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect param num", K(ret), K(const_param_store.count()), K(fixed_param_store_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < fixed_param_store_.count(); i++) {
      int64_t offset_of_plan_param_store = fixed_param_store_.at(i).offset_;
      const ObObj &plan_param_value = const_param_store.at(offset_of_plan_param_store);
      const ObFixedParam &cur_fixed_param = fixed_param_store_.at(i);
      is_same = cur_fixed_param.has_equal_value(plan_param_value);
    }
  }
  if (OB_SUCC(ret)) {
    is_match = is_same;
  }
  return ret;
}

//this func only compare fixed_params
int ObMaxConcurrentParam::same_param_as(const ObMaxConcurrentParam &other, bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (fixed_param_store_.count() != other.fixed_param_store_.count()) {
    is_same = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < fixed_param_store_.count(); i++) {
      const ObFixedParam &cur_fixed_param = fixed_param_store_.at(i);
      const ObFixedParam &other_fixed_param = other.fixed_param_store_.at(i);
      is_same = cur_fixed_param.is_equal(other_fixed_param);
    }
  }
  return ret;
}


int ObMaxConcurrentParam::assign(const ObMaxConcurrentParam &src_param)
{
  int ret = OB_SUCCESS;
  if (this != &src_param) {
    //copy concurent_num
    concurrent_num_ = src_param.concurrent_num_;
    mem_attr_ = src_param.mem_attr_;

    //copy outline_content
    if (OB_FAIL(deep_copy_outline_content(src_param.outline_content_))) {
      LOG_WARN("fail to deep copy outline_content", K(ret));
    }

    //copy fix_param
    fixed_param_store_.reset();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fixed_param_store_.reserve(src_param.fixed_param_store_.count()))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src_param.fixed_param_store_.count(); i++) {
        const ObFixedParam &cur_fixed_param = src_param.fixed_param_store_.at(i);
        ObFixedParam tmp_fixed_param;
        tmp_fixed_param.offset_ = cur_fixed_param.offset_;
        if (OB_FAIL(deep_copy_param_value(cur_fixed_param.value_, tmp_fixed_param.value_))) {
          LOG_WARN("fail to deep copy param value", K(ret));
        } else if (OB_FAIL(fixed_param_store_.push_back(tmp_fixed_param))) {
          LOG_WARN("fail to push back fix param", K(ret));
        } else {/*do nothing*/}
      }
    }

    //copy sql_text
    if (OB_SUCC(ret) && OB_FAIL(deep_copy_sql_text(src_param.sql_text_))) {
      LOG_WARN("fail to deep copy sql_text", K(ret));
    }
  }
  return ret;
}

int64_t ObMaxConcurrentParam::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += outline_content_.length() + 1;
  convert_size += fixed_param_store_.get_data_size();
  for (int64_t i = 0; i < fixed_param_store_.count(); ++i) {
    convert_size += sizeof(fixed_param_store_.at(i));
    convert_size += fixed_param_store_.at(i).value_.get_deep_copy_size();
  }
  convert_size += sql_text_.length() + 1;
  return convert_size;
}

int ObMaxConcurrentParam::deep_copy_outline_content(const ObString &src)
{
  int ret = OB_SUCCESS;
  char *content_buf = NULL;
  if (src.length() > 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_UNLIKELY(NULL == (content_buf = static_cast<char *>(allocator_->alloc(src.length() + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory", "len", src.length() + 1, K(ret));
    } else {
      MEMCPY(content_buf, src.ptr(), src.length());
      content_buf[src.length()] = '\0';
      outline_content_.assign_ptr(content_buf, static_cast<ObString::obstr_size_t>(src.length()));
    }
  } else {
    outline_content_.reset();
  }
  return ret;
}

int ObMaxConcurrentParam::deep_copy_sql_text(const ObString &src)
{
  int ret = OB_SUCCESS;
  char *text_buf = NULL;
  if (src.length() > 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_ISNULL(text_buf = static_cast<char *>(allocator_->alloc(src.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory", "len", src.length() + 1, K(ret));
    } else {
      MEMCPY(text_buf, src.ptr(), src.length());
      text_buf[src.length()] = '\0';
      sql_text_.assign_ptr(text_buf, static_cast<ObString::obstr_size_t>(src.length()));
    }
  } else {
    sql_text_.reset();
  }
  return ret;
}

int ObMaxConcurrentParam::deep_copy_param_value(const ObObj &src, ObObj &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();
  if (size > 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_->alloc(size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))){
      LOG_WARN("Fail to deep copy obj, ", K(ret));
    } else {/*do nothing*/}
  } else {
    dest = src;
  }
  return ret;
}

int ObMaxConcurrentParam::get_fixed_param_with_offset(int64_t offset,
                                                      ObFixedParam &fixed_param,
                                                      bool &is_found) const
{
  int ret = OB_SUCCESS;
  is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && false == is_found && i < fixed_param_store_.count(); ++i) {
    if (offset == fixed_param_store_.at(i).offset_) {
      is_found = true;
      fixed_param = fixed_param_store_.at(i);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObMaxConcurrentParam)
{
  int ret = OB_SUCCESS;

  const int64_t param_count = fixed_param_store_.count();
  LST_DO_CODE(OB_UNIS_ENCODE, concurrent_num_, outline_content_, param_count);

  //serilize fixed_param_store_
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    const ObFixedParam &cur_fixed_param = fixed_param_store_.at(i);
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cur_fixed_param.offset_))) {
      LOG_WARN("fail to serialize cur_fixed_param offset", K(ret));
    } else if (OB_FAIL(cur_fixed_param.value_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize cur_fixed_param value", K(ret));
    } else {/*do nothing*/ }
  }

  LST_DO_CODE(OB_UNIS_ENCODE, sql_text_);
  return ret;
}


OB_DEF_DESERIALIZE(ObMaxConcurrentParam)
{
  int ret = OB_SUCCESS;
  ObString outline_content;
  int64_t param_count = 0;;
  ObString sql_text;
  LST_DO_CODE(OB_UNIS_DECODE, concurrent_num_, outline_content, param_count);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_outline_content(outline_content))) {
    LOG_WARN("fail to deep copy outline_content", K(ret));
  } else {
    ObFixedParam fixed_param;
    ObObj tmp_value;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count ; i++) {
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &fixed_param.offset_))) {
        LOG_WARN("fail to deserialize fixed_param offset", K(ret));
      } else if (OB_FAIL(tmp_value.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize fixed_param value", K(ret));
      } else if (OB_FAIL(deep_copy_param_value(tmp_value, fixed_param.value_))){
        LOG_WARN("fail to deep_copy fixed_param value");
      } else if (OB_FAIL(fixed_param_store_.push_back(fixed_param))) {
        LOG_WARN("fail to push back fixed_param to param store", K(ret));
      } else {/*do nothing*/}
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, sql_text);
  if (OB_SUCC(ret) && OB_FAIL(deep_copy_sql_text(sql_text))) {
    LOG_WARN("fail to deep copy sql_text", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMaxConcurrentParam)
{
  int64_t len = 0;
  const int64_t param_count = fixed_param_store_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, concurrent_num_, outline_content_, param_count);

  for (int64_t i = 0; i < param_count; ++i) {
    const ObFixedParam &cur_fixed_param = fixed_param_store_.at(i);
    len += serialization::encoded_length_vi64(cur_fixed_param.offset_);
    len += cur_fixed_param.value_.get_serialize_size();
  }

  LST_DO_CODE(OB_UNIS_ADD_LEN, sql_text_);
  return len;
}


ObOutlineParamsWrapper::ObOutlineParamsWrapper() :
    allocator_(NULL),
    outline_params_()
{
}

ObOutlineParamsWrapper::ObOutlineParamsWrapper(common::ObIAllocator *allocator) :
    allocator_(allocator),
    outline_params_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                    ObWrapperAllocatorWithAttr(allocator))
{
}

ObOutlineParamsWrapper::~ObOutlineParamsWrapper()
{
}

int ObOutlineParamsWrapper::destroy()
{
  int ret = OB_SUCCESS;
  int64_t param_count = outline_params_.count();
  if (0 == param_count) {//do nothing
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("allocator is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < outline_params_.count(); ++i) {
      ObMaxConcurrentParam *param = outline_params_.at(i);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("param is NULL", K(ret), K(i));
      } else {
        param->destroy();
        param->~ObMaxConcurrentParam();
        allocator_->free(param);
      }
    }
  }
  if (OB_SUCC(ret)) {
    outline_params_.destroy();
  }
  return ret;
}

ObMaxConcurrentParam *ObOutlineParamsWrapper::get_outline_param(int64_t index) const
{
  ObMaxConcurrentParam *ret = NULL;
  if (index < outline_params_.count()) {
    ret = outline_params_.at(index);
  } else {
    LOG_ERROR_RET(OB_SIZE_OVERFLOW, "index overflow", K(index), K(outline_params_.count()));
  }
  return ret;
}

int ObOutlineInfo::replace_question_mark(const ObString &not_param_sql,
                                         const ObMaxConcurrentParam &concurrent_param,
                                         int64_t start_pos,
                                         int64_t cur_pos,
                                         int64_t &question_mark_offset,
                                         ObSqlString &string_helper)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator local_allocator;
  ObFixedParam fixed_param;
  bool is_found = false;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_VARCHAR_LENGTH;
  int64_t pos = 0;

  if (OB_FAIL(concurrent_param.get_fixed_param_with_offset(question_mark_offset, fixed_param, is_found))) {
    LOG_WARN("fail to get fixed_param with offset", K(ret), K(question_mark_offset));
  } else {
    ObString before_token(cur_pos - start_pos, not_param_sql.ptr() + start_pos);
    if (is_found) {
      if (OB_UNLIKELY(NULL == (buf = static_cast<char *>(local_allocator.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(buf_len));
      } else if (OB_FAIL(fixed_param.value_.print_sql_literal(buf, buf_len, pos))) {
        LOG_WARN("fail print obj", K(fixed_param), K(ret), K(buf), K(buf_len), K(pos));
      } else if (OB_FAIL(string_helper.append_fmt("%.*s%s", before_token.length(), before_token.ptr(), buf))) {
        LOG_WARN("fail to append fmt", K(ret), K(before_token), K(buf));
      } else {/*do nothing*/}
    } else {
      if (OB_FAIL(string_helper.append_fmt("%.*s%s", before_token.length(), before_token.ptr(), "?"))) {
        LOG_WARN("fail to append fmt", K(ret), K(before_token));
      }
    }
    ++question_mark_offset;
  }

  return ret;
}

int ObOutlineInfo::replace_not_param(const ObString &not_param_sql,
                                     const ParseNode &node,
                                     int64_t start_pos,
                                     int64_t cur_pos,
                                     ObSqlString &string_helper)
{
  int ret = OB_SUCCESS;
  ObString before_token(cur_pos - start_pos, not_param_sql.ptr() + start_pos);
  ObString param_value(node.text_len_, node.raw_text_);
  if (OB_FAIL(string_helper.append_fmt("%.*s%.*s",
                                       before_token.length(), before_token.ptr(),
                                       param_value.length(), param_value.ptr()))) {
    LOG_WARN("fail to append fmt", K(ret), K(before_token), K(param_value));
  }

  return ret;
}

int ObOutlineInfo::gen_limit_sql(const ObString &visible_signature,
                                 const ObMaxConcurrentParam *concurrent_param,
                                 const ObSQLSessionInfo &session,
                                 ObIAllocator &allocator,
                                 ObString &limit_sql)
{
  int ret = OB_SUCCESS;
  ObString not_param_sql;
  ObString last_token;
  int64_t start_pos = 0;
  int64_t cur_pos = 0;
  int64_t question_mark_offset = 0;
  ObSqlString string_helper;
  ObParser parser(allocator, session.get_sql_mode());
  ParseResult parse_result;
  parse_result.param_node_num_ = 0;
  ParamList *parser_param = NULL;
  ParseNode *cur_node = NULL;

  if (OB_ISNULL(concurrent_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(concurrent_param));
  } else if (OB_FAIL(parser.parse(visible_signature, parse_result, FP_MODE))) {
    LOG_WARN("fail to parser visible signature", K(ret), K(visible_signature));
  } else {
    parser_param = parse_result.param_nodes_;
    not_param_sql.assign_ptr(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
  }

  for(int64_t i = 0 ; OB_SUCC(ret) && i < parse_result.param_node_num_; ++i) {
    if (OB_ISNULL(parser_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parser param is NULL", K(ret));
    } else if(OB_ISNULL(cur_node = parser_param->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is NULL", K(ret));
    } else {
      cur_pos = cur_node->pos_;
      if (T_QUESTIONMARK == cur_node->type_) {
        if (OB_FAIL(replace_question_mark(not_param_sql, *concurrent_param, start_pos, cur_pos, question_mark_offset, string_helper))) {
          LOG_WARN("fail to replace question mark", K(ret), K(start_pos), K(cur_pos));
        }
      } else {
        if (OB_FAIL(replace_not_param(not_param_sql, *cur_node, start_pos, cur_pos, string_helper))) {
          LOG_WARN("fail to replace not param", K(ret), K(start_pos), K(cur_pos));
        }
      }
      start_pos = cur_pos + 1;
      parser_param = parser_param->next_;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(last_token.assign_ptr(not_param_sql.ptr() + start_pos, static_cast<ObString::obstr_size_t>(not_param_sql.length() - start_pos)))) {
  } else if (OB_FAIL(string_helper.append_fmt("%.*s", last_token.length(), last_token.ptr()))) {
    LOG_WARN("fail to append fmt", K(ret), K(not_param_sql), K(last_token), K(start_pos));
  } else if (OB_FAIL(ob_write_string(allocator, string_helper.string(), limit_sql))) {
    LOG_WARN("fail to deep copy string", K(ret), K(string_helper), K(limit_sql));
  } else {/*do nothing*/}

  return ret;
}

int ObOutlineParamsWrapper::set_allocator(ObIAllocator *allocator, const common::ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_UNLIKELY(allocator_ != NULL && allocator_ != allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't set different allocator", K(ret), K(allocator_), K(allocator));
  } else {
    allocator_ = allocator;
    mem_attr_ = attr;
    outline_params_.set_block_allocator(ObWrapperAllocatorWithAttr(allocator, mem_attr_));
  }
  return ret;
}

int ObOutlineParamsWrapper::assign(const ObOutlineParamsWrapper &src)
{
  int ret = OB_SUCCESS;
  if (this != &src) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_SUCCESS;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_FAIL(outline_params_.reserve(src.outline_params_.count()))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.outline_params_.count(); ++i) {
        void *buf = NULL;
        ObMaxConcurrentParam *param = NULL;
        if (OB_UNLIKELY(NULL == (buf = allocator_->alloc(sizeof(ObMaxConcurrentParam))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory", K(ret));
        } else if (FALSE_IT(param = new (buf) ObMaxConcurrentParam(allocator_, mem_attr_))) {
        } else if (OB_ISNULL(src.outline_params_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param pointer is NULL", K(ret));
        } else {
          if (OB_FAIL(param->assign(*src.outline_params_.at(i)))) {
            LOG_WARN("fail to assign params", K(ret));
          } else if (OB_FAIL(outline_params_.push_back(param))) {
            LOG_WARN("fail to push_back param", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObOutlineParamsWrapper::add_param(const ObMaxConcurrentParam& src_param)
{
  int ret = OB_SUCCESS;
  void *param_buf = NULL;
  ObMaxConcurrentParam *param = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_UNLIKELY(NULL == (param_buf = allocator_->alloc(sizeof(ObMaxConcurrentParam))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(sizeof(ObMaxConcurrentParam)), K(ret));
  } else if (FALSE_IT(param = new (param_buf) ObMaxConcurrentParam(allocator_))) {
  } else if (OB_FAIL(param->assign(src_param))) {
    LOG_WARN("fail to assgin param", K(ret));
  } else if (OB_FAIL(outline_params_.push_back(param))) {
    LOG_WARN("fail to push back param");
  } else {/*do nothing*/}
  return ret;
}

//only compare fixed params
int ObOutlineParamsWrapper::has_param(const ObMaxConcurrentParam& param, bool &has_param) const
{
  int ret = OB_SUCCESS;
  has_param = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_param && i < outline_params_.count(); i++) {
    if (OB_ISNULL(outline_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param pointer is NULL", K(ret));
    } else if (OB_FAIL(outline_params_.at(i)->same_param_as(param, has_param))) {
      LOG_WARN("failed to check if param is same with local params", K(i), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int64_t ObOutlineParamsWrapper::get_convert_size() const
{
  int64_t convert_size = 0;
  int ret = OB_SUCCESS;
  convert_size += sizeof(ObOutlineParamsWrapper);
  convert_size += outline_params_.get_data_size();
  for (int64_t i = 0; OB_SUCC(ret) && i < outline_params_.count(); i++) {
    if (OB_ISNULL(outline_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("param pointer is NULL", K(ret));
    } else {
      convert_size += outline_params_.at(i)->get_convert_size();
    }
  }
  return convert_size;
}

int ObOutlineParamsWrapper::has_concurrent_limit_param(bool &has_limit_param) const
{
  int ret = OB_SUCCESS;
  has_limit_param = false;
  for (int64_t i = 0; !has_limit_param && OB_SUCC(ret) && i < outline_params_.count(); ++i) {
    ObMaxConcurrentParam *param = outline_params_.at(i);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is NULL", K(ret), K(i));
    } else if (param->is_concurrent_limit_param()){
      has_limit_param = true;
    } else {/*do nothing*/}
  }
  return ret;
}

OB_DEF_SERIALIZE(ObOutlineParamsWrapper)
{
  int ret = OB_SUCCESS;
  const int64_t param_count = outline_params_.count();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, param_count))) {
    LOG_WARN("serialize count failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      if (OB_ISNULL(outline_params_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is NULL", K(ret));
      } else if (OB_FAIL(outline_params_.at(i)->serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serilize outline param", K(ret), K(i));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObOutlineParamsWrapper)
{
  int ret = OB_SUCCESS;
  int64_t param_count = 0;
  outline_params_.reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &param_count))) {
    LOG_WARN("deserialize count failed", K(ret));
  } else if (0 == param_count) { //do nohting
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      void *param_buf = NULL;
      ObMaxConcurrentParam *param = NULL;
      if (OB_UNLIKELY(NULL == (param_buf = allocator_->alloc(sizeof(ObMaxConcurrentParam))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory", K(sizeof(ObMaxConcurrentParam)), K(ret));
      } else if (FALSE_IT(param = new (param_buf) ObMaxConcurrentParam(allocator_))) {
      } else if (OB_FAIL(param->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserilize param", K(ret), K(i));
      } else if (OB_FAIL(outline_params_.push_back(param))) {
        LOG_WARN("fail to push back param", K(ret), K(i));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOutlineParamsWrapper)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(outline_params_.count());
  for (int64_t i = 0; i < outline_params_.count(); ++i) {
    if (OB_ISNULL(outline_params_.at(i))) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "param is NULL");
    } else {
      len += outline_params_.at(i)->get_serialize_size();
    }
  }
  return len;
}

ObOutlineInfo::ObOutlineInfo() : ObSchema()
{
  reset();
}

ObOutlineInfo::ObOutlineInfo(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator))) {
    LOG_ERROR("fail to set allocator", K(ret));
  }
}

ObOutlineInfo::ObOutlineInfo(const ObOutlineInfo &src_info) : ObSchema()
{
  reset();
  *this = src_info;
}

ObOutlineInfo::~ObOutlineInfo() {}

ObOutlineInfo &ObOutlineInfo::operator=(const ObOutlineInfo &src_info)
{
  if (this != &src_info) {
    reset();
    int ret = OB_SUCCESS;
    tenant_id_ = src_info.tenant_id_;
    database_id_ = src_info.database_id_;
    outline_id_ = src_info.outline_id_;
    owner_id_ = src_info.owner_id_;
    schema_version_ = src_info.schema_version_;
    used_ = src_info.used_;
    compatible_ = src_info.compatible_;
    enabled_ = src_info.enabled_;
    format_ = src_info.format_;
    format_outline_ = src_info.format_outline_;
    if (OB_FAIL(deep_copy_str(src_info.name_, name_))) {
      LOG_WARN("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.signature_, signature_))) {
      LOG_WARN("Fail to deep copy signature", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.sql_id_, sql_id_))) {
      LOG_WARN("Fail to deep copy signature", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.outline_content_, outline_content_))) {
      LOG_WARN("Fail to deep copy outline_content", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.sql_text_, sql_text_))) {
      LOG_WARN("Fail to deep copy sql_text", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.format_sql_text_, format_sql_text_))) {
      LOG_WARN("Fail to deep copy sql_text", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.format_sql_id_, format_sql_id_))) {
      LOG_WARN("Fail to deep copy signature", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.outline_target_, outline_target_))) {
      LOG_WARN("Fail to deep copy outline target", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.owner_, owner_))) {
      LOG_WARN("Fail to deep copy owner", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.version_, version_))) {
      LOG_WARN("Fail to deep copy version", K(ret));
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator_))) {
      LOG_WARN("fail to set allocator", K(ret));
    } else if (OB_FAIL(outline_params_wrapper_.assign(src_info.outline_params_wrapper_))) {
      LOG_WARN("fail to assgin OutlineParamWrapper", K(ret));
    } else {/*do nothing*/}

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

void ObOutlineInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  outline_id_ = OB_INVALID_ID;
  owner_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  reset_string(name_);
  reset_string(signature_);
  reset_string(sql_id_);
  reset_string(format_sql_id_);
  reset_string(format_sql_text_);
  reset_string(outline_content_);
  reset_string(sql_text_);
  reset_string(outline_target_);
  reset_string(owner_);
  used_ = false;
  reset_string(version_);
  compatible_ = true;
  enabled_ = true;
  format_ = HINT_NORMAL;
  format_outline_ = false;
  outline_params_wrapper_.destroy();
  ObSchema::reset();
}

bool ObOutlineInfo::is_sql_id_valid(const ObString &sql_id)
{
  bool is_valid = true;
  if (sql_id.length() != OB_MAX_SQL_ID_LENGTH) {
    is_valid = false;
  }
  if (is_valid) {
    for (int32_t i = 0; i < sql_id.length(); i ++) {
      char c = sql_id.ptr()[i];
      if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F'))) {
        is_valid = false;
        break;
      }
    }
  }
  return is_valid;
}

bool ObOutlineInfo::is_valid() const
{

  bool valid_ret = true;
  if (!ObSchema::is_valid()) {
    valid_ret = false;
  } else if (name_.empty() || !((!signature_.empty() && !sql_text_.empty() && sql_id_.empty())
             || (signature_.empty() && sql_text_.empty() && is_sql_id_valid(sql_id_)))
             || owner_.empty() || version_.empty()
             || (outline_content_.empty() && !has_outline_params())) {
    valid_ret = false;
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_ || OB_INVALID_ID == outline_id_) {
    valid_ret = false;
  } else if (schema_version_ <= 0) {
    valid_ret = false;
  } else {/*do nothing*/}
  return valid_ret;
}

bool ObOutlineInfo::is_valid_for_replace() const
{
  bool valid_ret = true;
  if (!ObSchema::is_valid()) {
    valid_ret = false;
  } else if (name_.empty() || !((!signature_.empty() && !sql_text_.empty() && sql_id_.empty())
             || (signature_.empty() && sql_text_.empty() && is_sql_id_valid(sql_id_)))
             || owner_.empty() || version_.empty()
             || (outline_content_.empty() && !has_outline_params())) {
    valid_ret = false;
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_
             || OB_INVALID_ID == outline_id_) {
    valid_ret = false;
  } else {/*do nothing*/}
  return valid_ret;
}

int64_t ObOutlineInfo::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObOutlineInfo);
  convert_size += name_.length() + 1;
  convert_size += signature_.length() + 1;
  convert_size += sql_id_.length() + 1;
  convert_size += outline_content_.length() + 1;
  convert_size += sql_text_.length() + 1;
  convert_size += format_sql_text_.length() + 1;
  convert_size += format_sql_id_.length() + 1;
  convert_size += outline_target_.length() + 1;
  convert_size += owner_.length() + 1;
  convert_size += version_.length() + 1;
  convert_size += outline_params_wrapper_.get_convert_size();
  return convert_size;
}

int ObOutlineInfo::gen_valid_allocator()
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    if (!THIS_WORKER.has_req_flag()) {
      if (NULL == (allocator_ = OB_NEW(ObArenaAllocator,
                                       ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA,
                                       ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to new allocator.", K(ret));
      } else {
        is_inner_allocator_ = true;
      }
    } else {
      allocator_ = &THIS_WORKER.get_allocator();
    }
  }
  return ret;
}

int ObOutlineInfo::set_outline_params(const ObString &outline_params_str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(gen_valid_allocator())) {
    LOG_WARN("fail to gen valid allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator_))) {
    LOG_WARN("fail to set allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.deserialize(
                         outline_params_str.ptr(), outline_params_str.length(), pos))) {
    LOG_WARN("fail to deserialize outline params str", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOutlineInfo::get_hex_str_from_outline_params(ObString &hex_str, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  char *hex_str_buf = NULL;
  int64_t hex_str_buf_size = outline_params_wrapper_.get_serialize_size();
  int64_t pos = 0;
  if (OB_UNLIKELY(NULL == (hex_str_buf = static_cast<char *>(allocator.alloc(hex_str_buf_size))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(hex_str_buf_size), K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.serialize(hex_str_buf, hex_str_buf_size, pos))) {
    LOG_WARN("fail to serialize_outline_params", K(ret));
  } else {
    hex_str.assign_ptr(hex_str_buf, static_cast<ObString::obstr_size_t>(pos));
  }
  return ret;
}

int ObOutlineInfo::add_param(const ObMaxConcurrentParam& src_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gen_valid_allocator())) {
    LOG_WARN("fail to gen valid allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator_))) {
    LOG_WARN("fail to set allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.add_param(src_param))) {
    LOG_WARN("fail to add_param", K(ret));
  }
  return ret;
}

int ObOutlineInfo::get_visible_signature(ObString &visiable_signature) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (signature_.empty()) {
    // do nothing
  } else if (OB_FAIL(visiable_signature.deserialize(signature_.ptr(), signature_.length(), pos))) {
    LOG_WARN("fail to deserialize signature", K(ret), K(signature_));
  }
  return ret;
}

int ObOutlineInfo::get_outline_sql(ObIAllocator &allocator,
                                   const ObSQLSessionInfo &session,
                                   ObString &outline_sql) const
{
  int ret = OB_SUCCESS;
  bool is_need_filter_hint = true;
  if (OB_FAIL(ObSQLUtils::construct_outline_sql(allocator,
                                                session,
                                                outline_content_,
                                                sql_text_,
                                                is_need_filter_hint,
                                                outline_sql))) {
    LOG_WARN("fail to construct outline sql", K(ret), K(outline_content_), K(sql_text_), K(outline_sql));
  }
  return ret;
}

int ObOutlineInfo::has_concurrent_limit_param(bool &has_limit_param) const
{
  return outline_params_wrapper_.has_concurrent_limit_param(has_limit_param);
}

OB_DEF_SERIALIZE(ObOutlineInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, database_id_, outline_id_, schema_version_,
              name_, signature_, outline_content_, sql_text_, outline_target_, owner_,
              used_, version_, compatible_, enabled_, format_, outline_params_wrapper_,
              sql_id_, owner_id_, format_sql_text_, format_sql_id_, format_outline_);
  return ret;
}


OB_DEF_DESERIALIZE(ObOutlineInfo)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObString signature;
  ObString sql_id;
  ObString outline_content;
  ObString sql_text;
  ObString outline_target;
  ObString owner;
  ObString version;
  ObString format_sql_id;
  ObString format_sql_text;

  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, database_id_, outline_id_, schema_version_,
              name, signature, outline_content, sql_text, outline_target, owner, used_,
              version, compatible_, enabled_, format_);

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(name, name_))) {
    LOG_WARN("Fail to deep copy outline name", K(ret));
  } else if (OB_FAIL(deep_copy_str(signature, signature_))) {
    LOG_WARN("Fail to deep copy signature", K(ret));
  } else if (OB_FAIL(deep_copy_str(outline_content, outline_content_))) {
    LOG_WARN("Fail to deep copy outline_content", K(ret));
  } else if (OB_FAIL(deep_copy_str(sql_text, sql_text_))) {
    LOG_WARN("Fail to deep copy sql_text", K(ret));
  } else if (OB_FAIL(deep_copy_str(outline_target, outline_target_))) {
    LOG_WARN("Fail to deep copy outline target", K(ret));
  } else if (OB_FAIL(deep_copy_str(owner, owner_))) {
    LOG_WARN("Fail to deep copy owner", K(ret));
  } else if (OB_FAIL(deep_copy_str(version, version_))) {
    LOG_WARN("Fail to deep copy owner", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator_))) {
    LOG_WARN("fail to set allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to desrialize OutlineParamsWrapper", K(ret));
  } else {
    if (pos < data_len) {
      if (OB_FAIL(sql_id.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to desrialize sql_id", K(ret));
      } else if (OB_FAIL(deep_copy_str(sql_id, sql_id_))) {
        LOG_WARN("Fail to deep copy sql_id", K(ret));
      } else {
        if (pos < data_len) {
          LST_DO_CODE(OB_UNIS_DECODE, owner_id_, format_sql_text, format_sql_id, format_outline_);
          if (OB_FAIL(ret)){
            // do nothing
          }else if (OB_FAIL(deep_copy_str(format_sql_text, format_sql_text_))) {
            LOG_WARN("Fail to deep copy sql_text", K(ret));
          } else if (OB_FAIL(deep_copy_str(format_sql_id, format_sql_id_))) {
            LOG_WARN("Fail to deep copy sql_id", K(ret));
          }
        } else {
          owner_id_ = OB_INVALID_ID;
        }
      }
    }
  }
  return ret;
}


OB_DEF_SERIALIZE_SIZE(ObOutlineInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, database_id_, outline_id_, schema_version_,
              name_, signature_, sql_id_, outline_content_, sql_text_, outline_target_, owner_,
              used_, version_, compatible_, enabled_, format_, outline_params_wrapper_, owner_id_,
              format_sql_text_, format_sql_id_, format_outline_);
  return len;
}


OB_SERIALIZE_MEMBER(ObTenantSynonymId, tenant_id_, synonym_id_);
OB_SERIALIZE_MEMBER(ObTenantSequenceId, tenant_id_, sequence_id_);
OB_SERIALIZE_MEMBER((ObAlterOutlineInfo, ObOutlineInfo), alter_option_bitset_);
OB_SERIALIZE_MEMBER(ObTenantLabelSePolicyId, tenant_id_, label_se_policy_id_);
OB_SERIALIZE_MEMBER(ObTenantLabelSeComponentId, tenant_id_, label_se_component_id_);
OB_SERIALIZE_MEMBER(ObTenantLabelSeLabelId, tenant_id_, label_se_label_id_);
OB_SERIALIZE_MEMBER(ObTenantLabelSeUserLevelId, tenant_id_, label_se_user_level_id_);
OB_SERIALIZE_MEMBER(ObTenantCommonSchemaId, tenant_id_, schema_id_);
OB_SERIALIZE_MEMBER((ObTenantProfileId, ObTenantCommonSchemaId));

void ObDbLinkBaseInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  owner_id_ = OB_INVALID_ID;
  dblink_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  dblink_name_.reset();
  cluster_name_.reset();
  tenant_name_.reset();
  user_name_.reset();
  password_.reset();
  encrypted_password_.reset();
  plain_password_.reset();
  host_addr_.reset();
  driver_proto_ = 0;
  if_not_exist_ = false;
  flag_ = 0;
  service_name_.reset();
  conn_string_.reset();
  authusr_.reset();
  authpwd_.reset();
  passwordx_.reset();
  authpwdx_.reset();
  host_name_.reset();
  host_port_ = 0;
  reverse_host_name_.reset();
  reverse_host_port_ = 0;
  reverse_cluster_name_.reset();
  reverse_tenant_name_.reset();
  reverse_user_name_.reset();
  reverse_password_.reset();
  plain_reverse_password_.reset();
  reverse_host_addr_.reset();
  database_name_.reset();
}

bool ObDbLinkBaseInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && OB_INVALID_ID != owner_id_
      && OB_INVALID_ID != dblink_id_
      && !dblink_name_.empty()
      && !tenant_name_.empty()
      && !user_name_.empty()
      && ((!host_name_.empty() && 0 != host_port_) || host_addr_.is_valid())
      && (!password_.empty() || !encrypted_password_.empty());
}

int ObDbLinkBaseInfo::do_decrypt_password()
{
  int ret = OB_SUCCESS;
  if (!plain_password_.empty()) {
    // do_nothing
  } else {
    if (!password_.empty()) {
      if (OB_FAIL(deep_copy_str(password_, plain_password_))) {
        LOG_WARN("failed to deep copy password_ to plain_password_", K(ret));
      }
    } else if (encrypted_password_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypted_password_ and password_ is empty", K(ret));
    } else if (OB_FAIL(ObDbLinkBaseInfo::dblink_decrypt(encrypted_password_, plain_password_))) {
      LOG_WARN("failed to decrypt encrypted_password_", K(ret));
    } else if (plain_password_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plain_password_ is empty", K(ret));
    }
  }
  return ret;
}

int ObDbLinkBaseInfo::do_decrypt_reverse_password()
{
  int ret = OB_SUCCESS;
  if (reverse_password_.empty()) {
    // do nothing
  } else if (OB_FAIL(ObDbLinkBaseInfo::dblink_decrypt(reverse_password_, plain_reverse_password_))) {
    LOG_WARN("failed to decrypt reverse_password_", K(ret));
  } else if (plain_reverse_password_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plain_password_ is empty", K(ret));
  }
  return ret;
}

int ObDbLinkBaseInfo::do_encrypt_password()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDbLinkBaseInfo::dblink_encrypt(plain_password_, encrypted_password_))) {
    LOG_WARN("failed to encrypt plain_password_", K(ret));
  }
  return ret;
}

int ObDbLinkBaseInfo::do_encrypt_reverse_password()
{
  int ret = OB_SUCCESS;
  if (plain_reverse_password_.empty()) {
    // do nothing
  } else if (OB_FAIL(ObDbLinkBaseInfo::dblink_encrypt(plain_reverse_password_, reverse_password_))) {
    LOG_WARN("failed to encrypt plain_password_", K(ret));
  }
  return ret;
}

int ObDbLinkBaseInfo::dblink_encrypt(common::ObString &src, common::ObString &dst)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (src.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src is empty", K(ret));
  } else {
    char encrypted_string[common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH] = {0};

    char hex_buff[common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH + 1] = {0}; // +1 to reserve space for \0
    int64_t encrypt_len = -1;
    if (OB_FAIL(oceanbase::share::ObEncryptionUtil::encrypt_sys_data(tenant_id_,
                                                   src.ptr(),
                                                   src.length(),
                                                   encrypted_string,
                                                   common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH,
                                                   encrypt_len))) {

      LOG_WARN("fail to encrypt_sys_data", KR(ret), K(src));
    } else if (0 >= encrypt_len || common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH < encrypt_len * 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypt_len is invalid", K(ret), K(encrypt_len), K(common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH));
    } else if (OB_FAIL(to_hex_cstr(encrypted_string, encrypt_len, hex_buff, common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH + 1))) {
      LOG_WARN("fail to print to hex str", K(ret));
    } else if (OB_FAIL(deep_copy_str(ObString(hex_buff), dst))) {
      LOG_WARN("failed to deep copy encrypted_string", K(ret));
    } else {
      LOG_TRACE("succ to encrypt src", K(src), K(dst));
    }
  }
#else
  if (src.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src is empty", K(ret));
  } else {
    dst = src;
  }
#endif
  return ret;
}
int ObDbLinkBaseInfo::dblink_decrypt(common::ObString &src, common::ObString &dst)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_DBLINK
  if (src.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src is empty", K(ret));
  } else if (0 != src.length() % 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src", K(src.length()), K(ret));
  } else {
    char encrypted_password_not_hex[common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH] = {0};
    char plain_string[common::OB_MAX_PASSWORD_LENGTH + 1] = { 0 }; // need +1 to reserve space for \0
    int64_t plain_string_len = -1;
    if (OB_FAIL(hex_to_cstr(src.ptr(),
                            src.length(),
                            encrypted_password_not_hex,
                            common::OB_MAX_ENCRYPTED_PASSWORD_LENGTH))) {
      LOG_WARN("failed to hex to cstr", K(src.length()), K(ret));
    } else if (OB_FAIL(ObEncryptionUtil::decrypt_sys_data(tenant_id_,
                                                          encrypted_password_not_hex,

                                                          src.length() / 2,
                                                          plain_string,
                                                          common::OB_MAX_PASSWORD_LENGTH + 1,
                                                          plain_string_len))) {
      LOG_WARN("failed to decrypt_sys_data", K(ret), K(src.length()));
    } else if (0 >= plain_string_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decrypt dblink password failed", K(ret), K(plain_string_len));
    } else if (OB_FAIL(deep_copy_str(ObString(plain_string_len, plain_string), dst))) {
      LOG_WARN("failed to deep copy plain_string", K(ret));
    } else {
      LOG_TRACE("succ to decrypt src", K(plain_string_len), K(src), K(dst));
    }
  }
#else
  if (src.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src is empty", K(ret));
  } else {
    dst = src;
  }

#endif
  return ret;
}

int ObDbLinkBaseInfo::set_host_ip(const common::ObString &host_ip)
{
  bool bret = host_addr_.set_ip_addr(host_ip, 0);
  return bret ? common::OB_SUCCESS : common::OB_ERR_UNEXPECTED;
}

OB_SERIALIZE_MEMBER(ObDbLinkInfo,
                    tenant_id_,
                    owner_id_,
                    dblink_id_,
                    schema_version_,
                    dblink_name_,
                    cluster_name_,
                    tenant_name_,
                    user_name_,
                    password_,
                    host_addr_,
                    driver_proto_,
                    flag_,
                    service_name_,
                    conn_string_,
                    authusr_,
                    authpwd_,
                    passwordx_,
                    authpwdx_,
                    plain_password_,
                    encrypted_password_,
                    reverse_cluster_name_,
                    reverse_tenant_name_,
                    reverse_user_name_,
                    reverse_password_,
                    plain_reverse_password_,
                    reverse_host_addr_,
                    database_name_,
                    if_not_exist_,
                    host_name_,
                    host_port_,
                    reverse_host_name_,
                    reverse_host_port_);

ObDbLinkSchema::ObDbLinkSchema(const ObDbLinkSchema &other)
  : ObDbLinkBaseInfo()
{
  reset();
  *this = other;
}

ObDbLinkSchema &ObDbLinkSchema::operator=(const ObDbLinkSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    owner_id_ = other.owner_id_;
    dblink_id_ = other.dblink_id_;
    schema_version_ = other.schema_version_;
    driver_proto_ = other.driver_proto_;
    if_not_exist_ = other.if_not_exist_;
    flag_ = other.flag_;
    host_port_ = other.host_port_;
    reverse_host_port_ = other.reverse_host_port_;
    if (OB_FAIL(deep_copy_str(other.dblink_name_, dblink_name_))) {
      LOG_WARN("Fail to deep copy dblink name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.cluster_name_, cluster_name_))) {
      LOG_WARN("Fail to deep copy cluster name", K(ret), K(other.cluster_name_));
    } else if (OB_FAIL(deep_copy_str(other.tenant_name_, tenant_name_))) {
      LOG_WARN("Fail to deep copy tenant name", K(ret), K(other.tenant_name_));
    } else if (OB_FAIL(deep_copy_str(other.user_name_, user_name_))) {
      LOG_WARN("Fail to deep copy user name", K(ret), K(other.user_name_));
    } else if (OB_FAIL(deep_copy_str(other.password_, password_))) {
      LOG_WARN("Fail to deep copy password", K(ret), K(other.password_));
    } else if (OB_FAIL(deep_copy_str(other.encrypted_password_, encrypted_password_))) {
      LOG_WARN("Fail to deep copy encrypted_password", K(ret), K(other.encrypted_password_));
    } else if (OB_FAIL(deep_copy_str(other.plain_password_, plain_password_))) {
      LOG_WARN("Fail to deep copy plain_password", K(ret), K(other.plain_password_));
    } else if (OB_FAIL(deep_copy_str(other.service_name_, service_name_))) {
      LOG_WARN("Fail to deep copy service_name", K(ret), K(other.service_name_));
    } else if (OB_FAIL(deep_copy_str(other.conn_string_, conn_string_))) {
      LOG_WARN("Fail to deep copy conn_string", K(ret), K(other.conn_string_));
    } else if (OB_FAIL(deep_copy_str(other.authusr_, authusr_))) {
      LOG_WARN("Fail to deep copy authusr", K(ret), K(other.authusr_));
    } else if (OB_FAIL(deep_copy_str(other.authpwd_, authpwd_))) {
      LOG_WARN("Fail to deep copy authpwd", K(ret), K(other.authpwd_));
    } else if (OB_FAIL(deep_copy_str(other.passwordx_, passwordx_))) {
      LOG_WARN("Fail to deep copy passwordx", K(ret), K(other.passwordx_));
    } else if (OB_FAIL(deep_copy_str(other.authpwdx_, authpwdx_))) {
      LOG_WARN("Fail to deep copy authpwdx", K(ret), K(other.authpwdx_));
    } else if (OB_FAIL(deep_copy_str(other.reverse_cluster_name_, reverse_cluster_name_))) {
      LOG_WARN("Fail to deep copy reverse_cluster_name", K(ret), K(other.reverse_cluster_name_));
    } else if (OB_FAIL(deep_copy_str(other.reverse_tenant_name_, reverse_tenant_name_))) {
      LOG_WARN("Fail to deep copy reverse_tenant_name", K(ret), K(other.reverse_tenant_name_));
    } else if (OB_FAIL(deep_copy_str(other.reverse_user_name_, reverse_user_name_))) {
      LOG_WARN("Fail to deep copy reverse_user_name", K(ret), K(other.reverse_user_name_));
    } else if (OB_FAIL(deep_copy_str(other.reverse_password_, reverse_password_))) {
      LOG_WARN("Fail to deep copy reverse_password", K(ret), K(other.reverse_password_));
    } else if (OB_FAIL(deep_copy_str(other.plain_reverse_password_, plain_reverse_password_))) {
      LOG_WARN("Fail to deep copy plain_reverse_password", K(ret), K(other.plain_reverse_password_));
    } else if (OB_FAIL(deep_copy_str(other.database_name_, database_name_))) {
      LOG_WARN("Fail to deep copy database_name", K(ret), K(other.database_name_));
    } else if (OB_FAIL(deep_copy_str(other.host_name_, host_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret), K(other.host_name_));
    } else if (OB_FAIL(deep_copy_str(other.reverse_host_name_, reverse_host_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret), K(other.reverse_host_name_));
    }
    host_addr_ = other.host_addr_;
    reverse_host_addr_ = other.reverse_host_addr_;
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObDbLinkSchema::operator==(const ObDbLinkSchema &other) const
{
  return (tenant_id_ == other.tenant_id_
       && owner_id_ == other.owner_id_
       && dblink_id_ == other.dblink_id_
       && schema_version_ == other.schema_version_
       && dblink_name_ == other.dblink_name_
       && cluster_name_ == other.cluster_name_
       && tenant_name_ == other.tenant_name_
       && user_name_ == other.user_name_
       && password_ == other.password_
       && encrypted_password_ == other.encrypted_password_
       && plain_password_ == other.plain_password_
       && host_addr_ == other.host_addr_
       && driver_proto_ == other.driver_proto_
       && flag_ == other.flag_
       && service_name_ == other.service_name_
       && conn_string_ == other.conn_string_
       && authusr_ == other.authusr_
       && authpwd_ == other.authpwd_
       && passwordx_ == other.passwordx_
       && authpwdx_ == other.authpwdx_
       && reverse_cluster_name_ == other.reverse_cluster_name_
       && reverse_tenant_name_ == other.reverse_tenant_name_
       && reverse_user_name_ == other.reverse_user_name_
       && reverse_password_ == other.reverse_password_
       && plain_reverse_password_ == other.plain_reverse_password_
       && reverse_host_addr_ == other.reverse_host_addr_
       && database_name_ == other.database_name_
       && if_not_exist_ == other.if_not_exist_
       && host_name_ == other.host_name_
       && host_port_ == other.host_port_
       && reverse_host_name_ == other.host_name_
       && reverse_host_port_ == other.host_port_);
}

ObSynonymInfo::ObSynonymInfo(common::ObIAllocator *allocator):
    ObSchema(allocator)
    {
      reset();
    }
ObSynonymInfo::ObSynonymInfo(): ObSchema(){reset();}

ObSynonymInfo::~ObSynonymInfo() {}

ObSynonymInfo::ObSynonymInfo(const ObSynonymInfo &src_info) : ObSchema()
{
  reset();
  *this = src_info;
}

ObSynonymInfo &ObSynonymInfo::operator=(const ObSynonymInfo &src_info)
{
  if (this != &src_info) {
    reset();
    int ret = OB_SUCCESS;
    tenant_id_ = src_info.tenant_id_;
    database_id_ = src_info.database_id_;
    synonym_id_ = src_info.synonym_id_;
    schema_version_ = src_info.schema_version_;
    object_db_id_ = src_info.object_db_id_;
    status_ = src_info.status_;
    if (OB_FAIL(deep_copy_str(src_info.name_, name_))) {
      LOG_WARN("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.object_name_, object_name_))) {
      LOG_WARN("Fail to deep object name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.version_, version_))) {
      LOG_WARN("Fail to deep copy version", K(ret));
    } else {/*do nothing*/}
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObSynonymInfo::assign(const ObSynonymInfo &src_info)
{
  int ret = OB_SUCCESS;
  *this = src_info;
  if (OB_UNLIKELY(OB_SUCCESS != error_ret_)) {
    ret = error_ret_;
    LOG_WARN("failed to assign synonym info", K(ret));
  }
  return ret;
}

/*
void *ObSynonymInfo::alloc(int64_t size)
{
  return allocator_.alloc(size);
}
*/

int64_t ObSynonymInfo::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObSynonymInfo);
  convert_size += name_.length() + 1;
  convert_size += version_.length() + 1;
  convert_size += object_name_.length() + 1;
  return convert_size;
}

void ObSynonymInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  synonym_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  reset_string(name_);
  reset_string(version_);
  reset_string(object_name_);
  object_db_id_ = 0;
  status_ = ObObjectStatus::VALID;
  ObSchema::reset();
  //allocator_.reset();
}

OB_DEF_SERIALIZE(ObSynonymInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, database_id_, synonym_id_, schema_version_,
              name_, version_, object_name_, object_db_id_, status_);
  return ret;
}


OB_DEF_DESERIALIZE(ObSynonymInfo)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObString object_name;
  ObString version;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, database_id_, synonym_id_, schema_version_,
              name, version, object_name, object_db_id_, status_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(name, name_))) {
    LOG_WARN("Fail to deep copy outline name", K(ret));
  } else if (OB_FAIL(deep_copy_str(object_name, object_name_))) {
    LOG_WARN("Fail to deep copy signature", K(ret));
  } else if (OB_FAIL(deep_copy_str(version, version_))) {
    LOG_WARN("Fail to deep copy outline_content", K(ret));
  } else {/*do nothing*/}
  return ret;
}


OB_DEF_SERIALIZE_SIZE(ObSynonymInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, database_id_, synonym_id_, schema_version_,
      name_, version_, object_name_, object_db_id_, status_);
  return len;
}

ObRecycleObject::ObRecycleObject(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}


ObRecycleObject::ObRecycleObject(const ObRecycleObject &src)
  : ObSchema()
{
  reset();
  *this = src;
}

void ObRecycleObject::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  reset_string(object_name_);
  reset_string(original_name_);
  reset_string(tablegroup_name_);
  reset_string(database_name_);
  type_ = INVALID;
  ObSchema::reset();
}

ObRecycleObject &ObRecycleObject::operator=(const ObRecycleObject &src)
{
  if (this != &src) {
    int ret = OB_SUCCESS;
    reset();
    error_ret_ = src.error_ret_;
    set_tenant_id(src.get_tenant_id());
    set_database_id(src.get_database_id());
    set_table_id(src.get_table_id());
    set_tablegroup_id(src.get_tablegroup_id());
    set_type(src.get_type());
    if (OB_FAIL(set_object_name(src.get_object_name()))) {
      LOG_WARN("set_object_name failed", K(ret));
    } else if (OB_FAIL(set_original_name(src.get_original_name()))) {
      LOG_WARN("set_original_name failed", K(ret));
    } else if (OB_FAIL(set_tablegroup_name(src.get_tablegroup_name()))) {
      LOG_WARN("set tabelgroup name failed", K(ret));
    } else if (OB_FAIL(set_database_name(src.get_database_name()))) {
      LOG_WARN("set database name failed", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

ObRecycleObject::RecycleObjType ObRecycleObject::get_type_by_table_schema(
    const ObSimpleTableSchemaV2 &table_schema)
{
  ObRecycleObject::RecycleObjType type = INVALID;
  if (table_schema.is_index_table()) {
    type = INDEX;
  } else if (table_schema.is_view_table()) {
    type = VIEW;
  } else if (table_schema.is_table() || table_schema.is_tmp_table() || table_schema.is_external_table()) {
    type = TABLE;
  } else if (table_schema.is_aux_vp_table()) {
    type = AUX_VP;
  } else if (table_schema.is_aux_lob_meta_table()) {
    type = AUX_LOB_META;
  } else if (table_schema.is_aux_lob_piece_table()) {
    type = AUX_LOB_PIECE;
  } else {
    type = INVALID;
  }
  return type;
}

int ObRecycleObject::set_type_by_table_schema(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = common::OB_SUCCESS;
  ObRecycleObject::RecycleObjType type = get_type_by_table_schema(table_schema);
  if (INVALID == type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema type is not correct", K(ret), K(table_schema));
  } else {
    type_ = type;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRecycleObject, tenant_id_, database_id_, table_id_,
    tablegroup_id_, object_name_, original_name_, type_, tablegroup_name_, database_name_);

//------end of funcs of outlineinfo-----//
// ObKeystoreSchema
OB_SERIALIZE_MEMBER(ObKeystoreSchema,
                    tenant_id_,
                    keystore_id_,
                    schema_version_,
                    keystore_name_,
                    password_,
                    status_,
                    master_key_id_,
                    master_key_,
                    encrypted_key_);
ObKeystoreSchema::ObKeystoreSchema()
  : ObSchema()
{
  reset();
}
ObKeystoreSchema::ObKeystoreSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}
ObKeystoreSchema::ObKeystoreSchema(const ObKeystoreSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}
ObKeystoreSchema::~ObKeystoreSchema()
{
}
ObKeystoreSchema& ObKeystoreSchema::operator =(const ObKeystoreSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_keystore_id(src_schema.keystore_id_);
    set_schema_version(src_schema.schema_version_);
    set_status(src_schema.status_);
    set_master_key_id(src_schema.master_key_id_);
    if (OB_FAIL(set_keystore_name(src_schema.keystore_name_))) {
      LOG_WARN("fail set keystore name", K(src_schema));
    } else if (OB_FAIL(set_password(src_schema.password_))) {
      LOG_WARN("fail set password", K(src_schema));
    } else if (OB_FAIL(set_master_key(src_schema.master_key_))) {
      LOG_WARN("fail set master key", K(src_schema));
    } else if (OB_FAIL(set_encrypted_key(src_schema.encrypted_key_))) {
      LOG_WARN("fail set encrypted key", K(src_schema));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}
void ObKeystoreSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  tenant_id_ = OB_INVALID_TENANT_ID;
  keystore_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  status_ = 0;
  master_key_id_ = 0;
  reset_string(keystore_name_);
  reset_string(password_);
  reset_string(master_key_);
  reset_string(encrypted_key_);
}
int64_t ObKeystoreSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObKeystoreSchema);
  convert_size += keystore_name_.length() + 1;
  convert_size += password_.length() + 1;
  convert_size += master_key_.length() + 1;
  convert_size += encrypted_key_.length() + 1;
  return convert_size;
}


OB_SERIALIZE_MEMBER(ObSequenceSchema,
                    tenant_id_,
                    database_id_,
                    sequence_id_,
                    schema_version_,
                    name_,
                    option_,
                    is_system_generated_,
                    dblink_id_);

ObSequenceSchema::ObSequenceSchema()
  : ObSchema()
{
  reset();
}

ObSequenceSchema::ObSequenceSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSequenceSchema::ObSequenceSchema(const ObSequenceSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObSequenceSchema::~ObSequenceSchema()
{
}

int ObSequenceSchema::assign(const ObSequenceSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_database_id(src_schema.database_id_);
    set_sequence_id(src_schema.sequence_id_);
    set_schema_version(src_schema.schema_version_);
    set_is_system_generated(src_schema.is_system_generated_);
    set_dblink_id(src_schema.get_dblink_id());
    if (OB_FAIL(option_.assign(src_schema.option_))) {
      LOG_WARN("fail assign option", K(src_schema));
    } else if (OB_FAIL(set_sequence_name(src_schema.name_))) {
      LOG_WARN("fail set seq name", K(src_schema));
    }
  }
  return ret;
}

ObSequenceSchema& ObSequenceSchema::operator =(const ObSequenceSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(src_schema))) {
    error_ret_ = ret;
  }
  return *this;
}

void ObSequenceSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  sequence_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  is_system_generated_ = false;
  reset_string(name_);
  option_.reset();
  dblink_id_ = OB_INVALID_ID;
}

int64_t ObSequenceSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObSequenceSchema);
  convert_size += name_.length() + 1;
  return convert_size;
}

// int ObSequenceSchema::get_value(const common::ObString &str, int64_t &val)
// {
//   int ret = OB_SUCCESS;
//   char buf[number::ObNumber::MAX_BYTE_LEN];
//   ObDataBuffer allocator(buf, number::ObNumber::MAX_BYTE_LEN);
//   number::ObNumber num;
//   if (OB_FAIL(num.from(str.ptr(), str.length(), allocator))) {
//     LOG_WARN("fail convert string to number", K(str), K(ret));
//   } else if (!num.is_valid_int64(val)) {
//     ret = OB_NOT_IMPLEMENT;
//     LOG_WARN("support only in range int64 now", K(ret));
//   }
//   return ret;
// }

// int ObSequenceSchema::set_max_value(const common::ObString &str)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(str, val))) {
//     set_max_value(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_min_value(const common::ObString &str)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(str, val))) {
//     set_min_value(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_increment_by(const common::ObString &str)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(str, val))) {
//     set_increment_by(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_start_with(const common::ObString &str)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(str, val))) {
//     set_start_with(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_cache_size(const common::ObString &str)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(str, val))) {
//     set_cache_size(val);
//   }
//   return ret;
// }
//
//
// int ObSequenceSchema::get_value(const common::number::ObNumber &num, int64_t &val)
// {
//   int ret = OB_SUCCESS;
//   if (!num.is_valid_int64(val)) {
//     ret = OB_NOT_IMPLEMENT;
//     LOG_WARN("support only in range int64 now", K(ret));
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_max_value(const common::number::ObNumber &num)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(num, val))) {
//     set_max_value(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_min_value(const common::number::ObNumber &num)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(num, val))) {
//     set_min_value(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_increment_by(const common::number::ObNumber &num)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(num, val))) {
//     set_increment_by(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_start_with(const common::number::ObNumber &num)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(num, val))) {
//     set_start_with(val);
//   }
//   return ret;
// }
//
// int ObSequenceSchema::set_cache_size(const common::number::ObNumber &num)
// {
//   int ret = OB_SUCCESS;
//   int64_t val = 0;
//   if (OB_SUCC(get_value(num, val))) {
//     set_cache_size(val);
//   }
//   return ret;
// }

OB_SERIALIZE_MEMBER(ObTenantAuditKey,
                    tenant_id_,
                    audit_id_);

OB_SERIALIZE_MEMBER(ObSAuditSchema,
                    schema_version_,
                    audit_key_,
                    audit_type_,
                    owner_id_,
                    operation_type_,
                    in_success_,
                    in_failure_);

OB_SERIALIZE_MEMBER(ObAuxTableMetaInfo,
                    table_id_,
                    table_type_,
                    index_type_);

ObForeignKeyInfo::ObForeignKeyInfo(ObIAllocator *allocator)
  : table_id_(common::OB_INVALID_ID),
    foreign_key_id_(common::OB_INVALID_ID),
    child_table_id_(common::OB_INVALID_ID),
    parent_table_id_(common::OB_INVALID_ID),
    child_column_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    parent_column_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    update_action_(ACTION_INVALID),
    delete_action_(ACTION_INVALID),
    foreign_key_name_(),
    enable_flag_(true),
    is_modify_enable_flag_(false),
    validate_flag_(CST_FK_VALIDATED),
    is_modify_validate_flag_(false),
    rely_flag_(false),
    is_modify_rely_flag_(false),
    is_modify_fk_state_(false),
    ref_cst_type_(CONSTRAINT_TYPE_INVALID),
    ref_cst_id_(common::OB_INVALID_ID),
    is_modify_fk_name_flag_(false),
    is_parent_table_mock_(false),
    name_generated_type_(GENERATED_TYPE_UNKNOWN)
{}

int ObForeignKeyInfo::assign(const ObForeignKeyInfo &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(child_column_ids_.assign(other.child_column_ids_))) {
    LOG_WARN("fail to assign array", K(ret));
  } else if (OB_FAIL(parent_column_ids_.assign(other.parent_column_ids_))) {
    LOG_WARN("fail to assign array", K(ret));
  } else {
    table_id_ = other.table_id_;
    foreign_key_id_ = other.foreign_key_id_;
    child_table_id_ = other.child_table_id_;
    parent_table_id_ = other.parent_table_id_;
    update_action_ = other.update_action_;
    delete_action_ = other.delete_action_;
    enable_flag_ = other.enable_flag_;
    is_modify_enable_flag_ = other.is_modify_enable_flag_;
    validate_flag_ = other.validate_flag_;
    is_modify_validate_flag_ = other.is_modify_validate_flag_;
    rely_flag_ = other.rely_flag_;
    is_modify_rely_flag_ = other.is_modify_rely_flag_;
    is_modify_fk_state_ = other.is_modify_fk_state_;
    ref_cst_type_ = other.ref_cst_type_;
    ref_cst_id_ = other.ref_cst_id_;
    foreign_key_name_ = other.foreign_key_name_; // Shallow copy
    is_modify_fk_name_flag_ = other.is_modify_fk_name_flag_;
    is_parent_table_mock_ = other.is_parent_table_mock_;
    name_generated_type_ = other.name_generated_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBasedSchemaObjectInfo,
                    schema_id_,
                    schema_type_,
                    schema_version_,
                    schema_tenant_id_);

const char *ObForeignKeyInfo::reference_action_str_[ACTION_MAX + 1] =
{
  "",
  "RESTRICT",
  "CASCADE",
  "SET NULL",
  "NO ACTION",
  "SET DEFAULT",
  "ACTION_CHECK_EXIST",
  ""
};

bool ObForeignKeyInfo::is_sys_generated_name(bool check_unknown) const
{
  bool bret = false;
  if (GENERATED_TYPE_SYSTEM == name_generated_type_) {
    bret = true;
  } else if (GENERATED_TYPE_UNKNOWN == name_generated_type_ && check_unknown) {
    const char *cst_type_name = "_OBFK_";
    const int64_t cst_type_name_len = static_cast<int64_t>(strlen(cst_type_name));
    bret = (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                  foreign_key_name_.ptr(), foreign_key_name_.length(), cst_type_name, cst_type_name_len));
  } else {
    bret = false;
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObForeignKeyInfo,
                    table_id_,
                    foreign_key_id_,
                    child_table_id_,
                    parent_table_id_,
                    child_column_ids_,
                    parent_column_ids_,
                    update_action_,
                    delete_action_,
                    foreign_key_name_,
                    ref_cst_type_,
                    ref_cst_id_,
                    is_modify_fk_name_flag_,
                    is_parent_table_mock_,
                    name_generated_type_);

OB_SERIALIZE_MEMBER(ObSimpleForeignKeyInfo,
                    tenant_id_,
                    database_id_,
                    table_id_,
                    foreign_key_name_,
                    foreign_key_id_);

OB_SERIALIZE_MEMBER(ObSimpleConstraintInfo,
                    tenant_id_,
                    database_id_,
                    table_id_,
                    constraint_name_,
                    constraint_id_);

int ObCompareNameWithTenantID::compare(const common::ObString &str1, const common::ObString &str2)
{
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  if (tenant_id_ != OB_INVALID_ID &&
      database_id_ != OB_INVALID_ID &&
      is_mysql_sys_database_id(database_id_)) {
    // If it is the oceanbase database, no matter what the tenant, I hope that it is not case sensitive
    cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (lib::is_oracle_mode()) {
    cs_type = common::CS_TYPE_UTF8MB4_BIN;
  } else if (tenant_id_ == OB_INVALID_ID) {
    // Used for scenarios that do not require the tenant id to be case sensitive, such as column, only rely on is_oracle_mode()
    /* ^-^ */
  } else {
    if (name_case_mode_ != OB_NAME_CASE_INVALID) {
      cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    }
    (void) ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode);
    if (compat_mode == lib::Worker::CompatMode::ORACLE) {
      cs_type = common::CS_TYPE_UTF8MB4_BIN;
    }
  }
  return common::ObCharset::strcmp(cs_type, str1, str2);
}

ObIAllocator *&schema_stack_allocator()
{
  RLOCAL(ObIAllocator *, allocator_);
  return allocator_;
}

common::ObString get_ssl_type_string(const ObSSLType ssl_type)
{
  static common::ObString const_strings[] = {
      common::ObString::make_string("NOT_SPECIFIED"),
      common::ObString::make_string(""),
      common::ObString::make_string("ANY"),
      common::ObString::make_string("X509"),
      common::ObString::make_string("SPECIFIED"),
      common::ObString::make_string("MAX_TYPE"),
  };
  return ((ssl_type >= ObSSLType::SSL_TYPE_NOT_SPECIFIED && ssl_type < ObSSLType::SSL_TYPE_MAX)
          ? const_strings[static_cast<int32_t>(ssl_type)]
          : const_strings[static_cast<int32_t>(ObSSLType::SSL_TYPE_MAX)]);
}

const char *get_ssl_spec_type_str(const ObSSLSpecifiedType ssl_spec_type)
{
  static const char * const const_str[] = {
      "CIPHER",
      "ISSUER",
      "SUBJECT",
      "MAX_TYPE",
  };
  return ((ssl_spec_type >= ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER && ssl_spec_type <= ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)
          ? const_str[static_cast<int32_t>(ssl_spec_type)]
          : const_str[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)]);
}

ObSSLType get_ssl_type_from_string(const common::ObString &ssl_type_str)
{
  ObSSLType ssl_type_enum = ObSSLType::SSL_TYPE_MAX;
  if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_NOT_SPECIFIED)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_NONE)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_NONE;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_ANY)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_ANY;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_X509) ) {
    ssl_type_enum = ObSSLType::SSL_TYPE_X509;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_SPECIFIED)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_SPECIFIED;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unknown ssl type", K(ssl_type_str), K(common::lbt()));
  }
  return ssl_type_enum;
}

ObLabelSePolicySchema::ObLabelSePolicySchema()
  : ObSchema()
{
  reset();
}
ObLabelSePolicySchema::ObLabelSePolicySchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}
ObLabelSePolicySchema::ObLabelSePolicySchema(const ObLabelSePolicySchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}
ObLabelSePolicySchema::~ObLabelSePolicySchema()
{
}
ObLabelSePolicySchema &ObLabelSePolicySchema::operator =(const ObLabelSePolicySchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    label_se_policy_id_ = other.label_se_policy_id_;
    schema_version_ = other.schema_version_;
    if (OB_FAIL(deep_copy_str(other.policy_name_, policy_name_))) {
      LOG_WARN("Fail to deep copy user_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.column_name_, column_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret));
    } else {
      default_options_ = other.default_options_;
      flag_ = other.flag_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}
bool ObLabelSePolicySchema::operator == (const ObLabelSePolicySchema &other) const
{
  bool ret = false;
  if (tenant_id_ == other.tenant_id_
      && label_se_policy_id_ == other.label_se_policy_id_
      && schema_version_ == other.schema_version_
      && policy_name_ == other.policy_name_
      && column_name_ == other.column_name_
      && default_options_ == other.default_options_
      && flag_ == other.flag_) {
    ret = true;
  }
  return ret;
}
void ObLabelSePolicySchema::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  label_se_policy_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  policy_name_.reset();
  column_name_.reset();
  default_options_ = static_cast<int64_t>(PolicyOptions::INVALID_OPTION);
  flag_ = static_cast<int64_t>(PolicyStatus::ERROR);
  ObSchema::reset();
}
bool ObLabelSePolicySchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_INVALID_ID == label_se_policy_id_
      || schema_version_ < 0
      || policy_name_.empty()
      || column_name_.empty()
      || default_options_ < 0
      || flag_ < 0) {
    ret = false;
  }
  return ret;
}
int64_t ObLabelSePolicySchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObLabelSePolicySchema);
  convert_size += policy_name_.length() + column_name_.length() + 2;
  return convert_size;
}
OB_SERIALIZE_MEMBER(ObLabelSePolicySchema,
                    tenant_id_,
                    label_se_policy_id_,
                    schema_version_,
                    policy_name_,
                    column_name_,
                    default_options_,
                    flag_);
ObLabelSeComponentSchema::ObLabelSeComponentSchema()
  : ObSchema()
{
  reset();
}
ObLabelSeComponentSchema::ObLabelSeComponentSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}
ObLabelSeComponentSchema::ObLabelSeComponentSchema(const ObLabelSeComponentSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}
ObLabelSeComponentSchema::~ObLabelSeComponentSchema()
{
}
ObLabelSeComponentSchema &ObLabelSeComponentSchema::operator =(const ObLabelSeComponentSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    label_se_policy_id_ = other.label_se_policy_id_;
    label_se_component_id_ = other.label_se_component_id_;
    comp_type_ = other.comp_type_;
    schema_version_ = other.schema_version_;
    comp_num_ = other.comp_num_;
    if (OB_FAIL(deep_copy_str(other.short_name_, short_name_))) {
      LOG_WARN("Fail to deep copy user_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.long_name_, long_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.parent_name_, parent_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}
bool ObLabelSeComponentSchema::operator == (const ObLabelSeComponentSchema &other) const
{
  bool ret = false;
  if (tenant_id_ == other.tenant_id_
      && label_se_policy_id_ == other.label_se_policy_id_
      && label_se_component_id_ == other.label_se_component_id_
      && comp_type_ == other.comp_type_
      && schema_version_ == other.schema_version_
      && comp_num_ == other.comp_num_
      && long_name_ == other.long_name_
      && short_name_ == other.short_name_
      && parent_name_ == other.parent_name_) {
    ret = true;
  }
  return ret;
}
void ObLabelSeComponentSchema::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  label_se_policy_id_ = OB_INVALID_ID;
  label_se_component_id_ = OB_INVALID_ID;
  comp_type_ = static_cast<int64_t>(CompType::INVALID);
  schema_version_ = OB_INVALID_VERSION;
  comp_num_ = 0;
  short_name_.reset();
  long_name_.reset();
  parent_name_.reset();
  ObSchema::reset();
}
bool ObLabelSeComponentSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_INVALID_ID == label_se_policy_id_
      || OB_INVALID_ID == label_se_component_id_
      || comp_type_ == static_cast<int64_t>(CompType::INVALID)
      || schema_version_ < 0
      || !is_valid_comp_num()
      || short_name_.empty()
      || long_name_.empty()) {
    ret = false;
  }
  return ret;
}
int64_t ObLabelSeComponentSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObLabelSeComponentSchema);
  convert_size += long_name_.length() + 1;
  convert_size += short_name_.length() + 1;
  convert_size += parent_name_.length() + 1;
  return convert_size;
}
OB_SERIALIZE_MEMBER(ObLabelSeComponentSchema,
                    tenant_id_,
                    label_se_policy_id_,
                    label_se_component_id_,
                    comp_type_,
                    schema_version_,
                    comp_num_,
                    short_name_,
                    long_name_,
                    parent_name_);
ObLabelSeLabelSchema::ObLabelSeLabelSchema()
  : ObSchema()
{
  reset();
}
ObLabelSeLabelSchema::ObLabelSeLabelSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}
ObLabelSeLabelSchema::ObLabelSeLabelSchema(const ObLabelSeLabelSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}
ObLabelSeLabelSchema::~ObLabelSeLabelSchema()
{
}
ObLabelSeLabelSchema &ObLabelSeLabelSchema::operator =(const ObLabelSeLabelSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    label_se_label_id_ = other.label_se_label_id_;
    schema_version_ = other.schema_version_;
    label_se_policy_id_ = other.label_se_policy_id_;
    label_tag_ = other.label_tag_;
    if (OB_FAIL(deep_copy_str(other.label_, label_))) {
      LOG_WARN("Fail to deep copy user_name", K(ret));
    } else {
      flag_ = other.flag_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}
bool ObLabelSeLabelSchema::operator == (const ObLabelSeLabelSchema &other) const
{
  bool ret = false;
  if (tenant_id_ == other.tenant_id_
      && label_se_label_id_ == other.label_se_label_id_
      && label_se_policy_id_ == other.label_se_policy_id_
      && schema_version_ == other.schema_version_
      && label_tag_ == other.label_tag_
      && label_ == other.label_
      && flag_ == other.flag_) {
    ret = true;
  }
  return ret;
}
void ObLabelSeLabelSchema::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  label_se_label_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  label_se_policy_id_ = OB_INVALID_ID;
  label_tag_ = -1;
  label_.reset();
  flag_ = static_cast<int64_t>(LabelFlag::INVALID);
  ObSchema::reset();
}
bool ObLabelSeLabelSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_INVALID_ID == label_se_label_id_
      || schema_version_ < 0
      || OB_INVALID_ID == label_se_policy_id_
      || label_tag_ < 0
      || label_.empty()
      || flag_ < 0) {
    ret = false;
  }
  return ret;
}
int64_t ObLabelSeLabelSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObLabelSeLabelSchema);
  convert_size += label_.length() + 1;
  return convert_size;
}
OB_SERIALIZE_MEMBER(ObLabelSeLabelSchema,
                    tenant_id_,
                    label_se_label_id_,
                    schema_version_,
                    label_se_policy_id_,
                    label_tag_,
                    label_,
                    flag_);
ObLabelSeUserLevelSchema::ObLabelSeUserLevelSchema()
  : ObSchema()
{
  reset();
}
ObLabelSeUserLevelSchema::ObLabelSeUserLevelSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}
ObLabelSeUserLevelSchema::ObLabelSeUserLevelSchema(const ObLabelSeUserLevelSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}
ObLabelSeUserLevelSchema::~ObLabelSeUserLevelSchema()
{
}
ObLabelSeUserLevelSchema &ObLabelSeUserLevelSchema::operator =(const ObLabelSeUserLevelSchema &other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    label_se_user_level_id_ = other.label_se_user_level_id_;
    schema_version_ = other.schema_version_;
    label_se_policy_id_ = other.label_se_policy_id_;
    maximum_level_ = other.maximum_level_;
    minimum_level_ = other.minimum_level_;
    default_level_ = other.default_level_;
    row_level_ = other.row_level_;
    error_ret_ = other.error_ret_;
  }
  return *this;
}
bool ObLabelSeUserLevelSchema::operator == (const ObLabelSeUserLevelSchema &other) const
{
  bool ret = false;
  if (tenant_id_ == other.tenant_id_
      && user_id_ == other.user_id_
      && label_se_user_level_id_ == other.label_se_user_level_id_
      && label_se_policy_id_ == other.label_se_policy_id_
      && schema_version_ == other.schema_version_
      && maximum_level_ == other.maximum_level_
      && minimum_level_ == other.minimum_level_
      && default_level_ == other.default_level_
      && row_level_ == other.row_level_) {
    ret = true;
  }
  return ret;
}
void ObLabelSeUserLevelSchema::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  user_id_ = OB_INVALID_ID;
  label_se_user_level_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  label_se_policy_id_ = OB_INVALID_ID;
  maximum_level_ = -1;
  minimum_level_ = -1;
  default_level_ = -1;
  row_level_ = -1;
  ObSchema::reset();
}
bool ObLabelSeUserLevelSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || OB_INVALID_ID == user_id_
      || OB_INVALID_ID == label_se_user_level_id_
      || schema_version_ < 0
      || OB_INVALID_ID == label_se_policy_id_
      || maximum_level_ < 0
      || minimum_level_ < 0
      || default_level_ < 0
      || row_level_ < 0) {
    ret = false;
  }
  return ret;
}
int64_t ObLabelSeUserLevelSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObLabelSeUserLevelSchema);
  return convert_size;
}
OB_SERIALIZE_MEMBER(ObLabelSeUserLevelSchema,
                    tenant_id_,
                    user_id_,
                    label_se_user_level_id_,
                    schema_version_,
                    label_se_policy_id_,
                    maximum_level_,
                    minimum_level_,
                    default_level_,
                    row_level_);
// ObTablespaceSchema
OB_SERIALIZE_MEMBER(ObTablespaceSchema,
                    tenant_id_,
                    tablespace_id_,
                    schema_version_,
                    tablespace_name_,
                    encryption_name_,
                    encrypt_key_,
                    master_key_id_);

ObTablespaceSchema::ObTablespaceSchema()
  : ObSchema()
{
  reset();
}

ObTablespaceSchema::ObTablespaceSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObTablespaceSchema::ObTablespaceSchema(const ObTablespaceSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObTablespaceSchema::~ObTablespaceSchema()
{
}

ObTablespaceSchema& ObTablespaceSchema::operator =(const ObTablespaceSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_tablespace_id(src_schema.tablespace_id_);
    set_schema_version(src_schema.schema_version_);
    set_master_key_id(src_schema.master_key_id_);
    if (OB_FAIL(set_tablespace_name(src_schema.tablespace_name_))) {
      LOG_WARN("fail set ts name", K(src_schema), K(ret));
    } else if (OB_FAIL(set_encryption_name(src_schema.encryption_name_))) {
      LOG_WARN("fail set encryption name", K(src_schema), K(ret));
    } else if (OB_FAIL(set_encrypt_key(src_schema.encrypt_key_))) {
      LOG_WARN("fail to set encrypt key", K(ret), K(src_schema));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

void ObTablespaceSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablespace_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  master_key_id_ = 0;
  reset_string(tablespace_name_);
  reset_string(encryption_name_);
  reset_string(encrypt_key_);
}

int64_t ObTablespaceSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObTablespaceSchema);
  convert_size += tablespace_name_.length() + 1;
  convert_size += encryption_name_.length() + 1;
  convert_size += encrypt_key_.length() + 1;
  return convert_size;
}

// ObProfileSchema

ObProfileSchema::ObProfileSchema()
  : ObSchema()
{
  reset();
}

ObProfileSchema::ObProfileSchema(ObIAllocator *allocator)
  : ObSchema(allocator),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    profile_id_(common::OB_INVALID_ID),
    schema_version_(common::OB_INVALID_VERSION),
    profile_name_(),
    failed_login_attempts_(-1),
    password_lock_time_(-1),
    password_verify_function_(),
    password_life_time_(-1),
    password_grace_time_(-1)
{
}

ObProfileSchema::ObProfileSchema(const ObProfileSchema &other)
  : ObSchema()
{
  *this = other;
}

ObProfileSchema::~ObProfileSchema()
{
}

ObProfileSchema &ObProfileSchema::operator=(const ObProfileSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    profile_id_ = other.profile_id_;
    schema_version_ = other.schema_version_;

    if (OB_FAIL(deep_copy_str(other.profile_name_, profile_name_))) {
      LOG_WARN("Fail to deep copy profile_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.password_verify_function_, password_verify_function_))) {
      LOG_WARN("Fail to deep copy verify function name", K(ret));
    } else {
      failed_login_attempts_ = other.failed_login_attempts_;
      password_lock_time_ = other.password_lock_time_;
      password_life_time_ = other.password_life_time_;
      password_grace_time_ = other.password_grace_time_;
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObProfileSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(profile_id_)
      || schema_version_ < 0
      || profile_name_.empty()) {
    ret = false;
  }
  return ret;
}

void ObProfileSchema::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  profile_id_ = common::OB_INVALID_ID;
  schema_version_ = common::OB_INVALID_VERSION;
  profile_name_.reset();
  failed_login_attempts_ = -1;
  password_lock_time_ = -1;
  password_verify_function_.reset();
  password_life_time_ = -1;
  password_grace_time_ = -1;
}

int64_t ObProfileSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += profile_name_.length() + 1;
  convert_size += password_verify_function_.length() + 1;
  return convert_size;
}

int ObProfileSchema::set_value(const int64_t type, const int64_t value)
{
  int ret = OB_SUCCESS;
  switch (type) {
  case FAILED_LOGIN_ATTEMPTS:
    set_failed_login_attempts(value);
    break;
  case PASSWORD_LOCK_TIME:
    set_password_lock_time(value);
    break;
  case PASSWORD_LIFE_TIME:
    set_password_life_time(value);
    break;
  case PASSWORD_GRACE_TIME:
    set_password_grace_time(value);
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown type", K(type));
    break;
  }
  return ret;
}

int ObProfileSchema::set_default_value(const int64_t type)
{
  int ret = OB_SUCCESS;
  if (type < 0 || type >= MAX_PARAMS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown type", K(type));
  } else if (type == PASSWORD_VERIFY_FUNCTION) {
    password_verify_function_.reset();
  } else if (OB_FAIL(set_value(type, DEFAULT_PARAM_VALUES[type]))) {
    LOG_WARN("fail to set schema default value", K(ret));
  }
  return ret;
}

int ObProfileSchema::set_default_values()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_PARAMS; ++i) {
    if (i != PASSWORD_VERIFY_FUNCTION && OB_FAIL(set_value(i, DEFAULT_PARAM_VALUES[i]))) {
      LOG_WARN("fail to set schema value", K(ret));
    }
  }
  return ret;
}

int ObProfileSchema::set_default_values_v2()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < MAX_PARAMS; ++i) {
    if (PASSWORD_VERIFY_FUNCTION == i) {
      password_verify_function_ = "DEFAULT";
    } else if (OB_FAIL(set_value(i, DEFAULT_VALUE))) {
      LOG_WARN("fail to set schema value", K(ret));
    }
  }
  return ret;
}

int ObProfileSchema::set_invalid_values()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_PARAMS; ++i) {
    if (i != PASSWORD_VERIFY_FUNCTION && OB_FAIL(set_value(i, INVALID_PARAM_VALUES[i]))) {
      LOG_WARN("fail to set schema value", K(ret));
    }
  }
  return ret;
}

int ObProfileSchema::get_default_value(const int64_t type, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (type < 0 || type >= MAX_PARAMS || type == PASSWORD_VERIFY_FUNCTION) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown type", K(type));
  } else {
    value = DEFAULT_PARAM_VALUES[type];
  }
  return ret;
}

const int64_t ObProfileSchema::DEFAULT_PARAM_VALUES[] = {
  10,                   //times
  USECS_PER_DAY,        //1 day
  -1,                   //nouse
  180 * USECS_PER_DAY,  //180 day
  7 * USECS_PER_DAY,    //7 day
};
const int64_t ObProfileSchema::INVALID_PARAM_VALUES[] = {
  -1,        //times
  -1,        //1 day
  -1,        //nouse
  -1,        //1 day
  -1,        //1 day
};
static_assert(ARRAYSIZEOF(ObProfileSchema::DEFAULT_PARAM_VALUES) == ObProfileSchema::MAX_PARAMS, "array size");
static_assert(ARRAYSIZEOF(ObProfileSchema::INVALID_PARAM_VALUES) == ObProfileSchema::MAX_PARAMS, "array size");

const char* ObProfileSchema::PARAM_VALUE_NAMES[] = {
  "FAILED_LOGIN_ATTEMPTS",
  "PASSWORD_LOCK_TIME",
  "PASSWORD_VERIFY_FUNCTION",
  "PASSWORD_LIFE_TIME",
  "PASSWORD_GRACE_TIME",
};

static_assert(ARRAYSIZEOF(ObProfileSchema::PARAM_VALUE_NAMES) == ObProfileSchema::MAX_PARAMS, "array size");

OB_SERIALIZE_MEMBER(ObProfileSchema,
                    tenant_id_,
                    profile_id_,
                    schema_version_,
                    profile_name_,
                    failed_login_attempts_,
                    password_lock_time_,
                    password_verify_function_,
                    password_life_time_,
                    password_grace_time_);

ObDirectorySchema::ObDirectorySchema()
  : ObSchema()
{
  reset();
}

ObDirectorySchema::ObDirectorySchema(common::ObIAllocator *allocator)
  : ObSchema(allocator),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    directory_id_(common::OB_INVALID_TENANT_ID),
    schema_version_(common::OB_INVALID_VERSION),
    directory_name_(),
    directory_path_()
{
}

ObDirectorySchema::~ObDirectorySchema()
{
}

ObDirectorySchema::ObDirectorySchema(const ObDirectorySchema &other)
  : ObSchema()
{
  *this = other;
}

ObDirectorySchema &ObDirectorySchema::operator=(const ObDirectorySchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    directory_id_ = other.directory_id_;
    schema_version_ = other.schema_version_;

    if (OB_FAIL(set_directory_name(other.directory_name_))) {
      LOG_WARN("Fail to deep copy directory name", K(ret));
    } else if (OB_FAIL(set_directory_path(other.directory_path_))) {
      LOG_WARN("Fail to deep copy directory path", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObDirectorySchema::assign(const ObDirectorySchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObDirectorySchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(directory_id_)
      || schema_version_ < 0
      || directory_name_.empty()
      || directory_path_.empty()) {
    ret = false;
  }
  return ret;
}

void ObDirectorySchema::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  directory_id_ = OB_INVALID_ID;
  schema_version_ = common::OB_INVALID_VERSION;
  directory_name_.reset();
  directory_path_.reset();
}

int64_t ObDirectorySchema::get_convert_size() const
{
  return sizeof(ObDirectorySchema)
       + directory_name_.length() + 1
       + directory_path_.length() + 1;
}

OB_SERIALIZE_MEMBER(ObDirectorySchema,
                    tenant_id_,
                    directory_id_,
                    schema_version_,
                    directory_name_,
                    directory_path_);

const char *get_audit_operation_type_str(const ObSAuditOperationType type)
{
  static const char * const ret_str[] = {
      "INVALID",
      "ALL STATEMENTS",
      "ALTER SYSTEM",
      "CLUSTER",
      "CONTEXT",
      "DATABASE LINK",
      "INDEX",
      "MATERIALIZED VIEW",
      "NOT EXISTS",
      "OUTLINE",
      "PROCEDURE",
      "PROFILE",
      "PUBLIC DATABASE LINK",
      "PUBLIC SYNONYM",
      "ROLE",
      "SEQUENCE",
      "SESSION",
      "SYNONYM",
      "SYSTEM AUDIT",
      "SYSTEM GRANT",
      "TABLE",
      "TABLESPACE",
      "TRIGGER",
      "TYPE",
      "USER",
      "VIEW",
      "ALTER SEQUENCE",
      "ALTER TABLE",
      "COMMENT TABLE",
      "DELETE TABLE",
      "EXECUTE PROCEDURE",
      "GRANT PROCEDURE",
      "GRANT SEQUENCE",
      "GRANT TABLE",
      "GRANT TYPE",
      "INSERT TABLE",
      "SELECT SEQUENCE",
      "SELECT TABLE",
      "UPDATE TABLE",
      "ALTER",
      "AUDIT",
      "COMMENT",
      "DELETE",
      "GRANT",
      "INDEX",
      "INSERT",
      "LOCK",
      "RENAME",
      "SELECT",
      "UPDATE",
      "REF",
      "EXECUTE",
      "CREATE",
      "READ",
      "WRITE",
      "FLASHBACK",
      "DIRECTORY"
  };
  static_assert(ARRAYSIZEOF(ret_str) == ObSAuditOperationType::AUDIT_OP_MAX, "array size mismatch");
  return (AUDIT_OP_INVALID < type && type < AUDIT_OP_MAX) ? ret_str[type] : "ERROR_TYPR";
}

int get_operation_type_from_item_type(const bool is_stmt_audit,
    const int32_t item_type_id, ObSAuditOperationType &operation_type, bool &is_ddl)
{
  int ret = OB_SUCCESS;
  operation_type = AUDIT_OP_INVALID;
  const ObItemType item_type = static_cast<ObItemType>(item_type_id);
  is_ddl = true;
  if (is_stmt_audit) {
    switch (item_type) {
      case T_AUDIT_ALTER_SYSTEM: {
        operation_type = AUDIT_OP_ALTER_SYSTEM;
        break;
      }
      case T_AUDIT_CLUSTER: {
        operation_type = AUDIT_OP_CLUSTER;
        break;
      }
      case T_AUDIT_CONTEXT: {
        operation_type = AUDIT_OP_CONTEXT;
        break;
      }
      case T_AUDIT_DBLINK: {
        operation_type = AUDIT_OP_DBLINK;
        break;
      }
      case T_AUDIT_DIRECTORY: {
        operation_type = AUDIT_OP_DIRECTORY;
        break;
      }
      case T_AUDIT_INDEX: {
        operation_type = AUDIT_OP_INDEX_TABLE;
        break;
      }
      case T_AUDIT_MATERIALIZED_VIEW: {
        operation_type = AUDIT_OP_MATERIALIZED_VIEW;
        break;
      }
      case T_AUDIT_NOT_EXIST: {
        operation_type = AUDIT_OP_NOT_EXIST;
        break;
      }
      case T_AUDIT_OUTLINE: {
        operation_type = AUDIT_OP_OUTLINE;
        break;
      }
      case T_AUDIT_PROCEDURE: {
        operation_type = AUDIT_OP_PROCEDURE;
        break;
      }
      case T_AUDIT_PROFILE: {
        operation_type = AUDIT_OP_PROFILE;
        break;
      }
      case T_AUDIT_PUBLIC_DBLINK: {
        operation_type = AUDIT_OP_PUBLIC_DBLINK;
        break;
      }
      case T_AUDIT_PUBLIC_SYNONYM: {
        operation_type = AUDIT_OP_PUBLIC_SYNONYM;
        break;
      }
      case T_AUDIT_ROLE: {
        operation_type = AUDIT_OP_ROLE;
        break;
      }
      case T_AUDIT_SEQUENCE: {
        operation_type = AUDIT_OP_SEQUENCE;
        break;
      }
      case T_AUDIT_SESSION: {
        operation_type = AUDIT_OP_SESSION;
        break;
      }
      case T_AUDIT_SYNONYM: {
        operation_type = AUDIT_OP_SYNONYM;
        break;
      }
      case T_AUDIT_SYSTEM_AUDIT: {
        operation_type = AUDIT_OP_SYSTEM_AUDIT;
        break;
      }
      case T_AUDIT_SYSTEM_GRANT: {
        operation_type = AUDIT_OP_SYSTEM_GRANT;
        break;
      }
      case T_AUDIT_TABLE: {
        operation_type = AUDIT_OP_TABLE;
        break;
      }
      case T_AUDIT_TABLESPACE: {
        operation_type = AUDIT_OP_TABLESPACE;
        break;
      }
      case T_AUDIT_TRIGGER: {
        operation_type = AUDIT_OP_TRIGGER;
        break;
      }
      case T_AUDIT_TYPE: {
        operation_type = AUDIT_OP_TYPE;
        break;
      }
      case T_AUDIT_USER: {
        operation_type = AUDIT_OP_USER;
        break;
      }
      case T_AUDIT_VIEW: {
        operation_type = AUDIT_OP_VIEW;
        break;
      }
      case T_AUDIT_ALTER_SEQUENCE: {
        operation_type = AUDIT_OP_ALTER_SEQUENCE;
        break;
      }
      case T_AUDIT_ALTER_TABLE: {
        operation_type = AUDIT_OP_ALTER_TABLE;
        break;
      }
      case T_AUDIT_COMMENT_TABLE: {
        operation_type = AUDIT_OP_COMMENT_TABLE;
        is_ddl = false;
        break;
      }
      case T_AUDIT_DELETE_TABLE: {
        operation_type = AUDIT_OP_DELETE_TABLE;
        is_ddl = false;
        break;
      }
      case T_AUDIT_EXECUTE_PROCEDURE: {
        operation_type = AUDIT_OP_EXECUTE_PROCEDURE;
        is_ddl = false;
        break;
      }
      case T_AUDIT_GRANT_PROCEDURE: {
        operation_type = AUDIT_OP_GRANT_PROCEDURE;
        break;
      }
      case T_AUDIT_GRANT_SEQUENCE: {
        operation_type = AUDIT_OP_GRANT_SEQUENCE;
        break;
      }
      case T_AUDIT_GRANT_TABLE: {
        operation_type = AUDIT_OP_GRANT_TABLE;
        break;
      }
      case T_AUDIT_GRANT_TYPE: {
        operation_type = AUDIT_OP_GRANT_TYPE;
        break;
      }
      case T_AUDIT_INSERT_TABLE: {
        operation_type = AUDIT_OP_INSERT_TABLE;
        is_ddl = false;
        break;
      }
      case T_AUDIT_SELECT_SEQUENCE: {
        operation_type = AUDIT_OP_SELECT_SEQUENCE;
        is_ddl = false;
        break;
      }
      case T_AUDIT_SELECT_TABLE: {
        operation_type = AUDIT_OP_SELECT_TABLE;
        is_ddl = false;
        break;
      }
      case T_AUDIT_UPDATE_TABLE: {
        operation_type = AUDIT_OP_UPDATE_TABLE;
        is_ddl = false;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "not support item type", "item_type", get_type_name(item_type), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "item type");
      }
    }
  } else {
    switch (item_type) {
      case T_AUDIT_ALTER: {
        operation_type = AUDIT_OP_ALTER;
        break;
      }
      case T_AUDIT_AUDIT: {
        operation_type = AUDIT_OP_AUDIT;
        break;
      }
      case T_AUDIT_COMMENT: {
        operation_type = AUDIT_OP_COMMENT;
        break;
      }
      case T_AUDIT_DELETE: {
        operation_type = AUDIT_OP_DELETE;
        break;
      }
      case T_AUDIT_EXECUTE: {
        operation_type = AUDIT_OP_EXECUTE;
        break;
      }
      case T_AUDIT_FLASHBACK: {
        operation_type = AUDIT_OP_FLASHBACK;
        break;
      }
      case T_AUDIT_GRANT: {
        operation_type = AUDIT_OP_GRANT;
        break;
      }
      case T_AUDIT_INDEX: {
        operation_type = AUDIT_OP_INDEX;
        break;
      }
      case T_AUDIT_INSERT: {
        operation_type = AUDIT_OP_INSERT;
        break;
      }
      case T_AUDIT_LOCK: {
        operation_type = AUDIT_OP_LOCK;
        break;
      }
      case T_AUDIT_RENAME: {
        operation_type = AUDIT_OP_RENAME;
        break;
      }
      case T_AUDIT_SELECT: {
        operation_type = AUDIT_OP_SELECT;
        break;
      }
      case T_AUDIT_UPDATE: {
        operation_type = AUDIT_OP_UPDATE;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "not support item type", "item_type", get_type_name(item_type), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "item type");
      }
    }
  }
  return ret;
}

const char *OB_OBJECT_TYPE_STR[] =
{
  "INVALID",
  "TABLE",
  "SEQUENCE",
  "PACKAGE",
  "TYPE",
  "PACKAGE_BODY",
  "TYPE_BODY",
  "TRIGGER",
  "VIEW",
  "FUNCTION",
  "DIRECTORY",
  "INDEX",
  "PROCEDURE",
  "SYNONYM",
  "SYS_PACKAGE",
  "SYS_PACKAGE_ONLY_OBJ_PRIV",
  "CONTEXT"
};
static_assert(ARRAYSIZEOF(OB_OBJECT_TYPE_STR) == static_cast<int64_t>(ObObjectType::MAX_TYPE),
              "array size mismatch");

const char *ob_object_type_str(const ObObjectType object_type)
{
  const char *ret = "Unknown";
  if (object_type >= ObObjectType::INVALID && object_type < ObObjectType::MAX_TYPE) {
    ret = OB_OBJECT_TYPE_STR[static_cast<int64_t>(object_type)];
  }
  return ret;
}

IObErrorInfo::~IObErrorInfo()
{}

uint64_t IObErrorInfo::get_tenant_id() const
{
  return OB_INVALID_ID;
}
uint64_t IObErrorInfo::get_database_id() const
{
  return OB_INVALID_ID;
}
uint64_t IObErrorInfo::get_object_id() const
{
  return OB_INVALID_ID;
}
int64_t IObErrorInfo::get_schema_version() const
{
  return OB_INVALID_SCHEMA_VERSION;
}
ObObjectType IObErrorInfo::get_object_type() const
{
  return ObObjectType::INVALID;
}

int ObZoneRegion::assign(const ObZoneRegion &that)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(zone_.assign(that.zone_.ptr()))) {
    SHARE_LOG(WARN, "fail to assign zone", K(ret));
  } else if (OB_FAIL(region_.assign(that.region_.ptr()))) {
    SHARE_LOG(WARN, "fail to assign region", K(ret));
  } else {
    check_zone_type_ = that.check_zone_type_;
  }
  return ret;
}

int ObZoneRegion::set_check_zone_type(const int64_t zone_type)
{
  int ret = OB_SUCCESS;
  ObZoneType my_zone_type = static_cast<ObZoneType>(zone_type);
  if (my_zone_type < ZONE_TYPE_READWRITE || my_zone_type >= ZONE_TYPE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_type));
  } else if (ZONE_TYPE_READONLY == my_zone_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("readonly zone do not supported", KR(ret), K(zone_type));
  } else if (ZONE_TYPE_READWRITE == my_zone_type) {
    check_zone_type_ = CZY_NO_ENCRYPTION;
  } else {
    check_zone_type_ = CZY_ENCRYPTION;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObContextSchema,
                    tenant_id_,
                    context_id_,
                    schema_version_,
                    namespace_,
                    schema_name_,
                    trusted_package_,
                    type_,
                    origin_con_id_,
                    tracking_);

ObContextSchema::ObContextSchema()
  : ObSchema()
{
  reset();
}

ObContextSchema::ObContextSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObContextSchema::ObContextSchema(const ObContextSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObContextSchema::~ObContextSchema()
{
}

int ObContextSchema::assign(const ObContextSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_context_id(src_schema.context_id_);
    set_schema_version(src_schema.schema_version_);
    set_origin_con_id(src_schema.origin_con_id_);
    set_is_tracking(src_schema.tracking_);
    set_context_type(src_schema.type_);
    if (OB_FAIL(set_namespace(src_schema.namespace_))) {
      LOG_WARN("failed to set ctx namespace", K(ret));
    } else if (OB_FAIL(set_schema_name(src_schema.schema_name_))) {
      LOG_WARN("failed to set schema name", K(ret));;
    } else if (OB_FAIL(set_trusted_package(src_schema.trusted_package_))) {
      LOG_WARN("failed to set trusted package name", K(ret));
    }
  }
  return ret;
}

ObContextSchema& ObContextSchema::operator =(const ObContextSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(src_schema))) {
    error_ret_ = ret;
    LOG_WARN("failed to assign context schema", K(ret));
  }
  return *this;
}

void ObContextSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  tenant_id_ = OB_INVALID_ID;
  context_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  origin_con_id_ = OB_INVALID_ID;
  type_ = ACCESSED_LOCALLY;
  tracking_ = true;
  reset_string(namespace_);
  reset_string(schema_name_);
  reset_string(trusted_package_);
  ObSchema::reset();
}


// ObSimpleMockFKParentTableSchema begin
ObSimpleMockFKParentTableSchema::ObSimpleMockFKParentTableSchema()
  : ObSchema()
{
  reset();
}

ObSimpleMockFKParentTableSchema::ObSimpleMockFKParentTableSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleMockFKParentTableSchema::ObSimpleMockFKParentTableSchema(const ObSimpleMockFKParentTableSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObSimpleMockFKParentTableSchema::~ObSimpleMockFKParentTableSchema()
{
}

ObSimpleMockFKParentTableSchema& ObSimpleMockFKParentTableSchema::operator=(
    const ObSimpleMockFKParentTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(src_schema))) {
    error_ret_ = ret;
    LOG_WARN("failed to assign ObSimpleMockFKParentTableSchema", K(ret), K(src_schema));
  }
  return *this;
}

int ObSimpleMockFKParentTableSchema::assign(const ObSimpleMockFKParentTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_database_id(src_schema.database_id_);
    set_mock_fk_parent_table_id(src_schema.mock_fk_parent_table_id_);
    set_schema_version(src_schema.schema_version_);
    if (OB_FAIL(set_mock_fk_parent_table_name(src_schema.mock_fk_parent_table_name_))) {
      LOG_WARN("failed to set mock_fk_parent_table_name", K(ret));
    }
  }
  return ret;
}

void ObSimpleMockFKParentTableSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  mock_fk_parent_table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  reset_string(mock_fk_parent_table_name_);
  ObSchema::reset();
}

int64_t ObSimpleMockFKParentTableSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObSimpleMockFKParentTableSchema);
  convert_size += mock_fk_parent_table_name_.length() + 1;
  return convert_size;
}
// ObSimpleMockFKParentTableSchema end


// ObMockFKParentTableSchema begin
ObMockFKParentTableSchema::ObMockFKParentTableSchema()
  : ObSimpleMockFKParentTableSchema()
{
  reset();
}

ObMockFKParentTableSchema::ObMockFKParentTableSchema(ObIAllocator *allocator)
  : ObSimpleMockFKParentTableSchema(allocator),
    foreign_key_infos_(SCHEMA_BIG_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    column_array_(SCHEMA_MID_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator))
{
  reset();
}

ObMockFKParentTableSchema::ObMockFKParentTableSchema(const ObMockFKParentTableSchema &src_schema)
  : ObSimpleMockFKParentTableSchema()
{
  reset();
  *this = src_schema;
}

ObMockFKParentTableSchema::~ObMockFKParentTableSchema()
{
}

int ObMockFKParentTableSchema::assign(const ObMockFKParentTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    if (OB_FAIL(ObSimpleMockFKParentTableSchema::assign(src_schema))) {
      LOG_WARN("failed to assign ObSimpleMockFKParentTableSchema", K(ret), K(src_schema));
    } else if (OB_FAIL(set_foreign_key_infos(src_schema.get_foreign_key_infos()))) {
      LOG_WARN("failed to assign column_array", K(ret));
    } else if (OB_FAIL(set_column_array(src_schema.get_column_array()))) {
      LOG_WARN("failed to set column_array", K(ret), K(src_schema.get_column_array()));
    } else if (FALSE_IT(set_operation_type(src_schema.operation_type_))) {
    }
  }
  return ret;
}

ObMockFKParentTableSchema& ObMockFKParentTableSchema::operator=(
    const ObMockFKParentTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(src_schema))) {
    error_ret_ = ret;
    LOG_WARN("failed to assign MockFKParentTableSchema", K(ret), K(src_schema));
  }
  return *this;
}

int64_t ObMockFKParentTableSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObSimpleMockFKParentTableSchema::get_convert_size();
  convert_size += sizeof(ObMockFKParentTableSchema) - sizeof(ObSimpleMockFKParentTableSchema);
  // foreign_key_infos_
  convert_size += foreign_key_infos_.get_data_size();
  for (int64_t i = 0; i < foreign_key_infos_.count(); ++i) {
    convert_size += foreign_key_infos_.at(i).get_convert_size();
  }
  // column_array_
  convert_size += column_array_.get_data_size();
  for (int64_t i = 0; i < column_array_.count(); ++i) {
    convert_size += column_array_.at(i).second.length() + 1;
  }
  return convert_size;
}

void ObMockFKParentTableSchema::reset()
{
  foreign_key_infos_.reset();
  column_array_.reset();
  operation_type_ = ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_INVALID;
  ObSimpleMockFKParentTableSchema::reset();
}

int ObMockFKParentTableSchema::set_foreign_key_infos(const ObIArray<ObForeignKeyInfo> &foreign_key_infos)
{
  int ret = OB_SUCCESS;
  foreign_key_infos_.reset();
  int64_t count = foreign_key_infos.count();
  if (OB_FAIL(foreign_key_infos_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(add_foreign_key_info(foreign_key_infos.at(i)))) {
        LOG_WARN("failed to add foreign_key_info", K(ret));
      }
    }
  }
  return ret;
}

int ObMockFKParentTableSchema::add_foreign_key_info(const ObForeignKeyInfo &foreign_key_info)
{
  int ret = OB_SUCCESS;
  int64_t new_fk_idx = foreign_key_infos_.count();
  if (OB_FAIL(foreign_key_infos_.push_back(ObForeignKeyInfo()))) {
    LOG_WARN("fail to push back empty element", K(ret), K(new_fk_idx));
  } else {
    const ObString &foreign_key_name = foreign_key_info.foreign_key_name_;
    ObForeignKeyInfo &foreign_info = foreign_key_infos_.at(new_fk_idx);
    if (nullptr == new (&foreign_info) ObForeignKeyInfo(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("placement new return nullptr", K(ret));
    } else if (OB_FAIL(foreign_info.assign(foreign_key_info))) {
      LOG_WARN("fail to assign foreign key info", K(ret), K(foreign_key_info));
    } else if (!foreign_key_name.empty()
               && OB_FAIL(deep_copy_str(foreign_key_name, foreign_info.foreign_key_name_))) {
      LOG_WARN("failed to deep copy foreign key name", K(ret), K(foreign_key_name));
    }
  }
  return ret;
}

int ObMockFKParentTableSchema::set_column_array(const ObMockFKParentTableColumnArray &other)
{
  int ret = OB_SUCCESS;
  column_array_.reset();
  if (OB_FAIL(column_array_.reserve(other.count()))) {
    LOG_WARN("fail to reserve array", K(ret), K(other.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
      if (OB_FAIL(add_column_info_to_column_array(other.at(i)))) {
        LOG_WARN("failed to add_column_info_to_column_array", K(ret), K(i), K(other.at(i).first), K(other.at(i).second));
      }
    }
  }
  return ret;
}

int ObMockFKParentTableSchema::add_column_info_to_column_array(const std::pair<uint64_t, common::ObString> &column_info)
{
  int ret = OB_SUCCESS;
  ObString column_name;
  if (OB_FAIL(column_array_.push_back(std::make_pair(column_info.first, column_info.second)))) {
    LOG_WARN("failed to push_back to column_array_", K(ret), K(column_info.first), K(column_info.second));
  } else if (!column_info.second.empty()
             && OB_FAIL(deep_copy_str(column_info.second, column_array_.at(column_array_.count() - 1).second))) {
    LOG_WARN("failed to deep copy column name", K(ret), K(column_info.first), K(column_info.second));
  }
  return ret;
}

void ObMockFKParentTableSchema::get_column_name_by_column_id(
    const uint64_t column_id, common::ObString &column_name, bool &is_column_exist) const
{
  is_column_exist = false;
  for (int64_t i = 0; !is_column_exist && i < column_array_.count(); ++i) {
    if (column_array_.at(i).first == column_id) {
      column_name = column_array_.at(i).second;
      is_column_exist = true;
    }
  }
}

void ObMockFKParentTableSchema::get_column_id_by_column_name(
    const common::ObString column_name, uint64_t &column_id, bool &is_column_exist) const
{
  is_column_exist = false;
  for (int64_t i = 0; !is_column_exist && i < column_array_.count(); ++i) {
    if (0 == column_array_.at(i).second.compare(column_name)) {
      column_id = column_array_.at(i).first;
      is_column_exist = true;
    }
  }
}

int ObMockFKParentTableSchema::reconstruct_column_array_by_foreign_key_infos(const ObMockFKParentTableSchema* orig_mock_fk_parent_table_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_mock_fk_parent_table_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_mock_fk_parent_table_ptr is null", K(ret));
  } else {
    reset_column_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos_.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_infos_.at(i).parent_column_ids_.count(); ++j) {
        ObString column_name;
        bool is_column_exist = false;
        get_column_name_by_column_id(foreign_key_infos_.at(i).parent_column_ids_.at(j), column_name, is_column_exist);
        if (!is_column_exist) {
          orig_mock_fk_parent_table_ptr->get_column_name_by_column_id(foreign_key_infos_.at(i).parent_column_ids_.at(j), column_name, is_column_exist);
          if (!is_column_exist) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column is not exist", K(ret), K(foreign_key_infos_.at(i).parent_column_ids_.at(j)), KPC(orig_mock_fk_parent_table_ptr));
          } else if (OB_FAIL(add_column_info_to_column_array(std::make_pair(foreign_key_infos_.at(i).parent_column_ids_.at(j), column_name)))) {
            LOG_WARN("add_column_info_to_column_array failed", K(ret), K(foreign_key_infos_.at(i).parent_column_ids_.at(j)), K(column_name));
          }
        }
      }
    }
  }
  return ret;
}
// ObMockFKParentTableSchema end

ObRlsSecColumnSchema::ObRlsSecColumnSchema()
  : ObSchema()
{
  reset();
}

ObRlsSecColumnSchema::ObRlsSecColumnSchema(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObRlsSecColumnSchema::~ObRlsSecColumnSchema()
{
}

ObRlsSecColumnSchema::ObRlsSecColumnSchema(const ObRlsSecColumnSchema &other)
  : ObSchema()
{
  *this = other;
}

ObRlsSecColumnSchema &ObRlsSecColumnSchema::operator=(const ObRlsSecColumnSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    rls_policy_id_ = other.rls_policy_id_;
    column_id_ = other.column_id_;
    schema_version_ = other.schema_version_;
  }
  return *this;
}

int ObRlsSecColumnSchema::assign(const ObRlsSecColumnSchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObRlsSecColumnSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(rls_policy_id_)
      || !is_valid_id(column_id_)) {
    ret = false;
  }
  return ret;
}

void ObRlsSecColumnSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  rls_policy_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  ObSchema::reset();
}

int64_t ObRlsSecColumnSchema::get_convert_size() const
{
  return sizeof(ObRlsSecColumnSchema);
}

OB_SERIALIZE_MEMBER(ObRlsSecColumnSchema,
                    tenant_id_,
                    rls_policy_id_,
                    column_id_,
                    schema_version_);

ObRlsPolicySchema::ObRlsPolicySchema()
  : ObSchema()
{
  reset();
}

ObRlsPolicySchema::ObRlsPolicySchema(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObRlsPolicySchema::~ObRlsPolicySchema()
{
}

ObRlsPolicySchema::ObRlsPolicySchema(const ObRlsPolicySchema &other)
  : ObSchema()
{
  *this = other;
}

ObRlsPolicySchema &ObRlsPolicySchema::operator=(const ObRlsPolicySchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    rls_policy_id_ = other.rls_policy_id_;
    schema_version_ = other.schema_version_;
    table_id_ = other.table_id_;
    rls_group_id_ = other.rls_group_id_;
    stmt_type_ = other.stmt_type_;
    check_opt_ = other.check_opt_;
    enable_flag_ = other.enable_flag_;

    if (OB_FAIL(set_policy_name(other.policy_name_))) {
      LOG_WARN("Fail to deep copy policy name", K(ret));
    } else if (OB_FAIL(set_policy_function_schema(other.policy_function_schema_))) {
      LOG_WARN("Fail to deep copy policy function schema", K(ret));
    } else if (OB_FAIL(set_policy_package_name(other.policy_package_name_))) {
      LOG_WARN("Fail to deep copy policy package name", K(ret));
    } else if (OB_FAIL(set_policy_function_name(other.policy_function_name_))) {
      LOG_WARN("Fail to deep copy policy function name", K(ret));
    }
    if (OB_SUCC(ret) && other.column_cnt_ > 0) {
      sec_column_array_ = static_cast<ObRlsSecColumnSchema**>(alloc(sizeof(ObRlsSecColumnSchema*) * other.column_cnt_));
      if (NULL == sec_column_array_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for sec_column_array_", KR(ret));
      } else {
        MEMSET(sec_column_array_, 0, sizeof(ObRlsSecColumnSchema*) * other.column_cnt_);
        column_array_capacity_ = other.column_cnt_;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.column_cnt_; ++i) {
      ObRlsSecColumnSchema *column = other.sec_column_array_[i];
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rls sec column is null", KR(ret));
      } else if (OB_FAIL(add_sec_column(*column))) {
        LOG_WARN("failed to add add sec column, ", KR(ret), KPC(column));
      }
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObRlsPolicySchema::assign(const ObRlsPolicySchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObRlsPolicySchema::is_valid() const
{
  bool valid_ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(rls_policy_id_)
      || !is_valid_id(table_id_)
      || !is_valid_id(rls_group_id_)
      || policy_name_.empty()
      || policy_function_schema_.empty()
      || policy_function_name_.empty()) {
    valid_ret = false;
  }
  for (int64_t i = 0; valid_ret && i < column_cnt_; ++i) {
    const ObRlsSecColumnSchema *column = sec_column_array_[i];
    if (OB_ISNULL(column)) {
      valid_ret = false;
    } else if (!column->is_valid()) {
      valid_ret = false;
    }
  }
  return valid_ret;
}

void ObRlsPolicySchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  rls_policy_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  table_id_ = OB_INVALID_ID;
  rls_group_id_ = OB_INVALID_ID;
  stmt_type_ = 0;
  check_opt_ = false;
  enable_flag_ = true;
  policy_name_.reset();
  policy_function_schema_.reset();
  policy_package_name_.reset();
  policy_function_name_.reset();
  sec_column_array_ = NULL;
  column_array_capacity_ = 0;
  column_cnt_ = 0;
  ObSchema::reset();
}

int64_t ObRlsPolicySchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObRlsPolicySchema);
  convert_size +=  policy_name_.length() + 1;
  convert_size +=  policy_function_schema_.length() + 1;
  convert_size +=  policy_package_name_.length() + 1;
  convert_size +=  policy_function_name_.length() + 1;
  convert_size += column_cnt_ * sizeof(ObRlsSecColumnSchema*);
  for (int64_t i = 0; i < column_cnt_ && NULL != sec_column_array_[i];  ++i) {
    convert_size += sec_column_array_[i]->get_convert_size();
  }
  return convert_size;
}

int ObRlsPolicySchema::add_sec_column(const ObRlsSecColumnSchema &sec_column)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObRlsSecColumnSchema *new_column = NULL;
  if (OB_ISNULL(buf = static_cast<char*>(alloc(sizeof(ObRlsSecColumnSchema))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), "size", sizeof(ObRlsSecColumnSchema));
  } else if (OB_ISNULL(new_column = new (buf) ObRlsSecColumnSchema(allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("failed to new new sec column", KR(ret));
  } else if (OB_FAIL(new_column->assign(sec_column))) {
    LOG_WARN("failed to assign sec column", KR(ret), K(sec_column));
  } else if (0 == column_array_capacity_) {
    if (NULL == (sec_column_array_ = static_cast<ObRlsSecColumnSchema**>(
        alloc(sizeof(ObRlsSecColumnSchema*) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for sec_column_array_", KR(ret));
    } else {
      column_array_capacity_ = DEFAULT_ARRAY_CAPACITY;
      MEMSET(sec_column_array_, 0, sizeof(ObRlsSecColumnSchema*) * DEFAULT_ARRAY_CAPACITY);
    }
  } else if (column_cnt_ >= column_array_capacity_) {
    int64_t tmp_size = 2 * column_array_capacity_;
    ObRlsSecColumnSchema **tmp = NULL;
    if (NULL == (tmp = static_cast<ObRlsSecColumnSchema**>(
        alloc(sizeof(ObRlsSecColumnSchema*) * tmp_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for sec_column_array_", KR(ret), K(tmp_size));
    } else {
      MEMCPY(tmp, sec_column_array_, sizeof(ObRlsSecColumnSchema*) * column_array_capacity_);
      free(sec_column_array_);
      sec_column_array_ = tmp;
      column_array_capacity_ = tmp_size;
    }
  }
  if (OB_SUCC(ret)) {
    sec_column_array_[column_cnt_++] = new_column;
  }
  return ret;
}

int ObRlsPolicySchema::set_ids_cascade()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    if (OB_ISNULL(sec_column_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls sec column is null", KR(ret));
    } else {
      sec_column_array_[i]->set_tenant_id(tenant_id_);
      sec_column_array_[i]->set_rls_policy_id(rls_policy_id_);
    }
  }
  return ret;
}

int ObRlsPolicySchema::rebuild_with_table_schema(const ObRlsPolicySchema &src_schema,
                                                 const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t tmp_column_cnt = 0;
  OZ (assign(src_schema));
  OX (table_id_ = table_schema.get_table_id());
  OX (tmp_column_cnt = column_cnt_);
  OX (column_cnt_ = 0);
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_cnt; ++i) {
    if (OB_ISNULL(sec_column_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls sec column is null", KR(ret));
    } else if (NULL == table_schema.get_column_schema(sec_column_array_[i]->get_column_id())) {
      // do nothing
    } else {
      sec_column_array_[column_cnt_++] = sec_column_array_[i];
    }
  }
  return ret;
}

const ObRlsSecColumnSchema* ObRlsPolicySchema::get_sec_column_by_idx(const int64_t idx) const
{
  const ObRlsSecColumnSchema *column = NULL;
  if (idx < 0 || idx >= column_cnt_) {
    column = NULL;
  } else {
    column = sec_column_array_[idx];
  }
  return column;
}

OB_DEF_SERIALIZE(ObRlsPolicySchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              rls_policy_id_,
              schema_version_,
              table_id_,
              rls_group_id_,
              stmt_type_,
              check_opt_,
              enable_flag_,
              policy_name_,
              policy_function_schema_,
              policy_package_name_,
              policy_function_name_);
  OZ (serialization::encode_vi64(buf, buf_len, pos, column_cnt_));
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    if (OB_ISNULL(sec_column_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls sec column is null", KR(ret));
    } else if (OB_FAIL(sec_column_array_[i]->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize sec column", KR(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRlsPolicySchema)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t count = 0;
  ObRlsSecColumnSchema sec_column;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              rls_policy_id_,
              schema_version_,
              table_id_,
              rls_group_id_,
              stmt_type_,
              check_opt_,
              enable_flag_,
              policy_name_,
              policy_function_schema_,
              policy_package_name_,
              policy_function_name_);
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("failed to decode column count", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    sec_column.reset();
    if (OB_FAIL(sec_column.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize sec column", KR(ret));
    } else if (OB_FAIL(add_sec_column(sec_column))) {
      LOG_WARN("failed to add sec column", KR(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRlsPolicySchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              rls_policy_id_,
              schema_version_,
              table_id_,
              rls_group_id_,
              stmt_type_,
              check_opt_,
              enable_flag_,
              policy_name_,
              policy_function_schema_,
              policy_package_name_,
              policy_function_name_);
  //get columms size
  len += serialization::encoded_length_vi64(column_cnt_);
  for (int64_t i = 0; i < column_cnt_; ++i) {
    if (NULL != sec_column_array_[i]) {
      len += sec_column_array_[i]->get_serialize_size();
    }
  }
  return len;
}

ObRlsGroupSchema::ObRlsGroupSchema()
  : ObSchema()
{
  reset();
}

ObRlsGroupSchema::ObRlsGroupSchema(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObRlsGroupSchema::~ObRlsGroupSchema()
{
}

ObRlsGroupSchema::ObRlsGroupSchema(const ObRlsGroupSchema &other)
  : ObSchema()
{
  *this = other;
}

ObRlsGroupSchema &ObRlsGroupSchema::operator=(const ObRlsGroupSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    rls_group_id_ = other.rls_group_id_;
    schema_version_ = other.schema_version_;
    table_id_ = other.table_id_;

    if (OB_FAIL(set_policy_group_name(other.policy_group_name_))) {
      LOG_WARN("Fail to deep copy policy group name", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObRlsGroupSchema::assign(const ObRlsGroupSchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObRlsGroupSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(rls_group_id_)
      || !is_valid_id(table_id_)
      || policy_group_name_.empty()) {
    ret = false;
  }
  return ret;
}

void ObRlsGroupSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  rls_group_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  table_id_ = OB_INVALID_ID;
  policy_group_name_.reset();
  ObSchema::reset();
}

int64_t ObRlsGroupSchema::get_convert_size() const
{
  return sizeof(ObRlsGroupSchema)
       + policy_group_name_.length() + 1;
}

OB_SERIALIZE_MEMBER(ObRlsGroupSchema,
                    tenant_id_,
                    rls_group_id_,
                    schema_version_,
                    table_id_,
                    policy_group_name_);

ObRlsContextSchema::ObRlsContextSchema()
  : ObSchema()
{
  reset();
}

ObRlsContextSchema::ObRlsContextSchema(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObRlsContextSchema::~ObRlsContextSchema()
{
}

ObRlsContextSchema::ObRlsContextSchema(const ObRlsContextSchema &other)
  : ObSchema()
{
  *this = other;
}

ObRlsContextSchema &ObRlsContextSchema::operator=(const ObRlsContextSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    rls_context_id_ = other.rls_context_id_;
    schema_version_ = other.schema_version_;
    table_id_ = other.table_id_;

    if (OB_FAIL(set_context_name(other.context_name_))) {
      LOG_WARN("Fail to deep copy context name", K(ret));
    } else if (OB_FAIL(set_attribute(other.attribute_))) {
      LOG_WARN("Fail to deep copy attribute", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObRlsContextSchema::assign(const ObRlsContextSchema &other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObRlsContextSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(rls_context_id_)
      || !is_valid_id(table_id_)
      || context_name_.empty()
      || attribute_.empty()) {
    ret = false;
  }
  return ret;
}

void ObRlsContextSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  rls_context_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  table_id_ = OB_INVALID_ID;
  context_name_.reset();
  attribute_.reset();
  ObSchema::reset();
}

int64_t ObRlsContextSchema::get_convert_size() const
{
  return sizeof(ObRlsContextSchema)
       + context_name_.length() + 1
       + attribute_.length() + 1;
}

OB_SERIALIZE_MEMBER(ObRlsContextSchema,
                    tenant_id_,
                    rls_context_id_,
                    schema_version_,
                    table_id_,
                    context_name_,
                    attribute_);

// ObColumnGroupSchema
OB_DEF_SERIALIZE(ObColumnGroupSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              column_group_id_,
              column_group_name_,
              column_group_type_,
              schema_version_,
              block_size_,
              compressor_type_,
              row_store_type_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, column_id_cnt_))) {
      LOG_WARN("fail to encode column_id_cnt", KR(ret), K_(column_id_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < column_id_cnt_); ++i) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, column_id_arr_[i]))) {
        LOG_WARN("fail to encode column_id", KR(ret), K(column_id_arr_[i]), K(i), K_(column_id_cnt));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObColumnGroupSchema)
{
  int ret = OB_SUCCESS;
  ObString column_group_name;
  LST_DO_CODE(OB_UNIS_DECODE,
              column_group_id_,
              column_group_name,
              column_group_type_,
              schema_version_,
              block_size_,
              compressor_type_,
              row_store_type_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_str(column_group_name, column_group_name_))) {
    LOG_WARN("fail to deep copy column_group_name", KR(ret), K(column_group_name));
  } else {
    int64_t column_id_count = 0;
    OB_UNIS_DECODE(column_id_count);
    if (OB_SUCC(ret) && column_id_count > 0) {
      const int64_t arr_size = sizeof(uint64_t) * column_id_count;
      column_id_arr_ = static_cast<uint64_t*>(alloc(arr_size));
      if (OB_ISNULL(column_id_arr_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for column_id_array", KR(ret), K(column_id_count), K(arr_size));
      } else {
        MEMSET(column_id_arr_, 0, sizeof(uint64_t) * column_id_count);
        column_id_arr_capacity_ = column_id_count;

        uint64_t column_id = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (i < column_id_count); ++i) {
          column_id = 0;
          if (OB_FAIL(serialization::decode(buf, data_len, pos, column_id))) {
            LOG_WARN("fail to deserialize column_id", KR(ret), K(i));
          } else if (OB_FAIL(add_column_id(column_id))) {
            LOG_WARN("fail to add column_id", KR(ret), K(i), K(column_id), K(column_id_count),
              K_(column_id_cnt), K_(column_id_arr_capacity));
          }
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObColumnGroupSchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              column_group_id_,
              column_group_name_,
              column_group_type_,
              schema_version_,
              block_size_,
              compressor_type_,
              row_store_type_);

  len += serialization::encoded_length_vi64(column_id_cnt_);
  for (int64_t i = 0; i < column_id_cnt_; ++i) {
    len += serialization::encoded_length_vi64(column_id_arr_[i]);
  }
  return len;
}

ObColumnGroupSchema::ObColumnGroupSchema()
  : ObSchema()
{
  reset();
}

ObColumnGroupSchema::ObColumnGroupSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObColumnGroupSchema::ObColumnGroupSchema(const ObColumnGroupSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObColumnGroupSchema::~ObColumnGroupSchema()
{
}

ObColumnGroupSchema& ObColumnGroupSchema::operator =(const ObColumnGroupSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_column_group_id(src_schema.column_group_id_);
    set_column_group_type(src_schema.column_group_type_);
    set_schema_version(src_schema.schema_version_);
    set_block_size(src_schema.block_size_);
    set_compressor_type(src_schema.compressor_type_);
    set_row_store_type(src_schema.row_store_type_);
    if (OB_FAIL(set_column_group_name(src_schema.column_group_name_))) {
      LOG_WARN("fail to set column group name", KR(ret), K(src_schema));
    } else {
      // column_id_cnt_ will increase when add_column_id()
      const int64_t column_id_cnt = src_schema.get_column_id_count();
      if (OB_SUCCESS == ret && column_id_cnt > 0) {
        const int64_t arr_size = sizeof(uint64_t) * column_id_cnt;
        column_id_arr_ = static_cast<uint64_t*>(alloc(arr_size));
        if (OB_ISNULL(column_id_arr_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to allocate memory for column_id_array", KR(ret), K(column_id_cnt), K(arr_size));
        } else {
          MEMSET(column_id_arr_, 0, sizeof(uint64_t) * column_id_cnt);
          column_id_arr_capacity_ = column_id_cnt;
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < column_id_cnt); ++i) {
        uint64_t tmp_column_id = 0;
        if (OB_FAIL(src_schema.get_column_id(i, tmp_column_id))) {
          LOG_WARN("fail to get column_id from src_schema", KR(ret), K(i), K(src_schema));
        } else if (OB_FAIL(add_column_id(tmp_column_id))) {
          LOG_WARN("fail to add column_id", KR(ret), K(i), K(tmp_column_id), K(column_id_cnt));
        }
      }
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObColumnGroupSchema::assign(const ObColumnGroupSchema &src_schema)
{
  int ret = OB_SUCCESS;
  *this = src_schema;
  ret = get_err_ret();
  return ret;
}

void ObColumnGroupSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  column_group_id_ = OB_INVALID_ID;
  column_group_type_ = ObColumnGroupType::MAX_COLUMN_GROUP;
  schema_version_ = OB_INVALID_VERSION;
  block_size_ = 0;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
  column_id_cnt_ = 0;
  column_id_arr_capacity_ = 0;
  column_id_arr_ = NULL;
  reset_string(column_group_name_);
  ObSchema::reset();
}

bool ObColumnGroupSchema::is_valid() const
{
  return !((column_group_id_ == OB_INVALID_ID)
         || (column_group_type_ == ObColumnGroupType::MAX_COLUMN_GROUP)
         || (column_group_name_.empty()));
}

int64_t ObColumnGroupSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObColumnGroupSchema);
  convert_size += column_id_cnt_ * sizeof(uint64_t);
  convert_size += column_group_name_.length() + 1;
  return convert_size;
}

int ObColumnGroupSchema::add_column_id(const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (0 == column_id_arr_capacity_) {
    const int64_t arr_size = sizeof(uint64_t) * DEFAULT_COLUMN_ID_ARRAY_CAPACITY;
    if (OB_ISNULL(column_id_arr_ = static_cast<uint64_t*>(alloc(arr_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory for column_id_arr", KR(ret), K(arr_size));
    } else {
      column_id_arr_capacity_ = DEFAULT_COLUMN_ID_ARRAY_CAPACITY;
      MEMSET(column_id_arr_, 0, arr_size);
    }
  } else if (column_id_cnt_ >= column_id_arr_capacity_) {
    int64_t tmp_capacity = 2 * column_id_arr_capacity_;
    uint64_t *tmp_arr = NULL;
    if (OB_ISNULL(tmp_arr = static_cast<uint64_t*>(
        alloc(sizeof(uint64_t) * tmp_capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory for column_id_arr", KR(ret), K(tmp_capacity));
    } else {
      MEMCPY(tmp_arr, column_id_arr_, sizeof(uint64_t) * column_id_arr_capacity_);
      // free old column_id_arr_
      free(column_id_arr_);
      column_id_arr_ = tmp_arr;
      column_id_arr_capacity_ = tmp_capacity;
    }
  }

  if (OB_SUCC(ret)) {
    // check column_id exist or not
    bool exist = false;
    for (int64_t i = 0; (i < column_id_cnt_) && (!exist); ++i) {
      if (column_id == column_id_arr_[i]) {
        exist = true;
      }
    }
    if (exist) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column_id should be unique", KR(ret), K(column_id));
    } else {
      column_id_arr_[column_id_cnt_++] = column_id;
    }
  }

  return ret;
}

int ObColumnGroupSchema::get_column_id(const int64_t idx, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  column_id = 0;
  if (idx >= column_id_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idx), K_(column_id_cnt));
  } else {
    column_id = column_id_arr_[idx];
  }
  return ret;
}

int ObColumnGroupSchema::remove_column_id(const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; i < column_id_cnt_; ++i) {
    if (column_id == column_id_arr_[i]) {
      idx = i;
      break;
    }
  }

  if (OB_INVALID_INDEX == idx) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    for (int64_t i = idx; i < column_id_cnt_ - 1; ++i) {
      column_id_arr_[i] = column_id_arr_[i + 1];
    }
    --column_id_cnt_;
  }
  return ret;
}

void ObColumnGroupSchema::remove_all_cols() {
  column_id_cnt_ = 0;
  MEMSET(column_id_arr_, 0, sizeof(uint64_t) * column_id_arr_capacity_);
}

int ObColumnGroupSchema::get_column_group_type_name(ObString &readable_cg_name) const
{
  int ret = OB_SUCCESS;
  if (column_group_type_ >=  ObColumnGroupType::NORMAL_COLUMN_GROUP ||
      column_group_type_ < ObColumnGroupType::DEFAULT_COLUMN_GROUP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("receive not suppoted column group type", K(ret), K(column_group_type_));
  } else {
    /* use column group type as index, and check whether out of range*/
    const char* readable_name = OB_COLUMN_GROUP_TYPE_NAME[column_group_type_];
    const int32_t readable_name_len = static_cast<int32_t>(strlen(readable_name));
    if (readable_name_len != readable_cg_name.write(readable_name, readable_name_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to wriet column group name, check whether buffer size enough", K(ret), K(readable_cg_name));
    }
  }
  return ret;
}
/*
The following function is used by partition exchange to compare whether cg-level attributes are the same and three attributes are not considered.
1、column_group_name: The same column group in the two tables may have different names specified by the user, so no comparison is required.
2、column_group_id: Adding and deleting column groups multiple times to the same table will cause the column group id to increase, and the rules are similar to column ids. Therefore, in two tables, the same column group may have different column group ids.
3、column id contained in column group: Since in the comparison of columns in two tables, the column id of the same column is not necessarily the same, therefore, the column id in the two column groups cannot distinguish whether they are the same column. You need to use the column id to get the column schema and then compare the column attributes in sequence.
*/
bool ObColumnGroupSchema::has_same_column_group_attributes_for_part_exchange(const ObColumnGroupSchema &other) const
{
  return column_group_type_ == other.get_column_group_type() &&
         block_size_ == other.get_block_size() &&
         compressor_type_ == other.get_compressor_type() &&
         row_store_type_ == other.get_row_store_type() &&
         column_id_cnt_ == other.get_column_id_count();
}

OB_DEF_SERIALIZE(ObSkipIndexColumnAttr)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, pack_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSkipIndexColumnAttr)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, pack_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSkipIndexColumnAttr)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, pack_);
  return len;
}

OB_DEF_SERIALIZE(ObSkipIndexAttrWithId)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, col_idx_, skip_idx_attr_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSkipIndexAttrWithId)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, col_idx_, skip_idx_attr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSkipIndexAttrWithId)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, col_idx_, skip_idx_attr_);
  return len;
}

ObTableLatestSchemaVersion::ObTableLatestSchemaVersion()
    : table_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      is_deleted_(false)
{
}

void ObTableLatestSchemaVersion::reset()
{
  table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  is_deleted_ = false;
}

int ObTableLatestSchemaVersion::init(
    const uint64_t table_id,
    const int64_t schema_version,
    const bool is_deleted)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id || OB_INVALID_VERSION == schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id), K(schema_version), K(is_deleted));
  } else {
    table_id_ = table_id;
    schema_version_ = schema_version;
    is_deleted_ = is_deleted;
  }
  return ret;
}

bool ObTableLatestSchemaVersion::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_VERSION != schema_version_;
}

int ObTableLatestSchemaVersion::assign(const ObTableLatestSchemaVersion &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    table_id_ = other.table_id_;
    schema_version_ = other.schema_version_;
    is_deleted_ = other.is_deleted_;
  }
  return ret;
}

int ObForeignKeyInfo::get_child_column_id(const uint64_t parent_column_id, uint64_t &child_column_id) const
{
  int ret = OB_SUCCESS;
  child_column_id = OB_INVALID_ID;
  if (parent_column_ids_.count() == child_column_ids_.count()) {
    for (int64_t i = 0; i < parent_column_ids_.count(); ++i) {
      if (parent_column_ids_.at(i) == parent_column_id) {
        child_column_id = child_column_ids_.at(i);
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The number of parent key columns and foreign key columns is different",
              K(ret), K(parent_column_ids_.count()), K(child_column_ids_.count()));
  }

  if (OB_SUCC(ret) && OB_INVALID_ID == child_column_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not find corresponding child column id", K(ret), K(parent_column_id));
  }
  return ret;
}

int ObForeignKeyInfo::get_parent_column_id(const uint64_t child_column_id, uint64_t &parent_column_id) const
{
  int ret = OB_SUCCESS;
  parent_column_id = OB_INVALID_ID;
  if (parent_column_ids_.count() == child_column_ids_.count()) {
    for (int64_t i = 0; i < child_column_ids_.count(); ++i) {
      if (child_column_ids_.at(i) == child_column_id) {
        parent_column_id = parent_column_ids_.at(i);
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The number of parent key columns and foreign key columns is different",
              K(ret), K(parent_column_ids_.count()), K(child_column_ids_.count()));
  }

  if (OB_SUCC(ret) && OB_INVALID_ID == parent_column_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not find corresponding parent column id", K(ret), K(child_column_id));
  }
 return ret;
}

ObIndexSchemaHashWrapper GetIndexNameKey<ObIndexSchemaHashWrapper, ObIndexNameInfo*>::operator()(
  const ObIndexNameInfo *index_name_info) const
{
  if (OB_NOT_NULL(index_name_info)) {
    bool is_oracle_mode = false;
    if (OB_UNLIKELY(OB_SUCCESS != ObCompatModeGetter::check_is_oracle_mode_with_table_id(
        index_name_info->get_tenant_id(), index_name_info->get_index_id(), is_oracle_mode))) {
      ObIndexSchemaHashWrapper null_wrap;
      return null_wrap;
    } else if (is_recyclebin_database_id(index_name_info->get_database_id())) {
      ObIndexSchemaHashWrapper index_schema_hash_wrapper(
          index_name_info->get_tenant_id(),
          index_name_info->get_database_id(),
          common::OB_INVALID_ID,
          index_name_info->get_index_name());
      return index_schema_hash_wrapper;
    } else {
      ObIndexSchemaHashWrapper index_schema_hash_wrapper(
          index_name_info->get_tenant_id(),
          index_name_info->get_database_id(),
          is_oracle_mode ? common::OB_INVALID_ID : index_name_info->get_data_table_id(),
          index_name_info->get_original_index_name());
      return index_schema_hash_wrapper;
    }
  } else {
    ObIndexSchemaHashWrapper null_wrap;
    return null_wrap;
  }
}

ObIndexNameInfo::ObIndexNameInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    database_id_(OB_INVALID_ID),
    data_table_id_(OB_INVALID_ID),
    index_id_(OB_INVALID_ID),
    index_name_(),
    original_index_name_()
{
}

void ObIndexNameInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  database_id_ = OB_INVALID_ID;
  data_table_id_ = OB_INVALID_ID;
  index_id_ = OB_INVALID_ID;
  index_name_.reset();
  original_index_name_.reset();
}

int ObIndexNameInfo::init(
    common::ObIAllocator &allocator,
    const share::schema::ObSimpleTableSchemaV2 &index_schema)
{
  int ret = OB_SUCCESS;
  reset();
  const bool c_style = true;
  if (OB_FAIL(ob_write_string(allocator,
      index_schema.get_table_name_str(), index_name_, c_style))) {
    LOG_WARN("fail to write string", KR(ret), K(index_schema));
  } else {
    tenant_id_ = index_schema.get_tenant_id();
    database_id_ = index_schema.get_database_id();
    data_table_id_ = index_schema.get_data_table_id();
    index_id_ = index_schema.get_table_id();
    // use shallow copy to reduce memory allocation
    if (is_recyclebin_database_id(database_id_)) {
      original_index_name_ = index_name_;
    } else {
      if (OB_FAIL(ObSimpleTableSchemaV2::get_index_name(index_name_, original_index_name_))) {
        LOG_WARN("fail to generate index name", KR(ret), K_(index_name));
      }
    }
  }
  return ret;
}

template<class T>
int ObLocalSessionVar::set_local_vars(T &var_array)
{
  int ret = OB_SUCCESS;
  if (!local_session_vars_.empty()) {
    local_session_vars_.reset();
  }
  if (OB_FAIL(local_session_vars_.reserve(var_array.count()))) {
    LOG_WARN("fail to reserve for local_session_vars", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < var_array.count(); ++i) {
      if (OB_FAIL(add_local_var(var_array.at(i)))) {
        LOG_WARN("fail to add session var", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::add_local_var(const ObSessionSysVar *var)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(var));
  } else if (OB_FAIL(add_local_var(var->type_, var->val_))) {
    LOG_WARN("fail to add local session var", K(ret));
  }
  return ret;
}

int ObLocalSessionVar::add_local_var(ObSysVarClassType var_type, const ObObj &value)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *cur_var = NULL;
  if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(alloc_));
  } else if (OB_FAIL(get_local_var(var_type, cur_var))) {
    LOG_WARN("get local var failed", K(ret));
  } else if (NULL == cur_var) {
    ObSessionSysVar *new_var = OB_NEWx(ObSessionSysVar, alloc_);
    if (OB_ISNULL(new_var)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc new var failed.", K(ret));
    } else if (OB_FAIL(local_session_vars_.push_back(new_var))) {
      LOG_WARN("push back new var failed", K(ret));
    } else if (OB_FAIL(deep_copy_obj(*alloc_, value, new_var->val_))) {
      LOG_WARN("fail to deep copy obj", K(ret));
    } else {
      new_var->type_ = var_type;
    }
  } else if (!cur_var->is_equal(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local session var added before is not equal to the new var", K(ret), KPC(cur_var), K(value));
  }
  return ret;
}

int ObLocalSessionVar::get_local_var(ObSysVarClassType var_type, ObSessionSysVar *&sys_var) const
{
  int ret = OB_SUCCESS;
  sys_var = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == sys_var && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(local_session_vars_));
    } else if (local_session_vars_.at(i)->type_ == var_type) {
      sys_var = local_session_vars_.at(i);
    }
  }
  return ret;
}

int ObLocalSessionVar::get_local_vars(ObIArray<const ObSessionSysVar *> &var_array) const
{
  int ret = OB_SUCCESS;
  var_array.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i){
    if (OB_FAIL(var_array.push_back(local_session_vars_.at(i)))) {
      LOG_WARN("push back local session vars failed", K(ret));
    }
  }
  return ret;
}

int ObLocalSessionVar::remove_vars_same_with_session(const sql::ObBasicSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(session));
  } else {
    ObSEArray<share::schema::ObSessionSysVar *, 4> new_var_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
      ObSessionSysVar *local_var = local_session_vars_.at(i);
      ObObj session_val;
      if (OB_ISNULL(local_var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KP(local_var));
      } else if (SYS_VAR_SQL_MODE == local_var->type_) {
        if (local_var->val_.get_uint64() != session->get_sql_mode()
            && OB_FAIL(new_var_array.push_back(local_var))) {
          LOG_WARN("fail to push into new var array", K(ret));
        }
      } else if (OB_FAIL(session->get_sys_variable(local_var->type_, session_val))) {
        LOG_WARN("fail to get session variable", K(ret));
      } else if (!local_var->is_equal(session_val) &&
                  OB_FAIL(new_var_array.push_back(local_var))) {
        LOG_WARN("fail to push into new var array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(local_session_vars_.assign(new_var_array))) {
        LOG_WARN("fail to set local session vars.", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::deep_copy(const ObLocalSessionVar &other)
{
  int ret = OB_SUCCESS;
  local_session_vars_.reset();
  if (this == &other) {
    //do nothing
  } else if (NULL != other.alloc_) {
    if (NULL == alloc_) {
      alloc_ = other.alloc_;
      local_session_vars_.set_allocator(other.alloc_);
    }
  }
  if (OB_FAIL(set_local_vars(other.local_session_vars_))) {
    LOG_WARN("fail to add session var", K(ret));
  }
  return ret;
}

int ObLocalSessionVar::deep_copy_self()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator", K(ret));
  } else {
    ObSEArray<const share::schema::ObSessionSysVar *, 4> var_array;
    if (OB_FAIL(get_local_vars(var_array))) {
      LOG_WARN("get local vars failed", K(ret));
    } else if (OB_FAIL(set_local_vars(var_array))) {
      LOG_WARN("set local vars failed", K(ret));
    }
  }
  return ret;
}

int ObLocalSessionVar::assign(const ObLocalSessionVar &other)
{
  int ret = OB_SUCCESS;
  local_session_vars_.reset();
  if (NULL != other.alloc_) {
    if (NULL == alloc_) {
      alloc_ = other.alloc_;
      local_session_vars_.set_allocator(other.alloc_);
    }
    if (OB_FAIL(local_session_vars_.reserve(other.local_session_vars_.count()))) {
      LOG_WARN("reserve failed", K(ret));
    } else if (OB_FAIL(local_session_vars_.assign(other.local_session_vars_))) {
      LOG_WARN("fail to push back local var", K(ret));
    }
  } else {
    //do nothing, other is not inited
  }
  return ret;
}

void ObLocalSessionVar::reset()
{
  local_session_vars_.reset();
}

int ObLocalSessionVar::set_local_var_capacity(int64_t sz)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(local_session_vars_.reserve(sz))) {
    LOG_WARN("reserve failed", K(ret), K(sz));
  }
  return ret;
}

bool ObLocalSessionVar::operator == (const ObLocalSessionVar& other) const
{
  bool is_equal = local_session_vars_.count() == other.local_session_vars_.count();
  if (is_equal) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; is_equal && i < local_session_vars_.count(); ++i) {
      ObSessionSysVar *var = local_session_vars_.at(i);
      ObSessionSysVar *other_val = NULL; ;
      if (OB_ISNULL(var)) {
       is_equal = false;
      } else if ((tmp_ret = other.get_local_var(var->type_, other_val)) != OB_SUCCESS) {
        is_equal = false;
      } else if (other_val == NULL) {
        is_equal = false;
      } else {
        is_equal = var->is_equal(other_val->val_);
      }
    }
  }
  return is_equal;
}

int64_t ObLocalSessionVar::get_deep_copy_size() const
{
  int64_t sz = sizeof(*this) + local_session_vars_.count() * sizeof(ObSessionSysVar *);
  for (int64_t i = 0; i < local_session_vars_.count(); ++i) {
    if (OB_NOT_NULL(local_session_vars_.at(i))) {
      sz += local_session_vars_.at(i)->get_deep_copy_size();
    }
  }
  return sz;
}

bool ObSessionSysVar::is_equal(const ObObj &other) const
{
  bool bool_ret = false;
  if (val_.get_meta() != other.get_meta()) {
    bool_ret = false;
    if (ob_is_string_type(val_.get_type())
        && val_.get_type() == other.get_type()
        && val_.get_collation_type() != other.get_collation_type()) {
      //the collation type of string system variables will be set to the current connection collation type after updating values.
      //return true if the string values are equal.
      bool_ret = common::ObCharset::case_sensitive_equal(val_.get_string(), other.get_string());
    }
  } else if (val_.is_equal(other, CS_TYPE_BINARY)) {
    bool_ret = true;
  }
  return bool_ret;
}

OB_DEF_SERIALIZE(ObSessionSysVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, type_, val_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSessionSysVar)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, type_, val_);
  return len;
}

OB_DEF_DESERIALIZE(ObSessionSysVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, type_, val_);
  return ret;
}

int64_t ObSessionSysVar::get_deep_copy_size() const {
  int64_t sz = sizeof(*this) + val_.get_deep_copy_size();
  return sz;
}

const ObSysVarClassType ObLocalSessionVar::ALL_LOCAL_VARS[] = {
  SYS_VAR_TIME_ZONE,
  SYS_VAR_SQL_MODE,
  SYS_VAR_NLS_DATE_FORMAT,
  SYS_VAR_NLS_TIMESTAMP_FORMAT,
  SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
  SYS_VAR_COLLATION_CONNECTION,
  SYS_VAR_MAX_ALLOWED_PACKET
};

//add all vars that can be solidified
int ObLocalSessionVar::load_session_vars(const sql::ObBasicSessionInfo *session) {
  int ret = OB_SUCCESS;
  int64_t var_num = sizeof(ALL_LOCAL_VARS) / sizeof(ObSysVarClassType);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (!local_session_vars_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local_session_vars can only be inited once", K(ret));
  } else if (OB_FAIL(local_session_vars_.reserve(var_num))) {
    LOG_WARN("reserve failed", K(ret), K(var_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < var_num; ++i) {
      ObObj var;
      if (OB_FAIL(session->get_sys_variable(ALL_LOCAL_VARS[i], var))) {
        LOG_WARN("fail to get session variable", K(ret));
      } else if (OB_FAIL(add_local_var(ALL_LOCAL_VARS[i], var))) {
        LOG_WARN("fail to add session var", K(ret), K(var));
      }
    }
  }
  return ret;
}

int ObLocalSessionVar::update_session_vars_with_local(sql::ObBasicSessionInfo &session) const {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(session.update_sys_variable(local_session_vars_.at(i)->type_, local_session_vars_.at(i)->val_))) {
      LOG_WARN("fail to update sys variable", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObLocalSessionVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, local_session_vars_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < local_session_vars_.count(); ++i) {
    if (OB_ISNULL(local_session_vars_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      LST_DO_CODE(OB_UNIS_ENCODE, *local_session_vars_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLocalSessionVar)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, local_session_vars_.count());
  for (int64_t i = 0; i < local_session_vars_.count(); ++i) {
    if (OB_NOT_NULL(local_session_vars_.at(i))) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, *local_session_vars_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObLocalSessionVar)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  OB_UNIS_DECODE(cnt);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(local_session_vars_.reserve(cnt))) {
      LOG_WARN("reserve failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    ObSessionSysVar var;
    LST_DO_CODE(OB_UNIS_DECODE, var);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_local_var(&var))) {
        LOG_WARN("fail to add local session var", K(ret));
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObLocalSessionVar)
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < local_session_vars_.count(); ++i) {
    if (i > 0) {
      J_COMMA();
    }
    if (OB_NOT_NULL(local_session_vars_.at(i))) {
      J_KV("type", local_session_vars_.at(i)->type_,
            "val", local_session_vars_.at(i)->val_);
    }
  }
  J_OBJ_END();
  return pos;
}

//
//
} //namespace schema
} //namespace share
} //namespace oceanbase
