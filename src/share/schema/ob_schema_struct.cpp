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
#include "ob_schema_mgr.h"  // ObSimpleTableSchemaV2
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"
#include "lib/number/ob_number_v2.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_primary_zone_util.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_parser.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_leader_coordinator.h"
#include "rootserver/ob_root_utils.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
namespace oceanbase {
namespace share {
namespace schema {
using namespace common;
using namespace sql;
using namespace rootserver;

uint64_t ObSysTableChecker::TableNameWrapper::hash() const
{
  uint64_t hash_ret = 0;
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::ObCharset::hash(cs_type, table_name_, hash_ret);
  return hash_ret;
}

bool ObSysTableChecker::TableNameWrapper::operator==(const TableNameWrapper& rv) const
{
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  return (database_id_ == rv.database_id_) && (name_case_mode_ == rv.name_case_mode_) &&
         (0 == common::ObCharset::strcmp(cs_type, table_name_, rv.table_name_));
}

ObSysTableChecker::ObSysTableChecker()
    : tenant_space_table_id_map_(),
      sys_table_name_map_(),
      tenant_space_sys_table_num_(0),
      allocator_(),
      is_inited_(false)
{}

ObSysTableChecker::~ObSysTableChecker()
{
  destroy();
}

ObSysTableChecker& ObSysTableChecker::instance()
{
  static ObSysTableChecker tenant_space_table_checker;
  return tenant_space_table_checker;
}

int ObSysTableChecker::is_tenant_space_table_id(const uint64_t table_id, bool& is_tenant_space_table)
{
  return instance().check_tenant_space_table_id(table_id, is_tenant_space_table);
}

int ObSysTableChecker::is_sys_table_name(
    const uint64_t database_id, const ObString& table_name, bool& is_sys_table_name)
{
  return instance().check_sys_table_name(database_id, table_name, is_sys_table_name);
}

int ObSysTableChecker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // do nothing
  } else if (OB_FAIL(tenant_space_table_id_map_.create(
                 TABLE_BUCKET_NUM, ObModIds::OB_TENANT_SPACE_TABLE_ID_SET, ObModIds::OB_TENANT_SPACE_TABLE_ID_SET))) {
    LOG_WARN("fail to create tenant_space_table_id_map", K(ret));
  } else if (OB_FAIL(sys_table_name_map_.create(
                 TABLE_BUCKET_NUM, ObModIds::OB_SYS_TABLE_NAME_MAP, ObModIds::OB_SYS_TABLE_NAME_MAP))) {
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
  const schema_create_func all_core_table_schema_creator[] = {&share::ObInnerTableSchema::all_core_table_schema, NULL};
  const schema_create_func* creator_ptr_array[] = {all_core_table_schema_creator,
      share::core_table_schema_creators,
      share::sys_table_schema_creators,
      share::virtual_table_schema_creators,
      share::sys_view_schema_creators,
      NULL};

  int back_ret = OB_SUCCESS;
  ObTableSchema table_schema;
  ObNameCaseMode mode = OB_ORIGIN_AND_INSENSITIVE;
  ObString table_name;
  for (const schema_create_func** creator_ptr_ptr = creator_ptr_array; OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
       ++creator_ptr_ptr) {
    for (const schema_create_func* creator_ptr = *creator_ptr_ptr; OB_SUCCESS == ret && NULL != *creator_ptr;
         ++creator_ptr) {
      table_schema.reset();
      if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_WARN("create table schema failed", K(ret));
      } else if (OB_FAIL(ob_write_string(table_schema.get_table_name(), table_name))) {
        LOG_WARN("fail to write table name", K(ret), K(table_schema));
      } else {
        uint64_t database_id = extract_pure_id(table_schema.get_database_id());
        TableNameWrapper table(database_id, mode, table_name);
        uint64_t key = table.hash();
        TableNameWrapperArray* value = NULL;
        ret = sys_table_name_map_.get_refactored(key, value);
        if (OB_HASH_NOT_EXIST == ret) {
          void* buffer = NULL;
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
            LOG_INFO("set tenant space table name", K(key), K(table));
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
    FOREACH(it, sys_table_name_map_)
    {
      TableNameWrapperArray* array = it->second;
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

int ObSysTableChecker::check_tenant_space_table_id(const uint64_t table_id, bool& is_tenant_space_table)
{
  int ret = OB_SUCCESS;
  uint64_t pure_table_id = extract_pure_id(table_id);
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
    const uint64_t database_id, const ObString& table_name, bool& is_system_table)
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
    uint64_t db_id = extract_pure_id(database_id);
    const TableNameWrapper table(db_id, mode, table_name);
    uint64_t key = table.hash();
    TableNameWrapperArray* value = NULL;
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
    LOG_DEBUG("check sys table name", K(ret), K(key), K(table));
  }
  return ret;
}

// which should create tenant space table while in upgrade phase
bool ObSysTableChecker::is_tenant_table_in_version_2200(const uint64_t table_id)
{
  int64_t pure_table_id = extract_pure_id(table_id);
  bool finded = false;
  switch (pure_table_id) {
#define MIGRATE_TABLE_BEFORE_2200_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef MIGRATE_TABLE_BEFORE_2200_SWITCH
    {
      finded = true;
      break;
    }
    default: {
      finded = false;
      break;
    }
  }
  return finded;
}

bool ObSysTableChecker::is_cluster_private_tenant_table(const uint64_t table_id)
{
  bool bret = false;
  uint64_t pure_id = extract_pure_id(table_id);
  switch (pure_id) {
#define CLUSTER_PRIVATE_TABLE_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef CLUSTER_PRIVATE_TABLE_SWITCH
    {
      bret = true;
      break;
    }
    default: {
      bret = false;
      break;
    }
  }
  return bret;
}

bool ObSysTableChecker::is_backup_private_tenant_table(const uint64_t table_id)
{
  bool bret = false;
  uint64_t pure_id = extract_pure_id(table_id);
  switch (pure_id) {
#define BACKUP_PRIVATE_TABLE_SWITCH
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef BACKUP_PRIVATE_TABLE_SWITCH
    {
      bret = true;
      break;
    }
    default: {
      bret = false;
      break;
    }
  }
  return bret;
}

bool ObSysTableChecker::is_rs_restart_related_table_id(const uint64_t table_id)
{
  uint64_t pure_id = extract_pure_id(table_id);
  bool bret = false;
  if (OB_SYS_TENANT_ID == extract_tenant_id(table_id) || OB_INVALID_TENANT_ID == extract_tenant_id(table_id)) {
    switch (pure_id) {
#define RS_RESTART_RELATED
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef RS_RESTART_RELATED
      {
        bret = true;
        break;
      }
      default: {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

// The system table partition that rs starts to depend on. This set affects the recovery speed of RS when the network is
// partitioned. The smaller the set, the better. As of version 2.2.30, the collection has about 120+ partitions
bool ObSysTableChecker::is_rs_restart_related_partition(const uint64_t table_id, const int64_t partition_id)
{
  bool bret = is_rs_restart_related_table_id(table_id);
  if (bret) {
    uint64_t pure_id = extract_pure_id(table_id);
    switch (pure_id) {
      // In the following table, due to the schema split, the system tenants still retain multiple partitions.
      // The actual RS startup only depends on one of the partitions. Special processing is done here.
      case OB_ALL_COLUMN_TID:
      case OB_ALL_COLUMN_HISTORY_TID:
      case OB_ALL_CONSTRAINT_TID:
      case OB_ALL_CONSTRAINT_HISTORY_TID:
      case OB_ALL_DATABASE_TID:
      case OB_ALL_DATABASE_HISTORY_TID:
      case OB_ALL_DATABASE_PRIVILEGE_TID:
      case OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID:
      case OB_ALL_DEF_SUB_PART_TID:
      case OB_ALL_DEF_SUB_PART_HISTORY_TID:
      case OB_ALL_FOREIGN_KEY_TID:
      case OB_ALL_FOREIGN_KEY_COLUMN_TID:
      case OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID:
      case OB_ALL_FOREIGN_KEY_HISTORY_TID:
      case OB_ALL_FUNC_TID:
      case OB_ALL_FUNC_HISTORY_TID:
      case OB_ALL_OUTLINE_TID:
      case OB_ALL_OUTLINE_HISTORY_TID:
      case OB_ALL_PACKAGE_TID:
      case OB_ALL_PACKAGE_HISTORY_TID:
      case OB_ALL_PART_TID:
      case OB_ALL_PART_HISTORY_TID:
      case OB_ALL_ROUTINE_TID:
      case OB_ALL_ROUTINE_HISTORY_TID:
      case OB_ALL_ROUTINE_PARAM_TID:
      case OB_ALL_ROUTINE_PARAM_HISTORY_TID:
      case OB_ALL_SEQUENCE_OBJECT_TID:
      case OB_ALL_SEQUENCE_OBJECT_HISTORY_TID:
      case OB_ALL_SYNONYM_TID:
      case OB_ALL_SYNONYM_HISTORY_TID:
      case OB_ALL_SYS_VARIABLE_TID:
      case OB_ALL_SYS_VARIABLE_HISTORY_TID:
      case OB_ALL_TABLE_TID:
      case OB_ALL_TABLE_HISTORY_TID:
      case OB_ALL_TABLE_PRIVILEGE_TID:
      case OB_ALL_TABLE_PRIVILEGE_HISTORY_TID:
      case OB_ALL_TABLEGROUP_TID:
      case OB_ALL_TABLEGROUP_HISTORY_TID:
      case OB_ALL_TEMP_TABLE_TID:
      case OB_ALL_TENANT_PLAN_BASELINE_TID:
      case OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TID:
      case OB_ALL_USER_TID:
      case OB_ALL_USER_HISTORY_TID: {
        // The partition method is key(tenant_id), and the number of partitions is 16, the corresponding partition
        // partition_id of OB_SYS_TENANT_ID is 3.
        // Here for performance considerations, directly write death
        // ObObj key;
        // key.set_int(OB_SYS_TENANT_ID);
        // uint64_t hash_code = 0;
        // hash_code = key.hash_v1(hash_code);
        // int64_t result = static_cast<int64_t>(hash_code);
        // result = result < 0 ? -result : result;
        // int64_t mod_id = result % 16;
        // bret = mod_id == partition_id;
        const int64_t TARGET_PARTITION_ID = 3;
        bret = TARGET_PARTITION_ID == partition_id;
        break;
      }
      default: {
        break;
      }
    }
  }
  return bret;
}

// tenant space sys tables in slave cluster which have no leader
int ObSysTableChecker::is_tenant_table_need_weak_read(const uint64_t table_id, bool& need_weak_read)
{
  int ret = OB_SUCCESS;
  bool in_tenant_space = false;
  if (!is_sys_table(table_id)) {
    need_weak_read = false;
  } else if (OB_FAIL(is_tenant_space_table_id(table_id, in_tenant_space))) {
    LOG_WARN("fail to check table_id in tenant space", K(ret), K(table_id));
  } else if (in_tenant_space && !is_cluster_private_tenant_table(table_id)) {
    need_weak_read = true;
  } else {
    need_weak_read = false;
  }
  return ret;
}
int ObSysTableChecker::ob_write_string(const ObString& src, ObString& dst)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  int64_t len = src.length();
  if (NULL == (buf = allocator_.alloc(len))) {
    dst.assign(NULL, 0);
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret), K(len));
  } else {
    MEMCPY(buf, src.ptr(), len);
    dst.assign_ptr(reinterpret_cast<char*>(buf), static_cast<ObString::obstr_size_t>(len));
  }
  return ret;
}

bool PartIdPartitionArrayCmp::operator()(
    const share::schema::ObPartition* left, const share::schema::ObPartition* right)
{
  bool bool_ret = false;
  if (common::OB_SUCCESS != ret_) {
    // by pass
  } else if (OB_UNLIKELY(nullptr == left || nullptr == right)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("left or right ptr is null", K(ret_), KP(left), KP(right));
  } else if (left->get_part_id() < right->get_part_id()) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool SubPartIdPartitionArrayCmp::operator()(
    const share::schema::ObSubPartition* left, const share::schema::ObSubPartition* right)
{
  bool bool_ret = false;
  if (common::OB_SUCCESS != ret_) {
    // by pass
  } else if (OB_UNLIKELY(nullptr == left || nullptr == right)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("left or right ptr is null", K(ret_), KP(left), KP(right));
  } else if (left->get_sub_part_id() < right->get_sub_part_id()) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

ObRefreshSchemaInfo::ObRefreshSchemaInfo(const ObRefreshSchemaInfo& other)
{
  assign(other);
}

int ObRefreshSchemaInfo::assign(const ObRefreshSchemaInfo& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  schema_version_ = other.schema_version_;
  sequence_id_ = other.sequence_id_;
  split_schema_version_ = other.split_schema_version_;
  return ret;
}

void ObRefreshSchemaInfo::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  schema_version_ = common::OB_INVALID_VERSION;
  sequence_id_ = common::OB_INVALID_ID;
  split_schema_version_ = common::OB_INVALID_VERSION;
}

// In schema split mode:
// 1. The observer is started in the old mode, and RS pushes the sequence_id at the last stage of do_restart. At this
// time,
//  the sequence_id of the heartbeat received by the observer may be an illegal value;
// 2. After RS restarts, if DDL has not been done, the broadcast (tenant_id, schema_version) is an illegal value.
bool ObRefreshSchemaInfo::is_valid() const
{
  return OB_INVALID_VERSION != split_schema_version_;
}

OB_SERIALIZE_MEMBER(ObRefreshSchemaInfo, tenant_id_, schema_version_, sequence_id_, split_schema_version_);

OB_SERIALIZE_MEMBER(ObSchemaObjVersion, object_id_, version_, object_type_);

void ObDropTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  schema_version_ = OB_INVALID_VERSION;
}

bool ObDropTenantInfo::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_VERSION != schema_version_;
}

ObSysParam::ObSysParam()
{
  reset();
}

ObSysParam::~ObSysParam()
{}

int ObSysParam::init(const uint64_t tenant_id, const common::ObZone& zone, const ObString& name, int64_t data_type,
    const ObString& value, const ObString& min_val, const ObString& max_val, const ObString& info, int64_t flags)
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
  } else if (OB_FAIL(databuff_printf(name_, OB_MAX_SYS_PARAM_NAME_LENGTH, pos, "%.*s", name.length(), name.ptr()))) {
    LOG_WARN("failed to print name", K(name), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(
                 databuff_printf(value_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", value.length(), value.ptr()))) {
    LOG_WARN("failed to print value", K(value), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(
                 min_val_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", min_val.length(), min_val.ptr()))) {
    LOG_WARN("failed to print min_val", K(min_val), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(
                 max_val_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", max_val.length(), max_val.ptr()))) {
    LOG_WARN("failed to print max_val", K(max_val), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(info_, OB_MAX_SYS_PARAM_INFO_LENGTH, pos, "%.*s", info.length(), info.ptr()))) {
    LOG_WARN("failed to print info", K(info), K(ret));
  } else { /*do nothing*/
  }
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

int64_t ObSysParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(zone), K_(name), K_(data_type), K_(value), K_(info), K_(flags));
  return pos;
}

ObSysVariableSchema::ObSysVariableSchema() : ObSchema()
{
  reset();
}

ObSysVariableSchema::ObSysVariableSchema(ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}

ObSysVariableSchema::ObSysVariableSchema(const ObSysVariableSchema& src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObSysVariableSchema::~ObSysVariableSchema()
{}

ObSysVariableSchema& ObSysVariableSchema::operator=(const ObSysVariableSchema& src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    schema_version_ = src_schema.schema_version_;
    read_only_ = src_schema.read_only_;
    name_case_mode_ = src_schema.name_case_mode_;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.get_sysvar_count(); ++i) {
      const ObSysVarSchema* sysvar = src_schema.get_sysvar_schema(i);
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
  return *this;
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
    const ObSysVarSchema* sysvar = get_sysvar_schema(i);
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
      // do nothing
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
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, schema_version_, read_only_, name_case_mode_);

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

int ObSysVariableSchema::add_sysvar_schema(const ObSysVarSchema& sysvar_schema)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  ObSysVarSchema* tmp_sysvar_schema = NULL;
  ObSysVarClassType var_id = ObSysVarFactory::find_sys_var_id_by_name(sysvar_schema.get_name(), true);
  int64_t var_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(SYS_VAR_INVALID == var_id)) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_DEBUG("system variable is unknown", K(sysvar_schema));
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(var_id, var_idx))) {
    if (ret != OB_SYS_VARS_MAYBE_DIFF_VERSION) {  // If the error is caused by a different version, just ignore it
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
    tmp_sysvar_schema = new (ptr) ObSysVarSchema(allocator_);
    *tmp_sysvar_schema = sysvar_schema;
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
      read_only_ = static_cast<bool>((sysvar_schema.get_value())[0] - '0');
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

int ObSysVariableSchema::get_sysvar_schema(const ObString& name, const ObSysVarSchema*& sysvar_schema) const
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

int ObSysVariableSchema::get_sysvar_schema(ObSysVarClassType var_id, const ObSysVarSchema*& sysvar_schema) const
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

ObSysVarSchema* ObSysVariableSchema::get_sysvar_schema(int64_t idx)
{
  ObSysVarSchema* ret = NULL;
  if (idx >= 0 && idx < get_sysvar_count()) {
    ret = sysvar_array_[idx];
  }
  return ret;
}

const ObSysVarSchema* ObSysVariableSchema::get_sysvar_schema(int64_t idx) const
{
  const ObSysVarSchema* ret = NULL;
  if (idx >= 0 && idx < get_sysvar_count()) {
    ret = sysvar_array_[idx];
  }
  return ret;
}

int ObSysVariableSchema::get_oracle_mode(bool& is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  is_oracle_mode = false;
  const ObSysVarSchema* sysvar_schema = nullptr;
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
ObSchema::ObSchema() : buffer_(this), error_ret_(OB_SUCCESS), is_inner_allocator_(false), allocator_(NULL)
{}

ObSchema::ObSchema(common::ObIAllocator* allocator)
    : buffer_(this), error_ret_(OB_SUCCESS), is_inner_allocator_(false), allocator_(allocator)
{}

ObSchema::~ObSchema()
{
  if (is_inner_allocator_) {
    common::ObArenaAllocator* arena = static_cast<common::ObArenaAllocator*>(allocator_);
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

common::ObIAllocator* ObSchema::get_allocator()
{
  if (NULL == allocator_) {
    if (!THIS_WORKER.has_req_flag()) {
      if (NULL == (allocator_ = OB_NEW(
                       ObArenaAllocator, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA))) {
        LOG_WARN("Fail to new allocator.");
      } else {
        is_inner_allocator_ = true;
      }
    } else if (NULL != schema_stack_allocator()) {
      allocator_ = schema_stack_allocator();
    } else {
      allocator_ = &THIS_WORKER.get_allocator();
    }
  }
  return allocator_;
}

void* ObSchema::alloc(int64_t size)
{
  void* ret = NULL;
  ObIAllocator* allocator = get_allocator();
  if (NULL == allocator) {
    LOG_WARN("Fail to get allocator.");
  } else {
    ret = allocator->alloc(size);
  }

  return ret;
}

void ObSchema::free(void* ptr)
{
  if (NULL != ptr) {
    if (NULL != allocator_) {
      allocator_->free(ptr);
    }
  }
}

int ObSchema::zone_array2str(const common::ObIArray<common::ObZone>& zone_list, char* str, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObString> zone_ptr_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
    const common::ObZone& this_zone = zone_list.at(i);
    common::ObString zone_ptr(this_zone.size(), this_zone.ptr());
    if (OB_FAIL(zone_ptr_array.push_back(zone_ptr))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(string_array2str(zone_ptr_array, str, buf_size))) {
    LOG_WARN("fail to convert Obstring array to C style string", K(ret));
  } else {
  }  // good
  return ret;
}

int ObSchema::string_array2str(
    const common::ObIArray<common::ObString>& string_array, char* str, const int64_t buf_size) const
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
      n = snprintf(str + nwrite,
          static_cast<uint32_t>(buf_size - nwrite),
          "%s%s",
          to_cstring(string_array.at(i)),
          (i != string_array.count() - 1) ? ";" : "");
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

int ObSchema::str2string_array(const char* str, common::ObIArray<common::ObString>& string_array) const
{
  int ret = OB_SUCCESS;
  char* item_str = NULL;
  char* save_ptr = NULL;
  while (OB_SUCC(ret)) {
    item_str = strtok_r((NULL == item_str ? const_cast<char*>(str) : NULL), ";", &save_ptr);
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

int ObSchema::deep_copy_str(const char* src, ObString& dest)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;

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
      MEMCPY(buf, src, len - 1);
      buf[len - 1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len - 1));
    }
  }

  return ret;
}

int ObSchema::deep_copy_str(const ObString& src, ObString& dest)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;

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
        MEMCPY(buf, src.ptr(), len - 1);
        buf[len - 1] = '\0';
        dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len - 1));
      }
    } else {
      dest.reset();
    }
  }

  return ret;
}

int ObSchema::deep_copy_obj(const ObObj& src, ObObj& dest)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
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
      } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
        LOG_WARN("Fail to deep copy obj, ", K(ret));
      }
    } else {
      dest = src;
    }
  }

  return ret;
}

int ObSchema::deep_copy_string_array(const ObIArray<ObString>& src_array, ObArrayHelper<ObString>& dst_array)
{
  int ret = OB_SUCCESS;
  if (NULL != dst_array.get_base_address()) {
    free(dst_array.get_base_address());
    dst_array.reset();
  }
  const int64_t alloc_size = src_array.count() * static_cast<int64_t>(sizeof(ObString));
  void* buf = NULL;
  if (src_array.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(alloc_size), K(ret));
  } else {
    dst_array.init(src_array.count(), static_cast<ObString*>(buf));
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

int ObSchema::add_string_to_array(const ObString& str, ObArrayHelper<ObString>& str_array)
{
  int ret = OB_SUCCESS;
  const int64_t extend_cnt = STRING_ARRAY_EXTEND_CNT;
  int64_t alloc_size = 0;
  void* buf = NULL;
  // if not init, alloc memory and init it
  if (!str_array.check_inner_stat()) {
    alloc_size = extend_cnt * static_cast<int64_t>(sizeof(ObString));
    if (NULL == (buf = alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(alloc_size), K(ret));
    } else {
      str_array.init(extend_cnt, static_cast<ObString*>(buf));
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
          ObArrayHelper<ObString> new_array(str_array.count() + extend_cnt, static_cast<ObString*>(buf));
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

int ObSchema::serialize_string_array(
    char* buf, const int64_t buf_len, int64_t& pos, const ObArrayHelper<ObString>& str_array) const
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

int ObSchema::deserialize_string_array(
    const char* buf, const int64_t data_len, int64_t& pos, common::ObArrayHelper<common::ObString>& str_array)
{
  int ret = OB_SUCCESS;
  int64_t temp_pos = pos;
  int64_t count = 0;
  str_array.reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("deserialize count failed", K(ret));
  } else if (0 == count) {
    // do nothing
  } else {
    void* array_buf = NULL;
    const int64_t alloc_size = count * static_cast<int64_t>(sizeof(ObString));
    if (NULL == (array_buf = alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(alloc_size), K(ret));
    } else {
      str_array.init(count, static_cast<ObString*>(array_buf));
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

int64_t ObSchema::get_string_array_serialize_size(const ObArrayHelper<ObString>& str_array) const
{
  int64_t serialize_size = 0;
  const int64_t count = str_array.count();
  serialize_size += serialization::encoded_length_vi64(count);
  for (int64_t i = 0; i < count; ++i) {
    serialize_size += str_array.at(i).get_serialize_size();
  }
  return serialize_size;
}

void ObSchema::reset_string(ObString& str)
{
  if (NULL != str.ptr() && 0 != str.length()) {
    free(str.ptr());
  }
  str.reset();
}

void ObSchema::reset_string_array(ObArrayHelper<ObString>& str_array)
{
  if (NULL != str_array.get_base_address()) {
    for (int64_t i = 0; i < str_array.count(); ++i) {
      ObString& this_str = str_array.at(i);
      reset_string(this_str);
    }
    free(str_array.get_base_address());
  }
  str_array.reset();
}

const char* ObSchema::extract_str(const ObString& str) const
{
  return str.empty() ? "" : str.ptr();
}

void ObSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  if (is_inner_allocator_ && NULL != allocator_) {
    // It's better to invoke the reset methods of allocator if the ObIAllocator has reset function
    ObArenaAllocator* arena = static_cast<ObArenaAllocator*>(allocator_);
    arena->reuse();
  }
}
common::ObCollationType ObSchema::get_cs_type_with_cmp_mode(const ObNameCaseMode mode)
{
  common::ObCollationType cs_type = common::CS_TYPE_INVALID;

  if (OB_ORIGIN_AND_INSENSITIVE == mode || OB_LOWERCASE_AND_INSENSITIVE == mode) {
    cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (OB_ORIGIN_AND_SENSITIVE == mode) {
    cs_type = common::CS_TYPE_UTF8MB4_BIN;
  } else {
    SHARE_SCHEMA_LOG(ERROR, "invalid ObNameCaseMode value", K(mode));
  }
  return cs_type;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTenantSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
static const char* tenant_display_status_strs[] = {
    "TENANT_STATUS_NORMAL", "TENANT_STATUS_CREATING", "TENANT_STATUS_DROPPING", "TENANT_STATUS_RESTORE"};

const char* ob_tenant_status_str(const ObTenantStatus status)
{
  STATIC_ASSERT(ARRAYSIZEOF(tenant_display_status_strs) == TENANT_STATUS_MAX,
      "type string array size mismatch with enum tenant status count");
  const char* str = NULL;
  if (status >= 0 && status < TENANT_STATUS_MAX) {
    str = tenant_display_status_strs[status];
  } else {
    LOG_WARN("invalid tenant status", K(status));
  }
  return str;
}

int get_tenant_status(const ObString& str, ObTenantStatus& status)
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

ObTenantSchema::ObTenantSchema() : ObSchema()
{
  reset();
}

ObTenantSchema::ObTenantSchema(ObIAllocator* allocator)
    : ObSchema(allocator), zone_list_(), zone_replica_attr_array_(), primary_zone_array_()
{
  reset();
}

ObTenantSchema::ObTenantSchema(const ObTenantSchema& src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObTenantSchema::~ObTenantSchema()
{}

ObTenantSchema& ObTenantSchema::operator=(const ObTenantSchema& src_schema)
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
    set_rewrite_merge_version(src_schema.get_rewrite_merge_version());
    set_storage_format_version(src_schema.get_storage_format_version());
    set_storage_format_work_version(src_schema.get_storage_format_work_version());
    set_default_tablegroup_id(src_schema.default_tablegroup_id_);
    set_compatibility_mode(src_schema.compatibility_mode_);
    set_drop_tenant_time(src_schema.drop_tenant_time_);
    set_status(src_schema.status_);
    set_in_recyclebin(src_schema.in_recyclebin_);
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
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.get_sysvar_count(); ++i) {
      const ObSysVarSchema* sysvar = src_schema.get_sysvar_schema(i);
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
  return *this;
}

int ObTenantSchema::assign(const ObTenantSchema& src_schema)
{
  int ret = OB_SUCCESS;
  reset();
  *this = src_schema;
  ret = error_ret_;
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
  replica_num_ = 0;
  reset_string_array(zone_list_);
  reset_string(primary_zone_);
  locked_ = false;
  read_only_ = false;
  rewrite_merge_version_ = 0;
  charset_type_ = ObCharset::get_default_charset();
  collation_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
  name_case_mode_ = OB_NAME_CASE_INVALID;
  reset_string(comment_);
  reset_string(locality_str_);
  logonly_replica_num_ = 0;
  reset_string(previous_locality_str_);
  reset_zone_replica_attr_array();
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    free(primary_zone_array_.at(i).zone_.ptr());
  }
  primary_zone_array_.reset();
  storage_format_version_ = 0;
  storage_format_work_version_ = 0;
  memset(sysvar_array_, 0, sizeof(sysvar_array_));
  default_tablegroup_id_ = OB_INVALID_ID;
  reset_string(default_tablegroup_name_);
  compatibility_mode_ = ObCompatibilityMode::OCEANBASE_MODE;
  drop_tenant_time_ = OB_INVALID_TIMESTAMP;
  status_ = TENANT_STATUS_NORMAL;
  in_recyclebin_ = false;
  ObSchema::reset();
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
  for (int64_t i = 0; i < get_sysvar_count(); ++i) {
    const ObSysVarSchema* sysvar = get_sysvar_schema(i);
    if (sysvar != NULL) {
      convert_size += sizeof(ObSysVarSchema);
      convert_size += sysvar->get_convert_size();
    }
  }
  convert_size += default_tablegroup_name_.length() + 1;
  return convert_size;
}

void ObTenantSchema::reset_zone_replica_attr_array()
{
  if (NULL != zone_replica_attr_array_.get_base_address()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
      SchemaZoneReplicaAttrSet& zone_locality = zone_replica_attr_array_.at(i);
      reset_string_array(zone_locality.zone_set_);
      SchemaReplicaAttrArray& full_attr_set =
          static_cast<SchemaReplicaAttrArray&>(zone_locality.replica_attr_set_.get_full_replica_attr_array());
      if (nullptr != full_attr_set.get_base_address()) {
        free(full_attr_set.get_base_address());
        full_attr_set.reset();
      }
      SchemaReplicaAttrArray& logonly_attr_set =
          static_cast<SchemaReplicaAttrArray&>(zone_locality.replica_attr_set_.get_logonly_replica_attr_array());
      if (nullptr != logonly_attr_set.get_base_address()) {
        free(logonly_attr_set.get_base_address());
        logonly_attr_set.reset();
      }
      SchemaReplicaAttrArray& readonly_attr_set =
          static_cast<SchemaReplicaAttrArray&>(zone_locality.replica_attr_set_.get_readonly_replica_attr_array());
      if (nullptr != readonly_attr_set.get_base_address()) {
        free(readonly_attr_set.get_base_address());
        readonly_attr_set.reset();
      }
    }
    free(zone_replica_attr_array_.get_base_address());
    zone_replica_attr_array_.reset();
  }
}

int ObTenantSchema::set_specific_replica_attr_array(
    SchemaReplicaAttrArray& this_schema_set, const common::ObIArray<ReplicaAttr>& src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.count();
  if (count > 0) {
    const int64_t size = count * static_cast<int64_t>(sizeof(share::ReplicaAttr));
    void* ptr = nullptr;
    if (nullptr == (ptr = alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(size));
    } else if (FALSE_IT(this_schema_set.init(count, static_cast<ReplicaAttr*>(ptr), count))) {
      // shall never by here
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
        const share::ReplicaAttr& src_replica_attr = src.at(i);
        ReplicaAttr* dst_replica_attr = &this_schema_set.at(i);
        if (nullptr == (dst_replica_attr = new (dst_replica_attr)
                               ReplicaAttr(src_replica_attr.num_, src_replica_attr.memstore_percent_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("placement new return nullptr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTenantSchema::set_zone_replica_attr_array(const common::ObIArray<SchemaZoneReplicaAttrSet>& src)
{
  int ret = OB_SUCCESS;
  reset_zone_replica_attr_array();
  const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  void* buf = NULL;
  if (src.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(ret), K(alloc_size));
  } else {
    zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet*>(buf), src.count());
    for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
      const SchemaZoneReplicaAttrSet& src_replica_attr_set = src.at(i);
      SchemaZoneReplicaAttrSet* this_schema_set = &zone_replica_attr_array_.at(i);
      if (nullptr == (this_schema_set = new (this_schema_set) SchemaZoneReplicaAttrSet())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("placement new return nullptr", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
                     static_cast<SchemaReplicaAttrArray&>(
                         this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
                     src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
                     static_cast<SchemaReplicaAttrArray&>(
                         this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
                     src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
                     static_cast<SchemaReplicaAttrArray&>(
                         this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
                     src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
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

int ObTenantSchema::set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet>& src)
{
  int ret = OB_SUCCESS;
  reset_zone_replica_attr_array();
  const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  void* buf = NULL;
  if (src.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(ret), K(alloc_size));
  } else {
    zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet*>(buf), src.count());
    for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
      const share::ObZoneReplicaAttrSet& src_replica_attr_set = src.at(i);
      SchemaZoneReplicaAttrSet* this_schema_set = &zone_replica_attr_array_.at(i);
      if (nullptr == (this_schema_set = new (this_schema_set) SchemaZoneReplicaAttrSet())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("placement new return nullptr", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
                     static_cast<SchemaReplicaAttrArray&>(
                         this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
                     src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
                     static_cast<SchemaReplicaAttrArray&>(
                         this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
                     src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else if (OB_FAIL(set_specific_replica_attr_array(
                     static_cast<SchemaReplicaAttrArray&>(
                         this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
                     src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
        LOG_WARN("fail to set specific replica attr array", K(ret));
      } else {
        common::ObArray<common::ObString> zone_set_ptrs;
        for (int64_t j = 0; OB_SUCC(ret) && j < src_replica_attr_set.zone_set_.count(); ++j) {
          const common::ObZone& zone = src_replica_attr_set.zone_set_.at(j);
          if (OB_FAIL(zone_set_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
          }  // no more to do
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

int ObTenantSchema::get_zone_replica_attr_array(ZoneLocalityIArray& locality) const
{
  int ret = OB_SUCCESS;
  locality.reset();
  for (int64_t i = 0; i < zone_replica_attr_array_.count() && OB_SUCC(ret); ++i) {
    const SchemaZoneReplicaAttrSet& schema_replica_attr_set = zone_replica_attr_array_.at(i);
    ObZoneReplicaAttrSet zone_replica_attr_set;
    for (int64_t j = 0; OB_SUCC(ret) && j < schema_replica_attr_set.zone_set_.count(); ++j) {
      if (OB_FAIL(zone_replica_attr_set.zone_set_.push_back(schema_replica_attr_set.zone_set_.at(j)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_replica_attr_set.replica_attr_set_.assign(schema_replica_attr_set.replica_attr_set_))) {
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
    ObSchemaGetterGuard& schema_guard, ZoneLocalityIArray& locality) const
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
  int64_t total = 0;
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    total += zone_replica_attr_array_.at(i).get_full_replica_num();
  }
  return total;
}

int ObTenantSchema::get_paxos_replica_num(share::schema::ObSchemaGetterGuard& schema_guard, int64_t& num) const
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
// with the TableSchema and DatabaseSchema interfaces, the get_zone_list for the tenantSchema also passes in the
// schema_guard.
int ObTenantSchema::get_zone_list(
    share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<common::ObZone>& zone_list) const
{
  UNUSED(schema_guard);
  return get_zone_list(zone_list);
}

int ObTenantSchema::get_zone_list(common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  common::ObZone tmp_zone;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list_.count(); ++i) {
    tmp_zone.reset();
    const common::ObString& zone_ptr = zone_list_.at(i);
    if (OB_FAIL(tmp_zone.assign(zone_ptr.ptr()))) {
      LOG_WARN("fail to assign zone", K(ret), K(zone_ptr));
    } else if (OB_FAIL(zone_list.push_back(tmp_zone))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObTenantSchema::set_zone_list(const common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  // The length of the string in zone_list_ptrs directly points to the ObZone of zone_list.
  common::ObArray<common::ObString> zone_list_ptrs;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
    const common::ObZone& zone = zone_list.at(i);
    if (OB_FAIL(zone_list_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_string_array(zone_list_ptrs, zone_list_))) {
    LOG_WARN("fail to copy zone list", K(ret), K(zone_list_ptrs));
  } else {
  }  // no more to do
  return ret;
}

OB_DEF_SERIALIZE(ObTenantSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      schema_version_,
      tenant_name_,
      replica_num_,
      primary_zone_,
      locked_,
      comment_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      read_only_,
      rewrite_merge_version_,
      locality_str_,
      logonly_replica_num_,
      previous_locality_str_,
      storage_format_version_,
      storage_format_work_version_);
  if (!OB_SUCC(ret)) {
    LOG_WARN("func_SERIALIZE failed", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, zone_list_))) {
    LOG_WARN("serialize_string_array failed", K(ret));
  } else {
  }  // no more to do
  LST_DO_CODE(OB_UNIS_ENCODE,
      default_tablegroup_id_,
      default_tablegroup_name_,
      compatibility_mode_,
      drop_tenant_time_,
      status_,
      in_recyclebin_);

  LOG_INFO("serialize schema",
      K_(tenant_id),
      K_(schema_version),
      K_(tenant_name),
      K_(replica_num),
      K_(primary_zone),
      K_(locked),
      K_(comment),
      K_(charset_type),
      K_(collation_type),
      K_(name_case_mode),
      K_(rewrite_merge_version),
      K_(locality_str),
      K_(logonly_replica_num),
      K_(primary_zone_array),
      K_(storage_format_version),
      K_(storage_format_work_version),
      K_(default_tablegroup_id),
      K_(default_tablegroup_name),
      K_(drop_tenant_time),
      K_(in_recyclebin),
      K(ret));
  return ret;
}

OB_DEF_DESERIALIZE(ObTenantSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      schema_version_,
      tenant_name_,
      replica_num_,
      primary_zone_,
      locked_,
      comment_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      read_only_,
      rewrite_merge_version_,
      locality_str_,
      logonly_replica_num_,
      previous_locality_str_,
      storage_format_version_,
      storage_format_work_version_);
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
      in_recyclebin_);

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
      } else if (OB_FAIL(locality_dist.parse_locality(locality_str_, zone_list_))) {
        SHARE_SCHEMA_LOG(WARN, "fail to parse locality", K(ret));
      } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_attr_array))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get zone region replica num array", K(ret));
      } else if (OB_FAIL(set_zone_replica_attr_array(zone_replica_attr_array))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set zone replica num array", K(ret));
      } else {
      }  // no more to do, good
    } else {
    }  // no need to parse locality
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
      } else {
      }  // set primary zone array success
    } else {
    }  // no need to parse primary zone
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTenantSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      schema_version_,
      tenant_name_,
      replica_num_,
      primary_zone_,
      locked_,
      comment_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      read_only_,
      rewrite_merge_version_,
      locality_str_,
      logonly_replica_num_,
      previous_locality_str_,
      storage_format_version_,
      storage_format_work_version_,
      default_tablegroup_id_,
      default_tablegroup_name_,
      compatibility_mode_,
      drop_tenant_time_,
      status_,
      in_recyclebin_);
  len += get_string_array_serialize_size(zone_list_);
  return len;
}

int ObTenantSchema::get_raw_first_primary_zone(
    const rootserver::ObRandomZoneSelector& random_selector, common::ObZone& first_primary_zone) const
{
  int ret = OB_SUCCESS;
  if (primary_zone_.empty()) {
    if (OB_FAIL(first_primary_zone.assign(primary_zone_.ptr()))) {
      LOG_WARN("fail to assign first primary zone", K(ret));
    } else {
    }  // no more to do
  } else if (primary_zone_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, primary zone array empty", K(ret), K(primary_zone_));
  } else {
    int64_t idx = -1;
    int64_t min_score = INT64_MAX;
    ObZone zone;
    const ObZoneScore& sample_zone = primary_zone_array_.at(0);
    for (int64_t i = 0; i < primary_zone_array_.count() && OB_SUCC(ret); ++i) {
      int64_t zone_score = INT64_MAX;
      if (sample_zone.score_ != primary_zone_array_.at(i).score_) {
        break;
      } else if (OB_FAIL(zone.assign(primary_zone_array_.at(i).zone_.ptr()))) {
        LOG_WARN("fail to assign zone", K(ret));
      } else if (OB_FAIL(random_selector.get_zone_score(zone, zone_score))) {
        LOG_WARN("fail to get zone score", K(ret), K(zone));
      } else if (zone_score < min_score) {
        min_score = zone_score;
        idx = i;
      } else {
      }  // do not update min score
    }
    if (OB_SUCC(ret)) {
      if (idx == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no zone found", K(ret));
      } else if (OB_FAIL(first_primary_zone.assign(primary_zone_array_.at(idx).zone_))) {
        LOG_WARN("fail to assign first primary zone", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObTenantSchema::get_first_primary_zone(const rootserver::ObRandomZoneSelector& random_selector,
    const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs, common::ObZone& first_primary_zone) const
{
  int ret = OB_SUCCESS;
  first_primary_zone.reset();
  if (primary_zone_.empty()) {
    // do nothing, return empty first primary zone
  } else {
    common::ObArray<rootserver::ObPrimaryZoneReplicaCandidate> candidate_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_addrs.count(); ++i) {
      const rootserver::ObReplicaAddr& replica_addr = replica_addrs.at(i);
      rootserver::ObPrimaryZoneReplicaCandidate candidate;
      candidate.is_full_replica_ = (common::REPLICA_TYPE_FULL == replica_addr.replica_type_);
      candidate.zone_ = replica_addr.zone_;
      if (OB_FAIL(get_primary_zone_score(candidate.zone_, candidate.zone_score_))) {
        LOG_WARN("fail to get primary zone score", K(ret), "zone", candidate.zone_);
      } else if (OB_FAIL(random_selector.get_zone_score(candidate.zone_, candidate.random_score_))) {
        LOG_WARN("fail to get zone random score", K(ret), "zone", candidate.zone_);
      } else if (OB_FAIL(candidate_array.push_back(candidate))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
    if (OB_SUCC(ret) && candidate_array.count() > 0) {
      std::sort(candidate_array.begin(), candidate_array.end(), rootserver::ObPrimaryZoneReplicaCmp());
      const rootserver::ObPrimaryZoneReplicaCandidate& candidate = candidate_array.at(0);
      if (candidate.is_full_replica_) {
        first_primary_zone = candidate.zone_;
      } else {
        first_primary_zone.reset();
      }
    }
  }
  return ret;
}

int ObTenantSchema::get_primary_zone_score(const common::ObZone& zone, int64_t& zone_score) const
{
  int ret = OB_SUCCESS;
  zone_score = INT64_MAX;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_UNLIKELY(primary_zone_.empty()) || OB_UNLIKELY(primary_zone_array_.count() <= 0)) {
    // zone score set to INT64_MAX above
  } else {
    for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
      if (zone == primary_zone_array_.at(i).zone_) {
        zone_score = primary_zone_array_.at(i).score_;
        break;
      } else {
      }  // go no find
    }
  }
  return ret;
}

int ObTenantSchema::set_primary_zone_array(const common::ObIArray<ObZoneScore>& primary_zone_array)
{
  int ret = OB_SUCCESS;
  primary_zone_array_.reset();
  int64_t count = primary_zone_array.count();
  for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
    const ObZoneScore& zone_score = primary_zone_array.at(i);
    ObString str;
    if (OB_FAIL(deep_copy_str(zone_score.zone_, str))) {
      LOG_WARN("deep copy str failed", K(ret));
    } else if (OB_FAIL(primary_zone_array_.push_back(ObZoneScore(str, zone_score.score_)))) {
      LOG_WARN("fail to push back", K(ret));
      for (int64_t j = 0; j < primary_zone_array_.count(); ++j) {
        free(primary_zone_array_.at(j).zone_.ptr());
      }
      free(str.ptr());
    } else {
    }  // ok
  }
  return ret;
}

int ObTenantSchema::add_sysvar_schema(const ObSysVarSchema& sysvar_schema)
{
  int ret = OB_SUCCESS;
  ObSysVarSchema* tmp_sysvar_schema = NULL;
  void* ptr = NULL;
  ObSysVarClassType var_id = ObSysVarFactory::find_sys_var_id_by_name(sysvar_schema.get_name(), true);
  int64_t var_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(SYS_VAR_INVALID == var_id)) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_DEBUG("system variable is unknown", K(sysvar_schema));
  } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(var_id, var_idx))) {
    if (ret != OB_SYS_VARS_MAYBE_DIFF_VERSION) {
      // If the error is caused by a different version, just ignore it
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
    tmp_sysvar_schema = new (ptr) ObSysVarSchema(allocator_);
    *tmp_sysvar_schema = sysvar_schema;
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
  return ret;
}

const ObSysVarSchema* ObTenantSchema::get_sysvar_schema(int64_t idx) const
{
  const ObSysVarSchema* ret = NULL;
  if (idx >= 0 && idx < get_sysvar_count()) {
    ret = sysvar_array_[idx];
  }
  return ret;
}

int ObTenantSchema::get_primary_zone_inherit(ObSchemaGetterGuard& schema_guard, ObPrimaryZone& primary_zone) const
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

ObSysVarSchema::ObSysVarSchema(const ObSysVarSchema& src_schema) : ObSchema()
{
  *this = src_schema;
}

ObSysVarSchema::ObSysVarSchema(ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}

ObSysVarSchema& ObSysVarSchema::operator=(const ObSysVarSchema& src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    if (!src_schema.is_valid()) {
      ret = src_schema.get_err_ret();
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
  }
  error_ret_ = ret;
  return *this;
}

int ObSysVarSchema::assign(const ObSysVarSchema& other)
{
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

int ObSysVarSchema::get_value(ObIAllocator* allocator, const ObDataTypeCastParams& dtc_params, ObObj& value) const
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
    const ObObj* res_val = NULL;
    if (OB_FAIL(ObObjCaster::to_type(data_type_, cast_ctx, var_value, casted_val, res_val))) {
      _LOG_WARN("failed to cast object, ret=%d cell=%s from_type=%s to_type=%s",
          ret,
          to_cstring(var_value),
          ob_obj_type_str(var_value.get_type()),
          ob_obj_type_str(data_type_));
    } else if (OB_ISNULL(res_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("casted success, but res_val is NULL", K(ret), K(var_value), K_(data_type));
    } else {
      value = *res_val;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObSysVarSchema, tenant_id_, name_, data_type_, value_, min_val_, max_val_, info_, zone_, schema_version_, flags_);
/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObDatabaseSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObDatabaseSchema::ObDatabaseSchema() : ObSchema()
{
  reset();
}

ObDatabaseSchema::ObDatabaseSchema(ObIAllocator* allocator) : ObSchema(allocator), zone_list_(), primary_zone_array_()
{
  reset();
}

ObDatabaseSchema::ObDatabaseSchema(const ObDatabaseSchema& src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObDatabaseSchema::~ObDatabaseSchema()
{}

ObDatabaseSchema& ObDatabaseSchema::operator=(const ObDatabaseSchema& src_schema)
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
    set_drop_schema_version(src_schema.get_drop_schema_version());

    if (OB_FAIL(set_database_name(src_schema.database_name_))) {
      LOG_WARN("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_zone_list(src_schema.zone_list_))) {
      LOG_WARN("set_zone_list failed", K(ret));
    } else if (OB_FAIL(set_primary_zone(src_schema.primary_zone_))) {
      LOG_WARN("set_primary_zone failed", K(ret));
    } else if (OB_FAIL(set_comment(src_schema.comment_))) {
      LOG_WARN("set_comment failed", K(ret));
    } else if (OB_FAIL(set_default_tablegroup_name(src_schema.default_tablegroup_name_))) {
      LOG_WARN("set_comment failed", K(ret));
    } else if (OB_FAIL(set_primary_zone_array(src_schema.primary_zone_array_))) {
      LOG_WARN("set primary zone array failed", K(ret));
    } else {
    }  // no more to do

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObDatabaseSchema::assign(const ObDatabaseSchema& src_schema)
{
  int ret = OB_SUCCESS;
  *this = src_schema;
  ret = src_schema.error_ret_;
  return ret;
}

int64_t ObDatabaseSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += database_name_.length() + 1;
  convert_size += zone_list_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < zone_list_.count(); ++i) {
    convert_size += zone_list_.at(i).length() + 1;
  }
  convert_size += primary_zone_.length() + 1;
  convert_size += comment_.length() + 1;
  convert_size += primary_zone_array_.get_data_size();
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    convert_size += primary_zone_array_.at(i).zone_.length() + 1;
  }
  return convert_size;
}

bool ObDatabaseSchema::is_valid() const
{
  return ObSchema::is_valid() && common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != database_id_ &&
         schema_version_ > 0;
}

void ObDatabaseSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  reset_string(database_name_);
  replica_num_ = 0;
  reset_string_array(zone_list_);
  reset_string(primary_zone_);
  reset_string(comment_);
  charset_type_ = common::CHARSET_INVALID;
  collation_type_ = common::CS_TYPE_INVALID;
  name_case_mode_ = OB_NAME_CASE_INVALID;
  read_only_ = false;
  default_tablegroup_id_ = OB_INVALID_ID;
  reset_string(default_tablegroup_name_);
  in_recyclebin_ = false;
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    free(primary_zone_array_.at(i).zone_.ptr());
  }
  primary_zone_array_.reset();
  drop_schema_version_ = OB_INVALID_VERSION;
  ObSchema::reset();
}

int ObDatabaseSchema::get_zone_replica_attr_array_inherit(
    share::schema::ObSchemaGetterGuard& schema_guard, ZoneLocalityIArray& locality) const
{
  int ret = OB_SUCCESS;
  locality.reset();
  const ObTenantSchema* tenant_schema = NULL;
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
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObDatabaseSchema::get_paxos_replica_num(share::schema::ObSchemaGetterGuard& schema_guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;
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
    FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret)
    {
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

int ObDatabaseSchema::set_zone_list(const common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  // The length of the string in zone_list_ptrs directly points to the ObZone of zone_list.
  common::ObArray<common::ObString> zone_list_ptrs;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
    const common::ObZone& zone = zone_list.at(i);
    if (OB_FAIL(zone_list_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_string_array(zone_list_ptrs, zone_list_))) {
    LOG_WARN("fail to copy zone list", K(ret), K(zone_list_ptrs));
  } else {
  }  // no more to do
  return ret;
}

int ObDatabaseSchema::get_zone_list(
    share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;
  zone_list.reset();
  if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(database_id_), K(tenant_id_));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(database_id_), K(tenant_id_), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else {
  }  // no more to do
  return ret;
}

OB_DEF_SERIALIZE(ObDatabaseSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      database_id_,
      schema_version_,
      database_name_,
      replica_num_,
      primary_zone_,
      comment_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      read_only_,
      default_tablegroup_id_,
      default_tablegroup_name_,
      in_recyclebin_);
  if (!OB_SUCC(ret)) {
    LOG_WARN("func_SERIALIZE failed", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, zone_list_))) {
    LOG_WARN("serialize_string_array failed", K(ret));
  } else {
  }  // no more to do
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, drop_schema_version_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDatabaseSchema)
{
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString comment;
  ObString default_tablegroup_name;
  ObString primary_zone;
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      database_id_,
      schema_version_,
      database_name,
      replica_num_,
      primary_zone,
      comment,
      charset_type_,
      collation_type_,
      name_case_mode_,
      read_only_,
      default_tablegroup_id_,
      default_tablegroup_name,
      in_recyclebin_);
  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(set_database_name(database_name))) {
    LOG_WARN("set_tenant_name failed", K(ret));
  } else if (OB_FAIL(set_primary_zone(primary_zone))) {
    LOG_WARN("set_primary_zone failed", K(ret));
  } else if (OB_FAIL(set_comment(comment))) {
    LOG_WARN("set_comment failed", K(ret));
  } else if (OB_FAIL(set_default_tablegroup_name(default_tablegroup_name))) {
    LOG_WARN("set_comment failed", K(ret));
  } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, zone_list_))) {
    LOG_WARN("deserialize_string_array failed", K(ret));
  } else if (primary_zone_.length() > 0 && zone_list_.count() > 0) {
    ObPrimaryZoneUtil primary_zone_util(primary_zone_);
    if (OB_FAIL(primary_zone_util.init())) {
      SHARE_SCHEMA_LOG(WARN, "fail to init primary zone util", K(ret));
    } else if (OB_FAIL(primary_zone_util.check_and_parse_primary_zone())) {
      SHARE_SCHEMA_LOG(WARN, "fail to check and parse primary zone", K(ret));
    } else if (OB_FAIL(set_primary_zone_array(primary_zone_util.get_zone_array()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set primary zone array", K(ret));
    } else {
    }  // set primary zone array success
  } else {
  }  // no more to do
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, drop_schema_version_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatabaseSchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      database_id_,
      schema_version_,
      database_name_,
      replica_num_,
      primary_zone_,
      comment_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      read_only_,
      default_tablegroup_id_,
      default_tablegroup_name_,
      in_recyclebin_,
      drop_schema_version_);

  len += get_string_array_serialize_size(zone_list_);
  return len;
}

int ObDatabaseSchema::get_raw_first_primary_zone(
    const rootserver::ObRandomZoneSelector& random_selector, common::ObZone& first_primary_zone) const
{
  int ret = OB_SUCCESS;
  if (primary_zone_.empty()) {
    if (OB_FAIL(first_primary_zone.assign(primary_zone_.ptr()))) {
      LOG_WARN("fail to assign first primary zone", K(ret));
    } else {
    }  // no more to do
  } else if (primary_zone_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, primary zone array empty", K(ret), K(primary_zone_));
  } else {
    int64_t idx = -1;
    int64_t min_score = INT64_MAX;
    ObZone zone;
    const ObZoneScore& sample_zone = primary_zone_array_.at(0);
    for (int64_t i = 0; i < primary_zone_array_.count() && OB_SUCC(ret); ++i) {
      int64_t zone_score = INT64_MAX;
      if (sample_zone.score_ != primary_zone_array_.at(i).score_) {
        break;
      } else if (OB_FAIL(zone.assign(primary_zone_array_.at(i).zone_.ptr()))) {
        LOG_WARN("fail to assign zone", K(ret));
      } else if (OB_FAIL(random_selector.get_zone_score(zone, zone_score))) {
        LOG_WARN("fail to get zone score", K(ret), K(zone));
      } else if (zone_score < min_score) {
        min_score = zone_score;
        idx = i;
      } else {
      }  // do not update min score
    }
    if (OB_SUCC(ret)) {
      if (idx == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no zone found", K(ret));
      } else if (OB_FAIL(first_primary_zone.assign(primary_zone_array_.at(idx).zone_))) {
        LOG_WARN("fail to assign first primary zone", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObDatabaseSchema::get_primary_zone_score(const common::ObZone& zone, int64_t& zone_score) const
{
  int ret = OB_SUCCESS;
  zone_score = INT64_MAX;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_UNLIKELY(primary_zone_.empty()) || OB_UNLIKELY(primary_zone_array_.count() <= 0)) {
    // zone score set to INT64_MAX above
  } else {
    for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
      if (zone == primary_zone_array_.at(i).zone_) {
        zone_score = primary_zone_array_.at(i).score_;
        break;
      } else {
      }  // go on find
    }
  }
  return ret;
}

int ObDatabaseSchema::get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard& schema_guard,
    const rootserver::ObRandomZoneSelector& random_selector,
    const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs, common::ObZone& first_primary_zone) const
{
  int ret = OB_SUCCESS;
  first_primary_zone.reset();
  if (primary_zone_.empty()) {
    const share::schema::ObTenantSchema* tenant_schema = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_first_primary_zone(random_selector, replica_addrs, first_primary_zone))) {
      LOG_WARN("fail to get first primary zone", K(ret));
    }
  } else {
    common::ObArray<rootserver::ObPrimaryZoneReplicaCandidate> candidate_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_addrs.count(); ++i) {
      const rootserver::ObReplicaAddr& replica_addr = replica_addrs.at(i);
      rootserver::ObPrimaryZoneReplicaCandidate candidate;
      candidate.is_full_replica_ = (common::REPLICA_TYPE_FULL == replica_addr.replica_type_);
      candidate.zone_ = replica_addr.zone_;
      if (OB_FAIL(get_primary_zone_score(candidate.zone_, candidate.zone_score_))) {
        LOG_WARN("fail to get primary zone score", K(ret), "zone", candidate.zone_);
      } else if (OB_FAIL(random_selector.get_zone_score(candidate.zone_, candidate.random_score_))) {
        LOG_WARN("fail to get zone random score", K(ret), "zone", candidate.zone_);
      } else if (OB_FAIL(candidate_array.push_back(candidate))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
    if (OB_SUCC(ret) && candidate_array.count() > 0) {
      std::sort(candidate_array.begin(), candidate_array.end(), rootserver::ObPrimaryZoneReplicaCmp());
      const rootserver::ObPrimaryZoneReplicaCandidate& candidate = candidate_array.at(0);
      if (candidate.is_full_replica_) {
        first_primary_zone = candidate.zone_;
      } else {
        first_primary_zone.reset();
      }
    }
  }
  return ret;
}

int ObDatabaseSchema::set_primary_zone_array(const common::ObIArray<ObZoneScore>& primary_zone_array)
{
  int ret = OB_SUCCESS;
  primary_zone_array_.reset();
  int64_t count = primary_zone_array.count();
  for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
    const ObZoneScore& zone_score = primary_zone_array.at(i);
    ObString str;
    if (OB_FAIL(deep_copy_str(zone_score.zone_, str))) {
      LOG_WARN("deep copy str failed", K(ret));
    } else if (OB_FAIL(primary_zone_array_.push_back(ObZoneScore(str, zone_score.score_)))) {
      LOG_WARN("fail to push back", K(ret));
      for (int64_t j = 0; j < primary_zone_array_.count(); ++j) {
        free(primary_zone_array_.at(j).zone_.ptr());
      }
      free(str.ptr());
    } else {
    }  // ok
  }
  return ret;
}

int ObDatabaseSchema::get_primary_zone_inherit(ObSchemaGetterGuard& schema_guard, ObPrimaryZone& primary_zone) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_primary_zone = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_;
  primary_zone.reset();

  if (OB_FAIL(ret)) {
  } else if (!get_primary_zone().empty() && !use_tenant_primary_zone) {
    if (OB_FAIL(primary_zone.set_primary_zone_array(get_primary_zone_array()))) {
      LOG_WARN("fail to set primary zone array", K(ret));
    } else if (OB_FAIL(primary_zone.set_primary_zone(get_primary_zone()))) {
      LOG_WARN("fail to set primary zone", K(ret));
    }
  } else {
    const ObTenantSchema* tenant_schema = NULL;
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
      SchemaZoneReplicaAttrSet& zone_locality = zone_replica_attr_array_.at(i);
      schema_->reset_string_array(zone_locality.zone_set_);
      SchemaReplicaAttrArray& full_attr_set =
          static_cast<SchemaReplicaAttrArray&>(zone_locality.replica_attr_set_.get_full_replica_attr_array());
      if (nullptr != full_attr_set.get_base_address()) {
        schema_->free(full_attr_set.get_base_address());
        full_attr_set.reset();
      }
      SchemaReplicaAttrArray& logonly_attr_set =
          static_cast<SchemaReplicaAttrArray&>(zone_locality.replica_attr_set_.get_logonly_replica_attr_array());
      if (nullptr != logonly_attr_set.get_base_address()) {
        schema_->free(logonly_attr_set.get_base_address());
        logonly_attr_set.reset();
      }
      SchemaReplicaAttrArray& readonly_attr_set =
          static_cast<SchemaReplicaAttrArray&>(zone_locality.replica_attr_set_.get_readonly_replica_attr_array());
      if (nullptr != readonly_attr_set.get_base_address()) {
        schema_->free(readonly_attr_set.get_base_address());
        readonly_attr_set.reset();
      }
    }
    schema_->free(zone_replica_attr_array_.get_base_address());
    zone_replica_attr_array_.reset();
  }
}

int ObLocality::set_specific_replica_attr_array(
    SchemaReplicaAttrArray& this_schema_set, const common::ObIArray<ReplicaAttr>& src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.count();
  if (count > 0) {
    const int64_t size = count * static_cast<int64_t>(sizeof(share::ReplicaAttr));
    void* ptr = nullptr;
    if (nullptr == (ptr = schema_->alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(size));
    } else if (FALSE_IT(this_schema_set.init(count, static_cast<ReplicaAttr*>(ptr), count))) {
      // shall never by here
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
        const share::ReplicaAttr& src_replica_attr = src.at(i);
        ReplicaAttr* dst_replica_attr = &this_schema_set.at(i);
        if (nullptr == (dst_replica_attr = new (dst_replica_attr)
                               ReplicaAttr(src_replica_attr.num_, src_replica_attr.memstore_percent_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("placement new return nullptr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocality::set_zone_replica_attr_array(const common::ObIArray<SchemaZoneReplicaAttrSet>& src)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else {
    reset_zone_replica_attr_array();
    const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
    void* buf = NULL;
    if (src.count() <= 0) {
      // do nothing
    } else if (NULL == (buf = schema_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(alloc_size));
    } else {
      zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet*>(buf), src.count());
      for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
        const SchemaZoneReplicaAttrSet& src_replica_attr_set = src.at(i);
        SchemaZoneReplicaAttrSet* this_schema_set = &zone_replica_attr_array_.at(i);
        if (nullptr == (this_schema_set = new (this_schema_set) SchemaZoneReplicaAttrSet())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("placement new return nullptr", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                       static_cast<SchemaReplicaAttrArray&>(
                           this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
                       src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                       static_cast<SchemaReplicaAttrArray&>(
                           this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
                       src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                       static_cast<SchemaReplicaAttrArray&>(
                           this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
                       src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(
                       schema_->deep_copy_string_array(src_replica_attr_set.zone_set_, this_schema_set->zone_set_))) {
          LOG_WARN("fail to copy schema replica attr set zone set", K(ret));
        } else {
          this_schema_set->zone_ = src_replica_attr_set.zone_;
        }
      }
    }
  }
  return ret;
}

int ObLocality::set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet>& src)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else {
    reset_zone_replica_attr_array();
    const int64_t alloc_size = src.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
    void* buf = NULL;
    if (src.count() <= 0) {
      // do nothing
    } else if (NULL == (buf = schema_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc failed", K(ret), K(alloc_size));
    } else {
      zone_replica_attr_array_.init(src.count(), static_cast<SchemaZoneReplicaAttrSet*>(buf), src.count());
      for (int64_t i = 0; i < src.count() && OB_SUCC(ret); ++i) {
        const share::ObZoneReplicaAttrSet& src_replica_attr_set = src.at(i);
        SchemaZoneReplicaAttrSet* this_schema_set = &zone_replica_attr_array_.at(i);
        if (nullptr == (this_schema_set = new (this_schema_set) SchemaZoneReplicaAttrSet())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("placement new return nullptr", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                       static_cast<SchemaReplicaAttrArray&>(
                           this_schema_set->replica_attr_set_.get_full_replica_attr_array()),
                       src_replica_attr_set.replica_attr_set_.get_full_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                       static_cast<SchemaReplicaAttrArray&>(
                           this_schema_set->replica_attr_set_.get_logonly_replica_attr_array()),
                       src_replica_attr_set.replica_attr_set_.get_logonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else if (OB_FAIL(set_specific_replica_attr_array(
                       static_cast<SchemaReplicaAttrArray&>(
                           this_schema_set->replica_attr_set_.get_readonly_replica_attr_array()),
                       src_replica_attr_set.replica_attr_set_.get_readonly_replica_attr_array()))) {
          LOG_WARN("fail to set specific replica attr array", K(ret));
        } else {
          common::ObArray<common::ObString> zone_set_ptrs;
          for (int64_t j = 0; OB_SUCC(ret) && j < src_replica_attr_set.zone_set_.count(); ++j) {
            const common::ObZone& zone = src_replica_attr_set.zone_set_.at(j);
            if (OB_FAIL(zone_set_ptrs.push_back(common::ObString(zone.size(), zone.ptr())))) {
              LOG_WARN("fail to push back", K(ret));
            } else {
            }  // no more to do
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

int ObLocality::assign(const ObLocality& other)
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

int ObLocality::set_locality_str(const ObString& other)
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
    LOG_ERROR("invalid schema info", K(schema_));
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
  if (!OB_SUCC(ret)) {
    LOG_WARN("func_SERIALIZE failed", K(ret));
  } else {
  }  // no more to do
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

int ObPrimaryZone::set_primary_zone_array(const common::ObIArray<ObZoneScore>& primary_zone_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else {
    primary_zone_array_.reset();
    int64_t count = primary_zone_array.count();
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      const ObZoneScore& zone_score = primary_zone_array.at(i);
      ObString str;
      if (OB_FAIL(schema_->deep_copy_str(zone_score.zone_, str))) {
        LOG_WARN("deep copy str failed", K(ret));
      } else if (OB_FAIL(primary_zone_array_.push_back(ObZoneScore(str, zone_score.score_)))) {
        LOG_WARN("fail to push back", K(ret));
        for (int64_t j = 0; j < primary_zone_array_.count(); ++j) {
          schema_->free(primary_zone_array_.at(j).zone_.ptr());
        }
        schema_->free(str.ptr());
      } else {
      }  // ok
    }
  }
  return ret;
}

int ObPrimaryZone::set_primary_zone(const ObString& zone)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else if (OB_FAIL(schema_->deep_copy_str(zone, primary_zone_str_))) {
    LOG_WARN("fail to assign primary zone", K(ret));
  }
  return ret;
}

int ObPrimaryZone::assign(const ObPrimaryZone& other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_));
  } else if (OB_FAIL(schema_->deep_copy_str(other.primary_zone_str_, primary_zone_str_))) {
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
  if (OB_ISNULL(schema_)) {
    LOG_ERROR("invalid schema_ info", K(schema_));
  } else {
    if (!OB_ISNULL(primary_zone_str_.ptr())) {
      schema_->free(primary_zone_str_.ptr());
    }
    for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
      if (!OB_ISNULL(primary_zone_array_.at(i).zone_.ptr())) {
        schema_->free(primary_zone_array_.at(i).zone_.ptr());
      }
    }
    primary_zone_str_.reset();
    primary_zone_array_.reset();
  }
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
  if (OB_ISNULL(schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema", K(ret));
  } else if (OB_FAIL(schema_->deep_copy_str(primary_zone, primary_zone_str_))) {
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

ObPartitionSchema::ObPartitionSchema() : ObSchema()
{
  reset();
}

ObPartitionSchema::ObPartitionSchema(common::ObIAllocator* allocator)
    : ObSchema(allocator), part_option_(allocator), sub_part_option_(allocator)
{
  reset();
}

ObPartitionSchema::ObPartitionSchema(const ObPartitionSchema& src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObPartitionSchema::~ObPartitionSchema()
{}

ObPartitionSchema& ObPartitionSchema::operator=(const ObPartitionSchema& src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    part_num_ = src_schema.part_num_;
    def_subpart_num_ = src_schema.def_subpart_num_;
    part_level_ = src_schema.part_level_;
    partition_schema_version_ = src_schema.partition_schema_version_;
    partition_status_ = src_schema.partition_status_;
    drop_schema_version_ = src_schema.drop_schema_version_;
    is_sub_part_template_ = src_schema.is_sub_part_template_;

    if (OB_SUCC(ret)) {
      part_option_ = src_schema.part_option_;
      if (OB_FAIL(part_option_.get_err_ret())) {
        LOG_WARN("fail to assign part_option", K(ret), K_(part_option), K(src_schema.part_option_));
      }
    }

    if (OB_SUCC(ret)) {
      sub_part_option_ = src_schema.sub_part_option_;
      if (OB_FAIL(sub_part_option_.get_err_ret())) {
        LOG_WARN("fail to assign sub_part_option", K(ret), K_(sub_part_option), K(src_schema.sub_part_option_));
      }
    }

    // partitions array
    if (OB_SUCC(ret)) {
      int64_t partition_num = src_schema.partition_num_;
      if (partition_num > 0) {
        partition_array_ = static_cast<ObPartition**>(alloc(sizeof(ObPartition*) * partition_num));
        if (OB_ISNULL(partition_array_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for partition_array_", K(ret));
        } else if (OB_ISNULL(src_schema.partition_array_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src_schema.partition_array_ is null", K(ret));
        } else {
          partition_array_capacity_ = partition_num;
        }
      }
      ObPartition* partition = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        partition = src_schema.partition_array_[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the partition is null", K(ret));
        } else if (OB_FAIL(add_partition(*partition))) {
          LOG_WARN("Fail to add partition", K(ret), K(i));
        }
      }
    }
    // def subpartitions array
    if (OB_SUCC(ret)) {
      int64_t def_subpartition_num = src_schema.def_subpartition_num_;
      if (def_subpartition_num > 0) {
        def_subpartition_array_ = static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * def_subpartition_num));
        if (OB_ISNULL(def_subpartition_array_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for def_subpartition_array_", K(ret), K(def_subpartition_num));
        } else if (OB_ISNULL(src_schema.def_subpartition_array_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src_schema.def_subpartition_array_ is null", K(ret));
        } else {
          def_subpartition_array_capacity_ = def_subpartition_num;
        }
      }
      ObSubPartition* subpartition = NULL;
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
    // dropped partitions array
    if (OB_SUCC(ret)) {
      int64_t dropped_partition_num = src_schema.dropped_partition_num_;
      if (dropped_partition_num > 0) {
        dropped_partition_array_ = static_cast<ObPartition**>(alloc(sizeof(ObPartition*) * dropped_partition_num));
        if (OB_ISNULL(dropped_partition_array_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for dropped_partition_array_", K(ret));
        } else if (OB_ISNULL(src_schema.dropped_partition_array_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src_schema.dropped_partition_array_ is null", K(ret));
        } else {
          dropped_partition_array_capacity_ = dropped_partition_num;
        }
      }
      ObPartition* partition = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < dropped_partition_num; i++) {
        partition = src_schema.dropped_partition_array_[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the partition is null", K(ret));
        } else if (OB_FAIL(add_partition(*partition))) {
          LOG_WARN("Fail to add partition", K(ret), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

int ObPartitionSchema::try_assign_part_array(const share::schema::ObPartitionSchema& that)
{
  int ret = OB_SUCCESS;
  if (nullptr != partition_array_) {
    for (int64_t i = 0; i < partition_num_; ++i) {
      ObPartition* this_part = partition_array_[i];
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
  part_num_ = that.get_part_option().get_part_num();
  part_option_ = that.get_part_option();
  if (OB_FAIL(part_option_.get_err_ret())) {
    LOG_WARN("fail to assign part option", K(ret), K(part_option_));
  } else {
    int64_t partition_num = that.get_partition_num();
    if (partition_num > 0) {
      partition_array_ = static_cast<ObPartition**>(alloc(sizeof(ObPartition*) * partition_num));
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
    ObPartition* partition = NULL;
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

int ObPartitionSchema::try_assign_def_subpart_array(const share::schema::ObPartitionSchema& that)
{
  int ret = OB_SUCCESS;
  if (nullptr != def_subpartition_array_) {
    for (int64_t i = 0; i < def_subpartition_num_; ++i) {
      ObSubPartition* this_part = def_subpartition_array_[i];
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
  def_subpart_num_ = that.get_sub_part_option().get_part_num();
  sub_part_option_ = that.get_sub_part_option();
  if (OB_FAIL(sub_part_option_.get_err_ret())) {
    LOG_WARN("fail to assign part option", K(ret), K(sub_part_option_));
  } else {
    int64_t def_subpartition_num = that.get_def_subpartition_num();
    if (def_subpartition_num > 0) {
      def_subpartition_array_ = static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * def_subpartition_num));
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
    ObSubPartition* subpartition = NULL;
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

bool ObPartitionSchema::is_valid() const
{
  return ObSchema::is_valid();
}

void ObPartitionSchema::reset()
{
  part_num_ = 0;
  def_subpart_num_ = 0;
  part_level_ = PARTITION_LEVEL_ZERO;
  part_option_.reset();
  sub_part_option_.reset();
  partition_array_ = NULL;
  partition_array_capacity_ = 0;
  partition_num_ = 0;
  def_subpartition_array_ = NULL;
  def_subpartition_num_ = 0;
  def_subpartition_array_capacity_ = 0;
  partition_schema_version_ = 0;
  partition_status_ = PARTITION_STATUS_ACTIVE;
  drop_schema_version_ = OB_INVALID_VERSION;
  dropped_partition_array_ = NULL;
  dropped_partition_array_capacity_ = 0;
  dropped_partition_num_ = 0;
  is_sub_part_template_ = true;
  ObSchema::reset();
}

int64_t ObPartitionSchema::distribute_by() const
{
  int by = 0;
  ObPartitionLevel part_level = get_part_level();
  switch (part_level) {
    case PARTITION_LEVEL_ZERO:
      by = 0;
      break;
    case PARTITION_LEVEL_ONE:
      by = 1;
      break;
    case PARTITION_LEVEL_TWO:
      if ((is_hash_part() || is_key_part()) && is_sub_part_template()) {
        by = 1;
      } else if (is_hash_subpart() || is_key_subpart()) {
        by = 2;
      } else {
        by = 2;
        // FIXME: If there is no hash / key partition in the primary and secondary partitions,
        //  the secondary partition will be broken up by default
      }
      break;
    default:
      by = 0;
      break;
  }
  return by;
}

// FIXME:() Non-templated table need to disable this interface
int64_t ObPartitionSchema::get_def_sub_part_num() const
{
  int64_t num = 0;
  if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template()) {
    num = OB_INVALID_ID;
  } else {
    num = sub_part_option_.get_part_num();
  }
  return num;
}

// TODO:The first phase of the second-level partition heterogeneity requires the same definition of non-templated
// second-level partitions. This function can be used to obtain the number of second-level partitions.
// After subsequent support for true heterogeneity, all functions that call this interface must be changed.
int ObPartitionSchema::get_first_individual_sub_part_num(int64_t& sub_part_num) const
{
  int ret = OB_SUCCESS;
  sub_part_num = OB_INVALID_ID;
  if (is_sub_part_template()) {
    sub_part_num = get_def_sub_part_num();
  } else if (OB_ISNULL(partition_array_) || OB_UNLIKELY(get_first_part_num() < 1) || OB_ISNULL(partition_array_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    sub_part_num = partition_array_[0]->get_sub_part_num();
  }
  return ret;
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
      if (is_sub_part_template()) {
        num = get_first_part_num() * get_def_sub_part_num();
      } else {
        num = 0;
        int64_t partition_num = get_partition_num();
        int ret = OB_SUCCESS;
        if (partition_num <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("partition num should greator than 0", K(ret), K(partition_num));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
            const ObPartition* partition = get_part_array()[i];
            if (OB_ISNULL(partition)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("partition is null", K(ret), K(i));
            } else {
              num += partition->get_sub_part_num();
            }
          }
        }
        num = OB_FAIL(ret) ? -1 : num;
      }
      break;
    }
    default: {
      LOG_WARN("invalid partition level", K_(part_level));
      break;
    }
  }
  return num;
}

int ObPartitionSchema::get_all_partition_num(bool check_dropped_partition /* = false */, int64_t& part_num) const
{
  int ret = OB_SUCCESS;
  part_num = 1;
  switch (part_level_) {
    case PARTITION_LEVEL_ZERO: {
      break;
    }
    case PARTITION_LEVEL_ONE: {
      part_num = get_first_part_num();
      if (check_dropped_partition) {
        part_num += get_dropped_partition_num();
      }
      break;
    }
    case PARTITION_LEVEL_TWO: {
      if (is_sub_part_template()) {
        part_num = get_first_part_num() * get_def_sub_part_num();
        if (check_dropped_partition) {
          part_num += get_dropped_partition_num() * get_def_sub_part_num();
        }
      } else {
        part_num = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < get_partition_num(); i++) {
          const ObPartition* part = get_part_array()[i];
          if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), K(i));
          } else {
            part_num += part->get_subpartition_num();
            if (check_dropped_partition) {
              part_num += part->get_dropped_subpartition_num();
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < get_dropped_partition_num(); i++) {
          const ObPartition* part = get_dropped_part_array()[i];
          if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), K(i));
          } else {
            part_num += part->get_subpartition_num();
            if (check_dropped_partition) {
              part_num += part->get_dropped_subpartition_num();
            }
          }
        }
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

int ObPartitionSchema::add_def_subpartition(const ObSubPartition& subpartition)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add def subpartition while is not is_sub_part_template", K(ret));
  } else {
    ObSubPartition* local = OB_NEWx(ObSubPartition, (get_allocator()), (get_allocator()));
    if (NULL == local) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_FAIL(local->assign(subpartition))) {
      LOG_WARN("failed to assign partition", K(ret));
    } else if (OB_FAIL(inner_add_partition(*local))) {
      LOG_WARN("add subpartition failed", K(subpartition), K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::check_part_name(const ObPartition& partition)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; ++i) {
    const ObString& part_name = partition.get_part_name();
    if (common::ObCharset::case_insensitive_equal(part_name, partition_array_[i]->get_part_name())) {
      ret = OB_ERR_SAME_NAME_PARTITION;
      LOG_WARN("part name is duplicate", K(ret), K(partition), K(i), "exists partition", partition_array_[i]);
      LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, part_name.length(), part_name.ptr());
    }
  }
  return ret;
}

int ObPartitionSchema::check_part_id(const ObPartition& partition)
{
  int ret = OB_SUCCESS;
  // Check if the part ID conflicts, backup and restore need to use
  if (OB_INVALID_INDEX == partition.get_part_id()) {
    // Part ID is processed at the rs layer when drop partition, this is not checked
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; ++i) {
      if (partition.get_part_id() == partition_array_[i]->get_part_id()) {
        ret = OB_ERR_SAME_NAME_PARTITION_FIELD;
        LOG_WARN("part id is duplicate", K(ret), K(partition), K(i), "exists partition", partition_array_[i]);
        LOG_USER_ERROR(
            OB_ERR_SAME_NAME_PARTITION_FIELD, partition.get_part_name().length(), partition.get_part_name().ptr());
      }
    }
  }
  return ret;
}

int ObPartitionSchema::add_partition(const ObPartition& partition)
{
  int ret = OB_SUCCESS;
  ObPartition* new_part = OB_NEWx(ObPartition, (get_allocator()), (get_allocator()));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory", K(ret));
  } else if (OB_FAIL(new_part->assign(partition))) {
    LOG_WARN("failed to assign partition", K(ret));
  } else if (OB_FAIL(inner_add_partition(*new_part))) {
    LOG_WARN("add partition failed", KPC(new_part), K(ret));
  } else {
    LOG_DEBUG("add partition succ", K(ret), K(partition), KPC(new_part));
  }
  return ret;
}

int ObPartitionSchema::inner_add_partition(const ObPartition& part)
{
  int ret = OB_SUCCESS;
  if (part.is_dropped_partition()) {
    if (OB_FAIL(inner_add_partition(
            part, dropped_partition_array_, dropped_partition_array_capacity_, dropped_partition_num_))) {
      LOG_WARN("add dropped partition failed", K(ret), K(part));
    }
  } else {
    if (OB_FAIL(inner_add_partition(part, partition_array_, partition_array_capacity_, partition_num_))) {
      LOG_WARN("add partition failed", K(ret), K(part));
    }
  }
  return ret;
}

int ObPartitionSchema::inner_add_partition(const ObSubPartition& part)
{
  return inner_add_partition(part, def_subpartition_array_, def_subpartition_array_capacity_, def_subpartition_num_);
}

template <typename T>
int ObPartitionSchema::inner_add_partition(
    const T& part, T**& part_array, int64_t& part_array_capacity, int64_t& part_num)
{
  int ret = common::OB_SUCCESS;
  if (0 == part_array_capacity) {
    if (NULL == (part_array = static_cast<T**>(alloc(sizeof(T*) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition arrary");
    } else {
      part_array_capacity = DEFAULT_ARRAY_CAPACITY;
    }
  } else if (part_num >= part_array_capacity) {
    int64_t new_size = 2 * part_array_capacity;
    T** tmp = NULL;
    if (NULL == (tmp = static_cast<T**>(alloc((sizeof(T*) * new_size))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition array", K(new_size));
    } else {
      MEMCPY(tmp, part_array, sizeof(T*) * part_num);
      MEMSET(tmp + part_num, 0, sizeof(T*) * (new_size - part_num));
      free(part_array);
      part_array = tmp;
      part_array_capacity = new_size;
    }
  }
  if (OB_SUCC(ret)) {
    part_array[part_num] = const_cast<T*>(&part);
    ++part_num;
  }
  return ret;
}

int ObPartitionSchema::serialize_partitions(char* buf, const int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, partition_num_))) {
    LOG_WARN("Fail to encode partition count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
    if (OB_ISNULL(partition_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_array_ element is null", K(ret));
    } else if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template() &&
               partition_array_[i]->get_sub_part_num() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition's sub_part_num should greator than 0 while is not is_sub_part_template",
          K(ret),
          "sub_part_num",
          partition_array_[i]->get_sub_part_num());
    } else if (OB_FAIL(partition_array_[i]->serialize(buf, data_len, pos))) {
      LOG_WARN("Fail to serialize partition", K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::serialize_def_subpartitions(char* buf, const int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template() && def_subpartition_num_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize def subpartition while is not is_sub_part_template", K(ret), K_(def_subpartition_num));
  } else if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, def_subpartition_num_))) {
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

int ObPartitionSchema::serialize_dropped_partitions(char* buf, const int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, dropped_partition_num_))) {
    LOG_WARN("Fail to encode dropped partition count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dropped_partition_num_; i++) {
    if (OB_ISNULL(dropped_partition_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dropped_partition_array_ element is null", K(ret));
    } else if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template() &&
               dropped_partition_array_[i]->get_sub_part_num() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition's sub_part_num should greator than 0 while is not is_sub_part_template",
          K(ret),
          "sub_part_num",
          dropped_partition_array_[i]->get_sub_part_num());
    } else if (OB_FAIL(dropped_partition_array_[i]->serialize(buf, data_len, pos))) {
      LOG_WARN("Fail to serialize dropped partition", K(ret));
    }
  }
  return ret;
}

int ObPartitionSchema::deserialize_partitions(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("Fail to decode partition count", K(ret));
  } else {
    ObPartition partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      partition.reset();
      if (OB_FAIL(partition.deserialize(buf, data_len, pos))) {
        LOG_WARN("Fail to deserialize partition", K(ret));
      } else if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template() &&
                 partition.get_sub_part_num() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition's sub_part_num should greator than 0 while is not is_sub_part_template",
            K(ret),
            "sub_part_num",
            partition.get_sub_part_num());
      } else if (OB_FAIL(add_partition(partition))) {
        LOG_WARN("Fail to add partition", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionSchema::deserialize_def_subpartitions(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("Fail to decode partition count", K(ret));
  } else if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template() && count > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialize def subpartition while is not is_sub_part_template", K(ret), K(count));
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

int64_t ObPartitionSchema::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(part_level),
      K_(part_option),
      K_(sub_part_option),
      K_(part_num),
      K_(def_subpart_num),
      K_(partition_num),
      K_(def_subpartition_num),
      K_(partition_status),
      K_(partition_schema_version),
      K_(drop_schema_version),
      "partition_array",
      ObArrayWrap<ObPartition*>(partition_array_, partition_num_),
      "def_subpartition_array",
      ObArrayWrap<ObSubPartition*>(def_subpartition_array_, def_subpartition_num_),
      "dropped_partition_array",
      ObArrayWrap<ObPartition*>(dropped_partition_array_, dropped_partition_num_),
      K_(is_sub_part_template));
  J_OBJ_END();
  return pos;
}

// FIXME:() It is not assumed here that the partition_array/subpartition_array are in order, to be optimized
int ObPartitionSchema::get_subpartition(
    const int64_t part_id, const int64_t subpart_id, const ObSubPartition*& subpartition) const
{
  int ret = OB_SUCCESS;
  const ObPartition* partition = NULL;
  bool check_dropped_partition = false;
  subpartition = NULL;
  if (PARTITION_LEVEL_TWO != get_part_level()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get subpartition while is part level is not level two", K(ret), "part_level", get_part_level());
  } else if (is_sub_part_template()) {
    UNUSED(
        part_id);  // Compatible with the case where the first-level hash does not exist in the resolver and ddl process
    if (OB_ISNULL(def_subpartition_array_) || subpart_id >= def_subpartition_num_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("subpartition_array is null or subpart_id is invalid",
          K(ret),
          KP_(def_subpartition_array),
          K_(def_subpartition_num));
    } else {
      // Templated secondary partitions, adding or deleting secondary partitions is not allowed, subpart_id is equal to
      // the array subscript
      subpartition = def_subpartition_array_[subpart_id];
    }
  } else {
    if (OB_FAIL(find_partition_by_part_id(part_id, check_dropped_partition, partition))) {
      LOG_WARN("fail to find partition by part_id", K(ret), K(part_id));
    } else if (OB_ISNULL(partition)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("partition is null", K(ret));
    } else {
      ObSubPartition** subpartition_array = partition->get_subpart_array();
      int64_t subpartition_num = partition->get_subpartition_num();
      if (OB_ISNULL(subpartition_array) || subpartition_num <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subpart_array is null ord subpartition_num is invalid",
            K(ret),
            KP(subpartition_array),
            K(subpartition_num));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; i++) {
          const ObSubPartition* subpart = subpartition_array[i];
          if (OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpartition is null", K(ret), K(i));
          } else if (subpart->get_sub_part_id() == subpart_id) {
            subpartition = subpart;
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionSchema::gen_hash_part_name(const int64_t part_id, const ObHashNameType name_type,
    const bool need_upper_case, char* buf, const int64_t buf_size, int64_t* pos, const ObPartition* partition) const
{
  int ret = OB_SUCCESS;
  if (NULL != pos) {
    *pos = 0;
  }
  int64_t part_name_size = 0;
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_size));
  } else if (OB_UNLIKELY(part_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid subpart id", K(part_id), K(ret));
  } else if (FIRST_PART == name_type) {
    if (!is_hash_like_part()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("should be hash like", K(ret), K(part_id));
    } else if (need_upper_case) {
      part_name_size += snprintf(buf, buf_size, "P%ld", part_id);
    } else {
      part_name_size += snprintf(buf, buf_size, "p%ld", part_id);
    }
  } else if (TEMPLATE_SUB_PART == name_type) {
    if (!is_hash_like_subpart()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("should be hash like", K(ret), K(part_id));
    } else if (need_upper_case) {
      part_name_size += snprintf(buf, buf_size, "P%ld", part_id);
    } else {
      part_name_size += snprintf(buf, buf_size, "p%ld", part_id);
    }
  } else if (INDIVIDUAL_SUB_PART == name_type) {
    if (!is_hash_like_subpart()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("should be hash like", K(ret), K(part_id));
    } else if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null partition", K(ret));
    } else {
      part_name_size += snprintf(buf, buf_size, "%s", partition->get_part_name().ptr());
      if (need_upper_case) {
        part_name_size += snprintf(buf + part_name_size, buf_size - part_name_size, "SP%ld", part_id);
      } else {
        part_name_size += snprintf(buf + part_name_size, buf_size - part_name_size, "sp%ld", part_id);
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

int ObPartitionSchema::get_partition_name(
    const int64_t partition_id, char* buf, const int64_t buf_size, int64_t* pos /* = NULL*/) const
{
  int ret = OB_SUCCESS;
  if (NULL != pos) {
    *pos = 0;
  }
  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    if (0 != partition_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition_id", K(ret), K(partition_id));
    } else {
      int64_t pname_size = snprintf(buf, buf_size, "p0");
      if ((OB_UNLIKELY(pname_size <= 0 || pname_size >= buf_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pname size is invalid", K(ret), K(pname_size), K(buf_size));
      } else if (NULL != pos) {
        *pos = pname_size;
      }
    }
  } else if (PARTITION_LEVEL_ONE == get_part_level()) {
    if (is_twopart(partition_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition_id", K(ret), K(partition_id));
    } else if (OB_FAIL(get_part_name(partition_id, buf, buf_size, pos))) {
      LOG_WARN("fail to get part_name", K(ret), K(partition_id));
    }
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    int64_t part_id = extract_part_idx(partition_id);
    int64_t subpart_id = extract_subpart_idx(partition_id);
    if (!is_twopart(partition_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition_id", K(ret), K(partition_id));
    } else if (!is_sub_part_template()) {
      if (OB_FAIL(get_subpart_name(part_id, subpart_id, buf, buf_size, pos))) {
        LOG_WARN("fail to get sub part name", K(ret), K(partition_id), K(part_id), K(subpart_id));
      }
    } else {
      int64_t pname_size = 0;
      if (OB_FAIL(get_part_name(part_id, buf, buf_size, &pname_size))) {
        LOG_WARN("fail to get part_name", K(ret), K(partition_id));
      } else {
        int n = snprintf(buf + pname_size, buf_size - pname_size, "s");
        if (OB_UNLIKELY(n <= 0 || n >= buf_size - pname_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("size is invalid", K(ret), K(n), K(buf_size), K(pname_size));
        } else {
          pname_size += n;
          int64_t subpart_name_size = 0;
          if (OB_FAIL(get_def_subpart_name(subpart_id, buf + pname_size, buf_size - pname_size, &subpart_name_size))) {
            LOG_WARN("fail to get def subpart name", K(ret), K(part_id), K(subpart_id));
          } else if (NULL != pos) {
            *pos = pname_size + subpart_name_size;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionSchema::get_part_name(
    const int64_t part_id, char* buf, const int64_t buf_size, int64_t* pos /* = NULL */) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_size));
  } else if (part_id < 0 || PARTITION_LEVEL_ZERO == get_part_level()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(part_id), "part_level", get_part_level());
  } else {
    if (NULL != pos) {
      *pos = 0;
    }
    int64_t pname_size = 0;
    ObString part_name;
    if (OB_ISNULL(partition_array_)) {
      if (is_hash_like_part()) {
        // FIXME() Partition_array is not specified in the first-level hash when the table is built
        if (is_oracle_mode()) {
          pname_size = snprintf(buf, buf_size, "P%ld", part_id);
        } else {
          pname_size = snprintf(buf, buf_size, "p%ld", part_id);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null", K(ret), K(partition_array_));
      }
    } else {
      // Compatible with table building scenarios, does not assume the order of partition_array
      bool found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; ++i) {
        if (OB_ISNULL(partition_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter is null", K(ret), K(partition_array_[i]));
        } else if (partition_array_[i]->get_part_id() == part_id) {
          part_name = partition_array_[i]->get_part_name();
          found = true;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (!found) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("part idx not found", K(ret), K(part_id));
        } else {
          pname_size = snprintf(buf, buf_size, "%s", part_name.ptr());
        }
      }
    }
    if (OB_SUCC(ret)) {
      // pname_size >= buf_size indicate overflow
      if (OB_UNLIKELY(pname_size <= 0 || pname_size >= buf_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pname size is invalid", K(pname_size), K(part_name), K(buf_size));
      } else if (NULL != pos) {
        *pos = pname_size;
      }
    }
  }
  return ret;
}

int ObPartitionSchema::get_subpart_name(const int64_t part_id, const int64_t subpart_id, const bool is_def_subpart,
    char* buf, const int64_t buf_size, int64_t* pos /* = NULL*/) const
{
  int ret = OB_SUCCESS;
  if (NULL != pos) {
    *pos = 0;
  }
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_size));
  } else if (OB_UNLIKELY(subpart_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid subpart id", K(subpart_id), K(ret));
  } else if (is_def_subpart) {
    if (OB_FAIL(get_def_subpart_name(subpart_id, buf, buf_size, pos))) {
      LOG_WARN("fail to gen subpart name", K(ret), K(part_id), K(subpart_id));
    }
  } else {
    if (OB_FAIL(get_subpart_name(part_id, subpart_id, buf, buf_size, pos))) {
      LOG_WARN("fail to subpart name", K(ret), K(part_id), K(subpart_id));
    }
  }
  return ret;
}

int ObPartitionSchema::get_subpart_name(
    const int64_t part_id, const int64_t subpart_id, char* buf, const int64_t buf_size, int64_t* pos /* = NULL*/) const
{
  int ret = OB_SUCCESS;
  int64_t subpname_size = 0;
  if (NULL != pos) {
    *pos = 0;
  }
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_size));
  } else if (OB_UNLIKELY(subpart_id < 0 || part_id < 0 || PARTITION_LEVEL_TWO != get_part_level())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(part_id), K(subpart_id), "part_level", get_part_level(), K(ret));
  } else if (is_sub_part_template()) {
    int64_t pname_pos = 0;
    // get part name
    int64_t max_used_part_id = get_part_option().get_max_used_part_id();
    if (max_used_part_id < 0) {
      max_used_part_id = get_first_part_num() - 1;
    }
    if (OB_UNLIKELY(part_id < 0) || part_id > max_used_part_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part id", K(part_id), K(max_used_part_id));
    } else if (OB_FAIL(get_part_name(part_id, buf, buf_size, &pname_pos))) {
      LOG_WARN("get part name failed", K(ret));
    } else {
      // get subpart name
      ObString subpart_name;
      if (is_range_subpart() || is_list_subpart()) {
        const ObSubPartition* subpartition = NULL;
        if (OB_FAIL(get_subpartition(part_id, subpart_id, subpartition))) {
          LOG_WARN("fail to find subpartition", K(ret), K(part_id), K(subpart_id));
        } else if (OB_ISNULL(subpartition)) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("subpartition not exist", K(ret), K(part_id), K(subpart_id));
        } else {
          subpart_name = subpartition->get_part_name();
          subpname_size = pname_pos;
          subpname_size += snprintf(buf + pname_pos, buf_size - pname_pos, "s%s", subpart_name.ptr());
        }
      } else {
        // FIXME() Create table secondary hash without specifying def_subpartition_array
        subpname_size = pname_pos;
        subpname_size += snprintf(buf + pname_pos, buf_size - pname_pos, "sp%ld", subpart_id);
      }
    }
  } else {
    ObString subpart_name;
    const ObSubPartition* subpartition = NULL;
    if (OB_FAIL(get_subpartition(part_id, subpart_id, subpartition))) {
      LOG_WARN("fail to find subpartition", K(ret), K(part_id), K(subpart_id));
    } else if (OB_ISNULL(subpartition)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("subpartition not exist", K(ret), K(part_id), K(subpart_id));
    } else {
      subpart_name = subpartition->get_part_name();
      subpname_size += snprintf(buf, buf_size, "%s", subpart_name.ptr());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(subpname_size <= 0 || subpname_size >= buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pname size is invalid", K(ret), K(subpname_size), K(buf_size));
  } else if (NULL != pos) {
    *pos = subpname_size;
  }
  return ret;
}

int ObPartitionSchema::get_def_subpart_name(
    const int64_t subpart_id, char* buf, const int64_t buf_size, int64_t* pos /* = NULL*/) const
{
  int ret = OB_SUCCESS;
  if (NULL != pos) {
    *pos = 0;
  }
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_size));
  } else if (OB_UNLIKELY(subpart_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid subpart id", K(subpart_id), K(ret));
  } else if (PARTITION_LEVEL_TWO != get_part_level() || !is_sub_part_template()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sub_part_type or invalid part level",
        K(ret),
        "is_sub_part_template",
        is_sub_part_template(),
        "part_level",
        get_part_level());
  } else {
    int64_t subpname_size = 0;
    if (is_range_subpart() || is_list_subpart()) {
      if (OB_ISNULL(def_subpartition_array_) || OB_ISNULL(def_subpartition_array_[subpart_id])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null", K(ret));
      } else if (subpart_id >= def_subpartition_num_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid subpart id", K(subpart_id), K_(def_subpartition_num), K(ret));
      } else {
        ObString subpart_name;
        subpart_name = def_subpartition_array_[subpart_id]->get_part_name();
        subpname_size += snprintf(buf, buf_size, "%s", subpart_name.ptr());
      }
    } else if (is_oracle_mode()) {
      subpname_size += snprintf(buf, buf_size, "P%ld", subpart_id);
    } else {
      // FIXME() Create table secondary hash without specifying def_subpartition_array
      subpname_size += snprintf(buf, buf_size, "p%ld", subpart_id);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(subpname_size <= 0 || subpname_size >= buf_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pname size is invalid", K(ret), K(subpname_size), K(buf_size));
    } else if (NULL != pos) {
      *pos = subpname_size;
    }
  }
  return ret;
}

int ObPartitionSchema::find_partition_by_part_id(
    const int64_t part_id, const bool check_dropped_partition, const ObPartition*& partition) const
{
  int ret = OB_SUCCESS;
  int64_t partition_index = OB_INVALID_INDEX;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_id));
  } else if (partition_num_ < 0 || dropped_partition_num_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition num is invalid", K(ret), K(part_id), K_(partition_num), K_(dropped_partition_num));
  } else if (OB_ISNULL(partition_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition array", K(ret));
  } else if (OB_FAIL(get_partition_index_loop(part_id, check_dropped_partition, partition_index))) {
    LOG_WARN("failed to get partition index by id", K(ret), K(part_id));
  } else if (partition_index < partition_num_) {
    partition = partition_array_[partition_index];
  } else if (check_dropped_partition) {
    partition_index = partition_index - partition_num_;
    if (partition_index >= dropped_partition_num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid idx", K(ret), K(part_id), K(partition_index), K_(partition_num), K_(dropped_partition_num));
    } else {
      partition = dropped_partition_array_[partition_index];
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(partition) || partition->get_part_id() != part_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition", K(ret), KPC(partition), K(part_id));
  }
  return ret;
}

int ObPartitionSchema::get_partition_by_part_id(
    const int64_t part_id, const bool check_dropped_partition, const ObPartition*& partition) const
{
  int ret = OB_SUCCESS;
  int64_t partition_index = OB_INVALID_INDEX;
  partition = NULL;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_id));
  } else if (partition_num_ < 0 || dropped_partition_num_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition num is invalid", K(ret), K(part_id), K_(partition_num), K_(dropped_partition_num));
  } else if (OB_ISNULL(partition_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition array", K(ret));
  } else if (OB_FAIL(get_partition_index_by_id(part_id, check_dropped_partition, partition_index))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("partition not exist", KR(ret), K(part_id), K(check_dropped_partition));
    } else {
      LOG_WARN("failed to get partition index by id", K(ret), K(part_id));
    }
  } else if (partition_index < partition_num_) {
    partition = partition_array_[partition_index];
  } else if (check_dropped_partition) {
    partition_index = partition_index - partition_num_;
    if (partition_index >= dropped_partition_num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid idx", K(ret), K(part_id), K(partition_index), K_(partition_num), K_(dropped_partition_num));
    } else {
      partition = dropped_partition_array_[partition_index];
    }
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
    const int64_t part_id, const bool check_dropped_partition, int64_t& partition_index) const
{
  int ret = OB_SUCCESS;
  partition_index = OB_INVALID_INDEX;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_id));
  } else if (OB_ISNULL(partition_array_) || partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition array", K(ret), K(part_id), K_(partition_num));
  } else {
    for (int64_t i = 0; i < partition_num_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partiion", K(ret), K(i), K(part_id));
      } else if (part_id == partition_array_[i]->get_part_id()) {
        partition_index = i;
        break;
      }
    }
  }
  // Dealing with delayed deletion of objects
  if (OB_SUCC(ret) && check_dropped_partition && OB_INVALID_INDEX == partition_index) {
    for (int64_t i = 0; i < dropped_partition_num_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(dropped_partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partiion", K(ret), K(i), K(part_id));
      } else if (part_id == dropped_partition_array_[i]->get_part_id()) {
        partition_index = i;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_INDEX != partition_index) {
      partition_index = partition_num_ + partition_index;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_INDEX == partition_index) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to find partition", K(ret), K(part_id));
    }
  }
  return ret;
}

int ObPartitionSchema::get_subpart_info(
    const int64_t part_id, ObSubPartition**& subpart_array, int64_t& subpart_num, int64_t& subpartition_num) const
{
  int ret = OB_SUCCESS;
  subpart_array = get_def_subpart_array();
  subpart_num = get_def_sub_part_num();
  subpartition_num = get_def_subpartition_num();
  if (!is_sub_part_template()) {
    const ObPartition* part = NULL;
    bool check_dropped_partition = false;
    if (OB_FAIL(get_partition_by_part_id(part_id, check_dropped_partition, part))) {
      LOG_WARN("fail to get partition", K(ret), K(part_id));
    } else if (OB_ISNULL(part)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get partition", K(ret), K(part_id));
    } else {
      subpart_array = part->get_subpart_array();
      subpart_num = part->get_sub_part_num();
      subpartition_num = part->get_subpartition_num();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (subpart_num != subpartition_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart_num not match", K(ret), K(part_id), K(subpart_num), K(subpartition_num));
  } else if (OB_ISNULL(subpart_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart_array is null", K(ret), K(part_id));
  }
  return ret;
}

int ObPartitionSchema::get_hash_subpart_id_by_idx(
    const int64_t part_id, const int64_t subpart_idx, int64_t& subpart_id) const
{
  int ret = OB_SUCCESS;
  subpart_id = OB_INVALID_ID;
  if (!is_hash_like_subpart()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only hash subpart can be here", K(ret), K(*this));
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    ObSubPartition** subpart_array = NULL;
    int64_t subpart_num = 0;
    int64_t subpartition_num = 0;
    if (OB_FAIL(get_subpart_info(part_id, subpart_array, subpart_num, subpartition_num))) {
      LOG_WARN("fail to get subpart info", K(ret), K(part_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < subpart_num; i++) {
      if (OB_ISNULL(subpart_array[i])) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("subpartition is null", K(ret), K(i));
      } else if (subpart_array[i]->get_sub_part_idx() == subpart_idx) {
        subpart_id = subpart_array[i]->get_sub_part_id();
        break;
      }
    }
  } else {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid table partition level", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret) && OB_INVALID_ID == subpart_id) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("subpart_id is invalid", K(ret), K(part_id), K(subpart_id), K(*this));
  }
  return ret;
}

int ObPartitionSchema::get_part_id_by_idx(const int64_t part_idx, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  part_id = OB_INVALID_ID;
  if (!is_hash_like_part()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this funciton is only for hash partition", K(ret), K(*this));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    if (part_idx != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(part_idx), K(part_level_));
    } else {
      part_id = 0;
    }
  } else if (PARTITION_LEVEL_ONE == part_level_ || PARTITION_LEVEL_TWO == part_level_) {
    if (part_idx < 0 || part_idx >= get_part_option().get_part_num()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(part_idx), K(partition_num_), K(partition_array_));
    } else if (OB_ISNULL(partition_array_)) {
      part_id = part_idx;
    } else {
      for (int64_t i = 0; i < partition_num_ && OB_SUCC(ret); i++) {
        if (OB_ISNULL(partition_array_[i])) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("get invalid partiton array", K(ret), K(i), K(partition_num_));
        } else if (partition_array_[i]->get_part_idx() == part_idx) {
          part_id = partition_array_[i]->get_part_id();
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_INVALID_ID == part_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_id is invalid", K(ret), K(part_idx), K(*this));
      }
    }
  } else {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid table partition level", K(ret), K(part_level_));
  }
  return ret;
}

int ObPartitionSchema::get_hash_part_idx_by_id(const int64_t part_id, int64_t& part_idx) const
{
  int ret = OB_SUCCESS;
  part_idx = OB_INVALID_ID;
  if (!is_hash_like_part()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this funciton is only for hash partition", K(ret), KPC(this));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    if (part_id != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(part_id), K(part_level_));
    } else {
      part_idx = 0;
    }
  } else if (PARTITION_LEVEL_ONE == part_level_ || PARTITION_LEVEL_TWO == part_level_) {
    if (OB_ISNULL(partition_array_)) {
      part_idx = part_id;
    } else {
      for (int64_t i = 0; i < partition_num_ && OB_SUCC(ret); i++) {
        if (OB_ISNULL(partition_array_[i])) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("get invalid partiton array", K(ret), K(i), K(partition_num_));
        } else if (partition_array_[i]->get_part_id() == part_id) {
          part_idx = partition_array_[i]->get_part_idx();
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_INVALID_ID == part_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_idx is invalid", K(ret), K(part_id), KPC(this));
      }
    }
  } else {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid table partition level", K(ret), K(part_level_));
  }
  return ret;
}

int ObPartitionSchema::get_hash_subpart_idx_by_id(
    const int64_t part_id, const int64_t subpart_id, int64_t& subpart_idx) const
{
  int ret = OB_SUCCESS;
  subpart_idx = OB_INVALID_ID;
  if (!is_hash_like_subpart()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only hash subpart can be here", K(ret), K(subpart_id), KPC(this));
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    ObSubPartition** subpart_array = NULL;
    int64_t subpart_num = 0;
    int64_t subpartition_num = 0;
    if (OB_FAIL(get_subpart_info(part_id, subpart_array, subpart_num, subpartition_num))) {
      LOG_WARN("fail to get subpart info", K(ret), K(part_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < subpart_num; i++) {
      if (OB_ISNULL(subpart_array[i])) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("get invalid partiton array", K(ret), K(i), K(subpart_num));
      } else if (subpart_array[i]->get_sub_part_id() == subpart_id) {
        subpart_idx = subpart_array[i]->get_sub_part_idx();
        break;
      }
    }
  } else {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid table partition level", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret) && OB_INVALID_ID == subpart_idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("subpart_idx is invalid", K(ret), K(part_id), K(subpart_id), KPC(this));
  }
  return ret;
}

// __all_virtual_proxy_schema use this interface
// The hash like scenario needs to obtain the partition_id corresponding to the partition_idx
int ObPartitionSchema::convert_partition_idx_to_id(const int64_t partition_idx, int64_t& partition_id) const
{
  INIT_SUCC(ret);
  partition_id = partition_idx;  // set to equal by default

  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    // non-partitioned table
  } else if (PARTITION_LEVEL_ONE == get_part_level()) {
    if (is_hash_like_part()) {
      if (OB_FAIL(get_part_id_by_idx(partition_idx, partition_id))) {
        LOG_WARN("fail to get partition id by idx", K(partition_idx), K(partition_id), K(ret));
      }
    }
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    if (is_hash_like_part() || is_hash_like_subpart()) {
      int64_t part_idx = extract_part_idx(partition_idx);
      int64_t part_id = part_idx;

      int64_t subpart_idx = extract_subpart_idx(partition_idx);
      int64_t subpart_id = subpart_idx;

      // level one is hash part
      if (is_hash_like_part()) {
        if (OB_FAIL(get_part_id_by_idx(part_idx, part_id))) {
          LOG_WARN("fail to get partition id by idx", K(part_idx), K(ret));
        }
      }

      // level two is hash part
      if (OB_SUCC(ret) && is_hash_like_subpart()) {
        if (OB_FAIL(get_hash_subpart_id_by_idx(part_id, subpart_idx, subpart_id))) {
          LOG_WARN("fail to get subpartition id by idx", K(subpart_idx), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        partition_id = generate_phy_part_id(part_id, subpart_id, PARTITION_LEVEL_TWO);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", "part_level", get_part_level(), K(ret));
  }

  return ret;
}

// For client send packet usage, the partition_id of the hash partition needs to be converted to partition_idx
int ObPartitionSchema::convert_partition_id_to_idx(const int64_t partition_id, int64_t& partition_idx) const
{
  INIT_SUCC(ret);
  partition_idx = partition_id;  // set to equal by default

  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    // non-partitioned table
  } else if (PARTITION_LEVEL_ONE == get_part_level()) {
    if (is_hash_like_part()) {
      if (OB_FAIL(get_hash_part_idx_by_id(partition_id, partition_idx))) {
        LOG_WARN("fail to get partition idx by id", K(partition_idx), K(partition_id), K(ret));
      }
    }
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    if (is_hash_like_part() || is_hash_like_subpart()) {
      int64_t part_id = extract_part_idx(partition_id);
      int64_t part_idx = part_id;

      int64_t subpart_id = extract_subpart_idx(partition_id);
      int64_t subpart_idx = subpart_id;

      // level one is hash part
      if (is_hash_like_part()) {
        if (OB_FAIL(get_hash_part_idx_by_id(part_id, part_idx))) {
          LOG_WARN("fail to get partition idx by id", K(part_id), K(ret));
        }
      }

      // level two is hash part
      if (OB_SUCC(ret) && is_hash_like_subpart()) {
        if (OB_FAIL(get_hash_subpart_idx_by_id(part_id, subpart_id, subpart_idx))) {
          LOG_WARN("fail to get subpartition idx by id", K(part_id), K(subpart_id), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        partition_idx = generate_phy_part_id(part_idx, subpart_idx, PARTITION_LEVEL_TWO);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", "part_level", get_part_level(), K(ret));
  }

  return ret;
}

// In the process of splitting, given the partition_key after splitting, get the source partition_key
// TODO: Currently does not support the splitting of the table/tablegroup of the secondary partition
// 1. When dst_part_key is a split partition, source_part_key returns the split source partition
// 2. When dst_part_key is not a partition after splitting, source_part_key returns itself
int ObPartitionSchema::get_split_source_partition_key(
    const ObPartitionKey& dst_part_key, ObPartitionKey& source_part_key) const
{
  int ret = OB_SUCCESS;
  if (!dst_part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dst partition key is invalid", K(ret), K(dst_part_key));
  } else if (dst_part_key.get_table_id() != get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id not matched", K(ret), K(dst_part_key), "table_id", get_table_id());
  } else if (!is_in_splitting()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("table partition_status should be spliting", K(ret), K_(partition_status));
  } else {
    // check if part_level and partition_id matched
    int64_t dst_partition_id = dst_part_key.get_partition_id();
    if (PARTITION_LEVEL_MAX == part_level_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_level is invalid", K(ret));
    } else if (PARTITION_LEVEL_ZERO == part_level_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table/tablegroup should has partitions", K(ret));
    } else if (PARTITION_LEVEL_ONE == part_level_ && is_twopart(dst_partition_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part_level and part_key not matched", K(ret), K(dst_part_key), K_(part_level));
    } else if (PARTITION_LEVEL_TWO == part_level_ && !is_twopart(dst_partition_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part_level and part_key not matched", K(ret), K(dst_part_key), K_(part_level));
    }
    // generate partition_key
    if (OB_SUCC(ret)) {
      if (PARTITION_LEVEL_ONE == part_level_) {
        // For a split table, its partition_cnt_with_partition_table must not be -1 value
        // When calculating the source partition_key of the primary partition table, partition_cnt can take this value
        // directly
        int64_t part_id = dst_part_key.get_partition_id();
        int64_t index = OB_INVALID_INDEX;
        bool check_dropped_partition = false;
        if (OB_FAIL(get_partition_index_loop(part_id, check_dropped_partition, index))) {
          LOG_WARN("fail to get partition by part_id", K(ret), K(part_id));
        } else if (OB_ISNULL(partition_array_[index])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", K(ret), K(index));
        } else if (0 == partition_array_[index]->get_source_part_ids().count()) {
          // When dst_part_key is not a partition after splitting, source_part_key returns itself
          if (OB_FAIL(source_part_key.init(
                  dst_part_key.get_table_id(), dst_part_key.get_partition_id(), dst_part_key.get_partition_cnt()))) {
            LOG_WARN("source_part_key init fail", K(ret), K(source_part_key), K(dst_part_key));
          }
        } else if (1 != partition_array_[index]->get_source_part_ids().count()) {
          ret = OB_INVALID_PARTITION_ID;
          LOG_WARN("spliting part's source part ids count should be 1", K(ret), "partition", partition_array_[index]);
        } else if (OB_FAIL(source_part_key.init(
                       get_table_id(), partition_array_[index]->get_source_part_ids().at(0), get_partition_cnt()))) {
          LOG_WARN("init part key fail", K(ret));
        }
      } else if (PARTITION_LEVEL_TWO == part_level_) {
        // TODO: Currently does not support the split of the secondary partition
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("split schema with subpartitions is not supported now", K(ret), K(dst_part_key));
      }
    }
  }
  return ret;
}

int ObPartitionSchema::clear_dropped_partition()
{
  int ret = OB_SUCCESS;

  reset_dropped_partition();
  if (PARTITION_LEVEL_TWO == get_part_level() && !is_sub_part_template()) {
    const int64_t part_num = get_partition_num();
    ObPartition** part_array = get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(i), K(ret));
        } else {
          part_array[i]->reset_dropped_subpartition();
        }
      }
    }
  }
  return ret;
}

int ObPartitionSchema::get_part_id_by_name(const common::ObString partition_name, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  part_id = OB_INVALID_ID;
  if (PARTITION_LEVEL_ZERO == part_level_ || OB_ISNULL(partition_array_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_name), K(part_level_), K(partition_array_));
  } else if (PARTITION_LEVEL_ONE == part_level_ || PARTITION_LEVEL_TWO == part_level_) {
    for (int64_t i = 0; i < partition_num_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("get invalid partiton array", K(ret), K(i), K(partition_num_));
      } else if (partition_array_[i]->get_part_name() == partition_name) {
        part_id = partition_array_[i]->get_part_id();
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == part_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id is invalid", K(ret), K(part_id), KPC(this));
    }
  } else {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid table partition level", K(ret), K(part_level_));
  }
  return ret;
}

int ObPartitionSchema::get_partition_index_by_id(
    const int64_t part_id, const bool check_dropped_partition, int64_t& partition_index) const
{
  int ret = OB_SUCCESS;
  partition_index = OB_INVALID_INDEX;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_id is invalid", KR(ret), K(part_id));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    // Partition table, return 0 directly
    partition_index = 0;
  } else if (OB_ISNULL(partition_array_)) {
    // If partition_array is empty, the offset is part_id
    partition_index = part_id;
  } else if (0 == partition_schema_version_) {
    // Undivided partition
    if (is_hash_like_part()) {
      // Hash-like partitions do not support partition management operations other than splits,
      // and there will be no partitions with delayed deletion.
      // Logical recovery + hash table split, this logic will have problems
      partition_index = part_id;
    } else if (is_range_part() || is_list_part()) {
      if (OB_FAIL(get_partition_index_loop(part_id, check_dropped_partition, partition_index))) {
        LOG_WARN("failed to get partition index loop", KR(ret), K(part_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part type", KR(ret), K(part_option_));
    }
  } else {
    // The divided partition can only be traversed
    if (OB_FAIL(get_partition_index_loop(part_id, check_dropped_partition, partition_index))) {
      LOG_WARN("failed to get partition index loop", KR(ret), K(part_id));
    }
  }
  return ret;
}

int ObPartitionSchema::get_partition_index_binary_search(
    const int64_t part_id, const bool check_dropped_partition, int64_t& partition_idx) const
{
  int ret = OB_SUCCESS;
  partition_idx = OB_INVALID_INDEX;
  if (OB_INVALID_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_id is invalid", K(ret), K(part_id));
  } else if (!is_range_part()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not range partition", K(ret), K_(part_option));
  } else if (OB_ISNULL(partition_array_) || partition_num_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition array is null", K(ret), K(part_id), K_(partition_num));
  } else if (0 != partition_schema_version_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has split", K(ret), K_(partition_schema_version));
  } else {
    // For range partitions that have not been split, you can use binary search
    int64_t low = 0;
    int64_t high = partition_num_ - 1;
    int64_t mid = 0;
    while (low <= high && OB_SUCC(ret)) {
      mid = (low + high) / 2;
      if (OB_ISNULL(partition_array_[mid])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(mid));
      } else if (part_id == partition_array_[mid]->get_part_id()) {
        partition_idx = mid;
        break;
      } else if (part_id > partition_array_[mid]->get_part_id()) {
        // The value to be queried is greater than the median
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    // Dealing with delayed deletion of objects
    if (OB_SUCC(ret) && check_dropped_partition && OB_INVALID_INDEX == partition_idx) {
      int64_t low = 0;
      int64_t high = dropped_partition_num_ - 1;
      int64_t mid = 0;
      while (low <= high && OB_SUCC(ret)) {
        mid = (low + high) / 2;
        if (OB_ISNULL(dropped_partition_array_[mid])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", K(ret), K(mid));
        } else if (part_id == dropped_partition_array_[mid]->get_part_id()) {
          partition_idx = mid;
          break;
        } else if (part_id > dropped_partition_array_[mid]->get_part_id()) {
          // The value to be queried is greater than the median
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }
      if (OB_SUCC(ret) && OB_INVALID_INDEX != partition_idx) {
        partition_idx = partition_num_ + partition_idx;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX == partition_idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("partition id not exist", K(ret), K(part_id));
  }
  return ret;
}
/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTablegroupSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObTablegroupSchema::ObTablegroupSchema() : ObPartitionSchema(), locality_info_(this), primary_zone_info_(this)
{
  reset();
}

ObTablegroupSchema::ObTablegroupSchema(common::ObIAllocator* allocator)
    : ObPartitionSchema(allocator), locality_info_(this), primary_zone_info_(this)
{
  reset();
}

ObTablegroupSchema::ObTablegroupSchema(const ObTablegroupSchema& src_schema)
    : ObPartitionSchema(), locality_info_(this), primary_zone_info_(this)
{
  reset();
  *this = src_schema;
}

ObTablegroupSchema::~ObTablegroupSchema()
{}

int ObTablegroupSchema::assign(const ObTablegroupSchema& src_schema)
{
  int ret = OB_SUCCESS;
  reset();
  *this = src_schema;
  ret = error_ret_;
  return ret;
}

ObTablegroupSchema& ObTablegroupSchema::operator=(const ObTablegroupSchema& src_schema)
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
      binding_ = src_schema.binding_;
      is_mock_global_index_invalid_ = src_schema.is_mock_global_index_invalid_;

      if (OB_FAIL(deep_copy_str(src_schema.tablegroup_name_, tablegroup_name_))) {
        LOG_WARN("Fail to deep copy tablegroup name, ", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
        LOG_WARN("Fail to deep copy comment, ", K(ret));
      } else if (OB_FAIL(primary_zone_info_.assign(src_schema.primary_zone_info_))) {
        LOG_WARN("fail to assign primary zone", K(ret));
      } else if (OB_FAIL(locality_info_.assign(src_schema.locality_info_))) {
        LOG_WARN("fail to assign locality", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.previous_locality_, previous_locality_))) {
        LOG_WARN("Fail to deep copy previous_locality, ", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.split_partition_name_, split_partition_name_))) {
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
  convert_size += locality_info_.get_convert_size() - sizeof(ObLocality);
  convert_size += primary_zone_info_.get_convert_size() - sizeof(ObPrimaryZone);
  convert_size += part_option_.get_convert_size() - sizeof(part_option_);
  convert_size += sub_part_option_.get_convert_size() - sizeof(sub_part_option_);
  convert_size += previous_locality_.length() + 1;
  convert_size += split_partition_name_.length() + 1;
  // all part info size
  for (int64_t i = 0; i < partition_num_ && NULL != partition_array_[i]; ++i) {
    convert_size += partition_array_[i]->get_convert_size();
  }
  convert_size += partition_num_ * sizeof(ObPartition*);
  // all sub part info size
  for (int64_t i = 0; i < def_subpartition_num_ && NULL != def_subpartition_array_[i]; ++i) {
    convert_size += def_subpartition_array_[i]->get_convert_size();
  }
  convert_size += def_subpartition_num_ * sizeof(ObSubPartition*);
  // all dropped part info size
  for (int64_t i = 0; i < dropped_partition_num_ && NULL != dropped_partition_array_[i]; ++i) {
    convert_size += dropped_partition_array_[i]->get_convert_size();
  }
  convert_size += partition_num_ * sizeof(ObPartition*);
  return convert_size;
}

bool ObTablegroupSchema::is_valid() const
{
  return ObSchema::is_valid() && OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != tablegroup_id_ && schema_version_ > 0;
}

void ObTablegroupSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  tablegroup_name_.reset();
  comment_.reset();
  locality_info_.reset();
  primary_zone_info_.reset();
  part_func_expr_num_ = OB_INVALID_INDEX;
  sub_part_func_expr_num_ = OB_INVALID_INDEX;
  previous_locality_.reset();
  split_partition_name_.reset();
  split_high_bound_val_.reset();
  split_list_row_values_.reset();
  binding_ = false;
  is_mock_global_index_invalid_ = false;
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
      previous_locality_,
      split_partition_name_);

  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_FAIL(locality_info_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize locality_info", K(ret));
  } else if (OB_FAIL(primary_zone_info_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize primary zone", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(serialize_partitions(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize partitions", K(ret));
      } else {
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_TWO == part_level_) {
      if (OB_FAIL(serialize_def_subpartitions(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize subpartitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, binding_, drop_schema_version_);
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(serialize_dropped_partitions(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize dropped partitions", K(ret));
      } else {
      }
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, is_sub_part_template_);
  LOG_DEBUG("serialize tablegroup schema", K(*this));

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
      previous_locality_,
      split_partition_name_,
      drop_schema_version_);

  len += locality_info_.get_serialize_size();
  len += primary_zone_info_.get_serialize_size();

  if (PARTITION_LEVEL_ONE <= part_level_) {
    // get partitions size
    len += serialization::encoded_length_vi64(partition_num_);
    for (int64_t i = 0; i < partition_num_; i++) {
      if (NULL != partition_array_[i]) {
        len += partition_array_[i]->get_serialize_size();
      }
    }
    // get dropped partitions size
    len += serialization::encoded_length_vi64(dropped_partition_num_);
    for (int64_t i = 0; i < dropped_partition_num_; i++) {
      if (NULL != dropped_partition_array_[i]) {
        len += dropped_partition_array_[i]->get_serialize_size();
      }
    }
  }
  if (PARTITION_LEVEL_TWO == part_level_) {
    // get subpartitions size
    len += serialization::encoded_length_vi64(def_subpartition_num_);
    for (int64_t i = 0; i < def_subpartition_num_; i++) {
      if (NULL != def_subpartition_array_[i]) {
        len += def_subpartition_array_[i]->get_serialize_size();
      }
    }
  }

  LST_DO_CODE(OB_UNIS_ADD_LEN, binding_, is_sub_part_template_);

  return len;
}

OB_DEF_DESERIALIZE(ObTablegroupSchema)
{
  int ret = OB_SUCCESS;
  ObString tablegroup_name;
  ObString comment;
  ObString previous_locality;
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
      previous_locality,
      split_partition_name,
      locality_info_,
      primary_zone_info_);

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
    // skip
  } else if (OB_FAIL(deep_copy_str(tablegroup_name, tablegroup_name_))) {
    LOG_WARN("Fail to deep copy tablegroup name, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(comment, comment_))) {
    LOG_WARN("Fail to deep copy comment, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(previous_locality, previous_locality_))) {
    LOG_WARN("Fail to deep copy previous_locality, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(split_partition_name, split_partition_name_))) {
    LOG_WARN("fail to deep copy split partition name", K(ret), K(split_partition_name));
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, binding_, drop_schema_version_);
  }
  // dropped partition array
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(deserialize_partitions(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize dropped partitions", K(ret));
      }
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, is_sub_part_template_);
  LOG_WARN("serialize tablegroup schema", K(*this));
  return ret;
}

int64_t ObTablegroupSchema::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
      K_(tablegroup_id),
      K_(schema_version),
      K_(tablegroup_name),
      K_(comment),
      K_(locality_info),
      K_(primary_zone_info),
      K_(part_level),
      K_(part_option),
      K_(sub_part_option),
      K_(part_func_expr_num),
      K_(sub_part_func_expr_num),
      K_(part_num),
      K_(def_subpart_num),
      K_(partition_num),
      K_(def_subpartition_num),
      K_(error_ret),
      K_(partition_status),
      K_(drop_schema_version),
      "partition_array",
      ObArrayWrap<ObPartition*>(partition_array_, partition_num_),
      "def_subpartition_array",
      ObArrayWrap<ObSubPartition*>(def_subpartition_array_, def_subpartition_num_),
      "dropped_partition_array",
      ObArrayWrap<ObPartition*>(dropped_partition_array_, dropped_partition_num_),
      K_(previous_locality),
      K_(binding),
      K_(is_mock_global_index_invalid),
      K_(split_high_bound_val),
      K_(split_list_row_values),
      K_(is_sub_part_template));
  J_OBJ_END();
  return pos;
}

int ObTablegroupSchema::get_primary_zone_score(const common::ObZone& zone, int64_t& zone_score) const
{
  int ret = OB_SUCCESS;
  zone_score = INT64_MAX;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_UNLIKELY(get_primary_zone().empty()) || OB_UNLIKELY(get_primary_zone_array().count() <= 0)) {
    // zone score set to INT64_MAX above
  } else {
    for (int64_t i = 0; i < get_primary_zone_array().count(); ++i) {
      if (zone == get_primary_zone_array().at(i).zone_) {
        zone_score = get_primary_zone_array().at(i).score_;
        break;
      } else {
      }  // go on find
    }
  }
  return ret;
}

int ObTablegroupSchema::get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard& schema_guard,
    const rootserver::ObRandomZoneSelector& random_selector,
    const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs, common::ObZone& first_primary_zone) const
{
  int ret = OB_SUCCESS;
  first_primary_zone.reset();
  if (get_primary_zone().empty()) {
    const share::schema::ObTenantSchema *tenant_schema = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_first_primary_zone(random_selector, replica_addrs, first_primary_zone))) {
      LOG_WARN("fail to get first primary zone", K(ret));
    }
  } else {
    common::ObArray<rootserver::ObPrimaryZoneReplicaCandidate> candidate_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_addrs.count(); ++i) {
      const rootserver::ObReplicaAddr& replica_addr = replica_addrs.at(i);
      rootserver::ObPrimaryZoneReplicaCandidate candidate;
      candidate.is_full_replica_ = (common::REPLICA_TYPE_FULL == replica_addr.replica_type_);
      candidate.zone_ = replica_addr.zone_;
      if (OB_FAIL(get_primary_zone_score(candidate.zone_, candidate.zone_score_))) {
        LOG_WARN("fail to get primary zone score", K(ret), "zone", candidate.zone_);
      } else if (OB_FAIL(random_selector.get_zone_score(candidate.zone_, candidate.random_score_))) {
        LOG_WARN("fail to get zone random score", K(ret), "zone", candidate.zone_);
      } else if (OB_FAIL(candidate_array.push_back(candidate))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
    if (OB_SUCC(ret) && candidate_array.count() > 0) {
      std::sort(candidate_array.begin(), candidate_array.end(), rootserver::ObPrimaryZoneReplicaCmp());
      const rootserver::ObPrimaryZoneReplicaCandidate& candidate = candidate_array.at(0);
      if (candidate.is_full_replica_) {
        first_primary_zone = candidate.zone_;
      } else {
        first_primary_zone.reset();
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::set_primary_zone_array(const common::ObIArray<ObZoneScore>& primary_zone_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(primary_zone_info_.set_primary_zone_array(primary_zone_array))) {
    LOG_WARN("fail to set primary zone array", K(ret));
  }
  return ret;
}
int ObTablegroupSchema::get_zone_list(
    share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  if (!locality_info_.locality_str_.empty()) {
    ObString locality = locality_info_.locality_str_;
    if (OB_FAIL(ObLocalityUtil::parse_zone_list_from_locality_str(locality, zone_list))) {
      LOG_WARN("fail to parse zone list", K(ret), K(locality_info_));
    }
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    zone_list.reset();
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObTablegroupSchema::check_is_duplicated(share::schema::ObSchemaGetterGuard& guard, bool& is_duplicated) const
{
  int ret = OB_SUCCESS;
  UNUSED(guard);
  is_duplicated = false;
  return ret;
}

void ObTablegroupSchema::reset_locality_options()
{
  locality_info_.reset();
}

int ObTablegroupSchema::get_zone_replica_attr_array(ZoneLocalityIArray& locality) const
{
  int ret = OB_SUCCESS;
  locality.reset();
  for (int64_t i = 0; i < locality_info_.zone_replica_attr_array_.count() && OB_SUCC(ret); ++i) {
    const SchemaZoneReplicaAttrSet& schema_set = locality_info_.zone_replica_attr_array_.at(i);
    ObZoneReplicaAttrSet zone_replica_attr_set;
    for (int64_t j = 0; OB_SUCC(ret) && j < schema_set.zone_set_.count(); ++j) {
      if (OB_FAIL(zone_replica_attr_set.zone_set_.push_back(schema_set.zone_set_.at(j)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_replica_attr_set.replica_attr_set_.assign(schema_set.replica_attr_set_))) {
      LOG_WARN("fail to set replica attr set", K(ret));
    } else {
      zone_replica_attr_set.zone_ = schema_set.zone_;
      if (OB_FAIL(locality.push_back(zone_replica_attr_set))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

// TODO: These functions can be extracted; as an implementation of locality/primary_zone
int ObTablegroupSchema::get_zone_replica_attr_array_inherit(
    ObSchemaGetterGuard& schema_guard, ZoneLocalityIArray& locality) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_locality = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_;
  locality.reset();
  if (!use_tenant_locality && !locality_info_.locality_str_.empty()) {
    const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, simple_tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
    } else if (simple_tenant->is_restore()) {
      bool has_not_f_replica = false;
      if (OB_FAIL(check_has_own_not_f_replica(has_not_f_replica))) {
        LOG_WARN("failed to check has not f replica", KR(ret));
      } else if (has_not_f_replica) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN(
            "has not full replica while tenant is restore, try latter", KR(ret), K(locality_info_), KPC(simple_tenant));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!locality_info_.locality_str_.empty() && !use_tenant_locality) {
    if (OB_FAIL(get_zone_replica_attr_array(locality))) {
      LOG_WARN("fail to get zone replica attr array", K(ret));
    }
  } else {
    const ObTenantSchema* tenant_schema = NULL;
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
    share::schema::ObSchemaGetterGuard& guard, const common::ObString*& locality_str) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_locality = OB_SYS_TENANT_ID != tenant_id_ && GCTX.is_standby_cluster();
  locality_str = &get_locality_str();

  if (OB_FAIL(ret)) {
  } else if (use_tenant_locality || nullptr == locality_str || locality_str->empty()) {
    const ObSimpleTenantSchema* tenant_schema = nullptr;
    if (OB_FAIL(guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
    } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
    } else {
      if (tenant_schema->is_restore()) {
        bool has_not_f_replica = false;
        if (OB_FAIL(check_has_own_not_f_replica(has_not_f_replica))) {
          LOG_WARN("failed to check has not f replica", KR(ret));
        } else if (has_not_f_replica) {
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("has not full replica while tenant is restore, try latter",
              KR(ret),
              K(locality_info_),
              KPC(tenant_schema));
        }
      }
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

int ObTablegroupSchema::set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet>& src)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(locality_info_.set_zone_replica_attr_array(src))) {
    LOG_WARN("fail to set locality", K(ret));
  }
  return ret;
}

int ObTablegroupSchema::set_zone_replica_attr_array(const common::ObIArray<SchemaZoneReplicaAttrSet>& src)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(locality_info_.set_zone_replica_attr_array(src))) {
    LOG_WARN("fail to set locality", K(ret));
  }
  return ret;
}

// Fill in other attributes of ObPrimaryZone/ObLocality according to the primary_zone_str or
// locality_str after the specification;
int ObTablegroupSchema::fill_additional_options()
{
  int ret = OB_SUCCESS;
  if (ObPrimaryZoneUtil::no_need_to_check_primary_zone(primary_zone_info_.primary_zone_str_)) {
    // nothing todo
  } else {
    ObIArray<ObZoneRegion>* zone_region_list = NULL;
    ObPrimaryZoneUtil primary_zone_util(primary_zone_info_.primary_zone_str_, zone_region_list);
    if (OB_FAIL(primary_zone_util.init())) {
      LOG_WARN("fail to init primary zone util", K(ret));
    } else if (OB_FAIL(primary_zone_util.check_and_parse_primary_zone())) {
      LOG_WARN("fail to check and parse primary zone", K(ret));
    } else if (OB_FAIL(set_primary_zone_array(primary_zone_util.get_zone_array()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set primary zone array", K(ret));
    }
  }
  LOG_DEBUG("fill primary info", K(ret), K(primary_zone_info_));

  if (OB_FAIL(ret) || locality_info_.locality_str_.empty()) {
    // nothing todo
  } else {
    ObArray<ObZone> zone_list;
    rootserver::ObLocalityDistribution locality_dist;
    ObArray<share::ObZoneReplicaAttrSet> zone_replica_attr_array;
    if (OB_FAIL(
            rootserver::ObLocalityUtil::parse_zone_list_from_locality_str(locality_info_.locality_str_, zone_list))) {
      LOG_WARN("fail to parse zone list", K(ret));
    } else if (OB_FAIL(locality_dist.init())) {
      SHARE_SCHEMA_LOG(WARN, "fail to init locality dist", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(locality_info_.locality_str_, zone_list))) {
      SHARE_SCHEMA_LOG(WARN, "fail to parse locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_attr_array))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get zone region replica num array", K(ret));
    } else if (OB_FAIL(set_zone_replica_attr_array(zone_replica_attr_array))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set zone replica num array", K(ret));
    } else {
    }  // no more to do, good
  }
  LOG_DEBUG("fill locality info", K(ret), K(locality_info_));
  return ret;
}

int ObTablegroupSchema::get_primary_zone_inherit(ObSchemaGetterGuard& schema_guard, ObPrimaryZone& primary_zone) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_primary_zone = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_;
  primary_zone.reset();

  if (OB_FAIL(ret)) {
  } else if (!get_primary_zone().empty() && !use_tenant_primary_zone) {
    if (OB_FAIL(primary_zone.set_primary_zone_array(get_primary_zone_array()))) {
      LOG_WARN("fail to set primary zone array", K(ret));
    } else if (OB_FAIL(primary_zone.set_primary_zone(get_primary_zone()))) {
      LOG_WARN("fail to set primary zone", K(ret));
    }
  } else {
    const ObTenantSchema* tenant_schema = NULL;
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

int ObTablegroupSchema::check_in_locality_modification(
    ObSchemaGetterGuard& schema_guard, bool& in_locality_modification) const
{
  int ret = OB_SUCCESS;
  if (get_previous_locality_str().empty()) {
    const ObTenantSchema* tenant_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
    } else {
      in_locality_modification = !tenant_schema->get_previous_locality_str().empty();
    }
  } else {
    in_locality_modification = !get_previous_locality_str().empty();
  }
  return ret;
}

int ObTablegroupSchema::check_is_readonly_at_all(ObSchemaGetterGuard& schema_guard, const common::ObZone& zone,
    const common::ObRegion& region, bool& readonly_at_all) const
{
  UNUSED(region);
  int ret = OB_SUCCESS;
  readonly_at_all = false;
  if (!locality_info_.locality_str_.empty()) {
    FOREACH_CNT_X(locality, locality_info_.zone_replica_attr_array_, OB_SUCC(ret))
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else if (zone == locality->zone_) {
        readonly_at_all = (OB_ALL_SERVER_CNT == locality->get_readonly_replica_num());
        break;
      }
    }
  } else {
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
  }
  return ret;
}

int ObTablegroupSchema::get_full_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  if (!locality_info_.locality_str_.empty()) {
    FOREACH_CNT_X(locality, locality_info_.zone_replica_attr_array_, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_full_replica_num();
      }
    }
  } else {
    common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
    if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else {
      for (int64_t i = 0; i < zone_locality.count(); ++i) {
        num += zone_locality.at(i).get_full_replica_num();
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::check_has_own_not_f_replica(bool &has_not_f_replica) const
{
  int ret = OB_SUCCESS;
  has_not_f_replica = false;
  if (locality_info_.locality_str_.empty()) {
    has_not_f_replica = false;
  } else {
    FOREACH_CNT_X(locality, locality_info_.zone_replica_attr_array_, OB_SUCC(ret) && !has_not_f_replica)
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else if (locality->get_specific_replica_num() != locality->get_full_replica_num() ||
                 OB_ALL_SERVER_CNT == locality->get_readonly_replica_num()) {
        has_not_f_replica = true;
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::get_paxos_replica_num(share::schema::ObSchemaGetterGuard& schema_guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  if (!locality_info_.locality_str_.empty()) {
    FOREACH_CNT_X(locality, locality_info_.zone_replica_attr_array_, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_paxos_replica_num();
      }
    }
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tablegroup_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(tablegroup_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(zone_locality))) {
      LOG_WARN("fail to get zone replica attr array", K(ret));
    } else {
      FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret)
      {
        if (OB_ISNULL(locality)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get valid locality set", K(ret), KP(locality));
        } else {
          num += locality->get_paxos_replica_num();
        }
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::get_all_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  if (!locality_info_.locality_str_.empty()) {
    FOREACH_CNT_X(locality, locality_info_.zone_replica_attr_array_, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_specific_replica_num();
      }
    }
  } else {
    common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
    if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else {
      for (int64_t i = 0; i < zone_locality.count(); ++i) {
        num += zone_locality.at(i).get_specific_replica_num();
      }
    }
  }
  return ret;
}

int ObTablegroupSchema::calc_part_func_expr_num(int64_t& part_func_expr_num) const
{
  int ret = OB_SUCCESS;
  part_func_expr_num = part_func_expr_num_;
  return ret;
}

int ObTablegroupSchema::calc_subpart_func_expr_num(int64_t& subpart_func_expr_num) const
{
  int ret = OB_SUCCESS;
  subpart_func_expr_num = sub_part_func_expr_num_;
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
      part_space_(0),
      is_columns_(false),
      max_used_part_id_(-1),
      partition_cnt_within_partition_table_(-1),
      auto_part_(false),
      auto_part_size_(-1)
{}

ObPartitionOption::ObPartitionOption(ObIAllocator* allocator)
    : ObSchema(allocator),
      part_func_type_(PARTITION_FUNC_TYPE_HASH),
      part_func_expr_(),
      part_num_(1),
      part_space_(0),
      is_columns_(false),
      max_used_part_id_(-1),
      partition_cnt_within_partition_table_(-1),
      auto_part_(false),
      auto_part_size_(-1)
{}

ObPartitionOption::~ObPartitionOption()
{}

ObPartitionOption::ObPartitionOption(const ObPartitionOption& expr)
    : ObSchema(),
      part_func_type_(PARTITION_FUNC_TYPE_HASH),
      part_num_(1),
      max_used_part_id_(-1),
      partition_cnt_within_partition_table_(-1),
      auto_part_(false),
      auto_part_size_(-1)
{
  *this = expr;
}

ObPartitionOption& ObPartitionOption::operator=(const ObPartitionOption& expr)
{
  if (this != &expr) {
    reset();
    int ret = OB_SUCCESS;

    part_num_ = expr.part_num_;
    part_func_type_ = expr.part_func_type_;
    max_used_part_id_ = expr.max_used_part_id_;
    partition_cnt_within_partition_table_ = expr.partition_cnt_within_partition_table_;
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

int64_t ObPartitionOption::assign(const ObPartitionOption& src_part)
{
  int ret = OB_SUCCESS;
  *this = src_part;
  ret = get_assign_ret();
  return ret;
}

bool ObPartitionOption::operator==(const ObPartitionOption& expr) const
{
  return (part_func_type_ == expr.part_func_type_) && (part_num_ == expr.part_num_) &&
         (part_func_expr_ == expr.part_func_expr_) && (max_used_part_id_ == expr.max_used_part_id_) &&
         (partition_cnt_within_partition_table_ == expr.partition_cnt_within_partition_table_) &&
         (auto_part_ == expr.auto_part_) && (auto_part_size_ == expr.auto_part_size_);
}

bool ObPartitionOption::operator!=(const ObPartitionOption& expr) const
{
  return !(*this == expr);
}

void ObPartitionOption::reset()
{
  part_func_type_ = PARTITION_FUNC_TYPE_HASH;
  part_num_ = 1;
  part_space_ = 0;
  is_columns_ = false;
  max_used_part_id_ = -1;
  partition_cnt_within_partition_table_ = -1;
  reset_string(part_func_expr_);
  reset_string(interval_start_);
  reset_string(part_interval_);
  auto_part_ = false;
  auto_part_size_ = -1;
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
  LST_DO_CODE(OB_UNIS_ENCODE,
      part_func_type_,
      part_func_expr_,
      part_num_,
      max_used_part_id_,
      partition_cnt_within_partition_table_,
      auto_part_,
      auto_part_size_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPartitionOption)
{
  int ret = OB_SUCCESS;
  ObString part_func_expr;

  LST_DO_CODE(OB_UNIS_DECODE,
      part_func_type_,
      part_func_expr,
      part_num_,
      max_used_part_id_,
      partition_cnt_within_partition_table_,
      auto_part_,
      auto_part_size_);

  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(part_func_expr, part_func_expr_))) {
    LOG_WARN("Fail to deep copy part_func_expr, ", K(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPartitionOption)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      part_func_type_,
      part_func_expr_,
      part_num_,
      max_used_part_id_,
      partition_cnt_within_partition_table_,
      auto_part_,
      auto_part_size_);
  return len;
}

ObBasePartition::ObBasePartition()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      part_id_(common::OB_INVALID_INDEX),
      schema_version_(OB_INVALID_VERSION),
      name_(),
      high_bound_val_(),
      status_(PARTITION_STATUS_ACTIVE),
      spare1_(common::OB_INVALID_INDEX),
      spare2_(common::OB_INVALID_INDEX),
      spare3_(common::OB_INVALID_INDEX),
      projector_(NULL),
      projector_size_(0),
      source_part_ids_(),
      part_idx_(OB_INVALID_INDEX),
      is_empty_partition_name_(false),
      tablespace_id_(common::OB_INVALID_ID)
{}

ObBasePartition::ObBasePartition(common::ObIAllocator* allocator)
    : ObSchema(allocator),
      tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      part_id_(common::OB_INVALID_INDEX),
      schema_version_(OB_INVALID_VERSION),
      name_(),
      high_bound_val_(),
      schema_allocator_(*allocator),
      list_row_values_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(schema_allocator_)),
      status_(PARTITION_STATUS_ACTIVE),
      spare1_(common::OB_INVALID_INDEX),
      spare2_(common::OB_INVALID_INDEX),
      spare3_(common::OB_INVALID_INDEX),
      projector_(NULL),
      projector_size_(0),
      source_part_ids_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(schema_allocator_)),
      part_idx_(OB_INVALID_INDEX),
      is_empty_partition_name_(false),
      tablespace_id_(common::OB_INVALID_ID)
{}

void ObBasePartition::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  part_id_ = -1;
  schema_version_ = OB_INVALID_VERSION;
  spare1_ = 0;
  spare2_ = 0;
  spare3_ = 0;
  status_ = PARTITION_STATUS_ACTIVE;
  projector_ = NULL;
  projector_size_ = 0;
  source_part_ids_.reset();
  part_idx_ = OB_INVALID_INDEX;
  high_bound_val_.reset();
  list_row_values_.reset();
  part_idx_ = OB_INVALID_INDEX;
  is_empty_partition_name_ = false;
  tablespace_id_ = OB_INVALID_ID;
  ObSchema::reset();
}

int ObBasePartition::assign(const ObBasePartition& src_part)
{
  int ret = OB_SUCCESS;
  if (this != &src_part) {
    reset();
    tenant_id_ = src_part.tenant_id_;
    table_id_ = src_part.table_id_;
    part_id_ = src_part.part_id_;
    schema_version_ = src_part.schema_version_;
    spare1_ = src_part.spare1_;
    spare2_ = src_part.spare2_;
    spare3_ = src_part.spare3_;
    status_ = src_part.status_;
    part_idx_ = src_part.part_idx_;
    is_empty_partition_name_ = src_part.is_empty_partition_name_;
    tablespace_id_ = src_part.tablespace_id_;
    if (OB_FAIL(deep_copy_str(src_part.name_, name_))) {
      LOG_WARN("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(set_high_bound_val(src_part.high_bound_val_))) {
      LOG_WARN("Fail to deep copy high_bound_val_", K(ret));
    } else if (OB_FAIL(source_part_ids_.assign(src_part.source_part_ids_))) {
      LOG_WARN("fail to assign", K(ret));
    } else {
      int64_t count = src_part.list_row_values_.count();
      if (OB_FAIL(list_row_values_.reserve(count))) {
        LOG_WARN("fail to reserve se array", K(ret), K(count));
      } else {
        ObNewRow tmp_row;
        ObIAllocator* allocator = get_allocator();
        for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          const ObNewRow& row = src_part.list_row_values_.at(i);
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

bool ObBasePartition::list_part_func_layout(const ObBasePartition* lhs, const ObBasePartition* rhs)
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
    LOG_ERROR("lhs or rhs should not be null", KP(lhs), KP(rhs));
  } else if (lhs->get_list_row_values().count() < rhs->get_list_row_values().count()) {
    bool_ret = false;
  } else if (lhs->get_list_row_values().count() > rhs->get_list_row_values().count()) {
    bool_ret = true;
  } else {
    bool finish = false;
    for (int64_t i = 0; !finish && i < lhs->get_list_row_values().count(); ++i) {
      const common::ObNewRow& l_row = lhs->get_list_row_values().at(i);
      const common::ObNewRow& r_row = rhs->get_list_row_values().at(i);
      int cmp = 0;
      if (OB_SUCCESS != ObRowUtil::compare_row(l_row, r_row, cmp)) {
        LOG_ERROR("l or r is invalid");
        finish = true;
      } else if (cmp < 0) {
        bool_ret = true;
        finish = true;
      } else if (cmp > 0) {
        bool_ret = false;
        finish = true;
      } else {
      }  // go on next
    }
    if (!finish) {
      bool_ret = (lhs->get_part_id() < rhs->get_part_id());
    }
  }
  return bool_ret;
}

int ObBasePartition::set_high_bound_val(const ObRowkey& high_bound_val)
{
  int ret = OB_SUCCESS;
  ObIAllocator* allocator = get_allocator();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (OB_FAIL(high_bound_val.deep_copy(high_bound_val_, *allocator))) {
    LOG_WARN("Fail to deep copy high_bound_val_", K(ret));
  } else {
  }
  return ret;
}

int ObBasePartition::set_list_vector_values_with_hex_str(const common::ObString& list_vector_vals_hex)
{
  int ret = OB_SUCCESS;
  char serialize_buf[OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  ObIAllocator* allocator = get_allocator();
  int64_t pos = 0;
  const int64_t hex_length = list_vector_vals_hex.length();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if ((hex_length % 2) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hex str length should be even", K(ret));
  } else if (OB_UNLIKELY(hex_length != str_to_hex(list_vector_vals_hex.ptr(),
                                           static_cast<int32_t>(hex_length),
                                           serialize_buf,
                                           OB_MAX_B_HIGH_BOUND_VAL_LENGTH))) {
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
    const int obj_capacity = 1024;
    ObObj obj_array[obj_capacity];
    row.assign(obj_array, obj_capacity);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      row.count_ = obj_capacity;  // reset count for sparse row
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
      std::sort(list_row_values_.begin(), list_row_values_.end(), part_list_vector_op);
      if (OB_FAIL(part_list_vector_op.get_ret())) {
        LOG_WARN("fail to sort list row values", K(ret));
      }
    }
  }

  return ret;
}

int ObBasePartition::set_high_bound_val_with_hex_str(const common::ObString& high_bound_val_hex)
{
  int ret = OB_SUCCESS;
  char serialize_buf[OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  ObIAllocator* allocator = get_allocator();
  int64_t pos = 0;
  const int64_t hex_length = high_bound_val_hex.length();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if ((hex_length % 2) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Hex str length should be even", K(ret));
  } else if (OB_UNLIKELY(hex_length != str_to_hex(high_bound_val_hex.ptr(),
                                           static_cast<int32_t>(hex_length),
                                           serialize_buf,
                                           OB_MAX_B_HIGH_BOUND_VAL_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("Failed to get hex_str buf", K(ret));
  } else if (OB_FAIL(high_bound_val_.deserialize(*allocator, serialize_buf, hex_length, pos))) {
    LOG_WARN("Failed to deserialize high bound val", K(ret));
  } else {
  }  // do nothing
  return ret;
}

OB_DEF_SERIALIZE(ObBasePartition)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      table_id_,
      part_id_,
      schema_version_,
      name_,
      high_bound_val_,
      status_,
      spare1_,
      spare2_,
      spare3_);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, list_row_values_.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < list_row_values_.count(); i++) {
    if (OB_FAIL(list_row_values_.at(i).serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to encode row", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, source_part_ids_, part_idx_, is_empty_partition_name_, tablespace_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasePartition)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey high_bound_val;
  high_bound_val.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
  ObObj sub_interval_start;
  ObObj sub_part_interval;
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      table_id_,
      part_id_,
      schema_version_,
      name,
      high_bound_val,
      status_,
      spare1_,
      spare2_,
      spare3_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(name, name_))) {
    LOG_WARN("Fail to deep copy name, ", K(ret), K_(name));
  } else if (OB_FAIL(set_high_bound_val(high_bound_val))) {
    LOG_WARN("Fail to deep copy high_bound_val", K(ret), K(high_bound_val));
  } else {
  }  // do nothing

  if (OB_SUCC(ret) && pos < data_len) {
    int64_t size = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &size))) {
      LOG_WARN("fail to decode vi64", K(ret));
    }
    const int obj_capacity = 1024;
    ObNewRow row;
    ObNewRow tmp_row;
    ObObj obj_array[obj_capacity];
    row.assign(obj_array, obj_capacity);

    ObIAllocator* allocator = get_allocator();

    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      row.count_ = obj_capacity;  // reset count for sparse row
      if (OB_FAIL(row.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize row", K(ret));
      } else if (OB_FAIL(ob_write_row(*allocator, row, tmp_row))) {
        LOG_WARN("fail to write row", K(ret));
      } else if (OB_FAIL(list_row_values_.push_back(tmp_row))) {
        LOG_WARN("fail to push back tmp_row", K(ret));
      }
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, source_part_ids_, part_idx_, is_empty_partition_name_, tablespace_id_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasePartition)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      table_id_,
      part_id_,
      schema_version_,
      name_,
      high_bound_val_,
      status_,
      spare1_,
      spare2_,
      spare3_,
      source_part_ids_,
      part_idx_,
      is_empty_partition_name_,
      tablespace_id_);
  len += serialization::encoded_length_vi64(list_row_values_.count());
  for (int64_t i = 0; i < list_row_values_.count(); i++) {
    len += list_row_values_.at(i).get_serialize_size();
  }
  return len;
}

int64_t ObBasePartition::get_deep_copy_size() const
{
  int64_t deep_copy_size = name_.length() + 1;
  deep_copy_size += high_bound_val_.get_deep_copy_size();
  int64_t list_row_size = list_row_values_.get_data_size();
  for (int64_t i = 0; i < list_row_values_.count(); i++) {
    list_row_size += list_row_values_.at(i).get_deep_copy_size();
  }
  deep_copy_size += list_row_size * 2 - 1;
  deep_copy_size += source_part_ids_.get_data_size();
  return deep_copy_size;
}
int ObBasePartition::set_source_part_id(ObString& partition_str)
{
  int ret = OB_SUCCESS;
  ObString trimed_string = partition_str.trim();
  ObSEArray<ObString, 2> partition_ids;
  source_part_ids_.reset();
  char buf[MAX_VALUE_LENGTH];
  if (OB_FAIL(split_on(trimed_string, ',', partition_ids))) {
    LOG_WARN("fail to split on string", K(ret), K(trimed_string));
  } else {
    for (int64_t i = 0; i < partition_ids.count() && OB_SUCC(ret); i++) {
      int64_t pos = partition_ids.at(i).to_string(buf, MAX_VALUE_LENGTH);
      if (pos == 0 || pos == MAX_VALUE_LENGTH - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to to_string", K(ret), K(partition_ids), K(i));
      } else {
        int64_t part_id = atoll(buf);
        if (part_id < 0 && part_id != -1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid partition id", K(ret), K(trimed_string), K(i), K(partition_ids));
        } else if (-1 == part_id) {
          // -1 means the split is completed, and there is no need to flush this value to the memory
        } else if (OB_FAIL(source_part_ids_.push_back(part_id))) {
          LOG_WARN("fail to push back", K(ret), K(part_id));
        }
      }
    }
  }
  return ret;
}

int ObBasePartition::get_source_part_ids_str(char* str, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str), K(buf_len));
  }
  for (int64_t i = 0; i < source_part_ids_.count() && OB_SUCC(ret); i++) {
    if (i == 0) {
      if (OB_FAIL(databuff_printf(str, buf_len, "%ld", source_part_ids_.at(i)))) {
        LOG_WARN("fail to print", K(ret), K(source_part_ids_));
      }
    } else {
      if (OB_FAIL(databuff_printf(str, buf_len, ", %ld", source_part_ids_.at(i)))) {
        LOG_WARN("fail to print", K(ret), K(source_part_ids_));
      }
    }
  }
  return ret;
}

bool ObBasePartition::less_than(const ObBasePartition* lhs, const ObBasePartition* rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR("lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
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
      LOG_ERROR("lhs or rhs is invalid", K(lrow), K(rrow), K(lhs), K(rhs));
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

bool ObBasePartition::range_like_func_less_than(const ObBasePartition* lhs, const ObBasePartition* rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR("lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
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
      LOG_ERROR("lhs or rhs is invalid", K(lrow), K(rrow), K(lhs), K(rhs));
    } else if (0 == cmp) {
      bret = lhs->get_part_id() < rhs->get_part_id();
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

bool ObBasePartition::hash_like_func_less_than(const ObBasePartition* lhs, const ObBasePartition* rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR("lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else {
    int cmp = static_cast<int32_t>(lhs->get_part_idx() - rhs->get_part_idx());
    if (0 == cmp) {
      bret = lhs->get_part_id() < rhs->get_part_id();
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

////////////////////////////////////
ObPartition::ObPartition() : ObBasePartition()
{
  reset();
}

ObPartition::ObPartition(ObIAllocator* allocator) : ObBasePartition(allocator)
{
  reset();
}

void ObPartition::reset()
{
  sub_part_num_ = 0;
  sub_part_space_ = 0;
  sub_interval_start_.reset();
  sub_part_interval_.reset();
  mapping_pg_part_id_ = OB_INVALID_ID;
  drop_schema_version_ = OB_INVALID_VERSION;
  max_used_sub_part_id_ = OB_INVALID_ID;
  subpartition_num_ = 0;
  subpartition_array_capacity_ = 0;
  subpartition_array_ = NULL;
  sorted_part_id_subpartition_array_ = NULL;
  dropped_subpartition_num_ = 0;
  dropped_subpartition_array_capacity_ = 0;
  dropped_subpartition_array_ = NULL;
  ObBasePartition::reset();
}

int ObPartition::clone(common::ObIAllocator& allocator, ObPartition*& dst) const
{
  int ret = OB_SUCCESS;
  dst = NULL;

  ObPartition* new_part = OB_NEWx(ObPartition, (&allocator));
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

int ObPartition::assign(const ObPartition& src_part)
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
      sub_part_space_ = src_part.sub_part_space_;
      mapping_pg_part_id_ = src_part.mapping_pg_part_id_;
      drop_schema_version_ = src_part.drop_schema_version_;
      max_used_sub_part_id_ = src_part.max_used_sub_part_id_;

      // subpartition_array_
      if (OB_SUCC(ret) && src_part.subpartition_num_ > 0) {
        int64_t subpartition_num = src_part.subpartition_num_;
        // reserved
        subpartition_array_ = static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * subpartition_num));
        if (OB_ISNULL(subpartition_array_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for subpartition_array_", K(ret), K(subpartition_num));
        } else if (OB_ISNULL(src_part.subpartition_array_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartition_array_ is null", K(ret));
        } else {
          subpartition_array_capacity_ = subpartition_num;
        }
        // push_back
        ObSubPartition* subpartition = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; i++) {
          subpartition = src_part.subpartition_array_[i];
          if (OB_ISNULL(subpartition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the partition is null", K(ret));
          } else if (OB_FAIL(add_partition(*subpartition))) {
            LOG_WARN("Fail to add partition", K(ret), K(i));
          }
        }
      }

      // dropped_subpartition_array_
      if (OB_SUCC(ret) && src_part.dropped_subpartition_num_ > 0) {
        int64_t dropped_subpartition_num = src_part.dropped_subpartition_num_;
        // reserved
        dropped_subpartition_array_ =
            static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * dropped_subpartition_num));
        if (OB_ISNULL(dropped_subpartition_array_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for subpartition_array_", K(ret), K(dropped_subpartition_num));
        } else if (OB_ISNULL(src_part.dropped_subpartition_array_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartition_array_ is null", K(ret));
        } else {
          dropped_subpartition_array_capacity_ = dropped_subpartition_num;
        }
        // push_back
        ObSubPartition* subpartition = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < dropped_subpartition_num; i++) {
          subpartition = src_part.dropped_subpartition_array_[i];
          if (OB_ISNULL(subpartition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the partition is null", K(ret));
          } else if (OB_FAIL(add_partition(*subpartition))) {
            LOG_WARN("Fail to add partition", K(ret), K(i));
          }
        }
      }

    }  // do nothing
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPartition)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObBasePartition));
  LST_DO_CODE(OB_UNIS_ENCODE,
      sub_part_num_,
      sub_part_space_,
      sub_interval_start_,
      sub_part_interval_,
      mapping_pg_part_id_,
      drop_schema_version_,
      max_used_sub_part_id_);
  // Non-templated secondary partition
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, subpartition_num_))) {
      LOG_WARN("Fail to encode subpartition count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num_; i++) {
      if (OB_ISNULL(subpartition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition_array_ element is null", K(ret));
      } else if (OB_FAIL(subpartition_array_[i]->serialize(buf, buf_len, pos))) {
        LOG_WARN("Fail to serialize partition", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, dropped_subpartition_num_))) {
      LOG_WARN("Fail to encode subpartition count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_subpartition_num_; i++) {
      if (OB_ISNULL(dropped_subpartition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition_array_ element is null", K(ret));
      } else if (OB_FAIL(dropped_subpartition_array_[i]->serialize(buf, buf_len, pos))) {
        LOG_WARN("Fail to serialize partition", K(ret));
      }
    }
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
      sub_part_space_,
      sub_interval_start,
      sub_part_interval,
      mapping_pg_part_id_,
      drop_schema_version_,
      max_used_sub_part_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_obj(sub_interval_start, sub_interval_start_))) {
    LOG_WARN("Fail to deep copy sub_interval_start", K(ret), K(sub_interval_start));
  } else if (OB_FAIL(deep_copy_obj(sub_part_interval, sub_part_interval_))) {
    LOG_WARN("Fail to deep copy sub_part_interval", K(ret), K(sub_part_interval));
  } else {
  }  // do nothing

  // subpartition_array
  if (OB_SUCC(ret) && data_len > 0) {
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
  // dropped_subpartition_array
  if (OB_SUCC(ret) && data_len > 0) {
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
      sub_part_space_,
      sub_interval_start_,
      sub_part_interval_,
      mapping_pg_part_id_,
      drop_schema_version_,
      max_used_sub_part_id_);

  len += serialization::encoded_length_vi64(subpartition_num_);
  for (int64_t i = 0; i < subpartition_num_; i++) {
    if (OB_NOT_NULL(subpartition_array_[i])) {
      len += subpartition_array_[i]->get_serialize_size();
    }
  }

  len += serialization::encoded_length_vi64(dropped_subpartition_num_);
  for (int64_t i = 0; i < dropped_subpartition_num_; i++) {
    if (OB_NOT_NULL(dropped_subpartition_array_[i])) {
      len += dropped_subpartition_array_[i]->get_serialize_size();
    }
  }
  return len;
}

int64_t ObPartition::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += ObBasePartition::get_deep_copy_size();
  convert_size += sub_interval_start_.get_deep_copy_size();
  convert_size += sub_part_interval_.get_deep_copy_size();
  // all sub part info size
  for (int64_t i = 0; i < subpartition_num_ && OB_NOT_NULL(subpartition_array_[i]); ++i) {
    convert_size += subpartition_array_[i]->get_convert_size();
  }
  convert_size += subpartition_num_ * sizeof(ObSubPartition*);

  for (int64_t i = 0; i < dropped_subpartition_num_ && OB_NOT_NULL(dropped_subpartition_array_[i]); ++i) {
    convert_size += dropped_subpartition_array_[i]->get_convert_size();
  }
  convert_size += dropped_subpartition_num_ * sizeof(ObSubPartition*);
  return convert_size;
}

int ObPartition::add_partition(const ObSubPartition& subpartition)
{
  int ret = OB_SUCCESS;
  ObSubPartition* local = OB_NEWx(ObSubPartition, (get_allocator()), (get_allocator()));
  if (NULL == local) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(local->assign(subpartition))) {
    LOG_WARN("failed to assign partition", K(ret));
  } else if (subpartition.is_dropped_partition()) {
    if (OB_FAIL(inner_add_partition(
            *local, dropped_subpartition_array_, dropped_subpartition_array_capacity_, dropped_subpartition_num_))) {
      LOG_WARN("add subpartition failed", K(subpartition), K(ret));
    }
  } else {
    if (OB_FAIL(inner_add_partition(*local, subpartition_array_, subpartition_array_capacity_, subpartition_num_))) {
      LOG_WARN("add subpartition failed", K(subpartition), K(ret));
    }
  }
  return ret;
}

int ObPartition::inner_add_partition(
    const ObSubPartition& part, ObSubPartition**& part_array, int64_t& part_array_capacity, int64_t& part_num)
{
  int ret = common::OB_SUCCESS;
  if (0 == part_array_capacity) {
    if (NULL == (part_array = static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * DEFAULT_ARRAY_CAPACITY)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition arrary");
    } else {
      part_array_capacity = DEFAULT_ARRAY_CAPACITY;
    }
  } else if (part_num >= part_array_capacity) {
    int64_t new_size = 2 * part_array_capacity;
    ObSubPartition** tmp = NULL;
    if (NULL == (tmp = static_cast<ObSubPartition**>(alloc((sizeof(ObSubPartition*) * new_size))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for partition array", K(new_size));
    } else {
      MEMCPY(tmp, part_array, sizeof(ObSubPartition*) * part_num);
      MEMSET(tmp + part_num, 0, sizeof(ObSubPartition*) * (new_size - part_num));
      free(part_array);
      part_array = tmp;
      part_array_capacity = new_size;
    }
  }
  if (OB_SUCC(ret)) {
    part_array[part_num] = const_cast<ObSubPartition*>(&part);
    ++part_num;
  }
  return ret;
}

int ObPartition::generate_mapping_pg_subpartition_array()
{
  int ret = OB_SUCCESS;
  const int64_t sub_part_num = subpartition_num_;
  const int64_t total_sub_part_num = dropped_subpartition_num_ + sub_part_num;
  if (OB_SUCC(ret) && total_sub_part_num > 0) {
    sorted_part_id_subpartition_array_ =
        static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * total_sub_part_num));
    if (OB_UNLIKELY(nullptr == sorted_part_id_subpartition_array_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (sub_part_num > 0 && OB_ISNULL(get_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpart array is null", K(ret), K(sub_part_num));
    } else if (dropped_subpartition_num_ > 0 && OB_ISNULL(get_dropped_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dropped subpart array is null", K(ret), K_(dropped_subpartition_num));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < total_sub_part_num; ++i) {
        if (i >= sub_part_num) {
          sorted_part_id_subpartition_array_[i] = get_dropped_subpart_array()[i - sub_part_num];
        } else {
          sorted_part_id_subpartition_array_[i] = get_subpart_array()[i];
        }
        if (OB_ISNULL(sorted_part_id_subpartition_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart is null", K(ret), K(sub_part_num), K(total_sub_part_num));
        }
      }
      if (OB_SUCC(ret)) {
        SubPartIdPartitionArrayCmp sub_part_id_array_cmp;
        std::sort(sorted_part_id_subpartition_array_,
            sorted_part_id_subpartition_array_ + total_sub_part_num,
            sub_part_id_array_cmp);
        if (OB_FAIL(sub_part_id_array_cmp.get_ret())) {
          LOG_WARN("fail to def sort part id array", K(ret));
        }
      }
    }
  }
  return ret;
}

ObSubPartition::ObSubPartition() : ObBasePartition()
{
  reset();
}

ObSubPartition::ObSubPartition(ObIAllocator* allocator) : ObBasePartition(allocator)
{
  reset();
}

void ObSubPartition::reset()
{
  subpart_id_ = OB_INVALID_INDEX;
  subpart_idx_ = OB_INVALID_INDEX;
  mapping_pg_sub_part_id_ = -1;
  drop_schema_version_ = OB_INVALID_VERSION;
  ObBasePartition::reset();
}

int ObSubPartition::clone(common::ObIAllocator& allocator, ObSubPartition*& dst) const
{
  int ret = OB_SUCCESS;
  dst = NULL;

  ObSubPartition* new_part = OB_NEWx(ObSubPartition, (&allocator));
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

bool ObSubPartition::less_than(const ObSubPartition* lhs, const ObSubPartition* rhs)
{
  bool b_ret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR("lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
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
      LOG_ERROR("lhs or rhs is invalid");
    } else {
      b_ret = (cmp < 0);
    }
  } else {
  }  // do nothing
  return b_ret;
}

int ObSubPartition::assign(const ObSubPartition& src_part)
{
  int ret = OB_SUCCESS;
  if (this != &src_part) {
    reset();
    if (OB_FAIL(ObBasePartition::assign(src_part))) {  // Must assign base class first
      LOG_WARN("Failed to assign ObBasePartition", K(ret));
    } else {
      subpart_id_ = src_part.subpart_id_;
      subpart_idx_ = src_part.subpart_idx_;
      mapping_pg_sub_part_id_ = src_part.mapping_pg_sub_part_id_;
      drop_schema_version_ = src_part.drop_schema_version_;
    }
  }
  return ret;
}

bool ObSubPartition::key_match(const ObSubPartition& other) const
{
  return get_table_id() == other.get_table_id() && get_part_id() == other.get_part_id() &&
         get_sub_part_id() == other.get_sub_part_id();
}

OB_DEF_SERIALIZE(ObSubPartition)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObBasePartition));
  LST_DO_CODE(OB_UNIS_ENCODE, subpart_id_, subpart_idx_, mapping_pg_sub_part_id_, drop_schema_version_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSubPartition)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObBasePartition));
  LST_DO_CODE(OB_UNIS_DECODE, subpart_id_, subpart_idx_, mapping_pg_sub_part_id_, drop_schema_version_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSubPartition)
{
  int64_t len = ObBasePartition::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, subpart_id_, subpart_idx_, mapping_pg_sub_part_id_, drop_schema_version_);
  return len;
}

int64_t ObSubPartition::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += ObBasePartition::get_deep_copy_size();
  return convert_size;
}

bool ObSubPartition::hash_like_func_less_than(const ObSubPartition* lhs, const ObSubPartition* rhs)
{
  bool bret = false;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    LOG_ERROR("lhs or rhs should not be NULL", KPC(lhs), KPC(rhs));
  } else {
    int cmp = static_cast<int32_t>(lhs->get_part_idx() - rhs->get_part_idx());
    if (0 == cmp) {
      cmp = static_cast<int32_t>(lhs->get_sub_part_idx() - rhs->get_sub_part_idx());
      bret = (cmp < 0);
    } else {
      bret = (cmp < 0);
    }
  }
  return bret;
}

int ObPartitionUtils::calc_hash_part_idx(const uint64_t val, const int64_t part_num, int64_t& partition_idx)
{
  int ret = OB_SUCCESS;
  int64_t N = 0;
  int64_t powN = 0;
  const static int64_t max_part_num_log2 = 64;
  if (share::is_oracle_mode()) {
    // It will not be a negative number, so use forced conversion instead of floor
    N = static_cast<int64_t>(std::log(part_num) / std::log(2));
    if (N >= max_part_num_log2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is too big", K(N), K(part_num), K(val));
    } else {
      powN = (1ULL << N);
      partition_idx = val & (powN - 1);  // pow(2, N));
      if (partition_idx + powN < part_num && (val & powN) == powN) {
        partition_idx += powN;
      }
    }
    LOG_DEBUG("get hash part idx", K(lbt()), K(ret), K(val), K(part_num), K(N), K(powN), K(partition_idx));
  } else {
    partition_idx = val % part_num;
    LOG_DEBUG("get hash part idx", K(lbt()), K(ret), K(val), K(part_num), K(partition_idx));
  }
  return ret;
}

int ObPartitionUtils::get_hash_part_ids(const ObTableSchema& table_schema, const common::ObNewRange& range,
    const int64_t part_num, common::ObIArray<int64_t>& part_ids)
{
  int ret = OB_SUCCESS;
  const ObRowkey& start_key = range.get_start_key();
  if (!range.is_single_rowkey() || 1 != start_key.get_obj_cnt() || ObIntType != start_key.get_obj_ptr()[0].get_type()) {
    if (OB_FAIL(get_all_part(table_schema, part_num, part_ids))) {
      LOG_WARN("Failed to get all part", K(ret));
    }
  } else {
    int64_t val = 0;
    int64_t part_idx = -1;
    if (OB_FAIL(start_key.get_obj_ptr()[0].get_int(val))) {
      LOG_WARN("Failed to get int val", K(ret));
    } else if (val < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Val should not be less than 0", K(ret));
    } else {
      int64_t part_id = -1;
      if (OB_FAIL(calc_hash_part_idx(val, part_num, part_idx))) {
        LOG_WARN("failed to calc hash part idx", K(ret));
      } else if (OB_FAIL(table_schema.get_part_id_by_idx(part_idx, part_id))) {
        LOG_WARN("fail to get partition id by idx", K(ret), K(part_idx));
      } else if (OB_FAIL(part_ids.push_back(part_id))) {
        LOG_WARN("Failed to add part ids", K(ret));
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObPartitionUtils::get_hash_part_ids(const ObTableSchema& table_schema, const ObNewRow& row, const int64_t part_num,
    ObIArray<int64_t>& part_ids, bool get_part_index /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret));
  } else if (1 != row.get_count() || (!row.get_cell(0).is_int() && !row.get_cell(0).is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(row), K(ret));
  } else {
    // Hash the null value to partition 0
    int64_t val = row.get_cell(0).is_int() ? row.get_cell(0).get_int() : 0;
    int64_t part_idx = val % part_num;
    int64_t part_id = -1;
    if (val < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Val should not be less than 0", K(ret));
    } else if (OB_FAIL(calc_hash_part_idx(val, part_num, part_idx))) {
      LOG_WARN("failed to calc hash part idx", K(ret));
    } else if (get_part_index) {
      if (OB_FAIL(part_ids.push_back(part_idx))) {
        LOG_WARN("Failed to add part idxs", K(ret));
      }
    } else if (OB_FAIL(table_schema.get_part_id_by_idx(part_idx, part_id))) {
      LOG_WARN("fail to get part_id by idx", K(ret), K(part_idx));
    } else if (OB_FAIL(part_ids.push_back(part_id))) {
      LOG_WARN("Failed to add part_id", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObPartitionUtils::get_hash_subpart_ids(const ObTableSchema& table_schema, const int64_t part_id,
    const common::ObNewRange& range, const int64_t subpart_num, common::ObIArray<int64_t>& subpart_ids)
{
  int ret = OB_SUCCESS;
  const ObRowkey& start_key = range.get_start_key();
  if (PARTITION_LEVEL_TWO != table_schema.get_part_level() || !table_schema.is_hash_like_subpart()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else if ((part_id != ObSubPartition::TEMPLATE_PART_ID && table_schema.is_sub_part_template()) ||
             (part_id < 0 && !table_schema.is_sub_part_template())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_id", K(ret), K(part_id), K(table_schema));
  } else if (!range.is_single_rowkey() || 1 != start_key.get_obj_cnt() ||
             ObIntType != start_key.get_obj_ptr()[0].get_type()) {
    if (OB_FAIL(table_schema.get_subpart_ids(part_id, subpart_ids))) {
      LOG_WARN("Failed to get all part", K(ret));
    }
  } else {
    int64_t val = 0;
    int64_t subpart_idx = -1;
    int64_t subpart_id = -1;
    if (OB_FAIL(start_key.get_obj_ptr()[0].get_int(val))) {
      LOG_WARN("Failed to get int val", K(ret));
    } else if (val < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Val should not be less than 0", K(ret));
    } else if (OB_FAIL(calc_hash_part_idx(val, subpart_num, subpart_idx))) {
      LOG_WARN("failed to calc hash part idx", K(ret));
    } else if (OB_FAIL(table_schema.get_hash_subpart_id_by_idx(part_id, subpart_idx, subpart_id))) {
      LOG_WARN("fail to get partition id by idx", K(ret), K(part_id), K(subpart_idx));
    } else if (OB_FAIL(subpart_ids.push_back(subpart_id))) {
      LOG_WARN("Failed to add part idxs", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObPartitionUtils::get_hash_subpart_ids(const ObTableSchema& table_schema, const int64_t part_id,
    const ObNewRow& row, const int64_t subpart_num, ObIArray<int64_t>& subpart_ids,
    bool get_subpart_index /* = false */)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_TWO != table_schema.get_part_level() || !table_schema.is_hash_like_subpart()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else if ((part_id != ObSubPartition::TEMPLATE_PART_ID && table_schema.is_sub_part_template()) ||
             (part_id < 0 && !table_schema.is_sub_part_template())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_id", K(ret), K(part_id), K(table_schema));
  } else if (OB_UNLIKELY(row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret));
  } else if (1 != row.get_count() || (!row.get_cell(0).is_int() && !row.get_cell(0).is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(row), K(ret));
  } else {
    // Hash the null value to partition 0
    int64_t val = row.get_cell(0).is_int() ? row.get_cell(0).get_int() : 0;
    int64_t subpart_idx = -1;
    int64_t subpart_id = -1;
    if (val < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Val should not be less than 0", K(ret));
    } else if (OB_FAIL(calc_hash_part_idx(val, subpart_num, subpart_idx))) {
      LOG_WARN("failed to calc hash part idx", K(ret));
    } else if (get_subpart_index) {
      if (OB_FAIL(subpart_ids.push_back(subpart_idx))) {
        LOG_WARN("Failed to add part idxs", K(ret));
      }
    } else if (OB_FAIL(table_schema.get_hash_subpart_id_by_idx(part_id, subpart_idx, subpart_id))) {
      LOG_WARN("fail to get partitoin id by idx", K(ret), K(part_id), K(subpart_idx));
    } else if (OB_FAIL(subpart_ids.push_back(subpart_id))) {
      LOG_WARN("Failed to add part idxs", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObPartitionUtils::get_all_part(
    const ObTableSchema& table_schema, int64_t part_num, common::ObIArray<int64_t>& part_ids)
{
  int ret = OB_SUCCESS;
  part_ids.reset();
  if (part_num != table_schema.get_part_option().get_part_num()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_num), K(table_schema));
  } else if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    int64_t partition_id = 0;
    if (OB_FAIL(part_ids.push_back(partition_id))) {
      LOG_WARN("fail to push back", K(ret), K(partition_id));
    }
  } else {
    ObPartition** part = table_schema.get_part_array();
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid part array", K(part));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_part_option().get_part_num(); i++) {
        if (OB_ISNULL(part[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid partition info", K(ret), K(i));
        } else if (OB_FAIL(part_ids.push_back(part[i]->get_part_id()))) {
          LOG_WARN("fail to push back", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObPartitionUtils::get_list_part_ids(const common::ObNewRange& range, const bool reverse,
    ObPartition* const* partition_array, const int64_t partition_num, common::ObIArray<int64_t>& part_ids)
{
  int ret = OB_SUCCESS;
  if (!range.is_single_rowkey()) {
    if (reverse) {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        if (OB_FAIL(part_ids.push_back(partition_array[partition_num - i - 1]->part_id_))) {
          LOG_WARN("fail to push back part idx", K(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        if (OB_FAIL(part_ids.push_back(partition_array[i]->part_id_))) {
          LOG_WARN("fail to push back part idx", K(ret));
        }
      }
    }
  } else {
    ObNewRow row;
    row.cells_ = const_cast<ObObj*>(range.start_key_.get_obj_ptr());
    row.count_ = range.start_key_.get_obj_cnt();
    if (OB_FAIL(get_list_part_ids(row, partition_array, partition_num, part_ids))) {
      LOG_WARN("get list part idxs failed", K(ret));
    }
  }
  return ret;
}

int ObPartitionUtils::get_list_part_ids(const int64_t part_id, const common::ObNewRange& range, const bool reverse,
    ObSubPartition* const* partition_array, const int64_t partition_num, common::ObIArray<int64_t>& part_ids)
{
  int ret = OB_SUCCESS;
  UNUSED(part_id);
  if (!range.is_single_rowkey()) {
    if (reverse) {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        if (OB_FAIL(part_ids.push_back(partition_array[partition_num - i - 1]->get_sub_part_id()))) {
          LOG_WARN("fail to push back part idx", K(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        if (OB_FAIL(part_ids.push_back(partition_array[i]->get_sub_part_id()))) {
          LOG_WARN("fail to push back part idx", K(ret));
        }
      }
    }
  } else {
    ObNewRow row;
    row.cells_ = const_cast<ObObj*>(range.start_key_.get_obj_ptr());
    row.count_ = range.start_key_.get_obj_cnt();
    if (OB_FAIL(get_list_part_ids(part_id, row, partition_array, partition_num, part_ids))) {
      LOG_WARN("get list subpart idxs failed", K(ret));
    }
  }
  return ret;
}

/// special case: char and varchar && oracle mode int and numberic
bool ObPartitionUtils::is_types_equal_for_partition_check(
    const common::ObObjType& type1, const common::ObObjType& type2)
{
  bool is_equal = false;
  if (type1 == type2) {
    is_equal = true;
  } else if ((common::ObCharType == type1 || common::ObVarcharType == type1) &&
             (common::ObCharType == type2 || common::ObVarcharType == type2)) {
    is_equal = true;
  } else if (share::is_oracle_mode()) {
    if ((common::ObIntType == type1 || common::ObNumberType == type1) &&
        (common::ObIntType == type2 || common::ObNumberType == type2)) {
      is_equal = true;
    } else if ((common::ObNumberFloatType == type1 || common::ObNumberType == type1) &&
               (common::ObNumberFloatType == type2 || common::ObNumberType == type2)) {
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

int ObPartitionUtils::get_range_part_ids(ObPartition& start_bound, ObPartition& end_bound,
    const ObBorderFlag& border_flag, const bool reverse, ObPartition* const* partition_array,
    const int64_t partition_num, ObIArray<int64_t>& part_ids, bool get_part_index /* = false */)
{
  int ret = OB_SUCCESS;
  part_ids.reset();
  int64_t start = 0;
  int64_t end = 0;
  if (partition_num < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected partition num", K(ret));
  } else if (OB_FAIL(get_start(partition_array, partition_num, start_bound, start))) {
    LOG_WARN("Failed to get start partition idx", K(ret));
  } else if (OB_FAIL(get_end(partition_array, partition_num, border_flag, end_bound, end))) {
    LOG_WARN("Failed to get end partition idx", K(ret));
  } else if (start < 0 || end >= partition_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid start or end", K(start), K(end), K(partition_num), K(ret));
  } else if (!reverse) {
    // TODO assump that subpartition template and no split
    for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) {
      if (OB_ISNULL(partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Partition is NULL", K(i), K(ret));
      } else if (get_part_index) {
        if (OB_FAIL(part_ids.push_back(i))) {
          COMMON_LOG(WARN, "fail to push part index", K(ret));
        }
      } else if (OB_FAIL(part_ids.push_back(partition_array[i]->get_part_id()))) {
        COMMON_LOG(WARN, "fail to push part id", K(ret));
      } else {
      }  // do nothing
    }
  } else {
    for (int64_t i = end; OB_SUCC(ret) && i >= start; --i) {
      if (OB_ISNULL(partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Partition is NULL", K(i), K(ret));
      } else if (get_part_index) {
        if (OB_FAIL(part_ids.push_back(i))) {
          COMMON_LOG(WARN, "fail to push part index", K(ret));
        }
      } else if (OB_FAIL(part_ids.push_back(partition_array[i]->get_part_id()))) {
        COMMON_LOG(WARN, "fail to push part id", K(ret));
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObPartitionUtils::get_list_part_ids(const ObNewRow& row, ObPartition* const* partition_array,
    const int64_t partition_num, ObIArray<int64_t>& part_ids, bool get_part_index /* = false */)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
    const ObIArray<common::ObNewRow>& list_row_values = partition_array[i]->get_list_row_values();
    for (int64_t j = 0; OB_SUCC(ret) && j < list_row_values.count(); j++) {
      const ObNewRow& list_row = list_row_values.at(j);
      if (row == list_row) {
        if (get_part_index) {
          if (OB_FAIL(part_ids.push_back(i))) {
            LOG_WARN("fail to push part index", K(ret));
          }
        } else if (OB_FAIL(part_ids.push_back(partition_array[i]->get_part_id()))) {
          LOG_WARN("fail to push back part idx", K(ret));
        }
        break;
      }
    }
  }

  if (OB_SUCC(ret) && part_ids.count() == 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      const ObIArray<common::ObNewRow>& list_row_values = partition_array[i]->get_list_row_values();
      if (list_row_values.count() == 1) {
        const ObNewRow& list_row = list_row_values.at(0);
        if (list_row.get_count() >= 1) {
          if (list_row.get_cell(0).is_max_value()) {
            // add default partition_id/partition_index
            if (get_part_index) {
              if (OB_FAIL(part_ids.push_back(i))) {
                LOG_WARN("fail to push part index", K(ret));
              }
            } else if (OB_FAIL(part_ids.push_back(partition_array[i]->get_part_id()))) {
              LOG_WARN("fail to push back part idx", K(ret));
            }
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionUtils::get_list_part_ids(const int64_t part_id, const ObNewRow& row,
    ObSubPartition* const* partition_array, const int64_t partition_num, ObIArray<int64_t>& subpart_ids,
    bool get_subpart_index /* = false */)
{
  UNUSED(part_id);
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
    const ObIArray<common::ObNewRow>& list_row_values = partition_array[i]->get_list_row_values();
    for (int64_t j = 0; OB_SUCC(ret) && j < list_row_values.count(); j++) {
      const ObNewRow& list_row = list_row_values.at(j);
      if (row == list_row) {
        if (get_subpart_index) {
          if (OB_FAIL(subpart_ids.push_back(i))) {
            LOG_WARN("fail to push part index", K(ret));
          }
        } else if (OB_FAIL(subpart_ids.push_back(partition_array[i]->get_sub_part_id()))) {
          LOG_WARN("fail to push back part idx", K(ret));
        }
        break;
      }
    }
  }

  if (OB_SUCC(ret) && subpart_ids.count() == 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      const ObIArray<common::ObNewRow>& list_row_values = partition_array[i]->get_list_row_values();
      if (list_row_values.count() == 1) {
        const ObNewRow& list_row = list_row_values.at(0);
        if (list_row.get_count() >= 1) {
          if (list_row.get_cell(0).is_max_value()) {
            // add default subpartition_id/subpartition_index
            if (get_subpart_index) {
              if (OB_FAIL(subpart_ids.push_back(i))) {
                LOG_WARN("fail to push part index", K(ret));
              }
            } else if (OB_FAIL(subpart_ids.push_back(partition_array[i]->get_sub_part_id()))) {
              LOG_WARN("fail to push back part idx", K(ret));
            }
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionUtils::get_range_part_ids(const ObNewRow& row, ObPartition* const* partition_array,
    const int64_t partition_num, ObIArray<int64_t>& part_ids, bool get_part_index /* = false */)
{
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
  return get_range_part_ids(
      start_tmp, end_tmp, border_flag, false, partition_array, partition_num, part_ids, get_part_index);
}

int ObPartitionUtils::get_range_part_ids(const common::ObNewRange& range, const bool reverse,
    ObPartition* const* partition_array, const int64_t partition_num, common::ObIArray<int64_t>& part_ids)
{
  ObPartition start_tmp;
  start_tmp.high_bound_val_ = range.start_key_;
  ObPartition end_tmp;
  end_tmp.high_bound_val_ = range.end_key_;
  return get_range_part_ids(start_tmp, end_tmp, range.border_flag_, reverse, partition_array, partition_num, part_ids);
}

int ObPartitionUtils::get_range_part_ids(const int64_t part_id, const common::ObNewRange& range, const bool reverse,
    ObSubPartition* const* partition_array, const int64_t partition_num, common::ObIArray<int64_t>& part_ids)
{
  ObSubPartition start_tmp;
  start_tmp.part_id_ = part_id;
  start_tmp.high_bound_val_ = range.start_key_;
  ObSubPartition end_tmp;
  end_tmp.part_id_ = part_id;
  end_tmp.high_bound_val_ = range.end_key_;
  return get_range_part_ids(start_tmp, end_tmp, range.border_flag_, reverse, partition_array, partition_num, part_ids);
}

int ObPartitionUtils::get_range_part_ids(const int64_t part_id, const ObNewRow& row,
    ObSubPartition* const* partition_array, const int64_t partition_num, ObIArray<int64_t>& part_ids,
    bool get_subpart_index /* = false */)
{
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
  return get_range_part_ids(
      start_tmp, end_tmp, border_flag, false, partition_array, partition_num, part_ids, get_subpart_index);
}

int ObPartitionUtils::get_range_part_ids(ObSubPartition& start_bound, ObSubPartition& end_bound,
    const ObBorderFlag& border_flag, const bool reverse, ObSubPartition* const* partition_array,
    const int64_t partition_num, common::ObIArray<int64_t>& subpart_ids, bool get_subpart_index /* = false */)
{
  int ret = OB_SUCCESS;
  subpart_ids.reset();
  int64_t start = 0;
  int64_t end = 0;
  if (partition_num < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected partition num", K(ret));
  } else if (OB_FAIL(get_start(partition_array, partition_num, start_bound, start))) {
    LOG_WARN("Failed to get start partition idx", K(ret));
  } else if (OB_FAIL(get_end(partition_array, partition_num, border_flag, end_bound, end))) {
    LOG_WARN("Failed to get end partition idx", K(ret));
  } else if (start < 0 || end >= partition_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid start or end", K(start), K(end), K(partition_num), K(ret));
  } else if (!reverse) {
    // TODO assump that subpartition template and no split
    for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) {
      if (OB_ISNULL(partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Partition is NULL", K(i), K(ret));
      } else if (get_subpart_index) {
        if (OB_FAIL(subpart_ids.push_back(i))) {
          COMMON_LOG(WARN, "fail to push part index", K(ret));
        }
      } else if (OB_FAIL(subpart_ids.push_back(partition_array[i]->get_sub_part_id()))) {
        COMMON_LOG(WARN, "fail to push part id", K(ret));
      } else {
      }  // do nothing
    }
  } else {
    for (int64_t i = end; OB_SUCC(ret) && i >= start; --i) {
      if (OB_ISNULL(partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Partition is NULL", K(i), K(ret));
      } else if (get_subpart_index) {
        if (OB_FAIL(subpart_ids.push_back(i))) {
          COMMON_LOG(WARN, "fail to push part index", K(ret));
        }
      } else if (OB_FAIL(subpart_ids.push_back(partition_array[i]->get_sub_part_id()))) {
        COMMON_LOG(WARN, "fail to push part id", K(ret));
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObPartitionUtils::convert_rows_to_sql_literal(const common::ObIArray<common::ObNewRow>& rows, char* buf,
    const int64_t buf_len, int64_t& pos, bool print_collation, const common::ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;

  for (int64_t j = 0; OB_SUCC(ret) && j < rows.count(); j++) {
    if (0 != j) {
      if (OB_FAIL(BUF_PRINTF(","))) {
        LOG_WARN("Failed to add comma", K(ret));
      }
    }
    const common::ObNewRow& row = rows.at(j);
    if (OB_SUCC(ret) && row.get_count() > 1) {
      if (OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("Failed to add comma", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
      const ObObj& tmp_obj = row.get_cell(i);
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
        } else if (OB_FAIL(databuff_printf(
                       buf, buf_len, pos, " collate %s", ObCharset::collation_name(tmp_obj.get_collation_type())))) {
          LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
        }
      } else if (lib::is_oracle_mode() && (tmp_obj.get_meta().is_otimestamp_type() || tmp_obj.is_datetime())) {
        if (OB_FAIL(print_oracle_datetime_literal(tmp_obj, buf, buf_len, pos, tz_info))) {
          LOG_WARN("Failed to print_oracle_datetime_literal", K(tmp_obj), K(ret));
        }
      } else if (tmp_obj.is_year()) {
        if (OB_FAIL(ObTimeConverter::year_to_str(tmp_obj.get_year(), buf, buf_len, pos))) {
          LOG_WARN("Failed to year_to_str", K(tmp_obj), K(ret));
        }
      } else if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos, tz_info))) {
        LOG_WARN("Failed to print sql literal", K(ret));
      } else {
      }
    }
    if (OB_SUCC(ret) && row.get_count() > 1) {
      if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("Failed to add comma", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionUtils::convert_rowkey_to_sql_literal(const ObRowkey& rowkey, char* buf, const int64_t buf_len,
    int64_t& pos, bool print_collation, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  if (!rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid rowkey", K(rowkey), K(ret));
  } else {
    const ObObj* objs = rowkey.get_obj_ptr();
    if (NULL == objs) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("objs is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
      const ObObj& tmp_obj = objs[i];
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
        } else if (OB_FAIL(databuff_printf(
                       buf, buf_len, pos, " collate %s", ObCharset::collation_name(tmp_obj.get_collation_type())))) {
          LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
        }
      } else if (lib::is_oracle_mode() && (tmp_obj.get_meta().is_otimestamp_type() || tmp_obj.is_datetime())) {
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

int ObPartitionUtils::print_oracle_datetime_literal(
    const common::ObObj& tmp_obj, char* buf, const int64_t buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && (tmp_obj.get_meta().is_otimestamp_type() || tmp_obj.is_datetime())) {
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos > buf_len)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("buf should not be null", K(buf), K(buf_len), K(pos), K(ret));
    } else {
      switch (tmp_obj.get_type()) {
        case ObObjType::ObDateTimeType: {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TO_DATE("))) {
            LOG_WARN("Failed to print collation", K(ret), K(tmp_obj));
          } else if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos, tz_info))) {
            LOG_WARN("Failed to print sql literal", K(ret));
          } else if (OB_FAIL(
                         databuff_printf(buf, buf_len, pos, ", 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"))) {
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
    const common::ObIArray<common::ObNewRow>& rows, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  char serialize_buf[OB_MAX_B_PARTITION_EXPR_LENGTH];
  int64_t seri_pos = 0;

  if (OB_FAIL(serialization::encode_vi64(serialize_buf, sizeof(serialize_buf), seri_pos, rows.count()))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); i++) {
    if (OB_FAIL(rows.at(i).serialize(serialize_buf, sizeof(serialize_buf), seri_pos))) {
      LOG_WARN("fail to encode row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(hex_print(serialize_buf, seri_pos, buf, buf_len, pos))) {
      LOG_WARN("Failed to print hex", K(ret), K(seri_pos), K(buf_len));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObPartitionUtils::convert_rowkey_to_hex(const ObRowkey& rowkey, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  char serialize_buf[OB_MAX_B_HIGH_BOUND_VAL_LENGTH];
  int64_t seri_pos = 0;
  if (OB_FAIL(rowkey.serialize(serialize_buf, sizeof(serialize_buf), seri_pos))) {
    LOG_WARN("Failed to serialize rowkey", K(rowkey), K(ret));
  } else if (OB_FAIL(hex_print(serialize_buf, seri_pos, buf, buf_len, pos))) {
    LOG_WARN("Failed to print hex", K(ret), K(seri_pos), K(buf_len));
  } else {
  }  // do nothing
  return ret;
}

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
      collation_connection_(CS_TYPE_INVALID)
{}

ObViewSchema::ObViewSchema(ObIAllocator* allocator)
    : ObSchema(allocator),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false),
      materialized_(false),
      character_set_client_(CHARSET_INVALID),
      collation_connection_(CS_TYPE_INVALID)
{}

ObViewSchema::~ObViewSchema()
{}

ObViewSchema::ObViewSchema(const ObViewSchema& src_schema)
    : ObSchema(),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false),
      materialized_(false),
      character_set_client_(CHARSET_INVALID),
      collation_connection_(CS_TYPE_INVALID)
{
  *this = src_schema;
}

ObViewSchema& ObViewSchema::operator=(const ObViewSchema& src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;

    view_check_option_ = src_schema.view_check_option_;
    view_is_updatable_ = src_schema.view_is_updatable_;
    materialized_ = src_schema.materialized_;
    character_set_client_ = src_schema.character_set_client_;
    collation_connection_ = src_schema.collation_connection_;

    if (OB_FAIL(deep_copy_str(src_schema.view_definition_, view_definition_))) {
      LOG_WARN("Fail to deep copy view definition, ", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObViewSchema::operator==(const ObViewSchema& other) const
{
  return view_definition_ == other.view_definition_ && view_check_option_ == other.view_check_option_ &&
         view_is_updatable_ == other.view_is_updatable_ && materialized_ == other.materialized_ &&
         character_set_client_ == other.character_set_client_ && collation_connection_ == other.collation_connection_;
}

bool ObViewSchema::operator!=(const ObViewSchema& other) const
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
      collation_connection_);
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
      collation_connection_);

  if (!OB_SUCC(ret)) {
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
      collation_connection_);
  return len;
}

const char* ob_view_check_option_str(const ViewCheckOption option)
{
  const char* ret = "invalid";
  const char* option_ptr[] = {"none", "local", "cascaded"};
  if (option >= 0 && option < VIEW_CHECK_OPTION_MAX) {
    ret = option_ptr[option];
  }
  return ret;
}

bool can_rereplicate_index_status(const ObIndexStatus &idst)
{
  // unusable index can also rereplicate
  return INDEX_STATUS_AVAILABLE == idst || INDEX_STATUS_UNUSABLE == idst;
}

const char* ob_index_status_str(ObIndexStatus status)
{
  const char* ret = "invalid";
  const char* status_ptr[] = {"not_found",
      "unavailable",
      "available",
      "unique_checking",
      "unique_inelegible",
      "index_error",
      "restore_index_error",
      "unusable"};
  if (status >= 0 && status < INDEX_STATUS_MAX) {
    ret = status_ptr[status];
  }
  return ret;
}

ObTenantTableId& ObTenantTableId::operator=(const ObTenantTableId& tenant_table_id)
{
  tenant_id_ = tenant_table_id.tenant_id_;
  table_id_ = tenant_table_id.table_id_;
  return *this;
}

/*************************For managing Privileges****************************/
// ObTenantUserId
OB_SERIALIZE_MEMBER(ObTenantUserId, tenant_id_, user_id_);

// ObTenantUrObjId
OB_SERIALIZE_MEMBER(ObTenantUrObjId, tenant_id_, grantee_id_, obj_id_, obj_type_, col_id_);

// ObPrintPrivSet
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
  if ((priv_set_ & OB_PRIV_EXECUTE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_EXECUTE,");
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
  if (OB_SUCCESS == ret && pos > 1) {
    pos--;  // Delete last ','
  }
  ret = BUF_PRINTF("\"");
  return pos;
}

// ObPrintSysPrivSet
DEF_TO_STRING(ObPrintPackedPrivArray)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = BUF_PRINTF("\"");
  if (packed_priv_array_.count() > 0) {
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

// ObPriv

ObPriv& ObPriv::operator=(const ObPriv& other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    schema_version_ = other.schema_version_;
    priv_set_ = other.priv_set_;
    priv_array_ = other.priv_array_;
  }
  return *this;
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

OB_SERIALIZE_MEMBER(ObPriv, tenant_id_, user_id_, schema_version_, priv_set_, priv_array_);

// ObUserInfo
ObUserInfo::ObUserInfo(ObIAllocator* allocator)
    : ObSchema(allocator),
      ObPriv(allocator),
      user_name_(),
      host_name_(),
      passwd_(),
      info_(),
      locked_(false),
      ssl_type_(ObSSLType::SSL_TYPE_NOT_SPECIFIED),
      ssl_cipher_(),
      x509_issuer_(),
      x509_subject_(),
      type_(OB_USER),
      grantee_id_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(*allocator)),
      role_id_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(*allocator)),
      profile_id_(OB_INVALID_ID),
      password_last_changed_timestamp_(OB_INVALID_TIMESTAMP),
      role_id_option_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(*allocator)),
      max_connections_(0),
      max_user_connections_(0)
{}

ObUserInfo::ObUserInfo(const ObUserInfo& other) : ObSchema(), ObPriv()
{
  *this = other;
}

ObUserInfo::~ObUserInfo()
{}

ObUserInfo& ObUserInfo::operator=(const ObUserInfo& other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);
    locked_ = other.locked_;
    ssl_type_ = other.ssl_type_;

    if (OB_FAIL(deep_copy_str(other.user_name_, user_name_))) {
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
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObUserInfo::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
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
  profile_id_ = OB_INVALID_ID;
  password_last_changed_timestamp_ = OB_INVALID_TIMESTAMP;
  max_connections_ = 0;
  max_user_connections_ = 0;
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
  LST_DO_CODE(
      OB_UNIS_DECODE, user_name, host_name, passwd, info, locked_, ssl_type_, ssl_cipher, x509_issuer, x509_subject);

  if (!OB_SUCC(ret)) {
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
  return len;
}

int ObUserInfo::add_role_id(const uint64_t id, const uint64_t admin_option, const uint64_t disable_flag)
{
  int ret = OB_SUCCESS;
  uint64_t option = 0;
  set_admin_option(option, admin_option);
  set_disable_flag(option, disable_flag);
  OZ(role_id_array_.push_back(id));
  OZ(role_id_option_array_.push_back(option));
  return ret;
}

bool ObUserInfo::role_exists(const uint64_t role_id, const uint64_t option) const
{
  bool exists = false;
  for (int64_t i = 0; !exists && i < get_role_count(); ++i) {
    if (role_id == get_role_id_array().at(i) &&
        (option == NO_OPTION || option == get_admin_option(get_role_id_option_array().at(i)))) {
      exists = true;
    }
  }
  return exists;
}

int ObUserInfo::get_seq_by_role_id(uint64_t role_id, uint64_t& seq) const
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

int ObUserInfo::get_nth_role_option(uint64_t nth, uint64_t& option) const
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

// ObDBPriv
ObDBPriv& ObDBPriv::operator=(const ObDBPriv& other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);
    sort_ = other.sort_;

    if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
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
  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WARN("Fail to deep copy user_name", K(db), K(ret));
  } else {
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDBPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, sort_);
  return len;
}

// ObTablePriv
ObTablePriv& ObTablePriv::operator=(const ObTablePriv& other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);

    if (OB_FAIL(deep_copy_str(other.db_, db_))) {
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
  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WARN("Fail to deep copy user_name", K(db), K(ret));
  } else if (OB_FAIL(deep_copy_str(table, table_))) {
    LOG_WARN("Fail to deep copy user_name", K(table), K(ret));
  } else {
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTablePriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, table_);
  return len;
}

// ObObjPriv
ObObjPriv& ObObjPriv::operator=(const ObObjPriv& other)
{
  if (this != &other) {
    reset();
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);

    obj_id_ = other.obj_id_;
    obj_type_ = other.obj_type_;
    col_id_ = other.col_id_;
    grantor_id_ = other.grantor_id_;
    grantee_id_ = other.grantee_id_;
  }
  return *this;
}

bool ObObjPriv::is_valid() const
{
  return ObSchema::is_valid() && tenant_id_ != common::OB_INVALID_ID && obj_id_ != common::OB_INVALID_ID &&
         obj_type_ != common::OB_INVALID_ID && col_id_ != common::OB_INVALID_ID &&
         grantor_id_ != common::OB_INVALID_ID && grantee_id_ != common::OB_INVALID_ID;
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

// ObSysPriv
ObSysPriv& ObSysPriv::operator=(const ObSysPriv& other)
{
  if (this != &other) {
    reset();
    error_ret_ = other.error_ret_;
    grantee_id_ = other.grantee_id_;
    ObPriv::operator=(other);
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
  if (!OB_SUCC(ret)) {
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

int ObNeedPriv::deep_copy(const ObNeedPriv& other, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  priv_level_ = other.priv_level_;
  priv_set_ = other.priv_set_;
  is_sys_table_ = other.is_sys_table_;
  if (OB_FAIL(ob_write_string(allocator, other.db_, db_))) {
    LOG_WARN("Fail to deep copy db", K_(db), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.table_, table_))) {
    LOG_WARN("Fail to deep copy table", K_(table), K(ret));
  }
  return ret;
}

int ObStmtNeedPrivs::deep_copy(const ObStmtNeedPrivs& other, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  need_privs_.reset();
  if (OB_FAIL(need_privs_.reserve(other.need_privs_.count()))) {
    LOG_WARN("fail to reserve need prives size", K(ret), K(other.need_privs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.need_privs_.count(); ++i) {
    const ObNeedPriv& priv_other = other.need_privs_.at(i);
    ObNeedPriv priv_new;
    if (OB_FAIL(priv_new.deep_copy(priv_other, allocator))) {
      LOG_WARN("Fail to deep copy ObNeedPriv", K(priv_new), K(ret));
    } else {
      need_privs_.push_back(priv_new);
    }
  }
  return ret;
}

int ObOraNeedPriv::deep_copy(const ObOraNeedPriv& other, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  obj_id_ = other.obj_id_;
  col_id_ = other.col_id_;
  obj_type_ = other.obj_type_;
  obj_privs_ = other.obj_privs_;
  grantee_id_ = other.grantee_id_;
  check_flag_ = other.check_flag_;
  owner_id_ = other.owner_id_;
  if (OB_FAIL(ob_write_string(allocator, other.db_name_, db_name_))) {
    LOG_WARN("Fail to deep copy db", K_(db_name), K(ret));
  }
  return ret;
}

bool ObOraNeedPriv::same_obj(const ObOraNeedPriv& other)
{
  if (grantee_id_ == other.grantee_id_ && obj_id_ == other.obj_id_ && col_id_ == other.col_id_ &&
      obj_type_ == other.obj_type_ && check_flag_ == other.check_flag_)
    return true;
  else
    return false;
}

int ObStmtOraNeedPrivs::deep_copy(const ObStmtOraNeedPrivs& other, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  need_privs_.reset();
  if (OB_FAIL(need_privs_.reserve(other.need_privs_.count()))) {
    LOG_WARN("fail to reserve need prives size", K(ret), K(other.need_privs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.need_privs_.count(); ++i) {
    const ObOraNeedPriv& priv_other = other.need_privs_.at(i);
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

const char* PART_TYPE_STR[PARTITION_FUNC_TYPE_MAX + 1] = {"hash",
    "key",
    "key",
    "range",
    "range columns",
    "list",
    "key_v2",
    "list columns",
    "hash_v2",
    "key_v3",
    "key_v3",
    "unknown"};

int get_part_type_str(ObPartitionFuncType type, common::ObString& str, bool can_change /*= true*/)
{
  int ret = common::OB_SUCCESS;
  if (type >= PARTITION_FUNC_TYPE_MAX) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid partition function type", K(type));
  } else {
    if (share::is_oracle_mode() && can_change) {
      if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == type) {
        type = PARTITION_FUNC_TYPE_RANGE;
      } else if (PARTITION_FUNC_TYPE_LIST_COLUMNS == type) {
        type = PARTITION_FUNC_TYPE_LIST;
      }
    }
    str = common::ObString::make_string(PART_TYPE_STR[type]);
  }
  return ret;
}

const char* OB_PRIV_LEVEL_STR[OB_PRIV_MAX_LEVEL] = {
    "INVALID_LEVEL", "USER_LEVEL", "DB_LEVEL", "TABLE_LEVEL", "DB_ACCESS_LEVEL"};

const char* ob_priv_level_str(const ObPrivLevel grant_level)
{
  const char* ret = "Unknown";
  if (grant_level < OB_PRIV_MAX_LEVEL && grant_level > OB_PRIV_INVALID_LEVEL) {
    ret = OB_PRIV_LEVEL_STR[grant_level];
  }
  return ret;
}

// ObTableType=>const char* ;
const char* ob_table_type_str(ObTableType type)
{
  const char* type_ptr = "UNKNOWN";
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
      // TODO: materialized view
    case USER_INDEX: {
      type_ptr = "USER INDEX";
      break;
    }
    case TMP_TABLE: {
      type_ptr = "TMP TABLE";
      break;
    }
    default: {
      LOG_WARN("unkonw table type", K(type));
      break;
    }
  }
  return type_ptr;
}

// ObTableType => mysql table type str : SYSTEM VIEW, BASE TABLE, VIEW
const char* ob_mysql_table_type_str(ObTableType type)
{
  const char* type_ptr = "UNKNOWN";
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
      LOG_WARN("unkonw table type", K(type));
      break;
  }
  return type_ptr;
}

ObTableType get_inner_table_type_by_id(const uint64_t tid)
{
  if (!is_inner_table(tid)) {
    LOG_WARN("tid is not inner table", K(tid));
  }
  ObTableType type = MAX_TABLE_TYPE;
  if (is_sys_table(tid)) {
    type = SYSTEM_TABLE;
  } else if (is_virtual_table(tid)) {
    type = VIRTUAL_TABLE;
  } else if (is_sys_view(tid)) {
    type = SYSTEM_VIEW;
  } else {
    // 30001 ~ 50000
    // MAX_TABLE_TYPE;
  }
  return type;
}

int need_change_schema_id(const ObSchemaType schema_type, const uint64_t schema_id, bool& need_change)
{
  int ret = OB_SUCCESS;
  need_change = false;
  if ((TABLE_SCHEMA == schema_type || TABLE_SIMPLE_SCHEMA == schema_type) &&
      OB_SYS_TENANT_ID != extract_tenant_id(schema_id) && !is_link_table_id(schema_id)) {
    if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(schema_id, need_change))) {
      LOG_WARN("fail to check table_id", K(ret), K(schema_id));
    }
  }
  return ret;
}

const char* schema_type_str(const ObSchemaType schema_type)
{
  const char* str = "";
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
  } else if (OUTLINE_SCHEMA == schema_type) {
    str = "outline_schema";
  } else if (SYNONYM_SCHEMA == schema_type) {
    str = "synonym_schema";
  } else if (PLAN_BASELINE_SCHEMA == schema_type) {
    str = "plan_baseline_schema";
  } else if (UDF_SCHEMA == schema_type) {
    str = "udf_schema";
  } else if (UDT_SCHEMA == schema_type) {
    str = "udt_schema";
  } else if (SEQUENCE_SCHEMA == schema_type) {
    str = "sequence_schema";
  } else if (PROFILE_SCHEMA == schema_type) {
    str = "profile_schema";
  } else if (DBLINK_SCHEMA == schema_type) {
    str = "dblink_schema";
  }
  return str;
}

bool is_normal_schema(const ObSchemaType schema_type)
{
  return schema_type == TENANT_SCHEMA || schema_type == USER_SCHEMA || schema_type == DATABASE_SCHEMA ||
         schema_type == TABLEGROUP_SCHEMA || schema_type == TABLE_SCHEMA || schema_type == PLAN_BASELINE_SCHEMA ||
         schema_type == OUTLINE_SCHEMA || schema_type == ROUTINE_SCHEMA || schema_type == SYNONYM_SCHEMA ||
         schema_type == PACKAGE_SCHEMA || schema_type == TRIGGER_SCHEMA || schema_type == SEQUENCE_SCHEMA ||
         schema_type == UDF_SCHEMA || schema_type == UDT_SCHEMA || schema_type == SYS_VARIABLE_SCHEMA ||
         schema_type == TABLE_SIMPLE_SCHEMA || schema_type == KEYSTORE_SCHEMA || schema_type == TABLESPACE_SCHEMA ||
         schema_type == PROFILE_SCHEMA || schema_type == DBLINK_SCHEMA || false;
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

bool ObFixedParam::has_equal_value(const ObObj& other_value) const
{
  bool is_equal = false;
  if (value_.get_type() != other_value.get_type()) {
    is_equal = false;
  } else {
    is_equal = value_ == other_value;
  }
  return is_equal;
}

bool ObFixedParam::is_equal(const ObFixedParam& other_param) const
{
  bool is_equal = false;
  if (has_equal_value(other_param.value_)) {
    is_equal = offset_ == other_param.offset_;
  }
  return is_equal;
}

ObMaxConcurrentParam::ObMaxConcurrentParam(common::ObIAllocator* allocator, const common::ObMemAttr& attr)
    : allocator_(allocator),
      concurrent_num_(UNLIMITED),
      outline_content_(),
      mem_attr_(attr),
      fixed_param_store_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocatorWithAttr(allocator, attr))
{}

ObMaxConcurrentParam::~ObMaxConcurrentParam()
{}

int ObMaxConcurrentParam::destroy()
{
  int ret = OB_SUCCESS;

  // reset outline_content
  if (outline_content_.ptr() != NULL) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("allocator is NULL", K(ret));
    } else {
      allocator_->free(outline_content_.ptr());
    }
  }
  outline_content_.reset();

  // reset fixed_param_store
  if (fixed_param_store_.count() != 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("allocator is NULL", K(ret));
    } else {
      for (int64_t i = 0; i < fixed_param_store_.count(); ++i) {
        fixed_param_store_.at(i).offset_ = OB_INVALID_INDEX;
        ObObj& cur_value = fixed_param_store_.at(i).value_;
        if (cur_value.need_deep_copy()) {
          if (OB_ISNULL(cur_value.get_data_ptr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("ptr is NULL", K(ret), K(cur_value));
          } else {
            allocator_->free(const_cast<void*>(cur_value.get_data_ptr()));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    fixed_param_store_.destroy();
    concurrent_num_ = UNLIMITED;
    // allocator_ = NULL;
  }

  return ret;
}

int ObMaxConcurrentParam::match_fixed_param(const ParamStore& const_param_store, bool& is_match) const
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
      const ObObj& plan_param_value = const_param_store.at(offset_of_plan_param_store);
      const ObFixedParam& cur_fixed_param = fixed_param_store_.at(i);
      is_same = cur_fixed_param.has_equal_value(plan_param_value);
    }
  }
  if (OB_SUCC(ret)) {
    is_match = is_same;
  }
  return ret;
}

// this func only compare fixed_params
int ObMaxConcurrentParam::same_param_as(const ObMaxConcurrentParam& other, bool& is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (fixed_param_store_.count() != other.fixed_param_store_.count()) {
    is_same = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < fixed_param_store_.count(); i++) {
      const ObFixedParam& cur_fixed_param = fixed_param_store_.at(i);
      const ObFixedParam& other_fixed_param = other.fixed_param_store_.at(i);
      is_same = cur_fixed_param.is_equal(other_fixed_param);
    }
  }
  return ret;
}

int ObMaxConcurrentParam::assign(const ObMaxConcurrentParam& src_param)
{
  int ret = OB_SUCCESS;
  if (this != &src_param) {
    // copy concurent_num
    concurrent_num_ = src_param.concurrent_num_;
    mem_attr_ = src_param.mem_attr_;

    // copy outline_content
    if (OB_FAIL(deep_copy_outline_content(src_param.outline_content_))) {
      LOG_WARN("fail to deep copy outline_content", K(ret));
    }

    // copy fix_param
    fixed_param_store_.reset();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fixed_param_store_.reserve(src_param.fixed_param_store_.count()))) {
      LOG_WARN("fail to reserve array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src_param.fixed_param_store_.count(); i++) {
        const ObFixedParam& cur_fixed_param = src_param.fixed_param_store_.at(i);
        ObFixedParam tmp_fixed_param;
        tmp_fixed_param.offset_ = cur_fixed_param.offset_;
        if (OB_FAIL(deep_copy_param_value(cur_fixed_param.value_, tmp_fixed_param.value_))) {
          LOG_WARN("fail to deep copy param value", K(ret));
        } else if (OB_FAIL(fixed_param_store_.push_back(tmp_fixed_param))) {
          LOG_WARN("fail to push back fix param", K(ret));
        } else { /*do nothing*/
        }
      }
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
  return convert_size;
}

int ObMaxConcurrentParam::deep_copy_outline_content(const ObString& src)
{
  int ret = OB_SUCCESS;
  char* content_buf = NULL;
  if (src.length() > 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_UNLIKELY(NULL == (content_buf = static_cast<char*>(allocator_->alloc(src.length() + 1))))) {
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

int ObMaxConcurrentParam::deep_copy_param_value(const ObObj& src, ObObj& dest)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();
  if (size > 0) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_->alloc(size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
      LOG_WARN("Fail to deep copy obj, ", K(ret));
    } else { /*do nothing*/
    }
  } else {
    dest = src;
  }
  return ret;
}

int ObMaxConcurrentParam::get_fixed_param_with_offset(int64_t offset, ObFixedParam& fixed_param, bool& is_found) const
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

  // serilize fixed_param_store_
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    const ObFixedParam& cur_fixed_param = fixed_param_store_.at(i);
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cur_fixed_param.offset_))) {
      LOG_WARN("fail to serialize cur_fixed_param offset", K(ret));
    } else if (OB_FAIL(cur_fixed_param.value_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize cur_fixed_param value", K(ret));
    } else { /*do nothing*/
    }
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObMaxConcurrentParam)
{
  int ret = OB_SUCCESS;
  ObString outline_content;
  int64_t param_count = 0;
  ;
  LST_DO_CODE(OB_UNIS_DECODE, concurrent_num_, outline_content, param_count);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_outline_content(outline_content))) {
    LOG_WARN("fail to deep copy outline_content", K(ret));
  } else {
    ObFixedParam fixed_param;
    ObObj tmp_value;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; i++) {
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &fixed_param.offset_))) {
        LOG_WARN("fail to deserialize fixed_param offset", K(ret));
      } else if (OB_FAIL(tmp_value.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize fixed_param value", K(ret));
      } else if (OB_FAIL(deep_copy_param_value(tmp_value, fixed_param.value_))) {
        LOG_WARN("fail to deep_copy fixed_param value");
      } else if (OB_FAIL(fixed_param_store_.push_back(fixed_param))) {
        LOG_WARN("fail to push back fixed_param to param store", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMaxConcurrentParam)
{
  int64_t len = 0;
  const int64_t param_count = fixed_param_store_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, concurrent_num_, outline_content_, param_count);

  for (int64_t i = 0; i < param_count; ++i) {
    const ObFixedParam& cur_fixed_param = fixed_param_store_.at(i);
    len += serialization::encoded_length_vi64(cur_fixed_param.offset_);
    len += cur_fixed_param.value_.get_serialize_size();
  }

  return len;
}

ObOutlineParamsWrapper::ObOutlineParamsWrapper() : allocator_(NULL), outline_params_()
{}

ObOutlineParamsWrapper::ObOutlineParamsWrapper(common::ObIAllocator *allocator)
    : allocator_(allocator), outline_params_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocatorWithAttr(allocator))
{}

ObOutlineParamsWrapper::~ObOutlineParamsWrapper()
{}

int ObOutlineParamsWrapper::destroy()
{
  int ret = OB_SUCCESS;
  int64_t param_count = outline_params_.count();
  if (0 == param_count) {  // do nothing
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("allocator is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < outline_params_.count(); ++i) {
      ObMaxConcurrentParam* param = outline_params_.at(i);
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

ObMaxConcurrentParam* ObOutlineParamsWrapper::get_outline_param(int64_t index) const
{
  ObMaxConcurrentParam* ret = NULL;
  if (index < outline_params_.count()) {
    ret = outline_params_.at(index);
  } else {
    LOG_ERROR("index overflow", K(index), K(outline_params_.count()));
  }
  return ret;
}

ObPlanBaselineInfo::ObPlanBaselineInfo() : ObSchema()
{
  reset();
}

ObPlanBaselineInfo::ObPlanBaselineInfo(common::ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}

ObPlanBaselineInfo::ObPlanBaselineInfo(const ObPlanBaselineInfo& src_info) : ObSchema()
{
  reset();
  *this = src_info;
}

ObPlanBaselineInfo::~ObPlanBaselineInfo()
{}

ObPlanBaselineInfo& ObPlanBaselineInfo::operator=(const ObPlanBaselineInfo& src_info)
{
  if (this != &src_info) {
    reset();
    set_tenant_id(src_info.key_.tenant_id_);
    set_database_id(src_info.key_.db_id_);
    set_plan_baseline_id(src_info.plan_baseline_id_);
    set_schema_version(src_info.schema_version_);
    set_plan_hash_value(src_info.plan_hash_value_);
    set_sql_text(src_info.key_.constructed_sql_);
    set_params_info(src_info.key_.params_info_str_);
    set_outline_data(src_info.outline_data_);
    set_sql_id(src_info.sql_id_);
    set_fixed(src_info.fixed_);
    set_enabled(src_info.enabled_);
    set_executions(src_info.executions_);
    set_cpu_time(src_info.cpu_time_);
    set_hints_info(src_info.hints_info_);
    set_hints_all_worked(src_info.hints_all_worked_);
  }

  return *this;
}

/*int ObPlanBaselineInfo::set_plan_hash_value(const common::number::ObNumber &num)*/
//{
// int ret = OB_SUCCESS;
// uint64_t val = 0;
// if (!num.is_valid_uint64(val)) {
// ret = OB_INVALID_ARGUMENT;
// LOG_WARN("support only in range uint64 now", K(ret));
//} else {
// set_plan_hash_value(val);
//}
// return ret;
/*}*/

int ObOutlineInfo::replace_question_mark(const ObString& not_param_sql, const ObMaxConcurrentParam& concurrent_param,
    int64_t start_pos, int64_t cur_pos, int64_t& question_mark_offset, ObSqlString& string_helper)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator local_allocator;
  ObFixedParam fixed_param;
  bool is_found = false;
  char* buf = NULL;
  const int64_t buf_len = OB_MAX_VARCHAR_LENGTH;
  int64_t pos = 0;

  if (OB_FAIL(concurrent_param.get_fixed_param_with_offset(question_mark_offset, fixed_param, is_found))) {
    LOG_WARN("fail to get fixed_param with offset", K(ret), K(question_mark_offset));
  } else {
    ObString before_token(cur_pos - start_pos, not_param_sql.ptr() + start_pos);
    if (is_found) {
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(local_allocator.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc memory", K(ret), K(buf_len));
      } else if (OB_FAIL(fixed_param.value_.print_sql_literal(buf, buf_len, pos))) {
        LOG_WARN("fail print obj", K(fixed_param), K(ret), K(buf), K(buf_len), K(pos));
      } else if (OB_FAIL(string_helper.append_fmt("%.*s%s", before_token.length(), before_token.ptr(), buf))) {
        LOG_WARN("fail to append fmt", K(ret), K(before_token), K(buf));
      } else { /*do nothing*/
      }
    } else {
      if (OB_FAIL(string_helper.append_fmt("%.*s%s", before_token.length(), before_token.ptr(), "?"))) {
        LOG_WARN("fail to append fmt", K(ret), K(before_token));
      }
    }
    ++question_mark_offset;
  }

  return ret;
}

int ObOutlineInfo::replace_not_param(const ObString& not_param_sql, const ParseNode& node, int64_t start_pos,
    int64_t cur_pos, ObSqlString& string_helper)
{
  int ret = OB_SUCCESS;
  ObString before_token(cur_pos - start_pos, not_param_sql.ptr() + start_pos);
  ObString param_value(node.text_len_, node.raw_text_);
  if (OB_FAIL(string_helper.append_fmt(
          "%.*s%.*s", before_token.length(), before_token.ptr(), param_value.length(), param_value.ptr()))) {
    LOG_WARN("fail to append fmt", K(ret), K(before_token), K(param_value));
  }

  return ret;
}

int ObOutlineInfo::gen_limit_sql(const ObString& visible_signature, const ObMaxConcurrentParam* concurrent_param,
    const ObSQLSessionInfo& session, ObIAllocator& allocator, ObString& limit_sql)
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
  ParamList* parser_param = NULL;
  ParseNode* cur_node = NULL;

  if (OB_ISNULL(concurrent_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(concurrent_param));
  } else if (OB_FAIL(parser.parse(visible_signature, parse_result, FP_MODE))) {
    LOG_WARN("fail to parser visible signature", K(ret), K(visible_signature));
  } else {
    parser_param = parse_result.param_nodes_;
    not_param_sql.assign_ptr(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.param_node_num_; ++i) {
    if (OB_ISNULL(parser_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parser param is NULL", K(ret));
    } else if (OB_ISNULL(cur_node = parser_param->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is NULL", K(ret));
    } else {
      cur_pos = cur_node->pos_;
      if (T_QUESTIONMARK == cur_node->type_) {
        if (OB_FAIL(replace_question_mark(
                not_param_sql, *concurrent_param, start_pos, cur_pos, question_mark_offset, string_helper))) {
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
  } else if (FALSE_IT(last_token.assign_ptr(not_param_sql.ptr() + start_pos,
                 static_cast<ObString::obstr_size_t>(not_param_sql.length() - start_pos)))) {
  } else if (OB_FAIL(string_helper.append_fmt("%.*s", last_token.length(), last_token.ptr()))) {
    LOG_WARN("fail to append fmt", K(ret), K(not_param_sql), K(last_token), K(start_pos));
  } else if (OB_FAIL(ob_write_string(allocator, string_helper.string(), limit_sql))) {
    LOG_WARN("fail to deep copy string", K(ret), K(string_helper), K(limit_sql));
  } else { /*do nothing*/
  }

  return ret;
}

int ObOutlineParamsWrapper::set_allocator(ObIAllocator* allocator, const common::ObMemAttr& attr)
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

int ObOutlineParamsWrapper::assign(const ObOutlineParamsWrapper& src)
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
        void* buf = NULL;
        ObMaxConcurrentParam* param = NULL;
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
  void* param_buf = NULL;
  ObMaxConcurrentParam* param = NULL;
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
  } else { /*do nothing*/
  }
  return ret;
}

// only compare fixed params
int ObOutlineParamsWrapper::has_param(const ObMaxConcurrentParam& param, bool& has_param) const
{
  int ret = OB_SUCCESS;
  has_param = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_param && i < outline_params_.count(); i++) {
    if (OB_ISNULL(outline_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param pointer is NULL", K(ret));
    } else if (OB_FAIL(outline_params_.at(i)->same_param_as(param, has_param))) {
      LOG_WARN("failed to check if param is same with local params", K(i), K(ret));
    } else { /*do nothing*/
    }
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

int ObOutlineParamsWrapper::has_concurrent_limit_param(bool& has_limit_param) const
{
  int ret = OB_SUCCESS;
  has_limit_param = false;
  for (int64_t i = 0; !has_limit_param && OB_SUCC(ret) && i < outline_params_.count(); ++i) {
    ObMaxConcurrentParam* param = outline_params_.at(i);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is NULL", K(ret), K(i));
    } else if (param->is_concurrent_limit_param()) {
      has_limit_param = true;
    } else { /*do nothing*/
    }
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
  } else if (0 == param_count) {  // do nohting
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      void* param_buf = NULL;
      ObMaxConcurrentParam* param = NULL;
      if (OB_UNLIKELY(NULL == (param_buf = allocator_->alloc(sizeof(ObMaxConcurrentParam))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory", K(sizeof(ObMaxConcurrentParam)), K(ret));
      } else if (FALSE_IT(param = new (param_buf) ObMaxConcurrentParam(allocator_))) {
      } else if (OB_FAIL(param->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserilize param", K(ret), K(i));
      } else if (OB_FAIL(outline_params_.push_back(param))) {
        LOG_WARN("fail to push back param", K(ret), K(i));
      } else { /*do nothing*/
      }
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
      LOG_ERROR("param is NULL");
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

ObOutlineInfo::ObOutlineInfo(common::ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator))) {
    LOG_ERROR("fail to set allocator", K(ret));
  }
}

ObOutlineInfo::ObOutlineInfo(const ObOutlineInfo& src_info) : ObSchema()
{
  reset();
  *this = src_info;
}

ObOutlineInfo::~ObOutlineInfo()
{}

ObOutlineInfo& ObOutlineInfo::operator=(const ObOutlineInfo& src_info)
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
    } else { /*do nothing*/
    }

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
  reset_string(outline_content_);
  reset_string(sql_text_);
  reset_string(outline_target_);
  reset_string(owner_);
  used_ = false;
  reset_string(version_);
  compatible_ = true;
  enabled_ = true;
  format_ = HINT_NORMAL;
  outline_params_wrapper_.destroy();
  ObSchema::reset();
}

bool ObOutlineInfo::is_sql_id_valid(const ObString& sql_id)
{
  bool is_valid = true;
  if (sql_id.length() != OB_MAX_SQL_ID_LENGTH) {
    is_valid = false;
  }
  if (is_valid) {
    for (int32_t i = 0; i < sql_id.length(); i++) {
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
  } else if (name_.empty() ||
             !((!signature_.empty() && !sql_text_.empty() && sql_id_.empty()) ||
                 (signature_.empty() && sql_text_.empty() && is_sql_id_valid(sql_id_))) ||
             owner_.empty() || version_.empty() || (outline_content_.empty() && !has_outline_params())) {
    valid_ret = false;
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_ || OB_INVALID_ID == outline_id_) {
    valid_ret = false;
  } else if (schema_version_ <= 0) {
    valid_ret = false;
  } else { /*do nothing*/
  }
  return valid_ret;
}

bool ObOutlineInfo::is_valid_for_replace() const
{
  bool valid_ret = true;
  if (!ObSchema::is_valid()) {
    valid_ret = false;
  } else if (name_.empty() ||
             !((!signature_.empty() && !sql_text_.empty() && sql_id_.empty()) ||
                 (signature_.empty() && sql_text_.empty() && is_sql_id_valid(sql_id_))) ||
             owner_.empty() || version_.empty() || (outline_content_.empty() && !has_outline_params())) {
    valid_ret = false;
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_ || OB_INVALID_ID == outline_id_) {
    valid_ret = false;
  } else { /*do nothing*/
  }
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
      if (NULL == (allocator_ = OB_NEW(
                       ObArenaAllocator, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA))) {
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

int ObOutlineInfo::set_outline_params(const ObString& outline_params_str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(gen_valid_allocator())) {
    LOG_WARN("fail to gen valid allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.set_allocator(allocator_))) {
    LOG_WARN("fail to set allocator", K(ret));
  } else if (OB_FAIL(outline_params_wrapper_.deserialize(outline_params_str.ptr(), outline_params_str.length(), pos))) {
    LOG_WARN("fail to deserialize outline params str", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOutlineInfo::get_hex_str_from_outline_params(ObString& hex_str, ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;
  char* hex_str_buf = NULL;
  int64_t hex_str_buf_size = outline_params_wrapper_.get_serialize_size();
  int64_t pos = 0;
  if (OB_UNLIKELY(NULL == (hex_str_buf = static_cast<char*>(allocator.alloc(hex_str_buf_size))))) {
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

int ObOutlineInfo::get_visible_signature(ObString& visiable_signature) const
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

int ObOutlineInfo::get_outline_sql(
    ObIAllocator& allocator, const ObSQLSessionInfo& session, ObString& outline_sql) const
{
  int ret = OB_SUCCESS;
  bool is_need_filter_hint = true;
  if (OB_FAIL(ObSQLUtils::construct_outline_sql(
          allocator, session, outline_content_, sql_text_, is_need_filter_hint, outline_sql))) {
    LOG_WARN("fail to construct outline sql", K(ret), K(outline_content_), K(sql_text_), K(outline_sql));
  }
  return ret;
}

int ObOutlineInfo::has_concurrent_limit_param(bool& has_limit_param) const
{
  return outline_params_wrapper_.has_concurrent_limit_param(has_limit_param);
}

OB_DEF_SERIALIZE(ObOutlineInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      database_id_,
      outline_id_,
      schema_version_,
      name_,
      signature_,
      outline_content_,
      sql_text_,
      outline_target_,
      owner_,
      used_,
      version_,
      compatible_,
      enabled_,
      format_,
      outline_params_wrapper_,
      sql_id_,
      owner_id_);
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

  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      database_id_,
      outline_id_,
      schema_version_,
      name,
      signature,
      outline_content,
      sql_text,
      outline_target,
      owner,
      used_,
      version,
      compatible_,
      enabled_,
      format_);

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
          LST_DO_CODE(OB_UNIS_DECODE, owner_id_);
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
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      database_id_,
      outline_id_,
      schema_version_,
      name_,
      signature_,
      sql_id_,
      outline_content_,
      sql_text_,
      outline_target_,
      owner_,
      used_,
      version_,
      compatible_,
      enabled_,
      format_,
      outline_params_wrapper_,
      owner_id_);
  return len;
}

OB_SERIALIZE_MEMBER(ObTenantSynonymId, tenant_id_, synonym_id_);
OB_SERIALIZE_MEMBER(ObTenantSequenceId, tenant_id_, sequence_id_);
OB_SERIALIZE_MEMBER((ObAlterOutlineInfo, ObOutlineInfo), alter_option_bitset_);
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
  host_addr_.reset();
}

bool ObDbLinkBaseInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != owner_id_ && OB_INVALID_ID != dblink_id_ &&
         !dblink_name_.empty() && !tenant_name_.empty() && !user_name_.empty() && !password_.empty() &&
         host_addr_.is_valid();
}

OB_SERIALIZE_MEMBER(ObDbLinkInfo, tenant_id_, owner_id_, dblink_id_, schema_version_, dblink_name_, cluster_name_,
    tenant_name_, user_name_, password_, host_addr_);

ObDbLinkSchema::ObDbLinkSchema(const ObDbLinkSchema& other) : ObDbLinkBaseInfo()
{
  reset();
  *this = other;
}

ObDbLinkSchema& ObDbLinkSchema::operator=(const ObDbLinkSchema& other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    owner_id_ = other.owner_id_;
    dblink_id_ = other.dblink_id_;
    schema_version_ = other.schema_version_;
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
    }
    host_addr_ = other.host_addr_;
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObDbLinkSchema::operator==(const ObDbLinkSchema& other) const
{
  return (tenant_id_ == other.tenant_id_ && owner_id_ == other.owner_id_ && dblink_id_ == other.dblink_id_ &&
          schema_version_ == other.schema_version_ && dblink_name_ == other.dblink_name_ &&
          cluster_name_ == other.cluster_name_ && tenant_name_ == other.tenant_name_ &&
          user_name_ == other.user_name_ && password_ == other.password_ && host_addr_ == other.host_addr_);
}

ObSynonymInfo::ObSynonymInfo(common::ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}
ObSynonymInfo::ObSynonymInfo() : ObSchema()
{
  reset();
}

ObSynonymInfo::~ObSynonymInfo()
{}

ObSynonymInfo::ObSynonymInfo(const ObSynonymInfo& src_info) : ObSchema()
{
  reset();
  *this = src_info;
}

ObSynonymInfo& ObSynonymInfo::operator=(const ObSynonymInfo& src_info)
{
  if (this != &src_info) {
    reset();
    int ret = OB_SUCCESS;
    tenant_id_ = src_info.tenant_id_;
    database_id_ = src_info.database_id_;
    synonym_id_ = src_info.synonym_id_;
    schema_version_ = src_info.schema_version_;
    object_db_id_ = src_info.object_db_id_;
    if (OB_FAIL(deep_copy_str(src_info.name_, name_))) {
      LOG_WARN("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.object_name_, object_name_))) {
      LOG_WARN("Fail to deep object name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.version_, version_))) {
      LOG_WARN("Fail to deep copy version", K(ret));
    } else { /*do nothing*/
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
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
  ObSchema::reset();
  // allocator_.reset();
}

OB_SERIALIZE_MEMBER(BaselineKey, tenant_id_, db_id_, constructed_sql_, params_info_str_);

int64_t ObPlanBaselineInfo::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObPlanBaselineInfo);

  convert_size += key_.constructed_sql_.length() + 1;
  convert_size += key_.params_info_str_.length() + 1;
  convert_size += outline_data_.length() + 1;
  convert_size += sql_id_.length() + 1;
  convert_size += hints_info_.length() + 1;

  return convert_size;
}

void ObPlanBaselineInfo::reset()
{
  key_.reset();
  plan_baseline_id_ = OB_INVALID_ID;
  schema_version_ = 0;

  outline_data_.reset();
  sql_id_.reset();
  plan_hash_value_ = OB_INVALID_ID;
  fixed_ = false;
  enabled_ = true;
  executions_ = 0;
  cpu_time_ = 0;
  hints_info_.reset();
  hints_all_worked_ = true;
}

OB_DEF_SERIALIZE(ObPlanBaselineInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      key_,
      plan_baseline_id_,
      schema_version_,
      outline_data_,
      sql_id_,
      plan_hash_value_,
      fixed_,
      enabled_,
      executions_,
      cpu_time_,
      hints_info_,
      hints_all_worked_);

  return ret;
}

OB_DEF_DESERIALIZE(ObPlanBaselineInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      key_,
      plan_baseline_id_,
      schema_version_,
      outline_data_,
      sql_id_,
      plan_hash_value_,
      fixed_,
      enabled_,
      executions_,
      cpu_time_,
      hints_info_,
      hints_all_worked_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPlanBaselineInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      key_,
      plan_baseline_id_,
      schema_version_,
      outline_data_,
      sql_id_,
      plan_hash_value_,
      fixed_,
      enabled_,
      executions_,
      cpu_time_,
      hints_info_,
      hints_all_worked_);

  return len;
}

OB_DEF_SERIALIZE(ObSynonymInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      database_id_,
      synonym_id_,
      schema_version_,
      name_,
      version_,
      object_name_,
      object_db_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSynonymInfo)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObString object_name;
  ObString version;
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      database_id_,
      synonym_id_,
      schema_version_,
      name,
      version,
      object_name,
      object_db_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(name, name_))) {
    LOG_WARN("Fail to deep copy outline name", K(ret));
  } else if (OB_FAIL(deep_copy_str(object_name, object_name_))) {
    LOG_WARN("Fail to deep copy signature", K(ret));
  } else if (OB_FAIL(deep_copy_str(version, version_))) {
    LOG_WARN("Fail to deep copy outline_content", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSynonymInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      database_id_,
      synonym_id_,
      schema_version_,
      name_,
      version_,
      object_name_,
      object_db_id_);
  return len;
}

ObRecycleObject::ObRecycleObject(ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}

ObRecycleObject::ObRecycleObject(const ObRecycleObject& src) : ObSchema()
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

ObRecycleObject& ObRecycleObject::operator=(const ObRecycleObject& src)
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

ObRecycleObject::RecycleObjType ObRecycleObject::get_type_by_table_schema(const ObSimpleTableSchemaV2& table_schema)
{
  ObRecycleObject::RecycleObjType type = INVALID;
  if (table_schema.is_index_table()) {
    type = INDEX;
  } else if (table_schema.is_view_table()) {
    type = VIEW;
  } else if (table_schema.is_table() || table_schema.is_tmp_table()) {
    type = TABLE;
  } else {
    type = INVALID;
  }
  return type;
}

int ObRecycleObject::set_type_by_table_schema(const ObSimpleTableSchemaV2& table_schema)
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

OB_SERIALIZE_MEMBER(ObRecycleObject, tenant_id_, database_id_, table_id_, tablegroup_id_, object_name_, original_name_,
    type_, tablegroup_name_, database_name_);

//------end of funcs of outlineinfo-----//
int ObHostnameStuct::get_int_value(const common::ObString& str, int64_t& value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is empty", K(str), K(ret));
  } else {
    static const int32_t MAX_INT64_STORE_LEN = 31;
    char int_buf[MAX_INT64_STORE_LEN + 1];
    int64_t len = std::min(str.length(), MAX_INT64_STORE_LEN);
    MEMCPY(int_buf, str.ptr(), len);
    int_buf[len] = '\0';
    char* end_ptr = NULL;
    value = strtoll(int_buf, &end_ptr, 10);
    if (('\0' != *int_buf) && ('\0' == *end_ptr)) {
      // succ, do nothing
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int value", K(value), K(str), K(ret));
    }
  }
  return ret;
}

bool ObHostnameStuct::calc_ip(const common::ObString& host_ip, common::ObAddr& addr)
{
  return addr.set_ip_addr(host_ip, FAKE_PORT);
}

bool ObHostnameStuct::calc_ip_mask(const common::ObString& host_ip_mask, common::ObAddr& mask)
{
  bool ret_bool = false;
  int64_t ip_mask_int64 = 0;
  if (OB_UNLIKELY(host_ip_mask.empty())) {
  } else {
    if (host_ip_mask.find('.') || host_ip_mask.find(':')) {
      ret_bool = mask.set_ip_addr(host_ip_mask, FAKE_PORT);
    } else {
      if (OB_SUCCESS != (get_int_value(host_ip_mask, ip_mask_int64))) {
        // break
      } else if (OB_UNLIKELY(ip_mask_int64 > MAX_IP_BITS) || OB_UNLIKELY(ip_mask_int64 < 0)) {
        // break
      } else {
        mask.as_mask(ip_mask_int64);
        ret_bool = true;
      }
    }
  }
  return ret_bool;
}

bool ObHostnameStuct::is_ip_match(const common::ObString& client_ip, common::ObString host_name)
{
  bool ret_bool = false;
  bool is_ip_valied = false;
  common::ObAddr client, host, mask;
  if (OB_UNLIKELY(host_name.empty()) || OB_UNLIKELY(client_ip.empty())) {
    // not match
  } else if (host_name.find('/')) {
    common::ObString ip = host_name.split_on('/');
    if (calc_ip(ip, host) && calc_ip(client_ip, client) && calc_ip_mask(host_name, mask)) {
      is_ip_valied = true;
    }
  } else {
    mask.set_max();
    if (calc_ip(host_name, host) && calc_ip(client_ip, client)) {
      is_ip_valied = true;
    }
  }

  if (is_ip_valied) {
    ret_bool = (client.as_subnet(mask) == host);
  }
  return ret_bool;
}

bool ObHostnameStuct::is_wild_match(const common::ObString& client_ip, const common::ObString& host_name)
{
  return ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, client_ip, host_name, 0, '_', '%');
}

bool ObHostnameStuct::is_in_white_list(const common::ObString& client_ip, common::ObString& ip_white_list)
{
  bool ret_bool = false;
  if (ip_white_list.empty() || client_ip.empty()) {
    LOG_WARN("ip_white_list or client_ip is emtpy, denied any client", K(client_ip), K(ip_white_list));
  } else {
    const char COMMA = ',';
    ObString orig_ip_white_list = ip_white_list;
    while (NULL != ip_white_list.find(COMMA) && !ret_bool) {
      ObString invited_ip = ip_white_list.split_on(COMMA).trim();
      if (!invited_ip.empty()) {
        if (ObHostnameStuct::is_wild_match(client_ip, invited_ip) ||
            ObHostnameStuct::is_ip_match(client_ip, invited_ip)) {
          ret_bool = true;
        }
        LOG_DEBUG("match result", K(ret_bool), K(client_ip), K(invited_ip));
      }
    }
    if (!ret_bool) {
      if (ip_white_list.empty()) {
        LOG_WARN("ip_white_list is emtpy, denied any client", K(client_ip), K(orig_ip_white_list));
      } else if (!ObHostnameStuct::is_wild_match(client_ip, ip_white_list) &&
                 !ObHostnameStuct::is_ip_match(client_ip, ip_white_list)) {
        LOG_WARN("client ip is not in ip_white_list", K(client_ip), K(orig_ip_white_list));
      } else {
        ret_bool = true;
        LOG_DEBUG("match result", K(ret_bool), K(client_ip), K(ip_white_list));
      }
    }
  }
  return ret_bool;
}

OB_SERIALIZE_MEMBER(ObSequenceSchema, tenant_id_, database_id_, sequence_id_, schema_version_, name_, option_);

ObSequenceSchema::ObSequenceSchema() : ObSchema()
{
  reset();
}

ObSequenceSchema::ObSequenceSchema(ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}

ObSequenceSchema::ObSequenceSchema(const ObSequenceSchema& src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObSequenceSchema::~ObSequenceSchema()
{}

int ObSequenceSchema::assign(const ObSequenceSchema& src_schema)
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
    if (OB_FAIL(option_.assign(src_schema.option_))) {
      LOG_WARN("fail assign option", K(src_schema));
    } else if (OB_FAIL(set_sequence_name(src_schema.name_))) {
      LOG_WARN("fail set seq name", K(src_schema));
    }
  }
  return ret;
}

ObSequenceSchema& ObSequenceSchema::operator=(const ObSequenceSchema& src_schema)
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
  reset_string(name_);
  option_.reset();
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

OB_SERIALIZE_MEMBER(ObAuxTableMetaInfo, table_id_, table_type_, drop_schema_version_);

ObForeignKeyInfo::ObForeignKeyInfo(ObIAllocator* allocator)
    : table_id_(common::OB_INVALID_ID),
      foreign_key_id_(common::OB_INVALID_ID),
      child_table_id_(common::OB_INVALID_ID),
      parent_table_id_(common::OB_INVALID_ID),
      child_column_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      parent_column_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      update_action_(ACTION_INVALID),
      delete_action_(ACTION_INVALID),
      foreign_key_name_(),
      enable_flag_(true),
      is_modify_enable_flag_(false),
      validate_flag_(true),
      is_modify_validate_flag_(false),
      rely_flag_(false),
      is_modify_rely_flag_(false),
      is_modify_fk_state_(false),
      ref_cst_type_(CONSTRAINT_TYPE_INVALID),
      ref_cst_id_(common::OB_INVALID_ID)
{}

int ObForeignKeyInfo::assign(const ObForeignKeyInfo& other)
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
    foreign_key_name_ = other.foreign_key_name_;  // Shallow copy
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBasedSchemaObjectInfo, schema_id_, schema_type_, schema_version_);

const char* ObForeignKeyInfo::reference_action_str_[ACTION_MAX + 1] = {
    "", "RESTRICT", "CASCADE", "SET NULL", "NO ACTION", "SET DEFAULT", "ACTION_CHECK_EXIST", ""};

OB_SERIALIZE_MEMBER(ObForeignKeyInfo, table_id_, foreign_key_id_, child_table_id_, parent_table_id_, child_column_ids_,
    parent_column_ids_, update_action_, delete_action_, foreign_key_name_, ref_cst_type_, ref_cst_id_);

OB_SERIALIZE_MEMBER(ObSimpleForeignKeyInfo, tenant_id_, database_id_, table_id_, foreign_key_name_, foreign_key_id_);

OB_SERIALIZE_MEMBER(ObSimpleConstraintInfo, tenant_id_, database_id_, table_id_, constraint_name_, constraint_id_);
int ObCompareNameWithTenantID::compare(const common::ObString& str1, const common::ObString& str2)
{
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
  if (tenant_id_ != OB_INVALID_ID && database_id_ != OB_INVALID_ID &&
      combine_id(tenant_id_, OB_SYS_DATABASE_ID) == database_id_) {
    // If it is the oceanbase database, no matter what the tenant, I hope that it is not case sensitive
    cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (share::is_oracle_mode()) {
    cs_type = common::CS_TYPE_UTF8MB4_BIN;
  } else if (tenant_id_ == OB_INVALID_ID) {
    // Used for scenarios that do not require the tenant id to be case sensitive, such as column, only rely on
    // is_oracle_mode()
    /* ^-^ */
  } else {
    if (name_case_mode_ != OB_NAME_CASE_INVALID) {
      cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    }
    ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode);
    if (compat_mode == ObWorker::CompatMode::ORACLE) {
      cs_type = common::CS_TYPE_UTF8MB4_BIN;
    }
  }
  return common::ObCharset::strcmp(cs_type, str1, str2);
}

ObIAllocator*& schema_stack_allocator()
{
  static RLOCAL(ObIAllocator*, allocator_);
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

const char* get_ssl_spec_type_str(const ObSSLSpecifiedType ssl_spec_type)
{
  static const char* const const_str[] = {
      "CIPHER",
      "ISSUER",
      "SUBJECT",
      "MAX_TYPE",
  };
  return ((ssl_spec_type >= ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER &&
              ssl_spec_type <= ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)
              ? const_str[static_cast<int32_t>(ssl_spec_type)]
              : const_str[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)]);
}

ObSSLType get_ssl_type_from_string(const common::ObString& ssl_type_str)
{
  ObSSLType ssl_type_enum = ObSSLType::SSL_TYPE_MAX;
  if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_NOT_SPECIFIED)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_NONE)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_NONE;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_ANY)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_ANY;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_X509)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_X509;
  } else if (ssl_type_str == get_ssl_type_string(ObSSLType::SSL_TYPE_SPECIFIED)) {
    ssl_type_enum = ObSSLType::SSL_TYPE_SPECIFIED;
  } else {
    LOG_WARN("unknown ssl type", K(ssl_type_str), K(common::lbt()));
  }
  return ssl_type_enum;
}

// ObProfileSchema

ObProfileSchema::ObProfileSchema() : ObSchema()
{
  reset();
}

ObProfileSchema::ObProfileSchema(ObIAllocator* allocator)
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
{}

ObProfileSchema::ObProfileSchema(const ObProfileSchema& other) : ObSchema()
{
  *this = other;
}

ObProfileSchema::~ObProfileSchema()
{}

ObProfileSchema& ObProfileSchema::operator=(const ObProfileSchema& other)
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
  if (!ObSchema::is_valid() || !is_valid_tenant_id(tenant_id_) || !is_valid_id(profile_id_) || schema_version_ < 0 ||
      profile_name_.empty()) {
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

int ObProfileSchema::get_default_value(const int64_t type, int64_t& value)
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
    10,                   // times
    USECS_PER_DAY,        // 1 day
    -1,                   // nouse
    180 * USECS_PER_DAY,  // 180 day
    7 * USECS_PER_DAY,    // 7 day
};
const int64_t ObProfileSchema::INVALID_PARAM_VALUES[] = {
    -1,  // times
    -1,  // 1 day
    -1,  // nouse
    -1,  // 1 day
    -1,  // 1 day
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

OB_SERIALIZE_MEMBER(ObProfileSchema, tenant_id_, profile_id_, schema_version_, profile_name_, failed_login_attempts_,
    password_lock_time_, password_verify_function_, password_life_time_, password_grace_time_);

const char* OB_OBJECT_TYPE_STR[] = {"INVALID",
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
    "SYNONYM"};
static_assert(ARRAYSIZEOF(OB_OBJECT_TYPE_STR) == static_cast<int64_t>(ObObjectType::MAX_TYPE), "array size mismatch");

const char* ob_object_type_str(const ObObjectType object_type)
{
  const char* ret = "Unknown";
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

//
//
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
