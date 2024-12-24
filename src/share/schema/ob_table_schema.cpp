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
#include "ob_table_schema.h"
#include <algorithm>
//#include <stdlib.h>
#include "lib/objectpool/ob_pool.h"
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_storage_format.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_replica_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_primary_zone_util.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_encryption_util.h"
#include "storage/ob_storage_schema.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"
#include "share/schema/ob_part_mgr_util.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace blocksstable;

const static char * ObTableModeFlagStr[] = {
    "NORMAL",
    "QUEUING",
    "PRIMARY_AUX_VP",
    "MODERATE",
    "SUPER",
    "EXTREME",
};

const char *table_mode_flag_to_str(const ObTableModeFlag &table_mode)
{
  STATIC_ASSERT(static_cast<int64_t>(TABLE_MODE_MAX) == ARRAYSIZEOF(ObTableModeFlagStr), "table mode flag str len is mismatch");
  const char *str = "";
  if (is_valid_table_mode_flag(table_mode)) {
    str = ObTableModeFlagStr[table_mode];
  } else {
    str = "invalid_table_mode_flag_type";
  }
  return str;
}

ObColumnIdKey ObGetColumnKey<ObColumnIdKey, ObColumnSchemaV2 *>::operator()(const ObColumnSchemaV2 *column_schema) const
{
  return ObColumnIdKey(column_schema->get_column_id());
}

ObColumnSchemaHashWrapper ObGetColumnKey<ObColumnSchemaHashWrapper, ObColumnSchemaV2 *>::operator()(const ObColumnSchemaV2 *column_schema) const
{
  return ObColumnSchemaHashWrapper(column_schema->get_column_name_str());
}

ObColumnGroupIdKey ObGetColumnKey<ObColumnGroupIdKey, ObColumnGroupSchema *>::operator()(const ObColumnGroupSchema *column_group_schema) const
{
  return ObColumnGroupIdKey(column_group_schema->get_column_group_id());
}

ObColumnGroupSchemaHashWrapper ObGetColumnKey<ObColumnGroupSchemaHashWrapper, ObColumnGroupSchema *>::operator()(const ObColumnGroupSchema *column_group_schema) const
{
  return ObColumnGroupSchemaHashWrapper(column_group_schema->get_column_group_name());
}

int ObTableMode::assign(const ObTableMode &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input ObTableMode is invalid", K(ret), K(other));
  } else {
    mode_ = other.mode_;
  }
  return ret;
}

ObTableMode & ObTableMode::operator=(const ObTableMode &other)
{
  if (this != &other) {
    mode_ = other.mode_;
  }
  return *this;
}

bool ObTableMode::is_valid() const
{
  bool bret = false;
  if (mode_flag_ < TABLE_MODE_MAX && pk_mode_ < TPKM_MAX && state_flag_ < TABLE_STATE_MAX) {
    bret = true;
  }
  return bret;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObTableMode,
                           mode_);

bool ObMvMode::is_valid() const
{
  bool bret = true;
  // reserve for new flags
  return bret;
}

int ObMvMode::assign(const ObMvMode &other)
{
  int ret = OB_SUCCESS;
  mode_ = other.mode_;
  return ret;
}

ObMvMode & ObMvMode::operator=(const ObMvMode &other)
{
  if (this != &other) {
    mode_ = other.mode_;
  }
  return *this;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObMvMode,
                           mode_);

common::ObString ObMergeSchema::EMPTY_STRING = common::ObString::make_string("");

int ObMergeSchema::get_mulit_version_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_rowkey_column_ids(column_ids))) {
    SHARE_SCHEMA_LOG(WARN, "failed to add rowkey cols", K(ret));
  } else if (OB_FAIL(storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(column_ids))) {
    SHARE_SCHEMA_LOG(WARN, "failed to add extra rowkey cols", K(ret));
  } else if (OB_FAIL(set_precision_to_column_desc(column_ids))) {
    SHARE_SCHEMA_LOG(WARN, "failed to set precision to cols", K(ret));
  }
  return ret;
}
ObSimpleTableSchemaV2::ObSimpleTableSchemaV2()
  : ObPartitionSchema()
{
  reset();
}

ObSimpleTableSchemaV2::ObSimpleTableSchemaV2(ObIAllocator *allocator)
    : ObPartitionSchema(allocator),
      simple_foreign_key_info_array_(SCHEMA_MID_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      simple_constraint_info_array_(SCHEMA_MID_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator))
{
  reset();
}

ObSimpleTableSchemaV2::~ObSimpleTableSchemaV2()
{
}

ObObjectID ObSimpleTableSchemaV2::get_object_id() const
{
  return static_cast<ObObjectID>(get_table_id());
}

int ObSimpleTableSchemaV2::assign(const ObSimpleTableSchemaV2 &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    ObSimpleTableSchemaV2::reset();
    ObPartitionSchema::operator=(other);
    if (OB_SUCCESS == error_ret_) {
      tenant_id_ = other.tenant_id_;
      table_id_ = other.table_id_;
      association_table_id_ = other.association_table_id_;
      tablet_id_ = other.get_tablet_id();
      schema_version_ = other.schema_version_;
      max_dependency_version_ = other.max_dependency_version_;
      database_id_ = other.database_id_;
      tablegroup_id_ = other.tablegroup_id_;
      data_table_id_ = other.data_table_id_;
      table_type_ = other.table_type_;
      name_case_mode_ = other.name_case_mode_;
      index_status_ = other.index_status_;
      index_type_ = other.index_type_;
      partition_status_ = other.partition_status_;
      partition_schema_version_ = other.partition_schema_version_;
      session_id_ = other.session_id_;
      duplicate_scope_ = other.duplicate_scope_;
      duplicate_read_consistency_ = other.duplicate_read_consistency_;
      tablespace_id_ = other.tablespace_id_;
      master_key_id_ = other.master_key_id_;
      dblink_id_ = other.dblink_id_;
      link_table_id_ = other.link_table_id_;
      link_schema_version_ = other.link_schema_version_;
      in_offline_ddl_white_list_ = other.in_offline_ddl_white_list_;
      object_status_ = other.object_status_;
      is_force_view_ = other.is_force_view_;
      truncate_version_ = other.truncate_version_;
      if (OB_FAIL(table_mode_.assign(other.table_mode_))) {
        LOG_WARN("Fail to assign table mode", K(ret), K(other.table_mode_));
      } else if (OB_FAIL(deep_copy_str(other.table_name_, table_name_))) {
        LOG_WARN("Fail to deep copy table_name", K(ret));
      } else if (OB_FAIL(set_simple_foreign_key_info_array(other.simple_foreign_key_info_array_))) {
        LOG_WARN("fail to set simple foreign key info array", K(ret));
      } else if (OB_FAIL(set_simple_constraint_info_array(other.simple_constraint_info_array_))) {
        LOG_WARN("fail to set simple constraint info array", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.origin_index_name_, origin_index_name_))) {
        LOG_WARN("Fail to deep copy primary_zone", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.encryption_, encryption_))) {
        LOG_WARN("fail to deep copy encrypt str", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.encrypt_key_, encrypt_key_))) {
        LOG_WARN("fail to deep copy encrypt str", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.link_database_name_, link_database_name_))) {
        LOG_WARN("Fail to deep copy database_name", K(ret));
      }
    } else {
      ret = error_ret_;
      LOG_WARN("failed to assign ObPartitionSchema", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to assign simple table schema", K(ret));
  }
  return ret;
}

bool ObSimpleTableSchemaV2::operator ==(const ObSimpleTableSchemaV2 &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_ &&
     table_id_ == other.table_id_ &&
     association_table_id_ == other.association_table_id_ &&
     tablet_id_ == other.get_tablet_id() &&
     schema_version_ == other.schema_version_ &&
     max_dependency_version_ == other.max_dependency_version_ &&
     database_id_ == other.database_id_ &&
     tablegroup_id_ == other.tablegroup_id_ &&
     data_table_id_ == other.data_table_id_ &&
     table_name_ == other.table_name_ &&
     name_case_mode_ == other.name_case_mode_ &&
     table_type_ == other.table_type_ &&
     part_level_ == other.part_level_ &&
     index_status_ == other.index_status_ &&
     index_type_ == other.index_type_ &&
     partition_status_ == other.partition_status_ &&
     partition_schema_version_ == other.partition_schema_version_ &&
     session_id_ == other.session_id_ &&
     origin_index_name_ == other.origin_index_name_ &&
     table_mode_ == other.table_mode_ &&
     encryption_ == other.encryption_ &&
     tablespace_id_ == other.tablespace_id_ &&
     encrypt_key_ == other.encrypt_key_ &&
     master_key_id_ == other.master_key_id_ &&
     dblink_id_ == other.dblink_id_ &&
     link_table_id_ == other.link_table_id_ &&
     link_schema_version_ == other.link_schema_version_ &&
     link_database_name_ == other.link_database_name_ &&
     object_status_ == other.object_status_ &&
     truncate_version_ == other.truncate_version_) {
     ret = true;
     if (true == ret) {
       if (simple_foreign_key_info_array_.count() == other.simple_foreign_key_info_array_.count()) {
         for (int64_t i = 0; ret && i < simple_foreign_key_info_array_.count(); ++i) {
           const ObSimpleForeignKeyInfo & fk_info = simple_foreign_key_info_array_.at(i);
           if (!has_exist_in_array(other.simple_foreign_key_info_array_, fk_info)) {
             ret = false;
           } else {} // go on next
         }
       } else if (simple_constraint_info_array_.count() == other.simple_constraint_info_array_.count()) {
         for (int64_t i = 0; ret && i < simple_constraint_info_array_.count(); ++i) {
           const ObSimpleConstraintInfo & cst_info = simple_constraint_info_array_.at(i);
           if (!has_exist_in_array(other.simple_constraint_info_array_, cst_info)) {
             ret = false;
           } else {} // go on next
         }
       } else {
         ret = false;
       }
     }
   }

   return ret;
}

void ObSimpleTableSchemaV2::reset_partition_schema()
{
  // Note: Do not directly call the reset of the base class
  // Here will reset the allocate in ObSchema, which will affect other variables
  // very dangerous
  reuse_partition_schema();
}

void ObSimpleTableSchemaV2::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  association_table_id_ = OB_INVALID_ID;
  tablet_id_.reset();
  schema_version_ = 0;
  max_dependency_version_ = OB_INVALID_VERSION;
  database_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  data_table_id_ = 0;
  table_name_.reset();
  origin_index_name_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
  table_type_ = USER_TABLE;
  table_mode_.reset();
  index_status_ = INDEX_STATUS_UNAVAILABLE;
  partition_status_ = PARTITION_STATUS_ACTIVE;
  index_type_ =  INDEX_TYPE_IS_NOT;
  session_id_ = 0;
  in_offline_ddl_white_list_ = false;
  object_status_ = ObObjectStatus::VALID;
  is_force_view_ = false;
  for (int64_t i = 0; i < simple_foreign_key_info_array_.count(); ++i) {
    free(simple_foreign_key_info_array_.at(i).foreign_key_name_.ptr());
  }
  for (int64_t i = 0; i < simple_constraint_info_array_.count(); ++i) {
    free(simple_constraint_info_array_.at(i).constraint_name_.ptr());
  }
  simple_foreign_key_info_array_.reset();
  dblink_id_ = OB_INVALID_ID;
  link_table_id_ = OB_INVALID_ID;
  link_schema_version_ = OB_INVALID_ID;
  link_database_name_.reset();
  duplicate_scope_ = ObDuplicateScope::DUPLICATE_SCOPE_NONE;
  duplicate_read_consistency_ = ObDuplicateReadConsistency::STRONG;
  simple_constraint_info_array_.reset();
  encryption_.reset();
  tablespace_id_ = OB_INVALID_ID;
  encrypt_key_.reset();
  master_key_id_ = OB_INVALID_ID;
  truncate_version_ = OB_INVALID_VERSION;
  ObPartitionSchema::reset();
}

bool ObSimpleTableSchemaV2::has_tablet() const
{
  return !(is_vir_table()
           || is_view_table()
           || is_aux_vp_table()
           || is_virtual_table(get_table_id()) // virtual table index
           || is_external_table()
           );
}

ObPartitionLevel ObSimpleTableSchemaV2::get_part_level() const
{
  ObPartitionLevel part_level = part_level_;
  if (PARTITION_LEVEL_ONE == part_level_
      && 1 == part_option_.get_part_num()
      && PARTITION_FUNC_TYPE_HASH == part_option_.get_part_func_type()
      && part_option_.get_part_func_expr_str().empty()) {
    part_level = PARTITION_LEVEL_ZERO;
  } else { }//do nothing

  return part_level;
}

int ObSimpleTableSchemaV2::get_zone_list(
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  zone_list.reset();
  const ObTenantSchema *tenant_schema = NULL;
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

int ObSimpleTableSchemaV2::get_first_primary_zone_inherit(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
    common::ObZone &first_primary_zone) const
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObSimpleTableSchemaV2::get_paxos_replica_num(
    share::schema::ObSchemaGetterGuard &guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret) {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_paxos_replica_num();
      }
    }
  }
  return ret;
}

/*
 * 1. Indexes, virtual tables, and other table schemas without partitions are not filled
 * 2. The locality field is not empty, directly fill it with zone_replica_attr_array of this table
 * 3. The all_dummy table of user tenants has a fully functional copy in each zone.
 * 4. The locality field is empty. If there is a tablegroup, it is filled with the zone_replica_attr_array of
 *  the corresponding tablegroup, otherwise it is filled with the zone_replica_attr_array of the corresponding tenant
 */
int ObSimpleTableSchemaV2::get_zone_replica_attr_array_inherit(
    ObSchemaGetterGuard &schema_guard,
    ZoneLocalityIArray &locality) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  locality.reuse();

  // Locality is not set when creating table, take tenant's fill
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(table_id_), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(table_id_), K(tenant_id), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array_inherit(schema_guard, locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret), K(table_id_), K(tenant_id));
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_primary_zone_inherit(
    ObSchemaGetterGuard &schema_guard,
    ObPrimaryZone &primary_zone) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  primary_zone.reset();
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema null", K(ret), K(tenant_id), KP(tenant_schema));
  } else if (OB_FAIL(tenant_schema->get_primary_zone_inherit(schema_guard, primary_zone))) {
    LOG_WARN("fail to get primary zone array", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSimpleTableSchemaV2::set_simple_foreign_key_info_array(const common::ObIArray<ObSimpleForeignKeyInfo> &simple_fk_info_array)
{
  int ret = OB_SUCCESS;

  simple_foreign_key_info_array_.reset();
  int64_t count = simple_fk_info_array.count();
  if (OB_FAIL(simple_foreign_key_info_array_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  FOREACH_CNT_X(simple_fk_info_iter, simple_fk_info_array, OB_SUCC(ret)) {
    ret = add_simple_foreign_key_info(simple_fk_info_iter->tenant_id_,
                                      simple_fk_info_iter->database_id_,
                                      simple_fk_info_iter->table_id_,
                                      simple_fk_info_iter->foreign_key_id_,
                                      simple_fk_info_iter->foreign_key_name_);
  }

  return ret;
}

int ObSimpleTableSchemaV2::add_simple_foreign_key_info(const uint64_t tenant_id,
                                                       const uint64_t database_id,
                                                       const uint64_t table_id,
                                                       const int64_t foreign_key_id,
                                                       const ObString &foreign_key_name)
{
  int ret = OB_SUCCESS;
  ObSimpleForeignKeyInfo simple_fk_info(tenant_id,
                                        database_id,
                                        table_id,
                                        foreign_key_name,
                                        foreign_key_id);
  if (!foreign_key_name.empty()
      && OB_FAIL(deep_copy_str(foreign_key_name, simple_fk_info.foreign_key_name_))) {
    LOG_WARN("failed to deep copy foreign key name", KR(ret), K(foreign_key_name));
  } else if(OB_FAIL(simple_foreign_key_info_array_.push_back(simple_fk_info))) {
    LOG_WARN("failed to push back simple foreign key info", KR(ret), K(simple_fk_info));
  }

  return ret;
}

int ObSimpleTableSchemaV2::set_simple_constraint_info_array(const common::ObIArray<ObSimpleConstraintInfo> &simple_cst_info_array)
{
  int ret = OB_SUCCESS;

  simple_constraint_info_array_.reset();
  int64_t count = simple_cst_info_array.count();
  if (OB_FAIL(simple_constraint_info_array_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  FOREACH_CNT_X(simple_cst_info_iter, simple_cst_info_array, OB_SUCC(ret)) {
    ret = add_simple_constraint_info(simple_cst_info_iter->tenant_id_,
                                     simple_cst_info_iter->database_id_,
                                     simple_cst_info_iter->table_id_,
                                     simple_cst_info_iter->constraint_id_,
                                     simple_cst_info_iter->constraint_name_);
  }

  return ret;
}

int ObSimpleTableSchemaV2::add_simple_constraint_info(const uint64_t tenant_id,
                                                      const uint64_t database_id,
                                                      const uint64_t table_id,
                                                      const int64_t constraint_id,
                                                      const common::ObString &constraint_name)
{
  int ret = OB_SUCCESS;
  ObSimpleConstraintInfo simple_cst_info(tenant_id,
                                         database_id,
                                         table_id,
                                         constraint_name,
                                         constraint_id);
  if (!constraint_name.empty()
      && OB_FAIL(deep_copy_str(constraint_name, simple_cst_info.constraint_name_))) {
    LOG_WARN("failed to deep copy constraint name", KR(ret), K(constraint_name));
  } else if(OB_FAIL(simple_constraint_info_array_.push_back(simple_cst_info))) {
    LOG_WARN("failed to push back simple constraint info", KR(ret), K(simple_cst_info));
  }

  return ret;
}

bool ObSimpleTableSchemaV2::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()) {
    ret = false;
    LOG_WARN("ob_schema is unvalid", K(ret));
  }
  if (ret) {
    if (OB_INVALID_ID == tenant_id_ ||
        OB_INVALID_ID == table_id_ ||
        schema_version_ < 0 ||
        OB_INVALID_ID == database_id_ ||
        table_name_.empty()) {
      if (!is_link_valid()) {
        ret = false;
        LOG_WARN("invalid argument",
                 K(tenant_id_), K(table_id_), K(schema_version_), K(database_id_), K(table_name_),
                 K(dblink_id_), K(link_table_id_), K(link_schema_version_), K(link_database_name_));
      }
    } else if (is_index_table()
        || is_aux_vp_table()
        || is_aux_lob_table()
        || is_mlog_table()) {
      if (OB_INVALID_ID == data_table_id_) {
        ret = false;
        LOG_WARN("invalid data table_id", K(ret), K(data_table_id_));
      } else if (is_index_table() && !is_normal_index() && !is_unique_index()
          && !is_domain_index() && !is_vec_index() && !is_fts_index() && !is_multivalue_index()) {
        ret = false;
        LOG_WARN("table_type is not consistent with index_type",
            "table_type", static_cast<int64_t>(table_type_),
            "index_type", static_cast<int64_t>(index_type_));
      }
    } else if (!is_index_table() && (INDEX_TYPE_IS_NOT != index_type_)) {
      ret = false;
      LOG_WARN("table_type is not consistent with index_type",
          "table_type", static_cast<int64_t>(table_type_),
          "index_type", static_cast<int64_t>(index_type_));
    }
  }
  return ret;
}

bool ObSimpleTableSchemaV2::is_link_valid() const
{
  return (OB_INVALID_ID != dblink_id_ &&
          OB_INVALID_ID != link_table_id_ &&
          !link_database_name_.empty() &&
          !table_name_.empty());
}

int64_t ObSimpleTableSchemaV2::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleTableSchemaV2);
  convert_size += table_name_.length() + 1;

  convert_size += part_option_.get_convert_size() - sizeof(part_option_);
  convert_size += sub_part_option_.get_convert_size() - sizeof(sub_part_option_);

  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  partition_array_, partition_num_);
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  def_subpartition_array_, def_subpartition_num_);
  convert_size += ObSchemaUtils::get_partition_array_convert_size(
                  hidden_partition_array_, hidden_partition_num_);
  convert_size += simple_foreign_key_info_array_.get_data_size();
  for (int64_t i = 0; i < simple_foreign_key_info_array_.count(); ++i) {
    convert_size += simple_foreign_key_info_array_.at(i).get_convert_size();
  }
  convert_size += simple_constraint_info_array_.get_data_size();
  for (int64_t i = 0; i < simple_constraint_info_array_.count(); ++i) {
    convert_size += simple_constraint_info_array_.at(i).get_convert_size();
  }
  convert_size += origin_index_name_.length() + 1;
  convert_size += encryption_.length() + 1;
  convert_size += encrypt_key_.length() + 1;
  convert_size += link_database_name_.length() + 1;
  convert_size += transition_point_.get_deep_copy_size();
  convert_size += interval_range_.get_deep_copy_size();
  return convert_size;
}

int ObSimpleTableSchemaV2::get_encryption_id(int64_t &encrypt_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObEncryptionUtil::parse_encryption_id(encryption_, encrypt_id))) {
    LOG_WARN("failed to parse_encrytion_id", K(ret), K(encryption_));
  }
  return ret;
}

bool ObSimpleTableSchemaV2::need_encrypt() const {
  bool ret = false;
  if (0 != encryption_.length() && 0 != encryption_.case_compare("none")) {
    ret = true;
  }
  return ret;
}

bool ObSimpleTableSchemaV2::is_equal_encryption(const ObSimpleTableSchemaV2 &t) const
{
  bool res = false;
  if ((0 == get_encryption_str().case_compare(t.get_encryption_str())) ||
      (0 == get_encryption_str().length() && 0 == t.get_encryption_str().case_compare("none")) ||
      (0 == t.get_encryption_str().length() && 0 == get_encryption_str().case_compare("none"))) {
    res = true;
  }
  return res;
}

int ObSimpleTableSchemaV2::set_specific_replica_attr_array(
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

int ObSimpleTableSchemaV2::get_full_replica_num(
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

int ObSimpleTableSchemaV2::get_all_replica_num(
    share::schema::ObSchemaGetterGuard &guard,
    int64_t &num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet &set = zone_locality.at(i);
      num += set.get_specific_replica_num();
    }
  }
  return ret;
}


int ObSimpleTableSchemaV2::check_has_all_server_readonly_replica(
    share::schema::ObSchemaGetterGuard &guard,
    bool &has) const
{
  int ret = OB_SUCCESS;
  has = false;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count() && !has; ++i) {
      has = OB_ALL_SERVER_CNT == zone_locality.at(i).get_readonly_replica_num();
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_is_readonly_at_all(
    share::schema::ObSchemaGetterGuard &guard,
    const common::ObZone &zone,
    const common::ObRegion &region,
    bool &readonly_at_all) const
{
  UNUSED(region);
  int ret = OB_SUCCESS;
  readonly_at_all = false;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
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

int ObSimpleTableSchemaV2::check_is_all_server_readonly_replica(
    share::schema::ObSchemaGetterGuard &guard,
    bool &is) const
{
  int ret = OB_SUCCESS;
  is = true;
  common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
  if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else {
    for (int64_t i = 0; i < zone_locality.count() && is; ++i) {
      is = OB_ALL_SERVER_CNT == zone_locality.at(i).get_readonly_replica_num();
    }
  }
  return ret;
}

#define ASSIGN_COMPARE_PARTITION_ERROR(ERROR_STRING, USER_ERROR) { \
  if (OB_SUCC(ret)) { \
    if (OB_NOT_NULL(ERROR_STRING)) { \
      if (OB_FAIL(ERROR_STRING->assign(USER_ERROR))) { \
        LOG_WARN("fail to assign user error", KR(ret));\
      }\
    }\
  }\
}\
// compare two table partition details
int ObSimpleTableSchemaV2::compare_partition_option(const schema::ObSimpleTableSchemaV2 &t1,
                                                    const schema::ObSimpleTableSchemaV2 &t2,
                                                    bool check_subpart,
                                                    bool &is_matched,
                                                    ObSqlString *user_error)
{
  int ret = OB_SUCCESS;
  bool t1_oracle_mode = false;
  bool t2_oracle_mode = false;
  is_matched = true;
  if (OB_FAIL(t1.check_if_oracle_compat_mode(t1_oracle_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(t1));
  } else if (OB_FAIL(t2.check_if_oracle_compat_mode(t2_oracle_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(t2));
  } else if (t1_oracle_mode != t2_oracle_mode) {
    is_matched = false;
    ASSIGN_COMPARE_PARTITION_ERROR(user_error, "table compatibilty mode not match")
  } else {
    const schema::ObPartitionOption &t1_part = t1.get_part_option();
    const schema::ObPartitionOption &t2_part = t2.get_part_option();
    schema::ObPartitionFuncType t1_part_func_type = t1_part.get_part_func_type();
    schema::ObPartitionFuncType t2_part_func_type = t2_part.get_part_func_type();

    //non-partitioned table do not need to compare with partitioned table
    if ((PARTITION_LEVEL_ZERO == t1.get_part_level() && PARTITION_LEVEL_ZERO != t2.get_part_level())
        || (PARTITION_LEVEL_ZERO != t1.get_part_level() && PARTITION_LEVEL_ZERO == t2.get_part_level())) {
      is_matched = false;
      LOG_WARN("not all tables are non-partitioned or partitioned", K(t1.get_part_level()), K(t2.get_part_level()));
      ASSIGN_COMPARE_PARTITION_ERROR(user_error, "not all tables are non-partitioned or partitioned");
    } else if (PARTITION_LEVEL_ZERO == t1.get_part_level()
              && PARTITION_LEVEL_ZERO == t2.get_part_level()) {
      //both non-partition table is matched
    } else if (t1_part_func_type != t2_part_func_type && (!::oceanbase::is_key_part(t1_part_func_type) || !::oceanbase::is_key_part(t2_part_func_type))) {
      is_matched = false;
      LOG_WARN("partition func type not matched", K(t1_part), K(t2_part));
      ASSIGN_COMPARE_PARTITION_ERROR(user_error, "partition func type not matched");
    } else if (schema::PARTITION_FUNC_TYPE_MAX == t1_part_func_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part_func_type", KR(ret), K(t1_part_func_type), K(t2_part_func_type));
    } else if (t1.get_partition_num() != t1_part.get_part_num()
            || t2.get_partition_num() != t2_part.get_part_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition num is not equal to part num", KR(ret), K(t1.get_partition_num()), K(t1_part.get_part_num()),
                                                               K(t2.get_partition_num()), K(t2_part.get_part_num()));
    } else if (t1.get_partition_num() != t2.get_partition_num()) {
      is_matched = false;
      LOG_WARN("partition num is not equal", K(t1.get_partition_num()), K(t2.get_partition_num()));
      ASSIGN_COMPARE_PARTITION_ERROR(user_error, "partition num not equal");
    } else if (::oceanbase::is_hash_part(t1_part_func_type)
              || ::oceanbase::is_key_part(t1_part_func_type)) {
      //level one is hash and key, just need to compare part num and part type
      //do nothing
    } else if (::oceanbase::is_range_part(t1_part_func_type)
            || ::oceanbase::is_list_part(t1_part_func_type)) {
      if (OB_ISNULL(t1.get_part_array())
          || OB_ISNULL(t2.get_part_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition_array is null", KR(ret), K(t1), K(t2));
      } else {
        int64_t t1_part_num = t1.get_partition_num();
        for (int64_t i = 0; i < t1_part_num && is_matched && OB_SUCC(ret); i++) {
          is_matched = false;
          schema::ObPartition *table_part1 = t1.get_part_array()[i];
          schema::ObPartition *table_part2 = t2.get_part_array()[i];
          if (OB_ISNULL(table_part1) || OB_ISNULL(table_part2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition is null", KR(ret), KP(table_part1), KP(table_part2));
          } else if (OB_FAIL(schema::ObPartitionUtils::check_partition_value(
                        t1_oracle_mode, *table_part1, *table_part2, t1_part_func_type, is_matched, user_error))) {
            LOG_WARN("fail to check partition value", KR(ret), KPC(table_part1), KPC(table_part2), K(t1_part_func_type));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid part func type", KR(ret), K(t1_part), K(t2_part));
    }
    if (OB_SUCC(ret) && is_matched && check_subpart) {
      if (t1.get_part_level() != t2.get_part_level()) {
        is_matched = false;
        LOG_WARN("two table part level is not equal");
        ASSIGN_COMPARE_PARTITION_ERROR(user_error, "part level is not equal");
      } else if (PARTITION_LEVEL_TWO != t1.get_part_level()) {
        //don't have sub part, just skip
      } else {
        const schema::ObPartitionOption &t1_subpart = t1.get_sub_part_option();
        const schema::ObPartitionOption &t2_subpart = t2.get_sub_part_option();
        schema::ObPartitionFuncType t1_subpart_func_type = t1_subpart.get_part_func_type();
        schema::ObPartitionFuncType t2_subpart_func_type = t2_subpart.get_part_func_type();
        if (t1_subpart_func_type != t2_subpart_func_type
          && (!::oceanbase::is_key_part(t1_subpart_func_type) || !::oceanbase::is_key_part(t2_subpart_func_type))) {
          is_matched = false;
          LOG_WARN("subpartition func type not matched", K(t1_subpart), K(t2_subpart));
          ASSIGN_COMPARE_PARTITION_ERROR(user_error, "subpartition func type not matched");
        } else if (schema::PARTITION_FUNC_TYPE_MAX == t1_subpart_func_type) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid part_func_type", KR(ret), K(t1_subpart_func_type), K(t2_subpart_func_type));
        } else {
          const int64_t t1_level_one_part_num = t1.get_partition_num();
          for (int64_t i = 0; OB_SUCC(ret) && i < t1_level_one_part_num && is_matched; i++) {
            schema::ObPartition *table_part1 = t1.get_part_array()[i];
            schema::ObPartition *table_part2 = t2.get_part_array()[i];
            if (OB_ISNULL(table_part1) || OB_ISNULL(table_part2)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("partition is null", KR(ret), KP(table_part1), KP(table_part2));
            } else if (table_part1->get_subpartition_num() != table_part1->get_sub_part_num()
                    || table_part2->get_subpartition_num() != table_part2->get_sub_part_num()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("subpartition num not equal", KR(ret), K(table_part1->get_subpartition_num()), K(table_part1->get_sub_part_num()),
                                                           K(table_part2->get_subpartition_num()), K(table_part2->get_sub_part_num()));
            } else if (table_part1->get_subpartition_num() != table_part2->get_subpartition_num()) {
              is_matched = false;
              LOG_WARN("subpartition num is not equal", K(table_part1->get_subpartition_num()), K(table_part2->get_subpartition_num()));
              ASSIGN_COMPARE_PARTITION_ERROR(user_error, "subpartition num not matched");
            } else if (::oceanbase::is_hash_part(t1_subpart_func_type)
                     || ::oceanbase::is_key_part(t1_subpart_func_type)) {
              //level two is hash and key, just need to compare part num and part type
              //do nothing
            } else if (::oceanbase::is_range_part(t1_subpart_func_type)
                    || ::oceanbase::is_list_part(t1_subpart_func_type)) {
              const int64_t t1_level_two_part_num = table_part1->get_subpartition_num();
              for (int64_t j = 0; OB_SUCC(ret) && j < t1_level_two_part_num && is_matched; j++) {
                is_matched = false;
                schema::ObSubPartition *table_subpart1 = table_part1->get_subpart_array()[j];
                schema::ObSubPartition *table_subpart2 = table_part2->get_subpart_array()[j];
                if (OB_ISNULL(table_subpart1) || OB_ISNULL(table_subpart2)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("subpartition is null", KR(ret), KP(table_subpart1), KP(table_subpart2));
                } else if (OB_FAIL(schema::ObPartitionUtils::check_partition_value(
                            t1_oracle_mode, *table_subpart1, *table_subpart2, t1_subpart_func_type, is_matched, user_error))) {
                  LOG_WARN("fail to check subpartition value", KR(ret), KPC(table_subpart1), KPC(table_subpart1), K(t1_subpart_func_type));
                }
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("invalid subpart func type", KR(ret), K(t1_subpart), K(t2_subpart));
            }
          }
        }
      }
    }
  }
  return ret;
}

int64_t ObSimpleTableSchemaV2::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
      K_(database_id),
      K_(tablegroup_id),
      K_(table_id),
      K_(association_table_id),
      K_(in_offline_ddl_white_list),
      K_(table_name),
      K_(session_id),
      "index_type", static_cast<int32_t>(index_type_),
      "table_type", static_cast<int32_t>(table_type_),
      K_(table_mode),
      K_(tablespace_id));
  J_COMMA();
  J_KV(K_(data_table_id),
    "name_casemode", static_cast<int32_t>(name_case_mode_),
    K_(schema_version),
    K_(part_level),
    K_(part_option),
    K_(sub_part_option),
    K_(partition_num),
    K_(def_subpartition_num),
    "partition_array", ObArrayWrap<ObPartition *>(partition_array_, partition_num_),
    K_(partition_array_capacity),
    "def_subpartition_array", ObArrayWrap<ObSubPartition *>(def_subpartition_array_, def_subpartition_num_),
    "hidden_partition_array",
    ObArrayWrap<ObPartition *>(hidden_partition_array_, hidden_partition_num_),
    K_(index_status),
    K_(duplicate_scope),
    K_(encryption),
    K_(encrypt_key),
    K_(master_key_id),
    K_(sub_part_template_flags),
    K(get_tablet_id()),
    K_(max_dependency_version),
    K_(object_status),
    K_(is_force_view),
    K_(truncate_version),
    K_(duplicate_read_consistency)
);
  J_OBJ_END();

  return pos;
}

bool ObSimpleTableSchemaV2::is_user_partition_table() const
{
  bool bret = false;
  if (!common::is_inner_table(get_table_id())) {
    if (is_partitioned_table() &&
        !is_view_table()) {
      bret = true;
    }
  }
  return bret;
}

bool ObSimpleTableSchemaV2::is_user_subpartition_table() const
{
  bool bret = false;
  if (!common::is_inner_table(get_table_id())) {
    if (PARTITION_LEVEL_TWO == get_part_level() &&
        !is_view_table()) {
     bret = true;
    }
  }
  return bret;
}

int ObSimpleTableSchemaV2::get_locality_str_inherit(
    share::schema::ObSchemaGetterGuard &guard,
    const common::ObString *&locality_str) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  locality_str = NULL;
  if (OB_FAIL(ret)) {
  } else {
    const ObSimpleTenantSchema *tenant = NULL;
    if (OB_FAIL(guard.get_tenant_info(get_tenant_id(), tenant))) {
      LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
    } else if (OB_ISNULL(tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
    } else {
      locality_str = &tenant->get_locality_str();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(locality_str) || locality_str->empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("locality_str should not be null or empty", K(ret), K(*this));
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_tablet_ids(common::ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = get_part_level();
  if (part_level >= PARTITION_LEVEL_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part level is unexpected", KPC(this), KR(ret));
  } else if (OB_UNLIKELY(!has_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("must be user table", KPC(this), KR(ret));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    if (OB_FAIL(tablet_ids.push_back(get_tablet_id()))) {
      LOG_WARN("fail to push_back", KR(ret), KPC(this));
    }
  } else {
    if (OB_ISNULL(partition_array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", KPC(this), KR(ret));
    } else if (part_option_.get_part_num() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_num less than 1", KPC(this), KR(ret));
    } else {
      for (int64_t i = 0; i < part_option_.get_part_num() && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(partition_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(i), KPC(this), KR(ret));
        } else if (PARTITION_LEVEL_ONE == part_level) {
          if (OB_FAIL(tablet_ids.push_back(partition_array_[i]->get_tablet_id()))) {
            LOG_WARN("fail to push_back", KR(ret), K(i), KPC(this));
          }
        } else if (PARTITION_LEVEL_TWO == part_level) {
          ObSubPartition **subpart_array = partition_array_[i]->get_subpart_array();
          int64_t subpart_num = partition_array_[i]->get_subpartition_num();
          if (OB_ISNULL(subpart_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part array is null", KPC(this), KR(ret));
          } else if (subpart_num < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub_part_num less than 1", KPC(this), KR(ret));
          } else {
            for (int64_t j = 0; j < subpart_num && OB_SUCC(ret); j++) {
              if (OB_ISNULL(subpart_array[j])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("NULL ptr", K(j), KPC(this), KR(ret));
              } else {
                if (OB_FAIL(tablet_ids.push_back(subpart_array[j]->get_tablet_id()))) {
                  LOG_WARN("fail to push_back", KR(ret), K(j), KPC(this));
                }
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("4.0 not support part type", KPC(this), KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_part_idx_by_tablet(const ObTabletID &tablet_id, int64_t &part_idx, int64_t &subpart_idx) const
{
  int ret = OB_SUCCESS;
  part_idx = OB_INVALID_INDEX;
  subpart_idx = OB_INVALID_INDEX;
  if (common::ObTabletID::INVALID_TABLET_ID == tablet_id.id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_id is invalid", KPC(this), KR(ret));
  } else if (part_level_ >= PARTITION_LEVEL_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part type", KR(ret), KPC(this));
  } else if (!has_tablet()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("There are no tablets in virtual table and view", KR(ret), KPC(this));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("nonpart table", KR(ret), KPC(this));
  } else {
    ObPartition **part_array = get_part_array();
    int64_t part_num = get_partition_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", KPC(this), KR(ret));
    } else {
      bool found = false;
      for (int64_t i = 0; i < part_num && !found && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(i), KPC(this), KR(ret));
        } else if (PARTITION_LEVEL_ONE == part_level_) {
          if (part_array[i]->get_tablet_id() == tablet_id) {
            part_idx = i;
            found = true;
          }
        } else if (PARTITION_LEVEL_TWO == part_level_) {
          ObSubPartition **subpart_array = part_array[i]->get_subpart_array();
          int64_t subpart_num = part_array[i]->get_subpartition_num();
          if (OB_ISNULL(subpart_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpart array is null", KPC(this), KR(ret));
          } else {
            for (int64_t j = 0; j < subpart_num && !found && OB_SUCC(ret); ++j) {
              if (OB_ISNULL(subpart_array[j])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("NULL ptr", KPC(this), KR(ret));
              } else if (subpart_array[j]->get_tablet_id() == tablet_id) {
                part_idx = i;
                subpart_idx = j;
                found = true;
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("4.0 not support part type", KR(ret), KPC(this));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!found) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("part is not exist", KPC(this), KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    part_idx = OB_INVALID_INDEX;
    subpart_idx = OB_INVALID_INDEX;
  }
  return ret;
}

// only used for the first level parition;
// not support get subpart_id by tablet_id;
int ObSimpleTableSchemaV2::get_hidden_part_id_by_tablet_id(const ObTabletID &tablet_id, int64_t &part_id /*OUT*/) const
{
  int ret = OB_SUCCESS;
  part_id = OB_INVALID_PARTITION_ID;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id is invalid", KR(ret), K(tablet_id), KPC(this));
  } else if (part_level_ != PARTITION_LEVEL_ONE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part type", KR(ret), KPC(this));
  } else if (!has_tablet()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("There are no tablets in virtual table and view", KR(ret), KPC(this));
  } else {
    ObPartition **part_array = get_hidden_part_array();
    int64_t part_num = get_hidden_partition_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("part array is null", KR(ret), KPC(this));
    } else {
      bool found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num && !found; ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(i), KR(ret), KPC(this));
        } else if (PARTITION_LEVEL_ONE == part_level_) {
          if (part_array[i]->get_tablet_id() == tablet_id) {
            part_id = part_array[i]->get_part_id();
            found = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
        // error occurred
      } else if (!found) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("part is not exist", KR(ret), KPC(this));
      }
    }
  }
  if (OB_FAIL(ret)) {
    part_id = OB_INVALID_PARTITION_ID;
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_part_id_by_tablet(const ObTabletID &tablet_id, int64_t &part_id, int64_t &subpart_id) const
{
  int ret = OB_SUCCESS;
  part_id = OB_INVALID_INDEX;
  subpart_id = OB_INVALID_INDEX;
  if (common::ObTabletID::INVALID_TABLET_ID == tablet_id.id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_id is invalid", KPC(this), KR(ret));
  } else if (part_level_ >= PARTITION_LEVEL_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part type", KR(ret), KPC(this));
  } else if (!has_tablet()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("There are no tablets in virtual table and view", KR(ret), KPC(this));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("nonpart table", KR(ret), KPC(this));
  } else {
    ObPartition **part_array = get_part_array();
    int64_t part_num = get_partition_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", KPC(this), KR(ret));
    } else {
      bool found = false;
      for (int64_t i = 0; i < part_num && !found && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(i), KPC(this), KR(ret));
        } else if (PARTITION_LEVEL_ONE == part_level_) {
          if (part_array[i]->get_tablet_id() == tablet_id) {
            part_id = part_array[i]->get_part_id();
            found = true;
          }
        } else if (PARTITION_LEVEL_TWO == part_level_) {
          ObSubPartition **subpart_array = part_array[i]->get_subpart_array();
          int64_t subpart_num = part_array[i]->get_subpartition_num();
          if (OB_ISNULL(subpart_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpart array is null", KPC(this), KR(ret));
          } else {
            for (int64_t j = 0; j < subpart_num && !found && OB_SUCC(ret); ++j) {
              if (OB_ISNULL(subpart_array[j])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("NULL ptr", KPC(this), KR(ret));
              } else if (subpart_array[j]->get_tablet_id() == tablet_id) {
                part_id = part_array[i]->get_part_id();
                subpart_id = subpart_array[j]->get_sub_part_id();
                found = true;
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("4.0 not support part type", KR(ret), KPC(this));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!found) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("part is not exist", KPC(this), KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    part_id = OB_INVALID_INDEX;
    subpart_id = OB_INVALID_INDEX;
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_all_first_level_part_ids(ObIArray<int64_t> &first_level_part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(PARTITION_LEVEL_TWO != get_part_level()) ||
      OB_ISNULL(partition_array_) || partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_array is null or is empty",
        K(ret), KP_(partition_array), K_(partition_num), K(get_part_level()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
    if (OB_ISNULL(partition_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret));
    } else if (OB_FAIL(first_level_part_ids.push_back(partition_array_[i]->get_part_id()))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_part_ids_by_subpart_ids(const ObIArray<int64_t> &subpart_ids,
                                                       ObIArray<int64_t> &part_ids,
                                                       int64_t &subpart_cnt_in_parts) const
{
  int ret = OB_SUCCESS;
  subpart_cnt_in_parts = 0;
  ObHashSet<int64_t> subpart_hashset;
  ObPartition **part_array = NULL;
  int64_t part_num = get_partition_num();
  part_ids.reuse();
  if (OB_UNLIKELY(PARTITION_LEVEL_TWO != part_level_) ||
      OB_ISNULL(part_array = get_part_array())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part", KR(ret), KPC(this));
  } else if (OB_FAIL(subpart_hashset.create(get_all_part_num()))) {
    LOG_WARN("create hashset failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subpart_ids.count(); i ++) {
    if (OB_FAIL(subpart_hashset.set_refactored(subpart_ids.at(i)))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
  }
  for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(part_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(i), KPC(this), KR(ret));
    } else {
      ObSubPartition **subpart_array = part_array[i]->get_subpart_array();
      int64_t subpart_num = part_array[i]->get_subpartition_num();
      if (OB_ISNULL(subpart_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpart array is null", KPC(this), KR(ret));
      } else {
        bool found = false;
        for (int64_t j = 0; j < subpart_num && !found && OB_SUCC(ret); ++j) {
          if (OB_ISNULL(subpart_array[j])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", KPC(this), KR(ret));
          } else {
            int tmp_ret = subpart_hashset.exist_refactored((subpart_array[j]->get_sub_part_id()));
            if (OB_HASH_EXIST == tmp_ret) {
              if (OB_FAIL(part_ids.push_back(part_array[i]->get_part_id()))) {
                LOG_WARN("failed to push back", K(ret));
              } else {
                found = true;
                subpart_cnt_in_parts += subpart_num;
              }
            } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
              ret = tmp_ret;
              LOG_WARN("fail to check subpartids exist", K(ret));
            }
          }
        }
      }
    }
  }
  if (subpart_hashset.created()) {
    int tmp_ret = subpart_hashset.destroy();
    if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
      LOG_WARN("failed to destory hashset", K(ret));
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_part_idx_by_part_id(const ObIArray<int64_t> &part_ids,
                                                   ObIArray<int64_t> &part_idx,
                                                   ObIArray<int64_t> &subpart_idx) const
{
  int ret = OB_SUCCESS;
  ObHashSet<int64_t> id_hashset;
  ObPartition **part_array = NULL;
  int64_t part_num = get_partition_num();
  part_idx.reuse();
  subpart_idx.reuse();
  if (OB_ISNULL(part_array = get_part_array()) ||
      OB_UNLIKELY(PARTITION_LEVEL_ZERO == part_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part", KR(ret), KPC(this));
  } else if (OB_FAIL(id_hashset.create(get_all_part_num()))) {
    LOG_WARN("create hashset failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); i ++) {
    if (OB_FAIL(id_hashset.set_refactored(part_ids.at(i)))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
  }
  for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(part_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(i), KPC(this), KR(ret));
    } else if (PARTITION_LEVEL_ONE == part_level_) {
      int tmp_ret = id_hashset.exist_refactored(part_array[i]->get_part_id());
      if (OB_HASH_EXIST == tmp_ret) {
        OZ(part_idx.push_back(i));
        OZ(subpart_idx.push_back(OB_INVALID_ID));
      } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
        ret = tmp_ret;
        LOG_WARN("fail to check part id exist", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == part_level_) {
      ObSubPartition **subpart_array = part_array[i]->get_subpart_array();
      int64_t subpart_num = part_array[i]->get_subpartition_num();
      if (OB_ISNULL(subpart_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpart array is null", KPC(this), KR(ret));
      }
      for (int64_t j = 0; j < subpart_num && OB_SUCC(ret); ++j) {
        if (OB_ISNULL(subpart_array[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KPC(this), KR(ret));
        } else {
          int tmp_ret = id_hashset.exist_refactored(subpart_array[j]->get_sub_part_id());
          if (OB_HASH_EXIST == tmp_ret) {
            OZ(part_idx.push_back(i));
            OZ(subpart_idx.push_back(j));
          } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
            ret = tmp_ret;
            LOG_WARN("fail to check part id exist", K(ret));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("4.0 not support part type", KR(ret), KPC(this));
    }
  }
  if (id_hashset.created()) {
    int tmp_ret = id_hashset.destroy();
    if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
      LOG_WARN("failed to destory hashset", K(ret));
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_part_id_and_tablet_id_by_idx(const int64_t part_idx,
                                                            const int64_t subpart_idx,
                                                            ObObjectID &object_id,
                                                            ObObjectID &first_level_part_id,
                                                            ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  ObBasePartition *base_part = NULL;
  first_level_part_id = OB_INVALID_ID;
  if (PARTITION_LEVEL_ZERO == part_level_) {
    object_id = get_object_id();
    tablet_id = get_tablet_id();
  } else if (OB_FAIL(get_part_by_idx(part_idx, subpart_idx, base_part))) {
    LOG_WARN("fail to get part by idx", KR(ret), K(part_idx), K(subpart_idx));
  } else {
    object_id = base_part->get_object_id();
    tablet_id = base_part->get_tablet_id();
    if (PARTITION_LEVEL_TWO == part_level_) {
      first_level_part_id = static_cast<ObObjectID>(base_part->get_part_id());
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_part_by_idx(const int64_t part_idx, const int64_t subpart_idx, ObBasePartition *&partition) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == part_level_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("nonpart table", KR(ret), KPC(this));
  } else if (OB_ISNULL(partition_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KPC(this), KR(ret));
  } else if (part_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_idx is invalid", KPC(this), KR(ret));
  } else if (partition_num_ <= part_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_idx is not less than part_num", K(part_idx), KPC(this), KR(ret));
  } else if (OB_ISNULL(partition_array_[part_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(part_idx), KPC(this), KR(ret));
  } else if (PARTITION_LEVEL_ONE == part_level_) {
    if (subpart_idx >= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("subpart_idx must be invalid in nonsubpart table", KPC(this), KR(ret));
    } else {
      partition = partition_array_[part_idx];
    }
  } else if (PARTITION_LEVEL_TWO == part_level_) {
    ObSubPartition **subpart_array = partition_array_[part_idx]->get_subpart_array();
    int64_t subpart_num = partition_array_[part_idx]->get_subpartition_num();
    if (OB_ISNULL(subpart_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", KPC(this), KR(ret));
    } else if (subpart_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("subpart_idx is not uninvalid", K(part_idx), KPC(this), KR(ret));
    } else if (subpart_num <= subpart_idx) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("subpart_idx is not less than subpart_num", K(part_idx), K(subpart_idx),
               KPC(this), KR(ret));
    } else if (OB_ISNULL(subpart_array[subpart_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(part_idx), K(subpart_idx), KPC(this), KR(ret));
    } else {
      partition = subpart_array[subpart_idx];
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("part level is unexpected", KPC(this), KR(ret));
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_tablet_ids_by_part_object_id(
    const ObObjectID &part_object_id,
    common::ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = get_part_level();
  const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
  int64_t part_idx = OB_INVALID_INDEX;
  const ObPartition *partition = NULL;
  tablet_ids.reset();
  if (PARTITION_LEVEL_ONE != part_level
      && PARTITION_LEVEL_TWO != part_level) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported part_level", KR(ret), K(part_level));
  } else if (OB_FAIL(get_partition_index_loop(part_object_id, mode, part_idx))) {
    LOG_WARN("fail to get part idx", KR(ret), K(part_object_id), K(mode));
  } else if (OB_FAIL(get_partition_by_partition_index(part_idx, mode, partition))) {
    LOG_WARN("fail to get partition", KR(ret), K(part_object_id), K(part_idx), K(mode));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null", KR(ret), K(part_object_id), K(part_idx), K(mode));
  } else if (PARTITION_LEVEL_ONE == part_level) {
    if (OB_FAIL(tablet_ids.push_back(partition->get_tablet_id()))) {
      LOG_WARN("fail to push back tablet_id", KR(ret), KPC(partition));
    }
  } else {
    if (partition->get_subpartition_num() <= 0
        || OB_ISNULL(partition->get_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartitions is empty", KR(ret), KPC(partition));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition->get_subpartition_num(); i++) {
      ObSubPartition *&subpartition = partition->get_subpart_array()[i];
      if (OB_ISNULL(subpartition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition is empty", KR(ret), KPC(partition), K(i));
      } else if (OB_FAIL(tablet_ids.push_back(subpartition->get_tablet_id()))) {
        LOG_WARN("fail to push back tablet_id", KR(ret), KPC(subpartition));
      }
    } // end for
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_tablet_id_by_object_id(
    const ObObjectID &object_id,
    ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
  ObPartitionSchemaIter iter(*this, mode);
  tablet_id.reset();
  ObPartitionSchemaIter::Info info;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.next_partition_info(info))) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("object_id not found", KR(ret), K(object_id));
      } else {
        LOG_WARN("iter partition failed", KR(ret));
      }
    } else if (info.object_id_ == object_id) {
      tablet_id = info.tablet_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && !tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_id is invalid", KR(ret), K(object_id));
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_if_tablet_exists(const ObTabletID &tablet_id, bool &exists) const
{
  int ret = OB_SUCCESS;
  const ObCheckPartitionMode mode = CHECK_PARTITION_MODE_NORMAL;
  ObPartitionSchemaIter iter(*this, mode);
  ObPartitionSchemaIter::Info info;
  exists = false;
  while (OB_SUCC(ret) && !exists) {
    if (OB_FAIL(iter.next_partition_info(info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("iter partition failed", KR(ret));
      }
    } else if (info.tablet_id_ == tablet_id) {
      exists = true;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

ObTableSchema::ObTableSchema()
    : ObSimpleTableSchemaV2(),
      local_session_vars_(get_allocator())
{
  reset();
}

ObTableSchema::ObTableSchema(ObIAllocator *allocator)
  : ObSimpleTableSchemaV2(allocator),
    view_schema_(allocator),
    base_table_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    depend_table_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    simple_index_infos_(SCHEMA_MID_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    aux_vp_tid_array_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    rowkey_info_(allocator),
    shadow_rowkey_info_(allocator),
    index_info_(allocator),
    partition_key_info_(allocator),
    subpartition_key_info_(allocator),
    foreign_key_infos_(SCHEMA_BIG_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    label_se_column_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    trigger_list_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    depend_mock_fk_parent_table_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    rls_policy_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    rls_group_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    rls_context_ids_(SCHEMA_SMALL_MALLOC_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    name_generated_type_(GENERATED_TYPE_UNKNOWN),
    lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
    micro_index_clustered_(false),
    local_session_vars_(allocator),
    index_params_(),
    exec_env_()
{
  reset();
}

ObTableSchema::~ObTableSchema()
{
}

void ObTableSchema::reset_partition_schema()
{
  ObSimpleTableSchemaV2::reset_partition_schema();
}

void ObTableSchema::reset_column_part_key_info()
{
  partition_key_info_.reset();
  subpartition_key_info_.reset();
  part_key_column_num_ = 0;
  subpart_key_column_num_ = 0;
  for (int64_t i = 0; i < column_cnt_; i++) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (nullptr != column && column->is_tbl_part_key_column()) {
      column->set_not_part_key();
    }
  }
}

int ObTableSchema::assign(const ObTableSchema &src_schema)
{
  int ret = OB_SUCCESS;

  if (this != &src_schema) {
    reset();
    if (OB_FAIL(ObSimpleTableSchemaV2::assign(src_schema))) {
      LOG_WARN("fail to assign simple table schema", K(ret));
    } else {
      ObColumnSchemaV2 *column = NULL;
      char *buf = NULL;
      int64_t column_cnt = 0;
      int64_t cst_cnt = 0;
      max_used_column_id_ = src_schema.max_used_column_id_;
      sess_active_time_ = src_schema.sess_active_time_;
      rowkey_column_num_ = src_schema.rowkey_column_num_;
      index_column_num_ = src_schema.index_column_num_;
      rowkey_split_pos_ = src_schema.rowkey_split_pos_;
      part_key_column_num_ = src_schema.part_key_column_num_;
      subpart_key_column_num_ = src_schema.subpart_key_column_num_;
      block_size_ = src_schema.block_size_;
      is_use_bloomfilter_ = src_schema.is_use_bloomfilter_;
      progressive_merge_num_ = src_schema.progressive_merge_num_;
      tablet_size_ = src_schema.tablet_size_;
      pctfree_ = src_schema.pctfree_;
      autoinc_column_id_ = src_schema.autoinc_column_id_;
      auto_increment_ = src_schema.auto_increment_;
      read_only_ = src_schema.read_only_;
      load_type_ = src_schema.load_type_;
      index_using_type_ = src_schema.index_using_type_;
      def_type_ = src_schema.def_type_;
      charset_type_ = src_schema.charset_type_;
      collation_type_ = src_schema.collation_type_;
      code_version_ = src_schema.code_version_;
      index_attributes_set_ = src_schema.index_attributes_set_;
      row_store_type_ = src_schema.row_store_type_;
      store_format_ = src_schema.store_format_;
      progressive_merge_round_ = src_schema.progressive_merge_round_;
      storage_format_version_ = src_schema.storage_format_version_;
      table_dop_ = src_schema.table_dop_;
      define_user_id_ = src_schema.define_user_id_;
      aux_lob_meta_tid_ = src_schema.aux_lob_meta_tid_;
      aux_lob_piece_tid_ = src_schema.aux_lob_piece_tid_;
      compressor_type_ = src_schema.compressor_type_;
      table_flags_ = src_schema.table_flags_;
      name_generated_type_ = src_schema.name_generated_type_;
      lob_inrow_threshold_ = src_schema.lob_inrow_threshold_;
      auto_increment_cache_size_ = src_schema.auto_increment_cache_size_;
      is_column_store_supported_ = src_schema.is_column_store_supported_;
      max_used_column_group_id_ = src_schema.max_used_column_group_id_;
      micro_index_clustered_ = src_schema.micro_index_clustered_;
      mlog_tid_ = src_schema.mlog_tid_;
      if (OB_FAIL(deep_copy_str(src_schema.tablegroup_name_, tablegroup_name_))) {
        LOG_WARN("Fail to deep copy tablegroup_name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
        LOG_WARN("Fail to deep copy comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.pk_comment_, pk_comment_))) {
        LOG_WARN("Fail to deep copy primary key comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.create_host_, create_host_))) {
        LOG_WARN("Fail to deep copy primary key comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.expire_info_, expire_info_))) {
        LOG_WARN("Fail to deep copy expire info string", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.parser_name_, parser_name_))) {
        LOG_WARN("deep copy parser name failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_location_, external_file_location_))) {
        LOG_WARN("deep copy external_file_location failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_location_access_info_, external_file_location_access_info_))) {
        LOG_WARN("deep copy external_file_location_access_info failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_format_, external_file_format_))) {
        LOG_WARN("deep copy external_file_format failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_pattern_, external_file_pattern_))) {
        LOG_WARN("deep copy external_file_pattern failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_properties_, external_properties_))) {
        LOG_WARN("deep copy external_properties failed", K(ret));
      }
      //view schema
      if (OB_SUCC(ret)) {
        view_schema_ = src_schema.view_schema_;
        if (OB_FAIL(view_schema_.get_err_ret())) {
          LOG_WARN("fail to assign view schema", K(ret), K_(view_schema), K(src_schema.view_schema_));
        } else if (OB_FAIL(local_session_vars_.deep_copy(src_schema.local_session_vars_))) {
          LOG_WARN("fail to deep copy sys var info", K(ret));
        }
      }

      if (FAILEDx(aux_vp_tid_array_.assign(src_schema.aux_vp_tid_array_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      if (FAILEDx(base_table_ids_.assign(src_schema.base_table_ids_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      if (FAILEDx(depend_table_ids_.assign(src_schema.depend_table_ids_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      if (FAILEDx(depend_mock_fk_parent_table_ids_.assign(src_schema.depend_mock_fk_parent_table_ids_))) {
        LOG_WARN("fail to assign depend_mock_fk_parent_table_ids_ array", K(ret));
      }

      if (FAILEDx(mv_mode_.assign(src_schema.mv_mode_))) {
        LOG_WARN("fail to assign mv_mode", K(ret));
      }

      //copy columns
      column_cnt = src_schema.column_cnt_;
      // copy constraints
      cst_cnt = src_schema.cst_cnt_;
      //prepare memory
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_info_.reserve(src_schema.rowkey_info_.get_size()))) {
          LOG_WARN("Fail to reserve rowkey_info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(shadow_rowkey_info_.reserve(src_schema.shadow_rowkey_info_.get_size()))) {
          LOG_WARN("Fail to reserve shadow_rowkey_info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(index_info_.reserve(src_schema.index_info_.get_size()))) {
          LOG_WARN("Fail to reserve index_info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition_key_info_.reserve(src_schema.partition_key_info_.get_size()))) {
          LOG_WARN("Fail to reserve partition_key_info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(subpartition_key_info_.reserve(src_schema.subpartition_key_info_.get_size()))) {
          LOG_WARN("Fail to reserve partition_key_info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(label_se_column_ids_.reserve(src_schema.label_se_column_ids_.count()))) {
          LOG_WARN("fail to reserve label_se_column_ids", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t id_hash_array_size = get_id_hash_array_mem_size(column_cnt);
        if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for id_hash_array, ", K(id_hash_array_size), K(ret));
        } else if (NULL == (id_hash_array_ = new (buf) IdHashArray(id_hash_array_size))){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Fail to new IdHashArray", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t name_hash_array_size = get_name_hash_array_mem_size(column_cnt);
        if (NULL == (buf = static_cast<char*>(alloc(name_hash_array_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (NULL == (name_hash_array_ = new (buf) NameHashArray(name_hash_array_size))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Fail to new NameHashArray", K(ret));
        }
      }
      if (OB_SUCCESS == ret && column_cnt > 0) {
        column_array_ = static_cast<ObColumnSchemaV2**>(alloc(sizeof(ObColumnSchemaV2*) * column_cnt));
        if (NULL == column_array_) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for column_array_", K(ret));
        } else {
          MEMSET(column_array_, 0, sizeof(ObColumnSchemaV2*) * column_cnt);
          column_array_capacity_ = column_cnt;
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        column = src_schema.column_array_[i];
        if (NULL == column) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is NULL.", K(ret));
        } else if (OB_FAIL(add_column(*column))) {
          LOG_WARN("Fail to add column, ", K(*column), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(assign_column_group(src_schema))) {
          LOG_WARN("fail to assign column_group", KR(ret), K(src_schema));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(assign_constraint(src_schema))) {
          LOG_WARN("failed to assign constraint", K(ret), K(src_schema), K(*this));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_foreign_key_infos(src_schema.get_foreign_key_infos()))) {
          LOG_WARN("failed to set foreign key infos", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_trigger_list(src_schema.get_trigger_list()))) {
          LOG_WARN("failed to set trigger list", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_simple_index_infos(src_schema.get_simple_index_infos()))) {
          LOG_WARN("fail to set simple index infos", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(assign_rls_objects(src_schema))) {
          LOG_WARN("failed to assign rls objects", K(ret));
        }
      }
    }
  }


  if (OB_SUCC(ret) && OB_FAIL(deep_copy_str(src_schema.ttl_definition_, ttl_definition_))) {
    LOG_WARN("deep copy ttl definition failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(deep_copy_str(src_schema.kv_attributes_, kv_attributes_))) {
    LOG_WARN("deep copy kv attributes failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(deep_copy_str(src_schema.index_params_, index_params_))) {
    LOG_WARN("deep copy vector index param failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(deep_copy_str(src_schema.exec_env_, exec_env_))) {
    LOG_WARN("deep copy vector exec_env failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to assign table schema", K(ret));
  }
  return ret;
}

int ObTableSchema::get_view_column_comment(ObIArray<ObString> &column_comments)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    column = column_array_[i];
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is NULL.", K(ret));
    } else if (OB_FAIL(column_comments.push_back(column->get_comment()))) {
      LOG_WARN("Fail to add column comment, ", K(*column), K(ret));
    }
  }
  return ret;
}

void ObTableSchema::clear_constraint()
{
  cst_cnt_ = 0;
  cst_array_capacity_ = 0;
  cst_array_ = NULL;
}

int ObTableSchema::assign_constraint(const ObTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    cst_cnt_ = 0;
    cst_array_capacity_ = 0;
    cst_array_ = NULL;
    const int64_t cst_cnt = src_schema.cst_cnt_;
    if (cst_cnt > 0) {
      cst_array_ = static_cast<ObConstraint**>(alloc(sizeof(ObConstraint*)
                                                            * cst_cnt));
      if (NULL == cst_array_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for cst_array", K(ret));
      } else {
        MEMSET(cst_array_, 0, sizeof(ObConstraint*) * cst_cnt);
        cst_array_capacity_ = cst_cnt;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < cst_cnt; ++i) {
        ObConstraint *constraint = src_schema.cst_array_[i];
        if (NULL == constraint) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The constraint is NULL.", K(ret));
        } else if (OB_FAIL(add_constraint(*constraint))) {
          LOG_WARN("Fail to add constraint, ", K(*constraint), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        error_ret_ = ret;
      }
    }
  }
  return ret;
}

int ObTableSchema::assign_column_group(const ObTableSchema &other)
{
  int ret = OB_SUCCESS;
  int64_t column_group_cnt = other.column_group_cnt_;
  if (column_group_cnt > 0) {
    ObColumnGroupSchema *column_group = NULL;
    char *buf = NULL;
    int64_t cg_id_hash_arr_size = get_hash_array_mem_size<CgIdHashArray>(column_group_cnt);
    if (NULL == (buf = static_cast<char*>(alloc(cg_id_hash_arr_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory for cg_id_hash_array", KR(ret), K(cg_id_hash_arr_size));
    } else if (NULL == (cg_id_hash_arr_ = new (buf) CgIdHashArray(cg_id_hash_arr_size))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new CgIdHashArray", KR(ret), K(cg_id_hash_arr_size));
    }

    if (OB_SUCC(ret)) {
      int64_t cg_name_hash_arr_size = get_hash_array_mem_size<CgNameHashArray>(column_group_cnt);
      if (OB_ISNULL(buf = static_cast<char*>(alloc(cg_name_hash_arr_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for cg_name_hash_array", KR(ret), K(cg_name_hash_arr_size));
      } else if (OB_ISNULL(cg_name_hash_arr_ = new (buf) CgNameHashArray(cg_name_hash_arr_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to new CgNameHashArray", KR(ret), K(cg_name_hash_arr_size));
      }
    }

    if (OB_SUCC(ret)) {
      column_group_arr_ = static_cast<ObColumnGroupSchema**>(alloc(sizeof(ObColumnGroupSchema*) * column_group_cnt));
      if (OB_ISNULL(column_group_arr_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for column_group_array", KR(ret), K(column_group_cnt));
      } else {
        MEMSET(column_group_arr_, 0, sizeof(ObColumnGroupSchema*) * column_group_cnt);
        column_group_arr_capacity_ = column_group_cnt;
      }
    }

    for(int64_t i = 0; OB_SUCC(ret) && (i < column_group_cnt); ++i) {
      column_group = other.column_group_arr_[i];
      if (OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the column group is null", KR(ret), K(i), K(column_group_cnt));
      } else if (OB_FAIL(do_add_column_group(*column_group))) {
        LOG_WARN("fail to do add column group", KR(ret), KPC(column_group));
      }
    }
  }
  return ret;
}

int ObTableSchema::assign_partition_schema_without_auto_part_attr(const ObTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    bool auto_part = part_option_.get_auto_part();
    int64_t auto_part_size = part_option_.get_auto_part_size();

    if (OB_FAIL(assign_partition_schema(src_schema))) {
      LOG_WARN("fail to assign partition schema", KR(ret), K(src_schema), KPC(this));
    } else {
      part_option_.set_auto_part(auto_part);
      part_option_.set_auto_part_size(auto_part_size);
    }
  }
  return ret;
}

int ObTableSchema::enable_auto_partition(const int64_t auto_part_size, const ObPartitionFuncType &part_func_type)
{
  int ret = OB_SUCCESS;
  if (is_partitioned_table()) {
    if (OB_FAIL(part_option_.enable_auto_partition(auto_part_size))) {
      LOG_WARN("fail to enable auto partition", KR(ret), K(auto_part_size));
    }
  } else {
    ObPartitionFuncType part_type = part_func_type;
    if (OB_FAIL(detect_auto_part_func_type(part_type))) {
      LOG_WARN("fail to check part func type", K(ret));
    } else if (OB_FAIL(part_option_.enable_auto_partition(auto_part_size, part_type))) {
      LOG_WARN("fail to enable auto partition", KR(ret), K(auto_part_size));
    }
  }
  return ret;
}

int ObTableSchema::detect_auto_part_func_type(ObPartitionFuncType &part_func_type)
{
  int ret = OB_SUCCESS;
  const ObString &part_expr = part_option_.get_part_func_expr_str();
  if (part_expr.empty()) {
    bool is_range_column_type = false;
    if (OB_FAIL(is_range_col_part_type(is_range_column_type))) {
      LOG_WARN("failed to check if first part key is range column type", K(ret), K(part_expr));
    } else if (is_range_column_type) {
      /*in case of create table t1(a timestamp, b VARCHAR(150), c int, d VARCHAR(4000), primary key(a)) partition by range ()*/
      part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    } else {
      part_func_type = PARTITION_FUNC_TYPE_RANGE;
    }
  }
  return ret;
}
//this function can only be used for auto split table check
int ObTableSchema::is_range_col_part_type(bool &is_range_column_type) const
{
  int ret = OB_SUCCESS;
  is_range_column_type = false;
  ObObjMeta type;
  if (!is_index_table()) {
    ObRowkeyColumn row_key_col;
    const common::ObRowkeyInfo &row_key_info = get_rowkey_info();
    if (row_key_info.get_size() > 1) {
      is_range_column_type = true;
    } else if (OB_FAIL(row_key_info.get_column(0/*since there is only one row key, we only need to check the first one*/, row_key_col))) {
      LOG_WARN("get row key column failed", K(ret), K(row_key_info));
    } else if (ObResolverUtils::is_partition_range_column_type(row_key_col.get_meta_type().get_type())) {
      is_range_column_type = true;
    }
  } else {
    ObIndexColumn index_key_col;
    const common::ObIndexInfo &index_key_info = get_index_info();
    if (index_key_info.get_size() > 1) {
      is_range_column_type = true;
    } else if (OB_FAIL(index_key_info.get_column(0/*since there is only one index key, we only need to check the first one*/, index_key_col))) {
      LOG_WARN("get index key column failed", K(ret), K(index_key_info));
    } else if (ObResolverUtils::is_partition_range_column_type(index_key_col.get_meta_type().get_type())) {
      is_range_column_type = true;
    }
  }
  return ret;
}

void ObTableSchema::forbid_auto_partition()
{
  part_option_.forbid_auto_partition(is_partitioned_table());
}

int ObTableSchema::check_valid(const bool count_varchar_size_by_byte) const
{
  int ret = OB_SUCCESS;

  if (!ObSimpleTableSchemaV2::is_valid()) {
    ret = OB_INVALID_ERROR;
    LOG_WARN_RET(OB_INVALID_ERROR, "schema is invalid", K_(error_ret));
  }

  if (OB_FAIL(ret) || is_view_table()) {
    // no need checking other options for view
    // XIYU: TODO for materialized view
  } else {
    if (is_virtual_table(table_id_) && 0 > rowkey_column_num_) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "invalid rowkey_column_num:", K_(table_name), K_(rowkey_column_num));
      //TODO:(xiyu) confirm to delte it
    } else if (!is_virtual_table(table_id_) && 1 > rowkey_column_num_ && OB_INVALID_ID == dblink_id_) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "no primary key specified:", K_(table_name));
    } else if (index_column_num_ < 0 || index_column_num_ > OB_MAX_ROWKEY_COLUMN_NUMBER) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "invalid index_column_num", K_(table_name), K_(index_column_num));
    } else if (part_key_column_num_ > OB_MAX_PARTITION_KEY_COLUMN_NUMBER) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "partition key column num invalid", K_(table_name),
          K_(part_key_column_num), K(OB_MAX_PARTITION_KEY_COLUMN_NUMBER));
    } else {
      int64_t def_rowkey_col = 0;
      int64_t def_index_col = 0;
      int64_t def_part_key_col = 0;
      int64_t def_subpart_key_col = 0;
      int64_t varchar_col_total_length = 0;
      int64_t rowkey_varchar_col_length = 0;
      ObColumnSchemaV2 *column = NULL;

      if (NULL == column_array_) {
        ret = OB_INVALID_ERROR;
        LOG_WARN_RET(OB_INVALID_ERROR, "The column_array is NULL.");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
        if (NULL == (column = column_array_[i])) {
          ret = OB_INVALID_ERROR;
          LOG_WARN_RET(OB_INVALID_ERROR, "The column is NULL.");
        } else {
          if (column->get_rowkey_position() > 0) {
            ++def_rowkey_col;
            if ((column->get_column_id() > max_used_column_id_)
                && (column->get_column_id() <= common::OB_MAX_TMP_COLUMN_ID)) {
              ret = OB_INVALID_ERROR;
              LOG_WARN_RET(OB_INVALID_ERROR, "column id is greater than max_used_column_id, ",
                        "column_name", column->get_column_name(),
                        "column_id", column->get_column_id(),
                        K_(max_used_column_id));
            }
          }

          if (column->is_index_column()) {
            ++def_index_col;
            if (column->get_column_id() > max_used_column_id_) {
              ret = OB_INVALID_ERROR;
              LOG_WARN_RET(OB_INVALID_ERROR, "column id is greater than max_used_column_id, ",
                       "column_name", column->get_column_name(),
                       "column_id", column->get_column_id(),
                       K_(max_used_column_id));
            }
          }
          if (column->is_part_key_column()) {
            ++def_part_key_col;
          }
          if (column->is_subpart_key_column()) {
            ++def_subpart_key_col;
          }

          // TODO@nijia.nj data_length should be checked according specified charset
          if ((is_table() || is_tmp_table() || is_external_table()) && !column->is_column_stored_in_sstable()) {
            // When the column is a virtual generated column in the table, the column will not be stored,
            // so there is no need to calculate its length
          } else if (is_storage_index_table() && column->is_fulltext_column()) {
            // The full text column in the index only counts the length of one word segment
            varchar_col_total_length += OB_MAX_OBJECT_NAME_LENGTH;
          } else {
            int64_t varchar_col_len = 0;
            if (ObVarcharType == column->get_data_type()) {
              if (OB_MAX_VARCHAR_LENGTH < column->get_data_length()) {
                ret = OB_INVALID_ERROR;
                LOG_WARN_RET(OB_INVALID_ERROR, "length of varchar column is larger than the max allowed length, ",
                    "data_length", column->get_data_length(),
                    "column_name", column->get_column_name(),
                    K(OB_MAX_VARCHAR_LENGTH));
              } else {
                if (count_varchar_size_by_byte) {
                  if (OB_FAIL(column->get_byte_length(varchar_col_len, lib::is_oracle_mode(), false))) {
                    LOG_WARN("get_byte_length failed ", K(ret));
                  }
                } else {
                  varchar_col_len = column->get_data_length();
                }
                varchar_col_total_length += varchar_col_len;
              }
              if (OB_FAIL(ret)) {
              } else if (column->is_rowkey_column() && !column->is_hidden()) {
                if (is_index_table() && 0 == column->get_index_position()) {
                  // Non-user-created index columns in the index table are not counted in rowkey_varchar_col_length
                } else {
                  rowkey_varchar_col_length += varchar_col_len;
                }
              }
            } else if (ob_is_text_tc(column->get_data_type()) || ob_is_json_tc(column->get_data_type())
                       || ob_is_geometry_tc(column->get_data_type()) || ob_is_roaringbitmap_tc(column->get_data_type())) {
              ObLength max_length = 0;
              max_length = ObAccuracy::MAX_ACCURACY[column->get_data_type()].get_length();
              if (max_length < column->get_data_length()) {
                ret = OB_INVALID_ERROR;
                LOG_WARN_RET(OB_INVALID_ERROR, "length of text/blob column is larger than the max allowed length, ",
                    "data_length", column->get_data_length(), "column_name",
                    column->get_column_name(), K(max_length));
              } else if (!column->is_shadow_column()) {
                // TODO @hanhui need seperate inline memtable length from store length
                varchar_col_total_length += min(column->get_data_length(), get_lob_inrow_threshold());
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObColumnGroupSchema *column_group = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && (i < column_group_cnt_); ++i) {
          if (OB_ISNULL(column_group = column_group_arr_[i])) {
            ret = OB_INVALID_ERROR;
            LOG_WARN_RET(OB_ERR_UNEXPECTED, "column_group should not be null", K(i), K_(column_group_cnt));
          } else if (!column_group->is_valid()) {
            ret = OB_INVALID_ERROR;
            LOG_WARN_RET(OB_ERR_UNEXPECTED, "column_group is invalid", K(i), KPC(column_group));
          } else if (column_group->get_column_group_id() > max_used_column_group_id_) {
            ret = OB_INVALID_ERROR;
            LOG_WARN_RET(OB_ERR_UNEXPECTED, "column_group id should not be greater than max_used_column_group_id",
              "cg_id", column_group->get_column_group_id(), K_(max_used_column_group_id));
          }
        }
      }

      if (OB_SUCC(ret)) {
        //TODO oushen confirm the length
        //
        // jiage: inner table shouldn't check VARCHAR length for
        // compatibility.  VARCHAR length in previous version means
        // maximum bytes of column, whereas new version changes to chars
        // of column.
        const int64_t max_row_length = is_sys_table() || is_vir_table() ? INT64_MAX : OB_MAX_USER_ROW_LENGTH;
        const int64_t max_rowkey_length = is_sys_table() || is_vir_table() ? OB_MAX_ROW_KEY_LENGTH : OB_MAX_USER_ROW_KEY_LENGTH;
        if (max_row_length < varchar_col_total_length) {
          LOG_WARN_RET(OB_INVALID_ERROR, "total length of varchar columns is larger than the max allowed length",
                   K(varchar_col_total_length), K(max_row_length));
          const ObString &col_name = column->get_column_name_str();
          ret = OB_INVALID_ERROR;
          LOG_USER_ERROR(OB_ERR_VARCHAR_TOO_LONG,
                         static_cast<int>(varchar_col_total_length), max_row_length, col_name.ptr());
        } else if (max_rowkey_length < rowkey_varchar_col_length) {
          ret = OB_ERR_TOO_LONG_KEY_LENGTH;
          LOG_WARN_RET(OB_INVALID_ERROR, "total length of varchar primary key columns is larger than the max allowed length",
                   K(rowkey_varchar_col_length), K(max_rowkey_length));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, max_rowkey_length);
        }
      }
      if (OB_SUCC(ret)) {
        if (def_rowkey_col != rowkey_column_num_) {
          ret = OB_INVALID_ERROR;
          LOG_WARN_RET(OB_INVALID_ERROR, "rowkey_column_num not equal with defined_num",
                   K_(rowkey_column_num), K(def_rowkey_col), K_(table_name));
        }
      }
      if (OB_SUCC(ret)) {
        if (def_index_col != index_column_num_) {
          ret = OB_INVALID_ERROR;
          LOG_WARN_RET(OB_INVALID_ERROR, "index_column_num not equal with defined_num",
                   K_(index_column_num), K(def_index_col), K_(table_name));
        }
      }
      if (OB_SUCC(ret)) {
        if (def_part_key_col != part_key_column_num_ || def_subpart_key_col != subpart_key_column_num_) {
          ret = OB_INVALID_ERROR;
          LOG_WARN_RET(OB_INVALID_ERROR, "partition key column num not equal with the defined num",
                   K_(part_key_column_num), K(def_part_key_col), K_(table_name));
        }
      }
    }
  }
  return ret;
}

bool ObTableSchema::is_valid() const
{
  return OB_SUCCESS == check_valid(false/*count varchar by byte size == false*/);
}

int ObTableSchema::set_compress_func_name(const char *compressor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compressor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compressor is null", K(ret));
  } else if (strlen(compressor) == 0) {
    compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(compressor, compressor_type_))) {
    LOG_WARN("fail to get compressor type", K(ret), K(*compressor));
  }
  return ret;
}

int ObTableSchema::set_compress_func_name(const ObString &compressor)
{
  return ObCompressorPool::get_instance().get_compressor_type(compressor, compressor_type_);
}

int ObTableSchema::set_row_store_type(const ObString &row_store)
{
  return ObStoreFormat::find_row_store_type(row_store, row_store_type_);
}

int ObTableSchema::set_store_format(const ObString &store_format)
{
  return ObStoreFormat::find_store_format_type(store_format, store_format_);
}

int ObTableSchema::delete_column_update_prev_id(ObColumnSchemaV2 *local_column)
{
  int ret = OB_SUCCESS;
  // local_column is the schema obtained from the current table through the column name, prev/nextID is complete
  if (OB_ISNULL(local_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The column is NULL");
  } else {
    ObColumnSchemaV2 *prev_col = get_column_schema(local_column->get_prev_column_id());
    ObColumnSchemaV2 *next_col = get_column_schema(local_column->get_next_column_id());
    if (OB_NOT_NULL(prev_col)) {
      // update nextID
      prev_col->set_next_column_id(local_column->get_next_column_id());
    } else if (OB_ISNULL(next_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is NULL");
    } else {
      // prev_col is NULL, so local_column is head column
      next_col->set_prev_column_id(BORDER_COLUMN_ID);
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(next_col)) {
        // update prevID
        next_col->set_prev_column_id(local_column->get_prev_column_id());
      } else {
        // next_col is null, so local_column is tail column
        prev_col->set_next_column_id(BORDER_COLUMN_ID);
      }
    }
  }
  return ret;
}

int ObTableSchema::add_column_update_prev_id(ObColumnSchemaV2 *local_column)
{
  int ret = OB_SUCCESS;
  // The local_column in add_column is provided by outside caller, prev/nextID may not be complete
  if (OB_ISNULL(local_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The column is NULL");
  } else {
    if (UINT64_MAX == local_column->get_prev_column_id()) {
      // Add to the end by default, the current column is the last column, then next is directly set to BORDER_COLUMN_ID
      local_column->set_prev_column_id(BORDER_COLUMN_ID);
      local_column->set_next_column_id(BORDER_COLUMN_ID);
      const_column_iterator iter = column_begin();
      for ( ; iter != column_end(); ++iter) {
        if (BORDER_COLUMN_ID == (*iter)->get_next_column_id()) {
          local_column->set_prev_column_id((*iter)->get_column_id());
          (*iter)->set_next_column_id(local_column->get_column_id());
        }
      }
    } else {
      // only build next(Read from the internal table with prev ID, or add column only specifies prev ID)
      ObColumnSchemaV2 *prev_col = get_column_schema_by_prev_next_id(local_column->get_prev_column_id());
      if (OB_NOT_NULL(prev_col)) {
        local_column->set_next_column_id(prev_col->get_next_column_id());
        prev_col->set_next_column_id(local_column->get_column_id());
      } else {
        local_column->set_next_column_id(BORDER_COLUMN_ID);
      }

      const_column_iterator iter = column_begin();
      for( ; iter != column_end(); ++iter) {
        if ((*iter)->get_prev_column_id() == local_column->get_column_id()) {
          local_column->set_next_column_id((*iter)->get_column_id());
        }
        if (local_column->get_prev_column_id() == (*iter)->get_column_id()) {
          (*iter)->set_next_column_id(local_column->get_column_id());
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::set_rowkey_info(const ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  if (!column.is_rowkey_column()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column isn't rowkey", K(column));
  } else {
    ObRowkeyColumn rowkey_column;
    rowkey_column.column_id_ = column.get_column_id();
    rowkey_column.length_ = column.get_data_length();
    rowkey_column.order_ = column.get_order_in_rowkey();
    rowkey_column.type_ = column.get_meta_type();
    if (rowkey_column.type_.is_decimal_int()) {
      rowkey_column.type_.set_scale(column.get_accuracy().get_scale());
    }
    if (OB_FAIL(rowkey_info_.set_column(column.get_rowkey_position() - 1, rowkey_column))) {
      LOG_WARN("Fail to set column to rowkey info", K(ret));
    } else {
      if (rowkey_column_num_ < rowkey_info_.get_size()) {
        rowkey_column_num_ = rowkey_info_.get_size();
        if ((is_user_table() || is_tmp_table()) && rowkey_column_num_ > OB_MAX_ROWKEY_COLUMN_NUMBER) {
          ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_MAX_ROWKEY_COLUMN_NUMBER);
        }
      }
    }
  }
  return ret;
}

bool ObTableSchema::is_column_in_check_constraint(const uint64_t col_id) const
{
  bool bool_ret = false;
  for (const_constraint_iterator iter = constraint_begin(); iter != constraint_end(); ++iter) {
    if (CONSTRAINT_TYPE_CHECK == (*iter)->get_constraint_type()) {
      for (ObConstraint::const_cst_col_iterator cst_iter = (*iter)->cst_col_begin();
           cst_iter != (*iter)->cst_col_end(); ++cst_iter) {
        if (col_id == (*cst_iter)) {
          bool_ret = true;
          break;
        }
      }
    }
  }
  return bool_ret;
}

bool ObTableSchema::is_column_in_foreign_key(const uint64_t col_id) const
{
  bool bool_ret = false;
  for (int64_t i = 0; !bool_ret && i < foreign_key_infos_.count(); i++) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos_.at(i);
    // parent table
    if (foreign_key_info.parent_table_id_ == table_id_) {
      for (int64_t j = 0; j < foreign_key_info.parent_column_ids_.count(); j++) {
        if (col_id == foreign_key_info.parent_column_ids_.at(j)) {
          bool_ret = true;
          break;
        }
      }
    } else if (foreign_key_info.child_table_id_ == table_id_) {
    // child table
      for (int64_t j = 0; j < foreign_key_info.child_column_ids_.count(); j++) {
        if (col_id == foreign_key_info.child_column_ids_.at(j)) {
          bool_ret = true;
          break;
        }
      }
    }
  }
  return bool_ret;
}

int ObTableSchema::is_column_in_partition_key(const uint64_t col_id, bool &is_in_partition_key) const
{
  int ret = OB_SUCCESS;
  is_in_partition_key = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_in_partition_key && i < partition_key_info_.get_size(); i++) {
    uint64_t pkey_col_id = OB_INVALID_ID;
    if (OB_FAIL(partition_key_info_.get_column_id(i, pkey_col_id))) {
      LOG_WARN("get_column_id failed", "index", i, K(ret));
    } else if (pkey_col_id == col_id) {
      is_in_partition_key = true;
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_in_partition_key && i < subpartition_key_info_.get_size(); i++) {
    uint64_t pkey_col_id = OB_INVALID_ID;
    if (OB_FAIL(subpartition_key_info_.get_column_id(i, pkey_col_id))) {
      LOG_WARN("get_column_id failed", "index", i, K(ret));
    } else if (pkey_col_id == col_id) {
      is_in_partition_key = true;
      break;
    }
  }
  return ret;
}

bool ObTableSchema::has_check_constraint() const
{
  bool bool_ret = false;

  for (const_constraint_iterator iter = constraint_begin(); iter != constraint_end(); ++iter) {
    if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
      continue;
    } else {
      bool_ret = true;
      break;
    }
  }

  return bool_ret;
}

int ObTableSchema::delete_column(const common::ObString &column_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column_schema = NULL;
  bool for_view = false;
  if (OB_ISNULL(column_name) || column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column name is NULL", K(ret));
  } else {
    column_schema = get_column_schema(column_name);
    if (NULL == column_schema) {
      ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
      LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, column_name.length(), column_name.ptr());
    } else if (OB_FAIL(delete_column_internal(column_schema, for_view))) {
      LOG_WARN("Failed to delete column, ", K(column_name), K(ret));
    }
  }
  return ret;
}

// for view = true, no constraint checking
int ObTableSchema::alter_column(ObColumnSchemaV2 &column_schema, ObColumnCheckMode check_mode, const bool for_view)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObColumnSchemaV2 *src_schema = get_column_schema(column_schema.get_column_id());
  if (NULL == src_schema) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_schema.get_column_name_str().length(),
                   column_schema.get_column_name(),
                   table_name_.length(),
                   table_name_.ptr());
    LOG_WARN("column not exist", K(ret),
             "column_name", column_schema.get_column_name());
  //if the src_schema is a rowkey column
  //check_column_can_be_altered will modify dst_schema's is_nullable attribute to not nullable
  //TODO @hualong should move it to other place
  } else if (!for_view && ObColumnCheckMode::CHECK_MODE_ONLINE == check_mode &&
    OB_FAIL(check_column_can_be_altered_online(src_schema, &column_schema))) {
    LOG_WARN("Failed to alter column schema", K(ret));
  } else if (!for_view && ObColumnCheckMode::CHECK_MODE_OFFLINE == check_mode &&
    OB_FAIL(check_column_can_be_altered_offline(src_schema, &column_schema))) {
    LOG_WARN("Failed to alter column schema", K(ret));
  } else {
    const ObString &dst_name = column_schema.get_column_name_str();
    if (!src_schema->is_autoincrement() && column_schema.is_autoincrement()) {
      autoinc_column_id_ = column_schema.get_column_id();
    }
    if (src_schema->is_autoincrement() && !column_schema.is_autoincrement()) {
      autoinc_column_id_ = 0;
    }
    if (src_schema->get_column_name_str() != dst_name && is_column_store_supported()) {
      bool is_oracle_mode = false;
      char cg_name[OB_MAX_COLUMN_GROUP_NAME_LENGTH] = {'\0'};
      ObString cg_name_str(OB_MAX_COLUMN_GROUP_NAME_LENGTH, 0, cg_name);

      if (OB_FAIL(src_schema->get_each_column_group_name(cg_name_str))) {
        LOG_WARN("fail to get each column group name", K(ret));
      } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check oracle mode", KR(ret));
      } else if (OB_FAIL(remove_col_from_name_hash_array(is_oracle_mode, src_schema))) {
        LOG_WARN("Failed to remove old column name from name_hash_array", K(ret));
      } else if (OB_FAIL(src_schema->set_column_name(dst_name))) {
        LOG_WARN("failed to change column name", K(ret));
      } else if (OB_FAIL(add_col_to_name_hash_array(is_oracle_mode, src_schema))) {
        LOG_WARN("Failed to add new column name to name_hash_array", K(ret));
      }

      /*alter relavent column group name*/
      ObColumnGroupSchema *column_group = nullptr;
      if (OB_FAIL(ret)){
      } else if (OB_FAIL(get_column_group_by_name(cg_name_str, column_group))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check column group exist", K(ret), K(cg_name_str));
        }
      } else if(OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group should no be null", K(ret));
      } else if (OB_FAIL(cg_name_hash_arr_->erase_refactored(ObColumnGroupSchemaHashWrapper(
                                                             column_group->get_column_group_name())))) {
        LOG_WARN("fail to remove from cg name arr", K(ret));
      } else {
        cg_name_str.set_length(0);
        if (OB_FAIL(src_schema->get_each_column_group_name(cg_name_str))) { /* src_schema column name has been changed*/
          LOG_WARN("fail to get column group name", K(ret));
        } else if (OB_FAIL(column_group->set_column_group_name(cg_name_str))) {
          LOG_WARN("fail to set column group name", K(ret));
        } else if (OB_FAIL((add_column_group_to_hash_array<ObColumnGroupSchemaHashWrapper, CgNameHashArray>(
          column_group, ObColumnGroupSchemaHashWrapper(column_group->get_column_group_name()), cg_name_hash_arr_)))) {
          LOG_WARN("fail to set cg_name _hash_arr", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(src_schema->assign(column_schema))) {
      LOG_WARN("failed to assign src schema", K(ret), K(column_schema));
    }
  }
  return ret;
}

int ObTableSchema::alter_mysql_table_columns(ObIArray<ObColumnSchemaV2> &columns,
                                             ObIArray<ObString> &orig_names,
                                             ObColumnCheckMode check_mode)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnSchemaV2 *, 16> src_cols;
  ObSEArray<ObColumnSchemaV2 *, 16> rename_cols;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to check oracle mode", K(ret));
  } else if (OB_UNLIKELY(is_oracle_mode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only for mysql mode", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObColumnSchemaV2 *src_col = get_column_schema(orig_names.at(i));
    if (OB_ISNULL(src_col)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, orig_names.at(i).length(),
                     orig_names.at(i).ptr(), table_name_.length(), table_name_.ptr());
      LOG_WARN("column not exists", K(ret), "column_name", columns.at(i).get_column_name());
    } else if (ObColumnCheckMode::CHECK_MODE_ONLINE == check_mode
               && OB_FAIL(check_column_can_be_altered_online(src_col, &columns.at(i)))) {
      if (OB_ERR_COLUMN_DUPLICATE == ret) {
        // create table t (a int, b int);
        // alter table t rename column a to b, rename column b to a;
        // `check_column_can_be_altered_online` will report for scenrio above, ignore error for now,
        // if the final columns have duplicated names, error will be reported later.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to alter column schema", K(ret));
      }
    } else if (ObColumnCheckMode::CHECK_MODE_OFFLINE == check_mode
               && OB_FAIL(check_column_can_be_altered_offline(src_col, &columns.at(i)))) {
      if (OB_ERR_COLUMN_DUPLICATE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("check column can be altered offline failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && !src_col->is_autoincrement() && columns.at(i).is_autoincrement()) {
      autoinc_column_id_ = columns.at(i).get_column_id();
    }
    if (OB_SUCC(ret) && src_col->is_autoincrement() && !columns.at(i).is_autoincrement()) {
      autoinc_column_id_ = 0;
    }
    if (OB_SUCC(ret) && OB_FAIL(src_cols.push_back(src_col))) {
      LOG_WARN("push back element failed", K(ret));
    }
    if (OB_SUCC(ret) && src_col->get_column_name_str() != columns.at(i).get_column_name_str()) {
      if (OB_FAIL(remove_col_from_name_hash_array(is_oracle_mode, src_col))) {
        LOG_WARN("failed to remove old column name from name_hash_array", K(ret));
      } else if (OB_FAIL(src_col->set_column_name(columns.at(i).get_column_name_str()))) {
        LOG_WARN("failed to change column namem", K(ret));
      } else if (OB_FAIL(rename_cols.push_back(src_col))) {
        LOG_WARN("push back element failed", K(ret));
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < rename_cols.count(); i++) {
    if (OB_FAIL(add_col_to_name_hash_array(is_oracle_mode, rename_cols.at(i)))) {
      LOG_WARN("failed to add new column name to name_hash_array", K(ret));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < src_cols.count(); i++) {
    if (OB_FAIL(src_cols.at(i)->assign(columns.at(i)))) {
      LOG_WARN("failed to assign src schema", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::reorder_column(const ObString &column_name, const bool is_first, const ObString &prev_column_name, const ObString &next_column_name)
{
  int ret = OB_SUCCESS;
  bool is_before = !next_column_name.empty();
  const bool is_after = !prev_column_name.empty();
  const int flag_cnt = static_cast<int>(is_first) + static_cast<int>(is_before) + static_cast<int>(is_after);
  ObColumnSchemaV2 *this_column = nullptr;
  ObColumnSchemaV2 *target_column = nullptr;
  int64_t this_column_id = -1;
  int64_t target_column_id = -1;
  if (OB_UNLIKELY(1 != flag_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only one of first before after is allowed", K(ret), K(flag_cnt));
  } else if (OB_ISNULL(this_column = get_column_schema(column_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get this column", K(ret), K(column_name));
  } else if (OB_FALSE_IT(this_column_id = this_column->get_column_id())) {
  } else if (!is_first) {
    const ObString &target_name = is_before ? next_column_name : prev_column_name;
    if (OB_ISNULL(target_column = get_column_schema(target_name))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, target_name.length(), target_name.ptr(),
                     table_name_.length(), table_name_.ptr());
      LOG_WARN("failed to get target table schema by name", K(ret), K(target_name));
    } else {
      target_column_id = target_column->get_column_id();
    }
  } else {
    ObColumnIterByPrevNextID iter(*this);
    const ObColumnSchemaV2 *column = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next(column))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("iterate failed", K(ret));
        }
      } else if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column", K(ret));
      } else if (!column->is_hidden() && !column->is_shadow_column()) {
        target_column_id = column->get_column_id();
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(target_column = get_column_schema(target_column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find first non-hidden non-shadow column", K(ret));
    } else {
      is_before = true; // before first column
    }
  }

  if (OB_SUCC(ret) && target_column_id != this_column_id) {
    if (OB_FAIL(delete_column_update_prev_id(this_column))) {
      LOG_WARN("failed to delete column", K(ret));
    } else {
      if (is_before) {
        this_column->set_prev_column_id(target_column->get_prev_column_id());
        this_column->set_next_column_id(target_column->get_column_id());
      } else {
        this_column->set_prev_column_id(target_column->get_column_id());
        this_column->set_next_column_id(target_column->get_next_column_id());
      }
      ObColumnSchemaV2 *prev_column = get_column_schema_by_prev_next_id(this_column->get_prev_column_id());
      ObColumnSchemaV2 *next_column = get_column_schema_by_prev_next_id(this_column->get_next_column_id());
      if (nullptr != prev_column) {
        prev_column->set_next_column_id(this_column_id);
      }
      if (nullptr != next_column) {
        next_column->set_prev_column_id(this_column_id);
      }
    }
  }
  return ret;
}

int ObTableSchema::add_aux_vp_tid(const uint64_t aux_vp_tid)
{
  int ret = OB_SUCCESS;
  bool need_add = true;
  // we are sure that index_tid are added in sorted order
  if (aux_vp_tid == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aux_vp table tid", K(ret), K(aux_vp_tid));
  } else {
    int64_t N = aux_vp_tid_array_.count();
    for (int64_t i = 0; need_add && i < N; i++) {
      if (aux_vp_tid == aux_vp_tid_array_.at(i)) {
        need_add = false;
      }
    }
  }

  if (OB_SUCC(ret) && need_add) {
    if (OB_FAIL(aux_vp_tid_array_.push_back(aux_vp_tid))) {
      LOG_WARN("fail to push back aux_vp_tid", K(aux_vp_tid_array_), K(aux_vp_tid));
    }
  }

  return ret;
}

// description: oracle mode, When the user creates an index, without explicitly declaring the index name,
//  the system will automatically generate a constraint name for it
//              Generation rules: index_name_sys_auto = tblname_OBIDX_timestamp
//              If the length of tblname exceeds 60 bytes, the first 60 bytes will be truncated as the tblname in the concatenated name
int ObTableSchema::create_idx_name_automatically_oracle(common::ObString &idx_name,
                                                        const common::ObString &table_name,
                                                        common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  ObString idx_name_str;
  ObString tmp_table_name;

  if (table_name.length() > OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN) {
    if (OB_FAIL(ob_sub_str(allocator, table_name, 0, OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN - 1, tmp_table_name))) {
      SQL_RESV_LOG(WARN, "failed to cut table to 60 byte", K(ret), K(table_name));
    }
  } else {
    tmp_table_name = table_name;
  }
  if (OB_SUCC(ret)) {
    if (snprintf(temp_str_buf, sizeof(temp_str_buf), "%.*s_OBIDX_%ld", tmp_table_name.length(), tmp_table_name.ptr(),
                 ObTimeUtility::current_time()) < 0) {
      ret = OB_SIZE_OVERFLOW;
      SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), idx_name_str))) {
      SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret));
    } else {
      idx_name = idx_name_str;
    }
  }
  return ret;
}

// description: oracle when the user creates a constraint, without explicitly declaring the constraint name,
//  the system will automatically generate a constraint name for it
//              Generation rules: pk_name_sys_auto = tblname_OBPK_timestamp
//                                check_name_sys_auto = tblname_OBCHECK_timestamp
//                                unique_name_sys_auto = tblname_OBUNIQUE_timestamp
// If the length of cst_name exceeds the max len of it, the part of table_name will be truncated unitil the len of cst_name equaling to the max len
// @param [in] cst_name
// @return oceanbase error code defined in lib/ob_errno.def
int ObTableSchema::create_cons_name_automatically(ObString &cst_name,
                                                  const ObString &table_name,
                                                  common::ObIAllocator &allocator,
                                                  ObConstraintType cst_type,
                                                  const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObSqlString cons_name_postfix_str;
  ObSqlString full_cons_name_str;
  const int64_t max_constraint_name_len = is_oracle_mode ? OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE : OB_MAX_CONSTRAINT_NAME_LENGTH_MYSQL;

  if (OB_SUCC(ret)) {
    switch (cst_type) {
      case CONSTRAINT_TYPE_PRIMARY_KEY: {
        if (OB_FAIL(cons_name_postfix_str.append_fmt("_OBPK_%ld",ObTimeUtility::current_time()))) {
          SHARE_SCHEMA_LOG(WARN, "Failed to append cons_name_postfix_str", K(ret));
        }
        break;
      }
      case CONSTRAINT_TYPE_CHECK: {
        if (OB_FAIL(cons_name_postfix_str.append_fmt("_OBCHECK_%ld", ObTimeUtility::current_time()))) {
          SHARE_SCHEMA_LOG(WARN, "Failed to append cons_name_postfix_str", K(ret));
        }
        break;
      }
      case CONSTRAINT_TYPE_UNIQUE_KEY: {
        if (OB_FAIL(cons_name_postfix_str.append_fmt("_OBUNIQUE_%ld",ObTimeUtility::current_time()))) {
          SHARE_SCHEMA_LOG(WARN, "Failed to append cons_name_postfix_str", K(ret));
        }
        break;
      }
      case CONSTRAINT_TYPE_NOT_NULL: {
        if (OB_FAIL(cons_name_postfix_str.append_fmt("_OBNOTNULL_%ld",ObTimeUtility::current_time()))) {
          SHARE_SCHEMA_LOG(WARN, "Failed to append cons_name_postfix_str", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED; // won't come here
        LOG_WARN("wrong type of ObConstraintType in this function", K(ret), K(cst_type));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t cons_name_postfix_len = cons_name_postfix_str.string().length();
    if (cons_name_postfix_len >= max_constraint_name_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong type of ObConstraintType in this function", K(ret), K(cons_name_postfix_len), K(max_constraint_name_len));
    } else if (table_name.length() + cons_name_postfix_len > max_constraint_name_len) {
      if (OB_FAIL(full_cons_name_str.append_fmt("%.*s%.*s",
                  static_cast<int32_t>(max_constraint_name_len - cons_name_postfix_len),
                  table_name.ptr(),
                  static_cast<int32_t>(cons_name_postfix_len),
                  cons_name_postfix_str.string().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to append full_cons_name_str", K(ret), K(max_constraint_name_len), K(cons_name_postfix_len));
      }
    } else {
      if (OB_FAIL(full_cons_name_str.append_fmt("%.*s%.*s",
          static_cast<int32_t>(table_name.length()), table_name.ptr(),
          static_cast<int32_t>(cons_name_postfix_len), cons_name_postfix_str.string().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to append full_cons_name_str", K(ret), K(table_name.length()), K(cons_name_postfix_len));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, full_cons_name_str.string(), cst_name))) {
      SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret), K(full_cons_name_str));
    }
  }

  return ret;
}


int ObTableSchema::create_cons_name_automatically_with_dup_check(ObString &cst_name,
                                                  const ObString &table_name,
                                                  common::ObIAllocator &allocator,
                                                  ObConstraintType cst_type,
                                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                                  const uint64_t tenant_id,
                                                  const uint64_t database_id,
                                                  const int64_t retry_times,
                                                  bool &cst_name_generated,
                                                  const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  cst_name_generated = false;
  uint64_t constraint_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i <= retry_times && !cst_name_generated; i++) {
    if (OB_FAIL(create_cons_name_automatically(cst_name, table_name, allocator, cst_type, is_oracle_mode))) {
      LOG_WARN("create constraint name failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_constraint_id(tenant_id, database_id,
                        cst_name, constraint_id))) {
      LOG_WARN("get constraint id failed", K(ret));
    } else {
      cst_name_generated = OB_INVALID_ID == constraint_id;
    }
  }
  return ret;
}

bool ObTableSchema::is_sys_generated_name(bool check_unknown) const
{
  bool bret = false;
  if (GENERATED_TYPE_SYSTEM == name_generated_type_) {
    bret = true;
  } else if (GENERATED_TYPE_UNKNOWN == name_generated_type_ && check_unknown) {
    const char *cst_type_name = is_unique_index() ? "_OBUNIQUE_" : "_OBIDX_";
    const int64_t cst_type_name_len = static_cast<int64_t>(strlen(cst_type_name));
    bret = (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN,
              table_name_.ptr(), table_name_.length(), cst_type_name, cst_type_name_len));
  } else {
    bret = false;
  }
  return bret;
}

// description: oracle mode, When the user flashbacks an indexed table, the system will automatically generate a new index name
//  for the index on the table
//              Generation rules: idx_name_flashback_auto = RECYCLE_OBIDX_timestamp
int ObTableSchema::create_new_idx_name_after_flashback(
    ObTableSchema &new_table_schema,
    common::ObString &new_idx_name,
    common::ObIAllocator &allocator,
    ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  ObString tmp_str = "RECYCLE";
  ObString temp_idx_name;
  bool is_dup_idx_name_exist = true;
  const ObSimpleTableSchemaV2* simple_table_schema = NULL;

  while (OB_SUCC(ret) && is_dup_idx_name_exist) {
    if (OB_FAIL(create_idx_name_automatically_oracle(temp_idx_name, tmp_str, allocator))) {
      LOG_WARN("create index name automatically failed", K(ret));
    } else if (OB_FAIL(build_index_table_name(allocator,
                                              new_table_schema.get_data_table_id(),
                                              temp_idx_name,
                                              new_idx_name))) {
      LOG_WARN("build_index_table_name failed", K(ret), K(new_table_schema.get_data_table_id()));
    } else if (OB_FAIL(guard.get_simple_table_schema(new_table_schema.get_tenant_id(),
                                                     new_table_schema.get_database_id(),
                                                     new_idx_name,
                                                     true,
                                                     simple_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (NULL != simple_table_schema) {
      is_dup_idx_name_exist = true;
    } else {
      is_dup_idx_name_exist = false;
    }
  }

  return ret;
}

int ObTableSchema::get_xml_hidden_column_id(const ObTableSchema *data_table_schema,
                                            const ObColumnSchemaV2 *data_column_schema,
                                            int64_t &data_column_id)
{
  INIT_SUCC(ret);
  data_column_id = -1;
  ObSEArray<ObColumnSchemaV2 *, 1> data_hidden_cols;
  if (OB_ISNULL(data_table_schema) || OB_ISNULL(data_column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schem or column schema is null", K(data_table_schema), K(data_column_schema));
  } else if (OB_FAIL(data_table_schema->get_column_schema_in_same_col_group(data_column_schema->get_column_id(),
                                                                     data_column_schema->get_udt_set_id(),
                                                                     data_hidden_cols))) {
    LOG_WARN("failed to get column schema", K(ret));
  } else if (data_hidden_cols.count() != 1 ||
             OB_ISNULL(data_hidden_cols.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get data data_hidden_cols count > 1.", K(ret), K(data_hidden_cols.count()));
  } else {
    ObColumnSchemaV2* data_hidden_col = data_hidden_cols.at(0);
    ObTableSchema::const_column_iterator iter = data_table_schema->column_begin();
    bool has_find = false;
    for ( ; OB_SUCC(ret) && !has_find && iter != data_table_schema->column_end(); ++iter) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (!column_schema->is_udt_hidden_column()) {
        // do nothing
      } else if (column_schema->get_udt_set_id() == data_hidden_col->get_udt_set_id()) {
        data_column_id = column_schema->get_column_id();
        has_find = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!has_find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The hidden column corresponding to column in the table was not found.", K(ret), K(*data_table_schema), K(*data_column_schema));
    }
  }
  return ret;
}

int ObTableSchema::find_xml_hidden_column_index(const ObTableSchema *table_schema,
                                                const ObColumnSchemaV2 *column_schema,
                                                const ObArray<ObColDesc> &desc_col_ids,
                                                int64_t &dst_index_col)
{
  INIT_SUCC(ret);
  dst_index_col = -1;
  ObSEArray<ObColumnSchemaV2 *, 1> hidden_cols;
  if (OB_ISNULL(table_schema) || OB_ISNULL(column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schem or column schema is null", K(table_schema), K(column_schema));
  } else if (OB_FAIL(table_schema->get_column_schema_in_same_col_group(column_schema->get_column_id(),
                                                                       column_schema->get_udt_set_id(),
                                                                       hidden_cols))) {
    LOG_WARN("failed to get column schema", K(ret));
  } else if (hidden_cols.count() != 1 || OB_ISNULL(hidden_cols.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get data data_hidden_cols count > 1.", K(ret), K(hidden_cols.count()));
  } else {
    bool has_find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !has_find && i < desc_col_ids.count(); i++) {
      const uint64_t hidden_column_id = desc_col_ids.at(i).col_id_;
      const ObColumnSchemaV2 *find_hidden_column_schema = table_schema->get_column_schema(hidden_column_id);
      if (OB_ISNULL(find_hidden_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get hidden col or hidden data column null", K(ret));
      } else if (!find_hidden_column_schema->is_udt_hidden_column()) {
        // do nothing
      } else if (find_hidden_column_schema->get_udt_set_id() == hidden_cols.at(0)->get_udt_set_id()) {
        dst_index_col = i;
        has_find = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!has_find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The hidden column corresponding to column in the table was not found.", K(ret), K(*table_schema), K(*column_schema));
    }
  }
  return ret;
}

void ObTableSchema::construct_partition_key_column(
    const ObColumnSchemaV2 &column,
    ObPartitionKeyColumn &partition_key_column)
{
  partition_key_column.column_id_ = column.get_column_id();
  partition_key_column.length_ = column.get_data_length();
  partition_key_column.type_ = column.get_meta_type();
}

int ObTableSchema::add_partition_key(const common::ObString &column_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = NULL;
  ObPartitionKeyColumn partition_key_column;
  if (NULL == (column = const_cast<ObColumnSchemaV2 *>(get_column_schema(column_name)))) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("fail to get column schema, return NULL", K(column_name), KR(ret));
  } else if (OB_FAIL(add_partition_key_(*column))) {
    LOG_WARN("Failed to add partition key", KR(ret), K(column_name));
  }
  return ret;
}

int ObTableSchema::add_partition_key(const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = NULL;
  ObPartitionKeyColumn partition_key_column;
  if (NULL == (column = const_cast<ObColumnSchemaV2 *>(get_column_schema(column_id)))) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("fail to get column schema, return NULL", K(column_id), KR(ret));
  } else if (OB_FAIL(add_partition_key_(*column))) {
    LOG_WARN("Failed to add partition key", KR(ret), K(column_id));
  }
  return ret;
}

int ObTableSchema::add_partition_key_(ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  ObPartitionKeyColumn partition_key_column;

  if (column.is_part_key_column()) {
    LOG_INFO("already partition key", K(column), KR(ret));
  } else if (FALSE_IT(construct_partition_key_column(column, partition_key_column))) {
  } else if (OB_FAIL(column.set_part_key_pos(partition_key_info_.get_size() + 1))) {
    LOG_WARN("Failed to set partition key position", KR(ret));
  } else if (OB_FAIL(partition_key_info_.set_column(partition_key_info_.get_size(),
                                                    partition_key_column))) {
    LOG_WARN("Failed to set partition column", KR(ret));
  } else {
    part_key_column_num_ = partition_key_info_.get_size();
  }
  return ret;
}

int ObTableSchema::add_subpartition_key(const common::ObString &column_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = NULL;
  ObPartitionKeyColumn partition_key_column;
  if (NULL == (column = const_cast<ObColumnSchemaV2 *>(get_column_schema(column_name)))) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("fail to get column schema, return NULL", K(column_name), K(ret));
  } else if (column->is_subpart_key_column()) {
    LOG_INFO("already partition key", K(column_name), K(ret));
  } else if (FALSE_IT(construct_partition_key_column(*column, partition_key_column))) {
  } else if (OB_FAIL(column->set_subpart_key_pos(subpartition_key_info_.get_size() + 1))) {
    LOG_WARN("Failed to set partition key position", K(ret));
  } else if (OB_FAIL(subpartition_key_info_.set_column(subpartition_key_info_.get_size(),
                                                       partition_key_column))) {
    LOG_WARN("Failed to set partition column", KR(ret));
  } else {
    subpart_key_column_num_ = subpartition_key_info_.get_size();
  }
  return ret;
}

int ObTableSchema::set_view_definition(const common::ObString &view_definition)
{
  return view_schema_.set_view_definition(view_definition);
}

int ObTableSchema::get_simple_index_infos(
    common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos_array) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if (OB_FAIL(simple_index_infos_array.push_back(simple_index_infos_[i]))) {
      LOG_WARN("fail to push back simple_index_infos_array", K(simple_index_infos_[i]));
    }
  }

  return ret;
}

int ObTableSchema::get_aux_vp_tid_array(ObIArray<uint64_t> &aux_vp_tid_array) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aux_vp_tid_array.assign(aux_vp_tid_array_))) {
    LOG_WARN("fail to assign aux vp tid array", K(ret));
  }
  return ret;
}

int ObTableSchema::get_aux_vp_tid_array(
    uint64_t *aux_vp_tid_array,
    int64_t &aux_vp_cnt) const
{
  int ret = OB_SUCCESS;

  const int64_t copy_cnt = aux_vp_tid_array_.count();
  if (OB_ISNULL(aux_vp_tid_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("aux_vp_tid_array is null", K(ret));
  } else if (aux_vp_cnt < copy_cnt) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf size is not enough, ", K(aux_vp_cnt), K(copy_cnt), K(ret));
  } else {
    for (int64_t i = 0; i < copy_cnt; ++i) {
      aux_vp_tid_array[i] = aux_vp_tid_array_.at(i);
    }
    aux_vp_cnt = copy_cnt;
  }
  return ret;
}

int ObTableSchema::get_default_row(
    get_default_value func,
    const common::ObIArray<ObColDesc> &column_ids,
    ObNewRow &default_row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(default_row.cells_) || default_row.count_ != column_ids.count() || column_ids.count() > column_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ",
        K(ret), K(column_cnt_), K(default_row.count_), K(column_ids.count()), "cells", default_row.cells_);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < column_cnt_; ++j) {
      ObColumnSchemaV2 *column = column_array_[j];
      if (NULL == column) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column must not null", K(ret), K(j), K(column_cnt_));
      } else if (column->get_column_id() == column_ids.at(i).col_id_) {
        if (column->is_identity_column()) {
          // Identity colunm's orig_default_value and cur_default_val are used to store sequence id
          // and desc table, it does not have the same semantics as normal default. so here we set
          // its default value as null to avoid type mismatch.
          default_row.cells_[i].set_null();
        } else {
          default_row.cells_[i] = (column->*func)();
        }
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_SYS;
      LOG_WARN("column id not found", K(ret), K(column_ids.at(i)));
    }
  }
  return ret;
}

int ObTableSchema::get_orig_default_row(const common::ObIArray<ObColDesc> &column_ids,
    common::ObNewRow &default_row) const
{
  return get_default_row(&ObColumnSchemaV2::get_orig_default_value, column_ids, default_row);
}

int ObTableSchema::get_orig_default_row(const common::ObIArray<ObColDesc> &column_ids,
                                        blocksstable::ObDatumRow &default_row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!default_row.is_valid() || default_row.count_ != column_ids.count() || column_ids.count() > column_cnt_ + 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(column_cnt_), K(default_row), K(column_ids.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    if (column_ids.at(i).col_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID ||
        column_ids.at(i).col_id_ == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
      default_row.storage_datums_[i].set_int(0);
    } else {
      bool found = false;
      for (int64_t j = 0; OB_SUCC(ret) && !found && j < column_cnt_; ++j) {
        ObColumnSchemaV2 *column = column_array_[j];
        if (NULL == column) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column must not null", K(ret), K(j), K(column_cnt_));
        } else if (column->get_column_id() == column_ids.at(i).col_id_) {
          if (OB_FAIL(default_row.storage_datums_[i].from_obj_enhance(column->get_orig_default_value()))) {
            STORAGE_LOG(WARN, "Failed to transefer obj to datum", K(ret));
          } else {
            found = true;
          }
        }
      }
      if (OB_SUCC(ret) && !found) {
        ret = OB_ERR_SYS;
        LOG_WARN("column id not found", K(ret), K(column_ids.at(i)));
      }
    }
  }
  return ret;
}

ObColumnSchemaV2* ObTableSchema::get_xml_hidden_column_parent_col_schema(uint64_t column_id, uint64_t udt_set_id) const
{
  ObColumnSchemaV2 *res = NULL;
  for (int64_t i = 0; udt_set_id > 0 && OB_ISNULL(res) && i < column_cnt_; ++i) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (OB_ISNULL(column) || !column->is_extend() || column_id == column->get_column_id()) {
    } else if (udt_set_id == column->get_udt_set_id()) {
      res = column;
    }
  }
  return res;
}

ObColumnSchemaV2* ObTableSchema::get_xml_hidden_column_schema(uint64_t column_id, uint64_t udt_set_id) const
{
  ObColumnSchemaV2 *res = NULL;
  for (int64_t i = 0; udt_set_id > 0 && OB_ISNULL(res) && i < column_cnt_; ++i) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (NULL != column && column_id != column->get_column_id()
        && udt_set_id == column->get_udt_set_id()) {
      res = column;
    }
  }
  return res;
}

int ObTableSchema::get_column_schema_in_same_col_group(uint64_t column_id, uint64_t udt_set_id,
                                                       common::ObIArray<ObColumnSchemaV2 *> &column_group) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; udt_set_id > 0 && OB_SUCC(ret) && i < column_cnt_; ++i) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (NULL == column) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column must not null", K(ret), K(i), K(column_cnt_));
    } else if (column_id == column->get_column_id()) {
      // do nothing
    } else if (udt_set_id == column->get_udt_set_id()
               && OB_FAIL(column_group.push_back(column))) {
      LOG_WARN("column must not null", K(ret), K(i), K(column_cnt_), K(column_id), K(udt_set_id));
    }
  }
  return ret;
}

ObColumnSchemaV2 *ObTableSchema::get_column_schema_by_id_internal(const uint64_t column_id) const
{
  ObColumnSchemaV2 *column = NULL;

  if (NULL != id_hash_array_) {
    if (OB_SUCCESS != id_hash_array_->get_refactored(ObColumnIdKey(column_id), column)) {
      column = NULL;
    }
  }
  return column;
}

void ObTableSchema::get_column_name_by_column_id(
    const uint64_t column_id, common::ObString &column_name, bool &is_column_exist) const
{
  is_column_exist = false;
  const ObColumnSchemaV2 *column = get_column_schema_by_id_internal(ObColumnIdKey(column_id));
  if (OB_NOT_NULL(column)) {
    column_name = column->get_column_name_str();
    is_column_exist = true;
  }
}

const ObColumnSchemaV2 *ObTableSchema::get_column_schema(const uint64_t column_id) const
{
  const ObColumnSchemaV2 *column = get_column_schema_by_id_internal(ObColumnIdKey(column_id));
  return column;
}

const ObColumnSchemaV2 *ObTableSchema::get_column_schema(uint64_t table_id, uint64_t column_id) const
{
  uint64_t col_id = column_id;
  if (has_depend_table(table_id)) {
    col_id = gen_materialized_view_column_id(column_id);
  }
  const ObColumnSchemaV2 *column = get_column_schema_by_id_internal(ObColumnIdKey(col_id));
  return column;
}

ObColumnSchemaV2 *ObTableSchema::get_column_schema(const uint64_t column_id)
{
  ObColumnSchemaV2 *column = get_column_schema_by_id_internal(ObColumnIdKey(column_id));
  return column;
}

ObColumnSchemaV2 *ObTableSchema::get_column_schema_by_name_internal(
    const ObString &column_name) const
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = NULL;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (static_cast<int64_t>(table_id_) > 0 // may be used in resolver
      && OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret));
  } else {
    lib::CompatModeGuard g(is_oracle_mode ?
                      lib::Worker::CompatMode::ORACLE :
                      lib::Worker::CompatMode::MYSQL);
    if (!column_name.empty() && NULL != name_hash_array_) {
      ObColumnSchemaHashWrapper column_name_key(column_name);
      if (OB_SUCCESS != name_hash_array_->get_refactored(column_name_key, column)) {
        column = NULL;
      }
    }
  }
  return column;
}

const ObColumnSchemaV2 *ObTableSchema::get_column_schema(const ObString &column_name) const
{
  return get_column_schema_by_name_internal(column_name);
}

ObColumnSchemaV2 *ObTableSchema::get_column_schema(const ObString &column_name)
{
  return get_column_schema_by_name_internal(column_name);
}

const ObColumnSchemaV2 *ObTableSchema::get_column_schema(const char *column_name) const
{
  const ObColumnSchemaV2 *column = NULL;
  if (NULL == column_name || '\0' == column_name[0]) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid column name, ", K(column_name));
  } else {
    column = get_column_schema(ObString::make_string(column_name));
  }
  return column;
}

ObColumnSchemaV2 *ObTableSchema::get_column_schema(const char *column_name)
{
  ObColumnSchemaV2 *column = NULL;
  if (NULL == column_name || '\0' == column_name[0]) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid column name, ", K(column_name));
  } else {
    column = get_column_schema(ObString::make_string(column_name));
  }
  return column;
}

const ObColumnSchemaV2 *ObTableSchema::get_column_schema_by_idx(const int64_t idx) const
{
  const ObColumnSchemaV2 *column = NULL;
  if (idx < 0 || idx >= column_cnt_) {
    column = NULL;
  } else {
    column = column_array_[idx];
  }
  return column;

}

ObColumnSchemaV2 *ObTableSchema::get_column_schema_by_idx(const int64_t idx)
{
  ObColumnSchemaV2 *column = NULL;
  if (idx < 0 || idx >= column_cnt_) {
    column = NULL;
  } else {
    column = column_array_[idx];
  }
  return column;
}

ObColumnSchemaV2 *ObTableSchema::get_column_schema_by_prev_next_id(const uint64_t id)
{
  ObColumnSchemaV2 *column = NULL;
  if (BORDER_COLUMN_ID == id) {
    column = NULL;
  } else {
    column = get_column_schema(id);
  }
  return column;
}

const ObColumnSchemaV2 *ObTableSchema::get_column_schema_by_prev_next_id(
      const uint64_t id) const
{
  const ObColumnSchemaV2 *column = NULL;
  if (BORDER_COLUMN_ID == id) {
    column = NULL;
  } else {
    column = get_column_schema(id);
  }
  return column;
}


uint64_t ObTableSchema::gen_materialized_view_column_id(uint64_t column_id)
{
  uint64_t mv_col_id = column_id + OB_MIN_MV_COLUMN_ID;
  return mv_col_id;
}

uint64_t ObTableSchema::get_materialized_view_column_id(uint64_t column_id)
{
  uint64_t mv_col_id = column_id - OB_MIN_MV_COLUMN_ID;
  return mv_col_id;
}

uint64_t ObTableSchema::gen_mlog_col_id_from_ref_col_id(const uint64_t column_id)
{
  uint64_t mlog_col_id = column_id;
  if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id) {
    mlog_col_id = OB_MLOG_ROWID_COLUMN_ID;
  }
  return mlog_col_id;
}

uint64_t ObTableSchema::gen_ref_col_id_from_mlog_col_id(const uint64_t column_id)
{
  uint64_t ref_col_id = column_id;
  if (OB_MLOG_ROWID_COLUMN_ID == column_id) {
    ref_col_id = OB_HIDDEN_PK_INCREMENT_COLUMN_ID;
  }
  return ref_col_id;
}

//
//  for mv, check it contains tabile_id in it's view define
//
bool ObTableSchema::has_table(uint64_t table_id) const
{
  bool has = false;
  if (is_materialized_view()) {
    for (int64_t i = 0; i < base_table_ids_.count() && !has; i++) {
      if (table_id == base_table_ids_.at(i)) {
        has = true;
      }
    }
    for (int64_t i = 0; i < depend_table_ids_.count() && !has; i++) {
      if (table_id == depend_table_ids_.at(i)) {
        has = true;
      }
    }
  }
  return has;
}

bool ObTableSchema::is_drop_index() const {
  return 0 != (index_attributes_set_ & ((uint64_t)(1) << INDEX_DROP_INDEX));
}

void ObTableSchema::set_drop_index(const uint64_t drop_index_value) {
  index_attributes_set_ &= ~((uint64_t)(1) << INDEX_DROP_INDEX);
  index_attributes_set_ |= drop_index_value << INDEX_DROP_INDEX;
}

bool ObTableSchema::is_invisible_before() const {
  return 0 != (index_attributes_set_ & ((uint64_t)1 << INDEX_VISIBILITY_SET_BEFORE));
}

void ObTableSchema::set_invisible_before(const uint64_t invisible_before) {
  index_attributes_set_ &= ~((uint64_t)(1) << INDEX_VISIBILITY_SET_BEFORE);
  index_attributes_set_ |= invisible_before << INDEX_VISIBILITY_SET_BEFORE;
}

bool ObTableSchema::has_depend_table(uint64_t table_id) const
{
  bool has = false;
  for (int64_t i = 0; i < depend_table_ids_.count() && !has; i++) {
    if (table_id == depend_table_ids_.at(i)) {
      has = true;
    }
  }
  return has;
}

// for baihua:
// Convert columns to dependent table columns
// For a given column,
// - If the table does not have mv, the returned table_id is the table_id of the table, and col_id is the incoming col_id,
// - If the table has mv, but the given column is a non-dependent table column, the returned table_id is the table_id of this table,
//  and the col_id is the passed col_id
// - If the table has mv, and the given column is a dependent column, the returned table_id is the column of the dependent table,
//  and col_id is the original column of the dependent table mapping
int ObTableSchema::convert_to_depend_table_column(
    uint64_t column_id,
    uint64_t &convert_table_id,
    uint64_t &convert_column_id) const
{
  int ret = OB_SUCCESS;
  convert_table_id = table_id_;
  convert_column_id = column_id;
  if (is_depend_column(column_id)) {
    convert_table_id = depend_table_ids_.at(0);
    convert_column_id = column_id - OB_MIN_MV_COLUMN_ID;
  }
  return ret;
}

bool ObTableSchema::is_depend_column(uint64_t column_id) const
{
  bool is_depend = false;
  if (is_materialized_view() &&
      column_id > OB_MIN_MV_COLUMN_ID &&
      column_id < OB_MIN_SHADOW_COLUMN_ID &&
      get_column_schema_by_id_internal(column_id)) {
    is_depend = true;
  }
  return is_depend;
}

const ObColumnSchemaV2 *ObTableSchema::get_fulltext_column(const ColumnReferenceSet &column_set) const
{
  const ObColumnSchemaV2 *column = NULL;
  for (const_column_iterator col_iter = column_begin();
      NULL == column && NULL != col_iter && col_iter != column_end();
      col_iter++) {
    if ((*col_iter)->is_generated_column() && (*col_iter)->is_fulltext_column()) {
      const ColumnReferenceSet *tmp_set = (*col_iter)->get_column_ref_set();
      if (tmp_set != NULL && *tmp_set == column_set) {
        column = *(col_iter);
      }
    }
  }
  return column;
}

int64_t ObTableSchema::get_column_idx(const uint64_t column_id, const bool ignore_hidden_column /* = false */ ) const
{
  int64_t ret_idx = -1;
  int64_t column_idx = 0;
  for (int64_t i = 0; i < column_cnt_ && NULL != column_array_[i]; ++i) {
    if (column_array_[i]->get_column_id() == column_id) {
      ret_idx = column_idx;
      break;
    }

    if (ignore_hidden_column && column_array_[i]->is_hidden()) {
      // When ignoring hidden columns, the number of hidden columns will not be counted
    } else {
      column_idx++;
    }
  }
  return ret_idx;
}

int64_t ObTableSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += ObSimpleTableSchemaV2::get_convert_size();
  convert_size += sizeof(ObTableSchema) - sizeof(ObSimpleTableSchemaV2);
  convert_size += tablegroup_name_.length() + 1;
  convert_size += comment_.length() + 1;
  convert_size += pk_comment_.length() + 1;
  convert_size += create_host_.length() + 1;
  convert_size += expire_info_.length() + 1;
  convert_size += parser_name_.length() + 1;
  convert_size += view_schema_.get_convert_size() - sizeof(view_schema_);
  convert_size += aux_vp_tid_array_.get_data_size();
  convert_size += base_table_ids_.get_data_size();
  convert_size += depend_table_ids_.get_data_size();
  convert_size += depend_mock_fk_parent_table_ids_.get_data_size();
  convert_size += rowkey_info_.get_convert_size() - sizeof(rowkey_info_);
  convert_size += shadow_rowkey_info_.get_convert_size() - sizeof(shadow_rowkey_info_);
  convert_size += index_info_.get_convert_size() - sizeof(index_info_);
  convert_size += partition_key_info_.get_convert_size() - sizeof(partition_key_info_);
  convert_size += subpartition_key_info_.get_convert_size() - sizeof(subpartition_key_info_);
  convert_size += get_id_hash_array_mem_size(column_cnt_);
  convert_size += get_name_hash_array_mem_size(column_cnt_);
  convert_size += label_se_column_ids_.get_data_size();

  convert_size += column_cnt_ * sizeof(ObColumnSchemaV2*);
  for (int64_t i = 0; i < column_cnt_ && NULL != column_array_[i];  ++i) {
    convert_size += column_array_[i]->get_convert_size();
  }

  convert_size += cst_cnt_ * sizeof(ObConstraint*);
  for (int64_t i = 0; i < cst_cnt_ && NULL != cst_array_[i];  ++i) {
    convert_size += cst_array_[i]->get_convert_size();
  }

  convert_size += foreign_key_infos_.get_data_size();
  for (int64_t i = 0; i < foreign_key_infos_.count(); ++i) {
    convert_size += foreign_key_infos_.at(i).get_convert_size();
  }

  convert_size += trigger_list_.get_data_size();

  convert_size += simple_index_infos_.get_data_size();
  for (int64_t i = 0; i < simple_index_infos_.count(); ++i) {
    convert_size += simple_index_infos_.at(i).get_convert_size();
  }

  convert_size += rls_policy_ids_.get_data_size();
  convert_size += rls_group_ids_.get_data_size();
  convert_size += rls_context_ids_.get_data_size();

  convert_size += external_file_format_.length() + 1;
  convert_size += external_file_location_.length() + 1;
  convert_size += external_file_location_access_info_.length() + 1;
  convert_size += external_file_pattern_.length() + 1;
  convert_size += ttl_definition_.length() + 1;
  convert_size += kv_attributes_.length() + 1;
  convert_size += index_params_.length() + 1;

  convert_size += get_hash_array_mem_size<CgIdHashArray>(column_group_cnt_);
  convert_size += get_hash_array_mem_size<CgNameHashArray>(column_group_cnt_);
  convert_size += column_group_cnt_ * sizeof(ObColumnGroupSchema*);
  for (int64_t i = 0; (i < column_group_cnt_) && OB_NOT_NULL(column_group_arr_[i]); ++i) {
    convert_size += column_group_arr_[i]->get_convert_size();
  }
  convert_size += local_session_vars_.get_deep_copy_size();
  convert_size += external_properties_.length() + 1;
  return convert_size;
}

void ObTableSchema::reset()
{
  max_used_column_id_ = 0;
  sess_active_time_ = 0;
  rowkey_column_num_ = 0;
  index_column_num_ = 0;
  rowkey_split_pos_ = 0;
  part_key_column_num_ = 0;
  subpart_key_column_num_ = 0;
  block_size_ = common::OB_DEFAULT_SSTABLE_BLOCK_SIZE;
  is_use_bloomfilter_ = false;
  progressive_merge_num_ = 0;
  tablet_size_ = OB_DEFAULT_TABLET_SIZE;
  pctfree_ = OB_DEFAULT_PCTFREE;
  autoinc_column_id_ = 0;
  auto_increment_ = 1;
  read_only_ = false;
  load_type_ = TABLE_LOAD_TYPE_IN_DISK;
  index_using_type_ = USING_BTREE;
  def_type_ = TABLE_DEF_TYPE_USER;
  charset_type_ = ObCharset::get_default_charset();
  collation_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
  code_version_ = OB_SCHEMA_CODE_VERSION;
  index_attributes_set_ = OB_DEFAULT_INDEX_ATTRIBUTES_SET;
  row_store_type_ = ObStoreFormat::get_default_row_store_type();
  store_format_ = OB_STORE_FORMAT_INVALID;
  progressive_merge_round_ = 0;
  storage_format_version_ = OB_STORAGE_FORMAT_VERSION_INVALID;
  define_user_id_ = OB_INVALID_ID;
  aux_lob_meta_tid_ = OB_INVALID_ID;
  aux_lob_piece_tid_ = OB_INVALID_ID;
  compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  reset_string(tablegroup_name_);
  reset_string(comment_);
  reset_string(pk_comment_);
  reset_string(create_host_);
  reset_string(expire_info_);
  reset_string(parser_name_);
  view_schema_.reset();

  aux_vp_tid_array_.reset();

  base_table_ids_.reset();
  depend_table_ids_.reset();
  depend_mock_fk_parent_table_ids_.reset();

  column_cnt_ = 0;
  column_array_capacity_ = 0;
  column_array_ = NULL;

  rowkey_info_.reset();
  shadow_rowkey_info_.reset();
  index_info_.reset();
  partition_key_info_.reset();
  subpartition_key_info_.reset();
  id_hash_array_ = NULL;
  name_hash_array_ = NULL;
  generated_columns_.reset();
  virtual_column_cnt_ = 0;
  micro_index_clustered_ = false;

  cst_cnt_ = 0;
  cst_array_capacity_ = 0;
  cst_array_ = NULL;
  foreign_key_infos_.reset();
  label_se_column_ids_.reset();
  simple_index_infos_.reset();
  trigger_list_.reset();
  table_dop_ = 1;
  table_flags_ = 0;

  rls_policy_ids_.reset();
  rls_group_ids_.reset();
  rls_context_ids_.reset();

  external_file_format_.reset();
  external_file_location_.reset();
  external_file_location_access_info_.reset();
  external_file_pattern_.reset();
  external_properties_.reset();
  ttl_definition_.reset();
  kv_attributes_.reset();
  index_params_.reset();
  exec_env_.reset();
  name_generated_type_ = GENERATED_TYPE_UNKNOWN;
  lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
  auto_increment_cache_size_ = 0;

  is_column_store_supported_ = false;
  max_used_column_group_id_ = COLUMN_GROUP_START_ID;
  column_group_cnt_ = 0;
  column_group_arr_capacity_ = 0;
  column_group_arr_ = NULL;
  cg_id_hash_arr_ = NULL;
  cg_name_hash_arr_ = NULL;
  mlog_tid_ = OB_INVALID_ID;
  local_session_vars_.reset();
  mv_mode_.reset();
  ObSimpleTableSchemaV2::reset();
}

int ObTableSchema::get_all_tablet_and_object_ids(ObIArray<ObTabletID> &tablet_ids,
                                                 ObIArray<ObObjectID> &partition_ids,
                                                 ObIArray<ObObjectID> *first_level_part_ids) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    OZ(tablet_ids.push_back(get_tablet_id()));
    OZ(partition_ids.push_back(get_object_id()));
    OZ(first_level_part_ids != NULL && first_level_part_ids->push_back(OB_INVALID_ID));
  } else if (OB_ISNULL(partition_array_) || partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_array is null or is empty", K(ret), KP_(partition_array), K_(partition_num));
  } else if (PARTITION_LEVEL_ONE == get_part_level()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else {
        OZ(tablet_ids.push_back(partition_array_[i]->get_tablet_id()));
        OZ(partition_ids.push_back(partition_array_[i]->get_part_id()));
        OZ(first_level_part_ids != NULL && first_level_part_ids->push_back(OB_INVALID_ID));
      }
    }
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else {
        int64_t part_id = partition_array_[i]->get_part_id();
        ObSubPartition **sub_part_array = partition_array_[i]->get_subpart_array();
        int64_t sub_part_num = partition_array_[i]->get_subpartition_num();
        if (OB_ISNULL(sub_part_array) || sub_part_num <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sub_part_array is null or is empty", K(ret), KP(sub_part_array), K(sub_part_num));
        } else {
          int64_t partition_id = OB_INVALID_ID;
          for (int64_t j = 0; OB_SUCC(ret) && j < sub_part_num; j++) {
            if (OB_ISNULL(sub_part_array[j])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null subpartition info", K(ret));
            } else {
              partition_id = sub_part_array[j]->get_sub_part_id();
              OZ(partition_ids.push_back(partition_id));
              OZ(tablet_ids.push_back(sub_part_array[j]->get_tablet_id()));
              OZ(first_level_part_ids != NULL && first_level_part_ids->push_back(partition_array_[i]->get_part_id()));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::alloc_partition(const ObPartition *&partition)
{
  int ret = OB_SUCCESS;
  partition = NULL;

  ObPartition *new_part = OB_NEWx(ObPartition, (get_allocator()));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(ret));
  } else {
    partition = new_part;
  }
  return ret;
}

int ObTableSchema::alloc_partition(const ObSubPartition *&subpartition)
{
  int ret = OB_SUCCESS;
  subpartition = NULL;

  ObSubPartition *new_part = OB_NEWx(ObSubPartition, (get_allocator()));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(ret));
  } else {
    subpartition = new_part;
  }
  return ret;
}

int ObTableSchema::is_need_padding_for_generated_column(bool &need_padding) const
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> gen_column_ids;
  if (OB_FAIL(get_generated_column_ids(gen_column_ids))) {
    LOG_WARN("get generated column ids failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < gen_column_ids.count(); ++i) {
    const ObColumnSchemaV2 *gen_col = get_column_schema(gen_column_ids.at(i));
    if (OB_ISNULL(gen_col)) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not found", K(ret), K(gen_column_ids.at(i)));
    } else if (gen_col->has_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG)) {
      need_padding = true;
    }
  }
  return ret;
}

int ObTableSchema::has_generated_column_using_udf_expr(bool &ans) const
{
  int ret = OB_SUCCESS;
  ans = false;
  ObSEArray<uint64_t, 8> gen_column_ids;
  if (OB_FAIL(get_generated_column_ids(gen_column_ids))) {
    LOG_WARN("get generated column ids failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < gen_column_ids.count(); ++i) {
    const ObColumnSchemaV2 *gen_col = get_column_schema(gen_column_ids.at(i));
    if (OB_ISNULL(gen_col)) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not found", K(ret), K(gen_column_ids.at(i)));
    } else if (gen_col->is_generated_column_using_udf()) {
      ans = true;
      break;
    }
  }
  return ret;
}

int ObTableSchema::generate_new_column_id_map(ObHashMap<uint64_t, uint64_t> &column_id_map) const
{
  int ret = OB_SUCCESS;
  uint64_t next_column_id = OB_APP_MIN_COLUMN_ID;
  ObColumnIterByPrevNextID iter(*this);
  const ObColumnSchemaV2 *column = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.next(column))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("iter failed", K(ret));
      }
    } else if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column schema", K(ret));
    } else {
      const uint64_t old_column_id = column->get_column_id();
      uint64_t new_column_id = old_column_id;
      if (OB_APP_MIN_COLUMN_ID <= old_column_id) {
        new_column_id = next_column_id;
        next_column_id += 1;
      }
      if (OB_FAIL(column_id_map.set_refactored(old_column_id, new_column_id))) {
        LOG_WARN("failed to set column id map", K(ret), K(old_column_id), K(new_column_id));
      }
    }
  }
  return ret;
}

int ObTableSchema::check_need_convert_id_hash_array(bool &need_convert_id_hash_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column cnt", K(ret), K(column_cnt_));
  } else if (nullptr == id_hash_array_) {
    // alter table schema in ddl service doesn't have id hash array
    need_convert_id_hash_array = false;
  } else if (OB_UNLIKELY(column_cnt_ != id_hash_array_->item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id hash array size", K(ret), K(column_cnt_), K(id_hash_array_->item_count()));
  } else {
    need_convert_id_hash_array = true;
  }
  return ret;
}

// convert column_array_, id_hash_array_, max_used_column_id_
int ObTableSchema::convert_basic_column_ids(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  uint64_t max_column_id = 0;
  bool need_convert_id_hash_array = false;
  if (OB_FAIL(check_need_convert_id_hash_array(need_convert_id_hash_array))) {
    LOG_WARN("failed to check if need to convert id hash array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column schema", K(ret));
    } else if (need_convert_id_hash_array && OB_FAIL(remove_col_from_id_hash_array(column))) {
      LOG_WARN("failed to remove column from id hash array", K(ret), K(*column));
    } else if (OB_FAIL(column->convert_column_id(column_id_map))) {
      LOG_WARN("failed to convert column id", K(ret), K(*column));
    } else {
      max_column_id = std::max(max_column_id, column->get_column_id());
    }
  }
  if (OB_SUCC(ret) && need_convert_id_hash_array) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
      ObColumnSchemaV2 *column = column_array_[i];
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(ret));
      } else if (OB_FAIL(add_col_to_id_hash_array(column))) {
        LOG_WARN("failed to add column to id hash array", K(ret), K(*column));
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_max_used_column_id(max_column_id);
  }
  return ret;
}

int ObTableSchema::convert_autoinc_column_id(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  const uint64_t old_autoinc_column_id = get_autoinc_column_id();
  uint64_t new_autoinc_column_id = 0;
  if (0 != old_autoinc_column_id) {
    if (OB_FAIL(column_id_map.get_refactored(old_autoinc_column_id, new_autoinc_column_id))) {
      LOG_WARN("failed to get column id", K(ret), K(old_autoinc_column_id));
    } else {
      set_autoinc_column_id(new_autoinc_column_id);
    }
  }
  return ret;
}

int ObTableSchema::convert_column_ids_in_generated_columns(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 10> generated_column_ids;
  if (OB_FAIL(get_generated_column_ids(generated_column_ids))) {
    LOG_WARN("failed to get generated column id", K(ret));
  } else {
    generated_columns_.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < generated_column_ids.count(); i++) {
    const uint64_t old_column_id = generated_column_ids.at(i);
    uint64_t new_column_id = 0;
    if (OB_FAIL(column_id_map.get_refactored(old_column_id, new_column_id))) {
      LOG_WARN("failed to get column id", K(ret), K(old_column_id));
    } else if (OB_UNLIKELY(new_column_id < OB_APP_MIN_COLUMN_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new column id too small", K(ret), K(new_column_id));
    } else if (OB_FAIL(generated_columns_.add_member(new_column_id - OB_APP_MIN_COLUMN_ID))) {
      LOG_WARN("failed to add member to generated columns", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::convert_column_ids_in_constraint(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  constraint_iterator it_end = constraint_end_for_non_const_iter();
  for (constraint_iterator it = constraint_begin_for_non_const_iter(); OB_SUCC(ret) && it != it_end; it++) {
    ObConstraint *cst = *it;
    if (OB_ISNULL(cst)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid constraint pointer", K(ret));
    } else {
      ObSEArray<uint64_t, 1> new_column_ids;
      for (ObConstraint::const_cst_col_iterator col_it = cst->cst_col_begin(); OB_SUCC(ret) && col_it != cst->cst_col_end(); col_it++) {
        const uint64_t old_column_id = *col_it;
        uint64_t new_column_id = 0;
        if (OB_FAIL(column_id_map.get_refactored(old_column_id, new_column_id))) {
          LOG_WARN("failed to get column id", K(ret));
        } else if (OB_FAIL(new_column_ids.push_back(new_column_id))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cst->assign_column_ids(new_column_ids))) {
        LOG_WARN("failed to assign column ids", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::convert_column_ids_in_info(const ObHashMap<uint64_t, uint64_t> &column_id_map, ObRowkeyInfo &rowkey_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); i++) {
    ObRowkeyColumn column;
    uint64_t new_column_id = 0;
    if (OB_FAIL(rowkey_info.get_column(i, column))) {
      LOG_WARN("failed to get column", K(ret));
    } else if (OB_FAIL(column_id_map.get_refactored(column.column_id_, new_column_id))) {
      LOG_WARN("column not found in map", K(ret));
    } else if (OB_FALSE_IT(column.column_id_ = new_column_id)) {
    } else if (OB_FAIL(rowkey_info.set_column(i, column))) {
      LOG_WARN("failed to update column", K(ret));
    }
  }
  return ret;
}

// Redistribute column ids for user columns such that they are sorted by column id in prev next list.
// Note that the column array won't changed during this function.
int ObTableSchema::convert_column_ids_for_ddl(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(convert_column_udt_set_ids(column_id_map))) {
    LOG_WARN("failed to convert column udt set id", K(ret));
  } else if (OB_FAIL(convert_geo_generated_col_ids(column_id_map))) {
    LOG_WARN("failed to convert column id in geo generated columns", K(ret));
  } else if (OB_FAIL(convert_basic_column_ids(column_id_map))) {
    LOG_WARN("failed to convert column id in column array and id hash array", K(ret));
  } else if (OB_FAIL(convert_column_ids_in_generated_columns(column_id_map))) {
    LOG_WARN("failed to convert column id in generated columns", K(ret));
  } else if (OB_FAIL(convert_column_ids_in_constraint(column_id_map))) {
    LOG_WARN("failed to convert column id in constraint", K(ret));
  } else if (OB_FAIL(convert_autoinc_column_id(column_id_map))) {
    LOG_WARN("failed to convert auto inc column id", K(ret));
  } else if (OB_FAIL(convert_column_ids_in_info(column_id_map, rowkey_info_))) {
    LOG_WARN("failed to convert column id in rowkey info", K(ret));
  } else if (OB_FAIL(convert_column_ids_in_info(column_id_map, shadow_rowkey_info_))) {
    LOG_WARN("failed to convert column id in shadow rowkey info", K(ret));
  } else if (OB_FAIL(convert_column_ids_in_info(column_id_map, partition_key_info_))) {
    LOG_WARN("failed to convert column id in part key info", K(ret));
  } else if (OB_FAIL(convert_column_ids_in_info(column_id_map, subpartition_key_info_))) {
    LOG_WARN("failed to convert column id in sub part key info", K(ret));
  } else {
    // index, foreign key will be converted when manually rebuilt
  }
  return ret;
}

int ObTableSchema::sort_column_array_by_column_id()
{
  int ret = OB_SUCCESS;
  if (nullptr == column_array_ || column_cnt_ <= 0) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
      if (OB_ISNULL(column_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(column_array_, column_array_ + column_cnt_, [](ObColumnSchemaV2 *&lhs, ObColumnSchemaV2 *&rhs) -> bool {
        return lhs->get_column_id() < rhs->get_column_id();
      });
    }
  }
 return ret;
}

int ObTableSchema::check_column_array_sorted_by_column_id(const bool skip_rowkey) const
{
  int ret = OB_SUCCESS;
  if (nullptr == column_array_ || column_cnt_ <= 0) {
    // do nothing
  } else {
    int64_t max_column_id = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
      const ObColumnSchemaV2 *column = nullptr;
      if (OB_ISNULL(column = column_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column", K(ret), K(i));
      } else if (skip_rowkey && column->is_rowkey_column()) {
        // skip
      } else if (OB_UNLIKELY(column->get_column_id() <= max_column_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column array not sorted by column id", K(ret));
      } else {
        max_column_id = column->get_column_id();
      }
    }
  }
  return ret;
}

int ObTableSchema::is_unique_key_column(ObSchemaGetterGuard &schema_guard,
                                        uint64_t column_id,
                                        bool &is_uni) const
{
  // This interface is compatible with MySQL.
  // For table t1(c1 int, c2 int, UNIQUE u1(c1, c2)), the
  // show columns query result is as following.
  // > show columns from t1;
  // +-------+---------+------+-----+---------+-------+
  // | Field | Type    | Null | Key | Default | Extra |
  // +-------+---------+------+-----+---------+-------+
  // | c1    | int(11) | YES  | MUL | NULL    |       |
  // | c2    | int(11) | YES  |     | NULL    |       |
  // +-------+---------+------+-----+---------+-------+
  // Only column within a single column unique index is considered
  // as UNI key.
  // So we cannot use this interface to judge whether one column
  // is really an unique index column.
  int ret = OB_SUCCESS;
  is_uni = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(get_tenant_id(),
                                                simple_index_infos.at(i).table_id_,
                                                index_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_UNLIKELY(NULL == index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index schema from schema guard is NULL", K(ret), K(index_schema));
      } else if (index_schema->is_unique_index() && 1 == index_schema->get_index_column_num()) {
        const ObIndexInfo &index_info = index_schema->get_index_info();
        uint64_t idx_col_id = OB_INVALID_ID;
        if (OB_FAIL(index_info.get_column_id(0, idx_col_id))) {
          LOG_WARN("get index column id fail", K(ret));
        } else if (column_id == idx_col_id) {
          is_uni = true;
        } else {/*do nothing*/}
      } else {/*do nothing*/}
    } // for
  }
  return ret;
}

int ObTableSchema::is_multiple_key_column(ObSchemaGetterGuard &schema_guard,
                                          uint64_t column_id,
                                          bool &is_mul) const
{
  int ret = OB_SUCCESS;
  is_mul = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  } else {
    // Both cases will cause a column to be displayed as MUL
     // 1. The first column of the non-unique index
     // 2. When there are multiple columns in the unique index, the first column
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema =  NULL;
      if (OB_FAIL(schema_guard.get_table_schema(get_tenant_id(),
                                                simple_index_infos.at(i).table_id_,
                                                index_schema))) {
        SERVER_LOG(WARN, "fail to get table schema", K(ret));
      } else if (OB_UNLIKELY(NULL == index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index schema from schema guard is NULL", K(ret), K(index_schema));
      } else if ((index_schema->is_unique_index() && 1 < index_schema->get_index_column_num()) ||
                 index_schema->is_normal_index()) {
        const ObIndexInfo &index_info = index_schema->get_index_info();
        uint64_t idx_col_id = OB_INVALID_ID;
        if (OB_FAIL(index_info.get_column_id(0, idx_col_id))) {
          LOG_WARN("get index column id fail", K(ret));
        } else if (column_id == idx_col_id) {
          is_mul = true;
        } else {/*do nothing*/}
      } else {/*do nothing*/}
    } // for
  }
  return ret;
}

int ObTableSchema::add_col_to_id_hash_array(ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int hash_ret = 0;
  int64_t id_hash_array_mem_size = 0;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else {
    if (NULL == id_hash_array_) {
      id_hash_array_mem_size = get_id_hash_array_mem_size(get_column_count());
      // reserve size equals to 2 * column_cnt_, if column_cnt_ == 0, array size equals to 2 * 16
      if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for id_hash_array, ", K(id_hash_array_mem_size));
      } else if (NULL == (id_hash_array_ = new (buf) IdHashArray(id_hash_array_mem_size))){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to new id_hash_array.");
      } else {
        if (OB_SUCCESS != (hash_ret = id_hash_array_->set_refactored(ObColumnIdKey(column->get_column_id()),
                                                                column))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("Fail to set column to id_hash_array, ", K(hash_ret));
        }
      }
    } else if (OB_SUCCESS
        != (hash_ret = id_hash_array_->set_refactored(ObColumnIdKey(column->get_column_id()), column))) {
      if (OB_HASH_FULL == hash_ret) {
        id_hash_array_mem_size = get_id_hash_array_mem_size(id_hash_array_->count() * 2);
        // if reserved size is not enough, alloc two times more memory
        if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_mem_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to alloc memory", K(id_hash_array_mem_size), K(ret));
        } else {
          IdHashArray *new_array = new (buf) IdHashArray(id_hash_array_mem_size);
          if (NULL == new_array) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Fail to new IdHashArray", K(ret));
          }
          for (IdHashArray::Iterator iter = id_hash_array_->begin();
            OB_SUCC(ret) && iter != id_hash_array_->end(); ++iter) {
            if (OB_FAIL(new_array->set_refactored(id_hash_array_->get_key(iter), *iter))) {
              LOG_WARN("fail to set refactored", K(ret), K(*iter));
            }
          }
          if (OB_SUCC(ret)) {
            ObColumnIdKey key(column->get_column_id());
            if (OB_SUCCESS != (hash_ret = new_array->set_refactored(key, column))) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("Fail to set column to id_hash_array, ", K(hash_ret));
            } else {
              // free old id_hash_array_
              free(id_hash_array_);
              id_hash_array_ = new_array;
            }
          }
        }
      } else {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("Fail to set column to id_hash_array, ", K(hash_ret), K(column->get_column_id()), K(id_hash_array_), K(column));
      }
    }
  }

  return ret;
}

int ObTableSchema::remove_col_from_id_hash_array(const ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column_tmp = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else if (NULL == id_hash_array_) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("id hash array is NULL", K(ret));
  } else {
    ObColumnIdKey key(column->get_column_id());
    if (OB_SUCCESS != id_hash_array_->get_refactored(key, column_tmp)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("The column does not exist in id hash array", K(ret));
    } else if (OB_SUCCESS != id_hash_array_->erase_refactored(key)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to erase column id from id hash array", K(ret));
    } else {/*do nothing*/}
  }

  return ret;
}

int ObTableSchema::add_col_to_name_hash_array(
    const bool is_oracle_mode,
    ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int hash_ret = 0;
  int64_t name_hash_array_mem_size = 0;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else {
    // In some scenarios, the tenant id is not initialized when add_column, 4002 will be reported here,
    // and the error code will not be processed temporarily.
    lib::CompatModeGuard g(is_oracle_mode ?
                      lib::Worker::CompatMode::ORACLE :
                      lib::Worker::CompatMode::MYSQL);
    ObColumnSchemaHashWrapper column_name_key(column->get_column_name_str());
    if (NULL == name_hash_array_) {
      name_hash_array_mem_size = get_name_hash_array_mem_size(get_column_count());
      if (NULL == (buf = static_cast<char*>(alloc(name_hash_array_mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory, ", K(name_hash_array_mem_size), K(ret));
      } else if (NULL == (name_hash_array_ = new (buf) NameHashArray(name_hash_array_mem_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to new NameHashArray", K(ret));
      } else {
        ObColumnSchemaV2 **column_ptr = name_hash_array_->get(column_name_key);
        if (NULL != column_ptr && NULL != *column_ptr) {
          ret = OB_ERR_COLUMN_DUPLICATE;
          LOG_WARN("Column already exist!", "column_name", column->get_column_name_str(), K(ret));
        } else if (OB_SUCCESS != (hash_ret = name_hash_array_->set_refactored(column_name_key, column))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("Fail to set column to name_hash_array, ", K(hash_ret), K(ret));
        }
      }
    } else if (OB_SUCCESS != (hash_ret = name_hash_array_->set_refactored(column_name_key, column))) {
      if (OB_HASH_FULL == hash_ret) {
        name_hash_array_mem_size = get_id_hash_array_mem_size(name_hash_array_->count() * 2);
        if (NULL == (buf = static_cast<char*>(alloc(name_hash_array_mem_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory, ", K(name_hash_array_mem_size), K(ret));
        } else {
          NameHashArray *new_array = new (buf) NameHashArray(name_hash_array_mem_size);
          if (NULL == new_array) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Fail to new NameHashArray", K(ret));
          }
          for (NameHashArray::Iterator iter = name_hash_array_->begin();
            OB_SUCC(ret) && iter != name_hash_array_->end(); ++iter) {
            if (OB_FAIL(new_array->set_refactored(name_hash_array_->get_key(iter), *iter))) {
              LOG_WARN("fail to set name hash array", K(ret), K(*iter));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_SUCCESS != (hash_ret = new_array->set_refactored(column_name_key, column))) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("Fail to set column to name_hash_array, ", K(hash_ret), K(ret));
            } else {
              // free old name_hash_array_
              free(name_hash_array_);
              name_hash_array_ = new_array;
            }
          }
        }
      } else if (hash_ret == OB_HASH_EXIST){
        ret = OB_ERR_COLUMN_DUPLICATE;
        LOG_WARN("duplicate column name", "column_name", column->get_column_name_str());
      } else {
        ret = hash_ret;
        LOG_WARN("Fail to set column to name_hash_array, ", K(hash_ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::remove_col_from_name_hash_array(
    const bool is_oracle_mode,
    const ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column_tmp = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else if (NULL == name_hash_array_) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("name hash array is NULL", K(ret));
  } else {
    // Tenant id is not initialized when add_column in some scenarios
    lib::CompatModeGuard g(is_oracle_mode ?
                      lib::Worker::CompatMode::ORACLE :
                      lib::Worker::CompatMode::MYSQL);
    ObColumnSchemaHashWrapper column_name_key(column->get_column_name());
    if (OB_SUCCESS != name_hash_array_->get_refactored(column_name_key, column_tmp)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("The column is not exist in name hash array", K(ret));
    } else if (OB_SUCCESS != name_hash_array_->erase_refactored(column_name_key)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to erase column id from name hash array", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::add_col_to_column_array(ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else {
    if (0 == column_array_capacity_) {
      if (NULL == (column_array_ = static_cast<ObColumnSchemaV2**>(
          alloc(sizeof(ObColumnSchemaV2*) * DEFAULT_ARRAY_CAPACITY)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for column_array_.");
      } else {
        column_array_capacity_ = DEFAULT_ARRAY_CAPACITY;
        MEMSET(column_array_, 0, sizeof(ObColumnSchemaV2*) * DEFAULT_ARRAY_CAPACITY);
      }
    } else if (column_cnt_ >= column_array_capacity_) {
      int64_t tmp_size = 2 * column_array_capacity_;
      ObColumnSchemaV2 **tmp = NULL;
      if (NULL == (tmp = static_cast<ObColumnSchemaV2**>(
          alloc(sizeof(ObColumnSchemaV2*) * tmp_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for column_array_, ", K(tmp_size), K(ret));
      } else {
        MEMCPY(tmp, column_array_, sizeof(ObColumnSchemaV2*) * column_array_capacity_);
        // free old column_array_
        free(column_array_);
        column_array_ = tmp;
        column_array_capacity_ = tmp_size;
      }
    }

    if (OB_SUCC(ret)) {
      column_array_[column_cnt_++] = column;
    }
  }

  return ret;
}

int ObTableSchema::remove_col_from_column_array(const ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *tmp_column = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else if (NULL == (tmp_column = get_column_schema(column->get_column_id()))) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("The column does not exist", K(ret));
  } else {
    int64_t i = 0;
    for (; i < column_cnt_ && tmp_column != column_array_[i]; ++i) {
      ;
    }
    for (; i < column_cnt_ - 1; ++i) {
      column_array_[i] = column_array_[i+1];
    }
  }
  return ret;
}

int ObTableSchema::delete_column_internal(ObColumnSchemaV2 *column_schema, const bool for_view)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column schema is NULL", K(ret));
  } else if (!column_schema->is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The column schema has error", K(ret));
  } else if (table_id_ != column_schema->get_table_id()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The column schema does not belong to this table", K(ret));
  } else if (!is_view_table() && !is_user_table() && !is_index_table() && !is_tmp_table()
             && !is_sys_table() && !is_aux_vp_table() && !is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Only NORMAL table and index table and SYSTEM table and view table are allowed", K(ret));
  } else if (!for_view && ((!is_heap_table() && column_cnt_ <= MIN_COLUMN_COUNT_WITH_PK_TABLE)
            || (is_heap_table() && column_cnt_ <=
              ((column_schema->is_rowkey_column() && column_schema->is_hidden()) ? MIN_COLUMN_COUNT_WITH_HEAP_TABLE - 1 :
              MIN_COLUMN_COUNT_WITH_HEAP_TABLE)))) {
    ret = OB_CANT_REMOVE_ALL_FIELDS;
    LOG_USER_ERROR(OB_CANT_REMOVE_ALL_FIELDS);
    LOG_WARN("Can not delete all columns in table", K(ret));
  } else {
    bool is_oracle_mode = lib::is_oracle_mode();
    if (static_cast<int64_t>(table_id_) > 0 // may be used in resolver
        && OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret));
    } else if (!for_view && OB_FAIL(delete_column_update_prev_id(column_schema))) {
      LOG_WARN("Failed to update column previous id", K(ret));
    } else if (OB_FAIL(remove_col_from_column_array(column_schema))) {
      LOG_WARN("Failed to remove col from column array", K(ret));
    } else if (OB_FAIL(remove_col_from_id_hash_array(column_schema))) {
      LOG_WARN("Failed to remove col from id hash array", K(ret));
    } else if (OB_FAIL(remove_col_from_name_hash_array(is_oracle_mode, column_schema))) {
      LOG_WARN("Failed to remove col from name hash array", K(ret));
    } else if (column_schema->is_label_se_column()
               && OB_FAIL(remove_column_id_from_label_se_array(column_schema->get_column_id()))){
      LOG_WARN("Failed to remove column id from label security array", K(ret));
    } else if (column_schema->is_generated_column()
              && OB_FAIL(generated_columns_.del_member(column_schema->get_column_id() - common::OB_APP_MIN_COLUMN_ID)) ) {
      LOG_WARN("Failed to remove column from generated columns", K(ret));
    } else {
      --column_cnt_;
      if (column_schema->is_autoincrement()) {
        autoinc_column_id_ = 0;
      }
      if (column_schema->is_virtual_generated_column()) {
        --virtual_column_cnt_;
      }
      free(column_schema);
      column_schema = NULL;
    }
  }

  return ret;
}

bool ObTableSchema::is_same_type_category(
                   const ObColumnSchemaV2 &src_column,
                   const ObColumnSchemaV2 &dst_column) const
{
  bool ret_bool = false;
  const ObObjMeta src_meta = src_column.get_meta_type();
  const ObObjMeta dst_meta = dst_column.get_meta_type();
  const ColumnTypeClass src_col_type_class = src_column.get_data_type_class();
  const ColumnTypeClass dst_col_type_class = dst_column.get_data_type_class();
  if ((src_meta.is_integer_type() && dst_meta.is_integer_type()) || //integer
     (ObNumberTC == src_col_type_class && ObNumberTC == dst_col_type_class) || //number
     (src_meta.is_string_type() && dst_meta.is_string_type()) || //string,text,binary
     ((ObDateTimeTC == src_col_type_class || ObDateTC == src_col_type_class ||
     ObTimeTC == src_col_type_class || ObYearTC == src_col_type_class ||
     ObOTimestampTC == src_col_type_class) &&
     (ObDateTimeTC == dst_col_type_class || ObDateTC == dst_col_type_class ||
     ObTimeTC == dst_col_type_class || ObYearTC == dst_col_type_class ||
     ObOTimestampTC == dst_col_type_class)) || // time
     (ObIntervalTC == src_col_type_class && ObIntervalTC == dst_col_type_class) ||
     (ObDecimalIntTC == src_col_type_class && ObDecimalIntTC == dst_col_type_class) || //decimal int
     (ObNumberTC == src_col_type_class && ObDecimalIntTC == dst_col_type_class) || // decimal int and number
     (ObDecimalIntTC == src_col_type_class && ObNumberTC == dst_col_type_class)) {
    ret_bool = true;
  }
  if (src_meta.get_type() == ObTinyTextType && dst_meta.is_lob_storage()) {
    ret_bool = false;
  }
  return ret_bool;
}

int ObTableSchema::check_alter_column_in_foreign_key(const ObColumnSchemaV2 &src_column,
                                                     const ObColumnSchemaV2 &dst_column,
                                                     const bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  ColumnType src_col_type = src_column.get_data_type();
  ColumnType dst_col_type = dst_column.get_data_type();
  const ObAccuracy &src_accuracy = src_column.get_accuracy();
  const ObAccuracy &dst_accuracy = dst_column.get_accuracy();
  if (is_column_in_foreign_key(src_column.get_column_id())) {
    char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
    if (is_oracle_mode) {
    // in oracle mode, only VARCHAR or NVARCHAR can be changed to large or small, and other types are not supported
      if (src_col_type != dst_col_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter the type of foreign key columns");
      } else {
        if (!src_column.get_meta_type().is_varying_len_char_type() ||
            !dst_column.get_meta_type().is_varying_len_char_type()) {
          ret = OB_NOT_SUPPORTED;
          (void)snprintf(err_msg, sizeof(err_msg), "Alter the precision of foreign key columns,"
          "column type %s", ob_obj_type_str(src_col_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
        }
      }
    } else {
    // if the column type class is ObFloatTC or ObDoubleTC, which supports changing the precision
    // if the column type class is VARCHAR, which supports changing to a larger size, but does
    // not support changing to a smaller size
      if (dst_column.get_data_type_class() == ObFloatTC ||
          dst_column.get_data_type_class() == ObDoubleTC) {
        if (src_column.get_meta_type().is_float() || src_column.get_meta_type().is_double()) {
          dst_col_type = dst_accuracy.get_precision() >= 25 ? ObDoubleType : ObFloatType;
          src_col_type = src_accuracy.get_precision() >= 25 ? ObDoubleType : ObFloatType;
        } else {
          dst_col_type = dst_accuracy.get_precision() >= 25 ? ObUDoubleType : ObUFloatType;
          src_col_type = src_accuracy.get_precision() >= 25 ? ObUDoubleType : ObUFloatType;
        }
      }
      if (src_col_type != dst_col_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter the type of foreign key columns");
      } else {
        if (src_column.get_data_type_class() != ObFloatTC &&
            src_column.get_data_type_class() != ObDoubleTC &&
            (!src_column.get_meta_type().is_varchar() ||
            dst_accuracy.get_length() < src_accuracy.get_length())) {
          ret = OB_NOT_SUPPORTED;
          (void)snprintf(err_msg, sizeof(err_msg), "Alter the precision of foreign key columns,"
          "column type %s", ob_obj_type_str(src_col_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::convert_char_to_byte_semantics(const ObColumnSchemaV2 *col_schema,
                                                  const bool is_oracle_mode,
                                                  int32_t &col_byte_len) const
{
  int ret = OB_SUCCESS;
  col_byte_len = col_schema->get_data_length();
  if (col_schema->get_meta_type().is_character_type()) {
    int64_t mbmaxlen = 0;
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(
                col_schema->get_collation_type(), mbmaxlen))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mbmaxlen", K(ret), K(col_schema->get_collation_type()));
    } else if (0 >= mbmaxlen) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mbmaxlen is less than 0", K(ret), K(mbmaxlen));
    } else {
      if (!is_oracle_byte_length(is_oracle_mode, col_schema->get_length_semantics())) {
        col_byte_len = static_cast<int32_t>(col_byte_len * mbmaxlen);
      }
    }
  }
  return ret;
}

int ObTableSchema::check_alter_column_accuracy(const ObColumnSchemaV2 &src_column,
                                              ObColumnSchemaV2 &dst_column,
                                              const int32_t src_col_byte_len,
                                              const int32_t dst_col_byte_len,
                                              const bool is_oracle_mode,
                                              bool &is_offline) const
{
  int ret = OB_SUCCESS;
  const ColumnType src_col_type = src_column.get_data_type();
  const ColumnType dst_col_type = dst_column.get_data_type();
  const ObAccuracy &src_accuracy = src_column.get_accuracy();
  const ObAccuracy &dst_accuracy = dst_column.get_accuracy();
  const ObObjMeta &src_meta = src_column.get_meta_type();
  const ObObjMeta &dst_meta = dst_column.get_meta_type();
  if (src_column.get_data_type() == dst_column.get_data_type()) {
    bool is_type_reduction = false;
    // In ObAccuracy, precision and length_semantics are union data structure, so when you change
    // varchar2(m byte) to varchar2(m char), the precision you get from ObAccuracy is an invalid value
    // because the length_semantics of byte is 2, the length_semantics of char is 1. this will lead to misjudgment
    // so, if it is a string type, length must be used to compare.
    if (ob_is_number_or_decimal_int_tc(src_col_type)) {
      if (ObAccuracy::is_default_number(src_accuracy) && !ObAccuracy::is_default_number(dst_accuracy)) {
        is_type_reduction = true;
      } else if (!ObAccuracy::is_default_number(src_accuracy) && ObAccuracy::is_default_number(dst_accuracy)) {
        const int64_t m1 = src_accuracy.get_fixed_number_precision();
        const int64_t d1 = src_accuracy.get_fixed_number_scale();
        is_type_reduction = (m1 - d1 > OB_MAX_NUMBER_PRECISION);
      } else if (!ObAccuracy::is_default_number(src_accuracy) && !ObAccuracy::is_default_number(dst_accuracy)) {
        const int64_t m1 = src_accuracy.get_fixed_number_precision();
        const int64_t d1 = src_accuracy.get_fixed_number_scale();
        const int64_t m2 = dst_accuracy.get_fixed_number_precision();
        const int64_t d2 = dst_accuracy.get_fixed_number_scale();
        is_type_reduction = !(d1 <= d2 && m1 - d1 <= m2 - d2);
      } else {
        // both are default number
      }
    } else if ((!src_column.is_string_type() && !src_meta.is_integer_type() &&
              (src_accuracy.get_precision() > dst_accuracy.get_precision() ||
              src_accuracy.get_scale() > dst_accuracy.get_scale()))
            || ((src_column.is_string_type() || src_column.is_raw() || ob_is_rowid_tc(src_col_type)) &&
              src_col_byte_len > dst_col_byte_len)) {
      is_type_reduction = true;
    }
    if (is_oracle_mode) {
      if (ob_is_float_tc(src_col_type)
       || ob_is_double_tc(src_col_type)
       || src_meta.is_datetime()
       || src_meta.is_blob()
       || src_meta.is_clob()) {
         // online, do nothing
      } else if (is_type_reduction) {
        if (src_meta.is_varchar() || src_meta.is_nvarchar2()
            || src_meta.is_urowid() || src_meta.is_raw()) {
          is_offline = true;
        } else {
          ret = OB_ERR_DECREASE_COLUMN_LENGTH;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Can not decrease precision or scale");
        }
      } else {
        // increase column length
        if (ob_is_number_tc(src_col_type) || src_meta.is_char() || src_meta.is_varchar()
         || src_meta.is_nvarchar2() || src_meta.is_raw() || src_meta.is_json()
         || src_meta.is_timestamp_nano() || src_meta.is_timestamp_tz()
         || src_meta.is_timestamp_ltz() || src_meta.is_interval_ym()
         || src_meta.is_interval_ds() || src_meta.is_urowid()) {
          // online, do nothing
        } else if (src_meta.is_nchar()) {
          is_offline = true;
        } else if (src_meta.is_decimal_int()) {
          // if scale or decimal int width is not equal, offline ddl is need.
          is_offline = (dst_accuracy.get_scale() != src_accuracy.get_scale()) ||
            (get_decimalint_type(dst_accuracy.get_precision()) != get_decimalint_type(src_accuracy.get_precision()));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Can not increase precision or scale");
        }
      }
    } else {
      // in mysql mode
      if (src_meta.is_date()
       || src_meta.is_year()) {
         // online, do nothing
      } else if (ObEnumSetTC == src_column.get_data_type_class()) {
        bool is_incremental = true;
        if (src_column.get_extended_type_info().count() >
            dst_column.get_extended_type_info().count()) {
          is_offline = true;
        } else if (src_column.get_collation_type() != dst_column.get_collation_type()) {
          is_offline = true;
        } else if (OB_FAIL(ObDDLResolver::check_type_info_incremental_change(
                   src_column, dst_column, is_incremental))) {
          LOG_WARN("failed to check type info incremental change", K(ret));
        } else if (!is_incremental) {
          is_offline = true;
        }
      } else if (is_type_reduction) {
        is_offline = true;
      } else {
        // increase column length
        if (ob_is_number_tc(src_col_type) || src_meta.is_bit() || src_meta.is_char()
         || src_meta.is_varchar() || src_meta.is_varbinary() || src_meta.is_text()
         || src_meta.is_blob() || src_meta.is_timestamp() || src_meta.is_datetime()
         || src_meta.is_integer_type() || src_meta.is_json()) {
           // online, do nothing
        } else if (ob_is_decimal_int_tc(src_col_type)
                     && dst_accuracy.get_scale() == src_accuracy.get_scale()
                     && (get_decimalint_type(dst_accuracy.get_precision())
                          == get_decimalint_type(src_accuracy.get_precision()))) {
          // online, do nothing
        } else {
          is_offline = true;
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::check_alter_column_type(const ObColumnSchemaV2 &src_column,
                                           ObColumnSchemaV2 &dst_column,
                                           const int32_t src_col_byte_len,
                                           const int32_t dst_col_byte_len,
                                           const bool is_oracle_mode,
                                           bool &is_offline) const
{
  int ret = OB_SUCCESS;
  const ColumnType src_col_type = src_column.get_data_type();
  const ColumnType dst_col_type = dst_column.get_data_type();
  const ObAccuracy &src_accuracy = src_column.get_accuracy();
  const ObAccuracy &dst_accuracy = dst_column.get_accuracy();
  const ObObjMeta &src_meta = src_column.get_meta_type();
  const ObObjMeta &dst_meta = dst_column.get_meta_type();
  if (src_column.get_data_type() != dst_column.get_data_type()) {
    char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
    bool is_same_category = is_same_type_category(src_column, dst_column);
    bool is_type_reduction = false;
    // In ObAccuracy, precision and length_semantics are union data structure, so when you change
    // varchar2(m byte) to varchar2(m char), the precision you get from ObAccuracy is an invalid value
    // because the length_semantics of byte is 2, the length_semantics of char is 1. this will lead to misjudgment
    // so, if it is a string type, length must be used to compare.
    // The number type does not specify precision, which means that it is the largest range and requires special judgment
    if (ob_is_number_or_decimal_int_tc(src_col_type) && ob_is_number_or_decimal_int_tc(dst_col_type)) {
      is_type_reduction = true;
      if (src_meta.is_number() || src_meta.is_decimal_int()) {
        if (dst_meta.is_unumber()) {
          // is_type_reduction = true;
        } else if (dst_meta.is_number_float()) {
          if (ObAccuracy::is_default_number(src_accuracy)) {
            // is_type_reduction = true;
          } else {
            const int64_t m1 = src_accuracy.get_fixed_number_precision();
            const int64_t d1 = src_accuracy.get_fixed_number_scale();
            is_type_reduction = static_cast<int64_t>(std::ceil(dst_accuracy.get_precision() * OB_PRECISION_BINARY_TO_DECIMAL_FACTOR)) < m1 - d1;
          }
        } else if ((src_meta.is_decimal_int() && dst_meta.is_number()) ||
                    (src_meta.is_number() && dst_meta.is_decimal_int())) {
          if (dst_meta.is_number() && ObAccuracy::is_default_number(dst_accuracy)) {
            is_type_reduction = false;
          } else if (dst_meta.is_decimal_int() && ObAccuracy::is_default_number(src_accuracy)) {
            // is_type_reduction = true;
          } else {
            const int64_t m1 = src_accuracy.get_fixed_number_precision();
            const int64_t d1 = src_accuracy.get_fixed_number_scale();
            const int64_t m2 = dst_accuracy.get_fixed_number_precision();
            const int64_t d2 = dst_accuracy.get_fixed_number_scale();
            is_type_reduction = !(d1 <= d2 && m1 - d1 <= m2 - d2);
          }
        }
      } else if (src_meta.is_unumber()) {
        if (dst_meta.is_number() || dst_meta.is_decimal_int()) {
          if (ObAccuracy::is_default_number(src_accuracy)) {
            // is_type_reduction = true;
          } else {
            const int64_t m1 = src_accuracy.get_fixed_number_precision();
            const int64_t d1 = src_accuracy.get_fixed_number_scale();
            const int64_t m2 = dst_accuracy.get_fixed_number_precision();
            const int64_t d2 = dst_accuracy.get_fixed_number_scale();
            is_type_reduction = !(d1 <= d2 && m1 - d1 <= m2 - d2);
          }
        } else if (dst_meta.is_number_float()) {
          // is_type_reduction = true;
        }
      } else if (src_meta.is_number_float()) {
        if (dst_meta.is_number() || dst_meta.is_decimal_int()) {
          is_type_reduction = !ObAccuracy::is_default_number(dst_accuracy);
        } else if (dst_meta.is_unumber()) {
          // is_type_reduction = true;
        }
      }
    } else if ((!src_column.is_string_type() &&
        (src_accuracy.get_precision() > dst_accuracy.get_precision() ||
        src_accuracy.get_scale() > dst_accuracy.get_scale()))
      || (src_column.is_string_type() &&
        src_col_byte_len > dst_col_byte_len)) {
      is_type_reduction = true;
    }
    if (is_same_category) {
      if (is_oracle_mode) {
        if ((src_meta.is_char() && dst_meta.is_nchar())
            || (src_meta.is_varchar() && dst_meta.is_nchar())
            || (src_meta.is_nvarchar2() && dst_meta.is_nchar())
            || (src_meta.is_datetime() && dst_meta.is_timestamp_nano())
            || (src_meta.is_datetime() && dst_meta.is_timestamp_ltz())
            || (src_meta.is_timestamp_nano() && dst_meta.is_datetime())
            || (src_meta.is_timestamp_ltz() && dst_meta.is_datetime())) {
           is_offline = true;
        } else if (is_type_reduction) {
          if ((src_meta.is_varchar() && dst_meta.is_char())) {
            is_offline = true;
          } else if (ob_is_number_tc(src_col_type) && !src_meta.is_number_float()
                        && dst_meta.is_decimal_int()) {
            ret = OB_ERR_DECREASE_COLUMN_LENGTH;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Can not decrease precision or scale");
          } else {
            ret = OB_NOT_SUPPORTED;
            (void)snprintf(err_msg, sizeof(err_msg), "Can not decrease precision or scale, src column type %s,"
            "dst column type %s", ob_obj_type_str(src_col_type), ob_obj_type_str(dst_col_type));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
          }
        } else {
          // increase column length
          if (ob_is_number_tc(src_col_type) && ob_is_number_tc(dst_col_type)) {
             // online, do nothing
          } else if (ob_is_number_or_decimal_int_tc(src_col_type) &&
                       ob_is_number_or_decimal_int_tc(src_col_type)) {
            // different type cast, need do offline ddl
            is_offline = true;
          } else if ((src_meta.is_varchar() && dst_meta.is_char()) ||
                     (src_meta.is_char() && dst_meta.is_varchar()) ||
                     (src_meta.is_nchar() && dst_meta.is_nvarchar2())) {
            is_offline = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            (void)snprintf(err_msg, sizeof(err_msg), "Can not increase precision or scale, src column type %s,"
            "dst column type %s", ob_obj_type_str(src_col_type), ob_obj_type_str(dst_col_type));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
          }
        }
      } else {
        // in mysql mode
        if (!is_type_reduction &&
           (common::is_match_alter_integer_column_online_ddl_rules(src_meta, dst_meta) // smaller integer -> larger integer
            || common::is_match_alter_string_column_online_ddl_rules(src_meta, dst_meta, src_col_byte_len, dst_col_byte_len))) { // varchar, tinytext; varbinary, tinyblob; lob;
          // online, do nothing
        } else {
          is_offline = true;
        }
      }
      if ((src_meta.is_signed_integer() && dst_meta.is_unsigned_integer())
        || (src_meta.is_unsigned_integer() && dst_meta.is_signed_integer())) {
          is_offline = true;
      }
      if (!src_meta.is_lob_storage() && dst_meta.is_lob_storage()) {
        is_offline = true;
      }
    } else {
      if ((dst_meta.is_json() && src_meta.is_string_type()) ||
          (src_meta.is_json() && dst_meta.is_string_type())) {
        if (is_oracle_mode) {
          is_offline = true;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter non string type");
        }
      } else if (dst_column.is_xmltype()) {
        // if xmltype, must be oracle mode
        ret = OB_INVALID_ALTERATIONG_DATATYPE;
        LOG_USER_ERROR(OB_INVALID_ALTERATIONG_DATATYPE);
      } else if (src_column.is_xmltype()) {
        // if xmltype, must be oracle mode
        ret = OB_INVALID_MODIFICATION_OF_COLUMNS;
        LOG_USER_ERROR(OB_INVALID_MODIFICATION_OF_COLUMNS);
      } else if (!is_oracle_mode) {
        is_offline = true;
      } else {
        // in oracle mode
        ret = OB_NOT_SUPPORTED;
        (void)snprintf(err_msg, sizeof(err_msg), "Alter the column type, src column type %s,"
        "dst column type %s", ob_obj_type_str(src_col_type), ob_obj_type_str(dst_col_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
      }
    }
  } else if (src_column.is_collection()) {
    bool is_same = false;
    if (OB_FAIL(src_column.is_same_collection_column(dst_column, is_same))) {
      LOG_WARN("failed to check whether is same collection cols", K(ret));
    } else {
      is_offline = !is_same;
    }
  }
  return ret;
}

int ObTableSchema::check_has_trigger_on_table(
    ObSchemaGetterGuard &schema_guard, bool &is_enable, uint64_t trig_event) const
{
  int ret = OB_SUCCESS;
  is_enable = false;
  const ObTriggerInfo *trigger_info = NULL;
  const uint64_t tenant_id = get_tenant_id();
  for (int i = 0; OB_SUCC(ret) && !is_enable && i < trigger_list_.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list_.at(i), trigger_info));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_list_.at(i));
    if (OB_SUCC(ret) &&
        trigger_info->is_enable() &&
        (trigger_info->get_trigger_events() & trig_event) != 0) {
      is_enable = true;
    }
  }
  return ret;
}

int ObTableSchema::get_not_null_constraint_map(hash::ObHashMap<uint64_t, uint64_t> &cst_map) const
{
  int ret = OB_SUCCESS;
  for (ObTableSchema::const_constraint_iterator iter = constraint_begin();
       OB_SUCC(ret) && iter != constraint_end();
       ++iter) {
    if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
      if (OB_UNLIKELY(0 == (*iter)->get_column_cnt()) || OB_ISNULL((*iter)->cst_col_begin())) {
        ret = OB_SUCCESS;
        LOG_WARN("column of not null cst is null", K(ret), K((*iter)->get_column_cnt()));
      } else if (OB_FAIL(cst_map.set_refactored(*(*iter)->cst_col_begin(), (*iter)->get_constraint_id()))) {
        LOG_WARN("set refactored failed", K(ret));
      }
    }
  }
  return ret;
}


int ObTableSchema::check_prohibition_rules(const ObColumnSchemaV2 &src_schema,
                                           const ObColumnSchemaV2 &dst_schema,
                                           ObSchemaGetterGuard &schema_guard,
                                           const bool is_oracle_mode,
                                           const bool is_offline) const
{
  int ret = OB_SUCCESS;
  bool is_enable = false;
  bool is_same = false;
  bool has_prefix_idx_col_deps = false;
  bool is_tbl_part_key = false;
  bool is_column_in_fk = is_column_in_foreign_key(src_schema.get_column_id());
  if (OB_FAIL(check_is_exactly_same_type(src_schema, dst_schema, is_same))) {
    LOG_WARN("failed to check is exactly same type", K(ret));
  } else if (is_same) {
    // do nothing
  } else if (OB_FAIL(check_alter_column_in_foreign_key(src_schema, dst_schema, is_oracle_mode))) {
    LOG_WARN("failed to check alter column in foreign key", K(ret));
  } else if (!is_oracle_mode
            && (is_column_in_check_constraint(src_schema.get_column_id())
              && !common::is_match_alter_integer_column_online_ddl_rules(src_schema.get_meta_type(), dst_schema.get_meta_type()))) {
  // The column contains the check constraint to prohibit modification of the type in mysql mode
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter column with check constraint");
  } else if (is_oracle_mode && OB_FAIL(is_tbl_partition_key(src_schema, is_tbl_part_key,
                                                            false /* ignore_presetting_key */))) {
    LOG_WARN("fail to check partition key", KR(ret), K(src_schema));
  } else if (is_oracle_mode && is_tbl_part_key) {
  // Partition key prohibited to modify the type in oracle mode
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter column with partition key");
  } else if (is_oracle_mode && src_schema.has_generated_column_deps()) {
  // It is forbidden to modify the type when the modified column is referenced by the generated column
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter column that the generated column depends on");
  } else if (!is_oracle_mode && is_offline
    && OB_FAIL(check_prefix_index_columns_depend(src_schema, schema_guard, has_prefix_idx_col_deps))) {
    LOG_WARN("check prefix index columns cascaded failed", K(ret));
  } else if (has_prefix_idx_col_deps) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter column that the prefix index column depends on");
  } else if ((src_schema.is_string_type() || src_schema.is_enum_or_set())
            && (src_schema.get_collation_type() != dst_schema.get_collation_type()
            || src_schema.get_charset_type() != dst_schema.get_charset_type())
            && is_column_in_fk) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter column charset or collation with foreign key");
  } else if (is_offline && OB_FAIL(check_has_trigger_on_table(schema_guard, is_enable))) {
    LOG_WARN("failed to check alter column in trigger", K(ret));
  } else if (is_enable) {
    // do nothing, change/modify column is allowed on table with trigger(enable/disable).
  }
  return ret;
}

int ObTableSchema::check_ddl_type_change_rules(const ObColumnSchemaV2 &src_column,
                                               const ObColumnSchemaV2 &dst_column,
                                               ObSchemaGetterGuard &schema_guard,
                                               const bool is_oracle_mode,
                                               bool &is_offline) const
{
  int ret = OB_SUCCESS;
  bool is_rowkey = false;
  bool is_index = false;
  bool is_same = false;
  uint64_t data_version = 0;
  const ColumnType src_col_type = src_column.get_data_type();
  const ColumnType dst_col_type = dst_column.get_data_type();
  const ObObjMeta &src_meta = src_column.get_meta_type();
  const ObObjMeta &dst_meta = dst_column.get_meta_type();
  OZ (check_alter_column_in_rowkey(src_column, dst_column, is_rowkey));
  OZ (check_alter_column_in_index(src_column, dst_column, schema_guard, is_index));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_is_exactly_same_type(src_column, dst_column, is_same))) {
    LOG_WARN("failed to check is exactly same type", K(ret));
  } else if (is_same) {
    // do nothing
  } else if (!is_offline) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("failed to get min data version", K(ret), K(tenant_id_), K(data_version));
    } else if (is_oracle_mode) {
      if (!(ob_is_number_tc(src_col_type)) &&
        ((!src_meta.is_varying_len_char_type() &&
        !src_meta.is_timestamp_tz() &&
        !src_meta.is_timestamp_ltz() &&
        !src_meta.is_raw() &&
        !src_meta.is_interval_ym() &&
        !src_meta.is_interval_ds() &&
        !src_meta.is_urowid()) ||
        src_col_type != dst_col_type)) {
        if (is_rowkey) {
          is_offline = true;
        }
        if (is_index && (!src_meta.is_char() || !dst_meta.is_char())) {
          is_offline = true;
        }
      }
      if (is_column_in_foreign_key(src_column.get_column_id()) ||
          is_column_in_check_constraint(src_column.get_column_id()) ||
          src_meta.is_unsigned() != dst_meta.is_unsigned()) {
        is_offline = true;
      }
    } else {
      if (!ob_is_number_tc(src_col_type) &&
          !ob_is_decimal_int_tc(src_col_type) &&
          ((!ob_is_text_tc(src_col_type) &&
          !src_meta.is_bit() &&
          !src_meta.is_varchar() &&
          !src_meta.is_varbinary() &&
          !src_meta.is_enum_or_set() &&
          !src_meta.is_datetime()) ||
          src_col_type != dst_col_type) &&
          (!src_meta.is_timestamp() &&
            !dst_meta.is_datetime()) &&
          !((src_meta.is_char() && dst_meta.is_char()) && // char(x) -> char(y) (y>=x) && src_column has no generated column depended on (rowkey or index)
            !src_column.has_generated_column_deps()) &&
          !(src_meta.is_integer_type() && dst_meta.is_integer_type() && data_version >= DATA_VERSION_4_2_2_0)) {
        /*
          Note of the judge of data_version:
            Determine data_version to avoid mixed deployment problems during the upgrade process.
            During the upgrade process, the memtable_key.h of the old version of the observer will not have different types of defense rules (common::is_match_alter_integer_column_online_ddl_rules).
            And, the type dismatch may cause 4016 problems;
              1. This issue only needs to consider the primary key, index and part_key columns, so put the check here.
              2. In the online ddl conversion released in 4.2.2 and 4.3, only the column type conversion of integer will involve this issue.
            Therefore, we make a special case where the integer column is used as the primary key or index column.
        */
        if (is_rowkey || src_column.is_tbl_part_key_column()) {
          is_offline = true;
        }
        if (is_index) {
          is_offline = true;
        }
      }

      if (!is_offline &&
          src_meta.is_char() &&
          dst_meta.is_char() && // char(x) -> char(y) (y>=x) && src_column has no generated column depended on (common column)
          src_column.has_generated_column_deps()) {
        is_offline = true;
      }
      if (src_column.is_string_type() || src_column.is_enum_or_set()) {
        if (src_column.get_collation_type() != dst_column.get_collation_type() ||
            src_column.get_charset_type() != dst_column.get_charset_type()) {
          is_offline = true;
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::check_alter_column_in_rowkey(const ObColumnSchemaV2 &src_column,
                                                const ObColumnSchemaV2 &dst_column,
                                                bool &is_in_rowkey) const
{
  int ret = OB_SUCCESS;
  if (src_column.is_original_rowkey_column()) {
    if (ob_is_text_tc(dst_column.get_data_type())) {
      ret = OB_ERR_WRONG_KEY_COLUMN;
      LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, dst_column.get_column_name_str().length(),
      dst_column.get_column_name_str().ptr());
      LOG_WARN("BLOB, TEXT column can't be primary key", K(dst_column), K(ret));
    } else if (ObTimestampTZType == dst_column.get_data_type()) {
      ret = OB_ERR_WRONG_KEY_COLUMN;
      LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, dst_column.get_column_name_str().length(),
      dst_column.get_column_name_str().ptr());
      LOG_WARN("TIMESTAMP WITH TIME ZONE column can't be primary key", K(dst_column), K(ret));
    } else {
      is_in_rowkey = true;
    }
  }
  return ret;
}

int ObTableSchema::check_alter_column_in_index(const ObColumnSchemaV2 &src_column,
                                               const ObColumnSchemaV2 &dst_column,
                                               ObSchemaGetterGuard &schema_guard,
                                               bool &is_in_index) const
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_ids;
  const uint64_t column_id = src_column.get_column_id();
  const uint64_t tenant_id = get_tenant_id();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
        simple_index_infos.at(i).table_id_, index_table_schema))) {
      LOG_WARN("fail to get table schema", K(tenant_id),
               K(simple_index_infos.at(i).table_id_), K(ret));
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index table schema must not be NULL", K(ret));
    } else {
      column_ids.reuse();
      if (OB_FAIL(index_table_schema->get_column_ids(column_ids))) {
        LOG_WARN("fail to get column ids", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
        if (column_id == column_ids.at(j).col_id_) {
          is_in_index = true;
        }
      }
      if (OB_SUCC(ret) && is_in_index) {
        if (!index_table_schema->is_vec_index() && ob_is_text_tc(dst_column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, dst_column.get_column_name_str().length(),
          dst_column.get_column_name_str().ptr());
          LOG_WARN("BLOB, TEXT column can't be primary key", K(dst_column), K(ret));
        } else if (index_table_schema->is_unique_index()
                  && ObTimestampTZType == dst_column.get_data_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, dst_column.get_column_name_str().length(),
          dst_column.get_column_name_str().ptr());
          LOG_WARN("TIMESTAMP WITH TIME ZONE column can't be primary key", K(dst_column), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::check_alter_column_is_offline(const ObColumnSchemaV2 *src_column,
                                                ObColumnSchemaV2 *dst_column,
                                                ObSchemaGetterGuard &schema_guard,
                                                bool &is_offline) const
{
  int ret = OB_SUCCESS;
  bool is_same = false;
  bool is_oracle_mode = false;
  if (OB_ISNULL(src_column) || NULL == dst_column) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column schema is NULL", K(ret));
  } else if (!src_column->is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The column schema has error", K(ret));
  } else if (get_table_id() != src_column->get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column does not belong to this table", K(ret));
  } else if (is_external_table()) {
    is_offline = false;
  } else if (!is_user_table() && !is_index_table() && !is_tmp_table() && !is_sys_table() && !is_view_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Only NORMAL table and INDEX table and SYSTEM table are allowed", K(ret));
  } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check if oracle compat mode failed", K(ret));
  } else if (OB_FAIL(check_is_exactly_same_type(*src_column, *dst_column, is_same))) {
    LOG_WARN("failed to check is exactly same type", K(ret));
  } else if (is_same) {
    is_offline = false;
  } else {
    int32_t src_col_byte_len = src_column->get_data_length();
    int32_t dst_col_byte_len = dst_column->get_data_length();
    // oracle mode the column length of char semantics needs to be converted into the length of byte semantics for comparison
    if (OB_SUCC(ret) && is_oracle_mode
                && src_column->get_meta_type().is_character_type()
                && dst_column->get_meta_type().is_character_type()
                && src_column->get_length_semantics() != dst_column->get_length_semantics()) {
      if (OB_FAIL(convert_char_to_byte_semantics(src_column, is_oracle_mode, src_col_byte_len))) {
        LOG_WARN("failed to convert char to byte semantics", K(ret));
      } else if (OB_FAIL(convert_char_to_byte_semantics(dst_column, is_oracle_mode, dst_col_byte_len))) {
        LOG_WARN("failed to convert char to byte semantics", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_alter_column_accuracy(*src_column, *dst_column, src_col_byte_len,
                  dst_col_byte_len, is_oracle_mode, is_offline))) {
        LOG_WARN("failed to check alter column accuracy", K(ret));
      } else if (OB_FAIL(check_alter_column_type(*src_column, *dst_column, src_col_byte_len,
                         dst_col_byte_len, is_oracle_mode, is_offline))) {
        LOG_WARN("failed to check alter column type", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_ddl_type_change_rules(*src_column, *dst_column,
                     schema_guard, is_oracle_mode, is_offline))) {
      LOG_WARN("failed to check ddl type change rules", K(ret));
  } else if (OB_FAIL(check_prohibition_rules(*src_column, *dst_column,
                     schema_guard, is_oracle_mode, is_offline))) {
    LOG_WARN("failed to check prohibition rules", K(ret));
  }
  // all alter skip_index operations are online ddl through progressive merge
  LOG_DEBUG("check_alter_column_is_offline", K(ret), K(is_offline));
  return ret;
}

int ObTableSchema::check_is_exactly_same_type(const ObColumnSchemaV2 &src_column,
                                              const ObColumnSchemaV2 &dst_column,
                                              bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (src_column.get_data_type() == dst_column.get_data_type()) {
    if (src_column.get_meta_type().is_enum_or_set()) {
      if (src_column.get_charset_type() == dst_column.get_charset_type() &&
          src_column.get_collation_type() == dst_column.get_collation_type()) {
        bool is_incremental = true;
        if (OB_FAIL(ObDDLResolver::check_type_info_incremental_change(
                    src_column, dst_column, is_incremental))) {
          LOG_WARN("failed to check type info incremental change", K(ret));
        } else if ((src_column.get_extended_type_info().count() ==
                  dst_column.get_extended_type_info().count()) &&
                  is_incremental) {
          is_same = true;
        }
      }
    } else if (src_column.is_collection()) {
      if (OB_FAIL(src_column.is_same_collection_column(dst_column, is_same))) {
        LOG_WARN("failed to check whether is same collection cols", K(ret));
      }
    } else {
      if (src_column.is_string_type() || src_column.is_raw()
          || ob_is_rowid_tc(src_column.get_data_type())
          || src_column.is_json()) {
        if (src_column.get_charset_type() == dst_column.get_charset_type() &&
            src_column.get_collation_type() == dst_column.get_collation_type() &&
            src_column.get_data_length() == dst_column.get_data_length() &&
            src_column.get_length_semantics() == dst_column.get_length_semantics()) {
          is_same = true;
        }
      } else {
        if ((ob_is_int_tc(src_column.get_data_type()) ||
            src_column.get_data_precision() == dst_column.get_data_precision()) &&
            src_column.get_data_scale() == dst_column.get_data_scale()) {
          is_same = true;
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::check_column_can_be_altered_offline(
                  const ObColumnSchemaV2 *src_column,
                  ObColumnSchemaV2 *dst_column) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  bool is_offline = false;
  if (OB_ISNULL(src_column) || NULL == dst_column) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column schema is NULL", K(ret));
  } else if (!src_column->is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The column schema has error", K(ret));
  } else if (get_table_id() != src_column->get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column does not belong to this table", K(ret));
  } else if (!is_user_table() && !is_index_table() && !is_tmp_table() && !is_sys_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Only NORMAL table and INDEX table and SYSTEM table are allowed", K(ret));
  } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check if oracle compat mode failed", K(ret));
  } else {
    const ObColumnSchemaV2 *tmp_column = NULL;
    int32_t src_col_byte_len = src_column->get_data_length();
    int32_t dst_col_byte_len = dst_column->get_data_length();
    const ColumnType src_col_type = src_column->get_data_type();
    const ColumnType dst_col_type = dst_column->get_data_type();
    const ObAccuracy &src_accuracy = src_column->get_accuracy();
    const ObAccuracy &dst_accuracy = dst_column->get_accuracy();
    char err_msg[number::ObNumber::MAX_PRINTABLE_SIZE] = {0};
    LOG_DEBUG("check column schema can be altered", KPC(src_column), KPC(dst_column));
    // oracle mode the column length of char semantics needs to be converted into the length of byte semantics for comparison
    if (OB_SUCC(ret) && is_oracle_mode
                && src_column->get_meta_type().is_character_type()
                && dst_column->get_meta_type().is_character_type()
                && src_column->get_length_semantics() != dst_column->get_length_semantics()) {
      if (OB_FAIL(convert_char_to_byte_semantics(src_column, is_oracle_mode, src_col_byte_len))) {
        LOG_WARN("failed to convert char to byte semantics", K(ret));
      } else if (OB_FAIL(convert_char_to_byte_semantics(dst_column, is_oracle_mode, dst_col_byte_len))) {
        LOG_WARN("failed to convert char to byte semantics", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_alter_column_accuracy(*src_column, *dst_column, src_col_byte_len,
                  dst_col_byte_len, is_oracle_mode, is_offline))) {
        LOG_WARN("failed to check alter column accuracy", K(ret));
      } else if (OB_FAIL(check_alter_column_type(*src_column, *dst_column, src_col_byte_len,
                         dst_col_byte_len, is_oracle_mode, is_offline))) {
        LOG_WARN("failed to check alter column type", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      tmp_column = get_column_schema(dst_column->get_column_name());
      if ((NULL != tmp_column) && (tmp_column != src_column)) {
        ret = OB_ERR_COLUMN_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, dst_column->get_column_name_str().length(),
                        dst_column->get_column_name_str().ptr());
        LOG_WARN("Column already exist!", K(ret), "column_name", dst_column->get_column_name_str());
      }
      if (OB_SUCC(ret) && src_column->is_rowkey_column()) {
        ObColumnSchemaV2 *dst_col = dst_column;
        if (!src_column->is_heap_alter_rowkey_column()) {
          dst_col->set_nullable(false);
        }
        if (OB_FAIL(check_rowkey_column_can_be_altered(src_column, dst_column))) {
          LOG_WARN("Row key column can not be altered", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_row_length(is_oracle_mode, src_column, dst_column))) {
          LOG_WARN("check row length failed", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObTableSchema::check_column_can_be_altered_online(
    const ObColumnSchemaV2 *src_schema,
    ObColumnSchemaV2 *dst_schema) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *tmp_column = NULL;
  if (OB_ISNULL(src_schema) || NULL == dst_schema) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column schema is NULL", K(ret));
  } else if (!src_schema->is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The column schema has error", K(ret));
  } else if (get_table_id() != src_schema->get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column does not belong to this table", K(ret));
  } else if (is_external_table()) {
    // external table canbe altered
  } else if (!is_user_table() && !is_index_table() && !is_tmp_table() && !is_sys_table() && !is_view_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Only NORMAL table and INDEX table and SYSTEM table are allowed", K(ret));
  } else {
    LOG_DEBUG("check column schema can be altered", KPC(src_schema), KPC(dst_schema));
    // Additional restriction for system table:
    // 1. Can't alter column name
    // 2. Can't alter column from "NULL" to "NOT NULL"
    if (is_system_table(get_table_id())) {
      if (0 != src_schema->get_column_name_str().compare(dst_schema->get_column_name_str())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter system table's column name is");
        LOG_WARN("Alter system table's column name is not supported", KR(ret), K(src_schema), K(dst_schema));
      } else if (src_schema->is_nullable() && !dst_schema->is_nullable()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter system table's column from `NULL` to `NOT NULL`");
        LOG_WARN("Alter system table's column from `NULL` to `NOT NULL` is not supported", KR(ret), K(src_schema), K(dst_schema));
      }
    }

    bool is_oracle_mode = false;
    if (FAILEDx(check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("check if oracle compat mode failed", K(ret));
    } else if (is_oracle_mode
              && ob_is_number_tc(src_schema->get_data_type())
              && ob_is_number_tc(dst_schema->get_data_type())) {
      // support number to float in oracle mode
    } else if ((src_schema->get_data_type() == dst_schema->get_data_type()
      && src_schema->get_collation_type() == dst_schema->get_collation_type())
      || common::is_match_alter_integer_column_online_ddl_rules(src_schema->get_meta_type(), dst_schema->get_meta_type()) // has to check the changing is valid
      || (src_schema->is_string_type() && dst_schema->is_string_type()
        && src_schema->get_charset_type() == dst_schema->get_charset_type()
        && src_schema->get_collation_type() == dst_schema->get_collation_type())) {
      if (ob_is_large_text(src_schema->get_data_type())
          && src_schema->get_data_type() != dst_schema->get_data_type()
          && src_schema->get_data_length() > dst_schema->get_data_length()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Truncate large text/lob column");
        LOG_WARN("The data of large text/lob column can not be truncated", K(ret), KPC(dst_schema), KPC(src_schema));
      } else if (((!is_oracle_mode && src_schema->is_string_type())
                  || (is_oracle_mode && src_schema->get_meta_type().is_lob())
                  || (is_oracle_mode && src_schema->get_meta_type().is_character_type()
                      && src_schema->get_length_semantics() == dst_schema->get_length_semantics())
                  || src_schema->is_raw()
                  || src_schema->get_meta_type().is_urowid())
                && (dst_schema->get_data_length() < src_schema->get_data_length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Truncate the data of column schema");
        LOG_WARN("The data of column schema can not be truncated",
                  K(ret), KPC(dst_schema), KPC(src_schema));
      } else if ((src_schema->get_data_type() == ObCharType
                  && dst_schema->get_data_type() != ObCharType)
                 || (src_schema->get_data_type() == ObNCharType
                     && dst_schema->get_data_type() != ObNCharType)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify char type to other data types");
        LOG_WARN("can not modify char type to other data types",
                  K(ret), KPC(src_schema), K(dst_schema));
      } else if (src_schema->get_data_type() == ObCharType
                 && src_schema->get_collation_type() == CS_TYPE_BINARY
                 && (dst_schema->get_data_length() != src_schema->get_data_length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Truncated data or change binary column length");
        LOG_WARN("The data of column schema can not be truncated, "
                 "binary column can't change length",
                  K(ret), KPC(src_schema), KPC(dst_schema));
      } else if (dst_schema->get_data_type() == ObCharType && dst_schema->get_collation_type() == CS_TYPE_BINARY
          && !(src_schema->get_data_type() == ObCharType && src_schema->get_collation_type() == CS_TYPE_BINARY && src_schema->get_data_length() == dst_schema->get_data_length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify column to binary type");
        LOG_WARN("can not modify data to binary type", K(ret), KPC(src_schema), KPC(dst_schema));
      } else if (ob_is_integer_type(src_schema->get_data_type()) && ob_is_integer_type(dst_schema->get_data_type())
          && !common::is_match_alter_integer_column_online_ddl_rules(src_schema->get_meta_type(), dst_schema->get_meta_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Change int data type to small scale");
        LOG_WARN("can't not change int data type to small scale",
                 "src", src_schema->get_data_type(),
                 "dst", dst_schema->get_data_type(),
                 K(ret));
      } else if (ob_is_geometry(src_schema->get_data_type())
                 && src_schema->get_geo_type() != dst_schema->get_geo_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify geometry type");
        LOG_WARN("can't not modify geometry type",
                 "src", src_schema->get_geo_type(),
                 "dst", dst_schema->get_geo_type(),
                 K(ret));
      } else if (ob_is_geometry(src_schema->get_data_type())
                 && src_schema->get_srid() != dst_schema->get_srid()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify geometry srid");
        LOG_WARN("can't not modify geometry srid",
                 "src", src_schema->get_srid(),
                 "dst", dst_schema->get_srid(),
                 K(ret));
      } else {
        tmp_column = get_column_schema(dst_schema->get_column_name());
        if ((NULL != tmp_column) && (tmp_column != src_schema)) {
          ret = OB_ERR_COLUMN_DUPLICATE;
          LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, dst_schema->get_column_name_str().length(),
                         dst_schema->get_column_name_str().ptr());
          LOG_WARN("Column already exist!", K(ret), "column_name", dst_schema->get_column_name_str());
        } else if (!src_schema->is_autoincrement() && dst_schema->is_autoincrement() &&
             autoinc_column_id_ != 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "More than one auto increment column");
          LOG_WARN("Only one auto increment row is allowed", K(ret));
        }
        if (OB_SUCC(ret) && src_schema->is_rowkey_column()) {
          ObColumnSchemaV2 *dst_col = dst_schema;
          if (!src_schema->is_heap_alter_rowkey_column()) {
            dst_col->set_nullable(false);
          }
          if (OB_FAIL(check_rowkey_column_can_be_altered(src_schema, dst_schema))) {
            LOG_WARN("Row key column can not be altered", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_oracle_mode
                   && src_schema->get_meta_type().is_character_type()
                   && dst_schema->get_meta_type().is_character_type()
                   && src_schema->get_length_semantics() != dst_schema->get_length_semantics()) {
          // oracle mode the column length of char semantics needs to be converted into the length of byte semantics for comparison,
          // and it is not allowed to change it to a smaller value.
          int64_t mbmaxlen = 0;
          if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(
                      src_schema->get_collation_type(), mbmaxlen))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get mbmaxlen", K(ret), K(src_schema->get_collation_type()));
          } else if (0 >= mbmaxlen) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mbmaxlen is less than 0", K(ret), K(mbmaxlen));
          } else {
            int32_t src_col_byte_len = src_schema->get_data_length();
            int32_t dst_col_byte_len = dst_schema->get_data_length();
            if (!is_oracle_byte_length(is_oracle_mode, src_schema->get_length_semantics())) {
              src_col_byte_len = static_cast<int32_t>(src_col_byte_len * mbmaxlen);
            } else {
              dst_col_byte_len = static_cast<int32_t>(dst_col_byte_len * mbmaxlen);
            }
            if (src_col_byte_len > dst_col_byte_len) {
              ret = OB_ERR_DECREASE_COLUMN_LENGTH;
              LOG_WARN("The data of column schema can not be truncated",
                       K(ret), K(mbmaxlen), K(src_col_byte_len), K(dst_col_byte_len));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_row_length(is_oracle_mode, src_schema, dst_schema))) {
            LOG_WARN("check row length failed", K(ret));
          }
        }
      }
    } else if (src_schema->is_string_type() && dst_schema->is_string_type()
               && src_schema->get_collation_type() != dst_schema->get_collation_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter charset or collation type");
    } else {
      //oracle support DATE column <-> TIMESTAMP or TIMESTAMP WITH LOCAL TIME ZONE column. BUT, ob NOT support now @yanhua
      //https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/ALTER-TABLE.html#GUID-552E7373-BF93-477D-9DA3-B2C9386F2877
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter non string type");
      LOG_WARN("The data type of column schema is non string type can not be altered", K(ret),
               K(*src_schema), K(*dst_schema));
    }
  }
  return ret;
}

// Non-indexed tables do not exceed the limit of OB_MAX_USER_ROW_KEY_LENGTH for the sum of the length of
// the primary key column of string type
// The index table does not exceed the limit of OB_MAX_USER_ROW_KEY_LENGTH for the total length of the index column in the primary key
// column of string type (the hidden primary key column in the index is not included in the total length)
int ObTableSchema::check_rowkey_column_can_be_altered(const ObColumnSchemaV2 *src_schema,
                                                      const ObColumnSchemaV2 *dst_schema) const
{
  int ret = OB_SUCCESS;
  //rowkey column will always be not null
  //  if (dst_schema->is_nullable()) {
  //    ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
  //    LOG_WARN("The rowkey column can not be null", K(ret));
  //  }
  //todo cangdi will check alter table add primary key
  //  if (!dst_schema->is_rowkey_column() && get_rowkey_column_num() <= 1) {
  //    ret = OB_NOT_SUPPORTED;
  //    LOG_WARN("There must be at least one primary key column", K(ret));
  //  }
  ObColumnSchemaV2 *column = NULL;
  int64_t rowkey_varchar_col_length = 0;
  int64_t length = 0;
  bool is_oracle_mode = false;
  if (OB_ISNULL(src_schema) || OB_ISNULL(dst_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_schema), K(dst_schema));
  } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle compat mode", KR(ret), KPC(this));
  } else {
    if (ob_is_string_tc(src_schema->get_data_type())
        && ob_is_string_tc(dst_schema->get_data_type())) {
      const int64_t max_rowkey_length = is_sys_table() ? OB_MAX_ROW_KEY_LENGTH : OB_MAX_USER_ROW_KEY_LENGTH;
      for (int64_t i = 0; OB_SUCC(ret) && (i < column_cnt_); ++i) {
        column = column_array_[i];
        if ((!is_index_table() && (column->get_rowkey_position() > 0))
            || (is_index_table() && (column->is_index_column()))) {
          if (ob_is_string_tc(column->get_data_type())) {
            if (OB_FAIL(column->get_byte_length(length, is_oracle_mode, false))) {
              LOG_WARN("fail to get byte length of column", KR(ret), K(is_oracle_mode));
            } else {
              rowkey_varchar_col_length += length;
            }
          }
        }
      }
      int64_t src_column_byte_length = 0;
      int64_t dst_column_byte_length = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(src_schema->get_byte_length(src_column_byte_length, is_oracle_mode, false))) {
        LOG_WARN("fail to get byte length of column", KR(ret), K(is_oracle_mode));
      } else if (OB_FAIL(dst_schema->get_byte_length(dst_column_byte_length, is_oracle_mode, false))) {
        LOG_WARN("fail to get byte length of column", KR(ret), K(is_oracle_mode));
      } else {
        rowkey_varchar_col_length -= src_column_byte_length;
        rowkey_varchar_col_length += dst_column_byte_length;
      }
      if (OB_FAIL(ret)) {
      } else if (rowkey_varchar_col_length > max_rowkey_length) {
        ret = OB_ERR_TOO_LONG_KEY_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, max_rowkey_length);
        LOG_WARN("total length of varchar primary key columns is larger than the max allowed length",
                 K(rowkey_varchar_col_length), K(max_rowkey_length), K(ret));
      }
    } else if (ObTextTC == dst_schema->get_data_type_class()
               || ObJsonTC == dst_schema->get_data_type_class()
               || ObGeometryTC == dst_schema->get_data_type_class()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify rowkey column to text/clob/blob");
    }
  }

  return ret;
}

// NULL == src_schema : for add_column
// NULL != src_schema : for alter_column
int ObTableSchema::check_row_length(
    const bool is_oracle_mode,
    const ObColumnSchemaV2 *src_schema,
    const ObColumnSchemaV2 *dst_schema) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dst_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst_schema));
  } else {
    const int64_t max_row_length = is_inner_table(get_table_id()) ? INT64_MAX : OB_MAX_USER_ROW_LENGTH;
    ObColumnSchemaV2 *col = NULL;
    int64_t row_length = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      col = column_array_[i];
      if ((is_table() || is_tmp_table() || is_external_table()) && !col->is_column_stored_in_sstable()) {
        // The virtual column in the table does not actually store data, and does not count the length
      } else if (is_storage_index_table() && col->is_fulltext_column()) {
        // The full text column in the index only counts the length of one word segment
        row_length += OB_MAX_OBJECT_NAME_LENGTH;
      } else if (ob_is_string_type(col->get_data_type()) || ob_is_json(col->get_data_type())
                 || ob_is_geometry(col->get_data_type()) || ob_is_roaringbitmap(col->get_data_type())) {
        int64_t length = 0;
        if (OB_FAIL(col->get_byte_length(length, is_oracle_mode, true))) {
          SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
        } else {
          row_length += length;
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t src_byte_length = 0;
      if (NULL != src_schema) {
        // is alter_column
        if (OB_FAIL(src_schema->get_byte_length(src_byte_length, is_oracle_mode, true))) {
          SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t dst_byte_length = 0;
        if (OB_FAIL(dst_schema->get_byte_length(dst_byte_length, is_oracle_mode, true))) {
          SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
        } else {
          row_length -= src_byte_length;
          if ((is_table() || is_tmp_table() || is_external_table()) && !dst_schema->is_column_stored_in_sstable()) {
            // The virtual column in the table does not actually store data, and does not count the length
          } else if (is_storage_index_table() && dst_schema->is_fulltext_column()) {
            // The full text column in the index only counts the length of one word segment
            row_length += OB_MAX_OBJECT_NAME_LENGTH;
          } else {
            row_length += dst_byte_length;
          }
          if (row_length > max_row_length) {
            ret = OB_ERR_TOO_BIG_ROWSIZE;
            SQL_RESV_LOG(WARN, "row_length is larger than max_row_length", K(ret), K(row_length), K(max_row_length), K(column_cnt_));
          }
        }
      }
    }
  }

  return ret;
}

void ObTableSchema::reset_column_info()
{
  column_cnt_ = 0;
  column_array_capacity_ = 0;
  max_used_column_id_ = 0;
  index_column_num_ = 0;
  rowkey_column_num_ = 0;
  rowkey_info_.reset();
  shadow_rowkey_info_.reset();
  index_info_.reset();
  column_array_ = NULL;
  id_hash_array_ = NULL;
  name_hash_array_ = NULL;
}

void ObTableSchema::reset_column_group_info()
{
  max_used_column_group_id_ = COLUMN_GROUP_START_ID;
  column_group_cnt_ = 0;
  column_group_arr_capacity_ = 0;
  column_group_arr_ = NULL;
  cg_id_hash_arr_ = NULL;
  cg_name_hash_arr_ = NULL;
}

int ObTableSchema::get_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  for (ObTableSchema::const_column_iterator iter = column_begin();
       OB_SUCC(ret) && iter != column_end();
       ++iter) {
    const ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("Fail to add column id to scan", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}


/**
 * @brief Get all the column id of index column and rowkey column.
 * @param column_ids[out] output all column ids of index column and rokey column.
 */
int ObTableSchema::get_index_and_rowkey_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    if (NULL == column_array_[i]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is NULL, ", K(i));
    } else if ((column_array_[i]->is_index_column() || column_array_[i]->is_rowkey_column()) &&
               OB_FAIL(column_ids.push_back(column_array_[i]->get_column_id()))) {
      LOG_WARN("failed to push back column id", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTableSchema::has_column(const uint64_t column_id, bool &has) const
{
  int ret = OB_SUCCESS;
  bool contain = false;
  for (ObTableSchema::const_column_iterator iter = column_begin();
       OB_SUCC(ret) && !contain && iter != column_end();
       ++iter) {
    const ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (column_id == column_schema->get_column_id()) {
      contain = true;
    }
  }
  if (OB_SUCC(ret)) {
    has = contain;
  }
  return ret;
}

int ObTableSchema::has_column(const ObString col_name, bool &has) const
{
  int ret = OB_SUCCESS;
  bool contain = false;
  for (ObTableSchema::const_column_iterator iter = column_begin();
       OB_SUCC(ret) && !contain && iter != column_end();
       ++iter) {
    const ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (0 == col_name.case_compare(column_schema->get_column_name_str())) {
      contain = true;
    }
  }
  OX (has = contain);
  return ret;
}

int ObTableSchema::has_lob_column(bool &has_lob, const bool check_large /*= false*/) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;

  has_lob = false;
  for (ObTableSchema::const_column_iterator iter = column_begin();
       OB_SUCC(ret) && !has_lob && iter != column_end();
       ++iter) {
    if (OB_ISNULL(column_schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (ob_is_json_tc(column_schema->get_data_type())
               || ob_is_geometry_tc(column_schema->get_data_type())
               || ob_is_roaringbitmap_tc(column_schema->get_data_type())) {
      has_lob = true; // cannot know whether a json is lob or not from schema
    } else if (check_large) {
      if (ob_is_large_text(column_schema->get_data_type())) {
        has_lob = true;
      }
    } else if (ob_is_text_tc(column_schema->get_data_type())) {
      has_lob = true;
    }
  }

  return ret;
}

int ObTableSchema::has_add_column_instant(bool &add_column_instant) const
{
  int ret = OB_SUCCESS;
  add_column_instant = false;
  int64_t max_column_id = 0;
  const ObColumnSchemaV2 *col = NULL;
  bool is_oracle_mode = false;
  ObColumnIterByPrevNextID iter(*this);
  if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to if oracle mode", KR(ret));
  }
  while (!is_oracle_mode && OB_SUCC(ret)) {
    if (OB_FAIL(iter.next(col))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("iterate failed", KR(ret));
      }
    } else if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col is NULL, ", KR(ret));
    } else if (col->get_column_id() < OB_APP_MIN_COLUMN_ID) {
      // ignore hidden column
    } else if (col->get_column_id() < max_column_id) {
      add_column_instant = true;
      break;
    } else {
      max_column_id = col->get_column_id();
    }
  }
  return ret;
}

// For the main VP table, it returns the primary key column and the VP column, get_column_ids() is different,
// it will return all columns including other VP columns
// For the secondary VP table, it returns the same as get_column_ids(), that is, the primary key column and the VP column
int ObTableSchema::get_vp_store_column_ids(common::ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  column_ids.reset();

  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (is_aux_vp_table()) {
    if (OB_FAIL(get_column_ids(column_ids))) {
      LOG_WARN("Fail to get column ids", K(ret));
    }
  } else if (is_primary_vp_table()) {
    if (OB_FAIL(get_vp_column_ids_with_rowkey(column_ids))) {
      LOG_WARN("Fail to get vp column ids", K(ret));
    }
  }
  return ret;
}

// Return all VP columns, including the VP column that is the primary key
int ObTableSchema::get_vp_column_ids(common::ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (OB_UNLIKELY(!column_ids.empty())) {
    // do not reset array for ObFixedArray
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid non-empty array to get vp column ids", K(column_ids));
  } else {
    ObColDesc col_desc;
    //add now-rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      const ObColumnSchemaV2 *it = get_column_schema_by_idx(i);
      if (NULL == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else if (it->is_primary_vp_column() || it->is_aux_vp_column()) {
        // The same VP table will not have a primary VP column and a secondary VP column at the same time
        // Therefore, the type of VP table is not judged here.
        col_desc.col_id_ = static_cast<int32_t>(it->get_column_id());
        col_desc.col_type_ = it->get_meta_type();
        if (col_desc.col_type_.is_decimal_int()) {
          col_desc.col_type_.set_scale(it->get_data_scale());
        }
        //for non-rowkey, col_desc.col_order_ is not meaningful
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("fail to add now-rowkey vp column id to column_ids", K(ret));
        }
      }
    }
  }
  return ret;
}

// Used in the primary partition table, returns all VP columns, including the primary key + VP column
int ObTableSchema::get_vp_column_ids_with_rowkey(common::ObIArray<ObColDesc> &column_ids,
    const bool no_virtual) const
{
  int ret = OB_SUCCESS;

  if (!is_primary_vp_table()) {
    // do nothing
  } else if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (OB_UNLIKELY(!column_ids.empty())) {
    // do not reset array for ObFixedArray
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid non-empty array to get vp column ids", K(column_ids));
  } else {
    ObColDesc col_desc;
    // firstly add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      const ObRowkeyColumn *rowkey_column = NULL;
      if (NULL == (rowkey_column = rowkey_info_.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else {
        col_desc.col_id_ = static_cast<int32_t>(rowkey_column->column_id_);
        col_desc.col_type_ = rowkey_column->type_;
        col_desc.col_order_ = rowkey_column->order_;
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("Fail to add rowkey column id to column_ids", K(ret));
        }
      }
    }
    // secondly add vp columns without rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      const ObColumnSchemaV2 *it = get_column_schema_by_idx(i);
      if (NULL == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else if (it->is_primary_vp_column() && !(it->is_rowkey_column())
          && (!no_virtual || !(it->is_virtual_generated_column()))) {
        // This column is a VP column, if it is also a primary key column, skip it,
        // because the first step has been added
        col_desc.col_id_ = static_cast<int32_t>(it->get_column_id());
        col_desc.col_type_ = it->get_meta_type();
        if (col_desc.col_type_.is_decimal_int()) {
          col_desc.col_type_.set_scale(it->get_data_scale());
        }
        //for non-rowkey, col_desc.col_order_ is not meaningful
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("fail to add now-rowkey vp column id to column_ids", K(ret));
        }
      }
    }
  }
  return ret;
}

// col id includes:
//  1. all rowkey
//  2. part key which is generate col
int ObTableSchema::get_column_ids_serialize_to_rowid(common::ObIArray<uint64_t> &col_ids,
                                                     int64_t &rowkey_cnt) const
{
  int ret = OB_SUCCESS;
  col_ids.reset();
  rowkey_cnt = -1;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    const ObRowkeyInfo &rowkey_info = get_rowkey_info();
    OZ(rowkey_info.get_column_ids(col_ids));
    OX(rowkey_cnt = col_ids.count());

    if (is_heap_table()) {
      // rowid of heap organized table is made up of (tablet id, rowkey)
    } else if (OB_SUCC(ret) && has_generated_column()) {
      const ObPartitionKeyInfo &part_key_info = get_partition_key_info();
      const ObPartitionKeyInfo &subpart_key_info = get_subpartition_key_info();

      const ObColumnSchemaV2 *col_schema = NULL;
      uint64_t col_id = OB_INVALID_ID;

      ObSEArray<const ObPartitionKeyInfo*, 2> tmp_key_infos;
      OZ(tmp_key_infos.push_back(&part_key_info));
      OZ(tmp_key_infos.push_back(&subpart_key_info));

      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_key_infos.count(); ++i) {
        const ObPartitionKeyInfo *info = tmp_key_infos.at(i);
        CK(OB_NOT_NULL(info));
        for (int64_t j = 0; OB_SUCC(ret) && j < info->get_size(); ++j) {
          OZ(info->get_column_id(j, col_id));
          CK(OB_NOT_NULL(col_schema = get_column_schema(col_id)));
          if (OB_SUCC(ret)) {
            if (!has_exist_in_array(col_ids, col_id)) {
              if (col_schema->is_generated_column()) {
                OZ(add_var_to_array_no_dup(col_ids, col_id));
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("part key must be pri key or generated col", K(ret), K(*col_schema));
              }
            }
          }
        } // end for
      } // end for
    }
  }
  return ret;
}

int ObTableSchema::get_multi_version_column_descs(common::ObIArray<ObColDesc> &column_descs) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (OB_FAIL(get_mulit_version_rowkey_column_ids(column_descs))) { // add rowkey columns
    LOG_WARN("Fail to get rowkey column descs", K(ret));
  } else if (OB_FAIL(get_column_ids_without_rowkey(column_descs, !is_storage_index_table()))) { //add other columns
    LOG_WARN("Fail to get column descs with out rowkey", K(ret));
  } else if (OB_FAIL(set_precision_to_column_desc(column_descs))) {
    LOG_WARN("failed to set precision to cols", K(ret));
  }
  return ret;
}

int ObTableSchema::get_skip_index_col_attr(common::ObIArray<ObSkipIndexColumnAttr> &skip_idx_attrs) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    skip_idx_attrs.reset();
    // add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      const ObRowkeyColumn *rowkey_column = nullptr;
      const ObColumnSchemaV2 *column_schema = nullptr;
      if (OB_ISNULL(rowkey_column = rowkey_info_.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null rowkey column", K(ret), K(i));
      } else if (OB_ISNULL(column_schema = get_column_schema(rowkey_column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", K(ret), K(i), KPC(rowkey_column));
      } else if (OB_FAIL(skip_idx_attrs.push_back(column_schema->get_skip_index_attr()))) {
        LOG_WARN("failed to add rowkey skip index attr", K(ret), K(i), KPC(column_schema));
      }
    }
    // add dummy idx for stored multi-version columns
    if (OB_SUCC(ret)) {
      ObSkipIndexColumnAttr dummy_multi_version_col_attr;
      if (OB_FAIL(skip_idx_attrs.push_back(dummy_multi_version_col_attr))) {
        LOG_WARN("failed to push dummy multi version column skip index attr", K(ret));
      } else if (OB_FAIL(skip_idx_attrs.push_back(dummy_multi_version_col_attr))) {
        LOG_WARN("failed to push dummy multi version column skip index attr", K(ret));
      }
    }
    // add non-rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      const ObColumnSchemaV2 *column_schema = nullptr;
      // bool has_null_count_column = false;
      if (OB_ISNULL(column_schema = column_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column", K(ret), K(i));
      } else if (column_schema->is_rowkey_column() || column_schema->is_virtual_generated_column()) {
        // skip
      } else if (OB_FAIL(skip_idx_attrs.push_back(column_schema->get_skip_index_attr()))) {
        LOG_WARN("failed to add skip index attr", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::is_need_check_merge_progress(bool &need_check) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    need_check = !(is_index_table() && is_global_index_table() && !can_read_index())
                    && !is_user_hidden_table();
    if (!need_check) {
      FLOG_INFO("not check merge progress", K(need_check), K(*this));
    }
  }
  return ret;
}

int ObTableSchema::get_rowid_version(int64_t rowkey_cnt,
                                     int64_t serialize_col_cnt,
                                     int64_t &version) const
{
  int ret = OB_SUCCESS;

  if (is_heap_table() && !is_external_table()) {
    version = is_extended_rowid_mode() ? ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION : ObURowIDData::HEAP_TABLE_ROWID_VERSION;
  } else if (is_heap_table() && is_external_table()) {
    version = ObURowIDData::EXTERNAL_TABLE_ROWID_VERSION;
  } else {
    version = ObURowIDData::PK_ROWID_VERSION;
    if (rowkey_cnt != serialize_col_cnt) {
      if (OB_FAIL(ObURowIDData::get_part_gen_col_version(rowkey_cnt, version))) {
        LOG_WARN("get_part_gen_col_version failed", K(ret), K(rowkey_cnt), K(version));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_store_column_ids(common::ObIArray<ObColDesc> &column_ids, const bool full_col) const
{
  int ret = OB_SUCCESS;
  bool no_virtual = true;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (!full_col && is_primary_vp_table()) {
    if (OB_FAIL(get_vp_column_ids_with_rowkey(column_ids, no_virtual))) {
      LOG_WARN("failed to get_vp_column_ids_with_rowkey", K(ret));
    }
  } else {
    if (is_storage_index_table()) {
      no_virtual = false;
    }
    if (OB_FAIL(get_column_ids(column_ids, no_virtual))) {
      LOG_WARN("failed to get_column_ids", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::get_store_column_count(int64_t &column_count, const bool full_col) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (is_storage_index_table()) {
    column_count = column_cnt_;
  } else if (!full_col && (is_aux_vp_table() || is_primary_vp_table())) {
    ObArray<ObColDesc> column_ids;
    if (OB_FAIL(get_store_column_ids(column_ids))) {
      LOG_WARN("failed to get store column ids", K(ret));
    } else {
      column_count = column_ids.count();
    }
  } else {
    column_count = column_cnt_ - virtual_column_cnt_;
  }
  return ret;
}

int ObTableSchema::get_column_ids(common::ObIArray<ObColDesc> &column_ids, bool no_virtual) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    if (OB_FAIL(get_rowkey_column_ids(column_ids))) { // add rowkey columns
      LOG_WARN("Fail to get rowkey column ids", K(ret));
    } else if (OB_FAIL(get_column_ids_without_rowkey(column_ids, no_virtual))) { //add other columns
      LOG_WARN("Fail to get column ids with out rowkey", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::get_rowkey_column_ids(common::ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn *rowkey_column = NULL;
  ObColDesc col_desc;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    //add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      if (NULL == (rowkey_column = rowkey_info_.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else {
        col_desc.col_id_ = static_cast<int32_t>(rowkey_column->column_id_);
        col_desc.col_type_ = rowkey_column->type_;
        col_desc.col_order_ = rowkey_column->order_;
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("Fail to add rowkey column id to column_ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::get_rowkey_column_ids(common::ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn *rowkey_column = NULL;
  ObColDesc col_desc;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    //add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      if (NULL == (rowkey_column = rowkey_info_.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else if (OB_FAIL(column_ids.push_back(rowkey_column->column_id_))) {
        LOG_WARN("failed to push back rowkey column id", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTableSchema::set_precision_to_column_desc(common::ObIArray<ObColDesc> &column_ids) const
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < column_ids.count(); i++) {
    const ObColumnSchemaV2 *col_schema = nullptr;
    if (column_ids.at(i).col_type_.is_decimal_int()) {
      if (OB_ISNULL(col_schema = get_column_schema(column_ids.at(i).col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column schema is null ", K(ret), K(i), K(column_ids.at(i).col_id_));
      } else {
        column_ids.at(i).col_type_.set_stored_precision(col_schema->get_accuracy().get_precision());
        column_ids.at(i).col_type_.set_scale(col_schema->get_accuracy().get_scale());
      }
    }
  }
  return ret;
}

int ObTableSchema::get_rowkey_partkey_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn *key_column = NULL;
  if (OB_FAIL(get_rowkey_column_ids(column_ids))) {
    LOG_WARN("get rowkey column ids failed", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < partition_key_info_.get_size(); ++i) {
    if (NULL == (key_column = partition_key_info_.get_column(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The key column is NULL, ", K(i));
    } else if (OB_FAIL(add_var_to_array_no_dup(column_ids, key_column->column_id_))) {
      LOG_WARN("failed to push back part key column id", K(ret));
    } else { /*do nothing*/ }
  }
  for (int i = 0; OB_SUCC(ret) && i < subpartition_key_info_.get_size(); ++i) {
    if (NULL == (key_column = subpartition_key_info_.get_column(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The key column is NULL, ", K(i));
    } else if (OB_FAIL(add_var_to_array_no_dup(column_ids, key_column->column_id_))) {
      LOG_WARN("failed to push back subpart key column id", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTableSchema::get_column_ids_without_rowkey(
    common::ObIArray<ObColDesc> &column_ids,
    bool no_virtual) const
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    //add other columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (NULL == column_array_[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL, ", K(i));
      } else if (!column_array_[i]->is_rowkey_column()
          && !(no_virtual && column_array_[i]->is_virtual_generated_column())) {
        col_desc.col_id_ = static_cast<int32_t>(column_array_[i]->get_column_id());
        col_desc.col_type_ = column_array_[i]->get_meta_type();
        if (col_desc.col_type_.is_decimal_int()) {
          col_desc.col_type_.set_scale(column_array_[i]->get_data_scale());
        }
        //for non-rowkey, col_desc.col_order_ is not meaningful
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("Fail to add column id to column_ids", K(ret), K(i), K(column_array_[i]), K(column_cnt_));
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::get_generated_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < generated_columns_.bit_count(); ++i) {
    if (generated_columns_.has_member(i)) {
      if (OB_FAIL(column_ids.push_back(i + OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("store column id failed", K(i));
      }
    }
  }
  return ret;
}

int ObTableSchema::is_real_unique_index_column(ObSchemaGetterGuard &schema_guard,
                                               uint64_t column_id,
                                               bool &is_uni) const
{
  // Whether the argument column with column_id is an unique index column.
  // This interface is different with is_unique_index_column(), the latter one
  // is compatible with MySQL, for multiple columns unique index, the first column
  // is considered as MUL key but not unique key.
  int ret = OB_SUCCESS;
  is_uni = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  } else {
    for (int64_t i = 0; !is_uni && OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObSimpleTableSchemaV2 *simple_index_schema = NULL;
      const ObTableSchema *index_schema = NULL;
      if (OB_FAIL(schema_guard.get_simple_table_schema(get_tenant_id(),
                        simple_index_infos.at(i).table_id_,
                        simple_index_schema))) {
        LOG_WARN("fail to get simple table schema", K(ret), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_UNLIKELY(NULL == simple_index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("simple index schema from schema guard is NULL", K(ret), K(simple_index_schema));
      } else if (!simple_index_schema->is_unique_index()) {
        // This is not an unique index, skip.
      } else if (OB_FAIL(schema_guard.get_table_schema(get_tenant_id(),
                                                simple_index_infos.at(i).table_id_,
                                                index_schema))) {
        LOG_WARN("fail to get table schema", K(ret), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_UNLIKELY(NULL == index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index schema from schema guard is NULL", K(ret), K(index_schema));
      } else {
        // check whether the columns of unique index are not nullable
        ObTableSchema::const_column_iterator iter = index_schema->column_begin();
        for ( ; OB_SUCC(ret) && !is_uni && iter != index_schema->column_end(); iter++) {
          const ObColumnSchemaV2 *column = *iter;
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNDEFINED;
            LOG_WARN("unexpected err", K(ret), KPC(column));
          } else if (!column->is_index_column()) {
            // this column is not index column, skip
          } else if (column_id == column->get_column_id()) {
            is_uni = true;
          } else { /*do nothing*/ }
        }
      }
    } // for
  }
  return ret;
}

int ObTableSchema::has_not_null_unique_key(ObSchemaGetterGuard &schema_guard, bool &bool_result) const
{
  int ret = OB_SUCCESS;
  bool_result = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  } else {
    const uint64_t tenant_id = get_tenant_id();
    for (int64_t i = 0; !bool_result && OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = NULL;
      const ObSimpleTableSchemaV2 *simple_index_schema = NULL;
      if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
          simple_index_infos.at(i).table_id_, simple_index_schema))) {
        LOG_WARN("fail to get simple table schema", K(ret), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_UNLIKELY(NULL == simple_index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("simple index schema from schema guard is NULL", K(ret), K(simple_index_schema));
      } else if (!simple_index_schema->is_unique_index()) {
        // This is not an unique index, skip.
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
          simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get table schema", K(tenant_id),
                K(simple_index_infos.at(i).table_id_), K(ret));
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema must not be NULL", K(ret), "table_id", simple_index_infos.at(i).table_id_);
      } else {
        // check whether all index columns of this unique index are not nullable
        ObTableSchema::const_column_iterator iter = index_table_schema->column_begin();
        bool has_nullable_index_column = false;
        for ( ; OB_SUCC(ret) && !has_nullable_index_column && iter != index_table_schema->column_end(); iter++) {
          const ObColumnSchemaV2 *column = *iter;
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNDEFINED;
            LOG_WARN("unexpected err", K(ret), KPC(column));
          } else if (!column->is_index_column()) {
            // this column is not index column, skip
          } else if (false == column->is_nullable() ||             // mysql mode
                     true == column->has_not_null_constraint()) {  // oracle mode
            // this index column is not nullable, continue
          } else {
            // this index column is nullable, end loop
            has_nullable_index_column = true;
          }
        }
        if (OB_SUCC(ret) && !has_nullable_index_column) {
          // All index columns of this unique key index are not nullable, return true.
          bool_result = true;
        }
      }
    }
  }
  return ret;
}

bool ObTableSchema::has_generated_and_partkey_column() const
{
  bool result = false;
  if (has_generated_column() && is_partitioned_table() ) {
    for (int64_t i = 0; i < generated_columns_.bit_count() && !result; ++i) {
      if (generated_columns_.has_member(i)) {
        uint64_t generated_column_id = i + OB_APP_MIN_COLUMN_ID;
        const ObColumnSchemaV2 *generated_column = get_column_schema(generated_column_id);
        if (OB_NOT_NULL(generated_column)) {
          if (generated_column->is_tbl_part_key_column()) {
            result = true;
          }
        }
      }
    }
  }
  return result;
}

int ObTableSchema::check_column_has_multivalue_index_depend(
  const ObColumnSchemaV2 &data_column_schema,
  bool &has_func_idx_col_deps) const
{
  int ret = OB_SUCCESS;
  has_func_idx_col_deps = false;
  const uint64_t tenant_id = get_tenant_id();

  if (data_column_schema.has_generated_column_deps()) {
    for (ObTableSchema::const_column_iterator iter = column_begin();
        OB_SUCC(ret) && iter != column_end(); iter++) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(this));
      } else if (column->is_multivalue_generated_column()) {
        if (column->has_cascaded_column_id(data_column_schema.get_column_id())) {
          has_func_idx_col_deps = true;
          break;
        }
      }
    }
  }

  return ret;
}

int ObTableSchema::check_functional_index_columns_depend(
  const ObColumnSchemaV2 &data_column_schema,
  ObSchemaGetterGuard &schema_guard,
  bool &has_func_idx_col_deps) const
{
  int ret = OB_SUCCESS;
  has_func_idx_col_deps = false;
  const uint64_t tenant_id = get_tenant_id();
  ObHashSet<ObString> deps_gen_columns; // generated columns depend on the data column.
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (!data_column_schema.has_generated_column_deps()) {
  } else if (OB_FAIL(deps_gen_columns.create(OB_MAX_COLUMN_NUMBER/2))) {
    LOG_WARN("create hashset failed", K(ret));
  } else if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple index infos failed", K(ret));
  } else {
    for (ObTableSchema::const_column_iterator iter = column_begin();
        OB_SUCC(ret) && iter != column_end(); iter++) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(this));
      } else if (column->is_func_idx_column()) {
        // prefix index columns are hidden generated column in data table.
        if (column->has_cascaded_column_id(data_column_schema.get_column_id())
          && OB_FAIL(deps_gen_columns.set_refactored(column->get_column_name()))) {
          LOG_WARN("set refactored failed", K(ret));
        }
      } else {/* do nothing. */}
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has_func_idx_col_deps && i < simple_index_infos.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table not exist", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else {
        const ObIndexInfo &index_info = index_schema->get_index_info();
        for (int j = 0; OB_SUCC(ret) && !has_func_idx_col_deps && j < index_info.get_size(); j++) {
          const ObColumnSchemaV2 *index_col = nullptr;
          if (OB_ISNULL(index_col = index_schema->get_column_schema(index_info.get_column(j)->column_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected err", K(ret), "column_id", index_info.get_column(j)->column_id_);
          } else if (OB_FAIL(deps_gen_columns.exist_refactored(index_col->get_column_name()))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else if (OB_HASH_EXIST == ret) {
              has_func_idx_col_deps = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to check whether column has functional index dependancy", K(ret), K(index_col));
            }
          }
        }
      }
    }
  }
  return ret;
}
int ObTableSchema::check_prefix_index_columns_depend(
    const ObColumnSchemaV2 &data_column_schema,
    ObSchemaGetterGuard &schema_guard,
    bool &has_prefix_idx_col_deps) const
{
  int ret = OB_SUCCESS;
  has_prefix_idx_col_deps = false;
  ObHashSet<ObString> deps_gen_columns; // generated columns depend on the data column.
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (!data_column_schema.has_generated_column_deps()) {
  } else if (OB_FAIL(deps_gen_columns.create(OB_MAX_COLUMN_NUMBER/2))) {
    LOG_WARN("create hashset failed", K(ret));
  } else if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple index infos failed", K(ret));
  } else {
    const uint64_t tenant_id = get_tenant_id();
    for (ObTableSchema::const_column_iterator iter = column_begin();
        OB_SUCC(ret) && iter != column_end(); iter++) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(this));
      } else if (column->is_prefix_column()) {
        // prefix index columns are hidden generated column in data table.
        if (column->has_cascaded_column_id(data_column_schema.get_column_id())
          && OB_FAIL(deps_gen_columns.set_refactored(column->get_column_name()))) {
          LOG_WARN("set refactored failed", K(ret));
        }
      } else {/* do nothing. */}
    }

    for (int64_t i = 0; OB_SUCC(ret) && !has_prefix_idx_col_deps && i < simple_index_infos.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table not exist", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else {
        const ObIndexInfo &index_info = index_schema->get_index_info();
        for (int j = 0; OB_SUCC(ret) && !has_prefix_idx_col_deps && j < index_info.get_size(); j++) {
          const ObColumnSchemaV2 *index_col = nullptr;
          if (OB_ISNULL(index_col = index_schema->get_column_schema(index_info.get_column(j)->column_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected err", K(ret), "column_id", index_info.get_column(j)->column_id_);
          } else if (OB_HASH_EXIST == deps_gen_columns.exist_refactored(index_col->get_column_name())) {
            has_prefix_idx_col_deps = true;
          } else { /* do nothing. */}
        }
      }
    }
  }
  return ret;
}

// Because there are too many places to call this function, you must be careful when modifying this function,
// and it is recommended not to modify
// If you must modify this function, pay attention to whether the location of calling this function also depends on the error code
// thrown by the function
// eg: ObSchemaMgr::get_index_name depends on the existing OB_SCHEMA_ERROR error code when calling get_index_name in get_index_schema
// If the newly added error code is still OB_SCHEMA_ERROR, it need consider whether the upper-level caller has handled
// the error code correctly
int ObSimpleTableSchemaV2::get_index_name(ObString &index_name) const
{
  int ret = OB_SUCCESS;
  if (!is_index_table()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table is not index table", K(ret));
  } else if (OB_FAIL(get_index_name(table_name_, index_name))) {
    LOG_WARN("fail to get index name", K(ret), K_(table_name));
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_index_name(const ObString &table_name, ObString &index_name)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!table_name.prefix_match(OB_INDEX_PREFIX)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("index table name not in valid format", K(ret), K(table_name));
  } else {
    pos = strlen(OB_INDEX_PREFIX);

    while (NULL != table_name.ptr() &&
        pos < table_name.length() &&
        isdigit(*(table_name.ptr() + pos))) {
      ++pos;
    }
    if (pos == table_name.length()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index table name not in valid format", K(table_name), K(ret));
    } else if ('_' != *(table_name.ptr() + pos)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index table name not in valid format", K(table_name), K(ret));
    } else {
      ++pos;
      if (pos == table_name.length()) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index table name not in valid format", K(table_name), K(ret));
      } else {
        index_name.assign_ptr(table_name.ptr() + pos,
            table_name.length() - static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_EXTRACT_DATA_TABLE_ID);
uint64_t ObSimpleTableSchemaV2::extract_data_table_id_from_index_name(const ObString &index_name)
{
  int64_t pos = 0;
  ObString data_table_id_str;
  uint64_t data_table_id = OB_INVALID_ID;
  if (!index_name.prefix_match(OB_INDEX_PREFIX)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "index table name not in valid format", K(index_name));
  } else {
    pos = strlen(OB_INDEX_PREFIX);
    if (OB_UNLIKELY(ERRSIM_EXTRACT_DATA_TABLE_ID)) {
      pos = index_name.length();
      int ret = OB_SUCCESS;
      LOG_WARN("turn on error injection ERRSIM_EXTRACT_DATA_TABLE_ID", KR(ret));
    }
    while (NULL != index_name.ptr() &&
        pos < index_name.length() &&
        isdigit(*(index_name.ptr() + pos))) {
      ++pos;
    }
    if (pos + 1 >= index_name.length()) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "index table name not in valid format", K(pos), K(index_name), K(index_name.length()));
    } else if ('_' != *(index_name.ptr() + pos)) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "index table name not in valid format", K(pos), K(index_name), K(index_name.length()));
    } else {
      data_table_id_str.assign_ptr(
          index_name.ptr() + strlen(OB_INDEX_PREFIX),
          static_cast<ObString::obstr_size_t>(pos) - strlen(OB_INDEX_PREFIX));
      int ret = (common_string_unsigned_integer(
                  0, ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI, data_table_id_str, false, data_table_id));
      if (OB_FAIL(ret)) {
        data_table_id = OB_INVALID_ID;
        LOG_WARN("convert string to uint failed", KR(ret), K(data_table_id_str), K(index_name));
      }
    }
  }
  return data_table_id;
}


int ObSimpleTableSchemaV2::generate_origin_index_name()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_index_name(origin_index_name_))) {
    LOG_WARN("generate origin index name failed", K(ret), K(table_name_));
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_mlog_name(ObString &mlog_name) const
{
  int ret = OB_SUCCESS;
  if (!is_mlog_table()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table is not materialized view log", KR(ret));
  } else {
    mlog_name = table_name_;
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_if_oracle_compat_mode(bool &is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  const int64_t table_id = get_table_id();
  is_oracle_mode = false;
  return ObCompatModeGetter::check_is_oracle_mode_with_table_id(tenant_id, table_id, is_oracle_mode);
}

int ObTableSchema::get_generated_column_by_define(const ObString &col_def,
                                                  const bool only_hidden_column,
                                                  ObColumnSchemaV2 *&gen_col)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> gen_column_ids;
  if (OB_FAIL(get_generated_column_ids(gen_column_ids))) {
    LOG_WARN("get generated column ids failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && gen_col == NULL && i < gen_column_ids.count(); ++i) {
    ObColumnSchemaV2 *tmp_col = get_column_schema(gen_column_ids.at(i));
    ObString tmp_def;
    if (OB_ISNULL(tmp_col)) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not found", K(ret), K(gen_column_ids.at(i)));
    } else if (!tmp_col->is_hidden() && only_hidden_column) {
      //do nothing, continue
    } else if (OB_FAIL(tmp_col->get_cur_default_value().get_string(tmp_def))) {
      LOG_WARN("get string of current default value failed", K(ret), K(tmp_col->get_cur_default_value()));
    } else if (ObCharset::case_insensitive_equal(tmp_def, col_def)) {
      gen_col = tmp_col;
    }
  }
  return ret;
}

int64_t ObTableSchema::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("simple_table_schema");
  J_COLON();
  pos += ObSimpleTableSchemaV2::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(max_used_column_id),
      K_(sess_active_time),
      K_(rowkey_column_num),
      K_(index_column_num),
      K_(rowkey_split_pos),
      K_(block_size),
      K_(is_use_bloomfilter),
      K_(progressive_merge_num),
      K_(tablet_size),
      K_(pctfree),
      "load_type", static_cast<int32_t>(load_type_),
      "index_using_type", static_cast<int32_t>(index_using_type_),
      "def_type", static_cast<int32_t>(def_type_),
      "charset_type", static_cast<int32_t>(charset_type_),
      "collation_type", static_cast<int32_t>(collation_type_));
  J_COMMA();
  J_KV("index_status", static_cast<int32_t>(index_status_),
    "partition_status", static_cast<int32_t>(partition_status_),
    K_(code_version),
    K_(comment),
    K_(pk_comment),
    K_(create_host),
    K_(tablegroup_name),
    K_(compressor_type),
    K_(row_store_type),
    K_(store_format),
    K_(expire_info),
    K_(view_schema),
    K_(autoinc_column_id),
    K_(auto_increment),
    K_(read_only),
    K_(simple_index_infos),
    K_(base_table_ids),
    //K_(depend_table_ids),
    //K_(join_types),
    //K_(join_conds),
    K_(rowkey_info),
    K_(partition_key_info),
    K_(column_cnt),
    K_(table_dop),
    "constraints", ObArrayWrap<ObConstraint* >(cst_array_, cst_cnt_),
    "column_array", ObArrayWrap<ObColumnSchemaV2* >(column_array_, column_cnt_),
    "aux_vp_tid_array", aux_vp_tid_array_,
    K_(index_info),
    K_(define_user_id),
    K_(aux_lob_meta_tid),
    K_(aux_lob_piece_tid),
    K_(name_generated_type),
    K_(lob_inrow_threshold),
    K_(is_column_store_supported),
    K_(max_used_column_group_id),
    K_(column_group_cnt),
    "column_group_array", ObArrayWrap<ObColumnGroupSchema* >(column_group_arr_, column_group_cnt_),
    K_(mlog_tid),
    K_(auto_increment_cache_size),
    K_(local_session_vars),
    K_(index_params),
    K_(exec_env));
  J_OBJ_END();

  return pos;
}

int ObTableSchema::fill_column_collation_info()
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *it = NULL;
  int64_t column_count = get_column_count();
  for (int64_t i = 0; i < column_count; i++) {
    it = const_cast<ObTableSchema *>(this)->get_column_schema_by_idx(i);
    if (it->get_data_type() == ObVarcharType
        || it->get_data_type() == ObCharType) {
      ObCharsetType charset_type = it->get_charset_type();
      ObCollationType collation_type = it->get_collation_type();
      if (it->get_collation_type() == CS_TYPE_INVALID
          && it->get_charset_type() == CHARSET_INVALID) {
        it->set_collation_type(get_collation_type());
        it->set_charset_type(get_charset_type());
      } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type, collation_type))) {
        LOG_WARN("fail to fiil charset collation info", K(ret));
        break;
      } else {
        it->set_charset_type(charset_type);
        it->set_collation_type(collation_type);
      }
    }
  }
  return ret;
}

/*
bool ObTableSchema::same_partitions(const ObTableSchema &other) const
{
  bool bret = true;
  if (partition_num_ != other.partition_num_) {
    bret = false;
  } else if (partition_array_ != NULL && other.partition_array_ != NULL) {
    for (int i = 0; bret && i < partition_num_; ++i) {
      const ObPartition *this_part = partition_array_[i];
      const ObPartition *other_part = other.partition_array_[i];
      if (OB_ISNULL(this_part) || OB_ISNULL(other_part)) {
        bret = false;
      } else {
        bret = this_part->same_partition(*other_part);
      }
    }
  }
  return bret;
}

bool ObTableSchema::same_subpartitions(const ObTableSchema &other) const
{
  bool bret = true;
  if (subpartition_num_ != other.subpartition_num_) {
    bret = false;
  } else if (subpartition_array_ != NULL && other.subpartition_array_ != NULL) {
    for (int i = 0; bret && i < subpartition_num_; ++i) {
      const ObSubPartition *this_part = subpartition_array_[i];
      const ObSubPartition *other_part = other.subpartition_array_[i];
      if (OB_ISNULL(this_part) || OB_ISNULL(other_part)) {
        bret = false;
      } else {
        bret = this_part->same_sub_partition(*other_part);
      }
    }
  }
  return bret;
}*/

OB_DEF_SERIALIZE(ObTableSchema)
{
  int ret = OB_SUCCESS;
  if (dblink_id_ != OB_INVALID_ID) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("serialize link table schema is not implemented", K(ret));
  }
  ObSArray<int64_t> mv_table_ids;
  int64_t aux_vp_tid_array_count = aux_vp_tid_array_.count();

  // !!! begin static check
  OB_UNIS_ENCODE(tenant_id_);
  OB_UNIS_ENCODE(database_id_);
  OB_UNIS_ENCODE(tablegroup_id_);
  OB_UNIS_ENCODE(table_id_);
  OB_UNIS_ENCODE(max_used_column_id_);
  OB_UNIS_ENCODE(rowkey_column_num_);
  OB_UNIS_ENCODE(index_column_num_);
  OB_UNIS_ENCODE(rowkey_split_pos_);
  OB_UNIS_ENCODE(part_key_column_num_);
  OB_UNIS_ENCODE(block_size_);
  OB_UNIS_ENCODE(is_use_bloomfilter_);
  OB_UNIS_ENCODE(progressive_merge_num_);
  OB_UNIS_ENCODE(autoinc_column_id_);
  OB_UNIS_ENCODE(auto_increment_);
  OB_UNIS_ENCODE(read_only_);
  OB_UNIS_ENCODE(load_type_);
  OB_UNIS_ENCODE(table_type_);
  OB_UNIS_ENCODE(index_type_);
  OB_UNIS_ENCODE(def_type_);
  OB_UNIS_ENCODE(charset_type_);
  OB_UNIS_ENCODE(collation_type_);
  OB_UNIS_ENCODE(data_table_id_);
  OB_UNIS_ENCODE(index_status_);
  OB_UNIS_ENCODE(name_case_mode_);
  OB_UNIS_ENCODE(code_version_);
  OB_UNIS_ENCODE(schema_version_);
  OB_UNIS_ENCODE(part_level_);
  OB_UNIS_ENCODE(part_option_);
  OB_UNIS_ENCODE(sub_part_option_);
  OB_UNIS_ENCODE(tablegroup_name_);
  OB_UNIS_ENCODE(comment_);
  OB_UNIS_ENCODE(table_name_);
  OB_UNIS_ENCODE(compressor_type_);
  OB_UNIS_ENCODE(expire_info_);
  OB_UNIS_ENCODE(view_schema_);
  OB_UNIS_ENCODE(index_using_type_);
  OB_UNIS_ENCODE(progressive_merge_round_);
  OB_UNIS_ENCODE(storage_format_version_);
  // !!! FOR STATIC CHECKER BEGIN
  // THE FOLLOWING CODE CANNOT BE DESCRIBED USING SERIALIZE MACROS. THEY ARE EQUIVALENT TO THESE CODES:
  // OB_UNIS_ENCODE_ARRAY_POINTER(column_array_, column_cnt_);
  if (FAILEDx(serialize_columns(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "failed to serialize columns, ", K_(column_cnt));
  }
  // !!! FOR STATIC CHECKER END
  OB_UNIS_ENCODE_IF(subpart_key_column_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_ENCODE_ARRAY_POINTER_IF(partition_array_, partition_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_ENCODE_ARRAY_POINTER_IF(def_subpartition_array_, def_subpartition_num_, (PARTITION_LEVEL_TWO == part_level_));
  OB_UNIS_ENCODE(index_attributes_set_);
  OB_UNIS_ENCODE(parser_name_);
  OB_UNIS_ENCODE(depend_table_ids_);
  OB_UNIS_ENCODE(mv_table_ids); // mv_cnt_ is removed, encode 0 for compatibility
  OB_UNIS_ENCODE(tablet_size_);
  OB_UNIS_ENCODE(pctfree_);
  OB_UNIS_ENCODE(foreign_key_infos_);
  OB_UNIS_ENCODE(partition_status_);
  OB_UNIS_ENCODE(partition_schema_version_);
  OB_UNIS_ENCODE(session_id_);
  OB_UNIS_ENCODE(sess_active_time_);
  OB_UNIS_ENCODE_ARRAY_POINTER(cst_array_, cst_cnt_);
  OB_UNIS_ENCODE(pk_comment_);
  OB_UNIS_ENCODE(create_host_);
  OB_UNIS_ENCODE(row_store_type_);
  OB_UNIS_ENCODE(store_format_);
  OB_UNIS_ENCODE(duplicate_scope_);
  OB_UNIS_ENCODE_ARRAY(aux_vp_tid_array_, aux_vp_tid_array_count);
  OB_UNIS_ENCODE(table_mode_);
  OB_UNIS_ENCODE(encryption_);
  OB_UNIS_ENCODE(tablespace_id_);
  OB_UNIS_ENCODE(trigger_list_);
  OB_UNIS_ENCODE(encrypt_key_);
  OB_UNIS_ENCODE(master_key_id_);
  OB_UNIS_ENCODE(simple_index_infos_);
  OB_UNIS_ENCODE(sub_part_template_flags_);
  OB_UNIS_ENCODE(table_dop_);
  OB_UNIS_ENCODE(max_dependency_version_);
  OB_UNIS_ENCODE(association_table_id_);
  OB_UNIS_ENCODE_ARRAY_POINTER_IF(hidden_partition_array_, hidden_partition_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_ENCODE(define_user_id_);
  OB_UNIS_ENCODE(transition_point_);
  OB_UNIS_ENCODE(interval_range_);
  OB_UNIS_ENCODE(tablet_id_);
  OB_UNIS_ENCODE(aux_lob_meta_tid_);
  OB_UNIS_ENCODE(aux_lob_piece_tid_);
  OB_UNIS_ENCODE(depend_mock_fk_parent_table_ids_);
  OB_UNIS_ENCODE(table_flags_);
  OB_UNIS_ENCODE(rls_policy_ids_);
  OB_UNIS_ENCODE(rls_group_ids_);
  OB_UNIS_ENCODE(rls_context_ids_);
  OB_UNIS_ENCODE(object_status_);
  OB_UNIS_ENCODE(is_force_view_);
  OB_UNIS_ENCODE(truncate_version_);
  OB_UNIS_ENCODE(external_file_location_);
  OB_UNIS_ENCODE(external_file_location_access_info_);
  OB_UNIS_ENCODE(external_file_format_);
  OB_UNIS_ENCODE(external_file_pattern_);
  OB_UNIS_ENCODE(ttl_definition_);
  OB_UNIS_ENCODE(kv_attributes_);
  OB_UNIS_ENCODE(name_generated_type_);
  OB_UNIS_ENCODE(lob_inrow_threshold_);
  OB_UNIS_ENCODE_ARRAY_POINTER(column_group_arr_, column_group_cnt_);
  OB_UNIS_ENCODE(is_column_store_supported_);
  OB_UNIS_ENCODE(max_used_column_group_id_);
  OB_UNIS_ENCODE(mlog_tid_);
  OB_UNIS_ENCODE(auto_increment_cache_size_);
  OB_UNIS_ENCODE(external_properties_);
  OB_UNIS_ENCODE(local_session_vars_);
  OB_UNIS_ENCODE(duplicate_read_consistency_);
  OB_UNIS_ENCODE(index_params_);
  OB_UNIS_ENCODE(micro_index_clustered_);
  OB_UNIS_ENCODE(mv_mode_);
  // !!! end static check
  /*
   * 在此end static check注释前新增反序列化的成员
   * 遵循以下规则：
   * 1. 使用OB_UNIS_ENCODE等标准序列化宏进行序列化，目前已有序列化宏如下：
   *  a. OB_UNIS_ENCODE
   *  b. OB_UNIS_ENCODE_IF
   *  c. OB_UNIS_ENCODE_ARRAY_POINTER_IF
   *  d. OB_UNIS_ENCODE_ARRAY_POINTER
   *  e. OB_UNIS_ENCODE_ARRAY
   * 2. 不用添加if (OB_SUCC(ret))
   * 3. 序列化宏与反序列化宏需要一一对应
   * 4. 如果有无法使用标准序列化宏处理的成员，需要使用
   * ```cpp
   * // !!! FOR STATIC CHECKER BEGIN
   * // THE FOLLOWING CODE CANNOT BE DESCRIBED USING SERIALIZE MACROS. THEY ARE EQUIVALENT TO THESE CODES:
   * // 这里写对应的标准序列化宏
   * 这里写代码
   * // !!! FOR STATIC CHECKER END
   * ```
   * 格式的注释和代码提醒序列化检测脚本忽略该段代码
   * 详细文档:
   */
  return ret;
}

int ObTableSchema::serialize_columns(char *buf, const int64_t data_len,
                                     int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, column_cnt_))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to encode column count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    if (OB_FAIL(column_array_[i]->serialize(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to serialize column", K(ret));
    }
  }
  return ret;
}


int ObTableSchema::deserialize_columns(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column;
  //the column_cnt_ will be increased in the add_column
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to decode column count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      column.reset();
      if (OB_FAIL(column.deserialize(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN,"Fail to deserialize column", K(ret));
      } else if (OB_FAIL(add_column(column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column", K(ret));
      }
    }
  }
  return ret;
}

bool ObTableSchema::has_lob_column() const
{
  bool bool_ret = false;

  bool_ret = has_lob_aux_table();
  for (int64_t i = 0; !bool_ret && i < column_cnt_; ++i) {
    ObColumnSchemaV2& col = *column_array_[i];
    if (is_lob_storage(col.get_data_type())) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int64_t ObTableSchema::get_lob_columns_count() const
{
  int64_t lob_cols_cnt = 0;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    ObColumnSchemaV2& col = *column_array_[i];
    if (is_lob_storage(col.get_data_type())) {
      lob_cols_cnt++;
    }
  }
  return lob_cols_cnt;
}

OB_DEF_DESERIALIZE(ObTableSchema)
{
  int ret = OB_SUCCESS;
  reset();

  ObSArray<int64_t> mv_table_ids;
  int64_t aux_vp_tid_array_count = 0;
  // !!! begin static check
  OB_UNIS_DECODE(tenant_id_);
  OB_UNIS_DECODE(database_id_);
  OB_UNIS_DECODE(tablegroup_id_);
  OB_UNIS_DECODE(table_id_);
  OB_UNIS_DECODE(max_used_column_id_);
  OB_UNIS_DECODE(rowkey_column_num_);
  OB_UNIS_DECODE(index_column_num_);
  OB_UNIS_DECODE(rowkey_split_pos_);
  OB_UNIS_DECODE(part_key_column_num_);
  OB_UNIS_DECODE(block_size_);
  OB_UNIS_DECODE(is_use_bloomfilter_);
  OB_UNIS_DECODE(progressive_merge_num_);
  OB_UNIS_DECODE(autoinc_column_id_);
  OB_UNIS_DECODE(auto_increment_);
  OB_UNIS_DECODE(read_only_);
  OB_UNIS_DECODE(load_type_);
  OB_UNIS_DECODE(table_type_);
  OB_UNIS_DECODE(index_type_);
  OB_UNIS_DECODE(def_type_);
  OB_UNIS_DECODE(charset_type_);
  OB_UNIS_DECODE(collation_type_);
  OB_UNIS_DECODE(data_table_id_);
  OB_UNIS_DECODE(index_status_);
  OB_UNIS_DECODE(name_case_mode_);
  OB_UNIS_DECODE(code_version_);
  OB_UNIS_DECODE(schema_version_);
  OB_UNIS_DECODE(part_level_);
  OB_UNIS_DECODE(part_option_);
  OB_UNIS_DECODE(sub_part_option_);
  OB_UNIS_DECODE_AND_FUNC(tablegroup_name_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(comment_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(table_name_, deep_copy_str);
  OB_UNIS_DECODE(compressor_type_);
  OB_UNIS_DECODE_AND_FUNC(expire_info_, deep_copy_str);
  OB_UNIS_DECODE(view_schema_);
  OB_UNIS_DECODE(index_using_type_);
  OB_UNIS_DECODE(progressive_merge_round_);
  OB_UNIS_DECODE(storage_format_version_);
  // !!! FOR STATIC CHECKER BEGIN
  // THE FOLLOWING CODE CANNOT BE DESCRIBED USING SERIALIZE MACROS. THEY ARE EQUIVALENT TO THESE CODES:
  // OB_UNIS_DECODE_ARRAY_POINTER(column_array_, column_cnt_, add_column);
  if (FAILEDx(deserialize_columns(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "failed to deserialize columns, ", K_(column_cnt));
  }
  // !!! FOR STATIC CHECKER END
  OB_UNIS_DECODE_IF(subpart_key_column_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_DECODE_ARRAY_POINTER_IF(partition_array_, partition_num_, add_partition, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_DECODE_ARRAY_POINTER_IF(def_subpartition_array_, def_subpartition_num_, add_def_subpartition, (PARTITION_LEVEL_TWO == part_level_));
  OB_UNIS_DECODE(index_attributes_set_);
  OB_UNIS_DECODE_AND_FUNC(parser_name_, deep_copy_str);
  OB_UNIS_DECODE(depend_table_ids_);
  OB_UNIS_DECODE(mv_table_ids);
  OB_UNIS_DECODE(tablet_size_);
  OB_UNIS_DECODE(pctfree_);
  OB_UNIS_DECODE(foreign_key_infos_);
  OB_UNIS_DECODE(partition_status_);
  OB_UNIS_DECODE(partition_schema_version_);
  OB_UNIS_DECODE(session_id_);
  OB_UNIS_DECODE(sess_active_time_);
  OB_UNIS_DECODE_ARRAY_POINTER(cst_array_, cst_cnt_, add_constraint);
  OB_UNIS_DECODE_AND_FUNC(pk_comment_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(create_host_, deep_copy_str);
  OB_UNIS_DECODE(row_store_type_);
  OB_UNIS_DECODE(store_format_);
  OB_UNIS_DECODE(duplicate_scope_);
  OB_UNIS_DECODE_ARRAY_AND_FUNC(aux_vp_tid_array_, aux_vp_tid_array_count, add_aux_vp_tid);
  OB_UNIS_DECODE(table_mode_);
  OB_UNIS_DECODE_AND_FUNC(encryption_, deep_copy_str);
  OB_UNIS_DECODE(tablespace_id_);
  OB_UNIS_DECODE(trigger_list_);
  OB_UNIS_DECODE_AND_FUNC(encrypt_key_, deep_copy_str);
  OB_UNIS_DECODE(master_key_id_);
  OB_UNIS_DECODE(simple_index_infos_);
  OB_UNIS_DECODE(sub_part_template_flags_);
  OB_UNIS_DECODE(table_dop_);
  OB_UNIS_DECODE(max_dependency_version_);
  OB_UNIS_DECODE(association_table_id_);
  OB_UNIS_DECODE_ARRAY_POINTER_IF(hidden_partition_array_, hidden_partition_num_, add_partition, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_DECODE(define_user_id_);

  // !!! FOR STATIC CHECKER BEGIN
  // THE FOLLOWING CODE CANNOT BE DESCRIBED USING SERIALIZE MACROS. THEY ARE EQUIVALENT TO THESE CODES:
  // OB_UNIS_DECODE(transition_point_);
  // OB_UNIS_DECODE(interval_range_);
  if (OB_SUCC(ret)) {
    static int64_t ROW_KEY_CNT = 1;
    ObObj obj_array[ROW_KEY_CNT];
    obj_array[0].reset();

    ObRowkey rowkey;
    rowkey.assign(obj_array, ROW_KEY_CNT);
    if (FAILEDx(rowkey.deserialize(buf, data_len, pos, true))) {
      LOG_WARN("fail to deserialize transintion point rowkey", KR(ret));
    } else if (OB_FAIL(set_transition_point(rowkey))) {
      LOG_WARN("Fail to deep copy high_bound_val", K(ret), K(rowkey));
    }

    obj_array[0].reset();
    rowkey.assign(obj_array, ROW_KEY_CNT);
    if (FAILEDx(rowkey.deserialize(buf, data_len, pos, true))) {
      LOG_WARN("fail to deserialize interval range rowkey", KR(ret));
    } else if (OB_FAIL(set_interval_range(rowkey))) {
      LOG_WARN("Fail to deep copy high_bound_val", K(ret), K(rowkey));
    }
  }
  // !!! FOR STATIC CHECKER END

  OB_UNIS_DECODE(tablet_id_);
  OB_UNIS_DECODE(aux_lob_meta_tid_);
  OB_UNIS_DECODE(aux_lob_piece_tid_);
  OB_UNIS_DECODE(depend_mock_fk_parent_table_ids_);
  OB_UNIS_DECODE(table_flags_);
  OB_UNIS_DECODE(rls_policy_ids_);
  OB_UNIS_DECODE(rls_group_ids_);
  OB_UNIS_DECODE(rls_context_ids_);
  OB_UNIS_DECODE(object_status_);
  OB_UNIS_DECODE(is_force_view_);
  OB_UNIS_DECODE(truncate_version_);
  OB_UNIS_DECODE_AND_FUNC(external_file_location_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(external_file_location_access_info_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(external_file_format_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(external_file_pattern_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(ttl_definition_, deep_copy_str);
  OB_UNIS_DECODE_AND_FUNC(kv_attributes_, deep_copy_str);
  OB_UNIS_DECODE(name_generated_type_);
  OB_UNIS_DECODE(lob_inrow_threshold_);
  OB_UNIS_DECODE_ARRAY_POINTER(column_group_arr_, column_group_cnt_, do_add_column_group);
  OB_UNIS_DECODE(is_column_store_supported_);
  OB_UNIS_DECODE(max_used_column_group_id_);
  OB_UNIS_DECODE(mlog_tid_);
  OB_UNIS_DECODE(auto_increment_cache_size_);
  OB_UNIS_DECODE_AND_FUNC(external_properties_, deep_copy_str);
  OB_UNIS_DECODE(local_session_vars_);
  OB_UNIS_DECODE(duplicate_read_consistency_);
  OB_UNIS_DECODE_AND_FUNC(index_params_, deep_copy_str);
  OB_UNIS_DECODE(micro_index_clustered_);
  OB_UNIS_DECODE(mv_mode_);
  // !!! end static check
  /*
   * 在此end static check注释前新增反序列化的成员
   * 遵循以下规则：
   * 1. 使用OB_UNIS_DECODE等标准序列化宏进行序列化，目前已有序列化宏如下，其中提供FUNC的宏可用于拷贝数据，例如将复制ObString：
   *  a. OB_UNIS_DECODE_ARRAY
   *  b. OB_UNIS_DECODE_ARRAY_AND_FUNC
   *  c. OB_UNIS_DECODE_ARRAY_POINTER_IF
   *  d. OB_UNIS_DECODE_AND_FUNC
   *  e. OB_UNIS_DECODE_IF
   *  f. OB_UNIS_DECODE
   * 2. 不用添加if (OB_SUCC(ret))
   * 3. 序列化宏与反序列化宏需要一一对应
   * 4. 如果有无法使用标准序列化宏处理的成员，需要使用
   * ```cpp
   * // !!! FOR STATIC CHECKER BEGIN
   * // THE FOLLOWING CODE CANNOT BE DESCRIBED USING SERIALIZE MACROS. THEY ARE EQUIVALENT TO THESE CODES:
   * // 这里写对应的标准序列化宏
   * 这里写代码
   * // !!! FOR STATIC CHECKER END
   * ```
   * 格式的注释和代码提醒序列化检测脚本忽略该段代码
   * 详细文档:
   */
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSchema)
{
  int64_t len = 0;
  ObSArray<int64_t> mv_table_ids;
  int64_t aux_vp_tid_array_count = aux_vp_tid_array_.count();

  // !!! begin static check
  OB_UNIS_ADD_LEN(tenant_id_);
  OB_UNIS_ADD_LEN(database_id_);
  OB_UNIS_ADD_LEN(tablegroup_id_);
  OB_UNIS_ADD_LEN(table_id_);
  OB_UNIS_ADD_LEN(max_used_column_id_);
  OB_UNIS_ADD_LEN(rowkey_column_num_);
  OB_UNIS_ADD_LEN(index_column_num_);
  OB_UNIS_ADD_LEN(rowkey_split_pos_);
  OB_UNIS_ADD_LEN(part_key_column_num_);
  OB_UNIS_ADD_LEN(block_size_);
  OB_UNIS_ADD_LEN(is_use_bloomfilter_);
  OB_UNIS_ADD_LEN(progressive_merge_num_);
  OB_UNIS_ADD_LEN(autoinc_column_id_);
  OB_UNIS_ADD_LEN(auto_increment_);
  OB_UNIS_ADD_LEN(read_only_);
  OB_UNIS_ADD_LEN(load_type_);
  OB_UNIS_ADD_LEN(table_type_);
  OB_UNIS_ADD_LEN(index_type_);
  OB_UNIS_ADD_LEN(def_type_);
  OB_UNIS_ADD_LEN(charset_type_);
  OB_UNIS_ADD_LEN(collation_type_);
  OB_UNIS_ADD_LEN(data_table_id_);
  OB_UNIS_ADD_LEN(index_status_);
  OB_UNIS_ADD_LEN(name_case_mode_);
  OB_UNIS_ADD_LEN(code_version_);
  OB_UNIS_ADD_LEN(schema_version_);
  OB_UNIS_ADD_LEN(part_level_);
  OB_UNIS_ADD_LEN(part_option_);
  OB_UNIS_ADD_LEN(sub_part_option_);
  OB_UNIS_ADD_LEN(tablegroup_name_);
  OB_UNIS_ADD_LEN(comment_);
  OB_UNIS_ADD_LEN(table_name_);
  OB_UNIS_ADD_LEN(compressor_type_);
  OB_UNIS_ADD_LEN(expire_info_);
  OB_UNIS_ADD_LEN(view_schema_);
  OB_UNIS_ADD_LEN(index_using_type_);
  OB_UNIS_ADD_LEN(progressive_merge_round_);
  OB_UNIS_ADD_LEN(storage_format_version_);
  OB_UNIS_ADD_LEN_ARRAY_POINTER(column_array_, column_cnt_);
  OB_UNIS_ADD_LEN_IF(subpart_key_column_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_ADD_LEN_ARRAY_POINTER_IF(partition_array_, partition_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_ADD_LEN_ARRAY_POINTER_IF(def_subpartition_array_, def_subpartition_num_, (PARTITION_LEVEL_TWO == part_level_));
  OB_UNIS_ADD_LEN(index_attributes_set_);
  OB_UNIS_ADD_LEN(parser_name_);
  OB_UNIS_ADD_LEN(depend_table_ids_);
  OB_UNIS_ADD_LEN(mv_table_ids); //mv_tid compatiable
  OB_UNIS_ADD_LEN(tablet_size_);
  OB_UNIS_ADD_LEN(pctfree_);
  OB_UNIS_ADD_LEN(foreign_key_infos_);
  OB_UNIS_ADD_LEN(partition_status_);
  OB_UNIS_ADD_LEN(partition_schema_version_);
  OB_UNIS_ADD_LEN(session_id_);
  OB_UNIS_ADD_LEN(sess_active_time_);
  OB_UNIS_ADD_LEN_ARRAY_POINTER(cst_array_, cst_cnt_);
  OB_UNIS_ADD_LEN(pk_comment_);
  OB_UNIS_ADD_LEN(create_host_);
  OB_UNIS_ADD_LEN(row_store_type_);
  OB_UNIS_ADD_LEN(store_format_);
  OB_UNIS_ADD_LEN(duplicate_scope_);
  OB_UNIS_ADD_LEN_ARRAY(aux_vp_tid_array_, aux_vp_tid_array_count);
  OB_UNIS_ADD_LEN(table_mode_);
  OB_UNIS_ADD_LEN(encryption_);
  OB_UNIS_ADD_LEN(tablespace_id_);
  OB_UNIS_ADD_LEN(trigger_list_);
  OB_UNIS_ADD_LEN(encrypt_key_);
  OB_UNIS_ADD_LEN(master_key_id_);
  OB_UNIS_ADD_LEN(simple_index_infos_);
  OB_UNIS_ADD_LEN(sub_part_template_flags_);
  OB_UNIS_ADD_LEN(table_dop_);
  OB_UNIS_ADD_LEN(max_dependency_version_);
  OB_UNIS_ADD_LEN(association_table_id_);
  OB_UNIS_ADD_LEN_ARRAY_POINTER_IF(hidden_partition_array_, hidden_partition_num_, (PARTITION_LEVEL_ONE <= part_level_));
  OB_UNIS_ADD_LEN(define_user_id_);
  OB_UNIS_ADD_LEN(transition_point_);
  OB_UNIS_ADD_LEN(interval_range_);
  OB_UNIS_ADD_LEN(tablet_id_);
  OB_UNIS_ADD_LEN(aux_lob_meta_tid_);
  OB_UNIS_ADD_LEN(aux_lob_piece_tid_);
  OB_UNIS_ADD_LEN(depend_mock_fk_parent_table_ids_);
  OB_UNIS_ADD_LEN(table_flags_);
  OB_UNIS_ADD_LEN(rls_policy_ids_);
  OB_UNIS_ADD_LEN(rls_group_ids_);
  OB_UNIS_ADD_LEN(rls_context_ids_);
  OB_UNIS_ADD_LEN(object_status_);
  OB_UNIS_ADD_LEN(is_force_view_);
  OB_UNIS_ADD_LEN(truncate_version_);
  OB_UNIS_ADD_LEN(external_file_location_);
  OB_UNIS_ADD_LEN(external_file_location_access_info_);
  OB_UNIS_ADD_LEN(external_file_format_);
  OB_UNIS_ADD_LEN(external_file_pattern_);
  OB_UNIS_ADD_LEN(ttl_definition_);
  OB_UNIS_ADD_LEN(kv_attributes_);
  OB_UNIS_ADD_LEN(name_generated_type_);
  OB_UNIS_ADD_LEN(lob_inrow_threshold_);
  OB_UNIS_ADD_LEN_ARRAY_POINTER(column_group_arr_, column_group_cnt_);
  OB_UNIS_ADD_LEN(is_column_store_supported_);
  OB_UNIS_ADD_LEN(max_used_column_group_id_);
  OB_UNIS_ADD_LEN(mlog_tid_);
  OB_UNIS_ADD_LEN(auto_increment_cache_size_);
  OB_UNIS_ADD_LEN(external_properties_);
  OB_UNIS_ADD_LEN(local_session_vars_);
  OB_UNIS_ADD_LEN(duplicate_read_consistency_);
  OB_UNIS_ADD_LEN(index_params_);
  OB_UNIS_ADD_LEN(micro_index_clustered_);
  OB_UNIS_ADD_LEN(mv_mode_);
  // !!! end static check
  /*
   * 在此end static check注释前新增反序列化的成员
   * 遵循以下规则：
   * 1. 使用OB_UNIS_DECODE等标准序列化宏进行序列化，目前已有序列化宏如下：
   *  a. OB_UNIS_ADD_LEN_ARRAY
   *  b. OB_UNIS_ADD_LEN_ARRAY_POINTER_IF
   *  c. OB_UNIS_ADD_LEN_IF
   *  d. OB_UNIS_ADD_LEN
   * 2. 不用添加if (OB_SUCC(ret))
   * 3. 序列化宏与反序列化宏需要一一对应
   * 4. 如果有无法使用标准序列化宏处理的成员，需要使用
   * ```cpp
   * // !!! FOR STATIC CHECKER BEGIN
   * // THE FOLLOWING CODE CANNOT BE DESCRIBED USING SERIALIZE MACROS. THEY ARE EQUIVALENT TO THESE CODES:
   * // 这里写对应的标准序列化宏
   * 这里写代码
   * // !!! FOR STATIC CHECKER END
   * ```
   * 格式的注释和代码提醒序列化检测脚本忽略该段代码
   * 详细文档:
   */
  return len;
}

// the part_level of an auto-partitioned table might be changed after auto partitioning is triggered:
//    if the table is a no primary key table,
//        target_part_level = PARTITION_LEVEL_MAX
//    if the table is non-partitioned table and presetting partition key matches rowkey prefix,
//        target_part_level = PARTITION_LEVEL_ONE
//    if the table is non-partitioned table and presetting partition key doesn't match rowkey prefix,
//        target_part_level = PARTITION_LEVEL_TWO (TODO)
//    if the table is partitioned table and partition key matches rowkey prefix,
//        target_part_level = PARTITION_LEVEL_ONE
//    if the table is partitioned table and partition key doesn't match rowkey prefix,
//        target_part_level = PARTITION_LEVEL_TWO (TODO)
//    if the table is subpartitioned table and partition key, subpartition key match rowkey prefix,
//        target_part_level = PARTITION_LEVEL_TWO (TODO)
//    if the table is subpartitioned table and partition key, subpartition key don't match rowkey prefix,
//        target_part_level = PARTITION_LEVEL_MAX
// Attention:
// PARTITION_LEVEL_MAX is invalid partition level which means the function run failed or
// the table can not auto split partition (such as no-primary-key table).
// caller should check whether can continue if get target_part_level of PARTITION_LEVEL_MAX.
ObPartitionLevel ObTableSchema::get_target_part_level_for_auto_partitioned_table() const
{
  int ret = OB_SUCCESS;
  ObPartitionLevel target_part_level = PARTITION_LEVEL_MAX;
  if (is_auto_partitioned_table()) {
    bool match_rowkey_prefix = false;
    if (is_heap_table()) {
      // not allow to auto partitioning no primary key table
      // target_part_level = PARTITION_LEVEL_MAX
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get target part level from a no primary key table", KR(ret), KPC(this));
    } else if (part_level_ == PARTITION_LEVEL_ZERO) {
      if (OB_FAIL(is_presetting_partition_key_match_rowkey_prefix(match_rowkey_prefix))) {
        // target_part_level = PARTITION_LEVEL_MAX;
        LOG_WARN("fail to check whether presetting partition key matches rowkey prefix", KR(ret), KPC(this));
      } else if (match_rowkey_prefix) {
        // when trigger auto partitioning, the non-partitioned table will be set as partitioned table
        target_part_level = PARTITION_LEVEL_ONE;
      } else {
        // when trigger auto partitioning, the non-partitioned table will be set as sub-partitioned table
        target_part_level = PARTITION_LEVEL_TWO;
      }
    } else if (OB_FAIL(is_partition_key_match_rowkey_prefix(match_rowkey_prefix))) {
      // target_part_level = PARTITION_LEVEL_MAX;
      LOG_WARN("fail to check primary key match partition column", KR(ret), KPC(this));
    } else if (part_level_ == PARTITION_LEVEL_ONE) {
      if (match_rowkey_prefix) {
        // when trigger auto partitioning, the partition will be split into two partitions
        target_part_level = PARTITION_LEVEL_ONE;
      } else {
        // when trigger auto partitioning, the partitioned table will be set as sub-partitioned table,
        target_part_level = PARTITION_LEVEL_TWO;
      }
    } else if (part_level_ == PARTITION_LEVEL_TWO) {
      if (match_rowkey_prefix) {
        // when trigger auto partitioning, the subpartition will be split into two subpartitions
        target_part_level = PARTITION_LEVEL_TWO;
      } else {
        // not allow to split
        // target_part_level = PARTITION_LEVEL_MAX;
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get target part level from a subpartitioned table"
                 "with mismatching between partition key, subpartition key and primary key prefix",
                 KR(ret), KPC(this));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid part level", KR(ret), K(part_level_));
    }
  } else {
    // target_part_level = PARTITION_LEVEL_MAX
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get target part level from a non-auto-partitioned table", KR(ret), KPC(this));
  }

  return target_part_level;
}

int ObTableSchema::check_validity_for_auto_partition() const
{
  int ret = OB_SUCCESS;
  if (is_auto_partitioned_table()) {
    if (OB_FAIL(check_enable_split_partition(true /*is_auto_partitioning*/))) {
      LOG_WARN("fail to check enable split partition", KR(ret));
    }
  }
  return ret;
}

int ObTableSchema::check_can_do_manual_split_partition() const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_enable_split_partition(false /*is_auto_partitioning*/))) {
    LOG_WARN("fail to check enable split partition", KR(ret));
  }

  return ret;
}

// 1. not support data version which is lower than 4.4
// 2. not support to split partition of sys_table/local_index/domain_index/spatial_index/lob
// 3. not support to split partition of no primary key table
// 4. not support to split partition of a table including spatial index or domain index
// 5. not support to split hash/list/interval partition
// 6. not support to split subpartition
// 7. not support mismatching between partition key and primary key prefix
// 8. not support column store table to split partition
// 9. only support automatic partitioning global index tables in non user tables
int ObTableSchema::check_enable_split_partition(bool is_auto_partitioning) const
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  bool is_table_column_store = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id_));
  } else if (data_version < DATA_VERSION_4_3_4_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current data version doesn't support to split partition", KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version lower than 4.4 is");
  } else if (is_in_recyclebin()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to split table in recyclebin", KR(ret), KPC(this));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "recyclebin table is");
  } else if (is_heap_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to split a partition of no primary key table", KR(ret), KPC(this));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reorganizing table without primary key(s) is");
  } else if (OB_FAIL(get_is_column_store(is_table_column_store))) {
    LOG_WARN("failed to get is column store", K(ret));
  } else if (is_table_column_store) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to split a partition of column store table", KR(ret), KPC(this));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "column store table is");
  } else if (is_user_table()) {
    // check indexes of auto-partitioned data table
    ObArray<ObAuxTableMetaInfo> simple_index_infos;

    if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get_simple_index_infos failed", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        if (is_spatial_index(simple_index_infos[i].index_type_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support spatial index", KR(ret), K(simple_index_infos[i].index_type_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition of a table with spatial index is");
        } else if (share::schema::is_fts_index(simple_index_infos[i].index_type_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support fulltext index", KR(ret), K(simple_index_infos[i].index_type_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition of a table with fulltext index is");
        } else if (share::schema::is_multivalue_index(simple_index_infos[i].index_type_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support multivalue index", KR(ret), K(simple_index_infos[i].index_type_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition of a table with multivalue index is");
        } else if (share::schema::is_vec_index(simple_index_infos[i].index_type_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support vec index", KR(ret), K(simple_index_infos[i].index_type_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition of a table with vec index is");
        } else if (INDEX_TYPE_DOMAIN_CTXCAT_DEPRECATED == simple_index_infos[i].index_type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support domain index", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition of a table with domain index is");
        }
      } // end for
    }
  } else { // !is_user_table()
    // 1. manual partition split is only supported for user-table
    // 2. auto partition split is supported for user-table and global index
    if (is_materialized_view() || is_mlog_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of materialized view or materialized view log table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "materialized view or materialized view log table is");
    } else if (is_index_table() && is_fts_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of fulltext index table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "fulltext index table is");
    } else if (is_index_table() && is_multivalue_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of multivalue index table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "multivalue index table is");
    } else if (is_index_table() && is_spatial_index()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of spatial index", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "spatial index is");
    } else if (is_index_table() && share::schema::is_vec_index(index_type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of vec index", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "spatial index is");
    } else if (is_tmp_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of temporary table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "temporary table is");
    } else if (is_external_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of external table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "external table is");
    } else if (is_aux_lob_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of auxiliary table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "auxiliary table is");
    } else if (is_aux_vp_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to split a partition of auxiliary vertical partition table", KR(ret), KPC(this));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "vertical partition table is");
    } else if (is_auto_partitioning && is_global_index_table()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table type", KR(ret), KPC(this));
    }
  }

  ObPartitionLevel target_part_level = is_auto_partitioning ?
                                       get_target_part_level_for_auto_partitioned_table() :
                                       get_part_level();
  if (OB_FAIL(ret)) {
  } else if (!is_valid_split_part_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support to split range or range columns partition", KR(ret), K(part_option_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "hash/list/interval partition is");
  } else if (target_part_level != ObPartitionLevel::PARTITION_LEVEL_ONE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to split subpartition", KR(ret), KPC(this));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "split subpartition is");
  } else if (!is_auto_partitioning) {
    bool is_match_rowkey_prefix = false;
    if (OB_FAIL(is_partition_key_match_rowkey_prefix(is_match_rowkey_prefix))) {
      LOG_WARN("fail to check whether primary key prefix match partition columns", KR(ret));
    } else if (!is_match_rowkey_prefix) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("split partition should keep primary key prefix equal to partition key", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "mismatching between primary key prefix and partition key is");
    }
  }

  return ret;
}

// for auto-partitioned non-partitioned table,
// the empty part_func_expr means when the table/global_index triggers auto partitioning,
// the primary_keys/index_columns will be choosen as partition key.
// thus, if using_auto_partitioned_mode==true, we will generate the "possible" partition key
// for auto-partitioned non-partitioned table with empty part_func_expr
int ObTableSchema::get_part_func_expr_str(ObString &part_func_expr, common::ObIAllocator &allocator,
                                          const bool using_auto_partitioned_mode) const
{
  int ret = OB_SUCCESS;
  part_func_expr.reset();
  if (!part_option_.get_part_func_expr_str().empty()) {
    if (OB_FAIL(ob_write_string(allocator, part_option_.get_part_func_expr_str(),
                                part_func_expr, true /*c_style*/))) {
      LOG_WARN("fail to write string", KR(ret), KPC(this));
    }
  } else if (is_auto_partitioned_table() && !is_partitioned_table() &&
             (is_user_table() || is_global_index_table()) &&
             using_auto_partitioned_mode) {
    ObSqlString presetting_partition_key_str;
    const ObRowkeyInfo &presetting_partition_keys = is_global_index_table() ?
                                                    get_index_info() : get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < presetting_partition_keys.get_size(); ++i) {
      const ObRowkeyColumn *partition_column = presetting_partition_keys.get_column(i);
      const ObColumnSchemaV2 *column = nullptr;
      if (OB_ISNULL(partition_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition key is NULL, ", KR(ret), K(i), K(presetting_partition_keys), KPC(this));
      } else if (is_shadow_column(partition_column->column_id_)) {
      } else if (OB_ISNULL(column = get_column_schema(partition_column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the column schema is NULL, ", KR(ret), KPC(partition_column), KPC(this));
      } else if (OB_FAIL(presetting_partition_key_str.append(column->get_column_name()))) {
        LOG_WARN("fail to append str", KR(ret), KPC(column), KPC(this));
      } else if (i != presetting_partition_keys.get_size() - 1 &&
                 OB_FAIL(presetting_partition_key_str.append(", "))) {
        LOG_WARN("fail to append str", KR(ret), KPC(column), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(presetting_partition_key_str.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to acquire presetting partition keys", KR(ret), KPC(this));
      } else if (OB_FAIL(ob_write_string(allocator, presetting_partition_key_str.string(),
                                         part_func_expr, true /*c_style*/))) {
        LOG_WARN("fail to write string", KR(ret), KPC(this));
      }
    }
  }

  return ret;
}

// if an auto-partitioned table is non-partitioned table,
// the "part_func_expr" of it records its presetting partition key.
// the presetting partition key will be set as partition key
// when the auto-partitioned table triggers partition splitting.
// if the part_func_expr is not empty, the columns of it are equal to presetting partition key;
// if the part_func_expr is empty, it means:
//    1. for user_table, the presetting partition key will always equal to "current" primary key,
//    2. for global_index, the presetting partition key will always equal to its index columns.
// Attention:
//    due to we only allow user-table to set presetting key by using auto-partition clause,
//    the presetting partition key of global index must be index columns (part_func_expr is empty).
int ObTableSchema::get_presetting_partition_keys(common::ObIArray<uint64_t> &partition_key_ids) const
{
  int ret = OB_SUCCESS;
  static const char DELIMITER = ',';
  ObArenaAllocator alloc;
  ObString part_func_str;
  const ObString &ori_part_func_str = part_option_.get_part_func_expr_str();
  bool is_oracle_mode = false;
  partition_key_ids.reset();

  if (!is_auto_partitioned_table() || is_partitioned_table()) {
    // only a auto-partitioned non-partitioned table has presetting partition key
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("attempt to get presetting partition key from invalid table",
              KR(ret), K(is_auto_partitioned_table()), K(is_partitioned_table()), KPC(this));
  } else if (!is_user_table() && !is_global_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table type", KR(ret), KPC(this));
  } else if (ori_part_func_str.empty()) {
    const ObRowkeyInfo &partition_keys = is_global_index_table() ? get_index_info() : get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_keys.get_size(); ++i) {
      const ObRowkeyColumn *partition_column = partition_keys.get_column(i);
      if (OB_ISNULL(partition_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition key is NULL, ", KR(ret), K(i), K(partition_keys), KPC(this));
      } else if (!is_shadow_column(partition_column->column_id_) &&
                 OB_FAIL(partition_key_ids.push_back(partition_column->column_id_))) {
        LOG_WARN("failed to push back rowkey column id", KR(ret), KPC(this));
      }
    }
  } else if (!is_user_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only user-table could set presetting partition key specially", KR(ret), KPC(this));
  } else if (OB_FAIL(ob_write_string(alloc, ori_part_func_str, part_func_str, true /*c_style*/))) {
    LOG_WARN("fail to write string", KR(ret), KPC(this));
  } else if (OB_FAIL(get_partition_keys_by_part_func_expr(part_func_str, partition_key_ids))) {
    LOG_WARN("fail to get partition key", K(ret), K(part_func_str), K(ori_part_func_str));
  }
  return ret;
}

int ObTableSchema::get_partition_keys_by_part_func_expr(common::ObString &part_func_expr_str, common::ObIArray<uint64_t> &partition_key_ids) const
{
  int ret = OB_SUCCESS;
  static const char DELIMITER = ',';
  bool is_oracle_mode = false;

  if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret));
  } else {
    ObArray<ObString> presetting_partition_keys;
    const char quote = is_oracle_mode ? '\"' : '`';
    bool parse_over = false;
    while (OB_SUCC(ret) && !parse_over) {
      // attention:
      // after calling split_on() for splitting string "a,b",
      // the function will return "a" and set the origin string as "b";
      // if the string couldn't be split, the function will return empty string
      ObString partition_key_name = part_func_expr_str.split_on(DELIMITER);
      if (partition_key_name.empty()) {
        parse_over = true;
        partition_key_name = part_func_expr_str;
      }
      // trim quote
      char* start = partition_key_name.ptr();
      char* end = partition_key_name.ptr() + partition_key_name.length() - 1;
      int64_t length = partition_key_name.length();
      if (OB_NOT_NULL(start) && OB_NOT_NULL(end) && length > 0) {
        if (*start == quote && *end == quote) {
          length -= 2;
          start++;
        } else if (*start != quote && *end != quote) {
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid part func str", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (FALSE_IT(partition_key_name.assign_ptr(start, length))) {
        } else if (is_oracle_mode && FALSE_IT(str_toupper(partition_key_name.ptr(),
                                                          partition_key_name.length()))) {
          // column name in oracle mode is capital
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(presetting_partition_keys.push_back(partition_key_name))) {
          LOG_WARN("fail to push back", KR(ret), K(partition_key_name));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < presetting_partition_keys.count(); i++) {
      const ObString& partition_key_name = presetting_partition_keys.at(i).trim();
      const ObColumnSchemaV2 *column = get_column_schema(partition_key_name);

      if (OB_ISNULL(column)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("fail to get column schema", KR(ret), K(partition_key_name));
      } else if (OB_FAIL(partition_key_ids.push_back(column->get_column_id()))) {
        LOG_WARN("fail to push back", KR(ret), KPC(column));
      }
    }
  }

  return ret;
}

int ObTableSchema::is_presetting_partition_key(const uint64_t partition_key_id,
                                               bool &is_presetting_partition_key) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> partition_key_ids;
  is_presetting_partition_key = false;

  if (OB_FAIL(get_presetting_partition_keys(partition_key_ids))) {
    LOG_WARN("fail to get presetting partition key", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; !is_presetting_partition_key && i < partition_key_ids.count(); i++) {
      if (partition_key_id == partition_key_ids.at(i)) {
        is_presetting_partition_key = true;
      }
    }
  }

  return ret;
}

int ObTableSchema::check_primary_key_cover_partition_column()
{
  int ret = OB_SUCCESS;
  if (!is_partitioned_table() || is_heap_table()) {
    //nothing todo
  } else if (OB_FAIL(check_rowkey_cover_partition_keys(partition_key_info_))) {
    LOG_WARN("Check rowkey cover partition key failed", K(ret));
  } else if (OB_FAIL(check_rowkey_cover_partition_keys(subpartition_key_info_))) {
    LOG_WARN("Check rowkey cover subpartition key failed", K(ret));
  }

  return ret;
}

int ObTableSchema::check_rowkey_cover_partition_keys(const ObPartitionKeyInfo &part_key_info)
{
  int ret = OB_SUCCESS;
  bool is_rowkey = true;
  uint64_t column_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && is_rowkey && i < part_key_info.get_size(); ++i) {
    if (OB_FAIL(part_key_info.get_column_id(i, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (OB_FAIL(rowkey_info_.is_rowkey_column(column_id, is_rowkey))) {
      LOG_WARN("Failed to check is rowkey column", K(ret));
    } else if (!is_rowkey) {
      const ObColumnSchemaV2 *column_schema = NULL;
      if (OB_ISNULL(column_schema = get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (column_schema->is_generated_column()) {
        ObSEArray<uint64_t, 5> cascaded_columns;
        if (OB_FAIL(column_schema->get_cascaded_column_ids(cascaded_columns))) {
          LOG_WARN("Failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t idx = 0; OB_SUCC(ret) && idx < cascaded_columns.count(); ++idx) {
            if (OB_FAIL(rowkey_info_.is_rowkey_column(cascaded_columns.at(idx), is_rowkey))) {
              LOG_WARN("Failed to check is rowkey column", K(ret));
            } else if (!is_rowkey) {
              ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
              LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "PRIMARY KEY");
            } else { }//do nothing
          }
        }
      } else {
        ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
        LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "PRIMARY KEY");
      }
    } else { }//do nothing
  }
  return ret;
}
// No primary key table Check whether the newly added index table meets the requirements
// The index table must contain all partition keys
int ObTableSchema::check_create_index_on_hidden_primary_key(
    const ObTableSchema &index_table) const
{
  int ret = OB_SUCCESS;
  if (!index_table.is_index_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_table));
  } else if (!is_partitioned_table()
             || !is_heap_table()
             || (is_heap_table() && index_table.is_index_local_storage())) {
    // update 2021.11: for 4.0 new heap table, index table doesn't need to cover partition keys
    // If it is not a non-partitioned table, or is not a hidden primary key, there is no need to check
  } else if (OB_FAIL(index_table.check_index_table_cover_partition_keys(partition_key_info_))) {
    LOG_WARN("failed to check index table cover partition key", K(ret), K_(partition_key_info));
  }
  return ret;
}

int ObTableSchema::check_index_table_cover_partition_keys(
    const common::ObPartitionKeyInfo &part_key) const
{
  int ret = OB_SUCCESS;
  if (!is_index_table()
      || !part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), "index_table", *this);
  }
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *column_schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key.get_size(); ++i) {
    if (OB_FAIL(part_key.get_column_id(i, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (OB_ISNULL(column_schema = get_column_schema(column_id))) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("can not find partition key in index", K(ret), K(column_id),
                K(part_key), "index_schema", *this);
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition key not in index table");
    } else { }//do nothing
  }
  return ret;
}

int ObTableSchema::check_skip_index_valid() const
{
  int ret = OB_SUCCESS;
  int64_t aggregate_row_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    const ObColumnSchemaV2 *column_schema = nullptr;
    int64_t column_agg_maximum_size = 0;
    if (OB_ISNULL(column_schema = column_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nul column", K(ret), K(i));
    } else if (!column_schema->get_skip_index_attr().has_skip_index()) {
      // skip
    } else if (OB_UNLIKELY(column_schema->is_virtual_generated_column())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "skip index on virtual generated column");
      LOG_WARN("unexpected skip index on virtual generated column", K(ret), KPC(column_schema));
    } else if (OB_UNLIKELY(is_skip_index_black_list_type(column_schema->get_meta_type().get_type()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "build skip index on invalid type");
      LOG_WARN("not supported skip index on column with invalid column type", K(ret), KPC(column_schema));
    } else if (column_schema->get_skip_index_attr().has_sum() &&
               !can_agg_sum(column_schema->get_meta_type().get_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "build skip index on invalid type");
      LOG_WARN("not supported skip index on column with invalid column type", K(ret), KPC(column_schema));
    } else if (OB_FAIL(blocksstable::ObSkipIndexColMeta::calc_skip_index_maximum_size(
        column_schema->get_skip_index_attr(),
        column_schema->get_meta_type().get_type(),
        column_schema->get_accuracy().get_precision(),
        column_agg_maximum_size))) {
      LOG_WARN("failed to calculate maximum store size for skip index aggregate data size",
          K(ret), KPC(column_schema));
    } else if (FALSE_IT(aggregate_row_size += column_agg_maximum_size)) {
    } else if (OB_UNLIKELY(aggregate_row_size > ObSkipIndexColMeta::SKIP_INDEX_ROW_SIZE_LIMIT)) {
      // TODO: adjust storage format to resolve thie limitation？
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
      "current version of oceanbase has a limitation for skip index size in a single table, too many skip index columns");
      LOG_WARN("skip index row size too large", K(ret), KPC(column_schema), K(aggregate_row_size));
    }
  }
  return ret;
}

// Distinguish the following three scenarios:
// 1. For a non-auto-partitioned non-partitioned table, return 0 directly;
// 2. For a auto-partitioned non-partitioned table
//    2.1: if its "part_func_expr" is not empty, calculate the number of presetting partition key
//    2.2: if its "part_func_expr" is empty, return index_column_num if it is global index, return
//         rowkey_column_num if it is user-table
// 3. For a partitioned table, and the first-level partition mode is key(), take the number of primary key columns;
//  otherwise, calculate the number of expression vectors
int ObTableSchema::calc_part_func_expr_num(int64_t &part_func_expr_num) const
{
  int ret = OB_SUCCESS;
  part_func_expr_num = OB_INVALID_INDEX;
  if (PARTITION_LEVEL_MAX == part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K_(part_level));
  } else if (PARTITION_LEVEL_ZERO == part_level_ && !is_auto_partitioned_table()) {
    part_func_expr_num = 0;
  } else {
    ObArray<ObString> sub_columns;
    ObString table_func_expr_str = get_part_option().get_part_func_expr_str();
    if (table_func_expr_str.empty()) {
      if (is_auto_partitioned_table()) {
        // current auto-partitioned table only support range partition
        if (is_partitioned_table()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partitioned table without part_func_expr", KR(ret), KPC(this));
        } else if (!is_valid_split_part_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid part type", KR(ret), KPC(this));
        } else if (is_global_index_table()) {
          part_func_expr_num = get_index_column_num();
        } else if (is_user_table()) {
          part_func_expr_num = get_rowkey_column_num();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid auto-partitioned table", KR(ret), KPC(this));
        }
      } else if (is_key_part()) {
        part_func_expr_num = get_rowkey_column_num();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition func expr is empty", K(ret));
      }
    } else if (OB_FAIL(split_on(table_func_expr_str, ',', sub_columns))) {
      LOG_WARN("fail to split func expr", K(ret), K(table_func_expr_str));
    } else {
      part_func_expr_num = sub_columns.count();
    }
  }
  return ret;
}

int ObTableSchema::extract_actual_index_rowkey_columns_name(ObIArray<ObString> &rowkey_columns_name) const
{
  int ret = OB_SUCCESS;
  rowkey_columns_name.reset();
  if ((!is_global_index_table() && !is_global_local_index_table() && !is_local_unique_index_table())
      || is_spatial_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not expected index type", KR(ret), KPC(this));
  } else {
    const ObColumnSchemaV2 *index_column = NULL;
    const ObRowkeyColumn *index_key_column = NULL;
    const ObIndexInfo &index_info = get_index_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_info.get_size(); ++i) {
      if (OB_ISNULL(index_key_column = index_info.get_column(i))) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("get index column failed", K(ret));
      } else if (OB_ISNULL(index_column = get_column_schema(index_key_column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get index column schema failed", K(ret));
      } else if (index_column->is_hidden() || index_column->is_shadow_column()) { // do nothing
      } else if (OB_FAIL(rowkey_columns_name.push_back(index_column->get_column_name()))) {
        LOG_WARN("push back index column failed", K(ret));
      }
    }
  }
  return ret;
}

// Distinguish the following two scenarios:
// 1. For non-partitioned tables, return 0 directly;
// 2. For the second-level partition table, and the second-level partition mode is key(), take the number of primary key columns;
//  otherwise, calculate the number of expression vectors
int ObTableSchema::calc_subpart_func_expr_num(int64_t &subpart_func_expr_num) const
{
  int ret = OB_SUCCESS;
  subpart_func_expr_num = OB_INVALID_INDEX;
  if (PARTITION_LEVEL_MAX == part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K_(part_level));
  } else if (PARTITION_LEVEL_ZERO == part_level_
             || PARTITION_LEVEL_ONE == part_level_) {
    subpart_func_expr_num = 0;
  } else {
    ObArray<ObString> sub_columns;
    ObString table_func_expr_str = get_sub_part_option().get_part_func_expr_str();
    if (table_func_expr_str.empty()) {
      if (is_key_subpart()) {
        subpart_func_expr_num = get_rowkey_column_num();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition func expr is empty", K(ret));
      }
    } else if (OB_FAIL(split_on(table_func_expr_str, ',', sub_columns))) {
      LOG_WARN("fail to split func expr", K(ret), K(table_func_expr_str));
    } else {
      subpart_func_expr_num = sub_columns.count();
    }
  }
  return ret;
}

int ObTableSchema::get_subpart_ids(
    const int64_t part_id,
    common::ObIArray<int64_t> &subpart_ids) const
{
  int ret = OB_SUCCESS;
  ObSubPartition **subpart_array = NULL;
  int64_t subpart_num = 0;
  int64_t subpartition_num = 0;
  if (OB_FAIL(get_subpart_info(part_id, subpart_array, subpart_num, subpartition_num))) {
    LOG_WARN("fail to get subpart info", K(ret), K(part_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subpart_num; i++) {
    if (OB_ISNULL(subpart_array[i])) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("get invalid partition array", K(ret), K(i), K(subpart_num));
    } else if (OB_FAIL(subpart_ids.push_back(subpart_array[i]->get_sub_part_id()))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

ObConstraint *ObTableSchema::get_constraint_internal(
    std::function<bool(const ObConstraint *val)> func)
{
  ObConstraint *cst_ret = NULL;

  if (cst_array_ != NULL && cst_cnt_ > 0) {
    ObConstraint **end = cst_array_ + cst_cnt_;
    ObConstraint **cst = std::find_if(cst_array_, end, func);
    if (cst != end) {
      cst_ret = *cst;
    }
  }

  return cst_ret;
}

const ObConstraint *ObTableSchema::get_constraint_internal(
    std::function<bool(const ObConstraint *val)> func) const
{
  ObConstraint *cst_ret = NULL;

  if (cst_array_ != NULL && cst_cnt_ > 0) {
    ObConstraint **end = cst_array_ + cst_cnt_;
    ObConstraint **cst = std::find_if(cst_array_, end, func);
    if (cst != end) {
      cst_ret = *cst;
    }
  }

  return cst_ret;
}

int ObTableSchema::get_pk_constraint_name(ObString &pk_name) const
{
  int ret = OB_SUCCESS;
  ObConstraintType cst_type = CONSTRAINT_TYPE_PRIMARY_KEY;

  const ObConstraint *cst = get_constraint_internal(
      [cst_type](const ObConstraint *val) {return val->get_constraint_type() == cst_type;});
  if (OB_NOT_NULL(cst)) {
    pk_name.assign_ptr(
        cst->get_constraint_name_str().ptr(), cst->get_constraint_name_str().length());
  }

  return ret;
}

const ObConstraint *ObTableSchema::get_pk_constraint() const
{
  ObConstraintType cst_type = CONSTRAINT_TYPE_PRIMARY_KEY;

  const ObConstraint *cst = get_constraint_internal(
      [cst_type](const ObConstraint *val) {return val->get_constraint_type() == cst_type;});
  return cst;
}

const ObConstraint *ObTableSchema::get_constraint(const uint64_t constraint_id) const
{
  return get_constraint_internal(
        [constraint_id](const ObConstraint *val) {
          return val->get_constraint_id() == constraint_id;
        });
}

int64_t ObTableSchema::get_index_count() const
{
  int64_t index_count = 0;
  bool is_rowkey_doc_id_exist = false;
  bool is_doc_id_rowkey_exist = false;
  int64_t fts_index_aux_count = 0;
  int64_t fts_doc_word_aux_count = 0;
  int64_t multivalue_index_aux_count = 0;
  bool is_vec_rowkey_vid_exist = false;
  bool is_vec_vid_rowkey_exist = false;
  int64_t vec_delta_buffer_count = 0;
  int64_t vec_index_id_count = 0;
  int64_t vec_index_snapshot_data_count = 0;
  for (int64_t i = 0; i < get_index_tid_count(); ++i) {
    ObIndexType index_type = simple_index_infos_.at(i).index_type_;
    // Count the number of various index aux tables to determine the number of indexes that can be added.
    // If there are other indexes with multiple auxiliary tables, you need to add processing branches.
    if (share::schema::is_rowkey_doc_aux(index_type)) {
      is_rowkey_doc_id_exist = true;
    } else if (share::schema::is_doc_rowkey_aux(index_type)) {
      is_doc_id_rowkey_exist = true;
    } else if (share::schema::is_fts_index_aux(index_type)) {
      ++fts_index_aux_count;
    } else if (share::schema::is_fts_doc_word_aux(index_type)) {
      ++fts_doc_word_aux_count;
    } else if (share::schema::is_multivalue_index_aux(index_type)) {
      ++multivalue_index_aux_count;
    } else if (share::schema::is_vec_rowkey_vid_type(index_type)) {
      is_vec_rowkey_vid_exist = true;
    } else if (share::schema::is_vec_vid_rowkey_type(index_type)) {
      is_vec_vid_rowkey_exist = true;
    } else if (share::schema::is_vec_delta_buffer_type(index_type)) {
      ++vec_delta_buffer_count;
    } else if (share::schema::is_vec_index_id_type(index_type)) {
      ++vec_index_id_count;
    } else if (share::schema::is_vec_index_snapshot_data_type(index_type)) {
      ++vec_index_snapshot_data_count;
    } else if (ObIndexType::INDEX_TYPE_IS_NOT != index_type) {
      ++index_count;
    }
  }
  // Taking OB_MIN can ensure that the final index number is not greater than OB_MAX_INDEX_PER_TABLE.
  // but cannot ensure aux table numbers does not exceed OB_MAX_AUX_TABLE_PER_MAIN_TABLE.
  // Therefore, this function often appears with the OB_MAX_AUX_TABLE_PER_MAIN_TABLE limit.
  index_count += (is_rowkey_doc_id_exist && is_doc_id_rowkey_exist) ?
                  OB_MIN(fts_index_aux_count, fts_doc_word_aux_count) +  multivalue_index_aux_count : 0;
  index_count += (is_vec_rowkey_vid_exist && is_vec_vid_rowkey_exist) ?
                  OB_MIN(vec_delta_buffer_count, OB_MIN(vec_index_id_count, vec_index_snapshot_data_count)) : 0;
  return index_count;
}

const ObConstraint *ObTableSchema::get_constraint(const ObString &constraint_name) const
{
  return get_constraint_internal(
        [constraint_name](const ObConstraint *val) {
          return val->get_constraint_name_str() == constraint_name;
        });
}

int ObTableSchema::add_cst_to_cst_array(ObConstraint *cst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The cst is NULL", K(ret));
  } else {
    if (0 == cst_array_capacity_) {
      if (NULL == (cst_array_ = static_cast<ObConstraint**>(
          alloc(sizeof(ObConstraint*) * DEFAULT_ARRAY_CAPACITY)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for cst_array_.");
      } else {
        cst_array_capacity_ = DEFAULT_ARRAY_CAPACITY;
        MEMSET(cst_array_, 0, sizeof(ObConstraint*) * DEFAULT_ARRAY_CAPACITY);
      }
    } else if (cst_cnt_ >= cst_array_capacity_) {
      int64_t tmp_size = 2 * cst_array_capacity_;
      ObConstraint **tmp = NULL;
      if (NULL == (tmp = static_cast<ObConstraint**>(
          alloc(sizeof(ObConstraint*) * tmp_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for cst_array_, ", K(tmp_size), K(ret));
      } else {
        MEMCPY(tmp, cst_array_, sizeof(ObConstraint*) * cst_array_capacity_);
        // free old cst_array_
        free(cst_array_);
        cst_array_ = tmp;
        cst_array_capacity_ = tmp_size;
      }
    }

    if (OB_SUCC(ret)) {
      cst_array_[cst_cnt_++] = cst;
    }
  }

  return ret;
}

int ObTableSchema::remove_cst_from_cst_array(const ObConstraint *cst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The cst is NULL", K(ret));
  } else {
    int64_t i = 0;
    for (; i < cst_cnt_ && cst != cst_array_[i]; ++i) {
      ;
    }
    if (i < cst_cnt_) {
      cst_cnt_--;
    }
    for (; i < cst_cnt_; ++i) {
      cst_array_[i] = cst_array_[i+1];
    }
  }

  return ret;
}

int ObTableSchema::remove_column_id_from_label_se_array(const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < label_se_column_ids_.count(); ++i) {
    if (label_se_column_ids_[i] == column_id) {
      if (OB_FAIL(label_se_column_ids_.remove(i))) {
        LOG_WARN("remove from array failed", K(ret), K_(label_se_column_ids));
      }
    }
  }
  return ret;
}

int ObTableSchema::add_constraint(const ObConstraint &constraint)
{
  int ret = common::OB_SUCCESS;
  char *buf = NULL;
  ObConstraint *cst = NULL;
  if (NULL == (buf = static_cast<char*>(alloc(sizeof(ObConstraint))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "Fail to allocate memory, ", "size", sizeof(ObConstraint), K(ret));
  } else if (NULL == (cst = new (buf) ObConstraint(allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Fail to new cst", K(ret));
  } else if (OB_FAIL(cst->assign(constraint))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to assign constraint", K(ret));
  } else if (OB_FAIL(add_cst_to_cst_array(cst))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to push constraint to array", K(ret));
  }

  return ret;
}

int ObTableSchema::delete_constraint(const ObString &constraint_name)
{
  int ret = common::OB_SUCCESS;
  const ObConstraint *cst = get_constraint(constraint_name);
  if (cst != NULL) {
    if (OB_FAIL(remove_cst_from_cst_array(cst))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to remove cst", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::is_tbl_partition_key(const uint64_t column_id, bool &result,
                                        const bool ignore_presetting_key) const
{
  int ret = OB_SUCCESS;
  result = false;
  if (is_partitioned_table()) {
    if (OB_FAIL(get_partition_key_info().is_rowkey_column(column_id, result))) {
      LOG_WARN("check is partition key failed", KR(ret), K(column_id));
    } else if (!result && PARTITION_LEVEL_TWO == get_part_level()) {
      if (OB_FAIL(get_subpartition_key_info().is_rowkey_column(column_id, result))) {
        LOG_WARN("check is subpartition key failed", KR(ret), K(column_id));
      }
    }
  } else if (is_auto_partitioned_table() && !ignore_presetting_key &&
             OB_FAIL(is_presetting_partition_key(column_id, result))) {
    LOG_WARN("fail to check presetting partition key", KR(ret), K(column_id));
  }
  return ret;
}

int ObTableSchema::is_tbl_partition_key(const share::schema::ObColumnSchemaV2 &orig_column_schema,
                                        bool& result,
                                        const bool ignore_presetting_key) const
{
  int ret = OB_SUCCESS;
  result = false;

  if (is_partitioned_table()) {
    result = orig_column_schema.is_tbl_part_key_column();
  } else if (is_auto_partitioned_table() && !ignore_presetting_key &&
             OB_FAIL(is_presetting_partition_key(orig_column_schema.get_column_id(),
                                                 result))) {
    LOG_WARN("fail to check presetting partition key", KR(ret), K(orig_column_schema));
  }

  return ret;
}

int ObTableSchema::is_partition_key(const share::schema::ObColumnSchemaV2 &orig_column_schema,
                                    bool& result,
                                    const bool ignore_presetting_key) const
{
  int ret = OB_SUCCESS;
  result = false;

  if (is_partitioned_table()) {
    result = orig_column_schema.is_part_key_column();
  } else if (is_auto_partitioned_table() && !ignore_presetting_key &&
             OB_FAIL(is_presetting_partition_key(orig_column_schema.get_column_id(),
                                                 result))) {
    LOG_WARN("fail to check presetting partition key", KR(ret), K(orig_column_schema));
  }

  return ret;
}

int ObTableSchema::is_subpartition_key(const share::schema::ObColumnSchemaV2 &orig_column_schema,
                                       bool& result,
                                       const bool ignore_presetting_key) const
{
  int ret = OB_SUCCESS;
  result = false;
  UNUSED(ignore_presetting_key);

  if (PARTITION_LEVEL_TWO == get_part_level()) {
    result = orig_column_schema.is_subpart_key_column();
  }

  return ret;
}

int ObTableSchema::set_simple_index_infos(
    const common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos)
{
  int ret = OB_SUCCESS;
  simple_index_infos_.reset();
  int64_t count = simple_index_infos.count();
  if (OB_FAIL(simple_index_infos_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  FOREACH_CNT_X(simple_index_info, simple_index_infos, OB_SUCC(ret)) {
    if (OB_FAIL(add_simple_index_info(*simple_index_info))) {
      LOG_WARN("failed to add simple index info", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::set_aux_vp_tid_array(const common::ObIArray<uint64_t> &aux_vp_tid_array)
{
  int ret = OB_SUCCESS;
  aux_vp_tid_array_.reset();
  int64_t count = aux_vp_tid_array.count();
  if (OB_FAIL(aux_vp_tid_array_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && (i < aux_vp_tid_array.count()); ++i) {
    if (OB_FAIL(add_aux_vp_tid(aux_vp_tid_array.at(i)))) {
      LOG_WARN("failed to add to aux_vp_tid_array", K(ret));
    }
  }
  return ret;
}

void ObTableSchema::clear_foreign_key_infos()
{
  foreign_key_infos_.reset();
}

int ObTableSchema::set_foreign_key_infos(const ObIArray<ObForeignKeyInfo> &foreign_key_infos)
{
  int ret = OB_SUCCESS;
  foreign_key_infos_.reset();
  int64_t count = foreign_key_infos.count();
  if (OB_FAIL(foreign_key_infos_.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate array", K(ret), K(count));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      const ObForeignKeyInfo &src_foreign_key_info = foreign_key_infos.at(i);
      ObForeignKeyInfo &foreign_info = foreign_key_infos_.at(i);
      if (nullptr == new (&foreign_info) ObForeignKeyInfo(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("placement new return nullptr", K(ret));
      } else if (OB_FAIL(foreign_info.assign(src_foreign_key_info))) {
        LOG_WARN("fail to assign foreign key info", K(ret), K(src_foreign_key_info));
      } else if (!src_foreign_key_info.foreign_key_name_.empty()
                 && OB_FAIL(deep_copy_str(src_foreign_key_info.foreign_key_name_,
                                          foreign_info.foreign_key_name_))) {
        LOG_WARN("failed to deep copy foreign key name", K(ret), K(src_foreign_key_info));
      }
    }
  }
  return ret;
}

// As long as it is the parent table in any foreign key relationship, it returns true
bool ObTableSchema::is_parent_table() const
{
  bool is_parent_table = false;
  for (int64_t i = 0; !is_parent_table && i < foreign_key_infos_.count(); ++i) {
    if (get_table_id() == foreign_key_infos_.at(i).parent_table_id_) {
      is_parent_table = true;
    }
  }
  return is_parent_table;
}

// As long as it is a child table in any foreign key relationship, it returns true
bool ObTableSchema::is_child_table() const
{
  bool is_child_table = false;
  for (int64_t i = 0; !is_child_table && i < foreign_key_infos_.count(); ++i) {
    if (get_table_id() == foreign_key_infos_.at(i).child_table_id_) {
      is_child_table = true;
    }
  }
  return is_child_table;
}

bool ObTableSchema::is_foreign_key(uint64_t column_id) const
{
  bool is_fk = false;
  for (int64_t i = 0; !is_fk && i < foreign_key_infos_.count(); ++i) {
    const ObForeignKeyInfo &fk_info = foreign_key_infos_.at(i);
    if (fk_info.table_id_ == fk_info.child_table_id_ && fk_info.enable_flag_) {
      is_fk = has_exist_in_array(fk_info.child_column_ids_, column_id);
    }
  }
  return is_fk;
}

int64_t ObTableSchema::get_foreign_key_real_count() const
{
  int64_t real_count = foreign_key_infos_.count();
  for (int64_t i = 0; i < foreign_key_infos_.count(); i++) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos_.at(i);
    if (foreign_key_info.child_table_id_ == foreign_key_info.parent_table_id_) {
      ++real_count;
    }
  }
  return real_count;
}

int ObTableSchema::add_foreign_key_info(const ObForeignKeyInfo &foreign_key_info)
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

int ObTableSchema::get_fk_check_index_tid(ObSchemaGetterGuard &schema_guard, const common::ObIArray<uint64_t> &parent_column_ids, uint64_t &scan_index_tid) const
{
  int ret = OB_SUCCESS;
  scan_index_tid = OB_INVALID_ID;
  bool is_rowkey_column = false;
  if (0 >= parent_column_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column number in parent key is zero", K(ret), K(parent_column_ids.count()));
  } else if (OB_FAIL(check_rowkey_column(parent_column_ids, is_rowkey_column))) {
    LOG_WARN("failed to check if parent key is rowkey", K(ret));
  } else if (is_rowkey_column) {
    scan_index_tid = table_id_;
  } else {
    bool find = false;
    for (int i = 0; OB_SUCC(ret) && i < simple_index_infos_.count() && !find; ++i) {
      const ObAuxTableMetaInfo &index_info = simple_index_infos_.at(i);
      const uint64_t index_tid = index_info.table_id_;
      const ObTableSchema *index_schema = NULL;
      if (!is_unique_index(index_info.index_type_)) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(get_tenant_id(),
                                                index_tid,
                                                index_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index schema from schema guard is NULL", K(ret), K(index_schema));
      } else if (parent_column_ids.count() == index_schema->get_index_column_num()) {
        const ObIndexInfo &index_info = index_schema->get_index_info();
        bool is_rowkey = true;
        int j = 0;
        for (; OB_SUCC(ret) && j < parent_column_ids.count() && is_rowkey; ++j) {
          if (OB_FAIL(index_info.is_rowkey_column(parent_column_ids.at(j), is_rowkey))) {
            LOG_WARN("failed to check parent column is unique index column", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_rowkey) {
          find = true;
          scan_index_tid = index_tid;
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::check_rowkey_column(const common::ObIArray<uint64_t> &parent_column_ids, bool &is_rowkey) const
{
  int ret = OB_SUCCESS;
  is_rowkey = true;
  if (parent_column_ids.count() != rowkey_column_num_) {
    is_rowkey = false;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < parent_column_ids.count() && is_rowkey; ++i) {
      const uint64_t parent_column_id = parent_column_ids.at(i);
      ret = rowkey_info_.is_rowkey_column(parent_column_id, is_rowkey);
    }
  }
  return ret;
}


int ObTableSchema::add_simple_index_info(const ObAuxTableMetaInfo &simple_index_info)
{
  int ret = OB_SUCCESS;
  bool need_add = true;
  int64_t N = get_index_tid_count();

  // we are sure that index_tid are added in sorted order
  if (simple_index_info.table_id_ == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aux table tid", K(ret), K(simple_index_info.table_id_));
  } else if (simple_index_info.table_type_ < SYSTEM_TABLE
             || simple_index_info.table_type_ >= MAX_TABLE_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", K(ret), K(simple_index_info.table_type_));
  } else if (N > 0) {
    need_add = !std::binary_search(simple_index_infos_.begin(),
                                   simple_index_infos_.end(),
                                   simple_index_info,
                                   [](const ObAuxTableMetaInfo &l, const ObAuxTableMetaInfo &r){ return l.table_id_ < r.table_id_;});
  }
  if (OB_SUCC(ret) && need_add) {
    const int64_t last_pos = N - 1;
    if (N >= OB_MAX_AUX_TABLE_PER_MAIN_TABLE || get_index_count() >= common::OB_MAX_INDEX_PER_TABLE) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("index num in table is more than limited num", K(ret));
    } else if ((last_pos >= 0)
               && (simple_index_info.table_id_ <= simple_index_infos_.at(last_pos).table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table id must bigger than last one", K(ret));
    } else if (OB_FAIL(simple_index_infos_.push_back(simple_index_info))) {
      LOG_WARN("failed to push back simple_index_info", K(ret), K(simple_index_info));
    }
  }

  return ret;
}

int ObTableSchema::set_trigger_list(const ObIArray<uint64_t> &trigger_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trigger_list_.assign(trigger_list))) {
    LOG_WARN("fail to assign trigger_list", KR(ret), K(trigger_list));
  }
  return ret;
}

int ObTableSchema::has_before_insert_row_trigger(ObSchemaGetterGuard &schema_guard,
                                                 bool &trigger_exist) const
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *trigger_info = NULL;
  trigger_exist = false;
  const uint64_t tenant_id = get_tenant_id();
  for (int i = 0; OB_SUCC(ret) && !trigger_exist && i < trigger_list_.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list_.at(i), trigger_info), trigger_list_.at(i));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_list_.at(i));
    OX (trigger_exist = trigger_info->has_insert_event() &&
                        trigger_info->has_before_row_point());
  }
  return ret;
}

int ObTableSchema::has_before_update_row_trigger(ObSchemaGetterGuard &schema_guard,
                                                 bool &trigger_exist) const
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *trigger_info = NULL;
  trigger_exist = false;
  const uint64_t tenant_id = get_tenant_id();
  for (int i = 0; OB_SUCC(ret) && !trigger_exist && i < trigger_list_.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list_.at(i), trigger_info), trigger_list_.at(i));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_list_.at(i));
    OX (trigger_exist = trigger_info->has_update_event() &&
                        trigger_info->has_before_row_point());
  }
  return ret;
}

int ObTableSchema::is_allow_parallel_of_trigger(ObSchemaGetterGuard &schema_guard,
                                                 bool &is_forbid_parallel) const
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *trigger_info = NULL;
  is_forbid_parallel = false;
  const uint64_t tenant_id = get_tenant_id();
  for (int i = 0; OB_SUCC(ret) && !is_forbid_parallel && i < trigger_list_.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list_.at(i), trigger_info), trigger_list_.at(i));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_list_.at(i));
    OX (is_forbid_parallel = trigger_info->is_reads_sql_data() ||
                             trigger_info->is_modifies_sql_data() ||
                             trigger_info->is_wps() ||
                             trigger_info->is_rps() ||
                             trigger_info->is_has_sequence() ||
                             trigger_info->is_external_state());
  }
  return ret;
}

const ObColumnSchemaV2 *ObColumnIterByPrevNextID::get_first_column() const
{
  ObColumnSchemaV2 *ret_col = NULL;
  ObTableSchema::const_column_iterator iter = table_schema_.column_begin();
  for (; iter != table_schema_.column_end(); ++iter) {
    if (BORDER_COLUMN_ID == (*iter)->get_prev_column_id()) {
      ret_col = *iter;
      break;
    }
  }
  return ret_col;
}

int ObColumnIterByPrevNextID::next(const ObColumnSchemaV2 *&column_schema)
{
  int ret = OB_SUCCESS;
  column_schema = NULL;
  if (is_end_) {
    ret = OB_ITER_END;
  } else if (table_schema_.is_index_table()) {
    if (OB_ISNULL(last_iter_)) {
      last_iter_ = table_schema_.column_begin();
      column_schema = *last_iter_;
    } else if (++last_iter_ == table_schema_.column_end()) {
      is_end_ = true;
      ret = OB_ITER_END;
    } else {
      column_schema = *last_iter_;
    }
  } else {
    if (OB_ISNULL(last_column_schema_)) {
      if ((table_schema_.is_sys_view() || table_schema_.get_object_status() == ObObjectStatus::INVALID)
           && 0 == table_schema_.get_column_count()) {
        is_end_ = true;
        ret = OB_ITER_END;
      } else {
        column_schema = get_first_column();
      }
    } else if (BORDER_COLUMN_ID == last_column_schema_->get_next_column_id()) {
      is_end_ = true;
      ret = OB_ITER_END;
    } else {
      column_schema = table_schema_.get_column_schema_by_prev_next_id(
                                    last_column_schema_->get_next_column_id());
    }
    if (OB_NOT_NULL(column_schema)) {
      last_column_schema_ = column_schema;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is null", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::set_column_encodings(const common::ObIArray<int64_t> &col_encodings)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (col_encodings.count() != column_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column encoding count mismatch", K(col_encodings), K(column_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (NULL == column_array_[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL, ", K(i));
      } else {
        column_array_[i]->set_encoding_type(col_encodings.at(i));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_column_encodings(common::ObIArray<int64_t> &col_encodings) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is invalid", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (NULL == column_array_[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL, ", K(i));
      } else if (!column_array_[i]->is_virtual_generated_column()
          && OB_FAIL(col_encodings.push_back(column_array_[i]->get_encoding_type()))) {
        LOG_WARN("Fail to add column encoding type", K(ret), K(i), K(column_array_[i]));
      }
    }
  }
  return ret;
}

int ObTableSchema::is_presetting_partition_key_match_rowkey_prefix(bool &is_prefix) const
{
  int ret = OB_SUCCESS;
  is_prefix = false;

  if (is_auto_partitioned_table() && !is_partitioned_table()) {
    ObArray<uint64_t> presetting_partition_key_column_ids;

    if (part_option_.get_part_func_expr_str().empty()) {
      // if user doesn't set presetting partition key,
      // we will choose primary key as presetting partition key for data table and
      // choose index columns as presetting partition key for global index
      is_prefix = true;
    } else if (OB_FAIL(get_presetting_partition_keys(presetting_partition_key_column_ids))) {
      LOG_WARN("fail to get presetting partition keys", KR(ret), KPC(this));
    } else if (presetting_partition_key_column_ids.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid presetting partition key", KR(ret), KPC(this));
    } else {
      ObArray<uint64_t> rowkey_column_ids;
      const ObRowkeyInfo &rowkey_info = get_rowkey_info();
      const ObRowkeyColumn *rowkey_column = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        if (NULL == (rowkey_column = rowkey_info.get_column(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The rowkey column is NULL, ", K(i));
        } else if (OB_FAIL(rowkey_column_ids.push_back(rowkey_column->column_id_))) {
          LOG_WARN("failed to push back rowkey column id", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (rowkey_column_ids.count() < presetting_partition_key_column_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column ids", KR(ret), K(rowkey_column_ids), K(presetting_partition_key_column_ids));
      } else {
        is_prefix = true;
        for (int64_t i = 0; is_prefix && OB_SUCC(ret) && i < presetting_partition_key_column_ids.count(); i++) {
          uint64_t rowkey_column_id = rowkey_column_ids.at(i);
          uint64_t presetting_partition_key_column_id = presetting_partition_key_column_ids.at(i);
          if (rowkey_column_id != presetting_partition_key_column_id) {
            is_prefix = false;
          }
        } // end for
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only auto-partitioned non-partitioned table could call this function", KR(ret), KPC(this));
  }

  return ret;
}

int ObTableSchema::is_partition_key_match_rowkey_prefix(bool &is_prefix) const
{
  int ret = OB_SUCCESS;
  is_prefix = false;
  if (is_partitioned_table()) {
    is_prefix = true;
    int64_t i = 0;
    int64_t j = 0;
    uint64_t rowkey_column_id = OB_INVALID_ID;
    uint64_t partkey_column_id = OB_INVALID_ID;
    while (OB_SUCC(ret) && is_prefix &&
           i < rowkey_info_.get_size() && j < partition_key_info_.get_size()) {
      if (OB_FAIL(rowkey_info_.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("failed to get rowkey column id", K(ret), K(i));
      } else if (OB_FAIL(partition_key_info_.get_column_id(j, partkey_column_id))) {
        LOG_WARN("failed to get partition key column id", K(ret));
      } else if (rowkey_column_id == partkey_column_id) {
        ++i;
        ++j;
      } else {
        is_prefix = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (PARTITION_LEVEL_TWO == get_part_level()) {
        j = 0;
        while (OB_SUCC(ret) && is_prefix &&
               i < rowkey_info_.get_size() && j < subpartition_key_info_.get_size()) {
          if (OB_FAIL(rowkey_info_.get_column_id(i, rowkey_column_id))) {
            LOG_WARN("failed to get rowkey column id", K(ret), K(i));
          } else if (OB_FAIL(subpartition_key_info_.get_column_id(j, partkey_column_id))) {
            LOG_WARN("failed to get partition key column id", K(ret));
          } else if (rowkey_column_id == partkey_column_id) {
            ++i;
            ++j;
          } else {
            is_prefix = false;
          }
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObTableSchema::init_column_meta_array(
    common::ObIArray<blocksstable::ObSSTableColumnMeta> &meta_array) const
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> columns;
  ObSSTableColumnMeta col_meta;
  if (OB_FAIL(get_multi_version_column_descs(columns))) {
    STORAGE_LOG(WARN, "fail to get store column ids", K(ret));
  } else {
    blocksstable::ObStorageDatum datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const uint64_t col_id = columns.at(i).col_id_;
      col_meta.column_id_ = col_id;
      col_meta.column_checksum_ = 0;

      if (common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == col_id ||
          common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == col_id) {
        col_meta.column_default_checksum_ = 0;
      } else {
        const ObColumnSchemaV2 *col_schema = get_column_schema(col_id);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "col_schema must not null", K(ret), K(col_id));
        } else if (!col_schema->is_valid()) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "invalid col schema", K(ret), K(col_schema));
        } else if (!col_schema->is_column_stored_in_sstable()
                  && !is_storage_index_table()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "virtual generated column should be filtered already", K(ret), K(col_schema));
        } else {
          if (ob_is_large_text(col_schema->get_data_type())) {
            col_meta.column_default_checksum_ = 0;
          } else if (OB_FAIL(datum.from_obj_enhance(col_schema->get_orig_default_value()))) {
            STORAGE_LOG(WARN, "Failed to transefer obj to datum", K(ret));
          } else {
            col_meta.column_default_checksum_ = datum.checksum(0);
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(meta_array.push_back(col_meta))) {
        STORAGE_LOG(WARN, "Fail to push column meta", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableSchema::get_spatial_geo_column_id(uint64_t &geo_column_id) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *cellid_column = NULL;
  uint64_t cellid_column_id = UINT64_MAX;
  if (OB_FAIL(get_index_info().get_spatial_cellid_col_id(cellid_column_id))) {
    LOG_WARN("fail to get cellid column id", K(ret), K(get_index_info()));
  } else if (OB_ISNULL(cellid_column = get_column_schema(cellid_column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get cellid column", K(ret), K(cellid_column_id));
  } else {
    geo_column_id = cellid_column->get_geo_col_id();
  }
  return ret;
}

int ObTableSchema::get_multivalue_column_id(uint64_t &multivalue_col_id) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_multivalue_generated_column()) {
      multivalue_col_id = column_schema->get_column_id();
      break;
    }
  }

  return ret;
}

int ObTableSchema::get_fulltext_column_ids(uint64_t &doc_id_col_id, uint64_t &ft_col_id) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_doc_id_column()) {
      doc_id_col_id = column_schema->get_column_id();
    } else if (column_schema->is_word_segment_column()) {
      ft_col_id = column_schema->get_column_id();
    }
  }
  return ret;
}

int ObTableSchema::get_vec_index_column_id(uint64_t &vec_vector_id) const
{
  int ret = OB_SUCCESS;
  vec_vector_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == vec_vector_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_vec_vector_column()) {
      vec_vector_id = column_schema->get_column_id();
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == vec_vector_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_vec_index_vid_col_id(uint64_t &vec_id_col_id) const
{
  int ret = OB_SUCCESS;
  vec_id_col_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == vec_id_col_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_vec_vid_column()) {
      vec_id_col_id = column_schema->get_column_id();
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == vec_id_col_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_rowkey_doc_tid(uint64_t &index_table_id) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  index_table_id = OB_INVALID_ID;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (is_rowkey_doc_aux(simple_index_infos.at(i).index_type_)) {
      index_table_id = simple_index_infos.at(i).table_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_ERR_INDEX_KEY_NOT_FOUND;
    LOG_DEBUG("not found rowkey doc index", K(ret), K(simple_index_infos));
  }
  return ret;
}

int ObTableSchema::get_rowkey_vid_tid(uint64_t &index_table_id) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  index_table_id = OB_INVALID_ID;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (share::schema::is_vec_rowkey_vid_type(simple_index_infos.at(i).index_type_)) {
      index_table_id = simple_index_infos.at(i).table_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_ERR_INDEX_KEY_NOT_FOUND;
    LOG_DEBUG("not found rowkey vid index", K(ret), K(simple_index_infos));
  }
  return ret;
}

int ObTableSchema::check_has_local_index(ObSchemaGetterGuard &schema_guard, bool &has_local_index) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  const uint64_t tenant_id = get_tenant_id();
  has_local_index = false;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
        simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema",
               K(ret), K(tenant_id), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", K(simple_index_infos.at(i).table_id_));
    } else if (index_schema->is_index_local_storage()) {
      has_local_index = true;
      break;
    }
  }
  return ret;
}

int ObTableSchema::check_has_fts_index(ObSchemaGetterGuard &schema_guard, bool &has_fts_index) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  const uint64_t tenant_id = get_tenant_id();
  has_fts_index = false;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", K(simple_index_infos.at(i).table_id_));
    } else if (index_schema->is_fts_index_aux() || index_schema->is_fts_doc_word_aux()) {
      has_fts_index = true;
      break;
    }
  }
  return ret;
}

int ObTableSchema::check_has_multivalue_index(ObSchemaGetterGuard &schema_guard, bool &has_multivalue_index) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  const uint64_t tenant_id = get_tenant_id();
  has_multivalue_index = false;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", K(simple_index_infos.at(i).table_id_));
    } else if (index_schema->is_multivalue_index_aux()) {
      has_multivalue_index = true;
      break;
    }
  }
  return ret;
}

int ObTableSchema::check_has_vector_index(ObSchemaGetterGuard &schema_guard, bool &has_vector_index) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  const uint64_t tenant_id = get_tenant_id();
  has_vector_index = false;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", K(simple_index_infos.at(i).table_id_));
    } else if (index_schema->is_vec_index()) {
      has_vector_index = true;
      break;
    }
  }
  return ret;
}

int ObTableSchema::get_spatial_index_column_ids(common::ObIArray<uint64_t> &column_ids) const
{
  // spatial index is a kind of domain index
  // other types of domain indexes or gin index may have more than one column
  int ret = OB_SUCCESS;
  uint64_t geo_column_id = UINT64_MAX;
  if (OB_FAIL(get_spatial_geo_column_id(geo_column_id))) {
    LOG_WARN("fail to get spatial geo column id", K(ret));
  } else if (OB_FAIL(column_ids.push_back(geo_column_id))) {
    LOG_WARN("fail to push back geo column id", K(ret), K(geo_column_id));
  } else {
    // do nothing
      }
  return ret;
}

int ObTableSchema::delete_all_view_columns()
{
  int ret = OB_SUCCESS;
  bool for_view = true;
  for (int64_t i = column_cnt_ - 1; OB_SUCC(ret) && i >= 0; --i) {
    ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (nullptr != column_schema) {
      OZ (delete_column_internal(column_schema, for_view));
    }
  }
  CK (0 == column_cnt_);
  return ret;
}

int ObTableSchema::alter_all_view_columns_type_undefined(bool &already_invalid)
{
  int ret = OB_SUCCESS;
  // for force create view, it is invalid when created
  already_invalid = (0 == column_cnt_);
  bool for_view = true;
  //useless param
  ObColumnCheckMode dummy_mode = ObColumnCheckMode::CHECK_MODE_ONLINE;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    ObColumnSchemaV2 new_column_schema;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(ret));
    } else if (ObObjType::ObExtendType == column_schema->get_data_type()) {
      already_invalid = true;
      break;
    } else if (OB_FAIL(new_column_schema.assign(*column_schema))) {
      LOG_WARN("failed to copy column schema", K(ret));
    } else {
      //ObExtendType only used internal, we user it to describe UNDEFINED type
      new_column_schema.set_data_type(ObObjType::ObExtendType);
      new_column_schema.set_data_length(0);
      new_column_schema.set_data_precision(-1);
      new_column_schema.set_data_scale(OB_MIN_NUMBER_SCALE - 1);
      OZ (alter_column(new_column_schema, dummy_mode, for_view));
    }
  }
  return ret;
}

int ObTableSchema::assign_rls_objects(const ObTableSchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(rls_policy_ids_.assign(other.get_rls_policy_ids()))) {
      LOG_WARN("failed to assign rls policy ids", K(ret));
    } else if (OB_FAIL(rls_group_ids_.assign(other.get_rls_group_ids()))) {
      LOG_WARN("failed to assign rls group ids", K(ret));
    } else if (OB_FAIL(rls_context_ids_.assign(other.get_rls_context_ids()))) {
      LOG_WARN("failed to assign rls context ids", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::get_store_column_group_count(
    int64_t &column_group_cnt,
    const bool filter_empty_cg) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_cnt = 0;
  if (column_group_cnt_ > 0) {
    if (OB_ISNULL(column_group_arr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group_array should not be null", KR(ret), K_(column_group_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < column_group_cnt_); ++i) {
        if (OB_ISNULL(column_group_arr_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group should not be null", KR(ret), K(i), K_(column_group_cnt));
        } else if (filter_empty_cg && (0 == column_group_arr_[i]->get_column_id_count())) {
          // skip column_group with empty column_ids
        } else {
          ++tmp_cnt;
        }
      }
    }
  }
  column_group_cnt = tmp_cnt;
  return ret;
}

int ObTableSchema::get_store_column_groups(
    ObIArray<const ObColumnGroupSchema *> &column_groups,
    const bool filter_empty_cg) const
{
  int ret = OB_SUCCESS;
  column_groups.reset();
  if (column_group_cnt_ > 0) {
    if (OB_ISNULL(column_group_arr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group_array should not be null", KR(ret), K_(column_group_cnt));
    } else {
      if (OB_FAIL(column_groups.reserve(column_group_cnt_))) {
        LOG_WARN("fail to reserve", KR(ret), K_(column_group_cnt));
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < column_group_cnt_); ++i) {
        if (OB_ISNULL(column_group_arr_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group should not be null", KR(ret), K(i), K_(column_group_cnt));
        } else if (filter_empty_cg && (0 == column_group_arr_[i]->get_column_id_count())) {
          // skip column_group with empty column_ids
        } else if (OB_FAIL(column_groups.push_back(column_group_arr_[i]))) {
          LOG_WARN("fail to push back", KR(ret), K(i), K_(column_group_cnt));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    column_groups.reset();
  }
  return ret;
}

int ObTableSchema::has_all_column_group(bool &has_all_column_group) const
{
  int ret = OB_SUCCESS;
  int64_t column_group_cnt = 0;
  has_all_column_group = false;
  ObSEArray<const ObColumnGroupSchema *, 8> column_group_metas;
  if (OB_FAIL(get_store_column_group_count(column_group_cnt))) {
    LOG_WARN("Failed to get column group count", K(ret), KPC(this));
  } else if (column_group_cnt <= 1) {
    has_all_column_group = true;
  } else if (OB_FAIL(get_store_column_groups(column_group_metas))) {
    LOG_WARN("Failed to get column group metas", K(ret), KPC(this));
  } else {
    const ObColumnGroupSchema *cg_schema = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < column_group_metas.count(); ++idx) {
      if (OB_ISNULL(cg_schema = column_group_metas.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null cg_schema", K(ret));
      } else if (cg_schema->get_column_group_type() == ALL_COLUMN_GROUP) {
        has_all_column_group = true;
        break;
      }
    }
  }
  return ret;
}

int ObTableSchema::has_non_default_column_group(bool &has_non_default_column_group) const
{
  int ret = OB_SUCCESS;
  has_non_default_column_group = false;
  ObSEArray<const ObColumnGroupSchema *, 8> column_group_metas;
  if (OB_FAIL(get_store_column_groups(column_group_metas, false/*filter_empty_cg*/))) {
    LOG_WARN("Failed to get column group metas", K(ret), KPC(this));
  } else {
    const ObColumnGroupSchema *cg_schema = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < column_group_metas.count(); ++idx) {
      if (OB_ISNULL(cg_schema = column_group_metas.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null cg_schema", K(ret));
      } else if (DEFAULT_COLUMN_GROUP != cg_schema->get_column_group_type()) {
        has_non_default_column_group = true;
        break;
      }
    }
  }
  return ret;
}

int ObTableSchema::adjust_column_group_array()
{
  // after v435, all cg and rowkey cg should exist in the front of column group array
  // this interface is only used in create table / create index / offline ddl with new table / delayed column transform
  int ret = OB_SUCCESS;
  bool is_rowkey_cg_exist = false;
  bool is_all_cg_exist = false;
  ObColumnGroupSchema *cg_schema = nullptr;
  uint64_t compat_version = 0;
  if (!is_column_store_supported()) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(get_tenant_id(), compat_version))) {
    LOG_WARN("fail to get min data version", K(ret), K(get_tenant_id()));
  } else if (compat_version >= DATA_VERSION_4_3_5_0) {
    if (OB_FAIL(is_column_group_exist(OB_ROWKEY_COLUMN_GROUP_NAME, is_rowkey_cg_exist))) {
      LOG_WARN("fail to check is each column group exist", K(ret));
    } else if (OB_FAIL(is_column_group_exist(OB_ALL_COLUMN_GROUP_NAME, is_all_cg_exist))) {
      LOG_WARN("fail to check is all column group exist", K(ret));
    } else if (is_rowkey_cg_exist && OB_FAIL(get_column_group_by_name(OB_ROWKEY_COLUMN_GROUP_NAME, cg_schema))) {
      LOG_WARN("fail to get rowkey column group", K(ret), KPC(this));
    } else if (is_all_cg_exist && OB_FAIL(get_column_group_by_name(OB_ALL_COLUMN_GROUP_NAME, cg_schema))) {
      LOG_WARN("fail to get all column group", K(ret), KPC(this));
    } else if (OB_NOT_NULL(cg_schema)) {
      int64_t new_loc = column_group_cnt_ - 1;
      for (int64_t i = column_group_cnt_ - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_ISNULL(column_group_arr_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group should not be null", K(ret), K(i), K_(column_group_cnt));
        } else if (column_group_arr_[i] != cg_schema) {
          column_group_arr_[new_loc] = column_group_arr_[i];
          new_loc -= 1;
        } else {
          // skip all cg or rowkey cg
        }
      }
      if (OB_SUCC(ret)) {
        column_group_arr_[0] = cg_schema;
      }
    }
  }
  return ret;
}

int ObTableSchema::get_column_group_by_id(
    const uint64_t column_group_id,
    ObColumnGroupSchema *&column_group) const
{
  int ret = OB_SUCCESS;
  column_group = NULL;
  const CgIdHashArray *cg_id_arr = get_cg_id_hash_array();
  if (OB_NOT_NULL(cg_id_arr)) {
    if (OB_FAIL(cg_id_arr->get_refactored(ObColumnGroupIdKey(column_group_id), column_group))) {
      column_group = NULL;
      LOG_WARN("fail to get column_group from hash array", KR(ret), K(column_group_id));
    } else if (OB_ISNULL(column_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group should not be null", KR(ret), K(column_group_id));
    }
  }
  return ret;
}

int ObTableSchema::get_column_group_by_name(const ObString &cg_name, ObColumnGroupSchema *&column_group) const
{
  int ret = OB_SUCCESS;
  column_group = nullptr;
  if (cg_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_name));
  } else if (OB_NOT_NULL(cg_name_hash_arr_)) {
    if (OB_FAIL(cg_name_hash_arr_->get_refactored(ObColumnGroupSchemaHashWrapper(cg_name), column_group))) {
      column_group = nullptr;
      if (OB_HASH_NOT_EXIST == ret) {
        /* skip, hash not exist normal situation no warn*/
      } else {
        LOG_WARN("fail to get column_group from hash array", K(ret), K(cg_name));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_all_cg_type_column_group(const ObColumnGroupSchema *&column_group) const
{
  int ret = OB_SUCCESS;
  column_group = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_group_cnt_; i++) {
    if (OB_ISNULL(column_group_arr_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group should not be null", K(ret), K(i), K(column_group_cnt_));
    } else if (column_group_arr_[i]->get_column_group_type() == ALL_COLUMN_GROUP) {
      column_group = column_group_arr_[i];
      break;
    }
  }
  return ret;
}

// used to add default type column_group for sys_schema or mock default column_group
int ObTableSchema::add_default_column_group()
{
  int ret = OB_SUCCESS;
  if (column_group_cnt_ != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can only use this function to add default cg for sys schema", KR(ret));
  } else {
    ObColumnGroupSchema column_group;
    column_group.set_column_group_id(DEFAULT_TYPE_COLUMN_GROUP_ID);
    column_group.set_column_group_type(ObColumnGroupType::DEFAULT_COLUMN_GROUP);
    column_group.set_block_size(block_size_);
    column_group.set_row_store_type(row_store_type_);
    column_group.set_compressor_type(compressor_type_);
    if (OB_FAIL(column_group.set_column_group_name(OB_DEFAULT_COLUMN_GROUP_NAME))) {
      LOG_WARN("fail to set default type column_group name", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < column_cnt_); ++i) {
        if (OB_NOT_NULL(column_array_[i]) && !column_array_[i]->is_virtual_generated_column()) {
          const uint64_t column_id = column_array_[i]->get_column_id();
          if (OB_FAIL(column_group.add_column_id(column_id))) {
            LOG_WARN("fail to add column_id into column_group", KR(ret), K(i), K(column_id));
          }
        }
      }

      if (FAILEDx(do_add_column_group(column_group))) {
        LOG_WARN("fail to do add column group", KR(ret), K(column_group));
      }
    }
  }
  return ret;
}

int ObTableSchema::add_column_group(const ObColumnGroupSchema &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (other.get_column_group_type() != ObColumnGroupType::DEFAULT_COLUMN_GROUP
             && !ObSchemaUtils::can_add_column_group(*this)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only default column group is allowded to add to not user/tmp table", K(ret), K(other), KPC(this));
  } else if (OB_FAIL(do_add_column_group(other))) {
    LOG_WARN("fail to do add column group", KR(ret), K(other));
  }
  return ret;
}

int ObTableSchema::do_add_column_group(
    const ObColumnGroupSchema &other)
{
  int ret = OB_SUCCESS;
  ObColumnGroupSchema *column_group = NULL;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_ISNULL(column_group = OB_NEWx(ObColumnGroupSchema, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else if (static_cast<int64_t>(table_id_) > 0  // resolver will add column when table_id is invalid
             && OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
    // When deserialize column in physical restore, tenant id is wrong. We need to use lib::is_oracle_mode() to do this check.
    SHARE_SCHEMA_LOG(WARN, "check if_oracle_compat_mode failed", K(ret), K(tenant_id_), K(table_id_));
    ret = OB_SUCCESS;
    SHARE_SCHEMA_LOG(WARN, "replace error code to OB_SUCCESS, because tenant_id is invalid in physical restore",
                     K(ret), K(tenant_id_), K(table_id_), K(is_oracle_mode));
  } else if (static_cast<int64_t>(table_id_) <= 0 // deserialize create table arg
             && OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "check if_oracle_compat_mode failed", K(ret), K(tenant_id_), K(table_id_));
  }
  if (OB_SUCC(ret)) {
    lib::CompatModeGuard g(is_oracle_mode ?
                  lib::Worker::CompatMode::ORACLE :
                  lib::Worker::CompatMode::MYSQL);
    if (OB_FAIL(column_group->assign(other))) {
      LOG_WARN("fail to assign column_group", KR(ret), K(other));
    } else if (OB_FAIL((add_column_group_to_hash_array<ObColumnGroupIdKey, CgIdHashArray>(column_group,
      ObColumnGroupIdKey(column_group->get_column_group_id()), cg_id_hash_arr_)))) {
      LOG_WARN("fail to add column_group to cg_id_hash_array", KR(ret), KPC(column_group));
    } else if (OB_FAIL((add_column_group_to_hash_array<ObColumnGroupSchemaHashWrapper, CgNameHashArray>(
      column_group, ObColumnGroupSchemaHashWrapper(column_group->get_column_group_name()), cg_name_hash_arr_)))) {
      LOG_WARN("fail to add column_group to cg_name_hash_array", KR(ret), KPC(column_group));
    } else if (OB_FAIL(add_column_group_to_array(column_group))) {
      LOG_WARN("fail to add column_group to cg_array", KR(ret), KPC(column_group));
    } else {
      max_used_column_group_id_ = (other.get_column_group_id() > max_used_column_group_id_)
                                  ? other.get_column_group_id()
                                  : max_used_column_group_id_;
    }
  }
  return ret;
}

int ObTableSchema::add_column_group_to_array(ObColumnGroupSchema *column_group)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    if (0 == column_group_arr_capacity_) {
      if (OB_ISNULL(column_group_arr_ = static_cast<ObColumnGroupSchema**>(
          alloc(sizeof(ObColumnGroupSchema *) * DEFAULT_COLUMN_GROUP_ARRAY_CAPACITY)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for column_group_arr", KR(ret));
      } else {
        column_group_arr_capacity_ = DEFAULT_COLUMN_GROUP_ARRAY_CAPACITY;
        MEMSET(column_group_arr_, 0, sizeof(ObColumnGroupSchema*) * DEFAULT_COLUMN_GROUP_ARRAY_CAPACITY);
      }
    } else if (column_group_cnt_ >= column_group_arr_capacity_) {
      int64_t tmp_size = 2 * column_group_arr_capacity_;
      ObColumnGroupSchema **tmp = NULL;
      if (OB_ISNULL(tmp = static_cast<ObColumnGroupSchema**>(
          alloc(sizeof(ObColumnGroupSchema*) * tmp_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for column_group_array", KR(ret), K(tmp_size));
      } else {
        MEMCPY(tmp, column_group_arr_, sizeof(ObColumnGroupSchema*) * column_group_arr_capacity_);
        // free old column_group_array_
        free(column_group_arr_);
        column_group_arr_ = tmp;
        column_group_arr_capacity_ = tmp_size;
      }
    }

    if (OB_SUCC(ret)) {
      column_group_arr_[column_group_cnt_++] = column_group;
    }
  }

  return ret;
}

int ObTableSchema::remove_column_group(const uint64_t column_group_id)
{
  int ret = OB_SUCCESS;
  bool is_cg_exist = false;
  ObColumnGroupSchema *column_group = nullptr;
  if (OB_FAIL(get_column_group_by_id(column_group_id, column_group))) {
    LOG_WARN("fail to get column group by id", K(ret), K(column_group_id));
  } else if (OB_ISNULL(column_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column group should be null", K(ret));
  } else if (OB_ISNULL(cg_id_hash_arr_) || OB_ISNULL(cg_name_hash_arr_) ||
             OB_ISNULL(column_group_arr_) || column_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column array and cnt should not be none or zero", K(ret), KP(column_group_arr_),
             KP(cg_id_hash_arr_), KP(cg_name_hash_arr_), K(column_cnt_));
  } else if (OB_FAIL(cg_id_hash_arr_->erase_refactored(ObColumnGroupIdKey(column_group->get_column_group_id())))) {
    LOG_WARN("faile to erase column group id from table schema", K(ret));
  } else if (OB_FAIL(cg_name_hash_arr_->erase_refactored(ObColumnGroupSchemaHashWrapper(
                                                             column_group->get_column_group_name())))){
    LOG_WARN("faile to erase column group name from table scheam", K(ret));
  } else {
    int new_loc = 0;
    for (int64_t i = 0; i < column_group_arr_capacity_ && i < column_group_cnt_; i++) {
      if (column_group_arr_[i] != column_group) {
        column_group_arr_[new_loc] = column_group_arr_[i];
        new_loc += 1;
      } else {
        /* skip column group need to be delete*/
      }
    }
    column_group_cnt_--;
  }
  return ret;
}


int ObTableSchema::is_column_group_exist(const ObString &cg_name, bool &exist) const
{
  int ret = OB_SUCCESS;
  exist = false;
  if (cg_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cg_name));
  } else if (column_group_cnt_ > 0) {
    if (OB_ISNULL(cg_name_hash_arr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg_name_hash_array should not be null", KR(ret));
    } else if (cg_name == OB_EACH_COLUMN_GROUP_NAME) {
      ObTableSchema::const_column_group_iterator iter_begin = column_group_begin();
      ObTableSchema::const_column_group_iterator iter_end = column_group_end();
      for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
        const ObColumnGroupSchema *cg = *iter_begin;
        if (ObColumnGroupType::SINGLE_COLUMN_GROUP == cg->get_column_group_type()) {
          exist = true;
          break;
        }
      }
    } else {
      ObColumnGroupSchema *column_group = NULL;
      if (OB_FAIL(cg_name_hash_arr_->get_refactored(ObColumnGroupSchemaHashWrapper(cg_name), column_group))) {
        exist = false;
        if (OB_HASH_NOT_EXIST == ret) {
          /* hash no exist is sucess situation */
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get refactored from cg_name_hash_arr ", K(ret));
        }
      } else{
        exist = true;
      }
    }
  }
  return ret;
}

int ObTableSchema::get_column_group_index(
    const share::schema::ObColumnParam &param,
    const bool need_calculate_cg_idx,
    int32_t &cg_idx) const
{
  int ret = OB_SUCCESS;
  const uint64_t column_id = param.get_column_id();
  cg_idx = -1;
  if (OB_UNLIKELY(1 >= column_group_cnt_ && !need_calculate_cg_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("No column group exist", K(ret), K(need_calculate_cg_idx), K_(is_column_store_supported), K_(column_group_cnt));
  } else if (param.is_virtual_gen_col()) {
    cg_idx = -1;
  } else if ((column_id < OB_END_RESERVED_COLUMN_ID_NUM || common::OB_MAJOR_REFRESH_MVIEW_OLD_NEW_COLUMN_ID == column_id) &&
      common::OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID != column_id &&
      common::OB_HIDDEN_SESSION_ID_COLUMN_ID != column_id &&
      common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID != column_id) { // this has its own column group now
    if (common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id ||
        common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id) {
      if (need_calculate_cg_idx) {
        cg_idx = OB_CS_COLUMN_REPLICA_ROWKEY_CG_IDX;
      } else if (OB_FAIL(get_base_rowkey_column_group_index(cg_idx))) {
        LOG_WARN("Fail to get base/rowkey column group index", K(ret), K(column_id));
      }
    } else {
      // TODO: check the following
      // TODO: after check, also see ObStorageSchema::get_column_group_index
      // common::OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id
      // common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == column_id
      cg_idx = -1;
    }
  } else if (need_calculate_cg_idx) {
    if (OB_FAIL(calc_column_group_index_(column_id, cg_idx))) {
      LOG_WARN("Fail to calc_column_group_index", K(ret), K(column_id));
    }
  } else {
    bool found = false;
    int64_t cg_column_cnt = 0;
    int32_t iter_cg_idx = 0;
    uint64_t *cg_column_ids = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < column_group_cnt_; i++) {
      if (OB_ISNULL(column_group_arr_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_group should not be null", K(ret), K(i), K_(column_group_cnt));
      } else if (FALSE_IT(cg_column_cnt = column_group_arr_[i]->get_column_id_count())) {
      } else if (0 == cg_column_cnt) {
        if (column_group_arr_[i]->get_column_group_type() != DEFAULT_COLUMN_GROUP) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column group type", K(ret), KPC(column_group_arr_[i]));
        }
      } else if (1 < cg_column_cnt || column_group_arr_[i]->get_column_group_type() != ObColumnGroupType::SINGLE_COLUMN_GROUP) {
        iter_cg_idx++;
        // ignore column group with more than one column or not each column group cg
      } else if (OB_ISNULL(cg_column_ids = column_group_arr_[i]->get_column_ids())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected error for null column ids", K(ret), KPC(column_group_arr_[i]));
      } else if (cg_column_ids[0] != column_id) {
        iter_cg_idx++;
      } else {
        cg_idx = iter_cg_idx;
        found = true;
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, can not find cg idx", K(ret), K(column_id), K_(max_used_column_group_id));
    }
  }
  LOG_TRACE("[CS-Replica] get column group index", K(ret), K(need_calculate_cg_idx), K(param), K(cg_idx), KPC(this));
  return ret;
}

int ObTableSchema::calc_column_group_index_(const uint64_t column_id, int32_t &cg_idx) const
{
  int ret = OB_SUCCESS;
  cg_idx = -1;
  int64_t virtual_column_cnt = 0;
  int64_t nullptr_column_cnt = 0;
  // for cs replica, constructed cg schemas start with rowkey cg so the cg idx of row key cg is ALWAYS 0
  // and cg idx of normal cg is shifted by offset 1.
  for (int64_t i = 0; i < column_cnt_; i++) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (OB_ISNULL(column)) {
      nullptr_column_cnt++;
    } else if (column->is_virtual_generated_column()) {
      virtual_column_cnt++;
    } else if (column->get_column_id() == column_id) {
      cg_idx = i + 1 - virtual_column_cnt - nullptr_column_cnt;
      break;
    }
  }

  if (OB_UNLIKELY(-1 == cg_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected cg idx", K(ret));
  }

  return ret;
}

int ObTableSchema::get_base_rowkey_column_group_index(int32_t &cg_idx) const
{
  int ret = OB_SUCCESS;
  cg_idx = -1;
  if (OB_UNLIKELY(1 >= column_group_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("No column group exist", K(ret), K_(is_column_store_supported), K_(column_group_cnt));
  } else {
    bool found = false;
    int32_t iter_cg_idx = 0;
    for (int32_t i = 0; OB_SUCC(ret) && !found && i < column_group_cnt_; i++) {
      if (OB_ISNULL(column_group_arr_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_group should not be null", K(ret), K(i), K_(column_group_cnt));
      } else if (0 == column_group_arr_[i]->get_column_id_count()) {
        if (column_group_arr_[i]->get_column_group_type() != DEFAULT_COLUMN_GROUP) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column group type", K(ret), KPC(column_group_arr_[i]));
        }
      } else if (ALL_COLUMN_GROUP == column_group_arr_[i]->get_column_group_type() ||
                 ROWKEY_COLUMN_GROUP == column_group_arr_[i]->get_column_group_type()) {
        cg_idx = iter_cg_idx;
        found = true;
      } else {
        iter_cg_idx++;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected not found base/rowkey column group", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::check_is_normal_cgs_at_the_end(bool &is_normal_cgs_at_the_end) const
{
  int ret = OB_SUCCESS;
  is_normal_cgs_at_the_end = false;
  if (0 < column_group_cnt_) {
    bool found_normal_cg = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_group_cnt_; i++) {
      if (OB_ISNULL(column_group_arr_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_group should not be null", K(ret), K(i), K_(column_group_cnt));
      } else if (0 == column_group_arr_[i]->get_column_id_count()) {
        if (column_group_arr_[i]->get_column_group_type() != DEFAULT_COLUMN_GROUP) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column group type", K(ret), KPC(column_group_arr_[i]));
        }
        // skip default column group
      } else if (SINGLE_COLUMN_GROUP == column_group_arr_[i]->get_column_group_type() ||
                 NORMAL_COLUMN_GROUP == column_group_arr_[i]->get_column_group_type()) {
        found_normal_cg = true;
      } else {
        if (!found_normal_cg) {
          is_normal_cgs_at_the_end = true;
        }
        break;
      }
    }
  }
  return ret;
}

int ObTableSchema::get_each_column_group(ObIArray<ObColumnGroupSchema*> &each_cgs) const
{
  int ret = OB_SUCCESS;
  each_cgs.reset();
  ObTableSchema::const_column_group_iterator iter_begin = column_group_begin();
  ObTableSchema::const_column_group_iterator iter_end = column_group_end();

  for (;OB_SUCC(ret) && iter_begin != iter_end; iter_begin++ ) {
    ObColumnGroupSchema *cg = *iter_begin;
    if (OB_ISNULL(cg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column group should not be null", K(ret), KP(cg));
    } else if (cg->get_column_group_type() == ObColumnGroupType::SINGLE_COLUMN_GROUP) {
      if (OB_FAIL(each_cgs.push_back(cg))) {
        LOG_WARN("fail to add column group pointer to the array", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_all_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  if (get_column_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table has no columns", K(ret));
  } else {
    ObArray<ObColDesc> col_desc;
    col_desc.reset();

    if (OB_FAIL(get_column_ids(col_desc, true /*no virtual columns*/))) {
      LOG_WARN("fail to get not virtual columns", K(ret));
    } else {
      ObArray<ObColDesc>::iterator iter_begin = col_desc.begin();
      ObArray<ObColDesc>::iterator iter_end = col_desc.end();
      for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
        if (OB_FAIL(column_ids.push_back(iter_begin->col_id_))) {
          LOG_WARN("fail to push column id to array", K(ret));
        }
      }
    }
  }
  return ret;
}

// convert column_udt_set_id
int ObTableSchema::convert_column_udt_set_ids(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<uint64_t, uint64_t> column_udt_set_id_map;
  // generate new column udt id
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column schema", K(ret));
    } else if (column->get_udt_set_id() > 0 && !column->is_hidden()) {
      uint64_t new_column_id = 0;
      if (OB_FAIL(column_id_map.get_refactored(column->get_column_id(), new_column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(new_column_id));
      } else if (OB_UNLIKELY(new_column_id < OB_APP_MIN_COLUMN_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new column id too small", K(ret), K(new_column_id));
      } else if (!column_udt_set_id_map.created() &&
                 OB_FAIL(column_udt_set_id_map.create(OB_MAX_COLUMN_NUMBER / 2, lib::ObLabel("DDLSrvTmp")))) {
        LOG_WARN("failed to create udt set id map", K(ret), K(new_column_id));
      } else if (OB_FAIL(column_udt_set_id_map.set_refactored(column->get_udt_set_id(), new_column_id))) {
        LOG_WARN("failed to set column set id map", K(ret), K(new_column_id));
      }
    }
  }
  // update new column udt id and hidden_column_name
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column schema", K(ret));
    } else if (column->get_udt_set_id() > 0) {
      uint64_t new_column_set_id = 0;
      if (OB_FAIL(column_udt_set_id_map.get_refactored(column->get_udt_set_id(), new_column_set_id))) {
        LOG_WARN("failed to get column id", K(ret), K(column->get_udt_set_id()));
      } else if (OB_UNLIKELY(new_column_set_id <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new column id too small", K(ret), K(new_column_set_id));
      } else {
        column->set_udt_set_id(new_column_set_id);
        if (column->is_hidden()) {
          uint64_t new_column_id = 0;
          if (OB_FAIL(column_id_map.get_refactored(column->get_column_id(), new_column_id))) {
            LOG_WARN("failed to get column id", K(ret), K(new_column_id));
          } else if (OB_UNLIKELY(new_column_id < OB_APP_MIN_COLUMN_ID)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new column id too small", K(ret), K(new_column_id));
          } else {
            // update hidden column name
            char col_name[OB_MAX_COLUMN_NAME_LENGTH] = {0};
            databuff_printf(col_name, OB_MAX_COLUMN_NAME_LENGTH, "SYS_NC%05lu$", new_column_id);
            if (OB_FAIL(remove_col_from_name_hash_array(true, column))) {
              LOG_WARN("Failed to remove old column name from name_hash_array", K(ret));
            } else if (OB_FAIL(column->set_column_name(col_name))) {
              LOG_WARN("failed to change column name", K(ret));
            } else if (OB_FAIL(add_col_to_name_hash_array(true, column))) {
              LOG_WARN("Failed to add new column name to name_hash_array", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::convert_geo_generated_col_ids(const ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  // generate new column udt id
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
    ObColumnSchemaV2 *column = column_array_[i];
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column schema", K(ret));
    } else if (column->is_spatial_generated_column()) {
      uint64_t new_column_id = 0;
      uint64_t old_geo_column_id = column->get_geo_col_id();
      if (OB_FAIL(column_id_map.get_refactored(old_geo_column_id, new_column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(new_column_id));
      } else if (OB_UNLIKELY(new_column_id < OB_APP_MIN_COLUMN_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new column id too small", K(ret), K(new_column_id));
      } else {
        column->set_geo_col_id(new_column_id);
      }
    }
  }
  return ret;
}

int ObTableSchema::get_is_row_store(bool &is_row_store) const
{
  int ret = OB_SUCCESS;
  int64_t not_empty_cg_cnt = 0;
  if (OB_FAIL(get_store_column_group_count(not_empty_cg_cnt))) {
    LOG_WARN("fail to get column group count", K(ret));
  } else {
    is_row_store = not_empty_cg_cnt <= 1;
  }
  return ret;
}


int ObTableSchema::get_is_column_store(bool &is_column_store) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_is_row_store(is_column_store))) {
    LOG_WARN("fail to get is row store");
  } else {
    is_column_store = !is_column_store;
  }
  return ret;
}

int ObTableSchema::check_is_stored_generated_column_base_column(uint64_t column_id, bool &is_stored_base_col) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column = NULL;
  is_stored_base_col = false;
  for (ObTableSchema::const_column_iterator iter = column_begin();
       OB_SUCC(ret) && iter != column_end() && !is_stored_base_col;
       ++iter) {
    const ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (column_schema->is_stored_generated_column()
               && column_schema->has_cascaded_column_id(column_id)) {
      is_stored_base_col = true;
    }
  }
  return ret;
}

int ObTableSchema::get_doc_id_rowkey_tid(uint64_t &doc_id_rowkey_tid) const
{
  int ret = OB_SUCCESS;
  doc_id_rowkey_tid = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if (is_doc_rowkey_aux(simple_index_infos_.at(i).index_type_)) {
      doc_id_rowkey_tid = simple_index_infos_.at(i).table_id_;
      break;
    }
  }
  if (OB_INVALID_ID == doc_id_rowkey_tid) {
    ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
  }
  return ret;
}

int ObTableSchema::get_rowkey_doc_id_tid(uint64_t &rowkey_doc_id_tid) const
{
  int ret = OB_SUCCESS;
  rowkey_doc_id_tid = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if (is_rowkey_doc_aux(simple_index_infos_.at(i).index_type_)) {
      rowkey_doc_id_tid = simple_index_infos_.at(i).table_id_;
      break;
    }
  }
  if (OB_INVALID_ID == rowkey_doc_id_tid) {
    ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
  }
  return ret;
}

int ObTableSchema::get_vec_id_rowkey_tid(uint64_t &vec_id_rowkey_tid) const
{
  int ret = OB_SUCCESS;
  vec_id_rowkey_tid = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if (share::schema::is_vec_vid_rowkey_type(simple_index_infos_.at(i).index_type_)) {
      vec_id_rowkey_tid = simple_index_infos_.at(i).table_id_;
      break;
    }
  }
  if (OB_INVALID_ID == vec_id_rowkey_tid) {
    ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
  }
  return ret;
}

int64_t ObPrintableTableSchema::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("simple_table_schema");
  J_COLON();
  J_KV(K_(tenant_id),
      K_(database_id),
      K_(tablegroup_id),
      K_(table_id),
      K_(table_name),
      K_(session_id),
      "index_type", static_cast<int32_t>(index_type_),
      "table_type", static_cast<int32_t>(table_type_),
      K_(table_mode),
      K_(tablespace_id));
  J_COMMA();
  J_KV(K_(data_table_id),
    "name_casemode", static_cast<int32_t>(name_case_mode_),
    K_(schema_version),
    K_(part_level),
    K_(part_option),
    K_(sub_part_option),
    K_(partition_num),
    K_(def_subpartition_num),
    K_(index_status),
    K_(duplicate_scope),
    K_(encryption),
    K_(tablespace_id),
    K_(encrypt_key),
    K_(master_key_id),
    K_(sub_part_template_flags)
  );
  J_COMMA();
  J_KV(K_(max_used_column_id),
      K_(sess_active_time),
      K_(rowkey_column_num),
      K_(index_column_num),
      K_(rowkey_info),
      K_(partition_key_info),
      K_(column_cnt));
  J_COMMA();
  J_NAME("column_array");
  J_COLON();
  for (int64_t i = 0; i < column_cnt_; i++) {
    const ObColumnSchemaV2 *col = column_array_[i];
    J_KV("column_id", col->get_column_id(), "column_name", col->get_column_name(), "rowkey_pos", col->get_rowkey_position(),
          "index_pos", col->get_index_position(), "meta_type", col->get_meta_type(), "accuracy", col->get_accuracy(),
          "is_nullable", col->is_nullable(), "is_zero_fill", col->is_zero_fill(), "cur_default_value", col->get_cur_default_value());
    J_COMMA();
  }
  J_KV(K_(rowkey_split_pos),
      K_(block_size),
      K_(is_use_bloomfilter),
      K_(progressive_merge_num),
      K_(tablet_size),
      K_(pctfree),
      K_(compressor_type),
      K_(row_store_type),
      K_(store_format),
      "load_type", static_cast<int32_t>(load_type_),
      "index_using_type", static_cast<int32_t>(index_using_type_),
      "def_type", static_cast<int32_t>(def_type_),
      "charset_type", static_cast<int32_t>(charset_type_),
      "collation_type", static_cast<int32_t>(collation_type_));
  J_COMMA();
  J_KV("index_status", static_cast<int32_t>(index_status_),
    "partition_status", static_cast<int32_t>(partition_status_),
    K_(code_version),
    K_(comment),
    K_(pk_comment),
    K_(create_host),
    K_(tablegroup_name),
    K_(expire_info),
    K_(view_schema),
    K_(autoinc_column_id),
    K_(auto_increment),
    K_(read_only),
    "aux_vp_tid_array", aux_vp_tid_array_,
    K_(base_table_ids),
    K_(aux_lob_meta_tid),
    K_(aux_lob_piece_tid),
    K_(is_column_store_supported),
    K_(max_used_column_group_id),
    K_(mlog_tid),
    K_(duplicate_read_consistency)
  );
  J_OBJ_END();
  return pos;
}

} //enf of namespace schema
} //end of namespace share
} //end of namespace oceanbase
