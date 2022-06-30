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
#include "rootserver/ob_leader_coordinator.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_encryption_util.h"
namespace oceanbase {
namespace share {
namespace schema {
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

const uint64_t HIDDEN_PK_COLUMN_IDS[] = {common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID,
    common::OB_HIDDEN_PK_CLUSTER_COLUMN_ID,
    common::OB_HIDDEN_PK_PARTITION_COLUMN_ID};

const char* HIDDEN_PK_COLUMN_NAMES[] = {common::OB_HIDDEN_PK_INCREMENT_COLUMN_NAME,
    common::OB_HIDDEN_PK_CLUSTER_COLUMN_NAME,
    common::OB_HIDDEN_PK_PARTITION_COLUMN_NAME};

ObColumnIdKey ObGetColumnKey<ObColumnIdKey, ObColumnSchemaV2*>::operator()(const ObColumnSchemaV2* column_schema) const
{
  return ObColumnIdKey(column_schema->get_column_id());
}

ObColumnSchemaHashWrapper ObGetColumnKey<ObColumnSchemaHashWrapper, ObColumnSchemaV2*>::operator()(
    const ObColumnSchemaV2* column_schema) const
{
  return ObColumnSchemaHashWrapper(column_schema->get_column_name_str());
}

int ObTableMode::assign(const ObTableMode& other)
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

ObTableMode& ObTableMode::operator=(const ObTableMode& other)
{
  if (this != &other) {
    mode_ = other.mode_;
  }
  return *this;
}

bool ObTableMode::is_valid() const
{
  bool bret = false;
  if (mode_flag_ < TABLE_MODE_MAX && pk_mode_ < TPKM_MAX) {
    bret = true;
  }
  return bret;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObTableMode, mode_);

ObSimpleTableSchemaV2::ObSimpleTableSchemaV2() : ObPartitionSchema()
{
  reset();
}

ObSimpleTableSchemaV2::ObSimpleTableSchemaV2(ObIAllocator* allocator)
    : ObPartitionSchema(allocator),
      zone_list_(),
      simple_foreign_key_info_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      simple_constraint_info_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      zone_replica_attr_array_(),
      primary_zone_array_()
{
  reset();
}

ObSimpleTableSchemaV2::~ObSimpleTableSchemaV2()
{}

int ObSimpleTableSchemaV2::assign(const ObSimpleTableSchemaV2& other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    ObSimpleTableSchemaV2::reset();
    ObPartitionSchema::operator=(other);
    if (OB_SUCCESS == error_ret_) {
      tenant_id_ = other.tenant_id_;
      table_id_ = other.table_id_;
      schema_version_ = other.schema_version_;
      database_id_ = other.database_id_;
      tablegroup_id_ = other.tablegroup_id_;
      data_table_id_ = other.data_table_id_;
      table_type_ = other.table_type_;
      name_case_mode_ = other.name_case_mode_;
      replica_num_ = other.replica_num_;
      index_status_ = other.index_status_;
      index_type_ = other.index_type_;
      min_partition_id_ = other.min_partition_id_;
      partition_status_ = other.partition_status_;
      partition_schema_version_ = other.partition_schema_version_;
      session_id_ = other.session_id_;
      duplicate_scope_ = other.duplicate_scope_;
      is_mock_global_index_invalid_ = other.is_mock_global_index_invalid_;
      binding_ = other.binding_;
      tablespace_id_ = other.tablespace_id_;
      master_key_id_ = other.master_key_id_;
      dblink_id_ = other.dblink_id_;
      link_table_id_ = other.link_table_id_;
      link_schema_version_ = other.link_schema_version_;
      if (OB_FAIL(table_mode_.assign(other.table_mode_))) {
        LOG_WARN("Fail to assign table mode", K(ret), K(other.table_mode_));
      } else if (OB_FAIL(deep_copy_str(other.table_name_, table_name_))) {
        LOG_WARN("Fail to deep copy table_name", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.primary_zone_, primary_zone_))) {
        LOG_WARN("Fail to deep copy primary_zone", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.locality_str_, locality_str_))) {
        LOG_WARN("fail to deep copy locality str", K(ret));
      } else if (OB_FAIL(deep_copy_str(other.previous_locality_str_, previous_locality_str_))) {
        LOG_WARN("fail to deep copy previous locality str", K(ret));
      } else if (OB_FAIL(set_zone_list(other.zone_list_))) {
        LOG_WARN("fail to set zone_list", K(ret));
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
      } else if (OB_FAIL(generate_mapping_pg_partition_array())) {
        // This method depends on binding_ and needs to be called after binding_ is assigned
        LOG_WARN("fail to generate mapping pg partition array", K(ret));
      } else if (OB_FAIL(set_zone_replica_attr_array(other.zone_replica_attr_array_))) {
        LOG_WARN("set_zone_replica_num_array failed", K(ret));
      } else if (OB_FAIL(set_primary_zone_array(other.primary_zone_array_))) {
        LOG_WARN("fail to set primary zone array", K(ret));
      } else {
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

int ObSimpleTableSchemaV2::generate_mapping_pg_partition_array()
{
  int ret = OB_SUCCESS;
  if (!binding_) {
    // bypass
  } else if (PARTITION_LEVEL_ZERO == get_part_level()) {
    // bypass
  } else {
    // The binding_ table also needs to be filled with sorted_part_array and sorted_def_subpart_array_
    const int64_t first_part_num = get_first_part_num();
    const int64_t total_part_num = first_part_num + get_dropped_partition_num();
    const int64_t def_sub_part_num = get_def_sub_part_num();
    if (total_part_num > 0 && PARTITION_LEVEL_ZERO != get_part_level()) {
      sorted_part_id_partition_array_ = static_cast<ObPartition**>(alloc(sizeof(ObPartition*) * total_part_num));
      if (OB_UNLIKELY(nullptr == sorted_part_id_partition_array_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == get_part_array()) {
        // bypass,
      } else {
        for (int64_t i = 0; i < total_part_num; ++i) {
          if (i >= first_part_num) {
            sorted_part_id_partition_array_[i] = get_dropped_part_array()[i - first_part_num];
          } else {
            sorted_part_id_partition_array_[i] = get_part_array()[i];
          }
        }
        PartIdPartitionArrayCmp part_id_array_cmp;
        std::sort(sorted_part_id_partition_array_, sorted_part_id_partition_array_ + total_part_num, part_id_array_cmp);
        if (OB_FAIL(part_id_array_cmp.get_ret())) {
          LOG_WARN("fail to sort part id array", K(ret));
        }
      }
    }
    // template table
    if (OB_SUCC(ret) && def_sub_part_num > 0 && PARTITION_LEVEL_TWO == get_part_level() && is_sub_part_template()) {
      sorted_part_id_def_subpartition_array_ =
          static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * def_sub_part_num));
      if (OB_UNLIKELY(nullptr == sorted_part_id_def_subpartition_array_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == get_def_subpart_array()) {
        // bypass
      } else {
        for (int64_t i = 0; i < def_sub_part_num; ++i) {
          sorted_part_id_def_subpartition_array_[i] = get_def_subpart_array()[i];
        }
        SubPartIdPartitionArrayCmp sub_part_id_array_cmp;
        std::sort(sorted_part_id_def_subpartition_array_,
            sorted_part_id_def_subpartition_array_ + def_sub_part_num,
            sub_part_id_array_cmp);
        if (OB_FAIL(sub_part_id_array_cmp.get_ret())) {
          LOG_WARN("fail to sort def subpart id array", K(ret));
        }
      }
    }
    // nontemplate table
    if (OB_SUCC(ret) && OB_NOT_NULL(get_part_array()) && PARTITION_LEVEL_TWO == get_part_level() &&
        !is_sub_part_template()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_partition_num(); i++) {
        ObPartition* partition = get_part_array()[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", K(ret), K(i));
        } else if (OB_FAIL(partition->generate_mapping_pg_subpartition_array())) {
          LOG_WARN("fail to sort sub part id array", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

bool ObSimpleTableSchemaV2::operator==(const ObSimpleTableSchemaV2& other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_ && table_id_ == other.table_id_ && schema_version_ == other.schema_version_ &&
      database_id_ == other.database_id_ && tablegroup_id_ == other.tablegroup_id_ &&
      data_table_id_ == other.data_table_id_ && table_name_ == other.table_name_ &&
      name_case_mode_ == other.name_case_mode_ && table_type_ == other.table_type_ &&
      primary_zone_ == other.primary_zone_ && part_num_ == other.part_num_ &&
      def_subpart_num_ == other.def_subpart_num_ && replica_num_ == other.replica_num_ &&
      part_level_ == other.part_level_ && locality_str_ == other.locality_str_ &&
      previous_locality_str_ == other.previous_locality_str_ && index_status_ == other.index_status_ &&
      index_type_ == other.index_type_ && min_partition_id_ == other.min_partition_id_ &&
      partition_status_ == other.partition_status_ && partition_schema_version_ == other.partition_schema_version_ &&
      session_id_ == other.session_id_ && origin_index_name_ == other.origin_index_name_ &&
      zone_list_.count() == other.zone_list_.count() && binding_ == other.binding_ &&
      is_mock_global_index_invalid_ == other.is_mock_global_index_invalid_ && table_mode_ == other.table_mode_ &&
      encryption_ == other.encryption_ && tablespace_id_ == other.tablespace_id_ &&
      encrypt_key_ == other.encrypt_key_ && master_key_id_ == other.master_key_id_ &&
      drop_schema_version_ == other.drop_schema_version_ && dblink_id_ == other.dblink_id_ &&
      link_table_id_ == other.link_table_id_ && link_schema_version_ == other.link_schema_version_ &&
      link_database_name_ == other.link_database_name_) {
    ret = true;
    for (int64_t i = 0; ret && i < zone_list_.count(); ++i) {
      const ObString& zone_ptr = zone_list_.at(i);
      if (!has_exist_in_array(other.zone_list_, zone_ptr)) {
        ret = false;
      } else {
      }  // go on next
    }
    if (true == ret) {
      if (simple_foreign_key_info_array_.count() == other.simple_foreign_key_info_array_.count()) {
        for (int64_t i = 0; ret && i < simple_foreign_key_info_array_.count(); ++i) {
          const ObSimpleForeignKeyInfo& fk_info = simple_foreign_key_info_array_.at(i);
          if (!has_exist_in_array(other.simple_foreign_key_info_array_, fk_info)) {
            ret = false;
          } else {
          }  // go on next
        }
      } else if (simple_constraint_info_array_.count() == other.simple_constraint_info_array_.count()) {
        for (int64_t i = 0; ret && i < simple_constraint_info_array_.count(); ++i) {
          const ObSimpleConstraintInfo& cst_info = simple_constraint_info_array_.at(i);
          if (!has_exist_in_array(other.simple_constraint_info_array_, cst_info)) {
            ret = false;
          } else {
          }  // go on next
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
  sorted_part_id_partition_array_ = nullptr;
  sorted_part_id_def_subpartition_array_ = nullptr;
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
}

void ObSimpleTableSchemaV2::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  database_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  data_table_id_ = 0;
  table_name_.reset();
  origin_index_name_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
  table_type_ = USER_TABLE;
  table_mode_.reset();
  primary_zone_.reset();
  replica_num_ = 0;
  locality_str_.reset();
  locality_str_.assign_ptr("", 0);
  previous_locality_str_.reset();
  previous_locality_str_.assign_ptr("", 0);
  reset_string_array(zone_list_);
  index_status_ = INDEX_STATUS_UNAVAILABLE;
  partition_status_ = PARTITION_STATUS_ACTIVE;
  index_type_ = INDEX_TYPE_IS_NOT;
  min_partition_id_ = 0;
  session_id_ = 0;
  is_mock_global_index_invalid_ = false;
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    free(primary_zone_array_.at(i).zone_.ptr());
  }
  primary_zone_array_.reset();
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
  binding_ = false;
  sorted_part_id_partition_array_ = nullptr;
  sorted_part_id_def_subpartition_array_ = nullptr;
  duplicate_scope_ = ObDuplicateScope::DUPLICATE_SCOPE_NONE;
  simple_constraint_info_array_.reset();
  encryption_.reset();
  tablespace_id_ = OB_INVALID_ID;
  reset_zone_replica_attr_array();
  encrypt_key_.reset();
  master_key_id_ = OB_INVALID_ID;
  ObPartitionSchema::reset();
}

ObPartitionLevel ObSimpleTableSchemaV2::get_part_level() const
{
  ObPartitionLevel part_level = part_level_;
  if (PARTITION_LEVEL_ONE == part_level_ && 1 == part_option_.get_part_num() &&
      PARTITION_FUNC_TYPE_HASH == part_option_.get_part_func_type() && part_option_.get_part_func_expr_str().empty()) {
    part_level = PARTITION_LEVEL_ZERO;
  } else {
  }  // do nothing
  return part_level;
}

int ObSimpleTableSchemaV2::get_zone_list(common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  if (locality_str_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get zone list from local schema", K(ret));
  } else {
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
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_zone_list(
    share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  if (!locality_str_.empty()) {
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
  } else if (is_new_tablegroup_id(get_tablegroup_id())) {
    const ObTablegroupSchema* tablegroup_schema = NULL;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(get_tablegroup_id(), tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(tenant_id_), K(tablegroup_id_));
    } else if (OB_UNLIKELY(NULL == tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablegroup schema null", K(ret), K(tenant_id_), K(tablegroup_id_), KP(tablegroup_schema));
    } else if (OB_FAIL(tablegroup_schema->get_zone_list(schema_guard, zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else {
    }  // no more to do
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(database_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(database_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard& schema_guard,
    const rootserver::ObRandomZoneSelector& random_selector,
    const common::ObIArray<rootserver::ObReplicaAddr>& replica_addrs, common::ObZone& first_primary_zone) const
{
  int ret = OB_SUCCESS;
  first_primary_zone.reset();
  //sys table use tenant
  bool use_tenant_primary_zone = false;
  const share::schema::ObTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema ptr is null", K(ret), KPC(tenant_schema));
  } else if (tenant_schema->is_restore() && common::is_sys_table(table_id_)) {
    use_tenant_primary_zone = true;
  }

  if (OB_FAIL(ret)) {
  } else if (primary_zone_.empty() || use_tenant_primary_zone) {
    const uint64_t tablegroup_id = get_tablegroup_id();
    if (OB_INVALID_ID == tablegroup_id || !is_new_tablegroup_id(tablegroup_id)) {
      const share::schema::ObDatabaseSchema* db_schema = nullptr;
      if (OB_FAIL(schema_guard.get_database_schema(get_database_id(), db_schema))) {
        LOG_WARN("fail to get database schema", K(ret), "db_id", get_database_id());
      } else if (OB_UNLIKELY(nullptr == db_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("db schema ptr is null", K(ret));
      } else if (OB_FAIL(db_schema->get_first_primary_zone_inherit(
                     schema_guard, random_selector, replica_addrs, first_primary_zone))) {
        LOG_WARN("fail to get first primary zone inherit from db", K(ret));
      }
    } else {
      const share::schema::ObTablegroupSchema* tg_schema = nullptr;
      if (OB_FAIL(schema_guard.get_tablegroup_schema(get_tablegroup_id(), tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", get_tablegroup_id());
      } else if (OB_UNLIKELY(nullptr == tg_schema)) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("tg schema ptr is null", K(ret));
      } else if (OB_FAIL(tg_schema->get_first_primary_zone_inherit(
                     schema_guard, random_selector, replica_addrs, first_primary_zone))) {
        LOG_WARN("fail to get first primary zone inherit from tg", K(ret));
      }
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

int ObSimpleTableSchemaV2::get_paxos_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  if (!locality_str_.empty()) {
    FOREACH_CNT_X(locality, zone_replica_attr_array_, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid locality set", K(ret), KP(locality));
      } else {
        num += locality->get_paxos_replica_num();
      }
    }
  } else {
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else {
      FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret)
      {
        if (OB_ISNULL(locality)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid locality set", K(ret), KP(locality));
        } else {
          num += locality->get_paxos_replica_num();
        }
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_zone_replica_attr_array(ZoneLocalityIArray& locality) const
{
  int ret = OB_SUCCESS;
  locality.reset();
  for (int64_t i = 0; i < zone_replica_attr_array_.count() && OB_SUCC(ret); ++i) {
    const SchemaZoneReplicaAttrSet& schema_set = zone_replica_attr_array_.at(i);
    ObZoneReplicaAttrSet zone_replica_attr_set;
    for (int64_t j = 0; OB_SUCC(ret) && j < schema_set.zone_set_.count(); ++j) {
      if (OB_FAIL(zone_replica_attr_set.zone_set_.push_back(schema_set.zone_set_.at(j)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_replica_attr_set.replica_attr_set_.assign(schema_set.replica_attr_set_))) {
      LOG_WARN("fail to set zone replica attr set", K(ret));
    } else {
      zone_replica_attr_set.zone_ = schema_set.zone_;
      if (OB_FAIL(locality.push_back(zone_replica_attr_set))) {
        LOG_WARN("fail to push back", K(ret));
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
    ObSchemaGetterGuard& schema_guard, ZoneLocalityIArray& locality) const
{
  int ret = OB_SUCCESS;
  bool use_tenant_locality = false;
  locality.reuse();
  if (!has_partition()) {
    // No partition, no concept of locality
  } else {
    const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, simple_tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
    } else if (simple_tenant->is_restore() && common::is_sys_table(table_id_)) {
      use_tenant_locality = true;
    }

    if (OB_FAIL(ret)) {
    } else if (!use_tenant_locality && !locality_str_.empty()) {
      if (simple_tenant->is_restore()) {
        bool has_not_f_replica = false;
        if (OB_FAIL(check_has_own_not_f_replica(has_not_f_replica))) {
          LOG_WARN("failed to check has not f replica", KR(ret));
        } else if (has_not_f_replica) {
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("has not full replica while tenant is restore, try latter", KR(ret),
              K(locality_str_), KPC(simple_tenant));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!locality_str_.empty() && !use_tenant_locality) {
    if (OB_FAIL(get_zone_replica_attr_array(locality))) {
      LOG_WARN("fail to get zone replica attr array", K(ret));
    }
  } else if (is_new_tablegroup_id(tablegroup_id_) && !use_tenant_locality) {
    // The table is located in the tablegroup, and the filling of the tablegroup is taken
    const ObTablegroupSchema* tablegroup_schema = NULL;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id_, tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K_(table_id), K_(tenant_id), K_(tablegroup_id));
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(table_id_), K(tenant_id_), K_(tablegroup_id), KP(tablegroup_schema));
    } else if (OB_FAIL(tablegroup_schema->get_zone_replica_attr_array_inherit(schema_guard, locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret), K(table_id_), K_(tablegroup_id));
    }
  } else {
    // Locality is not set when creating table, take tenant's fill
    const ObTenantSchema* tenant_schema = NULL;
    if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(table_id_), K(tenant_id_));
    } else if (OB_UNLIKELY(NULL == tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(table_id_), K(tenant_id_), KP(tenant_schema));
    } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array_inherit(schema_guard, locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret), K(table_id_), K(tenant_id_));
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_primary_zone_inherit(
    ObSchemaGetterGuard& schema_guard, ObPrimaryZone& primary_zone) const
{
  int ret = OB_SUCCESS;
  primary_zone.reset();
  bool use_tenant_primary_zone = false;
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema ptr is null", K(ret), KPC(tenant_schema));
  } else if (tenant_schema->is_restore() && common::is_sys_table(table_id_)) {
    use_tenant_primary_zone = true;
  }

  if (OB_FAIL(ret)) {
  } else if (use_tenant_primary_zone) {
    if (OB_FAIL(tenant_schema->get_primary_zone_inherit(schema_guard, primary_zone))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(database_id_), K(tenant_id_));
    }
  } else if (!get_primary_zone().empty()) {
    if (OB_FAIL(primary_zone.set_primary_zone_array(get_primary_zone_array()))) {
      LOG_WARN("fail to set primary zone array", K(ret));
    } else if (OB_FAIL(primary_zone.set_primary_zone(get_primary_zone()))) {
      LOG_WARN("fail to set primary zone", K(ret));
    }
  } else if (is_new_tablegroup_id(tablegroup_id_)) {
    // The table is located in the tablegroup created in 2.0, and the tablegroup is taken
    const ObTablegroupSchema* tablegroup_schema = NULL;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id_, tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K_(table_id), K_(tenant_id), K_(tablegroup_id));
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema null", K(ret), K(table_id_), K(tenant_id_), K_(tablegroup_id), KP(tablegroup_schema));
    } else if (OB_FAIL(tablegroup_schema->get_primary_zone_inherit(schema_guard, primary_zone))) {
      LOG_WARN("fail to get primary zone array inherit", K(ret), K(table_id_), K_(tablegroup_id));
    }
  } else {
    // For tables in the old tablegroup or not in the tablegroup, the database is taken
    const ObDatabaseSchema* database_schema = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(get_database_id(), database_schema))) {
      LOG_WARN("fail to get database schema", K(ret), K(table_id_), K(database_id_));
    } else if (OB_UNLIKELY(NULL == database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database schema null", K(ret), K(table_id_), K(database_id_), KP(database_schema));
    } else if (OB_FAIL(database_schema->get_primary_zone_inherit(schema_guard, primary_zone))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(table_id_), K(database_id_));
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::set_zone_list(const common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  // The length of the string in zone_list_ptrs directly points to the ObZone of zone_list
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

int ObSimpleTableSchemaV2::set_zone_list(const common::ObIArray<common::ObString>& zone_list)
{
  return deep_copy_string_array(zone_list, zone_list_);
}

int ObSimpleTableSchemaV2::set_simple_foreign_key_info_array(
    const common::ObIArray<ObSimpleForeignKeyInfo>& simple_fk_info_array)
{
  int ret = OB_SUCCESS;

  simple_foreign_key_info_array_.reset();
  int64_t count = simple_fk_info_array.count();
  if (OB_FAIL(simple_foreign_key_info_array_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  FOREACH_CNT_X(simple_fk_info_iter, simple_fk_info_array, OB_SUCC(ret))
  {
    ret = add_simple_foreign_key_info(simple_fk_info_iter->tenant_id_,
        simple_fk_info_iter->database_id_,
        simple_fk_info_iter->table_id_,
        simple_fk_info_iter->foreign_key_id_,
        simple_fk_info_iter->foreign_key_name_);
  }

  return ret;
}

int ObSimpleTableSchemaV2::add_simple_foreign_key_info(const uint64_t tenant_id, const uint64_t database_id,
    const uint64_t table_id, const int64_t foreign_key_id, const ObString& foreign_key_name)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  ObSimpleForeignKeyInfo* simple_fk_info = NULL;
  if (OB_ISNULL(buf = alloc(sizeof(ObSimpleForeignKeyInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory, ", "size", sizeof(ObSimpleForeignKeyInfo), K(ret));
  } else if (OB_ISNULL(simple_fk_info = new (buf) ObSimpleForeignKeyInfo(
                           tenant_id, database_id, table_id, foreign_key_name, foreign_key_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to new ObSimpleForeignKeyInfo", K(ret));
  } else if (!foreign_key_name.empty() && OB_FAIL(deep_copy_str(foreign_key_name, simple_fk_info->foreign_key_name_))) {
    LOG_WARN("failed to deep copy foreign key name", K(ret), K(foreign_key_name));
  } else if (OB_FAIL(simple_foreign_key_info_array_.push_back(*simple_fk_info))) {
    LOG_WARN("failed to push back simple foreign key info", K(ret), K(*simple_fk_info));
  }

  return ret;
}

int ObSimpleTableSchemaV2::set_simple_constraint_info_array(
    const common::ObIArray<ObSimpleConstraintInfo>& simple_cst_info_array)
{
  int ret = OB_SUCCESS;

  simple_constraint_info_array_.reset();
  int64_t count = simple_cst_info_array.count();
  if (OB_FAIL(simple_constraint_info_array_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  FOREACH_CNT_X(simple_cst_info_iter, simple_cst_info_array, OB_SUCC(ret))
  {
    ret = add_simple_constraint_info(simple_cst_info_iter->tenant_id_,
        simple_cst_info_iter->database_id_,
        simple_cst_info_iter->table_id_,
        simple_cst_info_iter->constraint_id_,
        simple_cst_info_iter->constraint_name_);
  }

  return ret;
}

int ObSimpleTableSchemaV2::add_simple_constraint_info(const uint64_t tenant_id, const uint64_t database_id,
    const uint64_t table_id, const int64_t constraint_id, const common::ObString& constraint_name)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  ObSimpleConstraintInfo* simple_cst_info = NULL;
  if (OB_ISNULL(buf = alloc(sizeof(ObSimpleConstraintInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory, ", "size", sizeof(ObSimpleConstraintInfo), K(ret));
  } else if (OB_ISNULL(simple_cst_info = new (buf)
                           ObSimpleConstraintInfo(tenant_id, database_id, table_id, constraint_name, constraint_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to new ObSimpleConstraintInfo", K(ret));
  } else if (!constraint_name.empty() && OB_FAIL(deep_copy_str(constraint_name, simple_cst_info->constraint_name_))) {
    LOG_WARN("failed to deep copy constraint name", K(ret), K(constraint_name));
  } else if (OB_FAIL(simple_constraint_info_array_.push_back(*simple_cst_info))) {
    LOG_WARN("failed to push back simple constraint info", K(ret), K(*simple_cst_info));
  }

  return ret;
}

// 1. For clusters upgraded from version 1.4.4 before, if the table has not been alter_table,
//  its partition_cnt_within_partition_table is -1;
// 2. For clusters upgraded from version 1.4.4 and later, the partition_cnt_within_partition_table of the table should
// be 0;
int64_t ObSimpleTableSchemaV2::get_partition_cnt() const
{
  int64_t partition_cnt = part_option_.get_partition_cnt_within_partition_table();
  if (-1 == partition_cnt) {
    partition_cnt = get_all_part_num();
  }
  return partition_cnt;
}

bool ObSimpleTableSchemaV2::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()) {
    ret = false;
    LOG_WARN("ob_schema is unvalid", K(ret));
  }
  if (ret) {
    if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == table_id_ || schema_version_ < 0 ||
        OB_INVALID_ID == database_id_ || table_name_.empty()) {
      if (!is_link_valid()) {
        ret = false;
        LOG_WARN("invalid argument",
            K(tenant_id_),
            K(table_id_),
            K(schema_version_),
            K(database_id_),
            K(table_name_),
            K(dblink_id_),
            K(link_table_id_),
            K(link_schema_version_),
            K(link_database_name_));
      }
    } else if (is_index_table() || is_materialized_view()) {
      if (OB_INVALID_ID == data_table_id_) {
        ret = false;
        LOG_WARN("invalid data table_id", K(ret), K(data_table_id_));
      } else if (is_index_table() && !is_normal_index() && !is_unique_index() && !is_domain_index()) {
        ret = false;
        LOG_WARN("table_type is not consistent with index_type",
            "table_type",
            static_cast<int64_t>(table_type_),
            "index_type",
            static_cast<int64_t>(index_type_));
      }
    } else if (!is_index_table() && (INDEX_TYPE_IS_NOT != index_type_)) {
      ret = false;
      LOG_WARN("table_type is not consistent with index_type",
          "table_type",
          static_cast<int64_t>(table_type_),
          "index_type",
          static_cast<int64_t>(index_type_));
    }
  }
  return ret;
}

bool ObSimpleTableSchemaV2::is_link_valid() const
{
  return (OB_INVALID_ID != dblink_id_ && OB_INVALID_ID != link_table_id_ && !link_database_name_.empty() &&
          !table_name_.empty());
}

int64_t ObSimpleTableSchemaV2::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleTableSchemaV2);
  convert_size += table_name_.length() + 1;
  convert_size += primary_zone_.length() + 1;
  convert_size += locality_str_.length() + 1;
  convert_size += previous_locality_str_.length() + 1;
  convert_size += zone_list_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < zone_list_.count(); ++i) {
    convert_size += zone_list_.at(i).length() + 1;
  }
  convert_size += part_option_.get_convert_size() - sizeof(part_option_);
  convert_size += sub_part_option_.get_convert_size() - sizeof(sub_part_option_);
  // all part info size
  for (int64_t i = 0; i < partition_num_ && NULL != partition_array_[i]; ++i) {
    convert_size += partition_array_[i]->get_convert_size();
  }
  convert_size += partition_num_ * sizeof(ObPartition*);  // partition_array size
  // all dropped part info size
  for (int64_t i = 0; i < dropped_partition_num_ && NULL != dropped_partition_array_[i]; ++i) {
    convert_size += dropped_partition_array_[i]->get_convert_size();
  }
  convert_size += partition_num_ * sizeof(ObPartition*);  // partition_array size
  if (binding_) {
    convert_size += partition_num_ * sizeof(ObPartition*);  // sorted_part_id_array size;
  }
  // all sub part info size
  for (int64_t i = 0; i < def_subpartition_num_ && NULL != def_subpartition_array_[i]; ++i) {
    convert_size += def_subpartition_array_[i]->get_convert_size();
  }
  convert_size += def_subpartition_num_ * sizeof(ObSubPartition*);  // def subpart_parray size
  if (binding_) {
    convert_size += partition_num_ * sizeof(ObSubPartition*);  // sorted_def_sub_part_id_array size
  }
  convert_size += zone_replica_attr_array_.count() * static_cast<int64_t>(sizeof(SchemaZoneReplicaAttrSet));
  for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
    convert_size += zone_replica_attr_array_.at(i).get_convert_size();
  }
  convert_size += primary_zone_array_.get_data_size();
  for (int64_t i = 0; i < primary_zone_array_.count(); ++i) {
    convert_size += primary_zone_array_.at(i).zone_.length() + 1;
  }
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
  return convert_size;
}

int ObSimpleTableSchemaV2::get_encryption_id(int64_t& encrypt_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObEncryptionUtil::parse_encryption_id(encryption_, encrypt_id))) {
    LOG_WARN("failed to parse_encrytion_id", K(ret), K(encryption_));
  }
  return ret;
}

bool ObSimpleTableSchemaV2::need_encrypt() const
{
  bool ret = false;
  if (0 != encryption_.length() && 0 != encryption_.case_compare("none")) {
    ret = true;
  }
  return ret;
}
bool ObSimpleTableSchemaV2::is_equal_encryption(const ObSimpleTableSchemaV2& t) const
{
  bool res = false;
  if ((0 == get_encryption_str().case_compare(t.get_encryption_str())) ||
      (0 == get_encryption_str().length() && 0 == t.get_encryption_str().case_compare("none")) ||
      (0 == t.get_encryption_str().length() && 0 == get_encryption_str().case_compare("none"))) {
    res = true;
  }
  return res;
}

void ObSimpleTableSchemaV2::reset_zone_replica_attr_array()
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

int ObSimpleTableSchemaV2::set_zone_replica_attr_array(const common::ObIArray<SchemaZoneReplicaAttrSet>& src)
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

int ObSimpleTableSchemaV2::set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet>& src)
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

int ObSimpleTableSchemaV2::set_specific_replica_attr_array(
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

int ObSimpleTableSchemaV2::get_full_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  if (!locality_str_.empty()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
      num += zone_replica_attr_array_.at(i).get_full_replica_num();
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

int ObSimpleTableSchemaV2::get_all_replica_num(share::schema::ObSchemaGetterGuard& guard, int64_t& num) const
{
  int ret = OB_SUCCESS;
  num = 0;
  if (!locality_str_.empty()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
      const SchemaZoneReplicaAttrSet& set = zone_replica_attr_array_.at(i);
      num += set.get_specific_replica_num();
    }
  } else {
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else {
      for (int64_t i = 0; i < zone_locality.count(); ++i) {
        const share::ObZoneReplicaAttrSet& set = zone_locality.at(i);
        num += set.get_specific_replica_num();
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_has_own_not_f_replica(bool &has_not_f_replica) const
{
  int ret = OB_SUCCESS;
  has_not_f_replica = false;
  if (locality_str_.empty()) {
    has_not_f_replica = false;
  } else {
    for (int64_t i = 0; i < zone_replica_attr_array_.count() && !has_not_f_replica; ++i) {
      const SchemaZoneReplicaAttrSet &set = zone_replica_attr_array_.at(i);
      if (set.get_specific_replica_num() != set.get_full_replica_num() ||
          OB_ALL_SERVER_CNT == set.get_readonly_replica_num()) {
        // R@all_server not in get_specific_replica_num
        has_not_f_replica = true;
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_has_all_server_readonly_replica(
    share::schema::ObSchemaGetterGuard& guard, bool& has) const
{
  int ret = OB_SUCCESS;
  has = false;
  if (!locality_str_.empty()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count() && !has; ++i) {
      has = OB_ALL_SERVER_CNT == zone_replica_attr_array_.at(i).get_readonly_replica_num();
    }
  } else {
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else {
      for (int64_t i = 0; i < zone_locality.count() && !has; ++i) {
        has = OB_ALL_SERVER_CNT == zone_locality.at(i).get_readonly_replica_num();
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_is_readonly_at_all(share::schema::ObSchemaGetterGuard& guard,
    const common::ObZone& zone, const common::ObRegion& region, bool& readonly_at_all) const
{
  UNUSED(region);
  int ret = OB_SUCCESS;
  readonly_at_all = false;
  if (!locality_str_.empty()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count(); ++i) {
      if (zone == zone_replica_attr_array_.at(i).zone_) {
        readonly_at_all = (OB_ALL_SERVER_CNT == zone_replica_attr_array_.at(i).get_readonly_replica_num());
        break;
      }
    }
  } else {
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
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_is_all_server_readonly_replica(
    share::schema::ObSchemaGetterGuard& guard, bool& is) const
{
  int ret = OB_SUCCESS;
  is = true;
  if (!locality_str_.empty()) {
    for (int64_t i = 0; i < zone_replica_attr_array_.count() && is; ++i) {
      is = OB_ALL_SERVER_CNT == zone_replica_attr_array_.at(i).get_readonly_replica_num();
    }
  } else {
    common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
    if (OB_FAIL(get_zone_replica_attr_array_inherit(guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else {
      for (int64_t i = 0; i < zone_locality.count() && is; ++i) {
        is = OB_ALL_SERVER_CNT == zone_locality.at(i).get_readonly_replica_num();
      }
    }
  }
  return ret;
}

// Represents inheritance semantics
void ObSimpleTableSchemaV2::reset_primary_zone_options()
{
  primary_zone_.reset();
  primary_zone_array_.reset();
}

// Represents inheritance semantics
void ObSimpleTableSchemaV2::reset_locality_options()
{
  locality_str_.reset();
  reset_zone_replica_attr_array();
  if (NULL != zone_list_.get_base_address()) {
    free(zone_list_.get_base_address());
  }
  zone_list_.reset();
}

int ObSimpleTableSchemaV2::get_raw_first_primary_zone(
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

int ObSimpleTableSchemaV2::get_primary_zone_score(const common::ObZone& zone, int64_t& zone_score) const
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

int ObSimpleTableSchemaV2::set_primary_zone_array(const common::ObIArray<ObZoneScore>& primary_zone_array)
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

int64_t ObSimpleTableSchemaV2::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
      K_(database_id),
      K_(tablegroup_id),
      K_(table_id),
      K_(table_name),
      K_(replica_num),
      K_(previous_locality_str),
      K_(min_partition_id),
      K_(session_id),
      "index_type",
      static_cast<int32_t>(index_type_),
      "table_type",
      static_cast<int32_t>(table_type_),
      K_(table_mode),
      K_(tablespace_id));
  J_COMMA();
  J_KV(K_(data_table_id),
      "name_casemode",
      static_cast<int32_t>(name_case_mode_),
      K_(schema_version),
      K_(part_level),
      K_(part_option),
      K_(sub_part_option),
      K_(locality_str),
      K_(zone_list),
      K_(primary_zone),
      K_(part_num),
      K_(def_subpart_num),
      K_(partition_num),
      K_(def_subpartition_num),
      "partition_array",
      ObArrayWrap<ObPartition*>(partition_array_, partition_num_),
      "def_subpartition_array",
      ObArrayWrap<ObSubPartition*>(def_subpartition_array_, def_subpartition_num_),
      "dropped_partition_array",
      ObArrayWrap<ObPartition*>(dropped_partition_array_, dropped_partition_num_),
      K_(zone_replica_attr_array),
      K_(primary_zone_array),
      K_(index_status),
      K_(binding),
      K_(duplicate_scope),
      K_(encryption),
      K_(encrypt_key),
      K_(master_key_id),
      K_(drop_schema_version),
      K_(is_sub_part_template));
  J_OBJ_END();

  return pos;
}

bool ObSimpleTableSchemaV2::is_user_partition_table() const
{
  bool bret = false;
  if (!common::is_inner_table(get_table_id())) {
    if (is_partitioned_table() && !is_index_local_storage() && !is_view_table()) {
      bret = true;
    }
  }
  return bret;
}

bool ObSimpleTableSchemaV2::is_user_subpartition_table() const
{
  bool bret = false;
  if (!common::is_inner_table(get_table_id())) {
    if (PARTITION_LEVEL_TWO == get_part_level() && !is_index_local_storage() && !is_view_table()) {
      bret = true;
    }
  }
  return bret;
}

int ObSimpleTableSchemaV2::get_locality_str_inherit(
    share::schema::ObSchemaGetterGuard& guard, const common::ObString*& locality_str) const
{
  int ret = OB_SUCCESS;
  locality_str = NULL;
  bool use_tenant_locality = false;
  const share::schema::ObSimpleTenantSchema *simple_tenant = nullptr;
  if (OB_FAIL(guard.get_tenant_info(tenant_id_, simple_tenant))) {
    LOG_WARN("fail to get tenant info", K(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(nullptr == simple_tenant)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema ptr is null", K(ret), KPC(simple_tenant));
  } else if (simple_tenant->is_restore() && common::is_sys_table(table_id_)) {
    use_tenant_locality = true;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_ID == get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K_(table_id));
  } else if (!use_tenant_locality && !get_locality_str().empty()) {
    if (simple_tenant->is_restore()) {
      bool has_not_f_replica = false;
      if (OB_FAIL(check_has_own_not_f_replica(has_not_f_replica))) {
        LOG_WARN("failed to check has not f replica", KR(ret));
      } else if (has_not_f_replica) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("has not full replica while tenant is restore, try latter", KR(ret), KPC(simple_tenant));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!get_locality_str().empty() && !use_tenant_locality) {
    locality_str = &get_locality_str();
  } else {
    if (is_new_tablegroup_id(get_tablegroup_id()) && !use_tenant_locality) {
      const ObSimpleTablegroupSchema* tablegroup = NULL;
      if (OB_FAIL(guard.get_tablegroup_schema(get_tablegroup_id(), tablegroup))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), "tablegroup_id", get_tablegroup_id());
      } else if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablegroup schema", K(ret), "tablegroup_id", get_tablegroup_id());
      } else {
        locality_str = &tablegroup->get_locality_str();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!OB_ISNULL(locality_str) && !locality_str->empty()) {
    } else {
      const ObSimpleTenantSchema* tenant = NULL;
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
  }
  return ret;
}

bool ObSimpleTableSchemaV2::is_binding_table() const
{
  return binding_ && has_partition();
}

/*
 * Before calling this interface, it need to call is_binding_table to confirm whether it is a binding table
 * nonbinding table call interface, return error
 */
int ObSimpleTableSchemaV2::get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;
  if (!is_binding_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a binding table", K(ret), "table_id", get_table_id());
  } else if (OB_INVALID_ID == get_tablegroup_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup id is invalid", K(ret), "table_id", get_table_id());
  } else if (get_table_id() != pkey.get_table_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_id not match", "inner_table_id", get_table_id(), "pkey_table_id", pkey.get_table_id());
  } else {
    const uint64_t tg_id = get_tablegroup_id();
    const int64_t tg_partition_cnt = 0;
    const int64_t table_partition_id = pkey.get_partition_id();
    int64_t tg_partition_id = -1;
    if (PARTITION_LEVEL_ZERO == get_part_level()) {
      tg_partition_id = 0;
    } else if (PARTITION_LEVEL_ONE == get_part_level()) {
      if (nullptr == sorted_part_id_partition_array_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort part id partition array ptr is null", K(ret));
      } else {
        const int64_t part_num = get_first_part_num() + get_dropped_partition_num();
        ObPartition** ppart = std::lower_bound(sorted_part_id_partition_array_,
            sorted_part_id_partition_array_ + part_num,
            table_partition_id,
            compare_part_id);
        if (ppart == &sorted_part_id_partition_array_[part_num] || nullptr == *ppart ||
            (*ppart)->get_part_id() != table_partition_id) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("pkey not exist", K(pkey), K(*this));
        } else {
          tg_partition_id = (*ppart)->get_mapping_pg_part_id();
        }
      }
    } else if (PARTITION_LEVEL_TWO == get_part_level()) {
      const int64_t first_part_num = get_first_part_num();
      const int64_t part_num = get_first_part_num() + get_dropped_partition_num();
      int64_t sub_part_num = get_def_sub_part_num();
      ObPartition** sorted_part_id_partition_array = sorted_part_id_partition_array_;
      ObSubPartition** sorted_part_id_subpartition_array = sorted_part_id_def_subpartition_array_;
      const int64_t first_part_id = extract_part_idx(table_partition_id);
      const int64_t sub_part_id = extract_subpart_idx(table_partition_id);
      if (!is_sub_part_template()) {
        const ObPartition* partition = NULL;
        const bool check_dropped_partition = true;
        if (OB_FAIL(get_partition_by_part_id(first_part_id, check_dropped_partition, partition))) {
          LOG_WARN("fail to get partition by part_id", K(ret), K(first_part_id));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("fail to get partition by part_id", K(ret), K(first_part_id));
        } else {
          sorted_part_id_subpartition_array = partition->get_sorted_part_id_subpartition_array();
          sub_part_num = partition->get_sub_part_num() + partition->get_dropped_subpartition_num();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(sorted_part_id_partition_array) || OB_ISNULL(sorted_part_id_subpartition_array) ||
                 first_part_num <= 0 || sub_part_num <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid arg",
            K(ret),
            KP(sorted_part_id_partition_array),
            K(sorted_part_id_subpartition_array),
            K(first_part_num),
            K(sub_part_num));
      } else {
        ObPartition** ppart = std::lower_bound(
            sorted_part_id_partition_array, sorted_part_id_partition_array + part_num, first_part_id, compare_part_id);
        ObSubPartition** psubpart = std::lower_bound(sorted_part_id_subpartition_array,
            sorted_part_id_subpartition_array + sub_part_num,
            sub_part_id,
            compare_sub_part_id);
        if (ppart == &sorted_part_id_partition_array_[part_num] || nullptr == *ppart ||
            (*ppart)->get_part_id() != first_part_id || psubpart == &sorted_part_id_subpartition_array[sub_part_num] ||
            nullptr == *psubpart || (*psubpart)->get_sub_part_id() != sub_part_id) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("pkey not exist", K(pkey), K(*this));
        } else {
          // nontemplate table subpart is phyical_partition_id of pg
          tg_partition_id = !is_sub_part_template() ? (*psubpart)->get_mapping_pg_sub_part_id()
                                                    : generate_phy_part_id((*ppart)->get_mapping_pg_part_id(),
                                                          (*psubpart)->get_mapping_pg_sub_part_id(),
                                                          PARTITION_LEVEL_TWO);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part level unexpected", K(ret), "part_level", get_part_level());
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg_key.init(tg_id, tg_partition_id, tg_partition_cnt))) {
        LOG_WARN("fail to init pg key", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::get_pg_key(
    const uint64_t table_id, const int64_t partition_id, common::ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;
  common::ObPartitionKey pkey;
  if (OB_FAIL(pkey.init(table_id, partition_id, get_partition_cnt()))) {
    LOG_WARN("fail to init pkey", K(ret));
  } else if (OB_FAIL(get_pg_key(pkey, pg_key))) {
    LOG_WARN("fail to get pg key", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

bool ObSimpleTableSchemaV2::compare_part_id(const ObPartition* part, const int64_t part_id)
{
  bool b_ret = false;
  if (nullptr != part) {
    if (part->get_part_id() < part_id) {
      b_ret = true;
    }
  }
  return b_ret;
}

bool ObSimpleTableSchemaV2::compare_sub_part_id(const ObSubPartition* part, const int64_t sub_part_id)
{
  bool b_ret = false;
  if (nullptr != part) {
    if (part->get_sub_part_id() < sub_part_id) {
      b_ret = true;
    }
  }
  return b_ret;
}

ObTableSchema::ObTableSchema() : ObSimpleTableSchemaV2()
{
  reset();
}

ObTableSchema::ObTableSchema(ObIAllocator* allocator)
    : ObSimpleTableSchemaV2(allocator),
      view_schema_(allocator),
      join_conds_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      base_table_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      depend_table_ids_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      simple_index_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
      rowkey_info_(allocator),
      shadow_rowkey_info_(allocator),
      index_info_(allocator),
      partition_key_info_(allocator),
      subpartition_key_info_(allocator),
      foreign_key_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator))
{
  reset();
}

ObTableSchema::~ObTableSchema()
{}

int ObTableSchema::assign(const ObTableSchema& src_schema)
{
  int ret = OB_SUCCESS;

  if (this != &src_schema) {
    reset();
    if (OB_FAIL(ObSimpleTableSchemaV2::assign(src_schema))) {
      LOG_WARN("fail to assign simple table schema", K(ret));
    } else {
      ObColumnSchemaV2* column = NULL;
      char* buf = NULL;
      int64_t column_cnt = 0;
      int64_t cst_cnt = 0;
      max_used_column_id_ = src_schema.max_used_column_id_;
      max_used_constraint_id_ = src_schema.max_used_constraint_id_;
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
      create_mem_version_ = src_schema.create_mem_version_;
      code_version_ = src_schema.code_version_;
      last_modified_frozen_version_ = src_schema.last_modified_frozen_version_;
      first_timestamp_index_ = src_schema.first_timestamp_index_;
      index_attributes_set_ = src_schema.index_attributes_set_;
      row_store_type_ = src_schema.row_store_type_;
      store_format_ = src_schema.store_format_;
      progressive_merge_round_ = src_schema.progressive_merge_round_;
      storage_format_version_ = src_schema.storage_format_version_;
      table_dop_ = src_schema.table_dop_;
      if (OB_FAIL(deep_copy_str(src_schema.tablegroup_name_, tablegroup_name_))) {
        LOG_WARN("Fail to deep copy tablegroup_name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
        LOG_WARN("Fail to deep copy comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.pk_comment_, pk_comment_))) {
        LOG_WARN("Fail to deep copy primary key comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.create_host_, create_host_))) {
        LOG_WARN("Fail to deep copy primary key comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.compress_func_name_, compress_func_name_))) {
        LOG_WARN("Fail to deep copy compress func name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.expire_info_, expire_info_))) {
        LOG_WARN("Fail to deep copy expire info string", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.parser_name_, parser_name_))) {
        LOG_WARN("deep copy parser name failed", K(ret));
      } else {
      }  // no more to do

      // view schema
      if (OB_SUCC(ret)) {
        view_schema_ = src_schema.view_schema_;
        if (OB_FAIL(view_schema_.get_err_ret())) {
          LOG_WARN("fail to assign view schema", K(ret), K_(view_schema), K(src_schema.view_schema_));
        }
      }
      mv_cnt_ = src_schema.mv_cnt_;
      MEMCPY(mv_tid_array_, src_schema.mv_tid_array_, sizeof(uint64_t) * src_schema.mv_cnt_);

      if (FAILEDx(join_conds_.assign(src_schema.join_conds_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      if (FAILEDx(base_table_ids_.assign(src_schema.base_table_ids_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      if (FAILEDx(depend_table_ids_.assign(src_schema.depend_table_ids_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      if (FAILEDx(join_types_.assign(src_schema.join_types_))) {
        LOG_WARN("fail to assign array", K(ret));
      }

      // copy columns
      column_cnt = src_schema.column_cnt_;
      // copy constraints
      cst_cnt = src_schema.cst_cnt_;
      // prepare memory
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
        int64_t id_hash_array_size = get_id_hash_array_mem_size(column_cnt);
        if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Fail to allocate memory for id_hash_array, ", K(id_hash_array_size), K(ret));
        } else if (NULL == (id_hash_array_ = new (buf) IdHashArray(id_hash_array_size))) {
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
      ObColumnSchemaV2* rowid_column = NULL;
      for (int64_t i = 0; i < column_cnt; i++) {
        if (OB_ISNULL(src_schema.column_array_[i]) ||
            OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID != src_schema.column_array_[i]->get_column_id()) {
          // do nothing
        } else {
          rowid_column = src_schema.column_array_[i];
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        column = src_schema.column_array_[i];
        if (NULL == column) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is NULL.");
        } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column->get_column_id()) {
          // do nothing
        } else if (rowid_column != NULL) {
          ObColumnSchemaV2 tmp_column = *column;
          tmp_column.set_prev_column_id(UINT64_MAX);
          if (OB_FAIL(add_column(tmp_column))) {
            LOG_WARN("failed to add column", K(ret));
          } else {
            // do nothing
          }
        } else if (OB_FAIL(add_column(*column))) {
          LOG_WARN("Fail to add column, ", K(*column), K(ret));
        } else {
          // do nothing
        }
      }
      // make sure rowid column add last
      if (OB_SUCC(ret) && NULL != rowid_column) {
        ObColumnSchemaV2 tmp_column = *rowid_column;
        tmp_column.set_prev_column_id(UINT64_MAX);
        if (OB_FAIL(add_column(tmp_column))) {
          LOG_WARN("failed to add column", K(ret), K(*rowid_column));
        } else {
          // do nothing
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
        if (OB_FAIL(set_simple_index_infos(src_schema.get_simple_index_infos()))) {
          LOG_WARN("fail to set simple index infos", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to assign table schema", K(ret));
  }
  return ret;
}

int ObTableSchema::assign_constraint(const ObTableSchema& src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    cst_cnt_ = 0;
    cst_array_capacity_ = 0;
    cst_array_ = NULL;
    const int64_t cst_cnt = src_schema.cst_cnt_;
    if (cst_cnt > 0) {
      cst_array_ = static_cast<ObConstraint**>(alloc(sizeof(ObConstraint*) * cst_cnt));
      if (NULL == cst_array_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for cst_array", K(ret));
      } else {
        MEMSET(cst_array_, 0, sizeof(ObConstraint*) * cst_cnt);
        cst_array_capacity_ = cst_cnt;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < cst_cnt; ++i) {
        ObConstraint* constraint = src_schema.cst_array_[i];
        if (NULL == constraint) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The constraint is NULL.");
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

bool ObTableSchema::is_valid() const
{
  bool valid_ret = true;

  if (!ObSimpleTableSchemaV2::is_valid()) {
    valid_ret = false;
    LOG_WARN("schema is invalid", K_(error_ret));
  }

  if (!valid_ret || is_view_table()) {
    // no need checking other options for view
    // : TODO for materialized view
  } else {
    if (is_virtual_table(table_id_) && 0 > rowkey_column_num_) {
      valid_ret = false;
      LOG_WARN("invalid rowkey_column_num:", K_(table_name), K_(rowkey_column_num));
      // TODO:() confirm to delete it
    } else if (!is_virtual_table(table_id_) && 1 > rowkey_column_num_ && OB_INVALID_ID == dblink_id_) {
      valid_ret = false;
      LOG_WARN("no primary key specified:", K_(table_name));
    } else if (index_column_num_ < 0 || index_column_num_ > OB_MAX_ROWKEY_COLUMN_NUMBER) {
      valid_ret = false;
      LOG_WARN("invalid index_column_num", K_(table_name), K_(index_column_num));
    } else if (part_key_column_num_ > OB_MAX_PARTITION_KEY_COLUMN_NUMBER) {
      valid_ret = false;
      LOG_WARN("partition key column num invalid",
          K_(table_name),
          K_(part_key_column_num),
          K(OB_MAX_PARTITION_KEY_COLUMN_NUMBER));
    } else {
      int64_t def_rowkey_col = 0;
      int64_t def_index_col = 0;
      int64_t def_part_key_col = 0;
      int64_t def_subpart_key_col = 0;
      int64_t varchar_col_total_length = 0;
      int64_t rowkey_varchar_col_length = 0;
      ObColumnSchemaV2* column = NULL;

      if (NULL == column_array_) {
        valid_ret = false;
        LOG_WARN("The column_array is NULL.");
      }
      for (int64_t i = 0; valid_ret && i < column_cnt_; ++i) {
        if (NULL == (column = column_array_[i])) {
          valid_ret = false;
          LOG_WARN("The column is NULL.");
        } else {
          if (column->get_rowkey_position() > 0) {
            ++def_rowkey_col;
            if (column->get_column_id() > max_used_column_id_) {
              valid_ret = false;
              LOG_WARN("column id is greater than max_used_column_id, ",
                  "column_name",
                  column->get_column_name(),
                  "column_id",
                  column->get_column_id(),
                  K_(max_used_column_id));
            }
          }

          if (column->is_index_column()) {
            ++def_index_col;
            if (column->get_column_id() > max_used_column_id_) {
              valid_ret = false;
              LOG_WARN("column id is greater than max_used_column_id, ",
                  "column_name",
                  column->get_column_name(),
                  "column_id",
                  column->get_column_id(),
                  K_(max_used_column_id));
            }
          }
          if (column->is_part_key_column()) {
            ++def_part_key_col;
          }
          if (column->is_subpart_key_column()) {
            ++def_subpart_key_col;
          }

          if ((is_table() || is_tmp_table()) && !column->is_column_stored_in_sstable()) {
            // When the column is a virtual generated column in the table, the column will not be stored,
            // so there is no need to calculate its length
          } else if (is_storage_index_table() && column->is_fulltext_column()) {
            // The full text column in the index only counts the length of one word segment
            varchar_col_total_length += OB_MAX_OBJECT_NAME_LENGTH;
          } else {
            if (ObVarcharType == column->get_data_type()) {
              if (OB_MAX_VARCHAR_LENGTH < column->get_data_length()) {
                LOG_WARN("length of varchar column is larger than the max allowed length, ",
                    "data_length",
                    column->get_data_length(),
                    "column_name",
                    column->get_column_name(),
                    K(OB_MAX_VARCHAR_LENGTH));
                valid_ret = false;
              }
              varchar_col_total_length += column->get_data_length();
              if (column->is_rowkey_column() && !column->is_hidden()) {
                if (is_index_table() && 0 == column->get_index_position()) {
                  // Non-user-created index columns in the index table are not counted in rowkey_varchar_col_length
                } else {
                  rowkey_varchar_col_length += column->get_data_length();
                }
              }
            } else if (ob_is_text_tc(column->get_data_type()) || ob_is_json_tc(column->get_data_type())) {
              ObLength max_length = 0;
              if ((GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1470) && !ObSchemaService::g_liboblog_mode_) {
                max_length = ObAccuracy::MAX_ACCURACY_OLD[column->get_data_type()].get_length();
              } else {
                max_length = ObAccuracy::MAX_ACCURACY[column->get_data_type()].get_length();
              }
              if (max_length < column->get_data_length()) {
                LOG_WARN("length of text/blob column is larger than the max allowed length, ",
                    "data_length",
                    column->get_data_length(),
                    "column_name",
                    column->get_column_name(),
                    K(max_length));
                valid_ret = false;
              } else if (!column->is_shadow_column()) {
                // TODO need seperate inline memtable length from store length
                if ((GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1470) && !ObSchemaService::g_liboblog_mode_) {
                  varchar_col_total_length += column->get_data_length();
                } else {
                  varchar_col_total_length += min(column->get_data_length(), OB_MAX_LOB_HANDLE_LENGTH);
                }
              }
            }
          }
        }
      }
      if (valid_ret) {
        // TODO oushen confirm the length
        //
        // jiage: inner table shouldn't check VARCHAR length for
        // compatibility.  VARCHAR length in previous version means
        // maximum bytes of column, whereas new version changes to chars
        // of column.
        const int64_t max_row_length = is_sys_table() || is_vir_table() ? INT64_MAX : OB_MAX_USER_ROW_LENGTH;
        const int64_t max_rowkey_length =
            is_sys_table() || is_vir_table() ? OB_MAX_ROW_KEY_LENGTH : OB_MAX_USER_ROW_KEY_LENGTH;
        if (max_row_length < varchar_col_total_length) {
          LOG_WARN("total length of varchar columns is larger than the max allowed length",
              K(varchar_col_total_length),
              K(max_row_length));
          const ObString& col_name = column->get_column_name_str();
          LOG_USER_ERROR(
              OB_ERR_VARCHAR_TOO_LONG, static_cast<int>(varchar_col_total_length), max_rowkey_length, col_name.ptr());
          valid_ret = false;
        } else if (max_rowkey_length < rowkey_varchar_col_length) {
          LOG_WARN("total length of varchar primary key columns is larger than the max allowed length",
              K(rowkey_varchar_col_length),
              K(max_rowkey_length));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, max_rowkey_length);
          valid_ret = false;
        }
      }
      if (valid_ret) {
        if (def_rowkey_col != rowkey_column_num_) {
          valid_ret = false;
          LOG_WARN(
              "rowkey_column_num not equal with defined_num", K_(rowkey_column_num), K(def_rowkey_col), K_(table_name));
        }
      }
      if (valid_ret) {
        if (def_index_col != index_column_num_) {
          valid_ret = false;
          LOG_WARN(
              "index_column_num not equal with defined_num", K_(index_column_num), K(def_index_col), K_(table_name));
        }
      }
      if (valid_ret) {
        if (def_part_key_col != part_key_column_num_ || def_subpart_key_col != subpart_key_column_num_) {
          valid_ret = false;
          LOG_WARN("partition key column num not equal with the defined num",
              K_(part_key_column_num),
              K(def_part_key_col),
              K_(table_name));
        }
      }
    }
  }
  return valid_ret;
}

int ObTableSchema::set_compress_func_name(const char* compressor)
{
  return deep_copy_str(compressor, compress_func_name_);
}

int ObTableSchema::set_compress_func_name(const ObString& compressor)
{
  return deep_copy_str(compressor, compress_func_name_);
}

int ObTableSchema::set_row_store_type(const ObString& row_store)
{
  return ObStoreFormat::find_row_store_type(row_store, row_store_type_);
}

int ObTableSchema::set_store_format(const ObString& store_format)
{
  return ObStoreFormat::find_store_format_type(store_format, store_format_);
}

int ObTableSchema::check_in_locality_modification(
    share::schema::ObSchemaGetterGuard& schema_guard, bool& in_locality_modification) const
{
  int ret = OB_SUCCESS;
  if (get_locality_str().empty()) {
    if (is_new_tablegroup_id(tablegroup_id_)) {
      const ObTablegroupSchema* tablegroup_schema = NULL;
      if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id_, tablegroup_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), K_(table_id), K_(tenant_id), K_(tablegroup_id));
      } else if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema null", K(ret), K(table_id_), K(tenant_id_), K_(tablegroup_id), KP(tablegroup_schema));
      } else if (OB_FAIL(tablegroup_schema->check_in_locality_modification(schema_guard, in_locality_modification))) {
        LOG_WARN("fail to check in locality modification", K(ret), K_(tablegroup_id));
      }
    } else {
      // locality derived from tenant
      const ObTenantSchema* tenant_schema = NULL;
      if (OB_FAIL(schema_guard.get_tenant_info(get_tenant_id(), tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), "tenant_id", get_tenant_id());
      } else if (OB_UNLIKELY(NULL == tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, tenant schema is null", K(ret), KP(tenant_schema));
      } else {
        in_locality_modification = !tenant_schema->get_previous_locality_str().empty();
      }
    }
  } else {
    // has its own locality
    in_locality_modification = !get_previous_locality_str().empty();
  }
  return ret;
}

int ObTableSchema::delete_column_update_prev_id(ObColumnSchemaV2* local_column)
{
  int ret = OB_SUCCESS;
  // local_column is the schema obtained from the current table through the column name, prev/nextID is complete
  if (OB_ISNULL(local_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The column is NULL");
  } else {
    ObColumnSchemaV2* prev_col = get_column_schema(local_column->get_prev_column_id());
    ObColumnSchemaV2* next_col = get_column_schema(local_column->get_next_column_id());
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
      } else if (OB_ISNULL(prev_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL");
      } else {
        // next_col is null, so local_column is tail column
        prev_col->set_next_column_id(BORDER_COLUMN_ID);
      }
    }
  }
  return ret;
}

int ObTableSchema::add_column_update_prev_id(ObColumnSchemaV2* local_column)
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
      for (; iter != column_end(); ++iter) {
        if (BORDER_COLUMN_ID == (*iter)->get_next_column_id()) {
          local_column->set_prev_column_id((*iter)->get_column_id());
          (*iter)->set_next_column_id(local_column->get_column_id());
        }
      }
    } else {
      // only build next(Read from the internal table with prev ID, or add column only specifies prev ID)
      ObColumnSchemaV2* prev_col = get_column_schema_by_prev_next_id(local_column->get_prev_column_id());
      if (OB_NOT_NULL(prev_col)) {
        local_column->set_next_column_id(prev_col->get_next_column_id());
        prev_col->set_next_column_id(local_column->get_column_id());
      } else {
        local_column->set_next_column_id(BORDER_COLUMN_ID);
      }

      const_column_iterator iter = column_begin();
      for (; iter != column_end(); ++iter) {
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

int ObTableSchema::set_rowkey_info(const ObColumnSchemaV2& column)
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

int ObTableSchema::delete_column(const common::ObString& column_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* column_schema = NULL;
  if (OB_ISNULL(column_name) || column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column name is NULL", K(ret));
  } else {
    column_schema = get_column_schema(column_name);
    if (NULL == column_schema) {
      ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
      LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, column_name.length(), column_name.ptr());
    } else if (OB_FAIL(delete_column_internal(column_schema))) {
      LOG_WARN("Failed to delete column, ", K(column_name), K(ret));
    }
  }
  return ret;
}

int ObTableSchema::alter_column(ObColumnSchemaV2& column_schema)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  ObColumnSchemaV2* src_schema = NULL;
  ObColumnSchemaV2* dst_schema = NULL;
  src_schema = get_column_schema(column_schema.get_column_id());
  if (NULL == src_schema) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
        column_schema.get_column_name_str().length(),
        column_schema.get_column_name(),
        table_name_.length(),
        table_name_.ptr());
    LOG_WARN("column not exist", K(ret), "column_name", column_schema.get_column_name());
    // if the src_schema is a rowkey column
    // check_column_can_be_altered will modify dst_schema's is_nullable attribute to not nullable
    // TODO should move it to other place
  } else if (OB_FAIL(check_column_can_be_altered(src_schema, &column_schema))) {
    LOG_WARN("Failed to alter column schema", K(ret));
  } else if (NULL == (buf = static_cast<char*>(alloc(sizeof(ObColumnSchemaV2))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory, ", "size", sizeof(ObColumnSchemaV2), K(ret));
  } else if (NULL == (dst_schema = new (buf) ObColumnSchemaV2(allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to new dst_schema", K(ret));
  } else {
    *dst_schema = column_schema;
  }

  if (OB_SUCC(ret)) {
    if (!src_schema->is_autoincrement() && dst_schema->is_autoincrement()) {
      autoinc_column_id_ = dst_schema->get_column_id();
    }
    if (src_schema->is_autoincrement() && !dst_schema->is_autoincrement()) {
      autoinc_column_id_ = 0;
    }

    if (src_schema->get_column_name_str() == dst_schema->get_column_name_str()) {
      *src_schema = *dst_schema;
    } else {
      if (OB_FAIL(remove_col_from_name_hash_array(src_schema))) {
        LOG_WARN("Failed to remove old column name from name_hash_array", K(ret));
      } else if (OB_FAIL(add_col_to_name_hash_array(dst_schema))) {
        LOG_WARN("Failed to add new column name to name_hash_array", K(ret));
      } else {
        *src_schema = *dst_schema;
      }
    }
  }
  return ret;
}

int ObTableSchema::add_mv_tid(const uint64_t mv_tid)
{
  int ret = OB_SUCCESS;
  bool need_add = true;
  // we are sure that index_tid are added in sorted order
  if (mv_cnt_ > 0) {
    if (mv_tid < mv_tid_array_[mv_cnt_ - 1]) {
      if (!std::binary_search(mv_tid_array_, mv_tid_array_ + mv_cnt_, mv_tid)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mv_tid are expected to be added in sorted order", K(mv_tid), K(ret));
      } else {
        need_add = false;
      }
    } else if (mv_tid == mv_tid_array_[mv_cnt_ - 1]) {
      need_add = false;
    }
  }

  if (OB_SUCCESS == ret && need_add) {
    if (mv_cnt_ >= common::OB_MAX_INDEX_PER_TABLE) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      mv_tid_array_[mv_cnt_++] = mv_tid;
    }
  }
  return ret;
}

// description: oracle mode, When the user creates an index, without explicitly declaring the index name,
//  the system will automatically generate a constraint name for it
//              Generation rules: index_name_sys_auto = tblname_OBIDX_timestamp
//              If the length of tblname exceeds 60 bytes, the first 60 bytes will be truncated as the tblname in the
//              concatenated name
int ObTableSchema::create_idx_name_automatically_oracle(
    common::ObString& idx_name, const common::ObString& table_name, common::ObIAllocator& allocator)
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
    if (snprintf(temp_str_buf,
            sizeof(temp_str_buf),
            "%.*s_OBIDX_%ld",
            tmp_table_name.length(),
            tmp_table_name.ptr(),
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
// If the length of tblname exceeds 60 bytes, the first 60 bytes will be truncated as the tblname in the concatenated
// name
// @param [in] pk_name

// @return oceanbase error code defined in lib/ob_errno.def
int ObTableSchema::create_cons_name_automatically(
    ObString& cst_name, const ObString& table_name, common::ObIAllocator& allocator, ObConstraintType cst_type)
{
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  ObString cons_name_str;
  ObString tmp_table_name;

  if (table_name.length() > OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN) {
    if (OB_FAIL(ob_sub_str(allocator, table_name, 0, OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN - 1, tmp_table_name))) {
      SQL_RESV_LOG(WARN, "failed to cut table to 60 byte", K(ret), K(table_name));
    }
  } else {
    tmp_table_name = table_name;
  }
  if (OB_SUCC(ret)) {
    switch (cst_type) {
      case CONSTRAINT_TYPE_PRIMARY_KEY: {
        if (snprintf(temp_str_buf,
                sizeof(temp_str_buf),
                "%.*s_OBPK_%ld",
                tmp_table_name.length(),
                tmp_table_name.ptr(),
                ObTimeUtility::current_time()) < 0) {
          ret = OB_SIZE_OVERFLOW;
          SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
        }
        break;
      }
      case CONSTRAINT_TYPE_CHECK: {
        if (snprintf(temp_str_buf,
                sizeof(temp_str_buf),
                "%.*s_OBCHECK_%ld",
                tmp_table_name.length(),
                tmp_table_name.ptr(),
                ObTimeUtility::current_time()) < 0) {
          ret = OB_SIZE_OVERFLOW;
          SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
        }
        break;
      }
      case CONSTRAINT_TYPE_UNIQUE_KEY: {
        if (snprintf(temp_str_buf,
                sizeof(temp_str_buf),
                "%.*s_OBUNIQUE_%ld",
                tmp_table_name.length(),
                tmp_table_name.ptr(),
                ObTimeUtility::current_time()) < 0) {
          ret = OB_SIZE_OVERFLOW;
          SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;  // won't come here
        LOG_WARN("wrong type of ObConstraintType in this function", K(ret), K(cst_type));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, ObString::make_string(temp_str_buf), cons_name_str))) {
      SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret));
    } else {
      cst_name = cons_name_str;
    }
  }

  return ret;
}

// description: oracle mode, When the user flashbacks an indexed table, the system will automatically generate a new
// index name
//  for the index on the table
//              Generation rules: idx_name_flashback_auto = RECYCLE_OBIDX_timestamp
int ObTableSchema::create_new_idx_name_after_flashback(ObTableSchema& new_table_schema, common::ObString& new_idx_name,
    common::ObIAllocator& allocator, ObSchemaGetterGuard& guard)
{
  int ret = OB_SUCCESS;
  ObString tmp_str = "RECYCLE";
  ObString temp_idx_name;
  bool is_dup_idx_name_exist = true;
  const ObSimpleTableSchemaV2* simple_table_schema = NULL;

  while (OB_SUCC(ret) && is_dup_idx_name_exist) {
    if (OB_FAIL(create_idx_name_automatically_oracle(temp_idx_name, tmp_str, allocator))) {
      LOG_WARN("create index name automatically failed", K(ret));
    } else if (OB_FAIL(build_index_table_name(
                   allocator, new_table_schema.get_data_table_id(), temp_idx_name, new_idx_name))) {
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

void ObTableSchema::construct_partition_key_column(
    const ObColumnSchemaV2& column, ObPartitionKeyColumn& partition_key_column)
{
  partition_key_column.column_id_ = column.get_column_id();
  partition_key_column.length_ = column.get_data_length();
  partition_key_column.type_ = column.get_meta_type();
}

int ObTableSchema::add_partition_key(const common::ObString& column_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* column = NULL;
  ObPartitionKeyColumn partition_key_column;
  if (NULL == (column = const_cast<ObColumnSchemaV2*>(get_column_schema(column_name)))) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("fail to get column schema, return NULL", K(column_name), K(ret));
  } else if (column->is_part_key_column()) {
    LOG_INFO("already partiton key", K(column_name), K(ret));
  } else if (FALSE_IT(construct_partition_key_column(*column, partition_key_column))) {
  } else if (OB_FAIL(column->set_part_key_pos(partition_key_info_.get_size() + 1))) {
    LOG_WARN("Failed to set partition key position", K(ret));
  } else if (OB_FAIL(partition_key_info_.set_column(partition_key_info_.get_size(), partition_key_column))) {
    LOG_WARN("Failed to set partition coumn");
  } else {
    part_key_column_num_ = partition_key_info_.get_size();
  }
  return ret;
}

int ObTableSchema::add_subpartition_key(const common::ObString& column_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* column = NULL;
  ObPartitionKeyColumn partition_key_column;
  if (NULL == (column = const_cast<ObColumnSchemaV2*>(get_column_schema(column_name)))) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("fail to get column schema, return NULL", K(column_name), K(ret));
  } else if (column->is_subpart_key_column()) {
    LOG_INFO("already partiton key", K(column_name), K(ret));
  } else if (FALSE_IT(construct_partition_key_column(*column, partition_key_column))) {
  } else if (OB_FAIL(column->set_subpart_key_pos(subpartition_key_info_.get_size() + 1))) {
    LOG_WARN("Failed to set partition key position", K(ret));
  } else if (OB_FAIL(subpartition_key_info_.set_column(subpartition_key_info_.get_size(), partition_key_column))) {
    LOG_WARN("Failed to set partition coumn");
  } else {
    subpart_key_column_num_ = subpartition_key_info_.get_size();
  }
  return ret;
}

int ObTableSchema::add_zone(const common::ObString& zone)
{
  return add_string_to_array(zone, zone_list_);
}

int ObTableSchema::set_view_definition(const common::ObString& view_definition)
{
  return view_schema_.set_view_definition(view_definition);
}

int ObTableSchema::get_simple_index_infos(
    common::ObIArray<ObAuxTableMetaInfo>& simple_index_infos_array, bool with_mv) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if (!with_mv && MATERIALIZED_VIEW == simple_index_infos_.at(i).table_type_) {
      continue;
    } else if (OB_FAIL(simple_index_infos_array.push_back(simple_index_infos_[i]))) {
      LOG_WARN("fail to push back simple_index_infos_array", K(simple_index_infos_[i]));
    }
  }

  return ret;
}

int ObTableSchema::get_simple_index_infos_without_delay_deleted_tid(
    common::ObIArray<ObAuxTableMetaInfo>& simple_index_infos_array, bool with_mv) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if ((!with_mv && MATERIALIZED_VIEW == simple_index_infos_.at(i).table_type_) ||
        simple_index_infos_.at(i).is_dropped_schema()) {
      continue;
    } else if (OB_FAIL(simple_index_infos_array.push_back(simple_index_infos_[i]))) {
      LOG_WARN("fail to push back simple_index_infos_array", K(simple_index_infos_[i]));
    }
  }

  return ret;
}

int ObTableSchema::get_default_row(
    get_default_value func, const common::ObIArray<ObColDesc>& column_ids, ObNewRow& default_row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(default_row.cells_) || default_row.count_ != column_ids.count() || column_ids.count() > column_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ",
        K(ret),
        K(column_cnt_),
        K(default_row.count_),
        K(column_ids.count()),
        "cells",
        default_row.cells_);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < column_cnt_; ++j) {
      ObColumnSchemaV2* column = column_array_[j];
      if (NULL == column) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column must not null", K(ret), K(j), K(column_cnt_));
      } else if (column->get_column_id() == column_ids.at(i).col_id_) {
        default_row.cells_[i] = (column->*func)();
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

int ObTableSchema::get_orig_default_row(
    const common::ObIArray<ObColDesc>& column_ids, common::ObNewRow& default_row) const
{
  return get_default_row(&ObColumnSchemaV2::get_orig_default_value, column_ids, default_row);
}

ObColumnSchemaV2* ObTableSchema::get_column_schema_by_id_internal(const uint64_t column_id) const
{
  ObColumnSchemaV2* column = NULL;

  if (NULL != id_hash_array_) {
    if (OB_SUCCESS != id_hash_array_->get_refactored(ObColumnIdKey(column_id), column)) {
      column = NULL;
    }
  }
  return column;
}

const ObColumnSchemaV2* ObTableSchema::get_column_schema(const uint64_t column_id) const
{
  const ObColumnSchemaV2* column = get_column_schema_by_id_internal(ObColumnIdKey(column_id));
  return column;
}

const ObColumnSchemaV2* ObTableSchema::get_column_schema(uint64_t table_id, uint64_t column_id) const
{
  uint64_t col_id = column_id;
  if (has_depend_table(table_id)) {
    col_id = gen_materialized_view_column_id(column_id);
  }
  const ObColumnSchemaV2* column = get_column_schema_by_id_internal(ObColumnIdKey(col_id));
  return column;
}

ObColumnSchemaV2* ObTableSchema::get_column_schema(const uint64_t column_id)
{
  ObColumnSchemaV2* column = get_column_schema_by_id_internal(ObColumnIdKey(column_id));
  return column;
}

ObColumnSchemaV2* ObTableSchema::get_column_schema_by_name_internal(const ObString& column_name) const
{
  // If it is a table under the oceanbase database, expect to be case-insensitive when accessing the column
  bool is_mysql = share::is_mysql_mode();
  if (is_inner_table(table_id_)) {
    is_mysql = true;
  }
  CompatModeGuard g(is_mysql ? ObWorker::CompatMode::MYSQL : ObWorker::CompatMode::ORACLE);
  ObColumnSchemaV2* column = NULL;
  if (!column_name.empty() && NULL != name_hash_array_) {
    ObColumnSchemaHashWrapper column_name_key(column_name);
    if (OB_SUCCESS != name_hash_array_->get_refactored(column_name_key, column)) {
      column = NULL;
    }
  }
  return column;
}

const ObColumnSchemaV2* ObTableSchema::get_column_schema(const ObString& column_name) const
{
  return get_column_schema_by_name_internal(column_name);
}

ObColumnSchemaV2* ObTableSchema::get_column_schema(const ObString& column_name)
{
  return get_column_schema_by_name_internal(column_name);
}

const ObColumnSchemaV2* ObTableSchema::get_column_schema(const char* column_name) const
{
  const ObColumnSchemaV2* column = NULL;
  if (NULL == column_name || '\0' == column_name[0]) {
    LOG_WARN("invalid column name, ", K(column_name));
  } else {
    column = get_column_schema(ObString::make_string(column_name));
  }
  return column;
}

ObColumnSchemaV2* ObTableSchema::get_column_schema(const char* column_name)
{
  ObColumnSchemaV2* column = NULL;
  if (NULL == column_name || '\0' == column_name[0]) {
    LOG_WARN("invalid column name, ", K(column_name));
  } else {
    column = get_column_schema(ObString::make_string(column_name));
  }
  return column;
}

const ObColumnSchemaV2* ObTableSchema::get_column_schema_by_idx(const int64_t idx) const
{
  const ObColumnSchemaV2* column = NULL;
  if (idx < 0 || idx >= column_cnt_) {
    column = NULL;
  } else {
    column = column_array_[idx];
  }
  return column;
}

ObColumnSchemaV2* ObTableSchema::get_column_schema_by_idx(const int64_t idx)
{
  ObColumnSchemaV2* column = NULL;
  if (idx < 0 || idx >= column_cnt_) {
    column = NULL;
  } else {
    column = column_array_[idx];
  }
  return column;
}

ObColumnSchemaV2* ObTableSchema::get_column_schema_by_prev_next_id(const uint64_t id)
{
  ObColumnSchemaV2* column = NULL;
  if (BORDER_COLUMN_ID == id) {
    column = NULL;
  } else {
    column = get_column_schema(id);
  }
  return column;
}

const ObColumnSchemaV2* ObTableSchema::get_column_schema_by_prev_next_id(const uint64_t id) const
{
  const ObColumnSchemaV2* column = NULL;
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

bool ObTableSchema::is_drop_index() const
{
  return 0 != (index_attributes_set_ & ((uint64_t)(1) << INDEX_DROP_INDEX));
}

void ObTableSchema::set_drop_index(const uint64_t drop_index_value)
{
  index_attributes_set_ &= ~((uint64_t)(1) << INDEX_DROP_INDEX);
  index_attributes_set_ |= drop_index_value << INDEX_DROP_INDEX;
}

bool ObTableSchema::is_invisible_before() const
{
  return 0 != (index_attributes_set_ & ((uint64_t)1 << INDEX_VISIBILITY_SET_BEFORE));
}

void ObTableSchema::set_invisible_before(const uint64_t invisible_before)
{
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

// for :
// Convert columns to dependent table columns
// For a given column,
// - If the table does not have mv, the returned table_id is the table_id of the table, and col_id is the incoming
// col_id,
// - If the table has mv, but the given column is a non-dependent table column, the returned table_id is the table_id of
// this table,
//  and the col_id is the passed col_id
// - If the table has mv, and the given column is a dependent column, the returned table_id is the column of the
// dependent table,
//  and col_id is the original column of the dependent table mapping
int ObTableSchema::convert_to_depend_table_column(
    uint64_t column_id, uint64_t& convert_table_id, uint64_t& convert_column_id) const
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
  if (is_materialized_view() && column_id > OB_MIN_MV_COLUMN_ID && column_id < OB_MIN_SHADOW_COLUMN_ID &&
      get_column_schema_by_id_internal(column_id)) {
    is_depend = true;
  }
  return is_depend;
}

const ObColumnSchemaV2* ObTableSchema::get_fulltext_column(const ColumnReferenceSet& column_set) const
{
  const ObColumnSchemaV2* column = NULL;
  for (const_column_iterator col_iter = column_begin(); NULL == column && NULL != col_iter && col_iter != column_end();
       col_iter++) {
    if ((*col_iter)->is_generated_column() && (*col_iter)->is_fulltext_column()) {
      const ColumnReferenceSet* tmp_set = (*col_iter)->get_column_ref_set();
      if (tmp_set != NULL && *tmp_set == column_set) {
        column = *(col_iter);
      }
    }
  }
  return column;
}

int64_t ObTableSchema::get_column_idx(const uint64_t column_id, const bool ignore_hidden_column /* = false */) const
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
  convert_size += compress_func_name_.length() + 1;
  convert_size += expire_info_.length() + 1;
  convert_size += view_schema_.get_convert_size() - sizeof(view_schema_);

  for (int64_t i = 0; i < column_cnt_ && NULL != column_array_[i]; ++i) {
    convert_size += column_array_[i]->get_convert_size();
  }
  for (int64_t i = 0; i < cst_cnt_ && NULL != cst_array_[i]; ++i) {
    convert_size += cst_array_[i]->get_convert_size();
  }

  // materialized view
  convert_size += base_table_ids_.get_data_size();
  convert_size += depend_table_ids_.get_data_size();
  convert_size += join_conds_.get_data_size();
  convert_size += join_types_.get_data_size();

  convert_size += column_cnt_ * sizeof(ObColumnSchemaV2*);
  convert_size += cst_cnt_ * sizeof(ObConstraint*);

  convert_size += rowkey_info_.get_convert_size() - sizeof(rowkey_info_);
  convert_size += shadow_rowkey_info_.get_convert_size() - sizeof(shadow_rowkey_info_);
  convert_size += index_info_.get_convert_size() - sizeof(index_info_);
  convert_size += partition_key_info_.get_convert_size() - sizeof(partition_key_info_);
  convert_size += subpartition_key_info_.get_convert_size() - sizeof(subpartition_key_info_);
  convert_size += get_id_hash_array_mem_size(column_cnt_);
  convert_size += get_name_hash_array_mem_size(column_cnt_);

  convert_size += parser_name_.length() + 1;
  convert_size += simple_index_infos_.get_data_size();
  convert_size += foreign_key_infos_.get_data_size();
  for (int64_t i = 0; i < foreign_key_infos_.count(); ++i) {
    convert_size += foreign_key_infos_.at(i).get_convert_size();
  }
  return convert_size;
}

void ObTableSchema::reset()
{
  max_used_column_id_ = 0;
  max_used_constraint_id_ = 0;
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
  create_mem_version_ = 0;
  code_version_ = OB_SCHEMA_CODE_VERSION;
  last_modified_frozen_version_ = 0;
  first_timestamp_index_ = -1;
  index_attributes_set_ = OB_DEFAULT_INDEX_ATTRIBUTES_SET;
  row_store_type_ = ObStoreFormat::get_default_row_store_type();
  store_format_ = OB_STORE_FORMAT_INVALID;
  progressive_merge_round_ = 0;
  storage_format_version_ = OB_STORAGE_FORMAT_VERSION_INVALID;

  reset_string(tablegroup_name_);
  reset_string(comment_);
  reset_string(pk_comment_);
  reset_string(create_host_);
  reset_string(compress_func_name_);
  reset_string(expire_info_);
  reset_string(parser_name_);
  view_schema_.reset();
  join_conds_.reset();

  mv_cnt_ = 0;
  MEMSET(mv_tid_array_, 0, sizeof(mv_tid_array_));

  base_table_ids_.reset();
  depend_table_ids_.reset();
  join_conds_.reset();
  join_types_.reset();

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

  cst_cnt_ = 0;
  cst_array_capacity_ = 0;
  cst_array_ = NULL;
  foreign_key_infos_.reset();
  simple_index_infos_.reset();
  table_dop_ = 1;
  ObSimpleTableSchemaV2::reset();
}

// FIXME: There is a problem with the logic, and it is not called
// int ObTableSchema::get_partition(const common::ObString &part_name, ObPartition *&dst) const
// {
//   int ret = OB_SUCCESS;
//   dst = NULL;
//
//   if (part_name.empty()) {
//     ret = OB_INVALID_ARGUMENT;
//     LOG_WARN("empty column name", K(ret));
//   } else {
//     ObPartition *part = NULL;
//     for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; ++i) {
//       if (common::ObCharset::case_insensitive_equal(part_name,
//                                                     partition_array_[i]->get_part_name())) {
//         part = partition_array_[i];
//         break;
//       }
//     }
//     if (OB_SUCC(ret)) {
//       if (NULL != part) {
//         ret = OB_ENTRY_NOT_EXIST;
//       }
//     }
//   }
//   return ret;
// }

// Get the partition_ids of a first-level partition of the second-level partition table,
// and the delayed deletion object is not visible
int ObTableSchema::get_physical_partition_ids(int64_t part_id, ObIArray<int64_t>& partition_ids) const
{
  int ret = OB_SUCCESS;
  const ObPartition* partition = NULL;
  bool check_dropped_partition = false;
  if (PARTITION_LEVEL_TWO != get_part_level()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table should be partition-level-two", K(ret));
  } else if (OB_FAIL(get_partition_by_part_id(part_id, check_dropped_partition, partition))) {
    LOG_WARN("fail to get partition", K(ret), K(part_id));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("partition not exist", K(ret), K(part_id));
  } else {
    ObSubPartition** sub_part_array = NULL;
    int64_t sub_part_num = 0;
    if (is_sub_part_template()) {
      sub_part_array = def_subpartition_array_;
      sub_part_num = def_subpartition_num_;
    } else {
      sub_part_array = partition->get_subpart_array();
      sub_part_num = partition->get_subpartition_num();
    }
    if (OB_ISNULL(sub_part_array) || sub_part_num <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub_part_array is null or is empty", K(ret), KP(sub_part_array), K(sub_part_num));
    } else {
      int64_t partition_id = OB_INVALID_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < sub_part_num; i++) {
        if (OB_ISNULL(sub_part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null subpartition info", K(ret));
        } else {
          partition_id = common::generate_phy_part_id(part_id, sub_part_array[i]->get_sub_part_id());
          if (OB_FAIL(partition_ids.push_back(partition_id))) {
            LOG_WARN("failed to push back partition_id", K(ret), K(partition_id));
          } else { /*do nothing*/
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::get_physical_partition_ids(ObIArray<int64_t>& partition_ids) const
{
  int ret = OB_SUCCESS;
  int64_t partition_id = OB_INVALID_ID;
  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    if (OB_FAIL(partition_ids.push_back(0))) {
      LOG_WARN("fail to push back partition_id", K(ret));
    }
  } else if (OB_ISNULL(partition_array_) || partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_array is null or is empty", K(ret), KP_(partition_array), K_(partition_num));
  } else if (PARTITION_LEVEL_ONE == get_part_level()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else if (FALSE_IT(partition_id = partition_array_[i]->get_part_id())) {
      } else if (OB_FAIL(partition_ids.push_back(partition_id))) {
        LOG_WARN("fail to push back partition_id", K(ret), K(partition_id));
      }
    }
  } else if (PARTITION_LEVEL_TWO == get_part_level()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
      if (OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else {
        int64_t part_id = partition_array_[i]->get_part_id();
        ObSubPartition** sub_part_array = NULL;
        int64_t sub_part_num = 0;
        if (is_sub_part_template()) {
          sub_part_array = def_subpartition_array_;
          sub_part_num = def_subpartition_num_;
        } else {
          sub_part_array = partition_array_[i]->get_subpart_array();
          sub_part_num = partition_array_[i]->get_subpartition_num();
        }
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
              partition_id = common::generate_phy_part_id(part_id, sub_part_array[j]->get_sub_part_id());
              if (OB_FAIL(partition_ids.push_back(partition_id))) {
                LOG_WARN("failed to push back partition_id", K(ret), K(partition_id));
              } else { /*do nothing*/
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::alloc_partition(const ObPartition*& partition)
{
  int ret = OB_SUCCESS;
  partition = NULL;

  ObPartition* new_part = OB_NEWx(ObPartition, (get_allocator()));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(ret));
  } else {
    partition = new_part;
  }
  return ret;
}

int ObTableSchema::alloc_partition(const ObSubPartition*& subpartition)
{
  int ret = OB_SUCCESS;
  subpartition = NULL;

  ObSubPartition* new_part = OB_NEWx(ObSubPartition, (get_allocator()));
  if (NULL == new_part) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Fail to allocate memory", K(ret));
  } else {
    subpartition = new_part;
  }
  return ret;
}

int ObTableSchema::add_col_to_id_hash_array(ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int hash_ret = 0;
  int64_t id_hash_array_mem_size = 0;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else {
    if (NULL == id_hash_array_) {
      id_hash_array_mem_size = get_id_hash_array_mem_size(common::OB_MAX_COLUMN_NUMBER);
      if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for id_hash_array, ", K(id_hash_array_mem_size));
      } else if (NULL == (id_hash_array_ = new (buf) IdHashArray(id_hash_array_mem_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to new id_hash_array.");
      } else {
        if (OB_SUCCESS != (hash_ret = id_hash_array_->set_refactored(ObColumnIdKey(column->get_column_id()), column))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("Fail to set column to id_hash_array, ", K(hash_ret));
        }
      }
    } else if (OB_SUCCESS !=
               (hash_ret = id_hash_array_->set_refactored(ObColumnIdKey(column->get_column_id()), column))) {
      if (OB_HASH_FULL == hash_ret) {
        id_hash_array_mem_size = get_id_hash_array_mem_size(id_hash_array_->count() * 2);
        if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_mem_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to alloc memory", K(id_hash_array_mem_size), K(ret));
        } else {
          IdHashArray* new_array = new (buf) IdHashArray(id_hash_array_mem_size);
          if (NULL == new_array) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Fail to new IdHashArray", K(ret));
          }

          for (int64_t i = 0; OB_SUCCESS == ret && i < column_cnt_ && NULL != column_array_[i]; ++i) {
            if (OB_SUCCESS != (hash_ret = new_array->set_refactored(
                                   ObColumnIdKey(column_array_[i]->get_column_id()), column_array_[i]))) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("Fail to set column to id_hash_array, ", K(hash_ret));
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
        LOG_WARN("Fail to set column to id_hash_array, ",
            K(hash_ret),
            K(column->get_column_id()),
            K(id_hash_array_),
            K(column));
      }
    }
  }

  return ret;
}

int ObTableSchema::remove_col_from_id_hash_array(const ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* column_tmp = NULL;
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
    } else { /*do nothing*/
    }
  }

  return ret;
}

int ObTableSchema::add_col_to_name_hash_array(ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int hash_ret = 0;
  int64_t name_hash_array_mem_size = 0;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else {
    // In some scenarios, the tenant id is not initialized when add_column, 4002 will be reported here,
    // and the error code will not be processed temporarily.
    ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode);
    CompatModeGuard g(compat_mode);
    ObColumnSchemaHashWrapper column_name_key(column->get_column_name_str());
    if (NULL == name_hash_array_) {
      name_hash_array_mem_size = get_name_hash_array_mem_size(common::OB_MAX_COLUMN_NUMBER);
      if (NULL == (buf = static_cast<char*>(alloc(name_hash_array_mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory, ", K(name_hash_array_mem_size), K(ret));
      } else if (NULL == (name_hash_array_ = new (buf) NameHashArray(name_hash_array_mem_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to new NameHashArray", K(ret));
      } else {
        ObColumnSchemaV2** column_ptr = name_hash_array_->get(column_name_key);
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
          NameHashArray* new_array = new (buf) NameHashArray(name_hash_array_mem_size);
          if (NULL == new_array) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Fail to new NameHashArray", K(ret));
          }

          for (int64_t i = 0; OB_SUCCESS == ret && NULL != column_array_[i] && i < column_cnt_; ++i) {
            ObColumnSchemaHashWrapper column_name_key_local(column_array_[i]->get_column_name_str());
            if (OB_SUCCESS != (hash_ret = new_array->set_refactored(column_name_key_local, column_array_[i]))) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("Fail to set column to name_hash_array, ", K(hash_ret), K(ret));
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
      } else if (hash_ret == OB_HASH_EXIST) {
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

int ObTableSchema::remove_col_from_name_hash_array(const ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* column_tmp = NULL;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else if (NULL == name_hash_array_) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("name hash array is NULL", K(ret));
  } else {
    // Tenant id is not initialized when add_column in some scenarios
    ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode);
    CompatModeGuard g(compat_mode);
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

int ObTableSchema::add_col_to_column_array(ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column is NULL", K(ret));
  } else {
    if (0 == column_array_capacity_) {
      if (NULL == (column_array_ =
                          static_cast<ObColumnSchemaV2**>(alloc(sizeof(ObColumnSchemaV2*) * DEFAULT_ARRAY_CAPACITY)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for column_array_.");
      } else {
        column_array_capacity_ = DEFAULT_ARRAY_CAPACITY;
        MEMSET(column_array_, 0, sizeof(ObColumnSchemaV2*) * DEFAULT_ARRAY_CAPACITY);
      }
    } else if (column_cnt_ >= column_array_capacity_) {
      int64_t tmp_size = 2 * column_array_capacity_;
      ObColumnSchemaV2** tmp = NULL;
      if (NULL == (tmp = static_cast<ObColumnSchemaV2**>(alloc(sizeof(ObColumnSchemaV2*) * tmp_size)))) {
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

int ObTableSchema::remove_col_from_column_array(const ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* tmp_column = NULL;
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
      column_array_[i] = column_array_[i + 1];
    }
  }
  return ret;
}

int ObTableSchema::delete_column_internal(ObColumnSchemaV2* column_schema)
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
  } else if (!is_user_table() && !is_index_table() && !is_tmp_table() && !is_sys_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Only NORMAL table and index table and SYSTEM table are allowed", K(ret));
  } else if ((!is_no_pk_table() && column_cnt_ <= 1) || (is_old_no_pk_table() && column_cnt_ <= 4) ||
             (is_new_no_pk_table() && column_cnt_ <= 2)) {
    ret = OB_CANT_REMOVE_ALL_FIELDS;
    LOG_USER_ERROR(OB_CANT_REMOVE_ALL_FIELDS);
    LOG_WARN("Can not delete all columns in table", K(ret));
  } else if (column_schema->is_rowkey_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Row key column is not allowed", K(ret));
  } else if (column_schema->is_index_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Index key column is not allowed", K(ret));
  } else if (column_schema->is_tbl_part_key_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Partition key column is not allowed delete", K(ret));
  } else {
    if (OB_FAIL(delete_column_update_prev_id(column_schema))) {
      LOG_WARN("Failed to update column previous id", K(ret));
    } else if (OB_FAIL(remove_col_from_column_array(column_schema))) {
      LOG_WARN("Failed to remove col from column array", K(ret));
    } else if (OB_FAIL(remove_col_from_id_hash_array(column_schema))) {
      LOG_WARN("Failed to remove col from id hash array", K(ret));
    } else if (OB_FAIL(remove_col_from_name_hash_array(column_schema))) {
      LOG_WARN("Failed to remove col from name hash array", K(ret));
    } else {
      --column_cnt_;
      if (column_schema->is_autoincrement()) {
        autoinc_column_id_ = 0;
      }
      free(column_schema);
      column_schema = NULL;
    }
  }

  return ret;
}

int ObTableSchema::check_column_can_be_altered(const ObColumnSchemaV2* src_schema, ObColumnSchemaV2* dst_schema)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* tmp_column = NULL;
  if (OB_ISNULL(src_schema) || NULL == dst_schema) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column schema is NULL", K(ret));
  } else if (!src_schema->is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The column schema has error", K(ret));
  } else if (get_table_id() != src_schema->get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The column does not belong to this table", K(ret));
  } else if (!is_user_table() && !is_index_table() && !is_tmp_table() && !is_sys_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Only NORMAL table and INDEX table and SYSTEM table are allowed", K(ret));
  } else {
    LOG_DEBUG("check column schema can be altered", KPC(src_schema), KPC(dst_schema));
    if ((src_schema->get_data_type() == dst_schema->get_data_type() &&
            src_schema->get_collation_type() == dst_schema->get_collation_type()) ||
        (ob_is_integer_type(src_schema->get_data_type()) &&  // can change int to large scale
            src_schema->get_data_type_class() == dst_schema->get_data_type_class()) ||
        (src_schema->is_string_type() && dst_schema->is_string_type() &&
            src_schema->get_charset_type() == dst_schema->get_charset_type() &&
            src_schema->get_collation_type() == dst_schema->get_collation_type())) {
      bool is_oracle_mode = false;
      if ((ob_is_large_text(src_schema->get_data_type())) && src_schema->get_data_type() != dst_schema->get_data_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify large text/lob column");
        LOG_WARN("The data of large text/lob column can not be altered", K(ret), KPC(dst_schema), KPC(src_schema));
      } else if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("check if oracle compat mode failed", K(ret));
      } else if (((!is_oracle_mode && src_schema->is_string_type()) ||
                     (is_oracle_mode && src_schema->get_meta_type().is_lob()) ||
                     (is_oracle_mode && src_schema->get_meta_type().is_character_type() &&
                         src_schema->get_length_semantics() == dst_schema->get_length_semantics()) ||
                     src_schema->is_raw() || src_schema->get_meta_type().is_urowid()) &&
                 (dst_schema->get_data_length() < src_schema->get_data_length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Truncate the data of column schema");
        LOG_WARN("The data of column schema can not be truncated", K(ret), KPC(dst_schema), KPC(src_schema));
      } else if ((src_schema->get_data_type() == ObCharType && dst_schema->get_data_type() != ObCharType) ||
                 (src_schema->get_data_type() == ObNCharType && dst_schema->get_data_type() != ObNCharType)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify char type to other data types");
        LOG_WARN("can not modify char type to other data types", K(ret), KPC(src_schema), K(dst_schema));
      } else if (src_schema->get_data_type() == ObCharType && src_schema->get_collation_type() == CS_TYPE_BINARY &&
                 (dst_schema->get_data_length() != src_schema->get_data_length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Truncated data or change binary column length");
        LOG_WARN("The data of column schema can not be truncated, "
                 "binary column can't change length",
            K(ret),
            KPC(src_schema),
            KPC(dst_schema));
      } else if (dst_schema->get_data_type() == ObCharType && dst_schema->get_collation_type() == CS_TYPE_BINARY &&
                 !(src_schema->get_data_type() == ObCharType && src_schema->get_collation_type() == CS_TYPE_BINARY &&
                     src_schema->get_data_length() == dst_schema->get_data_length())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify column to binary type");
        LOG_WARN("can not modify data to binary type", K(ret), KPC(src_schema), KPC(dst_schema));
      } else if (ob_is_integer_type(src_schema->get_data_type()) &&
                 src_schema->get_data_type() > dst_schema->get_data_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Change int data type to small scale");
        LOG_WARN("can't not change int data type to small scale",
            "src",
            src_schema->get_data_type(),
            "dst",
            dst_schema->get_data_type(),
            K(ret));
      } else if ((src_schema->get_meta_type().is_varying_len_char_type()
                  || src_schema->get_meta_type().is_text())
               && dst_schema->get_meta_type().is_fixed_len_char_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Change to fixed length char type");
        LOG_WARN("can't not change to fixed length char type",
            "src",
            src_schema->get_data_type(),
            "dst",
            dst_schema->get_data_type(),
            K(ret));
      } else {
        tmp_column = get_column_schema(dst_schema->get_column_name());
        if ((NULL != tmp_column) && (tmp_column != src_schema)) {
          ret = OB_ERR_COLUMN_DUPLICATE;
          LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE,
              dst_schema->get_column_name_str().length(),
              dst_schema->get_column_name_str().ptr());
          LOG_WARN("Column already exist!", K(ret), "column_name", dst_schema->get_column_name_str());
        } else if (!src_schema->is_autoincrement() && dst_schema->is_autoincrement() && autoinc_column_id_ != 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "More than one auto increment column");
          LOG_WARN("Only one auto increment row is allowed", K(ret));
        }
        if (OB_SUCC(ret) && src_schema->is_rowkey_column()) {
          ObColumnSchemaV2* dst_col = dst_schema;
          if (!src_schema->is_heap_alter_rowkey_column()) {
            dst_col->set_nullable(false);
          }
          if (OB_FAIL(check_rowkey_column_can_be_altered(src_schema, dst_schema))) {
            LOG_WARN("Row key column can not be altered", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_oracle_mode && src_schema->get_meta_type().is_character_type() &&
            dst_schema->get_meta_type().is_character_type() &&
            src_schema->get_length_semantics() != dst_schema->get_length_semantics()) {
          // oracle mode the column length of char semantics needs to be converted into the length of byte semantics for
          // comparison, and it is not allowed to change it to a smaller value.
          int64_t mbmaxlen = 0;
          if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(src_schema->get_collation_type(), mbmaxlen))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get mbmaxlen", K(ret), K(src_schema->get_collation_type()));
          } else if (0 >= mbmaxlen) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mbmaxlen is less than 0", K(ret), K(mbmaxlen));
          } else {
            int32_t src_col_byte_len = src_schema->get_data_length();
            int32_t dst_col_byte_len = dst_schema->get_data_length();
            if (!is_oracle_byte_length(is_oracle_mode, src_schema->get_length_semantics())) {
              src_col_byte_len = src_col_byte_len * mbmaxlen;
            } else {
              dst_col_byte_len = dst_col_byte_len * mbmaxlen;
            }
            if (src_col_byte_len > dst_col_byte_len) {
              ret = OB_ERR_DECREASE_COLUMN_LENGTH;
              LOG_WARN("The data of column schema can not be truncated",
                  K(ret),
                  K(mbmaxlen),
                  K(src_col_byte_len),
                  K(dst_col_byte_len));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_row_length(src_schema, dst_schema))) {
            LOG_WARN("check row length failed", K(ret));
          }
        }
      }
    } else if (src_schema->is_string_type() && dst_schema->is_string_type() &&
               src_schema->get_collation_type() != dst_schema->get_collation_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter charset or collation type");
    } else {
      // oracle support DATE column <-> TIMESTAMP or TIMESTAMP WITH LOCAL TIME ZONE column. BUT, ob NOT support now
      // https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/ALTER-TABLE.html#GUID-552E7373-BF93-477D-9DA3-B2C9386F2877
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter non string type");
      LOG_WARN("The data type of column schema is non string type can not be altered",
          K(ret),
          K(*src_schema),
          K(*dst_schema));
    }
  }
  return ret;
}

// Non-indexed tables do not exceed the limit of OB_MAX_USER_ROW_KEY_LENGTH for the sum of the length of
// the primary key column of string type
// The index table does not exceed the limit of OB_MAX_USER_ROW_KEY_LENGTH for the total length of the index column in
// the primary key column of string type (the hidden primary key column in the index is not included in the total
// length)
int ObTableSchema::check_rowkey_column_can_be_altered(
    const ObColumnSchemaV2* src_schema, const ObColumnSchemaV2* dst_schema)
{
  int ret = OB_SUCCESS;
  // rowkey column will always be not null
  //  if (dst_schema->is_nullable()) {
  //    ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
  //    LOG_WARN("The rowkey column can not be null", K(ret));
  //  }
  // todo  will check alter table add primary key
  //  if (!dst_schema->is_rowkey_column() && get_rowkey_column_num() <= 1) {
  //    ret = OB_NOT_SUPPORTED;
  //    LOG_WARN("There must be at least one primary key column", K(ret));
  //  }
  ObColumnSchemaV2* column = NULL;
  int64_t rowkey_varchar_col_length = 0;
  int64_t length = 0;
  if (OB_ISNULL(src_schema) || OB_ISNULL(dst_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_schema), K(dst_schema));
  } else {
    if (ob_is_string_tc(src_schema->get_data_type()) && ob_is_string_tc(dst_schema->get_data_type())) {
      const int64_t max_rowkey_length = is_sys_table() ? OB_MAX_ROW_KEY_LENGTH : OB_MAX_USER_ROW_KEY_LENGTH;
      for (int64_t i = 0; OB_SUCC(ret) && (i < column_cnt_); ++i) {
        column = column_array_[i];
        if ((!is_index_table() && (column->get_rowkey_position() > 0)) ||
            (is_index_table() && (column->is_index_column()))) {
          if (ob_is_string_tc(column->get_data_type())) {
            if (OB_FAIL(column->get_byte_length(length))) {
              LOG_WARN("fail to get byte length of column", K(ret));
            } else {
              rowkey_varchar_col_length += length;
            }
          }
        }
      }
      int64_t src_column_byte_length = 0;
      int64_t dst_column_byte_length = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(src_schema->get_byte_length(src_column_byte_length))) {
        LOG_WARN("fail to get byte length of column", K(ret));
      } else if (OB_FAIL(dst_schema->get_byte_length(dst_column_byte_length))) {
        LOG_WARN("fail to get byte length of column", K(ret));
      } else {
        rowkey_varchar_col_length -= src_column_byte_length;
        rowkey_varchar_col_length += dst_column_byte_length;
      }
      if (OB_FAIL(ret)) {
      } else if (rowkey_varchar_col_length > max_rowkey_length) {
        ret = OB_ERR_TOO_LONG_KEY_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, max_rowkey_length);
        LOG_WARN("total length of varchar primary key columns is larger than the max allowed length",
            K(rowkey_varchar_col_length),
            K(max_rowkey_length),
            K(ret));
      }
    } else if (ObTextTC == dst_schema->get_data_type_class()
               || ObJsonTC == dst_schema->get_data_type_class()) {     
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify rowkey column to text/clob/blob");
    }
  }

  return ret;
}

// NULL == src_schema : for add_column
// NULL != src_schema : for alter_column
int ObTableSchema::check_row_length(const ObColumnSchemaV2* src_schema, const ObColumnSchemaV2* dst_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dst_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst_schema));
  } else {
    const int64_t max_row_length = is_inner_table(get_table_id()) ? INT64_MAX : OB_MAX_USER_ROW_LENGTH;
    ObColumnSchemaV2* col = NULL;
    int64_t row_length = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      col = column_array_[i];
      if ((is_table() || is_tmp_table()) && !col->is_column_stored_in_sstable()) {
        // The virtual column in the table does not actually store data, and does not count the length
      } else if (is_storage_index_table() && col->is_fulltext_column()) {
        // The full text column in the index only counts the length of one word segment
        row_length += OB_MAX_OBJECT_NAME_LENGTH;
      } else if (ob_is_string_type(col->get_data_type()) || ob_is_json(col->get_data_type())) {
        int64_t length = 0;
        if (OB_FAIL(col->get_byte_length(length, true))) {
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
        if (OB_FAIL(src_schema->get_byte_length(src_byte_length, true))) {
          SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t dst_byte_length = 0;
        if (OB_FAIL(dst_schema->get_byte_length(dst_byte_length, true))) {
          SQL_RESV_LOG(WARN, "fail to get byte length of column", K(ret));
        } else {
          row_length -= src_byte_length;
          if ((is_table() || is_tmp_table()) && !dst_schema->is_column_stored_in_sstable()) {
            // The virtual column in the table does not actually store data, and does not count the length
          } else if (is_storage_index_table() && dst_schema->is_fulltext_column()) {
            // The full text column in the index only counts the length of one word segment
            row_length += OB_MAX_OBJECT_NAME_LENGTH;
          } else {
            row_length += dst_byte_length;
          }
          if (row_length > max_row_length) {
            ret = OB_ERR_TOO_BIG_ROWSIZE;
          }
        }
      }
    }
  }

  return ret;
}

// Determine whether it is an old table without a primary key
// (add pk_increment/partition_id/clustre_id three columns to hide the primary key)
// For a new table without a primary key (partition key + pk_increment as the primary key), return false
bool ObTableSchema::is_old_no_pk_table() const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  const ObRowkeyInfo& rowkey = get_rowkey_info();
  if (rowkey.get_size() > ARRAYSIZEOF(HIDDEN_PK_COLUMN_IDS) || rowkey.get_size() <= 0 ||
      TPKM_OLD_NO_PK != table_mode_.pk_mode_) {
    //    nothing to do
    //    If the rowkey column is greater than 3 or there is no primary key, it is definitely not a hidden primary key
  } else {
    uint64_t column_id = OB_INVALID_ID;
    enum RowkeyEntry { A_ROWKEY_COLUMN = 0, ANOTHER_ROWKEY_COLUMN = 1 };
    // we only check one column
    if (OB_FAIL(rowkey.get_column_id(A_ROWKEY_COLUMN, column_id))) {
      LOG_WARN("fail to get column id", K(ret));
    } else if (OB_HIDDEN_SESSION_ID_COLUMN_ID == column_id &&
               OB_FAIL(rowkey.get_column_id(ANOTHER_ROWKEY_COLUMN, column_id))) {
      // SYS_SESSION_ID is a hidden rowkey in either no_pk temporary table or
      // has_pk temporary table
      LOG_WARN("fail to get column id", K(ret));
    } else if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id) {
      bret = true;
    }
  }
  return bret;
}

int ObTableSchema::get_column_ids(ObIArray<uint64_t>& column_ids) const
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  for (ObTableSchema::const_column_iterator iter = column_begin(); OB_SUCC(ret) && iter != column_end(); ++iter) {
    const ObColumnSchemaV2* column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("Fail to add column id to scan", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

/**
 * @brief Get all the column id of index column and rowkey column.
 * @param column_ids[out] output all column ids of index column and rokey column.
 */
int ObTableSchema::get_index_and_rowkey_column_ids(ObIArray<uint64_t>& column_ids) const
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
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTableSchema::has_column(const uint64_t column_id, bool& has) const
{
  int ret = OB_SUCCESS;
  bool contain = false;
  for (ObTableSchema::const_column_iterator iter = column_begin(); OB_SUCC(ret) && !contain && iter != column_end();
       ++iter) {
    const ObColumnSchemaV2* column_schema = *iter;
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

int ObTableSchema::has_lob_column(bool& has_lob, const bool check_large /*= false*/) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;

  has_lob = false;
  for (ObTableSchema::const_column_iterator iter = column_begin(); OB_SUCC(ret) && !has_lob && iter != column_end();
       ++iter) {
    if (OB_ISNULL(column_schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column schema is NULL", K(ret));
    } else if (ob_is_json_tc(column_schema->get_data_type())) {
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

// col id includes:
//  1. all rowkey
//  2. part key which is generate col
int ObTableSchema::get_column_ids_serialize_to_rowid(common::ObIArray<uint64_t>& col_ids, int64_t& rowkey_cnt) const
{
  int ret = OB_SUCCESS;
  col_ids.reset();
  rowkey_cnt = -1;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    const ObRowkeyInfo& rowkey_info = get_rowkey_info();
    OZ(rowkey_info.get_column_ids(col_ids));
    OX(rowkey_cnt = col_ids.count());

    if (OB_SUCC(ret) && has_generated_column()) {
      const ObPartitionKeyInfo& part_key_info = get_partition_key_info();
      const ObPartitionKeyInfo& subpart_key_info = get_subpartition_key_info();

      const ObColumnSchemaV2* col_schema = NULL;
      uint64_t col_id = OB_INVALID_ID;

      ObSEArray<const ObPartitionKeyInfo*, 2> tmp_key_infos;
      OZ(tmp_key_infos.push_back(&part_key_info));
      OZ(tmp_key_infos.push_back(&subpart_key_info));

      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_key_infos.count(); ++i) {
        const ObPartitionKeyInfo* info = tmp_key_infos.at(i);
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
        }  // end for
      }    // end for
    }
  }
  return ret;
}

int ObTableSchema::get_store_column_ids(common::ObIArray<ObColDesc>& column_ids) const
{
  int ret = OB_SUCCESS;
  bool no_virtual = true;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
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

int ObTableSchema::get_store_column_count(int64_t& column_count) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else if (is_storage_index_table()) {
    column_count = column_cnt_;
  } else {
    column_count = column_cnt_ - virtual_column_cnt_;
  }
  return ret;
}

int ObTableSchema::get_column_ids(common::ObIArray<ObColDesc>& column_ids, bool no_virtual) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    if (OB_FAIL(get_rowkey_column_ids(column_ids))) {  // add rowkey columns
      LOG_WARN("Fail to get rowkey column ids", K(ret));
    } else if (OB_FAIL(get_column_ids_without_rowkey(column_ids, no_virtual))) {  // add other columns
      LOG_WARN("Fail to get column ids with out rowkey", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::get_rowkey_column_ids(common::ObIArray<ObColDesc>& column_ids) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn* rowkey_column = NULL;
  ObColDesc col_desc;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    // add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      if (NULL == (rowkey_column = rowkey_info_.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else {
        col_desc.col_id_ = rowkey_column->column_id_;
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

int ObTableSchema::get_rowkey_column_ids(common::ObIArray<uint64_t>& column_ids) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyColumn* rowkey_column = NULL;
  ObColDesc col_desc;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    // add rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      if (NULL == (rowkey_column = rowkey_info_.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else if (OB_FAIL(column_ids.push_back(rowkey_column->column_id_))) {
        LOG_WARN("failed to push back rowkey column id", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTableSchema::get_column_ids_without_rowkey(common::ObIArray<ObColDesc>& column_ids, bool no_virtual) const
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  if (!is_valid()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("The ObTableSchema is invalid", K(ret));
  } else {
    // add other columns
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      const bool is_virtul_col =
          column_array_[i]->is_virtual_generated_column() || column_array_[i]->is_rowid_pseudo_column();
      if (NULL == column_array_[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is NULL, ", K(i));
      } else if (!column_array_[i]->is_rowkey_column() && !(no_virtual && is_virtul_col)) {
        col_desc.col_id_ = column_array_[i]->get_column_id();
        col_desc.col_type_ = column_array_[i]->get_meta_type();
        // for non-rowkey, col_desc.col_order_ is not meaningful
        if (OB_FAIL(column_ids.push_back(col_desc))) {
          LOG_WARN("Fail to add column id to column_ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableSchema::get_generated_column_ids(ObIArray<uint64_t>& column_ids) const
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

// Because there are too many places to call this function, you must be careful when modifying this function,
// and it is recommended not to modify
// If you must modify this function, pay attention to whether the location of calling this function also depends on the
// error code thrown by the function eg: ObSchemaMgr::get_index_name depends on the existing OB_SCHEMA_ERROR error code
// when calling get_index_name in get_index_schema If the newly added error code is still OB_SCHEMA_ERROR, it need
// consider whether the upper-level caller has handled the error code correctly
int ObSimpleTableSchemaV2::get_index_name(ObString& index_name) const
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

int ObSimpleTableSchemaV2::get_index_name(const ObString& table_name, ObString& index_name)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!table_name.prefix_match(OB_INDEX_PREFIX)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("index table name not in valid format", K(ret), K(table_name));
  } else {
    pos = strlen(OB_INDEX_PREFIX);

    while (NULL != table_name.ptr() && isdigit(*(table_name.ptr() + pos)) && pos < table_name.length()) {
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
        index_name.assign_ptr(table_name.ptr() + pos, table_name.length() - static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaV2::generate_origin_index_name()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_index_name(origin_index_name_))) {
    LOG_WARN("generate origin index name failed", K(ret), K(table_name_));
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_if_oracle_compat_mode(bool& is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  const uint64_t tid = get_table_id();
  is_oracle_mode = false;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  if (ObSchemaService::g_liboblog_mode_) {
    is_oracle_mode = false;
  } else if (is_inner_table(tid) && !is_ora_virtual_table(tid)) {
    is_oracle_mode = false;
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(get_tenant_id(), compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else if (ObWorker::CompatMode::ORACLE == compat_mode) {
    is_oracle_mode = true;
  } else if (ObWorker::CompatMode::MYSQL == compat_mode) {
    is_oracle_mode = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compat_mode should not be INVALID.", K(ret));
  }
  return ret;
}

int ObSimpleTableSchemaV2::check_is_duplicated(share::schema::ObSchemaGetterGuard& guard, bool& is_duplicated) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  bool is_restore = false;
  is_duplicated = false;
  if (OB_FAIL(guard.check_tenant_is_restore(tenant_id, is_restore))) {
    LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
  } else if (is_restore) {
    is_duplicated = false;
  } else if (ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER == get_duplicate_scope() && get_locality_str().empty()) {
    is_duplicated = true;
  }
  return ret;
}

int ObTableSchema::get_generated_column_by_define(
    const ObString& col_def, const bool only_hidden_column, ObColumnSchemaV2*& gen_col)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> gen_column_ids;
  if (OB_FAIL(get_generated_column_ids(gen_column_ids))) {
    LOG_WARN("get generated column ids failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && gen_col == NULL && i < gen_column_ids.count(); ++i) {
    ObColumnSchemaV2* tmp_col = get_column_schema(gen_column_ids.at(i));
    ObString tmp_def;
    if (OB_ISNULL(tmp_col)) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not found", K(ret), K(gen_column_ids.at(i)));
    } else if (!tmp_col->is_hidden() && only_hidden_column) {
      // do nothing, continue
    } else if (OB_FAIL(tmp_col->get_cur_default_value().get_string(tmp_def))) {
      LOG_WARN("get string of current default value failed", K(ret), K(tmp_col->get_cur_default_value()));
    } else if (ObCharset::case_insensitive_equal(tmp_def, col_def)) {
      gen_col = tmp_col;
    }
  }
  return ret;
}

int64_t ObTableSchema::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("simple_table_schema");
  J_COLON();
  pos += ObSimpleTableSchemaV2::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(max_used_column_id),
      K_(max_used_constraint_id),
      K_(sess_active_time),
      K_(rowkey_column_num),
      K_(index_column_num),
      K_(rowkey_split_pos),
      K_(block_size),
      K_(is_use_bloomfilter),
      K_(progressive_merge_num),
      K_(tablet_size),
      K_(pctfree),
      "load_type",
      static_cast<int32_t>(load_type_),
      "index_using_type",
      static_cast<int32_t>(index_using_type_),
      "def_type",
      static_cast<int32_t>(def_type_),
      "charset_type",
      static_cast<int32_t>(charset_type_),
      "collation_type",
      static_cast<int32_t>(collation_type_));
  J_COMMA();
  J_KV(K_(create_mem_version),
      "index_status",
      static_cast<int32_t>(index_status_),
      "partition_status",
      static_cast<int32_t>(partition_status_),
      K_(code_version),
      K_(last_modified_frozen_version),
      K_(comment),
      K_(pk_comment),
      K_(create_host),
      K_(tablegroup_name),
      K_(compress_func_name),
      K_(row_store_type),
      K_(store_format),
      K_(expire_info),
      K_(view_schema),
      K_(autoinc_column_id),
      K_(auto_increment),
      K_(read_only),
      K_(simple_index_infos),
      "mv_tid_array",
      ObArrayWrap<uint64_t>(mv_tid_array_, mv_cnt_),
      K_(base_table_ids),
      // K_(depend_table_ids),
      // K_(join_types),
      // K_(join_conds),
      K_(rowkey_info),
      K_(partition_key_info),
      K_(column_cnt),
      K_(table_dop),
      "column_array",
      ObArrayWrap<ObColumnSchemaV2*>(column_array_, column_cnt_));
  J_OBJ_END();

  return pos;
}

int ObTableSchema::fill_column_collation_info()
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* it = NULL;
  int64_t column_count = get_column_count();
  for (int64_t i = 0; i < column_count; i++) {
    it = const_cast<ObTableSchema*>(this)->get_column_schema_by_idx(i);
    if (it->get_data_type() == ObVarcharType || it->get_data_type() == ObCharType) {
      ObCharsetType charset_type = it->get_charset_type();
      ObCollationType collation_type = it->get_collation_type();
      if (it->get_collation_type() == CS_TYPE_INVALID && it->get_charset_type() == CHARSET_INVALID) {
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

  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      database_id_,
      tablegroup_id_,
      table_id_,
      max_used_column_id_,
      rowkey_column_num_,
      index_column_num_,
      rowkey_split_pos_,
      part_key_column_num_,
      block_size_,
      is_use_bloomfilter_,
      progressive_merge_num_,
      replica_num_,
      autoinc_column_id_,
      auto_increment_,
      read_only_,
      load_type_,
      table_type_,
      index_type_,
      def_type_,
      charset_type_,
      collation_type_,
      data_table_id_,
      create_mem_version_,
      index_status_,
      name_case_mode_,
      code_version_,
      schema_version_,
      last_modified_frozen_version_,
      first_timestamp_index_,
      part_level_,
      part_option_,
      sub_part_option_,
      tablegroup_name_,
      comment_,
      table_name_,
      compress_func_name_,
      expire_info_,
      view_schema_,
      primary_zone_,
      index_using_type_,
      progressive_merge_round_,
      storage_format_version_);

  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to serialize fixed length data", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, zone_list_))) {
    LOG_WARN("serialize_string_array failed", K(ret));
  } else {
    // serialize index table id, for compatibility use only, index tid array has obsoleted
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, simple_index_infos_.count()))) {
        LOG_WARN("Fail to encode index table count", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
        if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, simple_index_infos_.at(i).table_id_))) {
          LOG_WARN("Fail to encode index tid, ", K(i), K(ret));
        }
      }
    }

    // serialize columns and partitions
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_columns(buf, buf_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "failed to serialize columns, ", K_(column_cnt));
      }
    }
    if (OB_SUCC(ret)) {
      if (PARTITION_LEVEL_TWO == part_level_ || part_option_.is_range_part() || part_option_.is_list_part()) {
        if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, subpart_key_column_num_))) {
          LOG_WARN("Fail to encode partition count", K(ret));
        } else if (OB_FAIL(serialize_partitions(buf, buf_len, pos))) {
          LOG_WARN("failed to serialize partitions", K(ret));
        } else {
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (PARTITION_LEVEL_TWO == part_level_) {
        if (OB_FAIL(serialize_def_subpartitions(buf, buf_len, pos))) {
          LOG_WARN("failed to serialize def subpartitions", K(ret));
        }
      }
    }
    OB_UNIS_ENCODE(index_attributes_set_);
    OB_UNIS_ENCODE(parser_name_);
    OB_UNIS_ENCODE(locality_str_);
  }

  LST_DO_CODE(OB_UNIS_ENCODE, depend_table_ids_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, mv_cnt_))) {
      LOG_WARN("Fail to encode mv table count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_cnt_; ++i) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, mv_tid_array_[i]))) {
        LOG_WARN("Fail to encode mv tid, ", K(i), K(ret));
      }
    }
  }

  LST_DO_CODE(OB_UNIS_ENCODE,
      tablet_size_,
      pctfree_,
      previous_locality_str_,
      foreign_key_infos_,
      min_partition_id_,
      partition_status_,
      partition_schema_version_,
      max_used_constraint_id_,
      session_id_,
      sess_active_time_);

  // serialize constraints and partitions
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_constraints(buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "failed to serialize constraints, ", K_(cst_cnt));
    }
  }

  LST_DO_CODE(OB_UNIS_ENCODE, pk_comment_, create_host_);
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE == part_level_ && part_option_.is_hash_like_part()) {
      if (OB_FAIL(serialize_partitions(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize partitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, row_store_type_, store_format_);
    LST_DO_CODE(OB_UNIS_ENCODE, duplicate_scope_);
    LST_DO_CODE(OB_UNIS_ENCODE, binding_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_mode_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize dropped table_mode", K(ret));
    } else {
      LST_DO_CODE(OB_UNIS_ENCODE, encryption_);
      LST_DO_CODE(OB_UNIS_ENCODE, tablespace_id_);
      LST_DO_CODE(OB_UNIS_ENCODE, encrypt_key_);
      LST_DO_CODE(OB_UNIS_ENCODE, master_key_id_);
      LST_DO_CODE(OB_UNIS_ENCODE, drop_schema_version_);
    }
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(serialize_dropped_partitions(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize dropped partitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, simple_index_infos_, is_sub_part_template_);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, table_dop_);
  }
  return ret;
}

int ObTableSchema::serialize_columns(char* buf, const int64_t data_len, int64_t& pos) const
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

int ObTableSchema::deserialize_columns(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column;
  // the column_cnt_ will be increased in the add_column
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to decode column count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      column.reset();
      if (OB_FAIL(column.deserialize(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to deserialize column", K(ret));
      } else if (OB_FAIL(add_column(column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::serialize_constraints(char* buf, const int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, cst_cnt_))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to encode cst count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cst_cnt_; ++i) {
    if (OB_FAIL(cst_array_[i]->serialize(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to serialize cst", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::deserialize_constraints(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObConstraint cst;
  // the cst_cnt_ will be increased in the add_cst
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to decode cst count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      cst.reset();
      if (OB_FAIL(cst.deserialize(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to deserialize cst", K(ret));
      } else if (OB_FAIL(add_constraint(cst))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add cst", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableSchema)
{
  int ret = OB_SUCCESS;
  reset();
  ObString tablegroup_name;
  ObString comment;
  ObString pk_comment;
  ObString create_host;
  ObString table_name;
  ObString compress_func_name;
  ObString expire_info;
  ObString primary_zone;
  int64_t index_cnt;

  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      database_id_,
      tablegroup_id_,
      table_id_,
      max_used_column_id_,
      rowkey_column_num_,
      index_column_num_,
      rowkey_split_pos_,
      part_key_column_num_,
      block_size_,
      is_use_bloomfilter_,
      progressive_merge_num_,
      replica_num_,
      autoinc_column_id_,
      auto_increment_,
      read_only_,
      load_type_,
      table_type_,
      index_type_,
      def_type_,
      charset_type_,
      collation_type_,
      data_table_id_,
      create_mem_version_,
      index_status_,
      name_case_mode_,
      code_version_,
      schema_version_,
      last_modified_frozen_version_,
      first_timestamp_index_,
      part_level_,
      part_option_,
      sub_part_option_,
      tablegroup_name,
      comment,
      table_name,
      compress_func_name,
      expire_info,
      view_schema_,
      primary_zone,
      index_using_type_,
      progressive_merge_round_,
      storage_format_version_);

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize fixed length data", K(ret));
  } else if (OB_FAIL(deep_copy_str(tablegroup_name, tablegroup_name_))) {
    LOG_WARN("deep_copy_str failed", K(ret));
  } else if (OB_FAIL(deep_copy_str(comment, comment_))) {
    LOG_WARN("deep_copy_str failed", K(ret));
  } else if (OB_FAIL(deep_copy_str(table_name, table_name_))) {
    LOG_WARN("deep_copy_str failed", K(ret));
  } else if (OB_FAIL(deep_copy_str(compress_func_name, compress_func_name_))) {
    LOG_WARN("deep_copy_str failed", K(ret));
  } else if (OB_FAIL(deep_copy_str(expire_info, expire_info_))) {
    LOG_WARN("deep_copy_str failed", K(ret));
  } else if (OB_FAIL(deep_copy_str(primary_zone, primary_zone_))) {
    LOG_WARN("deep_copy_str failed", K(ret));
  } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, zone_list_))) {
    LOG_WARN("deserialize_string_array failed", K(ret));
  }

  // deserialize index table ids, for compatibility use only, index tid array has obsoleted
  if (OB_SUCC(ret) && pos < data_len) {
    int64_t index_tid = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &index_cnt))) {
      LOG_WARN("Fail to decode index table count", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
        index_tid = 0;
        if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &index_tid))) {
          LOG_WARN("Fail to deserialize index tid", K(ret));
        }
      }
    }

    // deserialize columns and partitions
    if (OB_SUCC(ret)) {
      if (OB_FAIL(deserialize_columns(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "failed to deserialize columns, ", K_(column_cnt));
      }
    }
    if (OB_SUCC(ret) && pos < data_len) {
      if (PARTITION_LEVEL_TWO == part_level_ || part_option_.is_range_part() || part_option_.is_list_part()) {
        if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &subpart_key_column_num_))) {
          LOG_WARN("Fail to encode partition count", K(ret));
        } else if (OB_FAIL(deserialize_partitions(buf, data_len, pos))) {
          LOG_WARN("failed to deserialize partitions", K(ret));
        } else {
        }  // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      if (PARTITION_LEVEL_TWO == part_level_) {
        if (OB_FAIL(deserialize_def_subpartitions(buf, data_len, pos))) {
          LOG_WARN("failed to deserialize def subpartitions", K(ret));
        }
      }
    }
    OB_UNIS_DECODE(index_attributes_set_);
    OB_UNIS_DECODE(parser_name_);
    OB_UNIS_DECODE(locality_str_);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_locality(locality_str_))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set locality", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
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
    }
    if (OB_SUCC(ret)) {
      if (primary_zone_.length() > 0 && zone_list_.count() > 0) {
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
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, depend_table_ids_);

    // deserialize mv table ids
    int64_t count = 0;
    OB_UNIS_DECODE(count);
    if (count > 0) {
      int64_t mv_tid = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        mv_tid = 0;
        if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &mv_tid))) {
          LOG_WARN("Fail to deserialize mv tid", K(ret));
        } else if (OB_FAIL(add_mv_tid(mv_tid))) {
          LOG_WARN("Fail to add mv tid", K(ret));
        }
      }
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, tablet_size_, pctfree_);
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(previous_locality_str_);
    if (OB_FAIL(set_previous_locality(previous_locality_str_))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set locality", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, foreign_key_infos_);
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
        min_partition_id_,
        partition_status_,
        partition_schema_version_,
        max_used_constraint_id_,
        session_id_,
        sess_active_time_);
  }

  // deserialize constraints and partitions
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deserialize_constraints(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deserialize constraints, ", K_(cst_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, pk_comment, create_host);
    if (OB_FAIL(deep_copy_str(pk_comment, pk_comment_))) {
      LOG_WARN("deep_copy_str failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(create_host, create_host_))) {
      LOG_WARN("deep_copy_str failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE == part_level_ && part_option_.is_hash_like_part()) {
      if (OB_FAIL(deserialize_partitions(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize partitions", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, row_store_type_, store_format_);
    LST_DO_CODE(OB_UNIS_DECODE, duplicate_scope_);
    LST_DO_CODE(OB_UNIS_DECODE, binding_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_mode_.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to decode table mode", K(ret));
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, encryption_);
      if (OB_FAIL(set_encryption_str(encryption_))) {
        LOG_WARN("fail to set encryption str", K(ret));
      } else {
        LST_DO_CODE(OB_UNIS_DECODE, tablespace_id_);
        LST_DO_CODE(OB_UNIS_DECODE, encrypt_key_);
        if (OB_FAIL(set_encrypt_key(encrypt_key_))) {
          LOG_WARN("fail to set encrypt key", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, master_key_id_);
    LST_DO_CODE(OB_UNIS_DECODE, drop_schema_version_);
  }
  // dropped partitions
  if (OB_SUCC(ret)) {
    if (PARTITION_LEVEL_ONE <= part_level_) {
      if (OB_FAIL(deserialize_partitions(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize dropped partitions", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, simple_index_infos_, is_sub_part_template_);
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, table_dop_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      database_id_,
      tablegroup_id_,
      table_id_,
      max_used_column_id_,
      session_id_,
      sess_active_time_,
      max_used_constraint_id_,
      rowkey_column_num_,
      index_column_num_,
      rowkey_split_pos_,
      part_key_column_num_,
      block_size_,
      is_use_bloomfilter_,
      progressive_merge_num_,
      replica_num_,
      tablet_size_,
      pctfree_,
      autoinc_column_id_,
      auto_increment_,
      read_only_,
      load_type_,
      table_type_,
      index_type_,
      def_type_,
      charset_type_,
      collation_type_,
      data_table_id_,
      create_mem_version_,
      index_status_,
      name_case_mode_,
      code_version_,
      schema_version_,
      last_modified_frozen_version_,
      first_timestamp_index_,
      part_level_,
      part_option_,
      sub_part_option_,
      tablegroup_name_,
      comment_,
      table_name_,
      compress_func_name_,
      expire_info_,
      view_schema_,
      primary_zone_,
      index_using_type_,
      min_partition_id_,
      partition_status_,
      partition_schema_version_,
      pk_comment_,
      create_host_,
      progressive_merge_round_,
      storage_format_version_,
      tablespace_id_,
      encrypt_key_,
      master_key_id_,
      drop_schema_version_);

  len += table_mode_.get_serialize_size();
  len += get_string_array_serialize_size(zone_list_);

  // get index tid size
  len += serialization::encoded_length_vi64(simple_index_infos_.count());
  for (int64_t i = 0; i < simple_index_infos_.count(); ++i) {
    len += serialization::encoded_length_vi64(simple_index_infos_.at(i).table_id_);
  }

  // get columms size
  len += serialization::encoded_length_vi64(column_cnt_);
  for (int64_t i = 0; i < column_cnt_; ++i) {
    if (NULL != column_array_[i]) {
      len += column_array_[i]->get_serialize_size();
    }
  }
  // get constraints size
  len += serialization::encoded_length_vi64(cst_cnt_);
  for (int64_t i = 0; i < cst_cnt_; ++i) {
    if (NULL != cst_array_[i]) {
      len += cst_array_[i]->get_serialize_size();
    }
  }
  if (PARTITION_LEVEL_TWO == part_level_ || part_option_.is_list_part() || part_option_.is_range_part()) {
    // get partitions size
    len += serialization::encoded_length_vi64(subpart_key_column_num_);
    len += serialization::encoded_length_vi64(partition_num_);
    for (int64_t i = 0; i < partition_num_; i++) {
      if (NULL != partition_array_[i]) {
        len += partition_array_[i]->get_serialize_size();
      }
    }
  }
  if (PARTITION_LEVEL_TWO == part_level_) {
    // get def subpartitions size
    len += serialization::encoded_length_vi64(def_subpartition_num_);
    for (int64_t i = 0; i < def_subpartition_num_; i++) {
      if (NULL != def_subpartition_array_[i]) {
        len += def_subpartition_array_[i]->get_serialize_size();
      }
    }
  }
  OB_UNIS_ADD_LEN(index_attributes_set_);
  OB_UNIS_ADD_LEN(parser_name_);
  OB_UNIS_ADD_LEN(locality_str_);

  len += serialization::encoded_length_vi64(mv_cnt_);
  for (int64_t i = 0; i < mv_cnt_; ++i) {
    len += serialization::encoded_length_vi64(mv_tid_array_[i]);
  }

  len += depend_table_ids_.get_serialize_size();
  OB_UNIS_ADD_LEN(previous_locality_str_);
  len += foreign_key_infos_.get_serialize_size();
  if (PARTITION_LEVEL_ONE == part_level_ && part_option_.is_hash_like_part()) {
    len += serialization::encoded_length_vi64(partition_num_);
    for (int64_t i = 0; i < partition_num_; i++) {
      if (NULL != partition_array_[i]) {
        len += partition_array_[i]->get_serialize_size();
      }
    }
  }
  OB_UNIS_ADD_LEN(row_store_type_);
  OB_UNIS_ADD_LEN(store_format_);
  OB_UNIS_ADD_LEN(duplicate_scope_);

  OB_UNIS_ADD_LEN(binding_);
  OB_UNIS_ADD_LEN(encryption_);
  if (PARTITION_LEVEL_ONE <= part_level_) {
    len += serialization::encoded_length_vi64(dropped_partition_num_);
    for (int64_t i = 0; i < dropped_partition_num_; i++) {
      if (NULL != dropped_partition_array_[i]) {
        len += dropped_partition_array_[i]->get_serialize_size();
      }
    }
  }
  len += simple_index_infos_.get_serialize_size();
  OB_UNIS_ADD_LEN(is_sub_part_template_);

  OB_UNIS_ADD_LEN(table_dop_);
  return len;
}

int ObTableSchema::check_primary_key_cover_partition_column()
{
  int ret = OB_SUCCESS;
  if (!is_partitioned_table() || is_no_pk_table()) {
    // nothing todo
  } else if (OB_FAIL(check_rowkey_cover_partition_keys(partition_key_info_))) {
    LOG_WARN("Check rowkey cover partition key failed", K(ret));
  } else if (OB_FAIL(check_rowkey_cover_partition_keys(subpartition_key_info_))) {
    LOG_WARN("Check rowkey cover subpartiton key failed", K(ret));
  }

  return ret;
}

int ObTableSchema::check_rowkey_cover_partition_keys(const ObPartitionKeyInfo& part_key_info)
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
      const ObColumnSchemaV2* column_schema = NULL;
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
            } else {
            }  // do nothing
          }
        }
      } else {
        ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
        LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "PRIMARY KEY");
      }
    } else {
    }  // do nothing
  }
  return ret;
}
// No primary key table Check whether the newly added index table meets the requirements
// The index table must contain all partition keys
int ObTableSchema::check_create_index_on_hidden_primary_key(const ObTableSchema& index_table) const
{
  int ret = OB_SUCCESS;
  if (!index_table.is_index_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_table));
  } else if (!is_partitioned_table() || !is_no_pk_table()) {
    // If it is not a non-partitioned table, or is not a hidden primary key, there is no need to check
  } else if (OB_FAIL(index_table.check_index_table_cover_partition_keys(partition_key_info_))) {
    LOG_WARN("failed to check index table cover partition key", K(ret), K_(partition_key_info));
  }
  return ret;
}
int ObTableSchema::check_index_table_cover_partition_keys(const common::ObPartitionKeyInfo& part_key) const
{
  int ret = OB_SUCCESS;
  if (!is_index_table() || !part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), "index_table", *this);
  }
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2* column_schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key.get_size(); ++i) {
    if (OB_FAIL(part_key.get_column_id(i, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (OB_ISNULL(column_schema = get_column_schema(column_id))) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("can not find partition key in index", K(ret), K(column_id), K(part_key), "index_schema", *this);
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition key not in index table");
    } else {
    }  // do nothing
  }
  return ret;
}
int ObTableSchema::check_auto_partition_valid()
{
  int ret = OB_SUCCESS;
  if (!is_auto_partitioned_table()) {
  } else if (is_old_no_pk_table()) {
    // Hide the primary key table, you cannot specify the partition key
    if (0 < partition_key_info_.get_size()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("hidden table can not has partition key", KR(ret), K_(partition_key_info));
    }
  } else {
    bool is_prefix = false;
    if (0 >= partition_key_info_.get_size()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("none of partition key with primary key", KR(ret));
    } else if (OB_FAIL(is_partition_key_match_rowkey_prefix(is_prefix))) {
      LOG_WARN("failed to check is prefix", KR(ret));
    } else if (!is_prefix) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("partition key not prefix of rowkey", KR(ret));
    }
  }
  return ret;
}

/////////partition functions//////////////
int ObTableSchema::get_part(
    const common::ObNewRange& range, const bool reverse, common::ObIArray<int64_t>& part_ids) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    if (OB_FAIL(part_ids.push_back(0))) {
      LOG_WARN("Failed to push back part_idx 0", K(ret));
    }
  } else {
    if (is_range_part()) {
      if (OB_FAIL(ObPartitionUtils::get_range_part_ids(range, reverse, partition_array_, partition_num_, part_ids))) {
        LOG_WARN("Failed to get range part idxs", K(ret));
      }
    } else if (is_list_part()) {
      if (OB_FAIL(ObPartitionUtils::get_list_part_ids(range, reverse, partition_array_, partition_num_, part_ids))) {
        LOG_WARN("Failed to get list part idxs", K(ret));
      }
    } else {
      if (OB_FAIL(ObPartitionUtils::get_hash_part_ids(*this, range, get_first_part_num(), part_ids))) {
        LOG_WARN("Failed to get hash part", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_part(const common::ObNewRow& row, common::ObIArray<int64_t>& part_ids) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == get_part_level()) {
    if (OB_FAIL(part_ids.push_back(0))) {
      LOG_WARN("Failed to push back part_idx 0", K(ret));
    }
  } else {
    if (is_range_part()) {
      if (OB_FAIL(ObPartitionUtils::get_range_part_ids(row, partition_array_, partition_num_, part_ids))) {
        LOG_WARN("Failed to get range part idxs", K(ret));
      }
    } else if (is_list_part()) {
      if (OB_FAIL(ObPartitionUtils::get_list_part_ids(row, partition_array_, partition_num_, part_ids))) {
        LOG_WARN("Failed to get list part idxs", K(ret));
      }
    } else {
      if (OB_FAIL(ObPartitionUtils::get_hash_part_ids(*this, row, get_first_part_num(), part_ids))) {
        LOG_WARN("Failed to get hash part", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_subpart(
    const int64_t actual_part_id, const ObNewRange& range, const bool reverse, ObIArray<int64_t>& subpart_ids) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == get_part_level() || PARTITION_LEVEL_ONE == get_part_level()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("This is for subpartition", K(ret));
  } else {
    int64_t part_id = is_sub_part_template() ? ObSubPartition::TEMPLATE_PART_ID : actual_part_id;
    ObSubPartition** subpart_array = NULL;
    int64_t subpart_num = 0;
    int64_t subpartition_num = 0;
    if (OB_FAIL(get_subpart_info(actual_part_id, subpart_array, subpart_num, subpartition_num))) {
      LOG_WARN("fail to get subpart info", K(ret), K(actual_part_id));
    } else if (is_range_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_range_part_ids(
              part_id, range, reverse, subpart_array, subpartition_num, subpart_ids))) {
        LOG_WARN("Failed to get range part idxs", K(ret), K(def_subpartition_num_));
      }
    } else if (is_list_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_list_part_ids(
              part_id, range, reverse, subpart_array, subpartition_num, subpart_ids))) {
        LOG_WARN("Failed to get range part idxs", K(ret), K(def_subpartition_num_));
      }
    } else {
      if (OB_FAIL(ObPartitionUtils::get_hash_subpart_ids(*this, part_id, range, subpart_num, subpart_ids))) {
        LOG_WARN("Failed to get hash part", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_subpart(const int64_t actual_part_id, const ObNewRow& row, ObIArray<int64_t>& subpart_ids) const
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == get_part_level() || PARTITION_LEVEL_ONE == get_part_level()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("This is for subpartition", K(ret));
  } else {
    int64_t part_id = is_sub_part_template() ? ObSubPartition::TEMPLATE_PART_ID : actual_part_id;
    ObSubPartition** subpart_array = NULL;
    int64_t subpart_num = 0;
    int64_t subpartition_num = 0;
    if (OB_FAIL(get_subpart_info(actual_part_id, subpart_array, subpart_num, subpartition_num))) {
      LOG_WARN("fail to get subpart info", K(ret), K(actual_part_id));
    } else if (is_range_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_range_part_ids(part_id, row, subpart_array, subpartition_num, subpart_ids))) {
        LOG_WARN("Failed to get range part idxs", K(ret), K(def_subpartition_num_));
      }
    } else if (is_list_subpart()) {
      if (OB_FAIL(ObPartitionUtils::get_list_part_ids(part_id, row, subpart_array, subpartition_num, subpart_ids))) {
        LOG_WARN("Failed to get list part idxs", K(ret), K(def_subpartition_num_));
      }
    } else {
      if (OB_FAIL(ObPartitionUtils::get_hash_subpart_ids(*this, part_id, row, subpart_num, subpart_ids))) {
        LOG_WARN("Failed to get hash part", K(ret));
      }
    }
  }
  return ret;
}

int ObTableSchema::get_part_shuffle_key(const int64_t part_id, common::ObObj& part_key1, common::ObObj& part_key2) const
{
  int ret = OB_SUCCESS;
  int64_t part_num = get_first_part_num();
  int64_t part_offset = OB_INVALID_INDEX_INT64;
  if (is_range_part()) {
    if (OB_FAIL(get_part_offset(part_id, part_offset))) {
      LOG_WARN("Failed to get part offset");
    } else if (OB_INVALID_INDEX_INT64 == part_offset) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(part_num));
    } else if (OB_FAIL(ObPartitionUtils::get_range_part_bounds(
                   partition_array_, part_num, part_offset, part_key1, part_key2))) {
      LOG_WARN("fail to get range part bound", K(part_num), K(part_id), K(ret));
    }
    //  The shuffle key is mainly used for dynamic partition cutting,
    //  The list partition should be handled in the same way as the hash partition.
  } else {
    if (OB_UNLIKELY(!(0 <= part_id && part_id < part_num))) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("part idx is out of range", K(part_id), K(part_num));
    } else {
      part_key1.set_int(part_id);
      part_key2.set_int(part_num);
    }
  }
  return ret;
}

// FIXME:()
// This interface should not support the list partition. The non-templated subpartition of the first phase
// can still guarantee the same structure, and it is equivalently modified here.
int ObTableSchema::get_subpart_shuffle_key(
    const int64_t part_id, const int64_t subpart_id, common::ObObj& subpart_key1, common::ObObj& subpart_key2) const
{
  int ret = OB_SUCCESS;
  int64_t def_subpart_num = 0;
  int64_t subpart_offset = OB_INVALID_INDEX_INT64;
  if (OB_FAIL(get_first_individual_sub_part_num(def_subpart_num))) {
    LOG_WARN("fail to get def subpart num", K(ret));
  } else if (is_range_subpart()) {
    ObSubPartition** subpart_array = NULL;
    int64_t subpart_num = 0;
    int64_t subpartition_num = 0;
    if (OB_FAIL(get_subpart_info(part_id, subpart_array, subpart_num, subpartition_num))) {
      LOG_WARN("fail to get subpart info", K(ret), K(part_id));
    } else if (OB_FAIL(get_sub_part_offset(part_id, subpart_id, subpart_offset))) {
      LOG_WARN("Failed to get sub partition offset", K(ret));
    } else if (OB_INVALID_INDEX_INT64 == subpart_offset) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition is null", K(ret));
    } else if (OB_FAIL(ObPartitionUtils::get_range_part_bounds(
                   subpart_array, subpart_num, subpart_offset, subpart_key1, subpart_key2))) {
      LOG_WARN("fail to get range part bound", K(def_subpart_num), K(subpart_id), K(subpart_offset), K(ret));
    }
    //  The shuffle key is mainly used for dynamic partition cutting,
    //  The list partition should be handled in the same way as the hash partition.
  } else {
    if (OB_UNLIKELY(!(0 <= subpart_id && subpart_id < def_subpart_num))) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("subpart id is out of range", K(subpart_id), K(def_subpart_num));
    } else {
      subpart_key1.set_int(subpart_id);
      subpart_key2.set_int(def_subpart_num);
    }
  }
  return ret;
}

int ObTableSchema::assign_tablegroup_partition(const ObTablegroupSchema& tablegroup)
{
  int ret = OB_SUCCESS;
  reset_partition_schema();
  part_num_ = tablegroup.get_part_option().get_part_num();
  def_subpart_num_ = tablegroup.get_sub_part_option().get_part_num();
  part_level_ = tablegroup.get_part_level();
  is_sub_part_template_ = tablegroup.is_sub_part_template();

  if (OB_SUCC(ret)) {
    part_option_ = tablegroup.get_part_option();
    if (OB_FAIL(part_option_.get_err_ret())) {
      LOG_WARN("fail to assign part_option", K(ret), K_(part_option), K(tablegroup.get_part_option()));
    }
  }

  if (OB_SUCC(ret)) {
    sub_part_option_ = tablegroup.get_sub_part_option();
    if (OB_FAIL(sub_part_option_.get_err_ret())) {
      LOG_WARN("fail to assign sub_part_option", K(ret), K_(sub_part_option), K(tablegroup.get_sub_part_option()));
    }
  }

  // partitions array
  if (OB_SUCC(ret)) {
    int64_t partition_num = tablegroup.get_partition_num();
    if (partition_num > 0) {
      partition_array_ = static_cast<ObPartition**>(alloc(sizeof(ObPartition*) * partition_num));
      if (OB_ISNULL(partition_array_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for partition_array_", K(ret));
      } else if (OB_ISNULL(tablegroup.get_part_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup.partition_array_ is null", K(ret));
      } else {
        partition_array_capacity_ = partition_num;
      }
    }
    ObPartition* partition = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      partition = tablegroup.get_part_array()[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition is null", K(ret));
      } else if (OB_FAIL(add_partition(*partition))) {
        LOG_WARN("Fail to add partition", K(ret));
      }
    }
  }
  // subpartitions array
  if (OB_SUCC(ret)) {
    int64_t def_subpartition_num = tablegroup.get_def_subpartition_num();
    if (def_subpartition_num > 0) {
      def_subpartition_array_ = static_cast<ObSubPartition**>(alloc(sizeof(ObSubPartition*) * def_subpartition_num));
      if (OB_ISNULL(def_subpartition_array_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for subpartition_array_", K(ret), K(def_subpartition_num));
      } else if (OB_ISNULL(tablegroup.get_def_subpart_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup.def_subpartition_array_ is null", K(ret));
      } else {
        def_subpartition_array_capacity_ = def_subpartition_num;
      }
    }
    ObSubPartition* subpartition = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < def_subpartition_num; i++) {
      subpartition = tablegroup.get_def_subpart_array()[i];
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

// Distinguish the following two scenarios:
// 1. For non-partitioned tables, return 0 directly;
// 2. For a partitioned table, and the first-level partition mode is key(), take the number of primary key columns;
//  otherwise, calculate the number of expression vectors
int ObTableSchema::calc_part_func_expr_num(int64_t& part_func_expr_num) const
{
  int ret = OB_SUCCESS;
  part_func_expr_num = OB_INVALID_INDEX;
  if (PARTITION_LEVEL_MAX == part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K_(part_level));
  } else if (PARTITION_LEVEL_ZERO == part_level_) {
    part_func_expr_num = 0;
  } else {
    ObArray<ObString> sub_columns;
    ObString table_func_expr_str = get_part_option().get_part_func_expr_str();
    if (table_func_expr_str.empty()) {
      if (is_key_part()) {
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

// Distinguish the following two scenarios:
// 1. For non-partitioned tables, return 0 directly;
// 2. For the second-level partition table, and the second-level partition mode is key(), take the number of primary key
// columns;
//  otherwise, calculate the number of expression vectors
int ObTableSchema::calc_subpart_func_expr_num(int64_t& subpart_func_expr_num) const
{
  int ret = OB_SUCCESS;
  subpart_func_expr_num = OB_INVALID_INDEX;
  if (PARTITION_LEVEL_MAX == part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part level", K(ret), K_(part_level));
  } else if (PARTITION_LEVEL_ZERO == part_level_ || PARTITION_LEVEL_ONE == part_level_) {
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

// FIXME: Isomorphic assumption, interface to be removed
int ObTableSchema::get_all_part_ids_by_level_one_partition_values(
    const common::ObNewRow& part_row, common::ObIArray<int64_t>& part_ids, common::ObIArray<int64_t>& subpart_ids) const
{
  int ret = OB_SUCCESS;
  part_ids.reset();
  subpart_ids.reset();
  if (OB_ISNULL(def_subpartition_array_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Sub partition array should not be NULL", K(ret));
  } else if (OB_FAIL(get_part(part_row, part_ids))) {
    LOG_WARN("Get part idxs from table schema error", K(ret));
  } else if (OB_FAIL(get_subpart_ids(ObSubPartition::TEMPLATE_PART_ID, subpart_ids))) {
    LOG_WARN("Fail to get all part ids", K(ret));
  }
  return ret;
}

int ObTableSchema::get_all_part_ids_by_level_two_partition_values(const common::ObNewRow& subpart_row,
    common::ObIArray<int64_t>& part_ids, common::ObIArray<int64_t>& subpart_ids) const
{
  int ret = OB_SUCCESS;
  int64_t the_first_one_partition_id = 0;
  part_ids.reset();
  subpart_ids.reset();
  if (OB_ISNULL(partition_array_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Partition array should not be NULL", K(ret));
  } else if (OB_ISNULL(partition_array_[0])) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("Partition array at idx 0 should not be NULL", K(ret));
  } else if (FALSE_IT(the_first_one_partition_id = partition_array_[0]->get_part_id())) {
  } else if (OB_FAIL(get_subpart(the_first_one_partition_id, subpart_row, subpart_ids))) {
    LOG_WARN("Get part idxs from table schema error", K(the_first_one_partition_id), K(ret));
  } else if (OB_FAIL(get_all_part_ids(part_ids))) {
    LOG_WARN("Fail to get all part ids", K(ret));
  }
  return ret;
}

int ObTableSchema::get_all_part_ids(common::ObIArray<int64_t>& part_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < partition_num_ && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(partition_array_[i])) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("Do not access the null partition object", K(ret), K(i), K(partition_num_));
    } else if (OB_FAIL(part_ids.push_back(partition_array_[i]->get_part_id()))) {
      LOG_WARN("Push back failed", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::get_subpart_ids(const int64_t part_id, common::ObIArray<int64_t>& subpart_ids) const
{
  int ret = OB_SUCCESS;
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
    } else if (OB_FAIL(subpart_ids.push_back(subpart_array[i]->get_sub_part_id()))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

/////End partition functions////////////
DEFINE_SERIALIZE(ObColDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(col_id_);
  OB_UNIS_ENCODE(col_type_);
  OB_UNIS_ENCODE(col_order_);
  return ret;
}

DEFINE_DESERIALIZE(ObColDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(col_id_);
  OB_UNIS_DECODE(col_type_);
  OB_UNIS_DECODE(col_order_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObColDesc)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(col_id_);
  OB_UNIS_ADD_LEN(col_type_);
  OB_UNIS_ADD_LEN(col_order_);
  return len;
}

ObConstraint* ObTableSchema::get_constraint_internal(std::function<bool(const ObConstraint* val)> func)
{
  ObConstraint* cst_ret = NULL;

  if (cst_array_ != NULL && cst_cnt_ > 0) {
    ObConstraint** end = cst_array_ + cst_cnt_;
    ObConstraint** cst = std::find_if(cst_array_, end, func);
    if (cst != end) {
      cst_ret = *cst;
    }
  }

  return cst_ret;
}

const ObConstraint* ObTableSchema::get_constraint_internal(std::function<bool(const ObConstraint* val)> func) const
{
  ObConstraint* cst_ret = NULL;

  if (cst_array_ != NULL && cst_cnt_ > 0) {
    ObConstraint** end = cst_array_ + cst_cnt_;
    ObConstraint** cst = std::find_if(cst_array_, end, func);
    if (cst != end) {
      cst_ret = *cst;
    }
  }

  return cst_ret;
}

int ObTableSchema::get_pk_constraint_name(ObString& pk_name) const
{
  int ret = OB_SUCCESS;
  ObConstraintType cst_type = CONSTRAINT_TYPE_PRIMARY_KEY;

  const ObConstraint* cst =
      get_constraint_internal([cst_type](const ObConstraint* val) { return val->get_constraint_type() == cst_type; });
  if (OB_NOT_NULL(cst)) {
    pk_name.assign_ptr(cst->get_constraint_name_str().ptr(), cst->get_constraint_name_str().length());
  }

  return ret;
}

ObConstraint* ObTableSchema::get_constraint(const uint64_t constraint_id)
{
  return get_constraint_internal(
      [constraint_id](const ObConstraint* val) { return val->get_constraint_id() == constraint_id; });
}

ObConstraint* ObTableSchema::get_constraint(const ObString& constraint_name)
{
  return get_constraint_internal(
      [constraint_name](const ObConstraint* val) { return val->get_constraint_name_str() == constraint_name; });
}

int ObTableSchema::add_cst_to_cst_array(ObConstraint* cst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The cst is NULL", K(ret));
  } else {
    if (0 == cst_array_capacity_) {
      if (NULL == (cst_array_ = static_cast<ObConstraint**>(alloc(sizeof(ObConstraint*) * DEFAULT_ARRAY_CAPACITY)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for cst_array_.");
      } else {
        cst_array_capacity_ = DEFAULT_ARRAY_CAPACITY;
        MEMSET(cst_array_, 0, sizeof(ObConstraint*) * DEFAULT_ARRAY_CAPACITY);
      }
    } else if (cst_cnt_ >= cst_array_capacity_) {
      int64_t tmp_size = 2 * cst_array_capacity_;
      ObConstraint** tmp = NULL;
      if (NULL == (tmp = static_cast<ObConstraint**>(alloc(sizeof(ObConstraint*) * tmp_size)))) {
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

int ObTableSchema::remove_cst_from_cst_array(const ObConstraint* cst)
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
      cst_array_[i] = cst_array_[i + 1];
    }
  }

  return ret;
}

int ObTableSchema::add_constraint(const ObConstraint& constraint)
{
  int ret = common::OB_SUCCESS;
  char* buf = NULL;
  ObConstraint* cst = NULL;
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

int ObTableSchema::delete_constraint(const ObString& constraint_name)
{
  int ret = common::OB_SUCCESS;
  const ObConstraint* cst = get_constraint(constraint_name);
  if (cst != NULL) {
    if (OB_FAIL(remove_cst_from_cst_array(cst))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to remove cst", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::is_partition_key(uint64_t column_id, bool& result) const
{
  int ret = OB_SUCCESS;
  result = false;
  if (is_partitioned_table()) {
    if (OB_FAIL(get_partition_key_info().is_rowkey_column(column_id, result))) {
      LOG_WARN("check is partition key failed", K(ret), K(column_id));
    } else if (!result && PARTITION_LEVEL_TWO == get_part_level()) {
      if (OB_FAIL(get_subpartition_key_info().is_rowkey_column(column_id, result))) {
        LOG_WARN("check is subpartition key failed", K(ret), K(column_id));
      }
    }
  }
  return ret;
}

int ObTableSchema::set_simple_index_infos(const common::ObIArray<ObAuxTableMetaInfo>& simple_index_infos)
{
  int ret = OB_SUCCESS;
  simple_index_infos_.reset();
  int64_t count = simple_index_infos.count();
  if (OB_FAIL(simple_index_infos_.reserve(count))) {
    LOG_WARN("fail to reserve array", K(ret), K(count));
  }
  FOREACH_CNT_X(simple_index_info, simple_index_infos, OB_SUCC(ret))
  {
    if (OB_FAIL(add_simple_index_info(*simple_index_info))) {
      LOG_WARN("failed to add simple index info", K(ret));
    }
  }
  return ret;
}

int ObTableSchema::set_foreign_key_infos(const ObIArray<ObForeignKeyInfo>& foreign_key_infos)
{
  int ret = OB_SUCCESS;
  foreign_key_infos_.reset();
  int64_t count = foreign_key_infos.count();
  if (OB_FAIL(foreign_key_infos_.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate array", K(ret), K(count));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      const ObForeignKeyInfo& src_foreign_key_info = foreign_key_infos.at(i);
      ObForeignKeyInfo& foreign_info = foreign_key_infos_.at(i);
      if (nullptr == new (&foreign_info) ObForeignKeyInfo(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("placement new return nullptr", K(ret));
      } else if (OB_FAIL(foreign_info.assign(src_foreign_key_info))) {
        LOG_WARN("fail to assign foreign key info", K(ret), K(src_foreign_key_info));
      } else if (!src_foreign_key_info.foreign_key_name_.empty() &&
                 OB_FAIL(deep_copy_str(src_foreign_key_info.foreign_key_name_, foreign_info.foreign_key_name_))) {
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

int64_t ObTableSchema::get_foreign_key_real_count() const
{
  int64_t real_count = foreign_key_infos_.count();
  for (int64_t i = 0; i < foreign_key_infos_.count(); i++) {
    const ObForeignKeyInfo& foreign_key_info = foreign_key_infos_.at(i);
    if (foreign_key_info.child_table_id_ == foreign_key_info.parent_table_id_) {
      ++real_count;
    }
  }
  return real_count;
}

int ObTableSchema::add_foreign_key_info(const ObForeignKeyInfo& foreign_key_info)
{
  int ret = OB_SUCCESS;
  int64_t new_fk_idx = foreign_key_infos_.count();
  if (OB_FAIL(foreign_key_infos_.push_back(ObForeignKeyInfo()))) {
    LOG_WARN("fail to push back empty element", K(ret), K(new_fk_idx));
  } else {
    const ObString& foreign_key_name = foreign_key_info.foreign_key_name_;
    ObForeignKeyInfo& foreign_info = foreign_key_infos_.at(new_fk_idx);
    if (nullptr == new (&foreign_info) ObForeignKeyInfo(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("placement new return nullptr", K(ret));
    } else if (OB_FAIL(foreign_info.assign(foreign_key_info))) {
      LOG_WARN("fail to assign foreign key info", K(ret), K(foreign_key_info));
    } else if (!foreign_key_name.empty() && OB_FAIL(deep_copy_str(foreign_key_name, foreign_info.foreign_key_name_))) {
      LOG_WARN("failed to deep copy foreign key name", K(ret), K(foreign_key_name));
    }
  }

  return ret;
}

int ObTableSchema::add_simple_index_info(const ObAuxTableMetaInfo& simple_index_info)
{
  int ret = OB_SUCCESS;
  bool need_add = true;
  int64_t N = simple_index_infos_.count();

  // we are sure that index_tid are added in sorted order
  if (extract_pure_id(simple_index_info.table_id_) == extract_pure_id(OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aux table tid", K(ret), K(simple_index_info.table_id_));
  } else if (simple_index_info.table_type_ < SYSTEM_TABLE || simple_index_info.table_type_ >= MAX_TABLE_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", K(ret), K(simple_index_info.table_type_));
  } else if (N > 0) {
    need_add = !std::binary_search(simple_index_infos_.begin(),
        simple_index_infos_.end(),
        simple_index_info,
        [](const ObAuxTableMetaInfo& l, const ObAuxTableMetaInfo& r) { return l.table_id_ < r.table_id_; });
  }
  if (OB_SUCC(ret) && need_add) {
    const int64_t last_pos = N - 1;
    if (N >= common::OB_MAX_INDEX_PER_TABLE) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("index num in table is more than limited num", K(ret));
    } else if ((last_pos >= 0) && (simple_index_info.table_id_ <= simple_index_infos_.at(last_pos).table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table id must bigger than last one", K(ret));
    } else if (OB_FAIL(simple_index_infos_.push_back(simple_index_info))) {
      LOG_WARN("failed to push back simple_index_info", K(ret), K(simple_index_info));
    } else if (MATERIALIZED_VIEW == simple_index_info.table_type_ && OB_FAIL(add_mv_tid(simple_index_info.table_id_))) {
      LOG_WARN("failed to add mv tid", K(ret), K(simple_index_info));
    }
  }

  return ret;
}

const ObColumnSchemaV2* ObColumnIterByPrevNextID::get_first_column() const
{
  ObColumnSchemaV2* ret_col = NULL;
  ObTableSchema::const_column_iterator iter = table_schema_.column_begin();
  for (; iter != table_schema_.column_end(); ++iter) {
    if (BORDER_COLUMN_ID == (*iter)->get_prev_column_id()) {
      ret_col = *iter;
      break;
    }
  }
  return ret_col;
}

int ObColumnIterByPrevNextID::next(const ObColumnSchemaV2*& column_schema)
{
  int ret = OB_SUCCESS;
  column_schema = NULL;
  if (table_schema_.is_view_table() && !table_schema_.is_materialized_view()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("This iterator cannot support view", K(ret));
  } else if (is_end_) {
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
      column_schema = get_first_column();
    } else if (BORDER_COLUMN_ID == last_column_schema_->get_next_column_id()) {
      is_end_ = true;
      ret = OB_ITER_END;
    } else {
      column_schema = table_schema_.get_column_schema_by_prev_next_id(last_column_schema_->get_next_column_id());
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

int ObTableSchema::get_part_offset(int64_t part_id, int64_t& part_offset) const
{
  int ret = OB_SUCCESS;
  int64_t part_num = get_first_part_num();
  part_offset = OB_INVALID_INDEX_INT64;
  // The internal tables are all hash or key partitions, the partition_array_ structure is empty,
  // Since 1.4, the normal hash/key partition partition_array_ structure is also not empty
  // Earlier than 1.4 is empty.
  // The internal table does not support partition split, and the part idx of the internal table must be offset.
  if (is_inner_table(table_id_)) {
    part_offset = part_id;
  } else {
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(partition_array_) || OB_ISNULL(partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(partition_array_), K(ret));
      } else if (common::extract_idx_from_partid(partition_array_[i]->get_part_id()) == part_id) {
        part_offset = i;
        break;
      }
    }
  }
  return ret;
}

// FIXME:() Interface to be removed
int ObTableSchema::get_sub_part_offset(const int64_t part_id, const int64_t subpart_id, int64_t& subpart_offset) const
{
  int ret = OB_SUCCESS;
  subpart_offset = OB_INVALID_INDEX_INT64;
  if (PARTITION_LEVEL_TWO == get_part_level()) {
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
        subpart_offset = i;
        break;
      }
    }
  } else {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid table partition level", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret) && OB_INVALID_ID == subpart_offset) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("subpart_offset is invalid", K(ret), K(part_id), K(subpart_id), KPC(this));
  }
  return ret;
}

int ObTableSchema::set_column_encodings(const common::ObIArray<int64_t>& col_encodings)
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

int ObTableSchema::get_column_encodings(common::ObIArray<int64_t>& col_encodings) const
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
      } else if (!column_array_[i]->is_virtual_generated_column() && !column_array_[i]->is_rowid_pseudo_column() &&
                 OB_FAIL(col_encodings.push_back(column_array_[i]->get_encoding_type()))) {
        LOG_WARN("Fail to add column encoding type", K(ret), K(i), K(column_array_[i]));
      }
    }
  }
  return ret;
}

int ObTableSchema::generate_kv_schema(const int64_t tenant_id, const int64_t table_id)
{
  int ret = OB_SUCCESS;
  const char* const KEY_NAME = "key";
  const char* const VALUE_NAME = "value";
  const int64_t SCHEMA_VERSION = 1;
  const char* const TABLE_NAME = "trans_table";

  common::ObObjMeta type;
  type.set_binary();
  ObColumnSchemaV2 key_column;

  key_column.set_tenant_id(tenant_id);
  key_column.set_table_id(table_id);
  key_column.set_column_id(common::OB_APP_MIN_COLUMN_ID);
  key_column.set_schema_version(SCHEMA_VERSION);
  key_column.set_rowkey_position(1);
  key_column.set_data_length(100);
  key_column.set_order_in_rowkey(ObOrderType::ASC);
  key_column.set_meta_type(type);

  ObColumnSchemaV2 value_column;
  type.set_binary();
  value_column.set_tenant_id(tenant_id);
  value_column.set_table_id(table_id);
  value_column.set_column_id(common::OB_APP_MIN_COLUMN_ID + 1);
  value_column.set_schema_version(SCHEMA_VERSION);
  value_column.set_meta_type(type);

  set_tenant_id(tenant_id);
  set_database_id(combine_id(tenant_id, OB_SYS_DATABASE_ID));
  set_table_id(table_id);
  set_schema_version(SCHEMA_VERSION);

  if (OB_FAIL(key_column.set_column_name(KEY_NAME))) {
    LOG_WARN("failed to set column name", K(ret), K(KEY_NAME));
  } else if (OB_FAIL(value_column.set_column_name(VALUE_NAME))) {
    LOG_WARN("failed to set column name", K(ret), K(VALUE_NAME));
  } else if (OB_FAIL(set_table_name(TABLE_NAME))) {
    LOG_WARN("failed to set table name", K(ret), K(TABLE_NAME));
  } else if (OB_FAIL(add_column(key_column))) {
    LOG_WARN("failed to add column", K(ret), K(key_column));
  } else if (OB_FAIL(add_column(value_column))) {
    LOG_WARN("failed to add column", K(ret), K(value_column));
  }
  return ret;
}

int ObTableSchema::is_partition_key_match_rowkey_prefix(bool& is_prefix) const
{
  int ret = OB_SUCCESS;
  is_prefix = false;
  if (is_partitioned_table()) {
    is_prefix = true;
    int64_t i = 0;
    int64_t j = 0;
    uint64_t rowkey_column_id = OB_INVALID_ID;
    uint64_t partkey_column_id = OB_INVALID_ID;
    while (OB_SUCC(ret) && is_prefix && i < rowkey_info_.get_size() && j < partition_key_info_.get_size()) {
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
        while (OB_SUCC(ret) && is_prefix && i < rowkey_info_.get_size() && j < subpartition_key_info_.get_size()) {
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

// get split partition key from rowkey, for auto partition table only
int ObTableSchema::generate_partition_key_from_rowkey(const ObRowkey& rowkey, ObRowkey& partition_key) const
{
  int ret = OB_SUCCESS;
  bool is_prefix = false;
  partition_key = rowkey;
  if (OB_UNLIKELY(!is_auto_partitioned_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should be auto partitioned table", K(ret));
  } else if (OB_UNLIKELY(rowkey.get_obj_cnt() != get_rowkey_column_num())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rowkey size not match rowkey column num", K(ret), K(rowkey), K(get_rowkey_column_num()));
  } else if (OB_FAIL(is_partition_key_match_rowkey_prefix(is_prefix))) {
    LOG_WARN("failed to check is partition key match rowkey prefix", K(ret));
  } else if (!is_prefix) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition key should match rowkey prefix", K(ret));
  } else {
    partition_key.assign(const_cast<ObObj*>(rowkey.get_obj_ptr()), partition_key_info_.get_size());
  }
  return ret;
}

int64_t ObPrintableTableSchema::to_string(char* buf, const int64_t buf_len) const
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
      K_(replica_num),
      K_(previous_locality_str),
      K_(min_partition_id),
      K_(session_id),
      "index_type",
      static_cast<int32_t>(index_type_),
      "table_type",
      static_cast<int32_t>(table_type_),
      K_(table_mode),
      K_(tablespace_id));
  J_COMMA();
  J_KV(K_(data_table_id),
      "name_casemode",
      static_cast<int32_t>(name_case_mode_),
      K_(schema_version),
      K_(part_level),
      K_(part_option),
      K_(sub_part_option),
      K_(locality_str),
      K_(zone_list),
      K_(primary_zone),
      K_(part_num),
      K_(def_subpart_num),
      K_(partition_num),
      K_(def_subpartition_num),
      K_(zone_replica_attr_array),
      K_(primary_zone_array),
      K_(index_status),
      K_(binding),
      K_(duplicate_scope),
      K_(encryption),
      K_(tablespace_id),
      K_(encrypt_key),
      K_(master_key_id),
      K_(drop_schema_version),
      K_(is_sub_part_template));
  J_COMMA();
  J_KV(K_(max_used_column_id),
      K_(max_used_constraint_id),
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
    const ObColumnSchemaV2* col = column_array_[i];
    J_KV("column_id",
        col->get_column_id(),
        "column_name",
        col->get_column_name(),
        "rowkey_pos",
        col->get_rowkey_position(),
        "index_pos",
        col->get_index_position(),
        "meta_type",
        col->get_meta_type(),
        "accuracy",
        col->get_accuracy(),
        "is_nullable",
        col->is_nullable(),
        "is_zero_fill",
        col->is_zero_fill(),
        "cur_default_value",
        col->get_cur_default_value());
    J_COMMA();
  }
  J_KV(K_(rowkey_split_pos),
      K_(block_size),
      K_(is_use_bloomfilter),
      K_(progressive_merge_num),
      K_(tablet_size),
      K_(pctfree),
      K_(compress_func_name),
      K_(row_store_type),
      K_(store_format),
      "load_type",
      static_cast<int32_t>(load_type_),
      "index_using_type",
      static_cast<int32_t>(index_using_type_),
      "def_type",
      static_cast<int32_t>(def_type_),
      "charset_type",
      static_cast<int32_t>(charset_type_),
      "collation_type",
      static_cast<int32_t>(collation_type_));
  J_COMMA();
  J_KV(K_(create_mem_version),
      "index_status",
      static_cast<int32_t>(index_status_),
      "partition_status",
      static_cast<int32_t>(partition_status_),
      K_(code_version),
      K_(last_modified_frozen_version),
      K_(comment),
      K_(pk_comment),
      K_(create_host),
      K_(tablegroup_name),
      K_(expire_info),
      K_(view_schema),
      K_(autoinc_column_id),
      K_(auto_increment),
      K_(read_only),
      "mv_tid_array",
      ObArrayWrap<uint64_t>(mv_tid_array_, mv_cnt_),
      K_(base_table_ids));
  J_OBJ_END();
  return pos;
}

}  // namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
