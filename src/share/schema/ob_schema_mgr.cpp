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

#include "ob_schema_mgr.h"

#include "lib/oblog/ob_log.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_get_compat_mode.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_utils.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;

namespace share
{
namespace schema
{

ObSimpleTenantSchema::ObSimpleTenantSchema()
  : ObSchema()
{
  reset();
}

ObSimpleTenantSchema::ObSimpleTenantSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleTenantSchema::ObSimpleTenantSchema(const ObSimpleTenantSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleTenantSchema::~ObSimpleTenantSchema()
{
}

ObSimpleTenantSchema &ObSimpleTenantSchema::operator =(const ObSimpleTenantSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    schema_version_ = other.schema_version_;
    name_case_mode_ = other.name_case_mode_;
    read_only_ = other.read_only_;
    compatibility_mode_ = other.compatibility_mode_;
    gmt_modified_ = other.gmt_modified_;
    drop_tenant_time_ = other.drop_tenant_time_;
    status_ = other.status_;
    in_recyclebin_ = other.in_recyclebin_;
    arbitration_service_status_ = other.arbitration_service_status_;
    if (OB_FAIL(deep_copy_str(other.tenant_name_, tenant_name_))) {
      LOG_WARN("Fail to deep copy tenant_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.primary_zone_, primary_zone_))) {
      LOG_WARN("Fail to deep copy primary_zone", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.locality_, locality_))) {
      LOG_WARN("Fail to deep copy locality", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.previous_locality_, previous_locality_))) {
      LOG_WARN("Fail to deep copy previous_locality", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimpleTenantSchema::operator ==(const ObSimpleTenantSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_
      && schema_version_ == other.schema_version_
      && tenant_name_ == other.tenant_name_
      && name_case_mode_ == other.name_case_mode_
      && read_only_ == other.read_only_
      && primary_zone_ == other.primary_zone_
      && locality_ == other.locality_
      && previous_locality_ == other.previous_locality_
      && compatibility_mode_ == other.compatibility_mode_
      && gmt_modified_ == other.gmt_modified_
      && drop_tenant_time_ == other.drop_tenant_time_
      && status_ == other.status_
      && in_recyclebin_ == other.in_recyclebin_
      && arbitration_service_status_ == other.arbitration_service_status_) {
    ret = true;
  }

  return ret;
}

void ObSimpleTenantSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  tenant_name_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
  read_only_ = false;
  primary_zone_.reset();
  locality_.reset();
  previous_locality_.reset();
  compatibility_mode_ = ObCompatibilityMode::OCEANBASE_MODE;
  gmt_modified_ = 0;
  drop_tenant_time_ = 0;
  status_ = TENANT_STATUS_NORMAL;
  in_recyclebin_ = false;
  arbitration_service_status_ = ObArbitrationServiceStatus::DISABLED;
}

bool ObSimpleTenantSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_
      || schema_version_ < 0
      || tenant_name_.empty()) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleTenantSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleTenantSchema);
  convert_size += tenant_name_.length() + 1;
  convert_size += primary_zone_.length() + 1;
  convert_size += locality_.length() + 1;
  convert_size += previous_locality_.length() + 1;

  return convert_size;
}

ObSimpleUserSchema::ObSimpleUserSchema()
  : ObSchema()
{
  reset();
}

ObSimpleUserSchema::ObSimpleUserSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleUserSchema::ObSimpleUserSchema(const ObSimpleUserSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleUserSchema::~ObSimpleUserSchema()
{
}

ObSimpleUserSchema &ObSimpleUserSchema::operator =(const ObSimpleUserSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    type_ = other.type_;
    schema_version_ = other.schema_version_;
    if (OB_FAIL(deep_copy_str(other.user_name_, user_name_))) {
      LOG_WARN("Fail to deep copy user_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.host_name_, host_name_))) {
      LOG_WARN("Fail to deep copy host_name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimpleUserSchema::operator ==(const ObSimpleUserSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_
      && user_id_ == other.user_id_
      && schema_version_ == other.schema_version_
      && user_name_ == other.user_name_
      && host_name_ == other.host_name_
      && type_ == other.type_) {
    ret = true;
  }

  return ret;
}

void ObSimpleUserSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  user_name_.reset();
  host_name_.reset();
  type_ = OB_USER;
}

bool ObSimpleUserSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_
      || OB_INVALID_ID == user_id_
      || schema_version_ < 0) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleUserSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleUserSchema);
  convert_size += user_name_.length() + host_name_.length() + 2;

  return convert_size;
}

ObSimpleDatabaseSchema::ObSimpleDatabaseSchema()
  : ObSchema()
{
  reset();
}

ObSimpleDatabaseSchema::ObSimpleDatabaseSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleDatabaseSchema::ObSimpleDatabaseSchema(const ObSimpleDatabaseSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleDatabaseSchema::~ObSimpleDatabaseSchema()
{
}

ObSimpleDatabaseSchema &ObSimpleDatabaseSchema::operator =(const ObSimpleDatabaseSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    database_id_ = other.database_id_;
    schema_version_ = other.schema_version_;
    default_tablegroup_id_ = other.default_tablegroup_id_;
    name_case_mode_ = other.name_case_mode_;
    if (OB_FAIL(deep_copy_str(other.database_name_, database_name_))) {
      LOG_WARN("Fail to deep copy database_name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimpleDatabaseSchema::operator ==(const ObSimpleDatabaseSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_
      && database_id_ == other.database_id_
      && schema_version_ == other.schema_version_
      && default_tablegroup_id_ == other.default_tablegroup_id_
      && database_name_ == other.database_name_
      && name_case_mode_ == other.name_case_mode_) {
    ret = true;
  }

  return ret;
}

void ObSimpleDatabaseSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  default_tablegroup_id_ = OB_INVALID_ID;
  database_name_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
}

bool ObSimpleDatabaseSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_
      || OB_INVALID_ID == database_id_
      || schema_version_ < 0
      || database_name_.empty()) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleDatabaseSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleDatabaseSchema);
  convert_size += database_name_.length() + 1;

  return convert_size;
}

ObSimpleTablegroupSchema::ObSimpleTablegroupSchema()
  : ObSchema()
{
  reset();
}

ObSimpleTablegroupSchema::ObSimpleTablegroupSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleTablegroupSchema::ObSimpleTablegroupSchema(const ObSimpleTablegroupSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleTablegroupSchema::~ObSimpleTablegroupSchema()
{
}

ObSimpleTablegroupSchema &ObSimpleTablegroupSchema::operator =(const ObSimpleTablegroupSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    tablegroup_id_ = other.tablegroup_id_;
    schema_version_ = other.schema_version_;
    partition_status_ = other.partition_status_;
    partition_schema_version_ = other.partition_schema_version_;
    if (OB_FAIL(deep_copy_str(other.tablegroup_name_, tablegroup_name_))) {
      LOG_WARN("Fail to deep copy tablegroup_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.sharding_, sharding_))) {
      LOG_WARN("Fail to deep copy sharding", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimpleTablegroupSchema::operator ==(const ObSimpleTablegroupSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_
      && tablegroup_id_ == other.tablegroup_id_
      && schema_version_ == other.schema_version_
      && tablegroup_name_ == other.tablegroup_name_
      && partition_status_ == other.partition_status_
      && partition_schema_version_ == other.partition_schema_version_
      && sharding_ == other.sharding_) {
    ret = true;
  }

  return ret;
}

void ObSimpleTablegroupSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  tablegroup_name_.reset();
  partition_status_ = PARTITION_STATUS_ACTIVE;
  partition_schema_version_ = 0;// Issues left over from history, set to 0
  sharding_.reset();
}

bool ObSimpleTablegroupSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_
      || OB_INVALID_ID == tablegroup_id_
      || schema_version_ < 0
      || tablegroup_name_.empty()) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleTablegroupSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleTablegroupSchema);
  convert_size += tablegroup_name_.length() + 1;
  convert_size += sharding_.length() + 1;

  return convert_size;
}
//TODO:remove ObSimpleTablegroupSchema::get_zone_list
int ObSimpleTablegroupSchema::get_zone_list(
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

////////////////////////////////////////////////////////////////
ObSchemaMgr::ObSchemaMgr()
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_version_(OB_INVALID_VERSION),
      tenant_id_(OB_INVALID_TENANT_ID),
      is_consistent_(true),
      tenant_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TENANT_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      user_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_USER_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      database_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_DB_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      database_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_DATABASE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      tablegroup_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TABLEG_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      table_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TABLE_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      index_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_INDEX_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      aux_vp_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_AUX_VP_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      lob_meta_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LOB_META_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      lob_piece_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LOB_PIECE_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      table_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_TABLE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      table_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_TABLE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      normal_index_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_INDEX_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      aux_vp_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_AUX_VP_NAME_VEC, ObCtxIds::SCHEMA_SERVICE)),
      outline_mgr_(allocator_),
      routine_mgr_(allocator_),
      priv_mgr_(allocator_),
      synonym_mgr_(allocator_),
      package_mgr_(allocator_),
      trigger_mgr_(allocator_),
      udf_mgr_(allocator_),
      udt_mgr_(allocator_),
      sequence_mgr_(allocator_),
      label_se_policy_mgr_(allocator_),
      label_se_component_mgr_(allocator_),
      label_se_label_mgr_(allocator_),
      label_se_user_level_mgr_(allocator_),
      profile_mgr_(allocator_),
      audit_mgr_(allocator_),
      foreign_key_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_FOREIGN_KEY_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      constraint_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_CONSTRAINT_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      sys_variable_mgr_(allocator_),
      drop_tenant_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_DROP_TENANT_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      keystore_mgr_(allocator_),
      tablespace_mgr_(allocator_),
      hidden_table_name_map_(SET_USE_500("HiddenTblNames", ObCtxIds::SCHEMA_SERVICE)),
      built_in_index_name_map_(SET_USE_500("BuiltInIdxNames", ObCtxIds::SCHEMA_SERVICE)),
      dblink_mgr_(allocator_),
      directory_mgr_(allocator_),
      context_mgr_(allocator_),
      mock_fk_parent_table_mgr_(allocator_),
      rls_policy_mgr_(allocator_),
      rls_group_mgr_(allocator_),
      rls_context_mgr_(allocator_),
      timestamp_in_slot_(0),
      allocator_idx_(OB_INVALID_INDEX),
      mlog_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_MLOG_INFO_VEC, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSchemaMgr::ObSchemaMgr(ObIAllocator &allocator)
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_version_(OB_INVALID_VERSION),
      tenant_id_(OB_INVALID_TENANT_ID),
      is_consistent_(true),
      tenant_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TENANT_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      user_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TENANT_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      database_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_DB_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      database_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_DATABASE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      tablegroup_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TABLEG_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      table_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TABLE_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      index_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_INDEX_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      aux_vp_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_AUX_VP_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      lob_meta_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LOB_META_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      lob_piece_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LOB_PIECE_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      table_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_TABLE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      table_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_TABLE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      normal_index_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_INDEX_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      aux_vp_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_AUX_VP_NAME_VEC, ObCtxIds::SCHEMA_SERVICE)),
      outline_mgr_(allocator_),
      routine_mgr_(allocator_),
      priv_mgr_(allocator_),
      synonym_mgr_(allocator_),
      package_mgr_(allocator_),
      trigger_mgr_(allocator_),
      udf_mgr_(allocator_),
      udt_mgr_(allocator_),
      sequence_mgr_(allocator_),
      label_se_policy_mgr_(allocator_),
      label_se_component_mgr_(allocator_),
      label_se_label_mgr_(allocator_),
      label_se_user_level_mgr_(allocator_),
      profile_mgr_(allocator_),
      audit_mgr_(allocator_),
      foreign_key_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_FOREIGN_KEY_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      constraint_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_CONSTRAINT_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      sys_variable_mgr_(allocator_),
      drop_tenant_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_DROP_TENANT_INFO_VEC, ObCtxIds::SCHEMA_SERVICE)),
      keystore_mgr_(allocator_),
      tablespace_mgr_(allocator_),
      hidden_table_name_map_(SET_USE_500("HiddenTblNames", ObCtxIds::SCHEMA_SERVICE)),
      built_in_index_name_map_(SET_USE_500("BuiltInIdxNames", ObCtxIds::SCHEMA_SERVICE)),
      dblink_mgr_(allocator_),
      directory_mgr_(allocator_),
      context_mgr_(allocator_),
      mock_fk_parent_table_mgr_(allocator_),
      rls_policy_mgr_(allocator_),
      rls_group_mgr_(allocator_),
      rls_context_mgr_(allocator_),
      timestamp_in_slot_(0),
      allocator_idx_(OB_INVALID_INDEX),
      mlog_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_MLOG_INFO_VEC, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSchemaMgr::~ObSchemaMgr()
{
}

int ObSchemaMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(database_name_map_.init())) {
    LOG_WARN("init database name map failed", K(ret));
  } else if (OB_FAIL(table_id_map_.init())) {
    LOG_WARN("init table id map failed", K(ret));
  } else if (OB_FAIL(table_name_map_.init())) {
    LOG_WARN("init table name map failed", K(ret));
  } else if (OB_FAIL(normal_index_name_map_.init())) {
    LOG_WARN("init index name map failed", K(ret));
  } else if (OB_FAIL(aux_vp_name_map_.init())) {
    LOG_WARN("init index name map failed", K(ret));
  } else if (OB_FAIL(foreign_key_name_map_.init())) {
    LOG_WARN("init foreign key name map failed", K(ret));
  } else if (OB_FAIL(constraint_name_map_.init())) {
    LOG_WARN("init constraint name map failed", K(ret));
  } else if (OB_FAIL(outline_mgr_.init())) {
    LOG_WARN("init outline mgr failed", K(ret));
  } else if (OB_FAIL(routine_mgr_.init())) {
    LOG_WARN("init procedure mgr failed", K(ret));
  } else if (OB_FAIL(priv_mgr_.init())) {
    LOG_WARN("init priv mgr failed", K(ret));
  } else if (OB_FAIL(synonym_mgr_.init())) {
    LOG_WARN("init synonym mgr failed", K(ret));
  } else if (OB_FAIL(package_mgr_.init())) {
    LOG_WARN("init package mgr failed", K(ret));
  } else if (OB_FAIL(trigger_mgr_.init())) {
    LOG_WARN("init trigger mgr failed", K(ret));
  } else if (OB_FAIL(udf_mgr_.init())) {
    LOG_WARN("init udf mgr failed", K(ret));
  } else if (OB_FAIL(udt_mgr_.init())) {
    LOG_WARN("init udt mgr failed", K(ret));
  } else if (OB_FAIL(sequence_mgr_.init())) {
    LOG_WARN("init sequence mgr failed", K(ret));
  } else if (OB_FAIL(profile_mgr_.init())) {
    LOG_WARN("init profile mgr failed", K(ret));
  } else if (OB_FAIL(audit_mgr_.init())) {
    LOG_WARN("init audit mgr failed", K(ret));
  } else if (OB_FAIL(sys_variable_mgr_.init())) {
    LOG_WARN("init sys variable mgr failed", K(ret));
  } else if (OB_FAIL(keystore_mgr_.init())) {
    LOG_WARN("init keystore mgr failed", K(ret));
  } else if (OB_FAIL(label_se_policy_mgr_.init())) {
    LOG_WARN("init label security policy mgr failed", K(ret));
  } else if (OB_FAIL(label_se_component_mgr_.init())) {
    LOG_WARN("init label security component mgr failed", K(ret));
  } else if (OB_FAIL(label_se_label_mgr_.init())) {
    LOG_WARN("init label security label mgr failed", K(ret));
  } else if (OB_FAIL(label_se_user_level_mgr_.init())) {
    LOG_WARN("init label security label mgr failed", K(ret));
  } else if (OB_FAIL(tablespace_mgr_.init())) {
    LOG_WARN("init tablespace mgr failed", K(ret));
  } else if (OB_FAIL(dblink_mgr_.init())) {
    LOG_WARN("init dblink mgr failed", K(ret));
  } else if (OB_FAIL(directory_mgr_.init())) {
    LOG_WARN("init directory mgr failed", K(ret));
  } else if (OB_FAIL(rls_policy_mgr_.init())) {
    LOG_WARN("init rls_policy mgr failed", K(ret));
  } else if (OB_FAIL(rls_group_mgr_.init())) {
    LOG_WARN("init rls_group mgr failed", K(ret));
  } else if (OB_FAIL(rls_context_mgr_.init())) {
    LOG_WARN("init rls_context mgr failed", K(ret));
  } else if (OB_FAIL(hidden_table_name_map_.init())) {
    LOG_WARN("init hidden table name map failed", K(ret));
  } else if (OB_FAIL(built_in_index_name_map_.init())) {
    LOG_WARN("init built in index name map failed", K(ret));
  } else if (OB_FAIL(context_mgr_.init())) {
    LOG_WARN("init context mgr failed", K(ret));
  } else if (OB_FAIL(mock_fk_parent_table_mgr_.init())) {
    LOG_WARN("init mock_fk_parent_table_mgr_ failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
  }

  return ret;
}

void ObSchemaMgr::reset()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    timestamp_in_slot_ = 0;
    schema_version_ = OB_INVALID_VERSION;
    is_consistent_ = true;

    // reset will not free memory for vector
    tenant_infos_.clear();
    user_infos_.clear();
    database_infos_.clear();
    tablegroup_infos_.clear();
    table_infos_.clear();
    index_infos_.clear();
    aux_vp_infos_.clear();
    lob_meta_infos_.clear();
    lob_piece_infos_.clear();
    drop_tenant_infos_.clear();

    database_name_map_.clear();
    table_id_map_.clear();
    table_name_map_.clear();
    normal_index_name_map_.clear();
    aux_vp_name_map_.clear();
    foreign_key_name_map_.clear();
    constraint_name_map_.clear();
    outline_mgr_.reset();
    priv_mgr_.reset();
    synonym_mgr_.reset();
    package_mgr_.reset();
    routine_mgr_.reset();
    trigger_mgr_.reset();
    udf_mgr_.reset();
    udt_mgr_.reset();
    sequence_mgr_.reset();
    profile_mgr_.reset();
    audit_mgr_.reset();
    sys_variable_mgr_.reset();
    keystore_mgr_.reset();
    label_se_policy_mgr_.reset();
    label_se_component_mgr_.reset();
    label_se_label_mgr_.reset();
    label_se_user_level_mgr_.reset();
    tablespace_mgr_.reset();
    dblink_mgr_.reset();
    directory_mgr_.reset();
    rls_policy_mgr_.reset();
    rls_group_mgr_.reset();
    rls_context_mgr_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    hidden_table_name_map_.clear();
    built_in_index_name_map_.clear();
    context_mgr_.reset();
    mock_fk_parent_table_mgr_.reset();
    mlog_infos_.clear();
  }
}

ObSchemaMgr &ObSchemaMgr::operator =(const ObSchemaMgr &other)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }

  return *this;
}

int ObSchemaMgr::assign(const ObSchemaMgr &other)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    schema_version_ = other.schema_version_;
    tenant_id_ = other.tenant_id_;
    is_consistent_ = other.is_consistent_;
    #define ASSIGN_FIELD(x)                        \
      if (OB_SUCC(ret)) {                          \
        int64_t start_ts = ObTimeUtility::current_time();   \
        if (OB_FAIL(x.assign(other.x))) {          \
          LOG_WARN("assign " #x "failed", K(ret)); \
        }                                          \
        LOG_INFO("assign "#x" cost", KR(ret),                       \
                 "cost", ObTimeUtility::current_time() - start_ts); \
      }
    ASSIGN_FIELD(tenant_infos_);
    // System variables need to be assigned first
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sys_variable_mgr_.assign(other.sys_variable_mgr_))) {
        LOG_WARN("assign sys variable mgr failed", K(ret));
      }
    }
    ASSIGN_FIELD(user_infos_);
    ASSIGN_FIELD(database_infos_);
    ASSIGN_FIELD(database_name_map_);
    ASSIGN_FIELD(tablegroup_infos_);
    ASSIGN_FIELD(table_infos_);
    ASSIGN_FIELD(index_infos_);
    ASSIGN_FIELD(aux_vp_infos_);
    ASSIGN_FIELD(lob_meta_infos_);
    ASSIGN_FIELD(lob_piece_infos_);
    ASSIGN_FIELD(drop_tenant_infos_);
    ASSIGN_FIELD(table_id_map_);
    ASSIGN_FIELD(table_name_map_);
    ASSIGN_FIELD(normal_index_name_map_);
    ASSIGN_FIELD(aux_vp_name_map_);
    ASSIGN_FIELD(foreign_key_name_map_);
    ASSIGN_FIELD(constraint_name_map_);
    ASSIGN_FIELD(hidden_table_name_map_);
    ASSIGN_FIELD(mlog_infos_);
    ASSIGN_FIELD(built_in_index_name_map_);
    #undef ASSIGN_FIELD
    if (OB_SUCC(ret)) {
      if (OB_FAIL(outline_mgr_.assign(other.outline_mgr_))) {
        LOG_WARN("assign outline mgr failed", K(ret));
      } else if (OB_FAIL(priv_mgr_.assign(other.priv_mgr_))) {
        LOG_WARN("assign priv mgr failed", K(ret));
      } else if (OB_FAIL(routine_mgr_.assign(other.routine_mgr_))) {
        LOG_WARN("assign procedure mgr failed", K(ret));
      } else if (OB_FAIL(synonym_mgr_.assign(other.synonym_mgr_))) {
        LOG_WARN("assign synonym mgr failed", K(ret));
      } else if (OB_FAIL(package_mgr_.assign(other.package_mgr_))) {
        LOG_WARN("assign package mgr failed", K(ret));
      } else if (OB_FAIL(trigger_mgr_.assign(other.trigger_mgr_))) {
        LOG_WARN("assign trigger mgr failed", K(ret));
      } else if (OB_FAIL(udf_mgr_.assign(other.udf_mgr_))) {
        LOG_WARN("assign udf mgr failed", K(ret));
      } else if (OB_FAIL(udt_mgr_.assign(other.udt_mgr_))) {
        LOG_WARN("assign udt mgr failed", K(ret));
      } else if (OB_FAIL(sequence_mgr_.assign(other.sequence_mgr_))) {
        LOG_WARN("assign sequence mgr failed", K(ret));
      } else if (OB_FAIL(keystore_mgr_.assign(other.keystore_mgr_))) {
        LOG_WARN("assign keystore mgr failed", K(ret));
      } else if (OB_FAIL(tablespace_mgr_.assign(other.tablespace_mgr_))) {
        LOG_WARN("assign sequence mgr failed", K(ret));
      } else if (OB_FAIL(label_se_policy_mgr_.assign(other.label_se_policy_mgr_))) {
        LOG_WARN("assign label security mgr failed", K(ret));
      } else if (OB_FAIL(label_se_component_mgr_.assign(other.label_se_component_mgr_))) {
        LOG_WARN("assign label security mgr failed", K(ret));
      } else if (OB_FAIL(label_se_label_mgr_.assign(other.label_se_label_mgr_))) {
        LOG_WARN("assign label security mgr failed", K(ret));
      } else if (OB_FAIL(label_se_user_level_mgr_.assign(other.label_se_user_level_mgr_))) {
        LOG_WARN("assign label security mgr failed", K(ret));
      } else if (OB_FAIL(profile_mgr_.assign(other.profile_mgr_))) {
        LOG_WARN("assign profile mgr failed", K(ret));
      } else if (OB_FAIL(audit_mgr_.assign(other.audit_mgr_))) {
        LOG_WARN("assign audit mgr failed", K(ret));
      } else if (OB_FAIL(dblink_mgr_.assign(other.dblink_mgr_))) {
        LOG_WARN("assign dblink mgr failed", K(ret));
      } else if (OB_FAIL(directory_mgr_.assign(other.directory_mgr_))) {
        LOG_WARN("assign directory mgr failed", K(ret));
      } else if (OB_FAIL(context_mgr_.assign(other.context_mgr_))) {
        LOG_WARN("assign context mgr failed", K(ret));
      } else if (OB_FAIL(mock_fk_parent_table_mgr_.assign(other.mock_fk_parent_table_mgr_))) {
        LOG_WARN("assign mock_fk_parent_table_mgr_ failed", K(ret));
      } else if (OB_FAIL(rls_policy_mgr_.assign(other.rls_policy_mgr_))) {
        LOG_WARN("assign rls_policy mgr failed", K(ret));
      } else if (OB_FAIL(rls_group_mgr_.assign(other.rls_group_mgr_))) {
        LOG_WARN("assign rls_group mgr failed", K(ret));
      } else if (OB_FAIL(rls_context_mgr_.assign(other.rls_context_mgr_))) {
        LOG_WARN("assign rls_context mgr failed", K(ret));
      }
    }
  }
  LOG_INFO("ObSchemaMgr assign cost", KR(ret), "cost", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObSchemaMgr::deep_copy(const ObSchemaMgr &other)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    schema_version_ = other.schema_version_;
    tenant_id_ = other.tenant_id_;
    is_consistent_ = other.is_consistent_;
    #define ADD_SCHEMA(SCHEMA, SCHEMA_TYPE, SCHEMA_ITER)  \
      if (OB_SUCC(ret)) {                                 \
        int64_t start_ts = ObTimeUtility::current_time(); \
        for (SCHEMA_ITER iter = other.SCHEMA##_infos_.begin();               \
            OB_SUCC(ret) && iter != other.SCHEMA##_infos_.end(); iter++) {   \
          const SCHEMA_TYPE *schema = *iter;                                 \
          if (OB_ISNULL(schema)) {                                           \
            ret = OB_ERR_UNEXPECTED;                                         \
            LOG_WARN("NULL ptr", K(ret), KP(schema));                        \
          } else if (OB_FAIL(add_##SCHEMA(*schema))) {                       \
            LOG_WARN("add "#SCHEMA" failed", K(ret), K(*schema));            \
          }                                                                  \
        }                                                                    \
        LOG_INFO("add "#SCHEMA"s cost", KR(ret),                             \
                 "count", other.SCHEMA##_infos_.count(),                     \
                 "cost", ObTimeUtility::current_time() - start_ts);          \
      }
    ADD_SCHEMA(tenant, ObSimpleTenantSchema, ConstTenantIterator);
    // System variables need to be copied first
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sys_variable_mgr_.deep_copy(other.sys_variable_mgr_))) {
        LOG_WARN("deep copy sys variable mgr failed", K(ret));
      }
    }
    ADD_SCHEMA(user, ObSimpleUserSchema, ConstUserIterator);
    ADD_SCHEMA(database, ObSimpleDatabaseSchema, ConstDatabaseIterator);
    ADD_SCHEMA(tablegroup, ObSimpleTablegroupSchema, ConstTablegroupIterator);
    ADD_SCHEMA(table, ObSimpleTableSchemaV2, ConstTableIterator);
    #undef ADD_SCHEMA
    if (OB_SUCC(ret)) {
      if (OB_FAIL(outline_mgr_.deep_copy(other.outline_mgr_))) {
        LOG_WARN("deep copy outline mgr failed", K(ret));
      } else if (OB_FAIL(priv_mgr_.deep_copy(other.priv_mgr_))) {
        LOG_WARN("deep copy priv mgr failed", K(ret));
      } else if (OB_FAIL(routine_mgr_.deep_copy(other.routine_mgr_))) {
        LOG_WARN("deep copy procedure mgr failed", K(ret));
      } else if (OB_FAIL(synonym_mgr_.deep_copy(other.synonym_mgr_))) {
        LOG_WARN("deep copy synonym mgr failed", K(ret));
      } else if (OB_FAIL(package_mgr_.deep_copy(other.package_mgr_))) {
        LOG_WARN("deep copy package mgr failed", K(ret));
      } else if (OB_FAIL(trigger_mgr_.deep_copy(other.trigger_mgr_))) {
        LOG_WARN("deep copy trigger mgr failed", K(ret));
      } else if (OB_FAIL(udf_mgr_.deep_copy(other.udf_mgr_))) {
        LOG_WARN("deep copy udf mgr failed", K(ret));
      } else if (OB_FAIL(udt_mgr_.deep_copy(other.udt_mgr_))) {
        LOG_WARN("deep copy udt mgr failed", K(ret));
      } else if (OB_FAIL(sequence_mgr_.deep_copy(other.sequence_mgr_))) {
        LOG_WARN("deep copy sequence mgr failed", K(ret));
      } else if (OB_FAIL(keystore_mgr_.deep_copy(other.keystore_mgr_))) {
        LOG_WARN("deep copy keystore_mgr failed", K(ret));
      } else if (OB_FAIL(label_se_policy_mgr_.deep_copy(other.label_se_policy_mgr_))) {
        LOG_WARN("deep copy label security mgr failed", K(ret));
      } else if (OB_FAIL(label_se_component_mgr_.deep_copy(other.label_se_component_mgr_))) {
        LOG_WARN("deep copy label security mgr failed", K(ret));
      } else if (OB_FAIL(label_se_label_mgr_.deep_copy(other.label_se_label_mgr_))) {
        LOG_WARN("deep copy label security mgr failed", K(ret));
      } else if (OB_FAIL(label_se_user_level_mgr_.deep_copy(other.label_se_user_level_mgr_))) {
        LOG_WARN("deep copy label security mgr failed", K(ret));
      } else if (OB_FAIL(tablespace_mgr_.deep_copy(other.tablespace_mgr_))) {
        LOG_WARN("deep copy tablespace_mgr_ failed", K(ret));
      } else if (OB_FAIL(profile_mgr_.deep_copy(other.profile_mgr_))) {
        LOG_WARN("deep copy profile mgr failed", K(ret));
      } else if (OB_FAIL(audit_mgr_.deep_copy(other.audit_mgr_))) {
        LOG_WARN("deep copy audit mgr failed", K(ret));
      } else if (OB_FAIL(dblink_mgr_.deep_copy(other.dblink_mgr_))) {
        LOG_WARN("deep copy dblink mgr failed", K(ret));
      } else if (OB_FAIL(directory_mgr_.deep_copy(other.directory_mgr_))) {
        LOG_WARN("deep copy directory mgr failed", K(ret));
      } else if (OB_FAIL(context_mgr_.deep_copy(other.context_mgr_))) {
        LOG_WARN("deep copy context mgr failed", K(ret));
      } else if (OB_FAIL(mock_fk_parent_table_mgr_.deep_copy(other.mock_fk_parent_table_mgr_))) {
        LOG_WARN("deep copy mock_fk_parent_table_mgr_ failed", K(ret));
      } else if (OB_FAIL(rls_policy_mgr_.deep_copy(other.rls_policy_mgr_))) {
        LOG_WARN("deep copy rls_policy mgr failed", K(ret));
      } else if (OB_FAIL(rls_group_mgr_.deep_copy(other.rls_group_mgr_))) {
        LOG_WARN("deep copy rls_group mgr failed", K(ret));
      } else if (OB_FAIL(rls_context_mgr_.deep_copy(other.rls_context_mgr_))) {
        LOG_WARN("deep copy rls_context mgr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (ConstDropTenantInfoIterator iter = other.drop_tenant_infos_.begin();
          OB_SUCC(ret) && iter != other.drop_tenant_infos_.end(); iter++) {
        const ObDropTenantInfo &drop_tenant_info = *(*iter);
        if (OB_FAIL(add_drop_tenant_info(drop_tenant_info))) {
          LOG_WARN("add drop tenant info failed", K(ret), K(drop_tenant_info));
        }
      }
    }
  }
  LOG_INFO("ObSchemaMgr deep_copy cost", KR(ret), "cost", ObTimeUtility::current_time() - start_time);
  return ret;
}

bool ObSchemaMgr::check_inner_stat() const
{
  bool ret = true;
  return ret;
}

bool ObSchemaMgr::compare_tenant(const ObSimpleTenantSchema *lhs,
                                 const ObSimpleTenantSchema *rhs)
{
  return lhs->get_tenant_id() < rhs->get_tenant_id();
}

bool ObSchemaMgr::equal_tenant(const ObSimpleTenantSchema *lhs,
                               const ObSimpleTenantSchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id();
}

bool ObSchemaMgr::compare_with_tenant_id(const ObSimpleTenantSchema *lhs,
                                         const uint64_t tenant_id)
{
  return NULL != lhs ? (lhs->get_tenant_id() < tenant_id) : false;
}

bool ObSchemaMgr::equal_with_tenant_id(const ObSimpleTenantSchema *lhs,
                                       const uint64_t tenant_id)
{
  return NULL != lhs ? (lhs->get_tenant_id() == tenant_id) : false;
}

bool ObSchemaMgr::compare_user(const ObSimpleUserSchema *lhs,
                                   const ObSimpleUserSchema *rhs)
{
  return lhs->get_tenant_user_id() < rhs->get_tenant_user_id();
}

bool ObSchemaMgr::equal_user(const ObSimpleUserSchema *lhs,
                                 const ObSimpleUserSchema *rhs)
{
  return lhs->get_tenant_user_id() == rhs->get_tenant_user_id();
}

bool ObSchemaMgr::compare_with_tenant_user_id(const ObSimpleUserSchema *lhs,
                                                 const ObTenantUserId &tenant_user_id)
{
  return NULL != lhs ? (lhs->get_tenant_user_id() < tenant_user_id) : false;
}

bool ObSchemaMgr::equal_with_tenant_user_id(const ObSimpleUserSchema *lhs,
                                                const ObTenantUserId &tenant_user_id)
{
  return NULL != lhs ? (lhs->get_tenant_user_id() == tenant_user_id) : false;
}

bool ObSchemaMgr::compare_database(const ObSimpleDatabaseSchema *lhs,
                                   const ObSimpleDatabaseSchema *rhs)
{
  return lhs->get_tenant_database_id() < rhs->get_tenant_database_id();
}

bool ObSchemaMgr::equal_database(const ObSimpleDatabaseSchema *lhs,
                                 const ObSimpleDatabaseSchema *rhs)
{
  return lhs->get_tenant_database_id() == rhs->get_tenant_database_id();
}

bool ObSchemaMgr::compare_with_tenant_database_id(const ObSimpleDatabaseSchema *lhs,
                                                 const ObTenantDatabaseId &tenant_database_id)
{
  return NULL != lhs ? (lhs->get_tenant_database_id() < tenant_database_id) : false;
}

bool ObSchemaMgr::equal_with_tenant_database_id(const ObSimpleDatabaseSchema *lhs,
                                                const ObTenantDatabaseId &tenant_database_id)
{
  return NULL != lhs ? (lhs->get_tenant_database_id() == tenant_database_id) : false;
}

bool ObSchemaMgr::compare_tablegroup(const ObSimpleTablegroupSchema *lhs,
                                   const ObSimpleTablegroupSchema *rhs)
{
  return lhs->get_tenant_tablegroup_id() < rhs->get_tenant_tablegroup_id();
}

bool ObSchemaMgr::equal_tablegroup(const ObSimpleTablegroupSchema *lhs,
                                 const ObSimpleTablegroupSchema *rhs)
{
  return lhs->get_tenant_tablegroup_id() == rhs->get_tenant_tablegroup_id();
}

bool ObSchemaMgr::compare_with_tenant_tablegroup_id(const ObSimpleTablegroupSchema *lhs,
                                                 const ObTenantTablegroupId &tenant_tablegroup_id)
{
  return NULL != lhs ? (lhs->get_tenant_tablegroup_id() < tenant_tablegroup_id) : false;
}

bool ObSchemaMgr::equal_with_tenant_tablegroup_id(const ObSimpleTablegroupSchema *lhs,
                                                const ObTenantTablegroupId &tenant_tablegroup_id)
{
  return NULL != lhs ? (lhs->get_tenant_tablegroup_id() == tenant_tablegroup_id) : false;
}

bool ObSchemaMgr::compare_table(const ObSimpleTableSchemaV2 *lhs,
                                const ObSimpleTableSchemaV2 *rhs)
{
  return lhs->get_tenant_table_id() < rhs->get_tenant_table_id();
}

//bool ObSchemaMgr::compare_table_with_data_table_id(const ObSimpleTableSchemaV2 *lhs,
//                                                   const ObSimpleTableSchemaV2 *rhs)
//{
//  return lhs->get_tenant_data_table_id() < rhs->get_tenant_data_table_id();
//}

bool ObSchemaMgr::compare_aux_table(const ObSimpleTableSchemaV2 *lhs,
                                    const ObSimpleTableSchemaV2 *rhs)
{
  bool ret = lhs->get_tenant_data_table_id() < rhs->get_tenant_data_table_id();
  if (lhs->get_tenant_data_table_id() == rhs->get_tenant_data_table_id()) {
    ret = lhs->get_tenant_table_id() < rhs->get_tenant_table_id();
  }
  return ret;
}

bool ObSchemaMgr::equal_table(const ObSimpleTableSchemaV2 *lhs,
                              const ObSimpleTableSchemaV2 *rhs)
{
  return lhs->get_tenant_table_id() == rhs->get_tenant_table_id();
}

bool ObSchemaMgr::compare_with_tenant_table_id(const ObSimpleTableSchemaV2 *lhs,
                                               const ObTenantTableId &tenant_table_id)
{
  return NULL != lhs ? (lhs->get_tenant_table_id() < tenant_table_id) : false;
}

bool ObSchemaMgr::compare_with_tenant_data_table_id(const ObSimpleTableSchemaV2 *lhs,
                                                    const ObTenantTableId &tenant_table_id)
{
  return NULL != lhs ? (lhs->get_tenant_data_table_id() < tenant_table_id) : false;
}

bool ObSchemaMgr::equal_with_tenant_table_id(const ObSimpleTableSchemaV2 *lhs,
                                             const ObTenantTableId &tenant_table_id)
{
  return NULL != lhs ? (lhs->get_tenant_table_id() == tenant_table_id) : false;
}

bool ObSchemaMgr::compare_tenant_table_id_up(const ObTenantTableId &tenant_table_id,
                                             const ObSimpleTableSchemaV2 *lhs)
{
  return NULL != lhs ? (tenant_table_id < lhs->get_tenant_table_id()) : false;
}

bool ObSchemaMgr::compare_drop_tenant_info(const ObDropTenantInfo *lhs,
                                           const ObDropTenantInfo *rhs)
{
  return lhs->get_tenant_id() < rhs->get_tenant_id();
}

bool ObSchemaMgr::equal_drop_tenant_info(const ObDropTenantInfo *lhs,
                                         const ObDropTenantInfo *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id();
}

int ObSchemaMgr::add_tenants(const ObIArray<ObSimpleTenantSchema> &tenant_schemas)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tenant_schema, tenant_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_tenant(*tenant_schema))) {
        LOG_WARN("add tenant failed", K(ret),
                 "tenant_schema", *tenant_schema);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::del_tenants(const ObIArray<uint64_t> &tenants)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tenant, tenants, OB_SUCC(ret)) {
      if (OB_FAIL(del_tenant(*tenant))) {
        LOG_WARN("del tenant failed", K(ret),
                 "tenant_id", *tenant);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_tenant(const ObSimpleTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;

  ObSimpleTenantSchema *new_tenant_schema = NULL;
  TenantIterator iter = NULL;
  ObSimpleTenantSchema *replaced_tenant = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, tenant_schema, new_tenant_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_tenant_schema));
  } else if (OB_FAIL(tenant_infos_.replace(new_tenant_schema,
                                           iter,
                                           compare_tenant,
                                           equal_tenant,
                                           replaced_tenant))) {
    LOG_WARN("failed to add tenant schema", K(ret));
  } else {
    LOG_INFO("add tenant schema", K(ret), K_(tenant_id), K(tenant_schema));
  }

  return ret;
}

int ObSchemaMgr::del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  ObSimpleTenantSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_infos_.remove_if(tenant_id,
                                             compare_with_tenant_id,
                                             equal_with_tenant_id,
                                             schema_to_del))) {
    LOG_WARN("failed to remove tenant schema, ",
             K(tenant_id),
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed tenant schema return NULL, ",
             K(tenant_id),
             K(ret));
  }

  return ret;
}

int ObSchemaMgr::add_drop_tenant_info(const ObDropTenantInfo &drop_tenant_info)
{
  int ret = OB_SUCCESS;
  ObDropTenantInfo tmp_info;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!drop_tenant_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drop tenant info", K(ret), K(drop_tenant_info));
  } else if (OB_FAIL(get_drop_tenant_info(drop_tenant_info.get_tenant_id(), tmp_info))) {
    LOG_WARN("fail to get drop tenant info", K(ret), K(drop_tenant_info));
  } else if (tmp_info.is_valid()) {
    if (tmp_info.get_schema_version() != drop_tenant_info.get_schema_version()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("drop tenant info not match", K(ret), K(tmp_info), K(drop_tenant_info));
    } else {
      // The incremental refresh process may fail and retry, it needs to be reentrant here
      LOG_INFO("drop tenant info already exist", K(ret), K(tmp_info), K(drop_tenant_info));
    }
  } else {
    void *tmp_ptr = allocator_.alloc(sizeof(ObDropTenantInfo));
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc mem failed", K(ret));
    } else {
      DropTenantInfoIterator iter = drop_tenant_infos_.end();
      ObDropTenantInfo *new_ptr = new (tmp_ptr) ObDropTenantInfo;
      *new_ptr = drop_tenant_info;
      if (OB_FAIL(drop_tenant_infos_.insert(new_ptr,
                                            iter,
                                            compare_drop_tenant_info))) {
        LOG_WARN("fail to insert drop tenant info", K(ret), KPC(new_ptr));
      } else {
        LOG_INFO("add drop tenant info", K(ret), KPC(new_ptr));
      }
    }
  }
  return ret;
}

int ObSchemaMgr::add_drop_tenant_infos(const common::ObIArray<ObDropTenantInfo> &drop_tenant_infos)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < drop_tenant_infos.count(); i++) {
      const ObDropTenantInfo &drop_tenant_info = drop_tenant_infos.at(i);
      if (OB_FAIL(add_drop_tenant_info(drop_tenant_info))) {
        LOG_WARN("fail to add drop tenant info", K(ret));
      }
    }
  }
  return ret;
}

// for fallback schema_mgr used
int ObSchemaMgr::del_drop_tenant_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDropTenantInfo *drop_tenant_info = NULL;
  ObDropTenantInfo tmp_info;
  tmp_info.set_tenant_id(tenant_id);
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(drop_tenant_infos_.remove_if(&tmp_info,
                                                  compare_drop_tenant_info,
                                                  equal_drop_tenant_info,
                                                  drop_tenant_info))) {
    LOG_WARN("fail to remove drop tenant info", K(ret), K(tenant_id));
  } else {
    LOG_INFO("remove drop tenant info", K(ret), K(tenant_id), KPC(drop_tenant_info));
  }
  return ret;
}

int ObSchemaMgr::get_drop_tenant_info(const uint64_t tenant_id, ObDropTenantInfo &drop_tenant_info) const
{
  int ret = OB_SUCCESS;
  ObDropTenantInfo tmp_info;
  tmp_info.set_tenant_id(tenant_id);
  DropTenantInfoIterator iter = drop_tenant_infos_.end();
  drop_tenant_info.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    ret = drop_tenant_infos_.find(&tmp_info,
                                  iter,
                                  compare_drop_tenant_info,
                                  equal_drop_tenant_info);
    if (OB_SUCCESS == ret) {
      drop_tenant_info = *(*iter);
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // Not found, as a tenant exists
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to find drop tenant info", K(ret), K(drop_tenant_info));
    }
  }
  return ret;
}

int ObSchemaMgr::get_drop_tenant_ids(common::ObIArray<uint64_t> &drop_tenant_ids) const
{
  int ret = OB_SUCCESS;
  drop_tenant_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(drop_tenant_ids.reserve(drop_tenant_infos_.count()))) {
    LOG_WARN("reserve failed", KR(ret), "count", drop_tenant_infos_.count());
  } else {
    for (ConstDropTenantInfoIterator iter = drop_tenant_infos_.begin();
        OB_SUCC(ret) && iter != drop_tenant_infos_.end();
        iter++) {
      const ObDropTenantInfo &drop_tenant_info = *(*iter);
      if (OB_FAIL(drop_tenant_ids.push_back(drop_tenant_info.get_tenant_id()))) {
        LOG_WARN("push back failed", KR(ret), K(drop_tenant_info));
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_synonym_schema(
    const uint64_t tenant_id,
    const uint64_t synonym_id,
    const ObSimpleSynonymSchema *&synonym_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = synonym_mgr_.get_synonym_schema(synonym_id, synonym_schema);
  }
  return ret;
}

int ObSchemaMgr::get_sequence_schema(
    const uint64_t tenant_id,
    const uint64_t sequence_id,
    const ObSequenceSchema *&sequence_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = sequence_mgr_.get_sequence_schema(sequence_id, sequence_schema);
  }
  return ret;
}

int ObSchemaMgr::get_package_schema(
    const uint64_t tenant_id,
    const uint64_t package_id,
    const ObSimplePackageSchema *&package_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = package_mgr_.get_package_schema(package_id, package_schema);
  }
  return ret;
}
int ObSchemaMgr::get_routine_schema(
    const uint64_t tenant_id,
    const uint64_t routine_id,
    const ObSimpleRoutineSchema *&routine_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = routine_mgr_.get_routine_schema(routine_id, routine_schema);
  }
  return ret;
}
int ObSchemaMgr::get_trigger_schema(
    const uint64_t tenant_id,
    const uint64_t trigger_id,
    const ObSimpleTriggerSchema *&trigger_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = trigger_mgr_.get_trigger_schema(trigger_id, trigger_schema);
  }
  return ret;
}
int ObSchemaMgr::get_udf_schema(
    const uint64_t tenant_id,
    const uint64_t udf_id,
    const ObSimpleUDFSchema *&udf_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = udf_mgr_.get_udf_schema(udf_id, udf_schema);
  }
  return ret;
}
int ObSchemaMgr::get_udt_schema(
    const uint64_t tenant_id,
    const uint64_t udt_id,
    const ObSimpleUDTSchema *&udt_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = udt_mgr_.get_udt_schema(udt_id, udt_schema);
  }
  return ret;
}
int ObSchemaMgr::get_label_se_policy_schema(
    const uint64_t tenant_id,
    const uint64_t label_se_policy_id,
    const ObLabelSePolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = label_se_policy_mgr_.get_schema_by_id(label_se_policy_id, schema);
  }
  return ret;
}
int ObSchemaMgr::get_label_se_component_schema(
    const uint64_t tenant_id,
    const uint64_t label_se_component_id,
    const ObLabelSeComponentSchema *&schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = label_se_component_mgr_.get_schema_by_id(label_se_component_id, schema);
  }
  return ret;
}
int ObSchemaMgr::get_label_se_label_schema(
    const uint64_t tenant_id,
    const uint64_t label_se_label_id,
    const ObLabelSeLabelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = label_se_label_mgr_.get_schema_by_id(label_se_label_id, schema);
  }
  return ret;
}
int ObSchemaMgr::get_label_se_user_level_schema(
    const uint64_t tenant_id,
    const uint64_t label_se_user_level_id,
    const ObLabelSeUserLevelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = label_se_user_level_mgr_.get_schema_by_id(label_se_user_level_id, schema);
  }
  return ret;
}
int ObSchemaMgr::get_tablespace_schema(
    const uint64_t tenant_id,
    const uint64_t tablespace_id,
    const ObTablespaceSchema *&tablespace_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = tablespace_mgr_.get_tablespace_schema(tablespace_id, tablespace_schema);
  }
  return ret;
}
int ObSchemaMgr::get_profile_schema(
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const ObProfileSchema *&schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = profile_mgr_.get_schema_by_id(schema_id, schema);
  }
  return ret;
}
int ObSchemaMgr::get_directory_schema(
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const ObDirectorySchema *&schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = directory_mgr_.get_directory_schema_by_id(schema_id, schema);
  }
  return ret;
}
int ObSchemaMgr::get_keystore_schema(
    const uint64_t tenant_id,
    const ObKeystoreSchema *&keystore_schema) const
{
  return keystore_mgr_.get_keystore_schema(tenant_id, keystore_schema);
}

int ObSchemaMgr::get_tenant_schema(const uint64_t tenant_id,
    const ObSimpleTenantSchema *&tenant_schema) const
{
  int ret = OB_SUCCESS;
  tenant_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObSimpleTenantSchema *tmp_schema = NULL;
    ConstTenantIterator iter =
        tenant_infos_.lower_bound(tenant_id, compare_with_tenant_id);
    if (iter == tenant_infos_.end()) {
      // do-nothing
    } else if (OB_ISNULL(tmp_schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
    } else if (tenant_id != tmp_schema->get_tenant_id()) {
      // do-nothing
    } else {
      tenant_schema = tmp_schema;
    }
  }

  return ret;
}

int ObSchemaMgr::get_tenant_schema(
  const ObString &tenant_name,
  const ObSimpleTenantSchema *&tenant_schema) const
{
  int ret = OB_SUCCESS;
  tenant_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_name));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get tenant schema from non-sys schema mgr not allowed",
             K(ret), K_(tenant_id));
  } else {
    const ObSimpleTenantSchema *tmp_schema = NULL;
    bool is_stop = false;
    for (ConstTenantIterator iter = tenant_infos_.begin();
        OB_SUCC(ret) && iter != tenant_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(tmp_schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
      } else if (tmp_schema->get_tenant_name_str() != tenant_name) {
        // do-nothing
      } else {
        tenant_schema = tmp_schema;
        is_stop = true;
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_users(const ObIArray<ObSimpleUserSchema> &user_schemas)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(user_schema, user_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_user(*user_schema))) {
        LOG_WARN("add user failed", K(ret),
            "user_schema", *user_schema);
      }
    }
  }
  return ret;
}

// NOT USED
int ObSchemaMgr::del_users(const ObIArray<ObTenantUserId> &users)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(user, users, OB_SUCC(ret)) {
      if (OB_FAIL(del_user(*user))) {
        LOG_WARN("del user failed", K(ret),
                 "tenant_id", user->tenant_id_,
                 "user_id", user->user_id_);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_user(const ObSimpleUserSchema &user_schema)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleUserSchema *new_user_schema = NULL;
  UserIterator iter = NULL;
  ObSimpleUserSchema *replaced_user = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!user_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(user_schema));
  } else if (OB_FAIL(get_tenant_schema(user_schema.get_tenant_id(), tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", user_schema.get_tenant_id());
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, user_schema, new_user_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_user_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_user_schema));
  } else if (OB_FAIL(user_infos_.replace(new_user_schema,
                                         iter,
                                         compare_user,
                                         equal_user,
                                         replaced_user))) {
    LOG_WARN("failed to add user schema", K(ret));
  } else {
  }

  return ret;
}

int ObSchemaMgr::del_user(const ObTenantUserId user)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleUserSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!user.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(user));
  } else if (OB_FAIL(get_tenant_schema(user.tenant_id_, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", user.tenant_id_);
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  } else if (OB_FAIL(user_infos_.remove_if(user,
                                           compare_with_tenant_user_id,
                                           equal_with_tenant_user_id,
                                           schema_to_del))) {
    LOG_WARN("failed to remove user schema, ",
             "tenant_id",
             user.tenant_id_,
             "user_id",
             user.user_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed user schema return NULL, ",
             "tenant_id",
             user.tenant_id_,
             "user_id",
             user.user_id_,
             K(ret));
  }
  return ret;
}

int ObSchemaMgr::get_user_schema(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObSimpleUserSchema *&user_schema) const
{
  int ret = OB_SUCCESS;
  user_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(user_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleUserSchema *tmp_schema = NULL;
    ObTenantUserId tenant_user_id_lower(tenant_id, user_id);
    ConstUserIterator iter =
        user_infos_.lower_bound(tenant_user_id_lower, compare_with_tenant_user_id);
    if (iter == user_infos_.end()) {
      // do-nothing
    } else if (OB_ISNULL(tmp_schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
    } else if (tenant_id != tmp_schema->get_tenant_id()
               || user_id != tmp_schema->get_user_id()) {
      // do-nothing
    } else {
      user_schema = tmp_schema;
    }
  }

  return ret;
}

int ObSchemaMgr::get_user_schema(
  const uint64_t tenant_id,
  const ObString &user_name,
  const ObString &host_name,
  const ObSimpleUserSchema *&user_schema) const
{
  int ret = OB_SUCCESS;
  user_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObTenantUserId tenant_user_id_lower(tenant_id, OB_MIN_ID);
    const ObSimpleUserSchema *tmp_schema = NULL;
    ConstUserIterator iter =
        user_infos_.lower_bound(tenant_user_id_lower, compare_with_tenant_user_id);
    bool is_stop = false;
    for (; OB_SUCC(ret) && iter != user_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(tmp_schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
      } else if (tmp_schema->get_tenant_id() > tenant_id) {
        is_stop = true;
      } else if (tmp_schema->get_user_name_str() != user_name) {
        // do-nothing
      } else if (tmp_schema->get_host_name_str() != host_name) {
        // do-nothing
      } else {
        user_schema = tmp_schema;
        is_stop = true;
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_user_schema(const uint64_t tenant_id,
                                const ObString &user_name,
                                ObIArray<const ObSimpleUserSchema *> &users_schema) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObTenantUserId tenant_user_id_lower(tenant_id, OB_MIN_ID);
    const ObSimpleUserSchema *tmp_schema = NULL;
    ConstUserIterator iter = user_infos_.lower_bound(tenant_user_id_lower, compare_with_tenant_user_id);
    bool is_stop = false;
    for (; OB_SUCC(ret) && iter != user_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(tmp_schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
      } else if (tmp_schema->get_tenant_id() > tenant_id) {
        is_stop = true;
      } else if (tmp_schema->get_user_name_str() != user_name) {
        // do-nothing
      } else if (OB_FAIL(users_schema.push_back(tmp_schema))) {
        LOG_WARN("failed to push back user schema", K(tmp_schema), K(ret));
      } else {
        tmp_schema = NULL;;
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_databases(const ObIArray<ObSimpleDatabaseSchema> &database_schemas)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(database_schema, database_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_database(*database_schema))) {
        LOG_WARN("add database failed", K(ret),
                 "database_schema", *database_schema);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::del_databases(const ObIArray<ObTenantDatabaseId> &databases)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(database, databases, OB_SUCC(ret)) {
      if (OB_FAIL(del_database(*database))) {
        LOG_WARN("del database failed", K(ret),
                 "tenant_id", database->tenant_id_,
                 "database_id", database->database_id_);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_database(const ObSimpleDatabaseSchema &db_schema)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleDatabaseSchema *new_db_schema = NULL;
  DatabaseIterator db_iter = NULL;
  ObSimpleDatabaseSchema *replaced_db = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!db_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(db_schema));
  } else if (OB_FAIL(get_tenant_schema(db_schema.get_tenant_id(), tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret), "tenant_id", db_schema.get_tenant_id());
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  }

  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_SUCC(ret)) {
    if (is_sys_tenant(tenant_id_) || is_oceanbase_sys_database_id(db_schema.get_database_id())) {
      // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
      mode = OB_ORIGIN_AND_INSENSITIVE;
    } else if (OB_FAIL(get_tenant_name_case_mode(db_schema.get_tenant_id(), mode))) {
      LOG_WARN("fail to get_tenant_name_case_mode", K(ret), "tenant_id", db_schema.get_tenant_id());
    } else if (OB_NAME_CASE_INVALID == mode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid case mode", K(ret), K(mode));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, db_schema, new_db_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_db_schema));
  } else if (FALSE_IT(new_db_schema->set_name_case_mode(mode))) {
    // will not reach here
  } else if (OB_FAIL(database_infos_.replace(new_db_schema,
                                             db_iter,
                                             compare_database,
                                             equal_database,
                                             replaced_db))) {
    LOG_WARN("failed to add db schema", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (NULL == replaced_db) {
    //do-nothing
  } else if (OB_FAIL(deal_with_db_rename(*replaced_db, *new_db_schema))) {
    LOG_WARN("failed to deal with rename", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObDatabaseSchemaHashWrapper database_name_wrapper(new_db_schema->get_tenant_id(),
                                                      new_db_schema->get_name_case_mode(),
                                                      new_db_schema->get_database_name_str());
    int over_write = 1;
    int hash_ret = database_name_map_.set_refactored(database_name_wrapper, new_db_schema, over_write);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("build database name hashmap failed", K(ret), K(hash_ret),
               "tenant_id", new_db_schema->get_tenant_id(),
               "database_name", new_db_schema->get_database_name());
    }
  }

  return ret;
}

int ObSchemaMgr::del_database(const ObTenantDatabaseId database)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleDatabaseSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!database.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(database));
  } else if (OB_FAIL(get_tenant_schema(database.tenant_id_, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", database.tenant_id_);
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  }

  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_SUCC(ret)) {
    if (is_sys_tenant(tenant_id_) || is_oceanbase_sys_database_id(database.database_id_)) {
      // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
      mode = OB_ORIGIN_AND_INSENSITIVE;
    } else if (OB_FAIL(get_tenant_name_case_mode(database.tenant_id_, mode))) {
      LOG_WARN("fail to get_tenant_name_case_mode", K(ret), "tenant_id", database.tenant_id_);
    } else if (OB_NAME_CASE_INVALID == mode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid case mode", K(ret), K(mode));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(database_infos_.remove_if(database,
                                               compare_with_tenant_database_id,
                                               equal_with_tenant_database_id,
                                               schema_to_del))) {
    LOG_WARN("failed to remove db schema, ",
             "tenant_id",
             database.tenant_id_,
             "database_id",
             database.database_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed db schema return NULL, ",
             "tenant_id",
             database.tenant_id_,
             "database_id",
             database.database_id_,
             K(ret));
  } else {
    ObDatabaseSchemaHashWrapper database_name_wrapper(schema_to_del->get_tenant_id(),
                                                      mode,
                                                      schema_to_del->get_database_name_str());
    int hash_ret = database_name_map_.erase_refactored(database_name_wrapper);
    if (OB_SUCCESS != hash_ret) {
      LOG_WARN("failed delete database from database name hashmap",
               K(ret),
               K(hash_ret),
               "tenant_id", schema_to_del->get_tenant_id(),
               "database_name", schema_to_del->get_database_name());
      // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
      // and the solution is solved by rebuild logic
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
  }
  // ignore ret
  if (database_infos_.count() != database_name_map_.item_count()) {
    LOG_WARN("database info is non-consistent",
             "database_infos_count",
             database_infos_.count(),
             "database_name_map_item_count",
             database_name_map_.item_count(),
             "tenant_id",
             database.tenant_id_,
             "database_id",
             database.database_id_);
  }

  return ret;
}

int ObSchemaMgr::get_database_schema(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObSimpleDatabaseSchema *&database_schema) const
{
  int ret = OB_SUCCESS;
  database_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(database_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleDatabaseSchema *tmp_schema = NULL;
    ObTenantDatabaseId tenant_database_id_lower(tenant_id, database_id);
    ConstDatabaseIterator database_iter =
        database_infos_.lower_bound(tenant_database_id_lower, compare_with_tenant_database_id);
    if (database_iter == database_infos_.end()) {
      // do-nothing
    } else if (OB_ISNULL(tmp_schema = *database_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
    } else if (tenant_id != tmp_schema->get_tenant_id()
               || database_id != tmp_schema->get_database_id()) {
      // do-nothing
    } else {
      database_schema = tmp_schema;
    }
  }

  return ret;
}

int ObSchemaMgr::get_database_schema(
  const uint64_t tenant_id,
  const ObString &database_name,
  const ObSimpleDatabaseSchema *&database_schema) const
{
  int ret = OB_SUCCESS;
  database_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_name));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleDatabaseSchema *tmp_schema = NULL;
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (OB_SUCC(ret)) {
      if (OB_SYS_TENANT_ID == tenant_id_ || 0 == database_name.case_compare(OB_SYS_DATABASE_NAME)) {
        // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
        mode = OB_ORIGIN_AND_INSENSITIVE;
      } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
        LOG_WARN("fail to get_tenant_name_case_mode", K(ret), K(tenant_id));
      } else if (OB_NAME_CASE_INVALID == mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid case mode", K(ret), K(mode));
      }
    }
    if (OB_SUCC(ret)) {
      const ObDatabaseSchemaHashWrapper database_name_wrapper(tenant_id, mode, database_name);
      int hash_ret = database_name_map_.get_refactored(database_name_wrapper, tmp_schema);
      if (OB_SUCCESS == hash_ret) {
        if (OB_ISNULL(tmp_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
        } else {
          database_schema = tmp_schema;
        }
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_tablegroups(const ObIArray<ObSimpleTablegroupSchema> &tablegroup_schemas)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tablegroup_schema, tablegroup_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_tablegroup(*tablegroup_schema))) {
        LOG_WARN("add tablegroup failed", K(ret),
                 "tablegroup_schema", *tablegroup_schema);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::del_tablegroups(const ObIArray<ObTenantTablegroupId> &tablegroups)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tablegroup, tablegroups, OB_SUCC(ret)) {
      if (OB_FAIL(del_tablegroup(*tablegroup))) {
        LOG_WARN("del tablegroup failed", K(ret),
                 "tenant_id", tablegroup->tenant_id_,
                 "tablegroup_id", tablegroup->tablegroup_id_);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_tablegroup(const ObSimpleTablegroupSchema &tg_schema)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleTablegroupSchema *new_tg_schema = NULL;
  TablegroupIterator tg_iter = NULL;
  ObSimpleTablegroupSchema *replaced_tg = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tg_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tg_schema));
  } else if (OB_FAIL(get_tenant_schema(tg_schema.get_tenant_id(), tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", tg_schema.get_tenant_id());
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, tg_schema, new_tg_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_tg_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_tg_schema));
  } else if (OB_FAIL(tablegroup_infos_.replace(new_tg_schema,
                                             tg_iter,
                                             compare_tablegroup,
                                             equal_tablegroup,
                                             replaced_tg))) {
    LOG_WARN("failed to add tg schema", K(ret));
  }

  return ret;
}

int ObSchemaMgr::del_tablegroup(const ObTenantTablegroupId tablegroup)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleTablegroupSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tablegroup.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup));
  } else if (OB_FAIL(get_tenant_schema(tablegroup.tenant_id_, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", tablegroup.tenant_id_);
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  } else if (OB_FAIL(tablegroup_infos_.remove_if(tablegroup,
                                                 compare_with_tenant_tablegroup_id,
                                                 equal_with_tenant_tablegroup_id,
                                                 schema_to_del))) {
    LOG_WARN("failed to remove tg schema, ",
             "tenant_id",
             tablegroup.tenant_id_,
             "tablegroup_id",
             tablegroup.tablegroup_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed tg schema return NULL, ",
             "tenant_id",
             tablegroup.tenant_id_,
             "tablegroup_id",
             tablegroup.tablegroup_id_,
             K(ret));
  }

  return ret;
}

int ObSchemaMgr::get_tablegroup_schema(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    const ObSimpleTablegroupSchema *&tablegroup_schema) const
{
  int ret = OB_SUCCESS;
  tablegroup_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleTablegroupSchema *tmp_schema = NULL;
    ObTenantTablegroupId tenant_tablegroup_id_lower(tenant_id, tablegroup_id);
    ConstTablegroupIterator iter =
        tablegroup_infos_.lower_bound(tenant_tablegroup_id_lower, compare_with_tenant_tablegroup_id);
    if (iter == tablegroup_infos_.end()) {
      // do-nothing
    } else if (OB_ISNULL(tmp_schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
    } else if (tenant_id != tmp_schema->get_tenant_id()
               || tablegroup_id != tmp_schema->get_tablegroup_id()) {
      // do-nothing
    } else {
      tablegroup_schema = tmp_schema;
    }
  }

  return ret;
}

int ObSchemaMgr::get_tablegroup_schema(
  const uint64_t tenant_id,
  const ObString &tablegroup_name,
  const ObSimpleTablegroupSchema *&tablegroup_schema) const
{
  int ret = OB_SUCCESS;
  tablegroup_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObTenantTablegroupId tenant_tablegroup_id_lower(tenant_id, OB_MIN_ID);
    const ObSimpleTablegroupSchema *tmp_schema = NULL;
    ConstTablegroupIterator iter =
        tablegroup_infos_.lower_bound(tenant_tablegroup_id_lower, compare_with_tenant_tablegroup_id);
    bool is_stop = false;
    for (; OB_SUCC(ret) && iter != tablegroup_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(tmp_schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
      } else if (tmp_schema->get_tenant_id() > tenant_id) {
        is_stop = true;
      } else if (tmp_schema->get_tablegroup_name() != tablegroup_name) {
        // do-nothing
      } else {
        tablegroup_schema = tmp_schema;
        is_stop = true;
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_tables(
    const ObIArray<ObSimpleTableSchemaV2 *> &table_schemas,
    const bool refresh_full_schema/*= false*/)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  static const int64_t STAGE_CNT = 5;
  int64_t cost_time_array[STAGE_CNT] = {0};
  ObArrayWrap<int64_t> cost_array(cost_time_array, STAGE_CNT);
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (refresh_full_schema && OB_FAIL(reserved_mem_for_tables_(table_schemas))) {
    LOG_WARN("fail to reserved mem for tables", KR(ret));
  } else {
    bool desc_order = true;
    if (OB_SUCC(ret) && table_schemas.count() >= 2) {
      if (OB_ISNULL(table_schemas.at(0)) || OB_ISNULL(table_schemas.at(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", KR(ret), KP(table_schemas.at(0)), K(table_schemas.at(1)));
      } else {
        // 1. when refresh user simple table schemas, table_schemas will be sorted in desc order by sql.
        // 2. when broadcast schema or refresh core/system tables or other situations, table_schemas will be sorted in asc order.
        // Because table_infos_ are sorted in asc order, we should also add table in asc order to reduce performance lost.
        // Normally, we consider table_schemas are in desc order in most situations.
        desc_order = table_schemas.at(0)->get_table_id() > table_schemas.at(1)->get_table_id();
      }
    }

    if (OB_SUCC(ret)) {
      if (desc_order) {
        for (int64_t i = table_schemas.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
          const ObSimpleTableSchemaV2 *table = table_schemas.at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is null", KR(ret), K(i));
          } else if (OB_FAIL(add_table(*table, &cost_array))) {
            LOG_WARN("add table failed", KR(ret), KPC(table));
          }
        } // end for
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
          const ObSimpleTableSchemaV2 *table = table_schemas.at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is null", KR(ret), K(i));
          } else if (OB_FAIL(add_table(*table, &cost_array))) {
            LOG_WARN("add table failed", KR(ret), KPC(table));
          }
        } // end for
      }
    }
  }
  FLOG_INFO("add tables", KR(ret),
            "stage_cost", cost_array,
            "cost", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObSchemaMgr::reserved_mem_for_tables_(
    const ObIArray<ObSimpleTableSchemaV2*> &table_schemas)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  const int64_t table_cnt = table_schemas.count();
  int64_t index_cnt = 0;
  int64_t vp_cnt = 0;
  int64_t lob_meta_cnt = 0;
  int64_t lob_piece_cnt = 0;
  int64_t hidden_table_cnt = 0;
  int64_t mlog_cnt = 0;
  int64_t other_table_cnt = 0;
  int64_t fk_cnt = 0;
  int64_t cst_cnt = 0;
  const int64_t OBJECT_SIZE = sizeof(void*);
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(table_infos_.reserve(table_cnt))) {
    LOG_WARN("fail to reserved array", KR(ret), K(table_cnt));
  } else {
    //(void) table_id_map_.set_sub_map_mem_size(table_cnt * OBJECT_SIZE);

    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
      const ObSimpleTableSchemaV2 *table = table_schemas.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", KR(ret), K(i));
      } else {
        if (table->is_index_table()) {
          index_cnt++;
        } else if (table->is_aux_vp_table()) {
          vp_cnt++;
        } else if (table->is_aux_lob_meta_table()) {
          lob_meta_cnt++;
        } else if (table->is_aux_lob_piece_table()) {
          lob_piece_cnt++;
        } else if (table->is_user_hidden_table()) {
          hidden_table_cnt++;
        } else if (table->is_mlog_table()) {
          mlog_cnt++;
        } else {
          other_table_cnt++;
        }

        if ((table->is_table() || table->is_oracle_tmp_table())
            && !table->is_user_hidden_table()) {
          fk_cnt += table->get_simple_foreign_key_info_array().count();
        }

        if ((table->is_table() || table->is_oracle_tmp_table())
            && !table->is_user_hidden_table()
            && !table->is_mysql_tmp_table()) {
          cst_cnt += table->get_simple_constraint_info_array().count();
        }
      }
    } // end for

    if (OB_SUCC(ret) && index_cnt > 0) {
      if (OB_FAIL(index_infos_.reserve(index_cnt))) {
        LOG_WARN("fail to reserved array", KR(ret), K(index_cnt));
      } else {
        //(void) index_name_map_.set_sub_map_mem_size(index_cnt * OBJECT_SIZE);
      }
    }

    if (OB_SUCC(ret) && vp_cnt > 0) {
      if (OB_FAIL(aux_vp_infos_.reserve(vp_cnt))) {
        LOG_WARN("fail to reserved array", KR(ret), K(vp_cnt));
      } else {
        //(void) aux_vp_name_map_.set_sub_map_mem_size(vp_cnt * OBJECT_SIZE);
      }
    }

    if (OB_SUCC(ret) && lob_meta_cnt > 0) {
      if (OB_FAIL(lob_meta_infos_.reserve(lob_meta_cnt))) {
        LOG_WARN("fail to reserved array", KR(ret), K(lob_meta_cnt));
      }
    }

    if (OB_SUCC(ret) && lob_piece_cnt > 0) {
      if (OB_FAIL(lob_piece_infos_.reserve(lob_piece_cnt))) {
        LOG_WARN("fail to reserved array", KR(ret), K(lob_piece_cnt));
      }
    }

    if (OB_SUCC(ret) && mlog_cnt > 0) {
      if (OB_FAIL(mlog_infos_.reserve(mlog_cnt))) {
        LOG_WARN("fail to reserved array", KR(ret), K(mlog_cnt));
      }
    }

    if (OB_SUCC(ret) && other_table_cnt > 0) {
      //(void) table_name_map_.set_sub_map_mem_size(other_table_cnt * OBJECT_SIZE);
    }

    if (OB_SUCC(ret) && fk_cnt > 0) {
      //(void) foreign_key_name_map_.set_sub_map_mem_size(fk_cnt * OBJECT_SIZE);
    }

    if (OB_SUCC(ret) && cst_cnt > 0) {
      //(void) constraint_name_map_.set_sub_map_mem_size(cst_cnt * OBJECT_SIZE);
    }

  }
  FLOG_INFO("reserve mem", KR(ret),
            K(table_cnt), K(index_cnt), K(vp_cnt),
            K(lob_meta_cnt), K(lob_piece_cnt),
            K(hidden_table_cnt), K(mlog_cnt),
            K(other_table_cnt), K(fk_cnt), K(cst_cnt),
            "cost", ObTimeUtility::current_time() - start_time);
  return ret;
}


int ObSchemaMgr::del_tables(const ObIArray<ObTenantTableId> &tables)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(table, tables, OB_SUCC(ret)) {
      if (OB_FAIL(del_table(*table))) {
        LOG_WARN("del table failed", K(ret),
                 "tenant_id", table->tenant_id_,
                 "table_id", table->table_id_);
      }
    }
  }

  return ret;
}

int ObSchemaMgr::add_table(
    const ObSimpleTableSchemaV2 &table_schema,
    common::ObArrayWrap<int64_t> *cost_array /*= NULL*/)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleTableSchemaV2 *new_table_schema = NULL;
  TableIterator iter = NULL;
  ObSimpleTableSchemaV2 *replaced_table = NULL;
  const uint64_t table_id = table_schema.get_table_id();
  bool is_system_table = false;
  int64_t idx = 0;
  if (OB_ALL_CORE_TABLE_TID == table_schema.get_table_id()) {
    FLOG_INFO("add __all_core_table schema", KR(ret), K(table_schema), K(lbt()));
  }

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema));
  } else if (OB_FAIL(get_tenant_schema(table_schema.get_tenant_id(), tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", table_schema.get_tenant_id());
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  }

  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, is_system_table))) {
    LOG_WARN("fail to check if table_id in tenant space", K(ret), K(table_id));
  } else if (OB_SYS_TENANT_ID == tenant_id_ || is_system_table) {
    // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
    mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(get_tenant_name_case_mode(table_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", "tenant_id", table_schema.get_tenant_id(), K(ret));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  }

  int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(ret)){
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, table_schema, new_table_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_table_schema) || !new_table_schema->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_table_schema));
  }
  if (OB_NOT_NULL(cost_array) && idx < cost_array->count()) {
    cost_array->at(idx++) += ObTimeUtility::current_time() - start_time;
  }

  start_time = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(new_table_schema->set_name_case_mode(mode))) {
    // will not reach here
  } else if (OB_FAIL(table_infos_.replace(new_table_schema,
                                          iter,
                                          compare_table,
                                          equal_table,
                                          replaced_table))) {
    LOG_WARN("failed to add table schema", K(ret));
  } else if (new_table_schema->is_index_table()) {
    ObSimpleTableSchemaV2 *replaced_index_table = NULL;
    if (OB_FAIL(index_infos_.replace(new_table_schema,
                                     iter,
                                     compare_aux_table,
                                     equal_table,
                                     replaced_index_table))) {
      LOG_WARN("failed to add index schema", K(ret));
    }
  } else if (new_table_schema->is_aux_vp_table()) {
    ObSimpleTableSchemaV2 *replaced_aux_vp_table = NULL;
    if (OB_FAIL(aux_vp_infos_.replace(new_table_schema,
                                     iter,
                                     compare_aux_table,
                                     equal_table,
                                     replaced_aux_vp_table))) {
      LOG_WARN("failed to add aux_vp schema", K(ret));
    }
  } else if (new_table_schema->is_aux_lob_meta_table()) {
    ObSimpleTableSchemaV2 *replaced_lob_meta_table = NULL;
    if (OB_FAIL(lob_meta_infos_.replace(new_table_schema,
                                        iter,
                                        compare_aux_table,
                                        equal_table,
                                        replaced_lob_meta_table))) {
      LOG_WARN("failed to add lob meta schema", K(ret));
    }
  } else if (new_table_schema->is_aux_lob_piece_table()) {
    ObSimpleTableSchemaV2 *replaced_lob_piece_table = NULL;
    if (OB_FAIL(lob_piece_infos_.replace(new_table_schema,
                                         iter,
                                         compare_aux_table,
                                         equal_table,
                                         replaced_lob_piece_table))) {
      LOG_WARN("failed to add lob piece schema", K(ret));
    }
  } else if (new_table_schema->is_mlog_table()) {
    ObSimpleTableSchemaV2 *replaced_mlog_table = NULL;
    if (OB_FAIL(mlog_infos_.replace(new_table_schema,
                                    iter,
                                    compare_aux_table,
                                    equal_table,
                                    replaced_mlog_table))) {
      LOG_WARN("failed to add mlog schema", KR(ret));
    }
  }
  if (OB_NOT_NULL(cost_array) && idx < cost_array->count()) {
    cost_array->at(idx++) += ObTimeUtility::current_time() - start_time;
  }

  start_time = ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    if (NULL == replaced_table) {
      // do-nothing
    } else if (OB_FAIL(deal_with_table_rename(*replaced_table, *new_table_schema))) {
      LOG_WARN("failed to deal with rename", K(ret));
    } else if (OB_FAIL(deal_with_change_table_state(*replaced_table, *new_table_schema))) {
      LOG_WARN("failed to deal with change table state", K(ret));
    }
  }
  if (OB_NOT_NULL(cost_array) && idx < cost_array->count()) {
    cost_array->at(idx++) += ObTimeUtility::current_time() - start_time;
  }

  if (OB_SUCC(ret)) {
    start_time = ObTimeUtility::current_time();
    int over_write = 1;
    int hash_ret = table_id_map_.set_refactored(new_table_schema->get_table_id(),
                                     new_table_schema,
                                     over_write);
    if (OB_NOT_NULL(cost_array) && idx < cost_array->count()) {
      cost_array->at(idx++) += ObTimeUtility::current_time() - start_time;
    }

    start_time = ObTimeUtility::current_time();
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("build table id hashmap failed", K(ret), K(hash_ret),
               "table_id", new_table_schema->get_table_id());
    } else {
      bool is_oracle_mode = false;
      if (OB_FAIL(new_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
      } else if (new_table_schema->is_user_hidden_table()) { // hidden table will not be added to the map
        ObTableSchemaHashWrapper table_name_wrapper(new_table_schema->get_tenant_id(),
                                                    new_table_schema->get_database_id(),
                                                    new_table_schema->get_session_id(),
                                                    new_table_schema->get_name_case_mode(),
                                                    new_table_schema->get_table_name_str());
        hash_ret = hidden_table_name_map_.set_refactored(table_name_wrapper, new_table_schema,
                                                         over_write);
        if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build hidden table name hashmap failed", K(ret), K(hash_ret),
                   "table_id", new_table_schema->get_table_id(),
                   "table_name", new_table_schema->get_table_name());
        }
      } else if (new_table_schema->is_index_table()) { // index is in recyclebin
        const bool is_built_in_index = new_table_schema->is_built_in_fts_index();
        IndexNameMap &index_name_map = get_index_name_map_(is_built_in_index);
        if (new_table_schema->is_in_recyclebin()) {
          ObIndexSchemaHashWrapper index_name_wrapper(new_table_schema->get_tenant_id(),
                                                      new_table_schema->get_database_id(),
                                                      common::OB_INVALID_ID,
                                                      new_table_schema->get_table_name_str());
          hash_ret = index_name_map.set_refactored(index_name_wrapper, new_table_schema, over_write);
          if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("build index name hashmap failed", K(ret), K(hash_ret), K(is_built_in_index),
                     "table_id", new_table_schema->get_table_id(),
                     "index_name", new_table_schema->get_table_name());
          }
        } else { // index is not in recyclebin
          if (OB_FAIL(new_table_schema->generate_origin_index_name())) {
            LOG_WARN("generate origin index name failed", K(ret), K(new_table_schema->get_table_name_str()));
          } else {
            ObIndexSchemaHashWrapper cutted_index_name_wrapper(new_table_schema->get_tenant_id(),
                                                               new_table_schema->get_database_id(),
                                                               is_oracle_mode ? common::OB_INVALID_ID : new_table_schema->get_data_table_id(),
                                                               new_table_schema->get_origin_index_name_str());
            hash_ret = index_name_map.set_refactored(cutted_index_name_wrapper, new_table_schema, over_write);
            if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("build index name hashmap failed", K(ret), K(hash_ret), K(is_built_in_index),
                       K(new_table_schema->get_table_id()),
                       K(new_table_schema->get_data_table_id()),
                       K(new_table_schema->get_origin_index_name_str()));
            }
          }
        }
      } else if (new_table_schema->is_aux_vp_table()) {
        ObAuxVPSchemaHashWrapper aux_vp_name_wrapper(new_table_schema->get_tenant_id(),
                                                     new_table_schema->get_database_id(),
                                                     new_table_schema->get_table_name_str());
        hash_ret = aux_vp_name_map_.set_refactored(aux_vp_name_wrapper, new_table_schema, over_write);
        if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build aux_vp table name hashmap failed", K(ret), K(hash_ret),
                   "table_id", new_table_schema->get_table_id(),
                   "aux_vp_name", new_table_schema->get_table_name());
        }
      } else if (new_table_schema->is_aux_lob_table()) {
        // do nothing
      } else {
        ObTableSchemaHashWrapper table_name_wrapper(new_table_schema->get_tenant_id(),
                                                    new_table_schema->get_database_id(),
                                                    new_table_schema->get_session_id(),
                                                    new_table_schema->get_name_case_mode(),
                                                    new_table_schema->get_table_name_str());
        hash_ret = table_name_map_.set_refactored(table_name_wrapper, new_table_schema, over_write);
        if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build table name hashmap failed", K(ret), K(hash_ret),
                   "table_id", new_table_schema->get_table_id(),
                   "table_name", new_table_schema->get_table_name());
        }
      }
      if (OB_SUCC(ret) && (new_table_schema->is_table() || new_table_schema->is_oracle_tmp_table())) {
        if (NULL != replaced_table) {
          if (!replaced_table->is_user_hidden_table()
              && new_table_schema->is_user_hidden_table()) {
            if (OB_FAIL(delete_foreign_keys_in_table(*replaced_table))) {
              LOG_WARN("delete foreign keys info from a hash map failed",
              K(ret), K(*replaced_table));
            }
          // deal with the situation that alter table drop fk and truncate table enter the recycle bin,
          // and delete the foreign key information dropped from the hash map
          // First delete the foreign key information on the table from the hash map when truncate table,
          // and add it back when rebuild_table_hashmap
          } else if (OB_FAIL(check_and_delete_given_fk_in_table(replaced_table, new_table_schema))) {
            LOG_WARN("check and delete given fk in table failed", K(ret), K(*replaced_table), K(*new_table_schema));
          }
        }
        if (OB_SUCC(ret) && !new_table_schema->is_user_hidden_table()) {
          if (OB_FAIL(add_foreign_keys_in_table(new_table_schema->get_simple_foreign_key_info_array(), 1 /*over_write*/))) {
            LOG_WARN("add foreign keys info to a hash map failed", K(ret), K(*new_table_schema));
          } else {
            // do nothing
          }
        }
      }
      if (OB_SUCC(ret) && (new_table_schema->is_table() || new_table_schema->is_oracle_tmp_table())) {
        // In mysql mode, check constraints in non-temporary tables don't share namespace with constraints in temporary tables
        if (NULL != replaced_table) {
          if (!replaced_table->is_user_hidden_table()
              && new_table_schema->is_user_hidden_table()) {
            if (OB_FAIL(delete_constraints_in_table(*replaced_table))) {
              LOG_WARN("delete constraint info from a hash map failed",
              K(ret), K(*replaced_table));
            }
          // deal with the situation that alter table drop cst and truncate table enter the recycle bin,
          // delete the constraint information dropped from the hash map
          // When truncate table, delete the constraint information on the table from the hash map first,
          // and add it back when rebuild_table_hashmap
          } else if (OB_FAIL(check_and_delete_given_cst_in_table(replaced_table, new_table_schema))) {
            LOG_WARN("check and delete given cst in table failed", K(ret), K(*replaced_table), K(*new_table_schema));
          }
        }
        if (OB_SUCC(ret) && !new_table_schema->is_user_hidden_table()) {
          if (OB_FAIL(add_constraints_in_table(new_table_schema, 1 /*over_write*/))) {
            LOG_WARN("add foreign keys info to a hash map failed", K(ret), K(*new_table_schema));
          } else {
            // do nothing
          }
        }
      }
    }
    if (OB_NOT_NULL(cost_array) && idx < cost_array->count()) {
      cost_array->at(idx++) += ObTimeUtility::current_time() - start_time;
    }
  }

  return ret;
}

// Used to add all foreign key information in a table to the member variable ForeignKeyNameMap of ObSchemaMgr
int ObSchemaMgr::add_foreign_keys_in_table(
    const ObIArray<ObSimpleForeignKeyInfo> &fk_info_array,
    const int over_write)
{
  int ret = OB_SUCCESS;

  if (fk_info_array.empty()) {
    // If there is no foreign key in the table, do nothing
  } else {
    FOREACH_CNT_X(simple_foreign_key_info, fk_info_array, OB_SUCC(ret)) {
      ObForeignKeyInfoHashWrapper foreign_key_name_wrapper(simple_foreign_key_info->tenant_id_,
                                                           simple_foreign_key_info->database_id_,
                                                           simple_foreign_key_info->foreign_key_name_);
      int hash_ret = foreign_key_name_map_.set_refactored(foreign_key_name_wrapper,
                                                          const_cast<ObSimpleForeignKeyInfo*> (simple_foreign_key_info),
                                                          over_write);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
        LOG_ERROR("build fk name hashmap failed", K(ret), K(hash_ret),
                  "fk_id", simple_foreign_key_info->foreign_key_id_,
                  "fk_name", simple_foreign_key_info->foreign_key_name_);
      }
    }
  }

  return ret;
}

// According to table_schema and foreign key name, delete the specified foreign key related to the corresponding table_schema
int ObSchemaMgr::delete_given_fk_from_mgr(const ObSimpleForeignKeyInfo &fk_info)
{
  int ret = OB_SUCCESS;

  if (fk_info.tenant_id_ == common::OB_INVALID_ID
      || fk_info.database_id_ == common::OB_INVALID_ID
      || fk_info.foreign_key_name_.empty()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fk_info should not be null", K(ret), K(fk_info));
  } else {
    ObForeignKeyInfoHashWrapper foreign_key_name_wrapper(fk_info.tenant_id_,
                                                         fk_info.database_id_,
                                                         fk_info.foreign_key_name_);
    int hash_ret = foreign_key_name_map_.erase_refactored(foreign_key_name_wrapper);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // Because there is no guarantee to refresh in strict accordance with the version order of the schema version,
      // the return value of OB_HASH_NOT_EXIST is reasonable in very special scenarios
      // At this time, the foreign key information in foreign_key_name_map_ is inconsistent with the correct foreign key information.
      // It is necessary to rebuild foreign_key_name_map_ according to the correct foreign key information.
      is_consistent_= false;
      LOG_WARN("fail to delete fk from fk name hashmap", K(ret), K(hash_ret),
               "tenant id", fk_info.tenant_id_,
               "database id", fk_info.database_id_,
               "fk name", fk_info.foreign_key_name_);
    } else if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to delete fk from fk name hashmap", K(ret), K(hash_ret),
               "tenant id", fk_info.tenant_id_,
               "database id", fk_info.database_id_,
               "fk name", fk_info.foreign_key_name_);
    }
  }

  return ret;
}

// Handle the situation of alter table drop fk, delete the foreign key information dropped from the hash map
int ObSchemaMgr::check_and_delete_given_fk_in_table(const ObSimpleTableSchemaV2 *replaced_table, const ObSimpleTableSchemaV2 *new_table)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(replaced_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replaced_table should not be null", K(ret));
  } else if (OB_ISNULL(new_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_table should not be null", K(ret));
  } else {
    const ObIArray<ObSimpleForeignKeyInfo> &replaced_fk_info_array = replaced_table->get_simple_foreign_key_info_array();
    const ObIArray<ObSimpleForeignKeyInfo> &new_fk_info_array = new_table->get_simple_foreign_key_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < replaced_fk_info_array.count(); ++i) {
      const ObSimpleForeignKeyInfo & fk_info = replaced_fk_info_array.at(i);
      if (!has_exist_in_array(new_fk_info_array, fk_info)) {
        if (OB_FAIL(delete_given_fk_from_mgr(fk_info))) {
          LOG_WARN("fail to delete fk from fk name hashmap", K(ret));
        }
      }
    }
  }

  return ret;
}

// Used to delete all foreign key information in a table from the member variable ForeignKeyNameMap of ObSchemaMgr
int ObSchemaMgr::delete_foreign_keys_in_table(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;

  const ObIArray<ObSimpleForeignKeyInfo> &fk_info_array = table_schema.get_simple_foreign_key_info_array();

  if (fk_info_array.empty()) {
    // If there is no foreign key in the table, do nothing
  } else {
    FOREACH_CNT_X(simple_foreign_key_info, fk_info_array, OB_SUCC(ret)) {
      if (OB_FAIL(delete_given_fk_from_mgr(*simple_foreign_key_info))) {
        LOG_WARN("fail to delete fk from table name hashmap", K(ret));
      }
    }
  }

  return ret;
}

// Get foreign_key_id according to foreign_key_name
int ObSchemaMgr::get_foreign_key_id(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const ObString &foreign_key_name,
                                    uint64_t &foreign_key_id) const
{
  int ret = OB_SUCCESS;
  foreign_key_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_INVALID_ID == tenant_id
               || OB_INVALID_ID == database_id
               || foreign_key_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(foreign_key_name));
    } else {
      ObSimpleForeignKeyInfo *simple_foreign_key_info = NULL;
      const ObForeignKeyInfoHashWrapper foreign_key_name_wrapper(tenant_id, database_id, foreign_key_name);
      int hash_ret = foreign_key_name_map_.get_refactored(foreign_key_name_wrapper, simple_foreign_key_info);
      if (OB_SUCCESS == hash_ret) {
        if (OB_ISNULL(simple_foreign_key_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(simple_foreign_key_info));
        } else {
          foreign_key_id = simple_foreign_key_info->foreign_key_id_;
        }
      } else {
        // If the table id is not found based on the library name and table name, nothing will be done
      }
    }

  return ret;
}

// Get foreign_key_info according to foreign_key_name
int ObSchemaMgr::get_foreign_key_info(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const ObString &foreign_key_name,
                                    ObSimpleForeignKeyInfo &foreign_key_info) const
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_INVALID_ID == tenant_id
               || OB_INVALID_ID == database_id
               || foreign_key_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(foreign_key_name));
    } else {
      ObSimpleForeignKeyInfo *simple_foreign_key_info = NULL;
      const ObForeignKeyInfoHashWrapper foreign_key_name_wrapper(tenant_id, database_id,
                                                                foreign_key_name);
      int hash_ret = foreign_key_name_map_.get_refactored(foreign_key_name_wrapper,
                                                          simple_foreign_key_info);
      if (OB_SUCCESS == hash_ret) {
        if (OB_ISNULL(simple_foreign_key_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(simple_foreign_key_info));
        } else {
          foreign_key_info = *simple_foreign_key_info;
          foreign_key_info.foreign_key_name_.assign(const_cast<char *>(foreign_key_name.ptr()),
                                                    foreign_key_name.length());
        }
      } else {
        // If the table id is not found based on the library name and table name, nothing will be done
      }
    }

  return ret;
}

// Used to add all constraint information in a table to the member variable constraint_name_map_ of ObSchemaMgr
int ObSchemaMgr::add_constraints_in_table(
    const ObSimpleTableSchemaV2 *new_table_schema,
    const int over_write)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(new_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_table_schema is NULL ptr", K(ret));
  } else if (new_table_schema->is_mysql_tmp_table()) {
    // In mysql mode, check constraints in non-temporary tables don't share the namespace with constraints in temporary tables
  } else {
    const common::ObIArray<ObSimpleConstraintInfo> &cst_info_array = new_table_schema->get_simple_constraint_info_array();
    if (cst_info_array.empty()) {
      // If there is no cst in the table, do nothing
    } else {
      FOREACH_CNT_X(simple_constraint_info, cst_info_array, OB_SUCC(ret)) {
        ObConstraintInfoHashWrapper constraint_name_wrapper(simple_constraint_info->tenant_id_,
                                                            simple_constraint_info->database_id_,
                                                            simple_constraint_info->constraint_name_);
        int hash_ret = constraint_name_map_.set_refactored(constraint_name_wrapper,
                                                           const_cast<ObSimpleConstraintInfo*> (simple_constraint_info),
                                                           over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
          LOG_ERROR("build cst name hashmap failed", K(ret), K(hash_ret),
                    "tenant_id", simple_constraint_info->tenant_id_,
                    "database_id", simple_constraint_info->database_id_,
                    "table_id", simple_constraint_info->table_id_,
                    "cst_id", simple_constraint_info->constraint_id_,
                    "cst_name", simple_constraint_info->constraint_name_);
        }
      }
    }
  }

  return ret;
}

// According to table_schema and constraint name, delete the specified constraint related to the corresponding table_schema
int ObSchemaMgr::delete_given_cst_from_mgr(const ObSimpleConstraintInfo &cst_info)
{
  int ret = OB_SUCCESS;

  if (cst_info.tenant_id_ == common::OB_INVALID_ID
      || cst_info.database_id_ == common::OB_INVALID_ID
      || cst_info.constraint_name_.empty()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cst_info should not be null", K(ret), K(cst_info));
  } else {
    ObConstraintInfoHashWrapper constraint_name_wrapper(cst_info.tenant_id_,
                                                        cst_info.database_id_,
                                                        cst_info.constraint_name_);
    int hash_ret = constraint_name_map_.erase_refactored(constraint_name_wrapper);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // Because there is no guarantee to refresh in strict accordance with the version order of the schema version,
      // the return value of OB_HASH_NOT_EXIST is reasonable in very special scenarios
      // At this time, the cst information in constraint_name_map_ is inconsistent with the correct foreign key information.
      // It is necessary to rebuild the constraint_name_map_ according to the correct cst information.
      is_consistent_ = false;
      LOG_WARN("fail to delete cst from cst name hashmap", K(ret), K(hash_ret),
               "tenant id", cst_info.tenant_id_,
               "database id", cst_info.database_id_,
               "cst name", cst_info.constraint_name_);
    } else if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to delete cst from cst name hashmap", K(ret), K(hash_ret),
               "tenant id", cst_info.tenant_id_,
               "database id", cst_info.database_id_,
               "cst name", cst_info.constraint_name_);
    }
  }

  return ret;
}

// Handle the situation of alter table drop cst, delete the constraint information dropped from the hash map
int ObSchemaMgr::check_and_delete_given_cst_in_table(const ObSimpleTableSchemaV2 *replaced_table, const ObSimpleTableSchemaV2 *new_table)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(replaced_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replaced_table should not be null", K(ret));
  } else if (OB_ISNULL(new_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_table should not be null", K(ret));
  } else {
    const ObIArray<ObSimpleConstraintInfo> &replaced_cst_info_array = replaced_table->get_simple_constraint_info_array();
    const ObIArray<ObSimpleConstraintInfo> &new_cst_info_array = new_table->get_simple_constraint_info_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < replaced_cst_info_array.count(); ++i) {
      const ObSimpleConstraintInfo & cst_info = replaced_cst_info_array.at(i);
      if (!has_exist_in_array(new_cst_info_array, cst_info)) {
        if (OB_FAIL(delete_given_cst_from_mgr(cst_info))) {
          LOG_WARN("fail to delete cst from cst name hashmap", K(ret));
        }
      }
    }
  }

  return ret;
}

// Used to delete all constraint information in a table from the member variable ConstraintNameMap of ObSchemaMgr
int ObSchemaMgr::delete_constraints_in_table(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSimpleConstraintInfo> &cst_info_array = table_schema.get_simple_constraint_info_array();

  if (table_schema.is_mysql_tmp_table()) {
    // In mysql mode, check constraints in non-temporary tables don't share namespace with constraints in temporary tables
  } else if (cst_info_array.empty()) {
    // If there are no constraint in the table, do nothing
  } else {
    FOREACH_CNT_X(simple_constraint_info, cst_info_array, OB_SUCC(ret)) {
      if (OB_FAIL(delete_given_cst_from_mgr(*simple_constraint_info))) {
        LOG_WARN("fail to delete cst from table name hashmap", K(ret));
      }
    }
  }

  return ret;
}

// Obtain constraint_id according to constraint_name
int ObSchemaMgr::get_constraint_id(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const ObString &constraint_name,
                                   uint64_t &constraint_id) const
{
  int ret = OB_SUCCESS;
  constraint_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id ||
              OB_INVALID_ID == database_id ||
              constraint_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else {
    ObSimpleConstraintInfo *simple_constraint_info = NULL;
    const ObConstraintInfoHashWrapper constraint_name_wrapper(tenant_id, database_id, constraint_name);
    int hash_ret = constraint_name_map_.get_refactored(constraint_name_wrapper, simple_constraint_info);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(simple_constraint_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(simple_constraint_info));
      } else {
        constraint_id = simple_constraint_info->constraint_id_;
      }
    } else {
      // If the table id is not found based on the library name and table name, nothing will be done
    }
  }

  return ret;
}

int ObSchemaMgr::get_constraint_info(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &constraint_name,
                                    ObSimpleConstraintInfo &constraint_info) const
{
  int ret = OB_SUCCESS;
  constraint_info.constraint_id_ = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id ||
              OB_INVALID_ID == database_id ||
              constraint_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else {
    ObSimpleConstraintInfo *simple_constraint_info = NULL;
    const ObConstraintInfoHashWrapper constraint_name_wrapper(tenant_id, database_id,
                                                              constraint_name);
    int hash_ret = constraint_name_map_.get_refactored(constraint_name_wrapper,
                                                       simple_constraint_info);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(simple_constraint_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(simple_constraint_info));
      } else {
        constraint_info = *simple_constraint_info;
        constraint_info.constraint_name_.assign(const_cast<char *>(constraint_name.ptr()),
                                                constraint_name.length());
      }
    } else {
      LOG_INFO("get constraint info failed, entry not exist", K(constraint_name));
      // If the table id is not found based on the library name and table name, nothing will be done
    }
  }

  return ret;
}

int ObSchemaMgr::get_dblink_schema(
    const uint64_t tenant_id,
    const uint64_t dblink_id,
    const ObDbLinkSchema *&dblink_schema) const
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not match", KR(ret), K(tenant_id), K(tenant_id_));
  } else {
    ret = dblink_mgr_.get_dblink_schema(dblink_id, dblink_schema);
  }
  return ret;
}
int ObSchemaMgr::get_dblink_schema(const uint64_t tenant_id, const ObString &dblink_name,
                                   const ObDbLinkSchema *&dblink_schema) const
{
  return dblink_mgr_.get_dblink_schema(tenant_id, dblink_name, dblink_schema);
}

bool ObSchemaMgr::check_schema_meta_consistent()
{
  // Check the number of foreign keys here, if not, you need to rebuild
  if (!is_consistent_) {
    // false == is_consistent, do nothing
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fk or cst info is not consistent");
  }

  if (database_infos_.count() != database_name_map_.item_count()) {
    is_consistent_ = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "database info is not consistent",
             "database_infos_count", database_infos_.count(),
             "database_name_map_item_count", database_name_map_.item_count());
  }

  if (table_infos_.count() != table_id_map_.item_count()
      || table_id_map_.item_count() !=
        (table_name_map_.item_count() +
         normal_index_name_map_.item_count() +
         aux_vp_name_map_.item_count() +
         lob_meta_infos_.count() +
         lob_piece_infos_.count() +
         hidden_table_name_map_.item_count() +
         built_in_index_name_map_.item_count())) {
    is_consistent_ = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "schema meta is not consistent, need rebuild",
             "schema_mgr version", get_schema_version(),
             "table_infos_count", table_infos_.count(),
             "table_id_map_item_count", table_id_map_.item_count(),
             "table_name_map_item_count", table_name_map_.item_count(),
             "index_name_map_item_count", normal_index_name_map_.item_count(),
             "aux_vp_name_map_item_count", aux_vp_name_map_.item_count(),
             "lob_meta_infos_count", lob_meta_infos_.count(),
             "lob_piece_infos_count", lob_piece_infos_.count(),
             "hidden_table_map count", hidden_table_name_map_.item_count(),
             "built_in_index_map count", built_in_index_name_map_.item_count());
  }

  return is_consistent_;
}

int ObSchemaMgr::rebuild_schema_meta_if_not_consistent()
{
  int ret = OB_SUCCESS;
  uint64_t fk_cnt = 0;
  uint64_t cst_cnt = 0;

  if (!check_schema_meta_consistent()) {
    LOG_WARN("schema meta is not consistent, need rebuild", K(ret));
    //
    if (OB_FAIL(rebuild_table_hashmap(fk_cnt, cst_cnt))) {
      LOG_WARN("rebuild table hashmap failed", K(ret));
    } else if (OB_FAIL(rebuild_db_hashmap())) {
      LOG_WARN("rebuild db hashmap failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // If it is inconsistent (!is_consistent_), rebuild is required, after the rebuild is over,
    // check whether fk and cst are consistent
    // If they are the same, there is no need to rebuild and check whether fk and cst are the same
    if (!is_consistent_ && (fk_cnt != foreign_key_name_map_.item_count())) {
      is_consistent_ = false;
      LOG_WARN("fk info is still not consistent after rebuild, need fixing", K(fk_cnt), K(foreign_key_name_map_.item_count()));
    } else if (!is_consistent_ && (cst_cnt != constraint_name_map_.item_count())) {
      is_consistent_ = false;
      LOG_WARN("cst info is still not consistent after rebuild, need fixing", K(cst_cnt), K(constraint_name_map_.item_count()));
    } else {
      is_consistent_ = true;
    }
    // Check whether db and table are consistent
    if (!check_schema_meta_consistent()) {
      ret = OB_DUPLICATE_OBJECT_NAME_EXIST;
      LOG_ERROR("schema meta is still not consistent after rebuild, need fixing", KR(ret), K_(tenant_id));
      LOG_DBA_ERROR(OB_DUPLICATE_OBJECT_NAME_EXIST,
                    "msg", "duplicate table/database/foreign key/constraint exist", K_(tenant_id),
                    "db_cnt", database_infos_.count(), "db_name_cnt", database_name_map_.item_count(),
                    "table_cnt", table_infos_.count(), "table_id_cnt", table_id_map_.item_count(),
                    "table_name_cnt", table_name_map_.item_count(), "index_name_cnt", normal_index_name_map_.item_count(),
                    "aux_vp_name_cnt", aux_vp_name_map_.item_count(), "lob_meta_cnt", lob_meta_infos_.count(),
                    "log_piece_cnt", lob_piece_infos_.count(), "hidden_table_cnt", hidden_table_name_map_.item_count(),
                    "built_in_index_cnt", built_in_index_name_map_.item_count(),
                    "fk_cnt", fk_cnt, "fk_name_cnt", foreign_key_name_map_.item_count(),
                    "cst_cnt", cst_cnt, "cst_name_cnt", constraint_name_map_.item_count());
      right_to_die_or_duty_to_live();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trigger_mgr_.try_rebuild_trigger_hashmap())) {
      LOG_WARN("rebuild trigger hashmap failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaMgr::del_table(const ObTenantTableId table)
{
  int ret = OB_SUCCESS;

  const ObSimpleTenantSchema *tenant_schema = NULL;
  ObSimpleTableSchemaV2 *schema_to_del = NULL;
  const uint64_t table_id = table.table_id_;
  bool is_system_table = false;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table));
  } else if (OB_FAIL(get_tenant_schema(table.tenant_id_, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret),
             "tenant_id", table.tenant_id_);
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_schema));
  }

  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, is_system_table))) {
    LOG_WARN("fail to check if table_id in tenant space", K(ret), K(table_id));
  } else if (OB_SYS_TENANT_ID == tenant_id_ || is_system_table) {
    // The system tenant cannot obtain the name_case_mode of the other tenants,
    // and the system tenant shall prevail.
    mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(get_tenant_name_case_mode(table.tenant_id_, mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", "tenant_id", table.tenant_id_, K(ret));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  }

  if (OB_FAIL((ret))) {
  } else if (OB_FAIL(table_infos_.remove_if(table,
                                            compare_with_tenant_table_id,
                                            equal_with_tenant_table_id,
                                            schema_to_del))) {
    LOG_WARN("failed to remove table schema, ",
             "tenant_id",
             table.tenant_id_,
             "table_id",
             table.table_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed table schema return NULL, ",
             "tenant_id",
             table.tenant_id_,
             "table_id",
             table.table_id_,
             K(ret));
  } else {
    if (schema_to_del->is_index_table()) {
      if (OB_FAIL(remove_aux_table(*schema_to_del))) {
        LOG_WARN("failed to remove aux table schema", K(ret), K(*schema_to_del));
      }
    } else if (schema_to_del->is_aux_vp_table()) {
      if (OB_FAIL(remove_aux_table(*schema_to_del))) {
        LOG_WARN("failed to remove aux table schema", K(ret), K(*schema_to_del));
      }
    } else if (schema_to_del->is_aux_lob_meta_table()) {
      if (OB_FAIL(remove_aux_table(*schema_to_del))) {
        LOG_WARN("failed to remove aux table schema", K(ret), K(*schema_to_del));
      }
    } else if (schema_to_del->is_aux_lob_piece_table()) {
      if (OB_FAIL(remove_aux_table(*schema_to_del))) {
        LOG_WARN("failed to remove aux table schema", K(ret), K(*schema_to_del));
      }
    } else if (schema_to_del->is_mlog_table()) {
      if (OB_FAIL(remove_aux_table(*schema_to_del))) {
        LOG_WARN("failed to remove mlog table schema", KR(ret), K(*schema_to_del));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int hash_ret = table_id_map_.erase_refactored(schema_to_del->get_table_id());
    if (OB_SUCCESS != hash_ret) {
      LOG_WARN("failed delete table from table id hashmap, ",
               "hash_ret", hash_ret,
               "table_id", schema_to_del->get_table_id());
      // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
      // and the solution is solved by rebuild logic
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    } else {
      bool is_oracle_mode = false;
      if (OB_FAIL(schema_to_del->check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
      } else if (schema_to_del->is_user_hidden_table()) {
        // when delete a hidden table, need to remove it from hidden_table_name_map_
        ObTableSchemaHashWrapper table_schema_wrapper(schema_to_del->get_tenant_id(),
                                                      schema_to_del->get_database_id(),
                                                      schema_to_del->get_session_id(),
                                                      mode,
                                                      schema_to_del->get_table_name_str());
        int hash_ret = hidden_table_name_map_.erase_refactored(table_schema_wrapper);
        LOG_WARN("failed delete table from table name hashmap, ",
                   K(ret),
                   K(hash_ret),
                   "tenant_id", schema_to_del->get_tenant_id(),
                   "database_id", schema_to_del->get_database_id(),
                   "table_name", schema_to_del->get_table_name());
        if (OB_SUCCESS != hash_ret) {
          LOG_WARN("failed delete table from table name hashmap, ",
                   K(ret),
                   K(hash_ret),
                   "tenant_id", schema_to_del->get_tenant_id(),
                   "database_id", schema_to_del->get_database_id(),
                   "table_name", schema_to_del->get_table_name());
          // 增加增量schema刷新的容错处理，此时不报错，靠rebuild逻辑解
          ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
        }
      } else if (schema_to_del->is_index_table()) {
        const bool is_built_in_index = schema_to_del->is_built_in_fts_index();
        IndexNameMap &index_name_map = get_index_name_map_(is_built_in_index);
        if (schema_to_del->is_in_recyclebin()) { // index is in recyclebin
          ObIndexSchemaHashWrapper index_schema_wrapper(schema_to_del->get_tenant_id(),
                                                        schema_to_del->get_database_id(),
                                                        common::OB_INVALID_ID,
                                                        schema_to_del->get_table_name_str());
          int hash_ret = index_name_map.erase_refactored(index_schema_wrapper);
          if (OB_SUCCESS != hash_ret) {
            LOG_WARN("failed delete index from index name hashmap, ",
                     K(ret),
                     K(hash_ret),
                     K(is_built_in_index),
                     "index_name", schema_to_del->get_table_name());
            // 增加增量schema刷新的容错处理，此时不报错，靠rebuild逻辑解
            ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
          }
        } else { // index is not in recyclebin
          if (OB_FAIL(schema_to_del->generate_origin_index_name())) {
            LOG_WARN("generate origin index name failed", K(ret), K(schema_to_del->get_table_name_str()));
          } else {
            int hash_ret = OB_SUCCESS;
            ObIndexSchemaHashWrapper cutted_index_name_wrapper(schema_to_del->get_tenant_id(),
                                                               schema_to_del->get_database_id(),
                                                               is_oracle_mode ? common::OB_INVALID_ID : schema_to_del->get_data_table_id(),
                                                               schema_to_del->get_origin_index_name_str());
            hash_ret = index_name_map.erase_refactored(cutted_index_name_wrapper);
            if (OB_SUCCESS != hash_ret) {
              LOG_WARN("failed delete index from index name hashmap, ",
                       K(ret),
                       K(hash_ret),
                       K(is_built_in_index),
                       K(schema_to_del->get_tenant_id()),
                       K(schema_to_del->get_database_id()),
                       K(schema_to_del->get_data_table_id()),
                       "index_name", schema_to_del->get_origin_index_name_str());
              // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
              // and the solution is solved by rebuild logic
              ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
            }
          }
        }
      } else if (schema_to_del->is_aux_vp_table()) {
        ObAuxVPSchemaHashWrapper aux_vp_schema_wrapper(schema_to_del->get_tenant_id(),
                                                      schema_to_del->get_database_id(),
                                                      schema_to_del->get_table_name_str());
        int hash_ret = aux_vp_name_map_.erase_refactored(aux_vp_schema_wrapper);
        if (OB_SUCCESS != hash_ret) {
          LOG_WARN("failed delete aux vp table name from aux vp table name hashmap, ",
                   K(ret),
                   K(hash_ret),
                   "aux_vp_name", schema_to_del->get_table_name());
          // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
          // and the solution is solved by rebuild logic
          ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
        }
      } else if (schema_to_del->is_aux_lob_table()) {
        // do nothing
      } else {
        ObTableSchemaHashWrapper table_schema_wrapper(schema_to_del->get_tenant_id(),
                                                      schema_to_del->get_database_id(),
                                                      schema_to_del->get_session_id(),
                                                      mode,
                                                      schema_to_del->get_table_name_str());
        int hash_ret = table_name_map_.erase_refactored(table_schema_wrapper);
        if (OB_SUCCESS != hash_ret) {
          LOG_WARN("failed delete table from table name hashmap, ",
                   K(ret),
                   K(hash_ret),
                   "tenant_id", schema_to_del->get_tenant_id(),
                   "database_id", schema_to_del->get_database_id(),
                   "table_name", schema_to_del->get_table_name());
          // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
          // and the solution is solved by rebuild logic
          ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(delete_foreign_keys_in_table(*schema_to_del))) {
            LOG_WARN("delete foreign keys info from a hash map failed", K(ret), K(*schema_to_del));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(delete_constraints_in_table(*schema_to_del))) {
            LOG_WARN("delete constraint info from a hash map failed", K(ret), K(*schema_to_del));
          }
        }
      }
    }
  }
  // ignore ret
  if (table_infos_.count() != table_id_map_.item_count()
      || table_id_map_.item_count() !=
         (table_name_map_.item_count() +
          normal_index_name_map_.item_count() +
          aux_vp_name_map_.item_count() +
          lob_meta_infos_.count() +
          lob_piece_infos_.count() +
          hidden_table_name_map_.item_count() +
          built_in_index_name_map_.item_count())) {
    LOG_WARN("table info is non-consistent",
             "table_infos_count",
             table_infos_.count(),
             "table_id_map_item_count",
             table_id_map_.item_count(),
             "table_name_map_item_count",
             table_name_map_.item_count(),
             "index_name_map_item_count",
             normal_index_name_map_.item_count(),
             "aux_vp_name_map_item_count",
             aux_vp_name_map_.item_count(),
             "lob_meta_infos_count",
             lob_meta_infos_.count(),
             "lob_piece_infos_count",
             lob_piece_infos_.count(),
             "tenant_id",
             table.tenant_id_,
             "table_id",
             table.table_id_,
             "hidden_table_map_item_count",
             hidden_table_name_map_.item_count(),
             "built_in_index_map_item_count",
             built_in_index_name_map_.item_count());
  }

  return ret;
}

int ObSchemaMgr::remove_aux_table(const ObSimpleTableSchemaV2 &schema_to_del)
{
  int ret = OB_SUCCESS;
  ObSimpleTableSchemaV2 *aux_schema_to_del = NULL;
  ObTenantTableId tenant_table_id(schema_to_del.get_tenant_id(),
                                  schema_to_del.get_table_id());
  ObTenantTableId tenant_data_table_id(schema_to_del.get_tenant_id(),
                                       schema_to_del.get_data_table_id());
  TableInfos *infos = nullptr;
  if (schema_to_del.is_index_table()) {
    infos = &index_infos_;
  } else if (schema_to_del.is_aux_vp_table()) {
    infos = &aux_vp_infos_;
  } else if (schema_to_del.is_aux_lob_meta_table()) {
    infos = &lob_meta_infos_;
  } else if (schema_to_del.is_aux_lob_piece_table()) {
    infos = &lob_piece_infos_;
  } else if (schema_to_del.is_mlog_table()) {
    infos = &mlog_infos_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid table type.", K(ret), K(schema_to_del.get_table_type()));
  }
  TableIterator iter = infos->lower_bound(tenant_data_table_id, compare_with_tenant_data_table_id);
  TableIterator dst_iter = NULL;
  bool is_stop = false;
  for (;
      iter != (infos->end()) && OB_SUCC(ret) && !is_stop;
      ++iter) {
    if (OB_ISNULL(aux_schema_to_del = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(aux_schema_to_del), K(ret));
    } else if (!(aux_schema_to_del->get_tenant_data_table_id() == tenant_data_table_id)) {
      is_stop = true;
    } else if (!(aux_schema_to_del->get_tenant_table_id() == tenant_table_id)) {
      // do-nothing
    } else {
      dst_iter = iter;
      is_stop = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(dst_iter) || OB_ISNULL(aux_schema_to_del)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dst_iter or aux_schema_to_del is NULL",
        K(dst_iter), K(aux_schema_to_del), K(ret));
    } else if (OB_FAIL(infos->remove(dst_iter, dst_iter + 1))) {
      LOG_WARN("failed to remove aux schema, ",
          "tenant_id", tenant_table_id.tenant_id_,
          "table_id", tenant_table_id.table_id_, K(ret));
    }
  }
  return ret;
}

int ObSchemaMgr::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 *&table_schema) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleTableSchemaV2 *tmp_schema = NULL;
    int hash_ret = table_id_map_.get_refactored(table_id, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        table_schema = tmp_schema;
      }
    }
  }

  return ret;
}

//table_schema->session_id = 0, This is a general situation, the schema is visible to any session;
//table_schema->session_id<>0, schema is a) temp table; or b) The visibility of the table in the process of querying
//  the table creation is as follows:
// For the internal session (parameter value session_id = OB_INVALID_ID), only b# is visible, a# is not visible,
// because the temporary table T may exist between different sessions; (create temporary table as select not support yet);
// For non-internal sessions (including session_id = 0), judge according to session->session_id == table_schema->session_id;
// There may be problems, such as the SQL statement executed by ObMySQLProxy.write in the internal session, when it involves
// a temporary table or incorrectly uses a non-temporary table with the same name or reports an error that cannot be found;
// See the code for specific judgments ObTableSchemaHashWrapper::operator ==
int ObSchemaMgr::get_table_schema(
  const uint64_t tenant_id,
  const uint64_t database_id,
  // ObSchemaGetterGuard session_id, default value=0, initialized in ObSql::generate_stmt, if=OB_INVALID_ID is internal session
  const uint64_t session_id,
  const ObString &table_name,
  const ObSimpleTableSchemaV2 *&table_schema) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleTableSchemaV2 *tmp_schema = NULL;
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (is_sys_tenant(tenant_id)) {
      mode = OB_ORIGIN_AND_INSENSITIVE;
    } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
      LOG_WARN("fail to get_tenant_name_case_mode", K(tenant_id), KR(ret));
    } else if (OB_NAME_CASE_INVALID == mode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid case mode", KR(ret), K(mode));
    }
    if (OB_SUCC(ret)) {
      const ObTableSchemaHashWrapper table_name_wrapper(tenant_id, database_id, session_id, mode, table_name);
      int hash_ret = table_name_map_.get_refactored(table_name_wrapper, tmp_schema);
      if (OB_SUCCESS == hash_ret) {
        if (OB_ISNULL(tmp_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), K(table_name_wrapper));
        } else {
          table_schema = tmp_schema;
        }
      } else if (OB_HASH_NOT_EXIST == hash_ret && 0 != session_id && OB_INVALID_ID != session_id) {
        // If session_id != 0, the search just now is based on the possible match of the temporary table.
        // If it is not found, then it will be searched according to session_id = 0, which is the normal table.
        const ObTableSchemaHashWrapper table_name_wrapper1(tenant_id, database_id, 0, mode, table_name);
        hash_ret = table_name_map_.get_refactored(table_name_wrapper1, tmp_schema);
        if (OB_SUCCESS == hash_ret) {
          if (OB_ISNULL(tmp_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", KR(ret), K(table_name_wrapper1));
          } else {
            table_schema = tmp_schema;
          }
        }
      }
      // restrict creating duplicate table with existed inner table
      if (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
         bool is_system_table = false;
         if (OB_FAIL(ObSysTableChecker::is_sys_table_name(tenant_id, database_id, table_name, is_system_table))) {
           LOG_WARN("fail to check if table is system table", KR(ret), K(tenant_id), K(database_id), K(table_name));
         } else if (is_system_table) {
           // Inner table's ObTableSchemaHashWrapper is stored with OB_ORIGIN_AND_INSENSITIVE. Actually,
           // 1. For inner table in mysql database, comparision is insensitive.
           // 2. For inner table in oracle database, comparision is sensitive.
           const ObTableSchemaHashWrapper table_name_wrapper2(tenant_id, database_id,
                                                              0, OB_ORIGIN_AND_INSENSITIVE, table_name);
           hash_ret = table_name_map_.get_refactored(table_name_wrapper2, tmp_schema);
           if (OB_SUCCESS == hash_ret) {
             if (OB_ISNULL(tmp_schema)) {
               ret = OB_ERR_UNEXPECTED;
               LOG_WARN("NULL ptr", KR(ret), K(table_name_wrapper2));
             } else {
               table_schema = tmp_schema;
             }
           }
         } else {
           // not system table
         }
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_hidden_table_schema(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &table_name,
    const ObSimpleTableSchemaV2 *&table_schema) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleTableSchemaV2 *tmp_schema = NULL;
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
      LOG_WARN("fail to get_tenant_name_case_mode", K(tenant_id), K(ret));
    } else if (OB_NAME_CASE_INVALID == mode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid case mode", K(ret), K(mode));
    }
    if (OB_SUCC(ret)) {
      const ObTableSchemaHashWrapper table_name_wrapper(tenant_id, database_id, 0, mode, table_name);
      int hash_ret = hidden_table_name_map_.get_refactored(table_name_wrapper, tmp_schema);
      if (OB_SUCCESS == hash_ret) {
        if (OB_ISNULL(tmp_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
        } else {
          table_schema = tmp_schema;
        }
      }
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_INVALID_INDEX_NAME);

int ObSchemaMgr::get_index_schema(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &table_name,
  const ObSimpleTableSchemaV2 *&table_schema,
  const bool is_built_in/* = false*/) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObSimpleTableSchemaV2 *tmp_schema = NULL;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    const IndexNameMap &index_name_map = get_index_name_map_(is_built_in);
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret));
    } else if (is_recyclebin_database_id(database_id)) { // in recyclebin
      const ObIndexSchemaHashWrapper index_name_wrapper(
          tenant_id, database_id, common::OB_INVALID_ID, table_name);
      int hash_ret = index_name_map.get_refactored(index_name_wrapper, tmp_schema);
      if (OB_SUCCESS == hash_ret) {
        if (OB_ISNULL(tmp_schema)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("NULL ptr", K(ret), K(tenant_id), K(table_name), K(is_built_in), KP(tmp_schema));
        } else {
         table_schema = tmp_schema;
        }
      }
    } else { // not in recyclebin
      // FIXME: oracle mode not support drop user/database to recyclebin yet, now
      // can determine whether the index is in the recycle bin based on database_id
      ObString cutted_index_name;
      uint64_t data_table_id = ObSimpleTableSchemaV2::extract_data_table_id_from_index_name(table_name);
      if (OB_UNLIKELY(ERRSIM_INVALID_INDEX_NAME)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("turn on ERRSIM_INVALID_INDEX_NAME", KR(ret));
      } else if (OB_INVALID_ID == data_table_id) {
        // nothing to do, need to go on and it will get a empty ptr of dst table_schema
      } else if (OB_FAIL(ObSimpleTableSchemaV2::get_index_name(table_name, cutted_index_name))) {
        if (OB_SCHEMA_ERROR == ret) {
          // If the input table_name of the function does not conform to the prefixed index name format of'__idx_DataTableId_IndexName',
          // an empty table schema pointer should be returned, and no error should be reported, so reset the error code to OB_SUCCESS
          ret = OB_SUCCESS;
        }
        LOG_WARN("fail to get index name", K(ret));
      } else {
        // Notice that, operation on mysql_db table when compat_mode equals to lib::Worker::CompatMode::ORACLE is mysql mode.
        const bool is_oracle_mode = lib::Worker::CompatMode::ORACLE == compat_mode
                                     && !is_mysql_sys_database_id(database_id);
        const ObIndexSchemaHashWrapper cutted_index_name_wrapper(tenant_id, database_id,
            is_oracle_mode ? common::OB_INVALID_ID : data_table_id, cutted_index_name);
        int hash_ret = index_name_map.get_refactored(cutted_index_name_wrapper, tmp_schema);
        if (OB_SUCCESS == hash_ret) {
          if (OB_ISNULL(tmp_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), K(is_built_in), K(tmp_schema));
          } else {
            table_schema = tmp_schema;
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaMgr::deep_copy_index_name_map(
    common::ObIAllocator &allocator,
    ObIndexNameMap &index_name_cache)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
      tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K_(tenant_id));
  } else {
    // index_name_cache will destory or not init, so sub_map_mem_size should be set first
    // to reduce dynamic memory allocation and avoid error.
    (void) index_name_cache.set_sub_map_mem_size(normal_index_name_map_.get_sub_map_mem_size());
    if (OB_FAIL(index_name_cache.init())) {
      LOG_WARN("init index name cache failed", KR(ret));
    }
  }
  for (int64_t sub_map_id = 0;
       OB_SUCC(ret) && sub_map_id < normal_index_name_map_.get_sub_map_count();
       sub_map_id++) {
    IndexNameMap::iterator it = normal_index_name_map_.begin(sub_map_id);
    IndexNameMap::iterator end = normal_index_name_map_.end(sub_map_id);
    for (; OB_SUCC(ret) && it != end; ++it) {
      const ObSimpleTableSchemaV2 *index_schema = *it;
      void *buf = NULL;
      ObIndexNameInfo *index_name_info = NULL;
      uint64_t data_table_id = OB_INVALID_ID;
      uint64_t database_id = OB_INVALID_ID;
      ObString index_name;
      if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema is null", KR(ret));
      } else if (FALSE_IT(database_id = index_schema->get_database_id())) {
      } else if (OB_UNLIKELY(!is_recyclebin_database_id(database_id)
                 && index_schema->get_origin_index_name_str().empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid index schema", KR(ret), KPC(index_schema));
      } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIndexNameInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc index name info", KR(ret));
      } else if (FALSE_IT(index_name_info = new (buf) ObIndexNameInfo())) {
      } else if (OB_FAIL(index_name_info->init(allocator, *index_schema))) {
        LOG_WARN("fail to init index name info", KR(ret), KPC(index_schema));
      } else if (is_recyclebin_database_id(database_id)) {
        data_table_id = OB_INVALID_ID;
        index_name = index_name_info->get_index_name();
      } else {
        data_table_id = (is_oracle_mode && !is_mysql_sys_database_id(database_id)) ?
                        OB_INVALID_ID : index_name_info->get_data_table_id();
        index_name = index_name_info->get_original_index_name();
      }
      if (OB_SUCC(ret)) {
        int overwrite = 0;
        ObIndexSchemaHashWrapper index_name_wrapper(index_name_info->get_tenant_id(),
                                                    database_id,
                                                    data_table_id,
                                                    index_name);
        if (OB_FAIL(index_name_cache.set_refactored(
            index_name_wrapper, index_name_info, overwrite))) {
          LOG_WARN("fail to set refactored", KR(ret), KPC(index_name_info));
          if (OB_HASH_EXIST == ret) {
            ObIndexNameInfo **exist_index_info = index_name_cache.get(index_name_wrapper);
            if (OB_NOT_NULL(exist_index_info) && OB_NOT_NULL(*exist_index_info)) {
              FLOG_ERROR("duplicated index info exist", KR(ret),
                         KPC(index_name_info), KPC(*exist_index_info));
            }
          }
        }
      }
    } // end for
  } // end for
  return ret;
}

int ObSchemaMgr::get_table_schema(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const uint64_t session_id,
                                  const ObString &table_name,
                                  const bool is_index,
                                  const ObSimpleTableSchemaV2 *&table_schema,
                                  const bool with_hidden_flag/*false*/,
                                  const bool is_built_in_index/*false*/) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(with_hidden_flag)) {
    ret = get_hidden_table_schema(tenant_id, database_id, table_name, table_schema);
  } else {
    if (!is_index) {
      ret = get_table_schema(tenant_id, database_id, session_id, table_name, table_schema);
    } else {
      ret = get_index_schema(tenant_id, database_id, table_name, table_schema, is_built_in_index);
    }
  }
  return ret;
}

int ObSchemaMgr::get_tenant_schemas(
    ObIArray<const ObSimpleTenantSchema *> &tenant_schemas) const
{
  int ret = OB_SUCCESS;
  tenant_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get tenant ids from non-sys schema mgr not allowed",
             K(ret), K_(tenant_id));;
  } else {
    for (ConstTenantIterator iter = tenant_infos_.begin();
        OB_SUCC(ret) && iter != tenant_infos_.end(); ++iter) {
      ObSimpleTenantSchema *tenant_schema = *iter;
      if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_schema is nnull", K(ret));
      } else if (OB_FAIL(tenant_schemas.push_back(tenant_schema))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_tenant_ids(ObIArray<uint64_t> &tenant_ids) const
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get tenant ids from non-sys schema mgr not allowed",
             K(ret), K_(tenant_id));
  } else {
    for (ConstTenantIterator iter = tenant_infos_.begin();
        OB_SUCC(ret) && iter != tenant_infos_.end(); ++iter) {
      ObSimpleTenantSchema *tenant_schema = *iter;
      if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_schema is nnull", K(ret));
      } else if (OB_FAIL(tenant_ids.push_back(tenant_schema->get_tenant_id()))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_available_tenant_ids(ObIArray<uint64_t> &tenant_ids) const
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get tenant ids from non-sys schema mgr not allowed",
             K(ret), K_(tenant_id));;
  } else {
    for (ConstTenantIterator iter = tenant_infos_.begin();
        OB_SUCC(ret) && iter != tenant_infos_.end(); ++iter) {
      ObSimpleTenantSchema *tenant_schema = *iter;
      if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_schema is nnull", K(ret));
      } else if (TENANT_STATUS_NORMAL != tenant_schema->get_status()) {
        // tenant is creating or is dropping
      } else if (OB_FAIL(tenant_ids.push_back(tenant_schema->get_tenant_id()))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }

  return ret;
}

// The system tenant caches the simple schema of all tenant system tables, which can be accessed directly.
// For obtaining the simple table schema of the user tenant after the schema is split, it is necessary to obtain
// the schema of the system table from the system tenant and the schema of the ordinary table from the user tenant.
// TODO: check tenant schema mgr
#define GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(SCHEMA, SCHEMA_TYPE, TENANT_SCHEMA_ID_TYPE, SCHEMA_ITER) \
  int ObSchemaMgr::get_##SCHEMA##_schemas_in_tenant(                             \
      const uint64_t tenant_id,                                                  \
      ObIArray<const SCHEMA_TYPE *> &schema_array) const                         \
  {                                                                              \
    int ret = OB_SUCCESS;                                                        \
    if (!check_inner_stat()) {                                                   \
      ret = OB_NOT_INIT;                                                         \
      LOG_WARN("not init", K(ret));                                              \
    } else if (OB_INVALID_ID == tenant_id) {                                     \
      ret = OB_INVALID_ARGUMENT;                                                 \
      LOG_WARN("invalid argument", K(ret), K(tenant_id));                        \
    } else {                                                                     \
      const SCHEMA_TYPE *schema = NULL;                                          \
      TENANT_SCHEMA_ID_TYPE tenant_schema_id_lower(tenant_id, OB_MIN_ID);        \
      SCHEMA_ITER iter = SCHEMA##_infos_.lower_bound(tenant_schema_id_lower,     \
          compare_with_tenant_##SCHEMA##_id);                                    \
      bool is_stop = false;                                                      \
      for (; OB_SUCC(ret) && iter != SCHEMA##_infos_.end() && !is_stop; iter++) { \
        if (OB_ISNULL(schema = *iter)) {                                         \
          ret = OB_ERR_UNEXPECTED;                                               \
          LOG_WARN("NULL ptr", K(ret), KP(schema));                              \
        } else if (tenant_id != schema->get_tenant_id()) {                       \
          is_stop = true;                                                        \
        } else if (OB_FAIL(schema_array.push_back(schema))) {                    \
          LOG_WARN("failed to push back "#SCHEMA" schema", K(ret));              \
        }                                                                        \
      }                                                                          \
    }                                                                            \
    return ret;                                                                  \
  }
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(user, ObSimpleUserSchema, ObTenantUserId, ConstUserIterator);
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(database, ObSimpleDatabaseSchema, ObTenantDatabaseId, ConstDatabaseIterator);
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(tablegroup, ObSimpleTablegroupSchema, ObTenantTablegroupId, ConstTablegroupIterator);

#undef GET_SCHEMAS_IN_TENANT_FUNC_DEFINE

// The system tenant caches the simple schema of all tenant system tables, which can be accessed directly.
// For obtaining the simple table schema of the ordinary tenant after the schema is split, it is necessary to obtain
// the schema of the system table from the system tenant and the schema of the ordinary table from the ordinary tenant.
#define GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(DST_SCHEMA)                  \
  int ObSchemaMgr::get_table_schemas_in_##DST_SCHEMA(                            \
      const uint64_t tenant_id,                                                  \
      const uint64_t dst_schema_id,                                              \
      ObIArray<const ObSimpleTableSchemaV2 *> &schema_array) const               \
  {                                                                              \
    int ret = OB_SUCCESS;                                                        \
    schema_array.reset();                                                        \
    if (!check_inner_stat()) {                                                   \
      ret = OB_NOT_INIT;                                                         \
      LOG_WARN("not init", K(ret));                                              \
    } else if (OB_INVALID_ID == tenant_id                                        \
               || OB_INVALID_ID == dst_schema_id) {                              \
      ret = OB_INVALID_ARGUMENT;                                                 \
      LOG_WARN("invalid argument", K(ret), K(tenant_id),                         \
               #DST_SCHEMA"_id", dst_schema_id);                                 \
    } else if (OB_INVALID_TENANT_ID != tenant_id_ \
               && OB_SYS_TENANT_ID != tenant_id_ \
               && tenant_id_ != tenant_id) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id)); \
    } else {                                                                     \
      const ObSimpleTableSchemaV2 *schema = NULL;                                \
      ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);               \
      ConstTableIterator iter = table_infos_.lower_bound(tenant_table_id_lower,  \
          compare_with_tenant_table_id);                                         \
      bool is_stop = false;                                                      \
      for (; OB_SUCC(ret) && iter != table_infos_.end() && !is_stop; iter++) {   \
        if (OB_ISNULL(schema = *iter)) {                                         \
          ret = OB_ERR_UNEXPECTED;                                               \
          LOG_WARN("NULL ptr", K(ret), KP(schema));                              \
        } else if (tenant_id != schema->get_tenant_id()) {                       \
          is_stop = true;                                                        \
        } else if (dst_schema_id == schema->get_##DST_SCHEMA##_id()) {           \
          if (OB_FAIL(schema_array.push_back(schema))) {                         \
            LOG_WARN("failed to push back table schema", K(ret));                \
          }                                                                      \
        }                                                                        \
      }                                                                          \
    }                                                                            \
    return ret;                                                                  \
  }
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(database);
// GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablespace);

#undef GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE

int ObSchemaMgr::get_primary_table_schema_in_tablegroup(
      const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      const ObSimpleTableSchemaV2 *&primary_table_schema) const
  {
    int ret = OB_SUCCESS;
    primary_table_schema = NULL;
    if (OB_UNLIKELY(!check_inner_stat())) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", KR(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
               || OB_INVALID_ID == tablegroup_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_id),
               "tablegroup_id", tablegroup_id);
    } else if (OB_UNLIKELY(tenant_id_ != tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant_id not matched", KR(ret), K(tenant_id), K_(tenant_id));
    } else {
      const ObSimpleTableSchemaV2 *schema = NULL;
      ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);
      ConstTableIterator iter = table_infos_.lower_bound(tenant_table_id_lower,
          compare_with_tenant_table_id);
      bool is_stop = false;
      for (; OB_SUCC(ret) && iter != table_infos_.end() && !is_stop; iter++) {
        if (OB_ISNULL(schema = *iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), KP(schema));
        } else if (OB_UNLIKELY(tenant_id != schema->get_tenant_id())) {
          is_stop = true;
        } else if (tablegroup_id == schema->get_tablegroup_id()) {
          if (schema->is_user_table()
            || schema->is_mysql_tmp_table()
            || schema->is_sys_table()) {
            primary_table_schema = schema;
            is_stop = true;
          }
        }
      }
    }
    return ret;
  }

int ObSchemaMgr::get_table_schemas_in_tablegroup(
    const uint64_t tenant_id,
    const uint64_t dst_schema_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &schema_array) const
{
  int ret = OB_SUCCESS;
  schema_array.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
              || OB_INVALID_ID == dst_schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id),
              "tablegroup_id", dst_schema_id);
  } else if (OB_INVALID_TENANT_ID != tenant_id_
              && OB_SYS_TENANT_ID != tenant_id_
              && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    const ObSimpleTableSchemaV2 *schema = NULL;
    ObTenantTableId tenant_table_id_lower(tenant_id, OB_MIN_ID);
    ConstTableIterator iter = table_infos_.lower_bound(tenant_table_id_lower,
        compare_with_tenant_table_id);
    bool is_stop = false;
    for (; OB_SUCC(ret) && iter != table_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), KP(schema));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (dst_schema_id == schema->get_tablegroup_id()) {
        if (schema->is_user_table()
          || schema->is_mysql_tmp_table()
          || schema->is_sys_table()) {
          if (OB_FAIL(schema_array.push_back(schema))) {
            LOG_WARN("failed to push back table schema", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_table_schemas_in_tenant(
    const uint64_t tenant_id,
    ObIArray<const ObSimpleTableSchemaV2*> &schema_array) const
{
  int ret = OB_SUCCESS;
  schema_array.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    const ObSimpleTableSchemaV2 *schema = NULL;
    ObTenantTableId tenant_schema_id_lower(tenant_id, OB_MIN_ID);
    ConstTableIterator iter = table_infos_.lower_bound(tenant_schema_id_lower,
        compare_with_tenant_table_id);
    bool is_stop = false;
    for (; OB_SUCC(ret) && iter != table_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr",  K(ret), KP(schema));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (OB_FAIL(schema_array.push_back(schema))) {
        LOG_WARN("failed to push back SCHEMA schema", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaMgr::check_database_exists_in_tablegroup(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    bool &not_empty) const
{
  int ret = OB_SUCCESS;
  not_empty = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ObTenantDatabaseId tenant_database_id_lower(tenant_id, OB_MIN_ID);
    ConstDatabaseIterator iter =
        database_infos_.lower_bound(tenant_database_id_lower, compare_with_tenant_database_id);
    bool is_stop = false;
    const ObSimpleDatabaseSchema *tmp_schema = NULL;
    for (; OB_SUCC(ret) && iter != database_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(tmp_schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(tmp_schema), K(ret));
      } else if (tmp_schema->get_tenant_id() != tenant_id) {
        is_stop = true;
      } else if (tmp_schema->get_default_tablegroup_id() != tablegroup_id) {
        // do-nothing
      } else {
        is_stop = true;
        not_empty = true;
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_aux_schemas(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &aux_schemas,
    const ObTableType table_type) const
{
  int ret = OB_SUCCESS;
  aux_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_table_id));
  } else if (OB_INVALID_TENANT_ID != tenant_id_
             && OB_SYS_TENANT_ID != tenant_id_
             && tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not matched", K(ret), K(tenant_id), K(tenant_id_), K(data_table_id));
  } else {
    const TableInfos *infos = nullptr;
    if (table_type == USER_INDEX) {
      infos = &index_infos_;
    } else if (table_type == AUX_VERTIAL_PARTITION_TABLE) {
      infos = &aux_vp_infos_;
    } else if (table_type == AUX_LOB_META) {
      infos = &lob_meta_infos_;
    } else if (table_type == AUX_LOB_PIECE) {
      infos = &lob_piece_infos_;
    } else if (table_type == MATERIALIZED_VIEW_LOG) {
      infos = &mlog_infos_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid table type.", K(ret), K(table_type));
    }
    if (OB_SUCC(ret)) {
      // TODO: make aux_vp_infos_ added for mv
      ObTenantTableId tenant_data_table_id(tenant_id, data_table_id);
      TableIterator iter = infos->lower_bound(tenant_data_table_id, compare_with_tenant_data_table_id);
      const ObSimpleTableSchemaV2 *aux_schema = NULL;
      bool will_break = false;
      for (; iter != (infos->end()) && OB_SUCC(ret) && !will_break; ++iter) {
        if (OB_ISNULL(aux_schema = *iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(aux_schema), K(ret));
        } else if (!(aux_schema->get_tenant_data_table_id() == tenant_data_table_id)) {
          will_break = true;
        } else if (OB_FAIL(aux_schemas.push_back(aux_schema))) {
          LOG_WARN("push back aux_vp schema failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_non_sys_table_ids(
    const uint64_t tenant_id,
    ObIArray<uint64_t> &non_sys_table_ids) const
{
  int ret = OB_SUCCESS;
  non_sys_table_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(tenant_id));
  } else {
    const ObSimpleTableSchemaV2 *schema = NULL;
    ObTenantTableId tenant_table_id_lower(tenant_id,
                                          OB_MAX_SYS_TABLE_ID);
    ConstTableIterator iter = table_infos_.lower_bound(
                              tenant_table_id_lower,
                              compare_with_tenant_table_id);
    bool is_stop = false;
    uint64_t table_id = OB_INVALID_ID;
    for (; OB_SUCC(ret) && iter != table_infos_.end() && !is_stop; iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), K(tenant_id), KP(schema));
      } else if (FALSE_IT(table_id = schema->get_table_id())) {
      } else if (tenant_id != schema->get_tenant_id()
                 || table_id >= OB_MAX_SYS_VIEW_ID) {
        is_stop = true;
      } else if (is_inner_table(table_id) && !is_sys_table(table_id)) {
        if (OB_FAIL(non_sys_table_ids.push_back(table_id))) {
          LOG_WARN("failed to push back table id", KR(ret), K(tenant_id), K(table_id));
        }
      }
    } // end for
  }
  return ret;
}


int ObSchemaMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    #define DEL_SCHEMA(SCHEMA, SCHEMA_TYPE, TENANT_SCHEMA_ID_TYPE, SCHEMA_ITER)    \
      if (OB_SUCC(ret)) {                                                          \
        ObArray<const SCHEMA_TYPE *> schemas;                                      \
        const SCHEMA_TYPE *schema = NULL;                                          \
        TENANT_SCHEMA_ID_TYPE tenant_schema_id_lower(tenant_id, OB_MIN_ID);        \
        SCHEMA_ITER iter = SCHEMA##_infos_.lower_bound(tenant_schema_id_lower,     \
            compare_with_tenant_##SCHEMA##_id);                                    \
        bool is_stop = false;                                                      \
        for (; OB_SUCC(ret) && iter != SCHEMA##_infos_.end() && !is_stop; iter++) { \
          if (OB_ISNULL(schema = *iter)) {                                         \
            ret = OB_ERR_UNEXPECTED;                                               \
            LOG_WARN("NULL ptr", K(ret), KP(schema));                              \
          } else if (tenant_id != schema->get_tenant_id()) {                       \
            is_stop = true;                                                        \
          } else if (OB_FAIL(schemas.push_back(schema))) {                         \
            LOG_WARN("push back "#SCHEMA" schema failed", K(ret));                 \
          }                                                                        \
        }                                                                          \
        if (OB_SUCC(ret)) {                                                        \
          FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {                           \
            TENANT_SCHEMA_ID_TYPE tenant_schema_id(tenant_id,                      \
              (*schema)->get_##SCHEMA##_id());                                     \
            if (OB_FAIL(del_##SCHEMA(tenant_schema_id))) {                         \
              LOG_WARN("del "#SCHEMA" failed",                                     \
                       "tenant_id", tenant_schema_id.tenant_id_,                   \
                       #SCHEMA"_id", tenant_schema_id.SCHEMA##_id_,                \
                       K(ret));                                                    \
            }                                                                      \
          }                                                                        \
        }                                                                          \
      }
    DEL_SCHEMA(user, ObSimpleUserSchema, ObTenantUserId, ConstUserIterator);
    DEL_SCHEMA(database, ObSimpleDatabaseSchema, ObTenantDatabaseId, ConstDatabaseIterator);
    DEL_SCHEMA(tablegroup, ObSimpleTablegroupSchema, ObTenantTablegroupId, ConstTablegroupIterator);
    DEL_SCHEMA(table, ObSimpleTableSchemaV2, ObTenantTableId, ConstTableIterator);
    #undef DEL_SCHEMA

    if (OB_SUCC(ret)) {
      if (OB_FAIL(outline_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del schemas in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(synonym_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del synonym in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(package_mgr_.del_package_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del package in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(routine_mgr_.del_routine_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del routine in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(trigger_mgr_.del_trigger_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del trigger in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(udf_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del udf in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(udt_mgr_.del_udt_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del udt in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(sequence_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del sequence in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(audit_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del audit in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(sys_variable_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del sys variable in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(label_se_policy_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del label security policy schema in tenant failed", K(ret));
      } else if (OB_FAIL(label_se_component_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del label security component schema in tenant failed", K(ret));
      } else if (OB_FAIL(label_se_label_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del label security label schema in tenant failed", K(ret));
      } else if (OB_FAIL(label_se_user_level_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del label security user level schema in tenant failed", K(ret));
      } else if (OB_FAIL(tablespace_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del sequence in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(profile_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del profile in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(dblink_mgr_.del_dblink_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del dblink in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(directory_mgr_.del_directory_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del directory in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(context_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del context in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(mock_fk_parent_table_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del mock_fk_parent_table in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(rls_policy_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del rls_policy in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(rls_group_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del rls_group in tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(rls_context_mgr_.del_schemas_in_tenant(tenant_id))) {
        LOG_WARN("del rls_context in tenant failed", K(ret), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObSchemaMgr::get_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t tenant_schema_count = tenant_infos_.size();
    schema_count = tenant_schema_count + user_infos_.size() + database_infos_.size()
                   + tablegroup_infos_.size() + table_infos_.size() + index_infos_.size()
                   + aux_vp_infos_.size() + lob_meta_infos_.size() + lob_piece_infos_.size()
                   + mlog_infos_.size();
    int64_t outline_schema_count = 0;
    int64_t routine_schema_count = 0;
    int64_t priv_schema_count = 0;
    int64_t synonym_schema_count = 0;
    int64_t package_schema_count = 0;
    int64_t trigger_schema_count = 0;
    int64_t udf_schema_count = 0;
    int64_t udt_schema_count = 0;
    int64_t sequence_schema_count = 0;
    int64_t sys_variable_schema_count = 0;
    int64_t keystore_schema_count = 0;
    int64_t label_se_policy_count = 0;
    int64_t label_se_component_count = 0;
    int64_t label_se_label_count = 0;
    int64_t label_se_user_level_count = 0;
    int64_t tablespace_schema_count = 0;
    int64_t profile_schema_count = 0;
    int64_t audit_schema_count = 0;
    int64_t dblink_schema_count = 0;
    int64_t directory_schema_count = 0;
    int64_t context_schema_count = 0;
    int64_t mock_fk_parent_table_schema_count = 0;
    int64_t rls_policy_schema_count = 0;
    int64_t rls_group_schema_count = 0;
    int64_t rls_context_schema_count = 0;
    if (OB_FAIL(outline_mgr_.get_outline_schema_count(outline_schema_count))) {
      LOG_WARN("get_outline_schema_count failed", K(ret));
    } else if (OB_FAIL(routine_mgr_.get_routine_schema_count(routine_schema_count))) {
      LOG_WARN("get_routine_schema_count failed", K(ret));
    } else if (OB_FAIL(priv_mgr_.get_priv_schema_count(priv_schema_count))) {
      LOG_WARN("get_priv_schema_count failed", K(ret));
    } else if (OB_FAIL(synonym_mgr_.get_synonym_schema_count(synonym_schema_count))) {
      LOG_WARN("get_synonym_mgr_count failed", K(ret));
    } else if (OB_FAIL(package_mgr_.get_package_schema_count(package_schema_count))) {
      LOG_WARN("get_package_mgr_count failed", K(ret));
    } else if (OB_FAIL(trigger_mgr_.get_trigger_schema_count(trigger_schema_count))) {
      LOG_WARN("get_trigger_mgr_count failed", K(ret));
    } else if (OB_FAIL(udf_mgr_.get_udf_schema_count(udf_schema_count))) {
      LOG_WARN("get_udf_mgr_count failed", K(ret));
    } else if (OB_FAIL(udt_mgr_.get_udt_schema_count(udt_schema_count))) {
      LOG_WARN("get_udt_mgr_count failed", K(ret));
    } else if (OB_FAIL(sequence_mgr_.get_sequence_schema_count(sequence_schema_count))) {
      LOG_WARN("get_sequence_mgr_count failed", K(ret));
    } else if (OB_FAIL(sys_variable_mgr_.get_sys_variable_schema_count(sys_variable_schema_count))) {
      LOG_WARN("get_sys_variable_mgr_count failed", K(ret));
    } else if (OB_FAIL(keystore_mgr_.get_keystore_schema_count(keystore_schema_count))) {
      LOG_WARN("get_keystore_schema_count failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(label_se_policy_mgr_.get_schema_count(label_se_policy_count))) {
      LOG_WARN("get schema count failed", K(ret));
    } else if (OB_FAIL(label_se_component_mgr_.get_schema_count(label_se_component_count))) {
      LOG_WARN("get schema count failed", K(ret));
    } else if (OB_FAIL(label_se_label_mgr_.get_schema_count(label_se_label_count))) {
      LOG_WARN("get schema count failed", K(ret));
    } else if (OB_FAIL(label_se_user_level_mgr_.get_schema_count(label_se_user_level_count))) {
      LOG_WARN("get schema count failed", K(ret));
    } else if (OB_FAIL(tablespace_mgr_.get_tablespace_schema_count(tablespace_schema_count))) {
      LOG_WARN("get_tablespace_mgr_count failed", K(ret));
    } else if (OB_FAIL(profile_mgr_.get_schema_count(profile_schema_count))) {
      LOG_WARN("get profile schema count failed", K(ret));
    } else if (OB_FAIL(audit_mgr_.get_audit_schema_count(audit_schema_count))) {
      LOG_WARN("get_audit_schema_count failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(dblink_mgr_.get_dblink_schema_count(dblink_schema_count))) {
      LOG_WARN("get dblink schema count failed", K(ret));
    } else if (OB_FAIL(directory_mgr_.get_directory_schema_count(directory_schema_count))) {
      LOG_WARN("get directory schema count failed", K(ret));
    } else if (OB_FAIL(context_mgr_.get_context_schema_count(context_schema_count))) {
      LOG_WARN("get context schema count failed", K(ret));
    } else if (OB_FAIL(mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schema_count(mock_fk_parent_table_schema_count))) {
      LOG_WARN("get context schema count failed", K(ret));
    } else if (OB_FAIL(rls_policy_mgr_.get_schema_count(rls_policy_schema_count))) {
      LOG_WARN("get rls_policy schema count failed", K(ret));
    } else if (OB_FAIL(rls_group_mgr_.get_schema_count(rls_group_schema_count))) {
      LOG_WARN("get rls_group schema count failed", K(ret));
    } else if (OB_FAIL(rls_context_mgr_.get_schema_count(rls_context_schema_count))) {
      LOG_WARN("get rls_context schema count failed", K(ret));
    } else {
      schema_count += (outline_schema_count + routine_schema_count + priv_schema_count
                       + synonym_schema_count + package_schema_count
                       + udf_schema_count + udt_schema_count + sequence_schema_count
                       + sys_variable_schema_count + keystore_schema_count
                       + label_se_policy_count + label_se_component_count
                       + label_se_label_count + label_se_user_level_count
                       + tablespace_schema_count
                       + trigger_schema_count
                       + profile_schema_count
                       + audit_schema_count
                       + dblink_schema_count
                       + directory_schema_count
                       + rls_policy_schema_count
                       + rls_group_schema_count
                       + rls_context_schema_count
                       + sys_variable_schema_count
                       + context_schema_count
                       + mock_fk_parent_table_schema_count
                      );
    }
  }
  return ret;
}

int ObSchemaMgr::get_tenant_name_case_mode(const uint64_t tenant_id, ObNameCaseMode &mode) const
{
  int ret = OB_SUCCESS;
  mode = OB_NAME_CASE_INVALID;

  const ObSimpleSysVariableSchema *sys_variable = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
    LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id));
  } else if (NULL == sys_variable) {
    // do-nothing
  } else {
    mode = sys_variable->get_name_case_mode();
  }

  return ret;
}

int ObSchemaMgr::get_tenant_read_only(const uint64_t tenant_id, bool &read_only) const
{
  int ret = OB_SUCCESS;

  read_only = false;
  const ObSimpleSysVariableSchema *sys_variable = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
    LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id));
  } else if (NULL == sys_variable) {
    ret = OB_TENANT_NOT_EXIST;
  } else {
    read_only = sys_variable->get_read_only();
  }

  return ret;
}


int ObSchemaMgr::deal_with_db_rename(
  const ObSimpleDatabaseSchema &old_db_schema,
  const ObSimpleDatabaseSchema &new_db_schema)
{
  int ret = OB_SUCCESS;
  if (old_db_schema.get_database_id() != new_db_schema.get_database_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(old_db_schema), K(new_db_schema));
  } else {
    if (old_db_schema.get_database_name_str() != new_db_schema.get_database_name_str()) {
      LOG_INFO("db renamed", K(old_db_schema), K(new_db_schema));
      ObDatabaseSchemaHashWrapper db_name_wrapper(old_db_schema.get_tenant_id(),
                                                  old_db_schema.get_name_case_mode(),
                                                  old_db_schema.get_database_name_str());
      int hash_ret = database_name_map_.erase_refactored(db_name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        LOG_WARN("failed to delete database from database name hashmap",
                K(ret), K(hash_ret), K(old_db_schema));
        // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
        // and the solution is solved by rebuild logic
        ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
      }
    }
  }
  return ret;
}

int ObSchemaMgr::deal_with_change_table_state(const ObSimpleTableSchemaV2 &old_table_schema,
                                              const ObSimpleTableSchemaV2 &new_table_schema)
{
  int ret = OB_SUCCESS;
  bool is_system_table = false;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(
                      old_table_schema.get_table_id(), is_system_table))) {
    LOG_WARN("fail to check if table_id in tenant space",
              K(ret), "table_id", old_table_schema.get_table_id());
  } else if (OB_SYS_TENANT_ID == tenant_id_ || is_system_table) {
    mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(get_tenant_name_case_mode(old_table_schema.get_tenant_id(), mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", "tenant_id", old_table_schema.get_tenant_id(), K(ret));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  }
  if (OB_FAIL(ret)) {
  } else if (old_table_schema.is_user_hidden_table()
            && !new_table_schema.is_user_hidden_table()) {
    // hidden table to non-hidden table
    ObTableSchemaHashWrapper table_name_wrapper(old_table_schema.get_tenant_id(),
                                                old_table_schema.get_database_id(),
                                                old_table_schema.get_session_id(),
                                                mode,
                                                old_table_schema.get_table_name_str());
    int hash_ret = hidden_table_name_map_.erase_refactored(table_name_wrapper);
    if (OB_SUCCESS != hash_ret) {
      LOG_WARN("fail to delete table from table name hashmap",
                K(ret), K(hash_ret), K(old_table_schema.get_table_name_str()));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
  } else if (!old_table_schema.is_user_hidden_table()
            && new_table_schema.is_user_hidden_table()) {
    // non-hidden table to hidden table
    if (old_table_schema.is_index_table()) {
      bool is_oracle_mode = false;
      const bool is_built_in_index = old_table_schema.is_built_in_fts_index();
      IndexNameMap &index_name_map = get_index_name_map_(is_built_in_index);
      if (OB_FAIL(old_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
      } else if (old_table_schema.is_in_recyclebin()) { // index is in recyclebin
        ObIndexSchemaHashWrapper index_name_wrapper(old_table_schema.get_tenant_id(),
                                                    old_table_schema.get_database_id(),
                                                    common::OB_INVALID_ID,
                                                    old_table_schema.get_table_name_str());
        int hash_ret = index_name_map.erase_refactored(index_name_wrapper);
        if (OB_SUCCESS != hash_ret) {
          LOG_WARN("fail to delete index from index name hashmap",
                    K(ret), K(hash_ret), K(is_built_in_index), K(old_table_schema.get_table_name_str()));
          // increase the fault-tolerant processing of incremental schema refresh
          ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
        }
      } else { // index is in not recyclebin
        ObString cutted_index_name;
        if (OB_FAIL(old_table_schema.get_index_name(cutted_index_name))) {
          LOG_WARN("fail to get index name", K(ret));
        } else {
          ObIndexSchemaHashWrapper cutted_index_name_wrapper(old_table_schema.get_tenant_id(),
                                                             old_table_schema.get_database_id(),
                                                             is_oracle_mode ? common::OB_INVALID_ID : old_table_schema.get_data_table_id(),
                                                             cutted_index_name);
          int hash_ret = index_name_map.erase_refactored(cutted_index_name_wrapper);
          if (OB_SUCCESS != hash_ret) {
            LOG_WARN("failed delete index from index name hashmap, ",
                      K(ret), K(hash_ret), K(is_built_in_index), K(cutted_index_name));
            // increase the fault-tolerant processing of incremental schema refresh
            ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
          }
        }
      }
    } else if (old_table_schema.is_aux_vp_table()) {
      ObAuxVPSchemaHashWrapper aux_vp_name_wrapper(old_table_schema.get_tenant_id(),
                                                    old_table_schema.get_database_id(),
                                                    old_table_schema.get_table_name_str());
      int hash_ret = aux_vp_name_map_.erase_refactored(aux_vp_name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        LOG_WARN("fail to delete aux vp table from aux_vp name hashmap",
                  K(ret), K(hash_ret), K(old_table_schema.get_table_name_str()));
        // increase the fault-tolerant processing of incremental schema refresh
        ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
      }
    } else if (old_table_schema.is_aux_lob_table()) {
      // do nothing
    } else {
      ObTableSchemaHashWrapper table_name_wrapper(old_table_schema.get_tenant_id(),
                                                  old_table_schema.get_database_id(),
                                                  old_table_schema.get_session_id(),
                                                  mode,
                                                  old_table_schema.get_table_name_str());
      int hash_ret = table_name_map_.erase_refactored(table_name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        LOG_WARN("fail to delete table from table name hashmap",
                  K(ret), K(hash_ret), K(old_table_schema.get_table_name_str()));
        // increase the fault-tolerant processing of incremental schema refresh
        ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
      }
    }
  } else {
    /* do nothing */
  }
  return ret;
}

int ObSchemaMgr::deal_with_table_rename(
  const ObSimpleTableSchemaV2 &old_table_schema,
  const ObSimpleTableSchemaV2 &new_table_schema)
{
  int ret = OB_SUCCESS;

  if (old_table_schema.get_table_id() != new_table_schema.get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(old_table_schema),
        K(new_table_schema));
  } else {
    const uint64_t old_database_id = old_table_schema.get_database_id();
    const uint64_t new_database_id = new_table_schema.get_database_id();
    const ObString &old_table_name = old_table_schema.get_table_name_str();
    const ObString &new_table_name = new_table_schema.get_table_name_str();
    bool is_rename = (old_table_name != new_table_name) || (old_database_id != new_database_id);
    // if the old table is a hidden table, the hidden table will not be added to the map, need skip
    // if change a non-hidden table to a hidden table, skip it here and handle it in
    // deal_with_change_table_state_to_hidden
    if (!is_rename
        || old_table_schema.is_user_hidden_table()
        || (!old_table_schema.is_user_hidden_table()
        && new_table_schema.is_user_hidden_table())) {
      // do-nothing
    } else {
      LOG_INFO("table renamed",
               K(old_database_id),
               K(old_table_name),
               K(new_database_id),
               K(new_table_name));
      bool is_system_table = false;
      if (old_table_schema.is_index_table()) {
        const bool is_built_in_index = old_table_schema.is_built_in_fts_index();
        bool is_oracle_mode = false;
        IndexNameMap &index_name_map = get_index_name_map_(is_built_in_index);
        if (OB_FAIL(old_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
          LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
        } else if (old_table_schema.is_in_recyclebin()) { // index is in recyclebin
          ObIndexSchemaHashWrapper index_name_wrapper(old_table_schema.get_tenant_id(),
                                                      old_table_schema.get_database_id(),
                                                      common::OB_INVALID_ID,
                                                      old_table_schema.get_table_name_str());
          int hash_ret = index_name_map.erase_refactored(index_name_wrapper);
          if (OB_SUCCESS != hash_ret) {
            LOG_WARN("fail to delete index from index name hashmap",
                     K(ret), K(hash_ret), K(is_built_in_index), K(old_table_name));
            // 增加增量schema刷新的容错处理，此时不报错，靠rebuild逻辑解
            ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
          }
        } else { // index is not in recyclebin
          ObString cutted_index_name;
          if (OB_FAIL(old_table_schema.get_index_name(cutted_index_name))) {
            LOG_WARN("fail to get index name", K(ret));
          } else {
            ObIndexSchemaHashWrapper cutted_index_name_wrapper(old_table_schema.get_tenant_id(),
                                                               old_table_schema.get_database_id(),
                                                               is_oracle_mode ? common::OB_INVALID_ID : old_table_schema.get_data_table_id(),
                                                               cutted_index_name);
            int hash_ret = index_name_map.erase_refactored(cutted_index_name_wrapper);
            if (OB_SUCCESS != hash_ret) {
              LOG_WARN("failed delete index from index name hashmap, ",
                       K(ret), K(hash_ret), K(is_built_in_index), K(cutted_index_name));
              // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
              // and the solution is solved by rebuild logic
              ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
            }
          }
        }
      } else if (old_table_schema.is_aux_vp_table()) {
        ObAuxVPSchemaHashWrapper aux_vp_name_wrapper(old_table_schema.get_tenant_id(),
                                                     old_table_schema.get_database_id(),
                                                     old_table_schema.get_table_name_str());
        int hash_ret = aux_vp_name_map_.erase_refactored(aux_vp_name_wrapper);
        if (OB_SUCCESS != hash_ret) {
          LOG_WARN("fail to delete aux vp table from aux_vp name hashmap",
                   K(ret), K(hash_ret), K(old_table_name));
          // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
          // and the solution is solved by rebuild logic
          ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
        }
      } else if (old_table_schema.is_aux_lob_table()) {
        // do nothing
      } else {
        ObNameCaseMode mode = OB_NAME_CASE_INVALID;
        if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(
                           old_table_schema.get_table_id(), is_system_table))) {
          LOG_WARN("fail to check if table_id in tenant space",
                   K(ret), "table_id", old_table_schema.get_table_id());
        } else if (OB_SYS_TENANT_ID == tenant_id_ || is_system_table) {
          // The system tenant cannot obtain the name_case_mode of the other tenants, and the system tenant shall prevail.
          mode = OB_ORIGIN_AND_INSENSITIVE;
        } else if (OB_FAIL(get_tenant_name_case_mode(old_table_schema.get_tenant_id(), mode))) {
          LOG_WARN("fail to get_tenant_name_case_mode", "tenant_id", old_table_schema.get_tenant_id(), K(ret));
        } else if (OB_NAME_CASE_INVALID == mode) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid case mode", K(ret), K(mode));
        }
        if (OB_SUCC(ret)) {
          ObTableSchemaHashWrapper table_name_wrapper(old_table_schema.get_tenant_id(),
                                                      old_table_schema.get_database_id(),
                                                      old_table_schema.get_session_id(),
                                                      mode,
                                                      old_table_schema.get_table_name_str());
          int hash_ret = table_name_map_.erase_refactored(table_name_wrapper);
          if (OB_SUCCESS != hash_ret) {
            LOG_WARN("fail to delete table from table name hashmap",
                     K(ret), K(hash_ret), K(old_table_name));
            // Increase the fault-tolerant processing of incremental schema refresh, no error is reported at this time,
            // and the solution is solved by rebuild logic
            ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaMgr::rebuild_db_hashmap()
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    database_name_map_.clear();
    int over_write = 0;
    for (ConstDatabaseIterator iter = database_infos_.begin();
        iter != database_infos_.end() && OB_SUCC(ret); ++iter) {
      ObSimpleDatabaseSchema *database_schema = *iter;
      if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database schema is NULL", K(ret));
      } else {
        ObDatabaseSchemaHashWrapper db_name_wrapper(database_schema->get_tenant_id(),
                                                    database_schema->get_name_case_mode(),
                                                    database_schema->get_database_name());
        int hash_ret = database_name_map_.set_refactored(db_name_wrapper,
                                                         database_schema,
                                                         over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
          LOG_ERROR("build database name hashmap failed", K(ret), K(hash_ret), K(*database_schema));
        }
      }
    }
  }
  return ret;
}

int ObSchemaMgr::rebuild_table_hashmap(uint64_t &fk_cnt, uint64_t &cst_cnt)
{
  int ret = OB_SUCCESS;
  fk_cnt = 0;
  cst_cnt = 0;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    table_id_map_.clear();
    table_name_map_.clear();
    normal_index_name_map_.clear();
    aux_vp_name_map_.clear();
    foreign_key_name_map_.clear();
    constraint_name_map_.clear();
    hidden_table_name_map_.clear();
    built_in_index_name_map_.clear();
    ObSimpleTableSchemaV2 *table_schema = NULL;
    // It is expected that OB_HASH_EXIST should not appear in the rebuild process
    int over_write = 0;
    int tmp_ret = OB_SUCCESS;
    ObSimpleTableSchemaV2 *exist_schema = NULL;

    for (ConstTableIterator iter = table_infos_.begin();
        iter != table_infos_.end() && OB_SUCC(ret);
        ++iter) {
      table_schema = *iter;
      exist_schema = NULL;
      LOG_TRACE("table_info is", "table_id", table_schema->get_table_id());

      if (OB_ISNULL(table_schema) || !table_schema->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is unexpected", K(ret), K(table_schema));
      } else {
        int hash_ret = table_id_map_.set_refactored(table_schema->get_table_id(),
                                                    table_schema,
                                                    over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build table id hashmap failed", K(ret), K(hash_ret),
                   "table_id", table_schema->get_table_id());
        } else if (table_schema->is_user_hidden_table()) {
          ObTableSchemaHashWrapper table_name_wrapper(table_schema->get_tenant_id(),
                                                      table_schema->get_database_id(),
                                                      table_schema->get_session_id(),
                                                      table_schema->get_name_case_mode(),
                                                      table_schema->get_table_name_str());
          hash_ret = hidden_table_name_map_.set_refactored(table_name_wrapper, table_schema,
                                                           over_write);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
            tmp_ret = hidden_table_name_map_.get_refactored(table_name_wrapper, exist_schema);
            LOG_ERROR("build hidden table name hashmap failed",
                      KR(ret), KR(hash_ret), K(tmp_ret),
                      "exist_table_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_id() : OB_INVALID_ID,
                      "exist_database_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_database_id() : OB_INVALID_ID,
                      "exist_session_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_session_id() : OB_INVALID_ID,
                      "exist_name_case_mode", OB_NOT_NULL(exist_schema) ? exist_schema->get_name_case_mode() : OB_NAME_CASE_INVALID,
                      "exist_table_name", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_name() : "",
                      "table_id", table_schema->get_table_id(),
                      "databse_id", table_schema->get_database_id(),
                      "session_id", table_schema->get_session_id(),
                      "name_case_mode", table_schema->get_name_case_mode(),
                      "table_name", table_schema->get_table_name());
          }
        } else {
          bool is_oracle_mode = false;
          if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
            LOG_WARN("fail to check if tenant mode is oracle mode", K(ret));
          } else if (table_schema->is_index_table()) {
            LOG_TRACE("index is", "table_id", table_schema->get_table_id(),
                      "database_id", table_schema->get_database_id(),
                      "table_name", table_schema->get_table_name_str());
            const bool is_built_in_index = table_schema->is_built_in_fts_index();
            IndexNameMap &index_name_map = get_index_name_map_(is_built_in_index);
            // oracle mode and index is not in recyclebin
            if (table_schema->is_in_recyclebin()) {
              ObIndexSchemaHashWrapper index_name_wrapper(table_schema->get_tenant_id(),
                                                          table_schema->get_database_id(),
                                                          common::OB_INVALID_ID,
                                                          table_schema->get_table_name_str());
              hash_ret = index_name_map.set_refactored(index_name_wrapper, table_schema, over_write);
              if (OB_SUCCESS != hash_ret) {
                ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
                tmp_ret = index_name_map.get_refactored(index_name_wrapper, exist_schema);
                LOG_ERROR("build index name hashmap failed",
                          KR(ret), KR(hash_ret), K(tmp_ret), K(is_built_in_index),
                          "exist_table_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_id() : OB_INVALID_ID,
                          "exist_database_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_database_id() : OB_INVALID_ID,
                          "index_name",  OB_NOT_NULL(exist_schema) ? exist_schema->get_table_name() : "",
                          "table_id", table_schema->get_table_id(),
                          "databse_id", table_schema->get_database_id(),
                          "index_name", table_schema->get_table_name());
              }
            } else { // index is not in recyclebin
              if (OB_FAIL(table_schema->generate_origin_index_name())) {
                LOG_WARN("generate origin index name failed", K(ret), K(table_schema->get_table_name_str()));
              } else {
                ObIndexSchemaHashWrapper cutted_index_name_wrapper(table_schema->get_tenant_id(),
                                                                   table_schema->get_database_id(),
                                                                   is_oracle_mode ? common::OB_INVALID_ID : table_schema->get_data_table_id(),
                                                                   table_schema->get_origin_index_name_str());
                hash_ret = index_name_map.set_refactored(cutted_index_name_wrapper, table_schema, over_write);
                if (OB_SUCCESS != hash_ret) {
                  ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
                  tmp_ret = index_name_map.get_refactored(cutted_index_name_wrapper, exist_schema);
                  LOG_ERROR("build index name hashmap failed",
                            KR(ret), KR(hash_ret), K(tmp_ret), K(is_built_in_index),
                            "exist_table_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_id() : OB_INVALID_ID,
                            "exist_database_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_database_id() : OB_INVALID_ID,
                            "index_name",  OB_NOT_NULL(exist_schema) ? exist_schema->get_origin_index_name_str() : "",
                            "table_id", table_schema->get_table_id(),
                            "databse_id", table_schema->get_database_id(),
                            "index_name", table_schema->get_origin_index_name_str());
                }
              }
            }
          } else if (table_schema->is_aux_vp_table()) {
            LOG_TRACE("aux_vp is", "table_id", table_schema->get_table_id(),
                      "database_id", table_schema->get_database_id(),
                      "table_name", table_schema->get_table_name_str());
            ObAuxVPSchemaHashWrapper aux_vp_name_wrapper(table_schema->get_tenant_id(),
                                                         table_schema->get_database_id(),
                                                         table_schema->get_table_name_str());
            hash_ret = aux_vp_name_map_.set_refactored(aux_vp_name_wrapper, table_schema, over_write);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
              tmp_ret = aux_vp_name_map_.get_refactored(aux_vp_name_wrapper, exist_schema);
              LOG_ERROR("build aux vp name hashmap failed",
                        KR(ret), KR(hash_ret), K(tmp_ret),
                        "exist_table_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_id() : OB_INVALID_ID,
                        "exist_database_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_database_id() : OB_INVALID_ID,
                        "index_name",  OB_NOT_NULL(exist_schema) ? exist_schema->get_table_name() : "",
                        "table_id", table_schema->get_table_id(),
                        "databse_id", table_schema->get_database_id(),
                        "aux_vp_name", table_schema->get_table_name());
            }
          } else if (table_schema->is_aux_lob_table()) {
            // do nothing
          } else {
            LOG_TRACE("table is", "table_id", table_schema->get_table_id(),
                      "database_id", table_schema->get_database_id(),
                     "table_name", table_schema->get_table_name_str());
            ObTableSchemaHashWrapper table_name_wrapper(table_schema->get_tenant_id(),
                                                        table_schema->get_database_id(),
                                                        table_schema->get_session_id(),
                                                        table_schema->get_name_case_mode(),
                                                        table_schema->get_table_name_str());
            hash_ret = table_name_map_.set_refactored(table_name_wrapper, table_schema, over_write);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : OB_ERR_UNEXPECTED;
              tmp_ret = table_name_map_.get_refactored(table_name_wrapper, exist_schema);
              LOG_ERROR("build table name hashmap failed",
                        K(ret), K(hash_ret), K(tmp_ret),
                        "exist_table_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_id() : OB_INVALID_ID,
                        "exist_database_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_database_id() : OB_INVALID_ID,
                        "exist_session_id", OB_NOT_NULL(exist_schema) ? exist_schema->get_session_id() : OB_INVALID_ID,
                        "exist_name_case_mode", OB_NOT_NULL(exist_schema) ? exist_schema->get_name_case_mode() : OB_NAME_CASE_INVALID,
                        "exist_table_name", OB_NOT_NULL(exist_schema) ? exist_schema->get_table_name() : "",
                        "table_id", table_schema->get_table_id(),
                        "databse_id", table_schema->get_database_id(),
                        "session_id", table_schema->get_session_id(),
                        "name_case_mode", table_schema->get_name_case_mode(),
                        "table_name", table_schema->get_table_name());
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(add_foreign_keys_in_table(table_schema->get_simple_foreign_key_info_array(), over_write))) {
                LOG_WARN("add foreign keys info to a hash map failed", K(ret), K(table_schema->get_table_name_str()));
              } else {
                fk_cnt += table_schema->get_simple_foreign_key_info_array().count();
              }
            }
            if (OB_SUCC(ret)) {
              if (table_schema->is_mysql_tmp_table()) {
                // check constraints in non-temporary tables don't share namespace with constraints in temporary tables, do nothing
              } else if (OB_FAIL(add_constraints_in_table(table_schema, over_write))) {
                LOG_WARN("add constraint info to a hash map failed", K(ret), K(table_schema->get_table_name_str()));
              } else {
                cst_cnt += table_schema->get_simple_constraint_info_array().count();
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

// only use in oracle mode
int ObSchemaMgr::get_idx_schema_by_origin_idx_name(const uint64_t tenant_id,
                                                   const uint64_t database_id,
                                                   const common::ObString &ori_index_name,
                                                   const ObSimpleTableSchemaV2 *&table_schema) const
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || ori_index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(ori_index_name));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else if (lib::Worker::CompatMode::ORACLE != compat_mode
             || is_mysql_sys_database_id(database_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compat_mode is not oracle mode",
             KR(ret), K(tenant_id), K(database_id), K(compat_mode));
  } else {
    ObSimpleTableSchemaV2 *tmp_schema = NULL;
    const ObIndexSchemaHashWrapper index_name_wrapper(
        tenant_id, database_id, common::OB_INVALID_ID, ori_index_name);
    lib::CompatModeGuard g(lib::Worker::CompatMode::ORACLE);
    int hash_ret = normal_index_name_map_.get_refactored(index_name_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        table_schema = tmp_schema;
      }
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      // do nothing
    }
  }
  return ret;
}

void ObSchemaMgr::dump() const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t schema_count = 0;
  int64_t schema_size = 0;
  tmp_ret = get_schema_count(schema_count);
  ret = OB_SUCC(ret) ? tmp_ret : ret;
  tmp_ret = get_schema_size(schema_size);
  LOG_INFO("[SCHEMA_STATISTICS] dump schema_mgr",
           K(tmp_ret),
           K_(tenant_id),
           K_(schema_version),
           K(schema_count),
           K(schema_size));

  #define DUMP_SCHEMA(SCHEMA, SCHEMA_TYPE, SCHEMA_ITER)   \
    {                                                     \
      for (SCHEMA_ITER iter = SCHEMA##_infos_.begin();    \
          iter != SCHEMA##_infos_.end(); iter++) {        \
        SCHEMA_TYPE *schema = *iter;                      \
        if (NULL == schema) {                             \
          LOG_INFO("NULL ptr", KP(schema));                \
        } else {                                          \
          LOG_INFO(#SCHEMA, K(*schema));                  \
        }                                                 \
      }                                                   \
    }
//  DUMP_SCHEMA(tenant, ObSimpleTenantSchema, ConstTenantIterator);
//  DUMP_SCHEMA(user, ObSimpleUserSchema, ConstUserIterator);
//  DUMP_SCHEMA(database, ObSimpleDatabaseSchema, ConstDatabaseIterator);
//  DUMP_SCHEMA(tablegroup, ObSimpleTablegroupSchema, ConstTablegroupIterator);
//  DUMP_SCHEMA(table, ObSimpleTableSchemaV2, ConstTableIterator);
//  DUMP_SCHEMA(index, ObSimpleTableSchemaV2, ConstTableIterator);
  #undef DUMP_SCHEMA
}

int ObSchemaMgr::get_schema_size(int64_t &total_size) const
{
  int ret = OB_SUCCESS;
  ObArray<ObSchemaStatisticsInfo> schema_infos;
  total_size = 0;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_schema_statistics(schema_infos))) {
    LOG_WARN("fail to get schema size", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_infos.size(); i++) {
      ObSchemaStatisticsInfo &schema_statistics = schema_infos.at(i);
      if (schema_statistics.schema_type_ < TENANT_SCHEMA
          || schema_statistics.schema_type_ >= OB_MAX_SCHEMA) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema type", K(ret), K(schema_statistics));
      } else {
        total_size += schema_statistics.size_;
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_schema_statistics(common::ObIArray<ObSchemaStatisticsInfo> &schema_infos) const
{
  int ret = OB_SUCCESS;
  ObSchemaStatisticsInfo schema_info;
  schema_infos.reset();
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant_statistics(schema_info))) {
    LOG_WARN("fail to get tenant statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(get_user_statistics(schema_info))) {
    LOG_WARN("fail to get user statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(get_database_statistics(schema_info))) {
    LOG_WARN("fail to get database statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(get_tablegroup_statistics(schema_info))) {
    LOG_WARN("fail to get tablegroup statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(get_table_statistics(schema_info))) {
    LOG_WARN("fail to get table statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(outline_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get outline statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(routine_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get routine statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(priv_mgr_.get_schema_statistics(TABLE_PRIV, schema_info))) {
    LOG_WARN("fail to get table priv statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(priv_mgr_.get_schema_statistics(ROUTINE_PRIV, schema_info))) {
    LOG_WARN("fail to get table priv statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(priv_mgr_.get_schema_statistics(DATABASE_PRIV, schema_info))) {
    LOG_WARN("fail to get database priv statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(synonym_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get synonym statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(package_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get package statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(trigger_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get trigger statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(udf_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get udf statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(udt_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get udt statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(udf_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get udf statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(sequence_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get sequence statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(sys_variable_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get sys variable statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(label_se_policy_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get label_se_policy statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(label_se_component_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get label_se_component statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(label_se_label_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get label_se_label  statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(label_se_user_level_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get label_se_user_level statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(keystore_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get keystore statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(tablespace_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get keystore statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(profile_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get profile statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(audit_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get keystore statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(priv_mgr_.get_schema_statistics(SYS_PRIV, schema_info))) {
    LOG_WARN("fail to get system priv statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(priv_mgr_.get_schema_statistics(OBJ_PRIV, schema_info))) {
    LOG_WARN("fail to get obj priv statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(priv_mgr_.get_schema_statistics(COLUMN_PRIV, schema_info))) {
    LOG_WARN("fail to get column priv statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(dblink_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get dblink statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(directory_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get directory statistics", K(ret));
  } else if (OB_FAIL(rls_policy_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get rls_policy statistics", K(ret));
  } else if (OB_FAIL(rls_group_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get rls_group statistics", K(ret));
  } else if (OB_FAIL(rls_context_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get rls_context statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(context_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get context statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  } else if (OB_FAIL(mock_fk_parent_table_mgr_.get_schema_statistics(schema_info))) {
    LOG_WARN("fail to get mock_fk_parent_table statistics", K(ret));
  } else if (OB_FAIL(schema_infos.push_back(schema_info))) {
    LOG_WARN("fail to push back schema statistics", K(ret), K(schema_info));
  }
  return ret;
}

int ObSchemaMgr::get_audit_schema(const uint64_t tenant_id,
                                  const ObSAuditType audit_type,
                                  const uint64_t owner_id,
                                  const ObSAuditOperationType operation_type,
                                  const ObSAuditSchema *&ret_audit_schema) const
{
  int ret = OB_SUCCESS;
  const ObSAuditSchema *audit_schema = NULL;
  ret_audit_schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(audit_mgr_.get_audit_schema(tenant_id, audit_type, owner_id, operation_type, audit_schema))) {
    LOG_WARN("get audit schema failed", K(ret), K(tenant_id), K(audit_type), K(owner_id), K(operation_type));
  } else if (OB_NOT_NULL(audit_schema)) {
    ret_audit_schema = audit_schema;
  }
  LOG_DEBUG("get specified audit schema", KPC(audit_schema), K(ret), K(tenant_id),
            K(audit_type), K(owner_id), K(operation_type));
  return ret;
}

int ObSchemaMgr::get_audit_schemas_in_tenant(const uint64_t tenant_id,
    const ObSAuditType audit_type, const uint64_t owner_id,
    common::ObIArray<const ObSAuditSchema *> &schema_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(audit_mgr_.get_audit_schemas_in_tenant(tenant_id,
                                                            audit_type,
                                                            owner_id,
                                                            schema_array))) {
    LOG_WARN("get audit schema failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaMgr::get_audit_schemas_in_tenant(const uint64_t tenant_id,
    common::ObIArray<const ObSAuditSchema *> &schema_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(audit_mgr_.get_audit_schemas_in_tenant(tenant_id,
                                                            schema_array))) {
    LOG_WARN("get audit schema failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaMgr::check_allow_audit(
    const uint64_t tenant_id,
    ObSAuditType &audit_type,
    const uint64_t owner_id,
    ObSAuditOperationType &operation_type,
    const int return_code,
    uint64_t &audit_id,
    bool &is_allow_audit) const
{
  int ret = OB_SUCCESS;
  const ObSAuditSchema *audit_schema = NULL;
  is_allow_audit = false;
  audit_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(audit_mgr_.get_audit_schema(tenant_id,
                                                 audit_type,
                                                 owner_id,
                                                 operation_type,
                                                 audit_schema))) {
    LOG_WARN("get audit schema failed", K(ret));
  } else if (NULL == audit_schema
             && AUDIT_STMT == audit_type
             && AUDIT_OP_ALTER_SYSTEM <= operation_type
             && operation_type <= AUDIT_OP_UPDATE_TABLE) {
    //try all_stmt
    if (OB_FAIL(audit_mgr_.get_audit_schema(tenant_id,
                                            audit_type,
                                            owner_id,
                                            AUDIT_OP_ALL_STMTS,
                                            audit_schema))) {
      LOG_WARN("get audit schema failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != audit_schema) {
    is_allow_audit = audit_schema->is_access_audit(return_code);
    audit_id = audit_schema->get_audit_id();
    audit_type = audit_schema->get_audit_type();
    operation_type = audit_schema->get_operation_type();
  }
  LOG_DEBUG("check_allow_audit", K(audit_schema), K(ret), K(tenant_id), K(audit_type),
                                 K(owner_id), K(operation_type), K(audit_id), K(is_allow_audit));
  return ret;
}

int ObSchemaMgr::check_allow_audit_by_default(
    const uint64_t tenant_id,
    ObSAuditType &audit_type,
    ObSAuditOperationType &operation_type,
    const int return_code,
    uint64_t &audit_id,
    bool &is_allow_audit) const
{
  int ret = OB_SUCCESS;
  const ObSAuditSchema *audit_schema = NULL;
  is_allow_audit = false;
  audit_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(audit_mgr_.get_audit_schema(tenant_id,
                                                 audit_type,
                                                 OB_AUDIT_MOCK_USER_ID,
                                                 operation_type,
                                                 audit_schema))) {
    LOG_WARN("get audit schema failed", K(ret));
  } else if (NULL == audit_schema
             && AUDIT_STMT_ALL_USER == audit_type
             && AUDIT_OP_ALTER_SYSTEM <= operation_type
             && operation_type <= AUDIT_OP_UPDATE_TABLE) {
    if (OB_FAIL(audit_mgr_.get_audit_schema(tenant_id,
                                            audit_type,
                                            OB_AUDIT_MOCK_USER_ID,
                                            AUDIT_OP_ALL_STMTS,
                                            audit_schema))) {
      LOG_WARN("get audit schema failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != audit_schema) {
    is_allow_audit = audit_schema->is_access_audit(return_code);
    audit_id = audit_schema->get_audit_id();
    audit_type = audit_schema->get_audit_type();
    operation_type = audit_schema->get_operation_type();
  }
  LOG_DEBUG("check_allow_audit_by_default", K(audit_schema), K(ret), K(tenant_id), K(audit_type),
                                            K(operation_type), K(audit_id), K(is_allow_audit));
  return ret;
}

int ObSchemaMgr::get_tenant_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = TENANT_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = tenant_infos_.size();
    for (ConstTenantIterator it = tenant_infos_.begin(); OB_SUCC(ret) && it != tenant_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_user_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = USER_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = user_infos_.size();
    for (ConstUserIterator it = user_infos_.begin(); OB_SUCC(ret) && it != user_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_database_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = DATABASE_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = database_infos_.size();
    for (ConstDatabaseIterator it = database_infos_.begin(); OB_SUCC(ret) && it != database_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_tablegroup_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = TABLEGROUP_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = tablegroup_infos_.size();
    for (ConstTablegroupIterator it = tablegroup_infos_.begin(); OB_SUCC(ret) && it != tablegroup_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

int ObSchemaMgr::get_table_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = TABLE_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = table_infos_.size() + index_infos_.size() + aux_vp_infos_.size() + lob_meta_infos_.size() + lob_piece_infos_.size() + mlog_infos_.size();
    for (ConstTableIterator it = table_infos_.begin(); OB_SUCC(ret) && it != table_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
    for (ConstTableIterator it = index_infos_.begin(); OB_SUCC(ret) && it != index_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
    for (ConstTableIterator it = aux_vp_infos_.begin(); OB_SUCC(ret) && it != aux_vp_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
    for (ConstTableIterator it = lob_meta_infos_.begin(); OB_SUCC(ret) && it != lob_meta_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
    for (ConstTableIterator it = lob_piece_infos_.begin(); OB_SUCC(ret) && it != lob_piece_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
    for (ConstTableIterator it = mlog_infos_.begin(); OB_SUCC(ret) && it != mlog_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
