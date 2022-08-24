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

#include "share/schema/ob_schema_getter_guard.h"

#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_worker.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_get_compat_mode.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_synonym_mgr.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/resolver/ob_schema_checker.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
using namespace common;
using namespace observer;

namespace share {
namespace schema {
ObSchemaMgrInfo::~ObSchemaMgrInfo()
{
  mgr_handle_.reset();
}

void ObSchemaMgrInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  snapshot_version_ = OB_INVALID_VERSION;
  schema_mgr_ = NULL;
  mgr_handle_.reset();
  schema_status_.reset();
}

ObSchemaMgrInfo& ObSchemaMgrInfo::operator=(const ObSchemaMgrInfo& other)
{
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    snapshot_version_ = other.snapshot_version_;
    schema_mgr_ = other.schema_mgr_;
    mgr_handle_ = other.mgr_handle_;
    schema_status_ = other.schema_status_;
  }
  return *this;
}

ObSchemaMgrInfo::ObSchemaMgrInfo(const ObSchemaMgrInfo& other)
{
  *this = other;
}

ObSchemaGetterGuard::ObSchemaGetterGuard()
    : schema_service_(NULL),
      snapshot_version_(OB_INVALID_VERSION),
      session_id_(0),
      mgr_(NULL),
      mock_allocator_(ObModIds::OB_MOCK_SCHEMA),
      mock_gts_schema_(nullptr),
      mock_simple_gts_schema_(nullptr),
      schema_helper_(mock_allocator_),
      tenant_id_(OB_INVALID_TENANT_ID),
      local_allocator_(ObModIds::OB_SCHEMA_MGR_INFO_ARRAY),
      schema_mgr_infos_(DEFAULT_TENANT_NUM, ModulePageAllocator(local_allocator_)),
      schema_objs_(DEFAULT_RESERVE_SIZE, ModulePageAllocator(local_allocator_)),
      schema_guard_type_(INVALID_SCHEMA_GUARD_TYPE),
      is_standby_cluster_(false),
      is_inited_(false)
{}

ObSchemaGetterGuard::~ObSchemaGetterGuard()
{
  // Destruct handles_ will reduce reference count automatically.
}

int ObSchemaGetterGuard::init(const bool is_standby_cluster)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (is_standby_cluster && OB_FAIL(schema_helper_.init())) {
    LOG_WARN("fail to init schema helper", K(ret));
  } else {
    is_standby_cluster_ = is_standby_cluster;
    is_inited_ = true;
  }
  return ret;
}

int ObSchemaGetterGuard::reset()
{
  int ret = OB_SUCCESS;
  schema_service_ = NULL;
  snapshot_version_ = OB_INVALID_VERSION;
  schema_objs_.reset();
  mgr_ = NULL;
  mgr_handle_.reset();

  is_standby_cluster_ = false;

  mock_gts_schema_ = NULL;
  mock_simple_gts_schema_ = NULL;
  schema_helper_.reset();
  mock_allocator_.reuse();

  tenant_id_ = OB_INVALID_TENANT_ID;

  for (int64_t i = 0; i < schema_mgr_infos_.count(); i++) {
    schema_mgr_infos_.at(i).reset();
  }
  schema_mgr_infos_.reset();
  local_allocator_.reuse();

  is_inited_ = false;
  return ret;
}

void ObSchemaGetterGuard::dump()
{
  LOG_INFO("schema_service", K(schema_service_));
  LOG_INFO("snapshot_version", K(snapshot_version_));
  if (NULL != mgr_) {
    mgr_->dump();
  }
  mgr_handle_.dump();
  LOG_INFO("tenant_id", K(tenant_id_));
}

int ObSchemaGetterGuard::get_schema_version(const uint64_t tenant_id, int64_t& schema_version) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgrInfo* schema_mgr_info = NULL;
  if (!is_schema_splited()) {
    schema_version = snapshot_version_;
  } else if (OB_FAIL(get_schema_mgr_info(tenant_id, schema_mgr_info))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      const ObSimpleTenantSchema* tenant_schema = NULL;
      if (OB_FAIL(get_schema_mgr_info(OB_SYS_TENANT_ID, schema_mgr_info))) {
        LOG_WARN("fail to get sys schema_mgr_info", K(ret));
      } else if (OB_ISNULL(schema_mgr_info) || OB_ISNULL(schema_mgr_info->get_schema_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_mgr_info or schema_mgr is null", K(ret), KP(schema_mgr_info));
      } else if (OB_FAIL(schema_mgr_info->get_schema_mgr()->get_tenant_schema(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", K(ret), K(tenant_id));
      } else {
        // return special schema version while creating tenant
        schema_version = OB_CORE_SCHEMA_VERSION;
      }
    } else {
      LOG_WARN("fail to schema mgr info", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(schema_mgr_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_info is null", K(ret), K(tenant_id));
  } else {
    schema_version = schema_mgr_info->get_snapshot_version();
  }
  if (OB_FAIL(ret) && OB_TENANT_HAS_BEEN_DROPPED != ret && ObSchemaService::g_liboblog_mode_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(schema_service_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else {
      TenantStatus tenant_status = TENANT_STATUS_INVALID;
      tmp_ret = schema_service_->query_tenant_status(tenant_id, tenant_status);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("query tenant status failed", K(ret), K(tmp_ret), K(tenant_id));
      } else if (TENANT_DELETED == tenant_status) {
        LOG_INFO("tenant has been dropped, no need retry", K(ret), K(tenant_id));
        ret = OB_TENANT_HAS_BEEN_DROPPED;  // overwrite ret
      }
    }
  }
  return ret;
}

// For SQL only, delay-deleted index is not visable
int ObSchemaGetterGuard::get_can_read_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size,
    bool with_mv, bool with_global_index /* =true */, bool with_domain_index /*=true*/)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_table_schema(table_id, table_schema)) || OB_ISNULL(table_schema)) {
    LOG_WARN("cannot get table schema for table  ", K(table_id), K(ret));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObTableSchema* index_schema = NULL;
    int64_t can_read_count = 0;
    if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("cannot get table schema for table", K(ret), "index_table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (!with_mv && index_schema->is_materialized_view()) {
          // skip
        } else if (!with_global_index && index_schema->is_global_index_table()) {
          // skip
        } else if (!with_domain_index && index_schema->is_domain_index()) {
          // does not need domain index, skip it
        } else if (index_schema->can_read_index() && index_schema->is_index_visible()) {
          index_tid_array[can_read_count++] = simple_index_infos.at(i).table_id_;
        } else {
          // Do nothing.
        }
      }
    }
    size = can_read_count;
  }

  return ret;
}

int ObSchemaGetterGuard::check_has_local_unique_index(uint64_t table_id, bool& has_local_unique_index)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2* index_schema = NULL;
  has_local_unique_index = false;
  if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get table schema for table ", K(table_id));
  } else if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", K(simple_index_infos.at(i).table_id_));
    } else if (OB_UNLIKELY(index_schema->is_final_invalid_index())) {
      // invalid index status, need ingore
    } else if (index_schema->is_local_unique_index_table()) {
      has_local_unique_index = true;
      break;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_id(const ObString& tenant_name, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_name), K(ret));
  } else if (0 == tenant_name.case_compare(OB_GTS_TENANT_NAME)) {
    tenant_id = OB_GTS_TENANT_ID;
  } else {
    // FIXME: just compatible old code for lock, can it be moved to upper level?
    const ObTenantSchema* tenant_info = NULL;
    if (OB_FAIL(get_tenant_info(tenant_name, tenant_info))) {
      LOG_WARN("get tenant info failed", K(ret), K(tenant_name));
    } else if (NULL == tenant_info) {
      ret = OB_ERR_INVALID_TENANT_NAME;
      LOG_WARN("Can not find tenant", K(tenant_name));
    } else if (tenant_info->get_locked()) {
      ret = OB_ERR_TENANT_IS_LOCKED;
      LOG_WARN("Tenant is locked", K(ret));
    } else {
      tenant_id = tenant_info->get_tenant_id();
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_sys_variable_schema(
    const ObString& tenant_name, const ObSysVariableSchema*& sys_variable_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_name), K(ret));
  } else if (OB_FAIL(get_tenant_info(tenant_name, tenant_info))) {
    LOG_WARN("get tenant info failed", K(ret), K(tenant_name));
  } else if (NULL == tenant_info) {
    ret = OB_ERR_INVALID_TENANT_NAME;
    LOG_WARN("Can not find tenant", K(tenant_name));
  } else if (OB_FAIL(get_schema_v2(SYS_VARIABLE_SCHEMA, tenant_info->get_tenant_id(), sys_variable_schema))) {
    LOG_WARN("fail to get sys var schema", K(ret), K(tenant_info));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sys_variable_schema(
    const uint64_t tenant_id, const ObSysVariableSchema*& sys_variable_schema)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_schema_v2(SYS_VARIABLE_SCHEMA, tenant_id, sys_variable_schema))) {
    LOG_WARN("fail to get sys var schema", K(ret), K(snapshot_version_), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sys_variable_schema(
    const uint64_t tenant_id, const ObSimpleSysVariableSchema*& sys_variable_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id), "schema_version", mgr->get_schema_version());
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_system_variable(
    uint64_t tenant_id, const ObString& var_name, const ObSysVarSchema*& var_schema)
{
  int ret = OB_SUCCESS;
  const ObSysVariableSchema* sys_variable_schema = NULL;
  if (OB_FAIL(get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get tenant info failed", K(ret), K(tenant_id));
  } else if (NULL == sys_variable_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("Can not find tenant", K(tenant_id));
  } else if (OB_FAIL(sys_variable_schema->get_sysvar_schema(var_name, var_schema))) {
    if (OB_ERR_SYS_VARIABLE_UNKNOWN != ret) {
      LOG_WARN("get sysvar schema failed", K(tenant_id), K(var_name));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_system_variable(
    uint64_t tenant_id, ObSysVarClassType var_id, const ObSysVarSchema*& var_schema)
{
  int ret = OB_SUCCESS;
  const ObSysVariableSchema* sys_variable_schema = NULL;
  if (OB_FAIL(get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tenant info failed", K(ret), K(tenant_id));
  } else if (NULL == sys_variable_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("Can not find tenant", K(tenant_id));
  } else if (OB_FAIL(sys_variable_schema->get_sysvar_schema(var_id, var_schema))) {
    if (OB_ERR_SYS_VARIABLE_UNKNOWN != ret) {
      LOG_WARN("get sysvar schema failed", K(tenant_id), K(var_id));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_id(uint64_t tenant_id, const ObString& user_name, const ObString& host_name,
    uint64_t& user_id, const bool is_role /*false*/)
{
  int ret = OB_SUCCESS;
  UNUSED(is_role);
  const ObSchemaMgr* mgr = NULL;
  user_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleUserSchema* simple_user = NULL;
    ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret), K(tenant_id));
    } else if (0 == user_name.case_compare(OB_SYS_USER_NAME) && 0 == host_name.case_compare(OB_SYS_HOST_NAME) &&
               (ObWorker::CompatMode::ORACLE != compat_mode)) {
      // root is not an inner user in oracle mode.
      user_id = combine_id(tenant_id, OB_SYS_USER_ID);
    } else if (OB_FAIL(mgr->get_user_schema(tenant_id, user_name, host_name, simple_user))) {
      LOG_WARN("get simple user failed", K(ret), K(tenant_id), K(user_name), K(host_name));
    } else if (NULL == simple_user) {
      LOG_INFO("user not exist", K(tenant_id), K(user_name), K(host_name));
    } else {
      user_id = simple_user->get_user_id();
      LOG_DEBUG("succ to get user", K(tenant_id), K(user_id), K(user_name), K(host_name), KPC(simple_user));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_outline_infos_in_database(
    const uint64_t tenant_id, const uint64_t database_id, ObIArray<const ObOutlineInfo*>& outline_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  outline_infos.reset();

  ObArray<const ObSimpleOutlineSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schemas_in_database(tenant_id, database_id, schemas))) {
    LOG_WARN("get outlne schemas in database failed", K(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleOutlineSchema* tmp_schema = *schema;
      const ObOutlineInfo* outline_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (OB_FAIL(get_schema(
                     OUTLINE_SCHEMA, tmp_schema->get_outline_id(), tmp_schema->get_schema_version(), outline_info))) {
        LOG_WARN("get schema failed",
            K(ret),
            "outline_id",
            tmp_schema->get_outline_id(),
            "schema_version",
            tmp_schema->get_schema_version());
      } else if (OB_FAIL(outline_infos.push_back(outline_info))) {
        LOG_WARN("add outline schema failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_synonym_infos_in_database(
    const uint64_t tenant_id, const uint64_t database_id, ObIArray<const ObSynonymInfo*>& synonym_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  synonym_infos.reset();

  ObArray<const ObSimpleSynonymSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schemas_in_database(tenant_id, database_id, schemas))) {
    LOG_WARN("get synonym schemas in database failed", K(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleSynonymSchema* tmp_schema = *schema;
      const ObSynonymInfo* synonym_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (OB_FAIL(get_schema(
                     SYNONYM_SCHEMA, tmp_schema->get_synonym_id(), tmp_schema->get_schema_version(), synonym_info))) {
        LOG_WARN("get schema failed",
            K(ret),
            "synonym_id",
            tmp_schema->get_synonym_id(),
            "schema_version",
            tmp_schema->get_schema_version());
      } else if (OB_FAIL(synonym_infos.push_back(synonym_info))) {
        LOG_WARN("add synonym schema failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_sequence_infos_in_database(
    const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<const ObSequenceSchema*>& sequence_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  sequence_infos.reset();

  ObArray<const ObSequenceSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sequence_mgr_.get_sequence_schemas_in_database(tenant_id, database_id, schemas))) {
    LOG_WARN("get sequence schemas in database failed", K(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSequenceSchema* tmp_schema = *schema;
      const ObSequenceSchema* sequence_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (OB_FAIL(get_schema(SEQUENCE_SCHEMA,
                     tmp_schema->get_sequence_id(),
                     tmp_schema->get_schema_version(),
                     sequence_info))) {
        LOG_WARN("get schema failed",
            K(ret),
            "sequence_id",
            tmp_schema->get_sequence_id(),
            "schema_version",
            tmp_schema->get_schema_version());
      } else if (OB_FAIL(sequence_infos.push_back(sequence_info))) {
        LOG_WARN("add sequence schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_profile_schema_by_name(
    const uint64_t tenant_id, const common::ObString& name, const ObProfileSchema*& schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr* mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->profile_mgr_.get_schema_by_name(tenant_id, name, schema))) {
    LOG_WARN("get schema failed", K(name), K(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_profile_schema_by_id(
    const uint64_t tenant_id, const uint64_t profile_id, const ObProfileSchema*& schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr* mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !is_valid_id(profile_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(profile_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->profile_mgr_.get_schema_by_id(profile_id, schema))) {
    LOG_WARN("get schema failed", K(profile_id), K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("NULL ptr", K(ret), K(schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_profile_failed_login_limits(
    const uint64_t user_id, int64_t& failed_login_limit_num, int64_t& failed_login_limit_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo* user_info = nullptr;

  if (OB_FAIL(get_user_info(user_id, user_info))) {
    LOG_WARN("fail to get user id", K(ret));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user not exist", K(ret));
  } else {
    ObProfileSchema default_profile;
    const ObProfileSchema* profile_info = nullptr;
    failed_login_limit_num = INT64_MAX;
    int64_t profile_id = user_info->get_profile_id();
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250 && !is_valid_id(profile_id)) {
      if (OB_FAIL(default_profile.set_default_values())) {
        LOG_WARN("fail to set default value", K(ret));
      } else {
        profile_info = &default_profile;
      }
    } else {
      if (!is_valid_id(profile_id)) {
        profile_id = combine_id(user_info->get_tenant_id(), OB_ORACLE_TENANT_INNER_PROFILE_ID);
      }
      if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(), profile_id, profile_info))) {
        LOG_WARN("fail to get profile info", K(ret), K(profile_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(profile_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("profile not exist", K(ret));
    } else {
      failed_login_limit_num = profile_info->get_failed_login_attempts();
      failed_login_limit_time = profile_info->get_password_lock_time();
    }
  }
  return ret;
}

// only use in oracle mode
int ObSchemaGetterGuard::get_user_password_expire_times(
    const uint64_t user_id, int64_t& password_last_change, int64_t& password_life_time, int64_t& password_grace_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo* user_info = nullptr;
  if (OB_FAIL(get_user_info(user_id, user_info))) {
    LOG_WARN("fail to get user id", K(ret));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user not exist", K(ret));
  } else {
    ObProfileSchema default_profile;
    const ObProfileSchema* profile_info = nullptr;
    password_last_change = user_info->get_password_last_changed();
    int64_t profile_id = user_info->get_profile_id();
    if (!is_valid_id(profile_id)) {
      profile_id = combine_id(user_info->get_tenant_id(), OB_ORACLE_TENANT_INNER_PROFILE_ID);
    }
    if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(), profile_id, profile_info))) {
      LOG_WARN("fail to get profile info", K(ret), K(profile_id));
    } else if (OB_ISNULL(profile_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("profile not exist", K(ret));
    } else {
      password_life_time = profile_info->get_password_life_time();
      password_grace_time = profile_info->get_password_grace_time();
      password_life_time = (password_life_time == -1) ? INT64_MAX : password_life_time;
      password_grace_time = (password_grace_time == -1) ? INT64_MAX : password_grace_time;
    }
  }
  return ret;
}

// For SQL only, delay-deleted index is not visable
int ObSchemaGetterGuard::get_can_write_index_array(
    uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool only_global)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  int64_t can_write_count = 0;
  const ObTableSchema* index_schema = NULL;
  if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get table schema for table ", K(table_id));
  } else if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", K(simple_index_infos.at(i).table_id_));
    } else if (OB_UNLIKELY(index_schema->is_final_invalid_index())) {
      // invalid index status, need ingore
    } else if (!only_global) {
      index_tid_array[can_write_count] = simple_index_infos.at(i).table_id_;
      ++can_write_count;
    } else if (index_schema->is_global_index_table()) {
      index_tid_array[can_write_count] = simple_index_infos.at(i).table_id_;
      ++can_write_count;
    }
  }
  size = can_write_count;

  return ret;
}

// check if column is included in primary key/partition key/index columns.
int ObSchemaGetterGuard::column_is_key(uint64_t table_id, uint64_t column_id, bool& is_key)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = nullptr;
  is_key = false;
  if (OB_FAIL(get_column_schema(table_id, column_id, column_schema))) {
    LOG_WARN("column schema isn't found", K(ret), K(table_id), K(column_id));
  } else if (OB_ISNULL(column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), K(table_id), K(column_id));
  } else if (column_schema->is_rowkey_column() || column_schema->is_tbl_part_key_column()) {
    is_key = true;
  } else {
    int64_t index_tid_array_size = OB_MAX_INDEX_PER_TABLE;
    uint64_t index_tid_array[OB_MAX_INDEX_PER_TABLE];
    if (OB_FAIL(get_can_write_index_array(table_id, index_tid_array, index_tid_array_size))) {
      LOG_WARN("get index tid array failed", K(ret), K(index_tid_array_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_key && i < index_tid_array_size; ++i) {
      const ObTableSchema* index_schema = nullptr;
      if (OB_FAIL(get_column_schema(index_tid_array[i], column_id, column_schema))) {
        LOG_WARN("get column schema from index schema failed", K(ret), K(index_tid_array[i]), K(column_id));
      } else if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), K(table_id), K(column_id));
      } else if (column_schema->is_index_column()) {
        is_key = true;
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_database_id(uint64_t tenant_id, const ObString& database_name, uint64_t& database_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  database_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(ret));
  } else {
    const ObSimpleDatabaseSchema* simple_database = NULL;
    if ((database_name.length() == static_cast<int32_t>(strlen(OB_SYS_DATABASE_NAME))) &&
        (0 == STRNCASECMP(database_name.ptr(), OB_SYS_DATABASE_NAME, strlen(OB_SYS_DATABASE_NAME)))) {
      // To avoid cyclic dependence while create tenant
      database_id = combine_id(tenant_id, OB_SYS_DATABASE_ID);
    } else {
      if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
        LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
      } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
        LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(mgr->get_database_schema(tenant_id, database_name, simple_database))) {
        LOG_WARN("get simple database failed", K(ret), K(tenant_id), K(database_name));
      } else if (NULL == simple_database) {
        LOG_INFO("database not exist", K(tenant_id), K(database_name));
      } else {
        database_id = simple_database->get_database_id();
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_id(uint64_t tenant_id, const ObString& tablegroup_name, uint64_t& tablegroup_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  tablegroup_id = OB_INVALID_ID;

  const ObSimpleTablegroupSchema* simple_tablegroup = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(tablegroup_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schema(tenant_id, tablegroup_name, simple_tablegroup))) {
    LOG_WARN("get simple tablegroup failed", K(ret), K(tenant_id), K(tablegroup_name));
  } else if (NULL == simple_tablegroup) {
    LOG_INFO("tablegroup not exist", K(tenant_id), K(tablegroup_name));
  } else {
    tablegroup_id = simple_tablegroup->get_tablegroup_id();
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_id(uint64_t tenant_id, uint64_t database_id, const ObString& table_name,
    const bool is_index,
    const CheckTableType check_type,  // check if temporary table is visable
    uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  uint64_t session_id = session_id_;
  const ObSchemaMgr* mgr = NULL;
  const ObSimpleTableSchemaV2* simple_table = NULL;
  bool is_system_table = false;
  table_id = OB_INVALID_ID;

  if (NON_TEMP_TABLE_TYPE == check_type) {
    session_id = 0;
  } else { /* do nothing */
  }
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else if (OB_FAIL(ObSysTableChecker::is_sys_table_name(database_id, table_name, is_system_table))) {
    LOG_WARN("fail to check if table is system table", K(ret), K(database_id), K(table_name));
  } else {
    // system table's schema is always stored in system tenant.
    uint64_t fetch_tenant_id = is_system_table ? OB_SYS_TENANT_ID : tenant_id;
    if (OB_FAIL(check_tenant_schema_guard(fetch_tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(fetch_tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(fetch_tenant_id), K(tenant_id));
    } else if (OB_FAIL(mgr->get_table_schema(tenant_id, database_id, session_id, table_name, is_index, simple_table))) {
      LOG_WARN("get simple table failed",
          K(ret),
          K(fetch_tenant_id),
          K(tenant_id),
          K(database_id),
          K(session_id),
          K(table_name),
          K(is_index));
    } else if (NULL == simple_table) {
      LOG_INFO("table not exist",
          K(fetch_tenant_id),
          K(tenant_id),
          K(database_id),
          K(session_id),
          K(table_name),
          K(is_index),
          K_(snapshot_version),
          "is_schema_split",
          static_cast<int64_t>(is_schema_splited()),
          "schema_version",
          mgr->get_schema_version(),
          "schema_mgr_tenant_id",
          mgr->get_tenant_id());
    } else {
      if (TEMP_TABLE_TYPE == check_type && false == simple_table->is_tmp_table()) {
        // temporary table is not finded.
        LOG_DEBUG("request for temporary table but non-temporary table returned",
            K(session_id_),
            K(session_id),
            K(check_type));
      } else {
        table_id = simple_table->get_table_id();
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_id(uint64_t tenant_id, const ObString& database_name, const ObString& table_name,
    const bool is_index,
    const CheckTableType check_type,  // check if temporary table is visable
    uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(table_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("get database_id failed", K(ret), K(tenant_id), K(database_name));
    } else if (OB_INVALID_ID == database_id) {
      // do-nothing
    } else if (OB_FAIL(get_table_id(tenant_id, database_id, table_name, is_index, check_type, table_id))) {
      LOG_WARN("get table id failed", K(ret), K(tenant_id), K(database_id), K(table_name), K(is_index));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_foreign_key_id(
    const uint64_t tenant_id, const uint64_t database_id, const ObString& foreign_key_name, uint64_t& foreign_key_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  foreign_key_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || foreign_key_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_foreign_key_id(tenant_id, database_id, foreign_key_name, foreign_key_id))) {
    LOG_WARN("get foreign key id failed", K(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_INVALID_ID == foreign_key_id) {
    LOG_INFO("foreign key not exist", K(tenant_id), K(database_id), K(foreign_key_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_foreign_key_info(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& foreign_key_name, ObSimpleForeignKeyInfo& foreign_key_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  foreign_key_info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || foreign_key_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_foreign_key_info(tenant_id, database_id, foreign_key_name, foreign_key_info))) {
    LOG_WARN("get foreign key id failed", K(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_INVALID_ID == foreign_key_info.foreign_key_id_) {
    LOG_INFO("foreign key not exist", K(tenant_id), K(database_id), K(foreign_key_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_constraint_id(
    const uint64_t tenant_id, const uint64_t database_id, const ObString& constraint_name, uint64_t& constraint_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  constraint_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || constraint_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_constraint_id(tenant_id, database_id, constraint_name, constraint_id))) {
    LOG_WARN("get constraint id failed", K(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_INVALID_ID == constraint_id) {
    LOG_INFO("constraint not exist", K(tenant_id), K(database_id), K(constraint_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_constraint_info(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& constraint_name, ObSimpleConstraintInfo& constraint_info) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  constraint_info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || constraint_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_constraint_info(tenant_id, database_id, constraint_name, constraint_info))) {
    LOG_WARN("get constraint info failed", K(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_INVALID_ID == constraint_info.constraint_id_) {
    LOG_INFO("constraint not exist", K(tenant_id), K(database_id), K(constraint_name));
  }

  return ret;
}

// basic interface
int ObSchemaGetterGuard::get_tenant_info(uint64_t tenant_id, const ObTenantSchema*& tenant_schema)
{
  int ret = OB_SUCCESS;
  tenant_schema = NULL;
  LOG_DEBUG("begin to get tenant schema", K(tenant_id), K(snapshot_version_), K(lbt()));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_GTS_TENANT_ID == tenant_id) {
    if (nullptr != mock_gts_schema_) {
      tenant_schema = mock_gts_schema_;
    } else {
      void* ptr = mock_allocator_.alloc(sizeof(ObTenantSchema));
      if (OB_UNLIKELY(nullptr == ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == (mock_gts_schema_ = new (ptr) ObTenantSchema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("shall never be here", K(ret));
      } else if (OB_FAIL(mock_gts_schema_->set_tenant_name(OB_GTS_TENANT_NAME))) {
        LOG_WARN("fail to set tenant name", K(ret));
      } else {
        mock_gts_schema_->set_tenant_id(OB_GTS_TENANT_ID);
        tenant_schema = mock_gts_schema_;
      }
    }
  } else if (OB_FAIL(get_schema_v2(TENANT_SCHEMA, tenant_id, tenant_schema))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_tenant_info(uint64_t tenant_id, const ObSimpleTenantSchema*& tenant_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  tenant_schema = NULL;
  LOG_DEBUG("begin to get tenant schema", K(tenant_id), K(snapshot_version_), K(lbt()));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_GTS_TENANT_ID == tenant_id) {
    if (nullptr != mock_simple_gts_schema_) {
      tenant_schema = mock_simple_gts_schema_;
    } else {
      void* ptr = mock_allocator_.alloc(sizeof(ObSimpleTenantSchema));
      if (OB_UNLIKELY(nullptr == ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == (mock_simple_gts_schema_ = new (ptr) ObSimpleTenantSchema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("shall never be here", K(ret));
      } else if (OB_FAIL(mock_simple_gts_schema_->set_tenant_name(OB_GTS_TENANT_NAME))) {
        LOG_WARN("fail to set tenant name", K(ret));
      } else {
        mock_simple_gts_schema_->set_tenant_id(OB_GTS_TENANT_ID);
        tenant_schema = mock_simple_gts_schema_;
      }
    }
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->get_tenant_schema(tenant_id, tenant_schema);
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(uint64_t user_id, const ObUserInfo*& user_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(user_id);
  user_info = NULL;

  LOG_DEBUG("begin to get user schema", K(user_id), K(snapshot_version_));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(user_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_v2(USER_SCHEMA, user_id, user_info))) {
    LOG_WARN("get user schema failed", K(ret), K(user_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_database_schema(uint64_t database_id, const ObDatabaseSchema*& database_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(database_id);
  database_schema = NULL;

  LOG_DEBUG("begin to get database schema", K(database_id), K(snapshot_version_));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(database_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_v2(DATABASE_SCHEMA, database_id, database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(database_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_database_schema(uint64_t database_id, const ObSimpleDatabaseSchema*& database_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(database_id);
  const ObSchemaMgr* mgr = NULL;
  database_schema = NULL;

  LOG_DEBUG("begin to get database schema", K(database_id), K(snapshot_version_));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(database_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->get_database_schema(database_id, database_schema);
  }

  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_schema(uint64_t tablegroup_id, const ObTablegroupSchema*& tablegroup_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(tablegroup_id);
  const ObTablegroupSchema* orig_tablegroup = NULL;
  tablegroup_schema = NULL;

  LOG_DEBUG("begin to get tablegroup schema", K(tablegroup_id), K(snapshot_version_));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tablegroup_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_v2(TABLEGROUP_SCHEMA, tablegroup_id, orig_tablegroup))) {
    LOG_WARN("get tablegroup schema failed", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(schema_helper_.get_tablegroup_schema(*this, orig_tablegroup, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_schema(
    uint64_t tablegroup_id, const ObSimpleTablegroupSchema*& tablegroup_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(tablegroup_id);
  const ObSchemaMgr* mgr = NULL;
  const ObSimpleTablegroupSchema* orig_tablegroup = NULL;
  tablegroup_schema = NULL;

  LOG_DEBUG("begin to get tablegroup schema", K(tablegroup_id), K(snapshot_version_));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tablegroup_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schema(tablegroup_id, orig_tablegroup))) {
    LOG_WARN("fail to get simple tablegroup", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(schema_helper_.get_tablegroup_schema(*this, orig_tablegroup, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_table_schema(uint64_t table_id, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* orig_table = NULL;
  uint64_t base_table_id = OB_INVALID_ID;
  if (share::is_oracle_mode() && sql::ObSQLMockSchemaUtils::is_mock_index(table_id, base_table_id)) {
    const ObTableSchema* base_table_schema = NULL;
    if (OB_FAIL(get_table_schema(base_table_id, base_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table schema", K(ret));
    } else if (OB_FAIL(sql::ObSQLMockSchemaUtils::mock_rowid_index(base_table_schema, table_schema))) {
      LOG_WARN("failed to mock rowid index table schema", K(ret));
    }
  } else if (OB_FAIL(get_table_schema_inner(table_id, orig_table))) {
    LOG_WARN("fail to get table schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(orig_table)) {
    // nothing todo
  } else if (OB_FAIL(schema_helper_.get_table_schema(*this, orig_table, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(table_id));
  } else if (OB_FAIL(try_mock_rowid(table_schema, table_schema))) {
    LOG_WARN("failed to try mock rowid column", K(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema_inner(uint64_t table_id, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  LOG_DEBUG("begin to get table schema", K(table_id), K(snapshot_version_), K(lbt()));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_id), K(ret));
  } else if (is_fake_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (!check_fetch_table_id(table_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(table_id));
  } else if (is_link_table_id(table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("link table is not support here", K(ret), K(table_id));
  } else if (OB_FAIL(get_schema_v2(TABLE_SCHEMA, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_index_status(
    const uint64_t data_table_id, const bool with_global_index, common::ObIArray<ObIndexTableStat>& index_status)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* data_table_schema = NULL;
  index_status.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (is_link_table_id(data_table_id)) {
    // skip.
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_table_id), K(ret));
  } else if (OB_FAIL(get_table_schema(data_table_id, data_table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("data table schema not exists", K(ret), K(data_table_id));
  } else if (OB_FAIL(get_index_status_inner(*data_table_schema, with_global_index, index_status))) {
    LOG_WARN("failed to get index array", K(ret), K(data_table_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_index_status_inner(const ObTableSchema& data_table_schema, const bool with_global_index,
    common::ObIArray<ObIndexTableStat>& index_status)
{
  int ret = OB_SUCCESS;
  const bool with_mv = true;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos, with_mv))) {
    LOG_WARN("failed to get index array", K(ret), "data_table_id", data_table_schema.get_table_id());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema* index_schema = NULL;
      if (OB_FAIL(get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("failed to get index table schema", K(ret), K(i), "index_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_ERROR("index schema must not null", K(ret), K(i), "index_id", simple_index_infos.at(i).table_id_);
      } else if (with_global_index || !index_schema->is_global_index_table()) {
        if (OB_FAIL(index_status.push_back(ObIndexTableStat(simple_index_infos.at(i).table_id_,
                index_schema->get_index_status(),
                index_schema->is_dropped_schema())))) {
          LOG_WARN("failed to add index status", K(ret), K(i), "index_id", simple_index_infos.at(i).table_id_);
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_all_aux_table_status(
    const uint64_t data_table_id, const bool with_global_index, common::ObIArray<ObIndexTableStat>& index_status)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* data_table_schema = NULL;
  index_status.reuse();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data_table_id), K(ret));
  } else if (is_link_table_id(data_table_id)) {
    // skip.
  } else if (OB_FAIL(get_table_schema(data_table_id, data_table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("data table schema not exists", K(ret), K(data_table_id));
  } else if (OB_FAIL(get_index_status_inner(*data_table_schema, with_global_index, index_status))) {
    LOG_WARN("failed to get index array", K(ret), K(data_table_id));
  }
  return ret;
}

// for compatible
int ObSchemaGetterGuard::get_tenant_info(const ObString& tenant_name, const ObTenantSchema*& tenant_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  tenant_info = NULL;

  const ObSimpleTenantSchema* simple_tenant = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_name), K(ret));
  } else if (0 == tenant_name.case_compare(OB_GTS_TENANT_NAME)) {
    if (nullptr != mock_gts_schema_) {
      tenant_info = mock_gts_schema_;
    } else {
      void* ptr = mock_allocator_.alloc(sizeof(ObTenantSchema));
      if (OB_UNLIKELY(nullptr == ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (nullptr == (mock_gts_schema_ = new (ptr) ObTenantSchema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("shall never be here", K(ret));
      } else if (OB_FAIL(mock_gts_schema_->set_tenant_name(OB_GTS_TENANT_NAME))) {
        LOG_WARN("fail to set tenant name", K(ret));
      } else {
        mock_gts_schema_->set_tenant_id(OB_GTS_TENANT_ID);
        tenant_info = mock_gts_schema_;
      }
    }
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret));
  } else if (OB_FAIL(mgr->get_tenant_schema(tenant_name, simple_tenant))) {
    LOG_WARN("get simple tenant failed", K(ret), K(tenant_name));
  } else if (NULL == simple_tenant) {
    LOG_INFO("tenant not exist", K(tenant_name));
  } else if (OB_FAIL(get_schema_v2(
                 TENANT_SCHEMA, simple_tenant->get_tenant_id(), tenant_info, simple_tenant->get_schema_version()))) {
    LOG_WARN("get tenant schema failed", K(ret), "simple tenant schema", *simple_tenant);
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tenant_info));
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(uint64_t tenant_id, uint64_t user_id, const ObUserInfo*& user_info)
{
  int ret = OB_SUCCESS;
  user_info = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(user_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = get_user_info(user_id, user_info);
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(
    uint64_t tenant_id, const ObString& user_name, const ObString& host_name, const ObUserInfo*& user_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  user_info = NULL;

  const ObSimpleUserSchema* simple_user = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_user_schema(tenant_id, user_name, host_name, simple_user))) {
    LOG_WARN("get simple user failed", K(ret), K(tenant_id), K(user_name));
  } else if (NULL == simple_user) {
    LOG_INFO("user not exist", K(tenant_id), K(user_name));
  } else if (OB_FAIL(get_schema_v2(
                 USER_SCHEMA, simple_user->get_user_id(), user_info, simple_user->get_schema_version()))) {
    LOG_WARN("get user schema failed", K(ret), "simple user schema", *simple_user);
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(user_info));
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_info(
    uint64_t tenant_id, const ObString& user_name, ObIArray<const ObUserInfo*>& users_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObSimpleUserSchema*, DEFAULT_SAME_USERNAME_COUNT> simple_users;
    if (OB_FAIL(mgr->get_user_schema(tenant_id, user_name, simple_users))) {
      LOG_WARN("get simple user failed", K(ret), K(tenant_id), K(user_name));
    } else if (simple_users.empty()) {
      LOG_INFO("user not exist", K(tenant_id), K(user_name));
    } else {
      const ObUserInfo* user_info = NULL;
      for (int64_t i = 0; i < simple_users.count() && OB_SUCC(ret); ++i) {
        const ObSimpleUserSchema*& simple_user = simple_users.at(i);
        if (OB_FAIL(
                get_schema_v2(USER_SCHEMA, simple_user->get_user_id(), user_info, simple_user->get_schema_version()))) {
          LOG_WARN("get user schema failed", K(tenant_id), KPC(simple_user), K(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(user_info));
        } else if (OB_FAIL(users_info.push_back(user_info))) {
          LOG_WARN("failed to push back user_info", KPC(user_info), K(users_info), K(ret));
        } else {
          user_info = NULL;
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_database_schema(
    uint64_t tenant_id, uint64_t database_id, const ObDatabaseSchema*& database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    ret = get_database_schema(database_id, database_schema);
  }

  return ret;
}

int ObSchemaGetterGuard::get_database_schema(
    uint64_t tenant_id, const ObString& database_name, const ObDatabaseSchema*& database_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  database_schema = NULL;

  const ObSimpleDatabaseSchema* simple_database = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_database_schema(tenant_id, database_name, simple_database))) {
    LOG_WARN("get simple database failed", K(ret), K(tenant_id), K(database_name));
  } else if (NULL == simple_database) {
    LOG_INFO("database not exist", K(tenant_id), K(database_name));
  } else if (OB_FAIL(get_schema_v2(DATABASE_SCHEMA,
                 simple_database->get_database_id(),
                 database_schema,
                 simple_database->get_schema_version()))) {
    LOG_WARN("get database schema failed", K(ret), "simple database schema", *simple_database);
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(database_schema));
  }

  return ret;
}

int ObSchemaGetterGuard::get_table_schema(uint64_t tenant_id, uint64_t database_id, const ObString& table_name,
    const bool is_index, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* orig_table = NULL;
  if (OB_FAIL(get_table_schema_inner(tenant_id, database_id, table_name, is_index, orig_table))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_id), K(table_name), K(is_index));
  } else if (OB_ISNULL(orig_table)) {
    // nothing todo
  } else if (OB_FAIL(schema_helper_.get_table_schema(*this, orig_table, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_id), K(table_name), K(is_index));
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_table_schema(uint64_t tenant_id, uint64_t database_id, const ObString& table_name,
    const bool is_index, const ObSimpleTableSchemaV2*& simple_table_schema)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2* orig_table = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_simple_table_schema_inner(tenant_id, database_id, table_name, is_index, orig_table))) {
    LOG_WARN("fail to get simple table schema", K(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(schema_helper_.get_table_schema(*this, orig_table, simple_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(table_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_table_schema_inner(uint64_t tenant_id, uint64_t database_id,
    const ObString& table_name, const bool is_index, const ObSimpleTableSchemaV2*& simple_table_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  simple_table_schema = NULL;
  bool is_system_table = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(table_name), K(ret));
  } else if (OB_FAIL(ObSysTableChecker::is_sys_table_name(database_id, table_name, is_system_table))) {
    LOG_WARN("fail to check if table is system table", K(ret), K(database_id), K(table_name));
  } else if (!is_system_table && OB_INVALID_TENANT_ID != tenant_id_ && tenant_id != tenant_id_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch user table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(tenant_id));
  } else {
    // system table's schema is always stored in system tenant.
    uint64_t fetch_tenant_id = is_system_table ? OB_SYS_TENANT_ID : tenant_id;
    if (OB_FAIL(check_lazy_guard(fetch_tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(fetch_tenant_id), K(tenant_id));
    } else if (OB_FAIL(mgr->get_table_schema(
                   tenant_id, database_id, session_id_, table_name, is_index, simple_table_schema))) {
      LOG_WARN("get simple table failed",
          K(ret),
          K(fetch_tenant_id),
          K(tenant_id),
          K(database_id),
          K(table_name),
          K(is_index));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_table_schema_inner(uint64_t tenant_id, uint64_t database_id, const ObString& table_name,
    const bool is_index, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2* simple_table = NULL;
  table_schema = NULL;
  if (OB_FAIL(get_simple_table_schema_inner(tenant_id, database_id, table_name, is_index, simple_table))) {
    LOG_WARN("fail to get simple table schema", K(ret));
  } else if (NULL == simple_table) {
    LOG_INFO("table not exist", K(tenant_id), K(database_id), K(table_name), K(is_index));
  } else if (OB_FAIL(get_schema_v2(
                 TABLE_SCHEMA, simple_table->get_table_id(), table_schema, simple_table->get_schema_version()))) {
    LOG_WARN("get table schema failed", K(ret), "simple table schema", *simple_table);
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(table_schema), K(*simple_table));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema(uint64_t tenant_id, const ObString& database_name, const ObString& table_name,
    const bool is_index, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* orig_table = NULL;
  if (OB_FAIL(get_table_schema_inner(tenant_id, database_name, table_name, is_index, orig_table))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_name), K(table_name));
  } else if (OB_ISNULL(orig_table)) {
    // nothing todo
  } else if (OB_FAIL(schema_helper_.get_table_schema(*this, orig_table, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_name), K(table_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema_inner(uint64_t tenant_id, const ObString& database_name,
    const ObString& table_name, const bool is_index, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  table_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty() || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(table_name), K(ret));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    // do-nothing
  } else {
    ret = get_table_schema(tenant_id, database_id, table_name, is_index, table_schema);
  }

  return ret;
}

int ObSchemaGetterGuard::get_column_schema(
    uint64_t table_id, uint64_t column_id, const ObColumnSchemaV2*& column_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(table_id);
  column_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_id), K(column_id));
  } else if (is_fake_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (!check_fetch_table_id(table_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(get_table_schema(table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id));
    } else if (NULL == table_schema) {
      // do-nothing
    } else {
      column_schema = table_schema->get_column_schema(column_id);
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_column_schema(
    uint64_t table_id, const ObString& column_name, const ObColumnSchemaV2*& column_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(table_id);
  column_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id || column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_id), K(column_name));
  } else if (is_fake_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (!check_fetch_table_id(table_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(get_table_schema(table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id));
    } else if (NULL == table_schema) {
      // do-nothing
    } else {
      column_schema = table_schema->get_column_schema(column_name);
    }
  }

  return ret;
}

// for readonly
int ObSchemaGetterGuard::verify_read_only(const uint64_t tenant_id, const ObStmtNeedPrivs& stmt_need_privs)
{
  int ret = OB_SUCCESS;
  const ObStmtNeedPrivs::NeedPrivs& need_privs = stmt_need_privs.need_privs_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv& need_priv = need_privs.at(i);
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          // we do not check user priv level only check table and db
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(verify_db_read_only(tenant_id, need_priv))) {
            LOG_WARN("database is read only, can't not execute this statement", K(ret));
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_FAIL(verify_db_read_only(tenant_id, need_priv))) {
            LOG_WARN("db is read only, can't not execute this statement", K(ret));
          } else if (OB_FAIL(verify_table_read_only(tenant_id, need_priv))) {
            LOG_WARN("table is read only, can't not execute this statement", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown privilege level", K(need_priv), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::verify_db_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv)
{
  int ret = OB_SUCCESS;
  const ObString& db_name = need_priv.db_;
  const ObDatabaseSchema* db_schema = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_database_schema(tenant_id, db_name, db_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(tenant_id), K(db_name));
  } else if (NULL != db_schema) {
    if (db_schema->is_read_only()) {
      ret = OB_ERR_DB_READ_ONLY;
      LOG_USER_ERROR(OB_ERR_DB_READ_ONLY, db_name.length(), db_name.ptr());
      LOG_WARN("database is read only, can't not execute this statment", K(need_priv), K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::verify_table_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv)
{
  int ret = OB_SUCCESS;
  const ObString& db_name = need_priv.db_;
  const ObString& table_name = need_priv.table_;
  const ObTableSchema* table_schema = NULL;
  // FIXME: is it right?
  const bool is_index = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_table_schema(tenant_id, db_name, table_name, is_index, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (NULL != table_schema) {
    if (table_schema->is_read_only()) {
      ret = OB_ERR_TABLE_READ_ONLY;
      LOG_USER_ERROR(OB_ERR_TABLE_READ_ONLY, db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
      LOG_WARN("table is read only, can't not execute this statment", K(need_priv), K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schemas(common::ObIArray<const ObTableSchema*>& table_schemas)
{
  int ret = OB_SUCCESS;
  table_schemas.reset();

  ObArray<uint64_t> tenant_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get all schemas with tenant schema guard not allowed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else {
    ObArray<const ObTableSchema*> tmp_schemas;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret))
    {
      if (OB_FAIL(get_table_schemas_in_tenant(*tenant_id, tmp_schemas))) {
        LOG_WARN("get table schemas in tenant failed", K(ret), K(*tenant_id));
      } else {
        FOREACH_CNT_X(tmp_schema, tmp_schemas, OB_SUCC(ret))
        {
          if (OB_FAIL(table_schemas.push_back(*tmp_schema))) {
            LOG_WARN("add table schema failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::add_role_id_recursively(uint64_t role_id, ObSessionPrivInfo& s_priv)
{
  int ret = OB_SUCCESS;
  const ObUserInfo* role_info = NULL;

  if (!has_exist_in_array(s_priv.enable_role_id_array_, role_id)) {
    /* 1. put itself */
    OZ(s_priv.enable_role_id_array_.push_back(role_id));
    /* 2. get role recursively */
    OZ(get_user_info(role_id, role_info));
    if (OB_SUCC(ret) && role_info != NULL) {
      const ObSEArray<uint64_t, 8> &role_id_array = role_info->get_role_id_array();
      for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
        OZ(add_role_id_recursively(role_info->get_role_id_array().at(i), s_priv));
      }
    }
  }

  return ret;
}

// for privilege
int ObSchemaGetterGuard::check_user_access(
    const ObUserLoginInfo& login_info, ObSessionPrivInfo& s_priv, SSL* ssl_st, const ObUserInfo*& sel_user_info)
{
  int ret = OB_SUCCESS;
  sel_user_info = NULL;
  if (OB_FAIL(get_tenant_id(login_info.tenant_name_, s_priv.tenant_id_))) {
    LOG_WARN("Invalid tenant", "tenant_name", login_info.tenant_name_, K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(s_priv.tenant_id_))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(s_priv), K_(tenant_id));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObUserInfo*, DEFAULT_SAME_USERNAME_COUNT> users_info;
    if (OB_FAIL(get_user_info(s_priv.tenant_id_, login_info.user_name_, users_info))) {
      LOG_WARN("get user info failed", K(ret), K(s_priv.tenant_id_), K(login_info));
    } else if (users_info.empty()) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("No tenant user", K(login_info), K(ret));
    } else {
      bool is_found = false;
      const ObUserInfo* user_info = NULL;
      const ObUserInfo* matched_user_info = NULL;
      for (int64_t i = 0; i < users_info.count() && OB_SUCC(ret) && !is_found; ++i) {
        user_info = users_info.at(i);
        if (NULL == user_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", K(login_info), K(ret));
        } else if (user_info->is_role()) {
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error",
              "tenant_name",
              login_info.tenant_name_,
              "user_name",
              login_info.user_name_,
              "client_ip_",
              login_info.client_ip_,
              K(ret));
        } else if (!ObHostnameStuct::is_wild_match(login_info.client_ip_, user_info->get_host_name_str()) &&
                   !ObHostnameStuct::is_ip_match(login_info.client_ip_, user_info->get_host_name_str())) {
          LOG_DEBUG("account not matched, try next", KPC(user_info), K(login_info));
        } else {
          matched_user_info = user_info;
          if (0 == login_info.passwd_.length() && 0 == user_info->get_passwd_str().length()) {
            // passed
            is_found = true;
          } else if (0 == login_info.passwd_.length() || 0 == user_info->get_passwd_str().length()) {
            ret = OB_PASSWORD_WRONG;
            LOG_WARN("password error", K(ret), K(login_info.passwd_.length()), K(user_info->get_passwd_str().length()));
          } else {
            char stored_stage2_hex[SCRAMBLE_LENGTH] = {0};
            ObString stored_stage2_trimed;
            ObString stored_stage2_hex_str;
            if (user_info->get_passwd_str().length() < SCRAMBLE_LENGTH * 2 + 1) {
              ret = OB_NOT_IMPLEMENT;
              LOG_WARN("Currently hash method other than MySQL 4.1 hash is not implemented.",
                  "hash str length",
                  user_info->get_passwd_str().length());
            } else {
              // trim the leading '*'
              stored_stage2_trimed.assign_ptr(
                  user_info->get_passwd_str().ptr() + 1, user_info->get_passwd_str().length() - 1);
              stored_stage2_hex_str.assign_buffer(stored_stage2_hex, SCRAMBLE_LENGTH);
              stored_stage2_hex_str.set_length(SCRAMBLE_LENGTH);
              // first, we restore the stored, displayable stage2 hash to its hex form
              ObEncryptedHelper::displayable_to_hex(stored_stage2_trimed, stored_stage2_hex_str);
              // then, we call the mysql validation logic.
              if (OB_FAIL(ObEncryptedHelper::check_login(
                      login_info.passwd_, login_info.scramble_str_, stored_stage2_hex_str, is_found))) {
                LOG_WARN("Failed to check login", K(login_info), K(ret));
              } else if (!is_found) {
                LOG_INFO("password error",
                    "tenant_name",
                    login_info.tenant_name_,
                    "user_name",
                    login_info.user_name_,
                    "client_ip",
                    login_info.client_ip_,
                    "host_name",
                    user_info->get_host_name_str());
              } else {
                // found it
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (matched_user_info != NULL && matched_user_info->get_is_locked()) {
          ret = OB_ERR_USER_IS_LOCKED;
          LOG_WARN("User is locked", K(ret));
        } else if (!is_found) {
          user_info = NULL;
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error",
              "tenant_name",
              login_info.tenant_name_,
              "user_name",
              login_info.user_name_,
              "client_ip_",
              login_info.client_ip_,
              K(ret));
        } else if (OB_FAIL(check_ssl_access(*user_info, ssl_st))) {
          LOG_WARN("check_ssl_access failed",
              "tenant_name",
              login_info.tenant_name_,
              "user_name",
              login_info.user_name_,
              "client_ip_",
              login_info.client_ip_,
              K(ret));
        } else if (OB_FAIL(check_ssl_invited_cn(user_info->get_tenant_id(), ssl_st))) {
          LOG_WARN("check_ssl_invited_cn failed",
              "tenant_name",
              login_info.tenant_name_,
              "user_name",
              login_info.user_name_,
              "client_ip_",
              login_info.client_ip_,
              K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        s_priv.tenant_id_ = user_info->get_tenant_id();
        s_priv.user_id_ = user_info->get_user_id();
        s_priv.user_name_ = user_info->get_user_name_str();
        s_priv.host_name_ = user_info->get_host_name_str();
        s_priv.user_priv_set_ = user_info->get_priv_set();
        s_priv.db_ = login_info.db_;
        sel_user_info = user_info;
        // load role priv
        if (OB_SUCC(ret)) {
          const ObSEArray<uint64_t, 8>& role_id_array = user_info->get_role_id_array();
          CK(user_info->get_role_id_array().count() == user_info->get_role_id_option_array().count());
          for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
            const ObUserInfo* role_info = NULL;
            if (OB_FAIL(get_user_info(role_id_array.at(i), role_info))) {
              LOG_WARN("failed to get role ids", K(ret), K(role_id_array.at(i)));
            } else if (NULL == role_info) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("role info is null", K(ret), K(role_id_array.at(i)));
            } else {
              s_priv.user_priv_set_ |= role_info->get_priv_set();
              if (user_info->get_disable_option(user_info->get_role_id_option_array().at(i)) == 0) {
                OZ(add_role_id_recursively(role_id_array.at(i), s_priv));
              }
            }
          }
          OZ(add_role_id_recursively(combine_id(user_info->get_tenant_id(), OB_ORA_PUBLIC_ROLE_ID), s_priv));
        }

        // check db access and db existence
        if (!login_info.db_.empty() && OB_FAIL(check_db_access(s_priv, login_info.db_, s_priv.db_priv_set_))) {
          LOG_WARN("Database access deined", K(login_info), K(ret));
        } else {
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_ssl_access(const ObUserInfo& user_info, SSL* ssl_st)
{
  int ret = OB_SUCCESS;
  switch (user_info.get_ssl_type()) {
    case ObSSLType::SSL_TYPE_NOT_SPECIFIED:
    case ObSSLType::SSL_TYPE_NONE: {
      // do nothing
      break;
    }
    case ObSSLType::SSL_TYPE_ANY: {
      if (NULL == ssl_st) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("not use ssl", K(ret));
      }
      break;
    }
    case ObSSLType::SSL_TYPE_X509: {
      X509* cert = NULL;
      int64_t verify_result = 0;
      if (NULL == ssl_st || (X509_V_OK != (verify_result = SSL_get_verify_result(ssl_st))) ||
          (NULL == (cert = SSL_get_peer_certificate(ssl_st)))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KP(ssl_st), K(verify_result), K(ret));
      }
      X509_free(cert);
      break;
    }
    case ObSSLType::SSL_TYPE_SPECIFIED: {
      X509* cert = NULL;
      int64_t verify_result = 0;
      char* x509_issuer = NULL;
      char* x509_subject = NULL;
      if (NULL == ssl_st || (X509_V_OK != (verify_result = SSL_get_verify_result(ssl_st))) ||
          (NULL == (cert = SSL_get_peer_certificate(ssl_st)))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KP(ssl_st), K(verify_result), K(ret));
      }

      if (OB_SUCC(ret) && !user_info.get_ssl_cipher_str().empty() &&
          user_info.get_ssl_cipher_str().compare(SSL_get_cipher(ssl_st)) != 0) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 cipher check failed",
            "expect",
            user_info.get_ssl_cipher_str(),
            "receive",
            SSL_get_cipher(ssl_st),
            K(ret));
      }

      if (OB_SUCC(ret) && !user_info.get_x509_issuer_str().empty() &&
          FALSE_IT(x509_issuer = X509_NAME_oneline(X509_get_issuer_name(cert), 0, 0)) &&
          user_info.get_x509_issuer_str().compare(x509_issuer) != 0) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("x509 issue check failed", "expect", user_info.get_x509_issuer_str(), "receive", x509_issuer, K(ret));
      }

      if (OB_SUCC(ret) && !user_info.get_x509_subject_str().empty() &&
          FALSE_IT(x509_subject = X509_NAME_oneline(X509_get_subject_name(cert), 0, 0)) &&
          user_info.get_x509_subject_str().compare(x509_subject) != 0) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN(
            "x509 subject check failed", "expect", user_info.get_x509_subject_str(), "receive", x509_subject, K(ret));
      }

      OPENSSL_free(x509_issuer);
      OPENSSL_free(x509_subject);
      X509_free(cert);
      break;
    }
    default: {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("unknonw type", K(user_info), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_DEBUG("fail to check_ssl_access", K(user_info), K(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::check_ssl_invited_cn(const uint64_t tenant_id, SSL* ssl_st)
{
  int ret = OB_SUCCESS;
  if (NULL == ssl_st) {
    LOG_DEBUG("not use ssl, no need check invited_cn", K(tenant_id));
  } else {
    X509* cert = NULL;
    X509_name_st* x509Name = NULL;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get tenant_config", K(ret));
    } else {
      ObString ob_ssl_invited_common_names(tenant_config->ob_ssl_invited_common_names.str());
      if (ob_ssl_invited_common_names.empty()) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("ob_ssl_invited_common_names not match", "expect", ob_ssl_invited_common_names, K(ret));
      } else if (NULL == (cert = SSL_get_peer_certificate(ssl_st))) {
        LOG_DEBUG("use ssl, but without peer_certificate", K(tenant_id));
      } else if (OB_ISNULL(x509Name = X509_get_subject_name(cert))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", K(ret));
      } else {
        unsigned int count = X509_NAME_entry_count(x509Name);
        char name[1024] = {0};
        char* cn_used = NULL;
        for (unsigned int i = 0; i < count && NULL == cn_used; i++) {
          X509_NAME_ENTRY* entry = X509_NAME_get_entry(x509Name, i);
          OBJ_obj2txt(name, sizeof(name), X509_NAME_ENTRY_get_object(entry), 0);
          if (strcmp(name, "commonName") == 0) {
            ASN1_STRING_to_UTF8((unsigned char**)&cn_used, X509_NAME_ENTRY_get_data(entry));
          }
        }
        if (OB_ISNULL(cn_used)) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("failed to found cn", K(ret));
        } else if (NULL == strstr(ob_ssl_invited_common_names.ptr(), cn_used)) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN(
              "ob_ssl_invited_common_names not match", "expect", ob_ssl_invited_common_names, "curr", cn_used, K(ret));
        } else {
          LOG_DEBUG(
              "ob_ssl_invited_common_names match", "expect", ob_ssl_invited_common_names, "curr", cn_used, K(ret));
        }
      }
    }

    if (cert != NULL) {
      X509_free(cert);
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_access(ObSessionPrivInfo& s_priv, const ObString& database_name)
{
  int ret = OB_SUCCESS;

  uint64_t database_id = OB_INVALID_ID;
  uint64_t tenant_id = s_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(s_priv), K_(tenant_id));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    OB_LOG(WARN, "fail to get database id", K(ret), K(tenant_id), K(database_name));
  } else if (OB_INVALID_ID != database_id) {
    if (OB_FAIL(check_db_access(s_priv, database_name, s_priv.db_priv_set_))) {
      OB_LOG(WARN, "fail to check db access", K(database_name), K(ret));
    }
  } else {
    ret = OB_ERR_BAD_DATABASE;
    OB_LOG(WARN, "database not exist", K(ret), K(database_name), K(s_priv));
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
  }
  return ret;
}

/* check privilege of create session in oracle mode */
int ObSchemaGetterGuard::check_ora_conn_access(
    const uint64_t tenant_id, const uint64_t user_id, bool print_warn, const ObIArray<uint64_t>& role_id_array)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  UNUSED(print_warn);
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K_(tenant_id));
  } else {
    OZ(sql::ObOraSysChecker::check_ora_connect_priv(*this, tenant_id, user_id, role_id_array), tenant_id, user_id);
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_access(
    const ObSessionPrivInfo& session_priv, const ObString& db, ObPrivSet& db_priv_set, bool print_warn)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr* mgr = NULL;
  if (!session_priv.is_valid() || 0 == db.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(session_priv), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObPrivMgr& priv_mgr = mgr->priv_mgr_;
    ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
    db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", K(ret), K(db_priv_key));
    } else {
      bool is_grant = false;
      if (OB_FAIL(
              priv_mgr.table_grant_in_db(db_priv_key.tenant_id_, db_priv_key.user_id_, db_priv_key.db_, is_grant))) {
        LOG_WARN("check table grant in db failed", K(db_priv_key), K(ret));
      } else {
        // load db level prvilege from roles
        const ObUserInfo* user_info = NULL;
        if (OB_FAIL(get_user_info(session_priv.user_id_, user_info))) {
          LOG_WARN("failed to get user info", K(ret), K(session_priv.user_id_));
        } else if (NULL == user_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", K(ret), K(session_priv.user_id_));
        } else {
          const ObSEArray<uint64_t, 8>& role_id_array = user_info->get_role_id_array();
          bool is_grant_role = false;
          ObPrivSet total_db_priv_set_role = OB_PRIV_SET_EMPTY;
          for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
            const ObUserInfo* role_info = NULL;
            if (OB_FAIL(get_user_info(role_id_array.at(i), role_info))) {
              LOG_WARN("failed to get role ids", K(ret), K(role_id_array.at(i)));
            } else if (NULL == role_info) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("role info is null", K(ret), K(role_id_array.at(i)));
            } else {
              ObPrivSet db_priv_set_role = OB_PRIV_SET_EMPTY;
              ObOriginalDBKey db_priv_key_role(session_priv.tenant_id_, role_info->get_user_id(), db);
              if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key_role, db_priv_set_role))) {
                LOG_WARN("get db priv set failed", K(ret), K(db_priv_key_role));
              } else if (!is_grant_role && OB_FAIL(priv_mgr.table_grant_in_db(db_priv_key_role.tenant_id_,
                                               db_priv_key_role.user_id_,
                                               db_priv_key_role.db_,
                                               is_grant_role))) {
                LOG_WARN("check table grant in db failed", K(db_priv_key_role), K(ret));
              } else {
                // append db level privilege
                total_db_priv_set_role |= db_priv_set_role;
              }
            }
          }
          if (OB_SUCC(ret)) {
            // append db privilege from all roles
            db_priv_set |= total_db_priv_set_role;
            is_grant = is_grant || is_grant_role;
          }
        }
      }
      // check db level privilege
      ObWorker::CompatMode compat_mode;
      OZ(get_tenant_compat_mode(session_priv.tenant_id_, compat_mode));
      if (OB_SUCC(ret) && (compat_mode == ObWorker::CompatMode::ORACLE) &&
          (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260)) {
        /* For compatibility_mode, check if user has been granted all privileges first */
        if (((session_priv.user_priv_set_ | db_priv_set) & OB_PRIV_DB_ACC) || is_grant) {
        } else {
          OZ(check_ora_conn_access(
                 session_priv.tenant_id_, session_priv.user_id_, print_warn, session_priv.enable_role_id_array_),
              session_priv.tenant_id_,
              session_priv.user_id_);
        }
      } else {
        if (OB_FAIL(ret)) {
        } else if (((session_priv.user_priv_set_ | db_priv_set) & OB_PRIV_DB_ACC) || is_grant) {
        } else {
          ret = OB_ERR_NO_DB_PRIVILEGE;
          if (print_warn) {
            LOG_WARN("No privilege to access database", K(session_priv), K(db), K(ret));
            LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE,
                session_priv.user_name_.length(),
                session_priv.user_name_.ptr(),
                session_priv.host_name_.length(),
                session_priv.host_name_.ptr(),
                db.length(),
                db.ptr());
          }
        }
      }
    }
  }
  return ret;
}

// TODO: check arguments
int ObSchemaGetterGuard::check_db_show(const ObSessionPrivInfo& session_priv, const ObString& db, bool& allow_show)
{
  int ret = OB_SUCCESS;
  int can_show = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  allow_show = true;
  ObPrivSet db_priv_set = 0;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_SUCCESS != (can_show = check_db_access(session_priv, db, db_priv_set, false))) {
    allow_show = false;
  }
  return ret;
}

int ObSchemaGetterGuard::check_table_show(
    const ObSessionPrivInfo& session_priv, const ObString& db, const ObString& table, bool& allow_show)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr* mgr = NULL;
  allow_show = true;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, OB_PRIV_DB_ACC)) {
    // allow
  } else {
    const ObPrivMgr& priv_mgr = mgr->priv_mgr_;

    const ObTablePriv* table_priv = NULL;
    ObPrivSet db_priv_set = 0;
    // get db_priv_set
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // check db_priv_set, then check table_priv_set
      if (OB_PRIV_HAS_ANY(db_priv_set, OB_PRIV_DB_ACC)) {
        // allow
      } else {
        ObTablePrivSortKey table_priv_key(session_priv.tenant_id_, session_priv.user_id_, db, table);
        if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
          LOG_WARN("get table priv failed", K(table_priv_key), K(ret));
        } else if (NULL != table_priv && OB_PRIV_HAS_ANY(table_priv->get_priv_set(), OB_PRIV_TABLE_ACC)) {
          // allow
        } else {
          allow_show = false;
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_user_priv(const ObSessionPrivInfo& session_priv, const ObPrivSet priv_set)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr* mgr = NULL;
  ObPrivSet user_priv_set = session_priv.user_priv_set_;

  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (!OB_TEST_PRIVS(user_priv_set, priv_set)) {
    if ((priv_set == OB_PRIV_ALTER_TENANT || priv_set == OB_PRIV_ALTER_SYSTEM ||
            priv_set == OB_PRIV_CREATE_RESOURCE_POOL || priv_set == OB_PRIV_CREATE_RESOURCE_UNIT) &&
        (OB_TEST_PRIVS(user_priv_set, OB_PRIV_SUPER))) {
    } else {
      ret = OB_ERR_NO_PRIVILEGE;
    }
  }
  if (OB_ERR_NO_PRIVILEGE == ret) {
    ObPrivSet lack_priv_set = priv_set & (~user_priv_set);
    const ObPrivMgr& priv_mgr = mgr->priv_mgr_;
    const char* priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
    if (OB_ISNULL(priv_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid priv type", "priv_set", lack_priv_set);
    } else {
      if (priv_set == OB_PRIV_ALTER_TENANT || priv_set == OB_PRIV_ALTER_SYSTEM ||
          priv_set == OB_PRIV_CREATE_RESOURCE_POOL || priv_set == OB_PRIV_CREATE_RESOURCE_UNIT) {
        char priv_name_with_preffix[common::ObLogger::MAX_LOG_SIZE] = "SUPER or ";
        strcat(priv_name_with_preffix, priv_name);
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name_with_preffix);
      } else {
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name);
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_single_table_priv(
    const ObSessionPrivInfo& session_priv, const ObNeedPriv& table_need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr* mgr = NULL;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_, "user_id", session_priv.user_id_, K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    // first:check user and db priv.
    // second:If user_db_priv_set has no enough privileges, check table priv.
    const ObPrivMgr& priv_mgr = mgr->priv_mgr_;
    if (!OB_TEST_PRIVS(session_priv.user_priv_set_, table_need_priv.priv_set_)) {
      ObPrivSet user_db_priv_set = 0;
      if (OB_SUCCESS != check_db_priv(session_priv, table_need_priv.db_, table_need_priv.priv_set_, user_db_priv_set)) {
        // 1. fetch table priv
        const ObTablePriv* table_priv = NULL;
        ObPrivSet table_priv_set = 0;
        bool is_table_priv_empty = true;
        ObTablePrivSortKey table_priv_key(
            session_priv.tenant_id_, session_priv.user_id_, table_need_priv.db_, table_need_priv.table_);
        if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
          LOG_WARN("get table priv failed", K(ret), K(table_priv_key));
        } else if (NULL != table_priv) {
          table_priv_set = table_priv->get_priv_set();
          is_table_priv_empty = false;
        }

        if (OB_SUCC(ret)) {
          // 2. fetch roles privs
          const ObUserInfo* user_info = NULL;
          if (OB_FAIL(get_user_info(session_priv.user_id_, user_info))) {
            LOG_WARN("failed to get user info", K(ret), K(session_priv.user_id_));
          } else if (NULL == user_info) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("user info is null", K(ret), K(session_priv.user_id_));
          } else {
            const ObSEArray<uint64_t, 8>& role_id_array = user_info->get_role_id_array();
            for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
              const ObUserInfo* role_info = NULL;
              const ObTablePriv* role_table_priv = NULL;
              if (OB_FAIL(get_user_info(role_id_array.at(i), role_info))) {
                LOG_WARN("failed to get role ids", K(ret), K(role_id_array.at(i)));
              } else if (NULL == role_info) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("role info is null", K(ret), K(role_id_array.at(i)));
              } else {
                ObTablePrivSortKey role_table_priv_key(
                    session_priv.tenant_id_, role_info->get_user_id(), table_need_priv.db_, table_need_priv.table_);
                if (OB_FAIL(priv_mgr.get_table_priv(role_table_priv_key, role_table_priv))) {
                  LOG_WARN("get table priv failed", K(ret), K(role_table_priv_key));
                } else if (NULL != role_table_priv) {
                  is_table_priv_empty = false;
                  // append additional role
                  table_priv_set |= role_table_priv->get_priv_set();
                }
              }
            }
          }
        }

        // 3. check privs
        if (OB_SUCC(ret)) {
          if (is_table_priv_empty) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_WARN("No privilege, cannot find table priv info",
                "tenant_id",
                session_priv.tenant_id_,
                "user_id",
                session_priv.user_id_,
                K(table_need_priv));
          } else if (!OB_TEST_PRIVS(table_priv_set | user_db_priv_set, table_need_priv.priv_set_)) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_WARN("No privilege",
                "tenant_id",
                session_priv.tenant_id_,
                "user_id",
                session_priv.user_id_,
                K(table_need_priv),
                K(table_priv_set | user_db_priv_set));
          }
        }
        if (OB_ERR_NO_TABLE_PRIVILEGE == ret) {
          ObPrivSet lack_priv_set = table_need_priv.priv_set_ & (~(table_priv_set | user_db_priv_set));
          const char* priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
          if (OB_ISNULL(priv_name)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid priv type", "priv_set", table_need_priv.priv_set_);
          } else {
            LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE,
                (int)strlen(priv_name),
                priv_name,
                session_priv.user_name_.length(),
                session_priv.user_name_.ptr(),
                session_priv.host_name_.length(),
                session_priv.host_name_.ptr(),
                table_need_priv.table_.length(),
                table_need_priv.table_.ptr());
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_priv(
    const ObSessionPrivInfo& session_priv, const ObString& db, const ObPrivSet need_priv, ObPrivSet& user_db_priv_set)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr* mgr = NULL;
  ObPrivSet total_db_priv_set_role = OB_PRIV_SET_EMPTY;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_, "user_id", session_priv.user_id_, K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ObPrivSet db_priv_set = 0;
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      const ObPrivMgr& priv_mgr = mgr->priv_mgr_;
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), K(ret));
      }
    }
    /* load role db privs */
    if (OB_SUCC(ret)) {
      const ObUserInfo* user_info = NULL;
      // bool is_grant_role = false;
      OZ(get_user_info(session_priv.user_id_, user_info), session_priv.user_id_);
      CK(OB_NOT_NULL(user_info));
      if (OB_SUCC(ret)) {
        const ObSEArray<uint64_t, 8>& role_id_array = user_info->get_role_id_array();
        for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
          const ObUserInfo* role_info = NULL;
          if (OB_FAIL(get_user_info(role_id_array.at(i), role_info))) {
            LOG_WARN("failed to get role ids", K(ret), K(role_id_array.at(i)));
          } else if (NULL == role_info) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role info is null", K(ret), K(role_id_array.at(i)));
          } else {
            ObPrivSet db_priv_set_role = OB_PRIV_SET_EMPTY;
            ObOriginalDBKey db_priv_key_role(session_priv.tenant_id_, role_info->get_user_id(), db);
            if (OB_FAIL(get_db_priv_set(db_priv_key_role, db_priv_set_role))) {
              LOG_WARN("get db priv set failed", K(ret), K(db_priv_key_role));
            } else {
              // append db level privilege
              total_db_priv_set_role |= db_priv_set_role;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      user_db_priv_set = session_priv.user_priv_set_ | db_priv_set | total_db_priv_set_role;
      if (!OB_TEST_PRIVS(user_db_priv_set, need_priv)) {
        ret = OB_ERR_NO_DB_PRIVILEGE;
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_priv(
    const ObSessionPrivInfo& session_priv, const common::ObString& db, const ObPrivSet need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (!OB_TEST_PRIVS(session_priv.user_priv_set_, need_priv)) {
    ObPrivSet user_db_priv_set = 0;
    if (OB_FAIL(check_db_priv(session_priv, db, need_priv, user_db_priv_set))) {
      LOG_WARN("No db priv", "tenant_id", session_priv.tenant_id_, "user_id", session_priv.user_id_, K(db), K(ret));
      if (OB_ERR_NO_DB_PRIVILEGE == ret) {
        LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE,
            session_priv.user_name_.length(),
            session_priv.user_name_.ptr(),
            session_priv.host_name_.length(),
            session_priv.host_name_.ptr(),
            db.length(),
            db.ptr());
      }
    }
  }
  return ret;
}

/* check all needed privileges of object*/
int ObSchemaGetterGuard::check_single_obj_priv(const uint64_t tenant_id, const uint64_t uid,
    const ObOraNeedPriv& need_priv, const ObIArray<uint64_t>& role_id_array)
{
  int ret = OB_SUCCESS;
  uint64_t uid_to_be_check;
  bool exists = false;
  for (int i = OBJ_PRIV_ID_NONE; OB_SUCC(ret) && i < OBJ_PRIV_ID_MAX; i++) {
    OZ(share::ObOraPrivCheck::raw_obj_priv_exists(i, need_priv.obj_privs_, exists));
    if (OB_SUCC(ret) && exists) {
      uid_to_be_check = need_priv.grantee_id_ == common::OB_INVALID_ID ? uid : need_priv.grantee_id_;
      OZ(sql::ObOraSysChecker::check_ora_obj_priv(*this,
             tenant_id,
             uid_to_be_check,
             need_priv.db_name_,
             need_priv.obj_id_,
             need_priv.col_id_,
             need_priv.obj_type_,
             i,
             need_priv.check_flag_,
             need_priv.owner_id_,
             role_id_array),
          tenant_id,
          uid_to_be_check,
          need_priv.db_name_,
          need_priv.obj_id_,
          need_priv.col_id_,
          need_priv.obj_type_,
          i,
          need_priv.check_flag_,
          need_priv.owner_id_,
          need_priv.grantee_id_);
    }
  }
  return ret;
}

/* check all privileges of stmt */
int ObSchemaGetterGuard::check_ora_priv(const uint64_t tenant_id, const uint64_t uid,
    const ObStmtOraNeedPrivs& stmt_need_privs, const ObIArray<uint64_t>& role_id_array)
{
  int ret = OB_SUCCESS;
  const ObStmtOraNeedPrivs::OraNeedPrivs& need_privs = stmt_need_privs.need_privs_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObOraNeedPriv& need_priv = need_privs.at(i);
      if (OB_FAIL(check_single_obj_priv(tenant_id, uid, need_priv, role_id_array))) {
        LOG_WARN("No privilege",
            "tenant_id",
            tenant_id,
            "user_id",
            uid,
            "need_priv",
            need_priv.obj_privs_,
            "obj id",
            need_priv.obj_id_,
            K(ret));  // need print priv
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_priv(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs)
{
  int ret = OB_SUCCESS;
  const ObStmtNeedPrivs::NeedPrivs& need_privs = stmt_need_privs.need_privs_;
  uint64_t tenant_id = session_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (session_priv.is_valid()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv& need_priv = need_privs.at(i);
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          if (OB_FAIL(check_user_priv(session_priv, need_priv.priv_set_))) {
            LOG_WARN("No privilege",
                "tenant_id",
                session_priv.tenant_id_,
                "user_id",
                session_priv.user_id_,
                "need_priv",
                need_priv.priv_set_,
                "user_priv",
                session_priv.user_priv_set_,
                K(ret));  // need print priv
          }
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(check_db_priv(session_priv, need_priv.db_, need_priv.priv_set_))) {
            LOG_WARN("No privilege",
                "tenant_id",
                session_priv.tenant_id_,
                "user_id",
                session_priv.user_id_,
                "need_priv",
                need_priv.priv_set_,
                "user_priv",
                session_priv.user_priv_set_,
                K(ret));  // need print priv
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_FAIL(check_single_table_priv(session_priv, need_priv))) {
            LOG_WARN("No privilege",
                "tenant_id",
                session_priv.tenant_id_,
                "user_id",
                session_priv.user_id_,
                "need_priv",
                need_priv.priv_set_,
                "table",
                need_priv.table_,
                "db",
                need_priv.db_,
                "user_priv",
                session_priv.user_priv_set_,
                K(ret));  // need print priv
          }
          break;
        }
        case OB_PRIV_DB_ACCESS_LEVEL: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Privilege checking of database access should not use this function", K(ret));
          break;
        }
        default: {
          break;
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObSchemaGetterGuard::check_priv_or(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs)
{
  int ret = OB_SUCCESS;

  const ObStmtNeedPrivs::NeedPrivs& need_privs = stmt_need_privs.need_privs_;
  bool pass = false;
  ObPrivLevel max_priv_level = OB_PRIV_INVALID_LEVEL;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr* mgr = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (session_priv.is_valid()) {
    for (int64_t i = 0; !pass && OB_SUCCESS == ret && i < need_privs.count(); ++i) {
      const ObNeedPriv& need_priv = need_privs.at(i);
      if (need_priv.priv_level_ > max_priv_level) {
        max_priv_level = need_priv.priv_level_;
      }
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          pass = OB_PRIV_HAS_ANY(session_priv.user_priv_set_, need_priv.priv_set_);
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          pass = OB_PRIV_HAS_ANY(session_priv.db_priv_set_, need_priv.priv_set_);
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          const ObPrivMgr& priv_mgr = mgr->priv_mgr_;
          const ObTablePriv* table_priv = NULL;
          ObTablePrivSortKey table_priv_key(
              session_priv.tenant_id_, session_priv.user_id_, need_priv.db_, need_priv.table_);
          if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
            LOG_WARN("get table priv failed", K(ret), K(table_priv_key));
          } else if (NULL != table_priv) {
            pass = OB_PRIV_HAS_ANY(table_priv->get_priv_set(), need_priv.priv_set_);
          }
          break;
        }
        case OB_PRIV_DB_ACCESS_LEVEL: {
          // this should not occur
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should not reach here", K(ret));
          break;
        }
        default: {
          break;
        }
      }
    }
    if (!pass) {
      // User log is printed outside
      switch (max_priv_level) {
        case OB_PRIV_USER_LEVEL: {
          ret = OB_ERR_NO_PRIVILEGE;
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          ret = OB_ERR_NO_DB_PRIVILEGE;
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          ret = OB_ERR_NO_TABLE_PRIVILEGE;
          break;
        }
        default: {
          // this should not occur
          ret = OB_INVALID_ARGUMENT;
          break;
        }
          LOG_WARN("Or-ed privilege check not passed",
              "tenant id",
              session_priv.tenant_id_,
              "user id",
              session_priv.user_id_);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObSchemaGetterGuard::get_db_priv_set(
    const uint64_t tenant_id, const uint64_t user_id, const ObString& db, ObPrivSet& priv_set)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_priv_set(ObOriginalDBKey(tenant_id, user_id, db), priv_set))) {
    LOG_WARN("fail to get db priv set", K(ret), K(tenant_id), K(user_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_db_priv_set(const ObOriginalDBKey& db_priv_key, ObPrivSet& priv_set, bool is_pattern)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  uint64_t tenant_id = db_priv_key.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_priv_set(db_priv_key, priv_set, is_pattern))) {
    LOG_WARN("fail to get dv priv set", K(ret), K(db_priv_key));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_priv_set(const ObTablePrivSortKey& table_priv_key, ObPrivSet& priv_set)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  uint64_t tenant_id = table_priv_key.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_table_priv_set(table_priv_key, priv_set))) {
    LOG_WARN("fail to get table priv set", K(ret), K(table_priv_key));
  }
  return ret;
}

int ObSchemaGetterGuard::get_obj_privs(const ObObjPrivSortKey& obj_priv_key, ObPackedObjPriv& obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  uint64_t tenant_id = obj_priv_key.tenant_id_;
  const ObObjPriv* obj_priv = NULL;
  obj_privs = 0;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_priv(obj_priv_key, obj_priv))) {
    LOG_WARN("fail to get table priv set", K(ret), K(obj_priv_key));
  } else if (obj_priv != NULL) {
    obj_privs = obj_priv->get_obj_privs();
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_infos_with_tenant_id(
    const uint64_t tenant_id, common::ObIArray<const ObUserInfo*>& user_infos)
{
  int ret = OB_SUCCESS;
  user_infos.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_user_schemas_in_tenant(tenant_id, user_infos))) {
    LOG_WARN("get user schemas in tenant failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_db_priv_with_tenant_id(const uint64_t tenant_id, ObIArray<const ObDBPriv*>& db_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  db_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_privs_in_tenant(tenant_id, db_privs))) {
    LOG_WARN("get db priv with tenant_id failed", K(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_db_priv_with_user_id(
    const uint64_t tenant_id, const uint64_t user_id, ObIArray<const ObDBPriv*>& db_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  db_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_privs_in_user(tenant_id, user_id, db_privs))) {
    LOG_WARN("get db priv with user_id failed", K(ret), K(tenant_id), K(user_id));
  }

  return ret;
}

// TODO: Based on the following reasons, we don't maintain privileges of normal tenant's system tables in tenant space.
// 1. Normal tenant can't read/write system table in tenant space directly.
// 2. We don't check privileges if we query normal tenant's system tables with inner sql.
// 3. We check system tenant's privileges if we query normal tenant's system table after we execute change tenant cmd.
int ObSchemaGetterGuard::get_table_priv_with_tenant_id(
    const uint64_t tenant_id, ObIArray<const ObTablePriv*>& table_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  table_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_table_privs_in_tenant(tenant_id, table_privs))) {
    LOG_WARN("get table priv with tenant_id failed", K(ret), K(tenant_id));
  }

  return ret;
}

// TODO: Based on the following reasons, we don't maintain privileges of normal tenant's system tables in tenant space.
// 1. Normal tenant can't read/write system table in tenant space directly.
// 2. We don't check privileges if we query normal tenant's system tables with inner sql.
// 3. We check system tenant's privileges if we query normal tenant's system table after we execute change tenant cmd.
int ObSchemaGetterGuard::get_table_priv_with_user_id(
    const uint64_t tenant_id, const uint64_t user_id, ObIArray<const ObTablePriv*>& table_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  table_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_table_privs_in_user(tenant_id, user_id, table_privs))) {
    LOG_WARN("get table priv with user_id failed", K(ret), K(tenant_id), K(user_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_priv_with_grantee_id(
    const uint64_t tenant_id, const uint64_t grantee_id, ObIArray<const ObObjPriv*>& obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  obj_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == grantee_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(grantee_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantee(tenant_id, grantee_id, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", K(ret), K(tenant_id), K(grantee_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_priv_with_grantor_id(
    const uint64_t tenant_id, const uint64_t grantor_id, ObIArray<const ObObjPriv*>& obj_privs, bool reset_flag)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  if (reset_flag) {
    obj_privs.reset();
  }

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == grantor_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(grantor_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantor(tenant_id, grantor_id, obj_privs, reset_flag))) {
    LOG_WARN("get obj priv with grantee_id failed", K(ret), K(tenant_id), K(grantor_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_priv_with_obj_id(const uint64_t tenant_id, const uint64_t obj_id,
    const uint64_t obj_type, ObIArray<const ObObjPriv*>& obj_privs, bool reset_flag)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  if (reset_flag) {
    obj_privs.reset();
  }

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == obj_id || OB_INVALID_ID == obj_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(obj_id), K(obj_type));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_obj(tenant_id, obj_id, obj_type, obj_privs, reset_flag))) {
    LOG_WARN("get obj priv with grantee_id failed", K(ret), K(tenant_id), K(obj_id), K(obj_type));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_privs_in_ur_and_obj(
    const uint64_t tenant_id, const ObObjPrivSortKey& obj_key, ObPackedObjPriv& obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!obj_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_key));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_ur_and_obj(tenant_id, obj_key, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", K(ret), K(tenant_id), K(obj_key));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_privs_in_grantor_ur_obj_id(
    const uint64_t tenant_id, const ObObjPrivSortKey& obj_key, common::ObIArray<const ObObjPriv*>& obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!obj_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_key));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantor_ur_obj_id(tenant_id, obj_key, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", K(ret), K(tenant_id), K(obj_key));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_privs_in_grantor_obj_id(
    const uint64_t tenant_id, const ObObjPrivSortKey& obj_key, common::ObIArray<const ObObjPriv*>& obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!obj_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_key));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantor_obj_id(tenant_id, obj_key, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", K(ret), K(tenant_id), K(obj_key));
  }

  return ret;
}

inline bool ObSchemaGetterGuard::check_inner_stat() const
{
  bool ret = true;
  if (!is_inited_) {
    ret = false;
    LOG_WARN("schema guard not inited", K(ret));
  } else if (NULL == schema_service_ || (!is_schema_splited() && snapshot_version_ < 0) ||
             INVALID_SCHEMA_GUARD_TYPE == schema_guard_type_) {
    ret = false;
    LOG_WARN("invalid inner stat", K(schema_service_), K(snapshot_version_), K_(schema_guard_type), K(lbt()));
  }
  return ret;
}

// OB_INVALID_VERSION means schema doesn't exist.
int ObSchemaGetterGuard::get_schema_version_v2(
    const ObSchemaType schema_type, const uint64_t schema_id, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!is_normal_schema(schema_type) || OB_INVALID_ID == schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_type), K(schema_id));
  } else {
#define GET_SCHEMA_VERSION(SCHEMA, SCHEMA_TYPE)                                         \
  const SCHEMA_TYPE* schema = NULL;                                                     \
  const ObSchemaMgr* mgr = NULL;                                                        \
  const uint64_t tenant_id = extract_tenant_id(schema_id);                              \
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                  \
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id)); \
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {                               \
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));                         \
  } else if (OB_FAIL(mgr->get_##SCHEMA##_schema(schema_id, schema))) {                  \
    LOG_WARN("get " #SCHEMA " schema failed", K(ret), K(schema_id));                    \
  } else if (OB_NOT_NULL(schema)) {                                                     \
    schema_version = schema->get_schema_version();                                      \
  }
    switch (schema_type) {
      case TENANT_SCHEMA: {
        const ObSimpleTenantSchema* schema = NULL;
        const ObSchemaMgr* mgr = NULL;
        if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
          LOG_WARN("fail to check lazy guard", K(ret), K(schema_id));
        } else if (OB_FAIL(mgr->get_tenant_schema(schema_id, schema))) {
          LOG_WARN("get tenant schema failed", K(ret), K(schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
        }
        break;
      }
      case USER_SCHEMA: {
        GET_SCHEMA_VERSION(user, ObSimpleUserSchema);
        break;
      }
      case DATABASE_SCHEMA: {
        GET_SCHEMA_VERSION(database, ObSimpleDatabaseSchema);
        break;
      }
      case TABLEGROUP_SCHEMA: {
        GET_SCHEMA_VERSION(tablegroup, ObSimpleTablegroupSchema);
        break;
      }
      case TABLE_SCHEMA: {
        const ObSimpleTenantSchema* tenant_schema = NULL;
        const ObSimpleTableSchemaV2* schema = NULL;
        const ObSchemaMgr* mgr = NULL;
        // system table's schema is always stored in system tenant.
        uint64_t tenant_id = extract_tenant_id(schema_id);
        uint64_t fetch_tenant_id = tenant_id;
        uint64_t fetch_schema_id = schema_id;
        bool need_change = false;
        if (OB_FAIL(need_change_schema_id(schema_type, schema_id, need_change))) {
          LOG_WARN("fail to check if schema_id need change", K(ret), K(schema_type), K(schema_id));
        } else if (need_change) {
          fetch_tenant_id = OB_SYS_TENANT_ID;
          fetch_schema_id = combine_id(OB_SYS_TENANT_ID, schema_id);
        }
        if (OB_FAIL(ret)) {
        } else if (is_fake_table(schema_id)) {
          // fake table, we should avoid error in such situation.
          schema_version = OB_INVALID_VERSION;
        } else if (!check_fetch_table_id(schema_id)) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN(
              "fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(schema_id));
        } else if (OB_FAIL(check_tenant_schema_guard(fetch_tenant_id))) {
          LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
        } else if (OB_FAIL(check_lazy_guard(fetch_tenant_id, mgr))) {
          LOG_WARN("fail to check lazy guard", K(ret), K(fetch_tenant_id), K(tenant_id));
        } else if (OB_FAIL(mgr->get_table_schema(fetch_schema_id, schema))) {
          LOG_WARN("get table schema failed", K(ret), K(schema_id), K(fetch_schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
        } else if (fetch_schema_id != schema_id) {  // system table in tenant space
          // First, we try to use system tenant's corresponding table's schema version.
          // Because system tenant's system table in tenant space will be created last when cluster is in upgradation,
          // we can use normal tenant's schema version instead if corresponding table is not finded in system tenant.
          if (OB_FAIL(mgr->get_table_schema(schema_id, schema))) {
            LOG_WARN("get table schema failed", K(ret), K(schema_id), K(fetch_schema_id));
          } else if (OB_NOT_NULL(schema)) {
            schema_version = schema->get_schema_version();
          }
        }
        if (OB_SUCC(ret) && need_change && OB_INVALID_VERSION != schema_version) {
          // check if normal tenant exists
          if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
            LOG_WARN("fail to check lazy guard", K(ret), K(schema_id));
          } else if (OB_FAIL(mgr->get_tenant_schema(tenant_id, tenant_schema))) {
            LOG_WARN("get tenant schema failed", K(ret), K(schema_id));
          } else if (OB_ISNULL(tenant_schema)) {
            schema_version = OB_INVALID_VERSION;
          }
        }
        break;
      }
      case SYNONYM_SCHEMA: {
        GET_SCHEMA_VERSION(synonym, ObSimpleSynonymSchema);
        break;
      }
      case SEQUENCE_SCHEMA: {
        GET_SCHEMA_VERSION(sequence, ObSequenceSchema);
        break;
      }
      case SYS_VARIABLE_SCHEMA: {
        const ObSimpleSysVariableSchema* schema = NULL;
        const ObSchemaMgr* mgr = NULL;
        const uint64_t tenant_id = schema_id;
        if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
          LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
        } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
          LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
        } else if (OB_FAIL(mgr->sys_variable_mgr_.get_sys_variable_schema(schema_id, schema))) {
          LOG_WARN("get sys variable schema failed", K(ret), K(schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
          LOG_DEBUG("get sys variable schema",
              K(ret),
              K(schema_id),
              K(*schema),
              "snapshot_version",
              mgr->get_schema_version());
        }
        break;
      }
      case PROFILE_SCHEMA: {
        GET_SCHEMA_VERSION(profile, ObProfileSchema);
        break;
      }
      case DBLINK_SCHEMA: {
        GET_SCHEMA_VERSION(dblink, ObDbLinkSchema);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not reach here", K(ret));
        break;
      }
    }
#undef GET_SCHEMA_VERSION
  }
  LOG_DEBUG("get schema version v2", K(ret), K(schema_type), K(schema_id), K(schema_version), K(lbt()));
  return ret;
}

template <typename T>
int ObSchemaGetterGuard::get_from_local_cache(
    const ObSchemaType schema_type, const uint64_t schema_id, const T*& schema)
{
  int ret = OB_SUCCESS;
  schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == schema_id || !is_normal_schema(schema_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_id), K(schema_type));
  } else if (INT64_MAX == snapshot_version_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    const ObSchema* tmp_schema = NULL;
    bool found = false;
    FOREACH_CNT_X(id_schema, schema_objs_, !found)
    {
      if (id_schema->schema_type_ == schema_type && id_schema->schema_id_ == schema_id) {
        tmp_schema = id_schema->schema_;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_DEBUG("local cache miss [id to schema]", K(schema_type), K(schema_id));
    } else {
      // LOG_DEBUG("local cache hit [id to schema]",
      //          K(schema_type), K(schema_id));
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp schema is NULL", K(ret), K(tmp_schema));
      } else {
        schema = static_cast<const T*>(tmp_schema);
        LOG_DEBUG("schema cache hit", K(schema_id));
      }
    }
  }

  return ret;
}

template <typename T>
int ObSchemaGetterGuard::get_schema(
    const ObSchemaType schema_type, const uint64_t schema_id, const int64_t schema_version, const T*& schema)
{
  int ret = OB_SUCCESS;
  const ObSchema* tmp_schema = NULL;
  ObKVCacheHandle handle;
  const ObSchemaMgr* mgr = mgr_;
  ObRefreshSchemaStatus schema_status;
  schema = NULL;

  uint64_t tenant_id = extract_tenant_id(schema_id);
  if (TENANT_SCHEMA == schema_type || SYS_VARIABLE_SCHEMA == schema_type) {
    tenant_id = schema_id;
  }

  uint64_t fetch_tenant_id = tenant_id;
  bool need_change = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(need_change_schema_id(schema_type, schema_id, need_change))) {
    LOG_WARN("fail to check if schema_id need change", K(ret), K(schema_type), K(schema_id));
  } else if (need_change || TENANT_SCHEMA == schema_type) {
    fetch_tenant_id = OB_SYS_TENANT_ID;
  }

  if (OB_FAIL(ret)) {
  } else if (!is_normal_schema(schema_type) || OB_INVALID_ID == schema_id || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_type), K(schema_id), K(schema_version));
  } else if (OB_FAIL(get_schema_mgr(fetch_tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id), K(fetch_tenant_id));
  } else if (OB_FAIL(get_schema_status(fetch_tenant_id, schema_status))) {
    // This function is only be called when ObSchemaGetterGuard is not in lazy mode.
    LOG_WARN("fail to get schema status", K(ret), K(fetch_tenant_id), K(schema_type));
  } else if (is_standby_cluster() && OB_SYS_TENANT_ID != fetch_tenant_id &&
             OB_INVALID_VERSION != schema_status.readable_schema_version_ &&
             schema_status.readable_schema_version_ < schema_version) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("schema_version is not readable now, try later",
        K(ret),
        K(tenant_id),
        K(schema_type),
        K(schema_status),
        K(schema_version));
  } else if (OB_FAIL(schema_service_->get_schema(
                 mgr, schema_status, schema_type, schema_id, schema_version, handle, tmp_schema))) {
    LOG_WARN("get schema failed", K(ret), K(schema_type), K(schema_id), K(schema_version));
  } else if (NULL != tmp_schema) {
    SchemaObj schema_obj;
    schema = static_cast<const T*>(tmp_schema);
    schema_obj.schema_type_ = schema_type;
    schema_obj.schema_id_ = schema_id;
    schema_obj.schema_ = const_cast<ObSchema*>(tmp_schema);
    schema_obj.handle_ = handle;
    if (OB_FAIL(schema_objs_.push_back(schema_obj))) {
      LOG_WARN("add handle failed", K(ret));
    }
  }

  if (OB_FAIL(ret) && OB_TENANT_HAS_BEEN_DROPPED != ret && ObSchemaService::g_liboblog_mode_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(schema_service_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else {
      uint64_t tenant_id = extract_tenant_id(schema_id);
      if (TENANT_SCHEMA == schema_type || SYS_VARIABLE_SCHEMA == schema_type) {
        tenant_id = schema_id;
      }
      TenantStatus tenant_status = TENANT_STATUS_INVALID;
      tmp_ret = schema_service_->query_tenant_status(tenant_id, tenant_status);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("query tenant status failed", K(ret), K(tmp_ret), K(tenant_id));
      } else if (TENANT_DELETED == tenant_status) {
        LOG_INFO("tenant has been dropped, no need retry", K(ret), K(tenant_id));
        ret = OB_TENANT_HAS_BEEN_DROPPED;  // overwrite ret
      }
    }
  }
  return ret;
}

// specified_version will be overwrited if we try to get normal tenant's system table schema.
template <typename T>
int ObSchemaGetterGuard::get_schema_v2(const ObSchemaType schema_type, const uint64_t schema_id, const T*& schema,
    int64_t specified_version /*=OB_INVALID_VERSION*/)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  const ObSchemaMgr* mgr = NULL;
  schema = NULL;

  uint64_t tenant_id = extract_tenant_id(schema_id);
  if (TENANT_SCHEMA == schema_type || SYS_VARIABLE_SCHEMA == schema_type) {
    tenant_id = schema_id;
  }

  uint64_t fetch_tenant_id = tenant_id;
  bool need_change = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if ((TABLE_SCHEMA == schema_type || TABLE_SIMPLE_SCHEMA == schema_type) && !check_fetch_table_id(schema_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(schema_id));
  } else if (OB_FAIL(need_change_schema_id(schema_type, schema_id, need_change))) {
    LOG_WARN("fail to check if schema_id need change", K(ret), K(schema_type), K(schema_id));
  } else if (need_change || TENANT_SCHEMA == schema_type) {
    fetch_tenant_id = OB_SYS_TENANT_ID;
  }

  if (OB_FAIL(ret)) {
  } else if (!is_normal_schema(schema_type) || OB_INVALID_ID == schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_type), K(schema_id));
  } else if (OB_FAIL(get_schema_mgr(fetch_tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id), K(fetch_tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(mgr)) {
    if (TABLE_SIMPLE_SCHEMA == schema_type) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("should fetch simple table schema in lazy mode", K(ret), K(schema_id), K(specified_version));
    } else if (OB_FAIL(get_from_local_cache(schema_type, schema_id, schema)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get from local cache failed [id to schema]", K(ret), K(schema_type), K(schema_id));
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_INVALID_VERSION != specified_version && !need_change) {
        schema_version = specified_version;
      } else if (OB_FAIL(get_schema_version_v2(schema_type, schema_id, schema_version))) {
        LOG_WARN("get schema version failed", K(ret), K(schema_type), K(schema_id));
      }
      if (OB_SUCC(ret)) {
        if (OB_INVALID_VERSION == schema_version) {
          if (is_fake_table(schema_id)) {
            LOG_INFO("invalid version", K(schema_type), K(schema_id));
          }
        } else if (OB_FAIL(get_schema(schema_type, schema_id, schema_version, schema))) {
          LOG_WARN("get schema failed",
              K(ret),
              K(schema_type),
              K(schema_id),
              K(schema_version),
              "snapshot_version",
              mgr->get_schema_version());
        } else if (NULL == schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr, unexpected", K(ret), K(schema_type), K(schema_id));
        } else {
        }
      }
    }
  } else {
    LOG_DEBUG("fetch schema in lazy mode",
        K(schema_type),
        K(schema_id),
        K(snapshot_version_),
        "schema_guard_tenant_id",
        tenant_id_,
        K(fetch_tenant_id),
        K(snapshot_version_));
    // new lazy logic
    bool founded = false;
    for (int64_t i = 0; i < schema_objs_.count(); ++i) {
      SchemaObj& obj = schema_objs_.at(i);
      if (obj.schema_type_ == schema_type && obj.schema_id_ == schema_id) {
        schema = static_cast<T*>(obj.schema_);
        founded = true;
        break;
      }
    }
    if (!founded) {
      const ObSchema* base_schema = NULL;
      ObKVCacheHandle handle;
      ObRefreshSchemaStatus schema_status;
      if (is_schema_splited()) {
        schema_status.tenant_id_ = fetch_tenant_id;
        if (is_standby_cluster() && OB_SYS_TENANT_ID != fetch_tenant_id) {
          ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
          if (OB_ISNULL(GCTX.sql_proxy_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("proxy is null", K(ret));
          } else if (OB_ISNULL(schema_status_proxy)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema_status_proxy is null", K(ret));
          } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(fetch_tenant_id, schema_status))) {
            LOG_WARN("fail to get refresh schema status", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_schema_version(fetch_tenant_id, schema_version))) {
        LOG_INFO("fail to get snapshot version", K(ret));
      } else if (is_standby_cluster() && OB_SYS_TENANT_ID != fetch_tenant_id &&
                 OB_INVALID_VERSION != schema_status.readable_schema_version_ &&
                 schema_status.readable_schema_version_ < schema_version) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN(
            "schema_version is not readable now, try later", K(ret), K(tenant_id), K(schema_type), K(schema_status));
      } else if (OB_FAIL(schema_service_->get_schema(
                     NULL, schema_status, schema_type, schema_id, schema_version, handle, base_schema))) {
        LOG_WARN("get schema failed", K(schema_type), K(schema_id), K(schema_version), K(ret));
      } else {
        SchemaObj schema_obj;
        schema_obj.schema_type_ = schema_type;
        schema_obj.schema_id_ = schema_id;
        schema_obj.schema_ = NULL;
        if (base_schema != NULL) {
          schema_obj.schema_ = const_cast<ObSchema*>(base_schema);
          schema_obj.handle_ = handle;
          schema = static_cast<const T*>(base_schema);
        }
        ret = schema_objs_.push_back(schema_obj);
      }
    }
  }

  if (OB_FAIL(ret) && OB_TENANT_HAS_BEEN_DROPPED != ret && ObSchemaService::g_liboblog_mode_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(schema_service_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret), K(tmp_ret));
    } else {
      uint64_t tenant_id = extract_tenant_id(schema_id);
      if (TENANT_SCHEMA == schema_type || SYS_VARIABLE_SCHEMA == schema_type) {
        tenant_id = schema_id;
      }
      TenantStatus tenant_status = TENANT_STATUS_INVALID;
      tmp_ret = schema_service_->query_tenant_status(tenant_id, tenant_status);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("query tenant status failed", K(ret), K(tmp_ret), K(tenant_id));
      } else if (TENANT_DELETED == tenant_status) {
        LOG_INFO("tenant has been dropped, no need retry", K(ret), K(tenant_id));
        ret = OB_TENANT_HAS_BEEN_DROPPED;  // overwrite ret
      }
    }
  }
  return ret;
}

const ObUserInfo* ObSchemaGetterGuard::get_user_info(const uint64_t user_id)
{
  const ObUserInfo* user_info = NULL;
  get_user_info(user_id, user_info);
  return user_info;
}

#if 0
int ObSchemaGetterGuard::get_database_schema(const uint64_t database_id)
{
  int ret = OB_SUCCESS;

  const ObDatabaseSchema *database_schema = NULL;
  if (OB_FAIL(get_database_schema(database_id, database_schema)) {
    LOG_WARN("failed to get database schema", K(ret), K(database_id));
  }
  return database_schema;
}
#endif

const ObTablegroupSchema* ObSchemaGetterGuard::get_tablegroup_schema(const uint64_t tablegroup_id)
{
  const ObTablegroupSchema* tablegroup_schema = NULL;
  get_tablegroup_schema(tablegroup_id, tablegroup_schema);
  return tablegroup_schema;
}
#if 0
const ObTableSchema *ObSchemaGetterGuard::get_table_schema(const uint64_t table_id)
{
  const ObTableSchema *table_schema = NULL;
  get_table_schema(table_id, table_schema);
  return table_schema;
}
#endif
const ObColumnSchemaV2* ObSchemaGetterGuard::get_column_schema(const uint64_t table_id, const uint64_t column_id)
{
  const ObColumnSchemaV2* column_schema = NULL;
  get_column_schema(table_id, column_id, column_schema);
  return column_schema;
}

const ObTenantSchema* ObSchemaGetterGuard::get_tenant_info(const ObString& tenant_name)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_info = NULL;
  if (OB_FAIL(get_tenant_info(tenant_name, tenant_info))) {
    LOG_WARN("get tenant info failed", K(ret), K(tenant_name));
  }
  return tenant_info;
}

// This function will return OB_ITER_END if all tables in one tenant has been iterated.
int ObSchemaGetterGuard::batch_get_next_table(
    const ObTenantTableId& tenant_table_id, const int64_t get_size, common::ObIArray<ObTenantTableId>& table_array)
{
  int ret = OB_SUCCESS;
  bool extra_fetch = true;
  const ObSchemaMgr* mgr = NULL;
  const uint64_t tenant_id = tenant_table_id.tenant_id_;
  const uint64_t table_id = tenant_table_id.table_id_;
  table_array.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!tenant_table_id.is_valid() || get_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_table_id), K(get_size));
  } else if (OB_INVALID_TENANT_ID == tenant_id || (OB_MIN_ID != table_id && extract_tenant_id(table_id) != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_table_id), K(get_size));
  } else if (is_schema_splited() && OB_SYS_TENANT_ID != tenant_id && is_inner_table(table_id)) {
    // Get normal tenant's system table in system tenant.
    if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->batch_get_next_table(tenant_table_id, get_size, table_array))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to iter table", K(ret), K(tenant_table_id));
      } else {
        // Normal tenant's system tables has been iterated,
        // and we should iterate normal tenant's user tables in the next step.
        ret = OB_SUCCESS;
      }
    } else {
      extra_fetch = false;
    }
  }
  if (OB_SUCC(ret) && extra_fetch) {
    // Fetch normal tenant's user table schemas immediately if normal tenant' system table schemas are iterated.
    if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->batch_get_next_table(tenant_table_id, get_size, table_array))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to iter table", K(ret), K(tenant_table_id));
      }
    }
  }
  return ret;
}

#define GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(SCHEMA, SCHEMA_TYPE, SIMPLE_SCHEMA_TYPE, SCHEMA_TYPE_ENUM) \
  int ObSchemaGetterGuard::get_##SCHEMA##_schemas_in_tenant(                                         \
      const uint64_t tenant_id, ObIArray<const SCHEMA_TYPE*>& schema_array)                          \
  {                                                                                                  \
    int ret = OB_SUCCESS;                                                                            \
    const ObSchemaMgr* mgr = NULL;                                                                   \
    schema_array.reset();                                                                            \
    ObArray<const SIMPLE_SCHEMA_TYPE*> simple_schemas;                                               \
    if (!check_inner_stat()) {                                                                       \
      ret = OB_INNER_STAT_ERROR;                                                                     \
      LOG_WARN("inner stat error", K(ret));                                                          \
    } else if (OB_INVALID_ID == tenant_id) {                                                         \
      ret = OB_INVALID_ARGUMENT;                                                                     \
      LOG_WARN("invalid argument", K(ret), K(tenant_id));                                            \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                      \
      LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));            \
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {                                          \
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));                                    \
    } else if (OB_FAIL(mgr->get_##SCHEMA##_schemas_in_tenant(tenant_id, simple_schemas))) {          \
      LOG_WARN("get " #SCHEMA " schemas in tenant failed", K(ret), K(tenant_id));                    \
    } else {                                                                                         \
      FOREACH_CNT_X(simple_schema, simple_schemas, OB_SUCC(ret))                                     \
      {                                                                                              \
        const SIMPLE_SCHEMA_TYPE* tmp_schema = *simple_schema;                                       \
        const SCHEMA_TYPE* schema = NULL;                                                            \
        if (OB_ISNULL(tmp_schema)) {                                                                 \
          ret = OB_ERR_UNEXPECTED;                                                                   \
          LOG_WARN("NULL ptr", K(ret));                                                              \
        } else if (OB_FAIL(get_schema_v2(SCHEMA_TYPE_ENUM,                                           \
                       tmp_schema->get_##SCHEMA##_id(),                                              \
                       schema,                                                                       \
                       tmp_schema->get_schema_version()))) {                                         \
          LOG_WARN("get " #SCHEMA " schema failed", K(ret));                                         \
        } else if (OB_ISNULL(schema)) {                                                              \
          ret = OB_ERR_UNEXPECTED;                                                                   \
          LOG_WARN("NULL ptr", K(ret), K(schema));                                                   \
        } else if (OB_FAIL(schema_array.push_back(schema))) {                                        \
          LOG_WARN("push back schema failed", K(ret));                                               \
        }                                                                                            \
      }                                                                                              \
    }                                                                                                \
    return ret;                                                                                      \
  }
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(user, ObUserInfo, ObSimpleUserSchema, USER_SCHEMA);
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(database, ObDatabaseSchema, ObSimpleDatabaseSchema, DATABASE_SCHEMA);
#undef GET_SCHEMAS_IN_TENANT_FUNC_DEFINE

#define GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(MGR, SCHEMA, SCHEMA_TYPE, SIMPLE_SCHEMA_TYPE, SCHEMA_TYPE_ENUM) \
  int ObSchemaGetterGuard::get_##SCHEMA##_schemas_in_tenant(                                                       \
      const uint64_t tenant_id, ObIArray<const SCHEMA_TYPE*>& schema_array)                                        \
  {                                                                                                                \
    int ret = OB_SUCCESS;                                                                                          \
    const ObSchemaMgr* mgr = NULL;                                                                                 \
    schema_array.reset();                                                                                          \
    ObArray<const SIMPLE_SCHEMA_TYPE*> simple_schemas;                                                             \
    if (!check_inner_stat()) {                                                                                     \
      ret = OB_INNER_STAT_ERROR;                                                                                   \
      LOG_WARN("inner stat error", K(ret));                                                                        \
    } else if (OB_INVALID_ID == tenant_id) {                                                                       \
      ret = OB_INVALID_ARGUMENT;                                                                                   \
      LOG_WARN("invalid argument", K(ret), K(tenant_id));                                                          \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                                    \
      LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));                          \
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {                                                        \
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));                                                  \
    } else if (OB_FAIL((&mgr->MGR)->get_##SCHEMA##_schemas_in_tenant(tenant_id, simple_schemas))) {                \
      LOG_WARN("get " #SCHEMA " schemas in tenant failed", K(ret), K(tenant_id));                                  \
    } else {                                                                                                       \
      FOREACH_CNT_X(simple_schema, simple_schemas, OB_SUCC(ret))                                                   \
      {                                                                                                            \
        const SIMPLE_SCHEMA_TYPE* tmp_schema = *simple_schema;                                                     \
        const SCHEMA_TYPE* schema = NULL;                                                                          \
        if (OB_ISNULL(tmp_schema)) {                                                                               \
          ret = OB_ERR_UNEXPECTED;                                                                                 \
          LOG_WARN("NULL ptr", K(ret));                                                                            \
        } else if (OB_FAIL(get_schema_v2(SCHEMA_TYPE_ENUM,                                                         \
                       tmp_schema->get_##SCHEMA##_id(),                                                            \
                       schema,                                                                                     \
                       tmp_schema->get_schema_version()))) {                                                       \
          LOG_WARN("get " #SCHEMA " schema failed", K(ret));                                                       \
        } else if (OB_ISNULL(schema)) {                                                                            \
          ret = OB_ERR_UNEXPECTED;                                                                                 \
          LOG_WARN("NULL ptr", K(ret), K(schema));                                                                 \
        } else if (OB_FAIL(schema_array.push_back(schema))) {                                                      \
          LOG_WARN("push back schema failed", K(ret));                                                             \
        }                                                                                                          \
      }                                                                                                            \
    }                                                                                                              \
    return ret;                                                                                                    \
  }

GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(outline_mgr_, outline, ObOutlineInfo, ObSimpleOutlineSchema, OUTLINE_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(synonym_mgr_, synonym, ObSynonymInfo, ObSimpleSynonymSchema, SYNONYM_SCHEMA);
#undef GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE

#define GET_USER_TABLE_SCHEMAS_IN_TENANT(SCHEMA)                                                              \
  int ObSchemaGetterGuard::get_user_table_schemas_in_tenant(                                                  \
      const uint64_t tenant_id, common::ObIArray<const SCHEMA*>& table_schemas)                               \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    table_schemas.reset();                                                                                    \
    ObArray<const SCHEMA*> tmp_table_schemas;                                                                 \
    if (!check_inner_stat()) {                                                                                \
      ret = OB_INNER_STAT_ERROR;                                                                              \
      LOG_WARN("inner stat error", K(ret));                                                                   \
    } else if (OB_INVALID_ID == tenant_id) {                                                                  \
      ret = OB_INVALID_ARGUMENT;                                                                              \
      LOG_WARN("invalid argument", K(ret), K(tenant_id));                                                     \
    } else if (OB_SYS_TENANT_ID == tenant_id || ObSchemaService::g_liboblog_mode_ || !is_standby_cluster()) { \
      if (OB_FAIL(get_user_table_schemas_in_tenant_inner(tenant_id, table_schemas))) {                        \
        LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));                                         \
      }                                                                                                       \
    } else if (OB_FAIL(get_user_table_schemas_in_tenant_inner(tenant_id, tmp_table_schemas))) {               \
      LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));                                           \
    } else {                                                                                                  \
      for (int64_t i = 0; i < tmp_table_schemas.count() && OB_SUCC(ret); i++) {                               \
        const SCHEMA* schema = NULL;                                                                          \
        const SCHEMA* tmp_schema = tmp_table_schemas.at(i);                                                   \
        if (OB_FAIL(schema_helper_.get_table_schema(*this, tmp_schema, schema))) {                            \
          LOG_WARN("fail to get table schema", KR(ret));                                                      \
        } else if (OB_FAIL(table_schemas.push_back(schema))) {                                                \
          LOG_WARN("fail to push back schema", KR(ret));                                                      \
        }                                                                                                     \
      }                                                                                                       \
    }                                                                                                         \
    return ret;                                                                                               \
  }

GET_USER_TABLE_SCHEMAS_IN_TENANT(ObSimpleTableSchemaV2);
GET_USER_TABLE_SCHEMAS_IN_TENANT(ObTableSchema);
#undef GET_USER_TABLE_SCHEMAS_IN_TENANT

#define GET_INNER_TABLE_SCHEMAS_IN_TENANT(SCHEMA)                                                             \
  int ObSchemaGetterGuard::get_inner_table_schemas_in_tenant(                                                 \
      const uint64_t tenant_id, common::ObIArray<const SCHEMA*>& table_schemas)                               \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    table_schemas.reset();                                                                                    \
    ObArray<const SCHEMA*> tmp_table_schemas;                                                                 \
    if (!check_inner_stat()) {                                                                                \
      ret = OB_INNER_STAT_ERROR;                                                                              \
      LOG_WARN("inner stat error", K(ret));                                                                   \
    } else if (OB_INVALID_ID == tenant_id) {                                                                  \
      ret = OB_INVALID_ARGUMENT;                                                                              \
      LOG_WARN("invalid argument", K(ret), K(tenant_id));                                                     \
    } else if (OB_SYS_TENANT_ID == tenant_id || ObSchemaService::g_liboblog_mode_ || !is_standby_cluster()) { \
      if (OB_FAIL(get_inner_table_schemas_in_tenant_inner(tenant_id, table_schemas))) {                       \
        LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));                                         \
      }                                                                                                       \
    } else if (OB_FAIL(get_inner_table_schemas_in_tenant_inner(tenant_id, tmp_table_schemas))) {              \
      LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));                                           \
    } else {                                                                                                  \
      for (int64_t i = 0; i < tmp_table_schemas.count() && OB_SUCC(ret); i++) {                               \
        const SCHEMA* schema = NULL;                                                                          \
        const SCHEMA* tmp_schema = tmp_table_schemas.at(i);                                                   \
        if (OB_FAIL(schema_helper_.get_table_schema(*this, tmp_schema, schema))) {                            \
          LOG_WARN("fail to get table schema", KR(ret));                                                      \
        } else if (OB_FAIL(table_schemas.push_back(schema))) {                                                \
          LOG_WARN("fail to push back schema", KR(ret));                                                      \
        }                                                                                                     \
      }                                                                                                       \
    }                                                                                                         \
    return ret;                                                                                               \
  }

GET_INNER_TABLE_SCHEMAS_IN_TENANT(ObSimpleTableSchemaV2);
GET_INNER_TABLE_SCHEMAS_IN_TENANT(ObTableSchema);
#undef GET_INNER_TABLE_SCHEMAS_IN_TENANT

#define GET_TABLE_SCHEMAS_IN_TENANT(SCHEMA)                                                                   \
  int ObSchemaGetterGuard::get_table_schemas_in_tenant(                                                       \
      const uint64_t tenant_id, common::ObIArray<const SCHEMA*>& table_schemas)                               \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    table_schemas.reset();                                                                                    \
    ObArray<const SCHEMA*> tmp_table_schemas;                                                                 \
    if (!check_inner_stat()) {                                                                                \
      ret = OB_INNER_STAT_ERROR;                                                                              \
      LOG_WARN("inner stat error", K(ret));                                                                   \
    } else if (OB_INVALID_ID == tenant_id) {                                                                  \
      ret = OB_INVALID_ARGUMENT;                                                                              \
      LOG_WARN("invalid argument", K(ret), K(tenant_id));                                                     \
    } else if (OB_SYS_TENANT_ID == tenant_id || ObSchemaService::g_liboblog_mode_ || !is_standby_cluster()) { \
      if (OB_FAIL(get_table_schemas_in_tenant_inner(tenant_id, table_schemas))) {                             \
        LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));                                         \
      }                                                                                                       \
    } else if (OB_FAIL(get_table_schemas_in_tenant_inner(tenant_id, tmp_table_schemas))) {                    \
      LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));                                           \
    } else {                                                                                                  \
      for (int64_t i = 0; i < tmp_table_schemas.count() && OB_SUCC(ret); i++) {                               \
        const SCHEMA* schema = NULL;                                                                          \
        const SCHEMA* tmp_schema = tmp_table_schemas.at(i);                                                   \
        if (OB_FAIL(schema_helper_.get_table_schema(*this, tmp_schema, schema))) {                            \
          LOG_WARN("fail to get table schema", KR(ret));                                                      \
        } else if (OB_FAIL(table_schemas.push_back(schema))) {                                                \
          LOG_WARN("fail to push back schema", KR(ret));                                                      \
        }                                                                                                     \
      }                                                                                                       \
    }                                                                                                         \
    return ret;                                                                                               \
  }

GET_TABLE_SCHEMAS_IN_TENANT(ObSimpleTableSchemaV2);
GET_TABLE_SCHEMAS_IN_TENANT(ObTableSchema);
#undef GET_TABLE_SCHEMAS_IN_TENANT

int ObSchemaGetterGuard::get_outline_infos_in_tenant(
    const uint64_t tenant_id, common::ObIArray<const ObOutlineInfo*>& table_schemas)
{
  return get_outline_schemas_in_tenant(tenant_id, table_schemas);
}

int ObSchemaGetterGuard::get_synonym_infos_in_tenant(
    const uint64_t tenant_id, common::ObIArray<const ObSynonymInfo*>& synonym_infos)
{
  return get_synonym_schemas_in_tenant(tenant_id, synonym_infos);
}

// For normal tenant, we get table ids in such tenant from both system tenant and normal tenant.
#define GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE(DST_SCHEMA)                                                            \
  int ObSchemaGetterGuard::get_table_ids_in_##DST_SCHEMA(                                                              \
      const uint64_t tenant_id, const uint64_t dst_schema_id, ObIArray<uint64_t>& table_ids)                           \
  {                                                                                                                    \
    int ret = OB_SUCCESS;                                                                                              \
    const ObSchemaMgr* mgr = NULL;                                                                                     \
    bool need_reset = true;                                                                                            \
    ObArray<const ObSimpleTableSchemaV2*> schemas;                                                                     \
    table_ids.reset();                                                                                                 \
    if (!check_inner_stat()) {                                                                                         \
      ret = OB_INNER_STAT_ERROR;                                                                                       \
      LOG_WARN("inner stat error", K(ret));                                                                            \
    } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == dst_schema_id ||                                         \
               extract_tenant_id(dst_schema_id) != tenant_id) {                                                        \
      ret = OB_INVALID_ARGUMENT;                                                                                       \
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dst_schema_id));                                            \
    }                                                                                                                  \
    if (OB_FAIL(ret)) {                                                                                                \
    } else if (OB_SYS_TENANT_ID == tenant_id || is_schema_splited()) {                                                 \
      const ObSchemaMgr* sys_mgr = NULL;                                                                               \
      if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {                                                      \
        LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));                                                    \
      } else if (OB_FAIL(sys_mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id, dst_schema_id, need_reset, schemas))) { \
        LOG_WARN(                                                                                                      \
            "get table schemas in " #DST_SCHEMA " failed", K(ret), K(tenant_id), #DST_SCHEMA "_id", dst_schema_id);    \
      }                                                                                                                \
    }                                                                                                                  \
    if (OB_FAIL(ret)) {                                                                                                \
    } else if (OB_SYS_TENANT_ID != tenant_id) {                                                                        \
      need_reset = false;                                                                                              \
      if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                                             \
        LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));                            \
      } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {                                                            \
        if (OB_TENANT_NOT_EXIST == ret) {                                                                              \
          if (is_standby_cluster()) {                                                                                  \
            ret = OB_SUCCESS;                                                                                          \
          } else {                                                                                                     \
            bool is_restore = false;                                                                                   \
            int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);                                              \
            if (OB_SUCCESS != tmp_ret) {                                                                               \
              LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));                           \
            } else if (is_restore) {                                                                                   \
              ret = OB_SUCCESS;                                                                                        \
            }                                                                                                          \
          }                                                                                                            \
        }                                                                                                              \
        if (OB_FAIL(ret)) {                                                                                            \
          LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));                                                    \
        }                                                                                                              \
      } else if (OB_ISNULL(mgr)) {                                                                                     \
        ret = OB_SCHEMA_EAGAIN;                                                                                        \
        LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));                                \
      } else if (OB_FAIL(mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id, dst_schema_id, need_reset, schemas))) {     \
        LOG_WARN(                                                                                                      \
            "get table schemas in " #DST_SCHEMA " failed", K(ret), K(tenant_id), #DST_SCHEMA "_id", dst_schema_id);    \
      }                                                                                                                \
    }                                                                                                                  \
    if (OB_SUCC(ret)) {                                                                                                \
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))                                                                     \
      {                                                                                                                \
        const ObSimpleTableSchemaV2* tmp_schema = *schema;                                                             \
        if (OB_ISNULL(tmp_schema)) {                                                                                   \
          ret = OB_ERR_UNEXPECTED;                                                                                     \
          LOG_WARN("NULL ptr", K(ret), K(tmp_schema));                                                                 \
        } else if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {                                         \
          LOG_WARN("push back table id failed", K(ret));                                                               \
        }                                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
    return ret;                                                                                                        \
  }

GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE(database);
GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup);
#undef GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE

// For normal tenant, we get table ids in such tenant from both system tenant and normal tenant.
int ObSchemaGetterGuard::get_table_ids_in_tenant(const uint64_t tenant_id, ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  bool need_reset = true;
  ObArray<const ObSimpleTableSchemaV2*> schemas;
  table_ids.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == tenant_id || is_schema_splited()) {
    const ObSchemaMgr* sys_mgr = NULL;
    if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(sys_mgr->get_table_schemas_in_tenant(tenant_id, need_reset, schemas))) {
      LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    need_reset = false;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
      if (OB_TENANT_NOT_EXIST == ret) {
        // ignore schema error when cluster is in standby cluster or tenant is in restore.
        if (is_standby_cluster()) {
          ret = OB_SUCCESS;
        } else {
          bool is_restore = false;
          int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));
          } else if (is_restore) {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
      }
    } else if (OB_ISNULL(mgr)) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, need_reset, schemas))) {
      LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleTableSchemaV2* tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
        LOG_WARN("push back table id failed", K(ret));
      }
    }
  }
  return ret;
}

#define GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(DST_SCHEMA, SCHEMA)                                   \
  int ObSchemaGetterGuard::get_table_schemas_in_##DST_SCHEMA(                                             \
      const uint64_t tenant_id, const uint64_t dst_schema_id, ObIArray<const SCHEMA*>& schema_array)      \
  {                                                                                                       \
    int ret = OB_SUCCESS;                                                                                 \
    schema_array.reset();                                                                                 \
    ObArray<const SCHEMA*> tmp_array;                                                                     \
    if (OB_SYS_TENANT_ID == tenant_id || ObSchemaService::g_liboblog_mode_ || !is_standby_cluster()) {    \
      if (OB_FAIL(get_table_schemas_in_##DST_SCHEMA##_inner(tenant_id, dst_schema_id, schema_array))) {   \
        LOG_WARN("fail to get table schema", KR(ret));                                                    \
      }                                                                                                   \
    } else if (OB_FAIL(get_table_schemas_in_##DST_SCHEMA##_inner(tenant_id, dst_schema_id, tmp_array))) { \
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(dst_schema_id));                      \
    } else {                                                                                              \
      for (int64_t i = 0; i < tmp_array.count() && OB_SUCC(ret); i++) {                                   \
        const SCHEMA* schema = NULL;                                                                      \
        const SCHEMA* tmp_schema = tmp_array.at(i);                                                       \
        if (OB_ISNULL(tmp_array.at(i))) {                                                                 \
          ret = OB_SCHEMA_ERROR;                                                                          \
          LOG_WARN("get invalid schema", KR(ret), K(i));                                                  \
        } else if (OB_FAIL(schema_helper_.get_table_schema(*this, tmp_schema, schema))) {                 \
          LOG_WARN("fail to get table schema", KR(ret));                                                  \
        } else if (OB_FAIL(schema_array.push_back(schema))) {                                             \
          LOG_WARN("fail to push back", KR(ret));                                                         \
        }                                                                                                 \
      }                                                                                                   \
    }                                                                                                     \
    return ret;                                                                                           \
  }
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(database, ObTableSchema);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(database, ObSimpleTableSchemaV2);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup, ObTableSchema);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup, ObSimpleTableSchemaV2);
#undef GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE

// For normal tenant, we get table schemas in such tenant from both system tenant and normal tenant.
#define GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE_INNER(DST_SCHEMA)                                                  \
  int ObSchemaGetterGuard::get_table_schemas_in_##DST_SCHEMA##_inner(                                                  \
      const uint64_t tenant_id, const uint64_t dst_schema_id, ObIArray<const ObTableSchema*>& schema_array)            \
  {                                                                                                                    \
    int ret = OB_SUCCESS;                                                                                              \
    const ObSchemaMgr* mgr = NULL;                                                                                     \
    ObArray<const ObSimpleTableSchemaV2*> schemas;                                                                     \
    schema_array.reset();                                                                                              \
    bool need_reset = true;                                                                                            \
    if (!check_inner_stat()) {                                                                                         \
      ret = OB_INNER_STAT_ERROR;                                                                                       \
      LOG_WARN("inner stat error", K(ret));                                                                            \
    } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == dst_schema_id ||                                         \
               extract_tenant_id(dst_schema_id) != tenant_id) {                                                        \
      ret = OB_INVALID_ARGUMENT;                                                                                       \
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dst_schema_id));                                            \
    }                                                                                                                  \
    if (OB_FAIL(ret)) {                                                                                                \
    } else if (OB_SYS_TENANT_ID == tenant_id || is_schema_splited()) {                                                 \
      const ObSchemaMgr* sys_mgr = NULL;                                                                               \
      if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {                                                      \
        LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));                                                    \
      } else if (OB_FAIL(sys_mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id, dst_schema_id, need_reset, schemas))) { \
        LOG_WARN(                                                                                                      \
            "get table schemas in " #DST_SCHEMA " failed", K(ret), K(tenant_id), #DST_SCHEMA "_id", dst_schema_id);    \
      }                                                                                                                \
    }                                                                                                                  \
    if (OB_FAIL(ret)) {                                                                                                \
    } else if (OB_SYS_TENANT_ID != tenant_id) {                                                                        \
      need_reset = false;                                                                                              \
      if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                                             \
        LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));                            \
      } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {                                                            \
        if (OB_TENANT_NOT_EXIST == ret) {                                                                              \
          if (is_standby_cluster()) {                                                                                  \
            ret = OB_SUCCESS;                                                                                          \
          } else {                                                                                                     \
            bool is_restore = false;                                                                                   \
            int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);                                              \
            if (OB_SUCCESS != tmp_ret) {                                                                               \
              LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));                           \
            } else if (is_restore) {                                                                                   \
              ret = OB_SUCCESS;                                                                                        \
            }                                                                                                          \
          }                                                                                                            \
        }                                                                                                              \
        if (OB_FAIL(ret)) {                                                                                            \
          LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));                                                    \
        }                                                                                                              \
      } else if (OB_ISNULL(mgr)) {                                                                                     \
        ret = OB_SCHEMA_EAGAIN;                                                                                        \
        LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));                                \
      } else if (OB_FAIL(mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id, dst_schema_id, need_reset, schemas))) {     \
        LOG_WARN(                                                                                                      \
            "get table schemas in " #DST_SCHEMA " failed", K(ret), K(tenant_id), #DST_SCHEMA "_id", dst_schema_id);    \
      }                                                                                                                \
    }                                                                                                                  \
    if (OB_SUCC(ret)) {                                                                                                \
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))                                                                     \
      {                                                                                                                \
        const ObSimpleTableSchemaV2* tmp_schema = *schema;                                                             \
        const ObTableSchema* table_schema = NULL;                                                                      \
        if (OB_ISNULL(tmp_schema)) {                                                                                   \
          ret = OB_ERR_UNEXPECTED;                                                                                     \
          LOG_WARN("NULL ptr", K(ret), K(tmp_schema));                                                                 \
        } else if (OB_FAIL(get_schema_v2(                                                                              \
                       TABLE_SCHEMA, tmp_schema->get_table_id(), table_schema, tmp_schema->get_schema_version()))) {   \
          LOG_WARN("get table schema failed", K(ret), K(*tmp_schema));                                                 \
        } else if (OB_ISNULL(table_schema)) {                                                                          \
          LOG_WARN("NULL ptr", K(ret), K(table_schema));                                                               \
        } else if (OB_FAIL(schema_array.push_back(table_schema))) {                                                    \
          LOG_WARN("push back table schema failed", K(ret));                                                           \
        }                                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
    return ret;                                                                                                        \
  }

GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE_INNER(database);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE_INNER(tablegroup);
#undef GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE_INNER

int ObSchemaGetterGuard::get_inner_table_schemas_in_tenant_inner(
    const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  bool need_reset = true;
  ObArray<const ObSimpleTableSchemaV2*> schemas;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  }

  const ObSchemaMgr* sys_mgr = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_mgr->get_table_schemas_in_tenant(tenant_id, need_reset, schemas))) {
    LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleTableSchemaV2* tmp_schema = *schema;
      const ObTableSchema* table_schema = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (!is_inner_table(tmp_schema->get_table_id())) {
        // nothing todo
      } else if (OB_FAIL(get_schema_v2(
                     TABLE_SCHEMA, tmp_schema->get_table_id(), table_schema, tmp_schema->get_schema_version()))) {
        LOG_WARN("get table schema failed", K(ret), K(*tmp_schema));
      } else if (OB_ISNULL(table_schema)) {
        LOG_WARN("NULL ptr", K(ret), K(table_schema));
      } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
        LOG_WARN("push back table schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_table_schemas_in_tenant_inner(
    const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  bool need_reset = true;
  ObArray<const ObSimpleTableSchemaV2*> schemas;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, need_reset, schemas))) {
    LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleTableSchemaV2* tmp_schema = *schema;
      const ObTableSchema* table_schema = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (is_inner_table(tmp_schema->get_table_id())) {
        // nothing todo
      } else if (OB_FAIL(get_schema_v2(
                     TABLE_SCHEMA, tmp_schema->get_table_id(), table_schema, tmp_schema->get_schema_version()))) {
        LOG_WARN("get table schema failed", K(ret), K(*tmp_schema));
      } else if (OB_ISNULL(table_schema)) {
        LOG_WARN("NULL ptr", K(ret), K(table_schema));
      } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
        LOG_WARN("push back table schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schemas_in_tenant_inner(
    const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  bool need_reset = true;
  ObArray<const ObSimpleTableSchemaV2*> schemas;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == tenant_id || is_schema_splited()) {
    const ObSchemaMgr* sys_mgr = NULL;
    if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(sys_mgr->get_table_schemas_in_tenant(tenant_id, need_reset, schemas))) {
      LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    need_reset = false;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
      if (OB_TENANT_NOT_EXIST == ret) {
        // ignore schema error when cluster is in standby cluster or tenant is in restore.
        if (is_standby_cluster()) {
          ret = OB_SUCCESS;
        } else {
          bool is_restore = false;
          int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));
          } else if (is_restore) {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
      }
    } else if (OB_ISNULL(mgr)) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, need_reset, schemas))) {
      LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleTableSchemaV2* tmp_schema = *schema;
      const ObTableSchema* table_schema = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (OB_FAIL(get_schema_v2(
                     TABLE_SCHEMA, tmp_schema->get_table_id(), table_schema, tmp_schema->get_schema_version()))) {
        LOG_WARN("get table schema failed", K(ret), K(*tmp_schema));
      } else if (OB_ISNULL(table_schema)) {
        LOG_WARN("NULL ptr", K(ret), K(table_schema));
      } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
        LOG_WARN("push back table schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_inner_table_schemas_in_tenant_inner(
    const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  bool need_reset = true;
  table_schemas.reset();
  ObArray<const ObSimpleTableSchemaV2*> tmp_schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  }

  const ObSchemaMgr* sys_mgr = NULL;
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(sys_mgr->get_table_schemas_in_tenant(tenant_id, need_reset, tmp_schemas))) {
    LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < tmp_schemas.count() && OB_SUCC(ret); i++) {
      if (!is_inner_table(tmp_schemas.at(i)->get_table_id())) {
        // nothing todo
      } else if (OB_FAIL(table_schemas.push_back(tmp_schemas.at(i)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_table_schemas_in_tenant_inner(
    const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  table_schemas.reset();
  bool need_reset = true;
  ObArray<const ObSimpleTableSchemaV2*> tmp_schemas;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, need_reset, tmp_schemas))) {
    LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < tmp_schemas.count() && OB_SUCC(ret); i++) {
      if (is_inner_table(tmp_schemas.at(i)->get_table_id())) {
        // nothing todo
      } else if (OB_FAIL(table_schemas.push_back(tmp_schemas.at(i)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schemas_in_tenant_inner(
    const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  bool need_reset = true;
  table_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == tenant_id || is_schema_splited()) {
    const ObSchemaMgr* sys_mgr = NULL;
    if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(sys_mgr->get_table_schemas_in_tenant(tenant_id, need_reset, table_schemas))) {
      LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    need_reset = false;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
      if (OB_TENANT_NOT_EXIST == ret) {
        // ignore schema error when cluster is in standby cluster or tenant is in restore.
        if (is_standby_cluster()) {
          ret = OB_SUCCESS;
        } else {
          bool is_restore = false;
          int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));
          } else if (is_restore) {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
      }
    } else if (OB_ISNULL(mgr)) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, need_reset, table_schemas))) {
      LOG_WARN("get table schemas in tenant failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

#define GET_TABLE_SCHEMAS_IN_DST_SCHEMA_INNER_FUNC_DEFINE(DST_SCHEMA)                                                \
  int ObSchemaGetterGuard::get_table_schemas_in_##DST_SCHEMA##_inner(const uint64_t tenant_id,                       \
      const uint64_t dst_schema_id,                                                                                  \
      common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas)                                                 \
  {                                                                                                                  \
    int ret = OB_SUCCESS;                                                                                            \
    const ObSchemaMgr* mgr = NULL;                                                                                   \
    bool need_reset = true;                                                                                          \
    table_schemas.reset();                                                                                           \
    if (!check_inner_stat()) {                                                                                       \
      ret = OB_INNER_STAT_ERROR;                                                                                     \
      LOG_WARN("inner stat error", K(ret));                                                                          \
    } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == dst_schema_id ||                                       \
               extract_tenant_id(dst_schema_id) != tenant_id) {                                                      \
      ret = OB_INVALID_ARGUMENT;                                                                                     \
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dst_schema_id));                                          \
    }                                                                                                                \
    if (OB_FAIL(ret)) {                                                                                              \
    } else if (OB_SYS_TENANT_ID == tenant_id || is_schema_splited()) {                                               \
      const ObSchemaMgr* sys_mgr = NULL;                                                                             \
      if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, sys_mgr))) {                                                    \
        LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));                                                  \
      } else if (OB_FAIL(sys_mgr->get_table_schemas_in_##DST_SCHEMA(                                                 \
                     tenant_id, dst_schema_id, need_reset, table_schemas))) {                                        \
        LOG_WARN("get table schemas in " #DST_SCHEMA " failed", K(ret), K(tenant_id), K(dst_schema_id));             \
      }                                                                                                              \
    }                                                                                                                \
    if (OB_FAIL(ret)) {                                                                                              \
    } else if (OB_SYS_TENANT_ID != tenant_id) {                                                                      \
      need_reset = false;                                                                                            \
      if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                                           \
        LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));                          \
      } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {                                                          \
        if (OB_TENANT_NOT_EXIST == ret) {                                                                            \
          if (is_standby_cluster()) {                                                                                \
            ret = OB_SUCCESS;                                                                                        \
          } else {                                                                                                   \
            bool is_restore = false;                                                                                 \
            int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);                                            \
            if (OB_SUCCESS != tmp_ret) {                                                                             \
              LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));                         \
            } else if (is_restore) {                                                                                 \
              ret = OB_SUCCESS;                                                                                      \
            }                                                                                                        \
          }                                                                                                          \
        }                                                                                                            \
        if (OB_FAIL(ret)) {                                                                                          \
          LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));                                                  \
        }                                                                                                            \
      } else if (OB_ISNULL(mgr)) {                                                                                   \
        ret = OB_SCHEMA_EAGAIN;                                                                                      \
        LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));                              \
      } else if (OB_FAIL(                                                                                            \
                     mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id, dst_schema_id, need_reset, table_schemas))) { \
        LOG_WARN("get table schemas in " #DST_SCHEMA " failed", K(ret), K(tenant_id), K(dst_schema_id));             \
      }                                                                                                              \
    }                                                                                                                \
    return ret;                                                                                                      \
  }
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_INNER_FUNC_DEFINE(database)
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_INNER_FUNC_DEFINE(tablegroup)
#undef GET_TABLE_SCHEMAS_IN_DST_SCHEMA_INNER_FUNC_DEFINE

int ObSchemaGetterGuard::get_tenant_ids(ObIArray<uint64_t>& tenant_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  tenant_ids.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret));
  } else {
    ret = mgr->get_tenant_ids(tenant_ids);
  }
  return ret;
}

// For liboblog only, this function only return tenants in normal status.
int ObSchemaGetterGuard::get_available_tenant_ids(ObIArray<uint64_t>& tenant_ids) const
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  const ObSchemaMgr* schema_mgr = NULL;
  if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", K(ret));
  } else if (OB_FAIL(schema_mgr->get_available_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get avaliable tenant_ids", K(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_name_case_mode(const uint64_t tenant_id, ObNameCaseMode& mode)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  mode = OB_NAME_CASE_INVALID;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->get_tenant_name_case_mode(tenant_id, mode);
  }

  return ret;
}

int ObSchemaGetterGuard::get_tenant_meta_reserved_memory_percentage(
    const uint64_t tenant_id, common::ObIAllocator& allocator, int64_t& percentage)
{
  int ret = OB_SUCCESS;
  const share::schema::ObSysVarSchema* sys_schema = NULL;
  ObObj obj;
  percentage = 0;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSchemaGetterGuard has not been inited", K(ret));
  } else if (OB_FAIL(get_tenant_system_variable(tenant_id, SYS_VAR_OB_RESERVED_META_MEMORY_PERCENTAGE, sys_schema))) {
    LOG_WARN("fail to get tenant system varaible", K(ret));
  } else if (OB_ISNULL(sys_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(sys_schema->get_value(&allocator, NULL /*time zone*/, obj))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(obj.get_int(percentage))) {
    LOG_WARN("fail to get int", K(ret));
  }
  return ret;
}

// get read only attribute from sys variabl meta info
// FIXME: For the following reasons, inner sql won't check if tenant is read only after schema split.
// 1. To avoid cyclic dependence in the second stage of create tenant.
// 2. Inner sql should not be controlled by tenant's read only attribute.
int ObSchemaGetterGuard::get_tenant_read_only(const uint64_t tenant_id, bool& read_only)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;

  read_only = false;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->get_tenant_read_only(tenant_id, read_only);
  }

  return ret;
}

int ObSchemaGetterGuard::check_database_exists_in_tablegroup(
    const uint64_t tenant_id, const uint64_t tablegroup_id, bool& not_empty)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  not_empty = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->check_database_exists_in_tablegroup(tenant_id, tablegroup_id, not_empty);
  }
  return ret;
}

int ObSchemaGetterGuard::check_tenant_exist(const uint64_t tenant_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_version_v2(TENANT_SCHEMA, tenant_id, schema_version))) {
    LOG_WARN("check tenant exist failed", K(ret), K(tenant_id));
  } else if (OB_INVALID_VERSION != schema_version) {
    is_exist = true;
  }

  return ret;
}

int ObSchemaGetterGuard::check_outline_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& name, uint64_t& outline_id, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  outline_id = OB_INVALID_ID;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleOutlineSchema* schema = NULL;
    if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_name(tenant_id, database_id, name, schema))) {
      LOG_WARN("get outline schema failed", K(ret), K(tenant_id), K(database_id), K(name));
    } else if (NULL != schema) {
      outline_id = schema->get_outline_id();
      exist = true;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_outline_exist_with_sql_id(
    const uint64_t tenant_id, const uint64_t database_id, const common::ObString& sql_id, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || sql_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(sql_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleOutlineSchema* schema = NULL;
    if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_sql_id(tenant_id, database_id, sql_id, schema))) {
      LOG_WARN("get outline schema failed", K(ret), K(tenant_id), K(database_id), K(sql_id));
    } else if (NULL != schema) {
      exist = true;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_outline_exist_with_sql(
    const uint64_t tenant_id, const uint64_t database_id, const common::ObString& paramlized_sql, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || paramlized_sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(paramlized_sql));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleOutlineSchema* schema = NULL;
    if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_signature(tenant_id, database_id, paramlized_sql, schema))) {
      LOG_WARN("get outline schema failed", K(ret), K(tenant_id), K(database_id), K(paramlized_sql));
    } else if (NULL != schema) {
      exist = true;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_simple_synonym_info(
    const uint64_t tenant_id, const uint64_t synonym_id, const ObSimpleSynonymSchema*& synonym_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  synonym_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == synonym_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(synonym_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schema(synonym_id, synonym_info))) {
    LOG_WARN("get outline schema failed", K(tenant_id), K(synonym_id), K(ret));
  } else if (NULL == synonym_info) {
    LOG_INFO("synonym not exist", K(tenant_id), K(synonym_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_synonym_info(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& synonym_name, const ObSynonymInfo*& synonym_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  synonym_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || synonym_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(synonym_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleSynonymSchema* simple_synonym = NULL;
    if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schema_with_name(tenant_id, database_id, synonym_name, simple_synonym))) {
      LOG_WARN("get outline schema failed", K(tenant_id), K(database_id), K(synonym_name), K(ret));
    } else if (NULL == simple_synonym) {
      LOG_INFO("synonym not exist", K(tenant_id), K(database_id), K(synonym_name));
    } else if (OB_FAIL(get_schema_v2(SYNONYM_SCHEMA,
                   simple_synonym->get_synonym_id(),
                   synonym_info,
                   simple_synonym->get_schema_version()))) {
      LOG_WARN("get outline schema failed", K(ret), "simple outline schema", *simple_synonym);
    } else if (OB_ISNULL(synonym_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), KPC(simple_synonym));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_synonym_info(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& synonym_name, const ObSimpleSynonymSchema*& synonym_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  synonym_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || synonym_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(synonym_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 mgr->synonym_mgr_.get_synonym_schema_with_name(tenant_id, database_id, synonym_name, synonym_info))) {
    LOG_WARN("get synonym schema failed", K(tenant_id), K(database_id), K(synonym_name), K(ret));
  } else if (NULL == synonym_info) {
    LOG_INFO("synonym not exist", K(tenant_id), K(database_id), K(synonym_name));
  }
  return ret;
}

int ObSchemaGetterGuard::check_synonym_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& synonym_name, bool& exist, uint64_t& synonym_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || synonym_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(synonym_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleSynonymSchema* schema = NULL;
    if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schema_with_name(tenant_id, database_id, synonym_name, schema))) {
      LOG_WARN("get outline schema failed", K(ret), K(tenant_id), K(database_id), K(synonym_name));
    } else if (NULL != schema) {
      exist = true;
      synonym_id = schema->get_synonym_id();
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_synonym_info_version(uint64_t synonym_id, int64_t& synonym_version)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const uint64_t tenant_id = extract_tenant_id(synonym_id);
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->synonym_mgr_.get_synonym_info_version(synonym_id, synonym_version);
  }
  return ret;
}

int ObSchemaGetterGuard::get_object_with_synonym(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& name, uint64_t& obj_database_id, uint64_t& synonym_id, ObString& obj_table_name,
    bool& do_exist) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  do_exist = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ret = mgr->synonym_mgr_.get_object(
        tenant_id, database_id, name, obj_database_id, synonym_id, obj_table_name, do_exist);
  }
  return ret;
}

int ObSchemaGetterGuard::get_sequence_schema(
    const uint64_t tenant_id, const uint64_t sequence_id, const ObSequenceSchema*& sequence_schema)
{
  int ret = OB_SUCCESS;
  sequence_schema = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == sequence_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(sequence_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_v2(SEQUENCE_SCHEMA, sequence_id, sequence_schema))) {
    LOG_WARN("get sequence schema failed", K(sequence_id), K(ret));
  } else if (OB_ISNULL(sequence_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("sequence schema not exists", K(ret), K(sequence_id),
             "pure_sequence_id", extract_pure_id(sequence_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sequence_schema_with_name(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& sequence_name, const ObSequenceSchema*& sequence_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || sequence_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(
            mgr->sequence_mgr_.get_sequence_schema_with_name(tenant_id, database_id, sequence_name, sequence_schema))) {
      LOG_WARN("get schema failed", K(ret), K(tenant_id), K(database_id), K(sequence_name));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_outline_info_with_name(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& name, const ObOutlineInfo*& outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema* simple_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_name(tenant_id, database_id, name, simple_outline))) {
    LOG_WARN("get simple outline failed", K(ret), K(tenant_id), K(database_id), K(name));
  } else if (NULL == simple_outline) {
    LOG_INFO("outline not exist", K(tenant_id), K(database_id), K(name));
  } else if (OB_FAIL(get_schema_v2(OUTLINE_SCHEMA,
                 simple_outline->get_outline_id(),
                 outline_info,
                 simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", K(ret), "simple outline schema", *simple_outline);
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(outline_info));
  }

  return ret;
}

int ObSchemaGetterGuard::get_outline_info_with_name(
    const uint64_t tenant_id, const ObString& db_name, const ObString& outline_name, const ObOutlineInfo*& outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema* simple_outline = NULL;
  uint64_t database_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || db_name.empty() || outline_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(db_name), K(outline_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_database_id(tenant_id, db_name, database_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    // do-nothing
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_name(
                 tenant_id, database_id, outline_name, simple_outline))) {
    LOG_WARN("get simple outline failed", K(ret), K(tenant_id), K(database_id), K(outline_name));
  } else if (NULL == simple_outline) {
    LOG_DEBUG("outline not exist", K(tenant_id), K(database_id), K(outline_name));
  } else if (OB_FAIL(get_schema_v2(OUTLINE_SCHEMA,
                 simple_outline->get_outline_id(),
                 outline_info,
                 simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", K(ret), "simple outline schema", *simple_outline);
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(outline_info));
  } else { /*do nothing*/
  }

  return ret;
}
int ObSchemaGetterGuard::get_outline_info_with_signature(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& signature, const ObOutlineInfo*& outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema* simple_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(signature), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_signature(
                 tenant_id, database_id, signature, simple_outline))) {
    LOG_WARN("get simple outline failed", K(ret), K(tenant_id), K(database_id), K(signature));
  } else if (NULL == simple_outline) {
    LOG_DEBUG("outline not exist", K(tenant_id), K(database_id), K(signature));
  } else if (OB_FAIL(get_schema_v2(OUTLINE_SCHEMA,
                 simple_outline->get_outline_id(),
                 outline_info,
                 simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", K(ret), "simple outline schema", *simple_outline);
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(outline_info));
  }

  return ret;
}

int ObSchemaGetterGuard::get_outline_info_with_sql_id(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& sql_id, const ObOutlineInfo*& outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema* simple_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || sql_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(sql_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 mgr->outline_mgr_.get_outline_schema_with_sql_id(tenant_id, database_id, sql_id, simple_outline))) {
    LOG_WARN("get simple outline failed", K(ret), K(tenant_id), K(database_id), K(sql_id));
  } else if (NULL == simple_outline) {
    LOG_DEBUG("outline not exist", K(tenant_id), K(database_id), K(sql_id));
  } else if (OB_FAIL(get_schema_v2(OUTLINE_SCHEMA,
                 simple_outline->get_outline_id(),
                 outline_info,
                 simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", K(ret), "simple outline schema", *simple_outline);
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(outline_info));
  }
  return ret;
}

int ObSchemaGetterGuard::check_user_exist(const uint64_t tenant_id, const ObString& user_name,
    const ObString& host_name, bool& is_exist, uint64_t* user_id /*=NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != user_id) {
    *user_id = OB_INVALID_ID;
  }

  uint64_t tmp_user_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_user_id(tenant_id, user_name, host_name, tmp_user_id))) {
    LOG_WARN("check user exist failed", K(ret), K(tenant_id), K(user_name), K(host_name));
  } else if (OB_INVALID_ID != tmp_user_id) {
    is_exist = true;
    if (NULL != user_id) {
      *user_id = tmp_user_id;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_user_exist(const uint64_t tenant_id, const uint64_t user_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(user_id));
  } else if (OB_FAIL(get_schema_version_v2(USER_SCHEMA, user_id, schema_version))) {
    LOG_WARN("check user exist failed", K(ret), K(tenant_id), K(user_id));
  } else if (OB_INVALID_VERSION != schema_version) {
    is_exist = true;
  }

  return ret;
}

int ObSchemaGetterGuard::check_database_exist(
    const uint64_t tenant_id, const common::ObString& database_name, bool& is_exist, uint64_t* database_id /*= NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != database_id) {
    *database_id = OB_INVALID_ID;
  }

  uint64_t tmp_database_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_name));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, tmp_database_id))) {
    LOG_WARN("get database id failed", K(ret), K(tenant_id), K(database_name));
  } else if (OB_INVALID_ID != tmp_database_id) {
    is_exist = true;
    if (NULL != database_id) {
      *database_id = tmp_database_id;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_database_in_recyclebin(const uint64_t database_id, bool& in_recyclebin)
{
  int ret = OB_SUCCESS;
  in_recyclebin = false;
  const ObDatabaseSchema* database_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(database_id), K(ret));
  } else if (OB_FAIL(get_schema_v2(DATABASE_SCHEMA, database_id, database_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(database_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema should not be null", K(ret));
  } else {
    in_recyclebin = database_schema->is_in_recyclebin();
  }
  return ret;
}

int ObSchemaGetterGuard::check_database_exist(const uint64_t database_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(database_id));
  } else if (OB_FAIL(get_schema_version_v2(DATABASE_SCHEMA, database_id, schema_version))) {
    LOG_WARN("get schema version failed", K(ret), K(database_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }

  return ret;
}

int ObSchemaGetterGuard::check_tablegroup_exist(const uint64_t tenant_id, const common::ObString& tablegroup_name,
    bool& is_exist, uint64_t* tablegroup_id /*= NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != tablegroup_id) {
    *tablegroup_id = OB_INVALID_ID;
  }

  uint64_t tmp_tablegroup_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(get_tablegroup_id(tenant_id, tablegroup_name, tmp_tablegroup_id))) {
    LOG_WARN("get tablegroup id failed", K(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_INVALID_ID != tmp_tablegroup_id) {
    is_exist = true;
    if (NULL != tablegroup_id) {
      *tablegroup_id = tmp_tablegroup_id;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_tablegroup_exist(const uint64_t tablegroup_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(get_schema_version_v2(TABLEGROUP_SCHEMA, tablegroup_id, schema_version))) {
    LOG_WARN("get schema version failed", K(ret), K(tablegroup_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }

  return ret;
}

/*
 * Within a namespace, no two objects can have the same name.
   In oracle mode, the following schema objects share one namespace:
   Tables(create, rename, flashback)
   Views(create, create or replace, rename, flashback)
   Sequences(create, rename)
   Private synonyms(create, create or replace, rename)
   Stand-alone procedures(create, create or replace)
   Stand-alone stored functions(create, create or replace)
   Packages(create, create or replace)
   Materialized views (OB oracle mode is not supported now)
   User-defined types(create, create or replace)
*/
// This function is used to check object name is duplicate in other different schemas in oracle mode.
// This function should be as a supplement to the original oracle detection logic of duplicate object name.
// @param [in] tenant_id
// @param [in] db_id
// @param [in] object_name
// @param [in] schema_type : schema type of object to be checked
// @param [in] routine_type : If schema_type is ROUTINE_SCHEMA, routine_type is used to
//                            distinguish whether object is procedure or function.
// @param [in] is_or_replace : distinguish whether create schema with create_or_replace option
//
// @param [out] conflict_schema_types  return other conficted objects' schema types
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObSchemaGetterGuard::check_oracle_object_exist(const uint64_t tenant_id, const uint64_t db_id,
    const ObString& object_name, const ObSchemaType& schema_type, const bool is_or_replace,
    common::ObIArray<ObSchemaType>& conflict_schema_types)
{
  int ret = OB_SUCCESS;
  share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;
  conflict_schema_types.reset();
  bool is_exist = false;

  if (OB_FAIL(get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret), K(tenant_id), K(compat_mode));
  } else if (share::ObWorker::CompatMode::ORACLE == compat_mode) {

    // table
    const ObSimpleTableSchemaV2* table_schema = NULL;
    if (FAILEDx(get_simple_table_schema(tenant_id, db_id, object_name, false, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (NULL != table_schema) {
      if (TABLE_SCHEMA == schema_type && table_schema->is_view_table() && is_or_replace) {
        // create or replace view
      } else if (OB_FAIL(conflict_schema_types.push_back(TABLE_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", K(ret));
      }
    }

    // sequence
    is_exist = false;
    uint64_t sequence_id = OB_INVALID_ID;
    if (FAILEDx(check_sequence_exist_with_name(tenant_id, db_id, object_name, is_exist, sequence_id))) {
      LOG_WARN("fail to check sequence exist", K(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (is_exist && OB_FAIL(conflict_schema_types.push_back(SEQUENCE_SCHEMA))) {
      LOG_WARN("fail to push back to conflict_schema_types", K(ret));
    }

    // synonym
    const ObSynonymInfo* synonym_info = NULL;
    if (FAILEDx(get_synonym_info(tenant_id, db_id, object_name, synonym_info))) {
      LOG_WARN("fail to get synonym info", K(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (NULL != synonym_info) {
      if (SYNONYM_SCHEMA == schema_type && is_or_replace) {
        // create or replace synonym
      } else if (OB_FAIL(conflict_schema_types.push_back(SYNONYM_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", K(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_table_exist(const uint64_t tenant_id, const uint64_t database_id,
    const common::ObString& table_name, const bool is_index,
    const CheckTableType check_type,  // check if temporary table is visable
    bool& is_exist, uint64_t* table_id /*=NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != table_id) {
    *table_id = OB_INVALID_ID;
  }

  uint64_t tmp_table_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(get_table_id(tenant_id, database_id, table_name, is_index, check_type, tmp_table_id))) {
    LOG_WARN("get database id failed", K(ret), K(tenant_id), K(database_id), K(table_name), K(is_index));
  } else if (OB_INVALID_ID != tmp_table_id) {
    is_exist = true;
    if (NULL != table_id) {
      *table_id = tmp_table_id;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_table_exist(const uint64_t table_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (is_fake_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (!check_fetch_table_id(table_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(table_id));
  } else if (OB_FAIL(get_schema_version_v2(TABLE_SCHEMA, table_id, schema_version))) {
    LOG_WARN("get schema version failed", K(ret), K(table_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }

  return ret;
}

int ObSchemaGetterGuard::get_table_schema_version(const uint64_t table_id, int64_t& table_version)
{
  return get_schema_version_v2(TABLE_SCHEMA, table_id, table_version);
}

// FIXME: This function is not used any more and is not supported to query system table's table status.
int ObSchemaGetterGuard::query_table_status(const uint64_t table_id, TableStatus& table_status)
{
  int ret = OB_SUCCESS;
  table_status = TABLE_EXIST;
  const bool is_pg = is_new_tablegroup_id(table_id);

  bool is_exist = false;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  ObRefreshSchemaStatus schema_status;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(check_table_exist(table_id, is_exist))) {
    LOG_WARN("check table exist failed", K(ret), K(table_id));
  } else if (is_exist) {
    table_status = TABLE_EXIST;
  } else if (OB_FAIL(get_schema_status(tenant_id, schema_status))) {
    LOG_WARN("fail to get schema status", K(ret), K(table_id));
  } else if (OB_FAIL(schema_service_->query_table_status(
                 schema_status, snapshot_version_, tenant_id, table_id, is_pg, table_status))) {
    LOG_WARN("query table status failed", K(ret), K(snapshot_version_), K(table_id), K(is_pg));
  } else {
  }

  return ret;
}

/*
 * For liboblog only
 *
 * 1. This function is used to get partition's status.
 * 2. Partition status has the following values:
 *    - PART_EXIST : partition schema exists.
 *    - PART_NOT_CREATE : partition schema doesn't exists(maybe local schema is not new enough),
 *                        but partition may be created in the future(under specified schema version).
 *    - PART_DELETED : partition schema doesn't exists, and partition has been dropped(from inner tables).
 */
int ObSchemaGetterGuard::query_partition_status(const common::ObPartitionKey& pkey, PartitionStatus& part_status)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  uint64_t tenant_id = pkey.get_tenant_id();
  uint64_t table_id = pkey.get_table_id();
  const bool is_pg = pkey.is_pg();
  const ObSimpleTableSchemaV2* table_schema = NULL;
  const ObTablegroupSchema* tablegroup_schema = NULL;
  part_status = PART_EXIST;
  int64_t schema_version = OB_INVALID_VERSION;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("get_schema_version fail", K(ret), K(tenant_id), K(schema_version));
  } else if (OB_UNLIKELY(OB_INVALID_VERSION == schema_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_version is invalid", K(ret), K(tenant_id), K(schema_version));
  } else {
    // check if table/tablegroup schema exist
    if (!is_pg) {
      if (OB_FAIL(get_table_schema(table_id, table_schema))) {
        LOG_WARN("get table schema fail", K(ret), K(table_id), KPC(table_schema));
      }
    } else {
      if (OB_FAIL(get_tablegroup_schema(table_id, tablegroup_schema))) {
        LOG_WARN("get table schema fail", K(ret), K(table_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // table/tablegroup schema doesn't exist
    if ((!is_pg && NULL == table_schema) || (is_pg && NULL == tablegroup_schema)) {
      TableStatus table_status = TABLE_DELETED;

      // check if table/tablegroup exists in the future(from inner table)
      if (OB_FAIL(get_schema_status(tenant_id, schema_status))) {
        LOG_WARN("fail to get schema status", K(ret), K(pkey));
      } else if (OB_FAIL(schema_service_->query_table_status(
                     schema_status, schema_version, extract_tenant_id(table_id), table_id, is_pg, table_status))) {
        LOG_WARN("query table status failed", K(ret), K(schema_status), K(schema_version), K(table_id), K(is_pg));
      } else {
        if (TABLE_DELETED == table_status) {
          // table/tablegroup has been dropped, partition status must be PART_DELETED.
          part_status = PART_DELETED;
        } else if (TABLE_EXIST == table_status) {
          // If table/tablegroup exists in inner tables, we should check if tenant has been dropped.
          TenantStatus tenant_status = TENANT_STATUS_INVALID;
          if (OB_FAIL(schema_service_->query_tenant_status(tenant_id, tenant_status))) {
            LOG_WARN("query tenant status failed", K(ret), K(tenant_id));
          } else if (TENANT_DELETED == tenant_status) {
            // If tenant has been dropped, partition status must be PART_DELETED.
            part_status = PART_DELETED;
            LOG_INFO("table schema is NULL, query table status table is EXIST, and tenant has been dropped "
                     "overwrite part status to PART_DELETED",
                K(table_schema),
                K(tenant_id),
                K(tenant_status),
                K(schema_version),
                K(tenant_status),
                K(table_id),
                K(is_pg),
                K(ret));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("table schema is NULL, tenant exist and table exist, error unexpected",
                K(tenant_id),
                K(tenant_status),
                K(schema_version),
                K(table_id),
                K(is_pg),
                K(ret));
          }
        } else {
          part_status = PART_NOT_CREATE;
        }

        LOG_INFO("query_partition_status",
            K(ret),
            K(pkey),
            K(is_pg),
            "part_status",
            print_part_status(part_status),
            "table_status",
            print_table_status(table_status),
            "schema_version",
            schema_version);
      }
    } else {
      // table/tablegroup schema exists, check if partition schema exists
      bool check_dropped_schema = false;
      if (!is_pg) {
        ObTablePartitionKeyIter pkey_iter(*table_schema, check_dropped_schema);
        bool is_sub_part_template = table_schema->is_sub_part_template();
        if (OB_FAIL(query_partition_status_from_table_schema_(
                pkey, pkey_iter, schema_version, is_sub_part_template, part_status))) {
          LOG_ERROR("query_partition_status_from_table_schema_ fail",
              K(ret),
              K(pkey),
              K(schema_version),
              K(is_sub_part_template));
        }
      } else {
        ObTablegroupPartitionKeyIter pkey_iter(*tablegroup_schema, check_dropped_schema);
        bool is_sub_part_template = tablegroup_schema->is_sub_part_template();
        if (OB_FAIL(query_partition_status_from_table_schema_(
                pkey, pkey_iter, schema_version, is_sub_part_template, part_status))) {
          LOG_ERROR("query_partition_status_from_table_schema_ fail",
              K(ret),
              K(pkey),
              K(schema_version),
              K(is_sub_part_template));
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("query_partition_status succ when table exists",
            K(pkey),
            K(is_pg),
            "part_status",
            print_part_status(part_status),
            "schema_version",
            schema_version);
      }
    }
  }

  if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
    // tenant has been dropped, partition status must be PART_DELETED.
    ret = OB_SUCCESS;
    part_status = PART_DELETED;

    LOG_INFO("table or tablegroup schema is NULL, tenant has been dropped, "
             "overwrite part status to PART_DELETED",
        K(tenant_id),
        K(pkey),
        K(is_pg),
        K(table_id),
        K(schema_version),
        K(table_schema),
        K(tablegroup_schema));
  }

  return ret;
}

template <class PartitionKeyIter>
int ObSchemaGetterGuard::query_partition_status_from_table_schema_(const common::ObPartitionKey& pkey,
    PartitionKeyIter& pkey_iter, const int64_t schema_version, const bool is_sub_part_template,
    PartitionStatus& part_status)
{
  int ret = OB_SUCCESS;
  ObPartitionKey iter_pkey;
  bool exist = false;

  ObRefreshSchemaStatus schema_status;
  uint64_t tenant_id = pkey.get_tenant_id();
  if (OB_FAIL(get_schema_status(tenant_id, schema_status))) {
    LOG_WARN("fail to get schema status", K(ret), K(pkey));
  }

  while (!exist && OB_SUCCESS == ret && OB_SUCC(pkey_iter.next_partition_key_v2(iter_pkey))) {
    exist = (pkey == iter_pkey);
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    if (exist) {
      /* Table exists and partition exists, it's no need to query partition status from inner tables.
       *
       * This branch also deal with following scenes:
       * 1. Non-partitioned table: partition is always exists when table exists.
       * 2. Hash-like partitioned table which never be executed partition related ddl on:
       *    partition must exists when table exists.
       */
      part_status = PART_EXIST;
    } else {
      // Table exists but partition doesn't exists, we should query partition status
      // from inner table with specified schema version.
      if (OB_FAIL(schema_service_->query_partition_status_from_sys_table(
              schema_status, schema_version, pkey, is_sub_part_template, part_status))) {
        LOG_ERROR("query_partition_status_from_sys_table fail",
            K(ret),
            K(schema_status),
            K(schema_version),
            K(pkey),
            K(is_sub_part_template),
            K(part_status));
      }
    }
  }

  return ret;
}

/*
  interface for simple schema
*/

int ObSchemaGetterGuard::get_table_schema(const uint64_t table_id, const ObSimpleTableSchemaV2*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2* orig_table = NULL;
  table_schema = NULL;
  if (OB_FAIL(get_table_schema_inner(table_id, orig_table))) {
    LOG_WARN("fail to get table schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(orig_table)) {
    // nothing todo
  } else if (OB_FAIL(schema_helper_.get_table_schema(*this, orig_table, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(table_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema_inner(const uint64_t table_id, const ObSimpleTableSchemaV2*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  bool is_system_table = is_inner_table(table_id);
  table_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (is_fake_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (!check_fetch_table_id(table_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch table schema with other tenant schema guard not allowed", K(ret), K(tenant_id_), K(table_id));
  } else {
    // system table's schema is always stored in system tenant.
    uint64_t fetch_tenant_id = is_system_table ? OB_SYS_TENANT_ID : tenant_id;
    if (OB_FAIL(get_schema_mgr(fetch_tenant_id, mgr))) {
      LOG_WARN("fail to get schema mgr", K(ret), K(fetch_tenant_id), K(tenant_id));
    } else if (OB_ISNULL(mgr)) {
      // lazy mode
      if (!ObSchemaService::g_liboblog_mode_) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("only for liboblog used", K(ret), K(table_id));
      } else if (OB_FAIL(get_schema_v2(TABLE_SIMPLE_SCHEMA, table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(table_id));
      }
    } else if (OB_FAIL(mgr->get_table_schema(table_id, table_schema))) {
      LOG_WARN("get simple table failed", K(ret), K(fetch_tenant_id), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      LOG_INFO("table not exist", K(fetch_tenant_id), K(tenant_id), K(table_id));
    }
  }
  return ret;
}

bool ObSchemaGetterGuard::is_tenant_schema_valid(const int64_t tenant_id) const
{
  bool bret = true;
  int tmp_ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_SUCCESS != (tmp_ret = get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get schema version", K(tmp_ret), K(tenant_id));
    bret = false;
  } else if (schema_version <= OB_CORE_SCHEMA_VERSION) {
    bret = false;
  }
  return bret;
}

int ObSchemaGetterGuard::get_tablegroup_schemas_in_tenant(
    const uint64_t tenant_id, common::ObIArray<const ObSimpleTablegroupSchema*>& tablegroup_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const int64_t DEFAULT_TABLEGROUP_NUM = 100;
  ObSEArray<const ObSimpleTablegroupSchema*, DEFAULT_TABLEGROUP_NUM> tmp_tablegroups;
  tablegroup_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      // ignore schema error when cluster is in standby cluster or tenant is in restore.
      if (is_standby_cluster()) {
        ret = OB_SUCCESS;
      } else {
        bool is_restore = false;
        int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));
        } else if (is_restore) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schemas_in_tenant(tenant_id, tmp_tablegroups))) {
    LOG_WARN("fail to get tablegroup schemas in tenant", K(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(simple_schema, tmp_tablegroups, OB_SUCC(ret))
    {
      const ObSimpleTablegroupSchema* schema = NULL;
      if (OB_ISNULL(*simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else if (OB_FAIL(schema_helper_.get_tablegroup_schema(*this, *simple_schema, schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), KPC(schema));
      } else if (OB_FAIL(tablegroup_schemas.push_back(schema))) {
        LOG_WARN("fail to push back tablegroup", K(ret), KPC(schema));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_schemas_in_tenant(
    const uint64_t tenant_id, common::ObIArray<const ObTablegroupSchema*>& tablegroup_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const int64_t DEFAULT_TABLEGROUP_NUM = 100;
  ObSEArray<const ObSimpleTablegroupSchema*, DEFAULT_TABLEGROUP_NUM> tmp_tablegroups;
  tablegroup_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      // ignore schema error when cluster is in standby cluster or tenant is in restore.
      if (is_standby_cluster()) {
        ret = OB_SUCCESS;
      } else {
        bool is_restore = false;
        int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));
        } else if (is_restore) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schemas_in_tenant(tenant_id, tmp_tablegroups))) {
    LOG_WARN("fail to get tablegroup schemas in tenant", K(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(simple_schema, tmp_tablegroups, OB_SUCC(ret))
    {
      const ObTablegroupSchema* orig_schema = NULL;
      const ObTablegroupSchema* schema = NULL;
      if (OB_ISNULL(*simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ptr", K(ret));
      } else if (OB_FAIL(get_schema_v2(TABLEGROUP_SCHEMA,
                     (*simple_schema)->get_tablegroup_id(),
                     orig_schema,
                     (*simple_schema)->get_schema_version()))) {
        LOG_WARN("fail to get schema", K(ret), KPC(*simple_schema));
      } else if (OB_FAIL(schema_helper_.get_tablegroup_schema(*this, orig_schema, schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), KPC(orig_schema));
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ptr", K(ret));
      } else if (OB_FAIL(tablegroup_schemas.push_back(schema))) {
        LOG_WARN("fail to push back tablegroup", K(ret), KPC(schema));
      }
    }
  }
  return ret;
}
int ObSchemaGetterGuard::get_tablegroup_ids_in_tenant(
    const uint64_t tenant_id, common::ObIArray<uint64_t>& tablegroup_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  tablegroup_ids.reset();

  ObArray<const ObSimpleTablegroupSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      // ignore schema error when cluster is in standby cluster or tenant is in restore.
      if (is_standby_cluster()) {
        ret = OB_SUCCESS;
      } else {
        bool is_restore = false;
        int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to check tenant is restore", K(ret), K(tmp_ret), K(tenant_id));
        } else if (is_restore) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get tablegroup schemas in tenant failed", K(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
    {
      const ObSimpleTablegroupSchema* tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else if (OB_FAIL(tablegroup_ids.push_back(tmp_schema->get_tablegroup_id()))) {
        LOG_WARN("push back tablegroup id failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schemas(common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas)
{
  int ret = OB_SUCCESS;
  table_schemas.reset();

  ObArray<uint64_t> tenant_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else {
    ObArray<const ObSimpleTableSchemaV2*> tmp_schemas;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret))
    {
      if (OB_FAIL(get_table_schemas_in_tenant(*tenant_id, tmp_schemas))) {
        LOG_WARN("get table schemas in tenant failed", K(ret), K(*tenant_id));
      } else {
        FOREACH_CNT_X(tmp_schema, tmp_schemas, OB_SUCC(ret))
        {
          if (OB_FAIL(table_schemas.push_back(*tmp_schema))) {
            LOG_WARN("add table schema failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_part(const uint64_t table_id, const ObPartitionLevel part_level, const int64_t part_id,
    const common::ObNewRange& range, const bool reverse, ObIArray<int64_t>& part_idxs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (part_level != PARTITION_LEVEL_ONE && part_level != PARTITION_LEVEL_TWO) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(part_level));
  } else if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("Failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("Table not exist", K(ret));
  } else if (PARTITION_LEVEL_ONE == part_level) {
    if (OB_FAIL(table_schema->get_part(range, reverse, part_idxs))) {
      LOG_WARN("Failed to get part idxs", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == part_level) {
    if (OB_FAIL(table_schema->get_subpart(part_id, range, reverse, part_idxs))) {
      LOG_WARN("Failed to get subpart", K(ret));
    }
  } else {
  }
  return ret;
}

int ObSchemaGetterGuard::get_part(const uint64_t table_id, const ObPartitionLevel part_level, const int64_t part_id,
    const ObNewRow& row, ObIArray<int64_t>& part_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (part_level != PARTITION_LEVEL_ONE && part_level != PARTITION_LEVEL_TWO) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(part_level));
  } else if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("Failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("Table not exist", K(ret));
  } else if (PARTITION_LEVEL_ONE == part_level) {
    if (OB_FAIL(table_schema->get_part(row, part_ids))) {
      LOG_WARN("Failed to get part idxs", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == part_level) {
    if (OB_FAIL(table_schema->get_subpart(part_id, row, part_ids))) {
      LOG_WARN("Failed to get subpart", K(ret));
    }
  } else {
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_count(int64_t& schema_count)
{
  int ret = OB_SUCCESS;
  schema_count = 0;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (is_tenant_schema_guard()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch all table schema with tenant schema guard not allowed", K(ret), K(tenant_id_));
  } else if (!is_schema_splited()) {
    if (OB_FAIL(mgr_->get_schema_count(schema_count))) {
      LOG_WARN("get_schema_count failed", K(ret));
    }
  } else {
    const ObSchemaMgr* mgr = NULL;
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
      LOG_WARN("fail to check lazy guard", K(ret));
    } else if (OB_FAIL(mgr->get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant ids", K(ret));
    } else if (OB_FAIL(mgr->get_schema_count(schema_count))) {
      LOG_WARN("get_schema_count failed", K(ret));
    } else {
      FOREACH_X(tenant_id, tenant_ids, OB_SUCC(ret))
      {
        int64_t inc_schema_count = 0;
        mgr = NULL;
        if (OB_ISNULL(tenant_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant id is null", K(ret));
        } else if (OB_SYS_TENANT_ID == *tenant_id) {
          continue;
        } else if (OB_FAIL(check_lazy_guard(*tenant_id, mgr))) {
          LOG_WARN("fail to check lazy guard", K(ret), K(*tenant_id));
        } else if (OB_FAIL(mgr->get_schema_count(inc_schema_count))) {
          LOG_WARN("get_schema_count failed", K(ret));
        } else {
          schema_count += inc_schema_count;
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_count(const uint64_t tenant_id, int64_t& schema_count)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  schema_count = 0;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_schema_count(schema_count))) {
    LOG_WARN("get_schema_count failed", K(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_size(const uint64_t tenant_id, int64_t& schema_size)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  schema_size = 0;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_schema_size(schema_size))) {
    LOG_WARN("get_schema_size failed", K(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::check_partition_exist(
    const uint64_t schema_id, const int64_t phy_partition_id, const bool check_dropped_partition, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (!is_tablegroup_id(schema_id)) {
    bool is_table_exist = false;
    const ObSimpleTableSchemaV2* table_schema = NULL;
    if (OB_FAIL(check_table_exist(schema_id, is_table_exist))) {
      LOG_WARN("fail to check table exist", K(ret), K(schema_id));
    } else if (!is_table_exist) {
      is_exist = false;
    } else if (OB_FAIL(get_table_schema(schema_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(schema_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema_id));
    } else if (!check_dropped_partition && table_schema->is_dropped_schema()) {
      is_exist = false;
    } else if (ObPartMgrUtils::check_part_exist(*table_schema, phy_partition_id, check_dropped_partition, is_exist)) {
      LOG_WARN("check part exist failed", K(ret), K(schema_id), K(phy_partition_id));
    }
  } else {
    bool is_tg_exist = false;
    const ObTablegroupSchema* tg_schema = nullptr;
    if (OB_FAIL(check_tablegroup_exist(schema_id, is_tg_exist))) {
      LOG_WARN("fail to check tablegroup exist", K(ret), K(schema_id));
    } else if (!is_tg_exist) {
      is_exist = false;
    } else if (OB_FAIL(get_tablegroup_schema(schema_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(schema_id));
    } else if (OB_UNLIKELY(nullptr == tg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nullptr is null", K(ret), K(schema_id));
    } else if (!check_dropped_partition && tg_schema->is_dropped_schema()) {
      is_exist = false;
    } else if (OB_FAIL(
                   ObPartMgrUtils::check_part_exist(*tg_schema, phy_partition_id, check_dropped_partition, is_exist))) {
      LOG_WARN("check part exist failed", K(ret), K(schema_id), K(phy_partition_id));
    }
  }
  return ret;
}

// for gc only
// 1. check_delay = true: delay-deleted table is visable and can't be gc.
// 2. check_delay = false: delay-deleted table is not visable and can be gc.
int ObSchemaGetterGuard::check_partition_can_remove(
    const uint64_t schema_id, const int64_t phy_partition_id, const bool check_delay, bool& can)
{
  int ret = OB_SUCCESS;
  bool schema_exist = true;
  can = false;
  if (!is_new_tablegroup_id(schema_id)) {
    const ObSimpleTableSchemaV2* table_schema = NULL;
    if (OB_FAIL(check_table_exist(schema_id, schema_exist))) {
      LOG_WARN("fail to check table exist", K(ret), K(schema_id));
    } else if (!schema_exist) {
      can = true;
    } else if (OB_FAIL(get_table_schema(schema_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(schema_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema_id));
    } else if (!check_delay && table_schema->is_dropped_schema()) {
      can = true;
    } else if (OB_FAIL(ObPartMgrUtils::check_partition_can_remove(*table_schema, phy_partition_id, check_delay, can))) {
      LOG_WARN("check part exist failed", K(ret), KPC(table_schema), K(phy_partition_id));
    }
  } else {
    const ObTablegroupSchema* tg_schema = NULL;
    if (OB_FAIL(check_tablegroup_exist(schema_id, schema_exist))) {
      LOG_WARN("fail to check tablegroup exist", K(ret), K(schema_id));
    } else if (!schema_exist) {
      can = true;
    } else if (OB_FAIL(get_tablegroup_schema(schema_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(schema_id));
    } else if (OB_ISNULL(tg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema_id));
    } else if (!check_delay && tg_schema->is_dropped_schema()) {
      can = true;
    } else if (OB_FAIL(ObPartMgrUtils::check_partition_can_remove(*tg_schema, phy_partition_id, check_delay, can))) {
      LOG_WARN("check part exist failed", K(ret), KPC(tg_schema), K(phy_partition_id));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_mv_ids(const uint64_t tenant_id, ObArray<uint64_t>& mv_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  mv_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tenant_mv_ids(tenant_id, mv_ids))) {
    LOG_WARN("Failed to get all_mv_ids", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::check_udf_exist_with_name(const uint64_t tenant_id, const common::ObString& name, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleUDFSchema* schema = NULL;
    if (OB_FAIL(mgr->udf_mgr_.get_udf_schema_with_name(tenant_id, name, schema))) {
      LOG_WARN("get udf schema failed", K(ret), K(tenant_id), K(name));
    } else if (OB_NOT_NULL(schema)) {
      exist = true;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_udf_info(
    const uint64_t tenant_id, const common::ObString& name, const share::schema::ObUDF*& udf_info, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  udf_info = nullptr;
  exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSimpleUDFSchema* udf_schema = NULL;
    if (OB_FAIL(mgr->udf_mgr_.get_udf_schema_with_name(tenant_id, name, udf_schema))) {
      LOG_WARN("get outline schema failed", K(ret), K(tenant_id), K(name));
    } else if (OB_ISNULL(udf_schema)) {
      LOG_INFO("udf not exist", K(tenant_id), K(name));
    } else if (OB_FAIL(
                   get_schema_v2(UDF_SCHEMA, udf_schema->get_udf_id(), udf_info, udf_schema->get_schema_version()))) {
      LOG_WARN("get udf schema failed", K(ret), "simple udf schema", *udf_schema);
    } else if (OB_ISNULL(udf_info)) {
      LOG_INFO("udf does not exist", K(tenant_id), K(name), K(ret));
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_partition_cnt(uint64_t table_id, int64_t& part_cnt)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const ObSimpleTableSchemaV2* simple_table = NULL;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_id), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schema(table_id, simple_table))) {
    LOG_WARN("get simple table failed", K(ret), K(table_id));
  } else if (NULL == simple_table) {
    LOG_INFO("table not exist", K(table_id));
  } else if (OB_UNLIKELY(simple_table->is_index_local_storage())) {
    const ObSimpleTableSchemaV2* data_simple_table = NULL;
    if (OB_FAIL(mgr->get_table_schema(simple_table->get_data_table_id(), data_simple_table))) {
      LOG_WARN("get table schema from mgr failed", K(ret), KPC(simple_table), KPC(data_simple_table));
    } else {
      part_cnt = data_simple_table->get_partition_cnt();
    }
  } else {
    part_cnt = simple_table->get_partition_cnt();
  }
  return ret;
}

// This function return indexes which are in unavaliable status and are not delay-deleted.
// It's used in the following scenes:
// 1. Schedule unavaliable indexes build tasks in primary cluster.
// 2. Drop unavaliable indexes when cluster switchover.
// 3. Rebuild unavaliable indexes in physical restore.
int ObSchemaGetterGuard::get_tenant_unavailable_index(const uint64_t tenant_id, common::ObIArray<uint64_t>& index_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  index_ids.reset();
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2*> table_schemas;
    if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", K(ret));
        } else if (INDEX_STATUS_UNAVAILABLE == table_schema->get_index_status() && table_schema->is_index_table() &&
                   !table_schema->is_dropped_schema()) {
          if (OB_FAIL(index_ids.push_back(table_schema->get_table_id()))) {
            LOG_WARN("fail to push back index id", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_unavailable_index_exist(const uint64_t tenant_id, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2*> table_schemas;
    if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", K(ret));
        } else if (INDEX_STATUS_UNAVAILABLE == table_schema->get_index_status() && table_schema->is_index_table()) {
          exist = true;
          LOG_INFO("unavaliale index exist", K(ret), "table_id", table_schema->get_table_id());
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_restore_error_index_exist(const uint64_t tenant_id, bool& exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2*> table_schemas;
    if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", K(ret));
        } else if (INDEX_STATUS_RESTORE_INDEX_ERROR == table_schema->get_index_status() &&
                   table_schema->is_index_table()) {
          exist = true;
          LOG_INFO("restore error index exist", K(ret), "table_id", table_schema->get_table_id());
        }
      }
    }
  }
  return ret;
}

// Can't get other normal tenant's schema with tenant schema guard.
int ObSchemaGetterGuard::check_tenant_schema_guard(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  if (is_tenant_schema_guard() && OB_SYS_TENANT_ID != tenant_id && tenant_id_ != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get other tenant schema with tenant schema guard not allowed", K(ret), K(tenant_id), K(tenant_id_));
  }
  return ret;
}

int ObSchemaGetterGuard::check_sequence_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& sequence_name, bool& exist, uint64_t& sequence_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || sequence_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else {
    const ObSequenceSchema* schema = NULL;
    if (OB_FAIL(mgr->sequence_mgr_.get_sequence_schema_with_name(tenant_id, database_id, sequence_name, schema))) {
      LOG_WARN("get schema failed", K(ret), K(tenant_id), K(database_id), K(sequence_name));
    } else if (NULL != schema) {
      exist = true;
      sequence_id = schema->get_sequence_id();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_dblink_exist(const uint64_t tenant_id, const ObString& dblink_name, bool& exist) const
{
  int ret = OB_SUCCESS;
  uint64_t dblink_id = OB_INVALID_ID;
  exist = false;
  if (OB_FAIL(get_dblink_id(tenant_id, dblink_name, dblink_id))) {
    LOG_WARN("failed to get dblink id", K(ret), K(tenant_id), K(dblink_name));
  } else {
    exist = (OB_INVALID_ID != dblink_id);
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_id(const uint64_t tenant_id, const ObString& dblink_name, uint64_t& dblink_id) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const ObDbLinkSchema* dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dblink_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("get dblink schema failed", K(ret), K(tenant_id), K(dblink_name));
  } else if (OB_NOT_NULL(dblink_schema)) {
    dblink_id = dblink_schema->get_dblink_id();
  } else {
    dblink_id = OB_INVALID_ID;
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_user(
    const uint64_t tenant_id, const ObString& dblink_name, ObString& dblink_user, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  const ObDbLinkSchema* dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dblink_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("get dblink schema failed", K(ret), K(tenant_id), K(dblink_name));
  } else if (OB_NOT_NULL(dblink_schema)) {
    OZ(ob_write_string(allocator, dblink_schema->get_user_name(), dblink_user, true));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink user name is empty", K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_schema(
    const uint64_t tenant_id, const ObString& dblink_name, const ObDbLinkSchema*& dblink_schema) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dblink_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("get dblink schema failed", K(ret), K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_schema(const uint64_t dblink_id, const ObDbLinkSchema*& dblink_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(dblink_id);
  const ObSchemaMgr* mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == dblink_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dblink_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(dblink_id, dblink_schema))) {
    LOG_WARN("get dblink schema failed", K(ret), K(dblink_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_link_table_schema(uint64_t dblink_id, const ObString& database_name,
    const ObString& table_name, ObIAllocator& allocator, ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObDbLinkSchema* dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("schema service is NULL", K(ret));
  } else if (OB_FAIL(get_dblink_schema(dblink_id, dblink_schema))) {
    LOG_WARN("get dblink schema failed", K(ret));
  } else if (OB_ISNULL(dblink_schema)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("dblink schema is NULL", K(ret));
  } else if (OB_FAIL(schema_service_->fetch_link_table_schema(
                 *dblink_schema, database_name, table_name, allocator, table_schema))) {
    LOG_WARN("get link table schema failed", K(ret));
  }
  return ret;
}

// only use in oracle mode
int ObSchemaGetterGuard::get_idx_schema_by_origin_idx_name(
    uint64_t tenant_id, uint64_t database_id, const common::ObString& index_name, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  table_schema = NULL;

  const ObSimpleTableSchemaV2* simple_table = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(index_name), K(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_idx_schema_by_origin_idx_name(tenant_id, database_id, index_name, simple_table))) {
    LOG_WARN("get simple table failed", K(ret), K(tenant_id), K(database_id), K(index_name));
  } else if (NULL == simple_table) {
    LOG_INFO("table not exist", K(tenant_id), K(database_id), K(index_name));
  } else if (OB_FAIL(get_schema_v2(
                 TABLE_SCHEMA, simple_table->get_table_id(), table_schema, simple_table->get_schema_version()))) {
    LOG_WARN("get table schema failed", K(ret), "simple table schema", *simple_table);
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(table_schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_compat_mode(const uint64_t tenant_id, ObWorker::CompatMode& compat_mode)
{
  return ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode);
}

int ObSchemaGetterGuard::get_mock_schema_info(
    const uint64_t schema_id, const ObSchemaType schema_type, ObMockSchemaInfo& mock_schema_info)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_mock_schema_info(schema_id, schema_type, mock_schema_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get mock schema info", K(ret), K(schema_id), K(schema_type));
    }
  }
  return ret;
}

///////////////////////
int ObSchemaGetterGuardHelper::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(full_schemas_.create(
                 DEFAULT_SCHEMA_NUM, ObModIds::OB_MOCK_SCHEMA_MAP, ObModIds::OB_MOCK_SCHEMA_MAP))) {
    LOG_WARN("fail to init full schemas map", K(ret));
  } else if (OB_FAIL(simple_schemas_.create(
                 DEFAULT_SCHEMA_NUM, ObModIds::OB_MOCK_SCHEMA_MAP, ObModIds::OB_MOCK_SCHEMA_MAP))) {
    LOG_WARN("fail to init simple schemas map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSchemaGetterGuardHelper::reset()
{
  full_schemas_.destroy();
  simple_schemas_.destroy();
  is_inited_ = false;
}

#define GET_TABLEGEOUP_SCHEMA_FROM_SCHEMA_HELPER(SCHEMA, SCHEMA_MAP)                                                \
  int ObSchemaGetterGuardHelper::get_tablegroup_schema(                                                             \
      ObSchemaGetterGuard& guard, const SCHEMA* orig_tablegroup, const SCHEMA*& new_tablegroup)                     \
  {                                                                                                                 \
    int ret = OB_SUCCESS;                                                                                           \
    const ObSchema* tablegroup = NULL;                                                                              \
    new_tablegroup = orig_tablegroup;                                                                               \
    if (OB_ISNULL(orig_tablegroup)) {                                                                               \
      /* skip */                                                                                                    \
    } else if (!guard.is_standby_cluster() || OB_SYS_TENANT_ID == orig_tablegroup->get_tenant_id() ||               \
               !is_new_tablegroup_id(orig_tablegroup->get_tablegroup_id()) || orig_tablegroup->is_in_splitting()) { \
      /* Try to mock partition status when all the following condititions are met */                                \
      /* 1.In standby cluster 2.In normal tenant 3. It's tablegroup created after ver 2.0. 4. Tablegroup is not     \
       * splitting*/                                                                                                \
    } else if (!is_inited_) {                                                                                       \
      ret = OB_NOT_INIT;                                                                                            \
      LOG_WARN("not init yet", K(ret));                                                                             \
    } else if (OB_FAIL(SCHEMA_MAP.get_refactored(orig_tablegroup->get_tablegroup_id(), tablegroup))) {              \
      SCHEMA* tmp_tablegroup = NULL;                                                                                \
      uint64_t tablegroup_id = orig_tablegroup->get_tablegroup_id();                                                \
      if (OB_HASH_NOT_EXIST) { /* overwrite ret */                                                                  \
        ObMockSchemaInfo mock_schema_info;                                                                          \
        if (OB_FAIL(guard.get_mock_schema_info(tablegroup_id, TABLEGROUP_SCHEMA, mock_schema_info))) {              \
          if (OB_ENTRY_NOT_EXIST == ret) { /* overwrite ret */                                                      \
            /* case 1: record doesn't exist. no need to mock. */                                                    \
            new_tablegroup = orig_tablegroup;                                                                       \
            ret = OB_SUCCESS;                                                                                       \
          } else {                                                                                                  \
            LOG_WARN("fail to get mock schema info", K(ret), K(tablegroup_id));                                     \
          }                                                                                                         \
        } else {                                                                                                    \
          /* case 2: record exists. need to mock. */                                                                \
          if (OB_FAIL(ObSchemaUtils::alloc_schema(alloc_, *orig_tablegroup, tmp_tablegroup))) {                     \
            LOG_WARN("fail to alloc new table", K(ret), K(tablegroup_id));                                          \
          } else {                                                                                                  \
            bool is_exist = false;                                                                                  \
            /* table group schema need mock partition status only.*/                                                \
            if (OB_FAIL(mock_schema_info.has_mock_schema_type(ObMockSchemaInfo::MOCK_SCHEMA_SPLIT, is_exist))) {    \
              LOG_WARN("fail to get mock schema type", K(ret), K(tablegroup_id));                                   \
            } else if (!is_exist) {                                                                                 \
              ret = OB_ERR_UNEXPECTED;                                                                              \
              LOG_WARN("tablegroup should mock split status", K(ret), K(tablegroup_id));                            \
            } else {                                                                                                \
              tmp_tablegroup->set_partition_status(PARTITION_STATUS_PHYSICAL_SPLITTING);                            \
            }                                                                                                       \
            if (OB_SUCC(ret)) {                                                                                     \
              new_tablegroup = tmp_tablegroup;                                                                      \
            }                                                                                                       \
          }                                                                                                         \
        }                                                                                                           \
        bool overwrite = true;                                                                                      \
        if (OB_FAIL(ret)) {                                                                                         \
        } else if (OB_FAIL(SCHEMA_MAP.set_refactored(tablegroup_id, new_tablegroup, overwrite))) {                  \
          LOG_WARN("fail to set new tablegroup", K(ret), K(tablegroup_id));                                         \
        }                                                                                                           \
      } else {                                                                                                      \
        LOG_WARN("fail to find tablegroup schema", K(ret), K(tablegroup_id));                                       \
      }                                                                                                             \
    } else if (OB_ISNULL(tablegroup)) {                                                                             \
      ret = OB_ERR_UNEXPECTED;                                                                                      \
      LOG_WARN("tmp_tablegroup is null", K(ret));                                                                   \
    } else {                                                                                                        \
      /* case 3: use cached schema*/                                                                                \
      new_tablegroup = static_cast<const SCHEMA*>(tablegroup);                                                      \
    }                                                                                                               \
    if (OB_SUCC(ret)) {                                                                                             \
      if (OB_NOT_NULL(orig_tablegroup) && OB_NOT_NULL(new_tablegroup)) {                                            \
        LOG_DEBUG("[MOCK_SCHEMA] get tablegroup schema from helper",                                                \
            "tablegroup_id",                                                                                        \
            orig_tablegroup->get_tablegroup_id(),                                                                   \
            "tablegroup_schema_version",                                                                            \
            orig_tablegroup->get_schema_version(),                                                                  \
            "orig_partition_status",                                                                                \
            orig_tablegroup->get_partition_status(),                                                                \
            "new_partition_status",                                                                                 \
            new_tablegroup->get_partition_status());                                                                \
      }                                                                                                             \
    }                                                                                                               \
    return ret;                                                                                                     \
  }

GET_TABLEGEOUP_SCHEMA_FROM_SCHEMA_HELPER(ObTablegroupSchema, full_schemas_);
GET_TABLEGEOUP_SCHEMA_FROM_SCHEMA_HELPER(ObSimpleTablegroupSchema, simple_schemas_);

#define GET_TABLE_SCHEMA_FROM_SCHEMA_HELPER(SCHEMA, TABLEGROUP_SCHEMA, SCHEMA_MAP)                                    \
  int ObSchemaGetterGuardHelper::get_table_schema(                                                                    \
      ObSchemaGetterGuard& guard, const SCHEMA* orig_table, const SCHEMA*& new_table)                                 \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    const ObSchema* table = NULL;                                                                                     \
    new_table = orig_table;                                                                                           \
    if (OB_ISNULL(orig_table)) {                                                                                      \
      /* skip*/                                                                                                       \
    } else if (!guard.is_standby_cluster() || OB_SYS_TENANT_ID == orig_table->get_tenant_id() ||                      \
               is_sys_table(orig_table->get_table_id()) ||                                                            \
               (!orig_table->is_index_table() && orig_table->is_in_splitting()) ||                                    \
               (orig_table->is_index_table() && !orig_table->can_read_index())) {                                     \
      /* Try to mock index status when all the following condititions are met */                                      \
      /* 1.In standby cluster 2.In normal tenant 3. It's avaliable user index or not splitting user table*/           \
    } else if (!is_inited_) {                                                                                         \
      ret = OB_NOT_INIT;                                                                                              \
      LOG_WARN("not init yet", K(ret));                                                                               \
    } else if (OB_FAIL(SCHEMA_MAP.get_refactored(orig_table->get_table_id(), table))) {                               \
      uint64_t table_id = orig_table->get_table_id();                                                                 \
      SCHEMA* tmp_table = NULL;                                                                                       \
      if (OB_HASH_NOT_EXIST) {                                                                                        \
        const uint64_t tablegroup_id = orig_table->get_tablegroup_id();                                               \
        bool check_tablegroup = is_new_tablegroup_id(tablegroup_id);                                                  \
        const TABLEGROUP_SCHEMA* tablegroup = NULL;                                                                   \
        ObMockSchemaInfo mock_schema_info;                                                                            \
        if (OB_FAIL(guard.get_mock_schema_info(table_id, TABLE_SCHEMA, mock_schema_info))) {                          \
          if (OB_ENTRY_NOT_EXIST == ret) {                                                                            \
            /* case 1: record doesn't exist. try mock table's partition status with tablegroup's. */                  \
            ret = OB_SUCCESS;                                                                                         \
            if (!check_tablegroup) {                                                                                  \
              new_table = orig_table;                                                                                 \
            } else if (OB_FAIL(guard.get_tablegroup_schema(orig_table->get_tablegroup_id(), tablegroup))) {           \
              LOG_WARN("fail to get tablegroup", K(ret), K(table_id), K(tablegroup_id));                              \
            } else if (OB_ISNULL(tablegroup)) {                                                                       \
              ret = OB_ERR_UNEXPECTED;                                                                                \
              LOG_WARN("tablegroup is null", K(ret), K(table_id), K(tablegroup_id));                                  \
            } else if (tablegroup->get_partition_status() == new_table->get_partition_status()) {                     \
              new_table = orig_table;                                                                                 \
            } else if (OB_FAIL(ObSchemaUtils::alloc_schema(alloc_, *orig_table, tmp_table))) {                        \
              LOG_WARN("fail to alloc new table", K(ret), K(table_id));                                               \
            } else {                                                                                                  \
              tmp_table->set_partition_status(tablegroup->get_partition_status());                                    \
              new_table = tmp_table;                                                                                  \
            }                                                                                                         \
          } else {                                                                                                    \
            LOG_WARN("fail to get mock schema info", K(ret), K(table_id));                                            \
          }                                                                                                           \
        } else {                                                                                                      \
          /* __all_ddl_helper has corresponding records. So we need to mock index status */                           \
          if (OB_FAIL(ObSchemaUtils::alloc_schema(alloc_, *orig_table, tmp_table))) {                                 \
            LOG_WARN("fail to alloc new table", K(ret), K(table_id));                                                 \
          } else {                                                                                                    \
            bool is_exist = false;                                                                                    \
            /* mock index status */                                                                                   \
            if (OB_FAIL(mock_schema_info.has_mock_schema_type(ObMockSchemaInfo::MOCK_INDEX_UNAVAILABLE, is_exist))) { \
              LOG_WARN("fail to get mock schema type", K(ret), K(table_id));                                          \
            } else if (is_exist && orig_table->is_index_table() &&                                                    \
                       is_available_index_status(orig_table->get_index_status())) {                                   \
              tmp_table->set_index_status(INDEX_STATUS_UNAVAILABLE);                                                  \
              if (tmp_table->is_global_index_table()) {                                                               \
                bool is_mock = true;                                                                                  \
                tmp_table->set_mock_global_index_invalid(is_mock);                                                    \
              }                                                                                                       \
            }                                                                                                         \
            /* mock split status */                                                                                   \
            if (OB_FAIL(ret)) {                                                                                       \
            } else if (OB_FAIL(                                                                                       \
                           mock_schema_info.has_mock_schema_type(ObMockSchemaInfo::MOCK_SCHEMA_SPLIT, is_exist))) {   \
              LOG_WARN("fail to get mock schema type", K(ret), K(table_id));                                          \
            } else if (is_exist) {                                                                                    \
              tmp_table->set_partition_status(PARTITION_STATUS_PHYSICAL_SPLITTING);                                   \
            } else if (!check_tablegroup) {                                                                           \
              /* skip */                                                                                              \
            } else if (OB_FAIL(guard.get_tablegroup_schema(orig_table->get_tablegroup_id(), tablegroup))) {           \
              LOG_WARN("fail to get tablegroup", K(ret), K(table_id), K(tablegroup_id));                              \
            } else if (OB_ISNULL(tablegroup)) {                                                                       \
              ret = OB_ERR_UNEXPECTED;                                                                                \
              LOG_WARN("tablegroup_schema is null", K(ret), K(table_id), K(tablegroup_id));                           \
            } else {                                                                                                  \
              tmp_table->set_partition_status(tablegroup->get_partition_status());                                    \
            }                                                                                                         \
            if (OB_SUCC(ret)) {                                                                                       \
              new_table = tmp_table;                                                                                  \
            }                                                                                                         \
          }                                                                                                           \
        }                                                                                                             \
        bool overwrite = true;                                                                                        \
        if (OB_FAIL(ret)) {                                                                                           \
        } else if (OB_FAIL(SCHEMA_MAP.set_refactored(table_id, new_table, overwrite))) {                              \
          LOG_WARN("fail to set new_table", K(ret), K(table_id));                                                     \
        }                                                                                                             \
      } else {                                                                                                        \
        LOG_WARN("fail to get table schema", K(ret), K(table_id));                                                    \
      }                                                                                                               \
    } else if (OB_ISNULL(table)) {                                                                                    \
      ret = OB_ERR_UNEXPECTED;                                                                                        \
      LOG_WARN("table is null", K(ret));                                                                              \
    } else {                                                                                                          \
      /* use cached mocked schema */                                                                                  \
      new_table = static_cast<const SCHEMA*>(table);                                                                  \
    }                                                                                                                 \
    if (OB_SUCC(ret)) {                                                                                               \
      if (OB_NOT_NULL(orig_table) && OB_NOT_NULL(new_table)) {                                                        \
        LOG_DEBUG("[MOCK_SCHEMA] get table schema from helper",                                                       \
            "table_id",                                                                                               \
            orig_table->get_table_id(),                                                                               \
            "table_schema_version",                                                                                   \
            orig_table->get_schema_version(),                                                                         \
            "orig_partition_status",                                                                                  \
            orig_table->get_partition_status(),                                                                       \
            "new_partition_status",                                                                                   \
            new_table->get_partition_status(),                                                                        \
            "orig_index_status",                                                                                      \
            orig_table->get_index_status(),                                                                           \
            "new_index_status",                                                                                       \
            new_table->get_index_status(),                                                                            \
            "orig_mock_global_index_flag",                                                                            \
            orig_table->is_mock_global_index_invalid(),                                                               \
            "new_mock_global_index_flag",                                                                             \
            new_table->is_mock_global_index_invalid());                                                               \
      }                                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }

GET_TABLE_SCHEMA_FROM_SCHEMA_HELPER(ObTableSchema, ObTablegroupSchema, full_schemas_);
GET_TABLE_SCHEMA_FROM_SCHEMA_HELPER(ObSimpleTableSchemaV2, ObSimpleTablegroupSchema, simple_schemas_);

bool ObSchemaGetterGuard::compair_schema_mgr_info_with_tenant_id(
    const ObSchemaMgrInfo& schema_mgr, const uint64_t& tenant_id)
{
  return schema_mgr.get_tenant_id() < tenant_id;
}

int ObSchemaGetterGuard::get_schema_mgr(const uint64_t tenant_id, const ObSchemaMgr*& schema_mgr) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgrInfo* schema_mgr_info = NULL;
  schema_mgr = NULL;
  if (!is_schema_splited()) {
    schema_mgr = mgr_;
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_mgr_info(tenant_id, schema_mgr_info))) {
    LOG_WARN("fail to get schema_mgr_info", K(ret), K(tenant_id), K(tenant_id_));
  } else {
    schema_mgr = schema_mgr_info->get_schema_mgr();
    if (OB_ISNULL(schema_mgr)) {
      LOG_DEBUG("schema_mgr is null", K_(is_inited), K(tenant_id), K(tenant_id_), KPC(schema_mgr_info));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_mgr_info(const uint64_t tenant_id, const ObSchemaMgrInfo*& schema_mgr_info) const
{
  int ret = OB_SUCCESS;
  schema_mgr_info = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (schema_mgr_infos_.count() == 2) {
#define MATCH_TENANT_SCHEMA_MGR(x)                         \
  if (schema_mgr_infos_[x].get_tenant_id() == tenant_id) { \
    schema_mgr_info = &schema_mgr_infos_[x];               \
  }
    MATCH_TENANT_SCHEMA_MGR(1) else MATCH_TENANT_SCHEMA_MGR(0);
    if (OB_ISNULL(schema_mgr_info)) {
      ret = OB_TENANT_NOT_EXIST;
    }
  } else {
    int64_t left = 0;
    int64_t right = schema_mgr_infos_.count() - 1;
    const ObSchemaMgrInfo* tmp_schema_mgr = NULL;
    while (left <= right) {
      int64_t mid = (left + right) / 2;
      tmp_schema_mgr = &(schema_mgr_infos_.at(mid));
      if (tmp_schema_mgr->get_tenant_id() == tenant_id) {
        schema_mgr_info = tmp_schema_mgr;
        break;
      } else if (tmp_schema_mgr->get_tenant_id() > tenant_id) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
    if (OB_ISNULL(schema_mgr_info)) {
      ret = OB_TENANT_NOT_EXIST;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_lazy_guard(const uint64_t tenant_id, const ObSchemaMgr*& mgr) const
{
  int ret = OB_SUCCESS;
  mgr = NULL;
  if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_status(const uint64_t tenant_id, ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  schema_status.reset();
  if (!is_schema_splited()) {
    // skip
  } else {
    const ObSchemaMgrInfo* schema_mgr_info = NULL;
    if (OB_FAIL(get_schema_mgr_info(tenant_id, schema_mgr_info))) {
      LOG_WARN("fail to get schema_mgr_info", K(ret), K(tenant_id));
    } else {
      schema_status = schema_mgr_info->get_schema_status();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_tenant_is_restore(const uint64_t tenant_id, bool& is_restore)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* schema_mgr = NULL;
  is_restore = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    is_restore = false;
  } else if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    // lazy mode
    const ObTenantSchema* tenant_schema = NULL;
    if (OB_FAIL(get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(ret), K(tenant_id));
    } else {
      is_restore = tenant_schema->is_restore();
    }
  } else {
    const ObSimpleTenantSchema* tenant_schema = NULL;
    if (OB_FAIL(schema_mgr->get_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(ret), K(tenant_id));
    } else {
      is_restore = tenant_schema->is_restore();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool& is_dropped)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* schema_mgr = NULL;
  ObDropTenantInfo drop_tenant_info;
  is_dropped = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", K(ret));
  } else if (OB_FAIL(schema_mgr->get_drop_tenant_info(tenant_id, drop_tenant_info))) {
    LOG_WARN("fail to get drop tenant info", K(ret), K(tenant_id));
  } else {
    is_dropped = (drop_tenant_info.is_valid());
  }
  return ret;
}

// Can't get other normal tenant's user table with tenant schema guard
bool ObSchemaGetterGuard::check_fetch_table_id(const uint64_t table_id) const
{
  bool bret = true;
  uint64_t tenant_id = extract_tenant_id(table_id);
  if (!is_inner_table(table_id) && OB_INVALID_TENANT_ID != tenant_id_ && tenant_id != tenant_id_) {
    bret = false;
  }
  return bret;
}

/*
 * check if schema guard's schema version is a format schema version.
 * 1. Before schema split, we can schema version from schema_mgr_.
 * 2. After schema split, only system tenant may generate schema mgr with informat schema version,
 *    so we can get schema version from sys tenant's schema_mgr_.
 */
int ObSchemaGetterGuard::check_formal_guard() const
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(get_schema_version(OB_SYS_TENANT_ID, schema_version))) {
    LOG_WARN("fail to get schema_version", K(ret), K(schema_version));
  } else if (OB_CORE_SCHEMA_VERSION + 1 == schema_version || ObSchemaService::is_formal_version(schema_version)) {
    // We thought "OB_CORE_SCHEMA_VERSION + 1" is a format schema version, because
    // schema mgr with such schema version is the first complete schema mgr generated in the bootstrap stage.
    ret = OB_SUCCESS;
  } else {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("local schema_version is not formal, try again", K(ret), K(schema_version));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sys_priv_with_tenant_id(const uint64_t tenant_id, ObIArray<const ObSysPriv*>& sys_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;
  sys_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_sys_privs_in_tenant(tenant_id, sys_privs))) {
    LOG_WARN("get sys priv with tenant_id failed", K(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_sys_priv_with_grantee_id(
    const uint64_t tenant_id, const uint64_t grantee_id, ObSysPriv*& sys_priv)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == grantee_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(grantee_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_sys_priv_in_grantee(tenant_id, grantee_id, sys_priv))) {
    LOG_WARN("get sys priv with user_id failed", K(ret), K(tenant_id), K(grantee_id));
  }

  return ret;
}

int ObSchemaGetterGuard::is_lazy_mode(const uint64_t tenant_id, bool& is_lazy) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr* schema_mgr = NULL;
  is_lazy = false;
  if (OB_FAIL(get_schema_mgr(tenant_id, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", K(ret), K(tenant_id));
  } else {
    is_lazy = OB_ISNULL(schema_mgr);
  }
  return ret;
}

int ObSchemaGetterGuard::try_mock_rowid(const ObTableSchema* org_table, const ObTableSchema*& final_table)
{
  int ret = OB_SUCCESS;
  final_table = org_table;
  if (OB_ISNULL(org_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table schema", K(ret));
  } else if (!sql::ObSQLMockSchemaUtils::is_mock_table(org_table->get_table_id())) {
    // do nothing
  } else {
    // is_mock_table is true means this function works in sql worker thread,
    // sosql_arena_allocator must be valid.
    ObIAllocator& allocator = THIS_WORKER.get_sql_arena_allocator();
    ObTableSchema* tmp_table = NULL;
    if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, *org_table, tmp_table))) {
      LOG_WARN("failed to alloc schema", K(ret));
    } else if (OB_FAIL(sql::ObSQLMockSchemaUtils::mock_rowid_column(*tmp_table))) {
      LOG_WARN("failed to mock rowid column", K(ret));
    } else {
      final_table = tmp_table;
      LOG_TRACE("mocked rowid column", K(*final_table));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_timestamp_service_type(const uint64_t tenant_id, int64_t& ts_type)
{
  int ret = OB_SUCCESS;
  ObString gts_name(share::OB_SV_TIMESTAMP_SERVICE);
  const ObSysVarSchema* var_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_GTS_SWITCH_GETTER);
  ObObj gts_obj;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_system_variable(tenant_id, gts_name, var_schema))) {
    LOG_WARN("get tenant system variable failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, var schema is NULL", KR(ret), K(tenant_id));
  } else if (OB_FAIL(var_schema->get_value(&allocator, NULL, gts_obj))) {
    LOG_WARN("get gts obj failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(gts_obj.get_int(ts_type))) {
    LOG_WARN("get gts value failed", KR(ret), K(tenant_id));
  }
  return ret;
}

// check whether the given table has global index or not
int ObSchemaGetterGuard::check_global_index_exist(const uint64_t tenant_id, const uint64_t table_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  const ObTableSchema *table_schema = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("get null table schema", K(ret), K(table_id));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObTableSchema *index_schema = NULL;
    if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !exist && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(simple_index_infos.at(i)));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema should not be null", K(ret));
      } else if (index_schema->can_read_index() && index_schema->is_index_visible() &&
                  index_schema->is_global_index_table()) {
        exist = true;
      } else { /* do nothing */ }
    }
  }

  return ret;
}

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
