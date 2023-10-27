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
#include "lib/worker.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_get_compat_mode.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_synonym_mgr.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/resolver/ob_schema_checker.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_priv_common.h"
#include "sql/dblink/ob_dblink_utils.h"
namespace oceanbase
{
using namespace common;
using namespace observer;

namespace share
{
namespace schema
{
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

ObSchemaMgrInfo &ObSchemaMgrInfo::operator=(const ObSchemaMgrInfo &other)
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

ObSchemaMgrInfo::ObSchemaMgrInfo(const ObSchemaMgrInfo &other)
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    snapshot_version_(common::OB_INVALID_VERSION),
    schema_mgr_(NULL),
    mgr_handle_(),
    schema_status_()
{
  *this = other;
}

ObSchemaGetterGuard::ObSchemaGetterGuard()
  : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_MGR_INFO_ARRAY, ObCtxIds::SCHEMA_SERVICE)),
    schema_service_(NULL),
    session_id_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    schema_mgr_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(local_allocator_)),
    schema_objs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(local_allocator_)),
    mod_(ObSchemaMgrItem::MOD_STACK),
    schema_guard_type_(INVALID_SCHEMA_GUARD_TYPE),
    is_standby_cluster_(false),
    restore_tenant_exist_(false),
    is_inited_(false),
    pin_cache_size_(0)
{
}

ObSchemaGetterGuard::ObSchemaGetterGuard(const ObSchemaMgrItem::Mod mod)
  : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_MGR_INFO_ARRAY, ObCtxIds::SCHEMA_SERVICE)),
    schema_service_(NULL),
    session_id_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    schema_mgr_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(local_allocator_)),
    schema_objs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(local_allocator_)),
    mod_(mod),
    schema_guard_type_(INVALID_SCHEMA_GUARD_TYPE),
    is_standby_cluster_(false),
    restore_tenant_exist_(false),
    is_inited_(false),
    pin_cache_size_(0)
{
}

ObSchemaGetterGuard::~ObSchemaGetterGuard()
{
  // Destruct handles_ will reduce reference count automatically.
  if (pin_cache_size_ >= FULL_SCHEMA_MEM_THREHOLD) {
    int ret = OB_SUCCESS;
    FLOG_WARN("hold too much full schema memory", K(tenant_id_), K(pin_cache_size_), K(lbt()));
  }
}

int ObSchemaGetterGuard::init(
    const bool is_standby_cluster)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    is_standby_cluster_ = is_standby_cluster;
    pin_cache_size_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSchemaGetterGuard::reset()
{
  int ret = OB_SUCCESS;
  schema_service_ = NULL;
  schema_objs_.reset();

  is_standby_cluster_ = false;
  restore_tenant_exist_ = false;
  if (pin_cache_size_ >= FULL_SCHEMA_MEM_THREHOLD) {
    FLOG_WARN("hold too much full schema memory", K(tenant_id_), K(pin_cache_size_), K(lbt()));
  }
  pin_cache_size_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;

  for (int64_t i = 0; i < schema_mgr_infos_.count(); i++) {
    schema_mgr_infos_.at(i).reset();
  }
  schema_mgr_infos_.reset();
  local_allocator_.reuse();

  // mod_ should not be reset

  is_inited_ = false;
  return ret;
}

void ObSchemaGetterGuard::dump()
{
  LOG_INFO("tenant_id", K(tenant_id_));
}

int ObSchemaGetterGuard::get_schema_version(const uint64_t tenant_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgrInfo *schema_mgr_info = NULL;
  if (OB_FAIL(get_schema_mgr_info(tenant_id, schema_mgr_info))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      const ObSimpleTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(get_schema_mgr_info(OB_SYS_TENANT_ID, schema_mgr_info))) {
        LOG_WARN("fail to get sys schema_mgr_info", KR(ret));
      } else if (OB_ISNULL(schema_mgr_info)
                 || OB_ISNULL(schema_mgr_info->get_schema_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_mgr_info or schema_mgr is null", KR(ret), KP(schema_mgr_info));
      } else if (OB_FAIL(schema_mgr_info->get_schema_mgr()->get_tenant_schema(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
      } else {
        // return special schema version while creating tenant
        schema_version = OB_CORE_SCHEMA_VERSION;
      }
    } else {
      LOG_WARN("fail to schema mgr info", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(schema_mgr_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_info is null", KR(ret), K(tenant_id));
  } else {
    schema_version = schema_mgr_info->get_snapshot_version();
  }
  if (OB_FAIL(ret)
      && OB_TENANT_HAS_BEEN_DROPPED != ret
      && ObSchemaService::g_liboblog_mode_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(schema_service_))  {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", KR(ret));
    } else {
      TenantStatus tenant_status = TENANT_STATUS_INVALID;
      tmp_ret = schema_service_->query_tenant_status(tenant_id, tenant_status);
      if (OB_SUCCESS != tmp_ret){
        LOG_WARN("query tenant status failed", KR(ret), K(tmp_ret), K(tenant_id));
      } else if (TENANT_DELETED == tenant_status) {
        LOG_INFO("tenant has been dropped, no need retry", KR(ret), K(tenant_id));
        ret = OB_TENANT_HAS_BEEN_DROPPED; //overwrite ret
      }
    }
  }
  return ret;
}

// For SQL only
int ObSchemaGetterGuard::get_can_read_index_array(
    const uint64_t tenant_id,
    const uint64_t table_id,
    uint64_t *index_tid_array,
    int64_t &size,
    bool with_mv,
    bool with_global_index /* =true */,
    bool with_domain_index /*=true*/,
    bool with_spatial_index /*=true*/)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))
             || OB_ISNULL(table_schema)) {
    //TODO: ignore error even when table doesn't exist ?
    LOG_WARN("cannot get table schema for table  ", K(tenant_id), K(table_id), KR(ret));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObTableSchema *index_schema = NULL;
    int64_t can_read_count = 0;
    bool is_geo_default_srid = false;
    if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id), K(table_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const uint64_t index_id = simple_index_infos.at(i).table_id_;
      if (OB_FAIL(get_table_schema(tenant_id, index_id, index_schema))) {
        LOG_WARN("cannot get table schema for table", KR(ret), K(tenant_id), K(index_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", KR(ret), K(tenant_id), K(index_id));
      } else if (index_schema->is_spatial_index() && !with_spatial_index) {
        uint64_t geo_col_id = UINT64_MAX;
        const ObColumnSchemaV2 *geo_column = NULL;
        is_geo_default_srid = false;
        if (OB_FAIL(index_schema->get_spatial_geo_column_id(geo_col_id))) {
          LOG_WARN("failed to get geometry column id", K(ret));
        } else if (OB_ISNULL(geo_column = table_schema->get_column_schema(geo_col_id))) {
          LOG_WARN("failed to get geometry column", K(ret), K(geo_col_id));
        } else if (geo_column->is_default_srid()) {
          is_geo_default_srid = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (!with_mv && index_schema->is_materialized_view()) {
          // skip
        } else if (!with_global_index && index_schema->is_global_index_table()) {
          // skip
        } else if (!with_domain_index && index_schema->is_domain_index()) {
          // does not need domain index, skip it
        } else if (!with_spatial_index && index_schema->is_spatial_index() && is_geo_default_srid) {
          // skip spatial index when geometry column has not specific srid.
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

int ObSchemaGetterGuard::check_has_local_unique_index(
    const uint64_t tenant_id,
    const uint64_t table_id,
    bool &has_local_unique_index)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  has_local_unique_index = false;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("cannot get table schema for table ", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id), K(table_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const uint64_t index_id = simple_index_infos.at(i).table_id_;
    if (OB_FAIL(get_simple_table_schema(tenant_id, index_id, index_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ",
               KR(ret), K(tenant_id), K(index_id));
    } else if (OB_UNLIKELY(index_schema->is_final_invalid_index())) {
      //invalid index status, need ingore
    } else if (index_schema->is_local_unique_index_table()) {
      has_local_unique_index = true;
      break;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_all_unique_index(const uint64_t tenant_id,
                                              const uint64_t table_id,
                                              ObIArray<uint64_t> &unique_index_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("cannot get table schema for table ", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id), K(table_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const uint64_t index_id = simple_index_infos.at(i).table_id_;
    if (OB_FAIL(get_simple_table_schema(tenant_id, index_id, index_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ",
               KR(ret), K(tenant_id), K(index_id));
    } else if (OB_UNLIKELY(index_schema->is_final_invalid_index())) {
      //invalid index status, need ingore
    } else if ((index_schema->is_local_unique_index_table() ||
               index_schema->is_global_unique_index_table()) &&
               OB_FAIL(unique_index_ids.push_back(index_id))) {
      LOG_WARN("failed to push back local unique index", K(ret));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_id(const ObString &tenant_name,
                                       uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_name), KR(ret));
  } else if (0 == tenant_name.case_compare(OB_GTS_TENANT_NAME)) {
    tenant_id = OB_GTS_TENANT_ID;
  } else {
    // FIXME: just compatible old code for lock, can it be moved to upper level?
    const ObTenantSchema *tenant_info = NULL;
    if (OB_FAIL(get_tenant_info(tenant_name, tenant_info))) {
      LOG_WARN("get tenant info failed", KR(ret), K(tenant_name));
    } else if (NULL == tenant_info) {
      ret = OB_ERR_INVALID_TENANT_NAME;
      LOG_WARN("Can not find tenant", K(tenant_name));
    } else if (tenant_info->get_locked()) {
      ret = OB_ERR_TENANT_IS_LOCKED;
      LOG_WARN("Tenant is locked", KR(ret));
    } else {
      tenant_id = tenant_info->get_tenant_id();
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_sys_variable_schema(const ObString &tenant_name,
                                                 const ObSysVariableSchema *&sys_variable_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_name));
  } else if (OB_FAIL(get_tenant_info(tenant_name, tenant_info))) {
    LOG_WARN("get tenant info failed", KR(ret), K(tenant_name));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_INVALID_TENANT_NAME;
    LOG_WARN("Can not find tenant", KR(ret), K(tenant_name));
  } else if (OB_FAIL(get_schema(SYS_VARIABLE_SCHEMA,
                                tenant_info->get_tenant_id(),
                                tenant_info->get_tenant_id(),
                                sys_variable_schema))) {
    LOG_WARN("fail to get sys var schema", KR(ret), K(tenant_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sys_variable_schema(const uint64_t tenant_id,
                                                 const ObSysVariableSchema *&sys_variable_schema)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(get_schema(SYS_VARIABLE_SCHEMA,
                                tenant_id,
                                tenant_id,
                                sys_variable_schema))) {
    LOG_WARN("fail to get sys var schema", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sys_variable_schema(const uint64_t tenant_id,
                                                 const ObSimpleSysVariableSchema *&sys_variable_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", KR(ret), K(tenant_id), "schema_version", mgr->get_schema_version());
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_system_variable(uint64_t tenant_id, const ObString &var_name, const ObSysVarSchema *&var_schema)
{
  int ret = OB_SUCCESS;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_FAIL(get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get tenant info failed", KR(ret), K(tenant_id));
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

int ObSchemaGetterGuard::get_tenant_system_variable(uint64_t tenant_id, ObSysVarClassType var_id, const ObSysVarSchema *&var_schema)
{
  int ret = OB_SUCCESS;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_FAIL(get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get tenant info failed", KR(ret), K(tenant_id));
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

int ObSchemaGetterGuard::get_user_id(uint64_t tenant_id,
                                     const ObString &user_name,
                                     const ObString &host_name,
                                     uint64_t &user_id,
                                     const bool is_role /*false*/)
{
  int ret = OB_SUCCESS;
  UNUSED(is_role);
  const ObSchemaMgr *mgr = NULL;
  user_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleUserSchema *simple_user = NULL;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant mode", KR(ret), K(tenant_id));
    } else if (0 == user_name.case_compare(OB_SYS_USER_NAME)
               && 0 == host_name.case_compare(OB_SYS_HOST_NAME)
               && (lib::Worker::CompatMode::ORACLE != compat_mode)) {
      // root is not an inner user in oracle mode.
      user_id = OB_SYS_USER_ID;
    } else if (OB_FAIL(mgr->get_user_schema(tenant_id,
                                             user_name,
                                             host_name,
                                             simple_user))) {
      LOG_WARN("get simple user failed", KR(ret), K(tenant_id), K(user_name), K(host_name));
    } else if (NULL == simple_user) {
      LOG_INFO("user not exist", K(tenant_id), K(user_name), K(host_name));
    } else {
      user_id = simple_user->get_user_id();
      LOG_TRACE("succ to get user", K(tenant_id), K(user_id), K(user_name), K(host_name), KPC(simple_user));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_outline_infos_in_database(
    const uint64_t tenant_id,
    const uint64_t database_id,
    ObIArray<const ObOutlineInfo *> &outline_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  outline_infos.reset();

  ObArray<const ObSimpleOutlineSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schemas_in_database(tenant_id,
      database_id, schemas))) {
    LOG_WARN("get outlne schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleOutlineSchema *tmp_schema = *schema;
      const ObOutlineInfo *outline_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(OUTLINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_outline_id(),
                                    outline_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "outline_id", tmp_schema->get_outline_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(outline_infos.push_back(outline_info))) {
        LOG_WARN("add outline schema failed", KR(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_trigger_infos_in_database(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  common::ObIArray<const ObTriggerInfo *> &tg_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tg_infos.reset();

  ObArray<const ObSimpleTriggerSchema *> tg_schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->trigger_mgr_.get_trigger_schemas_in_database(tenant_id, database_id, tg_schemas))) {
    LOG_WARN("get trigger schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(tg, tg_schemas, OB_SUCC(ret)) {
      const ObSimpleTriggerSchema *tmp_tg = *tg;
      const ObTriggerInfo *tg_info = NULL;
      if (OB_ISNULL(tmp_tg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_tg));
      } else if (OB_FAIL(get_schema(TRIGGER_SCHEMA,
                                    tmp_tg->get_tenant_id(),
                                    tmp_tg->get_trigger_id(),
                                    tg_info,
                                    tmp_tg->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 K(tmp_tg->get_trigger_id()), K(tmp_tg->get_schema_version()));
      } else if (OB_FAIL(tg_infos.push_back(tg_info))) {
        LOG_WARN("add trigger infos failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_synonym_infos_in_database(
    const uint64_t tenant_id,
    const uint64_t database_id,
    ObIArray<const ObSynonymInfo *> &synonym_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  synonym_infos.reset();

  ObArray<const ObSimpleSynonymSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schemas_in_database(tenant_id,
      database_id, schemas))) {
    LOG_WARN("get synonym schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleSynonymSchema *tmp_schema = *schema;
      const ObSynonymInfo *synonym_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(SYNONYM_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_synonym_id(),
                                    synonym_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "synonym_id", tmp_schema->get_synonym_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(synonym_infos.push_back(synonym_info))) {
        LOG_WARN("add synonym schema failed", KR(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_package_infos_in_database(const uint64_t tenant_id,
                                                       const uint64_t database_id,
                                                       common::ObIArray<const ObPackageInfo *> &package_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  package_infos.reset();

  ObArray<const ObSimplePackageSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->package_mgr_.get_package_schemas_in_database(tenant_id,
      database_id, schemas))) {
    LOG_WARN("get package schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimplePackageSchema *tmp_schema = *schema;
      const ObPackageInfo *package_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(PACKAGE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_package_id(),
                                    package_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "package_id", tmp_schema->get_package_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(package_infos.push_back(package_info))) {
        LOG_WARN("add package schema failed", KR(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_routine_infos_in_database(const uint64_t tenant_id,
                                                       const uint64_t database_id,
                                                       common::ObIArray<const ObRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_infos.reset();

  ObArray<const ObSimpleRoutineSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schemas_in_database(tenant_id, database_id, schemas))) {
    LOG_WARN("get routine schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleRoutineSchema *tmp_schema = *schema;
      const ObRoutineInfo *routine_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_routine_id(),
                                    routine_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "routine_id", tmp_schema->get_routine_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(routine_infos.push_back(routine_info))) {
        LOG_WARN("add routine schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_routine_infos_in_udt(
  const uint64_t tenant_id, const uint64_t udt_id,
  common::ObIArray<const ObRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_infos.reset();

  ObArray<const ObSimpleRoutineSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == udt_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(udt_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schemas_in_udt(tenant_id, udt_id, schemas))) {
    LOG_WARN("get routine schemas in package failed", KR(ret), K(tenant_id), K(udt_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleRoutineSchema *tmp_schema = *schema;
      const ObRoutineInfo *routine_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_routine_id(),
                                    routine_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "routine_id", tmp_schema->get_routine_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(routine_infos.push_back(routine_info))) {
        LOG_WARN("add routine schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_routine_info_in_udt(const uint64_t tenant_id,
                                                 const uint64_t udt_id,
                                                 const uint64_t subprogram_id,
                                                 const ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_info = NULL;

  ObArray<const ObSimpleRoutineSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == udt_id || OB_INVALID_ID == subprogram_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(udt_id), K(subprogram_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schemas_in_udt(tenant_id, udt_id, schemas))) {
    LOG_WARN("get routine schemas in package failed", KR(ret), K(tenant_id), K(udt_id));
  } else {
    bool is_break = false;
    FOREACH_CNT_X(schema, schemas, (OB_SUCC(ret) && !is_break)) {
      const ObSimpleRoutineSchema *tmp_schema = *schema;
      const ObRoutineInfo *sub_routine_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_routine_id(),
                                    sub_routine_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "routine_id", tmp_schema->get_routine_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_ISNULL(sub_routine_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("routine info is null", KR(ret));
      } else if (subprogram_id == sub_routine_info->get_subprogram_id()) {
        routine_info = sub_routine_info;
        is_break = true;
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_routine_info_in_package(const uint64_t tenant_id,
                                                     const uint64_t package_id,
                                                     const uint64_t subprogram_id,
                                                     const ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_info = NULL;

  ObArray<const ObSimpleRoutineSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == package_id || OB_INVALID_ID == subprogram_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(package_id), K(subprogram_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schemas_in_package(tenant_id, package_id, schemas))) {
    LOG_WARN("get routine schemas in package failed", KR(ret), K(tenant_id), K(package_id));
  } else {
    bool is_break = false;
    FOREACH_CNT_X(schema, schemas, (OB_SUCC(ret) && !is_break)) {
      const ObSimpleRoutineSchema *tmp_schema = *schema;
      const ObRoutineInfo *sub_routine_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_routine_id(),
                                    sub_routine_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "routine_id", tmp_schema->get_routine_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_ISNULL(sub_routine_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("routine info is null", KR(ret));
      } else if (subprogram_id == sub_routine_info->get_subprogram_id()) {
        routine_info = sub_routine_info;
        is_break = true;
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_routine_infos_in_package(
  const uint64_t tenant_id, const uint64_t package_id,
  common::ObIArray<const ObRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_infos.reset();

  ObArray<const ObSimpleRoutineSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(package_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schemas_in_package(tenant_id, package_id, schemas))) {
    LOG_WARN("get routine schemas in package failed", KR(ret), K(tenant_id), K(package_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleRoutineSchema *tmp_schema = *schema;
      const ObRoutineInfo *routine_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_routine_id(),
                                    routine_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "routine_id", tmp_schema->get_routine_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(routine_infos.push_back(routine_info))) {
        LOG_WARN("add routine schema failed", KR(ret));
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_udt_infos_in_database(const uint64_t tenant_id,
                                                   const uint64_t database_id,
                                                   common::ObIArray<const ObUDTTypeInfo *> &udt_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  udt_infos.reset();

  ObArray<const ObSimpleUDTSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->udt_mgr_.get_udt_schemas_in_database(tenant_id, database_id, schemas))) {
    LOG_WARN("get udt schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleUDTSchema *tmp_schema = *schema;
      const ObUDTTypeInfo *udt_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(UDT_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_type_id(),
                                    udt_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "udt_id", tmp_schema->get_type_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(udt_infos.push_back(udt_info))) {
        LOG_WARN("add udt schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_sequence_infos_in_database(
    const uint64_t tenant_id,
    const uint64_t database_id,
    common::ObIArray<const ObSequenceSchema *> &sequence_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  sequence_infos.reset();

  ObArray<const ObSequenceSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sequence_mgr_.get_sequence_schemas_in_database(tenant_id, database_id, schemas))) {
    LOG_WARN("get sequence schemas in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSequenceSchema *tmp_schema = *schema;
      const ObSequenceSchema *sequence_info = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(get_schema(SEQUENCE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_sequence_id(),
                                    sequence_info,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get schema failed", KR(ret), K(tenant_id),
                 "sequence_id", tmp_schema->get_sequence_id(),
                 "schema_version", tmp_schema->get_schema_version());
      } else if (OB_FAIL(sequence_infos.push_back(sequence_info))) {
        LOG_WARN("add sequence schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_policy_infos_in_tenant(
    const uint64_t tenant_id,
    common::ObIArray<const ObLabelSePolicySchema *> &label_se_policy_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  label_se_policy_infos.reset();

  ObArray<const ObLabelSePolicySchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
      mgr->label_se_policy_mgr_.get_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get schemas in database failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObLabelSePolicySchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(label_se_policy_infos.push_back(tmp_schema))) {
        LOG_WARN("add schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_profile_schema_by_name(const uint64_t tenant_id,
                                                    const common::ObString &name,
                                                    const ObProfileSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->profile_mgr_.get_schema_by_name(tenant_id, name, schema))) {
    LOG_WARN("get schema failed", K(name), KR(ret));
  }
  return ret;
}


int ObSchemaGetterGuard::get_label_se_component_infos_in_tenant(const uint64_t tenant_id,
    common::ObIArray<const ObLabelSeComponentSchema *> &label_se_component_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  label_se_component_infos.reset();

  ObArray<const ObLabelSeComponentSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
      mgr->label_se_component_mgr_.get_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get schemas in database failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObLabelSeComponentSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(label_se_component_infos.push_back(tmp_schema))) {
        LOG_WARN("add schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_profile_schema_by_id(const uint64_t tenant_id,
                                                  const uint64_t profile_id,
                                                  const ObProfileSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_id(profile_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(profile_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_profile_schema(tenant_id, profile_id, schema))) {
    LOG_WARN("get schema failed", K(tenant_id), K(profile_id), KR(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("NULL ptr", KR(ret), K(schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_label_infos_in_tenant(
    const uint64_t tenant_id,
    common::ObIArray<const ObLabelSeLabelSchema *> &label_se_label_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  label_se_label_infos.reset();

  ObArray<const ObLabelSeLabelSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
      mgr->label_se_label_mgr_.get_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get sequence schemas in database failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObLabelSeLabelSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(label_se_label_infos.push_back(tmp_schema))) {
        LOG_WARN("add schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_user_level_infos_in_tenant(
    const uint64_t tenant_id,
    common::ObIArray<const ObLabelSeUserLevelSchema *> &label_se_user_level_infos)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  label_se_user_level_infos.reset();

  ObArray<const ObLabelSeUserLevelSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
      mgr->label_se_user_level_mgr_.get_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get schemas in database failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObLabelSeUserLevelSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(label_se_user_level_infos.push_back(tmp_schema))) {
        LOG_WARN("add schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int64_t combine_default_value(int64_t value, int64_t default_value)
{
  return ObProfileSchema::DEFAULT_VALUE == value ? default_value : value;
}

int ObSchemaGetterGuard::get_user_profile_failed_login_limits(
    const uint64_t tenant_id,
    const uint64_t user_id,
    int64_t &failed_login_limit_num,
    int64_t &failed_login_limit_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = nullptr;
  const ObProfileSchema *profile_info = nullptr;
  const ObProfileSchema *default_profile = nullptr;
  uint64_t profile_id = OB_INVALID_ID;

  if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else {
    uint64_t default_profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    profile_id = user_info->get_profile_id();
    if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                         is_valid_id(profile_id) ? profile_id : default_profile_id,
                                         profile_info))) {
       LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                                default_profile_id,
                                                default_profile))) {
      LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else {
      failed_login_limit_num = combine_default_value(profile_info->get_failed_login_attempts(),
                                                     default_profile->get_failed_login_attempts());
      failed_login_limit_time = combine_default_value(profile_info->get_password_lock_time(),
                                                      default_profile->get_password_lock_time());
    }
  }

  return ret;
}

// only use in oracle mode
int ObSchemaGetterGuard::get_user_password_expire_times(
    const uint64_t tenant_id,
    const uint64_t user_id,
    int64_t &password_last_change,
    int64_t &password_life_time,
    int64_t &password_grace_time)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = nullptr;
  const ObProfileSchema *profile_info = nullptr;
  const ObProfileSchema *default_profile = nullptr;
  uint64_t profile_id = OB_INVALID_ID;

  if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else {
    uint64_t default_profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    profile_id = user_info->get_profile_id();
    password_last_change = user_info->get_password_last_changed();
    if (!is_valid_id(profile_id)) {
      profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
    }
    if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                         is_valid_id(profile_id) ? profile_id : default_profile_id,
                                         profile_info))) {
       LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else if (OB_FAIL(get_profile_schema_by_id(user_info->get_tenant_id(),
                                                default_profile_id,
                                                default_profile))) {
      LOG_WARN("fail to get profile info", KR(ret), KPC(user_info));
    } else {
      password_life_time = combine_default_value(profile_info->get_password_life_time(),
                                                 default_profile->get_password_life_time());
      password_grace_time = combine_default_value(profile_info->get_password_grace_time(),
                                                  default_profile->get_password_grace_time());
    }
    password_life_time = (password_life_time == -1) ? INT64_MAX : password_life_time;
    password_grace_time = (password_grace_time == -1) ? INT64_MAX : password_grace_time;
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_profile_function_name(const uint64_t tenant_id,
                                                        const uint64_t profile_id,
                                                        ObString &function_name)
{
  int ret = OB_SUCCESS;
  const ObProfileSchema *profile_info = nullptr;
  const ObProfileSchema *default_profile = nullptr;

  uint64_t default_profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;

  if (OB_FAIL(get_profile_schema_by_id(tenant_id,
                                       is_valid_id(profile_id) ? profile_id : default_profile_id,
                                       profile_info))) {
     LOG_WARN("fail to get profile info", KR(ret));
  } else if (OB_FAIL(get_profile_schema_by_id(tenant_id,
                                              default_profile_id,
                                              default_profile))) {
    LOG_WARN("fail to get profile info", KR(ret));
  } else {
    function_name = profile_info->get_password_verify_function_str();
    if (0 == function_name.case_compare("DEFAULT")) {
      function_name = default_profile->get_password_verify_function_str();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_directory_schema_by_name(const uint64_t tenant_id,
                                                      const common::ObString &name,
                                                      const ObDirectorySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->directory_mgr_.get_directory_schema_by_name(tenant_id, name, schema))) {
    LOG_WARN("get directory schema failed", K(name), KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_directory_schema_by_id(const uint64_t tenant_id,
                                                    const uint64_t directory_id,
                                                    const ObDirectorySchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_UNLIKELY(!is_valid_id(directory_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(directory_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_directory_schema(tenant_id, directory_id, schema))) {
    LOG_WARN("get schema failed", K(tenant_id), K(directory_id), KR(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(schema));
  }
  return ret;
}


int ObSchemaGetterGuard::get_rls_policy_schema_by_name(const uint64_t tenant_id,
                                                       const uint64_t table_id,
                                                       const uint64_t rls_group_id,
                                                       const ObString &name,
                                                       const ObRlsPolicySchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id) ||
                         !is_valid_id(rls_group_id) || name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(rls_group_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_policy_mgr_.get_schema_by_name(tenant_id, table_id, rls_group_id,
                                                             name, schema))) {
    LOG_WARN("get schema failed", KR(ret), K(tenant_id), K(table_id), K(rls_group_id), K(name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_policy_schema_by_id(const uint64_t tenant_id,
                                                     const uint64_t rls_policy_id,
                                                     const ObRlsPolicySchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(rls_policy_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(rls_policy_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_policy_mgr_.get_schema_by_id(rls_policy_id, schema))) {
    LOG_WARN("get schema failed", KR(ret), K(rls_policy_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_policy_schemas_in_table(const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<const ObRlsPolicySchema *> &schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schemas.reset();
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_policy_mgr_.get_schemas_in_table(tenant_id, table_id, schemas))) {
    LOG_WARN("get rls policy schemas in table failed", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_policy_schemas_in_group(const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t rls_group_id,
    ObIArray<const ObRlsPolicySchema *> &schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schemas.reset();
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id)
                         || !is_valid_id(rls_group_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(rls_group_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_policy_mgr_.get_schemas_in_group(tenant_id, table_id,
      rls_group_id, schemas))) {
    LOG_WARN("get rls policy schemas in group failed", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_group_schema_by_name(const uint64_t tenant_id,
                                                       const uint64_t table_id,
                                                       const ObString &name,
                                                       const ObRlsGroupSchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id) ||
                         name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_group_mgr_.get_schema_by_name(tenant_id, table_id, name, schema))) {
    LOG_WARN("get schema failed", KR(ret), K(tenant_id), K(table_id), K(name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_group_schema_by_id(const uint64_t tenant_id,
                                                    const uint64_t rls_group_id,
                                                    const ObRlsGroupSchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(rls_group_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(rls_group_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_group_mgr_.get_schema_by_id(rls_group_id, schema))) {
    LOG_WARN("get schema failed", KR(ret), K(rls_group_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_group_schemas_in_table(const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<const ObRlsGroupSchema *> &schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schemas.reset();
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_group_mgr_.get_schemas_in_table(tenant_id, table_id, schemas))) {
    LOG_WARN("get rls group schemas in table failed", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_context_schema_by_name(const uint64_t tenant_id,
                                                       const uint64_t table_id,
                                                       const ObString &name,
                                                       const ObString &attribute,
                                                       const ObRlsContextSchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id) ||
                         name.empty() || attribute.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(name), K(attribute));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_context_mgr_.get_schema_by_name(tenant_id, table_id, name,
                                                             attribute, schema))) {
    LOG_WARN("get schema failed", KR(ret), K(tenant_id), K(table_id), K(name), K(attribute));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_context_schema_by_id(const uint64_t tenant_id,
                                                     const uint64_t rls_context_id,
                                                     const ObRlsContextSchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(rls_context_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(rls_context_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_context_mgr_.get_schema_by_id(rls_context_id, schema))) {
    LOG_WARN("get schema failed", KR(ret), K(rls_context_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_rls_context_schemas_in_table(const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<const ObRlsContextSchema *> &schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schemas.reset();
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->rls_context_mgr_.get_schemas_in_table(tenant_id, table_id, schemas))) {
    LOG_WARN("get rls context schemas in table failed", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

// For SQL only
int ObSchemaGetterGuard::get_can_write_index_array(
    const uint64_t tenant_id,
    const uint64_t table_id,
    uint64_t *index_tid_array,
    int64_t &size,
    bool only_global)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  int64_t can_write_count = 0;
  const ObSimpleTableSchemaV2 *index_schema = NULL;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("cannot get table schema for table ", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id), K(table_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const uint64_t index_id = simple_index_infos.at(i).table_id_;
    if (OB_FAIL(get_simple_table_schema(tenant_id, index_id, index_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot get index table schema for table ", KR(ret), K(tenant_id), K(index_id));
    } else if (OB_UNLIKELY(index_schema->is_final_invalid_index())) {
      //invalid index status, need ingore
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

// check if column is included in primary key/partition key/foreign key/index columns.
int ObSchemaGetterGuard::column_is_key(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t column_id,
    bool &is_key)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  const ObColumnSchemaV2 *column_schema = nullptr;
  is_key = false;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id), K(column_id));
  } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column schema is null", K(ret), K(table_id), K(column_id));
  } else if (table_schema->is_foreign_key(column_id)) {
    is_key = true;
  } else if (column_schema->is_rowkey_column() || column_schema->is_tbl_part_key_column()) {
    is_key = true;
  } else {
    int64_t index_tid_array_size = OB_MAX_INDEX_PER_TABLE;
    uint64_t index_tid_array[OB_MAX_INDEX_PER_TABLE];
    if (OB_FAIL(get_can_write_index_array(tenant_id, table_id, index_tid_array, index_tid_array_size))) {
      LOG_WARN("get index tid array failed", K(ret), K(tenant_id), K(index_tid_array_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_key && i < index_tid_array_size; ++i) {
      if (OB_FAIL(get_column_schema(tenant_id, index_tid_array[i], column_id, column_schema))) {
        LOG_WARN("get column schema from index schema failed", K(ret), K(tenant_id),
                 K(index_tid_array[i]), K(column_id), K(table_id));
      } else if (OB_ISNULL(column_schema)) {
        //not exists in this index, ignore it
      } else if (column_schema->is_index_column()) {
        is_key = true;
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_database_id(uint64_t tenant_id,
                                         const ObString &database_name,
                                         uint64_t &database_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  database_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), KR(ret));
  } else {
    const ObSimpleDatabaseSchema *simple_database = NULL;
    if ((database_name.length() == static_cast<int32_t> (strlen(OB_SYS_DATABASE_NAME)))
        && (0 == STRNCASECMP(database_name.ptr(), OB_SYS_DATABASE_NAME, strlen(OB_SYS_DATABASE_NAME)))) {
      // To avoid cyclic dependence while create tenant
      database_id = OB_SYS_DATABASE_ID;
    } else {
      if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
        LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
      } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
        LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(mgr->get_database_schema(tenant_id,
                                            database_name,
                                            simple_database))) {
        LOG_WARN("get simple database failed", KR(ret), K(tenant_id), K(database_name));
      } else if (NULL == simple_database) {
        if (0 == database_name.case_compare(OB_INFORMATION_SCHEMA_NAME)) {
          database_id = OB_INFORMATION_SCHEMA_ID;
        } else {
          LOG_INFO("database not exist", K(tenant_id), K(database_name));
        }
      } else {
        database_id = simple_database->get_database_id();
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_id(uint64_t tenant_id,
                                           const ObString &tablegroup_name,
                                           uint64_t &tablegroup_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tablegroup_id = OB_INVALID_ID;

  const ObSimpleTablegroupSchema *simple_tablegroup = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(tablegroup_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schema(tenant_id,
                                                 tablegroup_name,
                                                 simple_tablegroup))) {
    LOG_WARN("get simple tablegroup failed", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (NULL == simple_tablegroup) {
    LOG_INFO("tablegroup not exist", K(tenant_id), K(tablegroup_name));
  } else {
    tablegroup_id = simple_tablegroup->get_tablegroup_id();
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_id(uint64_t tenant_id,
                                      uint64_t database_id,
                                      const ObString &table_name,
                                      const bool is_index,
                                      const CheckTableType check_type, // check if temporary table is visable
                                      uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  uint64_t session_id = session_id_;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleTableSchemaV2 *simple_table = NULL;
  table_id = OB_INVALID_ID;

  if (NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE == check_type) {
    session_id = 0;
  } else { /* do nothing */ }
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(table_name), KR(ret));
  } else {
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id), K(tenant_id));
    } else if (OB_FAIL(mgr->get_table_schema(
                       tenant_id,
                       database_id,
                       session_id,
                       table_name,
                       is_index,
                       simple_table,
                       USER_HIDDEN_TABLE_TYPE == check_type ? true : false))) {
      LOG_WARN("get simple table failed", KR(ret), K(tenant_id),
               K(tenant_id), K(database_id), K(session_id), K(table_name), K(is_index));
    } else if (NULL == simple_table) {
      if (OB_CORE_SCHEMA_VERSION != mgr->get_schema_version()) {
        // this log is useless when observer restarts.
        LOG_INFO("table not exist", K(tenant_id), K(tenant_id), K(database_id),
                 K(session_id), K(table_name), K(is_index),
                 "schema_version", mgr->get_schema_version(),
                 "schema_mgr_tenant_id", mgr->get_tenant_id());
      }
    } else {
      if (TEMP_TABLE_TYPE == check_type
          && !is_inner_table(simple_table->get_table_id())
          && false == simple_table->is_tmp_table()) {
        // temporary table is not finded.
        LOG_TRACE("request for temporary table but non-temporary table returned", K(session_id_), K(session_id), K(check_type));
      } else {
        table_id = simple_table->get_table_id();
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_id(uint64_t tenant_id,
                                     const ObString &database_name,
                                     const ObString &table_name,
                                     const bool is_index,
                                     const CheckTableType check_type,  // check if temporary table is visable
                                     uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || database_name.empty()
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(table_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("get database_id failed", KR(ret), K(tenant_id), K(database_name));
    } else if (OB_INVALID_ID == database_id) {
      // do-nothing
    } else if (OB_FAIL(get_table_id(tenant_id, database_id, table_name, is_index,
                                    check_type, table_id))){
      LOG_WARN("get table id failed", KR(ret), K(tenant_id), K(database_id),
               K(table_name), K(is_index));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_foreign_key_id(const uint64_t tenant_id,
                                            const uint64_t database_id,
                                            const ObString &foreign_key_name,
                                            uint64_t &foreign_key_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  foreign_key_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || foreign_key_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_foreign_key_id(tenant_id, database_id, foreign_key_name, foreign_key_id))) {
    LOG_WARN("get foreign key id failed", KR(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_INVALID_ID == foreign_key_id) {
    LOG_INFO("foreign key not exist", K(tenant_id), K(database_id), K(foreign_key_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_foreign_key_info(const uint64_t tenant_id,
                                            const uint64_t database_id,
                                            const ObString &foreign_key_name,
                                            ObSimpleForeignKeyInfo &foreign_key_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  foreign_key_info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || foreign_key_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_foreign_key_info(tenant_id, database_id,
                                              foreign_key_name, foreign_key_info))) {
    LOG_WARN("get foreign key id failed", KR(ret), K(tenant_id), K(database_id),
              K(foreign_key_name));
  } else if (OB_INVALID_ID == foreign_key_info.foreign_key_id_) {
    LOG_INFO("foreign key not exist", K(tenant_id), K(database_id), K(foreign_key_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_constraint_id(const uint64_t tenant_id,
                                           const uint64_t database_id,
                                           const ObString &constraint_name,
                                           uint64_t &constraint_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  constraint_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id ||
             OB_INVALID_ID == database_id ||
             constraint_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_constraint_id(tenant_id, database_id, constraint_name, constraint_id))) {
    LOG_WARN("get constraint id failed", KR(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_INVALID_ID == constraint_id) {
    LOG_INFO("constraint not exist", K(tenant_id), K(database_id), K(constraint_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_constraint_info(const uint64_t tenant_id,
                                            const uint64_t database_id,
                                            const common::ObString &constraint_name,
                                            ObSimpleConstraintInfo &constraint_info) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  constraint_info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id ||
             OB_INVALID_ID == database_id ||
             constraint_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_constraint_info(tenant_id, database_id,
                                              constraint_name, constraint_info))) {
    LOG_WARN("get constraint info failed", KR(ret), K(tenant_id),
                K(database_id), K(constraint_name));
  } else if (OB_INVALID_ID == constraint_info.constraint_id_) {
    LOG_INFO("constraint not exist", K(tenant_id), K(database_id), K(constraint_name));
  }

  return ret;
}

// basic interface
int ObSchemaGetterGuard::get_tenant_info(uint64_t tenant_id,
                                         const ObTenantSchema *&tenant_schema)
{
  int ret = OB_SUCCESS;
  tenant_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(get_schema(TENANT_SCHEMA,
                                OB_SYS_TENANT_ID,
                                tenant_id,
                                tenant_schema))) {
    LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_tenant_info(uint64_t tenant_id,
                                         const ObSimpleTenantSchema *&tenant_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tenant_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_GTS_TENANT_ID == tenant_id) {
    tenant_schema = schema_service_->get_simple_gts_tenant();
  } else {
    ret = mgr->get_tenant_schema(tenant_id, tenant_schema);
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(
    const uint64_t tenant_id,
    const uint64_t user_id,
    const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  user_info = NULL;

  LOG_TRACE("begin to get user schema", K(user_id));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(user_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema(USER_SCHEMA,
                                tenant_id,
                                user_id,
                                user_info))) {
    LOG_WARN("get user schema failed", KR(ret), K(tenant_id), K(user_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_database_schema(const uint64_t tenant_id,
                                             const uint64_t database_id,
                                             const ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  LOG_TRACE("begin to get database schema", K(database_id));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(database_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema(DATABASE_SCHEMA,
                                tenant_id,
                                database_id,
                                database_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), K(database_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_database_schema(const uint64_t tenant_id,
                                             const uint64_t database_id,
                                             const ObSimpleDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  database_schema = NULL;

  LOG_TRACE("begin to get database schema", K(database_id));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(database_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->get_database_schema(tenant_id, database_id, database_schema);
  }

  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_schema(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    const ObTablegroupSchema *&tablegroup_schema)
{
  int ret = OB_SUCCESS;
  tablegroup_schema = NULL;

  LOG_TRACE("begin to get tablegroup schema", K(tablegroup_id));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tablegroup_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema(TABLEGROUP_SCHEMA,
                                tenant_id,
                                tablegroup_id,
                                tablegroup_schema))) {
    LOG_WARN("get tablegroup schema failed", KR(ret), K(tenant_id), K(tablegroup_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_schema(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    const ObSimpleTablegroupSchema *&tablegroup_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tablegroup_schema = NULL;

  LOG_TRACE("begin to get tablegroup schema", K(tablegroup_id));

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tablegroup_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get simple tablegroup", KR(ret), K(tenant_id), K(tablegroup_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(table_id), K(ret));
  } else if (is_cte_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema(TABLE_SCHEMA,
                                tenant_id,
                                table_id,
                                table_schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

// for compatible
int ObSchemaGetterGuard::get_tenant_info(const ObString &tenant_name,
                                         const ObTenantSchema *&tenant_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tenant_info = NULL;

  const ObSimpleTenantSchema *simple_tenant = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_name), KR(ret));
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret));
  } else if (0 == tenant_name.case_compare(OB_GTS_TENANT_NAME)) {
     if (OB_FAIL(get_tenant_info(OB_GTS_TENANT_ID, tenant_info))) {
       LOG_WARN("fail to get gts tenant schema", KR(ret));
     }
  } else if (OB_FAIL(mgr->get_tenant_schema(tenant_name, simple_tenant))) {
     LOG_WARN("get simple tenant failed", KR(ret), K(tenant_name));
  } else if (NULL == simple_tenant) {
    LOG_INFO("tenant not exist", K(tenant_name));
  } else if (OB_FAIL(get_schema(TENANT_SCHEMA,
                                OB_SYS_TENANT_ID,
                                simple_tenant->get_tenant_id(),
                                tenant_info,
                                simple_tenant->get_schema_version()))) {
    LOG_WARN("get tenant schema failed", KR(ret), KPC(simple_tenant));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), K(tenant_name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_user_info(const uint64_t tenant_id,
                                       const ObString &user_name,
                                       const ObString &host_name,
                                       const ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  user_info = NULL;

  const ObSimpleUserSchema *simple_user = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_user_schema(tenant_id,
                                          user_name,
                                          host_name,
                                          simple_user))) {
    LOG_WARN("get simple user failed", KR(ret), K(tenant_id), K(user_name));
  } else if (NULL == simple_user) {
    LOG_INFO("user not exist", K(tenant_id), K(user_name));
  } else if (OB_FAIL(get_schema(USER_SCHEMA,
                                simple_user->get_tenant_id(),
                                simple_user->get_user_id(),
                                user_info,
                                simple_user->get_schema_version()))) {
    LOG_WARN("get user schema failed", KR(ret), K(tenant_id), KPC(simple_user));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), K(tenant_id), K(user_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_info(const uint64_t tenant_id,
                                       const ObString &user_name,
                                       ObIArray<const ObUserInfo *> &users_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObSimpleUserSchema *, DEFAULT_SAME_USERNAME_COUNT> simple_users;
    if (OB_FAIL(mgr->get_user_schema(tenant_id, user_name, simple_users))) {
      LOG_WARN("get simple user failed", KR(ret), K(tenant_id), K(user_name));
    } else if (simple_users.empty()) {
      LOG_INFO("user not exist", K(tenant_id), K(user_name));
    } else {
      const ObUserInfo *user_info = NULL;
      for (int64_t i = 0; i < simple_users.count() && OB_SUCC(ret); ++i) {
        const ObSimpleUserSchema *&simple_user = simple_users.at(i);
        if (OB_FAIL(get_schema(USER_SCHEMA,
                               simple_user->get_tenant_id(),
                               simple_user->get_user_id(),
                               user_info,
                               simple_user->get_schema_version()))) {
          LOG_WARN("get user schema failed", K(tenant_id), KPC(simple_user), KR(ret));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", KR(ret), KP(user_info));
        } else if (OB_FAIL(users_info.push_back(user_info))) {
          LOG_WARN("failed to push back user_info", KPC(user_info), K(users_info), KR(ret));
        } else {
          user_info = NULL;
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_database_schema(uint64_t tenant_id,
                                             const ObString &database_name,
                                             const ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  database_schema = NULL;

  const ObSimpleDatabaseSchema *simple_database = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_database_schema(tenant_id,
                                               database_name,
                                               simple_database))) {
    LOG_WARN("get simple database failed", KR(ret), K(tenant_id), K(database_name));
  } else if (NULL == simple_database) {
    LOG_INFO("database not exist", K(tenant_id), K(database_name));
  } else if (OB_FAIL(get_schema(DATABASE_SCHEMA,
                                simple_database->get_tenant_id(),
                                simple_database->get_database_id(),
                                database_schema,
                                simple_database->get_schema_version()))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), KPC(simple_database));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(database_schema));
  }

  return ret;
}

int ObSchemaGetterGuard::get_simple_table_schema(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &table_name,
    const bool is_index,
    const ObSimpleTableSchemaV2 *&simple_table_schema,
    bool is_hidden/*false*/)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  simple_table_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(table_name), KR(ret));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id,
                                           database_id,
                                           session_id_,
                                           table_name,
                                           is_index,
                                           simple_table_schema,
                                           is_hidden))) {
    LOG_WARN("get simple table failed", KR(ret), K(tenant_id),
            K(tenant_id), K(database_id), K(table_name), K(is_index));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &table_name,
    const bool is_index,
    const ObTableSchema *&table_schema,
    bool is_hidden/*false*/)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *simple_table = NULL;
  table_schema = NULL;
  if (OB_FAIL(get_simple_table_schema(tenant_id,
                                      database_id,
                                      table_name,
                                      is_index,
                                      simple_table,
                                      is_hidden))) {
    LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id),
             K(database_id), K(table_name), K(is_index), K(is_hidden));
  } else if (NULL == simple_table) {
    LOG_INFO("table not exist", K(tenant_id),
             K(database_id), K(table_name), K(is_index));
  } else if (OB_FAIL(get_schema(TABLE_SCHEMA,
                                simple_table->get_tenant_id(),
                                simple_table->get_table_id(),
                                table_schema,
                                simple_table->get_schema_version()))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id),
             "table_id", simple_table->get_table_id(),
             "schema_version", simple_table->get_schema_version());
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), K(tenant_id),
             "table_id", simple_table->get_table_id());
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema(
    const uint64_t tenant_id,
    const ObString &database_name,
    const ObString &table_name,
    const bool is_index,
    const ObTableSchema *&table_schema,
    bool is_hidden/*false*/)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  table_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || database_name.empty()
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_name), K(table_name), KR(ret));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id)))  {
    LOG_WARN("get database id failed", KR(ret), K(tenant_id), K(database_name));
  } else if (OB_INVALID_ID == database_id) {
    // do-nothing
  } else {
    ret = get_table_schema(tenant_id, database_id, table_name, is_index, table_schema, is_hidden);
  }

  return ret;
}

int ObSchemaGetterGuard::get_index_schemas_with_data_table_id(
  const uint64_t tenant_id,
  const uint64_t data_table_id,
  ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas)
{
  int ret = OB_SUCCESS;
  index_schemas.reset();
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
            || OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, data_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(data_table_id));
  } else if (table_schema->is_table() || table_schema->is_tmp_table()) {
    if (OB_FAIL(mgr->get_aux_schemas(tenant_id, data_table_id, index_schemas, USER_INDEX))) {
      LOG_WARN("fail to get aux schemas", KR(ret), K(tenant_id), K(data_table_id));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_column_schema(
  const uint64_t tenant_id,
  const uint64_t table_id,
  const uint64_t column_id,
  const ObColumnSchemaV2 *&column_schema)
{
  int ret = OB_SUCCESS;
  column_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == table_id
             || OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(table_id), K(column_id));
  } else if (is_cte_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
    } else if (NULL == table_schema) {
      // do-nothing
    } else {
      column_schema = table_schema->get_column_schema(column_id);
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_column_schema(
  const uint64_t tenant_id,
  const uint64_t table_id,
  const ObString &column_name,
  const ObColumnSchemaV2 *&column_schema)
{
  int ret = OB_SUCCESS;
  column_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == table_id
             || column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(table_id), K(column_name));
  } else if (is_cte_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
    } else if (NULL == table_schema) {
      // do-nothing
    } else {
      column_schema = table_schema->get_column_schema(column_name);
    }
  }

  return ret;
}


// for readonly
int ObSchemaGetterGuard::verify_read_only(const uint64_t tenant_id,
                                          const ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;
  const ObStmtNeedPrivs::NeedPrivs &need_privs = stmt_need_privs.need_privs_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv &need_priv = need_privs.at(i);
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          //we do not check user priv level only check table and db
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(verify_db_read_only(tenant_id, need_priv))) {
            LOG_WARN("database is read only, can't not execute this statement", KR(ret));
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_FAIL(verify_db_read_only(tenant_id, need_priv))) {
            LOG_WARN("db is read only, can't not execute this statement", KR(ret));
          } else if (OB_FAIL(verify_table_read_only(tenant_id, need_priv))) {
            LOG_WARN("table is read only, can't not execute this statement", KR(ret));
          }
          break;
        }
        default:{
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown privilege level", K(need_priv), KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::verify_db_read_only(const uint64_t tenant_id,
                                             const ObNeedPriv &need_priv)
{
  int ret = OB_SUCCESS;
  const ObString &db_name = need_priv.db_;
  const ObPrivSet &priv_set = need_priv.priv_set_;
  const ObDatabaseSchema *db_schema =  NULL;
  const ObPrivSet &read_only_privs = OB_PRIV_SELECT | OB_PRIV_SHOW_VIEW | OB_PRIV_SHOW_DB |
                                     OB_PRIV_READ;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_database_schema(tenant_id, db_name, db_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), K(db_name));
  } else if (NULL != db_schema) {
    if (db_schema->is_read_only() && OB_PRIV_HAS_OTHER(priv_set, read_only_privs)) {
      ret = OB_ERR_DB_READ_ONLY;
      LOG_USER_ERROR(OB_ERR_DB_READ_ONLY, db_name.length(), db_name.ptr());
      LOG_WARN("database is read only, can't not execute this statment",
               K(need_priv), K(tenant_id), KR(ret));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::verify_table_read_only(const uint64_t tenant_id,
                                                const ObNeedPriv &need_priv)
{
  int ret = OB_SUCCESS;
  const ObString &db_name = need_priv.db_;
  const ObString &table_name = need_priv.table_;
  const ObPrivSet &priv_set = need_priv.priv_set_;
  const ObTableSchema *table_schema = NULL;
  const ObPrivSet &read_only_privs = OB_PRIV_SELECT | OB_PRIV_SHOW_VIEW | OB_PRIV_SHOW_DB |
                                     OB_PRIV_READ;
  // FIXME: is it right?
  const bool is_index = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_table_schema(tenant_id, db_name, table_name, is_index, table_schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (NULL != table_schema) {
    if (table_schema->is_read_only() && OB_PRIV_HAS_OTHER(priv_set, read_only_privs)) {
      ret = OB_ERR_TABLE_READ_ONLY;
      LOG_USER_ERROR(OB_ERR_TABLE_READ_ONLY, db_name.length(), db_name.ptr(),
                     table_name.length(), table_name.ptr());
      LOG_WARN("table is read only, can't not execute this statment",
               K(need_priv), K(tenant_id), KR(ret));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::add_role_id_recursively(
  const uint64_t tenant_id,
  uint64_t role_id,
  ObSessionPrivInfo &s_priv)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *role_info = NULL;

  if (!has_exist_in_array(s_priv.enable_role_id_array_, role_id)) {
    /* 1. put itself */
    OZ (s_priv.enable_role_id_array_.push_back(role_id));
    /* 2. get role recursively */
    OZ (get_user_info(tenant_id, role_id, role_info));
    if (OB_SUCC(ret) && role_info != NULL) {
      const ObSEArray<uint64_t, 8> &role_id_array = role_info->get_role_id_array();
      for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
        OZ (add_role_id_recursively(tenant_id, role_info->get_role_id_array().at(i), s_priv));
      }
    }
  }
  return ret;
}

// for privilege
int ObSchemaGetterGuard::check_user_access(
    const ObUserLoginInfo &login_info,
    ObSessionPrivInfo &s_priv,
    SSL *ssl_st,
    const ObUserInfo *&sel_user_info)
{
  int ret = OB_SUCCESS;
  sel_user_info = NULL;
  if (OB_FAIL(get_tenant_id(login_info.tenant_name_, s_priv.tenant_id_))) {
    LOG_WARN("Invalid tenant", "tenant_name", login_info.tenant_name_, KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(s_priv.tenant_id_))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(s_priv), K_(tenant_id));
  } else {
    const int64_t DEFAULT_SAME_USERNAME_COUNT = 4;
    ObSEArray<const ObUserInfo *, DEFAULT_SAME_USERNAME_COUNT> users_info;
    if (OB_FAIL(get_user_info(s_priv.tenant_id_, login_info.user_name_, users_info))) {
      LOG_WARN("get user info failed", KR(ret), K(s_priv.tenant_id_), K(login_info));
    } else if (users_info.empty()) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("No tenant user", K(login_info), KR(ret));
    } else {
      bool is_found = false;
      const ObUserInfo *user_info = NULL;
      const ObUserInfo *matched_user_info = NULL;
      for (int64_t i = 0; i < users_info.count() && OB_SUCC(ret) && !is_found; ++i) {
        user_info = users_info.at(i);
        if (NULL == user_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", K(login_info), KR(ret));
        } else if (user_info->is_role()) {
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
              "user_name", login_info.user_name_,
              "client_ip_", login_info.client_ip_, KR(ret));
        } else if (!ObHostnameStuct::is_wild_match(login_info.client_ip_, user_info->get_host_name_str())
            && !ObHostnameStuct::is_ip_match(login_info.client_ip_, user_info->get_host_name_str())) {
          LOG_TRACE("account not matched, try next", KPC(user_info), K(login_info));
        } else {
          matched_user_info = user_info;
          if (0 == login_info.passwd_.length() && 0 == user_info->get_passwd_str().length()) {
            //passed
            is_found = true;
          } else if (0 == login_info.passwd_.length() || 0 == user_info->get_passwd_str().length()) {
            ret = OB_PASSWORD_WRONG;
            LOG_WARN("password error", KR(ret), K(login_info.passwd_.length()),
                     K(user_info->get_passwd_str().length()));
          } else {
            char stored_stage2_hex[SCRAMBLE_LENGTH] = {0};
            ObString stored_stage2_trimed;
            ObString stored_stage2_hex_str;
            if (user_info->get_passwd_str().length() < SCRAMBLE_LENGTH *2 + 1) {
              ret = OB_NOT_IMPLEMENT;
              LOG_WARN("Currently hash method other than MySQL 4.1 hash is not implemented.",
                       "hash str length", user_info->get_passwd_str().length());
            } else {
              //trim the leading '*'
              stored_stage2_trimed.assign_ptr(user_info->get_passwd_str().ptr() + 1,
                                              user_info->get_passwd_str().length() - 1);
              stored_stage2_hex_str.assign_buffer(stored_stage2_hex, SCRAMBLE_LENGTH);
              stored_stage2_hex_str.set_length(SCRAMBLE_LENGTH);
              //first, we restore the stored, displayable stage2 hash to its hex form
              ObEncryptedHelper::displayable_to_hex(stored_stage2_trimed, stored_stage2_hex_str);
              //then, we call the mysql validation logic.
              if (OB_FAIL(ObEncryptedHelper::check_login(login_info.passwd_,
                                                         login_info.scramble_str_,
                                                         stored_stage2_hex_str,
                                                         is_found))) {
                LOG_WARN("Failed to check login", K(login_info), KR(ret));
              } else if (!is_found) {
                LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
                         "user_name", login_info.user_name_,
                         "client_ip", login_info.client_ip_,
                         "host_name", user_info->get_host_name_str());
              } else {
                //found it
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (matched_user_info != NULL
            && matched_user_info->get_is_locked()
            && !sql::ObOraSysChecker::is_super_user(matched_user_info->get_user_id())) {
          if (is_found) {
            s_priv.user_id_ = matched_user_info->get_user_id();
          }
          ret = OB_ERR_USER_IS_LOCKED;
          LOG_WARN("User is locked", KR(ret));
        } else if (!is_found) {
          user_info = NULL;
          ret = OB_PASSWORD_WRONG;
          LOG_INFO("password error", "tenant_name", login_info.tenant_name_,
                   "user_name", login_info.user_name_,
                   "client_ip_", login_info.client_ip_, KR(ret));
        } else if (OB_FAIL(check_ssl_access(*user_info, ssl_st))) {
          LOG_WARN("check_ssl_access failed", "tenant_name", login_info.tenant_name_,
                   "user_name", login_info.user_name_,
                   "client_ip_", login_info.client_ip_, KR(ret));
        } else if (OB_FAIL(check_ssl_invited_cn(user_info->get_tenant_id(), ssl_st))) {
          LOG_WARN("check_ssl_invited_cn failed", "tenant_name", login_info.tenant_name_,
                   "user_name", login_info.user_name_,
                   "client_ip_", login_info.client_ip_, KR(ret));
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
          const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
          CK (user_info->get_role_id_array().count() ==
              user_info->get_role_id_option_array().count());
          for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
            const ObUserInfo *role_info = NULL;
            if (OB_FAIL(get_user_info(s_priv.tenant_id_, role_id_array.at(i), role_info))) {
              LOG_WARN("failed to get role ids", KR(ret), K(role_id_array.at(i)));
            } else if (NULL == role_info) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
            } else {
              s_priv.user_priv_set_ |= role_info->get_priv_set();
              if (user_info->get_disable_option(
                             user_info->get_role_id_option_array().at(i)) == 0) {
                OZ (add_role_id_recursively(user_info->get_tenant_id(),
                                            role_id_array.at(i),
                                            s_priv));
              }
            }
          }
          OZ (add_role_id_recursively(user_info->get_tenant_id(),
                                      OB_ORA_PUBLIC_ROLE_ID,
                                      s_priv));
        }

        //check db access and db existence
        if (!login_info.db_.empty()
            && OB_FAIL(check_db_access(s_priv, login_info.db_, s_priv.db_priv_set_))) {
          LOG_WARN("Database access deined", K(login_info), KR(ret));
        } else { }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_ssl_access(const ObUserInfo &user_info, SSL *ssl_st)
{
  int ret = OB_SUCCESS;
  switch (user_info.get_ssl_type()) {
    case ObSSLType::SSL_TYPE_NOT_SPECIFIED:
    case ObSSLType::SSL_TYPE_NONE: {
      //do nothing
      break;
    }
    case ObSSLType::SSL_TYPE_ANY: {
      if (NULL == ssl_st) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("not use ssl", KR(ret));
      }
      break;
    }
    case ObSSLType::SSL_TYPE_X509: {
      X509 *cert = NULL;
      int64_t verify_result = 0;
      if (NULL == ssl_st
          || (X509_V_OK != (verify_result = SSL_get_verify_result(ssl_st)))
          || (NULL == (cert = SSL_get_peer_certificate(ssl_st)))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KP(ssl_st), K(verify_result), KR(ret));
      }
      X509_free(cert);
      break;
    }
    case ObSSLType::SSL_TYPE_SPECIFIED: {
      X509 *cert = NULL;
      int64_t verify_result = 0;
      char *x509_issuer = NULL;
      char *x509_subject = NULL;
      if (NULL == ssl_st
          || (X509_V_OK != (verify_result = SSL_get_verify_result(ssl_st)))
          || (NULL == (cert = SSL_get_peer_certificate(ssl_st)))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KP(ssl_st), K(verify_result), KR(ret));
      }


      if (OB_SUCC(ret)
          && !user_info.get_ssl_cipher_str().empty()
          && user_info.get_ssl_cipher_str().compare(SSL_get_cipher(ssl_st)) != 0) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 cipher check failed", "expect", user_info.get_ssl_cipher_str(),
                 "receive", SSL_get_cipher(ssl_st), KR(ret));
      }

      if (OB_SUCC(ret) && !user_info.get_x509_issuer_str().empty()) {
        x509_issuer = X509_NAME_oneline(X509_get_issuer_name(cert), 0, 0);
        if (user_info.get_x509_issuer_str().compare(x509_issuer) != 0) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("x509 issue check failed", "expect", user_info.get_x509_issuer_str(),
                   "receive", x509_issuer, KR(ret));
        }
      }

      if (OB_SUCC(ret) && !user_info.get_x509_subject_str().empty()) {
        x509_subject = X509_NAME_oneline(X509_get_subject_name(cert), 0, 0);
        if (user_info.get_x509_subject_str().compare(x509_subject) != 0) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("x509 subject check failed", "expect", user_info.get_x509_subject_str(),
                   "receive", x509_subject, KR(ret));
        }
      }

      OPENSSL_free(x509_issuer);
      OPENSSL_free(x509_subject);
      X509_free(cert);
      break;
    }
    default: {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("unknonw type", K(user_info), KR(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_TRACE("fail to check_ssl_access", K(user_info), KR(ret));
  }
  return ret;
}


int ObSchemaGetterGuard::check_ssl_invited_cn(const uint64_t tenant_id, SSL *ssl_st)
{
  int ret = OB_SUCCESS;
  if (NULL == ssl_st) {
    LOG_TRACE("not use ssl, no need check invited_cn", K(tenant_id));
  } else {
    X509 *cert = NULL;
    X509_name_st *x509Name = NULL;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get tenant_config", KR(ret));
    } else {
      ObString ob_ssl_invited_common_names(tenant_config->ob_ssl_invited_common_names.str());
      if (ob_ssl_invited_common_names.empty()) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("ob_ssl_invited_common_names not match", "expect", ob_ssl_invited_common_names, KR(ret));
      } else if (NULL == (cert = SSL_get_peer_certificate(ssl_st))) {
        LOG_TRACE("use ssl, but without peer_certificate", K(tenant_id));
      } else if (OB_ISNULL(x509Name = X509_get_subject_name(cert))) {
        ret = OB_PASSWORD_WRONG;
        LOG_WARN("X509 check failed", KR(ret));
      } else {
        unsigned int count = X509_NAME_entry_count(x509Name);
        char name[1024] = {0};
        char *cn_used = NULL;
        for (unsigned int i = 0; i < count && NULL == cn_used; i++) {
          X509_NAME_ENTRY *entry = X509_NAME_get_entry(x509Name, i);
          OBJ_obj2txt(name, sizeof(name), X509_NAME_ENTRY_get_object(entry), 0);
          if (strcmp(name, "commonName") == 0) {
            ASN1_STRING_to_UTF8((unsigned char **)&cn_used, X509_NAME_ENTRY_get_data(entry));
          }
        }
        if (OB_ISNULL(cn_used)) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("failed to found cn", KR(ret));
        } else if (NULL == strstr(ob_ssl_invited_common_names.ptr(), cn_used)) {
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("ob_ssl_invited_common_names not match", "expect",ob_ssl_invited_common_names, "curr", cn_used,  KR(ret));
        } else {
          LOG_TRACE("ob_ssl_invited_common_names match", "expect",ob_ssl_invited_common_names, "curr", cn_used,  KR(ret));
        }
      }
    }

    if (cert != NULL) {
      X509_free(cert);
    }
  }
  return ret;
}


int ObSchemaGetterGuard::check_db_access(ObSessionPrivInfo &s_priv,
                                         const ObString& database_name)
{
  int ret = OB_SUCCESS;

  uint64_t database_id = OB_INVALID_ID;
  uint64_t tenant_id = s_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(s_priv), K_(tenant_id));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, database_id))) {
    OB_LOG(WARN, "fail to get database id", KR(ret), K(tenant_id), K(database_name));
  } else if (OB_INVALID_ID != database_id) {
    if (OB_FAIL(check_db_access(s_priv, database_name, s_priv.db_priv_set_))) {
      OB_LOG(WARN, "fail to check db access", K(database_name), KR(ret));
    }
  } else {
    ret = OB_ERR_BAD_DATABASE;
    OB_LOG(WARN, "database not exist", KR(ret), K(database_name), K(s_priv));
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
  }
  return ret;
}

/* check privilege of create session in oracle mode */
int ObSchemaGetterGuard::check_ora_conn_access(
    const uint64_t tenant_id,
    const uint64_t user_id,
    bool print_warn,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  UNUSED(print_warn);
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K_(tenant_id));
  } else {
    OZ (sql::ObOraSysChecker::check_ora_connect_priv(*this, tenant_id, user_id, role_id_array),
        tenant_id, user_id);
  }
  return ret;
}

int ObSchemaGetterGuard::get_session_priv_info(const uint64_t tenant_id,
                                               const uint64_t user_id,
                                               const ObString &database_name,
                                               ObSessionPrivInfo &session_priv)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;
  if (OB_FAIL(get_user_info(tenant_id,
                            user_id,
                            user_info))) {
    LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(user_id));
  } else if (NULL == user_info) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user info is null", KR(ret), K(user_id));
  } else {
    const ObSchemaMgr *mgr = NULL;
    ObOriginalDBKey db_priv_key(tenant_id,
                                user_info->get_user_id(),
                                database_name);
    ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->priv_mgr_.get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key));
    } else {
      session_priv.tenant_id_ = tenant_id;
      session_priv.user_id_ = user_info->get_user_id();
      session_priv.user_name_ = user_info->get_user_name_str();
      session_priv.host_name_ = user_info->get_host_name_str();
      session_priv.db_ = database_name;
      session_priv.user_priv_set_ = user_info->get_priv_set();
      session_priv.db_priv_set_ = db_priv_set;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_access(
    const ObSessionPrivInfo &session_priv,
    const ObString &db,
    ObPrivSet &db_priv_set,
    bool print_warn)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  if (!session_priv.is_valid() || 0 == db.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(session_priv), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    ObOriginalDBKey db_priv_key(session_priv.tenant_id_,
                                session_priv.user_id_,
                                db);
    db_priv_set = OB_PRIV_SET_EMPTY;
    if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key));
    } else {
      bool is_grant = false;
      if (OB_FAIL(priv_mgr.table_grant_in_db(db_priv_key.tenant_id_,
                                            db_priv_key.user_id_,
                                            db_priv_key.db_,
                                            is_grant))) {
        LOG_WARN("check table grant in db failed", K(db_priv_key), KR(ret));
      } else {
        // load db level prvilege from roles
        const ObUserInfo *user_info = NULL;
        if (OB_FAIL(get_user_info(tenant_id, session_priv.user_id_, user_info))) {
          LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(session_priv.user_id_));
        } else if (NULL == user_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", KR(ret), K(session_priv.user_id_));
        } else {
          const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
          bool is_grant_role = false;
          ObPrivSet total_db_priv_set_role = OB_PRIV_SET_EMPTY;
          for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
            const ObUserInfo *role_info = NULL;
            if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
              LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
            } else if (NULL == role_info) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
            } else {
              ObPrivSet db_priv_set_role = OB_PRIV_SET_EMPTY;
              ObOriginalDBKey db_priv_key_role(session_priv.tenant_id_,
                  role_info->get_user_id(),
                  db);
              if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key_role, db_priv_set_role))) {
                LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key_role));
              } else if (!is_grant_role && OB_FAIL(priv_mgr.table_grant_in_db(db_priv_key_role.tenant_id_,
                        db_priv_key_role.user_id_,
                        db_priv_key_role.db_,
                        is_grant_role))) {
                LOG_WARN("check table grant in db failed", K(db_priv_key_role), KR(ret));
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
      lib::Worker::CompatMode compat_mode;
      OZ (get_tenant_compat_mode(session_priv.tenant_id_, compat_mode));
      if (OB_SUCC(ret) && compat_mode == lib::Worker::CompatMode::ORACLE) {
        /* For compatibility_mode, check if user has been granted all privileges first */
        if (((session_priv.user_priv_set_ | db_priv_set) & OB_PRIV_DB_ACC)
            || is_grant) {
        } else {
          OZ (check_ora_conn_access(session_priv.tenant_id_,
              session_priv.user_id_, print_warn, session_priv.enable_role_id_array_),
            session_priv.tenant_id_, session_priv.user_id_);
        }
      } else {
        if (OB_FAIL(ret)) {
        } else if (((session_priv.user_priv_set_ | db_priv_set) & OB_PRIV_DB_ACC)
            || is_grant) {
        } else {
          ret = OB_ERR_NO_DB_PRIVILEGE;
          if (print_warn) {
            LOG_WARN("No privilege to access database", K(session_priv), K(db), KR(ret));
            LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                          session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                          db.length(), db.ptr());
          }
        }
      }
    }
  }
  return ret;
}

// TODO: check arguments
int ObSchemaGetterGuard::check_db_show(const ObSessionPrivInfo &session_priv,
                                       const ObString &db,
                                       bool &allow_show)
{
  int ret = OB_SUCCESS;
  int can_show = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  allow_show = true;
  ObPrivSet db_priv_set = 0;
  ObPrivSet need_priv = OB_PRIV_SHOW_DB;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (!session_priv.is_valid() || 0 == db.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(session_priv), KR(ret));
  } else if (OB_TEST_PRIVS(session_priv.user_priv_set_, need_priv)) {
    /* user priv level has show_db */
  } else if (OB_SUCCESS != (can_show = check_db_access(session_priv, db, db_priv_set, false))) {
    allow_show = false;
  }
  return ret;
}

int ObSchemaGetterGuard::check_table_show(const ObSessionPrivInfo &session_priv,
                                                  const ObString &db,
                                                  const ObString &table,
                                                  bool &allow_show)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  allow_show = true;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, OB_PRIV_DB_ACC)) {
    //allow
  } else {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;

    const ObTablePriv *table_priv = NULL;
    ObPrivSet db_priv_set = 0;
    //get db_priv_set
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      //check db_priv_set, then check table_priv_set
      if (OB_PRIV_HAS_ANY(db_priv_set, OB_PRIV_DB_ACC)) {
        //allow
      } else {
        ObTablePrivSortKey table_priv_key(session_priv.tenant_id_, session_priv.user_id_, db, table);
        if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
          LOG_WARN("get table priv failed", K(table_priv_key), KR(ret));
        } else if (NULL != table_priv
                   && OB_PRIV_HAS_ANY(table_priv->get_priv_set(), OB_PRIV_TABLE_ACC)) {
          // allow
        } else {
          allow_show = false;
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_user_priv(const ObSessionPrivInfo &session_priv,
                                         const ObPrivSet priv_set)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  ObPrivSet user_priv_set = session_priv.user_priv_set_;

  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (!OB_TEST_PRIVS(user_priv_set, priv_set)) {
    if ((priv_set == OB_PRIV_ALTER_TENANT
        || priv_set == OB_PRIV_ALTER_SYSTEM
        || priv_set == OB_PRIV_CREATE_RESOURCE_POOL
        || priv_set == OB_PRIV_CREATE_RESOURCE_UNIT)
        && (OB_TEST_PRIVS(user_priv_set, OB_PRIV_SUPER))) {
    } else {
      ret = OB_ERR_NO_PRIVILEGE;
    }
  }
  if (OB_ERR_NO_PRIVILEGE == ret) {
    ObPrivSet lack_priv_set = priv_set &(~user_priv_set);
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    const char *priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
    if (OB_ISNULL(priv_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid priv type", "priv_set", lack_priv_set);
    } else {
      if (priv_set == OB_PRIV_ALTER_TENANT
          || priv_set == OB_PRIV_ALTER_SYSTEM
          || priv_set == OB_PRIV_CREATE_RESOURCE_POOL
          || priv_set == OB_PRIV_CREATE_RESOURCE_UNIT) {
        ObSqlString priv_name_with_prefix;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(priv_name_with_prefix.assign_fmt("SUPER or %s", priv_name))) {
          LOG_WARN("fail to assign fmt", KR(tmp_ret));
          LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name);
        } else {
          LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name_with_prefix.ptr());
        }
      } else {
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name);
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_single_table_priv_or(const ObSessionPrivInfo &session_priv,
                                                    const ObNeedPriv &table_need_priv)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_priv.tenant_id_;
  const uint64_t user_id = session_priv.user_id_;
  const ObSchemaMgr *mgr = NULL;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", tenant_id, "user_id", user_id, KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, table_need_priv.priv_set_)) {
    /* check success */
  } else {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    bool pass = false;
    if (OB_FAIL(check_priv_db_or_(session_priv, table_need_priv, priv_mgr, tenant_id, user_id, pass))) {
      LOG_WARN("failed to check priv db or", K(ret));
    } else if (pass) {
      /* check success */
    } else if (OB_FAIL(check_priv_table_or_(table_need_priv, priv_mgr, tenant_id, user_id, pass))) {
      LOG_WARN("fail to check priv table or", K(ret));
    } else if (pass) {
      /* check success */
    } else {
      ret = OB_ERR_NO_TABLE_PRIVILEGE;
      const char *priv_name = "ANY";
      LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                     session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                     session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                     table_need_priv.table_.length(), table_need_priv.table_.ptr());
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_single_table_priv(const ObSessionPrivInfo &session_priv,
                                                 const ObNeedPriv &table_need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_,
        "user_id", session_priv.user_id_,
        KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    //first:check user and db priv.
    //second:If user_db_priv_set has no enough privileges, check table priv.
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    if (!OB_TEST_PRIVS(session_priv.user_priv_set_, table_need_priv.priv_set_)) {
      ObPrivSet user_db_priv_set = 0;
      if (OB_SUCCESS != check_db_priv(session_priv, table_need_priv.db_,
          table_need_priv.priv_set_, user_db_priv_set)) {
        //1. fetch table priv
        const ObTablePriv *table_priv = NULL;
        ObPrivSet table_priv_set = 0;
        bool is_table_priv_empty = true;
        ObTablePrivSortKey table_priv_key(session_priv.tenant_id_,
                                          session_priv.user_id_,
                                          table_need_priv.db_,
                                          table_need_priv.table_);
        if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
          LOG_WARN("get table priv failed", KR(ret), K(table_priv_key) );
        } else if (NULL != table_priv) {
          table_priv_set = table_priv->get_priv_set();
          is_table_priv_empty = false;
        }

        if (OB_SUCC(ret)) {
          //2. fetch roles privs
          const ObUserInfo *user_info = NULL;
          if (OB_FAIL(get_user_info(tenant_id, session_priv.user_id_, user_info))) {
            LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(session_priv.user_id_));
          } else if (NULL == user_info) {
            ret = OB_USER_NOT_EXIST;
            LOG_WARN("user info is null", KR(ret), K(session_priv.user_id_));
          } else {
            const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
            for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
              const ObUserInfo *role_info = NULL;
              const ObTablePriv *role_table_priv = NULL;
              if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
                LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
              } else if (NULL == role_info) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
              } else {
                ObTablePrivSortKey role_table_priv_key(session_priv.tenant_id_,
                    role_info->get_user_id(),
                    table_need_priv.db_,
                    table_need_priv.table_);
                if (OB_FAIL(priv_mgr.get_table_priv(role_table_priv_key, role_table_priv))) {
                  LOG_WARN("get table priv failed", KR(ret), K(role_table_priv_key) );
                } else if (NULL != role_table_priv) {
                  is_table_priv_empty = false;
                  // append additional role
                  table_priv_set |= role_table_priv->get_priv_set();
                }
              }
            }
          }
        }

        //3. check privs
        if (OB_SUCC(ret)) {
          if (is_table_priv_empty) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_WARN("No privilege, cannot find table priv info",
                     "tenant_id", session_priv.tenant_id_,
                     "user_id", session_priv.user_id_, K(table_need_priv));
          } else if (!OB_TEST_PRIVS(table_priv_set | user_db_priv_set, table_need_priv.priv_set_)) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                K(table_need_priv),
                K(table_priv_set | user_db_priv_set));
          }
        }
        if (OB_ERR_NO_TABLE_PRIVILEGE == ret) {
          ObPrivSet lack_priv_set = table_need_priv.priv_set_ & (~(table_priv_set | user_db_priv_set));
          const char *priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
          if (OB_ISNULL(priv_name)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid priv type", "priv_set", table_need_priv.priv_set_);
          } else {
            LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                           session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                           session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                           table_need_priv.table_.length(), table_need_priv.table_.ptr());
          }
        }
      }
    }
    if (OB_SUCC(ret) && table_need_priv.is_for_update_) {
      if (OB_FAIL(check_single_table_priv_for_update_(session_priv, table_need_priv, priv_mgr))) {
        LOG_WARN("failed to check select table for update priv", K(ret));
      }
    }
  }
  return ret;
}

/* select ... from table for update, need select privilege and one of (delete, update lock tables).
 * ob donesn't have lock tables yet, then it checks select first and one of (delete、update on table level).
 */
int ObSchemaGetterGuard::check_single_table_priv_for_update_(const ObSessionPrivInfo &session_priv,
                                                             const ObNeedPriv &table_need_priv,
                                                             const ObPrivMgr &priv_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_priv.tenant_id_;
  const uint64_t user_id = session_priv.user_id_;
  bool pass = false;
  const ObNeedPriv need_priv(table_need_priv.db_, table_need_priv.table_, table_need_priv.priv_level_,
                             OB_PRIV_UPDATE | OB_PRIV_DELETE, table_need_priv.is_sys_table_,
                             table_need_priv.is_for_update_);
  if (OB_UNLIKELY(!table_need_priv.is_for_update_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not run this function without for update", K(ret), K(table_need_priv));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, need_priv.priv_set_)) {
    /* check ok */
  } else if (OB_FAIL(check_priv_db_or_(session_priv, need_priv, priv_mgr, tenant_id, user_id, pass))) {
    LOG_WARN("failed to check priv db or", K(ret));
  } else if (!pass && OB_FAIL(check_priv_table_or_(need_priv, priv_mgr, tenant_id, user_id, pass))) {
    LOG_WARN("fail to check priv table or", K(ret));
  } else if (!pass) {
    ret = OB_ERR_NO_TABLE_PRIVILEGE;
    const char *priv_name = "SELECT with locking clause";
    LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                                    session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                                    session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                                    table_need_priv.table_.length(), table_need_priv.table_.ptr());
  } else { /* check ok */ }
  return ret;
}

int ObSchemaGetterGuard::check_db_priv(const ObSessionPrivInfo &session_priv,
                              const ObString &db,
                              const ObPrivSet need_priv,
                              ObPrivSet &user_db_priv_set)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  ObPrivSet total_db_priv_set_role = OB_PRIV_SET_EMPTY;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_,
                                  "user_id", session_priv.user_id_,
                                  KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObPrivSet db_priv_set = 0;
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
      }
    }
    /* load role db privs */
    if (OB_SUCC(ret)) {
      const ObUserInfo *user_info = NULL;
      //bool is_grant_role = false;
      OZ (get_user_info(tenant_id, session_priv.user_id_, user_info), session_priv.user_id_);
      if (OB_SUCC(ret)) {
        if (NULL == user_info) {
          ret = OB_USER_NOT_EXIST;
          LOG_WARN("user info is null", KR(ret), K(session_priv.user_id_));
        }
      }
      if (OB_SUCC(ret)) {
        const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
        for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
          const ObUserInfo *role_info = NULL;
          if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
            LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
          } else if (NULL == role_info) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
          } else {
            ObPrivSet db_priv_set_role = OB_PRIV_SET_EMPTY;
            ObOriginalDBKey db_priv_key_role(session_priv.tenant_id_,
                                            role_info->get_user_id(),
                                            db);
            if (OB_FAIL(get_db_priv_set(db_priv_key_role, db_priv_set_role))) {
              LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key_role));
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

int ObSchemaGetterGuard::check_db_priv(const ObSessionPrivInfo &session_priv,
                              const common::ObString &db,
                              const ObPrivSet need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (!OB_TEST_PRIVS(session_priv.user_priv_set_, need_priv)) {
    ObPrivSet user_db_priv_set = 0;
    if (OB_FAIL(check_db_priv(session_priv, db, need_priv, user_db_priv_set))) {
      LOG_WARN("No db priv", "tenant_id", session_priv.tenant_id_,
                              "user_id", session_priv.user_id_,
                              K(db), KR(ret));
      if (OB_ERR_NO_DB_PRIVILEGE == ret) {
        LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                       session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                       db.length(), db.ptr());
      }
    }
  }
  return ret;
}

/* check all needed privileges of object*/
int ObSchemaGetterGuard::check_single_obj_priv(
    const uint64_t tenant_id,
    const uint64_t uid,
    const ObOraNeedPriv &need_priv,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  uint64_t uid_to_be_check;
  bool exists = false;
  for (int i = OBJ_PRIV_ID_NONE; OB_SUCC(ret) && i < OBJ_PRIV_ID_MAX; i++) {
    OZ (share::ObOraPrivCheck::raw_obj_priv_exists(i, need_priv.obj_privs_, exists));
    if (OB_SUCC(ret) && exists) {
      uid_to_be_check = need_priv.grantee_id_ == common::OB_INVALID_ID ? uid :
                        need_priv.grantee_id_;
      // If column privilege needs to be checked
      if (OBJ_LEVEL_FOR_COL_PRIV == need_priv.obj_level_) {
        // 1. Check sys and table privileges first
        OZX2 (sql::ObOraSysChecker::check_ora_obj_priv(*this,
                                                    tenant_id,
                                                    uid_to_be_check,
                                                    need_priv.db_name_,
                                                    need_priv.obj_id_,
                                                    OBJ_LEVEL_FOR_TAB_PRIV,
                                                    need_priv.obj_type_,
                                                    i,
                                                    need_priv.check_flag_,
                                                    need_priv.owner_id_,
                                                    role_id_array),
            OB_ERR_NO_SYS_PRIVILEGE, OB_TABLE_NOT_EXIST,
            tenant_id, uid_to_be_check, need_priv.db_name_, need_priv.obj_id_,
            OBJ_LEVEL_FOR_TAB_PRIV, need_priv.obj_type_, i, need_priv.check_flag_,
            need_priv.owner_id_, need_priv.grantee_id_);
        // 2. Check column privileges
        if (OB_ERR_NO_SYS_PRIVILEGE == ret || OB_TABLE_NOT_EXIST == ret) {
          bool table_accessible = (OB_ERR_NO_SYS_PRIVILEGE == ret); // check if table is visable to user
          bool column_priv_check_pass = true; // check all columns's privileges
          ret = OB_SUCCESS;
          for (int idx = 0; OB_SUCC(ret) && idx < need_priv.col_id_array_.count(); ++idx) {
            OZX2 (sql::ObOraSysChecker::check_ora_obj_priv(*this,
                                                        tenant_id,
                                                        uid_to_be_check,
                                                        need_priv.db_name_,
                                                        need_priv.obj_id_,
                                                        need_priv.col_id_array_.at(idx),
                                                        need_priv.obj_type_,
                                                        i,
                                                        need_priv.check_flag_,
                                                        need_priv.owner_id_,
                                                        role_id_array),
                OB_TABLE_NOT_EXIST, OB_ERR_NO_SYS_PRIVILEGE,
                tenant_id, uid_to_be_check, need_priv.db_name_, need_priv.obj_id_,
                need_priv.col_id_array_.at(idx), need_priv.obj_type_, i, need_priv.check_flag_,
                need_priv.owner_id_, need_priv.grantee_id_);
            if (OB_TABLE_NOT_EXIST == ret) { // overwrite ret
              column_priv_check_pass = false;
              ret = OB_SUCCESS;
            } else if (OB_ERR_NO_SYS_PRIVILEGE == ret) {
              column_priv_check_pass = false;
              table_accessible = true;
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && false == column_priv_check_pass) { // overwrite ret
            if (table_accessible) {
              ret = OB_ERR_NO_SYS_PRIVILEGE;
            } else {
              ret = OB_TABLE_NOT_EXIST;
            }
          }
        }
      } else {
        // Other cases
        OZ (sql::ObOraSysChecker::check_ora_obj_priv(*this,
                                                    tenant_id,
                                                    uid_to_be_check,
                                                    need_priv.db_name_,
                                                    need_priv.obj_id_,
                                                    need_priv.obj_level_,
                                                    need_priv.obj_type_,
                                                    i,
                                                    need_priv.check_flag_,
                                                    need_priv.owner_id_,
                                                    role_id_array),
            tenant_id, uid_to_be_check, need_priv.db_name_, need_priv.obj_id_,
            need_priv.obj_level_, need_priv.obj_type_, i, need_priv.check_flag_,
            need_priv.owner_id_, need_priv.grantee_id_);
      }
    }
  }
  return ret;
}

/* check all privileges of stmt */
int ObSchemaGetterGuard::check_ora_priv(
    const uint64_t tenant_id,
    const uint64_t uid,
    const ObStmtOraNeedPrivs &stmt_need_privs,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  const ObStmtOraNeedPrivs::OraNeedPrivs &need_privs = stmt_need_privs.need_privs_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObOraNeedPriv &need_priv = need_privs.at(i);
      if (OB_FAIL(check_single_obj_priv(tenant_id, uid, need_priv, role_id_array))) {
        LOG_WARN("No privilege", "tenant_id", tenant_id,
            "user_id", uid,
            "need_priv", need_priv.obj_privs_,
            "obj id", need_priv.obj_id_,
            KR(ret));//need print priv
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_priv(const ObSessionPrivInfo &session_priv,
                                    const ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;
  const ObStmtNeedPrivs::NeedPrivs &need_privs = stmt_need_privs.need_privs_;
  uint64_t tenant_id = session_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (session_priv.is_valid()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv &need_priv = need_privs.at(i);
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          if (OB_FAIL(check_user_priv(session_priv, need_priv.priv_set_))) {
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                "need_priv", need_priv.priv_set_,
                "user_priv", session_priv.user_priv_set_,
                KR(ret));//need print priv
          }
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(check_db_priv(session_priv, need_priv.db_, need_priv.priv_set_))) {
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                "need_priv", need_priv.priv_set_,
                "user_priv", session_priv.user_priv_set_,
                KR(ret));//need print priv
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_PRIV_CHECK_ALL == need_priv.priv_check_type_) {
            if (OB_FAIL(check_single_table_priv(session_priv, need_priv))) {
              LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                  "user_id", session_priv.user_id_,
                  "need_priv", need_priv.priv_set_,
                  "table", need_priv.table_,
                  "db", need_priv.db_,
                  "user_priv", session_priv.user_priv_set_,
                  KR(ret));//need print priv
            }
          } else if (OB_PRIV_CHECK_ANY == need_priv.priv_check_type_) {
            if (OB_FAIL(check_single_table_priv_or(session_priv, need_priv))) {
              LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                       "user_id", session_priv.user_id_,
                       "need_priv", need_priv.priv_set_,
                       "table", need_priv.table_,
                       "db", need_priv.db_,
                       "user_priv", session_priv.user_priv_set_,
                       KR(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Privilege checking of other not use this function yet", KR(ret));
          }
          break;
        }
        case OB_PRIV_DB_ACCESS_LEVEL: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Privilege checking of database access should not use this function", KR(ret));
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

int ObSchemaGetterGuard::check_priv_db_or_(const ObSessionPrivInfo &session_priv,
                                           const ObNeedPriv &need_priv,
                                           const ObPrivMgr &priv_mgr,
                                           const uint64_t tenant_id,
                                           const uint64_t user_id,
                                           bool& pass) {
  int ret = OB_SUCCESS;
  int64_t total_db_priv_set_role = 0;
  ObString db = need_priv.db_;
  ObPrivSet db_priv_set = 0;
  if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
    db_priv_set = session_priv.db_priv_set_;
  } else {
    ObOriginalDBKey db_priv_key(tenant_id, user_id, db);
    if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
    }
  }

  /* load role db privs */
  if (OB_SUCC(ret)) {
    const ObUserInfo *user_info = NULL;
    //bool is_grant_role = false;
    if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", KR(ret), K(tenant_id), K(user_id));
    } else {
      const ObIArray<uint64_t> &role_id_array = user_info->get_role_id_array();
      for (int64_t i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
        const ObUserInfo *role_info = NULL;
        if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
          LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
        } else if (OB_ISNULL(role_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
        } else {
          ObPrivSet db_priv_set_role = OB_PRIV_SET_EMPTY;
          ObOriginalDBKey db_priv_key_role(tenant_id, role_info->get_user_id(), db);
          if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key_role, db_priv_set_role))) {
            LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key_role));
          } else {
            db_priv_set |= db_priv_set_role;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    pass = OB_PRIV_HAS_ANY(db_priv_set, need_priv.priv_set_);
  }

  return ret;
}

int ObSchemaGetterGuard::check_priv_table_or_(const ObNeedPriv &need_priv,
                                              const ObPrivMgr &priv_mgr,
                                              const uint64_t tenant_id,
                                              const uint64_t user_id,
                                              bool& pass) {
  int ret = OB_SUCCESS;
  //1. fetch table priv
  const ObTablePriv *table_priv = NULL;
  ObPrivSet table_priv_set = 0;
  ObTablePrivSortKey table_priv_key(tenant_id,
                                    user_id,
                                    need_priv.db_,
                                    need_priv.table_);
  if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
    LOG_WARN("get table priv failed", KR(ret), K(table_priv_key));
  } else if (NULL != table_priv) {
    table_priv_set = table_priv->get_priv_set();
  }

  if (OB_SUCC(ret)) {
    //2. fetch roles privs
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", KR(ret), K(tenant_id), K(user_id));
    } else {
      const ObIArray<uint64_t> &role_id_array = user_info->get_role_id_array();
      for (int64_t i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
        const ObUserInfo *role_info = NULL;
        const ObTablePriv *role_table_priv = NULL;
        if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
          LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
        } else if (OB_ISNULL(role_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
        } else {
          ObTablePrivSortKey role_table_priv_key(tenant_id,
                                                  role_info->get_user_id(),
                                                  need_priv.db_,
                                                  need_priv.table_);
          if (OB_FAIL(priv_mgr.get_table_priv(role_table_priv_key, role_table_priv))) {
            LOG_WARN("get table priv failed", KR(ret), K(role_table_priv_key) );
          } else if (OB_ISNULL(role_table_priv)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role table priv is null", KR(ret), K(role_table_priv_key));
          } else {
            table_priv_set |= role_table_priv->get_priv_set();
          }
        }
      }
    }
  }

  //3. check privs
  if (OB_SUCC(ret)) {
    pass = OB_PRIV_HAS_ANY(table_priv_set, need_priv.priv_set_);
  }

  return ret;
}

int ObSchemaGetterGuard::check_priv_or(const ObSessionPrivInfo &session_priv,
                                       const ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;

  const ObStmtNeedPrivs::NeedPrivs &need_privs = stmt_need_privs.need_privs_;
  bool pass = false;
  ObPrivLevel max_priv_level = OB_PRIV_INVALID_LEVEL;
  uint64_t tenant_id = session_priv.tenant_id_;
  uint64_t user_id = session_priv.user_id_;
  const ObSchemaMgr *mgr = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr is NULL", KR(ret), K(tenant_id));
  } else if (session_priv.is_valid()) {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    for (int64_t i = 0; !pass && OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv &need_priv = need_privs.at(i);
      if (need_priv.priv_level_ > max_priv_level) {
        max_priv_level = need_priv.priv_level_;
      }
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          pass = OB_PRIV_HAS_ANY(session_priv.user_priv_set_, need_priv.priv_set_);
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(check_priv_db_or_(session_priv, need_priv, priv_mgr, tenant_id, user_id, pass))) {
            LOG_WARN("fail to check priv db only", KR(ret), K(tenant_id), K(user_id), K(need_priv.db_));
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_FAIL(check_priv_table_or_(need_priv, priv_mgr, tenant_id, user_id, pass))) {
            LOG_WARN("fail to check priv table only", KR(ret), K(tenant_id), K(user_id), K(need_priv.db_), K(need_priv.table_));
          }
          break;
        }
        case OB_PRIV_DB_ACCESS_LEVEL: {
          //this should not occur
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should not reach here", KR(ret));
          break;
        }
        default: {
          break;
        }
      }
    }
    if (!pass) {
      //User log is printed outside
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
        //this should not occur
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      LOG_WARN("Or-ed privilege check not passed",
               "tenant id", session_priv.tenant_id_, "user id", session_priv.user_id_);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObSchemaGetterGuard::get_db_priv_set(const uint64_t tenant_id,
                                         const uint64_t user_id,
                                         const ObString &db,
                                         ObPrivSet &priv_set)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_priv_set(
                     ObOriginalDBKey(tenant_id, user_id, db), priv_set))) {
    LOG_WARN("fail to get db priv set", KR(ret), K(tenant_id), K(user_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_db_priv_set(const ObOriginalDBKey &db_priv_key, ObPrivSet &priv_set, bool is_pattern)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  uint64_t tenant_id = db_priv_key.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_priv_set(db_priv_key, priv_set, is_pattern))) {
    LOG_WARN("fail to get dv priv set", KR(ret), K(db_priv_key));
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_priv_set(const ObTablePrivSortKey &table_priv_key,
        ObPrivSet &priv_set)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  uint64_t tenant_id = table_priv_key.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_table_priv_set(table_priv_key, priv_set))) {
    LOG_WARN("fail to get table priv set", KR(ret), K(table_priv_key));
  }
  return ret;
}

int ObSchemaGetterGuard::get_obj_privs(
    const ObObjPrivSortKey &obj_priv_key,
    ObPackedObjPriv &obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  uint64_t tenant_id = obj_priv_key.tenant_id_;
  const ObObjPriv *obj_priv = NULL;
  obj_privs = 0;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_priv(obj_priv_key, obj_priv))) {
    LOG_WARN("fail to get table priv set", KR(ret), K(obj_priv_key));
  } else if (obj_priv != NULL) {
    obj_privs = obj_priv->get_obj_privs();
  }
  return ret;
}

int ObSchemaGetterGuard::get_user_infos_with_tenant_id(
    const uint64_t tenant_id,
    common::ObIArray<const ObUserInfo *> &user_infos)
{
  int ret = OB_SUCCESS;
  user_infos.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_user_schemas_in_tenant(tenant_id,
                                                user_infos))) {
    LOG_WARN("get user schemas in tenant failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_db_priv_with_tenant_id(const uint64_t tenant_id,
                                                    ObIArray<const ObDBPriv *> &db_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  db_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_privs_in_tenant(tenant_id, db_privs))) {
    LOG_WARN("get db priv with tenant_id failed", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_db_priv_with_user_id(const uint64_t tenant_id,
                                                  const uint64_t user_id,
                                                  ObIArray<const ObDBPriv *> &db_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  db_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_db_privs_in_user(tenant_id, user_id, db_privs))) {
    LOG_WARN("get db priv with user_id failed", KR(ret), K(tenant_id), K(user_id));
  }

  return ret;
}

// TODO: Based on the following reasons, we don't maintain privileges of normal tenant's system tables in tenant space.
// 1. Normal tenant can't read/write system table in tenant space directly.
// 2. We don't check privileges if we query normal tenant's system tables with inner sql.
// 3. We check system tenant's privileges if we query normal tenant's system table after we execute change tenant cmd.
int ObSchemaGetterGuard::get_table_priv_with_tenant_id(const uint64_t tenant_id,
                                                       ObIArray<const ObTablePriv *> &table_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  table_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_table_privs_in_tenant(tenant_id, table_privs))) {
    LOG_WARN("get table priv with tenant_id failed", KR(ret), K(tenant_id));
  }

  return ret;
}

// TODO: Based on the following reasons, we don't maintain privileges of normal tenant's system tables in tenant space.
// 1. Normal tenant can't read/write system table in tenant space directly.
// 2. We don't check privileges if we query normal tenant's system tables with inner sql.
// 3. We check system tenant's privileges if we query normal tenant's system table after we execute change tenant cmd.
int ObSchemaGetterGuard::get_table_priv_with_user_id(const uint64_t tenant_id,
                                                     const uint64_t user_id,
                                                     ObIArray<const ObTablePriv *> &table_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  table_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_table_privs_in_user(tenant_id, user_id, table_privs))) {
    LOG_WARN("get table priv with user_id failed", KR(ret), K(tenant_id), K(user_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_priv_with_grantee_id(
    const uint64_t tenant_id,
    const uint64_t grantee_id,
    ObIArray<const ObObjPriv *> &obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  obj_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == grantee_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(grantee_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantee(tenant_id, grantee_id, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(grantee_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_priv_with_grantor_id(
    const uint64_t tenant_id,
    const uint64_t grantor_id,
    ObIArray<const ObObjPriv *> &obj_privs,
    bool reset_flag)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (reset_flag) {
    obj_privs.reset();
  }

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == grantor_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(grantor_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantor(tenant_id, grantor_id,
                     obj_privs, reset_flag))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(grantor_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_priv_with_obj_id(
    const uint64_t tenant_id,
    const uint64_t obj_id,
    const uint64_t obj_type,
    ObIArray<const ObObjPriv *> &obj_privs,
    bool reset_flag)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (reset_flag) {
    obj_privs.reset();
  }

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == obj_id
             || OB_INVALID_ID == obj_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(obj_id), K(obj_type));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_obj(tenant_id, obj_id, obj_type,
                     obj_privs, reset_flag))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(obj_id),
             K(obj_type));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_privs_in_ur_and_obj(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    ObPackedObjPriv &obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!obj_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_key));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_ur_and_obj(
                      tenant_id, obj_key, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(obj_key));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_privs_in_grantor_ur_obj_id(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    common::ObIArray<const ObObjPriv *> &obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!obj_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_key));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantor_ur_obj_id(
                      tenant_id, obj_key, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(obj_key));
  }

  return ret;
}

int ObSchemaGetterGuard::get_obj_privs_in_grantor_obj_id(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    common::ObIArray<const ObObjPriv *> &obj_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!obj_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_key));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_privs_in_grantor_obj_id(
                      tenant_id, obj_key, obj_privs))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(obj_key));
  }

  return ret;
}

inline bool ObSchemaGetterGuard::check_inner_stat() const
{
  bool ret = true;
  if (!is_inited_) {
    ret = false;
    LOG_WARN("schema guard not inited", KR(ret));
  } else if (NULL == schema_service_
      || INVALID_SCHEMA_GUARD_TYPE == schema_guard_type_) {
    ret = false;
    LOG_WARN("invalid inner stat", K(schema_service_), K_(schema_guard_type));
  }
  return ret;
}

// OB_INVALID_VERSION means schema doesn't exist.
// bugfix:
int ObSchemaGetterGuard::get_schema_version(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_normal_schema(schema_type)
             || OB_INVALID_ID == schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(schema_type), K(schema_id));
  } else {
#define GET_SCHEMA_VERSION(SCHEMA, SCHEMA_TYPE) \
      const SCHEMA_TYPE *schema = NULL;             \
      const ObSchemaMgr *mgr = NULL; \
      if (OB_FAIL(check_tenant_schema_guard(tenant_id))) { \
        LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
      } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) { \
        LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id)); \
      } else if (OB_FAIL(mgr->get_##SCHEMA##_schema(tenant_id, schema_id, schema))) {       \
        LOG_WARN("get "#SCHEMA" schema failed", KR(ret), K(tenant_id), K(schema_id));     \
      } else if (OB_NOT_NULL(schema)) {                                         \
        schema_version = schema->get_schema_version();                     \
      }
    switch (schema_type) {
    case TENANT_SCHEMA : {
        const ObSimpleTenantSchema *schema = NULL;
        const ObSchemaMgr *mgr = NULL;
        if (!is_sys_tenant(tenant_id)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant_id is not match with schema_id",
                   KR(ret), K(tenant_id), K(schema_id));
        } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
          LOG_WARN("fail to check lazy guard", KR(ret), K(schema_id));
        } else if (OB_FAIL(mgr->get_tenant_schema(schema_id, schema))) {
          LOG_WARN("get tenant schema failed", KR(ret), K(schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
        }
        break;
      }
    case USER_SCHEMA : {
        GET_SCHEMA_VERSION(user, ObSimpleUserSchema);
        break;
      }
    case DATABASE_SCHEMA : {
        GET_SCHEMA_VERSION(database, ObSimpleDatabaseSchema);
        break;
      }
    case TABLEGROUP_SCHEMA : {
        GET_SCHEMA_VERSION(tablegroup, ObSimpleTablegroupSchema);
        break;
      }
    case TABLE_SCHEMA : {
        if (is_cte_table(schema_id)) {
          // fake table, we should avoid error in such situation.
          schema_version = OB_INVALID_VERSION;
        } else {
          GET_SCHEMA_VERSION(table, ObSimpleTableSchemaV2);
        }
        break;
      }
    case SYNONYM_SCHEMA : {
        GET_SCHEMA_VERSION(synonym, ObSimpleSynonymSchema);
        break;
      }
    case PACKAGE_SCHEMA : {
        if (ObTriggerInfo::is_trigger_package_id(schema_id)) {
          const ObSimpleTriggerSchema *schema = NULL;
          const ObSchemaMgr *mgr = NULL;
          const uint64_t trigger_id = ObTriggerInfo::get_package_trigger_id(schema_id);
          if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
            LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
          } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
            LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
          } else if (OB_FAIL(mgr->get_trigger_schema(tenant_id, trigger_id, schema))) {
            LOG_WARN("get trigger schema failed", KR(ret), K(tenant_id), K(trigger_id));
          } else if (OB_NOT_NULL(schema)) {
            schema_version = schema->get_schema_version();
          }
        } else {
          GET_SCHEMA_VERSION(package, ObSimplePackageSchema);
        }
        break;
      }
    case ROUTINE_SCHEMA : {
        GET_SCHEMA_VERSION(routine, ObSimpleRoutineSchema);
        break;
      }
    case UDT_SCHEMA : {
        GET_SCHEMA_VERSION(udt, ObSimpleUDTSchema);
        break;
    }
    case UDF_SCHEMA : {
        GET_SCHEMA_VERSION(udf, ObSimpleUDFSchema);
        break;
      }
    case SEQUENCE_SCHEMA : {
        GET_SCHEMA_VERSION(sequence, ObSequenceSchema);
        break;
      }
    case SYS_VARIABLE_SCHEMA : {
        const ObSimpleSysVariableSchema *schema = NULL;
        const ObSchemaMgr *mgr = NULL;
        const uint64_t tenant_id = schema_id;
        if (tenant_id != schema_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant_id is not match with schema_id", KR(ret), K(tenant_id), K(schema_id));
        } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
          LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
        } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
          LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
        } else if (OB_FAIL(mgr->sys_variable_mgr_.get_sys_variable_schema(schema_id, schema))) {
          LOG_WARN("get sys variable schema failed", KR(ret), K(schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
          LOG_TRACE("get sys variable schema", KR(ret), K(schema_id), K(*schema),
                    "snapshot_version", mgr->get_schema_version());
        }
        break;
      }
    case KEYSTORE_SCHEMA: {
        const ObKeystoreSchema *schema = NULL;
        const ObSchemaMgr *mgr = NULL;
        if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
          LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
        } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
          LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
        } else if (OB_FAIL(mgr->get_keystore_schema(schema_id, schema))) {
          LOG_WARN("get keystore schema failed", KR(ret), K(tenant_id), K(schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
        }
        break;
      }
    case LABEL_SE_POLICY_SCHEMA: {
      GET_SCHEMA_VERSION(label_se_policy, ObLabelSePolicySchema);
      break;
    }
    case LABEL_SE_COMPONENT_SCHEMA: {
      GET_SCHEMA_VERSION(label_se_component, ObLabelSeComponentSchema);
      break;
    }
    case LABEL_SE_LABEL_SCHEMA: {
      GET_SCHEMA_VERSION(label_se_label, ObLabelSeLabelSchema);
      break;
    }
    case LABEL_SE_USER_LEVEL_SCHEMA: {
      GET_SCHEMA_VERSION(label_se_user_level, ObLabelSeUserLevelSchema);
      break;
    }
    case TABLESPACE_SCHEMA: {
      GET_SCHEMA_VERSION(tablespace, ObTablespaceSchema);
      break;
    }
    case PROFILE_SCHEMA : {
      GET_SCHEMA_VERSION(profile, ObProfileSchema);
      break;
    }
    case TRIGGER_SCHEMA: {
      GET_SCHEMA_VERSION(trigger, ObSimpleTriggerSchema);
      break;
    }
    case DBLINK_SCHEMA : {
        GET_SCHEMA_VERSION(dblink, ObDbLinkSchema);
        break;
      }
    case DIRECTORY_SCHEMA : {
        GET_SCHEMA_VERSION(directory, ObDirectorySchema);
        break;
      }
    case MOCK_FK_PARENT_TABLE_SCHEMA : {
        const ObSimpleMockFKParentTableSchema *schema = NULL;
        const ObSchemaMgr *mgr = NULL;
        if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
          LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id), K_(tenant_id));
        } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
          LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
        } else if (OB_FAIL(mgr->mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schema(tenant_id, schema_id, schema))) {
          LOG_WARN("get mock_fk_parent_table schema failed", K(ret), K(tenant_id), K(schema_id));
        } else if (OB_NOT_NULL(schema)) {
          schema_version = schema->get_schema_version();
        }
        break;
      }
    default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not reach here", KR(ret));
        break;
      }
    }
#undef GET_SCHEMA_VERSION
  }
  LOG_TRACE("get schema version v2", KR(ret), K(schema_type), K(schema_id), K(schema_version));
  return ret;
}

template<typename T>
int ObSchemaGetterGuard::get_from_local_cache(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const T *&schema)
{
  int ret = OB_SUCCESS;
  schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == schema_id
             || OB_INVALID_ID == tenant_id
             || OB_INVALID_TENANT_ID == tenant_id
             || !is_normal_schema(schema_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(schema_id), K(schema_type));
  } else {
    const ObSchema *tmp_schema = NULL;
    bool found = false;
    FOREACH_CNT_X(id_schema, schema_objs_, !found) {
      if (id_schema->schema_type_ == schema_type
          && id_schema->tenant_id_ == tenant_id
          && id_schema->schema_id_ == schema_id) {
        tmp_schema = id_schema->schema_;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("local cache miss [id to schema]", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp schema is NULL", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else {
      schema = static_cast<const T *>(tmp_schema);
      LOG_TRACE("schema cache hit", K(schema_type), K(tenant_id), K(schema_id));
    }
  }

  return ret;
}

template<typename T>
int ObSchemaGetterGuard::put_to_local_cache(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const T *&schema,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  SchemaObj schema_obj_tmp; // just for array push back
  if (OB_FAIL(schema_objs_.push_back(schema_obj_tmp))) {
    LOG_WARN("add handle failed", K(ret));
  } else {
    SchemaObj &schema_obj = schema_objs_.at(schema_objs_.count() - 1);
    schema_obj.schema_type_ = schema_type;
    schema_obj.tenant_id_ = tenant_id;
    schema_obj.schema_id_ = schema_id;
    schema_obj.schema_ = const_cast<ObSchema*>(schema);
    schema_obj.handle_.move_from(handle);
    if (schema_obj.handle_.is_valid()
        && OB_NOT_NULL(schema)
        && pin_cache_size_ < FULL_SCHEMA_MEM_THREHOLD) {
        pin_cache_size_ += schema->get_convert_size();
      if (pin_cache_size_ >= FULL_SCHEMA_MEM_THREHOLD) {
        FLOG_WARN("hold too much full schema memory", K(tenant_id), K(pin_cache_size_), K(lbt()));
      }
    }
  }
  return ret;
}


template<typename T>
int ObSchemaGetterGuard::get_schema(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const T *&schema,
    int64_t specified_version /*=OB_INVALID_VERSION*/)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  const ObSchemaMgr *mgr = NULL;
  const ObSchema *base_schema = NULL;
  ObKVCacheHandle handle;
  ObRefreshSchemaStatus schema_status;
  schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (TENANT_SCHEMA == schema_type && !is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id for TENANT_SCHEMA", KR(ret), K(tenant_id), K(schema_id));
  } else if (SYS_VARIABLE_SCHEMA == schema_type && tenant_id != schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and schema_id not match for TENANT_SCHEMA",
             KR(ret), K(tenant_id), K(schema_id));
  } else if (!is_normal_schema(schema_type)
             || OB_INVALID_ID == schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_type), K(schema_id));
  } else if (OB_FAIL(get_from_local_cache(schema_type, tenant_id, schema_id, schema))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get from local cache failed [id to schema]",
               KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {  // overwrite ret and fetch schema if OB_ENTRY_NOT_EXIST
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    } else if (OB_NOT_NULL(mgr)) {
      // case 1: not lazy mode
      if (TABLE_SIMPLE_SCHEMA == schema_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("should fetch simple table schema in lazy mode", KR(ret), K(schema_id), K(specified_version));
      } else {
        if (OB_INVALID_VERSION != specified_version) {
          schema_version = specified_version;
        } else if (OB_FAIL(get_schema_version(schema_type,
                                              tenant_id,
                                              schema_id,
                                              schema_version))) {
          LOG_WARN("get schema version failed",
                    KR(ret), K(tenant_id), K(schema_type), K(schema_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_INVALID_VERSION == schema_version) {
            if (is_cte_table(schema_id)) {
              LOG_INFO("invalid version", K(schema_type), K(tenant_id),
                       K(schema_id), K(specified_version));
            }
          } else if (OB_FAIL(get_schema_status(tenant_id, schema_status))) {
            LOG_WARN("fail to get schema status", KR(ret), K(tenant_id), K(schema_type));
          } else if (OB_FAIL(schema_service_->get_schema(mgr,
                                                         schema_status,
                                                         schema_type,
                                                         schema_id,
                                                         schema_version,
                                                         handle,
                                                         base_schema))) {
            LOG_WARN("get schema failed", KR(ret), K(schema_status), K(schema_type),
                     K(schema_id), K(schema_version), K(specified_version));
          } else if (OB_ISNULL(base_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr, unexpected", KR(ret), K(schema_status), K(schema_type),
                     K(schema_id), K(schema_version), K(specified_version));
          } else if (OB_FAIL(put_to_local_cache(schema_type, tenant_id, schema_id,
                                                base_schema, handle))) {
            LOG_WARN("fail to put to local cache", KR(ret),
                     K(schema_type), K(tenant_id), K(schema_id),
                     K(schema_version), K(specified_version));
          } else {
            schema = static_cast<const T *>(base_schema);
          }
        }
      }
    } else {
      // case 2: lazy mode
      LOG_TRACE("fetch schema in lazy mode", K(schema_type), K(schema_id),
                K(tenant_id_), K(tenant_id), K(specified_version));
      if (OB_INVALID_VERSION != specified_version) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("specified_version should be invalid for lazy mode", KR(ret),
                 K(schema_type), K(schema_id), K(specified_version));
      } else if (use_schema_status() && !is_sys_tenant(tenant_id)) {
        // ObSchemaGetterGuard doesn't cache schema_status when guard is in lazy mode,
        // so we need to fetch schema status by inner sql in such situation.
        ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
        if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("proxy is null", KR(ret));
        } else if (OB_ISNULL(schema_status_proxy)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_status_proxy is null", KR(ret));
        } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, schema_status))) {
          LOG_WARN("fail to get refresh schema status", KR(ret), K(tenant_id));
        }
      } else {
        schema_status.tenant_id_ = tenant_id;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_schema_version(tenant_id, schema_version))) {
        LOG_INFO("fail to get snapshot version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_schema(
          NULL,
          schema_status,
          schema_type,
          schema_id,
          schema_version,
          handle,
          base_schema))) {
        LOG_WARN("get schema failed", K(schema_type), K(schema_id),
                 K(schema_version), KR(ret));
      } else if (OB_ISNULL(base_schema)) {
        // schema may not exist
      } else if (OB_FAIL(put_to_local_cache(schema_type, tenant_id, schema_id,
                                            base_schema, handle))) {
        LOG_WARN("fail to put to local cache", KR(ret),
                 K(schema_type), K(tenant_id), K(schema_id));
      } else {
        schema = static_cast<const T *>(base_schema);
      }
    }
  }

  if (OB_FAIL(ret)
      && OB_TENANT_HAS_BEEN_DROPPED != ret
      && ObSchemaService::g_liboblog_mode_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(schema_service_))  {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", KR(ret), K(tmp_ret));
    } else {
      uint64_t query_tenant_id = (TENANT_SCHEMA == schema_type) ?
                                 schema_id : tenant_id;
      TenantStatus tenant_status = TENANT_STATUS_INVALID;
      tmp_ret = schema_service_->query_tenant_status(query_tenant_id, tenant_status);
      if (OB_SUCCESS != tmp_ret){
        LOG_WARN("query tenant status failed", KR(ret), K(tmp_ret),
                 K(query_tenant_id), K(tenant_id), K_(tenant_id));
      } else if (TENANT_DELETED == tenant_status) {
        LOG_INFO("tenant has been dropped, no need retry", KR(ret),
                 K(query_tenant_id), K(tenant_id), K_(tenant_id));
        ret = OB_TENANT_HAS_BEEN_DROPPED; //overwrite ret
      }
    }
  }
  return ret;
}

const ObUserInfo *ObSchemaGetterGuard::get_user_info(
      const uint64_t tenant_id, const uint64_t user_id)
{
  const ObUserInfo *user_info = NULL;
  int ret = get_user_info(tenant_id, user_id, user_info);
  return OB_SUCC(ret) ? user_info : NULL;
}


const ObTablegroupSchema *ObSchemaGetterGuard::get_tablegroup_schema(
      const uint64_t tenant_id, const uint64_t tablegroup_id)
{
  const ObTablegroupSchema *tablegroup_schema = NULL;
  int ret = get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup_schema);
  return OB_SUCC(ret) ? tablegroup_schema : NULL;
}
const ObColumnSchemaV2 *ObSchemaGetterGuard::get_column_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t column_id)
{
  const ObColumnSchemaV2 *column_schema = NULL;
  int ret = get_column_schema(tenant_id, table_id, column_id, column_schema);
  return OB_SUCC(ret) ? column_schema : NULL;
}

const ObTenantSchema *ObSchemaGetterGuard::get_tenant_info(const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_info = NULL;
  if (OB_FAIL(get_tenant_info(tenant_name, tenant_info))) {
    LOG_WARN("get tenant info failed", KR(ret), K(tenant_name));
  }
  return OB_SUCC(ret) ? tenant_info : NULL;
}

#define GET_SIMPLE_SCHEMAS_IN_TENANT_FUNC_DEFINE(SCHEMA, SIMPLE_SCHEMA_TYPE) \
  int ObSchemaGetterGuard::get_##SCHEMA##_schemas_in_tenant(                       \
      const uint64_t tenant_id, ObIArray<const SIMPLE_SCHEMA_TYPE*> &schema_array)       \
  {                                                                                \
    int ret = OB_SUCCESS;                                                          \
    const ObSchemaMgr *mgr = NULL;                                                 \
    schema_array.reset();                                                          \
    if (!check_inner_stat()) {                                                     \
      ret = OB_INNER_STAT_ERROR;                                                   \
      LOG_WARN("inner stat error", KR(ret));                                        \
    } else if (OB_INVALID_ID == tenant_id) {                                       \
      ret = OB_INVALID_ARGUMENT;                                                   \
      LOG_WARN("invalid argument", KR(ret), K(tenant_id));                          \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) { \
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) { \
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id)); \
    } else if (OB_FAIL(mgr->get_##SCHEMA##_schemas_in_tenant(tenant_id,          \
                                                              schema_array))) {  \
      LOG_WARN("get "#SCHEMA" schemas in tenant failed", KR(ret), K(tenant_id));    \
    }                                                                             \
    return ret;                                                                   \
  }
GET_SIMPLE_SCHEMAS_IN_TENANT_FUNC_DEFINE(database, ObSimpleDatabaseSchema);
#undef GET_SIMPLE_SCHEMAS_IN_TENANT_FUNC_DEFINE

#define GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(SCHEMA, SCHEMA_TYPE, SIMPLE_SCHEMA_TYPE, SCHEMA_TYPE_ENUM) \
  int ObSchemaGetterGuard::get_##SCHEMA##_schemas_in_tenant(                       \
      const uint64_t tenant_id, ObIArray<const SCHEMA_TYPE *> &schema_array)       \
  {                                                                                \
    int ret = OB_SUCCESS;                                                          \
    const ObSchemaMgr *mgr = NULL;                                                 \
    schema_array.reset();                                                          \
    ObArray<const SIMPLE_SCHEMA_TYPE *> simple_schemas;                            \
    if (!check_inner_stat()) {                                                     \
      ret = OB_INNER_STAT_ERROR;                                                   \
      LOG_WARN("inner stat error", KR(ret));                                        \
    } else if (OB_INVALID_ID == tenant_id) {                                       \
      ret = OB_INVALID_ARGUMENT;                                                   \
      LOG_WARN("invalid argument", KR(ret), K(tenant_id));                          \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) { \
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) { \
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id)); \
    } else if (OB_FAIL(mgr->get_##SCHEMA##_schemas_in_tenant(tenant_id,          \
                                                              simple_schemas))) {  \
      LOG_WARN("get "#SCHEMA" schemas in tenant failed", KR(ret), K(tenant_id));    \
    } else {                                                                       \
      FOREACH_CNT_X(simple_schema, simple_schemas, OB_SUCC(ret)) {                 \
        const SIMPLE_SCHEMA_TYPE *tmp_schema = *simple_schema;                    \
        const SCHEMA_TYPE *schema = NULL;                                         \
        if (OB_ISNULL(tmp_schema)) {                                              \
          ret = OB_ERR_UNEXPECTED;                                                \
          LOG_WARN("NULL ptr", KR(ret));                                           \
        } else if (OB_FAIL(get_schema(SCHEMA_TYPE_ENUM,                           \
                                 tmp_schema->get_tenant_id(),                     \
                                 tmp_schema->get_##SCHEMA##_id(),                 \
                                 schema,                                          \
                                 tmp_schema->get_schema_version()))) {            \
          LOG_WARN("get "#SCHEMA" schema failed", KR(ret), K(tenant_id));          \
        } else if (OB_ISNULL(schema)) {                                           \
          ret = OB_ERR_UNEXPECTED;                                                \
          LOG_WARN("NULL ptr", KR(ret), KP(schema));                               \
        } else if (OB_FAIL(schema_array.push_back(schema))) {                     \
          LOG_WARN("push back schema failed", KR(ret));                            \
        }                                                                         \
      }                                                                           \
    }                                                                             \
    return ret;                                                                   \
  }
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(user, ObUserInfo, ObSimpleUserSchema, USER_SCHEMA);
GET_SCHEMAS_IN_TENANT_FUNC_DEFINE(database, ObDatabaseSchema, ObSimpleDatabaseSchema, DATABASE_SCHEMA);
#undef GET_SCHEMAS_IN_TENANT_FUNC_DEFINE

#define GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(MGR, SCHEMA, SCHEMA_TYPE, SIMPLE_SCHEMA_TYPE, SCHEMA_TYPE_ENUM) \
  int ObSchemaGetterGuard::get_##SCHEMA##_schemas_in_tenant(                       \
      const uint64_t tenant_id, ObIArray<const SCHEMA_TYPE *> &schema_array)       \
  {                                                                                \
    int ret = OB_SUCCESS;                                                          \
    const ObSchemaMgr *mgr = NULL;                                                 \
    schema_array.reset();                                                          \
    ObArray<const SIMPLE_SCHEMA_TYPE *> simple_schemas;                            \
    if (!check_inner_stat()) {                                                     \
      ret = OB_INNER_STAT_ERROR;                                                   \
      LOG_WARN("inner stat error", KR(ret));                                        \
    } else if (OB_INVALID_ID == tenant_id) {                                       \
      ret = OB_INVALID_ARGUMENT;                                                   \
      LOG_WARN("invalid argument", KR(ret), K(tenant_id));                          \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) { \
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) { \
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id)); \
    } else if (OB_FAIL((mgr->MGR).get_##SCHEMA##_schemas_in_tenant(tenant_id,          \
                                                              simple_schemas))) {  \
      LOG_WARN("get "#SCHEMA" schemas in tenant failed", KR(ret), K(tenant_id));    \
    } else {                                                                       \
      FOREACH_CNT_X(simple_schema, simple_schemas, OB_SUCC(ret)) {                 \
        const SIMPLE_SCHEMA_TYPE *tmp_schema = *simple_schema;                    \
        const SCHEMA_TYPE *schema = NULL;                                         \
        if (OB_ISNULL(tmp_schema)) {                                              \
          ret = OB_ERR_UNEXPECTED;                                                \
          LOG_WARN("NULL ptr", KR(ret));                                           \
        } else if (OB_FAIL(get_schema(SCHEMA_TYPE_ENUM,                           \
                                 tmp_schema->get_tenant_id(),                     \
                                 tmp_schema->get_##SCHEMA##_id(),                 \
                                 schema,                                          \
                                 tmp_schema->get_schema_version()))) {            \
          LOG_WARN("get "#SCHEMA" schema failed", KR(ret), K(tenant_id));          \
        } else if (OB_ISNULL(schema)) {                                           \
          ret = OB_ERR_UNEXPECTED;                                                \
          LOG_WARN("NULL ptr", KR(ret), KP(schema));                               \
        } else if (OB_FAIL(schema_array.push_back(schema))) {                     \
          LOG_WARN("push back schema failed", KR(ret));                            \
        }                                                                         \
      }                                                                           \
    }                                                                             \
    return ret;                                                                   \
  }

GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(outline_mgr_, outline, ObOutlineInfo, ObSimpleOutlineSchema, OUTLINE_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(routine_mgr_, routine, ObRoutineInfo, ObSimpleRoutineSchema, ROUTINE_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(package_mgr_, package, ObPackageInfo, ObSimplePackageSchema, PACKAGE_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(trigger_mgr_, trigger, ObTriggerInfo, ObSimpleTriggerSchema, TRIGGER_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(udt_mgr_, udt, ObUDTTypeInfo, ObSimpleUDTSchema, UDT_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(tablespace_mgr_, tablespace, ObTablespaceSchema, ObTablespaceSchema, TABLESPACE_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(synonym_mgr_, synonym, ObSynonymInfo, ObSimpleSynonymSchema, SYNONYM_SCHEMA);
GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE(directory_mgr_, directory, ObDirectorySchema, ObDirectorySchema, DIRECTORY_SCHEMA);
#undef GET_SCHEMAS_WITH_MGR_IN_TENANT_FUNC_DEFINE

int ObSchemaGetterGuard::get_outline_infos_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObOutlineInfo *> &table_schemas)
{
  return get_outline_schemas_in_tenant(tenant_id, table_schemas);
}

int ObSchemaGetterGuard::get_package_infos_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObPackageInfo *> &package_infos)
{
  return get_package_schemas_in_tenant(tenant_id, package_infos);
}

int ObSchemaGetterGuard::get_routine_infos_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObRoutineInfo *> &routine_infos)
{
  return get_routine_schemas_in_tenant(tenant_id, routine_infos);
}

int ObSchemaGetterGuard::get_trigger_infos_in_tenant(const uint64_t tenant_id,
                                                     ObIArray<const ObTriggerInfo *> &triger_infos)
{
  return get_trigger_schemas_in_tenant(tenant_id, triger_infos);
}

int ObSchemaGetterGuard::get_synonym_infos_in_tenant(const uint64_t tenant_id,
                         common::ObIArray<const ObSynonymInfo *> &synonym_infos)
{
  return get_synonym_schemas_in_tenant(tenant_id, synonym_infos);
}

int ObSchemaGetterGuard::get_udt_infos_in_tenant(const uint64_t tenant_id,
                         common::ObIArray<const ObUDTTypeInfo *> &udt_infos)
{
  return get_udt_schemas_in_tenant(tenant_id, udt_infos);
}

int ObSchemaGetterGuard::get_audit_schema_in_tenant(const uint64_t tenant_id,
  const ObSAuditType audit_type,
  const uint64_t owner_id,
  const ObSAuditOperationType operation_type,
  const ObSAuditSchema *&ret_audit_schema) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  ret_audit_schema = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, schema_mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr->get_audit_schema(tenant_id, audit_type, owner_id, operation_type, ret_audit_schema))) {
    LOG_WARN("get audit schema failed", KR(ret), K(tenant_id), K(audit_type), K(owner_id), K(operation_type));
  }
  return ret;
}


int ObSchemaGetterGuard::get_audit_schema_in_owner(const uint64_t tenant_id,
    const ObSAuditType audit_type, const uint64_t owner_id,
    common::ObIArray<const ObSAuditSchema *> &audit_schemas) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, schema_mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr->get_audit_schemas_in_tenant(tenant_id,
                                                             audit_type,
                                                             owner_id,
                                                             audit_schemas))) {
    LOG_WARN("get audit schema failed", KR(ret), K(tenant_id), K(audit_type), K(owner_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_audit_schemas_in_tenant(const uint64_t tenant_id,
    common::ObIArray<const ObSAuditSchema *> &security_audits)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, schema_mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr->get_audit_schemas_in_tenant(tenant_id, security_audits))) {
    LOG_WARN("get audit schema failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::check_allow_audit(const uint64_t tenant_id,
                                           ObSAuditType &audit_type,
                                           const uint64_t owner_id,
                                           ObSAuditOperationType &operation_type,
                                           const int return_code,
                                           uint64_t &audit_id,
                                           bool &is_allow_audit)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  is_allow_audit = false;
  audit_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (AUDIT_OP_INVALID == operation_type) {
    //do nothing
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, schema_mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr->check_allow_audit(tenant_id,
                                                   audit_type,
                                                   owner_id,
                                                   operation_type,
                                                   return_code,
                                                   audit_id,
                                                   is_allow_audit))) {
    LOG_WARN("check_allow_audit failed", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::check_allow_audit_by_default(const uint64_t tenant_id,
                                                      ObSAuditType &audit_type,
                                                      ObSAuditOperationType &operation_type,
                                                      const int return_code,
                                                      uint64_t &audit_id,
                                                      bool &is_allow_audit)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  is_allow_audit = false;
  audit_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (AUDIT_OP_INVALID == operation_type) {
    //do nothing
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, schema_mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr->check_allow_audit_by_default(tenant_id,
                                                              audit_type,
                                                              operation_type,
                                                              return_code,
                                                              audit_id,
                                                              is_allow_audit))) {
    LOG_WARN("check_allow_audit_by_default failed", KR(ret));
  }
  return ret;
}

#define GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE(DST_SCHEMA)                          \
  int ObSchemaGetterGuard::get_table_ids_in_##DST_SCHEMA(const uint64_t tenant_id,   \
      const uint64_t dst_schema_id,                                                  \
      ObIArray<uint64_t> &table_ids)                                                 \
  {                                                                                  \
    int ret = OB_SUCCESS;                                                            \
    const ObSchemaMgr *mgr = NULL;                                                   \
    ObArray<const ObSimpleTableSchemaV2 *> schemas;                                  \
    table_ids.reset();                                                               \
    if (!check_inner_stat()) {                                                       \
      ret = OB_INNER_STAT_ERROR;                                                     \
      LOG_WARN("inner stat error", KR(ret));                                          \
    } else if (OB_INVALID_ID == tenant_id ||                                         \
               OB_INVALID_ID == dst_schema_id) {                                     \
      ret = OB_INVALID_ARGUMENT;                                                     \
      LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dst_schema_id));          \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                      \
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
    } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {                            \
      if (OB_TENANT_NOT_EXIST == ret) {                                              \
        ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;           \
      }                                                                              \
      if (OB_FAIL(ret)) {                                                            \
        LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));                    \
      }                                                                              \
    } else if (OB_ISNULL(mgr)) {                                                     \
      ret = OB_SCHEMA_EAGAIN;                                                        \
      LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));\
    } else if (OB_FAIL(mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id,             \
          dst_schema_id, schemas))) {                                                \
      LOG_WARN("get table schemas in "#DST_SCHEMA" failed", KR(ret),                  \
               K(tenant_id), #DST_SCHEMA"_id", dst_schema_id);                       \
    } else {                                                                         \
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {                                 \
        const ObSimpleTableSchemaV2 *tmp_schema = *schema;                           \
        if (OB_ISNULL(tmp_schema)) {                                                 \
          ret = OB_ERR_UNEXPECTED;                                                   \
          LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));                              \
        } else if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {       \
          LOG_WARN("push back table id failed", KR(ret));                             \
        }                                                                            \
      }                                                                              \
    }                                                                                \
    return ret;                                                                      \
  }

GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE(database);
GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup);
#undef GET_TABLE_IDS_IN_DST_SCHEMA_FUNC_DEFINE

int ObSchemaGetterGuard::get_table_ids_in_tenant(const uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  table_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get table schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(table_ids.push_back(tmp_schema->get_table_id()))) {
        LOG_WARN("push back table id failed", KR(ret));
      }
    }
  }
  return ret;
}
#define GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(DST_SCHEMA)                      \
  int ObSchemaGetterGuard::get_table_schemas_in_##DST_SCHEMA(                        \
      const uint64_t tenant_id,                                                      \
      const uint64_t dst_schema_id,                                                  \
      ObIArray<const ObTableSchema *> &schema_array)                                 \
  {                                                                                  \
    int ret = OB_SUCCESS;                                                            \
    const ObSchemaMgr *mgr = NULL;                                                   \
    ObArray<const ObSimpleTableSchemaV2 *> schemas;                                  \
    schema_array.reset();                                                            \
    if (!check_inner_stat()) {                                                       \
      ret = OB_INNER_STAT_ERROR;                                                     \
      LOG_WARN("inner stat error", KR(ret));                                          \
    } else if (OB_INVALID_ID == tenant_id ||                                         \
               OB_INVALID_ID == dst_schema_id) {                                     \
      ret = OB_INVALID_ARGUMENT;                                                     \
      LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dst_schema_id));          \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                      \
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
    } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {                            \
      if (OB_TENANT_NOT_EXIST == ret) {                                              \
        ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;           \
      }                                                                              \
      if (OB_FAIL(ret)) {                                                            \
        LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));                    \
      }                                                                              \
    } else if (OB_ISNULL(mgr)) {                                                     \
      ret = OB_SCHEMA_EAGAIN;                                                        \
      LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));\
    } else if (OB_FAIL(mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id,             \
          dst_schema_id, schemas))) {                                                \
      LOG_WARN("get table schemas in "#DST_SCHEMA" failed", KR(ret),                  \
               K(tenant_id),                                                         \
               #DST_SCHEMA"_id", dst_schema_id);                                     \
    } else {                                                                         \
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {                                 \
        const ObSimpleTableSchemaV2 *tmp_schema = *schema;                           \
        const ObTableSchema *table_schema = NULL;                                    \
        if (OB_ISNULL(tmp_schema)) {                                                 \
          ret = OB_ERR_UNEXPECTED;                                                   \
          LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));                              \
        } else if (OB_FAIL(get_schema(TABLE_SCHEMA,                                  \
            tmp_schema->get_tenant_id(), tmp_schema->get_table_id(),                 \
            table_schema, tmp_schema->get_schema_version()))) {                      \
          LOG_WARN("get table schema failed", KR(ret), K(tenant_id), KPC(tmp_schema));\
        } else if (OB_ISNULL(table_schema)) {                                        \
          ret = OB_ERR_UNEXPECTED;                                                   \
          LOG_WARN("NULL ptr", KR(ret), KP(table_schema));                            \
        } else if (OB_FAIL(schema_array.push_back(table_schema))) {                  \
          LOG_WARN("push back table schema failed", KR(ret));                         \
        }                                                                            \
      }                                                                              \
    }                                                                                \
    return ret;                                                                      \
  }

GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(database);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup);
GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablespace);
#undef GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE

int ObSchemaGetterGuard::get_table_schemas_in_tenant(
    const uint64_t tenant_id,
    common::ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  bool only_view_schema = false;
  ret = get_table_schemas_in_tenant_(tenant_id, only_view_schema, table_schemas);
  return ret;
}

int ObSchemaGetterGuard::get_view_schemas_in_tenant(const uint64_t tenant_id,
                                                    ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  bool only_view_schema = true;
  ret = get_table_schemas_in_tenant_(tenant_id, only_view_schema, table_schemas);
  return ret;
}

int ObSchemaGetterGuard::get_table_schemas_in_tenant_(const uint64_t tenant_id,
                                                      const bool only_view_schema,
                                                      ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get table schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *tmp_schema = *schema;
      const ObTableSchema *table_schema = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (only_view_schema && !tmp_schema->is_view_table()) {
        // do nothing
      } else if (OB_FAIL(get_schema(TABLE_SCHEMA,
          tmp_schema->get_tenant_id(), tmp_schema->get_table_id(),
          table_schema, tmp_schema->get_schema_version()))) {
        LOG_WARN("get table schema failed", KR(ret), K(tenant_id), KPC(tmp_schema));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(table_schema));
      } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
        LOG_WARN("push back table schema failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schemas_in_tenant(
    const uint64_t tenant_id,
    common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  table_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("get table schemas in tenant failed", KR(ret), K(tenant_id));
  }
  return ret;
}

# define GET_SIMPLE_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(DST_SCHEMA) \
int ObSchemaGetterGuard::get_table_schemas_in_##DST_SCHEMA( \
    const uint64_t tenant_id, \
    const uint64_t dst_schema_id, \
    common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas) \
{ \
  int ret = OB_SUCCESS; \
  const ObSchemaMgr *mgr = NULL; \
  table_schemas.reset(); \
  if (!check_inner_stat()) { \
    ret = OB_INNER_STAT_ERROR; \
    LOG_WARN("inner stat error", KR(ret)); \
  } else if (OB_INVALID_ID == tenant_id \
      || OB_INVALID_ID == dst_schema_id) { \
    ret = OB_INVALID_ARGUMENT; \
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dst_schema_id)); \
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) { \
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id)); \
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) { \
    if (OB_TENANT_NOT_EXIST == ret) {\
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret; \
    }\
    if (OB_FAIL(ret)) {\
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));\
    }\
  } else if (OB_ISNULL(mgr)) { \
    ret = OB_SCHEMA_EAGAIN; \
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id)); \
  } else if (OB_FAIL(mgr->get_table_schemas_in_##DST_SCHEMA(tenant_id, dst_schema_id, table_schemas))) { \
    LOG_WARN("get table schemas in "#DST_SCHEMA" failed", KR(ret), K(tenant_id), K(dst_schema_id)); \
  } \
  return ret; \
}
GET_SIMPLE_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(database)
GET_SIMPLE_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE(tablegroup)
# undef GET_SIMPLE_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DEFINE

int ObSchemaGetterGuard::get_primary_table_schema_in_tablegroup(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    const ObSimpleTableSchemaV2 *&primary_table_schema)
{
  int ret = OB_SUCCESS;
  primary_table_schema = NULL;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_primary_table_schema_in_tablegroup(tenant_id, tablegroup_id, primary_table_schema))) {
    LOG_WARN("get primary table schema in tablegroup failed", KR(ret), K(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_tenant_schemas(
    ObIArray<const ObSimpleTenantSchema *> &tenant_schemas) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tenant_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret));
  } else if (OB_FAIL(mgr->get_tenant_schemas(tenant_schemas))) {
    LOG_WARN("fail to get_tenant_schemas", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_ids(ObIArray<uint64_t> &tenant_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tenant_ids.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_lazy_guard(OB_SYS_TENANT_ID, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret));
  } else {
    ret = mgr->get_tenant_ids(tenant_ids);
  }
  return ret;
}

// For liboblog only, this function only return tenants in normal status.
int ObSchemaGetterGuard::get_available_tenant_ids(ObIArray<uint64_t> &tenant_ids) const
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  const ObSchemaMgr *schema_mgr = NULL;
  if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", KR(ret));
  } else if (OB_FAIL(schema_mgr->get_available_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get avaliable tenant_ids", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_name_case_mode(const uint64_t tenant_id,
                                                   ObNameCaseMode &mode)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  mode = OB_NAME_CASE_INVALID;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->get_tenant_name_case_mode(tenant_id, mode);
  }

  return ret;
}

// get read only attribute from sys variabl meta info
// FIXME: For the following reasons, inner sql won't check if tenant is read only after schema split.
// 1. To avoid cyclic dependence in the second stage of create tenant.
// 2. Inner sql should not be controlled by tenant's read only attribute.
int ObSchemaGetterGuard::get_tenant_read_only(const uint64_t tenant_id,
    bool &read_only)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  read_only = false;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->get_tenant_read_only(tenant_id, read_only);
  }

  return ret;
}

int ObSchemaGetterGuard::check_database_exists_in_tablegroup(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    bool &not_empty)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  not_empty = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->check_database_exists_in_tablegroup(tenant_id, tablegroup_id, not_empty);
  }
  return ret;
}

int ObSchemaGetterGuard::check_tenant_exist(const uint64_t tenant_id,
                                            bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_version(
             TENANT_SCHEMA, OB_SYS_TENANT_ID, tenant_id, schema_version))) {
    LOG_WARN("check tenant exist failed", KR(ret), K(tenant_id));
  } else if (OB_INVALID_VERSION != schema_version) {
    is_exist = true;
  }

  return ret;
}

int ObSchemaGetterGuard::check_outline_exist_with_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &name,
    uint64_t &outline_id,
    bool &exist)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  outline_id = OB_INVALID_ID;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleOutlineSchema *schema = NULL;
    if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_name(tenant_id, database_id,
        name, schema))) {
      LOG_WARN("get outline schema failed", KR(ret),
               K(tenant_id), K(database_id), K(name));
    } else if (NULL != schema) {
      outline_id = schema->get_outline_id();
      exist = true;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_outline_exist_with_sql_id(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &sql_id,
    bool &exist)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || sql_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(sql_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleOutlineSchema *schema = NULL;
    if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_sql_id(tenant_id, database_id,
        sql_id, schema))) {
      LOG_WARN("get outline schema failed", KR(ret),
               K(tenant_id), K(database_id), K(sql_id));
    } else if (NULL != schema) {
      exist = true;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_outline_exist_with_sql(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &paramlized_sql,
    bool &exist)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || paramlized_sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(paramlized_sql));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleOutlineSchema *schema = NULL;
    if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_signature(tenant_id, database_id,
        paramlized_sql, schema))) {
      LOG_WARN("get outline schema failed", KR(ret),
               K(tenant_id), K(database_id), K(paramlized_sql));
    } else if (NULL != schema) {
      exist = true;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_simple_synonym_info(const uint64_t tenant_id,
                                                 const uint64_t synonym_id,
                                                 const ObSimpleSynonymSchema *&synonym_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  synonym_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             ||OB_INVALID_ID == synonym_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(synonym_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_synonym_schema(tenant_id, synonym_id, synonym_info))) {
    LOG_WARN("get outline schema failed", K(tenant_id), K(synonym_id), KR(ret));
  } else if (NULL == synonym_info) {
    LOG_INFO("synonym not exist", K(tenant_id), K(synonym_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_synonym_info(const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const common::ObString &synonym_name,
                                          const ObSynonymInfo *&synonym_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleSynonymSchema *simple_synonym = NULL;
  synonym_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             ||OB_INVALID_ID == database_id
             || synonym_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(synonym_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schema_with_name(
           tenant_id, database_id, synonym_name, simple_synonym))) {
    LOG_WARN("get outline schema failed", K(tenant_id), K(database_id), K(synonym_name), KR(ret));
  } else if (NULL == simple_synonym) {
    LOG_INFO("synonym not exist", K(tenant_id), K(database_id), K(synonym_name));
  } else if (OB_FAIL(get_schema(SYNONYM_SCHEMA,
                                simple_synonym->get_tenant_id(),
                                simple_synonym->get_synonym_id(),
                                synonym_info,
                                simple_synonym->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id), KPC(simple_synonym));
  } else if (OB_ISNULL(synonym_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(simple_synonym));
  }
  return ret;
}

int ObSchemaGetterGuard::get_synonym_info(const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const common::ObString &synonym_name,
                                          const ObSimpleSynonymSchema *&synonym_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  synonym_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             ||OB_INVALID_ID == database_id
             || synonym_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(synonym_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schema_with_name(tenant_id, database_id,
                                                               synonym_name, synonym_info))) {
    LOG_WARN("get synonym schema failed", K(tenant_id), K(database_id),
              K(synonym_name), KR(ret));
  } else if (NULL == synonym_info) {
    LOG_INFO("synonym not exist", K(tenant_id), K(database_id), K(synonym_name));
  }
  return ret;
}

int ObSchemaGetterGuard::check_synonym_exist_with_name(const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &synonym_name,
    bool &exist,
    uint64_t &synonym_id)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             ||OB_INVALID_ID == database_id
             || synonym_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(synonym_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleSynonymSchema *schema = NULL;
    if (OB_FAIL(mgr->synonym_mgr_.get_synonym_schema_with_name(tenant_id,
                                                               database_id,
                                                               synonym_name,
                                                               schema))) {
      LOG_WARN("get outline schema failed", KR(ret),
               K(tenant_id), K(database_id), K(synonym_name));
    } else if (NULL != schema) {
      exist = true;
      synonym_id = schema->get_synonym_id();
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_object_with_synonym(const uint64_t tenant_id,
                                                 const uint64_t database_id,
                                                 const ObString &name,
                                                 uint64_t &obj_database_id,
                                                 uint64_t &synonym_id,
                                                 ObString &obj_table_name,
                                                 bool &do_exist,
                                                 bool search_public_schema,
                                                 bool *is_public) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  do_exist = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->synonym_mgr_.get_object(tenant_id, database_id, name, obj_database_id,
                                       synonym_id, obj_table_name, do_exist, search_public_schema,
                                       is_public);
  }
  return ret;
}

int ObSchemaGetterGuard::get_sequence_schema(
    const uint64_t tenant_id,
    const uint64_t sequence_id,
    const ObSequenceSchema *&sequence_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  sequence_schema = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == sequence_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(sequence_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_NOT_NULL(mgr)) {
     if (OB_FAIL(mgr->get_sequence_schema(tenant_id, sequence_id, sequence_schema))) {
       LOG_WARN("fail to get sequence schema", KR(ret), K(tenant_id), K(tenant_id));
     }
  } else {
    if (OB_FAIL(get_schema(SEQUENCE_SCHEMA,
                           tenant_id,
                           sequence_id,
                           sequence_schema))) {
      LOG_WARN("get sequence schema failed", K(tenant_id), K(sequence_id), KR(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(sequence_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("sequence schema not exists", KR(ret), K(tenant_id), K(sequence_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sequence_schema_with_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &sequence_name,
    const ObSequenceSchema *&sequence_schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             ||OB_INVALID_ID == database_id
             || sequence_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sequence_mgr_.get_sequence_schema_with_name(tenant_id,
                                                               database_id,
                                                               sequence_name,
                                                               sequence_schema))) {
    LOG_WARN("get schema failed", KR(ret),
             K(tenant_id), K(database_id), K(sequence_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_policy_schema_by_id(const uint64_t tenant_id,
    const uint64_t label_se_policy_id, const ObLabelSePolicySchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == label_se_policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(label_se_policy_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_label_se_policy_schema(
                     tenant_id, label_se_policy_id, schema))) {
    LOG_WARN("get schema failed", K(tenant_id), K(label_se_policy_id), KR(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("NULL ptr", KR(ret), K(schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_policy_schema_by_name(
    const uint64_t tenant_id,
    const ObString &policy_name,
    const ObLabelSePolicySchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;

  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->label_se_policy_mgr_.get_schema_by_name(tenant_id, policy_name, schema);
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_policy_schema_by_column_name(
        const uint64_t tenant_id,
        const ObString &column_name,
        const ObLabelSePolicySchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;

  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ret = mgr->label_se_policy_mgr_.get_schema_by_column_name(tenant_id, column_name, schema);
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_component_schema_by_id(
        const uint64_t tenant_id,
        const uint64_t label_se_comp_id,
        const ObLabelSeComponentSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == label_se_comp_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(label_se_comp_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_label_se_component_schema(
                     tenant_id, label_se_comp_id, schema))) {
    LOG_WARN("get schema failed", K(tenant_id), K(label_se_comp_id), KR(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("NULL ptr", KR(ret), K(schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_label_schema_by_id(
        const uint64_t tenant_id,
        const uint64_t label_se_label_id,
        const ObLabelSeLabelSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == label_se_label_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(label_se_label_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_label_se_label_schema(tenant_id, label_se_label_id, schema))) {
    LOG_WARN("get schema failed", K(tenant_id), K(label_se_label_id), KR(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("NULL ptr", KR(ret), K(schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_user_level_by_id(const uint64_t tenant_id,
                                                       const uint64_t user_id,
                                                       const uint64_t policy_id,
                                                       const ObLabelSeUserLevelSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == user_id
             || OB_INVALID_ID == policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(user_id), K(policy_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->label_se_user_level_mgr_.get_schema_by_user_policy_id(tenant_id,
                                                                                user_id,
                                                                                policy_id,
                                                                                schema))) {
     LOG_WARN("get schema failed", KR(ret), K(tenant_id), K(user_id), K(policy_id));
   }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_label_schema_by_name(const uint64_t tenant_id,
                                                           const ObString &short_name,
                                                           const ObLabelSeLabelSchema *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->label_se_label_mgr_.get_schema_by_label(tenant_id,
                                                                  short_name,
                                                                  schema))) {
     LOG_WARN("get schema failed", KR(ret), K(tenant_id));
   }
  return ret;
}

int ObSchemaGetterGuard::get_outline_info_with_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &name,
    const ObOutlineInfo *&outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema *simple_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_name(tenant_id,
      database_id, name, simple_outline))) {
    LOG_WARN("get simple outline failed", KR(ret), K(tenant_id), K(database_id), K(name));
  } else if (NULL == simple_outline) {
    LOG_INFO("outline not exist", K(tenant_id), K(database_id), K(name));
  } else if (OB_FAIL(get_schema(OUTLINE_SCHEMA,
                                simple_outline->get_tenant_id(),
                                simple_outline->get_outline_id(),
                                outline_info,
                                simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id), KPC(simple_outline));
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(outline_info));
  }

  return ret;
}

int ObSchemaGetterGuard::get_outline_info_with_name(
    const uint64_t tenant_id,
    const ObString &db_name,
    const ObString &outline_name,
    const ObOutlineInfo *&outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema *simple_outline = NULL;
  uint64_t database_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || db_name.empty()
             || outline_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(db_name), K(outline_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_database_id(tenant_id, db_name, database_id)))  {
    LOG_WARN("get database id failed", KR(ret));
  } else if (OB_INVALID_ID == database_id) {
    // do-nothing
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_name(tenant_id,
      database_id, outline_name, simple_outline))) {
    LOG_WARN("get simple outline failed", KR(ret), K(tenant_id), K(database_id), K(outline_name));
  } else if (NULL == simple_outline) {
    LOG_TRACE("outline not exist", K(tenant_id), K(database_id), K(outline_name));
  } else if (OB_FAIL(get_schema(OUTLINE_SCHEMA,
                                simple_outline->get_tenant_id(),
                                simple_outline->get_outline_id(),
                                outline_info,
                                simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id), KPC(simple_outline));
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(outline_info));
  } else {/*do nothing*/}

  return ret;
}
int ObSchemaGetterGuard::get_outline_info_with_signature(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &signature,
    const ObOutlineInfo *&outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema *simple_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(signature), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_signature(tenant_id,
      database_id, signature, simple_outline))) {
    LOG_WARN("get simple outline failed", KR(ret), K(tenant_id), K(database_id), K(signature));
  } else if (NULL == simple_outline) {
    LOG_TRACE("outline not exist", K(tenant_id), K(database_id), K(signature));
  } else if (OB_FAIL(get_schema(OUTLINE_SCHEMA,
                                simple_outline->get_tenant_id(),
                                simple_outline->get_outline_id(),
                                outline_info,
                                simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id), KPC(simple_outline));
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(outline_info));
  }

  return ret;
}

int ObSchemaGetterGuard::check_routine_exist(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                                             const ObString &routine_name, uint64_t overload,
                                             ObRoutineType routine_type, bool &exist) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || routine_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(routine_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleRoutineSchema *schema = NULL;
    if (OB_FAIL(mgr->routine_mgr_.get_routine_schema(tenant_id, database_id, package_id,
                                                      routine_name, overload, routine_type, schema))) {
      LOG_WARN("get routine schema failed", KR(ret), K(tenant_id), K(database_id), K(routine_name), K(routine_type));
    } else if (NULL != schema) {
      exist = true;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_package_exist(uint64_t tenant_id, uint64_t database_id,
                                             const common::ObString &package_name,
                                             ObPackageType package_type,
                                             int64_t compatible_mode, bool &exist) {
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || package_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(package_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimplePackageSchema *schema = NULL;
    if (OB_FAIL(mgr->package_mgr_.get_package_schema(tenant_id, database_id, package_name, package_type, compatible_mode, schema))) {
      LOG_WARN("get package schema failed", KR(ret), K(tenant_id), K(database_id), K(package_name), K(package_type));
    } else if (NULL != schema) {
      exist = true;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_package_id(uint64_t tenant_id, uint64_t database_id,
                                        const ObString &package_name, ObPackageType type,
                                        int64_t compatible_mode, uint64_t &package_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimplePackageSchema *schema = NULL;
  package_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || package_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(package_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->package_mgr_.get_package_schema(tenant_id, database_id, package_name, type, compatible_mode, schema))) {
    LOG_WARN("get package schema failed", KR(ret), K(tenant_id), K(database_id), K(package_name));
  } else if (NULL != schema) {
    package_id = schema->get_package_id();
  }
  return ret;
}

int ObSchemaGetterGuard::get_routine_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                                        const ObString &routine_name, uint64_t overload,
                                        ObRoutineType routine_type, uint64_t &routine_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_id = OB_INVALID_ID;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || routine_name.empty()
             || (overload == OB_INVALID_INDEX)
             || (INVALID_ROUTINE_TYPE == routine_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(routine_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleRoutineSchema *schema = NULL;
    if (OB_FAIL(mgr->routine_mgr_.get_routine_schema(tenant_id, database_id, package_id,
                                                      routine_name, overload, routine_type, schema))) {
      LOG_WARN("get routine schema failed", KR(ret), K(tenant_id), K(database_id), K(package_id),
               K(routine_name), K(overload), K(routine_type));
    } else if (NULL != schema) {
      routine_id = schema->get_routine_id();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_routine_info(
    const uint64_t tenant_id, const uint64_t database_id, const uint64_t package_id,
    const ObString &routine_name, uint64_t overload,
    ObRoutineType routine_type, const ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_info = NULL;

  const ObSimpleRoutineSchema *simple_routine = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if ((OB_INVALID_ID == tenant_id)
      || (OB_INVALID_ID == database_id)
      || routine_name.empty()
      || (overload == OB_INVALID_INDEX)
      || (INVALID_ROUTINE_TYPE == routine_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(package_id), K(routine_name),
             K(overload), K(routine_type), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schema(tenant_id, database_id, package_id,
                                                           routine_name, overload, routine_type, simple_routine))) {
    LOG_WARN("get simple routine schema failed", KR(ret), K(tenant_id), K(database_id),
             K(package_id), K(routine_name), K(overload), K(routine_type));
  } else if (NULL == simple_routine) {
    LOG_TRACE("routine not exist", K(tenant_id), K(database_id), K(routine_name));
  } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                simple_routine->get_tenant_id(),
                                simple_routine->get_routine_id(),
                                routine_info,
                                simple_routine->get_schema_version()))) {
    LOG_WARN("get routine schema failed", KR(ret), K(tenant_id), KPC(simple_routine));
  } else if (OB_ISNULL(routine_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(routine_info));
  } else {/*do nothing*/}
  return ret;
}

int ObSchemaGetterGuard::get_routine_info(
    const uint64_t tenant_id,
    const uint64_t routine_id,
    const ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleRoutineSchema *simple_routine = NULL;
  routine_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner state error", KR(ret));
  } else if (OB_UNLIKELY(routine_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(routine_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_routine_schema(tenant_id, routine_id, simple_routine))) {
    LOG_WARN("get simple routine schema failed", KR(ret), K(tenant_id), K(routine_id));
  } else if (NULL == simple_routine) {
    LOG_TRACE("routine not exist", K(routine_id));
  } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                simple_routine->get_tenant_id(),
                                simple_routine->get_routine_id(),
                                routine_info,
                                simple_routine->get_schema_version()))) {
    LOG_WARN("get routine schema failed", KR(ret), K(tenant_id), KPC(simple_routine));
  }
  return ret;
}

int ObSchemaGetterGuard::get_udt_routine_infos(uint64_t tenant_id,
                                               uint64_t database_id,
                                               uint64_t udt_id,
                                               const common::ObString &routine_name,
                                               ObRoutineType routine_type,
                            common::ObIArray<const ObIRoutineInfo *> &routine_infos,
                            ObRoutineType inside_routine_type)
{
  return get_package_routine_infos(tenant_id, database_id,
                                   udt_id, routine_name,
                                   routine_type, routine_infos,
                                   inside_routine_type);
}

int ObSchemaGetterGuard::get_package_routine_infos(uint64_t tenant_id,
  uint64_t database_id, uint64_t package_id, const common::ObString &routine_name,
  ObRoutineType routine_type, common::ObIArray<const ObIRoutineInfo *> &routine_infos,
  ObRoutineType inside_routine_type)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleRoutineSchema *simple_routine = NULL;
  routine_infos.reset();
  ObArray<const ObSimpleRoutineSchema *> simple_routines;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if ((OB_INVALID_ID == tenant_id)
      || (OB_INVALID_ID == database_id)
      || (OB_INVALID_ID == package_id)
      || routine_name.empty()
      || (ROUTINE_PROCEDURE_TYPE != routine_type && ROUTINE_FUNCTION_TYPE != routine_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(package_id),
                                               K(routine_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schema(tenant_id, database_id, package_id,
                                                 routine_name, 0,
                                                 inside_routine_type, simple_routine))) {
    LOG_WARN("get simple routine schema failed", KR(ret), K(tenant_id), K(database_id),
             K(package_id), K(routine_name));
  } else if (NULL != simple_routine) {
    if (OB_FAIL(simple_routines.push_back(simple_routine))) {
      LOG_WARN("push back schema failed", KR(ret));
    }
  } else {
    bool end_loop = false;
    for (int i=1; OB_SUCC(ret) && !end_loop; i++) {
      if (OB_FAIL(mgr->routine_mgr_.get_routine_schema(tenant_id, database_id, package_id,
                                                 routine_name, i,
                                                 inside_routine_type, simple_routine))) {
        LOG_WARN("get simple routine schema failed", KR(ret), K(tenant_id), K(database_id),
                 K(package_id), K(routine_name));
      } else if (NULL != simple_routine) {
        if (OB_FAIL(simple_routines.push_back(simple_routine))) {
          LOG_WARN("push back schema failed", KR(ret));
        }
      } else {
        end_loop = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(simple_routine, simple_routines, OB_SUCC(ret)) {
      const ObSimpleRoutineSchema *tmp_schema = *simple_routine;
      const ObRoutineInfo *schema = NULL;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret));
      } else if (OB_FAIL(get_schema(ROUTINE_SCHEMA,
                                    tmp_schema->get_tenant_id(),
                                    tmp_schema->get_routine_id(),
                                    schema,
                                    tmp_schema->get_schema_version()))) {
        LOG_WARN("get routine schema failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(schema));
      } else {
        if (ROUTINE_PROCEDURE_TYPE == routine_type) {
          if (schema->is_procedure()) {
            if (OB_FAIL(routine_infos.push_back(schema))) {
              LOG_WARN("push back schema failed", KR(ret));
            }
          }
        } else {  //ROUTINE_FUNCTION_TYPE
          if (schema->is_function()) {
            if (OB_FAIL(routine_infos.push_back(schema))) {
              LOG_WARN("push back schema failed", KR(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::get_package_info(
    const uint64_t tenant_id,
    const uint64_t package_id,
    const ObPackageInfo *&package_info)
{
  int ret = OB_SUCCESS;
  if (!ObTriggerInfo::is_trigger_package_id(package_id)) {
    const ObSchemaMgr *mgr = NULL;
    const ObSimplePackageSchema *simple_package = NULL;
    package_info = NULL;
    if (!check_inner_stat()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("inner state error", KR(ret));
    } else if (OB_UNLIKELY(package_id == OB_INVALID_ID)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(package_id), KR(ret));
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_package_schema(tenant_id, package_id, simple_package))) {
      LOG_WARN("get simple package schema failed", KR(ret), K(tenant_id), K(package_id));
    } else if (NULL == simple_package) {
      LOG_TRACE("package not exist", K(package_id));
    } else if (OB_FAIL(get_schema(PACKAGE_SCHEMA,
                                  simple_package->get_tenant_id(),
                                  simple_package->get_package_id(),
                                  package_info,
                                  simple_package->get_schema_version()))) {
      LOG_WARN("get package schema failed", KR(ret), K(tenant_id), KPC(simple_package));
    }
  } else {
    if (OB_FAIL(get_package_info_from_trigger(tenant_id, package_id, package_info))) {
      LOG_WARN("failed to get package info from trigger", KR(ret), K(tenant_id), K(package_id));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_package_info(
    const uint64_t tenant_id,
    const uint64_t package_id,
    const ObSimplePackageSchema *&package_info)
{
  int ret = OB_SUCCESS;
  package_info = NULL;
  if (!ObTriggerInfo::is_trigger_package_id(package_id)) {
    const ObSchemaMgr *mgr = NULL;
    if (!check_inner_stat()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("inner state error", KR(ret));
    } else if (OB_UNLIKELY(package_id == OB_INVALID_ID)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(package_id), KR(ret));
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_package_schema(tenant_id, package_id, package_info))) {
      LOG_WARN("get simple package schema failed", KR(ret), K(tenant_id), K(package_id));
    } else if (NULL == package_info) {
      LOG_TRACE("package not exist", K(package_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get simple package info error", KR(ret), K(package_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_trigger_schema(
    const uint64_t tenant_id,
    const uint64_t trigger_id,
    const ObSimpleTriggerSchema *&simple_trigger)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner state error", KR(ret));
  } else if (OB_UNLIKELY(trigger_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(trigger_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("failed to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_trigger_schema(tenant_id, trigger_id, simple_trigger))) {
    LOG_WARN("failed to get simple trigger schema", KR(ret), K(tenant_id), K(trigger_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_trigger_schema(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &trigger_name,
    const ObSimpleTriggerSchema *&simple_trigger)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner state error", KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("failed to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->trigger_mgr_.get_trigger_schema(tenant_id, database_id,
                                                          trigger_name, simple_trigger))) {
    LOG_WARN("failed to get simple trigger schema", KR(ret),
             K(tenant_id), K(database_id), K(trigger_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_trigger_info(const uint64_t tenant_id,
                                          const uint64_t trigger_id,
                                          const ObTriggerInfo *&trigger_info)
{
  int ret = OB_SUCCESS;
  const ObSimpleTriggerSchema *simple_trigger = NULL;
  if (OB_FAIL(get_simple_trigger_schema(tenant_id, trigger_id, simple_trigger))) {
    LOG_WARN("failed to get simple trigger schema", KR(ret), K(tenant_id), K(trigger_id));
  } else if (NULL == simple_trigger) {
    trigger_info = NULL;
    LOG_TRACE("trigger not exist", K(trigger_id));
  } else if (OB_FAIL(get_schema(TRIGGER_SCHEMA,
                                simple_trigger->get_tenant_id(),
                                simple_trigger->get_trigger_id(),
                                trigger_info,
                                simple_trigger->get_schema_version()))) {
    LOG_WARN("get trigger schema failed", KR(ret), K(tenant_id), KPC(simple_trigger));
  }
  return ret;
}

int ObSchemaGetterGuard::get_trigger_info(const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const ObString &trigger_name,
                                          const ObTriggerInfo *&trigger_info)
{
  int ret = OB_SUCCESS;
  const ObSimpleTriggerSchema *simple_trigger = NULL;
  if (OB_FAIL(get_simple_trigger_schema(tenant_id, database_id, trigger_name, simple_trigger))) {
    LOG_WARN("failed to get simple trigger schema", KR(ret),
             K(tenant_id), K(database_id), K(trigger_name));
  } else if (NULL == simple_trigger) {
    trigger_info = NULL;
    LOG_TRACE("trigger not exist", K(tenant_id), K(database_id), K(trigger_name));
  } else if (OB_FAIL(get_schema(TRIGGER_SCHEMA,
                                simple_trigger->get_tenant_id(),
                                simple_trigger->get_trigger_id(),
                                trigger_info,
                                simple_trigger->get_schema_version()))) {
    LOG_WARN("get trigger schema failed", KR(ret), K(tenant_id), KPC(simple_trigger));
  }
  return ret;
}

int ObSchemaGetterGuard::get_package_info_from_trigger(
    const uint64_t tenant_id,
    const uint64_t package_id,
    const ObPackageInfo *&package_info)
{
  int ret = OB_SUCCESS;
  uint64_t trigger_id = ObTriggerInfo::get_package_trigger_id(package_id);
  const ObTriggerInfo *trigger_info = NULL;
  if (OB_FAIL(get_trigger_info(tenant_id, trigger_id, trigger_info))) {
    LOG_WARN("failed to get trigger info", KR(ret), K(tenant_id), K(trigger_id));
  } else if (OB_ISNULL(trigger_info)) {
    package_info = NULL;
    LOG_TRACE("trigger not exist", K(trigger_id));
  } else {
    package_info = !ObTriggerInfo::is_trigger_body_package_id(package_id) ?
                     &trigger_info->get_package_spec_info() :
                     &trigger_info->get_package_body_info();
  }
  return ret;
}

int ObSchemaGetterGuard::get_package_info_from_trigger(
    const uint64_t tenant_id,
    const uint64_t package_id,
    const ObPackageInfo *&package_spec_info,
    const ObPackageInfo *&package_body_info)
{
  int ret = OB_SUCCESS;
  uint64_t trigger_id = ObTriggerInfo::get_package_trigger_id(package_id);
  const ObTriggerInfo *trigger_info = NULL;
  if (OB_FAIL(get_trigger_info(tenant_id, trigger_id, trigger_info))) {
    LOG_WARN("failed to get trigger info", KR(ret), K(tenant_id), K(trigger_id));
  } else if (OB_ISNULL(trigger_info)) {
    package_spec_info = NULL;
    package_body_info = NULL;
    LOG_TRACE("trigger not exist", K(trigger_id));
  } else {
    package_spec_info = &trigger_info->get_package_spec_info();
    package_body_info = &trigger_info->get_package_body_info();
  }
  return ret;
}

int ObSchemaGetterGuard::get_package_id_from_trigger(uint64_t tenant_id,
                                                     uint64_t database_id,
                                                     const ObString &package_name,
                                                     ObPackageType package_type,
                                                     uint64_t &package_id)
{
  int ret = OB_SUCCESS;
  const ObSimpleTriggerSchema *simple_trigger = NULL;
  if (OB_FAIL(get_simple_trigger_schema(tenant_id, database_id, package_name, simple_trigger))) {
    LOG_WARN("failed to get simple trigger schema", KR(ret),
             K(tenant_id), K(database_id), K(package_name));
  } else if (NULL == simple_trigger) {
    package_id = OB_INVALID_ID;
    LOG_TRACE("trigger not exist", K(tenant_id), K(database_id), K(package_name));
  } else {
    package_id = (PACKAGE_TYPE == package_type) ?
                   ObTriggerInfo::get_trigger_spec_package_id(simple_trigger->get_trigger_id()) :
                   ObTriggerInfo::get_trigger_body_package_id(simple_trigger->get_trigger_id());
  }
  return ret;
}

int ObSchemaGetterGuard::get_outline_info_with_sql_id(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &sql_id,
    const ObOutlineInfo *&outline_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  outline_info = NULL;

  const ObSimpleOutlineSchema *simple_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || sql_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(sql_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->outline_mgr_.get_outline_schema_with_sql_id(tenant_id,
      database_id, sql_id, simple_outline))) {
    LOG_WARN("get simple outline failed", KR(ret), K(tenant_id), K(database_id), K(sql_id));
  } else if (NULL == simple_outline) {
    LOG_DEBUG("outline not exist", K(tenant_id), K(database_id), K(sql_id));
  } else if (OB_FAIL(get_schema(OUTLINE_SCHEMA,
                                simple_outline->get_tenant_id(),
                                simple_outline->get_outline_id(),
                                outline_info,
                                simple_outline->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id), KPC(simple_outline));
  } else if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(outline_info));
  }
  return ret;
}

int ObSchemaGetterGuard::get_package_info(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &package_name,
    ObPackageType package_type,
    int64_t compatible_mode,
    const ObPackageInfo *&package_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimplePackageSchema *simple_package = NULL;
  package_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == database_id)
      || OB_UNLIKELY(package_name.empty())
      || OB_UNLIKELY(package_type == INVALID_PACKAGE_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(package_name), K(package_type), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->package_mgr_.get_package_schema(tenant_id, database_id, package_name, package_type, compatible_mode, simple_package))) {
    LOG_WARN("get simple package schema failed", KR(ret), K(tenant_id), K(database_id), K(package_name), K(package_type));
  } else if (NULL == simple_package) {
    LOG_DEBUG("package not exist", K(tenant_id), K(database_id), K(package_name));
  } else if (OB_FAIL(get_schema(PACKAGE_SCHEMA,
                                simple_package->get_tenant_id(),
                                simple_package->get_package_id(),
                                package_info,
                                simple_package->get_schema_version()))) {
    LOG_WARN("get package schema failed", KR(ret), K(tenant_id), KPC(simple_package));
  } else if (OB_ISNULL(package_info)) {
    LOG_DEBUG("NULL ptr", KR(ret), KP(package_info));
  } else {/*do nothing*/}
  return ret;
}

int ObSchemaGetterGuard::check_udt_exist(uint64_t tenant_id, uint64_t database_id,
                                         uint64_t package_id, ObUDTTypeCode type_code,
                                         const ObString &udt_name, bool &exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleUDTSchema *schema = NULL;
  const ObUDTTypeInfo *udt_info = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || udt_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(udt_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->udt_mgr_.get_udt_schema(tenant_id, database_id, package_id,
                                                  udt_name, schema))) {
    LOG_WARN("get routine schema failed", KR(ret), K(tenant_id), K(database_id), K(udt_name));
  } else if (OB_ISNULL(schema)) {
    exist = false;
  } else if (UDT_TYPE_OBJECT_BODY != type_code) {
    exist = true;
  } else if (OB_FAIL(get_schema(UDT_SCHEMA,
                                schema->get_tenant_id(),
                                schema->get_udt_id(),
                                udt_info,
                                schema->get_schema_version()))) {
    LOG_WARN("get udt schema failed", KR(ret), K(tenant_id), KPC(schema));
  } else if (OB_ISNULL(udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(udt_info));
  } else {
    if (2 == udt_info->get_object_type_infos().count()) {
      if (udt_info->is_object_type_legal()) {
         exist = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("illegal object type which has object body", K(*udt_info),
                                                              K(udt_name), K(type_code));
      }
    } else {
      exist = false;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_udt_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                                    const ObString &udt_name, uint64_t &udt_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleUDTSchema *schema = NULL;
  udt_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || udt_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(udt_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->udt_mgr_.get_udt_schema(tenant_id, database_id, package_id,
                                                  udt_name,
                                                  schema))) {
    LOG_WARN("get udt schema failed",
              KR(ret), K(tenant_id), K(database_id), K(package_id), K(udt_name));
  } else if (NULL != schema) {
    udt_id = schema->get_type_id();
  }
  return ret;
}

int ObSchemaGetterGuard::get_udt_info(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const uint64_t package_id,
    const ObString &udt_name,
    const ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  OZ (get_udt_info(tenant_id, database_id,
                   package_id,
                   udt_name,
                   share::schema::ObUDTTypeCode::UDT_TYPE_OBJECT,
                   udt_info));
  return ret;
}

int ObSchemaGetterGuard::get_udt_info(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const uint64_t package_id,
    const common::ObString &udt_name,
    const share::schema::ObUDTTypeCode &type_code,
    const ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  udt_info = NULL;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleUDTSchema *simple_udt = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if ((OB_INVALID_ID == tenant_id)
      || (OB_INVALID_ID == database_id)
      || udt_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(database_id), K(package_id), K(udt_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->udt_mgr_.get_udt_schema(tenant_id, database_id, package_id,
                                                  udt_name, simple_udt))) {
    LOG_WARN("get simple udt schema failed", KR(ret), K(tenant_id), K(database_id),
             K(package_id), K(udt_name));
  } else if (NULL == simple_udt) {
    LOG_TRACE("udt not exist", K(tenant_id), K(database_id), K(udt_name));
  } else if (OB_FAIL(get_schema(UDT_SCHEMA,
                                simple_udt->get_tenant_id(),
                                simple_udt->get_udt_id(),
                                udt_info,
                                simple_udt->get_schema_version()))) {
    LOG_WARN("get udt schema failed", KR(ret), K(tenant_id), KPC(simple_udt));
  } else if (OB_ISNULL(udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(udt_info));
  } else if (UDT_TYPE_OBJECT_BODY == type_code && !udt_info->has_type_body()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("want to get object type body, but no body info found", KR(ret),
                                                                K(udt_name),
                                                                K(package_id),
                                                                K(*udt_info));
  } else {/*do nothing*/}
  return ret;
}

int ObSchemaGetterGuard::get_udt_info(
    const uint64_t tenant_id,
    const uint64_t udt_id,
    const ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObSimpleUDTSchema *simple_udt = NULL;
  udt_info = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner state error", KR(ret));
  } else if (OB_UNLIKELY(udt_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(udt_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(udt_id), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_udt_schema(tenant_id, udt_id, simple_udt))) {
    LOG_WARN("get simple udt schema failed", KR(ret), K(tenant_id), K(udt_id));
  } else if (NULL == simple_udt) {
    LOG_TRACE("udt not exist", K(udt_id));
  } else if(OB_FAIL(get_schema(UDT_SCHEMA,
                               simple_udt->get_tenant_id(),
                               simple_udt->get_type_id(),
                               udt_info,
                               simple_udt->get_schema_version()))) {
      LOG_WARN("get udt schema failed", KR(ret), K(tenant_id),  KPC(simple_udt));
  } else if (OB_ISNULL(udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null udt info, unexpeced", KR(ret), K(udt_id));
  } else if (!udt_info->is_object_type_legal()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal udt type", KR(ret), K(udt_id), K(*udt_info));
  } else {
    // do nothing
  }
  return ret;
}

int ObSchemaGetterGuard::check_user_exist(const uint64_t tenant_id,
                                          const ObString &user_name,
                                          const ObString &host_name,
                                          bool &is_exist,
                                          uint64_t *user_id/*=NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != user_id) {
    *user_id = OB_INVALID_ID;
  }

  uint64_t tmp_user_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_user_id(tenant_id, user_name, host_name, tmp_user_id))) {
    LOG_WARN("check user exist failed", KR(ret), K(tenant_id), K(user_name), K(host_name));
  } else if (OB_INVALID_ID != tmp_user_id) {
    is_exist = true;
    if (NULL != user_id) {
      *user_id = tmp_user_id;
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_user_exist(const uint64_t tenant_id,
                                          const uint64_t user_id,
                                          bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_FAIL(get_schema_version(
             USER_SCHEMA, tenant_id, user_id, schema_version))) {
    LOG_WARN("check user exist failed", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_INVALID_VERSION != schema_version) {
    is_exist = true;
  }

  return ret;
}

int ObSchemaGetterGuard::check_database_exist(const uint64_t tenant_id,
                                              const common::ObString &database_name,
                                              bool &is_exist,
                                              uint64_t *database_id/*= NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != database_id) {
    *database_id = OB_INVALID_ID;
  }

  uint64_t tmp_database_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_name));
  } else if (OB_FAIL(get_database_id(tenant_id, database_name, tmp_database_id))) {
    LOG_WARN("get database id failed", KR(ret), K(tenant_id), K(database_name));
  } else if (OB_INVALID_ID != tmp_database_id) {
    is_exist = true;
    if (NULL != database_id) {
      *database_id = tmp_database_id;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_database_in_recyclebin(
    const uint64_t tenant_id,
    const uint64_t database_id,
    bool &in_recyclebin)
{
  int ret = OB_SUCCESS;
  in_recyclebin = false;
  const ObDatabaseSchema *database_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(database_id), KR(ret));
  } else if (OB_FAIL(get_schema(DATABASE_SCHEMA,
                                tenant_id,
                                database_id,
                                database_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("database schema should not be null", KR(ret), K(tenant_id), K(database_id));
  } else {
    in_recyclebin = database_schema->is_in_recyclebin();
  }
  return ret;
}

int ObSchemaGetterGuard::check_database_exist(
    const uint64_t tenant_id,
    const uint64_t database_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(database_id));
  } else if (OB_FAIL(get_schema_version(
             DATABASE_SCHEMA, tenant_id, database_id, schema_version))) {
    LOG_WARN("get schema version failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }

  return ret;
}

int ObSchemaGetterGuard::check_tablegroup_exist(const uint64_t tenant_id,
                                                const common::ObString &tablegroup_name,
                                                bool &is_exist,
                                                uint64_t *tablegroup_id/*= NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != tablegroup_id) {
    *tablegroup_id = OB_INVALID_ID;
  }

  uint64_t tmp_tablegroup_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || tablegroup_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(get_tablegroup_id(tenant_id, tablegroup_name, tmp_tablegroup_id))) {
    LOG_WARN("get tablegroup id failed", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_INVALID_ID != tmp_tablegroup_id) {
    is_exist = true;
    if (NULL != tablegroup_id) {
      *tablegroup_id = tmp_tablegroup_id;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_tablegroup_exist(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablegroup_id));
  } else if (OB_FAIL(get_schema_version(
             TABLEGROUP_SCHEMA, tenant_id, tablegroup_id, schema_version))) {
    LOG_WARN("get schema version failed", KR(ret), K(tenant_id), K(tablegroup_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }

  return ret;
}

/* https://docs.oracle.com/cd/E18283_01/server.112/e17118/sql_elements008.htm
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
    const ObString &object_name, const ObSchemaType &schema_type, const ObRoutineType &routine_type,
    const bool is_or_replace, common::ObIArray<ObSchemaType> &conflict_schema_types)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  conflict_schema_types.reset();
  bool is_exist = false;

  if (OB_FAIL(get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", KR(ret), K(tenant_id), K(compat_mode));
  } else if (lib::Worker::CompatMode::ORACLE == compat_mode) {

    // table
    const ObSimpleTableSchemaV2 *table_schema = NULL;
    if (FAILEDx(get_simple_table_schema(
                tenant_id, db_id, object_name, false, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (NULL != table_schema) {
      if (TABLE_SCHEMA == schema_type && table_schema->is_view_table() && is_or_replace) {
        // create or replace view
      } else if (OB_FAIL(conflict_schema_types.push_back(TABLE_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
      }
    }

    // sequence
    is_exist = false;
    uint64_t sequence_id = OB_INVALID_ID;
    bool is_system_generated = false;
    if (FAILEDx(check_sequence_exist_with_name(
                tenant_id, db_id, object_name, is_exist, sequence_id, is_system_generated))) {
      LOG_WARN("fail to check sequence exist", KR(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (is_exist && OB_FAIL(conflict_schema_types.push_back(SEQUENCE_SCHEMA))) {
      LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
    }

    // synonym
    const ObSynonymInfo *synonym_info = NULL;
    if (FAILEDx(get_synonym_info(tenant_id, db_id, object_name, synonym_info))) {
      LOG_WARN("fail to get synonym info", KR(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (NULL != synonym_info) {
      if (SYNONYM_SCHEMA == schema_type && is_or_replace) {
        // create or replace synonym
      } else if (OB_FAIL(conflict_schema_types.push_back(SYNONYM_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
      }
    }

    // package
    const ObPackageInfo *package_info = NULL;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_package_info(tenant_id, db_id, object_name, PACKAGE_TYPE, COMPATIBLE_ORACLE_MODE, package_info))) {
        LOG_WARN("failed to get package info",
                 KR(ret), K(tenant_id), K(db_id), K(object_name));
      } else if (NULL != package_info) {
        if (PACKAGE_SCHEMA == schema_type && is_or_replace) {
          // create or replace package
        } else if (OB_FAIL(conflict_schema_types.push_back(PACKAGE_SCHEMA))) {
          LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
        }
      }
    }

    // standalone procedure
    is_exist = false;
    if (FAILEDx(check_standalone_procedure_exist(tenant_id, db_id, object_name, is_exist))) {
      LOG_WARN("failed to check procedure exist", KR(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (is_exist) {
      if (ROUTINE_SCHEMA == schema_type
          && ROUTINE_PROCEDURE_TYPE == routine_type && is_or_replace) {
        // create or replace standalone procedure
      } else if (OB_FAIL(conflict_schema_types.push_back(ROUTINE_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
      }
    }

    // standalone function
    is_exist = false;
    if (FAILEDx(check_standalone_function_exist(tenant_id, db_id, object_name, is_exist))) {
      LOG_WARN("failed to check procedure exist", KR(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (is_exist) {
      if (ROUTINE_SCHEMA == schema_type
          && ROUTINE_PROCEDURE_TYPE != routine_type && is_or_replace) {
        // create or replace standalone function
      } else if (OB_FAIL(conflict_schema_types.push_back(ROUTINE_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
      }
    }

    // udt
    is_exist = false;
    if (FAILEDx(check_udt_exist(tenant_id, db_id, OB_INVALID_ID,
                 ObUDTTypeCode::UDT_TYPE_OBJECT, object_name, is_exist))) {
      LOG_WARN("failed to check udt info exist", KR(ret), K(tenant_id), K(db_id), K(object_name));
    } else if (is_exist) {
      if (UDT_SCHEMA == schema_type && is_or_replace) {
        // create or replace udt
      } else if (OB_FAIL(conflict_schema_types.push_back(UDT_SCHEMA))) {
        LOG_WARN("fail to push back to conflict_schema_types", KR(ret));
      }
    }
  }

  return ret;
}


int ObSchemaGetterGuard::check_table_exist(const uint64_t tenant_id,
                                           const uint64_t database_id,
                                           const common::ObString &table_name,
                                           const bool is_index,
                                           const CheckTableType check_type,  // check if temporary table is visable
                                           bool &is_exist,
                                           uint64_t *table_id/*=NULL*/)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (NULL != table_id) {
    *table_id = OB_INVALID_ID;
  }

  uint64_t tmp_table_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(get_table_id(tenant_id, database_id, table_name, is_index, check_type, tmp_table_id))) {
    LOG_WARN("get database id failed", KR(ret), K(tenant_id), K(database_id),
             K(table_name), K(is_index));
  } else if (OB_INVALID_ID != tmp_table_id) {
    is_exist = true;
    if (NULL != table_id) {
      *table_id = tmp_table_id;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_table_exist(
    const uint64_t tenant_id,
    const uint64_t table_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  int64_t schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else if (is_cte_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_version(TABLE_SCHEMA, tenant_id, table_id, schema_version))) {
    LOG_WARN("get schema version failed", KR(ret), K(tenant_id), K(table_id));
  } else {
    is_exist = OB_INVALID_VERSION != schema_version;
  }

  return ret;
}

template <>
int ObSchemaGetterGuard::check_flashback_object_exist<ObTriggerInfo>(
    const ObTriggerInfo &object_schema,
    const ObString &object_name,
    bool &object_exist)
{
  int ret = OB_SUCCESS;
  const ObSimpleTriggerSchema *simple_trigger = NULL;
  OZ (get_simple_trigger_schema(object_schema.get_tenant_id(),
                                object_schema.get_database_id(),
                                object_name, simple_trigger),
      object_schema.get_trigger_id(), object_name);
  OX (object_exist = (NULL != simple_trigger))
  return ret;
}

/*
  interface for simple schema
*/

int ObSchemaGetterGuard::get_simple_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 * &table_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  table_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (is_cte_table(table_id)) {
    // fake table is only used in sql execution process and doesn't have schema.
    // We should avoid error in such situation.
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K(tenant_id_));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id), K(tenant_id_));
  } else if (OB_ISNULL(mgr)) {
    // lazy mode
    if (!ObSchemaService::g_liboblog_mode_) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("only for liboblog used", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(get_schema(TABLE_SIMPLE_SCHEMA,
                                  tenant_id,
                                  table_id,
                                  table_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
    }
  } else if (OB_FAIL(mgr->get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get simple table failed", KR(ret), K(tenant_id), K(tenant_id_), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    LOG_INFO("table not exist", K(tenant_id), K(tenant_id_), K(table_id));
  }
  return ret;
}

bool ObSchemaGetterGuard::is_tenant_schema_valid(const int64_t tenant_id) const
{
  bool bret = true;
  int tmp_ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_SUCCESS != (tmp_ret = get_schema_version(tenant_id, schema_version))) {
    LOG_WARN_RET(tmp_ret, "fail to get schema version", K(tmp_ret), K(tenant_id));
    bret = false;
  } else if (schema_version <= OB_CORE_SCHEMA_VERSION) {
    bret = false;
  }
  return bret;
}

int ObSchemaGetterGuard::get_tablegroup_schemas_in_tenant(const uint64_t tenant_id,
    common::ObIArray<const ObSimpleTablegroupSchema *> &tablegroup_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const int64_t DEFAULT_TABLEGROUP_NUM = 100;
  ObSEArray<const ObSimpleTablegroupSchema*, DEFAULT_TABLEGROUP_NUM> tmp_tablegroups;
  tablegroup_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schemas_in_tenant(tenant_id, tmp_tablegroups))) {
    LOG_WARN("fail to get tablegroup schemas in tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(simple_schema, tmp_tablegroups, OB_SUCC(ret)) {
      if (OB_ISNULL(*simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", KR(ret));
      } else if (OB_FAIL(tablegroup_schemas.push_back(*simple_schema))) {
        LOG_WARN("fail to push back tablegroup", KR(ret), KPC(*simple_schema));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tablegroup_schemas_in_tenant(const uint64_t tenant_id,
    common::ObIArray<const ObTablegroupSchema*> &tablegroup_schemas)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const int64_t DEFAULT_TABLEGROUP_NUM = 100;
  ObSEArray<const ObSimpleTablegroupSchema*, DEFAULT_TABLEGROUP_NUM> tmp_tablegroups;
  tablegroup_schemas.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schemas_in_tenant(tenant_id, tmp_tablegroups))) {
    LOG_WARN("fail to get tablegroup schemas in tenant", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(simple_schema, tmp_tablegroups, OB_SUCC(ret)) {
      const ObTablegroupSchema* schema = NULL;
      if (OB_ISNULL(*simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ptr", KR(ret));
      } else if (OB_FAIL(get_schema(TABLEGROUP_SCHEMA,
                                    (*simple_schema)->get_tenant_id(),
                                    (*simple_schema)->get_tablegroup_id(),
                                    schema,
                                    (*simple_schema)->get_schema_version()))) {
        LOG_WARN("fail to get schema", KR(ret), K(tenant_id), KPC(*simple_schema));
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ptr", KR(ret));
      } else if (OB_FAIL(tablegroup_schemas.push_back(schema))) {
        LOG_WARN("fail to push back tablegroup", KR(ret), KPC(schema));
      }
    }
  }
  return ret;
}
int ObSchemaGetterGuard::get_tablegroup_ids_in_tenant(const uint64_t tenant_id,
                                                      common::ObIArray<uint64_t> &tablegroup_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tablegroup_ids.reset();

  ObArray<const ObSimpleTablegroupSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    if (OB_TENANT_NOT_EXIST == ret) {
      ret = ignore_tenant_not_exist_error(tenant_id) ? OB_SUCCESS : ret;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tablegroup_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get tablegroup schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleTablegroupSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(tablegroup_ids.push_back(tmp_schema->get_tablegroup_id()))) {
        LOG_WARN("push back tablegroup id failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_routine_ids_in_tenant(const uint64_t tenant_id,
                                                   common::ObIArray<uint64_t> &routine_ids,
                                                   bool is_agent_mode)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  routine_ids.reset();

  ObArray<const ObSimpleRoutineSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->routine_mgr_.get_routine_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get routine schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleRoutineSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (is_agent_mode && tmp_schema->get_routine_type() == ObRoutineType::ROUTINE_PACKAGE_TYPE) {
        // do nothing ...
      } else if (OB_FAIL(routine_ids.push_back(tmp_schema->get_routine_id()))) {
        LOG_WARN("push back routine id failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_tablespace_ids_in_tenant(const uint64_t tenant_id,
                                                      common::ObIArray<uint64_t> &tablespace_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  tablespace_ids.reset();

  ObArray<const ObTablespaceSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->tablespace_mgr_.get_tablespace_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get tablespace schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObTablespaceSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(tablespace_ids.push_back(tmp_schema->get_tablespace_id()))) {
        LOG_WARN("push back tablespace id failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_package_ids_in_tenant(const uint64_t tenant_id,
                                                   common::ObIArray<uint64_t> &package_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  package_ids.reset();

  ObArray<const ObSimplePackageSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->package_mgr_.get_package_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get package schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimplePackageSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(package_ids.push_back(tmp_schema->get_package_id()))) {
        LOG_WARN("push back package id failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_udt_ids_in_tenant(const uint64_t tenant_id,
                                                   common::ObIArray<uint64_t> &udt_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  udt_ids.reset();

  ObArray<const ObSimpleUDTSchema*> schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->udt_mgr_.get_udt_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get udt schemas in tenant failed", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      const ObSimpleUDTSchema *tmp_schema = *schema;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KR(ret), KP(tmp_schema));
      } else if (OB_FAIL(udt_ids.push_back(tmp_schema->get_udt_id()))) {
        LOG_WARN("push back udt id failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_count(const uint64_t tenant_id,
                                          int64_t &schema_count)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema_count = 0;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_schema_count(schema_count))) {
    LOG_WARN("get_schema_count failed", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_size(const uint64_t tenant_id,
                                         int64_t &schema_size)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema_size = 0;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_schema_size(schema_size))) {
    LOG_WARN("get_schema_size failed", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_mv_ids(const uint64_t tenant_id, ObArray<uint64_t> &mv_ids) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  mv_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_tenant_mv_ids(tenant_id, mv_ids))) {
    LOG_WARN("Failed to get all_mv_ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::check_udf_exist_with_name(const uint64_t tenant_id,
                                                   const common::ObString &name,
                                                   bool &exist,
                                                   uint64_t &udf_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  udf_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleUDFSchema *schema = NULL;
    if (OB_FAIL(mgr->udf_mgr_.get_udf_schema_with_name(tenant_id,
                                                        name,
                                                        schema))) {
      LOG_WARN("get udf schema failed", KR(ret),
               K(tenant_id), K(name));
    } else if (OB_NOT_NULL(schema)) {
      exist = true;
      udf_id = schema->get_udf_id();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_udf_info(const uint64_t tenant_id,
                                      const common::ObString &name,
                                      const share::schema::ObUDF *&udf_info,
                                      bool &exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  udf_info = nullptr;
  exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSimpleUDFSchema *udf_schema = NULL;
    if (OB_FAIL(mgr->udf_mgr_.get_udf_schema_with_name(tenant_id,
                                                        name,
                                                        udf_schema))) {
      LOG_WARN("get outline schema failed", KR(ret),
               K(tenant_id), K(name));
    } else if (OB_ISNULL(udf_schema)) {
      LOG_INFO("udf not exist", K(tenant_id), K(name));
    } else if (OB_FAIL(get_schema(UDF_SCHEMA,
                                  udf_schema->get_tenant_id(),
                                  udf_schema->get_udf_id(),
                                  udf_info,
                                  udf_schema->get_schema_version()))) {
      LOG_WARN("get udf schema failed", KR(ret), K(tenant_id), KPC(udf_schema));
    } else if (OB_ISNULL(udf_info)) {
      LOG_INFO("udf does not exist", K(tenant_id), K(name), KR(ret));
    } else {
      exist = true;
    }
  }
  return ret;
}

// This function return indexes which are in unavaliable status
// It's used in the following scenes:
// 1. Schedule unavaliable indexes build tasks in primary cluster.
// 2. Drop unavaliable indexes when cluster switchover.
// 3. Rebuild unavaliable indexes in physical restore.
int ObSchemaGetterGuard::get_tenant_unavailable_index(const uint64_t tenant_id, common::ObIArray<uint64_t> &index_ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  index_ids.reset();
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", KR(ret));
        } else if (INDEX_STATUS_UNAVAILABLE == table_schema->get_index_status()
                   && table_schema->is_index_table()) {
          if (OB_FAIL(index_ids.push_back(table_schema->get_table_id()))) {
            LOG_WARN("fail to push back index id", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_unavailable_index_exist(
    const uint64_t tenant_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", KR(ret));
        } else if (INDEX_STATUS_UNAVAILABLE == table_schema->get_index_status()
                   && table_schema->is_index_table()) {
          exist = true;
          LOG_INFO("unavaliale index exist", KR(ret), "table_id", table_schema->get_table_id());
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_restore_error_index_exist(
    const uint64_t tenant_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", KR(ret));
        } else if (INDEX_STATUS_RESTORE_INDEX_ERROR == table_schema->get_index_status()
                   && table_schema->is_index_table()) {
          exist = true;
          LOG_INFO("restore error index exist", KR(ret), "table_id", table_schema->get_table_id());
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
  if (is_tenant_schema_guard()
      && OB_SYS_TENANT_ID != tenant_id
      && tenant_id_ != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("get other tenant schema with tenant schema guard not allowed",
             KR(ret), K(tenant_id), K(tenant_id_));
  }
  return ret;
}

int ObSchemaGetterGuard::check_sequence_exist_with_name(const uint64_t tenant_id,
                                                        const uint64_t database_id,
                                                        const ObString &sequence_name,
                                                        bool &exist,
                                                        uint64_t &sequence_id,
                                                        bool &is_system_generated) const
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             ||OB_INVALID_ID == database_id
             || sequence_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    const ObSequenceSchema *schema = NULL;
    if (OB_FAIL(mgr->sequence_mgr_.get_sequence_schema_with_name(tenant_id,
                                                                 database_id,
                                                                 sequence_name,
                                                                 schema))) {
      LOG_WARN("get schema failed", KR(ret),
               K(tenant_id), K(database_id), K(sequence_name));
    } else if (NULL != schema) {
      exist = true;
      sequence_id = schema->get_sequence_id();
      is_system_generated = schema->get_is_system_generated();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_context_exist_with_name(const uint64_t tenant_id,
                                                       const ObString &context_name,
                                                       const ObContextSchema *&context_schema,
                                                       bool &exist)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  context_schema = nullptr;
  const ObContextSchema *schema = NULL;
  if (OB_FAIL(get_context_schema_with_name(tenant_id, context_name, context_schema))) {
    LOG_WARN("failed to get context schema", KR(ret));
  } else if (OB_NOT_NULL(context_schema)) {
    exist = true;
  }
  return ret;
}

int ObSchemaGetterGuard::check_context_exist_by_id(const uint64_t tenant_id,
                                                   const uint64_t context_id,
                                                   const ObContextSchema *&context_schema,
                                                   bool &exist)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  context_schema = nullptr;
  const ObContextSchema *schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == context_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(context_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->context_mgr_.get_context_schema(tenant_id,
                                                          context_id,
                                                          context_schema))) {
    LOG_WARN("get schema failed", KR(ret),
              K(tenant_id), K(context_id));
  } else if (OB_NOT_NULL(context_schema)) {
    exist = true;
  }
  return ret;
}

int ObSchemaGetterGuard::get_context_schema_with_name(const uint64_t tenant_id,
                                                      const ObString &context_name,
                                                      const ObContextSchema *&context_schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || context_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(context_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->context_mgr_.get_context_schema_with_name(tenant_id,
                                                               context_name,
                                                               context_schema))) {
    LOG_WARN("get schema failed", KR(ret),
              K(tenant_id), K(context_name));
  }
  return ret;
}

// mock_fk_parent_table begin
int ObSchemaGetterGuard::get_mock_fk_parent_table_schemas_in_database(
    const uint64_t tenant_id,
    const uint64_t database_id,
    ObIArray<const ObMockFKParentTableSchema *> &full_schemas)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObSimpleMockFKParentTableSchema *> simple_schemas;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schemas_in_database(
                     tenant_id, database_id, simple_schemas))) {
    LOG_WARN("get schemas failed", K(ret), K(tenant_id), K(database_id));
  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < simple_schemas.count(); ++i) {
      const ObMockFKParentTableSchema *full_schema = NULL;
      if (OB_ISNULL(simple_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret));
      } else if (OB_FAIL(get_schema(MOCK_FK_PARENT_TABLE_SCHEMA,
                                    simple_schemas.at(i)->get_tenant_id(),
                                    simple_schemas.at(i)->get_mock_fk_parent_table_id(),
                                    full_schema,
                                    simple_schemas.at(i)->get_schema_version()))) {
        LOG_WARN("get mock fk parent table schema failed", K(ret), K(simple_schemas.at(i)));
      } else if (OB_FAIL(full_schemas.push_back(full_schema))) {
        LOG_WARN("add outline schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_mock_fk_parent_table_schema(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &name,
    const ObSimpleMockFKParentTableSchema *&schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schema_with_name(
                                                    tenant_id, database_id, name, schema))) {
    LOG_WARN("get schema failed", K(ret), K(tenant_id), K(database_id), K(name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_simple_mock_fk_parent_table_schema(
    const uint64_t tenant_id,
    const uint64_t mock_fk_parent_table_id,
    const ObSimpleMockFKParentTableSchema *&schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == mock_fk_parent_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(mock_fk_parent_table_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schema(
                     tenant_id, mock_fk_parent_table_id, schema))) {
    LOG_WARN("get schema failed", K(ret), K(tenant_id), K(mock_fk_parent_table_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_mock_fk_parent_table_schema_with_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &name,
    const ObMockFKParentTableSchema *&mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  const ObSimpleMockFKParentTableSchema *simple_mock_fk_parent_table = NULL;
  if (OB_FAIL(get_simple_mock_fk_parent_table_schema(tenant_id, database_id, name, simple_mock_fk_parent_table))) {
    LOG_WARN("failed to get simple mock_fk_parent_table schema", K(ret), K(tenant_id), K(database_id), K(name));
  } else if (NULL == simple_mock_fk_parent_table) {
    mock_fk_parent_table_schema = NULL;
    LOG_DEBUG("mock_fk_parent_table schema not exist", K(tenant_id), K(database_id), K(name));
  } else if (OB_FAIL(get_schema(MOCK_FK_PARENT_TABLE_SCHEMA, simple_mock_fk_parent_table->get_tenant_id(), simple_mock_fk_parent_table->get_mock_fk_parent_table_id(),
                                   mock_fk_parent_table_schema, simple_mock_fk_parent_table->get_schema_version()))) {
    LOG_WARN("get mock_fk_parent_table schema failed", K(ret), KPC(simple_mock_fk_parent_table));
  }
  return ret;
}

int ObSchemaGetterGuard::get_mock_fk_parent_table_schema_with_id(
    const uint64_t tenant_id,
    const uint64_t mock_fk_parent_table_id,
    const ObMockFKParentTableSchema *&mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  const ObSimpleMockFKParentTableSchema *simple_mock_fk_parent_table = NULL;
  if (OB_FAIL(get_simple_mock_fk_parent_table_schema(tenant_id, mock_fk_parent_table_id, simple_mock_fk_parent_table))) {
    LOG_WARN("failed to get simple trigger schema", K(ret), K(tenant_id), K(mock_fk_parent_table_id));
  } else if (NULL == simple_mock_fk_parent_table) {
    mock_fk_parent_table_schema = NULL;
    LOG_DEBUG("mock_fk_parent_table not exist", K(mock_fk_parent_table_id));
  } else if (OB_FAIL(get_schema(MOCK_FK_PARENT_TABLE_SCHEMA, simple_mock_fk_parent_table->get_tenant_id(), simple_mock_fk_parent_table->get_mock_fk_parent_table_id(),
                                   mock_fk_parent_table_schema, simple_mock_fk_parent_table->get_schema_version()))) {
    LOG_WARN("get mock_fk_parent_table schema failed", K(ret), KPC(simple_mock_fk_parent_table));
  }
  return ret;
}


// mock_fk_parent_table end

int ObSchemaGetterGuard::get_label_se_component_schema_by_short_name(const uint64_t tenant_id,
                                                                     const uint64_t label_se_policy_id,
                                                                     const int64_t comp_type,
                                                                     const ObString &short_name,
                                                                     const ObLabelSeComponentSchema *&schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || OB_INVALID_ID == label_se_policy_id
                         || comp_type < 0
                         || short_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(label_se_policy_id), K(comp_type), K(short_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(mgr->label_se_component_mgr_.get_schema_by_short_name(
                ObTenantLabelSePolicyId(tenant_id, label_se_policy_id),
                comp_type,
                short_name,
                schema))) {
      LOG_WARN("get schema failed", KR(ret),
               K(tenant_id), K(comp_type), K(short_name));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_component_schema_by_long_name(const uint64_t tenant_id,
                                                                     const uint64_t label_se_policy_id,
                                                                     const int64_t comp_type,
                                                                     const ObString &long_name,
                                                                     const ObLabelSeComponentSchema *&schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || OB_INVALID_ID == label_se_policy_id
                         || comp_type < 0
                         || long_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(label_se_policy_id), K(comp_type), K(long_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(mgr->label_se_component_mgr_.get_schema_by_long_name(
                ObTenantLabelSePolicyId(tenant_id, label_se_policy_id),
                comp_type,
                long_name,
                schema))) {
      LOG_WARN("get schema failed", KR(ret),
               K(tenant_id), K(comp_type), K(long_name));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_component_schema_by_comp_num(const uint64_t tenant_id,
                                                                   const uint64_t label_se_policy_id,
                                                                   const int64_t comp_type,
                                                                   const int64_t comp_num,
                                                                   const ObLabelSeComponentSchema *&schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || OB_INVALID_ID == label_se_policy_id
                         || comp_type < 0
                         || comp_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(label_se_policy_id), K(comp_type), K(comp_num));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(mgr->label_se_component_mgr_.get_schema_by_comp_num(
                ObTenantLabelSePolicyId(tenant_id, label_se_policy_id),
                comp_type,
                comp_num,
                schema))) {
      LOG_WARN("get schema failed", KR(ret),
               K(tenant_id), K(comp_type), K(comp_num));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_label_se_label_by_label_tag(const uint64_t tenant_id,
                                                         const int64_t label_tag,
                                                         const ObLabelSeLabelSchema *&schema)
{
  int ret= OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    schema = NULL;
    if (OB_FAIL(mgr->label_se_label_mgr_.get_schema_by_label_tag(tenant_id,
                                                                 label_tag,
                                                                 schema))) {
      LOG_WARN("get schema failed", KR(ret),
               K(tenant_id), K(label_tag));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_dblink_exist(const uint64_t tenant_id,
                                            const ObString &dblink_name,
                                            bool &exist) const
{
  int ret = OB_SUCCESS;
  uint64_t dblink_id = OB_INVALID_ID;
  exist = false;
  if (OB_FAIL(get_dblink_id(tenant_id, dblink_name, dblink_id))) {
    LOG_WARN("failed to get dblink id", KR(ret), K(tenant_id), K(dblink_name));
  } else {
    exist = (OB_INVALID_ID != dblink_id);
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_id(const uint64_t tenant_id,
                                       const ObString &dblink_name,
                                       uint64_t &dblink_id) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObDbLinkSchema *dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dblink_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("get dblink schema failed", KR(ret), K(tenant_id), K(dblink_name));
  } else if (OB_NOT_NULL(dblink_schema)) {
    dblink_id = dblink_schema->get_dblink_id();
  } else {
    dblink_id = OB_INVALID_ID;
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_user(const uint64_t tenant_id,
                                       const ObString &dblink_name,
                                       ObString &dblink_user,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObDbLinkSchema *dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dblink_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("get dblink schema failed", KR(ret), K(tenant_id), K(dblink_name));
  } else if (OB_NOT_NULL(dblink_schema)) {
    OZ (ob_write_string(allocator, dblink_schema->get_user_name(), dblink_user, true));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink user name is empty", K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_schema(const uint64_t tenant_id,
                                           const ObString &dblink_name,
                                           const ObDbLinkSchema *&dblink_schema) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dblink_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_name, dblink_schema))) {
    LOG_WARN("get dblink schema failed", KR(ret), K(tenant_id), K(dblink_name));
  }
  return ret;
}

int ObSchemaGetterGuard::get_dblink_schema(
    const uint64_t tenant_id,
    const uint64_t dblink_id,
    const ObDbLinkSchema *&dblink_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == dblink_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dblink_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_dblink_schema(tenant_id, dblink_id, dblink_schema))) {
    LOG_WARN("get dblink schema failed", KR(ret), K(tenant_id), K(dblink_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_link_table_schema(
    const uint64_t tenant_id,
    const uint64_t dblink_id,
    const ObString &database_name,
    const ObString &table_name,
    ObIAllocator &allocator,
    ObTableSchema *&table_schema,
    sql::ObSQLSessionInfo *session_info,
    const ObString &dblink_name,
    bool is_reverse_link,
    uint64_t *current_scn)
{
  int ret = OB_SUCCESS;
  const ObDbLinkSchema *dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("schema service is NULL", KR(ret));
  } else if (!is_reverse_link && OB_FAIL(get_dblink_schema(tenant_id, dblink_id, dblink_schema))) {
    LOG_WARN("get dblink schema failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->fetch_link_table_schema(dblink_schema,
                                                              database_name, table_name,
                                                              allocator, table_schema,
                                                              session_info, dblink_name,
                                                              is_reverse_link,
                                                              current_scn))) {
    LOG_WARN("get link table schema failed", KR(ret));
  }
  LOG_DEBUG("get link table schema", K(is_reverse_link), KP(dblink_schema), K(ret));
  return ret;
}

// only use in oracle mode
int ObSchemaGetterGuard::get_idx_schema_by_origin_idx_name(uint64_t tenant_id,
                                                           uint64_t database_id,
                                                           const common::ObString &index_name,
                                                           const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  table_schema = NULL;

  const ObSimpleTableSchemaV2 *simple_table = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id), K(index_name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_idx_schema_by_origin_idx_name(tenant_id,
                                                            database_id,
                                                            index_name,
                                                            simple_table))) {
    LOG_WARN("get simple table failed", KR(ret), K(tenant_id), K(database_id),
             K(index_name));
  } else if (NULL == simple_table) {
    LOG_INFO("table not exist", K(tenant_id), K(database_id), K(index_name));
  } else if (OB_FAIL(get_schema(TABLE_SCHEMA,
                                simple_table->get_tenant_id(),
                                simple_table->get_table_id(),
                                table_schema,
                                simple_table->get_schema_version()))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), KPC(simple_table));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(table_schema));
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode &compat_mode)
{
  return ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode);
}

int ObSchemaGetterGuard::get_schema_mgr(const uint64_t tenant_id, const ObSchemaMgr *&schema_mgr) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgrInfo *schema_mgr_info = NULL;
  schema_mgr = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_mgr_info(tenant_id, schema_mgr_info))) {
    LOG_WARN("fail to get schema_mgr_info", KR(ret), K(tenant_id), K(tenant_id_));
  } else {
    schema_mgr = schema_mgr_info->get_schema_mgr();
    if (OB_ISNULL(schema_mgr)) {
      LOG_TRACE("schema_mgr is null", K_(is_inited), K(tenant_id), K(tenant_id_), KPC(schema_mgr_info));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_mgr_info(const uint64_t tenant_id, const ObSchemaMgrInfo *&schema_mgr_info) const
{
  int ret = OB_SUCCESS;
  schema_mgr_info = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (schema_mgr_infos_.count() == 2) {
#define MATCH_TENANT_SCHEMA_MGR(x) if (schema_mgr_infos_[x].get_tenant_id() == tenant_id) { schema_mgr_info = &schema_mgr_infos_[x]; }
    MATCH_TENANT_SCHEMA_MGR(1) else MATCH_TENANT_SCHEMA_MGR(0);
    if (OB_ISNULL(schema_mgr_info)) {
      ret = OB_TENANT_NOT_EXIST;
    }
  } else {
    int64_t left = 0;
    int64_t right = schema_mgr_infos_.count() - 1;
    const ObSchemaMgrInfo *tmp_schema_mgr = NULL;
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

int ObSchemaGetterGuard::check_lazy_guard(const uint64_t tenant_id, const ObSchemaMgr *&mgr) const
{
  int ret = OB_SUCCESS;
  mgr = NULL;
  if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_schema_status(const uint64_t tenant_id, ObRefreshSchemaStatus &schema_status)
{
  int ret = OB_SUCCESS;
  schema_status.reset();
  const ObSchemaMgrInfo *schema_mgr_info = NULL;
  if (OB_FAIL(get_schema_mgr_info(tenant_id, schema_mgr_info))) {
    LOG_WARN("fail to get schema_mgr_info", KR(ret), K(tenant_id));
  } else {
    schema_status = schema_mgr_info->get_schema_status();
  }
  return ret;
}

int ObSchemaGetterGuard::check_tenant_is_restore(const uint64_t tenant_id, bool &is_restore)
{
  int ret = OB_SUCCESS;
  ObTenantStatus status;
  if (OB_FAIL(get_tenant_status(tenant_id, status))) {
    LOG_WARN("fail to get tenant status", KR(ret), K(tenant_id));
  } else {
    is_restore = is_tenant_restore(status);
  }
  return ret;
}

int ObSchemaGetterGuard::get_tenant_status(const uint64_t tenant_id, ObTenantStatus &status)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  status = TENANT_STATUS_MAX;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    status = TENANT_STATUS_NORMAL;
  } else if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    // lazy mode
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
    } else {
      status = tenant_schema->get_status();
    }
  } else {
    const ObSimpleTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_mgr->get_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
    } else {
      status = tenant_schema->get_status();
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool &is_dropped)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  ObDropTenantInfo drop_tenant_info;
  is_dropped = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", KR(ret));
  } else if (OB_FAIL(schema_mgr->get_drop_tenant_info(tenant_id, drop_tenant_info))) {
    LOG_WARN("fail to get drop tenant info", KR(ret), K(tenant_id));
  } else {
    is_dropped = (drop_tenant_info.is_valid());
  }
  return ret;
}

int ObSchemaGetterGuard::get_dropped_tenant_ids(common::ObIArray<uint64_t> &dropped_tenant_ids) const
{
  int ret = OB_SUCCESS;
  dropped_tenant_ids.reset();
  const ObSchemaMgr *schema_mgr = NULL;
  if (OB_FAIL(get_schema_mgr(OB_SYS_TENANT_ID, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", KR(ret));
  } else if (OB_FAIL(schema_mgr->get_drop_tenant_ids(dropped_tenant_ids))) {
    LOG_WARN("fail to get drop tenant ids", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::check_is_creating_standby_tenant(const uint64_t tenant_id, bool &is_creating_standby)
{
  int ret = OB_SUCCESS;
  ObTenantStatus status;
  if (OB_FAIL(get_tenant_status(tenant_id, status))) {
    LOG_WARN("fail to get tenant status", KR(ret), K(tenant_id));
  } else {
    is_creating_standby = is_creating_standby_tenant_status(status);
  }
  return ret;
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
    LOG_WARN("fail to get schema_version", KR(ret), K(schema_version));
  } else if (OB_CORE_SCHEMA_VERSION + 1 == schema_version
             || ObSchemaService::is_formal_version(schema_version)) {
    // We thought "OB_CORE_SCHEMA_VERSION + 1" is a format schema version, because
    // schema mgr with such schema version is the first complete schema mgr generated in the bootstrap stage.
    ret = OB_SUCCESS;
  } else {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("local schema_version is not formal, try again", KR(ret), K(schema_version));
  }
  return ret;
}

int ObSchemaGetterGuard::check_keystore_exist(const uint64_t tenant_id,
                                              bool &exist)
{
  int ret = OB_SUCCESS;
  const ObKeystoreSchema *schema = NULL;
  const ObSchemaMgr *mgr = NULL;
  exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->keystore_mgr_.get_keystore_schema(tenant_id, schema))) {
    LOG_WARN("get keystore schema failed", KR(ret), K(tenant_id));
  } else if (NULL != schema) {
    exist = true;
  }
  return ret;
}

int ObSchemaGetterGuard::get_keystore_schema(const uint64_t tenant_id,
                                             const ObKeystoreSchema *&keystore_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObKeystoreSchema *schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->keystore_mgr_.get_keystore_schema(tenant_id, schema))) {
    LOG_WARN("get keystore schema failed", KR(ret), K(tenant_id));
  } else if (NULL != schema) {
    keystore_schema = schema;
  }
  return ret;
}

int ObSchemaGetterGuard::get_tablespace_schema_with_name(const uint64_t tenant_id,
    const common::ObString &tablespace_name,
    const ObTablespaceSchema *&tablespace_schema)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  const ObTablespaceSchema *schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || tablespace_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablespace_name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->tablespace_mgr_.get_tablespace_schema_with_name(
      tenant_id,
      tablespace_name,
      schema))) {
    LOG_WARN("get tablespace schema failed", KR(ret),
             K(tenant_id), K(tablespace_name));
  } else if (NULL != schema) {
    tablespace_schema = schema;
  }
  return ret;
}

int ObSchemaGetterGuard::get_tablespace_schema(const uint64_t tenant_id,
    const uint64_t tablespace_id,
    const ObTablespaceSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tablespace_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(tablespace_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema(TABLESPACE_SCHEMA,
                                tenant_id,
                                tablespace_id,
                                schema))) {
    LOG_WARN("get tablespace schema failed", K(tenant_id), K(tablespace_id), KR(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("NULL ptr", KR(ret), K(tablespace_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_sys_priv_with_tenant_id(
    const uint64_t tenant_id,
    ObIArray<const ObSysPriv *> &sys_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  sys_privs.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_sys_privs_in_tenant(tenant_id, sys_privs))) {
    LOG_WARN("get sys priv with tenant_id failed", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::get_sys_priv_with_grantee_id(
    const uint64_t tenant_id,
    const uint64_t grantee_id,
    ObSysPriv *&sys_priv)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == grantee_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(grantee_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_sys_priv_in_grantee(tenant_id, grantee_id, sys_priv))) {
    LOG_WARN("get sys priv with user_id failed", KR(ret), K(tenant_id), K(grantee_id));
  }

  return ret;
}

int ObSchemaGetterGuard::is_lazy_mode(const uint64_t tenant_id, bool &is_lazy) const
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  is_lazy = false;
  if (OB_FAIL(get_schema_mgr(tenant_id, schema_mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else {
    is_lazy = OB_ISNULL(schema_mgr);
  }
  return ret;
}

bool ObSchemaGetterGuard::ignore_tenant_not_exist_error(
     const uint64_t tenant_id)
{
  bool bret = false;
  if (is_standby_cluster()) {
    // ingore error while standby cluster create tenant.
    bret = true;
  } else {
    // ignore error when tenant is in physical restore.
    bool is_restore = false;
    int tmp_ret = check_tenant_is_restore(tenant_id, is_restore);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "fail to check tenant is restore", K(bret), K(tmp_ret), K(tenant_id));
    } else if (is_restore) {
      bret = true;
    }
  }
  return bret;
}

int ObSchemaGetterGuard::generate_tablet_table_map(
    const uint64_t tenant_id,
    common::hash::ObHashMap<ObTabletID, uint64_t> &tablet_map)
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;
  int64_t schema_version = OB_INVALID_VERSION;
  const int64_t BUCKET_NUM = 10007;
  ObArray<const ObSimpleTableSchemaV2 *> tables;
  if (OB_UNLIKELY(get_tenant_id() != tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant_id is not match with schema_guard", KR(ret), K(tenant_id_), K(tenant_id));
  } else if (OB_FAIL(is_lazy_mode(tenant_id, is_lazy))) {
    LOG_WARN("fail to check lazy mode", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(is_lazy)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't generate tablet-table pairs with lazy guard", KR(ret), K(tenant_id_), K(tenant_id));
  } else if (OB_FAIL(get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!ObSchemaService::is_formal_version(schema_version))) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("schema version is not formal, retry to get schema guard", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(tablet_map.create(BUCKET_NUM, "TabletPairMap", "TabletPairMap", tenant_id))) {
    LOG_WARN("create tablet-table map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("fail to get tables", KR(ret), K(tenant_id));
  } else {
    ObTabletID tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObSimpleTableSchemaV2* &table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr is null", KR(ret), K(tenant_id));
      } else if (table->has_tablet()) {
        const uint64_t table_id = table->get_table_id();
        ObPartitionSchemaIter iter(*table, ObCheckPartitionMode::CHECK_PARTITION_MODE_ALL);
        while (OB_SUCC(ret) && (OB_SUCC(iter.next_tablet_id(tablet_id)))) {
          if (OB_FAIL(tablet_map.set_refactored(tablet_id, table_id))) {
            LOG_WARN("fail to set tablet-table map", KR(ret), K(tenant_id), K(tablet_id), K(table_id));
          }
        } // end while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("iter tablet failed", KR(ret), K(tenant_id));
        }
      }
    } // end for
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
  } else if (OB_FAIL(get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("get null table schema", K(ret), K(table_id));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObTableSchema *index_schema = NULL;
    if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !exist && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_schema))) {
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

int ObSchemaGetterGuard::deep_copy_index_name_map(
    common::ObIAllocator &allocator,
    ObIndexNameMap &index_name_cache)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (OB_FAIL(check_lazy_guard(tenant_id_, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K_(tenant_id));
  // const_cast to iterate index_name_map_, mgr won't be changed actually
  } else if (OB_FAIL(const_cast<ObSchemaMgr*>(mgr)
             ->deep_copy_index_name_map(allocator, index_name_cache))) {
    LOG_WARN("fail to deep copy index name map", KR(ret), K_(tenant_id));
  }
  return ret;
}

#define GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(SCHEMA, SIMPLE_SCHEMA_TYPE)                       \
  int ObSchemaGetterGuard::get_simple_##SCHEMA##_schemas_in_database(                                \
      const uint64_t tenant_id,                                                                      \
      const uint64_t database_id,                                                                    \
      common::ObIArray<const SIMPLE_SCHEMA_TYPE*> &schema_array)                                     \
  {                                                                                                  \
    int ret = OB_SUCCESS;                                                                            \
    const ObSchemaMgr *mgr = NULL;                                                                   \
    schema_array.reset();                                                                            \
    if (!check_inner_stat()) {                                                                       \
      ret = OB_INNER_STAT_ERROR;                                                                     \
      LOG_WARN("inner stat error", KR(ret));                                                         \
    } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id) {                         \
      ret = OB_INVALID_ARGUMENT;                                                                     \
      LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id));                           \
    } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {                                      \
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));           \
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {                                          \
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));                                   \
    } else if (OB_FAIL(mgr->SCHEMA##_mgr_.get_##SCHEMA##_schemas_in_database(tenant_id,              \
        database_id, schema_array))) {                                                               \
      LOG_WARN("get "#SCHEMA" schemas in database failed", KR(ret), K(tenant_id), K(database_id));   \
    }                                                                                                \
    return ret;                                                                                      \
  }

GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(udt, ObSimpleUDTSchema);
GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(outline, ObSimpleOutlineSchema);
GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(synonym, ObSimpleSynonymSchema);
GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(package, ObSimplePackageSchema);
GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(routine, ObSimpleRoutineSchema);
GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DEFINE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
