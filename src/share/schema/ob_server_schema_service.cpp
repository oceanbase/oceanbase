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
#include "ob_server_schema_service.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_timeout_ctx.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_sys_variable_mgr.h"
#include "share/schema/ob_schema_mgr.h"
#include "lib/worker.h"
#include "share/ob_get_compat_mode.h"
#include "observer/ob_server_struct.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_global_stat_proxy.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;

ObServerSchemaService::ObServerSchemaService()
    : schema_manager_rwlock_(common::ObLatchIds::SCHEMA_MGR_CACHE_LOCK),
      mem_mgr_for_liboblog_mutex_(common::ObLatchIds::SCHEMA_MGR_CACHE_LOCK),
      schema_service_(NULL),
      sql_proxy_(NULL),
      config_(NULL),
      refresh_full_schema_map_(),
      schema_mgr_for_cache_map_(),
      mem_mgr_map_(),
      mem_mgr_for_liboblog_map_()
{
}

ObServerSchemaService::~ObServerSchemaService()
{
  destroy();
}

int ObServerSchemaService::destroy()
{
  int ret = OB_SUCCESS;
  if (NULL != schema_service_) {
    ObSchemaServiceSQLImpl *tmp = static_cast<ObSchemaServiceSQLImpl *>(schema_service_);
    OB_DELETE(ObSchemaServiceSQLImpl, ObModIds::OB_SCHEMA_SERVICE, tmp);
    schema_service_ = NULL;
  }
  if (OB_SUCC(ret)) {
    FOREACH(it, schema_mgr_for_cache_map_) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        (it->second)->~ObSchemaMgr();
        it->second = NULL;
      }
    }
  }
  if (OB_SUCC(ret)) {
    FOREACH(it, mem_mgr_map_) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        (it->second)->~ObSchemaMemMgr();
        it->second = NULL;
      }
    }
  }
  if (OB_SUCC(ret)) {
    FOREACH(it, mem_mgr_for_liboblog_map_) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        (it->second)->~ObSchemaMemMgr();
        it->second = NULL;
      }
    }
  }
  return ret;
}

int ObServerSchemaService::init_tenant_basic_schema(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(schema_manager_rwlock_);
  ObSchemaMgr *schema_mgr_for_cache = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema mgr for cache", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", KR(ret), K(tenant_id));
  } else {
    const char* tenant_name = is_sys_tenant(tenant_id) ?
                              OB_SYS_TENANT_NAME : OB_FAKE_TENANT_NAME;
    ObSimpleTenantSchema tenant;
    tenant.set_tenant_id(tenant_id);
    tenant.set_tenant_name(ObString(tenant_name));
    tenant.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    tenant.set_read_only(false);
    tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

    ObSimpleSysVariableSchema sys_variable;
    sys_variable.set_tenant_id(tenant_id);
    sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    sys_variable.set_schema_version(OB_CORE_SCHEMA_VERSION);

    if (OB_FAIL(schema_mgr_for_cache->add_tenant(tenant))) {
      LOG_WARN("add tenant failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_mgr_for_cache->sys_variable_mgr_.add_sys_variable(sys_variable))) {
      LOG_WARN("add sys variable failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(fill_all_core_table_schema(tenant_id, *schema_mgr_for_cache))) {
      LOG_WARN("init add core table schema failed", KR(ret), K(tenant_id));
    } else if (is_sys_tenant(tenant_id)) {
      // only sys tenant rely on root user schema
      ObSimpleUserSchema user;
      user.set_tenant_id(tenant_id);
      user.set_user_id(OB_SYS_USER_ID);
      user.set_schema_version(OB_CORE_SCHEMA_VERSION);
      if (OB_FAIL(user.set_user_name(OB_SYS_USER_NAME))) {
        LOG_WARN("set_user_name failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(user.set_host(OB_SYS_HOST_NAME))) {
        LOG_WARN("set_host failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_mgr_for_cache->add_user(user))) {
        LOG_WARN("add user failed", KR(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      schema_mgr_for_cache->set_tenant_id(tenant_id);
      schema_mgr_for_cache->set_schema_version(OB_CORE_SCHEMA_VERSION);
    }
  }
  return ret;
}

int ObServerSchemaService::init(ObMySQLProxy *sql_proxy,
                                ObDbLinkProxy *dblink_proxy,
                                const ObCommonConfig *config)
{
  int ret = OB_SUCCESS;
  auto attr = SET_USE_500(ObModIds::OB_SCHEMA_ID_VERSIONS, ObCtxIds::SCHEMA_SERVICE);
  if (OB_ISNULL(sql_proxy)
     || NULL != schema_service_
     || OB_ISNULL(sql_proxy->get_pool())
     || OB_ISNULL(config)) {
    ret = OB_INIT_FAIL;
    LOG_WARN("check param failed", KR(ret), KP(sql_proxy), KP_(schema_service),
        "proxy->pool", (NULL == sql_proxy ? NULL : sql_proxy->get_pool()), KP(config));
  } else if (OB_FAIL(ObSysTableChecker::instance().init())) {
    LOG_WARN("fail to init tenant space table checker", KR(ret));
  } else if (NULL
      == (schema_service_ = OB_NEW(ObSchemaServiceSQLImpl, ObModIds::OB_SCHEMA_SERVICE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate schema service sql impl failed, no memory", KR(ret));
  } else if (OB_FAIL(schema_service_->init(sql_proxy, dblink_proxy, this))) {
    LOG_ERROR("fail to init schema service,", KR(ret));
  } else if (FALSE_IT(schema_service_->set_common_config(config))) {
    // will not reach here
  } else if (OB_FAIL(version_his_map_.create(VERSION_HIS_MAP_BUCKET_NUM,
      attr))) {
    LOG_WARN("create version his map failed", KR(ret));
  } else {
    sql_proxy_ = sql_proxy;
    config_ = config;
  }
  // init schema management struct for tenant
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresh_full_schema_map_.create(TENANT_MAP_BUCKET_NUM,
                ObModIds::OB_REFRESH_FULL_SCHEMA_MAP, ObModIds::OB_REFRESH_FULL_SCHEMA_MAP))) {
      LOG_WARN("fail to create tenant_schema_info_map", KR(ret));
    } else if (OB_FAIL(mem_mgr_map_.create(TENANT_MAP_BUCKET_NUM,
                       ObModIds::OB_MEM_MGR_MAP, ObModIds::OB_MEM_MGR_MAP))) {
      LOG_WARN("fail to create map", KR(ret));
    } else if (OB_FAIL(mem_mgr_for_liboblog_map_.create(TENANT_MAP_BUCKET_NUM,
                       ObModIds::OB_MEM_MGR_FOR_LIBOBLOG_MAP, ObModIds::OB_MEM_MGR_FOR_LIBOBLOG_MAP))) {
      LOG_WARN("fail to create map", KR(ret));
    } else if (OB_FAIL(schema_mgr_for_cache_map_.create(TENANT_MAP_BUCKET_NUM,
                       ObModIds::OB_TENANT_SCHEMA_FOR_CACHE_MAP, ObModIds::OB_TENANT_SCHEMA_FOR_CACHE_MAP))) {
      LOG_WARN("fail to create map", KR(ret));
    } else if (OB_FAIL(init_schema_struct(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init schema struct", KR(ret));
    } else if (OB_FAIL(init_tenant_basic_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init basic schema for sys", KR(ret));
    } else {
      LOG_INFO("init schema service", KR(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::destroy_schema_struct(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id
      || is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else {
    SpinWLockGuard guard(schema_manager_rwlock_);

    bool overwrite = true;
    bool refresh_full_schema = true;
    if (OB_FAIL(refresh_full_schema_map_.set_refactored(tenant_id, refresh_full_schema, overwrite))) {
      LOG_WARN("fail to erase refresh_full_schema_map_", K(ret), K(tenant_id));
    } else {
      LOG_INFO("reset tenant refresh_full mark", K(ret), K(tenant_id));
    }

    ObSchemaMgr *schema_mgr = NULL;
    ObSchemaMemMgr *mem_mgr = NULL;
    if (OB_SUCC(ret)) {
      ObSchemaMgr * const *tmp_mgr = schema_mgr_for_cache_map_.get(tenant_id);
      if (OB_NOT_NULL(tmp_mgr)) {
        schema_mgr = *tmp_mgr;
      }
      if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
        LOG_WARN("fail to get mem mgr", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_mgr_for_cache_map_.erase_refactored(tenant_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get schema mgr for cache", K(ret));
        }
      } else {
        FLOG_INFO("[SCHEMA_RELEASE] erase tenant from schema_mgr_for_cache_map", K(ret), K(tenant_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_mgr)) {
      LOG_INFO("schema_mgr_for_cache has been released, just skip", K(ret), K(tenant_id));
    } else if (OB_ISNULL(mem_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_mgr is null", K(ret), K(tenant_id));
    } else {
      if (OB_FAIL(mem_mgr->free_schema_mgr(schema_mgr))) {
        LOG_ERROR("free schema mgr for cache failed", K(ret), K(tenant_id));
      } else {
        schema_mgr = NULL;
      }
    }
  }

  return ret;
}

bool ObServerSchemaService::check_inner_stat() const
{
  bool ret = true;
  if (NULL == schema_service_
      || NULL == sql_proxy_
      || NULL == config_) {
    ret = false;
    LOG_WARN("inner stat error", K(schema_service_),
             K(sql_proxy_), K(config_));
  }
  return ret;
}

int ObServerSchemaService::check_stop() const
{
  int ret = OB_SUCCESS;
  if (observer::SS_STOPPING == GCTX.status_
      || observer::SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("observer is stopping", K(ret));
  }
  return ret;
}

void ObServerSchemaService::AllSchemaKeys::reset()
{
  //use clear
  new_tenant_keys_.clear();
  alter_tenant_keys_.clear();
  del_tenant_keys_.clear();
  new_user_keys_.clear();
  del_user_keys_.clear();
  new_database_keys_.clear();
  del_database_keys_.clear();
  new_tablegroup_keys_.clear();
  del_tablegroup_keys_.clear();
  new_table_keys_.clear();
  del_table_keys_.clear();
  new_outline_keys_.clear();
  del_outline_keys_.clear();
  new_db_priv_keys_.clear();
  del_db_priv_keys_.clear();
  new_table_priv_keys_.clear();
  del_table_priv_keys_.clear();
  new_synonym_keys_.clear();
  del_synonym_keys_.clear();
  new_routine_keys_.clear();
  del_routine_keys_.clear();
  new_package_keys_.clear();
  del_package_keys_.clear();
  new_trigger_keys_.clear();
  del_trigger_keys_.clear();
  new_udf_keys_.clear();
  del_udf_keys_.clear();
  new_udt_keys_.clear();
  del_udt_keys_.clear();
  new_sequence_keys_.clear();
  del_sequence_keys_.clear();
  new_sys_variable_keys_.clear();
  del_sys_variable_keys_.clear();
  add_drop_tenant_keys_.clear();
  del_drop_tenant_keys_.clear();
  new_keystore_keys_.clear();
  del_keystore_keys_.clear();
  new_label_se_policy_keys_.clear();
  del_label_se_policy_keys_.clear();
  new_label_se_component_keys_.clear();
  del_label_se_component_keys_.clear();
  new_label_se_label_keys_.clear();
  del_label_se_label_keys_.clear();
  new_label_se_user_level_keys_.clear();
  del_label_se_user_level_keys_.clear();
  new_tablespace_keys_.clear();
  del_tablespace_keys_.clear();
  new_profile_keys_.clear();
  del_profile_keys_.clear();
  new_audit_keys_.clear();
  del_audit_keys_.clear();
  new_sys_priv_keys_.clear();
  del_sys_priv_keys_.clear();
  new_obj_priv_keys_.clear();
  del_obj_priv_keys_.clear();
  new_dblink_keys_.clear();
  del_dblink_keys_.clear();
  new_directory_keys_.clear();
  del_directory_keys_.clear();
  new_context_keys_.clear();
  del_context_keys_.clear();
  new_mock_fk_parent_table_keys_.clear();
  del_mock_fk_parent_table_keys_.clear();
  new_rls_policy_keys_.clear();
  del_rls_policy_keys_.clear();
  new_rls_group_keys_.clear();
  del_rls_group_keys_.clear();
  new_rls_context_keys_.clear();
  del_rls_context_keys_.clear();
}

int ObServerSchemaService::AllSchemaKeys::create(int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  if (bucket_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to create hashset,", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(alter_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create alter_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_user_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_user_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_user_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_user_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_database_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_database_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_database_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_database_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_tablegroup_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_tablegroup_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_tablegroup_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_tablegroup_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_table_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_table_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_table_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_table_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_outline_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_table_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_outline_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_table_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_db_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_db_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_db_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_db_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_table_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_table_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_table_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_table_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_routine_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_routine_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_routine_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_routine_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_synonym_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_synonym_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_synonym_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_synonym_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_package_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_package_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_package_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_package_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_trigger_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_trigger_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_trigger_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_trigger_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_udf_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_udf_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_udf_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_udf_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_sequence_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_sequence_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_sequence_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_sequence_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_udt_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_udt_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_udt_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_udt_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_sys_variable_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new sys vairable hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_sys_variable_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del sys variable hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(add_drop_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create add_drop_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_drop_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_drop_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_keystore_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_keystore_keys_ hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_keystore_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_keystore_keys_ hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_label_se_policy_keys_.create(bucket_size))) {
    LOG_WARN("failed to create label security policy hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_label_se_policy_keys_.create(bucket_size))) {
    LOG_WARN("failed to del label security policy hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_label_se_component_keys_.create(bucket_size))) {
    LOG_WARN("failed to create label security component hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_label_se_component_keys_.create(bucket_size))) {
    LOG_WARN("failed to del label security component hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_label_se_label_keys_.create(bucket_size))) {
    LOG_WARN("failed to create label security label hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_label_se_label_keys_.create(bucket_size))) {
    LOG_WARN("failed to del label security label hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_label_se_user_level_keys_.create(bucket_size))) {
    LOG_WARN("failed to create label security user level hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_label_se_user_level_keys_.create(bucket_size))) {
    LOG_WARN("failed to del label security user level hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_tablespace_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_tablespace_keys_ hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_tablespace_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_tablespace_keys_", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_profile_keys_.create(bucket_size))) {
    LOG_WARN("failed to create add_profile_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_profile_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_profile_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_audit_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_audit_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_audit_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_audit_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_sys_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_sys_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_sys_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_sys_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_obj_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_obj_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_obj_priv_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_obj_priv_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_dblink_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_dblink_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_dblink_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_dblink_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_directory_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_directory_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_directory_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_directory_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_context_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_context_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_context_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_context_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_mock_fk_parent_table_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_mock_fk_parent_table_keys_ hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_mock_fk_parent_table_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_mock_fk_parent_table_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_rls_policy_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_rls_policy_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_rls_policy_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_rls_policy_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_rls_group_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_rls_group_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_rls_group_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_rls_group_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_rls_context_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_rls_context_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_rls_context_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_rls_context_keys hashset", K(bucket_size), K(ret));
  }
  return ret;
}

void ObServerSchemaService::AllIncrementSchema::reset()
{
  table_schema_.reset();
  db_schema_.reset();
  tg_schema_.reset();
  max_used_tids_.reset();
  //For managing privileges
  tenant_info_array_.reset();
  user_info_array_.reset();
  db_priv_array_.reset();
  table_priv_array_.reset();
}

//////////////////////////////////////////////////////////////////////////////////////////////
//                              SCHEMA SERVICE RELATED                                      //
//////////////////////////////////////////////////////////////////////////////////////////////

ObSchemaService *ObServerSchemaService::get_schema_service(void) const
{
  return schema_service_;
}

int ObServerSchemaService::del_tenant_operation(
    const uint64_t tenant_id,
    AllSchemaKeys &schema_keys,
    const bool new_flag)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_user_keys_ : schema_keys.del_user_keys_))) {
    LOG_WARN("fail to del user operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_database_keys_ : schema_keys.del_database_keys_))) {
    LOG_WARN("fail to del database operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_tablegroup_keys_ : schema_keys.del_tablegroup_keys_))) {
    LOG_WARN("fail to del tablegroup operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_table_keys_ : schema_keys.del_table_keys_))) {
    LOG_WARN("fail to del table operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_outline_keys_ : schema_keys.del_outline_keys_))) {
    LOG_WARN("fail to del outline operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_db_priv_keys_ : schema_keys.del_db_priv_keys_))) {
    LOG_WARN("fail to del db_priv operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_table_priv_keys_ : schema_keys.del_table_priv_keys_))) {
    LOG_WARN("fail to del table_priv operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_synonym_keys_ : schema_keys.del_synonym_keys_))) {
    LOG_WARN("fail to del synonym operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_package_keys_ : schema_keys.del_package_keys_))) {
    LOG_WARN("fail to del package operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_routine_keys_ : schema_keys.del_routine_keys_))) {
    LOG_WARN("fail to del routine operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_trigger_keys_ : schema_keys.del_trigger_keys_))) {
    LOG_WARN("fail to del trigger operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_udf_keys_ : schema_keys.del_udf_keys_))) {
    LOG_WARN("fail to del udf operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_udt_keys_ : schema_keys.del_udt_keys_))) {
    LOG_WARN("fail to del udt operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_sequence_keys_ : schema_keys.del_sequence_keys_))) {
    LOG_WARN("fail to del sequence operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_sys_variable_keys_ : schema_keys.del_sys_variable_keys_))) {
    LOG_WARN("fail to del sys_variable operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_label_se_policy_keys_ : schema_keys.del_label_se_policy_keys_))) {
    LOG_WARN("fail to del label_se_policy operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_label_se_component_keys_ : schema_keys.del_label_se_component_keys_))) {
    LOG_WARN("fail to del label_se_component operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_label_se_label_keys_ : schema_keys.del_label_se_label_keys_))) {
    LOG_WARN("fail to del label_se_label operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_label_se_user_level_keys_ : schema_keys.del_label_se_user_level_keys_))) {
    LOG_WARN("fail to del label_se_user_level operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_tablespace_keys_ : schema_keys.del_tablespace_keys_))) {
    LOG_WARN("fail to del tablespace operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_profile_keys_ : schema_keys.del_profile_keys_))) {
    LOG_WARN("fail to del profile operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_audit_keys_ : schema_keys.del_audit_keys_))) {
    LOG_WARN("fail to del audit operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_sys_priv_keys_ : schema_keys.del_sys_priv_keys_))) {
    LOG_WARN("fail to del sys_priv operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_obj_priv_keys_ : schema_keys.del_obj_priv_keys_))) {
    LOG_WARN("fail to del obj_priv operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_directory_keys_ : schema_keys.del_directory_keys_))) {
    LOG_WARN("fail to del directory operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_context_keys_ : schema_keys.del_context_keys_))) {
    LOG_WARN("fail to del context operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_mock_fk_parent_table_keys_ : schema_keys.del_mock_fk_parent_table_keys_))) {
    LOG_WARN("fail to del mock_fk_parent_table operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_rls_policy_keys_ : schema_keys.del_rls_policy_keys_))) {
    LOG_WARN("fail to del rls_policy operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_rls_group_keys_ : schema_keys.del_rls_group_keys_))) {
    LOG_WARN("fail to del rls_group operation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_operation(tenant_id,
             new_flag ? schema_keys.new_rls_context_keys_ : schema_keys.del_rls_context_keys_))) {
    LOG_WARN("fail to del rls_context operation", KR(ret), K(tenant_id));
  }
  return ret;
}

#define REPLAY_OP(key, del_keys, new_keys, is_delete, is_exist)      \
  ({                                                                 \
    int ret = OB_SUCCESS;                           \
    int hash_ret = -1;                              \
    if (is_delete) {                                \
      hash_ret = new_keys.erase_refactored(key);                     \
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) { \
        ret = OB_ERR_UNEXPECTED;                        \
        LOG_WARN("erase failed", K(hash_ret), K(ret));  \
      } else if (is_exist) {                            \
        hash_ret = del_keys.set_refactored(key);        \
        if (OB_SUCCESS != hash_ret) {                   \
          ret = OB_ERR_UNEXPECTED;                      \
          LOG_WARN("erase failed", K(hash_ret), K(ret));        \
        }                                           \
      }                                             \
    } else {                                        \
      hash_ret = new_keys.set_refactored(key);      \
      if (OB_SUCCESS != hash_ret) {                 \
        ret = OB_ERR_UNEXPECTED;                    \
        LOG_WARN("erase failed", K(hash_ret), K(ret));  \
      }                                             \
    }                                               \
    ret;                                            \
  })

int ObServerSchemaService::get_increment_tenant_keys(const ObSchemaMgr &schema_mgr,
                                                     const ObSchemaOperation &schema_operation,
                                                     AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    //the system tenant's schema will refreshed incremently too
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), KR(ret));
    } else if (OB_DDL_ADD_TENANT == schema_operation.op_type_
               || OB_DDL_ADD_TENANT_START == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tenant_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new tenant id to new_tenant_ids", K(hash_ret), KR(ret));
      }
    } else if (OB_DDL_RENAME_TENANT == schema_operation.op_type_
               || OB_DDL_ALTER_TENANT == schema_operation.op_type_
               || OB_DDL_ADD_TENANT_END == schema_operation.op_type_
               || OB_DDL_DEL_TENANT_START == schema_operation.op_type_
               || OB_DDL_DROP_TENANT_TO_RECYCLEBIN == schema_operation.op_type_
               || OB_DDL_FLASHBACK_TENANT == schema_operation.op_type_) {
      //TODO: OB_DDL_DEL_TENANT_START does not perform any substantial schema recovery actions,
      //  only DDL and user table writes are prohibited.
      //      open a transaction in the DROP TENANT DDL to push up the schema_version of the user tenant,
      //      and do the processing in the RS inspection task to prevent the DDL writing of the user tenant from failing.
      //      When user tenants fresh the DDL, it triggers the recycling of non-system tables related schemas.
      hash_ret = schema_keys.alter_tenant_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add alter tenant id to alter_tenant_ids", K(hash_ret), KR(ret));
      }
    } else if (OB_DDL_DEL_TENANT == schema_operation.op_type_
               || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tenant_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped tenant id", K(hash_ret), KR(ret));
      } else {
        hash_ret = schema_keys.alter_tenant_keys_.erase_refactored(schema_key);
        if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to del dropped tenant id", K(hash_ret), KR(ret));
        } else if (OB_FAIL(del_tenant_operation(tenant_id, schema_keys, true))) {
          LOG_WARN("delete new op failed", K(tenant_id), KR(ret));
        } else if (OB_FAIL(del_tenant_operation(tenant_id, schema_keys, false))) {
          LOG_WARN("delete del op failed", K(tenant_id), KR(ret));
        } else {
          const ObSimpleTenantSchema *tenant = NULL;
          if (OB_FAIL(schema_mgr.get_tenant_schema(tenant_id, tenant))) {
            LOG_WARN("get tenant schema failed", K(tenant_id), KR(ret));
          } else if (NULL != tenant) {
            hash_ret = schema_keys.del_tenant_keys_.set_refactored_1(schema_key, 1);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to add del tenant id to del_tenant_ids",
                  K(hash_ret), KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t count = schema_keys.del_drop_tenant_keys_.size();
        if (0 != count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("del_drop_tenant_keys num should be 0", KR(ret), K(count));
        } else {
           hash_ret = schema_keys.add_drop_tenant_keys_.set_refactored_1(schema_key, 1);
           if (OB_SUCCESS != hash_ret) {
             ret = OB_ERR_UNEXPECTED;
             LOG_WARN("failed to add drop tenant key", K(hash_ret), KR(ret), K(schema_key));
           }
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_tenant_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_ADD_TENANT == schema_operation.op_type_
                      || OB_DDL_ADD_TENANT_START == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleTenantSchema *tenant = NULL;
    // After the schema is split, user tenants will not replay the tenant ddl
    if (!is_sys_tenant(schema_mgr.get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only sys tenant can revert tenant ddl operation after schema splited", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_mgr.get_tenant_schema(tenant_id, tenant))) {
      LOG_WARN("get tenant schema failed", K(tenant_id), KR(ret));
    } else if (NULL != tenant) {
      is_exist = true;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_DDL_DEL_TENANT == schema_operation.op_type_
               || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
      int hash_ret = schema_keys.new_tenant_keys_.set_refactored(schema_key);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set new tenant keys failed", KR(ret), K(hash_ret), K(tenant_id));
      }
    } else if (OB_DDL_RENAME_TENANT == schema_operation.op_type_
        || OB_DDL_ALTER_TENANT == schema_operation.op_type_
        || OB_DDL_ADD_TENANT_END == schema_operation.op_type_
        || OB_DDL_DEL_TENANT_START == schema_operation.op_type_
        || OB_DDL_DROP_TENANT_TO_RECYCLEBIN == schema_operation.op_type_
        || OB_DDL_FLASHBACK_TENANT == schema_operation.op_type_) {
      int hash_ret = schema_keys.alter_tenant_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add alter tenant id to alter_tenant_ids", K(hash_ret), KR(ret));
      }
    } else if (OB_DDL_ADD_TENANT == schema_operation.op_type_
               || OB_DDL_ADD_TENANT_START == schema_operation.op_type_) {
      int hash_ret = schema_keys.new_tenant_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("erase new tenant keys failed", KR(ret), K(hash_ret), K(tenant_id));
      }
      if (OB_SUCC(ret)) {
        hash_ret = schema_keys.alter_tenant_keys_.erase_refactored(schema_key);
        if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("erase alter tenant keys failed", KR(ret), K(hash_ret), K(tenant_id));
        }
      }
      if (OB_SUCC(ret) && is_exist) {
        hash_ret = schema_keys.del_tenant_keys_.set_refactored(schema_key);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set del tenant keys failed", KR(ret), K(hash_ret), K(tenant_id));
        }
      }
    }
    // sys variable, for compatible
    if (OB_INVALID_TENANT_ID != schema_mgr.get_tenant_id()
        && schema_operation.tenant_id_ != schema_mgr.tenant_id_) {
      LOG_INFO("sys variable key not match, just skip", KR(ret), K(schema_operation), "tenant_id", schema_mgr.get_tenant_id());
    } else {
      if (OB_SUCC(ret)) {
        const ObSimpleSysVariableSchema *sys_variable = NULL;
        is_exist = false;
        if (OB_FAIL(schema_mgr.sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
          LOG_WARN("get user info failed", K(tenant_id), KR(ret));
        } else if (NULL != sys_variable) {
          is_exist = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_sys_variable_keys_,
            schema_keys.new_sys_variable_keys_, is_delete, is_exist))) {
          LOG_WARN("replay operation failed", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_delete) {
        if (OB_FAIL(del_tenant_operation(tenant_id, schema_keys, true))) {
          LOG_WARN("delete new op failed", K(tenant_id), KR(ret));
        } else if (OB_FAIL(del_tenant_operation(tenant_id, schema_keys, false))) {
          LOG_WARN("delete del op failed", K(tenant_id), KR(ret));
        }
      }
    }
    // dropped tenant info
    if (OB_SUCC(ret)) {
      if (OB_DDL_DEL_TENANT == schema_operation.op_type_
          || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
        int64_t count = schema_keys.add_drop_tenant_keys_.size();
        if (0 != count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add_drop_tenant_keys num should be 0", KR(ret), K(count));
        } else {
          int hash_ret = schema_keys.del_drop_tenant_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to del drop tenant key", K(hash_ret), KR(ret), K(schema_key));
          }
        }
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_sys_variable_keys(const ObSchemaMgr &schema_mgr,
                                                           const ObSchemaOperation &schema_operation,
                                                           AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID != schema_mgr.get_tenant_id()
      && schema_operation.tenant_id_ != schema_mgr.tenant_id_) {
    LOG_INFO("sys variable key not match, just skip", KR(ret), K(schema_operation), "tenant_id", schema_mgr.get_tenant_id());
  } else if (!((schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN
                && schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END)
               || (schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    //the system tenant's schema will refreshed incremently too
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), KR(ret));
    } else if (OB_DDL_DEL_TENANT == schema_operation.op_type_
               || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
      hash_ret = schema_keys.new_sys_variable_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped sys variable", K(hash_ret), KR(ret));
      } else {
        const ObSimpleSysVariableSchema *sys_variable = NULL;
        if (OB_FAIL(schema_mgr.sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
          LOG_WARN("get sys variable schema failed", K(tenant_id), KR(ret));
        } else if (NULL != sys_variable) {
          hash_ret = schema_keys.del_sys_variable_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add del sys variable", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_sys_variable_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new sys variable keys", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_sys_variable_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = false;
    bool is_exist = false;
    const ObSimpleSysVariableSchema *sys_variable = NULL;
    is_exist = false;
    if (OB_FAIL(schema_mgr.sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
      LOG_WARN("get user info failed", K(tenant_id), KR(ret));
    } else if (NULL != sys_variable) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_sys_variable_keys_,
          schema_keys.new_sys_variable_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_user_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_USER_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.user_id_ = user_id;
    schema_key.schema_version_ = schema_version;
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), KR(ret));
    } else if (OB_DDL_DROP_USER == schema_operation.op_type_) {
      hash_ret = schema_keys.new_user_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del dropped user id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleUserSchema *user = NULL;
        if (OB_FAIL(schema_mgr.get_user_schema(tenant_id, user_id, user))) {
          LOG_WARN("get user info failed", K(user_id), KR(ret));
        } else if (NULL != user) {
          hash_ret = schema_keys.del_user_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add del user id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_user_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new user id", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_user_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_USER_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.user_id_ = user_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_USER == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleUserSchema *user = NULL;
    if (OB_FAIL(schema_mgr.get_user_schema(tenant_id, user_id, user))) {
      LOG_WARN("get user schema failed", K(user_id), KR(ret));
    } else if (NULL != user) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_user_keys_,
          schema_keys.new_user_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_database_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t db_id = schema_operation.database_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = db_id;
    schema_key.schema_version_ = schema_version;
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), KR(ret));
    } else if (OB_DDL_DEL_DATABASE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_database_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped db id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleDatabaseSchema *database = NULL;
        if (OB_FAIL(schema_mgr.get_database_schema(tenant_id, db_id, database))) {
          LOG_WARN("get database schema failed", K(db_id), KR(ret));
        } else if (NULL != database) {
          hash_ret = schema_keys.del_database_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del db id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_database_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new database id", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_database_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t database_id = schema_operation.database_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = database_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_ADD_DATABASE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleDatabaseSchema *database = NULL;
    if (OB_FAIL(schema_mgr.get_database_schema(tenant_id, database_id, database))) {
      LOG_WARN("get database schema failed", K(database_id), KR(ret));
    } else if (NULL != database) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_database_keys_,
          schema_keys.new_database_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_tablegroup_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t tg_id = schema_operation.tablegroup_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.tablegroup_id_ = tg_id;
    schema_key.schema_version_ = schema_version;
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), KR(ret));
    } else if (OB_DDL_DEL_TABLEGROUP == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tablegroup_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped tg id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleTablegroupSchema *tg = NULL;
        if (OB_FAIL(schema_mgr.get_tablegroup_schema(tenant_id, tg_id, tg))) {
          LOG_WARN("get tablegroup schema failed", K(tenant_id), K(tg_id), KR(ret));
        } else if (NULL != tg) {
          hash_ret = schema_keys.del_tablegroup_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del tg id to del_log", K(hash_ret), KR(ret));
          }
        } else { }
      }
    } else {
      hash_ret = schema_keys.new_tablegroup_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new tablegroup id", K(hash_ret), KR(ret));
      } else if (OB_DDL_ALTER_TABLEGROUP_PARTITION == schema_operation.op_type_
                 || OB_DDL_SPLIT_TABLEGROUP_PARTITION == schema_operation.op_type_
                 || OB_DDL_PARTITIONED_TABLEGROUP_TABLE == schema_operation.op_type_
                 || OB_DDL_FINISH_SPLIT_TABLEGROUP == schema_operation.op_type_
                 || OB_DDL_FINISH_LOGICAL_SPLIT_TABLEGROUP == schema_operation.op_type_
                 || OB_DDL_DELAY_DELETE_TABLEGROUP_PARTITION == schema_operation.op_type_) {
        // alter tablegroup partition is a batch operation, which needs to trigger the schema refresh of
        // the table under the same tablegroup
        ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
        if (OB_FAIL(schema_mgr.get_table_schemas_in_tablegroup(tenant_id, tg_id, table_schemas))) {
          LOG_WARN("fail to get table schemas in tablegroup", KR(ret));
        } else {
          SchemaKey table_schema_key;
          table_schema_key.tenant_id_ = tenant_id;
          table_schema_key.schema_version_ = schema_version;
          for(int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
            table_schema_key.table_id_ = table_schemas.at(i)->get_table_id();
            ret = schema_keys.del_table_keys_.exist_refactored(table_schema_key);
            if (OB_HASH_EXIST == ret) {
              // This schema refresh involves the drop table operation, there is no need to refresh the table's schema
              ret = OB_SUCCESS;
            } else if (OB_HASH_NOT_EXIST == ret) {
              if (OB_FAIL(schema_keys.new_table_keys_.set_refactored_1(table_schema_key, 1))) {
                LOG_WARN("fail to set table_schema_key", KR(ret), K(table_schema_key), K(tg_id));
              }
            } else {
              LOG_WARN("fail to check table schema key exist", KR(ret), K(table_schema_key), K(tg_id));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_tablegroup_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t tablegroup_id = schema_operation.tablegroup_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.tablegroup_id_ = tablegroup_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_ADD_TABLEGROUP == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleTablegroupSchema *tablegroup = NULL;
    if (OB_FAIL(schema_mgr.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup))) {
      LOG_WARN("get tablegroup schema failed", K(tenant_id), K(tablegroup_id), KR(ret));
    } else if (NULL != tablegroup) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_tablegroup_keys_,
          schema_keys.new_tablegroup_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      } else if (OB_DDL_ALTER_TABLEGROUP_PARTITION == schema_operation.op_type_
                 || OB_DDL_SPLIT_TABLEGROUP_PARTITION == schema_operation.op_type_
                 || OB_DDL_PARTITIONED_TABLEGROUP_TABLE == schema_operation.op_type_
                 || OB_DDL_FINISH_SPLIT_TABLEGROUP == schema_operation.op_type_
                 || OB_DDL_FINISH_LOGICAL_SPLIT_TABLEGROUP == schema_operation.op_type_) {
        // alter tablegroup partition is a batch operation, which needs to trigger the schema refresh of
        // the table under the same tablegroup
        ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
        //FIXME: Oceanbase tablegroup needs to fetch schema_mgr from the system tenant, but oceanbase tablegroup
        //  does not support partition management operations, so it can not be processed temporarily
        if (OB_FAIL(schema_mgr.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
          LOG_WARN("fail to get table schemas in tablegroup", KR(ret));
        } else {
          SchemaKey table_schema_key;
          table_schema_key.tenant_id_ = tenant_id;
          table_schema_key.schema_version_ = schema_version;
          is_delete = false;
          is_exist = false;
          for(int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
            table_schema_key.table_id_ = table_schemas.at(i)->get_table_id();
            ret = schema_keys.del_table_keys_.exist_refactored(table_schema_key);
            if (OB_HASH_EXIST == ret) {
              // This fallback schema refresh involves the create table operation,
              // there is no need to refresh the table's schema
              ret = OB_SUCCESS;
            } else if (OB_HASH_NOT_EXIST == ret) {
              if (OB_FAIL(schema_keys.new_table_keys_.set_refactored_1(table_schema_key, 1))) {
                LOG_WARN("fail to set table_schema_key", KR(ret), K(table_schema_key), K(tablegroup_id));
              }
            } else {
              LOG_WARN("fail to check table schema key exist", KR(ret), K(table_schema_key), K(tablegroup_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_table_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else if (OB_ALL_CORE_TABLE_TID == schema_operation.table_id_) {
    // won't load __all_core_table schema from inner_table
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t table_id = schema_operation.table_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.table_id_ = table_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_TABLE == schema_operation.op_type_
        || OB_DDL_DROP_INDEX == schema_operation.op_type_
        || OB_DDL_DROP_GLOBAL_INDEX == schema_operation.op_type_
        || OB_DDL_DROP_VIEW == schema_operation.op_type_
        || OB_DDL_TRUNCATE_TABLE_DROP == schema_operation.op_type_) {
      hash_ret = schema_keys.new_table_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped table id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleTableSchemaV2 *table = NULL;
        if (OB_FAIL(schema_mgr.get_table_schema(tenant_id, table_id, table))) {
          LOG_WARN("failed to get table schema", K(table_id), KR(ret));
        } else if (NULL != table) {
          hash_ret = schema_keys.del_table_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del table id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_table_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new table id", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_table_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else if (OB_ALL_CORE_TABLE_TID == schema_operation.table_id_) {
    // won't load __all_core_table schema from inner_table
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t table_id = schema_operation.table_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.table_id_ = table_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_TABLE == schema_operation.op_type_
                      || OB_DDL_CREATE_INDEX == schema_operation.op_type_
                      || OB_DDL_CREATE_GLOBAL_INDEX == schema_operation.op_type_
                      || OB_DDL_CREATE_VIEW == schema_operation.op_type_
                      || OB_DDL_TRUNCATE_TABLE_CREATE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleTableSchemaV2 *table = NULL;
    if (OB_FAIL(schema_mgr.get_table_schema(tenant_id, table_id, table))) {
      LOG_WARN("get table schema failed", K(table_id), KR(ret));
    } else if (NULL != table) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_table_keys_,
          schema_keys.new_table_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_outline_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t outline_id = schema_operation.outline_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.outline_id_ = outline_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_OUTLINE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_outline_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped outline id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleOutlineSchema *outline = NULL;
        if (OB_FAIL(schema_mgr.outline_mgr_.get_outline_schema(outline_id, outline))) {
          LOG_WARN("failed to get outline schema", K(outline_id), KR(ret));
        } else if (NULL != outline) {
          hash_ret = schema_keys.del_outline_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del outline id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_outline_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new outline id", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_outline_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t outline_id = schema_operation.outline_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.outline_id_ = outline_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_OUTLINE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleOutlineSchema *outline = NULL;
    if (OB_FAIL(schema_mgr.outline_mgr_.get_outline_schema(outline_id, outline))) {
      LOG_WARN("get outline schema failed", K(outline_id), KR(ret));
    } else if (NULL != outline) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_outline_keys_,
          schema_keys.new_outline_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_routine_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_ROUTINE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_ROUTINE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t object_id = schema_operation.routine_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.routine_id_ = object_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_ROUTINE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_routine_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped routine id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleRoutineSchema *routine = NULL;
        if (OB_FAIL(schema_mgr.routine_mgr_.get_routine_schema(object_id, routine))) {
          LOG_WARN("failed to get routine schema", K(object_id), KR(ret));
        } else if (NULL != routine) {
          hash_ret = schema_keys.del_routine_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del routine id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_routine_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new routine id", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_routine_keys_reversely(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_ROUTINE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_ROUTINE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t routine_id = schema_operation.routine_id_;
    int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.routine_id_ = routine_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_ROUTINE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleRoutineSchema *routine = NULL;
    if (OB_FAIL(schema_mgr.routine_mgr_.get_routine_schema(routine_id, routine))) {
      LOG_WARN("get procedure schema failed", K(routine_id), KR(ret));
    } else if (NULL != routine) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_routine_keys_,
          schema_keys.new_routine_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_synonym_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t synonym_id = schema_operation.synonym_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.synonym_id_ = synonym_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_SYNONYM == schema_operation.op_type_) {
      hash_ret = schema_keys.new_synonym_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped synonym id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleSynonymSchema *synonym = NULL;
        if (OB_FAIL(schema_mgr.synonym_mgr_.get_synonym_schema(synonym_id, synonym))) {
          LOG_WARN("failed to get synonym schema", K(synonym_id), KR(ret));
        } else if (NULL != synonym) {
          hash_ret = schema_keys.del_synonym_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del synonym id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_synonym_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new synonym id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_synonym_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t synonym_id = schema_operation.synonym_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.synonym_id_ = synonym_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_SYNONYM == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleSynonymSchema *synonym = NULL;
    if (OB_FAIL(schema_mgr.synonym_mgr_.get_synonym_schema(synonym_id, synonym))) {
      LOG_WARN("get synonym schema failed", K(synonym_id), KR(ret));
    } else if (NULL != synonym) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_synonym_keys_,
          schema_keys.new_synonym_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_package_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_PACKAGE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_PACKAGE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t package_id = schema_operation.package_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.package_id_ = package_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_PACKAGE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_package_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped package id", K(hash_ret), KR(ret));
      } else {
        const ObSimplePackageSchema *package = NULL;
        if (OB_FAIL(schema_mgr.package_mgr_.get_package_schema(package_id, package))) {
          LOG_WARN("failed to get package schema", K(package_id), KR(ret));
        } else if (NULL != package) {
          hash_ret = schema_keys.del_package_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del package id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_package_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new package id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_package_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_PACKAGE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_PACKAGE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t package_id = schema_operation.package_id_;
    int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.package_id_ = package_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_PACKAGE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimplePackageSchema *package = NULL;
    if (OB_FAIL(schema_mgr.package_mgr_.get_package_schema(package_id, package))) {
      LOG_WARN("get package schema failed", K(package_id), KR(ret));
    } else if (NULL != package) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_package_keys_,
          schema_keys.new_package_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_trigger_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TRIGGER_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TRIGGER_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t trigger_id = schema_operation.trigger_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.trigger_id_ = trigger_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_TRIGGER == schema_operation.op_type_) {
      hash_ret = schema_keys.new_trigger_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped trigger id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleTriggerSchema *trigger = NULL;
        if (OB_FAIL(schema_mgr.trigger_mgr_.get_trigger_schema(trigger_id, trigger))) {
          LOG_WARN("failed to get trigger schema", K(trigger_id), KR(ret));
        } else if (NULL != trigger) {
          hash_ret = schema_keys.del_trigger_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del trigger id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_trigger_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new trigger id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_trigger_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_TRIGGER_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TRIGGER_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t trigger_id = schema_operation.trigger_id_;
    int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.trigger_id_ = trigger_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_TRIGGER == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleTriggerSchema *trigger = NULL;
    if (OB_FAIL(schema_mgr.trigger_mgr_.get_trigger_schema(trigger_id, trigger))) {
      LOG_WARN("get trigger schema failed", K(trigger_id), KR(ret));
    } else if (NULL != trigger) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_trigger_keys_,
          schema_keys.new_trigger_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_db_priv_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const ObString &database_name = schema_operation.database_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey db_priv_key;
    db_priv_key.tenant_id_ = tenant_id;
    db_priv_key.user_id_ = user_id;
    db_priv_key.database_name_ = database_name;
    db_priv_key.schema_version_ = schema_version;
    if (OB_DDL_DEL_DB_PRIV == schema_operation.op_type_) { //delete
      hash_ret = schema_keys.new_db_priv_keys_.erase_refactored(db_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del db_priv_key from new_db_priv_keys", KR(ret));
      } else {
        const ObDBPriv *db_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_db_priv(
            ObOriginalDBKey(tenant_id, user_id, database_name), db_priv, true))) {
          LOG_WARN("get db priv set failed", KR(ret));
        } else if (NULL != db_priv) {
          hash_ret = schema_keys.del_db_priv_keys_.set_refactored_1(db_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add db_priv_key to del_db_priv_keys", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_db_priv_keys_.set_refactored_1(db_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new db_priv_key", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_db_priv_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const ObString &database_name = schema_operation.database_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.user_id_ = user_id;
    schema_key.database_name_ = database_name;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_GRANT_REVOKE_DB == schema_operation.op_type_);
    bool is_exist = false;
    const ObDBPriv *db_priv = NULL;
    if (OB_FAIL(schema_mgr.priv_mgr_.get_db_priv(schema_key.get_db_priv_key(), db_priv, true))) {
      LOG_WARN("get db_priv failed",
               "db_priv_key", schema_key.get_db_priv_key(),
               KR(ret));
    } else if (NULL != db_priv) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_db_priv_keys_,
          schema_keys.new_db_priv_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_sys_priv_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t grantee_id = schema_operation.grantee_id_;

    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey sys_priv_key;
    sys_priv_key.tenant_id_ = tenant_id;
    sys_priv_key.grantee_id_ = grantee_id;
    sys_priv_key.schema_version_ = schema_version;
    if (OB_DDL_SYS_PRIV_DELETE == schema_operation.op_type_) { //delete
      hash_ret = schema_keys.new_sys_priv_keys_.erase_refactored(sys_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del sys_priv_key from new_sys_priv_keys", KR(ret));
      } else {
        const ObSysPriv *sys_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_sys_priv(
            ObSysPrivKey(tenant_id, grantee_id), sys_priv))) {
          LOG_WARN("get sys priv set failed", KR(ret));
        } else if (NULL != sys_priv) {
          hash_ret = schema_keys.del_sys_priv_keys_.set_refactored_1(sys_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add sys_priv_key to del_sys_priv_keys", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_sys_priv_keys_.set_refactored_1(sys_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new sys_priv_key", K(hash_ret), KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_sys_priv_keys_reversely(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t grantee_id = schema_operation.grantee_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.grantee_id_ = grantee_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = OB_DDL_SYS_PRIV_GRANT_REVOKE == schema_operation.op_type_;
    bool is_exist = false;
    const ObSysPriv *sys_priv = NULL;
    if (OB_FAIL(schema_mgr.priv_mgr_.get_sys_priv(schema_key.get_sys_priv_key(), sys_priv))) {
      LOG_WARN("get sys_priv failed",
               "sys_priv_key", schema_key.get_sys_priv_key(),
               KR(ret));
    } else if (NULL != sys_priv) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_sys_priv_keys_,
          schema_keys.new_sys_priv_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_table_priv_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const ObString &database_name = schema_operation.database_name_;
    const ObString &table_name = schema_operation.table_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey table_priv_key;
    table_priv_key.tenant_id_ = tenant_id;
    table_priv_key.user_id_ = user_id;
    table_priv_key.database_name_ = database_name;
    table_priv_key.table_name_ = table_name;
    table_priv_key.schema_version_ = schema_version;
    if (OB_DDL_DEL_TABLE_PRIV == schema_operation.op_type_) { //delete
      hash_ret = schema_keys.new_table_priv_keys_.erase_refactored(table_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del table_priv_key from new_table_priv_keys", KR(ret));
      } else {
        const ObTablePriv *table_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_table_priv(
            ObTablePrivSortKey(tenant_id, user_id, database_name, table_name), table_priv))) {
          LOG_WARN("get table priv failed", KR(ret));
        } else if (NULL != table_priv) {
          hash_ret = schema_keys.del_table_priv_keys_.set_refactored_1(table_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add table_priv_key to del_table_priv_keys", KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_table_priv_keys_.set_refactored_1(table_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new table_priv_key", KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_table_priv_keys_reversely(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const ObString &database_name = schema_operation.database_name_;
    const ObString &table_name = schema_operation.table_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.user_id_ = user_id;
    schema_key.database_name_ = database_name;
    schema_key.table_name_ = table_name;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_GRANT_REVOKE_TABLE == schema_operation.op_type_);
    bool is_exist = false;
    const ObTablePriv *table_priv = NULL;
    if (OB_FAIL(schema_mgr.priv_mgr_.get_table_priv(schema_key.get_table_priv_key(),
                                                    table_priv))) {
      LOG_WARN("get table_priv failed",
               "table_priv_key", schema_key.get_table_priv_key(),
               KR(ret));
    } else if (NULL != table_priv) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_table_priv_keys_,
          schema_keys.new_table_priv_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_obj_priv_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t obj_id = schema_operation.get_obj_id();
    const uint64_t obj_type = schema_operation.get_obj_type();
    const uint64_t col_id = schema_operation.get_col_id();
    const uint64_t grantee_id = schema_operation.get_grantee_id();
    const uint64_t grantor_id = schema_operation.get_grantor_id();
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey obj_priv_key;
    obj_priv_key.tenant_id_ = tenant_id;
    obj_priv_key.table_id_ = obj_id;
    obj_priv_key.obj_type_ = obj_type;
    obj_priv_key.col_id_ = col_id;
    obj_priv_key.grantee_id_ = grantee_id;
    obj_priv_key.grantor_id_ = grantor_id;
    obj_priv_key.schema_version_ = schema_version;
    if (OB_DDL_OBJ_PRIV_DELETE == schema_operation.op_type_) { //delete
      hash_ret = schema_keys.new_obj_priv_keys_.erase_refactored(obj_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del table_priv_key from new_obj_priv_keys", KR(ret));
      } else {
        const ObObjPriv *obj_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_obj_priv(
            ObObjPrivSortKey(tenant_id, obj_id, obj_type, col_id, grantor_id, grantee_id),
            obj_priv))) {
          LOG_WARN("get obj priv failed", KR(ret));
        } else if (NULL != obj_priv) {
          hash_ret = schema_keys.del_obj_priv_keys_.set_refactored_1(obj_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add obj_priv_key to del_obj_priv_keys", KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_obj_priv_keys_.set_refactored_1(obj_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new obj_priv_key", KR(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_obj_priv_keys_reversely(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t obj_id = schema_operation.get_obj_id();
    const uint64_t obj_type = schema_operation.get_obj_type();
    const uint64_t col_id = schema_operation.get_col_id();
    const uint64_t grantee_id = schema_operation.get_grantee_id();
    const uint64_t grantor_id = schema_operation.get_grantor_id();
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.table_id_ = obj_id;
    schema_key.obj_type_ = obj_type;
    schema_key.col_id_ = col_id;
    schema_key.grantee_id_ = grantee_id;
    schema_key.grantor_id_ = grantor_id;
    schema_key.schema_version_ = schema_version;

    bool is_delete = (OB_DDL_OBJ_PRIV_GRANT_REVOKE == schema_operation.op_type_);
    bool is_exist = false;
    const ObObjPriv *obj_priv = NULL;
    if (OB_FAIL(schema_mgr.priv_mgr_.get_obj_priv(schema_key.get_obj_priv_key(),
                                                  obj_priv))) {
      LOG_WARN("get obj_priv failed",
               "obj_priv_key", schema_key.get_obj_priv_key(),
               KR(ret));
    } else if (NULL != obj_priv) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_obj_priv_keys_,
          schema_keys.new_obj_priv_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_udf_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const ObString &udf_name = schema_operation.udf_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.udf_name_ = udf_name;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_UDF == schema_operation.op_type_) {
      hash_ret = schema_keys.new_udf_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped udf id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleUDFSchema *udf = NULL;
        if (OB_FAIL(schema_mgr.udf_mgr_.get_udf_schema_with_name(tenant_id, udf_name, udf))) {
          LOG_WARN("failed to get udf schema", K(udf_name), KR(ret));
        } else if (NULL != udf) {
          hash_ret = schema_keys.del_udf_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del udf id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_udf_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new udf id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_udf_keys_reversely(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const ObString &udf_name = schema_operation.udf_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.udf_name_ = udf_name;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_UDF == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleUDFSchema *udf = NULL;
    if (OB_FAIL(schema_mgr.udf_mgr_.get_udf_schema_with_name(tenant_id, udf_name, udf))) {
      LOG_WARN("get udf schema failed", K(udf_name), KR(ret));
    } else if (NULL != udf) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_udf_keys_,
          schema_keys.new_udf_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_udt_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_UDT_OPERATION_BEGIN &&
      schema_operation.op_type_ < OB_DDL_UDT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t object_id = schema_operation.udt_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.udt_id_ = object_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_UDT == schema_operation.op_type_) {
      hash_ret = schema_keys.new_udt_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped udt id", K(hash_ret), KR(ret));
      } else {
        const ObSimpleUDTSchema *udt = NULL;
        if (OB_FAIL(schema_mgr.udt_mgr_.get_udt_schema(object_id, udt))) {
          LOG_WARN("failed to get udt schema", K(object_id), KR(ret));
        } else if (NULL != udt) {
          hash_ret = schema_keys.del_udt_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del udt id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_udt_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new udt id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_udt_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_UDT_OPERATION_BEGIN &&
      schema_operation.op_type_ < OB_DDL_UDT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t udt_id = schema_operation.udt_id_;
    int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.udt_id_ = udt_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_UDT == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleUDTSchema *udt = NULL;
    if (OB_FAIL(schema_mgr.udt_mgr_.get_udt_schema(udt_id, udt))) {
      LOG_WARN("get udt schema failed", K(udt_id), KR(ret));
    } else if (NULL != udt) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_udt_keys_,
          schema_keys.new_udt_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_policy_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_POLICY_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_POLICY_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t label_se_policy_id = schema_operation.label_se_policy_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_policy_id_ = label_se_policy_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_LABEL_SE_POLICY) {
      hash_ret = schema_keys.new_label_se_policy_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped policy id", K(hash_ret), KR(ret));
      } else {
        const ObLabelSePolicySchema *schema = NULL;
        if (OB_FAIL(schema_mgr.label_se_policy_mgr_.get_schema_by_id(label_se_policy_id, schema))) {
          LOG_WARN("failed to get label security policy schema", K(label_se_policy_id), KR(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_label_se_policy_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del policy id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_label_se_policy_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new policy id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_policy_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_POLICY_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_POLICY_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t schema_id = schema_operation.label_se_policy_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_policy_id_ = schema_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (ObSchemaOperation::type_semantic_str(schema_operation.op_type_)
                      == ObString("CREATE"));
    bool is_exist = false;
    const ObLabelSePolicySchema *schema = NULL;
    if (OB_FAIL(schema_mgr.label_se_policy_mgr_.get_schema_by_id(
                  schema_id,
                  schema))) {
      LOG_WARN("get schema failed", KR(ret), K(schema_id));
    } else if (NULL != schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_label_se_policy_keys_,
          schema_keys.new_label_se_policy_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_component_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_COMPONENT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_COMPONENT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t label_se_component_id = schema_operation.label_se_component_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_component_id_ = label_se_component_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_LABEL_SE_LEVEL
        || schema_operation.op_type_ == OB_DDL_DROP_LABEL_SE_COMPARTMENT
        || schema_operation.op_type_ == OB_DDL_DROP_LABEL_SE_GROUP) {
      hash_ret = schema_keys.new_label_se_component_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped policy id", K(hash_ret), KR(ret));
      } else {
        const ObLabelSeComponentSchema *schema = NULL;
        if (OB_FAIL(schema_mgr.label_se_component_mgr_.get_schema_by_id(label_se_component_id, schema))) {
          LOG_WARN("failed to get label security policy schema", K(label_se_component_id), KR(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_label_se_component_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del policy id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_label_se_component_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new policy id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_component_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_COMPONENT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_COMPONENT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t schema_id = schema_operation.label_se_component_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_component_id_ = schema_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (ObSchemaOperation::type_semantic_str(schema_operation.op_type_)
                      == ObString("CREATE"));
    bool is_exist = false;
    const ObLabelSeComponentSchema *schema = NULL;
    if (OB_FAIL(schema_mgr.label_se_component_mgr_.get_schema_by_id(
                  schema_id,
                  schema))) {
      LOG_WARN("get schema failed", KR(ret), K(schema_id));
    } else if (NULL != schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_label_se_component_keys_,
          schema_keys.new_label_se_component_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_label_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_LABEL_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_LABEL_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t label_se_label_id = schema_operation.label_se_label_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_label_id_ = label_se_label_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_LABEL_SE_LABEL) {
      hash_ret = schema_keys.new_label_se_label_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped policy id", K(hash_ret), KR(ret));
      } else {
        const ObLabelSeLabelSchema *schema = NULL;
        if (OB_FAIL(schema_mgr.label_se_label_mgr_.get_schema_by_id(label_se_label_id, schema))) {
          LOG_WARN("failed to get label security policy schema", K(label_se_label_id), KR(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_label_se_label_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del policy id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_label_se_label_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new policy id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_label_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_LABEL_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_LABEL_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t schema_id = schema_operation.label_se_label_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_label_id_ = schema_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (ObSchemaOperation::type_semantic_str(schema_operation.op_type_)
                      == ObString("CREATE"));
    bool is_exist = false;
    const ObLabelSeLabelSchema *schema = NULL;
    if (OB_FAIL(schema_mgr.label_se_label_mgr_.get_schema_by_id(
                  schema_id,
                  schema))) {
      LOG_WARN("get schema failed", KR(ret), K(schema_id));
    } else if (NULL != schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_label_se_label_keys_,
          schema_keys.new_label_se_label_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_user_level_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_USER_LABEL_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_USER_LABEL_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t label_se_user_level_id = schema_operation.label_se_user_level_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_user_level_id_ = label_se_user_level_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_LABEL_SE_USER_LEVELS) {
      hash_ret = schema_keys.new_label_se_user_level_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped schema id", K(hash_ret), KR(ret));
      } else {
        const ObLabelSeUserLevelSchema *schema = NULL;
        if (OB_FAIL(schema_mgr.label_se_user_level_mgr_.get_schema_by_id(label_se_user_level_id, schema))) {
          LOG_WARN("failed to get label security schema", K(label_se_user_level_id), KR(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_label_se_user_level_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del schema id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_label_se_user_level_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new schema id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_label_se_user_level_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_LABEL_SE_USER_LABEL_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_LABEL_SE_USER_LABEL_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t schema_id = schema_operation.label_se_user_level_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.label_se_user_level_id_ = schema_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (ObSchemaOperation::type_semantic_str(schema_operation.op_type_)
                      == ObString("CREATE"));
    bool is_exist = false;
    const ObLabelSeUserLevelSchema *schema = NULL;
    if (OB_FAIL(schema_mgr.label_se_user_level_mgr_.get_schema_by_id(
                  schema_id,
                  schema))) {
      LOG_WARN("get schema failed", KR(ret), K(schema_id));
    } else if (NULL != schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_label_se_user_level_keys_,
          schema_keys.new_label_se_user_level_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_sequence_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t sequence_id = schema_operation.sequence_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.sequence_id_ = sequence_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_SEQUENCE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_sequence_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped sequence id", K(hash_ret), KR(ret));
      } else {
        const ObSequenceSchema *sequence = NULL;
        if (OB_FAIL(schema_mgr.sequence_mgr_.get_sequence_schema(sequence_id, sequence))) {
          LOG_WARN("failed to get sequence schema", K(sequence_id), KR(ret));
        } else if (NULL != sequence) {
          hash_ret = schema_keys.del_sequence_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del sequence id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_sequence_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new sequence id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_sequence_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t database_id = schema_operation.database_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = database_id;
    schema_key.sequence_id_ = schema_operation.sequence_id_;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_SEQUENCE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSequenceSchema *sequence = NULL;
    if (OB_FAIL(schema_mgr.sequence_mgr_.get_sequence_schema(schema_key.sequence_id_, sequence))) {
      LOG_WARN("failed to get sequence schema", K(schema_key), KR(ret));
    } else if (NULL != sequence) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_sequence_keys_,
          schema_keys.new_sequence_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_context_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_CONTEXT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_CONTEXT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    schema_key.context_id_ = schema_operation.context_id_;
    if (OB_DDL_DROP_CONTEXT == schema_operation.op_type_) {
      hash_ret = schema_keys.new_context_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped context", K(hash_ret), KR(ret));
      } else {
        const ObContextSchema *context = NULL;
        if (OB_FAIL(schema_mgr.context_mgr_.get_context_schema(schema_key.tenant_id_, schema_operation.context_id_, context))) {
          LOG_WARN("failed to get context schema", K(schema_operation.context_id_), KR(ret));
        } else if (NULL != context) {
          hash_ret = schema_keys.del_context_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del context", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_context_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new context", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_context_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_CONTEXT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_CONTEXT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.context_id_ = schema_operation.context_id_;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_CONTEXT == schema_operation.op_type_);
    bool is_exist = false;
    const ObContextSchema *context = NULL;
    if (OB_FAIL(schema_mgr.context_mgr_.get_context_schema(schema_key.tenant_id_, schema_key.context_id_, context))) {
      LOG_WARN("failed to get context schema", K(schema_key), KR(ret));
    } else if (NULL != context) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_context_keys_,
          schema_keys.new_context_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_mock_fk_parent_table_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(schema_operation.op_type_));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.mock_fk_parent_table_id_ = schema_operation.mock_fk_parent_table_id_;
    if (OB_DDL_DROP_MOCK_FK_PARENT_TABLE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_mock_fk_parent_table_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped mock_fk_parent_table", KR(ret), K(hash_ret));
      } else {
        const ObSimpleMockFKParentTableSchema *mock_fk_parent_table = NULL;
        if (OB_FAIL(schema_mgr.mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schema(
            schema_key.tenant_id_,
            schema_operation.mock_fk_parent_table_id_,
            mock_fk_parent_table))) {
          LOG_WARN("failed to get schema", KR(ret), K(schema_operation.mock_fk_parent_table_id_));
        } else if (NULL != mock_fk_parent_table) {
          hash_ret = schema_keys.del_mock_fk_parent_table_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del mock_fk_parent_table", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_mock_fk_parent_table_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new mock_fk_parent_table", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_mock_fk_parent_table_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(schema_operation.op_type_));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.database_id_ = schema_operation.database_id_;
    schema_key.mock_fk_parent_table_id_ = schema_operation.mock_fk_parent_table_id_;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_MOCK_FK_PARENT_TABLE == schema_operation.op_type_);
    bool is_exist = false;
    const ObSimpleMockFKParentTableSchema *mock_fk_parent_table = NULL;
    if (OB_FAIL(schema_mgr.mock_fk_parent_table_mgr_.get_mock_fk_parent_table_schema(
        schema_key.tenant_id_,
        schema_operation.mock_fk_parent_table_id_,
        mock_fk_parent_table))) {
      LOG_WARN("failed to get mock_fk_parent_table schema", KR(ret), K(schema_key));
    } else if (NULL != mock_fk_parent_table) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_mock_fk_parent_table_keys_,
          schema_keys.new_mock_fk_parent_table_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_audit_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!(schema_operation.op_type_ > OB_DDL_AUDIT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_AUDIT_OPERATION_END))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = schema_operation.tenant_id_;
    schema_key.audit_id_ = schema_operation.audit_id_;
    schema_key.schema_version_ = schema_operation.schema_version_;
    LOG_DEBUG("get_increment_audit_keys", K(schema_operation), K(schema_key));
    if (OB_DDL_DEL_AUDIT == schema_operation.op_type_) {
      hash_ret = schema_keys.new_audit_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped audit id", K(hash_ret), KR(ret));
      } else {
        const ObSAuditSchema *audit = NULL;
        if (OB_FAIL(schema_mgr.audit_mgr_.get_audit_schema(schema_key.audit_id_, audit))) {
          LOG_WARN("failed to get audit schema", K(schema_key), KR(ret));
        } else if (NULL != audit) {
          hash_ret = schema_keys.del_audit_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del audit id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_audit_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new audit id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_audit_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!(schema_operation.op_type_ > OB_DDL_AUDIT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_AUDIT_OPERATION_END))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    SchemaKey schema_key;
    schema_key.tenant_id_ = schema_operation.tenant_id_;
    schema_key.audit_id_ = schema_operation.audit_id_;
    schema_key.schema_version_ = schema_operation.schema_version_;
    bool is_delete = (OB_DDL_ADD_AUDIT == schema_operation.op_type_);
    bool is_exist = false;
    const ObSAuditSchema *audit = NULL;
    if (OB_FAIL(schema_mgr.audit_mgr_.get_audit_schema(schema_key.audit_id_, audit))) {
      LOG_WARN("failed to get  schema", K(schema_key), KR(ret));
    } else if (NULL != audit) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_audit_keys_,
          schema_keys.new_audit_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_keystore_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_mgr);
  if (!(schema_operation.op_type_ > OB_DDL_KEYSTORE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_KEYSTORE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t keystore_id = schema_operation.keystore_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.keystore_id_ = keystore_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_CREATE_KEYSTORE == schema_operation.op_type_ ||
        OB_DDL_ALTER_KEYSTORE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_keystore_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new keystore id", K(hash_ret), KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the op_type is unexpected", KR(ret));
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_keystore_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_KEYSTORE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_KEYSTORE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t keystore_id = schema_operation.keystore_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.keystore_id_ = keystore_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_KEYSTORE == schema_operation.op_type_);
    bool is_exist = false;

    const ObKeystoreSchema *keystore = NULL;
    if (OB_FAIL(schema_mgr.keystore_mgr_.get_keystore_schema(schema_key.tenant_id_, keystore))) {
      LOG_WARN("failed to get keystore schema", K(schema_key), KR(ret));
    } else if (NULL != keystore) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_keystore_keys_,
          schema_keys.new_keystore_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_tablespace_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLESPACE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLESPACE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t tablespace_id = schema_operation.tablespace_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.tablespace_id_ = tablespace_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_TABLESPACE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tablespace_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped tablespace key", K(hash_ret), KR(ret));
      } else {
        const ObTablespaceSchema *tablespace_schema = NULL;
        if (OB_FAIL(schema_mgr.tablespace_mgr_.get_tablespace_schema(tablespace_id, tablespace_schema))) {
          LOG_WARN("failed to get tablespace_schema", K(tablespace_id), KR(ret));
        } else if (NULL != tablespace_schema) {
          hash_ret = schema_keys.del_tablespace_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del tablespace id", K(hash_ret), KR(ret));
          }
        }
      }
    } else if (OB_DDL_CREATE_TABLESPACE == schema_operation.op_type_ ||
               OB_DDL_ALTER_TABLESPACE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tablespace_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new tablespace id", K(hash_ret), KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it's unexpected operation type", KR(ret), K(schema_operation.op_type_));
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_tablespace_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_TABLESPACE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_TABLESPACE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.tablespace_id_ = schema_operation.tablespace_id_;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_TABLESPACE == schema_operation.op_type_);
    bool is_exist = false;
    const ObTablespaceSchema *tablespace_schema = NULL;
    if (OB_FAIL(schema_mgr.tablespace_mgr_.get_tablespace_schema(schema_key.tablespace_id_, tablespace_schema))) {
      LOG_WARN("failed to get tablespace_schema", K(schema_key), KR(ret));
    } else if (NULL != tablespace_schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_tablespace_keys_,
          schema_keys.new_tablespace_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_profile_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t profile_id = schema_operation.profile_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.profile_id_ = profile_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_PROFILE) {
      hash_ret = schema_keys.new_profile_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped profile id", K(hash_ret), KR(ret));
      } else {
        const ObProfileSchema *schema = NULL;
        if (OB_FAIL(schema_mgr.profile_mgr_.get_schema_by_id(profile_id, schema))) {
          LOG_WARN("failed to get label security profile schema", K(profile_id), KR(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_profile_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del profile id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_profile_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new profile id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_profile_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.profile_id_ = schema_operation.profile_id_;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (ObSchemaOperation::type_semantic_str(schema_operation.op_type_)
                     == ObString("CREATE"));
    bool is_exist = false;
    const ObProfileSchema *schema = NULL;
    if (OB_FAIL(schema_mgr.profile_mgr_.get_schema_by_id(schema_key.profile_id_, schema))) {
      LOG_WARN("failed to get schema", K(schema_key), KR(ret));
    } else if (NULL != schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_profile_keys_,
          schema_keys.new_profile_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_dblink_keys(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaOperation &schema_operation,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
      schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t dblink_id = schema_operation.dblink_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.dblink_id_ = dblink_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_DBLINK == schema_operation.op_type_) {
      hash_ret = schema_keys.new_dblink_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped dblink id", K(hash_ret), KR(ret));
      } else {
        const ObDbLinkSchema *dblink_schema = NULL;
        if (OB_FAIL(schema_mgr.dblink_mgr_.get_dblink_schema(dblink_id, dblink_schema))) {
          LOG_WARN("failed to get dblink schema", K(dblink_id), KR(ret));
        } else if (NULL != dblink_schema) {
          hash_ret = schema_keys.del_dblink_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del dblink id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_dblink_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new dblink id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_dblink_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
      schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t dblink_id = schema_operation.dblink_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.dblink_id_ = dblink_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_DBLINK == schema_operation.op_type_);
    bool is_exist = false;
    const ObDbLinkSchema *dblink_schema = NULL;
    if (OB_FAIL(schema_mgr.dblink_mgr_.get_dblink_schema(dblink_id, dblink_schema))) {
      LOG_WARN("get dblink schema failed", K(dblink_id), KR(ret));
    } else if (NULL != dblink_schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_dblink_keys_,
          schema_keys.new_dblink_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_directory_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DIRECTORY_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_DIRECTORY_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t directory_id = schema_operation.directory_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.directory_id_ = directory_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_DIRECTORY) {
      hash_ret = schema_keys.new_directory_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped directory id", K(hash_ret), KR(ret));
      } else {
        const ObDirectorySchema *schema = NULL;
        if (OB_FAIL(schema_mgr.directory_mgr_.get_directory_schema_by_id(directory_id, schema))) {
          LOG_WARN("failed to get directory schema", K(directory_id), KR(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_directory_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del directory id", K(hash_ret), KR(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_directory_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new directory id", K(hash_ret), KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_directory_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DIRECTORY_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_DIRECTORY_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t directory_id = schema_operation.directory_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.directory_id_ = directory_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_DIRECTORY == schema_operation.op_type_);
    bool is_exist = false;
    const ObDirectorySchema *directory_schema = NULL;
    if (OB_FAIL(schema_mgr.directory_mgr_.get_directory_schema_by_id(directory_id, directory_schema))) {
      LOG_WARN("get directory schema failed", K(directory_id), KR(ret));
    } else if (NULL != directory_schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_directory_keys_,
          schema_keys.new_directory_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_rls_policy_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_RLS_POLICY_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_RLS_POLICY_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t rls_policy_id = schema_operation.rls_policy_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.rls_policy_id_ = rls_policy_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_RLS_POLICY) {
      hash_ret = schema_keys.new_rls_policy_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped rls_policy id", K(hash_ret), K(ret));
      } else {
        const ObRlsPolicySchema *schema = NULL;
        if (OB_FAIL(schema_mgr.rls_policy_mgr_.get_schema_by_id(rls_policy_id, schema))) {
          LOG_WARN("failed to get rls_policy schema", K(rls_policy_id), K(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_rls_policy_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del rls_policy id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_rls_policy_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new rls_policy id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_rls_policy_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_RLS_POLICY_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_RLS_POLICY_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t rls_policy_id = schema_operation.rls_policy_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.rls_policy_id_ = rls_policy_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_RLS_POLICY == schema_operation.op_type_);
    bool is_exist = false;
    const ObRlsPolicySchema *rls_policy_schema = NULL;
    if (OB_FAIL(schema_mgr.rls_policy_mgr_.get_schema_by_id(rls_policy_id, rls_policy_schema))) {
      LOG_WARN("get rls_policy schema failed", K(rls_policy_id), KR(ret));
    } else if (NULL != rls_policy_schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_rls_policy_keys_,
          schema_keys.new_rls_policy_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_rls_group_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_RLS_GROUP_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_RLS_GROUP_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t rls_group_id = schema_operation.rls_group_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.rls_group_id_ = rls_group_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_RLS_GROUP) {
      hash_ret = schema_keys.new_rls_group_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped rls_group id", K(hash_ret), K(ret));
      } else {
        const ObRlsGroupSchema *schema = NULL;
        if (OB_FAIL(schema_mgr.rls_group_mgr_.get_schema_by_id(rls_group_id, schema))) {
          LOG_WARN("failed to get rls_group schema", K(rls_group_id), K(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_rls_group_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del rls_group id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_rls_group_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new rls_group id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_rls_group_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_RLS_GROUP_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_RLS_GROUP_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t rls_group_id = schema_operation.rls_group_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.rls_group_id_ = rls_group_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_RLS_GROUP == schema_operation.op_type_);
    bool is_exist = false;
    const ObRlsGroupSchema *rls_group_schema = NULL;
    if (OB_FAIL(schema_mgr.rls_group_mgr_.get_schema_by_id(rls_group_id, rls_group_schema))) {
      LOG_WARN("get rls_group schema failed", K(rls_group_id), KR(ret));
    } else if (NULL != rls_group_schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_rls_group_keys_,
          schema_keys.new_rls_group_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_rls_context_keys(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_RLS_CONTEXT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_RLS_CONTEXT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t rls_context_id = schema_operation.rls_context_id_;
    int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.rls_context_id_ = rls_context_id;
    schema_key.schema_version_ = schema_version;
    if (schema_operation.op_type_ == OB_DDL_DROP_RLS_CONTEXT) {
      hash_ret = schema_keys.new_rls_context_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped rls_context id", K(hash_ret), K(ret));
      } else {
        const ObRlsContextSchema *schema = NULL;
        if (OB_FAIL(schema_mgr.rls_context_mgr_.get_schema_by_id(rls_context_id, schema))) {
          LOG_WARN("failed to get rls_context schema", K(rls_context_id), K(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_rls_context_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del rls_context id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_rls_context_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new rls_context id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_rls_context_keys_reversely(
    const ObSchemaMgr &schema_mgr,
    const ObSchemaOperation &schema_operation,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_RLS_CONTEXT_OPERATION_BEGIN
        && schema_operation.op_type_ < OB_DDL_RLS_CONTEXT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), KR(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t rls_context_id = schema_operation.rls_context_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.rls_context_id_ = rls_context_id;
    schema_key.schema_version_ = schema_version;
    bool is_delete = (OB_DDL_CREATE_RLS_CONTEXT == schema_operation.op_type_);
    bool is_exist = false;
    const ObRlsContextSchema *rls_context_schema = NULL;
    if (OB_FAIL(schema_mgr.rls_context_mgr_.get_schema_by_id(rls_context_id, rls_context_schema))) {
      LOG_WARN("get rls_context schema failed", K(rls_context_id), KR(ret));
    } else if (NULL != rls_context_schema) {
      is_exist = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(REPLAY_OP(schema_key, schema_keys.del_rls_context_keys_,
          schema_keys.new_rls_context_keys_, is_delete, is_exist))) {
        LOG_WARN("replay operation failed", KR(ret));
      }
    }
  }
  return ret;
}

// Currently only the full tenant schema of the system tenant is cached
int ObServerSchemaService::add_tenant_schemas_to_cache(const TenantKeys &tenant_keys,
                                                       ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;
  if (OB_FAIL(convert_schema_keys_to_array(tenant_keys, schema_keys))) {
    LOG_WARN("convert set to array failed", K(ret));
  } else {
    FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret)) {
      const uint64_t tenant_id = schema_key->tenant_id_;
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(add_tenant_schema_to_cache(sql_client,
                                               tenant_id,
                                               schema_key->schema_version_))) {
          LOG_WARN("add tenant schema to cache failed", K(ret),
                   K(tenant_id), K(schema_key->schema_version_));
        } else {
          LOG_INFO("add tenant schema to cache success", K(ret), K(schema_key));
        }
        break;
      }
    }
  }
  return ret;
}

// Currently only the full sysvariable schema of system tenants is cached
int ObServerSchemaService::add_sys_variable_schemas_to_cache(
    const SysVariableKeys &sys_variable_keys,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;
  if (OB_FAIL(convert_schema_keys_to_array(sys_variable_keys, schema_keys))) {
    LOG_WARN("convert set to array failed", K(ret));
  } else {
    FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret)) {
      const uint64_t tenant_id = schema_key->tenant_id_;
      if (OB_SYS_TENANT_ID == tenant_id) {
         ObRefreshSchemaStatus schema_status;
         schema_status.tenant_id_ = tenant_id;
        if (OB_FAIL(add_sys_variable_schema_to_cache(sql_client,
                                                     schema_status,
                                                     tenant_id,
                                                     schema_key->schema_version_))) {
          LOG_WARN("add sys variable schema to cache failed", K(ret),
                   K(tenant_id), K(schema_key->schema_version_));
        } else {
          LOG_INFO("add sys variable schema to cache success", K(ret), KPC(schema_key));
        }
        break;
      }
    }
  }
  return ret;
}

int ObServerSchemaService::fetch_increment_schemas(
    const ObRefreshSchemaStatus &schema_status,
    const AllSchemaKeys &all_keys,
    const int64_t schema_version,
    ObISQLClient &sql_client,
    AllSimpleIncrementSchema &simple_incre_schemas)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;

#define GET_BATCH_SCHEMAS(SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS)                               \
  if (OB_SUCC(ret)) {                                                                     \
    schema_keys.reset();                                                                  \
    const SCHEMA_KEYS &new_keys = all_keys.new_##SCHEMA##_keys_;                          \
    ObArray<SCHEMA_TYPE> &simple_schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;\
    if (OB_FAIL(convert_schema_keys_to_array(new_keys, schema_keys))) {                   \
      LOG_WARN("convert set to array failed", KR(ret));                                   \
    } else if (OB_FAIL(schema_service_->get_batch_##SCHEMA##s(schema_status, sql_client,  \
        schema_version, schema_keys, simple_schemas))) {                                  \
      LOG_WARN("get batch "#SCHEMA"s failed", KR(ret), K(schema_keys));                   \
    } else {                                                                              \
      ALLOW_NEXT_LOG();                                                                   \
      LOG_INFO("get batch "#SCHEMA"s success", K(schema_keys));                           \
      if (schema_keys.size() != simple_schemas.size()) {                                  \
        ret = OB_ERR_UNEXPECTED;                                                          \
        LOG_ERROR("unexpected "#SCHEMA" result cnt",                                      \
                  KR(ret), K(schema_keys.size()), K(simple_schemas.size()));              \
      }                                                                                   \
    }                                                                                     \
  }

#define GET_BATCH_SCHEMAS_WITH_ALLOCATOR(SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS)                \
  if (OB_SUCC(ret)) {                                                                     \
    schema_keys.reset();                                                                  \
    const SCHEMA_KEYS &new_keys = all_keys.new_##SCHEMA##_keys_;                          \
    ObArray<SCHEMA_TYPE *> &simple_schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;\
    if (OB_FAIL(convert_schema_keys_to_array(new_keys, schema_keys))) {                   \
      LOG_WARN("convert set to array failed", KR(ret));                                   \
    } else if (OB_FAIL(schema_service_->get_batch_##SCHEMA##s(schema_status, sql_client,  \
      simple_incre_schemas.allocator_, schema_version, schema_keys, simple_schemas))) {   \
      LOG_WARN("get batch "#SCHEMA"s failed", KR(ret), K(schema_keys));                   \
    } else {                                                                              \
      ALLOW_NEXT_LOG();                                                                   \
      LOG_INFO("get batch "#SCHEMA"s success", K(schema_keys));                           \
      if (schema_keys.size() != simple_schemas.size()) {                                  \
        ret = OB_ERR_UNEXPECTED;                                                          \
        LOG_ERROR("unexpected "#SCHEMA" result cnt",                                      \
                  KR(ret), K(schema_keys.size()), K(simple_schemas.size()));              \
      }                                                                                   \
    }                                                                                     \
  }

#define GET_BATCH_SCHEMAS_WITHOUT_SCHEMA_STATUS(SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS) \
  if (OB_SUCC(ret)) {                          \
    schema_keys.reset();                        \
    const SCHEMA_KEYS &new_keys = all_keys.new_##SCHEMA##_keys_;    \
    ObArray<SCHEMA_TYPE> &simple_schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;   \
    if (OB_FAIL(convert_schema_keys_to_array(new_keys, schema_keys))) {      \
      LOG_WARN("convert set to array failed", K(ret));                                    \
    } else if (OB_FAIL(schema_service_->get_batch_##SCHEMA##s(sql_client, \
        schema_version, schema_keys, simple_schemas))) {                                  \
      LOG_WARN("get batch "#SCHEMA"s failed", K(ret), K(schema_keys));                    \
    } else {                                                                              \
      ALLOW_NEXT_LOG();                                                                   \
      LOG_INFO("get batch "#SCHEMA"s success", K(schema_keys));                           \
      if (schema_keys.size() != simple_schemas.size()) {                                  \
        ret = OB_ERR_UNEXPECTED;                                                          \
        LOG_ERROR("unexpected "#SCHEMA" result cnt", K(ret), K(schema_version), K(schema_keys.size()), K(simple_schemas.size())); \
      }                                                                                   \
    }\
  }

  GET_BATCH_SCHEMAS(user, ObSimpleUserSchema, UserKeys);
  GET_BATCH_SCHEMAS(database, ObSimpleDatabaseSchema, DatabaseKeys);
  GET_BATCH_SCHEMAS(tablegroup, ObSimpleTablegroupSchema, TablegroupKeys);
  GET_BATCH_SCHEMAS_WITH_ALLOCATOR(table, ObSimpleTableSchemaV2, TableKeys);
  GET_BATCH_SCHEMAS(outline, ObSimpleOutlineSchema, OutlineKeys);
  GET_BATCH_SCHEMAS(routine, ObSimpleRoutineSchema, RoutineKeys);
  GET_BATCH_SCHEMAS(package, ObSimplePackageSchema, PackageKeys);
  GET_BATCH_SCHEMAS(trigger, ObSimpleTriggerSchema, TriggerKeys);
  GET_BATCH_SCHEMAS(db_priv, ObDBPriv, DBPrivKeys);
  GET_BATCH_SCHEMAS(table_priv, ObTablePriv, TablePrivKeys);
  GET_BATCH_SCHEMAS(synonym, ObSimpleSynonymSchema, SynonymKeys);
  GET_BATCH_SCHEMAS(udf, ObSimpleUDFSchema, UdfKeys);
  GET_BATCH_SCHEMAS(udt, ObSimpleUDTSchema, UDTKeys);
  GET_BATCH_SCHEMAS(sequence, ObSequenceSchema, SequenceKeys);
  GET_BATCH_SCHEMAS(keystore, ObKeystoreSchema, KeystoreKeys);
  GET_BATCH_SCHEMAS(label_se_policy, ObLabelSePolicySchema, LabelSePolicyKeys);
  GET_BATCH_SCHEMAS(label_se_component, ObLabelSeComponentSchema, LabelSeComponentKeys);
  GET_BATCH_SCHEMAS(label_se_label, ObLabelSeLabelSchema, LabelSeLabelKeys);
  GET_BATCH_SCHEMAS(label_se_user_level, ObLabelSeUserLevelSchema, LabelSeUserLevelKeys);
  GET_BATCH_SCHEMAS(tablespace, ObTablespaceSchema, TablespaceKeys);
  GET_BATCH_SCHEMAS(profile, ObProfileSchema, ProfileKeys);
  GET_BATCH_SCHEMAS(audit, ObSAuditSchema, AuditKeys);
  GET_BATCH_SCHEMAS(sys_priv, ObSysPriv, SysPrivKeys);
  GET_BATCH_SCHEMAS(obj_priv, ObObjPriv, ObjPrivKeys);

  // After the schema is split, because the operation_type has not been updated,
  // the OB_DDL_TENANT_OPERATION is still reused
  // For system tenants, there is need to filter the SysVariableKeys of non-system tenants.
  // This is implemented in replay_log/replay_log_reversely
  // It is believed that SysVariableKeys has been filtered
  GET_BATCH_SCHEMAS(sys_variable, ObSimpleSysVariableSchema, SysVariableKeys);
  GET_BATCH_SCHEMAS(dblink, ObDbLinkSchema, DbLinkKeys);
  GET_BATCH_SCHEMAS(directory, ObDirectorySchema, DirectoryKeys);
  GET_BATCH_SCHEMAS(context, ObContextSchema, ContextKeys);
  GET_BATCH_SCHEMAS(mock_fk_parent_table, ObSimpleMockFKParentTableSchema, MockFKParentTableKeys);
  GET_BATCH_SCHEMAS(rls_policy, ObRlsPolicySchema, RlsPolicyKeys);
  GET_BATCH_SCHEMAS(rls_group, ObRlsGroupSchema, RlsGroupKeys);
  GET_BATCH_SCHEMAS(rls_context, ObRlsContextSchema, RlsContextKeys);

  // After the schema is split, ordinary tenants do not refresh the tenant schema and system table schema
  const uint64_t tenant_id = schema_status.tenant_id_;
  if (OB_FAIL(ret)) {
    // skip
  } else if (is_sys_tenant(tenant_id)) {

    GET_BATCH_SCHEMAS_WITHOUT_SCHEMA_STATUS(tenant, ObSimpleTenantSchema, TenantKeys);

    if (OB_SUCC(ret)) {
      schema_keys.reset();
      if (OB_FAIL(convert_schema_keys_to_array(all_keys.alter_tenant_keys_, schema_keys))) {
        LOG_WARN("convert set to array failed", K(ret));
      } else if (OB_FAIL(schema_service_->get_batch_tenants(sql_client, schema_version,
              schema_keys, simple_incre_schemas.alter_tenant_schemas_))) {
        LOG_WARN("get batch tenants failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> non_sys_table_ids;
    if (OB_FAIL(non_sys_table_ids.assign(all_keys.non_sys_table_ids_))) {
      LOG_WARN("fail to assign table_ids", KR(ret), K(schema_status));
    } else if (OB_FAIL(schema_service_->get_batch_table_schema(
        schema_status, schema_version, non_sys_table_ids, sql_client,
        simple_incre_schemas.allocator_, simple_incre_schemas.non_sys_tables_))) {
      LOG_WARN("get non core table schemas failed", KR(ret), K(schema_status), K(schema_version));
    }
  }

#undef GET_BATCH_SCHEMAS
  return ret;

}

int ObServerSchemaService::apply_increment_schema_to_cache(
    const AllSchemaKeys &all_keys,
    const AllSimpleIncrementSchema &simple_incre_schemas,
    ObSchemaMgr &schema_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_mgr.get_tenant_id();
  if (OB_FAIL(apply_tenant_schema_to_cache(
              tenant_id, all_keys, simple_incre_schemas, schema_mgr))) {
    LOG_WARN("fail to apply tenant schema to cache", KR(ret), K(tenant_id));
  } // Need to ensure that the system variables are added first
  else if (OB_FAIL(apply_sys_variable_schema_to_cache(
              tenant_id, all_keys, simple_incre_schemas, schema_mgr.sys_variable_mgr_))) {
    LOG_WARN("fail to apply sys_variable schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_user_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr))) {
    LOG_WARN("fail to apply user schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_database_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr))) {
    LOG_WARN("fail to apply database schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_tablegroup_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr))) {
    LOG_WARN("fail to apply tablegroup schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_table_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr))) {
    LOG_WARN("fail to apply table schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_outline_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.outline_mgr_))) {
    LOG_WARN("fail to apply outline schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_routine_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.routine_mgr_))) {
    LOG_WARN("fail to apply routine schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_package_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.package_mgr_))) {
    LOG_WARN("fail to apply package schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_trigger_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.trigger_mgr_))) {
    LOG_WARN("fail to apply trigger schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_db_priv_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.priv_mgr_))) {
    LOG_WARN("fail to apply db_priv schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_table_priv_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.priv_mgr_))) {
    LOG_WARN("fail to apply table_priv schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_synonym_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.synonym_mgr_))) {
    LOG_WARN("fail to apply synonym schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_udf_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.udf_mgr_))) {
    LOG_WARN("fail to apply udf schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_udt_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.udt_mgr_))) {
    LOG_WARN("fail to apply udt schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_sequence_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.sequence_mgr_))) {
    LOG_WARN("fail to apply sequence schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_keystore_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.keystore_mgr_))) {
    LOG_WARN("fail to apply keystore schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_label_se_policy_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.label_se_policy_mgr_))) {
    LOG_WARN("fail to apply label_se_policy schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_label_se_component_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.label_se_component_mgr_))) {
    LOG_WARN("fail to apply label_se_component schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_label_se_label_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.label_se_label_mgr_))) {
    LOG_WARN("fail to apply label_se_label schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_label_se_user_level_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.label_se_user_level_mgr_))) {
    LOG_WARN("fail to apply label_se_user_level schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_tablespace_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.tablespace_mgr_))) {
    LOG_WARN("fail to apply tablespace schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_profile_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.profile_mgr_))) {
    LOG_WARN("fail to apply profile schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_audit_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.audit_mgr_))) {
    LOG_WARN("fail to apply audit schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_sys_priv_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.priv_mgr_))) {
    LOG_WARN("fail to apply sys_priv schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_obj_priv_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.priv_mgr_))) {
    LOG_WARN("fail to apply obj_priv schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_dblink_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.dblink_mgr_))) {
    LOG_WARN("fail to apply dblink schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_directory_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.directory_mgr_))) {
    LOG_WARN("fail to apply directory schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_context_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.context_mgr_))) {
    LOG_WARN("fail to apply context schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_mock_fk_parent_table_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.mock_fk_parent_table_mgr_))) {
    LOG_WARN("fail to apply mock_fk_parent_table schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_rls_policy_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.rls_policy_mgr_))) {
    LOG_WARN("fail to apply rls_policy schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_rls_group_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.rls_group_mgr_))) {
    LOG_WARN("fail to apply rls_group schema to cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_rls_context_schema_to_cache(
             tenant_id, all_keys, simple_incre_schemas, schema_mgr.rls_context_mgr_))) {
    LOG_WARN("fail to apply rls_context schema to cache", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObServerSchemaService::apply_tenant_schema_to_cache(
    const uint64_t tenant_id,
    const AllSchemaKeys &all_keys,
    const AllSimpleIncrementSchema &simple_incre_schemas,
    ObSchemaMgr &schema_mgr)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  ObArray<SchemaKey> schema_keys;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_schema_keys_to_array(all_keys.del_tenant_keys_, schema_keys))) {
      LOG_WARN("convert set to array failed", K(ret));
    } else {
      FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret)) {
        const uint64_t tenant_id = schema_key->tenant_id_;
        ObDropTenantInfo drop_tenant_info;
        drop_tenant_info.set_tenant_id(schema_key->tenant_id_);
        drop_tenant_info.set_schema_version(schema_key->schema_version_);
        // Delete tenant operation does not record ddl_operation for the sub-schema
        if (OB_FAIL(schema_mgr.del_schemas_in_tenant(tenant_id))) {
          LOG_WARN("del schemas in tenant failed", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_mgr.del_tenant(tenant_id))) {
          LOG_WARN("del tenant failed", K(ret), K(tenant_id));
        }
      }
      ALLOW_NEXT_LOG();
      LOG_INFO("del tenants finish", K(schema_keys), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t del_drop_tenant_keys_count = all_keys.del_drop_tenant_keys_.size();
    int64_t add_drop_tenant_keys_count = all_keys.add_drop_tenant_keys_.size();
    if (0 != del_drop_tenant_keys_count && 0 != add_drop_tenant_keys_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid drop tenant keys count",
               K(ret), K(del_drop_tenant_keys_count), K(add_drop_tenant_keys_count));
    } else if (del_drop_tenant_keys_count > 0) {
      if (OB_FAIL(convert_schema_keys_to_array(all_keys.del_drop_tenant_keys_, schema_keys))) {
        LOG_WARN("convert set to array failed", K(ret));
      } else {
        FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret)) {
          if (OB_FAIL(schema_mgr.del_drop_tenant_info(schema_key->tenant_id_))) {
            LOG_WARN("fail to del drop tenant info", K(ret), K(*schema_key));
          }
        }
      }
    } else if (add_drop_tenant_keys_count > 0) {
      if (OB_FAIL(convert_schema_keys_to_array(all_keys.add_drop_tenant_keys_, schema_keys))) {
        LOG_WARN("convert set to array failed", K(ret));
      } else {
        FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret)) {
          ObDropTenantInfo drop_tenant_info;
          drop_tenant_info.set_tenant_id(schema_key->tenant_id_);
          drop_tenant_info.set_schema_version(schema_key->schema_version_);
          if (OB_FAIL(schema_mgr.add_drop_tenant_info(drop_tenant_info))) {
            LOG_WARN("fail to del drop tenant info", K(ret), K(drop_tenant_info));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_mgr.add_tenants(simple_incre_schemas.alter_tenant_schemas_))) {
        LOG_WARN("add tenants failed", K(ret));
    }
    ALLOW_NEXT_LOG();
    LOG_INFO("add tenants finish",
             "schemas", simple_incre_schemas.alter_tenant_schemas_, K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_mgr.add_tenants(simple_incre_schemas.simple_tenant_schemas_))) {
      LOG_WARN("add tenants failed", K(ret));
    }
    ALLOW_NEXT_LOG();
    LOG_INFO("add tenants finish",
             "schemas", simple_incre_schemas.simple_tenant_schemas_, K(ret));
  }
  return ret;
}

#define APPLY_SCHEMA_TO_CACHE_IMPL(SCHEMA_MGR, SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS) \
int ObServerSchemaService::apply_##SCHEMA##_schema_to_cache( \
    const uint64_t tenant_id, \
    const AllSchemaKeys &all_keys, \
    const AllSimpleIncrementSchema &simple_incre_schemas, \
    SCHEMA_MGR &mgr) \
{ \
  int ret = OB_SUCCESS; \
  ObArray<SchemaKey> schema_keys; \
  const SCHEMA_KEYS &del_keys = all_keys.del_##SCHEMA##_keys_;    \
  if (OB_FAIL(convert_schema_keys_to_array(del_keys, schema_keys))) {    \
    LOG_WARN("convert set to array failed", K(ret));                     \
  } else {                                                               \
    FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret)) {               \
      if (OB_FAIL(mgr.del_##SCHEMA(schema_key->get_##SCHEMA##_key()))) { \
        if (GCTX.is_standby_cluster() \
            && OB_SYS_TENANT_ID == tenant_id \
            && OB_ENTRY_NOT_EXIST == ret) { \
          ret = OB_SUCCESS; \
        } else { \
          LOG_WARN("del "#SCHEMA" failed", K(ret),                         \
                   #SCHEMA"_key", schema_key->get_##SCHEMA##_key());       \
        } \
      }                                                                  \
    }                                                                    \
    ALLOW_NEXT_LOG();                                                    \
    LOG_INFO("del "#SCHEMA"s finish", K(schema_keys), K(ret));           \
  }                                            \
  if (OB_SUCC(ret)) {                          \
    const ObArray<SCHEMA_TYPE> &schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;   \
    if (OB_FAIL(mgr.add_##SCHEMA##s(schemas))) {                          \
      LOG_WARN("add "#SCHEMA"s failed", K(ret),                           \
               #SCHEMA" schemas", schemas);                               \
    }                                                                     \
    ALLOW_NEXT_LOG();                                                     \
    LOG_INFO("add "#SCHEMA"s finish", K(schemas), K(ret));  \
  }                                                         \
  return ret; \
}

APPLY_SCHEMA_TO_CACHE_IMPL(ObSysVariableMgr, sys_variable, ObSimpleSysVariableSchema, SysVariableKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSchemaMgr, user, ObSimpleUserSchema, UserKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSchemaMgr, database, ObSimpleDatabaseSchema, DatabaseKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSchemaMgr, tablegroup, ObSimpleTablegroupSchema, TablegroupKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSchemaMgr, table, ObSimpleTableSchemaV2*, TableKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObOutlineMgr, outline, ObSimpleOutlineSchema, OutlineKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObRoutineMgr, routine, ObSimpleRoutineSchema, RoutineKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObPackageMgr, package, ObSimplePackageSchema, PackageKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObTriggerMgr, trigger, ObSimpleTriggerSchema, TriggerKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObPrivMgr, db_priv, ObDBPriv, DBPrivKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObPrivMgr, table_priv, ObTablePriv, TablePrivKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSynonymMgr, synonym, ObSimpleSynonymSchema, SynonymKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObUDFMgr, udf, ObSimpleUDFSchema, UdfKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObUDTMgr, udt, ObSimpleUDTSchema, UDTKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSequenceMgr, sequence, ObSequenceSchema, SequenceKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObKeystoreMgr, keystore, ObKeystoreSchema, KeystoreKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObLabelSePolicyMgr, label_se_policy, ObLabelSePolicySchema, LabelSePolicyKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObLabelSeCompMgr, label_se_component, ObLabelSeComponentSchema, LabelSeComponentKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObLabelSeLabelMgr, label_se_label, ObLabelSeLabelSchema, LabelSeLabelKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObLabelSeUserLevelMgr, label_se_user_level, ObLabelSeUserLevelSchema, LabelSeUserLevelKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObTablespaceMgr, tablespace, ObTablespaceSchema, TablespaceKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObProfileMgr, profile, ObProfileSchema, ProfileKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObSAuditMgr, audit, ObSAuditSchema, AuditKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObPrivMgr, sys_priv, ObSysPriv, SysPrivKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObPrivMgr, obj_priv, ObObjPriv, ObjPrivKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObDbLinkMgr, dblink, ObDbLinkSchema, DbLinkKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObDirectoryMgr, directory, ObDirectorySchema, DirectoryKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObContextMgr, context, ObContextSchema, ContextKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObMockFKParentTableMgr, mock_fk_parent_table, ObSimpleMockFKParentTableSchema, MockFKParentTableKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObRlsPolicyMgr, rls_policy, ObRlsPolicySchema, RlsPolicyKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObRlsGroupMgr, rls_group, ObRlsGroupSchema, RlsGroupKeys);
APPLY_SCHEMA_TO_CACHE_IMPL(ObRlsContextMgr, rls_context, ObRlsContextSchema, RlsContextKeys);

int ObServerSchemaService::update_schema_mgr(ObISQLClient &sql_client,
                                             const ObRefreshSchemaStatus &schema_status,
                                             ObSchemaMgr &schema_mgr,
                                             const int64_t schema_version,
                                             AllSchemaKeys &all_keys)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    //
    // note: Adjust the position of this code carefully
    // add sys tenant schema to cache
    if (!is_sys_tenant(tenant_id)) {
      // user tenant schema_mgr needs to insert the tenant schema for the placeholder
      ObString fake_tenant_name("fake_tenant");
      ObSimpleTenantSchema tenant;
      tenant.set_tenant_id(tenant_id);
      tenant.set_tenant_name(fake_tenant_name);
      tenant.set_schema_version(schema_version);
      if (OB_FAIL(schema_mgr.add_tenant(tenant))) {
        LOG_WARN("add tenant failed", K(ret), K(tenant));
      }
    } else {
      if (OB_FAIL(add_tenant_schemas_to_cache(all_keys.alter_tenant_keys_, sql_client))) {
        LOG_WARN("add alter tenant schemas to cache failed", K(ret));
      } else if (OB_FAIL(add_tenant_schemas_to_cache(all_keys.new_tenant_keys_, sql_client))) {
        LOG_WARN("add new tenant schemas to cache failed", K(ret));
      } else if (OB_FAIL(add_sys_variable_schemas_to_cache(all_keys.new_sys_variable_keys_, sql_client))) {
        LOG_WARN("add new sys variable schemas to cache failed", K(ret));
      } else {
        // When the system tenant flashes the DDL of the newly created tenant,
        // try to initialize the relevant data structure
        FOREACH_X(it, all_keys.new_tenant_keys_, OB_SUCC(ret)) {
          SchemaKey key = it->first;
          const uint64_t new_tenant_id = key.tenant_id_;
          // try refresh tenant compat mode before tenant refresh schema
          lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
          if (is_sys_tenant(new_tenant_id)) {
            continue;
          } else if (!ObSchemaService::g_liboblog_mode_) {
            ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
            ObRefreshSchemaStatus refresh_schema_status;
            if (OB_ISNULL(schema_status_proxy)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema_status_proxy is null", KR(ret));
            } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(new_tenant_id, refresh_schema_status))) {
              LOG_WARN("fail to get refresh schema status", KR(ret), K(new_tenant_id));
            }
          }
          if (FAILEDx(init_schema_struct(new_tenant_id))) {
            LOG_WARN("fail to init schema struct", K(ret), K(new_tenant_id));
          } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(new_tenant_id, compat_mode))) {
            LOG_WARN("fail to get tenant mode", KR(ret), K(new_tenant_id));
          } else if (OB_FAIL(init_tenant_basic_schema(new_tenant_id))) {
            LOG_WARN("fail to init basic schema struct", K(ret), K(new_tenant_id), K(schema_status));
          } else if (OB_FAIL(init_multi_version_schema_struct(new_tenant_id))) {
            LOG_WARN("fail to init multi version schema struct", K(ret), K(new_tenant_id));
          } else if (OB_FAIL(publish_schema(new_tenant_id))) {
            LOG_WARN("publish_schema failed", KR(ret), K(new_tenant_id));
          } else {
            FLOG_INFO("[REFRESH_SCHEMA] init tenant schema struct", K(new_tenant_id));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      AllSimpleIncrementSchema simple_incre_schemas;
      if (OB_FAIL(fetch_increment_schemas(schema_status, all_keys, schema_version, sql_client, simple_incre_schemas))) {
        LOG_WARN("fetch increment schemas failed", K(ret));
      } else if (OB_FAIL(apply_increment_schema_to_cache(all_keys, simple_incre_schemas, schema_mgr))) {
        LOG_WARN("apply increment schema to cache failed", K(ret));
      } else if (OB_FAIL(update_non_sys_schemas_in_cache_(schema_mgr, simple_incre_schemas.non_sys_tables_))) {
        LOG_WARN("update non sys schemas in cache faield", KR(ret));
      }
    }
  }

  // check shema consistent at last
  if (FAILEDx(schema_mgr.rebuild_schema_meta_if_not_consistent())) {
    LOG_ERROR("not consistency for schema meta data", KR(ret));
  }

  return ret;
}

// wrapper for add index and materialized view
int ObServerSchemaService::add_index_tids(
    const ObSchemaMgr &schema_mgr,
    ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  ObArray<const ObSimpleTableSchemaV2 *> simple_indexes;
  if (OB_FAIL(schema_mgr.get_aux_schemas(tenant_id, table_id, simple_indexes, USER_INDEX))) {
    LOG_WARN("get index schemas failed", K(ret));
  } else {
    FOREACH_CNT_X(tmp_simple_index, simple_indexes, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *simple_index = *tmp_simple_index;
      if (OB_ISNULL(simple_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(simple_index), K(ret));
      } else {
        if (simple_index->is_index_table() || simple_index->is_materialized_view()) {
          if (OB_FAIL(table.add_simple_index_info(ObAuxTableMetaInfo(
                             simple_index->get_table_id(),
                             simple_index->get_table_type(),
                             simple_index->get_index_type())))) {
            LOG_WARN("fail to add simple_index_info", K(ret), K(*simple_index));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("not index/mv table", K(simple_index->get_table_type()));
        }
      }
    }
  }
  return ret;
}

int ObServerSchemaService::extract_non_sys_table_ids_(
    const TableKeys &keys,
    ObIArray<uint64_t> &non_sys_table_ids)
{
  int ret = OB_SUCCESS;
  for (TableKeys::const_iterator it = keys.begin();
       OB_SUCC(ret) && it != keys.end(); ++it) {
    const uint64_t table_id = (it->first).table_id_;
    if (is_inner_table(table_id) && !is_sys_table(table_id)) {
      if (OB_FAIL(non_sys_table_ids.push_back(table_id))) {
        LOG_WARN("push_back failed", KR(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::update_non_sys_schemas_in_cache_(
    const ObSchemaMgr &schema_mgr,
    ObIArray<ObTableSchema *> &non_sys_tables)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(non_sys_table, non_sys_tables, OB_SUCC(ret)) {
    if (OB_ISNULL(non_sys_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", KP(non_sys_table), KR(ret));
    } else if (OB_ISNULL(*non_sys_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", KP(*non_sys_table), KR(ret));
    } else if (OB_FAIL(add_aux_schema_from_mgr(schema_mgr, **non_sys_table, USER_INDEX))) {
      LOG_WARN("fail to add aux schema", KR(ret), KPC(*non_sys_table));
    }
  }
  if (FAILEDx(update_schema_cache(non_sys_tables))) {
    LOG_WARN("failed to update schema cache", KR(ret));
  }
  return ret;
}

int ObServerSchemaService::fallback_schema_mgr(
    const ObRefreshSchemaStatus &schema_status,
    ObSchemaMgr &schema_mgr,
    const int64_t schema_version)
{
  FLOG_INFO("[FALLBACK_SCHEMA] fallback schema mgr start", K(schema_status),
            "from_version", schema_mgr.get_schema_version(),
            "target_version", schema_version);
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_version));
  } else {
    ObISQLClient &sql_client = *sql_proxy_;
    SMART_VAR(AllSchemaKeys, all_keys) {
      int64_t start_ver = OB_INVALID_VERSION;
      int64_t end_ver = OB_INVALID_VERSION;
      bool is_fallback = false;
      if (schema_mgr.get_schema_version() < schema_version) {
        start_ver = schema_mgr.get_schema_version();
        end_ver = schema_version;
        is_fallback = false;
      } else {
        start_ver = schema_version;
        end_ver = schema_mgr.get_schema_version();
        is_fallback = true;
      }

      if (start_ver < end_ver) {
        ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
        if (OB_FAIL(schema_service_->get_increment_schema_operations(
            schema_status, start_ver, end_ver, sql_client, schema_operations))) {
          LOG_WARN("get_increment_schema_operations failed",
              K(start_ver), K(end_ver), K(ret));
        } else if (schema_operations.count() > 0) {
          if (is_fallback) {
            if (OB_FAIL(replay_log_reversely(schema_mgr, schema_operations, all_keys))) {
              LOG_WARN("replay log reserse failed", K(ret));
            }
          } else {
            if (OB_FAIL(replay_log(schema_mgr, schema_operations, all_keys))) {
              LOG_WARN("replay log failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_INVALID_TENANT_ID == schema_status.tenant_id_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema status is invalid", KR(ret), K(schema_status));
            } else {
              if (OB_FAIL(update_schema_mgr(sql_client, schema_status, schema_mgr, schema_version, all_keys))) {
                LOG_WARN("update schema mgr failed", K(ret));
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    schema_mgr.set_schema_version(schema_version);
  }

  FLOG_INFO("[FALLBACK_SCHEMA] fallback schema mgr end",
            KR(ret), K(schema_status),
            "from_version", schema_mgr.get_schema_version(),
            "target_version", schema_version,
            "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObServerSchemaService::replay_log(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    int64_t bucket_size = schema_operations.count();
    if (OB_FAIL(schema_keys.create(bucket_size))) {
      LOG_WARN("fail to create hashset: ", K(bucket_size), K(ret));
      ret = OB_INNER_STAT_ERROR;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schema_operations.count(); ++i) {
        const ObSchemaOperation &schema_operation = schema_operations.at(i);
        LOG_INFO("schema operation", K(schema_operation));
        if (schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN
            && schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END) {
          if (OB_FAIL(get_increment_sys_variable_keys(schema_mgr,
                                                      schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sys variable keys", K(ret));
          } else if (OB_FAIL(get_increment_tenant_keys(schema_mgr,
                                               schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment tenant id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END) {
          if (OB_FAIL(get_increment_sys_variable_keys(schema_mgr,
                                                      schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sys variable keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_USER_OPERATION_END) {
          if (OB_FAIL(get_increment_user_keys(schema_mgr,
                                              schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment user id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END) {
          if (OB_FAIL(get_increment_database_keys(schema_mgr,
                                                  schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment database id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END) {
          if (OB_FAIL(get_increment_tablegroup_keys(schema_mgr,
                                                    schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment tablegroup id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END) {
          if (OB_FAIL(get_increment_table_keys(schema_mgr,
                                               schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment table id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END) {
          if (OB_FAIL(get_increment_synonym_keys(schema_mgr,
                                                 schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment synonym id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END) {
          if (OB_FAIL(get_increment_outline_keys(schema_mgr,
                                                 schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment outline id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_db_priv_keys(schema_mgr,
                                                 schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment db priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_table_priv_keys(schema_mgr,
                                                    schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment table priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_ROUTINE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_ROUTINE_OPERATION_END) {
          if (OB_FAIL(get_increment_routine_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment routine id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_PACKAGE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_PACKAGE_OPERATION_END) {
          if (OB_FAIL(get_increment_package_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment procedure id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TRIGGER_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_TRIGGER_OPERATION_END) {
          if (OB_FAIL(get_increment_trigger_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment procedure id", K(ret));
          }

        } else if (schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END) {
          if (OB_FAIL(get_increment_udf_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment udf id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_UDT_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_UDT_OPERATION_END) {
          if (OB_FAIL(get_increment_udt_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("failed to get increment udt id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END) {
          if (OB_FAIL(get_increment_sequence_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sequence id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLESPACE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_TABLESPACE_OPERATION_END) {
          if (OB_FAIL(get_increment_tablespace_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment tablespace keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_KEYSTORE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_KEYSTORE_OPERATION_END) {
          if (OB_FAIL(get_increment_keystore_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment keystore id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_POLICY_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_LABEL_SE_POLICY_OPERATION_END) {
          if (OB_FAIL(get_increment_label_se_policy_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment label security policy keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_COMPONENT_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_LABEL_SE_COMPONENT_OPERATION_END) {
          if (OB_FAIL(get_increment_label_se_component_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment label security component keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_LABEL_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_LABEL_SE_LABEL_OPERATION_END) {
          if (OB_FAIL(get_increment_label_se_label_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment label security label keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_USER_LABEL_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_LABEL_SE_USER_LABEL_OPERATION_END) {
          if (OB_FAIL(get_increment_label_se_user_level_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment label security user level keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END) {
          if (OB_FAIL(get_increment_profile_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment procedure id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_AUDIT_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_AUDIT_OPERATION_END) {
          if (OB_FAIL(get_increment_audit_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment audit id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_sys_priv_keys(schema_mgr,
                                                 schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sys priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_obj_priv_keys(schema_mgr,
                                                  schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment obj priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
            schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END) {
          if (OB_FAIL(get_increment_dblink_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment dblink id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DIRECTORY_OPERATION_BEGIN &&
            schema_operation.op_type_ < OB_DDL_DIRECTORY_OPERATION_END) {
          if (OB_FAIL(get_increment_directory_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment directory id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_CONTEXT_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_CONTEXT_OPERATION_END) {
          if (OB_FAIL(get_increment_context_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment context id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_BEGIN
                    && schema_operation.op_type_ < OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_END) {
          if (OB_FAIL(get_increment_mock_fk_parent_table_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment mock_fk_parent_table id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_RLS_POLICY_OPERATION_BEGIN &&
            schema_operation.op_type_ < OB_DDL_RLS_POLICY_OPERATION_END) {
          if (OB_FAIL(get_increment_rls_policy_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment rls_policy id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_RLS_GROUP_OPERATION_BEGIN &&
            schema_operation.op_type_ < OB_DDL_RLS_GROUP_OPERATION_END) {
          if (OB_FAIL(get_increment_rls_group_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment rls_group id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_RLS_CONTEXT_OPERATION_BEGIN &&
            schema_operation.op_type_ < OB_DDL_RLS_CONTEXT_OPERATION_END) {
          if (OB_FAIL(get_increment_rls_context_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment rls_context id", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(extract_non_sys_table_ids_(schema_keys.new_table_keys_,
                                             schema_keys.non_sys_table_ids_))) {
        LOG_WARN("extract non sys table ids failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::replay_log_reversely(
  const ObSchemaMgr &schema_mgr,
  const ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
  AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;

  int64_t bucket_size = schema_operations.count();
  if (OB_FAIL(schema_keys.create(bucket_size))) {
    LOG_WARN("fail to create hashset: ", K(bucket_size), K(ret));
    ret = OB_INNER_STAT_ERROR;
  } else {
    for (int64_t i = schema_operations.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObSchemaOperation &schema_operation = schema_operations.at(i);
      LOG_INFO("schema operation", K(schema_operation));
      if (schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN
          && schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END) {
        if (OB_FAIL(get_increment_tenant_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment tenant keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END) {
        if (OB_FAIL(get_increment_sys_variable_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment sys_variable keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_USER_OPERATION_END) {
        if (OB_FAIL(get_increment_user_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment user keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END) {
        if (OB_FAIL(get_increment_database_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment database keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END) {
        if (OB_FAIL(get_increment_tablegroup_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment tablegroup keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END) {
        if (OB_FAIL(get_increment_table_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment table keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END) {
        if (OB_FAIL(get_increment_synonym_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment synonym keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END) {
        if (OB_FAIL(get_increment_outline_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment outline keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_ROUTINE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_ROUTINE_OPERATION_END) {
        if (OB_FAIL(get_increment_routine_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment routine keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_UDT_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_UDT_OPERATION_END) {
        if (OB_FAIL(get_increment_udt_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment udt keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_PACKAGE_OPERATION_BEGIN
            && schema_operation.op_type_ < OB_DDL_PACKAGE_OPERATION_END) {
        if (OB_FAIL(get_increment_package_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment package keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_TRIGGER_OPERATION_BEGIN
            && schema_operation.op_type_ < OB_DDL_TRIGGER_OPERATION_END) {
        if (OB_FAIL(get_increment_trigger_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment trigger keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END) {
        if (OB_FAIL(get_increment_db_priv_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment db_priv keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END) {
        if (OB_FAIL(get_increment_table_priv_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment table_priv keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END) {
        if (OB_FAIL(get_increment_udf_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment udf keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END) {
        if (OB_FAIL(get_increment_sequence_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment sequence keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_KEYSTORE_OPERATION_BEGIN
                   && schema_operation.op_type_ < OB_DDL_KEYSTORE_OPERATION_END) {
        if (OB_FAIL(get_increment_keystore_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment keystore keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_POLICY_OPERATION_BEGIN
                && schema_operation.op_type_ < OB_DDL_LABEL_SE_POLICY_OPERATION_END) {
        if (OB_FAIL(get_increment_label_se_policy_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment label_se_policy keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_COMPONENT_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_LABEL_SE_COMPONENT_OPERATION_END) {
        if (OB_FAIL(get_increment_label_se_component_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment label_se_component keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_LABEL_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_LABEL_SE_LABEL_OPERATION_END) {
        if (OB_FAIL(get_increment_label_se_label_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment label_se_label keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_LABEL_SE_USER_LABEL_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_LABEL_SE_USER_LABEL_OPERATION_END) {
        if (OB_FAIL(get_increment_label_se_user_level_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment label_se_user_level keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLESPACE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_TABLESPACE_OPERATION_END) {
        if (OB_FAIL(get_increment_tablespace_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment tablespace keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END) {
        if (OB_FAIL(get_increment_profile_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment profile keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_AUDIT_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_AUDIT_OPERATION_END) {
        if (OB_FAIL(get_increment_audit_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment audit keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END) {
        if (OB_FAIL(get_increment_sys_priv_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment sys_priv keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END) {
        if (OB_FAIL(get_increment_obj_priv_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment obj_priv keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END) {
        if (OB_FAIL(get_increment_dblink_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment dblink keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_DIRECTORY_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_DIRECTORY_OPERATION_END) {
        if (OB_FAIL(get_increment_directory_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment directory keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_CONTEXT_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_CONTEXT_OPERATION_END) {
        if (OB_FAIL(get_increment_context_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment context keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_END) {
        if (OB_FAIL(get_increment_mock_fk_parent_table_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment mock_fk_parent_table keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_RLS_POLICY_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_RLS_POLICY_OPERATION_END) {
        if (OB_FAIL(get_increment_rls_policy_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment rls_policy keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_RLS_GROUP_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_RLS_GROUP_OPERATION_END) {
        if (OB_FAIL(get_increment_rls_group_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment rls_group keys reversely", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_RLS_CONTEXT_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_RLS_CONTEXT_OPERATION_END) {
        if (OB_FAIL(get_increment_rls_context_keys_reversely(schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment rls_context keys reversely", KR(ret));
        }
      } else {
        // ingore other operaton.
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_schemas_for_data_dict(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t start_version,
    common::ObIAllocator &allocator,
    common::ObIArray<const ObTenantSchema *> &tenant_schemas,
    common::ObIArray<const ObDatabaseSchema *> &database_schemas,
    common::ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
  ObRefreshSchemaStatus status;
  status.tenant_id_ = tenant_id;
  int64_t end_version = INT64_MAX - 1;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(tenant_id), K(start_version));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || start_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(start_version));
  } else if (OB_FAIL(schema_service_->get_increment_schema_operations(
             status, start_version, end_version, trans, schema_operations))) {
    LOG_WARN("fail to get increment operations", KR(ret), K(status), K(start_version));
  } else if (schema_operations.count() > 0) {
  SMART_VAR(AllSchemaKeys, schema_keys) {
    if (OB_FAIL(get_increment_schema_keys_for_data_dict_(schema_operations, schema_keys))) {
      LOG_WARN("fail to get increment operations", KR(ret), K(tenant_id), K(start_version));
    } else if (!schema_keys.need_fetch_schemas_for_data_dict()) {
      // skip
    } else if (OB_FAIL(fetch_increment_schemas_for_data_dict_(
               trans, allocator, tenant_id, schema_keys,
               tenant_schemas, database_schemas, table_schemas))) {
      LOG_WARN("fail to get increment schemas for data dict",
               KR(ret), K(tenant_id), K(start_version));
    }
  } // end SMART_VAR
  }
  return ret;
}

// the following members are valid in schema_keys:
// 1. new_tenant_keys_/alter_tenant_keys_
// 2. new_table_keys_
// 3. new_database_keys_
int ObServerSchemaService::get_increment_schema_keys_for_data_dict_(
    const ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
    AllSchemaKeys &schema_keys)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObSchemaMgr, schema_mgr) { // not used
  int64_t bucket_size = schema_operations.count();
  if (OB_FAIL(schema_keys.create(bucket_size))) {
    LOG_WARN("fail to create hashset", KR(ret), K(bucket_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_operations.count(); ++i) {
      const ObSchemaOperation &schema_operation = schema_operations.at(i);
      LOG_TRACE("schema operation", K(schema_operation));
      if (schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN
          && schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END) {
        if (OB_FAIL(get_increment_tenant_keys(
            schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment tenant id", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END) {
        if (OB_FAIL(get_increment_database_keys(
                    schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment database id", KR(ret));
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN
                 && schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END) {
        if (OB_FAIL(get_increment_table_keys(
                    schema_mgr, schema_operation, schema_keys))) {
          LOG_WARN("fail to get increment table id", KR(ret));
        }
      } else {
        // do nothing
      }
    } // end for
  }
  } // end HEAP_VAR
  return ret;
}

int ObServerSchemaService::fetch_increment_schemas_for_data_dict_(
    common::ObMySQLTransaction &trans,
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    const AllSchemaKeys &schema_keys,
    common::ObIArray<const ObTenantSchema *> &tenant_schemas,
    common::ObIArray<const ObDatabaseSchema *> &database_schemas,
    common::ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fetch_increment_tenant_schemas_for_data_dict_(
            trans, allocator, schema_keys, tenant_schemas))) {
    LOG_WARN("fail to fetch increment tenant schemas",
             KR(ret), K(tenant_id), "new_tenants", schema_keys.new_tenant_keys_,
             "alter_tenants", schema_keys.alter_tenant_keys_);
  } else if (OB_FAIL(fetch_increment_database_schemas_for_data_dict_(
            trans, allocator, tenant_id, schema_keys, database_schemas))) {
    LOG_WARN("fail to fetch increment database schemas",
             KR(ret), K(tenant_id), "new_databases", schema_keys.new_database_keys_);
  } else if (OB_FAIL(fetch_increment_table_schemas_for_data_dict_(
            trans, allocator, tenant_id, schema_keys, table_schemas))) {
    LOG_WARN("fail to fetch increment table schemas",
             KR(ret), K(tenant_id), "new_tables", schema_keys.new_table_keys_);
  }
  return ret;
}

int ObServerSchemaService::fetch_increment_tenant_schemas_for_data_dict_(
    common::ObMySQLTransaction &trans,
    common::ObIAllocator &allocator,
    const AllSchemaKeys &schema_keys,
    common::ObIArray<const ObTenantSchema *> &tenant_schemas)
{
  int ret = OB_SUCCESS;
  tenant_schemas.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObArray<uint64_t> tenant_ids;
    FOREACH_X(key, schema_keys.new_tenant_keys_, OB_SUCC(ret)) {
      const uint64_t tenant_id = key->first.get_tenant_key();
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push back tenant keys", KR(ret), K(tenant_id));
      }
    } // end FOREACH_X
    FOREACH_X(key, schema_keys.alter_tenant_keys_, OB_SUCC(ret)) {
      const uint64_t tenant_id = key->first.get_tenant_key();
      int hash_ret = schema_keys.new_tenant_keys_.exist_refactored(key->first);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          LOG_WARN("fail to push back tenant keys", KR(ret), K(tenant_id));
        }
      } else if (OB_HASH_EXIST == hash_ret) {
        // do nothing
      } else {
        ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
        LOG_WARN("fail to check key exist", KR(ret), K(tenant_id));
      }
    } // end FOREACH_X

    int64_t schema_version = INT64_MAX - 1;
    ObSEArray<ObTenantSchema, 2> tmp_tenants;
    if (FAILEDx(schema_service_->get_batch_tenants(
        trans, schema_version, tenant_ids, tmp_tenants))) {
      LOG_WARN("get batch tenants failed", KR(ret), K(tenant_ids));
    } else if (tmp_tenants.count() != tenant_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant cnt not match", KR(ret), K(tenant_ids), K(tmp_tenants));
    } else {
      ObTenantSchema *tenant_ptr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenants.count(); i++) {
        const ObTenantSchema &tenant = tmp_tenants.at(i);
        if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tenant, tenant_ptr))) {
          LOG_WARN("fail to alloc tenant", KR(ret), K(tenant));
        } else if (OB_ISNULL(tenant_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ptr is null", KR(ret), K(tenant));
        } else if (OB_FAIL(tenant_schemas.push_back(tenant_ptr))) {
          LOG_WARN("fail to push back tenant schema", KR(ret), KPC(tenant_ptr));
        } else {
          LOG_INFO("fetch tenant schema for data dict",
                   "tenant_id", tenant_ptr->get_tenant_id(),
                   "schema_version", tenant_ptr->get_schema_version());
        }
      } // end for
    }
  }
  return ret;
}

int ObServerSchemaService::fetch_increment_database_schemas_for_data_dict_(
    common::ObMySQLTransaction &trans,
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    const AllSchemaKeys &schema_keys,
    common::ObIArray<const ObDatabaseSchema *> &database_schemas)
{
  int ret = OB_SUCCESS;
  database_schemas.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObArray<uint64_t> database_ids;
    FOREACH_X(key, schema_keys.new_database_keys_, OB_SUCC(ret)) {
      const uint64_t database_id = key->first.get_database_key().database_id_;
      if (OB_FAIL(database_ids.push_back(database_id))) {
        LOG_WARN("fail to push back database key", KR(ret), K(database_id));
      }
    } // end FOREACH_X
    ObRefreshSchemaStatus status;
    status.tenant_id_ = tenant_id;
    int64_t schema_version = INT64_MAX - 1;
    ObSEArray<ObDatabaseSchema, 2> tmp_databases;
    if (FAILEDx(schema_service_->get_batch_databases(
        status, schema_version, database_ids, trans, tmp_databases))) {
      LOG_WARN("get batch databases failed", KR(ret), K(status), K(database_ids));
    } else if (tmp_databases.count() != database_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database cnt not match", KR(ret), K(database_ids), K(tmp_databases));
    } else {
      ObDatabaseSchema *database_ptr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_databases.count(); i++) {
        const ObDatabaseSchema &database = tmp_databases.at(i);
        if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, database, database_ptr))) {
          LOG_WARN("fail to alloc database", KR(ret), K(database));
        } else if (OB_ISNULL(database_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ptr is null", KR(ret), K(database));
        } else if (OB_FAIL(database_schemas.push_back(database_ptr))) {
          LOG_WARN("fail to push back database schema", KR(ret), KPC(database_ptr));
        } else {
          LOG_INFO("fetch increment database schema for data dict", K(tenant_id),
                   "database_id", database_ptr->get_database_id(),
                   "schema_version", database_ptr->get_schema_version());
        }
      } // end for
    }
  }
  return ret;
}

// won't fetch inner table schemas
int ObServerSchemaService::fetch_increment_table_schemas_for_data_dict_(
    common::ObMySQLTransaction &trans,
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    const AllSchemaKeys &schema_keys,
    common::ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  table_schemas.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObArray<uint64_t> table_ids;
    FOREACH_X(key, schema_keys.new_table_keys_, OB_SUCC(ret)) {
      const uint64_t table_id = key->first.get_table_key().table_id_;
      if (is_inner_table(table_id)) {
        // skip
      } else if (OB_FAIL(table_ids.push_back(table_id))) {
        LOG_WARN("fail to push back table key", KR(ret), K(table_id));
      }
    } // end FOREACH_X
    ObRefreshSchemaStatus status;
    status.tenant_id_ = tenant_id;
    int64_t schema_version = INT64_MAX - 1;
    ObArray<ObTableSchema *> tmp_tables;
    if (FAILEDx(schema_service_->get_batch_table_schema(
        status, schema_version, table_ids, trans, allocator, tmp_tables))) {
      LOG_WARN("get batch tables failed", KR(ret), K(status), K(table_ids));
    } else if (tmp_tables.count() != table_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table cnt not match", KR(ret), K(table_ids), K(tmp_tables));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tables.count(); i++) {
        ObTableSchema *table_schema = tmp_tables.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ptr is null", KR(ret));
        } else if (need_construct_aux_infos_(*table_schema)
                   && OB_FAIL(construct_aux_infos_(
          trans, status, tenant_id, *table_schema))) {
          LOG_WARN("fail to construct aux infos", KR(ret),
                   K(status), K(tenant_id), KPC(table_schema));
        } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
          LOG_WARN("fail to push back ptr", KR(ret), KPC(table_schema));
        } else {
          LOG_INFO("fetch increment table schema for data dict", K(tenant_id),
                   "table_id", table_schema->get_table_id(),
                   "schema_version", table_schema->get_schema_version());
        }
      } // end for
    }
  }
  return ret;
}

bool ObServerSchemaService::need_construct_aux_infos_(
     const ObTableSchema &table_schema)
{
  bool bret = true;
  if (table_schema.is_index_table()
      || (table_schema.is_view_table()
           && !table_schema.is_materialized_view())
       || table_schema.is_aux_vp_table()
       || table_schema.is_aux_lob_table()) {
    bret = false;
  }
  return bret;
}

int ObServerSchemaService::construct_aux_infos_(
    common::ObISQLClient &sql_client,
    const share::schema::ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 8> aux_table_metas;
  const int64_t schema_version = table_schema.get_schema_version();
  const uint64_t table_id = table_schema.get_table_id();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(schema_service_->fetch_aux_tables(
             schema_status, tenant_id, table_id,
             schema_version, sql_client, aux_table_metas))) {
    LOG_WARN("get index failed", KR(ret), K(tenant_id), K(table_id), K(schema_version));
  } else {
    FOREACH_CNT_X(tmp_aux_table_meta, aux_table_metas, OB_SUCC(ret)) {
      const ObAuxTableMetaInfo &aux_table_meta = *tmp_aux_table_meta;
      if (USER_INDEX == aux_table_meta.table_type_) {
        if (OB_FAIL(table_schema.add_simple_index_info(ObAuxTableMetaInfo(
                           aux_table_meta.table_id_,
                           aux_table_meta.table_type_,
                           aux_table_meta.index_type_)))) {
          LOG_WARN("fail to add simple_index_info", KR(ret), K(tenant_id), K(aux_table_meta));
        }
      } else if (AUX_LOB_META == aux_table_meta.table_type_) {
        table_schema.set_aux_lob_meta_tid(aux_table_meta.table_id_);
      } else if (AUX_LOB_PIECE == aux_table_meta.table_type_) {
        table_schema.set_aux_lob_piece_tid(aux_table_meta.table_id_);
      } else if (AUX_VERTIAL_PARTITION_TABLE == aux_table_meta.table_type_) {
        if (OB_FAIL(table_schema.add_aux_vp_tid(aux_table_meta.table_id_))) {
          LOG_WARN("add aux vp table id failed", KR(ret), K(tenant_id), K(aux_table_meta));
        }
      }
    } // end FOREACH_CNT_X
  }
  return ret;
}

int ObServerSchemaService::convert_to_simple_schema(
    const ObTableSchema &schema,
    ObSimpleTableSchemaV2 &simple_schema)
{
  int ret= OB_SUCCESS;

  if (OB_FAIL(simple_schema.assign(schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    simple_schema.set_part_num(schema.get_first_part_num());
    simple_schema.set_def_sub_part_num(schema.get_def_sub_part_num());
  }

  return ret;
}

int ObServerSchemaService::convert_to_simple_schema(
    common::ObIAllocator &allocator,
    const ObIArray<ObTableSchema> &schemas,
    ObIArray<ObSimpleTableSchemaV2 *> &simple_schemas)
{
  int ret= OB_SUCCESS;
  simple_schemas.reset();

  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
    ObSimpleTableSchemaV2 *simple_schema = NULL;
    if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, simple_schema))) {
      LOG_WARN("fail to alloc simple schema", KR(ret));
    } else if (OB_FAIL(convert_to_simple_schema(*schema, *simple_schema))) {
      LOG_WARN("convert failed", KR(ret));
    } else if (OB_FAIL(simple_schemas.push_back(simple_schema))) {
      LOG_WARN("push back failed", KR(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::convert_to_simple_schema(
    common::ObIAllocator &allocator,
    const ObIArray<ObTableSchema *> &schemas,
    ObIArray<ObSimpleTableSchemaV2 *> &simple_schemas)
{
  int ret= OB_SUCCESS;
  simple_schemas.reset();

  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
    const ObTableSchema *table_schema = *schema;
    ObSimpleTableSchemaV2 *simple_schema = NULL;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", KR(ret), KP(table_schema));
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, simple_schema))) {
      LOG_WARN("fail to alloc simple schema", KR(ret));
    } else if (OB_FAIL(convert_to_simple_schema(*table_schema, *simple_schema))) {
      LOG_WARN("convert failed", KR(ret));
    } else if (OB_FAIL(simple_schemas.push_back(simple_schema))) {
      LOG_WARN("push back failed", KR(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::fill_all_core_table_schema(
    const uint64_t tenant_id,
    ObSchemaMgr &schema_mgr_for_cache)
{
  int ret = OB_SUCCESS;
  ObTableSchema all_core_table_schema;
  ObSimpleTableSchemaV2 all_core_table_schema_simple;
  //TODO oushen defualt database and tablegroup schema is need?
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(schema_service_->get_all_core_table_schema(all_core_table_schema))) {
    LOG_WARN("failed to init schema service, ret=[%d]", K(ret));
  } else if (!is_sys_tenant(tenant_id)
             && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, all_core_table_schema))) {
    LOG_WARN("fail to construct __all_core_table schema", KR(ret), K(tenant_id));
  } else if (OB_FAIL(convert_to_simple_schema(all_core_table_schema, all_core_table_schema_simple))) {
    LOG_WARN("failed to add table schema into the schema manager, ret=[%d]", K(ret));
  } else if (OB_FAIL(schema_mgr_for_cache.add_table(all_core_table_schema_simple))) {
    LOG_WARN("failed to add table schema into the schema manager, ret=[%d]", K(ret));
  } else {
    schema_mgr_for_cache.set_schema_version(OB_CORE_SCHEMA_VERSION);
  }
  return ret;
}

// new schema refresh
int ObServerSchemaService::refresh_schema(
    const ObRefreshSchemaStatus &schema_status)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  const uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr *schema_mgr_for_cache = NULL;
  bool is_full_schema = true;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema status", K(ret), K(schema_status));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema mgr for cache", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(refresh_full_schema_map_.get_refactored(tenant_id, is_full_schema))) {
    LOG_WARN("refresh full schema", K(ret), K(tenant_id));
  } else if (is_full_schema) {
    FLOG_INFO("[REFRESH_SCHEMA] start to refresh full schema",
              "current schema_version", schema_mgr_for_cache->get_schema_version(), K(schema_status));

    if (OB_FAIL(refresh_full_schema(schema_status))) {
      LOG_WARN("tenant refresh full schema failed", K(ret), K(schema_status));
    }

    FLOG_INFO("[REFRESH_SCHEMA] finish refresh full schema", K(ret), K(schema_status),
              "current schema_version", schema_mgr_for_cache->get_schema_version(),
              "cost", ObTimeUtility::current_time() - start);

    if (OB_SUCC(ret)) {
      bool overwrite = true;
      is_full_schema = false;
      if (OB_FAIL(refresh_full_schema_map_.set_refactored(tenant_id, is_full_schema, overwrite))) {
        LOG_WARN("fail to set refresh full schema flag", K(ret), K(tenant_id));
      }
    }
  } else {
    FLOG_INFO("[REFRESH_SCHEMA] start to refresh increment schema",
              "current schema_version", schema_mgr_for_cache->get_schema_version(), K(schema_status));

    if (OB_FAIL(refresh_increment_schema(schema_status))) {
      LOG_WARN("tenant refresh increment schema failed", K(ret), K(schema_status));
    }

    FLOG_INFO("[REFRESH_SCHEMA] finish refresh increment schema", K(ret), K(schema_status),
              "current schema_version", schema_mgr_for_cache->get_schema_version(),
              "cost", ObTimeUtility::current_time() - start);
  }

  if (OB_SUCC(ret)) {
    const int64_t now = ObTimeUtility::current_time();
    EVENT_INC(REFRESH_SCHEMA_COUNT);
    EVENT_ADD(REFRESH_SCHEMA_TIME, now - start);
  }
  return ret;
}

int ObServerSchemaService::refresh_full_schema(
    const ObRefreshSchemaStatus &schema_status)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr *schema_mgr_for_cache = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(schema_status));
  } else {
    while (OB_SUCC(ret)) {
      int64_t retry_count = 0;
      bool core_schema_change = true;
      bool sys_schema_change = true;
      int64_t local_schema_version = 0;
      int64_t core_schema_version = 0;
      int64_t schema_version = 0;
      if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
        LOG_WARN("fail to get schema_mgr_for_cache", KR(ret), K(schema_status));
      } else if (OB_ISNULL(schema_mgr_for_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema mgr for cache is null", KR(ret), K(schema_status));
      } else {
        local_schema_version = schema_mgr_for_cache->get_schema_version();
      }
      // If refreshing the full amount fails, you need to reset and retry until it succeeds.
      // The outer layer avoids the scenario of failure to refresh the full amount of schema in the bootstrap stage.
      while (OB_SUCC(ret) && (core_schema_change || sys_schema_change)) {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("observer is stopping", KR(ret), K(schema_status));
          break;
        } else if (retry_count > 0) {
          LOG_WARN("refresh_full_schema failed, retry", K(schema_status), K(retry_count));
        }
        ObISQLClient &sql_client = *sql_proxy_;
        // refresh core table schemas
        if (OB_SUCC(ret) && core_schema_change) {
          if (OB_FAIL(schema_service_->get_core_version(
                      sql_client, schema_status, core_schema_version))) {
            LOG_WARN("get_core_version failed", KR(ret), K(schema_status));
          } else if (core_schema_version <= OB_CORE_SCHEMA_VERSION + 1) {
            ret = OB_EAGAIN;
            LOG_WARN("schema may be not persisted, try again",
                     KR(ret), K(schema_status), K(core_schema_version));
          } else if (core_schema_version > local_schema_version) {
            // for core table schema, we publish as core_temp_version
            int64_t publish_version = 0;
            if (OB_FAIL(ObSchemaService::gen_core_temp_version(core_schema_version, publish_version))) {
              LOG_WARN("gen_core_temp_version failed", KR(ret), K(schema_status), K(core_schema_version));
            } else if (OB_FAIL(try_fetch_publish_core_schemas(schema_status, core_schema_version,
                publish_version, sql_client, core_schema_change))) {
              LOG_WARN("try_fetch_publish_core_schemas failed", KR(ret),
                       K(schema_status), K(core_schema_version), K(publish_version));
            }
          } else {
            core_schema_change = false;
          }
        }

        // refresh sys table schemas
        if (OB_SUCC(ret) && !core_schema_change && sys_schema_change) {
          if (OB_FAIL(get_schema_version_in_inner_table(sql_client, schema_status, schema_version))) {
            LOG_WARN("fail to get schema version in inner table", KR(ret), K(schema_status));
          } else if (schema_version <= OB_CORE_SCHEMA_VERSION + 1) {
            ret = OB_EAGAIN;
            LOG_WARN("schema may be not persisted, try again",
                     KR(ret), K(schema_status), K(schema_version));
          } else if (core_schema_version > schema_version) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("schema version fallback, unexpected",
                      KR(ret), K(schema_status), K(core_schema_version), K(schema_version));
          } else if (OB_FAIL(check_core_schema_change_(sql_client, schema_status,
                     core_schema_version, core_schema_change))) {
             LOG_WARN("fail to check core schema version change", KR(ret), K(schema_status), K(core_schema_version));
          } else if (core_schema_change) {
            sys_schema_change = true;
            LOG_WARN("core schema version change, try again",
                     KR(ret), K(schema_status), K(core_schema_version), K(schema_version));
          } else if (OB_FAIL(check_sys_schema_change(sql_client, schema_status,
              local_schema_version, schema_version, sys_schema_change))) {
            LOG_WARN("check_sys_schema_change failed", KR(ret), K(schema_status), K(schema_version));
          } else if (sys_schema_change) {
            // for sys table schema, we publish as sys_temp_version
            const int64_t sys_formal_version = std::max(core_schema_version, schema_version);
            int64_t publish_version = 0;
            if (OB_FAIL(ObSchemaService::gen_sys_temp_version(sys_formal_version, publish_version))) {
              LOG_WARN("gen_sys_temp_version failed", KR(ret), K(schema_status), K(sys_formal_version));
            } else if (OB_FAIL(try_fetch_publish_sys_schemas(schema_status,
                                                             schema_version,
                                                             publish_version,
                                                             sql_client,
                                                             sys_schema_change))) {
              LOG_WARN("try_fetch_publish_sys_schemas failed", KR(ret), K(schema_status),
                       K(schema_version), K(publish_version));
            }
          }

          if (OB_FAIL(ret)) {
            // check whether failed because of core table schema change, go to suitable pos
            int temp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (temp_ret = check_core_schema_change_(
                sql_client, schema_status, core_schema_version, core_schema_change))) {
              LOG_WARN("get_core_version failed", KR(ret), KR(temp_ret), K(schema_status), K(core_schema_version));
            } else if (core_schema_change) {
              sys_schema_change = true;
              LOG_WARN("core schema version change, try again",
                       KR(ret), K(schema_status), K(core_schema_version), K(schema_version));
              ret = OB_SUCCESS;
            }
          }
        }

        // refresh full normal schema by schema_version
        if (OB_SUCC(ret) && !core_schema_change && !sys_schema_change) {
          const int64_t fetch_version = std::max(core_schema_version, schema_version);
          if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
            LOG_WARN("fail to get schema_mgr_for_cache", KR(ret), K(schema_status));
          } else if (OB_ISNULL(schema_mgr_for_cache)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema mgr for cache is null", KR(ret), K(schema_status));
          } else if (OB_FAIL(refresh_tenant_full_normal_schema(sql_client, schema_status, fetch_version))) {
            LOG_WARN("refresh_full_normal_schema failed", KR(ret), K(schema_status), K(fetch_version));
          } else {
            const int64_t publish_version = std::max(core_schema_version, schema_version);
            schema_mgr_for_cache->set_schema_version(publish_version);
            if (OB_FAIL(publish_schema(tenant_id))) {
              LOG_WARN("publish_schema failed", KR(ret), K(schema_status));
            } else {
              LOG_INFO("publish full normal schema by schema_version succeed", K(schema_status),
                       K(publish_version), K(core_schema_version), K(schema_version));
            }
          }
          if (OB_FAIL(ret)) {
            // check whether failed because of sys table schema change, go to suitable pos,
            // if during check core table schema change, go to suitable pos
            int temp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (temp_ret = check_core_or_sys_schema_change(
                sql_client, schema_status, core_schema_version, schema_version,
                core_schema_change, sys_schema_change))) {
              LOG_WARN("check_core_or_sys_schema_change failed", KR(ret),
                       K(schema_status), K(core_schema_version), K(schema_version));
            } else if (core_schema_change || sys_schema_change) {
              ret = OB_SUCCESS;
            }
          }
        }
        ++retry_count;
      } // end while

      // It must be reset before each refresh schema to prevent ddl from being in progress,
      // but refresh full may have added some tables
      // And the latter table was deleted again, at this time refresh will not delete this table in the cache
      if (OB_SUCC(ret)) {
        break;
      } else {
        FLOG_WARN("[REFRESH_SCHEMA] refresh full schema failed, do some clear", KR(ret), K(schema_status));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          LOG_ERROR("fail to get schema_mgr_for_cache", KR(ret), K(tmp_ret), K(schema_status));
        } else if (OB_ISNULL(schema_mgr_for_cache)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("schema mgr for cache is null", KR(ret), K(tmp_ret), K(schema_status));
        } else if (FALSE_IT(schema_mgr_for_cache->reset())) {
        } else if (OB_SUCCESS != (tmp_ret = init_tenant_basic_schema(tenant_id))) {
          LOG_ERROR("init basic schema failed", KR(ret), K(tmp_ret), K(schema_status));
        }
      }
    }
  }
  return ret;
}

int ObServerSchemaService::init_schema_struct(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else {

#define INIT_TENANT_MEM_MGR(map, tenant_id, mem_mgr_label, schema_mgr_label) \
    if (OB_FAIL(ret)) { \
    } else if (OB_ISNULL(map.get(tenant_id))) { \
      void *buff = ob_malloc(sizeof(ObSchemaMemMgr), SET_USE_500(mem_mgr_label, ObCtxIds::SCHEMA_SERVICE)); \
      ObSchemaMemMgr *schema_mem_mgr = NULL; \
      bool overwrite = true; \
      if (OB_ISNULL(buff)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        SQL_PC_LOG(ERROR, "alloc schema_mem_mgr failed", K(ret), K(tenant_id)); \
      } else if (NULL == (schema_mem_mgr = new(buff)ObSchemaMemMgr())) { \
        ret = OB_NOT_INIT; \
        SQL_PC_LOG(WARN, "fail to constructor schema_mem_mgr", K(ret), K(tenant_id)); \
      } else if (OB_FAIL(schema_mem_mgr->init(schema_mgr_label, tenant_id))) { \
        LOG_WARN("fail to init schema_mem_mgr", K(ret), K(tenant_id)); \
      } else if (OB_FAIL(map.set_refactored(tenant_id, schema_mem_mgr, overwrite))) { \
        LOG_WARN("fail to set schema_mem_mgr", K(ret), K(tenant_id)); \
      } \
      if (OB_FAIL(ret)) { \
        if (NULL != schema_mem_mgr) { \
          schema_mem_mgr->~ObSchemaMemMgr(); \
          ob_free(buff); \
          schema_mem_mgr = NULL; \
          buff = NULL; \
        } else if (NULL != buff) { \
          ob_free(buff); \
          buff = NULL; \
        } \
      } \
    } else { \
      LOG_INFO("schema_mgr_for_cache exist", K(ret), K(tenant_id)); \
    }

    INIT_TENANT_MEM_MGR(mem_mgr_map_, tenant_id,
                        ObModIds::OB_TENANT_SCHEMA_MEM_MGR, ObModIds::OB_TENANT_SCHEMA_MGR);
    INIT_TENANT_MEM_MGR(mem_mgr_for_liboblog_map_, tenant_id,
                        ObModIds::OB_TENANT_SCHEMA_MEM_MGR_FOR_LIBOBLOG, ObModIds::OB_TENANT_SCHEMA_MGR_FOR_LIBOBLOG);

#undef INIT_TENANT_MEM_MGR

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(refresh_full_schema_map_.get(tenant_id))) {
      bool refresh_full_schema = true;
      bool overwrite = true;
      if (OB_FAIL(refresh_full_schema_map_.set_refactored(tenant_id, refresh_full_schema, overwrite))) {
        LOG_WARN("fail to set refresh full schema flag", K(ret), K(tenant_id));
      }
    } else {
      LOG_INFO("refresh_full_schema_map exist", K(ret), K(tenant_id));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_mgr_for_cache_map_.get(tenant_id))) {
      ObSchemaMgr *schema_mgr_for_cache = NULL;
      ObSchemaMemMgr *mem_mgr = NULL;
      bool alloc_for_liboblog = false;
      if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
        LOG_WARN("fail to get mem_mgr", K(ret), K(tenant_id));
      } else if (OB_ISNULL(mem_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_mgr is null", K(ret), K(tenant_id));
      } else if (OB_FAIL(mem_mgr->alloc_schema_mgr(schema_mgr_for_cache, alloc_for_liboblog))) {
        LOG_WARN("alloc schema mgr failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->init(tenant_id))) {
        LOG_WARN("init schema mgr for cache failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache_map_.set_refactored(tenant_id, schema_mgr_for_cache))) {
        LOG_WARN("fail to set schema_mgr", K(ret), K(tenant_id));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(mem_mgr)) {
        int64_t tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(mem_mgr->free_schema_mgr(schema_mgr_for_cache))) {
          LOG_ERROR("fail to free mem_mgr", KR(ret));
        }
      }
    } else {
      LOG_INFO("schema_mgr exist", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObServerSchemaService::refresh_increment_schema(
    const ObRefreshSchemaStatus &schema_status)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  observer::ObUseWeakGuard use_weak_guard;
  const uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr *schema_mgr_for_cache = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(schema_status));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema_mgr_for_cache", KR(ret), K(schema_status));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", KR(ret), K(schema_status));
  } else {
    ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
    bool core_schema_change = true;
    bool sys_schema_change = true;
    int64_t local_schema_version = schema_mgr_for_cache->get_schema_version();
    int64_t core_schema_version = 0;
    int64_t schema_version = OB_INVALID_VERSION;
    int64_t retry_count = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("observer is stopping", KR(ret), K(schema_status));
        break;
      } else if (retry_count > 0) {
        LOG_WARN("refresh_increment_schema failed", K(retry_count), K(schema_status));
        if (OB_FAIL(set_timeout_ctx(ctx))) {
          LOG_WARN("fail to set timeout ctx", KR(ret), K(schema_status));
          break;
        }
      }
      ObISQLClient &sql_client = *sql_proxy_;
      if (OB_SUCC(ret) && core_schema_change) {
        if (OB_FAIL(schema_service_->get_core_version(
            sql_client, schema_status, core_schema_version))) {
          LOG_WARN("get_core_version failed", KR(ret), K(schema_status));
        } else if (core_schema_version <= OB_CORE_SCHEMA_VERSION + 1) {
          ret = OB_EAGAIN;
          LOG_WARN("schema may be not persisted, try again",
                   KR(ret), K(schema_status), K(core_schema_version));
        } else if (core_schema_version > local_schema_version) {
          int64_t publish_version = OB_INVALID_INDEX;
          if (OB_FAIL(ObSchemaService::gen_core_temp_version(
              core_schema_version, publish_version))) {
            LOG_WARN("gen_core_temp_version failed", KR(ret), K(schema_status), K(core_schema_version));
          } else if (OB_FAIL(try_fetch_publish_core_schemas(schema_status,
                     core_schema_version, publish_version, sql_client, core_schema_change))) {
            LOG_WARN("try_fetch_publish_core_schemas failed", KR(ret), K(schema_status),
                     K(core_schema_version), K(publish_version));
          } else {}
        } else {
          core_schema_change = false;
        }
      }

      if (OB_SUCC(ret) && !core_schema_change && sys_schema_change) {
        if (OB_FAIL(get_schema_version_in_inner_table(sql_client, schema_status, schema_version))) {
          LOG_WARN("fail to get schema version in inner table", KR(ret), K(schema_status));
        } else if (schema_version < local_schema_version) {
          if (local_schema_version <= OB_CORE_SCHEMA_VERSION + 1) {
            ret = OB_EAGAIN;
            LOG_WARN("schema may be not persisted, try again",
                     KR(ret), K(schema_status), K(schema_version), K(local_schema_version));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("schema version fallback, unexpected",
                      KR(ret), K(schema_status), K(schema_version), K(local_schema_version));
          }
        } else if (core_schema_version > schema_version) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("schema version fallback, unexpected",
                    KR(ret), K(schema_status), K(core_schema_version), K(schema_version));
        } else if (OB_FAIL(check_core_schema_change_(sql_client, schema_status,
                   core_schema_version, core_schema_change))) {
           LOG_WARN("fail to check core schema version change", KR(ret), K(schema_status), K(core_schema_version));
        } else if (core_schema_change) {
          sys_schema_change = true;
          LOG_WARN("core schema version change, try again",
                   KR(ret), K(schema_status), K(core_schema_version), K(schema_version));
        } else if (OB_FAIL(check_sys_schema_change(sql_client, schema_status,
                   local_schema_version, schema_version, sys_schema_change))) {
          LOG_WARN("check_sys_schema_change failed", KR(ret), K(schema_status), K(schema_version));
        } else if (sys_schema_change) {
          const int64_t sys_formal_version = std::max(core_schema_version, schema_version);
          int64_t publish_version = 0;
          if (OB_FAIL(ObSchemaService::gen_sys_temp_version(sys_formal_version, publish_version))) {
            LOG_WARN("gen_sys_temp_version failed", KR(ret), K(schema_status), K(sys_formal_version));
          } else if (OB_FAIL(try_fetch_publish_sys_schemas(schema_status, schema_version,
                     publish_version, sql_client, sys_schema_change))) {
            LOG_WARN("try_fetch_publish_sys_schemas failed", KR(ret), K(schema_status),
                     K(schema_version), K(publish_version));
          } else {}
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = check_core_schema_change_(
              sql_client, schema_status, core_schema_version, core_schema_change))) {
            LOG_WARN("get_core_version failed", KR(ret), KR(temp_ret), K(schema_status), K(core_schema_version));
          } else if (core_schema_change) {
            sys_schema_change = true;
            LOG_WARN("core schema version change, try again",
                     KR(ret), K(schema_status), K(core_schema_version), K(schema_version));
            ret = OB_SUCCESS;
          }
        }
      }

      if (OB_SUCC(ret) && !core_schema_change && !sys_schema_change) {
        const int64_t fetch_version = std::max(core_schema_version, schema_version);
        if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          LOG_WARN("fail to get schema_mgr_for_cache", KR(ret), K(schema_status));
        } else if (OB_ISNULL(schema_mgr_for_cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema mgr for cache is null", KR(ret));
        } else if (OB_FAIL(schema_service_->get_increment_schema_operations(schema_status,
            local_schema_version, fetch_version, sql_client, schema_operations))) {
          LOG_WARN("get_increment_schema_operations failed", KR(ret), K(schema_status),
              K(local_schema_version), K(fetch_version));
        } else if (schema_operations.count() > 0) {
          // new cache
          SMART_VAR(AllSchemaKeys, all_keys) {
            if (OB_FAIL(replay_log(*schema_mgr_for_cache, schema_operations, all_keys))) {
              LOG_WARN("replay_log failed", KR(ret), K(schema_status), K(schema_operations));
            } else if (OB_FAIL(update_schema_mgr(sql_client, schema_status,
                       *schema_mgr_for_cache, fetch_version, all_keys))){
              LOG_WARN("update schema mgr failed", KR(ret), K(schema_status));
            }
          }
        } else {}


        if (OB_SUCC(ret)) {
          schema_mgr_for_cache->set_schema_version(fetch_version);
          if (OB_FAIL(publish_schema(tenant_id))) {
            LOG_WARN("publish_schema failed", KR(ret), K(schema_status));
          } else {
            LOG_INFO("change schema version", K(schema_status), K(schema_version), K(core_schema_version));
            break;
          }
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of sys table schema change, go to suitable pos,
          // if during check core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = check_core_or_sys_schema_change(sql_client, schema_status,
              core_schema_version, schema_version, core_schema_change, sys_schema_change))) {
            LOG_WARN("check_core_or_sys_schema_change failed, need retry", KR(ret), KR(temp_ret),
                     K(schema_status), K(core_schema_version), K(schema_version));
          } else if (core_schema_change || sys_schema_change) {
            ret = OB_SUCCESS;
          }
        }
      }
      ++retry_count;
    } // end while

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
        LOG_ERROR("fail to get schema_mgr_for_cache", KR(ret), K(tmp_ret), K(schema_status));
      } else if (OB_ISNULL(schema_mgr_for_cache)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("schema mgr for cache is null", KR(ret), K(tmp_ret), K(schema_status));
      } else if (schema_mgr_for_cache->get_schema_version() != local_schema_version) {
        // Rrefresh increment schema may success partially and local schema version may be enhanced.
        // To avoid missing increment ddl operations, local schema version should be reset to last schema version
        // before refresh increment schema in the next round.
        schema_mgr_for_cache->set_schema_version(local_schema_version);
        FLOG_WARN("[REFRESH_SCHEMA] refresh increment schema failed, try reset to last schema version",
                  KR(ret), K(tenant_id), "last_schema_version", local_schema_version,
                  "cur_schema_version", schema_mgr_for_cache->get_schema_version());
      }
    }
  }

  return ret;
}

int ObServerSchemaService::set_timeout_ctx(ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ctx.get_abs_timeout();
  int64_t worker_timeout_us = THIS_WORKER.get_timeout_ts();

  if (abs_timeout_us < 0) {
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_FETCH_SCHEMA_TIMEOUT_US;
  }

  if (INT64_MAX == worker_timeout_us) {
    // The background schema refresh task triggered by the heartbeat, the system tenant schemea
    // needs to retry until it succeeds
    abs_timeout_us = ObTimeUtility::current_time() + MAX_FETCH_SCHEMA_TIMEOUT_US;
  } else if (worker_timeout_us > 0 && worker_timeout_us < abs_timeout_us) {
    abs_timeout_us = worker_timeout_us;
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else  if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout", ctx.get_abs_timeout(),
        "this worker timeout ts", THIS_WORKER.get_timeout_ts());
  }

  return ret;
}

int ObServerSchemaService::try_fetch_publish_core_schemas(
    const ObRefreshSchemaStatus &schema_status,
    const int64_t core_schema_version,
    const int64_t publish_version,
    ObISQLClient &sql_client,
    bool &core_schema_change)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(schema_status));
  } else {
    ObArray<ObTableSchema> core_schemas;
    ObArray<uint64_t> core_table_ids;
    if (OB_FAIL(schema_service_->get_core_table_schemas(
        sql_client, schema_status, core_schemas))) {
      LOG_WARN("get_core_table_schemas failed", KR(ret), K(schema_status), K(core_table_ids));
    } else if (OB_FAIL(check_core_schema_change_(sql_client, schema_status,
               core_schema_version, core_schema_change))) {
       LOG_WARN("fail to check core schema version change", KR(ret), K(schema_status), K(core_schema_version));
    } else if (core_schema_change) {
      LOG_WARN("core schema version change",
               KR(ret), K(schema_status), K(core_schema_version));
    } else {
      // core schema don't change, publish core schemas
      ObArray<ObTableSchema *> core_tables;
      for (int64_t i = 0; i < core_schemas.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(core_tables.push_back(&core_schemas.at(i)))) {
          LOG_WARN("add table schema failed", KR(ret), K(schema_status));
        }
      }
      if (OB_SUCC(ret)) {
        ObSchemaMgr *schema_mgr_for_cache = NULL;
        auto attr = SET_USE_500("PubCoreSchema", ObCtxIds::SCHEMA_SERVICE);
        ObArenaAllocator allocator(attr);
        ObArray<ObSimpleTableSchemaV2*> simple_core_schemas(
                         common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                         common::ModulePageAllocator(allocator));
        if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          LOG_WARN("fail to get schema mgr for cache", KR(ret), K(schema_status));
        } else if (OB_ISNULL(schema_mgr_for_cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_mgr_for_cache is null", KR(ret), K(schema_status));
        } else if (OB_FAIL(update_schema_cache(core_tables))) {
          LOG_WARN("failed to update schema cache", KR(ret), K(schema_status));
        } else if (OB_FAIL(convert_to_simple_schema(allocator, core_schemas, simple_core_schemas))) {
          LOG_WARN("convert to simple schema failed", KR(ret), K(schema_status));
        } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_core_schemas))) {
          LOG_WARN("add tables failed", KR(ret), K(schema_status));
        } else if (FALSE_IT(schema_mgr_for_cache->set_schema_version(publish_version))){
        } else if (OB_FAIL(publish_schema(tenant_id))) {
          LOG_WARN("publish_schema failed", KR(ret), K(schema_status));
        } else {
          FLOG_INFO("[REFRESH_SCHEMA] refresh core table schema succeed",
                    K(schema_status),
                    K(publish_version),
                    K(core_schema_version),
                    K(schema_mgr_for_cache->get_schema_version()));
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::try_fetch_publish_sys_schemas(
    const ObRefreshSchemaStatus &schema_status,
    const int64_t schema_version,
    const int64_t publish_version,
    common::ObISQLClient &sql_client,
    bool &sys_schema_change)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(schema_status));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(schema_status));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA_SYS_SCHEMA);
    ObArray<ObTableSchema *> sys_schemas;
    ObArray<uint64_t> sys_table_ids;
    int64_t new_schema_version = 0;
    if (OB_FAIL(get_sys_table_ids(tenant_id, sys_table_ids))) {
      LOG_WARN("get sys table ids failed", KR(ret), K(schema_status));
    } else if (OB_FAIL(schema_service_->get_sys_table_schemas(
               sql_client, schema_status, sys_table_ids, allocator, sys_schemas))) {
      LOG_WARN("get_batch_table_schema failed", KR(ret), K(schema_status), K(sys_table_ids));
    } else if (OB_FAIL(get_schema_version_in_inner_table(sql_client, schema_status, new_schema_version))) {
      LOG_WARN("fail to get schema version in inner table", KR(ret), K(schema_status));
    } else if (OB_FAIL(check_sys_schema_change(sql_client,
                                               schema_status,
                                               schema_version,
                                               new_schema_version,
                                               sys_schema_change))) {
      LOG_WARN("check_sys_schema_change failed", KR(ret), K(schema_status),
               K(schema_version), K(new_schema_version));
    } else if (sys_schema_change) {
      LOG_WARN("sys schema change during refresh full schema",
               K(schema_status), K(schema_version), K(new_schema_version));
    } else if (!sys_schema_change) {
      ObSchemaMgr *schema_mgr_for_cache = NULL;
      auto attr = SET_USE_500("PubSysSchema", ObCtxIds::SCHEMA_SERVICE);
      ObArenaAllocator allocator(attr);
      ObArray<ObSimpleTableSchemaV2*> simple_sys_schemas(
                       common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                       common::ModulePageAllocator(allocator));
      if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
        LOG_WARN("fail to get schema mgr for cache", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(schema_mgr_for_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_mgr_for_cache is null", KR(ret), K(schema_status));
      } else if (OB_FAIL(update_schema_cache(sys_schemas))) {
        LOG_WARN("failed to update schema cache", KR(ret), K(schema_status));
      } else if (OB_FAIL(convert_to_simple_schema(allocator, sys_schemas, simple_sys_schemas))) {
        LOG_WARN("convert to simple schema failed", KR(ret), K(schema_status));
      } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_sys_schemas))) {
        LOG_WARN("add tables failed", KR(ret), K(schema_status));
      } else if (FALSE_IT(schema_mgr_for_cache->set_schema_version(publish_version))){
      } else if (OB_FAIL(publish_schema(tenant_id))) {
        LOG_WARN("publish_schema failed", KR(ret), K(schema_status));
      } else {
        FLOG_INFO("[REFRESH_SCHEMA] refresh sys table schema succeed",
                  K(schema_status),
                  K(publish_version),
                  K(schema_version),
                  K(schema_mgr_for_cache->get_schema_version()));
      }
    }
  }
  return ret;
}


int ObServerSchemaService::add_tenant_schema_to_cache(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || schema_version < 0) {
    ret =  OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(schema_version));
  } else {
    ObSEArray<ObTenantSchema, 1> tenant_schema_array;
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("fail to push tenant_id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_->get_batch_tenants(
               sql_client, schema_version, tenant_ids, tenant_schema_array))) {
      LOG_WARN("failed to get new tenant schema", K(ret), K(tenant_id));
    } else if (OB_FAIL(update_schema_cache(tenant_schema_array))) {
      LOG_WARN("failed to update schema cache", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObServerSchemaService::add_sys_variable_schema_to_cache(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || schema_version < 0) {
    ret =  OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(schema_version));
  } else {
    ObSysVariableSchema new_sys_variable;
    if (OB_FAIL(schema_service_->get_sys_variable_schema(
        sql_client, schema_status, tenant_id, schema_version, new_sys_variable))) {
      LOG_WARN("failed to get new sys variable schema", K(ret));
    } else if (OB_FAIL(update_schema_cache(new_sys_variable))) {
      LOG_WARN("failed to update schema cache", K(ret));
    }
  }

  return ret;
}

// new schema full refresh
int ObServerSchemaService::refresh_tenant_full_normal_schema(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr *schema_mgr_for_cache = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(schema_status));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", K(ret));
  } else {
    // System tenants need to do the following:
    // 1. Add the full tenant schema of the system tenant to schema_cache
    // 2. Initialize the schema memory management structure of ordinary tenants
    // 3. Add the simple tenant schema of all tenants
    if (is_sys_tenant(tenant_id)) {
      ObArray<ObSimpleTenantSchema> simple_tenants;
      ObArray<ObDropTenantInfo> drop_tenant_infos;
      if (OB_FAIL(schema_service_->get_all_tenants(sql_client,
                                                   schema_version,
                                                   simple_tenants))) {
        LOG_WARN("get all tenant schema failed", K(ret), K(schema_version));
      } else if (simple_tenants.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant counts error", K(ret), K(simple_tenants.count()));
      } else if (OB_FAIL(schema_service_->get_drop_tenant_infos(sql_client, schema_version, drop_tenant_infos))) {
        LOG_WARN("fail to get drop tenant infos", K(ret), K(schema_version));
      } else {
        // bugfix: 52326403
        // Make sure refresh schema status ready before tenant schema is visible.
        if (!ObSchemaService::g_liboblog_mode_) {
          bool need_refresh_schema_status = false;
          for (int64_t i = 0; !need_refresh_schema_status && OB_SUCC(ret) && i < simple_tenants.count(); i++) {
            const ObSimpleTenantSchema &simple_tenant = simple_tenants.at(i);
            if (simple_tenant.is_restore()) {
              need_refresh_schema_status = true;
            }
          } // end for
          if (OB_SUCC(ret) && need_refresh_schema_status) {
            ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
            if (OB_ISNULL(schema_status_proxy)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema_status_proxy is null", KR(ret));
            } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status())) {
              LOG_WARN("fail to load refresh schema status", KR(ret));
            }
          }
        }
        FOREACH_CNT_X(simple_tenant, simple_tenants, OB_SUCC(ret)) {
          const uint64_t tmp_tenant_id = simple_tenant->get_tenant_id();
          //
          // note: Adjust the position of this code carefully
          // add sys tenant schema to cache
          if (OB_FAIL(schema_mgr_for_cache->add_tenant(*simple_tenant))) {
            LOG_WARN("add tenant failed", K(ret), K(*simple_tenant));
          } else if (is_sys_tenant(tmp_tenant_id)) {
            if (OB_FAIL(add_tenant_schema_to_cache(sql_client, tmp_tenant_id, schema_version))) {
              LOG_WARN("add tenant schema to cache failed", K(ret), K(tmp_tenant_id), K(schema_version));
            } else if (OB_FAIL(add_sys_variable_schema_to_cache(sql_client, schema_status, tmp_tenant_id, schema_version))) {
              LOG_WARN("add sys variable schema to cache failed", K(ret), K(tmp_tenant_id), K(schema_version));
            } else if (OB_FAIL(schema_mgr_for_cache->add_drop_tenant_infos(drop_tenant_infos))) {
              LOG_WARN("add drop tenant infos failed", K(ret));
            }
          } else {
            // When the system tenant flashes the DDL of the newly created tenant,
            // try to initialize the relevant data structure
            if (OB_FAIL(init_schema_struct(tmp_tenant_id))) {
              LOG_WARN("fail to init schema struct", K(ret), K(tmp_tenant_id), K(schema_status));
            } else if (OB_FAIL(init_tenant_basic_schema(tmp_tenant_id))) {
              LOG_WARN("fail to init basic schema struct", K(ret), K(tmp_tenant_id), K(schema_status));
            } else if (OB_FAIL(init_multi_version_schema_struct(tmp_tenant_id))) {
              LOG_WARN("fail to init multi version schema struct", K(ret), K(tmp_tenant_id));
            } else if (OB_FAIL(publish_schema(tmp_tenant_id))) {
              LOG_WARN("publish_schema failed", KR(ret), K(tmp_tenant_id));
            }
          }
        }
      }
    } else {
      // Ordinary tenant schema refreshing relies on the system tenant to refresh the tenant and
      // initialize the related memory structure
    }

    if (OB_SUCC(ret)) {
      auto attr = SET_USE_500("RefFullSchema", ObCtxIds::SCHEMA_SERVICE);
      ObArenaAllocator allocator(attr);
      #define INIT_ARRAY(TYPE, name) \
        ObArray<TYPE> name(common::OB_MALLOC_NORMAL_BLOCK_SIZE, \
                           common::ModulePageAllocator(allocator));
      INIT_ARRAY(ObSimpleUserSchema, simple_users);
      INIT_ARRAY(ObSimpleDatabaseSchema, simple_databases);
      INIT_ARRAY(ObSimpleTablegroupSchema, simple_tablegroups);
      INIT_ARRAY(ObSimpleTableSchemaV2*, simple_tables);
      INIT_ARRAY(ObSimpleOutlineSchema, simple_outlines);
      INIT_ARRAY(ObSimpleRoutineSchema, simple_routines);
      INIT_ARRAY(ObSimpleSynonymSchema, simple_synonyms);
      INIT_ARRAY(ObSimplePackageSchema, simple_packages);
      INIT_ARRAY(ObSimpleTriggerSchema, simple_triggers);
      INIT_ARRAY(ObDBPriv, db_privs);
      INIT_ARRAY(ObSysPriv, sys_privs);
      INIT_ARRAY(ObTablePriv, table_privs);
      INIT_ARRAY(ObObjPriv, obj_privs);
      INIT_ARRAY(ObSimpleUDFSchema, simple_udfs);
      INIT_ARRAY(ObSimpleUDTSchema, simple_udts);
      INIT_ARRAY(ObSequenceSchema, simple_sequences);
      INIT_ARRAY(ObKeystoreSchema, simple_keystores);
      INIT_ARRAY(ObProfileSchema, simple_profiles);
      INIT_ARRAY(ObSAuditSchema, simple_audits);
      INIT_ARRAY(ObLabelSePolicySchema, simple_label_se_policys);
      INIT_ARRAY(ObLabelSeComponentSchema, simple_label_se_components);
      INIT_ARRAY(ObLabelSeLabelSchema, simple_label_se_labels);
      INIT_ARRAY(ObLabelSeUserLevelSchema, simple_label_se_user_levels);
      INIT_ARRAY(ObTablespaceSchema, simple_tablespaces);
      INIT_ARRAY(ObDbLinkSchema, simple_dblinks);
      INIT_ARRAY(ObDirectorySchema, simple_directories);
      INIT_ARRAY(ObContextSchema, simple_contexts);
      INIT_ARRAY(ObSimpleMockFKParentTableSchema, simple_mock_fk_parent_tables);
      INIT_ARRAY(ObRlsPolicySchema, simple_rls_policys);
      INIT_ARRAY(ObRlsGroupSchema, simple_rls_groups);
      INIT_ARRAY(ObRlsContextSchema, simple_rls_contexts);
      #undef INIT_ARRAY
      ObSimpleSysVariableSchema simple_sys_variable;

      if (OB_FAIL(schema_service_->get_sys_variable(sql_client, schema_status, tenant_id,
          schema_version, simple_sys_variable))) {
        LOG_WARN("get all sys variables failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_users(
          sql_client, schema_status, schema_version, tenant_id, simple_users))) {
        LOG_WARN("get all users failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_databases(
          sql_client, schema_status, schema_version, tenant_id, simple_databases))) {
        LOG_WARN("get all databases failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_tablegroups(
          sql_client, schema_status, schema_version, tenant_id, simple_tablegroups))) {
        LOG_WARN("get all tablegroups failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_tables(
          sql_client, allocator, schema_status, schema_version, tenant_id, simple_tables))) {
        LOG_WARN("get all table schema failed", KR(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_outlines(
          sql_client, schema_status, schema_version, tenant_id, simple_outlines))) {
        LOG_WARN("get all outline schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_routines(
          sql_client, schema_status, schema_version, tenant_id, simple_routines))) {
        LOG_WARN("get all procedure schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_packages(
          sql_client, schema_status, schema_version, tenant_id, simple_packages))) {
        LOG_WARN("get all package schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_triggers(
          sql_client, schema_status, schema_version, tenant_id, simple_triggers))) {
        LOG_WARN("get all trigger schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_db_privs(
          sql_client, schema_status, schema_version, tenant_id, db_privs))) {
        LOG_WARN("get all db priv schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_table_privs(
          sql_client, schema_status, schema_version, tenant_id, table_privs))) {
        LOG_WARN("get all table priv failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_obj_privs(
          sql_client, schema_status, schema_version, tenant_id, obj_privs))) {
        LOG_WARN("get all obj priv failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_synonyms(
          sql_client, schema_status, schema_version, tenant_id, simple_synonyms))) {
        LOG_WARN("get all synonym schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_udfs(
          sql_client, schema_status, schema_version, tenant_id, simple_udfs))) {
        LOG_WARN("get all udfs schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_udts(
          sql_client, schema_status, schema_version, tenant_id, simple_udts))) {
        LOG_WARN("get all udts schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_sequences(
          sql_client, schema_status, schema_version, tenant_id, simple_sequences))) {
        LOG_WARN("get all sequences schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_keystores(
          sql_client, schema_status, schema_version, tenant_id, simple_keystores))) {
        LOG_WARN("get all keystore schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_label_se_policys(
          sql_client, schema_status, schema_version, tenant_id, simple_label_se_policys))) {
        LOG_WARN("get all label security schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_label_se_components(
          sql_client, schema_status, schema_version, tenant_id, simple_label_se_components))) {
        LOG_WARN("get all label security schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_label_se_labels(
          sql_client, schema_status, schema_version, tenant_id, simple_label_se_labels))) {
        LOG_WARN("get all label security schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_label_se_user_levels(
          sql_client, schema_status, schema_version, tenant_id, simple_label_se_user_levels))) {
        LOG_WARN("get all label security schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_tablespaces(
          sql_client, schema_status, schema_version, tenant_id, simple_tablespaces))) {
        LOG_WARN("get all tablespace schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_profiles(
          sql_client, schema_status, schema_version, tenant_id, simple_profiles))) {
        LOG_WARN("get all profiles schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_audits(
          sql_client, schema_status, schema_version, tenant_id, simple_audits))) {
        LOG_WARN("get all audit schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_sys_privs(
          sql_client, schema_status, schema_version, tenant_id, sys_privs))) {
        LOG_WARN("get all sys priv schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_dblinks(
          sql_client, schema_status, schema_version, tenant_id, simple_dblinks))) {
        LOG_WARN("get all dblink schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_directorys(
          sql_client, schema_status, schema_version, tenant_id, simple_directories))) {
        LOG_WARN("get all directory schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_contexts(
          sql_client, schema_status, schema_version, tenant_id, simple_contexts))) {
        LOG_WARN("get all contexts schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_mock_fk_parent_tables(
          sql_client, schema_status, schema_version, tenant_id, simple_mock_fk_parent_tables))) {
        LOG_WARN("get all mock_fk_parent_tables schema failed", K(ret), K(schema_version), K(tenant_id));
      } else {
      }
      if (OB_SUCC(ret)) {
        const ObSimpleTableSchemaV2 *tmp_table = NULL;
        if (OB_FAIL(schema_mgr_for_cache->get_table_schema(tenant_id, OB_ALL_RLS_POLICY_HISTORY_TID, tmp_table))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tmp_table)) {
          // for compatibility
        } else if (OB_FAIL(schema_mgr_for_cache->get_table_schema(tenant_id, OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TID, tmp_table))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tmp_table)) {
          // for compatibility
        } else if (OB_FAIL(schema_service_->get_all_rls_policys(
            sql_client, schema_status, schema_version, tenant_id, simple_rls_policys))) {
          LOG_WARN("get all rls_policy schema failed", K(ret), K(schema_version), K(tenant_id));
        }
      }
      if (OB_SUCC(ret)) {
        const ObSimpleTableSchemaV2 *tmp_table = NULL;
        if (OB_FAIL(schema_mgr_for_cache->get_table_schema(tenant_id, OB_ALL_RLS_GROUP_HISTORY_TID, tmp_table))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tmp_table)) {
          // for compatibility
        } else if (OB_FAIL(schema_service_->get_all_rls_groups(
            sql_client, schema_status, schema_version, tenant_id, simple_rls_groups))) {
          LOG_WARN("get all rls_group schema failed", K(ret), K(schema_version), K(tenant_id));
        }
      }
      if (OB_SUCC(ret)) {
        const ObSimpleTableSchemaV2 *tmp_table = NULL;
        if (OB_FAIL(schema_mgr_for_cache->get_table_schema(tenant_id, OB_ALL_RLS_CONTEXT_HISTORY_TID, tmp_table))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tmp_table)) {
          // for compatibility
        } else if (OB_FAIL(schema_service_->get_all_rls_contexts(
            sql_client, schema_status, schema_version, tenant_id, simple_rls_contexts))) {
          LOG_WARN("get all rls_context schema failed", K(ret), K(schema_version), K(tenant_id));
        }
      }

      const bool refresh_full_schema = true;
      // add simple schema for cache
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_mgr_for_cache->sys_variable_mgr_
                         .add_sys_variable(simple_sys_variable))) {
        LOG_WARN("add sys variables failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_users(simple_users))) {
        LOG_WARN("add users failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_databases(simple_databases))) {
        LOG_WARN("add databases failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_tablegroups(simple_tablegroups))) {
        LOG_WARN("add tablegroups failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_tables, refresh_full_schema))) {
        LOG_WARN("add tables failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->outline_mgr_.add_outlines(simple_outlines))) {
        LOG_WARN("add outlines failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->routine_mgr_.add_routines(simple_routines))) {
        LOG_WARN("add procedures failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->synonym_mgr_.add_synonyms(simple_synonyms))) {
        LOG_WARN("add synonyms failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->package_mgr_.add_packages(simple_packages))) {
        LOG_WARN("add package failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->trigger_mgr_.add_triggers(simple_triggers))) {
        LOG_WARN("add trigger failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_db_privs(db_privs))) {
        LOG_WARN("add db privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_table_privs(table_privs))) {
        LOG_WARN("add table privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_obj_privs(obj_privs))) {
        LOG_WARN("add obj privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->udf_mgr_.add_udfs(simple_udfs))) {
        LOG_WARN("add udfs privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->udt_mgr_.add_udts(simple_udts))) {
        LOG_WARN("add udts failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->sequence_mgr_.add_sequences(simple_sequences))) {
        LOG_WARN("add sequence failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->keystore_mgr_.add_keystores(simple_keystores))) {
        LOG_WARN("add keystore failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->tablespace_mgr_.add_tablespaces(simple_tablespaces))) {
        LOG_WARN("add tablespace failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->label_se_policy_mgr_.add_label_se_policys(simple_label_se_policys))) {
        LOG_WARN("add label security policy failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->label_se_component_mgr_.add_label_se_components(simple_label_se_components))) {
        LOG_WARN("add label security policy failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->label_se_label_mgr_.add_label_se_labels(simple_label_se_labels))) {
        LOG_WARN("add label security policy failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->label_se_user_level_mgr_.add_label_se_user_levels(simple_label_se_user_levels))) {
        LOG_WARN("add label security policy failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->tablespace_mgr_.add_tablespaces(simple_tablespaces))) {
        LOG_WARN("add tablespace failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->profile_mgr_.add_profiles(simple_profiles))) {
        LOG_WARN("add profiles failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->audit_mgr_.add_audits(simple_audits))) {
        LOG_WARN("add sequence failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_sys_privs(sys_privs))) {
        LOG_WARN("add sys privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->dblink_mgr_.add_dblinks(simple_dblinks))) {
        LOG_WARN("add dblinks failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->directory_mgr_.add_directorys(simple_directories))) {
        LOG_WARN("add directories failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->context_mgr_.add_contexts(simple_contexts))) {
        LOG_WARN("add contexts failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->mock_fk_parent_table_mgr_.add_mock_fk_parent_tables(
                         simple_mock_fk_parent_tables))) {
        LOG_WARN("add mock_fk_parent_tables failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->rls_policy_mgr_.add_rls_policys(simple_rls_policys))) {
        LOG_WARN("add rls_policys failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->rls_group_mgr_.add_rls_groups(simple_rls_groups))) {
        LOG_WARN("add rls_groups failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->rls_context_mgr_.add_rls_contexts(simple_rls_contexts))) {
        LOG_WARN("add rls_contexts failed", K(ret));
      }

      LOG_INFO("add schemas for tenant finish",
               K(tenant_id), K(schema_version), K(schema_status),
               "users", simple_users.count(),
               "databases", simple_databases.count(),
               "tablegroups", simple_tablegroups.count(),
               "tables", simple_tables.count(),
               "outlines", simple_outlines.count(),
               "synonyms", simple_synonyms.count(),
               "db_privs", db_privs.count(),
               "table_privs", table_privs.count(),
               "udfs", simple_udfs.count(),
               "udt", simple_udts.count(),
               "sequences", simple_sequences.count(),
               "keystore", simple_keystores.count(),
               "label se policy", simple_label_se_policys.count(),
               "label se component", simple_label_se_components.count(),
               "label se label", simple_label_se_labels.count(),
               "label se user level", simple_label_se_user_levels.count(),
               "tablespaces", simple_tablespaces.count(),
               "profiles", simple_profiles.count(),
               "audits", simple_audits.count());
      // the parameters count of previous LOG_INFO has reached maximum,
      // so we need a new LOG_INFO.
      LOG_INFO("add schemas for tenant finish",
               K(tenant_id), K(schema_version), K(schema_status),
               "sys_privs", sys_privs.count(),
               "dblinks", simple_dblinks.count(),
               "directories", simple_directories.count(),
               "rls_policys", simple_rls_policys.count(),
               "rls_groups", simple_rls_groups.count(),
               "rls_contexts", simple_rls_contexts.count()
              );
    }

    if (OB_SUCC(ret)) {
      ObArenaAllocator allocator;
      ObArray<uint64_t> non_sys_table_ids;
      ObArray<ObTableSchema *> non_sys_tables;
      if (OB_FAIL(schema_mgr_for_cache->get_non_sys_table_ids(tenant_id, non_sys_table_ids))) {
        LOG_WARN("fail to get non sys table_ids", KR(ret), K(schema_status));
      } else if (OB_FAIL(schema_service_->get_batch_table_schema(
                 schema_status, schema_version, non_sys_table_ids, sql_client,
                 allocator, non_sys_tables))) {
        LOG_WARN("get non core table schemas failed", KR(ret), K(schema_status), K(schema_version));
      } else if (OB_FAIL(update_non_sys_schemas_in_cache_(*schema_mgr_for_cache, non_sys_tables))) {
        LOG_WARN("update core and sys schemas in cache faield", KR(ret), K(schema_status));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_schema_version_in_inner_table(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    int64_t &target_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  const bool did_use_weak = (schema_status.snapshot_timestamp_ >= 0);
  if (OB_FAIL(schema_service_->fetch_schema_version(
      schema_status, sql_client, target_version))) {
    LOG_WARN("fail to fetch schema version", K(ret), K(schema_status));
  }
  return ret;
}

int ObServerSchemaService::check_core_or_sys_schema_change(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const int64_t core_schema_version,
    const int64_t schema_version,
    bool &core_schema_change,
    bool &sys_schema_change)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = 0;
  // check whether failed because of sys table schema change, go to suitable pos
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(schema_status));
  } else if (OB_FAIL(get_schema_version_in_inner_table(
    sql_client, schema_status, new_schema_version))) {
    LOG_WARN("fail to get schema version in inner table", KR(ret), K(schema_status));
  } else if (OB_FAIL(check_core_schema_change_(sql_client, schema_status,
             core_schema_version, core_schema_change))) {
    LOG_WARN("fail to check core schema change", KR(ret), K(schema_status), K(core_schema_version));
  } else if (core_schema_change) {
    sys_schema_change = true;
    LOG_WARN("core schema change", KR(ret), K(schema_status), K(core_schema_version), K(new_schema_version));
  } else if (OB_FAIL(check_sys_schema_change(sql_client, schema_status,
             schema_version, new_schema_version, sys_schema_change))) {
    LOG_WARN("sys schema change during refresh schema", KR(ret), K(schema_status),
             K(schema_version), K(new_schema_version));
  }
  return ret;
}

int ObServerSchemaService::check_core_schema_change_(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const int64_t core_schema_version,
    bool &core_schema_change)
{
  int ret = OB_SUCCESS;
  int64_t new_core_schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(schema_status));
  } else if (OB_FAIL(schema_service_->get_core_version(sql_client, schema_status, new_core_schema_version))) {
    LOG_WARN("fail to get core schema version", KR(ret), K(schema_status));
  } else if (core_schema_version != new_core_schema_version) {
    core_schema_change = true;
    LOG_WARN("core schema change during refresh sys schema", KR(ret),
             K(schema_status), K(core_schema_version), K(new_core_schema_version));
  } else {
    core_schema_change = false;
    LOG_INFO("core schema is not changed", KR(ret),
             K(schema_status), K(core_schema_version), K(new_core_schema_version));
  }
  return ret;
}

int ObServerSchemaService::check_sys_schema_change(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const int64_t schema_version,
    const int64_t new_schema_version,
    bool &sys_schema_change)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(schema_status));
  } else if (OB_FAIL(get_sys_table_ids(schema_status.tenant_id_, table_ids))) {
    LOG_WARN("get sys table_ ids failed", KR(ret), K(schema_status));
  } else if (OB_FAIL(schema_service_->check_sys_schema_change(sql_client, schema_status,
             table_ids, schema_version, new_schema_version, sys_schema_change))) {
    LOG_WARN("check_sys_schema_change failed", KR(ret),
             K(schema_status), K(schema_version), K(new_schema_version));
  }
  return ret;
}

int ObServerSchemaService::get_sys_table_ids(
    const uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_table_ids(tenant_id, sys_table_schema_creators, table_ids))) {
    LOG_WARN("fail to get table ids", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSysTableChecker::add_sys_table_index_ids(tenant_id, table_ids))) {
    LOG_WARN("fail to add sys table index ids", KR(ret), K(tenant_id));
  } else if (OB_FAIL(add_sys_table_lob_aux_ids(tenant_id, table_ids))) {
    LOG_WARN("fail to add sys table lob aux ids", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObServerSchemaService::get_table_ids(
    const uint64_t tenant_id,
    const schema_create_func *schema_creators,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  ObTableSchema schema;
  table_ids.reset();
  if (OB_ISNULL(schema_creators)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema creators should not be null", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && NULL != schema_creators[i]; ++i) {
    schema.reset();
    if (OB_FAIL(schema_creators[i](schema))) {
      LOG_WARN("create table schema failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_ids.push_back(schema.get_table_id()))) {
      LOG_WARN("push_back failed", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObServerSchemaService::add_sys_table_lob_aux_ids(
    const uint64_t tenant_id,
    ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    int64_t tbl_cnt = table_ids.count();
    // add sys table lob aux table id
    for (int64_t i = 0; OB_SUCC(ret) && i < tbl_cnt; i++) {
      uint64_t data_table_id = table_ids.at(i);
      uint64_t lob_meta_table_id = 0;
      uint64_t lob_piece_table_id = 0;
      if (is_system_table(data_table_id)) {
        if (OB_ALL_CORE_TABLE_TID == data_table_id) {
            // do nothing
        } else if (!(get_sys_table_lob_aux_table_id(data_table_id, lob_meta_table_id, lob_piece_table_id))) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("get lob aux table id failed.", K(ret), K(data_table_id));
        } else if (lob_meta_table_id == 0 || lob_piece_table_id == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get lob aux table id failed.", K(ret), K(data_table_id), K(lob_meta_table_id), K(lob_piece_table_id));
        } else if (OB_FAIL(table_ids.push_back(lob_meta_table_id))) {
          LOG_WARN("add lob meta table id failed", KR(ret), K(tenant_id), K(data_table_id), K(lob_meta_table_id));
        } else if (OB_FAIL(table_ids.push_back(lob_piece_table_id))) {
          LOG_WARN("add lob piece table id failed", KR(ret), K(tenant_id), K(data_table_id), K(lob_piece_table_id));
        }
      }
    }
  }
  return ret;
}

int ObServerSchemaService::query_tenant_status(
    const uint64_t tenant_id,
    TenantStatus &tenant_status)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else if (OB_FAIL(schema_service_->query_tenant_status(*sql_proxy_, tenant_id, tenant_status))) {
    LOG_WARN("fail to get tenant status", K(ret), K(tenant_id));
  }
  return ret;
}

int ObServerSchemaService::construct_schema_version_history(
    const ObRefreshSchemaStatus &schema_status,
    const int64_t snapshot_version,
    const VersionHisKey &key,
    VersionHisVal &val)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(schema_service_->construct_schema_version_history(
             schema_status, *sql_proxy_, snapshot_version, key, val))) {
    LOG_WARN("construct_schema_version_history failed", K(ret), K(snapshot_version), K(key));
  }

  return ret;
}

int ObServerSchemaService::get_tenant_schema_version(const uint64_t tenant_id, int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(schema_manager_rwlock_);
  schema_version = OB_INVALID_VERSION;
  ObSchemaVersionGetter getter;
  if (OB_FAIL(schema_mgr_for_cache_map_.atomic_refactored(tenant_id, getter))) {
    LOG_WARN("fail to get schema mgr for cache", K(ret));
  } else {
    schema_version = getter.get_schema_version();
  }
  return ret;
}

int ObServerSchemaService::get_refresh_schema_info(ObRefreshSchemaInfo &schema_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_refresh_schema_info(schema_info))) {
    LOG_WARN("fail to get schema_info", K(ret));
  }
  return ret;
}

int ObMaxSchemaVersionFetcher::operator() (common::hash::HashMapPair<uint64_t, ObSchemaMgr *> &entry)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if ((entry.second)->get_schema_version() > max_schema_version_) {
    max_schema_version_ = (entry.second)->get_schema_version();
  }
  return ret;
}

int ObSchemaVersionGetter::operator() (common::hash::HashMapPair<uint64_t, ObSchemaMgr *> &entry)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    schema_version_ = (entry.second)->get_schema_version();
  }
  return ret;
}

}    //end of namespace schema
}    //end of namespace share
}    //end of namespace oceanbase
