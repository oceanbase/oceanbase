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
#include "share/ob_worker.h"
#include "share/ob_get_compat_mode.h"
#include "observer/ob_server_struct.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_global_stat_proxy.h"
namespace oceanbase {
namespace share {
namespace schema {
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;

template <typename T>
class TenantIdTrait {
public:
  explicit TenantIdTrait(const T& value) : value_(value)
  {}
  uint64_t get_tenant_id() const
  {
    return value_.tenant_id_;
  }

private:
  const T& value_;
};
template <>
class TenantIdTrait<uint64_t> {
public:
  explicit TenantIdTrait(const uint64_t value) : value_(value)
  {}
  uint64_t get_tenant_id() const
  {
    return common::extract_tenant_id(value_);
  }

private:
  uint64_t value_;
};

ObServerSchemaService::ObServerSchemaService(bool enable_backup /* = false*/)
    : all_core_schema_init_(false),
      all_root_schema_init_(false),
      core_schema_init_(false),
      sys_schema_init_(false),
      sys_tenant_got_(false),
      backup_succ_(false),
      increment_basepoint_set_(false),
      schema_mgr_for_cache_(NULL),
      schema_service_(NULL),
      sql_proxy_(NULL),
      config_(NULL),
      refresh_times_(0),
      enable_backup_(enable_backup),
      guard_(NULL),
      load_sys_tenant_(false),
      core_schema_version_(0),
      schema_version_(0),
      baseline_schema_version_(-1),
      refresh_schema_type_(RST_INCREMENT_SCHEMA_ALL),
      refresh_full_schema_map_(),
      schema_mgr_for_cache_map_(),
      mem_mgr_map_(),
      mem_mgr_for_liboblog_map_(),
      mock_schema_info_map_(),
      schema_split_version_v2_map_()
{}

ObServerSchemaService::~ObServerSchemaService()
{
  destroy();
}

int ObServerSchemaService::destroy()
{
  int ret = OB_SUCCESS;
  if (NULL != schema_service_) {
    ObSchemaServiceSQLImpl* tmp = static_cast<ObSchemaServiceSQLImpl*>(schema_service_);
    OB_DELETE(ObSchemaServiceSQLImpl, ObModIds::OB_SCHEMA_SERVICE, tmp);
    schema_service_ = NULL;
  }
  if (OB_SUCC(ret)) {
    FOREACH(it, schema_mgr_for_cache_map_)
    {
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
    FOREACH(it, mem_mgr_map_)
    {
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
    FOREACH(it, mem_mgr_for_liboblog_map_)
    {
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

/**
 * add sys tenant schema
 * add sys user schema
 * add all_core table schema
 */
int ObServerSchemaService::init_basic_schema()
{
  int ret = OB_SUCCESS;
  ObSimpleTenantSchema sys_tenant;
  sys_tenant.set_tenant_id(OB_SYS_TENANT_ID);
  sys_tenant.set_tenant_name(OB_SYS_TENANT_NAME);
  sys_tenant.set_read_only(false);
  sys_tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

  ObSimpleUserSchema sys_user;
  sys_user.set_tenant_id(OB_SYS_TENANT_ID);
  sys_user.set_user_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_USER_ID));
  sys_user.set_schema_version(OB_CORE_SCHEMA_VERSION);

  ObSimpleSysVariableSchema sys_variable;
  sys_variable.set_tenant_id(OB_SYS_TENANT_ID);
  sys_variable.set_schema_version(OB_CORE_SCHEMA_VERSION);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  if (OB_FAIL(sys_user.set_user_name(OB_SYS_USER_NAME))) {
    LOG_WARN("set_user_name failed", K(ret));
  } else if (OB_FAIL(sys_user.set_host(OB_SYS_HOST_NAME))) {
    LOG_WARN("set_host failed", K(ret));
  } else {
    SpinWLockGuard guard(schema_manager_rwlock_);
    if (OB_ISNULL(schema_mgr_for_cache_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_mgr_for_cache_ is null", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache_->add_tenant(sys_tenant))) {
      LOG_WARN("add tenant failed", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache_->add_user(sys_user))) {
      LOG_WARN("add user failed", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache_->sys_variable_mgr_.add_sys_variable(sys_variable))) {
      LOG_WARN("add sys variable failed", K(ret));
    } else if (OB_FAIL(init_all_core_schema())) {
      LOG_WARN("init add core table schema failed", K(ret));
    } else {
      schema_mgr_for_cache_->set_tenant_id(OB_INVALID_TENANT_ID);
    }
  }
  return ret;
}

int ObServerSchemaService::init_sys_basic_schema()
{
  int ret = OB_SUCCESS;
  ObSimpleTenantSchema sys_tenant;
  sys_tenant.set_tenant_id(OB_SYS_TENANT_ID);
  sys_tenant.set_tenant_name(OB_SYS_TENANT_NAME);
  sys_tenant.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  sys_tenant.set_read_only(false);
  sys_tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

  ObSimpleUserSchema sys_user;
  sys_user.set_tenant_id(OB_SYS_TENANT_ID);
  sys_user.set_user_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_USER_ID));
  sys_user.set_schema_version(OB_CORE_SCHEMA_VERSION);

  ObSimpleSysVariableSchema sys_variable;
  sys_variable.set_tenant_id(OB_SYS_TENANT_ID);
  sys_variable.set_schema_version(OB_CORE_SCHEMA_VERSION);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  if (OB_FAIL(sys_user.set_user_name(OB_SYS_USER_NAME))) {
    LOG_WARN("set_user_name failed", K(ret));
  } else if (OB_FAIL(sys_user.set_host(OB_SYS_HOST_NAME))) {
    LOG_WARN("set_host failed", K(ret));
  } else {
    SpinWLockGuard guard(schema_manager_rwlock_);
    ObSchemaMgr* schema_mgr_for_cache = NULL;
    if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(OB_SYS_TENANT_ID, schema_mgr_for_cache))) {
      LOG_WARN("fail to get schema mgr for cache", K(ret));
    } else if (OB_ISNULL(schema_mgr_for_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema mgr for cache is null", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache->add_tenant(sys_tenant))) {
      LOG_WARN("add tenant failed", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache->add_user(sys_user))) {
      LOG_WARN("add user failed", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache->sys_variable_mgr_.add_sys_variable(sys_variable))) {
      LOG_WARN("add sys variable failed", K(ret));
    } else if (OB_FAIL(fill_all_core_table_schema(*schema_mgr_for_cache))) {
      LOG_WARN("init add core table schema failed", K(ret));
    } else {
      schema_mgr_for_cache->set_tenant_id(OB_SYS_TENANT_ID);
    }
  }
  return ret;
}

int ObServerSchemaService::init_tenant_basic_schema(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    ObString fake_tenant_name("fake_tenant");
    ObSimpleTenantSchema tenant;
    tenant.set_tenant_id(tenant_id);
    tenant.set_tenant_name(fake_tenant_name);
    tenant.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    tenant.set_read_only(false);
    tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

    ObSimpleSysVariableSchema sys_variable;
    sys_variable.set_tenant_id(tenant_id);
    sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    sys_variable.set_schema_version(OB_CORE_SCHEMA_VERSION);

    SpinWLockGuard guard(schema_manager_rwlock_);
    ObSchemaMgr* schema_mgr_for_cache = NULL;
    const ObSimpleTenantSchema* tenant_schema = NULL;
    if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
      LOG_WARN("fail to get schema mgr for cache", K(ret), K(tenant_id));
    } else if (OB_ISNULL(schema_mgr_for_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema mgr for cache is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_mgr_for_cache->get_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
    } else if (OB_NOT_NULL(tenant_schema)) {
      // already exist, do nothing
      LOG_INFO("tenant exist, do nothing", K(ret), K(tenant_id));
      ;
    } else if (OB_FAIL(schema_mgr_for_cache->add_tenant(tenant))) {
      LOG_WARN("add tenant failed", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache->sys_variable_mgr_.add_sys_variable(sys_variable))) {
      LOG_WARN("add sys variable failed", K(ret));
    } else {
      schema_mgr_for_cache->set_tenant_id(tenant_id);
      schema_mgr_for_cache->set_schema_version(OB_CORE_SCHEMA_VERSION);
    }
  }
  return ret;
}

int ObServerSchemaService::init(ObMySQLProxy* sql_proxy, ObDbLinkProxy* dblink_proxy, const ObCommonConfig* config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy) || NULL != schema_service_ || OB_ISNULL(sql_proxy->get_pool()) || OB_ISNULL(config)) {
    ret = OB_INIT_FAIL;
    LOG_WARN("check param failed",
        K(sql_proxy),
        K(schema_service_),
        "proxy->pool",
        (NULL == sql_proxy ? NULL : sql_proxy->get_pool()),
        K(config));
  } else if (OB_FAIL(ObSysTableChecker::instance().init())) {
    LOG_WARN("fail to init tenant space table checker", K(ret));
  } else if (NULL == (schema_service_ = OB_NEW(ObSchemaServiceSQLImpl, ObModIds::OB_SCHEMA_SERVICE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate schema service sql impl failed, no memory");
  } else if (OB_FAIL(schema_service_->init(sql_proxy, dblink_proxy, this))) {
    LOG_ERROR("fail to init schema service,", K(ret));
  } else if (FALSE_IT(schema_service_->set_common_config(config))) {
    // will not reach here
  } else if (OB_FAIL(version_his_map_.create(
                 VERSION_HIS_MAP_BUCKET_NUM, ObModIds::OB_SCHEMA_ID_VERSIONS, ObModIds::OB_SCHEMA_ID_VERSIONS))) {
    LOG_WARN("create version his map failed", K(ret));
  } else if (OB_FAIL(mem_mgr_.init(ObModIds::OB_SCHEMA_MGR, OB_INVALID_TENANT_ID))) {
    LOG_WARN("init mem mgr failed", K(ret));
  } else if (OB_FAIL(mem_mgr_for_liboblog_.init(ObModIds::OB_SCHEMA_MGR_FOR_ROLLBACK, OB_INVALID_TENANT_ID))) {
    LOG_WARN("init mem mgr for libolog failed", K(ret));
  } else {
    void* tmp_ptr = NULL;
    ObIAllocator* allocator = NULL;
    if (OB_FAIL(mem_mgr_.alloc(sizeof(ObSchemaMgr), tmp_ptr, &allocator))) {
      LOG_WARN("alloc schema mgr failed", K(ret));
    } else if (FALSE_IT(schema_mgr_for_cache_ = new (tmp_ptr) ObSchemaMgr(*allocator))) {
      // will not reach here
    } else if (OB_FAIL(schema_mgr_for_cache_->init())) {
      LOG_WARN("init schema mgr for cache failed", K(ret));
    } else {
      sql_proxy_ = sql_proxy;
      config_ = config;
      int32_t len = snprintf(schema_file_path_,
          sizeof(schema_file_path_),
          OB_BACKUP_SCHEMA_FILE_PATTERN,
          print_server_role(config_->get_server_type()));
      if (len < 0 || len >= static_cast<int32_t>(sizeof(schema_file_path_))) {
        ret = OB_INIT_FAIL;
        LOG_ERROR("schema file path size failed: ", K(ret));
      }
    }
  }
  // construct basic schema
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_basic_schema())) {
      LOG_WARN("failed to init basic schema from hard code: ", K(ret));
    }
  }

  // init schema management struct for tenant
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresh_full_schema_map_.create(
            TENANT_MAP_BUCKET_NUM, ObModIds::OB_REFRESH_FULL_SCHEMA_MAP, ObModIds::OB_REFRESH_FULL_SCHEMA_MAP))) {
      LOG_WARN("fail to create tenant_schema_info_map", K(ret));
    } else if (OB_FAIL(
                   mem_mgr_map_.create(TENANT_MAP_BUCKET_NUM, ObModIds::OB_MEM_MGR_MAP, ObModIds::OB_MEM_MGR_MAP))) {
      LOG_WARN("fail to create map", K(ret));
    } else if (OB_FAIL(mem_mgr_for_liboblog_map_.create(TENANT_MAP_BUCKET_NUM,
                   ObModIds::OB_MEM_MGR_FOR_LIBOBLOG_MAP,
                   ObModIds::OB_MEM_MGR_FOR_LIBOBLOG_MAP))) {
      LOG_WARN("fail to create map", K(ret));
    } else if (OB_FAIL(schema_mgr_for_cache_map_.create(TENANT_MAP_BUCKET_NUM,
                   ObModIds::OB_TENANT_SCHEMA_FOR_CACHE_MAP,
                   ObModIds::OB_TENANT_SCHEMA_FOR_CACHE_MAP))) {
      LOG_WARN("fail to create map", K(ret));
    } else if (OB_FAIL(mock_schema_info_map_.create(
                   MOCK_SCHEMA_BUCKET_NUM, ObModIds::OB_MOCK_SCHEMA_INFO_MAP, ObModIds::OB_MOCK_SCHEMA_INFO_MAP))) {
      LOG_WARN("fail to mock schema info map", K(ret));
    } else if (OB_FAIL(
                   schema_split_version_v2_map_.create(TENANT_MAP_BUCKET_NUM, "ScheSplitV2Map", "ScheSplitV2Map"))) {
      LOG_WARN("fail to create tenant_schema_info_map", K(ret));
    } else if (OB_FAIL(init_schema_struct(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init schema struct", K(ret));
    } else if (OB_FAIL(init_sys_basic_schema())) {
      LOG_WARN("fail to init basic schema for sys", K(ret));
    } else {
      LOG_INFO("init schema service", K(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::destroy_schema_struct(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || (OB_SYS_TENANT_ID == tenant_id && !GCTX.is_schema_splited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else {
    SpinWLockGuard guard(schema_manager_rwlock_);

    bool overwrite = true;
    bool refresh_full_schema = true;
    if (OB_FAIL(ret)) {
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      // System tenants clear the old schema memory management structure
      // to avoid affecting the new schema refresh logic
    } else if (OB_FAIL(refresh_full_schema_map_.set_refactored(tenant_id, refresh_full_schema, overwrite))) {
      LOG_WARN("fail to erase refresh_full_schema_map_", K(ret), K(tenant_id));
    } else {
      LOG_INFO("reset tenant refresh_full mark", K(ret), K(tenant_id));
    }

    ObSchemaMgr** schema_mgr = NULL;
    ObSchemaMemMgr* mem_mgr = NULL;
    if (OB_SYS_TENANT_ID == tenant_id) {
      schema_mgr = &schema_mgr_for_cache_;
      mem_mgr = &mem_mgr_;
    } else {
      ObSchemaMgr* const* tmp_mgr = schema_mgr_for_cache_map_.get(tenant_id);
      if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
        LOG_WARN("fail to get mem mgr", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_mgr_for_cache_map_.erase_refactored(tenant_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get schema mgr for cache", K(ret));
        }
      } else {
        schema_mgr = const_cast<ObSchemaMgr**>(tmp_mgr);
        LOG_INFO("[SCHEMA_RELEASE] erase tenant from schema_mgr_for_cache_map", K(ret), K(tenant_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_mgr) || OB_ISNULL(*schema_mgr)) {
      LOG_INFO("schema_mgr_for_cache has been released, just skip", K(ret), K(tenant_id));
    } else if (OB_ISNULL(mem_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_mgr is null", K(ret), K(tenant_id));
    } else {
      LOG_INFO("[SCHEMA_RELEASE] try release schema_mgr_for_cache",
          K(ret),
          K(tenant_id),
          "schema_mgr_tenant_id",
          (*schema_mgr)->get_tenant_id(),
          "schema_version",
          (*schema_mgr)->get_schema_version());
      (*schema_mgr)->~ObSchemaMgr();
      if (OB_FAIL(mem_mgr->free(static_cast<void*>(*schema_mgr)))) {
        LOG_ERROR("free schema mgr for cache failed", K(ret), K(tenant_id));
      } else {
        *schema_mgr = NULL;
      }
    }
  }

  return ret;
}

bool ObServerSchemaService::check_inner_stat() const
{
  bool ret = true;
  if (NULL == schema_service_ || NULL == sql_proxy_ || NULL == config_ ||
      (!GCTX.is_schema_splited() && NULL == schema_mgr_for_cache_)) {
    ret = false;
    LOG_WARN("inner stat error", K(schema_service_), K(sql_proxy_), K(config_), K(schema_mgr_for_cache_));
  }
  return ret;
}

int ObServerSchemaService::check_stop() const
{
  int ret = OB_SUCCESS;
  if (observer::SS_STOPPING == GCTX.status_ || observer::SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("observer is stopping", K(ret));
  }
  return ret;
}

void ObServerSchemaService::AllSchemaKeys::reset()
{
  // use clear
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
  new_udf_keys_.clear();
  del_udf_keys_.clear();
  new_sequence_keys_.clear();
  del_sequence_keys_.clear();
  new_sys_variable_keys_.clear();
  del_sys_variable_keys_.clear();
  add_drop_tenant_keys_.clear();
  del_drop_tenant_keys_.clear();
  new_profile_keys_.clear();
  del_profile_keys_.clear();
  new_sys_priv_keys_.clear();
  del_sys_priv_keys_.clear();
  new_obj_priv_keys_.clear();
  del_obj_priv_keys_.clear();
  new_dblink_keys_.clear();
  del_dblink_keys_.clear();
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
  } else if (OB_FAIL(new_synonym_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_synonym_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_synonym_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_synonym_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_udf_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_udf_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_udf_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_udf_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_sequence_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new_sequence_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_sequence_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_sequence_ids hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_sys_variable_keys_.create(bucket_size))) {
    LOG_WARN("failed to create new sys vairable hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_sys_variable_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del sys variable hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(add_drop_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create add_drop_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_drop_tenant_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_drop_tenant_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(new_profile_keys_.create(bucket_size))) {
    LOG_WARN("failed to create add_profile_keys hashset", K(bucket_size), K(ret));
  } else if (OB_FAIL(del_profile_keys_.create(bucket_size))) {
    LOG_WARN("failed to create del_profile_keys hashset", K(bucket_size), K(ret));
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
  }
  return ret;
}

void ObServerSchemaService::AllIncrementSchema::reset()
{
  table_schema_.reset();
  db_schema_.reset();
  tg_schema_.reset();
  max_used_tids_.reset();
  // For managing privileges
  tenant_info_array_.reset();
  user_info_array_.reset();
  db_priv_array_.reset();
  table_priv_array_.reset();
}

//////////////////////////////////////////////////////////////////////////////////////////////
//                              SCHEMA SERVICE RELATED                                      //
//////////////////////////////////////////////////////////////////////////////////////////////

ObSchemaService* ObServerSchemaService::get_schema_service(void) const
{
  return schema_service_;
}

#define DEL_OP(SCHEMA_KEYS, tenant_id, keys)                                                      \
  ({                                                                                              \
    int ret = OB_SUCCESS;                                                                         \
    if (OB_INVALID_ID == tenant_id) {                                                             \
      ret = OB_INVALID_ARGUMENT;                                                                  \
      LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));                                        \
    }                                                                                             \
    ObArray<SchemaKey> to_remove;                                                                 \
    for (SCHEMA_KEYS::const_iterator it = keys.begin(); OB_SUCC(ret) && it != keys.end(); ++it) { \
      if ((it->first).tenant_id_ == tenant_id) {                                                  \
        if (OB_FAIL(to_remove.push_back(it->first))) {                                            \
          LOG_WARN("push_back failed", K(ret));                                                   \
        }                                                                                         \
      }                                                                                           \
    }                                                                                             \
    FOREACH_CNT_X(v, to_remove, OB_SUCCESS == ret)                                                \
    {                                                                                             \
      int64_t hash_ret = keys.erase_refactored(*v);                                               \
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {                              \
        ret = OB_ERR_UNEXPECTED;                                                                  \
        LOG_WARN("erase failed", "value", *v, K(ret));                                            \
      }                                                                                           \
    }                                                                                             \
    ret;                                                                                          \
  })

#define DEL_TENANT_OP(tenant_id, schema_keys, OP)                                                  \
  ({                                                                                               \
    int ret = OB_SUCCESS;                                                                          \
    if (OB_INVALID_ID == tenant_id) {                                                              \
      ret = OB_INVALID_ARGUMENT;                                                                   \
      LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));                                         \
    } else if (OB_FAIL(DEL_OP(UserKeys, tenant_id, schema_keys.OP##_user_keys_))) {                \
      LOG_WARN("del " #OP "_user_keys failed", K(tenant_id), K(ret));                              \
    } else if (OB_FAIL(DEL_OP(DatabaseKeys, tenant_id, schema_keys.OP##_database_keys_))) {        \
      LOG_WARN("del " #OP "_db_keys failed", K(tenant_id), K(ret));                                \
    } else if (OB_FAIL(DEL_OP(TablegroupKeys, tenant_id, schema_keys.OP##_tablegroup_keys_))) {    \
      LOG_WARN("del " #OP "_tg_keys failed", K(tenant_id), K(ret));                                \
    } else if (OB_FAIL(DEL_OP(TableKeys, tenant_id, schema_keys.OP##_table_keys_))) {              \
      LOG_WARN("del " #OP "_table_keys failed", K(tenant_id), K(ret));                             \
    } else if (OB_FAIL(DEL_OP(OutlineKeys, tenant_id, schema_keys.OP##_outline_keys_))) {          \
      LOG_WARN("del " #OP "_outline_keys failed", K(tenant_id), K(ret));                           \
    } else if (OB_FAIL(DEL_OP(DBPrivKeys, tenant_id, schema_keys.OP##_db_priv_keys_))) {           \
      LOG_WARN("del " #OP "_db_priv_keys failed", K(tenant_id), K(ret));                           \
    } else if (OB_FAIL(DEL_OP(TablePrivKeys, tenant_id, schema_keys.OP##_table_priv_keys_))) {     \
      LOG_WARN("del " #OP "_table_priv_keys failed", K(tenant_id), K(ret));                        \
    } else if (OB_FAIL(DEL_OP(SynonymKeys, tenant_id, schema_keys.OP##_synonym_keys_))) {          \
      LOG_WARN("del " #OP "_synonym_keys failed", K(tenant_id), K(ret));                           \
    } else if (OB_FAIL(DEL_OP(UdfKeys, tenant_id, schema_keys.OP##_udf_keys_))) {                  \
      LOG_WARN("del " #OP "_udf_keys failed", K(tenant_id), K(ret));                               \
    } else if (OB_FAIL(DEL_OP(SequenceKeys, tenant_id, schema_keys.OP##_sequence_keys_))) {        \
      LOG_WARN("del " #OP "_sequence_keys failed", K(tenant_id), K(ret));                          \
    } else if (OB_FAIL(DEL_OP(SysVariableKeys, tenant_id, schema_keys.OP##_sys_variable_keys_))) { \
      LOG_WARN("del " #OP "_sys_variable_keys failed", K(tenant_id), K(ret));                      \
    } else if (OB_FAIL(DEL_OP(ProfileKeys, tenant_id, schema_keys.OP##_profile_keys_))) {          \
      LOG_WARN("del " #OP "_profile_keys failed", K(tenant_id), K(ret));                           \
    } else if (OB_FAIL(DEL_OP(SysPrivKeys, tenant_id, schema_keys.OP##_sys_priv_keys_))) {         \
      LOG_WARN("del " #OP "_sys_priv_keys failed", K(tenant_id), K(ret));                          \
    } else if (OB_FAIL(DEL_OP(ObjPrivKeys, tenant_id, schema_keys.OP##_obj_priv_keys_))) {         \
      LOG_WARN("del " #OP "_obj_priv_keys failed", K(tenant_id), K(ret));                          \
    }                                                                                              \
    ret;                                                                                           \
  })

int ObServerSchemaService::get_increment_tenant_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    // the system tenant's schema will refreshed incremently too
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), K(ret));
    } else if (OB_DDL_ADD_TENANT == schema_operation.op_type_ || OB_DDL_ADD_TENANT_START == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tenant_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new tenant id to new_tenant_ids", K(hash_ret), K(ret));
      }
    } else if (OB_DDL_RENAME_TENANT == schema_operation.op_type_ || OB_DDL_ALTER_TENANT == schema_operation.op_type_ ||
               OB_DDL_ADD_TENANT_END == schema_operation.op_type_ ||
               OB_DDL_DEL_TENANT_START == schema_operation.op_type_ ||
               OB_DDL_DROP_TENANT_TO_RECYCLEBIN == schema_operation.op_type_ ||
               OB_DDL_FLASHBACK_TENANT == schema_operation.op_type_) {
      // TODO: OB_DDL_DEL_TENANT_START does not perform any substantial schema recovery actions,
      //  only DDL and user table writes are prohibited.
      //      open a transaction in the DROP TENANT DDL to push up the schema_version of the user tenant,
      //      and do the processing in the RS inspection task to prevent the DDL writing of the user tenant from
      //      failing. When user tenants fresh the DDL, it triggers the recycling of non-system tables related schemas.
      hash_ret = schema_keys.alter_tenant_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add alter tenant id to alter_tenant_ids", K(hash_ret), K(ret));
      }
    } else if (OB_DDL_DEL_TENANT == schema_operation.op_type_ || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tenant_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped tenant id", K(hash_ret), K(ret));
      } else {
        hash_ret = schema_keys.alter_tenant_keys_.erase_refactored(schema_key);
        if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to del dropped tenant id", K(hash_ret), K(ret));
        } else if (OB_FAIL(DEL_TENANT_OP(tenant_id, schema_keys, new))) {
          LOG_WARN("delete new op failed", K(tenant_id), K(ret));
        } else if (OB_FAIL(DEL_TENANT_OP(tenant_id, schema_keys, del))) {
          LOG_WARN("delete del op failed", K(tenant_id), K(ret));
        } else {
          const ObSimpleTenantSchema* tenant = NULL;
          if (OB_FAIL(schema_mgr.get_tenant_schema(tenant_id, tenant))) {
            LOG_WARN("get tenant schema failed", K(tenant_id), K(ret));
          } else if (NULL != tenant) {
            hash_ret = schema_keys.del_tenant_keys_.set_refactored_1(schema_key, 1);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to add del tenant id to del_tenant_ids", K(hash_ret), K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t count = schema_keys.del_drop_tenant_keys_.size();
        if (0 != count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("del_drop_tenant_keys num should be 0", K(ret), K(count));
        } else {
          hash_ret = schema_keys.add_drop_tenant_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add drop tenant key", K(hash_ret), K(ret), K(schema_key));
          }
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_sys_variable_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID != schema_mgr.get_tenant_id() && schema_operation.tenant_id_ != schema_mgr.tenant_id_) {
    LOG_INFO(
        "sys variable key not match, just skip", K(ret), K(schema_operation), "tenant_id", schema_mgr.get_tenant_id());
  } else if (!((schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END) ||
                 (schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN &&
                     schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.schema_version_ = schema_version;
    // the system tenant's schema will refreshed incremently too
    if (!schema_operation.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(schema_operation), K(ret));
    } else if (OB_DDL_DEL_TENANT == schema_operation.op_type_ || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
      hash_ret = schema_keys.new_sys_variable_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped sys variable", K(hash_ret), K(ret));
      } else {
        const ObSimpleSysVariableSchema* sys_variable = NULL;
        if (OB_FAIL(schema_mgr.sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
          LOG_WARN("get sys variable schema failed", K(tenant_id), K(ret));
        } else if (NULL != sys_variable) {
          hash_ret = schema_keys.del_sys_variable_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add del sys variable", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_sys_variable_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new sys variable keys", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_user_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_USER_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
      LOG_WARN("invalid argument", K(schema_operation), K(ret));
    } else if (OB_DDL_DROP_USER == schema_operation.op_type_) {
      hash_ret = schema_keys.new_user_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del dropped user id", K(hash_ret), K(ret));
      } else {
        const ObSimpleUserSchema* user = NULL;
        if (OB_FAIL(schema_mgr.get_user_schema(user_id, user))) {
          LOG_WARN("get user info failed", K(user_id), K(ret));
        } else if (NULL != user) {
          hash_ret = schema_keys.del_user_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add del user id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_user_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new user id", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_database_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
      LOG_WARN("invalid argument", K(schema_operation), K(ret));
    } else if (OB_DDL_DEL_DATABASE == schema_operation.op_type_) {
      hash_ret = schema_keys.new_database_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped db id", K(hash_ret), K(ret));
      } else {
        const ObSimpleDatabaseSchema* database = NULL;
        if (OB_FAIL(schema_mgr.get_database_schema(db_id, database))) {
          LOG_WARN("get database schema failed", K(db_id), K(ret));
        } else if (NULL != database) {
          hash_ret = schema_keys.del_database_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del db id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_database_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new database id", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_tablegroup_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
      LOG_WARN("invalid argument", K(schema_operation), K(ret));
    } else if (OB_DDL_DEL_TABLEGROUP == schema_operation.op_type_) {
      hash_ret = schema_keys.new_tablegroup_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped tg id", K(hash_ret), K(ret));
      } else {
        const ObSimpleTablegroupSchema* tg = NULL;
        if (OB_FAIL(schema_mgr.get_tablegroup_schema(tg_id, tg))) {
          LOG_WARN("get tablegroup schema failed", K(tg_id), K(ret));
        } else if (NULL != tg) {
          hash_ret = schema_keys.del_tablegroup_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del tg id to del_log", K(hash_ret), K(ret));
          }
        } else {
        }
      }
    } else {
      hash_ret = schema_keys.new_tablegroup_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new tablegroup id", K(hash_ret), K(ret));
      } else if (OB_DDL_ALTER_TABLEGROUP_PARTITION == schema_operation.op_type_ ||
                 OB_DDL_SPLIT_TABLEGROUP_PARTITION == schema_operation.op_type_ ||
                 OB_DDL_PARTITIONED_TABLEGROUP_TABLE == schema_operation.op_type_ ||
                 OB_DDL_FINISH_SPLIT_TABLEGROUP == schema_operation.op_type_ ||
                 OB_DDL_FINISH_LOGICAL_SPLIT_TABLEGROUP == schema_operation.op_type_ ||
                 OB_DDL_DELAY_DELETE_TABLEGROUP_PARTITION == schema_operation.op_type_) {
        // alter tablegroup partition is a batch operation, which needs to trigger the schema refresh of
        // the table under the same tablegroup
        ObArray<const ObSimpleTableSchemaV2*> table_schemas;
        bool need_reset = true;
        if (OB_FAIL(schema_mgr.get_table_schemas_in_tablegroup(tenant_id, tg_id, need_reset, table_schemas))) {
          LOG_WARN("fail to get table schemas in tablegroup", K(ret));
        } else {
          SchemaKey table_schema_key;
          table_schema_key.tenant_id_ = tenant_id;
          table_schema_key.schema_version_ = schema_version;
          for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
            table_schema_key.table_id_ = table_schemas.at(i)->get_table_id();
            ret = schema_keys.del_table_keys_.exist_refactored(table_schema_key);
            if (OB_HASH_EXIST == ret) {
              // This schema refresh involves the drop table operation, there is no need to refresh the table's schema
              ret = OB_SUCCESS;
            } else if (OB_HASH_NOT_EXIST == ret) {
              if (OB_FAIL(schema_keys.new_table_keys_.set_refactored_1(table_schema_key, 1))) {
                LOG_WARN("fail to set table_schema_key", K(ret), K(table_schema_key), K(tg_id));
              }
            } else {
              LOG_WARN("fail to check table schema key exist", K(ret), K(table_schema_key), K(tg_id));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_table_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t table_id = schema_operation.table_id_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey schema_key;
    schema_key.tenant_id_ = tenant_id;
    schema_key.table_id_ = table_id;
    schema_key.schema_version_ = schema_version;
    if (OB_DDL_DROP_TABLE == schema_operation.op_type_ || OB_DDL_DROP_INDEX == schema_operation.op_type_ ||
        OB_DDL_DROP_GLOBAL_INDEX == schema_operation.op_type_ || OB_DDL_DROP_VIEW == schema_operation.op_type_ ||
        OB_DDL_TRUNCATE_TABLE_DROP == schema_operation.op_type_) {
      hash_ret = schema_keys.new_table_keys_.erase_refactored(schema_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to del dropped table id", K(hash_ret), K(ret));
      } else {
        const ObSimpleTableSchemaV2* table = NULL;
        if (OB_FAIL(schema_mgr.get_table_schema(table_id, table))) {
          LOG_WARN("failed to get table schema", K(table_id), K(ret));
        } else if (NULL != table) {
          hash_ret = schema_keys.del_table_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del table id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_table_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new table id", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_outline_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
        LOG_WARN("failed to del dropped outline id", K(hash_ret), K(ret));
      } else {
        const ObSimpleOutlineSchema* outline = NULL;
        if (OB_FAIL(schema_mgr.outline_mgr_.get_outline_schema(outline_id, outline))) {
          LOG_WARN("failed to get outline schema", K(outline_id), K(ret));
        } else if (NULL != outline) {
          hash_ret = schema_keys.del_outline_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del outline id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_outline_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new outline id", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_synonym_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
        LOG_WARN("failed to del dropped synonym id", K(hash_ret), K(ret));
      } else {
        const ObSimpleSynonymSchema* synonym = NULL;
        if (OB_FAIL(schema_mgr.synonym_mgr_.get_synonym_schema(synonym_id, synonym))) {
          LOG_WARN("failed to get synonym schema", K(synonym_id), K(ret));
        } else if (NULL != synonym) {
          hash_ret = schema_keys.del_synonym_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del synonym id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_synonym_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new synonym id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_db_priv_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const ObString& database_name = schema_operation.database_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey db_priv_key;
    db_priv_key.tenant_id_ = tenant_id;
    db_priv_key.user_id_ = user_id;
    db_priv_key.database_name_ = database_name;
    db_priv_key.schema_version_ = schema_version;
    if (OB_DDL_DEL_DB_PRIV == schema_operation.op_type_) {  // delete
      hash_ret = schema_keys.new_db_priv_keys_.erase_refactored(db_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del db_priv_key from new_db_priv_keys", K(ret));
      } else {
        const ObDBPriv* db_priv = NULL;
        if (OB_FAIL(
                schema_mgr.priv_mgr_.get_db_priv(ObOriginalDBKey(tenant_id, user_id, database_name), db_priv, true))) {
          LOG_WARN("get db priv set failed", K(ret));
        } else if (NULL != db_priv) {
          hash_ret = schema_keys.del_db_priv_keys_.set_refactored_1(db_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add db_priv_key to del_db_priv_keys", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_db_priv_keys_.set_refactored_1(db_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new db_priv_key", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_sys_priv_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t grantee_id = schema_operation.grantee_id_;

    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey sys_priv_key;
    sys_priv_key.tenant_id_ = tenant_id;
    sys_priv_key.grantee_id_ = grantee_id;
    sys_priv_key.schema_version_ = schema_version;
    if (OB_DDL_SYS_PRIV_DELETE == schema_operation.op_type_) {  // delete
      hash_ret = schema_keys.new_sys_priv_keys_.erase_refactored(sys_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del sys_priv_key from new_sys_priv_keys", K(ret));
      } else {
        const ObSysPriv* sys_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_sys_priv(ObSysPrivKey(tenant_id, grantee_id), sys_priv))) {
          LOG_WARN("get sys priv set failed", K(ret));
        } else if (NULL != sys_priv) {
          hash_ret = schema_keys.del_sys_priv_keys_.set_refactored_1(sys_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add sys_priv_key to del_sys_priv_keys", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_sys_priv_keys_.set_refactored_1(sys_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new sys_priv_key", K(hash_ret), K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_table_priv_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const uint64_t user_id = schema_operation.user_id_;
    const ObString& database_name = schema_operation.database_name_;
    const ObString& table_name = schema_operation.table_name_;
    const int64_t schema_version = schema_operation.schema_version_;
    int hash_ret = OB_SUCCESS;
    SchemaKey table_priv_key;
    table_priv_key.tenant_id_ = tenant_id;
    table_priv_key.user_id_ = user_id;
    table_priv_key.database_name_ = database_name;
    table_priv_key.table_name_ = table_name;
    table_priv_key.schema_version_ = schema_version;
    if (OB_DDL_DEL_TABLE_PRIV == schema_operation.op_type_) {  // delete
      hash_ret = schema_keys.new_table_priv_keys_.erase_refactored(table_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del table_priv_key from new_table_priv_keys", K(ret));
      } else {
        const ObTablePriv* table_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_table_priv(
                ObTablePrivSortKey(tenant_id, user_id, database_name, table_name), table_priv))) {
          LOG_WARN("get table priv failed", K(ret));
        } else if (NULL != table_priv) {
          hash_ret = schema_keys.del_table_priv_keys_.set_refactored_1(table_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add table_priv_key to del_table_priv_keys", K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_table_priv_keys_.set_refactored_1(table_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new table_priv_key", K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_obj_priv_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
    if (OB_DDL_OBJ_PRIV_DELETE == schema_operation.op_type_) {  // delete
      hash_ret = schema_keys.new_obj_priv_keys_.erase_refactored(obj_priv_key);
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to del table_priv_key from new_obj_priv_keys", K(ret));
      } else {
        const ObObjPriv* obj_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_obj_priv(
                ObObjPrivSortKey(tenant_id, obj_id, obj_type, col_id, grantor_id, grantee_id), obj_priv))) {
          LOG_WARN("get obj priv failed", K(ret));
        } else if (NULL != obj_priv) {
          hash_ret = schema_keys.del_obj_priv_keys_.set_refactored_1(obj_priv_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to add obj_priv_key to del_obj_priv_keys", K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_obj_priv_keys_.set_refactored_1(obj_priv_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to add new obj_priv_key", K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_increment_udf_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    const uint64_t tenant_id = schema_operation.tenant_id_;
    const ObString& udf_name = schema_operation.udf_name_;
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
        LOG_WARN("failed to del dropped udf id", K(hash_ret), K(ret));
      } else {
        const ObSimpleUDFSchema* udf = NULL;
        if (OB_FAIL(schema_mgr.udf_mgr_.get_udf_schema_with_name(tenant_id, udf_name, udf))) {
          LOG_WARN("failed to get udf schema", K(udf_name), K(ret));
        } else if (NULL != udf) {
          hash_ret = schema_keys.del_udf_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del udf id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_udf_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new udf id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_sequence_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  if (!(schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t sequence_id = schema_operation.table_id_;
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
        LOG_WARN("failed to del dropped sequence id", K(hash_ret), K(ret));
      } else {
        const ObSequenceSchema* sequence = NULL;
        if (OB_FAIL(schema_mgr.sequence_mgr_.get_sequence_schema(sequence_id, sequence))) {
          LOG_WARN("failed to get sequence schema", K(sequence_id), K(ret));
        } else if (NULL != sequence) {
          hash_ret = schema_keys.del_sequence_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del sequence id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_sequence_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new sequence id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_profile_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
  } else {
    uint64_t tenant_id = schema_operation.tenant_id_;
    uint64_t profile_id = schema_operation.table_id_;
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
        LOG_WARN("failed to del dropped profile id", K(hash_ret), K(ret));
      } else {
        const ObProfileSchema* schema = NULL;
        if (OB_FAIL(schema_mgr.profile_mgr_.get_schema_by_id(profile_id, schema))) {
          LOG_WARN("failed to get profile schema", K(profile_id), K(ret));
        } else if (NULL != schema) {
          hash_ret = schema_keys.del_profile_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del profile id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_profile_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new profile id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::get_increment_dblink_keys(
    const ObSchemaMgr& schema_mgr, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;
  if (!(schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(schema_operation.op_type_), K(ret));
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
        LOG_WARN("failed to del dropped dblink id", K(hash_ret), K(ret));
      } else {
        const ObDbLinkSchema* dblink_schema = NULL;
        if (OB_FAIL(schema_mgr.dblink_mgr_.get_dblink_schema(dblink_id, dblink_schema))) {
          LOG_WARN("failed to get dblink schema", K(dblink_id), K(ret));
        } else if (NULL != dblink_schema) {
          hash_ret = schema_keys.del_dblink_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add del dblink id", K(hash_ret), K(ret));
          }
        }
      }
    } else {
      hash_ret = schema_keys.new_dblink_keys_.set_refactored_1(schema_key, 1);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add new dblink id", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}

#define CONVERT_SCHEMA_KEYS_TO_ARRAY(SCHEMA_KEYS, key_set, key_arr)                                     \
  ({                                                                                                    \
    int ret = OB_SUCCESS;                                                                               \
    key_arr.reset();                                                                                    \
    for (SCHEMA_KEYS::const_iterator it = key_set.begin(); OB_SUCC(ret) && it != key_set.end(); it++) { \
      if (OB_FAIL(key_arr.push_back(it->first))) {                                                      \
        LOG_WARN("fail to push back:", K(ret));                                                         \
      }                                                                                                 \
    }                                                                                                   \
    ret;                                                                                                \
  })

#define EXTRACT_IDS_FROM_KEYS(SCHEMA_KEYS, SCHEMA, key_set, id_arr)                                     \
  ({                                                                                                    \
    int ret = OB_SUCCESS;                                                                               \
    id_arr.reset();                                                                                     \
    for (SCHEMA_KEYS::const_iterator it = key_set.begin(); OB_SUCC(ret) && it != key_set.end(); it++) { \
      if (OB_FAIL(id_arr.push_back(it->first.SCHEMA##_id_))) {                                          \
        LOG_WARN("fail to push back:", K(ret));                                                         \
      }                                                                                                 \
    }                                                                                                   \
    ret;                                                                                                \
  })

// Currently only the full tenant schema of the system tenant is cached
int ObServerSchemaService::add_tenant_schemas_to_cache(const TenantKeys& tenant_keys, ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;
  if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(TenantKeys, tenant_keys, schema_keys))) {
    LOG_WARN("convert set to array failed", K(ret));
  } else {
    FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret))
    {
      const uint64_t tenant_id = schema_key->tenant_id_;
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(add_tenant_schema_to_cache(sql_client, tenant_id, schema_key->schema_version_))) {
          LOG_WARN("add tenant schema to cache failed", K(ret), K(tenant_id), K(schema_key->schema_version_));
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
    const SysVariableKeys& sys_variable_keys, ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;
  if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(SysVariableKeys, sys_variable_keys, schema_keys))) {
    LOG_WARN("convert set to array failed", K(ret));
  } else {
    FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret))
    {
      const uint64_t tenant_id = schema_key->tenant_id_;
      if (OB_SYS_TENANT_ID == tenant_id) {
        ObRefreshSchemaStatus schema_status;
        schema_status.tenant_id_ = tenant_id;
        if (OB_FAIL(
                add_sys_variable_schema_to_cache(sql_client, schema_status, tenant_id, schema_key->schema_version_))) {
          LOG_WARN("add sys variable schema to cache failed", K(ret), K(tenant_id), K(schema_key->schema_version_));
        } else {
          LOG_INFO("add sys variable schema to cache success", K(ret), KPC(schema_key));
        }
        break;
      }
    }
  }
  return ret;
}

int ObServerSchemaService::fetch_increment_schemas(const ObRefreshSchemaStatus& schema_status,
    const AllSchemaKeys& all_keys, const int64_t schema_version, ObISQLClient& sql_client,
    AllSimpleIncrementSchema& simple_incre_schemas)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;

#define GET_BATCH_SCHEMAS(SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS)                                          \
  if (OB_SUCC(ret)) {                                                                                \
    schema_keys.reset();                                                                             \
    const SCHEMA_KEYS& new_keys = all_keys.new_##SCHEMA##_keys_;                                     \
    ObArray<SCHEMA_TYPE>& simple_schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;          \
    if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(SCHEMA_KEYS, new_keys, schema_keys))) {                 \
      LOG_WARN("convert set to array failed", K(ret));                                               \
    } else if (OB_FAIL(schema_service_->get_batch_##SCHEMA##s(                                       \
                   schema_status, sql_client, schema_version, schema_keys, simple_schemas))) {       \
      LOG_WARN("get batch " #SCHEMA "s failed", K(ret), K(schema_keys));                             \
    } else {                                                                                         \
      ALLOW_NEXT_LOG();                                                                              \
      LOG_INFO("get batch " #SCHEMA "s success", K(schema_keys));                                    \
      if (schema_keys.size() != simple_schemas.size()) {                                             \
        ret = OB_ERR_UNEXPECTED;                                                                     \
        LOG_ERROR("unexpected result cnt", K(ret), K(schema_keys.size()), K(simple_schemas.size())); \
      }                                                                                              \
    }                                                                                                \
  }

#define GET_BATCH_SCHEMAS_WITHOUT_SCHEMA_STATUS(SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS)                                      \
  if (OB_SUCC(ret)) {                                                                                                  \
    schema_keys.reset();                                                                                               \
    const SCHEMA_KEYS& new_keys = all_keys.new_##SCHEMA##_keys_;                                                       \
    ObArray<SCHEMA_TYPE>& simple_schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;                            \
    if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(SCHEMA_KEYS, new_keys, schema_keys))) {                                   \
      LOG_WARN("convert set to array failed", K(ret));                                                                 \
    } else if (OB_FAIL(                                                                                                \
                   schema_service_->get_batch_##SCHEMA##s(sql_client, schema_version, schema_keys, simple_schemas))) { \
      LOG_WARN("get batch " #SCHEMA "s failed", K(ret), K(schema_keys));                                               \
    } else {                                                                                                           \
      ALLOW_NEXT_LOG();                                                                                                \
      LOG_INFO("get batch " #SCHEMA "s success", K(schema_keys));                                                      \
      if (schema_keys.size() != simple_schemas.size()) {                                                               \
        ret = OB_ERR_UNEXPECTED;                                                                                       \
        LOG_ERROR(                                                                                                     \
            "unexpected result cnt", K(ret), K(schema_version), K(schema_keys.size()), K(simple_schemas.size()));      \
      }                                                                                                                \
    }                                                                                                                  \
  }

  GET_BATCH_SCHEMAS(user, ObSimpleUserSchema, UserKeys);
  GET_BATCH_SCHEMAS(database, ObSimpleDatabaseSchema, DatabaseKeys);
  GET_BATCH_SCHEMAS(tablegroup, ObSimpleTablegroupSchema, TablegroupKeys);
  GET_BATCH_SCHEMAS(table, ObSimpleTableSchemaV2, TableKeys);
  GET_BATCH_SCHEMAS(outline, ObSimpleOutlineSchema, OutlineKeys);
  GET_BATCH_SCHEMAS(db_priv, ObDBPriv, DBPrivKeys);
  GET_BATCH_SCHEMAS(table_priv, ObTablePriv, TablePrivKeys);
  GET_BATCH_SCHEMAS(synonym, ObSimpleSynonymSchema, SynonymKeys);
  GET_BATCH_SCHEMAS(udf, ObSimpleUDFSchema, UdfKeys);
  GET_BATCH_SCHEMAS(sequence, ObSequenceSchema, SequenceKeys);
  GET_BATCH_SCHEMAS(profile, ObProfileSchema, ProfileKeys);
  GET_BATCH_SCHEMAS(sys_priv, ObSysPriv, SysPrivKeys);
  GET_BATCH_SCHEMAS(obj_priv, ObObjPriv, ObjPrivKeys);

  // After the schema is split, because the operation_type has not been updated,
  // the OB_DDL_TENANT_OPERATION is still reused
  // For system tenants, there is need to filter the SysVariableKeys of non-system tenants.
  // This is implemented in replay_log/replay_log_reversely
  // It is believed that SysVariableKeys has been filtered
  GET_BATCH_SCHEMAS(sys_variable, ObSimpleSysVariableSchema, SysVariableKeys);
  GET_BATCH_SCHEMAS(dblink, ObDbLinkSchema, DbLinkKeys);

  // After the schema is split, ordinary tenants do not refresh the tenant schema and system table schema
  const uint64_t tenant_id = schema_status.tenant_id_;
  if (OB_FAIL(ret)) {
    // skip
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {

    GET_BATCH_SCHEMAS_WITHOUT_SCHEMA_STATUS(tenant, ObSimpleTenantSchema, TenantKeys);

    if (OB_SUCC(ret)) {
      schema_keys.reset();
      if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(TenantKeys, all_keys.alter_tenant_keys_, schema_keys))) {
        LOG_WARN("convert set to array failed", K(ret));
      } else if (OB_FAIL(schema_service_->get_batch_tenants(
                     sql_client, schema_version, schema_keys, simple_incre_schemas.alter_tenant_schemas_))) {
        LOG_WARN("get batch tenants failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(renew_core_and_sys_table_schemas(sql_client,
              simple_incre_schemas.allocator_,
              simple_incre_schemas.core_tables_,
              simple_incre_schemas.sys_tables_,
              &all_keys.core_table_ids_,
              &all_keys.sys_table_ids_))) {
        LOG_WARN("renew core and sys table schemas failed", K(ret));
      }
    }
  }

#undef GET_BATCH_SCHEMAS
  return ret;
}

int ObServerSchemaService::apply_increment_schema_to_cache(
    const AllSchemaKeys& all_keys, const AllSimpleIncrementSchema& simple_incre_schemas, ObSchemaMgr& schema_mgr)
{
  int ret = OB_SUCCESS;
  ObArray<SchemaKey> schema_keys;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(TenantKeys, all_keys.del_tenant_keys_, schema_keys))) {
      LOG_WARN("convert set to array failed", K(ret));
    } else {
      FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret))
      {
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
      LOG_WARN("invalid drop tenant keys count", K(ret), K(del_drop_tenant_keys_count), K(add_drop_tenant_keys_count));
    } else if (del_drop_tenant_keys_count > 0) {
      if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(TenantKeys, all_keys.del_drop_tenant_keys_, schema_keys))) {
        LOG_WARN("convert set to array failed", K(ret));
      } else {
        FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret))
        {
          if (OB_FAIL(schema_mgr.del_drop_tenant_info(schema_key->tenant_id_))) {
            LOG_WARN("fail to del drop tenant info", K(ret), K(*schema_key));
          }
        }
      }
    } else if (add_drop_tenant_keys_count > 0) {
      if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(TenantKeys, all_keys.add_drop_tenant_keys_, schema_keys))) {
        LOG_WARN("convert set to array failed", K(ret));
      } else {
        FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret))
        {
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
    LOG_INFO("add tenants finish", "schemas", simple_incre_schemas.alter_tenant_schemas_, K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_mgr.add_tenants(simple_incre_schemas.simple_tenant_schemas_))) {
      LOG_WARN("add tenants failed", K(ret));
    }
    ALLOW_NEXT_LOG();
    LOG_INFO("add tenants finish", "schemas", simple_incre_schemas.simple_tenant_schemas_, K(ret));
  }

#define UPDATE_MGR(tenant_id, mgr, SCHEMA, SCHEMA_TYPE, SCHEMA_KEYS)                                      \
  if (OB_SUCC(ret)) {                                                                                     \
    const SCHEMA_KEYS& del_keys = all_keys.del_##SCHEMA##_keys_;                                          \
    if (OB_FAIL(CONVERT_SCHEMA_KEYS_TO_ARRAY(SCHEMA_KEYS, del_keys, schema_keys))) {                      \
      LOG_WARN("convert set to array failed", K(ret));                                                    \
    } else {                                                                                              \
      FOREACH_CNT_X(schema_key, schema_keys, OB_SUCC(ret))                                                \
      {                                                                                                   \
        if (OB_FAIL(mgr.del_##SCHEMA(schema_key->get_##SCHEMA##_key()))) {                                \
          if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID == tenant_id && OB_ENTRY_NOT_EXIST == ret) {  \
            ret = OB_SUCCESS;                                                                             \
          } else {                                                                                        \
            LOG_WARN("del " #SCHEMA " failed", K(ret), #SCHEMA "_key", schema_key->get_##SCHEMA##_key()); \
          }                                                                                               \
        }                                                                                                 \
      }                                                                                                   \
      ALLOW_NEXT_LOG();                                                                                   \
      LOG_INFO("del " #SCHEMA "s finish", K(schema_keys), K(ret));                                        \
    }                                                                                                     \
    if (OB_SUCC(ret)) {                                                                                   \
      const ObArray<SCHEMA_TYPE>& schemas = simple_incre_schemas.simple_##SCHEMA##_schemas_;              \
      if (OB_FAIL(mgr.add_##SCHEMA##s(schemas))) {                                                        \
        LOG_WARN("add " #SCHEMA "s failed", K(ret), #SCHEMA " schemas", schemas);                         \
      }                                                                                                   \
      ALLOW_NEXT_LOG();                                                                                   \
      LOG_INFO("add " #SCHEMA "s finish", K(schemas), K(ret));                                            \
    }                                                                                                     \
  }

  const uint64_t tenant_id = schema_mgr.get_tenant_id();
  // Need to ensure that the system variables are added first
  UPDATE_MGR(tenant_id, schema_mgr.sys_variable_mgr_, sys_variable, ObSimpleSysVariableSchema, SysVariableKeys);
  UPDATE_MGR(tenant_id, schema_mgr, user, ObSimpleUserSchema, UserKeys);
  UPDATE_MGR(tenant_id, schema_mgr, database, ObSimpleDatabaseSchema, DatabaseKeys);
  UPDATE_MGR(tenant_id, schema_mgr, tablegroup, ObSimpleTablegroupSchema, TablegroupKeys);
  UPDATE_MGR(tenant_id, schema_mgr, table, ObSimpleTableSchemaV2, TableKeys);
  UPDATE_MGR(tenant_id, schema_mgr.outline_mgr_, outline, ObSimpleOutlineSchema, OutlineKeys);
  UPDATE_MGR(tenant_id, schema_mgr.priv_mgr_, db_priv, ObDBPriv, DBPrivKeys);
  UPDATE_MGR(tenant_id, schema_mgr.priv_mgr_, table_priv, ObTablePriv, TablePrivKeys);
  UPDATE_MGR(tenant_id, schema_mgr.synonym_mgr_, synonym, ObSimpleSynonymSchema, SynonymKeys);
  UPDATE_MGR(tenant_id, schema_mgr.udf_mgr_, udf, ObSimpleUDFSchema, UdfKeys);
  UPDATE_MGR(tenant_id, schema_mgr.sequence_mgr_, sequence, ObSequenceSchema, SequenceKeys);
  UPDATE_MGR(tenant_id, schema_mgr.profile_mgr_, profile, ObProfileSchema, ProfileKeys);
  UPDATE_MGR(tenant_id, schema_mgr.priv_mgr_, sys_priv, ObSysPriv, SysPrivKeys);
  UPDATE_MGR(tenant_id, schema_mgr.priv_mgr_, obj_priv, ObObjPriv, ObjPrivKeys);
  UPDATE_MGR(tenant_id, schema_mgr.dblink_mgr_, dblink, ObDbLinkSchema, DbLinkKeys);
#undef UPDATE_MGR
  return ret;
}

int ObServerSchemaService::update_schema_mgr(
    ObISQLClient& sql_client, ObSchemaMgr& schema_mgr, const int64_t schema_version, AllSchemaKeys& all_keys)
{
  int ret = OB_SUCCESS;
  const ObRefreshSchemaStatus dummy_schema_status;  // for compatible

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    // note: Adjust the position of this code carefully
    // add sys tenant schema to cache
    if (OB_FAIL(add_tenant_schemas_to_cache(all_keys.alter_tenant_keys_, sql_client))) {
      LOG_WARN("add alter tenant schemas to cache failed", K(ret));
    } else if (OB_FAIL(add_tenant_schemas_to_cache(all_keys.new_tenant_keys_, sql_client))) {
      LOG_WARN("add new tenant schemas to cache failed", K(ret));
    } else if (OB_FAIL(add_sys_variable_schemas_to_cache(all_keys.new_sys_variable_keys_, sql_client))) {
      LOG_WARN("add new sys variable schemas to cache failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      AllSimpleIncrementSchema simple_incre_schemas;
      if (OB_FAIL(fetch_increment_schemas(
              dummy_schema_status, all_keys, schema_version, sql_client, simple_incre_schemas))) {
        LOG_WARN("fetch increment schemas failed", K(ret));
      } else if (OB_FAIL(apply_increment_schema_to_cache(all_keys, simple_incre_schemas, schema_mgr))) {
        LOG_WARN("apply increment schema to cache failed", K(ret));
      }

      // process tenant space table
      if (OB_SUCC(ret)) {
        ObArray<uint64_t> new_tenant_ids;
        ObArray<uint64_t> del_table_ids;
        ObArray<uint64_t> new_table_ids;
        if (OB_FAIL(EXTRACT_IDS_FROM_KEYS(TenantKeys, tenant, all_keys.new_tenant_keys_, new_tenant_ids))) {
          LOG_WARN("convert set to array failed", K(ret));
        } else if (OB_FAIL(EXTRACT_IDS_FROM_KEYS(TableKeys, table, all_keys.del_table_keys_, del_table_ids))) {
          LOG_WARN("convert set to array failed", K(ret));
        } else if (OB_FAIL(EXTRACT_IDS_FROM_KEYS(TableKeys, table, all_keys.new_table_keys_, new_table_ids))) {
          LOG_WARN("convert set to array failed", K(ret));
        } else if (OB_FAIL(process_tenant_space_table(schema_mgr, new_tenant_ids, &del_table_ids, &new_table_ids))) {
          LOG_WARN("process tenant space table failed", K(ret));
        }
      }

      // The index information of the core table system table is only refreshed in the third stage,
      // where the index table schema of the first two stages of publish in the cache needs to be updated.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(update_core_and_sys_schemas_in_cache(
                *schema_mgr_for_cache_, simple_incre_schemas.core_tables_, simple_incre_schemas.sys_tables_))) {
          LOG_WARN("update core and sys schemas in cache faield", K(ret));
        }
      }
    }
  }

  // check shema consistent at last
  if (FAILEDx(schema_mgr.rebuild_schema_meta_if_not_consistent())) {
    LOG_ERROR("not consistency for schema meta data", KR(ret));
  }

  return ret;
}

int ObServerSchemaService::update_schema_mgr(ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status,
    ObSchemaMgr& schema_mgr, const int64_t schema_version, AllSchemaKeys& all_keys)
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
    // note: Adjust the position of this code carefully
    // add sys tenant schema to cache
    if (OB_SYS_TENANT_ID != tenant_id) {
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
        FOREACH_X(it, all_keys.new_tenant_keys_, OB_SUCC(ret))
        {
          SchemaKey key = it->first;
          if (OB_SYS_TENANT_ID == key.tenant_id_) {
            continue;
          } else if (init_schema_struct(key.tenant_id_)) {
            LOG_WARN("fail to init schema struct", K(ret), "tenant_id", key.tenant_id_);
          } else if (OB_FAIL(init_tenant_basic_schema(key.tenant_id_))) {
            LOG_WARN("fail to init basic schema struct", K(ret), "tenant_id", key.tenant_id_, K(schema_status));
          } else if (OB_FAIL(init_multi_version_schema_struct(key.tenant_id_))) {
            LOG_WARN("fail to init multi version schema struct", K(ret), "tenant_id", key.tenant_id_);
          } else {
            LOG_INFO("[REFRESH_SCHEMA] init tenant schema struct", "tenant_id", key.tenant_id_);
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
      } else if (OB_SYS_TENANT_ID == tenant_id) {
        // process tenant space table(sys tenant only)
        ObArray<uint64_t> new_tenant_ids;
        ObArray<uint64_t> del_table_ids;
        ObArray<uint64_t> new_table_ids;
        if (OB_FAIL(EXTRACT_IDS_FROM_KEYS(TenantKeys, tenant, all_keys.new_tenant_keys_, new_tenant_ids))) {
          LOG_WARN("convert set to array failed", K(ret));
        } else if (OB_FAIL(EXTRACT_IDS_FROM_KEYS(TableKeys, table, all_keys.del_table_keys_, del_table_ids))) {
          LOG_WARN("convert set to array failed", K(ret));
        } else if (OB_FAIL(EXTRACT_IDS_FROM_KEYS(TableKeys, table, all_keys.new_table_keys_, new_table_ids))) {
          LOG_WARN("convert set to array failed", K(ret));
        } else if (OB_FAIL(process_tenant_space_table(schema_mgr, new_tenant_ids, &del_table_ids, &new_table_ids))) {
          LOG_WARN("process tenant space table failed", K(ret));
        }
      }

      // The index information of the core table system table is only refreshed in the third stage,
      // where the index table schema of the first two stages of publish in the cache needs to be updated.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(update_core_and_sys_schemas_in_cache(
                schema_mgr, simple_incre_schemas.core_tables_, simple_incre_schemas.sys_tables_))) {
          LOG_WARN("update core and sys schemas in cache faield", K(ret));
        }
      }
    }
  }

  // check shema consistent at last
  if (FAILEDx(schema_mgr.rebuild_schema_meta_if_not_consistent())) {
    LOG_ERROR("not consistency for schema meta data", KR(ret));
  }

  return ret;
}

int ObServerSchemaService::update_baseline_schema_version(const int64_t baseline_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t bl_schema_version = ATOMIC_LOAD(&baseline_schema_version_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (baseline_schema_version < bl_schema_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(baseline_schema_version), K(bl_schema_version));
  } else {
    ATOMIC_STORE(&baseline_schema_version_, baseline_schema_version);
  }
  return ret;
}

int ObServerSchemaService::get_baseline_schema_version(int64_t& baseline_schema_version)
{
  int ret = OB_SUCCESS;
  baseline_schema_version = -1;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    int64_t bl_schema_version = ATOMIC_LOAD(&baseline_schema_version_);
    if (-1 == bl_schema_version) {
      ObISQLClient& sql_client = *sql_proxy_;
      const int64_t frozen_version = -1;
      if (OB_FAIL(schema_service_->get_baseline_schema_version(sql_client, frozen_version, bl_schema_version))) {
        LOG_WARN("get baseline schema version failed", K(ret));
      } else if (bl_schema_version < -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected baseline schema version", K(ret), K(bl_schema_version));
      } else {
        ATOMIC_STORE(&baseline_schema_version_, bl_schema_version);
      }
      LOG_INFO("fetch baseline schema version finish", K(ret), K(bl_schema_version), K(lbt()));
    }
    if (OB_SUCC(ret)) {
      baseline_schema_version = bl_schema_version;
    }
  }

  return ret;
}

// wrapper for add index and materialized view
int ObServerSchemaService::add_index_tids(const ObSchemaMgr& schema_mgr, ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table.get_table_id();
  ObArray<const ObSimpleTableSchemaV2*> simple_indexes;
  if (OB_FAIL(schema_mgr.get_aux_schemas(table_id, simple_indexes, USER_INDEX))) {
    LOG_WARN("get index schemas failed", K(ret));
  } else {
    FOREACH_CNT_X(tmp_simple_index, simple_indexes, OB_SUCC(ret))
    {
      const ObSimpleTableSchemaV2* simple_index = *tmp_simple_index;
      if (OB_ISNULL(simple_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(simple_index), K(ret));
      } else {
        if (simple_index->is_index_table() || simple_index->is_materialized_view()) {
          if (OB_FAIL(table.add_simple_index_info(ObAuxTableMetaInfo(simple_index->get_table_id(),
                  simple_index->get_table_type(),
                  simple_index->get_drop_schema_version())))) {
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

int ObServerSchemaService::extract_core_and_sys_table_ids(
    const TableKeys& keys, ObIArray<uint64_t>& core_table_ids, ObIArray<uint64_t>& sys_table_ids)
{
  int ret = OB_SUCCESS;
  for (TableKeys::const_iterator it = keys.begin(); OB_SUCC(ret) && it != keys.end(); ++it) {
    const uint64_t table_id = (it->first).table_id_;
    if (is_core_table(table_id)) {
      if (OB_FAIL(core_table_ids.push_back(table_id))) {
        LOG_WARN("push_back failed", K(ret), K(table_id));
      }
    }
    if (OB_SUCC(ret)) {
      // The core table is not recorded in __all_table, get_sys_table_schema will be taken from __all_table
      // So here to filter out the core table
      if (!is_core_table(table_id) && is_sys_table(table_id)) {
        if (OB_FAIL(sys_table_ids.push_back(table_id))) {
          LOG_WARN("push_back failed", K(ret), K(table_id));
        }
      }
    }
  }
  return ret;
}

int ObServerSchemaService::update_core_and_sys_schemas_in_cache(
    const ObSchemaMgr& schema_mgr, ObIArray<ObTableSchema>& core_tables, ObIArray<ObTableSchema*>& sys_tables)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(core_table, core_tables, OB_SUCC(ret))
    {
      if (OB_ISNULL(core_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(core_table), K(ret));
      } else if (OB_FAIL(add_index_tids(schema_mgr, *core_table))) {
        LOG_WARN("get core table schemas failed", K(ret));
      }
    }
  }
  // update schema cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_schema_cache(core_tables, true))) {
      LOG_WARN("failed to update schema cache", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(sys_table, sys_tables, OB_SUCC(ret))
    {
      if (OB_ISNULL(sys_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(sys_table), K(ret));
      } else if (OB_ISNULL(*sys_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(*sys_table), K(ret));
      } else if (OB_FAIL(add_index_tids(schema_mgr, **sys_table))) {
        LOG_WARN("get sys table schemas failed", K(ret));
      }
    }
  }
  // update schema cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_schema_cache(sys_tables, true))) {
      LOG_WARN("failed to update schema cache", K(ret));
    }
  }
  return ret;
}

// Find the index id of the core table/system table from the schema_mgr,
// and update the cache after adding to the schema
// When core_table_ids==NULL, all core tables are processed, otherwise only the tables specified by
// core_table_ids are processed, and sys_table_ids is the same
int ObServerSchemaService::renew_core_and_sys_table_schemas(ObISQLClient& sql_client, ObIAllocator& allocator,
    ObArray<ObTableSchema>& core_tables, ObArray<ObTableSchema*>& sys_tables,
    const ObIArray<uint64_t>* core_table_ids /* = NULL*/, const ObIArray<uint64_t>* sys_table_ids /* = NULL*/)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    bool do_renew_core = NULL == core_table_ids || core_table_ids->count() > 0;
    bool do_renew_sys = NULL == sys_table_ids || sys_table_ids->count() > 0;
    if (do_renew_core) {
      ObArray<ObTableSchema>* dst_core_tables = NULL;
      ObArray<ObTableSchema> tmp_core_tables;
      if (NULL != core_table_ids) {
        dst_core_tables = &tmp_core_tables;
      } else {
        dst_core_tables = &core_tables;
      }
      if (OB_FAIL(schema_service_->get_core_table_schemas(sql_client, *dst_core_tables))) {
        LOG_WARN("get core table schemas failed", K(ret));
      } else if (NULL != core_table_ids) {
        FOREACH_CNT_X(table, *dst_core_tables, OB_SUCC(ret))
        {
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(table), K(ret));
          }
          FOREACH_CNT_X(table_id, *core_table_ids, OB_SUCC(ret))
          {
            if (OB_ISNULL(table_id)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(table_id), K(ret));
            } else if (*table_id == table->get_table_id()) {
              if (OB_FAIL(core_tables.push_back(*table))) {
                LOG_WARN("push back failed", K(ret));
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && do_renew_sys) {
      ObArray<uint64_t> all_sys_table_ids;
      const ObIArray<uint64_t>* dst_sys_table_ids = NULL == sys_table_ids ? &all_sys_table_ids : sys_table_ids;
      if (NULL == sys_table_ids) {
        if (OB_FAIL(get_table_ids(sys_table_schema_creators, all_sys_table_ids))) {
          LOG_WARN("get_table_ids failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_service_->get_sys_table_schemas(*dst_sys_table_ids, sql_client, allocator, sys_tables))) {
          LOG_WARN("get sys table schemas failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::process_tenant_space_table(ObSchemaMgr& schema_mgr, const ObIArray<uint64_t>& new_tenant_ids,
    const ObIArray<uint64_t>* del_table_ids /* = NULL*/, const ObIArray<uint64_t>* new_table_ids /* = NULL*/)
{
  int ret = OB_SUCCESS;
  bool new_mode = (OB_INVALID_TENANT_ID != schema_mgr.get_tenant_id());

  // add tenant space tables alread exist to new tenants
  FOREACH_CNT_X(tmp_new_tenant_id, new_tenant_ids, OB_SUCC(ret))
  {
    const uint64_t tenant_id = *tmp_new_tenant_id;
    const ObSimpleTableSchemaV2* simple_table = NULL;
    ObSimpleTableSchemaV2 tmp_simple_table;

    bool is_skip = false;
    bool is_oracle_mode = false;
    ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant compat mode", K(ret), K(tenant_id));
    } else {
      is_oracle_mode = ObWorker::CompatMode::ORACLE == compat_mode;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tenant_space_tables); ++i) {
      if (OB_FAIL(schema_mgr.get_table_schema(combine_id(OB_SYS_TENANT_ID, tenant_space_tables[i]), simple_table))) {
        LOG_WARN("get simple table schema failed", "pure table id", tenant_space_tables[i], K(ret));
      } else if (NULL == simple_table) {
        LOG_INFO("current tenant space table has beed removed from sys tenant, ignore it!",
            "pure table id",
            tenant_space_tables[i]);
      } else {
        const uint64_t pure_database_id = extract_pure_id(simple_table->get_database_id());
        const uint64_t table_id = simple_table->get_table_id();
        if (is_oracle_mode) {
          // In MySQL tenant mode, there is no need to add Oracle-related internal tables
          is_skip = ((OB_INFORMATION_SCHEMA_ID == pure_database_id) || (OB_MYSQL_SCHEMA_ID == pure_database_id));
        } else {
          // In the Oracle tenant mode, there is no need to add MySQL related internal tables,
          // except for the SYS tenant
          is_skip = ((OB_ORA_SYS_DATABASE_ID == pure_database_id) && OB_SYS_TENANT_ID != tenant_id);
        }

        // 2.2 During the upgrade process, avoid stop zone failure
        if (OB_SYS_TENANT_ID != tenant_id && ObSysTableChecker::is_tenant_table_in_version_2200(table_id) &&
            !new_mode) {
          is_skip = true;
        }

        if (!is_skip) {
          ObArray<ObString> empty_zones;
          if (OB_FAIL(tmp_simple_table.assign(*simple_table))) {
            LOG_WARN("fail to assign schema", K(ret));
          } else {
            tmp_simple_table.set_tenant_id(tenant_id);
            tmp_simple_table.set_table_id(combine_id(tenant_id, tmp_simple_table.get_table_id()));
            tmp_simple_table.set_database_id(combine_id(tenant_id, tmp_simple_table.get_database_id()));
            tmp_simple_table.set_locality(ObString());
            tmp_simple_table.set_previous_locality(ObString());
            tmp_simple_table.set_zone_list(empty_zones);
          }
          if (OB_SUCC(ret)) {
            if (tmp_simple_table.has_partition()) {
              // reset partition option
              tmp_simple_table.reset_partition_schema();
              tmp_simple_table.get_part_option().set_max_used_part_id(0);
              tmp_simple_table.get_part_option().set_partition_cnt_within_partition_table(0);
            }
            if (tmp_simple_table.get_tablegroup_id() != OB_INVALID_ID) {
              tmp_simple_table.set_tablegroup_id(combine_id(tenant_id, tmp_simple_table.get_tablegroup_id()));
            }
            if (tmp_simple_table.get_data_table_id() > 0) {
              tmp_simple_table.set_data_table_id(combine_id(tenant_id, tmp_simple_table.get_data_table_id()));
            }
            if (OB_FAIL(schema_mgr.add_table(tmp_simple_table))) {
              LOG_WARN("add table failed", K(ret), "simple table schema", tmp_simple_table);
            } else {
              LOG_INFO("add tenant space table finish",
                  K(ret),
                  K(tenant_id),
                  K(tmp_simple_table),
                  "schema_mgr_tenant_id",
                  schema_mgr.get_tenant_id());
            }
          }
        } else {
          LOG_INFO("skip tenant space table ", K(ret), K(simple_table->get_table_id()), K(tenant_id));
        }
      }
      simple_table = NULL;
      tmp_simple_table.reset();
    }
  }

  // patch same transformers of tenant space tables to old tenants
  ObArray<uint64_t> del_space_ids;
  ObArray<uint64_t> new_space_ids;
  if (OB_SUCC(ret)) {
    if (NULL != del_table_ids) {
      FOREACH_CNT_X(tmp_del_table_id, *del_table_ids, OB_SUCC(ret))
      {
        const uint64_t table_id = *tmp_del_table_id;
        bool is_tenant_table = false;
        if (!UNDER_SYS_TENANT(table_id)) {
          // do-nothing
        } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, is_tenant_table))) {
          LOG_WARN("fail to check if table_id in tenant space", K(ret), K(table_id));
        } else if (!is_tenant_table) {
          // do-nothing
        } else if ((OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(table_id) ||
                       OB_ALL_SEQUENCE_V2_TID == extract_pure_id(table_id) ||
                       OB_ALL_TENANT_GC_PARTITION_INFO_TID == extract_pure_id(table_id)) &&
                   !new_mode) {
          // do-nothing
        } else if (OB_ALL_SEQUENCE_VALUE_TID == extract_pure_id(table_id) &&
                   GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100 && !new_mode) {
          // do-nothing
        } else if ((OB_ALL_WEAK_READ_SERVICE_TID == extract_pure_id(table_id) ||
                       OB_TENANT_PARAMETER_TID == extract_pure_id(table_id) ||
                       ObSysTableChecker::is_tenant_table_in_version_2200(table_id)) &&
                   !new_mode) {
          // do-nothing
        } else if (OB_FAIL(del_space_ids.push_back(table_id))) {
          LOG_WARN("push back table id failed", K(ret), K(table_id));
        }
      }
    }
    if (NULL != new_table_ids) {
      FOREACH_CNT_X(tmp_new_table_id, *new_table_ids, OB_SUCC(ret))
      {
        const uint64_t table_id = *tmp_new_table_id;
        bool is_tenant_table = false;
        if (!UNDER_SYS_TENANT(table_id)) {
          // do-nothing
        } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(table_id, is_tenant_table))) {
          LOG_WARN("fail to check if table_id in tenant space", K(ret), K(table_id));
        } else if (!is_tenant_table) {
          // do-nothing
        } else if ((OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(table_id) ||
                       OB_ALL_SEQUENCE_V2_TID == extract_pure_id(table_id) ||
                       OB_ALL_TENANT_GC_PARTITION_INFO_TID == extract_pure_id(table_id)) &&
                   GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000 && !new_mode) {
          // do-nothing
        } else if (OB_ALL_SEQUENCE_VALUE_TID == extract_pure_id(table_id) &&
                   GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100 && !new_mode) {
          // do-nothing
        } else if ((OB_ALL_WEAK_READ_SERVICE_TID == extract_pure_id(table_id) ||
                       OB_TENANT_PARAMETER_TID == extract_pure_id(table_id) ||
                       ObSysTableChecker::is_tenant_table_in_version_2200(table_id)) &&
                   !new_mode) {
          // do-nothing
        } else if (OB_FAIL(new_space_ids.push_back(table_id))) {
          LOG_WARN("push back table id failed", K(ret), K(table_id));
        }
      }
    }
  }

  ObArray<uint64_t> dst_tenant_ids;
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(schema_mgr.get_tenant_ids(tenant_ids))) {
      LOG_WARN("get tenant ids failed", K(ret));
    } else {
      FOREACH_CNT_X(tmp_tenant_id, tenant_ids, OB_SUCC(ret))
      {
        if (OB_SYS_TENANT_ID == *tmp_tenant_id) {
          // do-nothing
        } else {
          bool exist = false;
          FOREACH_CNT_X(tmp_new_tenant_id, new_tenant_ids, OB_SUCC(ret) && !exist)
          {
            if (*tmp_new_tenant_id == *tmp_tenant_id) {
              exist = true;
            }
          }
          if (!exist) {
            if (OB_FAIL(dst_tenant_ids.push_back(*tmp_tenant_id))) {
              LOG_WARN("push back tenant id failed", K(ret), "tenant id", *tmp_tenant_id);
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(tmp_del_space_id, del_space_ids, OB_SUCC(ret))
    {
      const uint64_t table_id = *tmp_del_space_id;
      FOREACH_CNT_X(tmp_tenant_id, dst_tenant_ids, OB_SUCC(ret))
      {
        const uint64_t tenant_id = *tmp_tenant_id;
        ObTenantTableId tenant_table_id(tenant_id, combine_id(tenant_id, table_id));
        const ObSimpleTableSchemaV2* simple_table = NULL;
        if (OB_FAIL(schema_mgr.get_table_schema(combine_id(tenant_id, table_id), simple_table))) {
          LOG_WARN("get simple table schema failed", K(ret), K(table_id));
        } else if (NULL == simple_table) {
          // do nothing since this table doesn't exist
        } else if (OB_FAIL(schema_mgr.del_table(tenant_table_id))) {
          LOG_WARN("del table failed",
              K(ret),
              "tenant_id",
              tenant_table_id.tenant_id_,
              "table_id",
              tenant_table_id.table_id_);
        }
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (OB_ALL_SEQUENCE_V2_TID == extract_pure_id(table_id) ||
              OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(table_id) ||
              OB_ALL_TENANT_GC_PARTITION_INFO_TID == extract_pure_id(table_id) ||
              OB_ALL_SEQUENCE_VALUE_TID == extract_pure_id(table_id) ||
              OB_ALL_WEAK_READ_SERVICE_TID == extract_pure_id(table_id) ||
              OB_TENANT_PARAMETER_TID == extract_pure_id(table_id) ||
              ObSysTableChecker::is_tenant_table_in_version_2200(table_id)) {
            LOG_INFO("tenant space table not exist is expected, ignore it", K(ret), K(table_id));
            ret = OB_SUCCESS;
          }
        }
        LOG_INFO("delete tenant space table finish", K(table_id), K(tenant_id));
      }
    }

    FOREACH_CNT_X(tmp_new_space_id, new_space_ids, OB_SUCC(ret))
    {
      const uint64_t table_id = *tmp_new_space_id;
      const ObSimpleTableSchemaV2* simple_table = NULL;
      if (OB_FAIL(schema_mgr.get_table_schema(table_id, simple_table))) {
        LOG_WARN("get simple table schema failed", K(ret), K(table_id));
      } else if (OB_ISNULL(simple_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(simple_table));
      } else {
        FOREACH_CNT_X(tmp_tenant_id, dst_tenant_ids, OB_SUCC(ret))
        {
          const uint64_t tenant_id = *tmp_tenant_id;
          bool is_skip = false;
          bool is_oracle_mode = false;
          ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
          if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
            LOG_WARN("fail to get tenant compat mode", K(ret), K(tenant_id));
          } else {
            is_oracle_mode = ObWorker::CompatMode::ORACLE == compat_mode;
            const uint64_t pure_database_id = extract_pure_id(simple_table->get_database_id());
            if (is_oracle_mode) {
              // In MySQL tenant mode, there is no need to add Oracle-related internal tables
              is_skip = ((OB_INFORMATION_SCHEMA_ID == pure_database_id) || (OB_MYSQL_SCHEMA_ID == pure_database_id));
            } else {
              // In the Oracle tenant mode, there is no need to add MySQL related internal tables,
              // except for the SYS tenant
              is_skip = ((OB_ORA_SYS_DATABASE_ID == pure_database_id) && OB_SYS_TENANT_ID != tenant_id);
            }
            if (!is_skip) {
              ObSimpleTableSchemaV2 tmp_simple;
              if (OB_FAIL(tmp_simple.assign(*simple_table))) {
                LOG_WARN("fail to assign schema", K(ret));
              } else {
                const uint64_t new_table_id = combine_id(tenant_id, tmp_simple.get_table_id());
                tmp_simple.set_tenant_id(tenant_id);
                tmp_simple.set_table_id(new_table_id);
                tmp_simple.set_database_id(combine_id(tenant_id, tmp_simple.get_database_id()));
                tmp_simple.set_locality(ObString());
                tmp_simple.set_previous_locality(ObString());
                ObArray<ObString> empty_zones;
                tmp_simple.set_zone_list(empty_zones);
                if (tmp_simple.has_partition()) {
                  // reset partition option
                  tmp_simple.reset_partition_schema();
                  tmp_simple.get_part_option().set_max_used_part_id(0);
                  tmp_simple.get_part_option().set_partition_cnt_within_partition_table(0);
                }
                if (tmp_simple.get_tablegroup_id() != OB_INVALID_ID) {
                  tmp_simple.set_tablegroup_id(combine_id(tenant_id, tmp_simple.get_tablegroup_id()));
                }
                if (tmp_simple.get_data_table_id() > 0) {
                  tmp_simple.set_data_table_id(combine_id(tenant_id, tmp_simple.get_data_table_id()));
                }
                if (OB_FAIL(schema_mgr.add_table(tmp_simple))) {
                  LOG_WARN("add table failed", K(ret), "simple table schema", tmp_simple);
                } else {
                  LOG_INFO("add tenant space table finish",
                      K(ret),
                      K(tenant_id),
                      K(new_table_id),
                      "schema_mgr_tenant_id",
                      schema_mgr.get_tenant_id());
                }
              }
            } else {
              LOG_INFO("skip tenant space table ", K(ret), K(table_id), K(tenant_id));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::fallback_schema_mgr(
    const ObRefreshSchemaStatus& schema_status, ObSchemaMgr& schema_mgr, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_version));
  } else if (schema_status.snapshot_timestamp_ > 0 && schema_status.readable_schema_version_ < schema_version) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("schema_version is not readable now", K(ret), K(schema_status), K(schema_version));
  } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
    LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
  } else {
    ObISQLClient& sql_client = *sql_proxy_;
    AllSchemaKeys all_keys;
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
        LOG_WARN("get_increment_schema_operations failed", K(start_ver), K(end_ver), K(ret));
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
            if (OB_FAIL(update_schema_mgr(sql_client, schema_mgr, schema_version, all_keys))) {
              LOG_WARN("update schema mgr failed", K(ret));
            }
          } else {
            if (OB_FAIL(update_schema_mgr(sql_client, schema_status, schema_mgr, schema_version, all_keys))) {
              LOG_WARN("update schema mgr failed", K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    schema_mgr.set_schema_version(schema_version);
  }

  return ret;
}

int ObServerSchemaService::replay_log(const ObSchemaMgr& schema_mgr,
    const ObSchemaService::SchemaOperationSetWithAlloc& schema_operations, AllSchemaKeys& schema_keys)
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
        const ObSchemaOperation& schema_operation = schema_operations.at(i);
        LOG_INFO("schema operation", K(schema_operation));
        if (schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN &&
            schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END) {
          if (OB_FAIL(get_increment_sys_variable_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sys variable keys", K(ret));
          } else if (OB_FAIL(get_increment_tenant_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment tenant id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END) {
          if (OB_FAIL(get_increment_sys_variable_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sys variable keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_USER_OPERATION_END) {
          if (OB_FAIL(get_increment_user_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment user id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END) {
          if (OB_FAIL(get_increment_database_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment database id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END) {
          if (OB_FAIL(get_increment_tablegroup_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment tablegroup id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END) {
          if (OB_FAIL(get_increment_table_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment table id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END) {
          if (OB_FAIL(get_increment_synonym_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment synonym id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END) {
          if (OB_FAIL(get_increment_outline_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment outline id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_db_priv_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment db priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_table_priv_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment table priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END) {
          if (OB_FAIL(get_increment_udf_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment udf id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END) {
          if (OB_FAIL(get_increment_sequence_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sequence id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END) {
          if (OB_FAIL(get_increment_profile_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment procedure id", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_sys_priv_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment sys priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END) {
          if (OB_FAIL(get_increment_obj_priv_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment obj priv keys", K(ret));
          }
        } else if (schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
                   schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END) {
          if (OB_FAIL(get_increment_dblink_keys(schema_mgr, schema_operation, schema_keys))) {
            LOG_WARN("fail to get increment dblink id", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(extract_core_and_sys_table_ids(
              schema_keys.new_table_keys_, schema_keys.core_table_ids_, schema_keys.sys_table_ids_))) {
        LOG_WARN("extract core and sys table ids failed", K(ret));
      }
    }
  }
  return ret;
}

#define REPLAY_OP(key, del_keys, new_keys, is_delete, is_exist)      \
  ({                                                                 \
    int ret = OB_SUCCESS;                                            \
    int hash_ret = -1;                                               \
    if (is_delete) {                                                 \
      hash_ret = new_keys.erase_refactored(key);                     \
      if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) { \
        ret = OB_ERR_UNEXPECTED;                                     \
        LOG_WARN("erase failed", K(hash_ret), K(ret));               \
      } else if (is_exist) {                                         \
        hash_ret = del_keys.set_refactored(key);                     \
        if (OB_SUCCESS != hash_ret) {                                \
          ret = OB_ERR_UNEXPECTED;                                   \
          LOG_WARN("erase failed", K(hash_ret), K(ret));             \
        }                                                            \
      }                                                              \
    } else {                                                         \
      hash_ret = new_keys.set_refactored(key);                       \
      if (OB_SUCCESS != hash_ret) {                                  \
        ret = OB_ERR_UNEXPECTED;                                     \
        LOG_WARN("erase failed", K(hash_ret), K(ret));               \
      }                                                              \
    }                                                                \
    ret;                                                             \
  })

int ObServerSchemaService::replay_log_reversely(const ObSchemaMgr& schema_mgr,
    const ObSchemaService::SchemaOperationSetWithAlloc& schema_operations, AllSchemaKeys& schema_keys)
{
  int ret = OB_SUCCESS;

  int64_t bucket_size = schema_operations.count();
  if (OB_FAIL(schema_keys.create(bucket_size))) {
    LOG_WARN("fail to create hashset: ", K(bucket_size), K(ret));
    ret = OB_INNER_STAT_ERROR;
  } else {
    for (int64_t i = schema_operations.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObSchemaOperation& schema_operation = schema_operations.at(i);
      LOG_INFO("schema operation", K(schema_operation));
      if (schema_operation.op_type_ > OB_DDL_TENANT_OPERATION_BEGIN &&
          schema_operation.op_type_ < OB_DDL_TENANT_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete =
            (OB_DDL_ADD_TENANT == schema_operation.op_type_ || OB_DDL_ADD_TENANT_START == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleTenantSchema* tenant = NULL;
        // After the schema is split, user tenants will not replay the tenant ddl
        if (OB_INVALID_TENANT_ID != schema_mgr.get_tenant_id() && OB_SYS_TENANT_ID != schema_mgr.get_tenant_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("only sys tenant can revert tenant ddl operation after schema splited", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_mgr.get_tenant_schema(tenant_id, tenant))) {
          LOG_WARN("get tenant schema failed", K(tenant_id), K(ret));
        } else if (NULL != tenant) {
          is_exist = true;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_DDL_DEL_TENANT == schema_operation.op_type_ ||
                   OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
          int hash_ret = schema_keys.new_tenant_keys_.set_refactored(schema_key);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set new tenant keys failed", K(ret), K(hash_ret), K(tenant_id));
          }
        } else if (OB_DDL_RENAME_TENANT == schema_operation.op_type_ ||
                   OB_DDL_ALTER_TENANT == schema_operation.op_type_ ||
                   OB_DDL_ADD_TENANT_END == schema_operation.op_type_ ||
                   OB_DDL_DEL_TENANT_START == schema_operation.op_type_ ||
                   OB_DDL_DROP_TENANT_TO_RECYCLEBIN == schema_operation.op_type_ ||
                   OB_DDL_FLASHBACK_TENANT == schema_operation.op_type_) {
          int hash_ret = schema_keys.alter_tenant_keys_.set_refactored_1(schema_key, 1);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add alter tenant id to alter_tenant_ids", K(hash_ret), K(ret));
          }
        } else if (OB_DDL_ADD_TENANT == schema_operation.op_type_ ||
                   OB_DDL_ADD_TENANT_START == schema_operation.op_type_) {
          int hash_ret = schema_keys.new_tenant_keys_.erase_refactored(schema_key);
          if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("erase new tenant keys failed", K(ret), K(hash_ret), K(tenant_id));
          }
          if (OB_SUCC(ret)) {
            hash_ret = schema_keys.alter_tenant_keys_.erase_refactored(schema_key);
            if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("erase alter tenant keys failed", K(ret), K(hash_ret), K(tenant_id));
            }
          }
          if (OB_SUCC(ret) && is_exist) {
            hash_ret = schema_keys.del_tenant_keys_.set_refactored(schema_key);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("set del tenant keys failed", K(ret), K(hash_ret), K(tenant_id));
            }
          }
        }
        // sys variable, for compatible
        if (OB_INVALID_TENANT_ID != schema_mgr.get_tenant_id() &&
            schema_operation.tenant_id_ != schema_mgr.tenant_id_) {
          LOG_INFO("sys variable key not match, just skip",
              K(ret),
              K(schema_operation),
              "tenant_id",
              schema_mgr.get_tenant_id());
        } else {
          if (OB_SUCC(ret)) {
            const ObSimpleSysVariableSchema* sys_variable = NULL;
            is_exist = false;
            if (OB_FAIL(schema_mgr.sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
              LOG_WARN("get user info failed", K(tenant_id), K(ret));
            } else if (NULL != sys_variable) {
              is_exist = true;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(REPLAY_OP(schema_key,
                    schema_keys.del_sys_variable_keys_,
                    schema_keys.new_sys_variable_keys_,
                    is_delete,
                    is_exist))) {
              LOG_WARN("replay operation failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (is_delete) {
            if (OB_FAIL(DEL_TENANT_OP(tenant_id, schema_keys, new))) {
              LOG_WARN("delete new op failed", K(tenant_id), K(ret));
            } else if (OB_FAIL(DEL_TENANT_OP(tenant_id, schema_keys, del))) {
              LOG_WARN("delete del op failed", K(tenant_id), K(ret));
            }
          }
        }
        // dropped tenant info
        if (OB_SUCC(ret)) {
          if (OB_DDL_DEL_TENANT == schema_operation.op_type_ || OB_DDL_DEL_TENANT_END == schema_operation.op_type_) {
            int64_t count = schema_keys.add_drop_tenant_keys_.size();
            if (0 != count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("add_drop_tenant_keys num should be 0", K(ret), K(count));
            } else {
              int hash_ret = schema_keys.del_drop_tenant_keys_.set_refactored_1(schema_key, 1);
              if (OB_SUCCESS != hash_ret) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to del drop tenant key", K(hash_ret), K(ret), K(schema_key));
              }
            }
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_SYS_VAR_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_SYS_VAR_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = false;
        bool is_exist = false;
        const ObSimpleSysVariableSchema* sys_variable = NULL;
        is_exist = false;
        if (OB_FAIL(schema_mgr.sys_variable_mgr_.get_sys_variable_schema(tenant_id, sys_variable))) {
          LOG_WARN("get user info failed", K(tenant_id), K(ret));
        } else if (NULL != sys_variable) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(schema_key,
                  schema_keys.del_sys_variable_keys_,
                  schema_keys.new_sys_variable_keys_,
                  is_delete,
                  is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_USER_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_USER_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t user_id = schema_operation.user_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.user_id_ = user_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_CREATE_USER == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleUserSchema* user = NULL;
        if (OB_FAIL(schema_mgr.get_user_schema(user_id, user))) {
          LOG_WARN("get user schema failed", K(user_id), K(ret));
        } else if (NULL != user) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  REPLAY_OP(schema_key, schema_keys.del_user_keys_, schema_keys.new_user_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_DATABASE_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_DATABASE_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t database_id = schema_operation.database_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.database_id_ = database_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_ADD_DATABASE == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleDatabaseSchema* database = NULL;
        if (OB_FAIL(schema_mgr.get_database_schema(database_id, database))) {
          LOG_WARN("get database schema failed", K(database_id), K(ret));
        } else if (NULL != database) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_database_keys_, schema_keys.new_database_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLEGROUP_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_TABLEGROUP_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t tablegroup_id = schema_operation.tablegroup_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.tablegroup_id_ = tablegroup_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_ADD_TABLEGROUP == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleTablegroupSchema* tablegroup = NULL;
        if (OB_FAIL(schema_mgr.get_tablegroup_schema(tablegroup_id, tablegroup))) {
          LOG_WARN("get tablegroup schema failed", K(tablegroup_id), K(ret));
        } else if (NULL != tablegroup) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(schema_key,
                  schema_keys.del_tablegroup_keys_,
                  schema_keys.new_tablegroup_keys_,
                  is_delete,
                  is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          } else if (OB_DDL_ALTER_TABLEGROUP_PARTITION == schema_operation.op_type_ ||
                     OB_DDL_SPLIT_TABLEGROUP_PARTITION == schema_operation.op_type_ ||
                     OB_DDL_PARTITIONED_TABLEGROUP_TABLE == schema_operation.op_type_ ||
                     OB_DDL_FINISH_SPLIT_TABLEGROUP == schema_operation.op_type_ ||
                     OB_DDL_FINISH_LOGICAL_SPLIT_TABLEGROUP == schema_operation.op_type_) {
            // alter tablegroup partition is a batch operation, which needs to trigger the schema refresh of
            // the table under the same tablegroup
            ObArray<const ObSimpleTableSchemaV2*> table_schemas;
            bool need_reset = true;
            // FIXME: Oceanbase tablegroup needs to fetch schema_mgr from the system tenant, but oceanbase tablegroup
            //  does not support partition management operations, so it can not be processed temporarily
            if (OB_FAIL(
                    schema_mgr.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, need_reset, table_schemas))) {
              LOG_WARN("fail to get table schemas in tablegroup", K(ret));
            } else {
              SchemaKey table_schema_key;
              table_schema_key.tenant_id_ = tenant_id;
              table_schema_key.schema_version_ = schema_version;
              is_delete = false;
              is_exist = false;
              for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
                table_schema_key.table_id_ = table_schemas.at(i)->get_table_id();
                ret = schema_keys.del_table_keys_.exist_refactored(table_schema_key);
                if (OB_HASH_EXIST == ret) {
                  // This fallback schema refresh involves the create table operation,
                  // there is no need to refresh the table's schema
                  ret = OB_SUCCESS;
                } else if (OB_HASH_NOT_EXIST == ret) {
                  if (OB_FAIL(schema_keys.new_table_keys_.set_refactored_1(table_schema_key, 1))) {
                    LOG_WARN("fail to set table_schema_key", K(ret), K(table_schema_key), K(tablegroup_id));
                  }
                } else {
                  LOG_WARN("fail to check table schema key exist", K(ret), K(table_schema_key), K(tablegroup_id));
                }
              }
            }
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLE_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_TABLE_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t table_id = schema_operation.table_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.table_id_ = table_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete =
            (OB_DDL_CREATE_TABLE == schema_operation.op_type_ || OB_DDL_CREATE_INDEX == schema_operation.op_type_ ||
                OB_DDL_CREATE_GLOBAL_INDEX == schema_operation.op_type_ ||
                OB_DDL_CREATE_VIEW == schema_operation.op_type_ ||
                OB_DDL_TRUNCATE_TABLE_CREATE == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleTableSchemaV2* table = NULL;
        if (OB_FAIL(schema_mgr.get_table_schema(table_id, table))) {
          LOG_WARN("get table schema failed", K(table_id), K(ret));
        } else if (NULL != table) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_table_keys_, schema_keys.new_table_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_SYNONYM_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_SYNONYM_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t synonym_id = schema_operation.synonym_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.synonym_id_ = synonym_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_CREATE_SYNONYM == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleSynonymSchema* synonym = NULL;
        if (OB_FAIL(schema_mgr.synonym_mgr_.get_synonym_schema(synonym_id, synonym))) {
          LOG_WARN("get synonym schema failed", K(synonym_id), K(ret));
        } else if (NULL != synonym) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_synonym_keys_, schema_keys.new_synonym_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_OUTLINE_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_OUTLINE_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t outline_id = schema_operation.outline_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.outline_id_ = outline_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_CREATE_OUTLINE == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleOutlineSchema* outline = NULL;
        if (OB_FAIL(schema_mgr.outline_mgr_.get_outline_schema(outline_id, outline))) {
          LOG_WARN("get outline schema failed", K(outline_id), K(ret));
        } else if (NULL != outline) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_outline_keys_, schema_keys.new_outline_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_DB_PRIV_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_DB_PRIV_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t user_id = schema_operation.user_id_;
        const ObString& database_name = schema_operation.database_name_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.user_id_ = user_id;
        schema_key.database_name_ = database_name;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_GRANT_REVOKE_DB == schema_operation.op_type_);
        bool is_exist = false;
        const ObDBPriv* db_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_db_priv(schema_key.get_db_priv_key(), db_priv, true))) {
          LOG_WARN("get db_priv failed", "db_priv_key", schema_key.get_db_priv_key(), K(ret));
        } else if (NULL != db_priv) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_db_priv_keys_, schema_keys.new_db_priv_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_TABLE_PRIV_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_TABLE_PRIV_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t user_id = schema_operation.user_id_;
        const ObString& database_name = schema_operation.database_name_;
        const ObString& table_name = schema_operation.table_name_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.user_id_ = user_id;
        schema_key.database_name_ = database_name;
        schema_key.table_name_ = table_name;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_GRANT_REVOKE_TABLE == schema_operation.op_type_);
        bool is_exist = false;
        const ObTablePriv* table_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_table_priv(schema_key.get_table_priv_key(), table_priv))) {
          LOG_WARN("get table_priv failed", "table_priv_key", schema_key.get_table_priv_key(), K(ret));
        } else if (NULL != table_priv) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(schema_key,
                  schema_keys.del_table_priv_keys_,
                  schema_keys.new_table_priv_keys_,
                  is_delete,
                  is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_UDF_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_UDF_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const ObString& udf_name = schema_operation.udf_name_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.udf_name_ = udf_name;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_CREATE_UDF == schema_operation.op_type_);
        bool is_exist = false;
        const ObSimpleUDFSchema* udf = NULL;
        if (OB_FAIL(schema_mgr.udf_mgr_.get_udf_schema_with_name(tenant_id, udf_name, udf))) {
          LOG_WARN("get udf schema failed", K(udf_name), K(ret));
        } else if (NULL != udf) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  REPLAY_OP(schema_key, schema_keys.del_udf_keys_, schema_keys.new_udf_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_SEQUENCE_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_SEQUENCE_OPERATION_END) {
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
        const ObSequenceSchema* sequence = NULL;
        if (OB_FAIL(schema_mgr.sequence_mgr_.get_sequence_schema(schema_key.sequence_id_, sequence))) {
          LOG_WARN("failed to get sequence schema", K(schema_key), K(ret));
        } else if (NULL != sequence) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_sequence_keys_, schema_keys.new_sequence_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_PROFILE_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_PROFILE_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.profile_id_ = schema_operation.table_id_;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (ObSchemaOperation::type_semantic_str(schema_operation.op_type_) == ObString("CREATE"));
        bool is_exist = false;
        const ObProfileSchema* schema = NULL;
        if (OB_FAIL(schema_mgr.profile_mgr_.get_schema_by_id(schema_key.profile_id_, schema))) {
          LOG_WARN("failed to get schema", K(schema_key), K(ret));
        } else if (NULL != schema) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_profile_keys_, schema_keys.new_profile_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_SYS_PRIV_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_SYS_PRIV_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t grantee_id = schema_operation.grantee_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.grantee_id_ = grantee_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = OB_DDL_SYS_PRIV_GRANT_REVOKE == schema_operation.op_type_;
        bool is_exist = false;
        const ObSysPriv* sys_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_sys_priv(schema_key.get_sys_priv_key(), sys_priv))) {
          LOG_WARN("get sys_priv failed", "sys_priv_key", schema_key.get_sys_priv_key(), K(ret));
        } else if (NULL != sys_priv) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_sys_priv_keys_, schema_keys.new_sys_priv_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_OBJ_PRIV_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_OBJ_PRIV_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t obj_id = schema_operation.get_obj_id();
        const uint64_t obj_type = schema_operation.get_obj_type();
        const uint64_t col_id = schema_operation.get_col_id();
        const uint64_t grantee_id = schema_operation.get_grantee_id();
        const uint64_t grantor_id = schema_operation.get_grantor_id();
        const int64_t schema_version = schema_operation.schema_version_;
        int hash_ret = OB_SUCCESS;
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
        const ObObjPriv* obj_priv = NULL;
        if (OB_FAIL(schema_mgr.priv_mgr_.get_obj_priv(schema_key.get_obj_priv_key(), obj_priv))) {
          LOG_WARN("get obj_priv failed", "obj_priv_key", schema_key.get_obj_priv_key(), K(ret));
        } else if (NULL != obj_priv) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_obj_priv_keys_, schema_keys.new_obj_priv_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else if (schema_operation.op_type_ > OB_DDL_DBLINK_OPERATION_BEGIN &&
                 schema_operation.op_type_ < OB_DDL_DBLINK_OPERATION_END) {
        const uint64_t tenant_id = schema_operation.tenant_id_;
        const uint64_t dblink_id = schema_operation.dblink_id_;
        const int64_t schema_version = schema_operation.schema_version_;
        SchemaKey schema_key;
        schema_key.tenant_id_ = tenant_id;
        schema_key.dblink_id_ = dblink_id;
        schema_key.schema_version_ = schema_version;
        bool is_delete = (OB_DDL_CREATE_DBLINK == schema_operation.op_type_);
        bool is_exist = false;
        const ObDbLinkSchema* dblink_schema = NULL;
        if (OB_FAIL(schema_mgr.dblink_mgr_.get_dblink_schema(dblink_id, dblink_schema))) {
          LOG_WARN("get dblink schema failed", K(dblink_id), K(ret));
        } else if (NULL != dblink_schema) {
          is_exist = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(REPLAY_OP(
                  schema_key, schema_keys.del_dblink_keys_, schema_keys.new_dblink_keys_, is_delete, is_exist))) {
            LOG_WARN("replay operation failed", K(ret));
          }
        }
      } else {
        // ingore other operaton.
      }
    }
  }

  return ret;
}

int ObServerSchemaService::convert_to_simple_schema(const ObTableSchema& schema, ObSimpleTableSchemaV2& simple_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(simple_schema.assign(schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    simple_schema.set_part_num(schema.get_first_part_num());
    simple_schema.set_def_sub_part_num(schema.get_def_sub_part_num());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(simple_schema.set_locality(schema.get_locality_str()))) {
    LOG_WARN("fail to set locality", K(ret));
  } else if (schema.get_locality_str().empty()) {
    // The locality of the schema is empty, no need to set zone_list
  } else {
    common::ObArray<common::ObZone> zone_list;
    if (OB_FAIL(schema.get_zone_list(zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else if (OB_FAIL(simple_schema.set_zone_list(zone_list))) {
      LOG_WARN("fail to set zone list", K(ret));
    } else {
    }  // no more to do
  }

  return ret;
}

int ObServerSchemaService::convert_to_simple_schema(
    const ObIArray<ObTableSchema>& schemas, ObIArray<ObSimpleTableSchemaV2>& simple_schemas)
{
  int ret = OB_SUCCESS;
  simple_schemas.reset();

  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
  {
    ObSimpleTableSchemaV2 simple_schema;
    if (OB_FAIL(convert_to_simple_schema(*schema, simple_schema))) {
      LOG_WARN("convert failed", K(ret));
    } else if (OB_FAIL(simple_schemas.push_back(simple_schema))) {
      LOG_WARN("push back failed", K(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::convert_to_simple_schema(
    const ObIArray<ObTableSchema*>& schemas, ObIArray<ObSimpleTableSchemaV2>& simple_schemas)
{
  int ret = OB_SUCCESS;
  simple_schemas.reset();

  FOREACH_CNT_X(schema, schemas, OB_SUCC(ret))
  {
    const ObTableSchema* table_schema = *schema;
    ObSimpleTableSchemaV2 simple_schema;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(table_schema));
    } else if (OB_FAIL(convert_to_simple_schema(*table_schema, simple_schema))) {
      LOG_WARN("convert failed", K(ret));
    } else if (OB_FAIL(simple_schemas.push_back(simple_schema))) {
      LOG_WARN("push back failed", K(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::init_all_core_schema()
{
  int ret = OB_SUCCESS;
  if (all_core_schema_init_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("core schema init twice: ", K(ret));
  } else if (OB_ISNULL(schema_mgr_for_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_for_cache is null", K(ret));
  } else if (OB_FAIL(fill_all_core_table_schema(*schema_mgr_for_cache_))) {
    LOG_ERROR("failed to fill core schema: ", K(ret));
  } else {
    all_core_schema_init_ = true;
    LOG_INFO("init all core schema succ.", "version", OB_CORE_SCHEMA_VERSION);
  }
  return ret;
}

int ObServerSchemaService::fill_all_core_table_schema(ObSchemaMgr& schema_mgr_for_cache)
{
  int ret = OB_SUCCESS;
  ObTableSchema all_core_table_schema;
  ObSimpleTableSchemaV2 all_core_table_schema_simple;
  // TODO oushen defualt database and tablegroup schema is need?
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(schema_service_->get_all_core_table_schema(all_core_table_schema))) {
    LOG_WARN("failed to init schema service, ret=[%d]", K(ret));
  } else if (OB_FAIL(convert_to_simple_schema(all_core_table_schema, all_core_table_schema_simple))) {
    LOG_WARN("failed to add table schema into the schema manager, ret=[%d]", K(ret));
  } else if (OB_FAIL(schema_mgr_for_cache.add_table(all_core_table_schema_simple))) {
    LOG_WARN("failed to add table schema into the schema manager, ret=[%d]", K(ret));
  } else {
    schema_mgr_for_cache.set_schema_version(OB_CORE_SCHEMA_VERSION);
  }
  return ret;
}

int ObServerSchemaService::refresh_schema(int64_t expected_version)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  bool is_full = true;
  ObSchemaService::SchemaOperationSetWithAlloc schema_operations;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(determine_refresh_type())) {
    LOG_WARN("fail to copy tenant id, ", K(ret));
  } else if (OB_ISNULL(schema_mgr_for_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_for_cache_ is null", K(ret));
  } else if (OB_LIKELY(!(is_full = next_refresh_type_is_full()))) {

    ALLOW_NEXT_LOG();
    LOG_INFO("[REFRESH_SCHEMA] start to refresh increment schema",
        "current schema_version",
        schema_mgr_for_cache_->get_schema_version(),
        K(expected_version));
    if (OB_FAIL(refresh_increment_schema(expected_version))) {
      LOG_WARN("refresh_increment_schema failed", K(ret));
    } else {
      ALLOW_NEXT_LOG();
      const int64_t cost = ObTimeUtility::current_time() - start;
      LOG_INFO("[REFRESH_SCHEMA] refresh increment schema succeed",
          "current schema_version",
          schema_mgr_for_cache_->get_schema_version(),
          "cost",
          cost);
    }
    if (OB_SUCC(ret)) {
      refresh_times_++;
      if (OB_UNLIKELY(!increment_basepoint_set_)) {
        increment_basepoint_set_ = true;
        LOG_INFO("[REFRESH_SCHEMA] full schema is ready", K(ret), K(expected_version));
      }
    }
  } else {
    // Solve the heartbeat version is a temporary version (rs init_sys_schema publish schema version: 2),
    // no need to refresh in full
    if ((OB_CORE_SCHEMA_VERSION + 1) == expected_version) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("refresh full schema with informal schema_version", K(ret), K(expected_version));
    } else {
      LOG_INFO("[REFRESH_SCHEMA] start to refresh full schema", K(expected_version));
      if (OB_FAIL(refresh_full_schema(expected_version))) {
        LOG_WARN("refresh_full_schema failed", K(ret));
      } else {
        increment_basepoint_set_ = true;
        LOG_INFO("full schema is ready", K(ret), K(expected_version));
        const int64_t cost = ObTimeUtility::current_time() - start;
        LOG_INFO("[REFRESH_SCHEMA] refresh full schema succeed",
            "current schema_version",
            schema_mgr_for_cache_->get_schema_version(),
            "cost",
            cost);
        refresh_times_++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t now = ObTimeUtility::current_time();
    EVENT_INC(REFRESH_SCHEMA_COUNT);
    EVENT_ADD(REFRESH_SCHEMA_TIME, now - start);
  }
  return ret;
}

int ObServerSchemaService::check_tenant_can_use_new_table(const uint64_t tenant_id, bool& can) const
{
  int ret = OB_SUCCESS;
  int64_t split_schema_version_v2 = OB_INVALID_VERSION;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2210) {
    can = false;
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    can = (GCTX.split_schema_version_v2_ >= 0);
  } else if (OB_FAIL(schema_split_version_v2_map_.get_refactored(tenant_id, split_schema_version_v2))) {
    LOG_WARN("tenant schema split info not init", K(ret), K(tenant_id));
  } else {
    can = (split_schema_version_v2 >= 0);
  }
  return ret;
}

int ObServerSchemaService::try_update_split_schema_version_v2(
    const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool can = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    // for compatible, do nothing
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_can_use_new_table(tenant_id, can))) {
    LOG_WARN("fail to check if can use new table", K(tenant_id));
  } else if (can) {
    // do nothing
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), KP_(sql_proxy), KP_(schema_service));
  } else {
    // try update split schema version v2
    int64_t split_schema_version_v2 = OB_INVALID_VERSION;
    if (OB_SYS_TENANT_ID == tenant_id) {
      ObGlobalStatProxy proxy(*sql_proxy_);
      if (OB_FAIL(proxy.get_split_schema_version_v2(split_schema_version_v2))) {
        LOG_WARN("fail to get split schema version v2", K(ret));
      } else if (split_schema_version_v2 >= 0) {
        (void)GCTX.set_split_schema_version_v2(split_schema_version_v2);
      }
    } else {
      bool overwrite = true;
      if (OB_FAIL(schema_service_->get_split_schema_version_v2(
              *sql_proxy_, schema_status, tenant_id, split_schema_version_v2))) {
        LOG_WARN("fail to get split schema version v2", K(ret), K(tenant_id), K(schema_status));
      } else if (split_schema_version_v2 < 0) {
        // do nothing
      } else if (OB_FAIL(schema_split_version_v2_map_.set_refactored(tenant_id, split_schema_version_v2, overwrite))) {
        LOG_WARN("set split schema version v2 failed", K(ret), K(tenant_id), K(split_schema_version_v2));
      } else {
        LOG_INFO("get split_schema_version_v2", K(ret), K(tenant_id), K(split_schema_version_v2));
      }
    }
  }
  return ret;
}

// new schema refresh
int ObServerSchemaService::refresh_schema(const ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  const uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr* schema_mgr_for_cache = NULL;
  bool refresh_full_schema = true;

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
  } else if (OB_FAIL(refresh_full_schema_map_.get_refactored(tenant_id, refresh_full_schema))) {
    LOG_WARN("refresh full schema", K(ret), K(tenant_id));
  } else if (refresh_full_schema) {
    ALLOW_NEXT_LOG();
    LOG_INFO("[REFRESH_SCHEMA] start to refresh full schema",
        "current schema_version",
        schema_mgr_for_cache->get_schema_version(),
        K(schema_status));

    if (OB_SYS_TENANT_ID == schema_status.tenant_id_) {
      if (OB_FAIL(refresh_sys_full_schema(schema_status))) {
        LOG_WARN("sys tenant refresh full schema failed", K(ret), K(schema_status));
      }
    } else {
      if (OB_FAIL(refresh_tenant_full_schema(schema_status))) {
        LOG_WARN("tenant refresh full schema failed", K(ret), K(schema_status));
      }
    }

    ALLOW_NEXT_LOG();
    LOG_INFO("[REFRESH_SCHEMA] finish refresh full schema",
        K(ret),
        K(schema_status),
        "current schema_version",
        schema_mgr_for_cache->get_schema_version(),
        "cost",
        ObTimeUtility::current_time() - start);

    if (OB_SUCC(ret)) {
      bool overwrite = true;
      refresh_full_schema = false;
      if (OB_FAIL(refresh_full_schema_map_.set_refactored(tenant_id, refresh_full_schema, overwrite))) {
        LOG_WARN("fail to set refresh full schema flag", K(ret), K(tenant_id));
      }
    }
  } else {
    ALLOW_NEXT_LOG();
    LOG_INFO("[REFRESH_SCHEMA] start to refresh increment schema",
        "current schema_version",
        schema_mgr_for_cache->get_schema_version(),
        K(schema_status));

    if (OB_SYS_TENANT_ID == schema_status.tenant_id_) {
      if (OB_FAIL(refresh_sys_increment_schema(schema_status))) {
        LOG_WARN("sys tenant refresh increment schema failed", K(ret), K(schema_status));
      }
    } else {
      if (OB_FAIL(refresh_tenant_increment_schema(schema_status))) {
        LOG_WARN("tenant refresh increment schema failed", K(ret), K(schema_status));
      }
    }

    ALLOW_NEXT_LOG();
    LOG_INFO("[REFRESH_SCHEMA] finish refresh increment schema",
        K(ret),
        K(schema_status),
        "current schema_version",
        schema_mgr_for_cache->get_schema_version(),
        "cost",
        ObTimeUtility::current_time() - start);
  }

  if (OB_SUCC(ret)) {
    // 1. If the system tenant schema is refreshed successfully, it is considered that the full refresh of
    //  the schema for the first time is successful, to avoid the server failing to start because
    //  the cluster restarts cannot refresh the common tenant's schema.
    // 2. If the user tenant schema refresh triggered subsequently fails, the local refreshed_schema_info
    //  is not pushed to ensure that the subsequent heartbeat can trigger the user tenant schema refresh.
    // 3. Before RS does DDL, it need to refresh the tenant schema version to the latest to ensure
    //  that the schema_version in the tenant increases monotonically.
    if (OB_SYS_TENANT_ID == tenant_id && OB_UNLIKELY(!increment_basepoint_set_)) {
      increment_basepoint_set_ = true;
      LOG_INFO("full schema is ready", K(ret));
    }
    const int64_t now = ObTimeUtility::current_time();
    EVENT_INC(REFRESH_SCHEMA_COUNT);
    EVENT_ADD(REFRESH_SCHEMA_TIME, now - start);
  }
  return ret;
}

int ObServerSchemaService::refresh_tenant_full_schema(const ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  int64_t fetch_version = OB_INVALID_VERSION;
  ObSchemaMgr* schema_mgr_for_cache = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(schema_status));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", K(ret));
  } else if (OB_FAIL(get_schema_version_in_inner_table(*sql_proxy_, schema_status, fetch_version))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_WARN("fail to get target_version", K(ret), K(tenant_id));
    } else {
      // At least two transactions are required to build a tenant. At this time, the tenant's schema may not be ready.
      // In this case, skip first
      ret = OB_SCHEMA_EAGAIN;
      LOG_INFO("tenant schema is not ready, just skip", K(ret), K(schema_status), K(fetch_version));
    }
  } else if (fetch_version <= 0) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_INFO("tenant schema is not ready, just skip", K(ret), K(schema_status), K(fetch_version));
  } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
    LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(refresh_tenant_full_normal_schema(*sql_proxy_, schema_status, fetch_version))) {
      LOG_WARN("refresh_full_normal_schema failed, do some clear", K(ret), K(schema_status), K(fetch_version));
    } else if (OB_FAIL(reload_mock_schema_info(*schema_mgr_for_cache))) {
      LOG_WARN("fail to reload mock schema info", K(ret), K(tenant_id));
    } else {
      schema_mgr_for_cache->set_schema_version(fetch_version);
      if (OB_FAIL(publish_schema(tenant_id))) {
        LOG_WARN("publish_schema failed", K(ret), K(tenant_id), K(schema_status), K(fetch_version));
      } else {
        LOG_INFO("publish full normal schema by schema_version succeed",
            K(ret),
            K(tenant_id),
            K(schema_status),
            K(fetch_version));
      }
    }

    if (OB_FAIL(ret)) {
      LOG_INFO("[REFRESH_SCHEMA] refresh full schema failed, do some clear", K(ret), K(schema_status));
      schema_mgr_for_cache->reset();
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = init_tenant_basic_schema(tenant_id))) {
        LOG_WARN("fail to init tenant basic schema", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::refresh_tenant_increment_schema(const ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const uint64_t tenant_id = schema_status.tenant_id_;
  int64_t fetch_version = OB_INVALID_VERSION;
  ObSchemaMgr* schema_mgr_for_cache = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(schema_status));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", K(ret));
  } else if (OB_FAIL(set_timeout_ctx(ctx))) {
    LOG_WARN("fail to set timeout ctx", K(ret));
  } else if (OB_FAIL(get_schema_version_in_inner_table(*sql_proxy_, schema_status, fetch_version))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_WARN("fail to get target_version", K(ret), K(tenant_id));
    } else {
      // At least two transactions are required to build a tenant. At this time, the tenant's schema may not be ready.
      // In this case, skip first
      ret = OB_SUCCESS;
      LOG_INFO("tenant schema is not ready, just skip", K(ret), K(schema_status), K(fetch_version));
    }
  } else if (fetch_version <= 0) {
    LOG_INFO("tenant schema is not ready, just skip", K(ret), K(schema_status), K(fetch_version));
  } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
    LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
  } else {
    ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
    int64_t local_schema_version = schema_mgr_for_cache->get_schema_version();
    if (OB_INVALID_TIMESTAMP != schema_status.snapshot_timestamp_ &&
        fetch_version > schema_status.readable_schema_version_) {
      // Incremental refresh ensures that the tenant's schema_version does not exceed readable_schema_version
      fetch_version = schema_status.readable_schema_version_;
    }
    if (OB_FAIL(schema_service_->get_increment_schema_operations(
            schema_status, local_schema_version, fetch_version, *sql_proxy_, schema_operations))) {
      LOG_WARN("get_increment_schema_operations failed",
          K(ret),
          K(schema_status),
          K(local_schema_version),
          K(fetch_version));
    } else if (schema_operations.count() > 0) {
      // new cache
      AllSchemaKeys all_keys;
      if (OB_FAIL(replay_log(*schema_mgr_for_cache, schema_operations, all_keys))) {
        LOG_WARN("replay_log failed",
            K(ret),
            K(schema_operations),
            K(schema_status),
            K(local_schema_version),
            K(fetch_version));
      } else if (OB_FAIL(
                     update_schema_mgr(*sql_proxy_, schema_status, *schema_mgr_for_cache, fetch_version, all_keys))) {
        LOG_WARN("update schema mgr failed", K(ret), K(schema_status), K(local_schema_version), K(fetch_version));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(reload_mock_schema_info(*schema_mgr_for_cache))) {
        LOG_WARN("fail to reload mock schema info", K(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      schema_mgr_for_cache->set_schema_version(fetch_version);
      if (OB_FAIL(publish_schema(tenant_id))) {
        LOG_WARN("publish_schema failed", K(ret), K(tenant_id));
      } else {
        LOG_INFO(
            "change schema version", K(ret), K(tenant_id), K(schema_status), K(local_schema_version), K(fetch_version));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::refresh_sys_full_schema(const ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr* schema_mgr_for_cache = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    while (OB_SUCC(ret)) {
      const int64_t frozen_version = -1;
      int64_t retry_count = 0;
      bool core_schema_change = true;
      bool sys_schema_change = true;
      int64_t local_schema_version = 0;
      int64_t core_schema_version = 0;
      int64_t new_core_schema_version = 0;
      int64_t schema_version = 0;
      if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
        LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
      } else if (OB_ISNULL(schema_mgr_for_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema mgr for cache is null", K(ret));
      } else {
        local_schema_version = schema_mgr_for_cache->get_schema_version();
      }
      // If refreshing the full amount fails, you need to reset and retry until it succeeds.
      // The outer layer avoids the scenario of failure to refresh the full amount of schema in the bootstrap stage.
      while (OB_SUCC(ret) && (core_schema_change || sys_schema_change)) {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("observer is stopping", K(ret));
          break;
        } else if (retry_count > 0) {
          LOG_WARN("refresh_full_schema failed, retry", K(retry_count));
        }
        ObISQLClient& sql_client = *sql_proxy_;
        // refresh core table schemas
        if (OB_SUCC(ret) && core_schema_change) {
          if (OB_FAIL(schema_service_->get_core_version(sql_client, frozen_version, core_schema_version))) {
            LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
          } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
            LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
          } else if (core_schema_version > local_schema_version) {
            // for core table schema, we publis as core_temp_version
            int64_t publish_version = 0;
            if (OB_FAIL(ObSchemaService::gen_core_temp_version(core_schema_version, publish_version))) {
              LOG_WARN("gen_core_temp_version failed", K(core_schema_version), K(ret));
            } else if (OB_FAIL(try_fetch_publish_core_schemas(
                           schema_status, core_schema_version, publish_version, sql_client, core_schema_change))) {
              LOG_WARN("try_fetch_publish_core_schemas failed", K(core_schema_version), K(publish_version), K(ret));
            }
          } else {
            core_schema_change = false;
          }
        }

        // refresh sys table schemas
        if (OB_SUCC(ret) && !core_schema_change && sys_schema_change) {
          if (OB_FAIL(get_schema_version_in_inner_table(sql_client, schema_status, schema_version))) {
            LOG_WARN("fail to get schema version in inner table", K(ret));
          } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
            LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
          } else if (OB_FAIL(check_sys_schema_change(
                         sql_client, local_schema_version, schema_version, sys_schema_change))) {
            LOG_WARN("check_sys_schema_change failed", K(schema_version), K(ret));
          } else if (sys_schema_change) {
            // for sys table schema, we publish as sys_temp_version
            const int64_t sys_formal_version = std::max(core_schema_version, schema_version);
            int64_t publish_version = 0;
            if (OB_FAIL(ObSchemaService::gen_sys_temp_version(sys_formal_version, publish_version))) {
              LOG_WARN("gen_sys_temp_version failed", K(sys_formal_version), K(ret));
            } else if (OB_FAIL(try_fetch_publish_sys_schemas(
                           schema_status, schema_version, publish_version, sql_client, sys_schema_change))) {
              LOG_WARN("try_fetch_publish_sys_schemas failed", K(schema_version), K(publish_version), K(ret));
            }
          }

          if (OB_FAIL(ret)) {
            // check whether failed because of core table schema change, go to suitable pos
            int temp_ret = OB_SUCCESS;
            if (OB_SUCCESS !=
                (temp_ret = schema_service_->get_core_version(sql_client, frozen_version, new_core_schema_version))) {
              LOG_WARN("get_core_version failed", K(frozen_version), K(temp_ret));
            } else if (new_core_schema_version != core_schema_version) {
              core_schema_change = true;
              LOG_WARN("core schema change during refresh sys schema",
                  K(core_schema_version),
                  K(new_core_schema_version),
                  K(ret));
              ret = OB_SUCCESS;
            }
          }
        }

        // refresh full normal schema by schema_version
        if (OB_SUCC(ret) && !core_schema_change && !sys_schema_change) {
          const int64_t fetch_version = std::max(core_schema_version, schema_version);
          if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
            LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
          } else if (OB_ISNULL(schema_mgr_for_cache)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema mgr for cache is null", K(ret));
          } else if (OB_FAIL(refresh_tenant_full_normal_schema(sql_client, schema_status, fetch_version))) {
            LOG_WARN("refresh_full_normal_schema failed", K(fetch_version), K(ret));

          } else if (OB_FAIL(reload_mock_schema_info(*schema_mgr_for_cache))) {
            LOG_WARN("fail to reload mock schema info", K(ret), K(tenant_id));
          } else {
            const int64_t publish_version = std::max(core_schema_version, schema_version);
            schema_mgr_for_cache->set_schema_version(publish_version);
            if (OB_FAIL(publish_schema(tenant_id))) {
              LOG_WARN("publish_schema failed", K(ret), K(tenant_id));
            } else {
              LOG_INFO("publish full normal schema by schema_version succeed",
                  K(publish_version),
                  K(core_schema_version),
                  K(schema_version));
            }
          }
          if (OB_FAIL(ret)) {
            // check whether failed because of sys table schema change, go to suitable pos,
            // if during check core table schema change, go to suitable pos
            int temp_ret = OB_SUCCESS;
            if (OB_SUCCESS !=
                (temp_ret = check_core_or_sys_schema_change(
                     sql_client, core_schema_version, schema_version, core_schema_change, sys_schema_change))) {
              LOG_WARN("check_core_or_sys_schema_change failed", K(core_schema_version), K(schema_version), K(ret));
            } else if (core_schema_change || sys_schema_change) {
              ret = OB_SUCCESS;
            }
          }
        }
        ++retry_count;
      }
      // It must be reset before each refresh schema to prevent ddl from being in progress,
      // but refresh full may have added some tables
      // And the latter table was deleted again, at this time refresh will not delete this table in the cache
      if (OB_SUCC(ret)) {
        break;
      } else {
        LOG_INFO("[REFRESH_SCHEMA] refresh full schema failed, do some clear", K(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          LOG_ERROR("fail to get schema_mgr_for_cache", K(ret), K(tmp_ret), K(schema_status));
        } else if (OB_ISNULL(schema_mgr_for_cache)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("schema mgr for cache is null", K(ret), K(tmp_ret));
        } else if (FALSE_IT(schema_mgr_for_cache->reset())) {
        } else if (OB_SUCCESS != (tmp_ret = init_sys_basic_schema())) {
          LOG_ERROR("init basic schema failed", K(ret), K(tmp_ret));
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

#define INIT_TENANT_MEM_MGR(map, tenant_id, mem_mgr_label, schema_mgr_label)        \
  if (OB_FAIL(ret)) {                                                               \
  } else if (OB_ISNULL(map.get(tenant_id))) {                                       \
    void* buff = ob_malloc(sizeof(ObSchemaMemMgr), mem_mgr_label);                  \
    ObSchemaMemMgr* schema_mem_mgr = NULL;                                          \
    bool overwrite = true;                                                          \
    if (OB_ISNULL(buff)) {                                                          \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                              \
      SQL_PC_LOG(ERROR, "alloc schema_mem_mgr failed", K(ret), K(tenant_id));       \
    } else if (NULL == (schema_mem_mgr = new (buff) ObSchemaMemMgr())) {            \
      ret = OB_NOT_INIT;                                                            \
      SQL_PC_LOG(WARN, "fail to constructor schema_mem_mgr", K(ret), K(tenant_id)); \
    } else if (OB_FAIL(schema_mem_mgr->init(schema_mgr_label, tenant_id))) {        \
      LOG_WARN("fail to init schema_mem_mgr", K(ret), K(tenant_id));                \
    } else if (OB_FAIL(map.set_refactored(tenant_id, schema_mem_mgr, overwrite))) { \
      LOG_WARN("fail to set schema_mem_mgr", K(ret), K(tenant_id));                 \
    }                                                                               \
    if (OB_FAIL(ret)) {                                                             \
      if (NULL != schema_mem_mgr) {                                                 \
        schema_mem_mgr->~ObSchemaMemMgr();                                          \
        ob_free(buff);                                                              \
        schema_mem_mgr = NULL;                                                      \
        buff = NULL;                                                                \
      } else if (NULL != buff) {                                                    \
        ob_free(buff);                                                              \
        buff = NULL;                                                                \
      }                                                                             \
    }                                                                               \
  } else {                                                                          \
    LOG_INFO("schema_mgr_for_cache exist", K(ret), K(tenant_id));                   \
  }

    INIT_TENANT_MEM_MGR(mem_mgr_map_, tenant_id, ObModIds::OB_TENANT_SCHEMA_MEM_MGR, ObModIds::OB_TENANT_SCHEMA_MGR);
    INIT_TENANT_MEM_MGR(mem_mgr_for_liboblog_map_,
        tenant_id,
        ObModIds::OB_TENANT_SCHEMA_MEM_MGR_FOR_LIBOBLOG,
        ObModIds::OB_TENANT_SCHEMA_MGR_FOR_LIBOBLOG);

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
    } else if (OB_ISNULL(schema_split_version_v2_map_.get(tenant_id))) {
      int64_t schema_version = OB_INVALID_VERSION;
      bool overwrite = true;
      if (OB_FAIL(schema_split_version_v2_map_.set_refactored(tenant_id, schema_version, overwrite))) {
        LOG_WARN("fail to set schema_split_version_v2_map ", K(ret), K(tenant_id));
      }
    } else {
      LOG_INFO("schema_split_version_v2_map exist", K(ret), K(tenant_id));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_mgr_for_cache_map_.get(tenant_id))) {
      void* buffer = NULL;
      ObIAllocator* allocator = NULL;
      ObSchemaMgr* schema_mgr_for_cache = NULL;
      ObSchemaMemMgr* mem_mgr = NULL;
      if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
        LOG_WARN("fail to get mem_mgr", K(ret), K(tenant_id));
      } else if (OB_ISNULL(mem_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_mgr is null", K(ret), K(tenant_id));
      } else if (OB_FAIL(mem_mgr->alloc(sizeof(ObSchemaMgr), buffer, &allocator))) {
        LOG_WARN("alloc schema mgr failed", K(ret));
      } else if (FALSE_IT(schema_mgr_for_cache = new (buffer) ObSchemaMgr(*allocator))) {
        // will not reach here
      } else if (OB_FAIL(schema_mgr_for_cache->init(tenant_id))) {
        LOG_WARN("init schema mgr for cache failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache_map_.set_refactored(tenant_id, schema_mgr_for_cache))) {
        LOG_WARN("fail to set schema_mgr", K(ret), K(tenant_id));
      }
      if (OB_FAIL(ret)) {
        if (NULL != schema_mgr_for_cache) {
          schema_mgr_for_cache->~ObSchemaMgr();
          ob_free(buffer);
          schema_mgr_for_cache = NULL;
          buffer = NULL;
        } else if (NULL != buffer) {
          ob_free(buffer);
          buffer = NULL;
        }
      }
    } else {
      LOG_INFO("schema_mgr exist", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObServerSchemaService::refresh_sys_increment_schema(const ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  observer::ObUseWeakGuard use_weak_guard;
  const uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr* schema_mgr_for_cache = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema mgr for cache is null", K(ret));
  } else {
    ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
    bool core_schema_change = true;
    bool sys_schema_change = true;
    int64_t local_schema_version = schema_mgr_for_cache->get_schema_version();
    int64_t core_schema_version = 0;
    int64_t schema_version = OB_INVALID_VERSION;
    const int64_t frozen_version = -1;
    int64_t retry_count = 0;
    // In order to avoid missing the schema during the incremental refresh process, it need to retry until it succeeds.
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("observer is stopping", K(ret));
        break;
      } else if (retry_count > 0) {
        LOG_WARN("refresh_increment_schema failed", K(retry_count));
        if (OB_FAIL(set_timeout_ctx(ctx))) {
          LOG_WARN("[REFRESH_SCHEMA] fail to set timeout ctx, need reset schema version",
              K(ret),
              "last_schema_version",
              local_schema_version,
              "cur_schema_version",
              schema_mgr_for_cache->get_schema_version());
          // Synchronous schema refresh triggered by SQL will cause the schema refresh to fail continuously
          // due to the timeout limitation.
          // In order to avoid missing the schema at this time, the schema_version needs to be reset to
          // the last schema_version.
          if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
            LOG_ERROR("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
          } else if (OB_ISNULL(schema_mgr_for_cache)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("schema mgr for cache is null", K(ret));
          } else {
            schema_mgr_for_cache->set_schema_version(local_schema_version);
          }
          break;
        }
      }
      ObISQLClient& sql_client = *sql_proxy_;
      if (OB_SUCC(ret) && core_schema_change) {
        if (OB_FAIL(schema_service_->get_core_version(sql_client, frozen_version, core_schema_version))) {
          LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
        } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
          LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
        } else if (core_schema_version > local_schema_version) {
          int64_t publish_version = OB_INVALID_INDEX;
          if (OB_FAIL(ObSchemaService::gen_core_temp_version(core_schema_version, publish_version))) {
            LOG_WARN("gen_core_temp_version failed", K(core_schema_version), K(ret));
          } else if (OB_FAIL(try_fetch_publish_core_schemas(
                         schema_status, core_schema_version, publish_version, sql_client, core_schema_change))) {
            LOG_WARN("try_fetch_publish_core_schemas failed", K(core_schema_version), K(publish_version), K(ret));
          } else {
          }
        } else {
          core_schema_change = false;
        }

        if (OB_FAIL(ret)) {
          core_schema_change = true;
          sys_schema_change = true;
          LOG_WARN("refresh core table schema failed, need retry", K(frozen_version), K(ret));
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && !core_schema_change && sys_schema_change) {
        if (OB_FAIL(get_schema_version_in_inner_table(sql_client, schema_status, schema_version))) {
          LOG_WARN("fail to get schema version in inner table", K(ret));
        } else if (OB_FAIL(try_update_split_schema_version_v2(schema_status, tenant_id))) {
          LOG_WARN("fail to update split schema version v2", K(ret), K(tenant_id));
        } else if (OB_FAIL(
                       check_sys_schema_change(sql_client, local_schema_version, schema_version, sys_schema_change))) {
          LOG_WARN("check_sys_schema_change failed", K(schema_version), K(ret));
        } else if (sys_schema_change) {
          const int64_t sys_formal_version = std::max(core_schema_version, schema_version);
          int64_t publish_version = 0;
          if (OB_FAIL(ObSchemaService::gen_sys_temp_version(sys_formal_version, publish_version))) {
            LOG_WARN("gen_sys_temp_version failed", K(sys_formal_version), K(ret));
          } else if (OB_FAIL(try_fetch_publish_sys_schemas(
                         schema_status, schema_version, publish_version, sql_client, sys_schema_change))) {
            LOG_WARN("try_fetch_publish_sys_schemas failed",
                K(schema_status),
                K(schema_version),
                K(publish_version),
                K(ret));
          } else {
          }
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          int64_t new_core_schema_version = 0;
          if (OB_SUCCESS !=
              (temp_ret = schema_service_->get_core_version(sql_client, frozen_version, new_core_schema_version))) {
            // Failed to obtain core_version, to be safe, trigger a retry
            core_schema_change = true;
            sys_schema_change = true;
            LOG_WARN("get_core_version failed, need retry", K(frozen_version), K(ret), K(temp_ret));
          } else if (new_core_schema_version != core_schema_version) {
            core_schema_change = true;
            LOG_WARN("core schema change during refresh sys schema",
                K(core_schema_version),
                K(new_core_schema_version),
                K(ret));
          }
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && !core_schema_change && !sys_schema_change) {
        const int64_t fetch_version = std::max(core_schema_version, schema_version);
        if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          LOG_WARN("fail to get schema_mgr_for_cache", K(ret), K(schema_status));
        } else if (OB_ISNULL(schema_mgr_for_cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema mgr for cache is null", K(ret));
        } else if (OB_FAIL(schema_service_->get_increment_schema_operations(
                       schema_status, local_schema_version, fetch_version, sql_client, schema_operations))) {
          LOG_WARN("get_increment_schema_operations failed", K(local_schema_version), K(fetch_version), K(ret));
        } else if (schema_operations.count() > 0) {
          // new cache
          AllSchemaKeys all_keys;
          if (OB_FAIL(replay_log(*schema_mgr_for_cache, schema_operations, all_keys))) {
            LOG_WARN("replay_log failed", K(schema_operations), K(ret));
          } else if (OB_FAIL(update_schema_mgr(
                         sql_client, schema_status, *schema_mgr_for_cache, fetch_version, all_keys))) {
            LOG_WARN("update schema mgr failed", K(ret));
          }
        } else {
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(reload_mock_schema_info(*schema_mgr_for_cache))) {
            LOG_WARN("fail to reload mock schema info", K(ret), K(tenant_id));
          }
        }

        if (OB_SUCC(ret)) {
          schema_mgr_for_cache->set_schema_version(fetch_version);
          if (OB_FAIL(publish_schema(tenant_id))) {
            LOG_WARN("publish_schema failed", K(ret));
          } else {
            schema_version_ = schema_version;
            core_schema_version_ = core_schema_version;
            LOG_INFO("change schema version", K_(core_schema_version), K_(schema_version));
            break;
          }
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of sys table schema change, go to suitable pos,
          // if during check core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS !=
              (temp_ret = check_core_or_sys_schema_change(
                   sql_client, core_schema_version, schema_version, core_schema_change, sys_schema_change))) {
            // Locating failed. To prevent tenant-level system table schema and system table index schema
            // from missing updates, a retry needs to be triggered
            core_schema_change = true;
            sys_schema_change = true;
            LOG_WARN("check_core_or_sys_schema_change failed, need retry",
                K(core_schema_version),
                K(schema_version),
                K(ret),
                K(temp_ret));
          }
          ret = OB_SUCCESS;
        }
      }
      ++retry_count;
    }
  }

  return ret;
}

int ObServerSchemaService::set_timeout_ctx(ObTimeoutCtx& ctx)
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
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }

  return ret;
}

int ObServerSchemaService::refresh_increment_schema(int64_t expected_version)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  observer::ObUseWeakGuard use_weak_guard;
  const ObRefreshSchemaStatus dummy_schema_status;  // for compatible
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (expected_version > 0 &&
             OB_FAIL(schema_service_->can_read_schema_version(dummy_schema_status, expected_version))) {
    LOG_WARN("incremant schema is not readable now, ignore to refresh this time", K(ret));
  } else if (OB_ISNULL(schema_mgr_for_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_for_cache_ is null", K(ret));
  } else {
    ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
    bool core_schema_change = true;
    bool sys_schema_change = true;
    int64_t local_schema_version = schema_mgr_for_cache_->get_schema_version();
    int64_t core_schema_version = 0;
    int64_t schema_version = OB_INVALID_VERSION;
    const int64_t frozen_version = -1;
    int64_t retry_count = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("observer is stopping", K(ret));
        break;
      } else if (retry_count > 0) {
        LOG_WARN("refresh_increment_schema failed", K(retry_count));
        if (OB_FAIL(set_timeout_ctx(ctx))) {
          LOG_WARN("[REFRESH_SCHEMA] fail to set timeout ctx, need reset schema version",
              K(ret),
              "last_schema_version",
              local_schema_version,
              "cur_schema_version",
              schema_mgr_for_cache_->get_schema_version());
          schema_mgr_for_cache_->set_schema_version(local_schema_version);
          break;
        }
      }
      ObISQLClient& sql_client = *sql_proxy_;
      if (OB_SUCC(ret) && core_schema_change) {
        if (OB_FAIL(schema_service_->get_core_version(sql_client, frozen_version, core_schema_version))) {
          LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
        } else if (core_schema_version > local_schema_version) {
          int64_t publish_version = OB_INVALID_INDEX;
          if (OB_FAIL(ObSchemaService::gen_core_temp_version(core_schema_version, publish_version))) {
            LOG_WARN("gen_core_temp_version failed", K(core_schema_version), K(ret));
          } else if (OB_FAIL(try_fetch_publish_core_schemas(
                         dummy_schema_status, core_schema_version, publish_version, sql_client, core_schema_change))) {
            LOG_WARN("try_fetch_publish_core_schemas failed", K(core_schema_version), K(publish_version), K(ret));
          } else {
          }
        } else {
          core_schema_change = false;
        }

        if (OB_FAIL(ret)) {
          core_schema_change = true;
          sys_schema_change = true;
          LOG_WARN("refresh core table schema failed, need retry", K(frozen_version), K(ret));
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && !core_schema_change && sys_schema_change) {
        if (OB_FAIL(get_schema_version_in_inner_table(sql_client, dummy_schema_status, schema_version))) {
          LOG_WARN("fail to get schema version in inner table", K(ret));
        } else if (OB_FAIL(
                       check_sys_schema_change(sql_client, local_schema_version, schema_version, sys_schema_change))) {
          LOG_WARN("check_sys_schema_change failed", K(schema_version), K(ret));
        } else if (sys_schema_change) {
          const int64_t sys_formal_version = std::max(core_schema_version, schema_version);
          int64_t publish_version = 0;
          if (OB_FAIL(ObSchemaService::gen_sys_temp_version(sys_formal_version, publish_version))) {
            LOG_WARN("gen_sys_temp_version failed", K(sys_formal_version), K(ret));
          } else if (OB_FAIL(try_fetch_publish_sys_schemas(
                         dummy_schema_status, schema_version, publish_version, sql_client, sys_schema_change))) {
            LOG_WARN("try_fetch_publish_sys_schemas failed", K(schema_version), K(publish_version), K(ret));
          } else {
          }
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          int64_t new_core_schema_version = 0;
          if (OB_SUCCESS !=
              (temp_ret = schema_service_->get_core_version(sql_client, frozen_version, new_core_schema_version))) {
            // Failed to obtain core_version, to be safe, trigger a retry
            core_schema_change = true;
            sys_schema_change = true;
            LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
          } else if (new_core_schema_version != core_schema_version) {
            core_schema_change = true;
            LOG_WARN("core schema change during refresh sys schema",
                K(core_schema_version),
                K(new_core_schema_version),
                K(ret));
          }
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && !core_schema_change && !sys_schema_change) {
        const int64_t fetch_version = std::max(core_schema_version, schema_version);
        if (OB_FAIL(schema_service_->get_increment_schema_operations(
                dummy_schema_status, local_schema_version, fetch_version, sql_client, schema_operations))) {
          LOG_WARN("get_increment_schema_operations failed", K(local_schema_version), K(fetch_version), K(ret));
        } else if (schema_operations.count() > 0) {
          // new cache
          AllSchemaKeys all_keys;
          if (OB_FAIL(replay_log(*schema_mgr_for_cache_, schema_operations, all_keys))) {
            LOG_WARN("replay_log failed", K(schema_operations), K(ret));
          } else if (OB_FAIL(update_schema_mgr(sql_client, *schema_mgr_for_cache_, fetch_version, all_keys))) {
            LOG_WARN("update schema mgr failed", K(ret));
          }
        } else {
        }

        if (OB_SUCC(ret)) {
          ret = E(EventTable::EN_REFRESH_INCREMENT_SCHEMA_PHASE_THREE_FAILED) OB_SUCCESS;
        }

        if (OB_SUCC(ret)) {
          schema_mgr_for_cache_->set_schema_version(fetch_version);
          if (OB_FAIL(publish_schema())) {
            LOG_WARN("publish_schema failed", K(ret));
          } else {
            schema_version_ = schema_version;
            core_schema_version_ = core_schema_version;
            LOG_INFO("change schema version", K_(core_schema_version), K_(schema_version));
            break;
          }
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of sys table schema change, go to suitable pos,
          // if during check core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS !=
              (temp_ret = check_core_or_sys_schema_change(
                   sql_client, core_schema_version, schema_version, core_schema_change, sys_schema_change))) {
            // Locating failed. To prevent tenant-level system table schema and system table index schema
            // from missing updates, a retry needs to be triggered
            core_schema_change = true;
            sys_schema_change = true;
            LOG_WARN("check_core_or_sys_schema_change failed", K(core_schema_version), K(schema_version), K(ret));
          }
          ret = OB_SUCCESS;
        }
      }
      ++retry_count;
    }
  }

  return ret;
}

int ObServerSchemaService::get_tenant_all_increment_schema_ids(const uint64_t tenant_id, const int64_t base_version,
    const int64_t new_schema_version, const ObSchemaOperationCategory& schema_category, ObIArray<uint64_t>& schema_ids)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus refresh_schema_status;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (GCTX.is_schema_splited()) {
    refresh_schema_status.tenant_id_ = tenant_id;
    if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
      ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
      if (OB_ISNULL(schema_status_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status_proxy is null", K(ret));
      } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, refresh_schema_status))) {
        LOG_WARN("fail to get refresh schema status", K(ret), K(tenant_id));
      } else {
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (new_schema_version > 0 &&
             OB_FAIL(schema_service_->can_read_schema_version(refresh_schema_status, new_schema_version))) {
    LOG_WARN("incremant schema is not readable now, ignore to refresh this time", K(ret));
  } else {
    observer::ObUseWeakGuard use_weak_guard;
    ObHashSet<uint64_t> schema_set;
    ObISQLClient& sql_client = *sql_proxy_;
    ObSchemaService::SchemaOperationSetWithAlloc schema_operations;
    int64_t schema_op_begin = 0;
    int64_t schema_op_end = 0;
    switch (schema_category) {
      case OB_DDL_TABLE_OPERATION:
        schema_op_begin = OB_DDL_TABLE_OPERATION_BEGIN;
        schema_op_end = OB_DDL_TABLE_OPERATION_END;
        break;
      case OB_DDL_TENANT_OPERATION:
        schema_op_begin = OB_DDL_TENANT_OPERATION_BEGIN;
        schema_op_end = OB_DDL_TENANT_OPERATION_END;
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported", K(schema_category));
        break;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service_->get_increment_schema_operations(
                   refresh_schema_status, base_version, new_schema_version, sql_client, schema_operations))) {
      LOG_WARN("fail to get increment schema operations", K(ret));
    } else if (OB_FAIL(schema_set.create(schema_operations.count()))) {
      LOG_WARN("fail to create table set", K(ret), "bucket num", schema_operations.count());
    } else {
      const bool is_over_write = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < schema_operations.count(); ++i) {
        const ObSchemaOperation& schema_operation = schema_operations.at(i);
        if (schema_operation.op_type_ > schema_op_begin && schema_operation.op_type_ < schema_op_end) {
          switch (schema_category) {
            case OB_DDL_TABLE_OPERATION:
              if (extract_tenant_id(schema_operation.table_id_) == tenant_id) {
                if (OB_FAIL(schema_set.set_refactored_1(schema_operation.table_id_, is_over_write))) {
                  LOG_WARN("fail to set refactored", K(ret));
                }
              }
              break;
            case OB_DDL_TENANT_OPERATION:
              if (schema_operation.tenant_id_ == tenant_id) {
                if (OB_FAIL(schema_set.set_refactored_1(schema_operation.tenant_id_, is_over_write))) {
                  LOG_WARN("fail to set refactored", K(ret));
                }
              }
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not supported", K(schema_category));
              break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        schema_ids.reuse();
        if (schema_set.size() > 0) {
          if (OB_FAIL(schema_ids.reserve(schema_set.size()))) {
            LOG_WARN("fail to reverse table id array", K(ret), "size", schema_set.size());
          }
          for (typename common::hash::ObHashSet<uint64_t>::const_iterator iter = schema_set.begin();
               OB_SUCC(ret) && iter != schema_set.end();
               ++iter) {
            if (OB_FAIL(schema_ids.push_back(iter->first))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
    }
    if (schema_set.created()) {
      schema_set.destroy();
    }
  }
  return ret;
}

int ObServerSchemaService::refresh_full_schema(int64_t expected_version)
{
  int ret = OB_SUCCESS;
  const ObRefreshSchemaStatus dummy_schema_status;  // for compatible

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    UNUSED(expected_version);
    const int64_t frozen_version = -1;
    int64_t retry_count = 0;
    bool core_schema_change = true;
    bool sys_schema_change = true;
    int64_t core_schema_version = 0;
    int64_t new_core_schema_version = 0;
    int64_t schema_version = 0;
    while (OB_SUCC(ret) && (core_schema_change || sys_schema_change)) {
      if (retry_count > 0) {
        LOG_WARN("refresh_full_schema failed, retry", K(retry_count));
      }
      ObISQLClient& sql_client = *sql_proxy_;
      // refresh core table schemas
      if (OB_SUCC(ret) && core_schema_change) {
        // already have the core table version we need to publish
        if ((RST_FULL_SCHEMA_IDS == refresh_schema_type_) && (0 != core_schema_version_)) {
          core_schema_version = core_schema_version_;
          core_schema_change = false;
        } else {
          if (OB_FAIL(schema_service_->get_core_version(sql_client, frozen_version, core_schema_version))) {
            LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
          } else {
            // for core table schema, we publis as core_temp_version
            int64_t publish_version = 0;
            if (OB_FAIL(ObSchemaService::gen_core_temp_version(core_schema_version, publish_version))) {
              LOG_WARN("gen_core_temp_version failed", K(core_schema_version), K(ret));
            } else if (OB_FAIL(try_fetch_publish_core_schemas(dummy_schema_status,
                           core_schema_version,
                           publish_version,
                           sql_client,
                           core_schema_change))) {
              LOG_WARN("try_fetch_publish_core_schemas failed", K(core_schema_version), K(publish_version), K(ret));
            }
          }
        }
      }

      // refresh sys table schemas
      if (OB_SUCC(ret) && !core_schema_change && sys_schema_change) {
        if ((RST_FULL_SCHEMA_IDS == refresh_schema_type_) && (0 != schema_version_)) {
          schema_version = schema_version_;
          sys_schema_change = false;
        } else {
          if (OB_FAIL(get_schema_version_in_inner_table(sql_client, dummy_schema_status, schema_version))) {
            LOG_WARN("fail to get schema version in inner table", K(ret));
          } else {
            // for sys table schema, we publish as sys_temp_version
            const int64_t sys_formal_version = std::max(core_schema_version, schema_version);
            int64_t publish_version = 0;
            if (OB_FAIL(ObSchemaService::gen_sys_temp_version(sys_formal_version, publish_version))) {
              LOG_WARN("gen_sys_temp_version failed", K(sys_formal_version), K(ret));
            } else if (OB_FAIL(try_fetch_publish_sys_schemas(
                           dummy_schema_status, schema_version, publish_version, sql_client, sys_schema_change))) {
              LOG_WARN("try_fetch_publish_sys_schemas failed", K(schema_version), K(publish_version), K(ret));
            }
          }
        }

        if (OB_FAIL(ret)) {
          // check whether failed because of core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          if ((RST_FULL_SCHEMA_ALL == refresh_schema_type_) || (0 == core_schema_version_)) {
            if (OB_SUCCESS !=
                (temp_ret = schema_service_->get_core_version(sql_client, frozen_version, new_core_schema_version))) {
              LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
            } else if (new_core_schema_version != core_schema_version) {
              core_schema_change = true;
              LOG_WARN("core schema change during refresh sys schema",
                  K(core_schema_version),
                  K(new_core_schema_version),
                  K(ret));
              ret = OB_SUCCESS;
            }
          }
        }
      }

      // refresh full normal schema by schema_version
      if (OB_SUCC(ret) && !core_schema_change && !sys_schema_change) {
        const int64_t fetch_version = std::max(core_schema_version, schema_version);
        if (OB_FAIL(refresh_full_normal_schema_v2(sql_client, fetch_version))) {
          LOG_WARN("refresh_full_normal_schema failed", K(fetch_version), K(ret));
        } else {
          const int64_t publish_version = std::max(core_schema_version, schema_version);
          schema_mgr_for_cache_->set_schema_version(publish_version);
          if (OB_FAIL(publish_schema())) {
            LOG_WARN("publish_schema failed", K(ret));
          } else {
            if (0 == core_schema_version_) {
              core_schema_version_ = core_schema_version;
              schema_version_ = schema_version;
            }
            LOG_INFO("publish full normal schema by schema_version succeed",
                K(publish_version),
                K(core_schema_version),
                K(schema_version));
          }
        }
        if (OB_FAIL(ret)) {
          // check whether failed because of sys table schema change, go to suitable pos,
          // if during check core table schema change, go to suitable pos
          int temp_ret = OB_SUCCESS;
          if ((RST_FULL_SCHEMA_ALL == refresh_schema_type_) || (0 == core_schema_version_)) {
            if (OB_SUCCESS !=
                (temp_ret = check_core_or_sys_schema_change(
                     sql_client, core_schema_version, schema_version, core_schema_change, sys_schema_change))) {
              LOG_WARN("check_core_or_sys_schema_change failed", K(core_schema_version), K(schema_version), K(ret));
            } else if (core_schema_change || sys_schema_change) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
      ++retry_count;
    }
    // It must be reset before each refresh schema to prevent ddl from being in progress,
    // but refresh full may have added some tables. And the latter table was deleted again, at this time refresh
    // will not delete this table in the cache
    if (OB_FAIL(ret)) {
      LOG_INFO("[REFRESH_SCHEMA] refresh full schema failed, do some clear", K(ret));
      schema_mgr_for_cache_->reset();
      all_core_schema_init_ = false;  // reset all
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = init_basic_schema())) {
        LOG_WARN("init basic schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::try_fetch_publish_core_schemas(const ObRefreshSchemaStatus& schema_status,
    const int64_t core_schema_version, const int64_t publish_version, ObISQLClient& sql_client,
    bool& core_schema_change)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(schema_status));
  } else {
    ObArray<ObTableSchema> core_schemas;
    ObArray<uint64_t> core_table_ids;
    const int64_t frozen_version = -1;
    int64_t new_core_schema_version = 0;
    if (OB_FAIL(schema_service_->get_core_table_schemas(sql_client, core_schemas))) {
      LOG_WARN("get_core_table_schemas failed", K(core_table_ids), K(ret));
    } else if (OB_FAIL(schema_service_->get_core_version(sql_client, frozen_version, new_core_schema_version))) {
      LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
    } else if (new_core_schema_version != core_schema_version) {
      core_schema_change = true;
      LOG_WARN("core schema change", K(core_schema_version), K(new_core_schema_version));
    } else {
      // core schema don't change, publish core schemas
      core_schema_change = false;
      ObArray<ObTableSchema*> core_tables;
      for (int64_t i = 0; i < core_schemas.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(core_tables.push_back(&core_schemas.at(i)))) {
          LOG_WARN("add table schema failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObSchemaMgr* schema_mgr_for_cache = NULL;
        ObArray<ObSimpleTableSchemaV2> simple_core_schemas;
        if (OB_INVALID_TENANT_ID == tenant_id) {
          schema_mgr_for_cache = schema_mgr_for_cache_;
        } else {
          if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
            LOG_WARN("fail to get schema mgr for cache", K(ret), K(tenant_id));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(schema_mgr_for_cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_mgr_for_cache is null", K(ret));
        } else if (OB_FAIL(update_schema_cache(core_tables))) {
          LOG_WARN("failed to update schema cache", K(ret));
        } else if (OB_FAIL(convert_to_simple_schema(core_schemas, simple_core_schemas))) {
          LOG_WARN("convert to simple schema failed", K(ret));
        } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_core_schemas))) {
          LOG_WARN("add tables failed", K(ret));
        } else if (FALSE_IT(schema_mgr_for_cache->set_schema_version(publish_version))) {
        } else if (OB_FAIL(publish_schema(tenant_id))) {
          LOG_WARN("publish_schema failed", K(ret));
        } else {
          LOG_INFO("[REFRESH_SCHEMA] refresh core table schema succeed",
              K(publish_version),
              K(core_schema_version),
              K(schema_mgr_for_cache->get_schema_version()));
        }
      }
    }
  }

  return ret;
}

int ObServerSchemaService::try_fetch_publish_sys_schemas(const ObRefreshSchemaStatus& schema_status,
    const int64_t schema_version, const int64_t publish_version, common::ObISQLClient& sql_client,
    bool& sys_schema_change)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID != tenant_id && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(schema_status));
  } else {
    ObArenaAllocator allocator1(ObModIds::OB_SCHEMA_SYS_SCHEMA);
    ObArenaAllocator allocator2(ObModIds::OB_SCHEMA_TENANT_SYS_SCHEMA);
    ObArray<ObTableSchema*> sys_schemas;
    ObArray<uint64_t> sys_table_ids;
    int64_t new_schema_version = 0;
    if (!check_inner_stat()) {
      ret = OB_NOT_INIT;
      LOG_WARN("check_inner_stat failed", K(ret));
    } else if (OB_FAIL(get_table_ids(sys_table_schema_creators, sys_table_ids))) {
      LOG_WARN("get_table_ids failed", K(ret));
    } else if (OB_FAIL(
                   sys_table_ids.push_back(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID)))) {
      LOG_WARN("add index table id failed", K(ret));
    } else if (OB_FAIL(sys_table_ids.push_back(
                   combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_V2_HISTORY_IDX_DATA_TABLE_ID_TID)))) {
      LOG_WARN("add index table id failed", K(ret));
    } else if (OB_FAIL(schema_service_->get_sys_table_schemas(sys_table_ids, sql_client, allocator1, sys_schemas))) {
      LOG_WARN("get_batch_table_schema failed", K(sys_table_ids), K(ret));
    } else if (OB_FAIL(get_schema_version_in_inner_table(sql_client, schema_status, new_schema_version))) {
      LOG_WARN("fail to get schema version in inner table", K(ret));
    } else if (OB_FAIL(check_sys_schema_change(sql_client, schema_version, new_schema_version, sys_schema_change))) {
      LOG_WARN("check_sys_schema_change failed", K(schema_version), K(new_schema_version), K(ret));
    } else if (sys_schema_change) {
      LOG_WARN("sys schema change during refresh full schema", K(schema_version), K(new_schema_version));
    } else if (!sys_schema_change) {
      ObSchemaMgr* schema_mgr_for_cache = NULL;
      ObArray<ObSimpleTableSchemaV2> simple_sys_schemas;
      if (OB_INVALID_TENANT_ID == tenant_id) {
        schema_mgr_for_cache = schema_mgr_for_cache_;
      } else {
        if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          LOG_WARN("fail to get schema mgr for cache", K(ret), K(tenant_id));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(schema_mgr_for_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_mgr_for_cache is null", K(ret));
      } else if (OB_FAIL(update_schema_cache(sys_schemas))) {
        LOG_WARN("failed to update schema cache", K(ret));
      } else if (OB_FAIL(convert_to_simple_schema(sys_schemas, simple_sys_schemas))) {
        LOG_WARN("convert to simple schema failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_sys_schemas))) {
        LOG_WARN("add tables failed", K(ret));
      } else if (FALSE_IT(schema_mgr_for_cache->set_schema_version(publish_version))) {
      } else if (OB_FAIL(publish_schema(tenant_id))) {
        LOG_WARN("publish_schema failed", K(ret));
      } else {
        LOG_INFO("[REFRESH_SCHEMA] refresh sys table schema succeed",
            K(publish_version),
            K(schema_version),
            K(schema_mgr_for_cache->get_schema_version()));
      }
    }
  }
  return ret;
}

int ObServerSchemaService::add_tenant_schema_to_cache(
    ObISQLClient& sql_client, const uint64_t tenant_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(schema_version));
  } else {
    ObTenantSchema new_tenant;
    ObArray<ObTenantSchema> tenant_schema_array;
    if (OB_FAIL(schema_service_->get_tenant_schema(schema_version, sql_client, tenant_id, new_tenant))) {
      LOG_WARN("failed to get new tenant schema", K(ret));
    } else if (OB_FAIL(tenant_schema_array.push_back(new_tenant))) {
      LOG_WARN("failed to add new tenant schema", K(ret));
    } else if (OB_FAIL(update_schema_cache(tenant_schema_array))) {
      LOG_WARN("failed to update schema cache", K(ret));
    }
  }

  return ret;
}

int ObServerSchemaService::add_sys_variable_schema_to_cache(ObISQLClient& sql_client,
    const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_ID == tenant_id || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
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

int ObServerSchemaService::update_table_columns(const uint64_t tenant_id, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  int64_t column_count = table_schema.get_column_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    ObColumnSchemaV2* column = NULL;
    if (NULL == (column = table_schema.get_column_schema_by_idx(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret));
    } else {
      column->set_tenant_id(tenant_id);
      column->set_table_id(table_schema.get_table_id());
    }
  }
  return ret;
}

int ObServerSchemaService::refresh_full_normal_schema_v2(ObISQLClient& sql_client, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const ObRefreshSchemaStatus dummy_schema_status;  // for compatible

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version));
  } else if (OB_ISNULL(schema_mgr_for_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_for_cache_ is null", K(ret));
  } else {
    ObArray<ObSimpleTenantSchema> simple_tenants;
    ObArray<ObDropTenantInfo> drop_tenant_infos;
    if (OB_FAIL(schema_service_->get_all_tenants(sql_client, schema_version, simple_tenants))) {
      LOG_WARN("get all tenant schema failed", K(ret), K(schema_version));
    } else if (simple_tenants.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant counts error", K(ret), K(simple_tenants.count()));
    } else if (OB_FAIL(schema_service_->get_drop_tenant_infos(sql_client, schema_version, drop_tenant_infos))) {
      LOG_WARN("fail to get drop tenant infos", K(ret), K(schema_version));
    } else {
      ObArray<ObSimpleUserSchema> simple_users;
      ObArray<ObSimpleDatabaseSchema> simple_databases;
      ObArray<ObSimpleTablegroupSchema> simple_tablegroups;
      ObArray<ObSimpleTableSchemaV2> simple_tables;
      ObArray<ObSimpleOutlineSchema> simple_outlines;
      ObArray<ObSimpleSynonymSchema> simple_synonyms;
      ObArray<ObDBPriv> db_privs;
      ObArray<ObSysPriv> sys_privs;
      ObArray<ObTablePriv> table_privs;
      ObArray<ObObjPriv> obj_privs;
      ObArray<ObSimpleUDFSchema> simple_udfs;
      ObArray<ObSequenceSchema> simple_sequences;
      ObArray<ObProfileSchema> simple_profiles;
      ObSimpleSysVariableSchema simple_sys_variable;
      ObArray<ObDbLinkSchema> simple_dblinks;
      FOREACH_CNT_X(simple_tenant, simple_tenants, OB_SUCC(ret))
      {
        const uint64_t tenant_id = simple_tenant->get_tenant_id();

        // note: Adjust the position of this code carefully
        // add sys tenant schema to cache
        if (OB_SYS_TENANT_ID == tenant_id) {
          if (OB_FAIL(add_tenant_schema_to_cache(sql_client, tenant_id, schema_version))) {
            LOG_WARN("add tenant schema to cache failed", K(ret), K(tenant_id), K(schema_version));
          } else if (OB_FAIL(add_sys_variable_schema_to_cache(
                         sql_client, dummy_schema_status, tenant_id, schema_version))) {
            LOG_WARN("add sys variable schema to cache failed", K(ret), K(tenant_id), K(schema_version));
          } else if (OB_FAIL(schema_mgr_for_cache_->add_drop_tenant_infos(drop_tenant_infos))) {
            LOG_WARN("add drop tenant infos failed", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(schema_mgr_for_cache_->add_tenant(*simple_tenant))) {
            LOG_WARN("add tenant failed", K(ret), K(*simple_tenant));
          } else if (OB_FAIL(schema_service_->get_sys_variable(
                         sql_client, dummy_schema_status, tenant_id, schema_version, simple_sys_variable))) {
            LOG_WARN("get all sys variables failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_users(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_users))) {
            LOG_WARN("get all users failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_databases(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_databases))) {
            LOG_WARN("get all databases failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_tablegroups(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_tablegroups))) {
            LOG_WARN("get all tablegroups failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_tables(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_tables))) {
            LOG_WARN("get all table schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_outlines(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_outlines))) {
            LOG_WARN("get all outline schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_db_privs(
                         sql_client, dummy_schema_status, schema_version, tenant_id, db_privs))) {
            LOG_WARN("get all db priv schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_table_privs(
                         sql_client, dummy_schema_status, schema_version, tenant_id, table_privs))) {
            LOG_WARN("get all table priv failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_obj_privs(
                         sql_client, dummy_schema_status, schema_version, tenant_id, obj_privs))) {
            LOG_WARN("get all obj priv failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_synonyms(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_synonyms))) {
            LOG_WARN("get all synonym schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_udfs(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_udfs))) {
            LOG_WARN("get all udfs schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_sequences(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_sequences))) {
            LOG_WARN("get all sequences schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_profiles(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_profiles))) {
            LOG_WARN("get all profile schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_sys_privs(
                         sql_client, dummy_schema_status, schema_version, tenant_id, sys_privs))) {
            LOG_WARN("get all sys priv schema failed", K(ret), K(schema_version), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_all_dblinks(
                         sql_client, dummy_schema_status, schema_version, tenant_id, simple_dblinks))) {
            LOG_WARN("get all dblink schema failed", K(ret), K(schema_version), K(tenant_id));
          }

          // add full schema for cache
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(schema_mgr_for_cache_->sys_variable_mgr_.add_sys_variable(simple_sys_variable))) {
            LOG_WARN("add sys variables failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->add_users(simple_users))) {
            LOG_WARN("add users failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->add_databases(simple_databases))) {
            LOG_WARN("add databases failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->add_tablegroups(simple_tablegroups))) {
            LOG_WARN("add tablegroups failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->add_tables(simple_tables))) {
            LOG_WARN("add tables failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->outline_mgr_.add_outlines(simple_outlines))) {
            LOG_WARN("add outlines failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->synonym_mgr_.add_synonyms(simple_synonyms))) {
            LOG_WARN("add synonyms failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->priv_mgr_.add_db_privs(db_privs))) {
            LOG_WARN("add db privs failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->priv_mgr_.add_table_privs(table_privs))) {
            LOG_WARN("add table privs failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->priv_mgr_.add_obj_privs(obj_privs))) {
            LOG_WARN("add obj privs failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->udf_mgr_.add_udfs(simple_udfs))) {
            LOG_WARN("add table privs failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->sequence_mgr_.add_sequences(simple_sequences))) {
            LOG_WARN("add sequence failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->profile_mgr_.add_profiles(simple_profiles))) {
            LOG_WARN("add profile failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->priv_mgr_.add_sys_privs(sys_privs))) {
            LOG_WARN("add sys privs failed", K(ret));
          } else if (OB_FAIL(schema_mgr_for_cache_->dblink_mgr_.add_dblinks(simple_dblinks))) {
            LOG_WARN("add dblinks failed", K(ret));
          }

          LOG_INFO("add schemas for tenant finish",
              K(tenant_id),
              K(schema_version),
              "users",
              simple_users.count(),
              "databases",
              simple_databases.count(),
              "tablegroups",
              simple_tablegroups.count(),
              "tables",
              simple_tables.count(),
              "outlines",
              simple_outlines.count(),
              "synonyms",
              simple_synonyms.count(),
              "db_privs",
              db_privs.count(),
              "table_privs",
              table_privs.count(),
              "udfs",
              simple_udfs.count(),
              "sequences",
              simple_sequences.count(),
              "profiles",
              simple_profiles.count(),
              "sys_privs",
              sys_privs.count());
          LOG_INFO("add schemas for tenant finish", K(tenant_id), K(schema_version), "dblinks", simple_dblinks.count());
        }
      }
    }
    // process tenant space table
    if (OB_SUCC(ret)) {
      ObArray<uint64_t> tenant_ids;
      ObArray<uint64_t> normal_tenant_ids;
      if (OB_FAIL(schema_mgr_for_cache_->get_tenant_ids(tenant_ids))) {
        LOG_WARN("get tenant ids failed", K(ret));
      } else {
        FOREACH_CNT_X(tmp_tenant_id, tenant_ids, OB_SUCC(ret))
        {
          if (OB_SYS_TENANT_ID != *tmp_tenant_id) {
            if (OB_FAIL(normal_tenant_ids.push_back(*tmp_tenant_id))) {
              LOG_WARN("push back tenant id failed", K(ret), "tenant id", *tmp_tenant_id);
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(process_tenant_space_table(*schema_mgr_for_cache_, normal_tenant_ids))) {
            LOG_WARN("process tenant space table failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObArenaAllocator allocator;
        ObArray<ObTableSchema> core_tables;
        ObArray<ObTableSchema*> sys_tables;
        ObSEArray<uint64_t, 16> sys_table_ids;
        if (OB_FAIL(get_table_ids(sys_table_schema_creators, sys_table_ids))) {
          LOG_WARN("get_table_ids failed", K(ret));
        } else if (OB_FAIL(renew_core_and_sys_table_schemas(sql_client,
                       allocator,
                       core_tables,
                       sys_tables,
                       NULL,  // core_table_ids
                       &sys_table_ids))) {
          LOG_WARN("renew core and sys table schemas failed", K(ret));
        } else if (OB_FAIL(update_core_and_sys_schemas_in_cache(*schema_mgr_for_cache_, core_tables, sys_tables))) {
          LOG_WARN("update core and sys schemas in cache faield", K(ret));
        }
      }
    }
  }

  return ret;
}

// new schema full refresh
int ObServerSchemaService::refresh_tenant_full_normal_schema(
    ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema_status.tenant_id_;
  ObSchemaMgr* schema_mgr_for_cache = NULL;

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
    if (OB_SYS_TENANT_ID == tenant_id) {
      ObArray<ObSimpleTenantSchema> simple_tenants;
      ObArray<ObDropTenantInfo> drop_tenant_infos;
      if (OB_FAIL(schema_service_->get_all_tenants(sql_client, schema_version, simple_tenants))) {
        LOG_WARN("get all tenant schema failed", K(ret), K(schema_version));
      } else if (simple_tenants.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant counts error", K(ret), K(simple_tenants.count()));
      } else if (OB_FAIL(schema_service_->get_drop_tenant_infos(sql_client, schema_version, drop_tenant_infos))) {
        LOG_WARN("fail to get drop tenant infos", K(ret), K(schema_version));
      } else {
        FOREACH_CNT_X(simple_tenant, simple_tenants, OB_SUCC(ret))
        {
          const uint64_t tmp_tenant_id = simple_tenant->get_tenant_id();
          // note: Adjust the position of this code carefully
          // add sys tenant schema to cache
          if (OB_FAIL(schema_mgr_for_cache->add_tenant(*simple_tenant))) {
            LOG_WARN("add tenant failed", K(ret), K(*simple_tenant));
          } else if (OB_SYS_TENANT_ID == tmp_tenant_id) {
            if (OB_FAIL(add_tenant_schema_to_cache(sql_client, tmp_tenant_id, schema_version))) {
              LOG_WARN("add tenant schema to cache failed", K(ret), K(tmp_tenant_id), K(schema_version));
            } else if (OB_FAIL(add_sys_variable_schema_to_cache(
                           sql_client, schema_status, tmp_tenant_id, schema_version))) {
              LOG_WARN("add sys variable schema to cache failed", K(ret), K(tmp_tenant_id), K(schema_version));
            } else if (OB_FAIL(schema_mgr_for_cache->add_drop_tenant_infos(drop_tenant_infos))) {
              LOG_WARN("add drop tenant infos failed", K(ret));
            }
          } else if (OB_SYS_TENANT_ID != tmp_tenant_id) {
            // When the system tenant flashes the DDL of the newly created tenant,
            // try to initialize the relevant data structure
            if (OB_FAIL(init_schema_struct(tmp_tenant_id))) {
              LOG_WARN("fail to init schema struct", K(ret), K(tmp_tenant_id), K(schema_status));
            } else if (OB_FAIL(init_tenant_basic_schema(tmp_tenant_id))) {
              LOG_WARN("fail to init basic schema struct", K(ret), K(tmp_tenant_id), K(schema_status));
            } else if (OB_FAIL(init_multi_version_schema_struct(tmp_tenant_id))) {
              LOG_WARN("fail to init multi version schema struct", K(ret), K(tmp_tenant_id));
            }
          }
        }
      }
    } else {
      // Ordinary tenant schema refreshing relies on the system tenant to refresh the tenant and
      // initialize the related memory structure
    }

    if (OB_SUCC(ret)) {
      ObArray<ObSimpleUserSchema> simple_users;
      ObArray<ObSimpleDatabaseSchema> simple_databases;
      ObArray<ObSimpleTablegroupSchema> simple_tablegroups;
      ObArray<ObSimpleTableSchemaV2> simple_tables;
      ObArray<ObSimpleOutlineSchema> simple_outlines;
      ObArray<ObSimpleSynonymSchema> simple_synonyms;
      ObArray<ObDBPriv> db_privs;
      ObArray<ObSysPriv> sys_privs;
      ObArray<ObTablePriv> table_privs;
      ObArray<ObObjPriv> obj_privs;
      ObArray<ObSimpleUDFSchema> simple_udfs;
      ObArray<ObSequenceSchema> simple_sequences;
      ObArray<ObProfileSchema> simple_profiles;
      ObSimpleSysVariableSchema simple_sys_variable;
      ObArray<ObDbLinkSchema> simple_dblinks;

      if (OB_FAIL(schema_service_->get_sys_variable(
              sql_client, schema_status, tenant_id, schema_version, simple_sys_variable))) {
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
                     sql_client, schema_status, schema_version, tenant_id, simple_tables))) {
        LOG_WARN("get all table schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_outlines(
                     sql_client, schema_status, schema_version, tenant_id, simple_outlines))) {
        LOG_WARN("get all outline schema failed", K(ret), K(schema_version), K(tenant_id));
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
      } else if (OB_FAIL(schema_service_->get_all_sequences(
                     sql_client, schema_status, schema_version, tenant_id, simple_sequences))) {
        LOG_WARN("get all sequences schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_profiles(
                     sql_client, schema_status, schema_version, tenant_id, simple_profiles))) {
        LOG_WARN("get all profiles schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_sys_privs(
                     sql_client, schema_status, schema_version, tenant_id, sys_privs))) {
        LOG_WARN("get all sys priv schema failed", K(ret), K(schema_version), K(tenant_id));
      } else if (OB_FAIL(schema_service_->get_all_dblinks(
                     sql_client, schema_status, schema_version, tenant_id, simple_dblinks))) {
        LOG_WARN("get all dblink schema failed", K(ret), K(schema_version), K(tenant_id));
      }

      // add simple schema for cache
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_mgr_for_cache->sys_variable_mgr_.add_sys_variable(simple_sys_variable))) {
        LOG_WARN("add sys variables failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_users(simple_users))) {
        LOG_WARN("add users failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_databases(simple_databases))) {
        LOG_WARN("add databases failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_tablegroups(simple_tablegroups))) {
        LOG_WARN("add tablegroups failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_tables))) {
        LOG_WARN("add tables failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->outline_mgr_.add_outlines(simple_outlines))) {
        LOG_WARN("add outlines failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->synonym_mgr_.add_synonyms(simple_synonyms))) {
        LOG_WARN("add synonyms failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_db_privs(db_privs))) {
        LOG_WARN("add db privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_table_privs(table_privs))) {
        LOG_WARN("add table privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_obj_privs(obj_privs))) {
        LOG_WARN("add obj privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->udf_mgr_.add_udfs(simple_udfs))) {
        LOG_WARN("add udfs privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->sequence_mgr_.add_sequences(simple_sequences))) {
        LOG_WARN("add sequence failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->profile_mgr_.add_profiles(simple_profiles))) {
        LOG_WARN("add profiles failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->priv_mgr_.add_sys_privs(sys_privs))) {
        LOG_WARN("add sys privs failed", K(ret));
      } else if (OB_FAIL(schema_mgr_for_cache->dblink_mgr_.add_dblinks(simple_dblinks))) {
        LOG_WARN("add dblinks failed", K(ret));
      }

      LOG_INFO("add schemas for tenant finish",
          K(tenant_id),
          K(schema_version),
          "users",
          simple_users.count(),
          "databases",
          simple_databases.count(),
          "tablegroups",
          simple_tablegroups.count(),
          "tables",
          simple_tables.count(),
          "outlines",
          simple_outlines.count(),
          "synonyms",
          simple_synonyms.count(),
          "db_privs",
          db_privs.count(),
          "table_privs",
          table_privs.count(),
          "udfs",
          simple_udfs.count(),
          "sequences",
          simple_sequences.count(),
          "profiles",
          simple_profiles.count(),
          "sys_privs",
          sys_privs.count());
      // the parameters count of previous LOG_INFO has reached maximum,
      // so we need a new LOG_INFO.
      LOG_INFO("add schemas for tenant finish", K(tenant_id), K(schema_version), "dblinks", simple_dblinks.count());
    }

    // process tenant space table
    if (OB_FAIL(ret)) {
      // skip
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      ObArray<uint64_t> tenant_ids;
      ObArray<uint64_t> normal_tenant_ids;
      if (OB_FAIL(schema_mgr_for_cache->get_tenant_ids(tenant_ids))) {
        LOG_WARN("get tenant ids failed", K(ret));
      } else {
        FOREACH_CNT_X(tmp_tenant_id, tenant_ids, OB_SUCC(ret))
        {
          if (OB_SYS_TENANT_ID != *tmp_tenant_id) {
            if (OB_FAIL(normal_tenant_ids.push_back(*tmp_tenant_id))) {
              LOG_WARN("push back tenant id failed", K(ret), "tenant id", *tmp_tenant_id);
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(process_tenant_space_table(*schema_mgr_for_cache, normal_tenant_ids))) {
            LOG_WARN("process tenant space table failed", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // skip
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      ObArenaAllocator allocator;
      ObArray<ObTableSchema> core_tables;
      ObArray<ObTableSchema*> sys_tables;
      ObSEArray<uint64_t, 16> sys_table_ids;
      if (OB_FAIL(get_table_ids(sys_table_schema_creators, sys_table_ids))) {
        LOG_WARN("get_table_ids failed", K(ret));
      } else if (OB_FAIL(renew_core_and_sys_table_schemas(sql_client,
                     allocator,
                     core_tables,
                     sys_tables,
                     NULL,  // core_table_ids
                     &sys_table_ids))) {
        LOG_WARN("renew core and sys table schemas failed", K(ret));
      } else if (OB_FAIL(update_core_and_sys_schemas_in_cache(*schema_mgr_for_cache, core_tables, sys_tables))) {
        LOG_WARN("update core and sys schemas in cache faield", K(ret));
      }
    }
  }

  return ret;
}

int ObServerSchemaService::get_schema_version_in_inner_table(
    ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status, int64_t& target_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_status.tenant_id_;
  const bool did_use_weak = (schema_status.snapshot_timestamp_ >= 0);
  target_version = OB_INVALID_VERSION;
  // The target_version of the user tenant of the standalone cluster is subject to the schema_status;
  // the system tenant and the primary cluster obtain the target_version from the inner table.
  if (did_use_weak && OB_INVALID_TENANT_ID != tenant_id && OB_SYS_TENANT_ID != tenant_id) {
    target_version = schema_status.readable_schema_version_;
  } else if (OB_FAIL(schema_service_->fetch_schema_version(schema_status, sql_client, target_version))) {
    LOG_WARN("fail to fetch schema version", K(ret), K(schema_status));
  }
  return ret;
}

int ObServerSchemaService::check_core_or_sys_schema_change(ObISQLClient& sql_client, const int64_t core_schema_version,
    const int64_t schema_version, bool& core_schema_change, bool& sys_schema_change)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = 0;
  int64_t new_core_schema_version = 0;
  int64_t frozen_version = -1;
  const ObRefreshSchemaStatus dummy_schema_status;
  // check whether failed because of sys table schema change, go to suitable pos
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_schema_version_in_inner_table(sql_client, dummy_schema_status, new_schema_version))) {
    LOG_WARN("fail to get schema version in inner table", K(ret));
  } else if (OB_FAIL(check_sys_schema_change(sql_client, schema_version, new_schema_version, sys_schema_change))) {
    LOG_WARN("sys schema change during refresh schema", K(schema_version), K(new_schema_version), K(ret));
  }
  if (OB_SUCCESS != ret && NULL != schema_service_) {
    // check whether failed because of core table schema schema
    if (OB_FAIL(schema_service_->get_core_version(sql_client, frozen_version, new_core_schema_version))) {
      LOG_WARN("get_core_version failed", K(frozen_version), K(ret));
    } else if (new_core_schema_version != core_schema_version) {
      ret = OB_SUCCESS;
      core_schema_change = true;
      LOG_WARN("core schema change during check whether failed because of sys schema change",
          K(core_schema_version),
          K(new_core_schema_version),
          K(ret));
    }
  }
  return ret;
}

int ObServerSchemaService::check_sys_schema_change(
    ObISQLClient& sql_client, const int64_t schema_version, const int64_t new_schema_version, bool& sys_schema_change)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(get_table_ids(sys_table_schema_creators, table_ids))) {
    LOG_WARN("get_table_ids failed", K(ret));
  } else if (OB_FAIL(schema_service_->check_sys_schema_change(
                 sql_client, table_ids, schema_version, new_schema_version, sys_schema_change))) {
    LOG_WARN("check_sys_schema_change failed", K(schema_version), K(new_schema_version), K(ret));
  }
  return ret;
}

int ObServerSchemaService::get_table_ids(const schema_create_func* schema_creators, ObIArray<uint64_t>& table_ids) const
{
  int ret = OB_SUCCESS;
  ObTableSchema schema;
  table_ids.reset();
  if (OB_ISNULL(schema_creators)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema creators should not be null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && NULL != schema_creators[i]; ++i) {
    schema.reset();
    if (OB_FAIL(schema_creators[i](schema))) {
      LOG_WARN("create table schema failed", K(ret));
    } else if (OB_FAIL(table_ids.push_back(combine_id(OB_SYS_TENANT_ID, schema.get_table_id())))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

int ObServerSchemaService::determine_refresh_type(void)
{
  int ret = OB_SUCCESS;
  if (!increment_basepoint_set_) {
    refresh_schema_type_ = RST_FULL_SCHEMA_ALL;  // full refresh for all tenants
  } else {
    refresh_schema_type_ = RST_INCREMENT_SCHEMA_ALL;  // increment refresh for all tenants
  }

  return ret;
}

bool ObServerSchemaService::next_refresh_type_is_full(void)
{
  bool is_full = false;
  if (RST_FULL_SCHEMA_ALL == refresh_schema_type_ || RST_FULL_SCHEMA_IDS == refresh_schema_type_) {
    is_full = true;
  } else {
    is_full = false;
  }

  return is_full;
}

int ObServerSchemaService::query_tenant_status(const uint64_t tenant_id, TenantStatus& tenant_status)
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

int ObServerSchemaService::query_table_status(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
    const uint64_t tenant_id, const uint64_t table_id, const bool is_pg, TableStatus& status)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(schema_service_->query_table_status(
                 schema_status, *sql_proxy_, schema_version, tenant_id, table_id, is_pg, status))) {
    LOG_WARN("query table status failed", K(ret), K(schema_version), K(tenant_id), K(table_id), K(is_pg));
  }

  return ret;
}

int ObServerSchemaService::query_partition_status_from_sys_table(const ObRefreshSchemaStatus& schema_status,
    const int64_t schema_version, const common::ObPartitionKey& pkey, const bool is_sub_part_template,
    PartitionStatus& status)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(schema_service_->query_partition_status_from_sys_table(
                 schema_status, *sql_proxy_, schema_version, pkey, is_sub_part_template, status))) {
    LOG_WARN("query_partition_status_from_sys_table failed", K(ret), K(schema_version), K(pkey));
  }

  return ret;
}

int ObServerSchemaService::construct_schema_version_history(const ObRefreshSchemaStatus& schema_status,
    const int64_t snapshot_version, const VersionHisKey& key, VersionHisVal& val)
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

int ObServerSchemaService::get_tenant_schema_version(const uint64_t tenant_id, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(schema_manager_rwlock_);
  schema_version = OB_INVALID_VERSION;
  if (!GCTX.is_schema_splited()) {
    if (OB_ISNULL(schema_mgr_for_cache_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_mgr_for_cache is null", K(ret));
    } else {
      schema_version = schema_mgr_for_cache_->get_schema_version();
    }
  } else {
    ObSchemaVersionGetter getter;
    if (OB_FAIL(schema_mgr_for_cache_map_.atomic_refactored(tenant_id, getter))) {
      LOG_WARN("fail to get schema mgr for cache", K(ret));
    } else {
      schema_version = getter.get_schema_version();
    }
  }
  return ret;
}

int ObServerSchemaService::get_refresh_schema_info(ObRefreshSchemaInfo& schema_info)
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

// It is used to advance the visible schema version of the standalone cluster RS.
// It is assumed that the schema has been split, and compatibility processing is not performed.
int ObServerSchemaService::get_increment_schema_operations(const ObRefreshSchemaStatus& schema_status,
    const int64_t base_version, ObSchemaService::SchemaOperationSetWithAlloc& schema_operations)
{
  int ret = OB_SUCCESS;
  schema_operations.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else if (0 == schema_status.snapshot_timestamp_) {
    // nothing todo
  } else if (OB_FAIL(schema_service_->get_increment_schema_operations(
                 schema_status, base_version, *sql_proxy_, schema_operations))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get_increment_schema_operations failed", K(schema_status), K(base_version), K(ret));
    }
  }
  return ret;
}

// Use mock_schema_info_map_ read-write lock, no need to lock here
int ObServerSchemaService::get_mock_schema_info(
    const uint64_t schema_id, const ObSchemaType schema_type, ObMockSchemaInfo& mock_schema_info)
{
  int ret = OB_SUCCESS;
  mock_schema_info.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if ((TABLE_SCHEMA != schema_type && TABLEGROUP_SCHEMA != schema_type) || OB_INVALID_TENANT_ID == schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_id or invalid schema_type", K(ret), K(schema_id), K(schema_type));
  } else if (!GCTX.is_standby_cluster() || OB_SYS_TENANT_ID == extract_tenant_id(schema_id)) {
    // primary cluster or system tenant no need to mock
    ret = OB_ENTRY_NOT_EXIST;
  } else if (TABLEGROUP_SCHEMA == schema_type && !is_new_tablegroup_id(schema_id)) {
    // old tablegroup no need to mock
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(mock_schema_info_map_.get_refactored(schema_id, mock_schema_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get mock schema info", K(ret), K(schema_id));
    }
  }
  return ret;
}

// For the standalone cluster, it need to update mock_schema_infos before publish_schema:
// 1. The system tenant triggers the update of mock_schema_infos of all tenants
// 2. user user tenants only trigger the update of mock_schema_infos of this tenant
int ObServerSchemaService::reload_mock_schema_info(const ObSchemaMgr& schema_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = schema_mgr.get_tenant_id();
  ObArray<uint64_t> tenant_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!GCTX.is_standby_cluster()) {
    // skip
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema mgr", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(schema_mgr.get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant_ids", K(ret));
    }
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
    LOG_WARN("fail to push back tenant_id", K(ret), K(tenant_id));
  }

  if (OB_SUCC(ret)) {
    ObHashMap<uint64_t, ObMockSchemaInfo> new_mock_schema_infos;
    ObSEArray<uint64_t, MOCK_SCHEMA_BUCKET_NUM> remove_schema_ids;
    if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("proxy is null", K(ret));
    } else if (OB_FAIL(new_mock_schema_infos.create(
                   MOCK_SCHEMA_BUCKET_NUM, ObModIds::OB_MOCK_SCHEMA_INFO_MAP, ObModIds::OB_MOCK_SCHEMA_INFO_MAP))) {
      LOG_WARN("fail to mock schema info map", K(ret));
    } else if (OB_FAIL(schema_service_->get_mock_schema_infos(*sql_proxy_, tenant_ids, new_mock_schema_infos))) {
      LOG_WARN("fail to load mock schema infos from inner_table", K(ret));
    } else {
      // get remove mock_schema_infos
      ObMockSchemaInfo mock_schema_info;
      FOREACH_X(it, mock_schema_info_map_, OB_SUCC(ret))
      {
        const uint64_t schema_id = it->first;
        if (OB_SYS_TENANT_ID != tenant_id && extract_tenant_id(schema_id) != tenant_id) {
          // System tenants are responsible for loading all tenants, user tenants only load themselves
          // Since mock_schema_info_map_ uses schema_id as the key, it needs to be skipped in order
          // to prevent user tenants from deleting more when updating.
        } else if (OB_FAIL(new_mock_schema_infos.get_refactored(schema_id, mock_schema_info))) {
          if (OB_HASH_NOT_EXIST == ret) {  // overwrite
            if (OB_FAIL(remove_schema_ids.push_back(schema_id))) {
              LOG_WARN("fail to get schema_id to be removed", K(ret), K(schema_id));
            }
          } else {
            LOG_WARN("fail to get mock schema info", K(ret), K(schema_id));
          }
        }
      }

      // overwrite new mock_schema_infos
      FOREACH_X(it, new_mock_schema_infos, OB_SUCC(ret))
      {
        const uint64_t schema_id = it->first;
        if (OB_FAIL(new_mock_schema_infos.get_refactored(schema_id, mock_schema_info))) {
          LOG_WARN("fail to get mock schema info", K(ret), K(schema_id));
        } else {
          bool overwrite = true;
          ObMockSchemaInfo pre_mock_schema_info;
          ret = mock_schema_info_map_.get_refactored(schema_id, pre_mock_schema_info);
          if (OB_SUCCESS == ret) {
            if (pre_mock_schema_info != mock_schema_info) {
              LOG_INFO("[MOCK_SCHEMA] update mock schema info", K(pre_mock_schema_info), K(mock_schema_info));
            }
          } else if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("[MOCK_SCHEMA] add mock schema info", K(mock_schema_info));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(mock_schema_info_map_.set_refactored(schema_id, mock_schema_info, overwrite))) {
            LOG_WARN("fail to overwrite mock schema info", K(ret), K(mock_schema_info));
          }
        }
      }

      // remove mock_schema_infos
      for (int64_t i = 0; OB_SUCC(ret) && i < remove_schema_ids.count(); i++) {
        const uint64_t schema_id = remove_schema_ids.at(i);
        mock_schema_info.reset();
        if (OB_FAIL(mock_schema_info_map_.erase_refactored(schema_id, &mock_schema_info))) {
          LOG_WARN("fail to erase mock schema info", K(ret), K(schema_id));
        } else {
          LOG_INFO("[MOCK_SCHEMA] erase mock schema info", K(mock_schema_info));
        }
      }
    }
  }
  return ret;
}

int ObMaxSchemaVersionFetcher::operator()(common::hash::HashMapPair<uint64_t, ObSchemaMgr*>& entry)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if ((entry.second)->get_schema_version() > max_schema_version_) {
    max_schema_version_ = (entry.second)->get_schema_version();
  }
  return ret;
}

int ObSchemaVersionGetter::operator()(common::hash::HashMapPair<uint64_t, ObSchemaMgr*>& entry)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    schema_version_ = (entry.second)->get_schema_version();
  }
  return ret;
}

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
