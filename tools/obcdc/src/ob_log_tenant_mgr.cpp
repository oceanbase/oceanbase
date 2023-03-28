/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX OBLOG

#include <algorithm>                  // std::min
#include "ob_log_tenant_mgr.h"        // ObLogTenantMgr
#include "ob_log_instance.h"          // TCTX
#include "ob_log_schema_getter.h"     // IObLogSchemaGetter, ObLogSchemaGuard
#include "ob_log_table_matcher.h"     // IObLogTableMatcher
#include "ob_log_trans_stat_mgr.h"    // IObLogTransStatMgr
#include "ob_log_common.h"            // DEFAULT_START_SEQUENCE_NUM
#include "ob_log_config.h"            // TCONF
#include "ob_log_store_service.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [TenantMgr] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [TenantMgr] " fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
ObLogTenantMgr::ObLogTenantMgr() :
    inited_(false),
    tenant_hash_map_(),
    part_info_map_(),
    gindex_cache_(),
    table_id_cache_(),
    part_add_cb_array_(),
    part_rc_cb_array_(),
    tenant_id_set_(),
    enable_oracle_mode_match_case_sensitive_(false)
{
}

ObLogTenantMgr::~ObLogTenantMgr()
{
  destroy();
}

int ObLogTenantMgr::init(const bool enable_oracle_mode_match_case_sensitive)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogTenantMgr has not been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(tenant_id_set_.create(DEFAULT_TENANT_SET_SIZE))) {
    LOG_ERROR("tenant_id_set_ create fail", KR(ret));
  } else if (OB_FAIL(tenant_hash_map_.init(ObModIds::OB_LOG_TENANT_MAP))) {
    LOG_ERROR("tenant_hash_map init fail", KR(ret));
  } else if (OB_FAIL(part_info_map_.init(CACHED_PART_INFO_COUNT,
      PART_INFO_BLOCK_SIZE,
      ObModIds::OB_LOG_PART_INFO))) {
    LOG_ERROR("init part info map fail", KR(ret));
  } else if (OB_FAIL(gindex_cache_.init(ObModIds::OB_LOG_GLOBAL_NORMAL_INDEX_CACHE))) {
    LOG_ERROR("global index cache init fail", KR(ret));
  } else if (OB_FAIL(table_id_cache_.init(ObModIds::OB_LOG_TABLE_ID_CACHE))) {
    LOG_ERROR("table id cache init fail", KR(ret));
  } else {
    inited_ = true;
    enable_oracle_mode_match_case_sensitive_ = enable_oracle_mode_match_case_sensitive;

    LOG_INFO("ObLogTenantMgr init succ", K(enable_oracle_mode_match_case_sensitive_));
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObLogTenantMgr::destroy()
{
  inited_ = false;

  tenant_hash_map_.destroy();
  part_info_map_.destroy();
  gindex_cache_.destroy();
  table_id_cache_.destroy();
  part_add_cb_array_.destroy();
  part_rc_cb_array_.destroy();
  tenant_id_set_.destroy();
  enable_oracle_mode_match_case_sensitive_ = false;

  LOG_INFO("ObLogTenantMgr destroy succ");
}

int ObLogTenantMgr::filter_tenant(const char *tenant_name, bool &chosen)
{
  int ret = OB_SUCCESS;
  IObLogTableMatcher *tb_matcher = TCTX.tb_matcher_;

  if (OB_ISNULL(tenant_name)) {
    LOG_ERROR("tenant name is null", K(tenant_name));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(tb_matcher)) {
    LOG_ERROR("invalid tb_matcher", K(tb_matcher));
    ret = OB_ERR_UNEXPECTED;
  } else {
    chosen = false;

    // Matching tenants only
    if (OB_FAIL(tb_matcher->tenant_match(tenant_name, chosen))) {
      LOG_ERROR("match tenant fail", KR(ret), K(tenant_name), K(chosen));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogTenantMgr::add_served_tenant_for_stat_(const char *tenant_name,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  IObLogTransStatMgr *trans_stat_mgr = TCTX.trans_stat_mgr_;

  if (OB_ISNULL(trans_stat_mgr)) {
    LOG_ERROR("trans_stat is null", K(trans_stat_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(trans_stat_mgr->add_served_tenant(tenant_name, tenant_id))) {
    LOG_ERROR("trans_stat_mgr add served tenant fail", KR(ret), K(tenant_id), K(tenant_name));
  } else {
    // do nothing
  }

  return ret;
}

int ObLogTenantMgr::add_served_tenant_into_set_(const char *tenant_name,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tenant_id_set_.set_refactored(tenant_id))) {
    LOG_ERROR("add tenant_id into tenant_id_set fail", KR(ret), K(tenant_name), K(tenant_id));
  } else {
    LOG_INFO("add tenant_id into tenant_id_set succ", K(tenant_name), K(tenant_id));
  }

  return ret;
}

// Get the tenant's starting-service-schema-version
//
// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_TIMEOUT                   timeout
// @retval other error code             fail
int ObLogTenantMgr::get_tenant_start_schema_version_(const uint64_t tenant_id,
    const bool is_new_created_tenant,   /* is new created tenant */
    const bool is_new_tenant_by_restore,  /* is new created tenant by restore */
    const int64_t start_serve_tstamp,   /* start serve timestamp for this tenant */
    const int64_t sys_schema_version,   /* corresponding schema version of sys tenant */
    int64_t &tenant_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
  if (OB_ISNULL(schema_getter)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // For sys tenants, the sys tenant schema version is returned directly
    // This only happens at bootstrap
    tenant_schema_version = sys_schema_version;
    ISTAT("[ADD_TENANT] sys tenant start schema version is already ready", K(tenant_id),
        K(is_new_created_tenant), K(is_new_tenant_by_restore), K(start_serve_tstamp), K(sys_schema_version),
        K(tenant_schema_version));
  } else if (is_new_tenant_by_restore) {
    // check is_new_tenant_by_restore is_new_tenant_by_restore = true is subset of is_new_created_tenant=true
    // The sys_schema_version at this point is actually the schema_version of the tenant carried in the backup-restore ddl_stmt_str parsed by ddl_handler
    tenant_schema_version = sys_schema_version;

    ISTAT("[ADD_TENANT] get tenant schema_version of CREATE_TENANT_END_DDL for tenant generate dynamicly by restore",
        K(tenant_id), K(is_new_created_tenant), K(is_new_tenant_by_restore),
        K(start_serve_tstamp), K(sys_schema_version));
  } else if (is_new_created_tenant) {
    // If it is a new tenant, need to take the first schema version of the tenant
    // because all DDL operations are synchronised to this tenant
    if (OB_FAIL(get_first_schema_version_of_tenant_(tenant_id, sys_schema_version, *schema_getter,
        tenant_schema_version, timeout))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("get_first_schema_version_of_tenant_ fail", KR(ret), K(tenant_id),
            K(sys_schema_version));
      }
    }

    ISTAT("[ADD_TENANT] get tenant first schema version as start schema version",
        KR(ret), K(tenant_id), K(is_new_created_tenant), K(sys_schema_version),
        K(tenant_schema_version), "is_schema_split_mode", TCTX.is_schema_split_mode_);
  } else {
    // Add a tenant that already exists and query for a schema version less than or equal to that timestamp based on the starting service timestamp
    if (OB_FAIL(schema_getter->get_schema_version_by_timestamp(tenant_id, start_serve_tstamp,
        tenant_schema_version, timeout))) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("get_schema_version_by_timestamp fail cause by tenant has been dropped, ignore", KR(ret), K(tenant_id), K(start_serve_tstamp));
      } else {
        LOG_ERROR("get_schema_version_by_timestamp fail", KR(ret), K(tenant_id), K(start_serve_tstamp));
      }
    }

    ISTAT("[ADD_TENANT] get start schema version by start timestamp",
        KR(ret), K(tenant_id), K(is_new_created_tenant), K(start_serve_tstamp),
        K(tenant_schema_version));
  }
  return ret;
}

int ObLogTenantMgr::add_ddl_table_if_needed_(const uint64_t tenant_id,
    ObLogTenant &tenant,
    const int64_t start_serve_tstamp,
    const int64_t tenant_start_schema_version,
    const bool is_new_created_tenant)
{
  int ret = OB_SUCCESS;
  bool is_schema_split_mode = TCTX.is_schema_split_mode_;
  // Is it necessary to add a ddl partition
  // 1. SYS tenants must add a DDL partition
  // 2. In split mode, normal tenants need to add a DDL partition
  bool need_add_ddl_table = ((OB_SYS_TENANT_ID == tenant_id) || is_schema_split_mode);

  if (need_add_ddl_table) {
    if (OB_FAIL(tenant.add_ddl_table(start_serve_tstamp,
        tenant_start_schema_version,
        is_new_created_tenant))) {
      LOG_WARN("tenant add ddl table fail",KR(ret), K(tenant_id), K(tenant), K(is_schema_split_mode),
          K(need_add_ddl_table));
    }
  }

  ISTAT("[ADD_DDL_TABLE]", K(tenant_id), K(need_add_ddl_table), K(is_schema_split_mode),
      K(start_serve_tstamp), K(tenant_start_schema_version), K(is_new_created_tenant), KR(ret));
  return ret;
}

// TODO: Only normal tenant all_sequence_value tables are supported after version 2.2 for now
int ObLogTenantMgr::add_inner_tables_on_backup_mode_(const uint64_t tenant_id,
    ObLogTenant &tenant,
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const bool is_schema_split_mode = TCTX.is_schema_split_mode_;
  const bool enable_backup_mode = is_backup_mode();
  // 1) normal tenant
  // 2) schema split mode(ob version greater than 2.2)
  // 3) backup_mode
  bool need_add_inner_table = ((OB_SYS_TENANT_ID != tenant_id)
      && is_schema_split_mode
      && enable_backup_mode);

  if (need_add_inner_table) {
    if (OB_FAIL(tenant.add_inner_tables(start_serve_tstamp, start_schema_version, timeout))) {
      LOG_ERROR("tenant add inner table on backup mode fail", KR(ret), K(tenant_id),
          K(tenant), K(is_schema_split_mode), K(enable_backup_mode), K(need_add_inner_table));
    } else {
      LOG_INFO("[ADD_INNER_TABLE] tenant add inner table on backup mode succ", KR(ret), K(tenant_id),
          K(tenant), K(is_schema_split_mode), K(enable_backup_mode), K(need_add_inner_table));
    }
  } else {
    LOG_INFO("not on backup mode, do not add inner tables", K(tenant_id),
        K(tenant), K(is_schema_split_mode), K(enable_backup_mode), K(need_add_inner_table));
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_TIMEOUT                   timeout
// @retval other error code             fail
int ObLogTenantMgr::do_add_tenant_(const uint64_t tenant_id,
    const char *tenant_name,
    const bool is_new_created_tenant,
    const bool is_new_tenant_by_restore,
    const bool is_tenant_served,
    const int64_t start_serve_tstamp,
    const int64_t sys_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  TenantID tid(tenant_id);
  ObLogTenant *tenant = NULL;
  int64_t tenant_start_schema_version = 0;
  int64_t start_seq = DEFAULT_START_SEQUENCE_NUM;
  IObStoreService *store_service = TCTX.store_service_;
  void *column_family_handle = NULL;

  if (OB_ISNULL(store_service)) {
    LOG_ERROR("store_service is NULL");
    ret = OB_ERR_UNEXPECTED;
  }
  // Get the starting schema version of the tenant
  else if (OB_FAIL(get_tenant_start_schema_version_(tenant_id, is_new_created_tenant, is_new_tenant_by_restore,
      start_serve_tstamp, sys_schema_version, tenant_start_schema_version, timeout))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      LOG_WARN("get_tenant_start_schema_version_ fail cause tenant has been dropped", KR(ret), K(tenant_id), K(tenant_name),
          K(is_new_created_tenant), K(is_new_tenant_by_restore), K(start_serve_tstamp), K(sys_schema_version));
    } else if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_start_schema_version_ fail", KR(ret), K(tenant_id), K(tenant_name),
          K(is_new_created_tenant), K(is_new_tenant_by_restore), K(start_serve_tstamp), K(sys_schema_version));
    }
  }
  else if (OB_ENTRY_EXIST == (ret = tenant_hash_map_.contains_key(tid))) {
    LOG_ERROR("cannot add duplicated tenant", KR(ret), K(tenant_id));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_ERROR("tenant hash map contains key failed", KR(ret), K(tenant_id));
  } else {
    ret = OB_SUCCESS;
    // alloc a tenant struct
    if (OB_FAIL(tenant_hash_map_.alloc_value(tenant))) {
      LOG_ERROR("alloc log tenant failed", KR(ret), K(tenant_id), K(tenant));
    } else if (OB_ISNULL(tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is NULL", KR(ret), K(tenant_id), K(tenant));
    } else if (OB_FAIL(store_service->create_column_family(
        std::to_string(tenant_id) + ":" + std::string(tenant_name),
        column_family_handle))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("create_column_family fail", KR(ret), K(tenant_id), K(tenant_name), K(column_family_handle));
      }
    }
    // init tenant
    else if (OB_FAIL(tenant->init(tenant_id, tenant_name, start_serve_tstamp, start_seq,
            tenant_start_schema_version, column_family_handle, *this))) {
      LOG_ERROR("tenant init fail", KR(ret), K(tenant_id), K(tenant_name), K(start_serve_tstamp),
          K(start_seq), K(tenant_start_schema_version));
    }
    // Ensure that a valid tenant is inserted and that the consumer will not see an invalid tenant
    else if (OB_FAIL(tenant_hash_map_.insert_and_get(tid, tenant))) {
      LOG_ERROR("tenant_hash_map_ insert and get failed", KR(ret), K(tenant_id));
    } else {
      // make sure to revert here, otherwise there will be a memory/ref leak
      revert_tenant_(tenant);
      // The tenant structure cannot be referenced again afterwards and may be deleted at any time
      tenant = NULL;
    }

    if (OB_FAIL(ret)) {
      if (NULL != tenant) {
        (void)tenant_hash_map_.del(tid);
        tenant_hash_map_.free_value(tenant);
        tenant = NULL;
      }
    }
    // start tenant service
    else if (OB_FAIL(start_tenant_service_(tenant_id, is_new_created_tenant, is_new_tenant_by_restore, start_serve_tstamp,
            tenant_start_schema_version, timeout))) {
      LOG_ERROR("start tenant service fail", KR(ret), K(tenant_id), K(is_new_created_tenant),
          K(start_serve_tstamp), K(tenant_start_schema_version));
    }
  }

  if (OB_SUCCESS == ret && is_tenant_served) {
    if (OB_FAIL(add_served_tenant_for_stat_(tenant_name, tenant_id))) {
      LOG_ERROR("trans stat mgr add serverd tenant fail", KR(ret), K(tenant_id), K(tenant_name));
    } else if (OB_FAIL(add_served_tenant_into_set_(tenant_name, tenant_id))) {
      LOG_ERROR("add tenant_id into tenant_id_set fail", KR(ret), K(tenant_id), K(tenant_name));
    } else {
      // do nothing
    }
  }

  if (OB_SUCCESS == ret) {
    ISTAT("[ADD_TENANT]", K(tenant_id), K(tenant_name), K(is_new_created_tenant), K(is_new_tenant_by_restore),
        K(is_tenant_served), K(start_serve_tstamp), K(sys_schema_version),
        "total_tenant_count", tenant_hash_map_.count());
  }

  return ret;
}

int ObLogTenantMgr::start_tenant_service_(const uint64_t tenant_id,
    const bool is_new_created_tenant,
    const bool is_new_tenant_by_restore,
    const int64_t start_serve_tstamp,
    const int64_t tenant_start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  // normal_new_created_tenant means tenant found by ADD_TENANT action AND NOT CREATE by RESTORE
  const bool is_normal_new_created_tenant = is_new_created_tenant && !is_new_tenant_by_restore;
  // Real-time access to tenant structure, tenants should not be non-existent
  if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant));
    ret = OB_ERR_UNEXPECTED;
  }
  // add DDL table(__all_ddl_operation)
  else if (OB_FAIL(add_ddl_table_if_needed_(tenant_id, *tenant, start_serve_tstamp,
        tenant_start_schema_version, is_normal_new_created_tenant))) {
    LOG_ERROR("add_ddl_table_if_needed_ fail", KR(ret), K(tenant_id), K(start_serve_tstamp),
        K(tenant_start_schema_version), K(is_new_created_tenant), K(is_normal_new_created_tenant));
  }
  // add inner tables
  else if (OB_FAIL(add_inner_tables_on_backup_mode_(tenant_id,
          *tenant,
          start_serve_tstamp,
          tenant_start_schema_version,
          timeout))) {
    LOG_ERROR("add_inner_tables_on_backup_mode_ fail", KR(ret), K(tenant_id), K(start_serve_tstamp),
        K(tenant_start_schema_version));
  }
  // add all user tables of tenant in such cases:
  // 1. tenant exist when oblog start
  // 2. or tenant create by DDL AND by restore
  // which means, won't add user table of tenant of a normal new tenant(which should not have any user table)
  else if (! is_normal_new_created_tenant) {
    if (OB_FAIL(tenant->add_all_tables(start_serve_tstamp,
            tenant_start_schema_version,
            timeout))) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        // FIXME: When a schema error (OB_TENANT_HAS_BEEN_DROPPED) is encountered, the table under the tenant is ignored and success is returned
        // Since the deletion of the tenant DDL will definitely be encountered later, there is no need to roll back the successful operation above
        LOG_WARN("schema error, tenant may be dropped. need not add_all_tables", KR(ret),
            K(tenant_id), K(tenant_start_schema_version),
            K(start_serve_tstamp), K(is_new_created_tenant));
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("add_all_tables fail", KR(ret), K(tenant_id), K(tenant_start_schema_version),
            K(start_serve_tstamp), K(is_new_created_tenant));
      }
    }
  }
  return ret;
}

// add tenant for oblog
//
// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant/database/... not exist, caller should ignore
// @retval other error code             error
int ObLogTenantMgr::add_tenant(const uint64_t tenant_id,
    const bool is_new_created_tenant,
    const bool is_new_tenant_by_restore,
    const int64_t start_serve_tstamp,
    const int64_t sys_schema_version,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const int64_t timeout,
    bool &add_tenant_succ)
{
  int ret = OB_SUCCESS;
  add_tenant_succ = false;
  TenantSchemaInfo tenant_schema_info;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(sys_schema_version <= 0) || OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid arguments", K(tenant_id), K(sys_schema_version), K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  }
  // Use system tenant identity to get schema of normal tenant
  // by lazy mode
  else if (OB_FAIL(schema_getter->get_lazy_schema_guard(OB_SYS_TENANT_ID, sys_schema_version,
      timeout, schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard of SYS tenant fail", KR(ret), K(sys_schema_version));
    }
  } else if (OB_FAIL(schema_guard.get_tenant_schema_info(tenant_id, tenant_schema_info, timeout))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // Note: Because the above gets the Guard of the SYS tenant, using the exact schema version of the SYS tenant.
      // Therefore, it is expected that the schema for that tenant must be visible, and if it is not, there is a bug.
      LOG_ERROR("get_tenant_schema_info fail: tenant has been dropped when add tenant, should not happen",
          KR(ret), K(tenant_id), K(sys_schema_version), K(is_new_created_tenant));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_TIMEOUT != ret) {
      LOG_ERROR("get tenant schema fail", KR(ret), K(tenant_id), K(sys_schema_version));
    }
  } else if (tenant_schema_info.is_restore_) {
    // 1. won't add tenant in restore status
    // 2. for new tenant found by ADD_TENENT_END DDL:
    // 2.1. normal new created tenant, start log id will set to 1 to simplify the ADD_PARTITION process, and won't add user tables
    // 2.2. if tenant new created by restore, should query server for start_serve_log_id, and should add user tables.
    LOG_INFO("won't add restore-state tenant", K(tenant_id), K(tenant_schema_info), K(sys_schema_version));
  } else {
    bool is_tenant_served = false;
    // whether the tenant should be added or not, if the tenant is in service, it must be added.
    // Tenants that are not in service(mean not config in oblog config file) may also need to be added, e.g. SYS tenant
    bool need_add_tenant = false;
    tenant_name = tenant_schema_info.name_;

    // Filtering tenants based on whitelists
    if (OB_FAIL(filter_tenant(tenant_name, is_tenant_served))) {
      LOG_ERROR("filter_tenant fail", KR(ret), K(tenant_id), K(tenant_name));
    } else if (! is_tenant_served) {
      // tenant is not serve
      LOG_INFO("tenant is not served", K(tenant_id), K(tenant_name));
      if (OB_SYS_TENANT_ID == tenant_id) {
        // must add sys tenant(cause __all_ddl_operation in sys tenant is needed)
        need_add_tenant = true;
      }
    } else {
      // must add served tenant
      need_add_tenant = true;
    }

    if (OB_FAIL(ret)) {
      // fail
    } else if (! need_add_tenant) {
      // don't need add tenant
      add_tenant_succ = false;
    // do real add tenant
    } else if (OB_FAIL(do_add_tenant_(tenant_id, tenant_name, is_new_created_tenant, is_new_tenant_by_restore,
      is_tenant_served, start_serve_tstamp, sys_schema_version, timeout))) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("will not add tenant has been dropped", KR(ret), K(tenant_id), K(tenant_name),
            K(is_new_created_tenant), K(is_new_tenant_by_restore), K(is_tenant_served), K(start_serve_tstamp),
            K(sys_schema_version), K(timeout));
      } else if (OB_TIMEOUT != ret) {
        LOG_ERROR("do add tenant fail", KR(ret), K(tenant_id), K(tenant_name),
            K(is_new_created_tenant), K(is_new_tenant_by_restore), K(is_tenant_served), K(start_serve_tstamp),
            K(sys_schema_version), K(timeout));
      }
    } else {
      // add tenant success
      add_tenant_succ = true;
    }
  }

  return ret;
}

// fetch first schema version of tenant
//
// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
// @retval OB_TIMEOUT                   timeout
// @retval other error code             fail
int ObLogTenantMgr::get_first_schema_version_of_tenant_(const uint64_t tenant_id,
    const int64_t sys_schema_version,
    IObLogSchemaGetter &schema_getter,
    int64_t &first_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool is_schema_split_mode = TCTX.is_schema_split_mode_;
  first_schema_version = OB_INVALID_VERSION;

  if (! is_schema_split_mode) {
    // use schema version of ADD_TENANT DDL(by sys tenant) if is NOT schema split mode
    first_schema_version = sys_schema_version;
  }
  // query first tenant schema version if tenant is created by schema split mode
  else if (OB_FAIL(schema_getter.get_first_trans_end_schema_version(tenant_id,
      first_schema_version,
      timeout))) {
    // OB_TENANT_HAS_BEEN_DROPPED return caller
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_first_trans_end_schema_version fail", KR(ret), K(tenant_id),
          K(first_schema_version));
    }
  } else if (OB_UNLIKELY(first_schema_version <= 0)) {
    LOG_ERROR("tenant first schema versioin is invalid", K(tenant_id), K(first_schema_version));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // success
  }

  return ret;
}

int ObLogTenantMgr::drop_tenant(const uint64_t tenant_id, const char *call_from)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  bool tenant_can_be_dropped = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("tenant not exist, need not drop tenant", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  }
  // Multiple drop_tenant() to be processed properly
  else if (OB_FAIL(tenant->drop_tenant(tenant_can_be_dropped, call_from))) {
    LOG_ERROR("part mgr drop tenant fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(drop_served_tenant_for_stat_(tenant_id))) {
    LOG_ERROR("trans stat mgr del server tenant fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(drop_served_tenant_from_set_(tenant_id))) {
    LOG_ERROR("drop tenant_id from tenant_id_set fail", KR(ret), K(tenant_id));
  } else {
    // do nothing
  }

  // Delete the tenant structure if the tenant can be deleted
  if (OB_SUCCESS == ret && tenant_can_be_dropped) {
    ISTAT("[DROP_TENANT] [REMOVE_TENANT] ready to remove tenant from tenant map after drop tenant",
        KPC(tenant), K(call_from));

    // Although it will be called many times externally, it will only be called once here
    if (OB_FAIL(remove_tenant_(tenant_id, tenant))) {
      LOG_ERROR("remove tenant fail", KR(ret), K(tenant_id), K(tenant_can_be_dropped));
    }
  }
  return ret;
}

int ObLogTenantMgr::drop_tenant_start(const uint64_t tenant_id,
    const int64_t drop_tenant_start_tstamp)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("tenant not exist, need not handle drop tenant start DDL", K(tenant_id),
          K(drop_tenant_start_tstamp));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant->mark_drop_tenant_start(drop_tenant_start_tstamp))) {
    LOG_ERROR("mark drop tenant start fail", KR(ret), KPC(tenant), K(drop_tenant_start_tstamp));
  } else {
    LOG_INFO("succeed to mark drop tenant start while handling drop tenant start DDL", KPC(tenant),
        K(drop_tenant_start_tstamp));
  }
  return ret;
}

int ObLogTenantMgr::drop_tenant_end(const uint64_t tenant_id,
    const int64_t drop_tenant_end_tstamp)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("tenant not exist, need not handle drop tenant end DDL", K(tenant_id),
          K(drop_tenant_end_tstamp));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  } else if (tenant->is_drop_tenant_start_marked()) {
    LOG_INFO("drop tenant start has been marked, need not handle drop tenent end DDL", K(tenant_id),
        K(drop_tenant_end_tstamp));
  } else if (OB_FAIL(tenant->mark_drop_tenant_start(drop_tenant_end_tstamp))) {
    LOG_ERROR("mark drop tenant start fail", KR(ret), KPC(tenant), K(drop_tenant_end_tstamp));
  } else {
    LOG_INFO("succeed to mark drop tenant start while handling drop tenant end DDL", KPC(tenant),
        K(drop_tenant_end_tstamp));
  }
  return ret;
}

int ObLogTenantMgr::alter_tenant_name(const uint64_t tenant_id,
    const int64_t schema_version_before_alter,
    const int64_t schema_version_after_alter,
    const int64_t timeout,
    const char *&tenant_name,
    bool &tenant_is_chosen)
{
  int ret = OB_SUCCESS;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
  ObLogSchemaGuard old_schema_guard;
  TenantSchemaInfo old_tenant_schema_info;
  ObLogSchemaGuard new_schema_guard;
  TenantSchemaInfo new_tenant_schema_info;
  tenant_is_chosen = true;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(schema_version_before_alter <= 0)
      || OB_UNLIKELY(schema_version_after_alter <= 0)
      || OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid arguments", K(schema_version_before_alter), K(schema_version_after_alter), K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schema_getter->get_lazy_schema_guard(OB_SYS_TENANT_ID, schema_version_before_alter,
      timeout, old_schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard of SYS tenant fail", KR(ret), K(schema_version_before_alter));
    }
  } else if (OB_FAIL(schema_getter->get_lazy_schema_guard(OB_SYS_TENANT_ID, schema_version_after_alter,
      timeout, new_schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard of SYS tenant fail", KR(ret), K(schema_version_after_alter));
    }
  } else if (OB_FAIL(old_schema_guard.get_tenant_schema_info(tenant_id, old_tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get tenant schema fail", KR(ret), K(tenant_id), K(schema_version_before_alter));
    }
  } else if (OB_FAIL(new_schema_guard.get_tenant_schema_info(tenant_id, new_tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get tenant schema fail", KR(ret), K(tenant_id), K(schema_version_after_alter));
    }
  } else {
    const char *old_tenant_name = old_tenant_schema_info.name_;
    const char *new_tenant_name = new_tenant_schema_info.name_;
    const bool skip_rename_tenant_ddl = (0 != TCONF.skip_rename_tenant_ddl);
    bool is_tenant_served = false;
    tenant_name = new_tenant_name;

    if (OB_ISNULL(old_tenant_name) || OB_ISNULL(new_tenant_name)) {
      LOG_ERROR("old_tenant_name or new_tenant_name is not valid", K(old_tenant_name), K(new_tenant_name));
      ret = OB_ERR_UNEXPECTED;
    // Filtering tenants based on whitelists
    } else if (OB_FAIL(filter_tenant(old_tenant_name, is_tenant_served))) {
      LOG_ERROR("filter_tenant fail", KR(ret), K(tenant_id), K(old_tenant_name));
    } else if (! is_tenant_served) {
      // If the current rename tenant does not match the current whitelist, then no processing is required
      tenant_is_chosen = false;
    } else {
      // If you are synchronising the whole cluster, there is no need to do this
      IObLogTableMatcher *tb_matcher = TCTX.tb_matcher_;
      bool matched = false;

      if (OB_ISNULL(tb_matcher)) {
        LOG_ERROR("tb_matcher is NULL", K(tb_matcher));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(tb_matcher->cluster_match(matched))) {
        LOG_ERROR("tb_matcher cluster_match fail", KR(ret), K(matched));
      } else if (matched) {
        LOG_INFO("[RENAME_TENANT] is supported when cluster match", K(old_tenant_name), K(new_tenant_name));
      } else {
        if (skip_rename_tenant_ddl) {
          LOG_INFO("[RENAME_TENANT] skip check", K(old_tenant_name), K(new_tenant_name), K(skip_rename_tenant_ddl));
          tenant_is_chosen = true;
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("[RENAME_TENANT] is not supported", K(old_tenant_name), K(new_tenant_name));
          ret = OB_NOT_SUPPORTED;
        }
      }
    }
  }

  return ret;
}

int ObLogTenantMgr::remove_tenant_(const uint64_t tenant_id, ObLogTenant *tenant)
{
  int ret = OB_SUCCESS;
  TenantID tid(tenant_id);
  IObStoreService *store_service = TCTX.store_service_;
  void *cf = NULL;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(store_service)) {
    LOG_ERROR("store_service is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(tenant)) {
    LOG_ERROR("tenant is NULL", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cf = tenant->get_cf())) {
    LOG_ERROR("cf is NULL", K(tid), KPC(tenant));
    ret= OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(store_service->drop_column_family(cf))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service drop_column_family fail", K(tid), KPC(tenant));
    }
  } else if (OB_FAIL(tenant_hash_map_.del(tid))) {
    LOG_ERROR("tenant_hash_map_ del failed", KR(ret), K(tenant_id));
  } else {
    //do nothing
  }

  return ret;
}

int ObLogTenantMgr::drop_served_tenant_for_stat_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  IObLogTransStatMgr *trans_stat_mgr = TCTX.trans_stat_mgr_;

  if (OB_ISNULL(trans_stat_mgr)) {
    LOG_ERROR("trans_stat is null", K(trans_stat_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(trans_stat_mgr->drop_served_tenant(tenant_id))) {
    LOG_ERROR("trans_stat_mgr del served tenant fail", KR(ret),
        K(tenant_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObLogTenantMgr::drop_served_tenant_from_set_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tenant_id_set_.erase_refactored(tenant_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("tenant_id has been dropped from tenant_id_set", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("del tenant_id from tenant_id_set fail", KR(ret), K(tenant_id));
    }
  } else {
    ISTAT("[DROP_TENANT] drop tenant_id from tenant_id_set succ", K(tenant_id));
  }

  return ret;
}

int ObLogTenantMgr::get_tenant(const uint64_t tenant_id, ObLogTenant *&tenant)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    LOG_ERROR("invalid argument", K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    TenantID tid(tenant_id);
    ObLogTenant *tmp_tenant = NULL;

    if (OB_FAIL(tenant_hash_map_.get(tid, tmp_tenant))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("tenant_hash_map_ get fail", KR(ret), K(tid), K(tmp_tenant));
      }
    } else if (OB_ISNULL(tmp_tenant)) {
      LOG_ERROR("tenant is null", K(tenant_id), K(tmp_tenant));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // succ
      tenant = tmp_tenant;
    }
  }
  return ret;
}

int ObLogTenantMgr::get_tenant_guard(const uint64_t tenant_id, ObLogTenantGuard &guard)
{
  int ret = OB_SUCCESS;
  ObLogTenant *tenant = NULL;
  if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    // Failed, or non-existent
  } else {
    guard.set_tenant(tenant);
  }
  return ret;
}

void ObLogTenantMgr::revert_tenant_(ObLogTenant *tenant)
{
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
  } else if (NULL != tenant) {
    tenant_hash_map_.revert(tenant);
  }
}

int ObLogTenantMgr::revert_tenant(ObLogTenant *tenant)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tenant)) {
    LOG_ERROR("tenant is NULL", K(tenant));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_hash_map_.revert(tenant);
  }
  return ret;
}

int ObLogTenantMgr::get_ddl_progress(uint64_t &tenant_id,
    int64_t &ddl_min_progress,
    uint64_t &ddl_min_handle_log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else {
    TenantDDLProgessGetter getter;

    if (OB_FAIL(tenant_hash_map_.for_each(getter))) {
      LOG_ERROR("tenant_hash_map_ for each fail", KR(ret));
    } else {
      tenant_id = getter.tenant_id_;
      ddl_min_progress = getter.ddl_min_progress_;
      ddl_min_handle_log_id = getter.ddl_min_handle_log_id_;
    }
  }

  return ret;
}

ObLogTenantMgr::TenantDDLProgessGetter::TenantDDLProgessGetter() :
    tenant_id_(OB_INVALID_TENANT_ID),
    ddl_min_progress_(OB_INVALID_TIMESTAMP),
    ddl_min_handle_log_id_(OB_INVALID_ID)
{
}

bool ObLogTenantMgr::TenantDDLProgessGetter::operator()(const TenantID &tid,
    ObLogTenant *tenant)
{
  // Ignore offlined tenants
  if (NULL != tenant && tenant->is_serving()) {
    int64_t ddl_progress = tenant->get_ddl_progress();
    uint64_t ddl_handle_log_id = tenant->get_handle_log_id();

    if (OB_INVALID_TIMESTAMP == ddl_min_progress_) {
      ddl_min_progress_ = ddl_progress;
      tenant_id_ = tid.tenant_id_;
    } else {
      if (ddl_progress < ddl_min_progress_) {
        ddl_min_progress_ = ddl_progress;
        tenant_id_ = tid.tenant_id_;
      }
    }

    if (OB_INVALID_ID == ddl_min_handle_log_id_) {
      ddl_min_handle_log_id_ = ddl_handle_log_id;
    } else {
      ddl_min_handle_log_id_ = std::min(ddl_handle_log_id, ddl_min_handle_log_id_);
    }
  }
  return true;
}

int ObLogTenantMgr::register_part_add_callback(PartAddCallback *callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(callback)) {
    LOG_ERROR("invalid argument", K(callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_add_cb_array_.push_back(reinterpret_cast<int64_t>(callback)))) {
    LOG_ERROR("push_back into part_add_cb_array fail", KR(ret), K(callback));
  } else {
    // succ
  }
  return ret;
}

int ObLogTenantMgr::register_part_recycle_callback(PartRecycleCallback *callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(callback)) {
    LOG_ERROR("invalid argument", K(callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_rc_cb_array_.push_back(reinterpret_cast<int64_t>(callback)))) {
    LOG_ERROR("push_back into part_rc_cb_array fail", KR(ret), K(callback));
  } else {
    // succ
  }
  return ret;
}

int ObLogTenantMgr::add_all_tenants(const int64_t start_tstamp,
    const int64_t sys_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  ObLogSchemaGuard sys_schema_guard;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited");
  } else if (OB_UNLIKELY(start_tstamp <= 0) || OB_UNLIKELY(sys_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(start_tstamp), K(sys_schema_version));
  } else if (OB_ISNULL(schema_getter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("schema_getter is invalid", K(schema_getter), KR(ret));
  }
  // Get the schema guard for the SYS tenant
  // Since get_available_tenant_ids() cannot use lazy mode
  else if (OB_FAIL(schema_getter->get_fallback_schema_guard(OB_SYS_TENANT_ID, sys_schema_version,
      timeout, sys_schema_guard))) {
    LOG_ERROR("get_fallback_schema_guard of SYS tenant fail", KR(ret), K(sys_schema_version), K(timeout));
  }
  // get available tenant id list
  else if (OB_FAIL(sys_schema_guard.get_available_tenant_ids(tenant_ids, timeout))) {
    LOG_ERROR("get_available_tenant_ids fail", KR(ret), K(tenant_ids), K(timeout));
  } else {
    int64_t chosen_tenant_count = 0;
    bool is_new_created_tenant = false;
    bool is_new_tenant_by_restore = false;
    const int64_t total_tenant_count = tenant_ids.count();
    const bool enable_filter_sys_tenant = (1 == TCONF.enable_filter_sys_tenant);
    ISTAT("[ADD_ALL_TENANTS] BEGIN", K(sys_schema_version), K(start_tstamp), K(total_tenant_count),
        K(tenant_ids));

    // add all tenants
    for (int64_t index = 0; OB_SUCCESS == ret && index < total_tenant_count; index++) {
      const uint64_t tenant_id = tenant_ids.at(index);
      bool add_tenant_succ = false;
      ObLogSchemaGuard schema_guard;
      const char *tenant_name = NULL;

      if (1 == tenant_id && enable_filter_sys_tenant) {
        ISTAT("[FILTE] sys tenant is filtered", K(tenant_id), K(enable_filter_sys_tenant));
      } else {
        if (OB_FAIL(add_tenant(tenant_id, is_new_created_tenant, is_new_tenant_by_restore, start_tstamp,
                sys_schema_version, schema_guard, tenant_name, timeout, add_tenant_succ))) {
          if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
            LOG_INFO("tenant has dropped, ignore it", KR(ret), K(tenant_id), K(sys_schema_version),
                K(index), K(start_tstamp));
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("add tenant fail", KR(ret), K(tenant_id), K(start_tstamp),K(sys_schema_version),
                K(index));
          }
        }
      }

      if (OB_SUCCESS == ret) {
        if (add_tenant_succ) {
          chosen_tenant_count++;
        }

        ISTAT("[ADD_ALL_TENANTS] ", K(index), K(tenant_id), K(tenant_name), K(add_tenant_succ),
            K(chosen_tenant_count), K(total_tenant_count));
      }
    }

    ISTAT("[ADD_ALL_TENANTS] DONE", K(chosen_tenant_count), K(total_tenant_count),
        K(sys_schema_version), K(start_tstamp), K(tenant_ids), KR(ret));
  }
  return ret;
}

int ObLogTenantMgr::filter_ddl_stmt(const uint64_t tenant_id, bool &chosen)
{
  int ret = OB_SUCCESS;
  chosen = false;

  if (OB_FAIL(tenant_id_set_.exist_refactored(tenant_id))) {
    if (OB_HASH_EXIST == ret) {
      chosen = true;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      chosen = false;
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("tenant_id_set exist_refactored fail", KR(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObLogTenantMgr::get_all_tenant_ids(std::vector<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.clear();

  for (typename common::hash::ObHashSet<uint64_t>::const_iterator iter = tenant_id_set_.begin();
      OB_SUCC(ret) && iter != tenant_id_set_.end(); ++iter) {
    const uint64_t tenant_id = iter->first;
    (void)tenant_ids.push_back(tenant_id);
    _ISTAT("[GET_TENANT] TENANT=%ld", tenant_id);
  }

  return ret;
}

// the following function needs to be compatible, pre-226 versions only had time zone tables for system tenants, so only one map needs to be maintained.
// After 226 the time zone table is split into tenant level and a tz_info_map needs to be maintained for each tenant
// Obj2Str uses this interface to get the tz_info_wrap of a particular tenant for obj to string conversion
int ObLogTenantMgr::get_tenant_tz_wrap(const uint64_t tenant_id, ObTimeZoneInfoWrap *&tz_info_wrap)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  const uint64_t tz_tenant_id = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 ? tenant_id : OB_SYS_TENANT_ID;

  if (OB_SYS_TENANT_ID == tz_tenant_id) {
    tz_info_wrap = &TCTX.tz_info_wrap_;
  } else {
    ObLogTenantGuard tenant_guard;
    ObLogTenant *tenant = NULL;
    if (OB_FAIL(get_tenant_guard(tz_tenant_id, guard))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_ERROR("tenant not exist when get tz_wrap", KR(ret), K(tenant_id), K(tz_tenant_id));
      } else {
        LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id), K(tz_tenant_id));
      }
    } else if (OB_ISNULL(tenant = guard.get_tenant())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid tenant", KR(ret), K(tenant), K(tenant_id), K(tz_tenant_id));
    } else {
      tz_info_wrap = tenant->get_tz_info_wrap();
    }
  }

  return ret;
}

int ObLogTenantMgr::get_tenant_tz_map(const uint64_t tenant_id,
    ObTZInfoMap *&tz_info_map)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  const uint64_t tz_tenant_id = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 ? tenant_id : OB_SYS_TENANT_ID;

  if (OB_SYS_TENANT_ID == tz_tenant_id) {
    tz_info_map = &TCTX.tz_info_map_;
  } else {
    ObLogTenantGuard tenant_guard;
    ObLogTenant *tenant = NULL;
    if (OB_FAIL(get_tenant_guard(tz_tenant_id, guard))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_ERROR("tenant not exist when get tz_wrap", KR(ret), K(tenant_id), K(tz_tenant_id));
      } else {
        LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id), K(tz_tenant_id));
      }
    } else if (OB_ISNULL(tenant = guard.get_tenant())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid tenant", KR(ret), K(tenant), K(tenant_id), K(tz_tenant_id));
    } else {
      tz_info_map = tenant->get_tz_info_map();
    }
  }
  return ret;
}

bool ObLogTenantMgr::TenantPrinter::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  if (NULL != tenant) {
    tenant->print_stat_info();

    if (tenant->is_offlined()) {
      offline_tenant_count_++;
    } else {
      serving_tenant_count_++;
    }
    (void)tenant_ids_.push_back(tid.tenant_id_);
    (void)cf_handles_.push_back(tenant->get_cf());
  }
  return true;
}

void ObLogTenantMgr::print_stat_info()
{
  TenantPrinter printer;
  (void)tenant_hash_map_.for_each(printer);
  _LOG_INFO("[STAT] [SERVE_INFO] TENANT_COUNT=%ld(SERVE=%ld,OFFLINE=%ld)",
      printer.offline_tenant_count_ + printer.serving_tenant_count_,
      printer.serving_tenant_count_,
      printer.offline_tenant_count_);
  part_info_map_.print_state("[STAT] [SERVE_INFO] TOTAL_PART_INFO:");

  IObStoreService *store_service = TCTX.store_service_;

  if (NULL != store_service) {
    store_service->get_mem_usage(printer.tenant_ids_, printer.cf_handles_);
  }
}

int ObLogTenantMgr::handle_schema_split_finish(
    const uint64_t ddl_tenant_id,
    const int64_t split_schema_version,
    const int64_t start_serve_tstamp,
    const int64_t timeout)
{
  UNUSED(timeout);
  int ret = OB_SUCCESS;
  TenantAddDDLTableFunc func(start_serve_tstamp, split_schema_version);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_SYS_TENANT_ID != ddl_tenant_id) {
    // If the schema split DDL is for a common tenant, it is not processed
    _ISTAT("[DDL] [SCHEMA_SPLIT_FINISH] ignore schema split DDL of NON-SYS tenant, "
        "DDL_TENANT_ID=%lu TENANT_COUNT=%ld SPLIT_SCHEMA_VERSION=%ld START_SERVER_TSTAMP=%ld",
        ddl_tenant_id, tenant_hash_map_.count(), split_schema_version, start_serve_tstamp);
  } else if (OB_FAIL(tenant_hash_map_.for_each(func))) {
    LOG_ERROR("add ddl table for all tenant fail", KR(ret), K(func.err_), K(start_serve_tstamp),
        K(split_schema_version));
  } else if (OB_UNLIKELY(OB_SUCCESS != func.err_)) {
    LOG_ERROR("add ddl table for all tenant fail", KR(func.err_), K(ddl_tenant_id),
        K(split_schema_version), K(start_serve_tstamp));
    ret = func.err_;
  } else {
    _ISTAT("[DDL] [SCHEMA_SPLIT_FINISH] DDL_TENANT_ID=%lu "
        "TENANT_COUNT=(TOTAL=%ld,SUCC=%ld,OFFLINE=%ld) SPLIT_SCHEMA_VERSION=%ld START_SERVER_TSTAMP=%ld",
        ddl_tenant_id, tenant_hash_map_.count(), func.succ_tenant_count_,
        func.offline_tenant_count_, split_schema_version, start_serve_tstamp);
  }

  return ret;
}

// Add DDL tables for all tenants, except SYS
bool ObLogTenantMgr::TenantAddDDLTableFunc::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  int ret = OB_SUCCESS;
  bool bret = true;
  bool is_create_tenant = false;
  if (OB_ISNULL(tenant)) {
    LOG_ERROR("invalid NULL tenant", K(tenant), K(tid));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SYS_TENANT_ID == tid.tenant_id_) { // SYS tenants don't need to be added
    LOG_INFO("sys tenant need not add ddl table", K(tid), K_(ddl_table_start_serve_tstamp),
        K_(ddl_table_start_schema_version), KPC(tenant));
  } else if (! tenant->is_serving()) {
    // No need to add a DDL partition if the tenant is no longer in service
    LOG_WARN("tenant is not serving, need not add ddl table", KPC(tenant));
  } else if (OB_FAIL(tenant->add_ddl_table(ddl_table_start_serve_tstamp_,
      ddl_table_start_schema_version_, is_create_tenant))) {
    LOG_ERROR("add ddl table fail", KR(ret), K(tid), K(ddl_table_start_schema_version_),
        K(ddl_table_start_serve_tstamp_), K(is_create_tenant), KPC(tenant));
  } else {
    // Normal tenant successfully adds DDL table
  }

  if (OB_SUCCESS != ret) {
    err_ = ret;
    bret = false;
  }
  return bret;
}

int ObLogTenantMgr::recycle_partition(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  bool tenant_can_be_dropped = false;
  uint64_t tenant_id = pkey.get_tenant_id();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // Tenants must wait until all partitions have been reclaimed before they are deleted, no tenant will not exist
      LOG_ERROR("tenant not exist when recycle partition, which should not happen", KR(ret),
          K(tenant_id), K(pkey));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id), K(pkey));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant), K(tenant_id), K(pkey));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant->recycle_partition(pkey, tenant_can_be_dropped))) {
    LOG_ERROR("tenant recycle partition fail", KR(ret), K(tenant_id), K(pkey), KPC(tenant));
  } else if (tenant_can_be_dropped) {
    ISTAT("[DROP_TENANT] [REMOVE_TENANT] remove tenant from tenant map after recycle partition",
        KPC(tenant), K(pkey));

    // If the tenant needs to be deleted, delete the tenant here
    if (OB_FAIL(remove_tenant_(tenant_id, tenant))) {
      LOG_ERROR("remove tenant fail", KR(ret), K(tenant_id), K(pkey));
    }
  }

  ISTAT("[RECYCLE_PARTITION]", KR(ret), K(tenant_id), K(tenant_can_be_dropped), K(pkey), KPC(tenant));

  return ret;
}

int ObLogTenantMgr::set_data_start_schema_version_on_split_mode()
{
  int ret = OB_SUCCESS;
  TenantUpdateStartSchemaFunc func;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret));
  } else if (OB_FAIL(tenant_hash_map_.for_each(func))) {
    LOG_ERROR("set data start schema version for all tenant fail", KR(ret));
  } else {
    // succ
  }

  return ret;
}

bool ObLogTenantMgr::TenantUpdateStartSchemaFunc::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  bool bret = true;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    bret = false;
    LOG_ERROR("tenant is NULL", KR(ret), K(bret), K(tid), K(tenant));
  } else if (OB_FAIL(tenant->update_data_start_schema_version_on_split_mode())) {
    bret = false;
    LOG_ERROR("update_data_start_schema_version_on_split_mode failed",
        KR(ret), K(bret), K(tid), K(tenant));
  } else {
    // succ
  }

  return bret;
}

int ObLogTenantMgr::set_data_start_schema_version_for_all_tenant(const int64_t version)
{
  int ret = OB_SUCCESS;
  SetDataStartSchemaVersionFunc func(version);
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(tenant_hash_map_.for_each(func))) {
    LOG_ERROR("set data start schema version for all tenant fail", KR(ret), K(version));
  } else {
    // success
  }
  return ret;
}

bool ObLogTenantMgr::SetDataStartSchemaVersionFunc::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  UNUSED(tid);
  if (NULL != tenant) {
    tenant->update_global_data_schema_version(data_start_schema_version_);
  }
  return true;
}

} // namespace liboblog
} // namespace oceanbase
