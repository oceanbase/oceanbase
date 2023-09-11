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
 *
 * Tenant Manager for OBCDC(ObLogTenantMgr)
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
#include "ob_log_timezone_info_getter.h"
#include "ob_cdc_tenant_sql_server_provider.h"
#include "lib/utility/ob_macro_utils.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [TenantMgr] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [TenantMgr] " fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
ObLogTenantMgr::ObLogTenantMgr() :
    inited_(false),
    refresh_mode_(RefreshMode::UNKNOWN_REFRSH_MODE),
    tenant_hash_map_(),
    add_tenant_start_ddl_info_map_(),
    ls_info_map_(),
    gindex_cache_(),
    table_id_cache_(),
    ls_add_cb_array_(),
    ls_rc_cb_array_(),
    tenant_id_set_(),
    ls_getter_(),
    enable_oracle_mode_match_case_sensitive_(false)
{
}

ObLogTenantMgr::~ObLogTenantMgr()
{
  destroy();
}

int ObLogTenantMgr::init(
    const bool enable_oracle_mode_match_case_sensitive,
    const RefreshMode &refresh_mode)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogTenantMgr has not been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(tenant_id_set_.create(DEFAULT_TENANT_SET_SIZE))) {
    LOG_ERROR("tenant_id_set_ create fail", KR(ret));
  } else if (OB_FAIL(tenant_hash_map_.init(ObModIds::OB_LOG_TENANT_MAP))) {
    LOG_ERROR("tenant_hash_map init fail", KR(ret));
  } else if (OB_FAIL(add_tenant_start_ddl_info_map_.init(ObModIds::OB_LOG_TENANT_MAP))) {
    LOG_ERROR("add_tenant_start_ddl_info_map_ init fail", KR(ret));
  } else if (OB_FAIL(ls_info_map_.init(CACHED_LS_INFO_COUNT,
      LS_INFO_BLOCK_SIZE,
      ObModIds::OB_LOG_PART_INFO))) {
    LOG_ERROR("init ls info map fail", KR(ret));
  } else if (OB_FAIL(gindex_cache_.init(ObModIds::OB_LOG_GLOBAL_NORMAL_INDEX_CACHE))) {
    LOG_ERROR("global index cache init fail", KR(ret));
  } else if (OB_FAIL(table_id_cache_.init(ObModIds::OB_LOG_TABLE_ID_CACHE))) {
    LOG_ERROR("table id cache init fail", KR(ret));
  } else {
    inited_ = true;
    refresh_mode_ = refresh_mode;
    enable_oracle_mode_match_case_sensitive_ = enable_oracle_mode_match_case_sensitive;

    LOG_INFO("ObLogTenantMgr init succ", K(enable_oracle_mode_match_case_sensitive_),
        "refresh_mode", print_refresh_mode(refresh_mode_));
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObLogTenantMgr::destroy()
{
  if (inited_) {
    inited_ = false;

    refresh_mode_ = RefreshMode::UNKNOWN_REFRSH_MODE;
    tenant_hash_map_.destroy();
    add_tenant_start_ddl_info_map_.destroy();
    ls_info_map_.destroy();
    gindex_cache_.destroy();
    table_id_cache_.destroy();
    ls_add_cb_array_.destroy();
    ls_rc_cb_array_.destroy();
    tenant_id_set_.destroy();
    ls_getter_.destroy();
    enable_oracle_mode_match_case_sensitive_ = false;

    LOG_INFO("ObLogTenantMgr destroy succ");
  }
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
    const int64_t start_tstamp_usec,   /* start serve timestamp for this tenant */
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
        K(is_new_created_tenant), K(is_new_tenant_by_restore), K(start_tstamp_usec), K(sys_schema_version),
        K(tenant_schema_version));
  } else if (is_new_tenant_by_restore) {
    // check is_new_tenant_by_restore is_new_tenant_by_restore = true is subset of is_new_created_tenant=true
    // The sys_schema_version at this point is actually the schema_version of the tenant carried in the backup-restore ddl_stmt_str parsed by ddl_handler
    tenant_schema_version = sys_schema_version;

    ISTAT("[ADD_TENANT] get tenant schema_version of CREATE_TENANT_END_DDL for tenant generate dynamicly by restore",
        K(tenant_id), K(is_new_created_tenant), K(is_new_tenant_by_restore),
        K(start_tstamp_usec), K(sys_schema_version));
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
        K(tenant_schema_version),
        "is_schema_split_mode", TCTX.is_schema_split_mode_);
  } else {
    // Add a tenant that already exists and query for a schema version less than or equal to that timestamp based on the starting service timestamp
    if (OB_FAIL(schema_getter->get_schema_version_by_timestamp(tenant_id, start_tstamp_usec,
        tenant_schema_version, timeout))) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("get_schema_version_by_timestamp fail cause by tenant has been dropped, ignore", KR(ret), K(tenant_id), K(start_tstamp_usec));
      } else {
        LOG_ERROR("get_schema_version_by_timestamp fail", KR(ret), K(tenant_id), K(start_tstamp_usec));
      }
    }

    ISTAT("[ADD_TENANT] get start schema version by start timestamp",
        KR(ret), K(tenant_id), K(is_new_created_tenant), K(start_tstamp_usec),
        K(tenant_schema_version));
  }
  return ret;
}

int ObLogTenantMgr::add_sys_ls_if_needed_(const uint64_t tenant_id,
    ObLogTenant &tenant,
    const int64_t start_tstamp_ns,
    const int64_t tenant_start_schema_version,
    const bool is_new_created_tenant)
{
  int ret = OB_SUCCESS;
  // TODO Support to logic consume SYS tenant LS

  if (OB_FAIL(tenant.add_sys_ls(start_tstamp_ns,
          tenant_start_schema_version,
          is_new_created_tenant))) {
    LOG_WARN("tenant add_sys_ls fail",KR(ret), K(tenant_id), K(tenant));
  }

  ISTAT("[ADD_SYS_LS]", K(tenant_id),
      K(start_tstamp_ns), K(tenant_start_schema_version), K(is_new_created_tenant), KR(ret));

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
    const int64_t start_tstamp_ns,
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
  void *lob_storage_cf_handle = nullptr;
  const int64_t start_tstamp_usec = start_tstamp_ns / NS_CONVERSION;
  int64_t tenant_start_serve_ts_ns = start_tstamp_ns;
  bool use_add_tenant_start_ddl_commit_version = false;

  if (OB_ISNULL(store_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("store_service is NULL", KR(ret));
  } else {
    if (is_online_refresh_mode(refresh_mode_)) {
      // Get the starting schema version of the tenant
      if (OB_FAIL(get_tenant_start_schema_version_(tenant_id, is_new_created_tenant, is_new_tenant_by_restore,
              start_tstamp_usec, sys_schema_version, tenant_start_schema_version, timeout))) {
        if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
          LOG_WARN("get_tenant_start_schema_version_ fail cause tenant has been dropped", KR(ret), K(tenant_id), K(tenant_name),
              K(is_new_created_tenant), K(is_new_tenant_by_restore), K(start_tstamp_usec), K(sys_schema_version));
        } else if (OB_TIMEOUT != ret) {
          LOG_ERROR("get_tenant_start_schema_version_ fail", KR(ret), K(tenant_id), K(tenant_name),
              K(is_new_created_tenant), K(is_new_tenant_by_restore), K(start_tstamp_usec), K(sys_schema_version));
        }
      }
    } else if (is_data_dict_refresh_mode(refresh_mode_)) {
      // TODO tenant_start_schema_version
      tenant_start_schema_version = start_tstamp_usec;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("not support refresh mode", KR(ret), K(refresh_mode_),
          "refresh_mode", print_refresh_mode(refresh_mode_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tenant_start_serve_timestamp_(
      tenant_id, is_new_created_tenant, tenant_start_serve_ts_ns, use_add_tenant_start_ddl_commit_version))) {
    LOG_ERROR("get_tenant_start_serve_timestamp_ failed", KR(ret),
        K(tenant_id), K(tenant_name), K(start_tstamp_ns), K(tenant_start_serve_ts_ns), K(is_new_created_tenant));
  } else if (OB_ENTRY_EXIST == (ret = tenant_hash_map_.contains_key(tid))) {
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
    } else if (OB_FAIL(store_service->create_column_family(
        std::to_string(tenant_id) + ":" + std::string(tenant_name) + ":lob",
        lob_storage_cf_handle))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("create_column_family fail", KR(ret), K(tenant_id), K(tenant_name), K(lob_storage_cf_handle));
      }
    }
    // init tenant
    else if (OB_FAIL(tenant->init(tenant_id, tenant_name, tenant_start_serve_ts_ns, start_seq,
            tenant_start_schema_version, column_family_handle, lob_storage_cf_handle, *this))) {
      LOG_ERROR("tenant init fail", KR(ret), K(tenant_id), K(tenant_name), K(start_tstamp_ns), K(start_seq),
          K(tenant_start_schema_version), K(tenant_start_serve_ts_ns), K(use_add_tenant_start_ddl_commit_version));
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

    if (OB_SUCC(ret)) {
      // start tenant service
      if (OB_FAIL(start_tenant_service_(tenant_id, is_new_created_tenant, is_new_tenant_by_restore, tenant_start_serve_ts_ns,
              tenant_start_schema_version, timeout))) {
        LOG_ERROR("start tenant service fail", KR(ret), K(tenant_id), K(is_new_created_tenant),
            K(start_tstamp_ns), K(tenant_start_serve_ts_ns), K(use_add_tenant_start_ddl_commit_version), K(tenant_start_schema_version));
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != tenant) {
        (void)tenant_hash_map_.del(tid);
        tenant_hash_map_.free_value(tenant);
        tenant = NULL;
      }
    }
  }

  if (OB_SUCC(ret) && is_tenant_served) {
    if (OB_FAIL(add_served_tenant_for_stat_(tenant_name, tenant_id))) {
      LOG_ERROR("trans stat mgr add serverd tenant fail", KR(ret), K(tenant_id), K(tenant_name));
    } else if (OB_FAIL(add_served_tenant_into_set_(tenant_name, tenant_id))) {
      LOG_ERROR("add tenant_id into tenant_id_set fail", KR(ret), K(tenant_id), K(tenant_name));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    ISTAT("[ADD_TENANT]", K(tenant_id), K(tenant_name), K(is_new_created_tenant), K(is_new_tenant_by_restore),
        K(is_tenant_served), K(start_tstamp_ns), K(tenant_start_serve_ts_ns), K(sys_schema_version),
        "with add_tenant_start_ddl", use_add_tenant_start_ddl_commit_version,
        "total_tenant_count", tenant_hash_map_.count());
  }

  return ret;
}

int ObLogTenantMgr::start_tenant_service_(
    const uint64_t tenant_id,
    const bool is_new_created_tenant,
    const bool is_new_tenant_by_restore,
    const int64_t start_tstamp_ns,
    const int64_t tenant_start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  // normal_new_created_tenant means tenant found by ADD_TENANT action AND NOT CREATE by RESTORE
  const bool is_normal_new_created_tenant = is_new_created_tenant && !is_new_tenant_by_restore;
  common::ObArray<share::ObLSID> ls_id_array;

  // Real-time access to tenant structure, tenants should not be non-existent
  if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", KR(ret), K(tenant));
  } else {
    // Note: The sys tenant does not have data dictionary data
    if (is_online_refresh_mode(refresh_mode_) || OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(tenant->add_all_user_tablets_info(timeout))) {
        LOG_ERROR("add_all_user_tablets_info failed", KR(ret), KPC(tenant), K(timeout));
      }

      // get ls ids when is not normal new created tenant
      if (OB_FAIL(ret)) {
      } else if (OB_SYS_TENANT_ID == tenant_id) {
        // sys tenant, do nothing
      } else if (! is_normal_new_created_tenant) {
        if (OB_FAIL(ls_getter_.get_ls_ids(tenant_id, ls_id_array))) {
          LOG_ERROR("ls_getter_ get_ls_ids failed", KR(ret), K(tenant_id), K(ls_id_array));
        }
      }
    } else if (is_data_dict_refresh_mode(refresh_mode_)) {
      ObDictTenantInfoGuard dict_tenant_info_guard;
      ObDictTenantInfo *tenant_info = nullptr;
      ObArray<const datadict::ObDictTableMeta *> table_metas;

      if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(tenant_id, dict_tenant_info_guard))) {
        LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tenant_info is nullptr", K(tenant_id));
      } else if (OB_FAIL(tenant_info->get_table_metas_in_tenant(table_metas))) {
        LOG_ERROR("tenant_info get_table_metas_in_tenant failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant->add_all_user_tablets_info(table_metas, timeout))) {
        LOG_ERROR("add_all_user_tablets_info failed", KR(ret), K(tenant_id), K(table_metas));
      }

      // get ls ids when is not normal new created tenant
      if (OB_FAIL(ret)) {
      } else if (! is_normal_new_created_tenant) {
        const share::ObLSArray &ls_array = tenant_info->get_dict_tenant_meta().get_ls_array();

        ARRAY_FOREACH_N(ls_array, idx, count) {
          if (OB_FAIL(ls_id_array.push_back(ls_array.at(idx)))) {
            LOG_ERROR("ls_id_array push_back failed", KR(ret), K(tenant_id), K(idx), K(ls_id_array));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("not support refresh mode", KR(ret), K(refresh_mode_),
          "refresh_mode", print_refresh_mode(refresh_mode_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_sys_ls_if_needed_(tenant_id, *tenant, start_tstamp_ns,
        tenant_start_schema_version, is_normal_new_created_tenant))) {
    LOG_ERROR("add_sys_ls_if_needed_ fail", KR(ret), K(tenant_id), K(start_tstamp_ns),
        K(tenant_start_schema_version), K(is_new_created_tenant), K(is_normal_new_created_tenant));
  }
  // add all user tables of tenant in such cases:
  // 1. tenant exist when oblog start
  // 2. or tenant create by DDL AND by restore
  // which means, won't add user table of tenant of a normal new tenant(which should not have any user table)
  else if (! is_normal_new_created_tenant) {
    if (OB_SYS_TENANT_ID  == tenant_id) {
      // sys tenant only fetch SYS_LS
    }
    else if (OB_FAIL(tenant->add_all_ls(ls_id_array,
            start_tstamp_ns,
            tenant_start_schema_version,
            timeout))) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        // FIXME: When a schema error (OB_TENANT_HAS_BEEN_DROPPED) is encountered, the table under the tenant is ignored and success is returned
        // Since the deletion of the tenant DDL will definitely be encountered later, there is no need to roll back the successful operation above
        LOG_WARN("schema error, tenant may be dropped. need not add_all_ls", KR(ret),
            K(tenant_id), K(tenant_start_schema_version),
            K(start_tstamp_ns), K(is_new_created_tenant));
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("add_all_ls fail", KR(ret), K(tenant_id), K(tenant_start_schema_version),
            K(start_tstamp_ns), K(is_new_created_tenant));
      }
    }
  }

  return ret;
}

int ObLogTenantMgr::add_tenant(
    const int64_t start_tstamp_ns,
    const int64_t sys_schema_version,
    const char *&tenant_name,
    const int64_t timeout,
    bool &add_tenant_succ)
{
  int ret = OB_SUCCESS;
  bool is_new_created_tenant = false;
  bool is_new_tenant_by_restore = false;
  ObLogSchemaGuard schema_guard;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_id_in_archive(start_tstamp_ns, tenant_id))) {
    LOG_ERROR("GLOGMETADATASERVICE get_tenant_id_in_archive failed", KR(ret), K(tenant_id), K(start_tstamp_ns),
        K(sys_schema_version));
  } else if (OB_FAIL(add_tenant(tenant_id, is_new_created_tenant, is_new_tenant_by_restore, start_tstamp_ns,
          sys_schema_version, schema_guard, tenant_name, timeout, add_tenant_succ))) {
    LOG_ERROR("add tenant fail", KR(ret), K(tenant_id), K(start_tstamp_ns), K(sys_schema_version));
  } else if (OB_UNLIKELY(! add_tenant_succ)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "add tenant failed", K(tenant_id), K(tenant_name));
  }

  return ret;
}

// add tenant for obcdc
//
// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant/database/... not exist, caller should ignore
// @retval other error code             error
int ObLogTenantMgr::add_tenant(
    const uint64_t tenant_id,
    const bool is_new_created_tenant,
    const bool is_new_tenant_by_restore,
    const int64_t start_tstamp_ns,
    const int64_t sys_schema_version,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const int64_t timeout,
    bool &add_tenant_succ)
{
  int ret = OB_SUCCESS;
  add_tenant_succ = false;
  bool is_restore = false;

  LOG_INFO("[ADD_TENANT] BEGIN", K(tenant_id), K(is_new_created_tenant), K(is_new_tenant_by_restore),
      K(start_tstamp_ns), K(sys_schema_version));
  if (is_new_created_tenant || is_new_tenant_by_restore) {
    TCTX.tenant_sql_proxy_.refresh_conn_pool();
  }

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_UNLIKELY(sys_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(tenant_id), K(sys_schema_version));
  } else if (is_meta_tenant(tenant_id)) {
    LOG_INFO("won't add meta tenant", K(tenant_id), K(sys_schema_version));
  } else if (OB_FAIL(ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id))) {
    LOG_ERROR("create and add tenant allocator failed", K(ret), K(tenant_id));
  } else {
    if (is_online_refresh_mode(refresh_mode_)) {
      TenantSchemaInfo tenant_schema_info;
      IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

      if (OB_ISNULL(schema_getter)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid arguments", KR(ret), K(schema_getter));
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
      } else {
        tenant_name = tenant_schema_info.name_;
        is_restore = tenant_schema_info.is_restore_;
      }
    } else if (is_data_dict_refresh_mode(refresh_mode_)) {
      if (OB_SYS_TENANT_ID == tenant_id) {
        // do nothing
      } else if (OB_FAIL(GLOGMETADATASERVICE.refresh_baseline_meta_data(tenant_id, start_tstamp_ns, timeout))) {
        LOG_ERROR("log_meta_data_service refresh_baseline_meta_data failed", KR(ret), K(tenant_id));
      } else {
        LOG_INFO("log_meta_data_service refresh_baseline_meta_data success", K(tenant_id), K(start_tstamp_ns));
      }

      if (OB_SYS_TENANT_ID != tenant_id) {
        ObDictTenantInfoGuard dict_tenant_info_guard;
        ObDictTenantInfo *tenant_info = nullptr;

        if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(tenant_id, dict_tenant_info_guard))) {
          LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tenant_info is nullptr", K(tenant_id));
        } else {
          LOG_INFO("get_tenant_info_guard success", K(tenant_id), KPC(tenant_info));
          tenant_name = tenant_info->get_dict_tenant_meta().get_tenant_name();
          is_restore = tenant_info->get_dict_tenant_meta().is_restore();
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("not support refresh mode", KR(ret), K(refresh_mode_),
          "refresh_mode", print_refresh_mode(refresh_mode_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_meta_tenant(tenant_id)) {
    LOG_INFO("won't add meta tenant", K(tenant_id), K(sys_schema_version));
  } else if (is_restore) {
    // 1. won't add tenant in restore status
    // 2. for new tenant found by ADD_TENENT_END DDL:
    // 2.1. normal new created tenant, start log id will set to 1 to simplify the ADD_PARTITION process, and won't add user tables
    // 2.2. if tenant new created by restore, should query server for start_serve_log_id, and should add user tables.
    LOG_INFO("won't add restore-state tenant", K(tenant_id), K(tenant_name), K(sys_schema_version));
  } else {
    bool is_tenant_served = false;
    // whether the tenant should be added or not, if the tenant is in service, it must be added.
    // Tenants that are not in service(mean not config in oblog config file) may also need to be added, e.g. SYS tenant
    bool need_add_tenant = false;

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
      is_tenant_served, start_tstamp_ns, sys_schema_version, timeout))) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("will not add tenant has been dropped", KR(ret), K(tenant_id), K(tenant_name),
            K(is_new_created_tenant), K(is_new_tenant_by_restore), K(is_tenant_served), K(start_tstamp_ns),
            K(sys_schema_version), K(timeout));
      } else if (OB_TIMEOUT != ret) {
        LOG_ERROR("do add tenant fail", KR(ret), K(tenant_id), K(tenant_name),
            K(is_new_created_tenant), K(is_new_tenant_by_restore), K(is_tenant_served), K(start_tstamp_ns),
            K(sys_schema_version), K(timeout));
      }
    } else {
      // add tenant success
      add_tenant_succ = true;
    }
  }

  // 1. NOTE: currently add_tenant is NOT serialize executed, thus reset all info in add_tenant_start_ddl_info_map_ is NOT safe.
  // Consider the following sequence:
  // (meta_tenant add_tenant_start -> user_tenant add_tenant_start -> meta_tenant add_tenant_end -> user_tenant add_tenant_end).
  // reset tenant_start_ddl_info for specified tenant_id regardless of ret exclude OB_TIMEOUT(The add_tenant interface will be externally retried)
  //
  // 2. The tenant start ddl info need be removed in case of tenant already dropped or not serve and other unexpected case,
  // otherwise global_heartbeat will be stucked.
  if (OB_TIMEOUT != ret) {
    try_del_tenant_start_ddl_info_(tenant_id);
  }

  if (! add_tenant_succ) {
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id);
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
  const bool is_schema_split_mode = TCTX.is_schema_split_mode_;
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
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("get_first_trans_end_schema_version fail cause tenant dropped", KR(ret), K(tenant_id),
            K(first_schema_version));
      } else {
        LOG_ERROR("get_first_trans_end_schema_version fail", KR(ret), K(tenant_id),
            K(first_schema_version));
      }
    }
  } else if (OB_UNLIKELY(first_schema_version <= 0)) {
    LOG_ERROR("tenant first schema versioin is invalid", K(tenant_id), K(first_schema_version));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // success
  }

  return ret;
}

// TODO physical backup restore tenant may need redesign this function.
int ObLogTenantMgr::get_tenant_start_serve_timestamp_(
    const uint64_t tenant_id,
    const bool is_new_created_tenant,
    int64_t &tenant_start_tstamp_ns,
    bool &use_add_tenant_start_ddl_commit_version)
{
  int ret = OB_SUCCESS;
  int64_t add_tenant_start_ddl_commit_version = OB_INVALID_VERSION;
  use_add_tenant_start_ddl_commit_version = false;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || is_meta_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (! is_new_created_tenant) {
    // use tenant_start_tstamp_ns directly if not new_created tenant.
  } else if (OB_FAIL(get_add_tenant_start_ddl_info_(tenant_id, add_tenant_start_ddl_commit_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("can't find registed add_tenant_start_ddl_info, use start_tstamp_usec instead",
          KR(ret), K(tenant_id), K(tenant_start_tstamp_ns));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get_add_tenant_start_ddl_info_ failed", KR(ret), K(tenant_id), K(tenant_start_tstamp_ns));
    }
  } else {
    // NOTE:
    // 1. tenant will start with gts greater than sys tenant add_tenant_start_ddl_commit_version
    // when time of ALL OBServers are synced.
    // 2. use add_tenant_start_ddl_commit_version + 1 cause ObLogTenant::init will use
    // tenant_start_tstamp_ns - 1 to init tenant output_trans_commit_version and output_global_heartbeat.
    add_tenant_start_ddl_commit_version += 1;

    if (tenant_start_tstamp_ns > add_tenant_start_ddl_commit_version) {
      tenant_start_tstamp_ns = add_tenant_start_ddl_commit_version;
      use_add_tenant_start_ddl_commit_version = true;
    }
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
  ObCDCTenantSQLServerProvider *tenant_server_provider = static_cast<ObCDCTenantSQLServerProvider*>(TCTX.tenant_server_provider_);
  void *cf = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret));
  } else if (OB_ISNULL(store_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("store_service is NULL", KR(ret));
  } else if (OB_ISNULL(tenant_server_provider)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_server_provider is NULL", KR(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is NULL", KR(ret), K(tenant_id), K(tenant));
  } else if (OB_ISNULL(cf = tenant->get_cf())) {
    ret= OB_ERR_UNEXPECTED;
    LOG_ERROR("cf is NULL", KR(ret), K(tid), KPC(tenant));
  } else if (OB_FAIL(store_service->drop_column_family(cf))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service drop_column_family fail", KR(ret), K(tid), KPC(tenant));
    }
  } else if (OB_FAIL(tenant_hash_map_.del(tid))) {
    LOG_ERROR("tenant_hash_map_ del failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_server_provider->del_tenant(tenant_id))) {
    LOG_WARN("delete tenant_server_info from tenant_server_provider failed", KR(ret), K(tenant_id));
    // should not affect following process for remove tenant.
    ret = OB_SUCCESS;
  } else {
    ObCDCTimeZoneInfoGetter::get_instance().remove_tenant_tz_info(tenant_id);
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
    LOG_ERROR_RET(OB_NOT_INIT, "ObLogTenantMgr has not inited");
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

int ObLogTenantMgr::get_sys_ls_progress(uint64_t &tenant_id,
    int64_t &sys_ls_min_progress,
    palf::LSN &sys_ls_min_handle_log_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret));
  } else if (OB_UNLIKELY(tenant_id_set_.size() <= 0)) {
    ret = OB_EMPTY_RESULT;
    LOG_INFO("no valid tenant is in serve", KR(ret));
  } else {
    TenantDDLProgessGetter getter;

    if (OB_FAIL(tenant_hash_map_.for_each(getter))) {
      LOG_ERROR("tenant_hash_map_ for each fail", KR(ret));
    } else {
      tenant_id = getter.tenant_id_;
      int64_t min_add_tenant_start_ddl_commit_version = OB_INVALID_VERSION;
      sys_ls_min_progress = getter.sys_ls_min_progress_;
      sys_ls_min_handle_log_lsn = getter.sys_ls_min_handle_log_lsn_;

      if (OB_FAIL(get_min_add_tenant_start_ddl_commit_version_(min_add_tenant_start_ddl_commit_version))) {
        LOG_WARN("get_min_add_tenant_start_ddl_commit_version_ failed", KR(ret));
      } else if (OB_UNLIKELY(OB_INVALID_VERSION != min_add_tenant_start_ddl_commit_version
          && min_add_tenant_start_ddl_commit_version < sys_ls_min_progress)) {
        LOG_DEBUG("use min_add_tenant_start_ddl_commit_version as sys_ls_min_progress for SYS_TENANT",
            K(min_add_tenant_start_ddl_commit_version), K(sys_ls_min_progress), K(sys_ls_min_handle_log_lsn));
        sys_ls_min_progress = min_add_tenant_start_ddl_commit_version;
      }
    }
  }

  return ret;
}

ObLogTenantMgr::TenantDDLProgessGetter::TenantDDLProgessGetter() :
    tenant_id_(OB_INVALID_TENANT_ID),
    sys_ls_min_progress_(OB_INVALID_TIMESTAMP),
    sys_ls_min_handle_log_lsn_(OB_INVALID_ID)
{
}

bool ObLogTenantMgr::TenantDDLProgessGetter::operator()(const TenantID &tid,
    ObLogTenant *tenant)
{
  // Ignore offlined tenants
  if (NULL != tenant && tenant->is_serving()) {
    int64_t sys_ls_progress = tenant->get_sys_ls_progress();
    palf::LSN sys_ls_handle_log_lsn = tenant->get_handle_log_lsn();

    if (OB_INVALID_TIMESTAMP == sys_ls_min_progress_) {
      sys_ls_min_progress_ = sys_ls_progress;
      tenant_id_ = tid.tenant_id_;
    } else {
      if (sys_ls_progress < sys_ls_min_progress_) {
        sys_ls_min_progress_ = sys_ls_progress;
        tenant_id_ = tid.tenant_id_;
      }
    }

    if (OB_INVALID_ID == sys_ls_min_handle_log_lsn_) {
      sys_ls_min_handle_log_lsn_ = sys_ls_handle_log_lsn;
    } else {
      sys_ls_min_handle_log_lsn_ = std::min(sys_ls_handle_log_lsn, sys_ls_min_handle_log_lsn_);
    }

  }

  return true;
}

int ObLogTenantMgr::register_ls_add_callback(LSAddCallback *callback)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(callback));
  } else if (OB_FAIL(ls_add_cb_array_.push_back(reinterpret_cast<int64_t>(callback)))) {
    LOG_ERROR("push_back into ls_add_cb_array fail", KR(ret), K(callback));
  } else {
    // succ
  }

  return ret;
}

int ObLogTenantMgr::register_ls_recycle_callback(LSRecycleCallback *callback)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(callback)) {
    LOG_ERROR("invalid argument", K(callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ls_rc_cb_array_.push_back(reinterpret_cast<int64_t>(callback)))) {
    LOG_ERROR("push_back into ls_rc_cb_array fail", KR(ret), K(callback));
  } else {
    // succ
  }
  return ret;
}

int ObLogTenantMgr::add_all_tenants(const int64_t start_tstamp_ns,
    const int64_t sys_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret));
  } else if (OB_UNLIKELY(start_tstamp_ns <= 0) || OB_UNLIKELY(sys_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(start_tstamp_ns), K(sys_schema_version));
  } else if (OB_FAIL(get_tenant_ids_(start_tstamp_ns, sys_schema_version, timeout, tenant_ids))) {
    LOG_ERROR("get_tenant_ids_ failed", KR(ret), K(start_tstamp_ns), K(sys_schema_version), K(tenant_ids));
  } else {
    int64_t chosen_tenant_count = 0;
    bool is_new_created_tenant = false;
    bool is_new_tenant_by_restore = false;
    const int64_t total_tenant_count = tenant_ids.count();
    const bool enable_filter_sys_tenant = TCTX.enable_filter_sys_tenant_;
    ISTAT("[ADD_ALL_TENANTS] BEGIN", K(sys_schema_version), K(start_tstamp_ns),
        "start_tstamp_ns", NTS_TO_STR(start_tstamp_ns), K(total_tenant_count), K(tenant_ids));

    // add all tenants
    for (int64_t index = 0; OB_SUCCESS == ret && index < total_tenant_count; index++) {
      const uint64_t tenant_id = tenant_ids.at(index);
      bool add_tenant_succ = false;
      ObLogSchemaGuard schema_guard;
      const char *tenant_name = (OB_SYS_TENANT_ID == tenant_id) ? "sys" : nullptr;

      if (OB_SYS_TENANT_ID == tenant_id && enable_filter_sys_tenant) {
        ISTAT("[FILTE] sys tenant is filtered", K(tenant_id), K(enable_filter_sys_tenant));
      } else {
        if (OB_FAIL(add_tenant(tenant_id, is_new_created_tenant, is_new_tenant_by_restore, start_tstamp_ns,
                sys_schema_version, schema_guard, tenant_name, timeout, add_tenant_succ))) {
          if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
            LOG_INFO("tenant has dropped, ignore it", KR(ret), K(tenant_id), K(sys_schema_version),
                K(index), K(start_tstamp_ns));
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("add tenant fail", KR(ret), K(tenant_id), K(start_tstamp_ns),K(sys_schema_version),
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
    } // for
      //
    if (OB_SUCC(ret)) {
      if (is_data_dict_refresh_mode(refresh_mode_)) {
        if (OB_FAIL(GLOGMETADATASERVICE.finish_when_all_tennats_are_refreshed())) {
          LOG_ERROR("log_meta_data_service finish_when_all_tennats_are_refreshed failed", KR(ret));
        } else {
          LOG_INFO("log_meta_data_service finish_when_all_tennats_are_refreshed success");
        }
      }
    }

    ISTAT("[ADD_ALL_TENANTS] DONE", KR(ret), K(chosen_tenant_count), K(total_tenant_count),
        K(sys_schema_version), K(start_tstamp_ns), "start_tstamp_ns", NTS_TO_STR(start_tstamp_ns), K(tenant_ids));
  }

  return ret;
}

int ObLogTenantMgr::get_tenant_ids_(
    const int64_t start_tstamp_ns,
    const int64_t sys_schema_version,
    const int64_t timeout,
    common::ObIArray<uint64_t> &tenant_id_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret));
  } else if (is_online_refresh_mode(refresh_mode_)) {
    ObLogSchemaGuard sys_schema_guard;
    IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

    if (OB_ISNULL(schema_getter)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("schema_getter is invalid", KR(ret), K(schema_getter));
    }
    // Get the schema guard for the SYS tenant
    // Since get_available_tenant_ids() cannot use lazy mode
    else if (OB_FAIL(schema_getter->get_fallback_schema_guard(OB_SYS_TENANT_ID, sys_schema_version,
            timeout, sys_schema_guard))) {
      LOG_ERROR("get_fallback_schema_guard of SYS tenant fail", KR(ret), K(sys_schema_version), K(timeout));
    }
    // get available tenant id list
    else if (OB_FAIL(sys_schema_guard.get_available_tenant_ids(tenant_id_list, timeout))) {
      LOG_ERROR("get_available_tenant_ids fail", KR(ret), K(tenant_id_list), K(timeout));
    } else if (OB_FAIL(ls_getter_.init(tenant_id_list))) {
      LOG_ERROR("ObLogLsGetter init fail", KR(ret), K(tenant_id_list));
    }
  } else if (is_data_dict_refresh_mode(refresh_mode_)) {
    IObLogSysTableHelper *systable_helper = TCTX.systable_helper_;
    ObSEArray<IObLogSysTableHelper::TenantInfo, 16> tenant_info_list;
    bool done = false;
    tenant_id_list.reset();

    if (OB_ISNULL(systable_helper)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("systable_helper is NULL", KR(ret));
    } else {
      while (! done && OB_SUCCESS == ret) {
        if (OB_FAIL(systable_helper->query_tenant_info_list(tenant_info_list))) {
          LOG_WARN("systable_helper query_tenant_ls_info fail", KR(ret), K(tenant_id_list));
        } else {
          const int64_t tenant_info_list_cnt = tenant_info_list.count();
          ARRAY_FOREACH_N(tenant_info_list, idx, tenant_info_list_cnt) {
            const IObLogSysTableHelper::TenantInfo &tenant_info = tenant_info_list.at(idx);
            bool chosen = true;
            if (OB_FAIL(filter_tenant(tenant_info.tenant_name.ptr(), chosen))) {
              LOG_ERROR("filter_tenant failed", K(tenant_info), K(chosen), K(start_tstamp_ns),
                  K(sys_schema_version));
            } else if (! chosen && OB_SYS_TENANT_ID != tenant_info.tenant_id) {
              // tenant have been filtered
              LOG_INFO("tenant has been filtered in advance", K(chosen), K(tenant_info), K(tenant_id_list));
            } else if (OB_FAIL(tenant_id_list.push_back(tenant_info.tenant_id))) {
              LOG_ERROR("push back tenant_id_list failed", K(tenant_info), K(tenant_id_list),
                  K(tenant_info_list), K(chosen));
            } else {
              LOG_INFO("tenant hasn't been filtered in advance", K(chosen), K(tenant_info), K(tenant_id_list));
            }
          }

          if (OB_SUCC(ret)) {
            done = true;
          }
        }

        if (OB_NEED_RETRY == ret) {
          ret = OB_SUCCESS;
          ob_usleep(100L * 1000L);
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("refresh_mode not support", KR(ret), K(refresh_mode_),
        "refresh_mode", print_refresh_mode(refresh_mode_));
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
    (void)lob_storage_cf_handles_.push_back(tenant->get_lob_storage_cf_handle());
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
  ls_info_map_.print_state("[STAT] [SERVE_INFO] TOTAL_LS_INFO:");

  IObStoreService *store_service = TCTX.store_service_;

  if (NULL != store_service) {
    store_service->get_mem_usage(printer.tenant_ids_, printer.cf_handles_);
    store_service->get_mem_usage(printer.tenant_ids_, printer.lob_storage_cf_handles_);
  }
}

int ObLogTenantMgr::recycle_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  bool tenant_can_be_dropped = false;
  const uint64_t tenant_id = tls_id.get_tenant_id();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // Tenants must wait until all partitions have been reclaimed before they are deleted, no tenant will not exist
      LOG_ERROR("tenant not exist when recycle ls, which should not happen", KR(ret),
          K(tenant_id), K(tls_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id), K(tls_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("invalid tenant", K(tenant), K(tenant_id), K(tls_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant->recycle_ls(tls_id, tenant_can_be_dropped))) {
    LOG_ERROR("tenant recycle ls fail", KR(ret), K(tenant_id), K(tls_id), KPC(tenant));
  } else if (tenant_can_be_dropped) {
    ISTAT("[DROP_TENANT] [REMOVE_TENANT] remove tenant from tenant map after recycle ls",
        KPC(tenant), K(tls_id));

    // If the tenant needs to be deleted, delete the tenant here
    if (OB_FAIL(remove_tenant_(tenant_id, tenant))) {
      LOG_ERROR("remove tenant fail", KR(ret), K(tenant_id), K(tls_id));
    }
  }

  ISTAT("[RECYCLE_PARTITION]", KR(ret), K(tenant_id), K(tenant_can_be_dropped), K(tls_id), KPC(tenant));

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
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited");
  } else if (OB_FAIL(tenant_hash_map_.for_each(func))) {
    LOG_ERROR("set data start schema version for all tenant fail", KR(ret), K(version));
  } else {
    // success
  }
  return ret;
}

int ObLogTenantMgr::update_committer_global_heartbeat(const int64_t global_heartbeat)
{
  int ret = OB_SUCCESS;
  GlobalHeartbeatUpdateFunc func(global_heartbeat);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret), K_(inited));
  } else if (OB_FAIL(tenant_hash_map_.for_each(func))) {
    LOG_ERROR("update_committer_global_heartbeat failed", KR(ret), K(global_heartbeat));
  } else {
    // success.
  }

  return ret;
}

int ObLogTenantMgr::get_min_output_checkpoint_for_all_tenant(int64_t &min_output_checkpoint)
{
  int ret = OB_SUCCESS;
  TenantOutputCheckpointGetter output_checkpoint_getter;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenantMgr has not inited", KR(ret), K_(inited));
  } else if (OB_FAIL(tenant_hash_map_.for_each(output_checkpoint_getter))) {
    LOG_ERROR("iterator each tenant with output_checkpoint_getter failed", KR(ret));
  } else {
    min_output_checkpoint = output_checkpoint_getter.min_output_checkpoint_;
    _STAT(DEBUG, "min_output_checkpoint: %ld(tenant: %lu, delay: %s); max_output_checkpoint: %ld(tenant: %lu, delay: %s)",
        output_checkpoint_getter.min_output_checkpoint_,
        output_checkpoint_getter.min_output_checkpoint_tenant_,
        NTS_TO_DELAY(output_checkpoint_getter.min_output_checkpoint_),
        output_checkpoint_getter.max_output_checkpoint_,
        output_checkpoint_getter.max_output_checkpoint_tenant_,
        NTS_TO_DELAY(output_checkpoint_getter.max_output_checkpoint_));
  }

  return ret;
}

int ObLogTenantMgr::regist_add_tenant_start_ddl_info(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  TenantID tid(tenant_id);
  AddTenantStartDDlInfo *create_tenant_info = NULL;
  add_tenant_start_ddl_info_map_.reset();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_mgr not init", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || is_meta_tenant(tenant_id)
      || OB_INVALID_VERSION == schema_version
      || OB_INVALID_VERSION == commit_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(tenant_id), K(schema_version), K(commit_version));
  } else if (OB_FAIL(add_tenant_start_ddl_info_map_.create(tid, create_tenant_info))) {
    LOG_ERROR("create add_tenant_start_ddl_info failed", KR(ret), K(tid), K(create_tenant_info));
  } else if (OB_ISNULL(create_tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expect valid create_tenant_info", KR(ret), KPC(create_tenant_info));
  } else {
    create_tenant_info->reset(schema_version, commit_version);
    LOG_INFO("regist_add_tenant_start_ddl_info succ", K(tenant_id), KPC(create_tenant_info));
    add_tenant_start_ddl_info_map_.revert(create_tenant_info);
  }

  return ret;
}

int ObLogTenantMgr::get_add_tenant_start_ddl_info_(
    const uint64_t tenant_id,
    int64_t &add_tenant_start_ddl_commit_version)
{
  int ret = OB_SUCCESS;
  TenantID tid(tenant_id);
  AddTenantStartDDlInfo *create_tenant_info = NULL;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_mgr not init", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(add_tenant_start_ddl_info_map_.get(tid, create_tenant_info))) {
    LOG_WARN("get create_tenant_info failed, may be not consume ADD_TENANT_START ddl in sys tenant",
        KR(ret), K(tid));
  } else if (OB_ISNULL(create_tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("create_tenant_info expect not null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(! create_tenant_info->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expect valid create_tenant_info", KR(ret), KPC(create_tenant_info));
  } else {
    add_tenant_start_ddl_commit_version = create_tenant_info->commit_version_;
    add_tenant_start_ddl_info_map_.revert(create_tenant_info);
    // success.
  }

  return ret;
}

int ObLogTenantMgr::get_min_add_tenant_start_ddl_commit_version_(int64_t &commit_version)
{
  int ret = OB_SUCCESS;
  commit_version = OB_INVALID_VERSION;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_mgr not init", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(0 != add_tenant_start_ddl_info_map_.size())) {
    auto min_add_tenant_start_ddl_commit_version_getter = [&commit_version](const TenantID &tid, AddTenantStartDDlInfo *tenant_info)
    {
      const int64_t add_tenant_start_ddl_commit_version = tenant_info->commit_version_;
      if (OB_INVALID_VERSION == commit_version || commit_version > add_tenant_start_ddl_commit_version)
      {
        commit_version = add_tenant_start_ddl_commit_version;
      }
      return true;
    };

    if (OB_FAIL(add_tenant_start_ddl_info_map_.for_each(min_add_tenant_start_ddl_commit_version_getter))) {
      LOG_ERROR("get min_add_tenant_start_ddl_commit_version failed", KR(ret), K(commit_version));
    } else {
      LOG_DEBUG("get_min_add_tenant_start_ddl_commit_version", "min_commit_version", commit_version);
    }
  }

  return ret;
}

void ObLogTenantMgr::try_del_tenant_start_ddl_info_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  TenantID tid(tenant_id);

  if (OB_FAIL(add_tenant_start_ddl_info_map_.del(tid))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("try_del_tenant_start_ddl_info_ failed, ignore", KR(ret), K(tenant_id));
    } else {
      LOG_INFO("add_tenant_start_ddl_info is not found, ignore", KR(ret), K(tenant_id));
    }
    // no need return error code.
  } else {
    LOG_INFO("try_del_tenant_start_ddl_info_ succ", K(tenant_id));
  }
}

bool ObLogTenantMgr::SetDataStartSchemaVersionFunc::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  UNUSED(tid);
  if (NULL != tenant) {
    tenant->update_global_data_schema_version(data_start_schema_version_);
  }
  return true;
}

bool ObLogTenantMgr::TenantOutputCheckpointGetter::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  if (OB_NOT_NULL(tenant)) {
    if (tenant->is_serving()) {
      const int64_t tenant_output_checkpoint = tenant->get_committer_output_checkpoint();

      if (tenant_output_checkpoint < min_output_checkpoint_
          || OB_INVALID_VERSION == min_output_checkpoint_) {
        min_output_checkpoint_ = tenant_output_checkpoint;
        min_output_checkpoint_tenant_ = tenant->get_tenant_id();
      }

      if (tenant_output_checkpoint > max_output_checkpoint_
          || OB_INVALID_VERSION == max_output_checkpoint_) {
        max_output_checkpoint_ = tenant_output_checkpoint;
        max_output_checkpoint_tenant_ = tenant->get_tenant_id();
      }
    } else {
      LOG_DEBUG("collect tenant output_checkpoint skip not serving tenant", K(tid), KPC(tenant));
    }
  }

  return true;
}

bool ObLogTenantMgr::GlobalHeartbeatUpdateFunc::operator()(const TenantID &tid, ObLogTenant *tenant)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;

  if (OB_NOT_NULL(tenant) && tenant->is_serving()) {
    if (OB_FAIL(tenant->update_committer_global_heartbeat(global_heartbeat_))) {
      LOG_ERROR("update_tenant_committer_global_heartbeat", KR(ret), K(tid), K_(global_heartbeat), KPC(tenant));
      bool_ret = false;
    }
  }

  return bool_ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef ISTAT
#undef _ISTAT
