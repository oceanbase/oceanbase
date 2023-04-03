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
 * PartMgr is used to manage logstream for OBCDC
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_part_mgr.h"

#include <fnmatch.h>                                  // FNM_CASEFOLD
#include "share/schema/ob_schema_struct.h"            // USER_TABLE
#include "share/inner_table/ob_inner_table_schema.h"  // OB_ALL_DDL_OPERATION_TID
#include "share/schema/ob_part_mgr_util.h"            // ObTablePartitionKeyIter

#include "ob_log_schema_getter.h"                     // IObLogSchemaGetter, ObLogSchemaGuard
#include "ob_log_utils.h"                             // is_ddl_table
#include "ob_log_config.h"                            // TCONF
#include "ob_log_instance.h"                          // TCTX
#include "ob_log_table_matcher.h"                     // IObLogTableMatcher
#include "ob_log_tenant.h"                            // ObLogTenant

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [PartMgr] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [PartMgr] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

#define CHECK_SCHEMA_VERSION(check_schema_version, fmt, arg...) \
    do { \
      if (OB_UNLIKELY(check_schema_version < ATOMIC_LOAD(&cur_schema_version_))) { \
        if (ATOMIC_LOAD(&enable_check_schema_version_)) { \
          LOG_ERROR(fmt, K(tenant_id_), K(cur_schema_version_), K(check_schema_version), ##arg); \
          if (!TCONF.skip_reversed_schema_verison) { \
            ret = OB_INVALID_ARGUMENT; \
          } \
        } \
      } else if (OB_UNLIKELY(! ATOMIC_LOAD(&enable_check_schema_version_))) { \
        ATOMIC_SET(&enable_check_schema_version_, true); \
      } \
    } while (0)

#define PROXY_INFO_TABLE_NAME "ob_all_proxy"
#define PROXY_CONFIG_TABLE_OLD_NAME "ob_all_proxy_config"
#define PROXY_CONFIG_TABLE_NAME "ob_all_proxy_app_config"
#define PROXY_STAT_TABLE_NAME "ob_all_proxy_stat"
#define PROXY_KV_TABLE_NAME "ob_all_proxy_kv_table"
#define PROXY_VIP_TENANT_TABLE_NAME "ob_all_proxy_vip_tenant"
#define PROXY_VIP_TENANT_TABLE_OLD_NAME "ob_all_proxy_vip_tenant_table"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

namespace libobcdc
{
ObLogPartMgr::ObLogPartMgr(ObLogTenant &tenant) : host_(tenant)
{
  reset();
}

ObLogPartMgr::~ObLogPartMgr()
{
  reset();
}

int ObLogPartMgr::init(const uint64_t tenant_id,
    const int64_t start_schema_version,
    const bool enable_oracle_mode_match_case_sensitive,
    GIndexCache &gi_cache,
    TableIDCache &table_id_cache)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || 0 == tenant_id)
      || OB_UNLIKELY(0 >= start_schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguemnts", KR(ret), K(tenant_id), K(start_schema_version));
  } else if (OB_FAIL(schema_cond_.init(common::ObWaitEventIds::OBCDC_PART_MGR_SCHEMA_VERSION_WAIT))) {
    LOG_ERROR("schema_cond_ init fail", KR(ret));
  } else if (OB_FAIL(tablet_to_table_info_.init(tenant_id))) {
    LOG_ERROR("init tablet_to_table_info fail", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    global_normal_index_table_cache_ = &gi_cache;
    table_id_cache_ = &table_id_cache;
    cur_schema_version_ = start_schema_version;
    enable_oracle_mode_match_case_sensitive_ = enable_oracle_mode_match_case_sensitive;
    enable_check_schema_version_ = false;

    inited_ = true;
    LOG_INFO("init PartMgr succ", K(tenant_id), K(start_schema_version));
  }

  return ret;
}

void ObLogPartMgr::reset()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  global_normal_index_table_cache_ = NULL;
  table_id_cache_ = NULL;
  tablet_to_table_info_.destroy();
  cur_schema_version_ = OB_INVALID_VERSION;
  enable_oracle_mode_match_case_sensitive_ = false;
  enable_check_schema_version_ = false;
  schema_cond_.destroy();
}

int ObLogPartMgr::add_all_user_tablets_info(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
  ObLogSchemaGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)
      || OB_UNLIKELY(0 >= cur_schema_version_)
      || OB_ISNULL(schema_getter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K_(tenant_id), K_(cur_schema_version));

  } else if (OB_FAIL(schema_getter->get_fallback_schema_guard(
      tenant_id_, cur_schema_version_, timeout, schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_fallback_schema_guard failed", KR(ret), K_(tenant_id), K_(cur_schema_version));
    }
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_table_schemas_in_tenant failed", KR(ret), K_(tenant_id), K_(cur_schema_version));
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      ObArray<common::ObTabletID> tablet_ids;

      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get invalid table_schema via schema_service for init tablet_to_table_info",
            KR(ret), K_(tenant_id), K_(cur_schema_version));
      } else if (table_schema->has_tablet()) {
        if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
          LOG_ERROR("get_tablet_ids failed", KR(ret), K_(tenant_id), K_(cur_schema_version));
        } else {
          if (OB_FAIL(insert_tablet_table_info_(*table_schema, tablet_ids))) {
            LOG_ERROR("insert_tablet_table_info_ failed", KR(ret), K_(tenant_id),
                KPC(table_schema));
          }
        }
      }
    } // for
  }

  ISTAT("[ADD_ALL_USER_TABLETS_INFO]", KR(ret), K_(tenant_id),
      K_(cur_schema_version), K_(tablet_to_table_info));

  return ret;
}

int ObLogPartMgr::add_all_user_tablets_info(
    const ObIArray<const datadict::ObDictTableMeta *> &table_metas,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K_(tenant_id));
  } else {
    ARRAY_FOREACH_N(table_metas, idx, count) {
      const datadict::ObDictTableMeta *table_meta = table_metas.at(idx);

      if (OB_ISNULL(table_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid table_meta", KR(ret), K_(tenant_id), K_(cur_schema_version));
      } else if (table_meta->has_tablet()) {
        const common::ObTabletIDArray &tablet_ids = table_meta->get_tablet_ids();
        if (OB_FAIL(insert_tablet_table_info_(*table_meta, tablet_ids))) {
          LOG_ERROR("insert_tablet_table_info_ failed", KR(ret), K_(tenant_id),
              KPC(table_meta));

        }
      }
    } // ARRAY_FOREACH_N
  }

  ISTAT("[ADD_ALL_USER_TABLETS_INFO]", KR(ret), K_(tenant_id),
      K_(cur_schema_version), K_(tablet_to_table_info));

  return ret;
}

template<class TableMeta>
int ObLogPartMgr::insert_tablet_table_info_(
    TableMeta &table_meta,
    const common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table_meta.get_table_id();
  ObTableType table_type = table_meta.get_table_type();
  ObCDCTableInfo table_info;
  table_info.reset(table_id, table_type);

  ARRAY_FOREACH_N(tablet_ids, idx, count) {
    const common::ObTabletID &tablet_id = tablet_ids.at(idx);

    if (OB_FAIL(tablet_to_table_info_.insert_tablet_table_info(tablet_id, table_info))) {
      LOG_ERROR("insert_tablet_table_info failed", KR(ret), K(tablet_id), K(table_info));
    }
  }

  return ret;
}

int ObLogPartMgr::add_table(
    const uint64_t table_id,
    const int64_t start_schema_version,
    const int64_t start_serve_tstamp,
    const bool is_create_partition,
    bool &is_table_should_ignore_in_committer,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const char *&db_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool table_is_ignored = false;
  is_table_should_ignore_in_committer = false;
  const ObSimpleTableSchemaV2 *table_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(start_schema_version <= 0)
      || OB_UNLIKELY(start_serve_tstamp <= 0)) {
    LOG_ERROR("invalid argument", K(start_schema_version), K(start_serve_tstamp), K(table_id),
        K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_schema_guard_and_schemas_(table_id, start_schema_version, timeout,
      table_is_ignored, schema_guard, table_schema, tenant_name, db_name))) {
    if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
      LOG_ERROR("get_schema_guard_and_schemas_ fail", KR(ret), K(table_id), K(start_schema_version));
    }
  } else if (table_is_ignored) {
    // table ignored
    if (table_schema->is_tmp_table()) {
      LOG_INFO("add table ddl is ignored in part mgr, and also should be ignored in committer output",
          "table_id", table_id,
          "table_name", table_schema->get_table_name(),
          "is_tmp_table", table_schema->is_tmp_table());
      is_table_should_ignore_in_committer = true;
    }
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(tenant_name) || OB_ISNULL(db_name)) {
    LOG_ERROR("invalid schema", K(table_schema), K(tenant_name), K(db_name));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Requires adding tables in order, encountering a Schema version reversal case,
    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(start_schema_version, "add-table schema version reversed",
        "table_id", table_schema->get_table_id(),
        "table_name", table_schema->get_table_name());

    if (TCONF.test_mode_on) {
      int64_t block_time_us = TCONF.test_mode_block_create_table_ddl_sec * _SEC_;
      if (block_time_us > 0) {
        ISTAT("[ADD_TABLE] [TEST_MODE_ON] block to create table",
            K_(tenant_id), K(table_id), K(block_time_us));
        ob_usleep((useconds_t)block_time_us);
      }
    }

    const ObSimpleTableSchemaV2 *primary_table_schema = NULL;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_get_offline_ddl_origin_table_schema_(*table_schema, schema_guard, timeout,
              primary_table_schema))) {
        LOG_ERROR("try_get_offline_ddl_origin_table_schema_ fail", KR(ret),
            KPC(table_schema), K(primary_table_schema));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, table_schema, tenant_name,
              db_name, timeout, primary_table_schema))) {
        LOG_ERROR("add table fail", KR(ret), K(table_id), K(tenant_name), K(db_name),
            "table_name", table_schema->get_table_name(), K(start_serve_tstamp),
            K(is_create_partition));
      } else {
        // success
      }
    }
  }

  return ret;
}

int ObLogPartMgr::try_get_offline_ddl_origin_table_schema_(const ObSimpleTableSchemaV2 &table_schema,
    ObLogSchemaGuard &schema_guard,
    const int64_t timeout,
    const ObSimpleTableSchemaV2 *&origin_table_schema)
{
  int ret = OB_SUCCESS;
  origin_table_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    // Offline DDL: hidden table
    const bool is_user_hidden_table = table_schema.is_user_hidden_table();

    if (is_user_hidden_table) {
      const uint64_t origin_table_id = table_schema.get_association_table_id();

      if (OB_UNLIKELY(OB_INVALID_ID == origin_table_id)) {
        LOG_ERROR("origin_table_id is not valid", K(origin_table_id), K(table_schema));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(get_simple_table_schema_(origin_table_id, timeout, schema_guard, origin_table_schema))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("get table schema for offline ddl fail", KR(ret),
              K(is_user_hidden_table),
              "hidden_table_id", table_schema.get_table_id(),
              "hidden_table_name", table_schema.get_table_name(),
              K(origin_table_id),
              "origin_table_name", origin_table_schema->get_table_name(),
              "origin_table_state_flag", origin_table_schema->get_table_state_flag());
        }
      } else if (OB_ISNULL(origin_table_schema)) {
        LOG_ERROR("invalid schema", K(origin_table_schema));
        ret = OB_ERR_UNEXPECTED;
      } else {
        LOG_INFO("[OFFLINE_DDL]", K(is_user_hidden_table),
            "hidden_table_id", table_schema.get_table_id(),
            "hidden_table_name", table_schema.get_table_name(),
            K(origin_table_id),
            "origin_table_name", origin_table_schema->get_table_name(),
            "is_offline_ddl_original_table", origin_table_schema->is_offline_ddl_original_table());
      }
    }
  }

  return ret;
}

int ObLogPartMgr::alter_table(const uint64_t table_id,
    const int64_t schema_version_before_alter,
    const int64_t schema_version_after_alter,
    const int64_t start_serve_timestamp,
    ObLogSchemaGuard &old_schema_guard,
    ObLogSchemaGuard &new_schema_guard,
    const char *&old_tenant_name,
    const char *&old_db_name,
    const char *event,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  // In order to support alter table add/drop parition, only need to get the corresponding
  // table_schema based on the old schema version, not the tenant/database schema based on the old version.
  bool table_is_ignored = false;
  const ObSimpleTableSchemaV2 *old_table_schema = NULL;
  const ObSimpleTableSchemaV2 *new_table_schema = NULL;
  // get tenant mode: MYSQL or ORACLE
  // 1. oracle database/table matc needs to be case sensitive
  // 2. mysql match don't needs to be case sensitive
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(schema_version_before_alter <= 0)
      || OB_UNLIKELY(schema_version_after_alter <= 0)) {
    LOG_ERROR("invalid argument", K(schema_version_before_alter), K(schema_version_after_alter),
        K(table_id), K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  }
  // Get the old version of schema
  else if (OB_FAIL(get_schema_guard_and_schemas_(table_id, schema_version_before_alter, timeout,
      table_is_ignored, old_schema_guard, old_table_schema, old_tenant_name, old_db_name))) {
    if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
      LOG_ERROR("get old schemas fail", KR(ret), K(table_id), K(schema_version_before_alter));
    }
  } else if (table_is_ignored) {
    // table is ignored
  }
  // get new schema
  else if (OB_FAIL(get_schema_guard_and_table_schema_(table_id, schema_version_after_alter,
      timeout, new_schema_guard, new_table_schema))) {
    if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
      LOG_ERROR("get schemas fail", KR(ret), K(table_id), K(schema_version_after_alter));
    }
  } else if (OB_ISNULL(old_tenant_name) || OB_ISNULL(old_db_name)) {
    LOG_ERROR("invalid schema", K(old_tenant_name), K(old_db_name));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id_, compat_mode, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id_),
          "compat_mode", print_compat_mode(compat_mode), KPC(new_table_schema));
    }
  } else {
    // Require sequential DDL, encounter Schema version reversal,
    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(schema_version_after_alter, "alter-table schema version reversed",
        "table_id", new_table_schema->get_table_id(),
        "table_name", new_table_schema->get_table_name());

    if (OB_SUCC(ret)) {
      bool table_is_chosen = false;
      bool is_primary_table_chosen = false;

      if (TCONF.test_mode_on) {
        int64_t block_time_us = TCONF.test_mode_block_alter_table_ddl_sec * _SEC_;
        if (block_time_us > 0) {
          ISTAT("[ALTER_TABLE] [TEST_MODE_ON] block to alter table",
              K_(tenant_id), K(table_id), K(block_time_us));
          ob_usleep((useconds_t)block_time_us);
        }
      }

      // Filtering tables to operate only on whitelisted tables
      // Use the old TENANT.DB.TABLE to filter
      //
      // In fact filtering with both the new and old names here is "problematic", as long as we whitelist to the DB level or table level.
      // Both RENAME and ALTER TABLE operations will have problems, for example, if a table that was initially
      // served is not served after RENAME, or if a table that is not served is not served after RENAME.
      // RENAME is serviced, neither of which is currently supported and will have correctness issues.
      if (OB_FAIL(filter_table_(old_table_schema, old_tenant_name, old_db_name, compat_mode,
          table_is_chosen, is_primary_table_chosen))) {
        LOG_ERROR("filter table fail", KR(ret), K(table_id),
            "compat_mode", print_compat_mode(compat_mode), K(old_tenant_name), K(old_db_name));
      } else if (! table_is_chosen) {
        LOG_INFO("table is not served, alter table DDL is filtered", K(table_is_chosen),
            "table_id", old_table_schema->get_table_id(),
            "table_name", old_table_schema->get_table_name(),
            K(old_db_name),
            K(old_tenant_name));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogPartMgr::drop_table(const uint64_t table_id,
    const int64_t schema_version_before_drop,
    const int64_t schema_version_after_drop,
    bool &is_table_should_ignore_in_committer,
    ObLogSchemaGuard &old_schema_guard,
    const char *&tenant_name,
    const char *&db_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  bool table_is_ignored = false;
  is_table_should_ignore_in_committer = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(schema_version_before_drop <= 0)
      || OB_UNLIKELY(schema_version_after_drop <= 0)
      || OB_UNLIKELY(schema_version_before_drop > schema_version_after_drop)) {
    LOG_ERROR("invalid arguments", K(schema_version_before_drop),
        K(schema_version_after_drop), K(cur_schema_version_), K(tenant_id_), K(table_id));
    ret = OB_INVALID_ARGUMENT;
  }
  // TODO: Currently you need to fetch the Schema every time you add a table, this process is time consuming and should consider not fetching the Schema afterwards
  else if (OB_FAIL(get_schema_guard_and_schemas_(table_id, schema_version_before_drop, timeout,
      table_is_ignored, old_schema_guard, table_schema, tenant_name, db_name))) {
    if (OB_TENANT_HAS_BEEN_DROPPED != ret && OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_and_schemas_ fail", KR(ret), K(table_id), K(schema_version_before_drop));
    }
  } else if (table_is_ignored) {
    // table is ignored
    if (table_schema->is_tmp_table()) {
      LOG_INFO("drop table ddl is ignored in part mgr, and also should be ignored in committer output",
          "table_id", table_id,
          "table_name", table_schema->get_table_name(),
          "is_tmp_table", table_schema->is_tmp_table());
      is_table_should_ignore_in_committer = true;
    }
  } else {
    // Ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(schema_version_after_drop, "drop-table schema version reversed",
        "table_id", table_schema->get_table_id(),
        "table_name", table_schema->get_table_name());

    if (OB_SUCC(ret)) {
      if (OB_FAIL(drop_table_(table_schema))) {
        LOG_ERROR("drop table fail", KR(ret), K(table_id));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogPartMgr::add_index_table(const uint64_t table_id,
    const int64_t start_schema_version,
    const int64_t start_serve_tstamp,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const char *&db_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool table_is_ignored = false;
  const ObSimpleTableSchemaV2 *index_table_schema = NULL;
  const ObSimpleTableSchemaV2 *primary_table_schema = NULL;
  uint64_t primary_table_id = OB_INVALID_ID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(start_schema_version <= 0)
      || OB_UNLIKELY(start_serve_tstamp <= 0)) {
    LOG_ERROR("invalid argument", K(start_schema_version), K(start_serve_tstamp), K(table_id),
        K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  }
  // TODO: Currently you need to fetch the Schema every time you add a table, this process is time consuming and should consider not fetching the Schema afterwards
  else if (OB_FAIL(get_schema_guard_and_schemas_(table_id, start_schema_version, timeout,
      table_is_ignored, schema_guard, index_table_schema, tenant_name, db_name))) {
    if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
      LOG_ERROR("get schemas fail", KR(ret), K(table_id), K(start_schema_version));
    }
  } else if (table_is_ignored) {
    // table is ignored
  } else if (OB_ISNULL(index_table_schema) || OB_ISNULL(tenant_name) || OB_ISNULL(db_name)) {
    LOG_ERROR("invalid schema", K(index_table_schema), K(tenant_name), K(db_name));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(OB_INVALID_ID == (primary_table_id = index_table_schema->get_data_table_id()))) {
    LOG_ERROR("primary_table_id is not valid", K(primary_table_id), KPC(index_table_schema));
    ret = OB_ERR_UNEXPECTED;
  // Get the global index table corresponding to the main table schema
  // Get table_schema based on the global index table schema, whitelist filter based on the master table,
  // If the master table matches, the global index table also matches; otherwise it does not match
  } else if (OB_FAIL(get_simple_table_schema_(primary_table_id, timeout, schema_guard, primary_table_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schema fail", KR(ret),
          "index_table_id", index_table_schema->get_table_id(),
          "index_table_name", index_table_schema->get_table_name(),
          K(primary_table_id), "primary_table_name", primary_table_schema->get_table_name());
    }
  } else if (OB_ISNULL(primary_table_schema)) {
    LOG_ERROR("invalid schema", K(primary_table_schema));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Requires adding tables in order, encountering a Schema version reversal case,
    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(start_schema_version, "add-index-table schema version reversed",
        "table_id", index_table_schema->get_table_id(),
        "table_name", index_table_schema->get_table_name());

    if (OB_SUCC(ret)) {
      const bool is_create_partition = true;
      if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, index_table_schema,
          tenant_name, db_name, timeout, primary_table_schema))) {
        LOG_ERROR("add table fail", KR(ret),
            "index_table_id", table_id,
            "index_table_name", index_table_schema->get_table_name(),
            K(tenant_name),
            K(db_name),
            "is_global_normal_index_table", index_table_schema->is_global_normal_index_table(),
            "is_global_unique_index_table",  index_table_schema->is_global_unique_index_table(),
            K(primary_table_id), "primary_table_name", primary_table_schema->get_table_name(),
            K(start_schema_version), K(start_serve_tstamp),
            K(is_create_partition));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogPartMgr::drop_index_table(const uint64_t table_id,
    const int64_t schema_version_before_drop,
    const int64_t schema_version_after_drop,
    ObLogSchemaGuard &old_schema_guard,
    const char *&tenant_name,
    const char *&db_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  bool table_is_ignored = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(schema_version_before_drop <= 0)
      || OB_UNLIKELY(schema_version_after_drop <= 0)
      || OB_UNLIKELY(schema_version_before_drop > schema_version_after_drop)) {
    LOG_ERROR("invalid arguments", K(schema_version_before_drop),
        K(schema_version_after_drop), K(cur_schema_version_), K(table_id), K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  }
  // TODO: Currently you need to fetch the Schema every time you add a table, this process is time consuming and should consider not fetching the Schema afterwards
  else if (OB_FAIL(get_schema_guard_and_schemas_(table_id, schema_version_before_drop, timeout,
      table_is_ignored, old_schema_guard, table_schema, tenant_name, db_name))) {
    if (OB_TENANT_HAS_BEEN_DROPPED != ret && OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_and_schemas_ fail", KR(ret), K(table_id), K(schema_version_before_drop));
    }
  } else if (table_is_ignored) {
    // table is ignored
  } else {
    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(schema_version_after_drop, "drop-index-table schema version reversed",
        "table_id", table_schema->get_table_id(),
        "table_name", table_schema->get_table_name());

    // drop_table_ supports handling of global general indexes and globally unique indexed tables
    // 1. for globally unique indexes, perform delete logic
    // 2. For global common indexes, clear the cache
    if (OB_SUCC(ret)) {
      if (OB_FAIL(drop_table_(table_schema))) {
        LOG_ERROR("drop table fail", KR(ret), "index_table_id", table_id,
            "index_table_name", table_schema->get_table_name(),
            "is_global_normal_index_table", table_schema->is_global_normal_index_table(),
            "is_global_unique_index_table",  table_schema->is_global_unique_index_table());
      } else {
        // succ
      }
    }
  }

  return ret;
}

// add all tables of current tenant
//
// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   caller should ignore if tenant/database not exist
// @retval other error code             fail
int ObLogPartMgr::add_all_tables(
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
  const int64_t served_part_count_before = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(start_serve_tstamp <= 0)
      || OB_UNLIKELY(start_schema_version <= 0)
      || OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid argument", K(start_serve_tstamp), K(start_schema_version), K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  }
  // Get schema guard based on tenant_id
  // use fallback mode to refresh schema
  // Because the following is to get the full schema, some interfaces only support fallback mode, e.g. get_table_schemas_in_tenant()
  else if (OB_FAIL(schema_getter->get_fallback_schema_guard(tenant_id_, start_schema_version,
      timeout, schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_fallback_schema_guard fail", KR(ret), K(tenant_id_), K(start_schema_version),
          K(start_serve_tstamp));
    }
  }
  // add all tables
  else if (OB_FAIL(do_add_all_tables_(
      schema_guard,
      start_serve_tstamp,
      start_schema_version,
      timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("add all tables fail", KR(ret), K(tenant_id_), K(start_serve_tstamp),
          K(start_schema_version));
    }
  } else {
    const int64_t total_served_part_count = 0;
    ISTAT("[ADD_ALL_TABLES_AND_TABLEGROUPS]", K_(tenant_id),
        K(start_serve_tstamp),
        K(start_schema_version),
        "tenant_served_part_count", total_served_part_count - served_part_count_before,
        K(total_served_part_count));
  }

  return ret;
}

// add normal user tables
int ObLogPartMgr::do_add_all_tables_(
    ObLogSchemaGuard &schema_guard,
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const bool is_create_partition = false;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;

  // get_table_schemas_in_tenant will fetch all table schema at this time, including primary tables, index tables
  if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schemas in tenant fail", KR(ret), K(tenant_id_), K(start_schema_version));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
    const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
    bool table_is_ignored = false;
    const char *tenant_name = NULL;
    const char *db_name = NULL;
    // 1. You need to get the primary table schema when dealing with global index tables/unique index tables
    // 2. You need to get the origin table schema when dealing with Offline DDL
    const ObSimpleTableSchemaV2 *primary_table_schema = NULL;

    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table_schema is null", K(i), K(table_schemas), K(tenant_id_));
    } else if (table_schema->is_sys_table()) {
      // skip
    }
    // get tenant、db schema
    else if (OB_FAIL(get_schema_info_based_on_table_schema_(table_schema, schema_guard, timeout,
        table_is_ignored, tenant_name, db_name))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("get schemas fail", KR(ret), "table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(), K(start_schema_version));
      }
    } else if (table_is_ignored) {
      // Tables are ignored
      // Globally indexed tables are not ignored
      // Uniquely indexed tables are not ignored
      // 1. The get_schemas_based_on_table_schema_ function does not filter global indexed tables, for consistency and correctness, because currently
      // get_table_schemas returns the table schema array, ensuring that the main table comes first and the indexed tables come second, in ascending order by table id ,
      // 2. here does not rely on the schema interface guarantee, global index table filtering first get the corresponding main table, then based on the main table to complete the whitelist filtering
      // 3. The get_schema_guard_and_table_schema_ function does not filter unique index tables, it is used to add TableIDCache
    } else if (table_schema->is_global_index_table() || table_schema->is_unique_index()) {
      uint64_t primary_table_id = table_schema->get_data_table_id();

      if (OB_UNLIKELY(OB_INVALID_ID == primary_table_id)) {
        LOG_ERROR("primary_table_id is not valid", K(primary_table_id), KPC(table_schema));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(get_simple_table_schema_(primary_table_id, timeout, schema_guard, primary_table_schema))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("get table schema fail", KR(ret),
              "index_table_id", table_schema->get_table_id(),
              "index_table_name", table_schema->get_table_name(),
              K(primary_table_id), "primary_table_name", primary_table_schema->get_table_name());
        }
      } else if (OB_ISNULL(primary_table_schema)) {
        LOG_ERROR("invalid schema", K(primary_table_schema));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, table_schema,
          tenant_name, db_name, timeout, primary_table_schema))) {
        LOG_ERROR("add table fail", KR(ret), "index_table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(), K(start_serve_tstamp),
            K(is_create_partition), K(tenant_name), K(db_name));
      }
    } else if (OB_FAIL(try_get_offline_ddl_origin_table_schema_(*table_schema, schema_guard, timeout,
              primary_table_schema))) {
      LOG_ERROR("try_get_offline_ddl_origin_table_schema_ fail", KR(ret),
          KPC(table_schema), K(primary_table_schema));
    } else if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, table_schema,
        tenant_name, db_name, timeout, primary_table_schema))) {
      LOG_ERROR("add table fail", KR(ret), "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(), K(start_serve_tstamp),
          K(is_create_partition), K(tenant_name), K(db_name));
    } else {
      // add table success
    }

    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // When a schema error is encountered (database schema, tenant schema not fetched)
      // simply ignore the table and do not add
      LOG_WARN("schema error when add table, ignore table", KR(ret),
          "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name());
      ret = OB_SUCCESS;
    }
  } // for

  ISTAT("[ADD_ALL_TABLES]", KR(ret), K_(tenant_id), "table_count", table_schemas.count(),
      K(start_serve_tstamp), K(start_schema_version));

  return ret;
}

int ObLogPartMgr::check_cur_schema_version_when_handle_future_table_(const int64_t schema_version,
    const int64_t end_time)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(schema_cond_);

  // Wait until Schema version upgrade
  // Parsing  row data, e.g. table_version=100, the current PartMgr is processing to version 90,
  // so you need to wait for the schema version to advance to a version greater than or equal to 100
  while (OB_SUCC(ret) && schema_version > ATOMIC_LOAD(&cur_schema_version_)) {
    int64_t left_time = end_time - get_timestamp();

    if (left_time <= 0) {
      ret = OB_TIMEOUT;
      break;
    }

    schema_cond_.wait_us(left_time);
  }

  return ret;
}

int ObLogPartMgr::update_schema_version(const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    CHECK_SCHEMA_VERSION(schema_version, "update-schema-version schema version reversed",
        K(schema_version), K_(cur_schema_version));

    if (OB_SUCC(ret)) {
      _ISTAT("[DDL] [UPDATE_SCHEMA] TENANT=%lu NEW_VERSION=%ld OLD_VERSION=%ld DELTA=%ld",
          tenant_id_, schema_version, cur_schema_version_, schema_version - cur_schema_version_);
      ObThreadCondGuard guard(schema_cond_);

      cur_schema_version_ = std::max(cur_schema_version_, schema_version);
      // Filtering data within PG: In filtering row data, multiple threads may encounter future table data,
      // at which point a uniform wake-up call is required via the broadcast mechanism
      schema_cond_.broadcast();
    }
  }

  return ret;
}

int ObLogPartMgr::is_exist_table_id_cache(const uint64_t table_id,
    bool &is_exist)
{
  const bool is_global_normal_index = false;
  is_exist = false;

  return is_exist_table_id_cache_(table_id, is_global_normal_index, is_exist);
}

int ObLogPartMgr::handle_future_table(const uint64_t table_id,
    const int64_t table_version,
    const int64_t timeout,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    int64_t start_time = get_timestamp();
    int64_t end_time = start_time + timeout;
    int64_t cur_schema_version = ATOMIC_LOAD(&cur_schema_version_);

    _ISTAT("[HANDLE_FUTURE_TABLE] [BEGIN] TENANT=%lu TABLE=%ld "
        "TABLE_VERSION=%ld CUR_SCHEMA_VERSION=%ld DELTA=%ld",
        tenant_id_, table_id, table_version, cur_schema_version, table_version - cur_schema_version);

    // 等待直到Schema版本升级
    if (OB_FAIL(check_cur_schema_version_when_handle_future_table_(table_version, end_time))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("check_cur_schema_version_when_handle_future_table_ fail", KR(ret), K(table_id), K(table_version));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(is_exist_table_id_cache(table_id, is_exist))) {
        LOG_ERROR("is_exist_table_id_cache fail", KR(ret), K(table_id), K(is_exist));
      }
    }

    cur_schema_version = ATOMIC_LOAD(&cur_schema_version_);
    _ISTAT("[HANDLE_FUTURE_TABLE] [END] RET=%d TENANT=%lu TABLE=%ld "
        "TABLE_VERSION=%ld IS_EXIST=%d CUR_SCHEMA_VERSION=%ld DELTA=%ld INTERVAL=%ld",
        ret, tenant_id_, table_id, table_version, is_exist, cur_schema_version, table_version - cur_schema_version, get_timestamp() - start_time);
  }

  return ret;
}

int ObLogPartMgr::apply_create_tablet_change(const ObCDCTabletChangeInfo &tablet_change_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! tablet_change_info.is_valid())
      || OB_UNLIKELY(! tablet_change_info.is_create_tablet_op())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tablet_change_info for create_tablet_op", KR(ret), K(tablet_change_info));
  } else {
    const ObArray<CreateTabletOp> &create_tablet_op_arr = tablet_change_info.get_create_tablet_op_arr();

    for (int64_t i = 0; OB_SUCC(ret) && i < create_tablet_op_arr.count(); i++) {
      const CreateTabletOp &create_tablet_op = create_tablet_op_arr.at(i);
      const common::ObTabletID &tablet_id = create_tablet_op.get_tablet_id();
      const ObCDCTableInfo &table_info = create_tablet_op.get_table_info();

      if (OB_UNLIKELY(! create_tablet_op.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("create_tablet_op is invalid", KR(ret), K(create_tablet_op));
      } else if (OB_FAIL(tablet_to_table_info_.insert_tablet_table_info(tablet_id, table_info))) {
        LOG_ERROR("insert_tablet_table_info failed", KR(ret), K(create_tablet_op),
            K(tablet_change_info), K_(tablet_to_table_info));
      }
    }
  }

  return ret;
}

int ObLogPartMgr::apply_delete_tablet_change(const ObCDCTabletChangeInfo &tablet_change_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! tablet_change_info.is_valid())
      || OB_UNLIKELY(! tablet_change_info.is_delete_tablet_op())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tablet_change_info for delete_tablet_op", KR(ret), K(tablet_change_info));
  } else {
    const ObArray<DeleteTabletOp> &delete_tablet_op_arr = tablet_change_info.get_delete_tablet_op_arr();

    for (int64_t i = 0; OB_SUCC(ret) && i < delete_tablet_op_arr.count(); i++) {
      const DeleteTabletOp &delete_tablet_op = delete_tablet_op_arr.at(i);
      const common::ObTabletID &tablet_id = delete_tablet_op.get_tablet_id();

      if (OB_UNLIKELY(! delete_tablet_op.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("delete_tablet_op is invalid", KR(ret), K(delete_tablet_op));
      } else if (OB_FAIL(tablet_to_table_info_.remove_tablet_table_info(tablet_id))) {
        LOG_ERROR("remove_table_id_from_cache_ failed", KR(ret), K(delete_tablet_op),
            K(tablet_change_info), K_(tablet_to_table_info));
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   caller should ignore error code if schema error like tenant/database not exist
// @retval other error code             fail
int ObLogPartMgr::get_schema_info_based_on_table_schema_(const ObSimpleTableSchemaV2 *tb_schema,
    ObLogSchemaGuard &schema_guard,
    const int64_t timeout,
    bool &table_is_ignored,
    const char *&tenant_name,
    const char *&db_name)
{
  int ret = OB_SUCCESS;
  table_is_ignored = false;
  tenant_name = NULL;
  db_name = NULL;

  if (OB_ISNULL(tb_schema)) {
    LOG_ERROR("invalid table schema", K(tb_schema));
    ret = OB_INVALID_ARGUMENT;
  }
  // 1. 由于Schema实现缺陷，如果一个租户被删除，取历史schema时，被删除租户的
  //    "oceanbase" DB schema将构建不出来。但oceanbase DB下面的某些系统表，
  //    比如__all_dummy等，会被构建出来，即出现table schema存在，但DB schema
  //    不存在的情况。由于libobcdc不需要同步系统表，因此此处通过过滤系统表的
  //    方法来规避DB schema不存在情况。
  //
  // 2. 保证全局索引表不被过滤掉
  //
  // 3. 保证唯一索引表不被过滤掉, 用于维护TableIDCache
  //    注意：is_unique_index接口和is_global_index_table存在交集：全局唯一索引
  //
  // 4. DDL表默认被过滤
  //
  // 5. backupm模式下指定表不被过滤
  //
  // 6. 临时表非用户表/系统表/唯一索引/全局索引，故该函数也会过滤临时表
  else if (! tb_schema->is_user_table()
      && ! BackupTableHelper::is_sys_table_exist_on_backup_mode(
            tb_schema->is_sys_table(), tb_schema->get_table_id())
      && ! tb_schema->is_global_index_table()
      && ! tb_schema->is_unique_index()) {
    LOG_INFO("ignore tmp table or non-user, sys table but not on backup mode, "
        "non-global-index and non-unique-index table",
        "table_name", tb_schema->get_table_name(),
        "table_id", tb_schema->get_table_id(),
        "is_tmp_table", tb_schema->is_tmp_table(),
        "is_user_table", tb_schema->is_user_table(),
        "is_sys_table", tb_schema->is_sys_table(),
        "is_backup_mode", is_backup_mode(),
        "is_index_table", tb_schema->is_index_table(),
        "is_unique_index_table", tb_schema->is_unique_index(),
        "id_ddl_table", is_ddl_table(tb_schema->get_table_id()),
        "is_global_index_table", tb_schema->is_global_index_table());
    // filter out
    table_is_ignored = true;
  } else {
    uint64_t tenant_id = tb_schema->get_tenant_id();
    uint64_t db_id = tb_schema->get_database_id();
    DBSchemaInfo db_schema_info;
    TenantSchemaInfo tenant_schema_info;

    table_is_ignored = false;

    if (OB_FAIL(schema_guard.get_tenant_schema_info(tenant_id, tenant_schema_info, timeout))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("get tenant schema info fail", KR(ret), K(tenant_id));
      }
    } else if (OB_FAIL(schema_guard.get_database_schema_info(tenant_id, db_id, db_schema_info, timeout))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("get database schema info fail", KR(ret), K(tenant_id), K(db_id));
      }
    } else {
      tenant_name = tenant_schema_info.name_;
      db_name = db_schema_info.name_;
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   caller should ignore error code if schema error like tenant/database not exist
// @retval other error code             fail
int ObLogPartMgr::get_schema_guard_and_table_schema_(const uint64_t table_id,
    const int64_t schema_version,
    const int64_t timeout,
    ObLogSchemaGuard &schema_guard,
    const ObSimpleTableSchemaV2 *&tb_schema)
{
  int ret = OB_SUCCESS;

  tb_schema = NULL;
  // TODO set tenant_id
  const uint64_t tenant_id = tenant_id_;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid schema getter", K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schema_getter->get_schema_guard_and_table_schema(
      tenant_id,
      table_id,
      schema_version,
      timeout,
      schema_guard,
      tb_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema fail", KR(ret), K(tenant_id), K(schema_version));
    }
  } else if (OB_ISNULL(tb_schema)) {
    LOG_ERROR("table schema is NULL, tenant may be dropped", K(table_id), K(schema_version));
    // TODO review this !!!!
    ret = OB_TENANT_HAS_BEEN_DROPPED;
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   caller should ignore if tenant/database not exist
// @retval other error code             fail
int ObLogPartMgr::get_lazy_schema_guard_and_tablegroup_schema_(
    const uint64_t tablegroup_id,
    const int64_t schema_version,
    const int64_t timeout,
    ObLogSchemaGuard &schema_guard,
    const ObTablegroupSchema *&tg_schema)
{
  return OB_NOT_SUPPORTED;
}

// fetch Simple Table Schema
// use Full Table Schema instead，cause could only get Full Table Schema under lazy mode
int ObLogPartMgr::get_simple_table_schema_(
    const uint64_t table_id,
    const int64_t timeout,
    ObLogSchemaGuard &schema_guard,
    const ObSimpleTableSchemaV2 *&tb_schema)
{
  int ret = OB_SUCCESS;
  // TODO set tenant_id
  const uint64_t tenant_id = tenant_id_;
  int64_t schema_version = OB_INVALID_TIMESTAMP;
  tb_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, tb_schema, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schema fail", KR(ret), K(table_id));
    }
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_ERROR("schema_gurard get_schema_version fail", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_ISNULL(tb_schema)) {
    ret = OB_TENANT_HAS_BEEN_DROPPED;
    LOG_WARN("schema error: table does not exist in target schema", K(table_id),
        "schema_version", schema_version);
  }

  return ret;
}

// 获取Full Table Schema
//
// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   caller should ignore if tenant not exist
// @retval other error code             fail
int ObLogPartMgr::get_full_table_schema_(const uint64_t table_id,
    const int64_t timeout,
    ObLogSchemaGuard &schema_guard,
    const ObTableSchema *&tb_schema)
{
  int ret = OB_SUCCESS;
  // TODO set tenant_id
  const uint64_t tenant_id = tenant_id_;
  int64_t schema_version = OB_INVALID_TIMESTAMP;
  tb_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, tb_schema, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schema fail", KR(ret), K(tenant_id), K(table_id));
    }
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_ERROR("schema_gurard get_schema_version fail", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_ISNULL(tb_schema)) {
    ret = OB_TENANT_HAS_BEEN_DROPPED;
    LOG_WARN("schema error: table does not exist in target schema", K(table_id),
        "schema_version", schema_version);
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TIMEOUT                   timeout
// @retval OB_TENANT_HAS_BEEN_DROPPED   caller should ignore if tenant not exist
// @retval other error code             fail
int ObLogPartMgr::get_schema_guard_and_schemas_(const uint64_t table_id,
    const int64_t schema_version,
    const int64_t timeout,
    bool &table_is_ignored,
    ObLogSchemaGuard &schema_guard,
    const ObSimpleTableSchemaV2 *&tb_schema,
    const char *&tenant_name,
    const char *&db_name)
{
  int ret = OB_SUCCESS;

  table_is_ignored = false;
  tb_schema = NULL;
  tenant_name = NULL;
  db_name = NULL;

  if (OB_FAIL(get_schema_guard_and_table_schema_(table_id, schema_version, timeout,
      schema_guard, tb_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schema fail", KR(ret), K(table_id), K(schema_version), KPC(tb_schema));
    }
  } else if (OB_FAIL(get_schema_info_based_on_table_schema_(tb_schema, schema_guard,
      timeout, table_is_ignored, tenant_name, db_name))) {
    if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
      LOG_ERROR("get_schema_info_based_on_table_schema_ fail", KR(ret), K(table_id),
          K(schema_version));
    }
  } else {
    // success
  }

  if (OB_SUCCESS != ret) {
    tenant_name = NULL;
    db_name = NULL;
  }
  return ret;
}

int ObLogPartMgr::add_table_(const int64_t start_serve_tstamp,
    const bool is_create_partition,
    const ObSimpleTableSchemaV2 *tb_schema,
    const char *tenant_name,
    const char *db_name,
    const int64_t timeout,
    const ObSimpleTableSchemaV2 *primary_table_schema)
{
  return OB_NOT_SUPPORTED;
}

int ObLogPartMgr::drop_table_(const ObSimpleTableSchemaV2 *table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(table_schema)) {
    LOG_ERROR("invalid argument", K(table_schema));
    ret = OB_INVALID_ARGUMENT;
  } else if (table_schema->is_global_normal_index_table()
      || is_unique_index_table_but_expect_global_unqiue_index_(*table_schema)) {
    if (OB_FAIL(remove_table_id_from_cache_(*table_schema))) {
      LOG_ERROR("remove_table_id_from_cache_ fail", KR(ret),
          "table_id", table_schema->get_table_id(),
          "table_name", table_schema->get_table_name(),
          "is_global_normal_index_table", table_schema->is_global_normal_index_table(),
          "is_unique_index", table_schema->is_unique_index());
    }
  } else {
    ObTableType table_type = table_schema->get_table_type();
    const uint64_t table_id = table_schema->get_table_id();
    const char *table_name = table_schema->get_table_name();
    int64_t served_part_count = 0;
    const bool is_tablegroup = false;

    // Delete only user tables and globally unique index tables
    if ((share::schema::USER_TABLE == table_type)
        || (table_schema->is_global_unique_index_table())) {
      if (OB_FAIL(remove_table_id_from_cache_(*table_schema))) {
        LOG_ERROR("remove_table_id_from_cache_ fail", KR(ret),
            "table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(),
            "is_global_unique_index_table", table_schema->is_global_unique_index_table());
      } else {
        int64_t total_served_part_count = 0;
        _ISTAT("[DDL] [DROP_TABLE] [END] TENANT=%lu TABLE=%s(%ld) "
            "IS_GLOBAL_UNIQUE_INDEX=%d SERVED_PART_COUNT=%ld TOTAL_PART_COUNT=%ld",
            tenant_id_,
            table_schema->get_table_name(),
            table_schema->get_table_id(),
            table_schema->is_global_unique_index_table(),
            served_part_count,
            total_served_part_count);
      }
    }
  }

  return ret;
}

int ObLogPartMgr::filter_table_(const ObSimpleTableSchemaV2 *table_schema,
    const char *tenant_name,
    const char *db_name,
    const lib::Worker::CompatMode &compat_mode,
    bool &chosen,
    bool &is_primary_table_chosen,
    const ObSimpleTableSchemaV2 *primary_table_schema)
{
  int ret = OB_SUCCESS;
  is_primary_table_chosen = false;
  // 1. filter_table_ only matches based on the primary table
  // 2. The current table is a global index table and needs to use the corresponding primary table
  const ObSimpleTableSchemaV2 *target_table_schema = NULL;
  IObLogTableMatcher *tb_matcher = TCTX.tb_matcher_;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(db_name) || OB_ISNULL(tenant_name) || OB_ISNULL(tb_matcher)) {
    LOG_ERROR("invalid argument", K(table_schema), K(db_name), K(tenant_name), K(tb_matcher));
    ret = OB_INVALID_ARGUMENT;
  } else if (table_schema->is_global_index_table() || table_schema->is_unique_index()) {
    // 1. When manipulating a global index table/unique index table, the primary schema should be valid
    // 2. The global index table/unique index table is filtered based on the primary table, if the primary table matches, the global index table also matches
    if (OB_ISNULL(primary_table_schema)) {
      LOG_ERROR("invalid argument", K(primary_table_schema), KPC(table_schema),
          K(db_name), K(tenant_name));
      ret = OB_INVALID_ARGUMENT;
    } else {
      target_table_schema = primary_table_schema;
    }
  } else if (table_schema->is_user_hidden_table()) {
    if (OB_ISNULL(primary_table_schema)) {
      LOG_ERROR("invalid argument", K(primary_table_schema), KPC(table_schema),
          K(db_name), K(tenant_name));
      ret = OB_INVALID_ARGUMENT;
    } else {
      target_table_schema = primary_table_schema;
    }
  } else {
    target_table_schema = table_schema;
  }

  if (OB_SUCC(ret)) {
    // match primary table
    ObTableType table_type = target_table_schema->get_table_type();
    const char *tb_name = target_table_schema->get_table_name();
    uint64_t table_id = target_table_schema->get_table_id();
    // Default mysql and oracle mode are both case-insensitive
    // when configured with enable_oracle_mode_match_case_sensitive=1, oracle is case sensitive
    int fnmatch_flags = FNM_CASEFOLD;
    if (compat_mode == lib::Worker::CompatMode::ORACLE
        && enable_oracle_mode_match_case_sensitive_) {
      fnmatch_flags = FNM_NOESCAPE;
    }

    chosen = false;

    if (OB_UNLIKELY(is_ddl_table(table_id))) {
      // No need to process DDL tables, DDL table partitions are added independently
      chosen = false;
      LOG_INFO("filter_table: DDL table is filtered", K_(tenant_id), K(table_id), K(tb_name),
          K(db_name), K(tenant_name));
    } else if (BackupTableHelper::is_sys_table_exist_on_backup_mode(
          target_table_schema->is_sys_table(),
          table_id)) {
      // Internal tables that need to be included in the backup schema must not be filtered
      chosen = true;
      LOG_INFO("do not filter inner tables on backup mode", K_(tenant_id), K(table_id), K(tb_name),
          K(db_name), K(tenant_name));
    } else if (OB_UNLIKELY(share::schema::USER_TABLE != table_type)) {
       // Synchronise only user tables
       chosen = false;
    }
    // Asynchronous PROXY table
    else if (OB_UNLIKELY(is_proxy_table(tenant_name, db_name, tb_name))) {
      chosen = false;
    } else if (OB_FAIL(tb_matcher->table_match(tenant_name, db_name, tb_name, chosen, fnmatch_flags))) {
      LOG_ERROR("match table fail", KR(ret), "table_name", target_table_schema->get_table_name(),
          K(db_name), K(tenant_name));
    } else {
      // succ
    }
  }

  if (OB_SUCC(ret)) {
    if (table_schema->is_global_index_table()) {
      // Primary Table Matching
      if (chosen) {
        is_primary_table_chosen = true;
      }

      if (table_schema->is_global_normal_index_table()) {
        // Global general indexes do not care about partition changes
        chosen = false;
      }

      LOG_DEBUG("filter_global_index_table_ succ", "index_table_id", table_schema->get_table_id(),
          "index_table", table_schema->get_table_name(),
          "table_name", primary_table_schema->get_table_name(),
          K(chosen), K(is_primary_table_chosen));
    } else if (is_unique_index_table_but_expect_global_unqiue_index_(*table_schema)) {
      // Primary Table Matching
      if (chosen) {
        is_primary_table_chosen = true;
      }
      // Unique index tables (not global unique index tables) do not care about partition changes and are only used to add TableIDCache
      chosen = false;
    } else {
      // succ
    }
  }

  if (OB_SUCC(ret)) {
    // If you are going to add a globally unique index table, check if it is a multi-instance scenario, which does not support globally unique indexes
    if (chosen
        && table_schema->is_global_unique_index_table()
        && TCONF.instance_num > SINGLE_INSTANCE_NUMBER
        && ! TCONF.enable_global_unique_index_belong_to_multi_instance) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("global unique index table under multi-instance NOT SUPPORTED",
          "instance_num", (int64_t)(TCONF.instance_num),
          "table_name", table_schema->get_table_name(),
          "table_id", table_schema->get_table_id(),
          K(primary_table_schema),
          "primary_table_name", primary_table_schema ? primary_table_schema->get_table_name() : "NULL",
          "primary_table_id", primary_table_schema ? primary_table_schema->get_table_id() : 0,
          K(is_primary_table_chosen), K(chosen));
    }
  }

  return ret;
}

bool ObLogPartMgr::is_unique_index_table_but_expect_global_unqiue_index_(const ObSimpleTableSchemaV2 &table_schema) const
{
  bool bool_ret = false;

  bool_ret = (table_schema.is_unique_index()) && (! table_schema.is_global_unique_index_table());

  return bool_ret;
}

bool ObLogPartMgr::is_proxy_table(const char *tenant_name, const char *db_name, const char *tb_name)
{
  bool bool_ret = false;

  // TODO: configure proxy tenant and database
  if (OB_ISNULL(tenant_name) || OB_ISNULL(db_name) || OB_ISNULL(tb_name)) {
    bool_ret = false;
  } else if (0 != STRCMP(OB_SYS_TENANT_NAME, tenant_name)) {
    bool_ret = false;
  } else if (0 != STRCMP(OB_SYS_DATABASE_NAME, db_name)) {
    bool_ret = false;
  } else {
    bool_ret = (0 == STRCMP(PROXY_INFO_TABLE_NAME, tb_name));
    bool_ret = bool_ret || (0 == STRCMP(PROXY_CONFIG_TABLE_NAME, tb_name));
    bool_ret = bool_ret || (0 == STRCMP(PROXY_CONFIG_TABLE_OLD_NAME, tb_name));
    bool_ret = bool_ret || (0 == STRCMP(PROXY_STAT_TABLE_NAME, tb_name));
    bool_ret = bool_ret || (0 == STRCMP(PROXY_KV_TABLE_NAME, tb_name));
    bool_ret = bool_ret || (0 == STRCMP(PROXY_VIP_TENANT_TABLE_NAME, tb_name));
    bool_ret = bool_ret || (0 == STRCMP(PROXY_VIP_TENANT_TABLE_OLD_NAME, tb_name));
  }

  return bool_ret;
}

int ObLogPartMgr::clean_table_id_cache_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    TableInfoEraserByTenant gidx_tb_eraser_by_tenant(tenant_id_, true/*is_global_normal_index*/);
    TableInfoEraserByTenant tableid_tb_eraser_by_tenant(tenant_id_, false/*is_global_normal_index*/);

    if (OB_FAIL(global_normal_index_table_cache_->remove_if(gidx_tb_eraser_by_tenant))) {
      LOG_ERROR("global_normal_index_table_cache_ remove_if fail", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(table_id_cache_->remove_if(tableid_tb_eraser_by_tenant))) {
      LOG_ERROR("table_id_cache_ remove_if fail", KR(ret), K(tenant_id_));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObLogPartMgr::add_table_id_into_cache_(const ObSimpleTableSchemaV2 &tb_schema,
    const char *db_name,
    const uint64_t primary_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = tb_schema.get_table_id();
  const uint64_t db_id = tb_schema.get_database_id();
  TableID table_id_key(table_id);
  TableInfo tb_info;
  const bool is_global_normal_index = tb_schema.is_global_normal_index_table();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == primary_table_id)) {
    LOG_ERROR("invalid argument", K(table_id), K(primary_table_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tb_info.init(primary_table_id))) {
    LOG_ERROR("tb_info init fail", KR(ret), K(table_id), K(primary_table_id));
  } else {
    if (is_global_normal_index) {
      if (OB_FAIL(global_normal_index_table_cache_->insert(table_id_key, tb_info))) {
        if (OB_ENTRY_EXIST == ret) {
          // cache ready exist
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("global_normal_index_table_cache_ insert fail", KR(ret),
              K(table_id_key), K(tb_info),
              K(table_id), "index_table_name", tb_schema.get_table_name(),
              "is_global_normal_index_table", tb_schema.is_global_normal_index_table(),
              K(db_id));
        }
      } else {
        LOG_INFO("[GLOBAL_NORMAL_INDEX_TBALE] [ADD]", K(table_id_key), K(tb_info),
            K(table_id), "index_table_name", tb_schema.get_table_name(),
            "is_global_normal_index_table", tb_schema.is_global_normal_index_table(),
            K(db_id), K(db_name));
      }
    } else {
      if (OB_FAIL(table_id_cache_->insert(table_id_key, tb_info))) {
        if (OB_ENTRY_EXIST == ret) {
          // cache ready exist
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("table_id_cache_ insert fail", KR(ret),
              K(table_id_key), K(tb_info),
              K(table_id), "table_name", tb_schema.get_table_name(),
              "is_unique_index", tb_schema.is_unique_index(),
              "is_global_unique_index_table", tb_schema.is_global_unique_index_table(),
              K(db_id), K(db_name));
        }
      } else {
        LOG_INFO("[SERVED_TABLE_ID_CACHE] [ADD]", K(table_id_key), K(tb_info),
            K(table_id), "table_name", tb_schema.get_table_name(),
            "is_unique_index", tb_schema.is_unique_index(),
            "is_global_unique_index_table", tb_schema.is_global_unique_index_table(),
            K(db_id), K(db_name));
      }
    }
  }

  return ret;
}

int ObLogPartMgr::remove_table_id_from_cache_(const ObSimpleTableSchemaV2 &tb_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = tb_schema.get_table_id();
  TableID table_id_key(table_id);
  const bool is_global_normal_index = tb_schema.is_global_normal_index_table();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    LOG_ERROR("invalid argument", K(table_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (is_global_normal_index) {
      // Global common index, operate global common index cache
      if (OB_FAIL(global_normal_index_table_cache_->erase(table_id_key))) {
        // Partition may not exist, normal
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("global_normal_index_table_cache_ erase fail", KR(ret), K(table_id_key));
        }
      } else {
        LOG_INFO("[GLOBAL_NORMAL_INDEX_TBALE] [REMOVE]", K(table_id_key),
            K(table_id), "index_table_name", tb_schema.get_table_name(),
            "is_global_normal_index_table", tb_schema.is_global_normal_index_table());
      }
    } else {
      if (OB_FAIL(table_id_cache_->erase(table_id_key))) {
        // Partition may not exist, normal
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("table_id_cache erase fail", KR(ret), K(table_id_key));
        }
      } else {
        LOG_INFO("[SERVED_TABLE_ID_CACHE] [REMOVE]", K(table_id_key),
            K(table_id), "table_name", tb_schema.get_table_name(),
            "is_unique_index", tb_schema.is_unique_index(),
            "is_global_unique_index_table", tb_schema.is_global_unique_index_table());
      }
    }
  }

  return ret;
}

int ObLogPartMgr::is_exist_table_id_cache_(const uint64_t table_id,
    const bool is_global_normal_index,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  TableID table_id_key(table_id);
  TableInfo info;
  is_exist = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    LOG_ERROR("invalid argument", K(table_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_exist = true;
    /* TODO imply for blacklist, remove Temporary
    if (is_global_normal_index) {
      if (OB_FAIL(global_normal_index_table_cache_->get(table_id_key, info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_exist = false;
        } else {
          LOG_ERROR("global_normal_index_table_cache_ get fail", KR(ret), K(table_id_key));
        }
      } else {
        is_exist = true;
      }
    } else {
      if (OB_FAIL(table_id_cache_->get(table_id_key, info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_exist = false;
        } else {
          LOG_ERROR("table_id_cache_ get fail", KR(ret), K(table_id_key));
        }
      } else {
        is_exist = true;
      }

      LOG_DEBUG("[SERVED_TABLE_ID_CACHE] [IS_EXIST]", K(tenant_id_), K(table_id), K(is_exist));
    }
    */
  }

  return ret;
}

}
}

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
