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
        LOG_ERROR(fmt, K(tenant_id_), K(cur_schema_version_), K(check_schema_version), ##arg); \
        if (!TCONF.skip_reversed_schema_verison) { \
          ret = OB_INVALID_ARGUMENT; \
        } \
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

namespace liboblog
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
    PartInfoMap &map,
    GIndexCache &gi_cache,
    TableIDCache &table_id_cache,
    PartCBArray &part_add_cb_array,
    PartCBArray &part_rc_cb_array)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(schema_cond_.init(common::ObWaitEventIds::OBLOG_PART_MGR_SCHEMA_VERSION_WAIT))) {
    LOG_ERROR("schema_cond_ init fail", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    map_ = &map;
    global_normal_index_table_cache_ = &gi_cache;
    table_id_cache_ = &table_id_cache;
    part_add_cb_array_ = &part_add_cb_array;
    part_rc_cb_array_ = &part_rc_cb_array;
    cur_schema_version_ = start_schema_version;
    enable_oracle_mode_match_case_sensitive_ = enable_oracle_mode_match_case_sensitive;

    inited_ = true;
    LOG_INFO("init PartMgr succ", K(tenant_id), K(start_schema_version));
  }

  return ret;
}

void ObLogPartMgr::reset()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  map_ = NULL;
  global_normal_index_table_cache_ = NULL;
  table_id_cache_ = NULL;
  part_add_cb_array_ = NULL;
  part_rc_cb_array_ = NULL;
  cur_schema_version_ = OB_INVALID_VERSION;
  enable_oracle_mode_match_case_sensitive_ = false;
  schema_cond_.destroy();
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
      || OB_UNLIKELY(start_serve_tstamp <= 0)
      || OB_UNLIKELY(extract_tenant_id(table_id) != tenant_id_)) {
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
        usleep((useconds_t)block_time_us);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, table_schema, tenant_name,
              db_name, timeout))) {
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

bool ObLogPartMgr::has_physical_part_(const ObSimpleTableSchemaV2 &table_schema)
{
  // Normal tables have physical partitions when not binding to a tablegroup
  return (! table_schema.get_binding());
}

bool ObLogPartMgr::has_physical_part_(const ObTablegroupSchema &tg_schema)
{
  // tablegroup has physical partitions when the binding attribute is in effect
  return (tg_schema.get_binding());
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
  share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(schema_version_before_alter <= 0)
      || OB_UNLIKELY(schema_version_after_alter <= 0)
      || OB_UNLIKELY(extract_tenant_id(table_id) != tenant_id_)) {
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
      const bool is_tablegroup = false;
      const bool has_physical_part = has_physical_part_(*new_table_schema);
      bool is_primary_table_chosen = false;

      if (TCONF.test_mode_on) {
        int64_t block_time_us = TCONF.test_mode_block_alter_table_ddl_sec * _SEC_;
        if (block_time_us > 0) {
          ISTAT("[ALTER_TABLE] [TEST_MODE_ON] block to alter table",
              K_(tenant_id), K(table_id), K(block_time_us));
          usleep((useconds_t)block_time_us);
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
      }
      // Add or delete partitions if the table is selected
      else if (OB_FAIL(alter_table_add_or_drop_partition_(
          is_tablegroup,
          has_physical_part,
          start_serve_timestamp,
          old_table_schema,
          new_table_schema,
          new_table_schema->get_database_id(),
          event))) {
        LOG_ERROR("alter table add or drop partition fail", KR(ret), K(is_tablegroup),
            K(has_physical_part), K(start_serve_timestamp));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogPartMgr::split_table(const uint64_t table_id,
    const int64_t new_schema_version,
    const int64_t start_serve_timestamp,
    ObLogSchemaGuard &new_schema_guard,
    const char *&tenant_name,
    const char *&db_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool table_is_ignored = false;
  const ObSimpleTableSchemaV2 *new_table_schema = NULL;
  // get tenant mode: MYSQL or ORACLE
  // 1. oracle database/table matc needs to be case sensitive
  // 2. mysql match don't needs to be case sensitive
  share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_UNLIKELY(new_schema_version <= 0)
      || OB_UNLIKELY(start_serve_timestamp <= 0)
      || OB_UNLIKELY(extract_tenant_id(table_id) != tenant_id_)) {
    LOG_ERROR("invalid argument", K(table_id), K(new_schema_version), K(start_serve_timestamp),
        K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_schema_guard_and_schemas_(table_id, new_schema_version, timeout,
      table_is_ignored, new_schema_guard, new_table_schema, tenant_name, db_name))) {
    if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
      LOG_ERROR("get schemas fail", KR(ret), K(table_id), K(new_schema_version));
    }
  } else if (table_is_ignored) {
    // table is ignored
  } else if (OB_ISNULL(new_table_schema) || OB_ISNULL(tenant_name) || OB_ISNULL(db_name)) {
    LOG_ERROR("invalid schema", K(new_table_schema), K(tenant_name), K(db_name));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id_, compat_mode, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id_),
          "compat_mode", print_compat_mode(compat_mode), KPC(new_table_schema));
    }
  } else {
    // Require sequential DDL, encounter Schema version reversal,
    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(new_schema_version, "table-split DDL schema version reversed",
        "table_id", new_table_schema->get_table_id(),
        "table_name", new_table_schema->get_table_name());

    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_table_(new_table_schema, tenant_name, db_name, start_serve_timestamp, compat_mode))) {
        LOG_ERROR("split_table_ fail", KR(ret), K(table_id), K(start_serve_timestamp),
            "compat_mode", print_compat_mode(compat_mode), K(tenant_name), K(db_name));
      } else {
        // success
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
      || OB_UNLIKELY(schema_version_before_drop > schema_version_after_drop)
      || OB_UNLIKELY(extract_tenant_id(table_id) != tenant_id_)) {
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
      || OB_UNLIKELY(start_serve_tstamp <= 0)
      || OB_UNLIKELY(extract_tenant_id(table_id) != tenant_id_)) {
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
      || OB_UNLIKELY(schema_version_before_drop > schema_version_after_drop)
      || OB_UNLIKELY(extract_tenant_id(table_id) != tenant_id_)) {
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

int ObLogPartMgr::add_tablegroup_partition(
    const uint64_t tablegroup_id,
    const int64_t schema_version,
    const int64_t start_serve_timestamp,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool is_create_partition = true;  // add new create partition
  const ObTablegroupSchema *tg_schema = NULL;
  TenantSchemaInfo tenant_schema_info;
  const uint64_t tenant_id = extract_tenant_id(tablegroup_id);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)
      || OB_UNLIKELY(schema_version <= 0)
      || OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_serve_timestamp)
      || OB_UNLIKELY(extract_tenant_id(tablegroup_id) != tenant_id_)) {
    LOG_ERROR("invalid argument", K(tablegroup_id), K(schema_version), K(start_serve_timestamp),
        K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_lazy_schema_guard_and_tablegroup_schema_(tablegroup_id, schema_version,
          timeout, schema_guard, tg_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard_and_tablegroup_schema_ fail", KR(ret), K(tablegroup_id),
          K(schema_version));
    }
  } else if (OB_FAIL(schema_guard.get_tenant_schema_info(tenant_id, tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_schema_info fail", KR(ret), K(tenant_id), K(tablegroup_id));
    }
  } else {
    // set tenant name
    tenant_name = tenant_schema_info.name_;

    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(schema_version, "add-tablegroup schema version reversed", K(tablegroup_id));

    if (OB_FAIL(ret)) {
      // fail
    } else if (OB_FAIL(add_tablegroup_partition_(
        tablegroup_id,
        *tg_schema,
        start_serve_timestamp,
        is_create_partition,
        tenant_name,
        timeout))) {
      LOG_ERROR("add_tablegroup_partition_ fail", KR(ret),
          K(tablegroup_id),
          K(tg_schema),
          K(schema_version),
          K(start_serve_timestamp),
          K(tenant_name),
          K(is_create_partition));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogPartMgr::add_tablegroup_partition_(
    const uint64_t tablegroup_id,
    const ObTablegroupSchema &tg_schema,
    const int64_t start_serve_timestamp,
    const bool is_create_partition,
    const char *tenant_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    int64_t served_part_count = 0;
    const uint64_t db_id = OB_INVALID_ID;
    // tablegroup bind indicates is PG, with entity partition
    const bool has_physical_part = has_physical_part_(tg_schema);
    bool check_dropped_schema = false;
    ObTablegroupPartitionKeyIter pkey_iter(tg_schema, check_dropped_schema);
    const char *tablegroup_name = tg_schema.get_tablegroup_name_str();
    const bool is_tablegroup = true;
  // get tenant mode: MYSQL or ORACLE
  // 1. oracle database/table matc needs to be case sensitive
  // 2. mysql match don't needs to be case sensitive
    share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;
    bool tablegroup_is_chosen = false;

    // TABLEGROUP whitelist filtering based on tablegroup
    if (OB_FAIL(get_tenant_compat_mode(tenant_id_, compat_mode, timeout))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id_));
      }
    } else if (OB_FAIL(filter_tablegroup_(&tg_schema, tenant_name, compat_mode, tablegroup_is_chosen))) {
      LOG_ERROR("filter_tablegroup_ fail", KR(ret), K(tablegroup_id),
          "tablegroup_name", tg_schema.get_tablegroup_name_str(),
          K(tenant_id_), K(tenant_name), K(tablegroup_is_chosen));
    } else if (! tablegroup_is_chosen) {
      // tablegroup is filtered and no longer processed
      LOG_INFO("tablegroup is not served, tablegroup add DDL is filtered", K(tablegroup_id),
          "tablegroup_name", tg_schema.get_tablegroup_name_str(),
          K(tenant_id_), K(tenant_name), K(tablegroup_is_chosen));
    } else if (OB_FAIL(add_table_or_tablegroup_(
        is_tablegroup,
        tablegroup_id,
        tablegroup_id,
        db_id,
        has_physical_part,
        is_create_partition,
        start_serve_timestamp,
        pkey_iter,
        tg_schema,
        served_part_count))) {
      LOG_ERROR("add_part_ fail", KR(ret),
          K(is_tablegroup),
          K(tablegroup_id),
          K(db_id),
          K(has_physical_part),
          K(is_create_partition),
          K(start_serve_timestamp),
          K(tg_schema));
    } else {
      _ISTAT("[DDL] [ADD_TABLEGROUP] TENANT=%lu TABLEGROUP=%s(%ld) HAS_PHY_PART=%d "
          "START_TSTAMP=%ld SERVED_PART_COUNT=%ld",
          tenant_id_, tablegroup_name, tablegroup_id,
          has_physical_part, start_serve_timestamp, served_part_count);
    }
  }

  return ret;
}

int ObLogPartMgr::drop_tablegroup_partition(
    const uint64_t tablegroup_id,
    const int64_t schema_version_before_drop,
    const int64_t schema_version_after_drop,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema *tg_schema = NULL;
  TenantSchemaInfo tenant_schema_info;
  const uint64_t tenant_id = extract_tenant_id(tablegroup_id);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)
      || OB_UNLIKELY(schema_version_after_drop <= 0)
      || OB_UNLIKELY(schema_version_before_drop > schema_version_after_drop)
      || OB_UNLIKELY(extract_tenant_id(tablegroup_id) != tenant_id_)) {
    LOG_ERROR("invalid argument", K(tablegroup_id), K(tenant_id_),
        K(schema_version_before_drop), K(schema_version_after_drop));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_lazy_schema_guard_and_tablegroup_schema_(tablegroup_id,
      schema_version_before_drop, timeout, schema_guard, tg_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard_and_tablegroup_schema_ fail", KR(ret), K(tablegroup_id),
          K(schema_version_before_drop));
    }
  } else if (OB_FAIL(schema_guard.get_tenant_schema_info(tenant_id, tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_schema_info fail", KR(ret), K(tenant_id), K(tablegroup_id));
    }
  } else {
    tenant_name = tenant_schema_info.name_;

    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(schema_version_after_drop, "drop-tablegroup schema version reversed", K(tablegroup_id));

    if (OB_FAIL(ret)) {
      // fail
    } else if (OB_FAIL(drop_tablegroup_partition_(tablegroup_id, *tg_schema))) {
      LOG_ERROR("drop_tablegroup_partition_ fail", KR(ret),
          K(tablegroup_id), "tablegroup_name", tg_schema->get_tablegroup_name_str());
    }
  }

  return ret;
}

// TODO
// Now when tablegroup is deleted, PG is deleted immediately, because now the table does not support PG migration, so this is no problem,
// in the future, after supporting PG migration, PG can not be deleted directly, to rely on OFFLINE log, because the data must be all processed, otherwise data will be lost
int ObLogPartMgr::drop_tablegroup_partition_(
    const uint64_t tablegroup_id,
    const ObTablegroupSchema &tg_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    const char *tablegroup_name = tg_schema.get_tablegroup_name_str();
    int64_t served_part_count = 0;
    const bool is_tablegroup = true;

    if (OB_FAIL(drop_table_or_tablegroup_(
        is_tablegroup,
        tablegroup_id,
        tablegroup_name,
        tg_schema,
        served_part_count))) {
      LOG_ERROR("drop_table_or_tablegroup_ fail", KR(ret),
          K(is_tablegroup),
          K(tablegroup_id),
          K(tablegroup_name));
    } else {
      _ISTAT("[DDL] [DROP_TABLEGROUP] [END] TENANT=%lu TABLEGROUP=%s(%ld) HAS_PHY_PART=%d "
          "SERVED_PART_COUNT=%ld TOTAL_PART_COUNT=%ld",
          tenant_id_, tg_schema.get_tablegroup_name_str(), tablegroup_id,
          tg_schema.get_binding(), served_part_count, map_->get_valid_count());
    }
  }

  return ret;
}

template <class PartitionSchema>
int ObLogPartMgr::drop_table_or_tablegroup_(
    const bool is_tablegroup,
    const uint64_t table_id,
    const char *table_name,
    PartitionSchema &table_schema,
    int64_t &served_part_count)
{
  int ret = OB_SUCCESS;
  served_part_count = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    const uint64_t tenant_id = extract_tenant_id(table_id);
    const bool is_binding = table_schema.get_binding();
    const char *drop_part_type_str = is_tablegroup ? "TABLEGROUP" : "TABLE";

    // Iterate through the partitions being served and delete the partitions in the corresponding table
    PartInfoScannerByTableID scanner_by_table(table_id);
    if (OB_FAIL(map_->for_each(scanner_by_table))) {
      LOG_ERROR("scan map by table id fail", KR(ret), K(table_id));
    } else {
      _ISTAT("[DDL] [DROP_%s] [BEGIN] TENANT=%lu %s=%s(%lu) IS_BINDING=%d PART_COUNT_IN_%s=%ld/%ld",
          drop_part_type_str, tenant_id, drop_part_type_str,
          table_name, table_id, is_binding, drop_part_type_str,
          scanner_by_table.pkey_array_.count(),
          map_->get_valid_count());

      for (int64_t idx = 0; OB_SUCC(ret) && idx < scanner_by_table.pkey_array_.count(); idx++) {
        const ObPartitionKey &pkey = scanner_by_table.pkey_array_.at(idx);

        ret = offline_partition_(pkey);

        if (OB_ENTRY_NOT_EXIST == ret) {
          DSTAT("[DDL] [DROP_TABLE] partition not served", K(pkey), K(is_tablegroup));
          ret = OB_SUCCESS;
        } else if (OB_SUCCESS != ret) {
          LOG_ERROR("offline partition fail", KR(ret), K(pkey), K(is_tablegroup));
        } else {
          served_part_count++;
        }
      } // for
    }
  }

  return ret;
}

int ObLogPartMgr::split_tablegroup_partition(
    const uint64_t tablegroup_id,
    const int64_t new_schema_version,
    const int64_t start_serve_timestamp,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema *tg_schema = NULL;
  // get tenant mode: MYSQL or ORACLE
  // 1. oracle database/table matc needs to be case sensitive
  // 2. mysql match don't needs to be case sensitive
  share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;
  bool tablegroup_is_chosen = false;
  TenantSchemaInfo tenant_schema_info;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)
      || OB_UNLIKELY(new_schema_version <= 0)
      || OB_UNLIKELY(start_serve_timestamp <= 0)
      || OB_UNLIKELY(extract_tenant_id(tablegroup_id) != tenant_id_)) {
    LOG_ERROR("invalid argument", K(tablegroup_id), K(new_schema_version), K(start_serve_timestamp),
        K(tenant_id_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_lazy_schema_guard_and_tablegroup_schema_(tablegroup_id, new_schema_version,
          timeout, schema_guard, tg_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard_and_tablegroup_schema_ fail", KR(ret), K(tablegroup_id),
          K(new_schema_version));
    }
  } else if (OB_FAIL(schema_guard.get_tenant_schema_info(tenant_id_, tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_schema_info fail", KR(ret), K(tenant_id_), K(tablegroup_id));
    }
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id_, compat_mode, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id_));
    }
  } else if (OB_FAIL(filter_tablegroup_(tg_schema, tenant_schema_info.name_, compat_mode, tablegroup_is_chosen))) {
    LOG_ERROR("filter_tablegroup_ fail", KR(ret), K(tablegroup_id),
        "tablegroup_name", tg_schema->get_tablegroup_name_str(),
        K(tenant_id_), K(tenant_schema_info),
        K(tablegroup_is_chosen));
  } else if (! tablegroup_is_chosen) {
    // tablegroup filtered
    LOG_INFO("tablegroup is not served, tablegroup split DDL is filtered", K(tablegroup_is_chosen),
        K(tablegroup_id),
        "tablegroup_name", tg_schema->get_tablegroup_name_str(),
        K(tenant_id_), K(tenant_schema_info));
  } else {
    tenant_name = tenant_schema_info.name_;

    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(new_schema_version, "split-tablegroup schema version reversed", K(tablegroup_id));

    if (OB_FAIL(ret)) {
      // fail
    } else if (OB_FAIL(split_tablegroup_partition_(tablegroup_id, *tg_schema,
        start_serve_timestamp))) {
      LOG_ERROR("split_tablegroup_partition_ fail", KR(ret), K(tablegroup_id),
          K(tg_schema), K(new_schema_version), K(start_serve_timestamp));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogPartMgr::split_tablegroup_partition_(
    const uint64_t tablegroup_id,
    const ObTablegroupSchema &tg_schema,
    const int64_t start_serve_timestamp)
{
  int ret = OB_SUCCESS;
  const uint64_t db_id = OB_INVALID_ID;
  const bool has_physical_part = has_physical_part_(tg_schema);
  bool check_dropped_schema = false;
  ObTablegroupPartitionKeyIter pkey_iter(tg_schema, check_dropped_schema);
  const bool is_tablegroup = true;

  if (OB_FAIL(split_table_or_tablegroup_(
      is_tablegroup,
      tablegroup_id,
      tablegroup_id,
      db_id,
      has_physical_part,
      start_serve_timestamp,
      pkey_iter,
      tg_schema))) {
    LOG_ERROR("split_table_or_tablegroup_ fail", KR(ret),
        K(tenant_id_),
        K(is_tablegroup),
        K(tablegroup_id),
        K(db_id),
        K(has_physical_part),
        K(start_serve_timestamp),
        K(tg_schema));
  } else {
    _ISTAT("[DDL] [SPLIT_TABLEGROUP] TENANT=%lu TABLEGROUP=%s(%ld) HAS_PHY_PART=%d START_TSTAMP=%ld",
        extract_tenant_id(tablegroup_id),
        tg_schema.get_tablegroup_name_str(), tablegroup_id,
        has_physical_part, start_serve_timestamp);
  }

  return ret;
}

template <class PartitionKeyIter, class PartitionSchema>
int ObLogPartMgr::split_table_or_tablegroup_(
    const bool is_tablegroup,
    const uint64_t table_id,
    const uint64_t tablegroup_id,
    const uint64_t db_id,
    const bool has_physical_part,
    const int64_t start_serve_timestamp,
    PartitionKeyIter &pkey_iter,
    PartitionSchema &table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (! need_to_support_split_when_in_multi_instance_()) {
    LOG_ERROR("split table under multi instance NOT SUPPORTED",
        "instance_index", (int64_t)(TCONF.instance_index),
        "instance_num", (int64_t)(TCONF.instance_num),
        "table_id", table_schema.get_table_id(),
        K(is_tablegroup), K(has_physical_part));
    ret = OB_NOT_SUPPORTED;
  } else if (! table_schema.is_in_logical_split()) {
    // When a user modifies a partition rule for a table that has no partition rule and keeps the partition count at 1, it does not enter the split state
    LOG_INFO("table is not in splitting, maybe its partition number is not modified",
        K_(tenant_id), K(is_tablegroup), K(table_id),
        "partition_status", table_schema.get_partition_status(),
        "schema_version", table_schema.get_schema_version(),
        K(has_physical_part));
  } else {
    ObPartitionKey dst_pkey;
    bool is_served = false;
    // The split new partition is a newly created partition that synchronises data from scratch
    const bool is_create_partition = true;

    // Iterate over each split partition, get its split source partition, and add the split partition if the split source partition is in service
    // Note that.
    // 1. This will only affect scenarios with multiple liboblog instances, if there is only one liboblog instance, all partitions are served
    // 2. This does not support multiple consecutive split scenarios, requiring all liboblog instances to be restarted and the data redistributed before the next split
    // 3. The purpose of this rule is to ensure that in a multiple instance scenario, data is not misplaced during the splitting process,
    // and that all instances must be restarted after the split is complete to redistribute the data between instances
    //
    // For example, if p0 and p1 are split into p3, p4, p5 and p6, and according to the instance hash rule, this instance only serves p0, not p1,
    // then p3 and p4 are served by p0, while p5 and p6 are not served by p1.
    while (OB_SUCC(ret) && OB_SUCC(pkey_iter.next_partition_key_v2(dst_pkey))) {
      // Get the partition before the split
      ObPartitionKey src_pkey;
      // get_split_source_partition_key guarantees that.
      // 1. when dst_pkey is a split partition, source_part_key returns the split source partition
      // 2. when dst_pkey is not a split partition, source_part_key returns its own
      if (OB_FAIL(table_schema.get_split_source_partition_key(dst_pkey, src_pkey))) {
        LOG_ERROR("get_split_source_partition_key fail", KR(ret), K(tenant_id_), K(dst_pkey),
            K(table_id), K(is_tablegroup));
      }
      // Check if the partitions are the same, if they are, then this partition is not split
      else if (src_pkey == dst_pkey) {
        LOG_INFO("partition does not split, need not add new partition", K(is_tablegroup),
            K(table_id), K(has_physical_part), K(dst_pkey));
      }
      // Determining whether a split partition will be serviced based on the partition before split
      else if (OB_FAIL(add_served_partition_(
          dst_pkey,
          src_pkey,
          start_serve_timestamp,
          is_create_partition,
          has_physical_part,
          tablegroup_id,
          db_id,
          is_served))) {
        LOG_ERROR("add_served_partition_ fail", KR(ret),
            K(dst_pkey),
            K(src_pkey),
            K(start_serve_timestamp),
            K(is_create_partition),
            K(has_physical_part),
            K(tablegroup_id),
            K(db_id));
      } else if (! is_served) {
        LOG_INFO("split source partition is not served, ignore split dst partition", K_(tenant_id),
            K(table_id), K(is_tablegroup), K(has_physical_part), K(src_pkey), K(dst_pkey),
            K(db_id), K(tablegroup_id));
      }

      dst_pkey.reset();
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObLogPartMgr::alter_tablegroup_partition(
    const uint64_t tablegroup_id,
    const int64_t schema_version_before_alter,
    const int64_t schema_version_after_alter,
    const int64_t start_serve_timestamp,
    ObLogSchemaGuard &old_schema_guard,
    ObLogSchemaGuard &new_schema_guard,
    const char *&tenant_name,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema *old_tg_schema = NULL;
  const ObTablegroupSchema *new_tg_schema = NULL;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
  // get tenant mode: MYSQL or ORACLE
  // 1. oracle database/table matc needs to be case sensitive
  // 2. mysql match don't needs to be case sensitive
  share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;
  TenantSchemaInfo tenant_schema_info;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)
      || OB_UNLIKELY(schema_version_before_alter <= 0)
      || OB_UNLIKELY(schema_version_after_alter <= 0)
      || OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_serve_timestamp)
      || OB_ISNULL(schema_getter)
      || OB_UNLIKELY(extract_tenant_id(tablegroup_id) != tenant_id_)) {
    LOG_ERROR("invalid argument", K(tenant_id_), K(tablegroup_id),
        K(schema_version_before_alter), K(schema_version_after_alter), K(start_serve_timestamp),
        K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id_, compat_mode, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id_),
          "compat_mode", print_compat_mode(compat_mode));
    }
  } else if (OB_FAIL(schema_getter->get_lazy_schema_guard(tenant_id_, schema_version_before_alter,
      timeout, old_schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema fail", KR(ret), K(tenant_id_), K(schema_version_before_alter));
    }
  } else if (OB_FAIL(schema_getter->get_lazy_schema_guard(tenant_id_, schema_version_after_alter,
      timeout, new_schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema fail", KR(ret), K(tenant_id_), K(schema_version_after_alter));
    }
  } else if (OB_FAIL(old_schema_guard.get_tablegroup_schema(tablegroup_id, old_tg_schema, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get tablegroup schema fail", KR(ret), K(tenant_id_), K(tablegroup_id));
    }
  } else if (OB_ISNULL(old_tg_schema)) {
    LOG_WARN("schema error: tablegroup does not exist in target schema", K(tenant_id_),
        K(tablegroup_id), "schema_version", schema_version_before_alter);
        // TODO Is it appropriate to replace the error code with OB_TENANT_HAS_BEEN_DROPPED?
    ret = OB_TENANT_HAS_BEEN_DROPPED;
  } else if (OB_FAIL(new_schema_guard.get_tablegroup_schema(tablegroup_id, new_tg_schema, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get tablegroup schema fail", KR(ret), K(tenant_id_), K(tablegroup_id));
    }
  } else if (OB_ISNULL(new_tg_schema)) {
    LOG_WARN("schema error: tablegroup does not exist in target schema", K(tenant_id_), K(tablegroup_id),
        "schema_version", schema_version_after_alter);
    ret = OB_TENANT_HAS_BEEN_DROPPED;
  } else if (OB_FAIL(old_schema_guard.get_tenant_schema_info(tenant_id_, tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_schema_info fail", KR(ret), K(tenant_id_), K(tablegroup_id));
    }
  } else {
    tenant_name = tenant_schema_info.name_;

    const bool has_physical_part = has_physical_part_(*new_tg_schema);
    const int64_t new_database_id = OB_INVALID_ID;    // tablegroup DB invalid
    const bool is_tablegroup = true;
    const char *tablegroup_name = new_tg_schema->get_tablegroup_name_str();

    _ISTAT("[DDL] [ALTER_TABLEGROUP] [BEGIN] TENANT=%s(%lu) TABLEGROUP=%s(%lu) HAS_PHY_PART=%d",
        tenant_name, tenant_id_, old_tg_schema->get_tablegroup_name_str(), tablegroup_id,
        has_physical_part);

    // ignore if skip_reversed_schema_version_=true, otherwise exit with an error
    CHECK_SCHEMA_VERSION(schema_version_after_alter, "alter-tablegroup schema version reversed",
        K(tablegroup_id));

    if (OB_FAIL(ret)) {
      // fail
    } else if (has_physical_part) {
      // PG Dynamic add/drop partition
      bool tablegroup_is_chosen = false;
      if (OB_FAIL(filter_tablegroup_(new_tg_schema, tenant_name, compat_mode, tablegroup_is_chosen))) {
        LOG_ERROR("filter_tablegroup_ fail", KR(ret), K(tablegroup_id), K(tablegroup_name),
            K(tenant_id_), K(tenant_name), K(tablegroup_is_chosen));
      } else if (! tablegroup_is_chosen) {
        // tablegroup is filtered and no longer processed
      } else if (OB_FAIL(alter_table_add_or_drop_partition_(
          is_tablegroup,
          has_physical_part,
          start_serve_timestamp,
          old_tg_schema,
          new_tg_schema,
          new_database_id,
          "alter_tablegroup_partition"))) {
        LOG_ERROR("alter table add or drop partition fail", KR(ret), K(is_tablegroup),
            K(start_serve_timestamp), K(new_database_id), K(has_physical_part));
      } else {
        // succ
      }

      _ISTAT("[DDL] [ALTER_TABLEGROUP] RET=%d TENANT=%s(%lu) TABLEGROUP=%s(%ld) IS_SERVED=%d HAS_PHY_PART=%d "
          "START_TSTAMP=%ld",
          ret, tenant_name, tenant_id_, tablegroup_name, tablegroup_id,
          tablegroup_is_chosen, has_physical_part, start_serve_timestamp);
    } else {
      if (OB_FAIL(alter_tablegroup_partition_when_is_not_binding_(
          tablegroup_id,
          schema_version_before_alter,
          old_schema_guard,
          schema_version_after_alter,
          new_schema_guard,
          start_serve_timestamp,
          compat_mode,
          timeout))) {
        LOG_ERROR("alter_tablegroup_partition_when_is_not_binding_ fail", KR(ret), K(tablegroup_id),
            K(schema_version_before_alter), K(schema_version_after_alter), K(start_serve_timestamp));
      }
    }
  }

  return ret;
}

int ObLogPartMgr::get_table_ids_in_tablegroup_(const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    const int64_t schema_version,
    const int64_t timeout,
    ObArray<uint64_t> &table_id_array)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_ISNULL(schema_getter)) {
    LOG_ERROR("schema getter is NULL", K(schema_getter));
    ret = OB_ERR_UNEXPECTED;
  }
  /// A fallback schema guard is needed to get all the tables under a tablegroup
  else if (OB_FAIL(schema_getter->get_fallback_schema_guard(tenant_id, schema_version, timeout,
      schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_fallback_schema_guard fail", KR(ret), K(tenant_id), K(schema_version), K(tablegroup_id));
    }
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(tenant_id, tablegroup_id,
      table_id_array, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_table_ids_in_tablegroup fail", KR(ret), K(tenant_id),
          K(tablegroup_id), K(table_id_array));
    }
  } else {
    LOG_INFO("get_table_ids_in_tablegroup by fallback schema mode succ", K(tenant_id),
        K(tablegroup_id), K(schema_version), K(table_id_array));
  }

  return ret;
}

int ObLogPartMgr::alter_tablegroup_partition_when_is_not_binding_(
    const uint64_t tablegroup_id,
    const int64_t schema_version_before_alter,
    ObLogSchemaGuard &old_schema_guard,
    const int64_t schema_version_after_alter,
    ObLogSchemaGuard &new_schema_guard,
    const int64_t start_serve_timestamp,
    const share::ObWorker::CompatMode &compat_mode,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_id_array;
  table_id_array.reset();

  // The get_table_ids_in_tablegroup() interface can only use the fallback schema guard
  // The new schema version is used here to get the table ids in the tablegroup
  if (OB_FAIL(get_table_ids_in_tablegroup_(tenant_id_,
      tablegroup_id,
      schema_version_after_alter,
      timeout,
      table_id_array))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_table_ids_in_tablegroup fail", KR(ret), K(tenant_id_),
          K(tablegroup_id), K(schema_version_after_alter), K(table_id_array));
    }
  } else {
    const int64_t table_count_in_tablegroup = table_id_array.count();
    ISTAT("[DDL] [ALTER_TABLEGROUP_NO_BINDING]", K_(tenant_id), K(tablegroup_id),
        K(table_count_in_tablegroup), K(schema_version_before_alter), K(schema_version_after_alter),
        K(start_serve_timestamp));

    // To ensure that functions can be called repeatedly, get the full schema first
    // Get the Full Table Schema here, Lazy Mode
    ObArray<const ObSimpleTableSchemaV2 *> new_tb_schema_array;
    ObArray<const ObSimpleTableSchemaV2 *> old_tb_schema_array;
    // The tenant_name of all tables in a tablegroup is constant, but get_schema_info_based_on_table_schema_ will initially reset the tenant_name/db_name to NULL each time
    // So it is not safe to use a global tenant_name when the tablegroup contains tables that need to be filtered out, e.g. including local indexes
    ObArray<const char *> tenant_name_array;
    ObArray<const char *> db_name_array;
    ObArray<bool> table_is_ignored_array;

    for (int64_t idx = 0; OB_SUCCESS == ret && idx < table_count_in_tablegroup; idx++) {
      const uint64_t table_id = table_id_array.at(idx);
      const ObSimpleTableSchemaV2 *new_tb_schema = NULL;
      const ObSimpleTableSchemaV2 *old_tb_schema = NULL;
      bool table_is_ignored = false;
      const char *tenant_name = NULL;
      const char *db_name = NULL;

      if (OB_FAIL(new_schema_guard.get_table_schema(table_id, new_tb_schema, timeout))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("get table schema fail", KR(ret), K(table_id));
        }
      } else if (OB_ISNULL(new_tb_schema)) {
        LOG_WARN("schema error: table does not exist in target schema", K(table_id),
            "schema_version", schema_version_after_alter);
        ret = OB_TENANT_HAS_BEEN_DROPPED;
      } else if (OB_FAIL(get_schema_info_based_on_table_schema_(new_tb_schema, new_schema_guard,
              timeout, table_is_ignored, tenant_name, db_name))) {
        if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
          LOG_ERROR("get schemas fail", KR(ret), "table_id", new_tb_schema->get_table_id(),
              "schema_version", schema_version_after_alter);
        }
      } else if (OB_FAIL(old_schema_guard.get_table_schema(table_id, old_tb_schema, timeout))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("get table schema fail", KR(ret), K(table_id));
        }
      } else if (OB_ISNULL(old_tb_schema)) {
        LOG_WARN("schema error: table does not exist in target schema", K(table_id),
            "schema_version", schema_version_before_alter);
        ret = OB_TENANT_HAS_BEEN_DROPPED;
      } else {
        if (OB_FAIL(new_tb_schema_array.push_back(new_tb_schema))) {
          LOG_ERROR("new_tb_schema_array push_back fail", KR(ret), K(table_id), KPC(new_tb_schema));
        } else if (OB_FAIL(old_tb_schema_array.push_back(old_tb_schema))) {
          LOG_ERROR("old_tb_schema_array push_back fail", KR(ret), K(table_id), KPC(old_tb_schema));
        } else if (OB_FAIL(tenant_name_array.push_back(tenant_name))) {
          LOG_ERROR("tenant_name_array push_back fail", KR(ret), K(table_id), K(tenant_name));
        } else if (OB_FAIL(db_name_array.push_back(db_name))) {
          LOG_ERROR("db_name_array push_back fail", KR(ret), K(table_id), K(db_name));
        } else if (OB_FAIL(table_is_ignored_array.push_back(table_is_ignored))) {
          LOG_ERROR("table_is_ignored_array push_back fail", KR(ret), K(table_id), K(table_is_ignored));
        }
      }
    } // for

    // Processing logic
    if (OB_SUCC(ret)) {
      for (int64_t idx = 0; OB_SUCCESS == ret && idx < table_count_in_tablegroup; idx++) {
        const uint64_t table_id = table_id_array.at(idx);
        bool table_is_ignored = table_is_ignored_array.at(idx);
        const ObSimpleTableSchemaV2 *new_tb_schema = new_tb_schema_array.at(idx);
        const ObSimpleTableSchemaV2 *old_tb_schema = old_tb_schema_array.at(idx);
        const char *tenant_name = tenant_name_array.at(idx);
        const char *db_name = db_name_array.at(idx);

        if (table_is_ignored) {
          // do nothing
          // Filter first to avoid invalid db_name/tenant_name of index table
        } else if (OB_ISNULL(new_tb_schema) || OB_ISNULL(old_tb_schema) || OB_ISNULL(db_name)
            || OB_ISNULL(tenant_name)) {
          LOG_ERROR("new_tb_schema or old_tb_schema or db_name or tenant_name is null",
              K(table_id), K(new_tb_schema), K(old_tb_schema), K(db_name), K(tenant_name));
          ret = OB_ERR_UNEXPECTED;
        } else if (new_tb_schema->is_in_logical_split()) {
          // tablegroup split
          // split_table_ performs the filter table logic, so the upper level does not judge
          if (OB_FAIL(split_table_(new_tb_schema, tenant_name, db_name, start_serve_timestamp, compat_mode))) {
            // split_table_ performs the filter table logic, so the upper level does not judge
            LOG_ERROR("split_table_ fail", KR(ret), K(tenant_id_), K(tablegroup_id),
                "table_id", new_tb_schema->get_table_id(),
                "schema_version", schema_version_after_alter,
                K(start_serve_timestamp), "compat_mode", print_compat_mode(compat_mode));
          }
        } else {
          // tablegroup dynamically adds and removes partitions
          // Only whitelisted tables are manipulated
          bool table_is_chosen = false;
          bool is_primary_table_chosen = false;
          const bool is_tablegroup = false;
          const bool has_physical_part = has_physical_part_(*new_tb_schema);

          if (OB_FAIL(filter_table_(new_tb_schema, tenant_name, db_name, compat_mode, table_is_chosen, is_primary_table_chosen))) {
            LOG_ERROR("filter table fail", KR(ret));
          } else if (! table_is_chosen) {
            // do nothing
          } else {
            if (OB_FAIL(alter_table_add_or_drop_partition_(
                is_tablegroup,
                has_physical_part,
                start_serve_timestamp,
                old_tb_schema,
                new_tb_schema,
                new_tb_schema->get_database_id(),
                "alter_tablegroup_partition_when_is_not_binding"))) {
              LOG_ERROR("alter table add or drop partition fail", KR(ret),
                  K(is_tablegroup),
                  K(has_physical_part),
                  K(start_serve_timestamp),
                  K(old_tb_schema),
                  K(new_tb_schema));
            } else {
              // succ
            }
          }
        }
      } // for
    }
  }

  return ret;
}

int ObLogPartMgr::add_inner_tables(const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;
  const int64_t served_part_count_before = NULL != map_ ? map_->get_valid_count() : 0;
  const bool enable_backup_mode = (TCONF.enable_backup_mode != 0);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! enable_backup_mode)) {
    LOG_ERROR("inner tables can only be added on backup mode", K(enable_backup_mode), K(tenant_id_),
        K(start_serve_tstamp), K(start_schema_version));
    ret = OB_NOT_SUPPORTED;
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
  // Add internal tables for normal tenants required in backup mode
  // TODO: Only all_sequence_value table is currently available in this mode
  else if (OB_FAIL(do_add_inner_tables_(
          schema_guard,
          start_serve_tstamp,
          start_schema_version,
          timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("add inner tables on backup mode fail", KR(ret), K(tenant_id_),
          K(start_serve_tstamp), K(start_schema_version));
    }
  } else {
    const int64_t total_served_part_count = map_->get_valid_count();
    ISTAT("[ADD_INNER_TABLES_ON_BACKUP_MODE]", K_(tenant_id),
        K(start_serve_tstamp),
        K(start_schema_version),
        "tenant_served_inner_table_part_count", total_served_part_count - served_part_count_before,
        K(total_served_part_count));
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
  const int64_t served_part_count_before = NULL != map_ ? map_->get_valid_count() : 0;

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
  }
  // add all tablegroups
  else if (OB_FAIL(do_add_all_tablegroups_(
      schema_guard,
      start_serve_tstamp,
      start_schema_version,
      timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("add all tablegroups fail", KR(ret), K(tenant_id_), K(start_serve_tstamp),
          K(start_schema_version));
    }
  } else {
    const int64_t total_served_part_count = map_->get_valid_count();
    ISTAT("[ADD_ALL_TABLES_AND_TABLEGROUPS]", K_(tenant_id),
        K(start_serve_tstamp),
        K(start_schema_version),
        "tenant_served_part_count", total_served_part_count - served_part_count_before,
        K(total_served_part_count));
  }

  return ret;
}

int ObLogPartMgr::do_add_all_tablegroups_(
    ObLogSchemaGuard &schema_guard,
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const bool is_create_partition = false;
  ObArray<const ObTablegroupSchema *> tg_schemas;
  TenantSchemaInfo tenant_schema_info;

  // get tenant schema info
  if (OB_FAIL(schema_guard.get_tenant_schema_info(tenant_id_, tenant_schema_info, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_schema_info fail", KR(ret), K(tenant_id_));
    }
  }
  // get all tablegroup schema
  else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id_, tg_schemas, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tablegroup_schemas_in_tenant fail", KR(ret), K(tenant_id_), K(tg_schemas));
    }
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tg_schemas.count(); ++idx) {
      const ObTablegroupSchema *tg_schema = tg_schemas.at(idx);

      if (OB_ISNULL(tg_schema)) {
        LOG_ERROR("tg_schema is NULL", K(idx), K(tg_schema));
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int64_t tablegroup_id = tg_schema->get_tablegroup_id();

        if (OB_FAIL(add_tablegroup_partition_(
            tablegroup_id,
            *tg_schema,
            start_serve_tstamp,
            is_create_partition,
            tenant_schema_info.name_,
            timeout))) {
          LOG_ERROR("add_tablegroup_partition_ fail", KR(ret), K(tablegroup_id),
              K(start_serve_tstamp), K(is_create_partition), K(tenant_schema_info));
        }
      }
    } // for
  }

  ISTAT("[ADD_ALL_TABLEGROUPS]", KR(ret), "tablegroup_count", tg_schemas.count(),
      K(start_serve_tstamp), K(start_schema_version), K(tenant_schema_info));

  return ret;
}

int ObLogPartMgr::do_add_inner_tables_(
    ObLogSchemaGuard &schema_guard,
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;

  // get_table_schemas_in_tenant will fetch all table schema at this time, including primary tables, index tables
  if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schemas in tenant fail", KR(ret), K(tenant_id_), K(start_schema_version));
    }
  } else {
    const int64_t table_schema_count = table_schemas.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_count; i++) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      const char *tenant_name = NULL;
      const char *db_name = NULL;
      bool table_is_ignored = false;
      bool is_create_partition = false;

      if (OB_ISNULL(table_schema)) {
        LOG_ERROR("table_schema is null", K(i), K(tenant_id_), K(table_schemas));
        ret = OB_ERR_UNEXPECTED;
      }
      // filter some table if in backup mode
      else if (! BackupTableHelper::is_sys_table_exist_on_backup_mode(
            table_schema->is_sys_table(), table_schema->get_table_id())) {
        // skip
      }
      // get tenant、db schema
      else if (OB_FAIL(get_schema_info_based_on_table_schema_(table_schema, schema_guard, timeout,
              table_is_ignored, tenant_name, db_name))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("get schemas fail", KR(ret), "table_id", table_schema->get_table_id(),
              "table_name", table_schema->get_table_name(), K(start_schema_version));
        }
      } else if (OB_UNLIKELY(table_is_ignored)) {
        LOG_ERROR("table should not be ignored by get_schema_info_based_on_table_schema_",
            "table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(),
            K(start_schema_version), K(table_is_ignored));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, table_schema,
              tenant_name, db_name, timeout))) {
        LOG_ERROR("add table fail", KR(ret), "table_id", table_schema->get_table_id(),
            "table_name", table_schema->get_table_name(), K(start_serve_tstamp),
            K(is_create_partition), K(tenant_name), K(db_name));
      } else {
        // succ
      }
    }

    if (OB_SUCC(ret)) {
      ISTAT("[DO_ADD_INNER_TABLES]", K(tenant_id_),
          K(start_serve_tstamp), K(start_schema_version));
    }
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
      // 当处理到全局索引表/唯一索引表, 需要获取主表schema
      const ObSimpleTableSchemaV2 *primary_table_schema = NULL;

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
    } else if (OB_FAIL(add_table_(start_serve_tstamp, is_create_partition, table_schema,
        tenant_name, db_name, timeout))) {
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

int ObLogPartMgr::get_ddl_pkey_(const uint64_t tenant_id, const int64_t schema_version,
    ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = combine_id(tenant_id, OB_ALL_DDL_OPERATION_TID);
  if (OB_SYS_TENANT_ID == tenant_id) {
    // The DDL partition of the sys tenant needs to be queried for schema, as it may have been upgraded from an old cluster and the partition count may be 1
    // Adding a DDL partition for a sys tenant must be the start-up time, the timeout here is written dead
    ObLogSchemaGuard schema_guard;
    int64_t timeout = GET_SCHEMA_TIMEOUT_ON_START_UP;
    const ObSimpleTableSchemaV2 *tb_schema = NULL;

    // The __all_ddl_operation table schema for the sys tenant should not fail to fetch, or report an error if it does
    if (OB_FAIL(get_schema_guard_and_table_schema_(table_id, schema_version, timeout,
        schema_guard, tb_schema))) {
      LOG_ERROR("get_schema_guard_and_table_schema_ fail", KR(ret), K(table_id), K(schema_version));
    } else if (OB_ISNULL(tb_schema)) {
      LOG_ERROR("table schema is NULL", K(table_id), K(schema_version));
      ret = OB_ERR_UNEXPECTED;
    } else {
      bool check_dropped_schema = false;
      ObTablePartitionKeyIter pkey_iter(*tb_schema, check_dropped_schema);
      if (OB_FAIL(pkey_iter.next_partition_key_v2(pkey))) {
        LOG_ERROR("iterate pkey fail", KR(ret), K(table_id), K(pkey));
      }
      // Only one DDL partition is supported
      else if (OB_UNLIKELY(1 != pkey_iter.get_partition_num())) {
        LOG_ERROR("partition number of DDL partition is not 1, not supported", K(tenant_id),
            K(schema_version), K(pkey_iter.get_partition_num()), K(pkey));
        ret = OB_NOT_SUPPORTED;
      } else {
        // success
      }
    }
  } else {
    // DDL partitioning is fixed for common tenants
    if (OB_FAIL(pkey.init(table_id, 0, 0))) {
      LOG_ERROR("partition key init fail", KR(ret), K(table_id), K(tenant_id));
    }
  }

  LOG_INFO("get_ddl_pkey", KR(ret), K(tenant_id), K(pkey), K(schema_version));
  return ret;
}

// add all_ddl_operation table for tenant
int ObLogPartMgr::add_ddl_table(
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const bool is_create_tenant)
{
  int ret = OB_SUCCESS;
  ObPartitionKey ddl_pkey;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(inited_));
  } else if (OB_FAIL(get_ddl_pkey_(tenant_id_, start_schema_version, ddl_pkey))) {
    LOG_ERROR("get_ddl_pkey_ fail", KR(ret), K(tenant_id_), K(start_schema_version), K(ddl_pkey));
  } else {
    bool add_succ = false;
    uint64_t tg_id = 0;
    uint64_t db_id = 0;
    const bool has_physical_part = true;    // Physical entities exist by default in DDL partitions

    // DDL partition adding process without tg_id and db_id
    if (OB_FAIL(add_served_partition_(
        ddl_pkey,
        ddl_pkey,
        start_serve_tstamp,
        is_create_tenant,
        has_physical_part,
        tg_id,
        db_id,
        add_succ))) {
      LOG_ERROR("add partition fail", KR(ret), K(ddl_pkey), K(start_serve_tstamp),
          K(is_create_tenant), K(has_physical_part), K(add_succ), K(tg_id), K(db_id));
    } else if (add_succ) {
      ISTAT("[ADD_DDL_TABLE]", K_(tenant_id), K(ddl_pkey),
          "is_schema_split_mode", TCTX.is_schema_split_mode_, K(start_schema_version),
          K(start_serve_tstamp), K(is_create_tenant));
    } else {
      LOG_ERROR("DDL partition add fail", K_(tenant_id), K(ddl_pkey),
          "is_schema_split_mode", TCTX.is_schema_split_mode_, K(start_schema_version),
          K(start_serve_tstamp), K(is_create_tenant));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int ObLogPartMgr::inc_part_trans_count_on_serving(bool &is_serving,
    const ObPartitionKey &key,
    const uint64_t prepare_log_id,
    const int64_t prepare_log_timestamp,
    const bool print_partition_not_serve_info,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  // TODO: verify prepare_log_id

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! key.is_valid())
      || OB_UNLIKELY(prepare_log_timestamp <= 0)
      || OB_UNLIKELY(OB_INVALID_ID == prepare_log_id)) {
    LOG_ERROR("invalid argument", K(key), K(prepare_log_timestamp), K(prepare_log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(prepare_log_timestamp < TCTX.start_tstamp_)) {
    // If the Prepare log timestamp is less than the start timestamp, it must not be served
    is_serving = false;
    _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] LOG_TSTAMP(%ld) <= START_TSTAMP(%ld) "
        "PART=%s LOG_ID=%ld TENATN_ID=%lu",
        prepare_log_timestamp, TCTX.start_tstamp_, to_cstring(key), prepare_log_id, tenant_id_);
  } else {
    // In test mode, determine if want to block the participant list confirmation process, and if so, wait for a period of time
    if (TCONF.test_mode_on) {
      int64_t block_time_us = TCONF.test_mode_block_verify_participants_time_sec * _SEC_;
      if (block_time_us > 0) {
        ISTAT("[INC_TRANS_COUNT] [TEST_MODE_ON] block to verify participants",
            K_(tenant_id), K(block_time_us));
        usleep((useconds_t)block_time_us);
      }
    }
    // First save the current version of Schema
    int64_t schema_version = ATOMIC_LOAD(&cur_schema_version_);

    // Then determine if the partitioned transaction is serviced, and if so, increase the number of transactions
    ret = inc_trans_count_on_serving_(is_serving, key, print_partition_not_serve_info);

    // Handle cases where partitions do not exist
    // Special treatment is needed here for normal global index partitions, they exist for a long time but don't pull
    // their logs, they need to be filtered out, here a cache of normal global index tables should be added to maintain their lifecycle as normal tables are maintained.
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;

      // Check if it is a global normal index partition
      bool is_exist = false;
      const bool is_global_normal_index = true;
      if (OB_FAIL(is_exist_table_id_cache_(key.get_table_id(), is_global_normal_index, is_exist))) {
        LOG_ERROR("is_exist_table_id_cache_ fail", KR(ret), K(key), K(is_global_normal_index), K(is_exist));
      } else if (is_exist) {
        // Filtering global general index partitions
        is_serving = false;

        if (print_partition_not_serve_info) {
          _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] [GLOBAL_NORMAL_INDEX_TABLE] "
              "TENANT=%lu PART=%s LOG_ID=%ld LOG_TSTAMP=%ld SCHEMA_VERSION=%ld",
              tenant_id_, to_cstring(key), prepare_log_id, prepare_log_timestamp, schema_version);
        } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
          _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] [GLOBAL_NORMAL_INDEX_TABLE] "
              "TENANT=%lu PART=%s LOG_ID=%ld LOG_TSTAMP=%ld SCHEMA_VERSION=%ld",
              tenant_id_, to_cstring(key), prepare_log_id, prepare_log_timestamp, schema_version);
        } else {
          // do nothing
        }
      } else {
        // Determine the status of the partition based on the previously saved schema
        PartitionStatus part_status = PART_STATUS_INVALID;
        ret = check_part_status_(key, schema_version, timeout, part_status);

        if (OB_SUCCESS != ret) {
          if (OB_TIMEOUT != ret) {
            LOG_ERROR("check_part_status_ fail", KR(ret), K(key), K(schema_version));
          }
        }
        // handle future partition
        else if (PART_NOT_CREATE == part_status) {
          if (OB_FAIL(handle_future_part_when_inc_trans_count_on_serving_(is_serving,
              key,
              print_partition_not_serve_info,
              schema_version,
              timeout))) {
            if (OB_TIMEOUT != ret) {
              LOG_ERROR("handle_future_part_when_inc_trans_count_on_serving_ fail",
                  KR(ret), K(key), K(schema_version));
            }
          }
        } else {
          // If it is not a future partition, the partition exists or the partition is deleted, both cases indicating that the partition is not serviced
          is_serving = false;

          if (print_partition_not_serve_info) {
            _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] TENANT=%lu "
                "PART=%s STATUS=%s LOG_ID=%ld LOG_TSTAMP=%ld SCHEMA_VERSION=%ld",
                tenant_id_, to_cstring(key), print_part_status(part_status), prepare_log_id,
                prepare_log_timestamp, schema_version);
          } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
            _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] TENANT=%lu "
                "PART=%s STATUS=%s LOG_ID=%ld LOG_TSTAMP=%ld SCHEMA_VERSION=%ld",
                tenant_id_, to_cstring(key), print_part_status(part_status), prepare_log_id,
                prepare_log_timestamp, schema_version);
          } else {
            // do nothing
          }
        }
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObLogPartMgr::inc_trans_count_on_serving_(bool &is_serving,
    const ObPartitionKey &key,
    const bool print_partition_not_serve_info)
{
  int ret = OB_SUCCESS;
  ObLogPartInfo *info = NULL;
  bool enable_create = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(map_->get(key, info, enable_create))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get PartInfo from map fail", KR(ret), K(key));
    } else {
      // not exist
    }
  } else if (OB_ISNULL(info)) {
    LOG_ERROR("get PartInfo from map fail, PartInfo is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {

    info->inc_trans_count_on_serving(is_serving);

    if (! is_serving) {
      if (print_partition_not_serve_info) {
        PART_ISTAT(info, "[INC_TRANS_COUNT] [PART_NOT_SERVE]");
      } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
        PART_ISTAT(info, "[INC_TRANS_COUNT] [PART_NOT_SERVE]");
      } else {
        // do nothing
      }
    } else {
      PART_DSTAT(info, "[INC_TRANS_COUNT]");
    }
  }

  REVERT_PART_INFO(info, ret);
  return ret;
}

int ObLogPartMgr::check_part_status_(const common::ObPartitionKey &pkey,
    const int64_t schema_version,
    const int64_t timeout,
    PartitionStatus &part_status)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  part_status = PART_STATUS_INVALID;
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid schema getter", K(schema_getter));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(schema_getter->get_lazy_schema_guard(pkey.get_tenant_id(), schema_version,
      timeout, schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard fail", "tenant_id", pkey.get_tenant_id(), KR(ret),
          K(schema_version));
    }
  } else if (OB_FAIL(schema_guard.query_partition_status(pkey, part_status, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("query_partition_status fail", KR(ret), K(pkey), K(part_status));
    }
  } else {
  }

  // Partition is considered deleted if the tenant does not exist
  if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
    part_status = PART_DELETED;
    ISTAT("[INC_TRANS_COUNT] [CHECK_PART_STATUS] tenant has been dropped, "
        "partition status set to PART_DELETED",
        KR(ret), K(tenant_id_), K(pkey), K(part_status), K(schema_version));
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    _ISTAT("[INC_TRANS_COUNT] [CHECK_PART_STATUS] TENANT=%lu PKEY=%s STATUS=%s SCHEMA_VERSION=%ld",
        tenant_id_, to_cstring(pkey), print_part_status(part_status), schema_version);
  }

  return ret;
}

int ObLogPartMgr::handle_future_part_when_inc_trans_count_on_serving_(bool &is_serving,
    const ObPartitionKey &key,
    const bool print_partition_not_serve_info,
    const int64_t base_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(base_schema_version <= 0)
      || OB_UNLIKELY(base_schema_version > cur_schema_version_)) {
    LOG_ERROR("invalid argument", K(base_schema_version), K(cur_schema_version_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t start_time = get_timestamp();
    int64_t end_time = start_time + timeout;
    int64_t schema_version = base_schema_version;
    // default to future partition
    PartitionStatus part_status = PART_NOT_CREATE;

    _ISTAT("[INC_TRANS_COUNT] [HANDLE_FUTURE_PART] [BEGIN] TENANT=%lu PART=%s "
        "BASE_SCHEMA_VERSION=%ld",
        tenant_id_, to_cstring(key), base_schema_version);

    // Wait for Schema version to advance until the target partition is no longer a future partition
    while (OB_SUCCESS == ret && PART_NOT_CREATE == part_status) {
      // wait until schema version update to desired value
      if (OB_FAIL(check_cur_schema_version_when_handle_future_part_(schema_version, end_time))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("check_cur_schema_version_when_handle_future_part_ fail", KR(ret), K(key), K(schema_version));
        }
      }

      if (OB_SUCCESS == ret) {
        int64_t left_time = end_time - get_timestamp();
        schema_version = cur_schema_version_;

        part_status = PART_STATUS_INVALID;

        // Check if the partition is still a future partition
        if (OB_FAIL(check_part_status_(key,
            schema_version,
            left_time,
            part_status))) {
          if (OB_TIMEOUT != ret) {
            LOG_ERROR("check_part_status_ fail", KR(ret), K(key), K(schema_version));
          }
        } else {
          // success
        }
      }
    }

    if (OB_SUCCESS == ret) {
      // Guaranteed no more future paritition
      if (OB_UNLIKELY(PART_NOT_CREATE == part_status)) {
        LOG_ERROR("partition is still future partition", K(part_status),
            K(print_part_status(part_status)), K(key),
            K(schema_version), K(cur_schema_version_),
            K(base_schema_version));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // If Schema advances to after partition creation, re-inc_trans_count
        ret = inc_trans_count_on_serving_(is_serving, key, print_partition_not_serve_info);

        // If the partition information does not exist, the partition is deleted, or the partition exists but is not serviced
        if (OB_ENTRY_NOT_EXIST == ret) {
          is_serving = false;
          ret = OB_SUCCESS;

          _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] TENANT=%lu PART=%s STATUS=%s SCHEMA_VERSION=%ld",
              tenant_id_, to_cstring(key), print_part_status(part_status), schema_version);
        }
      }
    }

    _ISTAT("[INC_TRANS_COUNT] [HANDLE_FUTURE_PART] [END] TENANT=%lu RET=%d PART=%s STATUS=%s "
        "IS_SERVING=%d END_SCHEMA_VERSION=%ld INTERVAL=%ld",
        tenant_id_, ret, to_cstring(key), print_part_status(part_status), is_serving, schema_version,
        get_timestamp() - start_time);
  }

  return ret;
}

int ObLogPartMgr::check_cur_schema_version_when_handle_future_part_(const int64_t schema_version,
    const int64_t end_time)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(schema_cond_);

  // Wait until Schema version upgrade
  while (OB_SUCC(ret) && schema_version >= ATOMIC_LOAD(&cur_schema_version_)) {
    int64_t left_time = end_time - get_timestamp();

    if (left_time <= 0) {
      ret = OB_TIMEOUT;
      break;
    }

    schema_cond_.wait_us(left_time);
  }

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

int ObLogPartMgr::dec_part_trans_count(const ObPartitionKey &key)
{
  int ret = OB_SUCCESS;
  bool need_remove = false;
  ObLogPartInfo *info = NULL;
  bool enable_create = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(map_->get(key, info, enable_create))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_ERROR("PartInfo does not exist", K(key));
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_ERROR("get PartInfo from map fail", KR(ret), K(key));
    }
  } else if (OB_ISNULL(info)) {
    LOG_ERROR("get PartInfo from map fail, PartInfo is NULL", KR(ret), K(info));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(info->dec_trans_count(need_remove))) {
    LOG_ERROR("dec_trans_count fail", KR(ret), K(*info));
  } else {
    PART_DSTAT(info, "[DEC_TRANS_COUNT]");

    if (need_remove) {
      if (OB_FAIL(recycle_partition_(key, info))) {
        LOG_ERROR("recycle_partition_ fail", KR(ret), K(key), K(info));
      }
    }
  }

  REVERT_PART_INFO(info, ret);

  return ret;
}

bool ObLogPartMgr::is_partition_served_(const ObPartitionKey &pkey,
      const uint64_t tablegroup_id,
      const uint64_t database_id) const
{
  bool bool_ret = false;

  if (! inited_) {
    bool_ret = false;
  }
  // DDL partition must serve
  else if (is_ddl_table(pkey.get_table_id())) {
    bool_ret = true;
  } else {
    uint64_t hash_v = 0;

    if (0 != TCONF.enable_new_partition_hash_algorithm) {
      // Allowing the use of the new calculation
      // table_id + partition_id to divide tasks
      hash_v = pkey.get_table_id() + pkey.get_partition_id();
    } else {
      uint64_t mod_key = (OB_INVALID_ID == tablegroup_id) ? database_id : tablegroup_id;
      hash_v = murmurhash(&mod_key, sizeof(mod_key), 0);
      uint64_t part_id = pkey.get_partition_id();
      hash_v = murmurhash(&part_id, sizeof(part_id), hash_v);
    }

    bool_ret = ((hash_v % TCONF.instance_num) == TCONF.instance_index);
  }

  return bool_ret;
}

void ObLogPartMgr::print_part_info(int64_t &serving_part_count,
    int64_t &offline_part_count,
    int64_t &not_served_part_count)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    // print PartInfo
    PartInfoPrinter part_info_printer(tenant_id_);
    if (OB_FAIL(map_->for_each(part_info_printer))) {
      LOG_ERROR("PartInfo map foreach fail", KR(ret));
    } else {
      // success
    }

    if (OB_SUCCESS == ret) {
      serving_part_count = part_info_printer.serving_part_count_;
      offline_part_count = part_info_printer.offline_part_count_;
      not_served_part_count = part_info_printer.not_served_part_count_;
    }
  }
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

int ObLogPartMgr::offline_partition(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  bool ensure_recycled_when_offlined = false;

  if (OB_FAIL(offline_partition_(pkey, ensure_recycled_when_offlined))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // partition not exist
      ISTAT("offline partition, but not served", K(pkey));
    } else {
      LOG_ERROR("offline_partition_ fail", K(pkey));
    }
  } else {
    ISTAT("offline and recycle partition success", K_(tenant_id), K(pkey));
  }
  return ret;
}

int ObLogPartMgr::offline_and_recycle_partition(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  bool ensure_recycled_when_offlined = true;

  if (OB_FAIL(offline_partition_(pkey, ensure_recycled_when_offlined))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // partition not exist
      ISTAT("offline and recycle partition, but not served", K(pkey));
    } else {
      LOG_ERROR("offline_partition_ fail", K(pkey));
    }
  } else {
    ISTAT("offline and recycle partition success", K_(tenant_id), K(pkey));
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
  //    不存在的情况。由于liboblog不需要同步系统表，因此此处通过过滤系统表的
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
    } else if (OB_FAIL(schema_guard.get_database_schema_info(db_id, db_schema_info, timeout))) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("get database schema info fail", KR(ret), K(db_id));
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
  const uint64_t tenant_id = extract_tenant_id(table_id);
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid schema getter", K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schema_getter->get_schema_guard_and_table_schema(table_id,
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
  int ret = OB_SUCCESS;

  tg_schema = NULL;
  const uint64_t tenant_id = extract_tenant_id(tablegroup_id);
  IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

  if (OB_ISNULL(schema_getter)) {
    LOG_ERROR("invalid schema getter", K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schema_getter->get_lazy_schema_guard(tenant_id, schema_version, timeout,
      schema_guard))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_lazy_schema_guard fail", KR(ret), K(tenant_id), K(schema_version));
    }
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tg_schema, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get tablegroup schema fail", KR(ret), K(tenant_id), K(tablegroup_id), KPC(tg_schema));
    }
  } else if (OB_ISNULL(tg_schema)) {
    LOG_WARN("schema error: tablegroup does not exist in target schema", K(tenant_id), K(tablegroup_id),
        "schema_version", schema_version);
    ret = OB_TENANT_HAS_BEEN_DROPPED;
  }

  return ret;
}

// fetch Simple Table Schema
// use Full Table Schema instead，cause could only get Full Table Schema under lazy mode
int ObLogPartMgr::get_simple_table_schema_(const uint64_t table_id,
    const int64_t timeout,
    ObLogSchemaGuard &schema_guard,
    const ObSimpleTableSchemaV2 *&tb_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  int64_t schema_version = OB_INVALID_TIMESTAMP;
  tb_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, tb_schema, timeout))) {
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
  const uint64_t tenant_id = extract_tenant_id(table_id);
  int64_t schema_version = OB_INVALID_TIMESTAMP;
  tb_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, tb_schema, timeout))) {
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

int ObLogPartMgr::alter_table_add_or_drop_partition_(
    const bool is_tablegroup,
    const bool has_physical_part,
    const int64_t start_serve_timestamp,
    const ObPartitionSchema *old_tb_schema,
    const ObPartitionSchema *new_tb_schema,
    const int64_t new_database_id,
    const char *event)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(old_tb_schema) || OB_ISNULL(new_tb_schema)) {
    LOG_ERROR("invalid schema", K(old_tb_schema), K(new_tb_schema));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObArray<int64_t> add_part_ids;
    ObArray<int64_t> drop_part_ids;

    uint64_t old_table_id = old_tb_schema->get_table_id();
    int64_t old_table_part_cnt = old_tb_schema->get_partition_cnt();
    uint64_t new_table_id = new_tb_schema->get_table_id();
    // PKEY的partition count字段
    int64_t new_table_part_cnt = new_tb_schema->get_partition_cnt();

    if (OB_UNLIKELY(old_table_id != new_table_id)
        || OB_UNLIKELY(old_table_part_cnt != new_table_part_cnt)) {
      LOG_ERROR("table_id or table_part_cnt is not equal in old/new table schema",
          K(old_table_id), K(new_table_id), K(old_table_part_cnt), K(new_table_part_cnt));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObPartMgrUtils::get_part_diff(*old_tb_schema, *new_tb_schema,
          drop_part_ids, add_part_ids))) {
      LOG_ERROR("get_part_diff fail", KR(ret), K(old_tb_schema), K(new_tb_schema),
          K(drop_part_ids), K(add_part_ids));
    } else {
      if (OB_FAIL(alter_table_drop_partition_(
          is_tablegroup,
          old_table_id,
          drop_part_ids,
          old_table_part_cnt))) {
        LOG_ERROR("alter table drop partition fail", KR(ret),
            K(is_tablegroup),
            K(old_table_id),
            K(drop_part_ids),
            K(old_table_part_cnt));
      } else if (OB_FAIL(alter_table_add_partition_(
          is_tablegroup,
          has_physical_part,
          new_table_id,
          add_part_ids,
          new_table_part_cnt,
          start_serve_timestamp,
          new_tb_schema->get_tablegroup_id(),
          new_database_id))) {
        LOG_ERROR("alter table add partition fail", KR(ret),
            K(is_tablegroup),
            K(has_physical_part),
            K(new_table_id),
            K(add_part_ids),
            K(new_table_part_cnt),
            K(start_serve_timestamp),
            K(new_database_id));
      } else {
        // do nothing
      }

      ISTAT("[DDL] [ALTER_TABLE_ADD_OR_DROP_PART]", KR(ret), K(event), K(is_tablegroup),
          K(has_physical_part), K_(tenant_id), K(new_table_id),
          "drop_cnt", drop_part_ids.count(), K(drop_part_ids),
          "add_cnt", add_part_ids.count(), K(add_part_ids));
    }
  }

  return ret;
}

int ObLogPartMgr::alter_table_drop_partition_(
    const bool is_tablegroup,
    const uint64_t table_id,
    const common::ObArray<int64_t> &drop_part_ids,
    const int64_t partition_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    // 删除分区
    for (int64_t idx = 0; OB_SUCC(ret) && idx < drop_part_ids.count(); ++idx) {
      int64_t partition_id = drop_part_ids.at(idx);
      ObPartitionKey pkey(table_id, partition_id, partition_cnt);

      if (OB_FAIL(offline_partition_(pkey))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ISTAT("[DDL] [ALTER_TABLE_DROP_PART] partition not served", K(pkey));
          ret = OB_SUCCESS;
        }
      } else {
        ISTAT("[DDL] [ALTER_TABLE_DROP_PART]", K(is_tablegroup), K_(tenant_id), K(pkey),
            K(table_id), K(idx), "drop_part_count", drop_part_ids.count());
      }
    } // for
  }

  return ret;
}

int ObLogPartMgr::alter_table_add_partition_(
    const bool is_tablegroup,
    const bool has_physical_part,
    const uint64_t table_id,
    const common::ObArray<int64_t> &add_part_ids,
    const int64_t partition_cnt,  // PKEY的partition count字段
    const int64_t start_serve_timestamp,
    const uint64_t tablegroup_id,
    const uint64_t database_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    // 动态新增的分区
    const bool is_create_partition = true;

    // 增加分区
    for (int64_t idx = 0; OB_SUCC(ret) && idx < add_part_ids.count(); ++idx) {
      int64_t partition_id = add_part_ids.at(idx);
      ObPartitionKey pkey(table_id, partition_id, partition_cnt);
      bool add_succ = false;

      if (OB_FAIL(add_served_partition_(
          pkey,
          pkey,
          start_serve_timestamp,
          is_create_partition,
          has_physical_part,
          tablegroup_id,
          database_id,
          add_succ))) {
        LOG_ERROR("alter table add partition fail", KR(ret), K(pkey), K(start_serve_timestamp),
            K(is_create_partition), K(has_physical_part), K(add_succ));
      } else if (add_succ) {
        ISTAT("[DDL] [ALTER_TABLE_ADD_PART]", K_(tenant_id), K(table_id), K(is_tablegroup),
            K(has_physical_part), K(pkey), K(add_succ));
      } else {
        // do nothing
      }
    } // for
  }

  return ret;
}

int ObLogPartMgr::split_table_(const ObSimpleTableSchemaV2 *tb_schema,
    const char *tenant_name,
    const char *db_name,
    const int64_t start_serve_timestamp,
    const share::ObWorker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;
  bool table_is_chosen = false;
  bool is_primary_table_chosen = false;

  if (OB_ISNULL(tb_schema) || OB_ISNULL(tenant_name) || OB_ISNULL(db_name)) {
    LOG_ERROR("invalid schema", K(tb_schema), K(tenant_name), K(db_name));
    ret = OB_INVALID_ARGUMENT;
  }
  // 过滤表，只操作白名单中的表
  else if (OB_FAIL(filter_table_(tb_schema, tenant_name, db_name, compat_mode, table_is_chosen, is_primary_table_chosen))) {
    LOG_ERROR("filter table fail", KR(ret));
  } else if (! table_is_chosen) {
    // table is ignored
    LOG_INFO("table is not served, table split DDL is filtered", K(table_is_chosen),
        "table_id", tb_schema->get_table_id(),
        "table_name", tb_schema->get_table_name(),
        K(db_name),
        K(tenant_name));
  } else {
    const bool is_tablegroup = false;
    const uint64_t table_id = tb_schema->get_table_id();
    const uint64_t tg_id = tb_schema->get_tablegroup_id();
    const uint64_t db_id = tb_schema->get_database_id();
    // table非bind, 非PG才有实体分区
    const bool has_physical_part = has_physical_part_(*tb_schema);
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter pkey_iter(*tb_schema, check_dropped_schema);

    if (OB_FAIL(split_table_or_tablegroup_(
        is_tablegroup,
        table_id,
        tg_id,
        db_id,
        has_physical_part,
        start_serve_timestamp,
        pkey_iter,
        *tb_schema))) {
      LOG_ERROR("split_table_or_tablegroup_ fail", KR(ret),
          K(is_tablegroup),
          K(table_id),
          K(tg_id),
          K(db_id),
          K(has_physical_part),
          K(start_serve_timestamp));
    } else {
      _ISTAT("[DDL] [SPLIT_TABLE] TENANT=%s(%lu) TABLE=%s.%s.%s(%ld) START_TSTAMP=%ld HAS_PHY_PART=%d",
          tenant_name, tenant_id_, tenant_name, db_name,
          tb_schema->get_table_name(), tb_schema->get_table_id(),
          start_serve_timestamp, has_physical_part);
    }
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
  int ret = OB_SUCCESS;
  bool table_is_chosen = false;
  bool is_primary_table_chosen = false;
  // get tenant mode: MYSQL or ORACLE
  // 1. oracle database/table matc needs to be case sensitive
  // 2. mysql match don't needs to be case sensitive
  share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;
  // The primary table schema is invalid, indicating that it is the primary table itself
 const uint64_t primary_table_id =
   (NULL == primary_table_schema) ? tb_schema->get_table_id() : primary_table_schema->get_table_id();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id_, compat_mode, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id_));
    }
  // The filter_table_ function checks the validity of the schema, so current function no longer checks
  } else if (OB_FAIL(filter_table_(tb_schema, tenant_name, db_name, compat_mode, table_is_chosen,
          is_primary_table_chosen, primary_table_schema))) {
    LOG_ERROR("filter table fail", KR(ret), K(db_name), K(tenant_name), K(compat_mode));
  } else if (! table_is_chosen) {
    // table is ignored
    // If the primary table matches:
    // 1. global common index, then add global common index cache
    // 2. unique index (not global unique index), then add TableIDCache directly

    if (is_primary_table_chosen) {
      if (tb_schema->is_global_normal_index_table()
          || is_unique_index_table_but_expect_global_unqiue_index_(*tb_schema)) {
        // Handling global general indexes
        if (OB_FAIL(add_table_id_into_cache_(*tb_schema, db_name, primary_table_id))) {
          LOG_ERROR("add_table_id_into_cache_ fail", KR(ret),
              "table_id", tb_schema->get_table_id(),
              "table_name", tb_schema->get_table_name(),
              K(db_name),
              K(primary_table_id),
              "is_global_normal_index_table", tb_schema->is_global_normal_index_table(),
              "is_unique_index", tb_schema->is_unique_index());
        } else {
          // succ
        }
      }
    }
  } else {
    const bool is_tablegroup = false;
    const uint64_t tb_id = tb_schema->get_table_id();
    const uint64_t tg_id = tb_schema->get_tablegroup_id();
    const uint64_t db_id = tb_schema->get_database_id();
    // table is not bind, only non-PG has entity partition
    const bool has_physical_part = has_physical_part_(*tb_schema);
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter pkey_iter(*tb_schema, check_dropped_schema);
    int64_t served_part_count = 0;

    if (TCONF.enable_hbase_mode && ! tb_schema->is_in_recyclebin()) {
      const char *tb_name = tb_schema->get_table_name();
      const int64_t schema_version = tb_schema->get_schema_version();
      // If the table name contains $, then it is probably an hbase model table, get ObTableSchema
      ObString tb_name_str(tb_name);
      if (NULL != tb_name_str.find('$')) {
        // HBase mode: If you encounter an HBase table, you need to get the ObTableSchema, cannot use force lazy mode at this time.
        const ObTableSchema *full_table_schema = NULL;
        ObLogSchemaGuard schema_guard_for_full_table_schema;
        IObLogSchemaGetter *schema_getter = TCTX.schema_getter_;

        if (OB_ISNULL(schema_getter)) {
          LOG_ERROR("invalid schema getter", K(schema_getter));
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(schema_getter->get_schema_guard_and_full_table_schema(tb_id, schema_version, timeout,
                schema_guard_for_full_table_schema, full_table_schema))) {
          if (OB_TIMEOUT != ret) {
            LOG_ERROR("get_schema_guard_and_full_table_schema fail", KR(ret), K(tb_id), KPC(full_table_schema));
          }
        } else if (OB_ISNULL(full_table_schema)) {
          LOG_ERROR("full_table_schema is NULL", K(tb_id), K(schema_version), K(full_table_schema));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(TCTX.hbase_util_.add_hbase_table_id(*full_table_schema))) {
          LOG_ERROR("hbase_util_ add_hbase_table_id", KR(ret), K(tb_id), K(tb_name));
        } else {
          // succ
        }
      }
    }

    if (OB_FAIL(ret)) {
      // fail
    } else if (OB_FAIL(add_table_id_into_cache_(*tb_schema, db_name, primary_table_id))) {
      // Adding primary tables and globally unique index tables to TableIDCache
      LOG_ERROR("add_table_id_into_cache_ fail", KR(ret),
          "table_id", tb_schema->get_table_id(),
          "table_name", tb_schema->get_table_name(),
          K(db_name),
          K(primary_table_id),
          K(db_id),
          "is_global_unique_index_table", tb_schema->is_global_unique_index_table());
    } else if (OB_FAIL(add_table_or_tablegroup_(
        is_tablegroup,
        tb_id,
        tg_id,
        db_id,
        has_physical_part,
        is_create_partition,
        start_serve_tstamp,
        pkey_iter,
        *tb_schema,
        served_part_count))) {
      LOG_ERROR("add_table_or_tablegroup_ fail", KR(ret),
          K(is_tablegroup),
          K(tb_id),
          K(tg_id),
          K(db_id),
          K(has_physical_part),
          K(is_create_partition),
          K(start_serve_tstamp),
          K(tb_schema));
    } else {
      _ISTAT("[DDL] [ADD_TABLE] TENANT=%s(%ld) TABLE=%s.%s.%s(%ld) HAS_PHY_PART=%d "
          "SERVED_PART_COUNT=%ld TABLE_GROUP=%ld DATABASE=%ld "
          "START_TSTAMP=%ld IS_CREATE=%d IS_GLOBAL_UNIQUE_INDEX=%d.",
          tenant_name, tenant_id_, tenant_name, db_name,
          tb_schema->get_table_name(), tb_id, has_physical_part,
          served_part_count, tg_id, db_id, start_serve_tstamp, is_create_partition,
          tb_schema->is_global_unique_index_table());
    }
  }

  return ret;
}

template <class PartitionKeyIter, class PartitionSchema>
int ObLogPartMgr::add_table_or_tablegroup_(
    const bool is_tablegroup,
    const uint64_t table_id,
    const uint64_t tablegroup_id,
    const uint64_t db_id,
    const bool has_physical_part,
    const bool is_create_partition,
    const int64_t start_serve_tstamp,
    PartitionKeyIter &pkey_iter,
    PartitionSchema &table_schema,
    int64_t &served_part_count)
{
  int ret = OB_SUCCESS;
  served_part_count = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    ObPartitionKey iter_pkey;
    const bool is_in_split = table_schema.is_in_logical_split();
    int64_t table_schema_version = table_schema.get_schema_version();
    // The start service time of the split source partition selects the time at which the split begins
    //
    // The purpose is: to ensure that the split source partition must be able to pull the split log so that the split source partition
    // can end itself based on the split log. Otherwise, if the split source partition's start timestamp is greater than the split log timestamp,
    // the split source partition will not be able to pull the split log
    // Since OB has no mechanism to guarantee that the ilog of the partition's OFFLINE log must fall off the disk, there may be scenarios
    // where the OFFLINE log is never pulled, so that the OFFLINE log cannot be relied upon to end the partition, and the split source partition
    // still relies on the split log to end itself.
    //
    // If a table is in split, the schema version of the table is the start time of the split (not the exact point in time,
    // it may be larger than the split log timestamp), here the minimum value of the schema version and the start service timestamp is chosen
    // as the source partition start timestamp.
    int64_t split_src_start_serve_tstamp = std::min(table_schema_version, start_serve_tstamp);

    // Iterate through all partitions
    while (OB_SUCCESS == ret && OB_SUCC(pkey_iter.next_partition_key_v2(iter_pkey))) {
      // The default is to decide for self whether to serve or not
      ObPartitionKey check_serve_info_pkey = iter_pkey;
      ObPartitionKey src_pkey;

      // Dealing with split situations
      if (is_in_split) {
        bool src_part_add_succ = false;

        // Get split source partition
        if (OB_FAIL(table_schema.get_split_source_partition_key(iter_pkey, src_pkey))) {
          LOG_ERROR("get_split_source_partition_key fail", KR(ret), K(iter_pkey), K(table_id),
              K(is_tablegroup));
        } else {
          // A partition is in the process of splitting and whether or not it is served should depend on whether or not its split source partition is served
          check_serve_info_pkey = src_pkey;

          if (src_pkey == iter_pkey) {
            // Partition not split, not processed
          }
          // Add the split source partition, it decides for itself whether to service it or not
          // Note: the same split source partition will be added more than once, here it returns success
          else if (OB_FAIL(add_served_partition_(
              src_pkey,
              src_pkey,
              split_src_start_serve_tstamp,
              is_create_partition,
              has_physical_part,
              tablegroup_id,
              db_id,
              src_part_add_succ))) {
            LOG_ERROR("add_served_partition_ fail", KR(ret),
                K(src_pkey),
                K(split_src_start_serve_tstamp),
                K(is_create_partition),
                K(has_physical_part),
                K(tablegroup_id), K(db_id));
          } else if (src_part_add_succ) {
            ISTAT("[DDL] [ADD_TABLE_OR_TABLEGROUP] [ADD_SPLIT_SRC_PART]",
                K_(tenant_id), K(is_tablegroup), K(has_physical_part),
                K(src_pkey), "dst_pkey", iter_pkey,
                "split_src_start_serve_tstamp", TS_TO_STR(split_src_start_serve_tstamp),
                "tablegroup_schema_version", TS_TO_STR(table_schema_version),
                "start_serve_tstamp", TS_TO_STR(start_serve_tstamp), K(is_create_partition));
          }
        }
      }

      if (OB_SUCC(ret)) {
        bool add_succ = false;
        if (OB_FAIL(add_served_partition_(
            iter_pkey,
            check_serve_info_pkey,
            start_serve_tstamp,
            is_create_partition,
            has_physical_part,
            tablegroup_id,
            db_id,
            add_succ))) {
          LOG_ERROR("add served partition fail", KR(ret),
              K(iter_pkey),
              K(start_serve_tstamp),
              K(is_create_partition),
              K(has_physical_part),
              K(add_succ),
              K(tablegroup_id),
              K(db_id));
        } else if (add_succ) {
          served_part_count++;
        } else {
          // do nothing
        }
      }

      DSTAT("[DDL] [ADD_TABLE_OR_TABLEGROUP] FOR_EACH_PART", KR(ret), K(is_tablegroup), K(is_in_split),
          K_(tenant_id), K(iter_pkey), K(src_pkey));

      iter_pkey.reset();
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
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
      } else if (OB_FAIL(drop_table_or_tablegroup_(
          is_tablegroup,
          table_id,
          table_name,
          *table_schema,
          served_part_count))) {
        LOG_ERROR("drop_table_or_tablegroup_ fail", KR(ret),
            K(is_tablegroup),
            K(table_id),
            K(table_name));
      } else {
        _ISTAT("[DDL] [DROP_TABLE] [END] TENANT=%lu TABLE=%s(%ld) "
            "IS_GLOBAL_UNIQUE_INDEX=%d SERVED_PART_COUNT=%ld TOTAL_PART_COUNT=%ld",
            tenant_id_,
            table_schema->get_table_name(),
            table_schema->get_table_id(),
            table_schema->is_global_unique_index_table(),
            served_part_count,
            map_->get_valid_count());
      }
    }
  }

  return ret;
}

int ObLogPartMgr::filter_table_(const ObSimpleTableSchemaV2 *table_schema,
    const char *tenant_name,
    const char *db_name,
    const share::ObWorker::CompatMode &compat_mode,
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
    if (compat_mode == share::ObWorker::CompatMode::ORACLE
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

int ObLogPartMgr::filter_tablegroup_(const ObTablegroupSchema *tg_schema,
    const char *tenant_name,
    const share::ObWorker::CompatMode &compat_mode,
    bool &chosen)
{
  int ret = OB_SUCCESS;
  IObLogTableMatcher *tb_matcher = TCTX.tb_matcher_;
  chosen = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tg_schema) || OB_ISNULL(tenant_name) || OB_ISNULL(tb_matcher)) {
    LOG_ERROR("invalid argument", K(tg_schema), K(tenant_name), K(tb_matcher));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *tablegroup_name = tg_schema->get_tablegroup_name_str();

    // Default mysql and oracle mode are both case-insensitive
    // when configured with enable_oracle_mode_match_case_sensitive=1, oracle is case sensitive
    int fnmatch_flags = FNM_CASEFOLD;
    if (compat_mode == share::ObWorker::CompatMode::ORACLE
        && enable_oracle_mode_match_case_sensitive_) {
      fnmatch_flags = FNM_NOESCAPE;
    }

    if (OB_FAIL(tb_matcher->tablegroup_match(tenant_name, tablegroup_name, chosen, fnmatch_flags))) {
      LOG_ERROR("match table fail", KR(ret), K(tablegroup_name), K(tenant_name));
    } else {
      // succ
    }
  }

  return ret;
}

// check_serve_info_pkey: partition key used to check whether the partition is served, normally the partition itself,
// feels itself served or not, in split scenarios the partition key before the split is used to determine whether the partition key after the split is served
int ObLogPartMgr::add_served_partition_(const ObPartitionKey &pkey,
    const ObPartitionKey &check_serve_info_pkey,
    const int64_t start_serve_tstamp,
    const bool is_create_partition,
    const bool has_physical_part,
    const uint64_t tablegroup_id,
    const uint64_t db_id,
    bool &add_succ)
{
  int ret = OB_SUCCESS;
  add_succ = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (! has_physical_part) {
    // No physical partition, no treatment
    ISTAT("[ADD_SERVED_PART] no physical partition, need not add", K(pkey), K(has_physical_part),
        K(tablegroup_id), K(db_id), K(is_create_partition), K(start_serve_tstamp));
  } else {
    // Check if the partition is serviced
    bool is_served = is_partition_served_(check_serve_info_pkey, tablegroup_id, db_id);

    // Add partitions
    // Include partitions that are serviced and partitions that are not serviced
    if (OB_FAIL(add_partition_(pkey, start_serve_tstamp, is_create_partition, is_served))) {
      if (OB_ENTRY_EXIST == ret) {
        // partition already exist
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("add_partition_ fail", KR(ret), K(pkey), K(start_serve_tstamp),
            K(is_create_partition), K(tablegroup_id), K(db_id), K(is_served));
      }
    } else {
      if (is_served) {
        add_succ = true;
      }
    }
  }

  return ret;
}

// Pre-check before adding a service partition
int ObLogPartMgr::add_served_part_pre_check_(const ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  bool is_tenant_serving = false;
  // Request a partition slot from the tenant structure and if the tenant is no longer in service, it cannot be added further
  if (OB_FAIL(host_.inc_part_count_on_serving(pkey, is_tenant_serving))) {
    LOG_ERROR("inc_part_count_on_serving fail", KR(ret), K(pkey), K_(host));
  } else if (OB_UNLIKELY(! is_tenant_serving)) {
    // The tenant is not in service
    // The current implementation, when a tenant is not in service, does not perform DDL tasks for that tenant, so that cannot happen here
    LOG_ERROR("add partition when tenant is not serving, unexpected",
        K(is_tenant_serving), K(host_), K(pkey));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

// Note: add_partition is not thread-safe
// The caller has to ensure that adding and deleting partitions are executed serially under lock protection
int ObLogPartMgr::add_partition_(const ObPartitionKey& pkey,
    const int64_t start_serve_tstamp,
    const bool is_create_partition,
    const bool is_served)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (start_serve_tstamp <= 0) {
    LOG_ERROR("invalid argument", K(start_serve_tstamp));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ENTRY_EXIST == (ret = map_->contains_key(pkey))) {
    // add repeatly
    LOG_INFO("partition has been added", KR(ret), K_(tenant_id), K(pkey), K(start_serve_tstamp),
        K(is_create_partition), K(is_served));
  } else if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
    LOG_ERROR("check partition exist fail", KR(ret), K(pkey), K(start_serve_tstamp),
        K(is_create_partition));
    ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
  } else {
    // If the element does not exist, continue adding
    ret = OB_SUCCESS;

    ObLogPartInfo *info = NULL;
    uint64_t start_log_id = is_create_partition ? 1 : OB_INVALID_ID;

    // If partitioning services, perform preflight checks
    if (is_served && OB_FAIL(add_served_part_pre_check_(pkey))) {
      LOG_ERROR("add_served_part_pre_check_ fail", KR(ret), K(pkey), K(is_served));
    }
    // Dynamic assignment of an element
    else if (OB_ISNULL(info = map_->alloc())) {
      LOG_ERROR("alloc part info fail", K(info), K(pkey));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    // Perform initialization
    else if (OB_FAIL(info->init(pkey, is_create_partition, start_serve_tstamp, is_served))) {
      LOG_ERROR("init part info fail", KR(ret), K(pkey), K(start_serve_tstamp),
          K(is_create_partition), K(is_served));
      map_->free(info);
      info = NULL;
    } else {
      // Print before performing map insertion, as the insertion will be seen by others afterwards and there is a concurrent modification scenario
      if (is_served) {
        PART_ISTAT(info, "[DDL] [ADD_PART]");
      } else {
        PART_ISTAT(info, "[DDL] [ADD_NOT_SERVED_PART]");
      }

      // Inserting elements, the insert interface does not have get() semantics and does not require revert
      if (OB_FAIL(map_->insert(pkey, info))) {
        LOG_ERROR("insert part into map fail", KR(ret), K(pkey), KPC(info));
      } else {
        // The info structure cannot be reapplied afterwards and may be deleted at any time
        info = NULL;

        // For partitioning of services, execute the add-partition callback
        if (is_served
            && OB_FAIL(call_add_partition_callbacks_(pkey, start_serve_tstamp, start_log_id))) {
          // The add partition callback should not add partitions repeatedly, here it returns OB_ERR_UNEXPECTED, an error exits
          if (OB_ENTRY_EXIST == ret) {
            LOG_ERROR("call add-partition callbacks fail, add repeated partition",
                KR(ret), K(pkey), K(start_serve_tstamp), K(start_log_id));
            ret = OB_ERR_UNEXPECTED;
          } else {
            LOG_ERROR("call add-partition callbacks fail", KR(ret), K(pkey), K(start_serve_tstamp),
                K(start_log_id));
          }
        }
      }
    }
  }

  return ret;
}

// ensure_recycled_when_offlined: Whether recycling is guaranteed to be successful in the event of going offline, 
// i.e. whether the number of transactions left behind is 0
int ObLogPartMgr::offline_partition_(const ObPartitionKey &pkey,
    const bool ensure_recycled_when_offlined)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    ObLogPartInfo *info = NULL;
    bool enable_create = false;
    int64_t end_trans_count = 0;

    ret = map_->get(pkey, info, enable_create);
    if (OB_ENTRY_NOT_EXIST == ret) {
      // not exist
    } else if (OB_SUCCESS != ret) {
      LOG_ERROR("get PartInfo from map fail", KR(ret), K(pkey));
    } else if (OB_ISNULL(info)) {
      LOG_ERROR("get PartInfo from map fail, PartInfo is NULL", KR(ret), K(pkey));
      ret = OB_ERR_UNEXPECTED;
    } else if (info->offline(end_trans_count)) {  // Atomic switch to OFFLINE state
      ISTAT("[OFFLINE_PART] switch offline state success", K(pkey), K(end_trans_count),
          K(ensure_recycled_when_offlined), KPC(info));

      // Recycle the partition if there are no ongoing transactions on it
      if (0 == end_trans_count) {
        if (OB_FAIL(recycle_partition_(pkey, info))) {
          LOG_ERROR("recycle_partition_ fail", KR(ret), K(pkey), K(info));
        }
      }
      // An error is reported if a recycle operation must be performed when asked to go offline,
      // indicating that in this case there should be no residual transactions for the external guarantee
      else if (ensure_recycled_when_offlined) {
        LOG_ERROR("there are still transactions waited, can not recycle", K(pkey),
            K(end_trans_count), KPC(info));
        ret = OB_INVALID_DATA;
      }
    } else {
      PART_ISTAT(info, "[OFFLINE_PART] partition has been in offline state");
    }

    REVERT_PART_INFO(info, ret);
  }

  return ret;
}

int ObLogPartMgr::recycle_partition_(const ObPartitionKey &pkey, ObLogPartInfo *info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(info)) {
    LOG_ERROR("invalid argument", K(info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PART_ISTAT(info, "[RECYCLE_PART]");

    // Notify the modules that the partition is being recycled
    if (OB_FAIL(call_recycle_partition_callbacks_(pkey))) {
      LOG_ERROR("call recycle-partition callbacks fail", KR(ret), K(pkey));
    } else if (OB_FAIL(map_->remove(pkey))) { // Deleting records from Map
      LOG_ERROR("remove PartInfo from map fail", KR(ret), K(pkey));
    } else {
      // succ
    }
  }
  return ret;
}

int ObLogPartMgr::call_add_partition_callbacks_(const ObPartitionKey &pkey,
    const int64_t start_serve_tstamp,
    const uint64_t start_log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    for (int64_t index = 0; OB_SUCCESS == ret && index < part_add_cb_array_->count(); index++) {
      PartAddCallback *cb = NULL;
      if (OB_FAIL(part_add_cb_array_->at(index, reinterpret_cast<int64_t &>(cb)))) {
        LOG_ERROR("get callback from array fail", KR(ret), K(index));
      } else if (OB_ISNULL(cb)) {
        LOG_ERROR("get callback from array fail, callback is NULL", KR(ret), K(index), K(cb));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(cb->add_partition(pkey, start_serve_tstamp, start_log_id))) {
        LOG_ERROR("add_partition fail", KR(ret), K(pkey), K(start_serve_tstamp), K(cb),
            K(start_log_id));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogPartMgr::call_recycle_partition_callbacks_(const ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("PartMgr has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    for (int64_t index = 0; OB_SUCCESS == ret && index < part_rc_cb_array_->count(); index++) {
      PartRecycleCallback *cb = NULL;
      if (OB_FAIL(part_rc_cb_array_->at(index, (int64_t &)cb))) {
        LOG_ERROR("get callback from array fail", KR(ret), K(index));
      } else if (OB_ISNULL(cb)) {
        LOG_ERROR("get callback from array fail, callback is NULL", KR(ret), K(index), K(cb));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(cb->recycle_partition(pkey))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // partition not exist as expected
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("recycle_partition fail", KR(ret), K(pkey), K(cb));
        }
      } else {
        // succ
      }
    }
  }

  return ret;
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

bool ObLogPartMgr::need_to_support_split_when_in_multi_instance_() const
{
  bool bool_ret = false;

  if (TCONF.instance_num > 1) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

int ObLogPartMgr::drop_all_tables()
{
  int ret = OB_SUCCESS;
  PartInfoScannerByTenant scanner(tenant_id_);
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited", K(inited_));
    ret = OB_NOT_INIT;
  }
  // Iterate through the partitions being served by the current tenant
  else if (OB_FAIL(map_->for_each(scanner))) {
    LOG_ERROR("scan map fail", KR(ret), K(tenant_id_));
  } else {
    _ISTAT("[DDL] [DROP_TENANT] [DROP_ALL_TABLES] [BEGIN] TENANT=%ld TENANT_PART_COUNT=%ld "
        "TOTAL_PART_COUNT=%ld",
        tenant_id_, scanner.pkey_array_.count(), map_->get_valid_count());

    for (int64_t index = 0; OB_SUCCESS == ret && index < scanner.pkey_array_.count(); index++) {
      const ObPartitionKey &pkey = scanner.pkey_array_.at(index);
      ret = offline_partition_(pkey);

      if (OB_ENTRY_NOT_EXIST == ret) {
        DSTAT("[DDL] [DROP_TENANT] partition not served", K(pkey));
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        LOG_ERROR("offline partition fail", KR(ret), K(pkey));
      } else {
        // succ
      }
    }

    if (OB_SUCCESS == ret) {
      if (OB_FAIL(clean_table_id_cache_())) {
        LOG_ERROR("clean_table_id_cache_ fail", KR(ret), K(tenant_id_));
      }
    }

    _ISTAT("[DDL] [DROP_TENANT] [DROP_ALL_TABLES] [END] TENANT=%ld TOTAL_PART_COUNT=%ld",
        tenant_id_, map_->get_valid_count());
  }
  return ret;
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
  }

  return ret;
}

}
}
