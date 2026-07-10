/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "pl/sys_package/ob_dbms_catalog_stats.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_lock_unlock.h"
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_executor.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_preferences.h"
#include "share/ob_share_util.h"
#include "sql/ob_sql_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/charset/ob_charset.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/parser/ob_parser.h"
#include "storage/ob_locality_manager.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/ob_cluster_version.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
using namespace share::schema;

namespace pl
{

static const char *CATALOG_STATS_BATCH_SIZE = "CATALOG_STATS_BATCH_SIZE";
static const int64_t MAX_CATALOG_PREFS_NAME_LEN = 30;

class ObCatalogStatPrefs
{
public:
  explicit ObCatalogStatPrefs(const ObString &pvalue) : pvalue_(pvalue) {}
  virtual ~ObCatalogStatPrefs() {}
  virtual int check_pref_value_validity() = 0;
protected:
  ObString pvalue_;
};

class ObCatalogGatherStatBatchSizePrefs : public ObCatalogStatPrefs
{
public:
  explicit ObCatalogGatherStatBatchSizePrefs(const ObString &pvalue) : ObCatalogStatPrefs(pvalue) {}
  virtual int check_pref_value_validity() override;
};

int ObCatalogGatherStatBatchSizePrefs::check_pref_value_validity()
{
  int ret = OB_SUCCESS;
  if (pvalue_.empty()) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("catalog gather stats batch size should not be empty", K(ret));
  } else {
    int64_t value = 0;
    bool is_negative = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pvalue_.length(); ++i) {
      const char c = pvalue_.ptr()[i];
      if (0 == i && c == '-') {
        is_negative = true;
      } else if (c >= '0' && c <= '9') {
        const int64_t digit = static_cast<int64_t>(c - '0');
        if (value > (INT64_MAX - digit) / 10) {
          ret = OB_ERR_DBMS_STATS_PL;
          LOG_WARN("catalog gather stats batch size overflow", K(ret), K(pvalue_));
        } else {
          value = value * 10 + digit;
        }
      } else {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_WARN("catalog gather stats batch size should be integer", K(ret), K(pvalue_));
      }
    }
    if (OB_SUCC(ret) && is_negative) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("catalog gather stats batch size should be >= 0", K(ret), K(pvalue_));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Illegal catalog gather stats batch size.");
  }
  return ret;
}

/**
 * @brief ObDbmsCatalogStats::gather_table_stat
 * Catalog table statistics collection entry
 * Corresponds to ObDbmsStats::gather_table_stats for internal tables
 *
 * @param ctx
 * @param params
 *      0. catname            VARCHAR2,
 *      1. dbname             VARCHAR2,
 *      2. tabname            VARCHAR2,
 *      3. partname           VARCHAR2 DEFAULT NULL,
 *      4. estimate_percent   NUMBER   DEFAULT AUTO_SAMPLE_SIZE,
 *      5. sample_type        VARCHAR2 DEFAULT NULL,
 *      6. method_opt         VARCHAR2 DEFAULT DEFAULT_METHOD_OPT,
 *      7. degree             NUMBER   DEFAULT NULL,
 *      8. granularity        VARCHAR2 DEFAULT DEFAULT_GRANULARITY,
 *      9. force              BOOLEAN  DEFAULT FALSE
 * @param result
 * @return
 */
int ObDbmsCatalogStats::gather_table_stat(sql::ObExecContext &ctx,
                                          sql::ParamStore &params,
                                          common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);

  ObCatalogTableStatParam stat_param;
  stat_param.allocator_ = &ctx.get_allocator();
  bool is_all_fast_gather = false;
  bool is_continue_collect = true; // Mark as incremental collect or not.
  ObArray<ObString> partitions_to_delete; // Partitions that need to be deleted (removed from remote)

  int64_t start_time = ObTimeUtility::current_time();
  ObOptStatTaskInfo task_info;
  int64_t task_cnt = 1;

  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(ctx.get_my_session()));
  } else if (OB_FAIL(init_gather_task_info(ctx,
                                           ObOptStatGatherType::MANUAL_GATHER,
                                           start_time,
                                           task_cnt,
                                           task_info))) {
    LOG_WARN("failed to init gather task info", K(ret));
  } else {
    ObOptStatGatherStat gather_stat(task_info);
    ObOptStatGatherAudit audit(ctx.get_allocator());
    ObOptStatGatherStatList::instance().push(gather_stat);
    ObOptStatRunningMonitor running_monitor(ctx.get_allocator(),
                                            start_time,
                                            stat_param.allocator_->used(),
                                            gather_stat,
                                            audit);
    const int64_t param_count_base = 3; // catalog_name, db_name, table_name.
    if (OB_FAIL(running_monitor.add_monitor_info(ObOptStatRunningPhase::GATHER_PREPARE))) {
      LOG_WARN("failed to add monitor info", K(ret));
    } else if (params.count() < param_count_base) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param count for catalog table stats", K(ret), K(params.count()));
    } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                             params.at(1),
                                             params.at(2),
                                             params.count() > 3 ? params.at(3) : ObObjParam(),
                                             ctx,
                                             stat_param))) {
      LOG_WARN("failed to parse table part info", K(ret));
    } else if (OB_FAIL(parse_catalog_table_stats_params(params, ctx, stat_param))) {
      LOG_WARN("failed to parse catalog table stats params", K(ret));
    } else if (OB_FAIL(parse_method_opt_and_filter_columns(ctx, stat_param))) {
      LOG_WARN("failed to parse method opt and filter columns", K(ret));
    } else if (OB_FAIL(get_stats_consumer_group_id(stat_param))) {
      LOG_WARN("failed to get stats consumer group id", K(ret));
    } else if (OB_FAIL(running_monitor.add_table_info(stat_param))) {
      LOG_WARN("failed to add table info", K(ret));
    } else if (stat_param.gather_options_.force_
               && OB_FAIL(
                   ObDbmsCatalogStatsLockUnlock::fill_catalog_stat_locked(ctx, stat_param))) {
      LOG_WARN("failed fill stat locked", K(ret));
    } else if (!stat_param.gather_options_.force_
               && OB_FAIL(
                   ObDbmsCatalogStatsLockUnlock::check_catalog_stat_locked(ctx, stat_param))) {
      LOG_WARN("failed check stat locked", K(ret));
    } else if (!stat_param.need_gather_stats()) {
      LOG_WARN("catalog table no need to gather stats", K(ret));
    }
    // TODO(bitao): support to collect incremental partition stats.
    // else if (!stat_param.gather_options_.force_
    //            && OB_FAIL(ObDbmsCatalogStatsUtils::filter_catalog_partition_infos_by_modify_time(
    //                ctx,
    //                stat_param,
    //                is_continue_collect,
    //                partitions_to_delete))) {
    //   LOG_WARN("failed to filter partition infos by modify time", K(ret));
    // }

    if (OB_FAIL(ret)) {
    // } else if (!is_continue_collect) {
    //   LOG_INFO("skip to collect catalog table stats",
    //            K(is_continue_collect),
    //            K_(stat_param.table_identity_.tenant_id),
    //            K_(stat_param.table_identity_.catalog_id),
    //            K_(stat_param.table_identity_.db_name),
    //            K_(stat_param.table_identity_.tab_name),
    //            K_(stat_param.part_name),
    //            K(stat_param.part_infos_.count()));
    } else {
      // Delete stale partition stats (partitions that no longer exist in remote)
      if (!partitions_to_delete.empty()) {
        if (OB_FAIL(ObDbmsCatalogStatsUtils::delete_stale_partition_stats(
                ctx, stat_param, partitions_to_delete))) {
          LOG_WARN("failed to delete stale partition stats", K(ret),
                   K(partitions_to_delete.count()));
          // Non-fatal error, continue with collection
          ret = OB_SUCCESS;
        } else {
          LOG_INFO("deleted stale partition stats", K(partitions_to_delete.count()));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_continue_collect) {
      // Already logged above
    } else if (OB_FAIL(ObDbmsCatalogStatsExecutor::gather_table_stats(stat_param,
                                                                      ctx,
                                                                      running_monitor))) {
      LOG_WARN("failed to gather catalog table stats", K(ret));
    } else if (OB_FAIL(update_catalog_stat_cache(ctx.get_my_session()->get_rpc_tenant_id(),
                                                 stat_param,
                                                 &running_monitor))) {
      LOG_WARN("failed to update catalog stat cache", K(ret));
    } else {
      LOG_TRACE("Succeed to gather catalog table stats", K(stat_param), K(running_monitor));
    }

    if (ret == OB_SUCCESS || ret == OB_TIMEOUT) {
      int tmp_ret = ret;
      if (OB_FAIL(running_monitor.flush_gather_audit())) {
        LOG_WARN("failed to flush gather audit", K(ret));
      } else {
        ret = tmp_ret;
      }
    }

    running_monitor.set_monitor_result(ret,
                                       ObTimeUtility::current_time(),
                                       stat_param.allocator_->used());
    task_info.task_end_time_ = ObTimeUtility::current_time();
    task_info.ret_code_ = ret;
    task_info.failed_count_ = ret == OB_SUCCESS ? 0 : 1;
    update_optimizer_gather_stat_info(&task_info, &gather_stat, stat_param);
    ObOptStatGatherStatList::instance().remove(gather_stat);
  }

  LOG_INFO("gather catalog table stats finish",
           K(ret),
           "cost_time",
           ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObDbmsCatalogStats::set_catalog_table_prefs(sql::ObExecContext &ctx,
                                                sql::ParamStore &params,
                                                common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObObjParam dummy_part;
  dummy_part.set_null();
  ObCatalogTableStatParam stat_param;
  ObString opt_name;
  ObString opt_value;
  stat_param.allocator_ = &ctx.get_allocator();
  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (params.count() < 5) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for set catalog table prefs", K(ret), K(params.count()));
  } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                           params.at(1),
                                           params.at(2),
                                           dummy_part,
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  } else if (params.at(3).is_null() || OB_FAIL(params.at(3).get_varchar(opt_name))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pname for catalog prefs", K(ret), K(params.at(3)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *stat_param.allocator_,
                                              false,
                                              opt_name))) {
    LOG_WARN("failed to convert pname", K(ret), K(opt_name));
  } else if (params.at(4).is_null() || OB_FAIL(params.at(4).get_varchar(opt_value))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pvalue for catalog prefs", K(ret), K(params.at(4)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pvalue");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *stat_param.allocator_,
                                              false,
                                              opt_value))) {
    LOG_WARN("failed to convert pvalue", K(ret), K(opt_value));
  } else if (OB_FAIL(check_catalog_prefs_validity(ctx,
                                                  *stat_param.allocator_,
                                                  opt_name,
                                                  opt_value,
                                                  true))) {
    LOG_WARN("failed to check catalog prefs validity", K(ret), K(opt_name), K(opt_value));
  } else if (OB_FAIL(ObDbmsCatalogStatsPreferences::set_prefs(ctx,
                                                              stat_param.table_identity_,
                                                              opt_name,
                                                              opt_value))) {
    LOG_WARN("failed to set catalog prefs", K(ret), K(stat_param.table_identity_), K(opt_name));
  }
  return ret;
}

int ObDbmsCatalogStats::get_catalog_prefs(sql::ObExecContext &ctx,
                                          sql::ParamStore &params,
                                          common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObObjParam dummy_part;
  dummy_part.set_null();
  ObCatalogTableStatParam stat_param;
  ObString opt_name;
  stat_param.allocator_ = &ctx.get_allocator();
  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (params.count() < 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for get catalog prefs", K(ret), K(params.count()));
  } else if (OB_FAIL(parse_table_part_info(params.at(1),
                                           params.at(2),
                                           params.at(3),
                                           dummy_part,
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  } else if (params.at(0).is_null() || OB_FAIL(params.at(0).get_varchar(opt_name))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pname for catalog prefs", K(ret), K(params.at(0)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *stat_param.allocator_,
                                              false,
                                              opt_name))) {
    LOG_WARN("failed to convert pname", K(ret), K(opt_name));
  } else if (OB_FAIL(check_catalog_prefs_validity(ctx,
                                                  *stat_param.allocator_,
                                                  opt_name,
                                                  ObString(),
                                                  false))) {
    LOG_WARN("failed to check catalog prefs validity", K(ret), K(opt_name));
  } else if (OB_FAIL(ObDbmsCatalogStatsPreferences::get_prefs(ctx.get_sql_proxy(),
                                                              *stat_param.allocator_,
                                                              stat_param.table_identity_,
                                                              opt_name,
                                                              result))) {
    LOG_WARN("failed to get catalog prefs", K(ret), K(stat_param.table_identity_), K(opt_name));
  }
  return ret;
}

int ObDbmsCatalogStats::delete_catalog_table_prefs(sql::ObExecContext &ctx,
                                                   sql::ParamStore &params,
                                                   common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObObjParam dummy_part;
  dummy_part.set_null();
  ObCatalogTableStatParam stat_param;
  ObString opt_name;
  stat_param.allocator_ = &ctx.get_allocator();
  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (params.count() < 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for delete catalog prefs", K(ret), K(params.count()));
  } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                           params.at(1),
                                           params.at(2),
                                           dummy_part,
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  } else if (params.at(3).is_null() || OB_FAIL(params.at(3).get_varchar(opt_name))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pname for catalog prefs", K(ret), K(params.at(3)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *stat_param.allocator_,
                                              false,
                                              opt_name))) {
    LOG_WARN("failed to convert pname", K(ret), K(opt_name));
  } else if (OB_FAIL(check_catalog_prefs_validity(ctx,
                                                  *stat_param.allocator_,
                                                  opt_name,
                                                  ObString(),
                                                  false))) {
    LOG_WARN("failed to check catalog prefs validity", K(ret), K(opt_name));
  } else if (OB_FAIL(ObDbmsCatalogStatsPreferences::delete_prefs(ctx,
                                                                 stat_param.table_identity_,
                                                                 opt_name))) {
    LOG_WARN("failed to delete catalog prefs", K(ret), K(stat_param.table_identity_), K(opt_name));
  }
  return ret;
}

int ObDbmsCatalogStats::set_catalog_global_prefs(sql::ObExecContext &ctx,
                                                 sql::ParamStore &params,
                                                 common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString opt_name;
  ObString opt_value;
  ObIAllocator *allocator = &ctx.get_allocator();
  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(ctx.get_my_session()), KP(allocator));
  } else if (params.count() < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for set catalog global prefs", K(ret), K(params.count()));
  } else if (params.at(0).is_null() || OB_FAIL(params.at(0).get_varchar(opt_name))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pname for catalog global prefs", K(ret), K(params.at(0)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *allocator,
                                              false,
                                              opt_name))) {
    LOG_WARN("failed to convert pname", K(ret), K(opt_name));
  } else if (params.at(1).is_null() || OB_FAIL(params.at(1).get_varchar(opt_value))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pvalue for catalog global prefs", K(ret), K(params.at(1)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pvalue");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *allocator,
                                              false,
                                              opt_value))) {
    LOG_WARN("failed to convert pvalue", K(ret), K(opt_value));
  } else if (OB_FAIL(check_catalog_prefs_validity(ctx, *allocator, opt_name, opt_value, true))) {
    LOG_WARN("failed to check catalog global prefs validity", K(ret), K(opt_name), K(opt_value));
  } else if (OB_FAIL(ObDbmsCatalogStatsPreferences::set_global_prefs(ctx, opt_name, opt_value))) {
    LOG_WARN("failed to set catalog global prefs", K(ret), K(opt_name));
  }
  return ret;
}

int ObDbmsCatalogStats::get_catalog_global_prefs(sql::ObExecContext &ctx,
                                                 sql::ParamStore &params,
                                                 common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString opt_name;
  ObIAllocator *allocator = &ctx.get_allocator();
  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(ctx.get_my_session()), KP(allocator));
  } else if (params.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for get catalog global prefs", K(ret), K(params.count()));
  } else if (params.at(0).is_null() || OB_FAIL(params.at(0).get_varchar(opt_name))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pname for catalog global prefs", K(ret), K(params.at(0)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *allocator,
                                              false,
                                              opt_name))) {
    LOG_WARN("failed to convert pname", K(ret), K(opt_name));
  } else if (OB_FAIL(check_catalog_prefs_validity(ctx, *allocator, opt_name, ObString(), false))) {
    LOG_WARN("failed to check catalog global prefs validity", K(ret), K(opt_name));
  } else if (OB_FAIL(ObDbmsCatalogStatsPreferences::get_global_prefs(ctx.get_sql_proxy(),
                                                                     *allocator,
                                                                     ctx.get_my_session()->get_effective_tenant_id(),
                                                                     opt_name,
                                                                     result))) {
    LOG_WARN("failed to get catalog global prefs", K(ret), K(opt_name));
  }
  return ret;
}

int ObDbmsCatalogStats::delete_catalog_global_prefs(sql::ObExecContext &ctx,
                                                    sql::ParamStore &params,
                                                    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObString opt_name;
  ObIAllocator *allocator = &ctx.get_allocator();
  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(ctx.get_my_session()), KP(allocator));
  } else if (params.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for delete catalog global prefs", K(ret), K(params.count()));
  } else if (params.at(0).is_null() || OB_FAIL(params.at(0).get_varchar(opt_name))) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid pname for catalog global prefs", K(ret), K(params.at(0)));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(convert_vaild_ident_name(ctx.get_my_session()->get_dtc_params(),
                                              *allocator,
                                              false,
                                              opt_name))) {
    LOG_WARN("failed to convert pname", K(ret), K(opt_name));
  } else if (OB_FAIL(check_catalog_prefs_validity(ctx, *allocator, opt_name, ObString(), false))) {
    LOG_WARN("failed to check catalog global prefs validity", K(ret), K(opt_name));
  } else if (OB_FAIL(ObDbmsCatalogStatsPreferences::delete_global_prefs(ctx, opt_name))) {
    LOG_WARN("failed to delete catalog global prefs", K(ret), K(opt_name));
  }
  return ret;
}

int ObDbmsCatalogStats::check_catalog_prefs_validity(sql::ObExecContext &ctx,
                                                     common::ObIAllocator &allocator,
                                                     const common::ObString &opt_name,
                                                     const common::ObString &opt_value,
                                                     bool is_set_op)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObCatalogStatPrefs *stat_pref = NULL;
  if (opt_name.empty() || opt_name.length() > MAX_CATALOG_PREFS_NAME_LEN) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("invalid catalog prefs name", K(ret), K(opt_name));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid input values for pname");
  } else if (OB_FAIL(get_new_catalog_stat_pref(allocator, opt_name, opt_value, stat_pref))) {
    LOG_WARN("failed to get catalog stat pref", K(ret), K(opt_name), K(opt_value));
  } else if (OB_ISNULL(stat_pref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stat pref", K(ret));
  } else if (is_set_op && OB_FAIL(stat_pref->check_pref_value_validity())) {
    LOG_WARN("failed to check catalog pref value validity", K(ret), K(opt_name), K(opt_value));
  }
  return ret;
}

int ObDbmsCatalogStats::get_new_catalog_stat_pref(common::ObIAllocator &allocator,
                                                  const common::ObString &opt_name,
                                                  const common::ObString &opt_value,
                                                  ObCatalogStatPrefs *&stat_pref)
{
  int ret = OB_SUCCESS;
  stat_pref = NULL;
  if (0 == opt_name.case_compare(CATALOG_STATS_BATCH_SIZE)) {
    void *ptr = allocator.alloc(sizeof(ObCatalogGatherStatBatchSizePrefs));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc catalog stat pref", K(ret));
    } else {
      stat_pref = new (ptr) ObCatalogGatherStatBatchSizePrefs(opt_value);
    }
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("unsupported catalog prefs pname", K(ret), K(opt_name));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,
                   "Invalid input values for pname, only support CATALOG_STATS_BATCH_SIZE");
  }
  return ret;
}

int ObDbmsCatalogStats::parse_table_part_info(
    const ObObjParam &catalog,
    const ObObjParam &db,
    const ObObjParam &table,
    const ObObjParam &part,
    sql::ObExecContext &ctx,
    ObCatalogTableStatParam &stat_param)
{
  int ret = OB_SUCCESS;
  sql::ObSqlSchemaGuard sql_schema_guard;
  ObArray<ObCatalogExtPartitionInfo *> partition_infos;
  const share::schema::ObTableSchema *table_schema = nullptr;
  share::ObILakeTableMetadata *lake_table_metadata = nullptr;

  if (OB_FAIL(parse_base_stat_param(catalog, db, table, part, ctx, stat_param))) {
    LOG_WARN("failed to parse base stat param", K(ret));
  } else if (OB_FAIL(get_table_schema(stat_param,
                                      ctx,
                                      sql_schema_guard,
                                      table_schema,
                                      lake_table_metadata))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema) || table_schema->is_view_table()
             || !table_schema->is_external_table()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null",
             K(ret),
             K(table_schema),
             K(stat_param.table_identity_.db_name_),
             K(stat_param.table_identity_.tab_name_));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null",
             K(ret),
             K(stat_param.table_identity_.tenant_id_),
             K(stat_param.table_identity_.catalog_id_),
             K(stat_param.table_identity_.db_name_),
             K(stat_param.table_identity_.tab_name_));
  } else {
    // Step 0: Extract external table information from table_schema at the top level
    // This will be used by estimate() to fetch file statistics
    if (OB_FAIL(ob_write_string(*stat_param.allocator_,
                                table_schema->get_external_file_location(),
                                stat_param.external_info_.location_))) {
      LOG_WARN("failed to copy external file location", K(ret));
    } else if (OB_FAIL(ob_write_string(*stat_param.allocator_,
                                       table_schema->get_external_file_location_access_info(),
                                       stat_param.external_info_.access_info_))) {
      LOG_WARN("failed to copy external file location access info", K(ret));
    } else if (OB_FAIL(ObSQLUtils::get_external_table_type(table_schema, stat_param.external_info_.format_type_))) {
      LOG_WARN("failed to get external table format type", K(ret));
    } else {
      stat_param.external_info_.lake_table_format_ = lake_table_metadata->get_format_type();
    }

    if (OB_FAIL(ret)) {
    } else if (share::ObLakeTableFormat::ICEBERG == lake_table_metadata->get_format_type()) {
      if (OB_FAIL(ObDbmsCatalogStatsUtils::collect_iceberg_partition_infos(
                                              *stat_param.allocator_,
                                              lake_table_metadata,
                                              stat_param.part_infos_,
                                              stat_param.global_modified_ts_))) {
        LOG_WARN("failed to collect iceberg partition infos", K(ret));
      } else {
        if (stat_param.part_infos_.count() > 1) {
          stat_param.part_level_ = share::schema::PARTITION_LEVEL_ONE;
        } else if (stat_param.part_infos_.count() == 1) {
          if (stat_param.part_infos_.at(0).partition_values_.empty()) {
            stat_param.part_level_ = share::schema::PARTITION_LEVEL_ZERO;
            stat_param.part_infos_.at(0).partition_ = ObString::make_empty_string();
          } else {
            stat_param.part_level_ = share::schema::PARTITION_LEVEL_ONE;
          }
        }
      }
    } else {
      int64_t refresh_interval_sec = 0;
      share::ObCachedCatalogMetaGetter catalog_meta_getter(*ctx.get_sql_ctx()->schema_guard_,
                                                           *stat_param.allocator_);
      if (OB_FAIL(catalog_meta_getter.get_cache_refresh_interval_sec(lake_table_metadata,
                                                                     refresh_interval_sec))) {
        LOG_WARN("failed to get refresh_interval_sec");
      } else if (OB_FAIL(ObExternalTableUtils::collect_partitions_info_with_cache(
                     *table_schema,
                     sql_schema_guard,
                     *stat_param.allocator_,
                     refresh_interval_sec * 1000,
                     partition_infos))) {
        LOG_WARN("failed get table partitions from cache", K(ret), K(table_schema));
      }

      if (OB_FAIL(ret)) {
      } else if (!table_schema->is_partitioned_table()) {
        LOG_TRACE("table is not partitioned table", K(table_schema->get_part_level()));
        if (OB_FAIL(OB_UNLIKELY(1 != partition_infos.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("non part table should only have one global part info",
                   K(ret),
                   K(partition_infos.count()));
        } else {
          // Store the global table modified time here.
          stat_param.global_modified_ts_ = partition_infos.at(0)->modify_ts_;
          ObCatalogExtPartitionInfo part_info_copy;
          if (OB_FAIL(part_info_copy.assign(*partition_infos.at(0)))) {
            LOG_WARN("failed to assign partition info", K(ret));
          } else {
            part_info_copy.partition_ = ObString::make_empty_string();
            if (OB_FAIL(stat_param.part_infos_.push_back(part_info_copy))) {
              LOG_WARN("failed to push back partition info for non-part table", K(ret));
            }
          }
        }
      } else {
        int64_t refresh_interval_sec = 0;
        const ObString &part_func_expr = table_schema->get_part_option().get_part_func_expr_str();
        if (OB_UNLIKELY(part_func_expr.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition func expr is empty for partitioned table", K(ret));
        } else if (OB_FAIL(split_part_func_exprs_(*stat_param.allocator_,
                                                  part_func_expr,
                                                  stat_param.part_cols_))) {
          LOG_WARN("failed to split partition func exprs", K(ret), K(part_func_expr));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
            const ObCatalogExtPartitionInfo *part_info = partition_infos.at(i);
            // partition_infos strings are on stat_param.allocator_; shallow copy into part_infos_ is ok.
            if (part_info->modify_ts_ > stat_param.global_modified_ts_) {
              stat_param.global_modified_ts_ = part_info->modify_ts_;
            }
            if (OB_FAIL(stat_param.part_infos_.push_back(*part_info))) {
              LOG_WARN("failed to push back partition info", K(ret), K(part_info));
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stat_param.all_part_infos_.assign(stat_param.part_infos_))) {
    LOG_WARN("failed to assign all part infos", K(ret));
  } else if (!part.is_null() && OB_FAIL(parse_partition_name(table_schema, ctx, stat_param))) {
    LOG_WARN("failed to parse partition name", K(ret));
  } else if (OB_FAIL(init_column_stat_params(table_schema,
                                             *stat_param.allocator_,
                                             stat_param.column_params_))) {
    LOG_WARN("failed to init column stat params", K(ret));
  }
  // TODO(bitao): adjust_text_column_basic_stats? and add support for column group stats and adjust
  // text column basic stats.

  return ret;
}

int ObDbmsCatalogStats::parse_base_stat_param(const ObObjParam &catalog,
                                              const ObObjParam &db,
                                              const ObObjParam &table,
                                              const ObObjParam &part,
                                              sql::ObExecContext &ctx,
                                              ObCatalogTableStatParam &stat_param)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  // Traverse params for getting the table schema.
  // 1. Set tenant id.
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    stat_param.table_identity_.tenant_id_ = session->get_effective_tenant_id();
  }
  // 2. Parse catalog name, db_name, table name, partition name.
  ObString catalog_name;
  if (OB_FAIL(ret)) {
  } else if (catalog.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("catalog is null", K(ret));
  } else if (OB_FAIL(catalog.get_varchar(catalog_name))) {
    LOG_WARN("failed to get catalog name", K(ret));
  } else if (OB_FAIL(convert_vaild_ident_name(session->get_dtc_params(),
                                              *stat_param.allocator_,
                                              lib::is_oracle_mode(),
                                              catalog_name))) {
    LOG_WARN("failed to convert valid ident name", K(ret));
  } else {
    stat_param.table_identity_.catalog_name_ = catalog_name;
  }
  ObString db_name;
  if (OB_FAIL(ret)) {
  } else if (db.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db is null", K(ret));
  } else if (OB_FAIL(db.get_varchar(db_name))) {
    LOG_WARN("failed to get db name", K(ret));
  } else {
    stat_param.table_identity_.db_name_ = db_name;
  }
  ObString table_name;
  if (OB_FAIL(ret)) {
  } else if (table.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(table.get_varchar(table_name))) {
    LOG_WARN("failed to get table name", K(ret));
  } else {
    stat_param.table_identity_.tab_name_ = table_name;
  }
  // Partition name is optional.
  ObString partition_name;
  if (OB_FAIL(ret)) {
  } else if (!part.is_null() && OB_FAIL(part.get_varchar(partition_name))) {
    LOG_WARN("failed to get partition name", K(ret));
  } else {
    stat_param.part_name_ = partition_name;
  }

  // 3. Get catalog id from catalog name.
  if (OB_FAIL(ret)) {
  } else if (stat_param.table_identity_.catalog_name_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog name is empty", K(ret));
  } else {
    share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
    const share::schema::ObCatalogSchema *catalog_schema = NULL;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_guard is null", K(ret));
    } else if (OB_FAIL(schema_guard->get_catalog_schema_by_name(stat_param.table_identity_.tenant_id_,
                                                                stat_param.table_identity_.catalog_name_,
                                                                catalog_schema))) {
      LOG_WARN("failed to get catalog schema by name", K(ret));
    } else if (OB_ISNULL(catalog_schema)) {
      ret = OB_CATALOG_NOT_EXIST;
      LOG_USER_ERROR(OB_CATALOG_NOT_EXIST,
                     stat_param.table_identity_.catalog_name_.length(),
                     stat_param.table_identity_.catalog_name_.ptr());
      LOG_WARN("catalog not found", K(ret), K(stat_param.table_identity_.catalog_name_),
               K(stat_param.table_identity_.tenant_id_));
    } else {
      stat_param.table_identity_.catalog_id_ = catalog_schema->get_catalog_id();
      LOG_INFO("successfully got catalog_id from catalog_name",
               K(stat_param.table_identity_.catalog_name_),
               K(stat_param.table_identity_.catalog_id_));
    }
  }
  return ret;
}

int ObDbmsCatalogStats::get_table_schema(ObCatalogTableStatParam &stat_param,
                                         sql::ObExecContext &ctx,
                                         sql::ObSqlSchemaGuard &sql_schema_guard,
                                         const share::schema::ObTableSchema *&table_schema,
                                         share::ObILakeTableMetadata *&lake_table_metadata)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_ctx or schema_guard is null", K(ret));
  } else {
    sql_schema_guard.set_schema_guard(ctx.get_sql_ctx()->schema_guard_);
  }

  uint64_t database_id = OB_INVALID_ID;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_schema_guard.get_catalog_database_id(stat_param.table_identity_.tenant_id_,
                                                              stat_param.table_identity_.catalog_id_,
                                                              stat_param.table_identity_.db_name_,
                                                              database_id))) {
    LOG_WARN("failed to get catalog database id",
             K(ret),
             K(stat_param.table_identity_.tenant_id_),
             K(stat_param.table_identity_.catalog_id_),
             K(stat_param.table_identity_.db_name_));
  } else if (OB_FAIL(sql_schema_guard.get_catalog_table_schema(stat_param.table_identity_.tenant_id_,
                                                               stat_param.table_identity_.catalog_id_,
                                                               database_id,
                                                               stat_param.table_identity_.db_name_,
                                                               stat_param.table_identity_.tab_name_,
                                                               table_schema))) {
    LOG_WARN("failed to get catalog table schema",
             K(ret),
             K(stat_param.table_identity_.tenant_id_),
             K(stat_param.table_identity_.catalog_id_),
             K(database_id),
             K(stat_param.table_identity_.db_name_),
             K(stat_param.table_identity_.tab_name_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    stat_param.part_level_ = table_schema->get_part_level();
  }

  // Fetch partitions from lake table metadata.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_schema_guard.get_lake_table_metadata(stat_param.table_identity_.tenant_id_,
                                                              table_schema->get_table_id(),
                                                              lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ret));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  }
  return ret;
}

int ObDbmsCatalogStats::parse_partition_name(const share::schema::ObTableSchema *table_schema,
                                             sql::ObExecContext &ctx,
                                             ObCatalogTableStatParam &stat_param)
{
  int ret = OB_SUCCESS;
  ObCatalogExtPartitionInfo found_part;
  if (stat_param.part_name_.empty()) {
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(stat_param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null table schema or allocator",
             K(ret),
             KP(table_schema),
             KP(stat_param.allocator_));
  } else if (!table_schema->is_partitioned_table() && stat_param.all_part_infos_.empty()) {
    ret = OB_ERR_NOT_PARTITIONED;
    LOG_WARN("the target table is not partitioned", K(ret));
  } else if (OB_FAIL(find_selected_part_info(stat_param.part_name_,
                                             stat_param.all_part_infos_,
                                             lib::is_oracle_mode(),
                                             found_part))) {
    LOG_WARN("failed to find selected part info", K(ret), K(stat_param.part_name_));
  } else {
    stat_param.part_infos_.reset();
    if (OB_FAIL(stat_param.part_infos_.push_back(found_part))) {
      LOG_WARN("failed to push back partition info", K(ret), K(found_part));
    }
  }
  return ret;
}

int ObDbmsCatalogStats::find_selected_part_info(const ObString &part_name,
                                                const ObIArray<ObCatalogExtPartitionInfo> &part_infos,
                                                bool is_sensitive_compare,
                                                ObCatalogExtPartitionInfo &found_part)
{
  int ret = OB_SUCCESS;
  if (part_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part name is empty", K(ret));
  } else if (!ObDbmsCatalogStatsUtils::find_part(part_infos,
                                                 part_name,
                                                 is_sensitive_compare,
                                                 found_part)) {
    ret = OB_UNKNOWN_PARTITION;
    LOG_WARN("the specified partition is not found", K(ret), K(part_name), K(part_infos));
  }
  return ret;
}

int ObDbmsCatalogStats::init_column_stat_params(const share::schema::ObTableSchema *table_schema,
                                                ObIAllocator &allocator,
                                                ObIArray<ObCatalogColumnStatParam> &column_params)
{
  int ret = OB_SUCCESS;
  column_params.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
    const share::schema::ObColumnSchemaV2 *col = table_schema->get_column_schema_by_idx(i);
    ObCatalogColumnStatParam col_param;
    ObString new_col_name;
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret), K(col));
    } else if (!check_column_validity(*table_schema, *col)) {
      // Skip hidden columns and partition-key columns for non-Iceberg tables.
    } else if (OB_UNLIKELY(col->get_column_name_str().empty())) {
      // Column name from table schema is empty, which should not happen for valid columns
      // Log detailed info for debugging
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column name is empty in table schema",
               K(ret),
               K(i),
               K(col->get_column_id()),
               K(table_schema->get_table_name_str()));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator,
                   col->get_column_name_str(),
                   new_col_name,
                   lib::is_oracle_mode()))) {
      LOG_WARN("fail to generate new name with escape character",
               K(ret),
               K(i),
               K(col->get_column_id()),
               K(col->get_column_name_str()));
    } else if (OB_FAIL(ob_write_string(allocator, new_col_name, col_param.column_name_))) {
      LOG_WARN("failed to write column name", K(ret));
    } else {
      col_param.cs_type_ = col->get_collation_type();
      col_param.gather_flag_ = ColumnGatherFlag::NO_NEED_STAT;
      col_param.set_size_manual();
      col_param.bucket_num_ = -1;
      col_param.column_attribute_ = 0;
      col_param.column_type_ = col->get_data_type();
      col_param.ndv_scale_algo_ = NDV_SCALE_ALGO_DEFAULT;
      if ((lib::is_oracle_mode() && col->get_meta_type().is_varbinary_or_binary())) {
        // oracle don't have this type. but agent table will have this type, such as
        // "SYS"."ALL_VIRTUAL_COLUMN_REAL_AGENT"
      } else {
        if (ObCatalogColumnStatParam::is_valid_opt_col_type(col->get_meta_type().get_type())) {
          col_param.set_valid_opt_col();
        }
        // check need avglen
        if (ObCatalogColumnStatParam::is_valid_avglen_type(col->get_meta_type().get_type())) {
          col_param.set_need_avg_len();
        }
      }
      if (col->is_virtual_generated_column() && !col->is_column_stored_in_sstable()
          && !col->is_tbl_part_key_column() && !col->is_part_key_column()
          && !col->is_subpart_key_column()) {
        col_param.set_is_virtual_col();
      }
      if (col->is_rowkey_column() && table_schema->is_table_with_pk()) {
        if (1 == table_schema->get_rowkey_column_num()) {
          col_param.set_is_unique_column();
          col_param.ndv_scale_algo_ = NDV_SCALE_ALGO_UNIQUE;
        } else if (col->get_rowkey_position() == 0) {
          col_param.ndv_scale_algo_ = NDV_SCALE_ALGO_LINEAR;
        }
      }
      if (!col->is_nullable()) {
        col_param.set_is_not_null_column();
      }
      if (lib::is_mysql_mode()
          && col->get_meta_type().get_type_class() == ColumnTypeClass::ObTextTC) {
        if (col->is_string_lob()) {
          col_param.set_is_string_column();
        } else {
          col_param.set_is_text_column();
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(column_params.push_back(col_param))) {
        LOG_WARN("failed to push back column param", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    ObCatalogColumnStatParam &param = column_params.at(i);
    if (param.is_virtual_column()) {
      param.gather_flag_ = ColumnGatherFlag::NO_NEED_STAT;
    }
  }
  return ret;
}

bool ObDbmsCatalogStats::check_column_validity(const share::schema::ObTableSchema &tab_schema,
                                               const share::schema::ObColumnSchemaV2 &col_schema)
{
  bool is_valid = false;
  if (col_schema.is_part_key_column()
      && share::ObLakeTableFormat::ICEBERG != tab_schema.get_lake_table_format()) {
    // For current Hive tables, partition columns are mostly independent and their
    // metadata is fetched from HMS directly, usually represented by partition paths.
    // Keep them out of this flow for now and adapt path-based handling later if needed.
  } else if (col_schema.is_hidden()) {
    // pass
  } else {
    is_valid = true;
  }
  return is_valid;
}

int ObDbmsCatalogStats::convert_vaild_ident_name(const common::ObDataTypeCastParams &dtc_params,
                                                 common::ObIAllocator &allocator,
                                                 bool need_extra_conv,
                                                 ObString &ident_name)
{
  int ret = OB_SUCCESS;
  if (!ident_name.empty()) {
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(allocator,
                                                                   dtc_params,
                                                                   ident_name))) {
      LOG_WARN("fail to convert charset", K(ret));
    } else if (need_extra_conv) {
      // oracle support lowercase name to gather and manager stats, eg:
      //   create table "t1"(c1 int);
      //   call dbms_stats.gather_table_stats(NULL, '"t1"');
      if (ident_name.length() > 1 && ident_name.ptr()[0] == '\"'
          && ident_name.ptr()[ident_name.length() - 1] == '\"') {
        ident_name.assign(ident_name.ptr() + 1, ident_name.length() - 2);
      } else {
        ObString upper_ident_name;
        if (OB_FAIL(
                ObCharset::toupper(CS_TYPE_UTF8MB4_BIN, ident_name, upper_ident_name, allocator))) {
          LOG_WARN("failed to toupper ident name", K(ret));
        } else {
          ident_name = upper_ident_name;
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStats::parse_catalog_gather_stat_options(const ObObjParam &method_opt,
                                                          const ObObjParam &degree,
                                                          const ObObjParam &granularity,
                                                          const ObObjParam &force,
                                                          sql::ObExecContext &ctx,
                                                          ObCatalogTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();

  if (OB_ISNULL(session) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(session), KP(param.allocator_));
  } else if (method_opt.is_null()) {
    param.gather_options_.method_opt_.assign_ptr("FOR ALL COLUMNS SIZE AUTO", 26);
  } else if (OB_FAIL(method_opt.get_varchar(param.gather_options_.method_opt_))) {
    LOG_WARN("failed to get method opt", K(ret));
  } else if (OB_FAIL(convert_vaild_ident_name(session->get_dtc_params(),
                                              *param.allocator_,
                                              false,
                                              param.gather_options_.method_opt_))) {
    LOG_WARN("failed to convert vaild ident name", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (degree.is_null()) {
    param.gather_options_.degree_ = 1;
  } else {
    number::ObNumber num_degree;
    if (OB_FAIL(degree.get_number(num_degree))) {
      LOG_WARN("failed to get degree number", K(ret));
    } else {
      int64_t degree_val = 0;
      if (OB_FAIL(num_degree.extract_valid_int64_with_trunc(degree_val))) {
        LOG_WARN("failed to extract int64", K(ret));
      } else if (degree_val < 1) {
        param.gather_options_.degree_ = 1;
      } else if (degree_val > INT32_MAX) {
        param.gather_options_.degree_ = INT32_MAX;
      } else {
        param.gather_options_.degree_ = degree_val;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (granularity.is_null()) {
    param.gather_options_.granularity_.assign_ptr("AUTO", 4);
  } else if (OB_FAIL(granularity.get_varchar(param.gather_options_.granularity_))) {
    LOG_WARN("failed to get granularity", K(ret));
  } else if (OB_FAIL(convert_vaild_ident_name(session->get_dtc_params(),
                                              *param.allocator_,
                                              false,
                                              param.gather_options_.granularity_))) {
    LOG_WARN("failed to convert vaild ident name", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (force.is_null()) {
    param.gather_options_.force_ = false;
  } else if (OB_FAIL(force.get_bool(param.gather_options_.force_))) {
    LOG_WARN("failed to get force", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDbmsStatsUtils::parse_granularity(param.gather_options_.granularity_,
                                                         param.gather_options_.granularity_type_))) {
    LOG_WARN("failed to parse granularity", K(ret));
  } else if (OB_FAIL(resolve_catalog_granularity(param))) {
    LOG_WARN("failed to resolve granularity", K(ret));
  } else if (param.gather_options_.method_opt_.empty() || 0 == param.gather_options_.method_opt_.case_compare("Z")) {
    param.gather_options_.method_opt_.assign_ptr("FOR ALL COLUMNS SIZE 1", 23);
  }

  return ret;
}

int ObDbmsCatalogStats::parse_catalog_sample_mode(
    const ObString &sample_type,
    common::ObCatalogAnalyzeSampleInfo::SampleMode &sample_mode)
{
  int ret = OB_SUCCESS;
  if (sample_type.empty() || 0 == sample_type.case_compare("ROW")) {
    sample_mode = common::ObCatalogAnalyzeSampleInfo::ROW;
  } else if (0 == sample_type.case_compare("BLOCK")) {
    sample_mode = common::ObCatalogAnalyzeSampleInfo::BLOCK;
  } else if (0 == sample_type.case_compare("FAST")) {
    sample_mode = common::ObCatalogAnalyzeSampleInfo::FAST;
  } else if (0 == sample_type.case_compare("FILE")) {
    sample_mode = common::ObCatalogAnalyzeSampleInfo::FILE;
  } else {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("illegal sample_type", K(ret), K(sample_type));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,
                   "Illegal value for sample_type: must be {ROW, BLOCK, FAST, FILE}");
  }
  return ret;
}

int ObDbmsCatalogStats::parse_method_opt_and_filter_columns(sql::ObExecContext &ctx,
                                                            ObCatalogTableStatParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.allocator_) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), KP(param.allocator_), KP(ctx.get_my_session()));
  } else if (param.gather_options_.method_opt_.empty() || 0 == param.gather_options_.method_opt_.case_compare("Z")) {
    param.gather_options_.method_opt_.assign_ptr("FOR ALL COLUMNS SIZE 1", 23);
  }

  // Use Parser to parse method_opt
  if (OB_FAIL(ret)) {
  } else {
    ObParser parser(*param.allocator_,
                    ctx.get_my_session()->get_sql_mode(),
                    ctx.get_my_session()->get_charsets4parser());
    ParseMode parse_mode = DYNAMIC_SQL_MODE;
    ParseResult parse_result;
    const ParseNode *for_stmt = NULL;
    if (OB_FAIL(parser.parse(param.gather_options_.method_opt_, parse_result, parse_mode))) {
      LOG_WARN("failed to parse result", K(ret), K(param.gather_options_.method_opt_));
    } else if (OB_ISNULL(parse_result.result_tree_) ||
               OB_ISNULL(for_stmt = parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("for stmt is invalid", K(ret), KP(for_stmt));
    } else if (OB_UNLIKELY(for_stmt->type_ != T_METHOD_OPT_LIST) ||
               OB_UNLIKELY(for_stmt->num_child_ < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse node is invalid", K(ret), K(for_stmt->type_), K(for_stmt->num_child_));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < for_stmt->num_child_; ++i) {
      ParseNode *child_node = for_stmt->children_[i];
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_FOR_ALL == child_node->type_) {
        if (OB_FAIL(parser_for_all_clause(child_node, param.column_params_))) {
          LOG_WARN("failed to parser for all clause", K(ret));
        }
      } else if (T_FOR_COLUMNS == child_node->type_) {
        if (OB_FAIL(parser_for_columns_clause(child_node, param.column_params_))) {
          LOG_WARN("failed to parser for columns clause", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected node type", K(ret), K(child_node->type_));
      }
    }
  }

  // Filter columns that do not need to be collected
  if (OB_FAIL(ret)) {
  } else {
    ObSEArray<ObCatalogColumnStatParam, 4> new_column_params;
    for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
      if (param.column_params_.at(i).need_col_stat()) {
        if (OB_FAIL(new_column_params.push_back(param.column_params_.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(param.column_params_.assign(new_column_params))) {
      LOG_WARN("failed to assign new column params", K(ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStats::parser_for_all_clause(const ParseNode *for_all_node,
                                              ObIArray<ObCatalogColumnStatParam> &column_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(for_all_node) || OB_UNLIKELY(for_all_node->type_ != T_FOR_ALL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KP(for_all_node));
  } else {
    ParseNode *first = NULL;
    bool is_for_all = false;
    if (OB_UNLIKELY(2 != for_all_node->num_child_) ||
        OB_ISNULL(first = for_all_node->children_[0]) ||
        OB_UNLIKELY(first->type_ != T_INT) ||
        OB_UNLIKELY(first->value_ < 0 || first->value_ > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid for all node", K(ret), KP(first), K(for_all_node->num_child_));
    } else if (first->value_ == 0) {
      is_for_all = true;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
      ObCatalogColumnStatParam &col_param = column_params.at(i);
      if (!is_for_all) {
        // do nothing
      } else if (!col_param.is_valid_opt_col() || col_param.is_text_column()) {
        // do nothing
      } else {
        col_param.set_need_basic_stat();
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStats::parser_for_columns_clause(const ParseNode *for_col_node,
                                                  ObIArray<ObCatalogColumnStatParam> &column_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(for_col_node)
      || OB_UNLIKELY(for_col_node->type_ != T_FOR_COLUMNS || for_col_node->num_child_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KP(for_col_node));
  } else {
    ObSEArray<ObString, 4> record_cols;
    for (int64_t i = 0; OB_SUCC(ret) && i < for_col_node->num_child_; ++i) {
      ParseNode *for_col_item = NULL;
      ObSEArray<ObString, 4> for_col_list;
      if (OB_ISNULL(for_col_item = for_col_node->children_[i])
          || OB_UNLIKELY(for_col_item->type_ != T_FOR_COLUMN_ITEM
                         || for_col_item->num_child_ != 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("for column item is null", K(ret), KP(for_col_item));
      } else if (NULL == for_col_item->children_[0]) {
        // size clause only, skip
      } else if (OB_UNLIKELY(T_EXTENSION == for_col_item->children_[0]->type_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("does not support gather stats for multi columns", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "gather stats for multi columns");
      } else {
        // Get column name - 对齐内部表 parse_for_columns
        ParseNode *col_node = for_col_item->children_[0];
        if (OB_ISNULL(col_node)
            || OB_UNLIKELY(col_node->type_ != T_COLUMN_REF || col_node->num_child_ != 3)
            || OB_ISNULL(col_node->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column node", K(ret), KP(col_node));
        } else if (OB_FAIL(check_is_valid_col(col_node->children_[1]->str_value_,
                                              column_params, record_cols))) {
          LOG_WARN("failed to check is valid col", K(ret));
        } else if (OB_FAIL(for_col_list.push_back(col_node->children_[1]->str_value_))) {
          LOG_WARN("failed to push back column", K(ret));
        } else if (OB_FAIL(record_cols.push_back(col_node->children_[1]->str_value_))) {
          LOG_WARN("failed to push back record col", K(ret));
        }
      }
      // Match columns and set - 对齐内部表
      for (int64_t j = 0; OB_SUCC(ret) && j < column_params.count(); ++j) {
        ObCatalogColumnStatParam &col_param = column_params.at(j);
        if (!is_match_column_option(col_param, for_col_list)) {
          // do nothing
        } else if (!col_param.is_valid_opt_col()) {
          // do nothing
        } else {
          col_param.set_need_basic_stat();
          // 不设置 set_need_avg_len，对齐内部表 compute_bucket_num
        }
      }
    }
  }
  return ret;
}

bool ObDbmsCatalogStats::is_match_column_option(const ObCatalogColumnStatParam &param,
                                                const ObIArray<ObString> &for_col_list)
{
  bool is_match = false;
  for (int64_t i = 0; !is_match && i < for_col_list.count(); ++i) {
    if (0 == for_col_list.at(i).case_compare(param.column_name_)) {
      is_match = true;
    }
  }
  return is_match;
}

int ObDbmsCatalogStats::check_is_valid_col(const ObString &src_str,
                                           const ObIArray<ObCatalogColumnStatParam> &column_params,
                                           const ObIArray<ObString> &record_cols)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  //check col in table
  for (int64_t i = 0; !is_valid && i < column_params.count(); ++i) {
    if (0 == src_str.case_compare(column_params.at(i).column_name_)) {
      is_valid = true;
    }
  }
  if (is_valid) {
    //check duplicate => for columns c1 c1
    for (int64_t i = 0; is_valid && i < record_cols.count(); ++i) {
      if (0 == src_str.case_compare(record_cols.at(i))) {
        is_valid = false;
      }
    }
    if (!is_valid) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_WARN("column duplicated", K(src_str), K(ret));
      LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, src_str.length(), src_str.ptr());
    }
  } else {
    ret = OB_WRONG_COLUMN_NAME;
    LOG_WARN("column schema is null", K(ret), K(src_str));
    LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int32_t>(src_str.length()), src_str.ptr());
  }
  return ret;
}

int ObDbmsCatalogStats::parse_catalog_table_stats_params(sql::ParamStore &params,
                                                         sql::ObExecContext &ctx,
                                                         ObCatalogTableStatParam &stat_param)
{
  int ret = OB_SUCCESS;

  stat_param.gather_options_.stattype_ = StatTypeLocked::NULL_TYPE;

  // Parse estimate_percent and sample_type (params 4-5, optional)
  ObObjParam empty_param;
  empty_param.set_null();
  ObObjParam *estimate_percent = params.count() > 4 ? &params.at(4) : &empty_param;
  ObObjParam *sample_type = params.count() > 5 ? &params.at(5) : &empty_param;

  // Parse gather stat options (params 6-9)
  ObObjParam *method_opt = params.count() > 6 ? &params.at(6) : &empty_param;
  ObObjParam *degree = params.count() > 7 ? &params.at(7) : &empty_param;
  ObObjParam *granularity = params.count() > 8 ? &params.at(8) : &empty_param;
  ObObjParam *force = params.count() > 9 ? &params.at(9) : &empty_param;

  // Parse estimate_percent parameter (align with internal table implementation)
  number::ObNumber num_est_percent;
  double percent = 0.0;
  if (OB_ISNULL(stat_param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stat_param.allocator_));
  } else if (estimate_percent->is_null()) {
    stat_param.gather_options_.sample_info_.set_percent(10.0);
  } else if (OB_FAIL(estimate_percent->get_number(num_est_percent))) {
    LOG_WARN("failed to get number", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(num_est_percent, percent))) {
    LOG_WARN("failed to cast number to double", K(ret));
  } else if (percent == 0.0) {
    // percent == 0.0 means use default sample size, align with internal table.
    stat_param.gather_options_.sample_info_.set_percent(10.0);
  } else if (OB_UNLIKELY(percent < 0.000001 || percent > 100.0)) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal sample percent: must be in the range[0.000001,100]", K(ret), K(percent));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,
                   "Illegal sample percent: must be in the range[0.000001,100]");
  } else {
    stat_param.gather_options_.sample_info_.set_percent(percent);
  }

  // Parse sample_type parameter
  if (OB_FAIL(ret)) {
  } else {
    ObString sample_type_str;
    common::ObCatalogAnalyzeSampleInfo::SampleMode sample_mode
        = common::ObCatalogAnalyzeSampleInfo::ROW;
    bool is_sample_type_specified = false;
    if (sample_type->is_null()) {
      // Keep default mode for now, resolve by file format later.
    } else if (OB_FAIL(sample_type->get_varchar(sample_type_str))) {
      LOG_WARN("failed to get sample_type", K(ret));
    } else if (sample_type_str.empty()) {
      // Treat empty string same as NULL — resolve by file format later.
    } else if (OB_FAIL(parse_catalog_sample_mode(sample_type_str, sample_mode))) {
      LOG_WARN("failed to parse sample mode", K(ret), K(sample_type_str));
    } else {
      is_sample_type_specified = true;
    }

    if (OB_FAIL(ret)) {
    } else {
      const sql::ObExternalFileFormat::FormatType format_type = stat_param.external_info_.format_type_;
      const bool is_csv = (sql::ObExternalFileFormat::CSV_FORMAT == format_type);
      const bool is_parquet_or_orc = (sql::ObExternalFileFormat::PARQUET_FORMAT == format_type
                                      || sql::ObExternalFileFormat::ORC_FORMAT == format_type);
      if (!is_sample_type_specified) {
        sample_mode = is_parquet_or_orc
            ? common::ObCatalogAnalyzeSampleInfo::FAST
            : common::ObCatalogAnalyzeSampleInfo::ROW;
      }
      stat_param.gather_options_.sample_info_.set_sample_mode(sample_mode);
      LOG_TRACE("resolved catalog sample mode",
                K(sample_mode),
                K(is_sample_type_specified),
                K(format_type),
                K(stat_param.gather_options_.sample_info_.percent_),
                K(stat_param.gather_options_.sample_info_.seed_));
      if (is_csv) {
        if (stat_param.gather_options_.sample_info_.is_block_family_sample()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("csv supports row sample only", K(ret), K(sample_mode), K(format_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sample type BLOCK/FAST for csv format");
        }
      } else if (is_parquet_or_orc) {
        if (stat_param.gather_options_.sample_info_.is_row_sample()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("parquet/orc does not support row sample", K(ret), K(sample_mode), K(format_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sample type ROW for parquet/orc format");
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_catalog_gather_stat_options(*method_opt,
                                                       *degree,
                                                       *granularity,
                                                       *force,
                                                       ctx,
                                                       stat_param))) {
    LOG_WARN("failed to parse gather stat options", K(ret));
  } else if (stat_param.column_params_.empty()) {
    LOG_TRACE("no columns specified in method_opt, will need to get columns from catalog table "
              "schema",
              K(stat_param.table_identity_.catalog_name_),
              K(stat_param.table_identity_.db_name_),
              K(stat_param.table_identity_.tab_name_));
  } else {
    LOG_INFO("parsed catalog table stats params",
             K(stat_param),
             K(stat_param.column_params_.count()));
  }
  return ret;
}

int ObDbmsCatalogStats::lock_catalog_table_stat(sql::ObExecContext &ctx,
                                                sql::ParamStore &params,
                                                common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObCatalogTableStatParam stat_param;
  stat_param.allocator_ = &ctx.get_allocator();
  ObString stat_type_str;
  ObObjParam empty_part;
  empty_part.set_null();

  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (params.count() < 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count", K(ret), K(params.count()));
  } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                           params.at(1),
                                           params.at(2),
                                           params.count() > 3 ? params.at(3) : empty_part,
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  }

  if (OB_SUCC(ret) && params.count() > 4 && !params.at(4).is_null()) {
    if (OB_FAIL(params.at(4).get_varchar(stat_type_str))) {
      LOG_WARN("failed to get stattype", K(ret));
    } else {
      if (0 == stat_type_str.case_compare("ALL") || stat_type_str.empty()) {
        stat_param.gather_options_.stattype_ = StatTypeLocked::TABLE_ALL_TYPE;
      } else if (0 == stat_type_str.case_compare("DATA")) {
        stat_param.gather_options_.stattype_ = StatTypeLocked::DATA_TYPE;
      } else if (0 == stat_type_str.case_compare("CACHE")) {
        stat_param.gather_options_.stattype_ = StatTypeLocked::CACHE_TYPE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid stat type", K(ret), K(stat_type_str));
      }
    }
  } else {
    stat_param.gather_options_.stattype_ = StatTypeLocked::TABLE_ALL_TYPE;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDbmsCatalogStatsLockUnlock::set_catalog_table_stats_lock(stat_param,
                                                                                ctx,
                                                                                true))) {
    LOG_WARN("failed to lock catalog table stats", K(ret));
  }

  return ret;
}

int ObDbmsCatalogStats::unlock_catalog_table_stat(sql::ObExecContext &ctx,
                                                  sql::ParamStore &params,
                                                  common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObCatalogTableStatParam stat_param;
  stat_param.allocator_ = &ctx.get_allocator();
  ObString stat_type_str;
  ObObjParam empty_part;
  empty_part.set_null();

  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (params.count() < 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count", K(ret), K(params.count()));
  } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                           params.at(1),
                                           params.at(2),
                                           params.count() > 3 ? params.at(3) : empty_part,
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  }

  if (OB_SUCC(ret) && params.count() > 4 && !params.at(4).is_null()) {
    if (OB_FAIL(params.at(4).get_varchar(stat_type_str))) {
      LOG_WARN("failed to get stattype", K(ret));
    } else {
      if (0 == stat_type_str.case_compare("ALL") || stat_type_str.empty()) {
        stat_param.gather_options_.stattype_ = StatTypeLocked::TABLE_ALL_TYPE;
      } else if (0 == stat_type_str.case_compare("DATA")) {
        stat_param.gather_options_.stattype_ = StatTypeLocked::DATA_TYPE;
      } else if (0 == stat_type_str.case_compare("CACHE")) {
        stat_param.gather_options_.stattype_ = StatTypeLocked::CACHE_TYPE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid stat type", K(ret), K(stat_type_str));
      }
    }
  } else {
    stat_param.gather_options_.stattype_ = StatTypeLocked::TABLE_ALL_TYPE;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDbmsCatalogStatsLockUnlock::set_catalog_table_stats_lock(stat_param,
                                                                                ctx,
                                                                                false))) {
    LOG_WARN("failed to unlock catalog table stats", K(ret));
  }

  return ret;
}

int ObDbmsCatalogStats::lock_catalog_partition_stat(sql::ObExecContext &ctx,
                                                    sql::ParamStore &params,
                                                    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObCatalogTableStatParam stat_param;
  stat_param.allocator_ = &ctx.get_allocator();
  stat_param.gather_options_.stattype_ = StatTypeLocked::PARTITION_ALL_TYPE;

  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (params.count() < 4 || params.at(3).is_null()) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("partition not specified", K(ret));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "partition not specified");
  } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                           params.at(1),
                                           params.at(2),
                                           params.at(3),
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDbmsCatalogStatsLockUnlock::set_catalog_table_stats_lock(stat_param,
                                                                                ctx,
                                                                                true))) {
    LOG_WARN("failed to lock catalog partition stats", K(ret));
  }

  return ret;
}

int ObDbmsCatalogStats::unlock_catalog_partition_stat(sql::ObExecContext &ctx,
                                                      sql::ParamStore &params,
                                                      common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  ObCatalogTableStatParam stat_param;
  stat_param.allocator_ = &ctx.get_allocator();
  stat_param.gather_options_.stattype_ = StatTypeLocked::PARTITION_ALL_TYPE;

  if (OB_FAIL(check_statistic_table_writeable(ctx))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::implicit_commit_before_gather_stats(ctx))) {
    LOG_WARN("failed to implicit commit before gather stats", K(ret));
  } else if (params.count() < 4 || params.at(3).is_null()) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("partition not specified", K(ret));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "partition not specified");
  } else if (OB_FAIL(parse_table_part_info(params.at(0),
                                           params.at(1),
                                           params.at(2),
                                           params.at(3),
                                           ctx,
                                           stat_param))) {
    LOG_WARN("failed to parse table part info", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDbmsCatalogStatsLockUnlock::set_catalog_table_stats_lock(stat_param,
                                                                                ctx,
                                                                                false))) {
    LOG_WARN("failed to unlock catalog partition stats", K(ret));
  }

  return ret;
}

int ObDbmsCatalogStats::update_catalog_stat_cache(
    const uint64_t rpc_tenant_id,
    const ObCatalogTableStatParam &stat_param,
    ObOptStatRunningMonitor *running_monitor /*default null*/)
{
  int ret = OB_SUCCESS;
  obrpc::ObUpdateCatalogStatCacheArg stat_arg;
  stat_arg.tenant_id_ = stat_param.table_identity_.tenant_id_;
  stat_arg.catalog_id_ = stat_param.table_identity_.catalog_id_;
  stat_arg.db_name_ = stat_param.table_identity_.db_name_;
  stat_arg.table_name_ = stat_param.table_identity_.tab_name_;

  // 填充 partition_values_
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.part_infos_.count(); ++i) {
    if (OB_FAIL(stat_arg.partition_values_.push_back(stat_param.part_infos_.at(i).partition_))) {
      LOG_WARN("failed to push back partition value", K(ret));
    }
  }
  // 填充 column_names_
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.column_params_.count(); ++i) {
    if (OB_FAIL(stat_arg.column_names_.push_back(stat_param.column_params_.at(i).column_name_))) {
      LOG_WARN("failed to push back column name", K(ret));
    }
  }

  // 分布式广播
  int64_t timeout = -1;
  ObSEArray<ObServerLocality, 4> all_server_arr;
  ObSEArray<ObServerLocality, 4> failed_server_arr;
  bool has_read_only_zone = false;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.locality_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy or locality_manager is null", K(ret));
  } else if (OB_FAIL(GCTX.locality_manager_->get_server_locality_array(all_server_arr,
                                                                       has_read_only_zone))) {
    LOG_WARN("fail to get server locality", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_server_arr.count(); ++i) {
      timeout = std::min(MAX_OPT_STATS_PROCESS_RPC_TIMEOUT, THIS_WORKER.get_timeout_remain());
      if (!all_server_arr.at(i).is_active()
          || ObServerStatus::OB_SERVER_ACTIVE != all_server_arr.at(i).get_server_status()
          || 0 == all_server_arr.at(i).get_start_service_time()
          || 0 != all_server_arr.at(i).get_server_stop_time()) {
        // server may not serving
      } else if (0 >= timeout) {
        ret = OB_TIMEOUT;
        LOG_WARN("query timeout is reached", K(ret), K(timeout));
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(all_server_arr.at(i).get_addr())
                             .timeout(timeout)
                             .by(rpc_tenant_id)
                             .update_local_catalog_stat_cache(stat_arg))) {
        LOG_WARN("failed to update local catalog stat cache",
                 K(ret),
                 K(all_server_arr.at(i).get_addr()),
                 K(stat_arg));
        if (OB_FAIL(failed_server_arr.push_back(all_server_arr.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    LOG_TRACE("update catalog stat cache", K(stat_arg), K(failed_server_arr), K(all_server_arr));
    // 处理失败的服务器列表 - 对齐内部表实现
    if (OB_SUCC(ret) && !failed_server_arr.empty() && OB_NOT_NULL(running_monitor)) {
      ObSqlString tmp_str;
      char *buf = NULL;
      if (failed_server_arr.count() * (common::MAX_IP_PORT_LENGTH + 1)
          <= common::MAX_VALUE_LENGTH) {
        for (int64_t i = 0; OB_SUCC(ret) && i < failed_server_arr.count(); ++i) {
          char svr_buf[common::MAX_IP_PORT_LENGTH] = {0};
          failed_server_arr.at(i).get_addr().to_string(svr_buf, common::MAX_IP_PORT_LENGTH);
          if (OB_FAIL(tmp_str.append_fmt("%s%s", svr_buf, i == 0 ? "" : ","))) {
            LOG_WARN("failed to append fmt", K(ret));
          }
        }
      } else if (OB_FAIL(tmp_str.append_fmt("more than %ld servers refresh stat cache failed",
                                            failed_server_arr.count()))) {
        LOG_WARN("failed to append fmt", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else {
        if (OB_ISNULL(buf
                      = static_cast<char *>(running_monitor->allocator_.alloc(tmp_str.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory is not enough", K(ret), K(tmp_str));
        } else {
          MEMCPY(buf, tmp_str.ptr(), tmp_str.length());
          ObString tmp_failed_list(tmp_str.length(), buf);
          ObOptStatGatherStatList::instance().update_gather_stat_refresh_failed_list(
              tmp_failed_list,
              running_monitor->opt_stat_gather_stat_);
        }
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStats::check_statistic_table_writeable(sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  bool is_primary = true;
  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KR(ret), KP(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog table statistics before cluster version 4.6.1.0");
    LOG_WARN("catalog table statistics not supported before 4.6.1.0", KR(ret));
  } else if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
    LOG_WARN("fail to execute mtl_check_if_tenant_role_is_primary", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_primary)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use dbms_stats during non-primary tenant");
  }
  return ret;
}

int ObDbmsCatalogStats::init_gather_task_info(sql::ObExecContext &ctx,
                                              ObOptStatGatherType type,
                                              int64_t start_time,
                                              int64_t task_table_count,
                                              ObOptStatTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  ObString task_id;
  char *server_uuid = NULL;
  int64_t length_uuid = 36;
  if (OB_ISNULL(server_uuid = static_cast<char *>(ctx.get_allocator().alloc(length_uuid)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(length_uuid));
  } else if (OB_FAIL(ObExprUuid::gen_server_uuid(server_uuid, length_uuid))) {
    LOG_WARN("failed to gen server uuid", K(ret));
  } else {
    task_id.assign_ptr(server_uuid, length_uuid);
    if (OB_ISNULL(ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(ctx.get_my_session()));
    } else if (OB_FAIL(task_info.init(ctx.get_allocator(),
                                      ctx.get_my_session(),
                                      task_id,
                                      type,
                                      start_time,
                                      task_table_count))) {
      LOG_WARN("failed to init", K(ret));
    } else {
      LOG_TRACE("Succeed to init gather task info", K(task_info));
    }
  }
  return ret;
}

int ObDbmsCatalogStats::get_stats_consumer_group_id(ObCatalogTableStatParam &param)
{
  int ret = OB_SUCCESS;
  uint64_t consumer_group_id = 0;
  if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_function_type(
          param.table_identity_.tenant_id_,
          ObFunctionType::PRIO_OPT_STATS,
          consumer_group_id))) {
    LOG_WARN("fail to get group id by function", K(param.table_identity_.tenant_id_));
  } else if (is_resource_manager_group(consumer_group_id)) {
    param.gather_options_.consumer_group_id_ = consumer_group_id;
  }
  return ret;
}

void ObDbmsCatalogStats::update_optimizer_gather_stat_info(
    const ObOptStatTaskInfo *task_info,
    const ObOptStatGatherStat *gather_stat,
    const ObCatalogTableStatParam &stat_param)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *origin_session = THIS_WORKER.get_session();
  int64_t origin_timeout = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_session(NULL);
  const int64_t MAX_UPDATE_OPT_GATHER_STAT_TIMEOUT = 10000000; // default 10 seconds
  THIS_WORKER.set_timeout_ts(MAX_UPDATE_OPT_GATHER_STAT_TIMEOUT + ObTimeUtility::current_time());

  // task 级别：复用
  if (task_info != NULL && task_info->task_table_count_ > 0) {
    if (OB_FAIL(ObOptStatManager::get_instance().update_opt_stat_task_stat(*task_info))) {
      LOG_WARN("failed to update opt stat task stat", K(ret));
      LOG_USER_WARN(OB_ERR_DBMS_STATS_PL, "failed to update opt stat task stat");
    }
  }

  // table 级别：通过 manager 调用 catalog service
  if (gather_stat != NULL) {
    if (OB_FAIL(ObOptStatManager::get_instance().update_catalog_opt_stat_gather_stat(
            *gather_stat,
            stat_param.table_identity_.catalog_id_,
            stat_param.table_identity_.db_name_,
            stat_param.table_identity_.tab_name_))) {
      LOG_WARN("failed to update catalog opt stat gather stat", K(ret));
      LOG_USER_WARN(OB_ERR_DBMS_STATS_PL, "failed to update catalog opt stat gather stat");
    }
  }
  THIS_WORKER.set_session(origin_session);
  THIS_WORKER.set_timeout_ts(origin_timeout);
}

int ObDbmsCatalogStats::split_part_func_exprs_(ObIAllocator &allocator,
                                               const ObString &part_func_expr,
                                               ObIArray<ObString> &part_exprs)
{
  int ret = OB_SUCCESS;
  part_exprs.reset();
  if (part_func_expr.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part func expr is empty", K(ret));
  } else {
    const char *ptr = part_func_expr.ptr();
    const int64_t len = part_func_expr.length();
    int64_t start = 0;
    int64_t paren_depth = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i <= len; ++i) {
      if (i < len && ptr[i] == '(') {
        ++paren_depth;
      } else if (i < len && ptr[i] == ')') {
        --paren_depth;
      } else if ((i == len || (ptr[i] == ',' && 0 == paren_depth))) {
        int64_t expr_start = start;
        int64_t expr_end = i;
        while (expr_start < expr_end && ptr[expr_start] == ' ') {
          ++expr_start;
        }
        while (expr_end > expr_start && ptr[expr_end - 1] == ' ') {
          --expr_end;
        }
        if (expr_start < expr_end) {
          ObString expr(expr_end - expr_start, ptr + expr_start);
          ObString copy_expr;
          if (OB_FAIL(ob_write_string(allocator, expr, copy_expr))) {
            LOG_WARN("failed to write string", K(ret), K(expr));
          } else if (OB_FAIL(part_exprs.push_back(copy_expr))) {
            LOG_WARN("failed to push back", K(ret), K(copy_expr));
          }
        }
        start = i + 1;
      }
    }
  }
  return ret;
}

int ObDbmsCatalogStats::resolve_catalog_granularity(ObCatalogTableStatParam &param)
{
  int ret = OB_SUCCESS;
  const bool is_partitioned
      = param.part_level_ != share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO;
  const bool is_specify_sample = param.gather_options_.sample_info_.is_specify_sample();
  const bool has_part_name = !param.part_name_.empty();

  // Note: External/Catalog tables only have one partition level (no subpartition)
  // External table's part corresponds to internal table's finest granularity partition
  switch (param.gather_options_.granularity_type_) {
    case GRANULARITY_AUTO:
      if (has_part_name) {
        param.global_stat_param_.reset_gather_stat();
        param.part_stat_param_.set_gather_stat(false);
      } else {
        param.global_stat_param_.set_gather_stat(true);
        param.part_stat_param_.set_gather_stat(false);
      }
      break;
    case GRANULARITY_ALL:
      param.global_stat_param_.set_gather_stat(false);
      param.part_stat_param_.set_gather_stat(false);
      break;
    case GRANULARITY_GLOBAL_AND_PARTITION:
      param.global_stat_param_.set_gather_stat(false);
      param.part_stat_param_.set_gather_stat(false);
      break;
    case GRANULARITY_APPROX_GLOBAL_AND_PARTITION:
      param.global_stat_param_.set_gather_stat(is_partitioned && !is_specify_sample);
      param.part_stat_param_.set_gather_stat(false);
      break;
    case GRANULARITY_GLOBAL:
      if (is_partitioned) {
        // Derive global stats from partition stats (aligned with internal table)
        param.global_stat_param_.set_gather_stat(true);
        param.part_stat_param_.set_gather_stat(false);
        param.part_stat_param_.gather_histogram_ = false;
      } else {
        param.global_stat_param_.set_gather_stat(false);
        param.part_stat_param_.reset_gather_stat();
      }
      break;
    case GRANULARITY_PARTITION:
    case GRANULARITY_SUBPARTITION:
      // External tables have no subpartition, treat SUBPARTITION same as PARTITION
      if (!has_part_name && is_partitioned && !is_specify_sample) {
        param.global_stat_param_.set_gather_stat(true);
      } else {
        param.global_stat_param_.reset_gather_stat();
      }
      param.part_stat_param_.set_gather_stat(false);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected granularity type", K(ret), K(param.gather_options_.granularity_type_));
      break;
  }

  if (OB_FAIL(ret)) {
  } else {
    // TODO(bitao): support gather historgram.
    param.global_stat_param_.gather_histogram_ = false;
    param.part_stat_param_.gather_histogram_ = false;
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
