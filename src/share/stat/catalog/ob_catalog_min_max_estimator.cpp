/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_catalog_min_max_estimator.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace common
{

// ======================== ObCatalogStatMinMaxAgg ========================

int ObCatalogStatMinMaxAgg::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     lib::is_oracle_mode()
                                         ? (is_min_ ? " sys_ext_min(\"%.*s\")"
                                                    : " sys_ext_max(\"%.*s\")")
                                         : (is_min_ ? " sys_ext_min(`%.*s`)"
                                                    : " sys_ext_max(`%.*s`)"),
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print sys_ext_min/max expr", K(ret), K(is_min_));
  }
  return ret;
}

int ObCatalogStatMinMaxAgg::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col stat is not given", K(ret), K(col_stat_));
  } else if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(obj, allocator))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else if (is_min_) {
    col_stat_->set_min_value(obj);
  } else {
    col_stat_->set_max_value(obj);
  }
  return ret;
}

// ======================== ObCatalogMinMaxEstimator ========================

ObCatalogMinMaxEstimator::ObCatalogMinMaxEstimator(
    sql::ObExecContext &ctx, ObIAllocator &allocator)
    : ObCatalogStatsEstimator(ctx, allocator)
{
}

// ---- shared ----

int ObCatalogMinMaxEstimator::add_min_max_stat_items(
    ObIAllocator &allocator,
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    ObOptCatalogStat &opt_catalog_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(opt_catalog_stat.table_stat_) ||
      OB_UNLIKELY(opt_catalog_stat.column_stats_.count() != column_params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(opt_catalog_stat),
             K(column_params.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
      const ObCatalogColumnStatParam *col_param = &column_params.at(i);
      if (OB_ISNULL(opt_catalog_stat.column_stats_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (!col_param->need_refine_min_max()) {
        LOG_TRACE("column need not refine min/max", K(col_param->column_name_));
      } else {
        void *p = NULL;
        ObCatalogStatMinMaxAgg *min_agg = NULL;
        ObCatalogStatMinMaxAgg *max_agg = NULL;
        if (OB_ISNULL(p = allocator.alloc(sizeof(ObCatalogStatMinMaxAgg)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (OB_FALSE_IT(min_agg = new (p) ObCatalogStatMinMaxAgg(
                       col_param, opt_catalog_stat.column_stats_.at(i), true))) {
        } else if (OB_FAIL(stat_items_.push_back(min_agg))) {
          LOG_WARN("failed to push back array", K(ret));
        } else if (OB_ISNULL(p = allocator.alloc(sizeof(ObCatalogStatMinMaxAgg)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (OB_FALSE_IT(max_agg = new (p) ObCatalogStatMinMaxAgg(
                       col_param, opt_catalog_stat.column_stats_.at(i), false))) {
        } else if (OB_FAIL(stat_items_.push_back(max_agg))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCatalogMinMaxEstimator::pack_sql(ObSqlString &raw_sql_str)
{
  int ret = OB_SUCCESS;
  if (catalog_name_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog_name is empty for catalog table",
             K(ret), K(db_name_), K(from_table_));
  } else if (OB_FAIL(raw_sql_str.append_fmt(
                 lib::is_oracle_mode()
                     ? "SELECT %.*s %.*s FROM \"%.*s\".\"%.*s\".\"%.*s\""
                       " %.*s %.*s"
                     : "SELECT %.*s %.*s FROM `%.*s`.`%.*s`.`%.*s`"
                       " %.*s %.*s",
                 other_hints_.length(), other_hints_.ptr(),
                 static_cast<int32_t>(select_fields_.length()),
                 select_fields_.ptr(),
                 catalog_name_.length(), catalog_name_.ptr(),
                 db_name_.length(), db_name_.ptr(),
                 from_table_.length(), from_table_.ptr(),
                 where_string_.length(), where_string_.ptr(),
                 group_by_string_.length(), group_by_string_.ptr()))) {
    LOG_WARN("failed to build sql", K(ret));
  } else {
    LOG_TRACE("catalog min max stat query sql", K(raw_sql_str));
  }
  return ret;
}

// ---- entry point ----

int ObCatalogMinMaxEstimator::estimate(
    const ObOptCatalogStatGatherParam &param,
    ObIArray<ObOptCatalogStat> &opt_catalog_stats)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("CatMinMaxEst", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             param.table_identity_.tenant_id_);

  if (OB_FAIL(init_escape_char_names(allocator, param))) {
    LOG_WARN("failed to init escape char names", K(ret));
  } else if (param.stat_level_ == TABLE_LEVEL) {
    if (OB_UNLIKELY(opt_catalog_stats.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table level expects exactly one stat",
               K(ret), K(opt_catalog_stats.count()));
    } else if (OB_FAIL(estimate_table_level(allocator, param,
                                            opt_catalog_stats.at(0)))) {
      LOG_WARN("failed to estimate table level min/max", K(ret));
    }
  } else if (param.stat_level_ == PARTITION_LEVEL) {
    if (OB_FAIL(estimate_partition_level(allocator, param,
                                         opt_catalog_stats))) {
      LOG_WARN("failed to estimate partition level min/max", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stat level", K(ret), K(param.stat_level_));
  }
  return ret;
}

// ======================== TABLE_LEVEL ========================

int ObCatalogMinMaxEstimator::estimate_table_level(
    ObIAllocator &allocator,
    const ObOptCatalogStatGatherParam &param,
    ObOptCatalogStat &opt_catalog_stat)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  ObSEArray<ObOptCatalogStat, 1> tmp_opt_stats;

  set_from_table(tab_name_);
  if (OB_FAIL(add_min_max_stat_items(allocator, param.column_params_,
                                     opt_catalog_stat))) {
    LOG_WARN("failed to add min max stat items", K(ret));
  } else if (get_item_size() <= 0) {
    LOG_TRACE("no columns need refine min/max");
  } else if (OB_FAIL(gen_select_filed())) {
    LOG_WARN("failed to generate select field", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to fill parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(
                 param.gather_start_time_, param.max_duration_time_,
                 duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(pack_sql(raw_sql))) {
    LOG_WARN("failed to pack raw sql", K(ret));
  } else if (OB_FAIL(tmp_opt_stats.push_back(opt_catalog_stat))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(do_estimate(param, raw_sql.string(), false,
                                 opt_catalog_stat, tmp_opt_stats))) {
    LOG_WARN("failed to evaluate min/max stats", K(ret));
  }
  stat_items_.reuse();
  return ret;
}

// ======================== PARTITION_LEVEL (batch) ========================
//
// Batch flow:
//   1. collect_eligible_partitions  – filter partitions with row_count > MAGIC_SAMPLE_SIZE
//   2. collect_refine_columns       – filter columns that need refine min/max
//   3. add_min_max_stat_items       – reuse ObCatalogStatMinMaxAgg for SQL generation
//   4. gen_select_filed             – build sys_ext_min/max SELECT fields from stat_items
//   5. prepend_partition_key_concat – add CONCAT(...) as first SELECT column
//   6. fill_batch_hints             – NO_REWRITE, DBMS_STATS, PARALLEL, INLIST_REWRITE, TIMEOUT
//   7. fill_batch_where_clause      – WHERE (part_cols) IN (eligible partition values)
//   8. fill_batch_group_by          – GROUP BY part_cols
//   9. pack_sql                     – assemble final SQL (shared with TABLE_LEVEL)
//  10. execute_and_route_results    – multi-row result routing by partition key hash map

int ObCatalogMinMaxEstimator::estimate_partition_level(
    ObIAllocator &allocator,
    const ObOptCatalogStatGatherParam &param,
    ObIArray<ObOptCatalogStat> &opt_catalog_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 32> eligible_indices;
  ObSEArray<int64_t, 16> refine_col_indices;
  ObSqlString raw_sql;

  if (OB_FAIL(collect_eligible_partitions(opt_catalog_stats,
                                          eligible_indices))) {
    LOG_WARN("failed to collect eligible partitions", K(ret));
  } else if (OB_FAIL(collect_refine_columns(param.column_params_,
                                            refine_col_indices))) {
    LOG_WARN("failed to collect refine columns", K(ret));
  } else if (eligible_indices.empty() || refine_col_indices.empty()) {
    LOG_TRACE("no partitions or columns need refine min/max",
              K(eligible_indices.count()), K(refine_col_indices.count()));
  } else if (OB_FAIL(add_min_max_stat_items(
                 allocator, param.column_params_,
                 opt_catalog_stats.at(eligible_indices.at(0))))) {
    LOG_WARN("failed to add min max stat items", K(ret));
  } else if (get_item_size() <= 0) {
    LOG_TRACE("no columns need refine min/max");
  } else if (OB_FAIL(gen_select_filed())) {
    LOG_WARN("failed to generate select field", K(ret));
  } else if (OB_FAIL(prepend_partition_key_concat(param.part_cols_))) {
    LOG_WARN("failed to prepend partition key concat", K(ret));
  } else if (OB_FAIL(fill_batch_hints(allocator, param,
                                      eligible_indices.count()))) {
    LOG_WARN("failed to fill batch hints", K(ret));
  } else if (OB_FAIL(fill_batch_where_clause(allocator, param,
                                             eligible_indices))) {
    LOG_WARN("failed to fill batch where clause", K(ret));
  } else if (OB_FAIL(fill_batch_group_by(allocator, param.part_cols_))) {
    LOG_WARN("failed to fill batch group by", K(ret));
  } else if (OB_FALSE_IT(set_from_table(tab_name_))) {
  } else if (OB_FAIL(pack_sql(raw_sql))) {
    LOG_WARN("failed to pack batch sql", K(ret));
  } else if (OB_FAIL(execute_and_route_results(param, raw_sql.string(),
                                               refine_col_indices,
                                               opt_catalog_stats))) {
    LOG_WARN("failed to execute and route results", K(ret));
  }
  stat_items_.reuse();
  return ret;
}

int ObCatalogMinMaxEstimator::collect_eligible_partitions(
    const ObIArray<ObOptCatalogStat> &opt_catalog_stats,
    ObIArray<int64_t> &eligible_indices)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < opt_catalog_stats.count(); ++i) {
    if (OB_ISNULL(opt_catalog_stats.at(i).table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table stat", K(ret), K(i));
    } else if (opt_catalog_stats.at(i).table_stat_->get_row_count() <= 0) {
      // empty partition, skip
    } else if (opt_catalog_stats.at(i).table_stat_->get_row_count()
               <= MAGIC_SAMPLE_SIZE) {
      // small partition, basic estimator already did full scan, skip
    } else if (OB_FAIL(eligible_indices.push_back(i))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObCatalogMinMaxEstimator::collect_refine_columns(
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    ObIArray<int64_t> &refine_col_indices)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    if (column_params.at(i).need_refine_min_max()) {
      if (OB_FAIL(refine_col_indices.push_back(i))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObCatalogMinMaxEstimator::prepend_partition_key_concat(
    const ObIArray<ObString> &part_cols)
{
  int ret = OB_SUCCESS;
  const bool is_oracle = lib::is_oracle_mode();
  const char *lq = is_oracle ? "\"" : "`";
  const char *rq = is_oracle ? "\"" : "`";
  ObSqlString concat_expr;

  if (OB_UNLIKELY(part_cols.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition columns is empty", K(ret));
  } else if (OB_FAIL(concat_expr.append("CONCAT("))) {
    LOG_WARN("failed to append", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_cols.count(); ++i) {
    if (i > 0 && OB_FAIL(concat_expr.append(", '/', "))) {
      LOG_WARN("failed to append separator", K(ret));
    } else if (OB_FAIL(concat_expr.append_fmt(
                   "'%.*s=', %s%.*s%s",
                   part_cols.at(i).length(), part_cols.at(i).ptr(),
                   lq,
                   part_cols.at(i).length(), part_cols.at(i).ptr(),
                   rq))) {
      LOG_WARN("failed to append partition key expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(concat_expr.append(")"))) {
    LOG_WARN("failed to append", K(ret));
  }

  // Prepend CONCAT(...) before existing select_fields_
  if (OB_SUCC(ret)) {
    ObSqlString new_fields;
    if (OB_FAIL(new_fields.append(concat_expr.string()))) {
      LOG_WARN("failed to append concat", K(ret));
    } else if (select_fields_.length() > 0
               && OB_FAIL(new_fields.append(","))) {
      LOG_WARN("failed to append comma", K(ret));
    } else if (select_fields_.length() > 0
               && OB_FAIL(new_fields.append(select_fields_.string()))) {
      LOG_WARN("failed to append select fields", K(ret));
    } else {
      select_fields_.reset();
      if (OB_FAIL(select_fields_.append(new_fields.string()))) {
        LOG_WARN("failed to replace select fields", K(ret));
      }
    }
  }
  return ret;
}

int ObCatalogMinMaxEstimator::fill_batch_hints(
    ObIAllocator &allocator,
    const ObOptCatalogStatGatherParam &param,
    int64_t eligible_part_cnt)
{
  int ret = OB_SUCCESS;
  int64_t duration_time = -1;

  if (OB_FAIL(add_hint(ObString::make_string("NO_REWRITE DBMS_STATS"),
                        allocator))) {
    LOG_WARN("failed to add hint", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to fill parallel info", K(ret));
  }
  if (OB_SUCC(ret) && eligible_part_cnt > 1) {
    ObSqlString inlist_hint;
    if (OB_FAIL(inlist_hint.append_fmt(
            "OPT_PARAM('INLIST_REWRITE_THRESHOLD', %ld)",
            eligible_part_cnt + 1))) {
      LOG_WARN("failed to build inlist hint", K(ret));
    } else if (OB_FAIL(add_hint(inlist_hint.string(), allocator))) {
      LOG_WARN("failed to add inlist hint", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(
            param.gather_start_time_, param.max_duration_time_,
            duration_time))) {
      LOG_WARN("failed to get valid duration time", K(ret));
    } else if (duration_time > 0
               && OB_FAIL(fill_query_timeout_info(allocator,
                                                  duration_time))) {
      LOG_WARN("failed to fill query timeout info", K(ret));
    }
  }
  return ret;
}

int ObCatalogMinMaxEstimator::fill_batch_where_clause(
    ObIAllocator &allocator,
    const ObOptCatalogStatGatherParam &param,
    const ObIArray<int64_t> &eligible_indices)
{
  int ret = OB_SUCCESS;
  const bool is_oracle = lib::is_oracle_mode();
  const char *lq = is_oracle ? "\"" : "`";
  const char *rq = is_oracle ? "\"" : "`";
  ObSqlString tmp;

  // WHERE (col1, col2) IN (("v1","v2"), ...)
  if (OB_FAIL(tmp.append("WHERE ("))) {
    LOG_WARN("failed to append", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param.part_cols_.count(); ++i) {
    if (i > 0 && OB_FAIL(tmp.append(", "))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(tmp.append_fmt(
                   "%s%.*s%s", lq,
                   param.part_cols_.at(i).length(),
                   param.part_cols_.at(i).ptr(), rq))) {
      LOG_WARN("failed to append", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(tmp.append(") IN ("))) {
    LOG_WARN("failed to append", K(ret));
  }
  for (int64_t ei = 0; OB_SUCC(ret) && ei < eligible_indices.count(); ++ei) {
    const ObCatalogExtPartitionInfo &pi =
        param.partition_infos_.at(eligible_indices.at(ei));
    if (ei > 0 && OB_FAIL(tmp.append(", "))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(tmp.append("("))) {
      LOG_WARN("failed to append", K(ret));
    }
    for (int64_t v = 0; OB_SUCC(ret) && v < pi.partition_values_.count();
         ++v) {
      if (v > 0 && OB_FAIL(tmp.append(", "))) {
        LOG_WARN("failed to append", K(ret));
      } else if (OB_FAIL(tmp.append_fmt(
                     "\"%.*s\"",
                     pi.partition_values_.at(v).length(),
                     pi.partition_values_.at(v).ptr()))) {
        LOG_WARN("failed to append val", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(tmp.append(")"))) {
      LOG_WARN("failed to append", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(tmp.append(")"))) {
    LOG_WARN("failed to append", K(ret));
  }

  if (OB_SUCC(ret)) {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(
                      allocator.alloc(tmp.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(tmp.length()));
    } else {
      MEMCPY(buf, tmp.ptr(), tmp.length());
      where_string_.assign(buf, tmp.length());
    }
  }
  return ret;
}

int ObCatalogMinMaxEstimator::fill_batch_group_by(
    ObIAllocator &allocator,
    const ObIArray<ObString> &part_cols)
{
  int ret = OB_SUCCESS;
  const bool is_oracle = lib::is_oracle_mode();
  const char *lq = is_oracle ? "\"" : "`";
  const char *rq = is_oracle ? "\"" : "`";
  ObSqlString tmp;

  if (OB_FAIL(tmp.append("GROUP BY "))) {
    LOG_WARN("failed to append", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_cols.count(); ++i) {
    if (i > 0 && OB_FAIL(tmp.append(", "))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(tmp.append_fmt(
                   "%s%.*s%s", lq,
                   part_cols.at(i).length(),
                   part_cols.at(i).ptr(), rq))) {
      LOG_WARN("failed to append", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(
                      allocator.alloc(tmp.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(tmp.length()));
    } else {
      MEMCPY(buf, tmp.ptr(), tmp.length());
      group_by_string_.assign(buf, tmp.length());
    }
  }
  return ret;
}

// Batch execution cannot use base class do_estimate() because:
// - Column 0 is the CONCAT partition key, not a stat item
// - Multiple result rows need hash-based routing to different partition stats
int ObCatalogMinMaxEstimator::execute_and_route_results(
    const ObOptCatalogStatGatherParam &param,
    const ObString &raw_sql,
    const ObIArray<int64_t> &refine_col_indices,
    ObIArray<ObOptCatalogStat> &opt_catalog_stats)
{
  int ret = OB_SUCCESS;
  ObCommonSqlProxy *sql_proxy = ctx_.get_sql_proxy();
  common::ObOracleSqlProxy oracle_proxy;
  sql::ObSQLSessionInfo *session = ctx_.get_my_session();
  sql::ObSQLSessionInfo::StmtSavedValue *session_value = NULL;
  SessionCharsetState old_charset_state;

  if (OB_ISNULL(sql_proxy) || OB_ISNULL(session)
      || OB_UNLIKELY(raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
             K(ret), K(sql_proxy), K(session), K(raw_sql.empty()));
  } else if (OB_FAIL(prepare_and_store_session(session, session_value,
                                               old_charset_state))) {
    LOG_WARN("failed to save session", K(ret));
  } else if (lib::is_oracle_mode()) {
    if (OB_FAIL(oracle_proxy.init(ctx_.get_sql_proxy()->get_pool()))) {
      LOG_WARN("failed to init oracle proxy", K(ret));
    } else {
      sql_proxy = &oracle_proxy;
    }
  }

  if (OB_SUCC(ret)) {
    observer::ObInnerSQLConnectionPool *pool =
        static_cast<observer::ObInnerSQLConnectionPool *>(
            sql_proxy->get_pool());
    sqlclient::ObISQLConnection *conn = NULL;
    hash::ObHashMap<ObString, int64_t> part_index_map;
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result)
    {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_FAIL(part_index_map.create(opt_catalog_stats.count(),
                                        "CatPartIdx"))) {
        LOG_WARN("failed to create partition index map",
                 K(ret), K(opt_catalog_stats.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < opt_catalog_stats.count();
             ++i) {
          if (OB_ISNULL(opt_catalog_stats.at(i).table_stat_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table stat is null", K(ret), K(i));
          } else if (OB_FAIL(part_index_map.set_refactored(
                         opt_catalog_stats.at(i)
                             .table_stat_->get_partition_value(),
                         i, 1))) {
            LOG_WARN("failed to set partition index map", K(ret), K(i),
                     K(opt_catalog_stats.at(i)
                           .table_stat_->get_partition_value()));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pool->acquire(session, conn,
                                       lib::is_oracle_mode()))) {
        LOG_WARN("failed to acquire inner connection", K(ret));
      } else if (OB_ISNULL(conn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("conn is null", K(ret));
      } else if (OB_FALSE_IT(conn->set_group_id(
                     static_cast<int64_t>(param.consumer_group_id_)))) {
      } else if (OB_FAIL(conn->execute_read(
                     param.table_identity_.tenant_id_,
                     raw_sql.ptr(), proxy_result))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
        // Column 0: partition key string built by CONCAT(...)
        ObSqlString part_val_buf;
        ObObj part_val_obj;
        int64_t part_val_idx = 0;
        if (OB_FAIL(client_result->get_obj(part_val_idx, part_val_obj))) {
          LOG_WARN("failed to get partition key", K(ret));
        } else if (OB_FAIL(part_val_buf.append(
                       part_val_obj.get_string()))) {
          LOG_WARN("failed to append partition value", K(ret));
        }

        // Route to matching OptCatalogStat by partition_value
        ObOptCatalogStat *dst = NULL;
        int64_t stat_idx = OB_INVALID_INDEX;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(part_index_map.get_refactored(
                       part_val_buf.string(), stat_idx))) {
          if (OB_HASH_NOT_EXIST == ret) {
            LOG_INFO("partition not found, skip",
                     K(part_val_buf.string()));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get partition index",
                     K(ret), K(part_val_buf.string()));
          }
        } else if (stat_idx < 0
                   || stat_idx >= opt_catalog_stats.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition index is invalid",
                   K(ret), K(stat_idx), K(opt_catalog_stats.count()));
        } else {
          dst = &opt_catalog_stats.at(stat_idx);
        }
        if (OB_FAIL(ret) || OB_ISNULL(dst)) {
          continue;
        }

        // Columns 1..N: min/max pairs for each refine column
        int64_t col_idx = 1;
        for (int64_t ri = 0; OB_SUCC(ret) && ri < refine_col_indices.count();
             ++ri) {
          int64_t ci = refine_col_indices.at(ri);
          ObObj min_tmp, max_tmp, min_val, max_val;
          if (OB_FAIL(client_result->get_obj(col_idx, min_tmp))
              || OB_FAIL(client_result->get_obj(col_idx + 1, max_tmp))) {
            LOG_WARN("failed to get min/max obj", K(ret), K(col_idx));
          } else if (ci < dst->column_stats_.count()
                     && OB_NOT_NULL(dst->column_stats_.at(ci))) {
            if (OB_FAIL(ob_write_obj(allocator_, min_tmp, min_val))
                || OB_FAIL(ob_write_obj(allocator_, max_tmp, max_val))) {
              LOG_WARN("failed to deep copy obj", K(ret));
            } else if (OB_FAIL(
                           ObDbmsStatsUtils::truncate_string_for_opt_stats(
                               min_val, allocator_))
                       || OB_FAIL(
                           ObDbmsStatsUtils::truncate_string_for_opt_stats(
                               max_val, allocator_))) {
              LOG_WARN("failed to truncate string", K(ret));
            } else {
              dst->column_stats_.at(ci)->set_min_value(min_val);
              dst->column_stats_.at(ci)->set_max_value(max_val);
            }
          }
          col_idx += 2;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = sql_proxy->close(conn, true))) {
        LOG_WARN("close connection failed", K(ret), K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
      if (part_index_map.created()) {
        part_index_map.destroy();
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS
                    != (tmp_ret = restore_session(session, session_value,
                                                  old_charset_state)))) {
      LOG_WARN("failed to restore session", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_NOT_NULL(session_value)) {
      session_value->reset();
    }
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase
