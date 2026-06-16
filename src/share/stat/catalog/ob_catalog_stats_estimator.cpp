/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_catalog_stats_estimator.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/ob_stat_item.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace common
{

ObCatalogStatsEstimator::ObCatalogStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator)
    : ctx_(ctx), allocator_(allocator), catalog_name_(), db_name_(), tab_name_(), from_table_(),
      select_fields_(), sample_hint_(), other_hints_(), partition_string_(), group_by_string_(),
      where_string_(), stat_items_(), results_(), sample_value_(100.0), is_block_sample_(false),
      current_partition_value_(), collected_partition_values_(), tmp_tab_stat_ptr_(nullptr),
      tmp_col_stats_ptr_(nullptr)
{
}

int ObCatalogStatsEstimator::gen_select_filed()
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = 8000;
  char buf[buf_size];
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_items_.count(); ++i) {
    int64_t pos = 0;
    if (i != 0 && OB_FAIL(select_fields_.append(", "))) {
      LOG_WARN("failed to append delimiter", K(ret));
    } else if (OB_FAIL(stat_items_.at(i)->gen_expr(buf, buf_size, pos))) {
      LOG_WARN("failed to gen select expr", K(ret));
    } else if (OB_FAIL(select_fields_.append(buf, pos))) {
      LOG_WARN("failed to append stat item expr", K(ret));
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::init_escape_char_names(common::ObIAllocator &allocator,
                                                    const ObOptCatalogStatGatherParam &param)
{
  int ret = OB_SUCCESS;
  // Catalog tables need catalog_name, db_name and tab_name for three-part naming:
  // `catalog`.`db`.`table`
  if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
                                                                       param.table_identity_.catalog_name_,
                                                                       catalog_name_,
                                                                       lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(param.table_identity_.catalog_name_));
  } else if (OB_FAIL(
                 sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
                                                                          param.table_identity_.db_name_,
                                                                          db_name_,
                                                                          lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(param.table_identity_.db_name_));
  } else if (OB_FAIL(
                 sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
                                                                          param.table_identity_.tab_name_,
                                                                          tab_name_,
                                                                          lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(param.table_identity_.tab_name_));
  }
  return ret;
}

// Catalog table specific pack function: supports three-part naming `catalog`.`db`.`table`
// Catalog tables MUST always use three-part naming with catalog name
// Catalog name is always available: either from param or from session context
int ObCatalogStatsEstimator::pack(ObSqlString &raw_sql_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gen_select_filed())) {
    LOG_WARN("failed to generate select filed", K(ret));
  } else if (catalog_name_.empty()) {
    // Catalog name is required for catalog tables - this should never happen
    // Either catalog is already switched to, or it can be obtained from session
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog_name is empty for catalog table, this should not happen",
             K(ret),
             K(db_name_),
             K(from_table_));
  } else {
    // Three-part naming: `catalog`.`db`.`table` (required for catalog tables)
    // Catalog tables don't support snapshot/SCN queries, so no "as of scn" clause
    // TODO: Catalog table sampling is not yet implemented, sample_hint_ is placeholder for future
    // SAMPLE FILES support
    if (OB_FAIL(raw_sql_str.append_fmt(
            is_oracle_mode()
                ? "SELECT %.*s %.*s FROM \"%.*s\".\"%.*s\".\"%.*s\" %.*s %.*s %.*s %.*s"
                : "SELECT %.*s %.*s FROM `%.*s`.`%.*s`.`%.*s` %.*s %.*s %.*s %.*s",
            other_hints_.length(),
            other_hints_.ptr(),
            static_cast<int32_t>(select_fields_.length()),
            select_fields_.ptr(),
            catalog_name_.length(),
            catalog_name_.ptr(),
            db_name_.length(),
            db_name_.ptr(),
            from_table_.length(),
            from_table_.ptr(),
            partition_string_.length(),
            partition_string_.ptr(),
            sample_hint_.length(),
            sample_hint_.ptr(),
            where_string_.length(),
            where_string_.ptr(),
            group_by_string_.length(),
            group_by_string_.ptr()))) {
      LOG_WARN("failed to build query sql stmt for catalog table", K(ret));
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::fill_sample_info(common::ObIAllocator &alloc,
                                              double est_percent,
                                              bool block_sample)
{
  int ret = OB_SUCCESS;
  if (est_percent >= 0.000001 && est_percent < 100.0) {
    char *buf = NULL;
    // double type is generally 15-16 digits, plus ~16 chars for text, so 50 is sufficient
    int32_t buf_len = 50;
    int64_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(buf_len));
    } else if (!block_sample) {
      real_len = sprintf(buf, "SAMPLE (%lf)", est_percent);
    } else {
      real_len = sprintf(buf, "SAMPLE BLOCK (%lf)", est_percent);
      is_block_sample_ = true;
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(real_len < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(real_len));
      } else {
        sample_hint_.assign_ptr(buf, real_len);
        LOG_TRACE("succeed to add sample string", K(buf), K(real_len));
      }
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::fill_sample_info(common::ObIAllocator &alloc,
                                              const ObCatalogAnalyzeSampleInfo &sample_info)
{
  int ret = OB_SUCCESS;
  int64_t seed = sample_info.seed_;
  bool need_seed_clause = false;
  if (!sample_info.is_sample_) {
    // No sampling.
  } else if (OB_UNLIKELY(sample_info.percent_ < 0.000001
                         || sample_info.percent_ > 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(sample_info));
  } else if (sample_info.percent_ == 100.0) {
    //do nothing
  } else if (sample_info.is_file_sample()) {
    // FILE sample: generate "SAMPLE (%lf)" without SEED clause (seed defaults to -1).
    // Iterators detect file sample via is_row_sample() && seed_ == -1.
    if (OB_FAIL(fill_sample_info(alloc, sample_info.percent_, false/*block_sample*/))) {
      LOG_WARN("failed to fill file sample info", K(ret));
    } else {
      sample_value_ = sample_info.percent_;
    }
  } else if (sample_info.is_block_family_sample()) {
    char *buf = NULL;
    // Sample clause with optional seed:
    // "SAMPLE BLOCK (%lf)" or "SAMPLE BLOCK (%lf) SEED (%ld)"
    const int32_t buf_len = 96;
    int64_t real_len = -1;
    if (sample_info.is_fast_sample()) {
      need_seed_clause = false;
      seed = -1;
    } else {
      need_seed_clause = true;
      seed = (seed > 0) ? seed : 0;
    }
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sample hint buffer", K(ret), K(buf_len));
    } else if ((real_len = (!need_seed_clause)
                           ? sprintf(buf, "SAMPLE BLOCK (%lf)", sample_info.percent_)
                           : sprintf(buf, "SAMPLE BLOCK (%lf) SEED (%ld)",
                                     sample_info.percent_, seed)) <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to build block sample hint", K(ret), K(real_len), K(sample_info));
    } else {
      sample_hint_.assign_ptr(buf, real_len);
      sample_value_ = sample_info.percent_;
      is_block_sample_ = true;
      LOG_TRACE("succeed to add block sample hint", K(sample_hint_), K(sample_info));
    }
  } else {
    // ROW sample: generate "SAMPLE (%lf) SEED (0)" with explicit SEED to distinguish from FILE.
    char *buf = NULL;
    const int32_t buf_len = 96;
    int64_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sample hint buffer", K(ret), K(buf_len));
    } else if ((real_len = sprintf(buf, "SAMPLE (%lf) SEED (0)", sample_info.percent_)) <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to build row sample hint", K(ret), K(real_len), K(sample_info));
    } else {
      sample_hint_.assign_ptr(buf, real_len);
      sample_value_ = sample_info.percent_;
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::fill_parallel_info(common::ObIAllocator &alloc, int64_t degree)
{
  int ret = OB_SUCCESS;
  if (degree > 1) {
    char *buf = NULL;
    // int64 value won't exceed 20 digits, plus 10 chars for common text, so 50 is sufficient.
    int32_t buf_len = 50;
    int64_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(buf_len));
    } else {
      real_len = sprintf(buf, "USE_PX, PARALLEL(%ld)", degree);
      if (OB_UNLIKELY(real_len < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(real_len));
      } else {
        ObString degree_str;
        degree_str.assign_ptr(buf, real_len);
        if (OB_FAIL(add_hint(degree_str, alloc))) {
          LOG_WARN("failed to add hint", K(ret));
        } else {
          LOG_TRACE("succeed to add degree string", K(degree_str));
        }
      }
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::fill_query_timeout_info(common::ObIAllocator &alloc,
                                                     const int64_t query_timeout)
{
  int ret = OB_SUCCESS;
  if (query_timeout > 0) {
    char *buf = NULL;
    // int64 value won't exceed 20 digits, plus 10 chars for common text, so
    // 50 is sufficient.
    int32_t buf_len = 50;
    int64_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(buf_len));
    } else {
      real_len = sprintf(buf, "QUERY_TIMEOUT(%ld)", query_timeout);
      if (OB_UNLIKELY(real_len < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(real_len));
      } else {
        ObString query_timeout_str;
        query_timeout_str.assign_ptr(buf, real_len);
        if (OB_FAIL(add_hint(query_timeout_str, alloc))) {
          LOG_WARN("failed to add hint", K(ret));
        } else {
          LOG_TRACE("succeed to add query timeout string", K(query_timeout_str));
        }
      }
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::add_hint(const ObString &hint_str, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (other_hints_.empty()) {
    const char *str = "/*+*/";
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(strlen(str))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(strlen(str)));
    } else {
      MEMCPY(buf, str, strlen(str));
      other_hints_.assign_ptr(buf, strlen(str));
    }
  }
  if (OB_SUCC(ret) && hint_str.length() > 0) {
    char *buf = NULL;
    int32_t len = 0;
    if (OB_ISNULL(buf = static_cast<char *>(
                      alloc.alloc(other_hints_.length() + hint_str.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory",
               K(ret),
               K(buf),
               K(other_hints_.length()),
               K(hint_str.length()));
    } else {
      MEMCPY(buf, other_hints_.ptr(), other_hints_.length() - 2);
      len += other_hints_.length() - 2;
      MEMCPY(buf + len, " ", 1);
      len += 1;
      MEMCPY(buf + len, hint_str.ptr(), hint_str.length());
      len += hint_str.length();
      MEMCPY(buf + len, other_hints_.ptr() + other_hints_.length() - 2, 2);
      len += 2;
      other_hints_.assign_ptr(buf, len);
      LOG_TRACE("Succeed to add hint", K(other_hints_));
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::fill_partition_info(const ObIArray<ObString> &part_col_names,
                                                 ObIAllocator &allocator,
                                                 const ObIArray<ObCatalogExtPartitionInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_where_str;
  if (OB_UNLIKELY(partition_infos.count() <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(partition_infos));
  } else if (OB_UNLIKELY(part_col_names.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty part col names", K(ret));
  } else if (OB_UNLIKELY(part_col_names.count()
                         != partition_infos.at(0).partition_values_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty part col names count not equals to part values count", K(ret));
  } else if (OB_FAIL(tmp_where_str.append_fmt("%s", "WHERE "))) {
    LOG_WARN("failed to append 'WHERE' str", K(ret));
  } else {
    if (OB_FAIL(tmp_where_str.append("("))) {
      LOG_WARN("failed to add left paren", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_col_names.count(); ++i) {
        if (i > 0 && OB_FAIL(tmp_where_str.append(", "))) {
          LOG_WARN("failed to append separator", K(ret));
        } else if (OB_FAIL(tmp_where_str.append(part_col_names.at(i)))) {
          LOG_WARN("failed to append col name", K(ret), K(part_col_names.at(i)));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append(") IN "))) {
        LOG_WARN("failed to append IN clause", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    // fill the where clause: (p1, p2, p3) in ((a,b,c), (...), ...)
    if (OB_FAIL(tmp_where_str.append("("))) {
      LOG_WARN("failed to append outer left paren", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
        if (OB_FAIL(tmp_where_str.append(i == 0 ? "(" : ", ("))) {
          LOG_WARN("failed to append left paren for values", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < partition_infos.at(i).partition_values_.count(); ++j) {
            if (j > 0 && OB_FAIL(tmp_where_str.append(", "))) {
              LOG_WARN("failed to append value separator", K(ret));
            } else if (OB_FAIL(tmp_where_str.append("\""))) {
              LOG_WARN("failed to append left quote", K(ret));
            } else if (OB_FAIL(tmp_where_str.append(partition_infos.at(i).partition_values_.at(j)))) {
              LOG_WARN("failed to append partition value", K(ret), K(partition_infos.at(i).partition_values_.at(j)));
            } else if (OB_FAIL(tmp_where_str.append("\""))) {
              LOG_WARN("failed to append right quote", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append(")"))) {
            LOG_WARN("failed to append right paren for values", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append(")"))) {
        LOG_WARN("failed to append outer right paren", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      char *buf = NULL;
      int64_t buf_len = tmp_where_str.length();
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(tmp_where_str.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(tmp_where_str.length()));
      } else {
        MEMCPY(buf, tmp_where_str.ptr(), tmp_where_str.length());
        where_string_.assign(buf, tmp_where_str.length());
      }
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::fill_single_partition_info(const ObIArray<ObString> &part_col_names,
                                                        ObIAllocator &allocator,
                                                        const ObIArrayWrap<ObString> &partition_values)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_where_str;
  if (OB_UNLIKELY(partition_values.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition values is empty", K(ret));
  } else if (OB_UNLIKELY(part_col_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition col names is empty", K(ret));
  } else if (OB_UNLIKELY(part_col_names.count() != partition_values.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition col names count not equals to partition values count", K(ret),
             K(part_col_names.count()), K(partition_values.count()));
  } else if (OB_FAIL(tmp_where_str.append_fmt("%s", "WHERE "))) {
    LOG_WARN("failed to append 'WHERE' str", K(ret));
  } else {
    if (OB_FAIL(tmp_where_str.append("("))) {
      LOG_WARN("failed to add left paren", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_col_names.count(); ++i) {
        if (i > 0 && OB_FAIL(tmp_where_str.append(", "))) {
          LOG_WARN("failed to append separator", K(ret));
        } else if (OB_FAIL(tmp_where_str.append(part_col_names.at(i)))) {
          LOG_WARN("failed to append col name", K(ret), K(part_col_names.at(i)));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append(") IN ("))) {
        LOG_WARN("failed to append IN clause", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append("("))) {
      LOG_WARN("failed to append left paren for single row", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_values.count(); ++i) {
        if (i > 0 && OB_FAIL(tmp_where_str.append(", "))) {
          LOG_WARN("failed to append value separator", K(ret));
        } else if (OB_FAIL(tmp_where_str.append("\""))) {
          LOG_WARN("failed to append left quote", K(ret));
        } else if (OB_FAIL(tmp_where_str.append(partition_values.at(i)))) {
          LOG_WARN("failed to append partition value", K(ret), K(partition_values.at(i)));
        } else if (OB_FAIL(tmp_where_str.append("\""))) {
          LOG_WARN("failed to append right quote", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append(")"))) {
        LOG_WARN("failed to append right paren for single row", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(tmp_where_str.append(")"))) {
      LOG_WARN("failed to append outer right paren", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char *buf = NULL;
    int64_t buf_len = tmp_where_str.length();
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(tmp_where_str.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(tmp_where_str.length()));
    } else {
      MEMCPY(buf, tmp_where_str.ptr(), tmp_where_str.length());
      where_string_.assign(buf, tmp_where_str.length());
      partition_string_.reset();
    }
  }
  return ret;
}

// Catalog table specific: use ObOptCatalogStat instead of ObOptStat (no conversion needed)
int ObCatalogStatsEstimator::do_estimate(const ObOptCatalogStatGatherParam &gather_param,
                                         const ObString &raw_sql,
                                         bool need_copy_basic_stat,
                                         ObOptCatalogStat &src_catalog_stat,
                                         ObIArray<ObOptCatalogStat> &dst_catalog_stats)
{
  int ret = OB_SUCCESS;
  common::ObOracleSqlProxy oracle_proxy; // TODO, check the usage, is there any postprocess
  ObCommonSqlProxy *sql_proxy = ctx_.get_sql_proxy();
  ObArenaAllocator tmp_alloc("CatStatGather", OB_MALLOC_NORMAL_BLOCK_SIZE, gather_param.table_identity_.tenant_id_);
  sql::ObSQLSessionInfo::StmtSavedValue *session_value = NULL;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  SessionCharsetState old_charset_state;
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(session)
      || OB_UNLIKELY(dst_catalog_stats.empty() || raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty",
             K(ret),
             K(sql_proxy),
             K(dst_catalog_stats.empty()),
             K(session),
             K(raw_sql.empty()));
  } else if (OB_FAIL(prepare_and_store_session(session,
                                               session_value,
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
    observer::ObInnerSQLConnectionPool *pool
        = static_cast<observer::ObInnerSQLConnectionPool *>(sql_proxy->get_pool());
    sqlclient::ObISQLConnection *conn = NULL;
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result)
    {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_FAIL(pool->acquire(session, conn, lib::is_oracle_mode()))) {
        LOG_WARN("failed to acquire inner connection", K(ret));
      } else if (OB_ISNULL(conn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("conn is null", K(ret), K(conn));
      } else if (OB_FALSE_IT(
                     conn->set_group_id(static_cast<int64_t>(gather_param.consumer_group_id_)))) {
      } else if (OB_FAIL(
                     conn->execute_read(gather_param.table_identity_.tenant_id_, raw_sql.ptr(), proxy_result))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          for (int64_t i = 0; OB_SUCC(ret) && i < get_item_size(); ++i) {
            ObObj tmp;
            ObObj val;
            if (OB_FAIL(client_result->get_obj(i, tmp))) {
              LOG_WARN("failed to get object", K(ret));
            } else if (OB_FAIL(ob_write_obj(allocator_, tmp, val))) {
              LOG_WARN("failed to write object", K(ret));
            } else if (OB_FAIL(add_result(val))) {
              LOG_WARN("failed to add result", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(decode(allocator_))) {
            LOG_WARN("failed to decode results", K(ret));
          } else if (need_copy_basic_stat
                     && OB_FAIL(copy_basic_catalog_stat(gather_param.column_params_,
                                                        src_catalog_stat,
                                                        dst_catalog_stats))) {
            LOG_WARN("failed to copy stat to target opt stat", K(ret));
          } else {
            results_.reset();
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = sql_proxy->close(conn, true))) {
        LOG_WARN("close result set failed", K(ret), K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS
                    != (tmp_ret = restore_session(session,
                                                  session_value,
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

int ObCatalogStatsEstimator::prepare_and_store_session(
    ObSQLSessionInfo *session,
    sql::ObSQLSessionInfo::StmtSavedValue *&session_value,
    SessionCharsetState &old_state)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session));
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(sql::ObSQLSessionInfo::StmtSavedValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for saved session value", K(ret));
  } else {
    session_value = new (ptr) sql::ObSQLSessionInfo::StmtSavedValue();
    if (OB_FAIL(session->save_session(*session_value))) {
      LOG_WARN("failed to save session", K(ret));
    } else if (session->is_in_external_catalog() && OB_FAIL(session->set_internal_catalog_db())) {
      LOG_WARN("failed to set catalog", K(ret));
    } else {
      ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
      ObCollationType default_collation_type = ObCharset::get_system_collation();
      ObCharsetType default_charset_type = ObCharset::charset_type_by_coll(default_collation_type);
      if (OB_FAIL(session->get_character_set_client(old_state.client_charset_type_))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(session->get_character_set_connection(old_state.connection_charset_type_))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(session->get_character_set_results(old_state.result_charset_type_))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(session->get_collation_connection(old_state.collation_type_))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::dbms_stat_set_names(session,
                                                               default_charset_type,
                                                               default_charset_type,
                                                               default_charset_type,
                                                               default_collation_type))) {
        LOG_WARN("fail to dbms_stat_set_names", K(ret));
      } else {
        session->set_inner_session();
        session->set_autocommit(true);
      }
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::restore_session(ObSQLSessionInfo *session,
                                             sql::ObSQLSessionInfo::StmtSavedValue *session_value,
                                             const SessionCharsetState &old_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session) || OB_ISNULL(session_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session), K(session_value));
  } else if (OB_FAIL(session->restore_session(*session_value))) {
    LOG_WARN("failed to restore session", K(ret));
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
    if (OB_FAIL(ObDbmsStatsUtils::dbms_stat_set_names(session,
                                                      old_state.client_charset_type_,
                                                      old_state.connection_charset_type_,
                                                      old_state.result_charset_type_,
                                                      old_state.collation_type_))) {
      LOG_WARN("fail to dbms_stat_set_names", K(ret));
    }
  }
  LOG_TRACE("prepare_and_store session",
            K(old_state.client_charset_type_),
            K(old_state.connection_charset_type_),
            K(old_state.result_charset_type_),
            K(old_state.collation_type_),
            K(ret));
  return ret;
}

int64_t ObCatalogStatsEstimator::scale_row_count_by_sample(int64_t row_cnt) const
{
  if (sample_value_ >= 0.000001 && sample_value_ < 100.0) {
    row_cnt = static_cast<int64_t>(row_cnt * 100 / sample_value_);
  }
  return row_cnt;
}

int ObCatalogStatsEstimator::decode(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stat_items_.count() != results_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size does not match", K(ret), K(stat_items_.count()), K(results_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_items_.count(); ++i) {
    if (OB_FAIL(stat_items_.at(i)->decode(results_.at(i), allocator))) {
      LOG_WARN("failed to decode statistic result", K(ret));
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::copy_basic_catalog_stat(
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    ObOptCatalogStat &src_catalog_stat,
    ObIArray<ObOptCatalogStat> &dst_catalog_stats)
{
  int ret = OB_SUCCESS;
  ObOptCatalogTableStat *tmp_tab_stat = src_catalog_stat.table_stat_;
  ObIArray<ObOptCatalogColumnStat *> &tmp_col_stats = src_catalog_stat.column_stats_;
  if (OB_ISNULL(tmp_tab_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(tmp_tab_stat));
  } else {
    ObString partition_value = tmp_tab_stat->get_partition_value();
    bool find_it = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find_it && i < dst_catalog_stats.count(); ++i) {
      if (OB_ISNULL(dst_catalog_stats.at(i).table_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(dst_catalog_stats.at(i).table_stat_));
      } else if (dst_catalog_stats.at(i).table_stat_->get_partition_value() == partition_value) {
        find_it = true;
        int64_t original_row_cnt = tmp_tab_stat->get_row_count();
        int64_t row_cnt = scale_row_count_by_sample(original_row_cnt);
        dst_catalog_stats.at(i).table_stat_->set_row_count(row_cnt);
        dst_catalog_stats.at(i).table_stat_->set_avg_row_len(tmp_tab_stat->get_avg_row_len());
        dst_catalog_stats.at(i).table_stat_->set_sample_size(tmp_tab_stat->get_row_count());
        if (OB_FAIL(copy_basic_col_stats(original_row_cnt,
                                         row_cnt,
                                         column_params,
                                         tmp_col_stats,
                                         dst_catalog_stats.at(i).column_stats_))) {
          LOG_WARN("failed to copy col stat", K(ret));
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !find_it) {
      LOG_INFO("partition value isn't in needed partition values, no need gather stats",
                K(ret),
                K(partition_value),
                K(find_it));
    }
  }
  return ret;
}

int ObCatalogStatsEstimator::copy_basic_col_stats(
    const int64_t cur_row_cnt,
    const int64_t total_row_cnt,
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    ObIArray<ObOptCatalogColumnStat *> &src_col_stats,
    ObIArray<ObOptCatalogColumnStat *> &dst_col_stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src_col_stats.count() != dst_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(src_col_stats.count()), K(dst_col_stats.count()), K(ret));
  } else if (OB_UNLIKELY(column_params.count() != src_col_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(column_params.count()), K(src_col_stats.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_col_stats.count(); ++i) {
      if (OB_ISNULL(dst_col_stats.at(i)) || OB_ISNULL(src_col_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(dst_col_stats.at(i)));
      } else {
        int64_t num_not_null = src_col_stats.at(i)->get_num_not_null();
        int64_t num_null = src_col_stats.at(i)->get_num_null();
        int64_t num_distinct = src_col_stats.at(i)->get_num_distinct();
        ObNdvScaleAlgo ndv_scale_algo = column_params.at(i).ndv_scale_algo_;
        if (sample_value_ >= 0.000001 && sample_value_ < 100.0) {
          num_not_null = static_cast<int64_t>(num_not_null * 100 / sample_value_);
          num_null = static_cast<int64_t>(num_null * 100 / sample_value_);
          if (ndv_scale_algo == NDV_SCALE_ALGO_UNIQUE) {
            num_distinct = std::min(num_not_null, total_row_cnt);
          } else if (ndv_scale_algo == NDV_SCALE_ALGO_LINEAR && is_block_sample_) {
            num_distinct = static_cast<int64_t>(num_distinct * 100 / sample_value_);
            num_distinct = std::min(num_distinct, total_row_cnt);
          } else {
            num_distinct
                = ObOptSelectivity::scale_distinct(total_row_cnt, cur_row_cnt, num_distinct);
          }
        }
        dst_col_stats.at(i)->set_max_value(src_col_stats.at(i)->get_max_value());
        dst_col_stats.at(i)->set_min_value(src_col_stats.at(i)->get_min_value());
        dst_col_stats.at(i)->set_num_not_null(num_not_null);
        dst_col_stats.at(i)->set_num_null(num_null);
        dst_col_stats.at(i)->set_num_distinct(num_distinct);
        dst_col_stats.at(i)->set_avg_length(src_col_stats.at(i)->get_avg_length());
        // Copy bitmap: aligned with internal table impl (ob_stats_estimator.cpp)
        if (OB_ISNULL(dst_col_stats.at(i)->get_llc_bitmap())
            || OB_ISNULL(src_col_stats.at(i)->get_llc_bitmap())
            || OB_UNLIKELY(src_col_stats.at(i)->get_llc_bitmap_size()
                           > dst_col_stats.at(i)->get_llc_bitmap_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error",
                   K(dst_col_stats.at(i)->get_llc_bitmap()),
                   K(src_col_stats.at(i)->get_llc_bitmap()),
                   K(dst_col_stats.at(i)->get_llc_bitmap_size()),
                   K(src_col_stats.at(i)->get_llc_bitmap_size()),
                   K(ret));
        } else {
          MEMCPY(const_cast<char *>(dst_col_stats.at(i)->get_llc_bitmap()),
                 src_col_stats.at(i)->get_llc_bitmap(),
                 src_col_stats.at(i)->get_llc_bitmap_size());
          LOG_TRACE("succeed to copy basic col stats with bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

} // end of common
} // end of oceanbase
