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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_stats_estimator.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/optimizer/ob_opt_selectivity.h"

namespace oceanbase
{
namespace common
{

ObStatsEstimator::ObStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator) :
  ctx_(ctx),
  allocator_(allocator),
  db_name_(),
  from_table_(),
  partition_hint_(),
  select_fields_(),
  sample_hint_(),
  other_hints_(),
  partition_string_(),
  group_by_string_(),
  where_string_(),
  stat_items_(),
  results_(),
  sample_value_(100.0),
  is_block_sample_(false)
{}

int ObStatsEstimator::gen_select_filed()
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

int ObStatsEstimator::init_escape_char_names(common::ObIAllocator &allocator,
                                             const ObOptStatGatherParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
              allocator,
              param.db_name_,
              db_name_,
              lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(param.db_name_));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     allocator,
                     param.tab_name_,
                     tab_name_,
                     lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(param.tab_name_));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     allocator,
                     param.data_table_name_,
                     data_table_name_,
                     lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(param.data_table_name_));
  }
  return ret;
}

int ObStatsEstimator::pack(ObSqlString &raw_sql_str)
{
  int ret = OB_SUCCESS;
  // SELECT <hint> <fields> FROM <table> <partition hint>
  if (OB_FAIL(gen_select_filed())) {
    LOG_WARN("failed to generate select filed", K(ret));
  } else if (OB_FAIL(raw_sql_str.append_fmt(is_oracle_mode() ?
                                            "SELECT %.*s %.*s FROM \"%.*s\".\"%.*s\" %.*s %.*s %.*s %.*s %.*s" :
                                            "SELECT %.*s %.*s FROM `%.*s`.`%.*s` %.*s %.*s %.*s %.*s %.*s" ,
                                            other_hints_.length(),
                                            other_hints_.ptr(),
                                            static_cast<int32_t>(select_fields_.length()),
                                            select_fields_.ptr(),
                                            db_name_.length(),
                                            db_name_.ptr(),
                                            from_table_.length(),
                                            from_table_.ptr(),
                                            partition_string_.length(),
                                            partition_string_.ptr(),
                                            sample_hint_.length(),
                                            sample_hint_.ptr(),
                                            current_scn_string_.length(),
                                            current_scn_string_.ptr(),
                                            where_string_.length(),
                                            where_string_.ptr(),
                                            group_by_string_.length(),
                                            group_by_string_.ptr()))) {
    LOG_WARN("failed to build query sql stmt", K(ret));
  } else {
    LOG_TRACE("OptStat: basic stat query sql", K(raw_sql_str));
  }
  return ret;
}

int ObStatsEstimator::fill_sample_info(common::ObIAllocator &alloc,
                                       double est_percent,
                                       bool block_sample)
{
  int ret = OB_SUCCESS;
  if (est_percent>= 0.000001 && est_percent < 100.0) {
    char *buf = NULL;
    int32_t buf_len = 50;//double类型一般15～16位，加上字符长度16左右，因此数组长度为50足够用
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

int ObStatsEstimator::fill_sample_info(common::ObIAllocator &alloc,
                                       const ObAnalyzeSampleInfo &sample_info)
{
  int ret = OB_SUCCESS;
  if (!sample_info.is_sample_ || sample_info.sample_type_ == SampleType::RowSample) {
  } else if (OB_UNLIKELY(sample_info.sample_value_ < 0.000001 || sample_info.sample_value_ > 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(sample_info));
  } else if (sample_info.sample_value_ == 100.0) {
    //do nothing
  } else if (OB_FAIL(fill_sample_info(alloc,
                                      sample_info.sample_value_,
                                      sample_info.is_block_sample_))) {
    LOG_WARN("failed to fill sample info", K(ret));
  } else {
    sample_value_ = sample_info.sample_value_;
  }
  return ret;
}

int ObStatsEstimator::fill_specify_scn_info(common::ObIAllocator &alloc,
                                            uint64_t sepcify_scn)
{
  int ret = OB_SUCCESS;
  if (sepcify_scn > 0) {
    const char* fmt_str = lib::is_oracle_mode() ? "as of scn %lu" : "as of snapshot %lu";
    char *buf = NULL;
    int64_t buf_len = strlen(fmt_str) + 30;//uint64_t:0 ~ 18446744073709551615,length no more than 20
    int64_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(buf_len));
    } else {
      real_len = sprintf(buf, fmt_str, sepcify_scn);
      if (OB_UNLIKELY(real_len <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(real_len));
      } else {
        current_scn_string_.assign_ptr(buf, real_len);
        LOG_TRACE("succeed to fill specify scn info", K(current_scn_string_));
      }
    }
  }
  return ret;
}

int ObStatsEstimator::fill_parallel_info(common::ObIAllocator &alloc,
                                         int64_t degree)
{
  int ret = OB_SUCCESS;
  if (degree > 1) {
    char *buf = NULL;
    int32_t buf_len = 50;//int64长度不会超过20位，加上普通字符长度10，因此数组长度为50足够用
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

int ObStatsEstimator::fill_query_timeout_info(common::ObIAllocator &alloc,
                                              const int64_t query_timeout)
{
  int ret = OB_SUCCESS;
  if (query_timeout > 0) {
    char *buf = NULL;
    int32_t buf_len = 50;//int64长度不会超过20位，加上普通字符长度10，因此数组长度为50足够用
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

int ObStatsEstimator::add_hint(const ObString &hint_str,
                               common::ObIAllocator &alloc)
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
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(other_hints_.length() + hint_str.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(other_hints_.length()), K(hint_str.length()));
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

int ObStatsEstimator::fill_partition_info(ObIAllocator &allocator,
                                          const ObIArray<PartInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_part_str;
  if (OB_UNLIKELY(partition_infos.count() <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(partition_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
      ObString new_part_name;
      if (OB_UNLIKELY(partition_infos.at(i).part_name_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition name is empty", K(ret), K(partition_infos), K(i));
      } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
                                                                                  partition_infos.at(i).part_name_,
                                                                                  new_part_name,
                                                                                  lib::is_oracle_mode()))) {
        LOG_WARN("failed to generate new name with escape character", K(ret), K(partition_infos.at(i).part_name_));
      } else if (OB_FAIL(tmp_part_str.append_fmt(lib::is_oracle_mode() ? "%s\"%.*s\"%s" : "%s`%.*s`%s",
                                                 i == 0 ? "PARTITION(" : " ",
                                                 new_part_name.length(),
                                                 new_part_name.ptr(),
                                                 i == partition_infos.count() - 1 ? ")" : ", "))) {
        LOG_WARN("failed to append", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      int64_t buf_len = tmp_part_str.length();
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(tmp_part_str.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(tmp_part_str.length()));
      } else {
        MEMCPY(buf, tmp_part_str.ptr(), tmp_part_str.length());
        partition_string_.assign(buf, tmp_part_str.length());
      }
    }
  }
  return ret;
}

int ObStatsEstimator::fill_partition_info(ObIAllocator &allocator,
                                          const ObString &part_name)
{
  int ret = OB_SUCCESS;
  ObString new_part_name;
  if (OB_UNLIKELY(part_name.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition name is empty", K(ret), K(part_name));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
                                                                              part_name,
                                                                              new_part_name,
                                                                              lib::is_oracle_mode()))) {
    LOG_WARN("failed to generate new name with escape character", K(ret), K(part_name));
  } else {
    const char *fmt_str = lib::is_oracle_mode() ? "PARTITION (\"%.*s\")" : "PARTITION (`%.*s`)";
    char *buf = NULL;
    const int64_t len = strlen(fmt_str) + new_part_name.length();
    int32_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(len));
    } else {
      real_len = sprintf(buf, fmt_str, new_part_name.length(), new_part_name.ptr());
      if (OB_UNLIKELY(real_len < 0 || real_len >= len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to print partition hint", K(ret), K(real_len), K(len));
      } else {
        partition_string_.assign(buf, real_len);
      }
    }
  }
  return ret;
}

//Specify the partition name or non-partition table don't use group by.
int ObStatsEstimator::fill_group_by_info(ObIAllocator &allocator,
                                         const ObOptStatGatherParam &param,
                                         ObString &calc_part_id_str)
{
  int ret = OB_SUCCESS;
  const char *fmt_str = lib::is_oracle_mode() ? "GROUP BY CALC_PARTITION_ID(\"%.*s\", %.*s)"
                                                : "GROUP BY CALC_PARTITION_ID(`%.*s`, %.*s)";
  char *buf = NULL;
  ObString type_str;
  if (param.stat_level_ == PARTITION_LEVEL) {
    type_str = ObString(4, "PART");
  } else if (param.stat_level_ == SUBPARTITION_LEVEL) {
    type_str = ObString(7, "SUBPART");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type", K(param.stat_level_), K(ret));
  }
  if (OB_SUCC(ret)) {
    const int64_t len = strlen(fmt_str) + from_table_.length() + type_str.length();
    int32_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(len));
    } else {
      real_len = sprintf(buf, fmt_str, from_table_.length(), from_table_.ptr(), type_str.length(), type_str.ptr());
      if (OB_UNLIKELY(real_len < 0 || real_len >= len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to print partition hint", K(ret), K(real_len), K(len));
      } else {
        group_by_string_.assign(buf, real_len);
        //"GROUP BY CALC_PARTITION_ID(xxxxx)"
        const int64_t len_group_by = strlen("GROUP BY ");
        calc_part_id_str.assign_ptr(group_by_string_.ptr() + len_group_by,
                                    group_by_string_.length() - len_group_by);
        LOG_TRACE("Succeed to fill group by info", K(group_by_string_), K(calc_part_id_str));
      }
    }
  }
  return ret;
}

int ObStatsEstimator::do_estimate(const ObOptStatGatherParam &gather_param,
                                  const ObString &raw_sql,
                                  bool need_copy_basic_stat,
                                  ObOptStat &src_opt_stat,
                                  ObIArray<ObOptStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  common::ObOracleSqlProxy oracle_proxy; // TODO, check the usage, is there any postprocess
  ObCommonSqlProxy *sql_proxy = ctx_.get_sql_proxy();
  ObArenaAllocator tmp_alloc("OptStatGather", OB_MALLOC_NORMAL_BLOCK_SIZE, gather_param.tenant_id_);
  sql::ObSQLSessionInfo::StmtSavedValue *session_value = NULL;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  ObCharsetType old_client_charset_type = CHARSET_UTF8MB4;
  ObCharsetType old_connection_charset_type = CHARSET_UTF8MB4;
  ObCharsetType old_result_charset_type = CHARSET_UTF8MB4;
  ObCollationType old_collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(session) ||
      OB_UNLIKELY(dst_opt_stats.empty() || raw_sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(sql_proxy), K(dst_opt_stats.empty()),
                                     K(session), K(raw_sql.empty()));
  } else if (OB_FAIL(prepare_and_store_session(session,
                                               session_value,
                                               old_client_charset_type,
                                               old_connection_charset_type,
                                               old_result_charset_type,
                                               old_collation_type))) {
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
                            static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool());
    sqlclient::ObISQLConnection *conn = NULL;
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_FAIL(pool->acquire(session, conn, lib::is_oracle_mode()))) {
        LOG_WARN("failed to acquire inner connection", K(ret));
      } else if (OB_ISNULL(conn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("conn is null", K(ret), K(conn));
      } else if (OB_FALSE_IT(conn->set_group_id(static_cast<int64_t>(gather_param.consumer_group_id_)))) {
      } else if (OB_FAIL(conn->execute_read(gather_param.tenant_id_, raw_sql.ptr(), proxy_result))) {
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
          if (OB_SUCC(ret)) {
            if (OB_FAIL(decode(allocator_))) {
              LOG_WARN("failed to decode results", K(ret));
            } else if (need_copy_basic_stat &&
                       OB_FAIL(copy_basic_opt_stat(gather_param.column_params_,
                                                   gather_param.partition_id_block_map_,
                                                   src_opt_stat,
                                                   dst_opt_stats))) {
              LOG_WARN("failed to copy stat to target opt stat", K(ret));
            } else {
              results_.reset();
            }
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
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = restore_session(session,
                                                             session_value,
                                                             old_client_charset_type,
                                                             old_connection_charset_type,
                                                             old_result_charset_type,
                                                             old_collation_type)))) {
      LOG_WARN("failed to restore session", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_NOT_NULL(session_value)) {
      session_value->reset();
    }
  }
  return ret;
}

int ObStatsEstimator::prepare_and_store_session(ObSQLSessionInfo *session,
                                                sql::ObSQLSessionInfo::StmtSavedValue *&session_value,
                                                ObCharsetType& old_client_charset_type,
                                                ObCharsetType& old_connection_charset_type,
                                                ObCharsetType& old_result_charset_type,
                                                ObCollationType& old_collation_type)
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
    session_value = new(ptr)sql::ObSQLSessionInfo::StmtSavedValue();
    if (OB_FAIL(session->save_session(*session_value))) {
      LOG_WARN("failed to save session", K(ret));
    } else if (session->is_in_external_catalog() && OB_FAIL(session->set_internal_catalog_db())) {
      LOG_WARN("failed to set catalog", K(ret));
    } else {
      ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
      ObCollationType default_collation_type = ObCharset::get_system_collation();
      ObCharsetType default_charset_type = ObCharset::charset_type_by_coll(default_collation_type);
      if (OB_FAIL(session->get_character_set_client(old_client_charset_type))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(session->get_character_set_connection(old_connection_charset_type))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(session->get_character_set_results(old_result_charset_type))) {
        LOG_WARN("failed to update sys var", K(ret));
      } else if (OB_FAIL(session->get_collation_connection(old_collation_type))) {
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

int ObStatsEstimator::restore_session(ObSQLSessionInfo *session,
                                      sql::ObSQLSessionInfo::StmtSavedValue *session_value,
                                      ObCharsetType old_client_charset_type,
                                      ObCharsetType old_connection_charset_type,
                                      ObCharsetType old_result_charset_type,
                                      ObCollationType old_collation_type)
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
                                                      old_client_charset_type,
                                                      old_connection_charset_type,
                                                      old_result_charset_type,
                                                      old_collation_type))) {
      LOG_WARN("fail to dbms_stat_set_names", K(ret));
    }
  }
  LOG_TRACE("prepare_and_store session", K(old_client_charset_type),
                                         K(old_connection_charset_type),
                                         K(old_result_charset_type),
                                         K(old_collation_type),K(ret));
  return ret;
}

int ObStatsEstimator::decode(ObIAllocator &allocator)
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

int ObStatsEstimator::copy_basic_opt_stat(const ObIArray<ObColumnStatParam> &column_params,
                                          const PartitionIdBlockMap *partition_id_block_map,
                                          ObOptStat &src_opt_stat,
                                          ObIArray<ObOptStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  ObOptTableStat *tmp_tab_stat = src_opt_stat.table_stat_;
  ObIArray<ObOptColumnStat*> &tmp_col_stats = src_opt_stat.column_stats_;
  if (OB_ISNULL(tmp_tab_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(tmp_tab_stat));
  } else {
    int64_t partition_id = tmp_tab_stat->get_partition_id();
    bool find_it = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find_it && i < dst_opt_stats.count(); ++i) {
      if (OB_ISNULL(dst_opt_stats.at(i).table_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(dst_opt_stats.at(i).table_stat_));
      } else if (dst_opt_stats.at(i).table_stat_->get_partition_id() == partition_id) {
        find_it = true;
        int64_t row_cnt = tmp_tab_stat->get_row_count();
        if (sample_value_ >= 0.000001 &&
            sample_value_ < 100.0 &&
            OB_FAIL(scale_row_count(partition_id_block_map,
                                    partition_id,
                                    tmp_tab_stat->get_row_count(),
                                    sample_value_,
                                    row_cnt))) {
          LOG_WARN("failed to scale row count", K(ret));
        } else {
          dst_opt_stats.at(i).table_stat_->set_row_count(row_cnt);
          dst_opt_stats.at(i).table_stat_->set_avg_row_size(tmp_tab_stat->get_avg_row_size());
          dst_opt_stats.at(i).table_stat_->set_sample_size(tmp_tab_stat->get_row_count());
          if (OB_FAIL(copy_basic_col_stats(tmp_tab_stat->get_row_count(),
                                          row_cnt,
                                          column_params,
                                          tmp_col_stats,
                                          dst_opt_stats.at(i).column_stats_))) {
            LOG_WARN("failed to copy col stat", K(ret));
          } else {/*do nothing*/}
        }
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && !find_it) {
      LOG_TRACE("this partition id isn't in needed partition ids, no need gather stats",K(partition_id));
    }
  }
  return ret;
}

int ObStatsEstimator::copy_basic_col_stats(const int64_t cur_row_cnt,
                                           const int64_t total_row_cnt,
                                           const ObIArray<ObColumnStatParam> &column_params,
                                           ObIArray<ObOptColumnStat *> &src_col_stats,
                                           ObIArray<ObOptColumnStat *> &dst_col_stats)
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
            num_distinct = ObOptSelectivity::scale_distinct(total_row_cnt, cur_row_cnt, num_distinct);
          }
        }
        dst_col_stats.at(i)->set_max_value(src_col_stats.at(i)->get_max_value());
        dst_col_stats.at(i)->set_min_value(src_col_stats.at(i)->get_min_value());
        dst_col_stats.at(i)->set_num_not_null(num_not_null);
        dst_col_stats.at(i)->get_histogram().set_sample_size(src_col_stats.at(i)->get_num_not_null());
        dst_col_stats.at(i)->set_num_null(num_null);
        dst_col_stats.at(i)->set_num_distinct(num_distinct);
        dst_col_stats.at(i)->set_avg_len(src_col_stats.at(i)->get_avg_len());
        if (OB_ISNULL(dst_col_stats.at(i)->get_llc_bitmap()) ||
            OB_ISNULL(src_col_stats.at(i)->get_llc_bitmap()) ||
            OB_UNLIKELY(src_col_stats.at(i)->get_llc_bitmap_size() >
                                                      dst_col_stats.at(i)->get_llc_bitmap_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(dst_col_stats.at(i)->get_llc_bitmap()),
                                           K(src_col_stats.at(i)->get_llc_bitmap()),
                                           K(dst_col_stats.at(i)->get_llc_bitmap_size()),
                                           K(src_col_stats.at(i)->get_llc_bitmap_size()), K(ret));
        } else {
          MEMCPY(dst_col_stats.at(i)->get_llc_bitmap(),
                 src_col_stats.at(i)->get_llc_bitmap(),
                 src_col_stats.at(i)->get_llc_bitmap_size());
          dst_col_stats.at(i)->set_llc_bitmap_size(src_col_stats.at(i)->get_llc_bitmap_size());
          LOG_TRACE("Succeed to copy basic col stats", K(*dst_col_stats.at(i)), K(*src_col_stats.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObStatsEstimator::scale_row_count(const PartitionIdBlockMap *partition_id_block_map,
                                      int64_t partition_id,
                                      int64_t row_cnt,
                                      double &sample_value,
                                      int64_t &scaled_row_cnt)
{
  int ret = OB_SUCCESS;
  BlockNumStat *block_num_stat = NULL;
  if (OB_ISNULL(partition_id_block_map)) {
    if (sample_value >= 0.000001 && sample_value < 100.0) {
      scaled_row_cnt = static_cast<int64_t>(row_cnt * 100 / sample_value);
    }
  } else if (OB_FAIL(partition_id_block_map->get_refactored(partition_id, block_num_stat))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      ret = OB_SUCCESS;
      if (sample_value >= 0.000001 && sample_value < 100.0) {
        scaled_row_cnt = static_cast<int64_t>(row_cnt * 100 / sample_value);
      }
    } else {
      LOG_WARN("failed to get refactored", K(ret));
    }
  } else if (OB_ISNULL(block_num_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(block_num_stat));
  } else if (block_num_stat->sstable_row_cnt_ + block_num_stat->memtable_row_cnt_ > 0) {
    scaled_row_cnt = block_num_stat->sstable_row_cnt_ + block_num_stat->memtable_row_cnt_;
    sample_value = static_cast<double>(row_cnt) / scaled_row_cnt * 100.0;
  } else if (sample_value >= 0.000001 && sample_value < 100.0) {
    scaled_row_cnt = static_cast<int64_t>(row_cnt * 100 / sample_value);
  }
  return ret;
}

} // end of common
} // end of ocenabase
