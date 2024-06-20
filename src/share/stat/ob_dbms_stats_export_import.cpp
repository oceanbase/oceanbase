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
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_dbms_stats_export_import.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/object/ob_obj_cast.h"
#include "share/stat/ob_opt_stat_sql_service.h"
#include "share/stat/ob_dbms_stats_history_manager.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase {
using namespace sql;
namespace common {

#define CREATE_ORACLE_STAT_TABLE "(STATID VARCHAR2(128), TYPE CHAR(1), VERSION NUMBER, FLAGS NUMBER,\
                            C1 VARCHAR2(128), C2 VARCHAR2(128), C3 VARCHAR2(128), C4 VARCHAR2(128),\
                            C5 VARCHAR2(128), C6 VARCHAR2(128), N1 NUMBER, N2 NUMBER, N3 NUMBER, \
                            N4 NUMBER, N5 NUMBER, N6 NUMBER, N7 NUMBER, N8 NUMBER, N9 NUMBER, \
                            N10 NUMBER, N11	NUMBER, N12	NUMBER, N13	NUMBER, D1 DATE,\
                            T1 TIMESTAMP(6) WITH TIME ZONE, R1 RAW(1000), R2 RAW(1000),\
                            R3 RAW(1000), CH1 VARCHAR2(1000), CL1 CLOB, BL1 BLOB, OB_SPEC1 NUMBER,\
                            OB_SPEC2 NUMBER, OB_SPEC3 VARCHAR(4096))"

#define CREATE_MYSQL_STAT_TABLE "(STATID VARCHAR(128), TYPE CHAR(1), VERSION DECIMAL,FLAGS DECIMAL,\
                                  C1 VARCHAR(128),C2 VARCHAR(128), C3 VARCHAR(128),C4 VARCHAR(128),\
                                  C5 VARCHAR(128), C6 VARCHAR(128), N1 DECIMAL, N2 DOUBLE,\
                                  N3 DECIMAL, N4 DECIMAL, N5 DECIMAL, N6 DECIMAL, N7 DECIMAL,\
                                  N8 DECIMAL, N9 DECIMAL, N10 DECIMAL, N11 DECIMAL, N12	DECIMAL,\
                                  N13 DECIMAL, D1 TIMESTAMP(6), T1 TIMESTAMP, R1 TEXT(1000), \
                                  R2 TEXT(1000), R3 TEXT(1000), CH1 VARCHAR(1000), CL1 BLOB,\
                                  BL1 BLOB, OB_SPEC1 DECIMAL, OB_SPEC2 DECIMAL, OB_SPEC3 VARCHAR(4096))"

#define CHECK_STAT_SELECT_LIST "select statid, type, version, flags, c1, c2, c3, c4, n1, n2, n3,\
                                n4, n5,n6, n7, n8, n9, d1, r1, r2, ob_spec1, ob_spec2, ob_spec3 "

#define DELETE_STAT_TABLE "delete from %.*s where c5 = '%.*s' and c1 = '%.*s'"

#define INSERT_STAT_TABLE "INSERT INTO %.*s (STATID, TYPE, VERSION, FLAGS, C1, C2, C3,\
                                             C4, C5, C6, N1, N2, N3, N4, N5, N6, N7, \
                                             N8, N9, N10, N11,N12, N13, D1, T1, R1, \
                                             R2, R3, CH1, CL1, BL1, OB_SPEC1, \
                                             OB_SPEC2, OB_SPEC3) "

#define FETCH_TABLE_STAT "select null statid, '%c' type, '8' version, null flags, '%.*s' c1, \
                          %.*s c2, %.*s c3, '%.*s' c4, '%.*s' c5, null c6, row_cnt n1, null n2,\
                          avg_row_len n3, null n4, null n5, null n6, null n7, null n8, null n9,\
                          null n10, null n11, null n12, null n13, last_analyzed d1, null t1, \
                          null r1,null r2, null r3, null ch1, null cl1, null bl1, \
                          macro_blk_cnt ob_spec1, micro_blk_cnt ob_spec2, null ob_spec3\
                          from %s stat\
                          where TABLE_ID = %lu"

#define FETCH_COLUMN_STAT "select null statid, 'C' type, '8' version, case stat.histogram_type \
                           when 1 then 4096 + 4 when 3 then 8192 + 4 when 4 then 4 else 0 end flags,\
                           '%.*s' c1, %.*s c2, %.*s c3, %.*s c4, '%.*s' c5, null c6,\
                           stat.distinct_cnt n1, stat.density n2, stat.spare1 n3, stat.sample_size n4, \
                           stat.null_cnt n5, NULL n6, NULL n7, stat.avg_len n8, 1 n9, \
                           hist.endpoint_num n10, hist.endpoint_normalized_value n11, \
                           hist.endpoint_repeat_cnt n12, stat.bucket_cnt n13, last_analyzed d1, null t1, \
                           stat.b_min_value r1, stat.b_max_value r2, hist.b_endpoint_value r3, \
                           null ch1, null cl1, null bl1, stat.distinct_cnt_synopsis_size ob_spec1,\
                           null ob_spec2, stat.distinct_cnt_synopsis ob_spec3 from\
                           %s stat left join %s hist on \
                           stat.TENANT_ID = hist.TENANT_ID and stat.TABLE_ID = hist.TABLE_ID and\
                           hist.PARTITION_ID = stat.PARTITION_ID and stat.COLUMN_ID=hist.COLUMN_ID\
                           where stat.TABLE_ID = %lu "

#define FETCH_STAT_FROM_STAT_TABLE "select statid, type, version, %s,\
                                    c1, c2, c3, c4, c5, c6,n1, n2, n3, n4, n5, n6, n7, n8, n9, n10,\
                                    n11, n12, n13, d1,t1, r1, r2, r3, ch1, cl1, bl1, ob_spec1,\
                                    ob_spec2, ob_spec3 from %.*s where c5 = '%.*s' and c1 = '%.*s'"

int ObDbmsStatsExportImport::create_stat_table(ObExecContext &ctx,
                                               const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(create_oracle_stat_table(ctx, param))) {
      LOG_WARN("failed to create oracle stat table", K(ret));
    } else {/*do nothing*/}
  } else {
    if (OB_FAIL(create_mysql_stat_table(ctx, param))) {
      LOG_WARN("failed to create oracle stat table", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsExportImport::create_oracle_stat_table(ObExecContext &ctx,
                                                      const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  if (OB_FAIL(raw_sql.append_fmt("CREATE TABLE \"%.*s\".\"%.*s\"",
                                  param.db_name_.length(), param.db_name_.ptr(),
                                  param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(raw_sql.append(CREATE_ORACLE_STAT_TABLE))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (param.tab_group_.empty() && OB_FAIL(raw_sql.append(";"))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (!param.tab_group_.empty() &&
             OB_FAIL(raw_sql.append_fmt(" tablegroup = '%.*s';",
                                        param.tab_group_.length(), param.tab_group_.ptr()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(do_execute_sql(ctx, param.tenant_id_, raw_sql))) {
    LOG_WARN("failed to do execute sql", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsExportImport::create_mysql_stat_table(ObExecContext &ctx,
                                                     const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  if (OB_FAIL(raw_sql.append_fmt("CREATE TABLE `%.*s`.`%.*s`",
                                 param.db_name_.length(), param.db_name_.ptr(),
                                 param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(raw_sql.append(CREATE_MYSQL_STAT_TABLE))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (param.tab_group_.empty() && OB_FAIL(raw_sql.append(";"))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (!param.tab_group_.empty() &&
             OB_FAIL(raw_sql.append_fmt(" tablegroup = '%.*s';",
                                        param.tab_group_.length(), param.tab_group_.ptr()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(do_execute_sql(ctx, param.tenant_id_, raw_sql))) {
    LOG_WARN("failed to do execute sql", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsExportImport::drop_stat_table(ObExecContext &ctx, const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString select_raw_sql;
  ObSqlString drop_raw_sql;
  const char* check_table_str = lib::is_oracle_mode() ? "from \"%.*s\".\"%.*s\" where 0 = 1;" :
                                                          "from `%.*s`.`%.*s` where 0 = 1;";
  const char* drop_table_str = lib::is_oracle_mode() ? "DROP TABLE \"%.*s\".\"%.*s\"" :
                                                         "DROP TABLE `%.*s`.`%.*s`";
  if (OB_FAIL(select_raw_sql.append(CHECK_STAT_SELECT_LIST))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(select_raw_sql.append_fmt(check_table_str,
                                               param.db_name_.length(), param.db_name_.ptr(),
                                               param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(do_execute_sql(ctx, param.tenant_id_, select_raw_sql))) {
    if (ret == OB_ERR_BAD_FIELD_ERROR) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Unable to drop table: does not appear to be a statistics table", K(ret));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Unable to drop table: does not appear to be a statistics table");
    } else {
      LOG_WARN("failed to do execute sql", K(ret));
    }
  } else if (OB_FAIL(drop_raw_sql.append_fmt(drop_table_str,
                                             param.db_name_.length(), param.db_name_.ptr(),
                                             param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(do_execute_sql(ctx, param.tenant_id_, drop_raw_sql))) {
    LOG_WARN("failed to do execute sql", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsExportImport::export_table_stats(ObExecContext &ctx,
                                                const ObTableStatParam &param,
                                                const ObString &data_name)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString delete_stat_sql;
  ObSqlString part_str;
  ObSqlString subpart_str;
  ObSqlString table_name_str;
  const uint64_t tenant_id = param.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t valid_tab_id = lib::is_oracle_mode() ? param.table_id_ :
                              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, param.table_id_);
  const char *from_table_name = lib::is_oracle_mode() ? "sys.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT"
                                                          : "oceanbase.__all_table_stat";
  const char *null_str = "NULL";
  int32_t null_str_len = static_cast<int32_t>(strlen(null_str));
  if (OB_FAIL(table_name_str.append_fmt(lib::is_oracle_mode() ? "\"%.*s\".\"%.*s\"" :
                                                                  "`%.*s`.`%.*s`",
                                        param.stat_own_.length(), param.stat_own_.ptr(),
                                        param.stat_tab_.length(), param.stat_tab_.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(table_name_str), K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(INSERT_STAT_TABLE,
                                        table_name_str.string().length(),
                                        table_name_str.string().ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(gen_part_and_subpart_sel_str(param, part_str, subpart_str))) {
    LOG_WARN("failed to gen part and subpart sel str", K(ret), K(part_str), K(subpart_str));
  } else if (OB_FAIL(raw_sql.append_fmt(FETCH_TABLE_STAT,
                                        param.is_index_stat_ ? 'I' : 'T',
                                        param.tab_name_.length(), param.tab_name_.ptr(),
                                        part_str.string().length(), part_str.string().ptr(),
                                        subpart_str.string().length(), subpart_str.string().ptr(),
                                        param.is_index_stat_ ? data_name.length() : null_str_len,
                                        param.is_index_stat_ ? data_name.ptr() : null_str,
                                        param.db_name_.length(), param.db_name_.ptr(),
                                        from_table_name,
                                        valid_tab_id))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(delete_stat_sql.append_fmt(DELETE_STAT_TABLE,
                                                table_name_str.string().length(),
                                                table_name_str.string().ptr(),
                                                param.db_name_.length(), param.db_name_.ptr(),
                                                param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
  } else if (!param.part_name_.empty()) {
    ObSEArray<int64_t, 4> partition_ids;
    ObSqlString partition_list;
    if (param.is_subpart_name_) {
      if (OB_UNLIKELY(param.part_infos_.count() != 1 && param.subpart_infos_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param.part_infos_.count()),
                                         K(param.subpart_infos_.count()));
      } else if (OB_FAIL(delete_stat_sql.append_fmt(" and c3 = '%.*s' and type = '%c';",
                                                    param.part_name_.length(),
                                                    param.part_name_.ptr(),
                                                    param.is_index_stat_ ? 'I' : 'T'))) {
        LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
      } else if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(0).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    } else {
      if (OB_UNLIKELY(param.part_infos_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param.part_infos_.count()));
      } else if (OB_FAIL(delete_stat_sql.append_fmt(" and c2 = '%.*s' and type = '%c';",
                                                    param.part_name_.length(),
                                                    param.part_name_.ptr(),
                                                    param.is_index_stat_ ? 'I' : 'T'))) {
        LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
      } else if (OB_FAIL(partition_ids.push_back(param.part_infos_.at(0).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
          if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing */}
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOptStatSqlService::generate_in_list(partition_ids, partition_list))) {
      LOG_WARN("failed to generate in list", K(ret));
    } else if (OB_FAIL(raw_sql.append_fmt(" and PARTITION_ID in %s;", partition_list.ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
    } else {/*do nothing*/}
  } else if (OB_FAIL(raw_sql.append(";"))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(delete_stat_sql.append_fmt(" and type = '%c';",
                                                param.is_index_stat_ ? 'I' : 'T'))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else {/*do nothing*/}
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_execute_sql(ctx, param.tenant_id_, delete_stat_sql, raw_sql))) {
    LOG_WARN("fail to do execute sql.", K(raw_sql), K(ret));
  } else if (param.cascade_ && OB_FAIL(export_column_stats(ctx, param))) {
    LOG_WARN("failed to export column stats", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsExportImport::export_column_stats(ObExecContext &ctx, const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString where_str;
  ObSqlString delete_where_str;
  ObSqlString delete_stat_sql;
  ObSqlString part_str;
  ObSqlString subpart_str;
  ObSqlString col_str;
  ObSqlString table_name_str;
  const uint64_t tenant_id = param.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t valid_tab_id = lib::is_oracle_mode() ? param.table_id_ :
                              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, param.table_id_);
  const char *col_stat_table = lib::is_oracle_mode() ? "sys.ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT" :
                                                         "oceanbase.__all_column_stat";
  const char *hist_stat_table = lib::is_oracle_mode() ? "sys.ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT" :
                                                          "oceanbase.__all_histogram_stat";
  if (!param.part_name_.empty()) {//specify part name
    ObSEArray<int64_t, 4> partition_ids;
    ObSqlString partition_list;
    if (param.is_subpart_name_) {
      if (OB_UNLIKELY(param.part_infos_.count() != 1 && param.subpart_infos_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param.part_infos_.count()),
                                         K(param.subpart_infos_.count()));
      } else if (OB_FAIL(delete_where_str.append_fmt(" and c3 = '%.*s'",
                                                     param.part_name_.length(),
                                                     param.part_name_.ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
      } else if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(0).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    } else {
      if (OB_UNLIKELY(param.part_infos_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param.part_infos_.count()));
      } else if (OB_FAIL(delete_where_str.append_fmt(" and c2 = '%.*s'",
                                                     param.part_name_.length(),
                                                     param.part_name_.ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
      } else if (OB_FAIL(partition_ids.push_back(param.part_infos_.at(0).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
          if (OB_FAIL(partition_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing */}
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptStatSqlService::generate_in_list(partition_ids, partition_list))) {
        LOG_WARN("failed to generate in list", K(ret));
      } else if (OB_FAIL(where_str.append_fmt(" and stat.PARTITION_ID in %s", partition_list.ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
      } else {/*do nothing*/}
    }
  }
  if (OB_SUCC(ret) && param.column_params_.count() == 1) {//specify column name
    if (OB_FAIL(where_str.append_fmt(" and stat.COLUMN_ID = %lu",
                                     param.column_params_.at(0).column_id_))) {
      LOG_WARN("fail to append SQL stmt string.", K(where_str), K(ret));
    } else if (OB_FAIL(delete_where_str.append_fmt(" and c4 = '%.*s'",
                                                  param.column_params_.at(0).column_name_.length(),
                                                  param.column_params_.at(0).column_name_.ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_name_str.append_fmt(lib::is_oracle_mode() ? "\"%.*s\".\"%.*s\"" :
                                                                  "`%.*s`.`%.*s`",
                                          param.stat_own_.length(), param.stat_own_.ptr(),
                                          param.stat_tab_.length(), param.stat_tab_.ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(table_name_str), K(ret));
    } else if (OB_FAIL(raw_sql.append_fmt(INSERT_STAT_TABLE,
                                          table_name_str.string().length(),
                                          table_name_str.string().ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
    } else if (OB_FAIL(gen_part_and_subpart_sel_str(param, part_str, subpart_str))) {
      LOG_WARN("failed to gen part and subpart sel str", K(ret), K(part_str), K(subpart_str));
    } else if (OB_FAIL(gen_col_sel_str(param, col_str))) {
      LOG_WARN("failed to gen part and subpart sel str", K(ret), K(col_str));
    } else if (OB_FAIL(raw_sql.append_fmt(FETCH_COLUMN_STAT,
                                          param.tab_name_.length(), param.tab_name_.ptr(),
                                          part_str.string().length(), part_str.string().ptr(),
                                          subpart_str.string().length(), subpart_str.string().ptr(),
                                          col_str.string().length(), col_str.string().ptr(),
                                          param.db_name_.length(), param.db_name_.ptr(),
                                          col_stat_table,
                                          hist_stat_table,
                                          valid_tab_id))) {
      LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
    } else if (OB_FAIL(delete_stat_sql.append_fmt(DELETE_STAT_TABLE,
                                                  table_name_str.string().length(),
                                                  table_name_str.string().ptr(),
                                                  param.db_name_.length(), param.db_name_.ptr(),
                                                  param.tab_name_.length(), param.tab_name_.ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
    } else if (OB_FAIL(raw_sql.append(where_str.string()))) {
      LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
    } else if (OB_FAIL(delete_stat_sql.append(delete_where_str.string()))) {
      LOG_WARN("fail to append SQL stmt string.", K(delete_stat_sql), K(ret));
    } else if (OB_FAIL(raw_sql.append(";"))) {
      LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
    } else if (OB_FAIL(delete_stat_sql.append(" and type = 'C';"))) {
      LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
    } else if (OB_FAIL(do_execute_sql(ctx, param.tenant_id_, delete_stat_sql, raw_sql))) {
      LOG_WARN("fail to do execute sql.", K(raw_sql), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsExportImport::import_table_stats(ObExecContext &ctx, const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString partition_list;
  ObSqlString column_list;
  ObSqlString table_name_str;
  const char *histflag = lib::is_oracle_mode() ? "bitand(flags, 29700) histflg" :
                                                   "cast(flags&29700 as decimal) histflg";
  if (OB_FAIL(table_name_str.append_fmt(lib::is_oracle_mode() ? "\"%.*s\".\"%.*s\"" :
                                                                  "`%.*s`.`%.*s`",
                                          param.stat_own_.length(), param.stat_own_.ptr(),
                                          param.stat_tab_.length(), param.stat_tab_.ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(table_name_str), K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(FETCH_STAT_FROM_STAT_TABLE,
                                        histflag,
                                        table_name_str.string().length(),
                                        table_name_str.string().ptr(),
                                        param.db_name_.length(), param.db_name_.ptr(),
                                        param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (!param.cascade_ && OB_FAIL(raw_sql.append_fmt(" and type = '%c'",
                                                           param.is_index_stat_ ? 'I' : 'T'))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (param.cascade_ && OB_FAIL(gen_import_column_list(param.column_params_, column_list))) {
    LOG_WARN("failed to gen import partition list", K(ret));
  } else if (param.cascade_ &&
             OB_FAIL(raw_sql.append_fmt(" and (type = 'T' or type = 'I' or (type = 'C' and c4 in %s))",
                                        column_list.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if  (OB_FAIL(gen_import_partition_list(param, partition_list))) {
    LOG_WARN("failed to gen import partition list", K(ret));
  } else if (OB_UNLIKELY(partition_list.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(partition_list), K(param));
  } else if (OB_FAIL(raw_sql.append_fmt(" and (%s) order by c5, c1, c2, c3, c4, n10;",
                                        partition_list.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(do_import_stats(ctx, param, raw_sql))) {
    LOG_WARN("failed to do execute sql", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsExportImport::import_column_stats(ObExecContext &ctx, const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  ObSqlString partition_list;
  ObSqlString column_list;
  ObSqlString table_name_str;
  const char *histflag = lib::is_oracle_mode() ? "bitand(flags, 29700) histflg" :
                                                   "cast(flags&29700 as decimal) histflg";
  if (OB_FAIL(table_name_str.append_fmt(lib::is_oracle_mode() ? "\"%.*s\".\"%.*s\"" :
                                                                  "`%.*s`.`%.*s`",
                                          param.stat_own_.length(), param.stat_own_.ptr(),
                                          param.stat_tab_.length(), param.stat_tab_.ptr()))) {
      LOG_WARN("fail to append SQL stmt string.", K(table_name_str), K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(FETCH_STAT_FROM_STAT_TABLE,
                                        histflag,
                                        table_name_str.string().length(),
                                        table_name_str.string().ptr(),
                                        param.db_name_.length(), param.db_name_.ptr(),
                                        param.tab_name_.length(), param.tab_name_.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(raw_sql.append(" and type = 'C'"))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(gen_import_column_list(param.column_params_, column_list))) {
    LOG_WARN("failed to gen import partition list", K(ret));
  } else if (OB_FAIL(raw_sql.append_fmt(" and c4 in %s", column_list.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if  (OB_FAIL(gen_import_partition_list(param, partition_list))) {
    LOG_WARN("failed to gen import partition list", K(ret));
  } else if (OB_UNLIKELY(partition_list.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(partition_list), K(param));
  } else if (OB_FAIL(raw_sql.append_fmt(" and (%s) order by c5, c1, c2, c3, c4, n10;",
                                        partition_list.ptr()))) {
    LOG_WARN("fail to append SQL stmt string.", K(raw_sql), K(ret));
  } else if (OB_FAIL(do_import_stats(ctx, param, raw_sql))) {
    LOG_WARN("failed to do execute sql", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObDbmsStatsExportImport::do_execute_sql(ObExecContext &ctx,
                                            uint64_t tenant_id,
                                            const ObSqlString &raw_sql)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObOracleSqlProxy oracle_proxy;
  ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(oracle_proxy.init(ctx.get_sql_proxy()->get_pool()))) {
      LOG_WARN("failed to init oracle proxy", K(ret));
    } else {
      sql_proxy = &oracle_proxy;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_proxy->write(tenant_id, raw_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(raw_sql), K(ret));
  } else {
    LOG_TRACE("Succeed to do execute sql", K(raw_sql));
  }
  return ret;
}

int ObDbmsStatsExportImport::do_execute_sql(ObExecContext &ctx,
                                            uint64_t tenant_id,
                                            const ObSqlString &delete_stat_sql,
                                            const ObSqlString &fetch_stat_sql)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
  common::ObOracleSqlProxy oracle_proxy;
  int64_t affected_rows = 0;
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(oracle_proxy.init(ctx.get_sql_proxy()->get_pool()))) {
      LOG_WARN("failed to init oracle proxy", K(ret));
    } else {
      sql_proxy = &oracle_proxy;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.start(sql_proxy, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, delete_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(delete_stat_sql), K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, fetch_stat_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(fetch_stat_sql), K(ret));
    } else {/*do nothing*/}
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::do_import_stats(ObExecContext &ctx,
                                             const ObTableStatParam &param,
                                             const ObSqlString &raw_sql)
{
  int ret = OB_SUCCESS;
  common::ObOracleSqlProxy oracle_proxy;
  ObArray<ObOptTableStat *> all_tstats;
  ObArray<ObOptColumnStat *> all_cstats;
  ObCommonSqlProxy *sql_proxy = ctx.get_sql_proxy();
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(oracle_proxy.init(ctx.get_sql_proxy()->get_pool()))) {
      LOG_WARN("failed to init oracle proxy", K(ret));
    } else {
      sql_proxy = &oracle_proxy;
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_UNLIKELY(raw_sql.empty()) || OB_ISNULL(param.allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected empty", K(ret), K(raw_sql), K(param));
      } else if (OB_FAIL(sql_client_retry_weak.read(proxy_result,
                                                    param.tenant_id_, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        bool is_index_stat = false;
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ObSEArray<ObObj, 31> result_objs;
          for (int64_t i = 0; OB_SUCC(ret) && i < StatTableColumnName::MAX_COL; ++i) {
            ObObj tmp;
            ObObj val;
            if (OB_FAIL(client_result->get_obj(i, tmp))) {
              LOG_WARN("failed to get object", K(ret));
            } else if (OB_FAIL(ob_write_obj(*param.allocator_, tmp, val))) {
              LOG_WARN("failed to write object", K(ret));
            } else if (OB_FAIL(result_objs.push_back(val))) {
              LOG_WARN("failed to add result", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(get_opt_stat(ctx, param, result_objs, is_index_stat,
                                     all_tstats, all_cstats))) {
              LOG_WARN("failed to get stat", K(ret));
            } else {/*do nothing*/}
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else if (OB_FAIL(check_col_stat_validity(all_cstats))) {
          LOG_WARN("failed to check col stat validity", K(ret));
        } else {
          //before import, we need record history stats.
          ObMySQLTransaction trans;
          //begin trans
          if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
            LOG_WARN("fail to start transaction", K(ret));
          } else if (!is_index_stat && !all_tstats.empty() && !param.is_temp_table_ &&
                     OB_FAIL(ObDbmsStatsHistoryManager::backup_opt_stats(ctx, trans, param, ObTimeUtility::current_time()))) {
            LOG_WARN("failed to get history stat handles", K(ret));
          } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, trans.get_connection(), all_tstats, all_cstats, is_index_stat))) {
            LOG_WARN("failed to split batch write", K(ret));
          }
          //end trans
          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans.end(true))) {
              LOG_WARN("fail to commit transaction", K(ret));
            }
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
              LOG_WARN("fail to roll back transaction", K(tmp_ret));
            }
          }
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

static int get_nmb_val(const ObObj &obj, ObIAllocator &allocator, number::ObNumber &nmb_val)
{
  int ret = OB_SUCCESS;
  if (obj.is_decimal_int()) {
    if (OB_FAIL(wide::to_number(obj.get_decimal_int(), obj.get_int_bytes(), obj.get_scale(), allocator, nmb_val))) {
      LOG_WARN("cast to number failed", K(ret));
    }
  } else if (OB_FAIL(obj.get_number(nmb_val))) {
    LOG_WARN("get number failed", K(ret));
  }
  return ret;
}

/*@brief ObDbmsStatsExportImport::get_opt_stat is used to get stat from stat table
 * stat table:
 *      column_name                         stat_info
 *   0.STATID VARCHAR2(30)     <==>         NULL
 *   1.TYPE CHAR(1)            <==>         stat type('T' is stand for table stat and 'C' )
 *   2.VERSION NUMBER          <==>         NULL
 *   3.FLAGS NUMBER            <==>         mark hist type('C')
 *   4.C1 VARCHAR2(30)         <==>         table name
 *   5.C2 VARCHAR2(30)         <==>         partition name
 *   6.C3 VARCHAR2(30)         <==>         subpartition name
 *   7.C4 VARCHAR2(30)         <==>         column name('C')
 *   8.C5 VARCHAR2(30)         <==>         Owner
 *   9.C6 VARCHAR2(30)         <==>         NULL
 *   10.N1 NUMBER              <==>         Num rows('T') or Num distinct('C')
 *   11.N2 NUMBER              <==>         Blocks('T') or Density('C')
 *   12.N3 NUMBER              <==>         Average row length('T') or compress_type('C')
 *   13.N4 NUMBER              <==>         Sample size
 *   14.N5 NUMBER              <==>         Num nulls('C')
 *   15.N6 NUMBER              <==>         Lower value('C')
 *   16.N7 NUMBER              <==>         high value('C')
 *   17.N8 NUMBER              <==>         Average column length('C')
 *   18.N9 NUMBER              <==>         1('C')
 *   19.N10 NUMBER             <==>         Endpoint number('C')
 *   20.N11 NUMBER             <==>         Endpoint value('C')
 *   21.N12 NUMBER             <==>         ENDPOINT_REPEAT_COUNT('C')
 *   22.N13 NUMBER             <==>         bucket_cnt('C')
 *   23.D1 DATE                <==>         Last analyzed
 *   24.T1 TIMESTAMP(6) WITH TIME ZONE <==> NULL
 *   25.R1 RAW(32)             <==>         Lower raw value('C')
 *   26.R2 RAW(32)             <==>         High raw value('C')
 *   27.R3 RAW(32)             <==>         ENDPOINT_ACTUAL_VALUE_RAW('C')
 *   28.CH1 VARCHAR2(1000)     <==>         NULL
 *   29.CL1 CLOB               <==>         NULL
 *   30.BL1 BLOB               <==>         NULL
 *   31.OB_SPEC1 NUMBER        <==>         ob macro block cnt('T') or ob llc bitmap size('C')
 *   32.OB_SPEC2 NUMBER        <==>         ob micro block cnt('T')
 *   33.OB_SPEC3 VARCHAR2(4096)<==>         ob llc bitmap('C') ==> ndv synopsis
 */
int ObDbmsStatsExportImport::get_opt_stat(ObExecContext &ctx,
                                          const ObTableStatParam &param,
                                          const ObIArray<ObObj> &result_objs,
                                          bool &is_index_stat,
                                          ObIArray<ObOptTableStat *> &all_tstats,
                                          ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  is_index_stat = false;
  if (OB_UNLIKELY(result_objs.count() != StatTableColumnName::MAX_COL) ||
      OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(result_objs.count()), K(param));
  } else {
    ObOptTableStat *tbl_stat = NULL;
    ObOptColumnStat *col_stat = NULL;
    StatType stat_type = INVALID_STAT_TYPE;
    ObHistType hist_type = INVALID_TYPE;
    ObString part_str;
    ObString subpart_str;
    ObString col_str;
    ObHistBucket hist_bucket;
    int64_t llc_bitmap_size = 0;
    int64_t compress_type = ObOptStatCompressType::MAX_COMPRESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < StatTableColumnName::MAX_COL; ++i) {
      if (OB_UNLIKELY(i >= StatTableColumnName::N1 &&
                     (stat_type == INVALID_STAT_TYPE ||
                      (stat_type == TABLE_STAT && tbl_stat == NULL) ||
                      (stat_type == INDEX_STAT && tbl_stat == NULL) ||
                      (stat_type == COLUMN_STAT && col_stat == NULL)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(i), K(stat_type), K(tbl_stat), K(col_stat), K(ret));
      } else {
        switch (i) {
          case StatTableColumnName::STATID: {/*no used*/
            break;
          }
          case StatTableColumnName::TYPE: {//stat type
            ObString type_str;
            if (!result_objs.at(i).is_null() && OB_FAIL(result_objs.at(i).get_varchar(type_str))) {
              LOG_WARN("failed to get varchar", K(ret));
            } else if (0 == type_str.case_compare("T")) {
              stat_type = TABLE_STAT;
            } else if (0 == type_str.case_compare("I")) {
              stat_type = INDEX_STAT;
              is_index_stat = true;
            } else if (0 == type_str.case_compare("C")) {
              stat_type = COLUMN_STAT;
            } else {
              ret = OB_ERR_DBMS_STATS_PL;
              LOG_WARN("Invalid or inconsistent input values", K(ret), K(type_str));
              LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
            }
            break;
          }
          case StatTableColumnName::VERSION: {//check validity
            number::ObNumber num_val;
            int64_t int_val = 0;
            const int64_t valid_num = 8;
            ObNumStackOnceAlloc tmp_alloc;
            if (!result_objs.at(i).is_null()
                && OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (int_val != valid_num) {
              ret = OB_ERR_DBMS_STATS_PL;
              LOG_WARN("Invalid or inconsistent input values", K(ret), K(int_val), K(valid_num));
              LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
            }
            break;
          }
          case StatTableColumnName::FLAGS: {//mark hist type('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (!result_objs.at(i).is_null()
                && OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret), K(result_objs.at(i)));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (stat_type == COLUMN_STAT && int_val != 0) {
              if ((int_val & 4096) && (int_val & 4)) {
                hist_type = ObHistType::FREQUENCY;
              } else if ((int_val & 8192) && (int_val & 4)) {
                hist_type = ObHistType::TOP_FREQUENCY;
              } else if (int_val & 4) {
                hist_type = ObHistType::HYBIRD;
              } else {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(int_val));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            }
            break;
          }
          case StatTableColumnName::C1: {//table name
            ObString tbl_name;
            if (!result_objs.at(i).is_null() && OB_FAIL(result_objs.at(i).get_varchar(tbl_name))) {
              LOG_WARN("failed to get varchar", K(ret));
            } else if (0 != tbl_name.case_compare(param.tab_name_)) {
              ret = OB_ERR_DBMS_STATS_PL;
              LOG_WARN("Invalid or inconsistent input values", K(ret), K(tbl_name));
              LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
            }
            break;
          }
          case StatTableColumnName::C2: {//partition name
            if (!result_objs.at(i).is_null() && OB_FAIL(result_objs.at(i).get_varchar(part_str))) {
              LOG_WARN("failed to get varchar", K(ret));
            }
            break;
          }
          case StatTableColumnName::C3: {//subpartition name
            if (!result_objs.at(i).is_null() &&
                OB_FAIL(result_objs.at(i).get_varchar(subpart_str))) {
              LOG_WARN("failed to get varchar", K(ret));
            }
            break;
          }
          case StatTableColumnName::C4: {//column name
            if (!result_objs.at(i).is_null() && OB_FAIL(result_objs.at(i).get_varchar(col_str))) {
              LOG_WARN("failed to get varchar", K(ret));
            }
            break;
          }
          case StatTableColumnName::C5: {//Owner
            ObString owner_str;
            if (!result_objs.at(i).is_null() && OB_FAIL(result_objs.at(i).get_varchar(owner_str))) {
              LOG_WARN("failed to get varchar", K(ret));
            } else if (OB_UNLIKELY(0 != owner_str.case_compare(param.db_name_))) {
              ret = OB_ERR_DBMS_STATS_PL;
              LOG_WARN("Invalid or inconsistent input values", K(ret), K(owner_str));
              LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
            } else if (OB_FAIL(init_opt_stat(ctx, param, stat_type, part_str, subpart_str, col_str,
                                             tbl_stat, col_stat, all_tstats, all_cstats))) {
              LOG_WARN("failed to init stat", K(ret));
            } else {/*do nothing*/}
            break;
          }
          case StatTableColumnName::C6: {/*no used*/
            break;
          }
          case StatTableColumnName::N1: {//Num rows('T'/'I') or Num distinct('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (!result_objs.at(i).is_null()
                && OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              tbl_stat->set_row_count(int_val);
            } else if (stat_type == COLUMN_STAT) {
              col_stat->set_num_distinct(int_val);
            }
            break;
          }
          case StatTableColumnName::N2: {//Blocks('T'/'I') or Density('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            double double_val = 0.0;
            ObNumStackOnceAlloc tmp_alloc;
            if ((result_objs.at(i).is_number() || result_objs.at(i).is_decimal_int()) &&
                (OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val)) ||
                 OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(num_val, double_val)))) {
              LOG_WARN("failed to get double", K(ret));
            } else if (result_objs.at(i).is_double() && OB_FAIL(result_objs.at(i).get_double(double_val))) {
              LOG_WARN("failed to get double", K(ret));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              /*do nothing*/
            } else if (stat_type == COLUMN_STAT) {
              if (double_val > 0.0) {
                /*do nothing*/
                if (OB_UNLIKELY(hist_type == INVALID_TYPE)) {
                  ret = OB_ERR_DBMS_STATS_PL;
                  LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                  LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
                } else {
                  col_stat->get_histogram().set_density(double_val);
                  col_stat->get_histogram().set_type(hist_type);
                }
              }
            }
            break;
          }
          case StatTableColumnName::N3: {//Average row length('T'/'I') or compress_type('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (stat_type == COLUMN_STAT) {
              if (result_objs.at(i).is_null()) {
                compress_type = ObOptStatCompressType::ZLIB_COMPRESS;
              } else if (OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
                LOG_WARN("failed to get number", K(ret));
              } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(compress_type))) {
                LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(compress_type));
              }
            } else if (!result_objs.at(i).is_null() &&
                       OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else {
              tbl_stat->set_avg_row_size(int_val);
            }
            break;
          }
          case StatTableColumnName::N4: {//Sample size
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (!result_objs.at(i).is_null()
                && OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              /*do nothing*/
            } else if (stat_type == COLUMN_STAT && int_val > 0) {
              col_stat->get_histogram().set_sample_size(int_val);
            }
            break;
          }
          case StatTableColumnName::N5: {//Num nulls('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (!result_objs.at(i).is_null() &&
                       OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else {
              col_stat->set_num_null(int_val);
            }
            break;
          }
          case StatTableColumnName::N6: {//low value('C')(not used)
            break;
          }
          case StatTableColumnName::N7: {//high value('C')(not used)
            break;
          }
          case StatTableColumnName::N8: {//Average column length('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (!result_objs.at(i).is_null() &&
                       OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else {
              col_stat->set_avg_len(int_val);
            }
            break;
          }
          case StatTableColumnName::N9: {//not used
            break;
          }
          case StatTableColumnName::N10: {//Endpoint number('C')
            number::ObNumber num_val;
            int64_t int_val = -1;
            ObNumStackOnceAlloc tmp_alloc;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (!result_objs.at(i).is_null() &&
                       OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else {
              hist_bucket.endpoint_num_ = int_val;
            }
            break;
          }
          case StatTableColumnName::N11: {//Endpoint value('C')(not used)
            break;
          }
          case StatTableColumnName::N12: {//ENDPOINT_REPEAT_COUNT('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (!result_objs.at(i).is_null() &&
                       OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else {
              hist_bucket.endpoint_repeat_count_ = int_val;
            }
            break;
          }
          case StatTableColumnName::N13: {//bucket_cnt('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (!result_objs.at(i).is_null() &&
                       OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (int_val > 0) {
              if (OB_UNLIKELY(col_stat->get_histogram().get_density() < 0.0)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected error", K(result_objs), K(ret), KPC(col_stat));
              } else if (col_stat->get_histogram().get_buckets().empty()) {
                if (OB_FAIL(col_stat->get_histogram().prepare_allocate_buckets(*param.allocator_,
                                                                               int_val))) {
                  LOG_WARN("failed to prepare allocate buckets", K(ret));
                } else {/*do nothing*/}
              }
            }
            break;
          }
          case StatTableColumnName::D1: {//Last analyzed
            int64_t last_date = 0;
            if (!result_objs.at(i).is_null() &&
                lib::is_oracle_mode() &&
                OB_FAIL(result_objs.at(i).get_datetime(last_date))) {
              LOG_WARN("failed to get date", K(ret), K(result_objs.at(i)));
            } else if (!result_objs.at(i).is_null() &&
                       lib::is_mysql_mode() &&
                       OB_FAIL(result_objs.at(i).get_timestamp(last_date))) {
              LOG_WARN("failed to get timestamp", K(ret), K(result_objs.at(i).get_type()));
            } else if (lib::is_oracle_mode() &&
                       OB_FAIL(ObTimeConverter::datetime_to_timestamp(last_date,
                                                         get_timezone_info(ctx.get_my_session()),
                                                         last_date))) {
              LOG_WARN("fail to convert datetime", K(ret));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              tbl_stat->set_last_analyzed(last_date);
            } else if (stat_type == COLUMN_STAT) {
              col_stat->set_last_analyzed(last_date);
            }
            break;
          }
          case StatTableColumnName::T1: {//not used
            break;
          }
          case StatTableColumnName::R1: {//Lower raw value('C')
            ObObj min_obj;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (lib::is_oracle_mode() &&
                       OB_FAIL(convert_bin_hex_raw_to_obj(*param.allocator_,
                                                          result_objs.at(i),
                                                          min_obj))) {
              LOG_WARN("failed to convert bin hex raw to obj", K(ret));
            } else if (lib::is_mysql_mode() &&
                       OB_FAIL(convert_bin_hex_text_to_obj(*param.allocator_,
                                                           result_objs.at(i),
                                                           min_obj))) {
              LOG_WARN("failed to convert bin hex text to obj", K(ret));
            } else {
              col_stat->set_min_value(min_obj);
            }
            break;
          }
          case StatTableColumnName::R2: {//High raw value('C')
            ObObj max_obj;
            if (stat_type != COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (lib::is_oracle_mode() &&
                       OB_FAIL(convert_bin_hex_raw_to_obj(*param.allocator_,
                                                          result_objs.at(i),
                                                          max_obj))) {
              LOG_WARN("failed to convert bin hex raw to obj", K(ret));
            } else if (lib::is_mysql_mode() &&
                       OB_FAIL(convert_bin_hex_text_to_obj(*param.allocator_,
                                                           result_objs.at(i),
                                                           max_obj))) {
              LOG_WARN("failed to convert bin hex text to obj", K(ret));
            } else {
              col_stat->set_max_value(max_obj);
            }
            break;
          }
          case StatTableColumnName::R3: {//ENDPOINT_ACTUAL_VALUE_RAW('C')
            if (stat_type != COLUMN_STAT || result_objs.at(i).is_null()) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            } else if (lib::is_oracle_mode() &&
                       OB_FAIL(convert_bin_hex_raw_to_obj(*param.allocator_,
                                                          result_objs.at(i),
                                                          hist_bucket.endpoint_value_))) {
              LOG_WARN("failed to convert bin hex raw to obj", K(ret));
            } else if (lib::is_mysql_mode() &&
                       OB_FAIL(convert_bin_hex_text_to_obj(*param.allocator_,
                                                           result_objs.at(i),
                                                           hist_bucket.endpoint_value_))) {
              LOG_WARN("failed to convert bin hex text to obj", K(ret));
            } else if (OB_UNLIKELY(col_stat->get_histogram().get_bucket_cnt() >=
                                                 col_stat->get_histogram().get_bucket_size())) {
              ret = OB_ERR_DBMS_STATS_PL;
              LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
              LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
            } else if (OB_FAIL(col_stat->get_histogram().add_bucket(hist_bucket))) {
              LOG_WARN("failed to push back", K(ret));
            } else {/*do nothing*/}
            break;
          }
          case StatTableColumnName::CH1:
          case StatTableColumnName::CL1:
          case StatTableColumnName::BL1: {/*no used*/
            break;
          }
          case StatTableColumnName::OB_SPEC1: {//ob macro block cnt('T') or ob llc bitmap size('C')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (!result_objs.at(i).is_null()
                && OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              tbl_stat->set_macro_block_num(int_val);
            } else if (stat_type == COLUMN_STAT) {
              llc_bitmap_size = int_val;
            }
            break;
          }
          case StatTableColumnName::OB_SPEC2: {//ob micro block cnt('T')
            number::ObNumber num_val;
            int64_t int_val = 0;
            ObNumStackOnceAlloc tmp_alloc;
            if (!result_objs.at(i).is_null()
                && OB_FAIL(get_nmb_val(result_objs.at(i), tmp_alloc, num_val))) {
              LOG_WARN("failed to get number", K(ret));
            } else if (OB_FAIL(num_val.extract_valid_int64_with_trunc(int_val))) {
              LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num_val));
            } else if (stat_type == TABLE_STAT || stat_type == INDEX_STAT) {
              tbl_stat->set_micro_block_num(int_val);
            } else if (stat_type == COLUMN_STAT) {
              if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              }
            }
            break;
          }
          case StatTableColumnName::OB_SPEC3: {//ob llc bitmap('C')
            ObString hex_str;
            if (!result_objs.at(i).is_null() && OB_FAIL(result_objs.at(i).get_varchar(hex_str))) {
              LOG_WARN("failed to get varchar", K(ret));
            } else if (stat_type == COLUMN_STAT && llc_bitmap_size > 0) {
              char *bitmap_buf = NULL;
              if (OB_UNLIKELY(compress_type < 0 || compress_type >= ObOptStatCompressType::MAX_COMPRESS)) {
                ret = OB_ERR_DBMS_STATS_PL;
                LOG_WARN("Invalid or inconsistent input values", K(ret), K(compress_type));
                LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
              } else if (OB_ISNULL(bitmap_buf = static_cast<char*>(param.allocator_->alloc(hex_str.length())))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("allocate memory for llc_bitmap failed.", K(hex_str.length()), K(ret));
              } else {
                common::str_to_hex(hex_str.ptr(), hex_str.length(), bitmap_buf, hex_str.length());
                // decompress llc bitmap;
                char *decomp_buf = NULL ;
                int64_t decomp_size = ObOptColumnStat::NUM_LLC_BUCKET;
                const int64_t bitmap_size = hex_str.length() / 2;
                if (OB_FAIL(ObOptStatSqlService::get_decompressed_llc_bitmap(*param.allocator_,
                                                                             bitmap_compress_lib_name[compress_type],
                                                                             bitmap_buf,
                                                                             bitmap_size,
                                                                             decomp_buf,
                                                                             decomp_size))) {
                  COMMON_LOG(WARN, "decompress bitmap buffer failed.", K(ret));
                } else {
                  col_stat->set_llc_bitmap(decomp_buf, decomp_size);
                }
              }
            } else if (OB_UNLIKELY(!result_objs.at(i).is_null())) {
              ret = OB_ERR_DBMS_STATS_PL;
              LOG_WARN("Invalid or inconsistent input values", K(ret), K(result_objs.at(i)));
              LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(i), K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::init_opt_stat(ObExecContext &ctx,
                                           const ObTableStatParam &param,
                                           const StatType stat_type,
                                           const ObString &part_str,
                                           const ObString &subpart_str,
                                           const ObString &col_str,
                                           ObOptTableStat *&tbl_stat,
                                           ObOptColumnStat *&col_stat,
                                           ObIArray<ObOptTableStat *> &all_tstats,
                                           ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  StatLevel type = INVALID_LEVEL;
  int64_t part_id = -1;
  uint64_t column_id = 0;
  uint64_t stattype = StatTypeLocked::NULL_TYPE;
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (OB_FAIL(get_part_info(param, part_str, subpart_str, part_id, type, stattype))) {
    LOG_WARN("failed to get part info", K(ret));
  } else if (TABLE_STAT == stat_type || stat_type == INDEX_STAT) {
    if (OB_ISNULL(ptr = param.allocator_->alloc(sizeof(ObOptTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      tbl_stat = new (ptr) ObOptTableStat();
      tbl_stat->set_table_id(param.table_id_);
      tbl_stat->set_partition_id(part_id);
      tbl_stat->set_object_type(type);
      tbl_stat->set_stattype_locked(stattype);
      if (OB_FAIL(all_tstats.push_back(tbl_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  } else if (COLUMN_STAT == stat_type) {
    if (OB_FAIL(get_opt_col_stat(param, col_str, part_id, type, column_id,
                                 cs_type, col_stat, all_cstats))) {
      LOG_WARN("failed to get opt col stat", K(ret));
    } else if (col_stat != NULL) {//find already exists opt column stat
      /*do nothing*/
    } else if (OB_ISNULL(col_stat = ObOptColumnStat::malloc_new_column_stat(*param.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      col_stat->set_table_id(param.table_id_);
      col_stat->set_partition_id(part_id);
      col_stat->set_stat_level(type);
      col_stat->set_column_id(column_id);
      col_stat->set_collation_type(cs_type);
      if (OB_FAIL(all_cstats.push_back(col_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stat type", K(ret), K(stat_type));
  }
  return ret;
}

int ObDbmsStatsExportImport::get_part_info(const ObTableStatParam &param,
                                           const ObString &part_str,
                                           const ObString &subpart_str,
                                           int64_t &part_id,
                                           StatLevel &type,
                                           uint64_t &stattype)
{
  int ret = OB_SUCCESS;
  bool find_it = false;
  if (!subpart_str.empty()) {
    for (int64_t i = 0; !find_it && i < param.subpart_infos_.count(); ++i) {
      if (0 == subpart_str.case_compare(param.subpart_infos_.at(i).part_name_)) {
        find_it = true;
        part_id = param.subpart_infos_.at(i).part_id_;
        stattype = param.subpart_infos_.at(i).part_stattype_;
      }
    }
    if (!find_it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(find_it), K(subpart_str), K(param.subpart_infos_));
    } else {
      type = SUBPARTITION_LEVEL;
    }
  } else if (!part_str.empty()) {
    for (int64_t i = 0; !find_it && i < param.part_infos_.count(); ++i) {
      if (0 == part_str.case_compare(param.part_infos_.at(i).part_name_)) {
        find_it = true;
        part_id = param.part_infos_.at(i).part_id_;
        stattype = param.part_infos_.at(i).part_stattype_;
      }
    }
    if (!find_it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(find_it), K(part_str), K(param.part_infos_));
    } else {
      type = PARTITION_LEVEL;
    }
  } else {
    part_id = param.global_part_id_;
    type = TABLE_LEVEL;
    stattype = param.stattype_;
  }
  return ret;
}

int ObDbmsStatsExportImport::get_opt_col_stat(const ObTableStatParam &param,
                                              const ObString &col_str,
                                              const int64_t part_id,
                                              const StatLevel type,
                                              uint64_t &column_id,
                                              common::ObCollationType &cs_type,
                                              ObOptColumnStat *&col_stat,
                                              ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  bool find_it = false;
  for (int64_t i = 0; !find_it && i < param.column_params_.count(); ++i) {
    if (0 == col_str.case_compare(param.column_params_.at(i).column_name_)) {
      find_it = true;
      column_id = param.column_params_.at(i).column_id_;
      cs_type = param.column_params_.at(i).cs_type_;
    }
  }
  if (!find_it) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(find_it), K(col_str), K(param.column_params_));
  } else {
    find_it = false;
    for (int64_t i = 0; !find_it && i < all_cstats.count(); ++i) {
      if (OB_ISNULL(all_cstats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(all_cstats.at(i)));
      } else if (param.table_id_ == all_cstats.at(i)->get_table_id() &&
                 part_id == all_cstats.at(i)->get_partition_id() &&
                 type == all_cstats.at(i)->get_stat_level() &&
                 column_id == all_cstats.at(i)->get_column_id()) {
        find_it = true;
        col_stat = all_cstats.at(i);
      }
    }
  }
  return ret;
}


int ObDbmsStatsExportImport::gen_part_and_subpart_sel_str(const ObTableStatParam &param,
                                                          ObSqlString &part_str,
                                                          ObSqlString &subpart_str)
{
  int ret = OB_SUCCESS;
  if (param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO) {
    if (OB_FAIL(part_str.append("NULL"))) {
      LOG_WARN("fail to append SQL stmt string.", K(part_str), K(ret));
    } else if (OB_FAIL(subpart_str.append("NULL"))) {
      LOG_WARN("fail to append SQL stmt string.", K(subpart_str), K(ret));
    } else {/*do nothing*/}
  } else if (OB_UNLIKELY(param.part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.part_infos_.empty()));
  } else if (OB_FAIL(part_str.append("CASE stat.PARTITION_ID"))) {
    LOG_WARN("fail to append SQL stmt string.", K(part_str), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(part_str.append_fmt(" WHEN %ld THEN '%.*s'", param.part_infos_.at(i).part_id_,
                                                       param.part_infos_.at(i).part_name_.length(),
                                                       param.part_infos_.at(i).part_name_.ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(part_str), K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (param.subpart_infos_.empty()) {
        if (OB_FAIL(subpart_str.append("NULL"))) {
          LOG_WARN("fail to append SQL stmt string.", K(subpart_str), K(ret));
        } else {/*do nothing*/}
      } else if (OB_FAIL(subpart_str.append("CASE stat.PARTITION_ID"))) {
        LOG_WARN("fail to append SQL stmt string.", K(subpart_str), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
          if (OB_FAIL(subpart_str.append_fmt(" WHEN %ld THEN '%.*s'",
                                              param.subpart_infos_.at(i).part_id_,
                                              param.subpart_infos_.at(i).part_name_.length(),
                                              param.subpart_infos_.at(i).part_name_.ptr()))) {
            LOG_WARN("fail to append SQL stmt string.", K(subpart_str), K(ret));
          } else {
            ObString part_name;
            bool find_it = false;
            int64_t cur_part_id = param.subpart_infos_.at(i).first_part_id_;
            for (int64_t j = 0; !find_it && j < param.part_infos_.count(); ++j) {
              if (cur_part_id != param.part_infos_.at(j).part_id_) {
                // do nothing
              } else {
                part_name = param.part_infos_.at(j).part_name_;
                find_it = true;
              }
            }
            if (OB_SUCC(ret) && !find_it) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error, partition id isn't found", K(ret), K(cur_part_id));
            } else if (OB_FAIL(part_str.append_fmt(" WHEN %ld THEN '%.*s'",
                                                    param.subpart_infos_.at(i).part_id_,
                                                    part_name.length(),
                                                    part_name.ptr()))) {
              LOG_WARN("fail to append SQL stmt string.", K(part_str), K(ret));
            } else {/*do nothing*/}
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(subpart_str.append(" ELSE NULL END"))) {
            LOG_WARN("fail to append SQL stmt string.", K(subpart_str), K(ret));
          } else {/*do nothing*/}
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_str.append(" ELSE NULL END"))) {
        LOG_WARN("fail to append SQL stmt string.", K(part_str), K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::gen_col_sel_str(const ObTableStatParam &param,
                                             ObSqlString &col_str)
{
  int ret = OB_SUCCESS;
  ObSqlString col_sql_str;
  char *buf_col = NULL;
  int64_t buf_col_len = 0;
  if (OB_UNLIKELY(param.column_params_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.column_params_.empty()));
  } else if (OB_FAIL(col_str.append("CASE stat.COLUMN_ID"))) {
    LOG_WARN("fail to append SQL stmt string.", K(col_str), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
      if (OB_FAIL(col_str.append_fmt(" WHEN %lu THEN '%.*s'",
                                     param.column_params_.at(i).column_id_,
                                     param.column_params_.at(i).column_name_.length(),
                                     param.column_params_.at(i).column_name_.ptr()))) {
        LOG_WARN("fail to append SQL stmt string.", K(col_str), K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(col_str.append(" ELSE NULL END"))) {
        LOG_WARN("fail to append SQL stmt string.", K(col_str), K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::convert_bin_hex_raw_to_obj(ObIAllocator &allocator,
                                                        const ObObj &src_obj,
                                                        ObObj &dst_obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src_obj.is_raw())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(src_obj), K(src_obj.get_type()));
  } else {
    ObObj tmp_result;
    ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
    ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
    if (OB_FAIL(ObHexUtils::rawtohex(src_obj, cast_ctx, tmp_result))) {
      LOG_WARN("fail to calc", K(ret), K(src_obj));
    } else {
      ObString b_hex_str = tmp_result.get_string();
      int64_t pos = 0;
      char *buf = NULL;
      int64_t buf_len = b_hex_str.length() / 2 + 1;
      if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(buf), K(buf_len));
      } else if (OB_FAIL(hex_to_cstr(b_hex_str.ptr(), b_hex_str.length(), buf, buf_len))) {
        LOG_WARN("failed to hex_to_cstr", K(ret), K(b_hex_str));
      } else if (OB_FAIL(dst_obj.deserialize(buf, buf_len, pos))) {
        LOG_WARN("failed to deserialize", K(buf), K(buf_len), K(ret));
      } else {
        LOG_TRACE("Succeed to convert bin hex str to obj", K(src_obj), K(b_hex_str), K(dst_obj));
      }
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::convert_bin_hex_text_to_obj(ObIAllocator &allocator,
                                                        const ObObj &src_obj,
                                                        ObObj &dst_obj)
{
  int ret = OB_SUCCESS;
  ObString str;
  ObTextStringIter text_iter(src_obj);
  if (OB_UNLIKELY(!src_obj.is_text())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(src_obj), K(src_obj.get_type()));
  } else if (OB_FAIL(text_iter.init(0, nullptr, &allocator))) {
    LOG_WARN("failed to init text iter", K(ret), K(text_iter));
  } else if (OB_FAIL(text_iter.get_full_data(str))) {
    LOG_WARN("failed to get full string", K(ret), K(text_iter));
  } else if (OB_FAIL(ObOptStatSqlService::hex_str_to_obj(str.ptr(), str.length(), allocator, dst_obj))) {
    LOG_WARN("deserialize object value failed.", K(stat), K(ret));
  } else {
    LOG_TRACE("Succeed to convert bin test str to obj", K(src_obj), K(str), K(dst_obj));
  }
  return ret;
}

int ObDbmsStatsExportImport::gen_import_partition_list(const ObTableStatParam &param,
                                                       ObSqlString &partition_list)
{
  int ret = OB_SUCCESS;
  bool need_or = false;
  if (param.global_stat_param_.need_modify_) {
    if (OB_FAIL(partition_list.append("c2 is NULL"))) {
      LOG_WARN("failed to append", K(ret), K(partition_list));
    } else {
      need_or = true;
    }
  }

  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    if (need_or && !param.part_infos_.empty() && OB_FAIL(partition_list.append(" or "))) {
      LOG_WARN("failed to append sql", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      const char *prefix = (i == 0 ? "(c3 is NULL and c2 in (" : " ");
      const char *suffix = (i == param.part_infos_.count() - 1 ? "))" : ",");
      if (OB_FAIL(partition_list.append_fmt("%s'%.*s'%s",
                                            prefix,
                                            param.part_infos_.at(i).part_name_.length(),
                                            param.part_infos_.at(i).part_name_.ptr(),
                                            suffix))) {
        LOG_WARN("failed to append sql", K(ret), K(partition_list));
      } else {
        need_or = true;
      }
    }
  }

  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    if (need_or && !param.subpart_infos_.empty() && OB_FAIL(partition_list.append(" or "))) {
      LOG_WARN("failed to append sql", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      const char *prefix = (i == 0 ? "c3 in (" : " ");
      char suffix = (i == param.subpart_infos_.count() - 1 ? ')' : ',');
      if (OB_FAIL(partition_list.append_fmt("%s'%.*s'%c",
                                            prefix,
                                            param.subpart_infos_.at(i).part_name_.length(),
                                            param.subpart_infos_.at(i).part_name_.ptr(),
                                            suffix))) {
        LOG_WARN("failed to append sql", K(ret), K(partition_list));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::gen_import_column_list(const ObIArray<ObColumnStatParam> &column_param,
                                                    ObSqlString &column_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_param.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(column_param));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_param.count(); ++i) {
      const char *prefix = (i == 0 ? " (" : " ");
      char suffix = (i == column_param.count() - 1 ? ')' : ',');
      if (OB_FAIL(column_list.append_fmt("%s'%.*s'%c",
                                            prefix,
                                            column_param.at(i).column_name_.length(),
                                            column_param.at(i).column_name_.ptr(),
                                            suffix))) {
        LOG_WARN("failed to append sql", K(ret), K(column_list));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObDbmsStatsExportImport::check_col_stat_validity(ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_cstats.count(); ++i) {
    if (OB_ISNULL(all_cstats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(all_cstats.at(i)), K(ret));
    } else if (OB_UNLIKELY(all_cstats.at(i)->get_histogram().get_bucket_cnt() !=
                                  all_cstats.at(i)->get_histogram().get_bucket_size())) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Invalid or inconsistent input values", K(ret), KPC(all_cstats.at(i)));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "Invalid or inconsistent input values");
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
