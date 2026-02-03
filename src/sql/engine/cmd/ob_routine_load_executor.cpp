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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/cmd/ob_routine_load_executor.h"
#include "sql/resolver/cmd/ob_routine_load_stmt.h"
#include "storage/routine_load/ob_routine_load_table_operator.h"
#include "storage/routine_load/ob_routine_load_dbms_sched_util.h"
#include "sql/engine/table/ob_kafka_table_row_iter.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObCreateRoutineLoadExecutor::execute(ObExecContext &ctx, ObCreateRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = stmt.get_tenant_id();
  ObRoutineLoadJob routine_load_job;
  int64_t job_id = OB_INVALID_ID;
  int64_t create_time = ObTimeUtility::current_time();
  ObRLoadDynamicFields dynamic_fields;
  ObRoutineLoadTableOperator table_op;
  ObMySQLTransaction trans;
  common::ObCurTraceId::TraceId trace_id;
  if (nullptr != ObCurTraceId::get_trace_id()) {
    trace_id = *ObCurTraceId::get_trace_id();
  } else {
    trace_id.init(GCONF.self_addr_);
  }

  //STEP1: generate routine_load_job, and insert into __all_routine_load_job
  //NOTE: routine load job's job_id and job_name are the same as dbms sched job's
  if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("fail to generate job id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
    LOG_WARN("fail to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(routine_load_job.init(tenant_id, job_id,
                          stmt.get_job_name(), create_time,
                          OB_INVALID_TIMESTAMP, OB_INVALID_TIMESTAMP,
                          stmt.get_database_id(),
                          stmt.get_table_id(),
                          ObRoutineLoadJobState::RUNNING,
                          stmt.get_job_prop_json_str(),
                          dynamic_fields  /*empty*/,
                          //TODO: err infos
                          ObString::make_empty_string()  /*err_infos*/,
                          trace_id, OB_SUCCESS))) {
    LOG_WARN("fail to init routine load job", KR(ret), K(stmt), K(job_id), K(create_time), K(dynamic_fields));
  } else if (OB_FAIL(table_op.insert_routine_load_job(routine_load_job))) {
    LOG_WARN("fail to init routine load table op", KR(ret), K(routine_load_job));
  }

  //STEP2: generate dbms_sched_job, and insert into __all_tenant_scheduler_job
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    ObSqlString job_action;
    uint32_t par_str_pos = 0;
    uint32_t off_str_pos = 0;
    uint32_t par_str_len = 0;
    uint32_t off_str_len = 0;
    //generate job_action execute sql.
    if (OB_FAIL(build_sql_(job_id, stmt, sql, par_str_pos, off_str_pos, par_str_len, off_str_len))) {
      LOG_WARN("fail to build sql", KR(ret), K(job_id), K(stmt), K(sql));
    } else {
      //TODO: DEFAULT_BUF_LENGTH
      char sql_buf[DEFAULT_BUF_LENGTH] = {0};
      ObHexEscapeSqlStr sql_escaped(sql.string());
      int64_t sql_len = sql_escaped.to_string(sql_buf, sizeof(sql_buf));
      ObSqlString repeat_interval;
      int64_t repeat_interval_s = stmt.get_max_batch_interval_s() / 2;
      if (repeat_interval_s < 1) {
        repeat_interval_s = 1;
      }
      if (OB_UNLIKELY(sql_len < 0 || sql_len >= sizeof(sql_buf))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("escaped string too long", KR(ret), K(sql_len));
      } else if (OB_FAIL(repeat_interval.assign_fmt("FREQ=SECONDLY;INTERVAL=%ld", repeat_interval_s))) {
        LOG_WARN("fail to assign repeat interval", KR(ret), K(repeat_interval_s));
      } else if (OB_FAIL(job_action.assign_fmt("dbms_routine_load.consume_kafka(%ld, %u, %u, %u, %u, '%.*s', '%.*s')",
                          job_id, par_str_pos, par_str_len,
                          off_str_pos, off_str_len,
                          static_cast<int>(stmt.get_job_name().length()), stmt.get_job_name().ptr(),
                          static_cast<int>(sql_len), sql_buf))) {
        LOG_WARN("fail to assign job action", KR(ret), K(job_id),
             K(par_str_pos), K(par_str_len), K(off_str_pos), K(off_str_len), K(stmt), K(sql));
      } else if (OB_FAIL(ObRoutineLoadSchedUtil::create_routine_load_sched_job(trans, tenant_id, job_id,
          stmt.get_job_name(), job_action.string(), create_time, stmt.get_exec_env(), repeat_interval.string()))) {
        LOG_WARN("fail to create routine load sched job", KR(ret), K(stmt), K(create_time), K(sql), K(repeat_interval));
      }
    }
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  return ret;
}

//TODO: orcale mode
int ObCreateRoutineLoadExecutor::build_sql_(
    const int64_t job_id,
    ObCreateRoutineLoadStmt &stmt,
    ObSqlString &sql,
    uint32_t &par_str_pos,
    uint32_t &off_str_pos,
    uint32_t &par_str_len,
    uint32_t &off_str_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stmt.get_job_name().empty()
                  || stmt.get_combined_name().empty()
                  || stmt.get_job_prop_sql_str().empty()
                  || stmt.get_field_or_var_list().empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(stmt));
  } else if (OB_FAIL(sql.append(stmt.get_dupl_action() == ObLoadDupActionType::LOAD_REPLACE ? "replace " : "insert "))) {
    LOG_WARN("fail to append dupl_action", KR(ret), K(stmt));
  } else if (0 < stmt.get_parallel() && OB_FAIL(sql.append_fmt("/*+ PARALLEL(%ld) */", stmt.get_parallel()))) {
    LOG_WARN("fail to append parallel", KR(ret), K(stmt));
  } else if (stmt.get_dupl_action() == ObLoadDupActionType::LOAD_IGNORE && OB_FAIL(sql.append(" ignore "))) {
    LOG_WARN("fail to append dupl_action", KR(ret), K(stmt));
  } else if (OB_FAIL(sql.append_fmt(" into %.*s ", stmt.get_combined_name().length(), stmt.get_combined_name().ptr()))) {
    LOG_WARN("fail to append table name", KR(ret), K(stmt));
  } else {
    if (stmt.get_part_names().count() > 0) {
      ObIArray<ObString> &part_names = stmt.get_part_names();
      if (OB_FAIL(sql.append("partition("))) {
        LOG_WARN("fail to append", KR(ret));
      } else {
        ARRAY_FOREACH_N(part_names, i, cnt) {
          if (i > 0 && OB_FAIL(sql.append(","))) {
            LOG_WARN("fail to append", KR(ret));
          } else if (OB_FAIL(sql.append_fmt("%.*s", part_names.at(i).length(), part_names.at(i).ptr()))) {
            LOG_WARN("fail to append", KR(ret));
          }
        }
      }
      if (FAILEDx(sql.append(") "))) {
        LOG_WARN("fail to append", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 获取字段列表
    const ObIArray<FieldOrVarStruct> &field_list = stmt.get_field_or_var_list();
    // 检查是否存在非table column
    ARRAY_FOREACH_N(field_list, i, cnt) {
      const FieldOrVarStruct &field = field_list.at(i);
      if (OB_UNLIKELY(!field.is_table_column_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("var is not supported", KR(ret), K(field), K(i), K(field_list));
      }
    }
    if (OB_SUCC(ret)) {
      // 添加列名列表
      if (OB_FAIL(sql.append("("))) {
        LOG_WARN("fail to append", KR(ret));
      } else {
        ARRAY_FOREACH_N(field_list, i, cnt) {
          const FieldOrVarStruct &field = field_list.at(i);
          if (0 < i && OB_FAIL(sql.append(","))) {
            LOG_WARN("fail to append", KR(ret));
          } else {
            const char quote_char = lib::is_oracle_mode() ? '"' : '`';
            if (OB_FAIL(sql.append_fmt("%c%.*s%c", quote_char, field.field_or_var_name_.length(), field.field_or_var_name_.ptr(), quote_char))) {
              LOG_WARN("fail to append", KR(ret));
            }
          }
        }
        if (FAILEDx(sql.append(") "))) {
          LOG_WARN("fail to append", KR(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const char quote_char = lib::is_oracle_mode() ? '\'' : '"';
    //添加job_properties
    if (OB_FAIL(sql.append(" SELECT * FROM SOURCE ( type = 'KAFKA', "))) {
      LOG_WARN("fail to append", KR(ret));
    } else if (OB_FAIL(sql.append_fmt("%.*s", stmt.get_job_prop_sql_str().length(), stmt.get_job_prop_sql_str().ptr()))) {
      LOG_WARN("fail to append job_properties", KR(ret), K(stmt.get_job_prop_sql_str()));
    } else if (OB_FAIL(add_kafka_partitions_and_offsets_str_(stmt, sql, par_str_pos, off_str_pos, par_str_len, off_str_len))) {
      LOG_WARN("fail to add kafka partitions and offsets", KR(ret), K(stmt), K(sql));
    } else if (OB_FAIL(sql.append(", "))) {
      LOG_WARN("fail to append", KR(ret));
    } else if (OB_FAIL(sql.append_fmt("%cjob_id%c = %c%ld%c", quote_char, quote_char, quote_char, job_id, quote_char))) {
      LOG_WARN("fail to append fmt", KR(ret), K(job_id));
    } else if (OB_FAIL(sql.append(", "))) {
      LOG_WARN("fail to append", KR(ret));
    } else if (OB_FAIL(add_dest_table_info_(stmt, sql))) {
      LOG_WARN("fail to add dest table info", KR(ret), K(stmt), K(sql));
    } else if (OB_FAIL(sql.append(" ) "))) {
      LOG_WARN("fail to append", KR(ret));
    }
  }

  if (OB_SUCC(ret) && !stmt.get_where_clause().empty()) {
    if (OB_FAIL(sql.append_fmt("%.*s", stmt.get_where_clause().length(), stmt.get_where_clause().ptr()))) {
      LOG_WARN("fail to append where clause", KR(ret), K(stmt.get_where_clause()));
    }
  }

  return ret;
}

int ObCreateRoutineLoadExecutor::add_kafka_partitions_and_offsets_str_(
  const ObCreateRoutineLoadStmt &stmt,
  common::ObSqlString &sql,
  uint32_t &par_str_pos,
  uint32_t &off_str_pos,
  uint32_t &par_str_len,
  uint32_t &off_str_len)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = stmt.get_tenant_id();
  const ObString &topic_name = stmt.get_topic_name();
  const ObString &partitions_str = stmt.get_kafka_partitions_str();
  const ObString &offsets_str = stmt.get_kafka_offsets_str();
  const ObIArray<std::pair<common::ObString, common::ObString>> &custom_properties = stmt.get_kafka_custom_properties();
  ObSEArray<int32_t, 8> partition_arr;
  ObSEArray<int64_t, 8> offset_arr;
  ObSqlString new_partitions_str;
  ObSqlString new_offsets_str;
  //step1: build new partitions and offsets string
  sql::ObKAFKATableRowIterator kafka_row_iterator(tenant_id);
  if (OB_FAIL(kafka_row_iterator.init_consumer(tenant_id, OB_INVALID_ID, custom_properties))) {
    LOG_WARN("fail to init consumer", KR(ret), K(tenant_id));
  } else if (OB_FAIL(kafka_row_iterator.parse_partitions_and_offsets(tenant_id,
                                                               OB_INVALID_ID,
                                                               topic_name,
                                                               partitions_str,
                                                               offsets_str,
                                                               partition_arr,
                                                               offset_arr))) {
    LOG_WARN("fail to parser partitions and offsets", KR(ret), K(tenant_id), K(topic_name),
                            K(partitions_str), K(offsets_str));
  } else if (OB_UNLIKELY(partition_arr.count() != offset_arr.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition_arr or offset_arr", KR(ret), K(partition_arr), K(offset_arr));
  } else {
    ARRAY_FOREACH_N(partition_arr, i, cnt) {
      if (i > 0 && OB_FAIL(new_partitions_str.append(","))) {
        LOG_WARN("fail to append", KR(ret), K(i));
      } else if (i > 0 && OB_FAIL(new_offsets_str.append(","))) {
        LOG_WARN("fail to append", KR(ret), K(i));
      } else if (OB_FAIL(new_partitions_str.append_fmt("%d", partition_arr.at(i)))) {
        LOG_WARN("fail to append", KR(ret), K(i));
      } else if (OB_FAIL(new_offsets_str.append_fmt("%ld", offset_arr.at(i)))) {
        LOG_WARN("fail to append", KR(ret), K(i));
      }
    }
  }
  //step2: add new partitions and offsets to sql
  if (OB_SUCC(ret)) {
    const char quote_char = lib::is_oracle_mode() ? '\'' : '"';
    par_str_len = new_partitions_str.length();
    off_str_len = new_offsets_str.length();
    if (OB_UNLIKELY(stmt.get_job_prop_sql_str().length() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected job_prop_sql_str", KR(ret), K(stmt));
    } else if (OB_FAIL(sql.append_fmt(", %c%s%c = %c",
                   quote_char, ObKAFKAGeneralFormat::KAFKA_PARTITIONS, quote_char, quote_char))) {
      LOG_WARN("fail to append fmt", KR(ret), K(quote_char), K(ObKAFKAGeneralFormat::KAFKA_PARTITIONS));
    } else if (FALSE_IT(par_str_pos = sql.length())) {
    } else if (OB_FAIL(sql.append_fmt("%.*s%c", new_partitions_str.length(), new_partitions_str.ptr(), quote_char))) {
      LOG_WARN("fail to printf", KR(ret), K(new_partitions_str));
    } else if (OB_FAIL(sql.append_fmt(", %c%s%c = %c",
                   quote_char, ObKAFKAGeneralFormat::KAFKA_OFFSETS, quote_char, quote_char))) {
      LOG_WARN("fail to append fmt", KR(ret), K(quote_char), K(ObKAFKAGeneralFormat::KAFKA_OFFSETS));
    } else if (FALSE_IT(off_str_pos = sql.length())) {
    } else if (OB_FAIL(sql.append_fmt("%.*s%c", new_offsets_str.length(), new_offsets_str.ptr(), quote_char))) {
      LOG_WARN("fail to printf", KR(ret), K(new_offsets_str));
    }
  }
  return ret;
}

int ObCreateRoutineLoadExecutor::check_kafka_partitions_and_offsets_(ObCreateRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  // ObString kafka_offsets(stmt.get_offsets_str_len(), stmt.get_job_prop_sql_str().ptr() + stmt.get_offsets_str_pos());
  // ObString kafka_partitions(stmt.get_partition_str_len(), stmt.get_job_prop_sql_str().ptr() + stmt.get_partition_str_pos());
  // int64_t kafka_partition_cnt = 0;
  // int64_t kafka_offset_cnt = 0;
  // const char delimiter = ',';
  // if (!kafka_offsets.empty()) {
  //   if (OB_FAIL(ObRoutineLoadSchedUtil::parser_and_check_kafka_offsets(kafka_offsets, delimiter, kafka_offset_cnt))) {
  //     LOG_WARN("fail to parser and check kafka offsets", KR(ret), K(kafka_offsets));
  //   }
  // }
  // if (OB_FAIL(ret)) {
  // } else if (!kafka_partitions.empty()) {
  //   if (OB_FAIL(ObRoutineLoadSchedUtil::parser_and_check_kafka_partitions(kafka_partitions, delimiter, kafka_partition_cnt))) {
  //     LOG_WARN("fail to parser and check kafka partitions", KR(ret), K(kafka_partitions));
  //   } else if (OB_UNLIKELY(!kafka_offsets.empty() && kafka_offset_cnt != kafka_partition_cnt)) {
  //     ret = OB_OP_NOT_ALLOW;
  //     LOG_WARN("not allow a mismatch between offsets and partitions", KR(ret), K(kafka_offsets), K(kafka_partitions),
  //                                             K(kafka_offset_cnt), K(kafka_partition_cnt));
  //     LOG_USER_ERROR(OB_OP_NOT_ALLOW, "a mismatch between kafka offsets and kafka partitions");
  //   }
  // } else {
  //   //kafka_partitions is empty (consume all partitions)
  //   if (OB_UNLIKELY(!kafka_offsets.empty() && 1 != kafka_offset_cnt)) {
  //     ret = OB_OP_NOT_ALLOW;
  //     LOG_WARN("not allow a mismatch between offsets and partitions", KR(ret), K(kafka_offsets), K(kafka_partitions),
  //                                             K(kafka_offset_cnt), K(kafka_partition_cnt));
  //     LOG_USER_ERROR(OB_OP_NOT_ALLOW, "a mismatch between kafka offsets and kafka partitions");
  //   }
  // }
  return ret;
}

int ObCreateRoutineLoadExecutor::add_dest_table_info_(
    const ObCreateRoutineLoadStmt &stmt,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char quote_char = lib::is_oracle_mode() ? '\'' : '"';
  if (OB_FAIL(sql.append_fmt("%ctable_id%c = %c%lu%c", quote_char, quote_char, quote_char, stmt.get_table_id(), quote_char))) {
    LOG_WARN("fail to append fmt", KR(ret), K(stmt.get_table_id()));
  } else if (OB_FAIL(sql.append_fmt(", %ccolumn_list%c = %c", quote_char, quote_char, quote_char))) {
    LOG_WARN("fail to append", KR(ret));
  } else {
    const ObIArray<FieldOrVarStruct> &field_list = stmt.get_field_or_var_list();
    ARRAY_FOREACH_N(field_list, i, cnt) {
      const FieldOrVarStruct &field = field_list.at(i);
      if (0 < i && OB_FAIL(sql.append(","))) {
        LOG_WARN("fail to append", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%lu", field.column_id_))) {
        LOG_WARN("fail to append", KR(ret));
      }
    }
  }
  if (FAILEDx(sql.append_fmt("%c", quote_char))) {
    LOG_WARN("fail to append", KR(ret));
  }
  return ret;
}

int ObPauseRoutineLoadExecutor::execute(ObExecContext &ctx, ObPauseRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    const bool is_oracle_mode = lib::is_oracle_mode();
    ObMySQLTransaction trans;
    ObRoutineLoadTableOperator table_op;
    ObArenaAllocator allocator;
    int64_t job_id = OB_INVALID_ID;
    const uint64_t tenant_id = stmt.get_tenant_id();
    const ObString &job_name = stmt.get_job_name();
    ObRoutineLoadJobState current_state = ObRoutineLoadJobState::MAX;
    dbms_scheduler::ObDBMSSchedJobInfo job_info;
    ObObj obj;
    obj.set_bool(false);
    int64_t pause_time = ObTimeUtility::current_time();
    if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
                  *GCTX.sql_proxy_,
                  tenant_id,
                  is_oracle_mode, // is_oracle_tenant
                  job_name,
                  allocator,
                  job_info))) {
      LOG_WARN("fail to get dbms sched job info", KR(ret), K(tenant_id), K(job_name));
    } else if (FAILEDx(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                        *GCTX.sql_proxy_, job_info, ObString("enabled"), obj))) {
      LOG_WARN("fail to update dbms sched job info", KR(ret), K(job_info), K(obj));
    } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::stop_dbms_sched_job(
                *GCTX.sql_proxy_, job_info, false /* is_delete_after_stop */))) {
      // If job is not running (OB_ENTRY_NOT_EXIST), it's not an error for routine load stop
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("dbms sched job is not running", K(job_id), K(job_name));
      } else {
        LOG_WARN("fail to stop dbms sched job", KR(ret), K(job_id), K(job_name));
      }
    }

    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
      LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.get_job_id_by_name(job_name, job_id))) {
      LOG_WARN("failed to get job id", KR(ret), K(job_name));
    } else if (OB_FAIL(table_op.get_status(job_name, current_state))) {
      LOG_WARN("failed to get current status", KR(ret), K(job_id));
    } else if (ObRoutineLoadJobState::RUNNING != current_state) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("routine load job is not in running state", KR(ret), K(job_name), K(current_state));
    } else if (OB_FAIL(table_op.update_status(job_id,
                                              ObRoutineLoadJobState::RUNNING,
                                              ObRoutineLoadJobState::PAUSED))) {
      LOG_WARN("failed to update status", KR(ret), K(job_id));
    } else if (OB_FAIL(table_op.update_pause_time(job_id, pause_time))) {
      LOG_WARN("failed to update pause time", KR(ret), K(job_id), K(pause_time));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObResumeRoutineLoadExecutor::execute(ObExecContext &ctx, ObResumeRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    const bool is_oracle_mode = lib::is_oracle_mode();
    ObMySQLTransaction trans;
    ObRoutineLoadTableOperator table_op;
    ObArenaAllocator allocator;
    int64_t job_id = OB_INVALID_ID;
    const uint64_t tenant_id = stmt.get_tenant_id();
    const ObString &job_name = stmt.get_job_name();
    dbms_scheduler::ObDBMSSchedJobInfo job_info;
    ObRoutineLoadJobState current_state = ObRoutineLoadJobState::MAX;
    ObObj obj;
    obj.set_bool(true);
    if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
                  *GCTX.sql_proxy_,
                  tenant_id,
                  is_oracle_mode, // is_oracle_tenant
                  job_name,
                  allocator,
                  job_info))) {
      LOG_WARN("fail to get dbms sched job info", KR(ret), K(tenant_id), K(job_name));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
      LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.get_job_id_by_name(job_name, job_id))) {
      LOG_WARN("failed to get job id", KR(ret), K(job_name));
    } else if (OB_FAIL(table_op.get_status(job_name, current_state))) {
      LOG_WARN("failed to get current status", KR(ret), K(job_id));
    } else if (ObRoutineLoadJobState::PAUSED != current_state) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("routine load job is not in paused state", KR(ret), K(job_name), K(current_state));
    } else if (OB_FAIL(table_op.update_status(job_id,
                                              ObRoutineLoadJobState::PAUSED,
                                              ObRoutineLoadJobState::RUNNING))) {
      LOG_WARN("failed to update status", KR(ret), K(job_id));
    } else if (OB_FAIL(table_op.update_pause_time(job_id, OB_INVALID_TIMESTAMP))) {
      LOG_WARN("failed to reset pause time", KR(ret), K(job_id));
    } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                                  trans, job_info, ObString("enabled"), obj))) {
      LOG_WARN("fail to update dbms sched job info", KR(ret), K(job_info), K(obj));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObStopRoutineLoadExecutor::execute(ObExecContext &ctx, ObStopRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    const bool is_oracle_mode = lib::is_oracle_mode();
    ObMySQLTransaction trans;
    ObRoutineLoadTableOperator table_op;
    ObArenaAllocator allocator;
    int64_t job_id = OB_INVALID_ID;
    const uint64_t tenant_id = stmt.get_tenant_id();
    const ObString &job_name = stmt.get_job_name();
    dbms_scheduler::ObDBMSSchedJobInfo job_info;
    ObRoutineLoadJobState current_state = ObRoutineLoadJobState::MAX;
    ObObj obj;
    obj.set_bool(false);
    int64_t end_time = ObTimeUtility::current_time();
    if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
                  *GCTX.sql_proxy_,
                  tenant_id,
                  is_oracle_mode, // is_oracle_tenant
                  job_name,
                  allocator,
                  job_info))) {
      LOG_WARN("fail to get dbms sched job info", KR(ret), K(tenant_id), K(job_name));
    }

    // Stop the dbms scheduler job (kill session if running) and delete it
    if (FAILEDx(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                 *GCTX.sql_proxy_, job_info, ObString("enabled"), obj))) {
      LOG_WARN("fail to update dbms sched job info", KR(ret), K(job_info), K(obj));
    } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::stop_dbms_sched_job(
                      *GCTX.sql_proxy_, job_info, true /* is_delete_after_stop */))) {
      // If job is not running (OB_ENTRY_NOT_EXIST), it's not an error for routine load stop
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("dbms sched job is not running", K(job_id), K(job_name));
      } else {
        LOG_WARN("fail to stop dbms sched job", KR(ret), K(job_id), K(job_name));
      }
    }
    // Remove the dbms sched job from metadata if stop_dbms_sched_job didn't already do it
    if (FAILEDx(dbms_scheduler::ObDBMSSchedJobUtils::remove_dbms_sched_job(
          *GCTX.sql_proxy_, tenant_id, job_name, true /* if_exists */))) {
      LOG_WARN("fail to remove dbms sched job", KR(ret), K(tenant_id), K(job_name));
    }

    // Update routine load job status to STOPPED
    if (FAILEDx(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
      LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.get_job_id_by_name(job_name, job_id))) {
      LOG_WARN("failed to get job id", KR(ret), K(job_name));
    } else if (OB_FAIL(table_op.get_status(job_name, current_state))) {
      LOG_WARN("failed to get current status", KR(ret), K(job_id));
    } else if (ObRoutineLoadJobState::RUNNING != current_state
               && ObRoutineLoadJobState::PAUSED != current_state) {
      LOG_WARN("routine load job is not in running state or paused state", KR(ret), K(job_name), K(current_state));
    }
    if (FAILEDx(table_op.update_status(job_id,
                                        current_state,
                                        ObRoutineLoadJobState::STOPPED))) {
      LOG_WARN("failed to update status", KR(ret), K(job_name), K(current_state));
    } else if (OB_FAIL(table_op.update_job_name_and_stop_time(job_id, job_name, end_time))) {
      LOG_WARN("failed to update stop time", KR(ret), K(job_id), K(job_name), K(end_time));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

} // sql
} // oceanbase
