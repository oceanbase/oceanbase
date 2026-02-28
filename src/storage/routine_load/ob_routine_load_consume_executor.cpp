/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/routine_load/ob_routine_load_consume_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "deps/oblib/src/lib/json/ob_json.h"
#include "storage/routine_load/ob_routine_load_dbms_sched_util.h"
#include "storage/mview/ob_mview_transaction.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;

ObRoutineLoadConsumeExecutor::ObRoutineLoadConsumeExecutor() :
    ctx_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObRoutineLoadConsumeExecutor::execute(ObExecContext &ctx, const ObRoutineLoadConsumeArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  const ObString &sql_str = arg.exec_sql_;
  const int64_t job_id = arg.job_id_;
  const ObString &job_name = arg.job_name_;
  ObSQLSessionInfo *session_info = nullptr;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;
  ObMViewTransaction trans;
  ObString new_kafka_partitions;
  ObString new_kafka_offsets;
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  ObArenaAllocator allocator;
  int64_t affected_rows = 0;
  ObSqlString exec_sql;
  common::ObCurTraceId::TraceId trace_id;
  ObRoutineLoadJobState state = ObRoutineLoadJobState::MAX;
  ObRLoadDynamicFields origin_dynamic_fields;
  const bool is_oracle_mode = lib::is_oracle_mode();
  // Setup timeout context for routine load execution
  // ObTimeoutCtx will be inherited by parallel workers (PX threads)
  ObTimeoutCtx timeout_ctx;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const int64_t timeout_us = GCONF._ob_ddl_timeout;
  int64_t abs_timeout_us = ObTimeUtility::current_time() + timeout_us;
  THIS_WORKER.set_timeout_ts(abs_timeout_us);
  if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout_us))) {
    LOG_WARN("failed to set trx timeout us", KR(ret), K(timeout_us));
  } else if (OB_FAIL(timeout_ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("failed to set abs timeout", KR(ret), K(abs_timeout_us));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(session_info = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session", KR(ret), KP(session_info));
  } else if (FALSE_IT(tenant_id = session_info->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant data version is below 4.5.1", KR(ret), K(tenant_id), K(data_version));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    //update trace id
    if (nullptr != ObCurTraceId::get_trace_id()) {
      trace_id = *ObCurTraceId::get_trace_id();
    } else {
      trace_id.init(GCONF.self_addr_);
    }
    ObRoutineLoadTableOperator table_op;
    if (OB_FAIL(table_op.init(tenant_id, GCTX.sql_proxy_))) {
      LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_op.reset_tmp_results(job_id))) {
      FLOG_WARN("failed to reset tmp results", KR(ret), K(job_id));
    } else if (OB_FAIL(table_op.update_trace_id(job_id, trace_id))) {
      FLOG_WARN("failed to update trace id", KR(ret), K(job_id), K(trace_id));
    } else if (OB_FAIL(table_op.get_status(job_name, state))) {
      FLOG_WARN("failed to get status", KR(ret), K(job_name));
    } else if (OB_FAIL(table_op.get_dynamic_fields(job_id, false, allocator, origin_dynamic_fields))) {
      FLOG_WARN("failed to get dynamic fields from table", KR(ret), K(job_id));
    } else if (OB_UNLIKELY(ObRoutineLoadJobState::RUNNING != state)) {
      FLOG_WARN("unexpected status", KR(ret), K(job_id), K(job_name), K(state));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
                          *GCTX.sql_proxy_,
                          tenant_id,
                          is_oracle_mode, // is_oracle_tenant
                          job_name,
                          allocator,
                          job_info))) {
      FLOG_WARN("fail to get dbms sched job info", KR(ret), KR(tmp_ret), K(tenant_id), K(arg));
    }
    ret = (OB_SUCC(ret)) ? tmp_ret : ret;
  }
  if (FAILEDx(trans.start(session_info,
                          ctx.get_sql_proxy(),
                          session_info->get_database_id(),
                          session_info->get_database_name()))) {
    FLOG_WARN("fail to start trans", KR(ret), K(tenant_id),
             K(session_info->get_database_id()), K(session_info->get_database_name()));
  }

  //STEP1: execute sql--insert into select from kafka
  if (OB_SUCC(ret)) {
    //TODO: user tenant or meta tenant
    if (OB_FAIL(exec_sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql string", KR(ret), K(sql_str));
    } else if (OB_FAIL(trans.write(tenant_id, exec_sql.ptr(), affected_rows))) {
      FLOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_sql));
    }
  }

  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans) {
      if (!is_zero_row(affected_rows)) {
      } else {
        LOG_WARN("kafka has no data to consume", KR(ret), K(affected_rows));
      }

      //STEP2: update routine_load_job (__all_routine_load_job)
      //NOTE: tmp_results字段的格式和progress/lag字段的格式一致，所以可以直接覆盖
      if (OB_SUCC(ret)) {
        // if (is_zero_row(affected_rows)) {
        //   //do nothing
        // } else {
          ObRoutineLoadTableOperator table_op;
          if (OB_FAIL(table_op.init(tenant_id, &trans))) {
            LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
          } else {
            ObRLoadDynamicFields dynamic_fields;
            //确保线程运行到这，不会有线程再去更新dynamic_fields
            if (OB_FAIL(table_op.get_dynamic_fields(job_id, false, allocator, dynamic_fields))) {
              FLOG_WARN("failed to get dynamic fields from table", KR(ret), K(job_id));
            } else if (OB_UNLIKELY(dynamic_fields.tmp_progress_.empty()
                                  || dynamic_fields.tmp_lag_.empty())) {
              ret = OB_ERR_UNEXPECTED;
              FLOG_WARN("unexpected dynamic_fields", KR(ret), K(dynamic_fields));
            } else if (OB_FAIL(check_and_parser_(allocator, dynamic_fields, new_kafka_partitions, new_kafka_offsets))) {
              FLOG_WARN("fail to check and parser", KR(ret), K(dynamic_fields));
            } else if (OB_FAIL(table_op.update_offsets_from_tmp_results(job_id, dynamic_fields, origin_dynamic_fields))) {
              FLOG_WARN("failed to update offsets from tmp results", KR(ret), K(job_id), K(dynamic_fields), K(origin_dynamic_fields));
            }
          }
        // }
      }

      //STEP3: update dbms_sched_job (__all_tenant_scheduler_job)
      if (OB_SUCC(ret)) {
        // if (is_zero_row(affected_rows)) {
        //   //do nothing
        // } else {
          ObString new_exec_sql;
          uint32_t new_par_str_pos = 0;
          uint32_t new_off_str_pos = 0;
          ObSqlString job_action;
          ObObj obj;
          if (OB_FAIL(generate_new_exec_sql_(allocator, arg, new_kafka_partitions, new_kafka_offsets,
                                          new_exec_sql, new_par_str_pos, new_off_str_pos))) {
            FLOG_WARN("failed to generate_new_exec_sql_", KR(ret), K(arg), K(new_kafka_partitions), K(new_kafka_offsets));
          } else {
            //TODO: DEFAULT_BUF_LENGTH
            char sql_buf[DEFAULT_BUF_LENGTH] = {0};
            ObHexEscapeSqlStr sql_escaped(new_exec_sql);
            int64_t sql_len = sql_escaped.to_string(sql_buf, sizeof(sql_buf));
            if (OB_FAIL(job_action.assign_fmt("dbms_routine_load.consume_kafka(%ld, %u, %u, %u, %u, '%.*s', '%.*s')",
                                                job_id, new_par_str_pos, new_kafka_partitions.length(),
                                                new_off_str_pos, new_kafka_offsets.length(),
                                                static_cast<int>(job_name.length()), job_name.ptr(),
                                                static_cast<int>(sql_len), sql_buf))) {
              FLOG_WARN("fail to assign job action", KR(ret), K(job_id), K(new_par_str_pos), K(new_off_str_pos),
                                                    K(new_kafka_partitions), K(new_kafka_offsets), K(new_exec_sql));
            } else if (FALSE_IT(obj.set_varchar(job_action.string()))) {
            } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                                            trans, job_info, ObString("job_action"), obj))) {
              FLOG_WARN("fail to update dbms sched job info", KR(ret), K(job_info), K(obj));
            }
          }
        // }
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

  if (OB_SUCCESS != ret) {
    int tmp_ret = OB_SUCCESS;
    bool need_retry = false;
    if (OB_KAFKA_NEED_RETRY == ret || OB_ALLOCATE_MEMORY_FAILED == ret || OB_EAGAIN == ret || OB_TIMEOUT == ret) {
      need_retry = true;
    } else {
      static const char *basic_error_msg_suffix = " If insert failed, you can check \"PROGRESS\" and \"TMP_PROGRESS\" for more information. "
                                                   "Invalid data between \"PROGRESS\" and \"TMP_PROGRESS\".";
      const char *error_name = common::ob_error_name(ret);
      ObSqlString full_error_msg;
      ObString error_msg;
      ObObj obj;
      obj.set_bool(false);
      ObRoutineLoadTableOperator table_op;
      if (OB_TMP_FAIL(table_op.init(tenant_id, GCTX.sql_proxy_))) {
        LOG_WARN("failed to init table op", KR(tmp_ret), KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(full_error_msg.append_fmt("Error: %s(%d).%s", error_name, ret, basic_error_msg_suffix))) {
        FLOG_WARN("failed to append error msg", KR(tmp_ret), KR(ret), K(job_id));
        // Fallback to basic message if append fails
        error_msg.assign_ptr(basic_error_msg_suffix, static_cast<int32_t>(strlen(basic_error_msg_suffix)));
      } else {
        error_msg = full_error_msg.string();
      }
      if (OB_TMP_FAIL(table_op.update_info_when_failed(job_id, ret))) {
        FLOG_WARN("failed to update info when failed", KR(tmp_ret), KR(ret), K(job_id));
      } else if (OB_TMP_FAIL(table_op.update_error_msg(job_id, error_msg))) {
        FLOG_WARN("failed to update error msg", KR(tmp_ret), KR(ret), K(job_id));
      } else if (OB_TMP_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
                  *GCTX.sql_proxy_, job_info, ObString("enabled"), obj))) {
        FLOG_WARN("fail to update dbms sched job info", KR(tmp_ret), KR(ret), K(job_info), K(obj));
      }
    }
    LOG_INFO("routine load this round execution fails", KR(ret), KR(tmp_ret), K(need_retry));
  } else {
    LOG_INFO("routine load this round execution success", KR(ret), K(job_id), K(job_name), K(affected_rows));
  }
  // Restore original timeout
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

int ObRoutineLoadConsumeExecutor::check_and_parser_(
    ObArenaAllocator &allocator,
    const ObRLoadDynamicFields &dynamic_fields,
    ObString &new_kafka_partitions,
    ObString &new_kafka_offsets)
{
  int ret = OB_SUCCESS;
  int64_t data_cnt = 0;
  if (OB_FAIL(parser_and_check_tmp_results_(allocator, dynamic_fields, new_kafka_partitions, new_kafka_offsets, data_cnt))) {
    LOG_WARN("fail to parser and check tmp results", KR(ret), K(dynamic_fields));
  } else if (OB_UNLIKELY(new_kafka_partitions.empty()
                         || new_kafka_offsets.empty()
                         || 0 == data_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new_kafka_partitions or new_kafka_offsets or data_cnt", KR(ret),
                                        K(new_kafka_partitions), K(new_kafka_offsets), K(data_cnt));
  }
  return ret;
}

int ObRoutineLoadConsumeExecutor::parser_and_check_tmp_results_(
    ObArenaAllocator &allocator,
    const ObRLoadDynamicFields &dynamic_fields,
    ObString &new_kafka_partitions,
    ObString &new_kafka_offsets,
    int64_t &data_cnt)
{
  int ret = OB_SUCCESS;
  new_kafka_partitions.reset();
  new_kafka_offsets.reset();
  data_cnt = 0;
  ObString tmp_pro_remain_str = dynamic_fields.tmp_progress_;
  ObString tmp_lag_remain_str = dynamic_fields.tmp_lag_;
  ObString pro_remain_str = dynamic_fields.progress_;
  ObString lag_remain_str = dynamic_fields.lag_;
  bool need_check_old = pro_remain_str.empty() && lag_remain_str.empty() ? false : true;
  char *par_buf = NULL;
  int64_t par_pos = 0;
  char *off_buf = NULL;
  int64_t off_pos = 0;
  const int64_t buf_len = MIN(OB_MAX_VARCHAR_LENGTH, dynamic_fields.tmp_progress_.length() + 1);
  if (OB_ISNULL(par_buf = (char *)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc par_buf", KR(ret), K(buf_len));
  } else if (OB_ISNULL(off_buf = (char *)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc off_buf", KR(ret), K(buf_len));
  } else {
    while (OB_SUCC(ret) && !tmp_pro_remain_str.empty()) {
      if (OB_UNLIKELY(tmp_lag_remain_str.empty()
                      || (need_check_old && pro_remain_str.empty())
                      || (need_check_old && lag_remain_str.empty()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected remain_str", KR(ret), K(tmp_pro_remain_str), K(tmp_lag_remain_str), K(pro_remain_str), K(lag_remain_str));
      } else {
        //TODO: 如果需要加上空格的话，需要trim()
        ObString item_str = tmp_pro_remain_str.split_on(',');
        if (item_str.empty()) {
          item_str = tmp_pro_remain_str;
          tmp_pro_remain_str.reset();
        }
        ObString item_partition = item_str.split_on(':');
        if (OB_UNLIKELY(item_partition.empty() || item_str.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected partition str or offset str", KR(ret), K(item_partition), K(item_str));
        } else if (0 < data_cnt && OB_FAIL(databuff_printf(par_buf, buf_len, par_pos, ","))) {
          LOG_WARN("fail to printf", KR(ret), K(buf_len), K(par_pos));
        } else if (OB_FAIL(databuff_printf(par_buf, buf_len, par_pos, "%.*s",
                            item_partition.length(), item_partition.ptr()))) {
          LOG_WARN("fail to printf", KR(ret), K(buf_len), K(par_pos), K(item_partition));
        } else if (0 < data_cnt && OB_FAIL(databuff_printf(off_buf, buf_len, off_pos, ","))) {
          LOG_WARN("fail to printf", KR(ret), K(buf_len), K(par_pos));
        } else if (OB_FAIL(databuff_printf(off_buf, buf_len, off_pos, "%.*s",
                                      item_str.length(), item_str.ptr()))) {
          LOG_WARN("fail to printf", KR(ret), K(buf_len), K(off_pos), K(item_str));
        } else {
          data_cnt++;
          //just for check
          if (tmp_lag_remain_str.split_on(',').empty()) {
            tmp_lag_remain_str.reset();
          }
          if (need_check_old) {
            if (pro_remain_str.split_on(',').empty()) {
              pro_remain_str.reset();
            }
            if (lag_remain_str.split_on(',').empty()) {
              lag_remain_str.reset();
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!tmp_lag_remain_str.empty()
                      || !pro_remain_str.empty()
                      || !lag_remain_str.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected remain_str", KR(ret), K(tmp_pro_remain_str), K(tmp_lag_remain_str), K(pro_remain_str), K(lag_remain_str));
      } else {
        new_kafka_partitions.assign_ptr(par_buf, par_pos);
        new_kafka_offsets.assign_ptr(off_buf, off_pos);
      }
    }
  }
  return ret;
}

int ObRoutineLoadConsumeExecutor::generate_new_exec_sql_(
    ObArenaAllocator &allocator,
    const ObRoutineLoadConsumeArg &arg,
    const ObString &new_kafka_partitions,
    const ObString &new_kafka_offsets,
    ObString &new_exec_sql,
    uint32_t &new_par_str_pos,
    uint32_t &new_off_str_pos)
{
  int ret = OB_SUCCESS;
  //TODO: 为什么在sql语句中KAFKA_PARTITIONS等变成小写了？双引号算几个字符？
  const ObString &old_exec_sql = arg.exec_sql_;
  uint32_t first_pos = arg.par_str_pos_;
  uint32_t first_len = arg.par_str_len_;
  uint32_t second_pos = arg.off_str_pos_;
  uint32_t second_len = arg.off_str_len_;
  //NOTE: first_pos不会位于sql字符串的第一个，first_len和second_len有可能为0
  if (OB_UNLIKELY(0 >= first_pos
                  || 0 >= second_pos
                  || 0 > first_len
                  || 0 > second_len
                  || first_len + second_len >= old_exec_sql.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected par_pos or off_pos or length", KR(ret), K(arg));
  } else if (OB_UNLIKELY(new_kafka_partitions.empty() || new_kafka_offsets.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new_kafka_partitions or new_kafka_offsets", KR(ret), K(new_kafka_partitions), K(new_kafka_offsets));
  } else {
    char *new_buf = nullptr;
    int64_t pos = 0;
    const int64_t new_buf_len = old_exec_sql.length() + new_kafka_partitions.length() + new_kafka_offsets.length()
                                  - first_len - second_len + 1;
    if (OB_ISNULL(new_buf = (char *)allocator.alloc(new_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc", KR(ret), K(new_buf_len));
    } else {
      const char *data_buf[5] = {nullptr};
      int64_t buf_len[5] = {0};
      data_buf[0] = old_exec_sql.ptr();
      buf_len[0] = first_pos;
      data_buf[1] = new_kafka_partitions.ptr();
      buf_len[1] = new_kafka_partitions.length();
      if (first_pos + first_len < old_exec_sql.length()) {
        data_buf[2] = old_exec_sql.ptr() + first_pos + first_len;
        buf_len[2] = second_pos - (first_pos + first_len);
      }
      data_buf[3] = new_kafka_offsets.ptr();
      buf_len[3] = new_kafka_offsets.length();
      if (second_pos + second_len < old_exec_sql.length()) {
        data_buf[4] = old_exec_sql.ptr() + second_pos + second_len;
        buf_len[4] = old_exec_sql.length() - (second_pos + second_len);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < 5; i++) {
        if (1 == i) {
          new_par_str_pos = pos;
        } else if (3 == i) {
          new_off_str_pos = pos;
        }
        if (NULL != data_buf[i] && 0 < buf_len[i]
           && OB_FAIL(databuff_printf(new_buf, new_buf_len, pos, "%.*s",
                                                buf_len[i], data_buf[i]))) {
          LOG_WARN("fail to printf", KR(ret), K(i), K(buf_len[i]), KP(data_buf[i]), K(pos), K(new_buf_len));
        }
      }
      if (OB_SUCC(ret)) {
        new_exec_sql.assign_ptr(new_buf, pos);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
