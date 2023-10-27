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

#ifndef OCEANBASE_SQL_OB_FLT_UTILS_H_
#define OCEANBASE_SQL_OB_FLT_UTILS_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rpc/obmysql/obp20_extra_info.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "sql/ob_sql_define.h"
#include "sql/monitor/flt/ob_flt_extra_info.h"

namespace oceanbase
{
namespace sql
{
  class ObFLTVars {
  public:
    ObFLTVars() : row_traceformat_(true),
                  trc_granuality_(ObTraceGranularity::TRANS_LEVEL)
    {
      flt_trace_id_[0] = '\0';
      flt_span_id_[0] = '\0';
      last_flt_trace_id_buf_[0] = '\0';
      last_flt_span_id_buf_[0] = '\0';
    }
    void reset() {
      flt_trace_id_[0] = '\0';
      flt_span_id_[0] = '\0';
      last_flt_trace_id_buf_[0] = '\0';
      last_flt_trace_id_.reset();
      last_flt_span_id_buf_[0] = '\0';
      last_flt_span_id_.reset();
      row_traceformat_ = true;
      trc_granuality_ = ObTraceGranularity::TRANS_LEVEL;
    }
  public:
    char flt_trace_id_[common::OB_MAX_UUID_LENGTH + 1];
    char flt_span_id_[common::OB_MAX_UUID_LENGTH + 1];
    ObString last_flt_trace_id_;
    char last_flt_trace_id_buf_[OB_MAX_UUID_STR_LENGTH + 1];
    ObString last_flt_span_id_;
    char last_flt_span_id_buf_[OB_MAX_UUID_STR_LENGTH + 1];
    bool row_traceformat_;
    ObTraceGranularity trc_granuality_;
  };
  class ObFLTUtils {
  public:
    static int init_flt_info(obmysql::Ob20ExtraInfo extra_info,
                              sql::ObSQLSessionInfo &session,
                              bool is_client_support_flt);
    static int append_flt_extra_info(common::ObIAllocator &allocator,
                                      ObIArray<obmysql::ObObjKV> *extra_info,
                                      ObIArray<obmysql::Obp20Encoder*> *extra_info_ecds,
                                      sql::ObSQLSessionInfo &sess,
                                      bool is_new_extra_info);
    static int process_flt_extra_info(const char *buf, const int64_t len, sql::ObSQLSessionInfo &sess);
    static int init_app_info(sql::ObSQLSessionInfo &sess, sql::FLTAppInfo &app_info);
    static int init_flt_log_framework(sql::ObSQLSessionInfo &session, bool is_client_support_flt);
    static int update_flush_policy_by_control_info(sql::ObSQLSessionInfo &sess);
    static int record_flt_last_trace_id(sql::ObSQLSessionInfo &session);
    static int clean_flt_show_trace_env(sql::ObSQLSessionInfo &session);
    static void clean_flt_env();
    static int process_flt_span_rec(const char *buf, const int64_t len);
    static int resolve_flt_span_rec(ObIJsonBase *j_tree, ObArenaAllocator& alloc);
    static int set_json_str_val(ObString key, ObIJsonBase *jobject_ptr, ObString& val);
    static int set_json_num_val(ObString key, ObIJsonBase *jobject_ptr, int64_t& val);
    static int set_json_bool_val(ObString key, ObIJsonBase *jobject_ptr, int64_t& val);
    static int set_json_obj_val(ObString key, ObIJsonBase *jobject_ptr, ObString& val, ObArenaAllocator& alloc);
  };
} // namespace sql
} // namespace oceanbase

#endif
