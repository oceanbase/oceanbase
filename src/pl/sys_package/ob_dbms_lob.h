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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LOB_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LOB_H_
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{

// 用于恢复 session 里的 prelock 属性
struct RestoreGuard
{
public:
  RestoreGuard(sql::ObSQLSessionInfo *session, bool ori_prelock_value)
    : session_(session), ori_prelock_value_(ori_prelock_value) {}
  ~RestoreGuard() { session_->set_prelock(ori_prelock_value_); }
private:
  sql::ObSQLSessionInfo *session_;
  bool ori_prelock_value_;
};

class ObDbmsLobBase {
public:
  static int get_schema_guard(
      sql::ObExecContext *exec_ctx,
      share::schema::ObSchemaGetterGuard *&schema_guard);
  static int get_lob_sqlstr(
      sql::ObExecContext *exec_ctx,
      common::ObSqlString &target,
      common::ObSqlString &col_name_str,
      const common::ObObj &in_lob,
      const common::ObObj &tmp_lob,
      const bool is_select);
  static int build_lob_locator(
      common::ObLobLocator *&locator,
      common::ObIAllocator &alloctor,
      const uint64_t tbl_id,
      const uint64_t col_id,
      const int64_t snapshot_version,
      const uint16_t flags,
      const common::ObString &rowid_str,
      const common::ObString &payload);
};

class ObDbmsLob
{
public:
  static int trim(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int append(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int write(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int open(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int close(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int isopen(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int getlength(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int read(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int substr(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int instr(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int writeappend(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int erase(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int copy(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int createtemporary(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int freetemporary(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int converttoblob(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int obci_write(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int compare(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

private:
  static int calc_trim_result(
      sql::ObExecContext &ctx, sql::ParamStore &params,
      common::ObObj &newest_lob);
  static int calc_append_result(
        sql::ObExecContext &ctx, sql::ParamStore &params,
        common::ObObj &newest_lob);
  static int calc_write_result(
        sql::ObExecContext &ctx, sql::ParamStore &params,
        common::ObObj &newest_lob);
  static int calc_copy_result(
        sql::ObExecContext &ctx, sql::ParamStore &params,
        common::ObObj &newest_lob);
  static int calc_converttoblob_result(
        sql::ObExecContext &ctx, sql::ParamStore &params,
        common::ObObj &newest_lob);
  static int calc_obci_write_result(
        sql::ObExecContext &ctx, sql::ParamStore &params,
        common::ObObj &newest_lob);
  static int calc_res_and_modify_ori_lob_value(
      sql::ObExecContext &ctx, sql::ParamStore &params,
      int(*calc_result_func)(sql::ObExecContext &ctx,
                             sql::ParamStore &params,
                             common::ObObj &newest_lob));
  static const int32_t LOB_READONLY = 0;
  static const int32_t LOB_READWRITE = 1;
  static const int32_t INVALID_TABLE_ID_IN_LOB_LOCATOR = 0;
  // DBMS_LOB.LOBMAXSIZE is only used in CONVERTTOBLOB and CONVERTTOCLOB.
  // we can specify DBMS_LOB.LOBMAXSIZE for the amount parameter to copy the entire LOB.
  static const uint64_t LOBMAXSIZE = UINT64_MAX; // 18446744073709551615
  static const uint64_t WARN_INCONVERTIBLE_CHAR = 1;
  static const uint64_t DEFAULT_CSID = 0;
  static const uint64_t DEFAULT_LANG_CTX = 0;
  static const uint64_t NO_WARNING = 0;
};
} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LOB_H_ */
