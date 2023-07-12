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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_XA_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_XA_H_

#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_type.h"

namespace oceanbase
{

namespace sql
{
class ObXaStmt;
}

namespace pl
{

// class ObDBMSXid
// {
//   OB_UNIS_VERSION(1);
// public:
//   TO_STRING_KV(K_(gtrid), K_(bqual), K_(formatid));
//   ObString gtrid_;
//   ObString bqual_;
//   int64_t formatid_;
// };

class ObDbmsXA
{
public:
  static const int64_t MAX_XID_LEN;
  static const int64_t MAX_GTRID_LEN;
  static const int64_t MAX_BQUAL_LEN;

  static const int XA_RBBASE = 100;
  static const int XA_RBROLLBACK = XA_RBBASE;
  static const int XA_RBCOMMFAIL = XA_RBBASE + 1;
  static const int XA_RBDEADLOCK = XA_RBBASE + 2;
  static const int XA_RBINTEGRITY = XA_RBBASE + 3;
  static const int XA_RBOTHER = XA_RBBASE + 4;
  static const int XA_RBPROTO = XA_RBBASE + 5;
  static const int XA_RBTIMEOUT = XA_RBBASE + 6;
  static const int XA_RBTRANSIENT = XA_RBBASE + 7;
  static const int XA_RBEND = XA_RBTRANSIENT;
  static const int XA_NOMIGRATE = 9;
  static const int XA_HEURHAZ = 8;
  static const int XA_HEURCOM = 7;
  static const int XA_HEURRB = 6;
  static const int XA_HEURMIX = 5;
  static const int XA_RETRY = 4;
  static const int XA_RDONLY = 3;
  static const int XA_OK = 0;
  static const int XAER_ASYNC = -2;
  static const int XAER_RMERR = -3;
  static const int XAER_NOTA = -4;
  static const int XAER_INVAL = -5;
  static const int XAER_PROTO = -6;
  static const int XAER_RMFAIL = -7;
  static const int XAER_DUPID = -8;
  static const int XAER_OUTSIDE = -9;

  static int xa_xid(sql::ObExecContext &ctx,
                    sql::ParamStore &params,
                    common::ObObj &result);
  static int xa_start(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_end(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_prepare(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_commit(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_commit_with_flags(sql::ObExecContext &ctx,
                                  sql::ParamStore &params,
                                  common::ObObj &result);
  static int xa_forget(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_getlastoer(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_recover(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_rollback(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int xa_rollback_savepoint(sql::ObExecContext &ctx);
  static int xa_rollback_origin_savepoint(sql::ObExecContext &ctx);
  static int xa_settimeout(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);
  static int dist_txn_sync(sql::ObExecContext &ctx,
                        sql::ParamStore &params,
                        common::ObObj &result);

private:
  static int generate_xid_(const ObObjParam &obj, sql::ObXaStmt &stmt);
  static int switch_ret_code_(int code);
  static int read_xid_from_result(sqlclient::ObMySQLResult &mysql_result,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        common::ObIAllocator &allocator,
                        common::ObObj &result);
};

}//pl
}//oceanbase

#endif  //OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_XA_H_
