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

#define USING_LOG_PREFIX SHARE

#include "ob_balance_task_helper_operator.h"
#include "lib/mysqlclient/ob_isql_client.h"//ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_proxy.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "share/inner_table/ob_inner_table_schema.h"//ALL_LS_BALANCE_TASK_HELPER_TNAME
#include "share/ob_dml_sql_splicer.h"//ObDMLSqlSplicer

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
static const char* LS_BALANCE_TASK_HELPER_OP_ARRAY[] =
{
  "ALTER_LS", "TRANSFER_BEGIN", "TRANSFER_END"
};

const char* ObBalanceTaskHelperOp::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(LS_BALANCE_TASK_HELPER_OP_ARRAY) == LS_BALANCE_TASK_OP_MAX, "array size mismatch");
  const char *op_str = "INVALID";
  if (OB_UNLIKELY(val_ >= ARRAYSIZEOF(LS_BALANCE_TASK_HELPER_OP_ARRAY) || val_ < 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fatal error, unknown balance ls_balance_task status", K_(val));
  } else {
    op_str = LS_BALANCE_TASK_HELPER_OP_ARRAY[val_];
  }
  return op_str;
}

ObBalanceTaskHelperOp::ObBalanceTaskHelperOp(const ObString &str)
{
  val_ = LS_BALANCE_TASK_OP_INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(LS_BALANCE_TASK_HELPER_OP_ARRAY); ++i) {
      if (0 == str.case_compare(LS_BALANCE_TASK_HELPER_OP_ARRAY[i])) {
        val_ = i;
        break;
      }
    }
  }
  if (LS_BALANCE_TASK_OP_INVALID == val_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid balance ls_balance_task status", K(val_), K(str));
  }
}
OB_SERIALIZE_MEMBER(ObBalanceTaskHelperOp, val_);

int ObBalanceTaskHelperMeta::init(
           const ObBalanceTaskHelperOp &task_op,
           const share::ObLSID &src_ls,
           const share::ObLSID &dest_ls,
           const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!task_op.is_valid()
                  || !src_ls.is_valid()
                  || OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_op), K(src_ls), K(dest_ls),
        K(ls_group_id));
  } else {
    task_op_ = task_op;
    src_ls_ = src_ls;
    dest_ls_ = dest_ls;
    ls_group_id_ = ls_group_id;
  }
  return ret;
}

bool ObBalanceTaskHelperMeta::is_valid() const
{
  return task_op_.is_valid()
         && src_ls_.is_valid()
         && OB_INVALID_ID != ls_group_id_;
}

void ObBalanceTaskHelperMeta::reset()
{
  task_op_.reset();
  src_ls_.reset();
  dest_ls_.reset();
  ls_group_id_ = OB_INVALID_ID;
}

int ObBalanceTaskHelperMeta::assign(const ObBalanceTaskHelperMeta &other)
{
  task_op_ = other.task_op_;
  src_ls_ = other.src_ls_;
  dest_ls_ = other.dest_ls_;
  ls_group_id_ = other.ls_group_id_;
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObBalanceTaskHelperMeta, task_op_, src_ls_,
    dest_ls_, ls_group_id_);

int ObBalanceTaskHelper::init(const uint64_t tenant_id,
           const share::SCN &operation_scn,
           const ObBalanceTaskHelperOp &task_op,
           const share::ObLSID &src_ls,
           const share::ObLSID &dest_ls,
           const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !operation_scn.is_valid()
                  || !task_op.is_valid()
                  || !src_ls.is_valid()
                  || OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_op), K(src_ls), K(dest_ls),
        K(ls_group_id), K(operation_scn));
  } else if (OB_FAIL(balance_task_helper_meta_.init(task_op, src_ls, dest_ls,
          ls_group_id))) {
    LOG_WARN("failed to init balance task helper meta", KR(ret), K(tenant_id), K(task_op), K(src_ls),
        K(dest_ls), K(ls_group_id), K(operation_scn));
  } else {
    operation_scn_ = operation_scn;
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObBalanceTaskHelper::init(const share::SCN &operation_scn,
    const uint64_t tenant_id,
    const ObBalanceTaskHelperMeta &balance_task_helper_meta)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!operation_scn.is_valid() || !balance_task_helper_meta.is_valid()
        || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(operation_scn), K(balance_task_helper_meta), K(tenant_id));
  } else if (OB_FAIL(balance_task_helper_meta_.assign(balance_task_helper_meta))) {
    LOG_WARN("failed to assign balance taks helper meta", KR(ret), K(balance_task_helper_meta));
  } else {
    operation_scn_ = operation_scn;
    tenant_id_ = tenant_id;
  }
  return ret;
}

bool ObBalanceTaskHelper::is_valid() const
{
  return operation_scn_.is_valid() && balance_task_helper_meta_.is_valid() && is_valid_tenant_id(tenant_id_);
}

void ObBalanceTaskHelper::reset()
{
  operation_scn_.reset();
  balance_task_helper_meta_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObBalanceTaskHelperTableOperator::insert_ls_balance_task(const ObBalanceTaskHelper &ls_balance_task,
                     ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  const uint64_t tenant_id = gen_meta_tenant_id(ls_balance_task.get_tenant_id());
  ObDMLExecHelper exec(client, tenant_id);
  if (OB_UNLIKELY(!ls_balance_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_balance_task is invalid", KR(ret), K(ls_balance_task));
  } else if (OB_FAIL(fill_dml_spliter(dml, ls_balance_task))) {
    LOG_WARN("failed to assign sql", KR(ret), K(ls_balance_task));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_BALANCE_TASK_HELPER_TNAME, dml, affected_rows))) {
    LOG_WARN("execute update failed", KR(ret), K(ls_balance_task), K(tenant_id));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObBalanceTaskHelperTableOperator::pop_task(const uint64_t tenant_id,
                      ObISQLClient &client,
                      ObBalanceTaskHelper &ls_balance_task)
{
  int ret = OB_SUCCESS;
  ls_balance_task.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s order by operation_scn asc limit 1",
                                    OB_ALL_BALANCE_TASK_HELPER_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else {
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("empty ls_balance_task", KR(ret), K(sql));
        } else {
          LOG_WARN("failed to get ls_balance_task", KR(ret), K(sql));
        }
      } else {
        ObString task_op_str;
        uint64_t op_scn_value = 0;
        SCN op_scn;
        int64_t src_ls = 0;
        int64_t dest_ls = 0;
        uint64_t ls_group_id = OB_INVALID_ID;
        EXTRACT_UINT_FIELD_MYSQL(*result, "operation_scn", op_scn_value, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "src_ls", src_ls, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "dest_ls", dest_ls, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "ls_group_id", ls_group_id, uint64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "operation_type", task_op_str);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get cell", KR(ret), K(task_op_str), K(op_scn), K(src_ls), K(dest_ls), K(ls_group_id));
        } else if (OB_FAIL(op_scn.convert_for_inner_table_field(op_scn_value))) {
          LOG_WARN("failed to convert for inner table", KR(ret), K(op_scn_value));
        } else if (OB_FAIL(ls_balance_task.init(tenant_id, op_scn, ObBalanceTaskHelperOp(task_op_str), ObLSID(src_ls),
                ObLSID(dest_ls), ls_group_id))) {
          LOG_WARN("failed to init ls_balance_task", KR(ret), K(tenant_id), K(task_op_str), K(op_scn), K(src_ls), K(ls_group_id));
        }
      }
    }
  }
  return ret;
}

int ObBalanceTaskHelperTableOperator::fill_dml_spliter(share::ObDMLSqlSplicer &dml,
                              const ObBalanceTaskHelper &ls_balance_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column("operation_type", ls_balance_task.get_task_op().to_str()))
      || OB_FAIL(dml.add_column("tenant_id", ls_balance_task.get_tenant_id()))
      || OB_FAIL(dml.add_column("operation_scn", ls_balance_task.get_operation_scn().get_val_for_inner_table_field()))
      || OB_FAIL(dml.add_column("src_ls", ls_balance_task.get_src_ls().id()))
      || OB_FAIL(dml.add_column("dest_ls", ls_balance_task.get_dest_ls().id()))
      || OB_FAIL(dml.add_column("ls_group_id", ls_balance_task.get_ls_group_id()))) {
    LOG_WARN("failed to fill dml spliter", KR(ret), K(ls_balance_task));
  }
  return ret;
}

int ObBalanceTaskHelperTableOperator::remove_task(const uint64_t tenant_id,
                       const share::SCN &operation_scn,
                       ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !operation_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(operation_scn));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where operation_scn = %lu", OB_ALL_BALANCE_TASK_HELPER_TNAME,
      operation_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(operation_scn));
  } else if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("expect one row", KR(ret), K(sql), K(affected_rows));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect one row", KR(ret), K(sql), K(affected_rows));
  }
  return ret;
}
}//end of share
}//end of ob
