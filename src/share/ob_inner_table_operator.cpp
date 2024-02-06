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
#include "share/ob_inner_table_operator.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_smart_var.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace sqlclient;

/**
 * ------------------------------ObIInnerTableKey---------------------
 */
int ObIInnerTableKey::build_pkey_predicates(ObSqlString &predicates) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill pkey dml", K(ret));
  } else if (OB_FAIL(dml.splice_predicates(predicates))) {
    LOG_WARN("failed to splice predicates", K(ret), K(this));
  }

  return ret;
}

/**
 * ------------------------------ObIInnerTableCol---------------------
 */
int ObIInnerTableCol::set_column_name(const char *name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(ret), KP(name));
  } else if (OB_FAIL(name_.assign(name))) {
    LOG_WARN("failed to assign name", K(ret), K(name));
  }
  return ret;
}

const char *ObIInnerTableCol::get_column_name() const
{
  return name_.ptr();
}

int ObIInnerTableCol::build_predicates(ObSqlString &predicates) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_value_dml(dml))) {
    LOG_WARN("failed to fill value dml", K(ret));
  } else if (OB_FAIL(dml.splice_predicates(predicates))) {
    LOG_WARN("failed to splice predicates", K(ret), K(this));
  }

  return ret;
}


/**
 * ------------------------------ObIInnerTableRow---------------------
 */
int ObIInnerTableRow::build_assignments(ObSqlString &assignments) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_dml(dml))) {
    LOG_WARN("failed to fill dml", K(ret));
  } else if (OB_FAIL(dml.splice_assignments(assignments))) {
    LOG_WARN("failed to splice assignments", K(ret), K(this));
  }

  return ret;
}


/**
 * ------------------------------ObInnerTableOperator---------------------
 */
ObInnerTableOperator::ObInnerTableOperator()
  : is_inited_(false), table_name_(), exec_tenant_id_provider_(nullptr)
{

}

ObInnerTableOperator::~ObInnerTableOperator()
{

}

int ObInnerTableOperator::init(
    const char *tname, const ObIExecTenantIdProvider &exec_tenant_id_provider)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObInnerTableOperator init twice", K(ret));
  } else if (OB_ISNULL(tname)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty table name", K(ret), K(tname));
  } else if (OB_FAIL(table_name_.assign(tname))) {
    LOG_WARN("failed to assign table name", K(ret), K(tname));
  } else {
    exec_tenant_id_provider_ = &exec_tenant_id_provider;
    is_inited_ = true;
  }

  return ret;
}

uint64_t ObInnerTableOperator::get_exec_tenant_id() const
{
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_NOT_NULL(exec_tenant_id_provider_)) {
    tenant_id = exec_tenant_id_provider_->get_exec_tenant_id();
  }
  return tenant_id;
}

const char *ObInnerTableOperator::get_table_name() const
{
  return table_name_.ptr();
}

const ObIExecTenantIdProvider *ObInnerTableOperator::get_exec_tenant_id_provider() const
{
  return exec_tenant_id_provider_;
}

int ObInnerTableOperator::lock_row(
    ObMySQLTransaction &trans, const ObIInnerTableKey &key) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key));
  } else if (OB_FAIL(do_lock_row_(trans, key))) {
    LOG_WARN("failed to lock row", K(ret), K(key));
  }

  return ret;
}

int ObInnerTableOperator::get_row(
    ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key,
    ObIInnerTableRow &row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key));
  } else if (OB_FAIL(do_get_row_(proxy, need_lock, key, row))) {
    LOG_WARN("failed to get row", K(ret), K(need_lock), K(key));
  }

  return ret;
}

int ObInnerTableOperator::insert_row(ObISQLClient &proxy, const ObIInnerTableRow &row,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row not valid", K(ret), K(row));
  } else if (OB_FAIL(do_insert_row_(proxy, row, affected_rows))) {
    LOG_WARN("failed to insert row", K(ret), K(row));
  }

  return ret;
}

int ObInnerTableOperator::update_row(ObISQLClient &proxy, const ObIInnerTableRow &row,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row not valid", K(ret), K(row));
  } else if (OB_FAIL(do_update_row_(proxy, row, affected_rows))) {
    LOG_WARN("failed to update row", K(ret), K(row));
  }

  return ret;
}

int ObInnerTableOperator::insert_or_update_row(ObISQLClient &proxy, const ObIInnerTableRow &row,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row not valid", K(ret), K(row));
  } else if (OB_FAIL(do_insert_or_update_row_(proxy, row, affected_rows))) {
    LOG_WARN("failed to insert or update row", K(ret), K(row));
  }

  return ret;
}

int ObInnerTableOperator::delete_row(ObISQLClient &proxy, const ObIInnerTableKey &key, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key));
  } else if (OB_FAIL(do_delete_row_(proxy, key, affected_rows))) {
    LOG_WARN("failed to delete row", K(ret), K(key));
  }

  return ret;
}

int ObInnerTableOperator::get_column(
    ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
    ObIInnerTableCol &col) const
{
  int ret = OB_SUCCESS;

  const char *column_name = col.get_column_name();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(col), K(need_lock));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty column_name", K(ret), K(key), K(col), K(need_lock));
  } else if (OB_FAIL(do_get_column_(proxy, need_lock, key, col))) {
    LOG_WARN("failed to get column", K(ret), K(key), K(col), K(need_lock));
  }

  return ret;
}

int ObInnerTableOperator::get_int_column(
    ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
    const char *column_name, int64_t &value) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(need_lock));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty column_name", K(ret), K(key), K(column_name), K(need_lock));
  } else if (OB_FAIL(do_get_int_column_(proxy, need_lock, key, column_name, value))) {
    LOG_WARN("failed to get int column", K(ret), K(key), K(column_name), K(need_lock));
  }

  return ret;
}

int ObInnerTableOperator::get_string_column(
    common::ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
    const char *column_name, StringValueType &value) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(need_lock));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty column_name", K(ret), K(key), K(column_name), K(need_lock));
  } else if (OB_FAIL(do_get_string_column_(proxy, need_lock, key, column_name, value))) {
    LOG_WARN("failed to get string column", K(ret), K(key), K(column_name), K(need_lock));
  }

  return ret;
}

int ObInnerTableOperator::increase_column_by(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, const int64_t value,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(value));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty column name", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(do_increase_column_by_(proxy, key, column_name, value, affected_rows))) {
    LOG_WARN("fail to increase column", K(ret), K(key), K(column_name), K(value));
  }

  return ret;
}

int ObInnerTableOperator::increase_column_by_one(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, int64_t &affected_rows) const
{
  return increase_column_by(proxy, key, column_name, 1LL, affected_rows);
}

int ObInnerTableOperator::update_column(
    ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *assignments/* c1=v1, c2=v2 */, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(assignments));
  } else if (OB_ISNULL(assignments)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid assignments", K(ret), K(key), K(assignments));
  } else if (OB_FAIL(do_update_column_(proxy, key, assignments, affected_rows))) {
    LOG_WARN("fail to increase column", K(ret), K(key), K(assignments));
  }

  return ret;
}

int ObInnerTableOperator::update_column(
    ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const ObIInnerTableCol &col, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString predicates;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(col));
  }  else if (OB_FAIL(col.build_predicates(predicates))) {
    LOG_WARN("failed to build predicates", K(ret), K(key), K(col));
  } else if (OB_FAIL(do_update_column_(proxy, key, predicates.ptr(), affected_rows))) {
    LOG_WARN("fail to increase column", K(ret), K(key), K(predicates));
  }

  return ret;
}

int ObInnerTableOperator::update_int_column(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, const int64_t value,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(value));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_name not valid", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(sql.assign_fmt("%s=%ld", column_name, value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(do_update_column_(proxy, key, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to update column", K(ret), K(key), K(column_name), K(value));
  }

  return ret;
}

int ObInnerTableOperator::update_uint_column(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, const uint64_t value,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(value));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_name not valid", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(sql.assign_fmt("%s=%lu", column_name, value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(do_update_column_(proxy, key, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to update column", K(ret), K(key), K(column_name), K(value));
  }

  return ret;
}

int ObInnerTableOperator::update_string_column(
    ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, const char *value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(value));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_name", K(ret), K(key), K(column_name), K(value));
  } else if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(sql.assign_fmt("%s='%s'", column_name, value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(do_update_column_(proxy, key, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to update column", K(ret), K(key), K(column_name), K(value));
  }

  return ret;
}

int ObInnerTableOperator::compare_and_swap(
    ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *assignments/* eg, c1=v1, c2=v2 */, const char *predicates/* c3=v3 AND c4=v4 */,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_ISNULL(assignments)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid assignments", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_ISNULL(predicates)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid predicates", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_FAIL(do_compare_and_swap_(proxy, key, assignments, predicates, affected_rows))) {
    LOG_WARN("fail to compare_and_swap", K(ret), K(key), K(assignments), K(predicates));
  }

  return ret;
}

int ObInnerTableOperator::compare_and_swap(
    ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, const int64_t old_value, 
    const int64_t new_value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_name", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_FAIL(sql.assign_fmt("%s=%ld", column_name, new_value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_FAIL(predicates.assign_fmt("%s=%ld", column_name, old_value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_FAIL(do_compare_and_swap_(proxy, key, sql.ptr(), predicates.ptr(), affected_rows))) {
    LOG_WARN("fail to update column", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  }

  return ret;
}

int ObInnerTableOperator::compare_and_swap(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const char *old_value, 
      const char *new_value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerTableOperator not init", K(ret));
  } else if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key not valid", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_name", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_ISNULL(old_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old_value", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_ISNULL(new_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid new_value", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_FAIL(sql.assign_fmt("%s='%s'", column_name, new_value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_FAIL(predicates.assign_fmt("%s='%s'", column_name, old_value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  } else if (OB_FAIL(do_compare_and_swap_(proxy, key, sql.ptr(), predicates.ptr(), affected_rows))) {
    LOG_WARN("fail to update column", K(ret), K(key), K(column_name), K(old_value), K(new_value));
  }

  return ret;
}

int ObInnerTableOperator::do_lock_row_(
    ObMySQLTransaction &trans, const ObIInnerTableKey &key) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(this), K(key));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where %s for update", tname, predicates.ptr()))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(predicates));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(trans.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("row not exist, cannot lock", K(ret), K(sql), K(exec_tenant_id));
        } else {
          LOG_WARN("get next failed", K(ret), K(sql), K(exec_tenant_id));
        }
      } else {
        // lock successfully.
      }
    }
  }

  return ret;
}

int ObInnerTableOperator::do_get_row_(
    ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, ObIInnerTableRow &row) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(sql.assign_fmt("select * from %s", tname))) {
    LOG_WARN("fail to assign sql", K(ret), K(key));
  } else if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(key));
  } else if (OB_FAIL(sql.append_fmt(" where %s", predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(predicates));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to append sql", K(ret), K(key));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(parse_one_row_(*result, row))) {
        LOG_WARN("failed to parse row", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id), K(row));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObInnerTableOperator::do_insert_row_(ObISQLClient &proxy, const ObIInnerTableRow &row,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(row.fill_dml(dml))) {
    LOG_WARN("fail to fill dml", K(ret), K(this), K(row));
  } else if (OB_FAIL(dml.splice_insert_sql(tname, sql))) {
    LOG_WARN("failed to splice insert sql", K(ret), K(this), K(row));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("insert one row", K(row), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::do_update_row_(ObISQLClient &proxy, const ObIInnerTableRow &row,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(row.fill_dml(dml))) {
    LOG_WARN("fail to fill dml", K(ret), K(this), K(row));
  } else if (OB_FAIL(dml.splice_update_sql(tname, sql))) {
    LOG_WARN("failed to splice update sql", K(ret), K(this), K(row));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("update one row", K(row), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::do_insert_or_update_row_(ObISQLClient &proxy, const ObIInnerTableRow &row,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(row.fill_dml(dml))) {
    LOG_WARN("fail to fill dml", K(ret), K(this), K(row));
  } else if (OB_FAIL(dml.splice_insert_update_sql(tname, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(this), K(row));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("insert/update one row", K(row), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::do_delete_row_(
    ObISQLClient &proxy, const ObIInnerTableKey &key, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(key.fill_pkey_dml(dml))) {
    LOG_WARN("fail to fill pkey dml", K(ret), K(this), K(key));
  } else if (OB_FAIL(dml.splice_delete_sql(tname, sql))) {
    LOG_WARN("failed to splice delete sql", K(ret), K(this), K(key));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("delete one row", K(key), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::do_get_column_(
    ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
    ObIInnerTableCol &col) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();
  const char *column_name = col.get_column_name();

  if (OB_ISNULL(column_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid col", K(ret), K(key), K(col), K(need_lock));
  } else if (OB_FAIL(sql.assign_fmt("select %s from %s", column_name, tname))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(col), K(need_lock));
  } else if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(key), K(col), K(need_lock));
  } else if (OB_FAIL(sql.append_fmt(" where %s", predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(predicates));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to append sql", K(ret), K(key), K(col), K(need_lock));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(parse_one_column_(*result, col))) {
        LOG_WARN("failed to parse one column", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id), K(col));
      }
    }
  }

  return ret;
}

int ObInnerTableOperator::do_get_int_column_(
    ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key,
    const char *column_name, int64_t &value) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(sql.assign_fmt("select %s from %s", column_name, tname))) {
    LOG_WARN("fail to assign sql", K(ret), K(this), K(key), K(column_name));
  } else if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(this), K(key), K(column_name));
  } else if (OB_FAIL(sql.append_fmt(" where %s", predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(predicates));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to append sql", K(ret), K(this), K(key), K(column_name));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(parse_one_column_(*result, column_name, value))) {
        LOG_WARN("failed to parse one column", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id), K(value));
      }
    }
  }

  return ret;
}

int ObInnerTableOperator::do_get_string_column_(
    common::ObISQLClient &proxy, 
    const bool need_lock, 
    const ObIInnerTableKey &key,
    const char *column_name,
    StringValueType &value) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(sql.assign_fmt("select %s from %s", column_name, tname))) {
    LOG_WARN("fail to assign sql", K(ret), K(this), K(key), K(column_name));
  } else if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(this), K(key), K(column_name));
  } else if (OB_FAIL(sql.append_fmt(" where %s", predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(predicates));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to append sql", K(ret), K(this), K(key), K(column_name));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_FAIL(parse_one_column_(*result, column_name, value))) {
        LOG_WARN("failed to parse one column", K(ret), K(sql), K(exec_tenant_id));
      } else if (OB_ITER_END != result->next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi value exist", K(ret), K(sql), K(exec_tenant_id), K(value));
      }
    }
  }

  return ret;
}

int ObInnerTableOperator::do_increase_column_by_(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *column_name, const int64_t value,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_FAIL(sql.assign_fmt("update %s set %s=%s+%ld", tname, column_name, column_name, value))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(key), K(column_name), K(value));
  } else if (OB_FAIL(sql.append_fmt(" where %s", predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(predicates));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("update one column", K(key), K(column_name), K(value), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::do_update_column_(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *assignments /* c1=v1, c2=v2 */, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_ISNULL(assignments)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assignments not valid ", K(ret), K(key), K(assignments));
  } else if (OB_FAIL(sql.assign_fmt("update %s set %s", tname, assignments))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(assignments));
  } else if (OB_FAIL(key.build_pkey_predicates(predicates))) {
    LOG_WARN("fail to fill predicates", K(ret), K(key), K(assignments));
  } else if (OB_FAIL(sql.append_fmt(" where %s", predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(predicates));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("update one column", K(key), K(assignments), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::do_compare_and_swap_(
    common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
    const char *assignments/* c1=v1, c2=v2 */, const char *predicates/* c3=v3 AND c4=v4 */, 
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString pkey_predicates;

  const uint64_t exec_tenant_id = get_exec_tenant_id();
  const char *tname = get_table_name();

  if (OB_ISNULL(assignments)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assignments not valid ", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_ISNULL(predicates)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("predicates not valid ", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_FAIL(sql.assign_fmt("update %s set %s", tname, assignments))) {
    LOG_WARN("fail to assign sql", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_FAIL(key.build_pkey_predicates(pkey_predicates))) {
    LOG_WARN("fail to fill pkey_predicates", K(ret), K(key), K(assignments), K(predicates));
  } else if (OB_FAIL(sql.append_fmt(" where %s", pkey_predicates.ptr()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(pkey_predicates));
  } else if (OB_FAIL(sql.append_fmt(" and %s", predicates))) {
    LOG_WARN("failed to append sql", K(ret), K(sql), K(assignments), K(predicates));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("compare and swap one column", K(key), K(assignments), K(predicates), K(affected_rows), K(sql));
  }

  return ret;
}

int ObInnerTableOperator::parse_one_row_(sqlclient::ObMySQLResult &result, ObIInnerTableRow &row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("no row exist", K(ret));
  } else if (OB_FAIL(row.parse_from(result))) {
    LOG_WARN("failed to parse row", K(ret));
  }

  return ret;
}

int ObInnerTableOperator::parse_one_column_(
    sqlclient::ObMySQLResult &result, const char *column_name, int64_t &value) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("no row exist", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, column_name, value, int64_t);
  }

  return ret;
}

int ObInnerTableOperator::parse_one_column_(
    ObMySQLResult &result, const char *column_name, StringValueType &value) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("no row exist", K(ret));
  } else {
    ObString field;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, column_name, field);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(value.assign(field))) {
      LOG_WARN("failed to assign value", K(ret), K(field));
    }
  }

  return ret;
}

int ObInnerTableOperator::parse_one_column_(
    ObMySQLResult &result, ObIInnerTableCol &col) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("no row exist", K(ret));
  } else if (OB_FAIL(col.parse_value_from(result))) {
    LOG_WARN("failed to parse col", K(ret));
  }

  return ret;
}