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
#include "share/ob_inner_kv_table_operator.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_smart_var.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace sqlclient;

/**
 * ------------------------------ObInnerKVItemValue---------------------
 */
ObInnerKVItemValue::ObInnerKVItemValue()
{
  const char *column_name = "name";
  name_.assign(column_name);
}

/**
 * ------------------------------ObInnerKVItemIntValue---------------------
 */
ObInnerKVItemIntValue::ObInnerKVItemIntValue()
  : value_(0)
{

}

int ObInnerKVItemIntValue::set_value(const int64_t value)
{
  int ret = OB_SUCCESS;
  value_ = value;
  return ret;
}

int64_t ObInnerKVItemIntValue::get_value() const
{
  return value_;
}

int ObInnerKVItemIntValue::fill_value_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column("value", value_))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

// Parse value from sql result, make sure the result has column of value.
int ObInnerKVItemIntValue::parse_value_from(sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObString value_str;
  ObSqlString bak_value_str;
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "value", value_str);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bak_value_str.assign(value_str))) {
    LOG_WARN("failed to assign str", K(ret), K(value_str));
  } else if (OB_FAIL(ob_atoll(bak_value_str.ptr(), value))) {
    LOG_WARN("failed to parse int", K(ret), K(bak_value_str));
  } else if (OB_FAIL(set_value(value))) {
    LOG_WARN("failed to set value", K(ret), K(value), K(bak_value_str));
  }

  return ret;
}


/**
 * ------------------------------ObInnerKVItemStringValue---------------------
 */
ObInnerKVItemStringValue::ObInnerKVItemStringValue()
  : value_()
{

}

int ObInnerKVItemStringValue::set_value(const char *value)
{
  int ret = OB_SUCCESS;
  ObString str(value);
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value", K(ret), KP(value));
  } else if (OB_FAIL(set_value(str))) {
    LOG_WARN("failed to set value", K(ret), K(str));
  }
  return ret;
}

int ObInnerKVItemStringValue::set_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(value_.assign(value))) {
    LOG_WARN("failed to assign value", K(ret), K(value));
  }
  return ret;
}

const char *ObInnerKVItemStringValue::get_value() const
{
  return value_.ptr();
}

int ObInnerKVItemStringValue::fill_value_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column("value", value_.string()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

int ObInnerKVItemStringValue::parse_value_from(sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;

  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, value, (*this));

  return ret;
}



/**
 * ------------------------------ObInnerKVItem---------------------
 */
ObInnerKVItem::ObInnerKVItem(ObInnerKVItemValue *value)
  : name_(), value_(value)
{

}

int ObInnerKVItem::set_kv_name(const char *name)
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

const char *ObInnerKVItem::get_kv_name() const
{
  return name_.ptr();
}

const ObInnerKVItemValue *ObInnerKVItem::get_kv_value() const
{
  return value_;
}

bool ObInnerKVItem::is_pkey_valid() const
{
  return !name_.is_empty();
}

// Return if primary key valid.
bool ObInnerKVItem::is_valid() const
{
  return is_pkey_valid() && nullptr != value_;
}

int ObInnerKVItem::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("name", name_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

int ObInnerKVItem::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a valid item", K(ret), K(this));
  } else if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill pkey dml", K(ret));
  } else if (OB_FAIL(value_->fill_value_dml(dml))) {
    LOG_WARN("failed to fill value dml", K(ret));
  }

  return ret;
}

// Parse one full item from sql result, the result has full columns.
int ObInnerKVItem::parse_from(sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
  
  EXTRACT_STRBUF_FIELD_MYSQL(result, "name", name_str, OB_INNER_TABLE_DEFAULT_KEY_LENTH, real_length);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_kv_name(name_str))) {
    LOG_WARN("failed to set name", K(ret), K(name_str));
  } else if (OB_FAIL(value_->parse_value_from(result))) {
    LOG_WARN("failed to parse value", K(ret), K(name_str));
  } 

  return ret;
}


/**
 * ------------------------------ObInnerKVItemTenantIdWrapper---------------------
 */
const char *ObInnerKVItemTenantIdWrapper::TENANT_ID_COLUMN_NAME = "tenant_id";

ObInnerKVItemTenantIdWrapper::ObInnerKVItemTenantIdWrapper(ObInnerKVItem *item)
  : ObInnerKVItem(nullptr), tenant_id_(OB_INVALID_TENANT_ID), item_(item)
{

}

int ObInnerKVItemTenantIdWrapper::set_tenant_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if(OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

uint64_t ObInnerKVItemTenantIdWrapper::get_tenant_id() const
{
  return tenant_id_;
}

// Return if primary key valid.
bool ObInnerKVItemTenantIdWrapper::is_pkey_valid() const
{
  return nullptr != item_ && item_->is_pkey_valid() && (is_sys_tenant(tenant_id_) || is_user_tenant(tenant_id_));
}

bool ObInnerKVItemTenantIdWrapper::is_valid() const
{
  return nullptr != item_ && is_pkey_valid() && item_->is_valid();
}

int ObInnerKVItemTenantIdWrapper::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column(TENANT_ID_COLUMN_NAME, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(item_->fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill item pkey dml", K(ret));
  }

  return ret;
}

int ObInnerKVItemTenantIdWrapper::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a valid item", K(ret), K(this));
  } else if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill pkey dml", K(ret));
  } else if (OB_FAIL(get_kv_value()->fill_value_dml(dml))) {
    LOG_WARN("failed to fill value dml", K(ret));
  }

  return ret;
}

// Parse one full item from sql result, the result has full columns.
int ObInnerKVItemTenantIdWrapper::parse_from(sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  EXTRACT_INT_FIELD_MYSQL(result, TENANT_ID_COLUMN_NAME, tenant_id, uint64_t);
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_tenant_id(tenant_id))) {
    LOG_WARN("failed to set tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(item_->parse_from(result))) {
    LOG_WARN("failed to parse result", K(ret), K(*this));
  }

  return ret;
}


/**
 * ------------------------------ObInnerKVTableOperator---------------------
 */
ObInnerKVTableOperator::ObInnerKVTableOperator()
  : is_inited_(false), operator_()
{

}

ObInnerKVTableOperator::~ObInnerKVTableOperator()
{

}

int ObInnerKVTableOperator::init(
  const char *tname, const ObIExecTenantIdProvider &exec_tenant_id_provider)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObInnerKVTableOperator init twice", K(ret));
  } else if (OB_FAIL(operator_.init(tname, exec_tenant_id_provider))) {
    LOG_WARN("failed to init operator", K(ret), K(tname));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObInnerKVTableOperator::get_item(ObISQLClient &proxy, const bool need_lock, ObInnerKVItem &item) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.get_row(proxy, need_lock, item, item))) {
    LOG_WARN("failed to get item", K(ret), K(item), K(need_lock));
  }

  return ret;
}

int ObInnerKVTableOperator::insert_item(ObISQLClient &proxy, const ObInnerKVItem &item,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.insert_row(proxy, item, affected_rows))) {
    LOG_WARN("failed to insert item", K(ret), K(item));
  }

  return ret;
}

int ObInnerKVTableOperator::update_item(ObISQLClient &proxy, const ObInnerKVItem &item,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.update_row(proxy, item, affected_rows))) {
    LOG_WARN("failed to update item", K(ret), K(item));
  }

  return ret;
}

int ObInnerKVTableOperator::clean_item(ObISQLClient &proxy, const ObInnerKVItem &item,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  const char *column_name = "value";
  const char *empty_value = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.update_string_column(proxy, item, column_name, empty_value, affected_rows))) {
    LOG_WARN("failed to clean item", K(ret), K(item));
  }

  return ret;
}


int ObInnerKVTableOperator::insert_or_update_item(ObISQLClient &proxy, const ObInnerKVItem &item,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.insert_or_update_row(proxy, item, affected_rows))) {
    LOG_WARN("failed to insert/update item", K(ret), K(item));
  }

  return ret;
}

int ObInnerKVTableOperator::delete_item(ObISQLClient &proxy, const ObInnerKVItem &item,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.delete_row(proxy, item, affected_rows))) {
    LOG_WARN("failed to delete item", K(ret), K(item));
  }

  return ret;
}

int ObInnerKVTableOperator::increase_value_by(
    ObISQLClient &proxy, const ObInnerKVItem &key, const int64_t value,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.increase_column_by(proxy, key, "value", value, affected_rows))) {
    LOG_WARN("failed to increase value by", K(ret), K(key), K(value));
  }

  return ret;
}

int ObInnerKVTableOperator::increase_value_by_one(
    ObISQLClient &proxy, const ObInnerKVItem &key, int64_t &affected_rows) const
{
  return increase_value_by(proxy, key, 1LL, affected_rows);
}

// Set column value to old_value + 'value' and return the old_value.
int ObInnerKVTableOperator::fetch_and_add(
  ObMySQLTransaction &trans, const ObInnerKVItem &key, 
  const int64_t value, int64_t old_value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  const bool need_lock = true;
  const char *column_name = "value";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.get_int_column(trans, need_lock, key, column_name, old_value))) {
    LOG_WARN("failed to get int value", K(ret), K(key));
  } else if (OB_FAIL(operator_.increase_column_by(trans, key, column_name, value, affected_rows))) {
    LOG_WARN("failed to increase column value", K(ret), K(key), K(value));
  }

  return ret;
}

// Set column value to old_value + 'value' and return the new_value.
int ObInnerKVTableOperator::add_and_fetch(
  ObMySQLTransaction &trans, const ObInnerKVItem &key, 
  const int64_t value, int64_t new_value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  int64_t old_value = 0;
  const bool need_lock = true;
  const char *column_name = "value";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.get_int_column(trans, need_lock, key, column_name, old_value))) {
    LOG_WARN("failed to get int value", K(ret), K(key));
  } else if (OB_FAIL(operator_.increase_column_by(trans, key, column_name, value, affected_rows))) {
    LOG_WARN("failed to increase column value", K(ret), K(key), K(old_value), K(value));
  } else if (OB_FAIL(operator_.get_int_column(trans, need_lock, key, column_name, new_value))) {
    LOG_WARN("failed to get int value", K(ret), K(key), K(old_value), K(value));
  } 

  return ret;
}

int ObInnerKVTableOperator::compare_and_swap(
  ObISQLClient &proxy, const ObInnerKVItem &key, 
  const int64_t old_value, int64_t new_value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  const char *column_name = "value";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.compare_and_swap(proxy, key, column_name, old_value, new_value, affected_rows))) {
    LOG_WARN("failed to compare and swap", K(ret), K(key), K(old_value), K(new_value));
  } 

  return ret;
}

int ObInnerKVTableOperator::compare_and_swap(
  ObISQLClient &proxy, const ObInnerKVItem &key, 
  const char *old_value, const char *new_value, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  const char *column_name = "value";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInnerKVTableOperator not init", K(ret));
  } else if (OB_FAIL(operator_.compare_and_swap(proxy, key, column_name, old_value, new_value, affected_rows))) {
    LOG_WARN("failed to compare and swap", K(ret), K(key), K(old_value), K(new_value));
  } 

  return ret;
}