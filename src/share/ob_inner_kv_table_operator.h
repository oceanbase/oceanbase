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

#ifndef OCEANBASE_SHARE_OB_INNER_KV_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_INNER_KV_TABLE_OPERATOR_H_

#include "share/ob_inner_table_operator.h"
#include "share/ob_delegate.h"

namespace oceanbase
{
namespace share
{


// Define kv item value interface.
class ObInnerKVItemValue : public ObIInnerTableCol
{
public:
  ObInnerKVItemValue();
  virtual ~ObInnerKVItemValue() {}
};

// Define kv item value, of which value type is int.
class ObInnerKVItemIntValue : public ObInnerKVItemValue
{
public:
  ObInnerKVItemIntValue();
  virtual ~ObInnerKVItemIntValue() {}

  int set_value(const int64_t value);
  int64_t get_value() const;

  // Fill value to dml.
  int fill_value_dml(share::ObDMLSqlSplicer &dml) const override;

  // Parse value from sql result, make sure the result has column of value.
  int parse_value_from(common::sqlclient::ObMySQLResult &result) override;

  TO_STRING_KV(K_(value));

private:
  int64_t value_; // default 0.
};

// Define kv item value, of which value type is string.
class ObInnerKVItemStringValue : public ObInnerKVItemValue
{
public:
  typedef ObSqlString ValueType;

  ObInnerKVItemStringValue();
  virtual ~ObInnerKVItemStringValue() {}

  int set_value(const char *value);
  int set_value(const common::ObString &value);
  const char *get_value() const;

  // get obstring kv value
  // const common::ObString string() const;
  CONST_DELEGATE_WITH_RET(value_, string, const common::ObString);

  // Fill value to dml.
  int fill_value_dml(share::ObDMLSqlSplicer &dml) const override;

  // Parse value from sql result, make sure the result has column of value.
  int parse_value_from(common::sqlclient::ObMySQLResult &result) override;

  TO_STRING_KV(K_(value));

private:
  ValueType value_;
};


// Define one kv item, which tenant id is not part of the primary key of the kv table.
class ObInnerKVItem : public ObIInnerTableRow
{
public:
  typedef common::ObFixedLengthString<common::OB_INNER_TABLE_DEFAULT_KEY_LENTH> NameType;

  explicit ObInnerKVItem(ObInnerKVItemValue *value);
  virtual ~ObInnerKVItem() {}

  // Set the name of kv item.
  virtual int set_kv_name(const char *name);
  virtual const char *get_kv_name() const;

  virtual const ObInnerKVItemValue *get_kv_value() const;

  // Return if primary key is valid.
  bool is_pkey_valid() const override;
  // Return if primary key and value are valid.
  bool is_valid() const override;
  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;
  // Fill primary key and value to dml.
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;
  
  TO_STRING_KV(K_(name), K_(*value));

protected:
  NameType name_;
  ObInnerKVItemValue *value_;
}; 

// Define one kv item, which tenant id is part of the primary key of the kv table.
class ObInnerKVItemTenantIdWrapper : public ObInnerKVItem
{
public:
  explicit ObInnerKVItemTenantIdWrapper(ObInnerKVItem *item);
  virtual ~ObInnerKVItemTenantIdWrapper() {}

  int set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const;

  // Set the name of kv item.
  int set_kv_name(const char *name) override
  {
    return item_->set_kv_name(name);
  }

  const char *get_kv_name() const override
  {
    return item_->get_kv_name();
  }

  const ObInnerKVItemValue *get_kv_value() const override
  {
    return item_->get_kv_value();
  }


  bool is_pkey_valid() const override;
  bool is_valid() const override;
  // Fill primary key to dml.
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;
  // Fill primary key and value to dml.
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(*item));

private:
  static const char *TENANT_ID_COLUMN_NAME;
  uint64_t tenant_id_;
  ObInnerKVItem *item_;
};


// Define kv table operator.
class ObInnerKVTableOperator final : public ObIExecTenantIdProvider
{
public:
  ObInnerKVTableOperator();
  ~ObInnerKVTableOperator();

  uint64_t get_exec_tenant_id() const override
  {
    return operator_.get_exec_tenant_id();
  }

  // get name of the operation table
  // const char *get_table_name() const;
  CONST_DELEGATE_WITH_RET(operator_, get_table_name, const char *);

  // Get exec tenant id provider
  // const ObIExecTenantIdProvider *get_exec_tenant_id_provider() const;
  CONST_DELEGATE_WITH_RET(operator_, get_exec_tenant_id_provider, const ObIExecTenantIdProvider *);

  // Init operator with operation table name.
  int init(
    const char *tname, const ObIExecTenantIdProvider &exec_tenant_id_provider);

  int get_item(
    common::ObISQLClient &proxy, const bool need_lock, ObInnerKVItem &item) const;
  // Return failed if item exist.
  int insert_item(
    common::ObISQLClient &proxy, const ObInnerKVItem &item, int64_t &affected_rows) const;
  // Return failed if item not exist.
  int update_item(
    common::ObISQLClient &proxy, const ObInnerKVItem &item, int64_t &affected_rows) const;
  // Let item value empty.
  int clean_item(
    common::ObISQLClient &proxy, const ObInnerKVItem &item, int64_t &affected_rows) const;
  int insert_or_update_item(
    common::ObISQLClient &proxy, const ObInnerKVItem &item, int64_t &affected_rows) const;
  int delete_item(
    common::ObISQLClient &proxy, const ObInnerKVItem &item, int64_t &affected_rows) const;

  // Set column value to old_value + 'value'.
  int increase_value_by(
    common::ObISQLClient &proxy, const ObInnerKVItem &key, const int64_t value,
    int64_t &affected_rows) const;

  // Set column value to old_value + 1.
  int increase_value_by_one(
    common::ObISQLClient &proxy, const ObInnerKVItem &key, int64_t &affected_rows) const;
  
  // Set column value to old_value + 'value' and return the old_value.
  int fetch_and_add(
    common::ObMySQLTransaction &trans, const ObInnerKVItem &key, 
    const int64_t value, int64_t old_value, int64_t &affected_rows) const;
  
  // Set column value to old_value + 'value' and return the new_value.
  int add_and_fetch(
    common::ObMySQLTransaction &trans, const ObInnerKVItem &key, 
    const int64_t value, int64_t new_value, int64_t &affected_rows) const;
  
  int compare_and_swap(
    common::ObISQLClient &proxy, const ObInnerKVItem &key, 
    const int64_t old_value, int64_t new_value, int64_t &affected_rows) const;
  
  int compare_and_swap(
    common::ObISQLClient &proxy, const ObInnerKVItem &key, 
    const char *old_value, const char *new_value, int64_t &affected_rows) const;

  TO_STRING_KV(K_(is_inited), K_(operator));

private:
  bool is_inited_;
  ObInnerTableOperator operator_;
};

}
}
#endif