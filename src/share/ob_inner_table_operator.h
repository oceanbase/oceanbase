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

#ifndef OCEANBASE_SHARE_OB_INNER_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_INNER_TABLE_OPERATOR_H_

#include "share/ob_i_exec_tenant_id_provider.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string_holder.h"

namespace oceanbase
{
namespace share
{

// Define table primary key interface.
class ObIInnerTableKey
{
public:
  virtual ~ObIInnerTableKey() {}

  // Build predicates with primary key.
  // "pk1=v1 AND pk2=v2"
  virtual int build_pkey_predicates(ObSqlString &predicates) const;

  // Return if primary key valid.
  virtual bool is_pkey_valid() const = 0;

  // Fill primary key to dml.
  virtual int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};


// Define table column interface.
class ObIInnerTableCol
{
public:
  virtual ~ObIInnerTableCol(){}

  int set_column_name(const char *name);
  const char *get_column_name() const;

  // Build predicates as 'column_name = column_value'
  virtual int build_predicates(ObSqlString &predicates) const;

  // Fill value to dml.
  virtual int fill_value_dml(share::ObDMLSqlSplicer &dml) const = 0;

  // Parse value from sql result, make sure the result has column of value.
  virtual int parse_value_from(common::sqlclient::ObMySQLResult &result) = 0;

  VIRTUAL_TO_STRING_KV(K_(name));

protected:
  typedef common::ObFixedLengthString<common::OB_MAX_COLUMN_NAME_BUF_LENGTH> ColumnName;
  ColumnName name_;
};


// Define table full row interface.
class ObIInnerTableRow : public ObIInnerTableKey
{
public:
  virtual ~ObIInnerTableRow() {}

  // "pk1=v1, pk2=v2, c3=v3, c4=v4"
  virtual int build_assignments(ObSqlString &assignments) const;

  // Return if both primary key and value are valid.
  virtual bool is_valid() const = 0;
  
  // Parse row from the sql result, the result has full columns.
  virtual int parse_from(common::sqlclient::ObMySQLResult &result) = 0;

  // Fill primary key and value to dml.
  virtual int fill_dml(share::ObDMLSqlSplicer &dml) const = 0;
};


// Define inner table operator.
class ObInnerTableOperator final : public ObIExecTenantIdProvider
{
public:
  typedef common::ObFixedLengthString<common::OB_MAX_TABLE_NAME_LENGTH> TableName;
  typedef ObSqlString StringValueType;

  ObInnerTableOperator();
  ~ObInnerTableOperator();

  // Return tenant id to execute sql.
  uint64_t get_exec_tenant_id() const override;

  // row operation
  // Init operator with operation table name.
  int init(
    const char *tname, const ObIExecTenantIdProvider &exec_tenant_id_provider, const int32_t group_id = 0);
  // Get operation table name.
  const char *get_table_name() const;
  const ObIExecTenantIdProvider *get_exec_tenant_id_provider() const;

  // Just only lock the specific row, return OB_ENTRY_NOT_EXIST if row not exist.
  int lock_row(
      common::ObMySQLTransaction &trans, const ObIInnerTableKey &key) const;
  // Get specific full row.
  int get_row(
      common::ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
      ObIInnerTableRow &row) const;
  // Return failed if row exist.
  int insert_row(
      common::ObISQLClient &proxy, const ObIInnerTableRow &row,
      int64_t &affected_rows) const;
  // Update full row, return OB_ENTRY_NOT_EXIST if row not exist.
  int update_row(
      common::ObISQLClient &proxy, const ObIInnerTableRow &row,
      int64_t &affected_rows) const;
  // Insert row if primary key not exist, otherwise update the row.
  int insert_or_update_row(
      common::ObISQLClient &proxy, const ObIInnerTableRow &row,
      int64_t &affected_rows) const;
  // Delete specific row.
  int delete_row(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, int64_t &affected_rows) const;


  // column operation
  int get_column(
      common::ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
      ObIInnerTableCol &col) const;
  // Get specific int column with primary key.
  int get_int_column(
      common::ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
      const char *column_name, int64_t &value) const;
  // Get specific string column with primary key.
  int get_string_column(
      common::ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
      const char *column_name, StringValueType &value) const;
  
  // Set column value to old_value + value.
  // Only column with type of int value can do this.
  int increase_column_by(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const int64_t value, int64_t &affected_rows) const;
  
  // Set column value to old_value + 1.
  // Only column with type of int value can do this.
  int increase_column_by_one(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, int64_t &affected_rows) const;
  
  // assignments: "c1=v1, c2=v2"
  int update_column(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *assignments, int64_t &affected_rows) const;
  
  int update_column(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const ObIInnerTableCol &col, int64_t &affected_rows) const;
  
  // Set new value to an int column directly.
  int update_int_column(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const int64_t value,
      int64_t &affected_rows) const;
  
  int update_uint_column(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const uint64_t value,
      int64_t &affected_rows) const;

  // Set new value to an string column directly.
  int update_string_column(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const char *value,
      int64_t &affected_rows) const;
  
  // Update column with prepared column assignments if only predicates is match.
  // assignments: c1=v1, c2=v2
  // predicates: c3=v3 AND c4=v4
  int compare_and_swap(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *assignments/* eg, value = 5 */, const char *predicates/* eg, status = 'DOING' */,
      int64_t &affected_rows) const;
  
  // Update column value to 'new_value' only if its old_value is equal to 'old_value'.
  int compare_and_swap(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const int64_t old_value, const int64_t new_value,
      int64_t &affected_rows) const;
  
  // Update column value to 'new_value' only if its old_value is equal to 'old_value'.
  int compare_and_swap(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const char *old_value, const char *new_value,
      int64_t &affected_rows) const;

  TO_STRING_KV(K_(is_inited), K_(table_name));

private:
  int do_lock_row_(
      common::ObMySQLTransaction &trans, 
      const ObIInnerTableKey &key) const;
  int do_get_row_(
      common::ObISQLClient &proxy, 
      const bool need_lock, 
      const ObIInnerTableKey &key,
      ObIInnerTableRow &row) const;
  int do_get_column_(
      common::ObISQLClient &proxy, const bool need_lock, const ObIInnerTableKey &key, 
      ObIInnerTableCol &col) const;
  int do_get_int_column_(
      common::ObISQLClient &proxy, 
      const bool need_lock, 
      const ObIInnerTableKey &key,
      const char *column_name,
      int64_t &value) const;
  int do_get_string_column_(
      common::ObISQLClient &proxy, 
      const bool need_lock, 
      const ObIInnerTableKey &key,
      const char *column_name,
      StringValueType &value) const;
  int do_insert_row_(
      common::ObISQLClient &proxy, const ObIInnerTableRow &row, int64_t &affected_rows) const;
  int do_update_row_(
      common::ObISQLClient &proxy, const ObIInnerTableRow &row, int64_t &affected_rows) const;
  int do_insert_or_update_row_(
      common::ObISQLClient &proxy, const ObIInnerTableRow &row, int64_t &affected_rows) const;
  int do_delete_row_(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, int64_t &affected_rows) const;
  

  int do_increase_column_by_(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *column_name, const int64_t value,
      int64_t &affected_rows) const;
  
  int do_update_column_(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *assigments, int64_t &affected_rows) const;
  
  int do_compare_and_swap_(
      common::ObISQLClient &proxy, const ObIInnerTableKey &key, 
      const char *assigments, const char *predicates,
      int64_t &affected_rows) const;

  int parse_one_row_(
      sqlclient::ObMySQLResult &result, ObIInnerTableRow &row) const;
  int parse_one_column_(
      sqlclient::ObMySQLResult &result, const char *column_name, int64_t &value) const;
  int parse_one_column_(
      sqlclient::ObMySQLResult &result, const char *column_name, StringValueType &value) const;
  int parse_one_column_(
      sqlclient::ObMySQLResult &result, ObIInnerTableCol &col) const;

private:
  bool is_inited_;
  TableName table_name_; // operation table name.
  const ObIExecTenantIdProvider *exec_tenant_id_provider_; // provide tenant id to exec sql.
  int32_t group_id_; //remote inner sql rpc queue
};





}
}
#endif
