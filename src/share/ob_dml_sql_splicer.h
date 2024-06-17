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

#ifndef OCEANBASE_SHARE_OB_DML_SQL_SPLICER_H_
#define OCEANBASE_SHARE_OB_DML_SQL_SPLICER_H_

#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_core_table_proxy.h"
namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObHexEscapeSqlStr;
namespace number
{
class ObNumber;
}
}

namespace share
{
// Splice dml sql, e.g: INSERT INTO, INSERT INTO ... ON DUPLICATE KEY UPDATE, REPLACE, UPDATE
//
// USAGE:
//   ObDMLSqlSplicer dml_splider;
//   if (OB_FAIL(dml_splider.add_pk_column("col1", 1))
//       || OB_FAIL(dml_splicer.add_pk_column("col2", 2))
//       || OB_FAIL(dml_splicer..add_column("col3", "str3"))) {
//    // log ....
//   }
//   ObSqString sql;
//
//   // result: INSERT INTO tname (col1, col2, col3) VALUES (1, 2, 'str3')
//   ret = dml_splicer.splice_insert_sql("tname", sql);
//
//   // result: INSERT INTO tname (col1, col2, col3) VALUES (1, 2, 'str3')
//   //         ON DUPLICATE KEY UPDATE col3 = 'str3'
//   ret = dml_splicer.splice_insert_update_sql("tname", sql);
//
//   // result: UPDATE tname SET col3 = 'str3' WHERE col1 = 1 AND col2 = 2
//   ret = dml_splicer.splice_update_sql("tname", sql);
//
class ObRealUInt64
{
public:
  ObRealUInt64(const uint64_t v) : v_(v) { }
  uint64_t value() const { return v_; }
  TO_STRING_KV(K_(v));
private:
  uint64_t v_;
};

class ObDMLSqlSplicer
{
public:
  friend class ObPTSqlSplicer;
  // for columns with NULL value
  static const char *const NULL_VALUE;
  static const int64_t MAX_TO_STRING_BUF_SIZE = common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
  static const int64_t DEF_COLUMN_CNT = 10;

  enum Mode
  {
    QUOTE_STRING_MODE, // add quote string to string value
    NAKED_VALUE_MODE, // don't quote string
  };
  explicit ObDMLSqlSplicer(Mode mode = QUOTE_STRING_MODE) : mode_(mode), is_hex_value_(false) {}
  virtual ~ObDMLSqlSplicer() {}

  void reset();
  void reuse() { reset(); }

  // add columns. (NOTE: only int and string values supported right now)
  template <typename T>
      int add_column(const char *col_name, const T &value);
  template <typename T>
      int add_pk_column(const char *col_name, const T &value);
  int add_gmt_modified(const int64_t now = -1) { return add_time_column("gmt_modified", now); }
  int add_gmt_create(const int64_t now = -1) { return add_time_column("gmt_create", now); }

  common::ObSqlString &get_extra_condition() { return extra_condition_; }
  // Append value to values_, then add column with only %col_name.
  // NOTE: caller must known how ObDMLSqlSplicer works.
  common::ObSqlString &get_values() { return values_; }

  int add_pk_column(const bool is_null, const char *col_name);
  int add_column(const bool is_null, const char *col_name);
  int add_uint64_pk_column(const char *col_name, const uint64_t value);
  int add_uint64_column(const char *col_name, const uint64_t value);
  int add_time_column(const char *col_name, const int64_t now, bool is_pk = false);
  int add_raw_time_column(const char *col_name, const int64_t now);
  int add_long_double_column(const char *col_name, const double value);
  // mark end of one row
  int finish_row();

  /// functions to splice sql string:
  int splice_insert_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_insert_sql_without_plancache(const char *table_name, common::ObSqlString &sql) const;
  int splice_insert_ignore_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_insert_update_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_replace_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_update_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_delete_sql(const char *table_name, common::ObSqlString &sql) const;
  // "pk1, pk2, c3, c4"
  int splice_column_names(common::ObSqlString &sql) const;
  // "v1, v2, v3, v4"
  int splice_values(common::ObSqlString &sql) const;
 // "pk1=v1, pk2=v2, c3=v3, c4=v4"
  int splice_assignments(common::ObSqlString &sql) const;
  // "pk1=v1 AND pk2=v2 AND c3=v3 AND c4=v4"
  int splice_predicates(common::ObSqlString &sql) const;
  // "SELECT 1 FROM %table_name WHERE pk1 = value1 AND pk2 = value2 ..."
  int splice_select_1_sql(const char *table_name, common::ObSqlString &sql) const;

  int splice_core_cells(ObCoreTableProxy &kv_proxy,
      common::ObIArray<ObCoreTableProxy::UpdateCell> &cells);

  // functions to splice batch sql statement
  int splice_batch_insert_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_batch_insert_update_sql(
      const char *table_name,
      common::ObSqlString &sql) const;
  int splice_batch_replace_sql_without_plancache(const char *table_name, common::ObSqlString &sql) const;
  int splice_batch_replace_sql(const char *table_name, common::ObSqlString &sql) const;
  int splice_batch_delete_sql(const char *table_name, common::ObSqlString &sql) const;
  // "(c1, c2, c3) IN ((v11, v12, v13), (v21, v22, v23))"
  int splice_batch_predicates_sql(common::ObSqlString &sql) const;
  int64_t get_row_count() const { return rows_end_pos_.count(); }
private:
  struct Column
  {
    const char *name_;
    // end position of %ObDMLSqlSplicer::values_
    int64_t value_end_pos_;
    bool primary_key_;
    bool is_null_;
    bool is_hex_value_;

    TO_STRING_KV(K_(primary_key), K_(is_null),
                 K_(name), K_(value_end_pos));

    Column()
        :name_(NULL), value_end_pos_(0),
         primary_key_(false), is_null_(false),
         is_hex_value_(false)
    {}
  };

  struct ColSet {
      enum Type {
      ALL,
      ONLY_PK,
      FILTER_PK,
    };
  };
  struct ValSet {
    enum Type {
      ALL,
      ONLY_COL_NAME,
      ONLY_VALUE,
    };
  };
  // splice columns to sql, e.g:
  //   col_name, col_name, col_name
  //   value, value, value
  //   col_name = value, col_name = value, col_name = value
  //   col_name = value AND col_name = value AND col_name = value
  int splice_column(const char *sep, const ColSet::Type col_set, const ValSet::Type val_set,
      common::ObSqlString &sql) const;

  int append_uint64_value(const uint64_t value, bool &is_null);
  int append_value(const uint64_t value, bool &is_null);
  int append_value(const int64_t value, bool &is_null);
  int append_value(const uint32_t value, bool &is_null);
  int append_value(const int32_t value, bool &is_null);
  int append_value(const uint16_t value, bool &is_null);
  int append_value(const int16_t value, bool &is_null);
  int append_value(const uint8_t value, bool &is_null);
  int append_value(const int8_t value, bool &is_null);
  int append_value(const bool value, bool &is_null);
  int append_value(const double value, bool &is_null);
  int append_value(const char *str, bool &is_null);
  int append_value(char *str, bool &is_null);
  int append_value(const common::number::ObNumber &nmb, bool &is_null);
  int append_value(const common::ObString &str, bool &is_null);
  int append_value(const common::ObHexEscapeSqlStr &escape_str, bool &is_null);
  int append_value(const ObRealUInt64 &value, bool &is_null);
  int append_value(const common::ObObj &obj, bool &is_null);
  template<typename T> int append_value(const T &obj, bool &is_null, common::FalseType);
  template<typename T> int append_value(const T &obj, bool &is_null, common::TrueType);
  template<typename T> int append_value(const T &obj, bool &is_null);

  template <typename T>
    int add_column(const bool is_primary_key, const char *col_name, const T &value);

  int add_column(const bool is_primary_key, const bool is_null, const char *col_name);
  int add_uint64_column(const bool is_primary_key, const char *col_name, const uint64_t value);
  int splice_insert(const char *table_name, const char *head, common::ObSqlString &sql) const;

  int build_rows_matrix(common::ObIArray<common::ObString> &all_names, common::ObIArray<int64_t> &rows_matrix) const;
  int splice_rows_matrix(const common::ObArray<common::ObString> &all_names, const common::ObArray<int64_t> &rows_matrix, common::ObSqlString &sql) const;
  int splice_batch_insert(const char *table_name, const char *head, common::ObSqlString &sql,
                          common::ObArray<common::ObString> &all_names, common::ObArray<int64_t> &rows_matrix) const;
  int splice_batch_predicates(const common::ObArray<common::ObString> &all_names, const common::ObArray<int64_t> &rows_matrix, common::ObSqlString &sql) const;
private:
  Mode mode_;
  common::ObSqlString values_;
  common::ObSEArray<Column, DEF_COLUMN_CNT> columns_;
  // additional condition for update sql
  common::ObSqlString extra_condition_;
  // mark value string is hex
  bool is_hex_value_;
  common::ObSEArray<int64_t, DEF_COLUMN_CNT> rows_end_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObDMLSqlSplicer);
};

class ObPTSqlSplicer : public ObDMLSqlSplicer
{
public:
  ObPTSqlSplicer() {}
  ~ObPTSqlSplicer() {}
  int splice_batch_insert_update_replica_sql(
      const char *table_name,
      const bool with_role,
      common::ObSqlString &sql) const;
  int splice_insert_update_replica_sql(const char *table_name, common::ObSqlString &sql) const;
private:
  int splice_batch_insert_update_replica_column(
      const bool with_role,
      const common::ObString &sep,
      const common::ObIArray<common::ObString> &names,
      common::ObSqlString &sql) const ;
  int splice_insert_update_replica_column(const char *sep,
                                          common::ObSqlString &sql) const;
};

#define OBJ_K(obj, name) #name, (obj).name##_
#define OBJ_GET_K(obj, name) #name, (obj).get_##name()

// help execute dml sql
class ObDMLExecHelper
{
public:
  ObDMLExecHelper(common::ObISQLClient &sql_client, const uint64_t tenant_id)
      : tenant_id_(tenant_id), sql_client_(sql_client) {}
  virtual ~ObDMLExecHelper() {}

  int exec_insert(const char *table_name, const ObDMLSqlSplicer &splicer,
      int64_t &affected_rows);
  int exec_insert_ignore(const char *table_name, const ObDMLSqlSplicer &splicer,
      int64_t &affected_rows);
  int exec_insert_update(const char *table_name, const ObDMLSqlSplicer &splicer,
      int64_t &affected_rows);
  int exec_replace(const char *table_name, const ObDMLSqlSplicer &splicer,
      int64_t &affected_rows);
  int exec_update(const char *table_name, const ObDMLSqlSplicer &splicer,
      int64_t &affected_rows);
   int exec_delete(const char *table_name, const ObDMLSqlSplicer &splicer,
                  int64_t &affected_rows);
private:
  int check_row_exist(const char *table_name, const ObDMLSqlSplicer &splicer,
                      bool &exist);
  uint64_t tenant_id_;
  common::ObISQLClient &sql_client_;
};

template<typename T>
int ObDMLSqlSplicer::append_value(const T &obj, bool &is_null, common::FalseType)
{
  int ret = common::OB_SUCCESS;
  char buf[MAX_TO_STRING_BUF_SIZE];
  int64_t pos = 0;
  if (0 > (pos = obj.to_string(buf, MAX_TO_STRING_BUF_SIZE)) || pos >= MAX_TO_STRING_BUF_SIZE) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SHARE_LOG(WARN, "obj to_string failed", K(pos), K(ret));
  } else {
    if (0 == pos) {
      is_null = true;
    } else {
      is_null = false;
      if (OB_FAIL(values_.append_fmt(mode_ == NAKED_VALUE_MODE ? "%.*s" : "'%.*s'",
          static_cast<int32_t>(pos), buf))) {
        SHARE_LOG(WARN, "append value failed", K(pos), K(ret));
      }
    }
  }
  return ret;
}

template <typename T>
int ObDMLSqlSplicer::append_value(const T &obj, bool &is_null, common::TrueType)
{
  is_null = false;
  return values_.append_fmt("%ld", static_cast<int64_t>(obj));
}

template <typename T>
int ObDMLSqlSplicer::append_value(const T &obj, bool &is_null)
{
  return append_value(obj, is_null, common::BoolType<__is_enum(T) >());
}

template <typename T>
int ObDMLSqlSplicer::add_column(const char *col_name, const T &value)
{
  const bool is_pk = false;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(add_column(is_pk, col_name, value))) {
    SHARE_LOG(WARN, "add column failed", K(ret), K(is_pk), K(col_name), K(value));
  }
  return ret;
}

template <typename T>
int ObDMLSqlSplicer::add_pk_column(const char *col_name, const T &value)
{
  const bool is_pk = true;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(add_column(is_pk, col_name, value))) {
    SHARE_LOG(WARN, "add column failed", K(ret), K(is_pk), K(col_name), K(value));
  }
  return ret;
}

template <typename T>
int ObDMLSqlSplicer::add_column(
    const bool is_primary_key, const char *col_name, const T &value)
{
  bool is_null = false;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(append_value(value, is_null))) {
    SHARE_LOG(WARN, "append value failed", K(ret), K(value));
  } else if (OB_FAIL(add_column(is_primary_key, is_null, col_name))) {
    SHARE_LOG(WARN, "add column failed", K(ret));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_DML_SQL_SPLICER_H_
