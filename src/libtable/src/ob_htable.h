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

#ifndef _OB_HTABLE_H
#define _OB_HTABLE_H 1
#include "ob_hkv_table.h"
namespace oceanbase
{
namespace table
{
/// HBase Interface
namespace hbase
{
class ObHMutation
{
public:
  ObHMutation();
  virtual ~ObHMutation();
  const ObString &get_row() const;

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHMutation);
  // function members
protected:
  // data members
  ObString rowkey_;
  int64_t ts_;
};

class ObHCell
{
public:
  const ObString &get_row();
  const ObString &get_column_family();
  const ObString &get_column_qualifier();
  int64_t get_timestamp();
  const ObString &get_value();
};

// http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html
class ObHPut: public ObHMutation
{
public:
  ObHPut(const ObString &row);
  virtual ~ObHPut();
  int add_column(const ObString &family, const ObString &qualifier, int64_t ts, const ObString &value);
  int64_t get_column_count() const;
  int get_column(int64_t i, ObHCell &cell);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHPut);
  // function members
private:
  // data members
};

// http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Delete.html
class ObHDelete: public ObHMutation
{
public:
  ObHDelete(const ObString &row);
  virtual ~ObHDelete();
  /// Delete the latest version of the specified column.
  int add_column(const ObString &family, const ObString &qualifier);
  /// Delete the specified version of the specified column.
  int add_column(const ObString &family, const ObString &qualifier, int64_t timestamp);
  /// Delete all versions of the specified column.
  int add_columns(const ObString &family, const ObString &qualifier);
  /// Delete all versions of the specified column with a timestamp less than or equal to the specified timestamp.
  int add_columns(const ObString &family, const ObString &qualifier, int64_t timestamp);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHDelete);
  // function members
private:
  // data members
};

class ObHRowMutation
{
public:
  ObHRowMutation(const ObString &row);
  virtual ~ObHRowMutation();

  int add(const ObHMutation &mutation);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHRowMutation);
  // function members
private:
  // data members
};

class ObHQuery
{
public:
  ObHQuery();
  virtual ~ObHQuery();
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHQuery);
  // function members
private:
  // data members
};

class ObHGet: public ObHQuery
{
public:
  ObHGet(const ObString &row);
  virtual ~ObHGet();
  /// Get the column from the specific family with the specified qualifier.
  int add_column(const ObString &family, const ObString &qualifier);
  /// Get all columns from the specified family.
  int add_family(const ObString &family);
  /// Get versions of columns with the specified timestamp.
  int set_timestamp(int64_t timestamp);
  /// Get versions of columns only within the specified timestamp range, [minStamp, maxStamp).
  int set_time_range(int64_t min_stamp, int64_t max_stamp);
  /// Get up to the specified number of versions of each column.
  int read_versions(int32_t versions);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHGet);
  // function members
private:
  // data members
};

/// http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Table.html
class ObHScan: public ObHQuery
{
public:
  ObHScan();
  virtual ~ObHScan();

  /// Set the start row of the scan.
  int with_start_row(const ObString &row, bool inclusive);
  /// Set the stop row of the scan.
  int with_stop_row(const ObString &row, bool inclusive);
  /// Get the column from the specified family with the specified qualifier.
  int add_column(const ObString &family, const ObString &qualifier);
  /// Get all columns from the specified family.
  int add_family(const ObString &family);
  /// Get versions of columns with the specified timestamp.
  int set_timestamp(int64_t timestamp);
  /// Get versions of columns only within the specified timestamp range, [minStamp, maxStamp).
  int set_time_range(int64_t min_stamp, int64_t max_stamp);
  /// Get up to the specified number of versions of each column.
  int read_versions(int32_t versions);
  int read_all_versions();
  /// Set the maximum number of cells to return for each call to next().
  int set_batch(int32_t batch_size);
  /// Set the limit of rows for this scan.
  /// We will terminate the scan if the number of returned rows reaches this value.
  /// @param limit - the limit of rows for this scan
  int set_limit(int32_t limit);
  /// Set the maximum result size.
  /// The default is -1; this means that no specific maximum result size will be set for this scan.
  /// @param max_result_size - The maximum result size in bytes.
  int set_max_result_size(int64_t max_result_size);
  /// Set the maximum number of values to return per row per Column Family
  /// @param limit - the maximum number of values returned / row / CF
  int set_max_results_per_column_family(int32_t limit);
  /// Set offset for the row per Column Family.
  /// @param offset - is the number of kvs that will be skipped.
  int set_row_offset_per_column_family(int32_t offset);
  /// Set whether this scan is a reversed one
  int set_reversed(bool reversed);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHScan);
  // function members
private:
  // data members
  common::ObArenaAllocator alloc_;
  ObString start_row_;
  ObString stop_row_;
  bool include_start_row_;
  bool include_stop_row_;
  ObArray<ObString> qualifiers_;
  int64_t timestamp_;
};

/// A HBase Table for demo
class ObHTable
{
public:
  ObHTable();
  virtual ~ObHTable();

  int put(const ObHPut &put);
  int multi_put(const ObIArray<ObHPut> &puts);
  int del(const ObHDelete &del);
  int multi_del(const ObIArray<ObHDelete> &deletes);
  /// Performs multiple mutations atomically on a single row.
  int mutate_row(const ObHRowMutations &row_mutations);
  int get(const ObHGet &get);
  int multi_get(const ObIArray<ObHGet> &gets);
  int scan(const ObHScan &scan);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTable);
  // function members
private:
  // data members
  ObHKVTable *hkv_table_;
};

} // end namespace hbase
} // end namespace table
} // end namespace oceanbase

#endif /* _OB_HTABLE_H */
