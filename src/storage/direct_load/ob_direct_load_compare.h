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
#pragma once

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "storage/blocksstable/ob_datum_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageDatumUtils;
class ObDatumRowkey;
class ObDatumRow;
} // namespace blocksstable
namespace storage
{
class ObDirectLoadDatumArray;
class ObDirectLoadConstDatumArray;
class ObDirectLoadExternalRow;
class ObDirectLoadExternalMultiPartitionRow;
class ObDirectLoadConstExternalMultiPartitionRow;

class ObDirectLoadDatumRowkeyCompare
{
public:
  ObDirectLoadDatumRowkeyCompare() : datum_utils_(nullptr), result_code_(common::OB_SUCCESS) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils);
  int compare(const blocksstable::ObDatumRowkey *lhs, const blocksstable::ObDatumRowkey *rhs,
              int &cmp_ret);
  bool operator()(const blocksstable::ObDatumRowkey *lhs, const blocksstable::ObDatumRowkey *rhs);
  int get_error_code() const { return result_code_; }

public:
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  int result_code_;
};

class ObDirectLoadDatumRowCompare
{
public:
  ObDirectLoadDatumRowCompare()
    : rowkey_size_(0), result_code_(common::OB_SUCCESS), is_inited_(false)
  {
  }
  int init(const blocksstable::ObStorageDatumUtils &datum_utils, int64_t rowkey_size);
  int compare(const blocksstable::ObDatumRow *lhs, const blocksstable::ObDatumRow *rhs,
              int &cmp_ret);
  bool operator()(const blocksstable::ObDatumRow *lhs, const blocksstable::ObDatumRow *rhs);
  int get_error_code() const { return result_code_; }

public:
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
  ObDirectLoadDatumRowkeyCompare rowkey_compare_;
  int64_t rowkey_size_;
  int result_code_;
  bool is_inited_;
};

class ObDirectLoadDatumArrayCompare
{
public:
  ObDirectLoadDatumArrayCompare() : result_code_(common::OB_SUCCESS), is_inited_(false) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils);
  bool operator()(const ObDirectLoadDatumArray *lhs, const ObDirectLoadDatumArray *rhs);
  int compare(const ObDirectLoadDatumArray *lhs, const ObDirectLoadDatumArray *rhs, int &cmp_ret);
  bool operator()(const ObDirectLoadConstDatumArray *lhs, const ObDirectLoadConstDatumArray *rhs);
  int compare(const ObDirectLoadConstDatumArray *lhs, const ObDirectLoadConstDatumArray *rhs,
              int &cmp_ret);
  int get_error_code() const { return result_code_; }

public:
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
  ObDirectLoadDatumRowkeyCompare rowkey_compare_;
  int result_code_;
  bool is_inited_;
};

class ObDirectLoadExternalRowCompare
{
public:
  ObDirectLoadExternalRowCompare() : result_code_(common::OB_SUCCESS), ignore_seq_no_(false), is_inited_(false) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils,
           sql::ObLoadDupActionType dup_action, bool ignore_seq_no = false);
  int compare(const ObDirectLoadExternalRow *lhs, const ObDirectLoadExternalRow *rhs, int &cmp_ret);
  bool operator()(const ObDirectLoadExternalRow *lhs, const ObDirectLoadExternalRow *rhs);
  int get_error_code() const { return result_code_; }

public:
  ObDirectLoadDatumArrayCompare datum_array_compare_;
  sql::ObLoadDupActionType dup_action_;
  int result_code_;
  bool ignore_seq_no_;
  bool is_inited_;
};

class ObDirectLoadExternalMultiPartitionRowCompare
{
public:
  ObDirectLoadExternalMultiPartitionRowCompare()
    : result_code_(common::OB_SUCCESS), ignore_seq_no_(false), is_inited_(false)
  {
  }
  int init(const blocksstable::ObStorageDatumUtils &datum_utils,
           sql::ObLoadDupActionType dup_action, bool ignore_seq_no = false);
  bool operator()(const ObDirectLoadExternalMultiPartitionRow *lhs,
                  const ObDirectLoadExternalMultiPartitionRow *rhs);
  bool operator()(const ObDirectLoadConstExternalMultiPartitionRow *lhs,
                  const ObDirectLoadConstExternalMultiPartitionRow *rhs);
  int compare(const ObDirectLoadExternalMultiPartitionRow *lhs,
              const ObDirectLoadExternalMultiPartitionRow *rhs, int &cmp_ret);
  int compare(const ObDirectLoadConstExternalMultiPartitionRow *lhs,
              const ObDirectLoadConstExternalMultiPartitionRow *rhs, int &cmp_ret);
  int get_error_code() const { return result_code_; }

public:
  ObDirectLoadDatumArrayCompare datum_array_compare_;
  sql::ObLoadDupActionType dup_action_;
  int result_code_;
  bool ignore_seq_no_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
