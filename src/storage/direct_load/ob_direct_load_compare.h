// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "storage/blocksstable/ob_datum_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageDatumUtils;
class ObDatumRowkey;
class ObDatumRow;
}  // namespace blocksstable
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
    : rowkey_size_(0), result_code_(common::OB_SUCCESS), is_inited_(false) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils, int64_t rowkey_size);
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
  ObDirectLoadDatumArrayCompare()
    : result_code_(common::OB_SUCCESS), is_inited_(false) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils);
  bool operator()(const ObDirectLoadDatumArray *lhs, const ObDirectLoadDatumArray *rhs);
  bool operator()(const ObDirectLoadConstDatumArray *lhs, const ObDirectLoadConstDatumArray *rhs);
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
  ObDirectLoadExternalRowCompare() : result_code_(common::OB_SUCCESS), is_inited_(false) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils);
  bool operator()(const ObDirectLoadExternalRow *lhs, const ObDirectLoadExternalRow *rhs);
  int get_error_code() const { return result_code_; }
public:
  ObDirectLoadDatumArrayCompare datum_array_compare_;
  int result_code_;
  bool is_inited_;
};

class ObDirectLoadExternalMultiPartitionRowCompare
{
public:
  ObDirectLoadExternalMultiPartitionRowCompare()
    : result_code_(common::OB_SUCCESS), is_inited_(false) {}
  int init(const blocksstable::ObStorageDatumUtils &datum_utils);
  bool operator()(const ObDirectLoadExternalMultiPartitionRow *lhs,
                  const ObDirectLoadExternalMultiPartitionRow *rhs);
  bool operator()(const ObDirectLoadConstExternalMultiPartitionRow *lhs,
                  const ObDirectLoadConstExternalMultiPartitionRow *rhs);
  int get_error_code() const { return result_code_; }
public:
  ObDirectLoadDatumArrayCompare datum_array_compare_;
  int result_code_;
  bool is_inited_;
};

}  // namespace storage
}  // namespace oceanbase
