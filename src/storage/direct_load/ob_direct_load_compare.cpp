// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_compare.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_datum.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_external_row.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadDatumRowkeyCompare
 */

int ObDirectLoadDatumRowkeyCompare::init(const ObStorageDatumUtils &datum_utils)
{
  int ret = OB_SUCCESS;
  datum_utils_ = &datum_utils;
  return ret;
}

bool ObDirectLoadDatumRowkeyCompare::operator()(const ObDatumRowkey *lhs,
                                                const ObDatumRowkey *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_ISNULL(datum_utils_) || OB_ISNULL(lhs) || OB_ISNULL(rhs) ||
      OB_UNLIKELY(!lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datum_utils_), KP(lhs), KP(rhs));
  } else {
    if (OB_FAIL(lhs->compare(*rhs, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare rowkey", KR(ret), KP(lhs), K(rhs), K(datum_utils_));
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * ObDirectLoadDatumRowCompare
 */

int ObDirectLoadDatumRowCompare::init(const ObStorageDatumUtils &datum_utils, int64_t rowkey_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDatumRowCompare init twice", KR(ret), KP(this));
  }
  if (OB_UNLIKELY(rowkey_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_size));
  } else {
    if (OB_FAIL(rowkey_compare_.init(datum_utils))) {
      LOG_WARN("fail to init rowkey compare", KR(ret));
    } else {
      rowkey_size_ = rowkey_size;
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObDirectLoadDatumRowCompare::operator()(const ObDatumRow *lhs, const ObDatumRow *rhs)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumRowCompare not init", KR(ret), KP(this));
  } else if (OB_ISNULL(lhs) || OB_ISNULL(rhs) ||
             OB_UNLIKELY(!lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(lhs), KP(rhs));
  } else if (OB_UNLIKELY(lhs->get_column_count() < rowkey_size_ ||
                         rhs->get_column_count() < rowkey_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row column cnt", KR(ret), K(lhs), K(rhs), K_(rowkey_size));
  } else {
    if (OB_FAIL(lhs_rowkey_.assign(lhs->storage_datums_, rowkey_size_))) {
      LOG_WARN("Failed to assign datum rowkey", KR(ret), K(lhs), K_(rowkey_size));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs->storage_datums_, rowkey_size_))) {
      LOG_WARN("Failed to assign datum rowkey", KR(ret), K(rhs), K_(rowkey_size));
    } else {
      bret = rowkey_compare_.operator()(&lhs_rowkey_, &rhs_rowkey_);
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  } else if (OB_FAIL(rowkey_compare_.get_error_code())) {
    result_code_ = rowkey_compare_.get_error_code();
  }
  return bret;
}

/**
 * ObDirectLoadDatumArrayCompare
 */

int ObDirectLoadDatumArrayCompare::init(const ObStorageDatumUtils &datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDatumArrayCompare init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(rowkey_compare_.init(datum_utils))) {
      LOG_WARN("fail to init rowkey compare", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObDirectLoadDatumArrayCompare::operator()(const ObDirectLoadDatumArray *lhs,
                                               const ObDirectLoadDatumArray *rhs)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumArrayCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() || !rhs->is_valid() ||
                         lhs->count_ != rhs->count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(lhs), KP(rhs));
  } else if (lhs->count_ > 0) {
    if (OB_FAIL(lhs_rowkey_.assign(lhs->datums_, lhs->count_))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs->datums_, rhs->count_))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else {
      bret = rowkey_compare_.operator()(&lhs_rowkey_, &rhs_rowkey_);
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  } else if (OB_FAIL(rowkey_compare_.get_error_code())) {
    result_code_ = rowkey_compare_.get_error_code();
  }
  return bret;
}

bool ObDirectLoadDatumArrayCompare::operator()(const ObDirectLoadConstDatumArray *lhs,
                                               const ObDirectLoadConstDatumArray *rhs)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumArrayCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() || !rhs->is_valid() ||
                         lhs->count_ != rhs->count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(lhs), KP(rhs));
  } else if (lhs->count_ > 0) {
    if (OB_FAIL(lhs_rowkey_.assign(lhs->datums_, lhs->count_))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs->datums_, rhs->count_))) {
      LOG_WARN("fail to assign rowkey", KR(ret));
    } else {
      bret = rowkey_compare_.operator()(&lhs_rowkey_, &rhs_rowkey_);
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  } else if (OB_FAIL(rowkey_compare_.get_error_code())) {
    result_code_ = rowkey_compare_.get_error_code();
  }
  return bret;
}

/**
 * ObDirectLoadExternalRowCompare
 */

int ObDirectLoadExternalRowCompare::init(const ObStorageDatumUtils &datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDatumRowCompare init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(datum_array_compare_.init(datum_utils))) {
      LOG_WARN("fail to init datum array compare", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObDirectLoadExternalRowCompare::operator()(const ObDirectLoadExternalRow *lhs,
                                                const ObDirectLoadExternalRow *rhs)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumRowCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() ||
                         !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(lhs), KP(rhs));
  } else {
    bret = datum_array_compare_.operator()(&lhs->rowkey_datum_array_, &rhs->rowkey_datum_array_);
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  } else if (OB_FAIL(datum_array_compare_.get_error_code())) {
    result_code_ = datum_array_compare_.get_error_code();
  }
  return bret;
}

/**
 * ObDirectLoadExternalMultiPartitionRowCompare
 */

int ObDirectLoadExternalMultiPartitionRowCompare::init(const ObStorageDatumUtils &datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadExternalMultiPartitionRowCompare init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(datum_array_compare_.init(datum_utils))) {
      LOG_WARN("fail to init datum array compare", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObDirectLoadExternalMultiPartitionRowCompare::operator()(
  const ObDirectLoadExternalMultiPartitionRow *lhs,
  const ObDirectLoadExternalMultiPartitionRow *rhs)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalMultiPartitionRowCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() ||
                         !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(lhs), KP(rhs));
  } else {
    if (lhs->tablet_id_ != rhs->tablet_id_) {
      bret = lhs->tablet_id_ < rhs->tablet_id_;
    } else {
      bret = datum_array_compare_.operator()(&lhs->external_row_.rowkey_datum_array_,
                                             &rhs->external_row_.rowkey_datum_array_);
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  } else if (OB_FAIL(datum_array_compare_.get_error_code())) {
    result_code_ = datum_array_compare_.get_error_code();
  }
  return bret;
}

bool ObDirectLoadExternalMultiPartitionRowCompare::operator()(
  const ObDirectLoadConstExternalMultiPartitionRow *lhs,
  const ObDirectLoadConstExternalMultiPartitionRow *rhs)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadExternalMultiPartitionRowCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() ||
                         !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(lhs), KP(rhs));
  } else {
    if (lhs->tablet_id_ != rhs->tablet_id_) {
      bret = lhs->tablet_id_ < rhs->tablet_id_;
    } else {
      bret = datum_array_compare_.operator()(&lhs->rowkey_datum_array_, &rhs->rowkey_datum_array_);
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  } else if (OB_FAIL(datum_array_compare_.get_error_code())) {
    result_code_ = datum_array_compare_.get_error_code();
  }
  return bret;
}

}  // namespace storage
}  // namespace oceanbase
