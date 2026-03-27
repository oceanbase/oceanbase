/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "ob_hbase_common_struct.h"
#include "observer/table/utils/ob_htable_utils.h"

namespace oceanbase
{
namespace table
{

bool ObHbaseMergeCompare::operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs)
{
  int cmp_ret = 0;
  result_code_ = compare(lhs, rhs, cmp_ret);
  return cmp_ret < 0;
}

int ObHbaseRowForwardCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid() ||
      lhs.count_ < ObHTableConstants::HTABLE_ROWKEY_SIZE ||
      rhs.count_ < ObHTableConstants::HTABLE_ROWKEY_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = 0;
    for (int i = 0; i < ObHTableConstants::COL_IDX_T && cmp_ret == 0; ++i) {
      cmp_ret = lhs.get_cell(i).get_string().compare(rhs.get_cell(i).get_string());
    }
    if (cmp_ret == 0 && need_compare_ts_) {
      cmp_ret = lhs.get_cell(ObHTableConstants::COL_IDX_T).get_int() -
        rhs.get_cell(ObHTableConstants::COL_IDX_T).get_int();
    }
  }
  return ret;
}

int ObHbaseRowReverseCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = rhs.get_cell(ObHTableConstants::COL_IDX_K).get_string().compare(
        lhs.get_cell(ObHTableConstants::COL_IDX_K).get_string());
    if (cmp_ret == 0) {
      cmp_ret = lhs.get_cell(ObHTableConstants::COL_IDX_Q).get_string().compare(
        rhs.get_cell(ObHTableConstants::COL_IDX_Q).get_string());
      if (cmp_ret == 0 && need_compare_ts_) {
        cmp_ret = lhs.get_cell(ObHTableConstants::COL_IDX_T).get_int() -
          rhs.get_cell(ObHTableConstants::COL_IDX_T).get_int();
      }
    }
  }
  return ret;
}

int ObHbaseRowKeyForwardCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid()) ||
      lhs.count_ < ObHTableConstants::COL_IDX_K + 1 ||
      rhs.count_ < ObHTableConstants::COL_IDX_K + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = lhs.get_cell(ObHTableConstants::COL_IDX_K).get_string().compare(
        rhs.get_cell(ObHTableConstants::COL_IDX_K).get_string());
  }
  return ret;
}

int ObHbaseRowKeyReverseCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid()) ||
      lhs.count_ < ObHTableConstants::COL_IDX_K + 1 ||
      rhs.count_ < ObHTableConstants::COL_IDX_K + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = rhs.get_cell(ObHTableConstants::COL_IDX_K).get_string().compare(
        lhs.get_cell(ObHTableConstants::COL_IDX_K).get_string());
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase