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

#ifndef OB_STORE_ROW_COMPARER_H_
#define OB_STORE_ROW_COMPARER_H_
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace storage
{

class ObIComparerCancel
{
public:
  ObIComparerCancel() = default;
  virtual ~ObIComparerCancel() = default;
  virtual bool need_cancel() = 0;
};

class ObStoreRowComparer
{
  public:
    ObStoreRowComparer(
        int &comp_ret,
        const common::ObIArray<int64_t> &sort_column_index)
    : result_code_(comp_ret),
      sort_column_index_(sort_column_index)
    {}
  virtual ~ObStoreRowComparer() {}
  OB_INLINE bool operator()(const ObStoreRow *left, const ObStoreRow *right);
  int &result_code_;
private:
  const common::ObIArray<int64_t> &sort_column_index_;
};

/**
 * --------------------------------------------------------Inline Function------------------------------------------------------
 */
OB_INLINE bool ObStoreRowComparer::operator()(const ObStoreRow *left, const ObStoreRow *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(common::OB_SUCCESS != result_code_)) {
    //do nothing
  } else if (OB_UNLIKELY(NULL == left)
      || OB_UNLIKELY(NULL == right)
      || OB_UNLIKELY(0 == sort_column_index_.count())) {
    result_code_ = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "Invalid argument, ", KP(left), KP(right),
      K(sort_column_index_.count()), K_(result_code));
  } else {
    for (int64_t i = 0; OB_LIKELY(common::OB_SUCCESS == result_code_) && i < sort_column_index_.count(); ++i) {
      const int64_t index = sort_column_index_.at(i);
      if (index < left->row_val_.count_ && index < right->row_val_.count_) {
        const int cmp = left->row_val_.cells_[index].compare(right->row_val_.cells_[index]);
        if (cmp < 0) {
          bool_ret = true;
          break;
        } else if (cmp > 0) {
          bool_ret = false;
          break;
        }
      } else {
        result_code_ = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG_RET(WARN, result_code_, "index is out of bound",
            K_(result_code),
            K(index),
            K(left->row_val_.count_),
            K(right->row_val_.count_),
            K(sort_column_index_.count()),
            K(i));
      }
    }
  }
  return bool_ret;
}

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_STORE_ROW_COMPARER_H_ */
