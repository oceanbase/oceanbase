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

#define USING_LOG_PREFIX STORAGE
#include "ob_imicro_block_decoder.h"
#include "storage/access/ob_block_row_store.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
using namespace storage;
namespace blocksstable
{
class EncodingCompareV2
{
public:
  EncodingCompareV2(int &ret, bool &equal, ObIMicroBlockDecoder *decoder)
    : ret_(ret), equal_(equal), decoder_(decoder)
  {
  }
  ~EncodingCompareV2() {}
  inline bool operator()(const int64_t row_idx, const ObDatumRowkey &rowkey)
  {
    return compare(row_idx, rowkey, true);
  }
  inline bool operator()(const ObDatumRowkey &rowkey, const int64_t row_idx)
  {
    return compare(row_idx, rowkey, false);
  }

private:
  inline bool compare(const int64_t row_idx, const ObDatumRowkey &rowkey, const bool lower_bound)
  {
    bool bret = false;
    int &ret = ret_;
    int32_t compare_result = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(decoder_->compare_rowkey(rowkey, row_idx, compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else {
      bret = lower_bound ? compare_result < 0 : compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal result, must prevent it from being modified again
      if (0 == compare_result && !equal_) {
        equal_ = true;
      }
    }
    return bret;
  }

private:
  int &ret_;
  bool &equal_;
  ObIMicroBlockDecoder *decoder_;
};

class EncodingRangeCompareV2
{
public:
  EncodingRangeCompareV2(int &ret, bool &equal, ObIMicroBlockDecoder *decoder,
    int64_t &end_key_begin_idx, int64_t &end_key_end_idx)
    : compare_with_range_(true), ret_(ret), equal_(equal), decoder_(decoder),
      end_key_begin_idx_(end_key_begin_idx), end_key_end_idx_(end_key_end_idx)
  {
  }
  ~EncodingRangeCompareV2() {}
  inline bool operator()(const int64_t row_idx, const ObDatumRange &range)
  {
    return compare(row_idx, range, true);
  }
  inline bool operator()(const ObDatumRange &range, const int64_t row_idx)
  {
    return compare(row_idx, range, false);
  }

private:
  inline bool compare(const int64_t row_idx, const ObDatumRange &range, const bool lower_bound)
  {
    bool bret = false;
    int &ret = ret_;
    int32_t start_key_compare_result = 0;
    int32_t end_key_compare_result = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (compare_with_range_ &&
      OB_FAIL(decoder_->compare_rowkey(
        range, row_idx, start_key_compare_result, end_key_compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else if (!compare_with_range_ &&
      OB_FAIL(decoder_->compare_rowkey(range.get_start_key(), row_idx, start_key_compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else {
      bret = lower_bound ? start_key_compare_result < 0 : start_key_compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal result, must prevent it from being modified again
      if (0 == start_key_compare_result && !equal_) {
        equal_ = true;
      }

      if (compare_with_range_) {
        if (start_key_compare_result > 0) {
          if (end_key_compare_result < 0) {
            end_key_begin_idx_ = row_idx;
          }
          if (end_key_compare_result > 0 && row_idx < end_key_end_idx_) {
            end_key_end_idx_ = row_idx;
          }
        }

        if (start_key_compare_result >= 0 && end_key_compare_result < 0) {
          compare_with_range_ = false;
        }
      }
    }
    return bret;
  }

private:
  bool compare_with_range_;
  int &ret_;
  bool &equal_;
  ObIMicroBlockDecoder *decoder_;
  int64_t &end_key_begin_idx_;
  int64_t &end_key_end_idx_;
};

int ObIMicroBlockDecoder::find_bound(const ObDatumRowkey &key, const bool lower_bound,
  const int64_t begin_idx, int64_t &row_idx, bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!key.is_valid() || begin_idx < 0 || begin_idx >= row_count_ || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin_idx), K_(row_count), KP_(datum_utils));
  } else if (key.get_datum_cnt() <= 0 || key.get_datum_cnt() > datum_utils_->get_rowkey_count()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compare column count", K(ret), K(key.get_datum_cnt()),
      K(datum_utils_->get_rowkey_count()));
  } else {
    EncodingCompareV2 encoding_compare(ret, equal, this);
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(row_count_);
    ObRowIndexIterator found_iter;
    if (lower_bound) {
      found_iter = std::lower_bound(begin_iter, end_iter, key, encoding_compare);
    } else {
      found_iter = std::upper_bound(begin_iter, end_iter, key, encoding_compare);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

int ObIMicroBlockDecoder::find_bound(const ObDatumRange &range, const int64_t begin_idx,
  int64_t &row_idx, bool &equal, int64_t &end_key_begin_idx, int64_t &end_key_end_idx)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!range.is_valid() || begin_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin_idx), K_(row_count));
  } else {
    EncodingRangeCompareV2 encoding_compare(ret, equal, this, end_key_begin_idx, end_key_end_idx);
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(row_count_);
    ObRowIndexIterator found_iter;
    found_iter = std::lower_bound(begin_iter, end_iter, range, encoding_compare);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

// for column store
int ObIMicroBlockDecoder::find_bound(const ObDatumRowkey &key, const bool lower_bound,
  const int64_t begin_idx, const int64_t end_idx, int64_t &row_idx, bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!key.is_valid() || begin_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin_idx), K_(row_count));
  } else if (key.get_datum_cnt() <= 0 || key.get_datum_cnt() > datum_utils_->get_rowkey_count()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compare column count", K(ret), K(key.get_datum_cnt()),
      K(datum_utils_->get_rowkey_count()));
  } else {
    EncodingCompareV2 encoding_compare(ret, equal, this);
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(end_idx);
    ObRowIndexIterator found_iter;
    if (lower_bound) {
      found_iter = std::lower_bound(begin_iter, end_iter, key, encoding_compare);
    } else {
      found_iter = std::upper_bound(begin_iter, end_iter, key, encoding_compare);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
