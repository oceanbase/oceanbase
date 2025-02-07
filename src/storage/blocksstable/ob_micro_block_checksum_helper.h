/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_CHECKSUM_HELPER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_CHECKSUM_HELPER_H_
#include "storage/blocksstable/ob_datum_row.h"
#include "share/schema/ob_table_param.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
class ObMicroBlockChecksumHelper final
{
public:
  ObMicroBlockChecksumHelper()
    : col_descs_(nullptr),
      integer_col_idx_(nullptr),
      integer_col_buf_(nullptr),
      integer_col_cnt_(0),
      allocator_("CkmHelper"),
      micro_block_row_checksum_(0) {}
  ~ObMicroBlockChecksumHelper() { reset(); }

  int init(
    const common::ObIArray<share::schema::ObColDesc> *col_descs,
    const bool need_opt_row_chksum);
  void reset();
  inline void reuse() { micro_block_row_checksum_ = 0; }
  inline int64_t get_row_checksum() const { return micro_block_row_checksum_; }
  inline bool is_local_buf() const { return integer_col_buf_ == local_integer_col_buf_; }
  inline bool is_local_idx() const { return integer_col_idx_ == local_integer_col_idx_; }
  template<typename T>
  int cal_row_checksum(
      const T* datums,
      const int64_t row_col_cnt);
  int cal_column_checksum(const ObDatumRow &row, int64_t *curr_micro_column_checksum)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!row.is_valid() || nullptr == curr_micro_column_checksum)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), K(row), K(curr_micro_column_checksum));
    } else {
      for (int64_t i = 0; i < row.get_column_count(); ++i) {
        curr_micro_column_checksum[i] += row.storage_datums_[i].checksum(0);
      }
    }
    return ret;
  }
  int cal_rows_checksum(const common::ObArray<ObColDatums *> &all_col_datums,
                        const int64_t row_count);

  int cal_column_checksum(const common::ObArray<ObColDatums *> &all_col_datums,
                          const int64_t row_count,
                          int64_t *curr_micro_column_checksum);

  TO_STRING_KV(KPC_(col_descs), KP_(integer_col_idx), KP_(integer_col_buf),
      K_(integer_col_cnt), K_(micro_block_row_checksum));

private:
  static const int64_t MAGIC_NOP_NUMBER = 0xa1b;
  static const int64_t MAGIC_NULL_NUMBER = 0xce75;
  static const int64_t NEED_INTEGER_BUF_CNT = 2;
  static const int64_t LOCAL_INTEGER_COL_CNT = 64;
private:
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  int16_t *integer_col_idx_;
  int64_t *integer_col_buf_;
  int64_t integer_col_cnt_;
  compaction::ObLocalArena allocator_;
  int64_t micro_block_row_checksum_;
  int16_t local_integer_col_idx_[LOCAL_INTEGER_COL_CNT];
  int64_t local_integer_col_buf_[LOCAL_INTEGER_COL_CNT];
};

template<typename T>
int ObMicroBlockChecksumHelper::cal_row_checksum(
    const T* datums,
    const int64_t row_col_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datums)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(datums));
  } else if (OB_ISNULL(integer_col_buf_) || OB_ISNULL(integer_col_idx_)) {
    for (int64_t i = 0; i < row_col_cnt; ++i) {
      micro_block_row_checksum_ = datums[i].checksum(micro_block_row_checksum_);
    }
  } else if (OB_ISNULL(col_descs_) || row_col_cnt != col_descs_->count()) { // defense
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect error", K(ret), KPC(col_descs_), K_(integer_col_cnt), K(row_col_cnt));
  } else {
    for (int64_t i = 0, idx = 0; i < row_col_cnt; ++i) {
      if (idx < integer_col_cnt_ && integer_col_idx_[idx] == i) {
        if (datums[i].is_nop()) {
          integer_col_buf_[idx] = MAGIC_NOP_NUMBER;
        } else if (datums[i].is_null()) {
          integer_col_buf_[idx] = MAGIC_NULL_NUMBER;
        } else {
          integer_col_buf_[idx] = datums[i].get_int();
        }
        ++idx;
      } else {
        micro_block_row_checksum_ = datums[i].checksum(micro_block_row_checksum_);
      }
    }
    micro_block_row_checksum_ = ob_crc64_sse42(micro_block_row_checksum_,
        static_cast<void*>(integer_col_buf_), sizeof(int64_t) * integer_col_cnt_);
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase

#endif