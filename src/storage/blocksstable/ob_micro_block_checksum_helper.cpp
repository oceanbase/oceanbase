/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/blocksstable/ob_micro_block_checksum_helper.h"
#include "common/ob_target_specific.h"
namespace oceanbase
{
namespace blocksstable
{
int ObMicroBlockChecksumHelper::init(
    const common::ObIArray<share::schema::ObColDesc> *col_descs,
    const bool need_opt_row_chksum)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(col_descs)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(col_descs));
  } else {
    if (need_opt_row_chksum) {
      for (int64_t i = 0; i < col_descs->count(); ++i) {
        if (col_descs->at(i).col_type_.is_integer_type()) {
          ++integer_col_cnt_;
        }
      }
    }
    if (NEED_INTEGER_BUF_CNT < integer_col_cnt_) {
      if (LOCAL_INTEGER_COL_CNT >= integer_col_cnt_) {
        integer_col_buf_ = local_integer_col_buf_;
        integer_col_idx_ = local_integer_col_idx_;
      } else if (OB_ISNULL(integer_col_buf_ =
          static_cast<int64_t*>(allocator_.alloc(sizeof(int64_t) * integer_col_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc integer_col_buf", K(ret), K_(integer_col_cnt));
      } else if (OB_ISNULL(integer_col_idx_ =
          static_cast<int16_t*>(allocator_.alloc(sizeof(int16_t) * integer_col_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc integer_col_idx", K(ret), K_(integer_col_cnt));
      }
    }
      }
  if (OB_SUCC(ret)) {
    // traverse once again to fill idx
    if (OB_NOT_NULL(integer_col_idx_)) {
      for (int64_t i = 0, idx = 0; i < col_descs->count(); ++i) {
        if (idx < integer_col_cnt_ && col_descs->at(i).col_type_.is_integer_type()) {
          integer_col_idx_[idx] = i;
          ++idx;
        }
      }
    }
    micro_block_row_checksum_ = 0;
    col_descs_ = col_descs;
  } else {
    reset();
  }
  return ret;
}

void ObMicroBlockChecksumHelper::reset()
{
  col_descs_ = nullptr;
  integer_col_cnt_ = 0;
  micro_block_row_checksum_ = 0;
    if (OB_NOT_NULL(integer_col_buf_)) {
    if (!is_local_buf()) {
      allocator_.free(integer_col_buf_);
    }
    integer_col_buf_ = nullptr;
  }
  if (OB_NOT_NULL(integer_col_idx_)) {
    if (!is_local_idx()) {
      allocator_.free(integer_col_idx_);
    }
    integer_col_idx_ = nullptr;
  }
  allocator_.reset();
}

int ObMicroBlockChecksumHelper::cal_rows_checksum(
    const common::ObArray<ObColDatums *> &all_col_datums,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = all_col_datums.count();
  if (OB_ISNULL(integer_col_buf_) || OB_ISNULL(integer_col_idx_)) {
    for (int64_t row_idx = 0; row_idx < row_count; row_idx++) {
      for (int64_t i = 0; i < col_cnt; ++i) {
        micro_block_row_checksum_ = all_col_datums[i]->at(row_idx).checksum(micro_block_row_checksum_);
      }
    }
  } else if (OB_ISNULL(col_descs_) || col_cnt != col_descs_->count()) { // defense
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect error", K(ret), KPC(col_descs_), K_(integer_col_cnt), K(col_cnt));
  } else {
    for (int64_t row_idx = 0; row_idx < row_count; row_idx++) {
      for (int64_t i = 0, idx = 0; i < col_cnt; ++i) {
        if (idx < integer_col_cnt_ && integer_col_idx_[idx] == i) {
          if (all_col_datums[i]->at(row_idx).is_nop()) {
            integer_col_buf_[idx] = MAGIC_NOP_NUMBER;
          } else if (all_col_datums[i]->at(row_idx).is_null()) {
            integer_col_buf_[idx] = MAGIC_NULL_NUMBER;
          } else {
            integer_col_buf_[idx] = all_col_datums[i]->at(row_idx).get_int();
          }
          ++idx;
        } else {
          micro_block_row_checksum_ = all_col_datums[i]->at(row_idx).checksum(micro_block_row_checksum_);
        }
      }
      micro_block_row_checksum_ = ob_crc64_sse42(micro_block_row_checksum_,
          static_cast<void*>(integer_col_buf_), sizeof(int64_t) * integer_col_cnt_);
    }
  }
  return ret;
}

int ObMicroBlockChecksumHelper::cal_column_checksum(
    const ObIArray<ObIVector *> &vectors,
    const int64_t start,
    const int64_t row_count,
    int64_t *curr_micro_column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr == curr_micro_column_checksum) || nullptr == col_descs_ || vectors.count() != col_descs_->count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(curr_micro_column_checksum), KPC(col_descs_), K(vectors.count()));
  } else {
    if (is_arch_supported(common::ObTargetArch::SSE42)) {
      if (OB_FAIL(cal_column_checksum_sse42(vectors, start, row_count, curr_micro_column_checksum))) {
        STORAGE_LOG(WARN, "failed to cal column checksum using sse42 func", K(ret));
      }
    } else {
      if (OB_FAIL(cal_column_checksum_normal(vectors, start, row_count, curr_micro_column_checksum))) {
        STORAGE_LOG(WARN, "failed to cal column checksum using normal func", K(ret));
      }
    }
  }
  return ret;
}

#if defined (__x86_64__)
__attribute__((target("sse4.2")))
int ObMicroBlockChecksumHelper::cal_column_checksum_sse42(
    const ObIArray<ObIVector *> &vectors,
    const int64_t start,
    const int64_t row_count,
    int64_t *curr_micro_column_checksum)
{
  int ret = OB_SUCCESS;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < vectors.count(); col_idx++) {
    ObIVector *vec = vectors.at(col_idx);
    const VectorFormat vec_format = vec->get_format();
    switch (vec_format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fix_vec = static_cast<ObFixedLengthBase *>(vec);
        ObLength len = fix_vec->get_length(0);
        ObDatum len_pack_datum(nullptr, len, false);
        const int64_t len_pack_checksum = __builtin_ia32_crc32si(0, len_pack_datum.pack_);
        switch (len) {
          case 1: {
            if (fix_vec->has_null()) {
              ObDatum null_pack_datum(nullptr, 0, true);
              const int64_t null_pack_checksum = __builtin_ia32_crc32si(0, null_pack_datum.pack_);
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                if (fix_vec->is_null(row_idx)) {
                  curr_micro_column_checksum[col_idx] += null_pack_checksum;
                } else {
                  curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32qi(len_pack_checksum, *((char *)(fix_vec->get_data() + row_idx * len)));
                }
              }
            } else {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32qi(len_pack_checksum, *((char *)(fix_vec->get_data() + row_idx * len)));
              }
            }
            break;
          }
          case 2: {
            if (fix_vec->has_null()) {
              ObDatum null_pack_datum(nullptr, 0, true);
              const int64_t null_pack_checksum = __builtin_ia32_crc32si(0, null_pack_datum.pack_);
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                if (fix_vec->is_null(row_idx)) {
                  curr_micro_column_checksum[col_idx] += null_pack_checksum;
                } else {
                  curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32hi(len_pack_checksum, *((short *)(fix_vec->get_data() + row_idx * len)));
                }
              }
            } else {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32hi(len_pack_checksum, *((short *)(fix_vec->get_data() + row_idx * len)));
              }
            }
            break;
          }
          case 4: {
            if (fix_vec->has_null()) {
              ObDatum null_pack_datum(nullptr, 0, true);
              const int64_t null_pack_checksum = __builtin_ia32_crc32si(0, null_pack_datum.pack_);
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                if (fix_vec->is_null(row_idx)) {
                  curr_micro_column_checksum[col_idx] += null_pack_checksum;
                } else {
                  curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32si(len_pack_checksum, *((int *)(fix_vec->get_data() + row_idx * len)));
                }
              }
            } else {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32si(len_pack_checksum, *((int *)(fix_vec->get_data() + row_idx * len)));
              }
            }
            break;
          }
          case 8: {
            if (fix_vec->has_null()) {
              ObDatum null_pack_datum(nullptr, 0, true);
              const int64_t null_pack_checksum = __builtin_ia32_crc32si(0, null_pack_datum.pack_);
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                if (fix_vec->is_null(row_idx)) {
                  curr_micro_column_checksum[col_idx] += null_pack_checksum;
                } else {
                  curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32di(len_pack_checksum, *((long long *)(fix_vec->get_data() + row_idx * len)));
                }
              }
            } else {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32di(len_pack_checksum, *((long long *)(fix_vec->get_data() + row_idx * len)));
              }
            }
            break;
          }
          case 16: {
            if (fix_vec->has_null()) {
              ObDatum null_pack_datum(nullptr, 0, true);
              const int64_t null_pack_checksum = __builtin_ia32_crc32si(0, null_pack_datum.pack_);
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                if (fix_vec->is_null(row_idx)) {
                  curr_micro_column_checksum[col_idx] += null_pack_checksum;
                } else {
                  const int64_t tmp_checksum = __builtin_ia32_crc32di(len_pack_checksum, *((long long *)(fix_vec->get_data() + row_idx * len)));
                  curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32di(tmp_checksum, *((long long *)(fix_vec->get_data() + row_idx * len + 8)));
                }
              }
            } else {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                const int64_t tmp_checksum = __builtin_ia32_crc32di(len_pack_checksum, *((long long *)(fix_vec->get_data() + row_idx * len)));
                curr_micro_column_checksum[col_idx] += __builtin_ia32_crc32di(tmp_checksum, *((long long *)(fix_vec->get_data() + row_idx * len + 8)));
              }
            }
            break;
          }
          default: {
            if (fix_vec->has_null()) {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                if (fix_vec->is_null(row_idx)) {
                  curr_micro_column_checksum[col_idx] += ObDatum(fix_vec->get_data() + row_idx * len, 0/*len*/, true/*is_null*/).checksum(0);
                } else {
                  curr_micro_column_checksum[col_idx] += ObDatum(fix_vec->get_data() + row_idx * len, len, false/*is_null*/).checksum(0);
                }
              }
            } else {
              for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
                curr_micro_column_checksum[col_idx] += ObDatum(fix_vec->get_data() + row_idx * len, len, false/*is_null*/).checksum(0);
              }
            }
          }
        }
        break;
      }
      case VEC_CONTINUOUS: {
        bool is_null = false;
        const char *payload = nullptr;
        ObLength length;
        ObContinuousFormat *continuous_vec = static_cast<ObContinuousFormat *>(vec);
        for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
          length = 0;
          continuous_vec->get_payload(row_idx, is_null, payload, length);
          curr_micro_column_checksum[col_idx] += ObDatum(payload, length, is_null).checksum(0);
        }
        break;
      }
      case VEC_DISCRETE: {
        bool is_null = false;
        const char *payload = nullptr;
        ObLength length;
        ObDiscreteFormat *discrete_vec = static_cast<ObDiscreteFormat *>(vec);
        for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
          length = 0;
          discrete_vec->get_payload(row_idx, is_null, payload, length);
          curr_micro_column_checksum[col_idx] += ObDatum(payload, length, is_null).checksum(0);
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformFormat<false> *uniform_vec = static_cast<ObUniformFormat<false> *>(vec);
        for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
          curr_micro_column_checksum[col_idx] += uniform_vec->get_datum(row_idx).checksum(0);
        }
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformFormat<true> *uniform_vec = static_cast<ObUniformFormat<true> *>(vec);
        for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
          curr_micro_column_checksum[col_idx] += uniform_vec->get_datum(row_idx).checksum(0);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected vector format", K(ret), K(vec_format));
      }
    }
  }
  return ret;
}
#else
int ObMicroBlockChecksumHelper::cal_column_checksum_sse42(
    const ObIArray<ObIVector *> &vectors,
    const int64_t start,
    const int64_t row_count,
    int64_t *curr_micro_column_checksum)
{
  return OB_ERR_UNEXPECTED;
}
#endif

int ObMicroBlockChecksumHelper::cal_column_checksum_normal(
    const ObIArray<ObIVector *> &vectors,
    const int64_t start,
    const int64_t row_count,
    int64_t *curr_micro_column_checksum)
{
  int ret = OB_SUCCESS;
  for (int64_t col_idx = 0; col_idx < vectors.count(); col_idx++) {
    ObIVector *vec = vectors.at(col_idx);
    for (int64_t row_idx = start; row_idx < start + row_count; row_idx++) {
      bool is_null = false;
      const char *payload = nullptr;
      ObLength length = 0;
      ObIVector *vec = vectors.at(col_idx);
      vec->get_payload(row_idx, is_null, payload, length);
      curr_micro_column_checksum[col_idx] += ObDatum(payload, length, is_null).checksum(0);
    }
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase
