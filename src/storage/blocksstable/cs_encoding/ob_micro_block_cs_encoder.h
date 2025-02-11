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

#ifndef OCEANBASE_ENCODING_OB_MICRO_BLOCK_CS_ENCODER_H_
#define OCEANBASE_ENCODING_OB_MICRO_BLOCK_CS_ENCODER_H_

#include "lib/container/ob_array.h"
#include "ob_cs_encoding_allocator.h"
#include "ob_column_encoding_struct.h"
#include "ob_icolumn_cs_encoder.h"
#include "ob_dict_encoding_hash_table.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIColumnCSEncoder;
class ObEncodingHashTable;

class ObVecBatchInfo
{
public:
  ObVecBatchInfo()
    : row_count_(0),
      fixed_len_(-1),
      offsets_(nullptr) ,
      nulls_(nullptr),
      start_offset_(0),
      is_integer_(false)
  {
  }
  int init(const int32_t row_count,
           const int32_t fixed_len,
           const uint32_t *offsets,
           sql::ObBitVector *nulls_,
           const uint32_t start_offset,
           bool is_integer);

  TO_STRING_KV(K_(row_count), K_(fixed_len), KP_(offsets), KP_(nulls), K_(start_offset), K_(is_integer));

  int32_t row_count_;
  int32_t fixed_len_; // fixed len for fixed format < 0 represent continuous format
  const uint32_t *offsets_; // offset arr for continuous format, ==nullptr represent fixed format
  sql::ObBitVector *nulls_; // null bitmap, ==nullptr represent no null
  uint32_t start_offset_; // start offset in row_buf_holder_ of this vec batch
  bool is_integer_; // the integer is format to 8Bytes in row_buf_holder_ which may be larger then fixed_len_
};
typedef ObPodFix2dArray<ObVecBatchInfo, 1 << 20, common::OB_MALLOC_NORMAL_BLOCK_SIZE> ObVecBatchInfoArr;

class ObMicroBlockCSEncoder : public ObIMicroBlockWriter
{
public:
  static const int64_t DEFAULT_ESTIMATE_REAL_SIZE_PCT = 150;
  static const int64_t RESERVE_SIZE_FOR_ESTIMATE_LIMIT = 200 << 10; // 200K

  ObMicroBlockCSEncoder();
  virtual ~ObMicroBlockCSEncoder();

  // Valid to double init. will call reuse() every time befor initialization.
  int init(const ObMicroBlockEncodingCtx &ctx);
  // return OB_BUF_NOT_ENOUGH if exceed micro block size
  virtual int append_row(const ObDatumRow &row);
  virtual int append_batch(const ObBatchDatumRows &vec_batch,
                           const int64_t start,
                           const int64_t row_count) override;
  virtual int build_block(char *&buf, int64_t &size);
  // clear status and release memory, reset along with macro block writer
  virtual void reset();
  // reuse() will clean status of members partially.
  // Can be called alone or along with init()
  virtual void reuse();
  virtual int64_t get_row_count() const override
  {
    return appended_row_count_;
  }
  virtual int64_t get_block_size() const override
  {
    return all_headers_size_ + estimate_size_ * 100 / expand_pct_;
  }
  virtual int64_t get_column_count() const override
  {
    return ctx_.column_cnt_;
  }
  virtual int64_t get_original_size() const override { return all_headers_size_ + estimate_size_; }
  virtual void dump_diagnose_info() override;
  virtual bool micro_block_row_data_buffered() const override { return true; }
  virtual int get_pre_agg_param(const int64_t col_idx, ObMicroDataPreAggParam &pre_agg_param) const override;

private:
  enum ObDataFormatType : uint8_t
  {
    INT_64BIT_FIXED,
    INT_NOT_64BIT_FIXED,
    INT_CONTINUOUS,
    INT_DISCRETE,
    INT_UNIFORM,
    INT_UNIFORM_CONST,
    STR_FIXED,
    STR_CONTINUOUS,
    STR_DISCRETE,
    STR_UNIFORM,
    STR_UNIFORM_CONST,
    MAX
  };

  int inner_init_();
  int reserve_header_(const ObMicroBlockEncodingCtx &ctx);
  int try_to_append_row_(const int64_t &store_size);
  int init_column_ctxs_();
  // only deep copy the cell part
  int process_out_row_columns_();
  int copy_and_append_row_(const ObDatumRow &src, int64_t &store_size);
  int copy_and_append_batch_(const ObBatchDatumRows &vec_batch,
                             const int64_t start,
                             const int64_t row_count,
                             int64_t &store_size);
  int copy_cell_(const ObColDesc &col_desc, const ObStorageDatum &src,
    ObDatum &dest, int64_t &store_size, bool &is_row_holder_not_enough);
  int copy_vector_(const ObIVector *vector,
                   const int64_t start,
                   const int64_t row_count,
                   const int64_t col_idx,
                   int64_t &store_size,
                   bool &is_row_holder_not_enough);
  int do_copy_vector_(const ObIVector *vector,
                      const int64_t start,
                      const int64_t row_count,
                      const int64_t col_idx,
                      const int64_t vec_data_size,
                      const ObDataFormatType type,
                      const bool is_signed);
  int build_all_col_datums_();
  int prepare_for_build_block_();
  int process_large_row_(const ObDatumRow &src, int64_t &store_size);
  int set_datum_rows_ptr_();
  int remove_invalid_datums_(const int32_t column_cnt);
  int remove_invalid_vec_batch_info_(const int32_t column_cnt);
  int encoder_detection_();
  // detect encoder with pre-scan result
  int fast_encoder_detect_(const int64_t column_idx);
  int64_t calc_datum_row_size_(const ObDatumRow &src) const;
  int calc_batch_data_size_(const ObIArray<ObIVector *> &vectors,
                                const int64_t start,
                                const int64_t row_count,
                                int64_t &size) const;
  int calc_col_batch_data_size_(const ObIVector *vector,
                                const int64_t start,
                                const int64_t row_count,
                                const int64_t col_idx,
                                int64_t &size) const;
  int prescan_(const int64_t column_index);
  int choose_encoder_(const int64_t column_idx);
  int choose_encoder_for_integer_(const int64_t column_idx, ObIColumnCSEncoder *&e);
  int choose_encoder_for_string_(const int64_t column_idx, ObIColumnCSEncoder *&e);
  int choose_specified_encoder_(const int64_t column_idx,
                               const ObObjTypeStoreClass store_class,
                               const ObCSColumnHeader::Type type,
                               ObIColumnCSEncoder *&e);
  int try_use_previous_encoder_(const int64_t column_idx,
                                const ObObjTypeStoreClass store_class,
                                ObIColumnCSEncoder *&e);
  int update_previous_info_after_choose_encoder_(const int32_t col_idx, ObIColumnCSEncoder &e);
  int update_previous_info_after_encoding_(const int32_t col_idx, ObIColumnCSEncoder &e);
  void free_encoders_();

  template <typename T>
  T *alloc_encoder_();
  void free_encoder_(ObIColumnCSEncoder *encoder);

  template <typename T>
  int alloc_and_init_encoder_(const int64_t column_index, ObIColumnCSEncoder *&e);
  void update_estimate_size_limit_(const ObMicroBlockEncodingCtx &ctx);
  int init_all_col_values_(const ObMicroBlockEncodingCtx &ctx);
  int init_vec_batch_info_arrs_(const ObMicroBlockEncodingCtx &ctx);
  void print_micro_block_encoder_status_();
  int store_columns_(int64_t &column_data_offset);
  int store_all_string_data_(uint32_t &data_size, bool &use_compress);
  int store_stream_offsets_(int64_t &stream_offsets_length);
  template <typename T>
  int do_encode_stream_offsets_(ObIntegerStreamEncoderCtx enc_ctx);

  static OB_INLINE bool is_integer_store_(const ObObjTypeStoreClass sc, const bool is_wide_int)
  {
    return ObCSEncodingUtil::is_integer_store_class(sc) || (sc == ObDecimalIntSC && !is_wide_int);
  }
  static OB_INLINE bool is_string_store_(const ObObjTypeStoreClass sc, const bool is_wide_int)
  {
    return ObCSEncodingUtil::is_string_store_class(sc) || is_wide_int;
  }

private:
  compaction::ObLocalArena allocator_;
  ObMicroBlockEncodingCtx ctx_;
  ObMicroBufferWriter row_buf_holder_;
  ObMicroBufferWriter data_buffer_;
  ObMicroBufferWriter all_string_data_buffer_;

  common::ObArray<ObColDatums *> all_col_datums_;
  ObArenaAllocator pivot_allocator_;
  common::ObSEArray<uint32_t, 128> datum_row_offset_arr_;
  common::ObArray<ObVecBatchInfoArr *> vec_batch_info_arrs_;
  int64_t appended_batch_count_;
  int64_t appended_row_count_;
  int64_t estimate_size_;
  int64_t estimate_size_limit_;
  int64_t all_headers_size_;
  int64_t expand_pct_;
  common::ObArray<ObIColumnCSEncoder *> encoders_;
  common::ObArray<uint32_t> stream_offsets_;
  ObCSEncoderAllocator encoder_allocator_;
  common::ObArray<ObDictEncodingHashTable *> hashtables_;
  ObDictEncodingHashTableFactory hashtable_factory_;
  common::ObArray<ObColumnCSEncodingCtx> col_ctxs_;
  int64_t length_;
  bool is_inited_;
  bool need_block_level_compression_;
  bool is_all_column_force_raw_;
  bool encoder_freezed_; // if encoder enable append data or build block
  bool block_generated_;
  uint32_t all_string_data_len_;

  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockCSEncoder);
};

template<typename T>
int ObMicroBlockCSEncoder::do_encode_stream_offsets_(ObIntegerStreamEncoderCtx enc_ctx)
{
  int ret = OB_SUCCESS;
  const int32_t stream_cnt = stream_offsets_.count();
  ObIntegerStreamEncoder encoder;
  if (sizeof(T) != sizeof(uint32_t)) {
    // the width_size of input array type of encoder must be equal with the width_size recorded in stream meta
    common::ObSEArray<T, 256> tmp_offsets;
    for (int64_t i = 0; OB_SUCC(ret) && i < stream_cnt; i++) {
      if (OB_FAIL(tmp_offsets.push_back(stream_offsets_.at(i)))) {
        STORAGE_LOG(WARN, "fail to push back", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(encoder.encode(enc_ctx, &tmp_offsets[0], stream_cnt, data_buffer_))) {
        STORAGE_LOG(WARN, "fail to encode stream offset", K(ret), K(enc_ctx));
      }
    }
  } else {
    if (OB_FAIL(encoder.encode(enc_ctx, &stream_offsets_[0], stream_cnt, data_buffer_))) {
      STORAGE_LOG(WARN, "fail to encoder stream offset", K(ret), K(enc_ctx));
    }
  }

  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_MICRO_BLOCK_CS_ENCODER_H_
