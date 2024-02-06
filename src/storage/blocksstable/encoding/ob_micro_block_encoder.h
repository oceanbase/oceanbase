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

#ifndef OCEANBASE_ENCODING_OB_MICRO_BLOCK_ENCODER_H_
#define OCEANBASE_ENCODING_OB_MICRO_BLOCK_ENCODER_H_

#include "lib/container/ob_array.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"
#include "storage/ob_i_store.h"
#include "ob_icolumn_encoder.h"
#include "ob_encoding_allocator.h"
#include "ob_encoding_util.h"
#include "ob_multi_prefix_tree.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIColumnEncoder;
class ObEncodingHashTable;

class ObMicroBlockEncoder : public ObIMicroBlockWriter
{
public:
  static const int64_t MAX_ENCODING_META_LENGTH = UINT16_MAX;
  static const int64_t DEFAULT_ESTIMATE_REAL_SIZE_PCT = 150;

  // maximum row count is restricted to 4 bytes in MicroBlockHeader
  // But all_col_datums_ is restricted to 64K, so we limit maximum row count to uint16_max
  static const int64_t MAX_MICRO_BLOCK_ROW_CNT = UINT16_MAX;
  // Unlike ObMicroBlockWriter, ObMicroBlockEncoder internally uses ObRowWriter and ObIColumnEncoder
  // to form row and column data. Both ObRowWriter and ObIColumnEncoder check buffer capacity by
  // calling ObBufferWriter::advance_zero. If buffer size is not enough, they return failure.
  // Compared to ObMicroBlockWriter, who calls ObBufferWriter::write to acquire buffer space
  // automatically, we have to prepared enough buffer space for ObMicroBlockEncoder at beginning.
  // So DEFAULT_ROWKEY_BUFFER_SIZE should be equal to macro block size
  //
  // For detail difference about the writing buffer between ObMicroBlockWriter and
  // ObMicroBlockEncoder, please take a look at method:
  // int ObMacroBlockWriter::get_current_micro_block_buffer(const char *&buf, int64_t &size)
  static const int64_t DEFAULT_DATA_BUFFER_SIZE = common::OB_DEFAULT_MACRO_BLOCK_SIZE;

  struct CellCopyIndex
  {
    uint32_t index_;
    uint32_t offset_;
    TO_STRING_KV(K_(index), K_(offset));
  };

  ObMicroBlockEncoder();
  virtual ~ObMicroBlockEncoder();

  // Valid to double init. will call reuse() every time befor initialization.
  int init(const ObMicroBlockEncodingCtx &ctx);
  // return OB_BUF_NOT_ENOUGH if exceed micro block size
  virtual int append_row(const ObDatumRow &row) override;
  virtual int build_block(char *&buf, int64_t &size) override;
  // clear status and release memory, reset along with macro block writer
  virtual void reset() override;
  // reuse() will clean status of members partially.
  // Can be called alone or along with init()
  virtual void reuse() override;
  virtual int64_t get_row_count() const override { return datum_rows_.count(); }
  virtual int64_t get_block_size() const override { return header_size_ + estimate_size_ * 100 / expand_pct_; }
  virtual int64_t get_column_count() const override { return ctx_.column_cnt_;}
  virtual int64_t get_original_size() const override { return estimate_size_; }
  virtual void dump_diagnose_info() const override;
private:
  int inner_init();
  int reserve_header(const ObMicroBlockEncodingCtx &ctx);
  int pivot();
  int try_to_append_row(const int64_t &store_size);
  int init_column_ctxs();
  // only deep copy the cell part
  int process_out_row_columns(const ObDatumRow &row);
  int copy_and_append_row(const ObDatumRow &src, int64_t &store_size);
  int copy_cell(
      const ObColDesc &col_desc,
      const ObStorageDatum &src,
      ObDatum &dest,
      int64_t &store_size,
      bool &is_large_row,
      bool &is_buffer_not_enough);
  int process_large_row(const ObDatumRow &src, ObDatum *&datum_arr, int64_t &store_size);
  int64_t calc_datum_row_size(const ObDatumRow &src, int64_t &var_len_column_cnt) const;
  int encoder_detection(int64_t &encoders_need_size);
  // detect encoder with pre-scan result
  int fast_encoder_detect(const int64_t column_idx, const ObColumnEncodingCtx &cc);
  int prescan(const int64_t column_index);
  int choose_encoder(const int64_t column_idx, ObColumnEncodingCtx &column_ctx);
  void free_encoders();

  template <typename T>
  T *alloc_encoder();
  void free_encoder(ObIColumnEncoder *encoder);

  // alloc and init encoder
  // %e may be NULL if encoder not suitable.
  template <typename T>
  int try_encoder(ObIColumnEncoder *&e, const int64_t column_index);

  int try_previous_encoder(ObIColumnEncoder *&e,
      const int64_t column_index, const ObPreviousEncoding &previous);

  int try_encoder(ObIColumnEncoder *&e, const int64_t column_index,
      const ObColumnHeader::Type type, const int64_t last_prefix_length,
      const int64_t ref_col_idx);

  int try_previous_encoder(ObIColumnEncoder *&e,
      const int64_t column_index,
      const int64_t acceptable_size, bool &try_more);

  template <typename T>
  int try_span_column_encoder(ObIColumnEncoder *&e, const int64_t column_idx);
  template <typename T>
  int try_span_column_encoder(ObIColumnEncoder *&e, const int64_t column_index,
      const int64_t ref_column_index);
  int try_span_column_encoder(ObSpanColumnEncoder *e, const int64_t column_index,
      const int64_t ref_column_index, bool &suitable);

  int force_raw_encoding(const int64_t column_idx);
  int force_raw_encoding(const int64_t column_idx, const bool var_store, ObIColumnEncoder *&e);

  // set column row data position and get fix data size of row.
  // fill %fix_data_encoders_ and %var_data_encoders_
  int set_row_data_pos(int64_t &fix_data_size);
  int fill_row_data(const int64_t fix_data_size);

  OB_INLINE int store_data(ObIColumnEncoder &e,
      const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len);

  void update_estimate_size_limit(const ObMicroBlockEncodingCtx &ctx);

  int store_encoding_meta_and_fix_cols(ObBufferWriter &buf_writer, int64_t &encoding_meta_offset);
  int init_all_col_values(const ObMicroBlockEncodingCtx &ctx);
  void print_micro_block_encoder_status() const;
  int set_datum_rows_ptr();

private:
  ObMicroBlockEncodingCtx ctx_;
  ObMicroBlockHeader *header_;
  ObArenaAllocator encoding_meta_allocator_;
  ObMicroBufferWriter data_buffer_;
  ObConstDatumRowArray datum_rows_;
  common::ObArray<ObColDatums *> all_col_datums_;
  ObArenaAllocator pivot_allocator_;
  int64_t estimate_size_;
  int64_t estimate_size_limit_;
  int64_t header_size_;
  int64_t expand_pct_;
  ObMicroBufferWriter row_buf_holder_;
  common::ObArray<ObIColumnEncoder *> encoders_;
  common::ObArray<ObIColumnEncoder *> fix_data_encoders_;
  common::ObArray<ObIColumnEncoder *> var_data_encoders_;
  ObEncoderAllocator encoder_allocator_;
  common::ObArray<int64_t> row_indexs_;
  common::ObArray<ObEncodingHashTable *> hashtables_;
  common::ObArray<ObMultiPrefixTree *> multi_prefix_trees_;
  ObEncodingHashTableFactory hashtable_factory_;
  ObMultiPrefixTreeFactory multi_prefix_tree_factory_;
  common::ObArray<CellCopyIndex> deep_copy_indexes_;
  int64_t string_col_cnt_;
  int64_t estimate_base_store_size_;
  common::ObArray<ObColumnEncodingCtx> col_ctxs_;
  int64_t length_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_MICRO_BLOCK_ENCODER_H_
