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

#ifndef OB_SSTABLE_PRINTER_H_
#define OB_SSTABLE_PRINTER_H_

#include <stdint.h>
#include "ob_block_sstable_struct.h"
#include "ob_macro_block_common_header.h"
#include "index_block/ob_index_block_row_struct.h"
#include "index_block/ob_agg_row_struct.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_icolumn_cs_decoder.h"

#define NONE_COLOR "\033[m"
#define RED "\033[0;32;31m"
#define LIGHT_RED "\033[1;31m"
#define GREEN "\033[0;32;32m"
#define LIGHT_GREEN "\033[1;32m"
#define BLUE "\033[0;32;34m"
#define LIGHT_BLUE "\033[1;34m"
#define DARY_GRAY "\033[1;30m"
#define CYAN "\033[0;36m"
#define LIGHT_CYAN "\033[1;36m"
#define PURPLE "\033[0;35m"
#define LIGHT_PURPLE "\033[1;35m"
#define BROWN "\033[0;33m"
#define YELLOW "\033[1;33m"
#define LIGHT_GRAY "\033[0;37m"
//#define WHITE "\033[1;37m"

namespace oceanbase
{
namespace blocksstable
{
class ObMicroBlockTransformDesc;
class ObSSTablePrinter
{
public:
  static void print_title(const char *name, const int64_t value, const int64_t level = 1);
  static void print_title(const char *name, const int64_t level = 1);
  static void print_line(const char *name, const int32_t value, const int64_t level = 1);
  static void print_line(const char *name, const int64_t value, const int64_t level = 1);
  static void print_line(const char *name, const uint32_t value, const int64_t level = 1);
  static void print_line(const char *name, const uint64_t value, const int64_t level = 1);
  static void print_line(const char *name, const char *value, const int64_t level = 1);
  static void print_row_title(const blocksstable::ObDatumRow *row, const int64_t row_index);
  static void print_cell(const common::ObObj &cell);
  static void print_cell(const blocksstable::ObStorageDatum &datum);
  static void print_end_line(const int64_t level = 1);
  static void print_cols_info_start(const char *n1, const char *n2, const char *n3, const char *n4, const char *n5);
  static void print_cols_info_line(const int32_t &v1, const common::ObObjType v2, const common::ObOrderType v3, const int64_t &v4, const int64_t & v5);

  static void print_hex_micro_block_header(const ObMicroBlockData &block_data);
  static void print_hex_micro_block(const ObMicroBlockData &block_data, char *hex_print_buf, const int64_t buf_size);
  static void print_common_header(const ObMacroBlockCommonHeader *common_header);
  static void print_macro_block_header(const ObSSTableMacroBlockHeader *sstable_header);
  static void print_macro_block_header(const ObBloomFilterMacroBlockHeader *bf_macro_header);
  static void print_macro_block_header(const storage::ObLinkedMacroBlockHeader *linked_macro_header);
  static void print_index_row_header(const ObIndexBlockRowHeader *idx_row_header);
  static void print_index_minor_meta(const ObIndexBlockRowMinorMetaInfo *minor_meta);
  static void print_pre_agg_row(const int64_t column_cnt, ObAggRowReader &agg_row_reader);
  static void print_macro_meta(const ObDataMacroBlockMeta *macro_meta);
  static void print_store_row(
      const blocksstable::ObDatumRow *row,
      const ObObjMeta *obj_metas,
      const int64_t type_array_column_cnt,
      const bool is_index_block,
      const bool is_trans_sstable);
  static void print_store_row_hex(const blocksstable::ObDatumRow *row, const ObObjMeta *obj_metas, const int64_t buf_size, char *hex_print_buf);
  static void print_micro_header(const ObMicroBlockHeader *micro_block_header);
  static void print_encoding_micro_header(const ObMicroBlockHeader *micro_header);
  static void print_encoding_column_header(const ObColumnHeader *col_header, const int64_t col_id);
  static void print_cs_encoding_all_column_header(const ObAllColumnHeader &all_header);
  static void print_cs_encoding_column_header(const ObCSColumnHeader &col_header, const int64_t col_id);
  static void print_cs_encoding_column_meta(const char *start, const int64_t len,
                                            const ObCSColumnHeader::Type type, const int64_t col_id,
                                            char *hex_print_buf, const int64_t hex_buf_size);
  static void print_cs_encoding_orig_stream_data(
      const uint32_t stream_cnt, const ObMicroBlockTransformDesc &desc, const char *payload,
      const uint32_t all_string_data_offset, const uint32_t all_string_data_length);

  static void print_integer_stream_decoder_ctx(const uint32_t stream_idx, const ObIntegerStreamDecoderCtx &ctx,
                                               char *hex_print_buf, const int64_t hex_buf_size);
  static void print_string_stream_decoder_ctx(const uint32_t stream_idx, const ObStringStreamDecoderCtx &ctx,
                                              char *hex_print_buf, const int64_t hex_buf_size);
  static void print_bloom_filter_micro_header(const ObBloomFilterMicroBlockHeader *micro_block_header);
  static void print_bloom_filter_micro_block(const char* micro_block_buf, const int64_t micro_block_size);
};

}
}

#endif /* OB_SSTABLE_PRINTER_H_ */
