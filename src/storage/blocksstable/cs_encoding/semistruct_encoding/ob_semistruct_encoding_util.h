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

#ifndef OCEANBASE_ENCODING_OB_SEMISTRUCT_ENCODING_UTIL_H_
#define OCEANBASE_ENCODING_OB_SEMISTRUCT_ENCODING_UTIL_H_

#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_json.h"

namespace oceanbase
{
namespace blocksstable
{

class ObSemiStructColumnEncodeCtx
{
public:
  static const int MAX_SCHEMA_NOT_MATCH_COUNT = 8;
  static const int MAX_NOT_ENCODE_COUNT = 16;
public:
  ObSemiStructColumnEncodeCtx(ObMicroBlockEncodingCtx &ctx):
    allocator_("SemiEnc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    schema_allocator_("SemiSchemAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    column_index_(-1),
    col_desc_(),
    datums_(nullptr),
    sub_schema_(nullptr),
    sub_col_datums_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SemiEnc", MTL_ID())),
    hashtables_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SemiEnc", MTL_ID())),
    hashtable_factory_(),
    encoder_allocator_(cs_encoder_sizes, lib::ObMemAttr(MTL_ID(), "SemiEnc")),
    sub_encoders_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SemiEnc", MTL_ID())),
    sub_col_ctxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SemiEnc", MTL_ID())),
    previous_cs_encoding_(),
    encoding_ctx_(ctx),
    all_string_buf_writer_(nullptr),
    schema_not_match_block_count_(0),
    not_encode_block_count_(0),
    encounter_outrow_block_cnt_(0),
    is_enable_(true),
    freq_threshold_(share::ObSemistructProperties::DEFAULT_SEMISTRUCT_FREQ_THRESHOLD)
  {}

  ~ObSemiStructColumnEncodeCtx() { reset(); }

  int init();
  void reset();
  void reuse();

  int scan();
  int init_sub_column_encoders();
  bool is_enable() const { return is_enable_; }
  void disable_encoding() { is_enable_ = false; }
  int get_sub_column_type(const int64_t column_idx, ObObjMeta &type) const;
  int64_t get_store_column_count() const;
  int64_t get_sub_schema_serialize_size() const;
  int serialize_sub_schema(ObMicroBufferWriter &buf_writer);

  TO_STRING_KV(KP(this), K_(is_enable), K_(sub_schema), K_(schema_not_match_block_count),
      K_(not_encode_block_count), K_(encounter_outrow_block_cnt), KP_(datums), K_(sub_col_datums));

private:
  int do_scan();
  int init_sub_schema();
  int init_sub_column_datums();
  int fill_sub_column_datums();
  int scan_sub_column_datums();
  int build_sub_schema(ObJsonSchemaFlatter &flatter, ObString &schema_buf);
  int scan_sub_column_datums(const int64_t column_index, const ObObjType obj_type, const ObColDatums &datums, ObColumnCSEncodingCtx &sub_col_ctx);
  int init_sub_column_encode_ctxs();
  int check_has_outrow(bool &has_outrow);
  int try_use_previous_encoder(const int64_t column_idx, const ObObjTypeStoreClass store_class, ObIColumnCSEncoder *&e);
  int choose_encoder_for_integer(const int64_t column_idx, ObIColumnCSEncoder *&e);
  int choose_encoder_for_string(const int64_t column_idx, ObIColumnCSEncoder *&e);
  template <typename T>
  int alloc_and_init_encoder(const int64_t column_index, ObIColumnCSEncoder *&e);
  void free_encoder(ObIColumnCSEncoder *encoder);
  void free_encoders();

  int update_previous_info_before_encoding(const int32_t col_idx, ObIColumnCSEncoder &e);

public:
  ObArenaAllocator allocator_;
  ObArenaAllocator schema_allocator_;
  int64_t column_index_;
  ObColDesc col_desc_;
  const ObColDatums *datums_;
  ObSemiSchemaAbstract *sub_schema_;
  ObArray<ObColDatums *> sub_col_datums_;
  ObArray<ObDictEncodingHashTable *> hashtables_;
  ObDictEncodingHashTableFactory hashtable_factory_;
  ObCSEncoderAllocator encoder_allocator_;
  ObArray<ObIColumnCSEncoder *> sub_encoders_;
  ObArray<ObColumnCSEncodingCtx> sub_col_ctxs_;
  ObPreviousCSEncoding previous_cs_encoding_;
  const ObMicroBlockEncodingCtx &encoding_ctx_;
  ObMicroBufferWriter *all_string_buf_writer_;
  int32_t schema_not_match_block_count_;
  int32_t not_encode_block_count_;
  int32_t encounter_outrow_block_cnt_;
  bool is_enable_;
  uint8_t freq_threshold_;
};

class ObSemiStructEncodeCtx
{
public:
  struct InnerWrapper {
    InnerWrapper():
      col_idx_(-1),
      col_ctx_(nullptr)
    {}
    int64_t col_idx_;
    ObSemiStructColumnEncodeCtx *col_ctx_;

    TO_STRING_KV(K_(col_idx), KPC_(col_ctx))
  };

public:
  ObSemiStructEncodeCtx():
    allocator_("SemiEncCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    col_ctxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SemiEncCtx", MTL_ID()))
  {}

  ~ObSemiStructEncodeCtx() { reset(); }

  void reset();
  void reuse();

  int get_col_ctx(const int64_t column_index, ObMicroBlockEncodingCtx &ctx, ObSemiStructColumnEncodeCtx *&res);

private:
  ObArenaAllocator allocator_;
  ObSEArray<InnerWrapper, 10> col_ctxs_;

};

class ObSemiStructDecodeBaseHandler
{
public:
  ObSemiStructDecodeBaseHandler() {}
  virtual ~ObSemiStructDecodeBaseHandler() {};

  virtual void reset() = 0;
  virtual int serialize(const ObDatumRow &row, ObString &result) = 0;
  virtual int check_can_pushdown(
      const sql::ObSemiStructWhiteFilterNode &filter_node,
      bool &can_pushdown, uint16_t &sub_col_idx) const = 0;

};

class ObSemiStructDecodeHandler : public ObSemiStructDecodeBaseHandler
{
public:
  ObSemiStructDecodeHandler(common::ObArenaAllocator &allocator):
      sub_schema_(nullptr),
      reassembler_(nullptr),
      allocator_(allocator)
   {}
  virtual ~ObSemiStructDecodeHandler() {}

  int init(ObIAllocator &allocator, const char* sub_schema_data_ptr, const int64_t sub_schema_data_len);
  void reuse();
  virtual void reset();
  ObDatumRow& get_sub_row() { return sub_row_; }
  virtual int serialize(const ObDatumRow &row, ObString &result);
  virtual int check_can_pushdown(
      const sql::ObSemiStructWhiteFilterNode &filter_node,
      bool &can_pushdown, uint16_t &sub_col_idx) const;

  TO_STRING_KV(KPC_(sub_schema), KP_(reassembler));

public:
  ObSemiSchemaAbstract *sub_schema_;
  ObJsonReassembler *reassembler_;
  common::ObArenaAllocator &allocator_;
  ObDatumRow sub_row_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_SEMISTRUCT_ENCODING_UTIL_H_
