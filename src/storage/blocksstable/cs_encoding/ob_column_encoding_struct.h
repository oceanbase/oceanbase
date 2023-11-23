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

#ifndef OCEANBASE_ENCODING_OB_COLUMN_ENCODING_STRUCT_H_
#define OCEANBASE_ENCODING_OB_COLUMN_ENCODING_STRUCT_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compress_util.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_store_format.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"
#include "ob_stream_encoding_struct.h"


namespace oceanbase
{
namespace blocksstable
{
struct ObCSColumnHeader
{
  static const uint8_t OB_COLUMN_HEADER_V1 = 0;
  static const int64_t MAX_INT_STREAM_COUNT_OF_COLUMN = 4;
  enum Type : uint8_t
  {
    INTEGER = 0,
    STRING = 1,
    INT_DICT = 2,
    STR_DICT = 3,
    MAX_TYPE
  };

  static OB_INLINE const char *get_type_name(const uint8_t type)
  {
    switch (type) {
      case INTEGER : { return "INTEGER"; }
      case STRING :  { return "STRING"; }
      case INT_DICT: { return "INT_DICT"; }
      case STR_DICT: { return "STR_DICT"; }
      default:       { return "MAX_TYPE"; }
    }
  }

  enum Attribute : uint8_t
  {
    IS_FIXED_LENGTH = 0x01,
    HAS_NULL_BITMAP = 0x02,
    OUT_ROW = 0x04,
    MAX_ATTRIBUTE,
  };

  uint8_t version_;
  uint8_t type_;
  uint8_t attrs_; //  bitwise-or of attr
  uint8_t obj_type_;

  static_assert(UINT8_MAX >= common::ObObjType::ObMaxType, "ObObjType is stored in ObColumnCSHeader with 1 byte");

  ObCSColumnHeader()
  {
    reuse();
  }
  void reuse()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const
  {
    return type_ >= 0 && type_ < MAX_TYPE && obj_type_ < ObObjType::ObMaxType;
  }

  inline bool is_fixed_length() const
  {
    return attrs_ & IS_FIXED_LENGTH;
  }
  inline void set_is_fixed_length()
  {
    attrs_ |= IS_FIXED_LENGTH;
  }
  inline bool is_integer_dict() const
  {
    return INT_DICT == type_;
  }
  inline bool has_null_bitmap() const
  {
    return attrs_ & HAS_NULL_BITMAP;
  }
  inline void set_has_null_bitmap()
  {
    attrs_ |= HAS_NULL_BITMAP;
  }
  inline bool is_out_row() const
  {
    return attrs_ & OUT_ROW;
  }
  inline void set_out_row()
  {
    attrs_ |= OUT_ROW;
  }
  inline ObObjType get_store_obj_type() const
  {
    return static_cast<ObObjType>(obj_type_);
  }

  TO_STRING_KV(K_(type), K_(attrs), K_(obj_type));

} __attribute__((packed));


struct ObAllColumnHeader
{
  static constexpr uint8_t OB_ALL_COLUMN_HEADER_V1 = 0;
  enum Attribute
  {
    // This bit is used to identify whether the memory data format has been transformed,
    // always set 0 when serialize to disk.
    IS_FULL_TRANSFORMED = 0x01,
    IS_ALL_STRING_COMPRESSED =0x02
  };

  ObAllColumnHeader()
  {
    reuse();
  }
  void reuse()
  {
    memset(this, 0, sizeof(*this));
  }

  TO_STRING_KV(K_(version), KP_(attrs), K_(all_string_data_length), K_(stream_offsets_length), K_(stream_count));
  bool is_valid() const { return OB_ALL_COLUMN_HEADER_V1 == version_; }

  inline bool is_full_transformed() const { return attrs_ & IS_FULL_TRANSFORMED; }
  inline void set_full_transformed() { attrs_ |= IS_FULL_TRANSFORMED; }
  inline bool is_all_string_compressed() const { return attrs_ & IS_ALL_STRING_COMPRESSED; }
  inline void set_is_all_string_compressed() { attrs_ |= IS_ALL_STRING_COMPRESSED; }


  uint8_t version_;
  uint8_t attrs_; // bitwise-or of Attribute
  uint32_t all_string_data_length_;
  uint32_t stream_offsets_length_;
  uint16_t stream_count_;

} __attribute__((packed));

struct ObDictEncodingMeta final
{
  static constexpr uint8_t OB_DICT_ENCODING_META_V1 = 0;
  enum Attribute
  {
    IS_SORTED = 0x1,
    HAS_NULL = 0x2,
    CONST_ENCODING_REF = 0x4,
  };
  ObDictEncodingMeta()
    : version_(OB_DICT_ENCODING_META_V1), attrs_(0),
      distinct_val_cnt_(0), ref_row_cnt_(0) {}
  void reuse()
  {
    version_ = OB_DICT_ENCODING_META_V1;
    attrs_ = 0;
    distinct_val_cnt_ = 0;
    ref_row_cnt_ = 0;
  }

  bool has_null() const { return attrs_ & HAS_NULL; }
  void set_has_null() { attrs_ |= HAS_NULL; }
  bool is_sorted() const { return attrs_ & IS_SORTED; }
  bool is_const_encoding_ref() const { return attrs_ & CONST_ENCODING_REF; }
  void set_const_encoding_ref(const uint32_t exception_cnt)
  {
    attrs_ |= CONST_ENCODING_REF;
    // 1(exception_cnt) + 1(const_ref) + exception_cnt(exception_row_ids) + exception_cnt(exception_refs)
    ref_row_cnt_ = 2 + 2 * exception_cnt;
  }

  uint8_t version_;
  uint8_t attrs_; // bitwise-or of Attribute
  uint32_t distinct_val_cnt_;
  uint32_t ref_row_cnt_;

  TO_STRING_KV(K_(version), K_(attrs), K_(distinct_val_cnt), K_(ref_row_cnt));

} __attribute__((packed));


struct ObColumnEncodingIdentifier
{
  ObColumnEncodingIdentifier()
    : column_encoding_type_(ObCSColumnHeader::MAX_TYPE),
      int_stream_count_(0),
      flags_(0) {}
  ObColumnEncodingIdentifier(const ObCSColumnHeader::Type type,
                             const int32_t int_stream_cnt,
                             const int32_t flags)
    : column_encoding_type_(type),
      int_stream_count_(int_stream_cnt),
      flags_(flags) {}
  void set(const ObCSColumnHeader::Type type, const int32_t int_stream_cnt, const int32_t flags)
  {
    column_encoding_type_ = type;
    int_stream_count_ = int_stream_cnt;
    flags_ = flags;
  }
  inline bool operator==(const ObColumnEncodingIdentifier &rhs) const
  {
    return column_encoding_type_ == rhs.column_encoding_type_ &&
        int_stream_count_ == rhs.int_stream_count_ && flags_ == rhs.flags_;
  }
  inline bool operator!=(const ObColumnEncodingIdentifier &rhs) const
  {
    return !(*this == rhs);
  }
  TO_STRING_KV("column_encoding_type", ObCSColumnHeader::get_type_name(column_encoding_type_),
               K_(int_stream_count), K_(flags));

  ObCSColumnHeader::Type column_encoding_type_;
  int32_t int_stream_count_;
  int32_t flags_;
};

struct ObPreviousColumnEncoding
{
  ObPreviousColumnEncoding() { memset(this, 0, sizeof(*this)); }

  TO_STRING_KV(K_(identifier),
               "stream0_encoding_type",  ObIntegerStream::get_encoding_type_name(stream_encoding_types_[0]),
               "stream1_encoding_type",  ObIntegerStream::get_encoding_type_name(stream_encoding_types_[1]),
               "stream2_encoding_type",  ObIntegerStream::get_encoding_type_name(stream_encoding_types_[2]),
               "stream3_encoding_type",  ObIntegerStream::get_encoding_type_name(stream_encoding_types_[3]),
               K_(redetect_cycle), K_(is_valid), K_(need_redetect),
               K_(cur_block_count), K_(force_no_redetect));

  ObColumnEncodingIdentifier identifier_;
  ObIntegerStream::EncodingType stream_encoding_types_[ObCSColumnHeader::MAX_INT_STREAM_COUNT_OF_COLUMN];
  int32_t redetect_cycle_;
  int64_t cur_block_count_;
  bool is_valid_;
  bool need_redetect_;
  bool force_no_redetect_; // just for test to specify the stream encoding type
};

class ObPreviousCSEncoding
{
public:
  static const int32_t MAX_REDETECT_CYCLE;
  ObPreviousCSEncoding() :
    is_inited_(false),
    previous_encoding_of_columns_() {}
  int init(const int32_t col_count);
  int check_and_set_state(const int32_t column_idx,
                          const ObColumnEncodingIdentifier identifier,
                          const int64_t cur_block_count);
  int update_column_encoding_types(const int32_t column_idx,
                                   const ObColumnEncodingIdentifier identifier,
                                   const ObIntegerStream::EncodingType *stream_types,
                                   bool force_no_redetect = false);
  ObPreviousColumnEncoding *get_column_encoding(const int32_t column_idx)
  {
    return &previous_encoding_of_columns_.at(column_idx);
  }
  TO_STRING_KV(K_(is_inited), K_(previous_encoding_of_columns));

private:
  bool is_inited_;
  common::ObArray<ObPreviousColumnEncoding> previous_encoding_of_columns_;
};

// TODO(fenggu.yh)  union { ObMicroBlockEncoderOpt; ObCSEncodingOpt}
struct ObCSEncodingOpt
{
  static const bool STREAM_ENCODINGS_DEFAULT[ObIntegerStream::EncodingType::MAX_TYPE];
  static const bool STREAM_ENCODINGS_NONE[ObIntegerStream::EncodingType::MAX_TYPE];

  ObCSEncodingOpt() { set_store_type(CS_ENCODING_ROW_STORE); }
  OB_INLINE void set_store_type(const common::ObRowStoreType store_type) {
    switch (store_type) {
      case CS_ENCODING_ROW_STORE:
        encodings_ = STREAM_ENCODINGS_DEFAULT;
        break;
      //case SELECTIVE_CS_ENCODING_ROW_STORE:
      //  encodings_ = STREAM_ENCODINGS_FOR_PERFORMANCE;
      //  break;
      default:
        encodings_ = STREAM_ENCODINGS_NONE;
        break;
    }
  }
  OB_INLINE bool is_enabled(const ObIntegerStream::EncodingType type) const { return encodings_[type]; }

  const bool *encodings_;
};


class ObEncodingHashTable;
class ObMicroBlockEncodingCtx;
struct ObColumnCSEncodingCtx
{
  ObIAllocator *allocator_;
  int64_t null_cnt_;
  int64_t nope_cnt_;
  int64_t var_data_size_;
  int64_t dict_var_data_size_;
  int64_t fix_data_size_;
  int64_t max_string_size_;
  const ObPodFix2dArray<ObDatum, 1 << 20, common::OB_MALLOC_NORMAL_BLOCK_SIZE> *col_datums_;
  ObEncodingHashTable *ht_;
  const ObMicroBlockEncodingCtx *encoding_ctx_;
  ObMicroBufferWriter *all_string_buf_writer_;

  bool need_sort_;
  bool force_raw_encoding_;
  bool has_zero_length_datum_;
  bool is_wide_int_;
  uint64_t integer_min_;
  uint64_t integer_max_;

  ObColumnCSEncodingCtx() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }

  void try_set_need_sort(const ObCSColumnHeader::Type type, const int64_t column_index, const bool micro_block_has_lob_out_row);

  TO_STRING_KV(K_(null_cnt), K_(nope_cnt), K_(var_data_size),
               K_(dict_var_data_size), K_(fix_data_size),
               KP_(col_datums), KP_(ht), KP_(encoding_ctx), K_(max_string_size),
               K_(need_sort), K_(force_raw_encoding),
               K_(has_zero_length_datum), K_(is_wide_int), K_(integer_min), K_(integer_max));
};

//========================== ObColumnCSDecoderCtx ===================================//
struct ObBaseColumnDecoderCtx
{
  ObBaseColumnDecoderCtx() { reset(); }

  common::ObObjMeta obj_meta_;
  enum ObNullFlag : uint8_t
  {
    HAS_NO_NULL = 0, // has no null
    HAS_NULL_BITMAP = 1, // must be not dict encoding, because dict encoding use max ref as null
    IS_NULL_REPLACED = 2, // represet use_null_replaced_value or use_zero_len_as_null
    IS_NULL_REPLACED_REF = 3, // must be dict encoding
    MAX = 4
  };
  ObNullFlag null_flag_;
  union
  {
    const void *null_desc_;
    const char *null_bitmap_; // 1 is null, 0 not null, valid when null_flag_ == HAS_NULL_BITMAP
    int64_t null_replaced_ref_; // dict may use a special ref(usually max_ref + 1) to replace null, valid when null_flag_ == IS_NULL_REPLACED_REF
    int64_t null_replaced_value_; // valid when column type is integer and  null_flag_ == IS_NULL_REPLACED
  };
  const ObMicroBlockHeader *micro_block_header_;
  const ObCSColumnHeader *col_header_;
   // Pointer to ColumnParam for padding in filter pushdown
  const share::schema::ObColumnParam *col_param_;
  common::ObIAllocator *allocator_;

  OB_INLINE bool has_no_null() const { return HAS_NO_NULL == null_flag_; }
  OB_INLINE bool has_null_bitmap() const { return HAS_NULL_BITMAP == null_flag_; }
  OB_INLINE bool is_null_replaced() const { return IS_NULL_REPLACED == null_flag_; }
  OB_INLINE bool is_null_replaced_ref() const { return IS_NULL_REPLACED_REF == null_flag_; }

  OB_INLINE void set_col_param(const share::schema::ObColumnParam *col_param)
  {
    col_param_ = col_param;
  }
  void reset() { MEMSET(this, 0, sizeof(ObBaseColumnDecoderCtx));}

  TO_STRING_KV(K_(obj_meta), KPC_(micro_block_header), KPC_(col_header), KP_(allocator), K_(null_flag), KP_(col_param));
};

struct ObIntegerColumnDecoderCtx : public ObBaseColumnDecoderCtx
{
  ObIntegerColumnDecoderCtx()
    : ObBaseColumnDecoderCtx(), data_(nullptr), ctx_(nullptr), datum_len_(0) {}

  const char *data_;
  const ObIntegerStreamDecoderCtx *ctx_;
  uint32_t datum_len_;

  INHERIT_TO_STRING_KV("ObBaseColumnDecoderCtx", ObBaseColumnDecoderCtx, KP_(data), K_(datum_len), KPC_(ctx));
};

struct ObStringColumnDecoderCtx : public ObBaseColumnDecoderCtx
{
  ObStringColumnDecoderCtx()
    : ObBaseColumnDecoderCtx(),
      str_data_(nullptr), str_ctx_(nullptr),
      offset_data_(nullptr), offset_ctx_(nullptr),
      need_copy_(false) {}
  const char *str_data_;
  const ObStringStreamDecoderCtx *str_ctx_;
  const char *offset_data_;
  const ObIntegerStreamDecoderCtx *offset_ctx_; // this is nullptr for fixed len string
  bool need_copy_; // whether need copy string to datum or just modify the datum's ptr

  INHERIT_TO_STRING_KV("ObBaseColumnDecoderCtx", ObBaseColumnDecoderCtx,
      KP_(str_data), KPC_(str_ctx), KP_(offset_data), KPC_(offset_ctx), K_(need_copy));
};

struct ObDictColumnDecoderCtx : public ObBaseColumnDecoderCtx
{
  ObDictColumnDecoderCtx()
    : ObBaseColumnDecoderCtx(),
      str_data_(nullptr), str_ctx_(nullptr),
      offset_data_(nullptr), offset_ctx_(nullptr),
      ref_data_(nullptr), ref_ctx_(nullptr),
      dict_meta_(nullptr), datum_len_(0) {}

  const char *str_data_; // for string
  const ObStringStreamDecoderCtx *str_ctx_; // for string
  union {
    const char *offset_data_;  // for string
    const char * int_data_; // for integer
  };
  union {
    const ObIntegerStreamDecoderCtx *offset_ctx_; // for string
    const ObIntegerStreamDecoderCtx *int_ctx_; // for integer
  };
  const char *ref_data_;
  const ObIntegerStreamDecoderCtx *ref_ctx_;
  const ObDictEncodingMeta *dict_meta_;

  union {
    bool need_copy_; // whether need copy string to datum or just modify the datum's ptr
    uint32_t datum_len_; // for integer
  };

  INHERIT_TO_STRING_KV("ObBaseColumnDecoderCtx", ObBaseColumnDecoderCtx, KP_(str_data), KPC_(str_ctx),
      KP_(offset_data), KPC_(offset_ctx), KP_(int_data), KPC_(int_ctx), KP_(ref_data), KPC_(ref_ctx),
      KPC_(dict_meta), K_(need_copy), K_(datum_len));
};


struct ObColumnCSDecoderCtx
{
  ObColumnCSDecoderCtx() { reset(); }
  ObCSColumnHeader::Type type_;
  union
  {
    ObIntegerColumnDecoderCtx integer_ctx_;
    ObStringColumnDecoderCtx string_ctx_;
    ObDictColumnDecoderCtx dict_ctx_;
  };
  void reset() { MEMSET(this, 0, sizeof(ObColumnCSDecoderCtx));}
  OB_INLINE bool is_integer_type() const { return ObCSColumnHeader::INTEGER == type_; }
  OB_INLINE bool is_string_type() const { return ObCSColumnHeader::STRING == type_; }
  OB_INLINE bool is_int_dict_type() const { return ObCSColumnHeader::INT_DICT == type_; }
  OB_INLINE bool is_string_dict_type() const { return ObCSColumnHeader::STR_DICT == type_; }

  ObBaseColumnDecoderCtx& get_base_ctx()
  {
    ObBaseColumnDecoderCtx *base_ctx = nullptr;
    if (is_integer_type()) {
      base_ctx = &integer_ctx_;
    } else if (is_string_type()) {
      base_ctx = &string_ctx_;
    } else if (is_int_dict_type() || is_string_dict_type()) {
      base_ctx = &dict_ctx_;
    }
    return *base_ctx;
  }

  TO_STRING_KV(K_(type));
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif
