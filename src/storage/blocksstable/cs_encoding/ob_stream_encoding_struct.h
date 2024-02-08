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

#ifndef OCEANBASE_ENCODING_OB_STREAM_ENCODING_STRUCT_H_
#define OCEANBASE_ENCODING_OB_STREAM_ENCODING_STRUCT_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase
{

namespace common
{
  class ObCompressor;
}
namespace blocksstable
{

struct ObStreamData
{
  const char *buf_;
  uint32_t len_;

  ObStreamData() : buf_(nullptr), len_(0) {}
  ObStreamData(const char *buf, const uint32_t len) : buf_(buf), len_(len) {}
  OB_INLINE bool is_valid() { return (OB_NOT_NULL(buf_) && len_ > 0); }

  TO_STRING_KV(KP_(buf), K_(len));
  bool is_valid() const { return buf_ != nullptr && len_ != 0; }
  void set(const char *buf, const uint32_t len)
  {
    buf_ = buf;
    len_ = len;
  }
};

class ObCSEncodingOpt;
class ObPreviousColumnEncoding;

// ========================== encoding struct for integer stream ==========================//

struct ObIntegerStream
{
  enum Attribute : uint8_t
  {
    USE_NONE             = 0x0,
    USE_BASE             = 0x1,
    REPLACE_NULL_VALUE   = 0x2,
    DECIMAL_INT          = 0x4,
  };

  enum EncodingType : uint8_t
  {
    MIN_TYPE                   = 0,
    RAW                        = 1,
    DOUBLE_DELTA_ZIGZAG_RLE    = 2,
    DOUBLE_DELTA_ZIGZAG_PFOR   = 3,
    DELTA_ZIGZAG_RLE           = 4,
    DELTA_ZIGZAG_PFOR          = 5,
    SIMD_FIXEDPFOR             = 6,
    UNIVERSAL_COMPRESS         = 7,
    XOR_FIXED_PFOR             = 8,
    MAX_TYPE                   = 9,
  };


  OB_INLINE static const char *get_encoding_type_name(const uint8_t type)
  {
    switch (type) {
      case MIN_TYPE :                  { return "MIN_TYPE"; }
      case RAW:                        { return "RAW"; }
      case DOUBLE_DELTA_ZIGZAG_RLE:    { return "DOUBLE_DELTA_ZIGZAG_RLE"; }
      case DOUBLE_DELTA_ZIGZAG_PFOR:   { return "DOUBLE_DELTA_ZIGZAG_PFOR"; }
      case DELTA_ZIGZAG_RLE:           { return "DELTA_ZIGZAG_RLE"; }
      case DELTA_ZIGZAG_PFOR:          { return "DELTA_ZIGZAG_PFOR"; }
      case SIMD_FIXEDPFOR:             { return "SIMD_FIXEDPFOR"; }
      case UNIVERSAL_COMPRESS:         { return "UNIVERSAL_COMPRESS"; }
      case XOR_FIXED_PFOR:             { return "XOR_FIXED_PFOR"; }
      default:                         { return "MAX_TYPE"; }
    }
  }

  enum UintWidth: uint8_t
  {
    UW_1_BYTE     = 0,
    UW_2_BYTE     = 1,
    UW_4_BYTE     = 2,
    UW_8_BYTE     = 3,
    UW_16_BYTE    = 4,
    UW_32_BYTE    = 5,
    UW_64_BYTE    = 6,
    UW_MAX_BYTE   = 7,
  };

};

struct ObIntegerStreamMeta
{
  static constexpr uint8_t OB_INTEGER_STREAM_META_V1 = 0;
  ObIntegerStreamMeta() { reset(); }

  OB_INLINE void reset()
  {
    // all member must be reset, or will checksum error between replicas
    memset(this, 0, sizeof(*this));
    width_ = ObIntegerStream::UW_MAX_BYTE;
  }

  OB_INLINE bool is_valid() const
  {
    bool is_valid = ((ObIntegerStream::EncodingType::MAX_TYPE != type_)
                     && (width_ <= ObIntegerStream::UintWidth::UW_8_BYTE));
    return is_valid;
  }

  // check
  OB_INLINE bool is_use_null_replace_value() const { return ObIntegerStream::Attribute::REPLACE_NULL_VALUE & attr_; }
  OB_INLINE bool is_use_base() const { return ObIntegerStream::Attribute::USE_BASE & attr_; }
  OB_INLINE bool is_decimal_int() const { return ObIntegerStream::Attribute::DECIMAL_INT & attr_; }
  OB_INLINE bool is_1_byte_width() const { return width_ == ObIntegerStream::UintWidth::UW_1_BYTE; }
  OB_INLINE bool is_2_byte_width() const { return width_ == ObIntegerStream::UintWidth::UW_2_BYTE; }
  OB_INLINE bool is_4_byte_width() const { return width_ == ObIntegerStream::UintWidth::UW_4_BYTE; }
  OB_INLINE bool is_8_byte_width() const { return width_ == ObIntegerStream::UintWidth::UW_8_BYTE; }
  OB_INLINE ObIntegerStream::EncodingType get_encoding_type() const { return (ObIntegerStream::EncodingType )type_; }
  OB_INLINE bool is_raw_encoding() const { return ObIntegerStream::EncodingType::RAW == type_; }
  OB_INLINE bool is_unspecified_encoding() const { return ObIntegerStream::EncodingType::MIN_TYPE == type_; }
  OB_INLINE bool is_universal_compress_encoding() const { return ObIntegerStream::EncodingType::UNIVERSAL_COMPRESS == type_; }

  // set
  OB_INLINE void set_use_base() { attr_ |=  ObIntegerStream::Attribute::USE_BASE; }
  OB_INLINE void set_use_null_replace_value() { attr_ |=  ObIntegerStream::Attribute::REPLACE_NULL_VALUE; }
  OB_INLINE void set_is_decimal_int() { attr_ |= ObIntegerStream::Attribute::DECIMAL_INT; }
  OB_INLINE void set_raw_encoding() { type_ =  ObIntegerStream::EncodingType::RAW; }
  OB_INLINE void set_encoding_type(const ObIntegerStream::EncodingType type) { type_ = type; }
  OB_INLINE void set_null_replaced_value(const uint64_t null_replaced_value)
  {
    set_use_null_replace_value();
    null_replaced_value_ = null_replaced_value;
  }
  OB_INLINE uint64_t null_replaced_value() const { return null_replaced_value_; }
  OB_INLINE void set_base_value(const uint64_t base_value)
  {
    set_use_base();
    base_value_ = base_value;
  }
  OB_INLINE int64_t base_value() const { return base_value_; }
  OB_INLINE void set_precision_width_tag(const uint8_t width)
  {
    set_is_decimal_int();
    decimal_precision_width_ = width;
  }
  OB_INLINE uint8_t precision_width_tag() const { return decimal_precision_width_; }

  OB_INLINE void set_1_byte_width()  { width_ =  ObIntegerStream::UintWidth::UW_1_BYTE; }
  OB_INLINE void set_2_byte_width() { width_ =  ObIntegerStream::UintWidth::UW_2_BYTE; }
  OB_INLINE void set_4_byte_width() { width_ = ObIntegerStream::UintWidth::UW_4_BYTE; }
  OB_INLINE void set_8_byte_width() { width_ = ObIntegerStream::UintWidth::UW_8_BYTE; }


  OB_INLINE int set_uint_width_size(const uint32_t byte_size)
  {
    int ret = OB_SUCCESS;

    switch(byte_size) {
    case 1 :
      set_1_byte_width();
      break;
    case 2 :
      set_2_byte_width();
      break;
    case 4 :
      set_4_byte_width();
      break;
    case 8 :
      set_8_byte_width();
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "uint byte width size not invalid", K(ret), K(byte_size));
      break;
    }
    return ret;
  }

  OB_INLINE uint32_t get_uint_width_size() const
  {
    static uint32_t width_size_map_[ObIntegerStream::UintWidth::UW_MAX_BYTE] = {1, 2, 4, 8, 16, 32, 64};
    uint32_t byte_size = 0;
    if (OB_UNLIKELY(width_ >= ObIntegerStream::UintWidth::UW_MAX_BYTE)) {
    } else {
      byte_size = width_size_map_[width_];
    }
    return byte_size;
  }

  OB_INLINE uint8_t get_width_tag() const { return width_; }

  OB_INLINE int set_precision_width_size(const uint32_t byte_size)
  {
    int ret = OB_SUCCESS;

    switch(byte_size) {
    case 4 :
      set_precision_width_tag(ObIntegerStream::UintWidth::UW_4_BYTE);
      break;
    case 8 :
      set_precision_width_tag(ObIntegerStream::UintWidth::UW_8_BYTE);
      break;
    case 16 :
      set_precision_width_tag(ObIntegerStream::UintWidth::UW_16_BYTE);
      break;
    case 32 :
      set_precision_width_tag(ObIntegerStream::UintWidth::UW_32_BYTE);
      break;
    case 64 :
      set_precision_width_tag(ObIntegerStream::UintWidth::UW_64_BYTE);
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "precision byte width size not invalid", K(ret), K(byte_size));
      break;
    }
    return ret;
  }

  OB_INLINE uint32_t get_precision_width_size() const
  {
    static uint32_t width_size_map_[ObIntegerStream::UintWidth::UW_MAX_BYTE] = {1, 2, 4, 8, 16, 32, 64};
    uint32_t byte_size = 0;
    if (OB_UNLIKELY(decimal_precision_width_ >= ObIntegerStream::UintWidth::UW_MAX_BYTE)) {
    } else {
      byte_size = width_size_map_[decimal_precision_width_];
    }
    return byte_size;
  }

  TO_STRING_KV(K(version_), K(attr_), "type_name", ObIntegerStream::get_encoding_type_name(type_),
               K(type_), K(width_), K(base_value_), K(null_replaced_value_), K(decimal_precision_width_));

  NEED_SERIALIZE_AND_DESERIALIZE;


  uint8_t version_;
  uint8_t attr_;
  uint8_t type_;
  uint8_t width_;
  uint64_t base_value_;
  uint64_t null_replaced_value_;
  uint8_t decimal_precision_width_;
};

struct ObIntegerStreamEncoderInfo
{
  ObIntegerStreamEncoderInfo() { reset(); }
  void reset()
  {
    has_null_datum_ = false;
    is_monotonic_inc_ = false;
    encoding_opt_ = nullptr;
    previous_encoding_ = nullptr;
    stream_idx_ = -1;
    ObIAllocator *allocator_ = nullptr;
    compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  }
  TO_STRING_KV(K_(has_null_datum), K_(is_monotonic_inc),
      KP_(encoding_opt), KPC_(previous_encoding), K_(stream_idx),
      KP_(allocator), "compressor_type", all_compressor_name[compressor_type_]);

  bool has_null_datum_;
  bool is_monotonic_inc_;
  const ObCSEncodingOpt *encoding_opt_;
  const ObPreviousColumnEncoding *previous_encoding_;
  int32_t stream_idx_;
  ObCompressorType compressor_type_;
  ObIAllocator *allocator_;
};

struct ObIntegerStreamEncoderCtx
{
  ObIntegerStreamEncoderCtx() { reset(); }
  OB_INLINE void reset()
  {
    meta_.reset();
    info_.reset();
  }
  OB_INLINE bool is_valid() const
  {
    bool is_valid = meta_.is_valid();
    return is_valid;
  }
  int build_signed_stream_meta(const int64_t min, const int64_t max,
      const bool is_replace_null, const int64_t replace_value,
      const int64_t precision_width_size,
      const bool force_raw, uint64_t &range);
  int build_unsigned_stream_meta(const uint64_t min, const uint64_t max,
      const bool is_replace_null, const uint64_t replace_value,
      const bool force_raw, uint64_t &range);
  int build_offset_array_stream_meta(const uint64_t end_offset, const bool force_raw);
  int build_stream_encoder_info(const bool has_null,
                                bool is_monotonic_inc,
                                const ObCSEncodingOpt *encoding_opt,
                                const ObPreviousColumnEncoding *previous_encoding,
                                const int32_t stream_idx,
                                const ObCompressorType compressor_type,
                                ObIAllocator *allocator);

  int try_use_previous_encoding(bool &use_previous);
  TO_STRING_KV(K_(meta), K_(info));

  ObIntegerStreamMeta meta_;
  ObIntegerStreamEncoderInfo info_;
};

// ========================== encoding struct for string stream ==========================//

struct ObStringStreamMeta
{
  static constexpr uint8_t OB_STRING_STREAM_META_V1 = 0;
  enum Attribute : uint8_t
  {
    USE_NONE = 0x0,
    USE_ZERO_LEN_AS_NULL = 0x1,
    IS_FIXED_LEN_STRING = 0x02,
  };

  ObStringStreamMeta() { reset(); }
  void reset()
  {
    memset(this, 0, sizeof(*this));
  }
  OB_INLINE bool is_fixed_len_string() const
  {
    return (attr_ & Attribute::IS_FIXED_LEN_STRING) > 0;
  }
  OB_INLINE uint32_t get_fixed_string_len() const { return fixed_str_len_; }
  OB_INLINE bool is_use_zero_len_as_null() const
  {
    return (attr_ & Attribute::USE_ZERO_LEN_AS_NULL) > 0;
  }
  OB_INLINE void set_use_zero_len_as_null()
  {
    attr_ = (attr_ | Attribute::USE_ZERO_LEN_AS_NULL);
  }
  OB_INLINE void set_is_fixed_len_string()
  {
    attr_ = (attr_ | Attribute::IS_FIXED_LEN_STRING);
  }

  TO_STRING_KV(K_(version), K_(attr), K_(uncompressed_len), K_(fixed_str_len));

  NEED_SERIALIZE_AND_DESERIALIZE;

  uint8_t version_;
  uint8_t attr_;
  uint32_t uncompressed_len_;
  uint32_t fixed_str_len_;
};

struct ObStringStreamEncoderInfo
{
  ObStringStreamEncoderInfo() { reset(); }
  void reset()
  {
    compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
    raw_encoding_str_offset_ = false;
    encoding_opt_ = nullptr;
    previous_encoding_ = nullptr;
    int_stream_idx_ = -1;
    allocator_ = nullptr;
  }

  TO_STRING_KV("compressor_type", all_compressor_name[compressor_type_],
      K_(raw_encoding_str_offset), KP_(encoding_opt), KPC_(previous_encoding),
      K_(int_stream_idx), KP_(allocator));

  common::ObCompressorType compressor_type_;
  bool raw_encoding_str_offset_;
  const ObCSEncodingOpt *encoding_opt_;
  const ObPreviousColumnEncoding *previous_encoding_;
  int32_t int_stream_idx_;
  ObIAllocator *allocator_;
};

struct ObStringStreamEncoderCtx
{
  ObStringStreamEncoderCtx() { reset(); }

  OB_INLINE void reset()
  {
    meta_.reset();
    info_.reset();
  }
  int build_string_stream_meta(const int64_t fix_len,
                               const bool use_zero_length_as_null,
                               const uint32_t uncompress_len);
  int build_string_stream_encoder_info(
       const common::ObCompressorType compressor_type,
       const bool raw_encoding_str_offset,
       const ObCSEncodingOpt *encoding_opt,
       const ObPreviousColumnEncoding *previous_encoding,
       const int32_t int_stream_idx,
       ObIAllocator *allocator);

  TO_STRING_KV(K_(meta), K_(info));


  ObStringStreamMeta meta_;
  ObStringStreamEncoderInfo info_;
};


struct ObIntegerStreamDecoderCtx
{
  ObIntegerStreamDecoderCtx()
    : meta_(), count_(0),
      compressor_type_(ObCompressorType::INVALID_COMPRESSOR)
  {}

  ObIntegerStreamDecoderCtx(const uint32_t count, const ObCompressorType type)
    : meta_(), count_(count), compressor_type_(type)
  {}
  OB_INLINE bool is_valid() const { return (count_ > 0); }

  TO_STRING_KV(K(meta_), K(count_), K_(compressor_type));

  ObIntegerStreamMeta meta_;
  uint32_t count_; // original integer count
  ObCompressorType compressor_type_;

};

struct ObStringStreamDecoderCtx
{
  ObStringStreamDecoderCtx() : meta_() {}

  TO_STRING_KV(K_(meta));
  ObStringStreamMeta meta_;
};
enum ObRefStoreWidthV : uint8_t
{
  REF_1_BYTE = ObIntegerStream::UintWidth::UW_1_BYTE,
  REF_2_BYTE = ObIntegerStream::UintWidth::UW_2_BYTE,
  REF_4_BYTE = ObIntegerStream::UintWidth::UW_4_BYTE,
  REF_8_BYTE = ObIntegerStream::UintWidth::UW_8_BYTE,
  NOT_REF = 4,
  REF_IN_DATUMS = 5,
  MAX_WIDTH_V = 6
};

enum ObVecDecodeRefWidth : uint8_t
{
  VDRW_1_BYTE = ObIntegerStream::UintWidth::UW_1_BYTE,
  VDRW_2_BYTE = ObIntegerStream::UintWidth::UW_2_BYTE,
  VDRW_4_BYTE = ObIntegerStream::UintWidth::UW_4_BYTE,
  VDRW_8_BYTE = ObIntegerStream::UintWidth::UW_8_BYTE,
  VDRW_NOT_REF = 4,
  VDRW_TEMP_UINT32_REF = 5, // ref is a temporarily allocated uint32-array, regardless of ref store width.
  VDRW_MAX = 6
};

template <int TYPE_TAG>
struct ObCSEncodingStoreTypeInference { typedef unsigned char Type; };

template <> struct ObCSEncodingStoreTypeInference<ObIntegerStream::UintWidth::UW_1_BYTE> { typedef uint8_t Type; };
template <> struct ObCSEncodingStoreTypeInference<ObIntegerStream::UintWidth::UW_2_BYTE> { typedef uint16_t Type; };
template <> struct ObCSEncodingStoreTypeInference<ObIntegerStream::UintWidth::UW_4_BYTE> { typedef uint32_t Type; };
template <> struct ObCSEncodingStoreTypeInference<ObIntegerStream::UintWidth::UW_8_BYTE> { typedef uint64_t Type; };
template <> struct ObCSEncodingStoreTypeInference<ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF> { typedef uint32_t Type; };

#define FIX_STRING_OFFSET_WIDTH_V 4 // represet fixed length string when do partial specialization for template

static OB_INLINE int32_t *get_width_tag_map()
{
  static int32_t width_tag_map_[] = {
    0, // 0
    ObIntegerStream::UW_1_BYTE, // 1
    ObIntegerStream::UW_2_BYTE, // 2
    0,
    ObIntegerStream::UW_4_BYTE, // 4
    0,
    0,
    0,
    ObIntegerStream::UW_8_BYTE // 8
  };
  return width_tag_map_;
}

}
}

#endif
