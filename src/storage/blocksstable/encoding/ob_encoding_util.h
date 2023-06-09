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

#ifndef OCEANBASE_ENCODING_OB_ENCODING_UTIL_H_
#define OCEANBASE_ENCODING_OB_ENCODING_UTIL_H_

#include "lib/allocator/ob_malloc.h"
#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"
#include "common/ob_action_flag.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace blocksstable
{
extern const char* OB_ENCODING_LABEL_HASH_TABLE;
extern const char* OB_ENCODING_LABEL_HT_FACTORY;
extern const char* OB_ENCODING_LABEL_PIVOT;
extern const char* OB_ENCODING_LABEL_DATA_BUFFER;
extern const char* OB_ENCODING_LABEL_ROWKEY_BUFFER;
extern const char* OB_ENCODING_LABEL_ROW_BUFFER;
extern const char* OB_ENCODING_LABEL_MULTI_PREFIX_TREE;
extern const char* OB_ENCODING_LABEL_PREFIX_TREE_FACTORY;
extern const char* OB_ENCODING_LABEL_STRING_DIFF;

#define ENCODING_ADAPT_MEMCPY(dst, src, len) \
  switch (len) { \
    case 1: { \
      MEMCPY(dst, src, 1); \
      break; \
    } \
    case 2: { \
      MEMCPY(dst, src, 2); \
      break; \
    } \
    case 4: { \
      MEMCPY(dst, src, 4); \
      break; \
    } \
    case 8: { \
      MEMCPY(dst, src, 8); \
      break; \
    } \
    default: { \
      MEMCPY(dst, src, len); \
    } \
  }

enum ObObjTypeStoreClass
{
  ObExtendSC = 0,
  ObIntSC, // signed integers, time types
  ObUIntSC, // unsigned integers, year, float, double
  ObNumberSC, // number
  ObStringSC, // varchar, char, binary, raw, nvarchar2, nchar, udt_bitmap
  ObTextSC, // text
  ObOTimestampSC, // timestamptz, timestamp ltz, timestamp nano
  ObIntervalSC, //oracle interval year to month interval day to second
  ObLobSC,  //lob
  ObJsonSC, // json
  ObGeometrySC, // geometry
  ObMaxSC,
};

OB_INLINE bool is_string_encoding_valid(const ObObjTypeStoreClass sc)
{
  return (sc == ObStringSC || sc == ObTextSC || sc == ObJsonSC || sc == ObGeometrySC);
}

OB_INLINE bool store_class_might_contain_lob_locator(const ObObjTypeStoreClass sc)
{
  return (sc == ObTextSC || sc == ObLobSC || sc == ObJsonSC || sc == ObGeometrySC);
}

OB_INLINE bool is_var_length_type(const ObObjTypeStoreClass sc)
{
  return (sc == ObNumberSC || sc == ObStringSC || sc == ObTextSC
      || sc == ObLobSC || sc == ObJsonSC || sc == ObGeometrySC);
}

OB_INLINE ObObjTypeStoreClass *get_store_class_map()
{
  static ObObjTypeStoreClass store_class_map[] = {
    ObExtendSC, // ObNullTC
    ObIntSC, // ObIntTC
    ObUIntSC, // ObUIntTC
    ObUIntSC, // ObFloatTC
    ObUIntSC, // ObDoubleTC
    ObNumberSC, // ObNumberTC
    ObIntSC, // ObDateTimeTC
    ObIntSC, // ObDateTC
    ObIntSC, // ObTimeTC
    ObUIntSC, // ObYearTC
    ObStringSC, // ObStringTC
    ObExtendSC, // ObExtendTC
    ObExtendSC, // ObUnknownTC
    ObTextSC, // ObTextTC
    ObIntSC, // ObBitTC
    ObIntSC, //ObEnumSetTC
    ObIntSC, //ObEnumSetInnerTC
    ObOTimestampSC,//ObOTimestampTC
    ObStringSC, // ObRawTC
    ObIntervalSC, //ObIntervalTC
    ObStringSC, // ObRowIDTC
    ObLobSC,    //ObLobTC
    ObJsonSC,   //ObJsonTC
    ObGeometrySC, //ObGeometryTC
    ObStringSC, // ObUserDefinedSQLTC， UDT null_bitmaps
    ObMaxSC // ObMaxTC
  };
  STATIC_ASSERT(ARRAYSIZEOF(store_class_map) == common::ObMaxTC + 1,
      "store class map count mismatch with type class count");
  return store_class_map;
}

OB_INLINE int64_t *get_type_size_map()
{
  static int64_t type_size_map[] = {
    0, // ObNullType
    1, // ObTinyIntType
    2, // ObSmallIntType
    4, // ObMediumIntType
    4, // ObInt32Type
    8, // ObIntType
    1, // ObUTinyIntType
    2, // ObUSmallIntType
    4, // ObUMediumIntType
    4, // ObUInt32Type
    8, // ObUInt64Type
    4, // ObFloatType
    8, // ObDoubleType
    4, // ObUFloatType
    8, // ObUDoubleType
    -1, // ObNumberType
    -1, // ObUNumberType
    8, // ObDateTimeType
    8, // ObTimestampType
    4, // ObDateType=19,
    8, // ObTimeType
    1, // ObYearType=21,
    -1, // ObVarcharType=22
    -1, // ObCharType=23
    -1, // ObHexStringType
    0, // ObExtendType
    -1, // ObUnknownType
    -1, // ObTinyTextType
    -1, // ObTextType
    -1, // ObMediumTextType
    -1, // ObLongTextType
    8, // ObBitType
    8, // ObEnumType
    8, // ObSetType
    8, //ObEnumInnerType
    8, //ObSetInnerType
    -1, //ObTimestampTZType
    -1, //ObTimestampLTZType
    -1, //ObTimestampNanoType
    -1, //ObRawType
    -1, //ObIntervalYMType
    -1, //ObIntervalDSType
    -1, //ObNumberFloatType
    -1, //ObNVarchar2Type
    -1, //ObNChar
    -1, // ObURowID
    -1, //Lob
    -1, //Json
    -1, //Geometry
    -1, //ObUserDefinedSQLType
    -1 // ObMaxType
  };
  STATIC_ASSERT(ARRAYSIZEOF(type_size_map) == common::ObMaxType + 1,
      "type size map count mismatch with type count");
  return type_size_map;
}

// Store size in @estimate_base_store_size_map must be larger than or equal to real store size
OB_INLINE int64_t *get_estimate_base_store_size_map()
{
  static int64_t estimate_base_store_size_map[] = {
    8, // ObNullType
    8, // ObTinyIntType
    8, // ObSmallIntType
    8, // ObMediumIntType
    8, // ObInt32Type
    8, // ObIntType
    8, // ObUTinyIntType
    8, // ObUSmallIntType
    8, // ObUMediumIntType
    8, // ObUInt32Type
    8, // ObUInt64Type
    8, // ObFloatType
    8, // ObDoubleType
    8, // ObUFloatType
    8, // ObUDoubleType
    8, // ObNumberType
    8, // ObUNumberType
    8, // ObDateTimeType
    8, // ObTimestampType
    8, // ObDateType=19,
    8, // ObTimeType
    8, // ObYearType=21,
    8, // ObVarcharType=22
    8, // ObCharType=23
    8, // ObHexStringType
    8, // ObExtendType
    8, // ObUnknownType
    9, // ObTinyTextType
    9, // ObTextType
    9, // ObMediumTextType
    9, // ObLongTextType
    8, // ObBitType
    8, // ObEnumType
    8, // ObSetType
    8, //ObEnumInnerType
    8, //ObSetInnerType
    12, //ObTimestampTZType
    10, //ObTimestampLTZType
    10, //ObTimestampNanoType
    8, //ObRawType
    8, //ObIntervalYMType
    12, //ObIntervalDSType
    8, //ObNumberFloatType
    8, //ObNVarchar2Type
    8, //ObNChar
    8, // ObURowID
    8, //Lob
    9, // ObJsonType
    9, // ObGeometryType
    8, // ObUserDefinedSQLType
    -1 // ObMaxType
  };
  STATIC_ASSERT(ARRAYSIZEOF(estimate_base_store_size_map) == common::ObMaxType + 1,
      "Base store size map count mismatch with type count");
  return estimate_base_store_size_map;
}

extern uint64_t INTEGER_MASK_TABLE[sizeof(int64_t) + 1];

extern int64_t get_packing_size(
    bool &bit_packing, const uint64_t v, bool enable_bit_packing = true);
extern int64_t get_int_size(const uint64_t v);
extern int64_t get_byte_packed_int_size(const uint64_t v);

enum ObStoredExtValue
{
  STORED_NOT_EXT = 0,
  STORED_NULL = 1,
  STORED_NOPE = 2,
  STORED_EXT_MAX
};

OB_INLINE ObStoredExtValue get_stored_ext_value(const ObDatum &datum)
{
  ObStoredExtValue v = STORED_NOT_EXT;
  if (datum.is_null()) {
    v = STORED_NULL;
  } else if (datum.is_nop()) {
    v = STORED_NOPE;
  }
  return v;
}

// OB_INLINE void set_stored_ext_value(ObDatum &datum, const ObStoredExtValue v)
// {
//   if (STORED_NULL == v) {
//     datum.set_null();
//   } else if (STORED_NOPE == v) {
//     datum.set_ext_value(common::ObActionFlag::OP_NOP);
//   }
// }

OB_INLINE ObStoredExtValue get_stored_ext_value(const common::ObObj &cell)
{
  ObStoredExtValue v = STORED_NOT_EXT;
  if (cell.is_null()) {
    v = STORED_NULL;
  } else if (common::ObActionFlag::OP_NOP == cell.get_ext()) {
    v = STORED_NOPE;
  }
  return v;
}

OB_INLINE void set_stored_ext_value(common::ObObj &cell, const ObStoredExtValue v)
{
  if (STORED_NULL == v) {
    cell.set_null();
  } else if (STORED_NOPE == v) {
    cell.set_ext(common::ObActionFlag::OP_NOP);
  }
}

OB_INLINE static int get_uint_data_datum_len(
    const common::ObObjDatumMapType &datum_type,
    uint32_t &datum_len)
{
  int ret = common::OB_SUCCESS;
  switch (datum_type) {
  case common::OBJ_DATUM_1BYTE_DATA: {
    datum_len = sizeof(uint8_t);
    break;
  }
  case common::OBJ_DATUM_4BYTE_DATA: {
    datum_len = sizeof(uint32_t);
    break;
  }
  case common::OBJ_DATUM_8BYTE_DATA: {
    datum_len = sizeof(uint64_t);
    break;
  }
  default: {
    ret = common::OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Unexpected datum obj mapping type for uint data", K(datum_type));
  }
  }
  return ret;
}

OB_INLINE static int batch_load_number_data_to_datum(
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(cell_datas) || OB_ISNULL(datums)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), KP(cell_datas), KP(datums));
  } else {
    for (int64_t i = 0; i < row_cap; ++i) {
      if (!datums[i].is_null()) {
        MEMCPY(const_cast<char *>(datums[i].ptr_), cell_datas[i], sizeof(ObNumberDesc));
        const uint8_t num_len = datums[i].num_->desc_.len_;
        datums[i].len_ = sizeof(ObNumberDesc) + num_len * sizeof(uint32_t);
        if (OB_LIKELY(1 == num_len)) {
          MEMCPY(const_cast<char *>(datums[i].ptr_) + sizeof(ObNumberDesc),
              cell_datas[i] + sizeof(ObNumberDesc), sizeof(uint32_t));
        } else {
          ENCODING_ADAPT_MEMCPY(const_cast<char *>(datums[i].ptr_) + sizeof(ObNumberDesc),
              cell_datas[i] + sizeof(ObNumberDesc), num_len * sizeof(uint32_t));
        }
      } else {
        datums[i].set_null();
      }
    }
  }
  return ret;
}

OB_INLINE static int batch_load_ots_intvl_data_to_datum(
    const common::ObObjDatumMapType &datum_type,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(cell_datas) || OB_ISNULL(datums)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(cell_datas), KP(datums));
  } else {
    const uint32_t size = ObDatum::get_reserved_size(datum_type);
    for (int64_t i = 0; i < row_cap; ++i) {
      if (!datums[i].is_null()) {
        MEMCPY(const_cast<char *>(datums[i].ptr_), cell_datas[i], size);
        datums[i].len_ = size;
      } else {
        datums[i].set_null();
      }
    }
  }
  return ret;
}

// For StringSC, data length should be already stored in datums[i].len_
inline static int batch_load_data_to_datum(
    const common::ObObjType &obj_type,
    const char **cell_datas,
    const int64_t row_cap,
    const int64_t integer_mask,
    common::ObDatum *datums)
{
  int ret = common::OB_SUCCESS;
  switch (get_store_class_map()[ob_obj_type_class(obj_type)]) {
  case ObIntSC:
  case ObUIntSC: {
    uint32_t datum_len = 0;
    if (OB_FAIL(get_uint_data_datum_len(
        common::ObDatum::get_obj_datum_map_type(obj_type),
        datum_len))) {
      STORAGE_LOG(WARN, "Failed to get datum len for int data", K(ret));
    } else {
      uint64_t value = 0;
      for (int64_t i = 0; i < row_cap; ++i) {
        if (!datums[i].is_null()) {
          value = 0;
          ENCODING_ADAPT_MEMCPY(&value, cell_datas[i], datums[i].len_);
          if (0 != integer_mask && (value & (integer_mask >> 1))) {
            value |= integer_mask;
          }
          datums[i].len_ = datum_len;
          ENCODING_ADAPT_MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len);
        } else {
          datums[i].set_null();
        }
      }
    }
    break;
  }
  case ObNumberSC: {
    if (OB_FAIL(batch_load_number_data_to_datum(cell_datas, row_cap, datums))) {
      STORAGE_LOG(WARN, "Failed to load batch data to datum", K(ret));
    }
    break;
  }
  case ObStringSC:
  case ObTextSC:
  case ObJsonSC:
  case ObGeometrySC: {
    for (int64_t i = 0; i < row_cap; ++i) {
      if (!datums[i].is_null()) {
        datums[i].ptr_ = cell_datas[i];
      } else {
        datums[i].set_null();
      }
    }
    break;
  }
  case ObOTimestampSC:
  case ObIntervalSC: {
    if (OB_FAIL(batch_load_ots_intvl_data_to_datum(
        common::ObDatum::get_obj_datum_map_type(obj_type),
        cell_datas,
        row_cap,
        datums))) {
      STORAGE_LOG(WARN, "Failed to load batch ots/intvl data to datum", K(ret), K(obj_type));
    }
    break;
  }
  default: {
    ret = common::OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Unexpected store class", K(ret), K(obj_type));
  }
  }
  return ret;
}


OB_INLINE int64_t number_store_size(const common::ObObj &obj)
{
  return sizeof(obj.nmb_desc_) + obj.nmb_desc_.len_ * sizeof(obj.v_.nmb_digits_[0]);
}

enum ObFPIntCmpOpType
{
  FP_INT_OP_EQ = 0, // WHITE_OP_EQ
  FP_INT_OP_LE, // WHITE_OP_LE
  FP_INT_OP_LT, // WHITE_OP_LT
  FP_INT_OP_GE, // WHITE_OP_GE
  FP_INT_OP_GT, // WHITE_OP_GT
  FP_INT_OP_NE, // WHITE_OP_NE
  FP_INT_OP_MAX,
};

template <typename T>
OB_INLINE bool fp_int_cmp(const T left, const T right, const ObFPIntCmpOpType cmp_op)
{
  // not check parameter for performance
  bool res = false;
  switch (cmp_op) {
  case FP_INT_OP_EQ:
    res = left == right;
    break;
  case FP_INT_OP_LE:
    res = left <= right;
    break;
  case FP_INT_OP_LT:
    res = left < right;
    break;
  case FP_INT_OP_GE:
    res = left >= right;
    break;
  case FP_INT_OP_GT:
    res = left > right;
    break;
  case FP_INT_OP_NE:
    res = left != right;
    break;
  default:
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Not Supported compare operation type", K(cmp_op));
  }
  return res;
}

OB_INLINE ObFPIntCmpOpType *get_white_op_int_op_map()
{
  static ObFPIntCmpOpType white_op_int_op_map[] = {
    FP_INT_OP_EQ, // WHITE_OP_EQ
    FP_INT_OP_LE, // WHITE_OP_LE
    FP_INT_OP_LT, // WHITE_OP_LT
    FP_INT_OP_GE, // WHITE_OP_GE
    FP_INT_OP_GT, // WHITE_OP_GT
    FP_INT_OP_NE  // WHITE_OP_NE
  };
  STATIC_ASSERT(ARRAYSIZEOF(white_op_int_op_map) == FP_INT_OP_NE + 1,
    "type size map count mismatch with type count");
  return white_op_int_op_map;
}

// fast 2d array for POD
template <typename T, int64_t MAX_COUNT,
         int64_t BLOCK_SIZE>
class ObPodFix2dArray
{
private:
  const static int64_t BLOCK_ITEM_CNT = BLOCK_SIZE / sizeof(T);
  const static int64_t MAX_BLOCK_CNT = (MAX_COUNT + BLOCK_ITEM_CNT - 1) / BLOCK_ITEM_CNT;
public:
  ObPodFix2dArray() : size_(0) { MEMSET(block_list_, 0, sizeof(T *) * MAX_BLOCK_CNT); }
  ~ObPodFix2dArray() { destroy(); }

  OB_INLINE int64_t count() const { return size_; }
  OB_INLINE bool empty() const { return size_ <= 0; }
  OB_INLINE T &at(int64_t idx)
  {
    return block_list_[idx / BLOCK_ITEM_CNT][idx % BLOCK_ITEM_CNT];
  }
  OB_INLINE const T &at(int64_t idx) const
  {
    return block_list_[idx / BLOCK_ITEM_CNT][idx % BLOCK_ITEM_CNT];
  }
  OB_INLINE int push_back(const T &v)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(size_ + 1 > MAX_COUNT)) {
      ret = common::OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "size will overflow", K(ret), K_(size));
    } else {
      const int64_t pos = size_ / BLOCK_ITEM_CNT;
      if (OB_UNLIKELY(NULL == block_list_[pos])) {
        if (OB_FAIL(extend(1))) {
          STORAGE_LOG(WARN, "extend block failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // new (&block_list_[pos][size_ % BLOCK_ITEM_CNT]) T(v);
        MEMCPY(&block_list_[pos][size_ % BLOCK_ITEM_CNT], &v, sizeof(v));
        ++size_;
      }
    }
    return ret;
  }
  void reuse() { size_ = 0; }
  void destroy()
  {
    reuse();
    for (int64_t i = 0; i < MAX_BLOCK_CNT; ++i) {
      if (NULL == block_list_[i]) {
        break;
      } else {
        common::ob_free(block_list_[i]);
        block_list_[i] = NULL;
      }
    }
  }
  int resize(const int64_t size) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(reserve(size))) {
      STORAGE_LOG(WARN, "reserve failed", K(ret));
    } else {
      size_ = size;
    }
    return ret;
  }
  int reserve(const int64_t size) {
    int ret = common::OB_SUCCESS;
    if (size > size_) {
      int64_t total_block_cnt = (size + BLOCK_ITEM_CNT - 1) / BLOCK_ITEM_CNT;
      int64_t cur_blocks_cnt = (size_ + BLOCK_ITEM_CNT - 1) / BLOCK_ITEM_CNT;
      if (total_block_cnt > cur_blocks_cnt) {
        if (OB_FAIL(extend(total_block_cnt - cur_blocks_cnt))) {
          STORAGE_LOG(WARN, "extend failed", K(ret), K(cur_blocks_cnt), K(total_block_cnt));
        }
      }
    }
    return ret;
  }
  TO_STRING_KV(K_(size));
private:
  int extend(const int64_t block_cnt)
  {
    int ret = common::OB_SUCCESS;
    const int64_t cur_cnt = (size_ + BLOCK_ITEM_CNT - 1) / BLOCK_ITEM_CNT
        + ((0 == size_) ? 0 : 1);
    if (cur_cnt + block_cnt > MAX_BLOCK_CNT) {
      ret = common::OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "size will overflow", K(ret), K_(size), K(block_cnt), K(cur_cnt));
    } else {
      common::ObMemAttr ma(MTL_ID(), blocksstable::OB_ENCODING_LABEL_PIVOT);
      for (int64_t i = 0; i < block_cnt && OB_SUCC(ret); ++i) {
        if (NULL == block_list_[cur_cnt + i]) {
          T *block = static_cast<T *>(common::ob_malloc(BLOCK_SIZE, ma));
          if (NULL == block) {
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "alloc block failed",
                K(ret), "block_size", static_cast<int64_t>(BLOCK_SIZE));
          } else {
            block_list_[cur_cnt + i] = block;
          }
        }
      }
    }
    return ret;
  }
private:
  T *block_list_[MAX_BLOCK_CNT];
  int64_t size_;
};

typedef ObPodFix2dArray<common::ObObj, 64 << 10, common::OB_MALLOC_MIDDLE_BLOCK_SIZE> ObColValues;
typedef ObPodFix2dArray<ObDatum, 64 << 10, common::OB_MALLOC_MIDDLE_BLOCK_SIZE> ObColDatums;

class ObMapAttrOperator
{
public:
  enum ObMapAttr
  {
    ROW_ID_ONE_BYTE = 0x1,
    ROW_ID_TWO_BYTE = 0x2,
    REF_ONE_BYTE = 0x4,
    REF_TWO_BYTE = 0x8
  };

  static int set_row_id_byte(const int64_t byte, int8_t &attr);
  static int set_ref_byte(const int64_t byte, int8_t &attr);
  static int get_row_id_byte(const int8_t attr, int64_t &byte);
  static int get_ref_byte(const int8_t attr, int64_t &byte);
};

class ObEncodingRowBufHolder
{
public:
  ObEncodingRowBufHolder(
      const lib::ObLabel &label = blocksstable::OB_ENCODING_LABEL_ROW_BUFFER,
      const int64_t page_size = common::OB_MALLOC_MIDDLE_BLOCK_SIZE)
    : allocator_(label, page_size), buf_size_limit_(0), alloc_size_(0), alloc_buf_(nullptr) {}
  virtual ~ObEncodingRowBufHolder() {}
  int init(const int64_t macro_block_size, const int64_t tenant_id = OB_SERVER_TENANT_ID);
  void reset();
  // try to re-alloc held memory buffer
  int try_alloc(const int64_t required_size);
  OB_INLINE char *get_buf() { return alloc_buf_; }
private:
  static constexpr int64_t EXTRA_MEM_FACTOR = 2;
  common::ObArenaAllocator allocator_;
  int64_t buf_size_limit_;
  int64_t alloc_size_;
  char *alloc_buf_;
  bool is_inited_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ENCODING_UTIL_H_
