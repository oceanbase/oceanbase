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

#ifndef OCEANBASE_OB_ARRAY_TYPE_
#define OCEANBASE_OB_ARRAY_TYPE_
#include <stdint.h>
#include <string.h>
#include "lib/string/ob_string.h"
#include "lib/container/ob_vector.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/hash/ob_hashset.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_fast_convert.h"
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "src/share/datum/ob_datum.h"

namespace oceanbase {
namespace common {

static constexpr int64_t MAX_ARRAY_SIZE = (1 << 20) * 16; // 16M
static constexpr int64_t MAX_ARRAY_ELEMENT_SIZE = 2000000;
enum ArrayAttr {
  ATTR_LENGTH = 0,
  ATTR_NULL_BITMAP = 1,
  ATTR_OFFSETS = 2,
  ATTR_DATA = 3,
};

struct ObArrayAttr {
  const char *ptr_;
  uint32_t length_;
};

OB_INLINE bool ob_is_array_supported_type(ObObjType type)
{
  return ObUNumberType >= type  || ObVarcharType == type || ObDecimalIntType == type;
}

template<typename T>
class ObArrayData {
public :
    ObArrayData(ObIAllocator &allocator)
    : raw_data_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ARRAYModule")),
      null_bitmaps_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ARRAYModule")),
      offsets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ARRAYModule")) {}

  using Container = common::ObArray<T, ModulePageAllocator, false>;
  using NullContainer = common::ObArray<uint8_t, ModulePageAllocator, false>;
  using OffsetContainer = common::ObArray<uint32_t, ModulePageAllocator, false>;
  inline size_t data_length() { return raw_data_.size() * sizeof(T); }
  inline size_t nullbitmaps_length() { return null_bitmaps_.size() * sizeof(uint8_t); }
  inline size_t offsets_length() { return offsets_.size() * sizeof(uint32_t); }
  inline uint32_t offset_at(uint32_t idx) { return idx == 0 ? 0 : offsets_[idx - 1]; }
  void clear() { raw_data_.reset(); null_bitmaps_.reset(); offsets_.reset(); }

  Container raw_data_;
  NullContainer null_bitmaps_;
  OffsetContainer offsets_;
};

enum ArrayFormat {
  Fixed_Size = 0,
  Vector = 1,
  Binary_Varlen = 2,
  Nested_Array = 3,
  Array_MAX_FORMAT
};

class ObIArrayType {
public:
  virtual int print(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0, bool print_whole = true) const = 0;
  virtual int print_element(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0, bool print_whole = true,
                            ObString delimiter = ObString(","), bool has_null_str = true, ObString null_str = ObString("NULL")) const = 0;
  virtual int32_t get_raw_binary_len() = 0;
  virtual int get_raw_binary(char *res_buf, int64_t buf_len) = 0;
  // without length_
  virtual int32_t get_data_binary_len() = 0;
  virtual int get_data_binary(char *res_buf, int64_t buf_len) = 0;
  virtual int init(ObString &raw_data) = 0;
  virtual int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true) = 0;
  virtual int init() = 0; // init array with self data_container
  virtual void set_scale(ObScale scale) = 0;  // only for decimalint array
  virtual ArrayFormat get_format() const = 0;
  virtual bool is_nested_array() const = 0;
  virtual uint32_t size() const = 0;
  virtual uint32_t cardinality() const = 0;
  virtual int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const = 0;
  virtual bool is_null(uint32_t idx) const = 0; // check if the idx-th element is null or not, idx validity is guaranteed by caller
  virtual int push_null() = 0;
  virtual bool contain_null() const = 0;
  virtual int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len = 1) = 0;
  int insert_from(const ObIArrayType &src) { return insert_from(src, 0, src.size()); }
  virtual int insert_elem_from(const ObIArrayType &src, uint32_t idx) = 0;
  virtual int32_t get_element_type() const = 0;
  virtual const ObCollectionArrayType *get_array_type() const = 0;
  virtual char *get_data() const = 0;
  virtual uint32_t *get_offsets() const = 0;
  virtual uint8_t *get_nullbitmap() const = 0;
  virtual void set_element_type(int32_t type) = 0;
  virtual void set_array_type(const ObCollectionArrayType *array_type) = 0;
  virtual int elem_at(uint32_t idx, ObObj &elem_obj) const = 0;
  virtual int at(uint32_t idx, ObIArrayType &dest) const = 0;
  virtual void clear() = 0;
  virtual int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx) = 0;
  virtual int set_null_bitmaps(uint8_t *nulls, int64_t length) = 0;
  virtual int set_offsets(uint32_t *offsets, int64_t length) = 0;
  virtual int compare(const ObIArrayType &right, int &cmp_ret) const = 0;
  virtual int compare_at(uint32_t left_begin, uint32_t left_len,
                         uint32_t right_begin, uint32_t right_len,
                         const ObIArrayType &right, int &cmp_ret) const = 0;
  virtual bool sort_cmp(uint32_t idx_l, uint32_t idx_r) const = 0;
  virtual int contains_all(const ObIArrayType &other, bool &bret) const = 0;
  virtual int overlaps(const ObIArrayType &other, bool &bret) const = 0;
  virtual int hash(uint64_t &hash_val) const = 0;
  virtual bool operator ==(const ObIArrayType &other) const = 0;
  virtual int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const = 0;
  virtual int distinct(ObIAllocator &alloc, ObIArrayType *&output) const = 0;
};

template<typename T>
class ObArrayBase : public ObIArrayType {
public :
  ObArrayBase() : length_(0), element_type_(0), null_bitmaps_(nullptr), data_container_(nullptr), array_type_(nullptr) {}
  ObArrayBase(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps)
    : length_(length), element_type_(elem_type), null_bitmaps_(null_bitmaps), data_container_(nullptr), array_type_(nullptr) {}

  uint32_t size() const { return length_; }
  bool contain_null() const
  {
    bool bret = false;
    for (int64_t i = 0; null_bitmaps_ != nullptr && !bret && i < length_; ++i) {
      if (null_bitmaps_[i] > 0) {
        bret = true;
      }
    }
    return bret;
  }
  virtual ArrayFormat get_format() const = 0;
  virtual bool is_nested_array() const { return get_format() == Nested_Array; }
  int32_t get_element_type() const { return element_type_; }
  const ObCollectionArrayType *get_array_type() const { return array_type_; }
  void set_element_type(int32_t type) { element_type_ = type; }
  void set_array_type(const ObCollectionArrayType *array_type) { array_type_ = array_type; }
  uint8_t *get_nullbitmap() const { return null_bitmaps_;}
  // make sure idx is less than length_
  bool is_null(uint32_t idx) const { return OB_ISNULL(null_bitmaps_) ? false : null_bitmaps_[idx] > 0; }
  // make sure offsets isn't nullptr and idx is less than length
  uint32_t offset_at(uint32_t idx, uint32_t *offsets) const { return idx == 0 ? 0 : offsets[idx - 1]; }
  inline void set_array_data(ObArrayData<T> *arr_data) { data_container_ = arr_data;}
  int set_null_bitmaps(uint8_t *nulls, int64_t length)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_container_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "try to modify read-only array", K(ret));
    } else {
      int64_t curr_pos = data_container_->null_bitmaps_.size();
      int64_t capacity = curr_pos + length;
      if (OB_FAIL(data_container_->null_bitmaps_.prepare_allocate(capacity))) {
        OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
      } else {
        uint8_t *cur_null_bitmap = data_container_->null_bitmaps_.get_data() + curr_pos;
        MEMCPY(cur_null_bitmap, nulls, length * sizeof(uint8_t));
      }
    }
    return ret;
  }
  int set_offsets(uint32_t *offsets, int64_t length)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_container_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "try to modify read-only array", K(ret));
    } else {
      int64_t curr_pos = data_container_->offsets_.size();
      int64_t capacity = curr_pos + length;
      if (OB_FAIL(data_container_->offsets_.prepare_allocate(capacity))) {
        OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
      } else {
        char *cur_offsets =  reinterpret_cast<char *>(data_container_->offsets_.get_data() + curr_pos * sizeof(uint32_t));
        MEMCPY(cur_offsets, offsets, length * sizeof(uint32_t));
      }
    }
    return ret;
  }
  int get_reserved_data(int64_t length, T *&data)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_container_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "try to modify read-only array", K(ret));
    } else {
      int64_t curr_pos = data_container_->raw_data_.size();
      int64_t capacity = curr_pos + length;
      if (OB_FAIL(data_container_->raw_data_.prepare_allocate(capacity))) {
        OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
      } else {
        data = reinterpret_cast<T *>(data_container_->raw_data_.get_data() + curr_pos);
      }
    }
    return ret;
  }
  int insert_elem_from(const ObIArrayType &src, uint32_t idx) { return insert_from(src, idx, 1); }

  bool operator ==(const ObIArrayType &other) const
  {
    bool b_ret = false;
    int ret = OB_SUCCESS;
    int cmp_ret = 0;
    if (OB_SUCC(compare(other, cmp_ret))) {
      b_ret = (cmp_ret == 0);
    }
    return b_ret;
  }

protected :
  uint32_t length_;
  int32_t element_type_;
  uint8_t *null_bitmaps_;
  ObArrayData<T> *data_container_;
  const ObCollectionArrayType *array_type_;
};

class ObArrayTypeObjFactory
{
public:
  ObArrayTypeObjFactory() {};
  virtual ~ObArrayTypeObjFactory() {};
  static int construct(common::ObIAllocator &alloc, const ObCollectionTypeBase  &array_meta, ObIArrayType *&arr_obj, bool read_only = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObArrayTypeObjFactory);
};
} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_TYPE_
