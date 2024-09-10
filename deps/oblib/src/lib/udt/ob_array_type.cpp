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

#define USING_LOG_PREFIX LIB
#include "ob_array_type.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {

#define CONSTRUCT_FIXED_ARRAY_OBJ(Element_Type)                                              \
  void *buf = alloc.alloc(sizeof(ObArrayFixedSize<Element_Type>));                           \
  if (OB_ISNULL(buf)) {                                                                      \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
    OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_));   \
  } else {                                                                                   \
    ObArrayFixedSize<Element_Type> *arr_ptr = new (buf) ObArrayFixedSize<Element_Type>();    \
    if (read_only) {                                                                         \
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<Element_Type>)))) {            \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                       \
      OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_)); \
    } else {                                                                                 \
      ObArrayData<Element_Type> *arr_data = new (buf) ObArrayData<Element_Type>(alloc);      \
      arr_ptr->set_array_data(arr_data);                                                     \
    }                                                                                        \
    if (OB_SUCC(ret)) {                                                                      \
      arr_obj = arr_ptr;                                                                     \
    }                                                                                        \
  }

#define CONSTRUCT_ARRAY_OBJ(Array_Type, Element_Type)                                        \
  void *buf = alloc.alloc(sizeof(Array_Type));                                               \
  if (OB_ISNULL(buf)) {                                                                      \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
    OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_));   \
  } else {                                                                                   \
    Array_Type *arr_ptr = new (buf) Array_Type();                                            \
    if (read_only) {                                                                         \
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<Element_Type>)))) {            \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                       \
      OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_)); \
    } else {                                                                                 \
      ObArrayData<Element_Type> *arr_data = new (buf) ObArrayData<Element_Type>(alloc);      \
      arr_ptr->set_array_data(arr_data);                                                     \
    }                                                                                        \
    if (OB_SUCC(ret)) {                                                                      \
      arr_obj = arr_ptr;                                                                     \
    }                                                                                        \
  }

int ObArrayTypeObjFactory::construct(common::ObIAllocator &alloc, const ObCollectionTypeBase &array_meta,
                                     ObIArrayType *&arr_obj, bool read_only)
{
  int ret = OB_SUCCESS;
  if (array_meta.type_id_ == ObNestedType::OB_ARRAY_TYPE) {
    const ObCollectionArrayType *arr_type = static_cast<const ObCollectionArrayType *>(&array_meta);
    if (arr_type->element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
      switch (elem_type->basic_meta_.get_obj_type()) {
        case ObNullType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int8_t);
          break;
        }
        case ObTinyIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int8_t);
          break;
        }
        case ObSmallIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int16_t);
          break;
        }
        case ObInt32Type: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int32_t);
          break;
        }
        case ObIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int64_t);
          break;
        }
        case ObUTinyIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint8_t);
          break;
        }
        case ObUSmallIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint16_t);
          break;
        }
        case ObUInt32Type: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint32_t);
          break;
        }
        case ObUInt64Type: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint64_t);
          break;
        }
        case ObFloatType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(float);
          break;
        }
        case ObDoubleType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(double);
          break;
        }
        case ObDecimalIntType: {
          ObPrecision preci = elem_type->basic_meta_.get_precision();
          if (get_decimalint_type(preci) == DECIMAL_INT_32) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int32_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_64) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int64_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_128) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int128_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_256) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int256_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_512) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int512_t);
          } else {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "unexpected precision", K(ret), K(preci));
          }
          if (OB_SUCC(ret)) {
            arr_obj->set_scale(elem_type->basic_meta_.get_scale());
          }
          break;
        }
        case ObVarcharType : {
          CONSTRUCT_ARRAY_OBJ(ObArrayBinary, char);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          OB_LOG(WARN, "unsupported type", K(ret), K(elem_type->basic_meta_.get_obj_type()));
        }
      }
      if (OB_SUCC(ret)) {
        arr_obj->set_element_type(static_cast<int32_t>(elem_type->basic_meta_.get_obj_type()));
      }
    } else if (array_meta.type_id_ == ObNestedType::OB_ARRAY_TYPE) {
      CONSTRUCT_ARRAY_OBJ(ObArrayNested, char);
      ObIArrayType *arr_child = NULL;
      if (FAILEDx(construct(alloc, *arr_type->element_type_, arr_child, read_only))) {
        OB_LOG(WARN, "failed to construct child element", K(ret), K(array_meta.type_id_));
      } else {
        arr_obj->set_element_type(static_cast<int32_t>(ObCollectionSQLType));
        ObArrayNested *nested_arr = static_cast<ObArrayNested *>(arr_obj);
        nested_arr->set_child_array(arr_child);
      }
    }
  } else if (array_meta.type_id_ == ObNestedType::OB_VECTOR_TYPE) {
    CONSTRUCT_ARRAY_OBJ(ObVectorData, float);
    if (OB_SUCC(ret)) {
      arr_obj->set_element_type(static_cast<int32_t>(ObFloatType));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected collect info type", K(ret), K(array_meta.type_id_));
  }
  return ret;
}

int ObArrayUtil::get_type_name(const ObDataType &elem_type, char *buf, int buf_len, uint32_t depth)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (uint32_t i = 0; OB_SUCC(ret) && i < depth; i++) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ARRAY("))) {
      LOG_WARN("failed to convert len to string", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ob_sql_type_str(elem_type.get_obj_type())))) {
    LOG_WARN("failed to convert len to string", K(ret));
  } else if (elem_type.get_obj_type() == ObDecimalIntType
             && OB_FAIL(databuff_printf(buf, buf_len, pos, "(%d,%d)", elem_type.get_precision(), elem_type.get_scale()))) {
    LOG_WARN("failed to add deciaml precision to string", K(ret));
  } else if (ob_is_string_tc(elem_type.get_obj_type())
             && OB_FAIL(databuff_printf(buf, buf_len, pos, "(%d)", elem_type.get_length()))) {
    LOG_WARN("failed to add string len to string", K(ret));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < depth; i++) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("failed to add ) to string", K(ret));
    }
  }
  return ret;
}

int ObArrayUtil::push_back_decimal_int(const ObPrecision prec, const ObDecimalInt *dec_val, bool is_null, ObIArrayType *arr_obj)
{
  int ret = OB_SUCCESS;
  if (get_decimalint_type(prec) == DECIMAL_INT_32) {
    ObArrayFixedSize<int32_t> *arr = static_cast<ObArrayFixedSize<int32_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int32_v_[0]))) {
      LOG_WARN("failed to push back decimal int32 value", K(ret), K(dec_val->int32_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_64) {
    ObArrayFixedSize<int64_t> *arr = static_cast<ObArrayFixedSize<int64_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int64_v_[0]))) {
      LOG_WARN("failed to push back decimal int64 value", K(ret), K(dec_val->int64_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_128) {
    ObArrayFixedSize<int128_t> *arr = static_cast<ObArrayFixedSize<int128_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int128_v_[0]))) {
      LOG_WARN("failed to push back decimal int128 value", K(ret), K(dec_val->int128_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_256) {
    ObArrayFixedSize<int256_t> *arr = static_cast<ObArrayFixedSize<int256_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int256_v_[0]))) {
      LOG_WARN("failed to push back decimal int256 value", K(ret), K(dec_val->int256_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_512) {
    ObArrayFixedSize<int512_t> *arr = static_cast<ObArrayFixedSize<int512_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int512_v_[0]))) {
      LOG_WARN("failed to push back decimal int512 value", K(ret), K(dec_val->int512_v_[0]));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected precision", K(ret), K(prec));
  }
  return ret;
}

// convert collection bin to string (for liboblog)
int ObArrayUtil::convert_collection_bin_to_string(const ObString &collection_bin,
                                                  const common::ObIArray<common::ObString> &extended_type_info,
                                                  common::ObIAllocator &allocator,
                                                  ObString &res_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(extended_type_info.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid extended type info for collection type", K(ret), K(extended_type_info.count()));
  } else {
    ObSqlCollectionInfo type_info_parse(allocator);
    ObString collection_type_name = extended_type_info.at(0);
    type_info_parse.set_name(collection_type_name);
    if (OB_FAIL(type_info_parse.parse_type_info())) {
      LOG_WARN("fail to parse type info", K(ret), K(collection_type_name));
    } else if (OB_ISNULL(type_info_parse.collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection meta is null", K(ret), K(collection_type_name));
    } else {
      ObCollectionArrayType *arr_type = nullptr;
      ObIArrayType *arr_obj = nullptr;
      ObStringBuffer buf(&allocator);
      if (OB_ISNULL(arr_type = static_cast<ObCollectionArrayType *>(type_info_parse.collection_meta_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("collection meta is null", K(ret), K(collection_type_name));
      } else if (OB_FAIL(ObArrayTypeObjFactory::construct(allocator, *arr_type, arr_obj, true))) {
        LOG_WARN("construct array obj failed", K(ret),  K(type_info_parse));
      } else if (OB_ISNULL(arr_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arr_obj is null", K(ret), K(collection_type_name));
      } else {
        ObString raw_binary = collection_bin;
        if (OB_FAIL(arr_obj->init(raw_binary))) {
          LOG_WARN("failed to init array", K(ret));
        } else if (OB_FAIL(arr_obj->print(arr_type->element_type_, buf))) {
          LOG_WARN("failed to format array", K(ret));
        } else {
          res_str.assign_ptr(buf.ptr(), buf.length());
        }
      }
    }
  }
  return ret;
}

// determine a collection type is vector or array
int ObArrayUtil::get_mysql_type(const common::ObIArray<common::ObString> &extended_type_info,
                                obmysql::EMySQLFieldType &type)
{
  int ret = OB_SUCCESS;
  type = obmysql::MYSQL_TYPE_NOT_DEFINED;
  ObArenaAllocator tmp_allocator("OB_ARRAY_UTIL");
  if (OB_UNLIKELY(extended_type_info.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid extended type info for collection type", K(ret), K(extended_type_info.count()));
  } else {
    ObSqlCollectionInfo type_info_parse(tmp_allocator);
    ObString collection_type_name = extended_type_info.at(0);
    type_info_parse.set_name(collection_type_name);
    if (OB_FAIL(type_info_parse.parse_type_info())) {
      LOG_WARN("fail to parse type info", K(ret), K(collection_type_name));
    } else if (OB_ISNULL(type_info_parse.collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection meta is null", K(ret), K(collection_type_name));
    } else {
      uint16_t detail_type = type_info_parse.collection_meta_->type_id_;
      if (detail_type == OB_ARRAY_TYPE) {
        type = obmysql::MYSQL_TYPE_OB_ARRAY;
      } else if (detail_type == OB_VECTOR_TYPE) {
        type = obmysql::MYSQL_TYPE_OB_VECTOR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected collection type", K(ret), K(detail_type));
      }
    }
  }
  tmp_allocator.reset();
  return ret;
}

int ObVectorData::push_back(float value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (OB_FAIL(data_container_->raw_data_.push_back(value))) {
    OB_LOG(WARN, "failed to push value to array data", K(ret));
  } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "vector data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
  } else {
    length_++;
  }
  return ret;
}

int ObVectorData::print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str, uint32_t begin, uint32_t print_size) const
{
  int ret = OB_SUCCESS;
  UNUSED(elem_type);
  if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else {
    if (print_size == 0) {
      // print whole array
      print_size = length_;
    }
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (i > begin && OB_FAIL(format_str.append(","))) {
        OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
      } else {
        int buf_size = FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE;
        if (OB_FAIL(format_str.reserve(buf_size + 1))) {
          OB_LOG(WARN, "fail to reserve memory for format_str", K(ret));
        } else {
          char *start = format_str.ptr() + format_str.length();
          uint64_t len = ob_gcvt(data_[i], ob_gcvt_arg_type::OB_GCVT_ARG_FLOAT, buf_size, start, NULL);
          if (OB_FAIL(format_str.set_length(format_str.length() + len))) {
            OB_LOG(WARN, "fail to set format_str len", K(ret), K(format_str.length()), K(len));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

int ObVectorData::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (get_raw_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (data_container_ == NULL) {
    MEMCPY(res_buf, reinterpret_cast<char *>(data_), sizeof(float) * length_);
  } else {
    MEMCPY(res_buf,
        reinterpret_cast<char *>(data_container_->raw_data_.get_data()),
        sizeof(float) * data_container_->raw_data_.size());
  }
  return ret;
}

int ObVectorData::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    length_ = data_container_->raw_data_.size();
    data_ = data_container_->raw_data_.get_data();
  }
  return ret;
}

int ObVectorData::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() % sizeof(float) != 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    length_ = raw_data.length() / sizeof(float);
    data_ = reinterpret_cast<float *>(raw_str);
  }
  return ret;
}

int ObVectorData::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  // attrs of vector are same as array now, maybe optimize later
  const uint32_t count = with_length ? 3 : 2;
  if (attr_count != count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    data_ = const_cast<float *>(reinterpret_cast<const float *>(attrs[count - 1].get_string().ptr()));
    length_ = attrs[count - 1].get_int_bytes() / sizeof(float);
  }
  return ret;
}

int ObVectorData::check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const
{
  int ret = OB_SUCCESS;
  if (arr_type.dim_cnt_ != array.size()) {
    ret = OB_ERR_INVALID_VECTOR_DIM;
    LOG_WARN("invalid vector dimension", K(ret), K(arr_type.dim_cnt_), K(array.size()));
  }
  return ret;
}

int ObVectorData::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
{
int ret = OB_SUCCESS;
  if (src.get_format() != get_format()
      || src.get_element_type() != element_type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(get_format()), K(element_type_));
  } else if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    const uint32_t src_data_offset = begin * sizeof(float);
    int64_t curr_pos = data_container_->raw_data_.size();
    int64_t capacity = curr_pos + len;
    data_container_->raw_data_.prepare_allocate(capacity);
    char *cur_data = reinterpret_cast<char *>(data_container_->raw_data_.get_data() + curr_pos);
    MEMCPY(cur_data, src.get_data() + src_data_offset, len * sizeof(float));
    length_ += len;
  }
  return ret;
}

void ObVectorData::clear()
{
  data_ = nullptr;
  length_ = 0;
  if (OB_NOT_NULL(data_container_)) {
    data_container_->clear();
  }
}

int ObVectorData::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 2;
  if (len + attr_idx >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = nullptr;
    attrs[attr_idx].length_ = 0;
    attr_idx++; // skip null
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(data_);
    attrs[attr_idx].length_ = sizeof(float) * length_;
    attr_idx++;
  }
  return ret;
}

int ObVectorData::compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                             const ObIArrayType &right, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  const ObVectorData *right_data = dynamic_cast<const ObVectorData *>(&right);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
  } else {
    uint32_t cmp_len = std::min(left_len, right_len);
    cmp_ret = 0;
    for (uint32_t i = 0; i < cmp_len && !cmp_ret; ++i) {
      if (data_[left_begin + i] != (*right_data)[right_begin + i]) {
        cmp_ret = data_[left_begin + i] > (*right_data)[right_begin + i] ? 1 : -1;
      }
    }
    if (cmp_ret == 0 && left_len != right_len) {
      cmp_ret = left_len > right_len ? 1 : -1;
    }
  }
  return ret;
}

int ObVectorData::compare(const ObIArrayType &right, int &cmp_ret)
{
  return compare_at(0, this->length_, 0, right.size(), right, cmp_ret);
}

int ObArrayBinary::push_back(const ObString &value, bool is_null)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else {
    uint32_t last_offset =  data_container_->raw_data_.size();
    if (is_null) {
      // push back null
      if (OB_FAIL(push_null())) {
        OB_LOG(WARN, "failed to push null", K(ret));
      }
    } else if (OB_FAIL(data_container_->offsets_.push_back(last_offset + value.length()))) {
        OB_LOG(WARN, "failed to push value to array data", K(ret));
    } else if (OB_FAIL(data_container_->null_bitmaps_.push_back(0))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else {
      for (uint32_t i = 0; i < value.length() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(data_container_->raw_data_.push_back(value[i]))) {
          OB_LOG(WARN, "failed to push value to array data", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
      } else {
        length_++;
      }
    }
  }
  return ret;
}

int ObArrayBinary::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
{
  int ret = OB_SUCCESS;
  if (src.get_format() != get_format()
      || src.get_element_type() != element_type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(get_format()), K(element_type_));
  } else if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    // insert data
    const uint32_t src_offset = offset_at(begin, src.get_offsets());
    uint32_t src_len = src.get_offsets()[begin + len - 1] - src_offset;
    int64_t curr_pos = data_container_->raw_data_.size();
    int64_t capacity = curr_pos + src_len;
    data_container_->raw_data_.prepare_allocate(capacity);
    char *cur_data = data_container_->raw_data_.get_data() + curr_pos;
    MEMCPY(cur_data, src.get_data() + src_offset, src_len);
    // insert offsets
    uint32_t last_offset = src_offset;
    uint32_t pre_max_offset = data_container_->offset_at(length_);
    for (uint32_t i = 0; i < len && OB_SUCC(ret); ++i) {
      if (OB_FAIL(data_container_->offsets_.push_back(pre_max_offset + src.get_offsets()[begin + i] - last_offset))) {
        OB_LOG(WARN, "failed to push value to array data", K(ret));
      } else {
        last_offset = src.get_offsets()[begin + i];
        pre_max_offset = data_container_->offset_at(data_container_->offsets_.size());
      }
    }
    // insert nullbitmaps
    for (uint32_t i = 0; i < len && OB_SUCC(ret); ++i) {
      if (OB_FAIL(data_container_->null_bitmaps_.push_back(src.get_nullbitmap()[begin + i]))) {
        OB_LOG(WARN, "failed to push null", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      length_ += len;
    }
  }
  return ret;
}

ObString ObArrayBinary::operator[](const int64_t i) const
{
  ObString str;
  uint32_t last_offset = offset_at(i, offsets_);
  if (i >= 0 && i < length_) {
    uint32_t offset = offsets_[i];
    str.assign_ptr(&data_[last_offset], offset - last_offset);
  }
  return str;
}

int ObArrayBinary::get_data_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (data_container_ == NULL) {
    uint32_t last_idx = length_ > 0 ? length_ - 1 : 0;
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(null_bitmaps_), sizeof(uint8_t) * length_);
    pos += sizeof(uint8_t) * length_;
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(offsets_), sizeof(uint32_t) * length_);
    pos += sizeof(uint32_t) * length_;
    MEMCPY(res_buf + pos, data_, offsets_[last_idx]);
  } else {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->null_bitmaps_.get_data()), sizeof(uint8_t) * data_container_->null_bitmaps_.size());
    pos += sizeof(uint8_t) * data_container_->null_bitmaps_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->offsets_.get_data()), sizeof(uint32_t) * data_container_->offsets_.size());
    pos += sizeof(uint32_t) * data_container_->offsets_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->raw_data_.get_data()), data_container_->raw_data_.size());
  }
  return ret;
}

int ObArrayBinary::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (get_raw_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else {
    int64_t pos = 0;
    MEMCPY(res_buf + pos, &length_, sizeof(length_));
    pos += sizeof(length_);
    if (OB_FAIL(get_data_binary(res_buf + pos,  buf_len - pos))) {
      OB_LOG(WARN, "get data binary failed", K(ret), K(buf_len));
    }
  }
  return ret;
}

int ObArrayBinary::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    length_ = data_container_->offsets_.size();
    offsets_ = data_container_->offsets_.get_data();
    null_bitmaps_ = data_container_->null_bitmaps_.get_data();
    data_ = data_container_->raw_data_.get_data();
  }
  return ret;
}

int ObArrayBinary::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() < sizeof(length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    length_ = *reinterpret_cast<uint32_t *>(raw_str);
    if (length_ > 0) {
      pos += sizeof(length_);
      // init null bitmap
      null_bitmaps_ = reinterpret_cast<uint8_t *>(raw_str + pos);
      if (pos + sizeof(uint8_t) * length_ > raw_data.length()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
      } else {
        pos += sizeof(uint8_t) * length_;
        if (pos + sizeof(uint32_t) * length_ > raw_data.length()) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
        } else {
          // init offset
          offsets_ = reinterpret_cast<uint32_t *>(raw_str + pos);
          pos += sizeof(uint32_t) * length_;
          // init data
          data_ = reinterpret_cast<char *>(raw_str + pos);
          // last offset should be equal to data_ length
          if (offsets_[length_ - 1] != raw_data.length() - pos) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
          }
        }
      }
    }
  }
  return ret;
}

int ObArrayBinary::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  const uint32_t count = with_length ? 4 : 3;
  if (attr_count != count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    uint32_t idx = 0;
    if (with_length) {
      length_ = attrs[idx++].get_uint32();
    }  else {
      length_ = attrs[0].get_int_bytes() / sizeof(uint8_t);
    }
    null_bitmaps_ = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(attrs[idx++].get_string().ptr()));
    offsets_ = const_cast<uint32_t *>(reinterpret_cast<const uint32_t *>(attrs[idx++].get_string().ptr()));
    data_ = const_cast<char *>(reinterpret_cast<const char *>(attrs[idx++].get_string().ptr()));
    if ((with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint8_t) || length_ != attrs[2].get_int_bytes() / sizeof(uint32_t)))
        || (!with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint32_t)))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected attrs", K(ret), K(with_length), K(length_));
    }
  }
  return ret;
}

int ObArrayBinary::push_null()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else {
    uint32_t last_offset =  data_container_->raw_data_.size();
    if (OB_FAIL(data_container_->null_bitmaps_.push_back(1))) {
      // push back null
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_container_->offsets_.push_back(last_offset))) {
      OB_LOG(WARN, "failed to push value to array data", K(ret));
    } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
    } else {
      length_++;
    }
  }
  return ret;
}

int ObArrayBinary::print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str, uint32_t begin, uint32_t print_size) const
{
  int ret = OB_SUCCESS;
  UNUSED(elem_type);
  if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else {
    if (print_size == 0) {
      // print whole array
      print_size = length_;
    }
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (i > begin && OB_FAIL(format_str.append(","))) {
        OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
      } else if (null_bitmaps_[i]) {
          // value is null
          if (OB_FAIL(format_str.append("NULL"))) {
            OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
          }
      } else if (OB_FAIL(format_str.append("\""))) {
        OB_LOG(WARN, "fail to append \"\"\" to buffer", K(ret));
      } else if (OB_FAIL(format_str.append((*this)[i]))) {
        OB_LOG(WARN, "fail to append string to format_str", K(ret));
      } else if (OB_FAIL(format_str.append("\""))) {
        OB_LOG(WARN, "fail to append \"\"\" to buffer", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

void ObArrayBinary::clear()
{
  data_ = nullptr;
  null_bitmaps_ = nullptr;
  offsets_ = nullptr;
  length_ = 0;
  if (OB_NOT_NULL(data_container_)) {
    data_container_->clear();
  }
}

int ObArrayBinary::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 3;
  if (len  >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(null_bitmaps_);
    attrs[attr_idx].length_ = sizeof(uint8_t) * length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(offsets_);
    attrs[attr_idx].length_ = sizeof(uint32_t) * length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = data_;
    attrs[attr_idx].length_ = offsets_[length_ - 1];
    attr_idx++;
  }
  return ret;
}

int ObArrayBinary::compare_at(uint32_t left_begin, uint32_t left_len,
                              uint32_t right_begin, uint32_t right_len,
                              const ObIArrayType &right, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  uint32_t cmp_len = std::min(left_len, right_len);
  cmp_ret = 0;
  for (uint32_t i = 0; i < cmp_len && !cmp_ret && OB_SUCC(ret); ++i) {
    if (this->is_null(left_begin + i) && !right.is_null(right_begin + i)) {
      cmp_ret = 1;
    } else if (!this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
      cmp_ret = -1;
    } else if (this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
    } else {
      const ObArrayBinary *right_data = dynamic_cast<const ObArrayBinary *>(&right);
      uint32_t l_start = offset_at(left_begin + i, get_offsets());
      uint32_t l_child_len = get_offsets()[left_begin + i] - l_start;
      uint32_t r_start = right_data->offset_at(right_begin + i, right_data->get_offsets());
      uint32_t r_child_len = right_data->get_offsets()[right_begin + i] - r_start;
      if (OB_ISNULL(right_data)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
      } else {
        uint32_t data_len = std::min(l_child_len, r_child_len);
        cmp_ret = MEMCMP(data_ + l_start, right_data->get_data() + r_start, data_len);
        if (!cmp_ret && l_child_len != r_child_len) {
          cmp_ret = l_child_len > r_child_len ? 1 : -1;
        }
      }
    }
  }
  if (!cmp_ret && OB_SUCC(ret) && left_len != right_len) {
    cmp_ret = left_len > right_len ? 1 : -1;
  }
  return ret;
}

int ObArrayBinary::compare(const ObIArrayType &right, int &cmp_ret)
{
  return compare_at(0, length_, 0, right.size(), right, cmp_ret);
}

int ObArrayNested::get_data_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (data_container_ == NULL) {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(null_bitmaps_), sizeof(uint8_t) * length_);
    pos += sizeof(uint8_t) * length_;
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(offsets_), sizeof(uint32_t) * length_);
    pos += sizeof(uint32_t) * length_;
  } else {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->null_bitmaps_.get_data()), sizeof(uint8_t) * data_container_->null_bitmaps_.size());
    pos += sizeof(uint8_t) * data_container_->null_bitmaps_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->offsets_.get_data()), sizeof(uint32_t) * data_container_->offsets_.size());
    pos += sizeof(uint32_t) * data_container_->offsets_.size();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_->get_data_binary(res_buf + pos,  buf_len - pos))) {
    OB_LOG(WARN, "get data binary failed", K(ret), K(pos), K(length_), K(buf_len));
  }
  return ret;
}

int ObArrayNested::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (get_raw_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else {
    int64_t pos = 0;
    MEMCPY(res_buf + pos, &length_, sizeof(length_));
    pos += sizeof(length_);
    if (OB_FAIL(get_data_binary(res_buf + pos,  buf_len - pos))) {
      OB_LOG(WARN, "get data binary failed", K(ret), K(buf_len));
    }
  }
  return ret;
}

int ObArrayNested::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
{
  int ret = OB_SUCCESS;
  if (src.get_format() != get_format()
      || src.get_element_type() != element_type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(get_format()), K(element_type_));
  } else if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    // insert offsets
    uint32_t last_offset = offset_at(begin, src.get_offsets());
    uint32_t pre_max_offset = data_container_->offset_at(length_);
    for (uint32_t i = 0; i < len && OB_SUCC(ret); ++i) {
      if (OB_FAIL(data_container_->offsets_.push_back(pre_max_offset + src.get_offsets()[begin + i] - last_offset))) {
        OB_LOG(WARN, "failed to push value to array data", K(ret));
      } else {
        last_offset = src.get_offsets()[begin + i];
        pre_max_offset = data_container_->offset_at(data_container_->offsets_.size());
      }
    }
    // insert nullbitmaps
    for (uint32_t i = 0; i < len && OB_SUCC(ret); ++i) {
      if (OB_FAIL(data_container_->null_bitmaps_.push_back(src.get_nullbitmap()[begin + i]))) {
        OB_LOG(WARN, "failed to push null", K(ret));
      }
    }
    // insert data
    if (OB_SUCC(ret)) {
      uint32_t start = offset_at(begin, src.get_offsets());
      uint32_t child_len = src.get_offsets()[begin + len - 1] - start;
      const ObIArrayType *child_arr = static_cast<const ObArrayNested&>(src).get_child_array();
      if (OB_FAIL(data_->insert_from(*child_arr, start, child_len))) {
        OB_LOG(WARN, "failed to insert child array", K(ret));
      } else {
        length_ += len;
      }
    }
  }
  return ret;
}

int ObArrayNested::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    length_ = data_container_->offsets_.size();
    offsets_ = data_container_->offsets_.get_data();
    null_bitmaps_ = data_container_->null_bitmaps_.get_data();
    if (data_ != NULL) {
      data_->init();
    }
  }
  return ret;
}

int ObArrayNested::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() < sizeof(length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    length_ = *reinterpret_cast<uint32_t *>(raw_str);
    if (length_ > 0) {
      pos += sizeof(length_);
      // init null bitmap
      null_bitmaps_ = reinterpret_cast<uint8_t *>(raw_str + pos);
      if (pos + sizeof(uint8_t) * length_ > raw_data.length()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
      } else {
        pos += sizeof(uint8_t) * length_;
        if (pos + sizeof(uint32_t) * length_ > raw_data.length()) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
        } else {
          // init offset
          offsets_ = reinterpret_cast<uint32_t *>(raw_str + pos);
          // caution : length_ - 1 means : last offset is length of data_
          pos += sizeof(uint32_t) * (length_ - 1);
          // init data
          ObString data_str(raw_data.length() - pos, raw_str + pos);
          if (OB_FAIL(data_->init(data_str))) {
            OB_LOG(WARN, "data init failed", K(ret), K(pos), K(length_), K(raw_data.length()));
          }
        }
      }
    }
  }
  return ret;
}

int ObArrayNested::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  const uint32_t count = with_length ? 4 : 3;
  if (attr_count < count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    uint32_t idx = 0;
    if (with_length) {
      length_ = attrs[idx++].get_uint32();
    }  else {
      length_ = attrs[0].get_int_bytes() / sizeof(uint8_t);
    }
    null_bitmaps_ = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(attrs[idx++].get_string().ptr()));
    offsets_ = const_cast<uint32_t *>(reinterpret_cast<const uint32_t *>(attrs[idx++].get_string().ptr()));
    if (OB_FAIL(data_->init(attrs + idx, attr_count - idx, false))) {
      OB_LOG(WARN, "failed to init attrs", K(ret), K(attr_count), K(count));
    }
    if ((with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint8_t) || length_ != attrs[2].get_int_bytes() / sizeof(uint32_t)))
        || (!with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint32_t)))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected attrs", K(ret), K(with_length), K(length_));
    }
  }
  return ret;
}

int ObArrayNested::print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str, uint32_t begin, uint32_t print_size) const
{
  int ret = OB_SUCCESS;
  const ObCollectionArrayType *array_type = dynamic_cast<const ObCollectionArrayType *>(elem_type);
  if (OB_ISNULL(array_type)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else {
    if (print_size == 0) {
      // print whole array
      print_size = length_;
    }
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (i > begin && OB_FAIL(format_str.append(","))) {
        OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
      } else if (null_bitmaps_[i]) {
          // value is null
          if (OB_FAIL(format_str.append("NULL"))) {
            OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
          }
      } else {
        uint32_t start = offset_at(i, offsets_);
        uint32_t elem_cnt = offsets_[i] - start;
        if (OB_FAIL(data_->print(array_type->element_type_, format_str, start, elem_cnt))) {
           OB_LOG(WARN, "fail to append string to format_str", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

int ObArrayNested::push_back(const ObIArrayType &src, bool is_null)
{
  int ret = OB_SUCCESS;
  if (src.get_format() != data_->get_format()
      || src.get_element_type() != data_->get_element_type()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(data_->get_format()), K(data_->get_element_type()));
  } else if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (is_null) {
    if (OB_FAIL(push_null())) {
      OB_LOG(WARN, "failed to push null", K(ret));
    }
  } else {
    uint32_t last_offset = data_container_->offset_at(length_);
    uint32_t cur_offset = last_offset + src.size();
    if (OB_FAIL(data_container_->null_bitmaps_.push_back(false))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_container_->offsets_.push_back(cur_offset))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_->insert_from(src, 0, src.size()))) {
      OB_LOG(WARN, "failed to insert child array", K(ret));
    } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
    } else {
      length_++;
    }
  }

  return ret;
}

int ObArrayNested::push_null()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else {
    uint32_t last_offset =  data_container_->offset_at(length_);
    if (OB_FAIL(data_container_->null_bitmaps_.push_back(true))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_container_->offsets_.push_back(last_offset))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
    } else {
      length_++;
    }
  }
  return ret;
}

void ObArrayNested::clear()
{
  null_bitmaps_ = nullptr;
  offsets_ = nullptr;
  length_ = 0;
  if (OB_NOT_NULL(data_)) {
    data_->clear();
  }
  if (OB_NOT_NULL(data_container_)) {
    data_container_->clear();
  }
}

int ObArrayNested::at(uint32_t idx, ObIArrayType &dest)
{
  int ret = OB_SUCCESS;
  uint32_t start = offset_at(idx, get_offsets());
  uint32_t child_len = get_offsets()[idx] - start;
  const ObIArrayType *child_arr = get_child_array();
  if (OB_FAIL(dest.insert_from(*child_arr, start, child_len))) {
    OB_LOG(WARN, "failed to insert child array", K(ret), K(idx), K(start), K(child_len));
  } else if (OB_FAIL(dest.init())) {
    OB_LOG(WARN, "failed to init array element", K(ret), K(idx), K(start), K(child_len));
  }
  return ret;
}

int ObArrayNested::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 2;
  if (len  >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(null_bitmaps_);
    attrs[attr_idx].length_ = sizeof(uint8_t) * length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(offsets_);
    attrs[attr_idx].length_ = sizeof(uint32_t) * length_;
    attr_idx++;
    if (OB_FAIL(data_->flatten(attrs, attr_count, attr_idx))) {
      OB_LOG(WARN, "failed to flatten data", K(ret), K(attr_count), K(attr_idx));
    }
  }
  return ret;
}

int ObArrayNested::compare_at(uint32_t left_begin, uint32_t left_len,
                              uint32_t right_begin, uint32_t right_len,
                              const ObIArrayType &right, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  uint32_t cmp_len = std::min(left_len, right_len);
  cmp_ret = 0;
  for (uint32_t i = 0; i < cmp_len && !cmp_ret && OB_SUCC(ret); ++i) {
    if (this->is_null(left_begin + i) && !right.is_null(right_begin + i)) {
      cmp_ret = 1;
    } else if (!this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
      cmp_ret = -1;
    } else if (this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
    } else {
      const ObArrayNested *right_nested = dynamic_cast<const ObArrayNested *>(&right);
      uint32_t l_start = offset_at(left_begin + i, get_offsets());
      uint32_t l_child_len = get_offsets()[left_begin + i] - l_start;
      uint32_t r_start = right_nested->offset_at(right_begin + i, right_nested->get_offsets());
      uint32_t r_child_len = right_nested->get_offsets()[right_begin + i] - r_start;
      if (OB_ISNULL(right_nested)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
      } else if (OB_FAIL(get_child_array()->compare_at(l_start, l_child_len, r_start, r_child_len, *right_nested->get_child_array(), cmp_ret))) {
        OB_LOG(WARN, "failed to do child array compare", K(ret), K(l_start), K(l_child_len), K(r_start), K(r_child_len));
      }
    }
  }
  if (!cmp_ret && OB_SUCC(ret) && left_len != right_len) {
    cmp_ret = (left_len > right_len ? 1 : -1);
  }
  return ret;
}

int ObArrayNested::compare(const ObIArrayType &right, int &cmp_ret)
{
  return compare_at(0, length_, 0, right.size(), right, cmp_ret);
}

#undef CONSTRUCT_ARRAY_OBJ
#undef CONSTRUCT_FIXED_ARRAY_OBJ

} // namespace common
} // namespace oceanbase