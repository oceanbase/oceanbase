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
 * This file contains implement for the xml & json type data basic interface abstraction.
 */
#define USING_LOG_PREFIX LIB

#include "lib/string/ob_string_buffer.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_multi_mode_bin.h"

namespace oceanbase {
namespace common {

class ObIMulModeBase;



ObMulBinHeaderSerializer::ObMulBinHeaderSerializer(
  ObStringBuffer* buffer,
  ObMulModeNodeType type,
  uint64_t total_size,
  uint64_t count)
  : buffer_(buffer),
    begin_(buffer->length()),
    total_(total_size),
    count_(count)
{
  type_ = type;

  obj_var_size_type_ = ObMulModeVar::get_var_type(total_);
  entry_var_size_type_ = ObMulModeVar::get_var_type(total_);
  count_var_size_type_ = ObMulModeVar::get_var_type(count_);

  obj_var_size_ = ObMulModeVar::get_var_size(obj_var_size_type_);
  entry_var_size_ = obj_var_size_;
  count_var_size_ = ObMulModeVar::get_var_size(count_var_size_type_);

  count_var_offset_ = MUL_MODE_BIN_HEADER_LEN;
  if (is_extend_type(type)) {
    count_var_offset_++;
  }

  obj_var_offset_ = count_var_offset_ + count_var_size_;
}

void ObMulBinHeaderSerializer::set_var_value(uint8_t var_size, uint8_t offset, uint64_t value)
{
  if (var_size == 1) {
    *reinterpret_cast<uint8_t*>(buffer_->ptr() + begin_ + offset) = static_cast<uint8_t>(value);
  } else if (var_size == 2) {
    *reinterpret_cast<uint16_t*>(buffer_->ptr() + begin_ + offset) = static_cast<uint16_t>(value);
  } else if (var_size == 4) {
    *reinterpret_cast<uint32_t*>(buffer_->ptr() + begin_ + offset) = static_cast<uint32_t>(value);
  } else {
    *reinterpret_cast<uint64_t*>(buffer_->ptr() + begin_ + offset) = value;
  }
}

void ObMulBinHeaderSerializer::set_obj_size(uint64_t size)
{
  set_var_value(obj_var_size_, obj_var_offset_, size);
}

void ObMulBinHeaderSerializer::set_count(uint64_t size)
{
  set_var_value(count_var_size_, count_var_offset_, size);
}

ObMulBinHeaderSerializer::ObMulBinHeaderSerializer(const char* data, uint64_t length)
  : data_(data),
    data_len_(length)
{
}

int ObMulBinHeaderSerializer::serialize()
{
  INIT_SUCC(ret);
  if (OB_FAIL(buffer_->reserve(MUL_MODE_BIN_HEADER_LEN))) {
    LOG_WARN("failed to reserve", K(ret), K(buffer_->length()));
  } else if (is_scalar_data_type(type_)) {
    if (is_extend_type(type_)) {
      ObMulModeExtendStorageType tmp = get_extend_storage_type(type_);
      if (OB_FAIL(buffer_->append(reinterpret_cast<const char*>(&tmp.first), sizeof(uint8_t)))
          || OB_FAIL(buffer_->append(reinterpret_cast<const char*>(&tmp.second), sizeof(uint8_t))))
      LOG_WARN("failed to append", K(ret), K(buffer_->length()));
    } else if (OB_FAIL(buffer_->append(reinterpret_cast<const char*>(&type_), sizeof(uint8_t)))) {
      LOG_WARN("failed to append", K(ret), K(buffer_->length()));
    }
  } else if (OB_FAIL(buffer_->reserve(header_size()))) {
    LOG_WARN("failed to reserve", K(ret), K(buffer_->length()));
  } else {
    buffer_->set_length(start() + header_size());
    new (buffer_->ptr() + start())ObMulModeBinHeader(static_cast<uint8_t>(type_),
                                                      ObMulModeVar::get_var_type(total_),
                                                      ObMulModeVar::get_var_type(count_),
                                                      ObMulModeVar::get_var_type(total_),
                                                      static_cast<uint8_t>(1));

    if (is_extend_type(type_)) {
      ObMulModeExtendStorageType tmp = get_extend_storage_type(type_);
      *reinterpret_cast<uint8_t*>(buffer_->ptr() + start()) = static_cast<uint8_t>(tmp.first);
      *reinterpret_cast<uint8_t*>(buffer_->ptr() + start() + MUL_MODE_BIN_HEADER_LEN) = static_cast<uint8_t>(tmp.first);
    }
    set_obj_size(total_);
    set_count(count_);
  }

  return ret;
}

int ObMulBinHeaderSerializer::deserialize()
{
  INIT_SUCC(ret);
  if (data_len_ < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to deserialize, data len is 0", K(ret));
  } else {
    type_ = static_cast<ObMulModeNodeType>(*data_);
    if (is_scalar_data_type(type_) && is_extend_type(type_)) {
      if (data_len_ <= 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to deserialize, data len is 2", K(ret), K(type_), K(data_len_));
      } else {
        type_ = eval_data_type(type_, static_cast<uint8_t>(data_[1]));
      }
    } else if (is_scalar_data_type(type_)) {
    } else if (data_len_ <= 2) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to deserialize, data len less than 2", K(ret), K(type_), K(data_len_));
    } else {
      const ObMulModeBinHeader* header = reinterpret_cast<const ObMulModeBinHeader*>(data_);
      obj_var_size_ = ObMulModeVar::get_var_size(header->obj_size_type_);
      entry_var_size_ = ObMulModeVar::get_var_size(header->kv_entry_size_type_);
      count_var_size_ = ObMulModeVar::get_var_size(header->count_size_type_);
      count_var_offset_ = MUL_MODE_BIN_HEADER_LEN;

      obj_var_size_type_ = header->obj_size_type_;
      entry_var_size_type_ = header->kv_entry_size_type_;
      count_var_size_type_ = header->count_size_type_;

      if (is_extend_type(type_)) {
        count_var_offset_++;
      }
      obj_var_offset_ = count_var_offset_ + count_var_size_;

      if (obj_var_offset_ + obj_var_size_ > data_len_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to deserialize, data len less than 2", K(ret), K(type_),
                K(data_len_), K(entry_var_size_), K(count_var_size_), K(obj_var_size_));
      } else {
        if (is_extend_type(type_)) {
          type_ = eval_data_type(type_, data_[2]);
        }
        ObMulModeVar::read_size_var(data_ + obj_var_offset_, obj_var_size_, &total_);
        ObMulModeVar::read_size_var(data_ + count_var_offset_, count_var_size_, &count_);
      }
    }
  }

  return ret;
}

int ObMulModeScalarSerializer::serialize_scalar_header(ObMulModeNodeType type, ObStringBuffer& buffer)
{
  INIT_SUCC(ret);

  bool is_extend = is_extend_type(type);
  ObMulModeExtendStorageType extend_type;

  if (is_extend) {
    extend_type = get_extend_storage_type(type);
  }

  uint8_t reserve_size = sizeof(uint8_t) + (is_extend ? sizeof(uint8_t) : 0);
  uint64_t pos = buffer.length();
  if (OB_FAIL(buffer.reserve(reserve_size))) {
    LOG_WARN("failed to reserve size for type header", K(ret), K(buffer.length()));
  } else {
    char* data = buffer.ptr();
    if (is_extend) {
      data[pos++] = extend_type.first;
      data[pos++] = extend_type.second;
    } else {
      data[pos++] = static_cast<char>(type);
    }
    buffer.set_length(pos);
  }

  return ret;
}

int ObMulModeScalarSerializer::serialize_integer(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);
  int64_t value = is_int_type(node->type()) ? node->get_int() : node->get_uint();
  int64_t ser_len = serialization::encoded_length_vi64(value);
  int64_t pos = buffer_->length();
  if (OB_FAIL(buffer_->reserve(ser_len))) {
    LOG_WARN("failed to reserver serialize size for int", K(ret), K(ser_len));
  } else if (OB_FAIL(serialization::encode_vi64(buffer_->ptr(), buffer_->capacity(), pos, value))) {
    LOG_WARN("failed to serialize for int", K(ret), K(ser_len));
  } else if (OB_FAIL(buffer_->set_length(pos))) {
    LOG_WARN("failed to update len for int", K(ret), K(pos));
  }
  return ret;
}

int ObMulModeScalarSerializer::serialize_decimal(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);
  ObPrecision prec = node->get_decimal_precision();
  ObScale scale = node->get_decimal_scale();
  int64_t ser_len = node->get_serialize_size();
  int64_t pos = buffer_->length();
  if (OB_FAIL(buffer_->reserve(ser_len))) {
    LOG_WARN("failed to reserver serialize size for decimal obj", K(ret), K(pos), K(ser_len));
  } else if (OB_FAIL(serialization::encode_i16(buffer_->ptr(), buffer_->capacity(), pos, prec))) {
    LOG_WARN("failed to serialize for decimal precision", K(ret), K(pos), K(prec));
  } else if (OB_FAIL(buffer_->set_length(pos))) {
    LOG_WARN("failed to set length for decimal precision", K(ret), K(pos), K(prec));
  } else if (OB_FAIL(serialization::encode_i16(buffer_->ptr(), buffer_->capacity(), pos, scale))) {
    LOG_WARN("failed to serialize for decimal precision", K(ret), K(pos), K(scale));
  } else if (OB_FAIL(buffer_->set_length(pos))) {
    LOG_WARN("failed to set length for decimal scale", K(ret), K(pos), K(scale));
  } else if (OB_FAIL(node->get_decimal_data().serialize(buffer_->ptr(), buffer_->capacity(), pos))) {
    LOG_WARN("failed to serialize for decimal value", K(ret), K(pos));
  } else if (OB_FAIL(buffer_->set_length(pos))){
    LOG_WARN("failed to update len for decimal obj", K(ret), K(pos));
  }
  return ret;
}

int ObMulModeScalarSerializer::serialize_string(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);

  ObString value;
  if (OB_FAIL(node->get_value(value))) {
    LOG_WARN("failed to get string value for obj", K(ret));
  } else if (OB_FAIL(buffer_->reserve(value.length() + 2))) {
    LOG_WARN("failed to reserver serialize size obj", K(ret), K(value.length()));
  } else {
    int64_t ser_len = serialization::encoded_length_vi64(value.length());
    int64_t pos = buffer_->length() + sizeof(uint8_t);
    ObMulModeNodeType type = node->type();

    if (OB_FAIL(serialize_scalar_header(type, *buffer_))) {
      LOG_WARN("failed to serialize type for str obj", K(ret), K(ser_len));
    } else if (OB_FAIL(buffer_->reserve(ser_len))) {
      LOG_WARN("failed to reserver serialize size for str obj", K(ret), K(ser_len));
    } else if (OB_FAIL(serialization::encode_vi64(buffer_->ptr(), buffer_->capacity(), pos, value.length()))) {
      LOG_WARN("failed to serialize for str obj", K(ret), K(ser_len));
    } else if (OB_FAIL(buffer_->set_length(pos))) {
      LOG_WARN("failed to update len for str obj", K(ret), K(pos));
    } else if (OB_FAIL(buffer_->append(value.ptr(), value.length()))) {
      LOG_WARN("failed to append string value", K(ret));
    }
  }

  return ret;
}

int ObMulModeScalarSerializer::serialize_null(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);

  if (OB_FAIL(buffer_->append("\0", sizeof(uint8_t)))) {
    LOG_WARN("failed to append null obj", K(ret));
  }

  return ret;
}

int ObMulModeScalarSerializer::serialize_boolean(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);
  char value = static_cast<char>(node->get_boolean());
  if (OB_FAIL(buffer_->append(reinterpret_cast<const char*>(&value), sizeof(char)))) {
    LOG_WARN("failed to append bool obj", K(ret));
  }

  return ret;
}

int ObMulModeScalarSerializer::serialize_time(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);

  ObTime ob_time = node->get_time();
  // todo: switch case for ObMulModeNodeType
  int64_t value = ObTimeConverter::ob_time_to_time(ob_time);

  if (OB_FAIL(buffer_->append(reinterpret_cast<const char*>(&value), sizeof(int64_t)))) {
    LOG_WARN("failed to append timestamp obj value", K(ret));
  }
  return ret;
}

int ObMulModeScalarSerializer::serialize_double(ObIMulModeBase* node, int32_t depth)
{
  INIT_SUCC(ret);

  double value = node->get_double();
  if (isnan(value) || isinf(value)) {
    ret = OB_INVALID_NUMERIC;
    LOG_WARN("invalid double value", K(ret), K(value));
  } else if (OB_FAIL(buffer_->append(reinterpret_cast<const char*>(&value), sizeof(double)))) {
    LOG_WARN("failed to append double obj", K(ret));
  }

  return ret;
}

ObMulModeContainerSerializer::ObMulModeContainerSerializer(ObIMulModeBase* root, ObStringBuffer* buffer, int64_t children_count)
  : header_(buffer, root->type(), root->get_serialize_size(), children_count)
{
  root_ = root;
  type_ = root->type();
}

ObMulModeContainerSerializer::ObMulModeContainerSerializer(ObIMulModeBase* root, ObStringBuffer* buffer)
  : header_(buffer, root->type(), root->get_serialize_size(), root->size())
{
  root_ = root;
  type_ = root->type();
}

ObMulModeContainerSerializer::ObMulModeContainerSerializer(const char* data, int64_t length)
  : header_(data, length),
    data_(data),
    length_(length)
{
}

/* var size */
int ObMulModeVar::read_var(const char *data, uint8_t type, uint64_t *var)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(data)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input data null val.", K(ret));
  } else {
    ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(type);
    switch (size) {
      case MBL_UINT8: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(data));
        break;
      }
      case MBL_UINT16: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint16_t*>(data));
        break;
      }
      case MBL_UINT32: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint32_t*>(data));
        break;
      }
      case MBL_UINT64: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint64_t*>(data));
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid var type.", K(ret), K(type));
        break;
      }
    }
  }
  return ret;
}

int ObMulModeVar::read_size_var(const char *data, uint8_t var_size, int64_t *var)
{
  INIT_SUCC(ret);
  if (var_size == 1) {
    *var = *reinterpret_cast<const int8_t*> (data);
  } else if (var_size == 2) {
    *var = *reinterpret_cast<const int16_t*>(data);
  } else if (var_size == 4) {
    *var = *reinterpret_cast<const int32_t*>(data);
  } else if (var_size == 8) {
    *var = *reinterpret_cast<const int64_t*>(data);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid var type.", K(ret), K(var_size));
  }
  return ret;
}

int ObMulModeVar::append_var(uint64_t var, uint8_t type, ObStringBuffer &result)
{
  INIT_SUCC(ret);
  ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(type);
  switch (size) {
    case MBL_UINT8: {
      uint8_t var_trans = static_cast<uint8_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint8_t));
      break;
    }
    case MBL_UINT16: {
      uint16_t var_trans = static_cast<uint16_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint16_t));
      break;
    }
    case MBL_UINT32: {
      uint32_t var_trans = static_cast<uint32_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint32_t));
      break;
    }
    case MBL_UINT64: {
      uint64_t var_trans = static_cast<uint64_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint64_t));
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to do append var.", K(ret), K(size), K(var));
  }
  return ret;
}

int ObMulModeVar::reserve_var(uint8_t type, ObStringBuffer &result)
{
  INIT_SUCC(ret);
  ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(type);
  switch (size) {
    case MBL_UINT8: {
      uint8_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint8_t));
      break;
    }
    case MBL_UINT16: {
      uint16_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint16_t));
      break;
    }
    case MBL_UINT32: {
      uint32_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint32_t));
      break;
    }
    case MBL_UINT64: {
      uint64_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint64_t));
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to do reserve var.", K(ret), K(size));
  }
  return ret;
}

int ObMulModeVar::set_var(uint64_t var, uint8_t type, char *pos)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(pos)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("output pos is null.", K(ret));
  } else {
    ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(type);
    switch (size) {
      case MBL_UINT8: {
        uint8_t *val_pos = reinterpret_cast<uint8_t*>(pos);
        *val_pos = static_cast<uint8_t>(var);
        break;
      }
      case MBL_UINT16: {
        uint16_t *val_pos = reinterpret_cast<uint16_t*>(pos);
        *val_pos = static_cast<uint16_t>(var);
        break;
      }
      case MBL_UINT32: {
        uint32_t *val_pos = reinterpret_cast<uint32_t*>(pos);
        *val_pos = static_cast<uint32_t>(var);
        break;
      }
      case MBL_UINT64: {
        uint64_t *val_pos = reinterpret_cast<uint64_t*>(pos);
        *val_pos = static_cast<uint64_t>(var);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid var type.", K(ret), K(size));
        break;
      }
    }
  }
  return ret;
}

uint64_t ObMulModeVar::get_var_size(uint8_t type)
{
  uint64_t var_size = MBL_MAX;
  ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(type);
  switch (size) {
    case MBL_UINT8: {
      var_size = sizeof(uint8_t);
      break;
    }
    case MBL_UINT16: {
      var_size = sizeof(uint16_t);
      break;
    }
    case MBL_UINT32: {
      var_size = sizeof(uint32_t);
      break;
    }
    case MBL_UINT64: {
      var_size = sizeof(uint64_t);
      break;
    }
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid var type.", K(OB_NOT_SUPPORTED), K(size));
      break;
    }
  }
  return var_size;
}

uint8_t ObMulModeVar::get_var_type(uint64_t var)
{
  ObMulModeBinLenSize lsize = MBL_UINT64;
  if ((var & 0xFFFFFFFFFFFFFF00ULL) == 0) {
    lsize = MBL_UINT8;
  } else if ((var & 0xFFFFFFFFFFFF0000ULL) == 0) {
    lsize = MBL_UINT16;
  } else if ((var & 0xFFFFFFFF00000000ULL) == 0) {
    lsize = MBL_UINT32;
  }
  return static_cast<uint8_t>(lsize);
}

int ObMulModeVar::read_var(const char *data, uint8_t type, int64_t *var)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(data)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input data is null.", K(ret));
  } else {
    ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(type);
    switch (size) {
      case MBL_UINT8: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int8_t*>(data));
        break;
      }
      case MBL_UINT16: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int16_t*>(data));
        break;
      }
      case MBL_UINT32: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int32_t*>(data));
        break;
      }
      case MBL_UINT64: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int64_t*>(data));
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid var type.", K(ret), K(type));
        break;
      }
    }
  }
  return ret;
}

uint64_t ObMulModeVar::var_int2uint(int64_t var)
{
  ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(ObMulModeVar::get_var_type(var));
  uint64 val = 0;
  switch (size) {
    case MBL_UINT8: {
      val = static_cast<uint64_t>(static_cast<int8_t>(var));
      break;
    }
    case MBL_UINT16: {
      val = static_cast<uint64_t>(static_cast<int16_t>(var));
      break;
    }
    case MBL_UINT32: {
      val = static_cast<uint64_t>(static_cast<int32_t>(var));
      break;
    }
    case MBL_UINT64: {
      val = static_cast<uint64_t>(static_cast<int64_t>(var));
      break;
    }
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid var type.", K(size));
      break;
    }
  }
  return val;
}

int64_t ObMulModeVar::var_uint2int(uint64_t var, uint8_t entry_size)
{
  ObMulModeBinLenSize size = static_cast<ObMulModeBinLenSize>(entry_size);
  int64_t val = 0;
  switch (size) {
    case MBL_UINT8: {
      if (var > INT8_MAX) {
        val = static_cast<int64_t>(static_cast<int8_t>(static_cast<uint8_t>(var)));
      } else {
        val = static_cast<int64_t>(static_cast<uint8_t>(static_cast<uint8_t>(var)));
      }
      break;
    }
    case MBL_UINT16: {
      if (var > INT16_MAX) {
        val = static_cast<int64_t>(static_cast<int16_t>(static_cast<uint16_t>(var)));
      } else {
        val = static_cast<int64_t>(static_cast<uint16_t>(static_cast<uint16_t>(var)));
      }
      break;
    }
    case MBL_UINT32: {
      val = static_cast<int64_t>(static_cast<int32_t>(static_cast<uint32_t>(var)));
      break;
    }
    case MBL_UINT64: {
      val = static_cast<int64_t>(var);
      break;
    }
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid var type.", K(size));
      break;
    }
  }
  return val;
}

uint8_t ObMulModeVar::get_var_type(int64_t var)
{
  ObMulModeBinLenSize lsize = MBL_UINT64;
  if (var <= INT8_MAX && var >= INT8_MIN) {
    lsize = MBL_UINT8;
  } else if (var <= INT16_MAX && var >= INT16_MIN) {
    lsize = MBL_UINT16;
  } else if (var <= INT32_MAX && var >= INT32_MIN) {
    lsize = MBL_UINT32;
  }
  return static_cast<uint8_t>(lsize);
}

bool ObBinMergeCtx::is_all_deleted()
{
  bool ret_bool = true;
  for (int i = 0; ret_bool && i < del_map_.size(); ++i) {
    ret_bool = del_map_.at(i);
  }
  return ret_bool;
}
int ObBinMergeCtx::get_valid_key_count()
{
  int count = 0;
  for (int i = 0; i < del_map_.size(); ++i) {
    if (!(del_map_.at(i))) {
      ++count;
    }
  }
  return count;
}
uint64_t ObMulModeBinMerge::estimated_count(bool retry, ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch)
{
  return retry ? ctx.retry_count_ : origin.attribute_count() + origin.size() + ctx.get_valid_key_count();
}
int ObMulModeBinMerge::merge(ObIMulModeBase& origin, ObIMulModeBase& patch, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  ObBinMergeCtx ctx(origin.get_allocator());
  // init ctx, and estimating buffer size
  if (origin.is_tree() || patch.is_tree()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must be binary.", K(patch.is_tree()), K(origin.is_tree()), K(ret));
  } else if (OB_FAIL(init_merge_info(ctx, origin, patch, res))) {
    LOG_WARN("fail to init ctx", K(ret));
  } else if (OB_FAIL(inner_merge(ctx, origin, patch, res))) {
    LOG_WARN("fail to merge", K(ret));
  }
  return ret;
}
int ObMulModeBinMerge::inner_merge(ObBinMergeCtx& ctx, ObIMulModeBase& origin,
                                  ObIMulModeBase& patch, ObIMulModeBase& res, bool retry)
{
  INIT_SUCC(ret);
  // duplicate ns that defined in this element should be delete
  // because namespaces with the same key are subject to the latest
  // but this definition is only valid in this element and its descendant
  // so, restore ns vec when finish merging this element, in case its sibling loses ns definition
  ObStack<bool> origin_del_map(ctx.allocator_);
  int64_t start = 0;
  bool need_merge = false;
  if (OB_ISNULL(ctx.buffer_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    start = ctx.buffer_->length();
  }
  if (ctx.reuse_del_map_ ) {
    origin_del_map = ctx.del_map_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(if_need_merge(ctx, origin, patch, res, need_merge))) {
    LOG_WARN("fail to check if need to merge", K(ret));
  } else if (!need_merge) {
    if (OB_FAIL(append_res_without_merge(ctx, origin, patch, res))) {
      LOG_WARN("fail to merge", K(ret));
    }
  } else {
    // init common_header, the total_size and count is not precise
    // check after all value is merged
    ObMulBinHeaderSerializer cur_header(ctx.buffer_, get_res_type(origin.type(), patch.type()),
                                        estimated_length(retry, ctx, origin, patch),
                                        estimated_count(retry, ctx, origin, patch));
    if (OB_FAIL(append_header_to_res(ctx, origin, patch, cur_header, res))) {
      LOG_WARN("fail to append header", K(ret));
    } else if (!if_need_append_key(ctx, origin, patch, res)) {
      if (OB_FAIL(append_key_without_merge(ctx, origin, cur_header, res))) {
        LOG_WARN("fail to copy key", K(ret));
      }
    } else if (OB_FAIL(append_merge_key(ctx, origin, patch, cur_header, res))) {
      LOG_WARN("fail to merge key", K(ret));
    }
    int64_t merged_len = 0;
    int append_key_count = ctx.defined_ns_idx_.size();
    for (int i = 0; OB_SUCC(ret) && i < cur_header.count_; ++i) {
      uint64_t origin_len = ctx.buffer_->length();
      bool is_origin = true;
      int idx = i;
      if (i < append_key_count) {
        is_origin = false;
      } else {
        idx -= append_key_count;
      }
      if (OB_FAIL(append_value_by_idx(is_origin, idx, ctx, origin, patch, cur_header, res))) {
        LOG_WARN("fail to append value", K(ret));
      } else if (OB_FALSE_IT(merged_len = ctx.buffer_->length())) {
      } else if (merged_len < origin_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value length must > 0", K(ret));
      } else if (OB_FAIL(set_value_offset(i, origin_len - start, ctx, res))) {
        LOG_WARN("fail to set value offset", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (merged_len < start) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error length", K(ret));
    } else if (ObMulModeVar::get_var_type(merged_len - start) > cur_header.get_obj_var_size_type()
      || ObMulModeVar::get_var_type(cur_header.count_) > cur_header.get_count_var_size_type()) {
      if (retry) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to serialize as meta info not match.", K(ret));
      } else {
        int obj_type_diff = ObMulModeVar::get_var_type(merged_len - start) - cur_header.get_obj_var_size_type();
        int count_type_diff = ObMulModeVar::get_var_type(cur_header.count_) - cur_header.get_count_var_size_type();
        ctx.retry_count_ = cur_header.count_;
        ctx.retry_len_ = (merged_len - start) + cur_header.count_ * count_type_diff + 4 * cur_header.count_ * obj_type_diff;
        new (&cur_header) ObMulBinHeaderSerializer(ctx.buffer_, get_res_type(origin.type(), patch.type()), merged_len, cur_header.count_);
        if (OB_FAIL(inner_merge(ctx, origin, patch, res, true))) {
          LOG_WARN("fail to retry", K(ret));
        } else {
          merged_len = ctx.buffer_->length();
        }
      }
    } else {
      cur_header.set_obj_size(merged_len - start);
      cur_header.set_count(cur_header.count_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ctx.reuse_del_map_) {
    ctx.del_map_ = origin_del_map;
  }
  return ret;
}
// serialize common header
int ObMulModeBinMerge::append_header_to_res(ObBinMergeCtx& ctx, ObIMulModeBase& origin, ObIMulModeBase& patch,
                                            ObMulBinHeaderSerializer& header, ObIMulModeBase& res)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(header.buffer_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL(header.serialize())) {
    LOG_WARN("fail to serialize common header", K(ret));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase