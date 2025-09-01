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

#define USING_LOG_PREFIX STORAGE

#include "ob_semistruct_json.h"
#include "storage/blocksstable/cs_encoding/ob_integer_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_int_dict_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_string_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_str_dict_column_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

static int datum_to_json(ObJsonNode &base_node, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType node_type = base_node.json_type();
  switch (node_type) {
    case ObJsonNodeType::J_DECIMAL: {
      ObJsonDecimal& node = static_cast<ObJsonDecimal&>(base_node);
      node.set_value(datum.get_number());
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      ObJsonInt& node = static_cast<ObJsonInt&>(base_node);
      node.set_value(datum.get_int());
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      ObJsonUint& node = static_cast<ObJsonUint&>(base_node);
      node.set_value(datum.get_uint());
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      ObJsonDouble& node = static_cast<ObJsonDouble&>(base_node);
      node.set_value(datum.get_double());
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      ObJsonOFloat& node = static_cast<ObJsonOFloat&>(base_node);
      node.set_value(datum.get_float());
      break;
    }
    case ObJsonNodeType::J_STRING: {
      ObJsonString& node = static_cast<ObJsonString&>(base_node);
      node.set_value(datum.ptr_, datum.len_);
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      ObJsonBoolean& node = static_cast<ObJsonBoolean&>(base_node);
      node.set_value(datum.get_int());
      break;
    }
    case ObJsonNodeType::J_DATE: {
      ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
      ObTime time;
      if (OB_FAIL(ObTimeConverter::date_to_ob_time(datum.get_date(), time))) {
        LOG_WARN("to ob time fail", K(ret), K(datum));
      } else {
        node.set_value(time);
      }
      break;
    }
    case ObJsonNodeType::J_TIME: {
      ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
      ObTime time;
      if (OB_FAIL(ObTimeConverter::time_to_ob_time(datum.get_int(), time))) {
        LOG_WARN("to ob time fail", K(ret), K(datum));
      } else {
        node.set_value(time);
      }
      break;
    }
    case ObJsonNodeType::J_DATETIME: {
      ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
      ObTime time;
      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(datum.get_int(), nullptr, time))) {
        LOG_WARN("to ob time fail", K(ret), K(datum));
      } else {
        node.set_value(time);
      }
      break;
    }
    case ObJsonNodeType::J_MYSQL_DATE: {
      ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
      ObTime time;
      if (OB_FAIL(ObTimeConverter::mdate_to_ob_time(datum.get_int(), time))) {
        LOG_WARN("to ob time fail", K(ret), K(datum));
      } else {
        node.set_value(time);
      }
      break;
    }
    case ObJsonNodeType::J_MYSQL_DATETIME: {
      ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
      ObTime time;
      if (OB_FAIL(ObTimeConverter::mdatetime_to_ob_time(datum.get_int(), time))) {
        LOG_WARN("to ob time fail", K(ret), K(datum));
      } else {
        node.set_value(time);
      }
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP: {
      ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
      ObTime time;
      if (OB_FAIL(ObTimeConverter::usec_to_ob_time(datum.get_int(), time))) {
        LOG_WARN("to ob time fail", K(ret), K(datum));
      } else {
        node.set_value(time);
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      ObJsonOpaque& node = static_cast<ObJsonOpaque&>(base_node);
      const char* buf = datum.ptr_;
      // [ObObjType(uint16_t)][length(uint64_t)][data]
      if (sizeof(uint16_t) + sizeof(uint64_t) > datum.len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough for opaque len", K(ret), K(datum));
      } else {
        node.set_field_type(static_cast<ObObjType>(*reinterpret_cast<const uint16_t*>(buf)));
        uint64_t str_len = *reinterpret_cast<const uint64_t*>(buf + sizeof(uint16_t));
        if (sizeof(uint16_t) + sizeof(uint64_t) + str_len > datum.len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for opaque data", K(ret), K(str_len), K(datum));
        } else {
          node.set_value(ObString(str_len, buf + sizeof(uint16_t) + sizeof(uint64_t)));
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid node type.", K(ret), K(node_type));
      break;
    }
  }
  return ret;
}

static int bin_to_tree(const ObJsonBin &bin, ObJsonNode &base_node)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType node_type = bin.json_type();
  if (node_type != base_node.json_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bin type is not equal to tree type", K(ret), K(bin.json_type()), K(base_node.json_type()));
  } else {
    switch (node_type) {
      case ObJsonNodeType::J_NULL: {
        // nothing
        break;
      }
      case ObJsonNodeType::J_DECIMAL: {
        ObJsonDecimal& node = static_cast<ObJsonDecimal&>(base_node);
        node.set_value(bin.get_decimal_data());
        break;
      }
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        ObJsonInt& node = static_cast<ObJsonInt&>(base_node);
        node.set_value(bin.get_int());
        break;
      }
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG: {
        ObJsonUint& node = static_cast<ObJsonUint&>(base_node);
        node.set_value(bin.get_uint());
        break;
      }
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        ObJsonDouble& node = static_cast<ObJsonDouble&>(base_node);
        node.set_value(bin.get_double());
        break;
      }
      case ObJsonNodeType::J_OFLOAT: {
        ObJsonOFloat& node = static_cast<ObJsonOFloat&>(base_node);
        node.set_value(bin.get_float());
        break;
      }
      case ObJsonNodeType::J_STRING: {
        ObJsonString& node = static_cast<ObJsonString&>(base_node);
        node.set_value(bin.get_data(), bin.get_data_length());
        break;
      }
      case ObJsonNodeType::J_BOOLEAN: {
        ObJsonBoolean& node = static_cast<ObJsonBoolean&>(base_node);
        node.set_value(bin.get_boolean());
        break;
      }
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_TIME:
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_MYSQL_DATE:
      case ObJsonNodeType::J_MYSQL_DATETIME:
      case ObJsonNodeType::J_TIMESTAMP: {
        ObJsonDatetime& node = static_cast<ObJsonDatetime&>(base_node);
        ObTime time;
        if (OB_FAIL(bin.get_obtime(time))) {
          LOG_WARN("get obtime fail", K(ret), K(bin));
        } else {
          node.set_value(time);
        }
        break;
      }
      case ObJsonNodeType::J_OPAQUE: {
        ObJsonOpaque& node = static_cast<ObJsonOpaque&>(base_node);
        node.set_field_type(bin.field_type());
        node.set_value(ObString(bin.get_data_length(), bin.get_data()));
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid node type.", K(ret), K(node_type));
        break;
      }
    }
  }
  return ret;
}

static bool is_int_len(const int len)
{
  return sizeof(uint8_t) == len || sizeof(uint16_t) == len || sizeof(uint32_t) == len || sizeof(uint64_t) == len;
}


/**
 * ---------------------------------------- ObSemiJsonNode ----------------------------------------
 */

void ObSemiJsonNode::set_node(ObSemiSchemaNode *schema_node)
{
  ObJsonNodeType json_type = schema_node->json_type();
  if (json_type == ObJsonNodeType::J_OBJECT) {
    object_node_ = dynamic_cast<ObSemiSchemaObject*>(schema_node);
  } else if (json_type == ObJsonNodeType::J_ARRAY) {
    array_node_ = dynamic_cast<ObSemiSchemaArray*>(schema_node);
  } else {
    scalar_node_ = dynamic_cast<ObSemiSchemaScalar*>(schema_node);
  }
}

int ObSemiJsonNode::inc_path_cnt(ObJsonNodeType &json_type)
{
  int ret = OB_SUCCESS;
  if (json_type == ObJsonNodeType::J_OBJECT) {
    if (OB_ISNULL(object_node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object node is null", K(ret));
    } else {
      object_node_->inc_path_cnt();
    }
  } else if (json_type == ObJsonNodeType::J_ARRAY) {
    if (OB_ISNULL(array_node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array node is null", K(ret));
    } else {
      array_node_->inc_path_cnt();
    }
  } else {
    if (OB_ISNULL(scalar_node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar node is null", K(ret));
    } else {
      scalar_node_->inc_path_cnt();
    }
  }
  return ret;
}

bool ObSemiJsonNode::contain_json_type(ObJsonNodeType json_type)
{
  bool contain_json_type = false;
  if (json_type == ObJsonNodeType::J_OBJECT) {
    contain_json_type = object_node_ != nullptr;
  } else if (json_type == ObJsonNodeType::J_ARRAY) {
    contain_json_type = array_node_ != nullptr;
  } else {
    contain_json_type = scalar_node_ != nullptr;
  }
  return contain_json_type;
}

bool ObSemiJsonNode::is_heterogeneous_column()
{
  int col_count = 0;
  if (OB_NOT_NULL(object_node_)) {
    if (object_node_->has_nop()) {
      col_count++;
    }
    if (object_node_->get_object_list().size() > 0) {
      col_count++;
    }
  }
  if (OB_NOT_NULL(array_node_)) {
    if (array_node_->has_nop()) {
      col_count++;
    }
    if (array_node_->get_array_list().size() > 0) {
      col_count++;
    }
  }
  if (OB_NOT_NULL(scalar_node_)) {
    col_count++;
  }
  return col_count > 1;
}

/**
 * ---------------------------------------- ObJsonBinVisitor ----------------------------------------
 */

int ObJsonBinVisitor::visit_value(ObSemiJsonNode *&json_node)
{
  int ret = OB_SUCCESS;
  switch (json_type_) {
    case ObJsonNodeType::J_OBJECT: {
      if (OB_ISNULL(json_node->object_node_)) {
        ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
        LOG_WARN("object node is null, need rebuild schema", K(ret));
      } else if (OB_FAIL(visit_object(json_node->object_node_))) {
        LOG_WARN("visit_object fail", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      if (OB_ISNULL(json_node->array_node_)) {
        ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
        LOG_WARN("array node is null, need rebuild schema", K(ret));
      } else if (OB_FAIL(visit_array(json_node->array_node_))) {
        LOG_WARN("visit_array fail", K(ret));
      }
      break;
    }
    default: {
      if (OB_ISNULL(json_node->scalar_node_)) {
        ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
        LOG_WARN("scalar node is null, need rebuild schema", K(ret));
      } else if (OB_FAIL(visit_scalar(json_node->scalar_node_))) {
        LOG_WARN("visit_scalar fail", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObJsonBinVisitor::flat_datums(const ObColDatums &datums)
{
  int ret = OB_SUCCESS;
  total_cnt_ = datums.count();
  for (int i = 0; OB_SUCC(ret) && i < datums.count(); ++i) {
    row_index_++;
    if (OB_FAIL(flat_datum(datums.at(i)))) {
      LOG_WARN("flat datum fail", K(ret), K(i));
    }
  }
  return ret;
}

int ObJsonBinVisitor::flat_datum(const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    ++null_cnt_;
    if (OB_FAIL(handle_null())) {
      LOG_WARN("handle null fail", K(ret), K(datum));
    }
  } else {
    // lob must have lob header in storage
    const ObLobCommon& lob_common = datum.get_lob_data();
    if (lob_common.is_mem_loc_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("found a mem locator", K(ret), K(lob_common));
    } else if (! lob_common.in_row_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("semstruct encoding do not support outrow", K(ret), K(lob_common));
    } else {
      ObString json_data(lob_common.get_byte_size(datum.len_), lob_common.get_inrow_data_ptr());
      reuse();
      if (OB_FAIL(visit(json_data))) {
        LOG_WARN("flat_json fail", K(ret), K(json_data));
      }
    }
  }
  return ret;
}


void ObJsonBinVisitor::reuse()
{
  ptr_ = nullptr;
  len_ = 0;
  pos_ = 0;
  meta_.reset();
}

void ObJsonBinVisitor::reset()
{
  ptr_ = nullptr;
  len_ = 0;
  pos_ = 0;
  meta_.reset();
}

int ObJsonBinVisitor::read_type()
{
  int ret = OB_SUCCESS;
  if (pos_ + sizeof(uint8_t) > len_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pos overflow len", K(ret), K(pos_), K(len_), KP(ptr_));
  } else {
    meta_.type_ = *reinterpret_cast<const uint8_t*>(ptr_ + pos_);
    json_type_ = meta_.json_type();
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_decimal()
{
  int ret = OB_SUCCESS;
  ObPrecision prec = -1;
  ObScale scale = -1;
  int64_t pos = pos_;
  if (OB_FAIL(serialization::decode_i16(ptr_, len_, pos, &prec))) {
    LOG_WARN("fail to deserialize decimal precision.", K(ret), KP(ptr_), K(len_), K(pos_), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(ptr_, len_, pos, &scale))) {
    LOG_WARN("fail to deserialize decimal scale.", K(ret), KP(ptr_), K(len_), K(pos_), K(pos));
  } else if (OB_FAIL(number_.deserialize(ptr_, len_, pos))) {
    LOG_WARN("failed to deserialize decimal data", K(ret), KP(ptr_), K(len_), K(pos_), K(pos));
  } else {
    prec_ = prec;
    scale_ = scale;
    meta_.bytes_ = pos - pos_;
    datum_.set_number(number_);
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_int()
{
  int ret = OB_SUCCESS;
  if (meta_.is_inline_vertype()) {
    uint64_t inline_val = 0;
    if (OB_FAIL(ObJsonVar::read_var(ptr_ + pos_, meta_.entry_size_, reinterpret_cast<int64_t*>(&inline_val)))) {
      LOG_WARN("read inline value fail", K(ret), K(meta_));
    } else {
      int_val_ = ObJsonVar::var_uint2int(inline_val, meta_.entry_size_);
      meta_.bytes_ = 0;
      datum_.set_int(int_val_);
    }
  } else {
    int64_t val = 0;
    int64_t pos = pos_;
    if (OB_FAIL(serialization::decode_vi64(ptr_, len_, pos, &val))) {
      LOG_WARN("decode int val failed.", K(ret), KP(ptr_), K(len_), K(pos_));
    } else {
      int_val_ = val;
      meta_.bytes_ = pos - pos_;
      datum_.set_int(int_val_);
    }
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_uint()
{
  int ret = OB_SUCCESS;
  if (meta_.is_inline_vertype()) {
    if (OB_FAIL(ObJsonVar::read_var(ptr_ + pos_, meta_.entry_size_, &uint_val_))) {
      LOG_WARN("read inline value fail", K(ret), K(meta_));
    } else {
      meta_.bytes_ = 0;
      datum_.set_uint(uint_val_);
    }
  } else {
    int64_t val = 0;
    int64_t pos = pos_;
    if (OB_FAIL(serialization::decode_vi64(ptr_, len_, pos, &val))) {
      LOG_WARN("decode uint val failed.", K(ret), KP(ptr_), K(len_), K(pos_));
    } else {
      uint_val_ = static_cast<uint64_t>(val);
      meta_.bytes_ = pos - pos_;
      datum_.set_uint(uint_val_);
    }
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_string()
{
  int ret = OB_SUCCESS;
  int64_t str_len = 0;
  int64_t offset = pos_ + sizeof(uint8_t);
  if (OB_FAIL(serialization::decode_vi64(ptr_, len_, offset, &str_len))) {
    LOG_WARN("decode string length fail", K(ret), KP(ptr_), K(len_), K(pos_), K(offset));
  } else if (offset + str_len > len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data buffer is not enough", K(ret),  K(ret), KP(ptr_), K(len_), K(pos_), K(offset), K(str_len));
  } else {
    meta_.set_element_count(static_cast<uint64_t>(str_len));
    meta_.bytes_ = offset - pos_ + str_len;
    meta_.str_data_offset_ = offset - pos_;
    data_ = ptr_ + offset;
    datum_.set_string(data_, str_len);
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_opaque()
{
  int ret = OB_SUCCESS;
  int64_t str_len = 0;
  int64_t offset = pos_ + sizeof(uint8_t);
  ObString data;
  // [vertype(uint8_t)][ObObjType(uint16_t)][length(uint64_t)][data]
  if (pos_ + sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint64_t) > len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data length is not enough for opaque len", K(ret), KP(ptr_), K(len_), K(pos_), K(offset));
  } else {
    meta_.field_type_ = static_cast<ObObjType>(*reinterpret_cast<const uint16_t*>(ptr_ + offset));
    offset += sizeof(uint16_t);
    str_len = *reinterpret_cast<const uint64_t*>(ptr_ + offset);
    offset += sizeof(uint64_t);
    meta_.set_element_count(static_cast<uint64_t>(str_len));
    meta_.bytes_ = offset - pos_ + str_len;
    meta_.str_data_offset_ = offset - pos_;
    data_ = ptr_ + offset;
    datum_.set_string(ptr_ + pos_ + sizeof(uint8_t), sizeof(uint16_t) + sizeof(uint64_t) + str_len);
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_boolean()
{
  int ret = OB_SUCCESS;
  if (meta_.is_inline_vertype()) {
    if (OB_FAIL(ObJsonVar::read_var(ptr_ + pos_, meta_.entry_size_, &uint_val_))) {
      LOG_WARN("read inline value fail", K(ret), K(meta_));
    } else {
      meta_.bytes_ = 0;
      datum_.set_uint(uint_val_);
    }
  } else {
    uint_val_ = static_cast<bool>(*(ptr_ + pos_));
    meta_.bytes_ = sizeof(bool);
    datum_.set_uint(uint_val_);
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_doc_header()
{
  int ret = OB_SUCCESS;
  const ObJsonBinDocHeader *header = nullptr;
  if (pos_ != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("doc header should be at the begining", K(ret), K(pos_), K(len_), KP(ptr_));
  } else if (pos_ + sizeof(ObJsonBinDocHeader) > len_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("data length is not enough for json bin doc header", K(ret), K(pos_), K(len_));
  } else if (OB_ISNULL(header = reinterpret_cast<const ObJsonBinDocHeader*>(ptr_ + pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header is null", K(ret), K(pos_), K(len_), KP(ptr_));
  } else if (header->extend_seg_offset_ != len_) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("may be partial update", K(ret), KPC(header));
  } else {
    use_lexicographical_order_ = header->use_lexicographical_order_;
    pos_ += sizeof(ObJsonBinDocHeader);
    visited_bytes_ += sizeof(ObJsonBinDocHeader);
  }
  return ret;
}

int ObJsonBinVisitor::deserialize_bin_header()
{
  int ret = OB_SUCCESS;
  ObString header_data;
  const ObJsonBinHeader *header = nullptr;
  uint64_t obj_size = 0;
  uint64_t element_count = 0;
  int offset = pos_;
  if (pos_ + sizeof(ObJsonBinHeader) > len_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("data length is not enough for json bin doc header", K(ret), K(pos_), K(len_));
  } else if (OB_ISNULL(header = reinterpret_cast<const ObJsonBinHeader*>(ptr_ + pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast header is null", K(ret), K(offset));
  } else if (OB_FALSE_IT(offset += sizeof(ObJsonBinHeader))) {
  } else if (OB_FAIL(ObJsonVar::read_var(ptr_ + offset, header->count_size_, &element_count))) {
    LOG_WARN("read element_count_ fail", K(ret), KPC(header));
  } else if (OB_FALSE_IT(offset += ObJsonVar::get_var_size(header->count_size_))) {
  } else if (OB_FAIL(ObJsonVar::read_var(ptr_ + offset, header->obj_size_size_, &obj_size))) {
    LOG_WARN("read obj_size_ fail", K(ret), KPC(header));
  } else {
    offset += ObJsonVar::get_var_size(header->obj_size_size_);
    meta_.set_type(header->type_);
    meta_.set_entry_var_type(header->entry_size_);
    meta_.set_element_count_var_type(header->count_size_);
    meta_.set_obj_size_var_type(header->obj_size_size_);
    meta_.set_is_continuous(header->is_continuous_);
    meta_.set_obj_size(obj_size);
    meta_.set_element_count(element_count);
    if (OB_FAIL(meta_.calc_entry_array())) {
      LOG_WARN("calc_entry_array fail", K(ret));
    } else {
      meta_.bytes_ = meta_.obj_size();
      visited_bytes_ += meta_.get_value_entry_offset(element_count);
    }
  }
  return ret;
}

int ObJsonBinVisitor::get_key_entry(const ObJsonBinMeta &meta, const char* buf_ptr, int index, ObString &key)
{
  int ret = OB_SUCCESS;
  uint64_t key_offset = 0;
  uint64_t key_len = 0;
  uint8_t var_type = meta.entry_var_type();
  uint64_t offset = meta.get_key_entry_offset(index);
  if (OB_FAIL(ObJsonVar::read_var(buf_ptr + offset, var_type, &key_offset))) {
    LOG_WARN("read key_offset fail", K(ret));
  } else if (OB_FAIL(ObJsonVar::read_var(buf_ptr + offset + ObJsonVar::get_var_size(var_type), var_type, &key_len))) {
    LOG_WARN("read key_len fail", K(ret));
  } else {
    key.assign_ptr(buf_ptr + key_offset, key_len);
  }
  return ret;
}

int ObJsonBinVisitor::get_value_entry(
    const ObJsonBinMeta &meta, const char* buf_ptr,
    int index, uint64_t &value_offset, uint8_t &value_type)
{
  int ret = OB_SUCCESS;
  uint8_t var_type = meta.entry_var_type();
  uint64_t offset = meta.get_value_entry_offset(index);
  if (OB_FAIL(ObJsonVar::read_var(buf_ptr + offset, var_type, &value_offset))) {
    LOG_WARN("read obj_size_ fail", K(ret), KP(buf_ptr), K(offset), K(var_type));
  } else {
    value_type = *reinterpret_cast<const uint8_t*>(buf_ptr + offset + ObJsonVar::get_var_size(var_type));
  }
  return ret;
}


int ObJsonBinVisitor::deserialize()
{
  int ret = OB_SUCCESS;
  ObJsonNodeType node_type = json_type_;
  switch (node_type) {
    case ObJsonNodeType::J_NULL: {
      datum_.set_null();
      meta_.bytes_ = meta_.is_inline_vertype() ? 0 : 1;
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      if (OB_FAIL(deserialize_decimal())) {
        LOG_WARN("fail to deserialize decimal", K(ret), KP(ptr_), K(len_), K(pos_));
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      if (OB_FAIL(deserialize_int())) {
        LOG_WARN("decode int val failed.", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG:  {
      if (OB_FAIL(deserialize_uint())) {
        LOG_WARN("decode uint val failed.", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      double_val_ = *reinterpret_cast<const double*>(ptr_ + pos_);
      meta_.bytes_ = sizeof(double);
      datum_.set_double(double_val_);
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      float_val_ = *reinterpret_cast<const float*>(ptr_ + pos_);
      meta_.bytes_ = sizeof(float);
      datum_.set_float(float_val_);
      break;
    }
    case ObJsonNodeType::J_STRING: {
      if (OB_FAIL(deserialize_string())) {
        LOG_WARN("deserialize_string fail", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      if (OB_FAIL(deserialize_boolean())) {
        LOG_WARN("deserialize_boolean fail", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_MYSQL_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      if (pos_ + sizeof(int32_t) > len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough for date.", K(ret), KP(ptr_), K(len_), K(pos_));
      } else {
        meta_.field_type_ = ObJsonBaseUtil::get_time_type(node_type);
        int_val_ = *reinterpret_cast<const int32_t*>(ptr_ + pos_);
        meta_.bytes_ = sizeof(int32_t);
        datum_.set_int(int_val_);
      }
      break;
    }
    case ObJsonNodeType::J_TIME: {
      if (pos_ + sizeof(int64_t) > len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough for time.", K(ret), KP(ptr_), K(len_), K(pos_));
      } else {
        meta_.field_type_ = ObTimeType;
        int_val_ = *reinterpret_cast<const int64_t*>(ptr_ + pos_);
        meta_.bytes_ = sizeof(int64_t);
        datum_.set_int(int_val_);
      }
      break;
    }
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_MYSQL_DATETIME:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      if (pos_ + sizeof(int64_t) > len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough for datetime.", K(ret), KP(ptr_), K(len_), K(pos_));
      } else {
        meta_.field_type_ = ObJsonBaseUtil::get_time_type(node_type);
        int_val_ = *reinterpret_cast<const int64_t*>(ptr_ + pos_);
        meta_.bytes_ = sizeof(int64_t);
        datum_.set_int(int_val_);
      }
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP: {
      if (pos_ + sizeof(int64_t) > len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough for timestamp.", K(ret), KP(ptr_), K(len_), K(pos_));
      } else {
        meta_.field_type_ = ObTimestampType;
        int_val_ = *reinterpret_cast<const int64_t*>(ptr_ + pos_);
        meta_.bytes_ = sizeof(int64_t);
        datum_.set_int(int_val_);
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      if (OB_FAIL(deserialize_opaque())) {
        LOG_WARN("deserialize_opaque fail", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid node type.", K(ret), K(node_type));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    visited_bytes_ += meta_.bytes_;
  }
  return ret;
}

int ObJsonBinVisitor::to_bin(ObJsonBin &bin)
{
  return bin.reset(meta_.type_, ObString(len_, ptr_), pos_, meta_.entry_var_type(), nullptr);
}

/**
 * ---------------------------------------- ObJsonDataFlatter ----------------------------------------
 */

ObStorageDatum ObJsonDataFlatter::NOP_DATUM = ObStorageDatum();

int ObJsonDataFlatter::init(const ObSemiSchemaAbstract *sub_schema, ObArray<ObColDatums *> &sub_col_datums)
{
  int ret = OB_SUCCESS;
  sub_schema_ = sub_schema;
  reset();
  sub_col_datums_ = &sub_col_datums;
  return ret;
}

void ObJsonDataFlatter::reset()
{
  spare_data_allocator_.reuse();
}

int ObJsonDataFlatter::visit(const ObString& data)
{
  int ret = OB_SUCCESS;
  reset();
  if (sub_schema_->has_spare_column() && OB_ISNULL(spare_col_ = OB_NEWx(ObJsonObject, &spare_data_allocator_, &spare_data_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc spare col fail", K(ret), "size", sizeof(ObJsonObject));
  } else if (OB_FAIL(do_visit(data))) {
    LOG_WARN("flat fail", K(ret));
  } else if (use_lexicographical_order_ != sub_schema_->use_lexicographical_order()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lexicographical_order is different", K(ret), K(use_lexicographical_order_), K(sub_schema_->use_lexicographical_order()));
  } else if (OB_NOT_NULL(spare_col_)) {
    ObJsonBuffer result_buffer(allocator_);
    ObString spare_col_data;
    ObDatum datum;
    if (spare_col_->element_count() == 0) {
      datum.set_null();
    } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_value(spare_col_, result_buffer, false/*enable_reserialize*/))) {
      LOG_WARN("serialize json tree fail", K(ret));
    } else if (OB_FAIL(result_buffer.get_result_string(spare_col_data))) {
      LOG_WARN("get result string fail", K(ret));
    } else {
      datum.set_string(spare_col_data);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sub_col_datums_->at(sub_col_datums_->count() - 1)->push_back(datum))) {
      LOG_WARN("add value fail", K(ret));
    }
    spare_col_->~ObJsonObject();
    spare_col_ = nullptr;
  }

  for (int i = 0; OB_SUCC(ret) && i < sub_schema_->get_store_column_cnt(); i++) {
    if (sub_col_datums_->at(i)->count() == row_index_ - 1) {
      if (OB_FAIL(sub_col_datums_->at(i)->push_back(NOP_DATUM))) {
        LOG_WARN("push back datum to sub_col_datums fail", K(ret), K(i), KPC(sub_schema_));
      }
    } else if (sub_col_datums_->at(i)->count() != row_index_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub_col_datums count is not match", K(ret), K(i), KPC(sub_schema_));
    }
  }
  return ret;
}

int ObJsonDataFlatter::copy_datum(ObDatum &src, ObDatum &dest)
{
  int ret = OB_SUCCESS;
  if (json_type_ != ObJsonNodeType::J_STRING) {
    const ObObjType obj_type = json_type_to_obj_type(json_type_);
    const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(obj_type)];
    const bool is_int_sc = store_class == ObIntSC || store_class == ObUIntSC;
    char* datum_ptr = nullptr;
    int64_t datum_size = sizeof(uint64_t);
    if (! is_int_sc) {
      if (OB_FAIL(dest.deep_copy(src, *allocator_))) {
        LOG_WARN("deep copy fail", K(ret), K(src));
      }
    } else if (OB_ISNULL(datum_ptr = static_cast<char*>(allocator_->alloc(datum_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), K(datum_size), K(src));
    } else {
      dest.ptr_ = datum_ptr;
      dest.pack_ = src.pack_;
      uint64_t value = 0;
      MEMCPY(&value, src.ptr_, src.len_);
      if (store_class == ObIntSC) {
        const int64_t type_store_size = get_type_size_map()[obj_type];
        uint64_t mask = INTEGER_MASK_TABLE[type_store_size];
        uint64_t reverse_mask = ~mask;
        value = value & mask;
        if (0 != reverse_mask && (value & (reverse_mask >> 1))) {
          value |= reverse_mask;
        }
      }
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(dest.ptr_), &value, datum_size);
    }
  } else {
    dest = src;
  }
  return ret;
}

int ObJsonDataFlatter::handle_null()
{
  int ret = OB_SUCCESS;
  int64_t sub_col_cnt = sub_col_datums_->count();
  ObDatum datum;
  datum.set_null();
  for (int i = 0 ; OB_SUCC(ret) && i < sub_col_cnt; ++i) {
    if (OB_FAIL(sub_col_datums_->at(i)->push_back(datum))) {
      LOG_WARN("add value fail", K(ret), K(i), K(sub_col_cnt));
    }
  }
  return ret;
}

int ObJsonDataFlatter::do_visit(const ObString& data)
{
  int ret = OB_SUCCESS;
  ptr_ = data.ptr();
  len_ = data.length();
  visited_bytes_ = 0;
  if (OB_FAIL(read_type())) {
    LOG_WARN("read type fail", K(ret));
  } else if (! ObJsonBin::is_doc_header(meta_.type_)) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("there is no json doc header", K(ret), K(meta_), K(pos_));
  } else if (OB_FAIL(deserialize_doc_header())) {
    LOG_WARN("deserialize_doc_header fail", K(ret));
  } else if (OB_FAIL(read_type())) {
    LOG_WARN("read type fail", K(ret));
  } else if (json_type_ != ObJsonNodeType::J_OBJECT && json_type_ != ObJsonNodeType::J_ARRAY) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("scalar json is not support encoding", K(ret), K(meta_), K(pos_));
  } else if (OB_FAIL(visit_value(base_node_))) {
    LOG_WARN("deserialize fail", K(ret));
  } else if (visited_bytes_ != data.length()) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("visited_bytes is not equal to data length, may be partial update, so do not trigger semistruct encoding",
      K(ret), K(visited_bytes_), K(data.length()));
  }
  return ret;
}

int ObJsonDataFlatter::gen_spare_key(const uint16_t col_id, ObString &key)
{
  int ret = OB_SUCCESS;
  if (min_data_version_ < DATA_VERSION_4_4_1_0) {
    uint32_t *key_id = nullptr;
    if (OB_ISNULL(key_id = reinterpret_cast<uint32_t*>(spare_data_allocator_.alloc(sizeof(uint32_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret));
    } else {
      *key_id = col_id;
      key.assign_ptr(reinterpret_cast<char*>(key_id), sizeof(uint32_t));
    }
  } else {
    uint16_t *key_id = nullptr;
    if (OB_ISNULL(key_id = reinterpret_cast<uint16_t*>(spare_data_allocator_.alloc(sizeof(uint16_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret));
    } else {
      *key_id = col_id;
      key.assign_ptr(reinterpret_cast<char*>(key_id), sizeof(uint16_t));
    }
  }
  return ret;
}

int ObJsonDataFlatter::handle_stop_container(uint16_t col_id, ObJsonNodeType json_type)
{
  int ret = OB_SUCCESS;
  datum_.reuse();
  ObJsonNode *node = nullptr;
  visited_bytes_ -= meta_.get_value_entry_offset(meta_.element_count());
  visited_bytes_ += meta_.obj_size();
  ObString key;
  if (OB_FAIL(ObJsonBaseFactory::alloc_node(spare_data_allocator_, ObJsonNodeType::J_SEMI_BIN, node))) {
    LOG_WARN("alloc node fail", K(ret), K(json_type_));
  } else {
    ObJsonSemiBin *semi_node = dynamic_cast<ObJsonSemiBin*>(node);
    semi_node->set_json_type(json_type);
    semi_node->set_value(ptr_ + pos_, meta_.obj_size());
    if (OB_FAIL(gen_spare_key(col_id, key))) {
      LOG_WARN("gen spare key fail", K(ret));
    } else if (OB_FAIL(spare_col_->add(key, node, true, true, false))) {
      LOG_WARN("add node fail", K(ret), K(key), K(node));
    }
  }
  pos_ += meta_.obj_size();
  return ret;
}

int ObJsonDataFlatter::visit_object(ObSemiSchemaObject *&json_object)
{
  int ret = OB_SUCCESS;
  ObJsonObjectList &object_list_ = json_object->get_object_list();
  if (OB_FAIL(deserialize_bin_header())) {
    LOG_WARN("init meta fail", K(ret));
  } else if (meta_.element_count() == 0 || json_object->get_need_stop_visit()) {
    if (OB_FAIL(handle_stop_container(json_object->get_col_id(), ObJsonNodeType::J_OBJECT))) {
      LOG_WARN("handle object stop container fail", K(ret), K(json_object->get_col_id()));
    }
  } else {
    const int64_t cur_pos = pos_;
    const ObJsonBinMeta cur_meta = meta_;
    const char *buf_ptr = ptr_ + pos_;
    ObList<ObSemiObjectPair, ObIAllocator>::iterator curr_iter = object_list_.begin();
    for (int i = 0; OB_SUCC(ret) && i < cur_meta.element_count(); ++i) {
      ObString key;
      uint64_t value_offset = 0;
      uint8_t value_type = 0;
      ObSemiJsonNode *child_semi_node = nullptr;
      if (OB_FAIL(get_key_entry(cur_meta, buf_ptr, i, key))) {
        LOG_WARN("get key fail", K(ret), K(i), K(cur_meta));
      } else if (OB_FAIL(get_value_entry(cur_meta, buf_ptr, i, value_offset, value_type))) {
        LOG_WARN("get value fail", K(ret), K(i), K(cur_meta));
      } else {
        int cmp = -1;
        while (curr_iter != object_list_.end() && (cmp = json_key_cmp_.compare(curr_iter->get_key(), key)) < 0) {
          curr_iter++;
        }
        if (cmp == 0) {
          child_semi_node = curr_iter->get_value();
          ObJsonNodeType value_json_type =
              ObJsonVerType::get_json_type(static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(value_type)));
          if (!child_semi_node->contain_json_type(value_json_type)) {
            ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
            LOG_WARN("schema not match", K(ret), K(key), K(curr_iter->get_key()), KPC(child_semi_node));
          } else {
            curr_iter++;
          }
        } else {
          ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
          LOG_WARN("schema not match", K(ret), K(key), K(curr_iter->get_key()), KPC(child_semi_node));
        }
        if (OB_SUCC(ret)) {
          meta_.type_ = value_type;
          meta_.entry_size_ = cur_meta.entry_size_;
          json_type_ = meta_.json_type();
          pos_ = cur_pos + (meta_.is_inline_vertype() ? cur_meta.get_value_entry_offset(i) : value_offset);
          if (OB_FAIL(visit_value(child_semi_node))) {
            LOG_WARN("deserialize fail", K(ret), K(pos_), K(value_offset), K(value_type));
          } else {
            visited_bytes_ += key.length();
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonDataFlatter::visit_array(ObSemiSchemaArray *&json_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_bin_header())) {
    LOG_WARN("init meta fail", K(ret));
  } else if (meta_.element_count() == 0 || json_array->get_need_stop_visit()) {
    if (OB_FAIL(handle_stop_container(json_array->get_col_id(), ObJsonNodeType::J_ARRAY))) {
      LOG_WARN("handle array stop container fail", K(ret), K(json_array->get_col_id()));
    }
  } else {
    const int64_t cur_pos = pos_;
    const ObJsonBinMeta cur_meta = meta_;
    const char *buf_ptr = ptr_ + pos_;
    ObJsonArrayList &array_list_ = json_array->get_array_list();
    ObSemiJsonNode *child_semi_node = nullptr;
    ObJsonArrayList::iterator curr_iter = array_list_.begin();
    for (int i = 0; OB_SUCC(ret) && i < cur_meta.element_count(); ++i, curr_iter++) {
      uint64_t value_offset = 0;
      uint8_t value_type = 0;
      ObSemiJsonNode *child_semi_node = nullptr;
      ObJsonNode *child_json_node = nullptr;
      if (OB_FAIL(get_value_entry(cur_meta, buf_ptr, i, value_offset, value_type))) {
        LOG_WARN("get value fail", K(ret), K(i), K(cur_meta));
      } else {
        if (curr_iter == array_list_.end()) {
          ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
          LOG_WARN("schema not match", K(ret), K(i), K(array_list_));
        } else {
          child_semi_node = *curr_iter;
          ObJsonNodeType value_json_type =
              ObJsonVerType::get_json_type(static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(value_type)));
          if (!child_semi_node->contain_json_type(value_json_type)) {
            ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
            LOG_WARN("schema not match", K(ret), K(i), KPC(child_semi_node));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          meta_.type_ = value_type;
          meta_.entry_size_ = cur_meta.entry_size_;
          json_type_ = meta_.json_type();
          pos_ = cur_pos + (meta_.is_inline_vertype() ? cur_meta.get_value_entry_offset(i) : value_offset);
          if (OB_FAIL(visit_value(child_semi_node))) {
            LOG_WARN("deserialize fail", K(ret), K(pos_), K(value_offset), K(value_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonDataFlatter::add_spare_col(ObSemiSchemaScalar *&scalar_node)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObJsonNode *node = nullptr;
  if (OB_FAIL(gen_spare_key(scalar_node->get_col_id(), key))) {
    LOG_WARN("gen spare key fail", K(ret), K(scalar_node->get_col_id()));
  } else if (json_type_ == ObJsonNodeType::J_NULL) {
    node = &ObSemiStructScalar::null_;
  } else if (OB_FAIL(ObJsonBaseFactory::alloc_node(spare_data_allocator_, json_type_, node))) {
    LOG_WARN("alloc node fail", K(ret), K(json_type_));
  } else if (json_type_ == ObJsonNodeType::J_DECIMAL) {
    ObJsonDecimal& decimal_node = static_cast<ObJsonDecimal&>(*node);
    ObDatum datum;
    // decimal has special binary format, need deep copy to avoid random memory
    if (OB_FAIL(datum.deep_copy(datum_, *allocator_))) {
      LOG_WARN("deep copy fail", K(ret), K(datum_));
    } else {
      decimal_node.set_value(datum.get_number());
      decimal_node.set_precision(prec_);
      decimal_node.set_scale(scale_);
    }
  } else if (OB_FAIL(datum_to_json(*node, datum_))) {
    LOG_WARN("set datum to json node fail", K(ret), K(datum_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(spare_col_->add(key, node, true, true, false))) {
    LOG_WARN("add fail", K(ret), K(key), K(node));
  }
  return ret;
}

int ObJsonDataFlatter::visit_scalar(ObSemiSchemaScalar *&scalar_node)
{
  int ret = OB_SUCCESS;
  datum_.reuse();
  ObDatum datum;
  if (OB_FAIL(deserialize())) {
    LOG_WARN("deserialize fail", K(ret), K(meta_));
  } else if (json_type_ != ObJsonNodeType::J_NULL && scalar_node->obj_type() != ObJsonType
                && json_type_ != scalar_node->json_type()) {
    ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
    LOG_WARN("schema not match", K(ret), K(json_type_), K(scalar_node->json_type()), K(scalar_node->obj_type()));
  } else if (!scalar_node->is_freq_column()) {
    if (OB_FAIL(add_spare_col(scalar_node))) {
      LOG_WARN("add_spare_col fail", K(ret), K(scalar_node));
    }
  } else if (OB_FAIL(copy_datum(datum_, datum))) {
    LOG_WARN("deep copy datum fail", K(ret));
  } else if (OB_FAIL(sub_col_datums_->at(scalar_node->get_col_id())->push_back(datum))) {
    LOG_WARN("add value fail", K(ret));
  }
  return ret;
}

/**
 * ---------------------------------------- ObJsonSchemaFlatter ----------------------------------------
 */
int ObJsonSchemaFlatter::init(const uint8_t freq_threshold)
{
  int ret = OB_SUCCESS;
  freq_col_threshold_ = freq_threshold;
  return ret;
}

int ObJsonSchemaFlatter::visit(const ObString& data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_visit(data))) {
    LOG_WARN("flat fail", K(ret));
  }
  return ret;
}

int ObJsonSchemaFlatter::add_schema_info(ObSemiSchemaNode &schema_node, ObSemiNewSchema &new_schema)
{
  int ret = OB_SUCCESS;
  ObSemiSchemaInfo info;
  info.is_freq_column_ = schema_node.is_freq_column();
  info.json_type_ = static_cast<uint8_t>(schema_node.json_type());
  info.obj_type_ = schema_node.obj_type();
  if (schema_node.json_type() == ObJsonNodeType::J_DECIMAL) {
    info.prec_ = schema_node.get_precision();
    info.scale_ = schema_node.get_scale();
  }
  if (info.is_freq_column_) {
    ObSemiSchemaScalar &scalar_node = dynamic_cast<ObSemiSchemaScalar&>(schema_node);
    if (schema_node.obj_type() == ObNullType) {
      if (schema_node.json_type() != ObJsonNodeType::J_NULL) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_node is not json null type", K(ret), K(schema_node));
      } else {
        info.obj_type_ = ObTinyIntType;
      }
    } else if (use_lexicographical_order_ && scalar_node.obj_type() == ObVarcharType &&
            scalar_node.get_max_len() == scalar_node.get_min_len()  && is_int_len(scalar_node.get_min_len())
            && OB_FAIL(info.handle_string_to_uint(scalar_node.get_min_len()))) {
      LOG_WARN("handle string to uint fail", K(ret));
    }

    if (OB_SUCC(ret)) {
      schema_node.set_col_id(freq_col_idx_++);
      schema_info_arr_.at(schema_node.get_col_id()) = info;
    }
  } else {
    schema_node.set_col_id(spare_col_idx_++);
    if (OB_FAIL(schema_info_arr_.push_back(info))) {
      LOG_WARN("add schema info fail", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::build_new_sub_schema(ObSemiNewSchema &new_schema, ObSemiSchemaArray *array_node, ObJsonNode &parent)
{
  int ret = OB_SUCCESS;
  ObJsonArrayList &array_list = array_node->get_array_list();
  for (ObJsonArrayList::iterator it = array_list.begin(); OB_SUCC(ret) && it != array_list.end(); ++it) {
    ObJsonNode *child_node = nullptr;
    if (OB_FAIL(build_schema_child_node(new_schema, *it, child_node))) {
      LOG_WARN("build sub schema fail", K(ret), KPC(child_node));
    } else if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node is null", K(ret));
    } else if (OB_FAIL(parent.array_append(child_node))) {
      LOG_WARN("add object path item fail", K(ret), KPC(child_node));
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::build_new_sub_schema(ObSemiNewSchema &new_schema, ObSemiSchemaObject *object_node, ObJsonNode &parent)
{
  int ret = OB_SUCCESS;
  ObJsonObjectList &object_list_ = object_node->get_object_list();
  for (ObJsonObjectList::iterator it = object_list_.begin(); OB_SUCC(ret) && it != object_list_.end(); ++it) {
    ObJsonNode *child_node = nullptr;
    if (OB_FAIL(build_schema_child_node(new_schema, it->get_value(), child_node))) {
      LOG_WARN("build sub schema fail", K(ret), KPC(child_node));
    } else if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node is null", K(ret));
    } else if (OB_FAIL(parent.object_add_v0(it->get_key(), child_node, true, true, false))) {
      LOG_WARN("add object path item fail", K(ret), K(it->get_key()));
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::handle_hete_col(ObSemiNewSchema &new_schema, ObSemiJsonNode *semi_node, ObJsonHeteCol &parent)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(semi_node->scalar_node_)) {
    ObJsonNode *child_node = nullptr;
    if (OB_FAIL(add_schema_info(*semi_node->scalar_node_, new_schema))) {
      LOG_WARN("add schema column fail", K(ret), KPC(semi_node->scalar_node_));
    } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonUint, allocator_, semi_node->scalar_node_->get_col_id()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(parent.array_append(child_node))) {
      LOG_WARN("add object path item fail", K(ret), KPC(child_node), KPC(semi_node));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(semi_node->object_node_)) {
    if (semi_node->object_node_->has_nop()) {
      ObJsonNode *child_node = nullptr;
      if (OB_FAIL(add_schema_info(*semi_node->object_node_, new_schema))) {
        LOG_WARN("add schema column fail", K(ret), KPC(semi_node->object_node_));
      } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonUint, allocator_, semi_node->object_node_->get_col_id()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(parent.array_append(child_node))) {
        LOG_WARN("add object path item fail", K(ret), KPC(child_node), KPC(semi_node));
      }
    }
    if (OB_SUCC(ret) && semi_node->object_node_->get_object_list().size() > 0) {
      ObJsonNode *child_node = nullptr;
      if (OB_ISNULL(child_node = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(build_new_sub_schema(new_schema, semi_node->object_node_, *child_node))) {
        LOG_WARN("build sub schema fail", K(ret), KPC(semi_node->object_node_), KPC(child_node));
      } else if (OB_FAIL(parent.array_append(child_node))) {
        LOG_WARN("add object path item fail", K(ret), KPC(child_node), KPC(semi_node));
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(semi_node->array_node_)) {
    if (semi_node->array_node_->has_nop()) {
      ObJsonNode *child_node = nullptr;
      if (OB_FAIL(add_schema_info(*semi_node->array_node_, new_schema))) {
        LOG_WARN("add schema column fail", K(ret), KPC(semi_node->array_node_));
      } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonUint, allocator_, semi_node->array_node_->get_col_id()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(parent.array_append(child_node))) {
        LOG_WARN("add object path item fail", K(ret), KPC(child_node), KPC(semi_node));
      }
    }
    if (OB_SUCC(ret) && semi_node->array_node_->get_array_list().size() > 0) {
      ObJsonNode *child_node = nullptr;
      if (OB_ISNULL(child_node = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(build_new_sub_schema(new_schema, semi_node->array_node_, *child_node))) {
        LOG_WARN("build sub schema fail", K(ret), KPC(semi_node->array_node_), KPC(child_node));
      } else if (OB_FAIL(parent.array_append(child_node))) {
        LOG_WARN("add object path item fail", K(ret), KPC(child_node), KPC(semi_node));
      }
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::build_sub_schema(ObSemiNewSchema &new_schema)
{
  int ret = OB_SUCCESS;
  ObJsonBuffer schema_buf(&new_schema.get_allocator());
  ObJsonNode *root_node = nullptr;
  ObString schema_str_buf;
  spare_col_idx_ = freq_column_count_;
  if (OB_FAIL(schema_info_arr_.prepare_allocate(freq_column_count_))) {
    LOG_WARN("prepare allocate fail", K(ret));
  } else if (base_node_->is_heterogeneous_column()) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("heterogeneous column not support in the first depth", K(ret));
  } else if (base_node_->contain_json_type(ObJsonNodeType::J_OBJECT) ||
                    base_node_->contain_json_type(ObJsonNodeType::J_ARRAY)) {
    if (OB_FAIL(build_schema_child_node(new_schema, base_node_, root_node))) {
      LOG_WARN("build sub schema fail", K(ret), KPC(base_node_->object_node_), KPC(root_node));
    } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_value(root_node, schema_buf, false/*enable_reserialize*/))) {
      LOG_WARN("serialize json binary fail", K(ret));
    } else if (OB_FAIL(schema_buf.get_result_string(schema_str_buf))) {
      LOG_WARN("get result string fail", K(ret));
    } else if (freq_column_count_ != freq_col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("freq column count not match", K(ret), K(freq_column_count_), K(freq_col_idx_));
    } else if (spare_col_idx_ == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("element count is 0", K(ret), K(freq_column_count_), KPC(base_node_));
    } else {
      new_schema.set_schema_buf(schema_str_buf);
      new_schema.set_element_cnt(spare_col_idx_);
      new_schema.set_freq_column_cnt(freq_column_count_);
      if (OB_FAIL(new_schema.set_schema_infos_ptr(schema_info_arr_))) {
        LOG_WARN("set schema infos fail", K(ret));
      }
    }
  } else {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("scalar node not support in the first depth", K(ret));
  }
  return ret;
}

int ObJsonSchemaFlatter::build_schema_child_node(ObSemiNewSchema &new_schema, ObSemiJsonNode *json_node, ObJsonNode *&child_node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(child_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node is not null", K(ret));
  } else if (json_node->is_heterogeneous_column()) {
    if (OB_ISNULL(child_node = OB_NEWx(ObJsonHeteCol, allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(handle_hete_col(new_schema, json_node, reinterpret_cast<ObJsonHeteCol&>(*child_node)))) {
      LOG_WARN("build sub schema fail", K(ret), KPC(json_node), KPC(child_node));
    }
  } else if (json_node->contain_json_type(ObJsonNodeType::J_OBJECT)) {
    if (json_node->object_node_->check_need_stop_visit(row_cnt_ - null_cnt_, freq_col_threshold_) || json_node->object_node_->has_nop()) {
      if (OB_FAIL(add_schema_info(*json_node->object_node_, new_schema))) {
        LOG_WARN("add schema column fail", K(ret), KPC(json_node->object_node_));
      } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonUint, allocator_, json_node->object_node_->get_col_id()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else if (json_node->object_node_->get_object_list().size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object list is empty", K(ret));
    } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(build_new_sub_schema(new_schema, json_node->object_node_, *child_node))) {
      LOG_WARN("build sub schema fail", K(ret), KPC(json_node->object_node_), KPC(child_node));
    }
  } else if (json_node->contain_json_type(ObJsonNodeType::J_ARRAY)) {
    if (json_node->array_node_->check_need_stop_visit(row_cnt_ - null_cnt_, freq_col_threshold_) || json_node->array_node_->has_nop()) {
      if (OB_FAIL(add_schema_info(*json_node->array_node_, new_schema))) {
        LOG_WARN("add schema column fail", K(ret), KPC(json_node->array_node_));
      } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonUint, allocator_, json_node->array_node_->get_col_id()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else if (json_node->array_node_->get_array_list().size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object list is empty", K(ret));
    } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(build_new_sub_schema(new_schema, json_node->array_node_, *child_node))) {
      LOG_WARN("build sub schema fail", K(ret), KPC(json_node->array_node_), KPC(child_node));
    }
  } else if (OB_FAIL(add_schema_info(*json_node->scalar_node_, new_schema))) {
    LOG_WARN("add schema column fail", K(ret), KPC(json_node->scalar_node_));
  } else if (OB_ISNULL(child_node = OB_NEWx(ObJsonUint, allocator_, json_node->scalar_node_->get_col_id()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  }
  return ret;
}

// ---------------------------- for old schema --------------------------------

int ObJsonSchemaFlatter::build_sub_schema(ObSemiSchemaObject *object_node, ObSemiStructSubSchema &result)
{
  int ret = OB_SUCCESS;
  ObJsonObjectList &object_list_ = object_node->get_object_list();
  if (object_node->has_nop() && OB_FAIL(result.add_spare_column(*this, object_node))) {
    LOG_WARN("add spare column fail", K(ret));
  }
  for (ObJsonObjectList::iterator it = object_list_.begin(); OB_SUCC(ret) && it != object_list_.end(); ++it) {
    if (OB_FAIL(path_.add_path_item(share::ObSubColumnPathItem::OBJECT, it->get_key()))) {
      LOG_WARN("add object path item fail", K(ret), K(it->get_key()));
    } else if (OB_FAIL(build_sub_schema(it->get_value(), result))) {
      LOG_WARN("build sub schema fail", K(ret), K(result));
    } else {
      path_.pop_back();
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::build_sub_schema(ObSemiSchemaArray *array_node, ObSemiStructSubSchema &result) {
  int ret = OB_SUCCESS;
  ObJsonArrayList &array_list_ = array_node->get_array_list();
  if (array_node->has_nop() && OB_FAIL(result.add_spare_column(*this, array_node))) {
    LOG_WARN("add spare column fail", K(ret));
  }
  int idx = 0;
  for (ObJsonArrayList::iterator it = array_list_.begin(); OB_SUCC(ret) && it != array_list_.end(); ++it, idx++) {
    if (OB_FAIL(path_.add_path_item(share::ObSubColumnPathItem::ARRAY, idx))) {
      LOG_WARN("add array path item fail", K(ret), K(idx));
    } else if (OB_FAIL(build_sub_schema(*it, result))) {
      LOG_WARN("build sub schema fail", K(ret), K(result));
    } else {
      path_.pop_back();
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::build_sub_schema(ObSemiSchemaScalar *scalar_node, ObSemiStructSubSchema &result) {
  int ret = OB_SUCCESS;
  if (!scalar_node->is_freq_column()) {
    if (OB_FAIL(result.add_spare_column(*this, scalar_node))) {
      LOG_WARN("add spare column fail", K(ret));
    }
  } else if (OB_FAIL(result.add_freq_column(*this, scalar_node))){
    LOG_WARN("add freq column fail", K(ret));
  }

  return ret;
}

int ObJsonSchemaFlatter::build_sub_schema(ObSemiJsonNode *json_node, ObSemiStructSubSchema &result)
{
  int ret = OB_SUCCESS;
  if (min_data_version_ < DATA_VERSION_4_4_1_0 && json_node->is_heterogeneous_column()) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("not support semistruct encoding json", K(ret), KPC(json_node));
  } else {
    if (OB_NOT_NULL(json_node->scalar_node_) && OB_FAIL(build_sub_schema(json_node->scalar_node_, result))) {
      LOG_WARN("build sub schema fail", K(ret), K(json_node->scalar_node_), K(result));
    }
    if (OB_SUCC(ret) && json_node->contain_json_type(ObJsonNodeType::J_OBJECT)) {
      if (!json_node->object_node_->check_need_stop_visit(row_cnt_ - null_cnt_, freq_col_threshold_)) {
        if (OB_FAIL(build_sub_schema(json_node->object_node_, result))) {
          LOG_WARN("build sub schema fail", K(ret), K(json_node->object_node_), K(result));
        }
      } else if (OB_FAIL(result.add_spare_column(*this, json_node->object_node_))) {
        LOG_WARN("add spare column fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && json_node->contain_json_type(ObJsonNodeType::J_ARRAY)) {
      if (!json_node->array_node_->check_need_stop_visit(row_cnt_ - null_cnt_, freq_col_threshold_)) {
        if (OB_FAIL(build_sub_schema(json_node->array_node_, result))) {
          LOG_WARN("build sub schema fail", K(ret), K(json_node->array_node_), K(result));
        }
      } else if (OB_FAIL(result.add_spare_column(*this, json_node->array_node_))) {
        LOG_WARN("add spare column fail", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::alloc_node(ObIAllocator &allocator, const uint8_t obj_type, ObSemiSchemaNode *&node)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType json_type =  ObJsonVerType::get_json_type(static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(obj_type)));
  if (json_type == ObJsonNodeType::J_OBJECT) {
    if (OB_ISNULL(node = OB_NEWx(ObSemiSchemaObject, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    }
  } else if (json_type == ObJsonNodeType::J_ARRAY) {
    if (OB_ISNULL(node = OB_NEWx(ObSemiSchemaArray, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    }
  } else {
    if (OB_ISNULL(node = OB_NEWx(ObSemiSchemaScalar, &allocator, json_type_to_obj_type(json_type), json_type))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::do_visit(const ObString& data)
{
  int ret = OB_SUCCESS;
  ptr_ = data.ptr();
  len_ = data.length();
  visited_bytes_ = 0;
  ObSemiSchemaNode *json_node = nullptr;
  if (OB_FAIL(read_type())) {
    LOG_WARN("read type fail", K(ret));
  } else if (! ObJsonBin::is_doc_header(meta_.type_)) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("there is no json doc header", K(ret), K(meta_), K(pos_));
  } else if (OB_FAIL(deserialize_doc_header())) {
    LOG_WARN("deserialize_doc_header fail", K(ret));
  } else if (OB_FAIL(read_type())) {
    LOG_WARN("read type fail", K(ret));
  } else if (json_type_ != ObJsonNodeType::J_OBJECT && json_type_ != ObJsonNodeType::J_ARRAY) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("scalar json is not support encoding", K(ret), K(meta_), K(pos_));
  } else if (!base_node_->contain_json_type(json_type_) && OB_FAIL(alloc_node(schema_alloc_, meta_.type_, json_node))) {
    LOG_WARN("alloc node fail", K(ret));
  } else if (OB_NOT_NULL(json_node) && FALSE_IT(base_node_->set_node(json_node))) {
  } else if (OB_ISNULL(json_node) && OB_FAIL(base_node_->inc_path_cnt(json_type_))) {
    LOG_WARN("inc path cnt fail", K(ret));
  } else if (OB_FAIL(visit_value(base_node_))) {
    LOG_WARN("deserialize fail", K(ret));
  } else if (visited_bytes_ != data.length()) {
    ret = OB_NOT_SUPPORT_SEMISTRUCT_ENCODE;
    LOG_WARN("visited_bytes is not equal to data length, may be partial update, so do not trigger semistruct encoding",
      K(ret), K(visited_bytes_), K(data.length()));
  } else {
    json_key_cmp_.use_lexicographical_order_ = use_lexicographical_order_;
  }
  return ret;
}

int ObJsonSchemaFlatter::visit_object(ObSemiSchemaObject *&json_object)
{
  int ret = OB_SUCCESS;
  ObJsonObjectList &object_list_ = json_object->get_object_list();
  if (OB_FAIL(deserialize_bin_header())) {
    LOG_WARN("init meta fail", K(ret));
  } else if (meta_.element_count() == 0) {
    json_object->set_has_nop();
  } else {
    const int64_t cur_pos = pos_;
    const ObJsonBinMeta cur_meta = meta_;
    const char *buf_ptr = ptr_ + pos_;
    ObList<ObSemiObjectPair, ObIAllocator>::iterator curr_iter = object_list_.begin();
    for (int i = 0; OB_SUCC(ret) && i < cur_meta.element_count(); ++i) {
      ObString key;
      uint64_t value_offset = 0;
      uint8_t value_type = 0;
      ObSemiJsonNode *child_semi_node = nullptr;
      ObSemiSchemaNode *child_json_node = nullptr;
      if (OB_FAIL(get_key_entry(cur_meta, buf_ptr, i, key))) {
        LOG_WARN("get key fail", K(ret), K(i), K(cur_meta));
      } else if (OB_FAIL(get_value_entry(cur_meta, buf_ptr, i, value_offset, value_type))) {
        LOG_WARN("get value fail", K(ret), K(i), K(cur_meta));
      } else {
        int cmp = -1;
        while (curr_iter != object_list_.end() && (cmp = json_key_cmp_.compare(curr_iter->get_key(), key)) < 0) {
          curr_iter++;
        }
        if (cmp == 0) {
          // When equal, check if it is a heterogeneous column.
          // If it does not contain child nodes, generate a child node and continue traversal.
          child_semi_node = curr_iter->get_value();
          ObJsonNodeType value_json_type =
              ObJsonVerType::get_json_type(static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(value_type)));
          if (child_semi_node->contain_json_type(value_json_type)) {
            if (OB_FAIL(child_semi_node->inc_path_cnt(value_json_type))) {
              LOG_WARN("inc path cnt fail", K(ret));
            }
          } else if (OB_FAIL(alloc_node(schema_alloc_, value_type, child_json_node))) {
            LOG_WARN("alloc node fail", K(ret), K(value_type));
          } else {
            child_semi_node->set_node(child_json_node);
          }
          curr_iter++;
        } else if (OB_ISNULL(child_semi_node = OB_NEWx(ObSemiJsonNode, &schema_alloc_, schema_alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory not enough", K(ret));
        } else if (OB_FAIL(alloc_node(schema_alloc_, value_type, child_json_node))) {
          LOG_WARN("alloc node fail", K(ret), K(value_type));
        } else {
          child_semi_node->set_node(child_json_node);
          ObString copy_key;
          if (OB_FAIL(ob_write_string(schema_alloc_, key, copy_key))) {
            LOG_WARN("copy string fail", K(ret), K(key));
          } else if (OB_FAIL(object_list_.insert(curr_iter, ObSemiObjectPair(copy_key, child_semi_node)))) {
            LOG_WARN("insert fail", K(ret), K(key));
          }
        }
        if (OB_SUCC(ret)) {
          meta_.type_ = value_type;
          meta_.entry_size_ = cur_meta.entry_size_;
          json_type_ = meta_.json_type();
          pos_ = cur_pos + (meta_.is_inline_vertype() ? cur_meta.get_value_entry_offset(i) : value_offset);
          if (OB_FAIL(visit_value(child_semi_node))) {
            LOG_WARN("deserialize fail", K(ret), K(pos_), K(value_offset), K(value_type));
          } else {
            visited_bytes_ += key.length();
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::visit_array(ObSemiSchemaArray *&json_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_bin_header())) {
    LOG_WARN("init meta fail", K(ret));
  } else if (meta_.element_count() == 0) {
    json_array->set_has_nop();
  } else {
    const int64_t cur_pos = pos_;
    const ObJsonBinMeta cur_meta = meta_;
    const char *buf_ptr = ptr_ + pos_;
    ObJsonArrayList &array_list_ = json_array->get_array_list();
    ObSemiJsonNode *child_semi_node = nullptr;
    ObJsonArrayList::iterator curr_iter = array_list_.begin();
    for (int i = 0; OB_SUCC(ret) && i < cur_meta.element_count(); ++i) {
      uint64_t value_offset = 0;
      uint8_t value_type = 0;
      ObSemiJsonNode *child_semi_node = nullptr;
      ObSemiSchemaNode *child_json_node = nullptr;
      if (OB_FAIL(get_value_entry(cur_meta, buf_ptr, i, value_offset, value_type))) {
        LOG_WARN("get value fail", K(ret), K(i), K(cur_meta));
      } else {
        if (curr_iter == array_list_.end()) {
          // add new node
          if (OB_ISNULL(child_semi_node = OB_NEWx(ObSemiJsonNode, &schema_alloc_, schema_alloc_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("memory not enough", K(ret));
          } else if (OB_FAIL(alloc_node(schema_alloc_, value_type, child_json_node))) {
            LOG_WARN("alloc node fail", K(ret), K(value_type));
          } else if (FALSE_IT(child_semi_node->set_node(child_json_node))) {
          } else if (OB_FAIL(array_list_.insert(curr_iter, child_semi_node))) {
            LOG_WARN("insert fail", K(ret), K(i));
          }
        } else {
          child_semi_node = *curr_iter;
          curr_iter++;
          ObJsonNodeType value_json_type =
              ObJsonVerType::get_json_type(static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(value_type)));
          if (child_semi_node->contain_json_type(value_json_type)) {
            if (OB_FAIL(child_semi_node->inc_path_cnt(value_json_type))) {
              LOG_WARN("inc path cnt fail", K(ret));
            }
          } else if (OB_FAIL(alloc_node(schema_alloc_, value_type, child_json_node))) {
            LOG_WARN("alloc node fail", K(ret), K(value_type));
          } else {
            child_semi_node->set_node(child_json_node);
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          meta_.type_ = value_type;
          meta_.entry_size_ = cur_meta.entry_size_;
          json_type_ = meta_.json_type();
          pos_ = cur_pos + (meta_.is_inline_vertype() ? cur_meta.get_value_entry_offset(i) : value_offset);
          if (OB_FAIL(visit_value(child_semi_node))) {
            LOG_WARN("deserialize fail", K(ret), K(pos_), K(value_offset), K(value_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaFlatter::visit_scalar(ObSemiSchemaScalar *&scalar_node)
{
  int ret = OB_SUCCESS;
  datum_.reuse();
  if (OB_FAIL(deserialize())) {
    LOG_WARN("deserialize fail", K(ret), K(meta_));
  } else {
    if (json_type_ == scalar_node->json_type()) {
      if (json_type_ == ObJsonNodeType::J_DECIMAL &&
            scalar_node->get_precision() == INT16_MIN && scalar_node->get_scale() == INT16_MIN) {
        scalar_node->set_precision_and_scale(get_decimal_precision(), get_decimal_scale());
      } else if (json_type_ == ObJsonNodeType::J_DECIMAL &&
          (scalar_node->get_precision() != get_decimal_precision() || scalar_node->get_scale() != get_decimal_scale())) {
        scalar_node->set_obj_type(ObJsonType);
        scalar_node->set_json_type(ObJsonNodeType::J_DECIMAL);
        if (scalar_node->is_freq_column()) {
          freq_column_count_--;
          scalar_node->set_freq_column(false);
        }
        scalar_node->set_min_len(0);
        scalar_node->set_max_len(0);
      } else if (json_type_ == ObJsonNodeType::J_STRING) {
        if (scalar_node->get_min_len() == -1) {
          scalar_node->set_min_len(datum_.len_);
          scalar_node->set_max_len(datum_.len_);
        } else {
          scalar_node->set_min_len(OB_MIN(scalar_node->get_min_len(), datum_.len_));
          scalar_node->set_max_len(OB_MAX(scalar_node->get_max_len(), datum_.len_));
        }
      }
    } else if (scalar_node->obj_type() != ObJsonType) {
      if (scalar_node->json_type() == ObJsonNodeType::J_NULL) {
        scalar_node->set_json_type(json_type_);
        scalar_node->set_obj_type(json_type_to_obj_type(json_type_));
      } else if (json_type_ == ObJsonNodeType::J_NULL) {
      } else {
        scalar_node->set_obj_type(ObJsonType);
        scalar_node->set_json_type(ObJsonNodeType::J_DECIMAL);
        if (scalar_node->is_freq_column()) {
          freq_column_count_--;
          scalar_node->set_freq_column(false);
        }
        scalar_node->set_min_len(0);
        scalar_node->set_max_len(0);
      }
    }
    if (!scalar_node->is_freq_column() && scalar_node->obj_type() != ObJsonType) {
      if (need_store_as_freq_column(scalar_node->get_path_cnt())) {
        scalar_node->set_freq_column(true);
        freq_column_count_++;
      }
    }
  }
  return ret;
}


/**
 * ---------------------------------------- ObSemiStructSubColumn ----------------------------------------
 */

int64_t ObSemiStructSubColumn::get_encode_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(json_type_);
  OB_UNIS_ADD_LEN(obj_type_);
  OB_UNIS_ADD_LEN(sub_col_id_);
  len += path_.get_encode_size();
  if (json_type_ == ObJsonNodeType::J_DECIMAL) {
    OB_UNIS_ADD_LEN(prec_);
    OB_UNIS_ADD_LEN(scale_);
  }
  return len;
}

int ObSemiStructSubColumn::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(json_type_);
  OB_UNIS_ENCODE(obj_type_);
  OB_UNIS_ENCODE(sub_col_id_);
  if (OB_SUCC(ret) && OB_FAIL(path_.encode(buf, buf_len, pos))) {
    LOG_WARN("encode sub column fail", K(ret), K(pos), K(buf_len));
  }
  if (OB_SUCC(ret) && json_type_ == ObJsonNodeType::J_DECIMAL) {
    OB_UNIS_ENCODE(prec_);
    OB_UNIS_ENCODE(scale_);
  }
  return ret;
}

int ObSemiStructSubColumn::decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(json_type_);
  OB_UNIS_DECODE(obj_type_);
  OB_UNIS_DECODE(sub_col_id_);
  if (OB_SUCC(ret) && OB_FAIL(path_.decode(buf, data_len, pos))) {
    LOG_WARN("decode sub column fail", K(ret), K(pos), K(data_len));
  }
  if (OB_SUCC(ret) && json_type_ == ObJsonNodeType::J_DECIMAL) {
    OB_UNIS_DECODE(prec_);
    OB_UNIS_DECODE(scale_);
  }
  return ret;
}

int ObSemiStructSubColumn::init(const share::ObSubColumnPath& path, const ObJsonNodeType json_type, const ObObjType obj_type, const int64_t sub_col_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(path_.assign(path))) {
    LOG_WARN("assign path item fail", K(ret));
  } else {
    json_type_ = json_type;
    obj_type_ = obj_type;
    sub_col_id_ = sub_col_id;
  }
  return ret;
}

int ObSemiStructSubColumn::deep_copy(ObIAllocator& allocator, const ObSemiStructSubColumn &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(path_.deep_copy(allocator, other.path_))) {
    LOG_WARN("deep_copy path fail", K(ret), K(other));
  } else {
    json_type_ = other.json_type_;
    obj_type_ = other.obj_type_;
    sub_col_id_ = other.sub_col_id_;
    flags_ = other.flags_;
    prec_ = other.prec_;
    scale_ = other.scale_;
  }
  return ret;
}

/**
 * ---------------------------------------- ObSubSchemaKeyDict ----------------------------------------
 */

void ObSubSchemaKeyDict::reset()
{
  array_.reset();
}

int ObSubSchemaKeyDict::get(const ObString &key, int64_t &id) const
{
  int ret = OB_SUCCESS;
  ObJsonKeyCompare comparator(use_lexicographical_order_);
  ObString cur_key;
  bool is_found = false;
  int64_t low = 0;
  int64_t high = array_.count() - 1;
  // do binary search
  while (low <= high) {
    int64_t mid = low + (high - low) / 2;
    cur_key = array_.at(mid);
    int compare_result = comparator.compare(cur_key, key);
    if (compare_result == 0) {
      id = mid;
      is_found = true;
      break;
    } else if (compare_result > 0) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  ret = (ret == OB_SUCCESS && !is_found) ? OB_SEARCH_NOT_FOUND : ret;
  return ret;
}

int ObSubSchemaKeyDict::get(const int64_t id, ObString &key) const
{
  int ret = OB_SUCCESS;
  if (array_.count() <= id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("id too large", K(ret), K(id), "count", array_.count());
  } else {
    key = array_[id];
  }
  return ret;
}

int64_t ObSubSchemaKeyDict::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "{");
  int64_t size = array_.count();
  common::databuff_print_kv(buf, buf_len, pos, "size", size);
  common::databuff_printf(buf, buf_len, pos, ", ");

  if (size > 0) {
    common::databuff_printf(buf, buf_len, pos, "{");
  }
  for(int i = 0; i < array_.count();  ++i) {
    const ObString& key = array_.at(i);
    if (i > 0) {
      common::databuff_printf(buf, buf_len, pos, ", ");
    }
    common::databuff_printf(buf, buf_len, pos, "{");
    common::databuff_print_kv(buf, buf_len, pos, "id", i);
    common::databuff_printf(buf, buf_len, pos, ", ");
    common::databuff_print_kv(buf, buf_len, pos, "key_len", key.length());
    common::databuff_print_kv(buf, buf_len, pos, ", key", key);
    common::databuff_printf(buf, buf_len, pos, "}");
  }
  if (size > 0) {
    common::databuff_printf(buf, buf_len, pos, "}");
  }
  common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

OB_DEF_SERIALIZE_SIZE(ObSubSchemaKeyDict)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(array_);
  return len;
}

OB_DEF_SERIALIZE(ObSubSchemaKeyDict)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(array_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSubSchemaKeyDict)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(array_);
  return ret;
}

/**
 * ---------------------------------------- ObSemiSchemaAbstract ----------------------------------------
 */

int ObSemiSchemaAbstract::check_can_pushdown(const sql::ObSemiStructWhiteFilterNode &filter_node, uint16_t &sub_col_idx, bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  const share::ObSubColumnPath &col_path = filter_node.get_sub_col_path();
  const sql::ObExpr *root_expr = filter_node.expr_;
  const sql::ObExpr *json_expr = root_expr->args_[0];
  ObObjType obj_type(ObMaxType);
  if (OB_FAIL(get_column_id(col_path, sub_col_idx))) {
    if (OB_SEARCH_NOT_FOUND != ret) {
      LOG_WARN("get sub column fail", K(ret), K(col_path), KPC(this));
    } else {
      ret = OB_SUCCESS; // override ret
    }
  } else if (sub_col_idx < 0 || sub_col_idx >= get_store_column_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column id is invalid", K(ret), K(sub_col_idx));
  } else if (!is_freq_column(sub_col_idx)) {
    LOG_INFO("pushdown not support for spare json sub column", K(filter_node), K(sub_col_idx));
  } else if (OB_FAIL(get_sub_column_type(sub_col_idx, obj_type))) {
    LOG_WARN("get sub column type fail", K(ret), K(sub_col_idx));
  } else if (obj_type != json_expr->datum_meta_.type_) {
    LOG_INFO("json sub column is different from json expr", K(sub_col_idx), K(json_expr->datum_meta_.type_), K(filter_node));
  } else {
    can_pushdown = true;
  }
  return ret;
}


/**
 * ---------------------------------------- ObSemiNewSchema ----------------------------------------
 */

int ObSemiSchemaInfo::handle_string_to_uint(const int32_t len)
{
  int ret = OB_SUCCESS;
  if (ObJsonNodeType(json_type_) != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column is not json null type", K(ret), K(json_type_));
  } else {
    if (sizeof(uint8_t) == len) {
      obj_type_ =  ObUTinyIntType;
    } else if (sizeof(uint16_t) == len) {
      obj_type_ = ObUSmallIntType;
    } else if (sizeof(uint32_t) == len) {
      obj_type_ = ObUInt32Type;
    } else if (sizeof(uint64_t) == len) {
      obj_type_ = ObUInt64Type;
    }
  }
  return ret;
}

void ObSemiNewSchema::reset()
{
  schema_header_ = 0;
  semi_schema_infos_ = nullptr;
  schema_buf_.reset();
  ObSemiSchemaAbstract::reset();
}

int ObSemiNewSchema::set_schema_infos_ptr(ObSEArray<ObSemiSchemaInfo, 16> &schema_info_arr)
{
  int ret = OB_SUCCESS;
  ObSemiSchemaInfo *tmp_schema_infos_ptr = nullptr;
  if (element_cnt_ != schema_info_arr.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid element cnt", K(ret), K(element_cnt_), K(schema_info_arr.count()));
  } else if (OB_ISNULL(tmp_schema_infos_ptr =
                     reinterpret_cast<ObSemiSchemaInfo *>(allocator_.alloc(sizeof(ObSemiSchemaInfo) * element_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    for (int i = 0; i < element_cnt_; i++) {
      tmp_schema_infos_ptr[i] = schema_info_arr.at(i);
    }
    semi_schema_infos_ = tmp_schema_infos_ptr;
  }
  return ret;
}

int ObSemiNewSchema::get_sub_column_type(const uint16_t column_idx, ObObjType &type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(semi_schema_infos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semi_schema_infos_ is null", K(ret));
  } else if (column_idx > freq_column_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(column_idx));
  } else if (column_idx == freq_column_cnt_) {
    type = ObJsonType;
  } else {
    type = ObObjType(semi_schema_infos_[column_idx].obj_type_);
  }
  return ret;
}

int ObSemiNewSchema::get_sub_column_json_type(const uint16_t sub_col_id, ObJsonNodeType &type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(semi_schema_infos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semi_schema_infos_ is null", K(ret));
  } else if (sub_col_id >= element_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(sub_col_id));
  } else {
    type = semi_schema_infos_[sub_col_id].json_type();
  }
  return ret;
}

int ObSemiNewSchema::get_column_id(const share::ObSubColumnPath& path, uint16_t &sub_col_idx) const
{
  int ret = OB_SUCCESS;
  uint16_t depth = path.get_path_item_count();
  uint16_t now_depth = 0;
  ObArenaAllocator tmp_allocator("NewSchemaTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObJsonBin json_bin = ObJsonBin(schema_buf_.ptr(), schema_buf_.length(), &tmp_allocator);
  ObIJsonBase *parent_node = &json_bin;
  if (OB_FAIL(json_bin.reset_iter())) {
    LOG_WARN("reset iter fail", K(ret));
  }
  while (OB_SUCC(ret) && depth != now_depth) {
    ObIJsonBase *child_node = nullptr;
    if (parent_node->json_type() == ObJsonNodeType::J_OBJECT) {
      ObIJsonBase *value = nullptr;
      if (path.get_path_item(now_depth).type_ != share::ObSubColumnPathItem::OBJECT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column path type is not object", K(ret), K(parent_node->json_type()));
      } else if (OB_FAIL(parent_node->get_object_value(path.get_path_item(now_depth).key_, child_node))) {
        LOG_WARN("get object value failed", K(ret), K(path), K(now_depth));
      } else if (OB_ISNULL(child_node)) {
        ret = OB_SEARCH_NOT_FOUND;
        LOG_WARN("get object value failed", K(ret), K(path), K(now_depth));
      } else {
        parent_node = child_node;
        now_depth++;
      }
    } else if (parent_node->json_type() == ObJsonNodeType::J_ARRAY) {
      if (path.get_path_item(now_depth).type_ != share::ObSubColumnPathItem::ARRAY) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column path type is not array", K(ret), K(parent_node->json_type()));
      } else if (OB_FAIL(parent_node->get_array_element(path.get_path_item(now_depth).array_idx_, child_node))) {
        LOG_WARN("get array value failed", K(ret), K(path), K(now_depth));
      } else if (OB_ISNULL(child_node)) {
        ret = OB_SEARCH_NOT_FOUND;
        LOG_WARN("get array value failed", K(ret), K(path), K(now_depth));
      } else {
        parent_node = child_node;
        now_depth++;
      }
    } else if (parent_node->json_type() == ObJsonNodeType::J_SEMI_HETE_COL) {
      int child_cnt = parent_node->element_count();
      int index = 0;
      for (; OB_SUCC(ret) && index < child_cnt; index++) {
        if (OB_FAIL(parent_node->get_array_element(index, child_node))) {
          LOG_WARN("get hete col value failed", K(ret), K(path), K(now_depth));
        } else if (child_node->json_type() == ObJsonNodeType::J_OBJECT &&
                        path.get_path_item(now_depth).type_ == share::ObSubColumnPathItem::OBJECT) {
          if (OB_FAIL(child_node->get_object_value(path.get_path_item(now_depth).key_, parent_node))) {
            LOG_WARN("get hete col value failed", K(ret), K(path), K(now_depth));
          } else {
            now_depth++;
            break;
          }
        } else if (child_node->json_type() == ObJsonNodeType::J_ARRAY &&
                        path.get_path_item(now_depth).type_ == share::ObSubColumnPathItem::ARRAY) {
          if (OB_FAIL(child_node->get_array_element(path.get_path_item(now_depth).array_idx_, parent_node))) {
            LOG_WARN("get hete col value failed", K(ret), K(path), K(now_depth));
          } else {
            now_depth++;
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (index == child_cnt) {
        ret = OB_SEARCH_NOT_FOUND;
        LOG_WARN("not find in hete col", K(ret), K(path), K(now_depth));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json bin type is not object or array or hete col", K(ret), K(parent_node->json_type()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (parent_node->json_type() == ObJsonNodeType::J_SEMI_HETE_COL) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("heterogeneous columns are not support as pushed-down query conditions for filtering.", K(ret), K(path), KPC(this));
  } else {
    sub_col_idx = parent_node->get_uint();
  }
  return ret;
}

int ObSemiNewSchema::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(version_);
  OB_UNIS_ENCODE(schema_header_);
  MEMCPY(buf + pos, semi_schema_infos_, sizeof(ObSemiSchemaInfo) * element_cnt_);
  pos += sizeof(ObSemiSchemaInfo) * element_cnt_;
  OB_UNIS_ENCODE(schema_buf_);
  return ret;
}

int ObSemiNewSchema::decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(version_);
  OB_UNIS_DECODE(schema_header_);
  semi_schema_infos_ = reinterpret_cast<const ObSemiSchemaInfo*>(buf + pos);
  pos += sizeof(ObSemiSchemaInfo) * element_cnt_;
  LST_DO_CODE(OB_UNIS_DECODE, schema_buf_);
  if (OB_SUCC(ret)) is_inited_ = true;
  return ret;
}

int64_t ObSemiNewSchema::get_encode_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(version_);
  OB_UNIS_ADD_LEN(schema_header_);
  len += sizeof(ObSemiSchemaInfo) * element_cnt_;
  OB_UNIS_ADD_LEN(schema_buf_);
  return len;
}

/**
 * ---------------------------------------- ObSemiStructSubSchema ----------------------------------------
 */

int64_t ObSemiStructSubSchema::get_encode_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(version_);
  OB_UNIS_ADD_LEN(flags_);
  if (has_key_dict_) OB_UNIS_ADD_LEN(key_dict_);
  int64_t column_count = columns_.count();
  OB_UNIS_ADD_LEN(column_count);
  for (int64_t i = 0; i < column_count; ++i) {
    len += columns_[i].get_encode_size();
  }
  int64_t spare_column_count = spare_columns_.count();
  OB_UNIS_ADD_LEN(spare_column_count);
  for (int64_t i = 0; i < spare_column_count; ++i) {
    len += spare_columns_[i].get_encode_size();
  }
  return len;
}

int ObSemiStructSubSchema::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(version_);
  OB_UNIS_ENCODE(flags_);
  if (OB_SUCC(ret) && has_key_dict_) OB_UNIS_ENCODE(key_dict_);
  int64_t column_count = columns_.count();
  OB_UNIS_ENCODE(column_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    if (OB_FAIL(columns_[i].encode(buf, buf_len, pos))) {
      LOG_WARN("encode failed", K(ret), K(i), K(column_count), K(pos), K(buf_len));
    }
  }
  int64_t spare_column_count = spare_columns_.count();
  OB_UNIS_ENCODE(spare_column_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < spare_column_count; ++i) {
    if (OB_FAIL(spare_columns_[i].encode(buf, buf_len, pos))) {
      LOG_WARN("encode spare_columns failed", K(ret), K(i), K(spare_column_count), K(pos), K(buf_len));
    }
  }
  return ret;
}

int ObSemiStructSubSchema::decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(version_);
  OB_UNIS_DECODE(flags_);
  if (OB_SUCC(ret) && has_key_dict_) OB_UNIS_DECODE(key_dict_);
  if (OB_SUCC(ret)) key_dict_.set_use_lexicographical_order(use_lexicographical_order_);
  int64_t column_count = 0;
  OB_UNIS_DECODE(column_count);
  if (OB_SUCC(ret) && OB_FAIL(columns_.prepare_allocate(column_count))) {
    LOG_WARN("fail to allocate space", K(ret), K(column_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    if (OB_FAIL(columns_.at(i).decode(buf, data_len, pos))) {
      LOG_WARN("decode failed", K(ret), K(i), K(column_count), K(pos), K(data_len));
    }
  }
  int64_t spare_column_count = 0;
  OB_UNIS_DECODE(spare_column_count);
  if (OB_SUCC(ret) && OB_FAIL(spare_columns_.prepare_allocate(spare_column_count))) {
    LOG_WARN("fail to allocate space", K(ret), K(spare_column_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < spare_column_count; ++i) {
    if (OB_FAIL(spare_columns_.at(i).decode(buf, data_len, pos))) {
      LOG_WARN("decode spare_columns failed", K(ret), K(i), K(spare_column_count), K(pos), K(data_len));
    }
  }
  if (OB_SUCC(ret)) is_inited_ = true;
  return ret;
}

void ObSemiStructSubSchema::reset()
{
  flags_ = 0;
  key_dict_.reset();
  columns_.reset();
  spare_columns_.reset();
  spare_col_.reset();
  ObSemiSchemaAbstract::reset();
}

int ObSemiStructSubSchema::get_sub_column_type(const uint16_t column_idx, ObObjType &type) const
{
  int ret = OB_SUCCESS;
  const ObSemiStructSubColumn *sub_column = nullptr;
  if (OB_FAIL(get_store_column(column_idx, sub_column))) {
    LOG_WARN("get sub column fail", K(ret), K(column_idx), KPC(this));
  } else {
    type = sub_column->get_obj_type();
  }
  return ret;
}

int ObSemiStructSubSchema::get_store_column(const int64_t idx, const ObSemiStructSubColumn*& sub_column) const
{
  int ret = OB_SUCCESS;
  if (idx > columns_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx));
  } else if (idx == columns_.count()) {
    sub_column = &spare_col_;
  } else {
    sub_column = &columns_.at(idx);
  }
  return ret;
}

int ObSemiStructSubSchema::get_column_id(const share::ObSubColumnPath& path, uint16_t &sub_col_idx) const
{
  int ret = OB_SUCCESS;
  share::ObSubColumnPath new_path;
  const share::ObSubColumnPath* path_ptr = &path;
  const ObSemiStructSubColumn* sub_column = nullptr;
  if (has_key_dict_) {
    if (OB_FAIL(make_column_path(path, new_path))) {
      LOG_WARN("make_column_path fail", K(ret), K(path));
    } else {
      path_ptr = &new_path;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_column(columns_, *path_ptr, sub_column))) {
    LOG_WARN("find column in freq columns fail", K(ret), K(path), KPC(path_ptr), K(columns_));
  } else if (OB_NOT_NULL(sub_column)) {
  // search spare columns
  } else if (OB_FAIL(find_column(spare_columns_, *path_ptr, sub_column))) {
    LOG_WARN("find column in spare columns fail", K(ret), K(path), KPC(path_ptr), K(spare_columns_));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(sub_column)) {
    if (sub_column->get_col_id() >= UINT16_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub column id is too large", K(ret), K(sub_column));
    } else {
      sub_col_idx = sub_column->get_col_id();
    }
  } else {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("column not found", K(ret), K(path), KPC(path_ptr));
  }
  return ret;
}

int ObSemiStructSubSchema::find_column(const ObIArray<ObSemiStructSubColumn>& cols, const share::ObSubColumnPath& path, const ObSemiStructSubColumn*& sub_column) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  int32_t left = 0;
  int32_t right = cols.count() - 1;
  while (! is_found && left <= right) {
    int32_t mid =  left + (right - left)/2;
    const ObSemiStructSubColumn& col = cols.at(mid);
    int cmp = col.get_path().compare(path, use_lexicographical_order_);
    if (cmp < 0) {
      left = mid + 1;
    } else if (cmp > 0) {
      right = mid - 1;
    } else {
      right = mid;
      is_found = true;
    }
  }
  if (! is_found) { // skip not found
  } else if (right < 0 || right > cols.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect result", K(ret), K(left), K(right), K(path), K(cols));
  } else {
    sub_column = &cols.at(right);
  }
  return ret;
}

int ObSemiStructSubSchema::handle_string_to_uint(const int32_t len, ObSemiStructSubColumn& sub_column) const
{
  int ret = OB_SUCCESS;
  if (sub_column.get_obj_type() != ObVarcharType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column is not obj varchar type", K(ret), K(sub_column));
  } else if (sub_column.get_json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column is not json null type", K(ret), K(sub_column));
  } else {
    if (sizeof(uint8_t) == len) {
      sub_column.set_obj_type(ObUTinyIntType);
    } else if (sizeof(uint16_t) == len) {
      sub_column.set_obj_type(ObUSmallIntType);
    } else if (sizeof(uint32_t) == len) {
      sub_column.set_obj_type(ObUInt32Type);
    } else if (sizeof(uint64_t) == len) {
      sub_column.set_obj_type(ObUInt64Type);
    }
  }
  return ret;
}

int ObSemiStructSubSchema::handle_null_type(ObSemiStructSubColumn& sub_column) const
{
  int ret = OB_SUCCESS;
  if (sub_column.get_obj_type() != ObNullType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column is not obj null type", K(ret), K(sub_column));
  } else if (sub_column.get_json_type() != ObJsonNodeType::J_NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column is not json null type", K(ret), K(sub_column));
  } else {
    sub_column.set_obj_type(ObTinyIntType);
    LOG_DEBUG("change obj null type to tinyint type", K(sub_column));
  }
  return ret;
}

int ObSemiStructSubSchema::add_freq_column(const ObJsonSchemaFlatter &flatter, ObSemiSchemaScalar *scalar_node)
{
  int ret = OB_SUCCESS;
  int64_t col_id = columns_.count();
  ObSemiStructSubColumn sub_column;
  scalar_node->set_col_id(col_id);
  if (scalar_node->json_type() == ObJsonNodeType::J_DECIMAL) {
    sub_column.set_precision_and_scale(scalar_node->get_precision(), scalar_node->get_scale());
  }
  if (OB_FAIL(sub_column.init(flatter.get_path(), scalar_node->json_type(), scalar_node->obj_type(), col_id))) {
    LOG_WARN("init sub column fail", K(ret), K(flatter.get_path()));
  } else if (OB_FAIL(columns_.push_back(ObSemiStructSubColumn()))) {
    LOG_WARN("push back fail", K(ret), "freq column size", columns_.count());
  } else if (OB_FAIL(columns_.at(col_id).deep_copy(allocator_, sub_column))) {
    LOG_WARN("deep copy fail", K(ret), K(col_id), K(sub_column));
  } else if (sub_column.get_obj_type() == ObNullType && OB_FAIL(handle_null_type(columns_.at(col_id)))) {
    LOG_WARN("handle null type fail", K(ret), K(sub_column));
  } else if (use_lexicographical_order_ && scalar_node->obj_type() == ObVarcharType &&
          scalar_node->get_max_len() == scalar_node->get_min_len() && is_int_len(scalar_node->get_min_len()) &&
          OB_FAIL(handle_string_to_uint(scalar_node->get_min_len(), columns_.at(col_id)))) {
    LOG_WARN("handle string to uint fail", K(ret), K(sub_column));
  }
  return ret;
}

int ObSemiStructSubSchema::add_spare_column(const ObJsonSchemaFlatter &flatter, ObSemiSchemaNode *node)
{
  int ret = OB_SUCCESS;
  int64_t col_id = spare_columns_.count() + flatter.get_freq_column_count();
  ObSemiStructSubColumn sub_column;
  sub_column.set_is_spare_storage();
  if (node->json_type() == ObJsonNodeType::J_DECIMAL) {
    ObSemiSchemaScalar* scalar_node = reinterpret_cast<ObSemiSchemaScalar*>(node);
    sub_column.set_precision_and_scale(scalar_node->get_precision(), scalar_node->get_scale());
  }
  if (col_id >= UINT16_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub column is too much", K(ret), K(col_id));
  } else if (OB_FAIL(sub_column.init(flatter.get_path(), node->json_type(), node->obj_type(), col_id))) {
    LOG_WARN("init sub column fail", K(ret), K(flatter.get_path()));
  } else if (OB_FAIL(spare_columns_.push_back(ObSemiStructSubColumn()))) {
    LOG_WARN("push back fail", K(ret), "spare size", spare_columns_.count());
  } else if (OB_FAIL(spare_columns_.at(spare_columns_.count() - 1).deep_copy(allocator_, sub_column))) {
    LOG_WARN("deep copy fail", K(ret), K(spare_columns_.count()), K(sub_column));
  } else {
    node->set_col_id(col_id);
  }
  return ret;
}

int ObSemiStructSubSchema::make_column_path(const share::ObSubColumnPath& path, share::ObSubColumnPath& result) const
{
  int ret = OB_SUCCESS;
  for (int i=0; OB_SUCC(ret) && i < path.get_path_item_count(); ++i) {
    const share::ObSubColumnPathItem &path_item = path.get_path_item(i);
    if (path_item.is_array() || path_item.is_dict_key()) {
      if (OB_FAIL(result.add_path_item(path_item.type_, path_item.array_idx_))) {
        LOG_WARN("add path item fail", K(ret), K(path_item));
      }
    } else if (path_item.is_object()) {
      const ObString &key = path_item.key_;
      int64_t id = -1;
      if (OB_FAIL(key_dict_.get(key, id))) {
        LOG_WARN("look up key dick fail", K(ret), K(key), K(key_dict_));
      }
      if (OB_SUCC(ret)) {
        if (id < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("id is negetive", K(ret), K(id));
        } else if (OB_FAIL(result.add_path_item(share::ObSubColumnPathItem::DICT_KEY, id))) {
          LOG_WARN("add path item fail", K(ret), K(id));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected path item", K(ret), K(i), K(path_item), K(path));
    }
  }
  return ret;
}

/**
 * ---------------------------------------- ObSemiStructObject ----------------------------------------
 */

int ObSemiStructObject::init(const int cap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(childs_ = reinterpret_cast<ObJsonObjectPair*>(allocator_->alloc(cap * sizeof(ObJsonObjectPair))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), K(cap), "size", sizeof(ObJsonObjectPair));
  } else if (OB_ISNULL(real_childs_ = reinterpret_cast<ObJsonObjectPair*>(allocator_->alloc(cap * sizeof(ObJsonObjectPair))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), K(cap), "size", sizeof(ObJsonObjectPair));
  } else {
    cap_ = cap;
    child_cnt_ = 0;
    real_child_cnt_ = 0;
  }
  return ret;
}

int ObSemiStructObject::get_key(uint64_t idx, ObString &key_out) const
{
  int ret = OB_SUCCESS;
  if (idx >= child_cnt_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("over bound", K(ret), K(child_cnt_), K(idx));
  } else {
    key_out = real_childs_[idx].get_key();
  }
  return ret;
}

ObJsonNode* ObSemiStructObject::get_value(uint64_t idx) const
{
  ObJsonNode *res = real_childs_[idx].get_value();
  if (res->is_scalar()) {
    res = static_cast<ObSemiStructScalar*>(res)->get_inner_node();
  }
  return res;
}

int ObSemiStructObject::object_add(const common::ObString &key, ObIJsonBase *value)
{
  int ret = OB_SUCCESS;
  ObJsonNode *node = nullptr;
  if (child_cnt_ >= cap_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("over bound", K(ret), K(child_cnt_), K(cap_));
  } else if (OB_ISNULL(node = dynamic_cast<ObJsonNode*>(value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value is not json node", K(ret), K(key), KPC(value));
  } else {
    ObJsonObjectPair &pair =  childs_[child_cnt_];
    pair.set_key(key);
    pair.set_value(node);
    ++child_cnt_;
    real_childs_[real_child_cnt_++] = pair;
  }

  if (OB_SUCC(ret) && child_cnt_ > 1) {
    ObJsonKeyCompare comparator(use_lexicographical_order_);
    if (comparator.compare(childs_[child_cnt_ - 1].get_key(), childs_[child_cnt_ - 2].get_key()) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key is not order", K(ret), K(child_cnt_), K(use_lexicographical_order_),
                         K(childs_[child_cnt_ - 1].get_key()), K(childs_[child_cnt_ - 2].get_key()));
    }
  }
  return ret;
}

uint64_t ObSemiStructObject::get_serialize_size()
{
  uint64_t serialize_size = 0;
  static const uint64_t ESTIMATE_OBJECT_SIZE = sizeof(uint32_t);
  static const uint64_t ESTIMATE_KEY_OFFSET_SIZE = sizeof(uint32_t);
  static const uint64_t ESTIMATE_VALUE_OFFSET_SIZE = sizeof(uint32_t);
  static const uint64_t TYPE_SIZE = sizeof(uint8_t);
  uint64_t count = element_count();
  uint64_t value_offset_size = (ESTIMATE_VALUE_OFFSET_SIZE + TYPE_SIZE) * count;
  uint64_t key_size = 0;
  uint64_t key_length_size = 0;
  uint8_t key_length_size_type;
  uint64_t node_size = 0;
  uint64_t value_size = 0;

  for (uint32_t i = 0; i < real_child_cnt_; i++) {
    const ObJsonObjectPair &obj_pair = real_childs_[i];
    ObString key = obj_pair.get_key();
    key_size += key.length();
    key_length_size_type = ObJsonVar::get_var_type(static_cast<uint64_t>(key.length()));
    key_length_size += ObJsonVar::get_var_size(key_length_size_type);
    ObJsonNode *value = obj_pair.get_value();
    node_size = value->get_serialize_size();
    value_size += node_size;
  }

  uint8_t count_type = ObJsonVar::get_var_type(count);
  uint64_t count_size = ObJsonVar::get_var_size(count_type);
  uint64_t estimated_total_offset_size =
      (ESTIMATE_KEY_OFFSET_SIZE + ESTIMATE_VALUE_OFFSET_SIZE + TYPE_SIZE) * count + key_length_size;
  uint64_t estimated_total_size = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + ESTIMATE_OBJECT_SIZE +
      estimated_total_offset_size + key_size + value_size);
  uint64_t last_offset = estimated_total_size - node_size;
  uint8_t offset_size_type = ObJsonVar::get_var_type(last_offset);
  uint64_t offset_type_size = ObJsonVar::get_var_size(offset_size_type);
  uint64_t total_offset_size = (offset_type_size * 2 + TYPE_SIZE) * count + key_length_size;
  uint64_t total_size = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + ESTIMATE_OBJECT_SIZE
      + total_offset_size + key_size + value_size);
  uint8_t object_size_type = ObJsonVar::get_var_type(total_size);
  uint64_t object_size = ObJsonVar::get_var_size(object_size_type);
  serialize_size = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + object_size
      + total_offset_size + key_size + value_size);
  return serialize_size;
}

/**
 * ---------------------------------------- ObSemiStructArray ----------------------------------------
 */

int ObSemiStructArray::init(const int cap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(childs_ = reinterpret_cast<ObJsonNode**>(allocator_->alloc(cap * sizeof(ObJsonNode*))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), K(cap), "size", sizeof(ObJsonNode));
  } else if (OB_ISNULL(real_childs_ = reinterpret_cast<ObJsonNode**>(allocator_->alloc(cap * sizeof(ObJsonNode*))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), K(cap), "size", sizeof(ObJsonNode));
  } else {
    cap_ = cap;
    child_cnt_ = 0;
    real_child_cnt_ = 0;
  }
  return ret;
}

ObJsonNode* ObSemiStructArray::get_value(uint64_t idx) const
{
  ObJsonNode *res = real_childs_[idx];
  if (res->is_scalar()) {
    res = static_cast<ObSemiStructScalar*>(res)->get_inner_node();
  }
  return res;
}

int ObSemiStructArray::array_append(ObIJsonBase *value)
{
  int ret = OB_SUCCESS;
  ObJsonNode *node = nullptr;
  if (child_cnt_ >= cap_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("over bound", K(ret), K(child_cnt_), K(cap_));
  } else if (OB_ISNULL(node = dynamic_cast<ObJsonNode*>(value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value is not json node", K(ret), KPC(value));
  } else {
    childs_[child_cnt_++] = node;
    real_childs_[real_child_cnt_++] = node;
  }
  return ret;
}

uint64_t ObSemiStructArray::get_serialize_size()
{
  uint64_t serialize_size = 0;

  static const uint64_t ESTIMATE_ARRAY_SIZE = sizeof(uint32_t);
  static const uint64_t ESTIMATE_OFFSET_SIZE = sizeof(uint32_t);
  static const uint64_t TYPE_SIZE = sizeof(uint8_t);
  uint64_t count = element_count();
  uint64_t node_total_size = 0;
  uint64_t node_size = 0;

  for (int i = 0; i < real_child_cnt_; i++) {
    node_size = real_childs_[i]->get_serialize_size();
    node_total_size += node_size;
  }

  uint8_t count_type = ObJsonVar::get_var_type(count);
  uint64_t count_size = ObJsonVar::get_var_size(count_type);
  uint64_t estimated_total_offset_size = (ESTIMATE_OFFSET_SIZE + TYPE_SIZE) * count;
  uint64_t estimated_total_size = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + ESTIMATE_ARRAY_SIZE
      + estimated_total_offset_size + node_total_size); // estimate array_size_type is uint32_t
  uint64_t last_offset = estimated_total_size - node_size;
  uint8_t offset_size_type = ObJsonVar::get_var_type(last_offset);
  uint64_t offset_type_size = ObJsonVar::get_var_size(offset_size_type);
  uint64_t total_offset_size = (offset_type_size + TYPE_SIZE) * count;
  uint64_t total_size = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + ESTIMATE_ARRAY_SIZE
      + total_offset_size + node_total_size); // estimate array_size_type is uint32_t
  uint8_t array_size_type = ObJsonVar::get_var_type(total_size);
  uint64_t array_size = ObJsonVar::get_var_size(array_size_type);
  serialize_size = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + array_size
      + total_offset_size + node_total_size);

  return serialize_size;
}

/**
 * ---------------------------------------- ObSemiStructScalar ----------------------------------------
 */

ObJsonNull ObSemiStructScalar::null_;

int64_t ObSemiStructScalar::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "semi struct scalar node type = %u", json_type_);
  return pos;
}

int ObSemiStructScalar::init(ObPrecision prec, ObScale scale)
{
  int ret = OB_SUCCESS;
  if (obj_type_ == ObJsonType) {

  } else if (OB_FAIL(ObJsonBaseFactory::alloc_node(*allocator_, json_type_, json_node_))) {
    LOG_WARN("alloc node fail", K(ret), K(json_type_));
  } else if (ObJsonNodeType::J_DECIMAL == json_type_) {
    ObJsonDecimal& decimal = static_cast<ObJsonDecimal&>(*json_node_);
    decimal.set_precision(prec);
    decimal.set_scale(scale);
  }
  return ret;
}

int ObSemiStructScalar::handle_int_to_string(ObJsonNode &base_node, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObJsonString& node = static_cast<ObJsonString&>(base_node);
  if (base_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node type.", K(ret), K(obj_type_), K(base_node.json_type()));
  } else if (obj_type_ == ObUTinyIntType) {
    uint8_val_ = datum.get_uint8();
    node.set_value(reinterpret_cast<char*>(&uint8_val_), sizeof(uint8_val_));
  } else if (obj_type_ == ObUSmallIntType) {
    uint16_val_ = datum.get_usmallint();
    node.set_value(reinterpret_cast<char*>(&uint16_val_), sizeof(uint16_val_));
  } else if (obj_type_ == ObUInt32Type) {
    uint32_val_ = datum.get_uint32();
    node.set_value(reinterpret_cast<char*>(&uint32_val_), sizeof(uint32_val_));
  } else if (obj_type_ == ObUInt64Type) {
    uint64_val_ = datum.get_uint64();
    node.set_value(reinterpret_cast<char*>(&uint64_val_), sizeof(uint64_val_));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node type.", K(ret), K(obj_type_));
  }
  return ret;
}

int ObSemiStructScalar::set_value(const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (obj_type_ == ObJsonType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not reach here for json type", K(ret));
  } else if (datum.is_null()) {
    has_value_ = true;
    datum_ = datum;
  } else if (json_type_ == ObJsonNodeType::J_STRING && obj_type_ != ObVarcharType) {
    if (OB_FAIL(handle_int_to_string(*json_node_, datum))) {
      LOG_WARN("handle_int_to_string fail", K(ret), K(json_type_), K(obj_type_));
    }
  } else if (OB_FAIL(datum_to_json(*json_node_, datum))) {
    LOG_WARN("from datum fail", K(ret), K(json_type_), KPC(json_node_));
  }
  if (OB_SUCC(ret)) {
    has_value_ = true;
    datum_ = datum;
  }
  return ret;
}

int ObSemiStructScalar::set_value(ObJsonBin &bin)
{
  int ret = OB_SUCCESS;
  ObStorageDatum datum;
  if (obj_type_ == ObJsonType) {
    if (OB_FAIL(bin.to_tree(json_node_))) {
      LOG_WARN("to tree fail", K(ret), K(bin));
    } else {
      has_value_ = true;
      datum_ = datum;
    }
  } else if (bin.get_vertype() == ObJBVerType::J_NULL_V0) {
    has_value_ = true;
    datum_.set_null();
  } else if (OB_FAIL(bin_to_tree(bin, *json_node_))) {
    LOG_WARN("to datum fail", K(ret), K(bin), KPC(json_node_));
  } else {
    has_value_ = true;
    datum_ = datum;
  }
  return ret;
}

/**
 * ---------------------------------------- ObJsonReassembler ----------------------------------------
 */

void ObJsonReassembler::reset()
{
  allocator_.reset();
  tmp_allocator_.reset();
  decode_allocator_ = nullptr;
  sub_schema_ = nullptr;
  json_ = nullptr;
  leaves_.reset();
  spare_leaves_.reset();
  sub_cols_.reset();
  is_inited_ = false;
}

int ObJsonReassembler::serialize(const ObDatumRow &row, ObString &result)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 1_s);
  if (OB_ISNULL(json_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json is null", K(ret));
  } else if (sub_schema_->get_version() == ObSemiStructSubSchema::SCHEMA_VERSION &&
                    (sub_cols_.count() != leaves_.count() + spare_leaves_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count not match", K(ret), K(sub_cols_.count()), K(leaves_.count()), K(spare_leaves_.count()));
  } else if (row.count_ != sub_schema_->get_store_column_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count not match", K(ret), K(sub_schema_->get_store_column_cnt()), K(row.count_));
  } else {
    tmp_allocator_.reuse();
    ObStringBuffer j_bin_buf(decode_allocator_);
    ObString json_data;
    if (OB_FAIL(fill_freq_column(row))) {
      LOG_WARN("fill freq column fail", K(ret), K(row));
    } else if (sub_schema_->has_spare_column() && OB_FAIL(fill_spare_column(row))) {
      LOG_WARN("fill spare column fail", K(ret), K(row));
    } else if (OB_FAIL(reshape(json_))) {
      LOG_WARN("reshape fail", K(ret));
    } else if (OB_FAIL(prepare_lob_common(j_bin_buf))) {
      LOG_WARN("prepare_lob_common fail", K(ret));
    } else if (OB_FAIL(ObJsonBin::add_doc_header_v0(j_bin_buf))) {
      LOG_WARN("add_doc_header_v0 fail", K(ret));
    } else if (OB_FAIL(ObJsonBinSerializer::serialize_json_value((ObJsonNode*)json_, j_bin_buf, false/*enable_reserialize*/))) {
      LOG_WARN("serialize json binary fail", K(ret));
    } else if (OB_FAIL(j_bin_buf.get_result_string(result))) {
      LOG_WARN("get_result_string fail", K(ret), K(row), K(j_bin_buf));
    } else if (OB_FALSE_IT(json_data.assign_ptr(result.ptr() + sizeof(ObLobCommon), result.length() - sizeof(ObLobCommon)))) {
    } else if (OB_FAIL(ObJsonBin::set_doc_header_v0(json_data, json_data.length(), sub_schema_->use_lexicographical_order()))) {
      LOG_WARN("set_doc_header_v0 fail", K(ret));
    }

    if (allocator_.used() >= OB_DEFAULT_MACRO_BLOCK_SIZE) {
      LOG_DEBUG("too much memroy used", KP(this), K(allocator_.used()), K(allocator_.total()), K(result.length()), K(j_bin_buf.capacity()));
    }
  }
  return ret;
}

int ObJsonReassembler::fill_freq_column(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < sub_schema_->get_freq_column_cnt(); ++i) {
    const ObDatum& datum = row.storage_datums_[i];
    if (datum.is_nop()) {
      leaves_.at(i)->set_no_value();
    } else if (OB_FAIL(leaves_.at(i)->set_value(datum))) {
      LOG_WARN("set value fail", K(ret), K(i));
    }
  }
  return ret;
}

int ObJsonReassembler::fill_spare_column(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObDatum& datum = row.storage_datums_[row.count_ - 1];
  ObString raw_data = datum.get_string();
  ObJsonBin bin(raw_data.ptr(), raw_data.length());
  if (datum.is_null()) {
    for (int i = 0; OB_SUCC(ret) && i < spare_leaves_.count(); ++i) {
      spare_leaves_.at(i)->set_no_value();
    }
  } else if (OB_FAIL(bin.reset_iter())) {
    LOG_WARN("reset bin fail", K(ret), K(datum));
  } else if (bin.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("spare column data is not json object", K(ret), K(bin));
  } else {
    ObJsonBin child(&tmp_allocator_);
    ObIJsonBase* child_ptr = &child;
    ObString key;
    for (int i = 0; OB_SUCC(ret) && i < spare_leaves_.count(); ++i) {
      spare_leaves_.at(i)->set_no_value();
    }
    for (int i = 0; OB_SUCC(ret) && i < bin.element_count(); ++i) {
      if (OB_FAIL(bin.get_object_value(i, key, child_ptr))) {
        LOG_WARN("get child fail", K(ret));
      } else {
        uint16_t col_id = 0;
        if (OB_UNLIKELY(sub_schema_->get_version() == ObSemiStructSubSchema::SCHEMA_VERSION)) {
          col_id = *reinterpret_cast<uint32_t*>(key.ptr());
        } else if (OB_LIKELY(sub_schema_->get_version() == ObSemiNewSchema::NEW_SCHEMA_VERSION)) {
          col_id = *reinterpret_cast<uint16_t*>(key.ptr());
        }
        uint16_t idx = col_id - sub_schema_->get_freq_column_cnt();
        if (idx < 0 || idx >= spare_leaves_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid spare col id", K(ret), K(col_id), K(idx), K(sub_schema_->get_freq_column_cnt()), K(spare_leaves_.count()));
        } else if (OB_FAIL(spare_leaves_.at(idx)->set_value(*static_cast<ObJsonBin*>(child_ptr)))) {
          LOG_WARN("set value fail", K(ret), K(i), K(idx), K(col_id));
        }
      }
    }
  }
  return ret;
}

bool ObJsonReassembler::has_value(ObIJsonBase *node) const
{
  bool res = false;
  if (node->json_type() == ObJsonNodeType::J_OBJECT) {
    ObSemiStructObject* object = static_cast<ObSemiStructObject*>(node);
    res = object->real_child_cnt_ > 0;
  } else if (node->json_type() == ObJsonNodeType::J_ARRAY) {
    ObSemiStructArray* array = static_cast<ObSemiStructArray*>(node);
    res = array->real_child_cnt_ > 0;
  } else {
    ObSemiStructScalar *scalar = static_cast<ObSemiStructScalar*>(node);
    res = scalar->has_value();
  }
  return res;
}

int ObJsonReassembler::reshape(ObIJsonBase *node)
{
  int ret = OB_SUCCESS;
  if (node->json_type() == ObJsonNodeType::J_OBJECT) {
    ObSemiStructObject* object = static_cast<ObSemiStructObject*>(node);
    object->real_child_cnt_ = 0;
    for (int i = 0; OB_SUCC(ret) && i < object->child_cnt_; ++i) {
      if (object->childs_[i].get_value()->is_scalar()) {
        ObSemiStructScalar *scalar = static_cast<ObSemiStructScalar*>(object->childs_[i].get_value());
        if (scalar->has_value()) {
          object->real_childs_[object->real_child_cnt_++] = object->childs_[i];
        } else {
          object->real_childs_[i] = ObJsonObjectPair();
        }
      } else if (OB_FAIL(reshape(object->childs_[i].get_value()))) {
        LOG_WARN("reshape fail", K(ret), K(i));
      } else if (has_value(object->childs_[i].get_value())) {
        object->real_childs_[object->real_child_cnt_++] = object->childs_[i];
      }
    }
  } else if (node->json_type() == ObJsonNodeType::J_ARRAY) {
    ObSemiStructArray* array = static_cast<ObSemiStructArray*>(node);
    array->real_child_cnt_ = 0;
    for (int i = 0; OB_SUCC(ret) && i < array->child_cnt_; ++i) {
      if (array->childs_[i]->is_scalar()) {
        ObSemiStructScalar *scalar = static_cast<ObSemiStructScalar*>(array->childs_[i]);
        if (scalar->has_value()) {
          array->real_childs_[array->real_child_cnt_++] = array->childs_[i];
        } else {
          array->real_childs_[i] = nullptr;
        }
      } else if (OB_FAIL(reshape(array->childs_[i]))) {
        LOG_WARN("reshape fail", K(ret), K(i));
      } else if (has_value(array->childs_[i])) {
        array->real_childs_[array->real_child_cnt_++] = array->childs_[i];
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar node should not reach here", K(ret), KPC(node));
  }
  return ret;
}

int ObJsonReassembler::prepare_lob_common(ObJsonBuffer &result)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = sizeof(ObLobCommon);
  char *buf = nullptr;
  if (OB_FAIL(result.reserve(sizeof(ObLobCommon)))) {
    LOG_WARN("reserve fail", K(ret), K(header_size));
  } else if (OB_ISNULL(buf = result.ptr() + result.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result ptr is null", K(ret), K(result));
  } else {
    ObLobCommon *lob_common = new (buf) ObLobCommon();
    if (OB_FAIL(result.set_length(result.length() + header_size))) {
      LOG_WARN("set length fail", K(ret), K(header_size), K(result));
    }
  }
  return ret;
}


int ObJsonReassembler::alloc_container_node(const share::ObSubColumnPathItem& item, const int child_cnt, ObIJsonBase *&node)
{
  int ret = OB_SUCCESS;
  if (item.type_ == share::ObSubColumnPathItem::ARRAY) {
    ObSemiStructArray *array = nullptr;
    if (OB_ISNULL(array = OB_NEWx(ObSemiStructArray, &allocator_, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObSemiStructArray));
    } else if (OB_FAIL(array->init(child_cnt))) {
      LOG_WARN("init array fail", K(ret), K(item));
    } else {
      node = array;
    }
  } else if (item.type_ == share::ObSubColumnPathItem::OBJECT || item.type_ == share::ObSubColumnPathItem::DICT_KEY) {
    ObSemiStructObject *object = nullptr;
    if (OB_ISNULL(object = OB_NEWx(ObSemiStructObject, &allocator_, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObSemiStructObject));
    } else if (OB_FAIL(object->init(child_cnt))) {
      LOG_WARN("init array fail", K(ret), K(item));
    } else {
      object->set_use_lexicographical_order(sub_schema_->use_lexicographical_order());
      node = object;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path item is incorrect", K(ret), K(item));
  }
  return ret;
}

int ObJsonReassembler::alloc_scalar_json_node(const ObSemiStructSubColumn& sub_column, ObIJsonBase *&node)
{
  int ret = OB_SUCCESS;
  ObSemiStructScalar *scalar = nullptr;
  const ObObjType obj_type = sub_column.get_obj_type();
  const ObJsonNodeType json_type = sub_column.get_json_type();
  if (OB_ISNULL(scalar = OB_NEWx(ObSemiStructScalar, &allocator_, &allocator_, obj_type, json_type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObSemiStructScalar));
  } else if (OB_FAIL(scalar->init(sub_column.get_precision(), sub_column.get_scale()))) {
    LOG_WARN("init semistruct scalar node fail", K(ret), K(obj_type), K(json_type), KPC(scalar));
  } else {
    node = scalar;
  }
  return ret;
}

int ObJsonReassembler::alloc_scalar_json_node(uint16_t col_id, ObSemiNewSchema &schema, ObJsonNode *&node)
{
  int ret = OB_SUCCESS;
  ObSemiStructScalar *scalar = nullptr;
  if (!schema.col_id_valid(col_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col id is invalid", K(ret), K(col_id), K(schema));
  } else {
    ObObjType obj_type = ObObjType(schema.get_semi_schema_infos()[col_id].obj_type_);
    ObJsonNodeType json_type = ObJsonNodeType(schema.get_semi_schema_infos()[col_id].json_type_);
    if (OB_ISNULL(scalar = OB_NEWx(ObSemiStructScalar, &allocator_, &allocator_, obj_type, json_type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObSemiStructScalar));
    } else if (OB_FAIL(scalar->init(schema.get_semi_schema_infos()[col_id].prec_, schema.get_semi_schema_infos()[col_id].scale_))) {
      LOG_WARN("init semistruct scalar node fail", K(ret), K(obj_type), K(json_type), KPC(scalar));
    } else {
      node = scalar;
    }
  }
  return ret;
}

int ObJsonReassembler::add_child(ObIJsonBase *parent, ObIJsonBase *child, const share::ObSubColumnPathItem &item)
{
  int ret = OB_SUCCESS;
  if (item.type_ == share::ObSubColumnPathItem::ARRAY) {
    if (OB_FAIL(parent->array_append(child))) {
      LOG_WARN("append fail", K(ret), K(item));
    }
  } else if (item.type_ == share::ObSubColumnPathItem::OBJECT) {
    if (OB_FAIL(parent->object_add(item.key_, child))) {
      LOG_WARN("append fail", K(ret), K(item));
    }
  } else if (item.type_ == share::ObSubColumnPathItem::DICT_KEY) {
    ObString key;
    if (OB_FAIL(reinterpret_cast<ObSemiStructSubSchema*>(sub_schema_)->get_key_str(item.id_, key))) {
      LOG_WARN("get key str fail", K(ret), K(item));
    } else if (OB_FAIL(parent->object_add(key, child))) {
      LOG_WARN("append fail", K(ret), K(item));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path item is incorrect", K(ret), K(item));
  }
  return ret;
}

bool ObJsonReassembler::is_heterigeneous_column(const share::ObSubColumnPathItem &item, ObIJsonBase *&current) {
  bool is_heterigeneous_column = false;
  if (OB_NOT_NULL(current)) {
    if ((current->json_type() == ObJsonNodeType::J_OBJECT && item.type_ == share::ObSubColumnPathItem::ARRAY) ||
              (current->json_type() == ObJsonNodeType::J_ARRAY && item.type_ == share::ObSubColumnPathItem::OBJECT)) {
      is_heterigeneous_column = true;
    }
  }
  return is_heterigeneous_column;
}

/*
----------------------------------------------
json1: {"K":["scala"]}
json2: {"K":{"Q":"value"}}
sub_column_path1: object->array
sub_column_path2: object->object
current_depth1 = object_node
current_depth2 = array_node
child_node1 = String_node  |   path_item_type = array
child_node2 = String_node  |   path_item_type = object
----------------------------------------------
*/
int ObJsonReassembler::reassemble(const int start, const int end, const int depth, ObIJsonBase *&current, int &real_end)
{
  int ret = OB_SUCCESS;
  int child_cnt = end - start;
  int i = start;
  for (; OB_SUCC(ret) && i < end;) {
    const ObSemiStructSubColumn& sub_column = *sub_cols_.at(i);
    const share::ObSubColumnPath& sub_cloumn_path = sub_column.get_path();
    const share::ObSubColumnPathItem &path_item = sub_cloumn_path.get_path_item(depth);
    ObIJsonBase *child = nullptr;
    if (is_heterigeneous_column(path_item, current)) {
      break;
    } else if (current == nullptr && OB_FAIL(alloc_container_node(path_item, child_cnt, current))) {
      LOG_WARN("alloc node fail", K(ret), K(path_item));
    } else if (depth + 1 == sub_cloumn_path.get_path_item_count()) {
      if (OB_FAIL(alloc_scalar_json_node(sub_column, child))) {
        LOG_WARN("alloc node fail", K(ret), K(path_item));
      } else if (sub_column.is_spare_storage()) {
        if (OB_FAIL(spare_leaves_.push_back((ObSemiStructScalar*)child))) {
          LOG_WARN("push back spare leaf fail", K(ret));
        }
      } else if (OB_FAIL(leaves_.push_back((ObSemiStructScalar*)child))) {
        LOG_WARN("push back freq leaf fail", K(ret));
      }
      if (OB_SUCC(ret)) {
        ++i;
      }
    } else {
      int child_start = i;
      int child_end = i + 1;
      for (; child_end < end; ++child_end) {
        const share::ObSubColumnPathItem &sibling_item = sub_cols_.at(child_end)->get_path().get_path_item(depth);
        if (sibling_item.compare(path_item, sub_schema_->use_lexicographical_order()) != 0) {
          break;
        }
      }
      if (OB_FAIL(reassemble(child_start, child_end, depth + 1, child, real_end))) {
        LOG_WARN("reassembler child fail", K(ret), K(depth));
      } else {
        i = real_end;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_child(current, child, path_item))) {
      LOG_WARN("add child fail", K(ret), K(i), K(depth));
    }
  }
  real_end = i;
  return ret;
}

int ObJsonReassembler::alloc_container_node(int real_child_cnt, int child_cnt, ObJsonNodeType type,
      ObSemiNewSchema &schema, ObIJsonBase **child_schemas, ObString *keys,  ObJsonNode *&current)
{
  int ret = OB_SUCCESS;
  if (type == ObJsonNodeType::J_ARRAY) {
    ObSemiStructArray *array = nullptr;
    if (OB_ISNULL(array = OB_NEWx(ObSemiStructArray, &allocator_, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObSemiStructArray));
    } else if (OB_FAIL(array->init(real_child_cnt))) {
      LOG_WARN("init array fail", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
        ObJsonNode **child_node = nullptr;
        if (OB_FAIL(json_to_schema(*child_schemas[i], schema, child_node))) {
          LOG_WARN("json to schema fail", K(ret), K(i), K(real_child_cnt), K(child_cnt), K(type), K(schema));
        } else if (OB_ISNULL(child_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child node is null", K(ret), K(i), K(real_child_cnt), K(child_cnt), K(type), K(schema));
        } else if (child_schemas[i]->json_type() == ObJsonNodeType::J_SEMI_HETE_COL) {
          for (int j = 0; OB_SUCC(ret) && j < child_schemas[i]->element_count(); j++) {
            if (OB_ISNULL(child_node[j])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("child node is null", K(ret), K(i), K(j), K(real_child_cnt), K(child_cnt), K(type), K(schema));
            } else if (OB_FAIL(array->array_append(child_node[j]))) {
              LOG_WARN("add child fail", K(ret), K(i), K(j), K(real_child_cnt), K(child_cnt), K(type), K(schema));
            }
          }
        } else {
          if (OB_FAIL(array->array_append(child_node[0]))) {
            LOG_WARN("add child fail", K(ret), K(i), K(real_child_cnt), K(child_cnt), K(type), K(schema));
          }
        }
      }
      current = array;
    }
  } else if (type == ObJsonNodeType::J_OBJECT) {
    ObSemiStructObject *object = nullptr;
    if (OB_ISNULL(object = OB_NEWx(ObSemiStructObject, &allocator_, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fail", K(ret), "size", sizeof(ObSemiStructObject));
    } else if (OB_FAIL(object->init(real_child_cnt))) {
      LOG_WARN("init array fail", K(ret), K(real_child_cnt), K(child_cnt), K(type), K(schema));
    } else {
      object->set_use_lexicographical_order(sub_schema_->use_lexicographical_order());
      for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
        ObJsonNode **child_node = nullptr;
        if (OB_FAIL(json_to_schema(*child_schemas[i], schema, child_node))) {
          LOG_WARN("json to schema fail", K(ret), K(i), K(real_child_cnt), K(child_cnt), K(type), K(schema));
        } else if (OB_ISNULL(child_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child node is null", K(ret), K(i), K(real_child_cnt), K(child_cnt), K(type), K(schema));
        } else if (child_schemas[i]->json_type() == ObJsonNodeType::J_SEMI_HETE_COL) {
          for (int j = 0; OB_SUCC(ret) && j < child_schemas[i]->element_count(); j++) {
            if (OB_ISNULL(child_node[j])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("child node is null", K(ret), K(i), K(j), K(real_child_cnt), K(child_cnt), K(type), K(schema));
            } else if (OB_FAIL(object->object_add(keys[i], child_node[j]))) {
              LOG_WARN("add child fail", K(ret), K(i), K(j), K(real_child_cnt), K(child_cnt), K(type), K(schema));
            }
          }
        } else {
          if (OB_FAIL(object->object_add(keys[i], child_node[0]))) {
            LOG_WARN("add child fail", K(ret), K(i), K(real_child_cnt), K(child_cnt), K(type), K(schema));
          }
        }
      }
      current = object;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path item is incorrect", K(ret), K(real_child_cnt), K(child_cnt), K(type), K(schema));
  }
  return ret;
}

int ObJsonReassembler::json_to_schema(ObIJsonBase &json_base, ObSemiNewSchema &semi_schema, ObJsonNode **&current)
{
  int ret = OB_SUCCESS;
  int child_cnt = json_base.element_count();
  if (json_base.json_type() == ObJsonNodeType::J_OBJECT) {
    ObIJsonBase *child_schemas[child_cnt];
    ObString keys[child_cnt];
    int real_child_cnt = json_base.element_count();
    if (OB_ISNULL(current = reinterpret_cast<ObJsonNode**>(allocator_.alloc(sizeof(ObJsonNode*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for obj json node", K(ret), "size", sizeof(ObJsonNode*));
    }
    for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      child_schemas[i] = nullptr;
      if (OB_FAIL(json_base.get_object_value(i, keys[i], child_schemas[i]))) {
        LOG_WARN("get child fail", K(ret), K(i));
      } else if (child_schemas[i]->json_type() == ObJsonNodeType::J_SEMI_HETE_COL){
        real_child_cnt += child_schemas[i]->element_count() - 1;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alloc_container_node(real_child_cnt, child_cnt, ObJsonNodeType::J_OBJECT, semi_schema, child_schemas, keys, current[0]))) {
      LOG_WARN("alloc container node fail", K(ret), K(real_child_cnt));
    }
  } else if (json_base.json_type() == ObJsonNodeType::J_ARRAY) {
    int real_child_cnt = json_base.element_count();
    ObIJsonBase *child_schemas[child_cnt];
    if (OB_ISNULL(current = reinterpret_cast<ObJsonNode**>(allocator_.alloc(sizeof(ObJsonNode*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for obj json node", K(ret), "size", sizeof(ObJsonNode*));
    }
    for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      child_schemas[i] = nullptr;
      if (OB_FAIL(json_base.get_array_element(i, child_schemas[i]))) {
        LOG_WARN("get child fail", K(ret), K(i));
      } else if (child_schemas[i]->json_type() == ObJsonNodeType::J_SEMI_HETE_COL){
        real_child_cnt += child_schemas[i]->element_count() - 1;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alloc_container_node(real_child_cnt, child_cnt, ObJsonNodeType::J_ARRAY, semi_schema, child_schemas, nullptr, current[0]))) {
      LOG_WARN("alloc container node fail", K(ret), K(real_child_cnt));
    }
  } else if (json_base.json_type() == ObJsonNodeType::J_SEMI_HETE_COL) {
    if (OB_ISNULL(current = reinterpret_cast<ObJsonNode**>(allocator_.alloc(sizeof(ObJsonNode*) * child_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for obj json node", K(ret), "size", sizeof(ObJsonNode*));
    }
    for (int i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      ObIJsonBase *child_schema = nullptr;
      ObJsonNode **child_node = nullptr;
      if (OB_FAIL(json_base.get_array_element(i, child_schema))) {
        LOG_WARN("get child fail", K(ret), K(i));
      } else if (OB_FAIL(json_to_schema(*child_schema, semi_schema, child_node))) {
        LOG_WARN("json to schema fail", K(ret), K(i));
      } else {
        current[i] = child_node[0];
      }
    }
  } else if (json_base.json_type() == ObJsonNodeType::J_UINT) {
    uint16_t col_id = json_base.get_uint();
    if (!semi_schema.col_id_valid(col_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col id is invalid", K(ret), K(col_id));
    } else if (OB_ISNULL(current = reinterpret_cast<ObJsonNode**>(allocator_.alloc(sizeof(ObJsonNode*))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for obj json node", K(ret), "size", sizeof(ObJsonNode*));
    } else if (OB_FAIL(alloc_scalar_json_node(col_id, semi_schema, current[0]))) {
      LOG_WARN("alloc node fail", K(ret), K(col_id), K(semi_schema));
    } else if (semi_schema.get_semi_schema_infos()[col_id].is_freq_column_ == 0) {
      if (OB_FAIL(spare_leaves_.push_back((ObSemiStructScalar*)current[0]))) {
        LOG_WARN("push back spare leaf fail", K(ret));
      }
    } else if (OB_FAIL(leaves_.push_back((ObSemiStructScalar*)current[0]))) {
      LOG_WARN("push back freq leaf fail", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json type is incorrect", K(ret), K(json_base.json_type()));
  }

  return ret;
}


int ObJsonReassembler::build_schema_tree(ObIJsonBase &json_base, ObSemiNewSchema &semi_schema, ObIJsonBase *&parent)
{
  int ret = OB_SUCCESS;
  ObJsonNode **current = nullptr;
  if (OB_FAIL(json_to_schema(json_base, semi_schema, current))) {
    LOG_WARN("json to schema fail", K(ret));
  } else if (OB_ISNULL(current) || OB_ISNULL(current[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current is null", K(ret));
  } else {
    parent = current[0];
  }
  return ret;
}


int ObJsonReassembler::deserialize_new_schema()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub schema is null", K(ret));
  } else {
    ObSemiNewSchema &semi_schema = dynamic_cast<ObSemiNewSchema&>(*sub_schema_);
    ObJsonBin json_bin(semi_schema.get_schema_buf().ptr(), semi_schema.get_schema_buf().length(), &allocator_);
    if (OB_FAIL(json_bin.reset_iter())) {
      LOG_WARN("reset json bin iter failed", K(ret));
    } else if (json_bin.json_type() != ObJsonNodeType::J_ARRAY && json_bin.json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json bin type is incorrect", K(ret), K(json_bin.json_type()));
    } else if (OB_FAIL(build_schema_tree(json_bin, semi_schema, json_))) {
      LOG_WARN("json to schema fail", K(ret));
    }
  }
  return ret;
}

int ObJsonReassembler::init()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 1_s);
  int real_end = 0;
  if (OB_ISNULL(sub_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub schema is null", K(ret));
  } else if (OB_FAIL(leaves_.reserve(sub_schema_->get_freq_column_cnt()))) {
    LOG_WARN("reserve array fail", K(ret), K(sub_schema_->get_freq_column_cnt()));
  } else if (sub_schema_->get_version() == ObSemiStructSubSchema::SCHEMA_VERSION) {
    if (OB_FAIL(merge_sub_cols())) {
      LOG_WARN("merge_sub_cols fail", K(ret), KPC(sub_schema_));
    } else if (FALSE_IT(real_end = sub_cols_.count())) {
    } else if (OB_FAIL(reassemble(0, sub_cols_.count(), 0, json_, real_end))) {
      LOG_WARN("reassemble fail", K(ret), KPC(sub_schema_));
    }
  } else if (sub_schema_->get_version() == ObSemiNewSchema::NEW_SCHEMA_VERSION) {
    if (OB_FAIL(deserialize_new_schema())) {
      LOG_WARN("deserialize new schema fail", K(ret), KPC(sub_schema_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub schema version is incorrect", K(ret), KPC(sub_schema_));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObJsonReassembler::merge_sub_cols()
{
  int ret = OB_SUCCESS;
  ObSemiStructSubSchema *semi_schema = dynamic_cast<ObSemiStructSubSchema*>(sub_schema_);
  const ObIArray<ObSemiStructSubColumn>& freq_columns = semi_schema->get_freq_columns();
  const ObIArray<ObSemiStructSubColumn>& spare_columns = semi_schema->get_spare_columns();

  int32_t freq_idx = 0;
  int32_t spare_idx = 0;
  while(OB_SUCC(ret) && freq_idx < freq_columns.count() && spare_idx < spare_columns.count()) {
    const ObSemiStructSubColumn& freq_sub_col = freq_columns.at(freq_idx);
    const ObSemiStructSubColumn& spare_sub_col = spare_columns.at(spare_idx);
    int cmp = freq_sub_col.compare(spare_sub_col, sub_schema_->use_lexicographical_order());
    if (cmp < 0) {
      if (OB_FAIL(sub_cols_.push_back(&freq_sub_col))) {
        LOG_WARN("push back freq sub col fail", K(ret), K(freq_sub_col));
      } else {
        ++freq_idx;
      }
    } else if (cmp > 0) {
      if (OB_FAIL(sub_cols_.push_back(&spare_sub_col))) {
        LOG_WARN("push back freq sub col fail", K(ret), K(spare_sub_col));
      } else {
        ++spare_idx;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("freq sub col and spare sub col is same", K(ret), K(freq_sub_col), K(spare_sub_col));
    }
  }

  while(OB_SUCC(ret) && freq_idx < freq_columns.count()) {
    const ObSemiStructSubColumn& freq_sub_col = freq_columns.at(freq_idx);
    if (OB_FAIL(sub_cols_.push_back(&freq_sub_col))) {
      LOG_WARN("push back freq sub col fail", K(ret), K(freq_sub_col));
    } else {
      ++freq_idx;
    }
  }

  while(OB_SUCC(ret) && spare_idx < spare_columns.count()) {
    const ObSemiStructSubColumn& spare_sub_col = spare_columns.at(spare_idx);
    if (OB_FAIL(sub_cols_.push_back(&spare_sub_col))) {
      LOG_WARN("push back freq sub col fail", K(ret), K(spare_sub_col));
    } else {
      ++spare_idx;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (sub_cols_.count() > 1) {
    const ObSemiStructSubColumn *prev_sub_column = sub_cols_.at(0);
    const ObSemiStructSubColumn *curr_sub_column = nullptr;
    for (int i = 1; OB_SUCC(ret) && i < sub_cols_.count(); ++i) {
      curr_sub_column = sub_cols_.at(i);
      int cmp = prev_sub_column->compare(*curr_sub_column, sub_schema_->use_lexicographical_order());
      if (cmp > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub column order is incorrect", K(ret), K(cmp), KPC(prev_sub_column), KPC(curr_sub_column));
      } else {
        prev_sub_column = curr_sub_column;
      }
    }
  }
  return ret;
}


}  // end namespace blocksstable
}  // end namespace oceanbase
