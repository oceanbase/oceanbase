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
#define USING_LOG_PREFIX SQL
#include "common/object/ob_obj_type.h"
#include "ob_json_bin.h"
#include "ob_json_tree.h"

namespace oceanbase {
namespace common {

static const double OB_JSON_BIN_REBUILD_THRESHOLD = 1.3; // 30%

int ObJsonBin::get_obtime(ObTime &t) const
{
  INIT_SUCC(ret);

  switch (json_type()) {
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      ret = ObTimeConverter::date_to_ob_time(int_val_, t);
      break;
    }
    case ObJsonNodeType::J_TIME: {
      ret = ObTimeConverter::time_to_ob_time(int_val_, t);
      break;
    }
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      ret = ObTimeConverter::datetime_to_ob_time(int_val_, NULL, t);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to do get obtime.", K(ret), K(json_type()), K(int_val_));
  }

  return ret;
}

int ObJsonBin::check_valid_object_op(ObIJsonBase *value) const
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is null", K(ret));
  } else if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(json_type()));
  } else if (value->is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is json tree, not supported", K(ret), K(*value));
  }

  return ret;
}

int ObJsonBin::check_valid_array_op(ObIJsonBase *value) const
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is NULL", K(ret));
  } else if (json_type() != ObJsonNodeType::J_ARRAY) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(json_type()));
  } else if (value->is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is json tree, not supported", K(ret), K(*value));
  }

  return ret;
}

int ObJsonBin::check_valid_object_op(uint64_t index) const
{
  INIT_SUCC(ret);

  if (index >= element_count()) { // check param
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index is out of range in object", K(ret), K(index), K(element_count()));
  } else if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node type", K(ret), K(json_type()));
  }

  return ret;
}

int ObJsonBin::check_valid_array_op(uint64_t index) const
{
  INIT_SUCC(ret);

  if (index >= element_count()) { // check param
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index is out of range in array", K(ret), K(index), K(element_count()));
  } else if (json_type() != ObJsonNodeType::J_ARRAY) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node type", K(ret), K(json_type()));
  }

  return ret;
}

int ObJsonBin::object_add(const common::ObString &key, ObIJsonBase *value)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_object_op(value))) {
    LOG_WARN("invalid json object operation", K(ret), K(key));
  } else {
    ObJsonBin *j_bin = static_cast<ObJsonBin *>(this);
    if (OB_FAIL(j_bin->add(key, static_cast<ObJsonBin *>(value)))) {
      LOG_WARN("fail to add value to object by key", K(ret), K(key), K(*value));
    }
  }

  return ret;
}

int ObJsonBin::array_insert(uint64_t index, ObIJsonBase *value)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_array_op(value))) {
    LOG_WARN("invalid json array operation", K(ret), K(index));
  } else {
    ObJsonBin *j_bin = static_cast<ObJsonBin *>(this);
    if (OB_FAIL(j_bin->insert(static_cast<ObJsonBin *>(value), index))) {
      LOG_WARN("fail to insert value to array", K(ret), K(index), K(*value));
    }
  }

  return ret;
}

int ObJsonBin::array_append(ObIJsonBase *value)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_array_op(value))) {
    LOG_WARN("invalid json array operation", K(ret));
  } else {
    ObJsonBin *j_bin = static_cast<ObJsonBin *>(this);
    if (OB_FAIL(j_bin->append(static_cast<ObJsonBin *>(value)))) {
      LOG_WARN("fail to append value to array", K(ret), K(*value));
    }
  }

  return ret;
}

int ObJsonBin::replace(const ObIJsonBase *old_node, ObIJsonBase *new_node)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(old_node) || OB_ISNULL(new_node)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null param", K(ret), KP(old_node), KP(new_node));
  } else if (old_node->is_tree() || new_node->is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is tree, but need binary", K(ret), K(old_node), K(new_node));
  } else if (is_alloc_ == false) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json bin is read only.", K(ret), K(is_alloc_));
  } else {
    ObJsonNodeType j_type = json_type();
    ObJBVerType vertype = get_vertype();
    if (j_type != ObJsonNodeType::J_ARRAY && j_type != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error curr type. not support replace", K(ret), K(j_type));
    } else {
      char *child_val_addr = NULL;
      const ObJsonBin *old_bin = static_cast<const ObJsonBin *>(old_node);
      const char *expect_val_addr = old_bin->curr_.ptr() + old_bin->pos_;
      for (int i = 0; OB_SUCC(ret) && i < element_count_; i++) {
        if (j_type == ObJsonNodeType::J_ARRAY) {
          if (OB_FAIL(get_element_in_array(i, &child_val_addr))) {
            LOG_WARN("failed to get child in array.", K(ret), K(i));
          }
        } else { // ObJsonNodeType::J_OBJECT
          if (OB_FAIL(get_element_in_object(i, &child_val_addr))) {
            LOG_WARN("failed to get child in object.", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret) && child_val_addr == expect_val_addr) {
          // found old child, do update
          if (OB_FAIL(update(i, static_cast<ObJsonBin *>(new_node)))) {
            LOG_WARN("replace with new value failed.", K(ret), K(i), K(old_node), K(new_node));
          }
          break;
        }
      }
    }
  }

  return ret;
}

int ObJsonBin::object_remove(const common::ObString &key)
{
  INIT_SUCC(ret);

  if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node type", K(ret), K(json_type()));
  } else if (OB_FAIL(remove(key))) {
    LOG_WARN("fail to remove value in object", K(ret), K(key));
  }
  return ret;
}

int ObJsonBin::array_remove(uint64_t index)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_array_op(index))) {
    LOG_WARN("invalid json array operation", K(ret), K(index));
  } else {
    if (OB_FAIL(remove(index))) {
      LOG_WARN("fail to remove value in array", K(ret), K(index));
    }
  }

  return ret;
}

int ObJsonBin::get_raw_binary(common::ObString &out, ObIAllocator *allocator) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(raw_binary(out, allocator))) {
    LOG_WARN("fail to get json raw binary", K(ret));
  }

  return ret;
}

int ObJsonBin::get_key(uint64_t index, common::ObString &key_out) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_object_op(index))) {
    LOG_WARN("invalid json object operation", K(ret), K(index));
  } else {
    if (OB_FAIL(get_key_in_object(index, key_out))) {
      LOG_WARN("fail to get object key", K(ret), K(index));
    }
  }

  return ret;
}

int ObJsonBin::create_new_binary(ObIJsonBase *&value, ObJsonBin *&new_bin) const
{
  INIT_SUCC(ret);
  ObString sub;

  common::ObIAllocator *allocator = NULL;
  void *buf = NULL;
  if (value != NULL) { // use stack memory
    buf = value;
    allocator = value->get_allocator();
  } else if (OB_ISNULL(allocator_)) { // check allocator_
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json bin allocator is null", K(ret));
  } else { // use allocator_
    allocator = allocator_;
    buf = allocator->alloc(sizeof(ObJsonBin));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc json bin fail", K(ret), K(sizeof(ObJsonBin)));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(raw_binary_at_iter(sub))) {
      LOG_WARN("fail to get sub json binary.", K(ret));
    } else {
      new_bin = new (buf) ObJsonBin(sub.ptr(), sub.length(), allocator);
      if (OB_FAIL(new_bin->reset_iter())) {
        LOG_WARN("fail to reset iter for new json bin", K(ret));
      }
    }
  }

  return ret;
}

int ObJsonBin::get_array_element(uint64_t index, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  ObJsonBin *new_bin = NULL;

  if (OB_FAIL(check_valid_array_op(index))) {
    LOG_WARN("invalid json array operation", K(ret), K(index));
  } else if (OB_FAIL(create_new_binary(value, new_bin))) {
    LOG_WARN("fail to create sub binary", K(ret), K(index));
  } else if (OB_FAIL(new_bin->element(index))) {
    LOG_WARN("fail to access index node for new json bin.", K(ret), K(index));
  } else {
    value = new_bin;
  }

  return ret;
}

int ObJsonBin::get_object_value(uint64_t index, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  ObJsonBin *new_bin = NULL;

  if (OB_FAIL(check_valid_object_op(index))) {
    LOG_WARN("invalid json object operation", K(ret), K(index));
  } else if (OB_FAIL(create_new_binary(value, new_bin))) {
    LOG_WARN("fail to create sub binary", K(ret), K(index));
  } else if (OB_FAIL(new_bin->element(index))) {
    LOG_WARN("fail to access index node for new json bin.", K(ret), K(index));
  } else {
    value = new_bin;
  }

  return ret;
}

int ObJsonBin::get_object_value(const ObString &key, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  ObJsonBin *new_bin = NULL;

  if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node type", K(ret));
  } else if (OB_FAIL(create_new_binary(value, new_bin))) {
    LOG_WARN("fail to create sub binary", K(ret), K(key));
  } else {
    ret = new_bin->lookup(key);
    if (OB_SUCC(ret)) {
      value = new_bin;
    } else if (ret == OB_SEARCH_NOT_FOUND) {
    } else {
      LOG_WARN("fail to access key node for new jsonbin.", K(ret), K(key));
    }
  }

  return ret;
}

int ObJsonBin::serialize_json_object(ObJsonObject *object, ObJsonBuffer &result, uint32_t depth)
{
  UNUSED(depth);
  INIT_SUCC(ret);
  const int64_t st_pos = result.length();
  bool with_key_dict = false; // TODO get from common header
  ObJsonNode *value = NULL;
  uint64_t count = object->element_count();
  // object header [node_type:uint8_t][type:uint8_t][member_count:var][object_size_:var]
  ObJsonBinObjHeader header;
  header.is_continuous_ = 1;
  uint64_t obj_size = object->get_serialize_size();
  header.entry_size_ = ObJsonVar::get_var_type(obj_size);
  header.obj_size_size_ = header.entry_size_;
  header.count_size_ = ObJsonVar::get_var_type(count);
  header.type_ = static_cast<uint8_t>(get_object_vertype());

  ret = result.append(reinterpret_cast<const char*>(&header), OB_JSON_BIN_OBJ_HEADER_LEN);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to append array header node type", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(count, header.count_size_, result))) {
    LOG_WARN("failed to append array header member count", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(obj_size, header.obj_size_size_, result))) {
    LOG_WARN("failed to append array header array size", K(ret));
  }
  // [key_entry][val_entry][key][val]
  // push key offset (check if with key dict)
  uint64_t type_size = ObJsonVar::get_var_size(header.entry_size_);
  uint64_t key_offset_size = type_size * 2 * count;
  uint64_t value_offset_size = (type_size + sizeof(uint8_t)) * count;
  if (OB_SUCC(ret)) {
    if (with_key_dict) {
      // todo fill key dict id
    } else {
      ObString key;
      uint32_t key_offset = static_cast<uint32_t>(result.length() - st_pos + key_offset_size + value_offset_size);
      for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_FAIL(object->get_key(i, key))) {
          LOG_WARN("get key failed.", K(ret), K(i));
        } else if (OB_FAIL(ObJsonVar::append_var(key_offset, header.entry_size_, result))) { // push key offset to st_pos
          LOG_WARN("append key failed.", K(ret), K(key_offset), K(i));
        } else {
          uint32_t key_len = static_cast<uint32_t>(key.length());
          if (OB_FAIL(ObJsonVar::append_var(key_len, header.entry_size_, result))) { //push key len
            LOG_WARN("append key length failed.", K(ret), K(key_len), K(i));
          } else {
            key_offset += key_len; // todo check overflow?
          }
        }
      }
    }
  }

  // extend and fill value entry
  int64_t value_entry_offset = result.length();
  if (OB_SUCC(ret)) {
    ret = result.reserve(value_offset_size);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to extend result", K(ret));
    } else {
      ret = result.set_length(result.length() + value_offset_size);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to set length result", K(ret));
      }
    }
  }

  // keys (when without keydict and has_key)
  if (!with_key_dict) {
    ObString key;
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (OB_FAIL(object->get_key(i, key))) {
        LOG_WARN("get key failed.", K(ret), K(i));
      } else if (OB_FAIL(result.append(key.ptr(), key.length()))) {
        LOG_WARN("failed to append key to result", K(ret), K(key));
      }
    }
  }

  // values
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (OB_ISNULL(value = object->get_value(i))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("value is null", K(ret), K(i));
    } else {
      uint32_t value_offset = result.length() - st_pos;
      // recursion(parse value entry into func, simple type can store on value entry)
      if (!try_update_inline(value, header.entry_size_, &value_entry_offset, result)) {
        ret = serialize_json_value(value, result);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to append key to result", K(ret));
        } else {
          // fill value entry
          if (OB_SUCC(ObJsonVar::set_var(value_offset, header.entry_size_, result.ptr() + value_entry_offset))) {
            value_entry_offset += type_size;
            // fill value type
            uint8_t *value_type_ptr = reinterpret_cast<uint8_t*>(result.ptr() + value_entry_offset);
            *value_type_ptr = static_cast<uint8_t>(value->json_type());
            value_entry_offset += sizeof(uint8_t);
          }
        }
      }
    }
  }

  // fill header obj size
  if (OB_SUCC(ret)) {
    uint64_t real_obj_size = static_cast<uint64_t>(result.length() - st_pos);
    if (ObJsonVar::get_var_type(real_obj_size) > ObJsonVar::get_var_type(obj_size)) {
      if (depth >= OB_JSON_BIN_MAX_SERIALIZE_TIME) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to calc object size", K(ret), K(real_obj_size), K(obj_size));
      } else {
        int64_t delta_size = real_obj_size - obj_size;
        object->set_serialize_delta_size(delta_size);
        result.set_length(st_pos);
        ret = serialize_json_object(object, result, depth + 1);
      }
    } else {
      ObJsonBinObjHeader *header = reinterpret_cast<ObJsonBinObjHeader*>(result.ptr() + st_pos);
      real_obj_size = static_cast<uint64_t>(result.length() - st_pos);
      ObJsonVar::set_var(real_obj_size, header->obj_size_size_, header->used_size_ + ObJsonVar::get_var_size(header->count_size_));
    }
  }

  return ret;
}

int ObJsonBin::serialize_json_array(ObJsonArray *array, ObJsonBuffer &result, uint32_t depth)
{
  UNUSED(depth);
  INIT_SUCC(ret);
  const int64_t st_pos = result.length();
  uint64_t count = array->element_count();
  // object header [node_type:uint8_t][type:uint8_t][member_count:var][object_size_:var]
  ObJsonBinArrHeader header;
  header.is_continuous_ = 1;
  uint64_t array_size = array->get_serialize_size();
  header.entry_size_ = ObJsonVar::get_var_type(array_size);
  header.obj_size_size_ = header.entry_size_;
  header.count_size_ = ObJsonVar::get_var_type(count);
  header.type_ = static_cast<uint8_t>(get_array_vertype());


  if (OB_FAIL(result.append(reinterpret_cast<const char*>(&header), OB_JSON_BIN_ARR_HEADER_LEN))) {
    LOG_WARN("failed to append array header node type", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(count, header.count_size_, result))) {
    LOG_WARN("failed to append array header member count", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(array_size, header.obj_size_size_, result))) {
    LOG_WARN("failed to append array header array size", K(ret));
  }

  uint64_t type_size = ObJsonVar::get_var_size(header.entry_size_);
  int64_t value_offset_size = (type_size + sizeof(uint8_t)) * count;
  int64_t value_entry_offset = result.length();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result.reserve(value_offset_size))) {
      LOG_WARN("failed to extend result", K(ret), K(value_offset_size));
    } else if (OB_FAIL(result.set_length(result.length() + value_offset_size))) {
      LOG_WARN("failed to set length result", K(ret));
    }
  }

  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObJsonNode *value = (*array)[i];
    uint32_t value_offset = result.length() - st_pos;
    // recursion(parse value entry into func, simple type can store on value entry)
    if (!try_update_inline(value, header.entry_size_, &value_entry_offset, result)) {
      ret = serialize_json_value(value, result);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append key to result", K(ret));
      } else {
        // fill value entry
        if (OB_SUCC(ObJsonVar::set_var(value_offset, header.entry_size_, result.ptr() + value_entry_offset))) {
          value_entry_offset += type_size;
          // fill value type
          uint8_t *value_type_ptr = reinterpret_cast<uint8_t*>(result.ptr() + value_entry_offset);
          *value_type_ptr = static_cast<uint8_t>(value->json_type());
          value_entry_offset += sizeof(uint8_t);
        }
      }
    }
  }

  // fill header obj size
  if (OB_SUCC(ret)) {
    ObJsonBinObjHeader *header = reinterpret_cast<ObJsonBinObjHeader*>(result.ptr() + st_pos);
    uint64_t real_array_size = static_cast<uint64_t>(result.length() - st_pos);
    if (ObJsonVar::get_var_type(real_array_size) > ObJsonVar::get_var_type(array_size)) {
      if (depth >= OB_JSON_BIN_MAX_SERIALIZE_TIME) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to calc array size", K(ret));
      } else {
        int64_t delta_size = real_array_size - array_size;
        array->set_serialize_delta_size(delta_size);
        result.set_length(st_pos);
        ret = serialize_json_array(array, result, depth + 1);
      }
    } else {
      uint64_t count_var_size = ObJsonVar::get_var_size(header->count_size_);
      if (OB_FAIL(ObJsonVar::set_var(real_array_size, header->obj_size_size_, header->used_size_ + count_var_size))) {
        LOG_WARN("failed to set array size.", K(ret), K(array_size));
      }
    }
  }

  return ret;
}

int ObJsonBin::serialize_json_integer(int64_t value, ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  int64_t ser_len = serialization::encoded_length_vi64(value);
  int64_t pos = result.length();
  if (OB_FAIL(result.reserve(ser_len))) {
    LOG_WARN("failed to reserver serialize size for int json obj", K(ret), K(ser_len));
  } else if (OB_FAIL(serialization::encode_vi64(result.ptr(), result.capacity(), pos, value))) {
    LOG_WARN("failed to serialize for int json obj", K(ret), K(ser_len));
  } else if (OB_FAIL(result.set_length(pos))) {
    LOG_WARN("failed to update len for int json obj", K(ret), K(pos));
  }
  return ret;
}

// [precision] [scale]   [value]
// [int16_t]   [int16_t] [val_len]
int ObJsonBin::serialize_json_decimal(ObJsonDecimal *json_dec, ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  ObPrecision prec = json_dec->get_precision();
  ObScale scale = json_dec->get_scale();
  int64_t ser_len = json_dec->get_serialize_size();
  int64_t pos = result.length();
  if (OB_FAIL(result.reserve(ser_len))) {
    LOG_WARN("failed to reserver serialize size for decimal json obj", K(ret), K(pos), K(ser_len));
  } else if (OB_FAIL(serialization::encode_i16(result.ptr(), result.capacity(), pos, prec))) {
    LOG_WARN("failed to serialize for decimal precision", K(ret), K(pos), K(prec));
  } else if (OB_FAIL(result.set_length(pos))) {
    LOG_WARN("failed to set length for decimal precision", K(ret), K(pos), K(prec));
  } else if (OB_FAIL(serialization::encode_i16(result.ptr(), result.capacity(), pos, scale))) {
    LOG_WARN("failed to serialize for decimal precision", K(ret), K(pos), K(scale));
  } else if (OB_FAIL(result.set_length(pos))) {
    LOG_WARN("failed to set length for decimal scale", K(ret), K(pos), K(scale));
  } else if (OB_FAIL(json_dec->value().serialize(result.ptr(), result.capacity(), pos))) {
    LOG_WARN("failed to serialize for decimal value", K(ret), K(pos));
  } else if (OB_FAIL(result.set_length(pos))){
    LOG_WARN("failed to update len for decimal json obj", K(ret), K(pos));
  }
  return ret;
}

int ObJsonBin::serialize_json_value(ObJsonNode *json_tree, ObJsonBuffer &result)
{
  INIT_SUCC(ret);
  switch (json_tree->json_type()) {
    case ObJsonNodeType::J_NULL: {
      if (OB_FAIL(result.append("\0", sizeof(char)))) {
        LOG_WARN("failed to append null json obj", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      ObJsonDecimal *json_dec = static_cast<ObJsonDecimal*>(json_tree);
      if (OB_FAIL(serialize_json_decimal(json_dec, result))) {
        LOG_WARN("failed to serialize json decimal", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      const ObJsonInt *i = static_cast<const ObJsonInt*>(json_tree);
      int64_t value = i->value();
      if (OB_FAIL(serialize_json_integer(value, result))) {
        LOG_WARN("failed to serialize json int", K(ret), K(value));
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      const ObJsonUint *i = static_cast<const ObJsonUint*>(json_tree);
      uint64_t value = i->value();
      if (OB_FAIL(serialize_json_integer(value, result))) {
        LOG_WARN("failed to serialize json uint", K(ret), K(value));
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      const ObJsonDouble *d = static_cast<const ObJsonDouble*>(json_tree);
      double value = d->value();
      if (isnan(value) || isinf(value)) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("invalid double value", K(ret), K(value));
      } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&value), sizeof(double)))) {
        LOG_WARN("failed to append double json obj", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      const ObJsonOFloat *d = static_cast<const ObJsonOFloat*>(json_tree);
      float value = d->value();
      if (isnan(value) || isinf(value)) {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("invalid float value", K(ret), K(value));
      } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&value), sizeof(float)))) {
        LOG_WARN("failed to append float json obj", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID:
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH:
    case ObJsonNodeType::J_STRING: { // [type][length][string]
      const ObJsonString *sub_obj = static_cast<const ObJsonString*>(json_tree);
      int64_t ser_len = serialization::encoded_length_vi64(sub_obj->length());
      int64_t pos = result.length() + sizeof(uint8_t);
      ObJBVerType vertype = ObJsonVerType::get_json_vertype(json_tree->json_type());
      if (OB_FAIL(result_.append(reinterpret_cast<const char*>(&vertype), sizeof(uint8_t)))) {
        LOG_WARN("failed to serialize type for str json obj", K(ret), K(ser_len));
      } else if (OB_FAIL(result.reserve(ser_len))) {
        LOG_WARN("failed to reserver serialize size for str json obj", K(ret), K(ser_len));
      } else if (OB_FAIL(serialization::encode_vi64(result.ptr(), result.capacity(), pos, sub_obj->length()))) {
        LOG_WARN("failed to serialize for str json obj", K(ret), K(ser_len));
      } else if (OB_FAIL(result.set_length(pos))) {
        LOG_WARN("failed to update len for str json obj", K(ret), K(pos));
      } else if (OB_FAIL(result.append(sub_obj->value().ptr(), sub_obj->length()))) {
        LOG_WARN("failed to append string json obj value", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_OBJECT: {
      ObJsonObject *object = static_cast<ObJsonObject*>(json_tree);
      if (OB_FAIL(serialize_json_object(object, result))) {
        LOG_WARN("failed to append object json obj", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      ObJsonArray *array = static_cast<ObJsonArray*>(json_tree);
      if (OB_FAIL(serialize_json_array(array, result))) {
        LOG_WARN("failed to append array json obj", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      const ObJsonBoolean *b = static_cast<const ObJsonBoolean*>(json_tree);
      char value = static_cast<char>(b->value());
      ret = result.append(reinterpret_cast<const char*>(&value), sizeof(char));
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append bool json obj", K(ret));
      }
      break;
    }

    case ObJsonNodeType::J_ORACLEDATE:
    case ObJsonNodeType::J_DATE: {
      const ObJsonDatetime *sub_obj = static_cast<const ObJsonDatetime*>(json_tree);
      ObTime ob_time = sub_obj->value();
      int32_t value = ObTimeConverter::ob_time_to_date(ob_time);
      if (OB_FAIL(result.append(reinterpret_cast<const char*>(&value), sizeof(int32_t)))) {
        LOG_WARN("failed to append date json obj value", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_TIME: {
      const ObJsonDatetime *sub_obj = static_cast<const ObJsonDatetime*>(json_tree);
      ObTime ob_time = sub_obj->value();
      int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
      if (OB_FAIL(result.append(reinterpret_cast<const char*>(&value), sizeof(int64_t)))) {
        LOG_WARN("failed to append time json obj value", K(ret));
      }
      break;
    }

    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ:
    case ObJsonNodeType::J_DATETIME: {
      const ObJsonDatetime *sub_obj = static_cast<const ObJsonDatetime*>(json_tree);
      ObTime ob_time = sub_obj->value();
      ObTimeConvertCtx crtx(NULL, false);
      int64_t value;
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, crtx, value))) {
        LOG_WARN("failed to convert time to datetime", K(ret));
      } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&value), sizeof(int64_t)))) {
        LOG_WARN("failed to append datetime json obj value", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP: {
      const ObJsonDatetime *sub_obj = static_cast<const ObJsonDatetime*>(json_tree);
      ObTime ob_time = sub_obj->value();
      ObTimeConvertCtx crtx(NULL, false);
      int64_t value;
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, crtx, value))) {
        LOG_WARN("failed to convert time to datetime", K(ret));
      } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&value), sizeof(int64_t)))) {
        LOG_WARN("failed to append timestamp json obj value", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: { // [type][field_type][length][value]
      const ObJsonOpaque *sub_obj = static_cast<const ObJsonOpaque*>(json_tree);
      uint64_t obj_size = sub_obj->size();
      uint16_t field_type = static_cast<uint16_t>(sub_obj->field_type());
      ObJBVerType vertype = get_opaque_vertype();
      if (OB_FAIL(result_.append(reinterpret_cast<const char*>(&vertype), sizeof(uint8_t)))) {
        LOG_WARN("failed to serialize type for str json obj", K(ret), K(vertype));
      } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&field_type), sizeof(uint16_t)))) {
        LOG_WARN("failed to append opaque json obj type", K(ret));
      } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&obj_size), sizeof(uint64_t)))) {
        LOG_WARN("failed to append opaque json obj size", K(ret));
      } else if (OB_FAIL(result.append(sub_obj->value(), sub_obj->size()))) {
        LOG_WARN("failed to append opaque json obj value", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid extend type.", K(ret), K(json_tree->json_type()));
      break;
    }
  }

  return ret;
}

bool ObJsonBin::try_update_inline(const ObJsonNode *value,
                                  uint8_t var_type,
                                  int64_t *value_entry_offset,
                                  ObJsonBuffer &result)
{
  bool is_update_inline = false;
  uint64_t inlined_val;
  uint8_t inlined_type = static_cast<uint8_t>(value->json_type());
  switch (value->json_type()) {
    case ObJsonNodeType::J_NULL: {
      inlined_val = 0;
      is_update_inline = true;
      inlined_type = static_cast<uint8_t>(get_null_vertype());
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      const ObJsonInt *i = static_cast<const ObJsonInt*>(value);
      if (ObJsonVar::get_var_type(i->value()) <= var_type) {
        inlined_val = ObJsonVar::var_int2uint(i->value());
        is_update_inline = true;
        inlined_type =  static_cast<uint8_t>(get_int_vertype());
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      const ObJsonUint *i = static_cast<const ObJsonUint*>(value);
      if (ObJsonVar::get_var_type(i->value()) <= var_type) {
        inlined_val = i->value();
        is_update_inline = true;
        inlined_type =  static_cast<uint8_t>(get_uint_vertype());
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      const ObJsonBoolean *i = static_cast<const ObJsonBoolean*>(value);
      inlined_val = static_cast<uint64_t>(i->value());
      is_update_inline = true;
      inlined_type =  static_cast<uint8_t>(get_boolean_vertype());
      break;
    }
    default: {
      break;
    }
  }

  // set inline
  if (is_update_inline) {
    uint64_t type_size = ObJsonVar::get_var_size(var_type);
    // if inlined set first high bit to 1
    inlined_type |= OB_JSON_TYPE_INLINE_MASK;
    INIT_SUCC(ret);
    if (OB_FAIL(ObJsonVar::set_var(inlined_val, var_type, result.ptr() + *value_entry_offset))) {
      is_update_inline = false;
      LOG_WARN("fail to set inlined val.", K(ret), K(inlined_val), K(var_type));
    } else {
      *value_entry_offset += type_size;
      // fill value type
      uint8_t *value_type_ptr = reinterpret_cast<uint8_t*>(result.ptr() + *value_entry_offset);
      *value_type_ptr = inlined_type;
      *value_entry_offset += sizeof(uint8_t);
    }
  }
  return is_update_inline;
}

bool ObJsonBin::try_update_inline(const ObJsonBin *value,
                                  uint8_t var_type,
                                  int64_t *value_entry_offset,
                                  ObJsonBuffer &result)
{
  bool is_update_inline = false;
  ObJsonNodeType j_type = value->json_type();
  uint64_t inlined_val;
  uint8_t inlined_type = static_cast<uint8_t>(j_type);
  switch (j_type) {
    case ObJsonNodeType::J_NULL: {
      inlined_val = 0;
      is_update_inline = true;
      inlined_type =  static_cast<uint8_t>(get_null_vertype());
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      if (ObJsonVar::get_var_type(value->get_int()) <= var_type) {
        inlined_val = ObJsonVar::var_int2uint(value->get_int());
        is_update_inline = true;
        inlined_type =  static_cast<uint8_t>(get_int_vertype());
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      if (ObJsonVar::get_var_type(value->get_uint()) <= var_type) {
        inlined_val = value->get_uint();
        is_update_inline = true;
        inlined_type = static_cast<uint8_t>(get_uint_vertype());
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      inlined_val = static_cast<uint64_t>(value->get_boolean());
      is_update_inline = true;
      inlined_type =  static_cast<uint8_t>(get_boolean_vertype());
      break;
    }
    default: {
      LOG_INFO("unsupport inline type.", K(j_type));
      break;
    }
  }

  // set inline
  if (is_update_inline) {
    uint64_t type_size = ObJsonVar::get_var_size(var_type);
    // if inlined set first high bit to 1
    inlined_type |= OB_JSON_TYPE_INLINE_MASK;
    INIT_SUCC(ret);
    if (OB_FAIL(ObJsonVar::set_var(inlined_val, var_type, result.ptr() + *value_entry_offset))) {
      is_update_inline = false;
      LOG_WARN("fail to set inlined val.", K(ret), K(inlined_val), K(var_type));
    } else {
      *value_entry_offset += type_size;
      // fill value type
      uint8_t *value_type_ptr = reinterpret_cast<uint8_t*>(result.ptr() + *value_entry_offset);
      *value_type_ptr = inlined_type;
      *value_entry_offset += sizeof(uint8_t);
    }
  }
  return is_update_inline;
}

int ObJsonBin::parse_tree(ObJsonNode *json_tree)
{
  INIT_SUCC(ret);
  result_.reuse();
  ObJsonNodeType root_type = json_tree->json_type();
  if (OB_FAIL(result_.reserve(json_tree->get_serialize_size()))) {
    LOG_WARN("failed to reserve bin buffer", K(ret), K(json_tree->get_serialize_size()));
  } else if (root_type == ObJsonNodeType::J_ARRAY || root_type == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(serialize_json_value(json_tree, result_))) { // do recursion
      LOG_WARN("failed to serialize json tree at recursion", K(ret));
      result_.reset();
    }
  } else {
    ObJBVerType ver_type = ObJsonVerType::get_json_vertype(root_type);
    if (!ObJsonVerType::is_opaque_or_string(ver_type) &&
        OB_FAIL(result_.append(reinterpret_cast<const char*>(&ver_type), sizeof(uint8_t)))) {
      LOG_WARN("failed to serialize json tree at append used size", K(ret), K(result_.length()));
      result_.reset();
    } else if (OB_FAIL(serialize_json_value(json_tree, result_))) { // do recursion
      LOG_WARN("failed to serialize json tree at recursion", K(ret));
      result_.reset();
    }
  }

  if (OB_SUCC(ret))  {
    curr_.assign_ptr(result_.ptr(), result_.length());
    is_alloc_ = true;
    reset_iter();
  }
  return ret;
}

// binary do to tree base on iter position
int ObJsonBin::to_tree(ObJsonNode *&json_tree)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to deserialize with NULL alloctor.", K(ret));
  } else {
    char *ptr = curr_.ptr();
    uint64_t offset = pos_;
    uint8_t type = type_;
    ObJsonNodeType curr_type = json_type();
    if (curr_type == ObJsonNodeType::J_OBJECT ||
        curr_type == ObJsonNodeType::J_ARRAY ||
        ObJsonVerType::is_opaque_or_string(static_cast<ObJBVerType>(type_))) {
      type = static_cast<uint8_t>(curr_type);
    } else {
      // inline value store all store in union
      offset = OB_JSON_TYPE_IS_INLINE(type) ? uint_val_ : offset;
      type = ObJsonVar::get_var_type(offset);
    }
    if (OB_FAIL(deserialize_json_value(ptr + offset, curr_.length() - offset, type_, offset, json_tree, type))) {
      LOG_WARN("deserialize failed", K(ret), K(offset), K(type));
    }
  }

  return ret;
}

int ObJsonBin:: deserialize_json_value(const char *data,
                                      uint64_t length,
                                      uint8_t type,
                                      uint64_t value_offset,
                                      ObJsonNode *&json_tree,
                                      uint64_t type_size)
{
  INIT_SUCC(ret);
  bool is_inlined = OB_JSON_TYPE_IS_INLINE(type);
  ObJBVerType node_vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type));
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(node_vertype);
  switch (node_type) {
    case ObJsonNodeType::J_NULL: {
      void *buf = allocator_->alloc(sizeof(ObJsonNull));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for null json node", K(ret));
      } else {
        ObJsonNull *node = new(buf)ObJsonNull();
        json_tree = static_cast<ObJsonNode*>(node);
      }
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      ObPrecision prec = -1;
      ObScale scale = -1;
      number::ObNumber num;
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_i16(data, length, pos, &prec))) {
        LOG_WARN("fail to deserialize decimal precision.", K(ret), K(length));
      } else if (OB_FAIL(serialization::decode_i16(data, length, pos, &scale))) {
        LOG_WARN("fail to deserialize decimal scale.", K(ret), K(length), K(prec));
      } else if (OB_FAIL(num.deserialize(data, length, pos))) {
        LOG_WARN("fail to deserialize number.", K(ret), K(length));
      } else {
        void *buf = allocator_->alloc(sizeof(ObJsonDecimal));
        if (buf == NULL) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for decimal json node", K(ret));
        } else {
          if (node_type == ObJsonNodeType::J_DECIMAL) {
            json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonDecimal(num, prec, scale));
          } else {
            json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonODecimal(num, prec, scale));
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT: {
      void *buf = allocator_->alloc(sizeof(ObJsonInt));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for int json node", K(ret));
      } else {
        if (is_inlined) {
          ObJsonInt *node = new(buf)ObJsonInt(ObJsonVar::var_uint2int(value_offset, type_size));
          json_tree = static_cast<ObJsonNode*>(node);
        } else {
          int64_t val = 0;
          int64_t pos = 0;
          if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
            LOG_WARN("fail to decode int val.", K(ret), K(length));
            allocator_->free(buf);
          } else {
            if (node_type == ObJsonNodeType::J_INT) {
              json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonInt(val));
            } else {
              json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonOInt(val));
            }
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      void *buf = allocator_->alloc(sizeof(ObJsonUint));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for uint json node", K(ret));
      } else {
        if (is_inlined) {
          ObJsonUint *node = new(buf)ObJsonUint(value_offset);
          json_tree = static_cast<ObJsonNode*>(node);
        } else {
          int64_t val = 0;
          int64_t pos = 0;
          if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
            LOG_WARN("fail to decode uint val.", K(ret), K(length));
            allocator_->free(buf);
          } else {
            uint64_t uval = static_cast<uint64_t>(val);
            if (node_type == ObJsonNodeType::J_UINT) {
              json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonUint(uval));
            } else {
              json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonOLong(uval));
            }
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      void *buf = allocator_->alloc(sizeof(ObJsonDouble));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for double json node", K(ret));
      } else if (length < sizeof(double)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough.", K(ret), K(length), K(sizeof(double)));
        allocator_->free(buf);
      } else {
        double val = *reinterpret_cast<const double*>(data);
        if (node_type == ObJsonNodeType::J_DOUBLE) {
          json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonDouble(val));
        } else {
          json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonODouble(val));
        }
      }
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      void *buf = allocator_->alloc(sizeof(ObJsonOFloat));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for float json node", K(ret));
      } else if (length < sizeof(float)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough.", K(ret), K(length));
        allocator_->free(buf);
      } else {
        float val = *reinterpret_cast<const float*>(data);
        ObJsonOFloat *node = new(buf) ObJsonOFloat(val);
        json_tree = static_cast<ObJsonNode*>(node);
      }
      break;
    }
    case ObJsonNodeType::J_STRING: {
      void *buf = allocator_->alloc(sizeof(ObJsonString));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for str json node", K(ret));
      } else {
        int64_t val = 0;
        int64_t pos = 0;

        ObJBVerType vertype = *reinterpret_cast<const ObJBVerType*>(data);
        if (vertype == ObJBVerType::J_STRING_V0) {
          pos += sizeof(uint8_t);
          if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
            LOG_WARN("decode str length failed.", K(ret));
          } else if (length < pos + val) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length is not enough.", K(ret), K(length), K(pos), K(val));
          } else {
            uint64_t str_length = static_cast<uint64_t>(val);
            if (str_length == 0) {
              LOG_DEBUG("empty string in json binary", K(str_length), K(pos), K(length));
              ObJsonString *empty_str_node = new(buf)ObJsonString(NULL, 0);
              json_tree = static_cast<ObJsonNode*>(empty_str_node);
            } else {
              void *str_buf = allocator_->alloc(str_length);
              if (str_buf == NULL) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc memory for data buf", K(ret), K(str_length));
              } else {
                MEMCPY(str_buf, data + pos, str_length);
                ObJsonString *node = new(buf)ObJsonString(reinterpret_cast<const char*>(str_buf), str_length);
                json_tree = static_cast<ObJsonNode*>(node);
              }
            }
          }
        } else {
          // other version process
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid ver type.", K(ret), K(vertype));
        }
      }
      break;
    }
    case ObJsonNodeType::J_OBJECT: {
      void *buf = allocator_->alloc(sizeof(ObJsonObject));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for obj json node", K(ret));
      } else {
        ObJsonObject *node = new(buf)ObJsonObject(allocator_);
        ret = deserialize_json_object(data, length, node, node_vertype);
        if (OB_SUCC(ret)) {
          json_tree = static_cast<ObJsonNode*>(node);
        } else {
          node->clear();
          allocator_->free(node);
        }
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      void *buf = allocator_->alloc(sizeof(ObJsonArray));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for array json node", K(ret));
      } else {
        ObJsonArray *node = new(buf)ObJsonArray(allocator_);
        ret = deserialize_json_array(data, length, node, node_vertype);
        if (OB_SUCC(ret)) {
          json_tree = static_cast<ObJsonNode*>(node);
        } else {
          node->clear();
          allocator_->free(node);
        }
      }
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      void *buf = allocator_->alloc(sizeof(ObJsonBoolean));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for boolean json node", K(ret));
      } else {
        bool val = is_inlined ? static_cast<bool>(value_offset) : static_cast<bool>(*data);
        ObJsonBoolean *node = new(buf)ObJsonBoolean(val);
        json_tree = static_cast<ObJsonNode*>(node);
      }
      break;
    }
    case ObJsonNodeType::J_DATE: {
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for date json node", K(ret));
      } else if (length < sizeof(int32_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough.", K(ret), K(length), K(sizeof(int32_t)));
      } else {
        ObTime ob_time;
        int32_t value = *reinterpret_cast<const int32_t*>(data);
        if (OB_FAIL(ObTimeConverter::date_to_ob_time(value, ob_time))) {
          LOG_WARN("fail to convert date to ob time", K(ret));
        } else {
          ObJsonDatetime *node = new(buf)ObJsonDatetime(node_type, ob_time);
          json_tree = static_cast<ObJsonNode*>(node);
        }
      }
      break;
    }
    case ObJsonNodeType::J_TIME: {
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for time json node", K(ret));
      } else if (length < sizeof(int64_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough.", K(ret), K(length), K(sizeof(int64_t)));
      } else {
        ObTime ob_time;
        int64_t value = *reinterpret_cast<const int64_t*>(data);
        ObTimeConverter::time_to_ob_time(value, ob_time);
        ObJsonDatetime *node = new(buf)ObJsonDatetime(node_type, ob_time);
        json_tree = static_cast<ObJsonNode*>(node);
      }
      break;
    }
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_ORACLEDATE: {
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for datetime json node", K(ret));
      } else if (length < sizeof(int64_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough.", K(ret), K(length), K(sizeof(int64_t)));
      } else {
        ObTime ob_time;
        int64_t value = *reinterpret_cast<const int64_t*>(data);
        if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, NULL, ob_time))) {
          LOG_WARN("fail to convert datetime to ob time", K(ret));
        } else {
          ObJsonDatetime *node = new(buf)ObJsonDatetime(node_type, ob_time);
          json_tree = static_cast<ObJsonNode*>(node);
        }
      }
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for timestamp json node", K(ret));
      } else if (length < sizeof(int64_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough.", K(ret), K(length), K(sizeof(int64_t)));
      } else {
        ObTime ob_time;
        int64_t value = *reinterpret_cast<const int64_t*>(data);
        if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, NULL, ob_time))) {
          LOG_WARN("fail to convert timestamp to ob time", K(ret));
        } else {
          ObJsonDatetime *node = new(buf)ObJsonDatetime(node_type, ob_time);
          json_tree = static_cast<ObJsonNode*>(node);
        }
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      ObJBVerType vertype = *reinterpret_cast<const ObJBVerType*>(data);
      if (vertype == ObJBVerType::J_OPAQUE_V0) {
        uint64_t need_len = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t);
        ObObjType field_type = static_cast<ObObjType>(*reinterpret_cast<const uint16_t*>(data + sizeof(uint8_t)));
        uint64_t val_length = *reinterpret_cast<const uint64_t*>(data + sizeof(uint8_t) + sizeof(uint16_t));
        void *buf = allocator_->alloc(sizeof(ObJsonOpaque));
        if (buf == NULL) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for opaque json node", K(ret));
        } else if (length < need_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough.", K(ret), K(length), K(need_len), K(val_length));
        } else if (val_length == 0) {
          LOG_DEBUG("empty opaque in json binary", K(val_length), K(field_type), K(length));
          ObString empty_value(0, NULL);
          ObJsonOpaque *empy_opa_node = new(buf)ObJsonOpaque(empty_value, field_type);
          json_tree = static_cast<ObJsonNode*>(empy_opa_node);
        } else {
          if (length < need_len + val_length) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length is not enough.", K(ret), K(length), K(need_len), K(val_length));
          } else {
            void *str_buf = allocator_->alloc(val_length);
            if (str_buf == NULL) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for data buf", K(ret), K(val_length));
            } else {
              MEMCPY(str_buf, data + sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint64_t), val_length);
              ObString value(val_length, reinterpret_cast<const char*>(str_buf));
              ObJsonOpaque *node = new(buf)ObJsonOpaque(value, field_type);
              json_tree = static_cast<ObJsonNode*>(node);
            }
          }
        }
      } else {
        // other version process
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid ver type.", K(ret), K(vertype));
      }
      break;
    }
    case ObJsonNodeType::J_OBINARY:
    case ObJsonNodeType::J_OOID:
    case ObJsonNodeType::J_ORAWHEX:
    case ObJsonNodeType::J_ORAWID: {
      void *buf = allocator_->alloc(sizeof(ObJsonORawString));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for str json node", K(ret));
      } else {
        int64_t val = 0;
        int64_t pos = 0;

        pos += sizeof(uint8_t);
        if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
          LOG_WARN("decode str length failed.", K(ret));
        } else if (length < pos + val) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough.", K(ret), K(length), K(pos), K(val));
        } else {
          uint64_t str_length = static_cast<uint64_t>(val);
          if (str_length == 0) {
            LOG_DEBUG("empty string in json binary", K(str_length), K(pos), K(length));
            ObJsonString *empty_str_node = new(buf)ObJsonString(NULL, 0);
            json_tree = static_cast<ObJsonNode*>(empty_str_node);
          } else {
            void *str_buf = allocator_->alloc(str_length);
            if (str_buf == NULL) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for data buf", K(ret), K(str_length));
            } else {
              MEMCPY(str_buf, data + pos, str_length);
              ObJsonORawString *node = new(buf)ObJsonORawString(reinterpret_cast<const char*>(str_buf),
                                                                str_length, node_type);
              json_tree = static_cast<ObJsonNode*>(node);
            }
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_ODAYSECOND:
    case ObJsonNodeType::J_OYEARMONTH: {
      void *buf = allocator_->alloc(sizeof(ObJsonOInterval));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for str json node", K(ret));
      } else {
        int64_t val = 0;
        int64_t pos = 0;

        pos += sizeof(uint8_t);
        if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
          LOG_WARN("decode str length failed.", K(ret));
        } else if (length < pos + val) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough.", K(ret), K(length), K(pos), K(val));
        } else {
          uint64_t str_length = static_cast<uint64_t>(val);
          if (str_length == 0) {
            LOG_DEBUG("empty string in json binary", K(str_length), K(pos), K(length));
            ObJsonString *empty_str_node = new(buf)ObJsonString(NULL, 0);
            json_tree = static_cast<ObJsonNode*>(empty_str_node);
          } else {
            void *str_buf = allocator_->alloc(str_length);
            if (str_buf == NULL) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for data buf", K(ret), K(str_length));
            } else {
              MEMCPY(str_buf, data + pos, str_length);
              ObObjType field_type = node_type == ObJsonNodeType::J_ODAYSECOND ? ObIntervalYMType : ObIntervalDSType;
              ObJsonOInterval *node = new(buf)ObJsonOInterval(reinterpret_cast<const char*>(str_buf),
                                                              str_length, field_type);
              json_tree = static_cast<ObJsonNode*>(node);
            }
          }
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

int ObJsonBin::deserialize_json_object_v0(const char *data, uint64_t length, ObJsonObject *object)
{
  INIT_SUCC(ret);
  bool with_key_dict = false; // TODO how to judge key dict
  uint64_t offset = 0;
  uint8_t node_type, type, obj_size_type;
  uint64_t count, obj_size;
  if (length <= OB_JSON_BIN_OBJ_HEADER_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to access data for length is not enough.", K(ret), K(length));
  } else {
    parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
    object->set_serialize_size(obj_size);
    uint64_t type_size = ObJsonVar::get_var_size(type);
    uint64_t key_entry_size = type_size * 2;
    uint64_t val_entry_size = (type_size + sizeof(uint8_t));

    const char *key_entry = (data + offset);
    const char *val_entry = (key_entry + key_entry_size * count);
    uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
    if (offset + key_entry_size * count + val_entry_size * count > length) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to access data for length is not enough.",
               K(ret), K(length), K(offset), K(key_entry_size), K(val_entry_size), K(count));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      uint64_t key_offset, key_len, value_offset, val_type;
      if (OB_FAIL(ObJsonVar::read_var(key_entry + key_entry_size * i, type, &key_offset))) {
        LOG_WARN("failed to read key offset", K(ret));
      } else if (OB_FAIL(ObJsonVar::read_var(key_entry + key_entry_size * i + type_size, type, &key_len))) {
        LOG_WARN("failed to read key len", K(ret));
      } else if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * i, type, &value_offset))) {
        LOG_WARN("failed to read val offset", K(ret));
      } else if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * i + type_size, v_type, &val_type))) {
        LOG_WARN("failed to read val type", K(ret));
      } else if (key_offset >= length) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to access data for length is not enough.", K(ret), K(length), K(key_offset));
      } else {
        // TODO if with key dict, read key from dict
        // to consider, add option to controll need alloc or not
        void *key_buf = nullptr;
        if (key_len > 0) {
          key_buf = allocator_->alloc(key_len);
          if (key_buf == NULL) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory for data buf", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          MEMCPY(key_buf, data + key_offset, key_len);
          ObString key(key_len, reinterpret_cast<const char*>(key_buf));
          const char *val = data + value_offset;
          ObJsonNode *node = NULL;
          ret = deserialize_json_value(val, length - value_offset, val_type, value_offset, node, type);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(object->add(key, node, false, true, false))) {
              LOG_WARN("failed to add node to obj", K(ret));
            }
          } else {
            LOG_WARN("failed to deserialize child node.", K(ret), K(i), K(val_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::deserialize_json_object(const char *data, uint64_t length, ObJsonObject *object, ObJBVerType vertype)
{
  INIT_SUCC(ret);
  switch(vertype) {
    case ObJBVerType::J_OBJECT_V0: {
      ret = deserialize_json_object_v0(data, length, object);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid vertype", K(vertype));
      break;
    }
  }
  return ret;
}

int ObJsonBin::deserialize_json_array_v0(const char *data, uint64_t length, ObJsonArray *array)
{
  INIT_SUCC(ret);
  uint64_t offset = 0;
  uint8_t node_type, type, obj_size_type;
  uint64_t count, obj_size;
  if (length <= OB_JSON_BIN_OBJ_HEADER_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to access data for length is not enough.", K(ret), K(length));
  } else {
    parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
    array->set_serialize_size(obj_size);
    uint64_t type_size = ObJsonVar::get_var_size(type);
    uint64_t val_entry_size = (type_size + sizeof(uint8_t));

    const char *val_entry = (data + offset);
    uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
    if (offset + val_entry_size * count > length) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to access data for length is not enough.",
               K(ret), K(length), K(offset), K(val_entry_size), K(count));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++){
      uint64_t val_offset, val_type;
      if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * i, type, &val_offset))) {
        LOG_WARN("failed to read val offset", K(ret), K(i), K(val_entry_size), K(type));
      } else if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * i + type_size, v_type, &val_type))) {
        LOG_WARN("failed to read val type", K(ret), K(i), K(val_entry_size), K(type));
      } else {
        const char *val = data + val_offset;
        ObJsonNode *node = NULL;
        if (OB_FAIL(deserialize_json_value(val, length - val_offset, val_type, val_offset, node, type))) {
          LOG_WARN("failed to deserialize child node", K(ret), K(i), K(val_type), K(val_offset));
        } else if (OB_FAIL(array->append(node))) {
          LOG_WARN("failed to append node to array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::deserialize_json_array(const char *data, uint64_t length, ObJsonArray *array, ObJBVerType vertype)
{
  INIT_SUCC(ret);
  switch(vertype) {
    case ObJBVerType::J_ARRAY_V0: {
      ret = deserialize_json_array_v0(data, length, array);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid vertype", K(ret), K(vertype));
      break;
    }
  }
  return ret;
}

int ObJsonBin::raw_binary(ObString &buf) const
{
  INIT_SUCC(ret);
  if (OB_ISNULL(curr_.ptr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json binary ptr is null.", K(ret));
  } else {
    buf.assign_ptr(curr_.ptr(), curr_.length());
  }
  return ret;
}

int ObJsonBin::get_max_offset(const char* data, ObJsonNodeType cur_node, uint64_t& max_offset) const
{
  INIT_SUCC(ret);
  uint8_t cur_node_type = static_cast<uint8_t>(cur_node);
  if (OB_JSON_TYPE_IS_INLINE(cur_node_type)) {
    max_offset = 1;
  } else if (!(cur_node == ObJsonNodeType::J_OBJECT || cur_node == ObJsonNodeType::J_ARRAY)) {
    if (ObJsonVerType::is_opaque_or_string(cur_node)) {
      int64_t decode_pos = 1;
      int64_t val = 0;
      if (OB_FAIL(serialization::decode_vi64(data, curr_.length() - (data - curr_.ptr()), decode_pos, &val))) {
        LOG_WARN("decode slength failed.", K(ret));
      } else {
        max_offset = decode_pos;
        max_offset += val;
      }
    } else if (ObJsonBaseUtil::is_time_type(cur_node)) {
      if (cur_node == ObJsonNodeType::J_TIME ||
          cur_node == ObJsonNodeType::J_DATE ||
          cur_node == ObJsonNodeType::J_ORACLEDATE) {
        max_offset = sizeof(int32_t);
      } else {
        max_offset = sizeof(uint64_t);
      }
    } else if (cur_node == ObJsonNodeType::J_DECIMAL ||
               cur_node == ObJsonNodeType::J_ODECIMAL) {
      ObPrecision prec = -1;
      ObScale scale = -1;
      int64_t pos = 0;
      number::ObNumber number;
      if (OB_FAIL(serialization::decode_i16(data, curr_.length() - (data - curr_.ptr()), pos, &prec))) {
        LOG_WARN("fail to deserialize decimal precision.", K(ret), K(data - curr_.ptr()), K(curr_.length()));
      } else if (OB_FAIL(serialization::decode_i16(data, curr_.length() - (data - curr_.ptr()), pos, &scale))) {
        LOG_WARN("fail to deserialize decimal scale.", K(ret), K(data - curr_.ptr()), K(curr_.length()));
      } else if (OB_FAIL(number.deserialize(data, curr_.length() - (data - curr_.ptr()), pos))) {
        LOG_WARN("failed to deserialize decimal data", K(ret));
      } else {
        max_offset = pos;
      }
    } else if (cur_node == ObJsonNodeType::J_INT ||
               cur_node == ObJsonNodeType::J_UINT ||
               cur_node == ObJsonNodeType::J_OINT ||
               cur_node == ObJsonNodeType::J_OLONG) {
      int64_t val = 0;
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_vi64(data, curr_.length() - (data - curr_.ptr()), pos, &val))) {
        LOG_WARN("decode int val failed.", K(ret));
      } else {
        max_offset = pos;
      }
    } else if (cur_node == ObJsonNodeType::J_OFLOAT) {
      max_offset = sizeof(float);
    } else if (cur_node == ObJsonNodeType::J_DOUBLE ||
               cur_node == ObJsonNodeType::J_ODOUBLE) {
      max_offset = sizeof(double);
    } else if (cur_node == ObJsonNodeType::J_NULL || cur_node == ObJsonNodeType::J_BOOLEAN) {
      max_offset = 1;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get length failed.", K(ret), K(cur_node));
    }
  } else {
    uint8_t node_type, type, obj_size_type;
    uint64_t count, obj_size, offset = 0;
    parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
    ObJsonNodeType cur_node = ObJsonVerType::get_json_type(static_cast<ObJBVerType>(node_type));

    uint64_t val_offset, val_len, max_val_offset = offset;
    uint8_t entry_size = ObJsonVar::get_var_size(type);
    uint8_t max_offset_type;
    bool is_first_uninline = true;
    bool is_continuous = (reinterpret_cast<const ObJsonBinHeader*>(data))->is_continuous_;

    max_offset = max_val_offset;

    for (int64_t i = count - 1; OB_SUCC(ret) && i >= 0 && is_first_uninline; --i) {
      val_offset = offset + (entry_size + sizeof(uint8_t)) * i;
      if (cur_node == ObJsonNodeType::J_OBJECT) {
        val_offset += count * (entry_size * 2);
      }
      const char* val_offset_ptr = data + val_offset;
      uint64_t node_offset;

      node_type = static_cast<uint8_t>(*static_cast<const char*>(val_offset_ptr + entry_size));
      if (OB_JSON_TYPE_IS_INLINE(node_type)) {
        if (max_val_offset < val_offset_ptr + 1 - data) {
          max_val_offset = val_offset_ptr + 1 - data;
          max_offset_type = node_type;
        }
      } else if (OB_FAIL(ObJsonVar::read_var(val_offset_ptr, type, &node_offset))) {
        LOG_WARN("get max offset failed.", K(ret), K(type));
      } else if (max_val_offset < node_offset) {
        max_val_offset = node_offset;
        max_offset_type = node_type;
        if (is_continuous) {
          is_first_uninline = false;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (max_val_offset > offset) {
        uint64_t node_max_offset = 0;
        if (!OB_JSON_TYPE_IS_INLINE(node_type) &&
            OB_FAIL(get_max_offset(data + max_val_offset, static_cast<ObJsonNodeType>(max_offset_type), node_max_offset))) {
          LOG_WARN("get max offset failed.", K(ret), K(cur_node));
        } else {
          max_val_offset += node_max_offset;
        }
      }
      if (max_val_offset < obj_size) {
        max_offset = obj_size;
      } else {
        max_offset = max_val_offset;
      }
    }
  }

  return ret;
}

int ObJsonBin::get_use_size(uint64_t& used_size) const
{
  INIT_SUCC(ret);
  int32_t stk_len = stack_size(stack_buf_);
  const char* data = curr_.ptr() + pos_;
  ObJBVerType ver_type = static_cast<ObJBVerType>(*reinterpret_cast<const uint8_t*>(data));
  ObJsonNodeType json_type = static_cast<ObJsonNodeType>(ver_type);
  if (stk_len == 0) {
    used_size = curr_.length();
  } else {
    uint64_t max_offset = 0;
    if (OB_FAIL(get_max_offset(data, json_type, max_offset))) {
      LOG_WARN("get max offset.", K(ret));
    } else {
      used_size = max_offset;
      if (curr_.length() - pos_ < used_size) {
        used_size = curr_.length() - pos_;
      }
    }
  }
  return ret;
}

int ObJsonBin::raw_binary(ObString &buf, ObIAllocator *allocator) const
{
  INIT_SUCC(ret);
  ObIAllocator * allocator_ptr = (allocator == NULL) ? allocator_ : allocator;
  uint8_t type = OB_JSON_TYPE_GET_INLINE(type_); // dst type take off inline mask
  ObJBVerType vertype = static_cast<ObJBVerType>(type);
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
  if (node_type == ObJsonNodeType::J_ARRAY ||
      node_type == ObJsonNodeType::J_OBJECT ||
      ObJsonVerType::is_opaque_or_string(vertype)) {
    ret = raw_binary_at_iter(buf);
  } else if (OB_ISNULL(allocator_ptr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("alloctor is null.", K(ret));
  } else {
    void *result_jbuf = allocator_ptr->alloc(sizeof(ObJsonBuffer));
    if (OB_ISNULL(result_jbuf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate jbuf", K(ret));
    } else {
      ObJsonBuffer* result = static_cast<ObJsonBuffer*>(new(result_jbuf)ObJsonBuffer(allocator_ptr));
      if (OB_FAIL(result->append(reinterpret_cast<const char*>(&type), sizeof(uint8_t)))) {
        LOG_WARN("failed to serialize jsonbin append type_", K(ret));
      } else {
        int64_t append_len = curr_.length() - pos_;
        if (append_len <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid append len.", K(ret), K(append_len), K(pos_), K(curr_));
        } else {
          if (bytes_ * OB_JSON_BIN_REBUILD_THRESHOLD < append_len || OB_JSON_TYPE_IS_INLINE(type_)) {
            // free space over 30% or inline type, do rebuild
            ObJsonBuffer& jbuf = *result;
            if (OB_FAIL(rebuild_json_value(curr_.ptr() + pos_, curr_.length() - pos_, type_, type, uint_val_, jbuf))) {
              LOG_WARN("failed to rebuild inline value", K(ret));
            }
          } else {
            // free space less than 30%, do append
            if (OB_FAIL(result->append(curr_.ptr() + pos_, curr_.length() - pos_))) {
              LOG_WARN("failed to copy data to result", K(ret), K(curr_.length() - pos_));
            }
          }
          if (OB_SUCC(ret)) {
            buf.assign_ptr(result->ptr(), result->length());
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::raw_binary_at_iter(ObString &buf) const
{
  INIT_SUCC(ret);
  uint64_t used_size = 0;
  if (OB_ISNULL(curr_.ptr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json binary ptr is null.", K(ret));
  } else if (pos_ >= curr_.length()) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("json binary iter pos invalid", K(ret), K(pos_), K(curr_.length()));
  } else if (OB_FAIL(get_use_size(used_size))) {
    LOG_WARN("get use size failed", K(ret));
  } else {
    buf.assign_ptr(curr_.ptr() + pos_, used_size);
  }
  return ret;
}

int ObJsonBin::get_free_space(size_t &space) const
{
  INIT_SUCC(ret);
  uint64_t actual_size = curr_.length();
  uint64_t used_size = 0;

  uint8_t node_type = *reinterpret_cast<const uint8_t*>(curr_.ptr());
  ObJBVerType node_type_val = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(node_type));
  if (ObJsonVerType::get_json_type(node_type_val) != ObJsonNodeType::J_ARRAY &&
      ObJsonVerType::get_json_type(node_type_val) != ObJsonNodeType::J_OBJECT) {
    space = 0;
  } else {
    uint8_t type, obj_size_type;
    uint64_t count, obj_size, offset = 0;
    parse_obj_header(curr_.ptr(), offset, node_type, type, obj_size_type, count, obj_size);

    used_size = obj_size;
    if (used_size > actual_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("length invalid.", K(ret), K(actual_size), K(used_size));
    } else {
      space = actual_size - used_size;
    }
  }

  return ret;
}

// reset iter to root
int ObJsonBin::reset_iter()
{
  INIT_SUCC(ret);
  char *ptr = curr_.ptr();
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json binary ptr is null.", K(ret));
  } else {
    // parse first byte
    type_ = *reinterpret_cast<ObJBVerType*>(ptr);
    pos_ = 0;
    stack_reset(stack_buf_);
    ObJBVerType node_vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type_));
    ObJsonNodeType node_type = ObJsonVerType::get_json_type(node_vertype);
    if (!(node_type == ObJsonNodeType::J_ARRAY ||
          node_type == ObJsonNodeType::J_OBJECT ||
          ObJsonVerType::is_opaque_or_string(node_vertype))) {
      pos_ += sizeof(uint8_t);
    }

    if (OB_FAIL(set_curr_by_type(pos_, 0, type_))) {
      LOG_WARN("falied to set root obj", K(ret), K(pos_), K(type_));
    }
  }
  return ret;
}

int64_t ObJsonBin::to_string(char *buf, int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos, "is_alloc=%d type=%u pos=%ld "
                  "element_count=%lu bytes=%lu field_type=%d int_val=%ld uint_val=%lu "
                  "double_val=%lf",
                  is_alloc_, type_, pos_, element_count_, bytes_, field_type_,
                  int_val_, uint_val_, double_val_);
  return pos;
}

int ObJsonBin::move_iter(ObJsonBuffer& stack, uint32_t start)
{
  INIT_SUCC(ret);
  uint32_t depth = stack_size(stack);
  uint64_t offset = 0;
  ObJBNodeMeta node_meta;
  curr_.assign_ptr(result_.ptr(), result_.length());
  char* data = result_.ptr();

  stack_at(stack, start, node_meta);
  data += node_meta.offset_;
  offset = node_meta.offset_;

  uint8_t node_type, type, obj_size_type;
  uint64_t count, obj_size;

  for (uint32_t idx = 0; OB_SUCC(ret) && idx < depth; ++idx) {
    stack_at(stack, idx, node_meta);
    node_meta.offset_ = data - result_.ptr();
    parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
    if (ObJsonVerType::is_array(static_cast<ObJBVerType>(node_type)) ||
        ObJsonVerType::is_object(static_cast<ObJBVerType>(node_type))) {
      uint64_t type_size = ObJsonVar::get_var_size(type);
      uint64_t key_entry_size = 2 * type_size;
      uint64_t val_entry_size = type_size + sizeof(uint8_t);
      char* val_entry = data + offset + (val_entry_size * node_meta.idx_);
      if (ObJsonVerType::is_object(static_cast<ObJBVerType>(node_type))) {
        val_entry += count * key_entry_size;
      }
      uint64_t val_offset;
      if (OB_FAIL(ObJsonVar::read_var(val_entry, type, &val_offset))) {
        LOG_WARN("falied to read value offset", K(ret), K(node_type));
      } else {
        stack_update(stack, idx, node_meta);
        data += val_offset;
        offset = 0;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("falied to parse, invalid node type", K(ret), K(node_type));
    }
  }

  if (OB_SUCC(ret)) {
    stack_back(stack, node_meta);
    if (OB_FAIL(set_curr_by_type(node_meta.offset_, 0, node_meta.ver_type_))) {
      LOG_WARN("falied to set curr type", K(ret), K(node_type));
    }
  }
  return ret;
}
// move iter to its parent
int ObJsonBin::move_parent_iter()
{
  INIT_SUCC(ret);
  ObJBNodeMeta curr_parent;
  if (OB_FAIL(stack_back(stack_buf_, curr_parent, true))) {
    LOG_WARN("fail to pop back from parent", K(ret), K(stack_size(stack_buf_)));
  } else if (curr_parent.offset_ >= curr_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to move iter", K(curr_parent.offset_), K(stack_size(stack_buf_)), K(curr_.length()));
  } else {
    // father must be a container
    char *ptr = curr_.ptr() + curr_parent.offset_;
    ObJsonBinHeader *obj_header = reinterpret_cast<ObJsonBinHeader *>(ptr);
    type_ = obj_header->type_;
    pos_ = curr_parent.offset_;
    if (OB_FAIL(set_curr_by_type(pos_, 0, type_))) {
      LOG_WARN("failed to move iter to parent", K(ret), K(pos_), K(type_));
    }
  }
  return ret;
}

// inlined will reuse value entry offset which length is type_size
int ObJsonBin::set_curr_by_type(int64_t new_pos, uint64_t val_offset, uint8_t type, uint8_t entry_size)
{
  INIT_SUCC(ret);
  char *ptr = curr_.ptr();
  data_ = ptr + new_pos;
  element_count_ = 1; // scalar is 1, container is acutual k-v pairs number
  bool is_inlined = OB_JSON_TYPE_IS_INLINE(type);
  if (!is_inlined && new_pos >= curr_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new pos invalid.", K(ret), K(curr_.length()), K(new_pos));
  } else {
    ObJBVerType node_vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type));
    ObJsonNodeType node_type = ObJsonVerType::get_json_type(node_vertype);
    switch (node_type) {
      case ObJsonNodeType::J_NULL: {
        bytes_ = is_inlined ? 0 : 1;
        break;
      }
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        ObPrecision prec = -1;
        ObScale scale = -1;
        int64_t pos = 0;
        if (OB_FAIL(serialization::decode_i16(data_, curr_.length() - new_pos, pos, &prec))) {
          LOG_WARN("fail to deserialize decimal precision.", K(ret), K(new_pos), K(curr_.length()));
        } else if (OB_FAIL(serialization::decode_i16(data_, curr_.length() - new_pos, pos, &scale))) {
          LOG_WARN("fail to deserialize decimal scale.", K(ret), K(new_pos), K(pos), K(curr_.length()));
        } else if (OB_FAIL(number_.deserialize(data_, curr_.length() - new_pos, pos))) {
          LOG_WARN("failed to deserialize decimal data", K(ret), K(new_pos), K(pos), K(curr_.length()));
        } else {
          prec_ = prec;
          scale_ = scale;
          bytes_ = pos;
        }
        break;
      }
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        if (is_inlined) {
          int_val_ = ObJsonVar::var_uint2int(val_offset, entry_size);
          bytes_ = 0;
        } else {
          int64_t val = 0;
          int64_t pos = 0;
          if (OB_FAIL(serialization::decode_vi64(data_, curr_.length() - new_pos, pos, &val))) {
            LOG_WARN("decode int val failed.", K(ret));
          } else {
            int_val_ = val;
            bytes_ = pos;
          }
        }
        break;
      }
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG:  {
        if (is_inlined) {
          uint_val_ = static_cast<uint64_t>(val_offset);
          bytes_ = 0;
        } else {
          int64_t val = 0;
          int64_t pos = 0;
          if (OB_FAIL(serialization::decode_vi64(data_, curr_.length() - new_pos, pos, &val))) {
            LOG_WARN("decode uint val failed.", K(ret));
          } else {
            uint64_t uval = static_cast<uint64_t>(val);
            uint_val_ = uval;
            bytes_ = pos;
          }
        }
        break;
      }
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        double_val_ = *reinterpret_cast<const double*>(data_);
        bytes_ = sizeof(double);
        break;
      }
      case ObJsonNodeType::J_OFLOAT: {
        float_val_ = *reinterpret_cast<const float*>(data_);
        bytes_ = sizeof(float);
        break;
      }
      case ObJsonNodeType::J_STRING: {
        int64_t val = 0;
        int64_t pos = 0;
        ObJBVerType vertype = *reinterpret_cast<const ObJBVerType*>(data_);
        if (vertype == ObJBVerType::J_STRING_V0) {
          pos += sizeof(uint8_t);
          if (OB_FAIL(serialization::decode_vi64(data_, curr_.length() - new_pos, pos, &val))) {
            LOG_WARN("decode string length failed.", K(ret));
          } else if (pos + val > curr_.length() - new_pos) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length is not enough for str.", K(ret), K(curr_.length()), K(new_pos), K(pos), K(val));
          } else {
            uint64_t length = static_cast<uint64_t>(val);
            element_count_ = length;
            bytes_ = length + pos;
            data_ = data_ + pos;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse string type invlaid vertype.", K(ret), K(vertype));
        }
        break;
      }
      case ObJsonNodeType::J_OBJECT:
      case ObJsonNodeType::J_ARRAY: {
        int64_t left_len = curr_.length() - new_pos;
        if (left_len <= OB_JSON_BIN_OBJ_HEADER_LEN) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for obj header.", K(ret), K(curr_.length()), K(new_pos));
        } else if (node_vertype == ObJBVerType::J_ARRAY_V0 || node_vertype == ObJBVerType::J_OBJECT_V0) {
          // different version process
          ObJsonBinObjHeader *header = reinterpret_cast<ObJsonBinObjHeader*>(data_);
          left_len -= OB_JSON_BIN_OBJ_HEADER_LEN;
          uint64_t count_size = ObJsonVar::get_var_size(header->count_size_);
          uint64_t obj_size_size = ObJsonVar::get_var_size(header->obj_size_size_);
          if (left_len < count_size + obj_size_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length is not enough for obj var size.",
                    K(ret), K(left_len), K(count_size), K(obj_size_size));
          } else {
            if (OB_FAIL(ObJsonVar::read_var(header->used_size_, header->count_size_, &element_count_))) {
              LOG_WARN("fail to read count var.", K(ret), K(header->count_size_));
            } else if (OB_FAIL(ObJsonVar::read_var(header->used_size_ + count_size, header->obj_size_size_, &bytes_))) {
              LOG_WARN("fail to read obj_size var.", K(ret), K(header->obj_size_size_));
            } else {
              left_len -= count_size;
              left_len -= obj_size_size;
              uint64_t entry_size = ObJsonVar::get_var_size(header->entry_size_);
              uint64_t kv_entry_len = element_count_ * (entry_size + 1); // val_entry
              if (node_type == ObJsonNodeType::J_OBJECT) {
                kv_entry_len += element_count_ * (entry_size * 2); // key_entry
              }
              if (left_len < kv_entry_len) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("data length is not enough for obj kv entry.",
                        K(ret), K(left_len), K(kv_entry_len), K(element_count_), K(entry_size));
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ver type.", K(ret), K(node_vertype));
        }
        break;
      }
      case ObJsonNodeType::J_BOOLEAN: {
        uint_val_ = is_inlined ? static_cast<bool>(val_offset) : static_cast<bool>(*data_);
        bytes_ = is_inlined ? 0 : 1;
        break;
      }
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_ORACLEDATE: {
        if (sizeof(int32_t) > curr_.length() - new_pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for date.", K(ret), K(curr_.length()), K(new_pos));
        } else {
          field_type_ = ObDateType;
          int_val_ = *reinterpret_cast<const int32_t*>(data_);
          bytes_ = sizeof(int32_t);
        }
        break;
      }
      case ObJsonNodeType::J_TIME: {
        if (sizeof(int64_t) > curr_.length() - new_pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for time.", K(ret), K(curr_.length()), K(new_pos));
        } else {
          field_type_ = ObTimeType;
          int_val_ = *reinterpret_cast<const int64_t*>(data_);
          bytes_ = sizeof(int64_t);
        }
        break;
      }
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_ODATE:
      case ObJsonNodeType::J_OTIMESTAMP:
      case ObJsonNodeType::J_OTIMESTAMPTZ: {
        if (sizeof(int64_t) > curr_.length() - new_pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for datetime.", K(ret), K(curr_.length()), K(new_pos));
        } else {
          field_type_ = ObJsonBaseUtil::get_time_type(node_type);
          int_val_ = *reinterpret_cast<const int64_t*>(data_);
          bytes_ = sizeof(int64_t);
        }
        break;
      }
      case ObJsonNodeType::J_TIMESTAMP: {
        if (sizeof(int64_t) > curr_.length() - new_pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for timestamp.", K(ret), K(curr_.length()), K(new_pos));
        } else {
          field_type_ = ObTimestampType;
          int_val_ = *reinterpret_cast<const int64_t*>(data_);
          bytes_ = sizeof(int64_t);
        }
        break;
      }
      case ObJsonNodeType::J_OPAQUE: {
        ObJBVerType vertype = *reinterpret_cast<const ObJBVerType*>(data_);
        if (vertype == ObJBVerType::J_OPAQUE_V0) {
          char* data = data_ + sizeof(uint8_t);
          if (sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t) > curr_.length() - new_pos) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length is not enough for opaque len.", K(ret), K(curr_.length()), K(new_pos));
          } else {
            field_type_ = static_cast<ObObjType>(*reinterpret_cast<uint16_t*>(data));
            element_count_ = *reinterpret_cast<uint64_t*>(data + sizeof(uint16_t));
            if (element_count_ + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t) > curr_.length() - new_pos) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("data length is not enough for opaque val.", K(ret),
                      K(curr_.length()), K(new_pos), K(element_count_));
            } else {
              bytes_ = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t) + element_count_;
              data_ = data_ + sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint64_t);
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse opaque type invlaid vertype.", K(ret), K(vertype));
        }
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

// move iter to element i without copy
int ObJsonBin::element(size_t index)
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  if (node_type != ObJsonNodeType::J_ARRAY && node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(node_type));
  } else if (index >= element_count_) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range.", K(ret), K(index), K(element_count_));
  } else {
    if (node_type == ObJsonNodeType::J_ARRAY) {
      ret = get_element_in_array(index);
    } else { // ObJsonNodeType::J_OBJECTs
      ret = get_element_in_object(index);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get element.", K(ret), K(index), K(node_type));
    }
  }
  return ret;
}

// get key id by key
int ObJsonBin::lookup_index(const ObString &key, size_t *idx) const
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  if (node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(node_type));
  }

  ObJsonKeyCompare comparator;
  ObString key_iter;
  bool is_found = false;
  int64_t low = 0;
  int64_t high = element_count_ - 1;
  // do binary search
  while (OB_SUCC(ret) && low <= high) {
    int64_t mid = low + (high - low) / 2;
    if (OB_FAIL(get_key_in_object(mid, key_iter))) {
      LOG_WARN("fail to get key.", K(ret), K(mid), K(low), K(high));
    } else {
      int compare_result = comparator.compare(key_iter, key);
      if (compare_result == 0) {
        *idx = mid;
        is_found = true;
        break;
      } else if (compare_result > 0) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
  }

  ret = (ret == OB_SUCCESS && !is_found) ? OB_SEARCH_NOT_FOUND : ret;
  return ret;
}

// mover iter to the value of key
int ObJsonBin::lookup(const ObString &key)
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  if (node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(node_type));
  } else {
    size_t idx = 0;
    ret = lookup_index(key, &idx);
    if (ret == OB_SEARCH_NOT_FOUND) {
    } else if (OB_FAIL(ret)) {
      LOG_WARN("look up child node failed.", K(ret), K(key));
    } else if (OB_FAIL(element(idx))) {
      LOG_WARN("move iter to child node failed.", K(ret), K(key), K(idx));
    }
  }
  return ret;
}

void ObJsonBin::parse_obj_header(const char *data, uint64_t &offset,
     uint8_t &node_type, uint8_t &type, uint8_t& obj_size_type, uint64_t &count, uint64_t &obj_size) const
{
  const ObJsonBinObjHeader *header = reinterpret_cast<const ObJsonBinObjHeader*>(data + offset);
  node_type = header->type_;
  offset += OB_JSON_BIN_OBJ_HEADER_LEN;
  type = header->entry_size_;
  obj_size_type = header->obj_size_size_;
  ObJsonVar::read_var(data + offset, header->count_size_, &count);
  offset += ObJsonVar::get_var_size(header->count_size_);
  ObJsonVar::read_var(data + offset, header->obj_size_size_, &obj_size);
  offset += ObJsonVar::get_var_size(header->obj_size_size_);
}

int ObJsonBin::get_element_in_array_v0(size_t index, char **get_addr_only)
{
  INIT_SUCC(ret);
  uint64_t offset = pos_;
  char *data = curr_.ptr();
  uint8_t node_type, type, obj_size_type;
  uint64_t count, obj_size;
  parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
  uint64_t type_size = ObJsonVar::get_var_size(type);
  uint64_t val_entry_size = (type_size + sizeof(uint8_t));

  const char *val_entry = (data + offset);
  uint64_t val_offset, val_type;
  uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
  if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * index, type, &val_offset))) {
    LOG_WARN("failed to read val offset", K(ret), K(index), K(val_entry_size), K(type));
  } else if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * index + type_size, v_type, &val_type))) {
    LOG_WARN("failed to read val type", K(ret), K(index), K(val_entry_size), K(v_type));
  } else {
    char *val = data + pos_ + val_offset;
    if (OB_NOT_NULL(get_addr_only)) {
      if (OB_JSON_TYPE_IS_INLINE(static_cast<uint8_t>(val_type))) {
        // for inline, set addr to val_offset
        *get_addr_only = data + offset + val_entry_size * index;
      } else {
        *get_addr_only = val;
      }
    } else {
      type_ = static_cast<uint8_t>(val_type);
      ObJBNodeMeta path_node(node_type, obj_size_type, type, index, pos_, obj_size);
      if (OB_NOT_NULL(allocator_) && OB_FAIL(stack_push(stack_buf_, path_node))) {
        LOG_WARN("failed to push parent pos.", K(ret), K(pos_), K(stack_size(stack_buf_)));
      } else {
        if (OB_JSON_TYPE_IS_INLINE(type_)) {
          // for inline, set pos_ to val_offset
          pos_ = offset + val_entry_size * index;
        } else {
          pos_ = pos_ + val_offset;
        }
        if (OB_FAIL(set_curr_by_type(pos_, val_offset, val_type, type))) {
          LOG_WARN("failed to move iter to sub obj.", K(ret), K(index));
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::get_element_in_array(size_t index, char **get_addr_only)
{
  INIT_SUCC(ret);
  ObJBVerType vertype = *reinterpret_cast<ObJBVerType*>(curr_.ptr() + pos_);
  switch (vertype)
  {
    case ObJBVerType::J_ARRAY_V0:
    {
      ret = get_element_in_array_v0(index, get_addr_only);
      break;
    }
    default:
    {
      LOG_WARN("failed, invalid vertype.", K(vertype));
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObJsonBin::get_element_in_object_v0(size_t i, char **get_addr_only)
{
  INIT_SUCC(ret);
  uint64_t offset = pos_;
  char *data = curr_.ptr();
  uint8_t node_type, type, obj_size_type;
  uint64_t count, obj_size;
  parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
  uint64_t type_size = ObJsonVar::get_var_size(type);
  uint64_t key_entry_size = type_size * 2;
  uint64_t val_entry_size = (type_size + sizeof(uint8_t));

  const char *key_entry = (data + offset);
  const char *val_entry = (key_entry + key_entry_size * count);
  uint64_t value_offset, val_type;
  uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
  if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * i, type, &value_offset))) {
    LOG_WARN("failed to read val offset", K(ret), K(i), K(val_entry_size), K(type));
  } else if (OB_FAIL(ObJsonVar::read_var(val_entry + val_entry_size * i + type_size, v_type, &val_type))) {
    LOG_WARN("failed to read val type", K(ret), K(i), K(val_entry_size), K(v_type));
  } else {
    char *val = data + pos_ + value_offset;
    if (OB_NOT_NULL(get_addr_only)) {
      if (OB_JSON_TYPE_IS_INLINE(static_cast<uint8_t>(val_type))) {
        // for inline, set addr to val_offset
        *get_addr_only = data + offset + key_entry_size * count + val_entry_size * i;
      } else {
        *get_addr_only = val;
      }
    } else {
      type_ = static_cast<uint8_t>(val_type);
      ObJBNodeMeta path_node(node_type, obj_size_type, type, i, pos_, obj_size);
      if (OB_NOT_NULL(allocator_) && OB_FAIL(stack_push(stack_buf_, path_node))) {
        LOG_WARN("failed to push parent pos.", K(ret), K(pos_), K(stack_size(stack_buf_)));
      } else {
        if (OB_JSON_TYPE_IS_INLINE(type_)) {
          // for inline, set pos_ to val_offset
          pos_ = offset + key_entry_size * count + val_entry_size * i;
        } else {
          pos_ = pos_ + value_offset;
        }
        if (OB_FAIL(set_curr_by_type(pos_, value_offset, val_type, type))) {
          LOG_WARN("failed to move iter to sub obj.", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::get_element_in_object(size_t i, char **get_addr_only)
{
  INIT_SUCC(ret);
  ObJBVerType vertype = *reinterpret_cast<ObJBVerType*>(curr_.ptr() + pos_);
  switch (vertype) {
    case ObJBVerType::J_OBJECT_V0: {
      ret = get_element_in_object_v0(i, get_addr_only);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed: vertype invalid", K(ret), K(vertype));
      break;
    }
  }
  return ret;
}

int ObJsonBin::get_key_in_object_v0(size_t i, ObString &key) const
{
  INIT_SUCC(ret);
  uint64_t offset = pos_;
  const char *data = curr_.ptr();
  uint8_t node_type, type, obj_size_type;
  uint64_t count, obj_size;
  parse_obj_header(data, offset, node_type, type, obj_size_type, count, obj_size);
  uint64_t type_size = ObJsonVar::get_var_size(type);
  uint64_t key_entry_size = type_size * 2;

  const char *key_entry = (data + offset);
  uint64_t key_offset, key_len;
  if (OB_FAIL(ObJsonVar::read_var(key_entry + key_entry_size * i, type, &key_offset))) {
    LOG_WARN("failed to read key offset", K(ret));
  } else if (OB_FAIL(ObJsonVar::read_var(key_entry + key_entry_size * i + type_size, type, &key_len))) {
    LOG_WARN("failed to read key len", K(ret));
  } else {
    const char *key_val = data + pos_ + key_offset;
    key.assign_ptr(key_val, key_len);
  }
  return ret;
}


int ObJsonBin::get_key_in_object(size_t i, ObString &key) const
{
  INIT_SUCC(ret);
  ObJBVerType vertype = *reinterpret_cast<const ObJBVerType*>(curr_.ptr() + pos_);
  switch (vertype) {
    case ObJBVerType::J_OBJECT_V0: {
      ret = get_key_in_object_v0(i, key);
      break;
    }
    default: {
      LOG_WARN("failed invalid vertype.", K(ret), K(vertype));
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObJsonBin::estimate_need_rebuild_kv_entry(ObJsonBuffer &result, ObJsonBuffer& origin_stack, ObJsonBuffer& update_stack,
                                     uint32_t& top_pos, bool& need_rebuild)
{
  INIT_SUCC(ret);
  need_rebuild = false;
  uint64_t new_offset = result.length();
  if (OB_FAIL(stack_copy(origin_stack, update_stack))) {
    LOG_WARN("failed to copy path stack", K(ret), K(origin_stack.length()));
  } else {
    top_pos = stack_size(update_stack) - 1;
    for (int idx = top_pos; idx >= 0; --idx) {
      ObJBNodeMeta path_node;
      stack_at(update_stack, idx, path_node);
      uint8_t curr_node_offset_type = path_node.entry_type_;
      uint8_t new_offset_type = ObJsonVar::get_var_type(new_offset - path_node.offset_ + path_node.obj_size_);
      if (new_offset_type > curr_node_offset_type) {
        need_rebuild = true;
        top_pos = idx;
        path_node.entry_type_ = new_offset_type;
        stack_update(update_stack, idx, path_node);
      }
    }
  }
  return ret;
}

int ObJsonBin::estimate_need_rebuild(ObJsonBuffer& update_stack, int64_t size_change,
                                    int32_t pos, uint32_t& top_pos, bool& need_rebuild)
{
  INIT_SUCC(ret);
  if (OB_FAIL(stack_copy(stack_buf_, update_stack))) {
    LOG_WARN("failed to copy path stack", K(ret), K(stack_buf_.length()));
  } else {
    top_pos = stack_size(update_stack) - 1;
    // if pos == 0, from last do scan, else from pos
    if (pos == 0) {
      pos = stack_size(update_stack) - 1;
    } else if (pos > top_pos) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("calc rebuild failed", K(pos), K(top_pos));
    } else {
      top_pos = pos;
    }
    for (int64_t idx = pos; size_change && stack_size(update_stack) > 0 && idx >= 0; idx--) {
      ObJBNodeMeta path_node;
      stack_at(update_stack, idx, path_node);

      int64_t new_data_size = path_node.obj_size_ + size_change;
      uint8_t new_var_type = ObJsonVar::get_var_type(new_data_size);
      if (new_var_type > path_node.size_type_ || new_var_type > path_node.entry_type_) {
        top_pos = idx;
        path_node.entry_type_ = new_var_type;
        need_rebuild = true;
      }
      path_node.obj_size_ = new_data_size;
      path_node.size_type_ = new_var_type;
      stack_update(update_stack, idx, path_node);
    }

    // if top pos == 0, do rebuild
    if (top_pos != 0 && need_rebuild) {
      ObJsonBuffer nw_stack(update_stack.get_allocator());
      uint32_t tmp_pos = top_pos;
      estimate_need_rebuild_kv_entry(result_, update_stack, nw_stack, tmp_pos, need_rebuild);
      if (top_pos > tmp_pos) {
        top_pos = tmp_pos;
      }
      stack_copy(nw_stack, update_stack);
    }
  }
  return ret;
}

int ObJsonBin::rebuild_with_meta(const char *data, uint64_t length, ObJsonBuffer& old_stack, ObJsonBuffer& new_meta,
                                 uint32_t min, uint32_t max, ObJsonBuffer &result, uint32_t depth)
{
  INIT_SUCC(ret);
  if (min <= max) {
    ObJBNodeMeta old_node;
    ObJBNodeMeta new_node;
    stack_at(old_stack, min, old_node);
    stack_at(new_meta, min, new_node);

    // use transform matrix function later
    if ((old_node.ver_type_ == ObJBVerType::J_ARRAY_V0 || old_node.ver_type_ == ObJBVerType::J_OBJECT_V0) &&
        (new_node.ver_type_ == ObJBVerType::J_ARRAY_V0 || new_node.ver_type_ == ObJBVerType::J_OBJECT_V0)) {
      int64_t st_pos = result.length();
      uint64_t offset = old_node.offset_;
      uint8_t node_type, var_type, obj_size_type;
      uint64_t count, obj_size;

      const ObJsonBinObjHeader *header = reinterpret_cast<const ObJsonBinObjHeader*>(data + offset);
      // parsing header using v0 format
      parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);

      ObJsonBinHeader new_header = *header;
      new_header.entry_size_ = new_node.entry_type_;
      new_header.obj_size_size_ = new_node.size_type_;
      new_header.is_continuous_ = 1;
      uint64_t new_count_size = ObJsonVar::get_var_size(header->count_size_);
      uint64_t new_type_size = ObJsonVar::get_var_size(new_node.entry_type_);
      uint64_t new_key_entry_size = new_type_size * 2;
      uint64_t new_val_entry_size = new_type_size + sizeof(uint8_t);
      uint64_t reserve_entry_size = count * new_val_entry_size;

      if (ObJsonVerType::is_object(static_cast<ObJBVerType>(old_node.ver_type_))) {
        reserve_entry_size += count * new_key_entry_size;
      }

      // rebuild using latest format
      // copy obj header, key entry, val entry, key(if need)
      uint64_t type_size = ObJsonVar::get_var_size(var_type);
      uint64_t key_entry_size = type_size * 2;
      uint64_t val_entry_size = (type_size + sizeof(uint8_t));
      uint64_t meta_len = (offset - old_node.offset_) + key_entry_size * count + val_entry_size * count;
      const char *old_val_entry = data + offset;

      if (ObJsonVerType::is_object(static_cast<ObJBVerType>(old_node.ver_type_))) {
        old_val_entry += key_entry_size * count;
      }

      uint64_t new_val_entry_offset;
      if (OB_FAIL(result.append(reinterpret_cast<const char*>(&new_header), OB_JSON_BIN_HEADER_LEN))) {
        LOG_WARN("failed to append header", K(ret));
      } else if (OB_FAIL(ObJsonVar::append_var(count, new_header.count_size_, result))) {
        LOG_WARN("failed to append count", K(ret));
      } else if (OB_FAIL(ObJsonVar::append_var(new_node.obj_size_, new_node.size_type_, result))) {
        LOG_WARN("failed to append obj size", K(ret));
      } else if (OB_FAIL(result.reserve(reserve_entry_size))) {
        LOG_WARN("failed to reserve mem", K(ret), K(reserve_entry_size));
      } else if (ObJsonVerType::is_object(static_cast<ObJBVerType>(old_node.ver_type_))) {
        if (length < meta_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for obj.", K(ret), K(length), K(meta_len));
        } else {
          // reserve key entry array
          uint64_t new_key_entry_offset = result.length() - st_pos;
          new_val_entry_offset = new_key_entry_offset + count * new_key_entry_size;
          result.set_length(result.length() + reserve_entry_size);

          // using latest bin format
          uint64_t key_offset, key_len;
          const char *key_entry = (data + offset);
          const char *last_key_offset_ptr = key_entry + key_entry_size * (count - 1);
          const char *last_key_len_ptr = key_entry + key_entry_size * (count - 1) + type_size;

          // get last key offest and len
          if (OB_FAIL(ObJsonVar::read_var(last_key_offset_ptr, var_type, &key_offset))) {
            LOG_WARN("failed to read key offset", K(ret));
          } else if (OB_FAIL(ObJsonVar::read_var(last_key_len_ptr, var_type, &key_len))) {
            LOG_WARN("failed to read key len", K(ret));
          } else {
            for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
              uint64_t new_key_offset = result.length() - st_pos;
              char* new_key_entry = result.ptr() + st_pos + new_key_entry_offset;
              if (OB_FAIL(ObJsonVar::read_var(key_entry, var_type, &key_offset))) {
                LOG_WARN("failed to read key offset.", K(ret));
              } else if (OB_FAIL(ObJsonVar::read_var(key_entry + type_size, var_type, &key_len))) {
                LOG_WARN("failed to read key len.", K(ret));
              } else if (OB_FAIL(ObJsonVar::set_var(new_key_offset, new_node.entry_type_, new_key_entry))) {
                LOG_WARN("failed to set key len.", K(ret));
              } else if (OB_FAIL(ObJsonVar::set_var(key_len, new_node.entry_type_, new_key_entry + new_type_size))) {
                LOG_WARN("failed to set key len.", K(ret));
              } else if (OB_FAIL(result.append(data + key_offset, key_len))) {
                LOG_WARN("failed to apend key.", K(ret));
              } else {
                // append key entry [key-offset][key-len]
                new_key_entry_offset += new_key_entry_size;
                key_entry += key_entry_size;
              }
            }
          }
        }
      }

      // reserve value entry array

      if (OB_SUCC(ret) && ObJsonVerType::is_array(static_cast<ObJBVerType>(old_node.ver_type_))) {
        new_val_entry_offset = result.length() - st_pos;
        result.set_length(result.length() + reserve_entry_size);
      }

      // process value
      for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        uint64_t new_val_offset = result.length() - st_pos;
        uint64_t val_offset, val_type;
        uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
        char* new_val_entry = result.ptr() + st_pos + new_val_entry_offset + i * new_val_entry_size;
        if (OB_FAIL(ObJsonVar::read_var(old_val_entry + val_entry_size * i, var_type, &val_offset))) {
          LOG_WARN("failed to read val offset", K(ret));
        } else if (OB_FAIL(ObJsonVar::read_var(old_val_entry + val_entry_size * i + type_size, v_type, &val_type))) {
          LOG_WARN("failed to read val type", K(ret));
        } else if (OB_FAIL(ObJsonVar::set_var(val_offset, new_node.entry_type_, new_val_entry))) {
          LOG_WARN("failed to set val offset", K(ret));
        } else if (OB_FAIL(ObJsonVar::set_var(val_type, v_type, new_val_entry + new_type_size))) {
          LOG_WARN("failed to set val type", K(ret));
        } else  {
          uint8_t type = static_cast<uint8_t>(val_type);
          if (!OB_JSON_TYPE_IS_INLINE(type)) {
            if (i == old_node.idx_) {
              if (min < max) {
                ret = rebuild_with_meta(data, length - val_offset, stack_buf_, new_meta, min+1, max, result);
              } else {
                ret = rebuild_json_value(data + old_node.offset_ + val_offset, length - val_offset, type, type, val_offset, result);
              }
            } else {
              ret = rebuild_json_value(data + val_offset + old_node.offset_, length - val_offset, type, type, val_offset, result);
            }
            if (OB_SUCC(ret)) {
              // fill value offset
              new_val_entry = result.ptr() + st_pos + new_val_entry_offset + i * new_val_entry_size;
              if (OB_FAIL(ObJsonVar::set_var(new_val_offset, new_node.entry_type_, new_val_entry))) {
                LOG_WARN("failed to set val offset.", K(ret), K(i), K(new_val_offset), K(var_type));
              }
            } else {
              LOG_WARN("rebuild child node failed.", K(ret), K(i), K(val_type));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        char* obj_size_ptr = result.ptr() + st_pos + OB_JSON_BIN_HEADER_LEN + new_count_size;
        uint64_t actual_obj_size = result.length() - st_pos;
        if (ObJsonVar::get_var_type(actual_obj_size) > new_node.size_type_) {
          if (depth < OB_JSON_BIN_MAX_SERIALIZE_TIME) {
            result.set_length(st_pos);
            new_node.size_type_ = ObJsonVar::get_var_type(actual_obj_size);
            stack_update(new_meta, min, new_node);
            ret = rebuild_with_meta(data, length, stack_buf_, new_meta, min, max, result_, depth + 1);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rebuild failed sub obj size too large.", K(ret), K(actual_obj_size), K(new_node.size_type_));
          }
        } else if (OB_FAIL(ObJsonVar::set_var(result.length() - st_pos, new_node.size_type_, obj_size_ptr))) {
          LOG_WARN("rebuild failed set obj size.", K(ret), K(actual_obj_size));
        } else {
          new_node.obj_size_ = actual_obj_size;
          new_node.offset_ = st_pos;
          stack_update(new_meta, min, new_node);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to rebuild with meta.", K(ret));
    }
  }
  return ret;
}

int ObJsonBin::update_parents(int64_t size_change, bool is_continous)
{
  INIT_SUCC(ret);
  uint32_t stack_len = stack_size(stack_buf_);
  char *data = result_.ptr();
  ObJsonBuffer new_stack(allocator_);
  uint32_t top_pos;
  int32_t update_begin_pos = -1;
  bool need_rebuild = false;
  bool is_rebuild = false;
  if (OB_FAIL(estimate_need_rebuild(new_stack, size_change, 0, top_pos, need_rebuild))) {
    LOG_WARN("failed calc new stack.", K(ret), K(stack_len));
  } else if (size_change < 0 || !need_rebuild) {
    // no need rebuild, just do update data_size
    update_begin_pos = stack_len - 1;
  } else {
    ObJBNodeMeta old_node, new_node;
    stack_at(stack_buf_, top_pos, old_node);
    stack_at(new_stack, top_pos, new_node);
    if (old_node.ver_type_ == ObJBVerType::J_OBJECT_V0 || old_node.ver_type_ == ObJBVerType::J_ARRAY_V0) {
      if (OB_FAIL(result_.reserve(new_node.obj_size_)) ||
          OB_FAIL(rebuild_with_meta(data, new_node.obj_size_, stack_buf_, new_stack, top_pos, stack_len-1, result_))) {
        LOG_WARN("failed to rebuild.", K(ret));
      } else {
        curr_.assign_ptr(result_.ptr(), result_.length());
        if (top_pos != 0) {
          ObJBNodeMeta upper_node, path_node, old_node;
          stack_at(new_stack, top_pos, path_node);
          stack_at(new_stack, top_pos - 1, upper_node);
          update_offset(upper_node.offset_, upper_node.idx_, path_node.offset_);
          update_begin_pos = top_pos - 1;
          size_change = path_node.obj_size_ - old_node.obj_size_;
          if (OB_FAIL(move_iter(new_stack, 0))) {
            reset_iter();
            LOG_WARN("failed to move iter.", K(ret));
          } else {
            stack_copy(new_stack, stack_buf_);
          }
        } else {
          is_rebuild = true;
          ObJsonBuffer tmp_buf(allocator_);
          ObJBNodeMeta root;
          stack_at(new_stack, 0, root);
          char* data = result_.ptr();
          uint64_t data_length = result_.length() - root.offset_;
          if (OB_FAIL(rebuild_with_meta(data, data_length, stack_buf_, new_stack, 0, stack_len - 1, tmp_buf))) {
            LOG_WARN("failed to rebuild all obj.", K(ret));
          } else {
            result_.reuse();
            if (OB_FAIL(result_.append(tmp_buf.ptr(), tmp_buf.length()))) {
              LOG_WARN("failed to copy result.", K(ret));
            } else if (OB_FAIL(move_iter(new_stack, 0))) {
              reset_iter();
              LOG_WARN("failed to move iter.", K(ret));
            } else {
              stack_copy(new_stack, stack_buf_);
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to update obj size.", K(ret));
    }
  }

  for (int i = update_begin_pos; OB_SUCC(ret) && !is_rebuild &&  i >= 0; i--) {
    data = result_.ptr();
    ObJBNodeMeta path_node;
    stack_at(stack_buf_, i, path_node);
    if (path_node.ver_type_ == ObJBVerType::J_OBJECT_V0 || path_node.ver_type_ == ObJBVerType::J_ARRAY_V0) {
      ObJsonBinHeader *header = reinterpret_cast<ObJsonBinHeader*>(data + path_node.offset_);
      if (!is_continous) { // set highest bit for append update, parent is not continuos
        header->is_continuous_ = 0;
      }
      uint64_t obj_size;
      uint64_t offset = ObJsonVar::get_var_size(header->count_size_);
      if (OB_FAIL(ObJsonVar::read_var(header->used_size_ + offset, header->obj_size_size_, &obj_size))) {
        LOG_WARN("failed to read obj size.", K(ret), K(header->obj_size_size_));
      } else {
        obj_size += size_change;
        if (OB_FAIL(ObJsonVar::set_var(obj_size, header->obj_size_size_, header->used_size_ + offset))) {
          LOG_WARN("failed to set new obj size.", K(ret), K(obj_size), K(header->obj_size_size_));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to update obj size.", K(ret));
    }
  }

  return ret;
}


int ObJsonBin::update_offset(uint64_t parent_offset, uint64_t idx, uint64_t value_offset)
{
  INIT_SUCC(ret);
  char* data = result_.ptr() + parent_offset;
  ObJBVerType vertype = *reinterpret_cast<const ObJBVerType*>(data + parent_offset);

  if (vertype == ObJBVerType::J_OBJECT_V0 || vertype == ObJBVerType::J_ARRAY_V0) {
    uint64_t offset = 0;
    uint8_t var_type, node_type, obj_size_type;
    uint64_t count, obj_size;
    const ObJsonBinObjHeader *header = reinterpret_cast<const ObJsonBinObjHeader*>(data);
    // parsing header using v0 format
    parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);

    uint64_t type_size = ObJsonVar::get_var_size(var_type);
    uint64_t key_entry_size = type_size * 2;
    uint64_t val_entry_size = (type_size + sizeof(uint8_t));

    char* value_offset_ptr = data + offset + idx * val_entry_size;
    if (vertype == ObJBVerType::J_OBJECT_V0) {
      value_offset_ptr += key_entry_size * count;
    }
    if (ObJsonVar::set_var(value_offset, var_type, value_offset_ptr)) {
      LOG_WARN("failed: set var.", K(ret), K(var_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed: wrong node vertype.", K(ret), K(vertype));
  }
  return ret;
}

bool ObJsonBin::is_discontinuous() const
{
  ObJsonBinObjHeader *header = reinterpret_cast<ObJsonBinObjHeader*>(data_);
  return (header->is_continuous_ == 0);
}

int ObJsonBin::get_update_val_ptr(ObJsonBin *new_value_bin, char *&val, uint64_t &len, ObJsonBuffer &str)
{
  INIT_SUCC(ret);
  ObJsonNodeType new_value_type = new_value_bin->json_type();
  switch (new_value_type) {
    case ObJsonNodeType::J_ARRAY: // non-continue array or object do rebuild first
    case ObJsonNodeType::J_OBJECT: {
      size_t free_space;
      if (OB_FAIL(new_value_bin->get_free_space(free_space))) {
        LOG_WARN("failed to get free space. ", K(ret));
        break;
      } else {
        if (free_space > 0 || new_value_bin->is_discontinuous()) {
          if (OB_FAIL(new_value_bin->rebuild_at_iter(str))) {
            LOG_WARN("rebuild failed", K(ret));
          } else {
            val = str.ptr();
            len = str.length();
          }
          break;
        }
      }
    }
    default: {
      val = new_value_bin->curr_.ptr() + new_value_bin->pos_;
      len = new_value_bin->get_used_bytes();
      break;
    }
  }
  return ret;
}

int ObJsonBin::update(const ObString &key, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  size_t idx;
  if (OB_FAIL(lookup_index(key, &idx))) {
    LOG_WARN("key not found.", K(ret), K(key));
  } else if (OB_FAIL(update(idx, new_value))) {
    LOG_WARN("update key failed.", K(idx), K(key));
  }
  return ret;
}

int ObJsonBin::insert(const ObString &key, ObJsonBin *new_value, int64_t pos)
{
  INIT_SUCC(ret);
  ObJsonNodeType cur_node_type = json_type();
  ObJBVerType ver_type = get_vertype();
  if (key.empty()) {
    ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
    LOG_WARN("key is NULL", K(ret));
  } else if (OB_ISNULL(new_value)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input json binary is null.", K(ret));
  } else if (cur_node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(cur_node_type));
  } else if (!is_alloc_) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json binary is read only.", K(ret), K(is_alloc_));
  } else if (ver_type == ObJBVerType::J_OBJECT_V0) {
    if (OB_FAIL(insert_v0(pos, key, new_value))) {
      LOG_WARN("json binary add object failed.", K(ret), K(ver_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong node version.", K(ret), K(ver_type));
  }
  return ret;
}

int ObJsonBin::insert(ObJsonBin *new_value, int64_t pos)
{
  INIT_SUCC(ret);
  ObJsonNodeType cur_node_type = json_type();
  ObJBVerType ver_type = this->get_vertype();
  if (OB_ISNULL(new_value)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input json binary is null.", K(ret));
  } else if (cur_node_type != ObJsonNodeType::J_ARRAY) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(cur_node_type));
  } else if (!is_alloc_) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json binary is read only.", K(ret), K(is_alloc_));
  } else if (ver_type == ObJBVerType::J_ARRAY_V0) {
    ObString key;
    if (OB_FAIL(insert_v0(pos, key, new_value))) {
      LOG_WARN("json binary add v0 failed.", K(ret), K(ver_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong node version.", K(ret), K(ver_type));
  }
  return ret;
}

int ObJsonBin::append(ObJsonBin *new_value)
{
  return insert(new_value, OB_JSON_INSERT_LAST);
}

int ObJsonBin::add(const ObString &key, ObJsonBin *new_value)
{
  return insert(key, new_value, OB_JSON_INSERT_LAST);
}

int ObJsonBin::update(int index, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  ObJsonNodeType cur_node_type =  this->json_type();
  ObJBVerType ver_type = this->get_vertype();
  if (OB_ISNULL(new_value)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input json binary is null.", K(ret));
  } else if (cur_node_type != ObJsonNodeType::J_ARRAY && cur_node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(cur_node_type));
  } else if (index >= element_count_) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("idx out of range.", K(ret), K(index), K(element_count_));
  } else if (!is_alloc_) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json binary is read only.", K(ret), K(is_alloc_));
  } else if (ver_type == ObJBVerType::J_ARRAY_V0 || ver_type == ObJBVerType::J_OBJECT_V0) {
    if (OB_FAIL(update_v0(index, new_value))) {
      LOG_WARN("json binary update v0 failed.", K(ret), K(ver_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong node version.", K(ret), K(ver_type));
  }
  return ret;
}

int ObJsonBin::insert_internal_v0(ObJBNodeMeta& meta, int64_t pos, const ObString &key, ObJsonBin *new_value, ObJsonBuffer& result)
{
  INIT_SUCC(ret);
  int64_t st_pos = result.length();
  uint64_t offset = pos_;
  uint8_t node_type, var_type, obj_size_type;
  uint64_t count, obj_size;
  bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;

  char* data = result_.ptr();
  const ObJsonBinHeader *header = reinterpret_cast<const ObJsonBinHeader*>(data + offset);
  // parsing header using v0 format
  parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);

  uint64_t length = obj_size;
  ObJsonBinHeader new_header;
  new_header.entry_size_ = meta.entry_type_;
  new_header.obj_size_size_ = meta.size_type_;
  new_header.is_continuous_ = 1;
  new_header.count_size_ = ObJsonVar::get_var_type(count + 1);
  new_header.type_ = meta.ver_type_;
  uint64_t new_count_size = ObJsonVar::get_var_size(new_header.count_size_);
  uint64_t new_type_size = ObJsonVar::get_var_size(new_header.entry_size_);
  uint64_t new_key_entry_size = new_type_size * 2;
  uint64_t new_val_entry_size = new_type_size + sizeof(uint8_t);
  uint64_t reserve_entry_size = (count + 1) * new_val_entry_size;

  if (is_obj_type) {
    reserve_entry_size += (count + 1) * new_key_entry_size;
  }

  // rebuild using meta format
  // copy obj header, key entry, val entry, key(if need)
  uint64_t type_size = ObJsonVar::get_var_size(var_type);
  uint64_t key_entry_size = type_size * 2;
  uint64_t val_entry_size = (type_size + sizeof(uint8_t));
  uint64_t meta_len = (offset - pos_) + val_entry_size * count;
  uint64_t old_val_entry_offset = offset;

  if (is_obj_type) {
    old_val_entry_offset += key_entry_size * count;
    meta_len += key_entry_size * count;
  }

  uint64_t new_val_entry_offset;
  if (OB_FAIL(result.reserve(meta.obj_size_))) {
    LOG_WARN("failed to reserve mem", K(ret), K(meta.obj_size_));
  } else if (OB_FAIL(result.append(reinterpret_cast<const char*>(&new_header), OB_JSON_BIN_HEADER_LEN))) {
    LOG_WARN("failed to append header", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(count + 1, new_header.count_size_, result))) {
    LOG_WARN("failed to append count", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(meta.obj_size_, new_header.obj_size_size_, result))) {
    LOG_WARN("failed to append obj size", K(ret));
  } else if (is_obj_type) {
    if (length < meta_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data length is not enough for obj.", K(ret), K(length), K(meta_len));
    } else {
      // reserve key entry array
      uint64_t new_key_entry_offset = result.length() - st_pos;
      new_val_entry_offset = new_key_entry_offset + (count + 1) * new_key_entry_size;
      result.set_length(result.length() + reserve_entry_size);

      // using latest bin format
      uint64_t key_offset, key_len;
      const char *key_entry = (result_.ptr() + offset);
      bool is_inserted = false;
      for (int i = 0; OB_SUCC(ret) && i <= count; i++) {
        uint64_t new_key_offset = result.length() - st_pos;
        char* new_key_entry = result.ptr() + st_pos + new_key_entry_offset;
        if ((i == pos || i == count) && !is_inserted) {
          if (OB_FAIL(ObJsonVar::set_var(new_key_offset, meta.entry_type_, new_key_entry))) {
            LOG_WARN("failed to set key len.", K(ret));
          } else if (OB_FAIL(ObJsonVar::set_var(key.length(), meta.entry_type_, new_key_entry + new_type_size))) {
            LOG_WARN("failed to set key len.", K(ret));
          } else if (OB_FAIL(result.append(key.ptr(), key.length()))) {
            LOG_WARN("failed to apend key.", K(ret));
          }
          is_inserted = true;
        } else if (OB_FAIL(ObJsonVar::read_var(key_entry, var_type, &key_offset))) {
          LOG_WARN("failed to read key offset.", K(ret));
        } else if (OB_FAIL(ObJsonVar::read_var(key_entry + type_size, var_type, &key_len))) {
          LOG_WARN("failed to read key len.", K(ret));
        } else if (OB_FAIL(ObJsonVar::set_var(new_key_offset, meta.entry_type_, new_key_entry))) {
          LOG_WARN("failed to set key len.", K(ret));
        } else if (OB_FAIL(ObJsonVar::set_var(key_len, meta.entry_type_, new_key_entry + new_type_size))) {
          LOG_WARN("failed to set key len.", K(ret));
        } else if (OB_FAIL(result.append(data + key_offset, key_len))) {
          LOG_WARN("failed to apend key.", K(ret));
        } else {
          key_entry += key_entry_size;
        }

        new_key_entry_offset += new_key_entry_size;
      }
    }
  } else {
    new_val_entry_offset = result.length() - st_pos;
    result.set_length(result.length() + reserve_entry_size);
  }

  // process value
  char* new_val_entry;
  char* old_val_entry;
  bool is_inserted = false;
  for (uint64_t i = 0; OB_SUCC(ret) && i <= count; ++i) {
    new_val_entry = result.ptr() + st_pos + new_val_entry_offset;
    old_val_entry = result_.ptr() + old_val_entry_offset;
    uint64_t new_val_offset = result.length() - st_pos;
    uint64_t val_offset, val_type;
    uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
    if ((i == pos || i == count) && !is_inserted) {
      is_inserted = true;
      int64_t tmp_new_val_entry_offset = st_pos + new_val_entry_offset;
      if (!try_update_inline(new_value, meta.entry_type_, &tmp_new_val_entry_offset, result)) {
        ObJsonBuffer str(allocator_);
        char *new_val_ptr = NULL;
        uint64_t new_val_length = 0;
        if (OB_FAIL(get_update_val_ptr(new_value, new_val_ptr, new_val_length, str))) {
          LOG_WARN("failed to get update val ptr", K(ret));
        } else if (OB_FAIL(result.append(new_val_ptr, new_val_length))) {
          LOG_WARN("failed to append new value. ", K(ret));
        } else if (OB_FAIL(ObJsonVar::set_var(new_val_offset, meta.entry_type_, new_val_entry))) {
          LOG_WARN("failed to set val offset", K(ret));
        } else if (OB_FAIL(ObJsonVar::set_var(new_value->get_vertype(), v_type, new_val_entry + new_type_size))) {
          LOG_WARN("failed to set val type", K(ret));
        }
      }
    } else if (OB_FAIL(ObJsonVar::read_var(old_val_entry, var_type, &val_offset))) {
      LOG_WARN("failed to read val offset", K(ret));
    } else if (OB_FAIL(ObJsonVar::read_var(old_val_entry + type_size, v_type, &val_type))) {
      LOG_WARN("failed to read val type", K(ret));
    } else if (OB_FAIL(ObJsonVar::set_var(val_offset, meta.entry_type_, new_val_entry))) {
      LOG_WARN("failed to set val offset", K(ret));
    } else if (OB_FAIL(ObJsonVar::set_var(val_type, v_type, new_val_entry + new_type_size))) {
      LOG_WARN("failed to set val type", K(ret));
    } else  {
      uint8_t type = static_cast<uint8_t>(val_type);
      if (!OB_JSON_TYPE_IS_INLINE(type)) {
        if (OB_SUCC(rebuild_json_value(result_.ptr() + val_offset + pos_, length - val_offset, type, type, val_offset, result))) {
          // fill value offset
          new_val_entry = result.ptr() + st_pos + new_val_entry_offset;
          if (OB_FAIL(ObJsonVar::set_var(new_val_offset, meta.entry_type_, new_val_entry))) {
            LOG_WARN("failed to set val offset.", K(ret), K(i), K(new_val_offset), K(var_type));
          }
        } else {
          LOG_WARN("rebuild child node failed.", K(ret), K(i), K(val_type));
        }
      }
      old_val_entry_offset += val_entry_size;
    }

    new_val_entry_offset += new_val_entry_size;
  }

  if (OB_SUCC(ret)) {
    char* obj_size_ptr = result.ptr() + st_pos + OB_JSON_BIN_HEADER_LEN + new_count_size;
    uint64_t actual_obj_size = result.length() - st_pos;
    if (OB_FAIL(ObJsonVar::set_var(actual_obj_size, meta.size_type_, obj_size_ptr))) {
      LOG_WARN("rebuild failed set obj size.", K(ret), K(actual_obj_size));
    } else {
      meta.obj_size_ = actual_obj_size;
      meta.offset_ = st_pos;
    }
  }
  return ret;
}

int ObJsonBin::insert_v0(int64_t pos, const ObString &key, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  bool is_exist = false;
  bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;

  if (is_obj_type) {
    size_t idx;
    if (OB_SUCC(lookup_index(key, &idx))) {
      if (OB_FAIL(update_v0(idx, new_value))) {
        LOG_WARN("failed: key exist, do update failed.", K(idx), K(key));
      } else {
        is_exist = true;
      }
    } else if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed: lookup key internel error.", K(ret), K(key));
    }
  }

  if (OB_SUCC(ret) && !is_exist) {
      // 1. seek index value offset and value type
    uint64_t offset = pos_;
    char *data = result_.ptr();
    uint8_t node_type, var_type, obj_size_type, old_entry_type;
    uint64_t count, obj_size;
    parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);
    uint64_t type_size = ObJsonVar::get_var_size(var_type);
    uint64_t key_entry_size = type_size * 2;
    uint64_t val_entry_size = (type_size + sizeof(uint8_t));

    uint64_t new_obj_size = obj_size + new_value->get_used_bytes();
    uint8_t new_count_type = ObJsonVar::get_var_type(count + 1);
    uint8_t new_entry_type = ObJsonVar::get_var_type(new_obj_size);
    old_entry_type = var_type;
    new_obj_size += (ObJsonVar::get_var_size(ObJsonVar::get_var_type(count + 1)) -
                      ObJsonVar::get_var_size(ObJsonVar::get_var_type(count)));
    new_obj_size += (count + 1) * (ObJsonVar::get_var_size(new_entry_type) - ObJsonVar::get_var_size(old_entry_type));
    if (is_obj_type) {
      new_obj_size += 2 * (count + 1) * (ObJsonVar::get_var_size(new_entry_type) - ObJsonVar::get_var_size(old_entry_type));
    }

    // cause entry type are relevent
    uint8_t old_obj_size_type = ObJsonVar::get_var_type(obj_size);
    uint8_t new_obj_size_type = ObJsonVar::get_var_type(new_obj_size);
    static const int64_t OB_JSONBIN_CONVERGEN_TIME = 3;
    for (int i = 0; i < OB_JSONBIN_CONVERGEN_TIME && new_obj_size_type >old_obj_size_type; ++i) {
      old_entry_type = new_entry_type;
      new_entry_type = ObJsonVar::get_var_type(new_obj_size);
      new_obj_size += (count + 1) * (ObJsonVar::get_var_size(new_entry_type) - ObJsonVar::get_var_size(old_entry_type));
      if (is_obj_type) {
        new_obj_size += 2 * (count + 1) * (ObJsonVar::get_var_size(new_entry_type) - ObJsonVar::get_var_size(old_entry_type));
      }
      old_obj_size_type = new_obj_size_type;
      new_obj_size_type = ObJsonVar::get_var_type(new_obj_size);
      new_entry_type = new_obj_size_type;
    }

    int32_t stk_len = stack_size(stack_buf_);
    ObJsonBuffer nw_stack(allocator_);
    uint32_t top_pos = stk_len;
    bool rebuild_all = false;
    ObJBNodeMeta node;
    if (stk_len > 0) {
      bool rebuild = false;
      if (OB_FAIL(estimate_need_rebuild(nw_stack, new_obj_size - obj_size, 0, top_pos, rebuild))) {
        LOG_WARN("failed: calc update objsize rebuild meta.", K(ret), K(stk_len));
      } else if (!rebuild && OB_FAIL(estimate_need_rebuild_kv_entry(result_, stack_buf_, nw_stack, top_pos, rebuild))) {
        LOG_WARN("failed: calc update entry type rebuild meta.", K(ret), K(stk_len));
      } else if (rebuild) {
        if (top_pos == 0) {
          rebuild_all = true;
        } else {
          stack_at(stack_buf_, top_pos, node);
          // node.obj-size + new_obj_size - obj_size => new object size
          // node.obj-size + new_obj_size - obj_size + new_object_size => the total hole size
          rebuild_all = (node.obj_size_ + new_obj_size - obj_size + new_obj_size) +
                        result_.length() > result_.length() * OB_JSON_BIN_REBUILD_THRESHOLD;
        }
      }
    } else {
      rebuild_all = true;
    }

    uint64_t st_pos = result_.length();
    node.entry_type_ = new_entry_type;
    node.size_type_ = new_obj_size_type;
    node.ver_type_ = get_vertype();
    node.obj_size_ = new_obj_size;
    if (OB_FAIL(insert_internal_v0(node, pos, key, new_value, result_))) {
      LOG_WARN("failed: insert value with meta.", K(ret), K(pos));
    } else if (stk_len == 0) {
      char* new_start = result_.ptr() + node.offset_;
      MEMMOVE(result_.ptr(), new_start, node.obj_size_);
      result_.set_length(node.obj_size_);
      element_count_++;
      bytes_ = node.obj_size_;
    } else if (OB_FAIL(stack_push(nw_stack, node)) || OB_FAIL(stack_push(stack_buf_, node))) {
      LOG_WARN("failed: stack push.", K(ret), K(pos));
    } else if (rebuild_all) {
      ObJsonBuffer tmp_buf(allocator_);
      if (OB_FAIL(rebuild_with_meta(result_.ptr(), result_.length(), stack_buf_, nw_stack, top_pos, stk_len, tmp_buf))) {
        LOG_WARN("failed: rebuild with meta.", K(ret));
      } else if (OB_FAIL(result_.set_length(0)) || OB_FAIL(result_.append(tmp_buf.ptr(), tmp_buf.length()))) {
        LOG_WARN("failed: copy new data.", K(ret));
      } else if (OB_FAIL(move_iter(nw_stack, 0))) {
        LOG_WARN("failed: move iter.", K(ret));
      } else if (OB_FAIL(stack_copy(nw_stack, stack_buf_))) {
        LOG_WARN("failed: stack copy.", K(ret));
      } else if (OB_FAIL(move_parent_iter())) {
        LOG_WARN("failed: move iter.", K(ret));
      }
    } else {
      stack_at(nw_stack, top_pos, node);
      if (OB_FAIL(result_.reserve(node.obj_size_))) {
        LOG_WARN("failed: reserve mem.", K(ret));
      } else if (OB_FAIL(rebuild_with_meta(result_.ptr(), result_.length(), stack_buf_, nw_stack, top_pos, stk_len, result_))) {
        LOG_WARN("failed: rebuild with meta.", K(ret));
      } else if (OB_FAIL(move_iter(nw_stack, top_pos))) {
        LOG_WARN("failed: move iter.", K(ret));
      } else if (OB_FAIL(stack_copy(nw_stack, stack_buf_))) {
        LOG_WARN("failed: stack copy.", K(ret));
      } else if (OB_FAIL(move_parent_iter())) {
        LOG_WARN("failed: move iter.", K(ret));
      } else {
        ObJBNodeMeta upper_node, path_node;
        stack_at(nw_stack, top_pos -1, upper_node);
        stack_at(nw_stack, top_pos, path_node);
        if (OB_FAIL(update_offset(upper_node.offset_, upper_node.idx_, path_node.offset_))) {
          LOG_WARN("failed: update offset.", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      curr_.assign_ptr(result_.ptr(), result_.length());
    }
  }
  return ret;
}

int ObJsonBin::update_v0(int index, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  ObJsonNodeType cur_node_type =  this->json_type();
  ObJBVerType ver_type = this->get_vertype();

  // 1. seek index value offset and value type
  uint64_t offset = pos_;
  char *data = result_.ptr();
  uint8_t node_type, var_type, obj_size_type;
  uint64_t count, obj_size;
  parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);
  uint64_t type_size = ObJsonVar::get_var_size(var_type);
  uint64_t key_entry_size = type_size * 2;
  uint64_t val_entry_size = (type_size + sizeof(uint8_t));
  if (OB_JSON_TYPE_GET_INLINE(node_type) == static_cast<uint8_t>(ObJsonNodeType::J_OBJECT)) {
    offset += key_entry_size * count; // if type is object, need to move over key entry size
  }
  int64_t val_entry_offset = offset + val_entry_size * index;
  bool need_update_parent = false;
  // 2. first try inline new_value
  if (!try_update_inline(new_value, var_type, &val_entry_offset, result_)) {
    // 3. check curr is inlined
    char *val_offset = (data + val_entry_offset);
    uint8_t *val_type = reinterpret_cast<uint8_t*>(val_offset + type_size);
    uint64_t val_type_offset = val_entry_offset + type_size;
    bool is_inlined = OB_JSON_TYPE_IS_INLINE(*val_type);
    // 4. check curr bytes and new_value bytes
    if (OB_FAIL(this->element(index))) {
      LOG_WARN("move iter to child failed.", K(ret), K(index));
    } else {
      ObJsonBin *new_value_bin = new_value;
      // local is not discontinuous, can not do inplace update
      bool can_do_inplace = true;
      if (this->json_type() == ObJsonNodeType::J_ARRAY || this->json_type() == ObJsonNodeType::J_OBJECT) {
        can_do_inplace = (this->is_discontinuous() == false);
      }
      int64_t bytes_changed = is_inlined ? new_value_bin->get_used_bytes() :
                              (new_value_bin->get_used_bytes() - this->get_used_bytes());
      if (bytes_changed <= 0 && can_do_inplace) {
        // 5. do inplace update
        ObJsonBuffer str(allocator_);
        char *new_val_ptr = NULL;
        uint64_t new_val_length = 0;
        if (OB_FAIL(get_update_val_ptr(new_value_bin, new_val_ptr, new_val_length, str))) {
          LOG_WARN("failed to get update val ptr", K(ret));
        } else {
          char *val_ptr = data + pos_;
          MEMCPY(val_ptr, new_val_ptr, new_val_length);
          *val_type = static_cast<uint8_t>(new_value_bin->json_type());
        }
        str.reset();
      } else {
        // new value bytes are bigger then curr bytes, do append
        // 5. do append update
        uint64_t new_val_offset = result_.length();
        ObJBNodeMeta path_node;
        stack_back(stack_buf_, path_node);
        uint8_t new_var_type = ObJsonVar::get_var_type(new_val_offset - path_node.offset_);
        if (new_var_type > var_type) {
          ObJsonBuffer nw_stack(allocator_);
          uint32_t top_pos = stack_size(stack_buf_) - 1;
          bool rebuild = false;
          if (OB_FAIL(estimate_need_rebuild_kv_entry(result_, stack_buf_, nw_stack, top_pos, rebuild))) {
            LOG_WARN("failed to estimate. ", K(ret));
          } else {
            stack_at(nw_stack, top_pos, path_node);
            char* rebuild_data = result_.ptr() + path_node.offset_;
            if (OB_FAIL(result_.reserve(path_node.obj_size_)) ||
                OB_FAIL(rebuild_with_meta(rebuild_data, result_.length() - path_node.offset_, stack_buf_, nw_stack, top_pos,
                                          stack_size(nw_stack) - 1, result_))) {
              LOG_WARN("failed to rebuild with meta.", K(ret), K(top_pos));
            } else {
              ObJBNodeMeta new_top_meta;
              stack_at(nw_stack, top_pos, new_top_meta);
              stack_at(stack_buf_, top_pos, path_node);

              int64_t meta_change = new_top_meta.obj_size_ - path_node.obj_size_;
              if (top_pos != 0) {
                ObJBNodeMeta upper_node, path_node;
                stack_at(nw_stack, top_pos -1, upper_node);
                stack_at(nw_stack, top_pos, path_node);
                if (OB_FAIL(update_offset(upper_node.offset_, upper_node.idx_, path_node.offset_))) {
                  LOG_WARN("failed update offset.", K(ret), K(path_node.offset_), K(upper_node.offset_));
                } else {
                  stack_update(nw_stack, top_pos - 1, upper_node);
                  while (top_pos >= stack_size(stack_buf_) - 1) {
                    stack_back(stack_buf_, path_node, true);
                  }

                  if (OB_FAIL(update_parents(meta_change, false))) {
                    LOG_WARN("failed update parent.", K(ret), K(meta_change));
                  } else {
                    curr_.assign_ptr(result_.ptr(), result_.length());
                    stack_at(stack_buf_, 0, path_node);
                    stack_update(nw_stack, 0, path_node);
                    if (OB_FAIL(move_iter(nw_stack))) {
                      LOG_WARN("failed to locate new pos.", K(ret));
                    } else {
                      stack_copy(nw_stack, stack_buf_);
                    }
                  }
                }
              } else {
                ObJsonBuffer tmp_buf(allocator_);
                ObJBNodeMeta root;
                stack_at(nw_stack, 0, root);
                char* data = result_.ptr();
                uint64_t data_length = result_.length() - root.offset_;
                if (OB_FAIL(rebuild_with_meta(data, data_length, stack_buf_, nw_stack, 0, stack_size(nw_stack) - 1, tmp_buf))) {
                  LOG_WARN("failed to rebuild all obj.", K(ret));
                } else {
                  result_.reuse();
                  if (OB_FAIL(result_.append(tmp_buf.ptr(), tmp_buf.length()))) {
                    LOG_WARN("failed to copy result.", K(ret));
                  } else if (OB_FAIL(move_iter(nw_stack, 0))) {
                    reset_iter();
                    LOG_WARN("failed to move iter.", K(ret));
                  } else {
                    stack_copy(nw_stack, stack_buf_);
                  }
                }
              }
            }
          }
        }
        data = result_.ptr();
        new_val_offset = result_.length();
        if (OB_SUCC(ret)) {
          ObJsonBuffer str(allocator_);
          char *new_val_ptr = NULL;
          uint64_t new_val_length = 0;
          if (OB_FAIL(get_update_val_ptr(new_value_bin, new_val_ptr, new_val_length, str))) {
            LOG_WARN("failed to get update val ptr", K(ret));
          } else {
            char *val_ptr = data + pos_;
            // do append for simple type
            if (OB_FAIL(result_.append(new_val_ptr, new_val_length))) {
              LOG_WARN("failed to append new value. ", K(ret));
            } else { // after result_ append, may do realloc, should refresh curr_
              curr_.assign_ptr(result_.ptr(), result_.length());
            }
          }
          str.reset();
        }
        // update val_offset and val_type
        if (OB_SUCC(ret)) {
          ObJBNodeMeta path_node;
          stack_back(stack_buf_, path_node);
          int64_t parent_pos = path_node.offset_;
          data = result_.ptr();

          type_size = ObJsonVar::get_var_size(path_node.entry_type_);
          key_entry_size = type_size * 2;
          val_entry_size = (type_size + sizeof(uint8_t));
          uint64_t count_size = ObJsonVar::get_var_size(ObJsonVar::get_var_type(count));
          uint64_t obj_size_size = ObJsonVar::get_var_size(path_node.size_type_);

          val_offset = data + path_node.offset_ + OB_JSON_BIN_HEADER_LEN + count_size + obj_size_size;
          val_offset += val_entry_size * index;

          ObJBVerType vertype = *reinterpret_cast<ObJBVerType*>(data + parent_pos);
          if (ObJsonVerType::get_json_type(vertype) == ObJsonNodeType::J_OBJECT) {
            val_offset += key_entry_size * count;
          }
          if (OB_FAIL(ObJsonVar::set_var(new_val_offset - parent_pos, path_node.entry_type_, val_offset))) {
            LOG_WARN("failed to set new value offset.", K(ret));
          } else {
            uint8_t *val_type_ptr = reinterpret_cast<uint8_t*>(val_offset) + ObJsonVar::get_var_size(path_node.entry_type_);
            *val_type_ptr = static_cast<uint8_t>(new_value_bin->json_type());
          }
        }
      }
      // update parent obj size
      if (OB_SUCC(ret)) {
        update_parents(bytes_changed, false);
        if (OB_FAIL(this->move_parent_iter())) { // move curr iter back to parent
          LOG_WARN("failed to move iter back to parent.", K(ret));
        }
      }
    }
  }
  return ret;
}

/*
remove child node at index, execute when iter locates at parent node
*/
int ObJsonBin::remove_v0(size_t index)
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  ObJBVerType ver_type = this->get_vertype();
  // 1. move into element index, get used bytes
  uint64_t used_bytes;
  if (OB_FAIL(this->element(index))) {
    LOG_WARN("failed to get element ", K(index), K(ret));
  } else {
    used_bytes = this->get_used_bytes();
    if (OB_FAIL(this->move_parent_iter())) {
      LOG_WARN("failed to move back to parent ", K(ret));
    }
  }
  // 2. seek index key entry and value entry, do memmove
  if (OB_SUCC(ret)) {
    uint64_t offset = pos_;
    char *data = result_.ptr();
    uint8_t node_type, var_type, obj_size_type;
    uint64_t count, obj_size;
    parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);
    uint64_t type_size = ObJsonVar::get_var_size(var_type);
    uint64_t val_entry_size = (type_size + sizeof(uint8_t));
    uint64_t extend_offset = 0;
    // if it's an object, first remove the key entry
    if (OB_JSON_TYPE_GET_INLINE(node_type) == static_cast<uint8_t>(ObJsonNodeType::J_OBJECT)) {
      uint64_t key_entry_size = type_size * 2;
      char *curr_key_entry = data + offset + key_entry_size * index;
      char *next_key_entry = curr_key_entry + key_entry_size;
      uint64_t len = key_entry_size * (count - index - 1) + val_entry_size * index;
      MEMMOVE(curr_key_entry, next_key_entry, len);
      // Adjust offset of value entry
      offset += key_entry_size * (count - 1);
      extend_offset = key_entry_size;
    }
    int64_t val_entry_offset = offset + val_entry_size * index;
    char *curr_val_entry = data + val_entry_offset;
    char *next_val_entry = curr_val_entry + val_entry_size + extend_offset;
    uint64_t len = val_entry_size * (count - index - 1);
    MEMMOVE(curr_val_entry, next_val_entry, len);

    // update obj size and count for curr iter node
    int64_t bytes_changed = 0 - used_bytes - val_entry_size - extend_offset;
    ObJsonBinObjHeader *header = reinterpret_cast<ObJsonBinObjHeader*>(data + pos_);

    uint64_t obj_count;
    if (OB_FAIL(ObJsonVar::read_var(header->used_size_, header->count_size_, &obj_count))) {
      LOG_WARN("fail to read header count.", K(ret), KP(header->used_size_), K(header->count_size_));
    } else {
      obj_count -= 1;
      if (OB_FAIL(ObJsonVar::set_var(obj_count, header->count_size_, header->used_size_))) {
        LOG_WARN("fail to set header count.", K(ret), K(obj_count), KP(header->used_size_), K(header->count_size_));
      } else {
        // update elememt count
        element_count_ -= 1;
      }
    }

    if (OB_SUCC(ret)) {
      uint64_t new_obj_size;
      uint64_t count_var_size = ObJsonVar::get_var_size(header->count_size_);
      if (OB_FAIL(ObJsonVar::read_var(header->used_size_ + count_var_size, header->obj_size_size_, &new_obj_size))) {
        LOG_WARN("fail to read header obj size.", K(ret), KP(header->used_size_), K(header->obj_size_size_));
      } else {
        new_obj_size += bytes_changed;
        if (OB_FAIL(ObJsonVar::set_var(new_obj_size, header->obj_size_size_, header->used_size_ + count_var_size))) {
          LOG_WARN("fail to set header obj size.", K(ret), K(new_obj_size),
                                                    KP(header->used_size_), K(header->obj_size_size_));
        } else {
          // update obj size for parents
          update_parents(bytes_changed, true);
        }
      }
    }
  }
  return ret;
}


int ObJsonBin::remove(size_t index)
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  ObJBVerType ver_type = this->get_vertype();
  if (node_type != ObJsonNodeType::J_ARRAY && node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(node_type));
  } else if (index >= element_count_) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range.", K(ret), K(index), K(element_count_));
  } else if (!is_alloc_) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json binary is read only.", K(ret), K(is_alloc_));
  } else {
    switch (ver_type) {
      case ObJBVerType::J_ARRAY_V0:
      case ObJBVerType::J_OBJECT_V0: {
        ret = remove_v0(index);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remove version unsurpported.", K(ret), K(index), K(ver_type));
        break;
      }
    }
  }
  return ret;
}

int ObJsonBin::remove(const ObString &key)
{
  INIT_SUCC(ret);
  size_t idx;
  ObJsonNodeType node_type =  this->json_type();
  if (node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
  } else if (!is_alloc_) {
    ret = OB_ERR_READ_ONLY;
  } else if (OB_FAIL(lookup_index(key, &idx))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS; // if not found, return succ
    } else {
      LOG_WARN("failed to lookup key for remove.", K(ret));
    }
  } else if (OB_FAIL(remove(idx))) {
    LOG_WARN("failed to remove idx.", K(ret), K(idx));
  }
  return ret;
}

int ObJsonBin::rebuild()
{
  INIT_SUCC(ret);
  if (!is_alloc_ || allocator_ == NULL) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json binary is read only.", K(ret));
  } else {
    ObJsonBuffer new_bin(allocator_);
    if (OB_FAIL(rebuild(new_bin))) {
      LOG_WARN("rebuild failed.", K(ret));
    } else {
      result_.reuse();
      if (OB_FAIL(result_.append(new_bin.ptr(), new_bin.length()))) {
        LOG_WARN("append for new result falied", K(ret));
      } else {
        curr_.assign_ptr(result_.ptr(), result_.length());
        ret = reset_iter();
      }
    }
  }
  return ret;
}

int ObJsonBin::rebuild_at_iter(ObJsonBuffer &buf)
{
  INIT_SUCC(ret);
  buf.reuse();
  uint8_t ver_type = static_cast<uint8_t>(get_vertype());
  ObJsonNodeType node_type = this->json_type();
  if (node_type != ObJsonNodeType::J_ARRAY && node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
  } else if (OB_FAIL(rebuild_json_value(curr_.ptr() + pos_, curr_.length() - pos_, ver_type, ver_type, uint_val_, buf))) {
    LOG_WARN("rebuild json binary iter falied, ", K(ret));
  }
  return ret;
}

int ObJsonBin::rebuild_json_process_value_v0(const char *data, uint64_t length, const char *old_val_entry,
  uint64_t new_val_entry_offset, uint64_t count, uint8_t var_type, int64_t st_pos, ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  uint64_t type_size = ObJsonVar::get_var_size(var_type);
  uint64_t val_entry_size = (type_size + sizeof(uint8_t));
  uint8_t v_type = static_cast<uint8_t>(JBLS_UINT8);
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t new_val_offset = result.length() - st_pos;
    uint64_t val_offset, val_type;
    if (OB_FAIL(ObJsonVar::read_var(old_val_entry + val_entry_size * i, var_type, &val_offset))) {
      LOG_WARN("failed to read val offset", K(ret));
    } else if (OB_FAIL(ObJsonVar::read_var(old_val_entry + val_entry_size * i + type_size, v_type, &val_type))) {
      LOG_WARN("failed to read val type", K(ret));
    } else {
      uint8_t type = static_cast<uint8_t>(val_type);
      if (!OB_JSON_TYPE_IS_INLINE(type)) {
        ret = rebuild_json_value(data + val_offset, length - val_offset, type, type, val_offset, result);
        if (OB_SUCC(ret)) {
          // fill value offset
          char* new_val_entry = result.ptr() + new_val_entry_offset;
          if (OB_FAIL(ObJsonVar::set_var(new_val_offset, var_type, new_val_entry + val_entry_size * i))) {
            LOG_WARN("failed to set val offset.", K(ret), K(i), K(new_val_offset), K(var_type));
          }
        } else {
          LOG_WARN("rebuild child node failed.", K(ret), K(i), K(val_type));
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::rebuild_json_process_value(const char *data, uint64_t length, const char *old_val_entry,
                                          uint64_t new_val_entry_offset, uint64_t count, uint8_t var_type, int64_t st_pos,
                                          ObJsonBuffer &result, ObJBVerType cur_vertype, ObJBVerType dest_vertype) const
{
  INIT_SUCC(ret);
  ObJsonNodeType type = ObJsonVerType::get_json_type(cur_vertype);
  if (((cur_vertype == ObJBVerType::J_ARRAY_V0 || cur_vertype == ObJBVerType::J_OBJECT_V0) &&
      (dest_vertype == ObJBVerType::J_ARRAY_V0 || dest_vertype == ObJBVerType::J_OBJECT_V0))) {
    ret = rebuild_json_process_value_v0(data, length, old_val_entry, new_val_entry_offset, count, var_type, st_pos, result);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed:rebuild value.", K(ret), K(cur_vertype), K(dest_vertype));
  }
  return ret;
}

/**
 * input data version is v0, rebuilding using latest format
*/
int ObJsonBin::rebuild_json_obj_v0(const char *data, uint64_t length, ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  bool with_key_dict = false;
  int64_t st_pos = result.length();
  uint64_t offset = 0;
  uint8_t node_type, var_type, obj_size_type;
  uint64_t count, obj_size;

  const ObJsonBinObjHeader *header = reinterpret_cast<const ObJsonBinObjHeader*>(data + offset);
  // parsing header using v0 format
  parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);

  // if v0 is latest version and is_continuous, do copy, else serialize as latest version
  if (header->is_continuous_ && ObJBVerType::J_OBJECT_V0 == get_object_vertype()) { // memory is continuous, can do memcopy
    if (OB_FAIL(result.append(data, obj_size))) {
      LOG_WARN("append obj failed.", K(ret), K(obj_size));
    }
  } else {
    // rebuild using latest format
    // copy obj header, key entry, val entry, key(if need)
    uint64_t type_size = ObJsonVar::get_var_size(var_type);
    uint64_t key_entry_size = type_size * 2;
    uint64_t val_entry_size = (type_size + sizeof(uint8_t));
    uint64_t copy_len = offset + key_entry_size * count + val_entry_size * count;
    if (length < copy_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data length is not enough for obj.", K(ret), K(length), K(copy_len));
    } else {
      if (!with_key_dict) { // without key dict, need copy key
        // using latest bin format
        uint64_t key_offset, key_len;
        const char *key_entry = (data + offset);
        const char *last_key_offset_ptr = key_entry + key_entry_size * (count - 1);
        const char *last_key_len_ptr = key_entry + key_entry_size * (count - 1) + type_size;
        // get last key offest and len
        if (OB_FAIL(ObJsonVar::read_var(last_key_offset_ptr, var_type, &key_offset))) {
          LOG_WARN("failed to read key offset", K(ret));
        } else if (OB_FAIL(ObJsonVar::read_var(last_key_len_ptr, var_type, &key_len))) {
          LOG_WARN("failed to read key len", K(ret));
        } else {
          copy_len = key_offset + key_len;
          if (copy_len > length) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length is not enough for obj.", K(ret), K(length), K(copy_len));
          } else if (OB_FAIL(result.append(data, copy_len))) { // if format is changed, must do modify
            LOG_WARN("failed to append data.", K(ret), K(copy_len));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      // process value
      const char *val_entry = (data + offset + key_entry_size * count);
      uint64_t new_val_entry_offset = st_pos + offset + key_entry_size * count;
      ret = rebuild_json_process_value(data, length, val_entry, new_val_entry_offset, count, var_type, st_pos, result,
                                       ObJBVerType::J_OBJECT_V0, ObJBVerType::J_OBJECT_V0);
      if (OB_SUCC(ret)) {
        // set new result is continuous, remove highest 1bit
        ObJsonBinObjHeader *new_header = reinterpret_cast<ObJsonBinObjHeader*>(result.ptr() + st_pos);
        new_header->is_continuous_ = 1;
      } else {
        LOG_WARN("rebuild child node failed.", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonBin::rebuild_json_obj(const char *data, uint64_t length, ObJsonBuffer &result,
                                ObJBVerType src_vertype, ObJBVerType dest_vertype) const
{
  INIT_SUCC(ret);
  if (ObJBVerType::J_OBJECT_V0 == src_vertype && ObJBVerType::J_OBJECT_V0 == dest_vertype) {
    ret = rebuild_json_obj_v0(data, length, result);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild json object, invalid vertype.", K(ret), K(src_vertype), K(dest_vertype));
  }
  return ret;
}

int ObJsonBin::rebuild_json_array_v0(const char *data, uint64_t length, ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  int64_t st_pos = result.length();
  uint64_t offset = 0;
  uint8_t node_type, var_type, obj_size_type;
  uint64_t count, obj_size;
  if (length <= OB_JSON_BIN_ARR_HEADER_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data length is not enough.", K(ret), K(length));
  } else {
    const ObJsonBinObjHeader *header = reinterpret_cast<const ObJsonBinObjHeader*>(data + offset);
    parse_obj_header(data, offset, node_type, var_type, obj_size_type, count, obj_size);
    if (header->is_continuous_) {
      if (OB_FAIL(result.append(data, obj_size))) {
        LOG_WARN("append array failed.", K(ret), K(obj_size));
      }
    } else {
      // copy obj header, val entry
      uint64_t type_size = ObJsonVar::get_var_size(var_type);
      uint64_t val_entry_size = (type_size + sizeof(uint8_t));
      uint64_t copy_len = offset + val_entry_size * count;
      if (copy_len > length) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length is not enough for val entry.", K(ret), K(length), K(copy_len));
      } else if (OB_FAIL(result.append(data, copy_len))) {
        LOG_WARN("failed to append data.", K(ret), K(copy_len));
      } else {
        // process value
        const char *val_entry = (data + offset);
        uint64_t new_val_entry_offset = st_pos + offset;
        ret = rebuild_json_process_value(data, length, val_entry, new_val_entry_offset, count, var_type, st_pos,
                                         result, ObJBVerType::J_ARRAY_V0, ObJBVerType::J_ARRAY_V0);
        if (OB_SUCC(ret)) {
          // set new result is continuous, remove highest 1bit
          ObJsonBinObjHeader *new_header = reinterpret_cast<ObJsonBinObjHeader*>(result.ptr() + st_pos);
          new_header->is_continuous_ = 1;
        } else {
          LOG_WARN("rebuild child node failed.", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJsonBin::rebuild_json_array(const char *data, uint64_t length, ObJsonBuffer &result,
                                  ObJBVerType src_vertype, ObJBVerType dest_vertype) const
{
  INIT_SUCC(ret);
  if (ObJBVerType::J_ARRAY_V0 == src_vertype && ObJBVerType::J_ARRAY_V0 == dest_vertype) {
    ret = rebuild_json_array_v0(data, length, result);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild json object.", K(ret));
  }
  return ret;
}

int ObJsonBin::rebuild_json_value(const char *data,
                                  uint64_t length,
                                  uint8_t src_type,
                                  uint8_t dst_type,
                                  uint64_t inline_data,
                                  ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  bool is_src_inlined = OB_JSON_TYPE_IS_INLINE(src_type);
  bool is_dst_inlined = OB_JSON_TYPE_IS_INLINE(dst_type);
  ObJBVerType src_vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(src_type));
  ObJBVerType dest_vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(dst_type));
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(src_vertype);
  switch (node_type) {
    case ObJsonNodeType::J_NULL: {
      if (!is_dst_inlined) {
        if (OB_FAIL(result.append("\0", sizeof(char)))) {
          LOG_WARN("failed to rebuild null type.", K(ret));
        }
      }
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      int64_t pos = 0;
      number::ObNumber temp_number;
      if (OB_FAIL(temp_number.deserialize(data, length, pos))) {
        LOG_WARN("failed to deserialize decimal data", K(ret));
      } else {
        ret = result.append(data, pos);
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      if (!is_dst_inlined) {
        if (is_src_inlined) {
          if (OB_FAIL(serialize_json_integer(inline_data, result))) {
            LOG_WARN("failed to rebuild serialize integer.", K(ret), K(inline_data));
          }
        } else {
          int64_t val = 0;
          int64_t pos = 0;
          if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
            LOG_WARN("decode integer failed.", K(ret), K(length));
          } else if (OB_FAIL(result.append(data, pos))) {
            LOG_WARN("failed to append integer date.", K(ret), K(pos));
          }
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      if (length < sizeof(double)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length not enough for double.", K(ret), K(length));
      } else {
        ret = result.append(data, sizeof(double));
      }
      break;
    }
    case ObJsonNodeType::J_STRING: {
      int64_t val = 0;
      int64_t pos = 0;
      if (src_vertype == ObJBVerType::J_STRING_V0) {
        pos += sizeof(uint8_t);
        if (OB_FAIL(serialization::decode_vi64(data, length, pos, &val))) {
          LOG_WARN("fail to decode str length.", K(ret), K(length));
        } else if (length < pos + val) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length is not enough for string.", K(ret), K(length), K(pos), K(val));
        } else {
          uint64_t str_length = static_cast<uint64_t>(val);
          ret = result.append(data, str_length + pos);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid string vertype.", K(ret), K(src_vertype));
      }
      break;
    }
    case ObJsonNodeType::J_OBJECT: {
      ret = rebuild_json_obj(data, length, result, src_vertype, dest_vertype);
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      ret = rebuild_json_array(data, length, result, src_vertype, dest_vertype);
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      if (!is_dst_inlined) {
        if (OB_FAIL(serialize_json_integer(inline_data, result))) {
          LOG_WARN("failed to rebuild serialize boolean.", K(ret), K(inline_data));
        }
      }
      break;
    }
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      if (length < sizeof(int32_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length not enough for date.", K(ret), K(length));
      } else {
        ret = result.append(data, sizeof(int32_t));
      }
      break;
    }
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      if (length < sizeof(int64_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data length not enough for datetime.", K(ret), K(length));
      } else {
        ret = result.append(data, sizeof(int64_t));
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      if (src_vertype == ObJBVerType::J_OPAQUE_V0) {
        if (length < sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length not enough for opaque len.", K(ret), K(length));
        } else {
          uint64_t val_len = *reinterpret_cast<const uint64_t*>(data + sizeof(uint8_t) + sizeof(uint16_t));
          if (length < sizeof(uint16_t) + sizeof(uint64_t) + val_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data length not enough for opaque val.", K(ret), K(length), K(val_len));
          } else {
            ret = result.append(data, val_len + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid json opaque vertype.", K(ret), K(src_vertype));
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

int ObJsonBin::rebuild(ObJsonBuffer &result)
{
  INIT_SUCC(ret);
  result.reuse();
  char *ptr = curr_.ptr();
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("binary is null ptr.", K(ret));
  } else {
    uint64_t offset = 0;
    uint8_t type = 0;
    // first parse header type
    type = *reinterpret_cast<uint8_t*>(ptr);

    // do recursion
    if (OB_FAIL(rebuild_json_value(ptr + offset, curr_.length() - offset, type, type, uint_val_, result))) {
      LOG_WARN("do rebuild recursion failed.", K(ret), K(type));
    }
  }
  return ret;
}

void ObJsonBin::destroy()
{
  result_.reset();
  stack_buf_.reset();
}

int ObJsonBin::stack_copy(ObJsonBuffer& src, ObJsonBuffer& dst)
{
  INIT_SUCC(ret);
  dst.reset();
  uint64_t len = src.length();
  if (OB_FAIL(dst.append(src.ptr(), src.length()))) {
    LOG_WARN("copy path stack failed", K(ret), K(src.length()));
  }
  return ret;
}

int ObJsonBin::stack_pop(ObJsonBuffer& stack)
{
  INIT_SUCC(ret);
  uint64_t len = stack.length();
  if (len > 0) {
    stack.set_length(len - JB_PATH_NODE_LEN);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed pop stack.", K(ret));
  }
  return ret;
}

int ObJsonBin::stack_back(ObJsonBuffer& stack, ObJBNodeMeta& node, bool is_pop)
{
  INIT_SUCC(ret);
  uint64_t len = stack.length();
  if (len > 0) {
    char* data = (stack.ptr() + len) - JB_PATH_NODE_LEN;
    node = *(reinterpret_cast<ObJBNodeMeta*>(data));
    if (is_pop) {
      stack.set_length(len - JB_PATH_NODE_LEN);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get stack node.", K(ret));
  }
  return ret;
}

int ObJsonBin::stack_push(ObJsonBuffer& stack, const ObJBNodeMeta& node)
{
  return stack.append(reinterpret_cast<const char*>(&node), JB_PATH_NODE_LEN);
}

int ObJsonBin::stack_at(ObJsonBuffer& stack, uint32_t idx, ObJBNodeMeta& node)
{
  INIT_SUCC(ret);
  uint32_t size = stack.length() / JB_PATH_NODE_LEN;
  if (size > idx) {
    char* data = (stack.ptr() + idx * JB_PATH_NODE_LEN);
    node = *(reinterpret_cast<ObJBNodeMeta*>(data));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed invalid id.", K(idx), K(size));
  }
  return ret;
}

void ObJsonBin::stack_update(ObJsonBuffer& stack, uint32_t idx, const ObJBNodeMeta& new_value)
{
  ObJBNodeMeta* node = reinterpret_cast<ObJBNodeMeta*>(stack.ptr() + idx * JB_PATH_NODE_LEN);
  *node = new_value;
}

int32_t ObJsonBin::stack_size(const ObJsonBuffer& stack) const
{
  return stack.length() / JB_PATH_NODE_LEN;
}

void ObJsonBin::stack_reset(ObJsonBuffer& stack)
{
  stack.reset();
}

ObJBVerType ObJsonVerType::get_json_vertype(ObJsonNodeType in_type)
{
  uint8_t in = static_cast<uint8_t>(in_type);
  ObJsonNodeType type = static_cast<ObJsonNodeType>(OB_JSON_TYPE_GET_INLINE(in));
  ObJBVerType ret_type;

  switch (type) {
    case ObJsonNodeType::J_NULL: {
      ret_type = ObJsonBin::get_null_vertype();
      break;
    }
    case ObJsonNodeType::J_DECIMAL: {
      ret_type = ObJsonBin::get_decimal_vertype();
      break;
    }
    case ObJsonNodeType::J_INT: {
      ret_type = ObJsonBin::get_int_vertype();
      break;
    }
    case ObJsonNodeType::J_UINT: {
      ret_type = ObJsonBin::get_uint_vertype();
      break;
    }
    case ObJsonNodeType::J_DOUBLE: {
      ret_type = ObJsonBin::get_double_vertype();
      break;
    }
    case ObJsonNodeType::J_STRING: {
      ret_type = ObJsonBin::get_string_vertype();
      break;
    }
    case ObJsonNodeType::J_OBJECT: {
      ret_type = ObJsonBin::get_object_vertype();
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      ret_type = ObJsonBin::get_array_vertype();
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      ret_type = ObJsonBin::get_boolean_vertype();
      break;
    }
    case ObJsonNodeType::J_DATE: {
      ret_type = ObJsonBin::get_date_vertype();
      break;
    }
    case ObJsonNodeType::J_TIME: {
      ret_type = ObJsonBin::get_time_vertype();
      break;
    }
    case ObJsonNodeType::J_DATETIME: {
      ret_type = ObJsonBin::get_datetime_vertype();
      break;
    }
    case ObJsonNodeType::J_TIMESTAMP: {
      ret_type = ObJsonBin::get_timestamp_vertype();
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      ret_type = ObJsonBin::get_opaque_vertype();
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      ret_type = ObJsonBin::get_ofloat_vertype();
      break;
    }
    case ObJsonNodeType::J_ODOUBLE: {
      ret_type = ObJsonBin::get_odouble_vertype();
      break;
    }
    case ObJsonNodeType::J_ODECIMAL: {
      ret_type = ObJsonBin::get_odecimal_vertype();
      break;
    }
    case ObJsonNodeType::J_OINT: {
      ret_type = ObJsonBin::get_oint_vertype();
      break;
    }
    case ObJsonNodeType::J_OLONG: {
      ret_type = ObJsonBin::get_olong_vertype();
      break;
    }
    case ObJsonNodeType::J_OBINARY: {
      ret_type = ObJsonBin::get_obinary_vertype();
      break;
    }
    case ObJsonNodeType::J_OOID: {
      ret_type = ObJsonBin::get_ooid_vertype();
      break;
    }
    case ObJsonNodeType::J_ORAWHEX: {
      ret_type = ObJsonBin::get_orawhex_vertype();
      break;
    }
    case ObJsonNodeType::J_ORAWID: {
      ret_type = ObJsonBin::get_orawid_vertype();
      break;
    }
    case ObJsonNodeType::J_ORACLEDATE: {
      ret_type = ObJsonBin::get_oracledate_vertype();
      break;
    }
    case ObJsonNodeType::J_ODATE: {
      ret_type = ObJsonBin::get_odate_vertype();
      break;
    }
    case ObJsonNodeType::J_OTIMESTAMP: {
      ret_type = ObJsonBin::get_otimestamp_vertype();
      break;
    }
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      ret_type = ObJsonBin::get_otimestamptz_vertype();
      break;
    }
    case ObJsonNodeType::J_ODAYSECOND: {
      ret_type = ObJsonBin::get_ointervalDS_vertype();
      break;
    }
    case ObJsonNodeType::J_OYEARMONTH: {
      ret_type = ObJsonBin::get_ointervalYM_vertype();
      break;
    }
    default: {
      ret_type = static_cast<ObJBVerType>(type);
      break;
    }
  }

  return ret_type;
}

ObJsonNodeType ObJsonVerType::get_json_type(ObJBVerType type)
{
  ObJsonNodeType ret_type;

  switch (type) {
    case ObJBVerType::J_NULL_V0: {
      ret_type = ObJsonNodeType::J_NULL;
      break;
    }
    case ObJBVerType::J_DECIMAL_V0: {
      ret_type = ObJsonNodeType::J_DECIMAL;
      break;
    }
    case ObJBVerType::J_INT_V0: {
      ret_type = ObJsonNodeType::J_INT;
      break;
    }
    case ObJBVerType::J_UINT_V0: {
      ret_type = ObJsonNodeType::J_UINT;
      break;
    }
    case ObJBVerType::J_DOUBLE_V0: {
      ret_type = ObJsonNodeType::J_DOUBLE;
      break;
    }
    case ObJBVerType::J_STRING_V0: {
      ret_type = ObJsonNodeType::J_STRING;
      break;
    }
    case ObJBVerType::J_OBJECT_V0: {
      ret_type = ObJsonNodeType::J_OBJECT;
      break;
    }
    case ObJBVerType::J_ARRAY_V0: {
      ret_type = ObJsonNodeType::J_ARRAY;
      break;
    }
    case ObJBVerType::J_BOOLEAN_V0: {
      ret_type = ObJsonNodeType::J_BOOLEAN;
      break;
    }
    case ObJBVerType::J_DATE_V0: {
      ret_type = ObJsonNodeType::J_DATE;
      break;
    }
    case ObJBVerType::J_TIME_V0: {
      ret_type = ObJsonNodeType::J_TIME;
      break;
    }
    case ObJBVerType::J_DATETIME_V0: {
      ret_type = ObJsonNodeType::J_DATETIME;
      break;
    }
    case ObJBVerType::J_TIMESTAMP_V0: {
      ret_type = ObJsonNodeType::J_TIMESTAMP;
      break;
    }
    case ObJBVerType::J_OPAQUE_V0: {
      ret_type = ObJsonNodeType::J_OPAQUE;
      break;
    }
    case ObJBVerType::J_OFLOAT_V0: {
      ret_type = ObJsonNodeType::J_OFLOAT;
      break;
    }
    case ObJBVerType::J_ODOUBLE_V0: {
      ret_type = ObJsonNodeType::J_ODOUBLE;
      break;
    }
    case ObJBVerType::J_ODECIMAL_V0: {
      ret_type = ObJsonNodeType::J_ODECIMAL;
      break;
    }
    case ObJBVerType::J_OINT_V0: {
      ret_type = ObJsonNodeType::J_OINT;
      break;
    }
    case ObJBVerType::J_OLONG_V0: {
      ret_type = ObJsonNodeType::J_OLONG;
      break;
    }
    case ObJBVerType::J_OBINARY_V0: {
      ret_type = ObJsonNodeType::J_OBINARY;
      break;
    }
    case ObJBVerType::J_OOID_V0: {
      ret_type = ObJsonNodeType::J_OOID;
      break;
    }
    case ObJBVerType::J_ORAWHEX_V0: {
      ret_type = ObJsonNodeType::J_ORAWHEX;
      break;
    }
    case ObJBVerType::J_ORAWID_V0: {
      ret_type = ObJsonNodeType::J_ORAWID;
      break;
    }
    case ObJBVerType::J_ORACLEDATE_V0: {
      ret_type = ObJsonNodeType::J_ORACLEDATE;
      break;
    }
    case ObJBVerType::J_ODATE_V0: {
      ret_type = ObJsonNodeType::J_ODATE;
      break;
    }
    case ObJBVerType::J_OTIMESTAMP_V0: {
      ret_type = ObJsonNodeType::J_OTIMESTAMP;
      break;
    }
    case ObJBVerType::J_OTIMESTAMPTZ_V0: {
      ret_type = ObJsonNodeType::J_OTIMESTAMPTZ;
      break;
    }
    case ObJBVerType::J_ODAYSECOND_V0: {
      ret_type = ObJsonNodeType::J_ODAYSECOND;
      break;
    }
    case ObJBVerType::J_OYEARMONTH_V0: {
      ret_type = ObJsonNodeType::J_OYEARMONTH;
      break;
    }
    default: {
      ret_type = static_cast<ObJsonNodeType>(type);
      break;
    }
  }

  return ret_type;
}

bool ObJsonVerType::is_array(ObJBVerType type)
{
  return (type == J_ARRAY_V0);
}

bool ObJsonVerType::is_object(ObJBVerType type)
{
  return (type == J_OBJECT_V0);
}

bool ObJsonVerType::is_custom(ObJBVerType type)
{
  return (type == J_OPAQUE_V0);
}

bool ObJsonVerType::is_opaque_or_string(ObJBVerType type)
{
  return (type == J_OPAQUE_V0 ||
          type == J_STRING_V0 ||
          type == J_OBINARY_V0 ||
          type == J_OOID_V0 ||
          type == J_ORAWHEX_V0 ||
          type == J_ORAWID_V0 ||
          type == J_ODAYSECOND_V0 ||
          type == J_OYEARMONTH_V0);
}

bool ObJsonVerType::is_opaque_or_string(ObJsonNodeType type)
{
  return (type == ObJsonNodeType::J_OPAQUE ||
          type == ObJsonNodeType::J_STRING ||
          type == ObJsonNodeType::J_OBINARY ||
          type == ObJsonNodeType::J_OOID ||
          type == ObJsonNodeType::J_ORAWHEX ||
          type == ObJsonNodeType::J_ORAWID ||
          type == ObJsonNodeType::J_ODAYSECOND ||
          type == ObJsonNodeType::J_OYEARMONTH);
}

bool ObJsonVerType::is_scalar(ObJBVerType type)
{
  bool ret_bool = false;
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(type);
  switch (node_type) {
    case ObJsonNodeType::J_NULL :
    case ObJsonNodeType::J_UINT :
    case ObJsonNodeType::J_INT :
    case ObJsonNodeType::J_DOUBLE :
    case ObJsonNodeType::J_STRING :
    case ObJsonNodeType::J_BOOLEAN :
    case ObJsonNodeType::J_DATE :
    case ObJsonNodeType::J_DATETIME :
    case ObJsonNodeType::J_TIMESTAMP :
    case ObJsonNodeType::J_OPAQUE :
    case ObJsonNodeType::J_OFLOAT :
    case ObJsonNodeType::J_ODOUBLE :
    case ObJsonNodeType::J_ODECIMAL :
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_OBINARY :
    case ObJsonNodeType::J_OINT :
    case ObJsonNodeType::J_OLONG :
    case ObJsonNodeType::J_OOID :
    case ObJsonNodeType::J_ORACLEDATE :
    case ObJsonNodeType::J_ODATE :
    case ObJsonNodeType::J_OTIMESTAMP :
    case ObJsonNodeType::J_OTIMESTAMPTZ :
    case ObJsonNodeType::J_ODAYSECOND :
    case ObJsonNodeType::J_OYEARMONTH : {
      ret_bool = true;
    }
    default : {
      // do nothing
    }
  };
  return ret_bool;
}

/* var size */
int ObJsonVar::read_var(const char *data, uint8_t type, uint64_t *var)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(data)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input data null val.", K(ret));
  } else {
    ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
    switch (size) {
      case JBLS_UINT8: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(data));
        break;
      }
      case JBLS_UINT16: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint16_t*>(data));
        break;
      }
      case JBLS_UINT32: {
        *var = static_cast<uint64_t>(*reinterpret_cast<const uint32_t*>(data));
        break;
      }
      case JBLS_UINT64: {
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

int ObJsonVar::append_var(uint64_t var, uint8_t type, ObJsonBuffer &result)
{
  INIT_SUCC(ret);
  ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
  switch (size) {
    case JBLS_UINT8: {
      uint8_t var_trans = static_cast<uint8_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint8_t));
      break;
    }
    case JBLS_UINT16: {
      uint16_t var_trans = static_cast<uint16_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint16_t));
      break;
    }
    case JBLS_UINT32: {
      uint32_t var_trans = static_cast<uint32_t>(var);
      ret = result.append(reinterpret_cast<const char*>(&var_trans), sizeof(uint32_t));
      break;
    }
    case JBLS_UINT64: {
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

int ObJsonVar::reserve_var(uint8_t type, ObJsonBuffer &result)
{
  INIT_SUCC(ret);
  ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
  switch (size) {
    case JBLS_UINT8: {
      uint8_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint8_t));
      break;
    }
    case JBLS_UINT16: {
      uint16_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint16_t));
      break;
    }
    case JBLS_UINT32: {
      uint32_t var = 0;
      ret = result.append(reinterpret_cast<const char*>(&var), sizeof(uint32_t));
      break;
    }
    case JBLS_UINT64: {
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

int ObJsonVar::set_var(uint64_t var, uint8_t type, char *pos)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(pos)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("output pos is null.", K(ret));
  } else {
    ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
    switch (size) {
      case JBLS_UINT8: {
        uint8_t *val_pos = reinterpret_cast<uint8_t*>(pos);
        *val_pos = static_cast<uint8_t>(var);
        break;
      }
      case JBLS_UINT16: {
        uint16_t *val_pos = reinterpret_cast<uint16_t*>(pos);
        *val_pos = static_cast<uint16_t>(var);
        break;
      }
      case JBLS_UINT32: {
        uint32_t *val_pos = reinterpret_cast<uint32_t*>(pos);
        *val_pos = static_cast<uint32_t>(var);
        break;
      }
      case JBLS_UINT64: {
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

uint64_t ObJsonVar::get_var_size(uint8_t type)
{
  uint64_t var_size = JBLS_MAX;
  ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
  switch (size) {
    case JBLS_UINT8: {
      var_size = sizeof(uint8_t);
      break;
    }
    case JBLS_UINT16: {
      var_size = sizeof(uint16_t);
      break;
    }
    case JBLS_UINT32: {
      var_size = sizeof(uint32_t);
      break;
    }
    case JBLS_UINT64: {
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

uint8_t ObJsonVar::get_var_type(uint64_t var)
{
  ObJsonBinLenSize lsize = JBLS_UINT64;
  if ((var & 0xFFFFFFFFFFFFFF00ULL) == 0) {
    lsize = JBLS_UINT8;
  } else if ((var & 0xFFFFFFFFFFFF0000ULL) == 0) {
    lsize = JBLS_UINT16;
  } else if ((var & 0xFFFFFFFF00000000ULL) == 0) {
    lsize = JBLS_UINT32;
  }
  return static_cast<uint8_t>(lsize);
}

int ObJsonVar::read_var(const char *data, uint8_t type, int64_t *var)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(data)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input data is null.", K(ret));
  } else {
    ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
    switch (size) {
      case JBLS_UINT8: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int8_t*>(data));
        break;
      }
      case JBLS_UINT16: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int16_t*>(data));
        break;
      }
      case JBLS_UINT32: {
        *var = static_cast<int64_t>(*reinterpret_cast<const int32_t*>(data));
        break;
      }
      case JBLS_UINT64: {
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

uint64_t ObJsonVar::var_int2uint(int64_t var)
{
  ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(ObJsonVar::get_var_type(var));
  uint64 val = 0;
  switch (size) {
    case JBLS_UINT8: {
      val = static_cast<uint64_t>(static_cast<int8_t>(var));
      break;
    }
    case JBLS_UINT16: {
      val = static_cast<uint64_t>(static_cast<int16_t>(var));
      break;
    }
    case JBLS_UINT32: {
      val = static_cast<uint64_t>(static_cast<int32_t>(var));
      break;
    }
    case JBLS_UINT64: {
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

int64_t ObJsonVar::var_uint2int(uint64_t var, uint8_t entry_size)
{
  ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(entry_size);
  int64_t val = 0;
  switch (size) {
    case JBLS_UINT8: {
      if (var > INT8_MAX) {
        val = static_cast<int64_t>(static_cast<int8_t>(static_cast<uint8_t>(var)));
      } else {
        val = static_cast<int64_t>(static_cast<uint8_t>(static_cast<uint8_t>(var)));
      }
      break;
    }
    case JBLS_UINT16: {
      if (var > INT16_MAX) {
        val = static_cast<int64_t>(static_cast<int16_t>(static_cast<uint16_t>(var)));
      } else {
        val = static_cast<int64_t>(static_cast<uint16_t>(static_cast<uint16_t>(var)));
      }
      break;
    }
    case JBLS_UINT32: {
      val = static_cast<int64_t>(static_cast<int32_t>(static_cast<uint32_t>(var)));
      break;
    }
    case JBLS_UINT64: {
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

uint8_t ObJsonVar::get_var_type(int64_t var)
{
  ObJsonBinLenSize lsize = JBLS_UINT64;
  if (var <= INT8_MAX && var >= INT8_MIN) {
    lsize = JBLS_UINT8;
  } else if (var <= INT16_MAX && var >= INT16_MIN) {
    lsize = JBLS_UINT16;
  } else if (var <= INT32_MAX && var >= INT32_MIN) {
    lsize = JBLS_UINT32;
  }
  return static_cast<uint8_t>(lsize);
}
} // namespace common
} // namespace oceanbase
