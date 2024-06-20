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
#include "common/object/ob_obj_type.h"
#include "ob_json_bin.h"
#include "ob_json_tree.h"
#include "ob_json_diff.h"

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
  int parent_idx = -1;
  int is_equal = -1;
  if (OB_ISNULL(old_node) || OB_ISNULL(new_node)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null param", K(ret), KP(old_node), KP(new_node));
  } else if (old_node->is_tree() || new_node->is_tree()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is tree, but need binary", K(ret), K(old_node), K(new_node));
  } else if (old_node->json_type() == new_node->json_type() && OB_FAIL(old_node->compare(*new_node, is_equal))) {
    LOG_WARN("compare fail", K(ret), KPC(old_node), KPC(new_node));
  } else if (0 == is_equal) { // if is equal, no need do update
  } else {
    ObJBNodeMeta node_meta;
    ObJsonNodeType j_type = json_type();
    ObJBVerType vertype = get_vertype();
    const ObJsonBin *old_bin = static_cast<const ObJsonBin*>(old_node);
    if (j_type != ObJsonNodeType::J_ARRAY && j_type != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error curr type. not support replace", K(ret), K(j_type));
    } else if (old_bin->node_stack_.size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old child no parent", K(ret), K(meta_), KPC(old_bin));
    } else if (OB_FAIL(old_bin->node_stack_.back(node_meta))) {
      LOG_WARN("old_bin get node meta fail", K(ret), KPC(old_bin));
    } else if (node_meta.offset_ != pos_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old_bin parent not match", K(ret), K(node_meta), K(pos_),KPC(old_bin));
    } else if (OB_FAIL(update(node_meta.idx_, static_cast<ObJsonBin *>(new_node)))) {
      LOG_WARN("replace with new value failed.", K(ret), K(node_meta), KPC(old_bin), KPC(new_node));
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

int ObJsonBin::create_new_binary(ObIJsonBase *value, ObJsonBin *&new_bin) const
{
  INIT_SUCC(ret);
  ObString sub;
  bool is_seek_only = get_seek_flag();
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

  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(new_bin = new (buf) ObJsonBin(allocator))) {
  } else if (!(json_type() == ObJsonNodeType::J_ARRAY || json_type() == ObJsonNodeType::J_OBJECT || ObJsonVerType::is_opaque_or_string(json_type()))) {
    if (OB_FAIL(reset_child(*new_bin, meta_.type_, pos_, meta_.entry_size_))) {
      LOG_WARN("reset child value fail", K(ret), K(meta_));
    } else {
      new_bin->set_seek_flag(is_seek_only);
    }
  } else if (OB_FAIL(reset_child(*new_bin, pos_))) {
    LOG_WARN("reset_child fail", K(ret), K(meta_));
  } else {
    new_bin->set_seek_flag(is_seek_only);
  }

  return ret;
}

int ObJsonBin::clone_new_node(ObJsonBin*& res, common::ObIAllocator *allocator) const
{
  INIT_SUCC(ret);
  void *buf = NULL;
  bool is_seek_only = get_seek_flag();
  if (res != NULL) { // use stack memory
    buf = res;
    allocator = res->get_allocator();
  } else if (OB_ISNULL(allocator)) { // check allocator_
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json bin allocator is null", K(ret));
  } else { // use allocator_
    buf = allocator->alloc(sizeof(ObJsonBin));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc json bin fail", K(ret), K(sizeof(ObJsonBin)));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(res = new (buf) ObJsonBin(allocator))) {
  } else if (!(json_type() == ObJsonNodeType::J_ARRAY || json_type() == ObJsonNodeType::J_OBJECT || ObJsonVerType::is_opaque_or_string(json_type()))) {
    if (OB_FAIL(reset_child(*res, meta_.type_, pos_, meta_.entry_size_))) {
      LOG_WARN("reset child value fail", K(ret), K(meta_));
    }
  } else if (OB_FAIL(reset_child(*res, pos_))) {
    LOG_WARN("reset_child fail", K(ret), K(meta_));
  }
  if (OB_FAIL(ret)) {
  } else if (! is_seek_only_ && OB_FAIL(res->node_stack_.copy(this->node_stack_))) {
    LOG_WARN("copy node stack fail", K(ret));
  } else {
    res->set_seek_flag(is_seek_only);
  }
  return ret;
}

int ObJsonBin::serialize_number_to_json_decimal(number::ObNumber number, ObJsonBuffer &result)
{
  INIT_SUCC(ret);
  ObPrecision prec = get_decimal_precision();
  ObScale scale = get_decimal_scale();
  int64_t ser_len = number.get_serialize_size() + serialization::encoded_length_i16(prec)
                    + serialization::encoded_length_i16(scale);
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
  } else if (OB_FAIL(number.serialize(result.ptr(), result.capacity(), pos))) {
    LOG_WARN("failed to serialize for decimal value", K(ret), K(pos));
  } else if (OB_FAIL(result.set_length(pos))){
    LOG_WARN("failed to update len for decimal json obj", K(ret), K(pos));
  }
  return ret;
}

int ObJsonBin::get_total_value(ObStringBuffer &result) const
{
  INIT_SUCC(ret);
  ObJBVerType j_type = get_vertype();
  if (ObJsonVerType::is_scalar(j_type)) {
    if (OB_FAIL(rebuild_json_value(result))) {
      LOG_WARN("rebuild_json_value fail", K(ret), K(pos_), KPC(this));
    }
  } else {
    uint64_t area_size = 0;
    uint64_t total_len = cursor_->get_length();
    ObString value;
    if (OB_NOT_NULL(ctx_) && ctx_->extend_seg_offset_ != 0 && ctx_->extend_seg_offset_ != total_len) {
      if (OB_FAIL(rebuild_json_value(result))) {
        LOG_WARN("rebuild_json_value fail", K(ret), K(pos_), KPC(this));
      }
    } else if (OB_FAIL(get_area_size(area_size))) {
      LOG_WARN("get_area_size", K(ret), K(pos_), KPC(this));
    } else if (OB_FAIL(cursor_->get(pos_, area_size, value))) {
      LOG_WARN("cursor get_data fail", K(ret), K(pos_), K(area_size), KPC(this));
    } else if (OB_FAIL(result.append(value.ptr(), value.length()))) {
      LOG_WARN("failed to append null json obj", K(ret));
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
  } else if (! is_seek_only_ && OB_FAIL(new_bin->node_stack_.copy(this->node_stack_))) {
    LOG_WARN("copy node stack fail", K(ret));
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
  } else if (! is_seek_only_ && OB_FAIL(new_bin->node_stack_.copy(this->node_stack_))) {
    LOG_WARN("copy node stack fail", K(ret));
  } else if (OB_FAIL(new_bin->element(index))) {
    LOG_WARN("fail to access index node for new json bin.", K(ret), K(index));
  } else {
    value = new_bin;
  }

  return ret;
}

int ObJsonBin::get_object_value(uint64_t index, ObString &key, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  ObJsonBin *new_bin = NULL;

  if (OB_FAIL(check_valid_object_op(index))) {
    LOG_WARN("invalid json object operation", K(ret), K(index));
  } else if (OB_FAIL(create_new_binary(value, new_bin))) {
    LOG_WARN("fail to create sub binary", K(ret), K(index));
  } else if (OB_FAIL(new_bin->get_key_in_object(index, key))) {
    LOG_WARN("fail to access index node for new json bin.", K(ret), K(index));
  } else if (OB_FAIL(new_bin->get_element_in_object(index))) {
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
  } else if (! is_seek_only_ && OB_FAIL(new_bin->node_stack_.copy(this->node_stack_))) {
    LOG_WARN("copy node stack fail", K(ret));
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

int ObJsonBinSerializer::serialize_json_object(ObJsonObject *object, ObJsonBuffer &result, uint32_t depth)
{
  INIT_SUCC(ret);
  uint64_t element_count = object->element_count();
  const int64_t start_pos = result.length();
  ObJsonBin obj_bin;
  ObJsonBinMeta meta;
  uint64_t obj_size = object->get_serialize_size();
  meta.set_type(ObJsonBin::get_object_vertype(), false);
  meta.set_element_count(element_count);
  meta.set_element_count_var_type(ObJsonVar::get_var_type(element_count));
  meta.set_obj_size(obj_size);
  meta.set_obj_size_var_type(ObJsonVar::get_var_type(obj_size));
  meta.set_entry_var_type(meta.obj_size_var_type());
  meta.set_is_continuous(true);
  meta.calc_entry_array();

  if (OB_FAIL(meta.to_header(result))) {
    LOG_WARN("to obj header fail", K(ret));
  } else if (OB_FAIL(obj_bin.reset(result.string(), start_pos, nullptr))) {
    LOG_WARN("init bin with meta fail", K(ret), K(meta));
  }

  ObString key;
  for (int i = 0; OB_SUCC(ret) && i < element_count; i++) {
    uint64_t key_offset = result.length() - start_pos;
    uint64_t key_len = 0;
    if (OB_FAIL(object->get_key(i, key))) {
      LOG_WARN("get key failed.", K(ret), K(i));
    } else if (OB_FALSE_IT(key_len = key.length())) {
    } else if (OB_FAIL(obj_bin.set_key_entry(i, key_offset, key_len, false))) {
      LOG_WARN("set_key_entry fail", K(ret), K(key));
    } else if (OB_FAIL(result.append(key))) {
      LOG_WARN("append key fail", K(ret), K(key));
    // result may realloc, so need ensure point same memory
    } else if (OB_FALSE_IT(obj_bin.set_current(result.string(), start_pos))) {
    }
  }


  for (int i = 0; OB_SUCC(ret) && i < element_count; i++) {
    ObJsonNode *value = nullptr;
    uint64_t value_offset = result.length() - start_pos;
    uint8_t value_type = 0;
    bool is_update_inline = false;
    if (OB_ISNULL(value = object->get_value(i))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("value is null", K(ret), K(i));
    } else if (OB_FAIL(obj_bin.try_update_inline(i, value, is_update_inline))) {
      LOG_WARN("try_update_inline fail", K(ret), K(i));
    } else if (is_update_inline) {
      LOG_DEBUG("try_update_inline success", K(i));
    } else if (OB_FALSE_IT(value_type = ObJsonVerType::get_json_vertype(value->json_type()))) {
    } else if (OB_FAIL(obj_bin.set_value_entry(i, value_offset, value_type, false))) {
      LOG_WARN("set_value_entry fail", K(ret), K(value_offset), K(value_type));
    } else if (OB_FAIL(serialize_json_value(value, result))) {
      LOG_WARN("serialize_json_value fail", K(ret));
    // result may realloc, so need ensure point same memory
    } else if (OB_FALSE_IT(obj_bin.set_current(result.string(), start_pos))) {
    }
  }

  // fill header obj size
  if (OB_SUCC(ret)) {
    uint64_t real_obj_size = static_cast<uint64_t>(result.length() - start_pos);
    if (ObJsonVar::get_var_type(real_obj_size) > ObJsonVar::get_var_type(obj_size)) {
      if (depth >= OB_JSON_BIN_MAX_SERIALIZE_TIME) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to calc object size", K(ret), K(real_obj_size), K(obj_size));
      } else {
        int64_t delta_size = real_obj_size - obj_size;
        object->set_serialize_delta_size(delta_size);
        result.set_length(start_pos);
        ret = serialize_json_object(object, result, depth + 1);
      }
    } else if (OB_FAIL(obj_bin.set_obj_size(real_obj_size))) {
      LOG_WARN("set_obj_size fail", K(ret));
    }
  }
  return ret;
}

int ObJsonBinSerializer::serialize_json_array(ObJsonArray *array, ObJsonBuffer &result, uint32_t depth)
{
  INIT_SUCC(ret);
  uint64_t element_count = array->element_count();
  const int64_t start_pos = result.length();
  ObJsonBin array_bin;
  ObJsonBinMeta meta;
  uint64_t array_size = array->get_serialize_size();
  meta.set_type(ObJsonBin::get_array_vertype(), false);
  meta.set_element_count(element_count);
  meta.set_element_count_var_type(ObJsonVar::get_var_type(element_count));
  meta.set_obj_size(array_size);
  meta.set_obj_size_var_type(ObJsonVar::get_var_type(array_size));
  meta.set_entry_var_type(meta.obj_size_var_type());
  meta.set_is_continuous(true);
  meta.calc_entry_array();

  if (OB_FAIL(meta.to_header(result))) {
    LOG_WARN("to obj header fail", K(ret));
  } else if (OB_FAIL(array_bin.reset(result.string(), start_pos, nullptr))) {
    LOG_WARN("init bin with meta fail", K(ret), K(meta));
  }

  for (int i = 0; OB_SUCC(ret) && i < element_count; i++) {
    ObJsonNode *value = nullptr;
    uint64_t value_offset = result.length() - start_pos;
    uint8_t value_type = 0;
    bool is_update_inline = false;
    if (OB_ISNULL(value = (*array)[i])) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("value is null", K(ret), K(i));
    } else if (OB_FAIL(array_bin.try_update_inline(i, value, is_update_inline))) {
      LOG_WARN("try_update_inline fail", K(ret), K(i));
    } else if (is_update_inline) {
      LOG_DEBUG("try_update_inline success", K(i));
    } else if (OB_FALSE_IT(value_type = ObJsonVerType::get_json_vertype(value->json_type()))) {
    } else if (OB_FAIL(array_bin.set_value_entry(i, value_offset, value_type, false))) {
      LOG_WARN("set_value_entry fail", K(ret), K(value_offset), K(value_type));
    } else if (OB_FAIL(serialize_json_value(value, result))) {
      LOG_WARN("serialize_json_value fail", K(ret));
    // result may realloc, so need ensure point same memory
    } else if (OB_FALSE_IT(array_bin.set_current(result.string(), start_pos))) {
    }
  }

  // fill header array size
  if (OB_SUCC(ret)) {
    uint64_t real_array_size = static_cast<uint64_t>(result.length() - start_pos);
    if (ObJsonVar::get_var_type(real_array_size) > ObJsonVar::get_var_type(array_size)) {
      if (depth >= OB_JSON_BIN_MAX_SERIALIZE_TIME) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to calc object size", K(ret), K(real_array_size), K(array_size));
      } else {
        int64_t delta_size = real_array_size - array_size;
        array->set_serialize_delta_size(delta_size);
        result.set_length(start_pos);
        ret = serialize_json_array(array, result, depth + 1);
      }
    } else if (OB_FAIL(array_bin.set_obj_size(real_array_size))) {
      LOG_WARN("set_obj_size fail", K(ret));
    }
  }
  return ret;
}

int ObJsonBinSerializer::serialize_json_integer(int64_t value, ObJsonBuffer &result)
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
int ObJsonBinSerializer::serialize_json_decimal(ObJsonDecimal *json_dec, ObJsonBuffer &result)
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

int ObJsonBinSerializer::serialize_json_value(ObJsonNode *json_tree, ObJsonBuffer &result)
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
      if (OB_FAIL(ObJsonBinSerializer::serialize_json_integer(value, result))) {
        LOG_WARN("failed to serialize json int", K(ret), K(value));
      }
      break;
    }
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      const ObJsonUint *i = static_cast<const ObJsonUint*>(json_tree);
      uint64_t value = i->value();
      if (OB_FAIL(ObJsonBinSerializer::serialize_json_integer(value, result))) {
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
      if (OB_FAIL(result.append(reinterpret_cast<const char*>(&vertype), sizeof(uint8_t)))) {
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
      ObJBVerType vertype = ObJsonBin::get_opaque_vertype();
      if (OB_FAIL(result.append(reinterpret_cast<const char*>(&vertype), sizeof(uint8_t)))) {
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

int ObJsonBin::try_update_inline(
    const int index,
    const ObJsonNode *value,
    bool &is_update_inline)
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  uint64_t inlined_val = 0;
  uint8_t inlined_type = 0;
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
  if (! is_update_inline) {
  } else if (OB_FAIL(set_value_entry(index, inlined_val, inlined_type | OB_JSON_TYPE_INLINE_MASK))) {
    LOG_WARN("set_value_entry for inline fail", K(ret), K(inlined_type), K(inlined_val), K(var_type));
  }
  return ret;
}
int ObJsonBin::try_update_inline(
    const int index,
    const ObJsonBin *value,
    bool &is_update_inline)
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  ObJsonNodeType j_type = value->json_type();
  uint64_t inlined_val;
  uint8_t inlined_type = 0;
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
      break;
    }
  }

  // set inline
  if (! is_update_inline) {
  } else if (OB_FAIL(set_value_entry(index, inlined_val, inlined_type | OB_JSON_TYPE_INLINE_MASK))) {
    LOG_WARN("set_value_entry for inline fail", K(ret), K(inlined_val), K(var_type));
  }
  return ret;
}

int ObJsonBinSerializer::serialize(ObJsonNode *json_tree, ObString &data)
{
  INIT_SUCC(ret);
  ObJsonBuffer result(allocator_);
  ObJsonNodeType root_type = json_tree->json_type();
  if (root_type == ObJsonNodeType::J_ARRAY || root_type == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(ObJsonBin::add_doc_header_v0(result))) {
      LOG_WARN("add_doc_header_v0 fail", K(ret));
    } else if (OB_FAIL(serialize_json_value(json_tree, result))) {
      LOG_WARN("serialize json tree fail", K(ret), K(root_type));
    } else if (OB_FAIL(ObJsonBin::set_doc_header_v0(result, result.length()))) {
      LOG_WARN("set_doc_header_v0 fail", K(ret));
    }
  } else {
    ObJBVerType ver_type = ObJsonVerType::get_json_vertype(root_type);
    if (!ObJsonVerType::is_opaque_or_string(ver_type) &&
        OB_FAIL(result.append(reinterpret_cast<const char*>(&ver_type), sizeof(uint8_t)))) {
      LOG_WARN("failed to serialize json tree at append used size", K(ret), K(result.length()));
    } else if (OB_FAIL(serialize_json_value(json_tree, result))) { // do recursion
      LOG_WARN("failed to serialize json tree at recursion", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    result.get_result_string(data);
  }
  return ret;
}

int ObJsonBin::parse_tree(ObJsonNode *json_tree)
{
  INIT_SUCC(ret);
  ObJsonBinSerializer serializer(allocator_);
  ObString data;
  if (nullptr != ctx_ && nullptr != ctx_->update_ctx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx not null, should not parse from tree", K(ret), KPC(ctx_));
  } else if (OB_FAIL(serializer.serialize(json_tree, data))) {
    LOG_WARN("serialize fail", K(ret));
  } else if (OB_FAIL(reset(data, 0, nullptr))) {
    LOG_WARN("set_current fail", K(ret));
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
  } else if (OB_FAIL(deserialize_json_value(json_tree))) {
    LOG_WARN("deserialize failed", K(ret), K(pos_), K(get_type()));
  }
  return ret;
}

int ObJsonBin::deserialize_json_value(ObJsonNode *&json_tree)
{
  INIT_SUCC(ret);
  bool is_inlined = OB_JSON_TYPE_IS_INLINE(get_type());
  ObJBVerType node_vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(get_type()));
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
      int64_t pos = pos_;
      if (OB_FAIL(cursor_->decode_i16(pos, &prec))) {
        LOG_WARN("fail to deserialize decimal precision.", K(ret), K(pos));
      } else if (OB_FAIL(cursor_->decode_i16(pos, &scale))) {
        LOG_WARN("fail to deserialize decimal scale.", K(ret), K(pos), K(prec));
      } else if (OB_FAIL(cursor_->deserialize(pos, &num))) {
        LOG_WARN("fail to deserialize number.", K(ret), K(pos));
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
          ObJsonInt *node = new(buf)ObJsonInt(ObJsonVar::var_uint2int(inline_value_, meta_.entry_size_));
          json_tree = static_cast<ObJsonNode*>(node);
        } else {
          int64_t val = 0;
          int64_t pos = pos_;
          if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
            LOG_WARN("fail to decode int val.", K(ret), K(pos));
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
          ObJsonUint *node = new(buf)ObJsonUint(uint_val_);
          json_tree = static_cast<ObJsonNode*>(node);
        } else {
          int64_t val = 0;
          int64_t pos = pos_;
          if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
            LOG_WARN("fail to decode uint val.", K(ret), K(pos));
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
      double val = 0;
      void *buf = allocator_->alloc(sizeof(ObJsonDouble));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for double json node", K(ret));
      } else if (OB_FAIL(cursor_->read_double(pos_, &val))) {
        LOG_WARN("read_double fail", K(ret), K(pos_), K(sizeof(double)));
        allocator_->free(buf);
      } else {
        if (node_type == ObJsonNodeType::J_DOUBLE) {
          json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonDouble(val));
        } else {
          json_tree = static_cast<ObJsonNode*>(new(buf)ObJsonODouble(val));
        }
      }
      break;
    }

    case ObJsonNodeType::J_OFLOAT: {
      float val = 0;
      void *buf = allocator_->alloc(sizeof(ObJsonOFloat));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for float json node", K(ret));
      } else if (OB_FAIL(cursor_->read_float(pos_, &val))) {
        LOG_WARN("read_float fail", K(ret), K(pos_), K(sizeof(float)));
        allocator_->free(buf);
      } else {
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
        int64_t pos = pos_;
        ObString str_data;
        if (node_vertype == ObJBVerType::J_STRING_V0) {
          pos += sizeof(uint8_t);
          if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
            LOG_WARN("decode str length failed.", K(ret), K(pos));
          } else {
            uint64_t str_length = static_cast<uint64_t>(val);
            if (str_length == 0) {
              LOG_DEBUG("empty string in json binary", K(str_length), K(pos));
              ObJsonString *empty_str_node = new(buf)ObJsonString(NULL, 0);
              json_tree = static_cast<ObJsonNode*>(empty_str_node);
            } else if (OB_FAIL(cursor_->get(pos, str_length, str_data))) {
              LOG_WARN("get str data fail", K(ret), K(pos), K(str_length));
            } else {
              void *str_buf = allocator_->alloc(str_length);
              if (str_buf == NULL) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc memory for data buf", K(ret), K(str_length));
              } else {
                MEMCPY(str_buf, str_data.ptr(), str_data.length());
                ObJsonString *node = new(buf)ObJsonString(reinterpret_cast<const char*>(str_buf), str_length);
                json_tree = static_cast<ObJsonNode*>(node);
              }
            }
          }
        } else {
          // other version process
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid ver type.", K(ret), K(node_vertype));
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
        ret = deserialize_json_object(node);
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
        ret = deserialize_json_array(node);
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
      bool val = false;
      void *buf = allocator_->alloc(sizeof(ObJsonBoolean));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for boolean json node", K(ret));
      } else {
        if (is_inlined) {
          val = static_cast<bool>(inline_value_);
        } else if (OB_FAIL(cursor_->read_bool(pos_, &val))) {
          LOG_WARN("read_bool fail", K(ret), K(pos_));
        }
        if (OB_SUCC(ret)) {
          ObJsonBoolean *node = new (buf) ObJsonBoolean(val);
          json_tree = static_cast<ObJsonNode*>(node);
        }
      }
      break;
    }
    case ObJsonNodeType::J_DATE: {
      int32_t value = 0;
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for date json node", K(ret));
      } else if (OB_FAIL(cursor_->read_i32(pos_, &value))) {
        LOG_WARN("read_i32 fail", K(ret), K(pos_), K(sizeof(int32_t)));
      } else {
        ObTime ob_time;
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
      int64_t value = 0;
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for time json node", K(ret));
      } else if (OB_FAIL(cursor_->read_i64(pos_, &value))) {
        LOG_WARN("read_i64 fail", K(ret), K(pos_), K(sizeof(int64_t)));
      } else {
        ObTime ob_time;
        ObTimeConverter::time_to_ob_time(value, ob_time);
        ObJsonDatetime *node = new(buf)ObJsonDatetime(node_type, ob_time);
        json_tree = static_cast<ObJsonNode*>(node);
      }
      break;
    }
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_ORACLEDATE: {
      int64_t value = 0;
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for datetime json node", K(ret));
      } else if (OB_FAIL(cursor_->read_i64(pos_, &value))) {
        LOG_WARN("read_i64 fail", K(ret), K(pos_), K(sizeof(int64_t)));
      } else {
        ObTime ob_time;
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
      int64_t value = 0;
      void *buf = allocator_->alloc(sizeof(ObJsonDatetime));
      if (buf == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for timestamp json node", K(ret));
      } else if (OB_FAIL(cursor_->read_i64(pos_, &value))) {
        LOG_WARN("read_i64 fail", K(ret), K(pos_), K(sizeof(int64_t)));
      } else {
        ObTime ob_time;
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
      if (node_vertype == ObJBVerType::J_OPAQUE_V0) {
        int64_t pos = pos_;
        ObString str_data;
        uint64_t need_len = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint8_t);
        ObObjType field_type = ObObjType::ObNullType;
        uint64_t val_length = 0;
        void *buf = allocator_->alloc(sizeof(ObJsonOpaque));
        if (buf == NULL) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for opaque json node", K(ret));
        } else if (OB_FAIL(cursor_->read_i16(pos + sizeof(uint8_t), reinterpret_cast<int16_t*>(&field_type)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read_i64 fail", K(ret), K(pos), K(need_len), K(val_length));
        } else if (OB_FAIL(cursor_->read_i64(pos + sizeof(uint8_t) + sizeof(uint16_t), reinterpret_cast<int64_t*>(&val_length)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read_i64 fail", K(ret), K(pos), K(need_len), K(val_length));
        } else if (val_length == 0) {
          LOG_DEBUG("empty opaque in json binary", K(val_length), K(field_type), K(pos));
          ObString empty_value(0, NULL);
          ObJsonOpaque *empy_opa_node = new(buf)ObJsonOpaque(empty_value, field_type);
          json_tree = static_cast<ObJsonNode*>(empy_opa_node);
        } else {
          if (OB_FAIL(cursor_->get(pos + sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint64_t), val_length, str_data))) {
            LOG_WARN("get data fail", K(ret), K(pos), K(need_len), K(val_length));
          } else {
            void *str_buf = allocator_->alloc(val_length);
            if (str_buf == NULL) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for data buf", K(ret), K(val_length));
            } else {
              MEMCPY(str_buf, str_data.ptr(), str_data.length());
              ObString value(val_length, reinterpret_cast<const char*>(str_buf));
              ObJsonOpaque *node = new(buf)ObJsonOpaque(value, field_type);
              json_tree = static_cast<ObJsonNode*>(node);
            }
          }
        }
      } else {
        // other version process
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid ver type.", K(ret), K(node_vertype));
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
        int64_t pos = pos_;
        ObString str_data;
        pos += sizeof(uint8_t);
        if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
          LOG_WARN("decode str length failed.", K(ret));
        } else if (OB_FAIL(cursor_->get(pos, val, str_data))) {
          LOG_WARN("get data fail", K(ret), K(pos), K(val));
        } else {
          uint64_t str_length = static_cast<uint64_t>(val);
          if (str_length == 0) {
            LOG_DEBUG("empty string in json binary", K(str_length), K(pos));
            ObJsonString *empty_str_node = new(buf)ObJsonString(NULL, 0);
            json_tree = static_cast<ObJsonNode*>(empty_str_node);
          } else {
            void *str_buf = allocator_->alloc(str_length);
            if (str_buf == NULL) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for data buf", K(ret), K(str_length));
            } else {
              MEMCPY(str_buf, str_data.ptr(), str_data.length());
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
        int64_t pos = pos_;
        ObString str_data;
        pos += sizeof(uint8_t);
        if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
          LOG_WARN("decode str length failed.", K(ret));
        } else if (OB_FAIL(cursor_->get(pos, val, str_data))) {
          LOG_WARN("get data fail", K(ret), K(pos), K(val));
        } else {
          uint64_t str_length = static_cast<uint64_t>(val);
          if (str_length == 0) {
            LOG_DEBUG("empty string in json binary", K(str_length), K(pos));
            ObJsonString *empty_str_node = new(buf)ObJsonString(NULL, 0);
            json_tree = static_cast<ObJsonNode*>(empty_str_node);
          } else {
            void *str_buf = allocator_->alloc(str_length);
            if (str_buf == NULL) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for data buf", K(ret), K(str_length));
            } else {
              MEMCPY(str_buf, str_data.ptr(), str_data.length());
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

  if (OB_SUCC(ret) && OB_NOT_NULL(json_tree)) {
    json_tree->set_allocator(allocator_);
  }
  return ret;
}

int ObJsonBin::deserialize_json_object_v0(ObJsonObject *object)
{
  INIT_SUCC(ret);
  uint64_t element_count = this->element_count();
  object->set_serialize_size(obj_size());
  ObJsonBin child_bin(allocator_);
  for (uint64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
    ObJsonNode *node = nullptr;
    ObString ori_key;
    ObString key;
    if (OB_FAIL(get_key_in_object(i, ori_key))) {
      LOG_WARN("get_key_in_object fail", K(ret), K(i));
    } else if (OB_FAIL(ob_write_string(*allocator_, ori_key, key))) {
      LOG_WARN("ob_write_string fail", K(ret), K(i), K(ori_key));
    } else if (OB_FAIL(get_value(i, child_bin))) {
      LOG_WARN("get child value fail", K(ret));
    } else if (OB_FAIL(child_bin.deserialize_json_value(node))) {
      LOG_WARN("deserialize child node fail", K(ret), K(i), K(child_bin));
    } else if (OB_FAIL(object->add(key, node, false, true, false, is_schema_))) {
      LOG_WARN("add node to obj fail", K(ret), K(i));
    }
  }
  return ret;
}

int ObJsonBin::deserialize_json_object(ObJsonObject *object)
{
  INIT_SUCC(ret);
  ObJBVerType vertype = get_vertype();
  switch(vertype) {
    case ObJBVerType::J_OBJECT_V0: {
      ret = deserialize_json_object_v0(object);
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

int ObJsonBin::deserialize_json_array_v0(ObJsonArray *array)
{
  INIT_SUCC(ret);
  uint64_t element_count = this->element_count();
  array->set_serialize_size(this->obj_size());
  ObJsonBin child_bin(allocator_);
  for (uint64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
    ObJsonNode *node = nullptr;
    if (OB_FAIL(get_value(i, child_bin))) {
      LOG_WARN("get_value fail", K(ret), K(i));
    } else if (OB_FAIL(child_bin.deserialize_json_value(node))) {
      LOG_WARN("failed to deserialize child node", K(ret), K(i), K(child_bin));
    } else if (OB_FAIL(array->append(node))) {
      LOG_WARN("failed to append node to array", K(ret), K(i));
    }
  }
  return ret;
}

int ObJsonBin::deserialize_json_array(ObJsonArray *array)
{
  INIT_SUCC(ret);
  ObJBVerType vertype = get_vertype();
  switch(vertype) {
    case ObJBVerType::J_ARRAY_V0: {
      ret = deserialize_json_array_v0(array);
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

// get need size when rebuild or serialize from tree for un-inline node
int ObJsonBin::get_area_size(uint64_t& size) const
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = json_type();
  if (OB_JSON_TYPE_IS_INLINE(get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inline type can not call this", K(ret), K(get_type()));
  } else {
    switch (node_type) {
      case ObJsonNodeType::J_NULL: {
        size = 1;
        break;
      }
      // [precision(int16_t)][scale(int16_t)][value]
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        size = meta_.bytes_;
        break;
      }
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        size = meta_.bytes_;
        break;
      }
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG:  {
        size = meta_.bytes_;
        break;
      }
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        size = sizeof(double);
        break;
      }
      case ObJsonNodeType::J_OFLOAT: {
        size = sizeof(float);
        break;
      }
      // string type : [vertype(uint8_t)][length(var uint64_t)][data]
      // length is var_size encoding
      // element_count_ store data length
      case ObJsonNodeType::J_OBINARY:
      case ObJsonNodeType::J_OOID:
      case ObJsonNodeType::J_ORAWHEX:
      case ObJsonNodeType::J_ORAWID:
      case ObJsonNodeType::J_ODAYSECOND:
      case ObJsonNodeType::J_OYEARMONTH:
      case ObJsonNodeType::J_STRING: {
        uint64_t str_len = get_element_count();
        size = OB_JSON_BIN_VALUE_TYPE_LEN + serialization::encoded_length_vi64(str_len) + str_len;
        break;
      }
      case ObJsonNodeType::J_OBJECT:
      case ObJsonNodeType::J_ARRAY: {
        size = obj_size();
        break;
      }
      case ObJsonNodeType::J_BOOLEAN: {
        size = sizeof(bool);
        break;
      }
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_ORACLEDATE: {
        size = sizeof(int32_t);
        break;
      }
      case ObJsonNodeType::J_TIME:
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_TIMESTAMP:
      case ObJsonNodeType::J_ODATE:
      case ObJsonNodeType::J_OTIMESTAMP:
      case ObJsonNodeType::J_OTIMESTAMPTZ: {
        size = sizeof(int64_t);
        break;
      }
      // opaque type : [vertype(uint8_t)][ObObjType(uint16_t)][length(uint64_t)][data]
      // length is fix_size encoding
      // element_count_ store data length
      case ObJsonNodeType::J_OPAQUE: {
        uint64_t str_len = get_element_count();
        size = OB_JSON_BIN_VALUE_TYPE_LEN + sizeof(uint16_t) + sizeof(uint64_t) + str_len;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid node type", K(ret), K(node_type), K(get_type()));
        break;
      }
    }
  }
  return ret;
}

int ObJsonBin::raw_binary_at_iter(ObString &buf) const
{
  INIT_SUCC(ret);
  uint8_t type = OB_JSON_TYPE_GET_INLINE(get_type());
  ObJBVerType vertype = static_cast<ObJBVerType>(type);
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
  ObJsonBuffer result(allocator_);
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("alloctor is null.", K(ret));
  } else if (! (node_type == ObJsonNodeType::J_ARRAY || node_type == ObJsonNodeType::J_OBJECT || ObJsonVerType::is_opaque_or_string(vertype))
      && OB_FAIL(result.append(reinterpret_cast<const char*>(&type), sizeof(uint8_t)))) {
    LOG_WARN("failed to serialize jsonbin append type_", K(ret));
  } else if (OB_FAIL(rebuild_json_value(result))) {
    LOG_WARN("failed to rebuild inline value", K(ret));
  } else {
    // need ensure memory not release
    result.get_result_string(buf);
  }
  return ret;
}

// ObJsonBin may be not continous when partial update
// calling get_serialize_size get need size when rebuilding,
int ObJsonBin::get_serialize_size(uint64_t &size) const
{
  INIT_SUCC(ret);
  uint64_t real_size = 0;
  ObJBVerType vertype = get_vertype();
  if ((ObJBVerType::J_ARRAY_V0 == vertype || ObJBVerType::J_OBJECT_V0 == vertype)) {
    bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;
    uint64_t element_count = this->element_count();
    int64_t total_child_size = 0;
    ObJsonBin child;
    for (uint64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
      uint64_t key_offset = 0;
      uint64_t key_len = 0;
      uint64_t child_size = 0;
      if (ObJBVerType::J_OBJECT_V0 == vertype && OB_FAIL(get_key_entry(i, key_offset, key_len))) {
        LOG_WARN("get_key_entry fail", K(ret), K(i));
      } else if (OB_FAIL(get_value(i, child))) {
        LOG_WARN("get_value fail", K(ret), K(i));
      } else if (! child.is_inline_vertype() && OB_FAIL(child.get_serialize_size(child_size))) {
        LOG_WARN("get child value size fail", K(ret), K(i));
      } else {
        total_child_size += key_len + child_size;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (obj_size_var_type() != entry_var_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("var type invalid", K(ret), K(meta_));
    } else {
      uint8_t old_entry_var_type = entry_var_type();
      real_size = OB_JSON_BIN_HEADER_LEN
          + element_count_var_size()
          + entry_var_size()
          + (is_obj_type ? element_count * (entry_var_size() * 2) : 0)
          + element_count * (entry_var_size() + OB_JSON_BIN_VALUE_TYPE_LEN)
          + total_child_size;
      // currently entry var_type is not support decrease when rebuild
      // so use max in old and new var_type
      uint8_t new_entry_var_type = OB_MAX(old_entry_var_type, ObJsonVar::get_var_type(real_size));
      if (OB_FAIL(extend_entry_var_type(is_obj_type, element_count, real_size, old_entry_var_type, new_entry_var_type, real_size))) {
        LOG_WARN("extend_entry_var_type fail", K(ret), K(meta_), K(is_obj_type), K(element_count), K(real_size), K(old_entry_var_type), K(new_entry_var_type));
      }
    }
  } else if (OB_FAIL(get_area_size(real_size))) {
    LOG_WARN("get_area_size fail", K(ret), K(meta_));
  }

  if (OB_SUCC(ret)) {
    size = real_size;
  }
  return ret;
}

// if var type not enough, need extend var type
int ObJsonBin::extend_entry_var_type(
  const bool is_obj_type,
  const uint64_t element_count,
  const uint64_t old_size,
  uint8_t old_entry_var_type,
  uint8_t &new_entry_var_type,
  uint64_t &new_size) const
{
  INIT_SUCC(ret);
  new_entry_var_type = ObJsonVar::get_var_type(old_size);
  new_size = old_size;
  static const int64_t OB_JSONBIN_CONVERGEN_TIME = 3;
  for (int i = 0; i < OB_JSONBIN_CONVERGEN_TIME && new_entry_var_type > old_entry_var_type; ++i) {
    uint8_t entry_var_size_inc = ObJsonVar::get_var_size(new_entry_var_type) - ObJsonVar::get_var_size(old_entry_var_type);
    // plus for obj_size
    new_size += entry_var_size_inc;
    // puus for value entry
    new_size += element_count * entry_var_size_inc;
    // plus key entry if is object
    new_size += (is_obj_type ? element_count * (entry_var_size_inc * 2) : 0);
    old_entry_var_type = new_entry_var_type;
    new_entry_var_type = ObJsonVar::get_var_type(new_size);
  }
  return ret;
}

int ObJsonBin::get_used_bytes(uint64_t &size) const
{
  INIT_SUCC(ret);
  if (! is_at_root()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should call at root", K(ret), K(pos_), K(node_stack_.size()));
  } else {
    size = cursor_->get_length();
  }
  return ret;
}

int ObJsonBin::get_value_binary(ObString &out) const
{
  INIT_SUCC(ret);
  uint64_t area_size = 0;
  uint64_t total_len = cursor_->get_length();
  // must no extend segment
  if (OB_NOT_NULL(ctx_) && ctx_->extend_seg_offset_ != 0 && ctx_->extend_seg_offset_ != total_len) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret), K(pos_), K(total_len), KPC(this));
  } else if (OB_FAIL(get_area_size(area_size))) {
    LOG_WARN("get_area_size", K(ret), K(pos_), KPC(this));
  } else if (OB_FAIL(cursor_->get(pos_, area_size, out))) {
    LOG_WARN("cursor get_data fail", K(ret), K(pos_), K(area_size), KPC(this));
  }
  return ret;
}

int ObJsonBin::get_raw_binary(ObString &buf, ObIAllocator *allocator) const
{
  INIT_SUCC(ret);
  ObIAllocator * allocator_ptr = (allocator == NULL) ? allocator_ : allocator;
  uint8_t type = OB_JSON_TYPE_GET_INLINE(get_type()); // dst type take off inline mask
  ObJBVerType vertype = static_cast<ObJBVerType>(type);
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
  ObJsonBuffer result(allocator_ptr);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  bool is_rebuild = true;
  if (node_type == ObJsonNodeType::J_ARRAY || node_type == ObJsonNodeType::J_OBJECT) {
    if (nullptr == update_ctx && is_at_root()) {
      if (OB_FAIL(cursor_->get_data(buf))) {
        LOG_WARN("get_data fail", K(ret));
      } else {
        is_rebuild = false;
      }
    } else if (OB_FAIL(add_doc_header_v0(result))) {
      LOG_WARN("add_doc_header_v0 fail", K(ret));
    } else if (OB_FAIL(rebuild_json_value(result))) {
      LOG_WARN("failed to rebuild inline value", K(ret));
    } else if (OB_FAIL(set_doc_header_v0(result, result.length()))) {
      LOG_WARN("set_doc_header_v0 fail", K(ret));
    }
  } else {
    // for scalar type, need add type byte except string type
    // so have to use ObStringBuffer
    if (!ObJsonVerType::is_opaque_or_string(vertype) &&
        OB_FAIL(result.append(reinterpret_cast<const char*>(&vertype), sizeof(uint8_t)))) {
      LOG_WARN("failed to serialize json tree at append used size", K(ret), K(result.length()));
    } else if (OB_FAIL(rebuild_json_value(result))) {
      LOG_WARN("failed to rebuild inline value", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_rebuild) {
    // need ensure memory not release
    result.get_result_string(buf);
  }
  return ret;
}

int ObJsonBin::get_raw_binary_v0(ObString &buf, ObIAllocator *allocator) const
{
  INIT_SUCC(ret);
  ObIAllocator * allocator_ptr = (allocator == NULL) ? allocator_ : allocator;
  uint8_t type = OB_JSON_TYPE_GET_INLINE(get_type()); // dst type take off inline mask
  ObJBVerType vertype = static_cast<ObJBVerType>(type);
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
  ObJsonBuffer result(allocator_ptr);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  bool is_rebuild = true;
  if (node_type == ObJsonNodeType::J_ARRAY || node_type == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(cursor_->get_data(buf))) {
      LOG_WARN("cursor get_data fail", K(ret));
    } else {
      buf.assign_ptr(buf.ptr() + pos_, buf.length() - pos_);
      is_rebuild = false;
    }
  } else {
    if (!ObJsonVerType::is_opaque_or_string(vertype) &&
        OB_FAIL(result.append(reinterpret_cast<const char*>(&vertype), sizeof(uint8_t)))) {
      LOG_WARN("failed to serialize json tree at append used size", K(ret), K(result.length()));
    } else if (OB_FAIL(rebuild_json_value(result))) {
      LOG_WARN("failed to rebuild inline value", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_rebuild) {
    // need ensure memory not release
    result.get_result_string(buf);
  }
  return ret;
}

int ObJsonBin::get_free_space(size_t &space) const
{
  INIT_SUCC(ret);
  uint64_t actual_size = cursor_->get_length();
  uint64_t used_size = 0;
  if (0 == actual_size) {
    space = 0;
  } else if (! is_at_root()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should call at root", K(ret), K(pos_), K(node_stack_.size()));
  } else if (OB_FAIL(get_serialize_size(used_size))) {
    LOG_WARN("get_serialize_size fail", K(ret));
  } else if (used_size > actual_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length invalid.", K(ret), K(actual_size), K(used_size));
  } else {
    space = actual_size - (used_size + pos_);
  }
  return ret;
}

int ObJsonBin::init_cursor(const ObString& data)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(ctx_) && (OB_NOT_NULL(ctx_->update_ctx_))) {
    if (! data.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data should empty", K(ret), KPC(ctx_), K(data));
    } else  if (OB_ISNULL(ctx_->update_ctx_->cursor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update_ctx cursor is null or data not empty", K(ret), KPC(ctx_), KPC(ctx_->update_ctx_), K(data));
    } else {
      cursor_ = ctx_->update_ctx_->cursor_;
    }
  } else if (OB_FAIL(local_cursor_.init(data))) {
    cursor_ = &local_cursor_;
  }
  return ret;
}

// reset iter to root
int ObJsonBin::reset_iter()
{
  node_stack_.reset();
  return reset(0);
}

int ObJsonBin::reset(const uint8_t type, const int64_t offset, const uint8_t value_entry_var_type)
{
  INIT_SUCC(ret);
  if (OB_FAIL(reset(type, local_cursor_.data(), offset, value_entry_var_type, ctx_))) {
    LOG_WARN("reset fail", K(ret), K(type), K(offset), K(value_entry_var_type));
  }
  return ret;
}

int ObJsonBin::reset(const int64_t offset)
{
  INIT_SUCC(ret);
  if (OB_FAIL(reset(local_cursor_.data(), offset, ctx_))) {
    LOG_WARN("reset fail", K(ret), K(offset));
  }
  return ret;
}

int ObJsonBin::reset_child(ObJsonBin &child, const int64_t child_offset) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(child.reset(local_cursor_.data(), child_offset, ctx_))) {
    LOG_WARN("reset fail", K(ret), K(child_offset));
  }
  return ret;
}

int ObJsonBin::reset_child(
    ObJsonBin &child,
    const uint8_t child_type,
    const int64_t child_offset,
    const uint8_t value_entry_var_type) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(child.reset(child_type, local_cursor_.data(), child_offset, value_entry_var_type, ctx_))) {
    LOG_WARN("reset fail", K(ret), K(child_type), K(child_offset), K(value_entry_var_type));
  }
  return ret;
}

// before this called, need set cursor
int ObJsonBin::parse_type_()
{
  return cursor_->read_i8(pos_, reinterpret_cast<int8_t*>(&meta_.type_));
}

int ObJsonBin::parse_doc_header_()
{
  INIT_SUCC(ret);
  if (OB_FAIL(parse_type_())) {
    LOG_WARN("parse_type fail", K(ret));
  } else if (is_doc_header_v0(meta_.type_)) {
    if (OB_FAIL(init_ctx())) {
      LOG_WARN("init_ctx v0 fail", K(ret));
    }
  }
  return ret;
}

int ObJsonBin::skip_type_byte_()
{
  INIT_SUCC(ret);
  ObJBVerType vertype = get_vertype();
  if (ObJsonVerType::is_array(vertype)
      || ObJsonVerType::is_object(vertype)
      || ObJsonVerType::is_opaque_or_string(vertype)) {
  } else {
    pos_ += sizeof(uint8_t);
  }
  return ret;
}

bool ObJsonBin::is_empty_data() const
{
  return nullptr == cursor_ || cursor_->get_length() <= 0;
}

int ObJsonBin::reset(const ObString &buffer, int64_t offset, ObJsonBinCtx *ctx)
{
  INIT_SUCC(ret);
  pos_ = offset;
  ctx_ = ctx;
  meta_.reset();  // notice : this clear meta_, all set meta info should after this

  if (OB_FAIL(init_cursor(buffer))) {
    LOG_WARN("init_cursor fail", K(ret));
  } else if (is_empty_data()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf or len is empty", K(ret), K(offset), K(buffer));
  } else if (OB_FAIL(parse_doc_header_())) {
    LOG_WARN("parse_doc_header_ fail", K(ret));
  } else if (OB_FAIL(parse_type_())) {
    LOG_WARN("parse_type_ fail", K(ret));
  } else if (OB_FAIL(skip_type_byte_())) {
    LOG_WARN("skip_type_byte_ fail", K(ret));
  } else if (OB_FAIL(init_bin_data())) {
    LOG_WARN("init_bin_data fail", K(ret), K(pos_), K(meta_.type_));
  }
  return ret;
}

int ObJsonBin::reset(
    const uint8_t type,
    const ObString &buffer,
    const int64_t offset,
    const uint8_t value_entry_var_type,
    ObJsonBinCtx *ctx)
{
  INIT_SUCC(ret);
  pos_ = offset;
  ctx_ = ctx;
  meta_.reset();  // notice : this clear meta_, all set meta info should after this
  meta_.type_ = type;
  meta_.entry_size_ = value_entry_var_type;

  if (OB_FAIL(init_cursor(buffer))) {
    LOG_WARN("init_cursor fail", K(ret));
  } else if (is_empty_data()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf or len is empty", K(ret), K(type), K(offset), K(value_entry_var_type), K(buffer));
  } else if (OB_FAIL(init_bin_data())) {
    LOG_WARN("falied to set root obj", K(ret), K(pos_), K(type));
  } else if (meta_.type_ != type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type not match parsed type", K(ret), K(type), K(meta_));
  }
  return ret;
}


int ObJsonBin::init_ctx()
{
  INIT_SUCC(ret);
  if (nullptr == ctx_) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret), K(ctx_));
    } else if (OB_ISNULL(ctx_ = OB_NEWx(ObJsonBinCtx, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctx fail", K(ret), K(sizeof(ObJsonBinCtx)));
    } else {
      is_alloc_ctx_ = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_doc_header_v0())) {
    LOG_WARN("parse doc header fail", K(ret));
  }
  return ret;
}

// move iter to its parent
int ObJsonBin::move_parent_iter()
{
  INIT_SUCC(ret);
  ObJBNodeMeta curr_parent;
  if (OB_FAIL(node_stack_.back(curr_parent, true))) {
    LOG_WARN("fail to pop back from parent", K(ret), K(node_stack_.size()));
  } else if (OB_FAIL(reset(curr_parent.offset_))) {
    LOG_WARN("failed to move iter to parent", K(ret), K(pos_), K(curr_parent));
  }
  return ret;
}

int ObJsonBin::get_parent(ObIJsonBase *& parent) const
{
  INIT_SUCC(ret);
  ObJsonBin *parent_bin = nullptr;
  // if not root, parent stack should not be null
  // Otherwise, cann't get the correct return value by get_parent()
  if (!is_at_root() && node_stack_.size() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("non-root node, but node_stack is empty", K(ret), K(meta_), K(pos_), KPC(this));
  } else if (node_stack_.size() <= 0) {
  } else if (OB_FAIL(create_new_binary(nullptr, parent_bin))) {
    LOG_WARN("create_new_binary fail", K(ret));
  } else if (OB_FAIL(parent_bin->node_stack_.copy(this->node_stack_))) {
    LOG_WARN("copy node stack fail", K(ret));
  } else if (OB_FAIL(parent_bin->move_parent_iter())) {
    LOG_WARN("move parent fail", K(ret));
  } else {
    parent = parent_bin;
  }
  return ret;
}


int ObJsonBin::init_string_node_v0()
{
  INIT_SUCC(ret);
  int64_t str_len = 0;
  int64_t offset = pos_ + sizeof(uint8_t);
  ObString data;
  if (OB_FAIL(cursor_->decode_vi64(offset, &str_len))) {
    LOG_WARN("decode string length fail", K(ret), K(pos_), K(offset), K(str_len));
  } else if (OB_FAIL(cursor_->get(offset, str_len, data))) {
    LOG_WARN("get string data fail", K(ret), K(pos_), K(offset), K(str_len));
  } else {
    meta_.set_element_count(static_cast<uint64_t>(str_len));
    meta_.bytes_ = offset - pos_ + str_len;
    meta_.str_data_offset_ = offset - pos_;
    data_ = data.ptr();
  }
  return ret;
}

int ObJsonBin::init_string_node()
{
  INIT_SUCC(ret);
  if (ObJBVerType::J_STRING_V0 == get_vertype()) {
    ret = init_string_node_v0();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse string type invlaid vertype.", K(ret), K(meta_));
  }
  return ret;
}

int ObJsonBin::init_opaque_node_v0()
{
  INIT_SUCC(ret);
  int64_t offset = sizeof(uint8_t);
  int64_t str_len = 0;
  ObString data;
  // [vertype(uint8_t)][ObObjType(uint16_t)][length(uint64_t)][data]
  if (OB_FAIL(cursor_->read_i16(pos_ + offset, reinterpret_cast<int16_t*>(&meta_.field_type_)))) {
    LOG_WARN("read_u16 fail", K(ret), K(pos_), K(offset), K(str_len));
  } else if (OB_FALSE_IT(offset += sizeof(uint16_t))) {
  } else if (OB_FAIL(cursor_->read_i64(pos_ + offset, &str_len))) {
    LOG_WARN("read_u16 fail", K(ret), K(pos_), K(offset), K(str_len));
  } else if (OB_FALSE_IT(offset += sizeof(uint64_t))) {
  } else if (OB_FAIL(cursor_->get(pos_ + offset, str_len, data))) {
    LOG_WARN("get data fail", K(ret), K(pos_), K(offset), K(str_len));
  } else {
    meta_.set_element_count(str_len);
    meta_.bytes_ = offset + str_len;
    meta_.str_data_offset_ = offset;
    data_ = data.ptr();
  }
  return ret;
}

int ObJsonBin::init_opaque_node()
{
  INIT_SUCC(ret);
  if (ObJBVerType::J_OPAQUE_V0 == get_vertype()) {
    ret = init_opaque_node_v0();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse string type invlaid vertype.", K(ret), K(meta_));
  }
  return ret;
}

// inlined will reuse value entry offset which length is type_size
int ObJsonBin::init_bin_data()
{
  INIT_SUCC(ret);
  meta_.set_element_count(1); // scalar is 1, container is acutual k-v pairs number
  bool is_inlined = is_inline_vertype();
  {
    ObJsonNodeType node_type = json_type();
    switch (node_type) {
      case ObJsonNodeType::J_NULL: {
        meta_.bytes_ = is_inlined ? 0 : 1;
        break;
      }
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_ODECIMAL: {
        ObPrecision prec = -1;
        ObScale scale = -1;
        int64_t pos = pos_;
        if (OB_FAIL(cursor_->decode_i16(pos, &prec))) {
          LOG_WARN("fail to deserialize decimal precision.", K(ret), K(pos));
        } else if (OB_FAIL(cursor_->decode_i16(pos, &scale))) {
          LOG_WARN("fail to deserialize decimal scale.", K(ret), K(pos), K(pos));
        } else if (OB_FAIL(cursor_->deserialize(pos, &number_))) {
          LOG_WARN("failed to deserialize decimal data", K(ret), K(pos), K(pos));
        } else {
          prec_ = prec;
          scale_ = scale;
          meta_.bytes_ = pos - pos_;
        }
        break;
      }
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_OINT: {
        if (is_inlined) {
          uint64_t inline_val = 0;
          if (OB_FAIL(ObJsonVar::read_var(cursor_, pos_, meta_.entry_size_, reinterpret_cast<int64_t*>(&inline_val)))) {
            LOG_WARN("read inline value fail", K(ret), K(meta_));
          } else {
            int_val_ = ObJsonVar::var_uint2int(inline_val, meta_.entry_size_);
            meta_.bytes_ = 0;
          }
        } else {
          int64_t val = 0;
          int64_t pos = pos_;
          if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
            LOG_WARN("decode int val failed.", K(ret));
          } else {
            int_val_ = val;
            meta_.bytes_ = pos - pos_;
          }
        }
        break;
      }
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_OLONG:  {
        if (is_inlined) {
          if (OB_FAIL(ObJsonVar::read_var(cursor_, pos_, meta_.entry_size_, &uint_val_))) {
            LOG_WARN("read inline value fail", K(ret), K(meta_));
          } else {
            meta_.bytes_ = 0;
          }
        } else {
          int64_t val = 0;
          int64_t pos = pos_;
          if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
            LOG_WARN("decode uint val failed.", K(ret));
          } else {
            uint_val_ = static_cast<uint64_t>(val);
            meta_.bytes_ = pos - pos_;
          }
        }
        break;
      }
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_ODOUBLE: {
        if (OB_FAIL(cursor_->read_double(pos_, &double_val_))) {
          LOG_WARN("read_double fail", K(ret));
        } else {
          meta_.bytes_ = sizeof(double);
        }
        break;
      }
      case ObJsonNodeType::J_OFLOAT: {
        if (OB_FAIL(cursor_->read_float(pos_, &float_val_))) {
          LOG_WARN("read_float fail", K(ret));
        } else {
          meta_.bytes_ = sizeof(float);
        }
        break;
      }
      case ObJsonNodeType::J_STRING: {
        if (OB_FAIL(init_string_node())) {
          LOG_WARN("init_string_node fail", K(ret));
        }
        break;
      }
      case ObJsonNodeType::J_OBJECT:
      case ObJsonNodeType::J_ARRAY: {
        if (OB_FAIL(init_meta())) {
          LOG_WARN("init meta fail", K(ret));
        } else {
          meta_.bytes_ = meta_.obj_size();
        }
        break;
      }
      case ObJsonNodeType::J_BOOLEAN: {
        if (is_inlined) {
          if (OB_FAIL(ObJsonVar::read_var(cursor_, pos_, meta_.entry_size_, &uint_val_))) {
            LOG_WARN("read inline value fail", K(ret), K(meta_));
          } else {
            meta_.bytes_ = 0;
          }
        } else {
          bool val = false;
          if (OB_FAIL(cursor_->read_bool(pos_, &val))) {
            LOG_WARN("read_float fail", K(ret));
          } else {
            meta_.bytes_ = sizeof(bool);
            uint_val_ = val;
          }
        }
        break;
      }
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_ORACLEDATE: {
        int32_t val = 0;
        if (OB_FAIL(cursor_->read_i32(pos_, &val))) {
          LOG_WARN("read_id32 fail", K(ret), K(pos_));
        } else {
          meta_.field_type_ = ObDateType;
          int_val_ = val;
          meta_.bytes_ = sizeof(int32_t);
        }
        break;
      }
      case ObJsonNodeType::J_TIME: {
        if (OB_FAIL(cursor_->read_i64(pos_, &int_val_))) {
          LOG_WARN("read_id32 fail", K(ret), K(pos_));
        } else {
          meta_.field_type_ = ObTimeType;
          meta_.bytes_ = sizeof(int64_t);
        }
        break;
      }
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_ODATE:
      case ObJsonNodeType::J_OTIMESTAMP:
      case ObJsonNodeType::J_OTIMESTAMPTZ: {
        if (OB_FAIL(cursor_->read_i64(pos_, &int_val_))) {
          LOG_WARN("read_id32 fail", K(ret), K(pos_));
        } else {
          meta_.field_type_ = ObJsonBaseUtil::get_time_type(node_type);
          meta_.bytes_ = sizeof(int64_t);
        }
        break;
      }
      case ObJsonNodeType::J_TIMESTAMP: {
        if (OB_FAIL(cursor_->read_i64(pos_, &int_val_))) {
          LOG_WARN("read_id32 fail", K(ret), K(pos_));
        } else {
          meta_.field_type_ = ObTimestampType;
          meta_.bytes_ = sizeof(int64_t);
        }
        break;
      }
      case ObJsonNodeType::J_OPAQUE: {
        if (OB_FAIL(init_opaque_node())) {
          LOG_WARN("init_opaque_node fail", K(ret));
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
  } else if (index >= get_element_count()) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range.", K(ret), K(index), K(get_element_count()));
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
  int64_t high = get_element_count() - 1;
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


// find first position that greater than key
int ObJsonBin::lookup_insert_postion(const ObString &key, size_t &idx) const
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  if (node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(node_type));
  }

  ObJsonKeyCompare comparator;
  ObString key_iter;
  int64_t low = 0;
  int64_t high = get_element_count() - 1;
  // do binary search, find last key that less or equal to key
  while (OB_SUCC(ret) && low <= high) {
    int64_t mid = low + (high - low) / 2;
    if (OB_FAIL(get_key_in_object(mid, key_iter))) {
      LOG_WARN("fail to get key.", K(ret), K(mid), K(low), K(high));
    } else {
      int compare_result = comparator.compare(key_iter, key);
      if (compare_result > 0) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
  }
  // low is the lower_bound, +1 is upper_bound
  if (OB_SUCC(ret)) {
    idx = low;
  }
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

int ObJsonBin::get_element_v0(size_t index, uint64_t *get_addr_only)
{
  INIT_SUCC(ret);
  uint64_t offset = pos_;
  uint64_t value_offset = 0;
  uint8_t value_type = 0;
  uint64_t value_entry_offset = get_value_entry_offset(index);

  if (OB_FAIL(get_value_entry(index, value_offset, value_type))) {
    LOG_WARN("get_value_entry fail", K(index));
  } else if (OB_JSON_TYPE_IS_INLINE(value_type)) {
    offset = pos_ + value_entry_offset;
  } else if (is_forward_v0(value_type)) {
    offset = get_extend_value_offset(value_offset);
    if (OB_FAIL(get_extend_value_type(offset, value_type))) {
      LOG_WARN("get_extend_value_type fail", K(ret), K(index), K(value_offset));
    } else if (! need_type_prefix(value_type)) {
      offset += sizeof(uint8_t);
    }
  } else {
    offset = pos_ + value_offset;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(get_addr_only)) {
    *get_addr_only = offset;
  } else if (OB_NOT_NULL(allocator_) && !is_seek_only_
      && OB_FAIL(node_stack_.push(ObJBNodeMeta(get_type(), obj_size_var_type(), entry_var_type(), index, pos_, obj_size())))) {
    LOG_WARN("failed to push parent pos.", K(ret), K(pos_), K(node_stack_.size()));
  } else if (OB_FAIL(reset(value_type, offset, entry_var_type()))) {
    LOG_WARN("failed to move iter to sub obj.", K(ret), K(index), K(offset), K(value_type));
  }
  return ret;
}

int ObJsonBin::get_element_in_array(size_t index, uint64_t *get_addr_only)
{
  INIT_SUCC(ret);
  ObJBVerType vertype = get_vertype();
  switch (vertype)
  {
    case ObJBVerType::J_ARRAY_V0:
    {
      ret = get_element_v0(index, get_addr_only);
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

int ObJsonBin::get_element_in_object(size_t i, uint64_t *get_addr_only)
{
  INIT_SUCC(ret);
  ObJBVerType vertype = get_vertype();
  switch (vertype) {
    case ObJBVerType::J_OBJECT_V0: {
      ret = get_element_v0(i, get_addr_only);
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
  uint64_t key_offset = 0;
  uint64_t key_len = 0;
  if (OB_FAIL(get_key_entry(i, key_offset, key_len))) {
    LOG_WARN("get_key_entry fail", K(ret), K(i), K(get_element_count()));
  } else if (OB_FAIL(cursor_->get(pos_ + key_offset, key_len, key))) {
    LOG_WARN("get_key_data fail", K(ret), K(i), K(get_element_count()), K(key_offset), K(key_len));
  }
  return ret;
}


int ObJsonBin::get_key_in_object(size_t i, ObString &key) const
{
  INIT_SUCC(ret);
  ObJBVerType vertype = get_vertype();
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
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  if (key.empty()) {
    ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
    LOG_WARN("key is NULL", K(ret));
  } else if (OB_ISNULL(new_value)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input json binary is null.", K(ret));
  } else if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("update_ctx is null", K(ret));
  } else if (cur_node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(cur_node_type));
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
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  if (OB_ISNULL(new_value)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input json binary is null.", K(ret));
  } else if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("update_ctx is null", K(ret));
  } else if (cur_node_type != ObJsonNodeType::J_ARRAY) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(cur_node_type));
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
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  if (OB_ISNULL(new_value)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input json binary is null.", K(ret));
  } else if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("update_ctx is null", K(ret));
  } else if (cur_node_type != ObJsonNodeType::J_ARRAY && cur_node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(cur_node_type));
  } else if (index >= get_element_count()) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("idx out of range.", K(ret), K(index), K(get_element_count()));
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

int ObJsonBin::rebuild_child_key(
    const int64_t index,
    const ObString& child_key,
    const int64_t key_offset,
    ObJsonBuffer& result)
{
  INIT_SUCC(ret);
  if (OB_FAIL(set_key_entry(index, key_offset, child_key.length()))) {
    LOG_WARN("set_key_entry fail", K(ret), K(child_key));
  } else if (OB_FAIL(result.append(child_key))) {
    LOG_WARN("append key fail", K(ret));
  }
  return ret;
}

int ObJsonBin::rebuild_child(
    const int64_t index,
    const ObJsonBin& child_value,
    const int64_t value_offset,
    ObJsonBuffer& result)
{
  INIT_SUCC(ret);
  uint8_t value_type = 0;
  bool is_update_inline = false;
  if (OB_FAIL(try_update_inline(index, &child_value, is_update_inline))) {
    LOG_WARN("try_update_inline fail", K(ret), K(index));
  } else if (is_update_inline) {
    LOG_DEBUG("try_update_inline success", K(index));
  } else if (OB_FALSE_IT(value_type = OB_JSON_TYPE_GET_INLINE(child_value.get_type()))) {
  } else if (OB_FAIL(set_value_entry(index, value_offset, value_type))) {
    LOG_WARN("set_value_entry fail", K(ret), K(value_offset), K(value_type));
  } else if (OB_FAIL(child_value.rebuild_json_value(result))) {
    LOG_WARN("rebuild_json_value fail", K(ret), K(index));
  }
  return ret;
}

// insert new element to current bin will cause rebuilding current bin
int ObJsonBin::rebuild_with_new_insert_value(int64_t index, const ObString &new_key, ObJsonBin *new_value, ObStringBuffer &result) const
{
  INIT_SUCC(ret);
  int64_t start_pos = result.length();
  bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;
  ObJsonBinMeta meta;
  ObJsonBin dst_bin;
  uint64_t element_count = get_element_count();

  if (OB_FAIL(calc_size_with_insert_new_value(new_key, new_value, meta))) {
    LOG_WARN("calc size fail", K(ret));
  } else if (OB_FAIL(meta.to_header(result))) {
    LOG_WARN("to obj header fail", K(ret));
  } else if (OB_FAIL(dst_bin.reset(result.string(), 0, nullptr))) {
    LOG_WARN("reset bin fail", K(ret), K(meta));
  } else {
    index = ((index == OB_JSON_INSERT_LAST || index > element_count) ? element_count : index);
  }
  // key entry
  for (int i = 0; OB_SUCC(ret) && is_obj_type && i < index; i++) {
    ObString src_key;
    uint64_t key_offset = result.length() - start_pos;
    if (OB_FAIL(get_key(i, src_key))) {
      LOG_WARN("get_key from src_bin fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child_key(i, src_key, key_offset, result))) {
      LOG_WARN("set_key fail", K(ret), K(src_key));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  if (OB_SUCC(ret) && is_obj_type) {
    uint64_t key_offset = result.length() - start_pos;
    if (OB_FAIL(dst_bin.rebuild_child_key(index, new_key, key_offset, result))) {
      LOG_WARN("set_key fail", K(ret), K(new_key));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  for (int i = index; OB_SUCC(ret) && is_obj_type && i < element_count; i++) {
    ObString src_key;
    uint64_t key_offset = result.length() - start_pos;
    if (OB_FAIL(get_key(i, src_key))) {
      LOG_WARN("get_key from src_bin fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child_key(i + 1, src_key, key_offset, result))) {
      LOG_WARN("set_key fail", K(ret), K(src_key));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  // value entry
  ObJsonBin child_value;
  for (int i = 0; OB_SUCC(ret) && i < index; i++) {
    uint64_t value_offset = result.length() - start_pos;
    if (OB_FAIL(get_value(i, child_value))) {
      LOG_WARN("get child value fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child(i, child_value, value_offset, result))) {
      LOG_WARN("rebuild_child fail", K(ret), K(i));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t value_offset = result.length() - start_pos;
    if (OB_FAIL(dst_bin.rebuild_child(index, *new_value, value_offset, result))) {
      LOG_WARN("try_update_inline fail", K(ret), K(index));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  for (int i = index; OB_SUCC(ret) && i < element_count; i++) {
    uint64_t value_offset = result.length() - start_pos;
    if (OB_FAIL(get_value(i, child_value))) {
      LOG_WARN("get child value fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child(i + 1, child_value, value_offset, result))) {
      LOG_WARN("try_update_inline fail", K(ret), K(i));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (dst_bin.obj_size() < (result.length() - start_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_size incorrect", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  } else if (dst_bin.obj_size() == (result.length() - start_pos)) { // if equal, just skip
  } else if (OB_FAIL(dst_bin.set_obj_size(result.length() - start_pos))) {
    LOG_WARN("set obj_size fail", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  }
  return ret;
}

int ObJsonBin::calc_size_with_insert_new_value(const ObString &new_key, const ObJsonBin *new_value, ObJsonBinMeta &new_meta) const
{
  INIT_SUCC(ret);
  bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;
  uint64_t new_serial_size = 0;
  uint64_t old_serial_size = 0;
  uint64_t new_value_serial_size = 0;
  uint64_t old_element_count = this->element_count();
  uint64_t new_element_count = old_element_count + 1;
  uint8_t old_count_var_type = element_count_var_type();
  uint8_t new_count_var_type = ObJsonVar::get_var_type(new_element_count);
  uint8_t old_entry_var_type = entry_var_type();

  if (OB_FAIL(get_serialize_size(old_serial_size))) {
    LOG_WARN("get_serialize_size fail", K(ret));
  } else if (OB_FAIL(new_value->get_serialize_size(new_value_serial_size))) {
    LOG_WARN("get_serialize_size for new_value fail", K(ret));
  } else {
    // plus value_size and value_entry size
    new_serial_size = old_serial_size + new_value_serial_size + (ObJsonVar::get_var_size(old_entry_var_type) + OB_JSON_BIN_VALUE_TYPE_LEN);
    // plus key size and key_entry size
    new_serial_size += (is_obj_type ? (new_key.length() + (ObJsonVar::get_var_size(old_entry_var_type) * 2)) : 0);
    // plus count size if changed
    new_serial_size += (new_count_var_type > old_count_var_type ? (ObJsonVar::get_var_size(new_count_var_type) - ObJsonVar::get_var_size(old_count_var_type)) : 0);

    // may change value entry var type and obj_size_var_type
    // entry_var_type calc from obj_size
    // so entry_var_type  generaly same as obj_size_var_type
    uint8_t new_entry_var_type = ObJsonVar::get_var_type(new_serial_size);
    uint8_t new_obj_size_type = new_entry_var_type;

    if (OB_FAIL(extend_entry_var_type(is_obj_type, new_element_count, new_serial_size, old_entry_var_type, new_entry_var_type, new_serial_size))) {
      LOG_WARN("extend_entry_var_type fail", K(ret), K(is_obj_type), K(new_element_count), K(new_serial_size), K(old_entry_var_type), K(new_entry_var_type));
    } else {
      new_obj_size_type = new_entry_var_type;

      new_meta.set_type(get_vertype(), false);
      new_meta.set_element_count(new_element_count);
      new_meta.set_element_count_var_type(new_count_var_type);
      new_meta.set_obj_size(new_serial_size);
      new_meta.set_obj_size_var_type(new_obj_size_type);
      new_meta.set_entry_var_type(new_entry_var_type);
      new_meta.set_is_continuous(true);
      new_meta.calc_entry_array();
    }
  }
  return ret;
}

// element count not change, just replace with new value at some position
int ObJsonBin::calc_size_with_new_value(const ObJsonBin *old_value, const ObJsonBin *new_value, ObJsonBinMeta &new_meta) const
{
  INIT_SUCC(ret);
  bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;
  uint64_t new_serial_size = 0;
  uint64_t old_serial_size = 0;
  uint64_t old_value_serial_size = 0;
  uint64_t new_value_serial_size = 0;
  uint8_t old_entry_var_type = entry_var_type();

  if (OB_FAIL(get_serialize_size(old_serial_size))) {
    LOG_WARN("get_serialize_size fail", K(ret));
  } else if (! old_value->is_inline_vertype() && OB_FAIL(old_value->get_serialize_size(old_value_serial_size))) {
    LOG_WARN("get_serialize_size for new_value fail", K(ret));
  } else if (OB_FAIL(new_value->get_serialize_size(new_value_serial_size))) {
    LOG_WARN("get_serialize_size for new_value fail", K(ret));
  } else {
    // plus value_size and value_entry size
    new_serial_size =  old_serial_size - old_value_serial_size + new_value_serial_size;

    // may change value entry var type and obj_size_var_type
    // entry_var_type calc from obj_size
    // so entry_var_type  generaly same as obj_size_var_type
    uint8_t new_entry_var_type = ObJsonVar::get_var_type(new_serial_size);
    uint8_t new_obj_size_type = new_entry_var_type;

    if (OB_FAIL(extend_entry_var_type(is_obj_type, this->element_count(), new_serial_size, old_entry_var_type, new_entry_var_type, new_serial_size))) {
      LOG_WARN("extend_entry_var_type fail", K(ret), K(meta_), K(is_obj_type), K(this->element_count()), K(new_serial_size), K(old_entry_var_type), K(new_entry_var_type));
    } else {
      new_obj_size_type = new_entry_var_type;

      new_meta.set_type(get_vertype(), false);
      new_meta.set_element_count(this->element_count());
      new_meta.set_element_count_var_type(element_count_var_type());
      new_meta.set_obj_size(new_serial_size);
      new_meta.set_obj_size_var_type(new_obj_size_type);
      new_meta.set_entry_var_type(new_entry_var_type);
      new_meta.set_is_continuous(true);
      new_meta.calc_entry_array();
    }
  }
  return ret;
}


// current node value entry size not enough store new_value offset, so need rebuild current node.
// if current node's parent value entry size not enough store, rebuild parent too.
// for simple, rebuild one by one
int ObJsonBin::rebuild_with_new_value(int64_t index, ObJsonBin *new_value, ObStringBuffer &result) const
{
  INIT_SUCC(ret);
  int64_t start_pos = result.length();
  bool is_obj_type = json_type() == ObJsonNodeType::J_OBJECT;
  uint64_t element_count = get_element_count();
  ObJsonBinMeta meta;
  ObJsonBin dst_bin;
  ObJsonBin child_value;
  if (OB_FAIL(get_value(index, child_value))) {
      LOG_WARN("get child value fail", K(ret), K(index));
  } else if (OB_FAIL(calc_size_with_new_value(&child_value, new_value, meta))) {
    LOG_WARN("calc size fail", K(ret));
  } else if (OB_FAIL(meta.to_header(result))) {
    LOG_WARN("to obj header fail", K(ret));
  } else if (OB_FAIL(dst_bin.reset(result.string(), 0, nullptr))) {
    LOG_WARN("reset bin fail", K(ret), K(meta));
  } else {
    index = ((index == OB_JSON_INSERT_LAST || index > element_count) ? element_count : index);
  }
  // key entry
  for (int i = 0; OB_SUCC(ret) && is_obj_type && i < element_count; i++) {
    ObString src_key;
    uint64_t key_offset = result.length() - start_pos;
    if (OB_FAIL(get_key(i, src_key))) {
      LOG_WARN("get_key from src_bin fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child_key(i, src_key, key_offset, result))) {
      LOG_WARN("set_key fail", K(ret), K(src_key));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  // value entry
  for (int i = 0; OB_SUCC(ret) && i < element_count; i++) {
    uint64_t value_offset = result.length() - start_pos;
    uint8_t value_type = 0;
    // bool is_update_inline = false;
    if (i == index) {
      if (OB_FAIL(dst_bin.rebuild_child(index, *new_value, value_offset, result))) {
        LOG_WARN("rebuild_child fail", K(ret), K(index));
      } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
      }
    } else if (OB_FAIL(get_value(i, child_value))) {
      LOG_WARN("get child value fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child(i, child_value, value_offset, result))) {
      LOG_WARN("rebuild_child fail", K(ret), K(i));
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), 0))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (dst_bin.obj_size() < (result.length() - start_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_size incorrect", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  } else if (dst_bin.obj_size() == (result.length() - start_pos)) { // if equal, just skip
  } else if (OB_FAIL(dst_bin.set_obj_size(result.length() - start_pos))) {
    LOG_WARN("set obj_size fail", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  }
  return ret;
}

int ObJsonBin::reset_root(const ObString &data)
{
  INIT_SUCC(ret);
  ObString header_data;
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update_ctx is null", K(ret), K(pos_), K(meta_));
  } else if (node_stack_.size() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current node is not root", K(ret), K(pos_), K(meta_));
  } else if (OB_FAIL(cursor_->set(pos_, data))) {
    LOG_WARN("set_data fail", K(ret), K(pos_));
  } else if (OB_FAIL(cursor_->get_for_write(0, sizeof(ObJsonBinDocHeader), header_data))) {
    LOG_WARN("get header data fail", K(ret), K(pos_));
  } else if (OB_FAIL(set_doc_header_v0(header_data, cursor_->get_length()))) {
    LOG_WARN("set_doc_header_v0 fail", K(ret), K(pos_));
  } else if (OB_FAIL(reset_iter())) {
    LOG_WARN("reset_iter fail", K(ret), K(pos_));
  } else {
    update_ctx->is_rebuild_all_ = true;
  }
  return ret;
}

/*
remove child node at index, execute when iter locates at parent node
*/
int ObJsonBin::object_remove_v0(size_t index)
{
  INIT_SUCC(ret);
  uint64_t old_elem_count = element_count();
  uint64_t new_elem_count = old_elem_count - 1;
  ObJBVerType vertype = this->get_vertype();
  ObString header_data;
  int64_t header_len = get_value_entry_offset(this->element_count());
  if (OB_FAIL(record_remove_offset(index))) {
    LOG_WARN("record_remove_offset fail", K(ret), K(index));
  } else if (OB_FAIL(cursor_->get_for_write(pos_, header_len, header_data))) {
    LOG_WARN("get_for_write fail", K(ret), K(index), K(header_len), K(pos_), K(meta_));
  } else {
    // move object key entry
    uint64_t curr_key_entry_offset = get_key_entry_offset(index);
    uint64_t next_key_entry_offset = get_key_entry_offset(index + 1);
    uint64_t key_entry_move_len = get_key_entry_offset(old_elem_count) - next_key_entry_offset;
    MEMMOVE(header_data.ptr() + curr_key_entry_offset, header_data.ptr() + next_key_entry_offset, key_entry_move_len);

    // [key_entry_start_offset, key_entry_end_offset][...][value_entry_start_offset, ..][curr_value_entry_offset][next_value_entry_offset, value_entry_end_offset]
    uint64_t key_entry_end_offset = get_key_entry_offset(new_elem_count);
    uint64_t value_entry_start_offset = get_value_entry_offset(0);
    uint64_t value_entry_end_offset = get_value_entry_offset(old_elem_count); // old_elem_end
    uint64_t curr_value_entry_offset = get_value_entry_offset(index);
    uint64_t next_value_entry_offset = get_value_entry_offset(index + 1);
    // move prev value entries
    uint64_t value_entry_prev_move_len = curr_value_entry_offset - value_entry_start_offset;
    MEMMOVE(header_data.ptr() + key_entry_end_offset, header_data.ptr() + value_entry_start_offset, value_entry_prev_move_len);

    // move next value entries
    uint64_t value_entry_next_move_len = value_entry_end_offset - next_value_entry_offset;
    MEMMOVE(header_data.ptr() + key_entry_end_offset + value_entry_prev_move_len, header_data.ptr() + next_value_entry_offset, value_entry_next_move_len);
    if (OB_FAIL(set_element_count(new_elem_count))) {
      LOG_WARN("set element count fail", K(ret), K(new_elem_count), K(old_elem_count), K(index));
    } else if (OB_FAIL(reset(pos_))) {
      LOG_WARN("reset fail", K(ret));
    }
  }
  return ret;
}

int ObJsonBin::array_remove_v0(size_t index)
{
  INIT_SUCC(ret);
  uint64_t old_elem_count = element_count();
  uint64_t new_elem_count = old_elem_count - 1;
  ObJBVerType vertype = this->get_vertype();
  int node_stack_size = node_stack_.size();
  ObJsonBuffer new_bin_str(allocator_);
  ObJBNodeMeta parent_node_meta;
  ObJsonBin new_bin;

  // move value entry
  uint64_t curr_value_entry_offset = get_value_entry_offset(index);
  uint64_t next_value_entry_offset = get_value_entry_offset(index + 1);
  uint64_t value_entry_move_len = get_value_entry_offset(old_elem_count) - next_value_entry_offset;
  // MEMMOVE(data + curr_value_entry_offset, data + next_value_entry_offset, value_entry_move_len);
  if (OB_FAIL(cursor_->move_data(pos_ + curr_value_entry_offset, pos_ + next_value_entry_offset, value_entry_move_len))) {
    LOG_WARN("move_data fail", K(ret), K(pos_), K(curr_value_entry_offset), K(next_value_entry_offset), K(value_entry_move_len), K(index));
  } else if (OB_FAIL(set_element_count(new_elem_count))) {
    LOG_WARN("set element count fail", K(ret), K(new_elem_count), K(old_elem_count), K(index));
  } else if (OB_FAIL(reset(pos_))) {
    LOG_WARN("reset fail", K(ret));
  } else if (OB_FAIL(rebuild_json_array(new_bin_str))) {
    LOG_WARN("rebuild array fail", K(ret), K(pos_), K(meta_));
  } else if (node_stack_size <= 0) {
    if (OB_FAIL(reset_root(new_bin_str.string()))) {
      LOG_WARN("reset_root fail", K(ret), K(index), K(node_stack_size), K(new_bin_str));
    }
  } else if (OB_FAIL(node_stack_.back(parent_node_meta))) {
    LOG_WARN("get node fail", K(ret), K(node_stack_size));
  } else if (OB_FAIL(new_bin.reset(new_bin_str.string(), 0, nullptr))) {
    LOG_WARN("reset fail", K(ret));
  } else if (OB_FAIL(move_parent_iter())) {
        LOG_WARN("move_parent_iter fail", K(ret), K(pos_), K(meta_));
  } else if (OB_FAIL(update(parent_node_meta.idx_, &new_bin))) {
    LOG_WARN("update parent fail", K(ret), K(meta_), K(pos_), K(parent_node_meta));
  } else if (OB_FAIL(element(parent_node_meta.idx_))) {
    LOG_WARN("move child fail", K(ret), K(meta_), K(pos_), K(parent_node_meta));
  }
  return ret;
}


int ObJsonBin::remove(size_t index)
{
  INIT_SUCC(ret);
  ObJsonNodeType node_type = this->json_type();
  ObJBVerType ver_type = this->get_vertype();
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  if (node_type != ObJsonNodeType::J_ARRAY && node_type != ObJsonNodeType::J_OBJECT) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("wrong node_type.", K(ret), K(node_type));
  } else if (index >= get_element_count()) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range.", K(ret), K(index), K(get_element_count()));
  } else if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("update_ctx is null", K(ret));
  } else {
    switch (ver_type) {
      case ObJBVerType::J_ARRAY_V0:
        ret = array_remove_v0(index);
        break;
      case ObJBVerType::J_OBJECT_V0: {
        ret = object_remove_v0(index);
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
  ObJsonBuffer buffer(allocator_);
  ObString new_bin;
  if (allocator_ == NULL) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("json binary is read only.", K(ret));
  } else if (node_stack_.size() > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not at root", K(ret), K(pos_));
  } else if (OB_FAIL(rebuild(buffer))) {
      LOG_WARN("rebuild failed.", K(ret));
  } else if (OB_FAIL(buffer.get_result_string(new_bin))) {
    LOG_WARN("get_result_string fail", K(ret));
  } else if (OB_FAIL(set_current(new_bin, pos_))) {
    LOG_WARN("set_current fail", K(ret));
  } else {
    ret = reset_iter();
  }
  return ret;
}

int ObJsonBin::rebuild_at_iter(ObJsonBuffer &buf)
{
  INIT_SUCC(ret);
  buf.reuse();
  if (OB_FAIL(rebuild_json_value(buf))) {
    LOG_WARN("rebuild json binary iter falied", K(ret));
  }
  return ret;
}

int ObJsonBin::insert_v0(int64_t index, const ObString &new_key, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  bool is_exist = false;
  bool is_object_type = json_type() == ObJsonNodeType::J_OBJECT;
  size_t idx = index;
  if (! is_object_type) {
  } else if (OB_FAIL(lookup_index(new_key, &idx))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("lookup key internel fail", K(ret), K(new_key));
    }
  } else if (OB_FAIL(update_v0(idx, new_value))) {
    LOG_WARN("key exist, do update fail", K(idx), K(new_key), K(idx));
  } else {
    is_exist = true;
  }

  if (OB_FAIL(ret) || is_exist) {
  } else if (is_object_type && OB_FAIL(lookup_insert_postion(new_key, idx))) {
    LOG_WARN("lookup key postion fail", K(ret), K(is_object_type), K(idx), K(new_key));
  } else if (OB_FAIL(insert_recursion(idx, new_key, new_value))) {
    LOG_WARN("fail", K(ret));
  }
  return ret;
}


int ObJsonBin::update_append_v0(int index, ObJsonBin *new_value, bool &is_update_append)
{
  INIT_SUCC(ret);
  uint8_t entry_var_type = this->entry_var_type();
  uint8_t value_type = new_value->get_type();
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  ObJsonBuffer &update_buffer = update_ctx->get_tmp_buffer();
  uint64_t start_pos = cursor_->get_length();
  uint64_t value_offset = start_pos - get_extend_seg_offset();
  uint64_t src_value_offset = 0;
  uint8_t src_value_type = 0;

  if (entry_var_type < ObJsonVar::get_var_type(value_offset)) {
  } else if (OB_FAIL(get_value_entry(index, src_value_offset, src_value_type))) {
    LOG_WARN("get_key_entry fail", K(ret), K(index));
  } else if (OB_FAIL(set_value_entry(index, value_offset, J_FORWARD_V0))) {
    LOG_WARN("set_value_entry fail", K(ret), K(value_offset), K(value_type));
  } else if (! need_type_prefix(value_type)  // append-update will record type for all type
      && (OB_FAIL(update_buffer.append(reinterpret_cast<const char*>(&value_type), sizeof(value_type))))) {
    LOG_WARN("append type fail", K(ret), K(value_type));
  } else if (OB_FAIL(new_value->rebuild_json_value(update_buffer))) {
    LOG_WARN("serialize_json_value fail", K(ret));
  } else if (OB_FAIL(cursor_->append(update_buffer.string()))) {
    LOG_WARN("append fail", K(ret));
  } else if (OB_FAIL(record_append_update_offset(
      index,
      start_pos,
      update_buffer.length(),
      new_value->get_type()))) {
    LOG_WARN("record_append_update_offset fail", K(ret));
  } else {
    is_update_append = true;
  }
  return ret;
}

// entry var type might get bigger, and affect parents.
// so may need update with tree
int ObJsonBin::update_recursion(int index, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  int node_stack_size = node_stack_.size();
  ObJBNodeMetaStack dup_stack(allocator_);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  ObJsonBuffer& result = update_ctx->get_tmp_buffer();
  uint64_t start_pos = cursor_->get_length();
  uint8_t cur_value_type = get_type();
  // rebuild self with new value
  if (OB_FAIL(rebuild_with_new_value(index, new_value, result))) {
    LOG_WARN("fail", K(ret));
  } else if (node_stack_size <= 0) {
    if (OB_FAIL(reset_root(result.string()))) {
      LOG_WARN("reset_root fail", K(ret), K(index), KPC(new_value), K(result));
    }
  // set and rebuild parent if need
  } else if (OB_FAIL(dup_stack.copy(node_stack_))) {
    LOG_WARN("copy node stack fail", K(ret), K(node_stack_size));
  } else {
    ObJBNodeMeta parent_node_meta;
    ObJsonBuffer tmp_buf(allocator_);
    ObJsonBin tmp_bin;
    int recursion_end_idx = -1;
    for(int i = node_stack_size - 1; OB_SUCC(ret) && i >= 0 && recursion_end_idx == -1; --i) {
      uint64_t child_pos = pos_;
      tmp_buf.reuse();
      bool is_updated = false;
      ObString child_data = result.string();
      if (OB_FAIL(node_stack_.back(parent_node_meta))) {
        LOG_WARN("get node fail", K(ret), K(node_stack_size), K(i));
      } else if (OB_FAIL(move_parent_iter())) {
        LOG_WARN("move fail", K(ret), K(parent_node_meta));
      } else if (parent_node_meta.entry_type_ >= ObJsonVar::get_var_type(start_pos - get_extend_seg_offset())) {
        if (OB_FAIL(set_value_entry(parent_node_meta.idx_, start_pos - get_extend_seg_offset(), J_FORWARD_V0))) {
          LOG_WARN("set_value_entry fail", K(ret), K(pos_), K(child_pos));
        } else if (OB_FAIL(cursor_->append(child_data))) {
          LOG_WARN("append fail", K(ret), K(pos_));
        } else if (OB_FAIL(record_append_update_offset(
            parent_node_meta.idx_,
            start_pos,
            cursor_->get_length() - start_pos,
            cur_value_type))) {
          LOG_WARN("record_append_update_offset fail", K(ret), K(start_pos), K(parent_node_meta));
        } else {
          recursion_end_idx = i;
        }
      } else if (OB_FAIL(tmp_buf.append(child_data))) {
        LOG_WARN("copy buffer fail", K(ret), K(pos_));
      } else if (OB_FAIL(tmp_bin.reset(tmp_buf.string(), 0, nullptr))) {
        LOG_WARN("reset fail", K(ret));
      } else if (OB_FALSE_IT(result.reuse())) {
      } else if (OB_FAIL(rebuild_with_new_value(parent_node_meta.idx_, &tmp_bin, result))) {
        LOG_WARN("fail", K(ret), K(parent_node_meta), K(tmp_bin));
      } else {
        cur_value_type = get_type();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (-1 == recursion_end_idx) {
      if (OB_FAIL(reset_root(result.string()))) {
        LOG_WARN("reset_root fail", K(ret), K(index), KPC(new_value), K(result));
      } else {
        recursion_end_idx = 0;
      }
    }

    for (int i = recursion_end_idx; OB_SUCC(ret) && i < node_stack_size; ++i) {
      if (OB_FAIL(dup_stack.at(i, parent_node_meta))) {
        LOG_WARN("get node fail", K(ret), K(node_stack_size), K(i));
      } else if (OB_FAIL(element(parent_node_meta.idx_))) {
        LOG_WARN("move back postion fail", K(ret), K(i), K(parent_node_meta));
      }
    }
  }
  return ret;
}

int ObJsonBin::insert_recursion(int index, const ObString &new_key, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  int node_stack_size = node_stack_.size();
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  uint64_t start_pos = cursor_->get_length();
  ObJBNodeMeta parent_node_meta;
  ObJsonBin tmp_bin;
  ObJsonBuffer tmp_buf(allocator_);
  bool is_updated = false;
  uint8_t cur_value_type = get_type();
  if (OB_FAIL(rebuild_with_new_insert_value(index, new_key, new_value, tmp_buf))) {
    LOG_WARN("fail", K(ret));
  } else if (node_stack_size <= 0) {
    if (OB_FAIL(reset_root(tmp_buf.string()))) {
      LOG_WARN("reset_root fail", K(ret), K(index), KPC(new_value), K(tmp_buf));
    }
  } else if (OB_FAIL(node_stack_.back(parent_node_meta))) {
    LOG_WARN("get node fail", K(ret), K(node_stack_size));
  } else if (OB_FAIL(move_parent_iter())) {
    LOG_WARN("move fail", K(ret), K(parent_node_meta));
  } else if (parent_node_meta.entry_type_ >= ObJsonVar::get_var_type(start_pos - get_extend_seg_offset())) {
    if (OB_FAIL(set_value_entry(parent_node_meta.idx_, start_pos - get_extend_seg_offset(), J_FORWARD_V0))) {
      LOG_WARN("set_value_entry fail", K(ret), K(pos_), K(start_pos));
    } else if (OB_FAIL(cursor_->append(tmp_buf.string()))) {
      LOG_WARN("copy buffer fail", K(ret), K(pos_));
    } else if (OB_FAIL(record_append_update_offset(
        parent_node_meta.idx_,
        start_pos,
        cursor_->get_length() - start_pos,
        cur_value_type))) {
      LOG_WARN("record_append_update_offset fail", K(ret), K(parent_node_meta.idx_));
    } else if (OB_FAIL(element(parent_node_meta.idx_))) {
      LOG_WARN("move back fail", K(ret), K(parent_node_meta.idx_));
    }
  } else if (OB_FAIL(tmp_bin.reset(tmp_buf.string(), 0, nullptr))) {
    LOG_WARN("reset fail", K(ret));
  } else if (tmp_bin.obj_size() != tmp_buf.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_size incorrect", K(ret), "bin", tmp_bin.obj_size(), "buf", tmp_buf.length());
  } else if (OB_FAIL(update_recursion(parent_node_meta.idx_, &tmp_bin))) {
    LOG_WARN("update_recursion fail", K(ret));
  } else if (OB_FAIL(element(parent_node_meta.idx_))) {
    LOG_WARN("move back fail", K(ret), K(parent_node_meta.idx_));
  }
  return ret;
}

int ObJsonBin::update_v0(int index, ObJsonBin *new_value)
{
  INIT_SUCC(ret);
  uint64_t src_value_offset = 0;
  uint8_t src_value_type = 0;
  bool is_update_inline = false;
  bool is_update_inplace = false;
  bool is_update_append = false;
  bool is_updated = false;
  if (OB_FAIL(get_value_entry(index, src_value_offset, src_value_type))) {
    LOG_WARN("get_key_entry fail", K(ret), K(index));
  } else if (OB_FAIL(try_update_inline(index, new_value, is_update_inline))) {
    LOG_WARN("try_update_inline fail", K(ret), K(index));
  } else if (is_update_inline) {
    LOG_DEBUG("try_update_inline success", K(index));
    if (OB_FAIL(record_inline_update_offset(index))) {
      LOG_WARN("record_inline_update_offset fail", K(ret), K(index));
    }
  } else if (OB_FAIL(try_update_inplace(index, new_value, is_update_inplace))) {
    LOG_WARN("try_update_inplace fail", K(ret), K(index));
  } else if (is_update_inplace) {
    LOG_DEBUG("try_update_inplace success", K(index));
  } else if (OB_FAIL(update_append_v0(index, new_value, is_update_append))) {
    LOG_DEBUG("update_append_v0 fail", K(ret), K(index));
  } else if (is_update_append) {
    LOG_DEBUG("is_update_append success", K(index));
  } else if (OB_FAIL(update_recursion(index, new_value))) {
    LOG_WARN("fail", K(ret));
  }
  return ret;
}

int ObJsonBin::rebuild_json_object_v0(ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  bool with_key_dict = false;
  int64_t start_pos = result.length();
  uint64_t offset = 0;
  uint64_t element_count = this->element_count();
  ObJsonBin dst_bin;
  ObJsonBinMeta meta;
  uint64_t new_obj_size = 0;

  if (OB_FAIL(this->get_serialize_size(new_obj_size))) {
    LOG_WARN("get_serialize_size fail", K(ret), K(meta_));
  } else {
    meta.set_type(get_object_vertype(), false);
    meta.set_element_count(element_count);
    meta.set_element_count_var_type(ObJsonVar::get_var_type(element_count));
    meta.set_obj_size(new_obj_size);
    meta.set_obj_size_var_type(OB_MAX(entry_var_type(), ObJsonVar::get_var_type(new_obj_size)));
    meta.set_entry_var_type(meta.obj_size_var_type());
    meta.set_is_continuous(true);
    meta.calc_entry_array();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta.to_header(result))) {
    LOG_WARN("to obj header fail", K(ret));
  } else if (OB_FAIL(dst_bin.reset(result.string(), start_pos, nullptr))) {
    LOG_WARN("reset bin fail", K(ret), K(meta));
  }

  for (uint64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
    ObString src_key;
    uint64_t key_offset = result.length() - start_pos;
    if (OB_FAIL(get_key(i, src_key))) {
      LOG_WARN("get_key from src_bin fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child_key(i, src_key, key_offset, result))) {
      LOG_WARN("set_key_entry fail", K(ret), K(src_key));
    // result may realloc, so need ensure point same memory
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), start_pos))) {
    }
  }

  ObJsonBin child_value;
  for (int i = 0; OB_SUCC(ret) && i < element_count; i++) {
    uint64_t value_offset = result.length() - start_pos;
    if (OB_FAIL(get_value(i, child_value))) {
      LOG_WARN("get child value fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child(i, child_value, value_offset, result))) {
      LOG_WARN("try_update_inline fail", K(ret), K(i));
    // result may realloc, so need ensure point same memory
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), start_pos))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (dst_bin.obj_size() < (result.length() - start_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_size incorrect", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  } else if (dst_bin.obj_size() == (result.length() - start_pos)) { // if equal, just skip
  } else if (OB_FAIL(dst_bin.set_obj_size(result.length() - start_pos))) {
    LOG_WARN("set obj_size fail", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  }
  return ret;
}

int ObJsonBin::rebuild_json_object(ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  if (ObJBVerType::J_OBJECT_V0 == get_vertype() && ObJBVerType::J_OBJECT_V0 == get_vertype()) {
    ret = rebuild_json_object_v0(result);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild json object, invalid vertype.", K(ret));
  }
  return ret;
}

int ObJsonBin::rebuild_json_array_v0(ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  int64_t start_pos = result.length();
  uint64_t offset = 0;
  uint64_t element_count = this->element_count();
  ObJsonBin dst_bin;
  ObJsonBinMeta meta;
  uint64_t new_obj_size = 0;
  if (OB_FAIL(this->get_serialize_size(new_obj_size))) {
    LOG_WARN("get_serialize_size fail", K(ret), K(meta_));
  } else {
    meta.set_type(get_array_vertype(), false);
    meta.set_element_count(element_count);
    meta.set_element_count_var_type(ObJsonVar::get_var_type(element_count));
    meta.set_obj_size(new_obj_size);
    meta.set_obj_size_var_type(OB_MAX(entry_var_type(), ObJsonVar::get_var_type(new_obj_size)));
    meta.set_entry_var_type(meta.obj_size_var_type());
    meta.set_is_continuous(true);
    meta.calc_entry_array();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta.to_header(result))) {
    LOG_WARN("to obj header fail", K(ret));
  } else if (OB_FAIL(dst_bin.reset(result.string(), start_pos, nullptr))) {
    LOG_WARN("reset bin fail", K(ret), K(meta));
  }
    ObJsonBin child_value;
  for (int i = 0; OB_SUCC(ret) && i < element_count; i++) {
    uint64_t value_offset = result.length() - start_pos;
    uint8_t value_type = 0;
    bool is_update_inline = false;
    if (OB_FAIL(get_value(i, child_value))) {
      LOG_WARN("get child value fail", K(ret), K(i));
    } else if (OB_FAIL(dst_bin.rebuild_child(i, child_value, value_offset, result))) {
      LOG_WARN("try_update_inline fail", K(ret), K(i));
    // result may realloc, so need ensure point same memory
    } else if (OB_FALSE_IT(dst_bin.set_current(result.string(), start_pos))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (dst_bin.obj_size() < (result.length() - start_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_size incorrect", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  } else if (dst_bin.obj_size() == (result.length() - start_pos)) { // if equal, just skip
  } else if (OB_FAIL(dst_bin.set_obj_size(result.length() - start_pos))) {
    LOG_WARN("set obj_size fail", K(ret), "bin", dst_bin.obj_size(), "buf", result.length(), K(start_pos));
  }
  return ret;
}

int ObJsonBin::rebuild_json_array(ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  if (ObJBVerType::J_ARRAY_V0 == get_vertype() && ObJBVerType::J_ARRAY_V0 == get_vertype()) {
    ret = rebuild_json_array_v0(result);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild json object.", K(ret));
  }
  return ret;
}

int ObJsonBin::rebuild_json_value(ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  bool is_inlined = OB_JSON_TYPE_IS_INLINE(get_type());
  ObJBVerType vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(get_type()));
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
  ObString data;
  switch (node_type) {
    case ObJsonNodeType::J_NULL: {
      if (OB_FAIL(result.append("\0", sizeof(char)))) {
        LOG_WARN("failed to rebuild null type.", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL: {
      if (OB_FAIL(cursor_->get(pos_, meta_.bytes_, data))) {
        LOG_WARN("get data fail", K(ret), K(pos_));
      } else if (OB_FAIL(result.append(data))) {
        LOG_WARN("failed to append", K(ret), K(pos_));
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG: {
      if (is_inlined) {
        if (OB_FAIL(ObJsonBinSerializer::serialize_json_integer(get_uint(), result))) {
          LOG_WARN("failed to rebuild serialize integer.", K(ret));
        }
      } else {
        int64_t val = 0;
        int64_t pos = pos_;
        if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
          LOG_WARN("decode integer failed.", K(ret), K(pos));
        } else if (OB_FAIL(cursor_->get(pos_, pos - pos_, data))) {
          LOG_WARN("get data fail", K(ret), K(pos), K(pos_));
        } else if (OB_FAIL(result.append(data))) {
          LOG_WARN("failed to append integer date.", K(ret), K(pos));
        }
      }
      break;
    }
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE: {
      double val = 0;
      if (OB_FAIL(cursor_->get(pos_, sizeof(double), data))) {
        LOG_WARN("get data fail", K(ret), K(pos_));
      } else if (OB_FAIL(result.append(data))) {
        LOG_WARN("failed to append integer date.", K(ret), K(pos_));
      }
      break;
    }
    case ObJsonNodeType::J_OFLOAT: {
      if (OB_FAIL(cursor_->get(pos_, sizeof(float), data))) {
        LOG_WARN("get data fail", K(ret), K(pos_));
      } else if (OB_FAIL(result.append(data))) {
        LOG_WARN("failed to append integer date.", K(ret), K(pos_));
      }
      break;
    }
    case ObJsonNodeType::J_STRING: {
      int64_t val = 0;
      int64_t pos = pos_;
      if (vertype == ObJBVerType::J_STRING_V0) {
        pos += sizeof(uint8_t);
        if (OB_FAIL(cursor_->decode_vi64(pos, &val))) {
          LOG_WARN("fail to decode str length.", K(ret), K(pos));
        } else if (OB_FAIL(cursor_->get(pos_, pos - pos_ + val, data))) {
          LOG_WARN("get data fail", K(ret), K(pos_), K(pos), K(val));
        } else if (OB_FAIL(result.append(data))) {
          LOG_WARN("failed to append", K(ret), K(pos), K(data));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid string vertype.", K(ret), K(vertype));
      }
      break;
    }
    case ObJsonNodeType::J_OBJECT: {
      ret = rebuild_json_object(result);
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      ret = rebuild_json_array(result);
      break;
    }
    case ObJsonNodeType::J_BOOLEAN: {
      if (OB_FAIL(ObJsonBinSerializer::serialize_json_integer(get_boolean(), result))) {
          LOG_WARN("failed to rebuild serialize boolean.", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_DATE:
    case ObJsonNodeType::J_ORACLEDATE: {
      if (OB_FAIL(cursor_->get(pos_, sizeof(int32_t), data))) {
        LOG_WARN("get data fail", K(ret), K(pos_));
      } else if (OB_FAIL(result.append(data))) {
        LOG_WARN("failed to append integer date.", K(ret), K(pos_));
      }
      break;
    }
    case ObJsonNodeType::J_TIME:
    case ObJsonNodeType::J_DATETIME:
    case ObJsonNodeType::J_TIMESTAMP:
    case ObJsonNodeType::J_ODATE:
    case ObJsonNodeType::J_OTIMESTAMP:
    case ObJsonNodeType::J_OTIMESTAMPTZ: {
      if (OB_FAIL(cursor_->get(pos_, sizeof(int64_t), data))) {
        LOG_WARN("get data fail", K(ret), K(pos_));
      } else if (OB_FAIL(result.append(data))) {
        LOG_WARN("failed to append integer date.", K(ret), K(pos_));
      }
      break;
    }
    case ObJsonNodeType::J_OPAQUE: {
      int64_t val = 0;
      if (vertype == ObJBVerType::J_OPAQUE_V0) {
        if (OB_FAIL(cursor_->read_i64(pos_ + sizeof(uint8_t) + sizeof(uint16_t), &val))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data length not enough for opaque len.", K(ret), K(pos_));
        } else if (OB_FAIL(cursor_->get(pos_, sizeof(uint8_t) + sizeof(uint16_t) + sizeof(int64_t) + val, data))) {
          LOG_WARN("get data fail", K(ret), K(pos_));
        } else if (OB_FAIL(result.append(data))) {
          LOG_WARN("failed to append integer date.", K(ret), K(pos_));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid json opaque vertype.", K(ret), K(vertype));
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

int ObJsonBin::replace_value(const ObString &new_data)
{
  INIT_SUCC(ret);
  uint64_t curr_area_size = 0;
  if (this->is_inline_vertype()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not replace inline", K(ret));
  } else if (OB_FAIL(get_area_size(curr_area_size))) {
    LOG_WARN("get area size fail", K(ret));
  } else if (curr_area_size < new_data.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_area_size is too large", K(ret), K(curr_area_size), "new_data_length", new_data.length());
  } else if (OB_FAIL(cursor_->set(pos_, new_data))) {
    LOG_WARN("set data fail", K(ret), K(pos_), K(curr_area_size), K(new_data));
  }
  return ret;
}

int ObJsonBin::try_update_inplace_in_extend(
    int index,
    ObJsonBin *new_value,
    bool &is_update_inplace)
{
  INIT_SUCC(ret);
  ObJsonBin child;
  ObString new_data;
  uint64_t child_area_size = 0;
  uint64_t new_area_size = 0;
  uint64_t src_value_offset = 0;
  uint8_t src_value_type = 0;
  uint8_t new_value_type = OB_JSON_TYPE_GET_INLINE(new_value->get_type());
  uint64_t real_offset = 0;
  if (OB_FAIL(get_value_entry(index, src_value_offset, src_value_type))) {
    LOG_WARN("get_value_entry fail", K(ret), K(index));
  } else if (! is_forward_v0(src_value_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not in extend", K(ret), K(index), K(src_value_type), K(src_value_offset));
  } else if (OB_FALSE_IT(real_offset = get_extend_value_offset(src_value_offset))) {
  } else if (OB_FAIL(get_value(index, child))) {
    LOG_WARN("get child fail", K(ret), K(index));
  } else if (OB_FAIL(child.get_area_size(child_area_size))) {
    LOG_WARN("get area size fail", K(ret));
  } else if (OB_FAIL(new_value->get_area_size(new_area_size))) {
    LOG_WARN("get area size fail", K(ret));
  } else if (! need_type_prefix(new_value_type)) {
    if (child_area_size + child.pos_ - real_offset < new_area_size + OB_JSON_BIN_VALUE_TYPE_LEN) { // skip
    } else if (OB_FAIL(new_value->get_value_binary(new_data))) {
      LOG_WARN("get_value_binary fail", K(ret));
    } else if (new_data.length() != new_area_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new data length not equal to new_area_size", K(new_data.length()), K(new_area_size));
    } else if (OB_FAIL(cursor_->write_i8(real_offset, new_value_type))) {
      LOG_WARN("write type byte fail", K(ret), K(index), K(src_value_offset), K(real_offset), K(child.get_type()), K(new_value_type));
    } else if (OB_FAIL(cursor_->set(real_offset + OB_JSON_BIN_VALUE_TYPE_LEN, new_data))) {
      LOG_WARN("set value fail", K(ret), K(index), K(new_data));
    } else if (OB_FAIL(record_extend_inplace_update_offset(
      index, real_offset, new_area_size + OB_JSON_BIN_VALUE_TYPE_LEN, new_value->get_type()))) {
      LOG_WARN("record_extend_inplace_update_offset fail", K(ret));
    } else {
      is_update_inplace = true;
    }
  } else if (child_area_size + child.pos_ - real_offset < new_area_size) { // skip
  } else if (OB_FAIL(new_value->get_value_binary(new_data))) {
    LOG_WARN("get_value_binary fail", K(ret));
  } else if (new_data.length() != new_area_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new data length not equal to new_area_size", K(new_data.length()), K(new_area_size));
  } else if (OB_FAIL(cursor_->set(real_offset, new_data))) {
    LOG_WARN("set value fail", K(ret), K(index), K(new_data));
  } else if (OB_FAIL(record_extend_inplace_update_offset(
    index, real_offset, new_area_size, new_value->get_type()))) {
    LOG_WARN("record_extend_inplace_update_offset fail", K(ret));
  } else {
    is_update_inplace = true;
  }
  return ret;
}

int ObJsonBin::try_update_inplace(
    int index,
    ObJsonBin *new_value,
    bool &is_update_inplace)
{
  INIT_SUCC(ret);
  ObJsonBin child;
  ObString new_data;
  uint64_t child_area_size = 0;
  uint64_t new_area_size = 0;
  uint64_t src_value_offset = 0;
  uint8_t src_value_type = 0;
  uint8_t new_value_type = OB_JSON_TYPE_GET_INLINE(new_value->get_type());
  if (OB_FAIL(get_value_entry(index, src_value_offset, src_value_type))) {
    LOG_WARN("get_value_entry fail", K(ret), K(index));
  } else if (is_forward_v0(src_value_type)) {
    if (OB_FAIL(try_update_inplace_in_extend(index, new_value, is_update_inplace))) {
      LOG_WARN("try_update_inplace_in_extend fail", K(ret), K(index));
    }
  } else if (OB_FAIL(get_value(index, child))) {
    LOG_WARN("get child fail", K(ret), K(index));
  } else if (child.is_inline_vertype()) { // can not inplace, skip
  } else if (OB_FAIL(child.get_area_size(child_area_size))) {
    LOG_WARN("get area size fail", K(ret));
  } else if (OB_FAIL(new_value->get_area_size(new_area_size))) {
    LOG_WARN("get area size fail", K(ret));
  } else if (child_area_size < new_area_size) { // skip
  } else if (OB_FAIL(new_value->get_value_binary(new_data))) {
    LOG_WARN("get area size fail", K(ret));
  } else if (new_data.length() != new_area_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new data length not equal to new_area_size", K(new_data.length()), K(new_area_size));
  } else if (OB_FAIL(child.replace_value(new_data))) {
    LOG_WARN("replace child value fail", K(ret), K(index), K(new_data));
  } else if (src_value_type != new_value_type && OB_FAIL(set_value_entry(index, src_value_offset, new_value_type))) {
    LOG_WARN("set_value_entry fail", K(ret), K(index), K(src_value_offset));
  } else if (OB_FAIL(record_inplace_update_offset(index, new_value, src_value_type != new_value_type))) {
    LOG_WARN("record_inplace_update_offset fail", K(ret), K(index));
  } else {
    is_update_inplace = true;
  }
  return ret;
}

int ObJsonBin::rebuild(ObJsonBuffer &result) const
{
  INIT_SUCC(ret);
  result.reuse();
  if (is_empty_data()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("binary is empty ptr.", K(ret), KPC(this));
  } else if (OB_FAIL(add_doc_header_v0(result))) {
    LOG_WARN("add_doc_header_v0 fail", K(ret));
  } else if (OB_FAIL(rebuild_json_value(result))) {
    LOG_WARN("do rebuild recursion failed.", K(ret), K(get_type()));
  } else if (OB_FAIL(set_doc_header_v0(result, result.length()))) {
    LOG_WARN("set_doc_header_v0 fail", K(ret));
  }
  return ret;
}

void ObJsonBin::destroy()
{
  if (OB_NOT_NULL(ctx_) && is_alloc_ctx_) {
    ctx_->~ObJsonBinCtx();
    allocator_->free(ctx_);
    ctx_ = nullptr;
  }
  node_stack_.reset();
  local_cursor_.reset();
  cursor_ = &local_cursor_;
}

int ObJsonBin::reset()
{
  INIT_SUCC(ret);
  destroy();
  return ret;
}

int ObJsonBin::init_meta()
{
  INIT_SUCC(ret);
  ObJsonBinMetaParser meta_parser(cursor_, pos_, meta_);
  if (OB_FAIL(meta_parser.parse())) {
    LOG_WARN("meta parse fail", K(ret));
  }
  return ret;
}

int ObJsonBin::ObJBNodeMetaStack::copy(const ObJBNodeMetaStack& src)
{
  INIT_SUCC(ret);
  buf_.reset();
  uint64_t len = src.buf_.length();
  if (0 == src.size()) {
  } else if (OB_FAIL(buf_.append(src.buf_.ptr(), len))) {
    LOG_WARN("copy path stack failed", K(ret), K(len));
  }
  return ret;
}

int ObJsonBin::ObJBNodeMetaStack::pop()
{
  INIT_SUCC(ret);
  uint64_t len = buf_.length();
  if (len > 0) {
    buf_.set_length(len - JB_PATH_NODE_LEN);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed pop stack", K(ret));
  }
  return ret;
}

int ObJsonBin::ObJBNodeMetaStack::back(ObJBNodeMeta& node, bool is_pop)
{
  INIT_SUCC(ret);
  uint64_t len = buf_.length();
  if (len > 0) {
    char* data = (buf_.ptr() + len) - JB_PATH_NODE_LEN;
    node = *(reinterpret_cast<ObJBNodeMeta*>(data));
    if (is_pop) {
      buf_.set_length(len - JB_PATH_NODE_LEN);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get stack node.", K(ret));
  }
  return ret;
}

int ObJsonBin::ObJBNodeMetaStack::back(ObJBNodeMeta& node) const
{
  INIT_SUCC(ret);
  uint64_t len = buf_.length();
  if (len > 0) {
    const char* data = (buf_.ptr() + len) - JB_PATH_NODE_LEN;
    node = *(reinterpret_cast<const ObJBNodeMeta*>(data));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get stack node.", K(ret));
  }
  return ret;
}


int ObJsonBin::ObJBNodeMetaStack::push(const ObJBNodeMeta& node)
{
  return buf_.append(reinterpret_cast<const char*>(&node), JB_PATH_NODE_LEN);
}

int ObJsonBin::ObJBNodeMetaStack::at(uint32_t idx, ObJBNodeMeta& node) const
{
  INIT_SUCC(ret);
  uint32_t size = buf_.length() / JB_PATH_NODE_LEN;
  if (size > idx) {
    const char* data = (buf_.ptr() + idx * JB_PATH_NODE_LEN);
    node = *(reinterpret_cast<const ObJBNodeMeta*>(data));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed invalid id.", K(idx), K(size));
  }
  return ret;
}

void ObJsonBin::ObJBNodeMetaStack::update(uint32_t idx, const ObJBNodeMeta& new_value)
{
  ObJBNodeMeta* node = reinterpret_cast<ObJBNodeMeta*>(buf_.ptr() + idx * JB_PATH_NODE_LEN);
  *node = new_value;
}

int32_t ObJsonBin::ObJBNodeMetaStack::size() const
{
  return buf_.length() / JB_PATH_NODE_LEN;
}

void ObJsonBin::ObJBNodeMetaStack::reset()
{
  buf_.reset();
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

bool ObJsonVerType::is_signed_online_integer(uint8_t type)
{
  bool res = false;
  ObJBVerType vertype = static_cast<ObJBVerType>(OB_JSON_TYPE_GET_INLINE(type));
  ObJsonNodeType node_type = ObJsonVerType::get_json_type(vertype);
  if (! OB_JSON_TYPE_IS_INLINE(type)) {
  } else if (ObJsonNodeType::J_INT  == node_type
      || ObJsonNodeType::J_OINT == node_type) {
    res = true;
  }
  return res;
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

int ObJsonVar::read_var(const ObILobCursor *cursor, int64_t offset, uint8_t type, uint64_t *var)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cursor)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input data is null.", K(ret));
  } else {
    ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
    switch (size) {
      case JBLS_UINT8: {
        uint8_t data = 0;
        if (OB_FAIL(cursor->read_i8(offset, reinterpret_cast<int8_t*>(&data)))) {
          LOG_WARN("read_i8 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<uint64_t>(data);
        }
        break;
      }
      case JBLS_UINT16: {
        uint16_t data = 0;
        if (OB_FAIL(cursor->read_i16(offset, reinterpret_cast<int16_t*>(&data)))) {
          LOG_WARN("read_i16 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<uint64_t>(data);
        }
        break;
      }
      case JBLS_UINT32: {
        uint32_t data = 0;
        if (OB_FAIL(cursor->read_i32(offset, reinterpret_cast<int32_t*>(&data)))) {
          LOG_WARN("read_i32 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<uint64_t>(data);
        }
        break;
      }
      case JBLS_UINT64: {
        uint64_t data = 0;
        if (OB_FAIL(cursor->read_i64(offset, reinterpret_cast<int64_t*>(&data)))) {
          LOG_WARN("read_i64 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<uint64_t>(data);
        }
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

int ObJsonVar::set_var(ObILobCursor *cursor, int64_t offset, uint64_t var, uint8_t type)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cursor)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("output pos is null.", K(ret));
  } else {
    ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
    switch (size) {
      case JBLS_UINT8: {
        ret = cursor->write_i8(offset, var);
        break;
      }
      case JBLS_UINT16: {
        ret = cursor->write_i16(offset, var);
        break;
      }
      case JBLS_UINT32: {
        ret = cursor->write_i32(offset, var);
        break;
      }
      case JBLS_UINT64: {
        ret = cursor->write_i64(offset, var);
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

int ObJsonVar::read_var(const ObILobCursor *cursor, int64_t offset, uint8_t type, int64_t *var)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cursor)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input data is null.", K(ret));
  } else {
    ObJsonBinLenSize size = static_cast<ObJsonBinLenSize>(type);
    switch (size) {
      case JBLS_UINT8: {
        int8_t data = 0;
        if (OB_FAIL(cursor->read_i8(offset, &data))) {
          LOG_WARN("read_i8 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<int64_t>(data);
        }
        break;
      }
      case JBLS_UINT16: {
        int16_t data = 0;
        if (OB_FAIL(cursor->read_i16(offset, &data))) {
          LOG_WARN("read_i16 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<int64_t>(data);
        }
        break;
      }
      case JBLS_UINT32: {
        int32_t data = 0;
        if (OB_FAIL(cursor->read_i32(offset, &data))) {
          LOG_WARN("read_i32 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<int64_t>(data);
        }
        break;
      }
      case JBLS_UINT64: {
        int64_t data = 0;
        if (OB_FAIL(cursor->read_i64(offset, &data))) {
          LOG_WARN("read_i64 fail", K(ret), K(offset), K(type));
        } else {
          *var = static_cast<int64_t>(data);
        }
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

bool ObJsonVar::is_fit_var_type(uint64_t var, uint8_t type)
{
  return ObJsonVar::get_var_type(var) <= type;
}

int ObJsonBin::get_key_entry(int index, uint64_t &key_offset, uint64_t &key_len) const
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  uint64_t offset = get_key_entry_offset(index);
  if (OB_FAIL(ObJsonVar::read_var(cursor_, pos_ + offset, var_type, &key_offset))) {
    LOG_WARN("read key_offset fail", K(ret));
  } else if (OB_FAIL(ObJsonVar::read_var(cursor_, pos_ + offset + ObJsonVar::get_var_size(var_type), var_type, &key_len))) {
    LOG_WARN("read key_len fail", K(ret));
  }
  return ret;
}

int64_t ObJsonBin::get_value_entry_size() const
{
  uint8_t var_type = entry_var_type();
  return 1/*type size*/ + ObJsonVar::get_var_size(var_type) /*offset size*/;
}

int ObJsonBin::get_value_entry(int index, uint64_t &value_offset, uint8_t &value_type) const
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  uint64_t offset = get_value_entry_offset(index);
  if (OB_FAIL(ObJsonVar::read_var(cursor_, pos_ + offset, var_type, &value_offset))) {
    LOG_WARN("read obj_size_ fail", K(ret), K(pos_), K(offset), K(var_type));
  } else if (OB_FAIL(cursor_->read_i8(pos_ + offset + ObJsonVar::get_var_size(var_type), reinterpret_cast<int8_t*>(&value_type)))) {
    LOG_WARN("read_i8 fail", K(ret), K(pos_), K(offset), K(var_type));
  }
  return ret;
}

int ObJsonBin::get_value(int index, ObJsonBin &value) const
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  uint64_t offset = get_value_entry_offset(index);
  uint64_t value_offset = 0;
  uint8_t value_type = 0;
  if (OB_FAIL(get_value_entry(index, value_offset, value_type))) {
    LOG_WARN("get_value_entry fail", K(ret), K(index));;
  } else if (OB_JSON_TYPE_IS_INLINE(value_type)) {
      offset += pos_;
  } else if (is_forward_v0(value_type)) {
    offset = get_extend_value_offset(value_offset);
    if (OB_FAIL(get_extend_value_type(offset, value_type))) {
      LOG_WARN("get_extend_value_type fail", K(ret), K(index), K(value_offset));
    } else if (! need_type_prefix(value_type)) {
      offset += sizeof(uint8_t);
    }
  } else {
    offset = pos_ + value_offset;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reset_child(value, value_type, offset, var_type))) {
    LOG_WARN("reset child value fail", K(ret), K(index), K(value_type), K(pos_), K(value_offset), K(offset));
  }
  return ret;
}

int ObJsonBin::set_key_entry(int index, uint64_t key_offset, uint64_t key_len, bool check)
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  uint64_t offset = get_key_entry_offset(index);
  if (check && ObJsonVar::get_var_type(key_offset) > var_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key_offset var type overflow", K(ret), K(key_offset), K(var_type));
  } else if (check && ObJsonVar::get_var_type(key_len) > var_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key_len var type overflow", K(ret), K(key_len), K(var_type));
  } else if (OB_FAIL(ObJsonVar::set_var(cursor_, pos_ + offset, key_offset, var_type))) {
    LOG_WARN("read key_offset fail", K(ret));
  } else if (OB_FAIL(ObJsonVar::set_var(cursor_, pos_ + offset + ObJsonVar::get_var_size(var_type), key_len, var_type))) {
    LOG_WARN("read key_len fail", K(ret));
  }
  return ret;
}

int ObJsonBin::set_value_entry(int index, uint64_t value_offset, uint8_t value_type, bool check)
{
  INIT_SUCC(ret);
  uint8_t var_type = entry_var_type();
  uint64_t offset = get_value_entry_offset(index);
  if (check && (ObJsonVerType::is_signed_online_integer(value_type) ?
      (ObJsonVar::get_var_type(ObJsonVar::var_uint2int(value_offset, var_type)) > var_type) :
      (ObJsonVar::get_var_type(value_offset) > var_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var type overflow", K(ret), K(value_offset), K(var_type), K(value_type));
  } else if (OB_FAIL(ObJsonVar::set_var(cursor_, pos_ + offset, value_offset, var_type))) {
    LOG_WARN("set_var fail", K(ret));
  } else if (OB_FAIL(cursor_->write_i8(pos_ + offset + ObJsonVar::get_var_size(var_type), value_type))) {
    LOG_WARN("write_i8 fail", K(ret));
  }
  return ret;
}

int ObJsonBin::set_obj_size(uint64_t obj_size)
{
  INIT_SUCC(ret);
  uint64_t offset = get_obj_size_offset();
  if (OB_FAIL(ObJsonVar::set_var(cursor_, pos_ + offset, obj_size, obj_size_var_type()))) {
    LOG_WARN("set_var fail", K(ret));
  } else {
    meta_.set_obj_size(obj_size);
  }
  return ret;
}

int ObJsonBin::set_element_count(uint64_t count)
{
  INIT_SUCC(ret);
  uint64_t offset = get_element_count_offset();
  if (OB_FAIL(ObJsonVar::set_var(cursor_, pos_ + offset, count, element_count_var_type()))) {
    LOG_WARN("set_var fail", K(ret));
  } else {
    meta_.set_element_count(count);
  }
  return ret;
}

int ObJsonBin::parse_doc_header_v0()
{
  INIT_SUCC(ret);
  ObString header_data;
  uint8_t type = 0;
  const ObJsonBinDocHeader *header = nullptr;
  if (OB_FAIL(cursor_->get(pos_, sizeof(ObJsonBinDocHeader), header_data))) {
    LOG_WARN("get header data fail", K(ret), K(pos_));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (OB_FALSE_IT(type = *reinterpret_cast<const uint8_t*>(header_data.ptr()))) {
  } else if (J_DOC_HEADER_V0 != type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type not J_DOC_HEADER_V0", K(ret), K(type));
  } else if (OB_ISNULL(header = reinterpret_cast<const ObJsonBinDocHeader*>(header_data.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get header fail", K(ret), K(header_data));
  } else {
    pos_ = sizeof(ObJsonBinDocHeader);
    ctx_->extend_seg_offset_ = header->extend_seg_offset_;
  }
  return ret;
}


int ObJsonBin::add_doc_header_v0(ObJsonBuffer &buffer)
{
  ObJsonBinDocHeader header;
  return buffer.append(reinterpret_cast<const char*>(&header), sizeof(header));
}

int ObJsonBin::set_doc_header_v0(
    ObJsonBuffer &buffer,
    int64_t extend_seg_offset)
{
  INIT_SUCC(ret);
  ObJsonBinDocHeader *header = nullptr;
  if (buffer.length() < sizeof(ObJsonBinDocHeader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length not enough", K(ret), K(buffer.length()));
  } else if (OB_ISNULL(header = reinterpret_cast<ObJsonBinDocHeader*>(buffer.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header is null", K(ret));
  } else {
    header->extend_seg_offset_ = extend_seg_offset;
  }
  return ret;
}

int ObJsonBin::set_doc_header_v0(
    ObString &buffer,
    int64_t extend_seg_offset)
{
  INIT_SUCC(ret);
  ObJsonBinDocHeader *header = nullptr;
  if (buffer.length() < sizeof(ObJsonBinDocHeader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length not enough", K(ret), K(buffer.length()));
  } else if (OB_ISNULL(header = reinterpret_cast<ObJsonBinDocHeader*>(buffer.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header is null", K(ret));
  } else {
    header->extend_seg_offset_ = extend_seg_offset;
  }
  return ret;
}

static int append_key_to_json_path(const ObString key, ObJsonBuffer &buf)
{
  INIT_SUCC(ret);
  if (key.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key is empty", K(ret));
  } else if (OB_FAIL(buf.append("\"", 1))) {
    LOG_WARN("append quote fail", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < key.length(); ++i) {
    char c = *(key.ptr() + i);
    if (c == '"' && OB_FAIL(buf.append("\\", 1))) {
      LOG_WARN("append slash fail", K(ret), K(i));
    } else if (OB_FAIL(buf.append(&c, 1))) {
      LOG_WARN("append fail", K(ret), K(i), K(c));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buf.append("\"", 1))) {
    LOG_WARN("append quote fail", K(ret));
  }
  return ret;
}

int ObJsonBin::get_json_path_at_iter(int index, ObString &path) const
{
  INIT_SUCC(ret);
  ObJsonBuffer buf(allocator_);

  int size = node_stack_.size();
  ObJBNodeMeta node;
  ObString key;
  ObJsonBin bin;

  if (OB_FAIL(buf.append("$", 1))) {
    LOG_WARN("append $ fail", K(ret));
  }

  for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
    if (OB_FAIL(node_stack_.at(i, node))) {
      LOG_WARN("get node fail", K(ret), K(i), K(size));
    } else if (OB_FAIL(reset_child(bin, node.offset_))) {
      LOG_WARN("reset fail", K(ret));
    } else if (ObJBVerType::J_ARRAY_V0 == OB_JSON_TYPE_GET_INLINE(node.ver_type_)) {
      ObFastFormatInt ffi(node.idx_);
      if (OB_FAIL(buf.append("[", 1))) {
        LOG_WARN("append [ fail", K(ret));
      } else if (OB_FAIL(buf.append(ffi.ptr(), ffi.length()))) {
        LOG_WARN("append idx fail", K(ret), K(node.idx_));
      } else if (OB_FAIL(buf.append("]", 1))) {
        LOG_WARN("append ] fail", K(ret));
      }
    } else if (OB_FAIL(bin.get_key(node.idx_, key))) {
      LOG_WARN("get key fail", K(ret), K(node));
    } else if (OB_FAIL(buf.append(".", 1))) {
      LOG_WARN("append . fail", K(ret), K(key));
    } else if (OB_FAIL(append_key_to_json_path(key, buf))) {
      LOG_WARN("append key fail", K(ret), K(key));
    }
  }

  if (index < 0 ) {
  } else if (ObJBVerType::J_ARRAY_V0 == get_vertype()) {
    ObFastFormatInt ffi(index);
    if (OB_FAIL(buf.append("[", 1))) {
      LOG_WARN("append [ fail", K(ret));
    } else if (OB_FAIL(buf.append(ffi.ptr(), ffi.length()))) {
      LOG_WARN("append idx fail", K(ret), K(index));
    } else if (OB_FAIL(buf.append("]", 1))) {
      LOG_WARN("append ] fail", K(ret));
    }
  } else if (ObJBVerType::J_OBJECT_V0 != get_vertype()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must call at object or array", K(ret), K(get_type()));
  } else if (OB_FAIL(get_key(index, key))) {
    LOG_WARN("get key fail", K(ret), K(index));
  } else if (OB_FAIL(buf.append(".", 1))) {
    LOG_WARN("append . fail", K(ret), K(key));
  } else if (OB_FAIL(append_key_to_json_path(key, buf))) {
    LOG_WARN("append key fail", K(ret), K(key));
  }

  if (OB_SUCC(ret)) {
    buf.get_result_string(path);
  }
  return ret;
}

int ObJsonBin::record_extend_inplace_update_offset(int index, int64_t value_offset, int64_t value_len, uint8_t value_type)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  ObString path;
  ObString new_data;
  ObString value;
  if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctx is null", K(ret), K(index), K(pos_));
  } else if (OB_FAIL(get_json_path_at_iter(index, path))) {
    LOG_WARN("get_json_path_at_iter fail", K(ret));
  } else if (OB_FAIL(cursor_->get(value_offset + (need_type_prefix(value_type) ? 0 : 1), value_len - (need_type_prefix(value_type) ? 0 : 1), new_data))) {
    LOG_WARN("get value data fail", K(ret), K(index), K(value_type));
  } else if (OB_FAIL(ob_write_string(*allocator_, new_data, value))) {
    LOG_WARN("copy value data fail", K(ret), K(value), K(value_offset), K(pos_));
  } else if (OB_FAIL(update_ctx->record_diff(ObJsonDiffOp::REPLACE, value_type, path, value))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(path));
  } else if (OB_FAIL(update_ctx->record_binary_diff(value_offset, value_len))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(path));
  }
  return ret;
}

int ObJsonBin::record_inline_update_offset(int index)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  uint64_t inline_value_offset = get_value_entry_offset(index);
  uint64_t inline_value = 0;
  uint8_t value_type = 0;
  ObString path;
  ObString new_data;
  ObString value;
  if (OB_FAIL(get_value_entry(index, inline_value, value_type))) {
    LOG_WARN("get value entry fail", K(ret), K(index));
  } else if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctx is null", K(ret), K(index), K(pos_));
  } else if (OB_FAIL(get_json_path_at_iter(index, path))) {
    LOG_WARN("get_json_path_at_iter fail", K(ret));
  } else if (OB_FAIL(cursor_->get(pos_ + inline_value_offset, entry_var_size(), new_data))) {
    LOG_WARN("get value data fail", K(ret), K(inline_value), K(value_type));
  } else if (OB_FAIL(ob_write_string(*allocator_, new_data, value))) {
    LOG_WARN("copy value data fail", K(ret), K(value), K(inline_value_offset), K(pos_));
  } else if (OB_FAIL(update_ctx->record_inline_diff(ObJsonDiffOp::REPLACE, value_type, path, entry_var_type(), value))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(inline_value), K(path));
  } else if (OB_FAIL(update_ctx->record_binary_diff(pos_ + inline_value_offset, entry_var_size() + OB_JSON_BIN_VALUE_TYPE_LEN))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(inline_value), K(path));
  }
  return ret;
}

int ObJsonBin::record_inplace_update_offset(int index, ObJsonBin *new_value, bool is_record_header_binary)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  uint64_t value_offset = 0;
  uint64_t value_len = 0;
  uint8_t value_type = 0;
  ObString path;
  ObString new_data;
  ObString value;
  if (OB_FAIL(get_value_entry(index, value_offset, value_type))) {
    LOG_WARN("get value entry fail", K(ret), K(index));
  } else if (OB_FAIL(new_value->get_area_size(value_len))) {
    LOG_WARN("get_area_size fail", K(ret), K(value_type), K(value_offset), K(index));
  } else if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctx is null", K(ret), K(index), K(pos_));
  } else if (OB_FAIL(get_json_path_at_iter(index, path))) {
    LOG_WARN("get_json_path_at_iter fail", K(ret));
  } else if (OB_FAIL(new_value->get_value_binary(new_data))) {
    LOG_WARN("get_value_binary fail", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, new_data, value))) {
    LOG_WARN("copy value data fail", K(ret), K(value_offset), K(value_len));
  } else if (OB_FAIL(update_ctx->record_diff(ObJsonDiffOp::REPLACE, value_type, path, value))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(value_len), K(path));
  } else if (OB_FAIL(update_ctx->record_binary_diff(pos_ + value_offset, value_len))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(value_len), K(path));
  } else if (is_record_header_binary && OB_FAIL(update_ctx->record_binary_diff(
      pos_ + get_value_entry_offset(index),
      entry_var_size() + OB_JSON_BIN_VALUE_TYPE_LEN))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(value_len), K(path));
  }
  return ret;
}

int ObJsonBin::record_append_update_offset(int index, int64_t value_offset, int64_t value_len, uint8_t value_type)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  ObString path;
  ObString new_data;
  ObString value;
  if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctx is null", K(ret), K(index), K(pos_));
  } else if (OB_FAIL(get_json_path_at_iter(index, path))) {
    LOG_WARN("get_json_path_at_iter fail", K(ret));
  } else if (OB_FAIL(cursor_->get(value_offset + (need_type_prefix(value_type) ? 0 : 1), value_len - (need_type_prefix(value_type) ? 0 : 1), new_data))) {
    LOG_WARN("get value data fail", K(ret), K(value_offset), K(value_len));
  } else if (OB_FAIL(ob_write_string(*allocator_, new_data, value))) {
    LOG_WARN("copy value data fail", K(ret), K(value_offset), K(value_len));
  } else if (OB_FAIL(update_ctx->record_diff(ObJsonDiffOp::REPLACE, value_type, path, value))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(value_len), K(path));
  } else if (OB_FAIL(update_ctx->record_binary_diff(value_offset, value_len))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(value_len), K(path));
  } else if (OB_FAIL(update_ctx->record_binary_diff(
      pos_ + get_value_entry_offset(index),
      entry_var_size() + OB_JSON_BIN_VALUE_TYPE_LEN))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(value_type), K(value_offset), K(value_len), K(path));
  }
  return ret;
}

int ObJsonBin::record_remove_offset(int index)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  uint64_t value_entry_end_offset = get_value_entry_offset(this->element_count());
  ObString path;
  if (OB_ISNULL(update_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctx is null", K(ret), K(index), K(pos_));
  } else if (OB_FAIL(get_json_path_at_iter(index, path))) {
    LOG_WARN("get_json_path_at_iter fail", K(ret));
  } else if (OB_FAIL(update_ctx->record_remove_diff(path))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(path));
  } else if (OB_FAIL(update_ctx->record_binary_diff(pos_, value_entry_end_offset))) {
    LOG_WARN("record diff fail", K(ret), K(index), K(pos_), K(path));
  }
  return ret;
}

int ObJsonBin::should_pack_diff(bool &is_should_pack) const
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = get_update_ctx();
  uint64_t total_len = cursor_->get_length();
  uint64_t ext_len = total_len - get_extend_seg_offset();
  uint64_t ori_len = total_len - ext_len;
  if (0 == total_len || 0 == ori_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("binary is empty", K(ret), K(ori_len), K(ori_len), K(ext_len));
  } else if (nullptr == update_ctx || update_ctx->is_rebuild_all_) {
    is_should_pack = false;
  } else if (0 == pos_) {
    // if pos is zero, means no json bin doc header, and means that old json data
    // old json data is clob, can not use chunk size, so can not do partial update
    is_should_pack = false;
  } else if (0 == ext_len || update_ctx->is_no_update()) { // no append update
    is_should_pack = true;
  } else {
    // if ext_len does not exceed 30% of ori_len, should pack diff
    is_should_pack = (((double)(ext_len)) / ori_len) < 0.3;
  }
  return ret;
}

int ObJsonBinMeta::to_header(ObJsonBinHeader &header)
{
  INIT_SUCC(ret);
  header.type_ = get_type();
  header.entry_size_ = entry_var_type();
  header.count_size_ = element_count_var_type();
  header.obj_size_size_ = obj_size_var_type();
  header.is_continuous_ = is_continuous();
  return ret;
}

int ObJsonBinMeta::to_header(ObJsonBuffer &buffer)
{
  INIT_SUCC(ret);
  ObJsonBinHeader header;
  uint64_t key_entry_size = 0;
  uint64_t value_entry_size = 0;
  if (OB_UNLIKELY(ObJsonNodeType::J_OBJECT != json_type() && ObJsonNodeType::J_ARRAY != json_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type not object or array", K(ret), "json type", json_type());
  } else if (OB_FAIL(to_header(header))) {
    LOG_WARN("to_header fail", K(ret));
  } else if (OB_FAIL(buffer.append(reinterpret_cast<char*>(&header), sizeof(header)))) {
    LOG_WARN("append header to buffer fail", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(element_count(), element_count_var_type(), buffer))) {
    LOG_WARN("failed to append array header member count", K(ret));
  } else if (OB_FAIL(ObJsonVar::append_var(obj_size(), obj_size_var_type(), buffer))) {
    LOG_WARN("failed to append array header array size", K(ret));
  } else {
    if (ObJsonNodeType::J_OBJECT == json_type()) {
      key_entry_size = element_count() * (entry_var_size() * 2);
    }
    value_entry_size = element_count() * (entry_var_size() + OB_JSON_BIN_VALUE_TYPE_LEN);
    if (OB_FAIL(buffer.reserve(key_entry_size + value_entry_size))) {
      LOG_WARN("reserve buffer fail", K(ret), K(key_entry_size), K(value_entry_size));
    } else if (OB_FAIL(buffer.set_length(buffer.length() + key_entry_size + value_entry_size))) {
      LOG_WARN("reserve buffer fail", K(ret), K(key_entry_size), K(value_entry_size));
    }
  }
  return ret;
}

uint64_t ObJsonBinMeta::get_obj_size_offset() const
{
  return sizeof(ObJsonBinObjHeader)
      + ObJsonVar::get_var_size(element_count_var_type());
}

uint64_t ObJsonBinMeta::get_element_count_offset() const
{
  return sizeof(ObJsonBinObjHeader);
}

int ObJsonBinMeta::calc_entry_array()
{
  INIT_SUCC(ret);
  key_offset_start_ = sizeof(ObJsonBinObjHeader)
      + ObJsonVar::get_var_size(element_count_var_type())
      + ObJsonVar::get_var_size(obj_size_var_type());
  value_offset_start_ = key_offset_start_;
  if (ObJsonNodeType::J_OBJECT == json_type()) {
    value_offset_start_ += element_count() * (entry_var_size() * 2);
  }
  return ret;
}

uint64_t ObJsonBinMeta::get_value_entry_offset(int index) const
{
  return value_offset_start_ + index * (entry_var_size() + OB_JSON_BIN_VALUE_TYPE_LEN);
}

uint64_t ObJsonBinMeta::get_key_entry_offset(int index) const
{
  return key_offset_start_ + index * (entry_var_size() * 2);
}

uint64_t ObJsonBinMeta::entry_var_size() const
{
  return ObJsonVar::get_var_size(entry_var_type());
}

uint64_t ObJsonBinMeta::obj_size_var_size() const
{
  return ObJsonVar::get_var_size(obj_size_var_type());
}

uint64_t ObJsonBinMeta::element_count_var_size() const
{
  return ObJsonVar::get_var_size(element_count_var_type());
}

int ObJsonBinMetaParser::parse()
{
  INIT_SUCC(ret);
  if (OB_ISNULL(cursor_)) { // skip
  } else if (OB_FAIL(parse_type_())) {
    LOG_WARN("parse type fail", K(ret), K(offset_));
  } else if (OB_FAIL(parse_header_())) {
    LOG_WARN("parse header fail", K(ret), K(offset_));
  }
  return ret;
}

int ObJsonBinMetaParser::parse_type_()
{
  INIT_SUCC(ret);
  // private method, null and length check at caller
  uint8_t header_type = 0;
  if (OB_FAIL(cursor_->read_i8(offset_, reinterpret_cast<int8_t*>(&header_type)))) {
    LOG_WARN("read_i8 fail", K(ret), K(offset_));
  } else {
    meta_.set_type(header_type);
  }
  return ret;
}

int ObJsonBinMetaParser::parse_header_()
{
  INIT_SUCC(ret);
  if (ObJsonNodeType::J_OBJECT != meta_.json_type() && ObJsonNodeType::J_ARRAY != meta_.json_type()) {
  } else if (ObJBVerType::J_OBJECT_V0 == meta_.vertype() || ObJBVerType::J_ARRAY_V0 == meta_.vertype()) {
    ret = parse_header_v0_();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support vertype", K(ret), K(meta_.vertype()));
  }
  return ret;
}

int ObJsonBinMetaParser::parse_header_v0_()
{
  INIT_SUCC(ret);
  ObString header_data;
  const ObJsonBinHeader *header = nullptr;
  uint64_t obj_size = 0;
  uint64_t element_count = 0;
  int offset = offset_;
  if (OB_FAIL(cursor_->get(offset, sizeof(ObJsonBinHeader), header_data))) {
    LOG_WARN("get data fail", K(ret), K(offset));
  } else if (OB_ISNULL(header = reinterpret_cast<const ObJsonBinHeader*>(header_data.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast header is null", K(ret), K(offset));
  } else if (OB_FALSE_IT(offset += sizeof(ObJsonBinHeader))) {
  } else if (OB_FAIL(ObJsonVar::read_var(cursor_, offset, header->count_size_, &element_count))) {
    LOG_WARN("read element_count_ fail", K(ret), KPC(header));
  } else if (OB_FALSE_IT(offset += ObJsonVar::get_var_size(header->count_size_))) {
  } else if (OB_FAIL(ObJsonVar::read_var(cursor_, offset, header->obj_size_size_, &obj_size))) {
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
    }
  }
  return ret;
}

const char *ObJsonBin::get_data() const
{
  INIT_SUCC(ret);
  ObString data;
  if (OB_FAIL(cursor_->get(pos_ + meta_.str_data_offset_, element_count(), data))) {
    LOG_WARN("get data fail", K(ret), K(pos_), K(meta_));
  }
  return data.ptr();
}

int ObJsonBin::get_data(ObString &data)
{
  INIT_SUCC(ret);
  if (OB_FAIL(cursor_->get(pos_ + meta_.str_data_offset_, element_count(), data))) {
    LOG_WARN("get data fail", K(ret), K(pos_), K(meta_));
  }
  return ret;
}

int ObJsonBin::get_extend_value_type(uint64_t offset, uint8_t &value_type) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(cursor_->read_i8(offset, reinterpret_cast<int8_t*>(&value_type)))) {
    LOG_WARN("read_i8 fail", K(ret), K(offset));
  }
  return ret;
}

int ObJsonBin::set_current(const ObString &data, int64_t offset)
{
  INIT_SUCC(ret);
  if (OB_FAIL(cursor_->reset_data(data))) {
    LOG_WARN("reset_data fail", K(ret), K(offset), K(pos_), KPC(cursor_));
  } else {
    pos_ = offset;
  }
  return ret;
}

ObJsonBinCtx::~ObJsonBinCtx()
{
  if (OB_NOT_NULL(update_ctx_) && is_update_ctx_alloc_) {
    update_ctx_->~ObJsonBinUpdateCtx();
    update_ctx_ = nullptr;
  }
}

} // namespace common
} // namespace oceanbase
