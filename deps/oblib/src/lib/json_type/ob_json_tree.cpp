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
 * This file contains implementation support for the json tree abstraction.
 */

#define USING_LOG_PREFIX SQL
#include "ob_json_tree.h"
#include "ob_json_bin.h"
#include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
#include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
#include "lib/charset/ob_dtoa.h" // ob_gcvt_opt
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "lib/charset/ob_charset.h" // for strntod

namespace oceanbase {
namespace common {

struct ObFindFunc {
  ObFindFunc(const ObJsonNode *node) : node_(node) {}

  bool operator()(const ObJsonNode *find)
  {
    return node_ == find;
  }

  const ObJsonNode *node_;
};

bool ObJsonNode::get_boolean() const
{
  return static_cast<const ObJsonBoolean *>(this)->value();
}

double ObJsonNode::get_double() const
{
  return static_cast<const ObJsonDouble *>(this)->value();
}

float ObJsonNode::get_float() const
{
  return static_cast<const ObJsonOFloat *>(this)->value();
}

int64_t ObJsonNode::get_int() const
{
  return static_cast<const ObJsonInt *>(this)->value();
}

uint64_t ObJsonNode::get_uint() const
{
  return static_cast<const ObJsonUint *>(this)->value();
}

const char *ObJsonNode::get_data() const
{
  const char* data;
  ObJsonNodeType type = json_type();
  bool is_string_type = (type == ObJsonNodeType::J_STRING ||
                         type == ObJsonNodeType::J_OBINARY ||
                         type == ObJsonNodeType::J_OOID ||
                         type == ObJsonNodeType::J_ORAWHEX ||
                         type == ObJsonNodeType::J_ORAWID ||
                         type == ObJsonNodeType::J_ODAYSECOND ||
                         type == ObJsonNodeType::J_OYEARMONTH);
  if (is_string_type) {
    data = (static_cast<const ObJsonString *>(this))->value().ptr();
  } else {
    data = (static_cast<const ObJsonOpaque *>(this))->value();
  }
  return data;
}

uint64_t ObJsonNode::get_data_length() const
{
  size_t len;
  ObJsonNodeType type = json_type();
  bool is_string_type = (type == ObJsonNodeType::J_STRING ||
                         type == ObJsonNodeType::J_OBINARY ||
                         type == ObJsonNodeType::J_OOID ||
                         type == ObJsonNodeType::J_ORAWHEX ||
                         type == ObJsonNodeType::J_ORAWID ||
                         type == ObJsonNodeType::J_ODAYSECOND ||
                         type == ObJsonNodeType::J_OYEARMONTH);
  if (is_string_type) {
    len = (static_cast<const ObJsonString *>(this))->length();
  } else {
    len = (static_cast<const ObJsonOpaque *>(this))->size();
  }
  return len;
}

number::ObNumber ObJsonNode::get_decimal_data() const
{
  return static_cast<const ObJsonDecimal *>(this)->value();
}

ObPrecision ObJsonNode::get_decimal_precision() const
{
  return static_cast<const ObJsonDecimal *>(this)->get_precision();
}

ObScale ObJsonNode::get_decimal_scale() const
{
  return static_cast<const ObJsonDecimal *>(this)->get_scale();
}

int ObJsonNode::get_obtime(ObTime &t) const
{
  t = static_cast<const ObJsonDatetime *>(this)->value();
  return OB_SUCCESS; // adapt json binary, so return OB_SUCCESS directly.
}


int ObJsonNode::check_valid_object_op(ObIJsonBase *value) const
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is null", K(ret));
  } else if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(json_type()));
  } else if (value->is_bin()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is json bin, not supported", K(ret), K(*value));
  }

  return ret;
}

int ObJsonNode::check_valid_array_op(ObIJsonBase *value) const
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is NULL", K(ret));
  } else if (json_type() != ObJsonNodeType::J_ARRAY) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(json_type()));
  } else if (value->is_bin()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is json bin, not supported", K(ret), K(*value));
  } 

  return ret;
}

int ObJsonNode::check_valid_object_op(uint64_t index) const
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

int ObJsonNode::check_valid_array_op(uint64_t index) const
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

int ObJsonNode::object_add(const common::ObString &key, ObIJsonBase *value)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_object_op(value))) {
    LOG_WARN("invalid json object operation", K(ret), K(key));
  } else {
    ObJsonObject *j_obj = static_cast<ObJsonObject *>(this);
    if (OB_FAIL(j_obj->add(key, static_cast<ObJsonNode *>(value)))) {
      LOG_WARN("fail to add value to object by key", K(ret), K(key));
    }
  }

  return ret;
}

int ObJsonNode::array_insert(uint64_t index, ObIJsonBase *value)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_array_op(value))) {
    LOG_WARN("invalid json array operation", K(ret), K(index));
  } else {
    ObJsonArray *j_arr = static_cast<ObJsonArray *>(this);
    if (OB_FAIL(j_arr->insert(index, static_cast<ObJsonNode *>(value)))) {
      LOG_WARN("fail to insert value to array", K(ret), K(index));
    }
  }
  
  return ret;
}

int ObJsonNode::array_append(ObIJsonBase *value)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_array_op(value))) {
    LOG_WARN("invalid json array operation", K(ret));
  } else {
    ObJsonArray *j_arr = static_cast<ObJsonArray *>(this);
    if (OB_FAIL(j_arr->append(static_cast<ObJsonNode *>(value)))) {
      LOG_WARN("fail to append value to array", K(ret));
    }
  }

  return ret;
}

int ObJsonNode::merge_tree(ObIAllocator *allocator, ObIJsonBase *other, ObIJsonBase *&result)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator) || OB_ISNULL(other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(allocator), KP(other));
  } else if (json_type() == ObJsonNodeType::J_OBJECT && other->json_type() == ObJsonNodeType::J_OBJECT) {
    ObJsonObject *this_obj = static_cast<ObJsonObject *>(this);
    ObJsonObject *other_obj = static_cast<ObJsonObject *>(other);
    if (OB_FAIL(this_obj->consume(allocator, other_obj))) {
      LOG_WARN("fail to consume object", K(ret), K(*this_obj), K(*other_obj));
    } else {
      result = this_obj;
    }
  } else {
    void *buf = NULL;
    ObJsonArray *this_arr = static_cast<ObJsonArray *>(this);
    ObJsonArray *other_arr = static_cast<ObJsonArray *>(other);
    if (json_type() != ObJsonNodeType::J_ARRAY) { // not array, maybe object or other
      buf = allocator->alloc(sizeof(ObJsonArray));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObJsonArray", K(ret), K(sizeof(ObJsonArray)));
      } else {
        this_arr = new (buf) ObJsonArray(allocator);
        if (OB_FAIL(this_arr->append(this))) {
          LOG_WARN("fail to append this array element", K(ret), K(json_type()));
        }
      }
    }
    
    if (OB_SUCC(ret) && other->json_type() != ObJsonNodeType::J_ARRAY) { // not array, maybe object or other
      buf = allocator->alloc(sizeof(ObJsonArray));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc right ObJsonArray", K(ret), K(sizeof(ObJsonArray)));
      } else {
        other_arr = new (buf) ObJsonArray (allocator);
        if (OB_FAIL(other_arr->append(static_cast<ObJsonNode *>(other)))) {
          LOG_WARN("fail to append other array element", K(ret), K(other->json_type()));
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      if (this_arr->consume(allocator, other_arr)) {
        LOG_WARN("fail to consume array", K(ret), K(*this_arr), K(*other_arr));
      } else {
        result = this_arr;
      }
    }
  }
  
  return ret;
}

int ObJsonNode::get_location(ObJsonBuffer &path) const
{
  INIT_SUCC(ret);
  
  if (OB_ISNULL(parent_)) {
    ret = path.append("$");
  } else if (OB_FAIL(parent_->get_location(path))) {
    LOG_WARN("failed to get parent location", K(ret));
  } else if (parent_->json_type() == ObJsonNodeType::J_OBJECT) {
    ObJsonObject *j_obj = static_cast<ObJsonObject *>(parent_);
    uint64_t size = j_obj->element_count();
    bool is_found = false;
    for (uint64_t i = 0; i < size && !is_found; i++) {
      if (j_obj->get_value(i) == this) {
        is_found = true;
        ObString key;
        if (OB_FAIL(j_obj->get_key(i, key))) {
          LOG_WARN("get key failed", K(ret), K(i));
        } else if (OB_FAIL(path.append("."))) {
          LOG_WARN("path append . failed", K(ret));
        } else if (OB_FAIL(path.append(key))) {
          LOG_WARN("path append key failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("parent not found this sub node", K(ret));
    }
  } else if (parent_->json_type() == ObJsonNodeType::J_ARRAY) {
    ObJsonArray *j_arr = static_cast<ObJsonArray*>(parent_);
    uint64_t size = j_arr->element_count();
    bool is_found = false;
    for (uint64_t i = 0; i < size && !is_found; i++) {
      if ((*j_arr)[i] == this) {
        is_found = true;
        char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
        char *ptr = NULL;
        if (OB_ISNULL(ptr = ObCharset::lltostr(i, res_ptr, 10, 1))) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("fail to transform the index(lltostr)", K(ret));
        } else if (OB_FAIL(path.append("["))) {
          LOG_WARN("path append [ failed", K(ret));
        } else if (OB_FAIL(path.append(res_ptr, static_cast<int32_t>(ptr - res_ptr)))) {
          LOG_WARN("fail to append the index", K(ret));
        } else if (OB_FAIL(path.append("]"))) {
          LOG_WARN("path append ] failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      ret = OB_ERR_INTERVAL_INVALID;
      LOG_WARN("parent not found this sub node", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected json type for get path location", K(ret), K(parent_->json_type()));
  }

  return ret;
}

int ObJsonNode::replace(const ObIJsonBase *old_node, ObIJsonBase *new_node)
{
  INIT_SUCC(ret);
  
  if (OB_ISNULL(old_node) || OB_ISNULL(new_node)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null param", K(ret), KP(old_node), KP(new_node));
  } else if (old_node->is_bin() || new_node->is_bin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input is binary, but need tree", K(ret), K(old_node), K(new_node));
  } else if (json_type() != ObJsonNodeType::J_ARRAY && json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this json type unsupport replace", K(ret), K(json_type()));
  } else {
    const ObJsonNode *j_old_node = static_cast<const ObJsonNode *>(old_node);
    ObJsonNode *j_new_node = static_cast<ObJsonNode *>(new_node);
    if (json_type() == ObJsonNodeType::J_OBJECT) {
      ObJsonObject *j_obj = static_cast<ObJsonObject *>(this);
      if (OB_FAIL(j_obj->replace(j_old_node, j_new_node))) {
        LOG_WARN("fail to replace in json object", K(ret), K(j_old_node), K(j_new_node));
      }
    } else { // ObJsonNodeType::J_ARRAY
      ObJsonArray *j_arr = static_cast<ObJsonArray *>(this);
      if (OB_FAIL(j_arr->replace(j_old_node, j_new_node))) {
        LOG_WARN("fail to replace in json array", K(ret), K(j_old_node), K(j_new_node));
      }
    }
  } 
  
  return ret;
}

int ObJsonNode::object_remove(const common::ObString &key)
{
  INIT_SUCC(ret);

  if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node type", K(ret), K(json_type()));
  } else {
    ObJsonObject *j_obj = static_cast<ObJsonObject *>(this);
    if (OB_FAIL(j_obj->remove(key))) {
      LOG_WARN("fail to remove value in object", K(ret), K(key));
    }
  }

  return ret;
}

int ObJsonNode::array_remove(uint64_t index)
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_array_op(index))) {
    LOG_WARN("invalid json array operation", K(ret), K(index));
  } else {
    ObJsonArray *j_arr = static_cast<ObJsonArray *>(this);
    if (OB_FAIL(j_arr->remove(index))) {
      LOG_WARN("fail to remove value in array", K(ret), K(index));
    }
  }

  return ret;
}

int ObJsonNode::get_key(uint64_t index, common::ObString &key_out) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(check_valid_object_op(index))) {
    LOG_WARN("invalid json object operation", K(ret), K(index));
  } else {
    const ObJsonObject *j_obj = static_cast<const ObJsonObject *>(this);
    if (OB_FAIL(j_obj->get_key(index, key_out))) {
      LOG_WARN("fail to get object key", K(ret), K(index));
    }
  }

  return ret;
}

int ObJsonNode::get_array_element(uint64_t index, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  value = NULL;

  if (OB_FAIL(check_valid_array_op(index))) {
    LOG_WARN("invalid json array operation", K(ret), K(index));
  } else {
    const ObJsonArray *j_arr = static_cast<const ObJsonArray *>(this);
    value = (*j_arr)[index];
  }

  return ret;
}

int ObJsonNode::get_object_value(uint64_t index, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  value = NULL;

  if (OB_FAIL(check_valid_object_op(index))) {
    LOG_WARN("invalid json object operation", K(ret), K(index));
  } else {
    const ObJsonObject *j_obj = static_cast<const ObJsonObject *>(this);
    if (OB_ISNULL(value = j_obj->get_value(index))) { // maybe not found.
      ret = OB_SEARCH_NOT_FOUND;
      LOG_INFO("not found value by index", K(ret), K(index));
    }
  }

  return ret;
}

int ObJsonNode::get_object_value(uint64_t index, ObString &key, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  value = NULL;

  if (OB_FAIL(check_valid_object_op(index))) {
    LOG_WARN("invalid json object operation", K(ret), K(index));
  } else {
    const ObJsonObject *j_obj = static_cast<const ObJsonObject *>(this);
    ObJsonNode* node  = nullptr;
    if (OB_FAIL(j_obj->get_value_by_idx(index, key, node))) {
      LOG_WARN("fail to find value by index", K(ret), K(index));
    } else if (OB_ISNULL(value = node)) { // maybe not found.
      ret = OB_SEARCH_NOT_FOUND;
      LOG_WARN("not found value by index", K(ret), K(index));
    }
  }

  return ret;
}

int ObJsonNode::get_object_value(const ObString &key, ObIJsonBase *&value) const
{
  INIT_SUCC(ret);
  value = NULL;

  if (json_type() != ObJsonNodeType::J_OBJECT) { // check json node type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json node type", K(ret), K(json_type()));
  } else {
    const ObJsonObject *j_obj = static_cast<const ObJsonObject *>(this);
    if (OB_ISNULL(value = j_obj->get_value(key))) { // maybe not found.
      ret = OB_SEARCH_NOT_FOUND;
      LOG_DEBUG("not found value by key", K(ret), K(key));
    }
  }

  return ret;
}

// "{ \"k1\" : 1, \"k2\" : \"a\" }"
// [commen_head][count][object_size][key0_offset][key0_length][key1_offset][key1_length]...
// [keyn_offset][keyn_length][value0_offset][type0][value1_offset][type1]...[valuen_offset][typen]
// [key0][key1]...[keyn][value0][value1]...[valuen]
void ObJsonObject::update_serialize_size(int64_t change_size)
{
  INIT_SUCC(ret);

  if (change_size != 0) {
    serialize_size_ += change_size;
  } else {
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

    // Scenario inline is not considered
    for (uint32_t i = 0; i < count && OB_SUCC(ret); i++) {
      const ObJsonObjectPair *obj_pair = &object_array_[i];
      ObString key = obj_pair->get_key();
      key_size += key.length();
      key_length_size_type = ObJsonVar::get_var_type(static_cast<uint64_t>(key.length()));
      key_length_size += ObJsonVar::get_var_size(key_length_size_type);
      ObJsonNode *value = obj_pair->get_value();
      if (OB_NOT_NULL(value)) {
        node_size = value->get_serialize_size();
        value_size += node_size;
      }
    }

    if (OB_SUCC(ret)) {
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
      serialize_size_ = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + object_size
          + total_offset_size + key_size + value_size);
    }
  }
}

ObJsonNode *ObJsonObject::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  INIT_SUCC(ret);

  ObJsonNode *new_node = ObJsonTreeUtil::clone_new_node<ObJsonObject>(allocator, allocator);
  if (OB_ISNULL(new_node)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("create new json obj failed", K(ret));
  } else {
    ObJsonObject *new_obj = static_cast<ObJsonObject *>(new_node);
    uint64_t len = element_count();
    ObString key_str;
    for (uint64_t i = 0; i < len && OB_SUCC(ret); i++) {
      if (is_deep_copy) {
        char* str_buf = NULL;
        bool is_key_empty = object_array_[i].get_key().length() == 0;
        if (!is_key_empty && OB_ISNULL(str_buf = static_cast<char*>(allocator->alloc(object_array_[i].get_key().length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(object_array_[i].get_key().length()));
        } else {
          key_str.assign_buffer(str_buf, object_array_[i].get_key().length());
          if (object_array_[i].get_key().length() !=
                key_str.write(object_array_[i].get_key().ptr(), object_array_[i].get_key().length())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get text from expr", K(ret), K(key_str));
          }
        }
      } else {
        key_str.assign_ptr(object_array_[i].get_key().ptr(), object_array_[i].get_key().length());
      }
      if (OB_SUCC(ret)) {
        ObJsonNode *old_value = object_array_[i].get_value();
        if (OB_FAIL(new_obj->add(key_str, old_value->clone(allocator, is_deep_copy)))) {
          LOG_WARN("add obj failed", K(ret), K(object_array_[i].get_key()));
        }
      }
    }
  }
  return ret != OB_SUCCESS ? NULL : new_node;
}

int ObJsonObject::get_key(uint64_t index, common::ObString &key_out) const
{
  INIT_SUCC(ret);

  if (index >= object_array_.size()) {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("index out of range", K(ret), K(index), K(object_array_.size()));
  } else {
    key_out = object_array_[index].get_key();
  }

  return ret;
}

ObJsonNode *ObJsonObject::get_value(const common::ObString &key) const
{
  ObJsonNode *j_node = NULL;
  ObJsonObjectPair pair(key, NULL);
  ObJsonKeyCompare cmp;

  ObJsonObjectArray::const_iterator low_iter = std::lower_bound(object_array_.begin(),
                                                                object_array_.end(),
                                                                pair, cmp);
  if (low_iter != object_array_.end() && low_iter->get_key() == key) {
    j_node = low_iter->get_value();
  }

  return j_node;
}

ObJsonNode *ObJsonObject::get_value(uint64_t index) const
{
  ObJsonNode *j_node = NULL;

  if (index < object_array_.size()) {
    j_node = object_array_[index].get_value();
  }

  return j_node;
}

int ObJsonObject::get_key_by_idx(uint64_t index, ObString& key) const
{
  INIT_SUCC(ret);
  if (index < object_array_.size()) {
    key = object_array_[index].get_key();
  } else {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("fail to get json node", K(ret), K(index));
  }
  return ret;
}

int ObJsonObject::get_value_by_idx(uint64_t index, ObString& key, ObJsonNode*& value) const
{
  INIT_SUCC(ret);
  if (index < object_array_.size()) {
    value = object_array_[index].get_value();
    key = object_array_[index].get_key();
  } else {
    ret = OB_OUT_OF_ELEMENT;
    LOG_WARN("fail to get json node", K(ret), K(index));
  }
  return ret;
}

int ObJsonObject::remove(const common::ObString &key)
{
  INIT_SUCC(ret);
  const ObJsonObjectPair pair(key, NULL);
  ObJsonKeyCompare cmp;
  ObJsonObjectArray::iterator low_iter = std::lower_bound(object_array_.begin(),
                                                          object_array_.end(), pair, cmp);
  if (low_iter != object_array_.end() && low_iter->get_key() == key) {
    int64_t delta_size = low_iter->get_value()->get_serialize_size();
    if (OB_FAIL(object_array_.remove(low_iter - object_array_.begin()))) {
      LOG_WARN("fail to remove json node", K(ret), K(key));
    } else {
      set_serialize_delta_size(-1 * delta_size);
    }
  }

  return ret;
}

int ObJsonObject::replace(const ObJsonNode *old_node, ObJsonNode *new_node)
{
  INIT_SUCC(ret);
  bool is_found = false;

  if (OB_ISNULL(old_node) || OB_ISNULL(new_node)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(old_node), KP(new_node));
  } else {
    for (ObJsonObjectArray::iterator it = object_array_.begin();
        it != object_array_.end() && !is_found; ++it) {
      if (it->get_value() == old_node) {
        it->set_value(new_node);
        new_node->set_parent(this);
        new_node->update_serialize_size_cascade();
        is_found = true;
      }
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    ret = OB_SEARCH_NOT_FOUND;
  }

  return ret;
}

// When constructing a JSON tree, if two keys have the same value, 
// the latter one will overwrite the former one
int ObJsonObject::add(const common::ObString &key, ObJsonNode *value, bool with_unique_key, bool is_lazy_sort, bool need_overwrite, bool is_schema)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is NULL", K(ret));
  } else {
    value->set_parent(this);
    ObJsonObjectPair pair(key, value);

    if (is_schema) {
      // if is schema, keep the first value, don't raise error or overwrite
      ObJsonKeyCompare cmp;
      ObJsonObjectArray::iterator low_iter = std::lower_bound(object_array_.begin(),
                                                              object_array_.end(), pair, cmp);
      if (low_iter != object_array_.end() && low_iter->get_key() == key) { // Found and covered
        // do nothing
      } else if (OB_FAIL(object_array_.push_back(pair))) {
        LOG_WARN("failed to store in object array.", K(ret));
      } else {
        sort();
      }
    } else if (need_overwrite) {
      ObJsonKeyCompare cmp;
      ObJsonObjectArray::iterator low_iter = std::lower_bound(object_array_.begin(),
                                                              object_array_.end(), pair, cmp);
      if (low_iter != object_array_.end() && low_iter->get_key() == key) { // Found and covered
        if (with_unique_key) {
          ret = OB_ERR_DUPLICATE_KEY;
          LOG_WARN("Found duplicate key inserted before!", K(key), K(ret));
        } else {
          low_iter->set_value(value);
        }
      } else if (OB_FAIL(object_array_.push_back(pair))) {
        LOG_WARN("failed to store in object array.", K(ret));
      } else if (!is_lazy_sort) {
        sort();
      }
    } else if (OB_FAIL(object_array_.push_back(pair))) {  // if don't check unique key, push directly
      LOG_WARN("failed to store in object array.", K(ret));
    } else if (!is_lazy_sort) {
      sort();
    }
    set_serialize_delta_size(value->get_serialize_size());
  }

  return ret;
}

int ObJsonObject::rename_key(const common::ObString &old_key, const common::ObString &new_key){
  INIT_SUCC(ret);

  if (new_key.empty() || old_key.empty()) {
    ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
    LOG_WARN("key is NULL", K(ret), K(new_key), K(old_key));
  } else {
    ObJsonObjectPair pair(old_key, NULL);
    ObJsonKeyCompare cmp;
    ObJsonObjectArray::iterator low_iter = std::lower_bound(object_array_.begin(),
                                                            object_array_.end(), pair, cmp);
    if (low_iter != object_array_.end() && low_iter->get_key() == old_key) { // Found and covered
      if (OB_ISNULL(get_value(new_key))) {
        low_iter->set_key(new_key);
        sort();
      } else {
        ret = OB_ERR_DUPLICATE_KEY;
        LOG_WARN("duplicated key in object array.", K(ret), K(old_key), K(new_key));
      }
    } else {
      ret = OB_ERR_JSON_KEY_NOT_FOUND;
      LOG_WARN("JSON key name not found.", K(ret), K(old_key));
    }
  }

  return ret;
}

void ObJsonObject::sort()
{
  ObJsonKeyCompare cmp;
  lib::ob_sort(object_array_.begin(), object_array_.end(), cmp);
}

void ObJsonObject::stable_sort()
{
  ObJsonKeyCompare cmp;
  std::stable_sort(object_array_.begin(), object_array_.end(), cmp);
}

void ObJsonObject::unique()
{
  int64_t pos = 1;
  int64_t cur = 0;
  int64_t last = object_array_.count();

  for (; pos < last; pos++) {
    ObJsonObjectPair& cur_ref = object_array_[cur];
    ObJsonObjectPair& pos_ref = object_array_[pos];

    common::ObString cur_key = cur_ref.get_key();
    common::ObString pos_key = pos_ref.get_key();

    if (cur_key.length() == pos_key.length() && cur_key.compare(pos_key) == 0) {
      cur_ref = pos_ref;
    } else {
      cur++;
      if (cur != pos) {
        object_array_[cur] = pos_ref;
      }
    }
  }

  while (++cur < last) {
    object_array_.pop_back();
  }

}

void ObJsonObject::clear()
{
  object_array_.destroy();
  update_serialize_size_cascade();
}

int ObJsonObject::consume(ObIAllocator *allocator, ObJsonObject *other)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator) || OB_ISNULL(other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(allocator), KP(other));
  } else {
    uint64_t count = other->element_count();
    common::ObString other_key;
    ObJsonNode *other_value = NULL;
    ObJsonNode *this_value = NULL;
    for (uint64_t i = 0; i < count && OB_SUCC(ret); i++) {
      if (OB_FAIL(other->get_key(i, other_key))) {
        LOG_WARN("fail to get other key", K(ret), K(i));
      } else if (OB_NOT_NULL(this_value = get_value(other_key))) { // Duplicate key. Merge the values.
        ObIJsonBase *res = NULL;
        other_value = other->get_value(other_key);
        if (OB_FAIL(this_value->merge_tree(allocator, other_value, res))) {
          LOG_WARN("fail to merge tree", K(ret), K(*this_value), K(*other_value));
        } else if (OB_FAIL(replace(this_value, static_cast<ObJsonNode *>(res)))) {
          LOG_WARN("fail to replace json node", K(ret), K(*this_value), K(*res));
        }
      } else {
        other_value = other->get_value(other_key);
        if (OB_NOT_NULL(other_value)) {
          if (OB_FAIL(add(other_key, other_value))) {
            LOG_WARN("fail to add object", K(ret), K(other_key), K(*other_value));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    other->clear();
  }

  return ret;
}

int ObJsonObject::merge_patch(ObIAllocator *allocator, ObJsonObject *patch_obj)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator) || OB_ISNULL(patch_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(allocator), KP(patch_obj));
  } else {
    uint64_t count = patch_obj->element_count();
    ObJsonNode *j_patch_node = NULL;
    common::ObString key;
    for (uint64_t i = 0; i < count && OB_SUCC(ret); i++) {
      j_patch_node = patch_obj->get_value(i);
      if (OB_ISNULL(j_patch_node)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("json node is null", K(ret), K(i));
      } else if (OB_FAIL(patch_obj->get_key(i, key))) {
        LOG_WARN("fail to get key", K(ret), K(i));
      } else if (j_patch_node->json_type() == ObJsonNodeType::J_NULL) {
        if (OB_FAIL(remove(key))) {
          LOG_WARN("fail to remove element", K(ret), K(key));
        }
      } else if (j_patch_node->json_type() != ObJsonNodeType::J_OBJECT) {
        if (OB_FAIL(add(key, j_patch_node))) {
          LOG_WARN("fail to add patch node to current object", K(ret), K(key), K(*j_patch_node));
        }
      } else { // j_patch_node->json_type() == ObJsonNodeType::J_OBJECT
        bool need_new_object = false;
        ObJsonNode *j_node = NULL;
        ObJsonNode *j_this_node = get_value(key);
        if (OB_ISNULL(j_this_node)) {
          need_new_object = true;
        } else if (j_this_node->json_type() != ObJsonNodeType::J_OBJECT) {
          need_new_object = true;
        }

        if (need_new_object) {
          void *buf = allocator->alloc(sizeof(ObJsonObject));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory for ObJsonObject", K(ret));
          } else {
            j_node = new(buf)ObJsonObject(allocator);
          }
        } else {
          j_node = j_this_node;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(static_cast<ObJsonObject *>(j_node)->merge_patch(allocator, 
              static_cast<ObJsonObject *>(j_patch_node)))) {
            LOG_WARN("fail to merge path", K(ret), K(key));
          } else if (OB_FAIL(add(key, j_node))) {
            LOG_WARN("fail to add object", K(ret), K(key));
          }
        }
      }
    }
  }

  return ret;
}

// [commen_head][count][array_size][node0_offset][node0_type][node1_offset][node1_type]...
// [noden_offset][noden_type][node0][node1]...[noden]
void ObJsonArray::update_serialize_size(int64_t change_size)
{
  INIT_SUCC(ret);

  if (change_size != 0) {
    serialize_size_ += change_size;
  } else {
    static const uint64_t ESTIMATE_ARRAY_SIZE = sizeof(uint32_t);
    static const uint64_t ESTIMATE_OFFSET_SIZE = sizeof(uint32_t);
    static const uint64_t TYPE_SIZE = sizeof(uint8_t);
    uint64_t count = element_count();
    uint64_t node_total_size = 0;
    uint64_t node_size = 0;

    for (uint32_t i = 0; i < count && OB_SUCC(ret); i++) {
      if (OB_NOT_NULL((*this)[i])) {
        node_size = (*this)[i]->get_serialize_size();
        node_total_size += node_size;
      }
    }

    if (OB_SUCC(ret)) {
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
      serialize_size_ = (OB_JSON_BIN_OBJ_HEADER_LEN + count_size + array_size
          + total_offset_size + node_total_size);
    }
  }
}

ObJsonNode *ObJsonArray::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  INIT_SUCC(ret);

  ObJsonNode *new_node = ObJsonTreeUtil::clone_new_node<ObJsonArray>(allocator, allocator);
  if (OB_ISNULL(new_node)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("create new json array failed", K(ret));
  } else {
    ObJsonArray *new_array = static_cast<ObJsonArray *>(new_node);
    uint64_t size = element_count();
    for (uint64_t i = 0; i < size && OB_SUCC(ret); i++) {
      if (OB_FAIL(new_array->append(node_vector_[i]->clone(allocator, is_deep_copy)))) {
        LOG_WARN("array append clone failed", K(ret), K(i), K(size));
      }
    }
  }

  return ret != OB_SUCCESS ? NULL : new_node;
}

int ObJsonArray::remove(uint64_t index)
{
  int ret = OB_ERROR_OUT_OF_RANGE;
  if (index < node_vector_.size()) {
    int64_t delta_size = node_vector_[index]->get_serialize_size();
    if (OB_FAIL(node_vector_.remove(index))) {
      LOG_WARN("fail to remove json node from array", K(ret), K(index));
    } else {
      set_serialize_delta_size(-1 * delta_size);
      ret = OB_SUCCESS;
    } 
  }
  return ret;
}
ObJsonNode *ObJsonArray::operator[](uint64_t index) const
{
  ObJsonNode *node = NULL;

  if (index >= element_count()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "index is out of range", K(index));
  } else if (node_vector_[index]->get_parent() != this) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected parent json node", K(index));
  } else {
    node = node_vector_[index];
  }

  return node;
}

int ObJsonArray::replace(const ObJsonNode *old_node, ObJsonNode *new_node)
{
  INIT_SUCC(ret);
  bool is_found = false;

  if (OB_ISNULL(old_node) || OB_ISNULL(new_node)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(old_node), KP(new_node));
  } else {
    ObFindFunc func(old_node);
    ObJsonNodeVector::iterator it = std::find_if(node_vector_.begin(), node_vector_.end(), func);
    if (it != node_vector_.end()) {
      *it = new_node;
      new_node->set_parent(this);
      new_node->update_serialize_size_cascade();
      is_found = true;
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    ret = OB_SEARCH_NOT_FOUND;
  }

  return ret;                                                     
}

int ObJsonArray::append(ObJsonNode *value)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is NULL", K(ret));
  } else {
    value->set_parent(this);
    if (OB_FAIL(node_vector_.push_back(value))) {
      LOG_WARN("fail to push back value", K(ret));
    } else {
      value->update_serialize_size_cascade(value->get_serialize_size());
    }
  }

  return ret;
}

int ObJsonArray::insert(uint64_t index, ObJsonNode *value)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(value)) { // check param
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param value is NULL", K(ret));
  } else {
    ObJsonNodeVector::iterator pos = index > node_vector_.size() ?
                                     node_vector_.end() : node_vector_.begin() + index;
    value->set_parent(this);
    if (OB_FAIL(node_vector_.insert(pos, value))) {
      LOG_WARN("fail to insert node to json array", K(ret), K(pos));
    } else {
      value->update_serialize_size_cascade(value->get_serialize_size());
    }
  }

  return ret;
}

void ObJsonArray::clear()
{
  node_vector_.clear();
  update_serialize_size_cascade();
}

int ObJsonArray::consume(ObIAllocator *allocator, ObJsonArray *other)
{
  UNUSED(allocator);
  INIT_SUCC(ret);
  uint64_t size = other->element_count();

  for (uint64_t i = 0; i < size && OB_SUCC(ret); i++){
    if (OB_FAIL(append((*other)[i]))) {
      LOG_WARN("fail to append value", K(ret), K(size), K(i), K((*other)[i]));
    }
  }

  if (OB_SUCC(ret)) {
    other->clear();
    update_serialize_size();
  }

  return ret;
}

ObJsonDatetime::ObJsonDatetime(const ObTime &time, ObObjType field_type)
      : ObJsonScalar(),
        value_(time)
{
  field_type_ = field_type;
  json_type_ = ObJsonNodeType::J_ERROR;
  if (field_type == ObDateType) {
    json_type_ = lib::is_mysql_mode() ? ObJsonNodeType::J_DATE : ObJsonNodeType::J_ORACLEDATE;
  } else if (field_type == ObDateTimeType) {
    json_type_ = ObJsonNodeType::J_DATETIME;
  } else if (field_type == ObTimestampType) {
    json_type_ = lib::is_mysql_mode() ? ObJsonNodeType::J_TIMESTAMP : ObJsonNodeType::J_OTIMESTAMP;
  } else if (field_type == ObTimestampTZType) {
    json_type_ = ObJsonNodeType::J_OTIMESTAMPTZ;
  } else if (field_type == ObTimeType) {
    json_type_ = ObJsonNodeType::J_TIME;
  }
}

ObJsonDatetime::ObJsonDatetime(ObJsonNodeType type, const ObTime &time)
    : ObJsonScalar()
{
  // how about oracle if we use mysql type in JsonScalar?
  // ToDo: mapping for types instead switch
  json_type_ = type;
  field_type_ = ObJsonBaseUtil::get_time_type(type);
  value_ = time;
}

template <typename T, typename... Args>
ObJsonNode *ObJsonTreeUtil::clone_new_node(ObIAllocator* allocator, Args &&... args)
{
  void *buf = allocator->alloc(sizeof(T));
  T *new_node = NULL;

  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for ObJsonNode");
  } else {
    new_node = new(buf)T(std::forward<Args>(args)...);
  }

  return static_cast<ObJsonNode *>(new_node);
}

int ObJsonOInterval::parse()
{
  int ret = OB_SUCCESS;
  if (field_type_ == ObIntervalYMType) {
    ObIntervalYMValue value;
    ObScale scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale();
    if ((NULL == str_val_.find('P')) ? //有P的是ISO格式
            OB_FAIL(ObTimeConverter::str_to_interval_ym(str_val_, value, scale))
          : OB_FAIL(ObTimeConverter::iso_str_to_interval_ym(str_val_, value))) {
      LOG_WARN("fail to convert string", K(ret), K(str_val_));
    } else {
      val_.ym_ = value;
    }
  } else {
    ObIntervalDSValue value;
    ObScale scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    if ((NULL == str_val_.find('P')) ? //有P的是ISO格式
            OB_FAIL(ObTimeConverter::str_to_interval_ds(str_val_, value, scale))
          : OB_FAIL(ObTimeConverter::iso_str_to_interval_ds(str_val_, value))) {
      LOG_WARN("fail to convert string", K(ret), K(str_val_));
    } else {
      val_.ds_ = value;
    }
  }
  return ret;
}

ObJsonNode *ObJsonDecimal::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonDecimal>(allocator, value(), get_precision(), get_scale());
}

ObJsonNode *ObJsonDouble::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonDouble>(allocator, value());
}

ObJsonNode *ObJsonOFloat::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonOFloat>(allocator, value());
}

ObJsonNode *ObJsonInt::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonInt>(allocator, value());
}

ObJsonNode *ObJsonUint::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonUint>(allocator, value());
}

ObJsonNode *ObJsonString::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  ObJsonNode* str_node = ObJsonTreeUtil::clone_new_node<ObJsonString>(allocator, value().ptr(), length());
  if (OB_NOT_NULL(str_node)) {
    (static_cast<ObJsonString*>(str_node))->set_ext(ext_);
  }
  if (is_deep_copy) {
    char* str_buf =NULL;
    if (OB_ISNULL(str_buf = static_cast<char*>(allocator->alloc(length())))) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
    } else {
      ObString key_str(length(), 0, str_buf);
      if (length() != key_str.write(value().ptr(), length())) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
      } else {
        ObJsonString *json_str = static_cast<ObJsonString *>(str_node);
        json_str->set_value(key_str.ptr(), key_str.length());
        str_node = json_str;
      }
    }
  }
  return str_node;
}

ObJsonNode *ObJsonORawString::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  ObJsonNode* str_node = ObJsonTreeUtil::clone_new_node<ObJsonORawString>(allocator, value().ptr(), length(), json_type_);
  int ret = OB_SUCCESS;
  if (is_deep_copy) {
    char* str_buf =NULL;
    if (OB_ISNULL(str_buf = static_cast<char*>(allocator->alloc(length())))) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
    } else {
      ObString key_str(length(), 0, str_buf);
      if (length() != key_str.write(value().ptr(), length())) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
      } else {
        ObJsonORawString *json_str = static_cast<ObJsonORawString *>(str_node);
        json_str->set_value(key_str.ptr(), key_str.length());
        str_node = json_str;
      }
    }
  }
  return str_node;
}

ObJsonNode *ObJsonOInterval::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  ObJsonNode* str_node = ObJsonTreeUtil::clone_new_node<ObJsonOInterval>(allocator, value().ptr(), length(), field_type_);
  if (is_deep_copy) {
    char* str_buf =NULL;
    if (OB_ISNULL(str_buf = static_cast<char*>(allocator->alloc(length())))) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
    } else {
      ObString key_str(length(), 0, str_buf);
      if (length() != key_str.write(value().ptr(), length())) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
      } else {
        ObJsonOInterval *json_str = static_cast<ObJsonOInterval *>(str_node);
        json_str->set_value(key_str.ptr(), key_str.length());
        str_node = json_str;
      }
    }
  }
  return str_node;
}

ObJsonNode *ObJsonNull::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonNull>(allocator, is_not_null_);
}

ObJsonNode *ObJsonDatetime::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonDatetime>(allocator, json_type(), value_);
}

ObJsonNode *ObJsonOpaque::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  ObString content(value_.length(), value_.ptr());
  ObJsonNode* str_node = ObJsonTreeUtil::clone_new_node<ObJsonOpaque>(allocator, content, field_type_);
  if (is_deep_copy) {
    char* str_buf =NULL;
    if (OB_ISNULL(str_buf = static_cast<char*>(allocator->alloc(content.length())))) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
    } else {
      ObString key_str(content.length(), 0, str_buf);
      if (content.length() != key_str.write(content.ptr(), content.length())) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string");
      } else {
        ObJsonOpaque *json_str = static_cast<ObJsonOpaque *>(str_node);
        json_str->set_value(key_str);
        str_node = json_str;
      }
    }
  }
  return str_node;
}

ObJsonNode *ObJsonBoolean::clone(ObIAllocator* allocator, bool is_deep_copy) const
{
  return ObJsonTreeUtil::clone_new_node<ObJsonBoolean>(allocator, value());
}

} // namespace common
} // namespace oceanbase
