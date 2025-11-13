/**
* Copyright (c) 2023 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*/

#define USING_LOG_PREFIX SHARE
#include "share/vector_index/ob_json_helper.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/number/ob_number_v2.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::json;

//=============================================== ObJsonBuilder ================================================

ObJsonBuilder::ObJsonBuilder(ObIAllocator &allocator)
  : allocator_(allocator), parser_()
{
}

ObJsonBuilder::~ObJsonBuilder()
{
}

int ObJsonBuilder::create_object(Value *&root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root = (Value*)allocator_.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc root value", K(ret));
  } else {
    new (root) Value();
    root->set_type(JT_OBJECT);
  }
  return ret;
}

int ObJsonBuilder::create_array(Value *&array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array = (Value*)allocator_.alloc(sizeof(Value)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc array value", K(ret));
  } else {
    new (array) Value();
    array->set_type(JT_ARRAY);
  }
  return ret;
}

int ObJsonBuilder::add_string_field(Value *obj, const ObString &key, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj) || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(obj), K(key));
  } else if (obj->get_type() != JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is not an object", K(ret));
  } else {
    Pair *pair = nullptr;
    Value *str_val = nullptr;
    int32_t escaped_len = 0;
    char *escaped_str = nullptr;
    if (OB_ISNULL(pair = (Pair*)allocator_.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc pair", K(ret));
    } else if (OB_ISNULL(str_val = (Value*)allocator_.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc string value", K(ret));
    } else {
     // convert the string to an escaped string
      ObJsonBuffer jbuf(&allocator_);
      if (OB_FAIL(ObJsonBaseUtil::add_double_quote(jbuf, value.ptr(), value.length()))) {
        LOG_WARN("failed to add double quote (escape string)", K(ret));
      } else if (jbuf.length() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected escaped length", K(ret), "len", jbuf.length());
      } else {
        ObString jbuf_str;
        if (OB_FAIL(jbuf.get_result_string(jbuf_str))) {
          LOG_WARN("failed to detach json buffer", K(ret));
        } else {
          escaped_str = jbuf_str.ptr() + 1;
          escaped_len = jbuf_str.length() - 2;
        }
      }
    }
    if (OB_SUCC(ret)) {
      new (pair) Pair();
      new (str_val) Value();
      str_val->set_type(JT_STRING);
      str_val->set_string(escaped_str, escaped_len);
      pair->name_ = key;
      pair->value_ = str_val;
      obj->object_add(pair);
    }
  }
  return ret;
}

int ObJsonBuilder::add_int_field(Value *obj, const ObString &key, int64_t value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj) || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(obj), K(key));
  } else if (obj->get_type() != JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is not an object", K(ret));
  } else {
    Pair *pair = nullptr;
    Value *int_val = nullptr;

    if (OB_ISNULL(pair = (Pair*)allocator_.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc pair", K(ret));
    } else if (OB_ISNULL(int_val = (Value*)allocator_.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc int value", K(ret));
    } else {
      new (pair) Pair();
      new (int_val) Value();

      int_val->set_type(JT_NUMBER);
      int_val->set_int(value);

      pair->name_ = key;
      pair->value_ = int_val;

      obj->object_add(pair);
    }
  }
  return ret;
}

int ObJsonBuilder::add_array_field(Value *obj, const ObString &key, Value *&array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj) || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(obj), K(key));
  } else if (obj->get_type() != JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is not an object", K(ret));
  } else {
    Pair *pair = nullptr;

    if (OB_FAIL(create_array(array))) {
      LOG_WARN("failed to create array", K(ret));
    } else if (OB_ISNULL(pair = (Pair*)allocator_.alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc pair", K(ret));
    } else {
      new (pair) Pair();
      pair->name_ = key;
      pair->value_ = array;
      obj->object_add(pair);
    }
  }
  return ret;
}

int ObJsonBuilder::array_add_string(Value *array, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(array));
  } else if (array->get_type() != JT_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is not an array", K(ret));
  } else {
    Value *str_val = nullptr;
    char *str_buf = nullptr;
    char *escaped_str = nullptr;
    int32_t escaped_len = 0;
    if (OB_ISNULL(str_val = (Value*)allocator_.alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc string value", K(ret));
    } else {
      ObJsonBuffer jbuf(&allocator_);
      // convert the string to an escaped string
      if (OB_FAIL(ObJsonBaseUtil::add_double_quote(jbuf, value.ptr(), value.length()))) {
        LOG_WARN("failed to add double quote (escape string)", K(ret));
      } else if (jbuf.length() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected escaped length", K(ret), "len", jbuf.length());
      } else {
        ObString jbuf_str;
        if (OB_FAIL(jbuf.get_result_string(jbuf_str))) {
          LOG_WARN("failed to detach json buffer", K(ret));
        } else {
          escaped_len = jbuf_str.length() - 2;
          escaped_str = jbuf_str.ptr() + 1;
        }
      }
    }
    if (OB_SUCC(ret)) {
      new (str_val) Value();
      str_val->set_type(JT_STRING);
      str_val->set_string(escaped_str, escaped_len);
      array->array_add(str_val);
    }
  }
  return ret;
}

int ObJsonBuilder::to_string(Value *root, char *buffer, int64_t buffer_len, int64_t &json_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root) || OB_ISNULL(buffer) || buffer_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(root), KP(buffer), K(buffer_len));
  } else {
    Tidy json_tidy(root);
    json_len = json_tidy.to_string(buffer, buffer_len);
    if (json_len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to convert json to string", K(ret), K(json_len));
    }
  }
  return ret;
}

//=============================================== ObJsonReaderHelper ================================================

ObJsonReaderHelper::ObJsonReaderHelper(ObIAllocator &allocator)
  : allocator_(allocator)
{
}

ObJsonReaderHelper::~ObJsonReaderHelper()
{
}

int ObJsonReaderHelper::parse(const char *json_str, size_t json_len, ObJsonNode *&root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_str) || json_len == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(json_str), K(json_len));
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator_, json_str, json_len, root))) {
    LOG_WARN("failed to parse json response", K(ret), KP(json_str), K(json_len));

    int64_t print_size = std::min(static_cast<size_t>(1000), json_len);
    char debug_buffer[1001];
    MEMCPY(debug_buffer, json_str, print_size);
    debug_buffer[print_size] = '\0';
    LOG_WARN("json parse failed, response content", K(ret), K(debug_buffer));
  }
  return ret;
}

int ObJsonReaderHelper::get_object_value(const ObIJsonBase *obj, const ObString &key, ObIJsonBase *&value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj) || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(obj), K(key));
  } else if (obj->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json node is not an object", K(ret));
  } else {
    if (OB_FAIL(obj->get_object_value(key, value))) {
      LOG_WARN("failed to get object value", K(ret), K(key));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object value is null", K(ret), K(key));
    }
  }
  return ret;
}

int ObJsonReaderHelper::get_array_element(const ObIJsonBase *array, uint64_t index, ObIJsonBase *&element)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(array));
  } else if (array->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json node is not an array", K(ret));
  } else if (index >= array->element_count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of range", K(ret), K(index), K(array->element_count()));
  } else if (OB_FAIL(array->get_array_element(index, element))) {
    LOG_WARN("failed to get array element", K(ret), K(index));
  } else if (OB_ISNULL(element)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array element is null", K(ret), K(index));
  }
  return ret;
}

uint64_t ObJsonReaderHelper::get_array_size(const ObIJsonBase *array)
{
  if (OB_ISNULL(array) || array->json_type() != ObJsonNodeType::J_ARRAY) {
    return 0;
  }
  return array->element_count();
}

int ObJsonReaderHelper::get_float_value(const ObIJsonBase *element, float &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(element)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(element));
  } else {
    ObJsonNodeType node_type = element->json_type();
    if (node_type == ObJsonNodeType::J_DOUBLE) {
      value = static_cast<float>(element->get_double());
    } else if (node_type == ObJsonNodeType::J_INT) {
      value = static_cast<float>(element->get_int());
    } else if (node_type == ObJsonNodeType::J_UINT) {
      value = static_cast<float>(element->get_uint());
    } else if (node_type == ObJsonNodeType::J_DECIMAL) {
      number::ObNumber decimal_val = element->get_decimal_data();
      char decimal_buf[512];
      int64_t pos = 0;
      if (OB_FAIL(decimal_val.format(decimal_buf, sizeof(decimal_buf), pos, -1))) {
        LOG_WARN("failed to format decimal", K(ret));
      } else {
        decimal_buf[pos] = '\0';
        value = static_cast<float>(atof(decimal_buf));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("json element is not a number", K(ret), K(node_type));
    }
  }
  return ret;
}

//=============================================== ObJsonHelper ================================================

bool ObJsonHelper::is_number_type(const ObIJsonBase *element)
{
  if (OB_ISNULL(element)) {
    return false;
  }

  ObJsonNodeType node_type = element->json_type();
  return (node_type == ObJsonNodeType::J_DOUBLE ||
          node_type == ObJsonNodeType::J_INT ||
          node_type == ObJsonNodeType::J_UINT ||
          node_type == ObJsonNodeType::J_DECIMAL ||
          node_type == ObJsonNodeType::J_OFLOAT ||
          node_type == ObJsonNodeType::J_ODOUBLE ||
          node_type == ObJsonNodeType::J_ODECIMAL ||
          node_type == ObJsonNodeType::J_OINT ||
          node_type == ObJsonNodeType::J_OLONG);
}

bool ObJsonHelper::is_array_type(const ObIJsonBase *element)
{
  if (OB_ISNULL(element)) {
    return false;
  }
  return element->json_type() == ObJsonNodeType::J_ARRAY;
}

bool ObJsonHelper::is_object_type(const ObIJsonBase *element)
{
  if (OB_ISNULL(element)) {
    return false;
  }
  return element->json_type() == ObJsonNodeType::J_OBJECT;
}

const char* ObJsonHelper::get_type_name(const ObIJsonBase *element)
{
  if (OB_ISNULL(element)) {
    return "null";
  }

  ObJsonNodeType node_type = element->json_type();
  switch (node_type) {
    case ObJsonNodeType::J_NULL:
      return "null";
    case ObJsonNodeType::J_BOOLEAN:
      return "boolean";
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE:
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_OINT:
    case ObJsonNodeType::J_OLONG:
      return "number";
    case ObJsonNodeType::J_STRING:
      return "string";
    case ObJsonNodeType::J_ARRAY:
      return "array";
    case ObJsonNodeType::J_OBJECT:
      return "object";
    default:
      return "unknown";
  }
}