/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_table_json_utils.h"
#include "lib/hash/ob_hashtable.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"


#define USING_LOG_PREFIX SERVER

using namespace oceanbase::table;
using namespace oceanbase::common;
using namespace oceanbase::json;

int ObTableJsonUtils::parse(ObIAllocator &allocator,
                            const ObString &json_str,
                            json::Value *&root)
{
  int ret = OB_SUCCESS;
  root = nullptr;
  json::Parser parser;
  if (json_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json_str is empty", K(ret));
  } else if (OB_FAIL(parser.init(dynamic_cast<ObArenaAllocator *>(&allocator)))) {
    LOG_WARN("failed to init parser", K(ret));
  } else if (OB_FAIL(parser.parse(json_str.ptr(), json_str.length(), root))) {
    LOG_WARN("failed to parse json string", K(ret));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is null", K(ret));
  } else if (root->get_type() != JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root is not a json object", K(ret));
  }

  return ret;
}

int ObTableJsonUtils::get_json_value(json::Value *root,
                                    const ObString &name,
                                    json::Type expect_type,
                                    json::Value *&value)
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root is null", K(ret));
  } else if (name.empty()) {
    // do nothing
  } else {
    json::Pair *pair = root->get_object().get_first();
    int size = root->get_object().get_size();
    for (int i = 0; OB_SUCC(ret) && i < size; ++i, pair = pair->get_next()) {
      if (OB_ISNULL(pair)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pair is null", K(ret));
      } else if (pair->name_.compare(name) == 0) {
        if (OB_ISNULL(pair->value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pair value is null", K(ret), K(name), K(expect_type));
        } else {
          if (pair->value_->get_type() == expect_type) {
            value = pair->value_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pair value type is not expected", K(ret), K(name), K(expect_type), K(pair->value_->get_type()));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && nullptr == value) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("value not exist", K(ret), K(name), K(expect_type));
  }

  return ret;
}

int ObTableJsonUtils::serialize(ObIAllocator &allocator,
                                json::Value *root,
                                ObString &dst)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(root)) {
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(BUFFER_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      json::Tidy tidy(root);
      int64_t pos = tidy.to_string(buf, BUFFER_SIZE);
      dst.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is null", K(ret));
  }
  return ret;
}

ObTableJsonObjectBuilder::ObTableJsonObjectBuilder(ObIAllocator &allocator)
    : allocator_(allocator),
      root_(nullptr)
{
}

int ObTableJsonObjectBuilder::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ = OB_NEWx(json::Value, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    root_->set_type(json::JT_OBJECT);
  }
  return ret;
}

int ObTableJsonObjectBuilder::add(const char* key, const int64_t key_len, int64_t value)
{
  int ret = OB_SUCCESS;
  json::Pair *pair = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jsob object builder not init", K(ret), K(key), K(value));
  } else if (OB_ISNULL(pair = OB_NEWx(json::Pair, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pair->name_.assign_ptr(key, key_len);
    if (OB_ISNULL(pair->value_ = OB_NEWx(json::Value, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      pair->value_->set_type(json::JT_NUMBER);
      pair->value_->set_int(value);
      root_->object_add(pair);
    }
  }
  return ret;
}

int ObTableJsonObjectBuilder::add(const char* key, const int64_t key_len, const char* value, int32_t value_len)
{
  int ret = OB_SUCCESS;
  json::Pair *pair = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jsob object builder not init", K(ret), K(key), K(value));
  } else if (OB_ISNULL(pair = OB_NEWx(json::Pair, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pair->name_.assign_ptr(key, key_len);
    if (OB_ISNULL(pair->value_ = OB_NEWx(json::Value, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      pair->value_->set_type(json::JT_STRING);
      pair->value_->set_string(const_cast<char *>(value), value_len);
      root_->object_add(pair);
    }
  }
  return ret;
}

int ObTableJsonObjectBuilder::add(const char* key, const int64_t key_len, const ObString &value)
{
  int ret = OB_SUCCESS;
  json::Pair *pair = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jsob object builder not init", K(ret), K(key), K(value));
  } else if (OB_ISNULL(pair = OB_NEWx(json::Pair, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pair->name_.assign_ptr(key, key_len);
    if (OB_ISNULL(pair->value_ = OB_NEWx(json::Value, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      pair->value_->set_type(json::JT_STRING);
      pair->value_->set_string(const_cast<char *>(value.ptr()), value.length());
      root_->object_add(pair);
    }
  }
  return ret;
}

int ObTableJsonObjectBuilder::add(const char* key, const int64_t key_len, json::Value *value)
{
  int ret = OB_SUCCESS;
  json::Pair *pair = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jsob object builder not init", K(ret), K(key), K(value));
  } else if (OB_ISNULL(pair = OB_NEWx(json::Pair, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pair->name_.assign_ptr(key, key_len);
    pair->value_ = value;
    root_->object_add(pair);
  }
  return ret;
}
int ObTableJsonObjectBuilder::add(const char* key, const int64_t key_len, ObTableJsonObjectBuilder &obj)
{
  int ret = OB_SUCCESS;
  json::Pair *pair = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jsob object builder not init", K(ret), K(key));
  } else if (OB_ISNULL(pair = OB_NEWx(json::Pair, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pair->name_.assign_ptr(key, key_len);
    pair->value_ = obj.build();
    root_->object_add(pair);
  }
  return ret;
}

int ObTableJsonObjectBuilder::add(const char* key, const int64_t key_len, ObTableJsonArrayBuilder &arr)
{
  int ret = OB_SUCCESS;
  json::Pair *pair = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jsob object builder not init", K(ret), K(key));
  } else if (OB_ISNULL(pair = OB_NEWx(json::Pair, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    pair->name_.assign_ptr(key, key_len);
    pair->value_->set_type(json::JT_ARRAY);
    pair->value_->array_add(arr.build());
    root_->object_add(pair);
  }
  return ret;
}

oceanbase::json::Value* ObTableJsonObjectBuilder::build()
{
  return root_;
}

ObTableJsonArrayBuilder::ObTableJsonArrayBuilder(ObIAllocator &allocator)
    : allocator_(allocator),
      root_(nullptr)
{
}

int ObTableJsonArrayBuilder::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ = OB_NEWx(json::Value, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    root_->set_type(json::JT_ARRAY);
  }
  return ret;
}

int ObTableJsonArrayBuilder::add(int64_t value)
{
  int ret = OB_SUCCESS;
  json::Value *v = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json array builder not init", K(ret), K(value));
  } else if (OB_ISNULL(v = OB_NEWx(json::Value, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    v->set_type(json::JT_NUMBER);
    v->set_int(value);
    root_->array_add(v);
  }
  return ret;
}

int ObTableJsonArrayBuilder::add(const char* value, int32_t len)
{
  int ret = OB_SUCCESS;
  json::Value *v = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json array builder not init", K(ret), K(value));
  } else if (OB_ISNULL(v = OB_NEWx(json::Value, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    v->set_type(json::JT_STRING);
    v->set_string(const_cast<char *>(value), len);
    root_->array_add(v);
  }
  return ret;
}

int ObTableJsonArrayBuilder::add(const ObString &value)
{
  int ret = OB_SUCCESS;
  json::Value *v = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json array builder not init", K(ret), K(value));
  } else if (OB_ISNULL(v = OB_NEWx(json::Value, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    v->set_type(json::JT_STRING);
    v->set_string(const_cast<char *>(value.ptr()), value.length());
    root_->array_add(v);
  }
  return ret;
}

oceanbase::json::Value* ObTableJsonArrayBuilder::build()
{
  return root_;
}

int ObTableJsonArrayBuilder::add(json::Value *value)
{
  int ret = OB_SUCCESS;
  json::Value *v = nullptr;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json array builder not init", K(ret), KP(value));
  } else if (OB_ISNULL(v = OB_NEWx(json::Value, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), KP(value));
  } else {
    v->set_type(json::JT_OBJECT);
    root_->array_add(v);
  }
  return ret;
}

int ObTableJsonArrayBuilder::add(ObTableJsonObjectBuilder &obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json array builder not init", K(ret));
  } else {
    root_->add_after(obj.build());
  }
  return ret;
}

int ObTableJsonArrayBuilder::add(ObTableJsonArrayBuilder &arr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("json array builder not init", K(ret));
  } else {
    root_->array_add(arr.build());
  }
  return ret;
}

