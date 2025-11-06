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

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_JSON_HELPER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_JSON_HELPER_H_

#include "lib/json/ob_json.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace share
{

// Forward declarations
using json::Value;
using json::Pair;
using json::Parser;
using json::Tidy;
using common::ObJsonNode;
using common::ObIJsonBase;
using common::ObJsonParser;
using common::ObString;
using common::ObIAllocator;

class ObJsonBuilder
{
public:
  explicit ObJsonBuilder(ObIAllocator &allocator);
  ~ObJsonBuilder();
  int create_object(Value *&root);

  int create_array(Value *&array);

  int add_string_field(Value *obj, const ObString &key, const ObString &value);

  int add_int_field(Value *obj, const ObString &key, int64_t value);

  int add_array_field(Value *obj, const ObString &key, Value *&array);

  int array_add_string(Value *array, const ObString &value);

  int to_string(Value *root, char *buffer, int64_t buffer_len, int64_t &json_len);

private:
  ObIAllocator &allocator_;
  Parser parser_;

  DISALLOW_COPY_AND_ASSIGN(ObJsonBuilder);
};

class ObJsonReaderHelper
{
public:
  explicit ObJsonReaderHelper(ObIAllocator &allocator);
  ~ObJsonReaderHelper();

  int parse(const char *json_str, size_t json_len, ObJsonNode *&root);

  int get_object_value(const ObIJsonBase *obj, const ObString &key, ObIJsonBase *&value);


  int get_array_element(const ObIJsonBase *array, uint64_t index, ObIJsonBase *&element);

  uint64_t get_array_size(const ObIJsonBase *array);

  int get_float_value(const ObIJsonBase *element, float &value);

private:
  ObIAllocator &allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObJsonReaderHelper);
};

class ObJsonHelper
{
public:
  static bool is_number_type(const ObIJsonBase *element);

  static bool is_array_type(const ObIJsonBase *element);

  static bool is_object_type(const ObIJsonBase *element);

  static const char* get_type_name(const ObIJsonBase *element);

private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonHelper);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_JSON_HELPER_H_
