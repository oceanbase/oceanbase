/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_JSON_UTILS_H_
#define OCEANBASE_OBSERVER_OB_TABLE_JSON_UTILS_H_

#include "lib/json/ob_json.h"
#include "share/table/ob_table.h"

namespace oceanbase
{
namespace table
{

class ObTableJsonUtils
{
public:
  static int parse(ObIAllocator &allocator, const ObString &json_str, json::Value *&root);
  static int get_json_value(json::Value *root, const ObString &name, json::Type expect_type, json::Value *&value);
  // deep copy
  static int serialize(ObIAllocator &allocator, json::Value *root, ObString &dst);

private:
  static const uint64_t BUFFER_SIZE = 65535;
};

class ObTableJsonArrayBuilder;

class ObTableJsonObjectBuilder
{
public:
  ObTableJsonObjectBuilder(ObIAllocator &allocator);
  int init();
  int add(const char* key, const int64_t key_len, int64_t value);
  int add(const char* key, const int64_t key_len, const char* value, int32_t value_len);
  int add(const char* key, const int64_t key_len, const ObString &value);
  int add(const char* key, const int64_t key_len, json::Value *value);
  int add(const char* key, const int64_t key_len, ObTableJsonObjectBuilder &obj);
  int add(const char* key, const int64_t key_len, ObTableJsonArrayBuilder &arr);
  json::Value* build();

private:
  ObIAllocator &allocator_;
  json::Value *root_;

  DISALLOW_COPY_AND_ASSIGN(ObTableJsonObjectBuilder);
};


class ObTableJsonArrayBuilder
{
public:
  ObTableJsonArrayBuilder(ObIAllocator &allocator);
  int init();
  int add(int64_t value);
  int add(const char* value, int32_t len);
  int add(const ObString &value);
  int add(json::Value *value);
  int add(ObTableJsonObjectBuilder &obj);
  int add(ObTableJsonArrayBuilder &arr);
  json::Value* build();

private:
  ObIAllocator &allocator_;
  json::Value *root_;

  DISALLOW_COPY_AND_ASSIGN(ObTableJsonArrayBuilder);
};

} // end namespace table
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_TABLE_JSON_UTILS_H_
