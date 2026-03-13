/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 * This file contains tests for ObSearchIndexEncoder.
 */

#include <gtest/gtest.h>
#define private public
#include "share/search_index/ob_search_index_encoder.h"
#include "share/search_index/ob_search_index_generator.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "storage/lob/ob_lob_manager.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#undef private

using namespace std;
namespace oceanbase {
namespace share {

// Typedef for test code simplicity
typedef ObSearchIndexRow ObIndexRow;

class TestSearchIndexEncoder : public ::testing::Test {
public:
  TestSearchIndexEncoder()
  {}
  ~TestSearchIndexEncoder()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSearchIndexEncoder);
};

// Helper function to convert JSON string to ObDatum with binary format
static int json_str_to_datum(ObIAllocator &allocator, const char *json_text, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_bin = nullptr;
  ObString json_str(json_text);
  ObString raw_binary;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_str, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin))) {
    COMMON_LOG(WARN, "get_json_base fail", K(ret), K(json_text));
  } else if (OB_FAIL(j_bin->get_raw_binary(raw_binary, &allocator))) {
    COMMON_LOG(WARN, "get_raw_binary fail", K(ret), K(json_text), KPC(j_bin));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, raw_binary, raw_binary))) {
    COMMON_LOG(WARN, "fill_lob_header fail", K(ret), K(json_text));
  } else {
    // Since ObDatumPtr is a union, ptr_ and lob_data_ share the same memory
    // set_string will set ptr_ which can be accessed as lob_data_ via get_lob_data()
    datum.set_string(raw_binary);
  }
  return ret;
}

// Helper function to build expected encoded path for testing
// Path encoding format: path_segment\x00\x1c ... \x00type
static ObString build_expected_path(ObIAllocator &allocator,
                                    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                                    ObJsonNodeType value_type)
{
  ObString result;
  int ret = ObSearchIndexPathEncoder::encode_path(allocator, path_items, value_type, result);
  if (OB_SUCCESS != ret) {
    COMMON_LOG(WARN, "failed to encode path", K(ret));
  }
  return result;
}

// Helper to check if a path exists in the generated rows
static bool find_path_in_rows(const ObSEArray<ObIndexRow, 16> &rows, const ObString &expected_path)
{
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObIndexRow &row = rows.at(i);
    if (!row.path_.is_null()) {
      ObString actual_path = row.path_.get_string();
      if (actual_path.length() == expected_path.length() &&
          0 == MEMCMP(actual_path.ptr(), expected_path.ptr(), expected_path.length())) {
        return true;
      }
    }
  }
  return false;
}

// Test ObSearchIndexRowGenerator with basic nested object
// JSON: {"a": {"b": 3}}
// Expected: $.a.b -> 3
TEST_F(TestSearchIndexEncoder, test_json_row_generator_basic_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"a": {"b": 3}})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row";

  // Build expected path: $.a.b
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> path_items;
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a")));
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("b")));
  ObString expected_path = build_expected_path(allocator, path_items, ObJsonNodeType::J_DECIMAL);

  const ObIndexRow &row = rows.at(0);
  ASSERT_FALSE(row.path_.is_null());
  ObString actual_path = row.path_.get_string();
  ASSERT_EQ(expected_path.length(), actual_path.length()) << "Path length mismatch";
  ASSERT_EQ(0, MEMCMP(expected_path.ptr(), actual_path.ptr(), expected_path.length()))
      << "Path encoding should match $.a.b";
  ASSERT_FALSE(row.value_.is_null());
}

// Test ObSearchIndexRowGenerator with multiple leaf nodes
// JSON: {"name": "Alice", "age": 25}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_multiple_leaves)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"name": "Alice", "age": 25})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 search index rows";

  // Build expected paths: $.name and $.age
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> name_path_items;
  name_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("name")));
  ObString expected_name_path = build_expected_path(allocator, name_path_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> age_path_items;
  age_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("age")));
  ObString expected_age_path = build_expected_path(allocator, age_path_items, ObJsonNodeType::J_DECIMAL);

  // Verify both paths exist
  ASSERT_TRUE(find_path_in_rows(rows, expected_name_path)) << "Should find $.name path";
  ASSERT_TRUE(find_path_in_rows(rows, expected_age_path)) << "Should find $.age path";

  // Verify values are not null
  for (int64_t i = 0; i < rows.count(); i++) {
    ASSERT_FALSE(rows.at(i).value_.is_null());
  }
}

// Test array elements with scalar values
TEST_F(TestSearchIndexEncoder, test_json_row_generator_array_scalars)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"arr": [1, 2]})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 search index rows for array scalars";

  // Build expected path: $.arr[] (array element path)
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> arr_path_items;
  arr_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("arr")));
  arr_path_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  ObString expected_path = build_expected_path(allocator, arr_path_items, ObJsonNodeType::J_DECIMAL);

  // All array elements should have the same path encoding
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObIndexRow &row = rows.at(i);
    ASSERT_FALSE(row.path_.is_null());
    ObString actual_path = row.path_.get_string();
    ASSERT_EQ(expected_path.length(), actual_path.length()) << "Array element path length mismatch";
    ASSERT_EQ(0, MEMCMP(expected_path.ptr(), actual_path.ptr(), expected_path.length()))
        << "Array element path should match $.arr[]";
    ASSERT_FALSE(row.value_.is_null());
  }
}

// Test array elements that contain nested objects
TEST_F(TestSearchIndexEncoder, test_json_row_generator_array_nested_objects)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"arr": [{"x": 1}, {"y": "foo"}]})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rows.count()) << "Expected 2 search index rows for array nested objects";

  // Build expected paths: $.arr[].x and $.arr[].y
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> x_path_items;
  x_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("arr")));
  x_path_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  x_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("x")));
  ObString expected_x_path = build_expected_path(allocator, x_path_items, ObJsonNodeType::J_DECIMAL);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> y_path_items;
  y_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("arr")));
  y_path_items.push_back(ObSearchIndexPathEncoder::make_array_path());
  y_path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("y")));
  ObString expected_y_path = build_expected_path(allocator, y_path_items, ObJsonNodeType::J_STRING);

  // Verify both paths exist
  ASSERT_TRUE(find_path_in_rows(rows, expected_x_path)) << "Should find $.arr[].x path";
  ASSERT_TRUE(find_path_in_rows(rows, expected_y_path)) << "Should find $.arr[].y path";

  // Verify values are not null
  for (int64_t i = 0; i < rows.count(); i++) {
    ASSERT_FALSE(rows.at(i).value_.is_null());
  }
}

// Test ObSearchIndexRowGenerator with deeply nested objects
// JSON: {"a": {"b": {"c": 42}}}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_deeply_nested)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({"a": {"b": {"c": 42}}})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row";

  // Build expected path: $.a.b.c
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> path_items;
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("a")));
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("b")));
  path_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("c")));
  ObString expected_path = build_expected_path(allocator, path_items, ObJsonNodeType::J_DECIMAL);

  const ObIndexRow &row = rows.at(0);
  ASSERT_FALSE(row.path_.is_null());
  ObString actual_path = row.path_.get_string();
  ASSERT_EQ(expected_path.length(), actual_path.length()) << "Path length mismatch";
  ASSERT_EQ(0, MEMCMP(expected_path.ptr(), actual_path.ptr(), expected_path.length()))
      << "Path encoding should match $.a.b.c";
  ASSERT_FALSE(row.value_.is_null());
}

// Test ObSearchIndexRowGenerator with empty object
// JSON: {}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_empty_object)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({})";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, rows.count()) << "Expected 0 search index rows for empty object";
}

// Test ObSearchIndexRowGenerator with null datum
TEST_F(TestSearchIndexEncoder, test_json_row_generator_null_datum)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  ObDatum datum;
  datum.set_null();

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  int ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, rows.count()) << "Expected 1 search index row for null";

  const ObIndexRow &row = rows.at(0);
  ASSERT_TRUE(row.value_.is_null());
}

// Test ObSearchIndexRowGenerator with complex nested structure
// JSON: {"user": {"name": "John", "age": 30, "address": {"city": "Beijing", "zip": 100000}}}
TEST_F(TestSearchIndexEncoder, test_json_row_generator_complex_structure)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  const char *json_text = R"({
    "user": {
      "name": "John",
      "age": 30,
      "address": {
        "city": "Beijing",
        "zip": 100000
      }
    }
  })";
  ObDatum datum;
  int ret = json_str_to_datum(allocator, json_text, datum);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObjMeta obj_meta;
  obj_meta.set_type(ObJsonType);
  obj_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);

  ObSearchIndexRowGenerator::Generator generator(allocator, allocator);
  ObString column_comment;
  ret = generator.init(0, obj_meta, nullptr, &column_comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObIndexRow, 16> rows;
  ret = generator.generate_rows(0, datum, rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, rows.count()) << "Expected 4 search index rows (user.name, user.age, user.address.city, user.address.zip)";

  // Build expected paths
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> name_items;
  name_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  name_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("name")));
  ObString expected_name = build_expected_path(allocator, name_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> age_items;
  age_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  age_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("age")));
  ObString expected_age = build_expected_path(allocator, age_items, ObJsonNodeType::J_DECIMAL);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> city_items;
  city_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  city_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("address")));
  city_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("city")));
  ObString expected_city = build_expected_path(allocator, city_items, ObJsonNodeType::J_STRING);

  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 4> zip_items;
  zip_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("user")));
  zip_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("address")));
  zip_items.push_back(ObSearchIndexPathEncoder::make_object_path(ObString("zip")));
  ObString expected_zip = build_expected_path(allocator, zip_items, ObJsonNodeType::J_DECIMAL);

  // Verify all paths exist
  ASSERT_TRUE(find_path_in_rows(rows, expected_name)) << "Should find $.user.name";
  ASSERT_TRUE(find_path_in_rows(rows, expected_age)) << "Should find $.user.age";
  ASSERT_TRUE(find_path_in_rows(rows, expected_city)) << "Should find $.user.address.city";
  ASSERT_TRUE(find_path_in_rows(rows, expected_zip)) << "Should find $.user.address.zip";

  // Verify values are not null
  for (int64_t i = 0; i < rows.count(); i++) {
    ASSERT_FALSE(rows.at(i).value_.is_null());
  }
}

} // namespace share
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
