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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/hash/ob_hashset.h"
#include "lib/udt/ob_array_utils.h"
#include "sql/engine/expr/ob_array_cast.h"
#undef private
#undef protected


using namespace std;

namespace oceanbase {
namespace common {
class TestArrayMeta : public ::testing::Test
{
public:
  TestArrayMeta()
  {}
  ~TestArrayMeta()
  {}

private:
  ObArenaAllocator allocator_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestArrayMeta);
};

TEST_F(TestArrayMeta, serialize_deserialize)
{
  ObCollectionBasicType int_type;
  int_type.basic_meta_.meta_.set_int32();
  int_type.basic_meta_.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObInt32Type].scale_);
  int_type.basic_meta_.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObInt32Type].precision_);
  int_type.type_id_ = ObNestedType::OB_BASIC_TYPE;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObCollectionArrayType arr1_type(allocator);
  arr1_type.element_type_ = &int_type;
  arr1_type.type_id_ = ObNestedType::OB_ARRAY_TYPE;
  ObCollectionArrayType arr2_type(allocator);
  arr2_type.element_type_ = &arr1_type;
  arr2_type.type_id_ = ObNestedType::OB_ARRAY_TYPE;
  ObSqlCollectionInfo type_info(allocator);
  type_info.collection_meta_ = &arr2_type;
  ObString type_name(strlen("ARRAY(ARRAY(INT))"), "ARRAY(ARRAY(INT))");
  type_info.set_name(type_name);
  char buf[1024] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, arr1_type.serialize(buf, 1024, pos));
  ObCollectionArrayType arr1_type_res(allocator);
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, arr1_type_res.deserialize(buf, data_len, pos));
  ASSERT_EQ(arr1_type_res.type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *basic_type = reinterpret_cast<ObCollectionBasicType*>(arr1_type_res.element_type_);
  ASSERT_EQ(basic_type->basic_meta_, int_type.basic_meta_);


  ObSqlCollectionInfo type_info_parse(allocator);
  type_info_parse.set_name(type_name);
  ASSERT_EQ(OB_SUCCESS, type_info_parse.parse_type_info());
  ObCollectionArrayType *arr_meta = static_cast<ObCollectionArrayType *>(type_info_parse.collection_meta_);
  ASSERT_EQ(arr_meta->type_id_, ObNestedType::OB_ARRAY_TYPE);
  arr_meta = static_cast<ObCollectionArrayType *>(arr_meta->element_type_);
  ASSERT_EQ(arr_meta->type_id_, ObNestedType::OB_ARRAY_TYPE);
  basic_type = static_cast<ObCollectionBasicType *>(arr_meta->element_type_);
  ASSERT_EQ(basic_type->basic_meta_, int_type.basic_meta_);


  ObSqlCollectionInfo type1_info_parse(allocator);
  ObCollectionBasicType varchar_type;
  varchar_type.basic_meta_.meta_.set_varchar();
  varchar_type.basic_meta_.set_length(256);
  // set default cs
  varchar_type.basic_meta_.meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  varchar_type.basic_meta_.meta_.set_collation_level(CS_LEVEL_COERCIBLE);
  ObString type1_name(strlen("ARRAY(ARRAY(VARCHAR(256)))"), "ARRAY(ARRAY(VARCHAR(256)))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  arr_meta = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(arr_meta->type_id_, ObNestedType::OB_ARRAY_TYPE);
  arr_meta = static_cast<ObCollectionArrayType *>(arr_meta->element_type_);
  ASSERT_EQ(arr_meta->type_id_, ObNestedType::OB_ARRAY_TYPE);
  basic_type = static_cast<ObCollectionBasicType *>(arr_meta->element_type_);
  ASSERT_EQ(basic_type->basic_meta_, varchar_type.basic_meta_);
}

TEST_F(TestArrayMeta, varchar_arra_construct)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[\"hello\", \"world\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;

  // construct array(arrray(varchar))
  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(ARRAY(VARCHAR(256)))"), "ARRAY(ARRAY(VARCHAR(256)))");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type2 = static_cast<ObCollectionArrayType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var2));
  // push ["hello", "world"] to array(arrray(varchar))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));

  // construct array from string ["hello", "world", "hi", "what", "are you?"]
  ObString arr2_text("[\"hi\", null, \"what\", \"are you?\"]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var1, dst_elem_type));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;
  // push ["hello", "world", "hi", "what", "are you?"] to array(arrray(varchar))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var2->print(format_str));
  std::cout << "arr_va2: " << format_str.ptr() << std::endl;

  char raw_binary[1024] = {0};
  ASSERT_EQ(OB_SUCCESS, arr_var2->get_raw_binary(raw_binary, 1024));
  int32_t raw_len = arr_var2->get_raw_binary_len();
  ObIArrayType *arr_var3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var3));
  ObString raw_str(raw_len, raw_binary);
  ASSERT_EQ(OB_SUCCESS, arr_var3->init(raw_str));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var3->print(format_str));
  std::cout << "arr_va3: " << format_str.ptr() << std::endl;

  // construct array(array(array(varchar)))
  ObSqlCollectionInfo type3_info_parse(allocator);
  ObString type3_name(strlen("ARRAY(ARRAY(ARRAY(VARCHAR(256))))"), "ARRAY(ARRAY(ARRAY(VARCHAR(256))))");
  type3_info_parse.set_name(type3_name);
  ASSERT_EQ(OB_SUCCESS, type3_info_parse.parse_type_info());
  ObIArrayType *arr_var4 = nullptr;
  ObCollectionArrayType *arr_type3 = static_cast<ObCollectionArrayType *>(type3_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type3, arr_var4));
  // push arr_var3 to array(arrray(varchar))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var4)->push_back(*arr_var3));
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var4)->push_back(*arr_var3));
  ASSERT_EQ(OB_SUCCESS, arr_var4->init());
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var4->print(format_str));
  std::cout << "arr_va4: " << format_str.ptr() << std::endl;

  // arr_var4->at(i)
  ObIArrayType *arr_var5 = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var5));
  for (uint32_t i = 0; i < arr_var4->size(); ++i) {
    ASSERT_EQ(OB_SUCCESS, arr_var4->at(i, *arr_var5));
    ASSERT_EQ(OB_SUCCESS, arr_var5->init());
    format_str.reset();
    ASSERT_EQ(OB_SUCCESS, arr_var5->print(format_str));
    std::cout << "arr_va5: " << i << ": "<< format_str.ptr() << std::endl;
    arr_var5->clear();
  }
}

TEST_F(TestArrayMeta, fixsize_array_construct)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;

  // construct array(arrray(varchar))
  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(ARRAY(FLOAT))"), "ARRAY(ARRAY(FLOAT))");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type2 = static_cast<ObCollectionArrayType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var2));
  // push [3.14, 1.414, 2.718] to array(arrray(float))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));

  // construct array from string [5, 6.88, null, 8.01]
  ObString arr2_text("[5, 6.88, null, 8.01]");
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var1, dst_elem_type));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;
  // push [5, 6.88, null, 8.01] to array(arrray(float))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var2->print(format_str));
  std::cout << "arr_va2: " << format_str.ptr() << std::endl;

  char raw_binary[1024] = {0};
  ASSERT_EQ(OB_SUCCESS, arr_var2->get_raw_binary(raw_binary, 1024));
  int32_t raw_len = arr_var2->get_raw_binary_len();
  ObIArrayType *arr_var3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var3));
  ObString raw_str(raw_len, raw_binary);
  ASSERT_EQ(OB_SUCCESS, arr_var3->init(raw_str));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var3->print(format_str));
  std::cout << "arr_va3: " << format_str.ptr() << std::endl;

}

TEST_F(TestArrayMeta, nested_vector_construct_float)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("VECTOR(3)"), "VECTOR(3)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "vector(3): " << format_str.ptr() << std::endl;

  // construct array(vector(3))
  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(VECTOR(3))"), "ARRAY(VECTOR(3))");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type2 = static_cast<ObCollectionArrayType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var2));
  // push [3.14, 1.414, 2.718] to array(arrray(float))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));

  // construct array from string [5, 6.88, null, 8.01]
  ObString arr2_text("[5, 6.88, 8.01]");
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var1, dst_elem_type));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "vector(3, FLOAT): " << format_str.ptr() << std::endl;
  // push [5, 6.88, null, 8.01] to array(arrray(float))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var2->print(format_str));
  std::cout << "ARRAY(VECTOR(3, FLOAT)): " << format_str.ptr() << std::endl;

  char raw_binary[1024] = {0};
  ASSERT_EQ(OB_SUCCESS, arr_var2->get_raw_binary(raw_binary, 1024));
  int32_t raw_len = arr_var2->get_raw_binary_len();
  ObIArrayType *arr_var3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var3));
  ObString raw_str(raw_len, raw_binary);
  ASSERT_EQ(OB_SUCCESS, arr_var3->init(raw_str));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var3->print(format_str));
  std::cout << "ARRAY(VECTOR(3, FLOAT)): " << format_str.ptr() << std::endl;
}

TEST_F(TestArrayMeta, nested_vector_construct_uint8)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("VECTOR(3, UTINYINT)"), "VECTOR(3, UTINYINT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  std::cout << arr_var1->get_element_type() << std::endl;
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("[3,1,3]"));
  // truncate to [3,1,3], compatible with OB tinyint col
  std::cout << "vector(3, UTINYINT): " << format_str.ptr() << std::endl;

  // construct array(vector(3, UTINYINT))
  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(VECTOR(3, UTINYINT))"), "ARRAY(VECTOR(3, UTINYINT))");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type2 = static_cast<ObCollectionArrayType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var2));
  // push [3.14, 1.414, 2.718] to array(arrray(UTINYINT))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));

  // construct array from string [1, 2, 3]
  ObString arr2_text("[1, 2, 3]");
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var1, dst_elem_type));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("[1,2,3]"));
  std::cout << "vector(3, UTINYINT): " << format_str.ptr() << std::endl;
  // push [1, 2, 3] to array(arrray(UTINYINT))
  ASSERT_EQ(OB_SUCCESS, static_cast<ObArrayNested*>(arr_var2)->push_back(*arr_var1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var2->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("[[3,1,3],[1,2,3]]"));
  std::cout << "ARRAY(VECTOR(3, UTINYINT)): " << format_str.ptr() << std::endl;

  char raw_binary[1024] = {0};
  ASSERT_EQ(OB_SUCCESS, arr_var2->get_raw_binary(raw_binary, 1024));
  int32_t raw_len = arr_var2->get_raw_binary_len();
  ObIArrayType *arr_var3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var3));
  ObString raw_str(raw_len, raw_binary);
  ASSERT_EQ(OB_SUCCESS, arr_var3->init(raw_str));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, arr_var3->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("[[3,1,3],[1,2,3]]"));
  std::cout << "ARRAY(VECTOR(3, UTINYINT)): " << format_str.ptr() << std::endl;

  // error: out of uint8 range
  ObString arr3_text("[5, 123456, 8.01]");
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var1, dst_elem_type));
  ObString arr4_text("[5, -1, 8.01]");
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, sql::ObArrayCastUtils::string_cast(allocator, arr4_text, arr_var1, dst_elem_type));
}

TEST_F(TestArrayMeta, nested_vector_construct_error)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  // 1. with not supported type
  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("VECTOR(3, TINYINT)"), "VECTOR(3, TINYINT)");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_NOT_SUPPORTED, type2_info_parse.parse_type_info());

  // 2. should be all caps
  ObSqlCollectionInfo type3_info_parse(allocator);
  ObString type3_name(strlen("VECTOR(3, float)"), "VECTOR(3, float)");
  type3_info_parse.set_name(type3_name);
  ASSERT_EQ(OB_NOT_SUPPORTED, type3_info_parse.parse_type_info());
}

TEST_F(TestArrayMeta, type_deduce)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());

  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(VARCHAR(16))"), "ARRAY(VARCHAR(16))");
  type2_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  // different str_len is still has same super type
  ASSERT_EQ(true, type2_info_parse.has_same_super_type(type1_info_parse));

  ObSqlCollectionInfo decimal_array(allocator);
  ObString type3_name(strlen("ARRAY(DECIMAL_INT(10,2))"), "ARRAY(DECIMAL_INT(10,2))");
  decimal_array.set_name(type3_name);
  ASSERT_EQ(OB_SUCCESS, decimal_array.parse_type_info());

  ObSqlCollectionInfo float_array(allocator);
  ObString type4_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  float_array.set_name(type4_name);
  ASSERT_EQ(OB_SUCCESS, float_array.parse_type_info());

  // array(float)/array(decimal) has same super type
  ASSERT_EQ(true, decimal_array.has_same_super_type(float_array));
}

TEST_F(TestArrayMeta, nested_array_parse)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[[3.14159, 95.27, null], [null, 8.878, 912.33], [333, 12.134, null]]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, arr_type1->element_type_));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;
}

TEST_F(TestArrayMeta, array_compare)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  uint64_t hash_val1 = 0;
  uint64_t hash_val2 = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->hash(hash_val1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  int cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(0, cmp_ret);
  EXPECT_TRUE((*arr_var1) == (*arr_var2));
  ASSERT_EQ(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;
  ObString arr2_text("[3.14, 2.414, 2.718]");
  arr_var2->clear();
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);
  EXPECT_FALSE((*arr_var1) == (*arr_var2));
  ASSERT_NE(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;

  ObString arr3_text("[3.14, 1.414]");
  arr_var2->clear();
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(1, cmp_ret);
  EXPECT_FALSE((*arr_var1) == (*arr_var2));
  ASSERT_NE(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;
}

TEST_F(TestArrayMeta, varchar_array_construct)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[\"hello\", \"hi\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  uint64_t hash_val1 = 0;
  uint64_t hash_val2 = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->hash(hash_val1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  int cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(0, cmp_ret);
  EXPECT_TRUE((*arr_var1) == (*arr_var2));
  ASSERT_EQ(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;

  ObString arr2_text("[\"hi\", \"hello\"]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);
  EXPECT_FALSE((*arr_var1) == (*arr_var2));
  ASSERT_NE(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;
}

TEST_F(TestArrayMeta, array_nested_compare)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[[3.14159, 95.27, null], [null, 8.878, 912.33], [333, 12.134, null]]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, arr_type1->element_type_));
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var2, arr_type1->element_type_));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  uint64_t hash_val1 = 0;
  uint64_t hash_val2 = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->hash(hash_val1));
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  int cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(0, cmp_ret);
  EXPECT_TRUE((*arr_var1) == (*arr_var2));
  ASSERT_EQ(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;

  ObString arr2_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null]]");
  arr_var2->clear();
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, arr_type1->element_type_));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  ASSERT_EQ(OB_SUCCESS, arr_var2->hash(hash_val2));
  cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, arr_var1->compare(*arr_var2, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);
  EXPECT_FALSE((*arr_var1) == (*arr_var2));
  ASSERT_NE(hash_val1, hash_val2);
  std::cout << "arr_hash1: " << hash_val1 << "; arr_hash2: " << hash_val2 << std::endl;
}

TEST_F(TestArrayMeta, array_contains)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  bool bret = false;
  float val_contain = 1.414;
  double val_contain_d = 1.414;
  float val_not_contain = 1.4;
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::contains(*arr_var1, val_contain, bret));
  ASSERT_EQ(bret, true);
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::contains(*arr_var1, val_contain_d, bret));
  ASSERT_EQ(bret, false);
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::contains(*arr_var1, val_not_contain, bret));
  ASSERT_EQ(bret, false);
}

TEST_F(TestArrayMeta, varchar_array_contains)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[\"hello\", \"hi\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ObString val_contain("hi");
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::contains(*arr_var1, val_contain, bret));
  ASSERT_EQ(bret, true);
  ObString val_not_contain("hell");
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::contains(*arr_var1, val_not_contain, bret));
  ASSERT_EQ(bret, false);
}

TEST_F(TestArrayMeta, array_nested_contains)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ObString arr1_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null]]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, arr_type1->element_type_));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());

  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(DOUBLE)"), "ARRAY(DOUBLE)");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type2 = static_cast<ObCollectionArrayType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var2));
  ObString arr2_text("[8.878, 912.33]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type2->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());

  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::contains(*arr_var1, *arr_var2, bret));
  ASSERT_EQ(bret, true);
}

TEST_F(TestArrayMeta, array_contains_all)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718, null]");
  ObString arr2_text("[1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr3_text("[1.414, null]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr4_text("[1.414, 88]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr4_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, false);

}

TEST_F(TestArrayMeta, varchar_array_contains_all)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());

  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[\"hello\", \"hi\", \"what\", \"is\", null]");
  ObString arr2_text("[\"is\", \"hi\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr3_text("[null, \"hi\"]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr4_text("[\"is\", \"what is\"]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr4_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, false);
}

TEST_F(TestArrayMeta, array_nested_contains_all)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());

  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null]]");
  ObString arr2_text("[[8.878, 912.33], [333, 12.134, null]]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr3_text("[[8.878, 912.33], null]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->contains_all(*arr_var2, bret));
  ASSERT_EQ(bret, false);
}

TEST_F(TestArrayMeta, array_overlaps)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718, null]");
  ObString arr2_text("[1.414, 2.718]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr3_text("[512, null]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr4_text("[5.31, 88]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr4_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, false);

}

TEST_F(TestArrayMeta, varchar_array_overlaps)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());

  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[\"hello\", \"hi\", \"what\", \"is\", null]");
  ObString arr2_text("[\"is\", \"hi\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr3_text("[null, \"how\"]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr4_text("[\"old\", \"what is\"]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr4_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, false);
}

TEST_F(TestArrayMeta, array_nested_overlaps)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());

  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null]]");
  ObString arr2_text("[null, [333, 12.134, null]]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bool bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, true);

  arr_var2->clear();
  ObString arr3_text("[[333, 12.134], null]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr3_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  bret = false;
  ASSERT_EQ(OB_SUCCESS, arr_var1->overlaps(*arr_var2, bret));
  ASSERT_EQ(bret, false);
}

TEST_F(TestArrayMeta, array_fix_distinct)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ObString arr1_text("[3.14, 1.414, 2.718, null, 1.414, null, 3.14, 3.14]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ObIArrayType *arr_dist = nullptr;
  ASSERT_EQ(OB_SUCCESS, arr_var1->distinct(allocator, arr_dist));

  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_dist->init());
  ASSERT_EQ(OB_SUCCESS, arr_dist->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;
}

TEST_F(TestArrayMeta, varchar_array_distinct)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[\"hello\", \"hi\", null, \"hi\", \"Hi\", null, \"what\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ObIArrayType *arr_dist = nullptr;
  ASSERT_EQ(OB_SUCCESS, arr_var1->distinct(allocator, arr_dist));

  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_dist->init());
  ASSERT_EQ(OB_SUCCESS, arr_dist->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;
}

TEST_F(TestArrayMeta, nested_array_distinct)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_dist = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ObString arr1_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null], [8.878, 912.33], [333, 12.134, null], [333, 12.134], [null]]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, arr_var1->distinct(allocator, arr_dist));

  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_dist->init());
  ASSERT_EQ(OB_SUCCESS, arr_dist->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;

}

TEST_F(TestArrayMeta, array_nested_hasset)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var2));
  ObString arr1_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null]]");
  ObString arr2_text("[[3.14159, 95.27, null, null], [8.878, 910.33], [333, 12.134, null]]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  hash::ObHashSet<ObArrayNested> nested_arrs;
  ASSERT_EQ(OB_SUCCESS, nested_arrs.create(10));
  ASSERT_EQ(OB_SUCCESS, nested_arrs.set_refactored(*static_cast<ObArrayNested *>(arr_var1), 0));
  ASSERT_EQ(OB_HASH_EXIST, nested_arrs.set_refactored(*static_cast<ObArrayNested *>(arr_var2), 0));

  arr_var2->clear();
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());
  ASSERT_EQ(OB_SUCCESS, nested_arrs.set_refactored(*static_cast<ObArrayNested *>(arr_var2), 0));
  ASSERT_EQ(2, nested_arrs.size());
  ObStringBuffer format_str(&allocator);
  hash::ObHashSet<ObArrayNested>::iterator iter = nested_arrs.begin();
  for (; iter != nested_arrs.end(); iter++) {
    ObArrayNested &arr_item = iter->first;
    ASSERT_EQ(OB_SUCCESS, arr_item.print(format_str));
    std::cout << "arr_va3: " << format_str.ptr() << std::endl;
    format_str.reset();
  }
}

TEST_F(TestArrayMeta, array_fix_remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(FLOAT)"), "ARRAY(FLOAT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_res = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string [3.14, 1.414, 2.718]
  ObString arr1_text("[3.14, 1.414, 2.718, null, 3.14]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  float val_remove = 3.14;
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::clone_except(allocator, *arr_var1, &val_remove, false, arr_res));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_res->init());
  ASSERT_EQ(OB_SUCCESS, arr_res->print(format_str));
  std::cout << "arr_va1 remove 3.14: " << format_str.ptr() << std::endl;

  arr_res->clear();
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::clone_except(allocator, *arr_var1, &val_remove, true, arr_res));
  ASSERT_EQ(OB_SUCCESS, arr_res->init());
  ASSERT_EQ(OB_SUCCESS, arr_res->print(format_str));
  std::cout << "arr_va1 remove null: " << format_str.ptr() << std::endl;
}

TEST_F(TestArrayMeta, varchar_array_remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(VARCHAR(256))"), "ARRAY(VARCHAR(256))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  // construct array from string ["hello", "world"]
  ObString arr1_text("[\"hello\", \"hi\", null, \"hi\", \"Hi\", null, \"what\"]");
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());
  ObIArrayType *arr_res = nullptr;
  ObString remove_text("hi");
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::clone_except(allocator, *arr_var1, &remove_text, false, arr_res));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_res->init());
  ASSERT_EQ(OB_SUCCESS, arr_res->print(format_str));
  std::cout << "arr_va1 remove hi: " << format_str.ptr() << std::endl;

  arr_res->clear();
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::clone_except(allocator, *arr_var1, &remove_text, true, arr_res));
  ASSERT_EQ(OB_SUCCESS, arr_res->init());
  ASSERT_EQ(OB_SUCCESS, arr_res->print(format_str));
  std::cout << "arr_va1 remove null: " << format_str.ptr() << std::endl;

}

TEST_F(TestArrayMeta, nested_array_remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("ARRAY(ARRAY(DOUBLE))"), "ARRAY(ARRAY(DOUBLE))");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *arr_var1 = nullptr;
  ObIArrayType *arr_res = nullptr;
  ObCollectionArrayType *arr_type1 = static_cast<ObCollectionArrayType *>(type1_info_parse.collection_meta_);
  ObCollectionBasicType *dst_elem_type = static_cast<ObCollectionBasicType *>(arr_type1->element_type_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type1, arr_var1));
  ObString arr1_text("[[3.14159, 95.27, null, null], [8.878, 912.33], [333, 12.134, null], null, [8.878, 912.33], [333, 12.134, null], [333, 12.134], [null]]");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr1_text, arr_var1, dst_elem_type));
  ASSERT_EQ(OB_SUCCESS, arr_var1->init());

  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("ARRAY(DOUBLE)"), "ARRAY(DOUBLE)");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *arr_var2 = nullptr;
  ObCollectionArrayType *arr_type2 = static_cast<ObCollectionArrayType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *arr_type2, arr_var2));
  ObString arr2_text("[8.878, 912.33]");
  ObCollectionBasicType *dst_elem_type2 = static_cast<ObCollectionBasicType *>(arr_type2->element_type_);
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast(allocator, arr2_text, arr_var2, dst_elem_type2));
  ASSERT_EQ(OB_SUCCESS, arr_var2->init());

  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::clone_except(allocator, *arr_var1, arr_var2, false, arr_res));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, arr_res->init());
  ASSERT_EQ(OB_SUCCESS, arr_res->print(format_str));
  std::cout << "arr_va1: " << format_str.ptr() << std::endl;

  arr_res->clear();
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, ObArrayUtil::clone_except(allocator, *arr_var1, arr_var2, true, arr_res));
  ASSERT_EQ(OB_SUCCESS, arr_res->init());
  ASSERT_EQ(OB_SUCCESS, arr_res->print(format_str));
  std::cout << "arr_va1 remove null: " << format_str.ptr() << std::endl;
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_array_meta.log");
  // OB_LOGGER.set_file_name("test_array_meta.log");
  // OB_LOGGER.set_log_level("DEBUG");
  return RUN_ALL_TESTS();
}