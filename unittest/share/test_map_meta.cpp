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
class TestMapMeta : public ::testing::Test
{
public:
  TestMapMeta()
  {}
  ~TestMapMeta()
  {}

private:
  ObArenaAllocator allocator_; 
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMapMeta);
};

class TestSparseVectorMeta : public ::testing::Test
{
public:
  TestSparseVectorMeta()
  {}
  ~TestSparseVectorMeta()
  {}

private:
  ObArenaAllocator allocator_; 
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSparseVectorMeta);
};

TEST_F(TestMapMeta, ObCollectionMapType)
{
  // MAP(INT,VARCHAR(256))
  ObArenaAllocator allocator(ObModIds::TEST);
  ObCollectionBasicType int_type;
  int_type.basic_meta_.meta_.set_int32();
  int_type.basic_meta_.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObInt32Type].scale_);
  int_type.basic_meta_.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObInt32Type].precision_);
  int_type.type_id_ = ObNestedType::OB_BASIC_TYPE;
  ObCollectionBasicType varchar_type;
  varchar_type.basic_meta_.meta_.set_varchar();
  varchar_type.basic_meta_.set_length(256);
  varchar_type.basic_meta_.meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  varchar_type.basic_meta_.meta_.set_collation_level(CS_LEVEL_COERCIBLE);
  varchar_type.type_id_ = ObNestedType::OB_BASIC_TYPE;
  ObCollectionArrayType key_type(allocator);
  key_type.element_type_ = &int_type;
  key_type.type_id_ = ObNestedType::OB_ARRAY_TYPE;
  ObCollectionArrayType value_type(allocator);
  value_type.element_type_ = &varchar_type;
  value_type.type_id_ = ObNestedType::OB_ARRAY_TYPE;
  ObCollectionMapType map_type(allocator);
  map_type.key_type_ = &key_type;
  map_type.value_type_ = &value_type;
  map_type.type_id_ = ObNestedType::OB_MAP_TYPE;

  // serialize - deserialize
  char buf[1024] = {0};
  int64_t pos = 0;
  ASSERT_EQ(57,map_type.get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, map_type.serialize(buf, 1024, pos));
  ObCollectionMapType map_type_res(allocator);
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, map_type_res.deserialize(buf, data_len, pos));
  ASSERT_EQ(map_type_res.type_id_, ObNestedType::OB_MAP_TYPE);
  ObCollectionArrayType *key_type_res = reinterpret_cast<ObCollectionArrayType*>(map_type_res.key_type_);
  ASSERT_EQ(key_type_res->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *int_type_res = reinterpret_cast<ObCollectionBasicType*>(key_type_res->element_type_);
  ASSERT_EQ(int_type_res->type_id_, ObNestedType::OB_BASIC_TYPE);
  ASSERT_EQ(int_type_res->basic_meta_, int_type.basic_meta_);
  ObCollectionArrayType *value_type_res = reinterpret_cast<ObCollectionArrayType*>(map_type_res.value_type_);
  ASSERT_EQ(value_type_res->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *varchar_type_res = reinterpret_cast<ObCollectionBasicType*>(value_type_res->element_type_);
  ASSERT_EQ(varchar_type_res->basic_meta_, varchar_type.basic_meta_);

  // parse type info
  ObString type_name(strlen("MAP(INT,VARCHAR(256))"), "MAP(INT,VARCHAR(256))");
  ObSqlCollectionInfo type_info(allocator);
  type_info.set_name(type_name);
  ASSERT_EQ(OB_SUCCESS, type_info.parse_type_info());
  ObCollectionMapType *map_type_res2 = static_cast<ObCollectionMapType *>(type_info.collection_meta_);
  ASSERT_EQ(map_type_res2->type_id_, ObNestedType::OB_MAP_TYPE);
  ObCollectionArrayType *key_type_res2 = reinterpret_cast<ObCollectionArrayType*>(map_type_res2->key_type_);
  ASSERT_EQ(key_type_res2->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *int_type_res2 = reinterpret_cast<ObCollectionBasicType*>(key_type_res2->element_type_);
  ASSERT_EQ(int_type_res2->type_id_, ObNestedType::OB_BASIC_TYPE);
  ASSERT_EQ(int_type_res2->basic_meta_, int_type.basic_meta_);
  ObCollectionArrayType *value_type_res2 = reinterpret_cast<ObCollectionArrayType*>(map_type_res2->value_type_);
  ASSERT_EQ(value_type_res2->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *varchar_type_res2 = reinterpret_cast<ObCollectionBasicType*>(value_type_res2->element_type_);
  ASSERT_EQ(varchar_type_res2->basic_meta_, varchar_type.basic_meta_);
}

TEST_F(TestMapMeta, map_construct)
{
  // MAP(INT,INT)
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSqlCollectionInfo type1_info_parse(allocator);
  ObString type1_name(strlen("MAP(INT,INT)"), "MAP(INT,INT)");
  type1_info_parse.set_name(type1_name);
  ASSERT_EQ(OB_SUCCESS, type1_info_parse.parse_type_info());
  ObIArrayType *map_var1 = nullptr;
  ObCollectionMapType *map_type1 = static_cast<ObCollectionMapType *>(type1_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *map_type1, map_var1));
  ObString map1_text("{1:11,2:22,3:33,NULL:0}");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast_map(allocator, map1_text, map_var1, map_type1, 0));
  ObStringBuffer format_str(&allocator);
  ASSERT_EQ(OB_SUCCESS, map_var1->init());
  ASSERT_EQ(OB_SUCCESS, map_var1->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("{NULL:0,1:11,2:22,3:33}"));

  // // MAP(VARCHAR(10),VARCHAR(256))
  ObSqlCollectionInfo type2_info_parse(allocator);
  ObString type2_name(strlen("MAP(VARCHAR(10),VARCHAR(256))"), "MAP(VARCHAR(10),VARCHAR(256))");
  type2_info_parse.set_name(type2_name);
  ASSERT_EQ(OB_SUCCESS, type2_info_parse.parse_type_info());
  ObIArrayType *map_var2 = nullptr;
  ObCollectionMapType *map_type2 = static_cast<ObCollectionMapType *>(type2_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *map_type2, map_var2));
  ObString map2_text("{1:11,\"2\":22,\"3\":\"33\",NULL:0}");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast_map(allocator, map2_text, map_var2, map_type2, 0));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, map_var2->init());
  ASSERT_EQ(OB_SUCCESS, map_var2->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("{NULL:\"0\",\"1\":\"11\",\"2\":\"22\",\"3\":\"33\"}"));

  // MAP(float,ARRAY(float))
  ObSqlCollectionInfo type3_info_parse(allocator);
  ObString type3_name(strlen("MAP(FLOAT,ARRAY(FLOAT))"), "MAP(FLOAT,ARRAY(FLOAT))");
  type3_info_parse.set_name(type3_name);
  ASSERT_EQ(OB_SUCCESS, type3_info_parse.parse_type_info());
  ObIArrayType *map_var3 = nullptr;
  ObCollectionMapType *map_type3 = static_cast<ObCollectionMapType *>(type3_info_parse.collection_meta_);
  ASSERT_EQ(OB_SUCCESS, ObArrayTypeObjFactory::construct(allocator, *map_type3, map_var3));
  ObString map3_text("{1.1:[11.1],2.2:[22.2],3.3:[33.3],NULL:[0]}");
  ASSERT_EQ(OB_SUCCESS, sql::ObArrayCastUtils::string_cast_map(allocator, map3_text, map_var3, map_type3, 0));
  format_str.reset();
  ASSERT_EQ(OB_SUCCESS, map_var3->init());
  ASSERT_EQ(OB_SUCCESS, map_var3->print(format_str));
  ASSERT_EQ(0, format_str.string().compare("{NULL:[0],1.1:[11.1],2.2:[22.2],3.3:[33.3]}"));
}

TEST_F(TestSparseVectorMeta, ObCollectionMapType)
{
  // TestSparseVectorMeta ==> MAP(UINT32, FLOAT)
  ObArenaAllocator allocator(ObModIds::TEST);
  ObCollectionBasicType uint_type;
  uint_type.basic_meta_.meta_.set_uint32();
  uint_type.basic_meta_.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt32Type].scale_);
  uint_type.basic_meta_.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt32Type].precision_);
  uint_type.type_id_ = ObNestedType::OB_BASIC_TYPE;
  ObCollectionBasicType float_type;
  float_type.basic_meta_.meta_.set_float();
  float_type.type_id_ = ObNestedType::OB_BASIC_TYPE;
  ObCollectionArrayType key_type(allocator);
  key_type.element_type_ = &uint_type;
  key_type.type_id_ = ObNestedType::OB_ARRAY_TYPE;
  ObCollectionArrayType value_type(allocator);
  value_type.element_type_ = &float_type;
  value_type.type_id_ = ObNestedType::OB_ARRAY_TYPE;
  ObCollectionMapType sparse_vec_type(allocator);
  sparse_vec_type.key_type_ = &key_type;
  sparse_vec_type.value_type_ = &value_type;
  sparse_vec_type.type_id_ = ObNestedType::OB_SPARSE_VECTOR_TYPE;

  // serialize - deserialize
  char buf[1024] = {0};
  int64_t pos = 0;
  ASSERT_EQ(57,sparse_vec_type.get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, sparse_vec_type.serialize(buf, 1024, pos));
  ObCollectionMapType map_type_res(allocator);
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, sparse_vec_type.deserialize(buf, data_len, pos));
  ASSERT_EQ(sparse_vec_type.type_id_, ObNestedType::OB_SPARSE_VECTOR_TYPE);
  ObCollectionArrayType *key_type_res = reinterpret_cast<ObCollectionArrayType*>(sparse_vec_type.key_type_);
  ASSERT_EQ(key_type_res->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *uint_type_res = reinterpret_cast<ObCollectionBasicType*>(key_type_res->element_type_);
  ASSERT_EQ(uint_type_res->type_id_, ObNestedType::OB_BASIC_TYPE);
  ASSERT_EQ(uint_type_res->basic_meta_, uint_type.basic_meta_);
  ObCollectionArrayType *value_type_res = reinterpret_cast<ObCollectionArrayType*>(sparse_vec_type.value_type_);
  ASSERT_EQ(value_type_res->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *float_type_res = reinterpret_cast<ObCollectionBasicType*>(value_type_res->element_type_);
  ASSERT_EQ(float_type_res->basic_meta_, float_type.basic_meta_);

  // parse type info
  ObString type_name(strlen("SPARSEVECTOR"), "SPARSEVECTOR");
  ObSqlCollectionInfo type_info(allocator);
  type_info.set_name(type_name);
  ASSERT_EQ(OB_SUCCESS, type_info.parse_type_info());
  ObCollectionMapType *sparse_vec_type_res2 = static_cast<ObCollectionMapType *>(type_info.collection_meta_);
  ASSERT_EQ(sparse_vec_type_res2->type_id_, ObNestedType::OB_SPARSE_VECTOR_TYPE);
  ObCollectionArrayType *key_type_res2 = reinterpret_cast<ObCollectionArrayType*>(sparse_vec_type_res2->key_type_);
  ASSERT_EQ(key_type_res2->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *uint_type_res2 = reinterpret_cast<ObCollectionBasicType*>(key_type_res2->element_type_);
  ASSERT_EQ(uint_type_res2->type_id_, ObNestedType::OB_BASIC_TYPE);
  ASSERT_EQ(uint_type_res2->basic_meta_, uint_type.basic_meta_);
  ObCollectionArrayType *value_type_res2 = reinterpret_cast<ObCollectionArrayType*>(sparse_vec_type_res2->value_type_);
  ASSERT_EQ(value_type_res2->type_id_, ObNestedType::OB_ARRAY_TYPE);
  ObCollectionBasicType *float_type_res2 = reinterpret_cast<ObCollectionBasicType*>(value_type_res2->element_type_);
  ASSERT_EQ(float_type_res2->basic_meta_, float_type.basic_meta_);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}