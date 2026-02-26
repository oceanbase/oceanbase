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

#include <gtest/gtest.h>
#define private public    // 获取private成员
#define protected public  // 获取protect成员
#include "observer/table/adapters/ob_hbase_series_adapter.h"

using namespace oceanbase::table;

class TestSeriesConvert : public ::testing::Test {

public:
  TestSeriesConvert() {};

  virtual ~TestSeriesConvert() {};
  virtual void SetUp() {}
  virtual void TearDown() {}

private:
  template <typename T, size_t N>
  void test_more_type(const T (&value_array)[N], ObHSeriesAdapter *adapter)
  {
    ObString V = ObString::make_string("V");
    ASSERT_TRUE(NULL != adapter);
    ObTableEntityFactory<ObTableEntity> entity_factory;
    ObITableEntity *entity = NULL;
    int64_t COLUMNS_SIZE = 10;
    char qualifier[COLUMNS_SIZE][128];
    char values[COLUMNS_SIZE][128];
    ObArenaAllocator alloc;
    ObString row("key");
    for (int i = 0; i < COLUMNS_SIZE; ++i) {
      sprintf(qualifier[i], "cq%d", i);
      sprintf(values[i], "value_string%d", i);
    }
    ObObj key1, key2, key3;
    ObObj value;
    ObSEArray<const ObITableEntity *, 16> entities;
    int32_t COLUMN_COUNT_1 = 8;
    int32_t COLUMN_COUNT_2 = 5;
    ObSEArray<const ObITableEntity *, 16> series_entities;
    ObSEArray<ObTabletID, 16> real_tablet_ids;
    entities.reset();

    key1.set_varbinary(row);
    key3.set_int(1);
    ASSERT_EQ(COLUMN_COUNT_2, N);
    for (int64_t i = 0; i < COLUMN_COUNT_2; ++i) {
      key2.set_varbinary(ObString::make_string(qualifier[i]));
      entity = entity_factory.alloc();
      ASSERT_TRUE(NULL != entity);
      ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key1));
      ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key2));
      ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key3));
      ObTabletID tablet_id(1);
      entity->set_tablet_id(tablet_id);
      T *int_val = static_cast<T *>(alloc.alloc(sizeof(T)));
      *int_val = value_array[i];
      char *buf = (char *)int_val;
      value.set_varbinary(ObString(sizeof(T), buf));
      ASSERT_EQ(OB_SUCCESS, entity->set_property(V, value));
      ASSERT_EQ(OB_SUCCESS, entities.push_back(entity));
    }

    ASSERT_EQ(OB_SUCCESS, adapter->convert_normal_to_series(entities, series_entities, real_tablet_ids));
    ASSERT_EQ(1, series_entities.count());
    ASSERT_EQ(1, real_tablet_ids.count());

    {
      entity = NULL;
      entity = const_cast<ObITableEntity*>(series_entities.at(0));
      ASSERT_TRUE(NULL != entity);
      ObObj timestamp;
      ASSERT_EQ(OB_SUCCESS, entity->get_rowkey_value(1, timestamp));
      int k = timestamp.get_int();
      ObObj &json_obj = const_cast<ObObj &>(entity->get_properties_value(0));
      ASSERT_TRUE(json_obj.has_lob_header());
      ASSERT_TRUE(is_lob_storage(json_obj.get_type()));
      ASSERT_EQ(OB_SUCCESS, ObTableCtx::read_real_lob(alloc, json_obj));
      ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_string_len(), &alloc);
      ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
      ASSERT_EQ(COLUMN_COUNT_2, bin.element_count());
      ObJsonBuffer buf(&alloc);
      ObIJsonBase *j_base = &bin;
      ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_base->json_type());
      for (int64_t i = 0; i < COLUMN_COUNT_2; ++i) {
        ObString q(qualifier[i]);
        ObIJsonBase *j_bin_sub = NULL;
        ASSERT_EQ(OB_SUCCESS, bin.get_object_value(q, j_bin_sub));
        ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_sub->json_type());
        T *expect_val = static_cast<T *>(alloc.alloc(sizeof(T)));
        *expect_val = value_array[i];
        char *expect_buf = (char *)expect_val;
        ObString element(sizeof(T), expect_buf);
        uint64_t data_len = j_bin_sub->get_data_length();
        const char *data = j_bin_sub->get_data();
        ObString actual(data_len, data);
        ASSERT_EQ(0, element.compare(actual));
      }
    }

    alloc.reuse();
  }

private:
  ObArenaAllocator alloc;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSeriesConvert);
};

// test normal cell convert to series cell
TEST_F(TestSeriesConvert, htable_normal_convert_series)
{
  ObString V = ObString::make_string("V");
  ObHSeriesAdapter *adapter = new ObHSeriesAdapter();
  ASSERT_TRUE(NULL != adapter);
  ObTableEntityFactory<ObTableEntity> entity_factory;
  ObITableEntity *entity = NULL;
  int64_t COLUMNS_SIZE = 10;
  char qualifier[COLUMNS_SIZE][128];
  char values[COLUMNS_SIZE][128];
  ObString row("key");
  for (int i = 0; i < COLUMNS_SIZE; ++i) {
    sprintf(qualifier[i], "cq%d", i);
    sprintf(values[i], "value_string%d", i);
  }  // end for
  ObObj key1, key2, key3;
  ObObj value;
  ObSEArray<const ObITableEntity *, 16> entities;
  int32_t COLUMN_COUNT_1 = 8;
  int32_t COLUMN_COUNT_2 = 5;

  // -------------------test varchar-------------------
  key1.set_varbinary(row);
  key3.set_int(1);
  for (int64_t i = 0; i < COLUMN_COUNT_1; ++i) {
    key2.set_varbinary(ObString::make_string(qualifier[i]));
    entity = entity_factory.alloc();
    ASSERT_TRUE(NULL != entity);
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key1));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key2));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key3));
    ObTabletID tablet_id(1);
    entity->set_tablet_id(tablet_id);
    value.set_varbinary(ObString::make_string(values[i]));
    ASSERT_EQ(OB_SUCCESS, entity->set_property(V, value));
    ASSERT_EQ(OB_SUCCESS, entities.push_back(entity));
  }  // end for

  key3.set_int(2);
  for (int64_t i = 0; i < COLUMN_COUNT_2; ++i) {
    key2.set_varbinary(ObString::make_string(qualifier[i]));
    entity = entity_factory.alloc();
    ASSERT_TRUE(NULL != entity);
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key1));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key2));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key3));
    ObTabletID tablet_id(1);
    entity->set_tablet_id(tablet_id);
    value.set_varbinary(ObString::make_string(values[i]));
    ASSERT_EQ(OB_SUCCESS, entity->set_property(V, value));
    ASSERT_EQ(OB_SUCCESS, entities.push_back(entity));
  }  // end for

  ObSEArray<const ObITableEntity *, 16> series_entities;
  ObSEArray<ObTabletID, 16> real_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, adapter->convert_normal_to_series(entities, series_entities, real_tablet_ids));
  ASSERT_EQ(2, series_entities.count());
  ASSERT_EQ(2, real_tablet_ids.count());

  {
    entity = NULL;
    entity = const_cast<ObITableEntity*>(series_entities.at(0));
    ASSERT_TRUE(NULL != entity);
    ObObj timestamp;
    ASSERT_EQ(OB_SUCCESS, entity->get_rowkey_value(1, timestamp));
    int k = timestamp.get_int();
    ObObj &json_obj = const_cast<ObObj &>(entity->get_properties_value(0));
    ASSERT_TRUE(json_obj.has_lob_header());
    ASSERT_TRUE(is_lob_storage(json_obj.get_type()));
    ASSERT_EQ(OB_SUCCESS, ObTableCtx::read_real_lob(alloc, json_obj));
    ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_string_len(), &alloc);
    ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
    ASSERT_EQ(COLUMN_COUNT_1, bin.element_count());
    ObJsonBuffer buf(&alloc);
    ObIJsonBase *j_base = &bin;
    ASSERT_EQ(OB_SUCCESS, j_base->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    std::string actual = "{\"cq0\": \"value_string0\", \"cq1\": \"value_string1\", \"cq2\": \"value_string2\", "
                         "\"cq3\": \"value_string3\", \"cq4\": \"value_string4\", \"cq5\": \"value_string5\", \"cq6\": "
                         "\"value_string6\", \"cq7\": \"value_string7\"}";
    EXPECT_STREQ(buf.ptr(), actual.c_str());
  }

  {
    entity = NULL;
    entity = const_cast<ObITableEntity*>(series_entities.at(1));
    ASSERT_TRUE(NULL != entity);
    ObObj timestamp;
    ASSERT_EQ(OB_SUCCESS, entity->get_rowkey_value(1, timestamp));
    int k = timestamp.get_int();
    ObObj &json_obj = const_cast<ObObj &>(entity->get_properties_value(0));
    ASSERT_TRUE(json_obj.has_lob_header());
    ASSERT_TRUE(is_lob_storage(json_obj.get_type()));
    ASSERT_EQ(OB_SUCCESS, ObTableCtx::read_real_lob(alloc, json_obj));
    ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_string_len(), &alloc);
    ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
    ASSERT_EQ(COLUMN_COUNT_2, bin.element_count());
    ObJsonBuffer buf(&alloc);
    ObIJsonBase *j_base = &bin;
    ASSERT_EQ(OB_SUCCESS, j_base->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    std::string actual = "{\"cq0\": \"value_string0\", \"cq1\": \"value_string1\", \"cq2\": \"value_string2\", "
                         "\"cq3\": \"value_string3\", \"cq4\": \"value_string4\"}";
    EXPECT_STREQ(buf.ptr(), actual.c_str());
  }

  series_entities.reset();
  real_tablet_ids.reset();
  entities.reset();
  alloc.reuse();

  // test set use_lexicographical_order
  key3.set_int(3);
  char qualifier_non_order[COLUMN_COUNT_1][128];
  sprintf(qualifier[0], "cq7");
  sprintf(qualifier[1], "cq71");
  sprintf(qualifier[2], "cq742");
  sprintf(qualifier[3], "cq712");
  sprintf(qualifier[4], "cq93");
  sprintf(qualifier[5], "cq123");
  sprintf(qualifier[6], "cq01");
  sprintf(qualifier[7], "cq73");
  for (int64_t i = 0; i < COLUMN_COUNT_1; ++i) {
    key2.set_varbinary(ObString::make_string(qualifier[i]));
    entity = entity_factory.alloc();
    ASSERT_TRUE(NULL != entity);
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key1));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key2));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key3));
    ObTabletID tablet_id(1);
    entity->set_tablet_id(tablet_id);
    value.set_varbinary(ObString::make_string(values[i]));
    ASSERT_EQ(OB_SUCCESS, entity->set_property(V, value));
    ASSERT_EQ(OB_SUCCESS, entities.push_back(entity));
  }  // end for

  ASSERT_EQ(OB_SUCCESS, adapter->convert_normal_to_series(entities, series_entities, real_tablet_ids));
  ASSERT_EQ(1, series_entities.count());
  ASSERT_EQ(1, real_tablet_ids.count());

  {
    entity = NULL;
    entity = const_cast<ObITableEntity*>(series_entities.at(0));
    ASSERT_TRUE(NULL != entity);
    ObObj timestamp;
    ASSERT_EQ(OB_SUCCESS, entity->get_rowkey_value(1, timestamp));
    int k = timestamp.get_int();
    ObObj &json_obj = const_cast<ObObj &>(entity->get_properties_value(0));
    ASSERT_TRUE(json_obj.has_lob_header());
    ASSERT_TRUE(is_lob_storage(json_obj.get_type()));
    ASSERT_EQ(OB_SUCCESS, ObTableCtx::read_real_lob(alloc, json_obj));
    ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_string_len(), &alloc);
    ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
    ASSERT_EQ(COLUMN_COUNT_1, bin.element_count());
    ObJsonBuffer buf(&alloc);
    ObIJsonBase *j_base = &bin;
    ASSERT_EQ(OB_SUCCESS, j_base->print(buf, true));
    std::cout << buf.ptr() << std::endl;
    std::string actual = "{\"cq01\": \"value_string6\", \"cq123\": \"value_string5\", \"cq7\": \"value_string0\", "
                           "\"cq71\": \"value_string1\", \"cq712\": \"value_string3\", \"cq73\": \"value_string7\", "
                           "\"cq742\": \"value_string2\", \"cq93\": \"value_string4\"}";
    EXPECT_STREQ(buf.ptr(), actual.c_str());
  }

  alloc.reuse();

  // -------------------test int64_t-------------------
  int64_t value_array1[] = {3, 5, 1, 9, 6};
  test_more_type(value_array1, adapter);

  // -------------------test int32_t-------------------
  int32_t value_array2[] = {3, 5, 1, 9, 6};
  test_more_type(value_array2, adapter);

  // -------------------test int16_t-------------------
  int16_t value_array3[] = {3, 5, 1, 9, 6};
  test_more_type(value_array3, adapter);

  // -------------------test int8_t-------------------
  int8_t value_array4[] = {3, 5, 1, 9, 6};
  test_more_type(value_array4, adapter);

  // -------------------test uint64_t-------------------
  uint64_t value_array5[] = {3, 5, 1, 9, 6};
  test_more_type(value_array5, adapter);

  // -------------------test uint32_t-------------------
  uint32_t value_array6[] = {3, 5, 1, 9, 6};
  test_more_type(value_array6, adapter);

  // -------------------test uint16_t-------------------
  uint16_t value_array7[] = {3, 5, 1, 9, 6};
  test_more_type(value_array7, adapter);

  // -------------------test uint8_t-------------------
  uint8_t value_array8[] = {3, 5, 1, 9, 6};
  test_more_type(value_array8, adapter);

  // -------------------test bool-------------------
  bool value_array9[] = {true, false, false, true, false};
  test_more_type(value_array9, adapter);

  // -------------------test float-------------------
  float value_array10[] = {3.5, 5.1, 1.864, 9.1, 6.32};
  test_more_type(value_array10, adapter);

  // -------------------test double-------------------
  double value_array11[] = {3.49353194381249, 5.123, 1.312741328948, 9.21, 6};
  test_more_type(value_array11, adapter);

  // -------------------test null-------------------
  series_entities.reset();
  entities.reset();
  real_tablet_ids.reset();

  key1.set_varbinary(row);
  key3.set_int(1);
  for (int64_t i = 0; i < COLUMN_COUNT_2; ++i) {
    key2.set_varbinary(ObString::make_string(qualifier[i]));
    entity = entity_factory.alloc();
    ASSERT_TRUE(NULL != entity);
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key1));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key2));
    ASSERT_EQ(OB_SUCCESS, entity->add_rowkey_value(key3));
    ObTabletID tablet_id(1);
    entity->set_tablet_id(tablet_id);
    value.set_null();
    ASSERT_EQ(OB_SUCCESS, entity->set_property(V, value));
    ASSERT_EQ(OB_SUCCESS, entities.push_back(entity));
  }

  ASSERT_EQ(OB_SUCCESS, adapter->convert_normal_to_series(entities, series_entities, real_tablet_ids));
  ASSERT_EQ(1, series_entities.count());
  ASSERT_EQ(1, real_tablet_ids.count());

  {
    entity = NULL;
    entity = const_cast<ObITableEntity*>(series_entities.at(0));
    ASSERT_TRUE(NULL != entity);
    ObObj timestamp;
    ASSERT_EQ(OB_SUCCESS, entity->get_rowkey_value(1, timestamp));
    int k = timestamp.get_int();
    ObObj &json_obj = const_cast<ObObj &>(entity->get_properties_value(0));
    ASSERT_TRUE(json_obj.has_lob_header());
    ASSERT_TRUE(is_lob_storage(json_obj.get_type()));
    ASSERT_EQ(OB_SUCCESS, ObTableCtx::read_real_lob(alloc, json_obj));
    ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_string_len(), &alloc);
    ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
    ASSERT_EQ(COLUMN_COUNT_2, bin.element_count());
    ObJsonBuffer buf(&alloc);
    ObIJsonBase *j_base = &bin;
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_base->json_type());
    for (int64_t i = 0; i < COLUMN_COUNT_2; ++i) {
      ObString q(qualifier[i]);
      ObIJsonBase *j_bin_sub = NULL;
      ASSERT_EQ(OB_SUCCESS, bin.get_object_value(q, j_bin_sub));
      ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin_sub->json_type());
      ObString element;
      uint64_t data_len = j_bin_sub->get_data_length();
      const char *data = j_bin_sub->get_data();
      ObString actual(data_len, data);
      ASSERT_EQ(0, element.compare(actual));
    }
  }

  alloc.reuse();

  adapter->~ObHSeriesAdapter();
  adapter = NULL;
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_series_convert.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
