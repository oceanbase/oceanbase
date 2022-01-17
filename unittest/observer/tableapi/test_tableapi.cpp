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
#include <gmock/gmock.h>
#define private public

#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server.h"
#include "observer/table/ob_table_api_row_iterator.h"
#include "observer/table/ob_table_service.h"

namespace oceanbase {

namespace observer {

// #define UNUSED(x) (x)
static const int64_t TEST_COLUMN_CNT = 3;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 1;

class TestTableApi : public::testing::Test {
public:
  TestTableApi();
  virtual ~TestTableApi()
  {}
  virtual void SetUp();
  virtual void TearDown();

private:
  void prepare_schema();

protected:
  ObArenaAllocator allocator_;
  ObTableSchema table_schema_;
};

class TestObTableApiRowIterator : public ObTableApiRowIterator {
public:
  void set_table_schema(const ObTableSchema *table_schema) { table_schema_ = table_schema; }
  void set_is_init(bool is_init) { is_inited_ = is_init; }
  void set_has_gen_column(bool is_has) { has_generate_column_ = is_has; }
  void set_entity(table::ObITableEntity *entity) { _entity = entity; }
  int open() { return cons_all_columns(*_entity, true); }
  virtual int get_next_row(ObNewRow*& row);
  int cons_row(const table::ObITableEntity &entity, common::ObNewRow *&row);

private: 
  table::ObITableEntity *_entity;
};

int TestObTableApiRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  if (OB_ISNULL(_entity)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(INFO, "The entity is null, ", K(ret));
  } else if (OB_FAIL(cons_row(*_entity, row))) {
    COMMON_LOG(INFO, "Fail to construct insert row, ", K(ret));
  } else {
    //success
    COMMON_LOG(INFO, "Api insert row iter, ", K(*row));
  }
  return ret;
}

int TestObTableApiRowIterator::cons_row(const table::ObITableEntity &entity, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != entity_to_row(entity, row_objs_)) {
    COMMON_LOG(INFO, "Fail to generate row from entity", K(ret));
  } else {
    const int64_t N = missing_default_objs_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(row_objs_.push_back(missing_default_objs_.at(i)))) {
        COMMON_LOG(INFO, "Fail to push default value to row, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      row_.assign(&row_objs_.at(0), row_objs_.count());
      if (has_generate_column_ && OB_FAIL(fill_generate_columns(row_))) {
        COMMON_LOG(INFO, "Fail to fill generate columns, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_row(row_))) {
        COMMON_LOG(INFO, "Fail to check row, ", K(ret), K_(row));
      } else {
        row = &row_;
      }
    }
  }
  return ret;
}

TestTableApi::TestTableApi() : allocator_(ObModIds::TEST)
{}

void TestTableApi::SetUp()
{
  prepare_schema();
}

void TestTableApi::TearDown()
{
  table_schema_.reset();
}

void TestTableApi::prepare_schema()
{
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_tableapi"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));

  for(int32_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(ObIntType);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "c%d", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  // check rowkey column
  const ObRowkeyInfo& rowkey_info = table_schema_.get_rowkey_info();
  for (int64_t i = 0; i < rowkey_info.get_size(); ++i) {
    uint64_t column_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, rowkey_info.get_column_id(i, column_id));
  }
}

TEST_F(TestTableApi, entity_factory)
{
  table::ObTableEntityFactory<table::ObTableEntity> entity_factory;
  static const int64_t N = 100;
  static const int64_t R = 3;
  for (int round = 0; round < R; ++round) {
    for (int i = 0; i < N; ++i) {
      table::ObITableEntity *entity = entity_factory.alloc();
      ASSERT_TRUE(NULL != entity);
    } // end for
    fprintf(stderr, "used=%ld free=%ld mem_total=%ld mem_used=%ld\n",
            entity_factory.get_used_count(), entity_factory.get_free_count(),
            entity_factory.get_total_mem(), entity_factory.get_used_mem());
    entity_factory.free_and_reuse();
    fprintf(stderr, "used=%ld free=%ld mem_total=%ld mem_used=%ld\n",
            entity_factory.get_used_count(), entity_factory.get_free_count(),
            entity_factory.get_total_mem(), entity_factory.get_used_mem());
  }
}

TEST_F(TestTableApi, serialize_batch_result)
{
  ObTableBatchOperationResult result;
  table::ObTableEntity result_entity;
  ObTableOperationResult single_op_result;
  single_op_result.set_entity(result_entity);
  single_op_result.set_errno(1234);
  single_op_result.set_type(table::ObTableOperationType::INSERT_OR_UPDATE);
  single_op_result.set_affected_rows(4321);
  ASSERT_EQ(OB_SUCCESS, result.push_back(single_op_result));
  int64_t expected_len = result.get_serialize_size();
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, result.serialize(buf, 1024, pos));
  ASSERT_EQ(expected_len, pos);

  ObTableBatchOperationResult result2;
  table::ObTableEntityFactory<table::ObTableEntity> entity_factory;
  result2.set_entity_factory(&entity_factory);
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, result2.deserialize(buf, data_len, pos));
  ASSERT_EQ(1, result2.count());
  ASSERT_EQ(1234, result2.at(0).get_errno());
  ASSERT_EQ(4321, result2.at(0).get_affected_rows());
  ASSERT_EQ(table::ObTableOperationType::INSERT_OR_UPDATE, result2.at(0).type());
}

TEST_F(TestTableApi, serialize_table_query)
{
  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.add_select_column("c1"));
  ASSERT_EQ(OB_SUCCESS, query.add_select_column("c2"));
  ASSERT_EQ(OB_SUCCESS, query.add_select_column("c3"));

  ObObj pk_objs_start[2];
  pk_objs_start[0].set_int(0);
  pk_objs_start[1].set_min_value();
  ObObj pk_objs_end[2];
  pk_objs_end[0].set_int(0);
  pk_objs_end[1].set_max_value();
  ObNewRange range;
  range.start_key_.assign(pk_objs_start, 2);
  range.end_key_.assign(pk_objs_end, 2);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  ASSERT_EQ(OB_SUCCESS, query.add_scan_range(range));
  int64_t serialize_len = query.get_serialize_size();
  fprintf(stderr, "serialize_size=%ld\n", serialize_len);
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, query.serialize(buf, 1024, pos));
  ASSERT_EQ(pos, serialize_len);

  ObTableQuery query2;
  ObArenaAllocator alloc;
  query2.set_deserialize_allocator(&alloc);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, query2.deserialize(buf, serialize_len, pos));
  const ObIArray<ObString> &select_columns = query2.get_select_columns();
  const ObIArray<ObNewRange> &scan_ranges = query2.get_scan_ranges();
  ASSERT_EQ(3, select_columns.count());
  ASSERT_EQ(1, scan_ranges.count());
}

TEST_F(TestTableApi, serialize_query_result)
{
  ObTableQueryResult query_result;
  ObObj objs[3];
  objs[0].set_int(123);
  objs[1].set_null();
  objs[2].set_varchar(ObString::make_string("serialize_query_result"));
  ObNewRow row;
  row.assign(objs, 3);
  ASSERT_EQ(OB_SUCCESS, query_result.add_property_name("c1"));
  ASSERT_EQ(OB_SUCCESS, query_result.add_property_name("c2"));
  ASSERT_EQ(OB_SUCCESS, query_result.add_property_name("c3"));
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, query_result.add_row(row));
  }
  ASSERT_EQ(1024, query_result.get_row_count());
  ASSERT_EQ(3, query_result.get_property_count());
  // serialize
  char *buf = static_cast<char*>(ob_malloc(OB_MALLOC_BIG_BLOCK_SIZE, ObModIds::TEST));
  ASSERT_TRUE(nullptr != buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, query_result.serialize(buf, OB_MALLOC_BIG_BLOCK_SIZE, pos));
  ASSERT_EQ(pos, query_result.get_serialize_size());
  fprintf(stderr, "serialize_size=%ld\n", pos);
  // deserialize & check
  ObTableQueryResult query_result2;
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, query_result2.deserialize(buf, data_len, pos));
  ASSERT_EQ(1024, query_result2.get_row_count());
  ASSERT_EQ(3, query_result2.get_property_count());
  const table::ObITableEntity *entity = NULL;
  for (int64_t i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, query_result2.get_next_entity(entity));
    ASSERT_TRUE(NULL != entity);
    ASSERT_EQ(3, entity->get_properties_count());
    ObObj value;
    ASSERT_EQ(OB_SUCCESS, entity->get_property("c1", value));
    ASSERT_EQ(123, value.get_int());
    ASSERT_EQ(OB_SUCCESS, entity->get_property("c2", value));
    ASSERT_TRUE(value.is_null());
    ASSERT_EQ(OB_SUCCESS, entity->get_property("c3", value));
    ObString str;
    ASSERT_EQ(OB_SUCCESS, value.get_varchar(str));
    ASSERT_TRUE(str == ObString::make_string("serialize_query_result"));
  }
  ASSERT_EQ(OB_ITER_END, query_result2.get_next_entity(entity));
  // cleanup
  if (NULL != buf) {
    ob_free(buf);
    buf = NULL;
  }
}

TEST_F(TestTableApi, table_entity)
{
  int ret;
  ObSEArray<ObString, 1> ppts;
  // set row key
  ObObj key_objs[3];
  key_objs[0].set_varbinary("table_entity");
  key_objs[1].set_varchar("hi");
  key_objs[2].set_int(1);
  ObRowkey rk(key_objs, 3);
  // cons entity
  table::ObTableEntity entity;
  ObObj value;
  entity.set_rowkey(rk);
  ASSERT_EQ(3, entity.get_rowkey_size());
  ASSERT_EQ(0, entity.get_rowkey_value(2, value));
  ASSERT_EQ(1, value.get_int());
  // properaties
  value.set_varchar("value");
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(0, entity.set_property("c1", value));
  ASSERT_EQ(0, entity.get_property("c1", value));
  ASSERT_EQ(ObString::make_string("value"), value.get_varchar());
  ASSERT_EQ(0, entity.get_properties_names(ppts));
  ASSERT_EQ(1, ppts.count());
  ASSERT_EQ(1, entity.get_properties_count());
  // reset entity
  entity.reset();
  ASSERT_TRUE(entity.is_empty());
}

TEST_F(TestTableApi, open_and_get_next_row)
{
  ObTableOperation table_operation;
  TestObTableApiRowIterator row_iterator;

  row_iterator.set_is_init(true);
  row_iterator.set_has_gen_column(false);
  row_iterator.set_table_schema(&table_schema_);

  table::ObTableEntity entity;
  // set rowkey
  ObObj key_objs[1];
  key_objs[0].set_int(1);
  ObRowkey rk(key_objs, 1);
  entity.set_rowkey(rk);
  // set properties
  ObObj value;
  value.set_int(111);
  ASSERT_EQ(OB_SUCCESS, entity.set_property("c1", value));
  value.set_int(222);
  ASSERT_EQ(OB_SUCCESS, entity.set_property("c2", value));
  
  ObNewRow *row = nullptr;
  row_iterator.set_entity(&entity);
  ASSERT_EQ(OB_SUCCESS, row_iterator.open());
  ASSERT_EQ(OB_SUCCESS, row_iterator.get_next_row(row));
}

}  // namespace observer
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_observer.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
