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
#include "observer/table/ob_table_ttl_manager.h"
#include "rootserver/ob_ttl_scheduler.h"
#include "share/table/ob_ttl_util.h"

namespace oceanbase {

namespace observer {

using namespace oceanbase::table;
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

#if 0

class TestTableTTL : public::testing::Test {
public:
  TestTableTTL() {}
  virtual ~TestTableTTL() {}
  virtual void SetUp(); /*global event*/
  virtual void TearDown();
  void insert_one_partition_task();

  common::ObMySQLProxy sql_proxy_;
};

void TestTableTTL::SetUp()
{
  ObTTLManager::get_instance().event_delay_ = 20 * 1000L;
  ObTTLManager::get_instance().periodic_delay_ = 20 * 1000L;
  ObTTLManager::get_instance().sql_proxy_ = &sql_proxy_;
  ObTTLManager::get_instance().init();
  ObTTLManager::get_instance().start();
  usleep(100 * 1000L);
}

void TestTableTTL::TearDown()
{
  ObTTLManager::get_instance().stop();
}

TEST_F(TestTableTTL, proc_rs_cmd)
{
  uint64_t tenant_id[3] = {1, 2, 3};
  uint64_t table_id[4];
  ObPartitionKey pk[5];
  int64_t task_id1 = 1;
  ObTTLTaskInfo task_info;
  ObTTLPara para;

  table_id[0] = combine_id(tenant_id[0], 1);
  table_id[1] = combine_id(tenant_id[1], 2);
  table_id[2] = combine_id(tenant_id[1], 1);
  table_id[3] = combine_id(tenant_id[2], 1);

  pk[0] = ObPartitionKey(table_id[0], 1, 1);
  pk[1] = ObPartitionKey(table_id[1], 1, 2);
  pk[2] = ObPartitionKey(table_id[2], 1, 3);
  pk[3] = ObPartitionKey(table_id[3], 1, 4);
  pk[4] = ObPartitionKey(table_id[3], 2, 5);
  
  ObTTLTaskCtx* ctx = NULL;
  int64_t rsp_time = OB_INVALID_ID;
  ObTTLManager::ObTTLTenantInfo* tenant_ttl = NULL;
  ObTTLManager& ttl_mgr = ObTTLManager::get_instance();
  /*1.1 simulate: rs send a trigger request*/
  ttl_mgr.proc_rs_cmd(tenant_id[0], task_id1, true, OB_TTL_TASK_RUNNING);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ASSERT_EQ(1, ttl_mgr.ttl_tenant_parts_map_.size());
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_EQ(tenant_ttl->state_, OB_TTL_TASK_RUNNING);
    ASSERT_EQ(tenant_ttl->is_usr_trigger_, true);
    rsp_time = tenant_ttl->rsp_time_;
  }

  /*1.2. simulate: new a partition task*/
  {
    for (int i = 0; i<5; i++) {
      if (pk[i].get_tenant_id() == tenant_id[0]) {
        task_info.pkey_ = pk[i];
        task_info.task_id_ = task_id1;
        ttl_mgr.generate_one_partition_task(task_info, para);
        common::ObSpinLockGuard guard(ttl_mgr.lock_);
        ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
        ASSERT_TRUE(NULL != ctx);
        ASSERT_EQ(ctx->task_status_ , OB_TTL_TASK_PREPARE);
      }      
    }
  }

  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ASSERT_EQ(tenant_ttl->part_task_map_.size() , 1);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_EQ(ctx->task_status_ , OB_TTL_TASK_PREPARE);

    /*1.3. simulate: sync sys table logical*/
    ctx->is_dirty_ = false;
    ctx->task_status_ = OB_TTL_TASK_RUNNING;
    ctx->rsp_time_ = tenant_ttl->rsp_time_;
  }

  /*1.4. reponse to rs*/
  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ASSERT_EQ(tenant_ttl->rsp_time_ , OB_INVALID_ID); 
    ASSERT_EQ(tenant_ttl->is_dirty_ , false); 
  }

  /*5.1 retrigger*/
  ttl_mgr.proc_rs_cmd(tenant_id[0], 3, true, OB_TTL_TASK_RUNNING);
  /*mock response rs*/
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ctx->is_dirty_ = false;
    ctx->rsp_time_ = tenant_ttl->rsp_time_;
  }
  ctx->rsp_time_ = tenant_ttl->rsp_time_;
  /*5.1. reponse to rs*/
  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ASSERT_EQ(tenant_ttl->rsp_time_ , OB_INVALID_ID);
    ASSERT_EQ(tenant_ttl->is_dirty_ , false);
    ASSERT_EQ(tenant_ttl->task_id_ , 3);
  }

  /*2.1 simulate: rs send a pending request*/
  ttl_mgr.proc_rs_cmd(tenant_id[0], task_id1, true, OB_TTL_TASK_PENDING);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(tenant_ttl->state_, OB_TTL_TASK_PENDING);
    ASSERT_TRUE(tenant_ttl->rsp_time_ != OB_INVALID_ID);
    ASSERT_TRUE(tenant_ttl->rsp_time_ != ctx->rsp_time_);
    ASSERT_EQ(tenant_ttl->is_dirty_, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_RUNNING);
    ASSERT_EQ(ctx->is_dirty_, false);
  }

  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(ctx->is_dirty_, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_PENDING);
    
    /*2.3. simulate: sync sys table logical*/
    ctx->is_dirty_ = false;
    ctx->rsp_time_ = tenant_ttl->rsp_time_;
  }

  /*2.4 wait rsp*/
  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(tenant_ttl->rsp_time_ , OB_INVALID_ID); 
    ASSERT_EQ(tenant_ttl->is_dirty_ , false); 
    ASSERT_EQ(tenant_ttl->state_ , ctx->task_status_); 
  }

  /*3.1 simulate: rs send a resume request*/
  ttl_mgr.proc_rs_cmd(tenant_id[0], task_id1, true, OB_TTL_TASK_RUNNING);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(tenant_ttl->state_, OB_TTL_TASK_RUNNING);
    ASSERT_TRUE(tenant_ttl->rsp_time_ != OB_INVALID_ID);
    ASSERT_TRUE(tenant_ttl->rsp_time_ != ctx->rsp_time_);
    ASSERT_EQ(tenant_ttl->is_dirty_, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_PENDING);
    ASSERT_EQ(ctx->is_dirty_, false);
  }

  usleep(100 * 1000L);
  {
    /*3.2. simulate: sync sys table logical*/
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(ctx->is_dirty_, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_RUNNING);
    
    /*3.3. simulate: sync sys table logical*/
    ctx->is_dirty_ = false;
    ctx->rsp_time_ = tenant_ttl->rsp_time_;
  }

  /*3.4 wait rsp*/
  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(tenant_ttl->rsp_time_ , OB_INVALID_ID);
    ASSERT_EQ(tenant_ttl->is_dirty_ , false);
    ASSERT_EQ(ctx->task_status_ , OB_TTL_TASK_RUNNING);
  }

    /*4.1 simulate: rs send a resume request*/
  ttl_mgr.proc_rs_cmd(tenant_id[0], task_id1, true, OB_TTL_TASK_CANCEL);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(tenant_ttl->state_, OB_TTL_TASK_CANCEL);
    ASSERT_TRUE(tenant_ttl->rsp_time_ != OB_INVALID_ID);
    ASSERT_TRUE(tenant_ttl->rsp_time_ != ctx->rsp_time_);
    ASSERT_EQ(tenant_ttl->is_dirty_, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_RUNNING);
    ASSERT_EQ(ctx->is_dirty_, false);
  }

  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_TRUE(NULL != ctx);
    ASSERT_EQ(ctx->is_dirty_, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_CANCEL);
    
    /*4.2. simulate: sync sys table logical*/
    ctx->is_dirty_ = false;
    ctx->rsp_time_ = tenant_ttl->rsp_time_;
  }

  /*4.3 wait rsp*/
  usleep(100 * 1000L);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(tenant_id[0], false);
    ctx = ttl_mgr.get_one_partition_ctx(pk[0]);
    ASSERT_TRUE(NULL == tenant_ttl);
    ASSERT_TRUE(NULL == ctx);
  }
}

TEST_F(TestTableTTL, dag_report)
{
  ObTTLManager& ttl_mgr = ObTTLManager::get_instance();
  ObPartitionKey pkey(combine_id(1, 1) ,1 , 1);
  ObTTLTaskCtx* ctx = NULL;
  ObTTLManager::ObTTLTenantInfo* tenant_ttl = NULL;
  ObTTLTaskInfo task_info;
  ObTTLPara para;
  bool is_stop = false;
  int ret = OB_SUCCESS;
  uint32_t tenant_id = 1001;
  uint32_t tenant_id1 = 1002;

  /*1.1 simulate: rs send a trigger request*/
  ttl_mgr.proc_rs_cmd(pkey.get_tenant_id(), 1, true, OB_TTL_TASK_RUNNING);
  {
    common::ObSpinLockGuard guard(ttl_mgr.lock_);
    tenant_ttl = (ObTTLManager::ObTTLTenantInfo*)ttl_mgr.get_tenant_info(pkey.get_tenant_id(), false);
    ASSERT_EQ(1, ttl_mgr.ttl_tenant_parts_map_.size());
    ASSERT_TRUE(NULL != tenant_ttl);
    ASSERT_EQ(tenant_ttl->state_, OB_TTL_TASK_RUNNING);
    ASSERT_EQ(tenant_ttl->is_usr_trigger_, true);
    tenant_ttl->ttl_continue_ = true;
  }
  task_info.pkey_ = pkey;
  ret = ttl_mgr.generate_one_partition_task(task_info, para);
  ASSERT_EQ(ret, OB_SUCCESS);

  usleep(100 * 1000L); /*wait schedule*/
  {
    /*1.1 simulate: rs send a trigger request*/
    
    task_info.err_code_ = OB_SUCCESS;
    ret = ttl_mgr.report_task_status(task_info, para, is_stop);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(is_stop, false);
    {
      common::ObSpinLockGuard guard(ttl_mgr.lock_);
      ctx = ttl_mgr.get_one_partition_ctx(pkey);
      ASSERT_TRUE(NULL != ctx);
      ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_PENDING);  /*config disable ttl*/
    }

    task_info.err_code_ = OB_NOT_INIT;
    ret = ttl_mgr.report_task_status(task_info, para, is_stop);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(is_stop, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_PENDING); 

    task_info.err_code_ = OB_ITER_END;
    ret = ttl_mgr.report_task_status(task_info, para, is_stop);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(is_stop, true);
    ASSERT_EQ(ctx->task_status_, OB_TTL_TASK_FINISH); 
  }

}
#endif 

#if 0
class TestRsTTL : public::testing::Test {
public:
  TestRsTTL() {};
  virtual ~TestRsTTL() {}
  virtual void SetUp(); /*global event*/
  virtual void TearDown();

};

class TestRsTTLTaskMgr : public rootserver::ObTTLTenantTaskMgr {

public:
  static TestRsTTLTaskMgr& get_instance() {
    static TestRsTTLTaskMgr instance_;
    return instance_;
  }

  int init() {
    return ObTTLTenantTaskMgr::init();
  }

  int get_alive_servers(uint64_t tenant_id,  rootserver::ServerList& server_infos) { 
    UNUSED(tenant_id);
    UNUSED(server_infos);
    return OB_SUCCESS; 
  }
  
  int get_tenant_ids(ObIArray<uint64_t>& tenant_ids)
  {
    UNUSED(tenant_ids);
    return OB_SUCCESS;
  }


  int read_tenant_status(uint64_t tenant_id, 
                        common::ObTTLStatusArray& tenant_tasks)
  {
    UNUSED(tenant_id);
    UNUSED(tenant_tasks);
    return OB_SUCCESS; 
  }

  int delete_task(uint64_t tenant_id, uint64_t task_id)
  {
    UNUSED(tenant_id);
    UNUSED(task_id);
    return OB_SUCCESS; 
  }

  int update_task_status(uint64_t tenant_id,
                      uint64_t task_id,
                      int64_t rs_new_status)
  {
    UNUSED(tenant_id);
    UNUSED(task_id);
    UNUSED(rs_new_status);
    return OB_SUCCESS; 
  }

  int fetch_ttl_task_id(int64_t &new_task_id, int64_t last_task_id) {
    UNUSED(last_task_id);
    new_task_id = ++task_id_;
    return OB_SUCCESS;
  }

  int insert_tenant_task(ObTTLStatus& ttl_task) {
    UNUSED(ttl_task);
    return OB_SUCCESS;
  }

  int in_active_time(uint64_t tenant_id, bool& is_active_time) {
   UNUSED(tenant_id);
   is_active_time = active_time_;
   return OB_SUCCESS; 
  }

  void set_active_time(bool active_time) {
    active_time_ = active_time;
  }

  bool is_enable_ttl(uint64_t tenant_id) {
    UNUSED(tenant_id);
    return enable_ttl_;
  }

  void set_enable_ttl(bool enable_ttl) {
    enable_ttl_ = enable_ttl;
  }

  int dispatch_ttl_request(rootserver::ServerList& addrs, 
                          rootserver::ServerList& eliminate_addrs, 
                          uint64_t tenant_id,
                          int ttl_cmd,
                          int trigger_type,
                          int64_t task_id) {
    UNUSED(addrs);
    UNUSED(eliminate_addrs);
    UNUSED(tenant_id);
    UNUSED(ttl_cmd);
    UNUSED(trigger_type);
    UNUSED(task_id);
    ++snd_server_cnt_;
    return OB_SUCCESS;
  }

private:
  TestRsTTLTaskMgr() 
    : ObTTLTenantTaskMgr(),
      task_id_(0),
      enable_ttl_(true),
      active_time_(true),
      snd_server_cnt_(0) {}

public:
  uint64_t task_id_;
  bool enable_ttl_;
  bool active_time_;
  uint64_t snd_server_cnt_ = 0;
};

#define TESTTTLMGR TestRsTTLTaskMgr::get_instance()

class TTLTestHelper {
public:
  static int construct_ttl_status(uint64_t tenant_id, size_t count, common::ObTTLStatusArray& tenant_tasks);
};


void TestRsTTL::SetUp()
{
  TESTTTLMGR.init();
}

void TestRsTTL::TearDown()
{ 
}


#define OB_TTL_RESPONSE_MASK (1 << 30)
#define OB_TTL_STATUS_MASK  (OB_TTL_RESPONSE_MASK - 1)

#define SET_TASK_PURE_STATUS(status, state) ((status) = ((state) & OB_TTL_STATUS_MASK) + ((status & OB_TTL_RESPONSE_MASK)))
#define SET_TASK_RESPONSE(status, state) ((status) |= (((state) & 1) << 30))
#define SET_TASK_STATUS(status, pure_status, is_responsed) { SET_TASK_PURE_STATUS(status, pure_status), SET_TASK_RESPONSE(status, is_responsed); }

#define EVAL_TASK_RESPONSE(status) (((status) & OB_TTL_RESPONSE_MASK) >> 30)
#define EVAL_TASK_PURE_STATUS(status) (static_cast<ObTTLTaskStatus>((status) & OB_TTL_STATUS_MASK))

/**
 * add tenant
 * add ttl task
 * send msg
 * insert into table
*/
TEST_F(TestRsTTL, ttl_basic)
{
  int ret = OB_SUCCESS;
  uint32_t tenant_id = 1001;
  uint32_t tenant_id1 = 1002;

  ret = TESTTTLMGR.add_tenant(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = TESTTTLMGR.refresh_tenant(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = TESTTTLMGR.refresh_tenant(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);

  rootserver::ObTTLTenantTask* task_ptr = NULL;
  ret = TESTTTLMGR.get_tenant_tasks_ptr(tenant_id, task_ptr);
  ASSERT_EQ(ret, OB_SUCCESS);
  rootserver::ObTTLTenantTask& task_ref = *task_ptr;
  ASSERT_EQ(task_ref.tasks_.count(), 0);
  
  ret = TESTTTLMGR.add_tenant(tenant_id1);
  ASSERT_EQ(ret, OB_SUCCESS);

  // no ttl task exist return error
  ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_CANCEL);
  ASSERT_NE(ret, OB_SUCCESS);
  
  ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_MOVE);
  ASSERT_NE(ret, OB_SUCCESS);
  
  ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_RESUME);
  ASSERT_NE(ret, OB_SUCCESS);
  
  ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_SUSPEND);
  ASSERT_NE(ret, OB_SUCCESS);
  
  // add ttl task
  ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_TRIGGER);
  ASSERT_EQ(ret, OB_SUCCESS);
  {
    ASSERT_EQ(task_ref.tasks_.count(), 1);

    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
    ASSERT_EQ(rs_task.send_servers_.count(), 0);
    ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
    ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));  
  }

  {
    // add ttl task, cancel first, and add new one
    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_TRIGGER);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(task_ref.tasks_.count(), 2);
    {
      rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
      ASSERT_EQ(rs_task.send_servers_.count(), 0);
      ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
      ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
      ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
      ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
      ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL));
    }

    {
      // new added task, init status means waiting to scheduler
      rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(1);
      ASSERT_EQ(rs_task.send_servers_.count(), 0);
      ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
      ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
      ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
      ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
      ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));
    }
  }

  {
    // add ttl task, 2 task in queue, alter second task status
    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_SUSPEND);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(task_ref.tasks_.count(), 2);
    // new added task, alter status
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(1);
    ASSERT_EQ(rs_task.send_servers_.count(), 0);
    ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
    ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND));
  }

  {
    // add ttl task, 2 task in queue, alter second task status
    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_RESUME);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(task_ref.tasks_.count(), 2);
    // new added task, alter status
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(1);
    ASSERT_EQ(rs_task.send_servers_.count(), 0);
    ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
    ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));
  }

  {
    // add ttl task, 2 task in queue, alter second task status
    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_CANCEL);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(task_ref.tasks_.count(), 1);
    // new added task, alter status
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
    ASSERT_EQ(rs_task.send_servers_.count(), 0);
    ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
    ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL));
  }

  {
    // add ttl task failed
    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_RESUME);
    ASSERT_NE(ret, OB_SUCCESS);
    ASSERT_EQ(task_ref.tasks_.count(), 1);
    // new added task, alter status
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
    ASSERT_EQ(rs_task.send_servers_.count(), 0);
    ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
    ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL));
  }

  // process revieve msg
  {
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
    int64_t addr_value1 = 0x1234123412;
    common::ObAddr addr1(addr_value1);
    
    ret = rs_task.send_servers_.push_back(addr1);
    ASSERT_EQ(ret, OB_SUCCESS);

    uint64_t task_id = rs_task.ttl_status_.task_id_;
    uint64_t invalid_task_id = OB_INVALID_ID ;

    int64_t rsp_task_type = static_cast<int64_t>(ObTTLTaskType::OB_TTL_CANCEL);
    //  ret = TESTTTLMGR.process_tenant_task_rsp(tenant_id, invalid_task_id, rsp_task_type, addr1);
    //  ASSERT_NE(ret, OB_SUCCESS);
    //  
    //  
    //  rsp_task_type = static_cast<int64_t>(ObTTLTaskType::OB_TTL_MOVE);
    //  ret = TESTTTLMGR.process_tenant_task_rsp(tenant_id, task_id, rsp_task_type, addr1);
    //  ASSERT_NE(ret, OB_SUCCESS);

    rsp_task_type = static_cast<int64_t>(ObTTLTaskType::OB_TTL_CANCEL);
    ret = TESTTTLMGR.process_tenant_task_rsp(tenant_id, task_id, rsp_task_type, addr1);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));

    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));

    rsp_task_type = static_cast<int64_t>(ObTTLTaskType::OB_TTL_MOVE);
    ASSERT_EQ(ret, OB_SUCCESS);
    ret = TESTTTLMGR.process_tenant_task_rsp(tenant_id, task_id, rsp_task_type, addr1);
    ASSERT_EQ(ret, OB_SUCCESS);

    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 1);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));
    task_ref.tasks_.remove(0);
  }

  TESTTTLMGR.reset_local_tenant_task();
  TESTTTLMGR.delete_tenant(tenant_id);
  TESTTTLMGR.delete_tenant(tenant_id1);
}

TEST_F(TestRsTTL, ttl_periodic)
{
  int ret = OB_SUCCESS;
  uint32_t tenant_id = 1001;

  ret = TESTTTLMGR.add_tenant(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  
  rootserver::ObTTLTenantTask* task_ptr = NULL;
  ret = TESTTTLMGR.get_tenant_tasks_ptr(tenant_id, task_ptr);
  ASSERT_EQ(ret, OB_SUCCESS);
  
  rootserver::ObTTLTenantTask& task_ref = *task_ptr;
  ASSERT_EQ(task_ref.tasks_.count(), 1);

  {
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
    ASSERT_EQ(rs_task.send_servers_.count(), 0);
    ASSERT_EQ(rs_task.ttl_status_.tenant_id_, tenant_id);
    ASSERT_EQ(rs_task.ttl_status_.table_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.partition_id_, OB_INVALID_ID);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::PERIODIC_TRIGGER));
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));

    rs_task.ttl_status_.task_update_time_ -= 360000 * 1000L * 1000L;
    ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);

    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_RESUME);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));
    ASSERT_EQ(task_ref.tasks_.count(), 1);


    // 0:canceling, 1:create
    ret = TESTTTLMGR.add_ttl_task(tenant_id, ObTTLTaskType::OB_TTL_TRIGGER);
    ASSERT_EQ(ret, OB_SUCCESS);    
    ASSERT_EQ(task_ref.tasks_.count(), 2);
    ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::PERIODIC_TRIGGER));
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL));
    {
      rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(1);
      ASSERT_EQ(rs_task.ttl_status_.trigger_type_, static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER));
      ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
      ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));
    }
    
    // first task set as move
    rs_task.all_responsed_ = true;
    ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));

    // first task moveing, just retry send msg to server
    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
    ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));

    // first task set as moved && delete first
    rs_task.all_responsed_ = true;
    ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(task_ref.tasks_.count(), 1);
    
    {
      rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);
      ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
      ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));

      rs_task.all_responsed_ = true;
      ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 0);
      ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));

      rs_task.all_responsed_ = true;
      ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 1);
      ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));


      ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_), 1);
      ASSERT_EQ(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_), static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE));
   }
}
  TESTTTLMGR.reset_local_tenant_task();
  TESTTTLMGR.delete_tenant(tenant_id);
}

TEST_F(TestRsTTL, ttl_config)
{
  int ret = OB_SUCCESS;
  uint32_t tenant_id = 1001;

  ret = TESTTTLMGR.add_tenant(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = TESTTTLMGR.alter_status_and_add_ttl_task(tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  
  rootserver::ObTTLTenantTask* task_ptr = NULL;
  ret = TESTTTLMGR.get_tenant_tasks_ptr(tenant_id, task_ptr);
  ASSERT_EQ(ret, OB_SUCCESS);
  
  rootserver::ObTTLTenantTask& task_ref = *task_ptr;
  ASSERT_EQ(task_ref.tasks_.count(), 1);

  {
    rootserver::RsTenantTask& rs_task = task_ref.tasks_.at(0);

    uint64_t send_server_cnt = TESTTTLMGR.snd_server_cnt_;
    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(TESTTTLMGR.snd_server_cnt_, send_server_cnt + 1);

    TESTTTLMGR.active_time_ = false;
    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<ObTTLTaskStatus>(ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND));

    TESTTTLMGR.active_time_ = true;
    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);

    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<ObTTLTaskStatus>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));

    TESTTTLMGR.enable_ttl_ = false;
    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);

    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<ObTTLTaskStatus>(ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND));

    TESTTTLMGR.enable_ttl_ = true;
    ret = TESTTTLMGR.process_tenant_tasks(tenant_id);
    ASSERT_EQ(ret, OB_SUCCESS);

    ASSERT_EQ(rs_task.ttl_status_.status_, static_cast<ObTTLTaskStatus>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE));

  }
}
#endif

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

