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

#include <gtest/gtest.h>
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "observer/table/ob_table_cg_service.h"
#include "observer/table/ob_table_cg_service.cpp"
#include "observer/table/ob_table_cache.h"
#include "observer/table/ob_table_context.h"
#include "../share/schema/mock_schema_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "lib/net/ob_addr.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "observer/ob_req_time_service.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/das/ob_data_access_service.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;
using namespace oceanbase::pl;

// copy from test_table_schema.cpp
void fill_table_schema(ObTableSchema &table)
{
  table.set_tenant_id(1);
  table.set_database_id(1);
  table.set_tablegroup_id(1);
  table.set_table_id(3001);
  table.set_max_used_column_id(16);
  table.set_rowkey_column_num(0);
  table.set_index_column_num(0);
  table.set_rowkey_split_pos(11);
  table.set_progressive_merge_num(11);
  table.set_compress_func_name(ObString::make_string("snappy_1.0"));
  table.set_autoinc_column_id(0);
  table.set_auto_increment(1);
  table.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table.set_def_type(TABLE_DEF_TYPE_USER);
  table.set_part_level(PARTITION_LEVEL_TWO);
  table.set_charset_type(CHARSET_UTF8MB4);
  table.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table.set_table_type(USER_TABLE);
  table.set_index_type(INDEX_TYPE_IS_NOT);
  table.set_index_status(INDEX_STATUS_AVAILABLE);
  table.set_data_table_id(0);
  table.set_is_use_bloomfilter(false);
  table.set_block_size(2097152);
  table.set_tablegroup_name("table group name 1");
  table.set_comment("This is a table");
  table.set_table_name("table_xxx");
  table.set_expire_info("expire: modify_time > 3000s");
  table.set_schema_version(1);
  table.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table.get_part_option().set_part_expr (ObString::make_string("rand() mod 111"));
  table.get_part_option().set_part_num(100);
  table.get_sub_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table.get_sub_part_option().set_part_expr (ObString::make_string("rand() mod 111"));
  table.get_sub_part_option().set_part_num(666);
}
// 填充一个column_schema，类型：ObIntType
// rowkey_pos： >0时为rowkey列，表示rowkey的顺序
// index_key_pos：>0时为索引列，表示索引列顺序
// part_key_pos：>0时为分区键，表示分区间顺序
void fill_column_schema(ObColumnSchemaV2 &column, uint64_t id, const char *name,
                        uint64_t rowkey_pos = 1, uint64_t index_key_pos = 1,
                        uint64_t part_key_pos = 1, ObOrderType rowkey_order = ObOrderType::ASC)
{
  ObObj value;
  column.set_column_id(id);
  column.set_column_name(ObString::make_string(name));
  column.set_rowkey_position(rowkey_pos);
  column.set_order_in_rowkey(rowkey_order);
  column.set_tbl_part_key_pos(part_key_pos);
  column.set_index_position(index_key_pos);
  column.set_data_length(100);
  column.set_data_precision(1100);
  column.set_data_scale(88);
  column.set_nullable(false);
  column.set_charset_type(CHARSET_UTF8MB4);
  column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  column.set_data_type(ObIntType);
  value.set_int(100);
  column.set_orig_default_value(value);
  value.set_int(101);
  column.set_cur_default_value(value);
  column.set_comment("black gives me black eyes");
}

// create table test (C1, C2, C3)
void create_table_schema(ObTableSchema *table_schema, ObColumnSchemaV2 *columns)
{
  ASSERT_NE(nullptr, table_schema);
  fill_table_schema(*table_schema);
  fill_column_schema(columns[0], 1, "C1", 1, 1, 1);
  fill_column_schema(columns[1], 2, "C2", 0, 0, 0);
  fill_column_schema(columns[2], 3, "C3", 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, table_schema->add_column(columns[0]));
  ASSERT_EQ(OB_SUCCESS, table_schema->add_column(columns[1]));
  ASSERT_EQ(OB_SUCCESS, table_schema->add_column(columns[2]));
  ASSERT_EQ(1, table_schema->get_tenant_id());
  ASSERT_EQ(1, table_schema->get_database_id());
  ASSERT_EQ(1, table_schema->get_tablegroup_id());
  ASSERT_EQ(3001, table_schema->get_table_id());
  ASSERT_EQ(3, table_schema->get_column_count());
  ASSERT_EQ(1, table_schema->get_index_column_num());
  ASSERT_EQ(1, table_schema->get_rowkey_column_num());
  ASSERT_EQ(1, table_schema->get_partition_key_column_num());
}

class TestCreateExecutor: public ::testing::Test
{
public:
  TestCreateExecutor();
  virtual ~TestCreateExecutor() {}
  virtual void SetUp();
  virtual void TearDown();
  void fake_ctx_init_common(ObTableCtx &fake_ctx, ObTableSchema *table_schema);
public:
  ObArenaAllocator allocator_;
  MockSchemaService schema_service_;
  ObSchemaGetterGuard schema_guard_;
  ObTableSchema table_schema_;
  ObColumnSchemaV2 columns_[3];
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestCreateExecutor);
};

TestCreateExecutor::TestCreateExecutor()
  : allocator_()
{
}

void TestCreateExecutor::SetUp()
{
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  schema_service_.init();
  create_table_schema(&table_schema_, columns_);
  schema_service_.add_table_schema(table_schema_, 1);
  schema_service_.get_schema_guard(schema_guard_, 1);
  // init MTL
  ObTenantBase tbase(1);
  static ObDataAccessService instance;
  tbase.inner_set(&instance);
  ASSERT_EQ(OB_SUCCESS, tbase.init());
  ObTenantEnv::set_tenant(&tbase);
}

void TestCreateExecutor::TearDown()
{
}

ObTableApiSessNodeVal g_sess_node_val(NULL);
void TestCreateExecutor::fake_ctx_init_common(ObTableCtx &fake_ctx, ObTableSchema *table_schema)
{
  fake_ctx.table_schema_ = table_schema;
  fake_ctx.tenant_id_ = table_schema->get_tenant_id();
  fake_ctx.database_id_ = table_schema->get_database_id();
  fake_ctx.table_name_ = table_schema->get_table_name();
  fake_ctx.ref_table_id_ = table_schema->get_table_id();
  fake_ctx.index_table_id_ = fake_ctx.ref_table_id_;
  fake_ctx.index_tablet_id_ = table_schema->get_table_id();
  fake_ctx.sess_guard_.sess_node_val_ = &g_sess_node_val;
  g_sess_node_val.is_inited_ = true;
  g_sess_node_val.sess_info_.test_init(0, 0, 0, NULL);
  g_sess_node_val.sess_info_.load_all_sys_vars(schema_guard_);
  fake_ctx.init_physical_plan_ctx(0, 1);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.construct_column_items());
}

TEST_F(TestCreateExecutor, scan)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  ObTableEntity entity;
  // init ctx
  fake_ctx_init_common(fake_ctx, &table_schema_);
  fake_ctx.set_entity(&entity);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_get());
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(columns_[i].get_column_id(), fake_ctx.select_col_ids_.at(i));
  }
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(3, fake_ctx.get_all_exprs().get_expr_array().count());
  ASSERT_EQ(OB_SUCCESS, fake_ctx.classify_scan_exprs());
  ASSERT_EQ(3, fake_ctx.select_exprs_.count());
  ASSERT_EQ(1, fake_ctx.rowkey_exprs_.count());

  ObTableApiSpec *root_spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTableSpecCgService::generate<TABLE_API_EXEC_SCAN>(allocator_, fake_ctx, root_spec));
  ASSERT_TRUE(nullptr != root_spec);
  ASSERT_EQ(TABLE_API_EXEC_SCAN, root_spec->get_type());
  ASSERT_EQ(nullptr, root_spec->get_parent());
  ASSERT_EQ(nullptr, root_spec->get_child());

  ASSERT_EQ(OB_SUCCESS, root_spec->create_executor(fake_ctx, executor));
  ASSERT_TRUE(nullptr != executor);
  ObTableApiScanExecutor *scan_executor = dynamic_cast<ObTableApiScanExecutor *>(executor);
  ASSERT_TRUE(nullptr != scan_executor);
  ASSERT_EQ(nullptr, executor->get_parent());
  ASSERT_EQ(nullptr, executor->get_child());
}

TEST_F(TestCreateExecutor, insert)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  // init ctx
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_insert());
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(3, fake_ctx.get_all_exprs().get_expr_array().count());

  ObTableApiSpec *root_spec = nullptr;
  ObTableApiExecutor *executor = nullptr;

  // generate insert spec tree
  ASSERT_EQ(OB_SUCCESS, ObTableSpecCgService::generate<TABLE_API_EXEC_INSERT>(allocator_, fake_ctx, root_spec));
  ASSERT_TRUE(nullptr != root_spec);
  ASSERT_EQ(TABLE_API_EXEC_INSERT, root_spec->get_type());
  ASSERT_EQ(nullptr, root_spec->get_parent());
  ASSERT_EQ(nullptr, root_spec->get_child());

  // create insert excutor tree;
  ASSERT_EQ(OB_SUCCESS, root_spec->create_executor(fake_ctx, executor));
  ASSERT_TRUE(nullptr != executor);
  ObTableApiInsertExecutor *ins_executor = dynamic_cast<ObTableApiInsertExecutor *>(executor);
  ASSERT_TRUE(nullptr != ins_executor);
  ASSERT_EQ(nullptr, executor->get_parent());
  ASSERT_EQ(nullptr, executor->get_child());
}

TEST_F(TestCreateExecutor, delete)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  // init ctx
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_delete());
  ASSERT_EQ(3, fake_ctx.select_col_ids_.count());
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(3, fake_ctx.get_all_exprs().get_expr_array().count());
  ObTableApiSpec *root_spec = nullptr;
  ObTableApiExecutor *executor = nullptr;

  // generate delete spec tree
  ASSERT_EQ(OB_SUCCESS, ObTableSpecCgService::generate<TABLE_API_EXEC_DELETE>(allocator_, fake_ctx, root_spec));
  ASSERT_TRUE(nullptr != root_spec);
  ASSERT_EQ(TABLE_API_EXEC_DELETE, root_spec->get_type());
  ASSERT_EQ(nullptr, root_spec->get_parent());
  ASSERT_TRUE(nullptr != root_spec->get_child());

  // check child spec (scan)
  const ObTableApiScanSpec *child_spec = dynamic_cast<const ObTableApiScanSpec *>(root_spec->get_child());
  ASSERT_TRUE(nullptr != child_spec);
  ASSERT_EQ(nullptr, child_spec->get_child());
  ASSERT_EQ(root_spec, child_spec->get_parent());

  // create executor tree
  ASSERT_EQ(OB_SUCCESS, root_spec->create_executor(fake_ctx, executor));
  ASSERT_TRUE(nullptr != executor);
  ObTableApiDeleteExecutor *del_executor = dynamic_cast<ObTableApiDeleteExecutor *>(executor);
  ASSERT_TRUE(nullptr != del_executor);
  ASSERT_EQ(nullptr, del_executor->get_parent());
  ASSERT_TRUE(nullptr != del_executor->get_child());

  // check child executor (scan)
  const ObTableApiScanExecutor *scan_executor = dynamic_cast<const ObTableApiScanExecutor *>(del_executor->get_child());
  ASSERT_TRUE(nullptr != scan_executor);
  ASSERT_EQ(del_executor, scan_executor->get_parent());
  ASSERT_EQ(nullptr, scan_executor->get_child());
}

TEST_F(TestCreateExecutor, update)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  // prepare
  ObTableEntity entity;
  ObObj obj;
  obj.set_int(1234);
  entity.add_rowkey_value(obj);
  entity.set_property(ObString::make_string("C2"), obj);
  // init ctx
  fake_ctx.set_entity(&entity);
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_update());

  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(4, fake_ctx.get_all_exprs().get_expr_array().count());
  ObTableApiSpec *root_spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  // generate update spec tree
  ASSERT_EQ(OB_SUCCESS, ObTableSpecCgService::generate<TABLE_API_EXEC_UPDATE>(allocator_, fake_ctx, root_spec));
  ASSERT_TRUE(nullptr != root_spec);
  ASSERT_EQ(TABLE_API_EXEC_UPDATE, root_spec->get_type());
  ASSERT_EQ(nullptr, root_spec->get_parent());
  ASSERT_TRUE(nullptr != root_spec->get_child());

  // check child spec (scan)
  const ObTableApiScanSpec *child_spec = dynamic_cast<const ObTableApiScanSpec *>(root_spec->get_child());
  ASSERT_TRUE(nullptr != child_spec);
  ASSERT_EQ(nullptr, child_spec->get_child());
  ASSERT_EQ(root_spec, child_spec->get_parent());

  // create executor tree
  ASSERT_EQ(OB_SUCCESS, root_spec->create_executor(fake_ctx, executor));
  ASSERT_TRUE(nullptr != executor);
  ObTableApiUpdateExecutor *upd_executor = dynamic_cast<ObTableApiUpdateExecutor *>(executor);
  ASSERT_TRUE(nullptr != upd_executor);
  ASSERT_EQ(nullptr, upd_executor->get_parent());
  ASSERT_TRUE(nullptr != upd_executor->get_child());

  // check child executor (scan)
  const ObTableApiScanExecutor *scan_executor = dynamic_cast<const ObTableApiScanExecutor *>(upd_executor->get_child());
  ASSERT_TRUE(nullptr != scan_executor);
  ASSERT_EQ(upd_executor, scan_executor->get_parent());
  ASSERT_EQ(nullptr, scan_executor->get_child());
}

TEST_F(TestCreateExecutor, insertup)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  ObTableEntity entity;
  ObObj obj;
  obj.set_int(1234);
  entity.add_rowkey_value(obj);
  entity.set_property(ObString::make_string("C2"), obj);
  // init ctx
  fake_ctx.set_entity(&entity);
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_insert_up());
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(4, fake_ctx.get_all_exprs().get_expr_array().count());
  ObTableApiSpec *root_spec = nullptr;
  ObTableApiExecutor *executor = nullptr;

  // generate insertup spec tree
  ASSERT_EQ(OB_SUCCESS, ObTableSpecCgService::generate<TABLE_API_EXEC_INSERT_UP>(allocator_, fake_ctx, root_spec));
  ASSERT_TRUE(nullptr != root_spec);
  ASSERT_EQ(TABLE_API_EXEC_INSERT_UP, root_spec->get_type());
  ASSERT_EQ(nullptr, root_spec->get_parent());
  ASSERT_EQ(nullptr, root_spec->get_child());

  // create insertup excutor tree;
  ASSERT_EQ(OB_SUCCESS, root_spec->create_executor(fake_ctx, executor));
  ASSERT_TRUE(nullptr != executor);
  ObTableApiInsertUpExecutor *insup_executor = dynamic_cast<ObTableApiInsertUpExecutor *>(executor);
  ASSERT_TRUE(nullptr != insup_executor);
  ASSERT_EQ(nullptr, executor->get_parent());
  ASSERT_EQ(nullptr, executor->get_child());
}

TEST_F(TestCreateExecutor, replace)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  // init ctx
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_replace());
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(3, fake_ctx.get_all_exprs().get_expr_array().count());
  ObTableApiSpec *root_spec = nullptr;
  ObTableApiExecutor *executor = nullptr;

  // generate replace spec tree
  ASSERT_EQ(OB_SUCCESS, ObTableSpecCgService::generate<TABLE_API_EXEC_REPLACE>(allocator_, fake_ctx, root_spec));
  ASSERT_TRUE(nullptr != root_spec);
  ASSERT_EQ(TABLE_API_EXEC_REPLACE, root_spec->get_type());
  ASSERT_EQ(nullptr, root_spec->get_parent());
  ASSERT_EQ(nullptr, root_spec->get_child());

  // create replace excutor tree;
  ASSERT_EQ(OB_SUCCESS, root_spec->create_executor(fake_ctx, executor));
  ASSERT_TRUE(nullptr != executor);
  ObTableApiReplaceExecutor *replace_executor = dynamic_cast<ObTableApiReplaceExecutor *>(executor);
  ASSERT_TRUE(nullptr != replace_executor);
  ASSERT_EQ(nullptr, executor->get_parent());
  ASSERT_EQ(nullptr, executor->get_child());
}

// refresh frame: init_datum_param_store + check_entity + refresh_rowkey_exprs_frame + refresh_properties_exprs_frame
TEST_F(TestCreateExecutor, refresh_exprs_frame)
{
  ObTableCtx fake_ctx(allocator_);
  ObExprFrameInfo fake_expr_info(allocator_);
  ObTableEntity entity;
  ObStaticEngineCG cg(GET_MIN_CLUSTER_VERSION());
  // prepare data
  ObObj obj;
  obj.set_int(1234);
  entity.add_rowkey_value(obj);
  obj.set_int(1235);
  entity.set_property(ObString::make_string("C2"), obj);
  // init ctx
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.init_insert());
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::generate_exprs(fake_ctx, allocator_, fake_expr_info));
  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::alloc_exprs_memory(fake_ctx, fake_expr_info));
  fake_ctx.set_expr_info(&fake_expr_info);
  ASSERT_EQ(3, fake_ctx.get_all_exprs().get_expr_array().count());
  ObArray<ObExpr *> rt_exprs;
  ASSERT_EQ(OB_SUCCESS, cg.generate_rt_exprs(fake_ctx.get_all_exprs().get_expr_array(), rt_exprs));

  ASSERT_EQ(OB_SUCCESS, ObTableExprCgService::refresh_exprs_frame(fake_ctx, rt_exprs, entity));
  // verify the refresh value
  ObEvalCtx eval_ctx(fake_ctx.get_exec_ctx());
  ObDatum *datum = nullptr;
  ObExpr *rt_expr = nullptr;
  ObObj objs[3];
  const ObIArray<ObRawExpr *> &all_exprs = fake_ctx.get_all_exprs().get_expr_array();
  for (int i = 0; i < all_exprs.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, cg.generate_rt_expr(*all_exprs.at(i), rt_expr));
    ASSERT_EQ(OB_SUCCESS, rt_expr->eval(eval_ctx, datum));
    ASSERT_EQ(OB_SUCCESS, datum->to_obj(objs[i], columns_[i].get_meta_type()));
  }
  ASSERT_EQ(1234, objs[0].get_int());
  ASSERT_EQ(1235, objs[1].get_int());
  // column default value
  ASSERT_EQ(101, objs[2].get_int());
}

// table context
TEST_F(TestCreateExecutor, cons_column_type)
{
  ObColumnSchemaV2 col_schema;
  // prepare data
  col_schema.set_data_type(ObVarcharType);
  col_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObAccuracy acc(1);
  col_schema.set_accuracy(acc);
  uint32_t res_flag = ObRawExprUtils::calc_column_result_flag(col_schema);

  ObExprResType column_type;
  ObTableCtx fake_ctx(allocator_);
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.cons_column_type(col_schema, column_type));
  ASSERT_EQ(ObVarcharType, column_type.get_type());
  ASSERT_EQ(res_flag, column_type.get_result_flag());
  ASSERT_EQ(CS_TYPE_UTF8MB4_GENERAL_CI, column_type.get_collation_type());
  ASSERT_EQ(CS_LEVEL_IMPLICIT, column_type.get_collation_level());
  ASSERT_EQ(1, column_type.get_accuracy().get_length());
}

TEST_F(TestCreateExecutor, check_column_type)
{
  ObExprResType column_type;
  ObObj obj;
  uint32_t res_flag = 0;
  ObTableCtx fake_ctx(allocator_);
  schema_service_.get_schema_guard(fake_ctx.schema_guard_, 1);
  fake_ctx_init_common(fake_ctx, &table_schema_);

  // check nullable
  obj.set_null();
  res_flag |= NOT_NULL_FLAG;
  column_type.set_result_flag(res_flag);
  ASSERT_EQ(OB_BAD_NULL_ERROR, fake_ctx.adjust_column_type(column_type, obj));
  // check data type mismatch
  res_flag = 0;
  obj.set_int(1);
  column_type.set_result_flag(res_flag);
  column_type.set_type(ObVarcharType);
  ASSERT_EQ(OB_OBJ_TYPE_ERROR, fake_ctx.adjust_column_type(column_type, obj));
  // check collation
  obj.set_binary("ttt");
  column_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_ERR_COLLATION_MISMATCH, fake_ctx.adjust_column_type(column_type, obj));
  // collation convert
  obj.set_varchar("test");
  obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.adjust_column_type(column_type, obj));
  ASSERT_EQ(CS_TYPE_UTF8MB4_GENERAL_CI, obj.get_collation_type());
}

TEST_F(TestCreateExecutor, generate_key_range)
{
  ObTableCtx fake_ctx(allocator_);
  fake_ctx_init_common(fake_ctx, &table_schema_);
  // prepare data
  ObArray<ObNewRange> scan_ranges;
  ObObj pk_objs_start[1];
  pk_objs_start[0].set_int(0);
  ObObj pk_objs_end[1];
  pk_objs_end[0].set_max_value();
  ObNewRange range;
  range.start_key_.assign(pk_objs_start, 1);
  range.end_key_.assign(pk_objs_end, 1);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  scan_ranges.push_back(range);
  ASSERT_EQ(OB_SUCCESS, fake_ctx.generate_key_range(scan_ranges));
  // primary key range
  ASSERT_EQ(1, fake_ctx.get_key_ranges().count());
}

TEST_F(TestCreateExecutor, test_cache)
{
  uint64_t tenant_id = 1;
  ObTenantBase tenant_ctx(tenant_id);
  ObTenantEnv::set_tenant(&tenant_ctx);
  // init plan cache
  ObPlanCache plan_cache;
  int ret = plan_cache.init(OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  plan_cache.set_mem_limit_pct(20);
  plan_cache.set_mem_high_pct(90);
  plan_cache.set_mem_low_pct(50);

  ObAddr addr;
  ObPCMemPctConf conf;
  ObLibCacheNameSpace ns = ObLibCacheNameSpace::NS_TABLEAPI;
  ObReqTimeGuard req_timeinfo_guard;
  // register cache obj
  ObLibCacheRegister::register_cache_objs();
  // get lib cache
  ObPlanCache *lib_cache = &plan_cache;
  // construct cache key
  ObTableApiCacheKey cache_key;
  cache_key.table_id_ = 1001;
  cache_key.index_table_id_ = 1001;
  cache_key.schema_version_ = 1;
  cache_key.operation_type_ = ObTableOperationType::Type::INSERT;
  cache_key.is_ttl_table_ = false;
  // construct ctx
  ObILibCacheCtx ctx;
  // construct cache obj
  {
    ObCacheObjGuard guard(CacheRefHandleID::TABLEAPI_NODE_HANDLE);
    ASSERT_EQ(OB_SUCCESS, lib_cache->alloc_cache_obj(guard, ns, tenant_id));
    ObILibCacheObject *cache_obj = guard.get_cache_obj();
    ASSERT_TRUE(nullptr != cache_obj);
    ObTableApiCacheObj *table_cache_obj = static_cast<ObTableApiCacheObj *>(cache_obj);
    ObIAllocator &cache_allocator = table_cache_obj->get_allocator();
    // construct spec
    ObTableApiInsertSpec spec(cache_allocator, TABLE_API_EXEC_INSERT);
    table_cache_obj->set_spec(&spec);
    // add an cache obj
    ASSERT_EQ(OB_SUCCESS, lib_cache->add_cache_obj(ctx, &cache_key , table_cache_obj));
  }
  // get cache obj
  {
    ObCacheObjGuard guard(CacheRefHandleID::TABLEAPI_NODE_HANDLE);
    ASSERT_EQ(OB_SUCCESS, lib_cache->get_cache_obj(ctx, &cache_key, guard));
    ObTableApiCacheObj *cache_obj = static_cast<ObTableApiCacheObj *>(guard.get_cache_obj());
    ASSERT_TRUE(nullptr != cache_obj);
    ObTableApiSpec *spec = cache_obj->get_spec();
    ASSERT_TRUE(nullptr != spec);
    ASSERT_EQ(TABLE_API_EXEC_INSERT, spec->type_);
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_create_executor.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
