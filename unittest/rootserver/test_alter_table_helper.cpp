/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_guard_wrapper.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_rpc_struct.h"
#include "share/parameter/ob_parameter_macro.h"
#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#define private public
#include "rootserver/parallel_ddl/ob_alter_table_helper.h"
#undef private
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace common;
using namespace rootserver;

namespace unittest
{

static int64_t g_next_column_id = OB_APP_MIN_COLUMN_ID;

static int add_alter_column_with_name(
    ObAlterTableArg &arg,
    ObSchemaOperationType alter_type,
    const char *col_name)
{
  int ret = OB_SUCCESS;
  AlterColumnSchema col;
  col.alter_type_ = alter_type;
  col.set_column_name(col_name);
  if (OB_FAIL(col.set_origin_column_name(col.get_column_name_str()))) {
    LOG_WARN("fail to set origin column name", KR(ret));
  } else {
    col.set_column_id(g_next_column_id++);
    col.set_data_type(ObIntType);
    col.set_tenant_id(OB_SYS_TENANT_ID);
    col.set_table_id(0);
    // Ensure the table schema has valid tenant_id for compat mode check
    arg.alter_table_schema_.set_tenant_id(OB_SYS_TENANT_ID);
    if (OB_FAIL(arg.alter_table_schema_.add_alter_column(col, true/*need_allocate*/))) {
      LOG_WARN("fail to add column", KR(ret));
    }
  }
  return ret;
}

// Helper to add a column with specified alter_type to an ObAlterTableArg.
// Sets tenant_id on the schema to OB_SYS_TENANT_ID so that the compat mode
// check in add_column succeeds without MTL context.
// Each call assigns a unique column_id to avoid hash collisions.
static int add_alter_column(ObAlterTableArg &arg, ObSchemaOperationType alter_type)
{
  char col_name[64];
  snprintf(col_name, sizeof(col_name), "test_col_%ld", g_next_column_id);
  return add_alter_column_with_name(arg, alter_type, col_name);
}

static int add_base_column(ObTableSchema &table_schema, const char *col_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 col;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_table_id(1);
  col.set_tenant_id(OB_SYS_TENANT_ID);
  col.set_table_id(table_schema.get_table_id());
  col.set_column_id(g_next_column_id++);
  col.set_column_name(col_name);
  col.set_data_type(ObIntType);
  if (OB_FAIL(table_schema.add_column(col))) {
    LOG_WARN("fail to add base column", KR(ret));
  }
  return ret;
}

TEST(TestParallelAlterTableVersion, release_and_upgrade_boundaries)
{
  ASSERT_FALSE(is_parallel_alter_table_data_version_supported(MOCK_DATA_VERSION_4_4_2_3));
  ASSERT_TRUE(is_parallel_alter_table_data_version_supported(MOCK_DATA_VERSION_4_4_2_4));
  ASSERT_TRUE(is_parallel_alter_table_data_version_supported(DATA_VERSION_4_5_0_0 - 1));
  ASSERT_FALSE(is_parallel_alter_table_data_version_supported(DATA_VERSION_4_5_0_0));
  ASSERT_FALSE(is_parallel_alter_table_data_version_supported(DATA_VERSION_5_0_1_0 - 1));
  ASSERT_TRUE(is_parallel_alter_table_data_version_supported(DATA_VERSION_5_0_1_0));
  ASSERT_TRUE(is_parallel_alter_table_data_version_supported(DATA_VERSION_5_0_1_0 + 1));
}

TEST(TestParallelDDLDependencies, consistency_across_array_storage)
{
  typedef std::pair<uint64_t, ObObjectType> DepObj;
  ObArray<DepObj> before;
  ObSEArray<DepObj, 4> after;
  ASSERT_EQ(OB_SUCCESS, ObDDLHelper::check_dep_objs_consistent(before, after));

  ASSERT_EQ(OB_SUCCESS, after.push_back(DepObj(100, ObObjectType::VIEW)));
  ASSERT_EQ(OB_ERR_PARALLEL_DDL_CONFLICT,
            ObDDLHelper::check_dep_objs_consistent(before, after));
  ASSERT_EQ(OB_SUCCESS, before.push_back(DepObj(100, ObObjectType::VIEW)));
  ASSERT_EQ(OB_SUCCESS, ObDDLHelper::check_dep_objs_consistent(before, after));

  // Exercise dependency sorting after the inline array has expanded.
  before.reset();
  after.reset();
  for (uint64_t i = 0; i < 6; ++i) {
    ASSERT_EQ(OB_SUCCESS, before.push_back(DepObj(100 + i, ObObjectType::VIEW)));
    ASSERT_EQ(OB_SUCCESS, after.push_back(DepObj(105 - i, ObObjectType::VIEW)));
  }
  ASSERT_EQ(OB_SUCCESS, ObDDLHelper::check_dep_objs_consistent(before, after));
  after.at(0).first = 106;
  ASSERT_EQ(OB_ERR_PARALLEL_DDL_CONFLICT,
            ObDDLHelper::check_dep_objs_consistent(before, after));
  after.at(0).first = 105;
  after.at(0).second = ObObjectType::TABLE;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObDDLHelper::check_dep_objs_consistent(before, after));
}

TEST(TestDropColumnParallelEligibility, mview_and_mlog_base_tables_use_serial_ddl)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.data_version_ = DATA_VERSION_4_6_1_0;
  arg.alter_algorithm_ = ObAlterTableArg::AlterAlgorithm::INSTANT;
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_DROP_COLUMN, "c2"));
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c1"));
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c2"));
  ObSchemaGetterGuard local_guard;
  ObLatestSchemaGuard latest_guard(nullptr, OB_SYS_TENANT_ID);
  ObSchemaGuardWrapper sql_wrapper(OB_SYS_TENANT_ID, nullptr);
  ObSchemaGuardWrapper rs_wrapper(OB_SYS_TENANT_ID, nullptr);
  ASSERT_EQ(OB_SUCCESS, sql_wrapper.init(local_guard));
  ASSERT_EQ(OB_SUCCESS, rs_wrapper.init(latest_guard));
  ObSchemaGuardWrapper *wrappers[] = {&sql_wrapper, &rs_wrapper};
  // MLOG-only, MVIEW-only, and MVIEW with MLOG use the same SQL/RS gate.
  for (int64_t scenario = 0; scenario < 3; ++scenario) {
    table_schema.set_mlog_tid(scenario == 1 ? OB_INVALID_ID : 50001);
    table_schema.set_table_referenced_by_mv(
        scenario == 0 ? IS_NOT_REFERENCED_BY_MV : IS_REFERENCED_BY_MV);
    for (int64_t i = 0; i < 2; ++i) {
      bool can_parallel = true;
      const char *reason = nullptr;
      ASSERT_EQ(OB_SUCCESS, check_drop_column_can_parallel(
          arg, table_schema, *wrappers[i], can_parallel, reason));
      ASSERT_FALSE(can_parallel);
      ASSERT_STREQ("table required by materialized view refresh", reason);
    }
  }
}

// =============================================================================
// Test check_alter_table_arg_for_parallel
// =============================================================================

class TestCheckAlterTableArgForParallel : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID));
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
    ASSERT_TRUE(tenant_config.is_valid());
    ASSERT_TRUE(tenant_config->_parallel_ddl_control.set_value("ADD_COLUMN:ON,DROP_COLUMN:ON"));
  }
  void TearDown() override {}
};

// Only ADD_COLUMN operations should be supported
TEST_F(TestCheckAlterTableArgForParallel, only_add_column)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ADD_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

// Only DROP_COLUMN operations should be supported
TEST_F(TestCheckAlterTableArgForParallel, only_drop_column)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
}

// Compound ADD+DROP is NOT supported for parallel; arg-level check should
// return can_parallel=false so the statement falls back to serial offline DDL.
TEST_F(TestCheckAlterTableArgForParallel, add_and_drop_column_rejected)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ADD_COLUMN));
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// Unsupported column alter type (MODIFY_COLUMN) should return can_parallel=false
TEST_F(TestCheckAlterTableArgForParallel, unsupported_modify_column)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_CHANGE_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// Mix of supported and unsupported types should return can_parallel=false
TEST_F(TestCheckAlterTableArgForParallel, mixed_supported_and_unsupported)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ADD_COLUMN));
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_MODIFY_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// An argument without column or partition operations is not a parallel ALTER.
TEST_F(TestCheckAlterTableArgForParallel, empty_column_list_no_partition)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.alter_part_type_ = ObAlterTableArg::NO_OPERATION;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, column_and_partition_operations_rejected)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
  ASSERT_TRUE(tenant_config->_parallel_ddl_control.set_value(
      "ADD_COLUMN:ON,DROP_COLUMN:ON,ADD_PARTITION:ON,DROP_PARTITION:ON,TRUNCATE_PARTITION:ON,"
      "ADD_SUB_PARTITION:ON,DROP_SUB_PARTITION:ON,TRUNCATE_SUB_PARTITION:ON"));
  const ObAlterTableArg::AlterPartitionType part_types[] = {
      ObAlterTableArg::ADD_PARTITION, ObAlterTableArg::DROP_PARTITION,
      ObAlterTableArg::TRUNCATE_PARTITION, ObAlterTableArg::ADD_SUB_PARTITION,
      ObAlterTableArg::DROP_SUB_PARTITION, ObAlterTableArg::TRUNCATE_SUB_PARTITION};
  const ObSchemaOperationType column_types[] = {OB_DDL_ADD_COLUMN, OB_DDL_DROP_COLUMN};
  for (int64_t i = 0; i < ARRAYSIZEOF(part_types); ++i) {
    for (int64_t j = 0; j < ARRAYSIZEOF(column_types); ++j) {
      ObAlterTableArg arg;
      arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      arg.alter_part_type_ = part_types[i];
      bool can_parallel = false;
      const char *reason = nullptr;
      ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
      ASSERT_TRUE(can_parallel);
      ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, column_types[j]));
      ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
      ASSERT_FALSE(can_parallel);
      ASSERT_STREQ("mixed column and partition operations", reason);
    }
  }
}

// Measure a valid argument so the serialized fields, rather than an invalid
// argument sentinel, contribute to the layout baseline. A size change requires
// reviewing has_no_non_column_operation() and its per-field rejection tests.
TEST_F(TestCheckAlterTableArgForParallel, serialize_size_tripwire)
{
  ObAlterTableArg arg;
  arg.alter_table_schema_.set_tenant_id(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, arg.alter_table_schema_.set_origin_database_name("test_db"));
  ASSERT_EQ(OB_SUCCESS, arg.alter_table_schema_.set_origin_table_name("test_table"));
  ASSERT_TRUE(arg.is_valid());
  arg.tz_info_wrap_.set_tz_info_offset(0);

  const int64_t sz = arg.get_serialize_size();
  const int64_t EXPECTED_SERIALIZE_SIZE = 869;
  ASSERT_EQ(EXPECTED_SERIALIZE_SIZE, sz)
      << "ObAlterTableArg serialize size changed. "
         "Review ObAlterTableArg::has_no_non_column_operation() and its field tests "
         "before updating this baseline.";
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(sz));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buf, sz, pos));
  ASSERT_EQ(sz, pos);
}

// =============================================================================
// Per-field coverage: check_non_column_arg_for_parallel
// =============================================================================

// Baseline: default-constructed arg should pass non-column check
TEST_F(TestCheckAlterTableArgForParallel, non_column_default_passes)
{
  ObAlterTableArg arg;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

// alter_part_type_ is a legitimate parallel partition operation, gated by the
// action whitelist in ObAlterTableHelper rather than by has_no_non_column_operation.
// So the non-column check should pass (can_parallel=true) when only this field is set.
TEST_F(TestCheckAlterTableArgForParallel, non_column_alter_part_type)
{
  ObAlterTableArg arg;
  arg.alter_part_type_ = ObAlterTableArg::ADD_PARTITION;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_foreign_key_arg_list)
{
  ObAlterTableArg arg;
  ObCreateForeignKeyArg fk_arg;
  ASSERT_EQ(OB_SUCCESS, arg.foreign_key_arg_list_.push_back(fk_arg));
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_alter_options)
{
  ObAlterTableArg arg;
  arg.is_alter_options_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, random_partition_attributes_rejected)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.is_alter_random_partition_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
  ASSERT_STREQ("is_alter_random_partition", reason);
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ADD_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_alter_option_bitset)
{
  ObAlterTableArg arg;
  arg.alter_table_schema_.alter_option_bitset_.add_member(ObAlterTableArg::BLOCK_SIZE);
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_alter_indexs)
{
  ObAlterTableArg arg;
  arg.is_alter_indexs_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_inner)
{
  ObAlterTableArg arg;
  arg.is_inner_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_update_global_indexes)
{
  ObAlterTableArg arg;
  arg.is_update_global_indexes_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_update_global_indexes_with_partition)
{
  ObAlterTableArg arg;
  arg.is_update_global_indexes_ = true;
  arg.alter_part_type_ = ObAlterTableArg::DROP_PARTITION;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_alter_constraint_type)
{
  ObAlterTableArg arg;
  arg.alter_constraint_type_ = ObAlterTableArg::ADD_CONSTRAINT;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_convert_to_character)
{
  ObAlterTableArg arg;
  arg.is_convert_to_character_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_need_rebuild_trigger)
{
  ObAlterTableArg arg;
  arg.need_rebuild_trigger_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_add_to_scheduler)
{
  ObAlterTableArg arg;
  arg.is_add_to_scheduler_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_alter_auto_partition_attr)
{
  ObAlterTableArg arg;
  arg.alter_auto_partition_attr_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_direct_load_partition)
{
  ObAlterTableArg arg;
  arg.is_direct_load_partition_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_alter_column_group_delayed)
{
  ObAlterTableArg arg;
  arg.is_alter_column_group_delayed_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_alter_mview_attributes)
{
  ObAlterTableArg arg;
  arg.is_alter_mview_attributes_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_is_alter_mlog_attributes)
{
  ObAlterTableArg arg;
  arg.is_alter_mlog_attributes_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_skip_sys_table_check)
{
  ObAlterTableArg arg;
  arg.skip_sys_table_check_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_enable_hidden_table_partition_pruning)
{
  ObAlterTableArg arg;
  arg.enable_hidden_table_partition_pruning_ = true;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_index_arg_list)
{
  ObAlterTableArg arg;
  ObIndexArg *dummy = nullptr;
  ASSERT_EQ(OB_SUCCESS, arg.index_arg_list_.push_back(dummy));
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// is_alter_partitions_ is a legitimate parallel partition operation, gated by the
// action whitelist in ObAlterTableHelper rather than by has_no_non_column_operation.
// So the non-column check should pass (can_parallel=true) when only this field is set.
TEST_F(TestCheckAlterTableArgForParallel, non_column_is_alter_partitions)
{
  ObAlterTableArg arg;
  arg.is_alter_partitions_ = true;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_foreign_key_checks_false)
{
  ObAlterTableArg arg;
  arg.foreign_key_checks_ = false;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_rebuild_index_arg_list)
{
  ObAlterTableArg arg;
  ObTableSchema dummy_schema;
  ASSERT_EQ(OB_SUCCESS, arg.rebuild_index_arg_list_.push_back(dummy_schema));
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_part_storage_cache_policy)
{
  ObAlterTableArg arg;
  arg.set_part_storage_cache_policy(ObString("some_policy"));
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_hidden_table_id)
{
  ObAlterTableArg arg;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
  ASSERT_EQ(nullptr, reason);

  arg.hidden_table_id_ = 50001;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
  ASSERT_STREQ("hidden_table_id", reason);

  arg.hidden_table_id_ = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
  ASSERT_EQ(nullptr, reason);
}

TEST_F(TestCheckAlterTableArgForParallel, non_column_ddl_task_type)
{
  ObAlterTableArg arg;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
  ASSERT_EQ(nullptr, reason);

  arg.ddl_task_type_ = REBUILD_INDEX_TASK;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
  ASSERT_STREQ("ddl_task_type", reason);

  arg.ddl_task_type_ = INVALID_TASK;
  ASSERT_EQ(OB_SUCCESS, check_non_column_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
  ASSERT_EQ(nullptr, reason);
}

TEST_F(TestCheckAlterTableArgForParallel, drop_column_requires_instant_algorithm)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.alter_algorithm_ = ObAlterTableArg::AlterAlgorithm::INPLACE;
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_DROP_COLUMN, "c1"));

  ObTableSchema orig_table_schema;
  ASSERT_EQ(OB_SUCCESS, add_base_column(orig_table_schema, "c1"));

  ObSchemaGetterGuard schema_guard;
  ObSchemaGuardWrapper wrapper(OB_SYS_TENANT_ID, nullptr);
  ASSERT_EQ(OB_SUCCESS, wrapper.init(schema_guard));
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_drop_column_can_parallel(
      arg, orig_table_schema, wrapper, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
  ASSERT_STREQ("alter algorithm is not instant", reason);
}

TEST_F(TestCheckAlterTableArgForParallel, drop_column_rechecks_instant_config)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.data_version_ = DATA_VERSION_4_6_1_0;
  arg.alter_algorithm_ = ObAlterTableArg::AlterAlgorithm::INSTANT;
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_DROP_COLUMN, "c2"));
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c1"));
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c2"));
  ObSchemaGetterGuard local_guard;
  ObLatestSchemaGuard latest_guard(nullptr, OB_SYS_TENANT_ID);
  ObSchemaGuardWrapper sql_wrapper(OB_SYS_TENANT_ID, nullptr);
  ObSchemaGuardWrapper rs_wrapper(OB_SYS_TENANT_ID, nullptr);
  ASSERT_EQ(OB_SUCCESS, sql_wrapper.init(local_guard));
  ASSERT_EQ(OB_SUCCESS, rs_wrapper.init(latest_guard));
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
  const bool saved_instant = tenant_config->_enable_drop_column_instant;
  ASSERT_TRUE(tenant_config->_enable_drop_column_instant.set_value("true"));
  bool sql_can_parallel = false;
  const char *reason = nullptr;
  const int sql_ret = check_drop_column_can_parallel(arg, table_schema, sql_wrapper, sql_can_parallel, reason);
  ASSERT_TRUE(tenant_config->_enable_drop_column_instant.set_value("false"));
  bool rs_can_parallel = true;
  const int rs_ret = check_drop_column_can_parallel(arg, table_schema, rs_wrapper, rs_can_parallel, reason);
  ASSERT_TRUE(tenant_config->_enable_drop_column_instant.set_value(saved_instant ? "true" : "false"));
  ASSERT_EQ(OB_SUCCESS, sql_ret);
  ASSERT_TRUE(sql_can_parallel);
  ASSERT_EQ(OB_SUCCESS, rs_ret);
  ASSERT_FALSE(rs_can_parallel);
  ASSERT_STREQ("drop column requires offline ddl", reason);
}

TEST_F(TestCheckAlterTableArgForParallel, drop_all_lob_columns_requires_serial)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.data_version_ = DATA_VERSION_4_6_1_0;
  arg.alter_algorithm_ = ObAlterTableArg::AlterAlgorithm::INSTANT;
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_DROP_COLUMN, "c2"));
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c0"));
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c1"));
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c2"));
  table_schema.get_column_schema("c1")->set_data_type(ObLongTextType);
  table_schema.get_column_schema("c2")->set_data_type(ObRoaringBitmapType);
  ObLatestSchemaGuard latest_guard(nullptr, OB_SYS_TENANT_ID);
  ObSchemaGuardWrapper wrapper(OB_SYS_TENANT_ID, nullptr);
  ASSERT_EQ(OB_SUCCESS, wrapper.init(latest_guard));
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_drop_column_can_parallel(arg, table_schema, wrapper, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_DROP_COLUMN, "c1"));
  ASSERT_EQ(OB_SUCCESS, check_drop_column_can_parallel(arg, table_schema, wrapper, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
  ASSERT_STREQ("dropping all lob columns", reason);
}

TEST_F(TestCheckAlterTableArgForParallel, add_column_returns_aggregate_ddl_type)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.data_version_ = DATA_VERSION_4_6_1_0;
  arg.alter_algorithm_ = ObAlterTableArg::AlterAlgorithm::INSTANT;
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_ADD_COLUMN, "c2"));
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_ADD_COLUMN, "c3"));
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, add_base_column(table_schema, "c1"));
  table_schema.set_table_id(OB_MAX_INNER_TABLE_ID + 1);
  AlterColumnSchema *c2 = static_cast<AlterColumnSchema *>(arg.alter_table_schema_.get_column_schema_by_idx(0));
  AlterColumnSchema *c3 = static_cast<AlterColumnSchema *>(arg.alter_table_schema_.get_column_schema_by_idx(1));
  ASSERT_NE(nullptr, c2);
  ASSERT_NE(nullptr, c3);
  bool can_parallel = false;
  const char *reason = nullptr;
  ObDDLType ddl_type = DDL_INVALID;
  ASSERT_EQ(OB_SUCCESS, check_add_column_can_parallel(arg, table_schema, can_parallel, reason, &ddl_type));
  ASSERT_TRUE(can_parallel) << (reason == nullptr ? "" : reason);
  ASSERT_EQ(DDL_ADD_COLUMN_ONLINE, ddl_type);
  for (int64_t i = 0; i < 2; ++i) {
    c2->is_first_ = (i == 0);
    c3->is_first_ = (i == 1);
    ASSERT_EQ(OB_SUCCESS, check_add_column_can_parallel(arg, table_schema, can_parallel, reason, &ddl_type));
    ASSERT_TRUE(can_parallel) << (reason == nullptr ? "" : reason);
    ASSERT_EQ(DDL_ADD_COLUMN_INSTANT, ddl_type);
  }
  c3->is_autoincrement_ = true;
  ASSERT_EQ(OB_SUCCESS, check_add_column_can_parallel(arg, table_schema, can_parallel, reason, &ddl_type));
  ASSERT_FALSE(can_parallel);
  ASSERT_EQ(DDL_INVALID, ddl_type);
}

// =============================================================================
// Test DDL control mode for ADD_COLUMN and DROP_COLUMN
// =============================================================================

ObConfigContainer l_container;
static ObConfigContainer *local_container()
{
  return &l_container;
}

class TestAlterTableDDLControl : public ::testing::Test
{
public:
#undef OB_TENANT_PARAMETER
#define OB_TENANT_PARAMETER(args...) args
DEF_MODE_WITH_PARSER(_parallel_ddl_control, OB_TENANT_PARAMETER, "",
        common::ObParallelDDLControlParser,
        "switch for parallel capability of parallel DDL",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#undef OB_TENANT_PARAMETER
};

// Default config: ADD_COLUMN and DROP_COLUMN should be OFF
TEST_F(TestAlterTableDDLControl, default_add_drop_column_off)
{
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::ADD_COLUMN, is_parallel));
  ASSERT_FALSE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::DROP_COLUMN, is_parallel));
  ASSERT_FALSE(is_parallel);
}

// Explicitly enable ADD_COLUMN
TEST_F(TestAlterTableDDLControl, enable_add_column)
{
  ASSERT_TRUE(_parallel_ddl_control.set_value("add_column:on"));
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::ADD_COLUMN, is_parallel));
  ASSERT_TRUE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::DROP_COLUMN, is_parallel));
  ASSERT_FALSE(is_parallel);
}

// Explicitly enable DROP_COLUMN
TEST_F(TestAlterTableDDLControl, enable_drop_column)
{
  ASSERT_TRUE(_parallel_ddl_control.set_value("drop_column:on"));
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::ADD_COLUMN, is_parallel));
  ASSERT_FALSE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::DROP_COLUMN, is_parallel));
  ASSERT_TRUE(is_parallel);
}

// Enable both ADD_COLUMN and DROP_COLUMN
TEST_F(TestAlterTableDDLControl, enable_both_add_and_drop)
{
  ASSERT_TRUE(_parallel_ddl_control.set_value("add_column:on, drop_column:on"));
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::ADD_COLUMN, is_parallel));
  ASSERT_TRUE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::DROP_COLUMN, is_parallel));
  ASSERT_TRUE(is_parallel);
}

// Enable then disable ADD_COLUMN (last setting wins)
TEST_F(TestAlterTableDDLControl, on_then_off)
{
  ASSERT_TRUE(_parallel_ddl_control.set_value("add_column:on, add_column:off"));
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::ADD_COLUMN, is_parallel));
  ASSERT_FALSE(is_parallel);
}

// Existing types should still work alongside new types
TEST_F(TestAlterTableDDLControl, coexist_with_existing_types)
{
  ASSERT_TRUE(_parallel_ddl_control.set_value("truncate_table:on, add_column:on, drop_column:on, create_index:off"));
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::TRUNCATE_TABLE, is_parallel));
  ASSERT_TRUE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::ADD_COLUMN, is_parallel));
  ASSERT_TRUE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::DROP_COLUMN, is_parallel));
  ASSERT_TRUE(is_parallel);

  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::CREATE_INDEX, is_parallel));
  ASSERT_FALSE(is_parallel);
}

// =============================================================================
// Test DDLType / NOT_SUPPORT_DDLType array consistency
// =============================================================================

TEST_F(TestAlterTableDDLControl, string_to_ddl_type_add_column)
{
  ObParallelDDLControlMode::ObParallelDDLType ddl_type = ObParallelDDLControlMode::MAX_TYPE;
  ObString str("ADD_COLUMN");
  ASSERT_EQ(OB_SUCCESS, ObParallelDDLControlMode::string_to_ddl_type(str, ddl_type));
  ASSERT_EQ(ObParallelDDLControlMode::ADD_COLUMN, ddl_type);
}

TEST_F(TestAlterTableDDLControl, string_to_ddl_type_drop_column)
{
  ObParallelDDLControlMode::ObParallelDDLType ddl_type = ObParallelDDLControlMode::MAX_TYPE;
  ObString str("DROP_COLUMN");
  ASSERT_EQ(OB_SUCCESS, ObParallelDDLControlMode::string_to_ddl_type(str, ddl_type));
  ASSERT_EQ(ObParallelDDLControlMode::DROP_COLUMN, ddl_type);
}

// generate_parallel_ddl_control_config_for_create_tenant should include ADD_COLUMN and DROP_COLUMN
TEST_F(TestAlterTableDDLControl, create_tenant_config_includes_new_types)
{
  ObSqlString config_value;
  ASSERT_EQ(OB_SUCCESS, ObParallelDDLControlMode::generate_parallel_ddl_control_config_for_create_tenant(config_value));
  const char *config_cstr = config_value.ptr();
  // The config string should contain ADD_COLUMN:ON and DROP_COLUMN:ON
  ASSERT_TRUE(nullptr != config_cstr);
  ASSERT_TRUE(nullptr != strstr(config_cstr, "ADD_COLUMN:ON"));
  ASSERT_TRUE(nullptr != strstr(config_cstr, "DROP_COLUMN:ON"));
}

// Parser should accept the config generated by create_tenant
TEST_F(TestAlterTableDDLControl, parse_create_tenant_config)
{
  ObSqlString config_value;
  ASSERT_EQ(OB_SUCCESS, ObParallelDDLControlMode::generate_parallel_ddl_control_config_for_create_tenant(config_value));

  ObParallelDDLControlParser parser;
  uint8_t arr[32];
  MEMSET(arr, 0, 32);
  ASSERT_TRUE(parser.parse(config_value.ptr(), arr, 32));
}

// =============================================================================
// Test cases for review fixes (#1-#11)
// =============================================================================

// Fix #1: ObAlterTableRes should have a sane default for schema_version
// even when construct_and_adjust_result_ short-circuits on failure.
TEST_F(TestCheckAlterTableArgForParallel, res_schema_version_default)
{
  ObAlterTableRes res;
  ASSERT_EQ(OB_INVALID_VERSION, res.schema_version_);
}

class ResultTestAlterTableHelper : public ObAlterTableHelper
{
public:
  ResultTestAlterTableHelper(const ObAlterTableArg &arg, ObAlterTableRes &res)
      : ObAlterTableHelper(nullptr, OB_SYS_TENANT_ID, arg, res) {}
protected:
  int check_inner_stat_() override { return OB_SUCCESS; }
};

class TestAlterTableResult : public ::testing::Test
{
protected:
  void SetUp() override
  {
    generator_ = GET_TSI(TSISchemaVersionGenerator);
    ASSERT_NE(nullptr, generator_);
    saved_generator_ = *generator_;
    generator_->reset();
  }
  void TearDown() override
  {
    if (nullptr != generator_) {
      *generator_ = saved_generator_;
    }
  }
  ObSchemaVersionGenerator *generator_ = nullptr;
  ObSchemaVersionGenerator saved_generator_;
};

TEST_F(TestAlterTableResult, preserves_transaction_error)
{
  ObAlterTableArg arg;
  ObAlterTableRes res;
  ResultTestAlterTableHelper helper(arg, res);
  int ddl_ret = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, helper.construct_and_adjust_result_(ddl_ret));
  ASSERT_EQ(OB_INVALID_VERSION, res.schema_version_);
  ASSERT_EQ(OB_SUCCESS, generator_->init(1000, 1016));
  ASSERT_EQ(OB_TIMEOUT, helper.construct_and_adjust_result_(ddl_ret));
  ASSERT_EQ(OB_INVALID_VERSION, res.schema_version_);
}

TEST_F(TestAlterTableResult, propagates_schema_version_error)
{
  ObAlterTableArg arg;
  ObAlterTableRes res;
  ResultTestAlterTableHelper helper(arg, res);
  int ddl_ret = OB_SUCCESS;
  ASSERT_EQ(OB_NOT_INIT, helper.construct_and_adjust_result_(ddl_ret));
  ASSERT_EQ(OB_INVALID_VERSION, res.schema_version_);
}

TEST_F(TestAlterTableResult, returns_current_schema_version)
{
  ObAlterTableArg arg;
  ObAlterTableRes res;
  ResultTestAlterTableHelper helper(arg, res);
  ASSERT_EQ(OB_SUCCESS, generator_->init(1000, 1016));
  int64_t version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, generator_->next_version(version));
  int ddl_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, helper.construct_and_adjust_result_(ddl_ret));
  ASSERT_EQ(version, res.schema_version_);
}

// Fix #3: When DROP_COLUMN is combined with a DROP_FOREIGN_KEY action
// (via index_arg_list), parallel DDL should be rejected so that the
// serial path handles mock FK parent table schema changes.
TEST_F(TestCheckAlterTableArgForParallel, drop_column_with_index_arg_rejects_parallel)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  // Simulate DROP_FOREIGN_KEY in index_arg_list
  ObIndexArg *dummy = nullptr;
  ASSERT_EQ(OB_SUCCESS, arg.index_arg_list_.push_back(dummy));
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// Fix #5: DROP_COLUMN + ALTER_CONSTRAINT (e.g. DROP_CONSTRAINT) should
// be rejected for parallel, ensuring constraint handling in serial path.
TEST_F(TestCheckAlterTableArgForParallel, drop_column_with_constraint_rejects_parallel)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  arg.alter_constraint_type_ = ObAlterTableArg::DROP_CONSTRAINT;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// Fix #3: Multiple DROP_COLUMN operations in a single ALTER statement
// should all be recognized (validates the per-column iteration logic used
// by register_identity_sequence_locks_ after the fix).
TEST_F(TestCheckAlterTableArgForParallel, multiple_drop_columns_allowed)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_TRUE(can_parallel);
}

// Fix #11: ALTER_COLUMN (ALTER COLUMN SET DEFAULT / DROP DEFAULT) should
// reject parallel DDL, not just CHANGE/MODIFY.
TEST_F(TestCheckAlterTableArgForParallel, unsupported_alter_column_type)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ALTER_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// Compound DDL: DROP_COLUMN + ADD_COLUMN in a single statement should be
// rejected at arg-level (check_alter_column_arg_for_parallel_ returns
// can_parallel=false for has_add && has_drop), so it falls back to serial.
TEST_F(TestCheckAlterTableArgForParallel, compound_drop_and_add_rejected)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_DROP_COLUMN));
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ADD_COLUMN));
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
}

// Fix #5: foreign_key_checks = false (explicit SET foreign_key_checks=0)
// should reject parallel DDL, ensuring FK-related DDL goes through serial.
// (This test already exists but we add it in the review-fix section for
// documentation; the existing test validates the fix is correct.)

// =============================================================================
// R4 invariant: prev_column_id / next_column_id linkage that compute/persist
// split in ObAlterTableAddColumnAction depends on.
//
// compute_added_column_schema_ (ob_alter_column_ddl_service.cpp) runs
// update_prev_id_for_add_column + new_table_schema.add_column, then reads
// the mem_col from new_table_schema and stamps its prev_column_id /
// next_column_id back onto alter_column_schema. persist_added_column_ later
// uses alter_column_schema.get_next_column_id() to locate the neighbor for
// update_single_column, deliberately avoiding a second call to
// update_prev_id_for_add_column (its is_first branch would re-scan and pick
// alter_column itself as head_col, corrupting the chain via self-reference).
//
// The tests below construct the in-memory schema state that compute would
// produce for the four insert positions and assert two things directly:
//   1. get_column_schema_by_prev_next_id(alter_col.next_column_id_) returns
//      the expected neighbor (or nullptr at tail), matching what persist
//      reads.
//   2. ObColumnIterByPrevNextID traverses the chain in the correct order
//      with no self-reference at the head.
// =============================================================================

class TestAddColumnPrevNextChain : public ::testing::Test
{
public:
  void SetUp() override
  {
    table_.reset();
    table_.set_tenant_id(OB_SYS_TENANT_ID);
    table_.set_database_id(OB_SYS_DATABASE_ID);
    table_.set_table_id(20001);
    ASSERT_EQ(OB_SUCCESS, table_.set_table_name(ObString::make_string("t")));
    table_.set_max_used_column_id(c3_id_);
    table_.set_table_type(USER_TABLE);
    // Build 3-column chain: c1(BORDER<-,->c2) c2(c1<-,->c3) c3(c2<-,->BORDER).
    ASSERT_EQ(OB_SUCCESS, add_linked_column_("c1", c1_id_, BORDER_COLUMN_ID, c2_id_));
    ASSERT_EQ(OB_SUCCESS, add_linked_column_("c2", c2_id_, c1_id_, c3_id_));
    ASSERT_EQ(OB_SUCCESS, add_linked_column_("c3", c3_id_, c2_id_, BORDER_COLUMN_ID));
  }
  void TearDown() override {}

protected:
  // Append a column to table_ with given prev/next ids, then fix the
  // neighbor link on the existing column whose next was BORDER but should now
  // point at this new column. Used by SetUp to build the baseline chain.
  int add_linked_column_(const char *name,
                         const uint64_t col_id,
                         const uint64_t prev_id,
                         const uint64_t next_id)
  {
    int ret = OB_SUCCESS;
    ObColumnSchemaV2 col;
    col.set_tenant_id(OB_SYS_TENANT_ID);
    col.set_table_id(table_.get_table_id());
    col.set_column_id(col_id);
    col.set_column_name(name);
    col.set_data_type(ObIntType);
    col.set_prev_column_id(prev_id);
    col.set_next_column_id(next_id);
    if (OB_FAIL(table_.add_column(col))) {
      LOG_WARN("fail to add column", KR(ret), "name", name);
    } else {
      // add_column_update_prev_id may overwrite next_column_id (e.g. when
      // prev==BORDER_COLUMN_ID it sets next=BORDER unconditionally); restore
      // the intended value.
      ObColumnSchemaV2 *stored = table_.get_column_schema(col_id);
      if (OB_NOT_NULL(stored)) {
        stored->set_next_column_id(next_id);
      }
      if (BORDER_COLUMN_ID != prev_id) {
        ObColumnSchemaV2 *prev_col = table_.get_column_schema(prev_id);
        if (OB_ISNULL(prev_col)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          prev_col->set_next_column_id(col_id);
        }
      }
    }
    return ret;
  }

  // Collect the column names traversed by ObColumnIterByPrevNextID into out.
  int collect_chain_order_(ObIArray<ObString> &out) const
  {
    int ret = OB_SUCCESS;
    ObColumnIterByPrevNextID iter(table_);
    const ObColumnSchemaV2 *col = NULL;
    while (OB_SUCC(ret) && OB_SUCC(iter.next(col))) {
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(out.push_back(col->get_column_name_str()))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    return ret;
  }

  ObTableSchema table_;
  const uint64_t c1_id_ = OB_APP_MIN_COLUMN_ID;
  const uint64_t c2_id_ = OB_APP_MIN_COLUMN_ID + 1;
  const uint64_t c3_id_ = OB_APP_MIN_COLUMN_ID + 2;
  const uint64_t cnew_id_ = OB_APP_MIN_COLUMN_ID + 100;
};

// AFTER c1 — new column inserted between c1 and c2.
// Mirrors compute's is_after branch and the stamp-back step that copies
// mem_col.next_column_id_ onto alter_column_schema.
TEST_F(TestAddColumnPrevNextChain, after_c1_locates_c2_as_neighbor)
{
  // Simulate compute's effect: insert new column linked to c1 / c2.
  ASSERT_EQ(OB_SUCCESS, add_linked_column_("cnew", cnew_id_, c1_id_, c2_id_));
  // c2's prev must now point at cnew (this is what update_prev_id_for_add_
  // column.is_after path does in memory).
  ObColumnSchemaV2 *c2 = table_.get_column_schema(c2_id_);
  ASSERT_NE(nullptr, c2);
  c2->set_prev_column_id(cnew_id_);
  // After compute, alter_column_schema.next_column_id_ holds mem_col.next,
  // which equals c2_id_ here. persist locates the neighbor via this id.
  const ObColumnSchemaV2 *neighbor =
      table_.get_column_schema_by_prev_next_id(c2_id_);
  ASSERT_NE(nullptr, neighbor);
  ASSERT_EQ(ObString("c2"), neighbor->get_column_name_str());
  // Full chain order should be c1, cnew, c2, c3.
  ObArray<ObString> order;
  ASSERT_EQ(OB_SUCCESS, collect_chain_order_(order));
  ASSERT_EQ(4, order.count());
  ASSERT_EQ(ObString("c1"), order.at(0));
  ASSERT_EQ(ObString("cnew"), order.at(1));
  ASSERT_EQ(ObString("c2"), order.at(2));
  ASSERT_EQ(ObString("c3"), order.at(3));
}

// FIRST — new column at the head. compute's is_first branch scans
// ObColumnIterByPrevNextID for head_col BEFORE add_column, then stamps the
// resulting head_col's name onto alter_column_schema.next_column_name_.
// The safety property the R4 design comment calls out is: after the chain
// is patched, the head must be cnew, not cnew itself referenced via a
// stale next_column_name_. We assert both: head=cnew, and cnew.next=c1.
TEST_F(TestAddColumnPrevNextChain, first_position_head_is_new_not_self)
{
  ASSERT_EQ(OB_SUCCESS, add_linked_column_("cnew", cnew_id_, BORDER_COLUMN_ID, c1_id_));
  ObColumnSchemaV2 *c1 = table_.get_column_schema(c1_id_);
  ASSERT_NE(nullptr, c1);
  c1->set_prev_column_id(cnew_id_);
  // persist locates neighbor via mem_col.next_column_id_ stamped onto
  // alter_column_schema; for FIRST that id is c1_id_.
  const ObColumnSchemaV2 *neighbor =
      table_.get_column_schema_by_prev_next_id(c1_id_);
  ASSERT_NE(nullptr, neighbor);
  ASSERT_EQ(ObString("c1"), neighbor->get_column_name_str());
  // Chain order: cnew, c1, c2, c3 — confirms head is cnew (not self-ref).
  ObArray<ObString> order;
  ASSERT_EQ(OB_SUCCESS, collect_chain_order_(order));
  ASSERT_EQ(4, order.count());
  ASSERT_EQ(ObString("cnew"), order.at(0));
  ASSERT_EQ(ObString("c1"), order.at(1));
  ASSERT_EQ(ObString("c2"), order.at(2));
  ASSERT_EQ(ObString("c3"), order.at(3));
}

// BEFORE c2 — equivalent linkage to AFTER c1 but driven by the is_before
// branch in compute. Asserted separately so a future refactor that diverges
// the two branches gets caught.
TEST_F(TestAddColumnPrevNextChain, before_c2_locates_c2_as_neighbor)
{
  ASSERT_EQ(OB_SUCCESS, add_linked_column_("cnew", cnew_id_, c1_id_, c2_id_));
  ObColumnSchemaV2 *c2 = table_.get_column_schema(c2_id_);
  ASSERT_NE(nullptr, c2);
  c2->set_prev_column_id(cnew_id_);
  // persist locates neighbor via alter_column.next_column_id_ = c2_id_.
  const ObColumnSchemaV2 *neighbor =
      table_.get_column_schema_by_prev_next_id(c2_id_);
  ASSERT_NE(nullptr, neighbor);
  ASSERT_EQ(ObString("c2"), neighbor->get_column_name_str());
}

// TAIL — new column at the end. compute's is_last branch is a no-op (no
// neighbor update). alter_column_schema.next_column_id_ should be
// BORDER_COLUMN_ID, and persist's get_column_schema_by_prev_next_id must
// return nullptr so the update_single_column path is skipped.
TEST_F(TestAddColumnPrevNextChain, tail_position_has_no_neighbor)
{
  ASSERT_EQ(OB_SUCCESS, add_linked_column_("cnew", cnew_id_, c3_id_, BORDER_COLUMN_ID));
  // No neighbor's prev pointer to update — tail has none.
  const ObColumnSchemaV2 *neighbor =
      table_.get_column_schema_by_prev_next_id(BORDER_COLUMN_ID);
  ASSERT_EQ(nullptr, neighbor);
  // Chain order: c1, c2, c3, cnew.
  ObArray<ObString> order;
  ASSERT_EQ(OB_SUCCESS, collect_chain_order_(order));
  ASSERT_EQ(4, order.count());
  ASSERT_EQ(ObString("c3"), order.at(2));
  ASSERT_EQ(ObString("cnew"), order.at(3));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
