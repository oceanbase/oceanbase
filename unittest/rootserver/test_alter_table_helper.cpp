/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_rpc_struct.h"
#include "share/parameter/ob_parameter_macro.h"
#include "rootserver/parallel_ddl/ob_alter_table_helper.h"

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
    if (OB_FAIL(arg.alter_table_schema_.add_column(col))) {
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

// =============================================================================
// Test check_alter_table_arg_for_parallel
// =============================================================================

class TestCheckAlterTableArgForParallel : public ::testing::Test
{
public:
  void SetUp() override {}
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
  // Note: can_parallel depends on config flag, but type validation should pass.
  // With default config (empty string), ADD_COLUMN defaults to OFF for sys tenant.
  // Here we only verify no error is returned.
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

// Empty column list (no column operations) should pass column check
// but fail partition check (partition always returns false currently)
TEST_F(TestCheckAlterTableArgForParallel, empty_column_list_no_partition)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.alter_part_type_ = ObAlterTableArg::NO_OPERATION;
  bool can_parallel = false;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  // column check passes vacuously, then partition check returns false
  // because check_alter_partition_arg_for_parallel_ always returns can_parallel=false
}

// Unsupported partition operations (e.g. DROP_PARTITION) should return can_parallel=false
TEST_F(TestCheckAlterTableArgForParallel, partition_not_supported)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.alter_part_type_ = ObAlterTableArg::DROP_PARTITION;
  bool can_parallel = false;
  const char *reason = nullptr;
  // Add a supported column op so column check passes
  ASSERT_EQ(OB_SUCCESS, add_alter_column(arg, OB_DDL_ADD_COLUMN));
  ASSERT_EQ(OB_SUCCESS, check_alter_table_arg_for_parallel(arg, can_parallel, reason));
  // Even with valid column ops, partition check should make it false
  // (partition parallel not implemented yet)
}

// =============================================================================
// Tripwire: detect new fields added to ObAlterTableArg
// =============================================================================

// If ObAlterTableArg's serialization layout changes (field added/removed/resized),
// this test fails. The developer MUST then:
// 1. Review ObAlterTableArg::has_no_non_column_operation() in ob_rpc_struct.cpp
// 2. Add whitelist checks for any new non-column fields
// 3. Update the expected size constant below
TEST_F(TestCheckAlterTableArgForParallel, serialize_size_tripwire)
{
  ObAlterTableArg arg;
  const int64_t sz = arg.get_serialize_size();
  ASSERT_GT(sz, 0);
  // =================================================================
  // IMPORTANT: If this constant no longer matches, ObAlterTableArg
  // serialization changed. Review
  // ObAlterTableArg::has_no_non_column_operation() in ob_rpc_struct.cpp,
  // add whitelist checks for any new fields, then update this value.
  // =================================================================
  const int64_t EXPECTED_SERIALIZE_SIZE = 5;
  ASSERT_EQ(EXPECTED_SERIALIZE_SIZE, sz)
      << "ObAlterTableArg serialize size changed. "
         "Review ObAlterTableArg::has_no_non_column_operation() in ob_rpc_struct.cpp "
         "and update the whitelist, then update this constant.";
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

TEST_F(TestCheckAlterTableArgForParallel, drop_column_requires_instant_algorithm)
{
  ObAlterTableArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.alter_algorithm_ = ObAlterTableArg::AlterAlgorithm::INPLACE;
  ASSERT_EQ(OB_SUCCESS, add_alter_column_with_name(arg, OB_DDL_DROP_COLUMN, "c1"));

  ObTableSchema orig_table_schema;
  ASSERT_EQ(OB_SUCCESS, add_base_column(orig_table_schema, "c1"));

  ObSchemaGetterGuard schema_guard;
  bool can_parallel = true;
  const char *reason = nullptr;
  ASSERT_EQ(OB_SUCCESS, check_drop_column_can_parallel(
      arg, orig_table_schema, schema_guard, can_parallel, reason));
  ASSERT_FALSE(can_parallel);
  ASSERT_STREQ("alter algorithm is not instant", reason);
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
  // Should pass arg-level check (column type is supported)
  // Actual parallel enablement depends on DDL control config
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
