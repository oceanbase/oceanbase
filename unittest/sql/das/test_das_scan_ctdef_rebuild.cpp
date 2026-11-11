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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_scan_ctdef_compact.h"
#include "sql/test_sql_utils.h"

#undef private
#undef protected

namespace oceanbase
{
namespace sql
{

using namespace common;
using namespace share::schema;

static constexpr int64_t TEST_COLUMN_COUNT = 40;

class ClusterVersionGuard
{
public:
  explicit ClusterVersionGuard(const uint64_t cluster_version) :
    saved_(ObClusterVersion::get_instance().get_cluster_version())
  {
    ObClusterVersion::get_instance().update_cluster_version(cluster_version);
  }
  ~ClusterVersionGuard()
  {
    ObClusterVersion::get_instance().update_cluster_version(saved_);
  }

private:
  uint64_t saved_;
};

class ExprSerializeStateGuard
{
public:
  ExprSerializeStateGuard(ObExpr::ObExprIArray *exprs, const bool use_varint) :
    saved_array_(ObExpr::get_serialize_array()), encoding_guard_(use_varint), decoding_guard_(use_varint)
  {
    ObExpr::get_serialize_array() = exprs;
  }
  ~ExprSerializeStateGuard()
  {
    ObExpr::get_serialize_array() = saved_array_;
  }

private:
  ObExpr::ObExprIArray *saved_array_;
  serialization::SerCompactEncodingGuard encoding_guard_;
  serialization::CompactExprPtrGuard decoding_guard_;
};

static int build_mixed_table_schema(ObTableSchema &table_schema, const uint64_t table_id, const bool include_lob)
{
  int ret = OB_SUCCESS;
  table_schema.reset();
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(table_id);
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_max_used_column_id(OB_APP_MIN_COLUMN_ID + TEST_COLUMN_COUNT - 1);
  table_schema.set_block_size(16 * 1024);
  if (OB_FAIL(table_schema.set_table_name("das_scan_ctdef_rebuild"))) {
    LOG_WARN("set test table name failed", K(ret));
  } else if (OB_FAIL(table_schema.set_compress_func_name("none"))) {
    LOG_WARN("set test table compression failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_COLUMN_COUNT; ++i) {
    ObColumnSchemaV2 column;
    char column_name[32];
    const int name_len = snprintf(column_name, sizeof(column_name), "c%02ld", i);
    ObObjMeta meta;
    ObObj orig_default;
    ObObj cur_default;
    ObObjType obj_type = ObIntType;
    if (include_lob && TEST_COLUMN_COUNT - 2 == i) {
      obj_type = ObLongTextType;
    } else {
      switch (i % 5) {
      case 0: obj_type = ObVarcharType; break;
      case 1: obj_type = ObNumberType; break;
      case 2: obj_type = ObDateTimeType; break;
      case 3: obj_type = ObIntType; break;
      default: obj_type = ObTinyIntType; break;
      }
    }
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID + i);
    column.set_rowkey_position(i < 2 ? i + 1 : 0);
    column.set_data_length(ObVarcharType == obj_type || ObLongTextType == obj_type ? 128 : 8);
    column.set_nullable(0 != i);
    column.set_is_hidden(TEST_COLUMN_COUNT - 1 == i);
    if (OB_UNLIKELY(name_len <= 0 || name_len >= static_cast<int>(sizeof(column_name)))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(column.set_column_name(column_name))) {
      LOG_WARN("set column name failed", K(ret), K(i));
    } else {
      meta.set_type(obj_type);
      if (ob_is_string_type(obj_type)) {
        meta.set_collation_level(CS_LEVEL_IMPLICIT);
        meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        column.set_charset_type(CHARSET_UTF8MB4);
        column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        orig_default.set_varchar(ObString::make_string("original-default"));
        cur_default.set_varchar(ObString::make_string("current-default"));
        orig_default.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        cur_default.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      } else if (ObDateTimeType == obj_type) {
        orig_default.set_datetime(1700000000000000L + i);
        cur_default.set_datetime(1800000000000000L + i);
      } else {
        orig_default.set_int(1000 + i);
        cur_default.set_int(2000 + i);
      }
      column.set_meta_type(meta);
      column.set_orig_default_value(orig_default);
      column.set_cur_default_value(cur_default, false);
      if (OB_FAIL(table_schema.add_column(column))) { LOG_WARN("add test table column failed", K(ret), K(i)); }
    }
  }
  return ret;
}

static void set_table_param_cg_flags(const ObTableSchema &table_schema, const bool rich_format,
                                     ObTableParam &table_param)
{
  table_param.set_is_spatial_index(table_schema.is_spatial_index());
  table_param.set_is_fts_index(table_schema.is_fts_index());
  table_param.set_is_multivalue_index(table_schema.is_multivalue_index_aux());
  table_param.set_is_vec_index(table_schema.is_vec_index());
  table_param.set_is_enable_semistruct_encoding(
    table_schema.get_semistruct_encoding_type().is_enable_semistruct_encoding());
  table_param.set_is_partition_table(table_schema.is_partitioned_table());
  table_param.set_is_mlog_table(table_schema.is_mlog_table());
  table_param.set_plan_enable_rich_format(rich_format);
  table_param.get_enable_lob_locator_v2() = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
}

static int init_sender_ctdef(ObDASScanCtDef &ctdef, const ObTableSchema &table_schema,
                             const ObIArray<uint64_t> &output_column_ids, const bool rich_format,
                             const bool force_mysql_mode = false, const bool query_cs_replica = false)
{
  int ret = OB_SUCCESS;
  ctdef.ref_table_id_ = table_schema.get_table_id();
  ctdef.schema_version_ = table_schema.get_schema_version();
  if (OB_FAIL(ctdef.access_column_ids_.init(TEST_COLUMN_COUNT))) {
    LOG_WARN("initialize test access-column ids failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_COLUMN_COUNT; ++i) {
    if (OB_FAIL(ctdef.access_column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("save test access-column id failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    set_table_param_cg_flags(table_schema, rich_format, ctdef.table_param_);
    if (OB_FAIL(ctdef.table_param_.convert(table_schema, ctdef.access_column_ids_, ctdef.pd_expr_spec_.pd_storage_flag_,
                                           &output_column_ids, force_mysql_mode, query_cs_replica))) {
      LOG_WARN("convert test table parameter failed", K(ret));
    }
  }
  return ret;
}

static int build_partial_output_ids(ObIArray<uint64_t> &output_column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_COLUMN_COUNT; i += 2) {
    if (OB_FAIL(output_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("save test output-column id failed", K(ret), K(i));
    }
  }
  return ret;
}

static int decode_table_param_mode(const char *buf, const int64_t data_len, int8_t &table_param_mode)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t version = 0;
  int64_t body_len = 0;
  bool use_range_v2 = false;
  uint64_t ref_table_id = OB_INVALID_ID;
  int64_t schema_version = 0;
  ObArenaAllocator allocator;
  ObFixedArray<uint64_t, ObIAllocator> access_column_ids(allocator);
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &version))) {
    LOG_WARN("decode test ScanCtDef version failed", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &body_len))) {
    LOG_WARN("decode test ScanCtDef body length failed", K(ret));
  }
  OB_UNIS_DECODE(use_range_v2);
  UNUSED(use_range_v2);
  OB_UNIS_DECODE(ref_table_id);
  OB_UNIS_DECODE(access_column_ids);
  OB_UNIS_DECODE(schema_version);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &table_param_mode))) {
    LOG_WARN("decode test ScanCtDef table-param mode failed", K(ret));
  }
  return ret;
}

static void assert_table_param_equal(const ObTableParam &expected, const ObTableParam &actual)
{
  ObArenaAllocator allocator;
  const int64_t expected_size = expected.get_serialize_size();
  const int64_t actual_size = actual.get_serialize_size();
  ASSERT_EQ(expected_size, actual_size);
  char *expected_buf = static_cast<char *>(allocator.alloc(expected_size));
  char *actual_buf = static_cast<char *>(allocator.alloc(actual_size));
  ASSERT_NE(nullptr, expected_buf);
  ASSERT_NE(nullptr, actual_buf);
  int64_t expected_pos = 0;
  int64_t actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, expected.serialize(expected_buf, expected_size, expected_pos));
  ASSERT_EQ(OB_SUCCESS, actual.serialize(actual_buf, actual_size, actual_pos));
  ASSERT_EQ(expected_size, expected_pos);
  ASSERT_EQ(actual_size, actual_pos);
  ASSERT_EQ(0, MEMCMP(expected_buf, actual_buf, expected_size));
  ASSERT_EQ(expected.get_table_id(), actual.get_table_id());
  ASSERT_EQ(expected.plan_enable_rich_format(), actual.plan_enable_rich_format());
  ASSERT_EQ(expected.enable_lob_locator_v2(), actual.enable_lob_locator_v2());
  ASSERT_EQ(expected.get_output_sel_mask().count(), actual.get_output_sel_mask().count());
  ASSERT_EQ(expected.get_read_info().is_oracle_mode(), actual.get_read_info().is_oracle_mode());
}

class TestDASScanCtDefRebuild : public ::testing::Test
{};

class TestDASScanCtDefRebuildWithSchema : public ::test::TestSqlUtils, public ::testing::Test
{
public:
  void SetUp() override
  {
    init();
  }
  void TearDown() override
  {
    destroy();
  }
};

class TenantSchemaGuardForwarder : public ObMultiVersionSchemaService
{
public:
  explicit TenantSchemaGuardForwarder(MockSchemaService &schema_service) : guard_calls_(0), schema_service_(schema_service)
  {}

  int get_tenant_schema_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard, const int64_t tenant_schema_version,
                              const int64_t sys_schema_version, const RefreshSchemaMode refresh_schema_mode) override
  {
    UNUSED(tenant_id);
    UNUSED(sys_schema_version);
    UNUSED(refresh_schema_mode);
    ++guard_calls_;
    return schema_service_.get_schema_guard(guard, tenant_schema_version);
  }

  int64_t guard_calls_;

private:
  MockSchemaService &schema_service_;
};

TEST_F(TestDASScanCtDefRebuild, u1_inline_descriptor_roundtrip_and_body_len)
{
  ObArenaAllocator alloc;
  ObDASScanCtDef src(alloc);
  src.ref_table_id_ = 500001;
  src.schema_version_ = 100;
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.init(2));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(16));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(17));
  ASSERT_FALSE(das_compact::can_rebuild_table_param(src));

  const int64_t size = das_compact::scan_ctdef_size(src);
  char *buf = static_cast<char *>(alloc.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
  ASSERT_EQ(size, pos);

  int64_t envelope_pos = 0;
  int64_t version = 0;
  int64_t body_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, size, envelope_pos, &version));
  ASSERT_EQ(serialization::SER_COMPACT_OPT_VERSION, version);
  ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, size, envelope_pos, &body_len));
  ASSERT_EQ(size - envelope_pos, body_len);
  int8_t table_param_mode = das_compact::TP_REBUILD;
  ASSERT_EQ(OB_SUCCESS, decode_table_param_mode(buf, size, table_param_mode));
  ASSERT_EQ(das_compact::TP_INLINE, table_param_mode);

  ObDASScanCtDef dst(alloc);
  int64_t dpos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, nullptr));
  ASSERT_EQ(size, dpos);
  ASSERT_EQ(src.ref_table_id_, dst.ref_table_id_);
  ASSERT_EQ(src.schema_version_, dst.schema_version_);
  ASSERT_EQ(src.access_column_ids_.count(), dst.access_column_ids_.count());

  const int64_t reserialized_size = das_compact::scan_ctdef_size(dst);
  char *reserialized_buf = static_cast<char *>(alloc.alloc(reserialized_size));
  ASSERT_NE(nullptr, reserialized_buf);
  int64_t reserialized_pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(dst, reserialized_buf, reserialized_size, reserialized_pos));
  ASSERT_EQ(size, reserialized_size);
  ASSERT_EQ(size, reserialized_pos);
  ASSERT_EQ(0, MEMCMP(buf, reserialized_buf, size));

  ObDASScanCtDef truncated_dst(alloc);
  int64_t truncated_pos = 0;
  ASSERT_NE(OB_SUCCESS, das_compact::deserialize_scan_ctdef(truncated_dst, buf, size - 1, truncated_pos, nullptr));
}

static void init_rebuild_candidate(ObDASScanCtDef &ctdef)
{
  ctdef.ref_table_id_ = 500002;
  ctdef.schema_version_ = 101;
}

TEST_F(TestDASScanCtDefRebuild, u1_rebuild_requires_decode_context)
{
  ObArenaAllocator alloc;
  ObDASScanCtDef src(alloc);
  init_rebuild_candidate(src);
  ASSERT_TRUE(das_compact::can_rebuild_table_param(src));

  const int64_t size = das_compact::scan_ctdef_size(src);
  char *buf = static_cast<char *>(alloc.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
  ASSERT_EQ(size, pos);
  int8_t table_param_mode = das_compact::TP_INLINE;
  ASSERT_EQ(OB_SUCCESS, decode_table_param_mode(buf, size, table_param_mode));
  ASSERT_EQ(das_compact::TP_REBUILD, table_param_mode);

  ObDASScanCtDef dst(alloc);
  int64_t dpos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, nullptr));
}

TEST_F(TestDASScanCtDefRebuildWithSchema, u2_rebuild_40_column_table_param_equivalence)
{
  ObTableSchema table_schema;
  const uint64_t table_id = 510001;
  ASSERT_EQ(OB_SUCCESS, build_mixed_table_schema(table_schema, table_id, true));
  ASSERT_EQ(OB_SUCCESS, add_table_schema(table_schema));

  ObSEArray<uint64_t, TEST_COLUMN_COUNT> output_column_ids;
  ASSERT_EQ(OB_SUCCESS, build_partial_output_ids(output_column_ids));
  ObArenaAllocator allocator;
  ObDASScanCtDef src(allocator);
  ASSERT_EQ(OB_SUCCESS, init_sender_ctdef(src, table_schema, output_column_ids, true));
  ASSERT_TRUE(das_compact::can_rebuild_table_param(src));
  ASSERT_EQ(TEST_COLUMN_COUNT, src.table_param_.get_output_sel_mask().count());

  const int64_t size = das_compact::scan_ctdef_size(src);
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
  ASSERT_EQ(size, pos);

  ObDASScanCtDef dst(allocator);
  das_compact::ObScanCtDefDecodeCtx decode_ctx = {&exec_ctx_};
  int64_t dpos = 0;
  ObSchemaGetterGuard *saved_sql_schema_guard = exec_ctx_.get_sql_ctx()->schema_guard_;
  ObMultiVersionSchemaService *saved_schema_service = GCTX.schema_service_;
  TenantSchemaGuardForwarder schema_guard_forwarder(*schema_service_);
  exec_ctx_.get_sql_ctx()->schema_guard_ = nullptr;
  GCTX.schema_service_ = &schema_guard_forwarder;
  const int decode_ret = das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, &decode_ctx);
  GCTX.schema_service_ = saved_schema_service;
  exec_ctx_.get_sql_ctx()->schema_guard_ = saved_sql_schema_guard;
  ASSERT_EQ(OB_SUCCESS, decode_ret);
  ASSERT_EQ(size, dpos);
  ASSERT_NO_FATAL_FAILURE(assert_table_param_equal(src.table_param_, dst.table_param_));

  // Exercise the tracepoint at each implementation site, without an errsim
  // argument. The forwarder supplies guards but has no versioned schema cache
  // or refresh service, so forced precise reads and refreshes return retry.
  const EventItem saved_event = EventTable::EN_DAS_REBUILD_TABLE_PARAM.item_;
  for (const int errsim_no : {OB_SUCCESS, 1, OB_SCHEMA_EAGAIN, OB_ERR_UNEXPECTED}) {
    SCOPED_TRACE(errsim_no);
    TP_SET_EVENT(EventTable::EN_DAS_REBUILD_TABLE_PARAM, errsim_no, 2, 0);
    ObDASScanCtDef injected(allocator);
    schema_guard_forwarder.guard_calls_ = 0;
    GCTX.schema_service_ = &schema_guard_forwarder;
    dpos = 0;
    const int injected_ret = das_compact::deserialize_scan_ctdef(injected, buf, size, dpos, &decode_ctx);
    const int64_t remaining_occurrences = EventTable::EN_DAS_REBUILD_TABLE_PARAM.item_.occur_;
    GCTX.schema_service_ = saved_schema_service;
    EventTable::EN_DAS_REBUILD_TABLE_PARAM.item_.set_event(saved_event);
    EXPECT_EQ(errsim_no == OB_SUCCESS ? OB_SUCCESS : OB_DAS_CTDEF_REBUILD_MISS, injected_ret);
    EXPECT_EQ(errsim_no == OB_SUCCESS ? 1 : 0, remaining_occurrences);
    EXPECT_EQ(errsim_no == 1 || errsim_no == OB_ERR_UNEXPECTED ? 1 : 0,
              schema_guard_forwarder.guard_calls_);
    if (OB_SUCCESS == injected_ret) {
      EXPECT_EQ(size, dpos);
      ASSERT_NO_FATAL_FAILURE(assert_table_param_equal(src.table_param_, injected.table_param_));
    }
  }
}

TEST_F(TestDASScanCtDefRebuildWithSchema, u3_schema_version_mismatch_is_retryable)
{
  ObTableSchema table_schema;
  const uint64_t table_id = 510002;
  ASSERT_EQ(OB_SUCCESS, build_mixed_table_schema(table_schema, table_id, false));
  ASSERT_EQ(OB_SUCCESS, add_table_schema(table_schema));

  ObSEArray<uint64_t, TEST_COLUMN_COUNT> output_column_ids;
  ASSERT_EQ(OB_SUCCESS, build_partial_output_ids(output_column_ids));
  const int64_t schema_versions[] = {table_schema.get_schema_version() - 1, table_schema.get_schema_version() + 1};
  for (int64_t i = 0; i < ARRAYSIZEOF(schema_versions); ++i) {
    ObArenaAllocator allocator;
    ObDASScanCtDef src(allocator);
    ASSERT_EQ(OB_SUCCESS, init_sender_ctdef(src, table_schema, output_column_ids, false));
    src.schema_version_ = schema_versions[i];
    ASSERT_TRUE(das_compact::can_rebuild_table_param(src));

    const int64_t size = das_compact::scan_ctdef_size(src);
    char *buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
    ObDASScanCtDef dst(allocator);
    das_compact::ObScanCtDefDecodeCtx decode_ctx = {&exec_ctx_};
    int64_t dpos = 0;
    ASSERT_EQ(OB_DAS_CTDEF_REBUILD_MISS, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, &decode_ctx));
  }
}

TEST_F(TestDASScanCtDefRebuild, u4_can_rebuild_predicate_matrix)
{
  ObArenaAllocator alloc;
  ObDASScanCtDef ctdef(alloc);
  init_rebuild_candidate(ctdef);
  ASSERT_TRUE(das_compact::can_rebuild_table_param(ctdef));

  ctdef.is_external_table_ = true;
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.is_external_table_ = false;

  ctdef.ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_SCAN;
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.ir_scan_type_ = ObTSCIRScanType::OB_NOT_A_SPEC_SCAN;

  ctdef.is_index_merge_ = true;
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.is_index_merge_ = false;

  ctdef.table_param_.set_is_fts_index(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.table_param_.set_is_fts_index(false);
  ctdef.table_param_.set_is_spatial_index(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.table_param_.set_is_spatial_index(false);
  ctdef.table_param_.set_is_multivalue_index(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.table_param_.set_is_multivalue_index(false);
  ctdef.table_param_.set_is_vec_index(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.table_param_.set_is_vec_index(false);
  ctdef.table_param_.set_is_mlog_table(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.table_param_.set_is_mlog_table(false);
  ctdef.table_param_.set_is_enable_semistruct_encoding(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  ctdef.table_param_.set_is_enable_semistruct_encoding(false);

  ASSERT_EQ(OB_SUCCESS, ctdef.aggregate_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, ctdef.aggregate_column_ids_.push_back(16));
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));

  ObDASScanCtDef group_by_ctdef(alloc);
  init_rebuild_candidate(group_by_ctdef);
  ASSERT_EQ(OB_SUCCESS, group_by_ctdef.group_by_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, group_by_ctdef.group_by_column_ids_.push_back(16));
  ASSERT_FALSE(das_compact::can_rebuild_table_param(group_by_ctdef));

  ObDASScanCtDef inline_src(alloc);
  init_rebuild_candidate(inline_src);
  inline_src.table_param_.set_is_fts_index(true);
  ASSERT_FALSE(das_compact::can_rebuild_table_param(inline_src));
  const int64_t size = das_compact::scan_ctdef_size(inline_src);
  char *buf = static_cast<char *>(alloc.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(inline_src, buf, size, pos));
  int8_t table_param_mode = das_compact::TP_REBUILD;
  ASSERT_EQ(OB_SUCCESS, decode_table_param_mode(buf, size, table_param_mode));
  ASSERT_EQ(das_compact::TP_INLINE, table_param_mode);
  ObDASScanCtDef inline_dst(alloc);
  int64_t dpos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::deserialize_scan_ctdef(inline_dst, buf, size, dpos, nullptr));
  const int64_t size2 = das_compact::scan_ctdef_size(inline_dst);
  char *buf2 = static_cast<char *>(alloc.alloc(size2));
  ASSERT_NE(nullptr, buf2);
  int64_t pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(inline_dst, buf2, size2, pos2));
  ASSERT_EQ(size, size2);
  ASSERT_EQ(0, MEMCMP(buf, buf2, size));
}

TEST_F(TestDASScanCtDefRebuildWithSchema, u4_column_replica_is_inline)
{
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_mixed_table_schema(table_schema, 510005, false));
  ASSERT_EQ(OB_SUCCESS, add_table_schema(table_schema));
  ObSEArray<uint64_t, TEST_COLUMN_COUNT> output_column_ids;
  ASSERT_EQ(OB_SUCCESS, build_partial_output_ids(output_column_ids));
  ObArenaAllocator allocator;
  ObDASScanCtDef ctdef(allocator);
  ASSERT_EQ(OB_SUCCESS, init_sender_ctdef(ctdef, table_schema, output_column_ids, false, false, true));
  ASSERT_TRUE(ctdef.table_param_.is_column_replica_table());
  ASSERT_FALSE(das_compact::can_rebuild_table_param(ctdef));
  const int64_t size = das_compact::scan_ctdef_size(ctdef);
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(ctdef, buf, size, pos));
  int8_t table_param_mode = das_compact::TP_REBUILD;
  ASSERT_EQ(OB_SUCCESS, decode_table_param_mode(buf, size, table_param_mode));
  ASSERT_EQ(das_compact::TP_INLINE, table_param_mode);
}

TEST_F(TestDASScanCtDefRebuild, u5_output_selection_mask_boundaries)
{
  ObArenaAllocator allocator;
  ObTableParam table_param(allocator);
  const char all_output[] = {static_cast<char>(0xFF), static_cast<char>(0x03)};
  ASSERT_EQ(OB_SUCCESS, table_param.assign_output_sel_mask_from_bitmap(10, all_output, sizeof(all_output)));
  ASSERT_EQ(10, table_param.get_output_sel_mask().count());
  for (int64_t i = 0; i < 10; ++i) { ASSERT_TRUE(table_param.get_output_sel_mask().at(i)); }

  const char partial_output[] = {static_cast<char>(0x55), static_cast<char>(0x02)};
  ASSERT_EQ(OB_SUCCESS, table_param.assign_output_sel_mask_from_bitmap(10, partial_output, sizeof(partial_output)));
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ((i < 8 && 0 == i % 2) || 9 == i, table_param.get_output_sel_mask().at(i));
  }

  const char forced_first_output[] = {static_cast<char>(0x01), 0};
  ASSERT_EQ(OB_SUCCESS,
            table_param.assign_output_sel_mask_from_bitmap(10, forced_first_output, sizeof(forced_first_output)));
  ASSERT_TRUE(table_param.get_output_sel_mask().at(0));
  for (int64_t i = 1; i < 10; ++i) { ASSERT_FALSE(table_param.get_output_sel_mask().at(i)); }
  ASSERT_EQ(OB_INVALID_ARGUMENT, table_param.assign_output_sel_mask_from_bitmap(10, forced_first_output, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, table_param.assign_output_sel_mask_from_bitmap(10, nullptr, 2));
  ASSERT_EQ(OB_SUCCESS, table_param.assign_output_sel_mask_from_bitmap(0, nullptr, 0));
  ASSERT_EQ(0, table_param.get_output_sel_mask().count());
}

TEST_F(TestDASScanCtDefRebuildWithSchema, u5_cg_zero_output_forces_first_column)
{
  ObArenaAllocator allocator;
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_mixed_table_schema(table_schema, 510006, false));
  ASSERT_EQ(OB_SUCCESS, add_table_schema(table_schema));
  ObSEArray<uint64_t, TEST_COLUMN_COUNT> no_output_columns;
  ObDASScanCtDef zero_output_ctdef(allocator);
  ASSERT_EQ(OB_SUCCESS, init_sender_ctdef(zero_output_ctdef, table_schema, no_output_columns, false));
  ASSERT_EQ(TEST_COLUMN_COUNT, zero_output_ctdef.table_param_.get_output_sel_mask().count());
  ASSERT_TRUE(zero_output_ctdef.table_param_.get_output_sel_mask().at(0));
  for (int64_t i = 1; i < TEST_COLUMN_COUNT; ++i) {
    ASSERT_FALSE(zero_output_ctdef.table_param_.get_output_sel_mask().at(i));
  }
}

TEST_F(TestDASScanCtDefRebuildWithSchema, u6_mysql_oracle_lob_and_force_mysql_equivalence)
{
  struct ModeCase
  {
    lib::Worker::CompatMode mode_;
    uint64_t table_id_;
    bool force_mysql_mode_;
  };
  const ModeCase cases[] = {
    {lib::Worker::CompatMode::MYSQL, 510003, false},
    {lib::Worker::CompatMode::ORACLE, 510004, false},
    {lib::Worker::CompatMode::ORACLE, share::OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORA_TID, true},
    // Models the real table ID after the Oracle virtual-table mapping is removed.
    {lib::Worker::CompatMode::ORACLE, 510005, true}};

  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    lib::CompatModeGuard compat_mode_guard(cases[i].mode_);
    ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, build_mixed_table_schema(table_schema, cases[i].table_id_, true));
    char table_name[48];
    ASSERT_GT(snprintf(table_name, sizeof(table_name), "das_rebuild_mode_%ld", i), 0);
    ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name(table_name));
    ASSERT_EQ(OB_SUCCESS, add_table_schema(table_schema));

    ObSEArray<uint64_t, TEST_COLUMN_COUNT> output_column_ids;
    ASSERT_EQ(OB_SUCCESS, build_partial_output_ids(output_column_ids));
    ObArenaAllocator allocator;
    ObDASScanCtDef src(allocator);
    ASSERT_EQ(OB_SUCCESS, init_sender_ctdef(src, table_schema, output_column_ids, true, cases[i].force_mysql_mode_));
    ASSERT_TRUE(das_compact::can_rebuild_table_param(src));
    ASSERT_EQ(lib::Worker::CompatMode::ORACLE == cases[i].mode_ && !cases[i].force_mysql_mode_,
              src.table_param_.get_read_info().is_oracle_mode());

    const int64_t size = das_compact::scan_ctdef_size(src);
    char *buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
    ObDASScanCtDef dst(allocator);
    das_compact::ObScanCtDefDecodeCtx decode_ctx = {&exec_ctx_};
    int64_t dpos = 0;
    ASSERT_EQ(OB_SUCCESS, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, &decode_ctx));
    ASSERT_NO_FATAL_FAILURE(assert_table_param_equal(src.table_param_, dst.table_param_));
    const auto &src_cmps = src.table_param_.get_read_info().get_datum_utils().get_cmp_funcs();
    const auto &dst_cmps = dst.table_param_.get_read_info().get_datum_utils().get_cmp_funcs();
    ASSERT_EQ(src_cmps.count(), dst_cmps.count());
    for (int64_t col = 0; col < src_cmps.count(); ++col) {
      ASSERT_EQ(src_cmps.at(col).get_cmp_func().cmp_func_, dst_cmps.at(col).get_cmp_func().cmp_func_);
    }
    ASSERT_EQ(cases[i].mode_ == lib::Worker::CompatMode::ORACLE, lib::is_oracle_mode());
    ASSERT_EQ(src.table_param_.use_lob_locator(), dst.table_param_.use_lob_locator());
    ASSERT_EQ(src.table_param_.get_rowid_projector().count(), dst.table_param_.get_rowid_projector().count());
  }
}

TEST_F(TestDASScanCtDefRebuild, u7_gate_off_keeps_legacy_class_bytes)
{
  ObArenaAllocator allocator;
  ObDASScanCtDef src(allocator);
  src.ref_table_id_ = 500005;
  src.schema_version_ = 104;
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(OB_APP_MIN_COLUMN_ID));
  const char output_mask[] = {1};
  ASSERT_EQ(OB_SUCCESS, src.table_param_.assign_output_sel_mask_from_bitmap(1, output_mask, sizeof(output_mask)));
  ASSERT_TRUE(das_compact::can_rebuild_table_param(src));

  char old_gate_buf[64 * 1024];
  char new_gate_buf[64 * 1024];
  int64_t old_gate_pos = 0;
  int64_t new_gate_pos = 0;
  {
    ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_2);
    ASSERT_EQ(OB_SUCCESS, src.serialize(old_gate_buf, sizeof(old_gate_buf), old_gate_pos));
  }
  {
    ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_3);
    ASSERT_EQ(OB_SUCCESS, src.serialize(new_gate_buf, sizeof(new_gate_buf), new_gate_pos));
  }
  ASSERT_EQ(old_gate_pos, new_gate_pos);
  ASSERT_EQ(0, MEMCMP(old_gate_buf, new_gate_buf, old_gate_pos));
}

TEST_F(TestDASScanCtDefRebuild, u8_expr_ref_varint_scope_restored_on_rebuild)
{
  ObArenaAllocator allocator;
  ObArray<ObExpr> exprs;
  ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(3));
  ExprSerializeStateGuard serialize_state(&exprs, false);
  ObDASScanCtDef src(allocator);
  src.ref_table_id_ = 500006;
  src.schema_version_ = 105;
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(OB_APP_MIN_COLUMN_ID));
  const char output_mask[] = {1};
  ASSERT_EQ(OB_SUCCESS, src.table_param_.assign_output_sel_mask_from_bitmap(1, output_mask, sizeof(output_mask)));
  ASSERT_EQ(OB_SUCCESS, src.result_output_.init(exprs.count()));
  for (int64_t i = 0; i < exprs.count(); ++i) { ASSERT_EQ(OB_SUCCESS, src.result_output_.push_back(&exprs.at(i))); }
  ASSERT_TRUE(das_compact::can_rebuild_table_param(src));

  const int64_t size = das_compact::scan_ctdef_size(src);
  ASSERT_EQ(&exprs, ObExpr::get_serialize_array());
  ASSERT_FALSE(serialization::using_v4_encoding());
  serialization::using_v4_encoding() = true;
  ASSERT_EQ(size, das_compact::scan_ctdef_size(src));
  serialization::using_v4_encoding() = false;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  {
    ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
  }
  ASSERT_EQ(&exprs, ObExpr::get_serialize_array());
  ASSERT_FALSE(serialization::using_v4_encoding());

  serialization::using_v4_encoding() = true;
  ASSERT_EQ(size, das_compact::scan_ctdef_size(src));
  ASSERT_EQ(&exprs, ObExpr::get_serialize_array());
  ASSERT_TRUE(serialization::using_v4_encoding());
  serialization::using_v4_encoding() = false;

  ObDASScanCtDef dst(allocator);
  int64_t dpos = 0;
  {
    ASSERT_EQ(OB_ERR_UNEXPECTED, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, nullptr));
  }
  ASSERT_EQ(&exprs, ObExpr::get_serialize_array());
  ASSERT_FALSE(serialization::using_v4_encoding());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
