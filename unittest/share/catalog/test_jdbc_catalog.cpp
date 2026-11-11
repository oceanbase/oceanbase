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

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE

#include "deps/oblib/src/lib/allocator/page_arena.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/worker.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_defer.h"
#include "plugin/external_table/ob_external_arrow_data_loader.h"
#include "plugin/external_table/ob_external_filter.h"
#include "plugin/external_table/ob_external_arrow_object.h"
#include <apache-arrow/arrow/c/bridge.h>
#include "share/catalog/jdbc/ob_jdbc_catalog_types.h"
#include "share/catalog/jdbc/ob_jdbc_catalog.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/rc/ob_tenant_base.h"
#include "share/object/ob_obj_cast.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_external_table_column_schema_helper.h"
#include "share/schema/ob_table_schema.h"
#include "sql/table_format/jdbc/ob_jdbc_table_metadata.h"
#include "sql/ob_sql_init.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_resolver_utils.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <csignal>
#include <cstring>
#include <string>

using namespace oceanbase;

namespace
{

sql::jdbc::ObJdbcCharsetContext charset_context(
    const common::ObCollationType ordinary = common::CS_TYPE_UTF8MB4_BIN,
    const common::ObCollationType national = common::CS_TYPE_UTF16_BIN)
{
  sql::jdbc::ObJdbcCharsetContext context;
  context.default_collation_ = ordinary;
  context.nchar_collation_ = national;
  return context;
}

std::string mysql_metadata_json()
{
  return R"({
    "contractVersion":1,
    "targetMode":"MYSQL",
    "qualifiedTableName":"[SalesDB].[sales].[customers]",
    "columns":[{
      "name":"display_name",
      "ordinalPosition":1,
      "sourceNullable":true,
      "targetNullable":true,
      "sourceCharset":"utf8mb4",
      "sourceCollation":"utf8mb4_bin",
      "targetType":{
        "name":"VARCHAR",
        "length":64,
        "lengthSemantics":"CHAR",
        "precision":null,
        "scale":null
      }
    }]
  })";
}

std::string oracle_metadata_json()
{
  return R"({
    "contractVersion":1,
    "targetMode":"ORACLE",
    "qualifiedTableName":"[SalesDB].[dbo].[orders]",
    "columns":[{
      "name":"order_id",
      "ordinalPosition":1,
      "sourceNullable":false,
      "targetNullable":false,
      "targetType":{
        "name":"NUMBER",
        "length":null,
        "lengthSemantics":null,
        "precision":10,
        "scale":0
      }
    },{
      "name":"clock_value",
      "ordinalPosition":2,
      "sourceNullable":true,
      "targetNullable":true,
      "targetType":{
        "name":"INTERVAL_DAY_TO_SECOND",
        "length":null,
        "lengthSemantics":null,
        "precision":1,
        "scale":7
      }
    }]
  })";
}

std::string unsupported_metadata_json()
{
  return R"({
    "contractVersion":1,
    "targetMode":"MYSQL",
    "qualifiedTableName":"[db].[dbo].[mixed]",
    "columns":[
      {"name":"id", "ordinalPosition":1, "sourceNullable":false, "targetNullable":false,
       "targetType":{"name":"INT"}},
      {"name":"v", "ordinalPosition":3, "sourceNullable":true, "targetNullable":true,
       "targetType":{"name":"UNSUPPORTED"}},
      {"name":"tail", "ordinalPosition":4, "sourceNullable":false, "targetNullable":false,
       "targetType":{"name":"INT"}}
    ]
  })";
}

void replace_once(std::string &text,
                  const std::string &source,
                  const std::string &replacement)
{
  const std::string::size_type position = text.find(source);
  ASSERT_NE(std::string::npos, position);
  text.replace(position, source.length(), replacement);
}

} // namespace

class TestJdbcCatalog : public ::testing::Test
{
public:
  TestJdbcCatalog() = default;
  ~TestJdbcCatalog() = default;

  int build_schema(const share::ObJdbcTableMetadataDesc &metadata,
                   const sql::jdbc::ObJdbcCharsetContext &context,
                   share::schema::ObTableSchema *&schema)
  {
    sql::jdbc::ObJdbcTableMetadata table_metadata(allocator_);
    int ret = table_metadata.init(OB_SYS_TENANT_ID, OB_MIN_USER_OBJECT_ID,
        OB_MIN_USER_OBJECT_ID + 1, OB_MIN_USER_OBJECT_ID + 2,
        common::ObString("sales"), common::ObString("customers"), common::OB_ORIGIN_AND_INSENSITIVE);
    if (OB_SUCCESS == ret) {
      ret = table_metadata.setup_table_schema(common::ObString("sqlserver"), common::ObString(), metadata, context);
    }
    if (OB_SUCCESS == ret) {
      ret = table_metadata.build_table_schema(schema);
    }
    return ret;
  }

  void expect_masked_url(const char *url, const char *expected)
  {
    common::ObString masked_url;
    ASSERT_EQ(OB_SUCCESS,
              share::ObJdbcCatalogProperties::mask_url_for_show(
                  common::ObString(url), allocator_, masked_url));
    EXPECT_EQ(0, masked_url.compare(expected));
  }

  void expect_generated_column_path(const share::schema::ObColumnSchemaV2 &column,
                                    const share::schema::ObTableSchema &table,
                                    sql::ObSQLSessionInfo &session,
                                    const char *expected_name)
  {
    using namespace common;
    ObString definition;
    ASSERT_EQ(OB_SUCCESS, column.get_cur_default_value().get_string(definition));
    // Follow the DML resolver's schema-text conversion before parsing the expression.
    ASSERT_EQ(OB_SUCCESS, sql::ObSQLUtils::convert_gen_col_def_text_from_schema_for_resolve(
        allocator_, session.get_dtc_params(), &column, definition));
    ObSQLMode sql_mode = session.get_sql_mode();
    ObCollationType collation = session.get_local_collation_connection();
    ASSERT_EQ(OB_SUCCESS, sql::ObSQLUtils::merge_solidified_var_into_sql_mode(
        &column.get_local_session_var(), sql_mode));
    ASSERT_EQ(OB_SUCCESS, sql::ObSQLUtils::merge_solidified_var_into_collation(
        column.get_local_session_var(), collation));

    sql::ObRawExprFactory factory(allocator_);
    sql::ObRawExpr *expr = nullptr;
    ObArray<sql::ObQualifiedName> columns;
    ASSERT_EQ(OB_SUCCESS, sql::ObRawExprUtils::build_generated_column_expr(
        definition, factory, session, sql_mode, collation, expr, columns, &table,
        false, nullptr, nullptr));
    ASSERT_TRUE(OB_NOT_NULL(expr));
    ASSERT_EQ(T_FUN_SYS_GET_PATH, expr->get_expr_type());
    ObString refined_name;
    ObString data_access_path;
    ASSERT_EQ(OB_SUCCESS, sql::ObResolverUtils::build_refined_external_column_expr_name(
        allocator_, &session, expr, sql::ColumnIndexType::NAME, 0,
        refined_name, data_access_path));
    EXPECT_EQ(ObString(expected_name), data_access_path);
    EXPECT_EQ(0, column.get_local_session_var().get_var_count());
    EXPECT_EQ(sql_mode, session.get_sql_mode());
  }

  template <typename Check>
  void expect_arrow_values(const std::shared_ptr<arrow::Array> &values,
                           const sql::ObDatumMeta &meta,
                           common::VecValueTypeClass value_tc,
                           Check check,
                           const bool expect_success = true,
                           const uint64_t cast_mode = 0)
  {
    using namespace common;
    using plugin::external::ObArrowDataLoader;
    ASSERT_TRUE(OB_NOT_NULL(values.get()));
    sql::ObExecContext exec_ctx(allocator_);
    sql::ObSQLSessionInfo session;
    ASSERT_EQ(OB_SUCCESS, session.set_time_zone(ObString("+00:00"), true, false));
    exec_ctx.set_my_session(&session);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    alignas(16) char frame[8192] = {};
    char *frames[] = {frame};
    eval_ctx.frames_ = frames;
    eval_ctx.set_max_batch_size(3);
    eval_ctx.reuse(3);
    sql::ObExpr expr;
    expr.obj_meta_.set_type(meta.get_type());
    expr.obj_meta_.set_collation_type(meta.cs_type_);
    expr.datum_meta_ = meta;
    expr.datum_meta_.length_semantics_ = LS_CHAR;
    expr.max_length_ = 64;
    expr.extra_ = cast_mode;
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = ~0;
    expr.frame_idx_ = 0;
    expr.vector_header_off_ = 0;
    expr.eval_info_off_ = 512;
    expr.datum_off_ = 1024;
    expr.dyn_buf_header_offset_ = 1536;
    expr.res_buf_off_ = 2048;
    expr.res_buf_len_ = 256;
    expr.vec_value_tc_ = value_tc;
    ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx, VEC_UNIFORM, 3, true /* use_reserve_buf */));
    plugin::external::ObArrowDataLoaderFactory factory;
    ObArrowDataLoader *loader = nullptr;
    ASSERT_EQ(OB_SUCCESS, factory.select_loader(allocator_, *values->type(), meta, loader));
    ASSERT_TRUE(OB_NOT_NULL(loader));
    if (OB_NOT_NULL(loader)) {
      const int ret = loader->load(*values, eval_ctx, &expr);
      if (expect_success) {
        EXPECT_EQ(OB_SUCCESS, ret);
        ObIVector *vector = expr.get_vector(eval_ctx);
        EXPECT_TRUE(OB_NOT_NULL(vector));
        if (OB_SUCCESS == ret && OB_NOT_NULL(vector)) {
          EXPECT_TRUE(vector->is_null(1));
          check(*vector);
        }
      } else {
        EXPECT_NE(OB_SUCCESS, ret);
      }
      loader->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
    }
  }

  template <typename ArrowType, typename Check>
  void expect_fixed_arrow_slices(const sql::ObDatumMeta &meta,
                                 const common::VecValueTypeClass value_tc,
                                 Check check)
  {
    using namespace common;
    using plugin::external::ObArrowDataLoader;
    constexpr int64_t max_batch_size = 70;
    sql::ObExecContext exec_ctx(allocator_);
    sql::ObSQLSessionInfo session;
    ASSERT_EQ(OB_SUCCESS, session.set_time_zone(ObString("+00:00"), true, false));
    exec_ctx.set_my_session(&session);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    alignas(16) char frame[8192] = {};
    char *frames[] = {frame};
    eval_ctx.frames_ = frames;
    eval_ctx.set_max_batch_size(max_batch_size);
    sql::ObExpr expr;
    expr.obj_meta_.set_type(meta.get_type());
    expr.obj_meta_.set_collation_type(meta.cs_type_);
    expr.datum_meta_ = meta;
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = ~0;
    expr.frame_idx_ = 0;
    expr.vector_header_off_ = 0;
    expr.eval_info_off_ = 512;
    expr.null_bitmap_off_ = 768;
    expr.datum_off_ = 1024;
    expr.dyn_buf_header_offset_ = 2560;
    expr.res_buf_off_ = 3072;
    expr.res_buf_len_ = sizeof(typename ArrowType::c_type);
    expr.vec_value_tc_ = value_tc;
    plugin::external::ObArrowDataLoaderFactory factory;
    ObArrowDataLoader *loader = nullptr;
    const ArrowType arrow_type;
    ASSERT_EQ(OB_SUCCESS, factory.select_loader(allocator_, arrow_type, meta, loader));
    ASSERT_TRUE(OB_NOT_NULL(loader));
    if (OB_NOT_NULL(loader)) {
      // Reuse the same output storage across mixed, all-NULL and all-valid batches.
      for (const int null_pattern : {0, 1, 2}) {
        SCOPED_TRACE(null_pattern);
        arrow::NumericBuilder<ArrowType> builder;
        for (int64_t i = 0; i < 160; ++i) {
          if (1 == null_pattern || (0 == null_pattern && (i * 37 + 11) % 17 < 7)) {
            ASSERT_TRUE(builder.AppendNull().ok());
          } else {
            const typename ArrowType::c_type value =
                static_cast<typename ArrowType::c_type>(i * 19 - 1000)
                + static_cast<typename ArrowType::c_type>(0.25);
            ASSERT_TRUE(builder.Append(value).ok());
          }
        }
        std::shared_ptr<arrow::Array> values;
        ASSERT_TRUE(builder.Finish(&values).ok());
        ASSERT_TRUE(OB_NOT_NULL(values.get()));
        for (const int64_t parent_offset : {0, 3}) {
          SCOPED_TRACE(parent_offset);
          const std::shared_ptr<arrow::Array> parent = values->Slice(parent_offset);
          ASSERT_TRUE(OB_NOT_NULL(parent.get()));
          for (const int64_t offset : {0, 1, 7, 8, 63, 64, 65}) {
            SCOPED_TRACE(offset);
            for (const int64_t length : {1, 7, 8, 63, 64, 65, 70}) {
              SCOPED_TRACE(length);
              const std::shared_ptr<arrow::Array> slice = parent->Slice(offset, length);
              ASSERT_TRUE(OB_NOT_NULL(slice.get()));
              eval_ctx.reuse(length);
              ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx, VEC_FIXED, length));
              ASSERT_EQ(OB_SUCCESS, loader->load(*slice, eval_ctx, &expr));
              ObIVector *vector = expr.get_vector(eval_ctx);
              ASSERT_TRUE(OB_NOT_NULL(vector));
              if (OB_NOT_NULL(vector)) {
                EXPECT_EQ(VEC_FIXED, vector->get_format());
                const auto &numeric = static_cast<const arrow::NumericArray<ArrowType> &>(*slice);
                for (int64_t i = 0; i < length; ++i) {
                  SCOPED_TRACE(i);
                  EXPECT_EQ(slice->IsNull(i), vector->is_null(i));
                  if (!slice->IsNull(i)) {
                    check(*vector, i, numeric.Value(i));
                  }
                }
              }
            }
          }
        }
      }
      loader->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
    }
  }

  common::ObArenaAllocator allocator_;
};

TEST_F(TestJdbcCatalog, resolve_and_serialize)
{
  const char *url_str = "jdbc:sqlserver://127.0.0.1:1433;databaseName=db;user=u;password=p";
  ParseNode url_value;
  url_value.str_len_ = std::strlen(url_str);
  url_value.str_value_ = url_str;
  ParseNode *url_children[1] = {&url_value};

  ParseNode url_property;
  url_property.type_ = ObItemType::T_URI;
  url_property.value_ = 1;
  url_property.num_child_ = 1;
  url_property.children_ = url_children;
  ParseNode *root_children[1] = {&url_property};

  ParseNode root;
  root.num_child_ = 1;
  root.children_ = root_children;

  share::ObJdbcCatalogProperties properties;
  ASSERT_EQ(OB_SUCCESS, properties.resolve_catalog_properties(root));
  common::ObString serialized;
  ASSERT_EQ(OB_SUCCESS, properties.to_string_with_alloc(serialized, allocator_));

  share::ObJdbcCatalogProperties loaded_properties;
  ASSERT_EQ(OB_SUCCESS, loaded_properties.load_from_string(serialized, allocator_));
  EXPECT_EQ(0, loaded_properties.url_.compare(url_str));
}

TEST_F(TestJdbcCatalog, serialize_long_url_round_trip)
{
  // Cover the CStringHelper boundary and the outer 4096-byte JSON buffer.
  // 544 and 2080 are encrypted/encoded URL lengths for 240 and 1023 input bytes.
  for (const int64_t length : {511, 512, 513, 544, 2080}) {
    SCOPED_TRACE(length);
    const std::string url(length, 'A');
    share::ObJdbcCatalogProperties properties;
    properties.url_.assign_ptr(url.data(), url.length());

    char small_buf[32];
    int64_t pos = 0;
    EXPECT_EQ(OB_SIZE_OVERFLOW, properties.to_string(small_buf, sizeof(small_buf), pos));

    common::ObString serialized;
    ASSERT_EQ(OB_SUCCESS, properties.to_string_with_alloc(serialized, allocator_));
    share::ObJdbcCatalogProperties loaded_properties;
    ASSERT_EQ(OB_SUCCESS, loaded_properties.load_from_string(serialized, allocator_));
    EXPECT_EQ(0, loaded_properties.url_.compare(properties.url_));
  }
}

TEST_F(TestJdbcCatalog, reject_invalid_properties)
{
  ParseNode empty_value;
  empty_value.str_len_ = 1;
  empty_value.str_value_ = " ";
  ParseNode *empty_children[1] = {&empty_value};

  ParseNode url_property;
  url_property.type_ = ObItemType::T_URI;
  url_property.value_ = 1;
  url_property.num_child_ = 1;
  url_property.children_ = empty_children;
  ParseNode *root_children[1] = {&url_property};

  ParseNode root;
  root.num_child_ = 1;
  root.children_ = root_children;

  share::ObJdbcCatalogProperties properties;
  EXPECT_EQ(OB_INVALID_ARGUMENT, properties.resolve_catalog_properties(root));

  // Every rejected property is tested with an otherwise valid JDBC URL.
  const char *valid_url = "jdbc:sqlserver://127.0.0.1:1433;databaseName=db";
  empty_value.str_value_ = valid_url;
  empty_value.str_len_ = std::strlen(valid_url);
  share::ObJdbcCatalogProperties valid_properties;
  ASSERT_EQ(OB_SUCCESS, valid_properties.resolve_catalog_properties(root));

  ParseNode warehouse_property = url_property;
  warehouse_property.type_ = ObItemType::T_WAREHOUSE;
  ParseNode *invalid_children[] = {&url_property, &warehouse_property};
  root.num_child_ = 2;
  root.children_ = invalid_children;
  share::ObJdbcCatalogProperties invalid_properties;
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_properties.resolve_catalog_properties(root));

  root.num_child_ = 1;
  root.children_ = root_children;
  url_property.type_ = ObItemType::T_URI;
  url_property.value_ = 0;
  share::ObJdbcCatalogProperties uri_properties;
  EXPECT_EQ(OB_INVALID_ARGUMENT, uri_properties.resolve_catalog_properties(root));
}

TEST_F(TestJdbcCatalog, resolve_plugin_name)
{
  common::ObString plugin_name;
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcCatalogProperties::resolve_plugin_name(
                common::ObString("jdbc:sqlserver://server:1433"), plugin_name));
  EXPECT_EQ(0, plugin_name.compare("sqlserver"));

  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcCatalogProperties::resolve_plugin_name(
                common::ObString("JDBC:SQLSERVER://server:1433"), plugin_name));
  EXPECT_EQ(0, plugin_name.compare("SQLSERVER"));

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcCatalogProperties::resolve_plugin_name(
                common::ObString("sqlserver://server:1433"), plugin_name));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcCatalogProperties::resolve_plugin_name(
                common::ObString("jdbc:://server:1433"), plugin_name));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcCatalogProperties::resolve_plugin_name(
                common::ObString("jdbc:sqlserver"), plugin_name));
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcCatalogProperties::resolve_plugin_name(
                common::ObString("jdbc:postgresql://server:5432/db"), plugin_name));
  EXPECT_EQ(0, plugin_name.compare("postgresql"));
}

TEST_F(TestJdbcCatalog, mask_url_preserves_only_driver_prefix)
{
  const char *urls[] = {
      "jdbc:sqlserver://server:1433;databaseName=db;user=userName;password=secret;encrypt=true",
      "jdbc:sqlserver://server;PaSsWoRd={p;a}}ss};encrypt=true",
      "jdbc:sqlserver://server;password=one;user=user;PASSWORD=two",
      "jdbc:sqlserver://server;databaseName=db;user=user",
      "jdbc:sqlserver://server;keyStoreSecret=before;password=secret",
      "jdbc:sqlserver://server;password=secret;keyStoreSecret=after",
      "jdbc:sqlserver://server;keyStoreAuthentication=KeyVaultClientSecret;keyStoreSecret=secret",
      "jdbc:sqlserver://server; keyStoreSecret = {p;a}}ss};encrypt=true",
      "jdbc:sqlserver://server;unknownCredential=secret",
      "jdbc:sqlserver://server;password={not-closed",
      "jdbc:sqlserver://server;password=secret;invalid"};
  for (const char *url : urls) {
    SCOPED_TRACE(url);
    expect_masked_url(url, "jdbc:sqlserver:********");
  }
  expect_masked_url("JDBC:SQLSERVER://server;password=secret", "JDBC:SQLSERVER:********");
  expect_masked_url("jdbc:postgresql://server/db?password=secret", "jdbc:postgresql:********");
  expect_masked_url("jdbc:oracle:thin:user/secret@server:1521/service", "jdbc:oracle:********");
  expect_masked_url("jdbc:sqlserver;keyStoreSecret=secret:x", "********");
  expect_masked_url("jdbc:sqlserver", "********");
  expect_masked_url("jdbc::secret", "********");
}

TEST_F(TestJdbcCatalog, mask_rejects_empty_url)
{
  common::ObString masked_url("previous value");
  EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcCatalogProperties::mask_url_for_show(
      common::ObString(), allocator_, masked_url));
  EXPECT_TRUE(masked_url.empty());
}

TEST_F(TestJdbcCatalog, parse_mysql_v1_metadata_with_source_collation)
{
  const std::string json = mysql_metadata_json();
  share::ObJdbcTableMetadataDesc metadata;
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(json.length(), json.data()), allocator_, metadata));
  EXPECT_EQ(1, metadata.contract_version_);
  EXPECT_EQ(common::MYSQL_MODE, metadata.target_mode_);
  ASSERT_EQ(1, metadata.columns_.count());
  const share::ObJdbcColumnMetadata &column = metadata.columns_.at(0);
  EXPECT_EQ(0, column.name_.compare("display_name"));
  EXPECT_EQ(0, column.source_charset_.compare("utf8mb4"));
  EXPECT_EQ(0, column.source_collation_.compare("utf8mb4_bin"));
  EXPECT_EQ(0, metadata.qualified_table_name_.compare(
                   "[SalesDB].[sales].[customers]"));
}

TEST_F(TestJdbcCatalog, parse_json_string_escapes)
{
  std::string json = mysql_metadata_json();
  replace_once(json, "\"name\":\"display_name\"",
               "\"name\":\"display_\\\"name\"");
  replace_once(json, R"("qualifiedTableName":"[SalesDB].[sales].[customers]")",
               R"("qualifiedTableName":"[SalesDB].[sales].[客户\"\\]]名😀]")");
  share::ObJdbcTableMetadataDesc metadata;
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(json.length(), json.data()), allocator_, metadata));
  ASSERT_EQ(1, metadata.columns_.count());
  EXPECT_EQ(0, metadata.columns_.at(0).name_.compare("display_\"name"));
  EXPECT_EQ(0, metadata.qualified_table_name_.compare(R"([SalesDB].[sales].[客户"\]]名😀])"));
}

#ifndef OB_USE_ASAN
// ASAN uses malloc directly and does not enforce OceanBase tenant limits.
TEST_F(TestJdbcCatalog, json_temporary_allocations_respect_tenant_memory_limit)
{
  using namespace common;
  // Expected allocation failures raise the Observer memory-dump signal; this
  // unit executable has no Observer signal-handling thread.
  const auto previous_handler = std::signal(49, SIG_IGN);
  ASSERT_NE(SIG_ERR, previous_handler);
  DEFER(std::signal(49, previous_handler));
  const std::string json = mysql_metadata_json();
  const ObString json_text(json.length(), json.data());
  share::ObJdbcTableMetadataDesc metadata;
  ObString parameters;
  ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(json_text, allocator_, metadata));
  ASSERT_EQ(OB_SUCCESS, metadata.build_scan_parameters(allocator_, parameters, 500, 123));

  lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
  ASSERT_TRUE(OB_NOT_NULL(malloc_allocator));
  constexpr uint64_t tenant_id = 1002;
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
  DEFER(malloc_allocator->recycle_tenant_allocator(tenant_id));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(tenant_id, 0));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_max_min(tenant_id, 0, 0));
  share::ObTenantBase tenant(tenant_id);
  share::ObTenantBase *previous_tenant = share::ObTenantEnv::get_tenant();
  share::ObTenantEnv::set_tenant(&tenant);
  DEFER(share::ObTenantEnv::set_tenant(previous_tenant));

  // Output allocations still use allocator_; only the temporary JSON arena is
  // constrained by the current tenant's limit.
  share::ObJdbcTableMetadataDesc parsed;
  ObString built_parameters;
  uint64_t catalog_id = OB_INVALID_ID;
  int64_t catalog_version = 0;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            share::ObJdbcTableMetadataParser::parse(json_text, allocator_, parsed));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            metadata.build_scan_parameters(allocator_, built_parameters, 500, 123));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            share::ObJdbcTableMetadataParser::parse_scan_parameters(parameters, catalog_id, catalog_version));

  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(tenant_id, 128L * 1024 * 1024));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_max_min(tenant_id, 128L * 1024 * 1024, 0));
  ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(json_text, allocator_, parsed));
  ASSERT_EQ(OB_SUCCESS, parsed.build_scan_parameters(allocator_, built_parameters, 500, 123));
  ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse_scan_parameters(
      built_parameters, catalog_id, catalog_version));
  EXPECT_EQ(500, catalog_id);
  EXPECT_EQ(123, catalog_version);
}
#endif

TEST_F(TestJdbcCatalog, build_scan_parameters_preserves_table_name_without_connection_properties)
{
  const common::ObString qualified_table_name(R"([SalesDB].[sales].[客户"\]]名😀])");
  share::ObJdbcTableMetadataDesc metadata;
  metadata.qualified_table_name_ = qualified_table_name;
  metadata.target_mode_ = common::ORACLE_MODE;
  common::ObString parameters;
  ASSERT_EQ(OB_SUCCESS, metadata.build_scan_parameters(allocator_, parameters, 501, 123456));
  uint64_t catalog_id = 0;
  int64_t catalog_version = 0;
  ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse_scan_parameters(
      parameters, catalog_id, catalog_version));
  EXPECT_EQ(501, catalog_id);
  EXPECT_EQ(123456, catalog_version);

  common::ObIJsonBase *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, common::ObJsonBaseFactory::get_json_base(
      &allocator_, parameters, common::ObJsonInType::JSON_TREE,
      common::ObJsonInType::JSON_TREE, root));
  ASSERT_TRUE(OB_NOT_NULL(root));
  if (OB_NOT_NULL(root)) {
    // Scan conversion needs the target mode, but no connection credentials.
    ASSERT_EQ(4, root->element_count());
    common::ObIJsonBase *value = nullptr;
    ASSERT_EQ(OB_SUCCESS, root->get_object_value(common::ObString("target_mode"), value));
    ASSERT_TRUE(OB_NOT_NULL(value));
    if (OB_NOT_NULL(value)) {
      EXPECT_EQ(common::ObString("ORACLE"), common::ObString(value->get_data_length(), value->get_data()));
    }
    ASSERT_EQ(OB_SUCCESS, root->get_object_value(common::ObString("table"), value));
    ASSERT_TRUE(OB_NOT_NULL(value));
    if (OB_NOT_NULL(value)) {
      ASSERT_EQ(common::ObJsonNodeType::J_STRING, value->json_type());
      EXPECT_EQ(qualified_table_name, common::ObString(value->get_data_length(), value->get_data()));
    }
  }
}

TEST_F(TestJdbcCatalog, reject_missing_scan_location)
{
  share::ObJdbcTableMetadataDesc metadata;
  common::ObString parameters;
  metadata.qualified_table_name_ = common::ObString("[SalesDB].[sales].[customers]");
  EXPECT_EQ(OB_SUCCESS, metadata.build_scan_parameters(allocator_, parameters, 501, 123456));
  EXPECT_EQ(OB_INVALID_ARGUMENT, metadata.build_scan_parameters(allocator_, parameters, OB_INVALID_ID, 123456));
  EXPECT_EQ(OB_INVALID_ARGUMENT, metadata.build_scan_parameters(allocator_, parameters, 501, 0));
  metadata.qualified_table_name_.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, metadata.build_scan_parameters(allocator_, parameters, 501, 123456));
}

TEST_F(TestJdbcCatalog, reject_missing_or_invalid_catalog_pool_identity)
{
  const char *invalid[] = {
      "", "[]", "null", R"({"table":"t"})",
      R"({"catalog_id":501,"catalog_schema_version":0})",
      R"({"catalog_id":-1,"catalog_schema_version":123})",
      R"({"catalog_id":501,"catalog_schema_version":"123"})",
      R"({"catalog_id":501,"catalog_id":502,"catalog_schema_version":123})"
  };
  for (const char *json : invalid) {
    uint64_t catalog_id = 0;
    int64_t version = 0;
    EXPECT_NE(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse_scan_parameters(
        common::ObString(json), catalog_id, version));
  }
}

TEST_F(TestJdbcCatalog, parse_oracle_v1_metadata)
{
  const std::string json = oracle_metadata_json();
  share::ObJdbcTableMetadataDesc metadata;
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(json.length(), json.data()), allocator_, metadata));
  EXPECT_EQ(common::ORACLE_MODE, metadata.target_mode_);
  ASSERT_EQ(2, metadata.columns_.count());
  EXPECT_EQ(0, metadata.columns_.at(0).target_type_.name_.compare("NUMBER"));
  EXPECT_EQ(10, metadata.columns_.at(0).target_type_.precision_);
  EXPECT_EQ(0, metadata.columns_.at(0).target_type_.scale_);
  EXPECT_EQ(0, metadata.columns_.at(1).target_type_.name_.compare(
                   "INTERVAL_DAY_TO_SECOND"));
  EXPECT_EQ(1, metadata.columns_.at(1).target_type_.precision_);
  EXPECT_EQ(7, metadata.columns_.at(1).target_type_.scale_);
}

TEST_F(TestJdbcCatalog, reject_incompatible_v1_metadata)
{
  std::string missing_qualified_table_name = mysql_metadata_json();
  replace_once(missing_qualified_table_name, "\"qualifiedTableName\"", "\"tableName\"");
  share::ObJdbcTableMetadataDesc missing_table_name_metadata;
  EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
      common::ObString(missing_qualified_table_name.length(), missing_qualified_table_name.data()),
      allocator_, missing_table_name_metadata));

  share::ObJdbcTableMetadataDesc metadata;

  std::string invalid_version = mysql_metadata_json();
  replace_once(invalid_version, "\"contractVersion\":1", "\"contractVersion\":2");
  EXPECT_EQ(OB_NOT_SUPPORTED,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(invalid_version.length(), invalid_version.data()),
                allocator_,
                metadata));

  std::string invalid_mode = mysql_metadata_json();
  replace_once(invalid_mode, "\"targetMode\":\"MYSQL\"",
                "\"targetMode\":\"POSTGRESQL\"");
  EXPECT_EQ(OB_NOT_SUPPORTED,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(invalid_mode.length(), invalid_mode.data()),
                allocator_,
                metadata));

  std::string invalid_ordinal = mysql_metadata_json();
  replace_once(invalid_ordinal, "\"ordinalPosition\":1",
                "\"ordinalPosition\":0");
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(invalid_ordinal.length(), invalid_ordinal.data()),
                allocator_,
                metadata));

  std::string invalid_source_collation = mysql_metadata_json();
  replace_once(invalid_source_collation, "\"sourceCollation\":\"utf8mb4_bin\"",
                "\"sourceCollation\":123");
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(invalid_source_collation.length(),
                                 invalid_source_collation.data()),
                allocator_,
                metadata));

  std::string duplicate_contract_version = mysql_metadata_json();
  replace_once(duplicate_contract_version,
               "\"contractVersion\":1,",
               "\"contractVersion\":1,\"contractVersion\":1,");
  EXPECT_NE(OB_SUCCESS,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(duplicate_contract_version.length(),
                                 duplicate_contract_version.data()),
                allocator_,
                metadata));

  std::string invalid_interval_precision = oracle_metadata_json();
  replace_once(invalid_interval_precision,
               "\"precision\":1,\n        \"scale\":7",
               "\"precision\":10,\n        \"scale\":7");
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(invalid_interval_precision.length(),
                                 invalid_interval_precision.data()),
                allocator_,
                metadata));

  std::string invalid_interval_scale = oracle_metadata_json();
  replace_once(invalid_interval_scale,
               "\"precision\":1,\n        \"scale\":7",
               "\"precision\":1,\n        \"scale\":10");
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(invalid_interval_scale.length(),
                                 invalid_interval_scale.data()),
                allocator_,
                metadata));
}

TEST_F(TestJdbcCatalog, reject_malformed_columns_and_required_fields)
{
  const struct { const char *from_; const char *to_; } invalid[] = {
      {"\"name\":\"clock_value\"", "\"name\":\"order_id\""},
      {"\"ordinalPosition\":2", "\"ordinalPosition\":1"},
      {"\"ordinalPosition\":1", "\"ordinalPosition\":3"},
      {"\"ordinalPosition\":2", "\"ordinalPosition\":2147483648"},
      {"\"name\":\"order_id\",", ""},
      {"\"ordinalPosition\":1,", ""},
      {"\"targetType\":{", "\"wrongTargetType\":{"},
      {"\"name\":\"order_id\"", "\"name\":null"},
      {"\"ordinalPosition\":1", "\"ordinalPosition\":\"1\""},
      {"\"contractVersion\":1,", ""},
      {"\"qualifiedTableName\":\"[SalesDB].[dbo].[orders]\",", ""},
      {"\"qualifiedTableName\":\"[SalesDB].[dbo].[orders]\"", "\"qualifiedTableName\":null"},
      {"\"qualifiedTableName\":\"[SalesDB].[dbo].[orders]\"", "\"qualifiedTableName\":\"\""},
      {"\"qualifiedTableName\":\"[SalesDB].[dbo].[orders]\"", "\"qualifiedTableName\":123"},
      {"\"targetMode\":\"ORACLE\",", ""}};
  for (const auto &test : invalid) {
    SCOPED_TRACE(test.from_);
    std::string json = oracle_metadata_json();
    replace_once(json, test.from_, test.to_);
    share::ObJdbcTableMetadataDesc metadata;
    EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
  }
  for (const char *columns : {"[]", "null", "{}", "[null]", "[1]"}) {
    std::string json = mysql_metadata_json();
    const size_t begin = json.find('[', json.find("\"columns\""));
    json.replace(begin, json.rfind(']') - begin + 1, columns);
    share::ObJdbcTableMetadataDesc metadata;
    EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
  }
  for (const std::string &json : {std::string("[]"), std::string("null"), std::string("7"),
                                mysql_metadata_json().substr(0, 80)}) {
    share::ObJdbcTableMetadataDesc metadata;
    EXPECT_NE(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
  }
}

TEST_F(TestJdbcCatalog, metadata_size_limit_accepts_exact_boundary)
{
  std::string json = mysql_metadata_json();
  json.resize(share::MAX_METADATA_JSON_SIZE, ' ');
  share::ObJdbcTableMetadataDesc metadata;
  ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
      common::ObString(json.length(), json.data()), allocator_, metadata));
  ASSERT_EQ(1, metadata.columns_.count());
  json.push_back(' ');
  EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
      common::ObString(json.length(), json.data()), allocator_, metadata));
}

TEST_F(TestJdbcCatalog, catalog_requires_initialization_and_does_not_offer_cached_versions)
{
  share::schema::ObSchemaGetterGuard guard;
  share::ObJdbcCatalog catalog(allocator_, guard, 123456);
  common::ObSEArray<common::ObString, 4> names;
  const common::ObString ns("dbo");
  const common::ObString table("Orders");
  const auto case_mode = common::OB_ORIGIN_AND_INSENSITIVE;
  EXPECT_EQ(OB_NOT_INIT, catalog.list_namespace_names(names));
  EXPECT_EQ(OB_NOT_INIT, catalog.list_table_names(ns, case_mode, names));
  share::schema::ObDatabaseSchema *schema = nullptr;
  EXPECT_EQ(OB_NOT_INIT, catalog.fetch_namespace_schema(1, ns, case_mode, schema));
  share::ObILakeTableMetadata *metadata = nullptr;
  EXPECT_EQ(OB_NOT_INIT, catalog.fetch_lake_table_metadata(allocator_, 1, 2, ns, table, case_mode, metadata));
  int64_t version = -1;
  EXPECT_EQ(OB_NOT_SUPPORTED, catalog.fetch_latest_table_schema_version(ns, table, case_mode, version));
}

TEST_F(TestJdbcCatalog, materialize_mysql_target_type_and_collation)
{
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  const std::string json = mysql_metadata_json();
  share::ObJdbcTableMetadataDesc metadata;
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(json.length(), json.data()), allocator_, metadata));

  sql::jdbc::ObJdbcTableMetadata table_metadata(allocator_);
  ASSERT_EQ(OB_SUCCESS,
            table_metadata.init(OB_SYS_TENANT_ID,
                                OB_MIN_USER_OBJECT_ID,
                                OB_MIN_USER_OBJECT_ID + 1,
                                OB_MIN_USER_OBJECT_ID + 2,
                                common::ObString("SalesDB"),
                                common::ObString("customers"),
                                common::OB_ORIGIN_AND_INSENSITIVE));
  ASSERT_EQ(OB_SUCCESS,
            // Empty scan parameters avoid the observer's TDE root-key service.
            table_metadata.setup_table_schema(common::ObString("sqlserver"), common::ObString(), metadata, charset_context()));
  share::schema::ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_metadata.build_table_schema(table_schema));
  ASSERT_NE(nullptr, table_schema);
  EXPECT_EQ(0, table_schema->get_schema_version());
  const share::schema::ObColumnSchemaV2 *column =
      table_schema->get_column_schema("display_name");
  ASSERT_NE(nullptr, column);
  EXPECT_EQ(common::ObVarcharType, column->get_data_type());
  EXPECT_EQ(common::CHARSET_UTF8MB4, column->get_charset_type());
  EXPECT_EQ(common::CS_TYPE_UTF8MB4_BIN, column->get_collation_type());
  EXPECT_EQ(64, column->get_data_length());
}

TEST_F(TestJdbcCatalog, pushdown_column_uses_remote_name)
{
  const char *remote_names[] = {"lowercase_id", "MiXeD.id", "amount]net", "显示名"};
  const char *logical_names[] = {"LOWERCASE_ID", "MIXED.ID", "AMOUNT]NET", "显示名"};
  for (int64_t i = 0; i < ARRAYSIZEOF(remote_names); ++i) {
    sql::ObColumnRefRawExpr column_expr;
    column_expr.set_column_name(common::ObString(logical_names[i]));
    sql::ObPseudoColumnRawExpr file_column_expr;
    file_column_expr.set_expr_type(T_PSEUDO_EXTERNAL_FILE_COL);
    file_column_expr.set_data_access_path(common::ObString(remote_names[i]));
    column_expr.set_dependant_expr(&file_column_expr);

    plugin::external::ObArrowExprBuilder builder;
    std::shared_ptr<arrow::RecordBatch> arrow_expr;
    ASSERT_EQ(OB_SUCCESS, builder.build_column_ref_expr(column_expr, arrow_expr));
    ASSERT_TRUE(OB_NOT_NULL(arrow_expr.get()));
    ASSERT_EQ(1, arrow_expr->num_columns());
    const std::shared_ptr<arrow::Array> values = arrow_expr->column(0);
    ASSERT_TRUE(OB_NOT_NULL(values.get()));
    ASSERT_EQ(arrow::Type::STRING, values->type_id());
    const auto &names = static_cast<const arrow::StringArray &>(*values);
    ASSERT_EQ(1, names.length());
    EXPECT_EQ(remote_names[i], names.GetString(0));
    EXPECT_EQ(0, column_expr.get_column_name().compare(logical_names[i]));
  }
}

TEST_F(TestJdbcCatalog, pushdown_column_without_source_name_uses_logical_name)
{
  sql::ObColumnRefRawExpr column_expr;
  column_expr.set_column_name(common::ObString("column_name"));
  sql::ObPseudoColumnRawExpr file_column_expr;
  file_column_expr.set_expr_type(T_PSEUDO_EXTERNAL_FILE_COL);
  for (int64_t i = 0; i < 2; ++i) {
    column_expr.set_dependant_expr(i == 0 ? nullptr : &file_column_expr);
    plugin::external::ObArrowExprBuilder builder;
    std::shared_ptr<arrow::RecordBatch> arrow_expr;
    ASSERT_EQ(OB_SUCCESS, builder.build_column_ref_expr(column_expr, arrow_expr));
    ASSERT_TRUE(OB_NOT_NULL(arrow_expr.get()));
    ASSERT_EQ(1, arrow_expr->num_columns());
    const std::shared_ptr<arrow::Array> values = arrow_expr->column(0);
    ASSERT_TRUE(OB_NOT_NULL(values.get()));
    ASSERT_EQ(arrow::Type::STRING, values->type_id());
    const auto &names = static_cast<const arrow::StringArray &>(*values);
    ASSERT_EQ(1, names.length());
    EXPECT_EQ("column_name", names.GetString(0));
  }
}

TEST_F(TestJdbcCatalog, mysql_charset_mapping_requires_a_supported_pair)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  struct MappingCase {
    const char *charset_;
    const char *collation_;
    ObCollationType expected_;
  };
  const MappingCase cases[] = {
      {"utf8mb4", "utf8mb4_bin", CS_TYPE_UTF8MB4_BIN},
      {"UTF8MB4", "UTF8MB4_GENERAL_CI", CS_TYPE_UTF8MB4_GENERAL_CI},
      {"gbk", "gbk_bin", CS_TYPE_GBK_BIN},
      {"utf8mb4", "SQL_Latin1_General_CP1_CI_AS", CS_TYPE_LATIN1_SWEDISH_CI},
      {"utf8mb4", "Latin1_General_100_BIN2_UTF8", CS_TYPE_LATIN1_SWEDISH_CI},
      {"unknown", "utf8mb4_bin", CS_TYPE_LATIN1_SWEDISH_CI},
      {"gbk", "utf8mb4_bin", CS_TYPE_LATIN1_SWEDISH_CI},
      {"", "utf8mb4_bin", CS_TYPE_LATIN1_SWEDISH_CI},
      {"utf8mb4", "", CS_TYPE_LATIN1_SWEDISH_CI},
      {"", "", CS_TYPE_LATIN1_SWEDISH_CI}};
  for (const MappingCase &test : cases) {
    for (const char *target : {"VARCHAR", "LONGTEXT"}) {
      const std::string json = mysql_metadata_json();
      share::ObJdbcTableMetadataDesc metadata;
      ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
          ObString(json.length(), json.data()), allocator_, metadata));
      auto &column = metadata.columns_.at(0);
      column.source_charset_ = ObString(test.charset_);
      column.source_collation_ = ObString(test.collation_);
      column.target_type_.name_ = ObString(target);
      if (column.target_type_.name_ == "LONGTEXT") {
        column.target_type_.length_ = -1;
        column.target_type_.length_semantics_.reset();
      }
      share::schema::ObTableSchema *schema = nullptr;
      ASSERT_EQ(OB_SUCCESS, build_schema(metadata, charset_context(CS_TYPE_LATIN1_SWEDISH_CI), schema));
      ASSERT_TRUE(OB_NOT_NULL(schema));
      if (OB_NOT_NULL(schema)) {
        EXPECT_EQ(CS_TYPE_LATIN1_SWEDISH_CI, schema->get_collation_type());
        const auto *actual = schema->get_column_schema("display_name");
        ASSERT_TRUE(OB_NOT_NULL(actual));
        if (OB_NOT_NULL(actual)) {
          EXPECT_EQ(test.expected_, actual->get_collation_type()) << test.charset_ << "/" << test.collation_;
          EXPECT_EQ(ObCharset::charset_type_by_coll(test.expected_), actual->get_charset_type());
        }
      }
    }
  }
}

TEST_F(TestJdbcCatalog, oracle_uses_nls_defaults_and_binary_types_remain_binary)
{
  using namespace common;
  struct TargetCase { const char *name_; bool oracle_; ObCollationType expected_; };
  const TargetCase cases[] = {
      {"CHAR", true, CS_TYPE_GBK_BIN}, {"VARCHAR2", true, CS_TYPE_GBK_BIN},
      {"CLOB", true, CS_TYPE_GBK_BIN}, {"NCHAR", true, CS_TYPE_UTF8MB4_BIN},
      {"NVARCHAR2", true, CS_TYPE_UTF8MB4_BIN}, {"RAW", true, CS_TYPE_BINARY},
      {"BLOB", true, CS_TYPE_BINARY}, {"BINARY", false, CS_TYPE_BINARY},
      {"VARBINARY", false, CS_TYPE_BINARY}, {"LONGBLOB", false, CS_TYPE_BINARY}};
  for (const TargetCase &test : cases) {
    lib::CompatModeGuard mode_guard(test.oracle_ ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    const std::string json = mysql_metadata_json();
    share::ObJdbcTableMetadataDesc metadata;
    ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
        ObString(json.length(), json.data()), allocator_, metadata));
    metadata.target_mode_ = test.oracle_ ? ORACLE_MODE : MYSQL_MODE;
    auto &target = metadata.columns_.at(0).target_type_;
    target.name_ = ObString(test.name_);
    if (target.name_ == "CLOB" || target.name_ == "BLOB" || target.name_ == "LONGBLOB") {
      target.length_ = -1;
      target.length_semantics_.reset();
    } else if (CS_TYPE_BINARY == test.expected_) {
      target.length_semantics_ = ObString("BYTE");
    }
    share::schema::ObTableSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_schema(metadata, charset_context(CS_TYPE_GBK_BIN, CS_TYPE_UTF8MB4_BIN), schema));
    ASSERT_TRUE(OB_NOT_NULL(schema));
    if (OB_NOT_NULL(schema)) {
      const auto *actual = schema->get_column_schema(test.oracle_ ? "DISPLAY_NAME" : "display_name");
      ASSERT_TRUE(OB_NOT_NULL(actual));
      if (OB_NOT_NULL(actual)) {
        EXPECT_EQ(test.expected_, actual->get_collation_type()) << test.name_;
        EXPECT_EQ(ObCharset::charset_type_by_coll(test.expected_), actual->get_charset_type());
      }
    }
  }
}

TEST_F(TestJdbcCatalog, character_length_limits_follow_the_resolved_charset)
{
  using namespace common;
  struct LengthCase { const char *type_; bool oracle_; ObCollationType collation_; int64_t max_length_; };
  const LengthCase cases[] = {
      {"VARCHAR", false, CS_TYPE_UTF8MB4_BIN, OB_MAX_MYSQL_VARCHAR_LENGTH / 4},
      {"VARCHAR", false, CS_TYPE_LATIN1_SWEDISH_CI, OB_MAX_MYSQL_VARCHAR_LENGTH},
      {"NCHAR", true, CS_TYPE_UTF16_BIN, OB_MAX_ORACLE_CHAR_LENGTH_BYTE / 2},
      {"NCHAR", true, CS_TYPE_UTF8MB4_BIN, OB_MAX_ORACLE_CHAR_LENGTH_BYTE}};
  for (const LengthCase &test : cases) {
    lib::CompatModeGuard mode_guard(test.oracle_ ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    const std::string json = mysql_metadata_json();
    share::ObJdbcTableMetadataDesc metadata;
    ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
        ObString(json.length(), json.data()), allocator_, metadata));
    metadata.target_mode_ = test.oracle_ ? ORACLE_MODE : MYSQL_MODE;
    auto &column = metadata.columns_.at(0);
    column.source_charset_.reset();
    column.source_collation_.reset();
    column.target_type_.name_ = ObString(test.type_);
    const auto context = charset_context(test.collation_, test.collation_);
    share::schema::ObTableSchema *schema = nullptr;
    column.target_type_.length_ = test.max_length_;
    EXPECT_EQ(OB_SUCCESS, build_schema(metadata, context, schema));
    column.target_type_.length_ = test.max_length_ + 1;
    ASSERT_EQ(OB_SUCCESS, build_schema(metadata, context, schema));
    const auto *unavailable = schema->get_column_schema(OB_END_RESERVED_COLUMN_ID_NUM);
    ASSERT_NE(nullptr, unavailable);
    EXPECT_EQ(share::ObLakeTableFormat::JDBC, schema->get_lake_table_format());
    EXPECT_EQ(ObNullType, unavailable->get_data_type());
    EXPECT_FALSE(unavailable->is_generated_column());
  }
}

TEST_F(TestJdbcCatalog, unsupported_columns_preserve_order_and_survive_metadata_copy)
{
  using namespace common;
  for (const bool oracle : {false, true}) {
    lib::CompatModeGuard mode_guard(oracle ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    sql::jdbc::ObJdbcTableMetadata copied(allocator_);
    {
      ObArenaAllocator source_allocator(ObMemAttr(OB_SYS_TENANT_ID, "JdbcMetaTest"));
      std::string json = unsupported_metadata_json();
      if (oracle) {
        replace_once(json, "\"targetMode\":\"MYSQL\"", "\"targetMode\":\"ORACLE\"");
        replace_once(json, "\"name\":\"INT\"", "\"name\":\"NUMBER\",\"precision\":10,\"scale\":0");
        replace_once(json, "\"name\":\"INT\"", "\"name\":\"NUMBER\",\"precision\":10,\"scale\":0");
      }
      share::ObJdbcTableMetadataDesc metadata;
      ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
          ObString(json.length(), json.data()), source_allocator, metadata));
      ASSERT_EQ(3, metadata.columns_.count());
      EXPECT_EQ(3, metadata.columns_.at(1).ordinal_position_);
      sql::jdbc::ObJdbcTableMetadata original(source_allocator);
      ASSERT_EQ(OB_SUCCESS, original.init(OB_SYS_TENANT_ID, OB_MIN_USER_OBJECT_ID,
          OB_MIN_USER_OBJECT_ID + 1, OB_MIN_EXTERNAL_OBJECT_ID + 10,
          ObString("dbo"), ObString("mixed"), OB_ORIGIN_AND_INSENSITIVE));
      ASSERT_EQ(OB_SUCCESS, original.setup_table_schema(
          ObString("sqlserver"), ObString(), metadata, charset_context()));
      ASSERT_EQ(OB_SUCCESS, copied.assign(original));
      EXPECT_EQ(original.get_convert_size(), copied.get_convert_size());
    }
    // The unsupported placeholder survives the source allocator and mock table ID changes.
    copied.table_id_ = OB_MIN_EXTERNAL_OBJECT_ID + 20;
    share::schema::ObTableSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, copied.build_table_schema(schema));
    ASSERT_NE(nullptr, schema);
    EXPECT_EQ(copied.table_id_, schema->get_table_id());
    const auto *id = schema->get_column_schema(OB_END_RESERVED_COLUMN_ID_NUM);
    const auto *v = schema->get_column_schema(OB_END_RESERVED_COLUMN_ID_NUM + 1);
    const auto *tail = schema->get_column_schema(OB_END_RESERVED_COLUMN_ID_NUM + 2);
    ASSERT_NE(nullptr, id);
    ASSERT_NE(nullptr, v);
    ASSERT_NE(nullptr, tail);
    EXPECT_EQ(share::ObLakeTableFormat::JDBC, schema->get_lake_table_format());
    EXPECT_NE(ObNullType, id->get_data_type());
    EXPECT_TRUE(id->is_generated_column());
    EXPECT_TRUE(v->is_user_visible_column());
    EXPECT_EQ(ObNullType, v->get_data_type());
    EXPECT_FALSE(v->is_generated_column());
    EXPECT_EQ(ObString(oracle ? "V" : "v"), v->get_column_name_str());
    EXPECT_NE(ObNullType, tail->get_data_type());
    EXPECT_TRUE(tail->is_generated_column());
  }
}

TEST_F(TestJdbcCatalog, malformed_unsupported_columns_still_fail_metadata_loading)
{
  const struct { const char *from_; const char *to_; } invalid[] = {
      {"\"name\":\"UNSUPPORTED\"", "\"name\":\"UNSUPPORTED\",\"precision\":1"},
      {"\"name\":\"UNSUPPORTED\"", "\"name\":\"UNSUPPORTED\",\"scale\":0"},
      {"\"name\":\"UNSUPPORTED\"", "\"name\":\"UNSUPPORTED\",\"length\":1"},
      {"\"name\":\"UNSUPPORTED\"", "\"name\":\"UNSUPPORTED\",\"lengthSemantics\":\"CHAR\""},
      {"\"name\":\"v\"", "\"name\":\"id\""}};
  for (const auto &test : invalid) {
    SCOPED_TRACE(test.to_);
    std::string json = unsupported_metadata_json();
    replace_once(json, test.from_, test.to_);
    share::ObJdbcTableMetadataDesc metadata;
    EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
  }
  std::string unknown = unsupported_metadata_json();
  replace_once(unknown, "UNSUPPORTED", "UNKNOWN_TARGET_TYPE");
  share::ObJdbcTableMetadataDesc metadata;
  EXPECT_EQ(OB_NOT_SUPPORTED, share::ObJdbcTableMetadataParser::parse(
      common::ObString(unknown.length(), unknown.data()), allocator_, metadata));
}

TEST_F(TestJdbcCatalog, source_charset_fields_are_optional_but_must_be_strings)
{
  for (const char *key : {"sourceCharset", "sourceCollation"}) {
    for (const char *value : {"null", "123", "true"}) {
      std::string json = mysql_metadata_json();
      const std::string original = std::string("\"") + key + "\":\""
          + (std::string(key) == "sourceCharset" ? "utf8mb4" : "utf8mb4_bin") + "\"";
      replace_once(json, original, std::string("\"") + key + "\":" + value);
      share::ObJdbcTableMetadataDesc metadata;
      EXPECT_EQ(std::string(value) == "null" ? OB_SUCCESS : OB_INVALID_ARGUMENT,
                share::ObJdbcTableMetadataParser::parse(
                    common::ObString(json.length(), json.data()), allocator_, metadata));
    }
  }
}

TEST_F(TestJdbcCatalog, materialize_oracle_target_number)
{
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::ORACLE);
  const std::string json = oracle_metadata_json();
  share::ObJdbcTableMetadataDesc metadata;
  ASSERT_EQ(OB_SUCCESS,
            share::ObJdbcTableMetadataParser::parse(
                common::ObString(json.length(), json.data()), allocator_, metadata));

  sql::jdbc::ObJdbcTableMetadata table_metadata(allocator_);
  ASSERT_EQ(OB_SUCCESS,
            table_metadata.init(OB_SYS_TENANT_ID,
                                OB_MIN_USER_OBJECT_ID,
                                OB_MIN_USER_OBJECT_ID + 1,
                                OB_MIN_USER_OBJECT_ID + 2,
                                common::ObString("SalesDB"),
                                common::ObString("orders"),
                                common::OB_ORIGIN_AND_INSENSITIVE));
  ASSERT_EQ(OB_SUCCESS,
            table_metadata.setup_table_schema(common::ObString("sqlserver"), common::ObString(), metadata, charset_context()));
  share::schema::ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_metadata.build_table_schema(table_schema));
  ASSERT_NE(nullptr, table_schema);
  EXPECT_EQ(0, table_schema->get_schema_version());
  const share::schema::ObColumnSchemaV2 *column =
      table_schema->get_column_schema("ORDER_ID");
  ASSERT_NE(nullptr, column);
  EXPECT_EQ(common::ObDecimalIntType, column->get_data_type());
  EXPECT_EQ(10, column->get_data_precision());
  EXPECT_EQ(0, column->get_data_scale());
  EXPECT_FALSE(column->is_nullable());

  const share::schema::ObColumnSchemaV2 *interval_column =
      table_schema->get_column_schema("CLOCK_VALUE");
  ASSERT_NE(nullptr, interval_column);
  EXPECT_EQ(common::ObIntervalDSType, interval_column->get_data_type());
  EXPECT_EQ(common::ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(1, 7),
            interval_column->get_data_scale());
  EXPECT_TRUE(interval_column->is_nullable());
}

TEST_F(TestJdbcCatalog, interval_precision_boundaries_survive_metadata_and_arrow_scan)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::ORACLE);
  struct SecondPrecisionCase {
    int16_t precision_;
    const char *fraction_;
    int32_t expected_fs_;
  };
  const SecondPrecisionCase second_cases[] = {
      {0, "", 0},
      {7, ".1234567", 123456700},
      {9, ".123456789", 123456789},
  };
  for (const int16_t day : {0, 1, 9}) {
    for (const auto &second : second_cases) {
      SCOPED_TRACE(day);
      SCOPED_TRACE(second.precision_);
      std::string json = oracle_metadata_json();
      replace_once(json, "\"precision\":1,\n        \"scale\":7",
                   "\"precision\":" + std::to_string(day) + ",\n        \"scale\":" + std::to_string(second.precision_));
      share::ObJdbcTableMetadataDesc metadata;
      ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
          ObString(json.length(), json.data()), allocator_, metadata));
      share::schema::ObTableSchema *schema = nullptr;
      ASSERT_EQ(OB_SUCCESS, build_schema(metadata, charset_context(), schema));
      ASSERT_TRUE(OB_NOT_NULL(schema));
      const share::schema::ObColumnSchemaV2 *column = schema->get_column_schema("CLOCK_VALUE");
      ASSERT_TRUE(OB_NOT_NULL(column));
      EXPECT_EQ(ObIntervalDSType, column->get_data_type());
      EXPECT_EQ(day, ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(column->get_data_scale()));
      EXPECT_EQ(second.precision_, ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(column->get_data_scale()));
      const sql::ObDatumMeta interval_meta(column->get_data_type(), CS_TYPE_BINARY, column->get_data_scale());
      auto check_scan = [&](auto &builder) {
        ASSERT_TRUE(builder.Append(std::string("+0 12:34:56") + second.fraction_).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append(std::string("-0 01:02:03") + second.fraction_).ok());
        std::shared_ptr<arrow::Array> values;
        ASSERT_TRUE(builder.Finish(&values).ok());
        expect_arrow_values(values, interval_meta, VEC_TC_INTERVAL_DS, [&](ObIVector &vector) {
          EXPECT_EQ(45296, vector.get_interval_ds(0).get_nsecond());
          EXPECT_EQ(second.expected_fs_, vector.get_interval_ds(0).get_fs());
          EXPECT_EQ(-3723, vector.get_interval_ds(2).get_nsecond());
          EXPECT_EQ(-second.expected_fs_, vector.get_interval_ds(2).get_fs());
        });
      };
      arrow::StringBuilder strings;
      check_scan(strings);
      arrow::LargeStringBuilder large_strings;
      check_scan(large_strings);
    }
  }
}

TEST_F(TestJdbcCatalog, interval_invalid_precision_is_rejected_at_each_boundary)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::ORACLE);
  const int16_t invalid_pairs[][2] = {{-1, 0}, {10, 0}, {0, -1}, {0, 10}};
  for (const auto &pair : invalid_pairs) {
    SCOPED_TRACE(pair[0]);
    SCOPED_TRACE(pair[1]);
    std::string json = oracle_metadata_json();
    replace_once(json, "\"precision\":1,\n        \"scale\":7",
                 "\"precision\":" + std::to_string(pair[0]) + ",\n        \"scale\":" + std::to_string(pair[1]));
    share::ObJdbcTableMetadataDesc metadata;
    EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
        ObString(json.length(), json.data()), allocator_, metadata));
    const std::string valid_json = oracle_metadata_json();
    ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
        ObString(valid_json.length(), valid_json.data()), allocator_, metadata));
    metadata.columns_.at(1).target_type_.precision_ = pair[0];
    metadata.columns_.at(1).target_type_.scale_ = pair[1];
    share::schema::ObTableSchema *schema = nullptr;
    EXPECT_EQ(OB_INVALID_ARGUMENT, build_schema(metadata, charset_context(), schema));
    share::schema::ObColumnSchemaV2 column;
    EXPECT_EQ(OB_INVALID_ARGUMENT, share::schema::ObExternalTableColumnSchemaHelper::setup_interval_ds(
        pair[0], pair[1], column));
  }
}

TEST_F(TestJdbcCatalog, arrow_int64_slices_preserve_values_and_nulls)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  expect_fixed_arrow_slices<arrow::Int64Type>(sql::ObDatumMeta(ObIntType, CS_TYPE_BINARY, -1),
      VEC_TC_INTEGER, [](ObIVector &vector, const int64_t i, const int64_t expected) {
        EXPECT_EQ(expected, vector.get_int(i));
      });
}

TEST_F(TestJdbcCatalog, arrow_double_slices_preserve_values_and_nulls)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  expect_fixed_arrow_slices<arrow::DoubleType>(sql::ObDatumMeta(ObDoubleType, CS_TYPE_BINARY, -1),
      VEC_TC_DOUBLE, [](ObIVector &vector, const int64_t i, const double expected) {
        EXPECT_DOUBLE_EQ(expected, vector.get_double(i));
      });
}

TEST_F(TestJdbcCatalog, sqlserver_oracle_scan_loaders_preserve_values_and_nulls)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::ORACLE);
  std::shared_ptr<arrow::Array> values;
  arrow::Int64Builder integers;
  ASSERT_TRUE(integers.Append(-123456789).ok());
  ASSERT_TRUE(integers.AppendNull().ok());
  ASSERT_TRUE(integers.Append(0).ok());
  ASSERT_TRUE(integers.Finish(&values).ok());
  expect_arrow_values(values, sql::ObDatumMeta(ObDecimalIntType, CS_TYPE_BINARY, 0, 10),
      VEC_TC_DEC_INT64, [](ObIVector &vector) {
        int64_t number = 0;
        ASSERT_TRUE(OB_NOT_NULL(vector.get_payload(0)));
        if (OB_NOT_NULL(vector.get_payload(0))) {
          MEMCPY(&number, vector.get_payload(0), sizeof(number));
          EXPECT_EQ(-123456789, number);
        }
        EXPECT_FALSE(vector.is_null(2));
      });
  arrow::DoubleBuilder floats;
  ASSERT_TRUE(floats.Append(1.25).ok());
  ASSERT_TRUE(floats.AppendNull().ok());
  ASSERT_TRUE(floats.Append(-2.5).ok());
  ASSERT_TRUE(floats.Finish(&values).ok());
  expect_arrow_values(values, sql::ObDatumMeta(ObFloatType, CS_TYPE_BINARY, -1),
      VEC_TC_FLOAT, [](ObIVector &vector) {
        EXPECT_FLOAT_EQ(1.25, vector.get_float(0));
        EXPECT_FLOAT_EQ(-2.5, vector.get_float(2));
      });
  arrow::BinaryBuilder binary;
  ASSERT_TRUE(binary.Append("\x00\xFF", 2).ok());
  ASSERT_TRUE(binary.AppendNull().ok());
  ASSERT_TRUE(binary.Append("").ok());
  ASSERT_TRUE(binary.Finish(&values).ok());
  expect_arrow_values(values, sql::ObDatumMeta(ObRawType, CS_TYPE_BINARY, -1),
      VEC_TC_RAW, [](ObIVector &vector) {
        EXPECT_EQ(ObString(2, "\x00\xFF"), vector.get_string(0));
        EXPECT_TRUE(vector.is_null(2));
      });
  arrow::StringBuilder timestamps;
  ASSERT_TRUE(timestamps.Append("1970-01-01 00:00:01.1234567").ok());
  ASSERT_TRUE(timestamps.AppendNull().ok());
  ASSERT_TRUE(timestamps.Append("1970-01-01 00:00:00").ok());
  ASSERT_TRUE(timestamps.Finish(&values).ok());
  expect_arrow_values(values, sql::ObDatumMeta(ObTimestampNanoType, CS_TYPE_BINARY, 7),
      VEC_TC_TIMESTAMP_TINY, [](ObIVector &vector) {
        EXPECT_EQ(1123456, vector.get_otimestamp_tiny(0).time_us_);
        EXPECT_EQ(700, static_cast<int32_t>(
            vector.get_otimestamp_tiny(0).to_timestamp_data().time_ctx_.tail_nsec_));
        EXPECT_EQ(0, vector.get_otimestamp_tiny(2).time_us_);
      });
  arrow::StringBuilder intervals;
  ASSERT_TRUE(intervals.Append("+0 12:34:56.1234567").ok());
  ASSERT_TRUE(intervals.AppendNull().ok());
  ASSERT_TRUE(intervals.Append("-2 01:02:03.0000001").ok());
  ASSERT_TRUE(intervals.Finish(&values).ok());
  const sql::ObDatumMeta interval_meta(ObIntervalDSType, CS_TYPE_BINARY,
      ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(1, 7));
  expect_arrow_values(values, interval_meta, VEC_TC_INTERVAL_DS, [](ObIVector &vector) {
        EXPECT_EQ(45296, vector.get_interval_ds(0).get_nsecond());
        EXPECT_EQ(123456700, vector.get_interval_ds(0).get_fs());
        EXPECT_EQ(-176523, vector.get_interval_ds(2).get_nsecond());
        EXPECT_EQ(-100, vector.get_interval_ds(2).get_fs());
      });
  arrow::StringBuilder invalid;
  ASSERT_TRUE(invalid.Append("invalid time").ok());
  ASSERT_TRUE(invalid.AppendNull().ok());
  ASSERT_TRUE(invalid.Append("00:00:00").ok());
  ASSERT_TRUE(invalid.Finish(&values).ok());
  expect_arrow_values(values, interval_meta, VEC_TC_INTERVAL_DS, [](ObIVector &) {}, false);
  expect_arrow_values(values, sql::ObDatumMeta(ObTimestampNanoType, CS_TYPE_BINARY, 7),
      VEC_TC_TIMESTAMP_TINY, [](ObIVector &) {}, false);
}

TEST_F(TestJdbcCatalog, mysql_is_filters_preserve_operands_for_plugins)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  ParamStore params;
  ObObj lhs_value;
  lhs_value.set_int(1);
  sql::ObConstRawExpr lhs(lhs_value, T_INT);
  for (ObItemType type : {T_OP_IS, T_OP_IS_NOT}) {
    for (int value : {-1, 0, 1}) {
      ObObj rhs_value;
      if (value < 0) {
        rhs_value.set_null();
      } else {
        rhs_value.set_bool(value != 0);
      }
      sql::ObConstRawExpr rhs(rhs_value, value < 0 ? T_NULL : T_BOOL);
      sql::ObOpRawExpr predicate(allocator_);
      predicate.set_expr_type(type);
      ASSERT_EQ(OB_SUCCESS, predicate.set_param_exprs(&lhs, &rhs));
      plugin::external::ObArrowFilterBuilder builder(params);
      std::shared_ptr<arrow::RecordBatch> filter;
      ASSERT_EQ(OB_SUCCESS, builder.build_filter(&predicate, filter));
      ASSERT_TRUE(OB_NOT_NULL(filter.get()));
      ASSERT_EQ(3, filter->num_columns());
      const auto rhs_column = filter->column(1);
      ASSERT_TRUE(OB_NOT_NULL(rhs_column.get()));
      EXPECT_EQ(value < 0, rhs_column->IsNull(0));
      if (value >= 0) {
        const auto scalar_result = rhs_column->GetScalar(0);
        ASSERT_TRUE(scalar_result.ok());
        const auto scalar = *scalar_result;
        ASSERT_TRUE(OB_NOT_NULL(scalar.get()));
        EXPECT_EQ(std::to_string(value), scalar->ToString());
      }
    }
  }
}

TEST_F(TestJdbcCatalog, arrow_exports_release_on_failure_before_import)
{
  for (bool consume_schema : {false, true}) {
    std::weak_ptr<arrow::Buffer> exported_buffer;
    ArrowArray array = {};
    ArrowSchema schema = {};
    {
      plugin::external::ObArrowCDataGuard guard(array, schema);
      arrow::Int64Builder builder;
      ASSERT_TRUE(builder.Append(42).ok());
      std::shared_ptr<arrow::Array> values;
      ASSERT_TRUE(builder.Finish(&values).ok());
      ASSERT_TRUE(OB_NOT_NULL(values.get()));
      ASSERT_TRUE(OB_NOT_NULL(values->data().get()));
      exported_buffer = values->data()->buffers[1];
      auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("v", arrow::int64())}), 1, {values});
      ASSERT_TRUE(OB_NOT_NULL(batch.get()));
      ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &array, &schema).ok());
      values.reset();
      batch.reset();
      ASSERT_FALSE(exported_buffer.expired());
      if (consume_schema && OB_NOT_NULL(schema.release)) {
        schema.release(&schema);
      }
      // Simulate get_jni_env/import failure after export, with no Java owner.
    }
    EXPECT_TRUE(exported_buffer.expired());
    EXPECT_EQ(nullptr, array.release);
    EXPECT_EQ(nullptr, schema.release);
  }
}

TEST_F(TestJdbcCatalog, arrow_export_guard_does_not_release_imported_batch)
{
  std::shared_ptr<arrow::RecordBatch> imported;
  ArrowArray array = {};
  ArrowSchema schema = {};
  {
    plugin::external::ObArrowCDataGuard guard(array, schema);
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(42).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(builder.Finish(&values).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("v", arrow::int64())}), 1, {values});
    ASSERT_TRUE(OB_NOT_NULL(batch.get()));
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &array, &schema).ok());
    auto result = arrow::ImportRecordBatch(&array, &schema);
    ASSERT_TRUE(result.ok());
    imported = *result;
    EXPECT_EQ(nullptr, array.release);
    EXPECT_EQ(nullptr, schema.release);
  }
  ASSERT_TRUE(OB_NOT_NULL(imported.get()));
  if (OB_NOT_NULL(imported.get())) {
    const auto values = std::static_pointer_cast<arrow::Int64Array>(imported->column(0));
    ASSERT_TRUE(OB_NOT_NULL(values.get()));
    if (OB_NOT_NULL(values.get())) {
      EXPECT_EQ(42, values->Value(0));
    }
  }
}

TEST_F(TestJdbcCatalog, arrow_unrepresentable_charset_obeys_cast_mode)
{
  using namespace common;
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  arrow::StringBuilder builder;
  ASSERT_TRUE(builder.Append("😀").ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append("").ok());
  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(builder.Finish(&values).ok());
  const sql::ObDatumMeta meta(ObVarcharType, CS_TYPE_GBK_BIN, -1);
  expect_arrow_values(values, meta, VEC_TC_STRING, [](ObIVector &vector) {
      EXPECT_EQ(ObString("?"), vector.get_string(0));
      EXPECT_EQ(0, vector.get_string(2).length());
    }, true, CM_CHARSET_CONVERT_IGNORE_ERR);
  expect_arrow_values(values, meta, VEC_TC_STRING, [](ObIVector &) {}, false, CM_NONE);
}

TEST_F(TestJdbcCatalog, arrow_strings_are_converted_to_the_column_charset)
{
  using namespace common;
  using plugin::external::ObArrowDataLoader;
  struct ConversionCase {
    ObObjType type_;
    ObCollationType collation_;
    bool oracle_;
    const char *expected_;
  };
  const ConversionCase cases[] = {
      {ObVarcharType, CS_TYPE_GBK_BIN, false, "\xD6\xD0\xB9\xFA"},
      {ObNVarchar2Type, CS_TYPE_UTF16_BIN, true, "\x4E\x2D\x56\xFD"},
      {ObLongTextType, CS_TYPE_GBK_BIN, false, "\xD6\xD0\xB9\xFA"},
      {ObLongTextType, CS_TYPE_GBK_BIN, true, "\xD6\xD0\xB9\xFA"}};
  arrow::StringBuilder builder;
  ASSERT_TRUE(builder.Append("中国").ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append("").ok());
  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(builder.Finish(&values).ok());
  ASSERT_TRUE(OB_NOT_NULL(values.get()));
  for (const ConversionCase &test : cases) {
    lib::CompatModeGuard mode_guard(test.oracle_ ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    sql::ObExecContext exec_ctx(allocator_);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    alignas(16) char frame[8192] = {};
    char *frames[] = {frame};
    eval_ctx.frames_ = frames;
    eval_ctx.set_max_batch_size(3);
    eval_ctx.reuse(3);
    sql::ObExpr expr;
    expr.obj_meta_.set_type(test.type_);
    expr.obj_meta_.set_collation_type(test.collation_);
    expr.datum_meta_ = sql::ObDatumMeta(test.type_, test.collation_, -1);
    expr.datum_meta_.length_semantics_ = LS_CHAR;
    expr.max_length_ = 64;
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = ~0;
    expr.frame_idx_ = 0;
    expr.vector_header_off_ = 0;
    expr.eval_info_off_ = 512;
    expr.datum_off_ = 1024;
    expr.dyn_buf_header_offset_ = 1536;
    expr.res_buf_off_ = 2048;
    expr.res_buf_len_ = 256;
    expr.vec_value_tc_ = ob_is_text_tc(test.type_) ? VEC_TC_LOB : VEC_TC_STRING;
    ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx, VEC_UNIFORM, 3));
    plugin::external::ObArrowDataLoaderFactory factory;
    plugin::external::ObArrowDataLoader *loader = nullptr;
    ASSERT_EQ(OB_SUCCESS, factory.select_loader(allocator_, *arrow::utf8(), expr.datum_meta_, loader));
    ASSERT_TRUE(OB_NOT_NULL(loader));
    if (OB_NOT_NULL(loader) && OB_NOT_NULL(values.get())) {
      ASSERT_EQ(OB_SUCCESS, loader->load(*values, eval_ctx, &expr));
      ObIVector *vector = expr.get_vector(eval_ctx);
      ASSERT_TRUE(OB_NOT_NULL(vector));
      if (OB_NOT_NULL(vector)) {
        EXPECT_EQ(ObString(4, test.expected_), vector->get_string(0));
        EXPECT_TRUE(vector->is_null(1));
        EXPECT_EQ(test.oracle_ && !ob_is_text_tc(test.type_), vector->is_null(2));
        if (!vector->is_null(2)) {
          EXPECT_EQ(0, vector->get_string(2).length());
        }
      }
      loader->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
    }
  }
}

TEST_F(TestJdbcCatalog, materialize_lobs_and_timestamp_with_time_zone)
{
  using plugin::external::ObArrowDataLoader;
  struct TargetCase {
    const char *name_;
    bool oracle_;
    bool binary_;
    bool timestamp_;
  };
  const TargetCase cases[] = {
      {"LONGTEXT", false, false, false}, {"LONGBLOB", false, true, false},
      {"CLOB", true, false, false}, {"BLOB", true, true, false},
      {"TIMESTAMP_WITH_TIME_ZONE", true, false, true}};
  for (const TargetCase &target : cases) {
    lib::CompatModeGuard mode_guard(target.oracle_
        ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    std::string json = mysql_metadata_json();
    replace_once(json, "\"name\":\"VARCHAR\"", std::string("\"name\":\"") + target.name_ + "\"");
    replace_once(json, "\"length\":64", "\"length\":null");
    replace_once(json, "\"lengthSemantics\":\"CHAR\"", "\"lengthSemantics\":null");
    replace_once(json, "\"sourceNullable\":true", "\"sourceNullable\":false");
    replace_once(json, "\"targetNullable\":true", "\"targetNullable\":false");
    if (target.oracle_) {
      replace_once(json, "\"targetMode\":\"MYSQL\"", "\"targetMode\":\"ORACLE\"");
    }
    if (target.timestamp_) {
      replace_once(json, "\"scale\":null", "\"scale\":7");
    }
    share::ObJdbcTableMetadataDesc metadata;
    ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
    sql::jdbc::ObJdbcTableMetadata table_metadata(allocator_);
    ASSERT_EQ(OB_SUCCESS, table_metadata.init(
        OB_SYS_TENANT_ID, OB_MIN_USER_OBJECT_ID, OB_MIN_USER_OBJECT_ID + 1,
        OB_MIN_USER_OBJECT_ID + 2, common::ObString("SalesDB"),
        common::ObString("customers"), common::OB_ORIGIN_AND_INSENSITIVE));
    ASSERT_EQ(OB_SUCCESS, table_metadata.setup_table_schema(
        common::ObString("sqlserver"), common::ObString(), metadata, charset_context()));
    share::schema::ObTableSchema *table_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, table_metadata.build_table_schema(table_schema));
    ASSERT_TRUE(OB_NOT_NULL(table_schema));
    const share::schema::ObColumnSchemaV2 *column = table_schema->get_column_schema(
        target.oracle_ ? "DISPLAY_NAME" : "display_name");
    ASSERT_TRUE(OB_NOT_NULL(column));
    EXPECT_FALSE(column->is_nullable());
    EXPECT_EQ(target.timestamp_ ? common::ObTimestampTZType : common::ObLongTextType,
              column->get_data_type());
    if (target.timestamp_) {
      EXPECT_EQ(7, column->get_data_scale());
    } else {
      EXPECT_EQ(target.binary_ ? common::CS_TYPE_BINARY : common::CS_TYPE_UTF8MB4_BIN,
                column->get_collation_type());
      EXPECT_EQ(common::OB_MAX_LONGTEXT_LENGTH, column->get_data_length());
    }
    plugin::external::ObArrowDataLoaderFactory factory;
    plugin::external::ObArrowDataLoader *loader = nullptr;
    const sql::ObDatumMeta datum_meta(column->get_data_type(), column->get_collation_type(),
                                     column->get_data_scale());
    const std::shared_ptr<arrow::DataType> arrow_type = target.binary_ ? arrow::binary() : arrow::utf8();
    ASSERT_TRUE(OB_NOT_NULL(arrow_type.get()));
    ASSERT_EQ(OB_SUCCESS, factory.select_loader(allocator_, *arrow_type, datum_meta, loader));
    ASSERT_TRUE(OB_NOT_NULL(loader));
    loader->destroy();
    OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
  }
}

TEST_F(TestJdbcCatalog, decimal_limits_follow_target_mode)
{
  share::schema::ObColumnSchemaV2 column;
  using share::schema::ObExternalTableColumnSchemaHelper;
  EXPECT_EQ(OB_SUCCESS, ObExternalTableColumnSchemaHelper::setup_decimal(true, 38, 38, column));
  EXPECT_EQ(common::ObDecimalIntType, column.get_data_type());
  EXPECT_EQ(38, column.get_data_scale());
  EXPECT_EQ(OB_ERR_TOO_BIG_SCALE,
            ObExternalTableColumnSchemaHelper::setup_decimal(false, 38, 31, column));
  EXPECT_EQ(OB_SUCCESS, ObExternalTableColumnSchemaHelper::setup_decimal(false, 65, 30, column));
  EXPECT_EQ(OB_ERR_TOO_BIG_PRECISION,
            ObExternalTableColumnSchemaHelper::setup_decimal(true, 39, 0, column));
  EXPECT_EQ(OB_ERR_M_BIGGER_THAN_D,
            ObExternalTableColumnSchemaHelper::setup_decimal(true, 30, 31, column));
}

TEST_F(TestJdbcCatalog, reject_missing_or_contradictory_nullability)
{
  for (const char *field : {"sourceNullable", "targetNullable"}) {
    for (const char *replacement : {"null", "0", "\"false\""}) {
      std::string json = mysql_metadata_json();
      replace_once(json, std::string("\"") + field + "\":true",
                   std::string("\"") + field + "\":" + replacement);
      share::ObJdbcTableMetadataDesc metadata;
      EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
          common::ObString(json.length(), json.data()), allocator_, metadata));
    }
    std::string json = mysql_metadata_json();
    replace_once(json, std::string("\"") + field + "\":true,", "");
    share::ObJdbcTableMetadataDesc metadata;
    EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
  }
  std::string json = mysql_metadata_json();
  replace_once(json, "\"targetNullable\":true", "\"targetNullable\":false");
  share::ObJdbcTableMetadataDesc metadata;
  EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
      common::ObString(json.length(), json.data()), allocator_, metadata));

  replace_once(json, "\"sourceNullable\":true", "\"sourceNullable\":false");
  replace_once(json, "\"targetMode\":\"MYSQL\"", "\"targetMode\":\"ORACLE\"");
  replace_once(json, "\"name\":\"VARCHAR\"", "\"name\":\"VARCHAR2\"");
  EXPECT_EQ(OB_INVALID_ARGUMENT, share::ObJdbcTableMetadataParser::parse(
      common::ObString(json.length(), json.data()), allocator_, metadata));
}

TEST_F(TestJdbcCatalog, target_nullability_controls_optimizer_not_null_flag)
{
  for (bool oracle : {false, true}) {
    lib::CompatModeGuard mode_guard(oracle ? lib::Worker::CompatMode::ORACLE
                                         : lib::Worker::CompatMode::MYSQL);
    std::string json = mysql_metadata_json();
    replace_once(json, "\"sourceNullable\":true", "\"sourceNullable\":false");
    if (oracle) {
      replace_once(json, "\"targetMode\":\"MYSQL\"", "\"targetMode\":\"ORACLE\"");
      replace_once(json, "\"name\":\"VARCHAR\"", "\"name\":\"VARCHAR2\"");
    } else {
      replace_once(json, "\"targetNullable\":true", "\"targetNullable\":false");
    }
    share::ObJdbcTableMetadataDesc metadata;
    ASSERT_EQ(OB_SUCCESS, share::ObJdbcTableMetadataParser::parse(
        common::ObString(json.length(), json.data()), allocator_, metadata));
    ASSERT_FALSE(metadata.columns_.at(0).source_nullable_);
    ASSERT_EQ(oracle, metadata.columns_.at(0).target_nullable_);
    sql::jdbc::ObJdbcTableMetadata table_metadata(allocator_);
    ASSERT_EQ(OB_SUCCESS, table_metadata.init(OB_SYS_TENANT_ID,
        OB_MIN_USER_OBJECT_ID, OB_MIN_USER_OBJECT_ID + 1, OB_MIN_USER_OBJECT_ID + 2,
        common::ObString("sales"), common::ObString("customers"),
        common::OB_ORIGIN_AND_INSENSITIVE));
    ASSERT_EQ(OB_SUCCESS, table_metadata.setup_table_schema(
        common::ObString("sqlserver"), common::ObString(), metadata, charset_context()));
    share::schema::ObTableSchema *table_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, table_metadata.build_table_schema(table_schema));
    ASSERT_NE(nullptr, table_schema);
    if (OB_NOT_NULL(table_schema)) {
      EXPECT_EQ(0, table_schema->get_schema_version());
      const auto *column = table_schema->get_column_schema(oracle ? "DISPLAY_NAME" : "display_name");
      ASSERT_NE(nullptr, column);
      if (OB_NOT_NULL(column)) {
        EXPECT_EQ(oracle, column->is_nullable());
        EXPECT_EQ(!oracle, column->is_not_null_for_read());
        EXPECT_EQ(!oracle, (sql::ObRawExprUtils::calc_column_result_flag(*column) & NOT_NULL_FLAG) != 0);
      }
    }
  }
}

TEST_F(TestJdbcCatalog, materialize_column_name_case)
{
  using namespace common;
  struct NameCase
  {
    const char *remote_name_;
    const char *oracle_name_;
  };
  const NameCase cases[] = {
      {"plain_name", "PLAIN_NAME"}, {"MiXeD123", "MIXED123"}, {"c_$#", "C_$#"},
      {"row id", "row id"}, {"MiXeD.id", "MiXeD.id"}, {"amount]net", "amount]net"},
      {"O'Brien", "O'Brien"}, {R"(slash\name)", R"(slash\name)"},
      {"a-b", "a-b"}, {"a/b", "a/b"}, {"a%b", "a%b"},
      {"double\"name", "double\"name"}, {"tab\tname", "tab\tname"},
      {"line\nname", "line\nname"}, {"显示名", "显示名"}, {"名称MiXeD", "名称MiXeD"},
      {"1abc", "1abc"}, {"_x", "_x"}, {"$a", "$a"}, {"a1_$#", "A1_$#"}
  };
  for (const bool oracle : {false, true}) {
    SCOPED_TRACE(oracle);
    lib::CompatModeGuard guard(oracle ? lib::Worker::CompatMode::ORACLE
                                    : lib::Worker::CompatMode::MYSQL);
    for (const bool unsupported : {false, true}) {
      SCOPED_TRACE(unsupported);
      share::ObJdbcTableMetadataDesc metadata;
      metadata.target_mode_ = oracle ? ORACLE_MODE : MYSQL_MODE;
      for (const NameCase &test : cases) {
        share::ObJdbcColumnMetadata column;
        column.name_ = ObString(test.remote_name_);
        column.ordinal_position_ = metadata.columns_.count() + 1;
        column.target_type_.name_ = ObString(unsupported ? "UNSUPPORTED" : oracle ? "NUMBER" : "INT");
        if (oracle && !unsupported) {
          column.target_type_.precision_ = 10;
          column.target_type_.scale_ = 0;
        }
        ASSERT_EQ(OB_SUCCESS, metadata.columns_.push_back(column));
      }
      share::schema::ObTableSchema *schema = nullptr;
      ASSERT_EQ(OB_SUCCESS, build_schema(metadata, charset_context(), schema));
      ASSERT_NE(nullptr, schema);
      for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
        SCOPED_TRACE(cases[i].remote_name_);
        const auto *column = schema->get_column_schema_by_idx(i);
        ASSERT_NE(nullptr, column);
        EXPECT_EQ(ObString(oracle ? cases[i].oracle_name_ : cases[i].remote_name_),
                  column->get_column_name_str());
      }
    }
  }
}

TEST_F(TestJdbcCatalog, generated_column_names_preserve_bytes_across_session_modes)
{
  using namespace common;
  ASSERT_EQ(OB_SUCCESS, sql::init_sql_factories());
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  const std::string repeated_quotes(64, '\'');
  const std::string repeated_backslashes(64, '\\');
  const char *names[] = {"plain_name", "O'Brien", R"(slash\name)", R"(quote'\name)",
                        R"(end\)", "MiXeD.id", "amount]net", "显示名", "tab\tname",
                        "line\nname", "double\"name", repeated_quotes.c_str(),
                        repeated_backslashes.c_str()};
  for (const bool oracle : {false, true}) {
#ifndef OB_BUILD_ORACLE_PARSER
    // Oracle expression parsing requires the Oracle SQL parser.
    if (oracle) {
      continue;
    }
#endif
    SCOPED_TRACE(oracle);
    lib::CompatModeGuard guard(oracle ? lib::Worker::CompatMode::ORACLE
                                    : lib::Worker::CompatMode::MYSQL);
    share::ObJdbcTableMetadataDesc metadata;
    metadata.target_mode_ = oracle ? ORACLE_MODE : MYSQL_MODE;
    for (const char *name : names) {
      share::ObJdbcColumnMetadata column;
      column.name_ = ObString(name);
      column.ordinal_position_ = metadata.columns_.count() + 1;
      column.target_type_.name_ = ObString(oracle ? "NUMBER" : "INT");
      if (oracle) {
        column.target_type_.precision_ = 10;
        column.target_type_.scale_ = 0;
      }
      ASSERT_EQ(OB_SUCCESS, metadata.columns_.push_back(column));
    }
    share::schema::ObTableSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_schema(metadata, charset_context(), schema));
    ASSERT_TRUE(OB_NOT_NULL(schema));
    for (const ObCollationType collation : {CS_TYPE_UTF8MB4_BIN, CS_TYPE_GBK_BIN}) {
      SCOPED_TRACE(collation);
      for (const bool no_backslash : {false, true}) {
        SCOPED_TRACE(no_backslash);
        sql::ObSQLSessionInfo session;
        sql::ObExecContext exec_ctx(allocator_);
        sql::LinkExecCtxGuard link_guard(session, exec_ctx);
        ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
        ASSERT_EQ(OB_SUCCESS, session.load_default_sys_variable(false, true));
        session.set_compatibility_mode(metadata.target_mode_);
        session.set_sql_mode(oracle ? DEFAULT_ORACLE_MODE
            : DEFAULT_MYSQL_MODE | (no_backslash ? SMO_NO_BACKSLASH_ESCAPES : 0));
        ASSERT_EQ(OB_SUCCESS, session.update_sys_variable(
            share::SYS_VAR_COLLATION_CONNECTION, static_cast<int64_t>(collation)));
        for (int64_t i = 0; i < ARRAYSIZEOF(names); ++i) {
          SCOPED_TRACE(names[i]);
          const share::schema::ObColumnSchemaV2 *column = schema->get_column_schema_by_idx(i);
          ASSERT_TRUE(OB_NOT_NULL(column));
          expect_generated_column_path(*column, *schema, session, names[i]);
        }
      }
    }
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  share::ObTenantBase tenant(OB_SERVER_TENANT_ID);
  share::ObTenantEnv::set_tenant(&tenant);
  DEFER(share::ObTenantEnv::set_tenant(nullptr));
  if (OB_SUCCESS != common::ObCharset::init_charset()) {
    return 1;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
