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

// DDL Pipeline 测试基础设施
// 提供租户创建、LS获取、Tablet创建、DAG初始化等通用能力

#ifndef OCEANBASE_UNITTEST_TEST_DDL_PIPELINE_BASE_H_
#define OCEANBASE_UNITTEST_TEST_DDL_PIPELINE_BASE_H_

#include <gtest/gtest.h>
#include <stdexcept>
#include <chrono>

// 为了在单测中访问必要的内部成员，这里临时打开 private/protected
#define private public
#define protected public

#include "storage/ddl/ob_full_text_index_write_task.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "common/ob_version_def.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "share/ob_ls_id.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/init_basic_struct.h"
#include "unittest/storage/test_tablet_helper.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/ddl/test_batch_rows_generater.h"
#include "share/ob_ddl_common.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "unittest/storage/ddl/test_mock_row_iters.h"
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "lib/rc/context.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"

#undef private
#undef protected

#include "env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::observer;

// =============================================================================
// TestColumnBuilder - 列构建器
// =============================================================================
class TestColumnBuilder
{
public:
  TestColumnBuilder()
    : table_id_(0),
      column_id_(0),
      obj_type_(ObNullType),
      rowkey_position_(0),
      collation_type_(CS_TYPE_BINARY),
      data_length_(0)
  {}

  TestColumnBuilder &set_table_id(uint64_t table_id) { table_id_ = table_id; return *this; }
  TestColumnBuilder &set_column_id(uint64_t column_id) { column_id_ = column_id; return *this; }
  TestColumnBuilder &set_obj_type(ObObjType obj_type) { obj_type_ = obj_type; return *this; }
  TestColumnBuilder &set_rowkey_position(int64_t pos) { rowkey_position_ = pos; return *this; }
  TestColumnBuilder &set_collation_type(ObCollationType type) { collation_type_ = type; return *this; }
  TestColumnBuilder &set_data_length(int64_t length) { data_length_ = length; return *this; }

  int build(ObColumnSchemaV2 &column, const char *col_name) const
  {
    int ret = OB_SUCCESS;
    column.reset();
    column.set_table_id(table_id_);
    column.set_column_id(column_id_);
    if (OB_FAIL(column.set_column_name(col_name))) {
      STORAGE_LOG(WARN, "fail to set column name", K(ret), K(col_name));
    } else {
      column.set_data_type(obj_type_);
      column.set_collation_type(collation_type_);
      column.set_rowkey_position(rowkey_position_);
      if (data_length_ > 0) {
        column.set_data_length(data_length_);
      }
    }
    return ret;
  }

private:
  uint64_t table_id_;
  uint64_t column_id_;
  ObObjType obj_type_;
  int64_t rowkey_position_;
  ObCollationType collation_type_;
  int64_t data_length_;
};

// =============================================================================
// TestTableSchemaBuilder - Schema构建器（支持复杂schema）
// =============================================================================
class TestTableSchemaBuilder
{
public:
  TestTableSchemaBuilder()
    : tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(500001),
      tablegroup_id_(1),
      database_id_(1),
      rowkey_column_num_(0),
      micro_block_size_(16 * 1024),
      row_store_type_(ENCODING_ROW_STORE),
      table_name_("ddl_test_table")
  {}

  TestTableSchemaBuilder &set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; return *this; }
  TestTableSchemaBuilder &set_table_id(uint64_t table_id) { table_id_ = table_id; return *this; }
  TestTableSchemaBuilder &set_tablegroup_id(uint64_t id) { tablegroup_id_ = id; return *this; }
  TestTableSchemaBuilder &set_database_id(uint64_t id) { database_id_ = id; return *this; }
  TestTableSchemaBuilder &set_rowkey_column_num(int64_t num) { rowkey_column_num_ = num; return *this; }
  TestTableSchemaBuilder &set_micro_block_size(int64_t size) { micro_block_size_ = size; return *this; }
  TestTableSchemaBuilder &set_row_store_type(ObRowStoreType type) { row_store_type_ = type; return *this; }
  TestTableSchemaBuilder &set_table_name(const char *name) { table_name_ = name; return *this; }

  // 添加列（简化版本，根据类型自动配置）
  TestTableSchemaBuilder &add_column(ObObjType obj_type, bool is_rowkey = false)
  {
    ColumnConfig config;
    config.obj_type_ = obj_type;
    config.is_rowkey_ = is_rowkey;
    columns_.push_back(config);
    if (is_rowkey) {
      rowkey_column_num_++;
    }
    return *this;
  }

  // 从数组添加列（兼容原有接口）
  TestTableSchemaBuilder &add_columns(const ObObjType *col_types, int64_t col_count, int64_t rowkey_count)
  {
    for (int64_t i = 0; i < col_count; ++i) {
      add_column(col_types[i], i < rowkey_count);
    }
    return *this;
  }

  int build(ObTableSchema &schema) const
  {
    int ret = OB_SUCCESS;
    schema.reset();

    if (OB_FAIL(schema.set_table_name(table_name_))) {
      STORAGE_LOG(WARN, "fail to set table name", K(ret));
    } else {
      schema.set_tenant_id(tenant_id_);
      schema.set_tablegroup_id(tablegroup_id_);
      schema.set_database_id(database_id_);
      schema.set_table_id(table_id_);
      schema.set_rowkey_column_num(rowkey_column_num_);
      schema.set_max_used_column_id(columns_.count());
      schema.set_block_size(micro_block_size_);
      schema.set_compress_func_name("none");
      schema.set_row_store_type(row_store_type_);
    }

    // 添加列
    ObSqlString name_str;
    int64_t rowkey_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
      const ColumnConfig &config = columns_.at(i);
      ObColumnSchemaV2 column;
      column.reset();
      column.set_table_id(table_id_);
      column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
      name_str.reuse();
      name_str.assign_fmt("col%ld", i);
      if (OB_FAIL(column.set_column_name(name_str.ptr()))) {
        STORAGE_LOG(WARN, "fail to set column name", K(ret));
      } else {
        column.set_data_type(config.obj_type_);
        // 设置 collation 和 data_length
        if (is_string_type(config.obj_type_)) {
          column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          if (ObCharType == config.obj_type_ || ObVarcharType == config.obj_type_) {
            const int64_t max_char_length = lib::is_oracle_mode()
                                            ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                            : OB_MAX_CHAR_LENGTH;
            column.set_data_length(max_char_length);
          }
        } else {
          column.set_collation_type(CS_TYPE_BINARY);
        }
        // 设置 rowkey position
        if (config.is_rowkey_) {
          column.set_rowkey_position(++rowkey_pos);
        } else {
          column.set_rowkey_position(0);
        }
        if (OB_FAIL(schema.add_column(column))) {
          STORAGE_LOG(WARN, "fail to add column", K(ret), K(i));
        }
      }
    }
    return ret;
  }

private:
  static bool is_string_type(ObObjType type)
  {
    return ObVarcharType == type || ObCharType == type || ObHexStringType == type
           || ObNVarchar2Type == type || ObNCharType == type || ObTextType == type;
  }

  struct ColumnConfig
  {
    ObObjType obj_type_;
    bool is_rowkey_;
    ColumnConfig() : obj_type_(ObNullType), is_rowkey_(false) {}
    TO_STRING_KV(K(obj_type_), K(is_rowkey_));
  };

  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablegroup_id_;
  uint64_t database_id_;
  int64_t rowkey_column_num_;
  int64_t micro_block_size_;
  ObRowStoreType row_store_type_;
  const char *table_name_;
  ObSEArray<ColumnConfig, 16> columns_;
};

// =============================================================================
// TestDagContext - DAG上下文配置
// =============================================================================
struct TestDagConfig
{
  uint64_t tenant_data_version_;
  bool is_no_logging_;
  int64_t ddl_task_id_;
  int64_t snapshot_version_;
  int64_t schema_version_;
  uint64_t target_table_id_;
  int64_t execution_id_;
  ObDirectLoadType direct_load_type_;
  int64_t ddl_thread_count_;

  TestDagConfig()
    : tenant_data_version_(DATA_VERSION_4_4_0_0),
      is_no_logging_(true),
      ddl_task_id_(5000000),
      snapshot_version_(0),
      schema_version_(0),
      target_table_id_(500001),
      execution_id_(0),
      direct_load_type_(ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL),
      ddl_thread_count_(1)
  {
    snapshot_version_ = common::ObTimeUtility::current_time();
    schema_version_ = common::ObTimeUtility::current_time();
  }
};

// =============================================================================
// TestTabletHelper - Tablet操作辅助类
// =============================================================================
class TestTabletFactory
{
public:
  // 创建 tablet 到指定 LS
  static int create_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObTableSchema &table_schema,
      ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle)
  {
    int ret = OB_SUCCESS;
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    ObDDLKvMgrHandle ddl_kv_mgr_handle;

    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls_service is null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      STORAGE_LOG(WARN, "fail to get ls", K(ret), K(ls_id));
    } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle,
                                                       tablet_id,
                                                       table_schema,
                                                       allocator,
                                                       ObTabletStatus::NORMAL,
                                                       share::SCN::min_scn(),
                                                       tablet_handle))) {
      STORAGE_LOG(WARN, "fail to create tablet", K(ret), K(ls_id), K(table_schema));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle,
                                                               true /*try_create*/))) {
      STORAGE_LOG(WARN, "failed to create ddl kv mgr", K(ret));
    }
    return ret;
  }
};

// =============================================================================
// TestDagBuilder - DAG构建器
// =============================================================================
class TestDagBuilder
{
public:
  static int init_dag(
      observer::ObTableLoadDag &dag,
      const TestDagConfig &config,
      ObArenaAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    dag.ddl_task_param_.tenant_data_version_ = config.tenant_data_version_;
    dag.ddl_task_param_.is_no_logging_ = config.is_no_logging_;
    dag.ddl_task_param_.ddl_task_id_ = config.ddl_task_id_;
    dag.ddl_task_param_.snapshot_version_ = config.snapshot_version_;
    dag.ddl_task_param_.schema_version_ = config.schema_version_;
    dag.direct_load_type_ = storage::ObDirectLoadMgrUtil::ddl_get_direct_load_type(
        GCTX.is_shared_storage_mode(), config.tenant_data_version_);
    dag.ddl_task_param_.target_table_id_ = config.target_table_id_;
    dag.ddl_task_param_.execution_id_ = config.execution_id_;
    dag.direct_load_type_ = config.direct_load_type_;
    dag.set_start_time();
    dag.ObDDLIndependentDag::is_inited_ = true;
    dag.is_inited_ = true;

    if (OB_FAIL(dag.ObIndependentDag::cond_.init(ObWaitEventIds::INDEPENDENT_DAG_COND_WAIT))) {
      STORAGE_LOG(WARN, "fail to init cond", K(ret));
    } else {
      dag.ObIndependentDag::allocator_ = &allocator;
      dag.ObIndependentDag::is_inited_ = true;
    }
    return ret;
  }

  // 初始化 DDL Table Schema
  static int init_ddl_table_schema(
      observer::ObTableLoadDag &dag,
      const ObTableSchema &table_schema,
      ObStorageSchema *storage_schema,
      int64_t rowkey_count)
  {
    int ret = OB_SUCCESS;
    ObArray<share::schema::ObColDesc> cols_desc;

    if (OB_FAIL(table_schema.get_multi_version_column_descs(cols_desc))) {
      STORAGE_LOG(WARN, "fail to get cols desc", K(ret));
    } else {
      dag.ddl_table_schema_.table_item_.rowkey_column_num_ = table_schema.get_rowkey_column_num();
      dag.ddl_table_schema_.storage_schema_ = storage_schema;

      for (int64_t i = 0; OB_SUCC(ret) && i < cols_desc.count(); ++i) {
        const ObColDesc &col_desc = cols_desc.at(i);
        const schema::ObColumnSchemaV2 *column_schema = nullptr;
        ObColumnSchemaItem column_item;

        if (i < rowkey_count) {
          column_item.is_rowkey_column_ = true;
        } else {
          column_item.is_rowkey_column_ = false;
        }

        if (i >= dag.ddl_table_schema_.table_item_.rowkey_column_num_
            && i < dag.ddl_table_schema_.table_item_.rowkey_column_num_
                   + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
          column_item.col_type_ = col_desc.col_type_;
        } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(col_desc.col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "the column schema is null", K(ret), K(col_desc));
        } else {
          column_item.is_valid_ = true;
          column_item.col_type_ = column_schema->get_meta_type();
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(dag.ddl_table_schema_.column_items_.push_back(column_item))) {
            STORAGE_LOG(WARN, "fail to push back column schema", K(ret));
          }
        }
      }
    }
    return ret;
  }

  // 创建并注册 TabletContext
  static int create_tablet_context(
      observer::ObTableLoadDag &dag,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      uint64_t tenant_id,
      ObStorageSchema *storage_schema,
      ObArenaAllocator &allocator,
      ObDDLTabletContext *&out_tablet_context)
  {
    int ret = OB_SUCCESS;
    ObDDLTabletContext *tablet_context = OB_NEWx(ObDDLTabletContext, &allocator);

    if (OB_ISNULL(tablet_context)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate ObDDLTabletContext", K(ret));
    } else {
      tablet_context->ls_id_ = ls_id;
      tablet_context->tablet_id_ = tablet_id;
      tablet_context->tablet_param_.storage_schema_ = storage_schema;
      tablet_context->tablet_param_.tablet_transfer_seq_ = common::ObTimeUtility::current_time();
      tablet_context->tablet_param_.is_micro_index_clustered_ = true;
      tablet_context->tablet_param_.with_cs_replica_ = false;

      const ObMemAttr bucket_attr(tenant_id, "DDLPipeTest");
      if (!dag.tablet_context_map_.created() && OB_FAIL(dag.tablet_context_map_.create(5, bucket_attr))) {
        STORAGE_LOG(WARN, "fail to create tablet context map", K(ret));
      } else if (OB_FAIL(dag.tablet_context_map_.set_refactored(tablet_id, tablet_context))) {
        STORAGE_LOG(WARN, "fail to set tablet context", K(ret), K(tablet_id));
      } else {
        out_tablet_context = tablet_context;
      }
    }
    return ret;
  }
};

// =============================================================================
// ObDDLPipelineTestBase - DDL Pipeline 测试基类
// =============================================================================
class ObDDLPipelineTestBase : public ObSimpleClusterTestBase
{
public:
  ObDDLPipelineTestBase(const char *test_name)
    : ObSimpleClusterTestBase(test_name),
      ls_id_(1001),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "DDLPipeTest")),
      ddl_dag_(),
      table_schema_(),
      storage_schema_(nullptr)
  {}

  virtual ~ObDDLPipelineTestBase() = default;

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (OB_INVALID_TENANT_ID == shared_tenant_id_) {
      STORAGE_LOG(INFO, "create shared tenant start");
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(shared_tenant_id_));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
      STORAGE_LOG(INFO, "create shared tenant end", K(shared_tenant_id_));
    }
    tenant_id_ = shared_tenant_id_;
    allocator_.set_attr(ObMemAttr(tenant_id_, "DDLPipeTest"));
  }

  virtual void TearDown() override
  {
    ObSimpleClusterTestBase::TearDown();
  }

  // 获取租户的 LS
  void get_tenant_ls(ObLS *&ls)
  {
    ls = nullptr;
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id_, handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls = handle.get_ls());
  }

  // 初始化测试环境（简化版本，使用列类型数组）
  int initialize_env(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count)
  {
    TestTableSchemaBuilder builder;
    builder.set_tenant_id(tenant_id_)
           .set_table_id(dag_config_.target_table_id_)
           .add_columns(col_types, col_count, rowkey_count);
    return initialize_env_with_schema_builder(builder, rowkey_count);
  }

  // 初始化测试环境（使用 Schema Builder）
  int initialize_env_with_schema_builder(TestTableSchemaBuilder &schema_builder, int64_t rowkey_count)
  {
    int ret = OB_SUCCESS;
    ObTabletHandle tablet_handle;

    // 构建 table schema
    if (OB_FAIL(schema_builder.build(table_schema_))) {
      STORAGE_LOG(WARN, "fail to build table schema", K(ret));
      return ret;
    }

    // 初始化 DAG
    if (OB_FAIL(TestDagBuilder::init_dag(ddl_dag_, dag_config_, allocator_))) {
      STORAGE_LOG(WARN, "fail to init dag", K(ret));
      return ret;
    }

    // 创建 storage schema
    ObStorageSchema *storage_schema = OB_NEWx(ObStorageSchema, &allocator_);
    if (OB_ISNULL(storage_schema)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate storage schema", K(ret));
      return ret;
    }
    if (OB_FAIL(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL))) {
      STORAGE_LOG(WARN, "fail to init storage schema", K(ret));
      return ret;
    }
    storage_schema_ = storage_schema;

    // 创建 TabletContext
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(TestDagBuilder::create_tablet_context(ddl_dag_, ls_id_, tablet_id_,
                                                      tenant_id_, storage_schema, allocator_, tablet_context))) {
      STORAGE_LOG(WARN, "fail to create tablet context", K(ret));
      return ret;
    }

    // 初始化 DDL Table Schema
    if (OB_FAIL(TestDagBuilder::init_ddl_table_schema(ddl_dag_, table_schema_, storage_schema, rowkey_count))) {
      STORAGE_LOG(WARN, "fail to init ddl table schema", K(ret));
      return ret;
    }

    // 创建 tablet
    STORAGE_LOG(INFO, "creating tablet", K(tenant_id_), K(ls_id_), K(tablet_id_));
    if (OB_FAIL(TestTabletFactory::create_tablet(ls_id_, tablet_id_, table_schema_, allocator_, tablet_handle))) {
      STORAGE_LOG(WARN, "fail to create tablet", K(ret), K(tablet_id_));
      return ret;
    }

    // 初始化 tablet context
    if (OB_FAIL(tablet_context->init(ls_id_,
                                     tablet_id_,
                                     dag_config_.ddl_thread_count_,
                                     dag_config_.snapshot_version_,
                                     ddl_dag_.direct_load_type_,
                                     ddl_dag_.ddl_table_schema_))) {
      STORAGE_LOG(WARN, "fail to initialize tablet context", K(ret), K(ls_id_), K(tablet_id_));
      return ret;
    }

    return ret;
  }

  // 获取 tablet context
  int get_tablet_context(ObDDLTabletContext *&tablet_context)
  {
    return ddl_dag_.get_tablet_context(tablet_id_, tablet_context);
  }

  // 获取或创建 slice
  int get_or_create_slice(int64_t slice_idx, ObDDLSlice *&ddl_slice, bool &is_new_slice)
  {
    int ret = OB_SUCCESS;
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(get_tablet_context(tablet_context))) {
      STORAGE_LOG(WARN, "fail to get tablet context", K(ret));
    } else if (OB_FAIL(tablet_context->get_or_create_slice(slice_idx, ddl_slice, is_new_slice))) {
      STORAGE_LOG(WARN, "fail to get or create slice", K(ret), K(slice_idx));
    }
    return ret;
  }

protected:
  static uint64_t shared_tenant_id_;

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObArenaAllocator allocator_;
  observer::ObTableLoadDag ddl_dag_;
  ObTableSchema table_schema_;
  ObStorageSchema *storage_schema_;
  TestDagConfig dag_config_;
};

// 静态成员变量定义
uint64_t ObDDLPipelineTestBase::shared_tenant_id_ = OB_INVALID_TENANT_ID;

} // end namespace unittest
} // end namespace oceanbase

#endif // OCEANBASE_UNITTEST_TEST_DDL_PIPELINE_BASE_H_
