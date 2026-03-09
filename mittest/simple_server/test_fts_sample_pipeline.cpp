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

// 测试 FTS Sample Pipeline 的功能
// 测试 ObFtsSamplePipeline 和相关 Operator 的功能

#define USING_LOG_PREFIX STORAGE

// #include "lib/oblog/ob_log_level.h"
#include "test_ddl_pipeline_base.h"


// 为了在单测中访问必要的内部成员，这里临时打开 private/protected
#define private public
#define protected public

// 被测代码的头文件
#include "storage/ddl/ob_fts_sample_pipeline.h"
#include "storage/ddl/ob_column_clustered_dag.h"

// 相关的依赖头文件
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/ddl/test_batch_rows_generater.h"

#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::observer;

int main(int argc, char **argv)
{
  system("rm -f test_fts_sample_pipeline.log*");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_fts_sample_pipeline.log", true, true);  // 输出到日志文件
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

namespace oceanbase
{

namespace unittest
{

// Mock sorter for FTS sample (same as in implementation)
class ObFtsRangeSorter
{
public:
  static int sort(oceanbase::common::ObIArray<const oceanbase::sql::ObPxTabletRange::DatumKey *> &keys,
                  const bool is_inverted)
  {
    STORAGE_LOG(INFO, "mock fts sample sorter invoked", K(is_inverted), "key_count", keys.count());
    return OB_SUCCESS;
  }
};

// Mock DDL task for testing
class MockDDLTask : public share::ObITask
{
public:
  MockDDLTask() : share::ObITask(share::ObITask::TASK_TYPE_DDL_FTS_SAMPLE_TASK) {}
  virtual ~MockDDLTask() = default;
  virtual int process() override { return OB_SUCCESS; }
  virtual share::ObITask::ObITaskPriority get_priority() override { return share::ObITask::TASK_PRIO_1; }
};

class TestFtsSamplePipeline : public ObDDLPipelineTestBase
{
public:
  TestFtsSamplePipeline()
    : ObDDLPipelineTestBase("test_fts_sample_pipeline_"),
      ls_id_(1001),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "TFSP")),
      ddl_dag_(),
      table_schema_(),
      fts_sample_pipeline_(),
      storage_schema_(nullptr)
  {}

  virtual ~TestFtsSamplePipeline() = default;

  virtual void SetUp() override
  {
    ObDDLPipelineTestBase::SetUp();
    if (OB_INVALID_TENANT_ID == shared_tenant_id_) {
      STORAGE_LOG(INFO,  "create shared tenant start");
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(shared_tenant_id_));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
      STORAGE_LOG(INFO,  "create shared tenant end", K(shared_tenant_id_));
    }
    tenant_id_ = shared_tenant_id_;
    allocator_.set_attr(ObMemAttr(tenant_id_, "TFSP"));
  }

  virtual void TearDown() override
  {
    ObDDLPipelineTestBase::TearDown();
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

  void prepare_table_schema(const ObObjType *col_obj_types, const int64_t col_count, const int64_t rowkey_count)
  {
    ObColumnSchemaV2 column;
    uint64_t table_id = 500001;
    int64_t micro_block_size = 16 * 1024; // 16K

    // set data table schema
    table_schema_.reset();
    EXPECT_EQ(OB_SUCCESS, table_schema_.set_table_name("fts_sample_test"));
    table_schema_.set_tenant_id(tenant_id_);
    table_schema_.set_tablegroup_id(1);
    table_schema_.set_database_id(1);
    table_schema_.set_table_id(table_id);
    table_schema_.set_rowkey_column_num(rowkey_count);
    table_schema_.set_max_used_column_id(col_count);
    table_schema_.set_block_size(micro_block_size);
    table_schema_.set_compress_func_name("none");
    table_schema_.set_row_store_type(ENCODING_ROW_STORE);

    // add column
    ObSqlString name_str;
    ObArray<share::schema::ObColDesc> cols_desc;
    for (int64_t i = 0; i < col_count; ++i) {
      ObObjType obj_type = col_obj_types[i];
      column.reset();
      column.set_table_id(table_id);
      column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
      name_str.assign_fmt("col%ld", i);
      EXPECT_EQ(OB_SUCCESS, column.set_column_name(name_str.ptr()));
      column.set_data_type(obj_type);
      if (ObVarcharType == obj_type || ObCharType == obj_type || ObHexStringType == obj_type
          || ObNVarchar2Type == obj_type || ObNCharType == obj_type || ObTextType == obj_type) {
        column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        if (ObCharType == obj_type || ObVarcharType == obj_type) {
          const int64_t max_char_length = lib::is_oracle_mode()
                                          ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                          : OB_MAX_CHAR_LENGTH;
          column.set_data_length(max_char_length);
        }
      } else {
        column.set_collation_type(CS_TYPE_BINARY);
      }

      if (i < rowkey_count) {
        column.set_rowkey_position(i + 1);
      } else {
        column.set_rowkey_position(0);
      }
      EXPECT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    }
  }

  int create_tablet_to_ls()
  {
    int ret = OB_SUCCESS;
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObDDLKvMgrHandle ddl_kv_mgr_handle;

    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,  "ls_service is null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      STORAGE_LOG(WARN,  "fail to get ls", K(ret), K(ls_id_));
    } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle,
                                                       tablet_id_,
                                                       table_schema_,
                                                       allocator_,
                                                       ObTabletStatus::NORMAL,
                                                       share::SCN::min_scn(),
                                                       tablet_handle))) {
      STORAGE_LOG(WARN,  "fail to create tablet", K(ret), K(ls_id_), K(table_schema_));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle,
                                                               true /*try_create*/))) {
      STORAGE_LOG(WARN,  "failed to create ddl kv mgr", K(ret));
    }
    return ret;
  }

  int initialize_args_and_env(const ObObjType *col_types, const int64_t col_count, const int64_t rowkey_count)
  {
    int ret = OB_SUCCESS;
    const int64_t px_thread_count = 16;
    uint64_t tenant_data_version = DATA_VERSION_4_3_1_0;
    int64_t snapshot_version = common::ObTimeUtility::current_time();
    const int64_t ddl_table_id = 500001;
    const int64_t ddl_task_id = 5000000;
    const int64_t ddl_execution_id = 0;

    allocator_.set_attr(ObMemAttr(tenant_id_, "TFSP"));
    prepare_table_schema(col_types, col_count, rowkey_count);
    ddl_dag_.ddl_task_param_.tenant_data_version_ = tenant_data_version;
    ddl_dag_.ddl_task_param_.is_no_logging_ = true;
    ddl_dag_.ddl_task_param_.ddl_task_id_ = ddl_task_id;
    ddl_dag_.ddl_task_param_.snapshot_version_ = snapshot_version;
    ddl_dag_.ddl_task_param_.schema_version_ = common::ObTimeUtility::current_time();
    ddl_dag_.direct_load_type_ = storage::ObDirectLoadMgrUtil::ddl_get_direct_load_type(GCTX.is_shared_storage_mode(), tenant_data_version);
    ddl_dag_.ddl_task_param_.target_table_id_ = ddl_table_id;
    ddl_dag_.ddl_task_param_.execution_id_ = ddl_execution_id;
    ddl_dag_.direct_load_type_ = ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL;
    ddl_dag_.set_start_time();
    ddl_dag_.ObDDLIndependentDag::is_inited_ = true;
    ddl_dag_.is_inited_ = true;
    ObDDLTabletContext *tablet_context = OB_NEWx(ObDDLTabletContext, &allocator_);
    ObStorageSchema *storage_schema = OB_NEWx(ObStorageSchema, &allocator_);

    if (nullptr == tablet_context || nullptr == storage_schema) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN,  "fail to allocate an ObDDLTabletContext obj", K(ret), KP(tablet_context), KP(storage_schema));
    } else if (OB_FAIL(ddl_dag_.ObIndependentDag::cond_.init(ObWaitEventIds::INDEPENDENT_DAG_COND_WAIT))) {
      // 绕过 basic_init 中对 dag scheduler 的检查，手动初始化 ObIndependentDag
      STORAGE_LOG(WARN,  "fail to init cond", K(ret));
    } else if (FALSE_IT(ddl_dag_.ObIndependentDag::allocator_ = &allocator_)) {
    } else if (FALSE_IT(ddl_dag_.ObIndependentDag::is_inited_ = true)) {
    } else {
      tablet_context->ls_id_ = ls_id_;
      tablet_context->tablet_id_ = tablet_id_;
      if (OB_FAIL(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL))) {
        STORAGE_LOG(WARN,  "fail to init storage schema", K(ret));
      } else {
        tablet_context->tablet_param_.storage_schema_ = storage_schema;
        tablet_context->tablet_param_.tablet_transfer_seq_ = common::ObTimeUtility::current_time();
        tablet_context->tablet_param_.is_micro_index_clustered_ = true;
        tablet_context->tablet_param_.with_cs_replica_ = false;
      }
    }

    if (OB_SUCC(ret)) {
      const ObMemAttr bucket_attr(tenant_id_, "TFSP");
      if (OB_FAIL(!ddl_dag_.tablet_context_map_.created() && ddl_dag_.tablet_context_map_.create(5, bucket_attr))) {
        STORAGE_LOG(WARN,  "fail to create tablet context map", K(ret));
      } else if (OB_FAIL(ddl_dag_.tablet_context_map_.set_refactored(tablet_context->tablet_id_, tablet_context))) {
        STORAGE_LOG(WARN,  "fail to set tablet context", K(ret), K(tablet_context->tablet_id_));
      }
    }

    if (OB_SUCC(ret)) { // prepare ddl table schema for FTS
      ObArray<share::schema::ObColDesc> cols_desc;
      if (OB_FAIL(table_schema_.get_rowkey_column_ids(cols_desc))) {
        STORAGE_LOG(WARN,  "fail to get cols desc", K(ret));
      } else {
        ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_ = table_schema_.get_rowkey_column_num();
        ddl_dag_.ddl_table_schema_.table_item_.index_type_ = INDEX_TYPE_FTS_INDEX_LOCAL;  // Set FTS index type
        ddl_dag_.ddl_table_schema_.storage_schema_ = storage_schema;
        for (int64_t i = 0; OB_SUCC(ret) && i < cols_desc.count(); ++i) {
          const ObColDesc &col_desc = cols_desc.at(i);
          const schema::ObColumnSchemaV2 *column_schema = nullptr;
          ObColumnSchemaItem column_item;
          if (i < rowkey_count) {
            column_item.is_rowkey_column_ = true;
          } else {
            column_item.is_rowkey_column_ = false;
          }
          if (i >= ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_
              && i < ddl_dag_.ddl_table_schema_.table_item_.rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
            column_item.col_type_ = col_desc.col_type_;
          } else if (OB_ISNULL(column_schema = table_schema_.get_column_schema(col_desc.col_id_))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN,  "the column schema is null", K(ret), K(col_desc));
          } else {
            column_item.is_valid_ = true;
            column_item.col_type_ = column_schema->get_meta_type();
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ddl_dag_.ddl_table_schema_.column_items_.push_back(column_item))) {
              STORAGE_LOG(WARN,  "fail to push back column schema", K(ret));
            }
          }
        }
      }
    }

    STORAGE_LOG(INFO,   "initialize_args_and_env: before create_tablet_to_ls", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_tablet_to_ls())) {
        STORAGE_LOG(WARN,  "fail to create tablet", K(ret), K(tablet_id_));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t ddl_thread_count = 1;
      if (OB_FAIL(tablet_context->init(ls_id_,
                                       tablet_id_,
                                       ddl_thread_count,
                                       snapshot_version,
                                       ddl_dag_.direct_load_type_,
                                       ddl_dag_.ddl_table_schema_))) {
        STORAGE_LOG(WARN,  "fail to initilize tablet context",
            K(ret), K(ls_id_), K(tablet_id_));
      }
    }

    if (OB_SUCC(ret)) {
      storage_schema_ = tablet_context->tablet_param_.storage_schema_;
    }
    return ret;
  }


  // 准备采样数据（使用ObBatchRowsGen生成测试数据）
  int prepare_sample_data(ObDDLTabletContext &tablet_context)
  {
    int ret = OB_SUCCESS;

    // 使用ObBatchRowsGen生成批量测试数据
    ObBatchRowsGen batch_rows_gen;
    blocksstable::ObBatchDatumRows batch_rows;
    ObArray<blocksstable::ObBatchDatumRows> container_batch_rows;

    const int64_t rowkey_count = 2;  // doc_id, word
    const int64_t col_count = 2;
    const int64_t batch_size = 512;
    const int64_t batch_count = 2;   // 生成两个批次即可覆盖样本

    ObObjType col_types[col_count] = {ObIntType, ObVarcharType};

    if (OB_FAIL(batch_rows_gen.init(col_types, col_count, rowkey_count))) {
      STORAGE_LOG(WARN, "fail to init batch rows gen", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count; ++i) {
        if (OB_FAIL(batch_rows_gen.get_batch_rows(batch_rows, batch_size, &allocator_))) {
          STORAGE_LOG(WARN, "fail to get batch rows", K(ret), K(i));
        } else if (OB_FAIL(container_batch_rows.push_back(batch_rows))) {
          STORAGE_LOG(WARN, "fail to push back batch rows", K(ret), K(i));
        }
      }
    }

    int64_t total_rows = 0;
    blocksstable::ObDatumRow datum_row;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(datum_row.init(col_count))) {
        STORAGE_LOG(WARN, "fail to init datum row", K(ret), K(col_count));
      }
    }

    // 构造正排PxTabletRange：每一行作为一个DatumKey，列顺序(doc_id, word)
    if (OB_SUCC(ret)) {
      common::Ob2DArray<sql::ObPxTabletRange> forward_ranges;
      if (OB_FAIL(forward_ranges.init(1))) {
        STORAGE_LOG(WARN, "fail to init forward ranges array", K(ret));
      } else {
        sql::ObPxTabletRange px_range;
        px_range.tablet_id_ = tablet_id_.id();

        for (int64_t bi = 0; OB_SUCC(ret) && bi < container_batch_rows.count(); ++bi) {
          const blocksstable::ObBatchDatumRows &current_batch = container_batch_rows.at(bi);
          for (int64_t ri = 0; OB_SUCC(ret) && ri < current_batch.row_count_; ++ri) {
            if (OB_FAIL(current_batch.to_datum_row(ri, datum_row))) {
              STORAGE_LOG(WARN, "fail to convert to datum row", K(ret), K(bi), K(ri));
            } else if (datum_row.count_ >= 2) {
              sql::ObPxTabletRange::DatumKey datum_key;
              if (OB_FAIL(datum_key.push_back(datum_row.storage_datums_[0]))) { // doc_id
                STORAGE_LOG(WARN, "fail to push back doc_id datum", K(ret), K(bi), K(ri));
              } else if (OB_FAIL(datum_key.push_back(datum_row.storage_datums_[1]))) { // word
                STORAGE_LOG(WARN, "fail to push back word datum", K(ret), K(bi), K(ri));
              } else if (OB_FAIL(px_range.range_cut_.push_back(datum_key))) {
                STORAGE_LOG(WARN, "fail to push back range cut", K(ret), K(bi), K(ri));
              } else {
                ++total_rows;
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(forward_ranges.push_back(px_range))) {
            STORAGE_LOG(WARN, "fail to push back px range", K(ret));
          } else if (OB_FAIL(tablet_context.fts_forward_part_ranges_.assign(forward_ranges))) {
            STORAGE_LOG(WARN, "fail to assign forward ranges", K(ret));
          }
        }
      }
    }

    // 构造倒排PxTabletRange：列顺序(word, doc_id)
    if (OB_SUCC(ret)) {
      common::Ob2DArray<sql::ObPxTabletRange> inverted_ranges;
      if (OB_FAIL(inverted_ranges.init(1))) {
        STORAGE_LOG(WARN, "fail to init inverted ranges array", K(ret));
      } else {
        sql::ObPxTabletRange inverted_px_range;
        inverted_px_range.tablet_id_ = tablet_id_.id();

        for (int64_t bi = 0; OB_SUCC(ret) && bi < container_batch_rows.count(); ++bi) {
          const blocksstable::ObBatchDatumRows &current_batch = container_batch_rows.at(bi);
          for (int64_t ri = 0; OB_SUCC(ret) && ri < current_batch.row_count_; ++ri) {
            if (OB_FAIL(current_batch.to_datum_row(ri, datum_row))) {
              STORAGE_LOG(WARN, "fail to convert to datum row for inverted", K(ret), K(bi), K(ri));
            } else if (datum_row.count_ >= 2) {
              sql::ObPxTabletRange::DatumKey inverted_key;
              if (OB_FAIL(inverted_key.push_back(datum_row.storage_datums_[1]))) { // word first
                STORAGE_LOG(WARN, "fail to push back word datum for inverted", K(ret), K(bi), K(ri));
              } else if (OB_FAIL(inverted_key.push_back(datum_row.storage_datums_[0]))) { // doc_id second
                STORAGE_LOG(WARN, "fail to push back doc_id datum for inverted", K(ret), K(bi), K(ri));
              } else if (OB_FAIL(inverted_px_range.range_cut_.push_back(inverted_key))) {
                STORAGE_LOG(WARN, "fail to push back inverted range cut", K(ret), K(bi), K(ri));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(inverted_ranges.push_back(inverted_px_range))) {
            STORAGE_LOG(WARN, "fail to push back inverted px range", K(ret));
          } else if (OB_FAIL(tablet_context.fts_inverted_part_ranges_.assign(inverted_ranges))) {
            STORAGE_LOG(WARN, "fail to assign inverted ranges", K(ret));
          }
        }
      }
    }

    // 设置预期范围数量为生成的总行数
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_context.set_expect_range_count(total_rows))) {
        STORAGE_LOG(WARN, "fail to set expect range count", K(ret), K(total_rows));
      }
    }
    STORAGE_LOG(INFO, "prepare_sample_data completed", K(total_rows), "forward_cnt", tablet_context.get_forward_sample_ranges().count(),
                "inverted_cnt", tablet_context.get_inverted_sample_ranges().count());

    return ret;
  }

  // 验证采样结果
  int verify_sample_results(ObDDLTabletContext &tablet_context)
  {
    int ret = OB_SUCCESS;

    // 检查最终的采样结果range（不是数组）
    const sql::ObPxTabletRange &forward_final_range = tablet_context.get_forward_final_sample_range();
    const sql::ObPxTabletRange &inverted_final_range = tablet_context.get_inverted_final_sample_range();

    STORAGE_LOG(INFO,   "verify final sample results",
                "forward_cut_count", forward_final_range.range_cut_.count(),
                "inverted_cut_count", inverted_final_range.range_cut_.count());

    // 验证正排最终采样结果
    if (forward_final_range.range_cut_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR,  "empty forward final range cut", K(ret));
    } else {
      STORAGE_LOG(INFO,   "forward final range has valid cuts", "count", forward_final_range.range_cut_.count());
      // 验证采样点的格式
      for (int64_t j = 0; OB_SUCC(ret) && j < forward_final_range.range_cut_.count(); ++j) {
        const sql::ObPxTabletRange::DatumKey &key = forward_final_range.range_cut_.at(j);
        if (key.count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR,  "invalid forward key count, expected 2", K(ret), K(key.count()), K(j));
        } else {
          const int64_t doc_id = key.at(0).get_int();
          const ObString word = key.at(1).get_string();
          STORAGE_LOG(INFO,   "forward final sample point", K(j), K(doc_id), K(word));
        }
      }
    }

    // 验证倒排最终采样结果
    if (inverted_final_range.range_cut_.empty()) {
      STORAGE_LOG(WARN,  "empty inverted final range cut - this may be expected if no inverted data");
    } else {
      STORAGE_LOG(INFO,   "inverted final range has valid cuts", "count", inverted_final_range.range_cut_.count());
      // 验证倒排采样点的格式：(word, docid)
      for (int64_t j = 0; OB_SUCC(ret) && j < inverted_final_range.range_cut_.count(); ++j) {
        const sql::ObPxTabletRange::DatumKey &key = inverted_final_range.range_cut_.at(j);
        if (key.count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR,  "invalid inverted key count, expected 2", K(ret), K(key.count()), K(j));
        } else {
          const ObString word = key.at(0).get_string();
          const int64_t doc_id = key.at(1).get_int();
          STORAGE_LOG(INFO,   "inverted final sample point", K(j), K(word), K(doc_id));
        }
      }
    }

    return ret;
  }

protected:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObArenaAllocator allocator_;
  observer::ObTableLoadDag ddl_dag_;
  ObTableSchema table_schema_;
  ObFtsSamplePipeline fts_sample_pipeline_;
  ObStorageSchema *storage_schema_;
};

// 测试用例1: 测试FTS Sample Pipeline初始化
TEST_F(TestFtsSamplePipeline, test_fts_sample_pipeline_init)
{
  // 切换到共享租户上下文
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  // 初始化测试环境
  tablet_id_ = 2000001;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 2;
  const ObObjType col_types[col_count] = {ObIntType, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));

  // 测试pipeline初始化
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.init(tablet_id_));
  // Note: is_valid() is not implemented for ObFtsSamplePipeline

  // 验证pipeline的基本属性
  ASSERT_EQ(share::ObITask::TASK_TYPE_DDL_FTS_SAMPLE_TASK, fts_sample_pipeline_.get_type());
  ASSERT_EQ(tablet_id_, fts_sample_pipeline_.tablet_id_);

  STORAGE_LOG(INFO,   "FTS sample pipeline init test passed");
}

// 测试用例2: 测试FTS Sample Pipeline执行流程
TEST_F(TestFtsSamplePipeline, test_fts_sample_pipeline_execute)
{
  // 切换到共享租户上下文
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  // 初始化测试环境
  tablet_id_ = 2000002;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 2;
  const ObObjType col_types[col_count] = {ObIntType, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));

  // 初始化pipeline
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.init(tablet_id_));

  // 获取tablet context并准备测试数据
  ObDDLTabletContext *tablet_context = nullptr;
  ASSERT_EQ(OB_SUCCESS, ddl_dag_.get_tablet_context(tablet_id_, tablet_context));
  ASSERT_NE(nullptr, tablet_context);

  // 准备采样数据 - 二元组 (doc_id, word)
  ASSERT_EQ(OB_SUCCESS, prepare_sample_data(*tablet_context));

  // 调试：检查tablet_context的状态
  STORAGE_LOG(INFO,   "before pipeline execution", K(tablet_context->tablet_id_), "expect_range_count", tablet_context->get_expect_range_count());
  const auto &forward_ranges = tablet_context->get_forward_sample_ranges();
  const auto &inverted_ranges = tablet_context->get_inverted_sample_ranges();
  STORAGE_LOG(INFO,   "sample ranges count", "forward", forward_ranges.count(), "inverted", inverted_ranges.count());
  for (int64_t i = 0; i < forward_ranges.count(); ++i) {
    STORAGE_LOG(INFO,   "forward range detail", K(i), "cut_count", forward_ranges.at(i).range_cut_.count());
  }
  for (int64_t i = 0; i < inverted_ranges.count(); ++i) {
    STORAGE_LOG(INFO,   "inverted range detail", K(i), "cut_count", inverted_ranges.at(i).range_cut_.count());
  }

  // 设置pipeline的DAG
  fts_sample_pipeline_.set_dag(ddl_dag_);

  // 执行pipeline - 通过process方法
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.process());

  // 验证结果 - 验证二元组采样结果
  ASSERT_EQ(OB_SUCCESS, verify_sample_results(*tablet_context));

  STORAGE_LOG(INFO,   "FTS sample pipeline execute test passed");
}

// 测试用例3: 测试FTS Sample Pipeline的chunk获取
TEST_F(TestFtsSamplePipeline, test_fts_sample_pipeline_get_chunk)
{
  // 切换到共享租户上下文
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  // 初始化测试环境
  tablet_id_ = 2000003;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 2;
  const ObObjType col_types[col_count] = {ObIntType, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));

  // 初始化pipeline
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.init(tablet_id_));

  // 设置pipeline的DAG
  fts_sample_pipeline_.set_dag(ddl_dag_);

  // 验证初始状态
  ASSERT_FALSE(fts_sample_pipeline_.is_chunk_generated_);

  // 测试get_next_chunk - 第一次应该返回DAG_TABLET_CONTEXT类型的chunk
  ObChunk *chunk = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.get_next_chunk(chunk));
  ASSERT_NE(nullptr, chunk);
  ASSERT_EQ(ObChunk::DAG_TABLET_CONTEXT, chunk->type_);

  // 验证状态改变
  ASSERT_TRUE(fts_sample_pipeline_.is_chunk_generated_);

  // 验证chunk包含正确的tablet context
  ObDDLTabletContext *chunk_tablet_context = nullptr;
  ASSERT_EQ(OB_SUCCESS, chunk->get_dag_tablet_context(chunk_tablet_context));
  ASSERT_NE(nullptr, chunk_tablet_context);
  ASSERT_EQ(tablet_id_, chunk_tablet_context->tablet_id_);

  // 测试get_next_chunk - 第二次应该返回ITER_END_TYPE类型的chunk
  chunk = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.get_next_chunk(chunk));
  ASSERT_NE(nullptr, chunk);
  ASSERT_EQ(ObChunk::ITER_END_TYPE, chunk->type_);

  STORAGE_LOG(INFO,   "FTS sample pipeline get chunk test passed");
}

// 测试用例4: 测试FTS Sample Pipeline的operator初始化
TEST_F(TestFtsSamplePipeline, test_fts_sample_pipeline_operators_init)
{
  // 切换到共享租户上下文
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  // 初始化测试环境
  tablet_id_ = 2000004;
  const int64_t rowkey_count = 2;
  const int64_t col_count = 2;
  const ObObjType col_types[col_count] = {ObIntType, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, initialize_args_and_env(col_types, col_count, rowkey_count));

  // 初始化pipeline
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.init(tablet_id_));

  // 验证operator是否正确初始化（通过私有成员访问）
  ASSERT_TRUE(fts_sample_pipeline_.final_sample_op_.is_inited_);
  ASSERT_TRUE(fts_sample_pipeline_.write_inner_table_op_.is_inited_);
  ASSERT_EQ(tablet_id_, fts_sample_pipeline_.final_sample_op_.tablet_id_);
  ASSERT_EQ(tablet_id_, fts_sample_pipeline_.write_inner_table_op_.tablet_id_);

  // 设置pipeline的DAG
  fts_sample_pipeline_.set_dag(ddl_dag_);

  // 获取第一个chunk，应该能正常获取
  ObChunk *chunk = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_sample_pipeline_.get_next_chunk(chunk));
  ASSERT_NE(nullptr, chunk);

  STORAGE_LOG(INFO,   "FTS sample pipeline operators init test passed");
}

} // namespace unittest
} // namespace oceanbase
