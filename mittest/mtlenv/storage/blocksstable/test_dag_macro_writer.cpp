// owner: zs475329
// owner group: storage

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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <stdexcept>
#include <chrono>
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define NOT_NULL(ass) ASSERT_NE(nullptr, (ass))
#define private public
#define protected public

#include "storage/blocksstable/ob_row_generate.h"
#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/ddl/ob_cg_block_tmp_files_iterator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "unittest/storage/blocksstable/cs_encoding/ob_row_vector_converter.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;
using namespace std;


int ObDDLRedoLogWriterCallback::wait()
{
  int ret = OB_SUCCESS;
  return ret;
}
int ObMacroBlockWriter::exec_callback(const ObStorageObjectHandle &macro_handle, ObMacroBlock *macro_block)
{
  int ret = OB_SUCCESS;
  return ret;
}

namespace unittest
{


class TestDagMacroWriter : public ::testing::Test
{
public:
  TestDagMacroWriter() = default;
  virtual ~TestDagMacroWriter() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
protected:
  void set_master_key(ObWholeDataStoreDesc &data_desc);
  void generate_cg_block(ObCGBlockFile &cg_block_file,
                         int &micro_block_cnt,
                         const bool is_complete_macro_block,
                         const ObMergeType merge_type = ObMergeType::MAJOR_MERGE,
                         const bool is_encrypt = false);
  void generate_cg_block_without_batch(ObCGBlockFile &cg_block_file,
                                       int &micro_block_cnt,
                                       const bool is_complete_macro_block,
                                       const ObMergeType merge_type = ObMergeType::MAJOR_MERGE,
                                       const bool is_encrypt = false);
  void large_batch_generate_cg_block(ObCGBlockFile &cg_block_file, int &micro_block_cnt);
  void random_change_cgblock_micro_block_id(ObCGBlock *cg_block, int64_t &actual_micro_block_count);
  void aggregate_cg_blocks_into_macro_block(ObCGBlockFilesIterator &cg_block_files_iter,
                                            const int64_t expected_all_micro_block_cnt,
                                            const ObMergeType merge_type = ObMergeType::MAJOR_MERGE,
                                            const bool is_encrypt = false,
                                            const uint64_t count = INT64_MAX);
  void test_buf_not_enough(ObCGBlockFilesIterator &cg_block_files_iter, const bool check_if_complete_macro_block, const uint64_t count = INT64_MAX);
  void test_reuse_macro_block(ObCGBlockFilesIterator &cg_block_files_iter, const uint64_t count = INT64_MAX);
  void prepare_index_builder(ObWholeDataStoreDesc &data_desc,
                             ObSSTableIndexBuilder &sstable_builder,
                             const bool need_submit_io,
                             const ObMergeType merge_type = ObMergeType::MAJOR_MERGE,
                             const bool is_encrypt = false);
  void prepare_data_desc(ObWholeDataStoreDesc &data_desc,
                         ObSSTableIndexBuilder *sstable_builder,
                         const bool need_submit_io,
                         const ObMergeType merge_type = ObMergeType::MAJOR_MERGE,
                         const bool is_encrypt = false);
  void prepare_schema();
  void prepare_index_desc(const ObWholeDataStoreDesc &data_desc, ObWholeDataStoreDesc &index_desc);

  static const int64_t TEST_COLUMN_CNT = ObNumberType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  static const int64_t TEST_ROW_CNT = 1000;
  static const int64_t TEST_MICRO_BLOCK_CNT = 30;
  static const int64_t MAX_TEST_COLUMN_CNT = TEST_COLUMN_CNT + 3;
  static const int64_t SNAPSHOT_VERSION = 2;
  static const int64_t TEST_BATCH_SIZE = 100;
  ObTableSchema table_schema_;
  ObTableSchema index_schema_;
  ObArray<VectorFormat> vec_formats_;
  ObRowVectorConverter vec_convertor_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  common::ObArray<share::schema::ObColDesc> multi_col_descs_;
  ObRowGenerate row_generate_;
  ObRowGenerate index_row_generate_;
  ObITable::TableKey table_key_;
  ObDatumRow row_;
  ObDatumRow multi_row_;
  ObArenaAllocator allocator_;
private:
  int64_t tenant_id_ = 1;
  int64_t master_key_id_ = 12332;
  ObCipherOpMode mode_ = ObCipherOpMode::ob_invalid_mode;
  char master_key_[OB_MAX_MASTER_KEY_LENGTH] = "12345";
  char raw_key_[OB_MAX_ENCRYPTION_KEY_NAME_LENGTH] = "54321";
  char encrypt_key_[OB_MAX_ENCRYPTION_KEY_NAME_LENGTH];
  int64_t encrypt_key_len_;
};


void TestDagMacroWriter::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "SetUpTestCase");
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestDagMacroWriter::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

static ObSimpleMemLimitGetter getter;

static void convert_to_multi_version_col_descs(const common::ObArray<share::schema::ObColDesc> &col_descs,
    const int64_t schema_rowkey_cnt, const int64_t column_cnt, common::ObArray<share::schema::ObColDesc> &multi_col_descs)
{
  for (int64_t i = 0; i < schema_rowkey_cnt; ++i) {
    multi_col_descs.push_back(col_descs[i]);
  }
  ObColDesc col_desc_0;
  col_desc_0.col_id_ = static_cast<int32_t>(schema_rowkey_cnt);
  col_desc_0.col_type_ = col_descs[ObInt32Type-1].col_type_;
  multi_col_descs.push_back(col_desc_0);
  ObColDesc col_desc_1;
  col_desc_1.col_id_ = static_cast<int32_t>(schema_rowkey_cnt+1);
  col_desc_1.col_type_ = col_descs[ObInt32Type-1].col_type_;
  multi_col_descs.push_back(col_desc_1);
  for(int64_t i = schema_rowkey_cnt; i < column_cnt; ++i) {
    multi_col_descs.push_back(col_descs[i]);
    multi_col_descs[i+2].col_id_ = static_cast<int32_t>(i+2);
  }
}

void TestDagMacroWriter::SetUp()
{
  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  prepare_schema();
  row_generate_.reset();
  index_row_generate_.reset();
  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, (row_generate_.get_schema().get_column_ids(col_descs_)));
  const ObColumnSchemaV2 *col_schema = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_COLUMN_CNT; ++i) {
    if (OB_ISNULL(col_schema = row_generate_.get_schema().get_column_schema(i + OB_APP_MIN_COLUMN_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column schema is NULL", K(ret), K(i));
    } else if (col_descs_.at(i).col_type_.is_decimal_int()) {
      col_descs_.at(i).col_type_.set_stored_precision(col_schema->get_accuracy().get_precision());
      col_descs_.at(i).col_type_.set_scale(col_schema->get_accuracy().get_scale());
    }
  }
  convert_to_multi_version_col_descs(col_descs_, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, multi_col_descs_);

  ret = index_row_generate_.init(index_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  table_key_.tablet_id_ = table_schema_.get_table_id();
  table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  table_key_.version_range_.snapshot_version_ = share::OB_MAX_SCN_TS_NS - 1;
  ASSERT_TRUE(table_key_.is_valid());
  GCONF.micro_block_merge_verify_level = MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE;
}

void TestDagMacroWriter::TearDown()
{
  row_generate_.reset();
  index_row_generate_.reset();
  table_schema_.reset();
}

static void convert_to_multi_version_row(const ObDatumRow &org_row,
    const int64_t schema_rowkey_cnt, const int64_t column_cnt, const int64_t snapshot_version, const ObDmlFlag dml_flag,
    ObDatumRow &multi_row)
{
  for (int64_t i = 0; i < schema_rowkey_cnt; ++i) {
    multi_row.storage_datums_[i] = org_row.storage_datums_[i];
  }
  multi_row.storage_datums_[schema_rowkey_cnt].set_int(-snapshot_version);
  multi_row.storage_datums_[schema_rowkey_cnt + 1].set_int(0);
  for(int64_t i = schema_rowkey_cnt; i < column_cnt; ++i) {
    multi_row.storage_datums_[i + 2] = org_row.storage_datums_[i];
  }

  multi_row.count_ = column_cnt + 2;
  if (ObDmlFlag::DF_DELETE == dml_flag) {
    multi_row.row_flag_= ObDmlFlag::DF_DELETE;
  } else {
    multi_row.row_flag_ = ObDmlFlag::DF_INSERT;
  }
  multi_row.mvcc_row_flag_.set_last_multi_version_row(true);
}

void TestDagMacroWriter::generate_cg_block(ObCGBlockFile &cg_block_file,
                                           int &micro_block_cnt,
                                           const bool is_complete_macro_block,
                                           const ObMergeType merge_type,
                                           const bool is_encrypt)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/, merge_type, is_encrypt);

  ObMacroDataSeq data_seq(0);
  ObDagTempMacroBlockWriter *dag_temp_macro_writer = new ObDagTempMacroBlockWriter();

  data_seq.set_parallel_degree(0);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = data_seq.macro_data_seq_;
  ObSSTablePrivateObjectCleaner cleaner;

  ObCGBlockFileWriter cg_block_writer;
  ASSERT_EQ(OB_SUCCESS, cg_block_writer.init(&cg_block_file));
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param, cleaner, &cg_block_writer));
  static const int64_t MAX_TEST_COLUMN_CNT = TestDagMacroWriter::TEST_COLUMN_CNT + 3;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;
  int64_t test_row_num = TestDagMacroWriter::TEST_ROW_CNT;
  int64_t test_micro_block_count = TestDagMacroWriter::TEST_MICRO_BLOCK_CNT;
  ObDatumRow row;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestDagMacroWriter::TEST_COLUMN_CNT));
  ASSERT_EQ(false, dag_temp_macro_writer->is_alloc_block_needed());

  int current_index = dag_temp_macro_writer->current_index_;
  ObMacroBlock &current_block = dag_temp_macro_writer->macro_blocks_[current_index];
  while (current_block.get_micro_block_count() < test_micro_block_count - 1) {
    vec_convertor_.reset();
    ASSERT_EQ(OB_SUCCESS, vec_convertor_.init(multi_col_descs_, vec_formats_, TEST_BATCH_SIZE));
    bool is_full = false;
    while(!is_full)
    {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
      convert_to_multi_version_row(row, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, 2, dml, multi_row);
      ASSERT_EQ(OB_SUCCESS, vec_convertor_.append_row(multi_row, is_full));
    }
    ObBatchDatumRows batch_rows;
    ASSERT_EQ(OB_SUCCESS, vec_convertor_.get_batch_datums(batch_rows));
    ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->append_batch(batch_rows));
  }
  //build the last micro block
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  convert_to_multi_version_row(row, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, 2, dml, multi_row);
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->append_row(multi_row));
  ASSERT_GT(dag_temp_macro_writer->micro_writer_->get_row_count(), 0);
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->build_micro_block());

  //check the micro_block_count
  ASSERT_EQ(test_micro_block_count, current_block.get_micro_block_count());
  ASSERT_EQ(true, current_block.is_dirty());
  int32_t row_count = current_block.get_row_count();
  micro_block_cnt += current_block.get_micro_block_count();

  //generate cg_block
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->flush_macro_block(current_block, !is_complete_macro_block));
  if (dag_temp_macro_writer->is_need_macro_buffer_) {
    ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->wait_io_finish(dag_temp_macro_writer->macro_handles_[current_index], &current_block));
  }
  ASSERT_EQ(false, current_block.is_dirty());

  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->close());
  delete dag_temp_macro_writer;
}

void TestDagMacroWriter::generate_cg_block_without_batch(ObCGBlockFile &cg_block_file,
                                                        int &micro_block_cnt,
                                                        const bool is_complete_macro_block,
                                                        const ObMergeType merge_type,
                                                        const bool is_encrypt)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/, merge_type, is_encrypt);

  ObMacroDataSeq data_seq(0);
  ObDagTempMacroBlockWriter *dag_temp_macro_writer = new ObDagTempMacroBlockWriter();

  data_seq.set_parallel_degree(0);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = data_seq.macro_data_seq_;
  ObSSTablePrivateObjectCleaner cleaner;

  ObCGBlockFileWriter cg_block_writer;
  ASSERT_EQ(OB_SUCCESS, cg_block_writer.init(&cg_block_file));
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param, cleaner, &cg_block_writer));
  static const int64_t MAX_TEST_COLUMN_CNT = TestDagMacroWriter::TEST_COLUMN_CNT + 3;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;
  int64_t test_row_num = TestDagMacroWriter::TEST_ROW_CNT;
  int64_t test_micro_block_count = TestDagMacroWriter::TEST_MICRO_BLOCK_CNT;
  ObDatumRow row;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestDagMacroWriter::TEST_COLUMN_CNT));
  ASSERT_EQ(false, dag_temp_macro_writer->is_alloc_block_needed());

  int current_index = dag_temp_macro_writer->current_index_;
  ObMacroBlock &current_block = dag_temp_macro_writer->macro_blocks_[current_index];
  while (current_block.get_micro_block_count() < test_micro_block_count - 1) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    convert_to_multi_version_row(row, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, 2, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->append_row(multi_row));
  }
  //build the last micro block
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  convert_to_multi_version_row(row, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, 2, dml, multi_row);
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->append_row(multi_row));
  ASSERT_GT(dag_temp_macro_writer->micro_writer_->get_row_count(), 0);
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->build_micro_block());

  //check the micro_block_count
  ASSERT_EQ(test_micro_block_count, current_block.get_micro_block_count());
  ASSERT_EQ(true, current_block.is_dirty());
  int32_t row_count = current_block.get_row_count();
  micro_block_cnt += current_block.get_micro_block_count();

  //generate cg_block
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->flush_macro_block(current_block, !is_complete_macro_block));
  if (dag_temp_macro_writer->is_need_macro_buffer_) {
    ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->wait_io_finish(dag_temp_macro_writer->macro_handles_[current_index], &current_block));
  }
  ASSERT_EQ(false, current_block.is_dirty());

  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->close());
  delete dag_temp_macro_writer;
}

void TestDagMacroWriter::large_batch_generate_cg_block(ObCGBlockFile &cg_block_file, int &micro_block_cnt)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/);

  ObMacroDataSeq data_seq(0);
  ObDagTempMacroBlockWriter *dag_temp_macro_writer = new ObDagTempMacroBlockWriter();

  data_seq.set_parallel_degree(0);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = data_seq.macro_data_seq_;
  ObSSTablePrivateObjectCleaner cleaner;

  ObCGBlockFileWriter cg_block_writer;
  ASSERT_EQ(OB_SUCCESS, cg_block_writer.init(&cg_block_file));
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param, cleaner, &cg_block_writer));
  static const int64_t MAX_TEST_COLUMN_CNT = TestDagMacroWriter::TEST_COLUMN_CNT + 3;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;
  int64_t test_row_num = TestDagMacroWriter::TEST_ROW_CNT;
  int64_t test_micro_block_count = TestDagMacroWriter::TEST_MICRO_BLOCK_CNT;
  ObDatumRow row;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestDagMacroWriter::TEST_COLUMN_CNT));
  ASSERT_EQ(false, dag_temp_macro_writer->is_alloc_block_needed());

  int full_macro_block_micro_count = 0;
  int current_index = dag_temp_macro_writer->current_index_;
  ObMacroBlock &current_block = dag_temp_macro_writer->macro_blocks_[current_index];
  ASSERT_EQ(0, current_index);
  ASSERT_EQ(0, micro_block_cnt);

  while (current_index == 0) {
    vec_convertor_.reset();
    ASSERT_EQ(OB_SUCCESS, vec_convertor_.init(multi_col_descs_, vec_formats_, TEST_BATCH_SIZE));
    bool is_full = false;
    while(!is_full)
    {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
      convert_to_multi_version_row(row, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, 2, dml, multi_row);
      ASSERT_EQ(OB_SUCCESS, vec_convertor_.append_row(multi_row, is_full));
    }
    ObBatchDatumRows batch_rows;
    ASSERT_EQ(OB_SUCCESS, vec_convertor_.get_batch_datums(batch_rows));
    ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->append_batch(batch_rows));
    current_index = dag_temp_macro_writer->current_index_;
  }
  //build the last micro block in the second macro_block
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  convert_to_multi_version_row(row, TestDagMacroWriter::TEST_ROWKEY_COLUMN_CNT, TestDagMacroWriter::TEST_COLUMN_CNT, 2, dml, multi_row);
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->append_row(multi_row));
  ASSERT_GT(dag_temp_macro_writer->micro_writer_->get_row_count(), 0);
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->build_micro_block());

  //collect the micro_block_count info
  ASSERT_EQ(1, current_index);
  ASSERT_EQ(true, current_block.is_dirty());  // because the is_need_macro_buffer = true, not init macro block now
  full_macro_block_micro_count = current_block.get_micro_block_count();
  ASSERT_GT(full_macro_block_micro_count, 0);
  ObMacroBlock &current_next_block = dag_temp_macro_writer->macro_blocks_[current_index];
  ASSERT_EQ(true, current_next_block.is_dirty());
  micro_block_cnt = full_macro_block_micro_count+current_next_block.get_micro_block_count();
  ASSERT_GT(micro_block_cnt, full_macro_block_micro_count);

  //generate second cg_block
  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->flush_macro_block(current_next_block, true/*is_close_flush*/));
  if (dag_temp_macro_writer->is_need_macro_buffer_) {
    ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->wait_io_finish(dag_temp_macro_writer->macro_handles_[current_index], &current_next_block));
  }
  ASSERT_EQ(false, current_block.is_dirty());

  ASSERT_EQ(OB_SUCCESS, dag_temp_macro_writer->close());
  delete dag_temp_macro_writer;
  STORAGE_LOG(INFO, "successfully write two macro blocks into temp file", K(micro_block_cnt), K(full_macro_block_micro_count));
}

void TestDagMacroWriter::set_master_key(ObWholeDataStoreDesc &data_desc)
{
  ASSERT_NE(ObCipherOpMode::ob_invalid_mode, mode_);
  data_desc.static_desc_.master_key_id_ = master_key_id_;
  data_desc.static_desc_.encrypt_id_ = static_cast<int64_t>(mode_);
  const int64_t copy_len = encrypt_key_len_ < share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH ? encrypt_key_len_ : share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH;
  memcpy(data_desc.static_desc_.encrypt_key_, encrypt_key_, copy_len);
  ObMicroBlockEncryption block_encrypt;
  int ret = block_encrypt.init(static_cast<int64_t>(mode_), tenant_id_, master_key_id_, encrypt_key_, encrypt_key_len_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestDagMacroWriter::prepare_index_builder(ObWholeDataStoreDesc &data_desc,
                                          ObSSTableIndexBuilder &sstable_builder,
                                          const bool need_submit_io,
                                          const ObMergeType merge_type,
                                          const bool is_encrypt)
{
  int ret = OB_SUCCESS;
  prepare_data_desc(data_desc, &sstable_builder, need_submit_io, merge_type, is_encrypt);
  ret = sstable_builder.init(data_desc.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestDagMacroWriter::prepare_data_desc(ObWholeDataStoreDesc &data_desc,
                                      ObSSTableIndexBuilder *sstable_builder,
                                      const bool need_submit_io,
                                      const ObMergeType merge_type,
                                      const bool is_encrypt)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_version = ObTimeUtility::fast_current_time();
  SCN end_scn;
  end_scn.convert_for_tx(snapshot_version);
  table_schema_.enable_macro_block_bloom_filter_ = true;
  ret = data_desc.init(false/*is_ddl*/, table_schema_, ObLSID(1), ObTabletID(1), merge_type,
                       ObTimeUtility::fast_current_time()/*snapshot_version*/, DATA_CURRENT_VERSION,
                       table_schema_.get_micro_index_clustered(), 0/*transfer_seq*/, 0/*concurrent_cnt*/,
                       end_scn, end_scn, nullptr, 0/*table_cg_idx*/,
                       compaction::ObExecMode::EXEC_MODE_LOCAL/*exec_mode*/, need_submit_io);
  if (is_encrypt) {
    set_master_key(data_desc);
  }

  data_desc.get_desc().sstable_index_builder_ = sstable_builder;
  ASSERT_EQ(OB_SUCCESS, ret);
}


void TestDagMacroWriter::prepare_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  index_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_concurrency_defenses"));
  ASSERT_EQ(OB_SUCCESS, index_schema_.set_table_name("test_concurrency_defenses"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(ENCODING_ROW_STORE);

  index_schema_.set_tenant_id(1);
  index_schema_.set_tablegroup_id(1);
  index_schema_.set_database_id(1);
  index_schema_.set_table_id(table_id);
  index_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  index_schema_.set_max_used_column_id(TEST_ROWKEY_COLUMN_CNT + 1);
  index_schema_.set_block_size(micro_block_size);
  index_schema_.set_compress_func_name("none");
  index_schema_.set_row_store_type(ENCODING_ROW_STORE);
  //init column
  ObArray<share::schema::ObColDesc> out_cols;
  const int64_t schema_version = 1;
  const int64_t rowkey_count = TEST_ROWKEY_COLUMN_CNT;
  const int64_t column_count = TEST_COLUMN_CNT;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    if(i == TEST_ROWKEY_COLUMN_CNT){
      ASSERT_EQ(OB_SUCCESS, vec_formats_.push_back(VectorFormat::VEC_FIXED));
      ASSERT_EQ(OB_SUCCESS, vec_formats_.push_back(VectorFormat::VEC_FIXED));
    }
    ASSERT_EQ(OB_SUCCESS, vec_formats_.push_back(VectorFormat::VEC_FIXED));
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_data_length(1);
    if(obj_type == common::ObInt32Type) {
      column.set_rowkey_position(1);
      column.set_order_in_rowkey(ObOrderType::ASC);
    } else if(obj_type == common::ObFloatType) {
      column.set_rowkey_position(2);
      column.set_order_in_rowkey(ObOrderType::DESC);
    } else {
      column.set_rowkey_position(0);
    }
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(obj_type);
    ASSERT_EQ(OB_SUCCESS, out_cols.push_back(col));
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    if (column.is_rowkey_column()) {
      ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
    }
  }
  column.reset();
  column.set_table_id(table_id);
  column.set_column_id(TEST_COLUMN_CNT +OB_APP_MIN_COLUMN_ID);
  column.set_data_type(ObVarcharType);
  column.set_collation_type(CS_TYPE_BINARY);
  column.set_data_length(1);
  column.set_rowkey_position(0);
  ASSERT_EQ(OB_SUCCESS, index_schema_.add_column(column));
}

void TestDagMacroWriter::random_change_cgblock_micro_block_id(ObCGBlock *cg_block, int64_t &actual_micro_block_count)
{
  static common::ObRandom RND;
  int64_t micro_id = RND.get(1, TestDagMacroWriter::TEST_MICRO_BLOCK_CNT-1);
  actual_micro_block_count += (TestDagMacroWriter::TEST_MICRO_BLOCK_CNT-micro_id);
  ObDagMicroBlockIterator micro_block_iter;
  ASSERT_EQ(0, cg_block->get_cg_block_offset());
  ASSERT_EQ(0, cg_block->get_micro_block_idx());
  ASSERT_EQ(OB_SUCCESS, micro_block_iter.open(cg_block->get_macro_block_buffer(),
                                              cg_block->get_macro_buffer_size(),
                                              false));
  ObMicroBlockDesc micro_block_desc;
  ObMicroIndexData micro_index_data;
  compaction::ObLocalArena allocator("TDMaBlkWriter");
  while (micro_id--) {
    allocator.reuse();
    ASSERT_EQ(OB_SUCCESS, micro_block_iter.get_next_micro_block_desc(micro_block_desc, micro_index_data, allocator));
  }
  cg_block->set_cg_block_offset(micro_block_iter.read_pos_);
  cg_block->set_micro_block_idx(micro_block_iter.iter_idx_);
}

void TestDagMacroWriter::aggregate_cg_blocks_into_macro_block(ObCGBlockFilesIterator &cg_block_files_iter,
                                                              const int64_t expected_all_micro_block_cnt,
                                                              const ObMergeType merge_type,
                                                              const bool is_encrypt,
                                                              const uint64_t count)
{
  compaction::ObLocalArena allocator("TDMaBlkWriter");
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/, merge_type, is_encrypt);

  ObMacroDataSeq data_seq(0);
  ObDagMacroBlockWriter *dag_macro_writer = new ObDagMacroBlockWriter();

  data_seq.set_parallel_degree(0);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = data_seq.macro_data_seq_;
  ObSSTablePrivateObjectCleaner cleaner;

  int64_t curr_count = 0;
  ObDDLRedoLogWriterCallback ddl_redo_callback;
  ASSERT_EQ(OB_SUCCESS, dag_macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param,
                                               cleaner, &ddl_redo_callback));
  int ret = OB_SUCCESS;
  // The total count of micro blocks in all cgblocks
  int64_t all_micro_block_count = 0;
  // Since the test randomly selects some starting points of micro blocks in cgblocks and writes them into macro blocks,
  // we need to count the actual number of micro blocks that have been written into macro blocks.
  int64_t actual_micro_block_count = 0;
  while (OB_SUCC(ret) && curr_count++ < count) {
    ObCGBlock *cg_block = nullptr;
    if (OB_ISNULL(cg_block = OB_NEWx(ObCGBlock, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to build cg block", K(ret));
    } else if (OB_FAIL(cg_block_files_iter.get_next_cg_block(*cg_block))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("finish get all cg block using iter", K(ret));
        break;
      } else {
        LOG_WARN("fail to get next cg block using iter", K(ret));
      }
    } else {
      random_change_cgblock_micro_block_id(cg_block, actual_micro_block_count);
      ASSERT_EQ(OB_SUCCESS, dag_macro_writer->append_cg_block(*cg_block, ObDagMacroBlockWriter::DEFAULT_MACRO_BLOCK_FILL_RATIO));
      ASSERT_GT(cg_block->get_cg_block_offset(), 0);
      ASSERT_GT(cg_block->get_micro_block_idx(), 0);
      ObDagMicroBlockIterator micro_block_iter;
      ASSERT_EQ(OB_SUCCESS, micro_block_iter.open(cg_block->get_macro_block_buffer(),
                                                  cg_block->get_macro_buffer_size(),
                                                  false));
      all_micro_block_count += (micro_block_iter.end_idx_+1);
      ASSERT_EQ(cg_block->get_micro_block_idx(), (micro_block_iter.end_idx_+1));
    }
  }
  int current_index = dag_macro_writer->current_index_;
  ObMacroBlock &current_block = dag_macro_writer->macro_blocks_[current_index];
  ASSERT_EQ(actual_micro_block_count, current_block.get_micro_block_count());
  ASSERT_EQ(OB_SUCCESS, dag_macro_writer->close());
  delete dag_macro_writer;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(all_micro_block_count, expected_all_micro_block_cnt);
  STORAGE_LOG(INFO, "successfully write the macro block", K(actual_micro_block_count), K(all_micro_block_count));
}

void TestDagMacroWriter::test_buf_not_enough(ObCGBlockFilesIterator &cg_block_files_iter,
                                             const bool check_if_complete_macro_block,
                                             const uint64_t count)
{
  compaction::ObLocalArena allocator("TDMaBlkWriter");
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/);

  ObMacroDataSeq data_seq(0);
  ObDagMacroBlockWriter *dag_macro_writer = new ObDagMacroBlockWriter();

  data_seq.set_parallel_degree(0);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = data_seq.macro_data_seq_;
  ObSSTablePrivateObjectCleaner cleaner;

  int64_t curr_count = 0;
  ObDDLRedoLogWriterCallback ddl_redo_callback;
  ASSERT_EQ(OB_SUCCESS, dag_macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param,
                                               cleaner, &ddl_redo_callback));
  int ret = OB_SUCCESS;
  int micro_block_count = 0;
  int cg_block_id = 0;
  while (OB_SUCC(ret) && curr_count++ < count) {
    ObCGBlock *cg_block = nullptr;
    if (OB_ISNULL(cg_block = OB_NEWx(ObCGBlock, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to build cg block", K(ret));
    } else if (OB_FAIL(cg_block_files_iter.get_next_cg_block(*cg_block))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("finish get all cg block using iter", K(ret));
        break;
      } else {
        LOG_WARN("fail to get next cg block using iter", K(ret));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    if (check_if_complete_macro_block && 0 == cg_block_id) {
      ASSERT_EQ(true, cg_block->is_complete_macro_block());
      cg_block_id++;
    }
    if (OB_FAIL(dag_macro_writer->append_cg_block(*cg_block, ObDagMacroBlockWriter::DEFAULT_MACRO_BLOCK_FILL_RATIO))) {
      if (ret == OB_BUF_NOT_ENOUGH) {
        ASSERT_EQ(ObDagMacroBlockWriter::ObDagMacroWriterStage::WAITTING_CLOSE, dag_macro_writer->get_dag_stage());
        int current_index = dag_macro_writer->current_index_;
        ObMacroBlock &current_block = dag_macro_writer->macro_blocks_[current_index];
        STORAGE_LOG(INFO, "macro block can not accommodate more micro blocks.", K(ret), K(current_block.get_data_size()));
      }
    } else {
      ASSERT_GT(cg_block->get_cg_block_offset(), 0);
      ASSERT_GT(cg_block->get_micro_block_idx(), 0);
      ObDagMicroBlockIterator micro_block_iter;
      ASSERT_EQ(OB_SUCCESS, (micro_block_iter.open(cg_block->get_macro_block_buffer(),
                                                   cg_block->get_macro_buffer_size(),
                                                   false)));
      micro_block_count += (micro_block_iter.end_idx_+1);
      ASSERT_EQ(cg_block->get_micro_block_idx(), (micro_block_iter.end_idx_+1));
    }
  }
  ASSERT_EQ(OB_SUCCESS, dag_macro_writer->close());
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
}

void TestDagMacroWriter::test_reuse_macro_block(ObCGBlockFilesIterator &cg_block_files_iter,
                                                              const uint64_t count)
{

}

TEST_F(TestDagMacroWriter, test_dag_macro_writer)
{
  LOG_INFO("BEGIN TestDagMacroWriter.test_dag_macro_writer");

  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  ObCGBlockFile *cg_block_file_1 = nullptr;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
  int micro_block_cnt = 0;
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(9), 9, 9, 9));
  generate_cg_block(*cg_block_file_0, micro_block_cnt, false);
  EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(10), 10, 10, 10));
  generate_cg_block(*cg_block_file_1, micro_block_cnt, false);

  ObCGBlockFilesIterator cg_block_files_iter;
  ObArray<ObCGBlockFile *> cg_block_files;
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));

  aggregate_cg_blocks_into_macro_block(cg_block_files_iter, micro_block_cnt);

  LOG_INFO("FINISH TestDagMacroWriter.test_dag_macro_writer");
}

TEST_F(TestDagMacroWriter, test_dag_macro_writer_reuse_micro_block)
{
  LOG_INFO("BEGIN TestDagMacroWriter.test_dag_macro_writer_reuse_micro_block");

  ObMemAttr attr(MTL_ID(), "cg_blok_file");

  int micro_block_cnt = 0;
  ObCGBlockFilesIterator cg_block_files_iter;
  ObArray<ObCGBlockFile *> cg_block_files;
  int cg_block_file_cnt = 15;
  int no = 11;
  for (int i = 0; i<cg_block_file_cnt; i++) {
    ObCGBlockFile *cg_block_file_tmp = nullptr;
    cg_block_file_tmp = OB_NEW(ObCGBlockFile, attr);
    EXPECT_EQ(OB_SUCCESS, cg_block_file_tmp->open(ObTabletID(no), no, no, no));
    generate_cg_block(*cg_block_file_tmp, micro_block_cnt, false/*is_complete_macro_block*/);
    no++;
    EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_tmp));
  }
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));
  test_buf_not_enough(cg_block_files_iter, false);

  LOG_INFO("FINISH TestDagMacroWriter.test_dag_macro_writer_reuse_micro_block");
}

TEST_F(TestDagMacroWriter, test_dag_phase1_large_batch)
{
  LOG_INFO("BEGIN TestDagMacroWriter.test_dag_phase1_large_batch");

  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  int micro_block_cnt = 0;
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(6), 6, 6, 6));
  large_batch_generate_cg_block(*cg_block_file_0, micro_block_cnt);

  ObCGBlockFilesIterator cg_block_files_iter;
  ObArray<ObCGBlockFile *> cg_block_files;
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));
  test_buf_not_enough(cg_block_files_iter, true);

  LOG_INFO("FINISH TestDagMacroWriter.test_dag_phase1_large_batch");
}

TEST_F(TestDagMacroWriter, test_dag_macro_writer_encrypt_data)
{
  LOG_INFO("BEGIN TestDagMacroWriter.test_dag_macro_writer_encrypt_data");
  if (share::ObMasterKeyGetter::instance().is_inited_) {
    share::ObMasterKeyGetter::instance().destroy();
  }
  ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().init(nullptr));
  ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().set_master_key(
    tenant_id_, master_key_id_, master_key_, strlen(master_key_)));
  ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().get_table_key_algorithm(tenant_id_, mode_));
  ASSERT_NE(ObCipherOpMode::ob_invalid_mode, mode_);
  ASSERT_EQ(OB_SUCCESS, share::ObBlockCipher::encrypt(master_key_, strlen(master_key_), raw_key_,
                        strlen(raw_key_), OB_MAX_ENCRYPTION_KEY_NAME_LENGTH, nullptr, 0, nullptr, 0,
                        0, mode_, encrypt_key_, encrypt_key_len_, nullptr));

  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  ObCGBlockFile *cg_block_file_1 = nullptr;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
  int micro_block_cnt = 0;
  const ObMergeType merge_type = ObMergeType::MAJOR_MERGE;
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(9), 9, 9, 9));
  generate_cg_block(*cg_block_file_0, micro_block_cnt, false, merge_type, true);
  EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(10), 10, 10, 10));
  generate_cg_block(*cg_block_file_1, micro_block_cnt, false, merge_type, true);

  ObCGBlockFilesIterator cg_block_files_iter;
  ObArray<ObCGBlockFile *> cg_block_files;
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));

  //major merge will generate logic id
  aggregate_cg_blocks_into_macro_block(cg_block_files_iter, micro_block_cnt, merge_type, true);

  LOG_INFO("FINISH TestDagMacroWriter.test_dag_macro_writer_encrypt_data");
}

TEST_F(TestDagMacroWriter, test_macro_writer_logic_id)
{
  LOG_INFO("BEGIN TestDagMacroWriter.test_macro_writer_logic_id");

  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  ObCGBlockFile *cg_block_file_1 = nullptr;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
  int micro_block_cnt = 0;
  const ObMergeType merge_type = ObMergeType::MINOR_MERGE;
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(9), 9, 9, 9));
  generate_cg_block_without_batch(*cg_block_file_0, micro_block_cnt, false, merge_type);
  EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(10), 10, 10, 10));
  generate_cg_block_without_batch(*cg_block_file_1, micro_block_cnt, false, merge_type);

  ObCGBlockFilesIterator cg_block_files_iter;
  ObArray<ObCGBlockFile *> cg_block_files;
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));

  //minor merge will not generate logic id
  aggregate_cg_blocks_into_macro_block(cg_block_files_iter, micro_block_cnt, merge_type);

  LOG_INFO("FINISH TestDagMacroWriter.test_macro_writer_logic_id");
}

TEST_F(TestDagMacroWriter, test_reuse_macro_block_writer)  // not reuse macro block
{
  LOG_INFO("BEGIN TestDagMacroWriter.test_reuse_macro_writer");
  ObWholeDataStoreDesc data_desc;
  ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
  prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/, ObMergeType::MAJOR_MERGE, false/* is_encrypt */);

  ObMacroDataSeq data_seq(0);
  ObMacroBlockWriter *macro_writer = new ObMacroBlockWriter(true/*is_need_macro_buffer*/);

  data_seq.set_parallel_degree(0);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = data_seq.macro_data_seq_;
  ObSSTablePrivateObjectCleaner cleaner;
  const ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  ASSERT_EQ(OB_SUCCESS, macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param, pre_warm_param, cleaner, nullptr, nullptr, nullptr));
  static const int64_t MAX_TEST_COLUMN_CNT = TestDagMacroWriter::TEST_COLUMN_CNT + 3;
  ObDatumRow multi_row;
  ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ObDmlFlag dml = DF_INSERT;
  int64_t test_row_num = TestDagMacroWriter::TEST_ROW_CNT;
  int64_t test_micro_block_count = TestDagMacroWriter::TEST_MICRO_BLOCK_CNT;
  ObDatumRow row;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestDagMacroWriter::TEST_COLUMN_CNT));
  for (int64_t i = 0; i < test_row_num; ++i) {
    OK(row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, macro_writer->append_row(multi_row));
  }
  ASSERT_EQ(OB_SUCCESS, macro_writer->close());
  macro_writer->reset();
  ASSERT_EQ(OB_SUCCESS, macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param, pre_warm_param, cleaner, nullptr, nullptr, nullptr));
  for (int64_t i = test_row_num; i < test_row_num * 2; ++i) {
    OK(row_generate_.get_next_row(i, row));
    convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
    ASSERT_EQ(OB_SUCCESS, macro_writer->append_row(multi_row));
  }
  ASSERT_EQ(OB_SUCCESS, macro_writer->close());
  macro_writer = nullptr;
  LOG_INFO("FINISH TestDagMacroWriter.test_reuse_macro_writer");
}

// TEST_F(TestDagMacroWriter, test_dag_slice_macro_flusher)
// {
//   LOG_INFO("BEGIN TestDagMacroWriter.test_dag_slice_macro_flusher");

//   ObWholeDataStoreDesc data_desc;
//   ObSSTableIndexBuilder sstable_builder(false /* not need writer buffer*/);
//   prepare_index_builder(data_desc, sstable_builder, true/*need_submit_io*/);

//   ObMacroDataSeq data_seq(0);
//   ObMacroBlockWriter *macro_writer = new ObMacroBlockWriter(true/*is_need_macro_buffer*/);

//   data_seq.set_parallel_degree(0);
//   ObMacroSeqParam seq_param;
//   seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
//   seq_param.start_ = data_seq.macro_data_seq_;
//   ObSSTablePrivateObjectCleaner cleaner;

//   int64_t curr_count = 0;
//   ObDDLRedoLogWriterCallback ddl_redo_callback;
//   const ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
//   ASSERT_EQ(OB_SUCCESS, macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param,
//                                            pre_warm_param, cleaner, &ddl_redo_callback));
//   static const int64_t MAX_TEST_COLUMN_CNT = TestDagMacroWriter::TEST_COLUMN_CNT + 3;
//   ObDatumRow multi_row;
//   ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));
//   ObDmlFlag dml = DF_INSERT;
//   int64_t test_row_num = TestDagMacroWriter::TEST_ROW_CNT;
//   ObDatumRow row;
//   ObArenaAllocator allocator;
//   ASSERT_EQ(OB_SUCCESS, row.init(allocator, TestDagMacroWriter::TEST_COLUMN_CNT));
//   for (int64_t i = 0; i < test_row_num; ++i) {
//     OK(row_generate_.get_next_row(i, row));
//     convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
//     ASSERT_EQ(OB_SUCCESS, macro_writer->append_row(multi_row));
//   }

//   ObMemAttr attr(MTL_ID(), "cg_blok_file");
//   ObCGBlockFile *cg_block_file = nullptr;
//   cg_block_file = OB_NEW(ObCGBlockFile, attr);
//   int micro_block_cnt = 0;
//   EXPECT_EQ(OB_SUCCESS, cg_block_file->open(ObTabletID(23), 23, 23, 23));
//   ObCGBlockFileWriter cg_block_writer;
//   ASSERT_EQ(OB_SUCCESS, cg_block_writer.init(cg_block_file));
//   ObDagSliceMacroFlusher *dag_slice_macro_flusher = new ObDagSliceMacroFlusher(cg_block_writer);
//   ASSERT_EQ(OB_SUCCESS, macro_writer->close(dag_slice_macro_flusher));
//   ASSERT_EQ(OB_SUCCESS, macro_writer->open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param, pre_warm_param, cleaner, nullptr, nullptr, nullptr));

//   for (int64_t i = test_row_num; i < test_row_num * 2; ++i) {
//     OK(row_generate_.get_next_row(i, row));
//     convert_to_multi_version_row(row, table_schema_.get_rowkey_column_num(), table_schema_.get_column_count(), SNAPSHOT_VERSION, dml, multi_row);
//     ASSERT_EQ(OB_SUCCESS, macro_writer->append_row(multi_row));
//   }
//   ASSERT_EQ(OB_SUCCESS, macro_writer->close());
//   ObSSTableMergeRes res;
//   ASSERT_EQ(OB_SUCCESS, sstable_builder.close(res));

//   LOG_INFO("FINISH TestDagMacroWriter.test_dag_slice_macro_flusher");
// }

// TEST_F(TestDagMacroWriter, test_dag_macro_writer_reuse_macro_block)
// {
//   LOG_INFO("BEGIN TestDagMacroWriter.test_dag_macro_writer_reuse_macro_block");

//   ObMemAttr attr(MTL_ID(), "cg_blok_file");
//   ObCGBlockFile *cg_block_file_0 = nullptr;
//   ObCGBlockFile *cg_block_file_1 = nullptr;
//   cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
//   cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
//   int micro_block_cnt = 0;
//   EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(4), 4, 4, 4));
//   generate_cg_block(*cg_block_file_0, micro_block_cnt, true);
//   EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(5), 5, 5, 5));
//   generate_cg_block(*cg_block_file_1, micro_block_cnt, true);

//   ObCGBlockFilesIterator cg_block_files_iter;
//   ObArray<ObCGBlockFile *> cg_block_files;
//   EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
//   EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
//   EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.init(cg_block_files));

//   test_reuse_macro_block(cg_block_files_iter);

//   LOG_INFO("FINISH TestDagMacroWriter.test_dag_macro_writer_reuse_macro_block");
// }

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_dag_macro_writer.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_dag_macro_writer.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
