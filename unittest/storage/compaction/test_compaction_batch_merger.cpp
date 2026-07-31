/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include <gtest/gtest.h>

#define protected public
#define private public
#include "storage/compaction/vectorization/ob_compaction_batch_merger.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace share::schema;

namespace compaction
{
namespace
{

class ReusingEncodingReader : public ObIMicroBlockReader
{
public:
  ReusingEncodingReader()
    : header_(),
      payload_()
  {
    reader_type_ = Decoder;
    header_.extend_value_bit_ = 2;
  }

  int get_row(const int64_t index, ObDatumRow &row) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(index < 0 || index >= 2 || row.get_capacity() < 1)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      row.reuse();
      MEMSET(payload_, 0 == index ? 'A' : 'B', sizeof(payload_));
      if (OB_FAIL(row.storage_datums_[0].from_buf_enhance(payload_, sizeof(payload_)))) {
      } else {
        row.count_ = 1;
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      }
    }
    return ret;
  }

  bool has_lob_out_row() const override { return false; }
  const ObMicroBlockHeader *get_micro_header() const override { return &header_; }
  int64_t get_column_count() const override { return 1; }

private:
  ObMicroBlockHeader header_;
  char payload_[128];
};

void prepare_vector_store(
    ObMergeVectorStore &store,
    const ObStaticMergeParam &static_param,
    ObMergeVectorStoreLayoutParam &layout_param)
{
  ObColDesc col_desc;
  ObSEArray<ObColDesc, 1> col_descs;
  col_desc.col_id_ = 1;
  col_desc.col_type_.set_int();
  ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));

  layout_param.column_count_ = 1;
  layout_param.rowkey_column_cnt_ = 0;
  layout_param.static_param_ = &static_param;
  ASSERT_EQ(OB_SUCCESS, store.init(
      layout_param, col_descs, false /*is_continuous*/));
}

void prepare_string_vector_store(
    ObMergeVectorStore &store,
    const ObStaticMergeParam &static_param,
    ObMergeVectorStoreLayoutParam &layout_param)
{
  ObColDesc col_desc;
  ObSEArray<ObColDesc, 1> col_descs;
  col_desc.col_id_ = 1;
  col_desc.col_type_.set_varchar();
  ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));

  layout_param.column_count_ = 1;
  layout_param.rowkey_column_cnt_ = 0;
  layout_param.static_param_ = &static_param;
  ASSERT_EQ(OB_SUCCESS, store.init(
      layout_param, col_descs, false /*is_continuous*/));
}

TEST(ObCompactionBatchMergerTest, co_reset_clears_batch_scan_state)
{
  ObTabletMergeDagParam dag_param;
  ObStaticMergeParam static_param(dag_param);
  ObLocalArena arena("BatchMergeUT");
  ObCOBatchMergeLogBuilder builder(arena, static_param);
  builder.batch_scan_iter_ = reinterpret_cast<ObPartitionMergeIter *>(1);
  builder.border_rowkey_.set_max_rowkey();
  builder.need_move_minor_iter_ = ObCOMergeLogBuilder::MoveNextOp::NEED_MOVE_NEXT;
  builder.need_move_major_iter_ = ObCOMergeLogBuilder::MoveNextOp::ONLY_REBUILD;

  builder.reset();
  ASSERT_EQ(nullptr, builder.batch_scan_iter_);
  ASSERT_FALSE(builder.border_rowkey_.is_valid());
  ASSERT_EQ(ObCOMergeLogBuilder::MoveNextOp::DO_NOTHING, builder.need_move_minor_iter_);
  ASSERT_EQ(ObCOMergeLogBuilder::MoveNextOp::DO_NOTHING, builder.need_move_major_iter_);
}

TEST(ObCompactionBatchMergerTest, encoding_nop_fallback_owns_scalar_payload)
{
  ObTabletMergeDagParam dag_param;
  ObStaticMergeParam static_param(dag_param);
  static_param.compaction_batch_size_ = 3;
  ObMergeVectorStoreLayoutParam layout_param;
  ObMergeVectorStore store;
  prepare_string_vector_store(store, static_param, layout_param);
  ASSERT_FALSE(store.is_continuous());
  ReusingEncodingReader reader;

  int64_t begin_index = 0;
  ASSERT_EQ(OB_SUCCESS, store.fill_rows_from_reader(nullptr, reader, begin_index, 2));
  ASSERT_EQ(2, begin_index);
  ASSERT_EQ(2, store.get_row_count());

  ObDatumRow output;
  ASSERT_EQ(OB_SUCCESS, output.init(1));
  ASSERT_EQ(OB_SUCCESS, store.get_datum_row(0, output));
  ASSERT_EQ(128, output.storage_datums_[0].len_);
  for (int64_t i = 0; i < output.storage_datums_[0].len_; ++i) {
    ASSERT_EQ('A', output.storage_datums_[0].ptr_[i]);
  }
  ASSERT_EQ(OB_SUCCESS, store.get_datum_row(1, output));
  for (int64_t i = 0; i < output.storage_datums_[0].len_; ++i) {
    ASSERT_EQ('B', output.storage_datums_[0].ptr_[i]);
  }
}

TEST(ObCompactionBatchMergerTest, odd_capacity_aligns_pointer_scratch)
{
  ObTabletMergeDagParam dag_param;
  ObStaticMergeParam static_param(dag_param);
  static_param.compaction_batch_size_ = 3;
  ObMergeVectorStoreLayoutParam layout_param;
  ObMergeVectorStore store;
  prepare_vector_store(store, static_param, layout_param);

  ASSERT_NE(nullptr, store.cell_data_ptrs_);
  ASSERT_EQ(0, reinterpret_cast<uintptr_t>(store.cell_data_ptrs_) % alignof(char *));
}

TEST(ObCompactionBatchMergerTest, single_row_projector_uses_source_row_width)
{
  ObTabletMergeDagParam dag_param;
  ObStaticMergeParam static_param(dag_param);
  static_param.compaction_batch_size_ = 3;
  ObMergeVectorStoreLayoutParam layout_param;
  ObMergeVectorStore store;
  prepare_vector_store(store, static_param, layout_param);

  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(3));
  for (int64_t i = 0; i < source.count_; ++i) {
    source.storage_datums_[i].set_int(10 + i);
  }
  source.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  ASSERT_EQ(OB_SUCCESS, store.set_single_row(&source));

  uint16_t projector_data[] = {2};
  ObArrayWrap<uint16_t> projector(projector_data, ARRAYSIZEOF(projector_data));
  ObDatumRow output;
  ASSERT_EQ(OB_SUCCESS, output.init(1));
  ASSERT_EQ(OB_SUCCESS, store.get_datum_row(0, output, &projector));
  ASSERT_EQ(12, output.storage_datums_[0].get_int());
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.get_datum_row(0, output));
}

} // namespace
} // namespace compaction
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_compaction_batch_merger.log*");
  OB_LOGGER.set_file_name("test_compaction_batch_merger.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
