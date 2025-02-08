// owner: baichangmin.bcm
// owner group: storage

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

#define USING_LOG_PREFIX STORAGE

#define private public
#define protected public

#include "src/storage/blocksstable/index_block/ob_index_block_tree_cursor.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
class TestIndexDumper : public TestIndexBlockDataPrepare
{
public:
  TestIndexDumper();
  virtual ~TestIndexDumper() {}
  virtual void SetUp();
  virtual void TearDown();
  bool is_equal(ObMicroBlockDesc &l, ObMicroBlockDesc &r);
};

TestIndexDumper::TestIndexDumper()
  : TestIndexBlockDataPrepare(
      "Test index dumper",
      MAJOR_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65535 * 4)
{
}

void TestIndexDumper::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestIndexDumper::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

bool TestIndexDumper::is_equal(ObMicroBlockDesc &l, ObMicroBlockDesc &r)
{
  bool res = true;
  if (!(l.last_rowkey_ == r.last_rowkey_)) {
    res = false;
  } else if (l.buf_size_ != r.buf_size_ || l.data_size_ != r.data_size_
      || l.original_size_ != r.original_size_ || l.row_count_ != r.row_count_
      || l.column_count_ != r.column_count_ || l.max_merged_trans_version_ != r.max_merged_trans_version_
      || l.macro_id_ != r.macro_id_ || l.block_offset_ != r.block_offset_
      || l.block_checksum_ != r.block_checksum_ || l.row_count_delta_ != r.row_count_delta_
      || l.contain_uncommitted_row_ != r.contain_uncommitted_row_ || l.can_mark_deletion_ != r.can_mark_deletion_
      || l.has_string_out_row_ != r.has_string_out_row_ || l.has_lob_out_row_ != r.has_lob_out_row_
      || l.is_last_row_last_flag_ != r.is_last_row_last_flag_) {
        res = false;
  } else if (l.header_->header_checksum_ != r.header_->header_checksum_
      || l.header_->data_checksum_ != r.header_->data_checksum_){
        res = false;
  }

  if (!res) {
  } else if (l.header_->has_column_checksum_) {
    for (int64_t i = 0; i < l.header_->column_count_; ++i) {
      if(l.header_->column_checksums_[i] != r.header_->column_checksums_[i]) {
        res = false;
        break;
      }
    }
  }

  if (!res) {
  } else if (0 != MEMCMP(l.buf_, r.buf_, l.buf_size_)) {
    res = false;
    OB_LOG(INFO, "buf mismatch", K(*l.header_), K(*r.header_));
  }

  return res;
}

static int print_meta_root(const char* buf, const int64_t size, const int64_t rk_col_cnt)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData micro_data;
  micro_data.buf_ = buf;
  micro_data.size_ = size;
  ObArenaAllocator my_allocator;
  ObMicroBlockReaderHelper micro_reader_helper;
  ObIMicroBlockReader *my_reader = nullptr;
  const ObMicroBlockHeader *micro_header = reinterpret_cast<const ObMicroBlockHeader *>(micro_data.get_buf());
  ObDatumRow my_row;
  const int64_t request_col_cnt = rk_col_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() + 1;
  ObDataMacroBlockMeta my_macro_meta;

  if (OB_FAIL(micro_reader_helper.init(my_allocator))) {
    LOG_ERROR("cooper init micro reader helper", K(ret));
  } else if (OB_FAIL(micro_reader_helper.get_reader(micro_data.get_store_type(), my_reader))) {
    LOG_ERROR("cooper get reader", K(ret));
  } else if (OB_FAIL(my_reader->init(micro_data, nullptr))) {
    LOG_ERROR("cooper init reader", K(ret));
  } else if (OB_FAIL(my_row.init(my_allocator, request_col_cnt))) {
    LOG_ERROR("cooper init row", K(ret));
  } else if (OB_FAIL(my_reader->get_row(0, my_row))) {
    LOG_ERROR("cooper get row", K(ret));
  } else if (OB_FAIL(my_macro_meta.parse_row(my_row))) {
    LOG_ERROR("cooper parse row", K(ret));
  } else {
    FLOG_INFO("cooper my meta", K(my_macro_meta));
  }
  return ret;
}

static int print_meta_root(const ObIndexBlockInfo& index_block_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator my_allocator;
  ObIndexBlockLoader my_loader;
  ObDatumRow my_row;
  ObDataMacroBlockMeta my_macro_meta;

  if (OB_FAIL(my_loader.init(my_allocator, CLUSTER_CURRENT_VERSION))) {
    LOG_ERROR("cooper init loader", K(ret));
  } else if (OB_FAIL(my_loader.open(index_block_info))) {
    LOG_ERROR("cooper open loader", K(ret));
  } else if (OB_FAIL(my_row.init(my_allocator, index_block_info.micro_block_desc_->column_count_))) {
    LOG_ERROR("cooper init row", K(ret));
  } else if (OB_FAIL(my_loader.get_next_row(my_row))) {
    LOG_ERROR("cooper get row", K(ret));
  } else if (OB_FAIL(my_macro_meta.parse_row(my_row))) {
    LOG_ERROR("cooper parse row", K(ret), K(my_row));
  } else {
    FLOG_INFO("cooper my meta", K(my_macro_meta));
  }
  return ret;
}

TEST_F(TestIndexDumper, test_deep_copy_micro)
{
  ObIMicroBlockWriter *micro_writer;
  ObDataStoreDesc mem_desc;
  ASSERT_EQ(OB_SUCCESS, mem_desc.shallow_copy(root_index_builder_->index_store_desc_.get_desc()));
  mem_desc.micro_block_size_ = mem_desc.get_micro_block_size_limit();
  ASSERT_EQ(OB_SUCCESS, ObMacroBlockWriter::build_micro_writer(&mem_desc, allocator_, micro_writer));
  ObDatumRow leaf_row;
  ASSERT_EQ(OB_SUCCESS, leaf_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));

  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.build_row(leaf_row, allocator_, CLUSTER_CURRENT_VERSION));
      ASSERT_EQ(OB_SUCCESS, micro_writer->append_row(leaf_row));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  ObMicroBlockDesc original_micro_block;
  ObMicroBlockDesc dst_micro_block;
  ASSERT_EQ(OB_SUCCESS, micro_writer->build_micro_block_desc(original_micro_block));
  ASSERT_EQ(OB_SUCCESS, data_macro_meta.end_key_.deep_copy(original_micro_block.last_rowkey_, allocator_));

  ASSERT_EQ(OB_SUCCESS, original_micro_block.deep_copy(allocator_, dst_micro_block));
  ASSERT_TRUE(is_equal(original_micro_block, dst_micro_block));
}

TEST_F(TestIndexDumper, append_single_meta)
{
  ObBaseIndexBlockDumper dumper;
  const ObDataStoreDesc &desc = root_index_builder_->index_store_desc_.get_desc();
  // ASSERT_EQ(OB_SUCCESS, mem_desc.shallow_copy(root_index_builder_->index_store_desc_.get_desc()));
  // mem_desc.micro_block_size_ = mem_desc.get_micro_block_size_limit();
  ASSERT_EQ(OB_SUCCESS, dumper.init(desc, root_index_builder_->container_store_desc_, desc.sstable_index_builder_, allocator_, allocator_, true));

  common::ObArenaAllocator row_allocator;
  ObDatumRow meta_row;
  ASSERT_EQ(OB_SUCCESS, meta_row.init(row_allocator, TEST_ROWKEY_COLUMN_CNT + 3));

  common::ObArenaAllocator num_allocator;
  number::ObNumber num_837;
  ASSERT_EQ(OB_SUCCESS, num_837.from((int64_t)837, num_allocator));
  meta_row.storage_datums_[0].set_number(num_837);

  const char char_n[8] =  {'N','\0'};
  ObString str_n(8, char_n);
  meta_row.storage_datums_[1].set_string(str_n);
  meta_row.storage_datums_[2].set_int(-1718463383265177000);
  meta_row.storage_datums_[3].set_int(0);

  ObDataBlockMetaVal val;
  val.version_ = ObDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION_V2;
  val.rowkey_count_ = 4;
  val.column_count_ = 7;
  val.micro_block_count_ = 1;
  val.occupy_size_ = 100;
  val.data_size_ = 100;
  val.data_zsize_ = 100;
  val.original_size_ = 100;
  val.progressive_merge_round_ = 0;
  val.block_offset_ = 0;
  val.block_size_ = 128;
  val.row_count_ = 1;
  val.max_merged_trans_version_ = 198;
  val.compressor_type_ = ObCompressorType::LZ4_191_COMPRESSOR;
  val.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  val.logic_id_.data_seq_ = 190;
  val.logic_id_.logic_version_ = 1;
  val.logic_id_.tablet_id_ = 298683190;
  val.macro_id_.set_block_index(30);

  char char_value[4096];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, val.serialize(char_value, 4096, pos, CLUSTER_CURRENT_VERSION));
  meta_row.storage_datums_[6].set_string(char_value, pos);

  ASSERT_EQ(OB_SUCCESS, dumper.append_row(meta_row));

  ObIndexBlockInfo index_block_info;
  ASSERT_EQ(OB_SUCCESS, dumper.close(index_block_info));

  ASSERT_EQ(OB_SUCCESS, print_meta_root(index_block_info));
}

TEST_F(TestIndexDumper, get_from_mem)
{
  ObArray<ObDataMacroBlockMeta> data_macro_metas;
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ObDataMacroBlockMeta *deep_copy_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(deep_copy_meta, allocator_));
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.push_back(
          *(deep_copy_meta)));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_TRUE(data_macro_metas.count() > 0);
  ObIndexBlockInfo meta_block_info;
  ObBaseIndexBlockDumper macro_meta_dumper;
  ObDataStoreDesc mem_desc;
  ASSERT_EQ(OB_SUCCESS, mem_desc.shallow_copy(root_index_builder_->index_store_desc_.get_desc()));
  mem_desc.micro_block_size_ = mem_desc.get_micro_block_size_limit();
  ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.init(mem_desc,
    root_index_builder_->container_store_desc_, mem_desc.sstable_index_builder_, allocator_, allocator_, true));
  ObDatumRow meta_row;
  ASSERT_EQ(OB_SUCCESS, meta_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));

  STORAGE_LOG(INFO, "test print metas count", K(data_macro_metas.count()));
  for (int64_t i = 0; i < data_macro_metas.count(); ++i) {
    ObDataMacroBlockMeta &meta = data_macro_metas.at(i);
    ASSERT_EQ(OB_SUCCESS, meta.build_row(meta_row, allocator_, CLUSTER_CURRENT_VERSION));
    ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.append_row(meta_row));
  }
  // close
  ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.close(meta_block_info));
  STORAGE_LOG(INFO, "test print meta block info", K(meta_block_info));
  ASSERT_TRUE(meta_block_info.in_mem());
  ASSERT_EQ(data_macro_metas.count(), meta_block_info.get_row_count());
  ASSERT_EQ(1, meta_block_info.get_micro_block_count());

  // test mem row
  ObDatumRow load_row;
  ObDataMacroBlockMeta macro_meta;
  ObIndexBlockLoader index_block_loader;
  ASSERT_EQ(OB_SUCCESS, index_block_loader.init(allocator_, CLUSTER_CURRENT_VERSION));
  ASSERT_EQ(OB_SUCCESS, index_block_loader.open(meta_block_info));
  ASSERT_EQ(OB_SUCCESS, load_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  tmp_ret = OB_SUCCESS;
  int iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = index_block_loader.get_next_row(load_row);
    STORAGE_LOG(INFO, "loader get next row", K(tmp_ret), K(load_row));
    if (OB_SUCCESS == tmp_ret) {
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(iter_cnt).build_row(meta_row, allocator_, CLUSTER_CURRENT_VERSION));
      for (int64_t i = 0; i < TEST_ROWKEY_COLUMN_CNT + 3; ++i) {
        ASSERT_TRUE(ObDatum::binary_equal(load_row.storage_datums_[i], meta_row.storage_datums_[i]));
      }
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, meta_block_info.get_row_count());

  // test mem micro
  ASSERT_NE(nullptr, meta_block_info.micro_block_desc_);
}

TEST_F(TestIndexDumper, get_from_mem_and_change_row_store_type)
{
  ObArray<ObDataMacroBlockMeta> data_macro_metas;
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    if (OB_SUCCESS == tmp_ret) {
      ObDataMacroBlockMeta *deep_copy_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(deep_copy_meta, allocator_));
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.push_back(
          *(deep_copy_meta)));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_TRUE(data_macro_metas.count() > 0);
  ObIndexBlockInfo meta_block_info;
  ObIndexTreeBlockDumper index_tree_dumper;
  ObDataStoreDesc mem_desc;
  ASSERT_EQ(OB_SUCCESS, mem_desc.shallow_copy(root_index_builder_->index_store_desc_.get_desc()));
  mem_desc.micro_block_size_ = mem_desc.get_micro_block_size_limit();
  ASSERT_EQ(OB_SUCCESS, index_tree_dumper.init(root_index_builder_->data_store_desc_.get_desc(),
    mem_desc, mem_desc.sstable_index_builder_, root_index_builder_->container_store_desc_, allocator_, allocator_, true));
  ObDatumRow index_row;
  ASSERT_EQ(OB_SUCCESS, index_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));

  for (int64_t i = 0; i < data_macro_metas.count(); ++i) {
    ObIndexBlockRowDesc row_desc(mem_desc);
    ObDataMacroBlockMeta &meta = data_macro_metas.at(i);
    ASSERT_EQ(OB_SUCCESS, meta.build_row(index_row, allocator_, CLUSTER_CURRENT_VERSION));
    ASSERT_EQ(OB_SUCCESS, ObBaseIndexBlockBuilder::meta_to_row_desc(meta, mem_desc, nullptr, row_desc));
    ASSERT_EQ(OB_SUCCESS, index_tree_dumper.append_row(row_desc));
  }
  // close
  ASSERT_EQ(OB_SUCCESS, index_tree_dumper.close(meta_block_info));
  ASSERT_TRUE(meta_block_info.in_mem());
  ASSERT_EQ(data_macro_metas.count(), meta_block_info.get_row_count());
  ASSERT_EQ(1, meta_block_info.get_micro_block_count());

  // row builder
  ObWholeDataStoreDesc row_builder_desc;
  ASSERT_EQ(OB_SUCCESS, row_builder_desc.assign(mem_desc));
  common::ObRowStoreType before_row_store_type = mem_desc.get_row_store_type();
  common::ObRowStoreType after_row_store_type;
  if (before_row_store_type <= common::ObRowStoreType::SELECTIVE_ENCODING_ROW_STORE) {
    after_row_store_type = common::ObRowStoreType::CS_ENCODING_ROW_STORE;
  } else {
    after_row_store_type = common::ObRowStoreType::FLAT_ROW_STORE;
  }
  row_builder_desc.get_desc().row_store_type_ = after_row_store_type;
  ASSERT_NE(row_builder_desc.get_desc().get_row_store_type(), mem_desc.get_row_store_type());
  ObIndexBlockRowBuilder row_builder;
  ASSERT_EQ(OB_SUCCESS, row_builder.init(allocator_, row_builder_desc.get_desc(), row_builder_desc.get_desc()));

  // test mem row
  ObDatumRow load_row;
  ObIndexBlockRowParser row_parser;
  ObWholeDataStoreDesc desc;
  ObDataMacroBlockMeta macro_meta;
  ObIndexBlockLoader index_block_loader;
  ASSERT_EQ(OB_SUCCESS, index_block_loader.init(allocator_, CLUSTER_CURRENT_VERSION));
  ASSERT_EQ(OB_SUCCESS, index_block_loader.open(meta_block_info));
  ASSERT_EQ(OB_SUCCESS, load_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  tmp_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, desc.assign(mem_desc));
  int iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    row_parser.reset();
    load_row.reuse();
    ObIndexBlockRowDesc row_desc;
    const ObDatumRow * new_row = nullptr;
    tmp_ret = index_block_loader.get_next_row(load_row);
    if (OB_SUCCESS == tmp_ret) {
      const ObDatumRow *row_to_append = NULL;
      ASSERT_EQ(OB_SUCCESS, row_parser.init(mem_desc.get_rowkey_column_count(), load_row));
      desc.get_desc().row_store_type_ = common::ObRowStoreType::DUMMY_ROW_STORE;
      ASSERT_NE(desc.get_desc().row_store_type_, mem_desc.row_store_type_);
      ASSERT_EQ(OB_SUCCESS, row_desc.init(desc.get_desc(), row_parser, load_row));
      ASSERT_EQ(desc.get_desc().row_store_type_, mem_desc.row_store_type_);
      ASSERT_EQ(OB_SUCCESS,row_builder.build_row(row_desc, new_row));
      for (int64_t i = 0; i < TEST_ROWKEY_COLUMN_CNT + 3; ++i) {
        bool bret = ObDatum::binary_equal(load_row.storage_datums_[i], new_row->storage_datums_[i]);
        if (bret != true) {
          ObIndexBlockRowParser tmp_row_parser;
          const ObIndexBlockRowHeader *tmp_row_header = nullptr;
          ASSERT_EQ(OB_SUCCESS, tmp_row_parser.init(mem_desc.get_rowkey_column_count(), load_row));
          ASSERT_EQ(OB_SUCCESS, tmp_row_parser.get_header(tmp_row_header));
          LOG_INFO("load row", KPC(tmp_row_header));
          tmp_row_parser.reset();
          ASSERT_EQ(OB_SUCCESS, tmp_row_parser.init(mem_desc.get_rowkey_column_count(), *new_row));
          ASSERT_EQ(OB_SUCCESS, tmp_row_parser.get_header(tmp_row_header));
          LOG_INFO("new row", KPC(tmp_row_header));
        }
        ASSERT_TRUE(bret);
      }
      ++iter_cnt;
    } else {
      ASSERT_EQ(OB_ITER_END, tmp_ret);
      break;
    }
  }
}

TEST_F(TestIndexDumper, get_from_disk)
{
  ObArray<ObDataMacroBlockMeta> data_macro_metas;
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ObDataMacroBlockMeta *deep_copy_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(deep_copy_meta, allocator_));
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.push_back(
          *(deep_copy_meta)));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_TRUE(data_macro_metas.count() > 0);
  ObIndexBlockInfo meta_block_info;
  ObBaseIndexBlockDumper macro_meta_dumper;
  ObDataStoreDesc mem_desc;
  ASSERT_EQ(OB_SUCCESS, mem_desc.shallow_copy(root_index_builder_->index_store_desc_.get_desc()));
  mem_desc.micro_block_size_ = mem_desc.get_micro_block_size_limit();
  ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.init(mem_desc,
    root_index_builder_->container_store_desc_, mem_desc.sstable_index_builder_, allocator_, allocator_, true));
  ObDatumRow meta_row;
  ASSERT_EQ(OB_SUCCESS, meta_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));

  STORAGE_LOG(INFO, "test print metas count", K(data_macro_metas.count()));
  for (int64_t i = 0; i < data_macro_metas.count(); ++i) {
    ObDataMacroBlockMeta &meta = data_macro_metas.at(i);
    ASSERT_EQ(OB_SUCCESS, meta.build_row(meta_row, allocator_, CLUSTER_CURRENT_VERSION));
    ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.append_row(meta_row));
    if (i != data_macro_metas.count() - 1) {
      ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.build_and_append_block());
      ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.meta_macro_writer_->try_switch_macro_block());
    }
  }
  // close
  ObTableBackupFlag table_backup_flag;
  table_backup_flag.clear();
  ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.close(meta_block_info));
  STORAGE_LOG(INFO, "test print meta block info", K(meta_block_info));
  ASSERT_TRUE(meta_block_info.in_disk());
  ASSERT_EQ(data_macro_metas.count(), meta_block_info.get_row_count());
  ASSERT_EQ(data_macro_metas.count(), meta_block_info.block_write_ctx_->get_macro_block_count());

 // test disk row
  ObDatumRow load_row;
  ObDataMacroBlockMeta macro_meta;
  ObIndexBlockLoader index_block_loader;
  ASSERT_EQ(OB_SUCCESS, index_block_loader.init(allocator_, CLUSTER_CURRENT_VERSION));
  ASSERT_EQ(OB_SUCCESS, index_block_loader.open(meta_block_info));
  ASSERT_EQ(OB_SUCCESS, load_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  tmp_ret = OB_SUCCESS;
  int iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = index_block_loader.get_next_row(load_row);
    STORAGE_LOG(INFO, "loader get next row", K(tmp_ret), K(load_row));
    if (OB_SUCCESS == tmp_ret) {
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(iter_cnt).build_row(meta_row, allocator_, CLUSTER_CURRENT_VERSION));
      for (int64_t i = 0; i < TEST_ROWKEY_COLUMN_CNT + 3; ++i) {
        ASSERT_TRUE(ObDatum::binary_equal(load_row.storage_datums_[i], meta_row.storage_datums_[i]));
      }
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, meta_block_info.get_row_count());


  // test disk micro
  tmp_ret = OB_SUCCESS;
  iter_cnt = 0;
  ObMicroBlockDesc load_micro_block_desc;
  index_block_loader.reuse();
  ASSERT_EQ(OB_SUCCESS, index_block_loader.open(meta_block_info));
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = index_block_loader.get_next_micro_block_desc(load_micro_block_desc, mem_desc, allocator_);
    STORAGE_LOG(INFO, "loader get next micro block", K(tmp_ret), K(load_micro_block_desc));
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, meta_block_info.get_micro_block_count());
}

TEST_F(TestIndexDumper, get_from_array)
{
  ObArray<ObDataMacroBlockMeta> data_macro_metas;
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ObDataMacroBlockMeta *deep_copy_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(deep_copy_meta, allocator_));
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.push_back(
          *(deep_copy_meta)));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_TRUE(data_macro_metas.count() > 0);
  ObIndexBlockInfo meta_block_info;
  ObBaseIndexBlockDumper macro_meta_dumper;
  ObDataStoreDesc mem_desc;
  ASSERT_EQ(OB_SUCCESS, mem_desc.shallow_copy(root_index_builder_->index_store_desc_.get_desc()));
  mem_desc.micro_block_size_ = mem_desc.get_micro_block_size_limit();
  ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.init(mem_desc,
    root_index_builder_->container_store_desc_, mem_desc.sstable_index_builder_, allocator_, allocator_, true, false));
  ObDatumRow meta_row;
  ASSERT_EQ(OB_SUCCESS, meta_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));

  STORAGE_LOG(INFO, "test print metas count", K(data_macro_metas.count()));
  for (int64_t i = 0; i < data_macro_metas.count(); ++i) {
    ObDataMacroBlockMeta &meta = data_macro_metas.at(i);
    ASSERT_EQ(OB_SUCCESS, meta.build_row(meta_row, allocator_, CLUSTER_CURRENT_VERSION));
    ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.append_row(meta_row));
  }
  // close
  ASSERT_EQ(OB_SUCCESS, macro_meta_dumper.close(meta_block_info));
  STORAGE_LOG(INFO, "test print meta block info", K(meta_block_info));
  ASSERT_TRUE(meta_block_info.in_array());
  ASSERT_EQ(data_macro_metas.count(), meta_block_info.get_row_count());


  // test array row
  ObDatumRow load_row;
  ObDataMacroBlockMeta macro_meta;
  ObIndexBlockLoader index_block_loader;
  ASSERT_EQ(OB_SUCCESS, index_block_loader.init(allocator_, CLUSTER_CURRENT_VERSION));
  ASSERT_EQ(OB_SUCCESS, index_block_loader.open(meta_block_info));
  ASSERT_EQ(OB_SUCCESS, load_row.init(allocator_, TEST_ROWKEY_COLUMN_CNT + 3));
  tmp_ret = OB_SUCCESS;
  int iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = index_block_loader.get_next_row(load_row);
    STORAGE_LOG(INFO, "loader get next row", K(iter_cnt), K(tmp_ret), K(load_row));
    if (OB_SUCCESS == tmp_ret) {
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(iter_cnt).build_row(meta_row, allocator_, CLUSTER_CURRENT_VERSION));
      STORAGE_LOG(INFO, "data macro meta", K(meta_row), K(data_macro_metas.at(iter_cnt)));
      for (int64_t i = 0; i < TEST_ROWKEY_COLUMN_CNT + 3; ++i) {
        ASSERT_TRUE(ObDatum::binary_equal(load_row.storage_datums_[i], meta_row.storage_datums_[i]));
      }
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, meta_block_info.get_row_count());

  // test mem micro
  ASSERT_EQ(nullptr, meta_block_info.micro_block_desc_);
}


} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_dumper.log*");
  OB_LOGGER.set_file_name("test_index_dumper.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
