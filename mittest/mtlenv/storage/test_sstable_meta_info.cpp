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

#include "storage/blocksstable/index_block/ob_sstable_meta_info.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/schema_utils.h"
#include "unittest/storage/test_tablet_helper.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;

namespace unittest
{
#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, expr)
#define ASSERT_FAIL(expr) ASSERT_NE(OB_SUCCESS, expr)
static const int64_t BUF_LEN = 10;
static const int64_t ROWKEY_COL_CNT = 2;
static constexpr int64_t COLUMN_CNT = 4;
static const int64_t BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
using block_type_t = ObMacroIdIterator::Type;
class TestSSTableMetaInfo : public ::testing::Test
{
public:
  TestSSTableMetaInfo() = default;
  virtual ~TestSSTableMetaInfo() = default;
  static void SetUpTestCase()
  {
    ASSERT_SUCC(MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
};

struct MacroIdComp final
{
  bool operator()(const MacroBlockId &a, const MacroBlockId &b) const
  {
    return MEMCMP(&a, &b, sizeof(MacroBlockId)) < 0;
  }
};

using macro_id_set_t = set<MacroBlockId, MacroIdComp>;

void gen_macro_id(MacroBlockId &block_id)
{
  static const int64_t block_index = 123;
  static int64_t write_seq = 0;
  block_id = MacroBlockId(write_seq++, block_index, 0);
  ASSERT_TRUE(block_id.is_valid());
}

void mock_micro_block_des_meta(ObMicroBlockDesMeta &des_meta)
{
  des_meta.encrypt_id_ = ObCipherOpMode::ob_invalid_mode;
  des_meta.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  des_meta.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  ASSERT_TRUE(des_meta.is_valid());
}

void mock_root_info(
  ObMetaDiskAddr &block_addr,
  ObMicroBlockData &block_data,
  ObArenaAllocator &allocator,
  ObRootBlockInfo &root_info)
{
  const int64_t block_size = 2L * 1024 * 1024L;
  ObMicroBlockWriter<> writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(block_size, ROWKEY_COL_CNT, COLUMN_CNT));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, COLUMN_CNT));
  ObObj obj;
  for (int64_t i = 0; i < COLUMN_CNT; ++i) {
    obj.set_int(OB_APP_MIN_COLUMN_ID + i);
    ASSERT_SUCC(row.storage_datums_[i].from_obj_enhance(obj));
  }
  row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  row.count_ = COLUMN_CNT;
  ASSERT_SUCC(writer.append_row(row));
  ObMicroBlockDesc micro_desc;
  ASSERT_SUCC(writer.build_micro_block_desc(micro_desc));

  ObMicroBlockHeader *header = const_cast<ObMicroBlockHeader *>(micro_desc.header_);
  ASSERT_NE(nullptr, header);
  ASSERT_EQ(true, header->is_valid());
  header->data_length_ = micro_desc.buf_size_;
  header->data_zlength_ = micro_desc.buf_size_;
  header->data_checksum_ = ob_crc64_sse42(0, micro_desc.buf_, micro_desc.buf_size_);
  header->original_length_ = micro_desc.buf_size_;
  header->set_header_checksum();

  const int64_t size = header->header_size_ + micro_desc.buf_size_;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_TRUE(nullptr != buf);
  int64_t pos = 0;
  ASSERT_SUCC(micro_desc.header_->serialize(buf, size, pos));
  MEMCPY(buf + pos, micro_desc.buf_, micro_desc.buf_size_);

  block_addr.offset_ = 1000;
  block_addr.size_ = size;
  block_addr.type_ = ObMetaDiskAddr::MEM;
  block_data.buf_ = buf;
  block_data.size_ = size;
  block_data.type_ = ObMicroBlockData::INDEX_BLOCK;
  ObMacroBlockWriteInfo write_info;
  ObMacroBlockHandle handle;
  const int64_t buf_size = block_size;
  char *io_buf = static_cast<char *>(allocator.alloc(buf_size));
  ASSERT_TRUE(nullptr != io_buf);
  MEMCPY(io_buf + block_addr.offset_, buf, block_addr.size_);
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info.buffer_ = io_buf;
  write_info.size_ = buf_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  ASSERT_SUCC(ObBlockManager::write_block(write_info, handle));
  block_addr.second_id_ = handle.get_macro_id().second_id();
  ASSERT_SUCC(root_info.init_root_block_info(allocator, block_addr,
      block_data, static_cast<ObRowStoreType>(header->row_store_type_)));
  ASSERT_TRUE(root_info.is_valid());
}

void mock_sstable_basic_meta(ObSSTableBasicMeta &meta)
{
  meta.version_ = ObSSTableBasicMeta::SSTABLE_BASIC_META_VERSION;
  meta.index_type_ = ObIndexType::INDEX_TYPE_NORMAL_LOCAL;

  meta.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  meta.table_mode_.pk_mode_ = ObTablePKMode::TPKM_NEW_NO_PK;
  meta.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  ASSERT_TRUE(meta.table_mode_.is_valid());

  meta.ddl_scn_ = SCN::max_scn();
  meta.filled_tx_scn_ = SCN::max_scn();

  meta.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  meta.latest_row_store_type_ = ObRowStoreType::DUMMY_ROW_STORE;

  meta.table_backup_flag_.has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  meta.table_backup_flag_.has_local_flag_ = ObTableHasLocalFlag::HAS_LOCAL;

  meta.table_shared_flag_.shared_flag_ = ObTableSharedFlag::PRIVATE;

  meta.tx_data_recycle_scn_ = SCN::max_scn();
  meta.rec_scn_ = SCN::max_scn();

  ASSERT_TRUE(meta.is_valid());
}

void make_sstable_macro_info(
  const int64_t data_block_cnt,
  const int64_t other_block_cnt,
  ObArenaAllocator &allocator,
  ObSSTableMacroInfo &macro_info,
  vector<MacroBlockId> *data_block_ids = nullptr,
  vector<MacroBlockId> *other_block_ids = nullptr)
{
  macro_info.reset();
  ObMetaDiskAddr block_addr;
  ObMicroBlockData block_data;
  mock_root_info(block_addr, block_data, allocator, macro_info.macro_meta_info_);

  macro_info.data_block_count_ = data_block_cnt;
  if (data_block_cnt > 0) {
    ASSERT_NE(nullptr, macro_info.data_block_ids_ =
      static_cast<MacroBlockId *>(allocator.alloc(sizeof(MacroBlockId) * data_block_cnt)));
  }
  for (int64_t i = 0; i < data_block_cnt; ++i) {
    MacroBlockId block_id;
    gen_macro_id(block_id);
    macro_info.data_block_ids_[i] = block_id;
    if (data_block_ids != nullptr) {
      data_block_ids->push_back(block_id);
    }
  }
  macro_info.other_block_count_ = other_block_cnt;
  if (other_block_cnt > 0) {
    ASSERT_NE(nullptr, macro_info.other_block_ids_ =
      static_cast<MacroBlockId *>(allocator.alloc(sizeof(MacroBlockId) * other_block_cnt)));
  }
  for (int64_t i = 0; i < other_block_cnt; ++i) {
    MacroBlockId block_id;
    gen_macro_id(block_id);
    macro_info.other_block_ids_[i] = block_id;
    if (other_block_ids != nullptr) {
      other_block_ids->push_back(block_id);
    }
  }
  macro_info.linked_block_count_ = 0;
  macro_info.entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  macro_info.is_meta_root_ = false;
  macro_info.nested_offset_ = 0;
  macro_info.nested_size_ = 123456;
  ASSERT_TRUE(macro_info.is_valid());
}

void persist_macro_info_to_linked_blocks(
    ObArenaAllocator &allocator,
    ObSSTableMacroInfo &macro_info,
    vector<MacroBlockId> *linked_block_ids = nullptr)
{
  ASSERT_TRUE(macro_info.is_valid());
  ASSERT_EQ(nullptr, macro_info.linked_block_ids_);
  ASSERT_EQ(0, macro_info.linked_block_count_);

  int64_t data_blk_cnt = 0, other_blk_cnt = 0;
  ASSERT_SUCC(macro_info.get_data_block_count(data_blk_cnt));
  ASSERT_SUCC(macro_info.get_other_block_count(other_blk_cnt));
  ASSERT_TRUE(data_blk_cnt + other_blk_cnt > 0);
  ObLinkedMacroInfoWriteParam param;
  {
    param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
    param.tablet_id_ = ObTabletID(200001);
    param.tablet_private_transfer_epoch_ = 0;
  }
  if (OB_NOT_NULL(macro_info.data_block_ids_)) {
    for (int64_t i = 0; i < macro_info.data_block_count_; ++i) {
      ASSERT_SUCC(OB_STORAGE_OBJECT_MGR.inc_ref(macro_info.data_block_ids_[i]));
    }
  }
    if (OB_NOT_NULL(macro_info.other_block_ids_)) {
    for (int64_t i = 0; i < macro_info.other_block_count_; ++i) {
      ASSERT_SUCC(OB_STORAGE_OBJECT_MGR.inc_ref(macro_info.other_block_ids_[i]));
    }
  }
  ASSERT_TRUE(param.is_valid());
  int64_t macro_seq = 0;
  ObSharedObjectsWriteCtx write_ctx;
  ASSERT_SUCC(macro_info.persist_block_ids(allocator, param, macro_seq, write_ctx));
  ASSERT_TRUE(!IS_EMPTY_BLOCK_LIST(macro_info.entry_id_));
  ASSERT_GT(macro_info.linked_block_count_, 0);
  ASSERT_NE(nullptr, macro_info.linked_block_ids_);
  for (int64_t i = 0; i < macro_info.linked_block_count_; ++i) {
      const MacroBlockId &block_id = macro_info.linked_block_ids_[i];
      if (linked_block_ids != nullptr) {
        linked_block_ids->push_back(block_id);
      }
      ASSERT_SUCC(OB_STORAGE_OBJECT_MGR.inc_ref(block_id));
  }
}

void serialize_macro_info(
  const bool inlined,
  ObArenaAllocator &allocator,
  ObSSTableMacroInfo &macro_info,
  vector<char> &buf,
  int64_t &buf_size,
  vector<MacroBlockId> *linked_block_ids = nullptr)
{
  ASSERT_TRUE(macro_info.is_valid());
  if (!inlined) {
   persist_macro_info_to_linked_blocks(allocator, macro_info, linked_block_ids);
  }
  int64_t pos = 0;
  buf_size = macro_info.get_serialize_size();
  buf.resize(buf_size);
  ASSERT_SUCC(macro_info.serialize(buf.data(), buf_size, pos));
}

void check_macro_info_deserialize(
  const ObSSTableMacroInfo &orig_macro_info,
  ObArenaAllocator &allocator,
  const char *buf,
  const int64_t buf_size)
{
  ObMicroBlockDesMeta des_meta;
  mock_micro_block_des_meta(des_meta);
  ObSSTableMacroInfo macro_info;
  const bool is_inline = IS_EMPTY_BLOCK_LIST(orig_macro_info.entry_id_);
  int64_t pos = 0;
  macro_info.deserialize(allocator, des_meta, buf, buf_size, pos);
  ASSERT_TRUE(macro_info.is_valid());
  ASSERT_EQ(macro_info.entry_id_, orig_macro_info.entry_id_);
  ASSERT_EQ(macro_info.is_meta_root_, orig_macro_info.is_meta_root_);
  ASSERT_EQ(macro_info.nested_offset_, orig_macro_info.nested_offset_);
  ASSERT_EQ(macro_info.nested_size_, macro_info.nested_size_);

  if (is_inline) {
    int64_t data_blk_cnt0 = 0, data_blk_cnt1 = 0;
    ASSERT_SUCC(orig_macro_info.get_data_block_count(data_blk_cnt0));
    ASSERT_SUCC(macro_info.get_data_block_count(data_blk_cnt1));
    ASSERT_EQ(data_blk_cnt0, data_blk_cnt1);
    if (data_blk_cnt0 > 0) {
      ASSERT_NE(nullptr, macro_info.data_block_ids_);
      ASSERT_NE(nullptr, orig_macro_info.data_block_ids_);
    }
    ASSERT_EQ(0, MEMCMP(
      macro_info.data_block_ids_, orig_macro_info.data_block_ids_, sizeof(MacroBlockId) * data_blk_cnt0));
    int64_t other_blk_cnt0 = 0, other_blk_cnt1 = 0;
    ASSERT_SUCC(macro_info.get_other_block_count(other_blk_cnt0));
    ASSERT_SUCC(orig_macro_info.get_other_block_count(other_blk_cnt1));
    ASSERT_EQ(other_blk_cnt0, other_blk_cnt1);
    if (other_blk_cnt0 > 0) {
      ASSERT_NE(nullptr, macro_info.other_block_ids_);
      ASSERT_NE(nullptr, orig_macro_info.other_block_ids_);
    }
    ASSERT_EQ(0, MEMCMP(
      macro_info.other_block_ids_, orig_macro_info.other_block_ids_, sizeof(MacroBlockId) * other_blk_cnt1));
    ASSERT_EQ(0, orig_macro_info.linked_block_count_);
    ASSERT_EQ(0, macro_info.linked_block_count_);
    ASSERT_EQ(nullptr, orig_macro_info.linked_block_ids_);
    ASSERT_EQ(nullptr, macro_info.linked_block_ids_);
  } else {
    ASSERT_EQ(-1, macro_info.data_block_count_);
    ASSERT_EQ(-1, macro_info.other_block_count_);
    ASSERT_EQ(-1, macro_info.linked_block_count_);
    ASSERT_EQ(nullptr, macro_info.data_block_ids_);
    ASSERT_EQ(nullptr, macro_info.other_block_ids_);
    ASSERT_EQ(nullptr, macro_info.linked_block_ids_);

    int64_t data_blk_cnt0 = 0, data_blk_cnt1 = 0;
    ASSERT_SUCC(macro_info.get_data_block_count(data_blk_cnt0));
    ASSERT_SUCC(orig_macro_info.get_data_block_count(data_blk_cnt1));
    ASSERT_EQ(data_blk_cnt0, data_blk_cnt1);
    int64_t other_blk_cnt0 = 0, other_blk_cnt1 = 0;
    ASSERT_SUCC(macro_info.get_other_block_count(other_blk_cnt0));
    ASSERT_SUCC(orig_macro_info.get_other_block_count(other_blk_cnt1));
    ASSERT_EQ(other_blk_cnt0, other_blk_cnt1);
    int64_t linked_blk_cnt0, linked_blk_cnt1 = 0;
    ASSERT_SUCC(macro_info.get_linked_block_count(linked_blk_cnt0));
    ASSERT_SUCC(orig_macro_info.get_linked_block_count(linked_blk_cnt1));
    ASSERT_EQ(linked_blk_cnt0, linked_blk_cnt1);

    int64_t data_blk_cnt2 = 0, other_blk_cnt2 = 0, linked_blk_cnt2 = 0;
    ASSERT_SUCC(macro_info.get_block_count(data_blk_cnt2, other_blk_cnt2, linked_blk_cnt2));
    ASSERT_EQ(data_blk_cnt2, data_blk_cnt0);
    ASSERT_EQ(other_blk_cnt2, other_blk_cnt0);
    ASSERT_EQ(linked_blk_cnt2, linked_blk_cnt0);
  }
}

using block_id_and_type = pair<MacroBlockId, block_type_t>;

bool is_macro_block_exists(
  const MacroBlockId &block_id,
  const block_type_t block_type,
  const vector<block_id_and_type> &block_ids)
{
  auto find = std::find_if(block_ids.cbegin(), block_ids.cend(),
    [&](const block_id_and_type &it)
      {
        return it.first == block_id && it.second == block_type;
      });
  if (find == block_ids.cend()) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "macro block id is missing", K(ret), K(block_id));
  }
  return find != block_ids.cend();
}

void check_macro_block_iter(
  ObMacroIdIterator &iter,
  const vector<block_id_and_type> &expected_result)
{
  MacroBlockId block_id;
  block_type_t block_type = block_type_t::MAX;
  for (size_t i = 0; i < expected_result.size(); ++i) {
    block_id.reset();
    block_type = block_type_t::MAX;
    ASSERT_SUCC(iter.get_next_macro_id(block_id, block_type));
    ASSERT_TRUE(is_macro_block_exists(block_id, block_type, expected_result));
  }
  ASSERT_EQ(OB_ITER_END, iter.get_next_macro_id(block_id, block_type));
}

void check_macro_block_iters(
  const ObSSTableMacroInfo &macro_info,
  const vector<MacroBlockId> &data_blk_ids,
  const vector<MacroBlockId> &other_blk_ids,
  const vector<MacroBlockId> &linked_blk_ids)
{
  ObMacroIdIterator iter;
  vector<block_id_and_type> expected_result;
  int64_t tmp_cnt = 0;
  int64_t data_blk_cnt = 0, other_blk_cnt = 0, linked_blk_cnt = 0, total_blk_count = 0;
  int64_t expected_data_blk_cnt = 0, expected_other_blk_cnt = 0, expected_linked_blk_cnt = 0;

  ASSERT_SUCC(macro_info.get_block_count(expected_data_blk_cnt, expected_other_blk_cnt, expected_linked_blk_cnt));

  ASSERT_SUCC(macro_info.get_data_block_iter(iter));
  for (const auto &id : data_blk_ids) {
    expected_result.push_back({id, block_type_t::DATA_BLOCK});
  }
  check_macro_block_iter(iter, expected_result);
  ASSERT_SUCC(iter.get_block_count(block_type_t::DATA_BLOCK, data_blk_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::NONE, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::LINKED_BLOCK, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::OTHER_BLOCK, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::MAX, tmp_cnt));
  ASSERT_EQ(expected_data_blk_cnt, data_blk_cnt);
  iter.reset();
  expected_result.clear();

  ASSERT_SUCC(macro_info.get_other_block_iter(iter));
  for (const auto &id : other_blk_ids) {
    expected_result.push_back({id, block_type_t::OTHER_BLOCK});
  }
  check_macro_block_iter(iter, expected_result);
  ASSERT_SUCC(iter.get_block_count(block_type_t::OTHER_BLOCK, other_blk_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::NONE, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::DATA_BLOCK, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::LINKED_BLOCK, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::MAX, tmp_cnt));
  ASSERT_EQ(expected_other_blk_cnt, other_blk_cnt);
  iter.reset();
  expected_result.clear();

  ASSERT_SUCC(macro_info.get_linked_block_iter(iter));
  for (const auto &id : linked_blk_ids) {
    expected_result.push_back({id, block_type_t::LINKED_BLOCK});
  }
  check_macro_block_iter(iter, expected_result);
  ASSERT_SUCC(iter.get_block_count(block_type_t::LINKED_BLOCK, linked_blk_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::NONE, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::DATA_BLOCK, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::OTHER_BLOCK, tmp_cnt));
  ASSERT_FAIL(iter.get_block_count(block_type_t::MAX, tmp_cnt));
  ASSERT_EQ(expected_linked_blk_cnt, linked_blk_cnt);
  iter.reset();
  expected_result.clear();

  for (const auto &id : data_blk_ids) {
    expected_result.push_back({id, block_type_t::DATA_BLOCK});
  }
  for (const auto &id : other_blk_ids) {
    expected_result.push_back({id, block_type_t::OTHER_BLOCK});
  }
  for (const auto &id : linked_blk_ids) {
    expected_result.push_back({id, block_type_t::LINKED_BLOCK});
  }
  ASSERT_SUCC(macro_info.get_all_block_iter(iter));
  check_macro_block_iter(iter, expected_result);
  ASSERT_SUCC(iter.get_block_count(block_type_t::DATA_BLOCK, data_blk_cnt));
  ASSERT_SUCC(iter.get_block_count(block_type_t::OTHER_BLOCK, other_blk_cnt));
  ASSERT_SUCC(iter.get_block_count(block_type_t::LINKED_BLOCK, linked_blk_cnt));
  ASSERT_SUCC(iter.get_block_count(block_type_t::MAX, total_blk_count));
  ASSERT_FAIL(iter.get_block_count(block_type_t::NONE, tmp_cnt));
  ASSERT_EQ(expected_data_blk_cnt, data_blk_cnt);
  ASSERT_EQ(expected_other_blk_cnt, other_blk_cnt);
  ASSERT_EQ(expected_linked_blk_cnt, linked_blk_cnt);
  ASSERT_EQ(total_blk_count, expected_data_blk_cnt + expected_other_blk_cnt + expected_linked_blk_cnt);
}

void construct_sstable(
    const ObTabletID &tablet_id,
    blocksstable::ObSSTable &sstable,
    common::ObArenaAllocator &allocator,
    int64_t data_block_count,
    int64_t other_block_count)
{
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObTabletCreateSSTableParam param;
  TestTabletHelper::prepare_sstable_param(tablet_id, schema, param);
  MacroBlockId block_id(0, 10000, 0);

  for (int64_t i = 0; i < data_block_count; i++) {
    block_id.block_index_ = ObRandom::rand(1, 10<<10);
    ASSERT_SUCC(param.data_block_ids_.push_back(block_id));
  }

  for (int64_t i = 0; i < other_block_count; i++) {
    block_id.block_index_ = ObRandom::rand(1, 10<<10);
    ASSERT_SUCC(param.other_block_ids_.push_back(block_id));
  }

  ASSERT_SUCC(sstable.init(param, &allocator));
}

void write_buffer(
    const char *buf,
    const int64_t buf_size,
    ObMetaDiskAddr &addr)
{
  addr.reset();
  ObTenantStorageMetaService *meta_svr = MTL(ObTenantStorageMetaService *);
  ASSERT_NE(nullptr, meta_svr);

  ObSharedObjectWriteInfo write_info;
  {
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = buf_size;
    write_info.ls_epoch_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  }
  ObStorageObjectOpt opt; // fake
  opt.set_private_meta_macro_object_opt();
  ObSharedObjectWriteHandle handle;
  ObSharedObjectsWriteCtx write_ctx;
  ASSERT_SUCC(meta_svr->get_shared_object_reader_writer().async_write(write_info, opt, handle));
  ASSERT_SUCC(handle.get_write_ctx(write_ctx));
  addr = write_ctx.addr_;
  ASSERT_TRUE(addr.is_valid());
  ASSERT_TRUE(addr.is_block());
}

TEST_F(TestSSTableMetaInfo, test_macro_iter)
{
  ObArenaAllocator allocator("MacroIter");
  // case1: test empty macro info iteration
  {
    const int64_t data_block_cnt = 0;
    const int64_t other_block_cnt = 0;
    vector<MacroBlockId> data_block_ids, other_block_ids, linked_block_ids;
    ObSSTableMacroInfo macro_info;
    make_sstable_macro_info(
      data_block_cnt, other_block_cnt, allocator, macro_info,
      &data_block_ids, &other_block_ids);

    vector<char> macro_info_buf;
    int64_t buf_size = 0;
    serialize_macro_info(true, allocator, macro_info, macro_info_buf, buf_size, &linked_block_ids);
    check_macro_info_deserialize(macro_info, allocator, macro_info_buf.data(), buf_size);
    check_macro_block_iters(macro_info, data_block_ids, other_block_ids, linked_block_ids);
  }

  // case2: test inline macro info iteration
  {
    const int64_t data_block_cnt = 5;
    const int64_t other_block_cnt = 7;
    vector<MacroBlockId> data_block_ids, other_block_ids, linked_block_ids;
    ObSSTableMacroInfo macro_info;
    make_sstable_macro_info(
      data_block_cnt, other_block_cnt, allocator, macro_info,
      &data_block_ids, &other_block_ids);

    vector<char> macro_info_buf;
    int64_t buf_size = 0;
    serialize_macro_info(/*inlined*/true, allocator, macro_info, macro_info_buf, buf_size, &linked_block_ids);
    check_macro_info_deserialize(macro_info, allocator, macro_info_buf.data(), buf_size);
    check_macro_block_iters(macro_info, data_block_ids, other_block_ids, linked_block_ids);
  }

  // case3: test non-inline macro info iteration
  {
    const int64_t data_block_cnt = 1234;
    const int64_t other_block_cnt = 5678;
    vector<MacroBlockId> data_block_ids, other_block_ids, linked_block_ids;
    ObSSTableMacroInfo macro_info;
    make_sstable_macro_info(
      data_block_cnt, other_block_cnt, allocator, macro_info,
      &data_block_ids, &other_block_ids);

    vector<char> macro_info_buf;
    int64_t buf_size = 0;
    serialize_macro_info(/*inlined*/false, allocator, macro_info, macro_info_buf, buf_size, &linked_block_ids);
    check_macro_info_deserialize(macro_info, allocator, macro_info_buf.data(), buf_size);
    check_macro_block_iters(macro_info, data_block_ids, other_block_ids, linked_block_ids);
  }
}

TEST_F(TestSSTableMetaInfo, test_macro_info_deep_copy)
{
  ObArenaAllocator allocator("MacroInfoCpy");
  // case1: test deep copy empty macro info
  {
    ObSSTableMacroInfo macro_info;
    const int64_t data_block_cnt = 0;
    const int64_t other_block_cnt = 0;
    make_sstable_macro_info(
      data_block_cnt, other_block_cnt, allocator, macro_info);
    vector<char> macro_info_buf;
    int64_t buf_size = 0;
    serialize_macro_info(/*inlined*/true, allocator, macro_info, macro_info_buf, buf_size);

    ObSSTableMacroInfo macro_info_copy;
    const int64_t copy_buf_size = macro_info.get_variable_size();
    printf("copy_buf_size:%ld\n", copy_buf_size);
    vector<char> copy_buf(copy_buf_size, '\0');
    int64_t pos = 0;
    ASSERT_SUCC(macro_info.deep_copy(copy_buf.data(), copy_buf_size, pos, macro_info_copy));

    vector<char> macro_info_buf2;
    int64_t buf_size2 = 0;
    serialize_macro_info(/*inlined*/true, allocator, macro_info_copy, macro_info_buf2, buf_size2);
    ASSERT_EQ(buf_size, buf_size2);
    ASSERT_EQ(0, MEMCMP(macro_info_buf.data(), macro_info_buf2.data(), buf_size));
  }

  // case2: test deep copy inline macro info
  {
    ObSSTableMacroInfo macro_info;
    const int64_t data_block_cnt = 15;
    const int64_t other_block_cnt = 15;
    make_sstable_macro_info(
      data_block_cnt, other_block_cnt, allocator, macro_info);
    vector<char> macro_info_buf;
    int64_t buf_size = 0;
    serialize_macro_info(/*inlined*/true, allocator, macro_info, macro_info_buf, buf_size);

    ObSSTableMacroInfo macro_info_copy;
    const int64_t copy_buf_size = macro_info.get_variable_size();
    printf("copy_buf_size:%ld\n", copy_buf_size);
    vector<char> copy_buf(copy_buf_size, '\0');
    int64_t pos = 0;
    ASSERT_SUCC(macro_info.deep_copy(copy_buf.data(), copy_buf_size, pos, macro_info_copy));

    vector<char> macro_info_buf2;
    int64_t buf_size2 = 0;
    serialize_macro_info(/*inlined*/true, allocator, macro_info_copy, macro_info_buf2, buf_size2);
    ASSERT_EQ(buf_size, buf_size2);
    ASSERT_EQ(0, MEMCMP(macro_info_buf.data(), macro_info_buf2.data(), buf_size));
  }

  // case3: test deep copy non-inline macro info
  {
    ObSSTableMacroInfo macro_info;
    const int64_t data_block_cnt = 1234;
    const int64_t other_block_cnt = 5678;
    make_sstable_macro_info(
      data_block_cnt, other_block_cnt, allocator, macro_info);
    vector<char> macro_info_buf;
    int64_t buf_size = 0;
    serialize_macro_info(/*inlined*/false, allocator, macro_info, macro_info_buf, buf_size);

    ObSSTableMacroInfo macro_info_copy;
    const int64_t copy_buf_size = macro_info.get_variable_size();
    printf("copy_buf_size:%ld\n", copy_buf_size);
    vector<char> copy_buf(copy_buf_size, '\0');
    int64_t pos = 0;
    ASSERT_SUCC(macro_info.deep_copy(copy_buf.data(), copy_buf_size, pos, macro_info_copy));
    vector<char> macro_info_buf2;
    int64_t buf_size2 = 0;
    serialize_macro_info(/*inlined*/true, allocator, macro_info_copy, macro_info_buf2, buf_size2);
    ASSERT_EQ(buf_size, buf_size2);
    ASSERT_EQ(0, MEMCMP(macro_info_buf.data(), macro_info_buf2.data(), buf_size));
  }
}

TEST_F(TestSSTableMetaInfo, test_sstable_get_meta)
{
  ObArenaAllocator allocator("SSTGetMeta");
  ObSafeArenaAllocator safe_allocator(allocator);
  // case1: loaded
  {
    const ObTabletID tablet_id(200001);
    ObSSTable sstable;
    const int64_t data_blk_cnt = 10, other_blk_cnt = 10;
    construct_sstable(tablet_id, sstable, allocator, data_blk_cnt, other_blk_cnt);
    ASSERT_TRUE(sstable.is_loaded());
    persist_macro_info_to_linked_blocks(allocator, sstable.meta_->macro_info_);

    const int64_t full_serialize_size = sstable.get_full_serialize_size(DATA_CURRENT_VERSION);
    vector<char> full_buf(full_serialize_size, '\0');
    int64_t pos = 0;
    ASSERT_SUCC(sstable.serialize_full_table(DATA_CURRENT_VERSION, full_buf.data(), full_buf.size(), pos));

    pos = 0;
    ObSSTable sstable2;
    ASSERT_SUCC(sstable2.deserialize(allocator, full_buf.data(), full_buf.size(), pos));
    ASSERT_TRUE(sstable2.is_loaded());

    ObSSTableMetaHandle meta_handle;
    ASSERT_SUCC(sstable2.get_meta(meta_handle));
    const ObSSTableMacroInfo &macro_info = meta_handle.get_sstable_meta().get_macro_info();
    int64_t cnt = 0;
    ASSERT_SUCC(macro_info.get_data_block_count(cnt));
    ASSERT_EQ(data_blk_cnt, cnt);
    cnt = 0;
    ASSERT_SUCC(macro_info.get_other_block_count(cnt));
    ASSERT_EQ(other_blk_cnt, cnt);
  }

  // case2: unloaded
  {
    const ObTabletID tablet_id(200002);
    ObSSTable sstable;
    const int64_t data_blk_cnt = 10, other_blk_cnt = 10;
    construct_sstable(tablet_id, sstable, allocator, data_blk_cnt, other_blk_cnt);
    ASSERT_TRUE(sstable.is_loaded());
    persist_macro_info_to_linked_blocks(allocator, sstable.meta_->macro_info_);

    const int64_t full_serialize_size = sstable.get_full_serialize_size(DATA_CURRENT_VERSION);
    vector<char> full_buf(full_serialize_size, '\0');
    int64_t pos = 0;
    ASSERT_SUCC(sstable.serialize_full_table(DATA_CURRENT_VERSION, full_buf.data(), full_buf.size(), pos));

    ObMetaDiskAddr addr;
    write_buffer(full_buf.data(), full_buf.size(), addr);
    sstable.set_addr(addr);

    const int64_t serialize_size = sstable.get_serialize_size(DATA_CURRENT_VERSION);
    vector<char> buf(serialize_size, '\0');
    pos = 0;
    ASSERT_SUCC(sstable.serialize(DATA_CURRENT_VERSION, buf.data(), buf.size(), pos));

    ObSSTable sstable2;
    pos = 0;
    ASSERT_SUCC(sstable2.deserialize(allocator, buf.data(), buf.size(), pos));
    ASSERT_FALSE(sstable2.is_loaded());

    ObSSTableMetaHandle meta_handle;
    ASSERT_SUCC(sstable2.get_meta(meta_handle));
    const ObSSTableMacroInfo &macro_info = meta_handle.get_sstable_meta().get_macro_info();
    int64_t cnt = 0;
    ASSERT_SUCC(macro_info.get_data_block_count(cnt));
    ASSERT_EQ(data_blk_cnt, cnt);
    cnt = 0;
    ASSERT_SUCC(macro_info.get_other_block_count(cnt));
    ASSERT_EQ(other_blk_cnt, cnt);

    ObStorageMetaHandle handle;
    ASSERT_SUCC(ObCacheSSTableHelper::load_sstable(addr, false, handle));
    ObSSTable *sstable_ptr = nullptr;
    ASSERT_SUCC(handle.get_sstable(sstable_ptr));
    ASSERT_NE(nullptr, sstable_ptr);
    {
      ObLinkedMacroInfoWriteParam param;
      param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
      param.tablet_id_ = ObTabletID(200001);
      param.tablet_private_transfer_epoch_ = 0;
      ObSharedObjectsWriteCtx linked_block_write_ctx;
      int64_t macro_start_seq = 0;
      ASSERT_SUCC(sstable_ptr->persist_linked_block_if_need(allocator, param, macro_start_seq, linked_block_write_ctx));

      const int64_t full_serialize_size = sstable.get_full_serialize_size(DATA_CURRENT_VERSION);
      vector<char> full_buf(full_serialize_size, '\0');
      int64_t pos = 0;
      ASSERT_SUCC(sstable_ptr->serialize_full_table(DATA_CURRENT_VERSION, full_buf.data(), full_buf.size(), pos));
      ObSSTableMetaHandle meta_handle2;
      ASSERT_SUCC(sstable_ptr->get_meta(meta_handle2));
      const ObSSTableMacroInfo &macro_info2 = meta_handle2.get_sstable_meta().get_macro_info();
      ASSERT_EQ(macro_info.entry_id_, macro_info2.entry_id_);
      int64_t cnt = 0;
      ASSERT_SUCC(macro_info2.get_data_block_count(cnt));
      ASSERT_EQ(data_blk_cnt, cnt);
      cnt = 0;
      ASSERT_SUCC(macro_info2.get_other_block_count(cnt));
      ASSERT_EQ(other_blk_cnt, cnt);
    }
  }
}
} // end namespace unittest
} // end namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -f test_sstable_meta_info.log*");
  OB_LOGGER.set_file_name("test_sstable_meta_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}