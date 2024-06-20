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

#include <gtest/gtest.h>
#include <codecvt>
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_lob_common.h"
#include "storage/lob/ob_lob_manager.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/random/ob_random.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/lob/ob_lob_piece.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/objectpool/ob_server_object_pool.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace blocksstable;
using namespace share::schema;

namespace unittest
{

static ObSimpleMemLimitGetter getter;

class TestLobManager : public ::testing::Test
{
public:
  TestLobManager();
  virtual ~TestLobManager() = default;
public:
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
      [](transaction::ObTransID &tx_id) { tx_id = transaction::ObTransID(1001); return OB_SUCCESS; };
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
  virtual void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());

  }

  virtual void TearDown()
  {

  }
public:
  void build_lob_meta_row(ObIAllocator& allocator,
    ObLobMetaInfo& info,
    ObStoreRow *&row);
  void insert_lob_meta(MockObAccessService *access_service,
    ObIAllocator& allocator,
    ObLobMetaInfo& info);
  void scan_lob_meta(ObIAllocator& allocator,
    uint64_t lob_id,
    uint64_t byte_size,
    uint64_t offset,
    uint64_t len,
    uint64_t expect_cnt,
    bool is_reverse,
    bool set_uft8,
    ObString& out_data);
  void prepare_random_data(
    ObIAllocator& allocator,
    uint64_t len,
    char **data);
  // void wirte_data_to_lob_oper(
  //   char *data,
  //   uint64_t offset,
  //   uint64_t len,
  //   MacroBlockId &id);
  void scan_check_lob_data(
    char *data,
    ObIAllocator &allocator,
    uint64_t lob_id,
    uint64_t byte_size,
    uint64_t offset,
    uint64_t len,
    uint64_t meta_cnt);
  void flush_lob_piece(
    ObIAllocator &allocator,
    ObLobPieceInfo& info);
  void build_lob_piece_row(
    ObIAllocator& allocator,
    ObLobPieceInfo& info,
    ObStoreRow *&row);
  void insert_lob_piece(
    MockObAccessService *access_service,
    ObIAllocator& allocator,
    ObLobPieceInfo& info);
  int random_range(const int low, const int high);
  void gen_random_unicode_string(const int len, char *res, int &real_len);
  void scan_check_lob_data_uft8(
    char *data,
    ObIAllocator &allocator,
    uint64_t lob_id,
    uint64_t byte_size,
    uint64_t offset,
    uint64_t char_len,
    uint64_t byte_len,
    uint64_t meta_cnt);
  void lob_write(
    ObIAllocator& allocator,
    ObLobCommon *lob_common,
    uint64_t handle_size,
    uint64_t offset,
    uint64_t len,
    bool set_uft8,
    ObString& data);
protected:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
};

TestLobManager::TestLobManager()
  // : TestDataFilePrepare(&getter, "TestLobManager", 2 * 1024 * 1024, 2048),
    : tenant_id_(OB_SYS_TENANT_ID),
    ls_id_(TestLobCommon::TEST_LOB_LS_ID),
    tablet_id_(TestLobCommon::TEST_LOB_TABLE_ID),
    lob_meta_tablet_id_(TestLobCommon::TEST_LOB_META_TABLE_ID),
    lob_piece_tablet_id_(TestLobCommon::TEST_LOB_PIECE_TABLE_ID)
{
  // tenant_id_ = OB_SYS_TENANT_ID;
  // ls_id_ = TestLobCommon::TEST_LOB_LS_ID;
  // tablet_id_ = TestLobCommon::TEST_LOB_TABLE_ID;
  // lob_meta_tablet_id_ = TestLobCommon::TEST_LOB_META_TABLE_ID;
}

int TestLobManager::random_range(const int low, const int high)
{
  return std::rand() % (high - low) + low;
}

void TestLobManager::gen_random_unicode_string(const int len, char *res, int &real_len)
{
  int i = 0;
  int unicode_point = 0;
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
  for (i = 0; i < len; ) {
    const int bytes = random_range(1, 7);
    if (bytes < 4) {
      unicode_point = random_range(0, 127);
    } else if (bytes < 6) {
      unicode_point = random_range(0xFF, 0xFFFF);
    } else if (bytes < 7) {
      unicode_point = random_range(0XFFFF, 0X10FFFF);
    }
    std::string utf_str = converter.to_bytes(unicode_point);
    //fprintf(stdout, "code_point=%d\n", unicode_point);
    //fprintf(stdout, "utf8_str=%s\n", utf_str.c_str());
    for (int j = 0; j < utf_str.size(); ++j) {
      res[i] = utf_str[j];
      i++;
    }
  }
  real_len = i;
}

void TestLobManager::flush_lob_piece(
    ObIAllocator &allocator,
    ObLobPieceInfo& info)
{
  ObLobManager *mgr = MTL(ObLobManager*);
  ObStoreRow *row = NULL;
  build_lob_piece_row(allocator, info, row);
  // ASSERT_EQ(OB_SUCCESS, mgr->flush(tablet_id_, row->row_val_));
}

void TestLobManager::scan_check_lob_data(
    char *data,
    ObIAllocator &allocator,
    uint64_t lob_id,
    uint64_t byte_size,
    uint64_t offset,
    uint64_t len,
    uint64_t meta_cnt)
{
  char *ptr = reinterpret_cast<char*>(allocator.alloc(len));
  ObString out_data;
  out_data.assign_buffer(ptr, len);
  printf("[SCAN] scan [%lu, %lu]:\n", offset, offset + len);
  scan_lob_meta(allocator, lob_id, byte_size, offset, len, meta_cnt, false, false, out_data);
  ASSERT_EQ(out_data.length(), len);
  char *aptr = out_data.ptr();
  for (uint64_t i = 0; i < len; i++) {
    ASSERT_EQ(aptr[i], data[offset+i]);
  }
  ASSERT_EQ(MEMCMP(out_data.ptr(), data + offset, len), 0);
  allocator.free(ptr);
}

void TestLobManager::scan_check_lob_data_uft8(
    char *data,
    ObIAllocator &allocator,
    uint64_t lob_id,
    uint64_t byte_size,
    uint64_t offset,
    uint64_t char_len,
    uint64_t byte_len,
    uint64_t meta_cnt)
{
  char *ptr = reinterpret_cast<char*>(allocator.alloc(byte_len));
  ObString out_data;
  out_data.assign_buffer(ptr, byte_len);
  printf("[SCAN] scan [%lu, %lu]:\n", offset, offset + char_len);
  scan_lob_meta(allocator, lob_id, byte_size, offset, char_len, meta_cnt, false, true, out_data);
  ASSERT_EQ(out_data.length(), byte_len);
  char *aptr = out_data.ptr();
  for (uint64_t i = 0; i < byte_len; i++) {
    ASSERT_EQ(aptr[i], data[offset+i]);
  }
  ASSERT_EQ(MEMCMP(out_data.ptr(), data + offset, byte_len), 0);
  int ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, out_data.ptr(), out_data.length(), data + offset, byte_len);
  fprintf(stdout, "char set cmp ret:%d\n", ret);
  ASSERT_EQ(0, ret);

  allocator.free(ptr);
}

// void TestLobManager::wirte_data_to_lob_oper(
//     char *data,
//     uint64_t offset,
//     uint64_t len,
//     MacroBlockId &id)
// {
//   ObLobManager *mgr = MTL(ObLobManager*);
//   ObLobCtx lob_ctx;
//   common::ObTabletID tablet_id(TestLobCommon::TEST_LOB_TABLE_ID);
//   ASSERT_EQ(OB_SUCCESS, mgr->lob_ctxs_.get(tablet_id, lob_ctx));
//   ASSERT_NE(lob_ctx.lob_oper_, nullptr);

//   ASSERT_EQ(OB_SUCCESS, lob_ctx.lob_oper_->get_new_macro_id(id));
//   LobPieceOperInfo in;
//   in.macro_id_ = id;
//   in.data_ = data + offset;
//   in.offset_ = 0;
//   in.len_ = len;
//   in.bytes_len_ = 0;

//   LobPieceOperInfo out;
//   ASSERT_EQ(OB_SUCCESS, lob_ctx.lob_oper_->write(in, out));
//   id = out.macro_id_;
// }

void TestLobManager::prepare_random_data(
    ObIAllocator& allocator,
    uint64_t len,
    char **data)
{
  char *ptr = reinterpret_cast<char*>(allocator.alloc(len));
  ASSERT_NE(ptr, nullptr);
  for (uint64_t i = 0; i < len; i++) {
    ptr[i] = ObRandom::rand('a', 'z');
  }
  *data = ptr;
}

void TestLobManager::build_lob_piece_row(
    ObIAllocator& allocator,
    ObLobPieceInfo& info,
    ObStoreRow *&row)
{
  ASSERT_EQ(OB_SUCCESS, malloc_store_row(allocator, 3, row, FLAT_ROW_STORE));
  row->flag_.set_flag(ObDmlFlag::DF_INSERT);
  for (int64_t i = 0; i < 3; ++i) {
    row->row_val_.cells_[i].set_nop_value();
  }

  { // piece_id
    row->row_val_.cells_[0].set_uint64(info.piece_id_);
  }
  { // len
    row->row_val_.cells_[1].set_uint32(info.len_);
  }
  { // macro_id
    int64_t pos = 0;
    // do serialize
    int64_t slen = info.macro_id_.get_serialize_size();
    char *buf = reinterpret_cast<char*>(allocator.alloc(slen));
    ASSERT_NE(nullptr, buf);
    ASSERT_EQ(OB_SUCCESS, info.macro_id_.serialize(buf, slen, pos));
    row->row_val_.cells_[2].set_varchar(buf, pos);
    row->row_val_.cells_[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  }
}

void TestLobManager::insert_lob_piece(
    MockObAccessService *access_service,
    ObIAllocator& allocator,
    ObLobPieceInfo& info)
{
  ASSERT_NE(nullptr, access_service);

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(lob_piece_tablet_id_, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert rows
  ObMockNewRowIterator mock_iter;
  ObSEArray<uint64_t, 512> column_ids;
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 0); // pk
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 1); // c1
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 2); // c2

  // build row
  ObStoreRow *row = NULL;
  build_lob_piece_row(allocator, info, row);
  ASSERT_EQ(OB_SUCCESS, mock_iter.iter_.add_row(row));
  //ASSERT_EQ(OB_SUCCESS, mock_iter.from(TestDmlCommon::data_row_str));

  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);

  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. create savepoint (can be rollbacked)
  ObTxParam tx_param;
  TestDmlCommon::build_tx_param(tx_param);
  ObTxSEQ savepoint;
  ASSERT_EQ(OB_SUCCESS, tx_service->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
  // 3. acquire snapshot (write also need snapshot)
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

  // 4. storage dml
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  dml_param.is_total_quantity_log_ = false;
  dml_param.tz_info_ = NULL;
  dml_param.sql_mode_ = SMO_DEFAULT;
  dml_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
  dml_param.snapshot_ = read_snapshot;

  share::schema::ObTableDMLParam table_dml_param(allocator);

  share::schema::ObTableSchema table_schema;
  TestLobCommon::build_lob_piece_table_schema(tenant_id_, table_schema);

  ObSEArray<const ObTableSchema *, 4> index_schema_array;

  ASSERT_EQ(OB_SUCCESS, table_dml_param.convert(&table_schema, 1, column_ids));
  dml_param.table_param_ = &table_dml_param;

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, access_service->insert_rows(ls_id_, lob_piece_tablet_id_,
      *tx_desc, dml_param, column_ids, &mock_iter, affected_rows));

  ASSERT_EQ(1, affected_rows);

  // 5. serialize trans result and ship
  // 6. merge result if necessar

  // 7. rollback if failed
  // expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  // ASSERT_EQ(OB_SUCCESS, tx_service->rollback_to_implicit_savepoint(*tx_desc, savepoint, expire_ts, NULL));

  // 8. submit transaction, or rollback
  expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

  // 9. release tx desc
  tx_service->release_tx(*tx_desc);
}

void TestLobManager::build_lob_meta_row(
    ObIAllocator& allocator,
    ObLobMetaInfo& info,
    ObStoreRow *&row)
{
  ASSERT_EQ(OB_SUCCESS, malloc_store_row(allocator, ObLobMetaUtil::LOB_META_COLUMN_CNT, row, FLAT_ROW_STORE));
  row->flag_.set_flag(ObDmlFlag::DF_INSERT);
  for (int64_t i = 0; i < ObLobMetaUtil::LOB_META_COLUMN_CNT; ++i) {
    row->row_val_.cells_[i].set_nop_value();
  }

  { // lob_id
    row->row_val_.cells_[0].set_varchar(reinterpret_cast<char*>(&info.lob_id_), sizeof(ObLobId));
    row->row_val_.cells_[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  }
  { // seq_id
    row->row_val_.cells_[1].set_varchar(info.seq_id_);
    row->row_val_.cells_[1].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  }
  { // byte_len
    row->row_val_.cells_[2].set_uint32(info.byte_len_);
  }
  { // char_len
    row->row_val_.cells_[3].set_uint32(info.char_len_);
  }
  { // piece_id
    row->row_val_.cells_[4].set_uint64(info.piece_id_);
  }
  { // lob_data
    row->row_val_.cells_[5].set_varchar(info.lob_data_);
    row->row_val_.cells_[5].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  }
}

void TestLobManager::insert_lob_meta(
    MockObAccessService *access_service,
    ObIAllocator& allocator,
    ObLobMetaInfo& info)
{
  ASSERT_NE(nullptr, access_service);

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(lob_meta_tablet_id_, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert rows
  ObMockNewRowIterator mock_iter;
  ObSEArray<uint64_t, 512> column_ids;
  for (int i = 0; i < ObLobMetaUtil::LOB_META_COLUMN_CNT; i++) {
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + i);
  }

  // build row
  ObStoreRow *row = NULL;
  build_lob_meta_row(allocator, info, row);
  ASSERT_EQ(OB_SUCCESS, mock_iter.iter_.add_row(row));
  //ASSERT_EQ(OB_SUCCESS, mock_iter.from(TestDmlCommon::data_row_str));

  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);

  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. create savepoint (can be rollbacked)
  ObTxParam tx_param;
  TestDmlCommon::build_tx_param(tx_param);
  ObTxSEQ savepoint;
  ASSERT_EQ(OB_SUCCESS, tx_service->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
  // 3. acquire snapshot (write also need snapshot)
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

  // 4. storage dml
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  dml_param.is_total_quantity_log_ = false;
  dml_param.tz_info_ = NULL;
  dml_param.sql_mode_ = SMO_DEFAULT;
  dml_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
  dml_param.snapshot_ = read_snapshot;

  share::schema::ObTableDMLParam table_dml_param(allocator);

  share::schema::ObTableSchema table_schema;
  TestLobCommon::build_lob_meta_table_schema(tenant_id_, table_schema);

  ObSEArray<const ObTableSchema *, 4> index_schema_array;

  ASSERT_EQ(OB_SUCCESS, table_dml_param.convert(&table_schema, 1, column_ids));
  dml_param.table_param_ = &table_dml_param;

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, access_service->insert_rows(ls_id_, lob_meta_tablet_id_,
      *tx_desc, dml_param, column_ids, &mock_iter, affected_rows));

  ASSERT_EQ(1, affected_rows);

  // 5. serialize trans result and ship
  // 6. merge result if necessary

  // 7. end data access, if failed, rollback
  // expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  // ASSERT_EQ(OB_SUCCESS, tx_service->rollback_to_implicit_savepoint(*tx_desc, savepoint, expire_ts, NULL));

  // 8. submit transaction, or rollback
  expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

  // 9. release tx desc
  tx_service->release_tx(*tx_desc);
}

// void TestLobManager::build_lob_piece_param(
//   share::schema::ObTableParam& param,
//   ObString& out_data)
// {
//   // prepare table schema
//   share::schema::ObTableSchema table_schema;
//   TestLobCommon::build_lob_piece_table_schema(tenant_id_, table_schema);

//   // build table param
//   ObSArray<uint64_t> colunm_ids;
//   colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 0);
//   colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 1);
//   colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 2);
//   ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(table_schema, colunm_ids, table_param));
// }

void TestLobManager::lob_write(
    ObIAllocator& allocator,
    ObLobCommon *lob_common,
    uint64_t handle_size,
    uint64_t offset,
    uint64_t len,
    bool set_uft8,
    ObString& data)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObLobManager *mgr = MTL(ObLobManager*);
  uint64_t byte_size = lob_common->get_byte_size(handle_size);
  ObLobData *lob_data = reinterpret_cast<ObLobData*>(lob_common->buffer_);
  lob_data->id_.tablet_id_ = tablet_id_.id();
    // prepare table schema
  share::schema::ObTableSchema table_schema;
  TestLobCommon::build_lob_meta_table_schema(tenant_id_, table_schema);

  // build table param
  share::schema::ObTableParam table_param(allocator);
  ObSArray<uint64_t> colunm_ids;
  for (int i = 0; i < ObLobMetaUtil::LOB_META_COLUMN_CNT; i++) {
    colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + i);
  }
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(table_schema, colunm_ids, table_param));

  ObExecContext exec_ctx(allocator);
  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. get read snapshot
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + 1200 * 1000 * 1000;
  ObTxReadSnapshot read_snapshot;
  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));


  // prepare param
  ObLobAccessParam param;
  param.tx_desc_ = tx_desc;
  param.snapshot_ = read_snapshot;
  param.tx_id_ = tx_desc->get_tx_id();
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.meta_table_schema_ = &table_schema;
  param.meta_tablet_param_ = &table_param;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  if (set_uft8) {
    param.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else {
    param.coll_type_ = CS_TYPE_BINARY;
  }
  param.lob_common_ = lob_common;
  param.byte_size_ = byte_size;
  param.handle_size_ = handle_size;
  param.timeout_ = expire_ts;
  param.offset_ = offset;
  param.len_ = len;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, data));
}

void TestLobManager::scan_lob_meta(
    ObIAllocator& allocator,
    uint64_t lob_id,
    uint64_t byte_size,
    uint64_t offset,
    uint64_t len,
    uint64_t expect_cnt,
    bool is_reverse,
    bool set_uft8,
    ObString& out_data)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObLobManager *mgr = MTL(ObLobManager*);
  char lob_data[1024];
  ObString ld;
  ld.assign_ptr(lob_data + sizeof(ObLobCommon) + sizeof(ObLobData), 1024 - sizeof(ObLobCommon) - sizeof(ObLobData));
  ObLobCommon *lob_common = new(lob_data)ObLobCommon();
  lob_common->is_init_ = 1;
  lob_common->in_row_ = 0;
  ObLobData *loc = new(lob_common->buffer_)ObLobData();
  loc->id_.tablet_id_ = tablet_id_.id();
  loc->id_.lob_id_ = lob_id;
  loc->byte_size_ = byte_size;

  // prepare table schema
  share::schema::ObTableSchema table_schema;
  TestLobCommon::build_lob_meta_table_schema(tenant_id_, table_schema);

  // build table param
  share::schema::ObTableParam table_param(allocator);
  ObSArray<uint64_t> colunm_ids;
  for (int i = 0; i < ObLobMetaUtil::LOB_META_COLUMN_CNT; i++) {
    colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + i);
  }
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(table_schema, colunm_ids, table_param));

  // prepare piece table schema
  share::schema::ObTableSchema ptable_schema;
  TestLobCommon::build_lob_piece_table_schema(tenant_id_, ptable_schema);

  // build table param
  share::schema::ObTableParam ptable_param(allocator);
  ObSArray<uint64_t> pcolunm_ids;
  pcolunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 0);
  pcolunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 1);
  pcolunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 2);
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(ptable_schema, pcolunm_ids, ptable_param));

  ObExecContext exec_ctx(allocator);
  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. get read snapshot
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  ObTxReadSnapshot read_snapshot;
  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));


  // prepare param
  ObLobAccessParam param;
  param.tx_desc_ = tx_desc;
  param.snapshot_ = read_snapshot;
  param.tx_id_ = tx_desc->get_tx_id();
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.meta_table_schema_ = &table_schema;
  param.piece_table_schema_ = &ptable_schema;
  param.meta_tablet_param_ = &table_param;
  param.piece_tablet_param_ = &ptable_param;
  param.ls_id_ = ls_id_;
  param.asscess_ptable_ = true;
  param.tablet_id_ = tablet_id_;
  if (set_uft8) {
    param.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else {
    param.coll_type_ = CS_TYPE_BINARY;
  }
  param.lob_common_ = lob_common;
  param.byte_size_ = byte_size;
  param.handle_size_ = 1024;
  param.timeout_ = expire_ts;
  param.scan_backward_ = is_reverse;
  param.offset_ = offset;
  param.len_ = len;
  ObLobQueryIter *iter = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, iter));
  {
    int ret = OB_SUCCESS;
    int cnt = 0;
    while (OB_SUCC(ret)) {
      ObLobQueryResult result;
      ret = iter->get_next_row(result);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
          // ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else {
        cnt++;
        int *tseq_id = (reinterpret_cast<int*>(result.meta_result_.info_.seq_id_.ptr()));
        char seq_str[10] = {'\0'};
        int seq_cnt = result.meta_result_.info_.seq_id_.length() / sizeof(int);
        int seq_str_pos = 0;
        for (int i = 0; i < seq_cnt; i++) {
          if (i == 0) {
            seq_str_pos = snprintf(seq_str, 10, "%d", tseq_id[i]);
          } else {
            int tpos = snprintf(seq_str + seq_str_pos, 10 - seq_str_pos, ".%d", tseq_id[i]);
            seq_str_pos += tpos;
          }
        }
        printf("[META] id : %lu, seq_id : %s, st : %u, len : %u, piece_id : %lu\n", result.meta_result_.info_.lob_id_.lob_id_, seq_str,
               result.meta_result_.st_, result.meta_result_.len_, result.meta_result_.info_.piece_id_);
        // get data
        ASSERT_EQ(OB_SUCCESS, mgr->get_real_data(param, result, out_data));
      }
    }
    if (expect_cnt != 0) { // do not need to check when 0
      ASSERT_EQ(expect_cnt, cnt);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  if (iter != NULL) {
    iter->reset();
  }
  // ASSERT_EQ(0, out_data.length());

  // 9. release tx desc
  tx_service->release_tx(*tx_desc);
}

// void TestLobManager::
TEST_F(TestLobManager, basic)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;
  ret = TestLobCommon::create_data_tablet(tenant_id_, ls_id_, tablet_id_, lob_meta_tablet_id_, lob_piece_tablet_id_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mock ls tablet service and access service
  ObLSTabletService *tablet_service = nullptr;
  ret = TestDmlCommon::mock_ls_tablet_service(ls_id_, tablet_service);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, tablet_service);

  MockObAccessService *access_service = nullptr;
  ret = TestDmlCommon::mock_access_service(tablet_service, access_service);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, access_service);

  // assume 1M * 10
  printf("[test] 10M lob binary\n");
  {
    // uint64_t lob_id = 213;
    // // prepare data
    // char *data;
    // uint64_t total_len = 10 * 1024 * 1024; // 10M
    // prepare_random_data(allocator, total_len, &data);
    // ObLobMetaInfo infos[10];
    // ObLobPieceInfo pinfos[10];
    // for (int i = 0; i < 10; i++) {
    //   ObLobMetaInfo *info = &infos[i];
    //   info->lob_id_ = lob_id;
    //   int seq_num[2];
    //   seq_num[0] = i/2;
    //   seq_num[1] = 5;
    //   if (i%2 != 0) {
    //     info->seq_id_.assign_ptr(reinterpret_cast<char*>(seq_num), sizeof(int) * 2);
    //   } else {
    //     info->seq_id_.assign_ptr(reinterpret_cast<char*>(seq_num), sizeof(int));
    //   }
    //   info->byte_len_ = 1 * 1024 * 1024;
    //   info->char_len_ = 1 * 1024 * 1024;
    //   info->piece_id_ = i;
    //   info->lob_data_.assign_ptr(nullptr, 0);
    //   // wirte_data_to_lob_oper(data, 1024*1024*i, info->byte_len_, pinfos[i].macro_id_);
    //   pinfos[i].piece_id_ = i;
    //   pinfos[i].len_ = info->byte_len_;
    //   insert_lob_piece(access_service, allocator, pinfos[i]);
    //   insert_lob_meta(access_service, allocator, *info);
    // }

    // // table scan
    // // full scan
    // scan_check_lob_data(data, allocator, lob_id, total_len, 0, 10*1024*1024, 10);
    // // range covers multi meta
    // scan_check_lob_data(data, allocator, lob_id, total_len, 1, 3*1024*1024, 4);
    // // range covered by one meta
    // scan_check_lob_data(data, allocator, lob_id, total_len, 1, 512*1024, 1);
    // scan_check_lob_data(data, allocator, lob_id, total_len, 1024*1024, 1024*1024, 1);

    // // do flush data
    // for (int i = 0; i < 10; i++) {
    //   flush_lob_piece(allocator, pinfos[i]);
    // }
    // printf("do read after flush.\n");
    // // table scan
    // // full scan
    // scan_check_lob_data(data, allocator, lob_id, total_len, 0, 10*1024*1024, 10);
    // // range covers multi meta
    // scan_check_lob_data(data, allocator, lob_id, total_len, 1, 3*1024*1024, 4);
    // // range covered by one meta
    // scan_check_lob_data(data, allocator, lob_id, total_len, 1, 512*1024, 1);
    // scan_check_lob_data(data, allocator, lob_id, total_len, 1024*1024, 1024*1024, 1);
    // allocator.free(data);
  }

  // insert lob2 with charset utf8
  printf("[test] 10M lob utf8\n");
  {
    // uint64_t lobid2 = 111;
    // uint64_t total_len2 = 10 * 1024 * 1024; // 10M
    // // assume lob meta 10
    // char *data2 = reinterpret_cast<char*>(allocator.alloc(sizeof(char) * total_len2));
    // ASSERT_NE(nullptr, data2);
    // ObLobMetaInfo infos2[10];
    // ObLobPieceInfo pinfos2[10];
    // uint64_t total_char_len = 0;
    // int doffset = 0;
    // int piece_id_base = 100;
    // for (int i = 0; i < 10; i++) {
    //   int real_len = 0;
    //   gen_random_unicode_string(1048000, data2 + doffset, real_len);
    //   ObLobMetaInfo *info = &infos2[i];
    //   info->lob_id_ = lobid2;
    //   int seq_num[2];
    //   seq_num[0] = i/2;
    //   seq_num[1] = 5;
    //   if (i%2 != 0) {
    //     info->seq_id_.assign_ptr(reinterpret_cast<char*>(seq_num), sizeof(int) * 2);
    //   } else {
    //     info->seq_id_.assign_ptr(reinterpret_cast<char*>(seq_num), sizeof(int));
    //   }
    //   info->byte_len_ = real_len;
    //   info->char_len_ = ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, data2 + doffset, real_len);
    //   total_char_len += info->char_len_;
    //   info->piece_id_ = piece_id_base + i;
    //   info->lob_data_.assign_ptr(nullptr, 0);
    //   wirte_data_to_lob_oper(data2, doffset, info->byte_len_, pinfos2[i].macro_id_);
    //   pinfos2[i].piece_id_ = info->piece_id_;
    //   pinfos2[i].len_ = info->byte_len_;
    //   insert_lob_piece(access_service, allocator, pinfos2[i]);
    //   insert_lob_meta(access_service, allocator, *info);
    //   doffset += real_len;
    // }
    // // full scan
    // scan_check_lob_data_uft8(data2, allocator, lobid2, doffset, 0, total_char_len, doffset, 10);
    // allocator.free(data2);
  }

  // insert lob3 with inline lob meta data
  printf("[test] 2.56M lob utf8 inline meta data\n");
  {
    uint64_t lobid3 = 222;
    uint64_t total_len3 = 10 * 1024 * 1024; // 10M
    // assume lob meta 10
    char *data3 = reinterpret_cast<char*>(allocator.alloc(sizeof(char) * total_len3));
    ASSERT_NE(nullptr, data3);
    ObLobMetaInfo infos2[10];
    // ObLobPieceInfo pinfos2[10];
    uint64_t total_char_len = 0;
    int doffset = 0;
    for (int i = 0; i < 10; i++) {
      int real_len = 0;
      gen_random_unicode_string(256000, data3 + doffset, real_len);
      ObLobMetaInfo *info = &infos2[i];
      info->lob_id_.tablet_id_ = tablet_id_.id();
      info->lob_id_.lob_id_ = lobid3;
      int seq_num[2];
      seq_num[0] = i/2;
      seq_num[1] = 5;
      if (i%2 != 0) {
        info->seq_id_.assign_ptr(reinterpret_cast<char*>(seq_num), sizeof(int) * 2);
      } else {
        info->seq_id_.assign_ptr(reinterpret_cast<char*>(seq_num), sizeof(int));
      }
      info->byte_len_ = real_len;
      info->char_len_ = ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, data3 + doffset, real_len);
      total_char_len += info->char_len_;
      info->piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
      info->lob_data_.assign_ptr(data3 + doffset, info->byte_len_);
      // wirte_data_to_lob_oper(data3, doffset, info->byte_len_, pinfos2[i].macro_id_);
      // pinfos2[i].piece_id_ = info->piece_id_;
      // pinfos2[i].len_ = info->byte_len_;
      // insert_lob_piece(access_service, allocator, pinfos2[i]);
      insert_lob_meta(access_service, allocator, *info);
      doffset += real_len;
    }
    // full scan
    scan_check_lob_data_uft8(data3, allocator, lobid3, doffset, 0, total_char_len, doffset, 10);
    allocator.free(data3);
  }

  printf("[test] write 10M lob utf8 inline meta data\n");
  {
    // uint64_t lobid3 = 233;
    // uint64_t total_len3 = 10 * 1024 * 1024; // 10M
    // char *data3 = reinterpret_cast<char*>(allocator.alloc(sizeof(char) * total_len3));
    // ASSERT_NE(nullptr, data3);
    // int real_len = 0;
    // gen_random_unicode_string(total_len3, data3, real_len);
    // uint64_t total_char_len = ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, data3, real_len);
    // // build lob common
    // char lob_header[100];
    // ObLobCommon *lob_common = new(lob_header)ObLobCommon();
    // lob_common->is_init_ = 1;
    // ObLobData *lob_data = new(lob_common->buffer_)ObLobData();
    // lob_data->id_.lob_id_ = lobid3;
    // ObString in_data(real_len, data3);
    // lob_write(allocator, lob_common, 100, 0, total_char_len, true, in_data);
    // // full scan
    // char *out_data = reinterpret_cast<char*>(allocator.alloc(sizeof(char) * total_len3));
    // ObString out_str;
    // out_str.assign_buffer(out_data, total_len3);
    // scan_check_lob_data_uft8(data3, allocator, lobid3, real_len, 0, total_char_len, real_len, 0);
    // allocator.free(data3);
  }

  // clean env
  TestDmlCommon::delete_mocked_access_service(access_service);
  TestDmlCommon::delete_mocked_ls_tablet_service(tablet_service);
  // for exist
  // the iter has store ctx and store ctx has one ls handle.
  // iter->reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_));
}

// TEST_F(TestLobManager, basic2)
// {
  // EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  // ObLobManager *mgr = MTL(ObLobManager*);
  // ASSERT_NE(nullptr, mgr);
  //
  // MTL(ObLSSerivce*)
  // do ...
  //
  // ObObj str1;
  // str1.set_varchar("发生什么事了");
  // str1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  // ObString data;
  // str1.get_string(data);
  // // test for charset lob
  // // check "生什么事"
  // size_t st = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, data.ptr(), data.length(), 1);
  // size_t len = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, data.ptr() + st, data.length() - st, 4);
  // char buf[100];
  // memset(buf, 0, 100);
  // ObString dest;
  // dest.assign_buffer(buf, 99);
  // dest.write(data.ptr() + st, len);
  // printf("[%zu, %zu] : %s\n", st, len, dest.ptr());
// }



// append / erase
TEST_F(TestLobManager, DISABLED_basic3)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ret = TestLobCommon::create_data_tablet(tenant_id_, ls_id_, tablet_id_, lob_meta_tablet_id_, lob_piece_tablet_id_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mock ls tablet service and access service
  ObLSTabletService *tablet_service = nullptr;
  ret = TestDmlCommon::mock_ls_tablet_service(ls_id_, tablet_service);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, tablet_service);

  MockObAccessService *access_service = nullptr;
  ret = TestDmlCommon::mock_access_service(tablet_service, access_service);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, access_service);

  ObLobManager *mgr = MTL(ObLobManager*);
  char lob_data[1024];
  uint64_t lob_id = 213;
  ObString ld;
  ld.assign_ptr(lob_data + sizeof(ObLobCommon) + sizeof(ObLobData), 1024 - sizeof(ObLobCommon) - sizeof(ObLobData));
  // TODO mock lob_id
  ObLobCommon *lob_common = new(lob_data)ObLobCommon();
  lob_common->is_init_ = 1;
  lob_common->in_row_ = 0;
  ObLobData *loc = new(lob_common->buffer_)ObLobData();
  loc->id_.tablet_id_ = tablet_id_.id();
  loc->id_.lob_id_ = lob_id;

  // prepare table schema
  share::schema::ObTableSchema table_schema;
  TestLobCommon::build_lob_meta_table_schema(tenant_id_, table_schema);

  // build table param
  share::schema::ObTableParam table_param(allocator);
  ObSArray<uint64_t> colunm_ids;
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 0);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 1);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 2);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 3);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 4);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 5);
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(table_schema, colunm_ids, table_param));

  // prepare piece table schema
  share::schema::ObTableSchema ptable_schema;
  TestLobCommon::build_lob_piece_table_schema(tenant_id_, ptable_schema);

  // build table param
  share::schema::ObTableParam ptable_param(allocator);
  ObSArray<uint64_t> pcolunm_ids;
  pcolunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 0);
  pcolunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 1);
  pcolunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 2);
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(ptable_schema, pcolunm_ids, ptable_param));


  uint64_t total_len = 10 * 1024 * 1024; // 10M
  // assume lob meta 10
  char *data;
  bool is_unicode = true;

  uint32_t data_len = total_len;
  if (is_unicode) {
    data = reinterpret_cast<char*>(allocator.alloc(total_len));
    ASSERT_NE(nullptr, data);
    int real_len = 0;
    gen_random_unicode_string(256 * 1024 * 16, data, real_len);
    data_len = real_len;
  } else {
    prepare_random_data(allocator, total_len, &data);
  }
  loc->byte_size_ = data_len;

  auto tx_service = MTL(transaction::ObTransService*);

  {
    ObString write_data;
    write_data.assign_ptr(data, data_len);

    // 1. get tx desc
    transaction::ObTxDesc *tx_desc = nullptr;
    ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));
    // 2. create savepoint (can be rollbacked)
    ObTxParam tx_param;
    TestDmlCommon::build_tx_param(tx_param);
    ObTxSEQ savepoint;
    ASSERT_EQ(OB_SUCCESS, tx_service->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
    // 3. acquire snapshot (write also need snapshot)
    ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
    int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

    ObLobAccessParam param;
    param.tx_desc_ = tx_desc;
    param.sql_mode_ = SMO_DEFAULT;
    param.allocator_ = &allocator;
    param.meta_table_schema_ = &table_schema;
    param.piece_table_schema_ = &ptable_schema;
    param.meta_tablet_param_ = &table_param;
    param.piece_tablet_param_ = &ptable_param;
    param.ls_id_ = ls_id_;
    param.scan_backward_ = true;
    param.tablet_id_ = tablet_id_;
    param.lob_common_ = lob_common;
    param.handle_size_ = 1024;
    // TODO param.byte_size_ = ?
    param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
    param.asscess_ptable_ = false;
    param.snapshot_ = read_snapshot;

    if (is_unicode) {
      param.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    } else {
      param.coll_type_ = CS_TYPE_BINARY; // TODO
    }
    param.offset_ = 0;
    param.len_ = 250 * 1024;

    //
    // ObString wr_data;
    // wr_data.assign_ptr(data, total_len);
    ret = mgr->append(param, write_data);
    ASSERT_EQ(ret, OB_SUCCESS);

    // 5. serialize trans result and ship
    // 6. merge result if necessary

    // 7. end data access, if failed, rollback
    expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ASSERT_EQ(OB_SUCCESS, tx_service->rollback_to_implicit_savepoint(*tx_desc, savepoint, expire_ts, NULL));

    // 8. submit transaction, or rollback
    expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

    // 9. release tx desc
    tx_service->release_tx(*tx_desc);
  }


    //  ObLobMetaInfo infos[10];
    //  ObLobPieceInfo pinfos[10];
    //   for (int i = 0; i < 8; ++i) {
    //     ObLobMetaInfo* info = &infos[i];
    //     info->byte_len_ = 250 * 1024;
    //     info->char_len_ = 250 * 1024;
    //     info->lob_id_ = lob_id;
    //     uint32_t seq_id[2] = {0};
    //     seq_id[0] = 256 * (i + 1);
    //     ObString seq_str;
    //     seq_str.assign_ptr(reinterpret_cast<char*>(seq_id), sizeof(uint32_t));
    //     info->seq_id_ = seq_str;
    //     info->lob_data_.assign_ptr(data + i * 250 * 1024, 250 * 1024);
    //     info->piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    //     insert_lob_meta(access_service, allocator, *info);
    //   }

  {
    // 1. get tx desc
    transaction::ObTxDesc *tx_desc = nullptr;
    ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

    // 2. create savepoint (can be rollbacked)
    ObTxParam tx_param;
    TestDmlCommon::build_tx_param(tx_param);
    ObTxSEQ savepoint;
    ASSERT_EQ(OB_SUCCESS, tx_service->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
    // 3. acquire snapshot (write also need snapshot)
    ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
    int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ObTxReadSnapshot read_snapshot;
    ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

    ObLobAccessParam param;
    param.tx_desc_ = tx_desc;
    param.sql_mode_ = SMO_DEFAULT;
    param.allocator_ = &allocator;
    param.meta_table_schema_ = &table_schema;
    param.piece_table_schema_ = &ptable_schema;
    param.meta_tablet_param_ = &table_param;
    param.piece_tablet_param_ = &ptable_param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.scan_backward_ = false;
    param.lob_common_ = lob_common;
    param.handle_size_ = 1024;
    // TODO param.byte_size_ = ?
    param.asscess_ptable_ = false;
    param.snapshot_ = read_snapshot;
    param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
    if (is_unicode) {
      param.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    } else {
      param.coll_type_ = CS_TYPE_BINARY; // TODO
    }
    param.offset_ = 0;
    param.len_ = 250 * 1024;

    //
    // ObString wr_data;
    // wr_data.assign_ptr(data, total_len);
    ret = mgr->erase(param);
    ASSERT_EQ(ret, OB_SUCCESS);


    // 5. serialize trans result and ship
    // 6. merge result if necessary

    // 7. end data access, if failed, rollback
    expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ASSERT_EQ(OB_SUCCESS, tx_service->rollback_to_implicit_savepoint(*tx_desc, savepoint, expire_ts, NULL));

    // 8. submit transaction, or rollback
    expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

    // 9. release tx desc
    tx_service->release_tx(*tx_desc);

  }


  // clean env
  TestDmlCommon::delete_mocked_access_service(access_service);
  TestDmlCommon::delete_mocked_ls_tablet_service(tablet_service);
}

TEST_F(TestLobManager, inrow_bin_test)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ObLobManager *mgr = MTL(ObLobManager*);
  char *lob_data = reinterpret_cast<char*>(allocator.alloc(4096));
  ObLobCommon *loc = new(lob_data)ObLobCommon();

  char *tmp_buf;
  uint32_t data_len = 900;
  prepare_random_data(allocator, data_len, &tmp_buf);

  ObLobAccessParam param;
  param.tx_desc_ = nullptr;
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.scan_backward_ = false;
  param.lob_common_ = loc;
  param.handle_size_ = 4096;
  param.asscess_ptable_ = false;
  param.coll_type_ = CS_TYPE_BINARY;
  param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  // append [0,400]
  ObString appeng_buf;
  appeng_buf.assign_ptr(tmp_buf, 400);
  ASSERT_EQ(OB_SUCCESS, mgr->append(param, appeng_buf));

  // query check [0,400]
  char *query_buf = reinterpret_cast<char*>(allocator.alloc(4096));
  ObString query_str;
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 400;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf + param.offset_, param.len_), 0);

  // erase [100, 250]
  param.offset_ = 100;
  param.len_ = 150;
  ASSERT_EQ(OB_SUCCESS, mgr->erase(param));
  // query check [0,100],[250,400]
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 400;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(250, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 150), 0);

  // replaced-write [50,100]
  appeng_buf.assign_ptr(tmp_buf + 400, 50);
  param.offset_ = 50;
  param.len_ = 50;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));
  // query check [0,100],[250,900],
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,250]
  // tmpbuf  [0,50]  [400,450]  [250,400]
  ASSERT_EQ(250, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 150), 0);

  // overwrite-no padding [200,300] = tmpbuf[800,900]
  appeng_buf.assign_ptr(tmp_buf + 800, 100);
  param.offset_ = 200;
  param.len_ = 100;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,200]  [200,300]
  // tmpbuf  [0,50]  [400,450]  [250,350]  [800,900]
  ASSERT_EQ(300, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 200, tmp_buf + 800, 100), 0);

  // overwrite with padding [1000, 1100] = tmpbuf[600,700]
  appeng_buf.assign_ptr(tmp_buf + 600, 200); // set data 200, but only write 100
  param.offset_ = 1000;
  param.len_ = 100;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));
  query_str.assign_buffer(query_buf, 4096);
  param.offset_ = 0;
  param.len_ = 2000;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,200]  [200,300] [300,1000] [1000,1100]
  // tmpbuf  [0,50]  [400,450]  [250,350]  [800,900] [0x00]     [600,700]
  ASSERT_EQ(1100, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 200, tmp_buf + 800, 100), 0);
  char empty_buf[1024];
  MEMSET(empty_buf, 0x00, 1024);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 300, empty_buf, 700), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 1000, tmp_buf + 600, 100), 0);
  allocator.free(query_buf);
  allocator.free(lob_data);
}

TEST_F(TestLobManager, inrow_utf8_test)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ObLobManager *mgr = MTL(ObLobManager*);
  char *lob_data = reinterpret_cast<char*>(allocator.alloc(4096));
  ObLobCommon *loc = new(lob_data)ObLobCommon();

  char tmp_buf[1024];
  uint32_t data_len = 1024;
  int real_len = 0;
  gen_random_unicode_string(1000, tmp_buf, real_len);
  size_t char_len = ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, tmp_buf, real_len);

  ObLobAccessParam param;
  param.tx_desc_ = nullptr;
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.scan_backward_ = false;
  param.lob_common_ = loc;
  param.handle_size_ = 4096;
  param.asscess_ptable_ = false;
  param.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  // append char len [0, char_len/2]
  size_t fp_len = char_len / 2;
  size_t byte_st = ObCharset::charpos(param.coll_type_, tmp_buf, real_len, 0);
  size_t byte_len = ObCharset::charpos(param.coll_type_, tmp_buf + byte_st, real_len - byte_st, fp_len);
  ObString appeng_buf;
  appeng_buf.assign_ptr(tmp_buf + byte_st, byte_len);
  param.offset_ = 0;
  param.len_ = fp_len;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));

  // query check [0,400]
  char *query_buf = reinterpret_cast<char*>(allocator.alloc(4096));
  ObString query_str;
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = fp_len;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(byte_len, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf + byte_st, byte_len), 0);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, query_str.ptr(), query_str.length(), tmp_buf + byte_st, byte_len);
  ASSERT_EQ(0, ret);

  // erase [fp/2, 2fp/3]
  param.offset_ = fp_len/2;
  param.len_ = fp_len/6;
  ASSERT_EQ(OB_SUCCESS, mgr->erase(param));
  // query check [0,fp/2],[2fp/3,fp]
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = fp_len;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // compare [0,fp/2]
  byte_st = ObCharset::charpos(param.coll_type_, tmp_buf, real_len, 0);
  byte_len = ObCharset::charpos(param.coll_type_, tmp_buf + byte_st, real_len - byte_st, fp_len/2);
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf + byte_st, byte_len), 0);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, query_str.ptr(), byte_len, tmp_buf + byte_st, byte_len);
  ASSERT_EQ(0, ret);
  // compare [2fp/3,fp]
  size_t q_byte_st = byte_len;
  byte_st = ObCharset::charpos(param.coll_type_, tmp_buf, real_len, fp_len/2 + fp_len/6);
  byte_len = ObCharset::charpos(param.coll_type_, tmp_buf + byte_st, real_len - byte_st, fp_len - fp_len/2 - fp_len/6);
  ASSERT_EQ(MEMCMP(query_str.ptr() + q_byte_st, tmp_buf + byte_st, byte_len), 0);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, query_str.ptr() + q_byte_st, query_str.length() - q_byte_st, tmp_buf + byte_st, byte_len);
  ASSERT_EQ(0, ret);

  allocator.free(query_buf);
  allocator.free(lob_data);
}

TEST_F(TestLobManager, inrow_tmp_full_locator)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ObLobManager *mgr = MTL(ObLobManager*);
  // char *lob_data = reinterpret_cast<char*>(allocator.alloc(4096));
  // ObLobCommon *loc = new(lob_data)ObLobCommon();

  char *tmp_buf;
  uint32_t data_len = 900;
  prepare_random_data(allocator, data_len, &tmp_buf);

  ObString data;
  ObLobLocatorV2 lob_locator;
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_full_lob_locator(allocator, data, CS_TYPE_BINARY, lob_locator));

  ObLobAccessParam param;
  param.tx_desc_ = nullptr;
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.scan_backward_ = false;
  param.lob_locator_ = &lob_locator;
  param.handle_size_ = param.lob_locator_->size_;
  param.asscess_ptable_ = false;
  param.coll_type_ = CS_TYPE_BINARY;
  param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  // append [0,400]
  ObString appeng_buf;
  appeng_buf.assign_ptr(tmp_buf, 400);
  ASSERT_EQ(OB_SUCCESS, mgr->append(param, appeng_buf));

  // query check [0,400]
  char *query_buf = reinterpret_cast<char*>(allocator.alloc(4096));
  ObString query_str;
  query_str.assign_buffer(query_buf, 1024);
  param.handle_size_ = param.lob_locator_->size_;
  param.offset_ = 0;
  param.len_ = 400;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf + param.offset_, param.len_), 0);

  // erase [100, 250]
  // param.handle_size_ = param.lob_locator_->size_;
  param.offset_ = 100;
  param.len_ = 150;
  ASSERT_EQ(OB_SUCCESS, mgr->erase(param));
  // query check [0,100],[250,400]
  query_str.assign_buffer(query_buf, 1024);
  // param.handle_size_ = param.lob_locator_->size_;
  param.offset_ = 0;
  param.len_ = 400;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(250, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 150), 0);

  // replaced-write [50,100]
  appeng_buf.assign_ptr(tmp_buf + 400, 50);
  param.offset_ = 50;
  param.len_ = 50;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));
  // query check [0,100],[250,900],
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,250]
  // tmpbuf  [0,50]  [400,450]  [250,400]
  ASSERT_EQ(250, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 150), 0);

  // overwrite-no padding [200,300] = tmpbuf[800,900]
  appeng_buf.assign_ptr(tmp_buf + 800, 100);
  param.offset_ = 200;
  param.len_ = 100;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,200]  [200,300]
  // tmpbuf  [0,50]  [400,450]  [250,350]  [800,900]
  ASSERT_EQ(300, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 200, tmp_buf + 800, 100), 0);

  // overwrite with padding [1000, 1100] = tmpbuf[600,700]
  appeng_buf.assign_ptr(tmp_buf + 600, 200); // set data 200, but only write 100
  param.offset_ = 1000;
  param.len_ = 100;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));
  query_str.assign_buffer(query_buf, 4096);
  param.offset_ = 0;
  param.len_ = 2000;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,200]  [200,300] [300,1000] [1000,1100]
  // tmpbuf  [0,50]  [400,450]  [250,350]  [800,900] [0x00]     [600,700]
  ASSERT_EQ(1100, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 200, tmp_buf + 800, 100), 0);
  char empty_buf[1024];
  MEMSET(empty_buf, 0x00, 1024);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 300, empty_buf, 700), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 1000, tmp_buf + 600, 100), 0);
  // allocator.free(query_buf);
  // allocator.free(lob_locator.ptr_);
  allocator.clear();
}

TEST_F(TestLobManager, inrow_tmp_delta_locator)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ObLobManager *mgr = MTL(ObLobManager*);
  char *persist_data = reinterpret_cast<char*>(allocator.alloc(4096));
  // mock lob persis
  {
    ObMemLobCommon *lob_common = new(persist_data)ObMemLobCommon(ObMemLobType::PERSISTENT_LOB, false);
    lob_common->set_read_only(false);
    ObLobCommon *loc = new(lob_common->data_)ObLobCommon();
  }
  uint32_t handle_size = sizeof(ObMemLobCommon) + sizeof(ObLobCommon);
  ObLobLocatorV2 persis_locator(persist_data, handle_size);

  char *tmp_buf;
  uint32_t data_len = 900;
  prepare_random_data(allocator, data_len, &tmp_buf);

  ObString data;
  ObLobLocatorV2 lob_locator;
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_full_lob_locator(allocator, data, CS_TYPE_BINARY, lob_locator));

  ObLobAccessParam param;
  // param.tx_desc_ = nullptr;
  // param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  // param.ls_id_ = ls_id_;
  // param.tablet_id_ = tablet_id_;
  // param.scan_backward_ = false;
  // param.lob_locator_ = &lob_locator;
  // param.handle_size_ = param.lob_locator_->size_;
  // param.asscess_ptable_ = false;
  param.coll_type_ = CS_TYPE_BINARY;
  // param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  // append [0,400]
  ObString appeng_buf;
  appeng_buf.assign_ptr(tmp_buf, 400);
  ObLobLocatorV2 delta_locator;
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_delta_lob_locator(allocator, &persis_locator, appeng_buf, false, ObLobDiffFlags(),
    ObLobDiff::DiffType::APPEND, 0, 0, 0, delta_locator));
  ASSERT_EQ(OB_SUCCESS, mgr->process_delta(param, delta_locator));

  // query check [0,400]
  char *query_buf = reinterpret_cast<char*>(allocator.alloc(4096));
  ObString query_str;
  query_str.assign_buffer(query_buf, 1024);
  // param.handle_size_ = param.lob_locator_->size_;
  param.offset_ = 0;
  param.len_ = 400;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf + param.offset_, param.len_), 0);

  // erase [100, 250]
  // mock lob persis
  {
    ObMemLobCommon *lob_common = new(persist_data)ObMemLobCommon(ObMemLobType::PERSISTENT_LOB, false);
    lob_common->set_read_only(false);
    MEMCPY(lob_common->data_, reinterpret_cast<char*>(param.lob_common_), param.handle_size_);
  }
  persis_locator.ptr_ = persist_data;
  persis_locator.size_ = sizeof(ObMemLobCommon) + param.handle_size_;
  // build delta
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_delta_lob_locator(allocator, &persis_locator, ObString(), false, ObLobDiffFlags(),
    ObLobDiff::DiffType::ERASE, 100, 150, 0, delta_locator));
  ASSERT_EQ(OB_SUCCESS, mgr->process_delta(param, delta_locator));
  // query check [0,100],[250,400]
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 400;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(250, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 150), 0);

  // replaced-write [50,100]
  // mock lob persis
  {
    ObMemLobCommon *lob_common = new(persist_data)ObMemLobCommon(ObMemLobType::PERSISTENT_LOB, false);
    lob_common->set_read_only(false);
    MEMCPY(lob_common->data_, reinterpret_cast<char*>(param.lob_common_), param.handle_size_);
  }
  persis_locator.ptr_ = persist_data;
  persis_locator.size_ = sizeof(ObMemLobCommon) + sizeof(ObLobCommon) + param.byte_size_;
  // build delta
  appeng_buf.assign_ptr(tmp_buf + 400, 50);
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_delta_lob_locator(allocator, &persis_locator, appeng_buf, false, ObLobDiffFlags(),
    ObLobDiff::DiffType::WRITE, 50, 50, 0, delta_locator));
  ASSERT_EQ(OB_SUCCESS, mgr->process_delta(param, delta_locator));
  // query check [0,100],[250,900],
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,250]
  // tmpbuf  [0,50]  [400,450]  [250,400]
  ASSERT_EQ(250, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 150), 0);

  // overwrite-no padding [200,300] = tmpbuf[800,900]
  // mock lob persis
  {
    ObMemLobCommon *lob_common = new(persist_data)ObMemLobCommon(ObMemLobType::PERSISTENT_LOB, false);
    lob_common->set_read_only(false);
    MEMCPY(lob_common->data_, reinterpret_cast<char*>(param.lob_common_), param.handle_size_);
  }
  persis_locator.ptr_ = persist_data;
  persis_locator.size_ = sizeof(ObMemLobCommon) + param.handle_size_;
  // biuld delta
  appeng_buf.assign_ptr(tmp_buf + 800, 100);
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_delta_lob_locator(allocator, &persis_locator, appeng_buf, false, ObLobDiffFlags(),
    ObLobDiff::DiffType::WRITE, 200, 100, 0, delta_locator));
  ASSERT_EQ(OB_SUCCESS, mgr->process_delta(param, delta_locator));
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,200]  [200,300]
  // tmpbuf  [0,50]  [400,450]  [250,350]  [800,900]
  ASSERT_EQ(300, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 200, tmp_buf + 800, 100), 0);

  // overwrite with padding [1000, 1100] = tmpbuf[600,700]
  // mock lob persis
  {
    ObMemLobCommon *lob_common = new(persist_data)ObMemLobCommon(ObMemLobType::PERSISTENT_LOB, false);
    lob_common->set_read_only(false);
    MEMCPY(lob_common->data_, reinterpret_cast<char*>(param.lob_common_), param.handle_size_);
  }
  persis_locator.ptr_ = persist_data;
  persis_locator.size_ = sizeof(ObMemLobCommon) + param.handle_size_;
  // build delta
  appeng_buf.assign_ptr(tmp_buf + 600, 200); // set data 200, but only write 100
  // param.offset_ = 1000;
  // param.len_ = 100;
  ASSERT_EQ(OB_SUCCESS, mgr->build_tmp_delta_lob_locator(allocator, &persis_locator, appeng_buf, false, ObLobDiffFlags(),
    ObLobDiff::DiffType::WRITE, 1000, 100, 0, delta_locator));
  ASSERT_EQ(OB_SUCCESS, mgr->process_delta(param, delta_locator));
  query_str.assign_buffer(query_buf, 4096);
  param.offset_ = 0;
  param.len_ = 2000;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  // 对应关系
  // query   [0,50]  [50,100]   [100,200]  [200,300] [300,1000] [1000,1100]
  // tmpbuf  [0,50]  [400,450]  [250,350]  [800,900] [0x00]     [600,700]
  ASSERT_EQ(1100, query_str.length());
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 50, tmp_buf + 400, 50), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 100, tmp_buf + 250, 100), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 200, tmp_buf + 800, 100), 0);
  char empty_buf[1024];
  MEMSET(empty_buf, 0x00, 1024);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 300, empty_buf, 700), 0);
  ASSERT_EQ(MEMCMP(query_str.ptr() + 1000, tmp_buf + 600, 100), 0);
  // allocator.free(query_buf);
  // allocator.free(lob_locator.ptr_);
  allocator.clear();
}

TEST_F(TestLobManager, inrow_bin_reverse_query)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ObLobManager *mgr = MTL(ObLobManager*);
  char *lob_data = reinterpret_cast<char*>(allocator.alloc(4096));
  ObLobCommon *loc = new(lob_data)ObLobCommon();

  char *tmp_buf;
  uint32_t data_len = 900;
  prepare_random_data(allocator, data_len, &tmp_buf);

  ObLobAccessParam param;
  param.tx_desc_ = nullptr;
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.scan_backward_ = false;
  param.lob_common_ = loc;
  param.handle_size_ = 4096;
  param.asscess_ptable_ = false;
  param.coll_type_ = CS_TYPE_BINARY;
  param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  // append [0,400]
  ObString appeng_buf;
  appeng_buf.assign_ptr(tmp_buf, 900);
  ASSERT_EQ(OB_SUCCESS, mgr->append(param, appeng_buf));

  // query check [0,900]
  char *query_buf = reinterpret_cast<char*>(allocator.alloc(4096));
  ObString query_str;
  query_str.assign_buffer(query_buf, 1024);
  param.offset_ = 0;
  param.len_ = 900;
  ASSERT_EQ(OB_SUCCESS, mgr->query(param, query_str));
  ASSERT_EQ(MEMCMP(query_str.ptr(), tmp_buf + param.offset_, param.len_), 0);

  // query by iter in reverse
  // [0, 900]
  {
    param.scan_backward_ = true;
    ObLobQueryIter *iter = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr->query(param, iter));
    // perpare read buffer
    int32_t read_buff_len = 200;
    char *read_buf = reinterpret_cast<char*>(allocator.alloc(read_buff_len));
    ObString read_str;
    int32_t offset = 0;
    while (OB_SUCC(ret)) {
      read_str.assign_buffer(read_buf, read_buff_len);
      ret = iter->get_next_row(read_str);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
        } else {
          ASSERT_EQ(ret, OB_SUCCESS);
        }
      } else {
        int32_t read_len = read_str.length();
        MEMCMP(read_str.ptr(), tmp_buf + 900 - offset - read_len, read_len);
        offset += read_len;
      }
    }
    iter->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter);
    allocator.free(read_buf);
  }

  // query [450, 700]
  {
    param.scan_backward_ = true;
    param.offset_ = 200; // offset from end
    param.len_ = 250;
    ObLobQueryIter *iter = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr->query(param, iter));
    // perpare read buffer
    int32_t read_buff_len = 200;
    char *read_buf = reinterpret_cast<char*>(allocator.alloc(read_buff_len));
    ObString read_str;
    int32_t offset = 0;
    while (OB_SUCC(ret)) {
      read_str.assign_buffer(read_buf, read_buff_len);
      ret = iter->get_next_row(read_str);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
        } else {
          ASSERT_EQ(ret, OB_SUCCESS);
        }
      } else {
        int32_t read_len = read_str.length();
        MEMCMP(read_str.ptr(), tmp_buf + 700 - offset - read_len, read_len);
        offset += read_len;
      }
    }
    iter->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter);
    allocator.free(read_buf);
  }

  allocator.free(query_buf);
  allocator.free(lob_data);
}

TEST_F(TestLobManager, inrow_utf8_reverse_query)
{
  ObArenaAllocator allocator;
  int ret = OB_SUCCESS;

  ObLobManager *mgr = MTL(ObLobManager*);
  char *lob_data = reinterpret_cast<char*>(allocator.alloc(4096));
  ObLobCommon *loc = new(lob_data)ObLobCommon();

  char tmp_buf[1024];
  uint32_t data_len = 1024;
  int real_len = 0;
  gen_random_unicode_string(1000, tmp_buf, real_len);
  size_t char_len = ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, tmp_buf, real_len);

  ObLobAccessParam param;
  param.tx_desc_ = nullptr;
  param.sql_mode_ = SMO_DEFAULT;
  param.allocator_ = &allocator;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.scan_backward_ = false;
  param.lob_common_ = loc;
  param.handle_size_ = 4096;
  param.asscess_ptable_ = false;
  param.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  param.timeout_ = ObTimeUtility::current_time() + 12 * 1000 * 1000;
  // append char len [0, char_len]
  ObString appeng_buf;
  appeng_buf.assign_ptr(tmp_buf, real_len);
  param.offset_ = 0;
  param.len_ = char_len;
  ASSERT_EQ(OB_SUCCESS, mgr->write(param, appeng_buf));

  // query by iter in reverse
  // [0, 900]
  {
    param.scan_backward_ = true;
    ObLobQueryIter *iter = NULL;
    ASSERT_EQ(OB_SUCCESS, mgr->query(param, iter));
    // perpare read buffer
    int32_t read_buff_len = 200;
    char *read_buf = reinterpret_cast<char*>(allocator.alloc(read_buff_len));
    ObString read_str;
    int32_t offset = 0;
    while (OB_SUCC(ret)) {
      read_str.assign_buffer(read_buf, read_buff_len);
      ret = iter->get_next_row(read_str);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
        } else {
          ASSERT_EQ(ret, OB_SUCCESS);
        }
      } else {
        int32_t read_len = read_str.length();
        MEMCMP(read_str.ptr(), tmp_buf + real_len - offset - read_len, read_len);
        offset += read_len;
      }
    }
    iter->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter);
    allocator.free(read_buf);
  }

}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_lob_manager.log");
  OB_LOGGER.set_file_name("test_lob_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  GCONF._enable_defensive_check = false;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
