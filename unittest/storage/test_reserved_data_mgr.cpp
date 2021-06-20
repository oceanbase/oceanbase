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

#include "storage/ob_reserved_data_mgr.h"
#include "blocksstable/ob_data_file_prepare.h"
#include "lib/container/ob_iarray.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase {
namespace unittest {
class TestRecoveryDataMgr : public TestDataFilePrepare {
public:
  TestRecoveryDataMgr();
  virtual ~TestRecoveryDataMgr() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  int prepare_pg_meta(const int64_t replay_log_ts, const int64_t publish_version, ObPartitionGroupMeta& meta);
  int prepare_pgpartition_store_meta(ObPGPartitionStoreMeta& partition_store_meta);
  int generate_table_key(const ObITable::TableType& type, common::ObVersionRange& trans_version_range,
      const int64_t major_version, const uint64 table_id, ObITable::TableKey& table_key);
  int create_sstable(
      const ObITable::TableKey& table_key, const blocksstable::ObSSTableBaseMeta& meta, ObTableHandle& handle);
  int fake_sstable(const bool is_major_sstable, common::ObVersionRange& trans_version_range,
      const int64_t major_version, const uint64_t table_id, const int64_t schema_version, ObTableHandle& handle,
      const ObLogTsRange& log_ts_range);

private:
  ObPartitionKey pg_key_;
  common::ObArenaAllocator allocator_;
};

TestRecoveryDataMgr::TestRecoveryDataMgr()
    : TestDataFilePrepare("TestRecoveryDataMgr"), pg_key_(combine_id(1, 3000), 0, 0)
{}

void TestRecoveryDataMgr::SetUp()
{
  TestDataFilePrepare::SetUp();
}

void TestRecoveryDataMgr::TearDown()
{
  TestDataFilePrepare::TearDown();
}

int TestRecoveryDataMgr::prepare_pg_meta(
    const int64_t replay_log_ts, const int64_t publish_version, ObPartitionGroupMeta& meta)
{
  int ret = OB_SUCCESS;
  meta.pg_key_ = pg_key_;
  meta.replica_type_ = ObReplicaType::REPLICA_TYPE_FULL;
  meta.storage_info_.get_data_info().set_last_replay_log_ts(replay_log_ts);
  meta.storage_info_.get_data_info().set_publish_version(publish_version);
  ret = meta.storage_info_.get_clog_info().init(1, ObMemberList(), 0, 0, true);
  return ret;
}

int TestRecoveryDataMgr::prepare_pgpartition_store_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  partition_store_meta.pkey_ = pg_key_;
  partition_store_meta.create_schema_version_ = 1;
  partition_store_meta.create_timestamp_ = 1;
  partition_store_meta.data_table_id_ = pg_key_.get_table_id();
  partition_store_meta.multi_version_start_ = 1;
  partition_store_meta.replica_type_ = REPLICA_TYPE_FULL;
  return ret;
}

int TestRecoveryDataMgr::generate_table_key(const ObITable::TableType& type,
    common::ObVersionRange& trans_version_range, const int64_t major_version, const uint64 table_id,
    ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  table_key.table_type_ = type;
  table_key.table_id_ = table_id;
  table_key.pkey_ = pg_key_;
  table_key.trans_version_range_ = trans_version_range;
  table_key.version_ = major_version;
  table_key.log_ts_range_.max_log_ts_ = table_key.log_ts_range_.end_log_ts_;
  return ret;
}

int TestRecoveryDataMgr::create_sstable(
    const ObITable::TableKey& table_key, const blocksstable::ObSSTableBaseMeta& meta, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObSSTable* table = NULL;
  void* buf = nullptr;

  if (!meta.is_valid() || (!ObITable::is_sstable(table_key.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_key), K(meta));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for sstable", K(ret));
  } else if (FALSE_IT(table = new (buf) ObSSTable())) {
    STORAGE_LOG(WARN, "failed to new table", K(ret));
  } else if (OB_FAIL(table->init(table_key))) {
    STORAGE_LOG(WARN, "failed to init sstable", K(ret), K(table_key));
  } else if (OB_FAIL(table->open(meta))) {
    STORAGE_LOG(WARN, "failed to open table", K(ret));
  } else if (OB_FAIL(handle.set_table(table))) {
    STORAGE_LOG(WARN, "failed to set table to handle", K(ret));
  }

  return ret;
}

int TestRecoveryDataMgr::fake_sstable(const bool is_major_sstable, common::ObVersionRange& trans_version_range,
    const int64_t major_version, const uint64_t table_id, const int64_t schema_version, ObTableHandle& handle,
    const ObLogTsRange& log_ts_range)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableBaseMeta meta;
  blocksstable::ObSSTableColumnMeta fake_column_meta;
  ObITable::TableKey table_key;
  ObITable::TableType type;
  ObSSTable* sstable = NULL;
  if (is_major_sstable) {
    type = ObITable::MAJOR_SSTABLE;
    meta.logical_data_version_ = major_version;
  } else {
    type = ObITable::MULTI_VERSION_MINOR_SSTABLE;
    meta.logical_data_version_ = trans_version_range.snapshot_version_;
  }
  meta.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
  meta.schema_version_ = schema_version;
  meta.rowkey_column_count_ = 1;
  meta.column_cnt_ = 1;
  meta.column_metas_.set_capacity(static_cast<int32_t>(meta.column_cnt_));
  meta.set_allocator(allocator_);
  meta.column_metas_.push_back(fake_column_meta);
  meta.multi_version_rowkey_type_ = storage::ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION;
  handle.reset();
  table_key.log_ts_range_ = log_ts_range;
  if (OB_FAIL(generate_table_key(type, trans_version_range, major_version, table_id, table_key))) {
  } else if (OB_FAIL(create_sstable(table_key, meta, handle))) {
    STORAGE_LOG(WARN, "Failed to create sstable", K(ret));
  } else if (OB_ISNULL(sstable = static_cast<ObSSTable*>(handle.get_table()))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Failed to get sstable", K(ret));
  } else if (OB_FAIL(sstable->close())) {
    STORAGE_LOG(WARN, "Failed to close sstable", K(ret));
  }

  return ret;
}

TEST_F(TestRecoveryDataMgr, enable_write_slog)
{
  ObRecoveryDataMgr mgr_;
  ASSERT_EQ(OB_SUCCESS, mgr_.init(pg_key_));
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.1", 80);
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, mgr_.set_storage_file_handle(file_handle));
  ObPGSSTableMgr sstable_mgr;
  ASSERT_EQ(OB_SUCCESS, mgr_.enable_write_slog(sstable_mgr));
  STORAGE_LOG(INFO, "enable_write_slog finished");
}

TEST_F(TestRecoveryDataMgr, serialize)
{
  ObRecoveryDataMgr mgr_;
  // int ObPGSSTableMgr::add_sstables(const bool in_slog_trans, ObTablesHandle &tables_handle)
  ObPGSSTableMgr sstable_mgr;
  ObPartitionGroupMeta pg_meta;
  ObTablesHandle handle;
  ObTableHandle handle_s;
  ObVersion version;
  ObLogTsRange log_ts_range;
  ObVersionRange version_range;
  // int ObPartitionKey::init(const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt)
  ObPartitionKey pg_key;
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.01", 80);
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  common::ObMalloc allocator;
  ObPGPartitionStoreMeta fake_meta;
  ObFixedArray<ObPGPartitionStoreMeta> partition_store_metas(allocator);
  common::ObVersionRange trans_version_range;
  ObITable *table1, *table2, *table3;
  int64_t table_id;
  int64_t snapshot_version;
  bool in_slog_trans = false;
  bool use_inc_macro_block_slog = false;
  bool is_major_sstable = true;
  int64_t major_version = 2;
  int64_t shcema_version = 2;

  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(0, 0, pg_meta));
  ASSERT_EQ(OB_SUCCESS, prepare_pgpartition_store_meta(fake_meta));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.init(1));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.push_back(fake_meta));
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.init(pg_key_));
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.enable_write_log());
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.set_storage_file_handle(file_handle));

  ASSERT_EQ(OB_SUCCESS, mgr_.init(pg_key_));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_storage_file_handle(file_handle));
  ASSERT_EQ(OB_SUCCESS, mgr_.enable_write_slog(sstable_mgr));

  // create sstable
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 100;
  trans_version_range.snapshot_version_ = 100;
  // table 1
  table_id = combine_id(1, 3000);
  major_version = 1;
  shcema_version = 1;
  // ASSERT_EQ(OB_SUCCESS, fake_sstable(true, trans_version_range, 1, pg_key_.get_table_id(), 1, handle_s,
  // log_ts_range));
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.add_sstable(in_slog_trans, handle_s));
  table1 = handle_s.get_table();

  // table 2
  handle_s.reset();
  table_id = combine_id(2, 3000);
  major_version = 2;
  shcema_version = 2;
  // ASSERT_EQ(OB_SUCCESS, fake_sstable(true, trans_version_range, 1, pg_key_.get_table_id(), 1, handle_s,
  // log_ts_range));
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.add_sstable(in_slog_trans, handle_s));
  table2 = handle_s.get_table();

  // table 3
  handle_s.reset();
  table_id = combine_id(3, 3000);
  major_version = 3;
  shcema_version = 3;
  // ASSERT_EQ(OB_SUCCESS, fake_sstable(true, trans_version_range, 1, pg_key_.get_table_id(), 1, handle_s,
  // log_ts_range));
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.add_sstable(in_slog_trans, handle_s));
  table3 = handle_s.get_table();

  // restore point 1 : <1, table1>, <2, table2>
  handle.reset();
  snapshot_version = 1;
  ASSERT_EQ(OB_SUCCESS, handle.add_table(table1));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(table2));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(snapshot_version, pg_meta, partition_store_metas, handle));
  // STORAGE_LOG(INFO, "restore point 1 add success");
  // restore point 2 : <3, table3>
  handle.reset();
  snapshot_version = 2;
  ASSERT_EQ(OB_SUCCESS, handle.add_table(table3));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(snapshot_version, pg_meta, partition_store_metas, handle));
  // STORAGE_LOG(INFO, "restore point 2 add success");

  // serialize
  char* buf = NULL;
  int64_t serialize_size = 0;
  int64_t pos = 0;
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, mgr_.serialize(allocator, buf, serialize_size));
  // STORAGE_LOG(INFO, "serialize success", K(serialize_size), K(buf), K(strlen(buf)));

  // deserialize
  ObRecoveryDataMgr mgr2_;
  ASSERT_EQ(OB_SUCCESS, mgr2_.init(pg_key_));
  ASSERT_EQ(OB_SUCCESS, mgr2_.set_storage_file_handle(file_handle));
  ASSERT_EQ(OB_SUCCESS, mgr2_.deserialize(buf, serialize_size, pos));
  ObTablesHandle tmp_handle;
  ASSERT_EQ(OB_SUCCESS, sstable_mgr.get_all_sstables(tmp_handle));
  // STORAGE_LOG(INFO, "deserialize success", K(tmp_handle));
  ASSERT_EQ(OB_SUCCESS, mgr2_.enable_write_slog(sstable_mgr));
  // STORAGE_LOG(INFO, "finish replay success");
  ASSERT_EQ(mgr_.get_recovery_point_cnt(), mgr2_.get_recovery_point_cnt());
  snapshot_version = 1;
  table_id = combine_id(1, 3000);
  ASSERT_EQ(OB_SUCCESS, mgr2_.check_restore_point_exist(1, is_exist));
  ASSERT_EQ(true, is_exist);
  handle.reset();
  ASSERT_EQ(OB_SUCCESS, mgr2_.get_restore_point_read_tables(table_id, snapshot_version, handle));
  ASSERT_EQ(1, handle.get_count());
  ASSERT_EQ(1, (static_cast<ObSSTable*>(handle.get_table(0)))->get_logical_data_version());
  STORAGE_LOG(INFO, "serialize finished");
}

TEST_F(TestRecoveryDataMgr, test_add_restore_point_data)
{
  ObRecoveryDataMgr mgr_, mgr2_;
  ASSERT_EQ(OB_SUCCESS, mgr_.init(pg_key_));
  ASSERT_EQ(OB_SUCCESS, mgr2_.init(pg_key_));
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.1", 80);
  common::ObMalloc allocator;
  ObPGPartitionStoreMeta fake_meta;
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, mgr_.set_storage_file_handle(file_handle));
  ASSERT_EQ(OB_SUCCESS, mgr2_.set_storage_file_handle(file_handle));
  ObPGSSTableMgr sstable_mgr;
  ASSERT_EQ(OB_SUCCESS, mgr_.enable_write_slog(sstable_mgr));
  ASSERT_EQ(OB_SUCCESS, mgr2_.enable_write_slog(sstable_mgr));
  ObPartitionGroupMeta pg_meta;
  ObTablesHandle handle;
  ObTableHandle handle_s;
  ObFixedArray<ObPGPartitionStoreMeta> partition_store_metas(allocator);
  ObVersionRange trans_version_range;
  ObVersion version;
  ObLogTsRange log_ts_range;
  int64_t table_id;
  bool is_major_sstable = true;
  int64_t major_version = 2;
  int64_t shcema_version = 2;

  ASSERT_EQ(OB_SUCCESS, prepare_pgpartition_store_meta(fake_meta));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.init(1));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.push_back(fake_meta));

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 100;
  trans_version_range.snapshot_version_ = 100;

  // case 1: <tableid, [sstable1, sstabe2]>
  ObTablesHandle res_handle;
  bool is_exist = false;
  // table 1
  table_id = combine_id(1, 3000);
  major_version = 1;
  shcema_version = 1;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  // table 2
  handle_s.reset();
  major_version = 2;
  shcema_version = 2;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  // add and check
  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(300, 250, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(1, pg_meta, partition_store_metas, handle));
  ASSERT_EQ(OB_ENTRY_EXIST, mgr_.add_restore_point(1, pg_meta, partition_store_metas, handle));
  ASSERT_EQ(OB_SUCCESS, mgr_.check_restore_point_exist(100, is_exist));
  ASSERT_EQ(false, is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.check_restore_point_exist(1, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_restore_point_read_tables(table_id, 1, res_handle));
  ASSERT_EQ(res_handle.get_count(), handle.get_count());

  // case 2: replay add
  ObRecoveryPointData point_data;
  ASSERT_EQ(OB_SUCCESS, point_data.init(1, pg_meta, partition_store_metas));
  ASSERT_EQ(OB_SUCCESS, point_data.add_sstables(handle));
  // the data exist, will not be inserted.
  ASSERT_EQ(1, mgr_.get_recovery_point_cnt());
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_add_recovery_point(ObRecoveryPointType::RESTORE_POINT, point_data));
  ASSERT_EQ(1, mgr_.get_recovery_point_cnt());

  ObRecoveryPointData point_data2;
  ASSERT_EQ(OB_SUCCESS, point_data2.init(2, pg_meta, partition_store_metas));
  ASSERT_EQ(OB_SUCCESS, point_data2.add_sstables(handle));
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_add_recovery_point(ObRecoveryPointType::RESTORE_POINT, point_data2));

  // case 3: replay remove
  ObRecoveryPointData point_data3;
  ASSERT_EQ(OB_SUCCESS, point_data3.init(4, pg_meta, partition_store_metas));
  ASSERT_EQ(OB_SUCCESS, point_data3.add_sstables(handle));
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_add_recovery_point(ObRecoveryPointType::RESTORE_POINT, point_data3));
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, mgr_.check_restore_point_exist(4, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_remove_recovery_point(ObRecoveryPointType::RESTORE_POINT, 4));
  ASSERT_EQ(OB_SUCCESS, mgr_.check_restore_point_exist(4, is_exist));
  ASSERT_EQ(false, is_exist);

  // case 4: add <tableid, [sstable]>
  handle.reset();
  handle_s.reset();
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(3, pg_meta, partition_store_metas, handle));

  STORAGE_LOG(INFO, "add restore point data finished");
}

TEST_F(TestRecoveryDataMgr, test_remove_restore_point)
{
  ObRecoveryDataMgr mgr_;
  ASSERT_EQ(OB_SUCCESS, mgr_.init(pg_key_));
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.1", 80);
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, mgr_.set_storage_file_handle(file_handle));
  ObPGSSTableMgr sstable_mgr;
  ASSERT_EQ(OB_SUCCESS, mgr_.enable_write_slog(sstable_mgr));
  ObPartitionGroupMeta pg_meta;
  ObTablesHandle handle;
  ObTableHandle handle_s;
  ObITable::TableKey key;
  ObSSTable table1;
  common::ObMalloc allocator;
  ObPGPartitionStoreMeta fake_meta;
  ObFixedArray<ObPGPartitionStoreMeta> partition_store_metas(allocator);
  ObVersionRange trans_version_range;
  ObVersion version;
  ObLogTsRange log_ts_range;
  int64_t table_id;
  bool is_major_sstable = true;
  int64_t major_version = 2;
  int64_t shcema_version = 2;

  ASSERT_EQ(OB_SUCCESS, prepare_pgpartition_store_meta(fake_meta));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.init(1));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.push_back(fake_meta));

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 100;
  trans_version_range.snapshot_version_ = 100;

  // table 1
  table_id = combine_id(1, 3000);
  major_version = 1;
  shcema_version = 1;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  // table 2
  handle_s.reset();
  major_version = 2;
  shcema_version = 2;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(300, 250, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(1, pg_meta, partition_store_metas, handle));

  // table 3
  handle_s.reset();
  major_version = 3;
  shcema_version = 3;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(400, 350, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(2, pg_meta, partition_store_metas, handle));

  ObArray<int64_t> array;
  array.reset();
  array.push_back(1);
  array.push_back(2);
  ASSERT_EQ(2, mgr_.get_recovery_point_cnt());
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_unneed_restore_point(array, INT64_MAX));
  ASSERT_EQ(2, mgr_.get_recovery_point_cnt());

  array.reset();
  array.push_back(2);
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_unneed_restore_point(array, INT64_MAX));
  ASSERT_EQ(1, mgr_.get_recovery_point_cnt());

  // table 4
  handle_s.reset();
  major_version = 4;
  shcema_version = 4;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(500, 450, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(3, pg_meta, partition_store_metas, handle));

  // table 5
  handle_s.reset();
  major_version = 5;
  shcema_version = 5;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(600, 550, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(4, pg_meta, partition_store_metas, handle));

  // table 6
  handle_s.reset();
  major_version = 6;
  shcema_version = 6;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(700, 650, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_restore_point(1, pg_meta, partition_store_metas, handle));
  ASSERT_EQ(4, mgr_.get_recovery_point_cnt());

  array.reset();
  array.push_back(1);
  array.push_back(3);
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_unneed_restore_point(array, INT64_MAX));
  ASSERT_EQ(2, mgr_.get_recovery_point_cnt());

  ObTablesHandle res_handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.get_restore_point_read_tables(pg_key_.get_table_id(), 2, res_handle));
  res_handle.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.get_restore_point_read_tables(pg_key_.get_table_id(), 1, res_handle));

  ASSERT_EQ(OB_SUCCESS, mgr_.replay_remove_recovery_point(ObRecoveryPointType::RESTORE_POINT, 1));
  ASSERT_EQ(1, mgr_.get_recovery_point_cnt());
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_remove_recovery_point(ObRecoveryPointType::RESTORE_POINT, 3));
  ASSERT_EQ(0, mgr_.get_recovery_point_cnt());
  STORAGE_LOG(INFO, "test_remove_restore_point finished");
}

TEST_F(TestRecoveryDataMgr, test_add_backup_point_data)
{
  ObRecoveryDataMgr mgr_, mgr2_;
  ASSERT_EQ(OB_SUCCESS, mgr_.init(pg_key_));
  ASSERT_EQ(OB_SUCCESS, mgr2_.init(pg_key_));
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.1", 80);
  common::ObMalloc allocator;
  ObPGPartitionStoreMeta fake_meta;
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, mgr_.set_storage_file_handle(file_handle));
  ASSERT_EQ(OB_SUCCESS, mgr2_.set_storage_file_handle(file_handle));
  ObPGSSTableMgr sstable_mgr;
  ASSERT_EQ(OB_SUCCESS, mgr_.enable_write_slog(sstable_mgr));
  ASSERT_EQ(OB_SUCCESS, mgr2_.enable_write_slog(sstable_mgr));
  ObPartitionGroupMeta pg_meta, res_pg_meta;
  ObTablesHandle handle, res_handle;
  ObTableHandle handle_s;
  ObFixedArray<ObPGPartitionStoreMeta> partition_store_metas(allocator);
  ObVersionRange trans_version_range;
  ObVersion version;
  ObLogTsRange log_ts_range;
  int64_t table_id;
  bool is_major_sstable = true;
  int64_t major_version = 2;
  int64_t shcema_version = 2;
  int64_t table_snapshot = 0;
  int64_t replay_log_ts = 0;
  int64_t backup_point_version = 1;

  ASSERT_EQ(OB_SUCCESS, prepare_pgpartition_store_meta(fake_meta));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.init(1));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.push_back(fake_meta));

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 0;

  // case 1: <tableid, [sstable1, sstabe2]>
  // table 1
  table_id = combine_id(1, 3000);
  major_version = 1;
  shcema_version = 1;
  table_snapshot = 1;
  trans_version_range.multi_version_start_ = table_snapshot;
  trans_version_range.snapshot_version_ = table_snapshot;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  // add and check
  replay_log_ts = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(replay_log_ts, 250, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_backup_point(backup_point_version, pg_meta, partition_store_metas, handle));
  int64_t query_snapshot = 1;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_backup_point_data(query_snapshot, res_pg_meta, res_handle));
  ASSERT_EQ(replay_log_ts, res_pg_meta.storage_info_.get_data_info().get_last_replay_log_ts());
  ASSERT_EQ(handle.get_count(), res_handle.get_count());

  // case 2: replay add
  ObRecoveryPointData point_data;
  pg_meta.reset();
  handle_s.reset();
  handle.reset();
  replay_log_ts = 2;
  table_snapshot = 2;
  backup_point_version = 2;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));
  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(replay_log_ts, 250, pg_meta));
  ASSERT_EQ(OB_SUCCESS, point_data.init(backup_point_version, pg_meta, partition_store_metas));
  ASSERT_EQ(OB_SUCCESS, point_data.add_sstables(handle));
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_add_recovery_point(ObRecoveryPointType::BACKUP, point_data));

  ObRecoveryPointData point_data2;
  ASSERT_EQ(OB_SUCCESS, point_data2.init(backup_point_version, pg_meta, partition_store_metas));
  ASSERT_EQ(OB_SUCCESS, point_data2.add_sstables(handle));
  // will not be inserted, because of the same table_snapshot
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_add_recovery_point(ObRecoveryPointType::BACKUP, point_data2));

  // case 3: replay remove
  // the backup point with the snapshot version of 2 will be deleted.
  ASSERT_EQ(2, mgr_.get_recovery_point_cnt());
  ASSERT_EQ(OB_SUCCESS, mgr_.replay_remove_recovery_point(ObRecoveryPointType::BACKUP, table_snapshot));
  ASSERT_EQ(1, mgr_.get_recovery_point_cnt());

  STORAGE_LOG(INFO, "add backup point data finished");
}

TEST_F(TestRecoveryDataMgr, test_remove_backup_point)
{
  ObRecoveryDataMgr mgr_;
  ASSERT_EQ(OB_SUCCESS, mgr_.init(pg_key_));
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.1", 80);
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, mgr_.set_storage_file_handle(file_handle));
  ObPGSSTableMgr sstable_mgr;
  ASSERT_EQ(OB_SUCCESS, mgr_.enable_write_slog(sstable_mgr));
  ObPartitionGroupMeta pg_meta;
  ObTablesHandle handle;
  ObTableHandle handle_s;
  ObITable::TableKey key;
  ObSSTable table1;
  common::ObMalloc allocator;
  ObPGPartitionStoreMeta fake_meta;
  ObFixedArray<ObPGPartitionStoreMeta> partition_store_metas(allocator);
  ObVersionRange trans_version_range;
  ObVersion version;
  ObLogTsRange log_ts_range;
  int64_t table_id;
  bool is_major_sstable = true;
  int64_t major_version = 2;
  int64_t shcema_version = 2;
  int64_t table_snapshot = 0;
  int64_t replay_log_ts = 0;
  int64_t backup_point_version = 1;

  ASSERT_EQ(OB_SUCCESS, prepare_pgpartition_store_meta(fake_meta));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.init(1));
  ASSERT_EQ(OB_SUCCESS, partition_store_metas.push_back(fake_meta));

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 0;

  // table 1
  table_id = combine_id(1, 3000);
  major_version = 1;
  shcema_version = 1;
  table_snapshot = 1;
  trans_version_range.multi_version_start_ = table_snapshot;
  trans_version_range.snapshot_version_ = table_snapshot;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  // table 2
  handle_s.reset();
  major_version = 2;
  shcema_version = 2;
  table_snapshot = 2;
  trans_version_range.multi_version_start_ = table_snapshot;
  trans_version_range.snapshot_version_ = table_snapshot;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  replay_log_ts = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(replay_log_ts, 250, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_backup_point(backup_point_version, pg_meta, partition_store_metas, handle));

  // table 3
  handle_s.reset();
  major_version = 3;
  shcema_version = 3;
  table_snapshot = 3;
  trans_version_range.multi_version_start_ = table_snapshot;
  trans_version_range.snapshot_version_ = table_snapshot;
  ASSERT_EQ(OB_SUCCESS,
      fake_sstable(
          is_major_sstable, trans_version_range, major_version, table_id, shcema_version, handle_s, log_ts_range));
  ASSERT_EQ(OB_SUCCESS, handle.add_table(handle_s));

  replay_log_ts = 2;
  backup_point_version = 2;
  ASSERT_EQ(OB_SUCCESS, prepare_pg_meta(replay_log_ts, 350, pg_meta));
  ASSERT_EQ(OB_SUCCESS, mgr_.add_backup_point(backup_point_version, pg_meta, partition_store_metas, handle));

  ObArray<int64_t> array;
  array.push_back(1);

  ASSERT_EQ(2, mgr_.get_recovery_point_cnt());
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_unneed_backup_point(array, 4));
  ASSERT_EQ(1, mgr_.get_recovery_point_cnt());

  STORAGE_LOG(INFO, "test_remove_backup_point finished");
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_reserved_data.log*");
  OB_LOGGER.set_file_name("test_reserved_data.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
