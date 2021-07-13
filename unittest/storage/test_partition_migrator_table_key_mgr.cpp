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
#include "storage/ob_partition_migrator_table_key_mgr.h"
#define private public
#define protected public
#include "storage/ob_partition_migrator.h"
#undef private
#include "storage/ob_partition_store.h"
#include <gtest/gtest.h>

namespace oceanbase {
using namespace common;
using namespace storage;
namespace unittest {
class TestPartitionMigrateTableKeyMgr : public ::testing::Test {
public:
  TestPartitionMigrateTableKeyMgr()
  {}
  ~TestPartitionMigrateTableKeyMgr()
  {}

  void add_table_key(const ObITable::TableType& type, const int64_t base_version, const int64_t snapshot_version,
      ObIArray<ObITable::TableKey>& table_keys);
  void add_table_key(const ObITable::TableType& type, const ObVersionRange& version_range,
      const ObLogTsRange& log_ts_range, ObIArray<ObITable::TableKey>& table_keys, ObVersion version = ObVersion(0));
  int check_same(const storage::ObMigrateTableInfo::SSTableInfo& info, const ObITable::TableKey& table_key);
  int check_same(const storage::ObMigrateTableInfo::SSTableInfo& info, const ObITable::TableKey& table_key,
      const int64_t dest_base_version);
  int check_same(const storage::ObMigrateTableInfo::SSTableInfo& info, const ObITable::TableKey& table_key,
      const int64_t dest_base_version, const ObLogTsRange& dest_log_ts_range);
};

void TestPartitionMigrateTableKeyMgr::add_table_key(const ObITable::TableType& type,
    const ObVersionRange& version_range, const ObLogTsRange& log_ts_range, ObIArray<ObITable::TableKey>& table_keys,
    ObVersion version)
{
  ObITable::TableKey key;
  key.table_type_ = type;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_ = version_range;
  key.log_ts_range_ = log_ts_range;
  key.version_ = version;
  ASSERT_EQ(OB_SUCCESS, table_keys.push_back(key));
}

void TestPartitionMigrateTableKeyMgr::add_table_key(const ObITable::TableType& type, const int64_t base_version,
    const int64_t snapshot_version, ObIArray<ObITable::TableKey>& table_keys)
{
  ObITable::TableKey key;
  key.table_type_ = type;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_.base_version_ = base_version;
  key.trans_version_range_.multi_version_start_ = snapshot_version;
  key.trans_version_range_.snapshot_version_ = snapshot_version;
  ASSERT_EQ(OB_SUCCESS, table_keys.push_back(key));
}

int TestPartitionMigrateTableKeyMgr::check_same(
    const storage::ObMigrateTableInfo::SSTableInfo& info, const ObITable::TableKey& table_key)
{
  const int64_t dest_base_version = table_key.trans_version_range_.base_version_;
  return check_same(info, table_key, dest_base_version);
}

int TestPartitionMigrateTableKeyMgr::check_same(const storage::ObMigrateTableInfo::SSTableInfo& info,
    const ObITable::TableKey& table_key, const int64_t dest_base_version)
{
  int ret = OB_SUCCESS;
  if (info.src_table_key_ != table_key) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table key not match", K(info), K(table_key));
  } else if (info.dest_base_version_ != dest_base_version) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dest base version not match", K(ret), K(info), K(table_key), K(dest_base_version));
  }
  return ret;
}

int TestPartitionMigrateTableKeyMgr::check_same(const storage::ObMigrateTableInfo::SSTableInfo& info,
    const ObITable::TableKey& table_key, const int64_t dest_base_version, const ObLogTsRange& dest_log_ts_range)
{
  int ret = OB_SUCCESS;
  if (info.src_table_key_ != table_key) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table key not match", K(info), K(table_key));
  } else if (info.dest_base_version_ != dest_base_version) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dest base version not match", K(ret), K(info), K(table_key), K(dest_base_version));
  } else if (info.dest_log_ts_range_ != dest_log_ts_range) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dest_start_log_id not match", K(ret), K(info), K(dest_log_ts_range));
  }
  return ret;
}

TEST_F(TestPartitionMigrateTableKeyMgr, single_src_table_key)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObITable::TableKey key;
  ObITable::TableKey local_table_key;

  key.table_type_ = ObITable::MEMTABLE;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_.base_version_ = 0;
  key.trans_version_range_.multi_version_start_ = 10;
  key.trans_version_range_.snapshot_version_ = 10;
  ret = remote_table_key_array.push_back(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  // local_table_key is empty
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(key == copy_sstables.at(0).src_table_key_);
  ASSERT_TRUE(copy_sstables.at(0).dest_base_version_ == key.trans_version_range_.base_version_);
  STORAGE_LOG(INFO, "copy_sstables is", K(copy_sstables));

  // local_table_key_array has one and same with remote table key
  copy_sstables.reuse();
  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 0;
  local_table_key.trans_version_range_.multi_version_start_ = 10;
  local_table_key.trans_version_range_.snapshot_version_ = 10;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(0 == copy_sstables.count());

  // local_table_key_range has one and remote contain whole range
  copy_sstables.reuse();
  local_table_key_array.reset();
  local_table_key.reset();
  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 4;
  local_table_key.trans_version_range_.multi_version_start_ = 4;
  local_table_key.trans_version_range_.snapshot_version_ = 6;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_TRUE(copy_sstables.at(0).src_table_key_ == key);
  STORAGE_LOG(INFO, "copy sstables", K(copy_sstables));
  ASSERT_TRUE(copy_sstables.at(0).dest_base_version_ == key.trans_version_range_.base_version_);

  // local_table_key_range is bigger than remote table key range
  copy_sstables.reuse();
  local_table_key_array.reset();
  local_table_key.reset();
  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 0;
  local_table_key.trans_version_range_.multi_version_start_ = 0;
  local_table_key.trans_version_range_.snapshot_version_ = 100;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, copy_sstables.count());

  // local_table_key_range base_version < remote version base_version and local_table_key_range snapshot_version <
  // remote snapshot_version
  remote_table_key_array.reset();
  key.table_type_ = ObITable::MEMTABLE;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_.base_version_ = 10;
  key.trans_version_range_.multi_version_start_ = 20;
  key.trans_version_range_.snapshot_version_ = 20;
  ret = remote_table_key_array.push_back(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  copy_sstables.reuse();
  local_table_key_array.reset();
  local_table_key.reset();
  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 0;
  local_table_key.trans_version_range_.multi_version_start_ = 0;
  local_table_key.trans_version_range_.snapshot_version_ = 15;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_TRUE(copy_sstables.at(0).src_table_key_ == key);
  ASSERT_TRUE(copy_sstables.at(0).dest_base_version_ == local_table_key.trans_version_range_.snapshot_version_);
  STORAGE_LOG(INFO, "copy sstables", K(copy_sstables));

  // local_table_key_range base_version > remote base_version
  // and local_table_key_range snapshot_version > remote snapshot_version

  copy_sstables.reuse();
  local_table_key_array.reset();
  local_table_key.reset();
  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 15;
  local_table_key.trans_version_range_.multi_version_start_ = 15;
  local_table_key.trans_version_range_.snapshot_version_ = 30;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, copy_sstables.count());
}

// major (0,50]
// minor(50,100]
// memtable (90,500)
// publish_version 200
// max_memtable_base_version 100
// expect: memtable[100, 200)

TEST_F(TestPartitionMigrateTableKeyMgr, src_table_normal)
{
  int ret = OB_SUCCESS;
  int64_t publish_version = 200;
  ObPartitionKey pkey = ObPartitionKey(combine_id(1, 3001), 0, 0);
  uint64_t table_id = pkey.get_table_id();
  const bool is_only_copy_major = false;

  ObArray<ObITable::TableKey> table_key_array;
  ObArray<ObITable::TableKey> convert_table_key_array;
  add_table_key(ObITable::MAJOR_SSTABLE, 0, 50, table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 50, 100, table_key_array);
  add_table_key(ObITable::MEMTABLE, 100, 500, table_key_array);
  table_key_array.at(table_key_array.count() - 1).trans_version_range_.multi_version_start_ = 100;
  ObSrcTableKeyManager src_table_key_manager;
  ret = src_table_key_manager.init(publish_version, table_id, pkey, is_only_copy_major);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < table_key_array.count(); ++i) {
    const ObITable::TableKey& table_key = table_key_array.at(i);
    ret = src_table_key_manager.add_table_key(table_key);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = src_table_key_manager.get_table_keys(convert_table_key_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.base_version_, 100);
  ASSERT_EQ(
      convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.snapshot_version_, 200);
}

// major (0,100]
// minor(50,80]
// memtable (80,500]
// publish_version 200
// max_memtable_base_version 80
// expect: memtable[80, 200)

TEST_F(TestPartitionMigrateTableKeyMgr, src_table_major_contain_minor)
{
  int ret = OB_SUCCESS;
  int64_t publish_version = 200;
  ObPartitionKey pkey = ObPartitionKey(combine_id(1, 3001), 0, 0);
  uint64_t table_id = pkey.get_table_id();
  const bool is_only_copy_major = false;

  ObArray<ObITable::TableKey> table_key_array;
  ObArray<ObITable::TableKey> convert_table_key_array;
  add_table_key(ObITable::MAJOR_SSTABLE, 0, 100, table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 50, 80, table_key_array);
  add_table_key(ObITable::MEMTABLE, 80, 500, table_key_array);
  table_key_array.at(table_key_array.count() - 1).trans_version_range_.multi_version_start_ = 80;
  ObSrcTableKeyManager src_table_key_manager;
  ret = src_table_key_manager.init(publish_version, table_id, pkey, is_only_copy_major);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < table_key_array.count(); ++i) {
    const ObITable::TableKey& table_key = table_key_array.at(i);
    ret = src_table_key_manager.add_table_key(table_key);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = src_table_key_manager.get_table_keys(convert_table_key_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.base_version_, 80);
  ASSERT_EQ(
      convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.snapshot_version_, 200);
}

// major (0,100]
// major (100,200]
// memtable (80,500]
// publish_version 200
// max_memtable_base_version 100
// expect: memtable[100, 200)
TEST_F(TestPartitionMigrateTableKeyMgr, src_table_no_minor)
{
  int ret = OB_SUCCESS;
  int64_t publish_version = 200;
  ObPartitionKey pkey = ObPartitionKey(combine_id(1, 3001), 0, 0);
  uint64_t table_id = pkey.get_table_id();
  const bool is_only_copy_major = false;

  ObArray<ObITable::TableKey> table_key_array;
  ObArray<ObITable::TableKey> convert_table_key_array;
  add_table_key(ObITable::MAJOR_SSTABLE, 0, 100, table_key_array);
  add_table_key(ObITable::MAJOR_SSTABLE, 0, 200, table_key_array);
  add_table_key(ObITable::MEMTABLE, 80, 500, table_key_array);
  table_key_array.at(table_key_array.count() - 1).trans_version_range_.multi_version_start_ = 80;
  ObSrcTableKeyManager src_table_key_manager;
  ret = src_table_key_manager.init(publish_version, table_id, pkey, is_only_copy_major);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < table_key_array.count(); ++i) {
    const ObITable::TableKey& table_key = table_key_array.at(i);
    ret = src_table_key_manager.add_table_key(table_key);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = src_table_key_manager.get_table_keys(convert_table_key_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.base_version_, 80);
  ASSERT_EQ(
      convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.snapshot_version_, 200);
}

// memtable (1,500]
// publish_version 200
// max_memtable_base_version 0
// expect: memtable[1, 200)
TEST_F(TestPartitionMigrateTableKeyMgr, src_table_only_memtable)
{
  int ret = OB_SUCCESS;
  int64_t publish_version = 200;
  ObPartitionKey pkey = ObPartitionKey(combine_id(1, 3001), 0, 0);
  uint64_t table_id = pkey.get_table_id();
  const bool is_only_copy_major = false;

  ObArray<ObITable::TableKey> table_key_array;
  ObArray<ObITable::TableKey> convert_table_key_array;
  add_table_key(ObITable::MEMTABLE, 1, 500, table_key_array);
  table_key_array.at(table_key_array.count() - 1).trans_version_range_.multi_version_start_ = 1;
  ObSrcTableKeyManager src_table_key_manager;
  ret = src_table_key_manager.init(publish_version, table_id, pkey, is_only_copy_major);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < table_key_array.count(); ++i) {
    const ObITable::TableKey& table_key = table_key_array.at(i);
    ret = src_table_key_manager.add_table_key(table_key);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = src_table_key_manager.get_table_keys(convert_table_key_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.base_version_, 1);
  ASSERT_EQ(
      convert_table_key_array.at(convert_table_key_array.count() - 1).trans_version_range_.snapshot_version_, 200);
}

TEST_F(TestPartitionMigrateTableKeyMgr, local_table_key_part_continue)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  ObITable::TableKey key;
  ObITable::TableKey local_table_key;
  const bool is_copy_cover_minor = true;
  key.table_type_ = ObITable::MEMTABLE;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_.base_version_ = 50;
  key.trans_version_range_.multi_version_start_ = 50;
  key.trans_version_range_.snapshot_version_ = 100;
  ret = remote_table_key_array.push_back(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  key.table_type_ = ObITable::MEMTABLE;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_.base_version_ = 100;
  key.trans_version_range_.multi_version_start_ = 100;
  key.trans_version_range_.snapshot_version_ = 150;
  ret = remote_table_key_array.push_back(key);
  ASSERT_EQ(OB_SUCCESS, ret);

  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 0;
  local_table_key.trans_version_range_.multi_version_start_ = 0;
  local_table_key.trans_version_range_.snapshot_version_ = 30;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);

  local_table_key.table_type_ = ObITable::MEMTABLE;
  local_table_key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  local_table_key.table_id_ = combine_id(1, 3001);
  local_table_key.trans_version_range_.base_version_ = 30;
  local_table_key.trans_version_range_.multi_version_start_ = 30;
  local_table_key.trans_version_range_.snapshot_version_ = 70;
  ret = local_table_key_array.push_back(local_table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDestTableKeyManager::convert_needed_inc_table_key(
      local_table_key_array, remote_table_key_array, remote_gc_table_key_array, is_copy_cover_minor, copy_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, copy_sstables.count());
  STORAGE_LOG(INFO, "copy sstables", K(copy_sstables));
  ASSERT_EQ(remote_table_key_array.at(0), copy_sstables.at(0).src_table_key_);
  ASSERT_EQ(remote_table_key_array.at(1), copy_sstables.at(1).src_table_key_);
}

// local (1,50]
// remote (1,100]
// remote gc (1,50],(50, 60], (60,100]
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_gc_sstable)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINOR_SSTABLE, 1, 100, remote_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, remote_gc_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 60, remote_gc_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 60, 100, remote_gc_table_key_array);

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_table_key_array.at(1)));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_gc_table_key_array.at(2)));
}

// local (1,50]
// remote (1,100]
// remote gc (1,50]
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_not_copy_gc_sstable)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINOR_SSTABLE, 1, 100, remote_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, remote_gc_table_key_array);

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_table_key_array.at(0), 50));
}

// local (1,50]
// remote (1,200]
// remote gc (1,100],(1,50],(50,100]
// expect: (50,100], (100,200]
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_gc_sstable2)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINOR_SSTABLE, 1, 200, remote_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 1, 100, remote_gc_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, remote_gc_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 100, remote_gc_table_key_array);

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_table_key_array.at(2)));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_table_key_array.at(0), 100));
}

// local (1,50],(50,200]
// remote (1,200], (200,300]
// expect: (200,300]
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_no_cover)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 200, local_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 1, 200, remote_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 200, 300, remote_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_table_key_array.at(1)));
}

// local (1,50],(50,250]
// remote (1,200], (200,300]
// expect: (200,300] ( 250)
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_cut_range)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 250, local_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 1, 200, remote_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 200, 300, remote_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_table_key_array.at(1), 250));
}

// local (1,50],(50,250]
// remote (1,200], (200,300]
// remote gc: (250,300]
// expect: (250,300]
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_cut_range2)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 250, local_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 1, 200, remote_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 200, 300, remote_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 250, 300, remote_gc_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_table_key_array.at(0)));
}

// local (1,50],(50,250]
// remote (1,200], (200,300]
// remote gc: (290,350]
// expect: (200,300] (250), (290,350](300)
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_cut_range3)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 250, local_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 1, 200, remote_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 200, 300, remote_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 290, 350, remote_gc_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_table_key_array.at(1), 250));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_gc_table_key_array.at(0), 300));
}

// local (1,50],(50,250]
// remote (1,200], (200,250]
// remote gc: (230,250]
// expect: none
TEST_F(TestPartitionMigrateTableKeyMgr, not_cover_minor_copy_no_copy)
{
  ObArray<ObITable::TableKey> remote_table_key_array;
  ObArray<ObITable::TableKey> remote_gc_table_key_array;
  ObArray<ObITable::TableKey> local_table_key_array;
  ObArray<storage::ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;

  add_table_key(ObITable::MINI_MINOR_SSTABLE, 1, 50, local_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 50, 250, local_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 1, 200, remote_table_key_array);
  add_table_key(ObITable::MINOR_SSTABLE, 200, 250, remote_table_key_array);
  add_table_key(ObITable::MINI_MINOR_SSTABLE, 230, 250, remote_gc_table_key_array);

  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key(local_table_key_array,
          remote_table_key_array,
          remote_gc_table_key_array,
          is_copy_cover_minor,
          copy_sstables));
  ASSERT_EQ(0, copy_sstables.count());
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_convert_sstable_info_to_sstable_key)
{
  ObMigrateTableInfo::SSTableInfo sstable_info;
  ObITable::TableKey key;
  ObITable::TableKey key1;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.table_type_ = ObITable::MINI_MINOR_SSTABLE;
  key.trans_version_range_.base_version_ = 100;
  key.trans_version_range_.multi_version_start_ = 120;
  key.trans_version_range_.snapshot_version_ = 200;
  key.log_ts_range_.start_log_ts_ = 10;
  key.log_ts_range_.end_log_ts_ = 20;
  key.log_ts_range_.max_log_ts_ = 20;

  sstable_info.src_table_key_ = key;
  sstable_info.dest_base_version_ = 120;
  sstable_info.dest_log_ts_range_ = key.log_ts_range_;

  key.trans_version_range_.base_version_ = 120;
  key.trans_version_range_.multi_version_start_ = 120;
  key.trans_version_range_.snapshot_version_ = 200;

  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_sstable_info_to_table_key(sstable_info, key1));
  ASSERT_TRUE(key == key1);

  sstable_info.src_table_key_ = key;
  sstable_info.dest_base_version_ = 110;

  key.trans_version_range_.base_version_ = 110;

  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_sstable_info_to_table_key(sstable_info, key1));
  ASSERT_TRUE(key == key1);
}

/*
TEST_F(TestPartitionMigrateTableKeyMgr, test_new_without_cover_perfect_continuous)
{
  const int64_t need_reserve_major_snapshot = 100;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 100, 400), (10,40)], [(400, 400, 500), (40, 50)]
  // remote_gc_inc_tables: [(100, 100, 290), (10, 30)], [(290, 300, 400), (30, 40)]
  // copy_sstables: [(300 //dest_base_version, 400 //dest_snapshot_version), (30, 40)], [(400, 500), (40, 50)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 290;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, remote_gc_inc_tables);

  version_range.base_version_ = 290;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_gc_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(1),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 100, 400), (10,40)], [(400, 400, 500), (40, 50)]
  // remote_gc_inc_tables: [(100, 100, 290), (10, 30)], [(310, 310, 400), (30, 40)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]

  remote_gc_inc_tables.at(0).trans_version_range_.snapshot_version_ = 310;
  remote_gc_inc_tables.at(1).trans_version_range_.base_version_ = 310;
  remote_gc_inc_tables.at(1).trans_version_range_.multi_version_start_ = 310;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(1),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 410), (20,30)]
  // remote_inc_tables: [(100, 100, 400), (10,40)], [(400, 400, 500), (40, 50)]
  // remote_gc_inc_tables: [(100, 100, 290), (10, 30)], [(310, 310, 400), (30, 40)]
  // copy_sstables: [(410, 410), (30, 40)], [(410, 500), (40, 50)]

  local_inc_tables.at(1).trans_version_range_.snapshot_version_ = 410;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_inc_tables.at(1),
        410, 410, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(1),
        410, 500, 40));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_without_cover_cross)
{
  const int64_t need_reserve_major_snapshot = 100;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 100, 250), (10,25)], [(250, 300, 500), (25, 50)]
  // remote_gc_inc_tables: [(0, 100, 100), (0, 10)]
  // copy_sstables: [(300, 500), (30, 50)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 250;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 25;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 250;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 25;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 100;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 10;
  add_table_key(table_type, version_range, log_ts_range, remote_gc_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 500, 30));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 100, 310), (10,25)], [(310, 310, 500), (25, 50)]
  // remote_gc_inc_tables: [(0, 100, 100), (0, 10)]
  // copy_sstables: [(300, 500), (30, 50)]

  remote_inc_tables.at(0).trans_version_range_.snapshot_version_= 310;
  remote_inc_tables.at(1).trans_version_range_.base_version_ = 310;
  remote_inc_tables.at(1).trans_version_range_.multi_version_start_ = 310;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 500, 30));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 510), (20,30)]
  // remote_inc_tables: [(100, 100, 310), (10,25)], [(310, 310, 500), (25, 50)]
  // remote_gc_inc_tables: [(0, 100, 100), (0, 10)]
  // copy_sstables: [(510, 500), (30, 50)]

  local_inc_tables.at(1).trans_version_range_.snapshot_version_ = 510;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        510, 510, 30));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_without_cover_not_continuous)
{
  const int64_t need_reserve_major_snapshot = 100;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(500, 500, 550), (50, 55)]
  // remote_gc_inc_tables: [(400, 400, 500), (40, 50)]
  // copy_sstables: [(400, 500), (40, 50)], [(500, 550), (50, 55)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 500;
  version_range.multi_version_start_ = 500;
  version_range.snapshot_version_ = 550;
  log_ts_range.start_log_ts_ = 50;
  log_ts_range.end_log_ts_ = 55;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_gc_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_inc_tables.at(0),
        400, 500, 40));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(0),
        500, 550, 50));

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  remote_gc_inc_tables.reuse();
  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(500, 500, 550), (50, 55)]
  // remote_gc_inc_tables: [(400, 400, 500), (30, 50)]
  // copy_sstables: [(300, 500), (30, 50)], [(500, 550), (50, 55)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 500;
  version_range.multi_version_start_ = 500;
  version_range.snapshot_version_ = 550;
  log_ts_range.start_log_ts_ = 50;
  log_ts_range.end_log_ts_ = 55;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_gc_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_inc_tables.at(0),
        300, 500, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(0),
        500, 550, 50));
}


TEST_F(TestPartitionMigrateTableKeyMgr, test_new_without_cover_has_cover)
{
  const int64_t need_reserve_major_snapshot = 100;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = false;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(500, 500, 550), (50, 55)]
  // remote_gc_inc_tables: [(50, 100, 500), (5, 50)]
  // copy_sstables: [(300, 500), (30, 50)], [(500, 550), (50, 55)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 500;
  version_range.multi_version_start_ = 500;
  version_range.snapshot_version_ = 550;
  log_ts_range.start_log_ts_ = 50;
  log_ts_range.end_log_ts_ = 55;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 50;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 5;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_gc_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_gc_inc_tables.at(0),
        300, 500, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(0),
        500, 550, 50));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_with_cover_perfect_continuous)
{
  int64_t need_reserve_major_snapshot = 100;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 290), (10, 30)], [(290, 300, 400), (30, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 290;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 290;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 30)], [(310, 310, 400), (30, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]

  remote_inc_tables.at(0).trans_version_range_.snapshot_version_ = 310;
  remote_inc_tables.at(1).trans_version_range_.base_version_ = 310;
  remote_inc_tables.at(1).trans_version_range_.multi_version_start_ = 310;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 410), (20,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 30)], [(310, 310, 400), (30, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(410, 410), (30, 40)], [(410, 500), (40, 50)]

  local_inc_tables.at(1).trans_version_range_.snapshot_version_ = 410;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        410, 410, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        410, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 410), (20,30)], [(410, 410, 510), (30,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 30)], [(310, 310, 400), (30, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(510, 510), (30, 40)], [(510, 510), (40, 50)]

  version_range.base_version_ = 410;
  version_range.multi_version_start_ = 410;
  version_range.snapshot_version_ = 510;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        510, 510, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        510, 510, 40));
  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 410), (20,30)], [(410, 410, 510), (30,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 30)], [(310, 310, 400), (30, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(510, 510), (30, 40)], [(510, 510), (40, 50)]

  need_reserve_major_snapshot = 250;
  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        510, 510, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        510, 510, 40));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_with_cover_cross)
{
  const int64_t need_reserve_major_snapshot = 100;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 290), (10, 25)], [(290, 300, 400), (25, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 290;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 25;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 290;
  version_range.multi_version_start_ = 290;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 25;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 25)], [(310, 310, 400), (25, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]

  remote_inc_tables.at(0).trans_version_range_.snapshot_version_ = 310;
  remote_inc_tables.at(1).trans_version_range_.base_version_ = 310;
  remote_inc_tables.at(1).trans_version_range_.multi_version_start_ = 310;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 410), (20,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 25)], [(310, 310, 400), (25, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(410, 410), (30, 40)], [(410, 500), (40, 50)]

  local_inc_tables.at(1).trans_version_range_.snapshot_version_ = 410;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        410, 410, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        410, 500, 40));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_with_cover_has_cover_cannot_cover)
{
  int64_t need_reserve_major_snapshot = 250;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 200), (10, 20)], [(200, 290, 400), (20, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 290;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 20)], [(310, 310, 400), (20, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(300, 400), (30, 40)], [(400, 500), (40, 50)]


  remote_inc_tables.at(0).trans_version_range_.snapshot_version_ = 310;
  remote_inc_tables.at(0).trans_version_range_.multi_version_start_ = 310;
  remote_inc_tables.at(1).trans_version_range_.base_version_ = 310;
  remote_inc_tables.at(1).trans_version_range_.multi_version_start_ = 310;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 400, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 410), (20,30)]
  // remote_inc_tables: [(100, 200, 310), (10, 20)], [(310, 310, 400), (20, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(410, 410), (30, 40)], [(410, 500), (40, 50)]

  local_inc_tables.at(1).trans_version_range_.snapshot_version_ = 410;

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        410, 410, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        410, 500, 40));

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(210, 210, 330), (20,20)]
  // remote_inc_tables: [(150, 300, 400), (10, 45)]
  // copy_sstables: [(330, 400), (20, 45)]

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  need_reserve_major_snapshot = 250;
  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 210;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 210;
  version_range.multi_version_start_ = 210;
  version_range.snapshot_version_ = 330;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 150;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 45;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(0),
        330, 400, 20));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_with_cover_has_cover_can_cover)
{
  int64_t need_reserve_major_snapshot = 250;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(210, 210, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 200), (10, 20)], [(200, 250, 400), (20, 40)], [(400, 400, 500), (40, 50)]
  // copy_sstables: [(210, 400), (30, 40)], [(400, 500), (40, 50)]

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 210;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 210;
  version_range.multi_version_start_ = 210;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 250;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        210, 400, 20));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        400, 500, 40));

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(210, 210, 300), (20,30)], [(300, 310, 410), (30, 40)]
  // remote_inc_tables: [(100, 200, 200), (10, 25)], [(200, 300, 400), (25, 45)], [(400, 400, 500), (45, 50)]
  // copy_sstables: [(300, 410), (30, 45)], [(410, 500), (45, 50)]

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 210;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 210;
  version_range.multi_version_start_ = 210;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 300;
  version_range.multi_version_start_ = 310;
  version_range.snapshot_version_ = 410;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 25;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 25;
  log_ts_range.end_log_ts_ = 45;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 45;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(1),
        300, 410, 30));
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(1), remote_inc_tables.at(2),
        410, 500, 45));

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(210, 210, 300), (20,30)], [(300, 310, 400), (30, 40)]
  // remote_inc_tables: [(150, 200, 300), (10, 45)]
  // copy_sstables: [(100, 400), (10, 45)]

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  need_reserve_major_snapshot = 250;
  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 210;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 210;
  version_range.multi_version_start_ = 210;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 300;
  version_range.multi_version_start_ = 310;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 150;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 45;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(0),
        100, 400, 10));


  // local_inc_tables: [(100, 100, 210), (10, 20)], [(210, 210, 330), (20,20)]
  // remote_inc_tables: [(150, 300, 400), (20, 45)]
  // copy_sstables: [(330, 400), (20, 45)]

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  need_reserve_major_snapshot = 250;
  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 210;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 210;
  version_range.multi_version_start_ = 210;
  version_range.snapshot_version_ = 330;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 150;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 45;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(0),
        330, 400, 20));

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(210, 210, 330), (20,20)]
  // remote_inc_tables: [(150, 300, 300), (20, 45)]
  // copy_sstables: [(330, 330), (20, 45)]

  remote_inc_tables.at(0).trans_version_range_.snapshot_version_ = 300;
  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(0),
        330, 330, 20));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_new_with_cover_not_continuous)
{
  int64_t need_reserve_major_snapshot = 250;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(200, 400, 500), (40, 50)]
  // copy_sstables: [(200, 500), (40, 50)]

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(0),
        200, 500, 40));

  // local_inc_tables: [(100, 100, 210), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(400, 400, 500), (30, 40)]
  // copy_sstables: [(300, 500), (30, 40)]

  local_inc_tables.reuse();
  remote_inc_tables.reuse();
  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS, ObDestTableKeyManager::convert_needed_inc_table_key_v2(
        need_reserve_major_snapshot,
        local_inc_tables,
        remote_inc_tables,
        remote_gc_inc_tables,
        is_copy_cover_minor,
        copy_sstables));

  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(OB_SUCCESS, check_same(copy_sstables.at(0), remote_inc_tables.at(0),
        300, 500, 30));
}
*/

TEST_F(TestPartitionMigrateTableKeyMgr, test_copy_nothing)
{
  int64_t need_reserve_major_snapshot = 250;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  // local_inc_tables: [(100, 100, 200), (10, 20)], [(200, 200, 300), (20,30)]
  // remote_inc_tables: [(100, 200, 300), (10, 30)]
  // copy_sstables:

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key_v2(need_reserve_major_snapshot,
          local_inc_tables,
          remote_inc_tables,
          remote_gc_inc_tables,
          is_copy_cover_minor,
          copy_sstables));

  ASSERT_EQ(0, copy_sstables.count());

  is_copy_cover_minor = false;
  ASSERT_EQ(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key_v2(need_reserve_major_snapshot,
          local_inc_tables,
          remote_inc_tables,
          remote_gc_inc_tables,
          is_copy_cover_minor,
          copy_sstables));

  ASSERT_EQ(0, copy_sstables.count());
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_check_continue)
{
  int64_t need_reserve_major_snapshot = 250;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> remote_gc_inc_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  const bool is_copy_cover_minor = true;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 210;
  version_range.multi_version_start_ = 210;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_NE(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key_v2(need_reserve_major_snapshot,
          local_inc_tables,
          remote_inc_tables,
          remote_gc_inc_tables,
          is_copy_cover_minor,
          copy_sstables));

  version_range.base_version_ = 100;
  version_range.multi_version_start_ = 100;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 20;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 21;
  log_ts_range.end_log_ts_ = 30;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables);

  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 40;
  log_ts_range.end_log_ts_ = 50;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables);

  copy_sstables.reuse();
  ASSERT_NE(OB_SUCCESS,
      ObDestTableKeyManager::convert_needed_inc_table_key_v2(need_reserve_major_snapshot,
          local_inc_tables,
          remote_inc_tables,
          remote_gc_inc_tables,
          is_copy_cover_minor,
          copy_sstables));
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_build_migrate_major_sstable_no_new_major)
{
  ObMigratePrepareTask task;
  ObMigrateCtx ctx;
  task.ctx_ = &ctx;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> local_major_tables;
  ObArray<ObITable::TableKey> remote_major_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  log_ts_range.max_log_ts_ = 1000;
  ObITable::TableType table_type = ObITable::MAJOR_SSTABLE;
  int64_t need_reserve_major_snapshot = 0;
  bool need_reuse_local_minor = true;
  ObVersion version(2);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 200]
  // remote_inc_tables: [(400, 400, 500), (30, 40)]
  // copy_sstables:

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(0, copy_sstables.count());
  ASSERT_EQ(0, need_reserve_major_snapshot);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 200]
  // remote_inc_tables: [(400, 400, 500), (32, 40)]
  // copy_sstables:

  local_major_tables.reuse();
  local_inc_tables.reuse();
  remote_major_tables.reuse();
  remote_inc_tables.reuse();
  copy_sstables.reuse();
  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 32;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  need_reuse_local_minor = false;
  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(0, copy_sstables.count());
  ASSERT_EQ(0, need_reserve_major_snapshot);
}

TEST_F(TestPartitionMigrateTableKeyMgr, test_build_migrate_major_sstable_with_new_major)
{
  ObMigratePrepareTask task;
  ObMigrateCtx ctx;
  task.ctx_ = &ctx;
  ObArray<ObITable::TableKey> local_inc_tables;
  ObArray<ObITable::TableKey> remote_inc_tables;
  ObArray<ObITable::TableKey> local_major_tables;
  ObArray<ObITable::TableKey> remote_major_tables;
  ObArray<ObMigrateTableInfo::SSTableInfo> copy_sstables;
  ObVersionRange version_range;
  ObLogTsRange log_ts_range;
  ObITable::TableType table_type = ObITable::MAJOR_SSTABLE;
  int64_t need_reserve_major_snapshot = 0;
  bool need_reuse_local_minor = true;
  ObVersion version(2);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 200], [0, 300]
  // remote_inc_tables: [(200, 300, 500), (30, 40)]
  // copy_sstables:

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 3;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  // ASSERT_EQ(300, need_reserve_major_snapshot);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 290), (10, 30)]
  // remote_major_tables: [0, 200], [0, 300]
  // remote_inc_tables: [(300, 400, 500), (20, 30)], [(500, 500, 600), (30, 40)]
  // copy_sstables: [0, 300]

  local_major_tables.reuse();
  local_inc_tables.reuse();
  remote_major_tables.reuse();
  remote_inc_tables.reuse();
  copy_sstables.reuse();
  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 290;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 3;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 300;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 500;
  version_range.multi_version_start_ = 500;
  version_range.snapshot_version_ = 600;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(remote_major_tables.at(1), copy_sstables.at(0).src_table_key_);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 200], [0, 300]
  // remote_inc_tables: [(300, 400, 500), (20, 30)], [(500, 500, 600), (30, 40)]
  // copy_sstables: [0, 300]

  local_major_tables.reuse();
  local_inc_tables.reuse();
  remote_major_tables.reuse();
  remote_inc_tables.reuse();
  copy_sstables.reuse();
  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 3;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 300;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 20;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 500;
  version_range.multi_version_start_ = 500;
  version_range.snapshot_version_ = 600;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  // ASSERT_EQ(300, need_reserve_major_snapshot);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 200], [0, 400]
  // remote_inc_tables: [(400, 400, 500), (32, 40)]
  // copy_sstables: [0, 300]

  local_major_tables.reuse();
  local_inc_tables.reuse();
  remote_major_tables.reuse();
  remote_inc_tables.reuse();
  copy_sstables.reuse();
  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 3;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 400;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 32;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  need_reuse_local_minor = false;
  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(remote_major_tables.at(1), copy_sstables.at(0).src_table_key_);
  ASSERT_EQ(0, need_reserve_major_snapshot);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 200], [0, 300]
  // remote_inc_tables: [(300, 400, 500), (32, 40)]
  // copy_sstables: [0, 300]

  local_major_tables.reuse();
  local_inc_tables.reuse();
  remote_major_tables.reuse();
  remote_inc_tables.reuse();
  copy_sstables.reuse();
  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 3;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 300;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 32;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(1, copy_sstables.count());
  ASSERT_EQ(remote_major_tables.at(1), copy_sstables.at(0).src_table_key_);
  ASSERT_EQ(0, need_reserve_major_snapshot);

  // local_major_tables: [0, 200]
  // local_inc_tables: [(180, 200, 300), (10, 30)]
  // remote_major_tables: [0, 300], [0, 400]
  // remote_inc_tables: [(200, 300, 500), (30, 40)]
  // copy_sstables: [0, 300]

  local_major_tables.reuse();
  local_inc_tables.reuse();
  remote_major_tables.reuse();
  remote_inc_tables.reuse();
  copy_sstables.reuse();
  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 200;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 2;
  add_table_key(table_type, version_range, log_ts_range, local_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 180;
  version_range.multi_version_start_ = 200;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 10;
  log_ts_range.end_log_ts_ = 30;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 300;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 3;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MAJOR_SSTABLE;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 400;
  version_range.snapshot_version_ = 400;
  log_ts_range.start_log_ts_ = 0;
  log_ts_range.end_log_ts_ = 0;
  version = 4;
  add_table_key(table_type, version_range, log_ts_range, remote_major_tables, version);

  table_type = ObITable::MINI_MINOR_SSTABLE;
  version_range.base_version_ = 200;
  version_range.multi_version_start_ = 300;
  version_range.snapshot_version_ = 500;
  log_ts_range.start_log_ts_ = 30;
  log_ts_range.end_log_ts_ = 40;
  version = 0;
  add_table_key(table_type, version_range, log_ts_range, remote_inc_tables, version);

  need_reuse_local_minor = true;
  ASSERT_EQ(OB_SUCCESS,
      task.build_migrate_major_sstable(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables));
  ASSERT_EQ(2, copy_sstables.count());
  ASSERT_EQ(remote_major_tables.at(0), copy_sstables.at(0).src_table_key_);
  ASSERT_EQ(remote_major_tables.at(1), copy_sstables.at(1).src_table_key_);
  ASSERT_EQ(0, need_reserve_major_snapshot);
}

/*
 *TEST_F(TestPartitionMigrateTableKeyMgr, test_build_remote_minor_sstables)
 *{
 *  ObMigratePrepareTask task;
 *  ObMigrateCtx ctx;
 *  task.ctx_ = &ctx;
 *  ctx.fetch_pg_info_compat_version_ = ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2;
 *  ObArray<ObITable::TableKey> local_inc_tables;
 *  ObArray<ObITable::TableKey> tmp_remote_inc_tables;
 *  ObArray<ObITable::TableKey> tmp_remote_gc_inc_tables;
 *  ObArray<ObITable::TableKey> remote_inc_tables;
 *  ObArray<ObITable::TableKey> remote_gc_inc_tables;
 *  ObVersionRange version_range;
 *  ObLogTsRange log_ts_range;
 *  ObITable::TableType table_type = ObITable::MINI_MINOR_SSTABLE;
 *  bool need_reuse_local_minor = true;
 *  ObVersion version(0);
 *
 *  // local_inc_tables: [(10, 30, 40)]
 *  // tmp_remote_inc_tables : [(10, 40, 45)]
 *  // tmp_remote_gc_inc_tables: [(30, 40, 45)]
 *  // remote_inc_tables: [(10, 40, 45)]
 *  // remote_gc_inc_tables: [(10, 40, 45)]
 *
 *
 *  table_type = ObITable::MINI_MINOR_SSTABLE;
 *  version_range.base_version_ = 100;
 *  version_range.multi_version_start_ = 200;
 *  version_range.snapshot_version_ = 200;
 *  log_ts_range.start_log_ts_ = 10;
 *  log_ts_range.end_log_ts_ = 30;
 *  log_ts_range.max_log_ts_ = 40;
 *  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);
 *
 *  table_type = ObITable::MINI_MINOR_SSTABLE;
 *  version_range.base_version_ = 100;
 *  version_range.multi_version_start_ = 200;
 *  version_range.snapshot_version_ = 300;
 *  log_ts_range.start_log_ts_ = 10;
 *  log_ts_range.end_log_ts_ = 40;
 *  log_ts_range.max_log_ts_ = 45;
 *  add_table_key(table_type, version_range, log_ts_range, tmp_remote_inc_tables, version);
 *
 *  table_type = ObITable::MINI_MINOR_SSTABLE;
 *  version_range.base_version_ = 200;
 *  version_range.multi_version_start_ = 200;
 *  version_range.snapshot_version_ = 300;
 *  log_ts_range.start_log_ts_ = 30;
 *  log_ts_range.end_log_ts_ = 40;
 *  log_ts_range.max_log_ts_ = 45;
 *  add_table_key(table_type, version_range, log_ts_range, tmp_remote_gc_inc_tables, version);
 *
 *  ASSERT_EQ(OB_SUCCESS, task.build_remote_minor_sstables(
 *        local_inc_tables,
 *        tmp_remote_inc_tables,
 *        tmp_remote_gc_inc_tables,
 *        remote_inc_tables,
 *        remote_gc_inc_tables,
 *        need_reuse_local_minor));
 *  ASSERT_EQ(1, remote_inc_tables.count());
 *  ASSERT_EQ(1, remote_gc_inc_tables.count());
 *  ASSERT_EQ(tmp_remote_inc_tables.at(0), remote_inc_tables.at(0));
 *  ASSERT_EQ(tmp_remote_gc_inc_tables.at(0), remote_gc_inc_tables.at(0));
 *  ASSERT_TRUE(need_reuse_local_minor);
 *
 *  // local_inc_tables: [(10, 30, 40)]
 *  // tmp_remote_inc_tables : [(32, 40, 45)]
 *  // tmp_remote_gc_inc_tables: [(35, 40, 45)]
 *  // remote_inc_tables: [(32, 40, 45)]
 *  // remote_gc_inc_tables:
 *
 *  need_reuse_local_minor = true;
 *  tmp_remote_inc_tables.reuse();
 *  tmp_remote_gc_inc_tables.reuse();
 *  remote_inc_tables.reuse();
 *  remote_gc_inc_tables.reuse();
 *  table_type = ObITable::MINI_MINOR_SSTABLE;
 *  version_range.base_version_ = 100;
 *  version_range.multi_version_start_ = 200;
 *  version_range.snapshot_version_ = 200;
 *  log_ts_range.start_log_ts_ = 10;
 *  log_ts_range.end_log_ts_ = 30;
 *  log_ts_range.max_log_ts_ = 40;
 *  add_table_key(table_type, version_range, log_ts_range, local_inc_tables, version);
 *
 *  table_type = ObITable::MINI_MINOR_SSTABLE;
 *  version_range.base_version_ = 200;
 *  version_range.multi_version_start_ = 200;
 *  version_range.snapshot_version_ = 300;
 *  log_ts_range.start_log_ts_ = 32;
 *  log_ts_range.end_log_ts_ = 40;
 *  log_ts_range.max_log_ts_ = 45;
 *  add_table_key(table_type, version_range, log_ts_range, tmp_remote_inc_tables, version);
 *
 *  table_type = ObITable::MINI_MINOR_SSTABLE;
 *  version_range.base_version_ = 200;
 *  version_range.multi_version_start_ = 200;
 *  version_range.snapshot_version_ = 300;
 *  log_ts_range.start_log_ts_ = 35;
 *  log_ts_range.end_log_ts_ = 40;
 *  log_ts_range.max_log_ts_ = 45;
 *  add_table_key(table_type, version_range, log_ts_range, tmp_remote_gc_inc_tables, version);
 *
 *  ASSERT_EQ(OB_SUCCESS, task.build_remote_minor_sstables(
 *        local_inc_tables,
 *        tmp_remote_inc_tables,
 *        tmp_remote_gc_inc_tables,
 *        remote_inc_tables,
 *        remote_gc_inc_tables,
 *        need_reuse_local_minor));
 *  ASSERT_EQ(1, remote_inc_tables.count());
 *  ASSERT_EQ(0, remote_gc_inc_tables.count());
 *  ASSERT_EQ(tmp_remote_inc_tables.at(0), remote_inc_tables.at(0));
 *  ASSERT_FALSE(need_reuse_local_minor);
 *
 *}
 */

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = 0;
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);

  // OB_LOGGER.set_log_level("ERROR");
  // ret = RUN_ALL_TESTS();
  return ret;
}
