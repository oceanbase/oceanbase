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
#include "gtest/gtest.h"
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "test_backup.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

class TestBackupTmpFile : public TestDataFilePrepare {
public:
  TestBackupTmpFile();
  virtual ~TestBackupTmpFile();
  virtual void SetUp();
  virtual void TearDown();

private:
  DISALLOW_COPY_AND_ASSIGN(TestBackupTmpFile);
};

TestBackupTmpFile::TestBackupTmpFile() : TestDataFilePrepare(&getter, "TestBackupTmpFile")
{}

TestBackupTmpFile::~TestBackupTmpFile()
{}

void TestBackupTmpFile::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();

  ret = getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  static ObTenantBase tenant_ctx(1);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
}

void TestBackupTmpFile::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}

static void make_meta_index(backup::ObBackupMetaIndex &meta_index)
{
  meta_index.reset();
  make_random_tablet_id(meta_index.meta_key_.tablet_id_);
  make_random_meta_type(meta_index.meta_key_.meta_type_);
  make_random_backup_set_id(meta_index.backup_set_id_);
  make_random_ls_id(meta_index.ls_id_);
  make_random_turn_id(meta_index.turn_id_);
  make_random_retry_id(meta_index.retry_id_);
  make_random_file_id(meta_index.file_id_);
  make_random_offset(meta_index.offset_);
  make_random_length(meta_index.length_);
}

static void make_macro_index(backup::ObBackupMacroBlockIndex &macro_index)
{
  macro_index.reset();
  make_random_logic_id(macro_index.logic_id_);
  make_random_backup_set_id(macro_index.backup_set_id_);
  make_random_ls_id(macro_index.ls_id_);
  make_random_turn_id(macro_index.turn_id_);
  make_random_retry_id(macro_index.retry_id_);
  make_random_file_id(macro_index.file_id_);
  make_random_offset(macro_index.offset_);
  make_random_length(macro_index.length_);
}

static int prepare_meta_index_list(const int64_t count, common::ObIArray<backup::ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  backup::ObBackupMetaIndex meta_index;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    meta_index.reset();
    make_meta_index(meta_index);
    EXPECT_EQ(true, meta_index.is_valid());
    EXPECT_EQ(OB_SUCCESS, index_list.push_back(meta_index));
  }
  return ret;
}

static int prepare_macro_index_list(const int64_t count, common::ObIArray<backup::ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  backup::ObBackupMacroBlockIndex macro_index;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    macro_index.reset();
    make_macro_index(macro_index);
    EXPECT_EQ(true, macro_index.is_valid());
    EXPECT_EQ(OB_SUCCESS, index_list.push_back(macro_index));
  }
  return ret;
}

template <class T>
static int put(const common::ObIArray<T> &list, backup::ObBackupIndexBufferNode &node)
{
  int ret = OB_SUCCESS;
  if (!node.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer node do not init", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const T &item = list.at(i);
    EXPECT_EQ(OB_SUCCESS, node.put_backup_index(item));
  }
  return ret;
}

template <class T>
int get(backup::ObBackupIndexBufferNode &node, common::ObIArray<T> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  const int64_t write_count = node.get_write_count();
  if (!node.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer node do not init", K(ret));
  }
  T item;
  for (int64_t i = 0; OB_SUCC(ret) && i < write_count; ++i) {
    item.reset();
    EXPECT_EQ(OB_SUCCESS, node.get_backup_index(item));
    EXPECT_EQ(OB_SUCCESS, list.push_back(item));
  }
  return ret;
}

TEST_F(TestBackupTmpFile, test_read_write_meta_index)
{
  int ret = OB_SUCCESS;
  const int64_t count = 7;
  const int64_t count_list[count] = {1, 10, 100, 1000, 10000, 100000, 1000000};
  common::ObArray<backup::ObBackupMetaIndex> write_meta_index_list;
  common::ObArray<backup::ObBackupMetaIndex> read_meta_index_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    write_meta_index_list.reset();
    read_meta_index_list.reset();
    ObBackupIndexBufferNode node;
    EXPECT_EQ(OB_SUCCESS, node.init(1 /*tenant_id*/, BACKUP_BLOCK_META_INDEX, 1 /*node_level*/));
    EXPECT_EQ(OB_SUCCESS, prepare_meta_index_list(count, write_meta_index_list));
    EXPECT_EQ(OB_SUCCESS, put(write_meta_index_list, node));
    EXPECT_EQ(OB_SUCCESS, get(node, read_meta_index_list));
    EXPECT_EQ(true, cmp(write_meta_index_list, read_meta_index_list));
    LOG_INFO("read and write meta index", K(i));
  }
}

TEST_F(TestBackupTmpFile, test_read_write_macro_index)
{
  int ret = OB_SUCCESS;
  const int64_t count = 7;
  const int64_t count_list[count] = {1, 10, 100, 1000, 10000, 100000, 1000000};
  common::ObArray<backup::ObBackupMacroBlockIndex> write_macro_index_list;
  common::ObArray<backup::ObBackupMacroBlockIndex> read_macro_index_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    write_macro_index_list.reset();
    read_macro_index_list.reset();
    ObBackupIndexBufferNode node;
    EXPECT_EQ(OB_SUCCESS, node.init(1 /*tenant_id*/, BACKUP_BLOCK_MACRO_INDEX, 1 /*node_level*/));
    EXPECT_EQ(OB_SUCCESS, prepare_macro_index_list(count, write_macro_index_list));
    EXPECT_EQ(OB_SUCCESS, put(write_macro_index_list, node));
    EXPECT_EQ(OB_SUCCESS, get(node, read_macro_index_list));
    EXPECT_EQ(true, cmp(write_macro_index_list, read_macro_index_list));
    LOG_INFO("read and write macro index", K(i));
  }
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_tmp_file.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_tmp_file.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
