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

#include <cstdio>

#include "lib/utility/ob_test_util.h"
#include "storage/backup/test_backup.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "lib/allocator/page_arena.h"
#include "storage/backup/ob_backup_linked_item.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace backup
{

static ObSimpleMemLimitGetter getter;

class TestBackupBlockFileReaderWriter : public TestDataFilePrepare
{
public:
  TestBackupBlockFileReaderWriter();
  virtual ~TestBackupBlockFileReaderWriter();
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

private:
  void clean_env_();

protected:
  ObTenantBase tenant_base_;
  DISALLOW_COPY_AND_ASSIGN(TestBackupBlockFileReaderWriter);
};

int prepare_backup_dest(ObBackupDest &backup_dest, ObBackupPath &file_list_dir)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  if (OB_FAIL(databuff_printf(test_dir_, sizeof(test_dir_),
      "%s/test_backup_block_file_reader_writer_dir", get_current_dir_name()))) {
    LOG_WARN("failed to databuff_printf", K(ret));
  } else if (OB_FAIL(databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_))) {
    LOG_WARN("failed to databuff printf", K(ret));
  } else if (OB_FAIL(backup_dest.set(test_dir_uri_))) {
    LOG_WARN("failed to set backup dest", K(ret));
  } else if (OB_FAIL(util.mkdir(test_dir_uri_, backup_dest.get_storage_info()))) {
    LOG_WARN("failed to mkdir", K(ret));
  } else if (OB_FAIL(file_list_dir.init(test_dir_uri_))) {
    LOG_WARN("failed to init file list dir", K(ret));
  }
  return ret;
}

int prepare_block_data(const int64_t block_size, ObArenaAllocator &allocator,
    blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  char *data = static_cast<char*>(allocator.alloc(block_size));
  if (OB_ISNULL(data)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(block_size));
  } else {
    // Fill with test pattern
    for (int64_t i = 0; i < block_size; ++i) {
      data[i] = static_cast<char>(i % 256);
    }
    buffer_reader.assign(data, block_size);
  }
  return ret;
}

int write_block_file_blocks(const int64_t block_count, ObBackupBlockFileWriter &writer,
    ObBackupBlockFileAddr &entry_block_addr)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("TestData");
  ObBackupBlockFileAddr current_addr;
  const int64_t block_size = OB_DEFAULT_MACRO_BLOCK_SIZE;

  for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
    blocksstable::ObBufferReader buffer_reader;
    if (OB_FAIL(prepare_block_data(block_size, allocator, buffer_reader))) {
      LOG_WARN("failed to prepare block data", K(ret), K(i));
    } else if (OB_FAIL(writer.write_block(buffer_reader, 1/*item_count*/, 0/*data_type*/, current_addr))) {
      LOG_WARN("failed to write block", K(ret), K(i));
    } else {
      entry_block_addr = current_addr; // Keep track of the last block

      if (i % 10 == 0) {
        LOG_INFO("written blocks", K(i), K(current_addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(writer.close(entry_block_addr))) {
      LOG_WARN("failed to close writer", K(ret), K(entry_block_addr));
    }
  }

  return ret;
}

int read_block_file_blocks(const int64_t expected_block_count, ObBackupBlockFileReader &reader)
{
  int ret = OB_SUCCESS;
  int64_t read_count = 0;

  while (OB_SUCC(ret)) {
    blocksstable::ObBufferReader buffer_reader;
    ObBackupBlockFileAddr block_addr;
    int64_t item_count = 0;
    int64_t data_type = 0;

    if (OB_FAIL(reader.get_next_block(buffer_reader, block_addr, item_count, data_type))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next block", K(ret));
      }
    } else {
      read_count++;
      if (read_count % 10 == 0) {
        LOG_INFO("read blocks", K(read_count), K(block_addr), K(item_count));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (read_count != expected_block_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read block count not match expected", K(ret), K(read_count), K(expected_block_count));
    } else {
      LOG_INFO("successfully read all blocks", K(read_count));
    }
  }

  return ret;
}

TestBackupBlockFileReaderWriter::TestBackupBlockFileReaderWriter()
  : TestDataFilePrepare(&getter, "TestBackupBlockFileReaderWriter", OB_DEFAULT_MACRO_BLOCK_SIZE, 800),
    tenant_base_(500)
{}

TestBackupBlockFileReaderWriter::~TestBackupBlockFileReaderWriter()
{}

void TestBackupBlockFileReaderWriter::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();

  ret = getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);
  ObTenantEnv::set_tenant(&tenant_ctx);
}

void TestBackupBlockFileReaderWriter::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestBackupBlockFileReaderWriter::clean_env_()
{
  char test_dir_[OB_MAX_URI_LENGTH];
  databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_block_file_reader_writer_dir", get_current_dir_name());
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

int prepare_one_block_file_item(const int64_t idx,
    blocksstable::MacroBlockId &item)
{
  int ret = OB_SUCCESS;

  // Set macro block id
  item.first_id_ = idx;
  item.second_id_ = idx;
  item.third_id_ = idx;
  item.fourth_id_ = idx;
  item.version_ = MacroBlockId::MACRO_BLOCK_ID_VERSION_V2;

  return ret;
}

int prepare_block_file_item_list(const int64_t item_count,
    common::ObIArray<blocksstable::MacroBlockId> &item_list)
{
  int ret = OB_SUCCESS;
  item_list.reset();
  blocksstable::MacroBlockId item;

  for (int64_t i = 1; OB_SUCC(ret) && i <= item_count; ++i) {
    if (OB_FAIL(prepare_one_block_file_item(i, item))) {
      LOG_WARN("failed to prepare one item", K(ret), K(i));
    } else if (!item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid item", K(ret), K(item));
    } else if (OB_FAIL(item_list.push_back(item))) {
      LOG_WARN("failed to push back", K(ret), K(item));
    }
  }

  return ret;
}

int prepare_file_path_item(const int64_t idx, ObBackupFileInfo &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  const bool is_file = (idx % 2 == 0);
  item.type_ = is_file ? BACKUP_FILE_LIST_FILE_INFO : BACKUP_FILE_LIST_DIR_INFO;
  item.file_size_ = is_file ? idx * 128 : 0;
  char path_buf[OB_MAX_URI_LENGTH] = {0};
  const int64_t written = snprintf(path_buf, sizeof(path_buf),
      "/backup/tenant_%ld/ls_%ld/path_item_%ld",
      static_cast<int64_t>(1), idx % 1024, idx);
  if (written <= 0 || written >= static_cast<int64_t>(sizeof(path_buf))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("path buffer not enough", K(ret), K(idx), K(written));
  } else if (OB_FAIL(item.path_.assign(path_buf))) {
    LOG_WARN("failed to assign path", K(ret), K(idx), KCSTRING(path_buf));
  }
  return ret;
}

int prepare_file_path_item_list(const int64_t item_count,
    common::ObIArray<ObBackupFileInfo> &item_list)
{
  int ret = OB_SUCCESS;
  item_list.reset();
  ObBackupFileInfo item;
  for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
    if (OB_FAIL(prepare_file_path_item(i, item))) {
      LOG_WARN("failed to prepare file path item", K(ret), K(i));
    } else if (OB_FAIL(item_list.push_back(item))) {
      LOG_WARN("failed to push back file path item", K(ret), K(item));
    }
  }
  return ret;
}

int write_file_path_items_to_writer(const common::ObIArray<ObBackupFileInfo> &item_list,
    ObBackupBlockFileItemWriter &writer)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(item_list, idx, cnt, OB_SUCC(ret)) {
    const ObBackupFileInfo &item = item_list.at(idx);
    if (OB_FAIL(writer.write_item(item))) {
      LOG_WARN("failed to write file path item", K(ret), K(item));
    }
  }
  return ret;
}

int read_file_path_items_from_reader(ObBackupBlockFileItemReader &reader,
    common::ObIArray<ObBackupFileInfo> &item_list)
{
  int ret = OB_SUCCESS;
  item_list.reset();
  ObBackupFileInfo item;
  while (OB_SUCC(ret)) {
    item.reset();
    if (OB_FAIL(reader.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get file path item", K(ret));
      }
    } else if (OB_FAIL(item_list.push_back(item))) {
      LOG_WARN("failed to push back file path item", K(ret));
    }
  }
  return ret;
}

int compare_file_path_item_lists(
    const common::ObIArray<ObBackupFileInfo> &expected,
    const common::ObIArray<ObBackupFileInfo> &actual)
{
  int ret = OB_SUCCESS;
  if (expected.count() != actual.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file path item count mismatch", K(ret), K(expected.count()), K(actual.count()));
  } else {
    ARRAY_FOREACH_X(expected, idx, cnt, OB_SUCC(ret)) {
      const ObBackupFileInfo &lhs = expected.at(idx);
      const ObBackupFileInfo &rhs = actual.at(idx);
      if (lhs.type_ != rhs.type_ || lhs.file_size_ != rhs.file_size_ || lhs.path_ != rhs.path_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file path item mismatch", K(ret), K(idx), K(lhs), K(rhs));
      }
    }
  }
  return ret;
}

int write_items_to_writer(const common::ObIArray<blocksstable::MacroBlockId> &item_list,
    ObBackupBlockFileItemWriter &writer)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(item_list, idx, cnt, OB_SUCC(ret)) {
    const blocksstable::MacroBlockId &item = item_list.at(idx);
    ObBackupBlockFileMacroIdItem wrap(item);
    if (OB_FAIL(writer.write_item(wrap))) {
      LOG_WARN("failed to write item", K(ret), K(item));
    }
  }
  return ret;
}

int read_items_from_reader(ObBackupBlockFileItemReader &reader,
    common::ObIArray<blocksstable::MacroBlockId> &item_list)
{
  int ret = OB_SUCCESS;
  ObBackupBlockFileMacroIdItem item;

  while (OB_SUCC(ret)) {
    item.reset();
    if (OB_FAIL(reader.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next item", K(ret));
      }
    } else if (OB_FAIL(item_list.push_back(item.macro_id_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  return ret;
}

struct ObBackupBlockFileItemCompare
{
  bool operator()(const blocksstable::MacroBlockId &lhs, const blocksstable::MacroBlockId &rhs)
  {
    return lhs < rhs;
  }
};

int sort_and_compare_item_lists(
    common::ObArray<blocksstable::MacroBlockId> &item_list1,
    common::ObArray<blocksstable::MacroBlockId> &item_list2)
{
  int ret = OB_SUCCESS;
  ObBackupBlockFileItemCompare cmp;

  if (item_list1.count() != item_list2.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item list count not same", "count1", item_list1.count(), "count2", item_list2.count());
  } else {
    std::sort(item_list1.begin(), item_list1.end(), cmp);
    std::sort(item_list2.begin(), item_list2.end(), cmp);

    ARRAY_FOREACH_X(item_list1, idx, cnt, OB_SUCC(ret)) {
      const blocksstable::MacroBlockId &item1 = item_list1.at(idx);
      const blocksstable::MacroBlockId &item2 = item_list2.at(idx);
      if (item1 != item2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item not same", K(ret), K(idx), K(cnt), K(item1), K(item2));
      }
    }
  }

  return ret;
}

int write_and_read_file_path_items(const int64_t item_count, const int64_t max_file_size)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  const int64_t backup_set_id = 1;
  ObBackupDest backup_set_dest;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = backup_set_id;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  const int64_t dest_id = 1;

  ObBackupBlockFileItemWriter writer;
  ObBackupBlockFileItemReader reader;
  ObBackupBlockFileAddr entry_block_addr;
  int64_t total_item_count = 0;
  int64_t total_block_count = 0;
  ObStorageIdMod mod;
  mod.storage_id_ = 1;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;

  ObArray<ObBackupFileInfo> write_item_list;
  ObArray<ObBackupFileInfo> read_item_list;
  ObBackupPath file_list_dir;

  if (OB_FAIL(prepare_backup_dest(backup_dest, file_list_dir))) {
    LOG_WARN("failed to prepare backup dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
      backup_dest, backup_set_desc, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret));
  } else if (OB_FAIL(prepare_file_path_item_list(item_count, write_item_list))) {
    LOG_WARN("failed to prepare file path item list", K(ret));
  } else if (OB_FAIL(writer.init(backup_dest.get_storage_info(), ObBackupBlockFileDataType::FILE_PATH_INFO,
                                    ObBackupFileSuffix::BACKUP, file_list_dir, dest_id, max_file_size))) {
    LOG_WARN("failed to init writer", K(ret));
  } else if (OB_FAIL(write_file_path_items_to_writer(write_item_list, writer))) {
    LOG_WARN("failed to write file path items", K(ret));
  } else if (OB_FAIL(writer.close(entry_block_addr, total_item_count, total_block_count))) {
    LOG_WARN("failed to close writer", K(ret));
  } else if (OB_FAIL(reader.init(backup_dest.get_storage_info(), ObBackupBlockFileDataType::FILE_PATH_INFO,
                                    file_list_dir, ObBackupFileSuffix::BACKUP, mod))) {
    LOG_WARN("failed to init reader", K(ret));
  } else if (OB_FAIL(read_file_path_items_from_reader(reader, read_item_list))) {
    LOG_WARN("failed to read file path items", K(ret));
  } else if (OB_FAIL(compare_file_path_item_lists(write_item_list, read_item_list))) {
    LOG_WARN("failed to compare file path items", K(ret));
  } else if (total_item_count != item_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("total file path item count mismatch", K(ret), K(total_item_count), K(item_count));
  }

  return ret;
}

int write_and_read_items(const int64_t item_count, const int64_t max_file_size)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  const int64_t backup_set_id = 1;
  ObBackupDest backup_set_dest;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = backup_set_id;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  const int64_t dest_id = 1;

  ObBackupBlockFileItemWriter writer;
  ObBackupBlockFileItemReader reader;
  ObBackupBlockFileAddr entry_block_addr;
  int64_t total_item_count = 0;
  int64_t total_block_count = 0;
  ObStorageIdMod mod;
  mod.storage_id_ = 1;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  ObMemAttr mem_attr(OB_SYS_TENANT_ID, "TestItemRW");

  ObArray<blocksstable::MacroBlockId> write_item_list;
  ObArray<blocksstable::MacroBlockId> read_item_list;

  ObBackupPath file_list_dir;
  if (OB_FAIL(prepare_backup_dest(backup_dest, file_list_dir))) {
    LOG_WARN("failed to prepare backup dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
      backup_dest, backup_set_desc, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret));
  } else if (OB_FAIL(prepare_block_file_item_list(item_count, write_item_list))) {
    LOG_WARN("failed to prepare block file item list", K(ret));
  } else if (OB_FAIL(writer.init(backup_dest.get_storage_info(), ObBackupBlockFileDataType::MACRO_BLOCK_ID,
                                    ObBackupFileSuffix::BACKUP, file_list_dir, dest_id, max_file_size))) {
    LOG_WARN("failed to init writer", K(ret));
  } else if (OB_FAIL(write_items_to_writer(write_item_list, writer))) {
    LOG_WARN("failed to write items to writer", K(ret));
  } else if (OB_FAIL(writer.close(entry_block_addr, total_item_count, total_block_count))) {
    LOG_WARN("failed to close writer", K(ret));
  } else if (OB_FAIL(reader.init(backup_dest.get_storage_info(), ObBackupBlockFileDataType::MACRO_BLOCK_ID,
                                    file_list_dir, ObBackupFileSuffix::BACKUP, mod))) {
    LOG_WARN("failed to init reader", K(ret));
  } else if (OB_FAIL(read_items_from_reader(reader, read_item_list))) {
    LOG_WARN("failed to read items from reader", K(ret));
  } else if (OB_FAIL(sort_and_compare_item_lists(write_item_list, read_item_list))) {
    LOG_WARN("failed to sort and compare item lists", K(ret));
  } else if (total_item_count != item_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("total item count not match", K(ret), K(total_item_count), K(item_count));
  } else {
    LOG_INFO("successfully write and read ss items", K(item_count), K(total_item_count), K(total_block_count));
  }

  return ret;
}

int write_and_read_blocks(const int64_t block_count, const int64_t max_file_size)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObBackupDest backup_set_dest;
  const int64_t backup_set_id = 1;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = backup_set_id;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  const int64_t dest_id = 1;

  ObBackupBlockFileWriter writer;
  ObBackupBlockFileReader reader;
  ObBackupBlockFileAddr entry_block_addr;
  ObStorageIdMod mod;
  mod.storage_id_ = 1;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  ObBackupPath file_list_dir;

  if (OB_FAIL(prepare_backup_dest(backup_dest, file_list_dir))) {
    LOG_WARN("failed to prepare backup dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
      backup_dest, backup_set_desc, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret));
  } else if (OB_FAIL(writer.init(backup_dest.get_storage_info(),
                                    ObBackupBlockFileDataType::MACRO_BLOCK_ID, ObBackupFileSuffix::BACKUP,
                                    file_list_dir, dest_id, max_file_size))) {
    LOG_WARN("failed to init writer", K(ret));
  } else if (OB_FAIL(write_block_file_blocks(block_count, writer, entry_block_addr))) {
    LOG_WARN("failed to write ss block file blocks", K(ret));
  } else if (OB_FAIL(reader.init(backup_dest.get_storage_info(), ObBackupBlockFileDataType::MACRO_BLOCK_ID,
                                    file_list_dir, ObBackupFileSuffix::BACKUP, mod))) {
    LOG_WARN("failed to init reader", K(ret));
  } else if (OB_FAIL(read_block_file_blocks(block_count, reader))) {
    LOG_WARN("failed to read ss block file blocks", K(ret));
  } else {
    LOG_INFO("successfully write and read ss blocks", K(block_count));
  }

  return ret;
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_single_block)
{
  clean_env_();
  const int64_t block_count = 1;
  OK(write_and_read_blocks(block_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_multiple_blocks)
{
  clean_env_();
  const int64_t block_count = 10;
  OK(write_and_read_blocks(block_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_large_file)
{
  clean_env_();
  const int64_t block_count = 100; // 100 * 2MB = 200MB
  OK(write_and_read_blocks(block_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_multi_file)
{
  clean_env_();
  const int64_t block_count = 3000; // 3000 * 2MB = 6GB, should create multiple files
  OK(write_and_read_blocks(block_count, 1L * 1024L * 1024L * 1024L/*max_file_size*/));
}

// Item-level tests
TEST_F(TestBackupBlockFileReaderWriter, write_and_read_single_item)
{
  clean_env_();
  const int64_t item_count = 1;
  OK(write_and_read_items(item_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_few_items)
{
  clean_env_();
  const int64_t item_count = 100;
  OK(write_and_read_items(item_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_many_items)
{
  clean_env_();
  const int64_t item_count = 10000; // Should fit in one block
  OK(write_and_read_items(item_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_items_multi_blocks)
{
  clean_env_();
  const int64_t item_count = 100000; // Should span multiple blocks
  OK(write_and_read_items(item_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_items_multi_files)
{
  clean_env_();
  const int64_t item_count = 1000000; // Should create multiple files (1M items)
  OK(write_and_read_items(item_count, 1L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_file_path_single_item)
{
  clean_env_();
  const int64_t item_count = 1;
  OK(write_and_read_file_path_items(item_count, 4L * 1024L * 1024L * 1024L/*max_file_size*/));
}

TEST_F(TestBackupBlockFileReaderWriter, write_and_read_file_path_many_items)
{
  clean_env_();
  const int64_t item_count = 5000; // exercises multi-block writing
  OK(write_and_read_file_path_items(item_count, 16L * 1024L * 1024L/*max_file_size*/));
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_block_file_reader_writer.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_block_file_reader_writer.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
